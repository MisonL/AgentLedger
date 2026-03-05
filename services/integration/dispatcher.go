package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	maxErrorBodyLogLength  = 2048
	maxCallbackPayloadSize = 1 << 20
	maxInt                 = int(^uint(0) >> 1)

	eventTypeAlert        = "alert"
	eventTypeWeeklyReport = "weekly_report"
	eventTypeCallback     = "callback_alert"
	labelChannelNone      = "none"
	labelChannelControl   = "control_plane"

	callbackSecretHeader    = "X-Integration-Callback-Secret"
	callbackSourceHeader    = "X-Integration-Callback-Source"
	callbackTimestampHeader = "X-Integration-Callback-Timestamp"
	callbackNonceHeader     = "X-Integration-Callback-Nonce"
	callbackSignatureHeader = "X-Integration-Callback-Signature"
	callbackSourceAPI       = "api"
	callbackSourceNATS      = "nats"
	callbackNonceBytes      = 16

	defaultDispatchProgressTTL       = 6 * time.Hour
	defaultDispatchProgressMaxEntrys = 20000
)

type alertDispatcher struct {
	ctx        context.Context
	log        *slog.Logger
	js         dlqPublisher
	httpClient *http.Client
	cfg        integrationConfig
	metrics    *integrationMetrics
	progress   *dispatchProgressStore
	dedupe     *alertDedupeStore
}

type dispatchError struct {
	retryable  bool
	statusCode int
	message    string
	err        error
}

type channelDispatchResult struct {
	channel  integrationChannel
	duration time.Duration
	err      error
}

type channelDispatchFailure struct {
	channel integrationChannel
	err     error
}

type multiDispatchError struct {
	failures []channelDispatchFailure
}

type retryableError interface {
	Retryable() bool
}

type dlqPublisher interface {
	Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

type callbackStreamManager interface {
	StreamInfo(ctx context.Context, stream string) (*jetstream.StreamInfo, error)
	CreateStream(ctx context.Context, cfg jetstream.StreamConfig) error
	UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) error
}

type jetStreamCallbackStreamManager struct {
	js jetstream.JetStream
}

type dispatchProgressStore struct {
	mu         sync.Mutex
	items      map[string]*dispatchProgressEntry
	ttl        time.Duration
	maxEntries int
}

type dispatchProgressEntry struct {
	successful   map[integrationChannel]struct{}
	dlqPublished bool
	occurredAt   time.Time
	updatedAt    time.Time
}

type alertDedupeStore struct {
	mu         sync.Mutex
	items      map[string]time.Time
	ttl        time.Duration
	maxEntries int
}

type dispatchProgressCleanupResult struct {
	expired int
	evicted int
}

type dlqPayload struct {
	EventType string          `json:"event_type,omitempty"`
	Event     json.RawMessage `json:"event,omitempty"`
	EventRaw  string          `json:"event_raw,omitempty"`
	Error     string          `json:"error"`
	Attempt   int             `json:"attempt"`
	FailedAt  time.Time       `json:"failed_at"`
}

func (e *dispatchError) Error() string {
	if e == nil {
		return ""
	}
	base := strings.TrimSpace(e.message)
	if e.err != nil {
		if base == "" {
			return e.err.Error()
		}
		return fmt.Sprintf("%s: %v", base, e.err)
	}
	return base
}

func (e *dispatchError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *dispatchError) Retryable() bool {
	if e == nil {
		return false
	}
	return e.retryable
}

func (e *multiDispatchError) Error() string {
	if e == nil || len(e.failures) == 0 {
		return "dispatch failed"
	}

	parts := make([]string, 0, len(e.failures))
	for _, failure := range e.failures {
		parts = append(parts, fmt.Sprintf("channel %s: %v", failure.channel, failure.err))
	}
	return strings.Join(parts, "; ")
}

func (e *multiDispatchError) Retryable() bool {
	if e == nil || len(e.failures) == 0 {
		return false
	}

	for _, failure := range e.failures {
		if !isRetryable(failure.err) {
			return false
		}
	}
	return true
}

func (e *multiDispatchError) channels() []integrationChannel {
	if e == nil || len(e.failures) == 0 {
		return nil
	}

	channels := make([]integrationChannel, 0, len(e.failures))
	seen := make(map[integrationChannel]struct{}, len(e.failures))
	for _, failure := range e.failures {
		if _, exists := seen[failure.channel]; exists {
			continue
		}
		seen[failure.channel] = struct{}{}
		channels = append(channels, failure.channel)
	}
	return channels
}

func newAlertDispatcher(ctx context.Context, log *slog.Logger, js jetstream.JetStream, cfg integrationConfig, metrics *integrationMetrics) *alertDispatcher {
	var dedupe *alertDedupeStore
	if cfg.AlertDedupeWindow > 0 {
		dedupe = newAlertDedupeStore(cfg.AlertDedupeWindow, cfg.AlertDedupeMaxEntries)
	}

	return &alertDispatcher{
		ctx:        ctx,
		log:        log,
		js:         js,
		httpClient: &http.Client{},
		cfg:        cfg,
		metrics:    metrics,
		progress:   newDispatchProgressStore(),
		dedupe:     dedupe,
	}
}

func (d *alertDispatcher) handleAlertMessage(msg jetstream.Msg) {
	d.handleMessage(msg, eventTypeAlert)
}

func (d *alertDispatcher) handleWeeklyReportMessage(msg jetstream.Msg) {
	d.handleMessage(msg, eventTypeWeeklyReport)
}

func (d *alertDispatcher) handleCallbackMessage(msg jetstream.Msg) {
	started := time.Now()
	payload := append([]byte(nil), msg.Data()...)
	attempt := extractAttempt(msg)

	err := d.forwardCallback(d.ctx, payload, callbackSourceNATS)
	if err != nil {
		retryable := isRetryable(err)
		if retryable && shouldRetry(attempt, d.cfg.RetryMax) {
			delay := backoffDelay(attempt, d.cfg.RetryBaseDelay, d.cfg.RetryMaxDelay)
			d.observeCallbackMetrics(outcomeRetry, started)
			if nakErr := msg.NakWithDelay(delay); nakErr != nil {
				d.log.Warn("nak callback message with delay failed",
					"error", nakErr,
					"attempt", attempt,
					"retry_delay", delay.String(),
				)
			}
			d.log.Warn("callback forward failed, message will retry",
				"error", err,
				"attempt", attempt,
				"max_retries", d.cfg.RetryMax,
				"retry_delay", delay.String(),
				"duration_ms", time.Since(started).Milliseconds(),
				"source", callbackSourceNATS,
			)
			return
		}

		failedAt := time.Now().UTC()
		if dlqErr := d.publishDLQ(payload, eventTypeCallback, err, attempt, failedAt); dlqErr != nil {
			delay := backoffDelay(attempt, d.cfg.RetryBaseDelay, d.cfg.RetryMaxDelay)
			d.observeCallbackMetrics(outcomeDLQFailed, started)
			if nakErr := msg.NakWithDelay(delay); nakErr != nil {
				d.log.Warn("nak callback message after dlq publish failure failed",
					"error", nakErr,
					"attempt", attempt,
					"retry_delay", delay.String(),
				)
			}
			d.log.Error("publish callback dlq failed, message will retry",
				"error", dlqErr,
				"dlq_subject", d.cfg.DLQSubject,
				"attempt", attempt,
				"retry_delay", delay.String(),
				"source", callbackSourceNATS,
			)
			return
		}

		d.observeCallbackMetrics(outcomeDLQ, started)
		if termErr := msg.Term(); termErr != nil {
			d.log.Warn("term callback message failed",
				"error", termErr,
				"attempt", attempt,
			)
			return
		}

		d.log.Error("callback forward failed permanently",
			"error", err,
			"retryable", retryable,
			"attempt", attempt,
			"max_retries", d.cfg.RetryMax,
			"duration_ms", time.Since(started).Milliseconds(),
			"source", callbackSourceNATS,
		)
		return
	}

	if ackErr := msg.Ack(); ackErr != nil {
		d.log.Warn("ack callback message failed", "error", ackErr, "attempt", attempt)
		return
	}

	d.observeCallbackMetrics(outcomeSuccess, started)
	d.log.Info("callback message forwarded",
		"attempt", attempt,
		"duration_ms", time.Since(started).Milliseconds(),
		"source", callbackSourceNATS,
	)
}

func (d *alertDispatcher) callbackHandler() http.Handler {
	return http.HandlerFunc(d.handleCallbackHTTP)
}

func (d *alertDispatcher) handleCallbackHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if !d.verifyIncomingCallbackSecret(r.Header.Get(callbackSecretHeader)) {
		d.log.Warn("callback request authentication failed", "source", callbackSourceAPI)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	payload, err := io.ReadAll(io.LimitReader(r.Body, maxCallbackPayloadSize+1))
	if err != nil {
		d.log.Warn("read callback request payload failed", "error", err, "source", callbackSourceAPI)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if len(payload) > maxCallbackPayloadSize {
		d.log.Warn("callback request payload exceeds limit", "size", len(payload), "limit", maxCallbackPayloadSize, "source", callbackSourceAPI)
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}

	started := time.Now()
	attempt := 1
	for {
		err = d.forwardCallback(r.Context(), payload, callbackSourceAPI)
		if err == nil {
			d.observeCallbackMetrics(outcomeSuccess, started)
			d.log.Info("callback request forwarded",
				"attempt", attempt,
				"duration_ms", time.Since(started).Milliseconds(),
				"source", callbackSourceAPI,
			)
			w.WriteHeader(http.StatusNoContent)
			return
		}

		retryable := isRetryable(err)
		if retryable && shouldRetry(attempt, d.cfg.RetryMax) {
			delay := backoffDelay(attempt, d.cfg.RetryBaseDelay, d.cfg.RetryMaxDelay)
			d.observeCallbackMetrics(outcomeRetry, started)
			d.log.Warn("callback request forward failed, retrying",
				"error", err,
				"attempt", attempt,
				"max_retries", d.cfg.RetryMax,
				"retry_delay", delay.String(),
				"source", callbackSourceAPI,
			)
			if !waitWithContext(r.Context(), delay) {
				d.observeCallbackMetrics(outcomeDLQ, started)
				d.log.Error("callback request canceled while waiting for retry",
					"attempt", attempt,
					"duration_ms", time.Since(started).Milliseconds(),
					"source", callbackSourceAPI,
				)
				w.WriteHeader(http.StatusRequestTimeout)
				return
			}
			attempt++
			continue
		}

		failedAt := time.Now().UTC()
		if dlqErr := d.publishDLQ(payload, eventTypeCallback, err, attempt, failedAt); dlqErr != nil {
			d.observeCallbackMetrics(outcomeDLQFailed, started)
			d.log.Error("publish callback request dlq failed",
				"error", dlqErr,
				"attempt", attempt,
				"source", callbackSourceAPI,
				"dlq_subject", d.cfg.DLQSubject,
			)
		} else {
			d.observeCallbackMetrics(outcomeDLQ, started)
		}

		d.log.Error("callback request forward failed permanently",
			"error", err,
			"retryable", retryable,
			"attempt", attempt,
			"max_retries", d.cfg.RetryMax,
			"duration_ms", time.Since(started).Milliseconds(),
			"source", callbackSourceAPI,
		)

		w.WriteHeader(callbackErrorToStatus(err))
		return
	}
}

func (d *alertDispatcher) handleMessage(msg jetstream.Msg, eventType string) {
	started := time.Now()
	payload := append([]byte(nil), msg.Data()...)
	attempt := extractAttempt(msg)
	messageKey := dispatchMessageKey(msg, eventType)

	if d.isDLQPublished(messageKey) {
		if termErr := msg.Term(); termErr != nil {
			d.log.Warn("term message failed after previous dlq publish",
				"error", termErr,
				"attempt", attempt,
				"event_type", eventType,
			)
			return
		}
		d.clearDispatchProgress(messageKey)
		d.log.Info("message terminated after previous dlq publish",
			"attempt", attempt,
			"event_type", eventType,
		)
		return
	}

	if eventType == eventTypeAlert && attempt <= 1 && d.shouldSuppressAlert(payload) {
		if ackErr := msg.Ack(); ackErr != nil {
			d.log.Warn("ack suppressed alert message failed",
				"error", ackErr,
				"attempt", attempt,
				"event_type", eventType,
			)
			return
		}
		d.observeSuccessMetrics(eventType, nil)
		d.log.Info("alert message suppressed by dedupe window",
			"attempt", attempt,
			"event_type", eventType,
		)
		return
	}

	results, err := d.dispatchWithMessageKey(messageKey, payload, eventType)
	if err != nil {
		retryable := isRetryable(err)
		if retryable && shouldRetry(attempt, d.cfg.RetryMax) {
			delay := backoffDelay(attempt, d.cfg.RetryBaseDelay, d.cfg.RetryMaxDelay)
			d.observeFailureMetrics(outcomeRetry, eventType, started, results, err)
			if nakErr := msg.NakWithDelay(delay); nakErr != nil {
				d.log.Warn("nak message with delay failed",
					"error", nakErr,
					"attempt", attempt,
					"retry_delay", delay.String(),
					"event_type", eventType,
				)
			}
			d.log.Warn("dispatch failed, message will retry",
				"error", err,
				"attempt", attempt,
				"max_retries", d.cfg.RetryMax,
				"retry_delay", delay.String(),
				"duration_ms", time.Since(started).Milliseconds(),
				"event_type", eventType,
			)
			return
		}

		failedAt := time.Now().UTC()
		if dlqErr := d.publishDLQ(payload, eventType, err, attempt, failedAt); dlqErr != nil {
			delay := backoffDelay(attempt, d.cfg.RetryBaseDelay, d.cfg.RetryMaxDelay)
			d.observeFailureMetrics(outcomeDLQFailed, eventType, started, results, err)
			if nakErr := msg.NakWithDelay(delay); nakErr != nil {
				d.log.Warn("nak message after dlq publish failure failed",
					"error", nakErr,
					"attempt", attempt,
					"retry_delay", delay.String(),
					"event_type", eventType,
				)
			}
			d.log.Error("publish dlq failed, message will retry",
				"error", dlqErr,
				"dlq_subject", d.cfg.DLQSubject,
				"attempt", attempt,
				"retry_delay", delay.String(),
				"event_type", eventType,
			)
			return
		}

		d.observeFailureMetrics(outcomeDLQ, eventType, started, results, err)
		d.markDLQPublished(messageKey)
		if termErr := msg.Term(); termErr != nil {
			d.log.Warn("term message failed",
				"error", termErr,
				"attempt", attempt,
				"event_type", eventType,
			)
			return
		}
		d.clearDispatchProgress(messageKey)

		d.log.Error("dispatch failed permanently",
			"error", err,
			"retryable", retryable,
			"attempt", attempt,
			"max_retries", d.cfg.RetryMax,
			"duration_ms", time.Since(started).Milliseconds(),
			"event_type", eventType,
		)
		return
	}

	if ackErr := msg.Ack(); ackErr != nil {
		d.log.Warn("ack message failed", "error", ackErr, "attempt", attempt, "event_type", eventType)
		return
	}

	d.clearDispatchProgress(messageKey)
	d.observeSuccessMetrics(eventType, results)
	d.log.Info("message dispatched",
		"attempt", attempt,
		"duration_ms", time.Since(started).Milliseconds(),
		"event_type", eventType,
	)
}

func (d *alertDispatcher) dispatch(payload []byte, eventType string) ([]channelDispatchResult, error) {
	return d.dispatchWithMessageKey("", payload, eventType)
}

func (d *alertDispatcher) dispatchWithMessageKey(messageKey string, payload []byte, eventType string) ([]channelDispatchResult, error) {
	channels := d.routeChannels(eventType, payload)
	channels = d.pendingChannels(messageKey, channels)
	results := make([]channelDispatchResult, 0, len(channels))
	if len(channels) == 0 {
		return results, nil
	}

	failures := make([]channelDispatchFailure, 0, len(channels))
	for _, channel := range channels {
		channelStarted := time.Now()
		err := d.dispatchToChannel(messageKey, channel, payload, eventType)
		result := channelDispatchResult{
			channel:  channel,
			duration: time.Since(channelStarted),
			err:      err,
		}
		results = append(results, result)
		if err != nil {
			failures = append(failures, channelDispatchFailure{channel: channel, err: err})
			continue
		}
		d.markChannelSuccess(messageKey, channel)
		if d.metrics != nil {
			d.metrics.observe(outcomeSuccess, string(channel), eventType, result.duration)
		}
	}

	if len(failures) > 0 {
		return results, &multiDispatchError{failures: failures}
	}

	return results, nil
}

func (d *alertDispatcher) routeChannels(eventType string, payload []byte) []integrationChannel {
	enabled := append([]integrationChannel(nil), d.cfg.Channels...)
	if len(enabled) == 0 {
		return nil
	}

	if d.cfg.RoutingMode != routingModeSeverity || eventType != eventTypeAlert {
		return enabled
	}

	switch extractSeverity(payload) {
	case "critical":
		return enabled
	case "warning":
		routed := filterChannels(enabled, channelWebhook, channelWeCom, channelEmail, channelEmailWebhook)
		if len(routed) > 0 {
			return routed
		}
		if d.log != nil {
			d.log.Warn("severity routing resolved to no channels, fallback to enabled channels",
				"severity", "warning",
				"event_type", eventType,
				"enabled_channels", channelsToStrings(enabled),
			)
		}
		return enabled
	default:
		return enabled
	}
}

func extractSeverity(payload []byte) string {
	var envelope struct {
		Severity string `json:"severity"`
	}

	if err := json.Unmarshal(payload, &envelope); err != nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(envelope.Severity))
}

func (d *alertDispatcher) shouldSuppressAlert(payload []byte) bool {
	if d == nil || d.dedupe == nil {
		return false
	}

	key := buildAlertDedupeFingerprint(payload)
	if key == "" {
		return false
	}

	return d.dedupe.rememberOrSuppress(key, time.Now().UTC())
}

func buildAlertDedupeFingerprint(payload []byte) string {
	var envelope struct {
		TenantID  string `json:"tenant_id"`
		BudgetID  string `json:"budget_id"`
		DedupeKey string `json:"dedupe_key"`
		Severity  string `json:"severity"`
		Stage     string `json:"stage"`
	}

	if err := json.Unmarshal(payload, &envelope); err != nil {
		return ""
	}

	tenantID := strings.TrimSpace(envelope.TenantID)
	if tenantID == "" {
		tenantID = "default"
	}
	severity := strings.ToLower(strings.TrimSpace(envelope.Severity))
	stage := strings.ToLower(strings.TrimSpace(envelope.Stage))
	dedupeKey := strings.TrimSpace(envelope.DedupeKey)
	if dedupeKey != "" {
		return strings.Join([]string{tenantID, dedupeKey, severity}, "|")
	}

	budgetID := strings.TrimSpace(envelope.BudgetID)
	if budgetID != "" {
		return strings.Join([]string{tenantID, budgetID, severity, stage}, "|")
	}

	return ""
}

func filterChannels(enabled []integrationChannel, targets ...integrationChannel) []integrationChannel {
	if len(enabled) == 0 || len(targets) == 0 {
		return nil
	}

	targetSet := make(map[integrationChannel]struct{}, len(targets))
	for _, target := range targets {
		targetSet[target] = struct{}{}
	}

	routed := make([]integrationChannel, 0, len(targets))
	for _, channel := range enabled {
		if _, ok := targetSet[channel]; ok {
			routed = append(routed, channel)
		}
	}
	return routed
}

type weComDingTalkTextPayload struct {
	MsgType string `json:"msgtype"`
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
}

type feishuTextPayload struct {
	MsgType string `json:"msg_type"`
	Content struct {
		Text string `json:"text"`
	} `json:"content"`
}

type weeklyReportTextPayload struct {
	ReportID      string  `json:"report_id"`
	TenantID      string  `json:"tenant_id"`
	WeekStart     string  `json:"week_start"`
	WeekEnd       string  `json:"week_end"`
	Tokens        int64   `json:"tokens"`
	Cost          float64 `json:"cost"`
	PeakDayDate   string  `json:"peak_day_date"`
	PeakDayTokens int64   `json:"peak_day_tokens"`
	PeakDayCost   float64 `json:"peak_day_cost"`
}

type emailWebhookChannelPayload struct {
	EventType  string          `json:"event_type"`
	Subject    string          `json:"subject"`
	From       string          `json:"from,omitempty"`
	To         []string        `json:"to,omitempty"`
	Body       string          `json:"body"`
	Event      json.RawMessage `json:"event,omitempty"`
	EventRaw   string          `json:"event_raw,omitempty"`
	OccurredAt time.Time       `json:"occurred_at"`
}

type stringList []string

func buildChannelPayload(channel integrationChannel, payload []byte, eventType string) ([]byte, error) {
	if channel == channelWebhook {
		return append([]byte(nil), payload...), nil
	}

	text := formatEventTextPayload(payload, eventType)
	switch channel {
	case channelWeCom, channelDingTalk:
		message := weComDingTalkTextPayload{MsgType: "text"}
		message.Text.Content = text
		body, err := json.Marshal(message)
		if err != nil {
			return nil, err
		}
		return body, nil
	case channelFeishu:
		message := feishuTextPayload{MsgType: "text"}
		message.Content.Text = text
		body, err := json.Marshal(message)
		if err != nil {
			return nil, err
		}
		return body, nil
	default:
		return nil, fmt.Errorf("unsupported channel %s", channel)
	}
}

func formatEventTextPayload(payload []byte, eventType string) string {
	normalizedEventType := strings.ToLower(strings.TrimSpace(eventType))
	if normalizedEventType == "" {
		normalizedEventType = "unknown"
	}
	if normalizedEventType == eventTypeWeeklyReport {
		if weeklyText, ok := formatWeeklyReportTextPayload(payload); ok {
			return weeklyText
		}
	}
	return fmt.Sprintf("[agentledger][%s]\n%s", normalizedEventType, compactJSONPayload(payload))
}

func formatWeeklyReportTextPayload(payload []byte) (string, bool) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return "", false
	}

	var report weeklyReportTextPayload
	if err := json.Unmarshal(trimmed, &report); err != nil {
		return "", false
	}

	text := fmt.Sprintf(
		"[agentledger][weekly_report]\nreport_id=%s tenant_id=%s week_start=%s week_end=%s tokens=%d cost=%s peak_day_date=%s peak_day_tokens=%d peak_day_cost=%s",
		normalizeTextField(report.ReportID),
		normalizeTextField(report.TenantID),
		normalizeTextField(report.WeekStart),
		normalizeTextField(report.WeekEnd),
		report.Tokens,
		formatMetricValue(report.Cost),
		normalizeTextField(report.PeakDayDate),
		report.PeakDayTokens,
		formatMetricValue(report.PeakDayCost),
	)
	return text, true
}

func normalizeTextField(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "-"
	}
	return trimmed
}

func formatMetricValue(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func compactJSONPayload(payload []byte) string {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return "{}"
	}

	if !json.Valid(trimmed) {
		return string(trimmed)
	}

	var buf bytes.Buffer
	if err := json.Compact(&buf, trimmed); err != nil {
		return string(trimmed)
	}
	return buf.String()
}

func (d *alertDispatcher) dispatchToChannel(messageKey string, channel integrationChannel, payload []byte, eventType string) error {
	switch channel {
	case channelEmail:
		return d.dispatchEmail(channel, payload, eventType)
	case channelEmailWebhook:
		webhookPayload, err := d.buildEmailWebhookPayload(messageKey, payload, eventType)
		if err != nil {
			return &dispatchError{
				retryable: false,
				message:   fmt.Sprintf("build %s payload failed", channel),
				err:       err,
			}
		}
		return d.dispatchHTTPChannel(channel, webhookPayload)
	default:
		channelPayload, err := buildChannelPayload(channel, payload, eventType)
		if err != nil {
			return &dispatchError{
				retryable: false,
				message:   fmt.Sprintf("build %s payload failed", channel),
				err:       err,
			}
		}
		return d.dispatchHTTPChannel(channel, channelPayload)
	}
}

func (d *alertDispatcher) dispatchHTTPChannel(channel integrationChannel, channelPayload []byte) error {
	endpoint := strings.TrimSpace(d.cfg.ChannelURLs[channel])
	if endpoint == "" {
		return &dispatchError{
			retryable: false,
			message:   fmt.Sprintf("channel %s is not configured", channel),
		}
	}

	baseCtx := d.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	requestTimeout := d.cfg.WebhookTimeout
	if d.cfg.CallbackSignatureTTL > 0 && d.cfg.CallbackSignatureTTL < requestTimeout {
		requestTimeout = d.cfg.CallbackSignatureTTL
	}
	reqCtx, cancel := context.WithTimeout(baseCtx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewReader(channelPayload))
	if err != nil {
		return &dispatchError{
			retryable: false,
			message:   fmt.Sprintf("build %s request failed", channel),
			err:       err,
		}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return &dispatchError{
			retryable: true,
			message:   fmt.Sprintf("send %s request failed", channel),
			err:       err,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	bodySnippet := readBodySnippet(resp.Body, maxErrorBodyLogLength)
	message := fmt.Sprintf("%s returned status %d", channel, resp.StatusCode)
	if bodySnippet != "" {
		message = fmt.Sprintf("%s: %s", message, bodySnippet)
	}

	retryable := false
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		retryable = true
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
		retryable = false
	}

	return &dispatchError{
		retryable:  retryable,
		statusCode: resp.StatusCode,
		message:    message,
	}
}

func (d *alertDispatcher) dispatchEmail(channel integrationChannel, payload []byte, eventType string) error {
	fromAddress, err := parseEmailAddress(d.cfg.EmailFrom)
	if err != nil {
		return &dispatchError{
			retryable: false,
			message:   fmt.Sprintf("invalid email sender for channel %s", channel),
			err:       err,
		}
	}

	recipients, err := resolveEmailRecipients(payload, fromAddress)
	if err != nil {
		return &dispatchError{
			retryable: false,
			message:   fmt.Sprintf("resolve %s recipients failed", channel),
			err:       err,
		}
	}

	subject := buildEmailSubject(payload, eventType)
	body := formatEventTextPayload(payload, eventType)
	message := buildSMTPMessage(fromAddress, recipients, subject, body)
	if err := d.sendSMTPMessage(fromAddress, recipients, message); err != nil {
		return err
	}
	return nil
}

func (d *alertDispatcher) buildEmailWebhookPayload(messageKey string, payload []byte, eventType string) ([]byte, error) {
	fromAddress := ""
	rawFrom := strings.TrimSpace(d.cfg.EmailFrom)
	if rawFrom != "" {
		parsedFrom, err := parseEmailAddress(rawFrom)
		if err != nil {
			return nil, err
		}
		fromAddress = parsedFrom
	}

	recipients, err := resolveEmailRecipients(payload, fromAddress)
	if err != nil {
		return nil, err
	}

	body := formatEventTextPayload(payload, eventType)
	wrapped := emailWebhookChannelPayload{
		EventType:  normalizeEventTypeLabel(eventType),
		Subject:    buildEmailSubject(payload, eventType),
		From:       fromAddress,
		To:         recipients,
		Body:       body,
		OccurredAt: d.resolveEmailWebhookOccurredAt(messageKey, payload),
	}

	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) > 0 {
		if json.Valid(trimmedPayload) {
			wrapped.Event = append([]byte(nil), trimmedPayload...)
		} else {
			wrapped.EventRaw = string(trimmedPayload)
		}
	}

	return json.Marshal(wrapped)
}

func (d *alertDispatcher) resolveEmailWebhookOccurredAt(messageKey string, payload []byte) time.Time {
	if parsed, ok := extractPayloadOccurredAt(payload); ok {
		return parsed.UTC()
	}

	now := time.Now().UTC()
	if messageKey == "" || d.progress == nil {
		return now
	}

	d.progress.mu.Lock()
	defer d.progress.mu.Unlock()

	d.cleanupDispatchProgressLocked(now)

	entry := d.progress.items[messageKey]
	if entry != nil {
		if !entry.occurredAt.IsZero() {
			entry.updatedAt = now
			return entry.occurredAt
		}
		entry.occurredAt = now
		entry.updatedAt = now
		return entry.occurredAt
	}

	d.progress.items[messageKey] = &dispatchProgressEntry{
		successful: make(map[integrationChannel]struct{}, len(d.cfg.Channels)),
		occurredAt: now,
		updatedAt:  now,
	}
	return now
}

func extractPayloadOccurredAt(payload []byte) (time.Time, bool) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return time.Time{}, false
	}

	var envelope struct {
		OccurredAt string `json:"occurred_at"`
		CreatedAt  string `json:"created_at"`
		Timestamp  string `json:"timestamp"`
		Event      struct {
			OccurredAt string `json:"occurred_at"`
			CreatedAt  string `json:"created_at"`
			Timestamp  string `json:"timestamp"`
		} `json:"event"`
	}
	if err := json.Unmarshal(trimmed, &envelope); err != nil {
		return time.Time{}, false
	}

	for _, candidate := range []string{
		envelope.OccurredAt,
		envelope.CreatedAt,
		envelope.Timestamp,
		envelope.Event.OccurredAt,
		envelope.Event.CreatedAt,
		envelope.Event.Timestamp,
	} {
		parsed, ok := parseOccurredAtValue(candidate)
		if ok {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func parseOccurredAtValue(value string) (time.Time, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, trimmed)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func (d *alertDispatcher) sendSMTPMessage(from string, recipients []string, message []byte) error {
	smtpHost := strings.TrimSpace(d.cfg.EmailSMTPHost)
	if smtpHost == "" {
		return &dispatchError{
			retryable: false,
			message:   "INTEGRATION_EMAIL_SMTP_HOST is not configured",
		}
	}
	if d.cfg.EmailSMTPPort <= 0 {
		return &dispatchError{
			retryable: false,
			message:   "INTEGRATION_EMAIL_SMTP_PORT is not configured",
		}
	}

	smtpMode := d.cfg.EmailSMTPTLSMode
	if smtpMode == "" {
		smtpMode = smtpTLSModeSTARTTLS
	}

	timeout := d.cfg.WebhookTimeout
	if timeout <= 0 {
		timeout = defaultWebhookTimeout
	}

	address := net.JoinHostPort(smtpHost, strconv.Itoa(d.cfg.EmailSMTPPort))
	dialer := net.Dialer{Timeout: timeout}
	tlsConfig := &tls.Config{
		ServerName: smtpHost,
		MinVersion: tls.VersionTLS12,
	}

	var conn net.Conn
	var err error

	switch smtpMode {
	case smtpTLSModeTLS:
		conn, err = tls.DialWithDialer(&dialer, "tcp", address, tlsConfig)
	case smtpTLSModeSTARTTLS, smtpTLSModeNone:
		conn, err = dialer.Dial("tcp", address)
	default:
		return &dispatchError{
			retryable: false,
			message:   fmt.Sprintf("unsupported smtp tls mode %q", smtpMode),
		}
	}

	if err != nil {
		return &dispatchError{
			retryable: true,
			message:   "smtp dial failed",
			err:       err,
		}
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	client, err := smtp.NewClient(conn, smtpHost)
	if err != nil {
		return classifySMTPError("create smtp client failed", err)
	}
	defer client.Close()

	if smtpMode == smtpTLSModeSTARTTLS {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return &dispatchError{
				retryable: false,
				message:   "smtp server does not support STARTTLS",
			}
		}
		if err := client.StartTLS(tlsConfig); err != nil {
			return classifySMTPError("smtp STARTTLS failed", err)
		}
	}

	if err := d.smtpAuth(client, smtpHost); err != nil {
		return err
	}

	if err := client.Mail(from); err != nil {
		return classifySMTPError("smtp MAIL FROM failed", err)
	}
	for _, recipient := range recipients {
		if err := client.Rcpt(recipient); err != nil {
			return classifySMTPError(fmt.Sprintf("smtp RCPT TO failed for %s", recipient), err)
		}
	}

	writer, err := client.Data()
	if err != nil {
		return classifySMTPError("smtp DATA command failed", err)
	}
	if _, err := writer.Write(message); err != nil {
		_ = writer.Close()
		return classifySMTPError("write smtp payload failed", err)
	}
	if err := writer.Close(); err != nil {
		return classifySMTPError("finalize smtp payload failed", err)
	}
	if err := client.Quit(); err != nil {
		return classifySMTPError("smtp QUIT failed", err)
	}

	return nil
}

func (d *alertDispatcher) smtpAuth(client *smtp.Client, host string) error {
	username := strings.TrimSpace(d.cfg.EmailSMTPUser)
	password := strings.TrimSpace(d.cfg.EmailSMTPPass)
	if username == "" && password == "" {
		return nil
	}
	if username == "" || password == "" {
		return &dispatchError{
			retryable: false,
			message:   "smtp credentials must include both username and password",
		}
	}
	if ok, _ := client.Extension("AUTH"); !ok {
		return &dispatchError{
			retryable: false,
			message:   "smtp server does not support AUTH",
		}
	}

	auth := smtp.PlainAuth("", username, password, host)
	if err := client.Auth(auth); err != nil {
		return classifySMTPError("smtp AUTH failed", err)
	}
	return nil
}

func classifySMTPError(message string, err error) error {
	retryable := true
	var protoErr *textproto.Error
	if errors.As(err, &protoErr) {
		if protoErr.Code >= 500 && protoErr.Code < 600 {
			retryable = false
		}
		if protoErr.Code >= 400 && protoErr.Code < 500 {
			retryable = true
		}
	}
	return &dispatchError{
		retryable: retryable,
		message:   message,
		err:       err,
	}
}

func buildSMTPMessage(from string, recipients []string, subject string, body string) []byte {
	normalizedBody := strings.ReplaceAll(body, "\r\n", "\n")
	normalizedBody = strings.ReplaceAll(normalizedBody, "\n", "\r\n")
	sanitizedSubject := sanitizeEmailHeader(subject)

	headers := []string{
		fmt.Sprintf("From: %s", from),
		fmt.Sprintf("To: %s", strings.Join(recipients, ", ")),
		fmt.Sprintf("Subject: %s", sanitizedSubject),
		"MIME-Version: 1.0",
		`Content-Type: text/plain; charset="UTF-8"`,
		"Content-Transfer-Encoding: 8bit",
		"",
		normalizedBody,
	}

	return []byte(strings.Join(headers, "\r\n"))
}

func sanitizeEmailHeader(value string) string {
	withoutCR := strings.ReplaceAll(value, "\r", " ")
	return strings.ReplaceAll(withoutCR, "\n", " ")
}

func buildEmailSubject(payload []byte, eventType string) string {
	parts := []string{fmt.Sprintf("[agentledger][%s]", normalizeEventTypeLabel(eventType))}
	if severity := extractSeverity(payload); severity != "" {
		parts = append(parts, fmt.Sprintf("[%s]", severity))
	}
	return strings.Join(parts, "")
}

func normalizeEventTypeLabel(eventType string) string {
	normalized := strings.ToLower(strings.TrimSpace(eventType))
	if normalized == "" {
		return "unknown"
	}
	return normalized
}

func parseEmailAddress(raw string) (string, error) {
	parsed, err := mail.ParseAddress(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if parsed.Address == "" {
		return "", errors.New("email address is empty")
	}
	return strings.ToLower(strings.TrimSpace(parsed.Address)), nil
}

func resolveEmailRecipients(payload []byte, fallback string) ([]string, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		trimmed = []byte("{}")
	}

	var envelope struct {
		To           stringList `json:"to"`
		EmailTo      stringList `json:"email_to"`
		Emails       stringList `json:"emails"`
		Recipients   stringList `json:"recipients"`
		Notification struct {
			To         stringList `json:"to"`
			EmailTo    stringList `json:"email_to"`
			Emails     stringList `json:"emails"`
			Recipients stringList `json:"recipients"`
		} `json:"notification"`
	}
	if err := json.Unmarshal(trimmed, &envelope); err != nil {
		return nil, err
	}

	candidates := make([]string, 0, len(envelope.To)+len(envelope.EmailTo)+len(envelope.Emails)+len(envelope.Recipients))
	candidates = append(candidates, envelope.To...)
	candidates = append(candidates, envelope.EmailTo...)
	candidates = append(candidates, envelope.Emails...)
	candidates = append(candidates, envelope.Recipients...)
	candidates = append(candidates, envelope.Notification.To...)
	candidates = append(candidates, envelope.Notification.EmailTo...)
	candidates = append(candidates, envelope.Notification.Emails...)
	candidates = append(candidates, envelope.Notification.Recipients...)

	if len(candidates) == 0 && strings.TrimSpace(fallback) != "" {
		candidates = append(candidates, fallback)
	}

	recipients := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		addr, err := parseEmailAddress(candidate)
		if err != nil {
			return nil, fmt.Errorf("invalid recipient %q: %w", candidate, err)
		}
		if _, exists := seen[addr]; exists {
			continue
		}
		seen[addr] = struct{}{}
		recipients = append(recipients, addr)
	}

	if len(recipients) == 0 {
		return nil, errors.New("no email recipients resolved")
	}
	return recipients, nil
}

func (s *stringList) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("null")) || len(trimmed) == 0 {
		*s = nil
		return nil
	}

	if len(trimmed) > 0 && trimmed[0] == '"' {
		var raw string
		if err := json.Unmarshal(trimmed, &raw); err != nil {
			return err
		}
		parts := strings.Split(raw, ",")
		items := make([]string, 0, len(parts))
		for _, part := range parts {
			if value := strings.TrimSpace(part); value != "" {
				items = append(items, value)
			}
		}
		*s = items
		return nil
	}

	var values []string
	if err := json.Unmarshal(trimmed, &values); err != nil {
		return err
	}
	items := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			items = append(items, value)
		}
	}
	*s = items
	return nil
}

func (d *alertDispatcher) callbackEndpoint() (string, error) {
	endpoint := strings.TrimSpace(d.cfg.ControlPlaneCallbackURL)
	if endpoint != "" {
		return endpoint, nil
	}

	baseURL := strings.TrimSpace(d.cfg.ControlPlaneBaseURL)
	if baseURL == "" {
		return "", &dispatchError{
			retryable: false,
			message:   "CONTROL_PLANE_BASE_URL is not configured",
		}
	}

	path := d.cfg.CallbackPath
	if strings.TrimSpace(path) == "" {
		path = defaultCallbackPath
	}
	endpoint, err := buildControlPlaneCallbackURL(baseURL, path)
	if err != nil {
		return "", &dispatchError{
			retryable: false,
			message:   "build control-plane callback endpoint failed",
			err:       err,
		}
	}
	return endpoint, nil
}

func (d *alertDispatcher) forwardCallback(ctx context.Context, payload []byte, source string) error {
	endpoint, err := d.callbackEndpoint()
	if err != nil {
		return err
	}

	baseCtx := ctx
	if baseCtx == nil {
		baseCtx = d.ctx
	}
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	requestTimeout := resolveCallbackRequestTimeout(d.cfg.WebhookTimeout, d.cfg.CallbackSignatureTTL)
	reqCtx, cancel := context.WithTimeout(baseCtx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return &dispatchError{
			retryable: false,
			message:   "build control-plane callback request failed",
			err:       err,
		}
	}
	req.Header.Set("Content-Type", "application/json")
	secret := strings.TrimSpace(d.cfg.CallbackSecret)
	if secret != "" {
		req.Header.Set(callbackSecretHeader, secret)
	}
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	nonce, err := generateCallbackNonce()
	if err != nil {
		return &dispatchError{
			retryable: false,
			message:   "generate callback signature nonce failed",
			err:       err,
		}
	}
	req.Header.Set(callbackTimestampHeader, timestamp)
	req.Header.Set(callbackNonceHeader, nonce)
	req.Header.Set(callbackSignatureHeader, signCallbackPayload(secret, timestamp, nonce, payload))
	if source = strings.TrimSpace(source); source != "" {
		req.Header.Set(callbackSourceHeader, source)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return &dispatchError{
			retryable: true,
			message:   "send control-plane callback request failed",
			err:       err,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	bodySnippet := readBodySnippet(resp.Body, maxErrorBodyLogLength)
	message := fmt.Sprintf("control-plane callback api returned status %d", resp.StatusCode)
	if bodySnippet != "" {
		message = fmt.Sprintf("%s: %s", message, bodySnippet)
	}

	return &dispatchError{
		retryable:  resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests,
		statusCode: resp.StatusCode,
		message:    message,
	}
}

func resolveCallbackRequestTimeout(webhookTimeout time.Duration, signatureTTL time.Duration) time.Duration {
	if signatureTTL > 0 && signatureTTL < webhookTimeout {
		return signatureTTL
	}
	return webhookTimeout
}

func generateCallbackNonce() (string, error) {
	nonce := make([]byte, callbackNonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return hex.EncodeToString(nonce), nil
}

func signCallbackPayload(secret, timestamp, nonce string, payload []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = io.WriteString(h, timestamp)
	_, _ = h.Write([]byte{'\n'})
	_, _ = io.WriteString(h, nonce)
	_, _ = h.Write([]byte{'\n'})
	_, _ = h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

func (d *alertDispatcher) verifyIncomingCallbackSecret(provided string) bool {
	expected := strings.TrimSpace(d.cfg.CallbackSecret)
	if expected == "" {
		return true
	}
	provided = strings.TrimSpace(provided)
	if len(expected) != len(provided) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(provided)) == 1
}

func (d *alertDispatcher) observeCallbackMetrics(outcome string, started time.Time) {
	if d.metrics == nil {
		return
	}
	d.metrics.observe(outcome, labelChannelControl, eventTypeCallback, time.Since(started))
}

func callbackErrorToStatus(err error) int {
	var dispatchErr *dispatchError
	if errors.As(err, &dispatchErr) {
		if dispatchErr.statusCode >= 400 && dispatchErr.statusCode < 500 {
			return http.StatusBadRequest
		}
		if dispatchErr.statusCode >= 500 {
			return http.StatusBadGateway
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout
	}
	return http.StatusBadGateway
}

func waitWithContext(ctx context.Context, delay time.Duration) bool {
	if ctx == nil {
		ctx = context.Background()
	}

	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (d *alertDispatcher) observeSuccessMetrics(eventType string, results []channelDispatchResult) {
	_ = eventType
	_ = results
}

func (d *alertDispatcher) observeFailureMetrics(outcome, eventType string, started time.Time, results []channelDispatchResult, dispatchErr error) {
	if d.metrics == nil {
		return
	}

	recorded := false
	for _, result := range results {
		if result.err == nil {
			continue
		}
		d.metrics.observe(outcome, string(result.channel), eventType, result.duration)
		recorded = true
	}

	if recorded {
		return
	}

	for _, channel := range failedChannelsFromError(dispatchErr) {
		d.metrics.observe(outcome, string(channel), eventType, time.Since(started))
		recorded = true
	}

	if !recorded {
		d.metrics.observe(outcome, labelChannelNone, eventType, time.Since(started))
	}
}

func failedChannelsFromError(err error) []integrationChannel {
	var multiErr *multiDispatchError
	if !errors.As(err, &multiErr) {
		return nil
	}
	return multiErr.channels()
}

func (d *alertDispatcher) publishDLQ(payload []byte, eventType string, dispatchErr error, attempt int, failedAt time.Time) error {
	if d.js == nil {
		return errors.New("jetstream publisher is not configured")
	}

	data, err := buildDLQPayload(payload, eventType, dispatchErr, attempt, failedAt)
	if err != nil {
		return err
	}

	baseCtx := d.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	publishCtx, cancel := context.WithTimeout(baseCtx, d.cfg.DLQPublishTimeout)
	defer cancel()

	_, err = d.js.Publish(publishCtx, d.cfg.DLQSubject, data)
	if err != nil {
		return fmt.Errorf("publish to %s failed: %w", d.cfg.DLQSubject, err)
	}
	return nil
}

func newAlertDedupeStore(ttl time.Duration, maxEntries int) *alertDedupeStore {
	if maxEntries <= 0 {
		maxEntries = defaultAlertDedupeMaxEntries
	}

	return &alertDedupeStore{
		items:      make(map[string]time.Time),
		ttl:        ttl,
		maxEntries: maxEntries,
	}
}

func (s *alertDedupeStore) rememberOrSuppress(key string, now time.Time) bool {
	if s == nil || key == "" || s.ttl <= 0 {
		return false
	}

	if now.IsZero() {
		now = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupLocked(now)
	if seenAt, ok := s.items[key]; ok && now.Sub(seenAt) < s.ttl {
		s.items[key] = now
		return true
	}

	s.items[key] = now
	s.evictOverflowLocked()
	return false
}

func (s *alertDedupeStore) cleanupLocked(now time.Time) {
	if s == nil {
		return
	}

	for key, seenAt := range s.items {
		if now.Sub(seenAt) >= s.ttl {
			delete(s.items, key)
		}
	}
}

func (s *alertDedupeStore) evictOverflowLocked() {
	if s == nil || s.maxEntries <= 0 || len(s.items) <= s.maxEntries {
		return
	}

	type keyedTime struct {
		key string
		at  time.Time
	}

	entries := make([]keyedTime, 0, len(s.items))
	for key, at := range s.items {
		entries = append(entries, keyedTime{key: key, at: at})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].at.Before(entries[j].at)
	})

	removeCount := len(entries) - s.maxEntries
	for index := 0; index < removeCount; index += 1 {
		delete(s.items, entries[index].key)
	}
}

func newDispatchProgressStore() *dispatchProgressStore {
	return &dispatchProgressStore{
		items:      make(map[string]*dispatchProgressEntry),
		ttl:        defaultDispatchProgressTTL,
		maxEntries: defaultDispatchProgressMaxEntrys,
	}
}

func dispatchMessageKey(msg jetstream.Msg, eventType string) string {
	if msg != nil {
		meta, err := msg.Metadata()
		if err == nil && meta != nil && meta.Stream != "" && meta.Sequence.Stream > 0 {
			return fmt.Sprintf("%s/%d", meta.Stream, meta.Sequence.Stream)
		}
	}

	if msg == nil {
		return ""
	}

	return buildDispatchFallbackKey(eventType, msg.Data(), msg.Subject(), msg.Reply(), msg.Headers())
}

func buildDispatchFallbackKey(eventType string, payload []byte, subject, reply string, headers nats.Header) string {
	normalizedEventType := strings.ToLower(strings.TrimSpace(eventType))
	if normalizedEventType == "" {
		normalizedEventType = "unknown"
	}

	hasher := sha256.New()
	hashWriteField(hasher, []byte("v1"))
	hashWriteField(hasher, []byte(normalizedEventType))
	hashWriteField(hasher, []byte(strings.TrimSpace(subject)))
	hashWriteField(hasher, []byte(strings.TrimSpace(reply)))

	headerKeys := make([]string, 0, len(headers))
	for key := range headers {
		headerKeys = append(headerKeys, key)
	}
	sort.Strings(headerKeys)
	hashWriteUint64(hasher, uint64(len(headerKeys)))
	for _, key := range headerKeys {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		hashWriteField(hasher, []byte(normalizedKey))

		values := append([]string(nil), headers[key]...)
		sort.Strings(values)
		hashWriteUint64(hasher, uint64(len(values)))
		for _, value := range values {
			hashWriteField(hasher, []byte(value))
		}
	}

	hashWriteField(hasher, payload)
	return fmt.Sprintf("fallback/%s/%x", normalizedEventType, hasher.Sum(nil))
}

func hashWriteUint64(w io.Writer, v uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	_, _ = w.Write(buf[:])
}

func hashWriteField(w io.Writer, data []byte) {
	hashWriteUint64(w, uint64(len(data)))
	if len(data) == 0 {
		return
	}
	_, _ = w.Write(data)
}

func (d *alertDispatcher) cleanupDispatchProgressLocked(now time.Time) {
	if d.progress == nil {
		return
	}
	result := d.progress.cleanupLocked(now)
	if result.evicted > 0 && d.log != nil {
		d.log.Warn("dispatch progress cleanup evicted entries",
			"evicted", result.evicted,
			"expired", result.expired,
			"remaining", len(d.progress.items),
			"max_entries", d.progress.maxEntries,
		)
	}
}

func (d *alertDispatcher) pendingChannels(messageKey string, channels []integrationChannel) []integrationChannel {
	if len(channels) == 0 || messageKey == "" || d.progress == nil {
		return append([]integrationChannel(nil), channels...)
	}

	d.progress.mu.Lock()
	defer d.progress.mu.Unlock()
	d.cleanupDispatchProgressLocked(time.Now())

	entry := d.progress.items[messageKey]
	if entry == nil || len(entry.successful) == 0 {
		return append([]integrationChannel(nil), channels...)
	}
	if entry.dlqPublished {
		return nil
	}

	pending := make([]integrationChannel, 0, len(channels))
	for _, channel := range channels {
		if _, ok := entry.successful[channel]; ok {
			continue
		}
		pending = append(pending, channel)
	}
	entry.updatedAt = time.Now()
	return pending
}

func (d *alertDispatcher) markChannelSuccess(messageKey string, channel integrationChannel) {
	if messageKey == "" || d.progress == nil {
		return
	}

	d.progress.mu.Lock()
	defer d.progress.mu.Unlock()
	now := time.Now()
	d.cleanupDispatchProgressLocked(now)

	entry := d.progress.items[messageKey]
	if entry == nil {
		entry = &dispatchProgressEntry{
			successful: make(map[integrationChannel]struct{}),
		}
		d.progress.items[messageKey] = entry
	}
	entry.successful[channel] = struct{}{}
	entry.updatedAt = now
}

func (d *alertDispatcher) markDLQPublished(messageKey string) {
	if messageKey == "" || d.progress == nil {
		return
	}

	d.progress.mu.Lock()
	defer d.progress.mu.Unlock()
	now := time.Now()
	d.cleanupDispatchProgressLocked(now)

	entry := d.progress.items[messageKey]
	if entry == nil {
		entry = &dispatchProgressEntry{
			successful: make(map[integrationChannel]struct{}),
		}
		d.progress.items[messageKey] = entry
	}
	entry.dlqPublished = true
	entry.updatedAt = now
}

func (d *alertDispatcher) isDLQPublished(messageKey string) bool {
	if messageKey == "" || d.progress == nil {
		return false
	}

	d.progress.mu.Lock()
	defer d.progress.mu.Unlock()
	d.cleanupDispatchProgressLocked(time.Now())

	entry := d.progress.items[messageKey]
	if entry == nil {
		return false
	}
	entry.updatedAt = time.Now()
	return entry.dlqPublished
}

func (d *alertDispatcher) clearDispatchProgress(messageKey string) {
	if messageKey == "" || d.progress == nil {
		return
	}

	d.progress.mu.Lock()
	defer d.progress.mu.Unlock()
	delete(d.progress.items, messageKey)
}

func (s *dispatchProgressStore) cleanupLocked(now time.Time) dispatchProgressCleanupResult {
	result := dispatchProgressCleanupResult{}
	if s == nil {
		return result
	}

	if s.ttl > 0 {
		expireAt := now.Add(-s.ttl)
		for key, entry := range s.items {
			if entry == nil || entry.updatedAt.Before(expireAt) {
				delete(s.items, key)
				result.expired++
			}
		}
	}

	if s.maxEntries <= 0 || len(s.items) <= s.maxEntries {
		return result
	}

	type itemRef struct {
		key       string
		updatedAt time.Time
	}

	list := make([]itemRef, 0, len(s.items))
	for key, entry := range s.items {
		updatedAt := time.Time{}
		if entry != nil {
			updatedAt = entry.updatedAt
		}
		list = append(list, itemRef{key: key, updatedAt: updatedAt})
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].updatedAt.Before(list[j].updatedAt)
	})

	toDelete := len(s.items) - s.maxEntries
	for i := 0; i < toDelete; i++ {
		delete(s.items, list[i].key)
		result.evicted++
	}
	return result
}

func buildDLQPayload(event []byte, eventType string, dispatchErr error, attempt int, failedAt time.Time) ([]byte, error) {
	payload := dlqPayload{
		EventType: strings.TrimSpace(eventType),
		Error:     "unknown dispatch error",
		Attempt:   attempt,
		FailedAt:  failedAt.UTC(),
	}

	if dispatchErr != nil {
		payload.Error = strings.TrimSpace(dispatchErr.Error())
	}
	if payload.Error == "" {
		payload.Error = "unknown dispatch error"
	}
	if payload.Attempt <= 0 {
		payload.Attempt = 1
	}

	if json.Valid(event) {
		payload.Event = json.RawMessage(append([]byte(nil), event...))
	} else {
		payload.EventRaw = string(event)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal dlq payload failed: %w", err)
	}
	return data, nil
}

func isRetryable(err error) bool {
	var rErr retryableError
	return errors.As(err, &rErr) && rErr.Retryable()
}

func shouldRetry(attempt, maxRetries int) bool {
	if maxRetries < 0 {
		return false
	}
	if attempt <= 0 {
		attempt = 1
	}
	return attempt <= maxRetries
}

func backoffDelay(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = defaultRetryBaseDelay
	}
	if max <= 0 {
		max = defaultRetryMaxDelay
	}
	if max < base {
		max = base
	}
	if attempt <= 1 {
		return base
	}

	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= max {
			return max
		}
		if delay > max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

func extractAttempt(msg jetstream.Msg) int {
	meta, err := msg.Metadata()
	if err != nil || meta == nil || meta.NumDelivered == 0 {
		return 1
	}
	if meta.NumDelivered > uint64(maxInt) {
		return maxInt
	}
	return int(meta.NumDelivered)
}

func readBodySnippet(reader io.Reader, limit int64) string {
	if reader == nil || limit <= 0 {
		return ""
	}
	data, err := io.ReadAll(io.LimitReader(reader, limit))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func initJetStream(cfg integrationConfig, log interface {
	Info(string, ...any)
	Warn(string, ...any)
}) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(
		cfg.Core.NATS.URL,
		nats.Name(cfg.Core.ServiceName),
		nats.Timeout(cfg.Core.NATS.ConnectTimeout),
		nats.MaxReconnects(cfg.Core.NATS.MaxReconnects),
		nats.ReconnectWait(cfg.Core.NATS.ReconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Warn("nats disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			log.Info("nats reconnected", "connected_url", conn.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			log.Info("nats connection closed")
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connect nats failed: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create jetstream context failed: %w", err)
	}

	return nc, js, nil
}

func (m jetStreamCallbackStreamManager) StreamInfo(ctx context.Context, stream string) (*jetstream.StreamInfo, error) {
	jsStream, err := m.js.Stream(ctx, stream)
	if err != nil {
		return nil, err
	}

	info, err := jsStream.Info(ctx)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (m jetStreamCallbackStreamManager) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) error {
	_, err := m.js.CreateStream(ctx, cfg)
	return err
}

func (m jetStreamCallbackStreamManager) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) error {
	_, err := m.js.UpdateStream(ctx, cfg)
	return err
}

func ensureCallbackStream(ctx context.Context, manager callbackStreamManager, stream, subject string, log interface {
	Info(string, ...any)
}) error {
	info, err := manager.StreamInfo(ctx, stream)
	if err != nil {
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("get callback stream failed: %w", err)
		}

		createCfg := jetstream.StreamConfig{
			Name:        stream,
			Description: "integration callback events",
			Subjects:    []string{subject},
		}
		if createErr := manager.CreateStream(ctx, createCfg); createErr != nil {
			if !errors.Is(createErr, jetstream.ErrStreamNameAlreadyInUse) {
				return fmt.Errorf("create callback stream failed: %w", createErr)
			}

			info, err = manager.StreamInfo(ctx, stream)
			if err != nil {
				return fmt.Errorf("get callback stream after concurrent create failed: %w", err)
			}
		} else {
			log.Info("jetstream callback stream created", "stream", stream, "subjects", createCfg.Subjects)
			return nil
		}
	}

	if info == nil {
		return errors.New("get callback stream failed: empty stream info")
	}
	if callbackStreamHasSubject(info.Config.Subjects, subject) {
		log.Info("jetstream callback stream ensured", "stream", stream, "subjects", info.Config.Subjects)
		return nil
	}

	updatedCfg := info.Config
	if updatedCfg.Name == "" {
		updatedCfg.Name = stream
	}
	updatedCfg.Subjects = appendCallbackStreamSubject(updatedCfg.Subjects, subject)

	if err := manager.UpdateStream(ctx, updatedCfg); err != nil {
		return fmt.Errorf("update callback stream subjects failed: %w", err)
	}

	log.Info("jetstream callback stream subjects updated", "stream", stream, "subjects", updatedCfg.Subjects)
	return nil
}

func callbackStreamHasSubject(subjects []string, target string) bool {
	for _, subject := range subjects {
		if callbackStreamSubjectMatches(subject, target) {
			return true
		}
	}
	return false
}

func callbackStreamSubjectMatches(filter, subject string) bool {
	if filter == subject {
		return true
	}
	if filter == "" || subject == "" {
		return false
	}
	// target 是我们配置期望绑定的 stream subject，若其自身含通配符，仅接受精确匹配，避免误判覆盖关系。
	if strings.ContainsAny(subject, "*>") {
		return false
	}

	filterTokens := strings.Split(filter, ".")
	subjectTokens := strings.Split(subject, ".")

	filterIndex := 0
	subjectIndex := 0
	for filterIndex < len(filterTokens) && subjectIndex < len(subjectTokens) {
		switch filterTokens[filterIndex] {
		case ">":
			return filterIndex == len(filterTokens)-1
		case "*":
			if subjectTokens[subjectIndex] == "" {
				return false
			}
			filterIndex++
			subjectIndex++
		default:
			if filterTokens[filterIndex] != subjectTokens[subjectIndex] {
				return false
			}
			filterIndex++
			subjectIndex++
		}
	}

	if filterIndex == len(filterTokens) && subjectIndex == len(subjectTokens) {
		return true
	}

	return filterIndex == len(filterTokens)-1 && filterTokens[filterIndex] == ">"
}

func appendCallbackStreamSubject(subjects []string, target string) []string {
	updated := make([]string, 0, len(subjects)+1)
	updated = append(updated, subjects...)
	if !callbackStreamHasSubject(updated, target) {
		updated = append(updated, target)
	}
	return updated
}

func ensureConsumer(ctx context.Context, js jetstream.JetStream, stream, subject, durable string, ackWait time.Duration) (jetstream.Consumer, error) {
	_, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		FilterSubject: subject,
		MaxDeliver:    -1,
	})
	if err != nil {
		return nil, fmt.Errorf("create or update consumer failed: %w", err)
	}

	consumer, err := js.Consumer(ctx, stream, durable)
	if err != nil {
		return nil, fmt.Errorf("get consumer failed: %w", err)
	}
	return consumer, nil
}
