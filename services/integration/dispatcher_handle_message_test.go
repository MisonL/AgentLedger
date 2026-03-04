package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestHandleMessageAckOnSuccess(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ChannelURLs[channelWebhook] = server.URL

	metrics := newIntegrationMetrics()
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), &fakeDLQPublisher{}, metrics)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"id":"evt-1","severity":"critical"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   1001,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleMessage(msg, eventTypeAlert)

	if msg.ackCalls != 1 {
		t.Fatalf("ack calls mismatch: got %d want %d", msg.ackCalls, 1)
	}
	if msg.nakWithDelayCalls != 0 {
		t.Fatalf("nak with delay should not be called on success, got %d", msg.nakWithDelayCalls)
	}
	if msg.termCalls != 0 {
		t.Fatalf("term should not be called on success, got %d", msg.termCalls)
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"success\",channel=\"webhook\",event_type=\"alert\"} 1")
}

func TestHandleMessageNakOnRetryableFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "temporary")
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ChannelURLs[channelWebhook] = server.URL
	cfg.RetryMax = 3
	cfg.RetryBaseDelay = 150 * time.Millisecond
	cfg.RetryMaxDelay = 2 * time.Second

	metrics := newIntegrationMetrics()
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), &fakeDLQPublisher{}, metrics)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"id":"evt-2","severity":"critical"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   1002,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleMessage(msg, eventTypeAlert)

	if msg.ackCalls != 0 {
		t.Fatalf("ack should not be called on retry path, got %d", msg.ackCalls)
	}
	if msg.termCalls != 0 {
		t.Fatalf("term should not be called on retry path, got %d", msg.termCalls)
	}
	if msg.nakWithDelayCalls != 1 {
		t.Fatalf("nak with delay calls mismatch: got %d want %d", msg.nakWithDelayCalls, 1)
	}
	wantDelay := backoffDelay(1, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
	if msg.lastNakDelay != wantDelay {
		t.Fatalf("retry delay mismatch: got %s want %s", msg.lastNakDelay, wantDelay)
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"retry\",channel=\"webhook\",event_type=\"alert\"} 1")
}

func TestHandleMessageTermOnDLQSuccess(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, "bad payload")
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ChannelURLs[channelWebhook] = server.URL

	metrics := newIntegrationMetrics()
	publisher := &fakeDLQPublisher{}
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), publisher, metrics)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"id":"evt-3","severity":"critical"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   1003,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleMessage(msg, eventTypeAlert)

	if publisher.publishCalls != 1 {
		t.Fatalf("dlq publish calls mismatch: got %d want %d", publisher.publishCalls, 1)
	}
	if msg.termCalls != 1 {
		t.Fatalf("term calls mismatch: got %d want %d", msg.termCalls, 1)
	}
	if msg.ackCalls != 0 {
		t.Fatalf("ack should not be called when terming message, got %d", msg.ackCalls)
	}
	if msg.nakWithDelayCalls != 0 {
		t.Fatalf("nak with delay should not be called when dlq succeeds, got %d", msg.nakWithDelayCalls)
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"dlq\",channel=\"webhook\",event_type=\"alert\"} 1")
}

func TestHandleMessageNakWhenDLQPublishFails(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, "bad payload")
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ChannelURLs[channelWebhook] = server.URL
	cfg.RetryBaseDelay = 200 * time.Millisecond
	cfg.RetryMaxDelay = time.Second

	metrics := newIntegrationMetrics()
	publisher := &fakeDLQPublisher{publishErr: errors.New("nats unavailable")}
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), publisher, metrics)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"id":"evt-4","severity":"critical"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   1004,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleMessage(msg, eventTypeAlert)

	if publisher.publishCalls != 1 {
		t.Fatalf("dlq publish calls mismatch: got %d want %d", publisher.publishCalls, 1)
	}
	if msg.termCalls != 0 {
		t.Fatalf("term should not be called when dlq publish fails, got %d", msg.termCalls)
	}
	if msg.ackCalls != 0 {
		t.Fatalf("ack should not be called when dlq publish fails, got %d", msg.ackCalls)
	}
	if msg.nakWithDelayCalls != 1 {
		t.Fatalf("nak with delay should be called when dlq publish fails, got %d", msg.nakWithDelayCalls)
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"dlq_failed\",channel=\"webhook\",event_type=\"alert\"} 1")
	if strings.Contains(rendered, "integration_dispatch_events_total{outcome=\"dlq\",channel=\"webhook\",event_type=\"alert\"}") {
		t.Fatalf("dlq success metric should not be recorded when dlq publish fails:\n%s", rendered)
	}
}

func TestHandleMessageRetrySkipsChannelsAlreadySuccessful(t *testing.T) {
	t.Parallel()

	var webhookCalls atomic.Int32
	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		webhookCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer webhookServer.Close()

	var wecomCalls atomic.Int32
	wecomServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call := wecomCalls.Add(1)
		if call == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "temporary")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer wecomServer.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook, channelWeCom})
	cfg.ChannelURLs[channelWebhook] = webhookServer.URL
	cfg.ChannelURLs[channelWeCom] = wecomServer.URL
	cfg.RetryMax = 3

	dispatcher := newTestDispatcherForHandleMessage(cfg, webhookServer.Client(), &fakeDLQPublisher{}, nil)
	msgAttempt1 := &fakeJetStreamMsg{
		data: []byte(`{"id":"evt-5","severity":"critical"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   1005,
				Consumer: 1,
			},
		},
	}
	msgAttempt2 := &fakeJetStreamMsg{
		data: []byte(`{"id":"evt-5","severity":"critical"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 2,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   1005,
				Consumer: 2,
			},
		},
	}

	dispatcher.handleMessage(msgAttempt1, eventTypeAlert)
	dispatcher.handleMessage(msgAttempt2, eventTypeAlert)

	if msgAttempt1.nakWithDelayCalls != 1 {
		t.Fatalf("first attempt should be nacked for retry, got %d", msgAttempt1.nakWithDelayCalls)
	}
	if msgAttempt2.ackCalls != 1 {
		t.Fatalf("second attempt should ack after remaining channel succeeds, got %d", msgAttempt2.ackCalls)
	}
	if got := webhookCalls.Load(); got != 1 {
		t.Fatalf("webhook should only be dispatched once across retries, got %d", got)
	}
	if got := wecomCalls.Load(); got != 2 {
		t.Fatalf("wecom should be retried once, got %d", got)
	}
}

func TestHandleMessageSuppressDuplicateAlertsWithinDedupeWindow(t *testing.T) {
	t.Parallel()

	var webhookCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		webhookCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ChannelURLs[channelWebhook] = server.URL
	cfg.AlertDedupeWindow = time.Minute
	cfg.AlertDedupeMaxEntries = 128

	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), &fakeDLQPublisher{}, nil)
	payload := []byte(`{"alert_id":1,"tenant_id":"tenant-a","budget_id":"budget-a","dedupe_key":"tenant-a:budget-a:warning","severity":"warning","stage":"warning"}`)
	msg1 := &fakeJetStreamMsg{
		data: payload,
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   4001,
				Consumer: 1,
			},
		},
	}
	msg2 := &fakeJetStreamMsg{
		data: payload,
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   4002,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleMessage(msg1, eventTypeAlert)
	dispatcher.handleMessage(msg2, eventTypeAlert)

	if msg1.ackCalls != 1 || msg2.ackCalls != 1 {
		t.Fatalf("duplicate alerts should both ack: first=%d second=%d", msg1.ackCalls, msg2.ackCalls)
	}
	if got := webhookCalls.Load(); got != 1 {
		t.Fatalf("duplicate alert within dedupe window should dispatch once, got %d", got)
	}
}

func TestHandleMessageRetryAttemptBypassesDedupeSuppression(t *testing.T) {
	t.Parallel()

	var webhookCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call := webhookCalls.Add(1)
		if call == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "temporary")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ChannelURLs[channelWebhook] = server.URL
	cfg.AlertDedupeWindow = time.Minute
	cfg.AlertDedupeMaxEntries = 128
	cfg.RetryMax = 3

	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), &fakeDLQPublisher{}, nil)
	payload := []byte(`{"alert_id":2,"tenant_id":"tenant-a","budget_id":"budget-a","dedupe_key":"tenant-a:budget-a:critical","severity":"critical","stage":"critical"}`)
	firstAttempt := &fakeJetStreamMsg{
		data: payload,
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   4003,
				Consumer: 1,
			},
		},
	}
	secondAttempt := &fakeJetStreamMsg{
		data: payload,
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 2,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   4003,
				Consumer: 2,
			},
		},
	}

	dispatcher.handleMessage(firstAttempt, eventTypeAlert)
	dispatcher.handleMessage(secondAttempt, eventTypeAlert)

	if firstAttempt.nakWithDelayCalls != 1 {
		t.Fatalf("first attempt should be retried, got nakWithDelay=%d", firstAttempt.nakWithDelayCalls)
	}
	if secondAttempt.ackCalls != 1 {
		t.Fatalf("second attempt should ack after retry, got ack=%d", secondAttempt.ackCalls)
	}
	if got := webhookCalls.Load(); got != 2 {
		t.Fatalf("retry attempt should bypass dedupe suppression, got webhook calls %d", got)
	}
}

func TestHandleWeeklyReportMessagePath(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ChannelURLs[channelWebhook] = server.URL
	cfg.RoutingMode = routingModeSeverity

	metrics := newIntegrationMetrics()
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), &fakeDLQPublisher{}, metrics)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"id":"weekly-1","severity":"warning"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_REPORTS_WEEKLY",
			Sequence: jetstream.SequencePair{
				Stream:   2001,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleWeeklyReportMessage(msg)

	if msg.ackCalls != 1 {
		t.Fatalf("weekly report message should be acked on success, got %d", msg.ackCalls)
	}
	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"success\",channel=\"webhook\",event_type=\"weekly_report\"} 1")
}

func TestHandleCallbackMessageAckOnSuccess(t *testing.T) {
	t.Parallel()

	var receivedSecret string
	var receivedSource string
	var receivedPath string
	var receivedTimestamp string
	var receivedNonce string
	var receivedSignature string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSecret = r.Header.Get(callbackSecretHeader)
		receivedSource = r.Header.Get(callbackSourceHeader)
		receivedPath = r.URL.Path
		receivedTimestamp = r.Header.Get(callbackTimestampHeader)
		receivedNonce = r.Header.Get(callbackNonceHeader)
		receivedSignature = r.Header.Get(callbackSignatureHeader)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ControlPlaneBaseURL = server.URL
	cfg.CallbackPath = defaultCallbackPath
	cfg.ControlPlaneCallbackURL = server.URL + defaultCallbackPath
	cfg.CallbackSecret = "callback-secret"

	metrics := newIntegrationMetrics()
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), &fakeDLQPublisher{}, metrics)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"id":"callback-1"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   3001,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleCallbackMessage(msg)

	if msg.ackCalls != 1 {
		t.Fatalf("callback ack calls mismatch: got %d want %d", msg.ackCalls, 1)
	}
	if msg.nakWithDelayCalls != 0 {
		t.Fatalf("callback should not be retried on success, got %d", msg.nakWithDelayCalls)
	}
	if msg.termCalls != 0 {
		t.Fatalf("callback should not be term on success, got %d", msg.termCalls)
	}
	if receivedSecret != "callback-secret" {
		t.Fatalf("callback secret header mismatch: got %q", receivedSecret)
	}
	if receivedSource != callbackSourceNATS {
		t.Fatalf("callback source header mismatch: got %q want %q", receivedSource, callbackSourceNATS)
	}
	if receivedPath != defaultCallbackPath {
		t.Fatalf("callback path mismatch: got %q want %q", receivedPath, defaultCallbackPath)
	}
	if receivedTimestamp == "" {
		t.Fatal("callback timestamp header should not be empty")
	}
	if _, err := strconv.ParseInt(receivedTimestamp, 10, 64); err != nil {
		t.Fatalf("callback timestamp format invalid: %v", err)
	}
	if len(receivedNonce) != callbackNonceBytes*2 {
		t.Fatalf("callback nonce length mismatch: got %d want %d", len(receivedNonce), callbackNonceBytes*2)
	}
	if _, err := hex.DecodeString(receivedNonce); err != nil {
		t.Fatalf("callback nonce format invalid: %v", err)
	}
	if len(receivedSignature) != sha256.Size*2 {
		t.Fatalf("callback signature length mismatch: got %d want %d", len(receivedSignature), sha256.Size*2)
	}
	if _, err := hex.DecodeString(receivedSignature); err != nil {
		t.Fatalf("callback signature format invalid: %v", err)
	}
	expectedSignature := signCallbackPayloadForTest("callback-secret", receivedTimestamp, receivedNonce, []byte(`{"id":"callback-1"}`))
	if receivedSignature != expectedSignature {
		t.Fatalf("callback signature mismatch: got %q want %q", receivedSignature, expectedSignature)
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"success\",channel=\"control_plane\",event_type=\"callback_alert\"} 1")
}

func TestHandleCallbackMessageRetryOnServerError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "temporary")
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ControlPlaneBaseURL = server.URL
	cfg.CallbackPath = defaultCallbackPath
	cfg.ControlPlaneCallbackURL = server.URL + defaultCallbackPath
	cfg.RetryMax = 3
	cfg.RetryBaseDelay = 10 * time.Millisecond
	cfg.RetryMaxDelay = 50 * time.Millisecond

	metrics := newIntegrationMetrics()
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), &fakeDLQPublisher{}, metrics)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"id":"callback-2"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   3002,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleCallbackMessage(msg)

	if msg.ackCalls != 0 {
		t.Fatalf("callback ack should not be called on retry path, got %d", msg.ackCalls)
	}
	if msg.termCalls != 0 {
		t.Fatalf("callback term should not be called on retry path, got %d", msg.termCalls)
	}
	if msg.nakWithDelayCalls != 1 {
		t.Fatalf("callback nak with delay calls mismatch: got %d want %d", msg.nakWithDelayCalls, 1)
	}
	wantDelay := backoffDelay(1, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
	if msg.lastNakDelay != wantDelay {
		t.Fatalf("callback retry delay mismatch: got %s want %s", msg.lastNakDelay, wantDelay)
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"retry\",channel=\"control_plane\",event_type=\"callback_alert\"} 1")
}

func TestHandleCallbackMessageTooManyRequestsRetry(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, "rate limited")
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ControlPlaneBaseURL = server.URL
	cfg.CallbackPath = defaultCallbackPath
	cfg.ControlPlaneCallbackURL = server.URL + defaultCallbackPath
	cfg.RetryMax = 3

	metrics := newIntegrationMetrics()
	publisher := &fakeDLQPublisher{}
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), publisher, metrics)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"id":"callback-3"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   3003,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleCallbackMessage(msg)

	if msg.ackCalls != 0 {
		t.Fatalf("callback ack should not be called on retry path, got %d", msg.ackCalls)
	}
	if msg.termCalls != 0 {
		t.Fatalf("callback term should not be called on retry path, got %d", msg.termCalls)
	}
	if msg.nakWithDelayCalls != 1 {
		t.Fatalf("callback should retry on 429, got nakWithDelay=%d", msg.nakWithDelayCalls)
	}
	if msg.lastNakDelay <= 0 {
		t.Fatalf("callback retry delay should be positive, got %s", msg.lastNakDelay)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("callback dlq should not be called on retryable 429, got %d", publisher.publishCalls)
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"retry\",channel=\"control_plane\",event_type=\"callback_alert\"} 1")
}

func TestCallbackHTTPHandlerForwardAndRetry(t *testing.T) {
	t.Parallel()

	var callbackCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := callbackCalls.Add(1)
		if call == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "temporary")
			return
		}

		if r.URL.Path != defaultCallbackPath {
			t.Fatalf("callback forward path mismatch: got %q want %q", r.URL.Path, defaultCallbackPath)
		}
		if got := r.Header.Get(callbackSecretHeader); got != "callback-secret" {
			t.Fatalf("callback forward secret mismatch: got %q", got)
		}
		if got := r.Header.Get(callbackSourceHeader); got != callbackSourceAPI {
			t.Fatalf("callback forward source mismatch: got %q want %q", got, callbackSourceAPI)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ControlPlaneBaseURL = server.URL
	cfg.CallbackPath = defaultCallbackPath
	cfg.ControlPlaneCallbackURL = server.URL + defaultCallbackPath
	cfg.CallbackSecret = "callback-secret"
	cfg.RetryMax = 2
	cfg.RetryBaseDelay = time.Millisecond
	cfg.RetryMaxDelay = 2 * time.Millisecond

	metrics := newIntegrationMetrics()
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), &fakeDLQPublisher{}, metrics)

	req := httptest.NewRequest(http.MethodPost, "/v1/callbacks/alerts", strings.NewReader(`{"id":"callback-http-1"}`))
	req.Header.Set(callbackSecretHeader, "callback-secret")
	resp := httptest.NewRecorder()

	dispatcher.callbackHandler().ServeHTTP(resp, req)

	if resp.Code != http.StatusNoContent {
		t.Fatalf("callback http status mismatch: got %d want %d", resp.Code, http.StatusNoContent)
	}
	if got := callbackCalls.Load(); got != 2 {
		t.Fatalf("callback forward retries mismatch: got %d want %d", got, 2)
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"retry\",channel=\"control_plane\",event_type=\"callback_alert\"} 1")
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"success\",channel=\"control_plane\",event_type=\"callback_alert\"} 1")
}

func TestCallbackHTTPHandlerRejectsInvalidSecret(t *testing.T) {
	t.Parallel()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.CallbackSecret = "callback-secret"

	dispatcher := newTestDispatcherForHandleMessage(cfg, &http.Client{Timeout: time.Second}, &fakeDLQPublisher{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/callbacks/alerts", strings.NewReader(`{"id":"callback-http-2"}`))
	resp := httptest.NewRecorder()

	dispatcher.callbackHandler().ServeHTTP(resp, req)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("callback http auth status mismatch: got %d want %d", resp.Code, http.StatusUnauthorized)
	}
}

func TestCallbackHTTPHandlerNoRetryOnClientError(t *testing.T) {
	t.Parallel()

	var callbackCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callbackCalls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, "invalid")
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ControlPlaneBaseURL = server.URL
	cfg.CallbackPath = defaultCallbackPath
	cfg.ControlPlaneCallbackURL = server.URL + defaultCallbackPath
	cfg.CallbackSecret = "callback-secret"
	cfg.RetryMax = 3
	cfg.RetryBaseDelay = time.Millisecond
	cfg.RetryMaxDelay = 2 * time.Millisecond

	metrics := newIntegrationMetrics()
	publisher := &fakeDLQPublisher{}
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), publisher, metrics)

	req := httptest.NewRequest(http.MethodPost, "/v1/callbacks/alerts", strings.NewReader(`{"id":"callback-http-3"}`))
	req.Header.Set(callbackSecretHeader, "callback-secret")
	resp := httptest.NewRecorder()

	dispatcher.callbackHandler().ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("callback http status mismatch: got %d want %d", resp.Code, http.StatusBadRequest)
	}
	if got := callbackCalls.Load(); got != 1 {
		t.Fatalf("callback 4xx should not retry, got %d calls", got)
	}
	if publisher.publishCalls != 1 {
		t.Fatalf("callback http dlq publish calls mismatch: got %d want %d", publisher.publishCalls, 1)
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"dlq\",channel=\"control_plane\",event_type=\"callback_alert\"} 1")
}

func TestResolveCallbackRequestTimeout(t *testing.T) {
	t.Parallel()

	if got := resolveCallbackRequestTimeout(10*time.Second, 0); got != 10*time.Second {
		t.Fatalf("timeout mismatch without signature ttl: got %s want %s", got, 10*time.Second)
	}
	if got := resolveCallbackRequestTimeout(10*time.Second, 2*time.Second); got != 2*time.Second {
		t.Fatalf("timeout mismatch when signature ttl is lower: got %s want %s", got, 2*time.Second)
	}
	if got := resolveCallbackRequestTimeout(2*time.Second, 10*time.Second); got != 2*time.Second {
		t.Fatalf("timeout mismatch when signature ttl is higher: got %s want %s", got, 2*time.Second)
	}
}

func testHandleMessageConfig(channels []integrationChannel) integrationConfig {
	return integrationConfig{
		Channels: channels,
		ChannelURLs: map[integrationChannel]string{
			channelWebhook:  "",
			channelWeCom:    "",
			channelDingTalk: "",
			channelFeishu:   "",
		},
		RoutingMode:             routingModeBroadcast,
		WebhookTimeout:          time.Second,
		RetryMax:                5,
		RetryBaseDelay:          100 * time.Millisecond,
		RetryMaxDelay:           time.Second,
		DLQSubject:              "integration.dispatch.dlq",
		DLQPublishTimeout:       time.Second,
		ControlPlaneBaseURL:     "https://control.example.com",
		CallbackPath:            defaultCallbackPath,
		ControlPlaneCallbackURL: "https://control.example.com/api/v1/integrations/callbacks/alerts",
	}
}

func newTestDispatcherForHandleMessage(cfg integrationConfig, client *http.Client, publisher dlqPublisher, metrics *integrationMetrics) *alertDispatcher {
	if client == nil {
		client = &http.Client{Timeout: time.Second}
	}

	var dedupe *alertDedupeStore
	if cfg.AlertDedupeWindow > 0 {
		dedupe = newAlertDedupeStore(cfg.AlertDedupeWindow, cfg.AlertDedupeMaxEntries)
	}

	return &alertDispatcher{
		ctx:        context.Background(),
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		js:         publisher,
		httpClient: client,
		cfg:        cfg,
		metrics:    metrics,
		progress:   newDispatchProgressStore(),
		dedupe:     dedupe,
	}
}

type fakeDLQPublisher struct {
	publishErr   error
	publishCalls int
	subjects     []string
	payloads     [][]byte
}

func (f *fakeDLQPublisher) Publish(_ context.Context, subject string, payload []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.publishCalls++
	f.subjects = append(f.subjects, subject)
	f.payloads = append(f.payloads, append([]byte(nil), payload...))
	if f.publishErr != nil {
		return nil, f.publishErr
	}
	return &jetstream.PubAck{Stream: "DLQ", Sequence: uint64(f.publishCalls)}, nil
}

type fakeJetStreamMsg struct {
	data        []byte
	metadata    *jetstream.MsgMetadata
	metadataErr error

	ackErr  error
	nakErr  error
	termErr error

	ackCalls          int
	nakCalls          int
	nakWithDelayCalls int
	termCalls         int
	lastNakDelay      time.Duration
}

func (m *fakeJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metadataErr != nil {
		return nil, m.metadataErr
	}
	return m.metadata, nil
}

func (m *fakeJetStreamMsg) Data() []byte {
	return append([]byte(nil), m.data...)
}

func (m *fakeJetStreamMsg) Headers() nats.Header {
	return nats.Header{}
}

func (m *fakeJetStreamMsg) Subject() string {
	return ""
}

func (m *fakeJetStreamMsg) Reply() string {
	return ""
}

func (m *fakeJetStreamMsg) Ack() error {
	m.ackCalls++
	return m.ackErr
}

func (m *fakeJetStreamMsg) DoubleAck(_ context.Context) error {
	return m.Ack()
}

func (m *fakeJetStreamMsg) Nak() error {
	m.nakCalls++
	return m.nakErr
}

func (m *fakeJetStreamMsg) NakWithDelay(delay time.Duration) error {
	m.nakWithDelayCalls++
	m.lastNakDelay = delay
	return m.nakErr
}

func (m *fakeJetStreamMsg) InProgress() error {
	return nil
}

func (m *fakeJetStreamMsg) Term() error {
	m.termCalls++
	return m.termErr
}

func (m *fakeJetStreamMsg) TermWithReason(_ string) error {
	return m.Term()
}

func signCallbackPayloadForTest(secret, timestamp, nonce string, payload []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = io.WriteString(h, timestamp)
	_, _ = h.Write([]byte{'\n'})
	_, _ = io.WriteString(h, nonce)
	_, _ = h.Write([]byte{'\n'})
	_, _ = h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}
