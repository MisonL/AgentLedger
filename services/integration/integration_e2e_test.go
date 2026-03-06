package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	nserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const integrationE2ETimeout = 10 * time.Second

type integrationE2EEnv struct {
	ctx    context.Context
	cancel context.CancelFunc
	server *nserver.Server
	nc     *nats.Conn
	js     jetstream.JetStream
	log    *slog.Logger
}

type integrationE2ERequest struct {
	Path    string
	Headers http.Header
	Body    []byte
}

type integrationE2EProbe struct {
	server   *httptest.Server
	requests chan integrationE2ERequest
}

func newIntegrationE2EEnv(t *testing.T) *integrationE2EEnv {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), integrationE2ETimeout)
	server, err := nserver.NewServer(&nserver.Options{
		ServerName: "agentledger-integration-e2e",
		Host:       "127.0.0.1",
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		NoLog:      true,
		NoSigs:     true,
	})
	if err != nil {
		cancel()
		t.Fatalf("create embedded nats server failed: %v", err)
	}
	go server.Start()
	if !server.ReadyForConnections(integrationE2ETimeout) {
		cancel()
		server.Shutdown()
		t.Fatal("embedded nats server not ready")
	}

	nc, err := nats.Connect(server.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		cancel()
		server.Shutdown()
		t.Fatalf("connect embedded nats failed: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		cancel()
		nc.Close()
		server.Shutdown()
		t.Fatalf("create jetstream client failed: %v", err)
	}

	env := &integrationE2EEnv{
		ctx:    ctx,
		cancel: cancel,
		server: server,
		nc:     nc,
		js:     js,
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	t.Cleanup(func() {
		env.cancel()
		env.nc.Close()
		env.server.Shutdown()
	})
	return env
}

func (e *integrationE2EEnv) ensureStream(
	t *testing.T,
	streamName string,
	subjects ...string,
) {
	t.Helper()

	_, err := e.js.CreateStream(e.ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  subjects,
		Storage:   jetstream.MemoryStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		t.Fatalf("create stream %s failed: %v", streamName, err)
	}
}

func (e *integrationE2EEnv) startConsumer(
	t *testing.T,
	cfg integrationConfig,
	stream string,
	subject string,
	durable string,
	handler func(jetstream.Msg),
) {
	t.Helper()

	consumer, err := ensureConsumer(e.ctx, e.js, stream, subject, durable, cfg.ConsumerAckWait)
	if err != nil {
		t.Fatalf("ensure consumer failed: %v", err)
	}
	consumeCtx, err := consumer.Consume(handler)
	if err != nil {
		t.Fatalf("start consumer failed: %v", err)
	}
	t.Cleanup(func() {
		consumeCtx.Stop()
	})
}

func (e *integrationE2EEnv) publish(
	t *testing.T,
	subject string,
	payload []byte,
) {
	t.Helper()

	if _, err := e.js.Publish(e.ctx, subject, payload); err != nil {
		t.Fatalf("publish %s failed: %v", subject, err)
	}
}

func newIntegrationE2EProbe(t *testing.T, statusCode int) *integrationE2EProbe {
	t.Helper()

	probe := &integrationE2EProbe{
		requests: make(chan integrationE2ERequest, 8),
	}
	probe.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		probe.requests <- integrationE2ERequest{
			Path:    r.URL.Path,
			Headers: r.Header.Clone(),
			Body:    body,
		}
		w.WriteHeader(statusCode)
	}))
	t.Cleanup(func() {
		probe.server.Close()
	})
	return probe
}

func (p *integrationE2EProbe) waitForRequest(t *testing.T) integrationE2ERequest {
	t.Helper()

	select {
	case request := <-p.requests:
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("wait for request timeout")
		return integrationE2ERequest{}
	}
}

func (p *integrationE2EProbe) assertNoRequest(t *testing.T) {
	t.Helper()

	select {
	case request := <-p.requests:
		t.Fatalf("unexpected request received: path=%s body=%s", request.Path, string(request.Body))
	case <-time.After(300 * time.Millisecond):
	}
}

func newIntegrationE2EConfig(channels ...integrationChannel) integrationConfig {
	return integrationConfig{
		Stream:               "GOVERNANCE_ALERTS",
		Subject:              "governance.alerts",
		Durable:              "INTEGRATION_ALERTS_DISPATCHER",
		WeeklyStream:         "GOVERNANCE_REPORTS_WEEKLY",
		WeeklySubject:        "governance.reports.weekly",
		WeeklyDurable:        "INTEGRATION_WEEKLY_REPORT_DISPATCHER",
		CallbackStream:       defaultCallbackStream,
		CallbackSubject:      defaultCallbackSubject,
		CallbackDurable:      defaultCallbackDurable,
		CallbackPath:         defaultCallbackPath,
		DLQSubject:           "integration.dispatch",
		Channels:             channels,
		ChannelURLs:          make(map[integrationChannel]string),
		RoutingMode:          routingModeSeverity,
		WebhookTimeout:       time.Second,
		RetryMax:             2,
		RetryBaseDelay:       50 * time.Millisecond,
		RetryMaxDelay:        150 * time.Millisecond,
		ConsumerAckWait:      2 * time.Second,
		DLQPublishTimeout:    time.Second,
		CallbackSignatureTTL: 5 * time.Second,
	}
}

func TestIntegrationE2EAlertOrchestrationOverrideDispatchesOnlySelectedChannels(t *testing.T) {
	env := newIntegrationE2EEnv(t)
	webhookProbe := newIntegrationE2EProbe(t, http.StatusNoContent)
	wecomProbe := newIntegrationE2EProbe(t, http.StatusNoContent)
	feishuProbe := newIntegrationE2EProbe(t, http.StatusNoContent)

	cfg := newIntegrationE2EConfig(channelWebhook, channelWeCom, channelFeishu)
	cfg.ChannelURLs[channelWebhook] = webhookProbe.server.URL
	cfg.ChannelURLs[channelWeCom] = wecomProbe.server.URL
	cfg.ChannelURLs[channelFeishu] = feishuProbe.server.URL

	env.ensureStream(t, cfg.Stream, cfg.Subject)
	dispatcher := newAlertDispatcher(env.ctx, env.log, env.js, cfg, nil)
	dispatcher.httpClient = &http.Client{Timeout: time.Second}
	env.startConsumer(t, cfg, cfg.Stream, cfg.Subject, cfg.Durable, dispatcher.handleAlertMessage)

	payload := []byte(`{"id":"alert-e2e-override","severity":"warning","orchestration":{"channels":["wecom"],"fallback":false}}`)
	env.publish(t, cfg.Subject, payload)

	request := wecomProbe.waitForRequest(t)
	if !strings.Contains(string(request.Body), `"msgtype":"text"`) {
		t.Fatalf("wecom payload should be text message, got %s", string(request.Body))
	}
	if !strings.Contains(string(request.Body), `[agentledger][alert]`) {
		t.Fatalf("wecom payload should include alert text envelope, got %s", string(request.Body))
	}
	webhookProbe.assertNoRequest(t)
	feishuProbe.assertNoRequest(t)
}

func TestIntegrationE2EAlertFallbackUsesLegacySeverityRouting(t *testing.T) {
	env := newIntegrationE2EEnv(t)
	webhookProbe := newIntegrationE2EProbe(t, http.StatusNoContent)
	wecomProbe := newIntegrationE2EProbe(t, http.StatusNoContent)
	feishuProbe := newIntegrationE2EProbe(t, http.StatusNoContent)

	cfg := newIntegrationE2EConfig(channelWebhook, channelWeCom, channelFeishu)
	cfg.ChannelURLs[channelWebhook] = webhookProbe.server.URL
	cfg.ChannelURLs[channelWeCom] = wecomProbe.server.URL
	cfg.ChannelURLs[channelFeishu] = feishuProbe.server.URL

	env.ensureStream(t, cfg.Stream, cfg.Subject)
	dispatcher := newAlertDispatcher(env.ctx, env.log, env.js, cfg, nil)
	dispatcher.httpClient = &http.Client{Timeout: time.Second}
	env.startConsumer(t, cfg, cfg.Stream, cfg.Subject, cfg.Durable, dispatcher.handleAlertMessage)

	payload := []byte(`{"id":"alert-e2e-fallback","severity":"warning","orchestration":{"channels":[],"fallback":true}}`)
	env.publish(t, cfg.Subject, payload)

	webhookRequest := webhookProbe.waitForRequest(t)
	if !bytes.Equal(webhookRequest.Body, payload) {
		t.Fatalf("webhook should keep raw payload on fallback routing")
	}
	wecomProbe.waitForRequest(t)
	feishuProbe.assertNoRequest(t)
}

func TestIntegrationE2EAlertOrchestrationSuppressedSkipsExternalDispatch(t *testing.T) {
	env := newIntegrationE2EEnv(t)
	webhookProbe := newIntegrationE2EProbe(t, http.StatusNoContent)
	wecomProbe := newIntegrationE2EProbe(t, http.StatusNoContent)

	cfg := newIntegrationE2EConfig(channelWebhook, channelWeCom)
	cfg.ChannelURLs[channelWebhook] = webhookProbe.server.URL
	cfg.ChannelURLs[channelWeCom] = wecomProbe.server.URL

	env.ensureStream(t, cfg.Stream, cfg.Subject)
	dispatcher := newAlertDispatcher(env.ctx, env.log, env.js, cfg, nil)
	dispatcher.httpClient = &http.Client{Timeout: time.Second}
	env.startConsumer(t, cfg, cfg.Stream, cfg.Subject, cfg.Durable, dispatcher.handleAlertMessage)

	payload := []byte(`{"id":"alert-e2e-suppressed","severity":"warning","orchestration":{"channels":[],"suppressed":true}}`)
	env.publish(t, cfg.Subject, payload)

	webhookProbe.assertNoRequest(t)
	wecomProbe.assertNoRequest(t)
}

func TestIntegrationE2EWeeklyOrchestrationDispatchesSelectedChannels(t *testing.T) {
	env := newIntegrationE2EEnv(t)
	webhookProbe := newIntegrationE2EProbe(t, http.StatusNoContent)
	wecomProbe := newIntegrationE2EProbe(t, http.StatusNoContent)

	cfg := newIntegrationE2EConfig(channelWebhook, channelWeCom)
	cfg.ChannelURLs[channelWebhook] = webhookProbe.server.URL
	cfg.ChannelURLs[channelWeCom] = wecomProbe.server.URL

	env.ensureStream(t, cfg.WeeklyStream, cfg.WeeklySubject)
	dispatcher := newAlertDispatcher(env.ctx, env.log, env.js, cfg, nil)
	dispatcher.httpClient = &http.Client{Timeout: time.Second}
	env.startConsumer(t, cfg, cfg.WeeklyStream, cfg.WeeklySubject, cfg.WeeklyDurable, dispatcher.handleWeeklyReportMessage)

	payload := []byte(`{"report_id":"weekly-e2e-1","tenant_id":"tenant-e2e","week_start":"2026-03-02T00:00:00Z","week_end":"2026-03-09T00:00:00Z","tokens":3100,"cost":6.35,"peak_day_date":"2026-03-04","peak_day_tokens":1900,"peak_day_cost":4.245,"orchestration":{"channels":["webhook"],"fallback":false}}`)
	env.publish(t, cfg.WeeklySubject, payload)

	webhookRequest := webhookProbe.waitForRequest(t)
	if !bytes.Equal(webhookRequest.Body, payload) {
		t.Fatalf("weekly webhook should keep raw payload, got %s", string(webhookRequest.Body))
	}
	wecomProbe.assertNoRequest(t)
}

func TestIntegrationE2EAlertOrchestrationDispatchesDingTalkEmailWebhookAndTicket(t *testing.T) {
	env := newIntegrationE2EEnv(t)
	dingTalkProbe := newIntegrationE2EProbe(t, http.StatusNoContent)
	emailWebhookProbe := newIntegrationE2EProbe(t, http.StatusNoContent)
	ticketProbe := newIntegrationE2EProbe(t, http.StatusNoContent)

	cfg := newIntegrationE2EConfig(
		channelDingTalk,
		channelEmailWebhook,
		channelTicket,
	)
	cfg.ChannelURLs[channelDingTalk] = dingTalkProbe.server.URL
	cfg.ChannelURLs[channelEmailWebhook] = emailWebhookProbe.server.URL
	cfg.ChannelURLs[channelTicket] = ticketProbe.server.URL
	cfg.EmailFrom = "alerts@example.com"

	env.ensureStream(t, cfg.Stream, cfg.Subject)
	dispatcher := newAlertDispatcher(env.ctx, env.log, env.js, cfg, nil)
	dispatcher.httpClient = &http.Client{Timeout: time.Second}
	env.startConsumer(
		t,
		cfg,
		cfg.Stream,
		cfg.Subject,
		cfg.Durable,
		dispatcher.handleAlertMessage,
	)

	payload := []byte(`{"id":"alert-e2e-extra-channels","alert_id":"alert-e2e-extra-channels","tenant_id":"tenant-e2e","budget_id":"budget-e2e","source_id":"source-e2e","rule_id":"rule-e2e","severity":"critical","status":"open","occurred_at":"2026-03-05T03:04:05Z","email_to":["ops@example.com","sre@example.com"],"orchestration":{"channels":["dingtalk","email_webhook","ticket"],"fallback":false}}`)
	env.publish(t, cfg.Subject, payload)

	wantText := formatEventTextPayload(payload, eventTypeAlert)

	dingTalkRequest := dingTalkProbe.waitForRequest(t)
	var dingTalkBody struct {
		MsgType string `json:"msgtype"`
		Text    struct {
			Content string `json:"content"`
		} `json:"text"`
	}
	if err := json.Unmarshal(dingTalkRequest.Body, &dingTalkBody); err != nil {
		t.Fatalf("unmarshal dingtalk payload failed: %v", err)
	}
	if dingTalkBody.MsgType != "text" {
		t.Fatalf("dingtalk msgtype mismatch: got %q want %q", dingTalkBody.MsgType, "text")
	}
	if dingTalkBody.Text.Content != wantText {
		t.Fatalf("dingtalk text mismatch:\ngot:  %s\nwant: %s", dingTalkBody.Text.Content, wantText)
	}

	emailWebhookRequest := emailWebhookProbe.waitForRequest(t)
	var emailWebhookBody emailWebhookChannelPayload
	if err := json.Unmarshal(emailWebhookRequest.Body, &emailWebhookBody); err != nil {
		t.Fatalf("unmarshal email webhook payload failed: %v", err)
	}
	if emailWebhookBody.EventType != normalizeEventTypeLabel(eventTypeAlert) {
		t.Fatalf("email webhook event type mismatch: got %q want %q", emailWebhookBody.EventType, normalizeEventTypeLabel(eventTypeAlert))
	}
	if emailWebhookBody.Subject != buildEmailSubject(payload, eventTypeAlert) {
		t.Fatalf("email webhook subject mismatch: got %q want %q", emailWebhookBody.Subject, buildEmailSubject(payload, eventTypeAlert))
	}
	if emailWebhookBody.From != "alerts@example.com" {
		t.Fatalf("email webhook from mismatch: got %q want %q", emailWebhookBody.From, "alerts@example.com")
	}
	if strings.Join(emailWebhookBody.To, ",") != "ops@example.com,sre@example.com" {
		t.Fatalf("email webhook recipients mismatch: got %v want %v", emailWebhookBody.To, []string{"ops@example.com", "sre@example.com"})
	}
	if emailWebhookBody.Body != wantText {
		t.Fatalf("email webhook body mismatch:\ngot:  %s\nwant: %s", emailWebhookBody.Body, wantText)
	}
	if !bytes.Equal(bytes.TrimSpace(emailWebhookBody.Event), payload) {
		t.Fatalf("email webhook raw event mismatch: got %s want %s", string(emailWebhookBody.Event), string(payload))
	}

	ticketRequest := ticketProbe.waitForRequest(t)
	var ticketBody ticketWebhookChannelPayload
	if err := json.Unmarshal(ticketRequest.Body, &ticketBody); err != nil {
		t.Fatalf("unmarshal ticket payload failed: %v", err)
	}
	if ticketBody.EventType != normalizeEventTypeLabel(eventTypeAlert) {
		t.Fatalf("ticket event type mismatch: got %q want %q", ticketBody.EventType, normalizeEventTypeLabel(eventTypeAlert))
	}
	if ticketBody.Title != buildEmailSubject(payload, eventTypeAlert) {
		t.Fatalf("ticket title mismatch: got %q want %q", ticketBody.Title, buildEmailSubject(payload, eventTypeAlert))
	}
	if ticketBody.Summary != wantText {
		t.Fatalf("ticket summary mismatch:\ngot:  %s\nwant: %s", ticketBody.Summary, wantText)
	}
	if ticketBody.Severity != "critical" {
		t.Fatalf("ticket severity mismatch: got %q want %q", ticketBody.Severity, "critical")
	}
	if ticketBody.Status != "open" {
		t.Fatalf("ticket status mismatch: got %q want %q", ticketBody.Status, "open")
	}
	if ticketBody.Context.TenantID != "tenant-e2e" {
		t.Fatalf("ticket tenant mismatch: got %q want %q", ticketBody.Context.TenantID, "tenant-e2e")
	}
	if ticketBody.Context.BudgetID != "budget-e2e" {
		t.Fatalf("ticket budget mismatch: got %q want %q", ticketBody.Context.BudgetID, "budget-e2e")
	}
	if ticketBody.Context.SourceID != "source-e2e" {
		t.Fatalf("ticket source mismatch: got %q want %q", ticketBody.Context.SourceID, "source-e2e")
	}
	if ticketBody.Context.AlertID != "alert-e2e-extra-channels" {
		t.Fatalf("ticket alert mismatch: got %q want %q", ticketBody.Context.AlertID, "alert-e2e-extra-channels")
	}
	if ticketBody.Context.RuleID != "rule-e2e" {
		t.Fatalf("ticket rule mismatch: got %q want %q", ticketBody.Context.RuleID, "rule-e2e")
	}
	if ticketBody.OccurredAt.UTC().Format(time.RFC3339) != "2026-03-05T03:04:05Z" {
		t.Fatalf("ticket occurred_at mismatch: got %s want %s", ticketBody.OccurredAt.UTC().Format(time.RFC3339), "2026-03-05T03:04:05Z")
	}
	if !bytes.Equal(bytes.TrimSpace(ticketBody.Event), payload) {
		t.Fatalf("ticket raw event mismatch: got %s want %s", string(ticketBody.Event), string(payload))
	}
}

func TestIntegrationE2EAlertOrchestrationDispatchesSMTPEmail(t *testing.T) {
	env := newIntegrationE2EEnv(t)
	smtpServer := newFakeSMTPServer(t)
	defer smtpServer.Close()

	cfg := newIntegrationE2EConfig(channelEmail)
	cfg.EmailSMTPHost = smtpServer.Host()
	cfg.EmailSMTPPort = smtpServer.Port()
	cfg.EmailFrom = "alerts@example.com"
	cfg.EmailSMTPTLSMode = smtpTLSModeNone

	env.ensureStream(t, cfg.Stream, cfg.Subject)
	dispatcher := newAlertDispatcher(env.ctx, env.log, env.js, cfg, nil)
	dispatcher.httpClient = &http.Client{Timeout: time.Second}
	env.startConsumer(
		t,
		cfg,
		cfg.Stream,
		cfg.Subject,
		cfg.Durable,
		dispatcher.handleAlertMessage,
	)

	payload := []byte(`{"id":"alert-e2e-email","severity":"warning","email_to":["ops@example.com","sre@example.com"],"orchestration":{"channels":["email"],"fallback":false}}`)
	env.publish(t, cfg.Subject, payload)

	message := smtpServer.WaitForMessage(t)
	if message.mailFrom != "<alerts@example.com>" {
		t.Fatalf("smtp mail from mismatch: got %q want %q", message.mailFrom, "<alerts@example.com>")
	}
	if strings.Join(message.rcptTo, ",") != "<ops@example.com>,<sre@example.com>" {
		t.Fatalf("smtp recipients mismatch: got %v want %v", message.rcptTo, []string{"<ops@example.com>", "<sre@example.com>"})
	}
	if !strings.Contains(message.data, "Subject: [agentledger][alert][warning]") {
		t.Fatalf("smtp subject mismatch: %s", message.data)
	}
	if !strings.Contains(message.data, "[agentledger][alert]") ||
		!strings.Contains(message.data, string(payload)) {
		t.Fatalf("smtp body mismatch: %s", message.data)
	}
}

func TestIntegrationE2ECallbackConsumerForwardsSignedControlPlaneRequest(t *testing.T) {
	env := newIntegrationE2EEnv(t)
	controlPlaneProbe := newIntegrationE2EProbe(t, http.StatusNoContent)

	cfg := newIntegrationE2EConfig(channelWebhook)
	cfg.CallbackSecret = "callback-secret-e2e"
	cfg.ControlPlaneCallbackURL = controlPlaneProbe.server.URL + defaultCallbackPath

	if err := ensureCallbackStream(env.ctx, jetStreamCallbackStreamManager{js: env.js}, cfg.CallbackStream, cfg.CallbackSubject, env.log); err != nil {
		t.Fatalf("ensure callback stream failed: %v", err)
	}

	dispatcher := newAlertDispatcher(env.ctx, env.log, env.js, cfg, nil)
	dispatcher.httpClient = &http.Client{Timeout: time.Second}
	env.startConsumer(t, cfg, cfg.CallbackStream, cfg.CallbackSubject, cfg.CallbackDurable, dispatcher.handleCallbackMessage)

	payload := []byte(`{"callback_id":"callback-e2e-1","tenant_id":"tenant-e2e","action":"ack","alert_id":"alert-1"}`)
	env.publish(t, cfg.CallbackSubject, payload)

	request := controlPlaneProbe.waitForRequest(t)
	if request.Path != defaultCallbackPath {
		t.Fatalf("callback path mismatch: got %q want %q", request.Path, defaultCallbackPath)
	}
	if got := request.Headers.Get(callbackSecretHeader); got != cfg.CallbackSecret {
		t.Fatalf("callback secret header mismatch: got %q want %q", got, cfg.CallbackSecret)
	}
	if got := request.Headers.Get(callbackSourceHeader); got != callbackSourceNATS {
		t.Fatalf("callback source header mismatch: got %q want %q", got, callbackSourceNATS)
	}

	timestamp := request.Headers.Get(callbackTimestampHeader)
	nonce := request.Headers.Get(callbackNonceHeader)
	signature := request.Headers.Get(callbackSignatureHeader)
	if timestamp == "" || nonce == "" || signature == "" {
		t.Fatalf("missing callback signing headers: timestamp=%q nonce=%q signature=%q", timestamp, nonce, signature)
	}
	if want := signCallbackPayload(cfg.CallbackSecret, timestamp, nonce, payload); signature != want {
		t.Fatalf("callback signature mismatch: got %q want %q", signature, want)
	}
	if !bytes.Equal(request.Body, payload) {
		t.Fatalf("callback forwarded payload mismatch: got %s want %s", string(request.Body), string(payload))
	}
}
