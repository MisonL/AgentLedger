package main

import (
	"bytes"
	"context"
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
