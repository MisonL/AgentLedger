package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestResolveAlertExternalStatusSyncChannel(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		payload []byte
		want    integrationChannel
		wantErr bool
	}{
		{
			name:    "incident",
			payload: []byte(`{"external_type":"incident"}`),
			want:    channelIncident,
		},
		{
			name:    "ticket",
			payload: []byte(`{"externalType":"ticket"}`),
			want:    channelTicket,
		},
		{
			name:    "case alias",
			payload: []byte(`{"external_type":"case"}`),
			want:    channelTicket,
		},
		{
			name:    "unsupported",
			payload: []byte(`{"external_type":"pagerduty"}`),
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveAlertExternalStatusSyncChannel(tc.payload)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveAlertExternalStatusSyncChannel returned error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("channel mismatch: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestHandleAlertExternalStatusSyncMessageAckOnSuccess(t *testing.T) {
	t.Parallel()

	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = append([]byte(nil), body...)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	var callbackBody []byte
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		callbackBody = append([]byte(nil), body...)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer callbackServer.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelIncident})
	cfg.ChannelURLs[channelIncident] = server.URL
	cfg.ControlPlaneCallbackURL = callbackServer.URL

	metrics := newIntegrationMetrics()
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), &fakeDLQPublisher{}, metrics)
	payload := []byte(`{"callback_id":"sync-1","tenant_id":"tenant-a","action":"upsert_external_link","alert_id":"alert-a","external_type":"incident","external_id":"incident-1","external_status":"acknowledged"}`)
	msg := &fakeJetStreamMsg{
		data: payload,
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       defaultAlertExternalStatusSyncStream,
			Sequence: jetstream.SequencePair{
				Stream:   5001,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleAlertExternalStatusSyncMessage(msg)

	if msg.ackCalls != 1 {
		t.Fatalf("ack calls mismatch: got %d want %d", msg.ackCalls, 1)
	}
	if msg.nakWithDelayCalls != 0 {
		t.Fatalf("nak with delay should not be called on success, got %d", msg.nakWithDelayCalls)
	}
	if msg.termCalls != 0 {
		t.Fatalf("term should not be called on success, got %d", msg.termCalls)
	}
	if !bytes.Equal(gotBody, payload) {
		t.Fatalf("forwarded payload mismatch: got %s want %s", string(gotBody), string(payload))
	}
	var callbackRecord map[string]any
	if err := json.Unmarshal(callbackBody, &callbackRecord); err != nil {
		t.Fatalf("unmarshal callback payload failed: %v", err)
	}
	if callbackRecord["action"] != "sync_external_link_result" {
		t.Fatalf("callback action mismatch: got %v", callbackRecord["action"])
	}
	if callbackRecord["sync_result"] != "success" {
		t.Fatalf("callback sync result mismatch: got %v", callbackRecord["sync_result"])
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, `integration_dispatch_events_total{outcome="success",channel="incident",event_type="alert_external_status_sync"} 1`)
}

func TestHandleAlertExternalStatusSyncMessageRetryOnServerError(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "temporary")
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelTicket})
	cfg.ChannelURLs[channelTicket] = server.URL
	cfg.RetryMax = 3
	cfg.RetryBaseDelay = 10 * time.Millisecond
	cfg.RetryMaxDelay = 50 * time.Millisecond

	metrics := newIntegrationMetrics()
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), &fakeDLQPublisher{}, metrics)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"callback_id":"sync-2","tenant_id":"tenant-a","action":"upsert_external_link","alert_id":"alert-a","external_type":"ticket","external_id":"ticket-1","external_status":"resolved"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       defaultAlertExternalStatusSyncStream,
			Sequence: jetstream.SequencePair{
				Stream:   5002,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleAlertExternalStatusSyncMessage(msg)

	if calls.Load() != 1 {
		t.Fatalf("request calls mismatch: got %d want %d", calls.Load(), 1)
	}
	if msg.ackCalls != 0 {
		t.Fatalf("ack should not be called on retry path, got %d", msg.ackCalls)
	}
	if msg.termCalls != 0 {
		t.Fatalf("term should not be called on retry path, got %d", msg.termCalls)
	}
	if msg.nakWithDelayCalls != 1 {
		t.Fatalf("nak with delay calls mismatch: got %d want %d", msg.nakWithDelayCalls, 1)
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, `integration_dispatch_events_total{outcome="retry",channel="ticket",event_type="alert_external_status_sync"} 1`)
}

func TestHandleAlertExternalStatusSyncMessageDLQOnUnsupportedExternalType(t *testing.T) {
	t.Parallel()

	var callbackBody []byte
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		callbackBody = append([]byte(nil), body...)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer callbackServer.Close()

	metrics := newIntegrationMetrics()
	publisher := &fakeDLQPublisher{}
	cfg := testHandleMessageConfig([]integrationChannel{channelTicket, channelIncident})
	cfg.ControlPlaneCallbackURL = callbackServer.URL
	dispatcher := newTestDispatcherForHandleMessage(
		cfg,
		&http.Client{Timeout: time.Second},
		publisher,
		metrics,
	)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"callback_id":"sync-3","tenant_id":"tenant-a","action":"upsert_external_link","alert_id":"alert-a","external_type":"pagerduty","external_id":"pd-1","external_status":"open"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       defaultAlertExternalStatusSyncStream,
			Sequence: jetstream.SequencePair{
				Stream:   5003,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleAlertExternalStatusSyncMessage(msg)

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
	var callbackRecord map[string]any
	if err := json.Unmarshal(callbackBody, &callbackRecord); err != nil {
		t.Fatalf("unmarshal failure callback payload failed: %v", err)
	}
	if callbackRecord["action"] != "sync_external_link_result" {
		t.Fatalf("failure callback action mismatch: got %v", callbackRecord["action"])
	}
	if callbackRecord["sync_result"] != "failed" {
		t.Fatalf("failure callback sync result mismatch: got %v", callbackRecord["sync_result"])
	}
	if callbackRecord["sync_error"] == nil {
		t.Fatalf("failure callback should include sync_error: %+v", callbackRecord)
	}
	if callbackRecord["failure_stage"] != "channel_resolution" {
		t.Fatalf("failure callback should include failure_stage, got %+v", callbackRecord["failure_stage"])
	}
	if callbackRecord["failure_code"] != "unsupported_external_type" {
		t.Fatalf("failure callback should include failure_code, got %+v", callbackRecord["failure_code"])
	}

	rendered := metrics.renderPrometheus()
	assertContains(t, rendered, `integration_dispatch_events_total{outcome="dlq",channel="unknown",event_type="alert_external_status_sync"} 1`)
}

func TestHandleAlertExternalStatusSyncMessageCallbackFailureMovesResultToDLQ(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	metrics := newIntegrationMetrics()
	publisher := &fakeDLQPublisher{}
	cfg := testHandleMessageConfig([]integrationChannel{channelIncident})
	cfg.ChannelURLs[channelIncident] = server.URL
	cfg.ControlPlaneCallbackURL = "http://127.0.0.1:1/api/v1/integrations/callbacks/alerts"
	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), publisher, metrics)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"callback_id":"sync-4","tenant_id":"tenant-a","action":"upsert_external_link","alert_id":"alert-a","external_type":"incident","external_id":"incident-1","external_status":"acknowledged"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       defaultAlertExternalStatusSyncStream,
			Sequence: jetstream.SequencePair{
				Stream:   5004,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleAlertExternalStatusSyncMessage(msg)

	if msg.ackCalls != 1 {
		t.Fatalf("ack calls mismatch: got %d want %d", msg.ackCalls, 1)
	}
	if msg.termCalls != 0 {
		t.Fatalf("term should not be called on downstream success, got %d", msg.termCalls)
	}
	if msg.nakWithDelayCalls != 0 {
		t.Fatalf("nak with delay should not be called on downstream success, got %d", msg.nakWithDelayCalls)
	}
	if publisher.publishCalls != 1 {
		t.Fatalf("dlq publish calls mismatch: got %d want %d", publisher.publishCalls, 1)
	}
	if len(publisher.subjects) != 1 || publisher.subjects[0] != cfg.DLQSubject {
		t.Fatalf("dlq subject mismatch: got %v want [%q]", publisher.subjects, cfg.DLQSubject)
	}

	var payload dlqPayload
	if err := json.Unmarshal(publisher.payloads[0], &payload); err != nil {
		t.Fatalf("unmarshal dlq payload failed: %v", err)
	}
	if payload.EventType != eventTypeCallback {
		t.Fatalf("dlq event type mismatch: got %q want %q", payload.EventType, eventTypeCallback)
	}
	if payload.Subject != defaultCallbackSubject {
		t.Fatalf("dlq subject field mismatch: got %q want %q", payload.Subject, defaultCallbackSubject)
	}
	if payload.CallbackID != "sync-result:sync-4" {
		t.Fatalf("dlq callback id mismatch: got %q", payload.CallbackID)
	}
	if payload.Channel != string(callbackSourceAPI) {
		t.Fatalf("dlq channel mismatch: got %q want %q", payload.Channel, callbackSourceAPI)
	}

	var callbackRecord map[string]any
	if err := json.Unmarshal(payload.Event, &callbackRecord); err != nil {
		t.Fatalf("unmarshal dlq callback event failed: %v", err)
	}
	if callbackRecord["action"] != "sync_external_link_result" {
		t.Fatalf("dlq callback action mismatch: got %v", callbackRecord["action"])
	}
	if callbackRecord["sync_result"] != "success" {
		t.Fatalf("dlq callback sync result mismatch: got %v", callbackRecord["sync_result"])
	}
}
