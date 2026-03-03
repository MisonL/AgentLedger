package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/agentledger/agentledger/services/internal/shared/config"
	"github.com/agentledger/agentledger/services/internal/shared/ingest"
)

type flakyPublisher struct {
	failures map[int]error
	calls    int
}

func (p *flakyPublisher) Publish(_ context.Context, _ string, _ []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.calls++
	if p.failures != nil {
		if err, ok := p.failures[p.calls]; ok {
			return nil, err
		}
	}
	return &jetstream.PubAck{Stream: "TEST", Sequence: uint64(p.calls)}, nil
}

type ensureStreamStub struct {
	jetstream.JetStream
	streamErr error
	createErr error
	updateErr error

	streamCalls int
	createCalls int
	updateCalls int
}

func (s *ensureStreamStub) Stream(ctx context.Context, stream string) (jetstream.Stream, error) {
	s.streamCalls++
	if s.streamErr != nil {
		return nil, s.streamErr
	}
	return nil, nil
}

func (s *ensureStreamStub) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	s.createCalls++
	if s.createErr != nil {
		return nil, s.createErr
	}
	return nil, nil
}

func (s *ensureStreamStub) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	s.updateCalls++
	if s.updateErr != nil {
		return nil, s.updateErr
	}
	return nil, nil
}

type testLogSink struct {
	infoCalls int
	warnCalls int
}

func (l *testLogSink) Info(msg string, args ...any) {
	l.infoCalls++
}

func (l *testLogSink) Warn(msg string, args ...any) {
	l.warnCalls++
}

func TestDecodeBatchRequest(t *testing.T) {
	t.Run("unsupported_content_type", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{"batch_id":"b1"}`))
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()

		_, err := decodeBatchRequest(rec, req)
		if err == nil || !strings.Contains(err.Error(), "unsupported content type") {
			t.Fatalf("decodeBatchRequest() error = %v, want unsupported content type", err)
		}
	})

	t.Run("empty_body", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader("  "))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		_, err := decodeBatchRequest(rec, req)
		if err == nil || !strings.Contains(err.Error(), "request body is empty") {
			t.Fatalf("decodeBatchRequest() error = %v, want empty body", err)
		}
	})

	t.Run("trailing_data", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{"batch_id":"b1"} {"x":1}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		_, err := decodeBatchRequest(rec, req)
		if err == nil || !strings.Contains(err.Error(), "trailing data") {
			t.Fatalf("decodeBatchRequest() error = %v, want trailing data", err)
		}
	})

	t.Run("success", func(t *testing.T) {
		body := `{
			"batch_id":"batch-1",
			"agent":{"agent_id":"agent-1"},
			"source":{"source_id":"source-1","provider":"ssh"},
			"events":[{"session_id":"s1","event_type":"message"}]
		}`
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		rec := httptest.NewRecorder()

		batch, err := decodeBatchRequest(rec, req)
		if err != nil {
			t.Fatalf("decodeBatchRequest() unexpected error: %v", err)
		}
		if batch.BatchID != "batch-1" || batch.Agent.AgentID != "agent-1" || len(batch.Events) != 1 {
			t.Fatalf("decodeBatchRequest() got invalid batch: %+v", batch)
		}
	})
}

func TestIngestHandlerProcessBatch(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("validation_error", func(t *testing.T) {
		h := &ingestHandler{js: &flakyPublisher{}, log: log}
		resp, status := h.processBatch(context.Background(), ingest.IngestBatch{
			BatchID: "batch-invalid",
			Events:  []ingest.RawEvent{{}},
		}, "127.0.0.1:1000")

		if status != http.StatusUnprocessableEntity {
			t.Fatalf("processBatch() status = %d, want 422", status)
		}
		if resp.Rejected != 1 || resp.Accepted != 0 || len(resp.Errors) == 0 {
			t.Fatalf("processBatch() invalid response: %+v", resp)
		}
	})

	t.Run("mixed_marshal_publish_success", func(t *testing.T) {
		pub := &flakyPublisher{
			failures: map[int]error{
				1: errors.New("publish failed once"),
			},
		}
		h := &ingestHandler{js: pub, log: log}
		batch := ingest.IngestBatch{
			BatchID: "batch-mixed",
			Agent: ingest.AgentInfo{
				AgentID: "agent-1",
			},
			Source: ingest.SourceInfo{
				SourceID: "source-1",
				Provider: "ssh",
			},
			Events: []ingest.RawEvent{
				{SessionID: "s1", EventType: "message", Payload: json.RawMessage("{")},
				{SessionID: "s2", EventType: "message"},
				{SessionID: "s3", EventType: "message"},
			},
		}

		resp, status := h.processBatch(context.Background(), batch, "127.0.0.1:1001")
		if status != http.StatusAccepted {
			t.Fatalf("processBatch() status = %d, want 202", status)
		}
		if resp.Accepted != 1 || resp.Rejected != 2 {
			t.Fatalf("processBatch() response = %+v, want accepted=1 rejected=2", resp)
		}
		if len(resp.Errors) != 2 {
			t.Fatalf("processBatch() errors len = %d, want 2", len(resp.Errors))
		}
	})

	t.Run("all_publish_failed_returns_bad_gateway", func(t *testing.T) {
		pub := &flakyPublisher{
			failures: map[int]error{
				1: errors.New("publish failed"),
			},
		}
		h := &ingestHandler{js: pub, log: log}
		batch := ingest.IngestBatch{
			BatchID: "batch-fail",
			Agent: ingest.AgentInfo{
				AgentID: "agent-1",
			},
			Source: ingest.SourceInfo{
				SourceID: "source-1",
				Provider: "ssh",
			},
			Events: []ingest.RawEvent{
				{SessionID: "s1", EventType: "message"},
			},
		}
		resp, status := h.processBatch(context.Background(), batch, "127.0.0.1:1002")
		if status != http.StatusBadGateway {
			t.Fatalf("processBatch() status = %d, want 502", status)
		}
		if resp.Accepted != 0 || resp.Rejected != 1 {
			t.Fatalf("processBatch() response = %+v, want accepted=0 rejected=1", resp)
		}
	})
}

func TestMainHelpers(t *testing.T) {
	hitKeys := claimHitKeysFromResolution(authClaimsResolution{
		Subject:     claimResolution{MatchedPath: "sub"},
		TenantID:    claimResolution{MatchedPath: "tenant_id"},
		WorkspaceID: claimResolution{MatchedPath: "workspace_id"},
		Audience:    claimResolution{MatchedPath: ""},
		Scope:       claimResolution{MatchedPath: ""},
		Issuer:      claimResolution{MatchedPath: ""},
	})
	if got := len(hitKeys); got != 3 {
		t.Fatalf("claimHitKeysFromResolution() len = %d, want 3", got)
	}
	if empty := claimHitKeysFromResolution(authClaimsResolution{}); len(empty) != 0 {
		t.Fatalf("claimHitKeysFromResolution(empty) = %#v, want empty map", empty)
	}

	if got := normalizeProtocol(" grpc "); got != protocolGRPC {
		t.Fatalf("normalizeProtocol(grpc) = %q, want %q", got, protocolGRPC)
	}
	if got := normalizeProtocol("unknown"); got != protocolHTTP {
		t.Fatalf("normalizeProtocol(default) = %q, want %q", got, protocolHTTP)
	}
	if got := normalizeTLSMode(" mtls "); got != tlsModeMTLS {
		t.Fatalf("normalizeTLSMode(mtls) = %q, want %q", got, tlsModeMTLS)
	}
	if got := normalizeTLSMode("none"); got != tlsModePlaintext {
		t.Fatalf("normalizeTLSMode(default) = %q, want %q", got, tlsModePlaintext)
	}

	if got := transportSecurityFromHTTPRequest(nil); got.TLSMode != tlsModePlaintext {
		t.Fatalf("transportSecurityFromHTTPRequest(nil) = %+v", got)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{}`))
	req.TLS = &tls.ConnectionState{}
	if got := transportSecurityFromHTTPRequest(req); got.TLSMode != tlsModeTLS {
		t.Fatalf("transportSecurityFromHTTPRequest(tls) = %+v", got)
	}
}

func TestEnsureStreamAndEnsureStreams(t *testing.T) {
	ctx := context.Background()
	log := &testLogSink{}

	t.Run("create_when_not_found", func(t *testing.T) {
		js := &ensureStreamStub{streamErr: jetstream.ErrStreamNotFound}
		err := ensureStream(ctx, js, jetstream.StreamConfig{Name: "S1", Subjects: []string{"a"}}, log)
		if err != nil {
			t.Fatalf("ensureStream() unexpected error: %v", err)
		}
		if js.createCalls != 1 || js.updateCalls != 0 {
			t.Fatalf("ensureStream() calls create=%d update=%d", js.createCalls, js.updateCalls)
		}
	})

	t.Run("update_existing_stream", func(t *testing.T) {
		js := &ensureStreamStub{}
		err := ensureStream(ctx, js, jetstream.StreamConfig{Name: "S2", Subjects: []string{"b"}}, log)
		if err != nil {
			t.Fatalf("ensureStream() unexpected error: %v", err)
		}
		if js.updateCalls != 1 {
			t.Fatalf("ensureStream() updateCalls = %d, want 1", js.updateCalls)
		}
	})

	t.Run("stream_error_passthrough", func(t *testing.T) {
		js := &ensureStreamStub{streamErr: errors.New("stream query failed")}
		err := ensureStream(ctx, js, jetstream.StreamConfig{Name: "S3", Subjects: []string{"c"}}, log)
		if err == nil || !strings.Contains(err.Error(), "stream query failed") {
			t.Fatalf("ensureStream() error = %v, want passthrough", err)
		}
	})

	t.Run("ensure_streams_wraps_error", func(t *testing.T) {
		js := &ensureStreamStub{streamErr: errors.New("boom")}
		err := ensureStreams(ctx, js, log)
		if err == nil || !strings.Contains(err.Error(), "ensure stream") {
			t.Fatalf("ensureStreams() error = %v, want wrapped ensure stream error", err)
		}
	})

	t.Run("ensure_all_required_streams", func(t *testing.T) {
		js := &ensureStreamStub{streamErr: jetstream.ErrStreamNotFound}
		err := ensureStreams(ctx, js, log)
		if err != nil {
			t.Fatalf("ensureStreams() unexpected error: %v", err)
		}
		if js.createCalls != len(requiredStreams) {
			t.Fatalf("ensureStreams() createCalls = %d, want %d", js.createCalls, len(requiredStreams))
		}
	})
}

func TestInitJetStream_InvalidConfig(t *testing.T) {
	log := &testLogSink{}
	_, _, err := initJetStream(config.Config{
		ServiceName: "ingestion-gateway-test",
		NATS: config.NATSConfig{
			URL:            "://invalid-url",
			ConnectTimeout: 50,
			MaxReconnects:  0,
			ReconnectWait:  50,
		},
	}, log)
	if err == nil || !strings.Contains(err.Error(), "connect nats") {
		t.Fatalf("initJetStream() error = %v, want connect nats error", err)
	}
}
