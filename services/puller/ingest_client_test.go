package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agentledger/agentledger/services/internal/shared/ingest"
)

func TestPostIngestBatch_WithBearer(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token-123" {
			t.Fatalf("Authorization header = %q, want %q", got, "Bearer token-123")
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0}`))
	}))
	defer server.Close()

	svc := &pullerService{
		httpClient: newHTTPClient(),
		runtime: pullerRuntimeConfig{
			IngestEndpoint: server.URL,
			IngestBearer:   "token-123",
			IngestTimeout:  mustDuration("2s"),
			AgentID:        "puller",
		},
		hostname: "localhost",
	}

	batch := ingest.IngestBatch{
		Agent:  ingest.AgentInfo{AgentID: "puller"},
		Source: ingest.SourceInfo{SourceID: "source-1", Provider: "ssh"},
		Events: []ingest.RawEvent{{SessionID: "s1", EventType: "message"}},
	}
	if err := svc.postIngestBatch(rctx(), batch); err != nil {
		t.Fatalf("postIngestBatch() unexpected error: %v", err)
	}
}

func TestPostIngestBatch_ServerError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`upstream failed`))
	}))
	defer server.Close()

	svc := &pullerService{
		httpClient: newHTTPClient(),
		runtime: pullerRuntimeConfig{
			IngestEndpoint: server.URL,
			IngestTimeout:  mustDuration("2s"),
			AgentID:        "puller",
		},
		hostname: "localhost",
	}

	batch := ingest.IngestBatch{
		Agent:  ingest.AgentInfo{AgentID: "puller"},
		Source: ingest.SourceInfo{SourceID: "source-1", Provider: "ssh"},
		Events: []ingest.RawEvent{{SessionID: "s1", EventType: "message"}},
	}

	err := svc.postIngestBatch(rctx(), batch)
	if err == nil {
		t.Fatalf("postIngestBatch() expected error, got nil")
	}
}

func TestPostIngestBatch_AcceptedMismatch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accepted":0,"rejected":0}`))
	}))
	defer server.Close()

	svc := &pullerService{
		httpClient: newHTTPClient(),
		runtime: pullerRuntimeConfig{
			IngestEndpoint: server.URL,
			IngestTimeout:  mustDuration("2s"),
			AgentID:        "puller",
		},
		hostname: "localhost",
	}

	batch := ingest.IngestBatch{
		Agent:  ingest.AgentInfo{AgentID: "puller"},
		Source: ingest.SourceInfo{SourceID: "source-1", Provider: "ssh"},
		Events: []ingest.RawEvent{{SessionID: "s1", EventType: "message"}},
	}
	err := svc.postIngestBatch(rctx(), batch)
	if err == nil {
		t.Fatalf("postIngestBatch() expected accepted mismatch error, got nil")
	}
}

func TestPostIngestBatch_RejectedNonZero(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":1}`))
	}))
	defer server.Close()

	svc := &pullerService{
		httpClient: newHTTPClient(),
		runtime: pullerRuntimeConfig{
			IngestEndpoint: server.URL,
			IngestTimeout:  mustDuration("2s"),
			AgentID:        "puller",
		},
		hostname: "localhost",
	}

	batch := ingest.IngestBatch{
		Agent:  ingest.AgentInfo{AgentID: "puller"},
		Source: ingest.SourceInfo{SourceID: "source-1", Provider: "ssh"},
		Events: []ingest.RawEvent{{SessionID: "s1", EventType: "message"}},
	}
	err := svc.postIngestBatch(rctx(), batch)
	if err == nil {
		t.Fatalf("postIngestBatch() expected rejected mismatch error, got nil")
	}
}

func TestPostIngestBatch_InvalidJSONResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`not-json`))
	}))
	defer server.Close()

	svc := &pullerService{
		httpClient: newHTTPClient(),
		runtime: pullerRuntimeConfig{
			IngestEndpoint: server.URL,
			IngestTimeout:  mustDuration("2s"),
			AgentID:        "puller",
		},
		hostname: "localhost",
	}

	batch := ingest.IngestBatch{
		Agent:  ingest.AgentInfo{AgentID: "puller"},
		Source: ingest.SourceInfo{SourceID: "source-1", Provider: "ssh"},
		Events: []ingest.RawEvent{{SessionID: "s1", EventType: "message"}},
	}
	err := svc.postIngestBatch(rctx(), batch)
	if err == nil {
		t.Fatalf("postIngestBatch() expected decode error, got nil")
	}
}

func TestPushEvents_BuildsValidBatch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var batch ingest.IngestBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Fatalf("decode request failed: %v", err)
		}
		if batch.Agent.AgentID != "puller" {
			t.Fatalf("agent_id = %q, want puller", batch.Agent.AgentID)
		}
		if batch.Source.SourceID != "source-1" {
			t.Fatalf("source_id = %q, want source-1", batch.Source.SourceID)
		}
		if len(batch.Events) != 1 {
			t.Fatalf("events len = %d, want 1", len(batch.Events))
		}
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0}`))
	}))
	defer server.Close()

	svc := &pullerService{
		httpClient: newHTTPClient(),
		runtime: pullerRuntimeConfig{
			IngestEndpoint: server.URL,
			IngestTimeout:  mustDuration("2s"),
			AgentID:        "puller",
		},
		hostname: "local-test",
	}

	err := svc.pushEvents(rctx(), sourceRecord{ID: "source-1", Type: "ssh"}, syncJob{ID: "job-1"}, []ingest.RawEvent{{
		SessionID: "s-1",
		EventType: "message",
	}})
	if err != nil {
		t.Fatalf("pushEvents() unexpected error: %v", err)
	}
}
