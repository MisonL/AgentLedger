package main

import (
	"encoding/json"
	"errors"
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
		if batch.Metadata["residency_mode"] != "disabled" {
			t.Fatalf("residency_mode = %q, want disabled", batch.Metadata["residency_mode"])
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

func TestPushEvents_ResidencyPolicyViolation(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0}`))
	}))
	defer server.Close()

	svc := &pullerService{
		httpClient: newHTTPClient(),
		runtime: pullerRuntimeConfig{
			IngestEndpoint:        server.URL,
			IngestTimeout:         mustDuration("2s"),
			AgentID:               "puller",
			ResidencyTargetRegion: "cn-shanghai",
		},
		hostname: "local-test",
	}

	err := svc.pushEvents(
		rctx(),
		sourceRecord{
			ID:   "source-1",
			Type: "ssh",
			Metadata: map[string]any{
				"region": "cn-hangzhou",
			},
		},
		syncJob{ID: "job-1"},
		[]ingest.RawEvent{{
			SessionID: "s-1",
			EventType: "message",
		}},
	)
	if err == nil {
		t.Fatalf("pushEvents() expected residency error, got nil")
	}
	if !errors.Is(err, errResidencyPolicyViolation) {
		t.Fatalf("pushEvents() error = %v, want errResidencyPolicyViolation", err)
	}
	if requestCount != 0 {
		t.Fatalf("requestCount = %d, want 0", requestCount)
	}
}

func TestPushEvents_EnrichesGovernanceMetadata(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var batch ingest.IngestBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Fatalf("decode request failed: %v", err)
		}
		if batch.Metadata["residency_mode"] != "enforce" {
			t.Fatalf("residency_mode = %q, want enforce", batch.Metadata["residency_mode"])
		}
		if batch.Metadata["residency_target_region"] != "cn-shanghai" {
			t.Fatalf("residency_target_region = %q, want cn-shanghai", batch.Metadata["residency_target_region"])
		}
		if batch.Metadata["source_region"] != "cn-shanghai" {
			t.Fatalf("source_region = %q, want cn-shanghai", batch.Metadata["source_region"])
		}
		if batch.Metadata["rule_asset_id"] != "asset-1" {
			t.Fatalf("rule_asset_id = %q, want asset-1", batch.Metadata["rule_asset_id"])
		}
		if len(batch.Events) != 1 {
			t.Fatalf("events len = %d, want 1", len(batch.Events))
		}
		if batch.Events[0].Metadata["rule_id"] != "rule-1" {
			t.Fatalf("event metadata rule_id = %q, want rule-1", batch.Events[0].Metadata["rule_id"])
		}
		if batch.Events[0].Metadata["residency_decision"] == "" {
			t.Fatalf("event metadata residency_decision should not be empty")
		}
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0}`))
	}))
	defer server.Close()

	svc := &pullerService{
		httpClient: newHTTPClient(),
		runtime: pullerRuntimeConfig{
			IngestEndpoint:        server.URL,
			IngestTimeout:         mustDuration("2s"),
			AgentID:               "puller",
			ResidencyTargetRegion: "cn-shanghai",
		},
		hostname: "local-test",
	}

	err := svc.pushEvents(
		rctx(),
		sourceRecord{
			ID:   "source-1",
			Type: "ssh",
			Metadata: map[string]any{
				"region":             "cn-shanghai",
				"rule_asset_id":      "asset-1",
				"rule_asset_version": "2",
				"rule_id":            "rule-1",
			},
		},
		syncJob{ID: "job-1"},
		[]ingest.RawEvent{{
			SessionID: "s-1",
			EventType: "message",
		}},
	)
	if err != nil {
		t.Fatalf("pushEvents() unexpected error: %v", err)
	}
}
