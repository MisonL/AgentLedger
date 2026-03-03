package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/agentledger/agentledger/services/internal/shared/ingest"
)

func TestExtractBearerToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    string
		wantErr error
	}{
		{
			name:  "success",
			value: "Bearer token-123",
			want:  "token-123",
		},
		{
			name:    "missing_header",
			value:   "   ",
			wantErr: errAuthHeaderMissing,
		},
		{
			name:    "not_bearer",
			value:   "Basic token-123",
			wantErr: errAuthHeaderInvalid,
		},
		{
			name:    "empty_token",
			value:   "Bearer   ",
			wantErr: errAuthHeaderInvalid,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := extractBearerToken(tt.value)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("extractBearerToken() error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("extractBearerToken() token = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestApplyAuthClaimsToBatch(t *testing.T) {
	t.Parallel()

	batch := &ingest.IngestBatch{
		Agent: ingest.AgentInfo{
			AgentID:     "agent-1",
			TenantID:    "client-tenant",
			WorkspaceID: "client-workspace",
		},
		Metadata: map[string]string{
			"keep": "batch-value",
		},
		Events: []ingest.RawEvent{
			{},
			{
				Metadata: map[string]string{
					"keep_event":   "event-value",
					"auth.subject": "client-subject",
				},
			},
		},
	}

	claims := authClaims{
		Subject:     "sub-1",
		Issuer:      "https://issuer.example.com",
		Audience:    []string{"aud-1", "aud-2"},
		Scope:       "ingest:write",
		TenantID:    "tenant-1",
		WorkspaceID: "workspace-1",
	}

	applyAuthClaimsToBatch(batch, claims)

	if batch.Agent.TenantID != "tenant-1" {
		t.Fatalf("batch.Agent.TenantID = %q, want %q", batch.Agent.TenantID, "tenant-1")
	}
	if batch.Agent.WorkspaceID != "workspace-1" {
		t.Fatalf("batch.Agent.WorkspaceID = %q, want %q", batch.Agent.WorkspaceID, "workspace-1")
	}

	wantAuthMetadata := map[string]string{
		"auth.subject":  "sub-1",
		"auth.issuer":   "https://issuer.example.com",
		"auth.audience": "aud-1,aud-2",
		"auth.scope":    "ingest:write",
	}

	for key, wantValue := range wantAuthMetadata {
		if gotValue := batch.Metadata[key]; gotValue != wantValue {
			t.Fatalf("batch.Metadata[%q] = %q, want %q", key, gotValue, wantValue)
		}
	}
	if batch.Metadata["keep"] != "batch-value" {
		t.Fatalf("batch.Metadata[keep] = %q, want %q", batch.Metadata["keep"], "batch-value")
	}

	for i := range batch.Events {
		if batch.Events[i].Metadata == nil {
			t.Fatalf("batch.Events[%d].Metadata should not be nil", i)
		}
		for key, wantValue := range wantAuthMetadata {
			if gotValue := batch.Events[i].Metadata[key]; gotValue != wantValue {
				t.Fatalf("batch.Events[%d].Metadata[%q] = %q, want %q", i, key, gotValue, wantValue)
			}
		}
	}

	if batch.Events[1].Metadata["keep_event"] != "event-value" {
		t.Fatalf("batch.Events[1].Metadata[keep_event] = %q, want %q", batch.Events[1].Metadata["keep_event"], "event-value")
	}
}

func TestExtractAuthClaims_AudienceAndAliases(t *testing.T) {
	t.Parallel()

	t.Run("audience_string_and_tid_wid_alias", func(t *testing.T) {
		t.Parallel()

		got := extractAuthClaims(map[string]any{
			"sub": "subject-1",
			"iss": "issuer-1",
			"aud": "api://ledger",
			"tid": "tenant-100",
			"wid": "workspace-100",
		})

		if !reflect.DeepEqual(got.Audience, []string{"api://ledger"}) {
			t.Fatalf("Audience = %#v, want %#v", got.Audience, []string{"api://ledger"})
		}
		if got.TenantID != "tenant-100" {
			t.Fatalf("TenantID = %q, want %q", got.TenantID, "tenant-100")
		}
		if got.WorkspaceID != "workspace-100" {
			t.Fatalf("WorkspaceID = %q, want %q", got.WorkspaceID, "workspace-100")
		}
	})

	t.Run("audience_array_and_tenant_workspace_alias", func(t *testing.T) {
		t.Parallel()

		got := extractAuthClaims(map[string]any{
			"aud":       []any{"aud-a", "aud-b"},
			"tenant_id": "tenant-200",
			"workspace": "workspace-200",
		})

		if !reflect.DeepEqual(got.Audience, []string{"aud-a", "aud-b"}) {
			t.Fatalf("Audience = %#v, want %#v", got.Audience, []string{"aud-a", "aud-b"})
		}
		if got.TenantID != "tenant-200" {
			t.Fatalf("TenantID = %q, want %q", got.TenantID, "tenant-200")
		}
		if got.WorkspaceID != "workspace-200" {
			t.Fatalf("WorkspaceID = %q, want %q", got.WorkspaceID, "workspace-200")
		}
	})

	t.Run("namespaced_tenant_workspace_claim_keys", func(t *testing.T) {
		t.Parallel()

		got := extractAuthClaims(map[string]any{
			"https://example.com/tenant_id": "tenant-300",
			"urn:example:workspace_id":      "workspace-300",
		})

		if got.TenantID != "tenant-300" {
			t.Fatalf("TenantID = %q, want %q", got.TenantID, "tenant-300")
		}
		if got.WorkspaceID != "workspace-300" {
			t.Fatalf("WorkspaceID = %q, want %q", got.WorkspaceID, "workspace-300")
		}
	})

	t.Run("client_id_should_not_match_tid_or_wid", func(t *testing.T) {
		t.Parallel()

		got := extractAuthClaims(map[string]any{
			"client_id": "client-123",
		})

		if got.TenantID != "" {
			t.Fatalf("TenantID = %q, want empty", got.TenantID)
		}
		if got.WorkspaceID != "" {
			t.Fatalf("WorkspaceID = %q, want empty", got.WorkspaceID)
		}
	})
}

func TestExtractAuthClaims_AliasPriorityDeterministic(t *testing.T) {
	t.Parallel()

	claims := map[string]any{
		"tenant":       "tenant-from-fallback-alias",
		"tenant_id":    "tenant-from-priority-alias",
		"workspace":    "workspace-from-fallback-alias",
		"workspace_id": "workspace-from-priority-alias",
	}

	for i := 0; i < 256; i++ {
		got := extractAuthClaims(claims)
		if got.TenantID != "tenant-from-priority-alias" {
			t.Fatalf("iteration %d: TenantID = %q, want %q", i, got.TenantID, "tenant-from-priority-alias")
		}
		if got.WorkspaceID != "workspace-from-priority-alias" {
			t.Fatalf("iteration %d: WorkspaceID = %q, want %q", i, got.WorkspaceID, "workspace-from-priority-alias")
		}
	}
}

func TestExtractClaimString_NestedMapDeterministic(t *testing.T) {
	t.Parallel()

	claims := map[string]any{
		"z_group": map[string]any{
			"workspace_id": "workspace-z",
		},
		"a_group": map[string]any{
			"workspace_id": "workspace-a",
		},
	}

	for i := 0; i < 256; i++ {
		got := extractClaimString(claims, []string{"workspace_id", "workspace"})
		if got != "workspace-a" {
			t.Fatalf("iteration %d: got = %q, want %q", i, got, "workspace-a")
		}
	}
}

func TestValidateRequiredTenantWorkspaceClaims(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		claims  authClaims
		wantErr error
	}{
		{
			name: "tenant_and_workspace_present",
			claims: authClaims{
				TenantID:    "tenant-1",
				WorkspaceID: "workspace-1",
			},
		},
		{
			name: "missing_tenant",
			claims: authClaims{
				WorkspaceID: "workspace-1",
			},
			wantErr: errRequiredTenantWorkspaceClaims,
		},
		{
			name: "missing_workspace",
			claims: authClaims{
				TenantID: "tenant-1",
			},
			wantErr: errRequiredTenantWorkspaceClaims,
		},
		{
			name:    "missing_both",
			claims:  authClaims{},
			wantErr: errRequiredTenantWorkspaceClaims,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateRequiredTenantWorkspaceClaims(tt.claims)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("validateRequiredTenantWorkspaceClaims() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractAuthClaimsWithResolution(t *testing.T) {
	t.Parallel()

	claims, resolution := extractAuthClaimsWithResolution(map[string]any{
		"subject":                       "subject-1",
		"https://example.com/tenant_id": "tenant-1",
		"profile": map[string]any{
			"workspace_id": "workspace-1",
		},
	})

	if claims.Subject != "subject-1" {
		t.Fatalf("claims.Subject = %q, want %q", claims.Subject, "subject-1")
	}
	if claims.TenantID != "tenant-1" {
		t.Fatalf("claims.TenantID = %q, want %q", claims.TenantID, "tenant-1")
	}
	if claims.WorkspaceID != "workspace-1" {
		t.Fatalf("claims.WorkspaceID = %q, want %q", claims.WorkspaceID, "workspace-1")
	}

	if resolution.Subject.MatchedPath != "subject" {
		t.Fatalf("resolution.Subject.MatchedPath = %q, want %q", resolution.Subject.MatchedPath, "subject")
	}
	if resolution.TenantID.MatchedPath != "https://example.com/tenant_id" {
		t.Fatalf("resolution.TenantID.MatchedPath = %q, want %q", resolution.TenantID.MatchedPath, "https://example.com/tenant_id")
	}
	if resolution.WorkspaceID.MatchedPath != "profile.workspace_id" {
		t.Fatalf("resolution.WorkspaceID.MatchedPath = %q, want %q", resolution.WorkspaceID.MatchedPath, "profile.workspace_id")
	}
}

func TestIngestHandlerServeHTTP_PublishAuthAuditOnAuthFailure(t *testing.T) {
	t.Parallel()

	publisher := &testPublisher{}
	handler := &ingestHandler{
		js:   publisher,
		auth: nil,
		log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{"batch_id":"batch-1","events":[]}`))
	req.RemoteAddr = "127.0.0.1:9090"
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("ServeHTTP() status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}

	messages := publisher.Messages()
	if len(messages) != 1 {
		t.Fatalf("published audit message count = %d, want 1", len(messages))
	}
	if messages[0].Subject != authAuditSubject {
		t.Fatalf("published subject = %q, want %q", messages[0].Subject, authAuditSubject)
	}

	var event authAuditEvent
	if err := json.Unmarshal(messages[0].Payload, &event); err != nil {
		t.Fatalf("unmarshal auth audit event failed: %v", err)
	}
	if event.Protocol != protocolHTTP {
		t.Fatalf("event.Protocol = %q, want %q", event.Protocol, protocolHTTP)
	}
	if event.TLSMode != tlsModePlaintext {
		t.Fatalf("event.TLSMode = %q, want %q", event.TLSMode, tlsModePlaintext)
	}
	if event.MTLSVerified {
		t.Fatalf("event.MTLSVerified = true, want false")
	}
	if strings.TrimSpace(event.FailureReason) == "" {
		t.Fatalf("event.FailureReason should not be empty")
	}
	if strings.Contains(string(messages[0].Payload), "Bearer ") {
		t.Fatalf("audit payload should not include bearer token content")
	}
}

type publishedMessage struct {
	Subject string
	Payload []byte
}

type testPublisher struct {
	mu       sync.Mutex
	messages []publishedMessage
}

func (p *testPublisher) Publish(_ context.Context, subject string, payload []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.messages = append(p.messages, publishedMessage{
		Subject: subject,
		Payload: append([]byte(nil), payload...),
	})
	return &jetstream.PubAck{
		Stream:   "TEST_AUDIT",
		Sequence: uint64(len(p.messages)),
	}, nil
}

func (p *testPublisher) Messages() []publishedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]publishedMessage, len(p.messages))
	copy(out, p.messages)
	return out
}
