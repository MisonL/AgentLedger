package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/agentledger/agentledger/services/internal/shared/config"
	"github.com/agentledger/agentledger/services/internal/shared/health"
	"github.com/agentledger/agentledger/services/internal/shared/ingest"
	"github.com/agentledger/agentledger/services/internal/shared/logger"
)

const (
	rawSubject         = "agent.events.raw"
	authAuditSubject   = "agent.audit.logs"
	archiveSubject     = "archive.enqueue"
	maxIngestBodyBytes = 8 << 20
)

const (
	protocolHTTP     = "http"
	protocolGRPC     = "grpc"
	tlsModePlaintext = "plaintext"
	tlsModeTLS       = "tls"
	tlsModeMTLS      = "mtls"
)

var requiredStreams = []jetstream.StreamConfig{
	{
		Name:        "AGENT_EVENTS_RAW",
		Description: "raw ingestion events",
		Subjects:    []string{rawSubject},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      7 * 24 * time.Hour,
	},
	{
		Name:        "AGENT_EVENTS_NORMALIZED",
		Description: "normalized ingestion events",
		Subjects:    []string{"agent.events.normalized"},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      7 * 24 * time.Hour,
	},
	{
		Name:        "AGENT_AUDIT_LOGS",
		Description: "audit trail logs",
		Subjects:    []string{authAuditSubject},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      30 * 24 * time.Hour,
	},
	{
		Name:        "GOVERNANCE_ALERTS",
		Description: "governance alerts",
		Subjects:    []string{"governance.alerts"},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      30 * 24 * time.Hour,
	},
	{
		Name:        "INTEGRATION_DISPATCH",
		Description: "integration dispatch requests",
		Subjects:    []string{"integration.dispatch"},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      14 * 24 * time.Hour,
	},
	{
		Name:        "ARCHIVE_ENQUEUE",
		Description: "archive enqueue requests",
		Subjects:    []string{archiveSubject},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      14 * 24 * time.Hour,
	},
}

func main() {
	cfg, err := config.Load("ingestion-gateway", ":8081")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config failed: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.LogLevel).With("service", cfg.ServiceName)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, js, err := initJetStream(cfg, log)
	if err != nil {
		log.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			log.Warn("nats drain failed", "error", err)
		}
		nc.Close()
	}()

	if err := ensureStreams(ctx, js, log); err != nil {
		log.Error("ensure required streams failed", "error", err)
		os.Exit(1)
	}

	authenticator, err := newJWTAuthenticator(ctx, cfg)
	if err != nil {
		log.Error("jwt authenticator init failed", "error", err)
		os.Exit(1)
	}

	ingestSvc := newIngestHandler(js, authenticator, log)
	oidcSvc := newOIDCHandler(cfg.OIDC, log)
	mux := http.NewServeMux()
	health.Register(mux, cfg.ServiceName)
	mux.Handle("/v1/ingest", ingestSvc)
	mux.Handle(oidcDeviceStartPath, http.HandlerFunc(oidcSvc.handleDeviceStart))
	mux.Handle(oidcDevicePollPath, http.HandlerFunc(oidcSvc.handleDevicePoll))
	httpErrCh := health.StartWithHandler(ctx, cfg.HTTPAddr, log, mux)

	grpcAddr := resolveGRPCAddr()
	grpcErrCh, err := startGRPCServer(ctx, grpcAddr, ingestSvc, log)
	if err != nil {
		log.Error("grpc server init failed", "error", err, "grpc_addr", grpcAddr)
		os.Exit(1)
	}

	log.Info("service started", "http_addr", cfg.HTTPAddr, "grpc_addr", grpcAddr, "nats_url", cfg.NATS.URL)

	for {
		select {
		case <-ctx.Done():
			log.Info("service stopping", "reason", ctx.Err())
			return
		case err, ok := <-httpErrCh:
			if ok && err != nil {
				log.Error("http server failed", "error", err)
				os.Exit(1)
			}
		case err, ok := <-grpcErrCh:
			if ok && err != nil {
				log.Error("grpc server failed", "error", err)
				os.Exit(1)
			}
		}
	}
}

type ingestHandler struct {
	js   jetStreamPublisher
	auth *jwtAuthenticator
	log  *slog.Logger
}

type jetStreamPublisher interface {
	Publish(context.Context, string, []byte, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

type ingestErrorItem struct {
	Index   int    `json:"index"`
	EventID string `json:"event_id,omitempty"`
	Message string `json:"message"`
}

type ingestResponse struct {
	BatchID    string            `json:"batch_id,omitempty"`
	Accepted   int               `json:"accepted"`
	Rejected   int               `json:"rejected"`
	Errors     []ingestErrorItem `json:"errors,omitempty"`
	DurationMS int64             `json:"duration_ms"`
}

type authTransportSecurity struct {
	TLSMode      string
	MTLSVerified bool
}

type authAuditEvent struct {
	TenantID        string               `json:"tenant_id"`
	WorkspaceID     string               `json:"workspace_id"`
	Subject         string               `json:"subject"`
	Protocol        string               `json:"protocol"`
	TLSMode         string               `json:"tls_mode"`
	MTLSVerified    bool                 `json:"mtls_verified"`
	ClaimHitKeys    map[string]string    `json:"claim_hit_keys"`
	FailureReason   string               `json:"failure_reason"`
	ClaimResolution authClaimsResolution `json:"claim_resolution"`
	OccurredAt      string               `json:"occurred_at"`
}

func newIngestHandler(js jetstream.JetStream, auth *jwtAuthenticator, log *slog.Logger) *ingestHandler {
	return &ingestHandler{js: js, auth: auth, log: log}
}

func (h *ingestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if r.URL.Path != "/v1/ingest" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}

	claims, authAudit, err := h.auth.AuthenticateHTTP(r)
	h.publishAuthAuditEvent(r.Context(), r.RemoteAddr, buildAuthAuditEvent(protocolHTTP, transportSecurityFromHTTPRequest(r), claims, authAudit))
	if err != nil {
		h.log.Warn("http ingest auth failed", "error", err, "remote_addr", r.RemoteAddr)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	batch, err := decodeBatchRequest(w, r)
	if err != nil {
		h.log.Warn("decode ingest batch failed", "error", err, "remote_addr", r.RemoteAddr)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	applyAuthClaimsToBatch(&batch, claims)

	resp, statusCode := h.processBatch(r.Context(), batch, r.RemoteAddr)
	writeJSON(w, statusCode, resp)
}

func (h *ingestHandler) processBatch(ctx context.Context, batch ingest.IngestBatch, remoteAddr string) (ingestResponse, int) {
	start := time.Now()

	validationErrors := ingest.ValidateBatch(batch)
	if len(validationErrors) > 0 {
		rejects := make([]ingestErrorItem, 0, len(validationErrors))
		for _, item := range validationErrors {
			rejects = append(rejects, ingestErrorItem{
				Index:   item.Index,
				Message: fmt.Sprintf("%s: %s", item.Field, item.Message),
			})
		}
		return ingestResponse{
			BatchID:    batch.BatchID,
			Accepted:   0,
			Rejected:   len(batch.Events),
			Errors:     rejects,
			DurationMS: time.Since(start).Milliseconds(),
		}, http.StatusUnprocessableEntity
	}

	ingest.NormalizeBatch(&batch, time.Now())
	resp := ingestResponse{BatchID: batch.BatchID}
	for idx, event := range batch.Events {
		envelope := ingest.RawEnvelope{
			EnvelopeID: ingest.NewID("env"),
			BatchID:    batch.BatchID,
			AcceptedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Agent:      batch.Agent,
			Source:     batch.Source,
			Event:      event,
		}

		payload, rawHash, marshalErr := ingest.MarshalEnvelope(envelope)
		if marshalErr != nil {
			resp.Rejected++
			resp.Errors = append(resp.Errors, ingestErrorItem{
				Index:   idx,
				EventID: event.EventID,
				Message: fmt.Sprintf("marshal envelope failed: %v", marshalErr),
			})
			continue
		}

		ack, publishErr := h.js.Publish(ctx, rawSubject, payload)
		if publishErr != nil {
			resp.Rejected++
			resp.Errors = append(resp.Errors, ingestErrorItem{
				Index:   idx,
				EventID: event.EventID,
				Message: fmt.Sprintf("publish to %s failed: %v", rawSubject, publishErr),
			})
			continue
		}

		resp.Accepted++
		h.log.Info("raw event published",
			"batch_id", batch.BatchID,
			"envelope_id", envelope.EnvelopeID,
			"event_id", event.EventID,
			"session_id", event.SessionID,
			"raw_hash", rawHash,
			"stream", ack.Stream,
			"sequence", ack.Sequence,
			"subject", rawSubject,
		)
	}

	resp.DurationMS = time.Since(start).Milliseconds()
	statusCode := http.StatusAccepted
	if resp.Accepted == 0 && resp.Rejected > 0 {
		statusCode = http.StatusBadGateway
	}

	h.log.Info("ingest batch processed",
		"batch_id", batch.BatchID,
		"accepted", resp.Accepted,
		"rejected", resp.Rejected,
		"duration_ms", resp.DurationMS,
		"remote_addr", remoteAddr,
	)
	return resp, statusCode
}

func buildAuthAuditEvent(protocol string, security authTransportSecurity, claims authClaims, auditInfo authAuditInfo) authAuditEvent {
	return authAuditEvent{
		TenantID:        strings.TrimSpace(claims.TenantID),
		WorkspaceID:     strings.TrimSpace(claims.WorkspaceID),
		Subject:         strings.TrimSpace(claims.Subject),
		Protocol:        normalizeProtocol(protocol),
		TLSMode:         normalizeTLSMode(security.TLSMode),
		MTLSVerified:    security.MTLSVerified,
		ClaimHitKeys:    claimHitKeysFromResolution(auditInfo.ClaimResolution),
		FailureReason:   strings.TrimSpace(auditInfo.FailureReason),
		ClaimResolution: auditInfo.ClaimResolution,
		OccurredAt:      time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func (h *ingestHandler) publishAuthAuditEvent(ctx context.Context, remoteAddr string, event authAuditEvent) {
	if h == nil || h.js == nil {
		return
	}
	log := h.log
	if log == nil {
		log = slog.Default()
	}

	payload, err := json.Marshal(event)
	if err != nil {
		log.Warn("marshal auth audit event failed", "error", err)
		return
	}

	baseCtx := context.Background()
	if ctx != nil {
		baseCtx = context.WithoutCancel(ctx)
	}
	publishCtx, cancel := context.WithTimeout(baseCtx, 3*time.Second)
	defer cancel()

	ack, err := h.js.Publish(publishCtx, authAuditSubject, payload)
	if err != nil {
		log.Warn("publish auth audit event failed",
			"error", err,
			"remote_addr", remoteAddr,
			"protocol", event.Protocol,
			"subject", authAuditSubject,
		)
		return
	}

	log.Info("auth audit event published",
		"remote_addr", remoteAddr,
		"protocol", event.Protocol,
		"stream", ack.Stream,
		"sequence", ack.Sequence,
		"subject", authAuditSubject,
	)
}

func claimHitKeysFromResolution(resolution authClaimsResolution) map[string]string {
	claimHitKeys := map[string]string{
		"subject":      strings.TrimSpace(resolution.Subject.MatchedPath),
		"issuer":       strings.TrimSpace(resolution.Issuer.MatchedPath),
		"audience":     strings.TrimSpace(resolution.Audience.MatchedPath),
		"scope":        strings.TrimSpace(resolution.Scope.MatchedPath),
		"tenant_id":    strings.TrimSpace(resolution.TenantID.MatchedPath),
		"workspace_id": strings.TrimSpace(resolution.WorkspaceID.MatchedPath),
	}
	for key, value := range claimHitKeys {
		if value == "" {
			delete(claimHitKeys, key)
		}
	}
	if len(claimHitKeys) == 0 {
		return map[string]string{}
	}
	return claimHitKeys
}

func normalizeProtocol(protocol string) string {
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case protocolHTTP:
		return protocolHTTP
	case protocolGRPC:
		return protocolGRPC
	default:
		return protocolHTTP
	}
}

func normalizeTLSMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case tlsModeTLS:
		return tlsModeTLS
	case tlsModeMTLS:
		return tlsModeMTLS
	default:
		return tlsModePlaintext
	}
}

func transportSecurityFromHTTPRequest(r *http.Request) authTransportSecurity {
	if r == nil || r.TLS == nil {
		return authTransportSecurity{TLSMode: tlsModePlaintext}
	}
	return transportSecurityFromTLSState(r.TLS)
}

func transportSecurityFromTLSState(state *tls.ConnectionState) authTransportSecurity {
	if state == nil {
		return authTransportSecurity{TLSMode: tlsModePlaintext}
	}
	if mtlsVerifiedFromTLSState(*state) {
		return authTransportSecurity{
			TLSMode:      tlsModeMTLS,
			MTLSVerified: true,
		}
	}
	return authTransportSecurity{TLSMode: tlsModeTLS}
}

func mtlsVerifiedFromTLSState(state tls.ConnectionState) bool {
	if len(state.PeerCertificates) == 0 {
		return false
	}
	if len(state.VerifiedChains) == 0 {
		return false
	}
	return true
}

func decodeBatchRequest(w http.ResponseWriter, r *http.Request) (ingest.IngestBatch, error) {
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if contentType != "" && !strings.Contains(strings.ToLower(contentType), "application/json") {
		return ingest.IngestBatch{}, fmt.Errorf("unsupported content type: %s", contentType)
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxIngestBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return ingest.IngestBatch{}, fmt.Errorf("request body too large")
		}
		return ingest.IngestBatch{}, fmt.Errorf("read request body: %w", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return ingest.IngestBatch{}, fmt.Errorf("request body is empty")
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()

	var batch ingest.IngestBatch
	if err := decoder.Decode(&batch); err != nil {
		return ingest.IngestBatch{}, fmt.Errorf("decode json: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return ingest.IngestBatch{}, fmt.Errorf("request body has trailing data")
	}

	return batch, nil
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("write json response failed", "error", err, "status_code", statusCode)
	}
}

func initJetStream(cfg config.Config, log interface {
	Info(string, ...any)
	Warn(string, ...any)
}) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(
		cfg.NATS.URL,
		nats.Name(cfg.ServiceName),
		nats.Timeout(cfg.NATS.ConnectTimeout),
		nats.MaxReconnects(cfg.NATS.MaxReconnects),
		nats.ReconnectWait(cfg.NATS.ReconnectWait),
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
		return nil, nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create jetstream context: %w", err)
	}

	return nc, js, nil
}

func ensureStreams(ctx context.Context, js jetstream.JetStream, log interface {
	Info(string, ...any)
}) error {
	for _, streamCfg := range requiredStreams {
		if err := ensureStream(ctx, js, streamCfg, log); err != nil {
			return fmt.Errorf("ensure stream %s: %w", streamCfg.Name, err)
		}
	}
	return nil
}

func ensureStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig, log interface {
	Info(string, ...any)
}) error {
	_, err := js.Stream(ctx, cfg.Name)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			if _, createErr := js.CreateStream(ctx, cfg); createErr != nil {
				return createErr
			}
			log.Info("jetstream stream created", "stream", cfg.Name, "subjects", cfg.Subjects)
			return nil
		}
		return err
	}

	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		return err
	}

	log.Info("jetstream stream ensured", "stream", cfg.Name, "subjects", cfg.Subjects)
	return nil
}
