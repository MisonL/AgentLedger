package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	ingestionv1 "github.com/agentledger/agentledger/packages/gen/go/ingestion/v1"
	"github.com/agentledger/agentledger/services/internal/shared/grpcx"
	"github.com/agentledger/agentledger/services/internal/shared/ingest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const defaultGRPCAddr = ":9091"

const (
	envGRPCTLSEnabled      = "GRPC_TLS_ENABLED"
	envGRPCTLSCertFile     = "GRPC_TLS_CERT_FILE"
	envGRPCTLSKeyFile      = "GRPC_TLS_KEY_FILE"
	envGRPCMTLSEnabled     = "GRPC_MTLS_ENABLED"
	envGRPCTLSClientCAFile = "GRPC_TLS_CLIENT_CA_FILE"
)

type grpcTLSConfig struct {
	TLSEnabled   bool
	CertFile     string
	KeyFile      string
	MTLSEnabled  bool
	ClientCAFile string
}

func resolveGRPCAddr() string {
	addr := strings.TrimSpace(os.Getenv("GRPC_ADDR"))
	if addr == "" {
		return defaultGRPCAddr
	}
	return addr
}

func startGRPCServer(ctx context.Context, addr string, handler *ingestHandler, log *slog.Logger) (<-chan error, error) {
	serverOpts, transportMode, err := grpcServerOptionsFromEnv()
	if err != nil {
		return nil, fmt.Errorf("configure grpc transport security: %w", err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen grpc addr %s: %w", addr, err)
	}

	server := grpcx.NewServer(serverOpts...)
	ingestionv1.RegisterIngestServiceServer(server, &ingestGRPCServer{handler: handler})

	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		log.Info("grpc server listening", "addr", addr, "transport_security", transportMode)
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			errCh <- serveErr
		}
	}()

	go func() {
		<-ctx.Done()
		done := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			log.Warn("grpc graceful stop timeout, force stopping")
			server.Stop()
		}
	}()

	return errCh, nil
}

func grpcServerOptionsFromEnv() ([]grpc.ServerOption, string, error) {
	tlsCfg, err := resolveGRPCTLSConfigFromEnv(os.Getenv)
	if err != nil {
		return nil, "", err
	}

	opts, err := grpcServerOptionsFromTLSConfig(tlsCfg)
	if err != nil {
		return nil, "", err
	}

	return opts, grpcTransportMode(tlsCfg), nil
}

func resolveGRPCTLSConfigFromEnv(getenv func(string) string) (grpcTLSConfig, error) {
	tlsEnabled, err := parseBoolEnv(getenv, envGRPCTLSEnabled, true)
	if err != nil {
		return grpcTLSConfig{}, err
	}
	mtlsEnabled, err := parseBoolEnv(getenv, envGRPCMTLSEnabled, false)
	if err != nil {
		return grpcTLSConfig{}, err
	}

	cfg := grpcTLSConfig{
		TLSEnabled:   tlsEnabled,
		CertFile:     strings.TrimSpace(getenv(envGRPCTLSCertFile)),
		KeyFile:      strings.TrimSpace(getenv(envGRPCTLSKeyFile)),
		MTLSEnabled:  mtlsEnabled,
		ClientCAFile: strings.TrimSpace(getenv(envGRPCTLSClientCAFile)),
	}

	if cfg.MTLSEnabled && !cfg.TLSEnabled {
		return grpcTLSConfig{}, fmt.Errorf("%s=true requires %s=true", envGRPCMTLSEnabled, envGRPCTLSEnabled)
	}
	if !cfg.TLSEnabled {
		return cfg, nil
	}

	if cfg.CertFile == "" {
		return grpcTLSConfig{}, fmt.Errorf("%s is required when %s=true", envGRPCTLSCertFile, envGRPCTLSEnabled)
	}
	if cfg.KeyFile == "" {
		return grpcTLSConfig{}, fmt.Errorf("%s is required when %s=true", envGRPCTLSKeyFile, envGRPCTLSEnabled)
	}
	if cfg.MTLSEnabled && cfg.ClientCAFile == "" {
		return grpcTLSConfig{}, fmt.Errorf("%s is required when %s=true", envGRPCTLSClientCAFile, envGRPCMTLSEnabled)
	}

	return cfg, nil
}

func grpcServerOptionsFromTLSConfig(cfg grpcTLSConfig) ([]grpc.ServerOption, error) {
	if !cfg.TLSEnabled {
		return nil, nil
	}

	serverCert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert/key (%s, %s): %w", cfg.CertFile, cfg.KeyFile, err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	}

	if cfg.MTLSEnabled {
		clientCAPEM, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read client ca file %s: %w", cfg.ClientCAFile, err)
		}
		clientCAPool := x509.NewCertPool()
		if ok := clientCAPool.AppendCertsFromPEM(clientCAPEM); !ok {
			return nil, fmt.Errorf("parse client ca pem from %s: no cert found", cfg.ClientCAFile)
		}
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		tlsCfg.ClientCAs = clientCAPool
	}

	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsCfg))}, nil
}

func grpcTransportMode(cfg grpcTLSConfig) string {
	if !cfg.TLSEnabled {
		return "plaintext"
	}
	if cfg.MTLSEnabled {
		return "mtls"
	}
	return "tls"
}

func parseBoolEnv(getenv func(string) string, key string, defaultValue bool) (bool, error) {
	if getenv == nil {
		return defaultValue, nil
	}

	raw := strings.TrimSpace(getenv(key))
	if raw == "" {
		return defaultValue, nil
	}

	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean value: %w", key, err)
	}
	return parsed, nil
}

type ingestGRPCServer struct {
	ingestionv1.UnimplementedIngestServiceServer
	handler *ingestHandler
}

func (s *ingestGRPCServer) PushBatch(ctx context.Context, req *ingestionv1.PushBatchRequest) (*ingestionv1.PushBatchResponse, error) {
	remoteAddr := grpcRemoteAddr(ctx)
	security := transportSecurityFromGRPCContext(ctx)

	claims, authAudit, err := s.handler.auth.AuthenticateGRPC(ctx)
	s.handler.publishAuthAuditEvent(ctx, remoteAddr, buildAuthAuditEvent(protocolGRPC, security, claims, authAudit))
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "unauthenticated")
	}

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	batch, err := batchFromProto(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid batch request: %v", err)
	}
	applyAuthClaimsToBatch(&batch, claims)

	resp, _ := s.handler.processBatch(ctx, batch, remoteAddr)
	return responseToProto(resp), nil
}

func grpcRemoteAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return "grpc"
}

func transportSecurityFromGRPCContext(ctx context.Context) authTransportSecurity {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return authTransportSecurity{TLSMode: tlsModePlaintext}
	}

	switch authInfo := p.AuthInfo.(type) {
	case credentials.TLSInfo:
		return transportSecurityFromTLSState(&authInfo.State)
	case *credentials.TLSInfo:
		return transportSecurityFromTLSState(&authInfo.State)
	default:
		return authTransportSecurity{TLSMode: tlsModePlaintext}
	}
}

func batchFromProto(req *ingestionv1.PushBatchRequest) (ingest.IngestBatch, error) {
	batch := ingest.IngestBatch{
		BatchID:  req.GetBatchId(),
		Metadata: cloneStringMap(req.GetMetadata()),
		SentAt:   req.GetSentAt(),
		Events:   make([]ingest.RawEvent, 0, len(req.GetEvents())),
	}

	if agent := req.GetAgent(); agent != nil {
		batch.Agent = ingest.AgentInfo{
			AgentID:     agent.GetAgentId(),
			TenantID:    agent.GetTenantId(),
			WorkspaceID: agent.GetWorkspaceId(),
			Hostname:    agent.GetHostname(),
			Version:     agent.GetVersion(),
		}
	}

	if source := req.GetSource(); source != nil {
		batch.Source = ingest.SourceInfo{
			SourceID:   source.GetSourceId(),
			Provider:   source.GetProvider(),
			SourceType: source.GetSourceType(),
		}
	}

	for idx, item := range req.GetEvents() {
		payload := item.GetPayload()
		if len(payload) > 0 && !json.Valid(payload) {
			return ingest.IngestBatch{}, fmt.Errorf("events[%d].payload 不是合法 JSON", idx)
		}

		event := ingest.RawEvent{
			EventID:    item.GetEventId(),
			SessionID:  item.GetSessionId(),
			EventType:  item.GetEventType(),
			Role:       item.GetRole(),
			Text:       item.GetText(),
			Model:      item.GetModel(),
			OccurredAt: item.GetOccurredAt(),
			Tokens: ingest.TokenUsage{
				InputTokens:      item.GetTokens().GetInputTokens(),
				OutputTokens:     item.GetTokens().GetOutputTokens(),
				CacheReadTokens:  item.GetTokens().GetCacheReadTokens(),
				CacheWriteTokens: item.GetTokens().GetCacheWriteTokens(),
				ReasoningTokens:  item.GetTokens().GetReasoningTokens(),
			},
			CostMode:   item.GetCostMode(),
			SourcePath: item.GetSourcePath(),
			Metadata:   cloneStringMap(item.GetMetadata()),
		}
		if item.CostUsd != nil {
			value := item.GetCostUsd()
			event.CostUSD = &value
		}
		if item.SourceOffset != nil {
			value := item.GetSourceOffset()
			event.SourceOffset = &value
		}
		if len(payload) > 0 {
			event.Payload = append(json.RawMessage(nil), payload...)
		}
		batch.Events = append(batch.Events, event)
	}

	return batch, nil
}

func responseToProto(resp ingestResponse) *ingestionv1.PushBatchResponse {
	out := &ingestionv1.PushBatchResponse{
		BatchId:    resp.BatchID,
		Accepted:   int32(resp.Accepted),
		Rejected:   int32(resp.Rejected),
		DurationMs: resp.DurationMS,
	}
	if len(resp.Errors) == 0 {
		return out
	}

	out.Errors = make([]*ingestionv1.PushBatchResponse_ErrorItem, 0, len(resp.Errors))
	for _, item := range resp.Errors {
		out.Errors = append(out.Errors, &ingestionv1.PushBatchResponse_ErrorItem{
			Index:   int32(item.Index),
			EventId: item.EventID,
			Message: item.Message,
		})
	}
	return out
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for k, v := range input {
		out[k] = v
	}
	return out
}
