package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	ingestionv1 "github.com/agentledger/agentledger/packages/gen/go/ingestion/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type grpcClientSecurityConfig struct {
	Plaintext          bool
	CAFile             string
	ServerName         string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
}

type grpcEndpoint struct {
	target          string
	usesGRPCSScheme bool
}

func sendIngestRequestGRPC(
	endpoint string,
	timeout time.Duration,
	authHeader string,
	request ingestBatchRequest,
	securityConfig grpcClientSecurityConfig,
) (int, []byte, error) {
	parsedEndpoint, err := parseGRPCEndpoint(endpoint)
	if err != nil {
		return 0, nil, err
	}

	transportCredentials, err := buildGRPCTransportCredentials(parsedEndpoint, securityConfig)
	if err != nil {
		return 0, nil, err
	}

	conn, err := grpc.NewClient(parsedEndpoint.target, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		return 0, nil, fmt.Errorf("创建 gRPC 连接失败: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if token := strings.TrimSpace(authHeader); token != "" {
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", token))
	}

	client := ingestionv1.NewIngestServiceClient(conn)
	grpcResp, err := client.PushBatch(ctx, requestToProto(request))
	if err != nil {
		return 0, nil, fmt.Errorf("调用 PushBatch 失败: %w", err)
	}

	resp := responseFromProto(grpcResp)
	body, err := json.Marshal(resp)
	if err != nil {
		return 0, nil, fmt.Errorf("序列化响应失败: %w", err)
	}

	return grpcResponseStatus(grpcResp), body, nil
}

func normalizeGRPCEndpoint(raw string) (string, error) {
	parsed, err := parseGRPCEndpoint(raw)
	if err != nil {
		return "", err
	}
	return parsed.target, nil
}

func parseGRPCEndpoint(raw string) (grpcEndpoint, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		return grpcEndpoint{}, fmt.Errorf("endpoint 不能为空")
	}

	endpoint := grpcEndpoint{}
	if strings.Contains(target, "://") {
		parsed, err := url.Parse(target)
		if err != nil {
			return grpcEndpoint{}, fmt.Errorf("解析 endpoint 失败: %w", err)
		}
		scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
		switch scheme {
		case "grpcs":
			endpoint.usesGRPCSScheme = true
		case "http", "https":
			return grpcEndpoint{}, fmt.Errorf("gRPC endpoint 不支持 %s://，请改用 grpcs:// 或 host:port", scheme)
		default:
			return grpcEndpoint{}, fmt.Errorf("gRPC endpoint scheme 仅支持 grpcs://，当前为 %s://", scheme)
		}
		if parsed.Host == "" {
			return grpcEndpoint{}, fmt.Errorf("endpoint 缺少主机信息: %s", raw)
		}
		if parsed.Path != "" && parsed.Path != "/" {
			return grpcEndpoint{}, fmt.Errorf("gRPC endpoint 不能包含路径: %s", raw)
		}
		if parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
			return grpcEndpoint{}, fmt.Errorf("gRPC endpoint 不能包含查询参数、片段或用户信息: %s", raw)
		}
		target = parsed.Host
	}
	if err := validateGRPCTarget(target); err != nil {
		return grpcEndpoint{}, err
	}
	endpoint.target = target
	return endpoint, nil
}

func validateGRPCTarget(target string) error {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return fmt.Errorf("gRPC endpoint 必须为 host:port 或 grpcs://host:port: %s", target)
	}
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("gRPC endpoint 缺少主机信息: %s", target)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("gRPC endpoint 端口非法: %s", target)
	}
	return nil
}

func validateRunGRPCConfig(protocol string, config grpcClientSecurityConfig) error {
	cfg := config.normalized()

	if protocol != "grpc" {
		if cfg.hasCustomSettings() {
			return fmt.Errorf("grpc-* 参数仅在 --protocol=grpc 时可用")
		}
		return nil
	}

	if cfg.Plaintext {
		if cfg.CAFile != "" || cfg.ServerName != "" || cfg.CertFile != "" || cfg.KeyFile != "" || cfg.InsecureSkipVerify {
			return fmt.Errorf("启用 grpc-plaintext 时不能同时设置 TLS 相关参数")
		}
		return nil
	}

	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return fmt.Errorf("grpc-cert-file 与 grpc-key-file 必须同时设置")
	}
	return nil
}

func (c grpcClientSecurityConfig) normalized() grpcClientSecurityConfig {
	c.CAFile = strings.TrimSpace(c.CAFile)
	c.ServerName = strings.TrimSpace(c.ServerName)
	c.CertFile = strings.TrimSpace(c.CertFile)
	c.KeyFile = strings.TrimSpace(c.KeyFile)
	return c
}

func (c grpcClientSecurityConfig) hasCustomSettings() bool {
	return c.Plaintext ||
		c.InsecureSkipVerify ||
		c.CAFile != "" ||
		c.ServerName != "" ||
		c.CertFile != "" ||
		c.KeyFile != ""
}

func buildGRPCTransportCredentials(
	endpoint grpcEndpoint,
	securityConfig grpcClientSecurityConfig,
) (credentials.TransportCredentials, error) {
	cfg := securityConfig.normalized()
	if cfg.Plaintext {
		if endpoint.usesGRPCSScheme {
			return nil, fmt.Errorf("endpoint 使用 grpcs:// 时不能启用 grpc-plaintext")
		}
		return insecure.NewCredentials(), nil
	}

	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}
	if cfg.ServerName != "" {
		tlsConfig.ServerName = cfg.ServerName
	} else {
		serverName, err := serverNameFromTarget(endpoint.target)
		if err != nil {
			return nil, err
		}
		tlsConfig.ServerName = serverName
	}

	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("读取 grpc-ca-file 失败: %w", err)
		}
		rootCAs := x509.NewCertPool()
		if !rootCAs.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("grpc-ca-file 未包含有效 PEM 证书")
		}
		tlsConfig.RootCAs = rootCAs
	}

	if cfg.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("加载 gRPC 客户端证书失败: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	return credentials.NewTLS(tlsConfig), nil
}

func serverNameFromTarget(target string) (string, error) {
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return "", fmt.Errorf("解析 endpoint 主机失败: %w", err)
	}
	if strings.TrimSpace(host) == "" {
		return "", fmt.Errorf("endpoint 缺少主机信息: %s", target)
	}
	return host, nil
}

func grpcResponseStatus(resp *ingestionv1.PushBatchResponse) int {
	if resp == nil {
		return http.StatusBadGateway
	}

	for _, item := range resp.GetErrors() {
		if item.GetIndex() < 0 {
			return http.StatusUnprocessableEntity
		}
	}

	if resp.GetAccepted() > 0 {
		return http.StatusAccepted
	}
	if resp.GetRejected() == 0 {
		return http.StatusAccepted
	}
	return http.StatusBadGateway
}

func requestToProto(request ingestBatchRequest) *ingestionv1.PushBatchRequest {
	out := &ingestionv1.PushBatchRequest{
		BatchId: request.BatchID,
		Agent: &ingestionv1.PushBatchRequest_AgentInfo{
			AgentId:     request.Agent.AgentID,
			TenantId:    request.Agent.TenantID,
			WorkspaceId: request.Agent.WorkspaceID,
			Hostname:    request.Agent.Hostname,
			Version:     request.Agent.Version,
		},
		Source: &ingestionv1.PushBatchRequest_SourceInfo{
			SourceId:   request.Source.SourceID,
			Provider:   request.Source.Provider,
			SourceType: request.Source.SourceType,
		},
		Events: make([]*ingestionv1.PushBatchRequest_Event, 0, len(request.Events)),
		SentAt: request.SentAt,
	}

	for _, event := range request.Events {
		pbEvent := &ingestionv1.PushBatchRequest_Event{
			EventId:    event.EventID,
			SessionId:  event.SessionID,
			EventType:  event.EventType,
			Role:       event.Role,
			Text:       event.Text,
			Model:      event.Model,
			OccurredAt: event.OccurredAt,
			Tokens: &ingestionv1.PushBatchRequest_TokenUsage{
				InputTokens:      event.Tokens.InputTokens,
				OutputTokens:     event.Tokens.OutputTokens,
				CacheReadTokens:  event.Tokens.CacheReadTokens,
				CacheWriteTokens: event.Tokens.CacheWriteTokens,
				ReasoningTokens:  event.Tokens.ReasoningTokens,
			},
			CostMode:   event.CostMode,
			SourcePath: event.SourcePath,
			Payload:    append([]byte(nil), event.Payload...),
		}
		if event.CostUSD != nil {
			value := *event.CostUSD
			pbEvent.CostUsd = &value
		}
		if event.SourceOffset != nil {
			value := *event.SourceOffset
			pbEvent.SourceOffset = &value
		}
		out.Events = append(out.Events, pbEvent)
	}

	return out
}

func responseFromProto(resp *ingestionv1.PushBatchResponse) ingestBatchResponse {
	out := ingestBatchResponse{
		BatchID:    resp.GetBatchId(),
		Accepted:   int(resp.GetAccepted()),
		Rejected:   int(resp.GetRejected()),
		DurationMS: resp.GetDurationMs(),
	}
	if len(resp.GetErrors()) == 0 {
		return out
	}

	out.Errors = make([]ingestRejectError, 0, len(resp.GetErrors()))
	for _, item := range resp.GetErrors() {
		out.Errors = append(out.Errors, ingestRejectError{
			Index:   int(item.GetIndex()),
			EventID: item.GetEventId(),
			Message: item.GetMessage(),
		})
	}
	return out
}
