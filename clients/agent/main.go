package main

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var (
	version   = "0.1.0-dev"
	commit    = "none"
	buildTime = "unknown"
)

const (
	defaultHTTPEndpoint        = "http://127.0.0.1:8081/v1/ingest"
	defaultGRPCEndpoint        = "127.0.0.1:9091"
	defaultControlPlaneBaseURL = "http://127.0.0.1:8080"
	agentQueueDirEnv           = "AGENT_QUEUE_DIR"
	agentConfigDirEnv          = "AGENT_CONFIG_DIR"
	agentGatewayURLEnv         = "AGENT_GATEWAY_URL"
	agentReleaseChannelEnv     = "AGENT_RELEASE_CHANNEL"
	agentReleasePublicKeyEnv   = "AGENT_RELEASE_SIGNING_PUBLIC_KEY_FILE"
	defaultGatewayURL          = "http://127.0.0.1:8080"
	defaultConfigWatchInterval = 5 * time.Minute
)

type runCommandDependencies struct {
	now                func() time.Time
	fetchRuntimeConfig func(string, string, string, time.Duration) (*agentRuntimeConfigResponse, error)
	postHeartbeat      func(string, string, time.Duration, agentHeartbeatRequest) error
}

type runCommandOptions struct {
	Endpoint          string
	EndpointExplicit  bool
	Protocol          string
	JSONLPath         string
	Generate          int
	Timeout           time.Duration
	TokenFile         string
	AgentID           string
	SourceID          string
	Provider          string
	SourceType        string
	SessionID         string
	BatchID           string
	QueueDir          string
	ResolvedQueueDir  string
	QueueEnabled      bool
	ControlPlane      string
	Daemon            bool
	HeartbeatInterval time.Duration
	GRPCConfig        grpcClientSecurityConfig
}

type daemonRunState struct {
	RuntimeConfig        *agentRuntimeConfigResponse
	LastIngestStatusCode int
	LastIngestAccepted   int
	LastIngestRejected   int
	LastError            string
	LastConfigFetchedAt  string
}

type agentRuntimeConfigResponse struct {
	TenantID      string                     `json:"tenantId"`
	Agent         agentRuntimeConfigAgent    `json:"agent"`
	Runtime       agentRuntimeConfigRuntime  `json:"runtime"`
	Bindings      agentRuntimeConfigBindings `json:"bindings"`
	ConfigVersion string                     `json:"configVersion"`
	UpdatedAt     string                     `json:"updatedAt"`
}

type agentRuntimeConfigAgent struct {
	AgentID     string `json:"agentId"`
	DeviceID    string `json:"deviceId,omitempty"`
	Hostname    string `json:"hostname,omitempty"`
	Version     string `json:"version,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
}

type agentRuntimeConfigRuntime struct {
	HeartbeatIntervalSeconds int    `json:"heartbeatIntervalSeconds,omitempty"`
	StaleAfterSeconds        int    `json:"staleAfterSeconds,omitempty"`
	IngestProtocol           string `json:"ingestProtocol,omitempty"`
	IngestEndpoint           string `json:"ingestEndpoint,omitempty"`
	SampleGenerateCount      int    `json:"sampleGenerateCount,omitempty"`
}

type agentRuntimeConfigBindings struct {
	SourceCount int                        `json:"sourceCount"`
	SourceIDs   []string                   `json:"sourceIds,omitempty"`
	Sources     []agentRuntimeConfigSource `json:"sources,omitempty"`
}

type agentRuntimeConfigSource struct {
	SourceID     string `json:"sourceId"`
	Name         string `json:"name,omitempty"`
	AccessMode   string `json:"accessMode,omitempty"`
	Enabled      bool   `json:"enabled"`
	Location     string `json:"location,omitempty"`
	SourceRegion string `json:"sourceRegion,omitempty"`
}

type agentHeartbeatRequest struct {
	AgentID              string   `json:"agentId"`
	SessionID            string   `json:"sessionId,omitempty"`
	Hostname             string   `json:"hostname,omitempty"`
	Version              string   `json:"version,omitempty"`
	Daemon               bool     `json:"daemon"`
	OccurredAt           string   `json:"occurredAt"`
	ConfigVersion        string   `json:"configVersion,omitempty"`
	ConfigFetchedAt      string   `json:"configFetchedAt,omitempty"`
	HeartbeatIntervalSec int64    `json:"heartbeatIntervalSec,omitempty"`
	IngestProtocol       string   `json:"ingestProtocol,omitempty"`
	IngestEndpoint       string   `json:"ingestEndpoint,omitempty"`
	SourceCount          int      `json:"sourceCount,omitempty"`
	SourceIDs            []string `json:"sourceIds,omitempty"`
	LastIngestStatusCode int      `json:"lastIngestStatusCode,omitempty"`
	LastAccepted         int      `json:"lastAccepted,omitempty"`
	LastRejected         int      `json:"lastRejected,omitempty"`
	LastError            string   `json:"lastError,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "run":
		os.Exit(runCommand(os.Args[2:]))
	case "collect":
		os.Exit(collectCommand(os.Args[2:]))
	case "oidc":
		os.Exit(oidcCommand(os.Args[2:]))
	case "doctor":
		os.Exit(doctorCommand(os.Args[2:]))
	case "config":
		os.Exit(configCommand(os.Args[2:]))
	case "status":
		os.Exit(statusCommand(os.Args[2:]))
	case "update":
		os.Exit(updateCommand(os.Args[2:]))
	case "version":
		os.Exit(versionCommand(os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n", os.Args[1])
		printUsage()
		os.Exit(2)
	}
}

func runCommand(args []string) int {
	return runCommandWithDependencies(args, defaultRunCommandDependencies())
}

func defaultRunCommandDependencies() runCommandDependencies {
	return runCommandDependencies{
		now:                func() time.Time { return time.Now().UTC() },
		fetchRuntimeConfig: fetchAgentRuntimeConfig,
		postHeartbeat:      postAgentHeartbeat,
	}
}

func runCommandWithDependencies(args []string, deps runCommandDependencies) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	host, _ := os.Hostname()
	if strings.TrimSpace(host) == "" {
		host = "local-host"
	}
	defaultSessionID := newID("session")
	defaultAgentID := fmt.Sprintf("%s-agent", host)

	endpoint := fs.String("endpoint", "", "ingestion-gateway 地址（未显式指定时按协议自动选择）")
	protocol := fs.String("protocol", "http", "上报协议：http|grpc")
	jsonlPath := fs.String("jsonl", "", "样本 JSONL 文件（每行一条事件 JSON）")
	generate := fs.Int("generate", 5, "自动生成样本事件数量（jsonl 为空时生效）")
	timeout := fs.Duration("timeout", 10*time.Second, "请求超时时间")
	tokenFile := fs.String("token-file", defaultTokenFilePath(), "本地 token 文件路径")
	agentID := fs.String("agent-id", defaultAgentID, "agent 标识")
	sourceID := fs.String("source-id", host, "来源 ID")
	provider := fs.String("provider", "codex-cli", "来源 provider")
	sourceType := fs.String("source-type", "agent", "来源类型")
	sessionID := fs.String("session-id", defaultSessionID, "会话 ID（事件未指定时自动填充）")
	batchID := fs.String("batch-id", "", "批次 ID（留空自动生成）")
	queueDir := fs.String("queue-dir", "", "本地持久队列目录（可选；未指定且未设置 AGENT_QUEUE_DIR 时关闭）")
	controlPlane := fs.String("control-plane", defaultControlPlaneBaseURL, "control-plane 基础地址（守护模式用于配置拉取与心跳上报）")
	daemon := fs.Bool("daemon", false, "启用守护模式：周期拉取配置、上报事件并回写心跳")
	heartbeatInterval := fs.Duration("heartbeat-interval", 30*time.Second, "守护模式心跳与采集循环间隔（服务端未下发时生效）")
	grpcPlaintext := fs.Bool("grpc-plaintext", false, "使用明文 gRPC（禁用 TLS）")
	grpcCAFile := fs.String("grpc-ca-file", "", "gRPC TLS CA 证书文件（PEM）")
	grpcServerName := fs.String("grpc-server-name", "", "gRPC TLS 服务端证书校验名称")
	grpcCertFile := fs.String("grpc-cert-file", "", "gRPC mTLS 客户端证书文件（PEM）")
	grpcKeyFile := fs.String("grpc-key-file", "", "gRPC mTLS 客户端私钥文件（PEM）")
	grpcInsecureSkipVerify := fs.Bool("grpc-insecure-skip-verify", false, "跳过 gRPC TLS 证书校验（仅测试环境）")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	endpointExplicit := isFlagProvided(fs, "endpoint")
	if endpointExplicit && strings.TrimSpace(*endpoint) == "" {
		fmt.Fprintln(os.Stderr, "endpoint 不能为空")
		return 2
	}

	if strings.TrimSpace(*agentID) == "" {
		fmt.Fprintln(os.Stderr, "agent-id 不能为空")
		return 2
	}
	if strings.TrimSpace(*sourceID) == "" {
		fmt.Fprintln(os.Stderr, "source-id 不能为空")
		return 2
	}
	if strings.TrimSpace(*provider) == "" {
		fmt.Fprintln(os.Stderr, "provider 不能为空")
		return 2
	}
	if *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "timeout 必须 > 0")
		return 2
	}
	if *heartbeatInterval <= 0 {
		fmt.Fprintln(os.Stderr, "heartbeat-interval 必须 > 0")
		return 2
	}
	protocolName := strings.ToLower(strings.TrimSpace(*protocol))
	if protocolName != "http" && protocolName != "grpc" {
		fmt.Fprintln(os.Stderr, "protocol 仅支持 http 或 grpc")
		return 2
	}
	endpointValue := resolveEndpoint(*endpoint, protocolName, endpointExplicit)
	grpcConfig := grpcClientSecurityConfig{
		Plaintext:          *grpcPlaintext,
		CAFile:             *grpcCAFile,
		ServerName:         *grpcServerName,
		CertFile:           *grpcCertFile,
		KeyFile:            *grpcKeyFile,
		InsecureSkipVerify: *grpcInsecureSkipVerify,
	}
	if err := validateRunGRPCConfig(protocolName, grpcConfig); err != nil {
		fmt.Fprintf(os.Stderr, "gRPC 参数错误: %v\n", err)
		return 2
	}
	controlPlaneBaseURL := ""
	if *daemon {
		var err error
		controlPlaneBaseURL, err = resolveControlPlaneBaseURL(*controlPlane, isFlagProvided(fs, "control-plane"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "control-plane 参数错误: %v\n", err)
			return 2
		}
	}
	resolvedQueueDir, queueEnabled, err := resolveAgentQueueDir(*queueDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "queue-dir 参数错误: %v\n", err)
		return 2
	}

	authHeader := ""
	token, tokenPath, err := loadLocalToken(*tokenFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if *daemon {
				fmt.Fprintf(os.Stderr, "守护模式要求本地 token，未找到：%s\n", tokenPath)
				return 1
			}
			fmt.Fprintf(os.Stderr, "提示: 未找到本地 token（%s），将以匿名方式请求。\n", tokenPath)
		} else {
			fmt.Fprintf(os.Stderr, "读取本地 token 失败: %v\n", err)
			return 1
		}
	} else {
		if token.IsExpired(time.Now().UTC()) {
			fmt.Fprintf(os.Stderr, "提示: 本地 token 可能已过期（%s），建议重新执行 `agent oidc login`。\n", token.ExpiresAt)
		}
		authHeader = token.AuthHeader()
	}

	options := runCommandOptions{
		Endpoint:          endpointValue,
		EndpointExplicit:  endpointExplicit,
		Protocol:          protocolName,
		JSONLPath:         *jsonlPath,
		Generate:          *generate,
		Timeout:           *timeout,
		TokenFile:         *tokenFile,
		AgentID:           strings.TrimSpace(*agentID),
		SourceID:          strings.TrimSpace(*sourceID),
		Provider:          strings.TrimSpace(*provider),
		SourceType:        strings.TrimSpace(*sourceType),
		SessionID:         *sessionID,
		BatchID:           *batchID,
		QueueDir:          *queueDir,
		ResolvedQueueDir:  resolvedQueueDir,
		QueueEnabled:      queueEnabled,
		ControlPlane:      controlPlaneBaseURL,
		Daemon:            *daemon,
		HeartbeatInterval: *heartbeatInterval,
		GRPCConfig:        grpcConfig,
	}
	if options.Daemon {
		return runDaemonLoop(host, options, authHeader, deps)
	}
	return runSingleCommand(host, options, authHeader, deps.now())
}

func buildRunRequest(host string, options runCommandOptions, now time.Time) (ingestBatchRequest, error) {
	events, err := buildRunEvents(options.JSONLPath, options.Generate, options.SessionID, options.Provider)
	if err != nil {
		return ingestBatchRequest{}, err
	}
	if len(events) == 0 {
		return ingestBatchRequest{}, fmt.Errorf("没有可推送事件")
	}
	return ingestBatchRequest{
		BatchID: resolveBatchID(options.BatchID),
		Agent: agentInfo{
			AgentID:     options.AgentID,
			Hostname:    host,
			Version:     version,
			WorkspaceID: "",
		},
		Source: sourceInfo{
			SourceID:   options.SourceID,
			Provider:   options.Provider,
			SourceType: options.SourceType,
		},
		Events: events,
		SentAt: now.UTC().Format(time.RFC3339Nano),
	}, nil
}

func executeRunRequest(
	options runCommandOptions,
	authHeader string,
	request ingestBatchRequest,
	requestTime time.Time,
) (int, []byte, error) {
	if options.QueueEnabled {
		entry, _, queueErr := enqueueAgentQueueRequest(
			options.ResolvedQueueDir,
			request,
			requestTime.UTC(),
		)
		if queueErr != nil {
			return 0, nil, fmt.Errorf("写入本地队列失败: %w", queueErr)
		}
		flushResult, flushErr := flushAgentQueue(
			options.ResolvedQueueDir,
			options.Endpoint,
			options.Timeout,
			options.Protocol,
			authHeader,
			options.GRPCConfig,
			entry.ID,
		)
		if flushErr != nil {
			queueStatus, statusErr := readStatusQueue(options.QueueDir)
			if statusErr == nil {
				return 0, nil, fmt.Errorf(
					"队列冲刷失败: %w（pending=%d oldest=%s total_bytes=%d）",
					flushErr,
					queueStatus.PendingCount,
					queueStatus.OldestEnqueuedAt,
					queueStatus.TotalBytes,
				)
			}
			return 0, nil, fmt.Errorf("队列冲刷失败: %w", flushErr)
		}
		if flushResult.CurrentResponse == nil {
			return 0, nil, fmt.Errorf("队列冲刷未返回当前批次结果")
		}
		return flushResult.CurrentResponse.StatusCode, flushResult.CurrentResponse.Body, nil
	}
	statusCode, responseBody, err := sendIngestRequest(
		options.Endpoint,
		options.Timeout,
		options.Protocol,
		authHeader,
		request,
		options.GRPCConfig,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("调用 ingestion-gateway 失败: %w", err)
	}
	return statusCode, responseBody, nil
}

func printIngestResult(protocol string, statusCode int, responseBody []byte) (int, int) {
	var response ingestBatchResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		fmt.Printf("推送完成: protocol=%s status=%d 响应=%s\n", protocol, statusCode, string(responseBody))
		return 0, 0
	}

	fmt.Printf(
		"推送完成: protocol=%s status=%d batch=%s accepted=%d rejected=%d duration_ms=%d\n",
		protocol,
		statusCode,
		response.BatchID,
		response.Accepted,
		response.Rejected,
		response.DurationMS,
	)
	for _, reject := range response.Errors {
		fmt.Printf("  reject index=%d event_id=%s message=%s\n", reject.Index, reject.EventID, reject.Message)
	}
	return response.Accepted, response.Rejected
}

func runSingleCommand(host string, options runCommandOptions, authHeader string, now time.Time) int {
	request, err := buildRunRequest(host, options, now)
	if err != nil {
		fmt.Fprintf(os.Stderr, "构建事件失败: %v\n", err)
		return 1
	}
	statusCode, responseBody, err := executeRunRequest(options, authHeader, request, now)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return 1
	}
	printIngestResult(options.Protocol, statusCode, responseBody)
	return exitCodeFromStatus(statusCode)
}

func runDaemonLoop(
	host string,
	options runCommandOptions,
	authHeader string,
	deps runCommandDependencies,
) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	state := daemonRunState{}
	if err := runDaemonCycle(ctx, host, options, authHeader, deps, &state); err != nil {
		fmt.Fprintf(os.Stderr, "daemon 首轮失败: %v\n", err)
	}

	interval := resolveDaemonHeartbeatInterval(options, state.RuntimeConfig)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "daemon 已停止")
			return 0
		case <-ticker.C:
			if err := runDaemonCycle(ctx, host, options, authHeader, deps, &state); err != nil {
				fmt.Fprintf(os.Stderr, "daemon 周期失败: %v\n", err)
			}
			nextInterval := resolveDaemonHeartbeatInterval(options, state.RuntimeConfig)
			if nextInterval != interval {
				ticker.Reset(nextInterval)
				interval = nextInterval
			}
		}
	}
}

func runDaemonCycle(
	ctx context.Context,
	host string,
	options runCommandOptions,
	authHeader string,
	deps runCommandDependencies,
	state *daemonRunState,
) error {
	runtimeConfig, err := deps.fetchRuntimeConfig(
		options.ControlPlane,
		authHeader,
		options.AgentID,
		options.Timeout,
	)
	if err != nil {
		state.LastError = err.Error()
		return err
	}
	state.RuntimeConfig = runtimeConfig
	state.LastConfigFetchedAt = deps.now().UTC().Format(time.RFC3339Nano)

	cycleOptions := applyRuntimeConfigOverrides(options, runtimeConfig)
	now := deps.now()
	request, err := buildRunRequest(host, cycleOptions, now)
	if err != nil {
		state.LastError = err.Error()
		heartbeat := buildHeartbeatRequest(host, cycleOptions, runtimeConfig, state, now)
		_ = deps.postHeartbeat(options.ControlPlane, authHeader, options.Timeout, heartbeat)
		return err
	}

	statusCode, responseBody, err := executeRunRequest(cycleOptions, authHeader, request, now)
	if err != nil {
		state.LastError = err.Error()
		heartbeat := buildHeartbeatRequest(host, cycleOptions, runtimeConfig, state, now)
		_ = deps.postHeartbeat(options.ControlPlane, authHeader, options.Timeout, heartbeat)
		return err
	}

	accepted, rejected := printIngestResult(cycleOptions.Protocol, statusCode, responseBody)
	state.LastIngestStatusCode = statusCode
	state.LastIngestAccepted = accepted
	state.LastIngestRejected = rejected
	if exitCodeFromStatus(statusCode) == 0 {
		state.LastError = ""
	} else {
		state.LastError = fmt.Sprintf("ingestion status=%d", statusCode)
	}

	heartbeat := buildHeartbeatRequest(host, cycleOptions, runtimeConfig, state, now)
	if err := deps.postHeartbeat(options.ControlPlane, authHeader, options.Timeout, heartbeat); err != nil {
		state.LastError = err.Error()
		return err
	}
	return nil
}

func buildHeartbeatRequest(
	host string,
	options runCommandOptions,
	runtimeConfig *agentRuntimeConfigResponse,
	state *daemonRunState,
	now time.Time,
) agentHeartbeatRequest {
	runtimeHostname := ""
	runtimeVersion := ""
	runtimeConfigVersion := ""
	runtimeSourceCount := 0
	runtimeSourceIDs := []string(nil)
	if runtimeConfig != nil {
		runtimeHostname = runtimeConfig.Agent.Hostname
		runtimeVersion = runtimeConfig.Agent.Version
		runtimeConfigVersion = runtimeConfig.ConfigVersion
		runtimeSourceCount = runtimeConfig.Bindings.SourceCount
		runtimeSourceIDs = runtimeConfigSourceIDs(runtimeConfig)
	}
	return agentHeartbeatRequest{
		AgentID:              options.AgentID,
		SessionID:            options.SessionID,
		Hostname:             firstNonEmpty(strings.TrimSpace(host), runtimeHostname),
		Version:              firstNonEmpty(version, runtimeVersion),
		Daemon:               true,
		OccurredAt:           now.UTC().Format(time.RFC3339Nano),
		ConfigVersion:        firstNonEmpty(runtimeConfigVersion),
		ConfigFetchedAt:      state.LastConfigFetchedAt,
		HeartbeatIntervalSec: int64(resolveDaemonHeartbeatInterval(options, runtimeConfig) / time.Second),
		IngestProtocol:       options.Protocol,
		IngestEndpoint:       options.Endpoint,
		SourceCount:          runtimeSourceCount,
		SourceIDs:            runtimeSourceIDs,
		LastIngestStatusCode: state.LastIngestStatusCode,
		LastAccepted:         state.LastIngestAccepted,
		LastRejected:         state.LastIngestRejected,
		LastError:            state.LastError,
	}
}

func resolveDaemonHeartbeatInterval(
	options runCommandOptions,
	runtimeConfig *agentRuntimeConfigResponse,
) time.Duration {
	if runtimeConfig != nil && runtimeConfig.Runtime.HeartbeatIntervalSeconds > 0 {
		return time.Duration(runtimeConfig.Runtime.HeartbeatIntervalSeconds) * time.Second
	}
	if options.HeartbeatInterval > 0 {
		return options.HeartbeatInterval
	}
	return 30 * time.Second
}

func applyRuntimeConfigOverrides(
	options runCommandOptions,
	runtimeConfig *agentRuntimeConfigResponse,
) runCommandOptions {
	next := options
	if runtimeConfig == nil {
		return next
	}
	if runtimeConfig.Runtime.IngestProtocol == "grpc" || runtimeConfig.Runtime.IngestProtocol == "http" {
		next.Protocol = runtimeConfig.Runtime.IngestProtocol
	}
	next.Endpoint = resolveEndpoint(
		runtimeConfig.Runtime.IngestEndpoint,
		next.Protocol,
		false,
	)
	if runtimeConfig.Runtime.SampleGenerateCount > 0 && strings.TrimSpace(next.JSONLPath) == "" {
		next.Generate = runtimeConfig.Runtime.SampleGenerateCount
	}
	return next
}

func runtimeConfigSourceIDs(runtimeConfig *agentRuntimeConfigResponse) []string {
	if runtimeConfig == nil {
		return nil
	}
	sourceIDs := make([]string, 0, len(runtimeConfig.Bindings.SourceIDs))
	for _, item := range runtimeConfig.Bindings.SourceIDs {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			sourceIDs = append(sourceIDs, trimmed)
		}
	}
	return sourceIDs
}

type ingestBatchRequest struct {
	BatchID string       `json:"batch_id"`
	Agent   agentInfo    `json:"agent"`
	Source  sourceInfo   `json:"source"`
	Events  []agentEvent `json:"events"`
	SentAt  string       `json:"sent_at"`
}

type agentInfo struct {
	AgentID     string `json:"agent_id"`
	TenantID    string `json:"tenant_id,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	Hostname    string `json:"hostname,omitempty"`
	Version     string `json:"version,omitempty"`
}

type sourceInfo struct {
	SourceID   string `json:"source_id"`
	Provider   string `json:"provider"`
	SourceType string `json:"source_type,omitempty"`
}

type tokenUsage struct {
	InputTokens      int64 `json:"input_tokens,omitempty"`
	OutputTokens     int64 `json:"output_tokens,omitempty"`
	CacheReadTokens  int64 `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens int64 `json:"cache_write_tokens,omitempty"`
	ReasoningTokens  int64 `json:"reasoning_tokens,omitempty"`
}

type agentEvent struct {
	EventID      string          `json:"event_id,omitempty"`
	SessionID    string          `json:"session_id"`
	EventType    string          `json:"event_type"`
	Role         string          `json:"role,omitempty"`
	Text         string          `json:"text,omitempty"`
	Model        string          `json:"model,omitempty"`
	OccurredAt   string          `json:"occurred_at,omitempty"`
	Tokens       tokenUsage      `json:"tokens,omitempty"`
	CostUSD      *float64        `json:"cost_usd,omitempty"`
	CostMode     string          `json:"cost_mode,omitempty"`
	SourcePath   string          `json:"source_path,omitempty"`
	SourceOffset *int64          `json:"source_offset,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
}

type ingestBatchResponse struct {
	BatchID    string              `json:"batch_id"`
	Accepted   int                 `json:"accepted"`
	Rejected   int                 `json:"rejected"`
	DurationMS int64               `json:"duration_ms"`
	Errors     []ingestRejectError `json:"errors,omitempty"`
}

type ingestRejectError struct {
	Index   int    `json:"index"`
	EventID string `json:"event_id,omitempty"`
	Message string `json:"message"`
}

func buildRunEvents(jsonlPath string, generate int, sessionID, provider string) ([]agentEvent, error) {
	if strings.TrimSpace(jsonlPath) != "" {
		return loadEventsFromJSONL(jsonlPath, sessionID)
	}
	if generate <= 0 {
		return nil, fmt.Errorf("generate 必须 > 0")
	}
	return generateSampleEvents(generate, sessionID, provider), nil
}

func loadEventsFromJSONL(path, fallbackSessionID string) ([]agentEvent, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开 jsonl 失败: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	events := make([]agentEvent, 0)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		var event agentEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, fmt.Errorf("解析第 %d 行失败: %w", lineNo, err)
		}
		if strings.TrimSpace(event.EventID) == "" {
			event.EventID = newID("evt")
		}
		if strings.TrimSpace(event.SessionID) == "" {
			event.SessionID = fallbackSessionID
		}
		if strings.TrimSpace(event.EventType) == "" {
			event.EventType = "message"
		}
		if strings.TrimSpace(event.OccurredAt) == "" {
			event.OccurredAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		if strings.TrimSpace(event.CostMode) == "" {
			event.CostMode = "reported"
		}
		if strings.TrimSpace(event.SourcePath) == "" {
			event.SourcePath = "agent://jsonl"
		}
		if len(event.Payload) == 0 {
			event.Payload = json.RawMessage(`{"from":"jsonl"}`)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取 jsonl 失败: %w", err)
	}
	return events, nil
}

func generateSampleEvents(count int, sessionID, provider string) []agentEvent {
	events := make([]agentEvent, 0, count)
	baseTime := time.Now().UTC().Add(-time.Duration(count) * time.Second)
	for i := 0; i < count; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano)
		cost := float64(200+i*15) / 100000.0
		payload, _ := json.Marshal(map[string]any{
			"source":   "generated",
			"index":    i,
			"provider": provider,
		})
		events = append(events, agentEvent{
			EventID:    newID("evt"),
			SessionID:  sessionID,
			EventType:  "message",
			Role:       "assistant",
			Text:       fmt.Sprintf("sample message %d", i+1),
			Model:      "gpt-5-codex",
			OccurredAt: ts,
			Tokens: tokenUsage{
				InputTokens:  int64(100 + i*3),
				OutputTokens: int64(200 + i*7),
			},
			CostUSD:    &cost,
			CostMode:   "estimated",
			SourcePath: "agent://generated",
			Payload:    payload,
		})
	}
	return events
}

func sendIngestRequest(
	endpoint string,
	timeout time.Duration,
	protocol, authHeader string,
	request ingestBatchRequest,
	grpcConfig grpcClientSecurityConfig,
) (int, []byte, error) {
	switch protocol {
	case "http":
		return sendIngestRequestHTTP(endpoint, timeout, authHeader, request)
	case "grpc":
		return sendIngestRequestGRPC(endpoint, timeout, authHeader, request, grpcConfig)
	default:
		return 0, nil, fmt.Errorf("不支持的协议: %s", protocol)
	}
}

func isFlagProvided(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(item *flag.Flag) {
		if item.Name == name {
			found = true
		}
	})
	return found
}

func resolveEndpoint(rawEndpoint, protocol string, endpointExplicit bool) string {
	endpoint := strings.TrimSpace(rawEndpoint)
	if endpoint != "" {
		return endpoint
	}
	if endpointExplicit {
		return endpoint
	}
	return defaultEndpointForProtocol(protocol)
}

func defaultEndpointForProtocol(protocol string) string {
	switch protocol {
	case "grpc":
		return defaultGRPCEndpoint
	case "http":
		return defaultHTTPEndpoint
	default:
		return ""
	}
}

func exitCodeFromStatus(statusCode int) int {
	if statusCode >= http.StatusMultipleChoices {
		return 1
	}
	return 0
}

func sendIngestRequestHTTP(endpoint string, timeout time.Duration, authHeader string, request ingestBatchRequest) (int, []byte, error) {
	requestBody, err := json.Marshal(request)
	if err != nil {
		return 0, nil, fmt.Errorf("序列化请求失败: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return 0, nil, fmt.Errorf("构建请求失败: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(authHeader) != "" {
		httpReq.Header.Set("Authorization", authHeader)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(httpReq)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("读取响应失败: %w", err)
	}
	return resp.StatusCode, body, nil
}

func resolveBatchID(input string) string {
	trimmed := strings.TrimSpace(input)
	if trimmed != "" {
		return trimmed
	}
	return newID("batch")
}

func newID(prefix string) string {
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UTC().UnixNano())
	}
	return fmt.Sprintf("%s_%d_%x", prefix, time.Now().UTC().UnixMilli(), buf)
}

func doctorCommand(args []string) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	verbose := fs.Bool("v", false, "输出详细信息")
	endpoint := fs.String("endpoint", "", "ingestion-gateway 地址（未显式指定时按协议自动选择）")
	protocol := fs.String("protocol", "http", "检查协议：http|grpc")
	tokenFile := fs.String("token-file", defaultTokenFilePath(), "本地 token 文件路径")
	timeout := fs.Duration("timeout", 10*time.Second, "探测超时时间")
	gateway := fs.String("gateway", "", "control-plane 地址（用于可选生命周期上报）")
	reportLifecycle := fs.Bool("report-lifecycle", false, "向 control-plane 上报 doctor 生命周期事件")
	tenantID := fs.String("tenant-id", "", "生命周期上报使用的 tenant ID")
	agentID := fs.String("agent-id", defaultLifecycleAgentID(), "生命周期上报使用的 agent ID")
	deviceID := fs.String("device-id", "", "生命周期上报使用的 device ID")
	reportTimeout := fs.Duration("report-timeout", 5*time.Second, "生命周期上报超时时间")
	grpcPlaintext := fs.Bool("grpc-plaintext", false, "使用明文 gRPC（禁用 TLS）")
	grpcCAFile := fs.String("grpc-ca-file", "", "gRPC TLS CA 证书文件（PEM）")
	grpcServerName := fs.String("grpc-server-name", "", "gRPC TLS 服务端证书校验名称")
	grpcCertFile := fs.String("grpc-cert-file", "", "gRPC mTLS 客户端证书文件（PEM）")
	grpcKeyFile := fs.String("grpc-key-file", "", "gRPC mTLS 客户端私钥文件（PEM）")
	grpcInsecureSkipVerify := fs.Bool("grpc-insecure-skip-verify", false, "跳过 gRPC TLS 证书校验（仅测试环境）")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "timeout 必须 > 0")
		return 2
	}
	endpointExplicit := isFlagProvided(fs, "endpoint")
	if endpointExplicit && strings.TrimSpace(*endpoint) == "" {
		fmt.Fprintln(os.Stderr, "endpoint 不能为空")
		return 2
	}
	protocolName := strings.ToLower(strings.TrimSpace(*protocol))
	if protocolName != "http" && protocolName != "grpc" {
		fmt.Fprintln(os.Stderr, "protocol 仅支持 http 或 grpc")
		return 2
	}
	if *reportLifecycle {
		if strings.TrimSpace(*tenantID) == "" {
			fmt.Fprintln(os.Stderr, "report-lifecycle 启用时 tenant-id 不能为空")
			return 2
		}
		if *reportTimeout <= 0 {
			fmt.Fprintln(os.Stderr, "report-timeout 必须 > 0")
			return 2
		}
	}
	options := doctorOptions{
		Protocol:  protocolName,
		Endpoint:  resolveEndpoint(*endpoint, protocolName, endpointExplicit),
		TokenFile: *tokenFile,
		Timeout:   *timeout,
		Verbose:   *verbose,
		GRPCConfig: grpcClientSecurityConfig{
			Plaintext:          *grpcPlaintext,
			CAFile:             *grpcCAFile,
			ServerName:         *grpcServerName,
			CertFile:           *grpcCertFile,
			KeyFile:            *grpcKeyFile,
			InsecureSkipVerify: *grpcInsecureSkipVerify,
		},
	}
	report := runDoctorChecks(options, time.Now().UTC())

	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "doctor output marshal failed: %v\n", err)
		return 1
	}

	fmt.Println(string(output))
	if *reportLifecycle {
		baseURL, err := resolveControlPlaneBaseURL(*gateway, isFlagProvided(fs, "gateway"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "警告: 生命周期事件上报跳过，gateway 参数错误: %v\n", err)
		} else {
			resultStatus := "success"
			if report.OverallStatus == doctorStatusWarn {
				resultStatus = "warn"
			}
			if report.OverallStatus == doctorStatusFail {
				resultStatus = "failed"
			}
			if err := maybeReportLifecycleEvent(lifecycleReportOptions{
				Enabled:   true,
				BaseURL:   baseURL,
				TokenFile: *tokenFile,
				TenantID:  *tenantID,
				AgentID:   *agentID,
				DeviceID:  *deviceID,
				Action:    "doctor",
				Result:    resultStatus,
				Version:   version,
				Timeout:   *reportTimeout,
				Metadata: map[string]any{
					"command":       "doctor",
					"protocol":      protocolName,
					"endpoint":      options.Endpoint,
					"overallStatus": report.OverallStatus,
					"checkCount":    len(report.Checks),
				},
			}); err != nil {
				fmt.Fprintf(os.Stderr, "警告: 生命周期事件上报失败: %v\n", err)
			}
		}
	}
	if report.OverallStatus == doctorStatusFail {
		return 1
	}
	return 0
}

type doctorOptions struct {
	Protocol   string
	Endpoint   string
	TokenFile  string
	Timeout    time.Duration
	Verbose    bool
	GRPCConfig grpcClientSecurityConfig
}

type doctorReport struct {
	OverallStatus string        `json:"overall_status"`
	Checks        []doctorCheck `json:"checks"`
	Component     string        `json:"component"`
	GoVersion     string        `json:"go_version"`
	OS            string        `json:"os"`
	Arch          string        `json:"arch"`
	VerboseMode   bool          `json:"verbose_mode"`
}

type doctorCheck struct {
	Name    string         `json:"name"`
	Status  string         `json:"status"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type doctorEndpointProbe struct {
	Protocol string
	Target   string
	HTTPURL  *url.URL
}

const (
	doctorStatusPass = "pass"
	doctorStatusWarn = "warn"
	doctorStatusFail = "fail"
)

func runDoctorChecks(options doctorOptions, now time.Time) doctorReport {
	checks := make([]doctorCheck, 0, 4)
	checks = append(checks, checkTokenFileStatus(options.TokenFile, now))
	checks = append(checks, checkDoctorGRPCConfig(options.Protocol, options.GRPCConfig))

	endpointCheck, probe := checkDoctorEndpoint(options.Protocol, options.Endpoint)
	checks = append(checks, endpointCheck)
	checks = append(checks, checkDoctorConnectivity(options.Timeout, endpointCheck, probe))

	return doctorReport{
		OverallStatus: summarizeDoctorStatus(checks),
		Checks:        checks,
		Component:     "agent-cli",
		GoVersion:     runtime.Version(),
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		VerboseMode:   options.Verbose,
	}
}

func summarizeDoctorStatus(checks []doctorCheck) string {
	hasWarn := false
	for _, item := range checks {
		if item.Status == doctorStatusFail {
			return doctorStatusFail
		}
		if item.Status == doctorStatusWarn {
			hasWarn = true
		}
	}
	if hasWarn {
		return doctorStatusWarn
	}
	return doctorStatusPass
}

func checkTokenFileStatus(tokenFile string, now time.Time) doctorCheck {
	resolvedPath, err := resolveTokenFilePath(tokenFile)
	if err != nil {
		return doctorCheck{
			Name:    "token_file",
			Status:  doctorStatusFail,
			Message: "token 文件路径解析失败",
			Details: map[string]any{"error": err.Error()},
		}
	}

	info, err := os.Stat(resolvedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return doctorCheck{
				Name:    "token_file",
				Status:  doctorStatusWarn,
				Message: "token 文件不存在，将以匿名方式请求",
				Details: map[string]any{
					"path":  resolvedPath,
					"found": false,
				},
			}
		}
		return doctorCheck{
			Name:    "token_file",
			Status:  doctorStatusFail,
			Message: "读取 token 文件失败",
			Details: map[string]any{
				"path":  resolvedPath,
				"error": err.Error(),
			},
		}
	}
	if info.IsDir() {
		return doctorCheck{
			Name:    "token_file",
			Status:  doctorStatusFail,
			Message: "token 路径指向目录，需为文件",
			Details: map[string]any{
				"path": resolvedPath,
			},
		}
	}

	token, _, err := loadLocalToken(resolvedPath)
	if err != nil {
		return doctorCheck{
			Name:    "token_file",
			Status:  doctorStatusFail,
			Message: "token 文件内容无效",
			Details: map[string]any{
				"path":  resolvedPath,
				"error": err.Error(),
			},
		}
	}
	expired := token.IsExpired(now.UTC())
	if expired {
		return doctorCheck{
			Name:    "token_file",
			Status:  doctorStatusWarn,
			Message: "token 可能已过期，建议重新执行登录",
			Details: map[string]any{
				"path":       resolvedPath,
				"found":      true,
				"expired":    true,
				"expires_at": strings.TrimSpace(token.ExpiresAt),
			},
		}
	}
	return doctorCheck{
		Name:    "token_file",
		Status:  doctorStatusPass,
		Message: "token 文件有效",
		Details: map[string]any{
			"path":       resolvedPath,
			"found":      true,
			"expired":    false,
			"expires_at": strings.TrimSpace(token.ExpiresAt),
		},
	}
}

func checkDoctorGRPCConfig(protocol string, config grpcClientSecurityConfig) doctorCheck {
	if err := validateRunGRPCConfig(protocol, config); err != nil {
		return doctorCheck{
			Name:    "grpc_config",
			Status:  doctorStatusFail,
			Message: "gRPC 参数组合不合法",
			Details: map[string]any{
				"error": err.Error(),
			},
		}
	}
	return doctorCheck{
		Name:    "grpc_config",
		Status:  doctorStatusPass,
		Message: "gRPC 参数组合合法",
		Details: map[string]any{
			"protocol": protocol,
		},
	}
}

func checkDoctorEndpoint(protocol, endpoint string) (doctorCheck, *doctorEndpointProbe) {
	switch protocol {
	case "grpc":
		parsed, err := parseGRPCEndpoint(endpoint)
		if err != nil {
			return doctorCheck{
				Name:    "endpoint_parse",
				Status:  doctorStatusFail,
				Message: "gRPC endpoint 解析失败",
				Details: map[string]any{
					"protocol": protocol,
					"endpoint": endpoint,
					"error":    err.Error(),
				},
			}, nil
		}
		return doctorCheck{
				Name:    "endpoint_parse",
				Status:  doctorStatusPass,
				Message: "gRPC endpoint 解析成功",
				Details: map[string]any{
					"protocol":      protocol,
					"endpoint":      endpoint,
					"target":        parsed.target,
					"uses_grpcs":    parsed.usesGRPCSScheme,
					"normalized_to": parsed.target,
				},
			}, &doctorEndpointProbe{
				Protocol: protocol,
				Target:   parsed.target,
			}
	case "http":
		parsedURL, target, err := parseHTTPEndpoint(endpoint)
		if err != nil {
			return doctorCheck{
				Name:    "endpoint_parse",
				Status:  doctorStatusFail,
				Message: "HTTP endpoint 解析失败",
				Details: map[string]any{
					"protocol": protocol,
					"endpoint": endpoint,
					"error":    err.Error(),
				},
			}, nil
		}
		return doctorCheck{
				Name:    "endpoint_parse",
				Status:  doctorStatusPass,
				Message: "HTTP endpoint 解析成功",
				Details: map[string]any{
					"protocol": protocol,
					"endpoint": endpoint,
					"target":   target,
					"scheme":   parsedURL.Scheme,
					"path":     parsedURL.EscapedPath(),
				},
			}, &doctorEndpointProbe{
				Protocol: protocol,
				Target:   target,
				HTTPURL:  parsedURL,
			}
	default:
		return doctorCheck{
			Name:    "endpoint_parse",
			Status:  doctorStatusFail,
			Message: "不支持的 protocol",
			Details: map[string]any{
				"protocol": protocol,
			},
		}, nil
	}
}

func parseHTTPEndpoint(raw string) (*url.URL, string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, "", fmt.Errorf("endpoint 不能为空")
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return nil, "", fmt.Errorf("解析 endpoint 失败: %w", err)
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme != "http" && scheme != "https" {
		return nil, "", fmt.Errorf("HTTP endpoint 仅支持 http:// 或 https://")
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return nil, "", fmt.Errorf("HTTP endpoint 缺少主机信息")
	}
	if parsed.User != nil {
		return nil, "", fmt.Errorf("HTTP endpoint 不支持用户信息")
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" {
		return nil, "", fmt.Errorf("HTTP endpoint 缺少主机信息")
	}
	port := strings.TrimSpace(parsed.Port())
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	} else {
		portNum, convErr := strconv.Atoi(port)
		if convErr != nil || portNum < 1 || portNum > 65535 {
			return nil, "", fmt.Errorf("HTTP endpoint 端口非法")
		}
	}
	parsed.Scheme = scheme
	parsed.Host = net.JoinHostPort(host, port)
	if strings.TrimSpace(parsed.Path) == "" {
		parsed.Path = "/"
	}
	return parsed, parsed.Host, nil
}

func checkDoctorConnectivity(timeout time.Duration, endpointCheck doctorCheck, probe *doctorEndpointProbe) doctorCheck {
	if probe == nil || endpointCheck.Status != doctorStatusPass {
		return doctorCheck{
			Name:    "endpoint_connectivity",
			Status:  doctorStatusFail,
			Message: "未执行连通性探测：endpoint 解析失败",
			Details: map[string]any{
				"depends_on": "endpoint_parse",
			},
		}
	}
	switch probe.Protocol {
	case "grpc":
		return checkTCPConnectivity("endpoint_connectivity", probe.Target, timeout, "gRPC endpoint TCP 连通")
	case "http":
		return checkHTTPConnectivity(probe, timeout)
	default:
		return doctorCheck{
			Name:    "endpoint_connectivity",
			Status:  doctorStatusFail,
			Message: "未执行连通性探测：不支持的 protocol",
			Details: map[string]any{
				"protocol": probe.Protocol,
			},
		}
	}
}

func checkTCPConnectivity(checkName, target string, timeout time.Duration, successMessage string) doctorCheck {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", target)
	if err != nil {
		return doctorCheck{
			Name:    checkName,
			Status:  doctorStatusFail,
			Message: "TCP 连通性探测失败",
			Details: map[string]any{
				"target":     target,
				"timeout_ms": timeout.Milliseconds(),
				"error":      err.Error(),
			},
		}
	}
	_ = conn.Close()
	return doctorCheck{
		Name:    checkName,
		Status:  doctorStatusPass,
		Message: successMessage,
		Details: map[string]any{
			"target":     target,
			"timeout_ms": timeout.Milliseconds(),
		},
	}
}

func checkHTTPConnectivity(probe *doctorEndpointProbe, timeout time.Duration) doctorCheck {
	tcpCheck := checkTCPConnectivity("endpoint_connectivity", probe.Target, timeout, "HTTP endpoint TCP 连通")
	if tcpCheck.Status != doctorStatusPass {
		return tcpCheck
	}

	probeURL := doctorHTTPProbeURL(probe.HTTPURL).String()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		return doctorCheck{
			Name:    "endpoint_connectivity",
			Status:  doctorStatusFail,
			Message: "构建 HTTP 探测请求失败",
			Details: map[string]any{
				"target":     probe.Target,
				"probe_url":  probeURL,
				"timeout_ms": timeout.Milliseconds(),
				"error":      err.Error(),
			},
		}
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return doctorCheck{
			Name:    "endpoint_connectivity",
			Status:  doctorStatusFail,
			Message: "HTTP 探测失败",
			Details: map[string]any{
				"target":     probe.Target,
				"probe_url":  probeURL,
				"timeout_ms": timeout.Milliseconds(),
				"error":      err.Error(),
			},
		}
	}
	defer resp.Body.Close()

	details := map[string]any{
		"target":      probe.Target,
		"probe_url":   probeURL,
		"status_code": resp.StatusCode,
		"timeout_ms":  timeout.Milliseconds(),
	}
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 400:
		return doctorCheck{
			Name:    "endpoint_connectivity",
			Status:  doctorStatusPass,
			Message: "HTTP 健康探测通过",
			Details: details,
		}
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return doctorCheck{
			Name:    "endpoint_connectivity",
			Status:  doctorStatusWarn,
			Message: "HTTP 已连通，但健康探测返回 4xx",
			Details: details,
		}
	default:
		return doctorCheck{
			Name:    "endpoint_connectivity",
			Status:  doctorStatusFail,
			Message: "HTTP 已连通，但健康探测返回 5xx",
			Details: details,
		}
	}
}

func doctorHTTPProbeURL(endpoint *url.URL) *url.URL {
	probeURL := *endpoint
	probeURL.RawQuery = ""
	probeURL.Fragment = ""
	if strings.TrimSpace(probeURL.Path) == "" || probeURL.Path == "/" {
		probeURL.Path = "/health"
	}
	return &probeURL
}

func configCommand(args []string) int {
	if len(args) < 1 {
		printConfigUsage()
		return 2
	}

	switch args[0] {
	case "pull":
		return configPullCommand(args[1:])
	case "activate":
		return configActivateCommand(args[1:])
	case "rollback":
		return configRollbackCommand(args[1:])
	case "watch":
		return configWatchCommand(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "未知 config 子命令: %s\n", args[0])
		printConfigUsage()
		return 2
	}
}

func configPullCommand(args []string) int {
	fs := flag.NewFlagSet("config pull", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	gateway := fs.String("gateway", "", "control-plane 地址（不含路径；默认取 AGENT_GATEWAY_URL 或内置默认值）")
	packageID := fs.String("package-id", "", "配置包 ID")
	tokenFile := fs.String("token-file", defaultTokenFilePath(), "本地 token 文件路径")
	configDir := fs.String("config-dir", "", "本地配置目录（默认 ~/.agentledger/config，可由 AGENT_CONFIG_DIR 覆盖）")
	timeout := fs.Duration("timeout", 10*time.Second, "请求超时时间")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*packageID) == "" {
		fmt.Fprintln(os.Stderr, "package-id 不能为空")
		return 2
	}
	if *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "timeout 必须 > 0")
		return 2
	}

	baseURL, err := resolveControlPlaneBaseURL(*gateway, isFlagProvided(fs, "gateway"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway 参数错误: %v\n", err)
		return 2
	}
	resolvedConfigDir, err := resolveAgentConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config-dir 参数错误: %v\n", err)
		return 2
	}
	authHeader, err := loadStatusAuthHeader(*tokenFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取本地 token 失败: %v\n", err)
		return 1
	}

	pkg, err := pullConfigPackage(
		baseURL,
		strings.TrimSpace(*packageID),
		authHeader,
		resolvedConfigDir,
		*timeout,
	)
	if err != nil {
		if resolvedConfigDir != "" {
			_, _ = writeConfigRuntimeState(resolvedConfigDir, localConfigRuntimeState{
				LastPullError: err.Error(),
			})
		}
		fmt.Fprintf(os.Stderr, "拉取配置包失败: %v\n", err)
		return 1
	}
	currentState, _ := readConfigRuntimeState(resolvedConfigDir)
	_, _ = writeConfigRuntimeState(resolvedConfigDir, localConfigRuntimeState{
		ActivePackageID:     currentState.ActivePackageID,
		PreviousPackageID:   currentState.PreviousPackageID,
		ActivatedAt:         currentState.ActivatedAt,
		LastPulledPackageID: pkg.PackageID,
		LastPulledVersion:   pkg.Version,
		LastPullAt:          pkg.PulledAt,
		RollbackAvailable:   currentState.RollbackAvailable,
	})

	fmt.Printf(
		"配置包已拉取: package_id=%s version=%s signature_status=%s dir=%s\n",
		pkg.PackageID,
		pkg.Version,
		pkg.SignatureStatus,
		resolvedConfigDir,
	)
	return 0
}

func configRollbackCommand(args []string) int {
	fs := flag.NewFlagSet("config rollback", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configDir := fs.String("config-dir", "", "本地配置目录（默认 ~/.agentledger/config，可由 AGENT_CONFIG_DIR 覆盖）")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	resolvedConfigDir, err := resolveAgentConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config-dir 参数错误: %v\n", err)
		return 2
	}
	state, err := readConfigRuntimeState(resolvedConfigDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取配置状态失败: %v\n", err)
		return 1
	}
	if strings.TrimSpace(state.PreviousPackageID) == "" {
		fmt.Fprintln(os.Stderr, "没有可回滚的配置包")
		return 1
	}
	pkg, err := loadLocalConfigPackageByID(resolvedConfigDir, state.PreviousPackageID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取回滚配置包失败: %v\n", err)
		return 1
	}
	activation, err := writeActiveConfigSelection(resolvedConfigDir, localConfigActivation{
		PackageID:   pkg.PackageID,
		ActivatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "回滚配置包失败: %v\n", err)
		return 1
	}
	_, _ = writeConfigRuntimeState(resolvedConfigDir, localConfigRuntimeState{
		ActivePackageID:     pkg.PackageID,
		PreviousPackageID:   state.ActivePackageID,
		ActivatedAt:         activation.ActivatedAt,
		LastPulledPackageID: state.LastPulledPackageID,
		LastPulledVersion:   state.LastPulledVersion,
		LastPullAt:          state.LastPullAt,
		LastPullError:       state.LastPullError,
		RollbackAvailable:   strings.TrimSpace(state.ActivePackageID) != "",
	})
	fmt.Printf("配置包已回滚: package_id=%s activated_at=%s\n", pkg.PackageID, activation.ActivatedAt)
	return 0
}

func configWatchCommand(args []string) int {
	fs := flag.NewFlagSet("config watch", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	gateway := fs.String("gateway", "", "control-plane 地址（默认取 AGENT_GATEWAY_URL 或内置默认值）")
	tokenFile := fs.String("token-file", defaultTokenFilePath(), "本地 token 文件路径")
	configDir := fs.String("config-dir", "", "本地配置目录")
	timeout := fs.Duration("timeout", 10*time.Second, "请求超时时间")
	interval := fs.Duration("interval", defaultConfigWatchInterval, "轮询间隔")
	iterations := fs.Int("iterations", 1, "轮询次数，0 表示持续运行")
	autoActivate := fs.Bool("auto-activate", false, "拉取后自动激活")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *timeout <= 0 || *interval <= 0 || *iterations < 0 {
		fmt.Fprintln(os.Stderr, "timeout/interval 必须 > 0，iterations 必须 >= 0")
		return 2
	}
	baseURL, err := resolveControlPlaneBaseURL(*gateway, isFlagProvided(fs, "gateway"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway 参数错误: %v\n", err)
		return 2
	}
	resolvedConfigDir, err := resolveAgentConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config-dir 参数错误: %v\n", err)
		return 2
	}
	authHeader, err := loadStatusAuthHeader(*tokenFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取本地 token 失败: %v\n", err)
		return 1
	}
	runOnce := func() error {
		pkg, err := fetchLatestConfigPackage(baseURL, authHeader, *timeout)
		if err != nil {
			_, _ = writeConfigRuntimeState(resolvedConfigDir, localConfigRuntimeState{
				LastPullError: err.Error(),
			})
			return err
		}
		currentState, _ := readConfigRuntimeState(resolvedConfigDir)
		if currentState.LastPulledPackageID == pkg.PackageID {
			fmt.Printf("未发现新配置包: package_id=%s version=%s\n", pkg.PackageID, pkg.Version)
			return nil
		}
		if _, err := writeLocalConfigPackage(resolvedConfigDir, *pkg); err != nil {
			return err
		}
		nextState := localConfigRuntimeState{
			ActivePackageID:     currentState.ActivePackageID,
			PreviousPackageID:   currentState.PreviousPackageID,
			ActivatedAt:         currentState.ActivatedAt,
			LastPulledPackageID: pkg.PackageID,
			LastPulledVersion:   pkg.Version,
			LastPullAt:          time.Now().UTC().Format(time.RFC3339),
			RollbackAvailable:   currentState.RollbackAvailable,
		}
		if *autoActivate {
			activation, err := writeActiveConfigSelection(resolvedConfigDir, localConfigActivation{
				PackageID:   pkg.PackageID,
				ActivatedAt: time.Now().UTC().Format(time.RFC3339),
			})
			if err != nil {
				return err
			}
			nextState.PreviousPackageID = currentState.ActivePackageID
			nextState.ActivePackageID = pkg.PackageID
			nextState.ActivatedAt = activation.ActivatedAt
			nextState.RollbackAvailable = strings.TrimSpace(currentState.ActivePackageID) != ""
		}
		if _, err := writeConfigRuntimeState(resolvedConfigDir, nextState); err != nil {
			return err
		}
		fmt.Printf("配置包已同步: package_id=%s version=%s auto_activate=%t\n", pkg.PackageID, pkg.Version, *autoActivate)
		return nil
	}
	if *iterations == 0 {
		for {
			if err := runOnce(); err != nil {
				fmt.Fprintf(os.Stderr, "watch 拉取失败: %v\n", err)
			}
			time.Sleep(*interval)
		}
	}
	for index := 0; index < *iterations; index++ {
		if err := runOnce(); err != nil {
			fmt.Fprintf(os.Stderr, "watch 拉取失败: %v\n", err)
			return 1
		}
		if index < *iterations-1 {
			time.Sleep(*interval)
		}
	}
	return 0
}

func updateCommand(args []string) int {
	if len(args) < 1 {
		printUpdateUsage()
		return 2
	}

	switch args[0] {
	case "check":
		return updateCheckCommand(args[1:])
	case "download":
		return updateDownloadCommand(args[1:])
	case "apply":
		return updateApplyCommand(args[1:])
	case "rollback":
		return updateRollbackCommand(args[1:])
	case "status":
		return updateStatusCommand(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "未知 update 子命令: %s\n", args[0])
		printUpdateUsage()
		return 2
	}
}

type agentUpdateCheckResponse struct {
	CheckedAt        string                  `json:"checkedAt"`
	CurrentVersion   string                  `json:"currentVersion"`
	Channel          string                  `json:"channel"`
	OS               string                  `json:"os"`
	Arch             string                  `json:"arch"`
	UpdateAvailable  bool                    `json:"updateAvailable"`
	Comparison       string                  `json:"comparison"`
	LatestRelease    *agentReleaseDescriptor `json:"latestRelease"`
	SelectedArtifact *agentReleaseArtifact   `json:"selectedArtifact"`
	Instructions     string                  `json:"instructions"`
}

type agentReleaseDescriptor struct {
	ReleaseID   string                 `json:"releaseId"`
	TenantID    string                 `json:"tenantId"`
	Version     string                 `json:"version"`
	Channel     string                 `json:"channel"`
	Notes       string                 `json:"notes,omitempty"`
	PublishedAt string                 `json:"publishedAt"`
	Artifacts   []agentReleaseArtifact `json:"artifacts"`
	CreatedAt   string                 `json:"createdAt"`
	UpdatedAt   string                 `json:"updatedAt"`
}

type agentReleaseArtifact struct {
	OS                 string `json:"os"`
	Arch               string `json:"arch"`
	DownloadURL        string `json:"downloadUrl"`
	ChecksumSHA256     string `json:"checksumSha256,omitempty"`
	Signature          string `json:"signature,omitempty"`
	SignatureAlgorithm string `json:"signatureAlgorithm,omitempty"`
	RolloutRing        string `json:"rolloutRing,omitempty"`
	RolloutPercentage  int    `json:"rolloutPercentage,omitempty"`
	MinAgentVersion    string `json:"minAgentVersion,omitempty"`
	FileName           string `json:"fileName,omitempty"`
	InstallHint        string `json:"installHint,omitempty"`
}

type agentUpdateCheckReport struct {
	Component        string                  `json:"component"`
	CheckedAt        string                  `json:"checked_at"`
	Gateway          string                  `json:"gateway"`
	CurrentVersion   string                  `json:"current_version"`
	Channel          string                  `json:"channel"`
	OS               string                  `json:"os"`
	Arch             string                  `json:"arch"`
	UpdateAvailable  bool                    `json:"update_available"`
	Comparison       string                  `json:"comparison"`
	LatestRelease    *agentReleaseDescriptor `json:"latest_release,omitempty"`
	SelectedArtifact *agentReleaseArtifact   `json:"selected_artifact,omitempty"`
	Mode             string                  `json:"mode"`
	Instructions     string                  `json:"instructions"`
}

type lifecycleReportOptions struct {
	Enabled   bool
	BaseURL   string
	TokenFile string
	TenantID  string
	AgentID   string
	DeviceID  string
	Action    string
	Result    string
	Version   string
	Timeout   time.Duration
	Metadata  map[string]any
}

func currentHostname() string {
	host, _ := os.Hostname()
	if strings.TrimSpace(host) == "" {
		return "local-host"
	}
	return host
}

func defaultLifecycleAgentID() string {
	return fmt.Sprintf("%s-agent", currentHostname())
}

func maybeReportLifecycleEvent(options lifecycleReportOptions) error {
	if !options.Enabled {
		return nil
	}
	if strings.TrimSpace(options.BaseURL) == "" {
		return fmt.Errorf("gateway 不能为空")
	}
	if strings.TrimSpace(options.TenantID) == "" {
		return fmt.Errorf("tenant-id 不能为空")
	}
	if strings.TrimSpace(options.AgentID) == "" {
		return fmt.Errorf("agent-id 不能为空")
	}
	if options.Timeout <= 0 {
		return fmt.Errorf("timeout 必须 > 0")
	}

	authHeader, err := loadStatusAuthHeader(options.TokenFile)
	if err != nil {
		return fmt.Errorf("读取本地 token 失败: %w", err)
	}

	payload := map[string]any{
		"tenantId":   strings.TrimSpace(options.TenantID),
		"agentId":    strings.TrimSpace(options.AgentID),
		"deviceId":   strings.TrimSpace(options.DeviceID),
		"hostname":   currentHostname(),
		"version":    strings.TrimSpace(options.Version),
		"action":     strings.TrimSpace(options.Action),
		"result":     strings.TrimSpace(options.Result),
		"occurredAt": time.Now().UTC().Format(time.RFC3339),
		"metadata":   options.Metadata,
	}
	if options.Metadata == nil {
		payload["metadata"] = map[string]any{}
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("序列化生命周期事件失败: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), options.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(options.BaseURL, "/")+"/api/v1/agents/lifecycle-events",
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("构建生命周期事件请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if strings.TrimSpace(authHeader) != "" {
		req.Header.Set("Authorization", authHeader)
	}

	resp, err := (&http.Client{Timeout: options.Timeout}).Do(req)
	if err != nil {
		return fmt.Errorf("发送生命周期事件失败: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取生命周期事件响应失败: %w", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf(
			"生命周期事件响应异常（HTTP %d）: %s",
			resp.StatusCode,
			summarizeResponseBody(responseBody),
		)
	}
	return nil
}

func updateCheckCommand(args []string) int {
	fs := flag.NewFlagSet("update check", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	gateway := fs.String("gateway", "", "control-plane 地址（不含路径；默认取 AGENT_GATEWAY_URL 或内置默认值）")
	tokenFile := fs.String("token-file", defaultTokenFilePath(), "本地 token 文件路径")
	timeout := fs.Duration("timeout", 10*time.Second, "请求超时时间")
	channel := fs.String("channel", "", "release 渠道（默认取 AGENT_RELEASE_CHANNEL 或 stable）")
	currentVersion := fs.String("current-version", version, "当前 agent 版本号")
	targetOS := fs.String("os", runtime.GOOS, "目标操作系统")
	targetArch := fs.String("arch", runtime.GOARCH, "目标架构")
	agentID := fs.String("agent-id", defaultLifecycleAgentID(), "升级检查使用的 agent ID")
	deviceID := fs.String("device-id", "", "升级检查使用的 device ID")
	hostname := fs.String("hostname", currentHostname(), "升级检查使用的 hostname")
	ring := fs.String("ring", "stable", "升级检查使用的 rollout ring")
	reportLifecycle := fs.Bool("report-lifecycle", false, "向 control-plane 上报 update-check 生命周期事件")
	tenantID := fs.String("tenant-id", "", "生命周期上报使用的 tenant ID")
	reportTimeout := fs.Duration("report-timeout", 5*time.Second, "生命周期上报超时时间")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "timeout 必须 > 0")
		return 2
	}
	if strings.TrimSpace(*currentVersion) == "" {
		fmt.Fprintln(os.Stderr, "current-version 不能为空")
		return 2
	}
	if strings.TrimSpace(*targetOS) == "" {
		fmt.Fprintln(os.Stderr, "os 不能为空")
		return 2
	}
	if strings.TrimSpace(*targetArch) == "" {
		fmt.Fprintln(os.Stderr, "arch 不能为空")
		return 2
	}
	if strings.TrimSpace(*agentID) == "" {
		fmt.Fprintln(os.Stderr, "agent-id 不能为空")
		return 2
	}
	if strings.TrimSpace(*hostname) == "" {
		fmt.Fprintln(os.Stderr, "hostname 不能为空")
		return 2
	}
	if strings.TrimSpace(*ring) == "" {
		fmt.Fprintln(os.Stderr, "ring 不能为空")
		return 2
	}
	if *reportLifecycle {
		if strings.TrimSpace(*tenantID) == "" {
			fmt.Fprintln(os.Stderr, "report-lifecycle 启用时 tenant-id 不能为空")
			return 2
		}
		if *reportTimeout <= 0 {
			fmt.Fprintln(os.Stderr, "report-timeout 必须 > 0")
			return 2
		}
	}

	baseURL, err := resolveControlPlaneBaseURL(*gateway, isFlagProvided(fs, "gateway"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway 参数错误: %v\n", err)
		return 2
	}
	resolvedChannel, err := resolveAgentReleaseChannel(*channel, isFlagProvided(fs, "channel"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "channel 参数错误: %v\n", err)
		return 2
	}
	authHeader, err := loadStatusAuthHeader(*tokenFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取本地 token 失败: %v\n", err)
		return 1
	}

	response, err := fetchAgentUpdateCheck(
		baseURL,
		strings.TrimSpace(*currentVersion),
		resolvedChannel,
		strings.TrimSpace(*targetOS),
		strings.TrimSpace(*targetArch),
		strings.TrimSpace(*agentID),
		strings.TrimSpace(*deviceID),
		strings.TrimSpace(*hostname),
		strings.ToLower(strings.TrimSpace(*ring)),
		authHeader,
		*timeout,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "检查更新失败: %v\n", err)
		return 1
	}

	report := agentUpdateCheckReport{
		Component:        "agent-cli",
		CheckedAt:        response.CheckedAt,
		Gateway:          baseURL,
		CurrentVersion:   response.CurrentVersion,
		Channel:          response.Channel,
		OS:               response.OS,
		Arch:             response.Arch,
		UpdateAvailable:  response.UpdateAvailable,
		Comparison:       response.Comparison,
		LatestRelease:    response.LatestRelease,
		SelectedArtifact: response.SelectedArtifact,
		Mode:             "manual_only",
		Instructions:     response.Instructions,
	}

	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "update output marshal failed: %v\n", err)
		return 1
	}
	fmt.Println(string(output))
	latestVersion := ""
	if report.LatestRelease != nil {
		latestVersion = report.LatestRelease.Version
	}
	resultStatus := "warn"
	if report.UpdateAvailable {
		resultStatus = "success"
	}
	if err := maybeReportLifecycleEvent(lifecycleReportOptions{
		Enabled:   *reportLifecycle,
		BaseURL:   baseURL,
		TokenFile: *tokenFile,
		TenantID:  *tenantID,
		AgentID:   *agentID,
		DeviceID:  *deviceID,
		Action:    "upgrade",
		Result:    resultStatus,
		Version:   version,
		Timeout:   *reportTimeout,
		Metadata: map[string]any{
			"command":         "update check",
			"channel":         report.Channel,
			"os":              report.OS,
			"arch":            report.Arch,
			"comparison":      report.Comparison,
			"currentVersion":  report.CurrentVersion,
			"latestVersion":   latestVersion,
			"updateAvailable": report.UpdateAvailable,
			"ring":            strings.ToLower(strings.TrimSpace(*ring)),
			"agentId":         strings.TrimSpace(*agentID),
			"deviceId":        strings.TrimSpace(*deviceID),
			"hostname":        strings.TrimSpace(*hostname),
		},
	}); err != nil {
		fmt.Fprintf(os.Stderr, "警告: 生命周期事件上报失败: %v\n", err)
	}
	return 0
}

func updateDownloadCommand(args []string) int {
	fs := flag.NewFlagSet("update download", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	gateway := fs.String("gateway", "", "control-plane 地址")
	tokenFile := fs.String("token-file", defaultTokenFilePath(), "本地 token 文件路径")
	configDir := fs.String("config-dir", "", "本地配置目录")
	timeout := fs.Duration("timeout", 30*time.Second, "请求超时时间")
	channel := fs.String("channel", "", "release 渠道")
	currentVersion := fs.String("current-version", version, "当前 agent 版本号")
	targetOS := fs.String("os", runtime.GOOS, "目标操作系统")
	targetArch := fs.String("arch", runtime.GOARCH, "目标架构")
	agentID := fs.String("agent-id", defaultLifecycleAgentID(), "升级下载使用的 agent ID")
	deviceID := fs.String("device-id", "", "升级下载使用的 device ID")
	hostname := fs.String("hostname", currentHostname(), "升级下载使用的 hostname")
	ring := fs.String("ring", "stable", "升级下载使用的 rollout ring")
	signingPublicKeyFile := fs.String("signature-public-key-file", "", "升级工件签名校验公钥文件（PEM；默认取 AGENT_RELEASE_SIGNING_PUBLIC_KEY_FILE）")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "timeout 必须 > 0")
		return 2
	}
	if strings.TrimSpace(*agentID) == "" {
		fmt.Fprintln(os.Stderr, "agent-id 不能为空")
		return 2
	}
	if strings.TrimSpace(*hostname) == "" {
		fmt.Fprintln(os.Stderr, "hostname 不能为空")
		return 2
	}
	if strings.TrimSpace(*ring) == "" {
		fmt.Fprintln(os.Stderr, "ring 不能为空")
		return 2
	}
	baseURL, err := resolveControlPlaneBaseURL(*gateway, isFlagProvided(fs, "gateway"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway 参数错误: %v\n", err)
		return 2
	}
	resolvedConfigDir, err := resolveAgentConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config-dir 参数错误: %v\n", err)
		return 2
	}
	authHeader, err := loadStatusAuthHeader(*tokenFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取本地 token 失败: %v\n", err)
		return 1
	}
	resolvedChannel, err := resolveAgentReleaseChannel(*channel, isFlagProvided(fs, "channel"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "channel 参数错误: %v\n", err)
		return 2
	}
	resolvedSigningPublicKeyFile, err := resolveAgentReleaseSigningPublicKeyPath(
		*signingPublicKeyFile,
		isFlagProvided(fs, "signature-public-key-file"),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "signature-public-key-file 参数错误: %v\n", err)
		return 2
	}
	response, err := fetchAgentUpdateCheck(
		baseURL,
		strings.TrimSpace(*currentVersion),
		resolvedChannel,
		strings.TrimSpace(*targetOS),
		strings.TrimSpace(*targetArch),
		strings.TrimSpace(*agentID),
		strings.TrimSpace(*deviceID),
		strings.TrimSpace(*hostname),
		strings.ToLower(strings.TrimSpace(*ring)),
		authHeader,
		*timeout,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "检查更新失败: %v\n", err)
		return 1
	}
	if !response.UpdateAvailable || response.SelectedArtifact == nil || response.LatestRelease == nil {
		fmt.Fprintln(os.Stderr, "当前没有可下载的新版本")
		return 1
	}
	state, _ := readUpdateState(resolvedConfigDir)
	state.DownloadAttempts++
	state.CurrentVersion = strings.TrimSpace(*currentVersion)
	state.Channel = resolvedChannel
	state.OS = strings.TrimSpace(*targetOS)
	state.Arch = strings.TrimSpace(*targetArch)
	state.DownloadedReleaseID = response.LatestRelease.ReleaseID
	state.DownloadedVersion = response.LatestRelease.Version
	state.RolloutRing = strings.TrimSpace(response.SelectedArtifact.RolloutRing)
	state.RolloutPercentage = response.SelectedArtifact.RolloutPercentage
	state.MinAgentVersion = strings.TrimSpace(response.SelectedArtifact.MinAgentVersion)
	state.LastApplyError = ""
	downloadedArtifact, err := downloadAgentReleaseArtifact(
		resolvedConfigDir,
		response.SelectedArtifact,
		resolvedSigningPublicKeyFile,
		*timeout,
	)
	if err != nil {
		state.LastDownloadError = err.Error()
		state.LastDownloadErrorCode = classifyUpdateDownloadError(err)
		_, _ = writeUpdateState(resolvedConfigDir, state)
		fmt.Fprintf(os.Stderr, "下载升级工件失败: %v\n", err)
		return 1
	}
	state.DownloadedAt = time.Now().UTC().Format(time.RFC3339)
	state.DownloadedArtifactPath = downloadedArtifact.Path
	state.DownloadedChecksumSHA256 = downloadedArtifact.ChecksumSHA256
	state.DownloadedSignatureStatus = downloadedArtifact.SignatureStatus
	state.DownloadedSignatureAlgorithm = downloadedArtifact.SignatureAlgorithm
	state.DownloadedSignerFingerprint = downloadedArtifact.SignerFingerprint
	state.LastDownloadError = ""
	state.LastDownloadErrorCode = ""
	if _, err := writeUpdateState(resolvedConfigDir, state); err != nil {
		fmt.Fprintf(os.Stderr, "写入升级状态失败: %v\n", err)
		return 1
	}
	fmt.Printf(
		"升级工件已下载: release_id=%s version=%s path=%s checksum=%s signature_status=%s\n",
		state.DownloadedReleaseID,
		state.DownloadedVersion,
		downloadedArtifact.Path,
		downloadedArtifact.ChecksumSHA256,
		state.DownloadedSignatureStatus,
	)
	return 0
}

func updateApplyCommand(args []string) int {
	fs := flag.NewFlagSet("update apply", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configDir := fs.String("config-dir", "", "本地配置目录")
	binaryPath := fs.String("binary-path", "", "当前 agent 二进制路径（默认当前可执行文件）")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	resolvedConfigDir, err := resolveAgentConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config-dir 参数错误: %v\n", err)
		return 2
	}
	state, err := readUpdateState(resolvedConfigDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取升级状态失败: %v\n", err)
		return 1
	}
	if strings.TrimSpace(state.DownloadedArtifactPath) == "" {
		fmt.Fprintln(os.Stderr, "当前没有已下载工件")
		return 1
	}
	resolvedBinaryPath, err := resolveAgentBinaryPath(*binaryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "binary-path 参数错误: %v\n", err)
		return 2
	}
	executableBytes, err := readExecutablePayload(state.DownloadedArtifactPath)
	if err != nil {
		state.LastApplyError = err.Error()
		_, _ = writeUpdateState(resolvedConfigDir, state)
		fmt.Fprintf(os.Stderr, "解析升级工件失败: %v\n", err)
		return 1
	}
	backupPath, err := backupCurrentBinary(resolvedConfigDir, resolvedBinaryPath)
	if err != nil {
		state.LastApplyError = err.Error()
		_, _ = writeUpdateState(resolvedConfigDir, state)
		fmt.Fprintf(os.Stderr, "备份当前二进制失败: %v\n", err)
		return 1
	}
	if err := writeExecutableFile(resolvedBinaryPath, executableBytes); err != nil {
		state.LastApplyError = err.Error()
		state.BackupBinaryPath = backupPath
		_, _ = writeUpdateState(resolvedConfigDir, state)
		fmt.Fprintf(os.Stderr, "应用升级失败: %v\n", err)
		return 1
	}
	now := time.Now().UTC().Format(time.RFC3339)
	state.AppliedReleaseID = state.DownloadedReleaseID
	state.AppliedVersion = state.DownloadedVersion
	state.AppliedAt = now
	state.RollbackReleaseID = state.CurrentVersion
	state.RollbackVersion = state.CurrentVersion
	state.BackupBinaryPath = backupPath
	state.CurrentVersion = state.DownloadedVersion
	state.RolledBackAt = ""
	state.LastApplyError = ""
	if _, err := writeUpdateState(resolvedConfigDir, state); err != nil {
		fmt.Fprintf(os.Stderr, "写入升级状态失败: %v\n", err)
		return 1
	}
	fmt.Printf("升级已应用: release_id=%s version=%s binary=%s backup=%s\n", state.AppliedReleaseID, state.AppliedVersion, resolvedBinaryPath, backupPath)
	return 0
}

func updateRollbackCommand(args []string) int {
	fs := flag.NewFlagSet("update rollback", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configDir := fs.String("config-dir", "", "本地配置目录")
	binaryPath := fs.String("binary-path", "", "当前 agent 二进制路径（默认当前可执行文件）")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	resolvedConfigDir, err := resolveAgentConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config-dir 参数错误: %v\n", err)
		return 2
	}
	state, err := readUpdateState(resolvedConfigDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取升级状态失败: %v\n", err)
		return 1
	}
	if strings.TrimSpace(state.BackupBinaryPath) == "" {
		fmt.Fprintln(os.Stderr, "当前没有可回滚的二进制备份")
		return 1
	}
	resolvedBinaryPath, err := resolveAgentBinaryPath(*binaryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "binary-path 参数错误: %v\n", err)
		return 2
	}
	backupBody, err := os.ReadFile(state.BackupBinaryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取回滚备份失败: %v\n", err)
		return 1
	}
	if err := writeExecutableFile(resolvedBinaryPath, backupBody); err != nil {
		fmt.Fprintf(os.Stderr, "回滚升级失败: %v\n", err)
		return 1
	}
	state.RolledBackAt = time.Now().UTC().Format(time.RFC3339)
	if strings.TrimSpace(state.RollbackVersion) != "" {
		state.CurrentVersion = state.RollbackVersion
	}
	state.LastApplyError = ""
	if _, err := writeUpdateState(resolvedConfigDir, state); err != nil {
		fmt.Fprintf(os.Stderr, "写入升级状态失败: %v\n", err)
		return 1
	}
	fmt.Printf("升级已回滚: version=%s binary=%s\n", state.CurrentVersion, resolvedBinaryPath)
	return 0
}

func updateStatusCommand(args []string) int {
	fs := flag.NewFlagSet("update status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configDir := fs.String("config-dir", "", "本地配置目录")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	resolvedConfigDir, err := resolveAgentConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config-dir 参数错误: %v\n", err)
		return 2
	}
	state, err := readUpdateState(resolvedConfigDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			state = localUpdateState{}
		} else {
			fmt.Fprintf(os.Stderr, "读取升级状态失败: %v\n", err)
			return 1
		}
	}
	statusSnapshot, err := readStatusUpdate(resolvedConfigDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取升级状态失败: %v\n", err)
		return 1
	}
	payload := struct {
		Component string           `json:"component"`
		Status    string           `json:"status"`
		Update    localUpdateState `json:"update"`
	}{
		Component: "agent-cli",
		Status:    statusSnapshot.Status,
		Update:    state,
	}
	body, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "status output marshal failed: %v\n", err)
		return 1
	}
	fmt.Println(string(body))
	return 0
}

func configActivateCommand(args []string) int {
	fs := flag.NewFlagSet("config activate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	packageID := fs.String("package-id", "", "配置包 ID")
	configDir := fs.String("config-dir", "", "本地配置目录（默认 ~/.agentledger/config，可由 AGENT_CONFIG_DIR 覆盖）")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*packageID) == "" {
		fmt.Fprintln(os.Stderr, "package-id 不能为空")
		return 2
	}

	resolvedConfigDir, err := resolveAgentConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config-dir 参数错误: %v\n", err)
		return 2
	}

	pkg, err := loadLocalConfigPackageByID(resolvedConfigDir, strings.TrimSpace(*packageID))
	if err != nil {
		fmt.Fprintf(os.Stderr, "激活配置包失败: %v\n", err)
		return 1
	}
	activation, err := writeActiveConfigSelection(
		resolvedConfigDir,
		localConfigActivation{
			PackageID:   pkg.PackageID,
			ActivatedAt: time.Now().UTC().Format(time.RFC3339),
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "激活配置包失败: %v\n", err)
		return 1
	}

	fmt.Printf(
		"配置包已激活: package_id=%s version=%s signature_status=%s activated_at=%s\n",
		pkg.PackageID,
		pkg.Version,
		pkg.SignatureStatus,
		activation.ActivatedAt,
	)
	return 0
}

type agentStatusReport struct {
	Component        string                    `json:"component"`
	ObservedAt       string                    `json:"observed_at"`
	Version          string                    `json:"version"`
	Commit           string                    `json:"commit"`
	BuildTime        string                    `json:"build_time"`
	Protocol         string                    `json:"protocol"`
	Endpoint         string                    `json:"endpoint"`
	DefaultEndpoints agentStatusDefaultTargets `json:"default_endpoints"`
	Token            agentStatusToken          `json:"token"`
	Queue            agentStatusQueue          `json:"queue"`
	Config           agentStatusConfig         `json:"config"`
	Update           agentStatusUpdate         `json:"update"`
	ConfigPackage    *agentStatusConfigPackage `json:"config_package,omitempty"`
}

type agentStatusDefaultTargets struct {
	HTTP string `json:"http"`
	GRPC string `json:"grpc"`
}

type agentStatusToken struct {
	Path            string `json:"path"`
	Found           bool   `json:"found"`
	Status          string `json:"status"`
	TokenType       string `json:"token_type,omitempty"`
	ExpiresAt       string `json:"expires_at,omitempty"`
	ObtainedAt      string `json:"obtained_at,omitempty"`
	HasRefreshToken bool   `json:"has_refresh_token,omitempty"`
	Error           string `json:"error,omitempty"`
}

type agentStatusConfigPackage struct {
	PackageID       string `json:"package_id,omitempty"`
	Path            string `json:"path"`
	Version         string `json:"version"`
	IssuedAt        string `json:"issued_at,omitempty"`
	SignatureStatus string `json:"signature_status"`
}

type agentStatusConfig struct {
	Dir               string `json:"dir"`
	Status            string `json:"status"`
	ActivePackageID   string `json:"active_package_id,omitempty"`
	PreviousPackageID string `json:"previous_package_id,omitempty"`
	Version           string `json:"version,omitempty"`
	IssuedAt          string `json:"issued_at,omitempty"`
	SignatureStatus   string `json:"signature_status,omitempty"`
	ActivatedAt       string `json:"activated_at,omitempty"`
	LastPullAt        string `json:"last_pull_at,omitempty"`
	LastPullError     string `json:"last_pull_error,omitempty"`
	RollbackAvailable bool   `json:"rollback_available,omitempty"`
	PackagePath       string `json:"package_path,omitempty"`
}

type agentStatusUpdate struct {
	Dir                          string `json:"dir"`
	Status                       string `json:"status"`
	CurrentVersion               string `json:"current_version,omitempty"`
	DownloadAttempts             int    `json:"download_attempts,omitempty"`
	DownloadedReleaseID          string `json:"downloaded_release_id,omitempty"`
	DownloadedVersion            string `json:"downloaded_version,omitempty"`
	DownloadedAt                 string `json:"downloaded_at,omitempty"`
	DownloadedSignatureStatus    string `json:"downloaded_signature_status,omitempty"`
	DownloadedSignatureAlgorithm string `json:"downloaded_signature_algorithm,omitempty"`
	DownloadedSignerFingerprint  string `json:"downloaded_signer_fingerprint,omitempty"`
	LastDownloadError            string `json:"last_download_error,omitempty"`
	LastDownloadErrorCode        string `json:"last_download_error_code,omitempty"`
	RolloutRing                  string `json:"rollout_ring,omitempty"`
	RolloutPercentage            int    `json:"rollout_percentage,omitempty"`
	MinAgentVersion              string `json:"min_agent_version,omitempty"`
	AppliedReleaseID             string `json:"applied_release_id,omitempty"`
	AppliedVersion               string `json:"applied_version,omitempty"`
	AppliedAt                    string `json:"applied_at,omitempty"`
	LastApplyError               string `json:"last_apply_error,omitempty"`
	RollbackReleaseID            string `json:"rollback_release_id,omitempty"`
	RollbackVersion              string `json:"rollback_version,omitempty"`
	RolledBackAt                 string `json:"rolled_back_at,omitempty"`
}

type agentStatusQueue struct {
	Enabled          bool   `json:"enabled"`
	Path             string `json:"path,omitempty"`
	PendingCount     int    `json:"pending_count"`
	OldestEnqueuedAt string `json:"oldest_enqueued_at,omitempty"`
	TotalBytes       int64  `json:"total_bytes"`
}

type agentQueueEntry struct {
	ID         string             `json:"id"`
	EnqueuedAt string             `json:"enqueued_at"`
	Request    ingestBatchRequest `json:"request"`
}

type agentQueueFile struct {
	Path      string
	SizeBytes int64
	Entry     agentQueueEntry
}

type agentQueueSendResult struct {
	EntryID    string
	StatusCode int
	Body       []byte
}

type agentQueueFlushResult struct {
	FlushedCount    int
	CurrentResponse *agentQueueSendResult
}

type localConfigPackage struct {
	PackageID       string                 `json:"package_id"`
	TenantID        string                 `json:"tenant_id,omitempty"`
	Version         string                 `json:"version"`
	IssuedAt        string                 `json:"issued_at,omitempty"`
	SignatureStatus string                 `json:"signature_status"`
	Payload         map[string]any         `json:"payload"`
	CreatedAt       string                 `json:"created_at,omitempty"`
	UpdatedAt       string                 `json:"updated_at,omitempty"`
	PulledAt        string                 `json:"pulled_at,omitempty"`
	SourceURL       string                 `json:"source_url,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

type localConfigActivation struct {
	PackageID   string `json:"package_id"`
	ActivatedAt string `json:"activated_at"`
}

type localConfigRuntimeState struct {
	ActivePackageID     string `json:"active_package_id,omitempty"`
	PreviousPackageID   string `json:"previous_package_id,omitempty"`
	ActivatedAt         string `json:"activated_at,omitempty"`
	LastPulledPackageID string `json:"last_pulled_package_id,omitempty"`
	LastPulledVersion   string `json:"last_pulled_version,omitempty"`
	LastPullAt          string `json:"last_pull_at,omitempty"`
	LastPullError       string `json:"last_pull_error,omitempty"`
	RollbackAvailable   bool   `json:"rollback_available"`
}

type localUpdateState struct {
	CurrentVersion               string `json:"current_version,omitempty"`
	DownloadAttempts             int    `json:"download_attempts,omitempty"`
	DownloadedReleaseID          string `json:"downloaded_release_id,omitempty"`
	DownloadedVersion            string `json:"downloaded_version,omitempty"`
	DownloadedAt                 string `json:"downloaded_at,omitempty"`
	DownloadedArtifactPath       string `json:"downloaded_artifact_path,omitempty"`
	DownloadedChecksumSHA256     string `json:"downloaded_checksum_sha256,omitempty"`
	DownloadedSignatureStatus    string `json:"downloaded_signature_status,omitempty"`
	DownloadedSignatureAlgorithm string `json:"downloaded_signature_algorithm,omitempty"`
	DownloadedSignerFingerprint  string `json:"downloaded_signer_fingerprint,omitempty"`
	LastDownloadError            string `json:"last_download_error,omitempty"`
	LastDownloadErrorCode        string `json:"last_download_error_code,omitempty"`
	RolloutRing                  string `json:"rollout_ring,omitempty"`
	RolloutPercentage            int    `json:"rollout_percentage,omitempty"`
	MinAgentVersion              string `json:"min_agent_version,omitempty"`
	AppliedReleaseID             string `json:"applied_release_id,omitempty"`
	AppliedVersion               string `json:"applied_version,omitempty"`
	AppliedAt                    string `json:"applied_at,omitempty"`
	LastApplyError               string `json:"last_apply_error,omitempty"`
	BackupBinaryPath             string `json:"backup_binary_path,omitempty"`
	RollbackReleaseID            string `json:"rollback_release_id,omitempty"`
	RollbackVersion              string `json:"rollback_version,omitempty"`
	RolledBackAt                 string `json:"rolled_back_at,omitempty"`
	Channel                      string `json:"channel,omitempty"`
	OS                           string `json:"os,omitempty"`
	Arch                         string `json:"arch,omitempty"`
}

func statusCommand(args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	protocol := fs.String("protocol", "http", "观测协议：http|grpc")
	endpoint := fs.String("endpoint", "", "ingestion-gateway 地址（未显式指定时按协议自动选择）")
	tokenFile := fs.String("token-file", defaultTokenFilePath(), "本地 token 文件路径")
	queueDir := fs.String("queue-dir", "", "本地持久队列目录（可选；未指定且未设置 AGENT_QUEUE_DIR 时关闭）")
	configDir := fs.String("config-dir", "", "本地配置目录（默认 ~/.agentledger/config，可由 AGENT_CONFIG_DIR 覆盖）")
	configFile := fs.String("config-file", "", "本地配置包 JSON 文件路径（可选）")
	gateway := fs.String("gateway", "", "control-plane 地址（用于可选生命周期上报）")
	reportLifecycle := fs.Bool("report-lifecycle", false, "向 control-plane 上报 status 生命周期事件")
	tenantID := fs.String("tenant-id", "", "生命周期上报使用的 tenant ID")
	agentID := fs.String("agent-id", defaultLifecycleAgentID(), "生命周期上报使用的 agent ID")
	deviceID := fs.String("device-id", "", "生命周期上报使用的 device ID")
	reportTimeout := fs.Duration("report-timeout", 5*time.Second, "生命周期上报超时时间")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	endpointExplicit := isFlagProvided(fs, "endpoint")
	if endpointExplicit && strings.TrimSpace(*endpoint) == "" {
		fmt.Fprintln(os.Stderr, "endpoint 不能为空")
		return 2
	}

	protocolName := strings.ToLower(strings.TrimSpace(*protocol))
	if protocolName != "http" && protocolName != "grpc" {
		fmt.Fprintln(os.Stderr, "protocol 仅支持 http 或 grpc")
		return 2
	}
	if *reportLifecycle {
		if strings.TrimSpace(*tenantID) == "" {
			fmt.Fprintln(os.Stderr, "report-lifecycle 启用时 tenant-id 不能为空")
			return 2
		}
		if *reportTimeout <= 0 {
			fmt.Fprintln(os.Stderr, "report-timeout 必须 > 0")
			return 2
		}
	}
	queueStatus, err := readStatusQueue(*queueDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取队列状态失败: %v\n", err)
		return 1
	}
	configStatus, err := readStatusManagedConfig(*configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取本地配置状态失败: %v\n", err)
		return 1
	}
	updateStatus, err := readStatusUpdate(*configDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取本地升级状态失败: %v\n", err)
		return 1
	}

	tokenStatus, err := readStatusToken(*tokenFile, time.Now().UTC())
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取本地 token 状态失败: %v\n", err)
		return 1
	}

	configPackage, err := readStatusConfigPackage(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取配置包失败: %v\n", err)
		return 1
	}

	report := agentStatusReport{
		Component:  "agent-cli",
		ObservedAt: time.Now().UTC().Format(time.RFC3339),
		Version:    version,
		Commit:     commit,
		BuildTime:  buildTime,
		Protocol:   protocolName,
		Endpoint:   resolveEndpoint(*endpoint, protocolName, endpointExplicit),
		DefaultEndpoints: agentStatusDefaultTargets{
			HTTP: defaultHTTPEndpoint,
			GRPC: defaultGRPCEndpoint,
		},
		Token:         tokenStatus,
		Queue:         queueStatus,
		Config:        configStatus,
		Update:        updateStatus,
		ConfigPackage: configPackage,
	}

	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "status output marshal failed: %v\n", err)
		return 1
	}

	fmt.Println(string(output))
	if *reportLifecycle {
		baseURL, err := resolveControlPlaneBaseURL(*gateway, isFlagProvided(fs, "gateway"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "警告: 生命周期事件上报跳过，gateway 参数错误: %v\n", err)
		} else if err := maybeReportLifecycleEvent(lifecycleReportOptions{
			Enabled:   true,
			BaseURL:   baseURL,
			TokenFile: *tokenFile,
			TenantID:  *tenantID,
			AgentID:   *agentID,
			DeviceID:  *deviceID,
			Action:    "status",
			Result:    "success",
			Version:   version,
			Timeout:   *reportTimeout,
			Metadata: map[string]any{
				"command":         "status",
				"protocol":        protocolName,
				"endpoint":        report.Endpoint,
				"queuePending":    report.Queue.PendingCount,
				"configStatus":    report.Config.Status,
				"updateStatus":    report.Update.Status,
				"tokenStatus":     report.Token.Status,
				"activePackageId": report.Config.ActivePackageID,
			},
		}); err != nil {
			fmt.Fprintf(os.Stderr, "警告: 生命周期事件上报失败: %v\n", err)
		}
	}
	return 0
}

func readStatusToken(tokenFile string, now time.Time) (agentStatusToken, error) {
	resolvedPath, err := resolveTokenFilePath(tokenFile)
	if err != nil {
		return agentStatusToken{}, err
	}

	report := agentStatusToken{
		Path:   resolvedPath,
		Found:  false,
		Status: "missing",
	}

	info, err := os.Stat(resolvedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return report, nil
		}
		return agentStatusToken{}, err
	}
	if info.IsDir() {
		report.Found = true
		report.Status = "invalid"
		report.Error = "token 路径指向目录"
		return report, nil
	}

	token, _, err := loadLocalToken(resolvedPath)
	if err != nil {
		report.Found = true
		report.Status = "invalid"
		report.Error = err.Error()
		return report, nil
	}

	report.Found = true
	report.TokenType = strings.TrimSpace(token.TokenType)
	report.ExpiresAt = strings.TrimSpace(token.ExpiresAt)
	report.ObtainedAt = strings.TrimSpace(token.ObtainedAt)
	report.HasRefreshToken = strings.TrimSpace(token.RefreshToken) != ""
	if token.IsExpired(now.UTC()) {
		report.Status = "expired"
		return report, nil
	}
	report.Status = "valid"
	return report, nil
}

func readStatusConfigPackage(configFile string) (*agentStatusConfigPackage, error) {
	trimmedPath := strings.TrimSpace(configFile)
	if trimmedPath == "" {
		return nil, nil
	}

	expandedPath, err := expandPath(trimmedPath)
	if err != nil {
		return nil, fmt.Errorf("解析配置包路径失败: %w", err)
	}
	resolvedPath, err := filepath.Abs(expandedPath)
	if err != nil {
		return nil, fmt.Errorf("解析配置包绝对路径失败: %w", err)
	}

	info, err := os.Stat(resolvedPath)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("配置包路径指向目录")
	}

	content, err := os.ReadFile(resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置包文件失败: %w", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(content, &payload); err != nil {
		return nil, fmt.Errorf("解析配置包 JSON 失败: %w", err)
	}

	versionValue := firstStringValue(
		payload["version"],
		payload["config_version"],
		payload["configVersion"],
	)
	if versionValue == "" {
		return nil, fmt.Errorf("配置包缺少 version/config_version")
	}

	issuedAtValue := firstStringValue(
		payload["issued_at"],
		payload["issuedAt"],
	)
	if issuedAtValue != "" {
		if _, err := time.Parse(time.RFC3339, issuedAtValue); err != nil {
			return nil, fmt.Errorf("配置包 issued_at 非法: %w", err)
		}
	}

	signatureStatus, err := normalizeSignatureStatus(
		firstStringValue(
			payload["signature_status"],
			payload["signatureStatus"],
		),
	)
	if err != nil {
		return nil, err
	}

	return &agentStatusConfigPackage{
		PackageID:       firstStringValue(payload["package_id"], payload["packageId"]),
		Path:            resolvedPath,
		Version:         versionValue,
		IssuedAt:        issuedAtValue,
		SignatureStatus: signatureStatus,
	}, nil
}

func readStatusManagedConfig(rawConfigDir string) (agentStatusConfig, error) {
	resolvedDir, err := resolveAgentConfigDir(rawConfigDir)
	if err != nil {
		return agentStatusConfig{}, err
	}

	status := agentStatusConfig{
		Dir:    resolvedDir,
		Status: "inactive",
	}
	runtimeState, runtimeErr := readConfigRuntimeState(resolvedDir)
	if runtimeErr == nil {
		status.ActivePackageID = runtimeState.ActivePackageID
		status.PreviousPackageID = runtimeState.PreviousPackageID
		status.ActivatedAt = runtimeState.ActivatedAt
		status.LastPullAt = runtimeState.LastPullAt
		status.LastPullError = runtimeState.LastPullError
		status.RollbackAvailable = runtimeState.RollbackAvailable
	}
	activation, err := readActiveConfigSelection(resolvedDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return status, nil
		}
		return agentStatusConfig{}, err
	}
	pkg, err := loadLocalConfigPackageByID(resolvedDir, activation.PackageID)
	if err != nil {
		status.Status = "broken"
		status.ActivePackageID = activation.PackageID
		status.ActivatedAt = activation.ActivatedAt
		return status, nil
	}

	status.Status = "active"
	status.ActivePackageID = pkg.PackageID
	status.Version = pkg.Version
	status.IssuedAt = pkg.IssuedAt
	status.SignatureStatus = pkg.SignatureStatus
	status.ActivatedAt = activation.ActivatedAt
	status.PackagePath = configPackageFilePath(resolvedDir, pkg.PackageID)
	return status, nil
}

func readStatusUpdate(rawConfigDir string) (agentStatusUpdate, error) {
	resolvedDir, err := resolveAgentConfigDir(rawConfigDir)
	if err != nil {
		return agentStatusUpdate{}, err
	}
	status := agentStatusUpdate{
		Dir:    resolvedDir,
		Status: "idle",
	}
	state, err := readUpdateState(resolvedDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return status, nil
		}
		return agentStatusUpdate{}, err
	}
	status.CurrentVersion = state.CurrentVersion
	status.DownloadAttempts = state.DownloadAttempts
	status.DownloadedReleaseID = state.DownloadedReleaseID
	status.DownloadedVersion = state.DownloadedVersion
	status.DownloadedAt = state.DownloadedAt
	status.DownloadedSignatureStatus = state.DownloadedSignatureStatus
	status.DownloadedSignatureAlgorithm = state.DownloadedSignatureAlgorithm
	status.DownloadedSignerFingerprint = state.DownloadedSignerFingerprint
	status.LastDownloadError = state.LastDownloadError
	status.LastDownloadErrorCode = state.LastDownloadErrorCode
	status.RolloutRing = state.RolloutRing
	status.RolloutPercentage = state.RolloutPercentage
	status.MinAgentVersion = state.MinAgentVersion
	status.AppliedReleaseID = state.AppliedReleaseID
	status.AppliedVersion = state.AppliedVersion
	status.AppliedAt = state.AppliedAt
	status.LastApplyError = state.LastApplyError
	status.RollbackReleaseID = state.RollbackReleaseID
	status.RollbackVersion = state.RollbackVersion
	status.RolledBackAt = state.RolledBackAt
	switch {
	case strings.TrimSpace(state.LastDownloadError) != "":
		status.Status = "failed"
	case strings.TrimSpace(state.LastApplyError) != "":
		status.Status = "failed"
	case strings.TrimSpace(state.RolledBackAt) != "":
		status.Status = "rolled_back"
	case strings.TrimSpace(state.AppliedReleaseID) != "":
		status.Status = "applied"
	case strings.TrimSpace(state.DownloadedReleaseID) != "":
		status.Status = "downloaded"
	}
	return status, nil
}

func resolveAgentConfigDir(rawConfigDir string) (string, error) {
	target := strings.TrimSpace(rawConfigDir)
	if target == "" {
		target = strings.TrimSpace(os.Getenv(agentConfigDirEnv))
	}
	if target == "" {
		target = "~/.agentledger/config"
	}
	expandedPath, err := expandPath(target)
	if err != nil {
		return "", fmt.Errorf("解析配置目录失败: %w", err)
	}
	resolvedPath, err := filepath.Abs(expandedPath)
	if err != nil {
		return "", fmt.Errorf("解析配置目录绝对路径失败: %w", err)
	}
	return resolvedPath, nil
}

func configPackagesDir(configDir string) string {
	return filepath.Join(configDir, "packages")
}

func configPackageFilePath(configDir, packageID string) string {
	return filepath.Join(configPackagesDir(configDir), packageID+".json")
}

func activeConfigSelectionPath(configDir string) string {
	return filepath.Join(configDir, "active.json")
}

func configRuntimeStatePath(configDir string) string {
	return filepath.Join(configDir, "config-state.json")
}

func updateStatePath(configDir string) string {
	return filepath.Join(configDir, "update-state.json")
}

func updateArtifactsDir(configDir string) string {
	return filepath.Join(configDir, "updates", "artifacts")
}

func updateBackupsDir(configDir string) string {
	return filepath.Join(configDir, "updates", "backups")
}

func ensureAgentConfigLayout(configDir string) error {
	if err := os.MkdirAll(configPackagesDir(configDir), 0o700); err != nil {
		return fmt.Errorf("创建配置目录失败: %w", err)
	}
	if err := os.MkdirAll(updateArtifactsDir(configDir), 0o700); err != nil {
		return fmt.Errorf("创建更新工件目录失败: %w", err)
	}
	if err := os.MkdirAll(updateBackupsDir(configDir), 0o700); err != nil {
		return fmt.Errorf("创建更新备份目录失败: %w", err)
	}
	return nil
}

func loadStatusAuthHeader(tokenFile string) (string, error) {
	token, _, err := loadLocalToken(tokenFile)
	if err != nil {
		return "", err
	}
	return token.AuthHeader(), nil
}

func resolveControlPlaneBaseURL(rawGateway string, explicit bool) (string, error) {
	target := strings.TrimSpace(rawGateway)
	if target == "" && !explicit {
		target = strings.TrimSpace(os.Getenv(agentGatewayURLEnv))
	}
	if target == "" {
		target = defaultGatewayURL
	}
	return normalizeGatewayBaseURL(target)
}

func fetchAgentRuntimeConfig(
	baseURL string,
	authHeader string,
	agentID string,
	timeout time.Duration,
) (*agentRuntimeConfigResponse, error) {
	normalizedAgentID := strings.TrimSpace(agentID)
	if normalizedAgentID == "" {
		return nil, fmt.Errorf("agentId 不能为空")
	}
	endpoint := fmt.Sprintf(
		"%s/api/v1/system/config/agent-runtime?agentId=%s",
		strings.TrimRight(baseURL, "/"),
		url.QueryEscape(normalizedAgentID),
	)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("构建 runtime-config 请求失败: %w", err)
	}
	if strings.TrimSpace(authHeader) != "" {
		req.Header.Set("Authorization", authHeader)
	}

	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求 runtime-config 失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取 runtime-config 响应失败: %w", err)
	}
	if resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("runtime-config 返回错误（HTTP %d）: %s", resp.StatusCode, summarizeResponseBody(body))
	}

	var response agentRuntimeConfigResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("解析 runtime-config 响应失败: %w", err)
	}
	return &response, nil
}

func postAgentHeartbeat(
	baseURL string,
	authHeader string,
	timeout time.Duration,
	payload agentHeartbeatRequest,
) error {
	endpoint := strings.TrimRight(baseURL, "/") + "/api/v1/system/config/agent-heartbeat"
	statusCode, body, err := sendJSONRequest(context.Background(), endpoint, payload, authHeader, timeout)
	if err != nil {
		return fmt.Errorf("请求 agent-heartbeat 失败: %w", err)
	}
	if statusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("agent-heartbeat 返回错误（HTTP %d）: %s", statusCode, summarizeResponseBody(body))
	}
	return nil
}

func resolveAgentReleaseChannel(rawChannel string, explicit bool) (string, error) {
	target := strings.TrimSpace(rawChannel)
	if target == "" && !explicit {
		target = strings.TrimSpace(os.Getenv(agentReleaseChannelEnv))
	}
	if target == "" {
		target = "stable"
	}
	target = strings.ToLower(target)
	switch target {
	case "stable", "beta", "canary":
		return target, nil
	default:
		return "", fmt.Errorf("仅支持 stable/beta/canary")
	}
}

func resolveAgentReleaseSigningPublicKeyPath(rawPath string, explicit bool) (string, error) {
	target := strings.TrimSpace(rawPath)
	if target == "" && !explicit {
		target = strings.TrimSpace(os.Getenv(agentReleasePublicKeyEnv))
	}
	if target == "" {
		return "", nil
	}
	expandedPath, err := expandPath(target)
	if err != nil {
		return "", fmt.Errorf("解析公钥路径失败: %w", err)
	}
	resolvedPath, err := filepath.Abs(expandedPath)
	if err != nil {
		return "", fmt.Errorf("解析公钥绝对路径失败: %w", err)
	}
	return resolvedPath, nil
}

func fetchAgentUpdateCheck(
	baseURL string,
	currentVersion string,
	channel string,
	targetOS string,
	targetArch string,
	agentID string,
	deviceID string,
	hostname string,
	ring string,
	authHeader string,
	timeout time.Duration,
) (*agentUpdateCheckResponse, error) {
	query := url.Values{}
	query.Set("currentVersion", currentVersion)
	query.Set("channel", channel)
	query.Set("os", targetOS)
	query.Set("arch", targetArch)
	if strings.TrimSpace(agentID) != "" {
		query.Set("agentId", agentID)
	}
	if strings.TrimSpace(deviceID) != "" {
		query.Set("deviceId", deviceID)
	}
	if strings.TrimSpace(hostname) != "" {
		query.Set("hostname", hostname)
	}
	if strings.TrimSpace(ring) != "" {
		query.Set("ring", ring)
	}
	endpoint := fmt.Sprintf(
		"%s/api/v1/system/agent-releases/check?%s",
		strings.TrimRight(baseURL, "/"),
		query.Encode(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("构建请求失败: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if strings.TrimSpace(authHeader) != "" {
		req.Header.Set("Authorization", authHeader)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf(
			"服务端返回错误（HTTP %d）: %s",
			resp.StatusCode,
			summarizeResponseBody(body),
		)
	}

	var response agentUpdateCheckResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("解析更新检查响应失败: %w", err)
	}
	if strings.TrimSpace(response.CheckedAt) == "" {
		return nil, fmt.Errorf("更新检查响应缺少 checkedAt")
	}
	if strings.TrimSpace(response.CurrentVersion) == "" {
		return nil, fmt.Errorf("更新检查响应缺少 currentVersion")
	}
	if strings.TrimSpace(response.Channel) == "" {
		return nil, fmt.Errorf("更新检查响应缺少 channel")
	}
	if strings.TrimSpace(response.OS) == "" || strings.TrimSpace(response.Arch) == "" {
		return nil, fmt.Errorf("更新检查响应缺少 os/arch")
	}
	if strings.TrimSpace(response.Comparison) == "" {
		return nil, fmt.Errorf("更新检查响应缺少 comparison")
	}
	if strings.TrimSpace(response.Instructions) == "" {
		return nil, fmt.Errorf("更新检查响应缺少 instructions")
	}
	return &response, nil
}

func fetchConfigPackage(
	baseURL string,
	packageID string,
	authHeader string,
	timeout time.Duration,
) (*localConfigPackage, error) {
	endpoint := fmt.Sprintf(
		"%s/api/v1/system/config/packages/%s",
		strings.TrimRight(baseURL, "/"),
		url.PathEscape(packageID),
	)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("构建请求失败: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if strings.TrimSpace(authHeader) != "" {
		req.Header.Set("Authorization", authHeader)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf(
			"服务端返回错误（HTTP %d）: %s",
			resp.StatusCode,
			summarizeResponseBody(body),
		)
	}

	var response struct {
		PackageID       string         `json:"packageId"`
		TenantID        string         `json:"tenantId"`
		Version         string         `json:"version"`
		IssuedAt        string         `json:"issuedAt"`
		SignatureStatus string         `json:"signatureStatus"`
		Payload         map[string]any `json:"payload"`
		CreatedAt       string         `json:"createdAt"`
		UpdatedAt       string         `json:"updatedAt"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("解析配置包响应失败: %w", err)
	}
	if strings.TrimSpace(response.PackageID) == "" {
		return nil, fmt.Errorf("配置包响应缺少 package_id")
	}
	if strings.TrimSpace(response.Version) == "" {
		return nil, fmt.Errorf("配置包响应缺少 version")
	}
	signatureStatus, err := normalizeSignatureStatus(response.SignatureStatus)
	if err != nil {
		return nil, err
	}
	pkg := &localConfigPackage{
		PackageID:       strings.TrimSpace(response.PackageID),
		TenantID:        strings.TrimSpace(response.TenantID),
		Version:         strings.TrimSpace(response.Version),
		IssuedAt:        strings.TrimSpace(response.IssuedAt),
		SignatureStatus: signatureStatus,
		Payload:         response.Payload,
		CreatedAt:       strings.TrimSpace(response.CreatedAt),
		UpdatedAt:       strings.TrimSpace(response.UpdatedAt),
	}
	if pkg.Payload == nil {
		pkg.Payload = map[string]any{}
	}
	return pkg, nil
}

func fetchLatestConfigPackage(
	baseURL string,
	authHeader string,
	timeout time.Duration,
) (*localConfigPackage, error) {
	endpoint := fmt.Sprintf(
		"%s/api/v1/system/config/packages?limit=1",
		strings.TrimRight(baseURL, "/"),
	)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("构建请求失败: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if strings.TrimSpace(authHeader) != "" {
		req.Header.Set("Authorization", authHeader)
	}

	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("服务端返回错误（HTTP %d）: %s", resp.StatusCode, summarizeResponseBody(body))
	}
	var response struct {
		Items []struct {
			PackageID       string         `json:"packageId"`
			TenantID        string         `json:"tenantId"`
			Version         string         `json:"version"`
			IssuedAt        string         `json:"issuedAt"`
			SignatureStatus string         `json:"signatureStatus"`
			Payload         map[string]any `json:"payload"`
			CreatedAt       string         `json:"createdAt"`
			UpdatedAt       string         `json:"updatedAt"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("解析配置包列表失败: %w", err)
	}
	if len(response.Items) == 0 {
		return nil, fmt.Errorf("当前没有可用配置包")
	}
	item := response.Items[0]
	signatureStatus, err := normalizeSignatureStatus(item.SignatureStatus)
	if err != nil {
		return nil, err
	}
	return &localConfigPackage{
		PackageID:       strings.TrimSpace(item.PackageID),
		TenantID:        strings.TrimSpace(item.TenantID),
		Version:         strings.TrimSpace(item.Version),
		IssuedAt:        strings.TrimSpace(item.IssuedAt),
		SignatureStatus: signatureStatus,
		Payload:         item.Payload,
		CreatedAt:       strings.TrimSpace(item.CreatedAt),
		UpdatedAt:       strings.TrimSpace(item.UpdatedAt),
	}, nil
}

func writeLocalConfigPackage(
	configDir string,
	pkg localConfigPackage,
) (string, error) {
	if err := ensureAgentConfigLayout(configDir); err != nil {
		return "", err
	}
	if strings.TrimSpace(pkg.PackageID) == "" {
		return "", fmt.Errorf("配置包缺少 package_id")
	}
	if strings.TrimSpace(pkg.Version) == "" {
		return "", fmt.Errorf("配置包缺少 version")
	}
	signatureStatus, err := normalizeSignatureStatus(pkg.SignatureStatus)
	if err != nil {
		return "", err
	}
	pkg.SignatureStatus = signatureStatus
	if pkg.Payload == nil {
		pkg.Payload = map[string]any{}
	}
	targetPath := configPackageFilePath(configDir, pkg.PackageID)
	body, err := json.MarshalIndent(pkg, "", "  ")
	if err != nil {
		return "", fmt.Errorf("序列化配置包失败: %w", err)
	}
	body = append(body, '\n')
	tempFile := targetPath + ".tmp"
	if err := os.WriteFile(tempFile, body, 0o600); err != nil {
		return "", fmt.Errorf("写入配置包临时文件失败: %w", err)
	}
	if err := os.Rename(tempFile, targetPath); err != nil {
		_ = os.Remove(tempFile)
		return "", fmt.Errorf("保存配置包文件失败: %w", err)
	}
	return targetPath, nil
}

func loadLocalConfigPackageByID(
	configDir string,
	packageID string,
) (*localConfigPackage, error) {
	path := configPackageFilePath(configDir, strings.TrimSpace(packageID))
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pkg localConfigPackage
	if err := json.Unmarshal(content, &pkg); err != nil {
		return nil, fmt.Errorf("解析本地配置包失败: %w", err)
	}
	if strings.TrimSpace(pkg.PackageID) == "" {
		return nil, fmt.Errorf("本地配置包缺少 package_id")
	}
	if strings.TrimSpace(pkg.Version) == "" {
		return nil, fmt.Errorf("本地配置包缺少 version")
	}
	signatureStatus, err := normalizeSignatureStatus(pkg.SignatureStatus)
	if err != nil {
		return nil, err
	}
	pkg.SignatureStatus = signatureStatus
	if pkg.Payload == nil {
		pkg.Payload = map[string]any{}
	}
	return &pkg, nil
}

func writeActiveConfigSelection(
	configDir string,
	activation localConfigActivation,
) (localConfigActivation, error) {
	if err := ensureAgentConfigLayout(configDir); err != nil {
		return localConfigActivation{}, err
	}
	if strings.TrimSpace(activation.PackageID) == "" {
		return localConfigActivation{}, fmt.Errorf("package_id 不能为空")
	}
	targetPath := activeConfigSelectionPath(configDir)
	body, err := json.MarshalIndent(activation, "", "  ")
	if err != nil {
		return localConfigActivation{}, fmt.Errorf("序列化激活状态失败: %w", err)
	}
	body = append(body, '\n')
	tempFile := targetPath + ".tmp"
	if err := os.WriteFile(tempFile, body, 0o600); err != nil {
		return localConfigActivation{}, fmt.Errorf("写入激活状态临时文件失败: %w", err)
	}
	if err := os.Rename(tempFile, targetPath); err != nil {
		_ = os.Remove(tempFile)
		return localConfigActivation{}, fmt.Errorf("保存激活状态文件失败: %w", err)
	}
	return activation, nil
}

func readActiveConfigSelection(configDir string) (localConfigActivation, error) {
	content, err := os.ReadFile(activeConfigSelectionPath(configDir))
	if err != nil {
		return localConfigActivation{}, err
	}
	var activation localConfigActivation
	if err := json.Unmarshal(content, &activation); err != nil {
		return localConfigActivation{}, fmt.Errorf("解析激活状态失败: %w", err)
	}
	if strings.TrimSpace(activation.PackageID) == "" {
		return localConfigActivation{}, fmt.Errorf("激活状态缺少 package_id")
	}
	return activation, nil
}

func writeConfigRuntimeState(configDir string, state localConfigRuntimeState) (localConfigRuntimeState, error) {
	if err := ensureAgentConfigLayout(configDir); err != nil {
		return localConfigRuntimeState{}, err
	}
	body, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return localConfigRuntimeState{}, fmt.Errorf("序列化配置状态失败: %w", err)
	}
	body = append(body, '\n')
	targetPath := configRuntimeStatePath(configDir)
	tempFile := targetPath + ".tmp"
	if err := os.WriteFile(tempFile, body, 0o600); err != nil {
		return localConfigRuntimeState{}, fmt.Errorf("写入配置状态临时文件失败: %w", err)
	}
	if err := os.Rename(tempFile, targetPath); err != nil {
		_ = os.Remove(tempFile)
		return localConfigRuntimeState{}, fmt.Errorf("保存配置状态文件失败: %w", err)
	}
	return state, nil
}

func readConfigRuntimeState(configDir string) (localConfigRuntimeState, error) {
	content, err := os.ReadFile(configRuntimeStatePath(configDir))
	if err != nil {
		return localConfigRuntimeState{}, err
	}
	var state localConfigRuntimeState
	if err := json.Unmarshal(content, &state); err != nil {
		return localConfigRuntimeState{}, fmt.Errorf("解析配置状态失败: %w", err)
	}
	return state, nil
}

func writeUpdateState(configDir string, state localUpdateState) (localUpdateState, error) {
	if err := ensureAgentConfigLayout(configDir); err != nil {
		return localUpdateState{}, err
	}
	body, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return localUpdateState{}, fmt.Errorf("序列化升级状态失败: %w", err)
	}
	body = append(body, '\n')
	targetPath := updateStatePath(configDir)
	tempFile := targetPath + ".tmp"
	if err := os.WriteFile(tempFile, body, 0o600); err != nil {
		return localUpdateState{}, fmt.Errorf("写入升级状态临时文件失败: %w", err)
	}
	if err := os.Rename(tempFile, targetPath); err != nil {
		_ = os.Remove(tempFile)
		return localUpdateState{}, fmt.Errorf("保存升级状态文件失败: %w", err)
	}
	return state, nil
}

func readUpdateState(configDir string) (localUpdateState, error) {
	content, err := os.ReadFile(updateStatePath(configDir))
	if err != nil {
		return localUpdateState{}, err
	}
	var state localUpdateState
	if err := json.Unmarshal(content, &state); err != nil {
		return localUpdateState{}, fmt.Errorf("解析升级状态失败: %w", err)
	}
	return state, nil
}

func pullConfigPackage(
	baseURL string,
	packageID string,
	authHeader string,
	configDir string,
	timeout time.Duration,
) (*localConfigPackage, error) {
	pkg, err := fetchConfigPackage(baseURL, packageID, authHeader, timeout)
	if err != nil {
		return nil, err
	}
	pkg.PulledAt = time.Now().UTC().Format(time.RFC3339)
	pkg.SourceURL = strings.TrimRight(baseURL, "/")
	if _, err := writeLocalConfigPackage(configDir, *pkg); err != nil {
		return nil, err
	}
	return pkg, nil
}

func resolveAgentBinaryPath(raw string) (string, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		executablePath, err := os.Executable()
		if err != nil {
			return "", fmt.Errorf("解析当前二进制路径失败: %w", err)
		}
		target = executablePath
	}
	expandedPath, err := expandPath(target)
	if err != nil {
		return "", fmt.Errorf("解析二进制路径失败: %w", err)
	}
	resolvedPath, err := filepath.Abs(expandedPath)
	if err != nil {
		return "", fmt.Errorf("解析二进制绝对路径失败: %w", err)
	}
	return resolvedPath, nil
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return fmt.Sprintf("%x", sum[:])
}

type downloadedReleaseArtifact struct {
	Path               string
	ChecksumSHA256     string
	SignatureStatus    string
	SignatureAlgorithm string
	SignerFingerprint  string
}

func classifyUpdateDownloadError(err error) string {
	if err == nil {
		return ""
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "checksum"):
		return "checksum_mismatch"
	case strings.Contains(message, "签名校验失败"):
		return "signature_invalid"
	case strings.Contains(message, "签名元数据不完整"):
		return "signature_metadata_invalid"
	case strings.Contains(message, "未配置 signature-public-key-file"):
		return "signature_key_missing"
	case strings.Contains(message, "签名公钥"):
		return "signature_key_invalid"
	case strings.Contains(message, "http "):
		return "download_http_error"
	case strings.Contains(message, "读取工件失败"):
		return "download_read_error"
	case strings.Contains(message, "写入工件失败"):
		return "artifact_write_failed"
	default:
		return "download_failed"
	}
}

func downloadAgentReleaseArtifact(
	configDir string,
	artifact *agentReleaseArtifact,
	signingPublicKeyPath string,
	timeout time.Duration,
) (*downloadedReleaseArtifact, error) {
	if artifact == nil || strings.TrimSpace(artifact.DownloadURL) == "" {
		return nil, fmt.Errorf("缺少下载地址")
	}
	if err := ensureAgentConfigLayout(configDir); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, artifact.DownloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("构建下载请求失败: %w", err)
	}
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("下载工件失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("下载工件失败（HTTP %d）: %s", resp.StatusCode, summarizeResponseBody(body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取工件失败: %w", err)
	}
	checksum := sha256Hex(body)
	if strings.TrimSpace(artifact.ChecksumSHA256) != "" && !strings.EqualFold(strings.TrimSpace(artifact.ChecksumSHA256), checksum) {
		return nil, fmt.Errorf("工件 checksum 校验失败")
	}
	signatureStatus, signatureAlgorithm, signerFingerprint, err := verifyAgentReleaseArtifactSignature(artifact, body, signingPublicKeyPath)
	if err != nil {
		return nil, err
	}
	fileName := strings.TrimSpace(artifact.FileName)
	if fileName == "" {
		fileName = pathBaseFromURL(artifact.DownloadURL)
	}
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s.bin", artifact.OS, artifact.Arch, time.Now().UTC().Format("20060102150405"))
	}
	targetDir := filepath.Join(updateArtifactsDir(configDir), fmt.Sprintf("%s-%s", artifact.OS, artifact.Arch))
	if err := os.MkdirAll(targetDir, 0o700); err != nil {
		return nil, fmt.Errorf("创建工件目录失败: %w", err)
	}
	targetPath := filepath.Join(targetDir, fileName)
	if err := os.WriteFile(targetPath, body, 0o600); err != nil {
		return nil, fmt.Errorf("写入工件失败: %w", err)
	}
	return &downloadedReleaseArtifact{
		Path:               targetPath,
		ChecksumSHA256:     checksum,
		SignatureStatus:    signatureStatus,
		SignatureAlgorithm: signatureAlgorithm,
		SignerFingerprint:  signerFingerprint,
	}, nil
}

func verifyAgentReleaseArtifactSignature(
	artifact *agentReleaseArtifact,
	body []byte,
	signingPublicKeyPath string,
) (string, string, string, error) {
	signature := strings.TrimSpace(artifact.Signature)
	signatureAlgorithm := strings.ToLower(strings.TrimSpace(artifact.SignatureAlgorithm))
	if signature == "" && signatureAlgorithm == "" {
		return "unsigned", "", "", nil
	}
	if signature == "" || signatureAlgorithm == "" {
		return "", "", "", fmt.Errorf("工件签名元数据不完整")
	}
	if signatureAlgorithm != "ed25519" {
		return "", "", "", fmt.Errorf("当前仅支持 ed25519 签名校验")
	}
	if strings.TrimSpace(signingPublicKeyPath) == "" {
		return "", "", "", fmt.Errorf("工件包含签名，但未配置 signature-public-key-file/AGENT_RELEASE_SIGNING_PUBLIC_KEY_FILE")
	}
	publicKey, fingerprint, err := loadAgentReleaseSigningPublicKey(signingPublicKeyPath)
	if err != nil {
		return "", "", "", err
	}
	signatureBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return "", "", "", fmt.Errorf("解析工件签名失败: %w", err)
	}
	if !ed25519.Verify(publicKey, body, signatureBytes) {
		return "", "", "", fmt.Errorf("工件签名校验失败")
	}
	return "verified", signatureAlgorithm, fingerprint, nil
}

func loadAgentReleaseSigningPublicKey(path string) (ed25519.PublicKey, string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("读取升级签名公钥失败: %w", err)
	}
	block, _ := pem.Decode(body)
	if block == nil {
		return nil, "", fmt.Errorf("升级签名公钥不是合法 PEM")
	}
	parsedKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, "", fmt.Errorf("解析升级签名公钥失败: %w", err)
	}
	publicKey, ok := parsedKey.(ed25519.PublicKey)
	if !ok {
		return nil, "", fmt.Errorf("升级签名公钥必须是 ed25519")
	}
	return publicKey, sha256Hex(publicKey), nil
}

func pathBaseFromURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return filepath.Base(parsed.Path)
}

func readExecutablePayload(path string) ([]byte, error) {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".zip"):
		return readExecutableFromZip(path)
	case strings.HasSuffix(lower, ".tar.gz"), strings.HasSuffix(lower, ".tgz"):
		return readExecutableFromTarGz(path)
	default:
		return os.ReadFile(path)
	}
}

func readExecutableFromZip(path string) ([]byte, error) {
	reader, err := zip.OpenReader(path)
	if err != nil {
		return nil, fmt.Errorf("打开 zip 工件失败: %w", err)
	}
	defer reader.Close()
	for _, file := range reader.File {
		if file.FileInfo().IsDir() {
			continue
		}
		name := strings.ToLower(filepath.Base(file.Name))
		if name != "agent" && name != "agent.exe" {
			continue
		}
		handle, err := file.Open()
		if err != nil {
			return nil, fmt.Errorf("读取 zip 工件内容失败: %w", err)
		}
		defer handle.Close()
		return io.ReadAll(handle)
	}
	return nil, fmt.Errorf("zip 工件中未找到 agent 可执行文件")
}

func readExecutableFromTarGz(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开 tar.gz 工件失败: %w", err)
	}
	defer file.Close()
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("解析 gzip 工件失败: %w", err)
	}
	defer gzReader.Close()
	tarReader := tar.NewReader(gzReader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取 tar 工件失败: %w", err)
		}
		if header == nil || header.FileInfo().IsDir() {
			continue
		}
		name := strings.ToLower(filepath.Base(header.Name))
		if name != "agent" && name != "agent.exe" {
			continue
		}
		return io.ReadAll(tarReader)
	}
	return nil, fmt.Errorf("tar.gz 工件中未找到 agent 可执行文件")
}

func backupCurrentBinary(configDir, binaryPath string) (string, error) {
	if err := ensureAgentConfigLayout(configDir); err != nil {
		return "", err
	}
	body, err := os.ReadFile(binaryPath)
	if err != nil {
		return "", fmt.Errorf("读取当前二进制失败: %w", err)
	}
	targetPath := filepath.Join(updateBackupsDir(configDir), fmt.Sprintf("agent-%s.bak", time.Now().UTC().Format("20060102150405")))
	if err := os.WriteFile(targetPath, body, 0o700); err != nil {
		return "", fmt.Errorf("写入二进制备份失败: %w", err)
	}
	return targetPath, nil
}

func writeExecutableFile(binaryPath string, body []byte) error {
	info, err := os.Stat(binaryPath)
	mode := fs.FileMode(0o700)
	if err == nil {
		mode = info.Mode()
	}
	if err := os.WriteFile(binaryPath, body, mode); err != nil {
		return fmt.Errorf("写入二进制失败: %w", err)
	}
	return nil
}

func readStatusQueue(rawQueueDir string) (agentStatusQueue, error) {
	resolvedPath, enabled, err := resolveAgentQueueDir(rawQueueDir)
	if err != nil {
		return agentStatusQueue{}, err
	}
	if !enabled {
		return agentStatusQueue{
			Enabled:      false,
			PendingCount: 0,
			TotalBytes:   0,
		}, nil
	}

	files, err := loadAgentQueueFiles(resolvedPath)
	if err != nil {
		return agentStatusQueue{}, err
	}

	queueStatus := agentStatusQueue{
		Enabled:      true,
		Path:         resolvedPath,
		PendingCount: len(files),
		TotalBytes:   0,
	}
	var oldest time.Time
	for _, item := range files {
		queueStatus.TotalBytes += item.SizeBytes
		enqueuedAt, err := time.Parse(time.RFC3339Nano, item.Entry.EnqueuedAt)
		if err != nil {
			return agentStatusQueue{}, fmt.Errorf("解析队列 enqueued_at 失败: %w", err)
		}
		if oldest.IsZero() || enqueuedAt.Before(oldest) {
			oldest = enqueuedAt
		}
	}
	if !oldest.IsZero() {
		queueStatus.OldestEnqueuedAt = oldest.UTC().Format(time.RFC3339)
	}
	return queueStatus, nil
}

func resolveAgentQueueDir(rawQueueDir string) (string, bool, error) {
	target := strings.TrimSpace(rawQueueDir)
	if target == "" {
		target = strings.TrimSpace(os.Getenv(agentQueueDirEnv))
	}
	if target == "" {
		return "", false, nil
	}

	expandedPath, err := expandPath(target)
	if err != nil {
		return "", false, fmt.Errorf("解析队列目录失败: %w", err)
	}
	resolvedPath, err := filepath.Abs(expandedPath)
	if err != nil {
		return "", false, fmt.Errorf("解析队列目录绝对路径失败: %w", err)
	}
	return resolvedPath, true, nil
}

func enqueueAgentQueueRequest(
	queueDir string,
	request ingestBatchRequest,
	enqueuedAt time.Time,
) (agentQueueEntry, string, error) {
	if strings.TrimSpace(queueDir) == "" {
		return agentQueueEntry{}, "", fmt.Errorf("queueDir 不能为空")
	}
	if err := os.MkdirAll(queueDir, 0o700); err != nil {
		return agentQueueEntry{}, "", fmt.Errorf("创建队列目录失败: %w", err)
	}

	entry := agentQueueEntry{
		ID:         newID("queue"),
		EnqueuedAt: enqueuedAt.UTC().Format(time.RFC3339Nano),
		Request:    request,
	}
	fileName := fmt.Sprintf("%020d_%s.json", enqueuedAt.UTC().UnixNano(), entry.ID)
	finalPath := filepath.Join(queueDir, fileName)
	body, err := json.Marshal(entry)
	if err != nil {
		return agentQueueEntry{}, "", fmt.Errorf("序列化队列项失败: %w", err)
	}

	tempFile, err := os.CreateTemp(queueDir, ".agent-queue-*.tmp")
	if err != nil {
		return agentQueueEntry{}, "", fmt.Errorf("创建队列临时文件失败: %w", err)
	}
	tempPath := tempFile.Name()
	if _, err := tempFile.Write(body); err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		return agentQueueEntry{}, "", fmt.Errorf("写入队列临时文件失败: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		return agentQueueEntry{}, "", fmt.Errorf("关闭队列临时文件失败: %w", err)
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		_ = os.Remove(tempPath)
		return agentQueueEntry{}, "", fmt.Errorf("写入队列文件失败: %w", err)
	}

	return entry, finalPath, nil
}

func loadAgentQueueFiles(queueDir string) ([]agentQueueFile, error) {
	info, err := os.Stat(queueDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []agentQueueFile{}, nil
		}
		return nil, fmt.Errorf("读取队列目录失败: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("队列路径指向文件")
	}

	dirEntries, err := os.ReadDir(queueDir)
	if err != nil {
		return nil, fmt.Errorf("列出队列目录失败: %w", err)
	}
	candidates := make([]string, 0, len(dirEntries))
	for _, item := range dirEntries {
		if item.IsDir() {
			continue
		}
		if strings.HasPrefix(item.Name(), ".") || !strings.HasSuffix(item.Name(), ".json") {
			continue
		}
		candidates = append(candidates, filepath.Join(queueDir, item.Name()))
	}
	sort.Strings(candidates)

	files := make([]agentQueueFile, 0, len(candidates))
	for _, path := range candidates {
		item, err := readAgentQueueFile(path)
		if err != nil {
			return nil, err
		}
		files = append(files, item)
	}
	return files, nil
}

func readAgentQueueFile(path string) (agentQueueFile, error) {
	info, err := os.Stat(path)
	if err != nil {
		return agentQueueFile{}, fmt.Errorf("读取队列文件信息失败: %w", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return agentQueueFile{}, fmt.Errorf("读取队列文件失败: %w", err)
	}
	var entry agentQueueEntry
	if err := json.Unmarshal(content, &entry); err != nil {
		return agentQueueFile{}, fmt.Errorf("解析队列文件失败: %w", err)
	}
	if strings.TrimSpace(entry.ID) == "" {
		return agentQueueFile{}, fmt.Errorf("队列文件缺少 id")
	}
	if strings.TrimSpace(entry.EnqueuedAt) == "" {
		return agentQueueFile{}, fmt.Errorf("队列文件缺少 enqueued_at")
	}
	if strings.TrimSpace(entry.Request.BatchID) == "" {
		return agentQueueFile{}, fmt.Errorf("队列文件缺少 request.batch_id")
	}
	return agentQueueFile{
		Path:      path,
		SizeBytes: info.Size(),
		Entry:     entry,
	}, nil
}

func flushAgentQueue(
	queueDir string,
	endpoint string,
	timeout time.Duration,
	protocol string,
	authHeader string,
	grpcConfig grpcClientSecurityConfig,
	currentEntryID string,
) (agentQueueFlushResult, error) {
	files, err := loadAgentQueueFiles(queueDir)
	if err != nil {
		return agentQueueFlushResult{}, err
	}

	result := agentQueueFlushResult{}
	for _, item := range files {
		statusCode, responseBody, err := sendIngestRequest(
			endpoint,
			timeout,
			protocol,
			authHeader,
			item.Entry.Request,
			grpcConfig,
		)
		if err != nil {
			return result, fmt.Errorf("发送队列批次 %s 失败: %w", item.Entry.Request.BatchID, err)
		}
		if exitCodeFromStatus(statusCode) != 0 {
			return result, fmt.Errorf(
				"发送队列批次 %s 失败: status=%d body=%s",
				item.Entry.Request.BatchID,
				statusCode,
				strings.TrimSpace(string(responseBody)),
			)
		}
		if err := os.Remove(item.Path); err != nil {
			return result, fmt.Errorf("删除队列文件失败: %w", err)
		}
		result.FlushedCount++
		if item.Entry.ID == currentEntryID {
			result.CurrentResponse = &agentQueueSendResult{
				EntryID:    item.Entry.ID,
				StatusCode: statusCode,
				Body:       responseBody,
			}
		}
	}
	return result, nil
}

func firstStringValue(values ...any) string {
	for _, value := range values {
		if text, ok := value.(string); ok {
			trimmed := strings.TrimSpace(text)
			if trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func normalizeSignatureStatus(raw string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return "unknown", nil
	}
	switch normalized {
	case "verified", "unverified", "invalid", "unsigned", "skipped", "unknown":
		return normalized, nil
	default:
		return "", fmt.Errorf("配置包 signature_status 非法: %s", raw)
	}
}

func versionCommand(args []string) int {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	short := fs.Bool("short", false, "仅输出版本号")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *short {
		fmt.Println(version)
		return 0
	}

	fmt.Printf("version=%s commit=%s build_time=%s\n", version, commit, buildTime)
	return 0
}

func printConfigUsage() {
	fmt.Fprintln(os.Stderr, "AgentLedger Agent CLI - config")
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  agent config pull --package-id <id> [--gateway <url>] [--config-dir <dir>] [--token-file <path>]")
	fmt.Fprintln(os.Stderr, "  agent config activate --package-id <id> [--config-dir <dir>]")
	fmt.Fprintln(os.Stderr, "  agent config rollback [--config-dir <dir>]")
	fmt.Fprintln(os.Stderr, "  agent config watch [--gateway <url>] [--config-dir <dir>] [--token-file <path>] [--interval <dur>] [--iterations <n>] [--auto-activate]")
}

func printUpdateUsage() {
	fmt.Fprintln(os.Stderr, "AgentLedger Agent CLI - update")
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  agent update check [--gateway <url>] [--channel <stable|beta|canary>] [--current-version <ver>] [--os <os>] [--arch <arch>] [--agent-id <id>] [--device-id <id>] [--hostname <name>] [--ring <name>] [--token-file <path>] [--report-lifecycle --tenant-id <id>]")
	fmt.Fprintln(os.Stderr, "  agent update download [--gateway <url>] [--channel <stable|beta|canary>] [--current-version <ver>] [--os <os>] [--arch <arch>] [--agent-id <id>] [--device-id <id>] [--hostname <name>] [--ring <name>] [--token-file <path>] [--config-dir <dir>]")
	fmt.Fprintln(os.Stderr, "  agent update apply [--config-dir <dir>] [--binary-path <path>]")
	fmt.Fprintln(os.Stderr, "  agent update rollback [--config-dir <dir>] [--binary-path <path>]")
	fmt.Fprintln(os.Stderr, "  agent update status [--config-dir <dir>]")
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "AgentLedger Agent CLI")
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  agent <command> [flags]")
	fmt.Fprintln(os.Stderr, "Commands:")
	fmt.Fprintln(os.Stderr, "  run      运行采集流程，支持 --daemon 守护模式拉取配置并上报心跳")
	fmt.Fprintln(os.Stderr, "  collect  采集本地会话并输出 agentEvent JSONL")
	fmt.Fprintln(os.Stderr, "  oidc     OIDC 设备码登录")
	fmt.Fprintln(os.Stderr, "  config   拉取/激活本地配置包")
	fmt.Fprintln(os.Stderr, "  doctor   环境自检占位流程")
	fmt.Fprintln(os.Stderr, "  status   查看本地 token / endpoint / 配置包 / 队列状态")
	fmt.Fprintln(os.Stderr, "  update   检查指定渠道的 Agent 新版本")
	fmt.Fprintln(os.Stderr, "  version  输出版本信息")
}
