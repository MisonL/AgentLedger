package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	version   = "0.1.0-dev"
	commit    = "none"
	buildTime = "unknown"
)

const (
	defaultHTTPEndpoint = "http://127.0.0.1:8081/v1/ingest"
	defaultGRPCEndpoint = "127.0.0.1:9091"
)

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
	case "version":
		os.Exit(versionCommand(os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n", os.Args[1])
		printUsage()
		os.Exit(2)
	}
}

func runCommand(args []string) int {
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

	authHeader := ""
	token, tokenPath, err := loadLocalToken(*tokenFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
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

	events, err := buildRunEvents(*jsonlPath, *generate, *sessionID, *provider)
	if err != nil {
		fmt.Fprintf(os.Stderr, "构建事件失败: %v\n", err)
		return 1
	}
	if len(events) == 0 {
		fmt.Fprintln(os.Stderr, "没有可推送事件")
		return 1
	}

	request := ingestBatchRequest{
		BatchID: resolveBatchID(*batchID),
		Agent: agentInfo{
			AgentID:     strings.TrimSpace(*agentID),
			Hostname:    host,
			Version:     version,
			WorkspaceID: "",
		},
		Source: sourceInfo{
			SourceID:   strings.TrimSpace(*sourceID),
			Provider:   strings.TrimSpace(*provider),
			SourceType: strings.TrimSpace(*sourceType),
		},
		Events: events,
		SentAt: time.Now().UTC().Format(time.RFC3339Nano),
	}

	statusCode, responseBody, err := sendIngestRequest(endpointValue, *timeout, protocolName, authHeader, request, grpcConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "调用 ingestion-gateway 失败: %v\n", err)
		return 1
	}

	var response ingestBatchResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		fmt.Printf("推送完成: protocol=%s status=%d 响应=%s\n", protocolName, statusCode, string(responseBody))
		return exitCodeFromStatus(statusCode)
	}

	fmt.Printf("推送完成: protocol=%s status=%d batch=%s accepted=%d rejected=%d duration_ms=%d\n",
		protocolName, statusCode, response.BatchID, response.Accepted, response.Rejected, response.DurationMS)
	for _, reject := range response.Errors {
		fmt.Printf("  reject index=%d event_id=%s message=%s\n", reject.Index, reject.EventID, reject.Message)
	}
	return exitCodeFromStatus(statusCode)
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

func printUsage() {
	fmt.Fprintln(os.Stderr, "AgentLedger Agent CLI")
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  agent <command> [flags]")
	fmt.Fprintln(os.Stderr, "Commands:")
	fmt.Fprintln(os.Stderr, "  run      运行采集占位流程")
	fmt.Fprintln(os.Stderr, "  collect  采集本地会话并输出 agentEvent JSONL")
	fmt.Fprintln(os.Stderr, "  oidc     OIDC 设备码登录")
	fmt.Fprintln(os.Stderr, "  doctor   环境自检占位流程")
	fmt.Fprintln(os.Stderr, "  version  输出版本信息")
}
