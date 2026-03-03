package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	oidcDeviceStartPath = "/v1/oidc/device/start"
	oidcDevicePollPath  = "/v1/oidc/device/poll"
)

type oidcDeviceStartRequest struct {
	ClientID string `json:"client_id,omitempty"`
	Scope    string `json:"scope,omitempty"`
	Audience string `json:"audience,omitempty"`
}

type oidcDeviceStartResponse struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete,omitempty"`
	ExpiresIn               int64  `json:"expires_in,omitempty"`
	Interval                int64  `json:"interval,omitempty"`
	Message                 string `json:"message,omitempty"`
	Error                   string `json:"error,omitempty"`
	ErrorDescription        string `json:"error_description,omitempty"`
}

type oidcDevicePollRequest struct {
	ClientID   string `json:"client_id,omitempty"`
	DeviceCode string `json:"device_code"`
}

type oidcDevicePollResponse struct {
	AccessToken      string `json:"access_token,omitempty"`
	TokenType        string `json:"token_type,omitempty"`
	RefreshToken     string `json:"refresh_token,omitempty"`
	IDToken          string `json:"id_token,omitempty"`
	Scope            string `json:"scope,omitempty"`
	ExpiresIn        int64  `json:"expires_in,omitempty"`
	Interval         int64  `json:"interval,omitempty"`
	Status           string `json:"status,omitempty"`
	Message          string `json:"message,omitempty"`
	Error            string `json:"error,omitempty"`
	ErrorDescription string `json:"error_description,omitempty"`
}

type oidcPollState int

const (
	oidcPollStatePending oidcPollState = iota + 1
	oidcPollStateSlowDown
	oidcPollStateReady
)

func oidcCommand(args []string) int {
	if len(args) < 1 {
		printOIDCUsage()
		return 2
	}

	switch args[0] {
	case "login":
		return oidcLoginCommand(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "未知 oidc 子命令: %s\n", args[0])
		printOIDCUsage()
		return 2
	}
}

func oidcLoginCommand(args []string) int {
	fs := flag.NewFlagSet("oidc login", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	gateway := fs.String("gateway", "http://127.0.0.1:8081", "ingestion-gateway 地址（不含路径）")
	clientID := fs.String("client-id", "agent-cli", "OIDC client_id")
	scope := fs.String("scope", "openid profile offline_access", "OIDC scope")
	audience := fs.String("audience", "", "OIDC audience（可选）")
	timeout := fs.Duration("timeout", 5*time.Minute, "登录总超时时间")
	requestTimeout := fs.Duration("request-timeout", 10*time.Second, "单次 HTTP 请求超时时间")
	interval := fs.Duration("interval", 0, "轮询间隔（默认使用服务端返回值）")
	tokenFile := fs.String("token-file", defaultTokenFilePath(), "token 文件路径")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*clientID) == "" {
		fmt.Fprintln(os.Stderr, "client-id 不能为空")
		return 2
	}
	if *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "timeout 必须 > 0")
		return 2
	}
	if *requestTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "request-timeout 必须 > 0")
		return 2
	}

	baseURL, err := normalizeGatewayBaseURL(*gateway)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway 参数错误: %v\n", err)
		return 2
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(rootCtx, *timeout)
	defer cancel()

	startResp, err := startOIDCDeviceFlow(ctx, baseURL, oidcDeviceStartRequest{
		ClientID: strings.TrimSpace(*clientID),
		Scope:    strings.TrimSpace(*scope),
		Audience: strings.TrimSpace(*audience),
	}, *requestTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "启动 OIDC 设备码登录失败: %v\n", err)
		return 1
	}

	fmt.Println("请在浏览器完成登录授权：")
	fmt.Printf("  verification_uri: %s\n", startResp.VerificationURI)
	if strings.TrimSpace(startResp.VerificationURIComplete) != "" {
		fmt.Printf("  verification_uri_complete: %s\n", startResp.VerificationURIComplete)
	}
	fmt.Printf("  user_code: %s\n", startResp.UserCode)
	if startResp.ExpiresIn > 0 {
		fmt.Printf("  设备码有效期: %ds\n", startResp.ExpiresIn)
	}
	if strings.TrimSpace(startResp.Message) != "" {
		fmt.Printf("  提示: %s\n", strings.TrimSpace(startResp.Message))
	}

	pollInterval := *interval
	if pollInterval <= 0 && startResp.Interval > 0 {
		pollInterval = time.Duration(startResp.Interval) * time.Second
	}
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}

	pollResp, err := pollOIDCToken(ctx, baseURL, strings.TrimSpace(*clientID), startResp.DeviceCode, pollInterval, *requestTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "OIDC 登录失败: %v\n", err)
		return 1
	}

	token := buildLocalTokenFromOIDC(*pollResp)
	path, err := saveLocalToken(*tokenFile, token)
	if err != nil {
		fmt.Fprintf(os.Stderr, "保存 token 失败: %v\n", err)
		return 1
	}

	fmt.Printf("登录成功，token 已保存到: %s\n", path)
	if token.ExpiresAt != "" {
		fmt.Printf("token 过期时间(UTC): %s\n", token.ExpiresAt)
	}
	return 0
}

func startOIDCDeviceFlow(ctx context.Context, baseURL string, payload oidcDeviceStartRequest, requestTimeout time.Duration) (*oidcDeviceStartResponse, error) {
	endpoint := baseURL + oidcDeviceStartPath
	statusCode, body, err := sendJSONRequest(ctx, endpoint, payload, "", requestTimeout)
	if err != nil {
		return nil, err
	}

	var response oidcDeviceStartResponse
	if len(strings.TrimSpace(string(body))) != 0 {
		if err := json.Unmarshal(body, &response); err != nil {
			return nil, fmt.Errorf("解析启动登录响应失败: %w", err)
		}
	}

	if statusCode >= 300 {
		message := firstNonEmpty(response.ErrorDescription, response.Message, summarizeResponseBody(body))
		errorCode := strings.TrimSpace(response.Error)
		if errorCode != "" {
			return nil, fmt.Errorf("服务端返回错误（HTTP %d, code=%s）: %s", statusCode, errorCode, message)
		}
		return nil, fmt.Errorf("服务端返回错误（HTTP %d）: %s", statusCode, message)
	}

	if strings.TrimSpace(response.DeviceCode) == "" ||
		strings.TrimSpace(response.UserCode) == "" ||
		strings.TrimSpace(response.VerificationURI) == "" {
		return nil, fmt.Errorf("启动响应缺少必要字段(device_code/user_code/verification_uri)")
	}

	return &response, nil
}

func pollOIDCToken(
	ctx context.Context,
	baseURL, clientID, deviceCode string,
	initialInterval, requestTimeout time.Duration,
) (*oidcDevicePollResponse, error) {
	endpoint := baseURL + oidcDevicePollPath
	currentInterval := initialInterval
	if currentInterval <= 0 {
		currentInterval = 5 * time.Second
	}

	for {
		resp, state, err := pollOIDCTokenOnce(ctx, endpoint, oidcDevicePollRequest{
			ClientID:   clientID,
			DeviceCode: deviceCode,
		}, requestTimeout)
		if err != nil {
			return nil, err
		}

		if resp != nil && resp.Interval > 0 {
			serverInterval := time.Duration(resp.Interval) * time.Second
			if serverInterval > currentInterval {
				currentInterval = serverInterval
			}
		}

		if state == oidcPollStateReady {
			return resp, nil
		}
		if state == oidcPollStateSlowDown {
			currentInterval += 2 * time.Second
		}

		select {
		case <-ctx.Done():
			return nil, wrapOIDCContextError(ctx.Err())
		case <-time.After(currentInterval):
		}
	}
}

func pollOIDCTokenOnce(ctx context.Context, endpoint string, payload oidcDevicePollRequest, requestTimeout time.Duration) (*oidcDevicePollResponse, oidcPollState, error) {
	statusCode, body, err := sendJSONRequest(ctx, endpoint, payload, "", requestTimeout)
	if err != nil {
		return nil, oidcPollStatePending, err
	}

	var response oidcDevicePollResponse
	if len(strings.TrimSpace(string(body))) != 0 {
		if err := json.Unmarshal(body, &response); err != nil {
			return nil, oidcPollStatePending, fmt.Errorf("解析轮询响应失败: %w", err)
		}
	}

	if strings.TrimSpace(response.AccessToken) != "" && statusCode >= 200 && statusCode < 300 {
		return &response, oidcPollStateReady, nil
	}

	responseCode := strings.ToLower(strings.TrimSpace(firstNonEmpty(response.Error, response.Status)))
	switch responseCode {
	case "authorization_pending", "pending", "waiting":
		return &response, oidcPollStatePending, nil
	case "slow_down":
		return &response, oidcPollStateSlowDown, nil
	case "access_denied":
		return nil, oidcPollStatePending, fmt.Errorf("授权被拒绝，请重新执行 `agent oidc login` 并确认授权")
	case "expired_token", "expired_device_code":
		return nil, oidcPollStatePending, fmt.Errorf("设备码已过期，请重新执行 `agent oidc login`")
	}

	if statusCode >= 300 {
		message := firstNonEmpty(response.ErrorDescription, response.Message, summarizeResponseBody(body))
		if responseCode == "" {
			responseCode = "unknown"
		}
		return nil, oidcPollStatePending, fmt.Errorf("轮询失败（HTTP %d, code=%s）: %s", statusCode, responseCode, message)
	}

	if statusCode >= 200 && statusCode < 300 {
		return nil, oidcPollStatePending, fmt.Errorf("轮询响应未包含 access_token")
	}
	return nil, oidcPollStatePending, fmt.Errorf("轮询失败（HTTP %d）", statusCode)
}

func sendJSONRequest(ctx context.Context, endpoint string, payload any, authHeader string, requestTimeout time.Duration) (int, []byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("序列化请求失败: %w", err)
	}

	requestCtx := ctx
	cancel := func() {}
	if requestTimeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, requestTimeout)
	}
	defer cancel()

	httpReq, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, strings.NewReader(string(body)))
	if err != nil {
		return 0, nil, fmt.Errorf("构建请求失败: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(authHeader) != "" {
		httpReq.Header.Set("Authorization", authHeader)
	}

	client := &http.Client{Timeout: requestTimeout}
	resp, err := client.Do(httpReq)
	if err != nil {
		if requestCtx.Err() != nil {
			return 0, nil, wrapOIDCContextError(requestCtx.Err())
		}
		return 0, nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("读取响应失败: %w", err)
	}
	return resp.StatusCode, responseBody, nil
}

func buildLocalTokenFromOIDC(resp oidcDevicePollResponse) localToken {
	now := time.Now().UTC()
	token := localToken{
		AccessToken:  strings.TrimSpace(resp.AccessToken),
		TokenType:    firstNonEmpty(strings.TrimSpace(resp.TokenType), "Bearer"),
		RefreshToken: strings.TrimSpace(resp.RefreshToken),
		IDToken:      strings.TrimSpace(resp.IDToken),
		Scope:        strings.TrimSpace(resp.Scope),
		ExpiresIn:    resp.ExpiresIn,
		ObtainedAt:   now.Format(time.RFC3339),
	}
	if token.ExpiresIn > 0 {
		token.ExpiresAt = now.Add(time.Duration(token.ExpiresIn) * time.Second).Format(time.RFC3339)
	}
	return token
}

func normalizeGatewayBaseURL(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("gateway 不能为空")
	}

	parsed, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("无效 URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("仅支持 http/https 地址")
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return "", fmt.Errorf("gateway 缺少主机地址")
	}

	return strings.TrimRight(parsed.String(), "/"), nil
}

func wrapOIDCContextError(err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("操作已取消")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("操作超时")
	}
	return err
}

func summarizeResponseBody(body []byte) string {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return "空响应"
	}
	const maxLen = 240
	if len(text) <= maxLen {
		return text
	}
	return text[:maxLen] + "..."
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		v := strings.TrimSpace(value)
		if v != "" {
			return v
		}
	}
	return ""
}

func printOIDCUsage() {
	fmt.Fprintln(os.Stderr, "OIDC Commands:")
	fmt.Fprintln(os.Stderr, "  agent oidc login   使用设备码完成登录并保存本地 token")
}
