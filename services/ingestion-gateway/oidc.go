package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/agentledger/agentledger/services/internal/shared/config"
)

const (
	oidcDeviceStartPath = "/v1/oidc/device/start"
	oidcDevicePollPath  = "/v1/oidc/device/poll"

	oidcDeviceCodeGrantType = "urn:ietf:params:oauth:grant-type:device_code"
	oidcMaxRequestBodyBytes = 64 << 10
	oidcMaxResponseBytes    = 1 << 20
	oidcUpstreamTimeout     = 15 * time.Second
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

type oidcHandler struct {
	cfg        config.OIDCConfig
	httpClient *http.Client
	log        *slog.Logger
}

func newOIDCHandler(cfg config.OIDCConfig, log *slog.Logger) *oidcHandler {
	return &oidcHandler{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: oidcUpstreamTimeout,
		},
		log: log,
	}
}

func (h *oidcHandler) handleDeviceStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeOIDCStartError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}

	if strings.TrimSpace(h.cfg.DeviceAuthEndpoint) == "" {
		writeOIDCStartError(w, http.StatusServiceUnavailable, "server_config_error", "OIDC device authorization endpoint is not configured")
		return
	}

	req, err := decodeOIDCStartRequest(w, r)
	if err != nil {
		writeOIDCStartError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	clientID := firstNonEmpty(req.ClientID, h.cfg.ClientID)
	if strings.TrimSpace(clientID) == "" {
		writeOIDCStartError(w, http.StatusBadRequest, "invalid_request", "client_id is required when OIDC_CLIENT_ID is not configured")
		return
	}

	audience := firstNonEmpty(req.Audience, h.cfg.Audience)
	form := url.Values{}
	form.Set("client_id", strings.TrimSpace(clientID))
	if scope := strings.TrimSpace(req.Scope); scope != "" {
		form.Set("scope", scope)
	}
	if audience = strings.TrimSpace(audience); audience != "" {
		form.Set("audience", audience)
	}

	statusCode, body, err := h.sendOIDCFormRequest(r.Context(), strings.TrimSpace(h.cfg.DeviceAuthEndpoint), form)
	if err != nil {
		h.log.Warn("oidc device start upstream request failed", "error", err)
		writeOIDCStartError(w, http.StatusBadGateway, "upstream_request_failed", fmt.Sprintf("request upstream device endpoint failed: %v", err))
		return
	}

	response, parseErr := decodeOIDCStartUpstreamResponse(body)
	if parseErr != nil {
		writeOIDCStartError(w, http.StatusBadGateway, "invalid_upstream_response", parseErr.Error())
		return
	}
	if statusCode >= 300 {
		normalizeOIDCStartError(&response, statusCode)
		writeJSON(w, statusCode, response)
		return
	}

	if strings.TrimSpace(response.DeviceCode) == "" ||
		strings.TrimSpace(response.UserCode) == "" ||
		strings.TrimSpace(response.VerificationURI) == "" {
		writeOIDCStartError(w, http.StatusBadGateway, "invalid_upstream_response", "upstream response missing device_code/user_code/verification_uri")
		return
	}

	writeJSON(w, statusCode, response)
}

func (h *oidcHandler) handleDevicePoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeOIDCPollError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}

	if strings.TrimSpace(h.cfg.TokenEndpoint) == "" {
		writeOIDCPollError(w, http.StatusServiceUnavailable, "server_config_error", "OIDC token endpoint is not configured")
		return
	}

	req, err := decodeOIDCPollRequest(w, r)
	if err != nil {
		writeOIDCPollError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	clientID := firstNonEmpty(req.ClientID, h.cfg.ClientID)
	if strings.TrimSpace(clientID) == "" {
		writeOIDCPollError(w, http.StatusBadRequest, "invalid_request", "client_id is required when OIDC_CLIENT_ID is not configured")
		return
	}

	form := url.Values{}
	form.Set("grant_type", oidcDeviceCodeGrantType)
	form.Set("client_id", strings.TrimSpace(clientID))
	form.Set("device_code", strings.TrimSpace(req.DeviceCode))

	statusCode, body, err := h.sendOIDCFormRequest(r.Context(), strings.TrimSpace(h.cfg.TokenEndpoint), form)
	if err != nil {
		h.log.Warn("oidc device poll upstream request failed", "error", err)
		writeOIDCPollError(w, http.StatusBadGateway, "upstream_request_failed", fmt.Sprintf("request upstream token endpoint failed: %v", err))
		return
	}

	response, parseErr := decodeOIDCPollUpstreamResponse(body)
	if parseErr != nil {
		writeOIDCPollError(w, http.StatusBadGateway, "invalid_upstream_response", parseErr.Error())
		return
	}
	if statusCode >= 300 {
		normalizeOIDCPollError(&response, statusCode)
		writeJSON(w, statusCode, response)
		return
	}

	if strings.TrimSpace(response.AccessToken) == "" &&
		strings.TrimSpace(response.Error) == "" &&
		strings.TrimSpace(response.Status) == "" {
		writeOIDCPollError(w, http.StatusBadGateway, "invalid_upstream_response", "upstream response missing access_token or status/error")
		return
	}

	writeJSON(w, statusCode, response)
}

func decodeOIDCStartRequest(w http.ResponseWriter, r *http.Request) (oidcDeviceStartRequest, error) {
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if contentType != "" && !strings.Contains(strings.ToLower(contentType), "application/json") {
		return oidcDeviceStartRequest{}, fmt.Errorf("unsupported content type: %s", contentType)
	}

	r.Body = http.MaxBytesReader(w, r.Body, oidcMaxRequestBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return oidcDeviceStartRequest{}, fmt.Errorf("read request body: %w", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return oidcDeviceStartRequest{}, fmt.Errorf("request body is empty")
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()

	var req oidcDeviceStartRequest
	if err := decoder.Decode(&req); err != nil {
		return oidcDeviceStartRequest{}, fmt.Errorf("decode json: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return oidcDeviceStartRequest{}, fmt.Errorf("request body has trailing data")
	}

	return req, nil
}

func decodeOIDCPollRequest(w http.ResponseWriter, r *http.Request) (oidcDevicePollRequest, error) {
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if contentType != "" && !strings.Contains(strings.ToLower(contentType), "application/json") {
		return oidcDevicePollRequest{}, fmt.Errorf("unsupported content type: %s", contentType)
	}

	r.Body = http.MaxBytesReader(w, r.Body, oidcMaxRequestBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return oidcDevicePollRequest{}, fmt.Errorf("read request body: %w", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return oidcDevicePollRequest{}, fmt.Errorf("request body is empty")
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()

	var req oidcDevicePollRequest
	if err := decoder.Decode(&req); err != nil {
		return oidcDevicePollRequest{}, fmt.Errorf("decode json: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return oidcDevicePollRequest{}, fmt.Errorf("request body has trailing data")
	}
	if strings.TrimSpace(req.DeviceCode) == "" {
		return oidcDevicePollRequest{}, fmt.Errorf("device_code is required")
	}

	return req, nil
}

func (h *oidcHandler) sendOIDCFormRequest(ctx context.Context, endpoint string, form url.Values) (int, []byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, oidcUpstreamTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return 0, nil, fmt.Errorf("build upstream request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		if requestCtx.Err() != nil {
			return 0, nil, requestCtx.Err()
		}
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, oidcMaxResponseBytes))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("read upstream response: %w", err)
	}

	return resp.StatusCode, body, nil
}

func decodeOIDCStartUpstreamResponse(body []byte) (oidcDeviceStartResponse, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return oidcDeviceStartResponse{}, fmt.Errorf("upstream returned empty response body")
	}

	var response oidcDeviceStartResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return oidcDeviceStartResponse{}, fmt.Errorf("upstream response is not valid json: %s", summarizeOIDCUpstreamBody(body))
	}

	return response, nil
}

func decodeOIDCPollUpstreamResponse(body []byte) (oidcDevicePollResponse, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return oidcDevicePollResponse{}, fmt.Errorf("upstream returned empty response body")
	}

	var response oidcDevicePollResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return oidcDevicePollResponse{}, fmt.Errorf("upstream response is not valid json: %s", summarizeOIDCUpstreamBody(body))
	}

	return response, nil
}

func normalizeOIDCStartError(response *oidcDeviceStartResponse, statusCode int) {
	if strings.TrimSpace(response.Error) == "" {
		response.Error = "upstream_error"
	}
	if strings.TrimSpace(response.ErrorDescription) == "" {
		response.ErrorDescription = firstNonEmpty(response.Message, fmt.Sprintf("upstream returned HTTP %d", statusCode))
	}
	if strings.TrimSpace(response.Message) == "" {
		response.Message = response.ErrorDescription
	}
}

func normalizeOIDCPollError(response *oidcDevicePollResponse, statusCode int) {
	if strings.TrimSpace(response.Error) == "" {
		response.Error = "upstream_error"
	}
	if strings.TrimSpace(response.ErrorDescription) == "" {
		response.ErrorDescription = firstNonEmpty(response.Message, fmt.Sprintf("upstream returned HTTP %d", statusCode))
	}
	if strings.TrimSpace(response.Message) == "" {
		response.Message = response.ErrorDescription
	}
	if strings.TrimSpace(response.Status) == "" {
		response.Status = response.Error
	}
}

func writeOIDCStartError(w http.ResponseWriter, statusCode int, code, description string) {
	desc := strings.TrimSpace(description)
	writeJSON(w, statusCode, oidcDeviceStartResponse{
		Error:            strings.TrimSpace(code),
		ErrorDescription: desc,
		Message:          desc,
	})
}

func writeOIDCPollError(w http.ResponseWriter, statusCode int, code, description string) {
	desc := strings.TrimSpace(description)
	errCode := strings.TrimSpace(code)
	writeJSON(w, statusCode, oidcDevicePollResponse{
		Status:           errCode,
		Error:            errCode,
		ErrorDescription: desc,
		Message:          desc,
	})
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func summarizeOIDCUpstreamBody(body []byte) string {
	const maxLen = 240
	text := strings.TrimSpace(string(body))
	if text == "" {
		return "empty body"
	}
	if len(text) <= maxLen {
		return text
	}
	return text[:maxLen] + "..."
}
