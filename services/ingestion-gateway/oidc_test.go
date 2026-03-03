package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/agentledger/agentledger/services/internal/shared/config"
)

func TestDecodeOIDCStartRequest(t *testing.T) {
	t.Parallel()

	t.Run("valid_request", func(t *testing.T) {
		t.Parallel()

		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, oidcDeviceStartPath, strings.NewReader(`{"client_id":"cid","scope":"openid profile","audience":"api://ledger"}`))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")

		got, err := decodeOIDCStartRequest(recorder, req)
		if err != nil {
			t.Fatalf("decodeOIDCStartRequest() unexpected error: %v", err)
		}
		if got.ClientID != "cid" || got.Scope != "openid profile" || got.Audience != "api://ledger" {
			t.Fatalf("decodeOIDCStartRequest() got %+v", got)
		}
	})

	t.Run("empty_body", func(t *testing.T) {
		t.Parallel()

		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, oidcDeviceStartPath, strings.NewReader("   "))
		req.Header.Set("Content-Type", "application/json")

		_, err := decodeOIDCStartRequest(recorder, req)
		if err == nil || !strings.Contains(err.Error(), "request body is empty") {
			t.Fatalf("decodeOIDCStartRequest() error = %v, want contains %q", err, "request body is empty")
		}
	})

	t.Run("unsupported_content_type", func(t *testing.T) {
		t.Parallel()

		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, oidcDeviceStartPath, strings.NewReader(`{"client_id":"cid"}`))
		req.Header.Set("Content-Type", "text/plain")

		_, err := decodeOIDCStartRequest(recorder, req)
		if err == nil || !strings.Contains(err.Error(), "unsupported content type") {
			t.Fatalf("decodeOIDCStartRequest() error = %v, want contains %q", err, "unsupported content type")
		}
	})
}

func TestDecodeOIDCPollRequest(t *testing.T) {
	t.Parallel()

	t.Run("valid_request", func(t *testing.T) {
		t.Parallel()

		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"client_id":"cid","device_code":"dc-123"}`))
		req.Header.Set("Content-Type", "application/json")

		got, err := decodeOIDCPollRequest(recorder, req)
		if err != nil {
			t.Fatalf("decodeOIDCPollRequest() unexpected error: %v", err)
		}
		if got.ClientID != "cid" || got.DeviceCode != "dc-123" {
			t.Fatalf("decodeOIDCPollRequest() got %+v", got)
		}
	})

	t.Run("empty_body", func(t *testing.T) {
		t.Parallel()

		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(""))
		req.Header.Set("Content-Type", "application/json")

		_, err := decodeOIDCPollRequest(recorder, req)
		if err == nil || !strings.Contains(err.Error(), "request body is empty") {
			t.Fatalf("decodeOIDCPollRequest() error = %v, want contains %q", err, "request body is empty")
		}
	})

	t.Run("unsupported_content_type", func(t *testing.T) {
		t.Parallel()

		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"device_code":"dc-123"}`))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		_, err := decodeOIDCPollRequest(recorder, req)
		if err == nil || !strings.Contains(err.Error(), "unsupported content type") {
			t.Fatalf("decodeOIDCPollRequest() error = %v, want contains %q", err, "unsupported content type")
		}
	})

	t.Run("missing_device_code", func(t *testing.T) {
		t.Parallel()

		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"client_id":"cid"}`))
		req.Header.Set("Content-Type", "application/json")

		_, err := decodeOIDCPollRequest(recorder, req)
		if err == nil || !strings.Contains(err.Error(), "device_code is required") {
			t.Fatalf("decodeOIDCPollRequest() error = %v, want contains %q", err, "device_code is required")
		}
	})
}

func TestNormalizeOIDCStartError(t *testing.T) {
	t.Parallel()

	t.Run("fill_empty_fields", func(t *testing.T) {
		t.Parallel()

		response := oidcDeviceStartResponse{}
		normalizeOIDCStartError(&response, http.StatusBadGateway)

		if response.Error != "upstream_error" {
			t.Fatalf("Error = %q, want %q", response.Error, "upstream_error")
		}
		if response.ErrorDescription != "upstream returned HTTP 502" {
			t.Fatalf("ErrorDescription = %q, want %q", response.ErrorDescription, "upstream returned HTTP 502")
		}
		if response.Message != "upstream returned HTTP 502" {
			t.Fatalf("Message = %q, want %q", response.Message, "upstream returned HTTP 502")
		}
	})

	t.Run("reuse_message_as_description", func(t *testing.T) {
		t.Parallel()

		response := oidcDeviceStartResponse{
			Error:   "access_denied",
			Message: "user denied",
		}
		normalizeOIDCStartError(&response, http.StatusForbidden)

		if response.Error != "access_denied" {
			t.Fatalf("Error = %q, want %q", response.Error, "access_denied")
		}
		if response.ErrorDescription != "user denied" {
			t.Fatalf("ErrorDescription = %q, want %q", response.ErrorDescription, "user denied")
		}
		if response.Message != "user denied" {
			t.Fatalf("Message = %q, want %q", response.Message, "user denied")
		}
	})
}

func TestNormalizeOIDCPollError(t *testing.T) {
	t.Parallel()

	t.Run("fill_empty_fields", func(t *testing.T) {
		t.Parallel()

		response := oidcDevicePollResponse{}
		normalizeOIDCPollError(&response, http.StatusBadRequest)

		if response.Error != "upstream_error" {
			t.Fatalf("Error = %q, want %q", response.Error, "upstream_error")
		}
		if response.ErrorDescription != "upstream returned HTTP 400" {
			t.Fatalf("ErrorDescription = %q, want %q", response.ErrorDescription, "upstream returned HTTP 400")
		}
		if response.Message != "upstream returned HTTP 400" {
			t.Fatalf("Message = %q, want %q", response.Message, "upstream returned HTTP 400")
		}
		if response.Status != "upstream_error" {
			t.Fatalf("Status = %q, want %q", response.Status, "upstream_error")
		}
	})

	t.Run("preserve_existing_error_and_status", func(t *testing.T) {
		t.Parallel()

		response := oidcDevicePollResponse{
			Error:  "authorization_pending",
			Status: "authorization_pending",
		}
		normalizeOIDCPollError(&response, http.StatusUnauthorized)

		if response.Error != "authorization_pending" {
			t.Fatalf("Error = %q, want %q", response.Error, "authorization_pending")
		}
		if response.Status != "authorization_pending" {
			t.Fatalf("Status = %q, want %q", response.Status, "authorization_pending")
		}
		if response.ErrorDescription != "upstream returned HTTP 401" {
			t.Fatalf("ErrorDescription = %q, want %q", response.ErrorDescription, "upstream returned HTTP 401")
		}
		if response.Message != "upstream returned HTTP 401" {
			t.Fatalf("Message = %q, want %q", response.Message, "upstream returned HTTP 401")
		}
	})
}

func TestOIDCDecodeUpstreamResponses(t *testing.T) {
	t.Parallel()

	t.Run("start_empty_body", func(t *testing.T) {
		t.Parallel()

		_, err := decodeOIDCStartUpstreamResponse([]byte("   "))
		if err == nil || !strings.Contains(err.Error(), "empty response body") {
			t.Fatalf("decodeOIDCStartUpstreamResponse() error = %v, want contains %q", err, "empty response body")
		}
	})

	t.Run("start_invalid_json", func(t *testing.T) {
		t.Parallel()

		_, err := decodeOIDCStartUpstreamResponse([]byte("not-json"))
		if err == nil || !strings.Contains(err.Error(), "not valid json") {
			t.Fatalf("decodeOIDCStartUpstreamResponse() error = %v, want contains %q", err, "not valid json")
		}
	})

	t.Run("start_valid_json", func(t *testing.T) {
		t.Parallel()

		got, err := decodeOIDCStartUpstreamResponse([]byte(`{"device_code":"dev","user_code":"user","verification_uri":"https://example.com"}`))
		if err != nil {
			t.Fatalf("decodeOIDCStartUpstreamResponse() unexpected error: %v", err)
		}
		if got.DeviceCode != "dev" || got.UserCode != "user" || got.VerificationURI != "https://example.com" {
			t.Fatalf("decodeOIDCStartUpstreamResponse() got %+v", got)
		}
	})

	t.Run("poll_empty_body", func(t *testing.T) {
		t.Parallel()

		_, err := decodeOIDCPollUpstreamResponse([]byte(" \n\t "))
		if err == nil || !strings.Contains(err.Error(), "empty response body") {
			t.Fatalf("decodeOIDCPollUpstreamResponse() error = %v, want contains %q", err, "empty response body")
		}
	})

	t.Run("poll_invalid_json", func(t *testing.T) {
		t.Parallel()

		_, err := decodeOIDCPollUpstreamResponse([]byte("{"))
		if err == nil || !strings.Contains(err.Error(), "not valid json") {
			t.Fatalf("decodeOIDCPollUpstreamResponse() error = %v, want contains %q", err, "not valid json")
		}
	})

	t.Run("poll_valid_json", func(t *testing.T) {
		t.Parallel()

		got, err := decodeOIDCPollUpstreamResponse([]byte(`{"access_token":"token","token_type":"Bearer"}`))
		if err != nil {
			t.Fatalf("decodeOIDCPollUpstreamResponse() unexpected error: %v", err)
		}
		if got.AccessToken != "token" || got.TokenType != "Bearer" {
			t.Fatalf("decodeOIDCPollUpstreamResponse() got %+v", got)
		}
	})
}

func TestOIDCSummarizeOIDCUpstreamBody(t *testing.T) {
	t.Parallel()

	if got := summarizeOIDCUpstreamBody([]byte("   ")); got != "empty body" {
		t.Fatalf("summarizeOIDCUpstreamBody() = %q, want %q", got, "empty body")
	}
	if got := summarizeOIDCUpstreamBody([]byte("  short body  ")); got != "short body" {
		t.Fatalf("summarizeOIDCUpstreamBody() = %q, want %q", got, "short body")
	}

	longBody := strings.Repeat("a", 260)
	got := summarizeOIDCUpstreamBody([]byte(longBody))
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("summarizeOIDCUpstreamBody() long suffix mismatch: got %q", got)
	}
	if len(got) != 243 {
		t.Fatalf("summarizeOIDCUpstreamBody() long length mismatch: got %d want %d", len(got), 243)
	}
}

func TestOIDCWriteErrors(t *testing.T) {
	t.Parallel()

	t.Run("start_error_payload", func(t *testing.T) {
		t.Parallel()

		rr := httptest.NewRecorder()
		writeOIDCStartError(rr, http.StatusBadRequest, " invalid_request ", " bad body ")

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("writeOIDCStartError() status = %d, want %d", rr.Code, http.StatusBadRequest)
		}
		var payload oidcDeviceStartResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if payload.Error != "invalid_request" || payload.ErrorDescription != "bad body" || payload.Message != "bad body" {
			t.Fatalf("writeOIDCStartError() payload = %+v", payload)
		}
	})

	t.Run("poll_error_payload", func(t *testing.T) {
		t.Parallel()

		rr := httptest.NewRecorder()
		writeOIDCPollError(rr, http.StatusBadGateway, " upstream_error ", " upstream failed ")

		if rr.Code != http.StatusBadGateway {
			t.Fatalf("writeOIDCPollError() status = %d, want %d", rr.Code, http.StatusBadGateway)
		}
		var payload oidcDevicePollResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if payload.Status != "upstream_error" || payload.Error != "upstream_error" {
			t.Fatalf("writeOIDCPollError() code fields = %+v", payload)
		}
		if payload.ErrorDescription != "upstream failed" || payload.Message != "upstream failed" {
			t.Fatalf("writeOIDCPollError() description fields = %+v", payload)
		}
	})
}

func TestOIDCSendOIDCFormRequest(t *testing.T) {
	t.Parallel()

	t.Run("build_request_error", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			httpClient: &http.Client{Timeout: time.Second},
			log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		_, _, err := handler.sendOIDCFormRequest(context.Background(), "://bad-endpoint", url.Values{"client_id": {"cid"}})
		if err == nil || !strings.Contains(err.Error(), "build upstream request") {
			t.Fatalf("sendOIDCFormRequest() error = %v, want contains %q", err, "build upstream request")
		}
	})

	t.Run("context_canceled", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			httpClient: &http.Client{
				Transport: oidcRoundTripper(func(_ *http.Request) (*http.Response, error) {
					return nil, errors.New("network error")
				}),
				Timeout: time.Second,
			},
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _, err := handler.sendOIDCFormRequest(ctx, "https://example.com/device", url.Values{"client_id": {"cid"}})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("sendOIDCFormRequest() error = %v, want %v", err, context.Canceled)
		}
	})

	t.Run("read_upstream_response_error", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			httpClient: &http.Client{
				Transport: oidcRoundTripper(func(_ *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusBadGateway,
						Body:       errorReadCloser{readErr: errors.New("read failed")},
						Header:     make(http.Header),
					}, nil
				}),
				Timeout: time.Second,
			},
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}

		statusCode, body, err := handler.sendOIDCFormRequest(context.Background(), "https://example.com/device", url.Values{"client_id": {"cid"}})
		if statusCode != http.StatusBadGateway {
			t.Fatalf("sendOIDCFormRequest() status = %d, want %d", statusCode, http.StatusBadGateway)
		}
		if body != nil {
			t.Fatalf("sendOIDCFormRequest() body should be nil when read fails, got %q", string(body))
		}
		if err == nil || !strings.Contains(err.Error(), "read upstream response") {
			t.Fatalf("sendOIDCFormRequest() error = %v, want contains %q", err, "read upstream response")
		}
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		var gotContentType string
		var gotAccept string
		var gotMethod string
		var gotForm url.Values

		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotContentType = r.Header.Get("Content-Type")
			gotAccept = r.Header.Get("Accept")
			body, _ := io.ReadAll(r.Body)
			gotForm, _ = url.ParseQuery(string(body))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer upstream.Close()

		handler := &oidcHandler{
			httpClient: upstream.Client(),
			log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		}

		form := url.Values{
			"client_id": {"cid"},
			"scope":     {"openid profile"},
		}
		statusCode, body, err := handler.sendOIDCFormRequest(context.Background(), upstream.URL, form)
		if err != nil {
			t.Fatalf("sendOIDCFormRequest() unexpected error: %v", err)
		}
		if statusCode != http.StatusCreated {
			t.Fatalf("sendOIDCFormRequest() status = %d, want %d", statusCode, http.StatusCreated)
		}
		if string(body) != `{"ok":true}` {
			t.Fatalf("sendOIDCFormRequest() body = %q", string(body))
		}
		if gotMethod != http.MethodPost {
			t.Fatalf("upstream request method = %q, want %q", gotMethod, http.MethodPost)
		}
		if !strings.Contains(strings.ToLower(gotContentType), "application/x-www-form-urlencoded") {
			t.Fatalf("upstream content-type = %q", gotContentType)
		}
		if !strings.Contains(strings.ToLower(gotAccept), "application/json") {
			t.Fatalf("upstream accept = %q", gotAccept)
		}
		if gotForm.Get("client_id") != "cid" || gotForm.Get("scope") != "openid profile" {
			t.Fatalf("upstream form = %v", gotForm)
		}
	})
}

func TestOIDCHandleDeviceStart(t *testing.T) {
	t.Parallel()

	t.Run("method_not_allowed", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodGet, oidcDeviceStartPath, nil)
		rr := httptest.NewRecorder()

		handler.handleDeviceStart(rr, req)

		if rr.Code != http.StatusMethodNotAllowed {
			t.Fatalf("handleDeviceStart() status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
		}
		var payload oidcDeviceStartResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "method_not_allowed" {
			t.Fatalf("handleDeviceStart() payload = %+v", payload)
		}
	})

	t.Run("endpoint_not_configured", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			cfg: config.OIDCConfig{},
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDeviceStartPath, strings.NewReader(`{"client_id":"cid"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDeviceStart(rr, req)
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("handleDeviceStart() status = %d, want %d", rr.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("invalid_request_body", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			cfg: config.OIDCConfig{DeviceAuthEndpoint: "https://example.com/device"},
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDeviceStartPath, strings.NewReader(" "))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDeviceStart(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("handleDeviceStart() status = %d, want %d", rr.Code, http.StatusBadRequest)
		}
		var payload oidcDeviceStartResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "invalid_request" || !strings.Contains(payload.Message, "request body is empty") {
			t.Fatalf("handleDeviceStart() payload = %+v", payload)
		}
	})

	t.Run("missing_client_id", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			cfg: config.OIDCConfig{DeviceAuthEndpoint: "https://example.com/device"},
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDeviceStartPath, strings.NewReader(`{"scope":"openid"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDeviceStart(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("handleDeviceStart() status = %d, want %d", rr.Code, http.StatusBadRequest)
		}
		var payload oidcDeviceStartResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "invalid_request" || !strings.Contains(payload.ErrorDescription, "client_id is required") {
			t.Fatalf("handleDeviceStart() payload = %+v", payload)
		}
	})

	t.Run("upstream_request_failed", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			cfg: config.OIDCConfig{DeviceAuthEndpoint: "https://example.com/device"},
			httpClient: &http.Client{
				Transport: oidcRoundTripper(func(*http.Request) (*http.Response, error) {
					return nil, errors.New("dial failed")
				}),
				Timeout: time.Second,
			},
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDeviceStartPath, strings.NewReader(`{"client_id":"cid"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDeviceStart(rr, req)
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("handleDeviceStart() status = %d, want %d", rr.Code, http.StatusBadGateway)
		}
		var payload oidcDeviceStartResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "upstream_request_failed" {
			t.Fatalf("handleDeviceStart() payload = %+v", payload)
		}
	})

	t.Run("invalid_upstream_response", func(t *testing.T) {
		t.Parallel()

		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not-json"))
		}))
		defer upstream.Close()

		handler := &oidcHandler{
			cfg:        config.OIDCConfig{DeviceAuthEndpoint: upstream.URL},
			httpClient: upstream.Client(),
			log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDeviceStartPath, strings.NewReader(`{"client_id":"cid"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDeviceStart(rr, req)
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("handleDeviceStart() status = %d, want %d", rr.Code, http.StatusBadGateway)
		}
		var payload oidcDeviceStartResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "invalid_upstream_response" {
			t.Fatalf("handleDeviceStart() payload = %+v", payload)
		}
	})

	t.Run("upstream_error_status", func(t *testing.T) {
		t.Parallel()

		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"message":"bad grant"}`))
		}))
		defer upstream.Close()

		handler := &oidcHandler{
			cfg:        config.OIDCConfig{DeviceAuthEndpoint: upstream.URL},
			httpClient: upstream.Client(),
			log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDeviceStartPath, strings.NewReader(`{"client_id":"cid"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDeviceStart(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("handleDeviceStart() status = %d, want %d", rr.Code, http.StatusBadRequest)
		}
		var payload oidcDeviceStartResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "upstream_error" || payload.ErrorDescription != "bad grant" || payload.Message != "bad grant" {
			t.Fatalf("handleDeviceStart() payload = %+v", payload)
		}
	})

	t.Run("success_but_missing_required_fields", func(t *testing.T) {
		t.Parallel()

		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"device_code":"d1","user_code":"u1"}`))
		}))
		defer upstream.Close()

		handler := &oidcHandler{
			cfg:        config.OIDCConfig{DeviceAuthEndpoint: upstream.URL},
			httpClient: upstream.Client(),
			log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDeviceStartPath, strings.NewReader(`{"client_id":"cid"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDeviceStart(rr, req)
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("handleDeviceStart() status = %d, want %d", rr.Code, http.StatusBadGateway)
		}
		var payload oidcDeviceStartResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "invalid_upstream_response" || !strings.Contains(payload.Message, "missing device_code/user_code/verification_uri") {
			t.Fatalf("handleDeviceStart() payload = %+v", payload)
		}
	})

	t.Run("success_with_config_fallback", func(t *testing.T) {
		t.Parallel()

		var gotForm url.Values
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			gotForm, _ = url.ParseQuery(string(body))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"device_code":"d1","user_code":"u1","verification_uri":"https://verify.example.com"}`))
		}))
		defer upstream.Close()

		handler := &oidcHandler{
			cfg: config.OIDCConfig{
				ClientID:           "cfg-client",
				Audience:           "api://cfg",
				DeviceAuthEndpoint: upstream.URL,
			},
			httpClient: upstream.Client(),
			log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDeviceStartPath, strings.NewReader(`{"scope":"openid profile"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDeviceStart(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("handleDeviceStart() status = %d, want %d", rr.Code, http.StatusOK)
		}
		if gotForm.Get("client_id") != "cfg-client" || gotForm.Get("scope") != "openid profile" || gotForm.Get("audience") != "api://cfg" {
			t.Fatalf("handleDeviceStart() upstream form = %v", gotForm)
		}
		var payload oidcDeviceStartResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.DeviceCode != "d1" || payload.UserCode != "u1" || payload.VerificationURI != "https://verify.example.com" {
			t.Fatalf("handleDeviceStart() payload = %+v", payload)
		}
	})
}

func TestOIDCHandleDevicePoll(t *testing.T) {
	t.Parallel()

	t.Run("method_not_allowed", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodGet, oidcDevicePollPath, nil)
		rr := httptest.NewRecorder()

		handler.handleDevicePoll(rr, req)

		if rr.Code != http.StatusMethodNotAllowed {
			t.Fatalf("handleDevicePoll() status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
		}
		var payload oidcDevicePollResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "method_not_allowed" || payload.Status != "method_not_allowed" {
			t.Fatalf("handleDevicePoll() payload = %+v", payload)
		}
	})

	t.Run("endpoint_not_configured", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			cfg: config.OIDCConfig{},
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"client_id":"cid","device_code":"dc-1"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDevicePoll(rr, req)
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("handleDevicePoll() status = %d, want %d", rr.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("invalid_request_body", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			cfg: config.OIDCConfig{TokenEndpoint: "https://example.com/token"},
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"client_id":"cid"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDevicePoll(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("handleDevicePoll() status = %d, want %d", rr.Code, http.StatusBadRequest)
		}
		var payload oidcDevicePollResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "invalid_request" || !strings.Contains(payload.Message, "device_code is required") {
			t.Fatalf("handleDevicePoll() payload = %+v", payload)
		}
	})

	t.Run("missing_client_id", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			cfg: config.OIDCConfig{TokenEndpoint: "https://example.com/token"},
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"device_code":"dc-1"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDevicePoll(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("handleDevicePoll() status = %d, want %d", rr.Code, http.StatusBadRequest)
		}
		var payload oidcDevicePollResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "invalid_request" || !strings.Contains(payload.ErrorDescription, "client_id is required") {
			t.Fatalf("handleDevicePoll() payload = %+v", payload)
		}
	})

	t.Run("upstream_request_failed", func(t *testing.T) {
		t.Parallel()

		handler := &oidcHandler{
			cfg: config.OIDCConfig{TokenEndpoint: "https://example.com/token"},
			httpClient: &http.Client{
				Transport: oidcRoundTripper(func(*http.Request) (*http.Response, error) {
					return nil, errors.New("dial failed")
				}),
				Timeout: time.Second,
			},
			log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"client_id":"cid","device_code":"dc-1"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDevicePoll(rr, req)
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("handleDevicePoll() status = %d, want %d", rr.Code, http.StatusBadGateway)
		}
		var payload oidcDevicePollResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "upstream_request_failed" {
			t.Fatalf("handleDevicePoll() payload = %+v", payload)
		}
	})

	t.Run("invalid_upstream_response", func(t *testing.T) {
		t.Parallel()

		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("invalid-json"))
		}))
		defer upstream.Close()

		handler := &oidcHandler{
			cfg:        config.OIDCConfig{TokenEndpoint: upstream.URL},
			httpClient: upstream.Client(),
			log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"client_id":"cid","device_code":"dc-1"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDevicePoll(rr, req)
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("handleDevicePoll() status = %d, want %d", rr.Code, http.StatusBadGateway)
		}
		var payload oidcDevicePollResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "invalid_upstream_response" {
			t.Fatalf("handleDevicePoll() payload = %+v", payload)
		}
	})

	t.Run("upstream_error_status", func(t *testing.T) {
		t.Parallel()

		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"authorization pending"}`))
		}))
		defer upstream.Close()

		handler := &oidcHandler{
			cfg:        config.OIDCConfig{TokenEndpoint: upstream.URL},
			httpClient: upstream.Client(),
			log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"client_id":"cid","device_code":"dc-1"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDevicePoll(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("handleDevicePoll() status = %d, want %d", rr.Code, http.StatusUnauthorized)
		}
		var payload oidcDevicePollResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "upstream_error" || payload.Status != "upstream_error" || payload.Message != "authorization pending" {
			t.Fatalf("handleDevicePoll() payload = %+v", payload)
		}
	})

	t.Run("success_but_missing_expected_fields", func(t *testing.T) {
		t.Parallel()

		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"token_type":"Bearer"}`))
		}))
		defer upstream.Close()

		handler := &oidcHandler{
			cfg:        config.OIDCConfig{TokenEndpoint: upstream.URL},
			httpClient: upstream.Client(),
			log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"client_id":"cid","device_code":"dc-1"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDevicePoll(rr, req)
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("handleDevicePoll() status = %d, want %d", rr.Code, http.StatusBadGateway)
		}
		var payload oidcDevicePollResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Error != "invalid_upstream_response" || !strings.Contains(payload.Message, "missing access_token or status/error") {
			t.Fatalf("handleDevicePoll() payload = %+v", payload)
		}
	})

	t.Run("success_pending_status_without_access_token", func(t *testing.T) {
		t.Parallel()

		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			form, _ := url.ParseQuery(string(body))
			if form.Get("grant_type") != oidcDeviceCodeGrantType {
				t.Fatalf("grant_type = %q, want %q", form.Get("grant_type"), oidcDeviceCodeGrantType)
			}
			if form.Get("client_id") != "cfg-client" {
				t.Fatalf("client_id = %q, want %q", form.Get("client_id"), "cfg-client")
			}
			if form.Get("device_code") != "dc-1" {
				t.Fatalf("device_code = %q, want %q", form.Get("device_code"), "dc-1")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"authorization_pending","interval":5}`))
		}))
		defer upstream.Close()

		handler := &oidcHandler{
			cfg: config.OIDCConfig{
				ClientID:      "cfg-client",
				TokenEndpoint: upstream.URL,
			},
			httpClient: upstream.Client(),
			log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"device_code":"dc-1"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDevicePoll(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("handleDevicePoll() status = %d, want %d", rr.Code, http.StatusOK)
		}
		var payload oidcDevicePollResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.Status != "authorization_pending" || payload.Interval != 5 {
			t.Fatalf("handleDevicePoll() payload = %+v", payload)
		}
	})

	t.Run("success_access_token", func(t *testing.T) {
		t.Parallel()

		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"access_token":"at","token_type":"Bearer","expires_in":3600}`))
		}))
		defer upstream.Close()

		handler := &oidcHandler{
			cfg:        config.OIDCConfig{TokenEndpoint: upstream.URL},
			httpClient: upstream.Client(),
			log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		req := httptest.NewRequest(http.MethodPost, oidcDevicePollPath, strings.NewReader(`{"client_id":"cid","device_code":"dc-1"}`))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler.handleDevicePoll(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("handleDevicePoll() status = %d, want %d", rr.Code, http.StatusOK)
		}
		var payload oidcDevicePollResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &payload)
		if payload.AccessToken != "at" || payload.TokenType != "Bearer" || payload.ExpiresIn != 3600 {
			t.Fatalf("handleDevicePoll() payload = %+v", payload)
		}
	})
}

type oidcRoundTripper func(req *http.Request) (*http.Response, error)

func (rt oidcRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return rt(req)
}

type errorReadCloser struct {
	readErr error
}

func (r errorReadCloser) Read(_ []byte) (int, error) {
	if r.readErr == nil {
		return 0, io.EOF
	}
	return 0, r.readErr
}

func (r errorReadCloser) Close() error {
	return nil
}
