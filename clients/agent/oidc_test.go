package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartOIDCDeviceFlow(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var gotMethod string
		var gotPath string
		var gotPayload oidcDeviceStartRequest
		var decodeErr error

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotPath = r.URL.Path
			decodeErr = json.NewDecoder(r.Body).Decode(&gotPayload)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"device_code":"dev-1","user_code":"user-1","verification_uri":"https://id.example/verify","verification_uri_complete":"https://id.example/verify?code=user-1","expires_in":900,"interval":5,"message":"please continue"}`)
		}))
		defer server.Close()

		resp, err := startOIDCDeviceFlow(context.Background(), server.URL, oidcDeviceStartRequest{
			ClientID: "agent-cli",
			Scope:    "openid profile",
			Audience: "ingestion-api",
		}, 2*time.Second)
		if err != nil {
			t.Fatalf("startOIDCDeviceFlow() error: %v", err)
		}
		if decodeErr != nil {
			t.Fatalf("request decode error: %v", decodeErr)
		}
		if gotMethod != http.MethodPost {
			t.Fatalf("method=%q, want=%q", gotMethod, http.MethodPost)
		}
		if gotPath != oidcDeviceStartPath {
			t.Fatalf("path=%q, want=%q", gotPath, oidcDeviceStartPath)
		}
		if gotPayload.ClientID != "agent-cli" || gotPayload.Scope != "openid profile" || gotPayload.Audience != "ingestion-api" {
			t.Fatalf("unexpected payload: %+v", gotPayload)
		}
		if resp.DeviceCode != "dev-1" || resp.UserCode != "user-1" || resp.VerificationURI != "https://id.example/verify" {
			t.Fatalf("unexpected response: %+v", *resp)
		}
	})

	tests := []struct {
		name        string
		status      int
		body        string
		errContains string
	}{
		{
			name:        "http error with code",
			status:      http.StatusBadRequest,
			body:        `{"error":"invalid_client","error_description":"invalid client id"}`,
			errContains: "HTTP 400, code=invalid_client",
		},
		{
			name:        "invalid json body",
			status:      http.StatusOK,
			body:        `{"device_code":`,
			errContains: "解析启动登录响应失败",
		},
		{
			name:        "missing required fields",
			status:      http.StatusOK,
			body:        `{"device_code":"dev-1","verification_uri":"https://id.example/verify"}`,
			errContains: "启动响应缺少必要字段",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer server.Close()

			_, err := startOIDCDeviceFlow(context.Background(), server.URL, oidcDeviceStartRequest{ClientID: "agent-cli"}, time.Second)
			if err == nil {
				t.Fatalf("startOIDCDeviceFlow() error=nil, want contains %q", tt.errContains)
			}
			if !strings.Contains(err.Error(), tt.errContains) {
				t.Fatalf("startOIDCDeviceFlow() error=%q, want contains %q", err.Error(), tt.errContains)
			}
		})
	}
}

func TestPollOIDCTokenOnce(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		body         string
		wantState    oidcPollState
		wantErr      bool
		errContains  string
		wantRespNil  bool
		wantAccess   string
		wantInterval int64
	}{
		{
			name:         "ready with access token",
			status:       http.StatusOK,
			body:         `{"access_token":"token-ok","token_type":"Bearer","expires_in":300}`,
			wantState:    oidcPollStateReady,
			wantRespNil:  false,
			wantAccess:   "token-ok",
			wantInterval: 0,
		},
		{
			name:         "authorization pending",
			status:       http.StatusBadRequest,
			body:         `{"error":"authorization_pending"}`,
			wantState:    oidcPollStatePending,
			wantRespNil:  false,
			wantAccess:   "",
			wantInterval: 0,
		},
		{
			name:         "pending from status alias",
			status:       http.StatusOK,
			body:         `{"status":"waiting"}`,
			wantState:    oidcPollStatePending,
			wantRespNil:  false,
			wantAccess:   "",
			wantInterval: 0,
		},
		{
			name:         "slow down",
			status:       http.StatusBadRequest,
			body:         `{"error":"slow_down","interval":3}`,
			wantState:    oidcPollStateSlowDown,
			wantRespNil:  false,
			wantAccess:   "",
			wantInterval: 3,
		},
		{
			name:        "access denied",
			status:      http.StatusBadRequest,
			body:        `{"error":"access_denied"}`,
			wantState:   oidcPollStatePending,
			wantErr:     true,
			errContains: "授权被拒绝",
			wantRespNil: true,
		},
		{
			name:        "device code expired",
			status:      http.StatusBadRequest,
			body:        `{"error":"expired_device_code"}`,
			wantState:   oidcPollStatePending,
			wantErr:     true,
			errContains: "设备码已过期",
			wantRespNil: true,
		},
		{
			name:        "http error unknown code",
			status:      http.StatusInternalServerError,
			body:        `{"message":"upstream failed"}`,
			wantState:   oidcPollStatePending,
			wantErr:     true,
			errContains: "HTTP 500, code=unknown",
			wantRespNil: true,
		},
		{
			name:        "2xx without access token",
			status:      http.StatusOK,
			body:        `{"status":"ok"}`,
			wantState:   oidcPollStatePending,
			wantErr:     true,
			errContains: "轮询响应未包含 access_token",
			wantRespNil: true,
		},
		{
			name:        "invalid json",
			status:      http.StatusOK,
			body:        `{"access_token":`,
			wantState:   oidcPollStatePending,
			wantErr:     true,
			errContains: "解析轮询响应失败",
			wantRespNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer server.Close()

			resp, state, err := pollOIDCTokenOnce(context.Background(), server.URL, oidcDevicePollRequest{
				ClientID:   "agent-cli",
				DeviceCode: "device-1",
			}, time.Second)

			if state != tt.wantState {
				t.Fatalf("state=%v, want=%v", state, tt.wantState)
			}

			if tt.wantErr {
				if err == nil {
					t.Fatalf("pollOIDCTokenOnce() error=nil, want contains %q", tt.errContains)
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("pollOIDCTokenOnce() error=%q, want contains %q", err.Error(), tt.errContains)
				}
			} else if err != nil {
				t.Fatalf("pollOIDCTokenOnce() unexpected error: %v", err)
			}

			if tt.wantRespNil {
				if resp != nil {
					t.Fatalf("response=%+v, want nil", *resp)
				}
				return
			}
			if resp == nil {
				t.Fatalf("response=nil, want non-nil")
			}
			if tt.wantAccess != "" && resp.AccessToken != tt.wantAccess {
				t.Fatalf("access_token=%q, want=%q", resp.AccessToken, tt.wantAccess)
			}
			if tt.wantInterval > 0 && resp.Interval != tt.wantInterval {
				t.Fatalf("interval=%d, want=%d", resp.Interval, tt.wantInterval)
			}
		})
	}
}

func TestPollOIDCToken(t *testing.T) {
	t.Run("pending then slow_down then success", func(t *testing.T) {
		var calls int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seq := atomic.AddInt32(&calls, 1)
			w.Header().Set("Content-Type", "application/json")
			switch seq {
			case 1:
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":"authorization_pending"}`)
			case 2:
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":"slow_down"}`)
			default:
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, `{"access_token":"token-final","token_type":"Bearer","expires_in":300}`)
			}
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := pollOIDCToken(ctx, server.URL, "agent-cli", "device-1", time.Millisecond, time.Second)
		if err != nil {
			t.Fatalf("pollOIDCToken() error: %v", err)
		}
		if resp == nil {
			t.Fatalf("pollOIDCToken() response=nil, want non-nil")
		}
		if resp.AccessToken != "token-final" {
			t.Fatalf("access_token=%q, want=%q", resp.AccessToken, "token-final")
		}
		if got := atomic.LoadInt32(&calls); got != 3 {
			t.Fatalf("poll calls=%d, want=3", got)
		}
	})

	t.Run("context timeout", func(t *testing.T) {
		var calls int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&calls, 1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":"authorization_pending"}`)
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		resp, err := pollOIDCToken(ctx, server.URL, "agent-cli", "device-1", 5*time.Millisecond, time.Second)
		if err == nil {
			t.Fatalf("pollOIDCToken() error=nil, want timeout")
		}
		if !strings.Contains(err.Error(), "操作超时") {
			t.Fatalf("pollOIDCToken() error=%q, want contains %q", err.Error(), "操作超时")
		}
		if resp != nil {
			t.Fatalf("pollOIDCToken() response=%+v, want nil", *resp)
		}
		if got := atomic.LoadInt32(&calls); got < 1 {
			t.Fatalf("poll calls=%d, want >= 1", got)
		}
	})

	t.Run("fail fast when access denied", func(t *testing.T) {
		var calls int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&calls, 1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":"access_denied"}`)
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := pollOIDCToken(ctx, server.URL, "agent-cli", "device-1", time.Millisecond, time.Second)
		if err == nil {
			t.Fatalf("pollOIDCToken() error=nil, want access denied")
		}
		if !strings.Contains(err.Error(), "授权被拒绝") {
			t.Fatalf("pollOIDCToken() error=%q, want contains %q", err.Error(), "授权被拒绝")
		}
		if resp != nil {
			t.Fatalf("pollOIDCToken() response=%+v, want nil", *resp)
		}
		if got := atomic.LoadInt32(&calls); got != 1 {
			t.Fatalf("poll calls=%d, want=1", got)
		}
	})
}

func TestBuildLocalTokenFromOIDC(t *testing.T) {
	resp := oidcDevicePollResponse{
		AccessToken:  "  access-token  ",
		TokenType:    "  ",
		RefreshToken: "  refresh-token ",
		IDToken:      "  id-token ",
		Scope:        "  openid profile ",
		ExpiresIn:    2,
	}

	token := buildLocalTokenFromOIDC(resp)

	if token.AccessToken != "access-token" {
		t.Fatalf("access_token=%q, want=%q", token.AccessToken, "access-token")
	}
	if token.TokenType != "Bearer" {
		t.Fatalf("token_type=%q, want=%q", token.TokenType, "Bearer")
	}
	if token.RefreshToken != "refresh-token" {
		t.Fatalf("refresh_token=%q, want=%q", token.RefreshToken, "refresh-token")
	}
	if token.IDToken != "id-token" {
		t.Fatalf("id_token=%q, want=%q", token.IDToken, "id-token")
	}
	if token.Scope != "openid profile" {
		t.Fatalf("scope=%q, want=%q", token.Scope, "openid profile")
	}
	if token.ExpiresAt == "" {
		t.Fatalf("expires_at is empty, want non-empty")
	}
	if token.ObtainedAt == "" {
		t.Fatalf("obtained_at is empty, want non-empty")
	}
}
