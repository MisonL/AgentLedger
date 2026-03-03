package main

import (
	"net/http"
	"strings"
	"testing"

	ingestionv1 "github.com/agentledger/agentledger/packages/gen/go/ingestion/v1"
)

func TestNormalizeGRPCEndpoint(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		want        string
		wantErr     bool
		errContains string
	}{
		{
			name:    "host port",
			raw:     "127.0.0.1:9091",
			want:    "127.0.0.1:9091",
			wantErr: false,
		},
		{
			name:    "grpcs url",
			raw:     "grpcs://ingest.example.com:9443",
			want:    "ingest.example.com:9443",
			wantErr: false,
		},
		{
			name:    "grpcs ipv6",
			raw:     "  grpcs://[::1]:9443 ",
			want:    "[::1]:9443",
			wantErr: false,
		},
		{
			name:        "http scheme rejected",
			raw:         "http://127.0.0.1:9091",
			wantErr:     true,
			errContains: "不支持 http://",
		},
		{
			name:        "https scheme rejected",
			raw:         "https://127.0.0.1:9091",
			wantErr:     true,
			errContains: "不支持 https://",
		},
		{
			name:        "grpc scheme rejected",
			raw:         "grpc://127.0.0.1:9091",
			wantErr:     true,
			errContains: "仅支持 grpcs://",
		},
		{
			name:        "missing port rejected",
			raw:         "ingest.example.com",
			wantErr:     true,
			errContains: "必须为 host:port",
		},
		{
			name:        "path rejected",
			raw:         "grpcs://ingest.example.com:9443/v1/ingest",
			wantErr:     true,
			errContains: "不能包含路径",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeGRPCEndpoint(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("normalizeGRPCEndpoint(%q) error=nil, want contains %q", tt.raw, tt.errContains)
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Fatalf("normalizeGRPCEndpoint(%q) error=%q, want contains %q", tt.raw, err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("normalizeGRPCEndpoint(%q) unexpected error: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("normalizeGRPCEndpoint(%q)=%q, want=%q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestValidateRunGRPCConfig(t *testing.T) {
	tests := []struct {
		name        string
		protocol    string
		config      grpcClientSecurityConfig
		wantErr     bool
		errContains string
	}{
		{
			name:     "http without grpc settings",
			protocol: "http",
			config:   grpcClientSecurityConfig{},
			wantErr:  false,
		},
		{
			name:     "http with grpc settings rejected",
			protocol: "http",
			config: grpcClientSecurityConfig{
				Plaintext: true,
			},
			wantErr:     true,
			errContains: "仅在 --protocol=grpc",
		},
		{
			name:     "grpc default tls",
			protocol: "grpc",
			config:   grpcClientSecurityConfig{},
			wantErr:  false,
		},
		{
			name:     "grpc plaintext",
			protocol: "grpc",
			config: grpcClientSecurityConfig{
				Plaintext: true,
			},
			wantErr: false,
		},
		{
			name:     "grpc plaintext with tls settings rejected",
			protocol: "grpc",
			config: grpcClientSecurityConfig{
				Plaintext: true,
				CAFile:    "ca.pem",
			},
			wantErr:     true,
			errContains: "不能同时设置 TLS 相关参数",
		},
		{
			name:     "grpc cert only rejected",
			protocol: "grpc",
			config: grpcClientSecurityConfig{
				CertFile: "client.pem",
			},
			wantErr:     true,
			errContains: "必须同时设置",
		},
		{
			name:     "grpc key only rejected",
			protocol: "grpc",
			config: grpcClientSecurityConfig{
				KeyFile: "client.key",
			},
			wantErr:     true,
			errContains: "必须同时设置",
		},
		{
			name:     "grpc mtls with pair",
			protocol: "grpc",
			config: grpcClientSecurityConfig{
				CertFile: "client.pem",
				KeyFile:  "client.key",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRunGRPCConfig(tt.protocol, tt.config)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("validateRunGRPCConfig(%q, %+v) error=nil, want contains %q", tt.protocol, tt.config, tt.errContains)
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Fatalf("validateRunGRPCConfig(%q, %+v) error=%q, want contains %q", tt.protocol, tt.config, err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("validateRunGRPCConfig(%q, %+v) unexpected error: %v", tt.protocol, tt.config, err)
			}
		})
	}
}

func TestBuildGRPCTransportCredentials_DefaultTLS(t *testing.T) {
	endpoint, err := parseGRPCEndpoint("ingest.example.com:9443")
	if err != nil {
		t.Fatalf("parseGRPCEndpoint() error: %v", err)
	}

	creds, err := buildGRPCTransportCredentials(endpoint, grpcClientSecurityConfig{})
	if err != nil {
		t.Fatalf("buildGRPCTransportCredentials() error: %v", err)
	}
	if creds == nil {
		t.Fatalf("buildGRPCTransportCredentials() creds=nil, want non-nil")
	}
}

func TestBuildGRPCTransportCredentials_RejectPlaintextWithGRPCS(t *testing.T) {
	endpoint, err := parseGRPCEndpoint("grpcs://ingest.example.com:9443")
	if err != nil {
		t.Fatalf("parseGRPCEndpoint() error: %v", err)
	}

	_, err = buildGRPCTransportCredentials(endpoint, grpcClientSecurityConfig{Plaintext: true})
	if err == nil {
		t.Fatalf("buildGRPCTransportCredentials() error=nil, want reject plaintext with grpcs://")
	}
	if !contains(err.Error(), "不能启用 grpc-plaintext") {
		t.Fatalf("buildGRPCTransportCredentials() error=%q, want contains %q", err.Error(), "不能启用 grpc-plaintext")
	}
}

func contains(input, fragment string) bool {
	return len(fragment) == 0 || strings.Contains(input, fragment)
}

func TestGRPCResponseStatus_BatchLevelErrorFirst(t *testing.T) {
	resp := &ingestionv1.PushBatchResponse{
		Accepted: 7,
		Rejected: 0,
		Errors: []*ingestionv1.PushBatchResponse_ErrorItem{
			{
				Index:   -1,
				Message: "batch validation failed",
			},
		},
	}

	got := grpcResponseStatus(resp)
	if got != http.StatusUnprocessableEntity {
		t.Fatalf("grpcResponseStatus()=%d, want=%d", got, http.StatusUnprocessableEntity)
	}
}

func TestGRPCResponseStatus_NilResponse(t *testing.T) {
	got := grpcResponseStatus(nil)
	if got != http.StatusBadGateway {
		t.Fatalf("grpcResponseStatus(nil)=%d, want=%d", got, http.StatusBadGateway)
	}
}

func TestGRPCResponseStatus_RejectedOnly(t *testing.T) {
	resp := &ingestionv1.PushBatchResponse{
		Accepted: 0,
		Rejected: 2,
		Errors: []*ingestionv1.PushBatchResponse_ErrorItem{
			{
				Index:   0,
				EventId: "evt-1",
				Message: "invalid payload",
			},
		},
	}

	got := grpcResponseStatus(resp)
	if got != http.StatusBadGateway {
		t.Fatalf("grpcResponseStatus()=%d, want=%d", got, http.StatusBadGateway)
	}
}

func TestGRPCResponseStatus_AcceptedWithoutErrors(t *testing.T) {
	resp := &ingestionv1.PushBatchResponse{
		Accepted: 3,
		Rejected: 0,
	}

	got := grpcResponseStatus(resp)
	if got != http.StatusAccepted {
		t.Fatalf("grpcResponseStatus()=%d, want=%d", got, http.StatusAccepted)
	}
}
