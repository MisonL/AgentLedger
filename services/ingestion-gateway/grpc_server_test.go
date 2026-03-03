package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"testing"

	ingestionv1 "github.com/agentledger/agentledger/packages/gen/go/ingestion/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestResolveGRPCTLSConfigFromEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		env             map[string]string
		want            grpcTLSConfig
		wantErrContains string
	}{
		{
			name:            "default_tls_enabled_requires_cert",
			env:             map[string]string{},
			wantErrContains: envGRPCTLSCertFile,
		},
		{
			name: "tls_enabled_missing_key",
			env: map[string]string{
				envGRPCTLSCertFile: "/etc/certs/server.crt",
			},
			wantErrContains: envGRPCTLSKeyFile,
		},
		{
			name: "tls_disabled_allows_missing_paths",
			env: map[string]string{
				envGRPCTLSEnabled: "false",
			},
			want: grpcTLSConfig{
				TLSEnabled:  false,
				MTLSEnabled: false,
			},
		},
		{
			name: "tls_enabled_with_cert_and_key",
			env: map[string]string{
				envGRPCTLSCertFile: " /etc/certs/server.crt ",
				envGRPCTLSKeyFile:  " /etc/certs/server.key ",
			},
			want: grpcTLSConfig{
				TLSEnabled:  true,
				CertFile:    "/etc/certs/server.crt",
				KeyFile:     "/etc/certs/server.key",
				MTLSEnabled: false,
			},
		},
		{
			name: "mtls_enabled_requires_client_ca",
			env: map[string]string{
				envGRPCTLSCertFile: "/etc/certs/server.crt",
				envGRPCTLSKeyFile:  "/etc/certs/server.key",
				envGRPCMTLSEnabled: "true",
			},
			wantErrContains: envGRPCTLSClientCAFile,
		},
		{
			name: "mtls_enabled_with_client_ca",
			env: map[string]string{
				envGRPCTLSCertFile:     "/etc/certs/server.crt",
				envGRPCTLSKeyFile:      "/etc/certs/server.key",
				envGRPCMTLSEnabled:     "true",
				envGRPCTLSClientCAFile: "/etc/certs/ca.crt",
			},
			want: grpcTLSConfig{
				TLSEnabled:   true,
				CertFile:     "/etc/certs/server.crt",
				KeyFile:      "/etc/certs/server.key",
				MTLSEnabled:  true,
				ClientCAFile: "/etc/certs/ca.crt",
			},
		},
		{
			name: "mtls_cannot_enable_when_tls_disabled",
			env: map[string]string{
				envGRPCTLSEnabled:  "false",
				envGRPCMTLSEnabled: "true",
			},
			wantErrContains: envGRPCTLSEnabled,
		},
		{
			name: "invalid_tls_enabled_bool",
			env: map[string]string{
				envGRPCTLSEnabled: "definitely-not-bool",
			},
			wantErrContains: envGRPCTLSEnabled,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveGRPCTLSConfigFromEnv(mapEnvGetter(tt.env))
			if tt.wantErrContains != "" {
				if err == nil {
					t.Fatalf("resolveGRPCTLSConfigFromEnv() error = nil, want contains %q", tt.wantErrContains)
				}
				if !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Fatalf("resolveGRPCTLSConfigFromEnv() error = %q, want contains %q", err.Error(), tt.wantErrContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("resolveGRPCTLSConfigFromEnv() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("resolveGRPCTLSConfigFromEnv() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func mapEnvGetter(values map[string]string) func(string) string {
	return func(key string) string {
		return values[key]
	}
}

func TestTransportSecurityFromGRPCContext(t *testing.T) {
	t.Parallel()

	t.Run("plaintext", func(t *testing.T) {
		t.Parallel()

		got := transportSecurityFromGRPCContext(context.Background())
		if got.TLSMode != tlsModePlaintext {
			t.Fatalf("TLSMode = %q, want %q", got.TLSMode, tlsModePlaintext)
		}
		if got.MTLSVerified {
			t.Fatalf("MTLSVerified = true, want false")
		}
	})

	t.Run("tls_without_mtls", func(t *testing.T) {
		t.Parallel()

		ctx := peer.NewContext(context.Background(), &peer.Peer{
			AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{}},
		})
		got := transportSecurityFromGRPCContext(ctx)
		if got.TLSMode != tlsModeTLS {
			t.Fatalf("TLSMode = %q, want %q", got.TLSMode, tlsModeTLS)
		}
		if got.MTLSVerified {
			t.Fatalf("MTLSVerified = true, want false")
		}
	})

	t.Run("mtls_verified", func(t *testing.T) {
		t.Parallel()

		ctx := peer.NewContext(context.Background(), &peer.Peer{
			AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{&x509.Certificate{}},
				VerifiedChains:   [][]*x509.Certificate{{&x509.Certificate{}}},
			}},
		})
		got := transportSecurityFromGRPCContext(ctx)
		if got.TLSMode != tlsModeMTLS {
			t.Fatalf("TLSMode = %q, want %q", got.TLSMode, tlsModeMTLS)
		}
		if !got.MTLSVerified {
			t.Fatalf("MTLSVerified = false, want true")
		}
	})
}

func TestIngestGRPCServerPushBatch_PublishAuthAuditOnAuthFailure(t *testing.T) {
	t.Parallel()

	publisher := &testPublisher{}
	server := &ingestGRPCServer{
		handler: &ingestHandler{
			js:   publisher,
			auth: nil,
			log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50051},
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{&x509.Certificate{}},
				VerifiedChains:   [][]*x509.Certificate{{&x509.Certificate{}}},
			},
		},
	})

	_, err := server.PushBatch(ctx, &ingestionv1.PushBatchRequest{BatchId: "batch-1"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("PushBatch() grpc code = %v, want %v", status.Code(err), codes.Unauthenticated)
	}

	messages := publisher.Messages()
	if len(messages) != 1 {
		t.Fatalf("published audit message count = %d, want 1", len(messages))
	}
	if messages[0].Subject != authAuditSubject {
		t.Fatalf("published subject = %q, want %q", messages[0].Subject, authAuditSubject)
	}

	var event authAuditEvent
	if err := json.Unmarshal(messages[0].Payload, &event); err != nil {
		t.Fatalf("unmarshal auth audit event failed: %v", err)
	}
	if event.Protocol != protocolGRPC {
		t.Fatalf("event.Protocol = %q, want %q", event.Protocol, protocolGRPC)
	}
	if event.TLSMode != tlsModeMTLS {
		t.Fatalf("event.TLSMode = %q, want %q", event.TLSMode, tlsModeMTLS)
	}
	if !event.MTLSVerified {
		t.Fatalf("event.MTLSVerified = false, want true")
	}
	if strings.TrimSpace(event.FailureReason) == "" {
		t.Fatalf("event.FailureReason should not be empty")
	}
}
