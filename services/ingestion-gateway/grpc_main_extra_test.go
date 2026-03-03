package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	ingestionv1 "github.com/agentledger/agentledger/packages/gen/go/ingestion/v1"
	"google.golang.org/protobuf/proto"
)

type mockNATSServer struct {
	listener net.Listener
	wg       sync.WaitGroup
}

func startMockNATSServer(t *testing.T) *mockNATSServer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("start mock nats listen failed: %v", err)
	}
	s := &mockNATSServer{listener: ln}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func(c net.Conn) {
				defer s.wg.Done()
				defer c.Close()
				handleMockNATSConn(c)
			}(conn)
		}
	}()
	t.Cleanup(func() {
		_ = s.listener.Close()
		s.wg.Wait()
	})
	return s
}

func (s *mockNATSServer) URL() string {
	return "nats://" + s.listener.Addr().String()
}

func handleMockNATSConn(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	replySID := "1"

	_, _ = writer.WriteString(`INFO {"server_id":"test","server_name":"test","version":"2.10.0","go":"go1.24","host":"127.0.0.1","port":4222,"max_payload":1048576}` + "\r\n")
	_ = writer.Flush()

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		switch {
		case strings.HasPrefix(line, "PING"):
			_, _ = writer.WriteString("PONG\r\n")
			_ = writer.Flush()
		case strings.HasPrefix(line, "SUB "):
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				replySID = parts[len(parts)-1]
			}
		case strings.HasPrefix(line, "PUB "):
			parts := strings.Fields(line)
			if len(parts) < 3 {
				continue
			}
			subject := parts[1]
			var reply string
			var sizeRaw string
			if len(parts) == 4 {
				reply = parts[2]
				sizeRaw = parts[3]
			} else {
				sizeRaw = parts[2]
			}

			size, err := strconv.Atoi(sizeRaw)
			if err != nil || size < 0 {
				continue
			}
			payload := make([]byte, size+2)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return
			}
			if reply == "" {
				continue
			}

			resp := `{"type":"io.nats.jetstream.api.v1.stream_info_response","config":{"name":"S","subjects":["s"]},"state":{"messages":0,"bytes":0}}`
			if strings.HasPrefix(subject, "$JS.API.STREAM.UPDATE.") {
				resp = `{"type":"io.nats.jetstream.api.v1.stream_update_response","config":{"name":"S","subjects":["s"]},"state":{"messages":0,"bytes":0}}`
			}
			if strings.HasPrefix(subject, "$JS.API.STREAM.CREATE.") {
				resp = `{"type":"io.nats.jetstream.api.v1.stream_create_response","config":{"name":"S","subjects":["s"]},"state":{"messages":0,"bytes":0}}`
			}

			_, _ = writer.WriteString("MSG " + reply + " " + replySID + " " + strconv.Itoa(len(resp)) + "\r\n")
			_, _ = writer.WriteString(resp + "\r\n")
			_ = writer.Flush()
		}
	}
}

func writeSelfSignedCertPair(t *testing.T) (certFile string, keyFile string) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() failed: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("x509.CreateCertificate() failed: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})

	certPath := t.TempDir() + "/server.crt"
	keyPath := t.TempDir() + "/server.key"
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert file failed: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key file failed: %v", err)
	}
	return certPath, keyPath
}

func runMainInSubprocess(t *testing.T, extraEnv ...string) {
	t.Helper()

	if os.Getenv("INGESTION_GATEWAY_MAIN_HELPER") == "1" {
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestMainInSubprocess$")
	cmd.Env = append(os.Environ(), "INGESTION_GATEWAY_MAIN_HELPER=1")
	cmd.Env = append(cmd.Env, extraEnv...)
	err := cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("subprocess should exit with os.Exit, got err=%v", err)
	}
	if exitErr.ExitCode() != 1 {
		t.Fatalf("subprocess exit code = %d, want 1", exitErr.ExitCode())
	}
}

func TestMainInSubprocess(t *testing.T) {
	if os.Getenv("INGESTION_GATEWAY_MAIN_HELPER") == "1" {
		main()
		return
	}

	t.Run("config_load_failed", func(t *testing.T) {
		runMainInSubprocess(t, "PG_MAX_CONNS=bad")
	})
	t.Run("jetstream_init_failed", func(t *testing.T) {
		runMainInSubprocess(t, "NATS_URL=://invalid")
	})
	t.Run("jwt_authenticator_init_failed_after_streams_ok", func(t *testing.T) {
		mockNATS := startMockNATSServer(t)
		runMainInSubprocess(t,
			"NATS_URL="+mockNATS.URL(),
			"GRPC_TLS_ENABLED=false",
			"OIDC_ISSUER=",
			"OIDC_AUDIENCE=",
			"OIDC_JWKS_URI=",
		)
	})
}

func TestResolveGRPCAddrAndMode(t *testing.T) {
	t.Setenv("GRPC_ADDR", "")
	if got := resolveGRPCAddr(); got != defaultGRPCAddr {
		t.Fatalf("resolveGRPCAddr() = %q, want %q", got, defaultGRPCAddr)
	}

	t.Setenv("GRPC_ADDR", " 127.0.0.1:19091 ")
	if got := resolveGRPCAddr(); got != "127.0.0.1:19091" {
		t.Fatalf("resolveGRPCAddr() = %q, want %q", got, "127.0.0.1:19091")
	}

	if got := grpcTransportMode(grpcTLSConfig{TLSEnabled: false}); got != "plaintext" {
		t.Fatalf("grpcTransportMode(plaintext) = %q", got)
	}
	if got := grpcTransportMode(grpcTLSConfig{TLSEnabled: true, MTLSEnabled: false}); got != "tls" {
		t.Fatalf("grpcTransportMode(tls) = %q", got)
	}
	if got := grpcTransportMode(grpcTLSConfig{TLSEnabled: true, MTLSEnabled: true}); got != "mtls" {
		t.Fatalf("grpcTransportMode(mtls) = %q", got)
	}
}

func TestGRPCServerOptionsFromTLSConfig(t *testing.T) {
	opts, err := grpcServerOptionsFromTLSConfig(grpcTLSConfig{TLSEnabled: false})
	if err != nil {
		t.Fatalf("grpcServerOptionsFromTLSConfig(plaintext) unexpected error: %v", err)
	}
	if opts != nil {
		t.Fatalf("grpcServerOptionsFromTLSConfig(plaintext) opts = %#v, want nil", opts)
	}

	_, err = grpcServerOptionsFromTLSConfig(grpcTLSConfig{
		TLSEnabled: true,
		CertFile:   "/not/exist.crt",
		KeyFile:    "/not/exist.key",
	})
	if err == nil || !strings.Contains(err.Error(), "load server cert/key") {
		t.Fatalf("grpcServerOptionsFromTLSConfig(invalid paths) error = %v, want load server cert/key", err)
	}

	certFile, keyFile := writeSelfSignedCertPair(t)
	opts, err = grpcServerOptionsFromTLSConfig(grpcTLSConfig{
		TLSEnabled: true,
		CertFile:   certFile,
		KeyFile:    keyFile,
	})
	if err != nil {
		t.Fatalf("grpcServerOptionsFromTLSConfig(valid tls) unexpected error: %v", err)
	}
	if len(opts) != 1 {
		t.Fatalf("grpcServerOptionsFromTLSConfig(valid tls) opts len = %d, want 1", len(opts))
	}

	invalidCAPath := t.TempDir() + "/bad-ca.pem"
	if err := os.WriteFile(invalidCAPath, []byte("not-pem"), 0o600); err != nil {
		t.Fatalf("write invalid ca failed: %v", err)
	}
	_, err = grpcServerOptionsFromTLSConfig(grpcTLSConfig{
		TLSEnabled:   true,
		CertFile:     certFile,
		KeyFile:      keyFile,
		MTLSEnabled:  true,
		ClientCAFile: invalidCAPath,
	})
	if err == nil || !strings.Contains(err.Error(), "parse client ca pem") {
		t.Fatalf("grpcServerOptionsFromTLSConfig(invalid ca) error = %v, want parse client ca pem", err)
	}
}

func TestGRPCServerOptionsFromEnvAndStartServer(t *testing.T) {
	t.Setenv(envGRPCTLSEnabled, "false")
	t.Setenv(envGRPCMTLSEnabled, "")
	t.Setenv(envGRPCTLSCertFile, "")
	t.Setenv(envGRPCTLSKeyFile, "")
	t.Setenv(envGRPCTLSClientCAFile, "")

	opts, mode, err := grpcServerOptionsFromEnv()
	if err != nil {
		t.Fatalf("grpcServerOptionsFromEnv() unexpected error: %v", err)
	}
	if len(opts) != 0 || mode != "plaintext" {
		t.Fatalf("grpcServerOptionsFromEnv() = (opts=%d, mode=%q), want (0, plaintext)", len(opts), mode)
	}

	ctx, cancel := context.WithCancel(context.Background())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := &ingestHandler{
		js:   &flakyPublisher{},
		auth: nil,
		log:  log,
	}

	errCh, err := startGRPCServer(ctx, "127.0.0.1:0", handler, log)
	if err != nil {
		cancel()
		t.Fatalf("startGRPCServer() unexpected error: %v", err)
	}

	cancel()
	select {
	case _, ok := <-errCh:
		if ok {
			t.Fatalf("startGRPCServer() error channel should close without value")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("startGRPCServer() did not stop in time")
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	_, err = startGRPCServer(ctx2, "bad::addr", handler, log)
	if err == nil || !strings.Contains(err.Error(), "listen grpc addr") {
		t.Fatalf("startGRPCServer(invalid addr) error = %v, want listen grpc addr", err)
	}
}

func TestBatchFromProtoResponseAndClone(t *testing.T) {
	request := &ingestionv1.PushBatchRequest{
		BatchId:  "batch-1",
		Metadata: map[string]string{"k1": "v1"},
		SentAt:   "2026-03-03T10:00:00Z",
		Agent: &ingestionv1.PushBatchRequest_AgentInfo{
			AgentId:     "agent-1",
			TenantId:    "tenant-1",
			WorkspaceId: "workspace-1",
			Hostname:    "host-1",
			Version:     "v1",
		},
		Source: &ingestionv1.PushBatchRequest_SourceInfo{
			SourceId:   "source-1",
			Provider:   "ssh",
			SourceType: "agent",
		},
		Events: []*ingestionv1.PushBatchRequest_Event{
			{
				EventId:      "evt-1",
				SessionId:    "sess-1",
				EventType:    "message",
				Role:         "assistant",
				Text:         "hello",
				Model:        "gpt",
				OccurredAt:   "2026-03-03T10:00:00Z",
				Tokens:       &ingestionv1.PushBatchRequest_TokenUsage{InputTokens: 10, OutputTokens: 20},
				CostUsd:      proto.Float64(1.23),
				CostMode:     "reported",
				SourcePath:   "/tmp/chat.log",
				SourceOffset: proto.Int64(12),
				Metadata:     map[string]string{"m": "1"},
				Payload:      []byte(`{"text":"hello"}`),
			},
		},
	}
	batch, err := batchFromProto(request)
	if err != nil {
		t.Fatalf("batchFromProto() unexpected error: %v", err)
	}
	if batch.BatchID != "batch-1" || batch.Agent.AgentID != "agent-1" || len(batch.Events) != 1 {
		t.Fatalf("batchFromProto() batch invalid: %+v", batch)
	}
	if batch.Events[0].CostUSD == nil || *batch.Events[0].CostUSD != 1.23 {
		t.Fatalf("batchFromProto() cost_usd not mapped: %+v", batch.Events[0].CostUSD)
	}
	if batch.Events[0].SourceOffset == nil || *batch.Events[0].SourceOffset != 12 {
		t.Fatalf("batchFromProto() source_offset not mapped: %+v", batch.Events[0].SourceOffset)
	}

	_, err = batchFromProto(&ingestionv1.PushBatchRequest{
		Events: []*ingestionv1.PushBatchRequest_Event{
			{Payload: []byte("not-json")},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("batchFromProto(invalid payload) error = %v, want payload invalid", err)
	}

	resp := responseToProto(ingestResponse{
		BatchID:    "batch-1",
		Accepted:   2,
		Rejected:   1,
		DurationMS: 321,
		Errors: []ingestErrorItem{
			{Index: 0, EventID: "evt-1", Message: "bad event"},
		},
	})
	if resp.GetBatchId() != "batch-1" || resp.GetAccepted() != 2 || len(resp.GetErrors()) != 1 {
		t.Fatalf("responseToProto() invalid response: %+v", resp)
	}

	if out := responseToProto(ingestResponse{}); out.Errors != nil {
		t.Fatalf("responseToProto(empty).Errors = %#v, want nil", out.Errors)
	}

	if clone := cloneStringMap(nil); clone != nil {
		t.Fatalf("cloneStringMap(nil) = %#v, want nil", clone)
	}
	input := map[string]string{"a": "1"}
	cloned := cloneStringMap(input)
	input["a"] = "2"
	if cloned["a"] != "1" {
		t.Fatalf("cloneStringMap() should deep copy map, got %q", cloned["a"])
	}
}

func TestDecodeBatchRequestAdditional(t *testing.T) {
	t.Run("unknown_field", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{"batch_id":"b1","unknown":1}`))
		req.Header.Set("Content-Type", "application/json")

		_, err := decodeBatchRequest(rec, req)
		if err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("decodeBatchRequest() error = %v, want unknown field", err)
		}
	})

	t.Run("nested_unknown_fields", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(
			http.MethodPost,
			"/v1/ingest",
			strings.NewReader(`{
					"batch_id":"b2",
					"agent":{"agent_id":"agent-1","extra":"x"},
					"source":{"source_id":"source-1","provider":"codex","extra":"y"},
					"events":[{"session_id":"s1","event_type":"message","text":"hello","extra":"z"}],
					"metadata":{"k":"v"}
				}`),
		)
		req.Header.Set("Content-Type", "application/json")

		_, err := decodeBatchRequest(rec, req)
		if err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("decodeBatchRequest() error = %v, want unknown field", err)
		}
	})

	t.Run("request_body_too_large", func(t *testing.T) {
		rec := httptest.NewRecorder()
		large := strings.Repeat("a", maxIngestBodyBytes+1)
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(large))
		req.Header.Set("Content-Type", "application/json")

		_, err := decodeBatchRequest(rec, req)
		if err == nil || !strings.Contains(err.Error(), "request body too large") {
			t.Fatalf("decodeBatchRequest() error = %v, want request body too large", err)
		}
	})
}

func TestNewIngestHandler(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := newIngestHandler(nil, nil, log)
	if handler == nil {
		t.Fatalf("newIngestHandler() returned nil")
	}
	if handler.log != log {
		t.Fatalf("newIngestHandler() did not keep logger reference")
	}
}
