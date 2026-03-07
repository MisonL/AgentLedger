package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ingestionv1 "github.com/agentledger/agentledger/packages/gen/go/ingestion/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type grpcMTLSTestIngestServer struct {
	ingestionv1.UnimplementedIngestServiceServer
	t *testing.T
}

func (s *grpcMTLSTestIngestServer) PushBatch(
	ctx context.Context,
	req *ingestionv1.PushBatchRequest,
) (*ingestionv1.PushBatchResponse, error) {
	s.t.Helper()

	if req.GetBatchId() != "batch-mtls-e2e" {
		s.t.Fatalf("batch id = %q, want %q", req.GetBatchId(), "batch-mtls-e2e")
	}
	if len(req.GetEvents()) != 1 {
		s.t.Fatalf("events len = %d, want 1", len(req.GetEvents()))
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		s.t.Fatal("metadata.FromIncomingContext() = false, want true")
	}
	if got := firstMetadataValue(md, "authorization"); got != "Bearer mtls-token" {
		s.t.Fatalf("authorization metadata = %q, want %q", got, "Bearer mtls-token")
	}

	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		s.t.Fatal("peer.FromContext() = false, want true")
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok {
		s.t.Fatalf("peer auth info = %T, want credentials.TLSInfo", peerInfo.AuthInfo)
	}
	if len(tlsInfo.State.VerifiedChains) == 0 {
		s.t.Fatal("verified chains is empty, want client cert to be verified")
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		s.t.Fatal("peer certificates is empty, want client certificate")
	}
	if got := tlsInfo.State.PeerCertificates[0].Subject.CommonName; got != "agent-mtls-client" {
		s.t.Fatalf("client certificate CN = %q, want %q", got, "agent-mtls-client")
	}

	return &ingestionv1.PushBatchResponse{
		Accepted: 1,
		Rejected: 0,
	}, nil
}

func TestSendIngestRequestGRPC_MTLSEndToEnd(t *testing.T) {
	certs := writeGRPCMTLSTestCertificates(t)

	serverCert, err := tls.LoadX509KeyPair(certs.serverCertFile, certs.serverKeyFile)
	if err != nil {
		t.Fatalf("tls.LoadX509KeyPair(server) error: %v", err)
	}
	clientCAPEM, err := os.ReadFile(certs.caCertFile)
	if err != nil {
		t.Fatalf("os.ReadFile(ca) error: %v", err)
	}
	clientCAPool := x509.NewCertPool()
	if !clientCAPool.AppendCertsFromPEM(clientCAPEM) {
		t.Fatal("AppendCertsFromPEM(ca) = false, want true")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAPool,
	})))
	ingestionv1.RegisterIngestServiceServer(
		grpcServer,
		&grpcMTLSTestIngestServer{t: t},
	)

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		select {
		case err := <-serverErrCh:
			if err != nil &&
				err != grpc.ErrServerStopped &&
				!strings.Contains(err.Error(), "use of closed network connection") {
				t.Fatalf("grpc server serve error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("grpc server stop timeout")
		}
	})

	statusCode, responseBody, err := sendIngestRequestGRPC(
		listener.Addr().String(),
		3*time.Second,
		"Bearer mtls-token",
		ingestBatchRequest{
			BatchID: "batch-mtls-e2e",
			Events: []agentEvent{
				{
					EventID:    "evt-mtls-e2e",
					SessionID:  "session-mtls-e2e",
					EventType:  "message",
					Role:       "user",
					Text:       "hello mtls",
					OccurredAt: "2026-03-07T00:00:00.000Z",
				},
			},
		},
		grpcClientSecurityConfig{
			CAFile:   certs.caCertFile,
			CertFile: certs.clientCertFile,
			KeyFile:  certs.clientKeyFile,
		},
	)
	if err != nil {
		t.Fatalf("sendIngestRequestGRPC() error: %v", err)
	}
	if statusCode != 202 {
		t.Fatalf("statusCode = %d, want 202", statusCode)
	}

	var response ingestBatchResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		t.Fatalf("json.Unmarshal(responseBody) error: %v, body=%s", err, string(responseBody))
	}
	if response.Accepted != 1 || response.Rejected != 0 {
		t.Fatalf("response = %+v, want accepted=1 rejected=0", response)
	}
}

type grpcMTLSTestCertFiles struct {
	caCertFile     string
	serverCertFile string
	serverKeyFile  string
	clientCertFile string
	clientKeyFile  string
}

func writeGRPCMTLSTestCertificates(t *testing.T) grpcMTLSTestCertFiles {
	t.Helper()

	tempDir := t.TempDir()
	caCertPEM, _, caCert, caKey := mustCreateCertificateAuthority(t, "agent-mtls-ca")
	serverCertPEM, serverKeyPEM := mustCreateSignedCertificate(
		t,
		caCert,
		caKey,
		"agent-mtls-server",
		[]string{"localhost"},
		[]net.IP{net.ParseIP("127.0.0.1")},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	clientCertPEM, clientKeyPEM := mustCreateSignedCertificate(
		t,
		caCert,
		caKey,
		"agent-mtls-client",
		nil,
		nil,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)

	files := grpcMTLSTestCertFiles{
		caCertFile:     filepath.Join(tempDir, "ca.crt"),
		serverCertFile: filepath.Join(tempDir, "server.crt"),
		serverKeyFile:  filepath.Join(tempDir, "server.key"),
		clientCertFile: filepath.Join(tempDir, "client.crt"),
		clientKeyFile:  filepath.Join(tempDir, "client.key"),
	}
	writePEMFile(t, files.caCertFile, caCertPEM)
	writePEMFile(t, files.serverCertFile, serverCertPEM)
	writePEMFile(t, files.serverKeyFile, serverKeyPEM)
	writePEMFile(t, files.clientCertFile, clientCertPEM)
	writePEMFile(t, files.clientKeyFile, clientKeyPEM)

	return files
}

func mustCreateCertificateAuthority(
	t *testing.T,
	commonName string,
) ([]byte, []byte, *x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey(ca) error: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            1,
	}

	der, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		&privateKey.PublicKey,
		privateKey,
	)
	if err != nil {
		t.Fatalf("x509.CreateCertificate(ca) error: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("x509.ParseCertificate(ca) error: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
		},
	)
	return certPEM, keyPEM, cert, privateKey
}

func mustCreateSignedCertificate(
	t *testing.T,
	caCert *x509.Certificate,
	caKey *rsa.PrivateKey,
	commonName string,
	dnsNames []string,
	ipAddresses []net.IP,
	extKeyUsage []x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey(%s) error: %v", commonName, err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: extKeyUsage,
		DNSNames:    dnsNames,
		IPAddresses: ipAddresses,
	}

	der, err := x509.CreateCertificate(
		rand.Reader,
		template,
		caCert,
		&privateKey.PublicKey,
		caKey,
	)
	if err != nil {
		t.Fatalf("x509.CreateCertificate(%s) error: %v", commonName, err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
		},
	)
	return certPEM, keyPEM
}

func writePEMFile(t *testing.T, path string, content []byte) {
	t.Helper()

	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("os.WriteFile(%s) error: %v", path, err)
	}
}

func firstMetadataValue(md metadata.MD, key string) string {
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
