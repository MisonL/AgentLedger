package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolveEndpoint_DefaultByProtocol(t *testing.T) {
	tests := []struct {
		name             string
		rawEndpoint      string
		protocol         string
		endpointExplicit bool
		want             string
	}{
		{
			name:             "grpc default endpoint",
			rawEndpoint:      "",
			protocol:         "grpc",
			endpointExplicit: false,
			want:             defaultGRPCEndpoint,
		},
		{
			name:             "http default endpoint",
			rawEndpoint:      "",
			protocol:         "http",
			endpointExplicit: false,
			want:             defaultHTTPEndpoint,
		},
		{
			name:             "explicit empty keeps empty",
			rawEndpoint:      "",
			protocol:         "grpc",
			endpointExplicit: true,
			want:             "",
		},
		{
			name:             "custom endpoint keeps value",
			rawEndpoint:      " 127.0.0.1:9999 ",
			protocol:         "grpc",
			endpointExplicit: false,
			want:             "127.0.0.1:9999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveEndpoint(tt.rawEndpoint, tt.protocol, tt.endpointExplicit)
			if got != tt.want {
				t.Fatalf("resolveEndpoint(%q, %q, %v)=%q, want=%q", tt.rawEndpoint, tt.protocol, tt.endpointExplicit, got, tt.want)
			}
		})
	}
}

func TestRunCommand_EndpointExplicitEmptyReturnsArgumentError(t *testing.T) {
	tests := []struct {
		name string
		arg  string
	}{
		{name: "empty string", arg: "--endpoint="},
		{name: "whitespace only", arg: "--endpoint=   "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runCommand([]string{tt.arg})
			if got != 2 {
				t.Fatalf("runCommand(%q)=%d, want=2", tt.arg, got)
			}
		})
	}
}

func TestRunCommand_InvalidGRPCFlagsReturnArgumentError(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "plaintext with ca file",
			args: []string{"--protocol=grpc", "--grpc-plaintext", "--grpc-ca-file=ca.pem"},
		},
		{
			name: "cert without key",
			args: []string{"--protocol=grpc", "--grpc-cert-file=client.pem"},
		},
		{
			name: "grpc tls flag with http protocol",
			args: []string{"--protocol=http", "--grpc-insecure-skip-verify"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runCommand(tt.args)
			if got != 2 {
				t.Fatalf("runCommand(%v)=%d, want=2", tt.args, got)
			}
		})
	}
}

func TestExitCodeFromStatus(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		want       int
	}{
		{name: "200 ok", statusCode: 200, want: 0},
		{name: "299 still success", statusCode: 299, want: 0},
		{name: "300 redirect considered failure", statusCode: 300, want: 1},
		{name: "422 client error failure", statusCode: 422, want: 1},
		{name: "500 server error failure", statusCode: 500, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exitCodeFromStatus(tt.statusCode)
			if got != tt.want {
				t.Fatalf("exitCodeFromStatus(%d)=%d, want=%d", tt.statusCode, got, tt.want)
			}
		})
	}
}

func TestDoctorCommand_InvalidArgsReturnArgumentError(t *testing.T) {
	if got := doctorCommand([]string{"--endpoint="}); got != 2 {
		t.Fatalf("doctorCommand(--endpoint=)=%d, want=2", got)
	}
	if got := doctorCommand([]string{"--timeout=0s"}); got != 2 {
		t.Fatalf("doctorCommand(--timeout=0s)=%d, want=2", got)
	}
	if got := doctorCommand([]string{"--protocol=ftp"}); got != 2 {
		t.Fatalf("doctorCommand(--protocol=ftp)=%d, want=2", got)
	}
}

func TestRunDoctorChecks_HTTPPass(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer server.Close()

	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-ok",
		TokenType:   "Bearer",
		ExpiresAt:   time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339),
	})

	report := runDoctorChecks(doctorOptions{
		Protocol:   "http",
		Endpoint:   server.URL,
		TokenFile:  tokenPath,
		Timeout:    500 * time.Millisecond,
		Verbose:    true,
		GRPCConfig: grpcClientSecurityConfig{},
	}, time.Now().UTC())

	if report.OverallStatus != doctorStatusPass {
		t.Fatalf("runDoctorChecks(http) overall_status=%q, want=%q", report.OverallStatus, doctorStatusPass)
	}
	mustCheckStatus(t, report, "token_file", doctorStatusPass)
	mustCheckStatus(t, report, "grpc_config", doctorStatusPass)
	mustCheckStatus(t, report, "endpoint_parse", doctorStatusPass)
	mustCheckStatus(t, report, "endpoint_connectivity", doctorStatusPass)
}

func TestRunDoctorChecks_TokenWarnBranches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	missingPath := filepath.Join(t.TempDir(), "missing-token.json")
	missingReport := runDoctorChecks(doctorOptions{
		Protocol:   "http",
		Endpoint:   server.URL,
		TokenFile:  missingPath,
		Timeout:    500 * time.Millisecond,
		GRPCConfig: grpcClientSecurityConfig{},
	}, time.Now().UTC())
	if missingReport.OverallStatus != doctorStatusWarn {
		t.Fatalf("missing token overall_status=%q, want=%q", missingReport.OverallStatus, doctorStatusWarn)
	}
	mustCheckStatus(t, missingReport, "token_file", doctorStatusWarn)

	expiredPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-expired",
		TokenType:   "Bearer",
		ExpiresAt:   time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339),
	})
	expiredReport := runDoctorChecks(doctorOptions{
		Protocol:   "http",
		Endpoint:   server.URL,
		TokenFile:  expiredPath,
		Timeout:    500 * time.Millisecond,
		GRPCConfig: grpcClientSecurityConfig{},
	}, time.Now().UTC())
	if expiredReport.OverallStatus != doctorStatusWarn {
		t.Fatalf("expired token overall_status=%q, want=%q", expiredReport.OverallStatus, doctorStatusWarn)
	}
	mustCheckStatus(t, expiredReport, "token_file", doctorStatusWarn)
}

func TestRunDoctorChecks_EndpointAndConnectivityFail(t *testing.T) {
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-ok",
		TokenType:   "Bearer",
		ExpiresAt:   time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339),
	})

	parseFailReport := runDoctorChecks(doctorOptions{
		Protocol:   "http",
		Endpoint:   "127.0.0.1:8081",
		TokenFile:  tokenPath,
		Timeout:    300 * time.Millisecond,
		GRPCConfig: grpcClientSecurityConfig{},
	}, time.Now().UTC())
	if parseFailReport.OverallStatus != doctorStatusFail {
		t.Fatalf("parse fail overall_status=%q, want=%q", parseFailReport.OverallStatus, doctorStatusFail)
	}
	mustCheckStatus(t, parseFailReport, "endpoint_parse", doctorStatusFail)
	mustCheckStatus(t, parseFailReport, "endpoint_connectivity", doctorStatusFail)

	connectFailReport := runDoctorChecks(doctorOptions{
		Protocol:   "grpc",
		Endpoint:   "127.0.0.1:1",
		TokenFile:  tokenPath,
		Timeout:    300 * time.Millisecond,
		GRPCConfig: grpcClientSecurityConfig{},
	}, time.Now().UTC())
	if connectFailReport.OverallStatus != doctorStatusFail {
		t.Fatalf("connect fail overall_status=%q, want=%q", connectFailReport.OverallStatus, doctorStatusFail)
	}
	mustCheckStatus(t, connectFailReport, "endpoint_parse", doctorStatusPass)
	mustCheckStatus(t, connectFailReport, "endpoint_connectivity", doctorStatusFail)
}

func TestRunDoctorChecks_GRPCConfigFailAndGRPCConnectivityPass(t *testing.T) {
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-ok",
		TokenType:   "Bearer",
		ExpiresAt:   time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339),
	})

	grpcConfigFailReport := runDoctorChecks(doctorOptions{
		Protocol:  "http",
		Endpoint:  "http://127.0.0.1:8080/health",
		TokenFile: tokenPath,
		Timeout:   300 * time.Millisecond,
		GRPCConfig: grpcClientSecurityConfig{
			Plaintext: true,
		},
	}, time.Now().UTC())
	if grpcConfigFailReport.OverallStatus != doctorStatusFail {
		t.Fatalf("grpc config fail overall_status=%q, want=%q", grpcConfigFailReport.OverallStatus, doctorStatusFail)
	}
	mustCheckStatus(t, grpcConfigFailReport, "grpc_config", doctorStatusFail)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}
	defer listener.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		_ = conn.Close()
	}()

	grpcReport := runDoctorChecks(doctorOptions{
		Protocol:   "grpc",
		Endpoint:   listener.Addr().String(),
		TokenFile:  tokenPath,
		Timeout:    500 * time.Millisecond,
		GRPCConfig: grpcClientSecurityConfig{},
	}, time.Now().UTC())
	if grpcReport.OverallStatus != doctorStatusPass {
		t.Fatalf("grpc overall_status=%q, want=%q", grpcReport.OverallStatus, doctorStatusPass)
	}
	mustCheckStatus(t, grpcReport, "endpoint_parse", doctorStatusPass)
	mustCheckStatus(t, grpcReport, "endpoint_connectivity", doctorStatusPass)
	<-done
}

func TestDoctorCommand_JSONOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-ok",
		TokenType:   "Bearer",
		ExpiresAt:   time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339),
	})

	exitCode, output := captureStdout(t, func() int {
		return doctorCommand([]string{
			"--protocol=http",
			"--endpoint=" + server.URL,
			"--token-file=" + tokenPath,
			"--timeout=500ms",
		})
	})
	if exitCode != 0 {
		t.Fatalf("doctorCommand()=%d, want=0, output=%s", exitCode, output)
	}

	var payload struct {
		OverallStatus string `json:"overall_status"`
		Checks        []struct {
			Name    string         `json:"name"`
			Status  string         `json:"status"`
			Message string         `json:"message"`
			Details map[string]any `json:"details"`
		} `json:"checks"`
	}
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		t.Fatalf("doctor output json unmarshal error: %v, output=%q", err, output)
	}
	if strings.TrimSpace(payload.OverallStatus) == "" {
		t.Fatalf("doctor output missing overall_status: %s", output)
	}
	if len(payload.Checks) == 0 {
		t.Fatalf("doctor output missing checks: %s", output)
	}
}

func TestDoctorCommand_ReturnsNonZeroWhenChecksFail(t *testing.T) {
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-ok",
		TokenType:   "Bearer",
		ExpiresAt:   time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339),
	})

	exitCode, output := captureStdout(t, func() int {
		return doctorCommand([]string{
			"--protocol=grpc",
			"--endpoint=127.0.0.1:1",
			"--token-file=" + tokenPath,
			"--timeout=300ms",
		})
	})
	if exitCode == 0 {
		t.Fatalf("doctorCommand()=%d, want non-zero when checks fail, output=%s", exitCode, output)
	}

	var payload struct {
		OverallStatus string `json:"overall_status"`
	}
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		t.Fatalf("doctor output json unmarshal error: %v, output=%q", err, output)
	}
	if payload.OverallStatus != doctorStatusFail {
		t.Fatalf("overall_status=%q, want=%q", payload.OverallStatus, doctorStatusFail)
	}
}

func mustCheckStatus(t *testing.T, report doctorReport, name, wantStatus string) {
	t.Helper()
	for _, item := range report.Checks {
		if item.Name != name {
			continue
		}
		if item.Status != wantStatus {
			t.Fatalf("check %q status=%q, want=%q", name, item.Status, wantStatus)
		}
		return
	}
	t.Fatalf("check %q not found", name)
}

func writeTokenFileForTest(t *testing.T, token localToken) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token.json")
	body, err := json.Marshal(token)
	if err != nil {
		t.Fatalf("json.Marshal(token) error: %v", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("os.WriteFile(token) error: %v", err)
	}
	return path
}

func captureStdout(t *testing.T, fn func() int) (int, string) {
	t.Helper()

	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error: %v", err)
	}

	os.Stdout = writer
	exitCode := fn()
	_ = writer.Close()
	os.Stdout = oldStdout

	content, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil {
		t.Fatalf("io.ReadAll(stdout) error: %v", readErr)
	}
	return exitCode, string(content)
}
