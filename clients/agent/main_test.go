package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
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

func TestRunCommand_DaemonRequiresLocalToken(t *testing.T) {
	missingTokenPath := filepath.Join(t.TempDir(), "missing-token.json")
	exitCode, _, stderr := captureOutput(t, func() int {
		return runCommand([]string{
			"--daemon",
			"--token-file=" + missingTokenPath,
			"--agent-id=agent-daemon",
			"--source-id=source-daemon",
		})
	})
	if exitCode != 1 {
		t.Fatalf("runCommand(--daemon)=%d, want=1, stderr=%s", exitCode, stderr)
	}
	if !strings.Contains(stderr, "守护模式要求本地 token") {
		t.Fatalf("stderr=%q, want contains 守护模式要求本地 token", stderr)
	}
}

func TestFetchAgentRuntimeConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method=%q, want=GET", r.Method)
		}
		if r.URL.Path != "/api/v1/system/config/agent-runtime" {
			t.Fatalf("path=%q, want=/api/v1/system/config/agent-runtime", r.URL.Path)
		}
		if got := r.URL.Query().Get("agentId"); got != "agent-fetch-1" {
			t.Fatalf("agentId=%q, want=agent-fetch-1", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer runtime-token" {
			t.Fatalf("authorization=%q, want=Bearer runtime-token", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tenantId": "default",
			"agent": map[string]any{
				"agentId":     "agent-fetch-1",
				"hostname":    "host-1",
				"version":     "0.1.0",
				"displayName": "Agent Fetch 1",
			},
			"runtime": map[string]any{
				"heartbeatIntervalSeconds": 30,
				"staleAfterSeconds":        90,
				"ingestProtocol":           "http",
				"ingestEndpoint":           "http://127.0.0.1:8081/v1/ingest",
				"sampleGenerateCount":      5,
			},
			"bindings": map[string]any{
				"sourceCount": 1,
				"sourceIds":   []string{"source-1"},
				"sources": []map[string]any{
					{
						"sourceId":     "source-1",
						"name":         "Source One",
						"accessMode":   "realtime",
						"enabled":      true,
						"location":     "/var/log/agent",
						"sourceRegion": "cn-shanghai",
					},
				},
			},
			"configVersion": "cfg:test-001",
			"updatedAt":     "2026-03-09T01:00:01.000Z",
		})
	}))
	defer server.Close()

	config, err := fetchAgentRuntimeConfig(server.URL, "Bearer runtime-token", "agent-fetch-1", 3*time.Second)
	if err != nil {
		t.Fatalf("fetchAgentRuntimeConfig() error: %v", err)
	}
	if config.Agent.AgentID != "agent-fetch-1" {
		t.Fatalf("agent_id=%q, want=agent-fetch-1", config.Agent.AgentID)
	}
	if config.ConfigVersion != "cfg:test-001" {
		t.Fatalf("configVersion=%q, want=cfg:test-001", config.ConfigVersion)
	}
	if config.Bindings.SourceCount != 1 {
		t.Fatalf("sourceCount=%d, want=1", config.Bindings.SourceCount)
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

func TestStatusCommand_InvalidArgsReturnArgumentError(t *testing.T) {
	if got := statusCommand([]string{"--protocol=ftp"}); got != 2 {
		t.Fatalf("statusCommand(--protocol=ftp)=%d, want=2", got)
	}
	if got := statusCommand([]string{"--endpoint="}); got != 2 {
		t.Fatalf("statusCommand(--endpoint=)=%d, want=2", got)
	}
}

func TestConfigCommand_InvalidArgsReturnArgumentError(t *testing.T) {
	if got := configCommand([]string{}); got != 2 {
		t.Fatalf("configCommand()=%d, want=2", got)
	}
	if got := configCommand([]string{"pull"}); got != 2 {
		t.Fatalf("configCommand(pull)=%d, want=2", got)
	}
	if got := configCommand([]string{"activate"}); got != 2 {
		t.Fatalf("configCommand(activate)=%d, want=2", got)
	}
}

func TestStatusCommand_JSONOutputWithConfigPackage(t *testing.T) {
	now := time.Now().UTC()
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken:  "token-status-ok",
		TokenType:    "Bearer",
		RefreshToken: "refresh-status-ok",
		ObtainedAt:   now.Add(-10 * time.Minute).Format(time.RFC3339),
		ExpiresAt:    now.Add(1 * time.Hour).Format(time.RFC3339),
	})
	configPath := writeJSONFileForTest(t, "config-package.json", map[string]any{
		"package_id":       "pkg-status-2026-03-07",
		"version":          "cfg-2026-03-07",
		"issued_at":        "2026-03-07T08:00:00Z",
		"signature_status": "verified",
	})

	exitCode, output := captureStdout(t, func() int {
		return statusCommand([]string{
			"--protocol=grpc",
			"--token-file=" + tokenPath,
			"--config-file=" + configPath,
		})
	})
	if exitCode != 0 {
		t.Fatalf("statusCommand()=%d, want=0, output=%s", exitCode, output)
	}

	var payload struct {
		Component        string `json:"component"`
		Protocol         string `json:"protocol"`
		Endpoint         string `json:"endpoint"`
		DefaultEndpoints struct {
			HTTP string `json:"http"`
			GRPC string `json:"grpc"`
		} `json:"default_endpoints"`
		Token struct {
			Path            string `json:"path"`
			Found           bool   `json:"found"`
			Status          string `json:"status"`
			TokenType       string `json:"token_type"`
			ExpiresAt       string `json:"expires_at"`
			ObtainedAt      string `json:"obtained_at"`
			HasRefreshToken bool   `json:"has_refresh_token"`
		} `json:"token"`
		ConfigPackage struct {
			PackageID       string `json:"package_id"`
			Path            string `json:"path"`
			Version         string `json:"version"`
			IssuedAt        string `json:"issued_at"`
			SignatureStatus string `json:"signature_status"`
		} `json:"config_package"`
	}
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		t.Fatalf("status output json unmarshal error: %v, output=%q", err, output)
	}
	if payload.Component != "agent-cli" {
		t.Fatalf("component=%q, want=agent-cli", payload.Component)
	}
	if payload.Protocol != "grpc" {
		t.Fatalf("protocol=%q, want=grpc", payload.Protocol)
	}
	if payload.Endpoint != defaultGRPCEndpoint {
		t.Fatalf("endpoint=%q, want=%q", payload.Endpoint, defaultGRPCEndpoint)
	}
	if payload.DefaultEndpoints.HTTP != defaultHTTPEndpoint {
		t.Fatalf("default http endpoint=%q, want=%q", payload.DefaultEndpoints.HTTP, defaultHTTPEndpoint)
	}
	if payload.DefaultEndpoints.GRPC != defaultGRPCEndpoint {
		t.Fatalf("default grpc endpoint=%q, want=%q", payload.DefaultEndpoints.GRPC, defaultGRPCEndpoint)
	}
	if !payload.Token.Found || payload.Token.Status != "valid" {
		t.Fatalf("token status=%q found=%v, want valid/true", payload.Token.Status, payload.Token.Found)
	}
	if payload.Token.TokenType != "Bearer" {
		t.Fatalf("token type=%q, want=Bearer", payload.Token.TokenType)
	}
	if !payload.Token.HasRefreshToken {
		t.Fatal("has_refresh_token=false, want true")
	}
	if payload.ConfigPackage.PackageID != "pkg-status-2026-03-07" {
		t.Fatalf("config package_id=%q, want=pkg-status-2026-03-07", payload.ConfigPackage.PackageID)
	}
	if payload.ConfigPackage.Version != "cfg-2026-03-07" {
		t.Fatalf("config version=%q, want=cfg-2026-03-07", payload.ConfigPackage.Version)
	}
	if payload.ConfigPackage.IssuedAt != "2026-03-07T08:00:00Z" {
		t.Fatalf("config issued_at=%q, want=2026-03-07T08:00:00Z", payload.ConfigPackage.IssuedAt)
	}
	if payload.ConfigPackage.SignatureStatus != "verified" {
		t.Fatalf("config signature_status=%q, want=verified", payload.ConfigPackage.SignatureStatus)
	}
}

func TestConfigPullActivateAndStatusShowActivePackage(t *testing.T) {
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "config-token",
		TokenType:   "Bearer",
		ExpiresAt:   time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339),
	})
	configDir := filepath.Join(t.TempDir(), "config")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer config-token" {
			t.Fatalf("authorization header=%q, want=Bearer config-token", got)
		}
		if r.Method != http.MethodGet {
			t.Fatalf("method=%q, want=GET", r.Method)
		}
		if r.URL.Path != "/api/v1/system/config/packages/pkg-test-1" {
			t.Fatalf("path=%q, want=/api/v1/system/config/packages/pkg-test-1", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"packageId":       "pkg-test-1",
			"tenantId":        "tenant-test",
			"version":         "2026.03.08",
			"issuedAt":        "2026-03-08T08:00:00Z",
			"signatureStatus": "verified",
			"payload":         map[string]any{"mode": "observe"},
			"createdAt":       "2026-03-08T08:00:00Z",
			"updatedAt":       "2026-03-08T08:00:00Z",
		})
	}))
	defer server.Close()

	pullExitCode, pullStdout, pullStderr := captureOutput(t, func() int {
		return configCommand([]string{
			"pull",
			"--gateway=" + server.URL,
			"--package-id=pkg-test-1",
			"--token-file=" + tokenPath,
			"--config-dir=" + configDir,
		})
	})
	if pullExitCode != 0 {
		t.Fatalf("config pull exit=%d, want=0, stdout=%s stderr=%s", pullExitCode, pullStdout, pullStderr)
	}
	if !strings.Contains(pullStdout, "package_id=pkg-test-1") {
		t.Fatalf("pull stdout=%q, want contains package_id=pkg-test-1", pullStdout)
	}

	activateExitCode, activateStdout, activateStderr := captureOutput(t, func() int {
		return configCommand([]string{
			"activate",
			"--package-id=pkg-test-1",
			"--config-dir=" + configDir,
		})
	})
	if activateExitCode != 0 {
		t.Fatalf("config activate exit=%d, want=0, stdout=%s stderr=%s", activateExitCode, activateStdout, activateStderr)
	}
	if !strings.Contains(activateStdout, "package_id=pkg-test-1") {
		t.Fatalf("activate stdout=%q, want contains package_id=pkg-test-1", activateStdout)
	}

	statusExitCode, statusOutput := captureStdout(t, func() int {
		return statusCommand([]string{"--config-dir=" + configDir})
	})
	if statusExitCode != 0 {
		t.Fatalf("statusCommand()=%d, want=0, output=%s", statusExitCode, statusOutput)
	}

	var payload struct {
		Config struct {
			Dir             string `json:"dir"`
			Status          string `json:"status"`
			ActivePackageID string `json:"active_package_id"`
			Version         string `json:"version"`
			IssuedAt        string `json:"issued_at"`
			SignatureStatus string `json:"signature_status"`
			ActivatedAt     string `json:"activated_at"`
			PackagePath     string `json:"package_path"`
		} `json:"config"`
	}
	if err := json.Unmarshal([]byte(statusOutput), &payload); err != nil {
		t.Fatalf("status output json unmarshal error: %v, output=%q", err, statusOutput)
	}
	if payload.Config.Dir != configDir {
		t.Fatalf("config.dir=%q, want=%q", payload.Config.Dir, configDir)
	}
	if payload.Config.Status != "active" {
		t.Fatalf("config.status=%q, want=active", payload.Config.Status)
	}
	if payload.Config.ActivePackageID != "pkg-test-1" {
		t.Fatalf("config.active_package_id=%q, want=pkg-test-1", payload.Config.ActivePackageID)
	}
	if payload.Config.Version != "2026.03.08" {
		t.Fatalf("config.version=%q, want=2026.03.08", payload.Config.Version)
	}
	if payload.Config.SignatureStatus != "verified" {
		t.Fatalf("config.signature_status=%q, want=verified", payload.Config.SignatureStatus)
	}
	if payload.Config.IssuedAt != "2026-03-08T08:00:00Z" {
		t.Fatalf("config.issued_at=%q, want=2026-03-08T08:00:00Z", payload.Config.IssuedAt)
	}
	if strings.TrimSpace(payload.Config.ActivatedAt) == "" {
		t.Fatalf("config.activated_at=%q, want non-empty", payload.Config.ActivatedAt)
	}
	if !strings.HasSuffix(payload.Config.PackagePath, filepath.Join("packages", "pkg-test-1.json")) {
		t.Fatalf("config.package_path=%q, want packages/pkg-test-1.json suffix", payload.Config.PackagePath)
	}
}

func TestStatusCommand_QueueMetrics(t *testing.T) {
	queueDir := filepath.Join(t.TempDir(), "queue")
	firstTime := time.Date(2026, 3, 7, 8, 0, 0, 0, time.UTC)
	secondTime := firstTime.Add(2 * time.Minute)

	if _, _, err := enqueueAgentQueueRequest(queueDir, ingestBatchRequest{
		BatchID: "batch-queue-1",
		Events: []agentEvent{{
			EventID:    "evt-queue-1",
			SessionID:  "session-queue-1",
			EventType:  "message",
			OccurredAt: firstTime.Format(time.RFC3339),
		}},
	}, firstTime); err != nil {
		t.Fatalf("enqueueAgentQueueRequest(first) error: %v", err)
	}
	if _, _, err := enqueueAgentQueueRequest(queueDir, ingestBatchRequest{
		BatchID: "batch-queue-2",
		Events: []agentEvent{{
			EventID:    "evt-queue-2",
			SessionID:  "session-queue-2",
			EventType:  "message",
			OccurredAt: secondTime.Format(time.RFC3339),
		}},
	}, secondTime); err != nil {
		t.Fatalf("enqueueAgentQueueRequest(second) error: %v", err)
	}

	exitCode, output := captureStdout(t, func() int {
		return statusCommand([]string{"--queue-dir=" + queueDir})
	})
	if exitCode != 0 {
		t.Fatalf("statusCommand()=%d, want=0, output=%s", exitCode, output)
	}

	var payload struct {
		Queue struct {
			Enabled          bool   `json:"enabled"`
			Path             string `json:"path"`
			PendingCount     int    `json:"pending_count"`
			OldestEnqueuedAt string `json:"oldest_enqueued_at"`
			TotalBytes       int64  `json:"total_bytes"`
		} `json:"queue"`
	}
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		t.Fatalf("status output json unmarshal error: %v, output=%q", err, output)
	}
	if !payload.Queue.Enabled {
		t.Fatal("queue.enabled=false, want true")
	}
	if payload.Queue.Path != queueDir {
		t.Fatalf("queue.path=%q, want=%q", payload.Queue.Path, queueDir)
	}
	if payload.Queue.PendingCount != 2 {
		t.Fatalf("queue.pending_count=%d, want=2", payload.Queue.PendingCount)
	}
	if payload.Queue.OldestEnqueuedAt != firstTime.Format(time.RFC3339) {
		t.Fatalf("queue.oldest_enqueued_at=%q, want=%q", payload.Queue.OldestEnqueuedAt, firstTime.Format(time.RFC3339))
	}
	if payload.Queue.TotalBytes <= 0 {
		t.Fatalf("queue.total_bytes=%d, want > 0", payload.Queue.TotalBytes)
	}
}

func TestStatusCommand_ConfigFileErrorReturnsNonZero(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "invalid-config.json")
	if err := os.WriteFile(configPath, []byte(`{"version":"cfg-1","issued_at":"invalid"`), 0o600); err != nil {
		t.Fatalf("os.WriteFile(config) error: %v", err)
	}

	exitCode, _, stderr := captureOutput(t, func() int {
		return statusCommand([]string{"--config-file=" + configPath})
	})
	if exitCode != 1 {
		t.Fatalf("statusCommand()=%d, want=1", exitCode)
	}
	if !strings.Contains(stderr, "读取配置包失败") {
		t.Fatalf("stderr=%q, want contains 读取配置包失败", stderr)
	}
}

func TestConfigPullCommand_DownloadsPackageToManagedDirectory(t *testing.T) {
	now := time.Now().UTC()
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-config-pull",
		TokenType:   "Bearer",
		ObtainedAt:  now.Add(-5 * time.Minute).Format(time.RFC3339),
		ExpiresAt:   now.Add(30 * time.Minute).Format(time.RFC3339),
	})
	configDir := filepath.Join(t.TempDir(), "config")
	expectedAuthHeader := "Bearer token-config-pull"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/system/config/packages/pkg-config-pull" {
			http.NotFound(w, r)
			return
		}
		if got := strings.TrimSpace(r.Header.Get("Authorization")); got != expectedAuthHeader {
			t.Fatalf("authorization=%q, want=%q", got, expectedAuthHeader)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"packageId":       "pkg-config-pull",
			"tenantId":        "tenant-a",
			"version":         "cfg-2026-03-08",
			"issuedAt":        "2026-03-08T08:00:00Z",
			"signatureStatus": "verified",
			"payload": map[string]any{
				"mode":     "strict",
				"queueDir": "/var/lib/agentledger/queue",
			},
			"createdAt": "2026-03-08T08:00:00Z",
			"updatedAt": "2026-03-08T08:00:00Z",
		})
	}))
	defer server.Close()

	exitCode, stdout, stderr := captureOutput(t, func() int {
		return configCommand([]string{
			"pull",
			"--gateway=" + server.URL,
			"--package-id=pkg-config-pull",
			"--token-file=" + tokenPath,
			"--config-dir=" + configDir,
		})
	})
	if exitCode != 0 {
		t.Fatalf("configCommand(pull)=%d, want=0, stdout=%s stderr=%s", exitCode, stdout, stderr)
	}
	if !strings.Contains(stdout, "配置包已拉取") {
		t.Fatalf("stdout=%q, want contains 配置包已拉取", stdout)
	}

	body, err := os.ReadFile(filepath.Join(configDir, "packages", "pkg-config-pull.json"))
	if err != nil {
		t.Fatalf("os.ReadFile(pulled package) error: %v", err)
	}
	var pkg localConfigPackage
	if err := json.Unmarshal(body, &pkg); err != nil {
		t.Fatalf("json.Unmarshal(pulled package) error: %v", err)
	}
	if pkg.PackageID != "pkg-config-pull" {
		t.Fatalf("package_id=%q, want=pkg-config-pull", pkg.PackageID)
	}
	if pkg.Version != "cfg-2026-03-08" {
		t.Fatalf("version=%q, want=cfg-2026-03-08", pkg.Version)
	}
	if pkg.SignatureStatus != "verified" {
		t.Fatalf("signature_status=%q, want=verified", pkg.SignatureStatus)
	}
	if got, _ := pkg.Payload["mode"].(string); got != "strict" {
		t.Fatalf("payload.mode=%q, want=strict", got)
	}
}

func TestConfigActivateCommand_StatusShowsCurrentPackage(t *testing.T) {
	configDir := filepath.Join(t.TempDir(), "config")
	if _, err := writeLocalConfigPackage(configDir, localConfigPackage{
		PackageID:       "pkg-config-active",
		TenantID:        "tenant-a",
		Version:         "cfg-2026-03-08-active",
		IssuedAt:        "2026-03-08T09:00:00Z",
		SignatureStatus: "verified",
		Payload: map[string]any{
			"mode": "audit",
		},
		CreatedAt: "2026-03-08T09:00:00Z",
		UpdatedAt: "2026-03-08T09:00:00Z",
	}); err != nil {
		t.Fatalf("writeLocalConfigPackage() error: %v", err)
	}

	exitCode, stdout, stderr := captureOutput(t, func() int {
		return configCommand([]string{
			"activate",
			"--package-id=pkg-config-active",
			"--config-dir=" + configDir,
		})
	})
	if exitCode != 0 {
		t.Fatalf("configCommand(activate)=%d, want=0, stdout=%s stderr=%s", exitCode, stdout, stderr)
	}
	if !strings.Contains(stdout, "配置包已激活") {
		t.Fatalf("stdout=%q, want contains 配置包已激活", stdout)
	}

	statusExitCode, statusOutput := captureStdout(t, func() int {
		return statusCommand([]string{"--config-dir=" + configDir})
	})
	if statusExitCode != 0 {
		t.Fatalf("statusCommand()=%d, want=0, output=%s", statusExitCode, statusOutput)
	}

	var payload struct {
		Config struct {
			Status          string `json:"status"`
			ActivePackageID string `json:"active_package_id"`
			Version         string `json:"version"`
			IssuedAt        string `json:"issued_at"`
			SignatureStatus string `json:"signature_status"`
			ActivatedAt     string `json:"activated_at"`
			PackagePath     string `json:"package_path"`
		} `json:"config"`
	}
	if err := json.Unmarshal([]byte(statusOutput), &payload); err != nil {
		t.Fatalf("status output json unmarshal error: %v, output=%q", err, statusOutput)
	}
	if payload.Config.Status != "active" {
		t.Fatalf("config.status=%q, want=active", payload.Config.Status)
	}
	if payload.Config.ActivePackageID != "pkg-config-active" {
		t.Fatalf("config.active_package_id=%q, want=pkg-config-active", payload.Config.ActivePackageID)
	}
	if payload.Config.Version != "cfg-2026-03-08-active" {
		t.Fatalf("config.version=%q, want=cfg-2026-03-08-active", payload.Config.Version)
	}
	if payload.Config.IssuedAt != "2026-03-08T09:00:00Z" {
		t.Fatalf("config.issued_at=%q, want=2026-03-08T09:00:00Z", payload.Config.IssuedAt)
	}
	if payload.Config.SignatureStatus != "verified" {
		t.Fatalf("config.signature_status=%q, want=verified", payload.Config.SignatureStatus)
	}
	if payload.Config.ActivatedAt == "" {
		t.Fatal("config.activated_at empty, want non-empty")
	}
	if !strings.HasSuffix(payload.Config.PackagePath, "/pkg-config-active.json") &&
		!strings.HasSuffix(payload.Config.PackagePath, "\\pkg-config-active.json") {
		t.Fatalf("config.package_path=%q, want ends with pkg-config-active.json", payload.Config.PackagePath)
	}
}

func TestConfigRollbackCommand_RestoresPreviousPackage(t *testing.T) {
	configDir := filepath.Join(t.TempDir(), "config")
	if _, err := writeLocalConfigPackage(configDir, localConfigPackage{
		PackageID:       "pkg-prev",
		Version:         "cfg-prev",
		SignatureStatus: "verified",
		Payload:         map[string]any{"mode": "prev"},
	}); err != nil {
		t.Fatalf("writeLocalConfigPackage(prev) error: %v", err)
	}
	if _, err := writeLocalConfigPackage(configDir, localConfigPackage{
		PackageID:       "pkg-current",
		Version:         "cfg-current",
		SignatureStatus: "verified",
		Payload:         map[string]any{"mode": "current"},
	}); err != nil {
		t.Fatalf("writeLocalConfigPackage(current) error: %v", err)
	}
	if _, err := writeActiveConfigSelection(configDir, localConfigActivation{
		PackageID:   "pkg-current",
		ActivatedAt: "2026-03-08T10:00:00Z",
	}); err != nil {
		t.Fatalf("writeActiveConfigSelection() error: %v", err)
	}
	if _, err := writeConfigRuntimeState(configDir, localConfigRuntimeState{
		ActivePackageID:   "pkg-current",
		PreviousPackageID: "pkg-prev",
		ActivatedAt:       "2026-03-08T10:00:00Z",
		RollbackAvailable: true,
	}); err != nil {
		t.Fatalf("writeConfigRuntimeState() error: %v", err)
	}
	exitCode, stdout, stderr := captureOutput(t, func() int {
		return configCommand([]string{"rollback", "--config-dir=" + configDir})
	})
	if exitCode != 0 {
		t.Fatalf("configCommand(rollback)=%d, want=0, stdout=%s stderr=%s", exitCode, stdout, stderr)
	}
	if !strings.Contains(stdout, "配置包已回滚") {
		t.Fatalf("stdout=%q, want contains 配置包已回滚", stdout)
	}
	status, err := readStatusManagedConfig(configDir)
	if err != nil {
		t.Fatalf("readStatusManagedConfig() error: %v", err)
	}
	if status.ActivePackageID != "pkg-prev" {
		t.Fatalf("active package=%q, want=pkg-prev", status.ActivePackageID)
	}
}

func TestConfigWatchCommand_PullsLatestPackageAndOptionallyActivates(t *testing.T) {
	now := time.Now().UTC()
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-config-watch",
		TokenType:   "Bearer",
		ExpiresAt:   now.Add(1 * time.Hour).Format(time.RFC3339),
	})
	configDir := filepath.Join(t.TempDir(), "config")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/system/config/packages" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"packageId":       "pkg-watch-1",
					"tenantId":        "tenant-a",
					"version":         "cfg-watch-1",
					"issuedAt":        "2026-03-08T10:00:00Z",
					"signatureStatus": "verified",
					"payload":         map[string]any{"mode": "observe"},
					"createdAt":       "2026-03-08T10:00:00Z",
					"updatedAt":       "2026-03-08T10:00:00Z",
				},
			},
			"total":   1,
			"filters": map[string]any{"limit": 1},
		})
	}))
	defer server.Close()

	exitCode, stdout, stderr := captureOutput(t, func() int {
		return configCommand([]string{
			"watch",
			"--gateway=" + server.URL,
			"--token-file=" + tokenPath,
			"--config-dir=" + configDir,
			"--iterations=1",
			"--interval=1s",
			"--auto-activate",
		})
	})
	if exitCode != 0 {
		t.Fatalf("configCommand(watch)=%d, want=0, stdout=%s stderr=%s", exitCode, stdout, stderr)
	}
	if !strings.Contains(stdout, "配置包已同步") {
		t.Fatalf("stdout=%q, want contains 配置包已同步", stdout)
	}
	status, err := readStatusManagedConfig(configDir)
	if err != nil {
		t.Fatalf("readStatusManagedConfig() error: %v", err)
	}
	if status.ActivePackageID != "pkg-watch-1" {
		t.Fatalf("active package=%q, want=pkg-watch-1", status.ActivePackageID)
	}
}

func TestUpdateCommand_InvalidArgsReturnArgumentError(t *testing.T) {
	if got := updateCommand([]string{}); got != 2 {
		t.Fatalf("updateCommand()=%d, want=2", got)
	}
	if got := updateCommand([]string{"check", "--timeout=0s"}); got != 2 {
		t.Fatalf("updateCommand(check --timeout=0s)=%d, want=2", got)
	}
	if got := updateCommand([]string{"check", "--channel=nightly"}); got != 2 {
		t.Fatalf("updateCommand(check --channel=nightly)=%d, want=2", got)
	}
}

func TestUpdateCheckCommand_UsesGatewayAndChannelDefaults(t *testing.T) {
	now := time.Now().UTC()
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-update-check",
		TokenType:   "Bearer",
		ObtainedAt:  now.Add(-5 * time.Minute).Format(time.RFC3339),
		ExpiresAt:   now.Add(1 * time.Hour).Format(time.RFC3339),
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/system/agent-releases/check" {
			http.NotFound(w, r)
			return
		}
		if got := strings.TrimSpace(r.Header.Get("Authorization")); got != "Bearer token-update-check" {
			t.Fatalf("authorization=%q, want=%q", got, "Bearer token-update-check")
		}
		if got := r.URL.Query().Get("currentVersion"); got != "0.9.0" {
			t.Fatalf("currentVersion=%q, want=0.9.0", got)
		}
		if got := r.URL.Query().Get("channel"); got != "beta" {
			t.Fatalf("channel=%q, want=beta", got)
		}
		if got := r.URL.Query().Get("os"); got != "darwin" {
			t.Fatalf("os=%q, want=darwin", got)
		}
		if got := r.URL.Query().Get("arch"); got != "arm64" {
			t.Fatalf("arch=%q, want=arm64", got)
		}
		if got := r.URL.Query().Get("agentId"); got != "agent-rollout-check" {
			t.Fatalf("agentId=%q, want=agent-rollout-check", got)
		}
		if got := r.URL.Query().Get("deviceId"); got != "device-rollout-check" {
			t.Fatalf("deviceId=%q, want=device-rollout-check", got)
		}
		if got := r.URL.Query().Get("hostname"); got != "host-rollout-check" {
			t.Fatalf("hostname=%q, want=host-rollout-check", got)
		}
		if got := r.URL.Query().Get("ring"); got != "beta-ring" {
			t.Fatalf("ring=%q, want=beta-ring", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"checkedAt":       "2026-03-08T10:00:00Z",
			"currentVersion":  "0.9.0",
			"channel":         "beta",
			"os":              "darwin",
			"arch":            "arm64",
			"updateAvailable": true,
			"comparison":      "upgrade_available",
			"latestRelease": map[string]any{
				"releaseId":   "release-beta-1",
				"tenantId":    "tenant-a",
				"version":     "1.0.0-beta.2",
				"channel":     "beta",
				"notes":       "预发布验证版",
				"publishedAt": "2026-03-08T09:00:00Z",
				"artifacts": []map[string]any{
					{
						"os":             "darwin",
						"arch":           "arm64",
						"downloadUrl":    "https://downloads.example.com/agent-darwin-arm64.zip",
						"checksumSha256": "abc123",
						"fileName":       "agent-darwin-arm64.zip",
						"installHint":    "解压后覆盖二进制，再执行 agent version 验证",
					},
				},
				"createdAt": "2026-03-08T09:00:00Z",
				"updatedAt": "2026-03-08T09:00:00Z",
			},
			"selectedArtifact": map[string]any{
				"os":                "darwin",
				"arch":              "arm64",
				"downloadUrl":       "https://downloads.example.com/agent-darwin-arm64.zip",
				"checksumSha256":    "abc123",
				"rolloutRing":       "beta-ring",
				"rolloutPercentage": 25,
				"minAgentVersion":   "0.8.0",
				"fileName":          "agent-darwin-arm64.zip",
				"installHint":       "解压后覆盖二进制，再执行 agent version 验证",
			},
			"instructions": "当前仅提供升级检查结果，不执行真实下载升级。",
		})
	}))
	defer server.Close()

	t.Setenv(agentGatewayURLEnv, server.URL)
	t.Setenv(agentReleaseChannelEnv, "beta")

	exitCode, output := captureStdout(t, func() int {
		return updateCommand([]string{
			"check",
			"--token-file=" + tokenPath,
			"--current-version=0.9.0",
			"--os=darwin",
			"--arch=arm64",
			"--agent-id=agent-rollout-check",
			"--device-id=device-rollout-check",
			"--hostname=host-rollout-check",
			"--ring=beta-ring",
		})
	})
	if exitCode != 0 {
		t.Fatalf("updateCommand(check)=%d, want=0, output=%s", exitCode, output)
	}

	var payload struct {
		Component       string `json:"component"`
		CheckedAt       string `json:"checked_at"`
		Gateway         string `json:"gateway"`
		CurrentVersion  string `json:"current_version"`
		Channel         string `json:"channel"`
		OS              string `json:"os"`
		Arch            string `json:"arch"`
		UpdateAvailable bool   `json:"update_available"`
		Comparison      string `json:"comparison"`
		Mode            string `json:"mode"`
		Instructions    string `json:"instructions"`
		LatestRelease   struct {
			ReleaseID string `json:"releaseId"`
			Version   string `json:"version"`
			Channel   string `json:"channel"`
		} `json:"latest_release"`
		SelectedArtifact struct {
			OS                string `json:"os"`
			Arch              string `json:"arch"`
			DownloadURL       string `json:"downloadUrl"`
			FileName          string `json:"fileName"`
			RolloutRing       string `json:"rolloutRing"`
			RolloutPercentage int    `json:"rolloutPercentage"`
			MinAgentVersion   string `json:"minAgentVersion"`
		} `json:"selected_artifact"`
	}
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		t.Fatalf("update output json unmarshal error: %v, output=%q", err, output)
	}
	if payload.Component != "agent-cli" {
		t.Fatalf("component=%q, want=agent-cli", payload.Component)
	}
	if payload.CheckedAt != "2026-03-08T10:00:00Z" {
		t.Fatalf("checked_at=%q, want=2026-03-08T10:00:00Z", payload.CheckedAt)
	}
	if payload.Gateway != server.URL {
		t.Fatalf("gateway=%q, want=%q", payload.Gateway, server.URL)
	}
	if payload.CurrentVersion != "0.9.0" {
		t.Fatalf("current_version=%q, want=0.9.0", payload.CurrentVersion)
	}
	if payload.Channel != "beta" {
		t.Fatalf("channel=%q, want=beta", payload.Channel)
	}
	if payload.OS != "darwin" || payload.Arch != "arm64" {
		t.Fatalf("os/arch=%q/%q, want darwin/arm64", payload.OS, payload.Arch)
	}
	if !payload.UpdateAvailable {
		t.Fatal("update_available=false, want true")
	}
	if payload.Comparison != "upgrade_available" {
		t.Fatalf("comparison=%q, want=upgrade_available", payload.Comparison)
	}
	if payload.Mode != "manual_only" {
		t.Fatalf("mode=%q, want=manual_only", payload.Mode)
	}
	if payload.Instructions != "当前仅提供升级检查结果，不执行真实下载升级。" {
		t.Fatalf("instructions=%q, want expected text", payload.Instructions)
	}
	if payload.LatestRelease.ReleaseID != "release-beta-1" {
		t.Fatalf("latest_release.releaseId=%q, want=release-beta-1", payload.LatestRelease.ReleaseID)
	}
	if payload.LatestRelease.Version != "1.0.0-beta.2" {
		t.Fatalf("latest_release.version=%q, want=1.0.0-beta.2", payload.LatestRelease.Version)
	}
	if payload.SelectedArtifact.DownloadURL != "https://downloads.example.com/agent-darwin-arm64.zip" {
		t.Fatalf("selected_artifact.downloadUrl=%q, want expected url", payload.SelectedArtifact.DownloadURL)
	}
	if payload.SelectedArtifact.RolloutRing != "beta-ring" {
		t.Fatalf("selected_artifact.rolloutRing=%q, want=beta-ring", payload.SelectedArtifact.RolloutRing)
	}
	if payload.SelectedArtifact.RolloutPercentage != 25 {
		t.Fatalf("selected_artifact.rolloutPercentage=%d, want=25", payload.SelectedArtifact.RolloutPercentage)
	}
	if payload.SelectedArtifact.MinAgentVersion != "0.8.0" {
		t.Fatalf("selected_artifact.minAgentVersion=%q, want=0.8.0", payload.SelectedArtifact.MinAgentVersion)
	}
}

func TestUpdateDownloadApplyRollbackAndStatus(t *testing.T) {
	now := time.Now().UTC()
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-update-flow",
		TokenType:   "Bearer",
		ExpiresAt:   now.Add(1 * time.Hour).Format(time.RFC3339),
	})
	configDir := filepath.Join(t.TempDir(), "config")
	currentBinary := filepath.Join(t.TempDir(), "agent-current")
	signaturePublicKeyPath, artifactSignature := writeAgentReleaseSigningKeyForTest(t, []byte("new-binary"))
	if err := os.WriteFile(currentBinary, []byte("old-binary"), 0o700); err != nil {
		t.Fatalf("os.WriteFile(currentBinary) error: %v", err)
	}
	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/system/agent-releases/check":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"checkedAt":       "2026-03-08T11:00:00Z",
				"currentVersion":  "1.0.0",
				"channel":         "stable",
				"os":              "linux",
				"arch":            "amd64",
				"updateAvailable": true,
				"comparison":      "upgrade_available",
				"latestRelease": map[string]any{
					"releaseId":   "release-stable-1",
					"tenantId":    "tenant-a",
					"version":     "1.1.0",
					"channel":     "stable",
					"publishedAt": "2026-03-08T10:00:00Z",
					"artifacts": []map[string]any{
						{
							"os":                 "linux",
							"arch":               "amd64",
							"downloadUrl":        serverURL + "/downloads/agent-linux-amd64",
							"checksumSha256":     sha256Hex([]byte("new-binary")),
							"signature":          artifactSignature,
							"signatureAlgorithm": "ed25519",
							"fileName":           "agent-linux-amd64",
						},
					},
					"createdAt": "2026-03-08T10:00:00Z",
					"updatedAt": "2026-03-08T10:00:00Z",
				},
				"selectedArtifact": map[string]any{
					"os":                 "linux",
					"arch":               "amd64",
					"downloadUrl":        serverURL + "/downloads/agent-linux-amd64",
					"checksumSha256":     sha256Hex([]byte("new-binary")),
					"signature":          artifactSignature,
					"signatureAlgorithm": "ed25519",
					"rolloutRing":        "ring-a",
					"rolloutPercentage":  30,
					"minAgentVersion":    "1.0.0",
					"fileName":           "agent-linux-amd64",
				},
				"instructions": "download/apply manually",
			})
		case "/downloads/agent-linux-amd64":
			_, _ = w.Write([]byte("new-binary"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	downloadExitCode, downloadStdout, downloadStderr := captureOutput(t, func() int {
		return updateCommand([]string{
			"download",
			"--gateway=" + server.URL,
			"--token-file=" + tokenPath,
			"--config-dir=" + configDir,
			"--current-version=1.0.0",
			"--os=linux",
			"--arch=amd64",
			"--signature-public-key-file=" + signaturePublicKeyPath,
		})
	})
	if downloadExitCode != 0 {
		t.Fatalf("update download exit=%d, want=0, stdout=%s stderr=%s", downloadExitCode, downloadStdout, downloadStderr)
	}

	applyExitCode, applyStdout, applyStderr := captureOutput(t, func() int {
		return updateCommand([]string{
			"apply",
			"--config-dir=" + configDir,
			"--binary-path=" + currentBinary,
		})
	})
	if applyExitCode != 0 {
		t.Fatalf("update apply exit=%d, want=0, stdout=%s stderr=%s", applyExitCode, applyStdout, applyStderr)
	}
	updatedBody, err := os.ReadFile(currentBinary)
	if err != nil {
		t.Fatalf("os.ReadFile(updated binary) error: %v", err)
	}
	if string(updatedBody) != "new-binary" {
		t.Fatalf("updated binary=%q, want=new-binary", string(updatedBody))
	}

	statusExitCode, statusOutput := captureStdout(t, func() int {
		return updateCommand([]string{"status", "--config-dir=" + configDir})
	})
	if statusExitCode != 0 {
		t.Fatalf("update status exit=%d, want=0, output=%s", statusExitCode, statusOutput)
	}
	var statusPayload struct {
		Status string `json:"status"`
		Update struct {
			CurrentVersion               string `json:"current_version"`
			DownloadAttempts             int    `json:"download_attempts"`
			DownloadedReleaseID          string `json:"downloaded_release_id"`
			DownloadedSignatureStatus    string `json:"downloaded_signature_status"`
			DownloadedSignatureAlgorithm string `json:"downloaded_signature_algorithm"`
			RolloutRing                  string `json:"rollout_ring"`
			RolloutPercentage            int    `json:"rollout_percentage"`
			MinAgentVersion              string `json:"min_agent_version"`
			AppliedReleaseID             string `json:"applied_release_id"`
		} `json:"update"`
	}
	if err := json.Unmarshal([]byte(statusOutput), &statusPayload); err != nil {
		t.Fatalf("json.Unmarshal(status) error: %v", err)
	}
	if statusPayload.Status != "applied" {
		t.Fatalf("status=%q, want=applied", statusPayload.Status)
	}
	if statusPayload.Update.AppliedReleaseID != "release-stable-1" {
		t.Fatalf("applied_release_id=%q, want=release-stable-1", statusPayload.Update.AppliedReleaseID)
	}
	if statusPayload.Update.DownloadedSignatureStatus != "verified" {
		t.Fatalf("downloaded_signature_status=%q, want=verified", statusPayload.Update.DownloadedSignatureStatus)
	}
	if statusPayload.Update.DownloadedSignatureAlgorithm != "ed25519" {
		t.Fatalf("downloaded_signature_algorithm=%q, want=ed25519", statusPayload.Update.DownloadedSignatureAlgorithm)
	}
	if statusPayload.Update.DownloadAttempts != 1 {
		t.Fatalf("download_attempts=%d, want=1", statusPayload.Update.DownloadAttempts)
	}
	if statusPayload.Update.RolloutRing != "ring-a" {
		t.Fatalf("rollout_ring=%q, want=ring-a", statusPayload.Update.RolloutRing)
	}
	if statusPayload.Update.RolloutPercentage != 30 {
		t.Fatalf("rollout_percentage=%d, want=30", statusPayload.Update.RolloutPercentage)
	}
	if statusPayload.Update.MinAgentVersion != "1.0.0" {
		t.Fatalf("min_agent_version=%q, want=1.0.0", statusPayload.Update.MinAgentVersion)
	}

	rollbackExitCode, rollbackStdout, rollbackStderr := captureOutput(t, func() int {
		return updateCommand([]string{
			"rollback",
			"--config-dir=" + configDir,
			"--binary-path=" + currentBinary,
		})
	})
	if rollbackExitCode != 0 {
		t.Fatalf("update rollback exit=%d, want=0, stdout=%s stderr=%s", rollbackExitCode, rollbackStdout, rollbackStderr)
	}
	rolledBackBody, err := os.ReadFile(currentBinary)
	if err != nil {
		t.Fatalf("os.ReadFile(rolled back binary) error: %v", err)
	}
	if string(rolledBackBody) != "old-binary" {
		t.Fatalf("rolled back binary=%q, want=old-binary", string(rolledBackBody))
	}
	rollbackStatusExitCode, rollbackStatusOutput := captureStdout(t, func() int {
		return updateCommand([]string{"status", "--config-dir=" + configDir})
	})
	if rollbackStatusExitCode != 0 {
		t.Fatalf("update status after rollback exit=%d, want=0, output=%s", rollbackStatusExitCode, rollbackStatusOutput)
	}
	var rollbackStatusPayload struct {
		Status string `json:"status"`
		Update struct {
			CurrentVersion string `json:"current_version"`
		} `json:"update"`
	}
	if err := json.Unmarshal([]byte(rollbackStatusOutput), &rollbackStatusPayload); err != nil {
		t.Fatalf("json.Unmarshal(rollback status) error: %v", err)
	}
	if rollbackStatusPayload.Status != "rolled_back" {
		t.Fatalf("rollback status=%q, want=rolled_back", rollbackStatusPayload.Status)
	}
	if rollbackStatusPayload.Update.CurrentVersion != "1.0.0" {
		t.Fatalf("rollback current_version=%q, want=1.0.0", rollbackStatusPayload.Update.CurrentVersion)
	}
}

func TestUpdateDownloadCommand_FailsWhenSignatureVerificationFails(t *testing.T) {
	now := time.Now().UTC()
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-update-invalid-signature",
		TokenType:   "Bearer",
		ExpiresAt:   now.Add(1 * time.Hour).Format(time.RFC3339),
	})
	configDir := filepath.Join(t.TempDir(), "config")
	signaturePublicKeyPath, _ := writeAgentReleaseSigningKeyForTest(t, []byte("expected-binary"))
	invalidSignature := base64.StdEncoding.EncodeToString([]byte("bad-signature"))
	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/system/agent-releases/check":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"checkedAt":       "2026-03-08T11:30:00Z",
				"currentVersion":  "1.0.0",
				"channel":         "stable",
				"os":              "linux",
				"arch":            "amd64",
				"updateAvailable": true,
				"comparison":      "upgrade_available",
				"latestRelease": map[string]any{
					"releaseId":   "release-stable-bad-signature",
					"tenantId":    "tenant-a",
					"version":     "1.1.0",
					"channel":     "stable",
					"publishedAt": "2026-03-08T10:30:00Z",
					"artifacts": []map[string]any{
						{
							"os":                 "linux",
							"arch":               "amd64",
							"downloadUrl":        serverURL + "/downloads/agent-linux-amd64",
							"checksumSha256":     sha256Hex([]byte("new-binary")),
							"signature":          invalidSignature,
							"signatureAlgorithm": "ed25519",
							"fileName":           "agent-linux-amd64",
						},
					},
					"createdAt": "2026-03-08T10:30:00Z",
					"updatedAt": "2026-03-08T10:30:00Z",
				},
				"selectedArtifact": map[string]any{
					"os":                 "linux",
					"arch":               "amd64",
					"downloadUrl":        serverURL + "/downloads/agent-linux-amd64",
					"checksumSha256":     sha256Hex([]byte("new-binary")),
					"signature":          invalidSignature,
					"signatureAlgorithm": "ed25519",
					"fileName":           "agent-linux-amd64",
				},
				"instructions": "download/apply manually",
			})
		case "/downloads/agent-linux-amd64":
			_, _ = w.Write([]byte("new-binary"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	exitCode, _, stderr := captureOutput(t, func() int {
		return updateCommand([]string{
			"download",
			"--gateway=" + server.URL,
			"--token-file=" + tokenPath,
			"--config-dir=" + configDir,
			"--current-version=1.0.0",
			"--os=linux",
			"--arch=amd64",
			"--signature-public-key-file=" + signaturePublicKeyPath,
		})
	})
	if exitCode != 1 {
		t.Fatalf("update download exit=%d, want=1, stderr=%s", exitCode, stderr)
	}
	if !strings.Contains(stderr, "工件签名校验失败") {
		t.Fatalf("stderr=%q, want contains 工件签名校验失败", stderr)
	}
	statusExitCode, statusOutput := captureStdout(t, func() int {
		return updateCommand([]string{"status", "--config-dir=" + configDir})
	})
	if statusExitCode != 0 {
		t.Fatalf("update status exit=%d, want=0, output=%s", statusExitCode, statusOutput)
	}
	var statusPayload struct {
		Status string `json:"status"`
		Update struct {
			DownloadAttempts      int    `json:"download_attempts"`
			LastDownloadErrorCode string `json:"last_download_error_code"`
		} `json:"update"`
	}
	if err := json.Unmarshal([]byte(statusOutput), &statusPayload); err != nil {
		t.Fatalf("json.Unmarshal(failed status) error: %v", err)
	}
	if statusPayload.Status != "failed" {
		t.Fatalf("status=%q, want=failed", statusPayload.Status)
	}
	if statusPayload.Update.DownloadAttempts != 1 {
		t.Fatalf("download_attempts=%d, want=1", statusPayload.Update.DownloadAttempts)
	}
	if statusPayload.Update.LastDownloadErrorCode != "signature_invalid" {
		t.Fatalf("last_download_error_code=%q, want=signature_invalid", statusPayload.Update.LastDownloadErrorCode)
	}
}

func TestRunCommand_WithQueueFlushesRecoveredEntriesInOrder(t *testing.T) {
	queueDir := filepath.Join(t.TempDir(), "queue")
	oldestTime := time.Date(2026, 3, 7, 9, 0, 0, 0, time.UTC)
	if _, _, err := enqueueAgentQueueRequest(queueDir, ingestBatchRequest{
		BatchID: "batch-recovered",
		Events: []agentEvent{{
			EventID:    "evt-recovered",
			SessionID:  "session-recovered",
			EventType:  "message",
			OccurredAt: oldestTime.Format(time.RFC3339),
		}},
	}, oldestTime); err != nil {
		t.Fatalf("enqueueAgentQueueRequest(recovered) error: %v", err)
	}

	receivedBatchIDs := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request ingestBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("json.NewDecoder(request) error: %v", err)
		}
		receivedBatchIDs = append(receivedBatchIDs, request.BatchID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(ingestBatchResponse{
			BatchID:    request.BatchID,
			Accepted:   len(request.Events),
			Rejected:   0,
			DurationMS: 1,
		})
	}))
	defer server.Close()

	exitCode, _, stderr := captureOutput(t, func() int {
		return runCommand([]string{
			"--protocol=http",
			"--endpoint=" + server.URL,
			"--queue-dir=" + queueDir,
			"--generate=1",
			"--batch-id=batch-current",
			"--session-id=session-current",
		})
	})
	if exitCode != 0 {
		t.Fatalf("runCommand()=%d, want=0, stderr=%s", exitCode, stderr)
	}
	if strings.TrimSpace(stderr) != "" &&
		!strings.Contains(stderr, "提示: 未找到本地 token") {
		t.Fatalf("stderr=%q, want empty or token prompt", stderr)
	}
	if strings.Join(receivedBatchIDs, ",") != "batch-recovered,batch-current" {
		t.Fatalf("received batch ids=%v, want [batch-recovered batch-current]", receivedBatchIDs)
	}
	queueStatus, err := readStatusQueue(queueDir)
	if err != nil {
		t.Fatalf("readStatusQueue() error: %v", err)
	}
	if queueStatus.PendingCount != 0 || queueStatus.TotalBytes != 0 {
		t.Fatalf("queue status after flush=%+v, want empty queue", queueStatus)
	}
}

func TestRunCommand_WithQueueFailureKeepsPendingFile(t *testing.T) {
	queueDir := filepath.Join(t.TempDir(), "queue")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": "queue-send-failed",
		})
	}))
	defer server.Close()

	exitCode, _, stderr := captureOutput(t, func() int {
		return runCommand([]string{
			"--protocol=http",
			"--endpoint=" + server.URL,
			"--queue-dir=" + queueDir,
			"--generate=1",
			"--batch-id=batch-failed",
			"--session-id=session-failed",
		})
	})
	if exitCode != 1 {
		t.Fatalf("runCommand()=%d, want=1, stderr=%s", exitCode, stderr)
	}
	if !strings.Contains(stderr, "队列冲刷失败") {
		t.Fatalf("stderr=%q, want contains 队列冲刷失败", stderr)
	}

	queueStatus, err := readStatusQueue(queueDir)
	if err != nil {
		t.Fatalf("readStatusQueue() error: %v", err)
	}
	if queueStatus.PendingCount != 1 {
		t.Fatalf("queue.pending_count=%d, want=1", queueStatus.PendingCount)
	}
	if queueStatus.TotalBytes <= 0 {
		t.Fatalf("queue.total_bytes=%d, want > 0", queueStatus.TotalBytes)
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

func TestStatusCommand_ReportLifecycleEvent(t *testing.T) {
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-status-lifecycle",
		TokenType:   "Bearer",
		ExpiresAt:   time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339),
	})

	reported := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agents/lifecycle-events" {
			http.NotFound(w, r)
			return
		}
		if got := strings.TrimSpace(r.Header.Get("Authorization")); got != "Bearer token-status-lifecycle" {
			t.Fatalf("authorization=%q, want=%q", got, "Bearer token-status-lifecycle")
		}
		var payload struct {
			TenantID string         `json:"tenantId"`
			AgentID  string         `json:"agentId"`
			Action   string         `json:"action"`
			Result   string         `json:"result"`
			Metadata map[string]any `json:"metadata"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("json.NewDecoder(payload) error: %v", err)
		}
		if payload.TenantID != "tenant-status" {
			t.Fatalf("tenantId=%q, want=tenant-status", payload.TenantID)
		}
		if payload.AgentID != "agent-status" {
			t.Fatalf("agentId=%q, want=agent-status", payload.AgentID)
		}
		if payload.Action != "status" {
			t.Fatalf("action=%q, want=status", payload.Action)
		}
		if payload.Result != "success" {
			t.Fatalf("result=%q, want=success", payload.Result)
		}
		if got, _ := payload.Metadata["command"].(string); got != "status" {
			t.Fatalf("metadata.command=%q, want=status", got)
		}
		reported = true
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	exitCode, output := captureStdout(t, func() int {
		return statusCommand([]string{
			"--token-file=" + tokenPath,
			"--gateway=" + server.URL,
			"--report-lifecycle",
			"--tenant-id=tenant-status",
			"--agent-id=agent-status",
		})
	})
	if exitCode != 0 {
		t.Fatalf("statusCommand()=%d, want=0, output=%s", exitCode, output)
	}
	if !reported {
		t.Fatal("lifecycle report not received")
	}
}

func TestDoctorCommand_ReportLifecycleEvent(t *testing.T) {
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-doctor-lifecycle",
		TokenType:   "Bearer",
		ExpiresAt:   time.Now().UTC().Add(1 * time.Hour).Format(time.RFC3339),
	})

	reported := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		case "/api/v1/agents/lifecycle-events":
			var payload struct {
				TenantID string         `json:"tenantId"`
				AgentID  string         `json:"agentId"`
				Action   string         `json:"action"`
				Result   string         `json:"result"`
				Metadata map[string]any `json:"metadata"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("json.NewDecoder(payload) error: %v", err)
			}
			if payload.Action != "doctor" || payload.Result != "success" {
				t.Fatalf("action/result=%q/%q, want doctor/success", payload.Action, payload.Result)
			}
			if got, _ := payload.Metadata["command"].(string); got != "doctor" {
				t.Fatalf("metadata.command=%q, want=doctor", got)
			}
			reported = true
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	exitCode, output := captureStdout(t, func() int {
		return doctorCommand([]string{
			"--protocol=http",
			"--endpoint=" + server.URL,
			"--token-file=" + tokenPath,
			"--timeout=500ms",
			"--gateway=" + server.URL,
			"--report-lifecycle",
			"--tenant-id=tenant-doctor",
			"--agent-id=agent-doctor",
		})
	})
	if exitCode != 0 {
		t.Fatalf("doctorCommand()=%d, want=0, output=%s", exitCode, output)
	}
	if !reported {
		t.Fatal("doctor lifecycle report not received")
	}
}

func TestUpdateCheckCommand_ReportLifecycleEvent(t *testing.T) {
	now := time.Now().UTC()
	tokenPath := writeTokenFileForTest(t, localToken{
		AccessToken: "token-update-lifecycle",
		TokenType:   "Bearer",
		ObtainedAt:  now.Add(-5 * time.Minute).Format(time.RFC3339),
		ExpiresAt:   now.Add(1 * time.Hour).Format(time.RFC3339),
	})

	reported := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/system/agent-releases/check":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"checkedAt":       "2026-03-08T11:00:00Z",
				"currentVersion":  "1.0.0",
				"channel":         "stable",
				"os":              "linux",
				"arch":            "amd64",
				"updateAvailable": true,
				"comparison":      "upgrade_available",
				"latestRelease": map[string]any{
					"releaseId":   "release-stable-1",
					"tenantId":    "tenant-a",
					"version":     "1.1.0",
					"channel":     "stable",
					"publishedAt": "2026-03-08T10:00:00Z",
					"artifacts": []map[string]any{
						{
							"os":          "linux",
							"arch":        "amd64",
							"downloadUrl": "https://downloads.example.com/agent-linux-amd64.tar.gz",
						},
					},
					"createdAt": "2026-03-08T10:00:00Z",
					"updatedAt": "2026-03-08T10:00:00Z",
				},
				"selectedArtifact": map[string]any{
					"os":          "linux",
					"arch":        "amd64",
					"downloadUrl": "https://downloads.example.com/agent-linux-amd64.tar.gz",
				},
				"instructions": "当前仅提供升级检查结果，不执行真实下载升级。",
			})
		case "/api/v1/agents/lifecycle-events":
			var payload struct {
				Action   string         `json:"action"`
				Result   string         `json:"result"`
				Metadata map[string]any `json:"metadata"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("json.NewDecoder(payload) error: %v", err)
			}
			if payload.Action != "upgrade" || payload.Result != "success" {
				t.Fatalf("action/result=%q/%q, want upgrade/success", payload.Action, payload.Result)
			}
			if got, _ := payload.Metadata["command"].(string); got != "update check" {
				t.Fatalf("metadata.command=%q, want=update check", got)
			}
			reported = true
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	exitCode, output := captureStdout(t, func() int {
		return updateCommand([]string{
			"check",
			"--gateway=" + server.URL,
			"--token-file=" + tokenPath,
			"--current-version=1.0.0",
			"--os=linux",
			"--arch=amd64",
			"--report-lifecycle",
			"--tenant-id=tenant-update",
			"--agent-id=agent-update",
		})
	})
	if exitCode != 0 {
		t.Fatalf("updateCommand(check)=%d, want=0, output=%s", exitCode, output)
	}
	if !reported {
		t.Fatal("update lifecycle report not received")
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

func writeJSONFileForTest(t *testing.T, fileName string, payload map[string]any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), fileName)
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(payload) error: %v", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("os.WriteFile(payload) error: %v", err)
	}
	return path
}

func writeAgentReleaseSigningKeyForTest(t *testing.T, payload []byte) (string, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey() error: %v", err)
	}
	publicKeyDER, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatalf("x509.MarshalPKIXPublicKey() error: %v", err)
	}
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyDER,
	})
	publicKeyPath := filepath.Join(t.TempDir(), "agent-release-public.pem")
	if err := os.WriteFile(publicKeyPath, publicKeyPEM, 0o600); err != nil {
		t.Fatalf("os.WriteFile(publicKeyPath) error: %v", err)
	}
	signature := ed25519.Sign(privateKey, payload)
	return publicKeyPath, base64.StdEncoding.EncodeToString(signature)
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

func captureOutput(t *testing.T, fn func() int) (int, string, string) {
	t.Helper()

	oldStdout := os.Stdout
	oldStderr := os.Stderr
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(stdout) error: %v", err)
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(stderr) error: %v", err)
	}

	os.Stdout = stdoutWriter
	os.Stderr = stderrWriter
	exitCode := fn()
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	os.Stdout = oldStdout
	os.Stderr = oldStderr

	stdoutContent, readStdoutErr := io.ReadAll(stdoutReader)
	_ = stdoutReader.Close()
	if readStdoutErr != nil {
		t.Fatalf("io.ReadAll(stdout) error: %v", readStdoutErr)
	}
	stderrContent, readStderrErr := io.ReadAll(stderrReader)
	_ = stderrReader.Close()
	if readStderrErr != nil {
		t.Fatalf("io.ReadAll(stderr) error: %v", readStderrErr)
	}
	return exitCode, string(stdoutContent), string(stderrContent)
}
