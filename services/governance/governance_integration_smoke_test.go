package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const governanceIntegrationSmokeTimeout = 45 * time.Second

type governanceIntegrationDownstreamRequest struct {
	Path    string
	Headers http.Header
	Body    []byte
}

type governanceIntegrationDownstreamProbe struct {
	server   *httptest.Server
	requests chan governanceIntegrationDownstreamRequest
}

func TestGovernanceIntegrationDownstreamSmoke(t *testing.T) {
	env := newGovernanceE2EEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), governanceIntegrationSmokeTimeout)
	defer cancel()

	downstreamProbe := newGovernanceIntegrationDownstreamProbe(t, http.StatusNoContent)
	ticketPath := "/ticket"
	healthAddr := reserveGovernanceIntegrationTCPAddr(t)
	ensureGovernanceIntegrationDLQStream(t, ctx, env)

	integrationCtx, integrationCancel := context.WithCancel(context.Background())
	defer integrationCancel()

	integrationBin := buildGovernanceIntegrationBinary(t)
	logBuffer := &bytes.Buffer{}
	cmd := exec.CommandContext(integrationCtx, integrationBin)
	cmd.Stdout = logBuffer
	cmd.Stderr = logBuffer
	cmd.Env = append(os.Environ(),
		"APP_ENV=test",
		"GO_ENV=test",
		"SERVICE_NAME=integration-smoke",
		"LOG_LEVEL=info",
		"HTTP_ADDR="+healthAddr,
		"NATS_URL="+env.nc.ConnectedUrl(),
		"CONTROL_PLANE_BASE_URL="+downstreamProbe.server.URL,
		"INTEGRATION_CALLBACK_SECRET=smoke-callback-secret",
		"INTEGRATION_CHANNELS=ticket",
		"INTEGRATION_TICKET_WEBHOOK_URL="+downstreamProbe.server.URL+ticketPath,
		"INTEGRATION_WEBHOOK_TIMEOUT=1s",
		"INTEGRATION_RETRY_MAX=1",
		"INTEGRATION_RETRY_BASE_DELAY=50ms",
		"INTEGRATION_RETRY_MAX_DELAY=100ms",
		"INTEGRATION_CONSUMER_ACK_WAIT=2s",
		"INTEGRATION_DLQ_PUBLISH_TIMEOUT=1s",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start integration service failed: %v", err)
	}

	integrationExited := make(chan struct{})
	integrationErrCh := make(chan error, 1)
	go func() {
		integrationErrCh <- cmd.Wait()
		close(integrationExited)
	}()

	t.Cleanup(func() {
		integrationCancel()
		select {
		case <-integrationExited:
			if err := <-integrationErrCh; err != nil && integrationCtx.Err() == nil {
				t.Logf("integration 进程退出：%v", err)
			}
		case <-time.After(5 * time.Second):
			t.Log("等待 integration 进程退出超时。")
		}
	})

	healthURL := "http://" + healthAddr + "/healthz"
	if err := waitGovernanceIntegrationHealth(ctx, healthURL, integrationExited, logBuffer); err != nil {
		t.Fatalf("wait integration health failed: %v", err)
	}

	tenantID := fmt.Sprintf("tenant-e2e-governance-integration-%d", time.Now().UnixNano())
	alert := newGovernanceE2EAlert(tenantID, 501)
	env.mustInsertRule(t, ctx, governanceE2ERuleSeed{
		TenantID:                 tenantID,
		RuleID:                   fmt.Sprintf("rule-governance-integration-%d", time.Now().UnixNano()),
		EventType:                "alert",
		Severity:                 "critical",
		SourceID:                 asOptionalString(alert.SourceID),
		DedupeWindowSeconds:      0,
		SuppressionWindowSeconds: 0,
		ChannelsJSON:             `["ticket"]`,
	})

	if err := env.svc.publishAlert(ctx, alert); err != nil {
		t.Fatalf("publishAlert failed: %v", err)
	}

	request := downstreamProbe.waitForRequest(t, logBuffer)
	if request.Path != ticketPath {
		t.Fatalf("alert downstream path mismatch: got %q want %q", request.Path, ticketPath)
	}
	var payload struct {
		EventType string `json:"event_type"`
		Severity  string `json:"severity"`
		Status    string `json:"status"`
		Context   struct {
			TenantID string `json:"tenant_id"`
			BudgetID string `json:"budget_id"`
			SourceID string `json:"source_id"`
			AlertID  string `json:"alert_id"`
		} `json:"context"`
	}
	if err := json.Unmarshal(request.Body, &payload); err != nil {
		t.Fatalf("unmarshal downstream payload failed: %v; body=%s", err, string(request.Body))
	}
	if payload.EventType != "alert" {
		t.Fatalf("downstream event type mismatch: got %q want %q", payload.EventType, "alert")
	}
	if payload.Severity != "critical" {
		t.Fatalf("downstream severity mismatch: got %q want %q", payload.Severity, "critical")
	}
	if payload.Status != "open" {
		t.Fatalf("downstream status mismatch: got %q want %q", payload.Status, "open")
	}
	if payload.Context.TenantID != tenantID {
		t.Fatalf("downstream tenant mismatch: got %q want %q", payload.Context.TenantID, tenantID)
	}
	if payload.Context.BudgetID != alert.BudgetID {
		t.Fatalf("downstream budget mismatch: got %q want %q", payload.Context.BudgetID, alert.BudgetID)
	}
	if payload.Context.SourceID != asOptionalString(alert.SourceID) {
		t.Fatalf("downstream source mismatch: got %q want %q", payload.Context.SourceID, asOptionalString(alert.SourceID))
	}
	if payload.Context.AlertID != fmt.Sprintf("%d", alert.AlertID) {
		t.Fatalf("downstream alert mismatch: got %q want %q", payload.Context.AlertID, fmt.Sprintf("%d", alert.AlertID))
	}

	weeklyTenantID := fmt.Sprintf("tenant-e2e-governance-weekly-integration-%d", time.Now().UnixNano())
	env.mustInsertRule(t, ctx, governanceE2ERuleSeed{
		TenantID:                 weeklyTenantID,
		RuleID:                   fmt.Sprintf("rule-governance-weekly-integration-%d", time.Now().UnixNano()),
		EventType:                "weekly",
		DedupeWindowSeconds:      0,
		SuppressionWindowSeconds: 0,
		ChannelsJSON:             `["ticket"]`,
	})
	report := newGovernanceE2EWeeklyReport(weeklyTenantID)
	if _, err := env.svc.publishWeeklyReport(ctx, report); err != nil {
		t.Fatalf("publishWeeklyReport failed: %v", err)
	}

	weeklyRequest := downstreamProbe.waitForRequest(t, logBuffer)
	if weeklyRequest.Path != ticketPath {
		t.Fatalf("weekly downstream path mismatch: got %q want %q", weeklyRequest.Path, ticketPath)
	}
	var weeklyPayload struct {
		EventType string `json:"event_type"`
		Context   struct {
			TenantID string `json:"tenant_id"`
			ReportID string `json:"report_id"`
		} `json:"context"`
	}
	if err := json.Unmarshal(weeklyRequest.Body, &weeklyPayload); err != nil {
		t.Fatalf("unmarshal weekly downstream payload failed: %v; body=%s", err, string(weeklyRequest.Body))
	}
	if weeklyPayload.EventType != "weekly_report" {
		t.Fatalf("weekly downstream event type mismatch: got %q want %q", weeklyPayload.EventType, "weekly_report")
	}
	if weeklyPayload.Context.TenantID != weeklyTenantID {
		t.Fatalf("weekly downstream tenant mismatch: got %q want %q", weeklyPayload.Context.TenantID, weeklyTenantID)
	}
	if weeklyPayload.Context.ReportID != report.ReportID {
		t.Fatalf("weekly downstream report mismatch: got %q want %q", weeklyPayload.Context.ReportID, report.ReportID)
	}
}

func newGovernanceIntegrationDownstreamProbe(t *testing.T, statusCode int) *governanceIntegrationDownstreamProbe {
	t.Helper()

	probe := &governanceIntegrationDownstreamProbe{
		requests: make(chan governanceIntegrationDownstreamRequest, 8),
	}
	probe.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		probe.requests <- governanceIntegrationDownstreamRequest{
			Path:    r.URL.Path,
			Headers: r.Header.Clone(),
			Body:    body,
		}
		w.WriteHeader(statusCode)
	}))
	t.Cleanup(func() {
		probe.server.Close()
	})
	return probe
}

func buildGovernanceIntegrationBinary(t *testing.T) string {
	t.Helper()

	repoRoot := governanceRepoRoot(t)
	outputPath := filepath.Join(t.TempDir(), "integration-smoke")
	buildCmd := exec.Command("go", "build", "-o", outputPath, "./services/integration")
	buildCmd.Dir = repoRoot
	buildOutput, err := buildCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build integration binary failed: %v\n%s", err, string(buildOutput))
	}
	return outputPath
}

func ensureGovernanceIntegrationDLQStream(
	t *testing.T,
	ctx context.Context,
	env *governanceE2EEnv,
) {
	t.Helper()

	_, err := env.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "INTEGRATION_DISPATCH_DLQ",
		Subjects:  []string{"integration.dispatch"},
		Storage:   jetstream.MemoryStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err == nil {
		return
	}

	if _, loadErr := env.js.Stream(ctx, "INTEGRATION_DISPATCH_DLQ"); loadErr != nil {
		t.Fatalf("ensure integration dlq stream failed: create=%v load=%v", err, loadErr)
	}
}

func (p *governanceIntegrationDownstreamProbe) waitForRequest(
	t *testing.T,
	logBuffer *bytes.Buffer,
) governanceIntegrationDownstreamRequest {
	t.Helper()

	select {
	case request := <-p.requests:
		return request
	case <-time.After(5 * time.Second):
		t.Fatalf(
			"wait governance integration downstream request timeout; integration logs:\n%s",
			logBuffer.String(),
		)
		return governanceIntegrationDownstreamRequest{}
	}
}

func reserveGovernanceIntegrationTCPAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen free tcp addr failed: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func governanceRepoRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve governance test file path failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func waitGovernanceIntegrationHealth(
	ctx context.Context,
	healthURL string,
	integrationExited <-chan struct{},
	logBuffer *bytes.Buffer,
) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		select {
		case <-integrationExited:
			return fmt.Errorf("integration 进程在 ready 前退出，日志：%s", logBuffer.String())
		case <-ctx.Done():
			return fmt.Errorf("等待 integration health 超时: %w；日志：%s", ctx.Err(), logBuffer.String())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
