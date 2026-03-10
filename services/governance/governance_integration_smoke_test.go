package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
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

type governanceControlPlaneAuthSession struct {
	AccessToken string
	TenantID    string
	UserID      string
	Email       string
	Password    string
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
	if err := waitGovernanceHTTPHealth(ctx, "integration", healthURL, integrationExited, logBuffer); err != nil {
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

func TestGovernanceControlPlaneIntegrationExternalStatusSyncSmoke(t *testing.T) {
	env := newGovernanceE2EEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	callbackSecret := fmt.Sprintf("smoke-callback-secret-%d", time.Now().UnixNano())
	ticketPath := "/ticket"
	controlPlaneAddr := reserveGovernanceIntegrationTCPAddr(t)
	controlPlanePort := governanceIntegrationAddrPort(t, controlPlaneAddr)
	integrationAddr := reserveGovernanceIntegrationTCPAddr(t)
	ensureGovernanceIntegrationDLQStream(t, ctx, env)

	downstreamProbe := newGovernanceIntegrationDownstreamProbeWithResponder(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"resolved","ticketId":"ticket-sync-1"}`)
	})

	controlPlaneCtx, controlPlaneCancel := context.WithCancel(context.Background())
	defer controlPlaneCancel()

	controlPlaneLogs := &bytes.Buffer{}
	controlPlaneCmd := exec.CommandContext(controlPlaneCtx, "bun", "src/index.ts")
	controlPlaneCmd.Dir = filepath.Join(governanceRepoRoot(t), "apps", "control-plane")
	controlPlaneCmd.Stdout = controlPlaneLogs
	controlPlaneCmd.Stderr = controlPlaneLogs
	controlPlaneCmd.Env = append(os.Environ(),
		"APP_ENV=test",
		"GO_ENV=test",
		"PORT="+controlPlanePort,
		"DATABASE_URL="+os.Getenv(governanceE2EDatabaseURLEnv),
		"NATS_URL="+env.nc.ConnectedUrl(),
		"INTEGRATION_CALLBACK_SECRET="+callbackSecret,
	)
	if err := controlPlaneCmd.Start(); err != nil {
		t.Fatalf("start control-plane failed: %v", err)
	}

	controlPlaneExited := make(chan struct{})
	controlPlaneErrCh := make(chan error, 1)
	go func() {
		controlPlaneErrCh <- controlPlaneCmd.Wait()
		close(controlPlaneExited)
	}()

	t.Cleanup(func() {
		controlPlaneCancel()
		select {
		case <-controlPlaneExited:
			if err := <-controlPlaneErrCh; err != nil && controlPlaneCtx.Err() == nil {
				t.Logf("control-plane 进程退出：%v", err)
			}
		case <-time.After(5 * time.Second):
			t.Log("等待 control-plane 进程退出超时。")
		}
	})

	controlPlaneBaseURL := "http://127.0.0.1:" + controlPlanePort
	if err := waitGovernanceHTTPHealth(
		ctx,
		"control-plane",
		controlPlaneBaseURL+"/api/v1/health",
		controlPlaneExited,
		controlPlaneLogs,
	); err != nil {
		t.Fatalf("wait control-plane health failed: %v", err)
	}

	integrationCtx, integrationCancel := context.WithCancel(context.Background())
	defer integrationCancel()

	integrationBin := buildGovernanceIntegrationBinary(t)
	integrationLogs := &bytes.Buffer{}
	integrationCmd := exec.CommandContext(integrationCtx, integrationBin)
	integrationCmd.Stdout = integrationLogs
	integrationCmd.Stderr = integrationLogs
	integrationCmd.Env = append(os.Environ(),
		"APP_ENV=test",
		"GO_ENV=test",
		"SERVICE_NAME=integration-control-plane-smoke",
		"LOG_LEVEL=info",
		"HTTP_ADDR="+integrationAddr,
		"NATS_URL="+env.nc.ConnectedUrl(),
		"CONTROL_PLANE_BASE_URL="+controlPlaneBaseURL,
		"INTEGRATION_CALLBACK_SECRET="+callbackSecret,
		"INTEGRATION_CHANNELS=ticket",
		"INTEGRATION_TICKET_WEBHOOK_URL="+downstreamProbe.server.URL+ticketPath,
		"INTEGRATION_WEBHOOK_TIMEOUT=1s",
		"INTEGRATION_RETRY_MAX=1",
		"INTEGRATION_RETRY_BASE_DELAY=50ms",
		"INTEGRATION_RETRY_MAX_DELAY=100ms",
		"INTEGRATION_CONSUMER_ACK_WAIT=2s",
		"INTEGRATION_DLQ_PUBLISH_TIMEOUT=1s",
	)
	if err := integrationCmd.Start(); err != nil {
		t.Fatalf("start integration service failed: %v", err)
	}

	integrationExited := make(chan struct{})
	integrationErrCh := make(chan error, 1)
	go func() {
		integrationErrCh <- integrationCmd.Wait()
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

	if err := waitGovernanceHTTPHealth(
		ctx,
		"integration",
		"http://"+integrationAddr+"/healthz",
		integrationExited,
		integrationLogs,
	); err != nil {
		t.Fatalf("wait integration health failed: %v", err)
	}

	authSession := governanceControlPlaneRegisterAndLogin(t, ctx, controlPlaneBaseURL)
	alertSeed := newGovernanceE2EAlert(authSession.TenantID, time.Now().UnixNano())
	env.mustInsertStoredAlert(t, ctx, alertSeed)
	alertID := fmt.Sprintf("%d", alertSeed.AlertID)

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := env.pool.Exec(cleanupCtx, `
DELETE FROM alert_external_links
WHERE tenant_id = $1
  AND alert_id = $2
`, authSession.TenantID, alertID); err != nil {
			t.Logf("cleanup alert_external_links failed: %v", err)
		}
		if _, err := env.pool.Exec(cleanupCtx, `
DELETE FROM governance_alerts
WHERE tenant_id = $1
  AND id = $2::bigint
`, authSession.TenantID, alertID); err != nil {
			t.Logf("cleanup governance_alerts failed: %v", err)
		}
	})

	upsertCallbackPayload := map[string]any{
		"callback_id":     fmt.Sprintf("cb-upsert-%d", time.Now().UnixNano()),
		"tenant_id":       authSession.TenantID,
		"action":          "upsert_external_link",
		"alert_id":        alertID,
		"external_type":   "ticket",
		"external_system": "ticket",
		"external_id":     "ticket-sync-1",
		"external_status": "open",
	}
	upsertResponse := governanceControlPlanePostCallback(
		t,
		ctx,
		controlPlaneBaseURL,
		callbackSecret,
		upsertCallbackPayload,
	)
	if upsertResponse.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(upsertResponse.Body)
		_ = upsertResponse.Body.Close()
		t.Fatalf("upsert external link callback failed: status=%d body=%s", upsertResponse.StatusCode, string(body))
	}
	_ = upsertResponse.Body.Close()

	patchResponse := governanceControlPlanePatchAlertStatus(
		t,
		ctx,
		controlPlaneBaseURL,
		authSession.AccessToken,
		alertID,
		"resolved",
	)
	if patchResponse.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(patchResponse.Body)
		_ = patchResponse.Body.Close()
		t.Fatalf("patch alert status failed: status=%d body=%s", patchResponse.StatusCode, string(body))
	}
	var patchedAlert governanceControlPlaneAlertRecord
	if err := json.NewDecoder(patchResponse.Body).Decode(&patchedAlert); err != nil {
		_ = patchResponse.Body.Close()
		t.Fatalf("decode patched alert failed: %v", err)
	}
	_ = patchResponse.Body.Close()
	if got := governanceControlPlaneFindExternalLink(patchedAlert.ExternalLinks, "ticket", "ticket-sync-1"); got == nil {
		t.Fatalf("patched alert missing external link: %+v", patchedAlert.ExternalLinks)
	} else {
		if got.ExternalStatus != "open" {
			t.Fatalf("patched alert externalStatus mismatch: got %q want %q", got.ExternalStatus, "open")
		}
		if got.PendingExternalStatus != "resolved" {
			t.Fatalf("patched alert pendingExternalStatus mismatch: got %q want %q", got.PendingExternalStatus, "resolved")
		}
		if got.PublishStatus != "success" {
			t.Fatalf("patched alert publishStatus mismatch: got %q want %q", got.PublishStatus, "success")
		}
	}

	request := downstreamProbe.waitForRequest(t, integrationLogs)
	if request.Path != ticketPath {
		t.Fatalf("external status sync downstream path mismatch: got %q want %q", request.Path, ticketPath)
	}

	var downstreamPayload map[string]any
	if err := json.Unmarshal(request.Body, &downstreamPayload); err != nil {
		t.Fatalf("unmarshal downstream external status sync payload failed: %v; body=%s", err, string(request.Body))
	}
	if got := strings.TrimSpace(asString(downstreamPayload["callback_id"])); got == "" {
		t.Fatalf("downstream payload missing callback_id: %+v", downstreamPayload)
	}
	if got := strings.TrimSpace(asString(downstreamPayload["tenant_id"])); got != authSession.TenantID {
		t.Fatalf("downstream tenant mismatch: got %q want %q", got, authSession.TenantID)
	}
	if got := strings.TrimSpace(asString(downstreamPayload["alert_id"])); got != alertID {
		t.Fatalf("downstream alert mismatch: got %q want %q", got, alertID)
	}
	if got := strings.TrimSpace(asString(downstreamPayload["external_type"])); got != "ticket" {
		t.Fatalf("downstream external_type mismatch: got %q want %q", got, "ticket")
	}
	if got := strings.TrimSpace(asString(downstreamPayload["external_id"])); got != "ticket-sync-1" {
		t.Fatalf("downstream external_id mismatch: got %q want %q", got, "ticket-sync-1")
	}
	if got := strings.TrimSpace(asString(downstreamPayload["external_status"])); got != "resolved" {
		t.Fatalf("downstream external_status mismatch: got %q want %q", got, "resolved")
	}

	refreshedAlert := governanceControlPlaneWaitForAlertExternalLinkState(
		t,
		ctx,
		controlPlaneBaseURL,
		authSession.AccessToken,
		alertID,
		func(link governanceControlPlaneExternalLinkRecord) bool {
			return link.ExternalType == "ticket" &&
				link.ExternalID == "ticket-sync-1" &&
				link.ExternalStatus == "resolved" &&
				link.PendingExternalStatus == "" &&
				link.LastSyncResult == "success" &&
				link.PublishStatus == "success"
		},
		controlPlaneLogs,
		integrationLogs,
	)
	link := governanceControlPlaneFindExternalLink(refreshedAlert.ExternalLinks, "ticket", "ticket-sync-1")
	if link == nil {
		t.Fatalf("refreshed alert missing external link: %+v", refreshedAlert.ExternalLinks)
	}
}

func newGovernanceIntegrationDownstreamProbe(t *testing.T, statusCode int) *governanceIntegrationDownstreamProbe {
	return newGovernanceIntegrationDownstreamProbeWithResponder(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		w.WriteHeader(statusCode)
	})
}

func newGovernanceIntegrationDownstreamProbeWithResponder(
	t *testing.T,
	responder func(http.ResponseWriter, *http.Request, []byte),
) *governanceIntegrationDownstreamProbe {
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
		responder(w, r, body)
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

func waitGovernanceHTTPHealth(
	ctx context.Context,
	serviceName string,
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
			return fmt.Errorf("%s 进程在 ready 前退出，日志：%s", serviceName, logBuffer.String())
		case <-ctx.Done():
			return fmt.Errorf("等待 %s health 超时: %w；日志：%s", serviceName, ctx.Err(), logBuffer.String())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

type governanceControlPlaneAuthRegisterResponse struct {
	User struct {
		UserID   string `json:"userId"`
		TenantID string `json:"tenantId"`
		Email    string `json:"email"`
	} `json:"user"`
	Tokens struct {
		AccessToken string `json:"accessToken"`
	} `json:"tokens"`
}

type governanceControlPlaneAlertRecord struct {
	ID            string                                     `json:"id"`
	Status        string                                     `json:"status"`
	ExternalLinks []governanceControlPlaneExternalLinkRecord `json:"externalLinks"`
}

type governanceControlPlaneExternalLinkRecord struct {
	ExternalType          string `json:"externalType"`
	ExternalID            string `json:"externalId"`
	ExternalStatus        string `json:"externalStatus"`
	PendingExternalStatus string `json:"pendingExternalStatus"`
	PublishStatus         string `json:"publishStatus"`
	LastSyncResult        string `json:"lastSyncResult"`
}

func governanceControlPlaneRegisterAndLogin(
	t *testing.T,
	ctx context.Context,
	baseURL string,
) governanceControlPlaneAuthSession {
	t.Helper()

	email := fmt.Sprintf("governance-integration-smoke-%d@example.com", time.Now().UnixNano())
	password := "SmokePassw0rd!"
	payload := map[string]string{
		"email":       email,
		"password":    password,
		"displayName": "Governance Integration Smoke",
	}
	response := governanceJSONRequest(t, ctx, http.MethodPost, baseURL+"/api/v1/auth/register", "", payload, nil)
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("register user failed: status=%d body=%s", response.StatusCode, string(body))
	}

	var register governanceControlPlaneAuthRegisterResponse
	if err := json.NewDecoder(response.Body).Decode(&register); err != nil {
		t.Fatalf("decode register response failed: %v", err)
	}
	if strings.TrimSpace(register.Tokens.AccessToken) == "" {
		t.Fatal("register response missing access token")
	}
	if strings.TrimSpace(register.User.TenantID) == "" {
		t.Fatal("register response missing tenantId")
	}
	if strings.TrimSpace(register.User.UserID) == "" {
		t.Fatal("register response missing userId")
	}

	return governanceControlPlaneAuthSession{
		AccessToken: register.Tokens.AccessToken,
		TenantID:    register.User.TenantID,
		UserID:      register.User.UserID,
		Email:       email,
		Password:    password,
	}
}

func governanceControlPlanePostCallback(
	t *testing.T,
	ctx context.Context,
	baseURL string,
	secret string,
	payload map[string]any,
) *http.Response {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal callback payload failed: %v", err)
	}
	timestamp := time.Now().UTC().Format(time.RFC3339)
	nonce := fmt.Sprintf("nonce-%d", time.Now().UnixNano())
	signature := governanceIntegrationCallbackSignature(secret, timestamp, nonce, body)

	headers := map[string]string{
		"content-type":                     "application/json",
		"x-integration-callback-secret":    secret,
		"x-integration-callback-timestamp": timestamp,
		"x-integration-callback-nonce":     nonce,
		"x-integration-callback-signature": signature,
	}
	return governanceJSONRequest(
		t,
		ctx,
		http.MethodPost,
		baseURL+"/api/v1/integrations/callbacks/alerts",
		"",
		json.RawMessage(body),
		headers,
	)
}

func governanceControlPlanePatchAlertStatus(
	t *testing.T,
	ctx context.Context,
	baseURL string,
	accessToken string,
	alertID string,
	status string,
) *http.Response {
	t.Helper()

	return governanceJSONRequest(
		t,
		ctx,
		http.MethodPatch,
		baseURL+"/api/v1/alerts/"+alertID+"/status",
		accessToken,
		map[string]string{"status": status},
		map[string]string{"content-type": "application/json"},
	)
}

func governanceControlPlaneWaitForAlertExternalLinkState(
	t *testing.T,
	ctx context.Context,
	baseURL string,
	accessToken string,
	alertID string,
	match func(governanceControlPlaneExternalLinkRecord) bool,
	controlPlaneLogs *bytes.Buffer,
	integrationLogs *bytes.Buffer,
) governanceControlPlaneAlertRecord {
	t.Helper()

	client := &http.Client{Timeout: time.Second}
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v1/alerts?limit=50", nil)
		if err != nil {
			t.Fatalf("build alerts list request failed: %v", err)
		}
		request.Header.Set("Authorization", "Bearer "+accessToken)
		response, err := client.Do(request)
		if err == nil {
			var payload struct {
				Items []governanceControlPlaneAlertRecord `json:"items"`
			}
			if response.StatusCode == http.StatusOK {
				if decodeErr := json.NewDecoder(response.Body).Decode(&payload); decodeErr == nil {
					_ = response.Body.Close()
					for _, alert := range payload.Items {
						if alert.ID != alertID {
							continue
						}
						for _, link := range alert.ExternalLinks {
							if match(link) {
								return alert
							}
						}
					}
				} else {
					_ = response.Body.Close()
				}
			} else {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf(
				"wait alert external link state timeout: %v\ncontrol-plane logs:\n%s\nintegration logs:\n%s",
				ctx.Err(),
				controlPlaneLogs.String(),
				integrationLogs.String(),
			)
		case <-time.After(150 * time.Millisecond):
		}
	}
}

func governanceControlPlaneFindExternalLink(
	links []governanceControlPlaneExternalLinkRecord,
	externalType string,
	externalID string,
) *governanceControlPlaneExternalLinkRecord {
	for index := range links {
		if links[index].ExternalType == externalType && links[index].ExternalID == externalID {
			return &links[index]
		}
	}
	return nil
}

func governanceJSONRequest(
	t *testing.T,
	ctx context.Context,
	method string,
	url string,
	accessToken string,
	payload any,
	extraHeaders map[string]string,
) *http.Response {
	t.Helper()

	var bodyReader io.Reader
	if payload != nil {
		body, err := governanceJSONBody(payload)
		if err != nil {
			t.Fatalf("marshal request payload failed: %v", err)
		}
		bodyReader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	if strings.TrimSpace(accessToken) != "" {
		request.Header.Set("Authorization", "Bearer "+accessToken)
	}
	for key, value := range extraHeaders {
		request.Header.Set(key, value)
	}
	if payload != nil && request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("perform request failed: %v", err)
	}
	return response
}

func governanceJSONBody(payload any) ([]byte, error) {
	switch typed := payload.(type) {
	case json.RawMessage:
		return typed, nil
	case []byte:
		return typed, nil
	default:
		return json.Marshal(payload)
	}
}

func governanceIntegrationCallbackSignature(
	secret string,
	timestamp string,
	nonce string,
	body []byte,
) string {
	mac := governanceIntegrationHMAC([]byte(secret))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("\n"))
	_, _ = mac.Write([]byte(nonce))
	_, _ = mac.Write([]byte("\n"))
	_, _ = mac.Write(body)
	return fmt.Sprintf("%x", mac.Sum(nil))
}

func governanceIntegrationHMAC(secret []byte) hash.Hash {
	return hmac.New(sha256.New, secret)
}

func governanceIntegrationAddrPort(t *testing.T, addr string) string {
	t.Helper()

	parts := strings.Split(addr, ":")
	if len(parts) == 0 {
		t.Fatalf("invalid tcp addr: %q", addr)
	}
	return parts[len(parts)-1]
}
