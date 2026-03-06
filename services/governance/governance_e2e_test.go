package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	nserver "github.com/nats-io/nats-server/v2/server"
)

const (
	governanceE2EFlagEnv        = "AGENTLEDGER_E2E"
	governanceE2EDatabaseURLEnv = "GOV_E2E_DATABASE_URL"
	governanceE2ETimeout        = 10 * time.Second
)

type governanceE2EEnv struct {
	pool *pgxpool.Pool
	nc   *nats.Conn
	js   jetstream.JetStream
	svc  *governanceService
}

func TestGovernanceE2EAlertFallbackPublishesAndLogs(t *testing.T) {
	env := newGovernanceE2EEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), governanceE2ETimeout)
	defer cancel()

	tenantID := fmt.Sprintf("tenant-e2e-alert-fallback-%d", time.Now().UnixNano())
	consumer := env.mustConsumer(t, ctx, alertsStreamName, alertsSubject, "alert-fallback")
	alert := newGovernanceE2EAlert(tenantID, 101)

	if err := env.svc.publishAlert(ctx, alert); err != nil {
		t.Fatalf("publishAlert failed: %v", err)
	}

	published := env.mustFetchPublishedAlert(t, consumer)
	if published.Orchestration == nil || !published.Orchestration.Fallback {
		t.Fatalf("expected fallback orchestration, got %+v", published.Orchestration)
	}

	row := env.mustLoadLatestExecution(t, ctx, tenantID, "alert")
	if row.RuleID != fallbackExecutionRuleID("alert") {
		t.Fatalf("fallback rule id mismatch: got %q want %q", row.RuleID, fallbackExecutionRuleID("alert"))
	}
	if dispatchMode := strings.TrimSpace(asString(row.Metadata["dispatchMode"])); dispatchMode != "fallback" {
		t.Fatalf("fallback dispatch mode mismatch: got %q want %q", dispatchMode, "fallback")
	}
	if row.DedupeHit || row.Suppressed {
		t.Fatalf("fallback execution should not be deduped/suppressed: %+v", row)
	}
	if got := env.mustCountAuditLogs(t, ctx, tenantID, alertDispatchedAuditAction); got != 1 {
		t.Fatalf("fallback audit count mismatch: got %d want %d", got, 1)
	}
}

func TestGovernanceE2EAlertDedupePublishesEmptyChannelsButRecordsHit(t *testing.T) {
	env := newGovernanceE2EEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), governanceE2ETimeout)
	defer cancel()

	tenantID := fmt.Sprintf("tenant-e2e-alert-dedupe-%d", time.Now().UnixNano())
	alert := newGovernanceE2EAlert(tenantID, 201)
	ruleID := fmt.Sprintf("rule-alert-dedupe-%d", time.Now().UnixNano())
	matchKey := buildAlertOrchestrationMatchKey(alert)
	env.mustInsertRule(t, ctx, governanceE2ERuleSeed{
		TenantID:                 tenantID,
		RuleID:                   ruleID,
		EventType:                "alert",
		Severity:                 "critical",
		SourceID:                 asOptionalString(alert.SourceID),
		DedupeWindowSeconds:      3600,
		SuppressionWindowSeconds: 0,
		ChannelsJSON:             `["webhook","email"]`,
	})
	env.mustInsertExecution(t, ctx, governanceE2EExecutionSeed{
		TenantID:     tenantID,
		RuleID:       ruleID,
		EventType:    "alert",
		AlertID:      fmt.Sprintf("%d", alert.AlertID-1),
		Severity:     "critical",
		SourceID:     asOptionalString(alert.SourceID),
		MetadataJSON: fmt.Sprintf(`{"matchKey":%q,"dispatchMode":"rule"}`, matchKey),
		CreatedAt:    alert.EvaluatedAt.Add(-5 * time.Minute),
	})

	consumer := env.mustConsumer(t, ctx, alertsStreamName, alertsSubject, "alert-dedupe")
	if err := env.svc.publishAlert(ctx, alert); err != nil {
		t.Fatalf("publishAlert failed: %v", err)
	}

	published := env.mustFetchPublishedAlert(t, consumer)
	if published.Orchestration == nil {
		t.Fatalf("expected orchestration payload")
	}
	if !published.Orchestration.DedupeHit || published.Orchestration.Suppressed || published.Orchestration.Fallback {
		t.Fatalf("unexpected dedupe orchestration payload: %+v", published.Orchestration)
	}
	if len(published.Orchestration.Channels) != 0 {
		t.Fatalf("dedupe publish should have no active channels, got %+v", published.Orchestration.Channels)
	}

	row := env.mustLoadLatestExecution(t, ctx, tenantID, "alert")
	if row.RuleID != ruleID {
		t.Fatalf("dedupe rule id mismatch: got %q want %q", row.RuleID, ruleID)
	}
	if !row.DedupeHit || row.Suppressed {
		t.Fatalf("dedupe execution flags mismatch: %+v", row)
	}
	if dispatchMode := strings.TrimSpace(asString(row.Metadata["dispatchMode"])); dispatchMode != "rule" {
		t.Fatalf("dedupe dispatch mode mismatch: got %q want %q", dispatchMode, "rule")
	}
}

func TestGovernanceE2EAlertSuppressedPublishesEmptyChannelsButRecordsSuppression(t *testing.T) {
	env := newGovernanceE2EEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), governanceE2ETimeout)
	defer cancel()

	tenantID := fmt.Sprintf("tenant-e2e-alert-suppressed-%d", time.Now().UnixNano())
	alert := newGovernanceE2EAlert(tenantID, 301)
	ruleID := fmt.Sprintf("rule-alert-suppressed-%d", time.Now().UnixNano())
	matchKey := buildAlertOrchestrationMatchKey(alert)
	env.mustInsertRule(t, ctx, governanceE2ERuleSeed{
		TenantID:                 tenantID,
		RuleID:                   ruleID,
		EventType:                "alert",
		Severity:                 "critical",
		SourceID:                 asOptionalString(alert.SourceID),
		DedupeWindowSeconds:      0,
		SuppressionWindowSeconds: 3600,
		ChannelsJSON:             `["wecom"]`,
	})
	env.mustInsertExecution(t, ctx, governanceE2EExecutionSeed{
		TenantID:     tenantID,
		RuleID:       ruleID,
		EventType:    "alert",
		AlertID:      fmt.Sprintf("%d", alert.AlertID-1),
		Severity:     "critical",
		SourceID:     asOptionalString(alert.SourceID),
		MetadataJSON: fmt.Sprintf(`{"matchKey":%q,"dispatchMode":"rule"}`, matchKey),
		CreatedAt:    alert.EvaluatedAt.Add(-5 * time.Minute),
	})

	consumer := env.mustConsumer(t, ctx, alertsStreamName, alertsSubject, "alert-suppressed")
	if err := env.svc.publishAlert(ctx, alert); err != nil {
		t.Fatalf("publishAlert failed: %v", err)
	}

	published := env.mustFetchPublishedAlert(t, consumer)
	if published.Orchestration == nil {
		t.Fatalf("expected orchestration payload")
	}
	if published.Orchestration.DedupeHit || !published.Orchestration.Suppressed || published.Orchestration.Fallback {
		t.Fatalf("unexpected suppressed orchestration payload: %+v", published.Orchestration)
	}
	if len(published.Orchestration.Channels) != 0 {
		t.Fatalf("suppressed publish should have no active channels, got %+v", published.Orchestration.Channels)
	}

	row := env.mustLoadLatestExecution(t, ctx, tenantID, "alert")
	if row.RuleID != ruleID {
		t.Fatalf("suppressed rule id mismatch: got %q want %q", row.RuleID, ruleID)
	}
	if row.DedupeHit || !row.Suppressed {
		t.Fatalf("suppressed execution flags mismatch: %+v", row)
	}
}

func TestGovernanceE2EAlertFailOpenStillPublishes(t *testing.T) {
	env := newGovernanceE2EEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), governanceE2ETimeout)
	defer cancel()

	tenantID := fmt.Sprintf("tenant-e2e-alert-fail-open-%d", time.Now().UnixNano())
	alert := newGovernanceE2EAlert(tenantID, 401)
	env.mustInsertRule(t, ctx, governanceE2ERuleSeed{
		TenantID:                 tenantID,
		RuleID:                   fmt.Sprintf("rule-alert-fail-open-%d", time.Now().UnixNano()),
		EventType:                "alert",
		Severity:                 "critical",
		SourceID:                 asOptionalString(alert.SourceID),
		DedupeWindowSeconds:      60,
		SuppressionWindowSeconds: 60,
		ChannelsJSON:             `{"invalid":true}`,
	})

	consumer := env.mustConsumer(t, ctx, alertsStreamName, alertsSubject, "alert-fail-open")
	if err := env.svc.publishAlert(ctx, alert); err != nil {
		t.Fatalf("publishAlert failed: %v", err)
	}

	published := env.mustFetchPublishedAlert(t, consumer)
	if published.Orchestration == nil || !published.Orchestration.Fallback {
		t.Fatalf("fail-open should publish fallback orchestration, got %+v", published.Orchestration)
	}
	if got := env.mustCountExecutions(t, ctx, tenantID, "alert"); got != 0 {
		t.Fatalf("fail-open should not write orchestration execution rows before fallback, got %d", got)
	}
	if got := env.mustCountAuditLogs(t, ctx, tenantID, alertDispatchedAuditAction); got != 1 {
		t.Fatalf("fail-open dispatch audit count mismatch: got %d want %d", got, 1)
	}
}

func TestGovernanceE2EWeeklyRulePublishesAndRecordsExecution(t *testing.T) {
	env := newGovernanceE2EEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), governanceE2ETimeout)
	defer cancel()

	tenantID := fmt.Sprintf("tenant-e2e-weekly-rule-%d", time.Now().UnixNano())
	ruleID := fmt.Sprintf("rule-weekly-%d", time.Now().UnixNano())
	env.mustInsertRule(t, ctx, governanceE2ERuleSeed{
		TenantID:                 tenantID,
		RuleID:                   ruleID,
		EventType:                "weekly",
		DedupeWindowSeconds:      0,
		SuppressionWindowSeconds: 0,
		ChannelsJSON:             `["email"]`,
	})

	consumer := env.mustConsumer(t, ctx, weeklyReportsStreamName, weeklyReportsSubject, "weekly-rule")
	report := newGovernanceE2EWeeklyReport(tenantID)
	if _, err := env.svc.publishWeeklyReport(ctx, report); err != nil {
		t.Fatalf("publishWeeklyReport failed: %v", err)
	}

	published := env.mustFetchPublishedWeeklyReport(t, consumer)
	if published.Orchestration == nil || published.Orchestration.Fallback {
		t.Fatalf("weekly orchestration fallback mismatch: %+v", published.Orchestration)
	}
	if len(published.Orchestration.Channels) != 1 || published.Orchestration.Channels[0] != "email" {
		t.Fatalf("weekly channels mismatch: %+v", published.Orchestration.Channels)
	}
	if len(published.Orchestration.MatchedRuleIDs) != 1 || published.Orchestration.MatchedRuleIDs[0] != ruleID {
		t.Fatalf("weekly matched rules mismatch: %+v", published.Orchestration.MatchedRuleIDs)
	}

	row := env.mustLoadLatestExecution(t, ctx, tenantID, "weekly")
	if row.RuleID != ruleID {
		t.Fatalf("weekly rule id mismatch: got %q want %q", row.RuleID, ruleID)
	}
	if dispatchMode := strings.TrimSpace(asString(row.Metadata["dispatchMode"])); dispatchMode != "rule" {
		t.Fatalf("weekly dispatch mode mismatch: got %q want %q", dispatchMode, "rule")
	}
	if got := env.mustCountAuditLogs(t, ctx, tenantID, weeklyReportAuditAction); got != 1 {
		t.Fatalf("weekly audit count mismatch: got %d want %d", got, 1)
	}
}

func newGovernanceE2EEnv(t *testing.T) *governanceE2EEnv {
	t.Helper()

	if os.Getenv(governanceE2EFlagEnv) != "1" {
		t.Skipf("%s != 1，跳过真实 PG/NATS E2E。", governanceE2EFlagEnv)
	}

	databaseURL := strings.TrimSpace(os.Getenv(governanceE2EDatabaseURLEnv))
	if databaseURL == "" {
		t.Skipf("%s 未配置，跳过真实 PG/NATS E2E。", governanceE2EDatabaseURLEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), governanceE2ETimeout)
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse %s failed: %v", governanceE2EDatabaseURLEnv, err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("connect postgres failed: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping postgres failed: %v", err)
	}

	if err := applyGovernanceE2ESchema(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("apply governance e2e schema failed: %v", err)
	}

	server, err := nserver.NewServer(&nserver.Options{
		ServerName: "agentledger-governance-e2e",
		Host:       "127.0.0.1",
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		NoLog:      true,
		NoSigs:     true,
	})
	if err != nil {
		pool.Close()
		t.Fatalf("create embedded nats server failed: %v", err)
	}
	go server.Start()
	if !server.ReadyForConnections(governanceE2ETimeout) {
		server.Shutdown()
		pool.Close()
		t.Fatalf("embedded nats server not ready")
	}

	nc, err := nats.Connect(server.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		server.Shutdown()
		pool.Close()
		t.Fatalf("connect embedded nats failed: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		server.Shutdown()
		pool.Close()
		t.Fatalf("create jetstream client failed: %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := ensureAlertsStream(ctx, js, log); err != nil {
		nc.Close()
		server.Shutdown()
		pool.Close()
		t.Fatalf("ensure alerts stream failed: %v", err)
	}
	if err := ensureWeeklyReportsStream(ctx, js, log); err != nil {
		nc.Close()
		server.Shutdown()
		pool.Close()
		t.Fatalf("ensure weekly reports stream failed: %v", err)
	}

	env := &governanceE2EEnv{
		pool: pool,
		nc:   nc,
		js:   js,
		svc: &governanceService{
			log:                  log,
			pool:                 pool,
			js:                   js,
			weeklyReportSchedule: weeklyReportSchedule{Weekday: time.Monday, Hour: 9, Minute: 0},
			weeklyReportDedupe:   newWeeklyReportDedupeCache(),
		},
	}
	t.Cleanup(func() {
		env.nc.Close()
		server.Shutdown()
		env.pool.Close()
	})
	return env
}

func applyGovernanceE2ESchema(ctx context.Context, pool *pgxpool.Pool) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS alert_orchestration_rules (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      name TEXT NOT NULL,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      event_type TEXT NOT NULL,
      severity TEXT,
      source_id TEXT,
      dedupe_window_seconds INTEGER NOT NULL DEFAULT 0,
      suppression_window_seconds INTEGER NOT NULL DEFAULT 0,
      merge_window_seconds INTEGER NOT NULL DEFAULT 0,
      channels JSONB NOT NULL DEFAULT '[]'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,
		`CREATE TABLE IF NOT EXISTS alert_orchestration_executions (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      rule_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      alert_id TEXT,
      severity TEXT,
      source_id TEXT,
      channels JSONB NOT NULL DEFAULT '[]'::jsonb,
      conflict_rule_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
      dedupe_hit BOOLEAN NOT NULL DEFAULT FALSE,
      suppressed BOOLEAN NOT NULL DEFAULT FALSE,
      simulated BOOLEAN NOT NULL DEFAULT FALSE,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,
		`CREATE TABLE IF NOT EXISTS audit_logs (
      id TEXT PRIMARY KEY,
      event_id TEXT,
      action TEXT NOT NULL,
      level TEXT NOT NULL,
      detail TEXT NOT NULL,
      tenant_id TEXT NOT NULL,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`,
	}

	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

type governanceE2ERuleSeed struct {
	TenantID                 string
	RuleID                   string
	EventType                string
	Severity                 string
	SourceID                 string
	DedupeWindowSeconds      int
	SuppressionWindowSeconds int
	ChannelsJSON             string
}

func (e *governanceE2EEnv) mustInsertRule(t *testing.T, ctx context.Context, seed governanceE2ERuleSeed) {
	t.Helper()
	_, err := e.pool.Exec(ctx, `
INSERT INTO alert_orchestration_rules (
  id,
  tenant_id,
  name,
  enabled,
  event_type,
  severity,
  source_id,
  dedupe_window_seconds,
  suppression_window_seconds,
  merge_window_seconds,
  channels,
  updated_at
)
VALUES ($1, $2, $3, TRUE, $4, $5, $6, $7, $8, 0, $9::jsonb, $10)
`,
		seed.RuleID,
		normalizeTenantID(seed.TenantID),
		seed.RuleID,
		strings.TrimSpace(seed.EventType),
		nullableString(seed.Severity),
		nullableString(seed.SourceID),
		seed.DedupeWindowSeconds,
		seed.SuppressionWindowSeconds,
		seed.ChannelsJSON,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("insert orchestration rule failed: %v", err)
	}
}

type governanceE2EExecutionSeed struct {
	TenantID     string
	RuleID       string
	EventType    string
	AlertID      string
	Severity     string
	SourceID     string
	MetadataJSON string
	CreatedAt    time.Time
}

func (e *governanceE2EEnv) mustInsertExecution(
	t *testing.T,
	ctx context.Context,
	seed governanceE2EExecutionSeed,
) {
	t.Helper()
	_, err := e.pool.Exec(ctx, `
INSERT INTO alert_orchestration_executions (
  id,
  tenant_id,
  rule_id,
  event_type,
  alert_id,
  severity,
  source_id,
  channels,
  conflict_rule_ids,
  dedupe_hit,
  suppressed,
  simulated,
  metadata,
  created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, '[]'::jsonb, '[]'::jsonb, FALSE, FALSE, FALSE, $8::jsonb, $9)
`,
		fmt.Sprintf("exec-seed-%d", time.Now().UnixNano()),
		normalizeTenantID(seed.TenantID),
		seed.RuleID,
		seed.EventType,
		nullableString(seed.AlertID),
		nullableString(seed.Severity),
		nullableString(seed.SourceID),
		seed.MetadataJSON,
		seed.CreatedAt.UTC(),
	)
	if err != nil {
		t.Fatalf("insert orchestration execution failed: %v", err)
	}
}

func (e *governanceE2EEnv) mustConsumer(
	t *testing.T,
	ctx context.Context,
	stream string,
	subject string,
	name string,
) jetstream.Consumer {
	t.Helper()
	durable := fmt.Sprintf("e2e-%s-%d", name, time.Now().UnixNano())
	if _, err := e.js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
	}); err != nil {
		t.Fatalf("create consumer failed: %v", err)
	}
	consumer, err := e.js.Consumer(ctx, stream, durable)
	if err != nil {
		t.Fatalf("load consumer failed: %v", err)
	}
	return consumer
}

func (e *governanceE2EEnv) mustFetchPublishedAlert(
	t *testing.T,
	consumer jetstream.Consumer,
) alertEvent {
	t.Helper()
	msg, err := consumer.Next(jetstream.FetchMaxWait(3 * time.Second))
	if err != nil {
		t.Fatalf("fetch published alert failed: %v", err)
	}
	defer func() {
		_ = msg.Ack()
	}()

	var alert alertEvent
	if err := json.Unmarshal(msg.Data(), &alert); err != nil {
		t.Fatalf("unmarshal alert payload failed: %v", err)
	}
	return alert
}

func (e *governanceE2EEnv) mustFetchPublishedWeeklyReport(
	t *testing.T,
	consumer jetstream.Consumer,
) weeklyReportEvent {
	t.Helper()
	msg, err := consumer.Next(jetstream.FetchMaxWait(3 * time.Second))
	if err != nil {
		t.Fatalf("fetch published weekly report failed: %v", err)
	}
	defer func() {
		_ = msg.Ack()
	}()

	var report weeklyReportEvent
	if err := json.Unmarshal(msg.Data(), &report); err != nil {
		t.Fatalf("unmarshal weekly report payload failed: %v", err)
	}
	return report
}

type governanceE2EExecutionRow struct {
	RuleID    string
	DedupeHit bool
	Suppressed bool
	Metadata  map[string]any
}

func (e *governanceE2EEnv) mustLoadLatestExecution(
	t *testing.T,
	ctx context.Context,
	tenantID string,
	eventType string,
) governanceE2EExecutionRow {
	t.Helper()

	var (
		row governanceE2EExecutionRow
		raw []byte
	)
	err := e.pool.QueryRow(ctx, `
SELECT rule_id, dedupe_hit, suppressed, metadata::text
FROM alert_orchestration_executions
WHERE tenant_id = $1
  AND event_type = $2
ORDER BY created_at DESC, id DESC
LIMIT 1
`, normalizeTenantID(tenantID), strings.TrimSpace(eventType)).Scan(&row.RuleID, &row.DedupeHit, &row.Suppressed, &raw)
	if err != nil {
		t.Fatalf("load latest execution failed: %v", err)
	}
	if err := json.Unmarshal(raw, &row.Metadata); err != nil {
		t.Fatalf("unmarshal execution metadata failed: %v", err)
	}
	return row
}

func (e *governanceE2EEnv) mustCountExecutions(
	t *testing.T,
	ctx context.Context,
	tenantID string,
	eventType string,
) int {
	t.Helper()
	var count int
	if err := e.pool.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM alert_orchestration_executions
WHERE tenant_id = $1
  AND event_type = $2
`, normalizeTenantID(tenantID), strings.TrimSpace(eventType)).Scan(&count); err != nil {
		t.Fatalf("count executions failed: %v", err)
	}
	return count
}

func (e *governanceE2EEnv) mustCountAuditLogs(
	t *testing.T,
	ctx context.Context,
	tenantID string,
	action string,
) int {
	t.Helper()
	var count int
	if err := e.pool.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM audit_logs
WHERE tenant_id = $1
  AND action = $2
`, normalizeTenantID(tenantID), strings.TrimSpace(action)).Scan(&count); err != nil {
		t.Fatalf("count audit logs failed: %v", err)
	}
	return count
}

func newGovernanceE2EAlert(tenantID string, alertID int64) alertEvent {
	sourceID := fmt.Sprintf("src-%d", alertID)
	tokenLimit := int64(1000)
	costLimit := 100.0
	evaluatedAt := time.Now().UTC()
	return alertEvent{
		AlertID:               alertID,
		TenantID:              tenantID,
		BudgetID:              fmt.Sprintf("budget-%d", alertID),
		SourceID:              &sourceID,
		Period:                "monthly",
		WindowStart:           evaluatedAt.Add(-time.Hour),
		WindowEnd:             evaluatedAt,
		TokensUsed:            950,
		CostUsed:              95,
		TokenLimit:            &tokenLimit,
		CostLimit:             &costLimit,
		Threshold:             0.95,
		Stage:                 "critical",
		ThresholdSnapshot:     thresholdSnapshot{Warning: 0.5, Escalated: 0.8, Critical: 0.9},
		Severity:              "critical",
		Status:                "open",
		DedupeKey:             fmt.Sprintf("dedupe-%d", alertID),
		GovernanceStateBefore: "active",
		GovernanceStateAfter:  "frozen",
		CreatedAt:             evaluatedAt,
		EvaluatedAt:           evaluatedAt,
	}
}

func newGovernanceE2EWeeklyReport(tenantID string) weeklyReportEvent {
	generatedAt := time.Now().UTC()
	weekStart := generatedAt.AddDate(0, 0, -7)
	weekEnd := generatedAt
	return weeklyReportEvent{
		TenantID:      tenantID,
		WeekStart:     weekStart,
		WeekEnd:       weekEnd,
		Tokens:        12345,
		Cost:          98.76,
		PeakDayDate:   weekEnd.Format("2006-01-02"),
		PeakDayTokens: 4567,
		PeakDayCost:   32.1,
		Sessions:      42,
		TopModels: []weeklyReportModelUse{
			{Model: "gpt-4.1", Tokens: 9000, Cost: 70.5, Sessions: 20},
		},
		GeneratedAt: generatedAt,
		ReportID:    buildWeeklyReportID(tenantID, weekStart, weekEnd),
	}
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func asOptionalString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func asString(value any) string {
	text, _ := value.(string)
	return text
}
