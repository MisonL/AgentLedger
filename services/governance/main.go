package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/agentledger/agentledger/services/internal/shared/config"
	"github.com/agentledger/agentledger/services/internal/shared/health"
	"github.com/agentledger/agentledger/services/internal/shared/ingest"
	"github.com/agentledger/agentledger/services/internal/shared/logger"
)

const (
	alertsSubject                     = "governance.alerts"
	alertsStreamName                  = "GOVERNANCE_ALERTS"
	weeklyReportsSubject              = "governance.reports.weekly"
	weeklyReportsStreamName           = "GOVERNANCE_REPORTS_WEEKLY"
	defaultEvalInterval               = 60 * time.Second
	defaultWarningThreshold           = 0.50
	defaultEscalatedThreshold         = 0.80
	defaultCriticalThreshold          = 1.00
	defaultAlertThreshold             = defaultEscalatedThreshold
	minCycleTimeout                   = 5 * time.Second
	maxCycleTimeout                   = 2 * time.Minute
	alertsStreamRetention             = 30 * 24 * time.Hour
	alertsStreamDuplicateTTL          = 30 * 24 * time.Hour
	weeklyReportsRetention            = 90 * 24 * time.Hour
	weeklyReportsDuplicateTTL         = 14 * 24 * time.Hour
	defaultReportWeekday              = time.Monday
	defaultReportHourUTC              = 9
	defaultReportMinuteUTC            = 0
	defaultWeeklyTopModelLimit        = 5
	alertCreatedAuditAction           = "governance.alert_created"
	alertDispatchedAuditAction        = "governance.alert_dispatched"
	budgetFrozenAuditAction           = "governance.budget_frozen"
	weeklyReportAuditAction           = "governance.weekly_report_published"
	alertCostRoundDecimals            = 1e8
	weeklyScheduleWeekdayEnv          = "GOV_WEEKLY_REPORT_WEEKDAY"
	weeklyScheduleTimeUTCEnv          = "GOV_WEEKLY_REPORT_TIME_UTC"
	weeklyScheduleTimeUTCFormat       = "15:04"
	weeklyReportDedupeCacheTTL        = 14 * 24 * time.Hour
	weeklyReportDedupeCacheMax        = 50000
	orchestrationDispatchModeRule     = "rule"
	orchestrationDispatchModeFallback = "fallback"
)

type governanceService struct {
	log                  *slog.Logger
	pool                 *pgxpool.Pool
	js                   jetstream.JetStream
	evalInterval         time.Duration
	weeklyReportSchedule weeklyReportSchedule
	weeklyReportDedupe   *weeklyReportDedupeCache
}

type budgetRecord struct {
	ID                 string
	TenantID           string
	Scope              string
	SourceID           *string
	OrganizationID     *string
	UserID             *string
	ModelName          *string
	Period             string
	TokenLimit         *int64
	CostLimit          *float64
	AlertThreshold     float64
	WarningThreshold   float64
	EscalatedThreshold float64
	CriticalThreshold  float64
}

const (
	budgetScopeSkipNone              = ""
	budgetScopeSkipMissingScopeValue = "missing_scope_value"
	budgetScopeSkipUnknownScope      = "unknown_scope"

	governanceStateActive  = "active"
	governanceStateFrozen  = "frozen"
	governanceStateUnknown = "unknown"

	budgetStageWarning   = "warning"
	budgetStageEscalated = "escalated"
	budgetStageCritical  = "critical"
)

type budgetScopeFilter struct {
	Scope string
	Value string
}

type thresholdSnapshot struct {
	Warning   float64 `json:"warning"`
	Escalated float64 `json:"escalated"`
	Critical  float64 `json:"critical"`
}

type usageSnapshot struct {
	TokensUsed int64
	CostUsed   float64
}

type alertEvent struct {
	AlertID               int64               `json:"alert_id"`
	TenantID              string              `json:"tenant_id"`
	BudgetID              string              `json:"budget_id"`
	SourceID              *string             `json:"source_id,omitempty"`
	Period                string              `json:"period"`
	WindowStart           time.Time           `json:"window_start"`
	WindowEnd             time.Time           `json:"window_end"`
	TokensUsed            int64               `json:"tokens_used"`
	CostUsed              float64             `json:"cost_used"`
	TokenLimit            *int64              `json:"token_limit,omitempty"`
	CostLimit             *float64            `json:"cost_limit,omitempty"`
	Threshold             float64             `json:"threshold"`
	Stage                 string              `json:"stage"`
	ThresholdSnapshot     thresholdSnapshot   `json:"threshold_snapshot"`
	Severity              string              `json:"severity"`
	Status                string              `json:"status"`
	DedupeKey             string              `json:"dedupe_key"`
	GovernanceStateBefore string              `json:"governance_state_before"`
	GovernanceStateAfter  string              `json:"governance_state_after"`
	TokenUsageRatio       *float64            `json:"token_usage_ratio,omitempty"`
	CostUsageRatio        *float64            `json:"cost_usage_ratio,omitempty"`
	CreatedAt             time.Time           `json:"created_at"`
	EvaluatedAt           time.Time           `json:"evaluated_at"`
	Orchestration         *eventOrchestration `json:"orchestration,omitempty"`
}

type weeklyReportSchedule struct {
	Weekday time.Weekday
	Hour    int
	Minute  int
}

type weeklyReportEvent struct {
	TenantID      string                 `json:"tenant_id"`
	WeekStart     time.Time              `json:"week_start"`
	WeekEnd       time.Time              `json:"week_end"`
	Tokens        int64                  `json:"tokens"`
	Cost          float64                `json:"cost"`
	PeakDayDate   string                 `json:"peak_day_date"`
	PeakDayTokens int64                  `json:"peak_day_tokens"`
	PeakDayCost   float64                `json:"peak_day_cost"`
	Sessions      int64                  `json:"sessions"`
	TopModels     []weeklyReportModelUse `json:"top_models"`
	GeneratedAt   time.Time              `json:"generated_at"`
	ReportID      string                 `json:"report_id"`
	Orchestration *eventOrchestration    `json:"orchestration,omitempty"`
}

type eventOrchestration struct {
	MatchedRuleIDs []string `json:"matchedRuleIds,omitempty"`
	Channels       []string `json:"channels,omitempty"`
	DedupeHit      bool     `json:"dedupeHit,omitempty"`
	Suppressed     bool     `json:"suppressed,omitempty"`
	Fallback       bool     `json:"fallback,omitempty"`
}

type alertOrchestrationRule struct {
	ID                       string
	Name                     string
	EventType                string
	Severity                 string
	SourceID                 string
	DedupeWindowSeconds      int
	SuppressionWindowSeconds int
	MergeWindowSeconds       int
	Channels                 []string
}

type alertOrchestrationExecution struct {
	RuleID          string
	EventType       string
	AlertID         *string
	Severity        *string
	SourceID        *string
	Channels        []string
	ConflictRuleIDs []string
	DedupeHit       bool
	Suppressed      bool
	Metadata        map[string]any
	CreatedAt       time.Time
}

type orchestrationResolutionInput struct {
	TenantID     string
	EventType    string
	AlertID      *string
	Severity     *string
	SourceID     *string
	MatchKey     string
	OccurredAt   time.Time
	Subject      string
	BaseMetadata map[string]any
}

type weeklyReportModelUse struct {
	Model    string  `json:"model"`
	Tokens   int64   `json:"tokens"`
	Cost     float64 `json:"cost"`
	Sessions int64   `json:"sessions"`
}

type weeklyModelAggregate struct {
	Model    string
	DayDate  string
	Tokens   int64
	Cost     float64
	Sessions int64
}

type weeklyUsageSummary struct {
	Tokens        int64
	Cost          float64
	PeakDayDate   string
	PeakDayTokens int64
	PeakDayCost   float64
	Sessions      int64
	TopModels     []weeklyReportModelUse
}

type auditLogEntry struct {
	TenantID  string
	EventID   string
	Action    string
	Level     string
	Detail    string
	Metadata  []byte
	CreatedAt time.Time
}

type weeklyReportDedupeCache struct {
	mu         sync.Mutex
	items      map[string]time.Time
	ttl        time.Duration
	maxEntries int
}

func normalizeOptionalString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func normalizeOrchestrationChannels(raw []string) []string {
	if len(raw) == 0 {
		return nil
	}

	normalized := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		value := strings.ToLower(strings.TrimSpace(item))
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	return normalized
}

func matchesSeverityWithWildcard(expected, actual string) bool {
	expected = strings.ToLower(strings.TrimSpace(expected))
	actual = strings.ToLower(strings.TrimSpace(actual))
	if expected == "" || actual == "" {
		return true
	}
	return expected == actual
}

func matchesSourceWithWildcard(expected, actual string) bool {
	expected = strings.TrimSpace(expected)
	actual = strings.TrimSpace(actual)
	if expected == "" || actual == "" {
		return true
	}
	return expected == actual
}

func hasChannelOverlap(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	leftSet := make(map[string]struct{}, len(left))
	for _, item := range left {
		value := strings.ToLower(strings.TrimSpace(item))
		if value == "" {
			continue
		}
		leftSet[value] = struct{}{}
	}
	for _, item := range right {
		value := strings.ToLower(strings.TrimSpace(item))
		if value == "" {
			continue
		}
		if _, exists := leftSet[value]; exists {
			return true
		}
	}
	return false
}

func detectOrchestrationConflicts(rules []alertOrchestrationRule) map[string][]string {
	conflicts := make(map[string][]string, len(rules))
	for _, rule := range rules {
		conflicts[rule.ID] = nil
	}

	for i := 0; i < len(rules); i++ {
		left := rules[i]
		for j := i + 1; j < len(rules); j++ {
			right := rules[j]
			if left.EventType != right.EventType {
				continue
			}
			if !hasChannelOverlap(left.Channels, right.Channels) {
				continue
			}
			if !matchesSeverityWithWildcard(left.Severity, right.Severity) {
				continue
			}
			if !matchesSourceWithWildcard(left.SourceID, right.SourceID) {
				continue
			}
			conflicts[left.ID] = append(conflicts[left.ID], right.ID)
			conflicts[right.ID] = append(conflicts[right.ID], left.ID)
		}
	}

	for ruleID, items := range conflicts {
		if len(items) == 0 {
			continue
		}
		sort.Strings(items)
		conflicts[ruleID] = items
	}
	return conflicts
}

func fallbackExecutionRuleID(eventType string) string {
	return fmt.Sprintf("builtin:fallback:%s", strings.TrimSpace(eventType))
}

func buildAlertOrchestrationMatchKey(alert alertEvent) string {
	if dedupeKey := strings.TrimSpace(alert.DedupeKey); dedupeKey != "" {
		return fmt.Sprintf("alert:%s", dedupeKey)
	}
	return strings.Join([]string{
		"alert",
		normalizeTenantID(alert.TenantID),
		strings.TrimSpace(alert.BudgetID),
		strings.ToLower(strings.TrimSpace(alert.Severity)),
		normalizeOptionalString(alert.SourceID),
		strings.ToLower(strings.TrimSpace(alert.Stage)),
	}, "|")
}

func buildWeeklyReportOrchestrationMatchKey(report weeklyReportEvent) string {
	if reportID := strings.TrimSpace(report.ReportID); reportID != "" {
		return fmt.Sprintf("weekly:%s", reportID)
	}
	return strings.Join([]string{
		"weekly",
		normalizeTenantID(report.TenantID),
		report.WeekStart.UTC().Format(time.RFC3339),
		report.WeekEnd.UTC().Format(time.RFC3339),
	}, "|")
}

func asJSONString(value any) []byte {
	if value == nil {
		return []byte(`{}`)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return []byte(`{}`)
	}
	return raw
}

func main() {
	cfg, err := config.Load("governance", ":8084")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config failed: %v\n", err)
		os.Exit(1)
	}

	evalInterval, err := loadEvalInterval()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load GOV_EVAL_INTERVAL failed: %v\n", err)
		os.Exit(1)
	}

	weeklySchedule, err := loadWeeklyReportSchedule()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load weekly report schedule failed: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.LogLevel).With("service", cfg.ServiceName)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := initPGPool(ctx, cfg)
	if err != nil {
		log.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	nc, js, err := initJetStream(cfg, log)
	if err != nil {
		log.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			log.Warn("nats drain failed", "error", err)
		}
		nc.Close()
	}()

	if err := ensureAlertsStream(ctx, js, log); err != nil {
		log.Error("ensure alerts stream failed", "error", err)
		os.Exit(1)
	}
	if err := ensureWeeklyReportsStream(ctx, js, log); err != nil {
		log.Error("ensure weekly reports stream failed", "error", err)
		os.Exit(1)
	}

	svc := &governanceService{
		log:                  log,
		pool:                 pool,
		js:                   js,
		evalInterval:         evalInterval,
		weeklyReportSchedule: weeklySchedule,
		weeklyReportDedupe:   newWeeklyReportDedupeCache(),
	}

	healthErrCh := health.Start(ctx, cfg.HTTPAddr, cfg.ServiceName, log)
	log.Info(
		"service started",
		"http_addr", cfg.HTTPAddr,
		"nats_url", cfg.NATS.URL,
		"eval_interval", evalInterval.String(),
		"alerts_subject", alertsSubject,
		"weekly_reports_subject", weeklyReportsSubject,
		"weekly_schedule_weekday_utc", weeklySchedule.Weekday.String(),
		"weekly_schedule_time_utc", fmt.Sprintf("%02d:%02d", weeklySchedule.Hour, weeklySchedule.Minute),
	)

	if err := svc.runOnce(ctx); err != nil {
		log.Error("initial budget evaluation failed", "error", err)
	}

	ticker := time.NewTicker(evalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("service stopping", "reason", ctx.Err())
			return
		case err, ok := <-healthErrCh:
			if ok && err != nil {
				log.Error("health server failed", "error", err)
				os.Exit(1)
			}
		case <-ticker.C:
			if err := svc.runOnce(ctx); err != nil {
				log.Error("budget evaluation cycle failed", "error", err)
			}
		}
	}
}

func (s *governanceService) runOnce(ctx context.Context) error {
	cycleTimeout := s.evalInterval - time.Second
	if cycleTimeout < minCycleTimeout {
		cycleTimeout = minCycleTimeout
	}
	if cycleTimeout > maxCycleTimeout {
		cycleTimeout = maxCycleTimeout
	}

	evalCtx, cancel := context.WithTimeout(ctx, cycleTimeout)
	defer cancel()

	startedAt := time.Now()
	budgets, err := s.loadEnabledBudgets(evalCtx)
	if err != nil {
		return fmt.Errorf("load enabled budgets failed: %w", err)
	}

	evaluatedAt := time.Now().UTC()
	newAlerts := 0
	failed := 0

	for _, budget := range budgets {
		alert, evalErr := s.evaluateBudget(evalCtx, budget, evaluatedAt)
		if evalErr != nil {
			failed++
			s.log.Error(
				"evaluate budget failed",
				"error", evalErr,
				"budget_id", budget.ID,
				"tenant_id", budget.TenantID,
			)
			continue
		}
		if alert == nil {
			continue
		}

		if pubErr := s.publishAlert(evalCtx, *alert); pubErr != nil {
			failed++
			s.log.Error(
				"publish governance alert failed",
				"error", pubErr,
				"alert_id", alert.AlertID,
				"budget_id", alert.BudgetID,
				"tenant_id", alert.TenantID,
			)
			continue
		}
		newAlerts++
	}

	s.log.Info(
		"budget evaluation completed",
		"budgets", len(budgets),
		"new_alerts", newAlerts,
		"failed", failed,
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)

	if err := s.publishWeeklyReportsIfDue(evalCtx, evaluatedAt); err != nil {
		return fmt.Errorf("publish weekly reports failed: %w", err)
	}

	return nil
}

func (s *governanceService) loadEnabledBudgets(ctx context.Context) ([]budgetRecord, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  id,
  tenant_id,
  scope,
  source_id,
  organization_id,
  user_id,
  model_name,
  period,
  token_limit,
  cost_limit::double precision,
  alert_threshold::double precision,
  warning_threshold::double precision,
  escalated_threshold::double precision,
  critical_threshold::double precision
FROM budgets
WHERE enabled = TRUE
ORDER BY tenant_id, id
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	budgets := make([]budgetRecord, 0)
	for rows.Next() {
		var item budgetRecord
		if err := rows.Scan(
			&item.ID,
			&item.TenantID,
			&item.Scope,
			&item.SourceID,
			&item.OrganizationID,
			&item.UserID,
			&item.ModelName,
			&item.Period,
			&item.TokenLimit,
			&item.CostLimit,
			&item.AlertThreshold,
			&item.WarningThreshold,
			&item.EscalatedThreshold,
			&item.CriticalThreshold,
		); err != nil {
			return nil, fmt.Errorf("scan budget row failed: %w", err)
		}

		item.Scope = strings.ToLower(strings.TrimSpace(item.Scope))
		item.Period = strings.ToLower(strings.TrimSpace(item.Period))
		item.SourceID = trimOptionalString(item.SourceID)
		item.OrganizationID = trimOptionalString(item.OrganizationID)
		item.UserID = trimOptionalString(item.UserID)
		item.ModelName = trimOptionalString(item.ModelName)
		item.TenantID = normalizeTenantID(item.TenantID)
		if item.AlertThreshold <= 0 || item.AlertThreshold > 1 {
			item.AlertThreshold = defaultAlertThreshold
		}
		if item.WarningThreshold <= 0 || item.WarningThreshold > 1 {
			item.WarningThreshold = 0
		}
		if item.EscalatedThreshold <= 0 || item.EscalatedThreshold > 1 {
			item.EscalatedThreshold = 0
		}
		if item.CriticalThreshold <= 0 || item.CriticalThreshold > 1 {
			item.CriticalThreshold = 0
		}

		budgets = append(budgets, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate budget rows failed: %w", err)
	}

	return budgets, nil
}

func (s *governanceService) evaluateBudget(ctx context.Context, budget budgetRecord, evaluatedAt time.Time) (*alertEvent, error) {
	windowStart, windowEnd, err := budgetWindow(budget.Period, evaluatedAt)
	if err != nil {
		s.log.Warn(
			"skip budget with unsupported period",
			"budget_id", budget.ID,
			"period", budget.Period,
			"error", err,
		)
		if touchErr := s.touchBudget(ctx, budget.ID, evaluatedAt); touchErr != nil {
			return nil, fmt.Errorf("update last_evaluated_at for unsupported period failed: %w", touchErr)
		}
		return nil, nil
	}

	scopeFilter, skip, skipReason := resolveBudgetScope(budget)
	if skip {
		switch skipReason {
		case budgetScopeSkipMissingScopeValue:
			s.log.Warn(
				"skip scoped budget without scope identity value",
				"budget_id", budget.ID,
				"scope", budget.Scope,
				"scope_value_field", scopeValueFieldForScope(budget.Scope),
			)
		case budgetScopeSkipUnknownScope:
			s.log.Warn(
				"skip budget with unknown scope (fail-closed)",
				"budget_id", budget.ID,
				"scope", budget.Scope,
			)
		default:
			s.log.Warn(
				"skip budget due to invalid scope",
				"budget_id", budget.ID,
				"scope", budget.Scope,
				"reason", skipReason,
			)
		}
		if touchErr := s.touchBudget(ctx, budget.ID, evaluatedAt); touchErr != nil {
			return nil, fmt.Errorf("update last_evaluated_at for invalid scope failed: %w", touchErr)
		}
		return nil, nil
	}

	usage, err := s.aggregateUsage(ctx, budget.TenantID, scopeFilter, windowStart, windowEnd)
	if err != nil {
		return nil, fmt.Errorf("aggregate usage failed: %w", err)
	}

	thresholds := resolveThresholdSnapshot(budget)
	stage := resolveStage(budget, usage)
	if stage == "" {
		if err := s.touchBudget(ctx, budget.ID, evaluatedAt); err != nil {
			return nil, fmt.Errorf("update last_evaluated_at failed: %w", err)
		}
		return nil, nil
	}
	severity := severityFromStage(stage)

	dedupeKey := buildDedupeKey(budget.ID, budget.Period, windowStart, windowEnd, stage)
	alert, inserted, err := s.persistAlert(
		ctx,
		budget,
		usage,
		windowStart,
		windowEnd,
		evaluatedAt,
		stage,
		thresholds,
		dedupeKey,
	)
	if err != nil {
		return nil, err
	}
	if alert == nil {
		return nil, nil
	}

	if inserted {
		s.log.Warn(
			"governance alert created",
			"alert_id", alert.AlertID,
			"budget_id", budget.ID,
			"tenant_id", budget.TenantID,
			"stage", stage,
			"severity", severity,
			"window_start", windowStart,
			"window_end", windowEnd,
			"tokens_used", usage.TokensUsed,
			"cost_used", usage.CostUsed,
		)
	} else {
		s.log.Info(
			"governance alert republish required",
			"alert_id", alert.AlertID,
			"budget_id", budget.ID,
			"tenant_id", budget.TenantID,
			"stage", alert.Stage,
			"severity", alert.Severity,
			"dedupe_key", alert.DedupeKey,
		)
	}

	return alert, nil
}

func resolveBudgetScope(budget budgetRecord) (filter *budgetScopeFilter, skip bool, skipReason string) {
	normalizedScope := strings.ToLower(strings.TrimSpace(budget.Scope))

	var value *string
	switch normalizedScope {
	case "source":
		value = trimOptionalString(budget.SourceID)
	case "org":
		value = trimOptionalString(budget.OrganizationID)
	case "user":
		value = trimOptionalString(budget.UserID)
	case "model":
		value = trimOptionalString(budget.ModelName)
	case "global", "tenant", "":
		// tenant scope 不额外过滤。
		return nil, false, budgetScopeSkipNone
	default:
		return nil, true, budgetScopeSkipUnknownScope
	}

	if value == nil {
		return nil, true, budgetScopeSkipMissingScopeValue
	}

	return &budgetScopeFilter{
		Scope: normalizedScope,
		Value: *value,
	}, false, budgetScopeSkipNone
}

func scopeValueFieldForScope(scope string) string {
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case "source":
		return "source_id"
	case "org":
		return "organization_id"
	case "user":
		return "user_id"
	case "model":
		return "model_name"
	default:
		return ""
	}
}

func (s *governanceService) aggregateUsage(
	ctx context.Context,
	tenantID string,
	scopeFilter *budgetScopeFilter,
	windowStart time.Time,
	windowEnd time.Time,
) (usageSnapshot, error) {
	var usage usageSnapshot
	tenantID = normalizeTenantID(tenantID)

	query := `
SELECT
  COALESCE(SUM(sess.tokens), 0)::bigint AS tokens_used,
  COALESCE(SUM(sess.cost), 0)::double precision AS cost_used
FROM sessions AS sess
JOIN sources AS src ON src.id = sess.source_id
WHERE COALESCE(NULLIF(src.tenant_id, ''), 'default') = $1
  AND COALESCE(sess.started_at, sess.created_at) >= $2
  AND COALESCE(sess.started_at, sess.created_at) < $3
`
	args := []any{tenantID, windowStart, windowEnd}

	if scopeFilter != nil {
		args = append(args, scopeFilter.Value)
		switch scopeFilter.Scope {
		case "source":
			query += "  AND sess.source_id = $4\n"
		case "org":
			query += "  AND COALESCE(NULLIF(TRIM(src.workspace_id), ''), '') = $4\n"
		case "user":
			query += "  AND COALESCE(NULLIF(TRIM(sess.workspace), ''), '') = $4\n"
		case "model":
			query += "  AND COALESCE(NULLIF(TRIM(sess.model), ''), '') = $4\n"
		default:
			return usageSnapshot{}, fmt.Errorf("unsupported scope filter: %s", scopeFilter.Scope)
		}
	}

	err := s.pool.QueryRow(ctx, query, args...).Scan(&usage.TokensUsed, &usage.CostUsed)
	if err != nil {
		return usageSnapshot{}, err
	}

	usage.CostUsed = roundCost(usage.CostUsed)
	return usage, nil
}

func (s *governanceService) touchBudget(ctx context.Context, budgetID string, evaluatedAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
UPDATE budgets
SET last_evaluated_at = $2,
    updated_at = NOW()
WHERE id = $1
`, budgetID, evaluatedAt)
	if err != nil {
		return fmt.Errorf("update budget %s failed: %w", budgetID, err)
	}
	return nil
}

func (s *governanceService) persistAlert(
	ctx context.Context,
	budget budgetRecord,
	usage usageSnapshot,
	windowStart time.Time,
	windowEnd time.Time,
	evaluatedAt time.Time,
	stage string,
	thresholds thresholdSnapshot,
	dedupeKey string,
) (*alertEvent, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Rollback(ctx)

	createdAt := evaluatedAt
	var alertID int64
	tokenLimitValue := int64(0)
	if budget.TokenLimit != nil {
		tokenLimitValue = *budget.TokenLimit
	}
	costLimitValue := 0.0
	if budget.CostLimit != nil {
		costLimitValue = *budget.CostLimit
	}
	stageThreshold := thresholdForStage(stage, thresholds)
	severity := severityFromStage(stage)

	err = tx.QueryRow(ctx, `
INSERT INTO governance_alerts (
  tenant_id,
  budget_id,
  source_id,
  period,
  window_start,
  window_end,
  tokens_used,
  cost_used,
  token_limit,
  cost_limit,
  threshold,
  severity,
  status,
  dedupe_key,
  created_at,
  updated_at
)
VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8,
  $9, $10, $11, $12, 'open', $13, $14, $14
)
ON CONFLICT (dedupe_key) DO NOTHING
RETURNING id, created_at
`,
		budget.TenantID,
		budget.ID,
		budget.SourceID,
		budget.Period,
		windowStart,
		windowEnd,
		usage.TokensUsed,
		usage.CostUsed,
		tokenLimitValue,
		costLimitValue,
		stageThreshold,
		severity,
		dedupeKey,
		evaluatedAt,
	).Scan(&alertID, &createdAt)

	inserted := true
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			inserted = false
		} else {
			return nil, false, fmt.Errorf("insert governance_alerts failed: %w", err)
		}
	}

	governanceStateBefore, governanceStateAfter := defaultGovernanceStates(stage)
	if inserted && stage == budgetStageCritical {
		before, after, freezeErr := s.freezeBudgetIfNeeded(
			ctx,
			tx,
			alertID,
			budget,
			stage,
			thresholds,
			evaluatedAt,
		)
		if freezeErr != nil {
			return nil, false, freezeErr
		}
		governanceStateBefore = before
		governanceStateAfter = after
	}

	tokenRatio, costRatio := usageRatios(budget, usage)
	alert := &alertEvent{
		AlertID:               alertID,
		TenantID:              budget.TenantID,
		BudgetID:              budget.ID,
		SourceID:              budget.SourceID,
		Period:                budget.Period,
		WindowStart:           windowStart,
		WindowEnd:             windowEnd,
		TokensUsed:            usage.TokensUsed,
		CostUsed:              usage.CostUsed,
		TokenLimit:            budget.TokenLimit,
		CostLimit:             budget.CostLimit,
		Threshold:             stageThreshold,
		Stage:                 stage,
		ThresholdSnapshot:     thresholds,
		Severity:              severity,
		Status:                "open",
		DedupeKey:             dedupeKey,
		GovernanceStateBefore: governanceStateBefore,
		GovernanceStateAfter:  governanceStateAfter,
		TokenUsageRatio:       tokenRatio,
		CostUsageRatio:        costRatio,
		CreatedAt:             createdAt,
		EvaluatedAt:           evaluatedAt,
	}

	if inserted {
		auditEntry, err := buildAlertCreatedAuditLog(*alert)
		if err != nil {
			return nil, false, err
		}
		if err := insertAuditLog(ctx, tx, auditEntry); err != nil {
			return nil, false, fmt.Errorf("insert audit_logs failed: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
UPDATE budgets
SET last_evaluated_at = $2,
    updated_at = NOW()
WHERE id = $1
`, budget.ID, evaluatedAt)
	if err != nil {
		return nil, false, fmt.Errorf("update budget last_evaluated_at failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit tx failed: %w", err)
	}

	if !inserted {
		alert, err := s.loadAlertByDedupeKey(ctx, dedupeKey)
		if err != nil {
			return nil, false, fmt.Errorf("load existing alert failed: %w", err)
		}
		if alert == nil {
			return nil, false, nil
		}
		published, err := s.isAlertDispatched(ctx, alert.TenantID, alert.AlertID)
		if err != nil {
			return nil, false, fmt.Errorf("check alert dispatch audit failed: %w", err)
		}
		if published {
			return nil, false, nil
		}
		return alert, false, nil
	}

	return alert, true, nil
}

func (s *governanceService) loadAlertOrchestrationRules(
	ctx context.Context,
	tenantID string,
	eventType string,
) ([]alertOrchestrationRule, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  id,
  name,
  event_type,
  severity,
  source_id,
  dedupe_window_seconds,
  suppression_window_seconds,
  merge_window_seconds,
  channels
FROM alert_orchestration_rules
WHERE tenant_id = $1
  AND enabled = TRUE
  AND event_type = $2
ORDER BY updated_at DESC, id ASC
`, normalizeTenantID(tenantID), strings.TrimSpace(eventType))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rules := make([]alertOrchestrationRule, 0)
	for rows.Next() {
		var (
			rule              alertOrchestrationRule
			severity          *string
			sourceID          *string
			channelsRaw       []byte
			dedupeWindow      int32
			suppressionWindow int32
			mergeWindow       int32
		)
		if err := rows.Scan(
			&rule.ID,
			&rule.Name,
			&rule.EventType,
			&severity,
			&sourceID,
			&dedupeWindow,
			&suppressionWindow,
			&mergeWindow,
			&channelsRaw,
		); err != nil {
			return nil, fmt.Errorf("scan alert orchestration rule failed: %w", err)
		}

		rule.Severity = strings.ToLower(strings.TrimSpace(normalizeOptionalString(severity)))
		rule.SourceID = normalizeOptionalString(sourceID)
		rule.DedupeWindowSeconds = int(dedupeWindow)
		rule.SuppressionWindowSeconds = int(suppressionWindow)
		rule.MergeWindowSeconds = int(mergeWindow)
		if len(channelsRaw) > 0 {
			if err := json.Unmarshal(channelsRaw, &rule.Channels); err != nil {
				return nil, fmt.Errorf("unmarshal alert orchestration rule channels failed: %w", err)
			}
		}
		rule.Channels = normalizeOrchestrationChannels(rule.Channels)
		rules = append(rules, rule)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate alert orchestration rules failed: %w", err)
	}
	return rules, nil
}

func (s *governanceService) hasRecentOrchestrationExecutionWithMatchKey(
	ctx context.Context,
	tenantID string,
	ruleID string,
	eventType string,
	matchKey string,
	window time.Duration,
	before time.Time,
) (bool, error) {
	if window <= 0 || strings.TrimSpace(matchKey) == "" {
		return false, nil
	}

	since := before.UTC().Add(-window)
	var exists bool
	err := s.pool.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM alert_orchestration_executions
  WHERE tenant_id = $1
    AND rule_id = $2
    AND event_type = $3
    AND simulated = FALSE
    AND COALESCE(metadata ->> 'matchKey', '') = $4
    AND created_at >= $5
  LIMIT 1
)
`, normalizeTenantID(tenantID), strings.TrimSpace(ruleID), strings.TrimSpace(eventType), strings.TrimSpace(matchKey), since).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (s *governanceService) createAlertOrchestrationExecutionLog(
	ctx context.Context,
	tenantID string,
	execution alertOrchestrationExecution,
) error {
	createdAt := execution.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	_, err := s.pool.Exec(ctx, `
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
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8::jsonb,
  $9::jsonb,
  $10,
  $11,
  FALSE,
  $12::jsonb,
  $13
)
`,
		ingest.NewID("aoe"),
		normalizeTenantID(tenantID),
		strings.TrimSpace(execution.RuleID),
		strings.TrimSpace(execution.EventType),
		execution.AlertID,
		execution.Severity,
		execution.SourceID,
		asJSONString(normalizeOrchestrationChannels(execution.Channels)),
		asJSONString(execution.ConflictRuleIDs),
		execution.DedupeHit,
		execution.Suppressed,
		asJSONString(execution.Metadata),
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("insert alert orchestration execution failed: %w", err)
	}
	return nil
}

func (s *governanceService) applyAlertOrchestration(
	ctx context.Context,
	alert alertEvent,
) (alertEvent, error) {
	createdAt := alert.EvaluatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = alert.CreatedAt.UTC()
	}
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	rules, err := s.loadAlertOrchestrationRules(ctx, alert.TenantID, "alert")
	if err != nil {
		return alert, err
	}

	severity := strings.ToLower(strings.TrimSpace(alert.Severity))
	sourceID := normalizeOptionalString(alert.SourceID)
	matched := make([]alertOrchestrationRule, 0, len(rules))
	for _, rule := range rules {
		if !matchesSeverityWithWildcard(rule.Severity, severity) {
			continue
		}
		if !matchesSourceWithWildcard(rule.SourceID, sourceID) {
			continue
		}
		matched = append(matched, rule)
	}

	alertID := strconv.FormatInt(alert.AlertID, 10)
	matchKey := buildAlertOrchestrationMatchKey(alert)
	if len(matched) == 0 {
		metadata := map[string]any{
			"dispatchMode":     orchestrationDispatchModeFallback,
			"fallback":         true,
			"matchKey":         matchKey,
			"natsSubject":      alertsSubject,
			"alertId":          alertID,
			"severity":         severity,
			"sourceId":         sourceID,
			"deliveryChannels": []string{},
		}
		if err := s.createAlertOrchestrationExecutionLog(ctx, alert.TenantID, alertOrchestrationExecution{
			RuleID:    fallbackExecutionRuleID("alert"),
			EventType: "alert",
			AlertID:   &alertID,
			Severity:  &severity,
			SourceID:  alert.SourceID,
			Metadata:  metadata,
			CreatedAt: createdAt,
		}); err != nil {
			return alert, err
		}

		alert.Orchestration = &eventOrchestration{
			Fallback: true,
		}
		return alert, nil
	}

	conflicts := detectOrchestrationConflicts(matched)
	matchedRuleIDs := make([]string, 0, len(matched))
	for _, rule := range matched {
		matchedRuleIDs = append(matchedRuleIDs, rule.ID)
	}
	activeChannels := make([]string, 0)
	activeChannelSet := make(map[string]struct{})
	anyDedupeHit := false
	anySuppressed := false
	for _, rule := range matched {
		dedupeHit, err := s.hasRecentOrchestrationExecutionWithMatchKey(
			ctx,
			alert.TenantID,
			rule.ID,
			"alert",
			matchKey,
			time.Duration(rule.DedupeWindowSeconds)*time.Second,
			createdAt,
		)
		if err != nil {
			return alert, err
		}
		suppressed, err := s.hasRecentOrchestrationExecutionWithMatchKey(
			ctx,
			alert.TenantID,
			rule.ID,
			"alert",
			matchKey,
			time.Duration(rule.SuppressionWindowSeconds)*time.Second,
			createdAt,
		)
		if err != nil {
			return alert, err
		}

		anyDedupeHit = anyDedupeHit || dedupeHit
		anySuppressed = anySuppressed || suppressed
		deliveryChannels := normalizeOrchestrationChannels(rule.Channels)
		if dedupeHit || suppressed {
			deliveryChannels = nil
		}
		for _, channel := range deliveryChannels {
			if _, exists := activeChannelSet[channel]; exists {
				continue
			}
			activeChannelSet[channel] = struct{}{}
			activeChannels = append(activeChannels, channel)
		}

		metadata := map[string]any{
			"dispatchMode":     orchestrationDispatchModeRule,
			"fallback":         false,
			"matchKey":         matchKey,
			"natsSubject":      alertsSubject,
			"alertId":          alertID,
			"severity":         severity,
			"sourceId":         sourceID,
			"ruleName":         rule.Name,
			"matchedRuleIds":   matchedRuleIDs,
			"deliveryChannels": deliveryChannels,
		}
		if len(conflicts[rule.ID]) > 0 {
			metadata["conflictRuleIds"] = conflicts[rule.ID]
		}
		if err := s.createAlertOrchestrationExecutionLog(ctx, alert.TenantID, alertOrchestrationExecution{
			RuleID:          rule.ID,
			EventType:       "alert",
			AlertID:         &alertID,
			Severity:        &severity,
			SourceID:        alert.SourceID,
			Channels:        rule.Channels,
			ConflictRuleIDs: conflicts[rule.ID],
			DedupeHit:       dedupeHit,
			Suppressed:      suppressed,
			Metadata:        metadata,
			CreatedAt:       createdAt,
		}); err != nil {
			return alert, err
		}
	}

	alert.Orchestration = &eventOrchestration{
		MatchedRuleIDs: matchedRuleIDs,
		Channels:       activeChannels,
		DedupeHit:      anyDedupeHit,
		Suppressed:     anySuppressed,
		Fallback:       false,
	}
	return alert, nil
}

func (s *governanceService) applyWeeklyReportOrchestration(
	ctx context.Context,
	report weeklyReportEvent,
) (weeklyReportEvent, error) {
	createdAt := report.GeneratedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	rules, err := s.loadAlertOrchestrationRules(ctx, report.TenantID, "weekly")
	if err != nil {
		return report, err
	}

	matched := make([]alertOrchestrationRule, 0, len(rules))
	for _, rule := range rules {
		if !matchesSeverityWithWildcard(rule.Severity, "") {
			continue
		}
		if !matchesSourceWithWildcard(rule.SourceID, "") {
			continue
		}
		matched = append(matched, rule)
	}

	matchKey := buildWeeklyReportOrchestrationMatchKey(report)
	if len(matched) == 0 {
		metadata := map[string]any{
			"dispatchMode":     orchestrationDispatchModeFallback,
			"fallback":         true,
			"matchKey":         matchKey,
			"natsSubject":      weeklyReportsSubject,
			"reportId":         strings.TrimSpace(report.ReportID),
			"deliveryChannels": []string{},
		}
		if err := s.createAlertOrchestrationExecutionLog(ctx, report.TenantID, alertOrchestrationExecution{
			RuleID:    fallbackExecutionRuleID("weekly"),
			EventType: "weekly",
			Metadata:  metadata,
			CreatedAt: createdAt,
		}); err != nil {
			return report, err
		}

		report.Orchestration = &eventOrchestration{
			Fallback: true,
		}
		return report, nil
	}

	conflicts := detectOrchestrationConflicts(matched)
	matchedRuleIDs := make([]string, 0, len(matched))
	for _, rule := range matched {
		matchedRuleIDs = append(matchedRuleIDs, rule.ID)
	}
	activeChannels := make([]string, 0)
	activeChannelSet := make(map[string]struct{})
	anyDedupeHit := false
	anySuppressed := false
	for _, rule := range matched {
		dedupeHit, err := s.hasRecentOrchestrationExecutionWithMatchKey(
			ctx,
			report.TenantID,
			rule.ID,
			"weekly",
			matchKey,
			time.Duration(rule.DedupeWindowSeconds)*time.Second,
			createdAt,
		)
		if err != nil {
			return report, err
		}
		suppressed, err := s.hasRecentOrchestrationExecutionWithMatchKey(
			ctx,
			report.TenantID,
			rule.ID,
			"weekly",
			matchKey,
			time.Duration(rule.SuppressionWindowSeconds)*time.Second,
			createdAt,
		)
		if err != nil {
			return report, err
		}

		anyDedupeHit = anyDedupeHit || dedupeHit
		anySuppressed = anySuppressed || suppressed
		deliveryChannels := normalizeOrchestrationChannels(rule.Channels)
		if dedupeHit || suppressed {
			deliveryChannels = nil
		}
		for _, channel := range deliveryChannels {
			if _, exists := activeChannelSet[channel]; exists {
				continue
			}
			activeChannelSet[channel] = struct{}{}
			activeChannels = append(activeChannels, channel)
		}

		metadata := map[string]any{
			"dispatchMode":     orchestrationDispatchModeRule,
			"fallback":         false,
			"matchKey":         matchKey,
			"natsSubject":      weeklyReportsSubject,
			"reportId":         strings.TrimSpace(report.ReportID),
			"ruleName":         rule.Name,
			"matchedRuleIds":   matchedRuleIDs,
			"deliveryChannels": deliveryChannels,
		}
		if len(conflicts[rule.ID]) > 0 {
			metadata["conflictRuleIds"] = conflicts[rule.ID]
		}
		if err := s.createAlertOrchestrationExecutionLog(ctx, report.TenantID, alertOrchestrationExecution{
			RuleID:          rule.ID,
			EventType:       "weekly",
			Channels:        rule.Channels,
			ConflictRuleIDs: conflicts[rule.ID],
			DedupeHit:       dedupeHit,
			Suppressed:      suppressed,
			Metadata:        metadata,
			CreatedAt:       createdAt,
		}); err != nil {
			return report, err
		}
	}

	report.Orchestration = &eventOrchestration{
		MatchedRuleIDs: matchedRuleIDs,
		Channels:       activeChannels,
		DedupeHit:      anyDedupeHit,
		Suppressed:     anySuppressed,
		Fallback:       false,
	}
	return report, nil
}

func (s *governanceService) freezeBudgetIfNeeded(
	ctx context.Context,
	tx pgx.Tx,
	alertID int64,
	budget budgetRecord,
	stage string,
	thresholds thresholdSnapshot,
	evaluatedAt time.Time,
) (string, string, error) {
	var enabled bool
	err := tx.QueryRow(ctx, `
SELECT enabled
FROM budgets
WHERE id = $1
FOR UPDATE
`, budget.ID).Scan(&enabled)
	if err != nil {
		return governanceStateUnknown, governanceStateUnknown, fmt.Errorf("load budget enabled state failed: %w", err)
	}

	before, after, shouldFreeze := resolveGovernanceStateTransition(stage, enabled)
	if !shouldFreeze {
		return before, after, nil
	}

	freezeReason := fmt.Sprintf("governance critical freeze by alert %d", alertID)
	_, err = tx.Exec(ctx, `
UPDATE budgets
SET enabled = FALSE,
    governance_state = 'frozen',
    freeze_reason = $2,
    frozen_at = $3,
    frozen_by_alert_id = $4,
    updated_at = $3
WHERE id = $1
`, budget.ID, freezeReason, evaluatedAt.UTC(), strconv.FormatInt(alertID, 10))
	if err != nil {
		return governanceStateUnknown, governanceStateUnknown, fmt.Errorf("freeze budget failed: %w", err)
	}

	auditEntry, err := buildBudgetFrozenAuditLog(
		alertID,
		budget,
		stage,
		thresholds,
		before,
		after,
		evaluatedAt,
	)
	if err != nil {
		return governanceStateUnknown, governanceStateUnknown, err
	}
	if err := insertAuditLog(ctx, tx, auditEntry); err != nil {
		return governanceStateUnknown, governanceStateUnknown, fmt.Errorf("insert budget frozen audit failed: %w", err)
	}

	return before, after, nil
}

func (s *governanceService) publishAlert(ctx context.Context, alert alertEvent) error {
	orchestratedAlert, orchestrationErr := s.applyAlertOrchestration(ctx, alert)
	if orchestrationErr != nil {
		s.log.Warn("apply alert orchestration failed, fallback to legacy dispatch",
			"error", orchestrationErr,
			"alert_id", alert.AlertID,
			"tenant_id", normalizeTenantID(alert.TenantID),
		)
		orchestratedAlert = alert
		orchestratedAlert.Orchestration = &eventOrchestration{Fallback: true}
	}

	payload, err := json.Marshal(orchestratedAlert)
	if err != nil {
		return fmt.Errorf("marshal alert payload failed: %w", err)
	}

	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	messageID := alertMessageID(alert.AlertID)
	ack, err := s.js.Publish(publishCtx, alertsSubject, payload, jetstream.WithMsgID(messageID))
	if err != nil {
		return fmt.Errorf("publish to %s failed: %w", alertsSubject, err)
	}
	if err := s.recordAlertPublished(ctx, orchestratedAlert, ack.Duplicate); err != nil {
		return fmt.Errorf("record alert publish audit failed: %w", err)
	}

	s.log.Info(
		"governance alert published",
		"alert_id", alert.AlertID,
		"budget_id", alert.BudgetID,
		"tenant_id", alert.TenantID,
		"stage", alert.Stage,
		"severity", alert.Severity,
		"subject", alertsSubject,
		"stream", ack.Stream,
		"sequence", ack.Sequence,
		"duplicate", ack.Duplicate,
	)
	return nil
}

func alertMessageID(alertID int64) string {
	return fmt.Sprintf("governance_alert:%d", alertID)
}

func budgetFrozenMessageID(budgetID string) string {
	return fmt.Sprintf("governance_budget_frozen:%s", strings.TrimSpace(budgetID))
}

func (s *governanceService) loadAlertByDedupeKey(ctx context.Context, dedupeKey string) (*alertEvent, error) {
	var (
		alertID     int64
		tenantID    string
		budgetID    string
		sourceID    *string
		period      string
		windowStart time.Time
		windowEnd   time.Time
		tokensUsed  int64
		costUsed    float64
		tokenLimitV int64
		costLimitV  float64
		threshold   float64
		severity    string
		status      string
		createdAt   time.Time
	)

	err := s.pool.QueryRow(ctx, `
SELECT
  id,
  tenant_id,
  budget_id,
  source_id,
  period,
  window_start,
  window_end,
  tokens_used,
  cost_used::double precision,
  token_limit,
  cost_limit::double precision,
  threshold::double precision,
  severity,
  status,
  created_at
FROM governance_alerts
WHERE dedupe_key = $1
LIMIT 1
`, dedupeKey).Scan(
		&alertID,
		&tenantID,
		&budgetID,
		&sourceID,
		&period,
		&windowStart,
		&windowEnd,
		&tokensUsed,
		&costUsed,
		&tokenLimitV,
		&costLimitV,
		&threshold,
		&severity,
		&status,
		&createdAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	var tokenLimit *int64
	if tokenLimitV > 0 {
		value := tokenLimitV
		tokenLimit = &value
	}
	var costLimit *float64
	if costLimitV > 0 {
		value := roundCost(costLimitV)
		costLimit = &value
	}

	budget := budgetRecord{
		TokenLimit: tokenLimit,
		CostLimit:  costLimit,
	}
	usage := usageSnapshot{
		TokensUsed: tokensUsed,
		CostUsed:   roundCost(costUsed),
	}
	tokenRatio, costRatio := usageRatios(budget, usage)
	normalizedSeverity := strings.ToLower(strings.TrimSpace(severity))
	stage := stageFromStoredAlert(normalizedSeverity, threshold)
	thresholds := inferThresholdSnapshot(stage, threshold)
	governanceStateBefore, governanceStateAfter := loadedAlertGovernanceStates(stage)

	return &alertEvent{
		AlertID:               alertID,
		TenantID:              normalizeTenantID(tenantID),
		BudgetID:              strings.TrimSpace(budgetID),
		SourceID:              trimOptionalString(sourceID),
		Period:                strings.ToLower(strings.TrimSpace(period)),
		WindowStart:           windowStart.UTC(),
		WindowEnd:             windowEnd.UTC(),
		TokensUsed:            usage.TokensUsed,
		CostUsed:              usage.CostUsed,
		TokenLimit:            tokenLimit,
		CostLimit:             costLimit,
		Threshold:             threshold,
		Stage:                 stage,
		ThresholdSnapshot:     thresholds,
		Severity:              normalizedSeverity,
		Status:                strings.ToLower(strings.TrimSpace(status)),
		DedupeKey:             strings.TrimSpace(dedupeKey),
		GovernanceStateBefore: governanceStateBefore,
		GovernanceStateAfter:  governanceStateAfter,
		TokenUsageRatio:       tokenRatio,
		CostUsageRatio:        costRatio,
		CreatedAt:             createdAt.UTC(),
		EvaluatedAt:           createdAt.UTC(),
	}, nil
}

func (s *governanceService) isAlertDispatched(
	ctx context.Context,
	tenantID string,
	alertID int64,
) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM audit_logs
  WHERE action = $1
    AND event_id = $2
    AND tenant_id = $3
  LIMIT 1
)
`, alertDispatchedAuditAction, alertMessageID(alertID), normalizeTenantID(tenantID)).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (s *governanceService) recordAlertDispatched(ctx context.Context, alert alertEvent, duplicate bool) error {
	eventID := alertMessageID(alert.AlertID)
	tenantID := normalizeTenantID(alert.TenantID)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin alert publish audit tx failed: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
SELECT pg_advisory_xact_lock(hashtext($1 || ':' || $2), hashtext($3))
`, alertDispatchedAuditAction, eventID, tenantID)
	if err != nil {
		return fmt.Errorf("acquire alert dispatch audit lock failed: %w", err)
	}

	exists := false
	err = tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM audit_logs
  WHERE action = $1
    AND event_id = $2
    AND tenant_id = $3
  LIMIT 1
)
`, alertDispatchedAuditAction, eventID, tenantID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check alert dispatch audit exists failed: %w", err)
	}
	if exists {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit alert dispatch audit tx failed: %w", err)
		}
		return nil
	}

	auditEntry, err := buildAlertDispatchedAuditLog(alert, duplicate, time.Now().UTC())
	if err != nil {
		return err
	}
	if err := insertAuditLog(ctx, tx, auditEntry); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit alert dispatch audit tx failed: %w", err)
	}
	return nil
}

func (s *governanceService) recordAlertPublished(ctx context.Context, alert alertEvent, duplicate bool) error {
	return s.recordAlertDispatched(ctx, alert, duplicate)
}

func buildAlertCreatedAuditLog(alert alertEvent) (auditLogEntry, error) {
	tenantID := normalizeTenantID(alert.TenantID)
	normalizedSeverity := strings.ToLower(strings.TrimSpace(alert.Severity))
	normalizedStage := normalizeBudgetStage(alert.Stage)
	metadata, err := json.Marshal(map[string]any{
		"alert_id":                alert.AlertID,
		"budget_id":               strings.TrimSpace(alert.BudgetID),
		"tenant_id":               tenantID,
		"source_id":               alert.SourceID,
		"period":                  strings.ToLower(strings.TrimSpace(alert.Period)),
		"window_start":            alert.WindowStart.UTC(),
		"window_end":              alert.WindowEnd.UTC(),
		"tokens_used":             alert.TokensUsed,
		"cost_used":               alert.CostUsed,
		"token_limit":             alert.TokenLimit,
		"cost_limit":              alert.CostLimit,
		"threshold":               alert.Threshold,
		"stage":                   normalizedStage,
		"threshold_snapshot":      alert.ThresholdSnapshot,
		"severity":                normalizedSeverity,
		"dedupe_key":              strings.TrimSpace(alert.DedupeKey),
		"governance_state_before": normalizeGovernanceState(alert.GovernanceStateBefore),
		"governance_state_after":  normalizeGovernanceState(alert.GovernanceStateAfter),
		"evaluated_at":            alert.EvaluatedAt.UTC(),
	})
	if err != nil {
		return auditLogEntry{}, fmt.Errorf("marshal alert created audit metadata failed: %w", err)
	}

	createdAt := alert.EvaluatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	return auditLogEntry{
		TenantID:  tenantID,
		EventID:   alertMessageID(alert.AlertID),
		Action:    alertCreatedAuditAction,
		Level:     auditLevelFromSeverity(normalizedSeverity),
		Detail:    fmt.Sprintf("governance alert %d created for budget %s", alert.AlertID, strings.TrimSpace(alert.BudgetID)),
		Metadata:  metadata,
		CreatedAt: createdAt,
	}, nil
}

func buildBudgetFrozenAuditLog(
	alertID int64,
	budget budgetRecord,
	stage string,
	thresholds thresholdSnapshot,
	governanceStateBefore string,
	governanceStateAfter string,
	createdAt time.Time,
) (auditLogEntry, error) {
	tenantID := normalizeTenantID(budget.TenantID)
	normalizedStage := normalizeBudgetStage(stage)

	metadata, err := json.Marshal(map[string]any{
		"alert_id":                alertID,
		"budget_id":               strings.TrimSpace(budget.ID),
		"tenant_id":               tenantID,
		"scope":                   strings.ToLower(strings.TrimSpace(budget.Scope)),
		"source_id":               budget.SourceID,
		"stage":                   normalizedStage,
		"threshold_snapshot":      thresholds,
		"governance_state_before": normalizeGovernanceState(governanceStateBefore),
		"governance_state_after":  normalizeGovernanceState(governanceStateAfter),
		"frozen":                  true,
		"evaluated_at":            createdAt.UTC(),
	})
	if err != nil {
		return auditLogEntry{}, fmt.Errorf("marshal budget frozen audit metadata failed: %w", err)
	}

	createdAt = createdAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	return auditLogEntry{
		TenantID:  tenantID,
		EventID:   budgetFrozenMessageID(strings.TrimSpace(budget.ID)),
		Action:    budgetFrozenAuditAction,
		Level:     "critical",
		Detail:    fmt.Sprintf("budget %s frozen by governance stage %s", strings.TrimSpace(budget.ID), normalizedStage),
		Metadata:  metadata,
		CreatedAt: createdAt,
	}, nil
}

func buildAlertDispatchedAuditLog(alert alertEvent, duplicate bool, createdAt time.Time) (auditLogEntry, error) {
	tenantID := normalizeTenantID(alert.TenantID)
	normalizedSeverity := strings.ToLower(strings.TrimSpace(alert.Severity))
	normalizedStage := normalizeBudgetStage(alert.Stage)
	metadata, err := json.Marshal(map[string]any{
		"alert_id":                alert.AlertID,
		"budget_id":               strings.TrimSpace(alert.BudgetID),
		"tenant_id":               tenantID,
		"stage":                   normalizedStage,
		"threshold_snapshot":      alert.ThresholdSnapshot,
		"governance_state_before": normalizeGovernanceState(alert.GovernanceStateBefore),
		"governance_state_after":  normalizeGovernanceState(alert.GovernanceStateAfter),
		"severity":                normalizedSeverity,
		"subject":                 alertsSubject,
		"duplicate":               duplicate,
		"dedupe_key":              strings.TrimSpace(alert.DedupeKey),
		"window_start":            alert.WindowStart.UTC(),
		"window_end":              alert.WindowEnd.UTC(),
		"created_at":              alert.CreatedAt.UTC(),
	})
	if err != nil {
		return auditLogEntry{}, fmt.Errorf("marshal alert dispatched audit metadata failed: %w", err)
	}

	createdAt = createdAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	detail := fmt.Sprintf("governance alert %d published to %s", alert.AlertID, alertsSubject)
	if duplicate {
		detail = fmt.Sprintf("governance alert %d duplicate publish acknowledged on %s", alert.AlertID, alertsSubject)
	}

	return auditLogEntry{
		TenantID:  tenantID,
		EventID:   alertMessageID(alert.AlertID),
		Action:    alertDispatchedAuditAction,
		Level:     auditLevelFromSeverity(normalizedSeverity),
		Detail:    detail,
		Metadata:  metadata,
		CreatedAt: createdAt,
	}, nil
}

func insertAuditLog(ctx context.Context, tx pgx.Tx, entry auditLogEntry) error {
	metadata := entry.Metadata
	if len(metadata) == 0 {
		metadata = []byte(`{}`)
	}

	action := strings.TrimSpace(entry.Action)
	if action == "" {
		action = "unknown"
	}

	detail := strings.TrimSpace(entry.Detail)
	createdAt := entry.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	var eventID any
	if trimmed := strings.TrimSpace(entry.EventID); trimmed != "" {
		eventID = trimmed
	}

	_, err := tx.Exec(ctx, `
INSERT INTO audit_logs (id, event_id, action, level, detail, tenant_id, metadata, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
`,
		ingest.NewID("audit"),
		eventID,
		action,
		normalizeAuditLevel(entry.Level),
		detail,
		normalizeTenantID(entry.TenantID),
		metadata,
		createdAt,
	)
	return err
}

func auditLevelFromSeverity(severity string) string {
	return normalizeAuditLevel(severity)
}

func normalizeAuditLevel(level string) string {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "critical":
		return "critical"
	case "error":
		return "error"
	case "warn", "warning":
		return "warning"
	case "info":
		return "info"
	default:
		return "info"
	}
}

func newWeeklyReportDedupeCache() *weeklyReportDedupeCache {
	return &weeklyReportDedupeCache{
		items:      make(map[string]time.Time),
		ttl:        weeklyReportDedupeCacheTTL,
		maxEntries: weeklyReportDedupeCacheMax,
	}
}

func (c *weeklyReportDedupeCache) Exists(reportID string) bool {
	if c == nil {
		return false
	}
	key := strings.TrimSpace(reportID)
	if key == "" {
		return false
	}

	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked(now)
	_, ok := c.items[key]
	if ok {
		c.items[key] = now
	}
	return ok
}

func (c *weeklyReportDedupeCache) Mark(reportID string) {
	if c == nil {
		return
	}
	key := strings.TrimSpace(reportID)
	if key == "" {
		return
	}

	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked(now)
	c.items[key] = now
}

func (c *weeklyReportDedupeCache) cleanupLocked(now time.Time) {
	if c == nil {
		return
	}

	if c.ttl > 0 {
		expireAt := now.Add(-c.ttl)
		for key, markAt := range c.items {
			if markAt.Before(expireAt) {
				delete(c.items, key)
			}
		}
	}

	if c.maxEntries <= 0 || len(c.items) <= c.maxEntries {
		return
	}

	type cacheItem struct {
		key string
		at  time.Time
	}
	items := make([]cacheItem, 0, len(c.items))
	for key, at := range c.items {
		items = append(items, cacheItem{key: key, at: at})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].at.Before(items[j].at)
	})

	toDelete := len(c.items) - c.maxEntries
	for i := 0; i < toDelete; i++ {
		delete(c.items, items[i].key)
	}
}

func (s *governanceService) publishWeeklyReportsIfDue(ctx context.Context, now time.Time) error {
	weekStart, weekEnd, due := resolveWeeklyReportWindow(now, s.weeklyReportSchedule)
	if !due {
		return nil
	}

	tenants, err := s.loadWeeklyReportTenants(ctx)
	if err != nil {
		return fmt.Errorf("load tenants failed: %w", err)
	}
	if len(tenants) == 0 {
		s.log.Info(
			"weekly report skipped: no tenant found",
			"week_start", weekStart,
			"week_end", weekEnd,
		)
		return nil
	}

	published := 0
	skipped := 0
	failed := 0
	generatedAt := now.UTC()

	for _, tenantID := range tenants {
		tenantID = normalizeTenantID(tenantID)
		reportID := buildWeeklyReportID(tenantID, weekStart, weekEnd)
		publishedTenant, skippedTenant, err := s.publishWeeklyReportForTenant(
			ctx,
			tenantID,
			weekStart,
			weekEnd,
			generatedAt,
			reportID,
		)
		if err != nil {
			failed++
			s.log.Error(
				"publish weekly report for tenant failed",
				"error", err,
				"tenant_id", tenantID,
				"report_id", reportID,
				"week_start", weekStart,
				"week_end", weekEnd,
			)
			continue
		}
		if publishedTenant {
			published++
			continue
		}
		if skippedTenant {
			skipped++
		}
	}

	s.log.Info(
		"weekly report cycle completed",
		"week_start", weekStart,
		"week_end", weekEnd,
		"tenants", len(tenants),
		"published", published,
		"skipped", skipped,
		"failed", failed,
	)

	if failed > 0 {
		return fmt.Errorf("weekly report cycle has %d failed tenant(s)", failed)
	}
	return nil
}

func (s *governanceService) publishWeeklyReportForTenant(
	ctx context.Context,
	tenantID string,
	weekStart time.Time,
	weekEnd time.Time,
	generatedAt time.Time,
	reportID string,
) (bool, bool, error) {
	if s.weeklyReportDedupe.Exists(reportID) {
		return false, true, nil
	}

	exists, err := s.isWeeklyReportPublished(ctx, tenantID, reportID)
	if err != nil {
		return false, false, fmt.Errorf("check weekly report published failed: %w", err)
	}
	if exists {
		s.weeklyReportDedupe.Mark(reportID)
		return false, true, nil
	}

	usage, err := s.aggregateWeeklyUsage(ctx, tenantID, weekStart, weekEnd)
	if err != nil {
		return false, false, fmt.Errorf("aggregate weekly usage failed: %w", err)
	}

	report := weeklyReportEvent{
		TenantID:      tenantID,
		WeekStart:     weekStart,
		WeekEnd:       weekEnd,
		Tokens:        usage.Tokens,
		Cost:          usage.Cost,
		PeakDayDate:   usage.PeakDayDate,
		PeakDayTokens: usage.PeakDayTokens,
		PeakDayCost:   usage.PeakDayCost,
		Sessions:      usage.Sessions,
		TopModels:     usage.TopModels,
		GeneratedAt:   generatedAt,
		ReportID:      reportID,
	}

	ack, err := s.publishWeeklyReport(ctx, report)
	if err != nil {
		return false, false, err
	}

	s.weeklyReportDedupe.Mark(reportID)
	if ack.Duplicate {
		return false, true, nil
	}
	return true, false, nil
}

func (s *governanceService) loadWeeklyReportTenants(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
SELECT tenant_id
FROM (
  SELECT COALESCE(NULLIF(tenant_id, ''), 'default') AS tenant_id FROM sources
  UNION
  SELECT COALESCE(NULLIF(tenant_id, ''), 'default') AS tenant_id FROM budgets
) AS tenants
ORDER BY tenant_id
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tenants := make([]string, 0)
	for rows.Next() {
		var tenantID string
		if err := rows.Scan(&tenantID); err != nil {
			return nil, fmt.Errorf("scan tenant row failed: %w", err)
		}
		tenants = append(tenants, normalizeTenantID(tenantID))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenant rows failed: %w", err)
	}

	return tenants, nil
}

func (s *governanceService) aggregateWeeklyUsage(
	ctx context.Context,
	tenantID string,
	weekStart time.Time,
	weekEnd time.Time,
) (weeklyUsageSummary, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  COALESCE(NULLIF(TRIM(sess.model), ''), 'unknown') AS model,
  TO_CHAR((COALESCE(sess.started_at, sess.created_at) AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD') AS day_date,
  COALESCE(SUM(sess.tokens), 0)::bigint AS tokens,
  COALESCE(SUM(sess.cost), 0)::double precision AS cost,
  COUNT(*)::bigint AS sessions
FROM sessions AS sess
JOIN sources AS src ON src.id = sess.source_id
WHERE COALESCE(NULLIF(src.tenant_id, ''), 'default') = $1
  AND COALESCE(sess.started_at, sess.created_at) >= $2
  AND COALESCE(sess.started_at, sess.created_at) < $3
GROUP BY
  COALESCE(NULLIF(TRIM(sess.model), ''), 'unknown'),
  (COALESCE(sess.started_at, sess.created_at) AT TIME ZONE 'UTC')::date
`, tenantID, weekStart, weekEnd)
	if err != nil {
		return weeklyUsageSummary{}, err
	}
	defer rows.Close()

	aggregates := make([]weeklyModelAggregate, 0)
	for rows.Next() {
		var item weeklyModelAggregate
		if err := rows.Scan(&item.Model, &item.DayDate, &item.Tokens, &item.Cost, &item.Sessions); err != nil {
			return weeklyUsageSummary{}, fmt.Errorf("scan weekly usage row failed: %w", err)
		}
		item.Cost = roundCost(item.Cost)
		aggregates = append(aggregates, item)
	}
	if err := rows.Err(); err != nil {
		return weeklyUsageSummary{}, fmt.Errorf("iterate weekly usage rows failed: %w", err)
	}

	return summarizeWeeklyModelUsage(aggregates, defaultWeeklyTopModelLimit), nil
}

func summarizeWeeklyModelUsage(rows []weeklyModelAggregate, topLimit int) weeklyUsageSummary {
	if topLimit <= 0 {
		topLimit = defaultWeeklyTopModelLimit
	}

	models := make(map[string]weeklyReportModelUse, len(rows))
	type dailyUsage struct {
		Tokens int64
		Cost   float64
	}
	dayUsage := make(map[string]dailyUsage, len(rows))
	summary := weeklyUsageSummary{
		TopModels: make([]weeklyReportModelUse, 0),
	}

	for _, row := range rows {
		model := strings.TrimSpace(row.Model)
		if model == "" {
			model = "unknown"
		}

		current := models[model]
		current.Model = model
		current.Tokens += row.Tokens
		current.Cost = roundCost(current.Cost + row.Cost)
		current.Sessions += row.Sessions
		models[model] = current

		summary.Tokens += row.Tokens
		summary.Cost = roundCost(summary.Cost + row.Cost)
		summary.Sessions += row.Sessions

		dayDate := strings.TrimSpace(row.DayDate)
		if dayDate != "" {
			day := dayUsage[dayDate]
			day.Tokens += row.Tokens
			day.Cost = roundCost(day.Cost + row.Cost)
			dayUsage[dayDate] = day
		}
	}

	for dayDate, day := range dayUsage {
		if summary.PeakDayDate == "" ||
			day.Tokens > summary.PeakDayTokens ||
			(day.Tokens == summary.PeakDayTokens && (day.Cost > summary.PeakDayCost || (day.Cost == summary.PeakDayCost && dayDate < summary.PeakDayDate))) {
			summary.PeakDayDate = dayDate
			summary.PeakDayTokens = day.Tokens
			summary.PeakDayCost = roundCost(day.Cost)
		}
	}

	for _, model := range models {
		model.Cost = roundCost(model.Cost)
		summary.TopModels = append(summary.TopModels, model)
	}

	sort.Slice(summary.TopModels, func(i, j int) bool {
		left := summary.TopModels[i]
		right := summary.TopModels[j]
		if left.Tokens != right.Tokens {
			return left.Tokens > right.Tokens
		}
		if left.Cost != right.Cost {
			return left.Cost > right.Cost
		}
		if left.Sessions != right.Sessions {
			return left.Sessions > right.Sessions
		}
		return left.Model < right.Model
	})

	if len(summary.TopModels) > topLimit {
		summary.TopModels = summary.TopModels[:topLimit]
	}
	summary.Cost = roundCost(summary.Cost)
	summary.PeakDayCost = roundCost(summary.PeakDayCost)
	return summary
}

func (s *governanceService) isWeeklyReportPublished(
	ctx context.Context,
	tenantID string,
	reportID string,
) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM audit_logs
  WHERE action = $1
    AND event_id = $2
    AND tenant_id = $3
  LIMIT 1
)
`, weeklyReportAuditAction, reportID, normalizeTenantID(tenantID)).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (s *governanceService) recordWeeklyReportPublished(ctx context.Context, report weeklyReportEvent, duplicate bool) error {
	tenantID := normalizeTenantID(report.TenantID)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin weekly report audit tx failed: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
SELECT pg_advisory_xact_lock(hashtext($1 || ':' || $2), hashtext($3))
`, weeklyReportAuditAction, report.ReportID, tenantID)
	if err != nil {
		return fmt.Errorf("acquire weekly report audit lock failed: %w", err)
	}

	exists := false
	err = tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM audit_logs
  WHERE action = $1
    AND event_id = $2
    AND tenant_id = $3
  LIMIT 1
)
`, weeklyReportAuditAction, report.ReportID, tenantID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check weekly report audit exists failed: %w", err)
	}
	if exists {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit weekly report audit tx failed: %w", err)
		}
		return nil
	}

	metadata, err := json.Marshal(map[string]any{
		"report_id":       report.ReportID,
		"tenant_id":       report.TenantID,
		"week_start":      report.WeekStart,
		"week_end":        report.WeekEnd,
		"tokens":          report.Tokens,
		"cost":            report.Cost,
		"peak_day_date":   report.PeakDayDate,
		"peak_day_tokens": report.PeakDayTokens,
		"peak_day_cost":   report.PeakDayCost,
		"sessions":        report.Sessions,
		"top_models":      report.TopModels,
		"generated_at":    report.GeneratedAt,
		"duplicate":       duplicate,
		"subject":         weeklyReportsSubject,
	})
	if err != nil {
		return fmt.Errorf("marshal weekly report audit metadata failed: %w", err)
	}

	detail := fmt.Sprintf("weekly report published for tenant %s", report.TenantID)
	_, err = tx.Exec(ctx, `
INSERT INTO audit_logs (id, event_id, action, level, detail, tenant_id, metadata, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
`,
		ingest.NewID("audit"),
		report.ReportID,
		weeklyReportAuditAction,
		"info",
		detail,
		tenantID,
		metadata,
		report.GeneratedAt,
	)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit weekly report audit tx failed: %w", err)
	}
	return nil
}

func (s *governanceService) publishWeeklyReport(ctx context.Context, report weeklyReportEvent) (*jetstream.PubAck, error) {
	orchestratedReport, orchestrationErr := s.applyWeeklyReportOrchestration(ctx, report)
	if orchestrationErr != nil {
		s.log.Warn("apply weekly orchestration failed, fallback to legacy dispatch",
			"error", orchestrationErr,
			"report_id", strings.TrimSpace(report.ReportID),
			"tenant_id", normalizeTenantID(report.TenantID),
		)
		orchestratedReport = report
		orchestratedReport.Orchestration = &eventOrchestration{Fallback: true}
	}

	payload, err := json.Marshal(orchestratedReport)
	if err != nil {
		return nil, fmt.Errorf("marshal weekly report payload failed: %w", err)
	}

	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ack, err := s.js.Publish(
		publishCtx,
		weeklyReportsSubject,
		payload,
		jetstream.WithMsgID(report.ReportID),
	)
	if err != nil {
		return nil, fmt.Errorf("publish to %s failed: %w", weeklyReportsSubject, err)
	}
	if err := s.recordWeeklyReportPublished(ctx, orchestratedReport, ack.Duplicate); err != nil {
		return nil, fmt.Errorf("record weekly report audit failed: %w", err)
	}

	s.log.Info(
		"governance weekly report published",
		"report_id", report.ReportID,
		"tenant_id", report.TenantID,
		"week_start", report.WeekStart,
		"week_end", report.WeekEnd,
		"subject", weeklyReportsSubject,
		"stream", ack.Stream,
		"sequence", ack.Sequence,
		"duplicate", ack.Duplicate,
	)
	return ack, nil
}

func resolveWeeklyReportWindow(
	now time.Time,
	schedule weeklyReportSchedule,
) (time.Time, time.Time, bool) {
	weekday := schedule.Weekday
	if weekday < time.Sunday || weekday > time.Saturday {
		weekday = defaultReportWeekday
	}
	hour := schedule.Hour
	if hour < 0 || hour > 23 {
		hour = defaultReportHourUTC
	}
	minute := schedule.Minute
	if minute < 0 || minute > 59 {
		minute = defaultReportMinuteUTC
	}

	nowUTC := now.UTC()
	anchorDay := weekAnchor(nowUTC, weekday)
	scheduledAt := anchorDay.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)
	if nowUTC.Before(scheduledAt) {
		return time.Time{}, time.Time{}, false
	}

	weekEnd := anchorDay
	weekStart := weekEnd.AddDate(0, 0, -7)
	return weekStart, weekEnd, true
}

func weekAnchor(nowUTC time.Time, weekday time.Weekday) time.Time {
	dayStart := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), 0, 0, 0, 0, time.UTC)
	offset := (int(dayStart.Weekday()) - int(weekday) + 7) % 7
	return dayStart.AddDate(0, 0, -offset)
}

func buildWeeklyReportID(tenantID string, weekStart time.Time, weekEnd time.Time) string {
	return fmt.Sprintf(
		"weekly:%s:%s:%s",
		normalizeTenantID(tenantID),
		weekStart.UTC().Format(time.RFC3339),
		weekEnd.UTC().Format(time.RFC3339),
	)
}

func resolveSeverity(budget budgetRecord, usage usageSnapshot) string {
	return severityFromStage(resolveStage(budget, usage))
}

func resolveStage(budget budgetRecord, usage usageSnapshot) string {
	thresholds := resolveThresholdSnapshot(budget)
	tokenRatio, costRatio := usageRatios(budget, usage)
	return resolveStageByRatios(tokenRatio, costRatio, thresholds)
}

func resolveStageByRatios(tokenRatio, costRatio *float64, thresholds thresholdSnapshot) string {
	maxRatio := -1.0
	if tokenRatio != nil {
		maxRatio = math.Max(maxRatio, *tokenRatio)
	}
	if costRatio != nil {
		maxRatio = math.Max(maxRatio, *costRatio)
	}
	if maxRatio < 0 {
		return ""
	}

	if maxRatio >= thresholds.Critical {
		return budgetStageCritical
	}
	if maxRatio >= thresholds.Escalated {
		return budgetStageEscalated
	}
	if maxRatio >= thresholds.Warning {
		return budgetStageWarning
	}
	return ""
}

func resolveThresholdSnapshot(budget budgetRecord) thresholdSnapshot {
	alertFallback := budget.AlertThreshold
	if alertFallback <= 0 || alertFallback > defaultCriticalThreshold {
		alertFallback = defaultAlertThreshold
	}

	warning := budget.WarningThreshold
	if warning <= 0 || warning > defaultCriticalThreshold {
		warning = defaultWarningThreshold
	}

	escalated := budget.EscalatedThreshold
	if escalated <= 0 || escalated > defaultCriticalThreshold {
		escalated = alertFallback
	}

	critical := budget.CriticalThreshold
	if critical <= 0 || critical > defaultCriticalThreshold {
		critical = defaultCriticalThreshold
	}

	if warning > escalated {
		warning = escalated
	}
	if escalated > critical {
		escalated = critical
	}
	if warning <= 0 || warning > defaultCriticalThreshold {
		warning = math.Max(0.01, math.Min(defaultWarningThreshold, escalated))
	}
	if escalated <= 0 || escalated > defaultCriticalThreshold {
		escalated = defaultEscalatedThreshold
	}
	if critical <= 0 || critical > defaultCriticalThreshold {
		critical = defaultCriticalThreshold
	}

	return thresholdSnapshot{
		Warning:   warning,
		Escalated: escalated,
		Critical:  critical,
	}
}

func inferThresholdSnapshot(stage string, threshold float64) thresholdSnapshot {
	stage = normalizeBudgetStage(stage)
	if threshold <= 0 || threshold > defaultCriticalThreshold {
		threshold = defaultEscalatedThreshold
	}

	snapshot := thresholdSnapshot{
		Warning:   defaultWarningThreshold,
		Escalated: defaultEscalatedThreshold,
		Critical:  defaultCriticalThreshold,
	}

	switch stage {
	case budgetStageWarning:
		snapshot.Warning = threshold
	case budgetStageEscalated:
		snapshot.Escalated = threshold
	case budgetStageCritical:
		snapshot.Critical = threshold
	default:
		snapshot.Escalated = threshold
	}

	if snapshot.Escalated <= 0 || snapshot.Escalated >= snapshot.Critical {
		snapshot.Escalated = defaultEscalatedThreshold
	}
	if snapshot.Warning <= 0 || snapshot.Warning >= snapshot.Escalated {
		snapshot.Warning = math.Max(0.01, snapshot.Escalated/2)
	}
	if snapshot.Critical <= snapshot.Escalated {
		snapshot.Critical = defaultCriticalThreshold
	}
	return snapshot
}

func thresholdForStage(stage string, thresholds thresholdSnapshot) float64 {
	switch normalizeBudgetStage(stage) {
	case budgetStageWarning:
		return thresholds.Warning
	case budgetStageEscalated:
		return thresholds.Escalated
	case budgetStageCritical:
		return thresholds.Critical
	default:
		return thresholds.Escalated
	}
}

func normalizeBudgetStage(stage string) string {
	switch strings.ToLower(strings.TrimSpace(stage)) {
	case budgetStageWarning:
		return budgetStageWarning
	case budgetStageEscalated:
		return budgetStageEscalated
	case budgetStageCritical:
		return budgetStageCritical
	default:
		return ""
	}
}

func severityFromStage(stage string) string {
	switch normalizeBudgetStage(stage) {
	case budgetStageCritical:
		return "critical"
	case budgetStageWarning, budgetStageEscalated:
		return "warning"
	default:
		return ""
	}
}

func stageFromStoredAlert(severity string, threshold float64) string {
	switch strings.ToLower(strings.TrimSpace(severity)) {
	case "critical":
		return budgetStageCritical
	case "warning":
		if threshold <= 0 || threshold > defaultWarningThreshold+1e-12 {
			return budgetStageEscalated
		}
		return budgetStageWarning
	default:
		return ""
	}
}

func resolveGovernanceStateTransition(stage string, budgetEnabled bool) (string, string, bool) {
	before := governanceStateFrozen
	if budgetEnabled {
		before = governanceStateActive
	}

	if normalizeBudgetStage(stage) != budgetStageCritical {
		return before, before, false
	}
	if budgetEnabled {
		return governanceStateActive, governanceStateFrozen, true
	}
	return governanceStateFrozen, governanceStateFrozen, false
}

func defaultGovernanceStates(stage string) (string, string) {
	switch normalizeBudgetStage(stage) {
	case budgetStageWarning, budgetStageEscalated:
		return governanceStateActive, governanceStateActive
	case budgetStageCritical:
		return governanceStateActive, governanceStateFrozen
	default:
		return governanceStateUnknown, governanceStateUnknown
	}
}

func loadedAlertGovernanceStates(stage string) (string, string) {
	switch normalizeBudgetStage(stage) {
	case budgetStageWarning, budgetStageEscalated:
		return governanceStateActive, governanceStateActive
	case budgetStageCritical:
		return governanceStateUnknown, governanceStateFrozen
	default:
		return governanceStateUnknown, governanceStateUnknown
	}
}

func normalizeGovernanceState(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case governanceStateActive:
		return governanceStateActive
	case governanceStateFrozen:
		return governanceStateFrozen
	default:
		return governanceStateUnknown
	}
}

func usageRatios(budget budgetRecord, usage usageSnapshot) (*float64, *float64) {
	var tokenRatio *float64
	if budget.TokenLimit != nil && *budget.TokenLimit > 0 {
		value := float64(usage.TokensUsed) / float64(*budget.TokenLimit)
		tokenRatio = &value
	}

	var costRatio *float64
	if budget.CostLimit != nil && *budget.CostLimit > 0 {
		value := usage.CostUsed / *budget.CostLimit
		costRatio = &value
	}

	return tokenRatio, costRatio
}

func budgetWindow(period string, now time.Time) (time.Time, time.Time, error) {
	nowUTC := now.UTC()
	switch strings.ToLower(strings.TrimSpace(period)) {
	case "daily":
		windowStart := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), 0, 0, 0, 0, time.UTC)
		return windowStart, windowStart.AddDate(0, 0, 1), nil
	case "monthly":
		windowStart := time.Date(nowUTC.Year(), nowUTC.Month(), 1, 0, 0, 0, 0, time.UTC)
		return windowStart, windowStart.AddDate(0, 1, 0), nil
	default:
		return time.Time{}, time.Time{}, fmt.Errorf("unsupported period: %s", period)
	}
}

func buildDedupeKey(
	budgetID string,
	period string,
	windowStart time.Time,
	windowEnd time.Time,
	stage string,
) string {
	return fmt.Sprintf(
		"%s:%s:%s:%s:%s",
		strings.TrimSpace(budgetID),
		strings.ToLower(strings.TrimSpace(period)),
		windowStart.UTC().Format(time.RFC3339),
		windowEnd.UTC().Format(time.RFC3339),
		normalizeBudgetStage(stage),
	)
}

func loadEvalInterval() (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv("GOV_EVAL_INTERVAL"))
	if raw == "" {
		return defaultEvalInterval, nil
	}

	interval, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid GOV_EVAL_INTERVAL: %w", err)
	}
	if interval <= 0 {
		return 0, fmt.Errorf("invalid GOV_EVAL_INTERVAL: must be > 0")
	}

	return interval, nil
}

func loadWeeklyReportSchedule() (weeklyReportSchedule, error) {
	weekdayRaw := strings.TrimSpace(os.Getenv(weeklyScheduleWeekdayEnv))
	if weekdayRaw == "" {
		weekdayRaw = defaultReportWeekday.String()
	}
	weekday, err := parseWeekday(weekdayRaw)
	if err != nil {
		return weeklyReportSchedule{}, fmt.Errorf("%s: %w", weeklyScheduleWeekdayEnv, err)
	}

	timeRaw := strings.TrimSpace(os.Getenv(weeklyScheduleTimeUTCEnv))
	if timeRaw == "" {
		timeRaw = fmt.Sprintf("%02d:%02d", defaultReportHourUTC, defaultReportMinuteUTC)
	}
	hour, minute, err := parseHHMM(timeRaw)
	if err != nil {
		return weeklyReportSchedule{}, fmt.Errorf("%s: %w", weeklyScheduleTimeUTCEnv, err)
	}

	return weeklyReportSchedule{
		Weekday: weekday,
		Hour:    hour,
		Minute:  minute,
	}, nil
}

func parseWeekday(raw string) (time.Weekday, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "sun", "sunday":
		return time.Sunday, nil
	case "1", "mon", "monday":
		return time.Monday, nil
	case "2", "tue", "tuesday":
		return time.Tuesday, nil
	case "3", "wed", "wednesday":
		return time.Wednesday, nil
	case "4", "thu", "thursday":
		return time.Thursday, nil
	case "5", "fri", "friday":
		return time.Friday, nil
	case "6", "sat", "saturday":
		return time.Saturday, nil
	default:
		return 0, fmt.Errorf("invalid weekday: %s", raw)
	}
}

func parseHHMM(raw string) (int, int, error) {
	parts := strings.Split(strings.TrimSpace(raw), ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid time %q, expected %s", raw, weeklyScheduleTimeUTCFormat)
	}

	hour, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid hour: %w", err)
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid minute: %w", err)
	}
	if hour < 0 || hour > 23 {
		return 0, 0, fmt.Errorf("hour out of range: %d", hour)
	}
	if minute < 0 || minute > 59 {
		return 0, 0, fmt.Errorf("minute out of range: %d", minute)
	}
	return hour, minute, nil
}

func initPGPool(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	if strings.TrimSpace(cfg.PG.DatabaseURL) == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.PG.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL failed: %w", err)
	}

	poolCfg.MaxConns = cfg.PG.MaxConns
	poolCfg.MinConns = cfg.PG.MinConns
	poolCfg.MaxConnLifetime = cfg.PG.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.PG.MaxConnIdleTime
	poolCfg.HealthCheckPeriod = cfg.PG.HealthCheckPeriod
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET TIME ZONE 'UTC'")
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("new pgx pool failed: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres failed: %w", err)
	}

	return pool, nil
}

func initJetStream(cfg config.Config, log interface {
	Info(string, ...any)
	Warn(string, ...any)
}) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(
		cfg.NATS.URL,
		nats.Name(cfg.ServiceName),
		nats.Timeout(cfg.NATS.ConnectTimeout),
		nats.MaxReconnects(cfg.NATS.MaxReconnects),
		nats.ReconnectWait(cfg.NATS.ReconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Warn("nats disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			log.Info("nats reconnected", "connected_url", conn.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			log.Info("nats connection closed")
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connect nats failed: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create jetstream context failed: %w", err)
	}

	return nc, js, nil
}

func ensureAlertsStream(ctx context.Context, js jetstream.JetStream, log interface {
	Info(string, ...any)
}) error {
	streamCfg := jetstream.StreamConfig{
		Name:        alertsStreamName,
		Description: "governance alerts",
		Subjects:    []string{alertsSubject},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      alertsStreamRetention,
		Duplicates:  alertsStreamDuplicateTTL,
	}

	_, err := js.Stream(ctx, alertsStreamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			if _, createErr := js.CreateStream(ctx, streamCfg); createErr != nil {
				return fmt.Errorf("create alerts stream failed: %w", createErr)
			}
			log.Info("jetstream stream created", "stream", alertsStreamName, "subjects", streamCfg.Subjects)
			return nil
		}
		return fmt.Errorf("get alerts stream failed: %w", err)
	}

	if _, err := js.UpdateStream(ctx, streamCfg); err != nil {
		return fmt.Errorf("update alerts stream failed: %w", err)
	}

	log.Info("jetstream stream ensured", "stream", alertsStreamName, "subjects", streamCfg.Subjects)
	return nil
}

func ensureWeeklyReportsStream(ctx context.Context, js jetstream.JetStream, log interface {
	Info(string, ...any)
}) error {
	streamCfg := jetstream.StreamConfig{
		Name:        weeklyReportsStreamName,
		Description: "governance weekly reports",
		Subjects:    []string{weeklyReportsSubject},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      weeklyReportsRetention,
		Duplicates:  weeklyReportsDuplicateTTL,
	}

	_, err := js.Stream(ctx, weeklyReportsStreamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			if _, createErr := js.CreateStream(ctx, streamCfg); createErr != nil {
				return fmt.Errorf("create weekly reports stream failed: %w", createErr)
			}
			log.Info("jetstream stream created", "stream", weeklyReportsStreamName, "subjects", streamCfg.Subjects)
			return nil
		}
		return fmt.Errorf("get weekly reports stream failed: %w", err)
	}

	if _, err := js.UpdateStream(ctx, streamCfg); err != nil {
		return fmt.Errorf("update weekly reports stream failed: %w", err)
	}

	log.Info("jetstream stream ensured", "stream", weeklyReportsStreamName, "subjects", streamCfg.Subjects)
	return nil
}

func normalizeTenantID(input string) string {
	tenantID := strings.TrimSpace(input)
	if tenantID == "" {
		return "default"
	}
	return tenantID
}

func trimOptionalString(input *string) *string {
	if input == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*input)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func roundCost(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return math.Round(value*alertCostRoundDecimals) / alertCostRoundDecimals
}
