package main

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestSummarizeWeeklyModelUsage(t *testing.T) {
	rows := []weeklyModelAggregate{
		{Model: "gpt-4o", DayDate: "2026-03-03", Tokens: 1200, Cost: 2.105, Sessions: 4},
		{Model: "claude-3.7", DayDate: "2026-03-04", Tokens: 1500, Cost: 3.2, Sessions: 3},
		{Model: "", DayDate: "2026-03-04", Tokens: 300, Cost: 0.8, Sessions: 2},
		{Model: "gpt-4o", DayDate: "2026-03-04", Tokens: 100, Cost: 0.245, Sessions: 1},
	}

	summary := summarizeWeeklyModelUsage(rows, 2)
	if summary.Tokens != 3100 {
		t.Fatalf("tokens mismatch: got %d want %d", summary.Tokens, 3100)
	}
	if summary.Sessions != 10 {
		t.Fatalf("sessions mismatch: got %d want %d", summary.Sessions, 10)
	}
	if math.Abs(summary.Cost-6.35) > 1e-9 {
		t.Fatalf("cost mismatch: got %.8f want %.8f", summary.Cost, 6.35)
	}
	if summary.PeakDayDate != "2026-03-04" {
		t.Fatalf("peak day date mismatch: got %q want %q", summary.PeakDayDate, "2026-03-04")
	}
	if summary.PeakDayTokens != 1900 {
		t.Fatalf("peak day tokens mismatch: got %d want %d", summary.PeakDayTokens, 1900)
	}
	if math.Abs(summary.PeakDayCost-4.245) > 1e-9 {
		t.Fatalf("peak day cost mismatch: got %.8f want %.8f", summary.PeakDayCost, 4.245)
	}
	if len(summary.TopModels) != 2 {
		t.Fatalf("top models count mismatch: got %d want %d", len(summary.TopModels), 2)
	}

	first := summary.TopModels[0]
	if first.Model != "claude-3.7" || first.Tokens != 1500 || first.Sessions != 3 {
		t.Fatalf("unexpected first top model: %+v", first)
	}
	if math.Abs(first.Cost-3.2) > 1e-9 {
		t.Fatalf("first top model cost mismatch: got %.8f want %.8f", first.Cost, 3.2)
	}

	second := summary.TopModels[1]
	if second.Model != "gpt-4o" || second.Tokens != 1300 || second.Sessions != 5 {
		t.Fatalf("unexpected second top model: %+v", second)
	}
	if math.Abs(second.Cost-2.35) > 1e-9 {
		t.Fatalf("second top model cost mismatch: got %.8f want %.8f", second.Cost, 2.35)
	}
}

func TestBuildWeeklyReportID(t *testing.T) {
	cst := time.FixedZone("UTC+8", 8*3600)
	weekStart := time.Date(2026, 3, 2, 8, 0, 0, 0, cst)
	weekEnd := weekStart.AddDate(0, 0, 7)

	got := buildWeeklyReportID(" tenant-A ", weekStart, weekEnd)
	want := "weekly:tenant-A:2026-03-02T00:00:00Z:2026-03-09T00:00:00Z"
	if got != want {
		t.Fatalf("report id mismatch: got %q want %q", got, want)
	}

	gotDefault := buildWeeklyReportID("   ", weekStart, weekEnd)
	wantDefault := "weekly:default:2026-03-02T00:00:00Z:2026-03-09T00:00:00Z"
	if gotDefault != wantDefault {
		t.Fatalf("default report id mismatch: got %q want %q", gotDefault, wantDefault)
	}
}

func TestBuildAlertOrchestrationMatchKeyPrefersDedupeKey(t *testing.T) {
	alert := alertEvent{
		AlertID:   12,
		TenantID:  " tenant-a ",
		BudgetID:  "budget-1",
		Severity:  "critical",
		DedupeKey: "dedupe-42",
	}

	got := buildAlertOrchestrationMatchKey(alert)
	if got != "alert:dedupe-42" {
		t.Fatalf("match key mismatch: got %q want %q", got, "alert:dedupe-42")
	}
}

func TestBuildAlertOrchestrationMatchKeyFallsBackToScopedFields(t *testing.T) {
	sourceID := "src-1"
	alert := alertEvent{
		AlertID:  12,
		TenantID: " tenant-a ",
		BudgetID: "budget-1",
		SourceID: &sourceID,
		Severity: "CRITICAL",
		Stage:    "warning",
	}

	got := buildAlertOrchestrationMatchKey(alert)
	want := "alert|tenant-a|budget-1|critical|src-1|warning"
	if got != want {
		t.Fatalf("match key mismatch: got %q want %q", got, want)
	}
}

func TestDetectOrchestrationConflicts(t *testing.T) {
	rules := []alertOrchestrationRule{
		{
			ID:        "rule-a",
			EventType: "alert",
			Severity:  "critical",
			Channels:  []string{"webhook", "email"},
		},
		{
			ID:        "rule-b",
			EventType: "alert",
			Severity:  "critical",
			Channels:  []string{"email"},
		},
		{
			ID:        "rule-c",
			EventType: "weekly",
			Channels:  []string{"webhook"},
		},
	}

	conflicts := detectOrchestrationConflicts(rules)
	if len(conflicts["rule-a"]) != 1 || conflicts["rule-a"][0] != "rule-b" {
		t.Fatalf("rule-a conflicts mismatch: got %v want %v", conflicts["rule-a"], []string{"rule-b"})
	}
	if len(conflicts["rule-b"]) != 1 || conflicts["rule-b"][0] != "rule-a" {
		t.Fatalf("rule-b conflicts mismatch: got %v want %v", conflicts["rule-b"], []string{"rule-a"})
	}
	if len(conflicts["rule-c"]) != 0 {
		t.Fatalf("rule-c should have no conflicts, got %v", conflicts["rule-c"])
	}
}

func TestResolveWeeklyReportWindow(t *testing.T) {
	schedule := weeklyReportSchedule{Weekday: time.Monday, Hour: 9, Minute: 0}

	before := time.Date(2026, 3, 2, 8, 59, 59, 0, time.UTC)
	_, _, due := resolveWeeklyReportWindow(before, schedule)
	if due {
		t.Fatalf("expected not due before schedule")
	}

	at := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	weekStart, weekEnd, due := resolveWeeklyReportWindow(at, schedule)
	if !due {
		t.Fatalf("expected due at schedule")
	}
	wantStart := time.Date(2026, 2, 23, 0, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	if !weekStart.Equal(wantStart) || !weekEnd.Equal(wantEnd) {
		t.Fatalf("window mismatch: got [%s, %s) want [%s, %s)", weekStart, weekEnd, wantStart, wantEnd)
	}
}

func TestResolveSeverityOnlyEvaluatesEnabledDimensions(t *testing.T) {
	tests := []struct {
		name   string
		budget budgetRecord
		usage  usageSnapshot
		want   string
	}{
		{
			name: "disabled cost limit should rely on token dimension",
			budget: budgetRecord{
				TokenLimit:         int64Ptr(100),
				CostLimit:          float64Ptr(0),
				WarningThreshold:   0.5,
				EscalatedThreshold: 0.8,
				CriticalThreshold:  1,
			},
			usage: usageSnapshot{
				TokensUsed: 70,
				CostUsed:   999,
			},
			want: "warning",
		},
		{
			name: "disabled token limit should rely on cost dimension",
			budget: budgetRecord{
				TokenLimit:         int64Ptr(0),
				CostLimit:          float64Ptr(100),
				WarningThreshold:   0.5,
				EscalatedThreshold: 0.8,
				CriticalThreshold:  1,
			},
			usage: usageSnapshot{
				TokensUsed: 9999,
				CostUsed:   70,
			},
			want: "warning",
		},
		{
			name: "enabled token limit can trigger warning compatible severity",
			budget: budgetRecord{
				TokenLimit:         int64Ptr(100),
				CostLimit:          float64Ptr(0),
				WarningThreshold:   0.5,
				EscalatedThreshold: 0.8,
				CriticalThreshold:  1,
			},
			usage: usageSnapshot{
				TokensUsed: 80,
				CostUsed:   1,
			},
			want: "warning",
		},
		{
			name: "enabled cost limit can trigger critical",
			budget: budgetRecord{
				TokenLimit:         int64Ptr(0),
				CostLimit:          float64Ptr(10),
				WarningThreshold:   0.5,
				EscalatedThreshold: 0.8,
				CriticalThreshold:  1,
			},
			usage: usageSnapshot{
				TokensUsed: 1,
				CostUsed:   12,
			},
			want: "critical",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveSeverity(tt.budget, tt.usage); got != tt.want {
				t.Fatalf("resolveSeverity() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveStageThresholdBoundaries(t *testing.T) {
	budget := budgetRecord{
		TokenLimit:         int64Ptr(100),
		CostLimit:          float64Ptr(0),
		AlertThreshold:     0.2,
		WarningThreshold:   0.5,
		EscalatedThreshold: 0.8,
		CriticalThreshold:  1,
	}

	tests := []struct {
		name       string
		tokensUsed int64
		want       string
	}{
		{name: "49 percent below warning", tokensUsed: 49, want: ""},
		{name: "50 percent warning boundary", tokensUsed: 50, want: budgetStageWarning},
		{name: "79 percent warning", tokensUsed: 79, want: budgetStageWarning},
		{name: "80 percent escalated boundary", tokensUsed: 80, want: budgetStageEscalated},
		{name: "99 percent escalated", tokensUsed: 99, want: budgetStageEscalated},
		{name: "100 percent critical boundary", tokensUsed: 100, want: budgetStageCritical},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := resolveStage(budget, usageSnapshot{TokensUsed: tt.tokensUsed})
			if got != tt.want {
				t.Fatalf("resolveStage() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveStageUsesMaxRatioAcrossDimensions(t *testing.T) {
	budget := budgetRecord{
		TokenLimit:         int64Ptr(1000),
		CostLimit:          float64Ptr(10),
		WarningThreshold:   0.5,
		EscalatedThreshold: 0.8,
		CriticalThreshold:  1,
	}
	usage := usageSnapshot{
		TokensUsed: 400,
		CostUsed:   10,
	}

	if got := resolveStage(budget, usage); got != budgetStageCritical {
		t.Fatalf("resolveStage() = %q, want %q", got, budgetStageCritical)
	}
}

func TestResolveBudgetScope(t *testing.T) {
	sourceID := " src-1 "
	orgID := " org-a "
	userID := " user-a "
	modelID := " gpt-4o-mini "

	tests := []struct {
		name           string
		scope          string
		sourceID       *string
		organizationID *string
		userID         *string
		modelName      *string
		wantSkip       bool
		wantSkipReason string
		wantFilter     *budgetScopeFilter
	}{
		{
			name:           "unknown scope should fail closed",
			scope:          "workspace",
			sourceID:       nil,
			wantSkip:       true,
			wantSkipReason: budgetScopeSkipUnknownScope,
		},
		{
			name:           "source scope without source id should skip",
			scope:          "source",
			sourceID:       nil,
			wantSkip:       true,
			wantSkipReason: budgetScopeSkipMissingScopeValue,
		},
		{
			name:           "source scope with source id should filter source",
			scope:          " source ",
			sourceID:       &sourceID,
			wantSkip:       false,
			wantSkipReason: budgetScopeSkipNone,
			wantFilter: &budgetScopeFilter{
				Scope: "source",
				Value: "src-1",
			},
		},
		{
			name:           "org scope with source id should filter workspace_id",
			scope:          "org",
			organizationID: &orgID,
			wantSkip:       false,
			wantSkipReason: budgetScopeSkipNone,
			wantFilter: &budgetScopeFilter{
				Scope: "org",
				Value: "org-a",
			},
		},
		{
			name:           "user scope with source id should filter sessions workspace",
			scope:          "user",
			userID:         &userID,
			wantSkip:       false,
			wantSkipReason: budgetScopeSkipNone,
			wantFilter: &budgetScopeFilter{
				Scope: "user",
				Value: "user-a",
			},
		},
		{
			name:           "model scope with source id should filter sessions model",
			scope:          "model",
			modelName:      &modelID,
			wantSkip:       false,
			wantSkipReason: budgetScopeSkipNone,
			wantFilter: &budgetScopeFilter{
				Scope: "model",
				Value: "gpt-4o-mini",
			},
		},
		{
			name:           "tenant scope should pass",
			scope:          "tenant",
			sourceID:       nil,
			wantSkip:       false,
			wantSkipReason: budgetScopeSkipNone,
		},
		{
			name:           "global scope should pass",
			scope:          "global",
			sourceID:       nil,
			wantSkip:       false,
			wantSkipReason: budgetScopeSkipNone,
		},
		{
			name:           "empty scope should pass as tenant",
			scope:          "   ",
			sourceID:       nil,
			wantSkip:       false,
			wantSkipReason: budgetScopeSkipNone,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gotFilter, gotSkip, gotSkipReason := resolveBudgetScope(budgetRecord{
				Scope:          tt.scope,
				SourceID:       tt.sourceID,
				OrganizationID: tt.organizationID,
				UserID:         tt.userID,
				ModelName:      tt.modelName,
			})
			if gotSkip != tt.wantSkip {
				t.Fatalf("skip mismatch: got %v want %v", gotSkip, tt.wantSkip)
			}
			if gotSkipReason != tt.wantSkipReason {
				t.Fatalf("skip reason mismatch: got %q want %q", gotSkipReason, tt.wantSkipReason)
			}
			if !sameBudgetScopeFilter(gotFilter, tt.wantFilter) {
				t.Fatalf("scope filter mismatch: got %#v want %#v", gotFilter, tt.wantFilter)
			}
		})
	}
}

func TestResolveThresholdSnapshotUsesDedicatedThresholdFields(t *testing.T) {
	budget := budgetRecord{
		AlertThreshold:     0.2,
		WarningThreshold:   0.55,
		EscalatedThreshold: 0.83,
		CriticalThreshold:  0.97,
	}

	got := resolveThresholdSnapshot(budget)
	if math.Abs(got.Warning-0.55) > 1e-9 {
		t.Fatalf("warning threshold mismatch: got %.8f want %.8f", got.Warning, 0.55)
	}
	if math.Abs(got.Escalated-0.83) > 1e-9 {
		t.Fatalf("escalated threshold mismatch: got %.8f want %.8f", got.Escalated, 0.83)
	}
	if math.Abs(got.Critical-0.97) > 1e-9 {
		t.Fatalf("critical threshold mismatch: got %.8f want %.8f", got.Critical, 0.97)
	}
}

func TestResolveGovernanceStateTransitionIdempotent(t *testing.T) {
	before, after, shouldFreeze := resolveGovernanceStateTransition(budgetStageCritical, true)
	if before != governanceStateActive || after != governanceStateFrozen || !shouldFreeze {
		t.Fatalf("first transition mismatch: before=%s after=%s shouldFreeze=%v", before, after, shouldFreeze)
	}

	before, after, shouldFreeze = resolveGovernanceStateTransition(budgetStageCritical, false)
	if before != governanceStateFrozen || after != governanceStateFrozen || shouldFreeze {
		t.Fatalf("idempotent transition mismatch: before=%s after=%s shouldFreeze=%v", before, after, shouldFreeze)
	}
}

func TestAuditLevelFromSeverity(t *testing.T) {
	tests := []struct {
		name     string
		severity string
		want     string
	}{
		{name: "critical should stay critical", severity: "critical", want: "critical"},
		{name: "critical should keep after normalize", severity: " CRITICAL ", want: "critical"},
		{name: "warning maps to warning", severity: "warning", want: "warning"},
		{name: "unknown maps to info", severity: "panic", want: "info"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := auditLevelFromSeverity(tt.severity); got != tt.want {
				t.Fatalf("auditLevelFromSeverity() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildAlertCreatedAuditLogIncludesTenantAndCriticalLevel(t *testing.T) {
	evaluatedAt := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
	windowStart := evaluatedAt.Add(-time.Hour)
	windowEnd := evaluatedAt

	alert := alertEvent{
		AlertID:               42,
		TenantID:              " tenant-a ",
		BudgetID:              " budget-1 ",
		SourceID:              strPtr("src-1"),
		Period:                " Daily ",
		WindowStart:           windowStart,
		WindowEnd:             windowEnd,
		TokensUsed:            120,
		CostUsed:              12,
		TokenLimit:            int64Ptr(100),
		CostLimit:             float64Ptr(10),
		Threshold:             1,
		Stage:                 budgetStageCritical,
		ThresholdSnapshot:     thresholdSnapshot{Warning: 0.5, Escalated: 0.8, Critical: 1},
		Severity:              "CRITICAL",
		DedupeKey:             "dedupe-1",
		GovernanceStateBefore: governanceStateActive,
		GovernanceStateAfter:  governanceStateFrozen,
		EvaluatedAt:           evaluatedAt,
	}

	entry, err := buildAlertCreatedAuditLog(alert)
	if err != nil {
		t.Fatalf("buildAlertCreatedAuditLog() error = %v", err)
	}
	if entry.TenantID != "tenant-a" {
		t.Fatalf("tenant mismatch: got %q want %q", entry.TenantID, "tenant-a")
	}
	if entry.Level != "critical" {
		t.Fatalf("level mismatch: got %q want %q", entry.Level, "critical")
	}

	var metadata map[string]any
	if err := json.Unmarshal(entry.Metadata, &metadata); err != nil {
		t.Fatalf("unmarshal metadata failed: %v", err)
	}
	if gotTenant, ok := metadata["tenant_id"].(string); !ok || gotTenant != "tenant-a" {
		t.Fatalf("metadata tenant mismatch: got %#v want %q", metadata["tenant_id"], "tenant-a")
	}
	if gotStage, ok := metadata["stage"].(string); !ok || gotStage != budgetStageCritical {
		t.Fatalf("metadata stage mismatch: got %#v want %q", metadata["stage"], budgetStageCritical)
	}
	if gotAfter, ok := metadata["governance_state_after"].(string); !ok || gotAfter != governanceStateFrozen {
		t.Fatalf("metadata governance_state_after mismatch: got %#v want %q", metadata["governance_state_after"], governanceStateFrozen)
	}
}

func TestBuildAlertDispatchedAuditLogIncludesTenantAndCriticalLevel(t *testing.T) {
	createdAt := time.Date(2026, 3, 2, 12, 30, 0, 0, time.UTC)
	alert := alertEvent{
		AlertID:               9,
		TenantID:              "   ",
		BudgetID:              "budget-9",
		Stage:                 budgetStageCritical,
		ThresholdSnapshot:     thresholdSnapshot{Warning: 0.5, Escalated: 0.8, Critical: 1},
		Severity:              " critical ",
		DedupeKey:             "dedupe-key",
		GovernanceStateBefore: governanceStateActive,
		GovernanceStateAfter:  governanceStateFrozen,
		WindowStart:           createdAt.Add(-time.Hour),
		WindowEnd:             createdAt,
		CreatedAt:             createdAt,
	}

	entry, err := buildAlertDispatchedAuditLog(alert, false, createdAt)
	if err != nil {
		t.Fatalf("buildAlertDispatchedAuditLog() error = %v", err)
	}
	if entry.TenantID != "default" {
		t.Fatalf("tenant mismatch: got %q want %q", entry.TenantID, "default")
	}
	if entry.Level != "critical" {
		t.Fatalf("level mismatch: got %q want %q", entry.Level, "critical")
	}
}

func TestBuildBudgetFrozenAuditLogUsesGovernanceAction(t *testing.T) {
	now := time.Date(2026, 3, 2, 13, 0, 0, 0, time.UTC)
	budget := budgetRecord{
		ID:       " budget-2 ",
		TenantID: " tenant-b ",
		Scope:    "org",
		SourceID: strPtr("org-1"),
	}

	entry, err := buildBudgetFrozenAuditLog(
		101,
		budget,
		budgetStageCritical,
		thresholdSnapshot{Warning: 0.5, Escalated: 0.8, Critical: 1},
		governanceStateActive,
		governanceStateFrozen,
		now,
	)
	if err != nil {
		t.Fatalf("buildBudgetFrozenAuditLog() error = %v", err)
	}
	if entry.Action != budgetFrozenAuditAction {
		t.Fatalf("action mismatch: got %q want %q", entry.Action, budgetFrozenAuditAction)
	}
	if entry.TenantID != "tenant-b" {
		t.Fatalf("tenant mismatch: got %q want %q", entry.TenantID, "tenant-b")
	}
}

func int64Ptr(v int64) *int64 {
	return &v
}

func float64Ptr(v float64) *float64 {
	return &v
}

func strPtr(v string) *string {
	return &v
}

func sameBudgetScopeFilter(a, b *budgetScopeFilter) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Scope == b.Scope && a.Value == b.Value
}
