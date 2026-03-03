package main

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestParseUsageQueryDefaults(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 2, 12, 30, 0, 0, time.UTC)
	query, err := parseUsageQuery(url.Values{}, now, true)
	if err != nil {
		t.Fatalf("parseUsageQuery() unexpected error: %v", err)
	}

	if query.TenantID != defaultTenantID {
		t.Fatalf("TenantID = %q, want %q", query.TenantID, defaultTenantID)
	}
	if query.Metric != metricTokens {
		t.Fatalf("Metric = %q, want %q", query.Metric, metricTokens)
	}
	if query.Timezone != defaultTimezone {
		t.Fatalf("Timezone = %q, want %q", query.Timezone, defaultTimezone)
	}
	if query.Location == nil || query.Location.String() != "UTC" {
		t.Fatalf("Location = %v, want UTC", query.Location)
	}

	wantStart := time.Date(2025, 12, 9, 0, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)
	if !query.WindowStartUTC.Equal(wantStart) {
		t.Fatalf("WindowStartUTC = %s, want %s", query.WindowStartUTC, wantStart)
	}
	if !query.WindowEndUTC.Equal(wantEnd) {
		t.Fatalf("WindowEndUTC = %s, want %s", query.WindowEndUTC, wantEnd)
	}
}

func TestParseUsageQueryTimezoneRange(t *testing.T) {
	t.Parallel()

	values := url.Values{
		"tenant_id": []string{"tenant-a"},
		"metric":    []string{"cost"},
		"tz":        []string{"Asia/Shanghai"},
		"from":      []string{"2026-02-01"},
		"to":        []string{"2026-02-07"},
	}

	query, err := parseUsageQuery(values, time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC), true)
	if err != nil {
		t.Fatalf("parseUsageQuery() unexpected error: %v", err)
	}

	if query.TenantID != "tenant-a" {
		t.Fatalf("TenantID = %q, want tenant-a", query.TenantID)
	}
	if query.Metric != metricCost {
		t.Fatalf("Metric = %q, want %q", query.Metric, metricCost)
	}
	if query.Timezone != "Asia/Shanghai" {
		t.Fatalf("Timezone = %q, want Asia/Shanghai", query.Timezone)
	}

	wantStart := time.Date(2026, 1, 31, 16, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 2, 7, 16, 0, 0, 0, time.UTC)
	if !query.WindowStartUTC.Equal(wantStart) {
		t.Fatalf("WindowStartUTC = %s, want %s", query.WindowStartUTC, wantStart)
	}
	if !query.WindowEndUTC.Equal(wantEnd) {
		t.Fatalf("WindowEndUTC = %s, want %s", query.WindowEndUTC, wantEnd)
	}
}

func TestParseUsageQueryInvalidCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input url.Values
	}{
		{
			name: "invalid metric",
			input: url.Values{
				"metric": []string{"bad-metric"},
			},
		},
		{
			name: "invalid timezone",
			input: url.Values{
				"tz": []string{"Mars/Olympus"},
			},
		},
		{
			name: "local timezone should be rejected",
			input: url.Values{
				"tz": []string{"Local"},
			},
		},
		{
			name: "timezone format should be validated",
			input: url.Values{
				"tz": []string{"Asia/Shanghai;DROP TABLE sessions"},
			},
		},
		{
			name: "timezone should respect max length",
			input: url.Values{
				"tz": []string{strings.Repeat("A", maxTimezoneLength+1)},
			},
		},
		{
			name: "invalid from",
			input: url.Values{
				"from": []string{"2026-02-31"},
			},
		},
		{
			name: "from after to",
			input: url.Values{
				"from": []string{"2026-03-10"},
				"to":   []string{"2026-03-02"},
			},
		},
		{
			name: "too large window",
			input: url.Values{
				"from": []string{"2024-01-01"},
				"to":   []string{"2026-01-01"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseUsageQuery(tt.input, time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC), true); err == nil {
				t.Fatalf("parseUsageQuery() expected error")
			}
		})
	}
}

func TestParseUsageQueryNonStrictMetricFallsBackToDefault(t *testing.T) {
	t.Parallel()

	values := url.Values{
		"metric": []string{"bad-metric"},
	}

	query, err := parseUsageQuery(values, time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC), false)
	if err != nil {
		t.Fatalf("parseUsageQuery() unexpected error: %v", err)
	}
	if query.Metric != metricTokens {
		t.Fatalf("Metric = %q, want %q", query.Metric, metricTokens)
	}
}

func TestBuildHeatmapResponse(t *testing.T) {
	t.Parallel()

	payload, err := buildHeatmapResponse("UTC", time.UTC, []usageBucketRow{
		{Bucket: "2026-03-02", Tokens: 200, Cost: 1.2345678, Sessions: 2},
		{Bucket: "2026-03-01", Tokens: 100, Cost: 0.1111111, Sessions: 1},
	})
	if err != nil {
		t.Fatalf("buildHeatmapResponse() unexpected error: %v", err)
	}

	if len(payload.Cells) != 2 {
		t.Fatalf("len(payload.Cells) = %d, want 2", len(payload.Cells))
	}
	if payload.Timezone != "UTC" {
		t.Fatalf("timezone = %q, want UTC", payload.Timezone)
	}

	if payload.Cells[0].Date != "2026-03-01T00:00:00.000Z" {
		t.Fatalf("cells[0].Date = %q, want 2026-03-01T00:00:00.000Z", payload.Cells[0].Date)
	}
	if payload.Cells[1].Date != "2026-03-02T00:00:00.000Z" {
		t.Fatalf("cells[1].Date = %q, want 2026-03-02T00:00:00.000Z", payload.Cells[1].Date)
	}
	if !almostEqual(payload.Cells[1].Cost, 1.234568) {
		t.Fatalf("cells[1].Cost = %.6f, want 1.234568", payload.Cells[1].Cost)
	}
	if !almostEqual(payload.Summary.Cost, 1.35) {
		t.Fatalf("summary.Cost = %.2f, want 1.35", payload.Summary.Cost)
	}
	if payload.Summary.Tokens != 300 {
		t.Fatalf("summary.Tokens = %d, want 300", payload.Summary.Tokens)
	}
	if payload.Summary.Sessions != 3 {
		t.Fatalf("summary.Sessions = %d, want 3", payload.Summary.Sessions)
	}
}

func TestBuildHeatmapResponseTimezoneSemantics(t *testing.T) {
	t.Parallel()

	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("load location failed: %v", err)
	}

	payload, err := buildHeatmapResponse("Asia/Shanghai", location, []usageBucketRow{
		{Bucket: "2026-03-01", Tokens: 100, Cost: 1.0, Sessions: 1},
	})
	if err != nil {
		t.Fatalf("buildHeatmapResponse() unexpected error: %v", err)
	}

	if payload.Cells[0].Date != "2026-03-01T00:00:00.000+08:00" {
		t.Fatalf("cells[0].Date = %q, want 2026-03-01T00:00:00.000+08:00", payload.Cells[0].Date)
	}
}

func TestBuildWeeklySummaryResponse(t *testing.T) {
	t.Parallel()

	payload, err := buildWeeklySummaryResponse(metricSessions, "UTC", time.UTC, []usageBucketRow{
		{Bucket: "2026-03-02", Tokens: 400, Cost: 8.0, Sessions: 10},
		{Bucket: "2026-02-23", Tokens: 500, Cost: 5.5, Sessions: 4},
	})
	if err != nil {
		t.Fatalf("buildWeeklySummaryResponse() unexpected error: %v", err)
	}

	if payload.Metric != metricSessions {
		t.Fatalf("Metric = %q, want %q", payload.Metric, metricSessions)
	}
	if payload.Timezone != "UTC" {
		t.Fatalf("timezone = %q, want UTC", payload.Timezone)
	}
	if len(payload.Weeks) != 2 {
		t.Fatalf("len(payload.Weeks) = %d, want 2", len(payload.Weeks))
	}
	if payload.Weeks[0].WeekStart != "2026-02-23T00:00:00.000Z" {
		t.Fatalf("weeks[0].WeekStart = %q, want 2026-02-23T00:00:00.000Z", payload.Weeks[0].WeekStart)
	}
	if payload.Weeks[0].WeekEnd != "2026-03-02T00:00:00.000Z" {
		t.Fatalf("weeks[0].WeekEnd = %q, want 2026-03-02T00:00:00.000Z", payload.Weeks[0].WeekEnd)
	}
	if payload.PeakWeek == nil {
		t.Fatalf("PeakWeek should not be nil")
	}
	if payload.PeakWeek.WeekStart != "2026-03-02T00:00:00.000Z" {
		t.Fatalf("peak.WeekStart = %q, want 2026-03-02T00:00:00.000Z", payload.PeakWeek.WeekStart)
	}
	if payload.PeakWeek.Sessions != 10 {
		t.Fatalf("peak.Sessions = %d, want 10", payload.PeakWeek.Sessions)
	}
	if payload.Summary.Tokens != 900 {
		t.Fatalf("summary.Tokens = %d, want 900", payload.Summary.Tokens)
	}
	if payload.Summary.Sessions != 14 {
		t.Fatalf("summary.Sessions = %d, want 14", payload.Summary.Sessions)
	}
	if !almostEqual(payload.Summary.Cost, 13.5) {
		t.Fatalf("summary.Cost = %.2f, want 13.5", payload.Summary.Cost)
	}
}

func TestBuildWeeklySummaryResponseTimezoneSemantics(t *testing.T) {
	t.Parallel()

	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("load location failed: %v", err)
	}

	payload, err := buildWeeklySummaryResponse(metricTokens, "Asia/Shanghai", location, []usageBucketRow{
		{Bucket: "2026-02-23", Tokens: 10, Cost: 1.2, Sessions: 1},
	})
	if err != nil {
		t.Fatalf("buildWeeklySummaryResponse() unexpected error: %v", err)
	}

	if payload.Weeks[0].WeekStart != "2026-02-23T00:00:00.000+08:00" {
		t.Fatalf("weeks[0].WeekStart = %q, want 2026-02-23T00:00:00.000+08:00", payload.Weeks[0].WeekStart)
	}
	if payload.Weeks[0].WeekEnd != "2026-03-02T00:00:00.000+08:00" {
		t.Fatalf("weeks[0].WeekEnd = %q, want 2026-03-02T00:00:00.000+08:00", payload.Weeks[0].WeekEnd)
	}
}

func almostEqual(a, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < 0.000001
}
