package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentledger/agentledger/services/internal/shared/config"
)

const (
	defaultTenantID           = "default"
	defaultTimezone           = "UTC"
	defaultWeekStartWeekday   = time.Monday
	defaultWindowDays         = 84
	maxWindowDays             = 366
	maxTenantIDLength         = 128
	maxTimezoneLength         = 128
	dateLayout                = "2006-01-02"
	isoDateTimeMillisLayout   = "2006-01-02T15:04:05.000Z07:00"
	costRoundCellsPrecision   = 1e6
	costRoundSummaryPrecision = 1e2
	costRoundWeeklyPrecision  = 1e6
	analyticsWeekStartEnv     = "ANALYTICS_WEEK_START_WEEKDAY"
	governanceWeekStartEnv    = "GOV_WEEKLY_REPORT_WEEKDAY"
)

var timezoneNamePattern = regexp.MustCompile(`^[A-Za-z0-9._+-]+(?:/[A-Za-z0-9._+-]+)*$`)

type usageMetric string

const (
	metricTokens   usageMetric = "tokens"
	metricCost     usageMetric = "cost"
	metricSessions usageMetric = "sessions"
)

type usageQuery struct {
	TenantID       string
	Metric         usageMetric
	Timezone       string
	Location       *time.Location
	From           time.Time
	To             time.Time
	WindowStartUTC time.Time
	WindowEndUTC   time.Time
}

type usageSummary struct {
	Tokens   int64   `json:"tokens"`
	Cost     float64 `json:"cost"`
	Sessions int64   `json:"sessions"`
}

type usageHeatmapCell struct {
	Date     string  `json:"date"`
	Tokens   int64   `json:"tokens"`
	Cost     float64 `json:"cost"`
	Sessions int64   `json:"sessions"`
}

type usageHeatmapResponse struct {
	Timezone string             `json:"timezone"`
	Cells    []usageHeatmapCell `json:"cells"`
	Summary  usageSummary       `json:"summary"`
}

type usageWeekItem struct {
	WeekStart string  `json:"week_start"`
	WeekEnd   string  `json:"week_end"`
	Tokens    int64   `json:"tokens"`
	Cost      float64 `json:"cost"`
	Sessions  int64   `json:"sessions"`
}

type usageWeeklySummaryResponse struct {
	Metric   usageMetric     `json:"metric"`
	Timezone string          `json:"timezone"`
	Weeks    []usageWeekItem `json:"weeks"`
	Summary  usageSummary    `json:"summary"`
	PeakWeek *usageWeekItem  `json:"peak_week,omitempty"`
}

type usageBucketRow struct {
	Bucket   string
	Tokens   int64
	Cost     float64
	Sessions int64
}

type analyticsService struct {
	log              *slog.Logger
	pool             *pgxpool.Pool
	now              func() time.Time
	weekStartWeekday time.Weekday
}

func newAnalyticsService(log *slog.Logger, pool *pgxpool.Pool, weekStartWeekday time.Weekday) *analyticsService {
	if weekStartWeekday < time.Sunday || weekStartWeekday > time.Saturday {
		weekStartWeekday = defaultWeekStartWeekday
	}
	return &analyticsService{
		log:              log,
		pool:             pool,
		now:              time.Now,
		weekStartWeekday: weekStartWeekday,
	}
}

func (s *analyticsService) handleHeatmap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	query, err := parseUsageQuery(r.URL.Query(), s.now().UTC(), false)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	rows, err := s.queryDailyUsage(r.Context(), query)
	if err != nil {
		s.log.Error("query heatmap failed", "error", err, "tenant_id", query.TenantID)
		writeError(w, http.StatusInternalServerError, "query usage heatmap failed")
		return
	}

	payload, err := buildHeatmapResponse(query.Timezone, query.Location, rows)
	if err != nil {
		s.log.Error("build heatmap response failed", "error", err, "tenant_id", query.TenantID)
		writeError(w, http.StatusInternalServerError, "build usage heatmap failed")
		return
	}

	writeJSON(w, http.StatusOK, payload)
}

func (s *analyticsService) handleWeeklySummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	query, err := parseUsageQuery(r.URL.Query(), s.now().UTC(), true)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	rows, err := s.queryWeeklyUsage(r.Context(), query)
	if err != nil {
		s.log.Error("query weekly summary failed", "error", err, "tenant_id", query.TenantID)
		writeError(w, http.StatusInternalServerError, "query usage weekly summary failed")
		return
	}

	payload, err := buildWeeklySummaryResponse(query.Metric, query.Timezone, query.Location, rows)
	if err != nil {
		s.log.Error("build weekly summary failed", "error", err, "tenant_id", query.TenantID)
		writeError(w, http.StatusInternalServerError, "build usage weekly summary failed")
		return
	}

	writeJSON(w, http.StatusOK, payload)
}

func (s *analyticsService) queryDailyUsage(ctx context.Context, query usageQuery) ([]usageBucketRow, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  to_char(date_trunc('day', timezone($4, COALESCE(sess.started_at, sess.created_at)))::date, 'YYYY-MM-DD') AS bucket,
  COALESCE(SUM(sess.tokens), 0)::bigint AS tokens,
  COALESCE(SUM(sess.cost), 0)::double precision AS cost,
  COUNT(*)::bigint AS sessions
FROM sessions AS sess
JOIN sources AS src ON src.id = sess.source_id
WHERE COALESCE(NULLIF(src.tenant_id, ''), 'default') = $1
  AND (
    (sess.started_at IS NOT NULL AND sess.started_at >= $2 AND sess.started_at < $3)
    OR
    (sess.started_at IS NULL AND sess.created_at >= $2 AND sess.created_at < $3)
  )
GROUP BY 1
ORDER BY 1 ASC
`, query.TenantID, query.WindowStartUTC, query.WindowEndUTC, query.Timezone)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]usageBucketRow, 0)
	for rows.Next() {
		var item usageBucketRow
		if err := rows.Scan(&item.Bucket, &item.Tokens, &item.Cost, &item.Sessions); err != nil {
			return nil, fmt.Errorf("scan daily usage row failed: %w", err)
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate daily usage rows failed: %w", err)
	}

	return items, nil
}

func (s *analyticsService) queryWeeklyUsage(ctx context.Context, query usageQuery) ([]usageBucketRow, error) {
	rows, err := s.pool.Query(ctx, `
WITH base AS (
  SELECT
    date_trunc('day', timezone($4, COALESCE(sess.started_at, sess.created_at)))::date AS local_day,
    sess.tokens AS tokens,
    sess.cost AS cost
  FROM sessions AS sess
  JOIN sources AS src ON src.id = sess.source_id
  WHERE COALESCE(NULLIF(src.tenant_id, ''), 'default') = $1
    AND (
      (sess.started_at IS NOT NULL AND sess.started_at >= $2 AND sess.started_at < $3)
      OR
      (sess.started_at IS NULL AND sess.created_at >= $2 AND sess.created_at < $3)
    )
)
SELECT
  to_char((local_day - ((extract(dow FROM local_day)::int - $5 + 7) % 7))::date, 'YYYY-MM-DD') AS bucket,
  COALESCE(SUM(tokens), 0)::bigint AS tokens,
  COALESCE(SUM(cost), 0)::double precision AS cost,
  COUNT(*)::bigint AS sessions
FROM base
GROUP BY 1
ORDER BY 1 ASC
`, query.TenantID, query.WindowStartUTC, query.WindowEndUTC, query.Timezone, int(s.weekStartWeekday))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]usageBucketRow, 0)
	for rows.Next() {
		var item usageBucketRow
		if err := rows.Scan(&item.Bucket, &item.Tokens, &item.Cost, &item.Sessions); err != nil {
			return nil, fmt.Errorf("scan weekly usage row failed: %w", err)
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate weekly usage rows failed: %w", err)
	}

	return items, nil
}

func buildHeatmapResponse(timezone string, location *time.Location, rows []usageBucketRow) (usageHeatmapResponse, error) {
	sorted := append([]usageBucketRow(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Bucket < sorted[j].Bucket
	})

	cells := make([]usageHeatmapCell, 0, len(sorted))
	summary := usageSummary{}

	for _, row := range sorted {
		day, err := parseBucketDate(row.Bucket, location)
		if err != nil {
			return usageHeatmapResponse{}, fmt.Errorf("invalid daily bucket %q: %w", row.Bucket, err)
		}

		cell := usageHeatmapCell{
			Date:     formatDateTimeMillis(day),
			Tokens:   nonNegativeInt64(row.Tokens),
			Cost:     roundValue(row.Cost, costRoundCellsPrecision),
			Sessions: nonNegativeInt64(row.Sessions),
		}
		cells = append(cells, cell)

		summary.Tokens += cell.Tokens
		summary.Cost += cell.Cost
		summary.Sessions += cell.Sessions
	}

	summary.Cost = roundValue(summary.Cost, costRoundSummaryPrecision)

	return usageHeatmapResponse{
		Timezone: timezone,
		Cells:    cells,
		Summary:  summary,
	}, nil
}

func buildWeeklySummaryResponse(metric usageMetric, timezone string, location *time.Location, rows []usageBucketRow) (usageWeeklySummaryResponse, error) {
	sorted := append([]usageBucketRow(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Bucket < sorted[j].Bucket
	})

	weeks := make([]usageWeekItem, 0, len(sorted))
	summary := usageSummary{}

	var peakWeek *usageWeekItem
	var peakValue float64
	var peakWeekStart time.Time

	for _, row := range sorted {
		weekStart, err := parseBucketDate(row.Bucket, location)
		if err != nil {
			return usageWeeklySummaryResponse{}, fmt.Errorf("invalid weekly bucket %q: %w", row.Bucket, err)
		}
		weekEnd := weekStart.AddDate(0, 0, 7)

		item := usageWeekItem{
			WeekStart: formatDateTimeMillis(weekStart),
			WeekEnd:   formatDateTimeMillis(weekEnd),
			Tokens:    nonNegativeInt64(row.Tokens),
			Cost:      roundValue(row.Cost, costRoundWeeklyPrecision),
			Sessions:  nonNegativeInt64(row.Sessions),
		}
		weeks = append(weeks, item)

		summary.Tokens += item.Tokens
		summary.Cost += item.Cost
		summary.Sessions += item.Sessions

		currentMetricValue := metricValue(metric, item)
		if peakWeek == nil || currentMetricValue > peakValue || (currentMetricValue == peakValue && weekStart.Before(peakWeekStart)) {
			clone := item
			peakWeek = &clone
			peakValue = currentMetricValue
			peakWeekStart = weekStart
		}
	}

	summary.Cost = roundValue(summary.Cost, costRoundSummaryPrecision)

	return usageWeeklySummaryResponse{
		Metric:   metric,
		Timezone: timezone,
		Weeks:    weeks,
		Summary:  summary,
		PeakWeek: peakWeek,
	}, nil
}

func parseUsageQuery(values url.Values, now time.Time, strictMetric bool) (usageQuery, error) {
	tenantID := strings.TrimSpace(values.Get("tenant_id"))
	if tenantID == "" {
		tenantID = defaultTenantID
	}
	if len(tenantID) > maxTenantIDLength {
		return usageQuery{}, fmt.Errorf("tenant_id 超过最大长度 %d", maxTenantIDLength)
	}

	metric := metricTokens
	metricRaw := values.Get("metric")
	if strictMetric || strings.TrimSpace(metricRaw) != "" {
		parsedMetric, metricErr := parseUsageMetric(metricRaw)
		if metricErr != nil {
			if strictMetric {
				return usageQuery{}, metricErr
			}
		} else {
			metric = parsedMetric
		}
	}

	timezone, location, err := parseTimezoneParam(values.Get("tz"))
	if err != nil {
		return usageQuery{}, err
	}

	fromRaw := strings.TrimSpace(values.Get("from"))
	toRaw := strings.TrimSpace(values.Get("to"))

	today := toDateInLocation(now, location)
	var fromDay time.Time
	var toDay time.Time

	switch {
	case fromRaw == "" && toRaw == "":
		toDay = today
		fromDay = today.AddDate(0, 0, -(defaultWindowDays - 1))
	case fromRaw != "" && toRaw != "":
		fromDay, err = parseDateParam(fromRaw, location)
		if err != nil {
			return usageQuery{}, fmt.Errorf("from 参数非法: %w", err)
		}
		toDay, err = parseDateParam(toRaw, location)
		if err != nil {
			return usageQuery{}, fmt.Errorf("to 参数非法: %w", err)
		}
	case fromRaw != "":
		fromDay, err = parseDateParam(fromRaw, location)
		if err != nil {
			return usageQuery{}, fmt.Errorf("from 参数非法: %w", err)
		}
		toDay = fromDay.AddDate(0, 0, defaultWindowDays-1)
	case toRaw != "":
		toDay, err = parseDateParam(toRaw, location)
		if err != nil {
			return usageQuery{}, fmt.Errorf("to 参数非法: %w", err)
		}
		fromDay = toDay.AddDate(0, 0, -(defaultWindowDays - 1))
	}

	if fromDay.After(toDay) {
		return usageQuery{}, fmt.Errorf("from 不能晚于 to")
	}

	if daySpan(fromDay, toDay) > maxWindowDays {
		return usageQuery{}, fmt.Errorf("查询时间窗超过 %d 天", maxWindowDays)
	}

	return usageQuery{
		TenantID:       tenantID,
		Metric:         metric,
		Timezone:       timezone,
		Location:       location,
		From:           fromDay,
		To:             toDay,
		WindowStartUTC: fromDay.UTC(),
		WindowEndUTC:   toDay.AddDate(0, 0, 1).UTC(),
	}, nil
}

func loadAnalyticsWeekStartWeekday() (time.Weekday, error) {
	raw := strings.TrimSpace(os.Getenv(analyticsWeekStartEnv))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv(governanceWeekStartEnv))
	}
	if raw == "" {
		return defaultWeekStartWeekday, nil
	}
	weekday, err := parseWeekday(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid week start weekday %q: %w", raw, err)
	}
	return weekday, nil
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
		return 0, fmt.Errorf("unsupported weekday")
	}
}

func parseUsageMetric(raw string) (usageMetric, error) {
	resolved := strings.ToLower(strings.TrimSpace(raw))
	if resolved == "" {
		return metricTokens, nil
	}

	switch usageMetric(resolved) {
	case metricTokens, metricCost, metricSessions:
		return usageMetric(resolved), nil
	default:
		return "", fmt.Errorf("metric 仅支持 tokens/cost/sessions")
	}
}

func parseTimezoneParam(raw string) (string, *time.Location, error) {
	timezone := strings.TrimSpace(raw)
	if timezone == "" {
		timezone = defaultTimezone
	}
	if len(timezone) > maxTimezoneLength {
		return "", nil, fmt.Errorf("tz 超过最大长度 %d", maxTimezoneLength)
	}
	if strings.EqualFold(timezone, "local") {
		return "", nil, fmt.Errorf("tz 必须是 IANA 时区名称或 UTC")
	}
	if !timezoneNamePattern.MatchString(timezone) {
		return "", nil, fmt.Errorf("tz 格式非法")
	}

	location, err := time.LoadLocation(timezone)
	if err != nil {
		return "", nil, fmt.Errorf("tz 无效: %w", err)
	}

	canonical := location.String()
	if canonical == "" || strings.EqualFold(canonical, "local") {
		return "", nil, fmt.Errorf("tz 必须是 IANA 时区名称或 UTC")
	}
	if len(canonical) > maxTimezoneLength {
		return "", nil, fmt.Errorf("tz 超过最大长度 %d", maxTimezoneLength)
	}
	if !timezoneNamePattern.MatchString(canonical) {
		return "", nil, fmt.Errorf("tz 格式非法")
	}
	if !strings.EqualFold(canonical, defaultTimezone) && !strings.Contains(canonical, "/") {
		return "", nil, fmt.Errorf("tz 必须是 IANA 时区名称或 UTC")
	}

	return canonical, location, nil
}

func parseDateParam(raw string, location *time.Location) (time.Time, error) {
	resolved := strings.TrimSpace(raw)
	if resolved == "" {
		return time.Time{}, fmt.Errorf("为空")
	}

	day, err := time.Parse(dateLayout, resolved)
	if err == nil {
		return time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, location), nil
	}

	instant, err := time.Parse(time.RFC3339, resolved)
	if err == nil {
		local := instant.In(location)
		return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location), nil
	}

	return time.Time{}, fmt.Errorf("必须是 YYYY-MM-DD 或 RFC3339")
}

func parseBucketDate(value string, location *time.Location) (time.Time, error) {
	if location == nil {
		location = time.UTC
	}
	day, err := time.ParseInLocation(dateLayout, strings.TrimSpace(value), location)
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, location), nil
}

func toDateInLocation(now time.Time, location *time.Location) time.Time {
	local := now.In(location)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
}

func formatDateTimeMillis(input time.Time) string {
	return input.Format(isoDateTimeMillisLayout)
}

func daySpan(fromDay, toDay time.Time) int {
	fromUTC := time.Date(fromDay.Year(), fromDay.Month(), fromDay.Day(), 0, 0, 0, 0, time.UTC)
	toUTC := time.Date(toDay.Year(), toDay.Month(), toDay.Day(), 0, 0, 0, 0, time.UTC)
	return int(toUTC.Sub(fromUTC).Hours()/24) + 1
}

func metricValue(metric usageMetric, item usageWeekItem) float64 {
	switch metric {
	case metricCost:
		return item.Cost
	case metricSessions:
		return float64(item.Sessions)
	default:
		return float64(item.Tokens)
	}
}

func nonNegativeInt64(input int64) int64 {
	if input < 0 {
		return 0
	}
	return input
}

func roundValue(input float64, precision float64) float64 {
	if math.IsNaN(input) || math.IsInf(input, 0) {
		return 0
	}
	if precision <= 0 {
		return input
	}
	return math.Round(input*precision) / precision
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("write json response failed", "error", err, "status_code", statusCode)
	}
}

func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]string{
		"error": strings.TrimSpace(message),
	})
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
