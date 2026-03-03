package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentledger/agentledger/services/internal/shared/config"
)

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

func ensurePullerSchema(ctx context.Context, pool *pgxpool.Pool) error {
	queries := []string{
		`ALTER TABLE sync_jobs
		   ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 0,
		   ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
		   ADD COLUMN IF NOT EXISTS ended_at TIMESTAMPTZ,
		   ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
		   ADD COLUMN IF NOT EXISTS "trigger" TEXT,
		   ADD COLUMN IF NOT EXISTS error_code TEXT,
		   ADD COLUMN IF NOT EXISTS error_detail TEXT,
		   ADD COLUMN IF NOT EXISTS duration_ms BIGINT,
		   ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT FALSE`,
		`UPDATE sync_jobs
		 SET attempt = COALESCE(attempt, 0),
		     cancel_requested = COALESCE(cancel_requested, FALSE),
		     "trigger" = COALESCE(NULLIF("trigger", ''), 'manual'),
		     ended_at = COALESCE(ended_at, finished_at),
		     finished_at = COALESCE(finished_at, ended_at)
		 WHERE attempt IS NULL
		    OR cancel_requested IS NULL
		    OR "trigger" IS NULL
		    OR "trigger" = ''
		    OR (ended_at IS NULL AND finished_at IS NOT NULL)
		    OR (finished_at IS NULL AND ended_at IS NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS idx_sync_jobs_status_created_at
		 ON sync_jobs (status, created_at ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_sync_jobs_source_trigger_created_at
		 ON sync_jobs (source_id, "trigger", created_at DESC)`,
		`ALTER TABLE sources
		   ADD COLUMN IF NOT EXISTS sync_cron TEXT`,
		`CREATE TABLE IF NOT EXISTS source_watermarks (
		   source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
		   provider TEXT NOT NULL DEFAULT 'unknown',
		   parser_key TEXT NOT NULL DEFAULT 'unknown',
		   host_key TEXT NOT NULL DEFAULT '__legacy__',
		   watermark TEXT NOT NULL DEFAULT '0',
		   created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		 )`,
		`ALTER TABLE source_watermarks
		   ADD COLUMN IF NOT EXISTS provider TEXT,
		   ADD COLUMN IF NOT EXISTS parser_key TEXT,
		   ADD COLUMN IF NOT EXISTS host_key TEXT,
		   ADD COLUMN IF NOT EXISTS watermark TEXT,
		   ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		   ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`UPDATE source_watermarks
		 SET parser_key = COALESCE(NULLIF(parser_key, ''), NULLIF(provider, ''), 'unknown'),
		     host_key = COALESCE(NULLIF(host_key, ''), '__legacy__'),
		     provider = COALESCE(NULLIF(provider, ''), NULLIF(parser_key, ''), 'unknown'),
		     watermark = COALESCE(NULLIF(watermark, ''), '0'),
		     created_at = COALESCE(created_at, NOW()),
		     updated_at = COALESCE(updated_at, created_at, NOW())
		 WHERE parser_key IS NULL
		    OR parser_key = ''
		    OR host_key IS NULL
		    OR host_key = ''
		    OR provider IS NULL
		    OR provider = ''
		    OR watermark IS NULL
		    OR watermark = ''
		    OR created_at IS NULL
		    OR updated_at IS NULL`,
		`ALTER TABLE source_watermarks
		   ALTER COLUMN provider SET DEFAULT 'unknown',
		   ALTER COLUMN parser_key SET DEFAULT 'unknown',
		   ALTER COLUMN host_key SET DEFAULT '__legacy__',
		   ALTER COLUMN watermark SET DEFAULT '0'`,
		`ALTER TABLE source_watermarks
		   ALTER COLUMN provider SET NOT NULL,
		   ALTER COLUMN parser_key SET NOT NULL,
		   ALTER COLUMN host_key SET NOT NULL,
		   ALTER COLUMN watermark SET NOT NULL`,
		`WITH ranked AS (
		   SELECT ctid,
		          ROW_NUMBER() OVER (
		            PARTITION BY source_id, parser_key, host_key
		            ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, ctid DESC
		          ) AS rn
		   FROM source_watermarks
		 )
		 DELETE FROM source_watermarks AS sw
		 USING ranked
		 WHERE sw.ctid = ranked.ctid
		   AND ranked.rn > 1`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_source_watermarks_source_parser_host
		 ON source_watermarks (source_id, parser_key, host_key)`,
		`CREATE INDEX IF NOT EXISTS idx_source_watermarks_source_provider
		 ON source_watermarks (source_id, provider)`,
		`CREATE INDEX IF NOT EXISTS idx_source_watermarks_source_updated_at
		 ON source_watermarks (source_id, updated_at DESC)`,
	}

	for _, query := range queries {
		if _, err := pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("ensure schema query failed: %w", err)
		}
	}
	return nil
}

func (s *pullerService) claimNextPendingJob(ctx context.Context) (*syncJob, error) {
	row := s.pool.QueryRow(ctx, `
WITH picked AS (
  SELECT id
  FROM sync_jobs
  WHERE status = 'pending'
  ORDER BY created_at ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE sync_jobs AS job
SET status = 'running',
    attempt = COALESCE(job.attempt, 0) + 1,
    started_at = NOW(),
    ended_at = NULL,
    finished_at = NULL,
    updated_at = NOW(),
    error = NULL,
    error_code = NULL,
    error_detail = NULL,
    duration_ms = NULL
FROM picked
WHERE job.id = picked.id
RETURNING job.id,
          job.source_id,
          COALESCE(job.mode, 'realtime'),
          job.status,
          COALESCE(job.attempt, 1),
          COALESCE(job.started_at, NOW()),
          COALESCE(job.cancel_requested, FALSE)
`)

	var job syncJob
	if err := row.Scan(
		&job.ID,
		&job.SourceID,
		&job.Mode,
		&job.Status,
		&job.Attempt,
		&job.StartedAt,
		&job.CancelRequested,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("claim sync job failed: %w", err)
	}

	return &job, nil
}

func (s *pullerService) loadSource(ctx context.Context, sourceID string) (sourceRecord, error) {
	row := s.pool.QueryRow(ctx, `
SELECT
  id,
  COALESCE(name, ''),
  COALESCE(type, 'local'),
  COALESCE(location, ''),
  COALESCE(enabled, TRUE),
  COALESCE(provider, ''),
  COALESCE(hostname, ''),
  COALESCE(tenant_id, ''),
  COALESCE(workspace_id, '')
FROM sources
WHERE id = $1
LIMIT 1
`, sourceID)

	var source sourceRecord
	if err := row.Scan(
		&source.ID,
		&source.Name,
		&source.Type,
		&source.Location,
		&source.Enabled,
		&source.Provider,
		&source.Hostname,
		&source.TenantID,
		&source.WorkspaceID,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return sourceRecord{}, fmt.Errorf("source not found")
		}
		return sourceRecord{}, fmt.Errorf("query source failed: %w", err)
	}

	return source, nil
}

func (s *pullerService) listEnabledScheduledSources(ctx context.Context) ([]scheduledSource, error) {
	rows, err := s.pool.Query(ctx, `
SELECT id,
       COALESCE(sync_cron, ''),
       COALESCE(NULLIF(access_mode, ''), 'realtime')
FROM sources
WHERE COALESCE(enabled, TRUE) = TRUE
  AND COALESCE(sync_cron, '') <> ''
ORDER BY id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("query cron-enabled sources failed: %w", err)
	}
	defer rows.Close()

	sources := make([]scheduledSource, 0)
	for rows.Next() {
		var source scheduledSource
		if err := rows.Scan(&source.ID, &source.SyncCron, &source.AccessMode); err != nil {
			return nil, fmt.Errorf("scan cron-enabled source failed: %w", err)
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cron-enabled sources failed: %w", err)
	}

	return sources, nil
}

func (s *pullerService) listScheduledSourceIDsForMinute(ctx context.Context, minuteStart time.Time) (map[string]struct{}, error) {
	start, end := cronMinuteWindow(minuteStart)
	rows, err := s.pool.Query(ctx, `
SELECT source_id
FROM sync_jobs
WHERE COALESCE("trigger", '') = 'schedule'
  AND created_at >= $1
  AND created_at < $2
`, start, end)
	if err != nil {
		return nil, fmt.Errorf("query scheduled jobs failed: %w", err)
	}
	defer rows.Close()

	sourceIDs := make(map[string]struct{})
	for rows.Next() {
		var sourceID string
		if err := rows.Scan(&sourceID); err != nil {
			return nil, fmt.Errorf("scan scheduled source_id failed: %w", err)
		}
		sourceID = strings.TrimSpace(sourceID)
		if sourceID == "" {
			continue
		}
		sourceIDs[sourceID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scheduled jobs failed: %w", err)
	}

	return sourceIDs, nil
}

func (s *pullerService) createScheduledSyncJob(ctx context.Context, sourceID, mode string, now time.Time) (bool, error) {
	normalizedSourceID := strings.TrimSpace(sourceID)
	if normalizedSourceID == "" {
		return false, fmt.Errorf("source_id is required")
	}
	normalizedMode := normalizeSyncJobMode(mode)

	createdAt := now.UTC()
	minuteStart, minuteEnd := cronMinuteWindow(createdAt)
	jobID := buildScheduledJobID(normalizedSourceID, minuteStart)

	result, err := s.pool.Exec(ctx, `
INSERT INTO sync_jobs (
  id,
  source_id,
  mode,
  status,
  "trigger",
  attempt,
  created_at,
  updated_at
)
SELECT
  $1,
  $2,
  $3,
  'pending',
  'schedule',
  0,
  $4,
  $4
WHERE NOT EXISTS (
  SELECT 1
  FROM sync_jobs
  WHERE source_id = $2
    AND COALESCE("trigger", '') = 'schedule'
    AND created_at >= $5
    AND created_at < $6
)
ON CONFLICT (id) DO NOTHING
`, jobID, normalizedSourceID, normalizedMode, createdAt, minuteStart, minuteEnd)
	if err != nil {
		return false, fmt.Errorf("create scheduled sync job failed: %w", err)
	}

	return result.RowsAffected() > 0, nil
}

func (s *pullerService) isCancelRequested(ctx context.Context, jobID string) (bool, error) {
	var cancelRequested bool
	err := s.pool.QueryRow(ctx, `
SELECT COALESCE(cancel_requested, FALSE)
FROM sync_jobs
WHERE id = $1
`, jobID).Scan(&cancelRequested)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return true, nil
		}
		return false, fmt.Errorf("query cancel_requested failed: %w", err)
	}
	return cancelRequested, nil
}

func (s *pullerService) getWatermark(ctx context.Context, sourceID, parserKey, hostKey string) (int64, error) {
	normalizedParserKey := normalizeWatermarkParserKey(parserKey)
	normalizedHostKey := normalizeWatermarkHostKey(hostKey)
	lookupHostKeys := watermarkLookupHostKeys(normalizedHostKey)

	var watermarkRaw string
	err := s.pool.QueryRow(ctx, `
SELECT watermark
FROM source_watermarks
WHERE source_id = $1
  AND parser_key = $2
  AND host_key = ANY($3)
ORDER BY CASE WHEN host_key = $4 THEN 0 ELSE 1 END,
         updated_at DESC,
         created_at DESC
LIMIT 1
`, sourceID, normalizedParserKey, lookupHostKeys, normalizedHostKey).Scan(&watermarkRaw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("query watermark failed: %w", err)
	}

	value, err := parseWatermark(watermarkRaw)
	if err != nil {
		return 0, fmt.Errorf("parse watermark failed: %w", err)
	}
	return value, nil
}

func (s *pullerService) upsertWatermark(ctx context.Context, sourceID, parserKey, hostKey string, lineNo int64) error {
	if lineNo < 0 {
		lineNo = 0
	}

	normalizedParserKey := normalizeWatermarkParserKey(parserKey)
	normalizedHostKey := normalizeWatermarkHostKey(hostKey)
	provider := watermarkProviderFromParserKey(normalizedParserKey)

	_, err := s.pool.Exec(ctx, `
INSERT INTO source_watermarks (source_id, provider, parser_key, host_key, watermark, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
ON CONFLICT (source_id, parser_key, host_key)
DO UPDATE
SET provider = EXCLUDED.provider,
    watermark = (
      CASE
        WHEN source_watermarks.watermark ~ '^[0-9]+$'
             AND source_watermarks.watermark::bigint > EXCLUDED.watermark::bigint
          THEN source_watermarks.watermark
        ELSE EXCLUDED.watermark
      END
    ),
    updated_at = NOW()
`, sourceID, provider, normalizedParserKey, normalizedHostKey, fmt.Sprintf("%d", lineNo))
	if err != nil {
		return fmt.Errorf("upsert watermark failed: %w", err)
	}
	return nil
}

func (s *pullerService) finishJobStatus(ctx context.Context, job syncJob, status, code, detail string) error {
	trimmedDetail := strings.TrimSpace(detail)
	if len(trimmedDetail) > 8000 {
		trimmedDetail = trimmedDetail[:8000]
	}

	durationMS := time.Since(job.StartedAt).Milliseconds()
	if job.StartedAt.IsZero() {
		durationMS = 0
	}

	legacyError := ""
	if status == "failed" || status == "cancelled" {
		legacyError = trimmedDetail
	}

	_, err := s.pool.Exec(ctx, `
UPDATE sync_jobs
SET status = $2,
    updated_at = NOW(),
    ended_at = NOW(),
    finished_at = NOW(),
    duration_ms = $3,
    error_code = $4,
    error_detail = $5,
    error = $6
WHERE id = $1
`,
		job.ID,
		status,
		durationMS,
		nullableString(code),
		nullableString(trimmedDetail),
		nullableString(legacyError),
	)
	if err != nil {
		return fmt.Errorf("update sync job status failed: %w", err)
	}
	return nil
}

func finalizeCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}
