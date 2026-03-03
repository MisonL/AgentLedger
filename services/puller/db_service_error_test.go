package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	sharedconfig "github.com/agentledger/agentledger/services/internal/shared/config"
)

func newUnreachablePGPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	poolCfg, err := pgxpool.ParseConfig("postgres://test:test@127.0.0.1:1/testdb?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig() failed: %v", err)
	}
	poolCfg.MinConns = 0
	poolCfg.MaxConns = 1
	poolCfg.MaxConnLifetime = time.Second
	poolCfg.MaxConnIdleTime = time.Second
	poolCfg.HealthCheckPeriod = time.Hour

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		t.Fatalf("pgxpool.NewWithConfig() failed: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newDBErrorTestService(t *testing.T) *pullerService {
	t.Helper()
	return &pullerService{
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		pool:     newUnreachablePGPool(t),
		runtime:  pullerRuntimeConfig{JobTimeout: 150 * time.Millisecond},
		hostname: "test-host",
	}
}

func TestInitPGPool_ErrorPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := initPGPool(ctx, sharedconfig.Config{PG: sharedconfig.PGConfig{}})
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Fatalf("initPGPool() error = %v, want missing DATABASE_URL", err)
	}

	_, err = initPGPool(ctx, sharedconfig.Config{
		PG: sharedconfig.PGConfig{
			DatabaseURL: "://bad-url",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "parse DATABASE_URL failed") {
		t.Fatalf("initPGPool() error = %v, want parse DATABASE_URL failed", err)
	}

	_, err = initPGPool(ctx, sharedconfig.Config{
		PG: sharedconfig.PGConfig{
			DatabaseURL:       "postgres://test:test@127.0.0.1:1/testdb?sslmode=disable&connect_timeout=1",
			MaxConns:          1,
			MinConns:          0,
			MaxConnLifetime:   time.Second,
			MaxConnIdleTime:   time.Second,
			HealthCheckPeriod: time.Second,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "ping postgres failed") {
		t.Fatalf("initPGPool() error = %v, want ping postgres failed", err)
	}
}

func TestEnsurePullerSchema_ErrorPath(t *testing.T) {
	pool := newUnreachablePGPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := ensurePullerSchema(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "ensure schema query failed") {
		t.Fatalf("ensurePullerSchema() error = %v, want ensure schema query failed", err)
	}
}

func TestDBMethods_ErrorPaths(t *testing.T) {
	svc := newDBErrorTestService(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := svc.claimNextPendingJob(ctx); err == nil || !strings.Contains(err.Error(), "claim sync job failed") {
		t.Fatalf("claimNextPendingJob() error = %v, want claim sync job failed", err)
	}
	if _, err := svc.loadSource(ctx, "source-1"); err == nil || !strings.Contains(err.Error(), "query source failed") {
		t.Fatalf("loadSource() error = %v, want query source failed", err)
	}
	if _, err := svc.listEnabledScheduledSources(ctx); err == nil || !strings.Contains(err.Error(), "query cron-enabled sources failed") {
		t.Fatalf("listEnabledScheduledSources() error = %v, want query cron-enabled sources failed", err)
	}
	if _, err := svc.listScheduledSourceIDsForMinute(ctx, time.Now()); err == nil || !strings.Contains(err.Error(), "query scheduled jobs failed") {
		t.Fatalf("listScheduledSourceIDsForMinute() error = %v, want query scheduled jobs failed", err)
	}

	created, err := svc.createScheduledSyncJob(ctx, "   ", "sync", time.Now())
	if err == nil || created || !strings.Contains(err.Error(), "source_id is required") {
		t.Fatalf("createScheduledSyncJob(blank source) = (%v, %v), want required error", created, err)
	}
	created, err = svc.createScheduledSyncJob(ctx, "source-1", "sync", time.Now())
	if err == nil || created || !strings.Contains(err.Error(), "create scheduled sync job failed") {
		t.Fatalf("createScheduledSyncJob(query error) = (%v, %v), want create failed", created, err)
	}
	if _, err := svc.createManualSyncJob(ctx, "  ", "sync"); err == nil || !strings.Contains(err.Error(), "source_id is required") {
		t.Fatalf("createManualSyncJob(blank source) error = %v, want required error", err)
	}
	if _, err := svc.createManualSyncJob(ctx, "source-1", "sync"); err == nil || !strings.Contains(err.Error(), "create manual sync job failed") {
		t.Fatalf("createManualSyncJob(query error) error = %v, want create failed", err)
	}
	if _, err := svc.loadSyncJobResult(ctx, "job-1"); err == nil || !strings.Contains(err.Error(), "query sync job result failed") {
		t.Fatalf("loadSyncJobResult() error = %v, want query sync job result failed", err)
	}

	if _, err := svc.isCancelRequested(ctx, "job-1"); err == nil || !strings.Contains(err.Error(), "query cancel_requested failed") {
		t.Fatalf("isCancelRequested() error = %v, want query cancel_requested failed", err)
	}
	if _, err := svc.getWatermark(ctx, "source-1", parserKeyJSONL, "host-key"); err == nil || !strings.Contains(err.Error(), "query watermark failed") {
		t.Fatalf("getWatermark() error = %v, want query watermark failed", err)
	}
	if err := svc.upsertWatermark(ctx, "source-1", parserKeyJSONL, "host-key", -10); err == nil || !strings.Contains(err.Error(), "upsert watermark failed") {
		t.Fatalf("upsertWatermark() error = %v, want upsert watermark failed", err)
	}
	if err := svc.insertParseFailures(ctx, "", "source-1", []parseFailure{{SourceOffset: 1, Error: "bad"}}); err == nil || !strings.Contains(err.Error(), "job_id is required") {
		t.Fatalf("insertParseFailures(empty job) error = %v, want job_id is required", err)
	}
	if err := svc.insertParseFailures(ctx, "job-1", "", []parseFailure{{SourceOffset: 1, Error: "bad"}}); err == nil || !strings.Contains(err.Error(), "source_id is required") {
		t.Fatalf("insertParseFailures(empty source) error = %v, want source_id is required", err)
	}
	if err := svc.insertParseFailures(ctx, "job-1", "source-1", []parseFailure{{SourceOffset: 1, Error: "bad"}}); err == nil || !strings.Contains(err.Error(), "insert parse failures failed") {
		t.Fatalf("insertParseFailures() error = %v, want insert parse failures failed", err)
	}

	job := syncJob{ID: "job-1", SourceID: "source-1", StartedAt: time.Time{}}
	if err := svc.finishJobStatus(ctx, job, "failed", "code1", strings.Repeat("x", 9000)); err == nil || !strings.Contains(err.Error(), "update sync job status failed") {
		t.Fatalf("finishJobStatus() error = %v, want update sync job status failed", err)
	}
	if err := svc.scheduleJobRetry(ctx, job, "code1", "retry later", time.Now().Add(10*time.Second)); err == nil || !strings.Contains(err.Error(), "schedule sync job retry failed") {
		t.Fatalf("scheduleJobRetry() error = %v, want schedule sync job retry failed", err)
	}

	finalCtx, finalizeCancel := finalizeCtx()
	defer finalizeCancel()
	if deadline, ok := finalCtx.Deadline(); !ok || time.Until(deadline) <= 0 {
		t.Fatalf("finalizeCtx() should return timeout context")
	}
}

func TestServiceMethods_ErrorPaths(t *testing.T) {
	svc := newDBErrorTestService(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := svc.pollOnce(ctx); err == nil || !strings.Contains(err.Error(), "sync cron failed") {
		t.Fatalf("pollOnce() error = %v, want sync cron failed", err)
	}

	job := syncJob{
		ID:        "job-1",
		SourceID:  "source-1",
		Attempt:   1,
		StartedAt: time.Now().Add(-1 * time.Second),
	}
	if err := svc.executeJob(ctx, job); err == nil || !strings.Contains(err.Error(), "status update failed") {
		t.Fatalf("executeJob() error = %v, want status update failed", err)
	}

	if err := svc.failJob(job, errCodeParseFailed, context.DeadlineExceeded); err == nil || !strings.Contains(err.Error(), "status update failed") {
		t.Fatalf("failJob() error = %v, want status update failed", err)
	}
	if err := svc.cancelJob(job, "cancel for test"); err == nil || !strings.Contains(err.Error(), "status update failed") {
		t.Fatalf("cancelJob() error = %v, want status update failed", err)
	}

	outputs := map[string]parserOutput{
		parserKeyJSONL: {MaxLine: 12},
	}
	if got := getOutputMaxLine(outputs, parserKeyJSONL); got != 12 {
		t.Fatalf("getOutputMaxLine(existing) = %d, want 12", got)
	}
	if got := getOutputMaxLine(outputs, parserKeyNative); got != 0 {
		t.Fatalf("getOutputMaxLine(missing) = %d, want 0", got)
	}
}
