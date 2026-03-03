package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	errCodeCancelled             = "cancel_requested"
	errCodeSourceNotFound        = "source_not_found"
	errCodeSourceDisabled        = "source_disabled"
	errCodeSourceTypeUnsupported = "source_type_unsupported"
	errCodeSSHLocationInvalid    = "ssh_location_invalid"
	errCodeSSHPullFailed         = "ssh_pull_failed"
	errCodeLocalLocationInvalid  = "local_location_invalid"
	errCodeReadLocalFailed       = "read_local_failed"
	errCodeReadRemoteFailed      = "read_remote_failed"
	errCodeParseFailed           = "parse_failed"
	errCodeIngestFailed          = "ingest_failed"
	errCodeWatermarkFailed       = "watermark_failed"
)

type pullerService struct {
	log        *slog.Logger
	pool       *pgxpool.Pool
	httpClient *httpClient
	runtime    pullerRuntimeConfig
	hostname   string
	connectors *connectorRegistry
	deps       *pullerServiceDeps
}

type pullerServiceDeps struct {
	syncCron                 func(context.Context, time.Time) error
	claimNextPendingJob      func(context.Context) (*syncJob, error)
	executeJob               func(context.Context, syncJob) error
	isCancelRequested        func(context.Context, string) (bool, error)
	loadSource               func(context.Context, string) (sourceRecord, error)
	pullSSHFile              func(context.Context, sshLocation) ([]byte, error)
	fetchLocalSourceContents func(context.Context, sourceRecord) ([]sourceContent, error)
	getWatermark             func(context.Context, string, string, string) (int64, error)
	upsertWatermark          func(context.Context, string, string, string, int64) error
	insertParseFailures      func(context.Context, string, string, []parseFailure) error
	createManualSyncJob      func(context.Context, string, string) (syncJob, error)
	loadSyncJobResult        func(context.Context, string) (syncJobResult, error)
	scheduleJobRetry         func(context.Context, syncJob, string, string, time.Time) error
	finishJobStatus          func(context.Context, syncJob, string, string, string) error
}

func (s *pullerService) depSyncCron(ctx context.Context, now time.Time) error {
	if s != nil && s.deps != nil && s.deps.syncCron != nil {
		return s.deps.syncCron(ctx, now)
	}
	return s.syncCron(ctx, now)
}

func (s *pullerService) depClaimNextPendingJob(ctx context.Context) (*syncJob, error) {
	if s != nil && s.deps != nil && s.deps.claimNextPendingJob != nil {
		return s.deps.claimNextPendingJob(ctx)
	}
	return s.claimNextPendingJob(ctx)
}

func (s *pullerService) depExecuteJob(ctx context.Context, job syncJob) error {
	if s != nil && s.deps != nil && s.deps.executeJob != nil {
		return s.deps.executeJob(ctx, job)
	}
	return s.executeJob(ctx, job)
}

func (s *pullerService) depIsCancelRequested(ctx context.Context, jobID string) (bool, error) {
	if s != nil && s.deps != nil && s.deps.isCancelRequested != nil {
		return s.deps.isCancelRequested(ctx, jobID)
	}
	return s.isCancelRequested(ctx, jobID)
}

func (s *pullerService) depLoadSource(ctx context.Context, sourceID string) (sourceRecord, error) {
	if s != nil && s.deps != nil && s.deps.loadSource != nil {
		return s.deps.loadSource(ctx, sourceID)
	}
	return s.loadSource(ctx, sourceID)
}

func (s *pullerService) depPullSSHFile(ctx context.Context, location sshLocation) ([]byte, error) {
	if s != nil && s.deps != nil && s.deps.pullSSHFile != nil {
		return s.deps.pullSSHFile(ctx, location)
	}
	return s.pullSSHFile(ctx, location)
}

func (s *pullerService) depFetchLocalSourceContents(ctx context.Context, source sourceRecord) ([]sourceContent, error) {
	if s != nil && s.deps != nil && s.deps.fetchLocalSourceContents != nil {
		return s.deps.fetchLocalSourceContents(ctx, source)
	}
	return s.fetchLocalSourceContents(ctx, source)
}

func (s *pullerService) depGetWatermark(ctx context.Context, sourceID, parserKey, hostKey string) (int64, error) {
	if s != nil && s.deps != nil && s.deps.getWatermark != nil {
		return s.deps.getWatermark(ctx, sourceID, parserKey, hostKey)
	}
	return s.getWatermark(ctx, sourceID, parserKey, hostKey)
}

func (s *pullerService) depUpsertWatermark(ctx context.Context, sourceID, parserKey, hostKey string, line int64) error {
	if s != nil && s.deps != nil && s.deps.upsertWatermark != nil {
		return s.deps.upsertWatermark(ctx, sourceID, parserKey, hostKey, line)
	}
	return s.upsertWatermark(ctx, sourceID, parserKey, hostKey, line)
}

func (s *pullerService) depInsertParseFailures(ctx context.Context, jobID, sourceID string, failures []parseFailure) error {
	if s != nil && s.deps != nil && s.deps.insertParseFailures != nil {
		return s.deps.insertParseFailures(ctx, jobID, sourceID, failures)
	}
	return s.insertParseFailures(ctx, jobID, sourceID, failures)
}

func (s *pullerService) depCreateManualSyncJob(ctx context.Context, sourceID, mode string) (syncJob, error) {
	if s != nil && s.deps != nil && s.deps.createManualSyncJob != nil {
		return s.deps.createManualSyncJob(ctx, sourceID, mode)
	}
	return s.createManualSyncJob(ctx, sourceID, mode)
}

func (s *pullerService) depLoadSyncJobResult(ctx context.Context, jobID string) (syncJobResult, error) {
	if s != nil && s.deps != nil && s.deps.loadSyncJobResult != nil {
		return s.deps.loadSyncJobResult(ctx, jobID)
	}
	return s.loadSyncJobResult(ctx, jobID)
}

func (s *pullerService) depScheduleJobRetry(ctx context.Context, job syncJob, errorCode, errorDetail string, nextRunAt time.Time) error {
	if s != nil && s.deps != nil && s.deps.scheduleJobRetry != nil {
		return s.deps.scheduleJobRetry(ctx, job, errorCode, errorDetail, nextRunAt)
	}
	return s.scheduleJobRetry(ctx, job, errorCode, errorDetail, nextRunAt)
}

func (s *pullerService) depFinishJobStatus(ctx context.Context, job syncJob, status, errorCode, errorDetail string) error {
	if s != nil && s.deps != nil && s.deps.finishJobStatus != nil {
		return s.deps.finishJobStatus(ctx, job, status, errorCode, errorDetail)
	}
	return s.finishJobStatus(ctx, job, status, errorCode, errorDetail)
}

type httpClient struct {
	do func(req *http.Request) (*http.Response, error)
}

func newHTTPClient() *httpClient {
	client := &http.Client{}
	return &httpClient{do: client.Do}
}

func (c *httpClient) Do(req *http.Request) (*http.Response, error) {
	return c.do(req)
}

func (s *pullerService) pollOnce(ctx context.Context) error {
	if err := s.depSyncCron(ctx, time.Now().UTC()); err != nil {
		return fmt.Errorf("sync cron failed: %w", err)
	}

	for {
		job, err := s.depClaimNextPendingJob(ctx)
		if err != nil {
			return err
		}
		if job == nil {
			return nil
		}

		if err := s.depExecuteJob(ctx, *job); err != nil {
			if errors.Is(err, errJobCancelled) {
				s.log.Info("sync job cancelled", "job_id", job.ID, "source_id", job.SourceID)
				continue
			}
			s.log.Error("sync job failed", "job_id", job.ID, "source_id", job.SourceID, "error", err)
		}
	}
}

func (s *pullerService) executeJob(ctx context.Context, job syncJob) error {
	jobCtx, cancel := context.WithTimeout(ctx, s.runtime.JobTimeout)
	defer cancel()

	if canceled, err := s.depIsCancelRequested(jobCtx, job.ID); err != nil {
		return s.failJob(job, errCodeCancelled, fmt.Errorf("pre-check cancel failed: %w", err))
	} else if canceled || job.CancelRequested {
		return s.cancelJob(job, "job cancelled before execution")
	}

	source, err := s.depLoadSource(jobCtx, job.SourceID)
	if err != nil {
		return s.failJob(job, errCodeSourceNotFound, err)
	}
	if !source.Enabled {
		return s.failJob(job, errCodeSourceDisabled, fmt.Errorf("source is disabled"))
	}

	normalizedSourceType := strings.ToLower(strings.TrimSpace(source.Type))
	switch normalizedSourceType {
	case "local":
		return s.executeLocalSourceJob(jobCtx, job, source)
	case "ssh":
		// keep existing SSH execution flow below.
	default:
		return s.failJob(job, errCodeSourceTypeUnsupported, fmt.Errorf("source type %q is not supported", source.Type))
	}

	location, err := parseSSHLocation(source.Location)
	if err != nil {
		return s.failJob(job, errCodeSSHLocationInvalid, err)
	}

	if canceled, err := s.depIsCancelRequested(jobCtx, job.ID); err != nil {
		return s.failJob(job, errCodeCancelled, fmt.Errorf("cancel check before ssh failed: %w", err))
	} else if canceled {
		return s.cancelJob(job, "job cancelled before ssh pull")
	}

	content, err := s.depPullSSHFile(jobCtx, location)
	if err != nil {
		if errors.Is(jobCtx.Err(), context.Canceled) || errors.Is(jobCtx.Err(), context.DeadlineExceeded) {
			return s.cancelJob(job, "job cancelled during ssh pull")
		}
		return s.failJob(job, errCodeSSHPullFailed, err)
	}

	lines, err := splitLines(content)
	if err != nil {
		return s.failJob(job, errCodeReadRemoteFailed, err)
	}

	hostKey := location.HostKey()
	jsonLineWatermark, err := s.depGetWatermark(jobCtx, source.ID, parserKeyJSONL, hostKey)
	if err != nil {
		return s.failJob(job, errCodeWatermarkFailed, err)
	}
	nativeLineWatermark, err := s.depGetWatermark(jobCtx, source.ID, parserKeyNative, hostKey)
	if err != nil {
		return s.failJob(job, errCodeWatermarkFailed, err)
	}

	parseReq := parseInput{
		Source:      source,
		SourcePath:  location.Path,
		Lines:       lines,
		JSONLStart:  jsonLineWatermark,
		NativeStart: nativeLineWatermark,
		CheckCancel: func(ctx context.Context) (bool, error) {
			return s.depIsCancelRequested(ctx, job.ID)
		},
	}
	connector := s.effectiveConnectorRegistry().Select(source, location.Path)
	outputs, err := parseWithConnector(jobCtx, connector, parseReq)
	if err != nil {
		if errors.Is(err, errJobCancelled) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return s.cancelJob(job, "job cancelled while parsing")
		}
		return s.failJob(job, errCodeParseFailed, err)
	}

	events := collectAndSortEvents(outputs)
	if len(events) > 0 {
		if canceled, err := s.depIsCancelRequested(jobCtx, job.ID); err != nil {
			return s.failJob(job, errCodeCancelled, fmt.Errorf("cancel check before ingest failed: %w", err))
		} else if canceled {
			return s.cancelJob(job, "job cancelled before ingestion")
		}

		if err := s.pushEvents(jobCtx, source, job, events); err != nil {
			if errors.Is(jobCtx.Err(), context.Canceled) || errors.Is(jobCtx.Err(), context.DeadlineExceeded) {
				return s.cancelJob(job, "job cancelled while pushing ingestion")
			}
			return s.failJob(job, errCodeIngestFailed, err)
		}
	}

	parseFailures := collectAndSortParseFailures(outputs)
	if len(parseFailures) > 0 {
		if err := s.depInsertParseFailures(jobCtx, job.ID, source.ID, parseFailures); err != nil && s.log != nil {
			s.log.Warn("persist parse failures failed", "job_id", job.ID, "source_id", source.ID, "error", err, "count", len(parseFailures))
		}
	}

	if jsonOutput, ok := outputs[parserKeyJSONL]; ok && jsonOutput.MaxLine > jsonLineWatermark {
		if err := s.depUpsertWatermark(jobCtx, source.ID, parserKeyJSONL, hostKey, jsonOutput.MaxLine); err != nil {
			return s.failJob(job, errCodeWatermarkFailed, err)
		}
	}
	if nativeOutput, ok := outputs[parserKeyNative]; ok && nativeOutput.MaxLine > nativeLineWatermark {
		if err := s.depUpsertWatermark(jobCtx, source.ID, parserKeyNative, hostKey, nativeOutput.MaxLine); err != nil {
			return s.failJob(job, errCodeWatermarkFailed, err)
		}
	}

	finalize, finalizeCancel := finalizeCtx()
	defer finalizeCancel()
	if err := s.depFinishJobStatus(finalize, job, "success", "", ""); err != nil {
		return fmt.Errorf("mark sync job success failed: %w", err)
	}

	s.log.Info(
		"sync job completed",
		"job_id", job.ID,
		"source_id", job.SourceID,
		"attempt", job.Attempt,
		"events", len(events),
		"jsonl_watermark", getOutputMaxLine(outputs, parserKeyJSONL),
		"native_watermark", getOutputMaxLine(outputs, parserKeyNative),
	)

	return nil
}

func (s *pullerService) executeLocalSourceJob(jobCtx context.Context, job syncJob, source sourceRecord) error {
	contents, err := s.depFetchLocalSourceContents(jobCtx, source)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return s.cancelJob(job, "job cancelled while reading local source")
		}
		return s.failJob(job, mapLocalFetchErrorCode(err), err)
	}

	totalEvents := 0
	for _, content := range contents {
		if canceled, err := s.depIsCancelRequested(jobCtx, job.ID); err != nil {
			return s.failJob(job, errCodeCancelled, fmt.Errorf("cancel check before local read failed: %w", err))
		} else if canceled {
			return s.cancelJob(job, "job cancelled before local parse")
		}

		lines, err := splitLines(content.Content)
		if err != nil {
			return s.failJob(job, errCodeReadLocalFailed, fmt.Errorf("split local file failed (%s): %w", content.SourcePath, err))
		}

		jsonLineWatermark, err := s.depGetWatermark(jobCtx, source.ID, parserKeyJSONL, content.HostKey)
		if err != nil {
			return s.failJob(job, errCodeWatermarkFailed, err)
		}
		nativeLineWatermark, err := s.depGetWatermark(jobCtx, source.ID, parserKeyNative, content.HostKey)
		if err != nil {
			return s.failJob(job, errCodeWatermarkFailed, err)
		}

		parseReq := parseInput{
			Source:      source,
			SourcePath:  content.SourcePath,
			Lines:       lines,
			JSONLStart:  jsonLineWatermark,
			NativeStart: nativeLineWatermark,
			CheckCancel: func(ctx context.Context) (bool, error) {
				return s.depIsCancelRequested(ctx, job.ID)
			},
		}
		connector := s.effectiveConnectorRegistry().Select(source, content.SourcePath)
		outputs, err := parseWithConnector(jobCtx, connector, parseReq)
		if err != nil {
			if errors.Is(err, errJobCancelled) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return s.cancelJob(job, "job cancelled while parsing local source")
			}
			return s.failJob(job, errCodeParseFailed, err)
		}

		events := collectAndSortEvents(outputs)
		if len(events) > 0 {
			if canceled, err := s.depIsCancelRequested(jobCtx, job.ID); err != nil {
				return s.failJob(job, errCodeCancelled, fmt.Errorf("cancel check before local ingest failed: %w", err))
			} else if canceled {
				return s.cancelJob(job, "job cancelled before local ingestion")
			}

			if err := s.pushEvents(jobCtx, source, job, events); err != nil {
				if errors.Is(jobCtx.Err(), context.Canceled) || errors.Is(jobCtx.Err(), context.DeadlineExceeded) {
					return s.cancelJob(job, "job cancelled while pushing local ingestion")
				}
				return s.failJob(job, errCodeIngestFailed, err)
			}
			totalEvents += len(events)
		}

		parseFailures := collectAndSortParseFailures(outputs)
		if len(parseFailures) > 0 {
			if err := s.depInsertParseFailures(jobCtx, job.ID, source.ID, parseFailures); err != nil && s.log != nil {
				s.log.Warn("persist parse failures failed", "job_id", job.ID, "source_id", source.ID, "source_path", content.SourcePath, "error", err, "count", len(parseFailures))
			}
		}

		if jsonOutput, ok := outputs[parserKeyJSONL]; ok && jsonOutput.MaxLine > jsonLineWatermark {
			if err := s.depUpsertWatermark(jobCtx, source.ID, parserKeyJSONL, content.HostKey, jsonOutput.MaxLine); err != nil {
				return s.failJob(job, errCodeWatermarkFailed, err)
			}
		}
		if nativeOutput, ok := outputs[parserKeyNative]; ok && nativeOutput.MaxLine > nativeLineWatermark {
			if err := s.depUpsertWatermark(jobCtx, source.ID, parserKeyNative, content.HostKey, nativeOutput.MaxLine); err != nil {
				return s.failJob(job, errCodeWatermarkFailed, err)
			}
		}
	}

	finalize, finalizeCancel := finalizeCtx()
	defer finalizeCancel()
	if err := s.depFinishJobStatus(finalize, job, "success", "", ""); err != nil {
		return fmt.Errorf("mark sync job success failed: %w", err)
	}

	s.log.Info(
		"sync job completed (local)",
		"job_id", job.ID,
		"source_id", job.SourceID,
		"attempt", job.Attempt,
		"files", len(contents),
		"events", totalEvents,
	)
	return nil
}

func mapLocalFetchErrorCode(err error) string {
	switch {
	case errors.Is(err, errLocalLocationInvalid):
		return errCodeLocalLocationInvalid
	case errors.Is(err, errLocalReadFailed):
		return errCodeReadLocalFailed
	default:
		return errCodeReadLocalFailed
	}
}

func (s *pullerService) failJob(job syncJob, code string, err error) error {
	detail := ""
	if err != nil {
		detail = err.Error()
	}

	if shouldRetrySyncJobFailure(job, code, s.runtime.JobMaxRetries) {
		delay := retryBackoffDelay(s.runtime.JobRetryBaseDelay, job.Attempt)
		nextRunAt := time.Now().UTC().Add(delay)
		finalize, finalizeCancel := finalizeCtx()
		defer finalizeCancel()
		if updateErr := s.depScheduleJobRetry(finalize, job, code, detail, nextRunAt); updateErr != nil {
			return fmt.Errorf("%s (status update failed: %v)", detail, updateErr)
		}
		if s.log != nil {
			s.log.Warn(
				"sync job scheduled for retry",
				"job_id", job.ID,
				"source_id", job.SourceID,
				"attempt", job.Attempt,
				"max_retries", s.runtime.JobMaxRetries,
				"retry_after", delay.String(),
				"next_run_at", nextRunAt.Format(time.RFC3339),
				"error_code", code,
			)
		}
		return err
	}

	finalize, finalizeCancel := finalizeCtx()
	defer finalizeCancel()
	if updateErr := s.depFinishJobStatus(finalize, job, "failed", code, detail); updateErr != nil {
		return fmt.Errorf("%s (status update failed: %v)", detail, updateErr)
	}
	return err
}

func shouldRetrySyncJobFailure(job syncJob, code string, maxRetries int) bool {
	if maxRetries <= 0 {
		return false
	}
	if !isRetryableSyncJobError(code) {
		return false
	}
	return job.Attempt > 0 && job.Attempt <= maxRetries
}

func isRetryableSyncJobError(code string) bool {
	switch strings.TrimSpace(code) {
	case errCodeSSHPullFailed,
		errCodeReadLocalFailed,
		errCodeReadRemoteFailed,
		errCodeIngestFailed,
		errCodeWatermarkFailed:
		return true
	default:
		return false
	}
}

func retryBackoffDelay(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if attempt <= 1 {
		return base
	}

	delay := base
	for i := 1; i < attempt; i++ {
		if delay > time.Duration(math.MaxInt64/2) {
			return time.Duration(math.MaxInt64)
		}
		delay *= 2
	}
	return delay
}

func (s *pullerService) cancelJob(job syncJob, detail string) error {
	finalize, finalizeCancel := finalizeCtx()
	defer finalizeCancel()
	if err := s.depFinishJobStatus(finalize, job, "cancelled", errCodeCancelled, detail); err != nil {
		return fmt.Errorf("%s (status update failed: %v)", detail, err)
	}
	return errJobCancelled
}

func getOutputMaxLine(outputs map[string]parserOutput, key string) int64 {
	output, ok := outputs[key]
	if !ok {
		return 0
	}
	return output.MaxLine
}

func (s *pullerService) registerInternalRoutes(mux *http.ServeMux) {
	if s == nil || mux == nil {
		return
	}
	mux.HandleFunc("/v1/sources/", s.handleInternalSourceRoutes)
}

func (s *pullerService) handleInternalSourceRoutes(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/v1/sources/") || !strings.HasSuffix(r.URL.Path, "/sync-now") {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if !s.isInternalRequestAuthorized(r) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "unauthorized",
		})
		return
	}

	sourceID := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/sources/"), "/sync-now"))
	if sourceID == "" || strings.Contains(sourceID, "/") {
		http.NotFound(w, r)
		return
	}

	result, err := s.executeSyncNow(r.Context(), sourceID)
	if err != nil {
		if s.log != nil {
			s.log.Error("sync-now failed", "source_id", sourceID, "error", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(result)
}

func (s *pullerService) isInternalRequestAuthorized(r *http.Request) bool {
	if s == nil || r == nil {
		return false
	}

	expected := strings.TrimSpace(s.runtime.InternalToken)
	if expected == "" {
		return false
	}

	token := strings.TrimSpace(r.Header.Get("X-Internal-Token"))
	if token == "" {
		token = parseBearerToken(r.Header.Get("Authorization"))
	}
	if token == "" {
		return false
	}

	return token == expected
}

func parseBearerToken(raw string) string {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func (s *pullerService) executeSyncNow(ctx context.Context, sourceID string) (syncJobResult, error) {
	job, err := s.depCreateManualSyncJob(ctx, sourceID, "sync")
	if err != nil {
		return syncJobResult{}, err
	}

	execErr := s.depExecuteJob(ctx, job)
	if execErr != nil && !errors.Is(execErr, errJobCancelled) && s.log != nil {
		s.log.Warn("sync-now execute job returned error", "job_id", job.ID, "source_id", sourceID, "error", execErr)
	}

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := s.depLoadSyncJobResult(queryCtx, job.ID)
	if err != nil {
		fallback := syncJobResult{
			JobID:    job.ID,
			SourceID: job.SourceID,
			Attempt:  job.Attempt,
		}
		switch {
		case execErr == nil:
			fallback.Status = "success"
		case errors.Is(execErr, errJobCancelled):
			fallback.Status = "cancelled"
			fallback.ErrorCode = errCodeCancelled
			fallback.ErrorDetail = execErr.Error()
		default:
			fallback.Status = "failed"
			fallback.ErrorDetail = execErr.Error()
		}
		return fallback, nil
	}

	return result, nil
}
