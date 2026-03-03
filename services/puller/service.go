package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

	finalize, finalizeCancel := finalizeCtx()
	defer finalizeCancel()
	if updateErr := s.depFinishJobStatus(finalize, job, "failed", code, detail); updateErr != nil {
		return fmt.Errorf("%s (status update failed: %v)", detail, updateErr)
	}
	return err
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
