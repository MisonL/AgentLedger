package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agentledger/agentledger/services/internal/shared/ingest"
)

type serviceTestConnector struct {
	outputs map[string]parserOutput
	err     error
}

func (c serviceTestConnector) Name() string {
	return "service-test"
}

func (c serviceTestConnector) Match(source sourceRecord, sourcePath string) bool {
	return true
}

func (c serviceTestConnector) Parse(ctx context.Context, input parseInput) (map[string]parserOutput, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.outputs, nil
}

func TestMapLocalFetchErrorCode_MappingBranches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want string
	}{
		{name: "location_invalid", err: errLocalLocationInvalid, want: errCodeLocalLocationInvalid},
		{name: "read_failed", err: errLocalReadFailed, want: errCodeReadLocalFailed},
		{name: "default", err: errors.New("boom"), want: errCodeReadLocalFailed},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := mapLocalFetchErrorCode(tt.err); got != tt.want {
				t.Fatalf("mapLocalFetchErrorCode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExecuteJob_UnsupportedSourceType_FailsWithCode(t *testing.T) {
	var (
		gotStatus string
		gotCode   string
	)

	svc := &pullerService{
		runtime: pullerRuntimeConfig{JobTimeout: time.Second},
		deps: &pullerServiceDeps{
			isCancelRequested: func(context.Context, string) (bool, error) {
				return false, nil
			},
			loadSource: func(context.Context, string) (sourceRecord, error) {
				return sourceRecord{
					ID:      "source-1",
					Type:    "sftp",
					Enabled: true,
				}, nil
			},
			finishJobStatus: func(ctx context.Context, job syncJob, status, errorCode, errorDetail string) error {
				gotStatus = status
				gotCode = errorCode
				return nil
			},
		},
	}

	err := svc.executeJob(context.Background(), syncJob{
		ID:       "job-1",
		SourceID: "source-1",
	})
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("executeJob() error = %v, want unsupported source type", err)
	}
	if gotStatus != "failed" || gotCode != errCodeSourceTypeUnsupported {
		t.Fatalf("finishJobStatus = (%q, %q), want (failed, %q)", gotStatus, gotCode, errCodeSourceTypeUnsupported)
	}
}

func TestExecuteJob_CancelBeforeExecution_CancelsJob(t *testing.T) {
	var (
		gotStatus string
		gotCode   string
	)

	svc := &pullerService{
		runtime: pullerRuntimeConfig{JobTimeout: time.Second},
		deps: &pullerServiceDeps{
			isCancelRequested: func(context.Context, string) (bool, error) {
				return true, nil
			},
			finishJobStatus: func(ctx context.Context, job syncJob, status, errorCode, errorDetail string) error {
				gotStatus = status
				gotCode = errorCode
				return nil
			},
		},
	}

	err := svc.executeJob(context.Background(), syncJob{
		ID:       "job-2",
		SourceID: "source-2",
	})
	if !errors.Is(err, errJobCancelled) {
		t.Fatalf("executeJob() error = %v, want errJobCancelled", err)
	}
	if gotStatus != "cancelled" || gotCode != errCodeCancelled {
		t.Fatalf("finishJobStatus = (%q, %q), want (cancelled, %q)", gotStatus, gotCode, errCodeCancelled)
	}
}

func TestExecuteJob_LocalSource_Success_UpdatesWatermarkAndFinish(t *testing.T) {
	var (
		finishStatus   string
		upsertCalls    int
		getWaterCalled int
		parseFailCalls int
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0}`))
	}))
	defer server.Close()

	svc := &pullerService{
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		httpClient: newHTTPClient(),
		runtime: pullerRuntimeConfig{
			JobTimeout:     time.Second,
			IngestTimeout:  time.Second,
			IngestEndpoint: server.URL,
			AgentID:        "puller-test",
		},
		hostname: "local-host",
		connectors: newConnectorRegistry(serviceTestConnector{
			outputs: map[string]parserOutput{
				parserKeyJSONL: {
					ParserKey: parserKeyJSONL,
					MaxLine:   2,
					Events: []rawEventWithLine{
						{
							LineNo: 2,
							Event: ingest.RawEvent{
								SessionID: "session-1",
								EventType: "message",
							},
						},
					},
					Failures: []parseFailure{
						{
							ParserKey:    parserKeyJSONL,
							SourcePath:   "/tmp/chat.log",
							SourceOffset: 1,
							Error:        "invalid json",
						},
					},
				},
				parserKeyNative: {
					ParserKey: parserKeyNative,
					MaxLine:   1,
				},
			},
		}),
		deps: &pullerServiceDeps{
			isCancelRequested: func(context.Context, string) (bool, error) {
				return false, nil
			},
			loadSource: func(context.Context, string) (sourceRecord, error) {
				return sourceRecord{
					ID:          "source-local-1",
					Type:        "local",
					Location:    "/tmp/chat.log",
					Enabled:     true,
					TenantID:    "tenant-a",
					WorkspaceID: "ws-a",
				}, nil
			},
			fetchLocalSourceContents: func(context.Context, sourceRecord) ([]sourceContent, error) {
				return []sourceContent{
					{
						SourcePath: "/tmp/chat.log",
						HostKey:    "local:/tmp/chat.log",
						Content:    []byte("hello\n"),
					},
				}, nil
			},
			getWatermark: func(context.Context, string, string, string) (int64, error) {
				getWaterCalled++
				return 0, nil
			},
			upsertWatermark: func(context.Context, string, string, string, int64) error {
				upsertCalls++
				return nil
			},
			insertParseFailures: func(ctx context.Context, jobID, sourceID string, failures []parseFailure) error {
				parseFailCalls++
				if jobID == "" || sourceID == "" || len(failures) != 1 {
					t.Fatalf("insertParseFailures args invalid: jobID=%q sourceID=%q failures=%#v", jobID, sourceID, failures)
				}
				return nil
			},
			finishJobStatus: func(ctx context.Context, job syncJob, status, errorCode, errorDetail string) error {
				finishStatus = status
				return nil
			},
		},
	}

	err := svc.executeJob(context.Background(), syncJob{
		ID:       "job-local-1",
		SourceID: "source-local-1",
		Attempt:  1,
	})
	if err != nil {
		t.Fatalf("executeJob(local) unexpected error: %v", err)
	}
	if finishStatus != "success" {
		t.Fatalf("finish status = %q, want success", finishStatus)
	}
	if getWaterCalled != 2 {
		t.Fatalf("getWatermark calls = %d, want 2", getWaterCalled)
	}
	if upsertCalls != 2 {
		t.Fatalf("upsertWatermark calls = %d, want 2", upsertCalls)
	}
	if parseFailCalls != 1 {
		t.Fatalf("insertParseFailures calls = %d, want 1", parseFailCalls)
	}
}

func TestExecuteLocalSourceJob_LocalFetchErrorMapped(t *testing.T) {
	var (
		gotStatus string
		gotCode   string
	)

	svc := &pullerService{
		runtime: pullerRuntimeConfig{JobTimeout: time.Second},
		deps: &pullerServiceDeps{
			fetchLocalSourceContents: func(context.Context, sourceRecord) ([]sourceContent, error) {
				return nil, errLocalLocationInvalid
			},
			finishJobStatus: func(ctx context.Context, job syncJob, status, errorCode, errorDetail string) error {
				gotStatus = status
				gotCode = errorCode
				return nil
			},
		},
	}

	err := svc.executeLocalSourceJob(context.Background(), syncJob{
		ID:       "job-local-2",
		SourceID: "source-local-2",
	}, sourceRecord{
		ID:       "source-local-2",
		Type:     "local",
		Location: "/invalid/path",
		Enabled:  true,
	})
	if !errors.Is(err, errLocalLocationInvalid) {
		t.Fatalf("executeLocalSourceJob() error = %v, want errLocalLocationInvalid", err)
	}
	if gotStatus != "failed" || gotCode != errCodeLocalLocationInvalid {
		t.Fatalf("finishJobStatus = (%q, %q), want (failed, %q)", gotStatus, gotCode, errCodeLocalLocationInvalid)
	}
}

func TestPollOnce_ContinuesAfterCancelledJob(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	claimCalls := 0
	execCalls := 0

	svc := &pullerService{
		log: log,
		deps: &pullerServiceDeps{
			syncCron: func(context.Context, time.Time) error {
				return nil
			},
			claimNextPendingJob: func(context.Context) (*syncJob, error) {
				claimCalls++
				if claimCalls == 1 {
					return &syncJob{ID: "job-1", SourceID: "source-1"}, nil
				}
				return nil, nil
			},
			executeJob: func(context.Context, syncJob) error {
				execCalls++
				return errJobCancelled
			},
		},
	}

	if err := svc.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce() unexpected error: %v", err)
	}
	if execCalls != 1 {
		t.Fatalf("executeJob calls = %d, want 1", execCalls)
	}
}

func TestPollOnce_JobErrorContinuesToNextJob(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	claimCalls := 0
	execCalls := 0

	svc := &pullerService{
		log: log,
		deps: &pullerServiceDeps{
			syncCron: func(context.Context, time.Time) error {
				return nil
			},
			claimNextPendingJob: func(context.Context) (*syncJob, error) {
				claimCalls++
				switch claimCalls {
				case 1:
					return &syncJob{ID: "job-1", SourceID: "source-1"}, nil
				case 2:
					return &syncJob{ID: "job-2", SourceID: "source-2"}, nil
				default:
					return nil, nil
				}
			},
			executeJob: func(context.Context, syncJob) error {
				execCalls++
				if execCalls == 1 {
					return errors.New("boom")
				}
				return nil
			},
		},
	}

	if err := svc.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce() unexpected error: %v", err)
	}
	if execCalls != 2 {
		t.Fatalf("executeJob calls = %d, want 2", execCalls)
	}
}

func TestFailJob_FirstFailureSchedulesRetry(t *testing.T) {
	var (
		scheduled    bool
		finishCalled bool
		gotNextRunAt time.Time
		gotCode      string
		gotDetail    string
	)

	svc := &pullerService{
		runtime: pullerRuntimeConfig{
			JobMaxRetries:     3,
			JobRetryBaseDelay: 2 * time.Second,
		},
		deps: &pullerServiceDeps{
			scheduleJobRetry: func(ctx context.Context, job syncJob, errorCode, errorDetail string, nextRunAt time.Time) error {
				scheduled = true
				gotNextRunAt = nextRunAt
				gotCode = errorCode
				gotDetail = errorDetail
				return nil
			},
			finishJobStatus: func(ctx context.Context, job syncJob, status, errorCode, errorDetail string) error {
				finishCalled = true
				return nil
			},
		},
	}

	job := syncJob{
		ID:       "job-retry-1",
		SourceID: "source-1",
		Attempt:  1,
	}
	start := time.Now().UTC()
	originalErr := errors.New("temporary ingest failure")
	err := svc.failJob(job, errCodeIngestFailed, originalErr)
	if !errors.Is(err, originalErr) {
		t.Fatalf("failJob() error = %v, want original error", err)
	}
	if !scheduled {
		t.Fatalf("scheduleJobRetry() was not called")
	}
	if finishCalled {
		t.Fatalf("finishJobStatus() should not be called when scheduling retry")
	}
	if gotCode != errCodeIngestFailed {
		t.Fatalf("scheduleJobRetry errorCode = %q, want %q", gotCode, errCodeIngestFailed)
	}
	if gotDetail != originalErr.Error() {
		t.Fatalf("scheduleJobRetry errorDetail = %q, want %q", gotDetail, originalErr.Error())
	}
	if gotNextRunAt.Before(start.Add(2 * time.Second)) {
		t.Fatalf("nextRunAt = %s, want >= %s", gotNextRunAt, start.Add(2*time.Second))
	}
}

func TestFailJob_OverMaxRetriesMarksFailed(t *testing.T) {
	var (
		scheduled    bool
		finishCalled bool
		gotStatus    string
		gotCode      string
	)

	svc := &pullerService{
		runtime: pullerRuntimeConfig{
			JobMaxRetries:     3,
			JobRetryBaseDelay: 2 * time.Second,
		},
		deps: &pullerServiceDeps{
			scheduleJobRetry: func(ctx context.Context, job syncJob, errorCode, errorDetail string, nextRunAt time.Time) error {
				scheduled = true
				return nil
			},
			finishJobStatus: func(ctx context.Context, job syncJob, status, errorCode, errorDetail string) error {
				finishCalled = true
				gotStatus = status
				gotCode = errorCode
				return nil
			},
		},
	}

	originalErr := errors.New("still failing")
	err := svc.failJob(syncJob{
		ID:       "job-retry-2",
		SourceID: "source-1",
		Attempt:  4,
	}, errCodeIngestFailed, originalErr)
	if !errors.Is(err, originalErr) {
		t.Fatalf("failJob() error = %v, want original error", err)
	}
	if scheduled {
		t.Fatalf("scheduleJobRetry() should not be called when attempt exceeds max retries")
	}
	if !finishCalled {
		t.Fatalf("finishJobStatus() was not called")
	}
	if gotStatus != "failed" || gotCode != errCodeIngestFailed {
		t.Fatalf("finishJobStatus = (%q, %q), want (failed, %q)", gotStatus, gotCode, errCodeIngestFailed)
	}
}

func TestFailJob_NonRetryableErrorDoesNotRetry(t *testing.T) {
	var (
		scheduled    bool
		finishCalled bool
		gotStatus    string
		gotCode      string
	)

	svc := &pullerService{
		runtime: pullerRuntimeConfig{
			JobMaxRetries:     3,
			JobRetryBaseDelay: 2 * time.Second,
		},
		deps: &pullerServiceDeps{
			scheduleJobRetry: func(ctx context.Context, job syncJob, errorCode, errorDetail string, nextRunAt time.Time) error {
				scheduled = true
				return nil
			},
			finishJobStatus: func(ctx context.Context, job syncJob, status, errorCode, errorDetail string) error {
				finishCalled = true
				gotStatus = status
				gotCode = errorCode
				return nil
			},
		},
	}

	originalErr := errors.New("parse failed permanently")
	err := svc.failJob(syncJob{
		ID:       "job-retry-3",
		SourceID: "source-1",
		Attempt:  1,
	}, errCodeParseFailed, originalErr)
	if !errors.Is(err, originalErr) {
		t.Fatalf("failJob() error = %v, want original error", err)
	}
	if scheduled {
		t.Fatalf("scheduleJobRetry() should not be called for non-retryable error")
	}
	if !finishCalled {
		t.Fatalf("finishJobStatus() was not called")
	}
	if gotStatus != "failed" || gotCode != errCodeParseFailed {
		t.Fatalf("finishJobStatus = (%q, %q), want (failed, %q)", gotStatus, gotCode, errCodeParseFailed)
	}
}

func TestExecuteJob_ParseFailurePersistError_DoesNotFailJob(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0}`))
	}))
	defer server.Close()

	finishStatus := ""
	svc := &pullerService{
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		httpClient: newHTTPClient(),
		runtime: pullerRuntimeConfig{
			JobTimeout:     time.Second,
			IngestTimeout:  time.Second,
			IngestEndpoint: server.URL,
			AgentID:        "puller-test",
		},
		hostname: "local-host",
		connectors: newConnectorRegistry(serviceTestConnector{
			outputs: map[string]parserOutput{
				parserKeyJSONL: {
					ParserKey: parserKeyJSONL,
					MaxLine:   2,
					Events: []rawEventWithLine{
						{
							LineNo: 2,
							Event: ingest.RawEvent{
								SessionID: "session-1",
								EventType: "message",
							},
						},
					},
					Failures: []parseFailure{
						{
							ParserKey:    parserKeyJSONL,
							SourcePath:   "/tmp/chat.log",
							SourceOffset: 1,
							Error:        "invalid json",
						},
					},
				},
				parserKeyNative: {
					ParserKey: parserKeyNative,
					MaxLine:   0,
				},
			},
		}),
		deps: &pullerServiceDeps{
			isCancelRequested: func(context.Context, string) (bool, error) {
				return false, nil
			},
			loadSource: func(context.Context, string) (sourceRecord, error) {
				return sourceRecord{
					ID:       "source-local-1",
					Type:     "local",
					Location: "/tmp/chat.log",
					Enabled:  true,
				}, nil
			},
			fetchLocalSourceContents: func(context.Context, sourceRecord) ([]sourceContent, error) {
				return []sourceContent{
					{
						SourcePath: "/tmp/chat.log",
						HostKey:    "local:/tmp/chat.log",
						Content:    []byte("hello\n"),
					},
				}, nil
			},
			getWatermark: func(context.Context, string, string, string) (int64, error) {
				return 0, nil
			},
			upsertWatermark: func(context.Context, string, string, string, int64) error {
				return nil
			},
			insertParseFailures: func(context.Context, string, string, []parseFailure) error {
				return errors.New("parse failure table unavailable")
			},
			finishJobStatus: func(ctx context.Context, job syncJob, status, errorCode, errorDetail string) error {
				finishStatus = status
				return nil
			},
		},
	}

	err := svc.executeJob(context.Background(), syncJob{
		ID:       "job-local-parse-failure",
		SourceID: "source-local-1",
		Attempt:  1,
	})
	if err != nil {
		t.Fatalf("executeJob(local) unexpected error: %v", err)
	}
	if finishStatus != "success" {
		t.Fatalf("finish status = %q, want success", finishStatus)
	}
}

func TestHandleInternalSourceRoutes_SyncNow(t *testing.T) {
	svc := &pullerService{
		runtime: pullerRuntimeConfig{
			InternalToken: "token-123",
		},
		deps: &pullerServiceDeps{
			createManualSyncJob: func(context.Context, string, string) (syncJob, error) {
				return syncJob{
					ID:       "job-1",
					SourceID: "source-1",
					Attempt:  1,
				}, nil
			},
			executeJob: func(context.Context, syncJob) error {
				return nil
			},
			loadSyncJobResult: func(context.Context, string) (syncJobResult, error) {
				return syncJobResult{
					JobID:    "job-1",
					SourceID: "source-1",
					Status:   "success",
					Attempt:  1,
				}, nil
			},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/sources/source-1/sync-now", nil)
	req.Header.Set("Authorization", "Bearer token-123")
	rr := httptest.NewRecorder()
	svc.handleInternalSourceRoutes(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200", rr.Code)
	}

	var result syncJobResult
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if result.JobID != "job-1" || result.SourceID != "source-1" || result.Status != "success" {
		t.Fatalf("response = %#v, want success job result", result)
	}
}

func TestHandleInternalSourceRoutes_Unauthorized(t *testing.T) {
	svc := &pullerService{
		runtime: pullerRuntimeConfig{
			InternalToken: "token-123",
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/sources/source-1/sync-now", nil)
	rr := httptest.NewRecorder()
	svc.handleInternalSourceRoutes(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status code = %d, want 401", rr.Code)
	}
}
