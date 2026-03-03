package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/agentledger/agentledger/services/internal/shared/ingest"
)

func (s *pullerService) pushEvents(ctx context.Context, source sourceRecord, job syncJob, events []ingest.RawEvent) error {
	if len(events) == 0 {
		return nil
	}

	provider := strings.TrimSpace(source.Provider)
	if provider == "" {
		provider = firstNonEmpty(strings.TrimSpace(source.Type), "ssh")
	}

	sourceType := strings.TrimSpace(source.Type)
	if sourceType == "" {
		sourceType = "ssh"
	}

	chunkSize := ingest.MaxBatchEvents
	for i := 0; i < len(events); i += chunkSize {
		end := i + chunkSize
		if end > len(events) {
			end = len(events)
		}

		batch := ingest.IngestBatch{
			BatchID: stableID("batch", job.ID, fmt.Sprintf("%d", i), fmt.Sprintf("%d", time.Now().UTC().UnixNano())),
			Agent: ingest.AgentInfo{
				AgentID:     s.runtime.AgentID,
				TenantID:    strings.TrimSpace(source.TenantID),
				WorkspaceID: strings.TrimSpace(source.WorkspaceID),
				Hostname:    s.hostname,
				Version:     "puller-v1",
			},
			Source: ingest.SourceInfo{
				SourceID:   source.ID,
				Provider:   provider,
				SourceType: sourceType,
			},
			Events: events[i:end],
			Metadata: map[string]string{
				"sync_job_id": job.ID,
				"puller":      "true",
			},
		}
		ingest.NormalizeBatch(&batch, time.Now().UTC())

		if err := s.postIngestBatch(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

func (s *pullerService) postIngestBatch(ctx context.Context, batch ingest.IngestBatch) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal ingest batch failed: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, s.runtime.IngestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, s.runtime.IngestEndpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build ingest request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(s.runtime.IngestBearer); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send ingest request failed: %w", err)
	}
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	responseText := strings.TrimSpace(string(responseBody))

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if responseText == "" {
			responseText = "empty response body"
		}
		return fmt.Errorf("ingest endpoint returned %d: %s", resp.StatusCode, responseText)
	}

	var parsed struct {
		Accepted int `json:"accepted"`
		Rejected int `json:"rejected"`
	}
	if err := json.Unmarshal(responseBody, &parsed); err != nil {
		return fmt.Errorf("decode ingest response failed: %w", err)
	}

	expectedAccepted := len(batch.Events)
	if parsed.Accepted != expectedAccepted || parsed.Rejected != 0 {
		return fmt.Errorf(
			"ingest response mismatch: accepted=%d rejected=%d expected_accepted=%d",
			parsed.Accepted,
			parsed.Rejected,
			expectedAccepted,
		)
	}

	return nil
}
