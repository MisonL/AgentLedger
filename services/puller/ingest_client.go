package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/agentledger/agentledger/services/internal/shared/ingest"
)

var errResidencyPolicyViolation = errors.New("residency policy violation")

func normalizeRegionCode(value string) string {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	if trimmed == "" {
		return ""
	}
	return strings.ReplaceAll(trimmed, "_", "-")
}

func toMetadataString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case []byte:
		return strings.TrimSpace(string(typed))
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	}
}

func firstSourceMetadataString(source sourceRecord, keys ...string) string {
	if len(source.Metadata) == 0 || len(keys) == 0 {
		return ""
	}

	loweredMetadata := make(map[string]any, len(source.Metadata))
	for key, value := range source.Metadata {
		loweredMetadata[strings.ToLower(strings.TrimSpace(key))] = value
	}

	for _, key := range keys {
		normalizedKey := strings.TrimSpace(key)
		if normalizedKey == "" {
			continue
		}
		if value, ok := source.Metadata[normalizedKey]; ok {
			if normalized := toMetadataString(value); normalized != "" {
				return normalized
			}
		}
		if value, ok := loweredMetadata[strings.ToLower(normalizedKey)]; ok {
			if normalized := toMetadataString(value); normalized != "" {
				return normalized
			}
		}
	}
	return ""
}

func resolveSourceResidencyRegion(source sourceRecord) string {
	return normalizeRegionCode(
		firstSourceMetadataString(
			source,
			"source_region",
			"sourceRegion",
			"residency_region",
			"residencyRegion",
			"region",
		),
	)
}

func extractRuleHitMetadata(source sourceRecord) map[string]string {
	ruleMetadata := map[string]string{}
	if ruleAssetID := firstSourceMetadataString(source, "rule_asset_id", "ruleAssetId"); ruleAssetID != "" {
		ruleMetadata["rule_asset_id"] = ruleAssetID
	}
	if ruleAssetVersion := firstSourceMetadataString(source, "rule_asset_version", "ruleAssetVersion"); ruleAssetVersion != "" {
		ruleMetadata["rule_asset_version"] = ruleAssetVersion
	}
	if ruleID := firstSourceMetadataString(source, "rule_id", "ruleId"); ruleID != "" {
		ruleMetadata["rule_id"] = ruleID
	}
	if ruleMatchReason := firstSourceMetadataString(source, "rule_match_reason", "ruleMatchReason"); ruleMatchReason != "" {
		ruleMetadata["rule_match_reason"] = ruleMatchReason
	}
	return ruleMetadata
}

func buildResidencyPolicyViolationError(source sourceRecord, sourceRegion, targetRegion string) error {
	return fmt.Errorf(
		"%w: source_id=%s source_region=%s target_region=%s",
		errResidencyPolicyViolation,
		source.ID,
		sourceRegion,
		targetRegion,
	)
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func enrichEventsWithMetadata(events []ingest.RawEvent, extra map[string]string) []ingest.RawEvent {
	if len(events) == 0 || len(extra) == 0 {
		return events
	}

	enriched := make([]ingest.RawEvent, 0, len(events))
	for _, event := range events {
		nextEvent := event
		nextMetadata := cloneStringMap(event.Metadata)
		for key, value := range extra {
			nextMetadata[key] = value
		}
		nextEvent.Metadata = nextMetadata
		enriched = append(enriched, nextEvent)
	}
	return enriched
}

func (s *pullerService) pushEvents(ctx context.Context, source sourceRecord, job syncJob, events []ingest.RawEvent) error {
	if len(events) == 0 {
		return nil
	}

	provider := strings.TrimSpace(source.Provider)
	if provider == "" {
		provider = firstNonEmpty(strings.TrimSpace(source.Type), "unknown")
	}

	sourceType := strings.TrimSpace(source.Type)
	if sourceType == "" {
		sourceType = "local"
	}

	targetRegion := normalizeRegionCode(s.runtime.ResidencyTargetRegion)
	sourceRegion := resolveSourceResidencyRegion(source)
	residencyDecision := "allow"
	residencyMode := "disabled"
	if targetRegion != "" {
		residencyMode = "enforce"
		if sourceRegion != "" && sourceRegion != targetRegion {
			return buildResidencyPolicyViolationError(source, sourceRegion, targetRegion)
		}
		if sourceRegion == "" {
			residencyDecision = "allow_unknown_source_region"
		}
	}

	ruleMetadata := extractRuleHitMetadata(source)
	batchGovernanceMetadata := map[string]string{
		"sync_job_id":        job.ID,
		"puller":             "true",
		"residency_decision": residencyDecision,
		"residency_mode":     residencyMode,
	}
	if targetRegion != "" {
		batchGovernanceMetadata["residency_target_region"] = targetRegion
	}
	if sourceRegion != "" {
		batchGovernanceMetadata["source_region"] = sourceRegion
	}
	if len(ruleMetadata) > 0 {
		batchGovernanceMetadata["rule_asset_hit_count"] = "1"
		for key, value := range ruleMetadata {
			batchGovernanceMetadata[key] = value
		}
	}

	eventGovernanceMetadata := map[string]string{
		"residency_decision": residencyDecision,
	}
	if targetRegion != "" {
		eventGovernanceMetadata["residency_target_region"] = targetRegion
	}
	if sourceRegion != "" {
		eventGovernanceMetadata["source_region"] = sourceRegion
	}
	for key, value := range ruleMetadata {
		eventGovernanceMetadata[key] = value
	}

	chunkSize := ingest.MaxBatchEvents
	for i := 0; i < len(events); i += chunkSize {
		end := i + chunkSize
		if end > len(events) {
			end = len(events)
		}
		chunkEvents := enrichEventsWithMetadata(events[i:end], eventGovernanceMetadata)

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
			Events:   chunkEvents,
			Metadata: cloneStringMap(batchGovernanceMetadata),
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
