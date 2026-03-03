package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agentledger/agentledger/services/internal/shared/ingest"
)

const parserCancelCheckEvery = 100

var (
	nativeRolePattern = regexp.MustCompile(`(?i)^\s*(user|assistant|system|tool)\s*[:：]\s*(.+)$`)
	timePrefixPattern = regexp.MustCompile(`^\s*(\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)\s+(.+)$`)
)

type cancelChecker func(ctx context.Context) (bool, error)

type parseInput struct {
	Source      sourceRecord
	SourcePath  string
	Lines       []lineRecord
	JSONLStart  int64
	NativeStart int64
	CheckCancel cancelChecker
}

func splitLines(content []byte) ([]lineRecord, error) {
	scanner := bufio.NewScanner(bytes.NewReader(content))
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	lines := make([]lineRecord, 0)
	var lineNo int64 = 0
	for scanner.Scan() {
		lineNo++
		lines = append(lines, lineRecord{No: lineNo, Text: scanner.Text()})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan lines failed: %w", err)
	}
	return lines, nil
}

func parseLinesConcurrently(ctx context.Context, input parseInput) (map[string]parserOutput, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	outputs := make(map[string]parserOutput, 2)
	var mu sync.Mutex
	var once sync.Once
	var parseErr error

	setErr := func(err error) {
		once.Do(func() {
			parseErr = err
			cancel()
		})
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		out, err := parseJSONLLines(ctx, input)
		if err != nil {
			setErr(err)
			return
		}
		mu.Lock()
		outputs[parserKeyJSONL] = out
		mu.Unlock()
	}()

	go func() {
		defer wg.Done()
		out, err := parseNativeLines(ctx, input)
		if err != nil {
			setErr(err)
			return
		}
		mu.Lock()
		outputs[parserKeyNative] = out
		mu.Unlock()
	}()

	wg.Wait()
	if parseErr != nil {
		return nil, parseErr
	}
	return outputs, nil
}

func parseJSONLLines(ctx context.Context, input parseInput) (parserOutput, error) {
	out := parserOutput{ParserKey: parserKeyJSONL, MaxLine: input.JSONLStart}
	checkCounter := 0

	for _, line := range input.Lines {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if line.No <= input.JSONLStart {
			continue
		}

		trimmed := strings.TrimSpace(line.Text)
		if trimmed == "" {
			continue
		}

		checkCounter++
		if checkCounter >= parserCancelCheckEvery {
			checkCounter = 0
			if canceled, err := checkCancelled(ctx, input.CheckCancel); err != nil {
				return out, err
			} else if canceled {
				return out, errJobCancelled
			}
		}

		var payload any
		if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
			out.Failures = append(out.Failures, parseFailure{
				ParserKey:    parserKeyJSONL,
				SourcePath:   input.SourcePath,
				SourceOffset: line.No,
				Error:        err.Error(),
			})
			continue
		}

		data, ok := payload.(map[string]any)
		if !ok {
			data = map[string]any{"value": payload}
		}

		rawPayload, err := json.Marshal(data)
		if err != nil {
			return out, fmt.Errorf("marshal json line payload failed: %w", err)
		}

		event := ingest.RawEvent{}
		event.EventID = firstNonEmpty(
			valueFromMap(data, "event_id", "eventId", "id"),
			stableID("evt", input.Source.ID, parserKeyJSONL, fmt.Sprintf("%d", line.No), trimmed),
		)

		sessionID := extractSessionIDFromJSON(data)
		occurredAtRaw := firstNonEmpty(valueFromMap(data, "occurred_at", "occurredAt", "timestamp", "time", "created_at"))
		occurredAt, dateKey := normalizeOccurredAt(occurredAtRaw, time.Now().UTC())
		if sessionID == "" {
			sessionID = stableID("sess", input.Source.ID, parserKeyJSONL, dateKey)
		}

		event.SessionID = sessionID
		event.EventType = firstNonEmpty(valueFromMap(data, "event_type", "eventType", "type"), "message")
		event.Role = firstNonEmpty(
			valueFromMap(data, "role"),
			extractStringByPaths(data,
				[]string{"author", "role"},
				[]string{"message", "role"},
			),
		)
		event.Text = firstNonEmpty(
			valueFromMap(data, "text", "message", "content"),
			extractStringByPaths(data,
				[]string{"message", "text"},
				[]string{"message", "content", "text"},
				[]string{"content", "text"},
				[]string{"content", "0", "text"},
				[]string{"message", "content", "0", "text"},
				[]string{"candidates", "0", "content", "parts", "0", "text"},
			),
			extractTextFromContent(pathValue(data, "content")),
			extractTextFromContent(pathValue(data, "message", "content")),
			extractTextFromContent(pathValue(data, "candidates", "0", "content", "parts")),
		)
		event.Model = firstNonEmpty(
			valueFromMap(data, "model"),
			extractStringByPaths(data,
				[]string{"model_name"},
				[]string{"modelName"},
				[]string{"usage", "model"},
				[]string{"metadata", "model"},
			),
		)
		event.OccurredAt = occurredAt
		event.Tokens = extractTokenUsageFromPayload(data)
		event.CostUSD = extractFloatByPaths(data,
			[]string{"cost_usd"},
			[]string{"costUsd"},
			[]string{"cost"},
			[]string{"total_cost"},
			[]string{"estimated_cost"},
			[]string{"usage", "cost"},
			[]string{"usage", "cost_usd"},
			[]string{"usage", "total_cost"},
		)
		event.CostMode = firstNonEmpty(
			valueFromMap(data, "cost_mode", "costMode"),
			extractStringByPaths(data, []string{"usage", "cost_mode"}),
			"reported",
		)
		event.SourcePath = input.SourcePath
		event.SourceOffset = int64Ptr(line.No)
		event.Metadata = map[string]string{
			"parser": parserKeyJSONL,
			"line":   fmt.Sprintf("%d", line.No),
		}
		event.Payload = rawPayload

		out.Events = append(out.Events, rawEventWithLine{LineNo: line.No, Event: event})
		if line.No > out.MaxLine {
			out.MaxLine = line.No
		}
	}

	return out, nil
}

func parseNativeLines(ctx context.Context, input parseInput) (parserOutput, error) {
	out := parserOutput{ParserKey: parserKeyNative, MaxLine: input.NativeStart}
	checkCounter := 0

	for _, line := range input.Lines {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if line.No <= input.NativeStart {
			continue
		}

		trimmed := strings.TrimSpace(line.Text)
		if trimmed == "" {
			continue
		}
		if json.Valid([]byte(trimmed)) {
			continue
		}

		checkCounter++
		if checkCounter >= parserCancelCheckEvery {
			checkCounter = 0
			if canceled, err := checkCancelled(ctx, input.CheckCancel); err != nil {
				return out, err
			} else if canceled {
				return out, errJobCancelled
			}
		}

		role, text, occurredAt, dateKey := parseNativeLine(trimmed, time.Now().UTC())
		eventID := stableID("evt", input.Source.ID, parserKeyNative, fmt.Sprintf("%d", line.No), trimmed)
		sessionID := stableID("sess", input.Source.ID, parserKeyNative, dateKey)
		payload, err := json.Marshal(map[string]any{
			"raw_line": trimmed,
			"parser":   parserKeyNative,
		})
		if err != nil {
			return out, fmt.Errorf("marshal native payload failed: %w", err)
		}

		event := ingest.RawEvent{
			EventID:      eventID,
			SessionID:    sessionID,
			EventType:    "message",
			Role:         role,
			Text:         text,
			OccurredAt:   occurredAt,
			SourcePath:   input.SourcePath,
			SourceOffset: int64Ptr(line.No),
			Metadata: map[string]string{
				"parser": parserKeyNative,
				"line":   fmt.Sprintf("%d", line.No),
			},
			Payload: payload,
		}

		out.Events = append(out.Events, rawEventWithLine{LineNo: line.No, Event: event})
		if line.No > out.MaxLine {
			out.MaxLine = line.No
		}
	}

	return out, nil
}

func collectAndSortEvents(outputs map[string]parserOutput) []ingest.RawEvent {
	combined := make([]rawEventWithLine, 0)
	for _, output := range outputs {
		combined = append(combined, output.Events...)
	}

	sort.Slice(combined, func(i, j int) bool {
		if combined[i].LineNo == combined[j].LineNo {
			return combined[i].Event.EventID < combined[j].Event.EventID
		}
		return combined[i].LineNo < combined[j].LineNo
	})

	out := make([]ingest.RawEvent, 0, len(combined))
	for _, item := range combined {
		out = append(out, item.Event)
	}
	return out
}

func collectAndSortParseFailures(outputs map[string]parserOutput) []parseFailure {
	combined := make([]parseFailure, 0)
	for _, output := range outputs {
		combined = append(combined, output.Failures...)
	}

	sort.Slice(combined, func(i, j int) bool {
		if combined[i].SourceOffset == combined[j].SourceOffset {
			if combined[i].ParserKey == combined[j].ParserKey {
				return combined[i].Error < combined[j].Error
			}
			return combined[i].ParserKey < combined[j].ParserKey
		}
		return combined[i].SourceOffset < combined[j].SourceOffset
	})

	return combined
}

func checkCancelled(ctx context.Context, checker cancelChecker) (bool, error) {
	if checker == nil {
		return false, nil
	}
	canceled, err := checker(ctx)
	if err != nil {
		return false, fmt.Errorf("check cancel_requested failed: %w", err)
	}
	return canceled, nil
}

func parseNativeLine(line string, now time.Time) (role, text, occurredAt, dateKey string) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		date := now.Format("2006-01-02")
		return "assistant", "", now.Format(time.RFC3339Nano), date
	}

	parsedTime := now.UTC()
	remaining := trimmed

	if matches := timePrefixPattern.FindStringSubmatch(trimmed); len(matches) == 3 {
		if parsed, err := ingest.ParseTimestamp(matches[1]); err == nil {
			parsedTime = parsed
			remaining = strings.TrimSpace(matches[2])
		}
	}

	resolvedRole := "assistant"
	resolvedText := remaining
	if matches := nativeRolePattern.FindStringSubmatch(remaining); len(matches) == 3 {
		resolvedRole = strings.ToLower(strings.TrimSpace(matches[1]))
		resolvedText = strings.TrimSpace(matches[2])
	}
	if resolvedText == "" {
		resolvedText = trimmed
	}

	occurred := parsedTime.UTC().Format(time.RFC3339Nano)
	dateKey = parsedTime.UTC().Format("2006-01-02")
	return resolvedRole, resolvedText, occurred, dateKey
}

func extractSessionIDFromJSON(payload map[string]any) string {
	if sessionID := firstNonEmpty(valueFromMap(payload, "session_id", "sessionId", "sessionID")); sessionID != "" {
		return sessionID
	}

	rawSession, ok := payload["session"]
	if !ok {
		return ""
	}

	switch typed := rawSession.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]any:
		return firstNonEmpty(valueFromMap(typed, "id", "session_id", "sessionId"))
	default:
		return ""
	}
}

func normalizeOccurredAt(raw string, fallback time.Time) (occurredAt string, dateKey string) {
	trimmed := strings.TrimSpace(raw)
	if trimmed != "" {
		if parsed, err := ingest.ParseTimestamp(trimmed); err == nil {
			utc := parsed.UTC()
			return utc.Format(time.RFC3339Nano), utc.Format("2006-01-02")
		}
	}

	utc := fallback.UTC()
	return utc.Format(time.RFC3339Nano), utc.Format("2006-01-02")
}

func valueFromMap(input map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := input[key]
		if !ok {
			continue
		}
		if text := anyToString(value); text != "" {
			return text
		}
	}
	return ""
}

func anyToString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	case float64:
		return strings.TrimSpace(fmt.Sprintf("%.0f", typed))
	case float32:
		return strings.TrimSpace(fmt.Sprintf("%.0f", typed))
	case int, int8, int16, int32, int64:
		return strings.TrimSpace(fmt.Sprintf("%d", typed))
	case uint, uint8, uint16, uint32, uint64:
		return strings.TrimSpace(fmt.Sprintf("%d", typed))
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func stableID(prefix string, parts ...string) string {
	joined := strings.Join(parts, "|")
	sum := sha256.Sum256([]byte(joined))
	return prefix + "_" + hex.EncodeToString(sum[:12])
}

func pathValue(root any, path ...string) any {
	current := root
	for _, segment := range path {
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[segment]
			if !ok {
				return nil
			}
			current = next
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(typed) {
				return nil
			}
			current = typed[index]
		default:
			return nil
		}
	}
	return current
}

func extractStringByPaths(payload map[string]any, paths ...[]string) string {
	for _, path := range paths {
		if len(path) == 0 {
			continue
		}
		if text := anyToString(pathValue(payload, path...)); text != "" {
			return text
		}
	}
	return ""
}

func extractTextFromContent(value any) string {
	parts := collectTextParts(value)
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "\n")
}

func collectTextParts(value any) []string {
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return nil
		}
		return []string{trimmed}
	case []any:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			result = append(result, collectTextParts(item)...)
		}
		return result
	case map[string]any:
		if text := anyToString(typed["text"]); text != "" {
			return []string{text}
		}
		if nested := collectTextParts(typed["content"]); len(nested) > 0 {
			return nested
		}
		if nested := collectTextParts(typed["parts"]); len(nested) > 0 {
			return nested
		}
		return nil
	default:
		if text := anyToString(typed); text != "" {
			return []string{text}
		}
		return nil
	}
}

func extractTokenUsageFromPayload(payload map[string]any) ingest.TokenUsage {
	usage := ingest.TokenUsage{
		InputTokens: firstInt64ByPaths(payload,
			[]string{"input_tokens"},
			[]string{"inputTokens"},
			[]string{"prompt_tokens"},
			[]string{"promptTokenCount"},
			[]string{"usage", "input_tokens"},
			[]string{"usage", "inputTokens"},
			[]string{"usage", "prompt_tokens"},
			[]string{"usage", "promptTokenCount"},
			[]string{"usageMetadata", "promptTokenCount"},
			[]string{"usage_metadata", "prompt_token_count"},
		),
		OutputTokens: firstInt64ByPaths(payload,
			[]string{"output_tokens"},
			[]string{"outputTokens"},
			[]string{"completion_tokens"},
			[]string{"candidatesTokenCount"},
			[]string{"usage", "output_tokens"},
			[]string{"usage", "outputTokens"},
			[]string{"usage", "completion_tokens"},
			[]string{"usage", "candidatesTokenCount"},
			[]string{"usageMetadata", "candidatesTokenCount"},
			[]string{"usage_metadata", "candidates_token_count"},
		),
		CacheReadTokens: firstInt64ByPaths(payload,
			[]string{"cache_read_tokens"},
			[]string{"cacheReadTokens"},
			[]string{"cachedContentTokenCount"},
			[]string{"usage", "cache_read_tokens"},
			[]string{"usage", "cacheReadTokens"},
			[]string{"usageMetadata", "cachedContentTokenCount"},
			[]string{"usage_metadata", "cached_content_token_count"},
		),
		CacheWriteTokens: firstInt64ByPaths(payload,
			[]string{"cache_write_tokens"},
			[]string{"cacheWriteTokens"},
			[]string{"usage", "cache_write_tokens"},
			[]string{"usage", "cacheWriteTokens"},
		),
		ReasoningTokens: firstInt64ByPaths(payload,
			[]string{"reasoning_tokens"},
			[]string{"reasoningTokens"},
			[]string{"usage", "reasoning_tokens"},
			[]string{"usage", "reasoningTokens"},
			[]string{"usageMetadata", "thoughtsTokenCount"},
			[]string{"usage_metadata", "thoughts_token_count"},
		),
	}
	return usage
}

func firstInt64ByPaths(payload map[string]any, paths ...[]string) int64 {
	for _, path := range paths {
		if len(path) == 0 {
			continue
		}
		if value, ok := anyToInt64(pathValue(payload, path...)); ok {
			return value
		}
	}
	return 0
}

func extractFloatByPaths(payload map[string]any, paths ...[]string) *float64 {
	for _, path := range paths {
		if len(path) == 0 {
			continue
		}
		if value, ok := anyToFloat64(pathValue(payload, path...)); ok {
			return &value
		}
	}
	return nil
}

func anyToInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case int:
		return int64(typed), true
	case int8:
		return int64(typed), true
	case int16:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	case uint:
		return int64(typed), true
	case uint8:
		return int64(typed), true
	case uint16:
		return int64(typed), true
	case uint32:
		return int64(typed), true
	case uint64:
		if typed > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(typed), true
	case float32:
		return int64(typed), true
	case float64:
		return int64(typed), true
	case json.Number:
		if parsed, err := typed.Int64(); err == nil {
			return parsed, true
		}
		if parsed, err := typed.Float64(); err == nil {
			return int64(parsed), true
		}
		return 0, false
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, false
		}
		if parsed, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return parsed, true
		}
		if parsed, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return int64(parsed), true
		}
		return 0, false
	default:
		return 0, false
	}
}

func anyToFloat64(value any) (float64, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		return parsed, true
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, false
		}
		parsed, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}
