package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	collectToolAuto   = "auto"
	collectToolCodex  = "codex"
	collectToolClaude = "claude"
	collectToolGemini = "gemini"

	defaultCollectCodexDir  = "~/.codex/sessions"
	defaultCollectClaudeDir = "~/.claude/projects"
	defaultCollectGeminiDir = "~/.gemini/tmp"

	collectScannerMaxTokenBytes = 4 * 1024 * 1024
	collectMaxFileSizeBytes     = 20 * 1024 * 1024
)

var collectRolePattern = regexp.MustCompile(`(?i)^\s*(user|assistant|system|tool)\s*[:：]\s*(.+)$`)

type collectSource struct {
	Tool string
	Dir  string
}

type collectFile struct {
	Tool string
	Path string
}

func collectCommand(args []string) int {
	fs := flag.NewFlagSet("collect", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	tool := fs.String("tool", collectToolAuto, "采集工具：auto|codex|claude|gemini")
	dir := fs.String("dir", "", "采集目录或文件（覆盖默认路径）")
	output := fs.String("output", "", "输出 JSONL 文件（默认 stdout）")
	maxEvents := fs.Int("max-events", 0, "最多输出事件数（<=0 表示不限制）")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *maxEvents < 0 {
		fmt.Fprintln(os.Stderr, "max-events 不能为负数")
		return 2
	}

	sources, err := resolveCollectSources(*tool, *dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析采集参数失败: %v\n", err)
		return 2
	}

	files, err := collectSourceFiles(sources, strings.TrimSpace(*dir) != "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "扫描采集文件失败: %v\n", err)
		return 1
	}
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "没有可采集的文件")
		return 1
	}

	events, err := collectEvents(files, *maxEvents)
	if err != nil {
		fmt.Fprintf(os.Stderr, "采集事件失败: %v\n", err)
		return 1
	}
	if len(events) == 0 {
		fmt.Fprintln(os.Stderr, "没有可输出事件")
		return 1
	}

	writer, outputPath, err := openCollectWriter(*output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "打开输出失败: %v\n", err)
		return 1
	}
	defer writer.Close()

	if err := writeCollectEventsJSONL(writer, events); err != nil {
		fmt.Fprintf(os.Stderr, "写入输出失败: %v\n", err)
		return 1
	}

	if outputPath != "" {
		fmt.Fprintf(os.Stderr, "collect 完成: files=%d events=%d output=%s\n", len(files), len(events), outputPath)
	}
	return 0
}

func resolveCollectSources(tool, overrideDir string) ([]collectSource, error) {
	normalizedTool, err := normalizeCollectTool(tool)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(overrideDir) != "" {
		resolved, err := resolveCollectPath(overrideDir)
		if err != nil {
			return nil, err
		}
		return []collectSource{{Tool: normalizedTool, Dir: resolved}}, nil
	}

	if normalizedTool != collectToolAuto {
		dir, err := resolveCollectPath(defaultCollectDirForTool(normalizedTool))
		if err != nil {
			return nil, err
		}
		return []collectSource{{Tool: normalizedTool, Dir: dir}}, nil
	}

	defaultSources := []struct {
		tool string
		dir  string
	}{
		{tool: collectToolCodex, dir: defaultCollectCodexDir},
		{tool: collectToolClaude, dir: defaultCollectClaudeDir},
		{tool: collectToolGemini, dir: defaultCollectGeminiDir},
	}

	result := make([]collectSource, 0, len(defaultSources))
	for _, item := range defaultSources {
		resolved, err := resolveCollectPath(item.dir)
		if err != nil {
			return nil, err
		}
		result = append(result, collectSource{Tool: item.tool, Dir: resolved})
	}
	return result, nil
}

func normalizeCollectTool(tool string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(tool))
	switch normalized {
	case collectToolAuto, collectToolCodex, collectToolClaude, collectToolGemini:
		return normalized, nil
	default:
		return "", fmt.Errorf("tool 仅支持 auto|codex|claude|gemini")
	}
}

func defaultCollectDirForTool(tool string) string {
	switch tool {
	case collectToolCodex:
		return defaultCollectCodexDir
	case collectToolClaude:
		return defaultCollectClaudeDir
	case collectToolGemini:
		return defaultCollectGeminiDir
	default:
		return ""
	}
}

func resolveCollectPath(raw string) (string, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		return "", fmt.Errorf("路径不能为空")
	}
	expanded, err := expandPath(target)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(expanded)
	if err != nil {
		return "", fmt.Errorf("解析绝对路径失败: %w", err)
	}
	return filepath.Clean(abs), nil
}

func collectSourceFiles(sources []collectSource, strict bool) ([]collectFile, error) {
	files := make([]collectFile, 0)
	for _, source := range sources {
		info, err := os.Stat(source.Dir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && !strict {
				continue
			}
			return nil, fmt.Errorf("访问路径失败(%s): %w", source.Dir, err)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			if strict {
				return nil, fmt.Errorf("路径不是文件或目录: %s", source.Dir)
			}
			continue
		}

		sourceFiles, err := scanCollectFiles(source.Dir)
		if err != nil {
			if strict {
				return nil, err
			}
			continue
		}
		for _, path := range sourceFiles {
			files = append(files, collectFile{
				Tool: source.Tool,
				Path: path,
			})
		}
	}
	return files, nil
}

func scanCollectFiles(root string) ([]string, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("读取采集路径失败: %w", err)
	}

	cleaned := filepath.Clean(root)
	if info.Mode().IsRegular() {
		if !isCollectSupportedFile(cleaned) {
			return nil, fmt.Errorf("不支持的文件类型: %s", cleaned)
		}
		if info.Size() > collectMaxFileSizeBytes {
			return nil, fmt.Errorf("文件过大，已跳过: %s", cleaned)
		}
		return []string{cleaned}, nil
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("采集路径必须是文件或目录: %s", cleaned)
	}

	files := make([]string, 0, 16)
	err = filepath.WalkDir(cleaned, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !entry.Type().IsRegular() {
			return nil
		}

		normalizedPath := filepath.Clean(path)
		if !isCollectSupportedFile(normalizedPath) {
			return nil
		}

		fileInfo, err := entry.Info()
		if err != nil {
			return err
		}
		if fileInfo.Size() > collectMaxFileSizeBytes {
			return nil
		}

		files = append(files, normalizedPath)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("遍历采集目录失败: %w", err)
	}

	sort.Strings(files)
	return files, nil
}

func isCollectSupportedFile(path string) bool {
	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(path)))
	switch ext {
	case "", ".json", ".jsonl", ".log", ".txt", ".md", ".ndjson":
		return true
	default:
		return false
	}
}

func collectEvents(files []collectFile, maxEvents int) ([]agentEvent, error) {
	if maxEvents < 0 {
		return nil, fmt.Errorf("max-events 不能为负数")
	}

	events := make([]agentEvent, 0, 128)
	for _, file := range files {
		tool := inferCollectTool(file.Tool, file.Path)
		if err := collectEventsFromFile(file.Path, tool, maxEvents, &events); err != nil {
			return nil, err
		}
		if maxEvents > 0 && len(events) >= maxEvents {
			break
		}
	}
	return events, nil
}

func collectEventsFromFile(path, tool string, maxEvents int, events *[]agentEvent) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("打开采集文件失败(%s): %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), collectScannerMaxTokenBytes)

	var lineNo int64 = 0
	for scanner.Scan() {
		lineNo++
		event, ok, err := collectLineToEvent(scanner.Text(), path, lineNo, tool, time.Now().UTC())
		if err != nil {
			return fmt.Errorf("解析采集文件失败(%s:%d): %w", path, lineNo, err)
		}
		if !ok {
			continue
		}
		*events = append(*events, event)
		if maxEvents > 0 && len(*events) >= maxEvents {
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("读取采集文件失败(%s): %w", path, err)
	}
	return nil
}

func collectLineToEvent(line, sourcePath string, sourceOffset int64, tool string, now time.Time) (agentEvent, bool, error) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return agentEvent{}, false, nil
	}

	offset := sourceOffset
	event := agentEvent{
		EventID:      collectStableID("evt", tool, sourcePath, strconv.FormatInt(sourceOffset, 10), trimmed),
		SessionID:    collectStableID("session", tool, sourcePath),
		EventType:    "message",
		OccurredAt:   now.UTC().Format(time.RFC3339Nano),
		CostMode:     "reported",
		SourcePath:   sourcePath,
		SourceOffset: &offset,
	}

	if json.Valid([]byte(trimmed)) {
		payload, err := parseCollectPayload(trimmed, tool)
		if err != nil {
			return agentEvent{}, false, err
		}

		if sessionID := extractCollectSessionID(payload); sessionID != "" {
			event.SessionID = sessionID
		}
		event.EventType = firstNonEmpty(lookupCollectString(payload, "event_type", "eventType", "type"), event.EventType)
		event.Role = lookupCollectString(payload, "role")
		event.Text = lookupCollectString(payload, "text", "message", "content")
		event.Model = lookupCollectString(payload, "model", "model_name", "modelName")
		event.OccurredAt = normalizeCollectOccurredAt(lookupCollectAny(payload, "occurred_at", "occurredAt", "timestamp", "time", "created_at"), now)
		event.Tokens = collectTokenUsageFromPayload(payload)
		if cost := extractCollectFloat(payload, "cost_usd", "costUsd", "cost"); cost != nil {
			event.CostUSD = cost
		}
		event.CostMode = firstNonEmpty(lookupCollectString(payload, "cost_mode", "costMode"), event.CostMode)

		rawPayload, err := json.Marshal(payload)
		if err != nil {
			return agentEvent{}, false, fmt.Errorf("序列化 payload 失败: %w", err)
		}
		event.Payload = rawPayload
	} else {
		role, text := parseCollectRoleText(trimmed)
		event.Role = role
		event.Text = text
		rawPayload, err := json.Marshal(map[string]any{
			"parser":         "native",
			"raw_line":       trimmed,
			"collector":      "agent collect",
			"collected_tool": tool,
		})
		if err != nil {
			return agentEvent{}, false, fmt.Errorf("序列化 native payload 失败: %w", err)
		}
		event.Payload = rawPayload
	}

	if strings.TrimSpace(event.Text) == "" {
		event.Text = trimmed
	}
	if strings.TrimSpace(event.Role) == "" {
		if role, _ := parseCollectRoleText(event.Text); role != "" {
			event.Role = role
		}
	}
	if len(event.Payload) == 0 {
		event.Payload = json.RawMessage(`{"collector":"agent collect"}`)
	}

	return event, true, nil
}

func parseCollectPayload(line, tool string) (map[string]any, error) {
	var raw any
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return nil, fmt.Errorf("解析 JSON 失败: %w", err)
	}

	payload, ok := raw.(map[string]any)
	if !ok {
		payload = map[string]any{"value": raw}
	}
	if _, exists := payload["collector"]; !exists {
		payload["collector"] = "agent collect"
	}
	if _, exists := payload["collected_tool"]; !exists {
		payload["collected_tool"] = tool
	}
	return payload, nil
}

func parseCollectRoleText(line string) (string, string) {
	matches := collectRolePattern.FindStringSubmatch(line)
	if len(matches) == 3 {
		return strings.ToLower(strings.TrimSpace(matches[1])), strings.TrimSpace(matches[2])
	}
	return "", strings.TrimSpace(line)
}

func extractCollectSessionID(payload map[string]any) string {
	if sessionID := lookupCollectString(payload, "session_id", "sessionId", "sessionID"); sessionID != "" {
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
		return lookupCollectString(typed, "id", "session_id", "sessionId", "sessionID")
	default:
		return ""
	}
}

func normalizeCollectOccurredAt(value any, fallback time.Time) string {
	parsed, ok := parseCollectTime(value)
	if !ok {
		return fallback.UTC().Format(time.RFC3339Nano)
	}
	return parsed.UTC().Format(time.RFC3339Nano)
}

func parseCollectTime(value any) (time.Time, bool) {
	switch typed := value.(type) {
	case nil:
		return time.Time{}, false
	case string:
		raw := strings.TrimSpace(typed)
		if raw == "" {
			return time.Time{}, false
		}

		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
		}
		for _, layout := range layouts {
			if ts, err := time.Parse(layout, raw); err == nil {
				return ts, true
			}
		}
		if unix, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return parseCollectUnix(unix), true
		}
		return time.Time{}, false
	case float64:
		return parseCollectUnix(int64(typed)), true
	case int64:
		return parseCollectUnix(typed), true
	case int:
		return parseCollectUnix(int64(typed)), true
	default:
		return time.Time{}, false
	}
}

func parseCollectUnix(unix int64) time.Time {
	switch {
	case unix > 9_999_999_999_999_999:
		return time.Unix(0, unix)
	case unix > 9_999_999_999_999:
		return time.UnixMicro(unix)
	case unix > 9_999_999_999:
		return time.UnixMilli(unix)
	default:
		return time.Unix(unix, 0)
	}
}

func collectTokenUsageFromPayload(payload map[string]any) tokenUsage {
	usage := tokenUsage{
		InputTokens:      extractCollectInt64(payload, "input_tokens", "inputTokens", "prompt_tokens", "promptTokens"),
		OutputTokens:     extractCollectInt64(payload, "output_tokens", "outputTokens", "completion_tokens", "completionTokens"),
		CacheReadTokens:  extractCollectInt64(payload, "cache_read_tokens", "cacheReadTokens"),
		CacheWriteTokens: extractCollectInt64(payload, "cache_write_tokens", "cacheWriteTokens"),
		ReasoningTokens:  extractCollectInt64(payload, "reasoning_tokens", "reasoningTokens"),
	}

	rawUsage, ok := payload["usage"].(map[string]any)
	if !ok {
		return usage
	}
	if usage.InputTokens == 0 {
		usage.InputTokens = extractCollectInt64(rawUsage, "input_tokens", "inputTokens", "prompt_tokens", "promptTokens")
	}
	if usage.OutputTokens == 0 {
		usage.OutputTokens = extractCollectInt64(rawUsage, "output_tokens", "outputTokens", "completion_tokens", "completionTokens")
	}
	if usage.CacheReadTokens == 0 {
		usage.CacheReadTokens = extractCollectInt64(rawUsage, "cache_read_tokens", "cacheReadTokens")
	}
	if usage.CacheWriteTokens == 0 {
		usage.CacheWriteTokens = extractCollectInt64(rawUsage, "cache_write_tokens", "cacheWriteTokens")
	}
	if usage.ReasoningTokens == 0 {
		usage.ReasoningTokens = extractCollectInt64(rawUsage, "reasoning_tokens", "reasoningTokens")
	}
	return usage
}

func extractCollectInt64(payload map[string]any, keys ...string) int64 {
	value := lookupCollectAny(payload, keys...)
	switch typed := value.(type) {
	case nil:
		return 0
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	case json.Number:
		parsed, err := typed.Int64()
		if err == nil {
			return parsed
		}
		floatParsed, err := typed.Float64()
		if err == nil {
			return int64(floatParsed)
		}
		return 0
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

func extractCollectFloat(payload map[string]any, keys ...string) *float64 {
	value := lookupCollectAny(payload, keys...)
	switch typed := value.(type) {
	case nil:
		return nil
	case float64:
		result := typed
		return &result
	case float32:
		result := float64(typed)
		return &result
	case int:
		result := float64(typed)
		return &result
	case int64:
		result := float64(typed)
		return &result
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return nil
		}
		return &parsed
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return nil
		}
		return &parsed
	default:
		return nil
	}
}

func lookupCollectAny(payload map[string]any, keys ...string) any {
	for _, key := range keys {
		value, ok := payload[key]
		if !ok {
			continue
		}
		return value
	}
	return nil
}

func lookupCollectString(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := payload[key]
		if !ok {
			continue
		}
		if text := collectValueToString(value); text != "" {
			return text
		}
	}
	return ""
}

func collectValueToString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	case json.Number:
		return strings.TrimSpace(typed.String())
	case int:
		return strconv.Itoa(typed)
	case int8:
		return strconv.FormatInt(int64(typed), 10)
	case int16:
		return strconv.FormatInt(int64(typed), 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint:
		return strconv.FormatUint(uint64(typed), 10)
	case uint8:
		return strconv.FormatUint(uint64(typed), 10)
	case uint16:
		return strconv.FormatUint(uint64(typed), 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 64)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		content, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(content))
	}
}

func collectStableID(prefix string, parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(sum[:8]))
}

func inferCollectTool(defaultTool, sourcePath string) string {
	if defaultTool != collectToolAuto {
		return defaultTool
	}
	normalized := strings.ToLower(filepath.ToSlash(sourcePath))
	switch {
	case strings.Contains(normalized, ".codex/") || strings.Contains(normalized, "codex"):
		return collectToolCodex
	case strings.Contains(normalized, ".claude/") || strings.Contains(normalized, "claude"):
		return collectToolClaude
	case strings.Contains(normalized, ".gemini/") || strings.Contains(normalized, "gemini"):
		return collectToolGemini
	default:
		return collectToolAuto
	}
}

func writeCollectEventsJSONL(writer io.Writer, events []agentEvent) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
	return nil
}

func openCollectWriter(rawOutput string) (io.WriteCloser, string, error) {
	target := strings.TrimSpace(rawOutput)
	if target == "" || target == "-" {
		return nopWriteCloser{Writer: os.Stdout}, "", nil
	}

	resolved, err := resolveCollectPath(target)
	if err != nil {
		return nil, "", err
	}
	if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
		return nil, "", fmt.Errorf("创建输出目录失败: %w", err)
	}
	file, err := os.OpenFile(resolved, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, "", err
	}
	return file, resolved, nil
}

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error {
	return nil
}
