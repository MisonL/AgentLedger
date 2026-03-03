package main

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestResolveCollectSources_DefaultAndOverride(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	sources, err := resolveCollectSources("auto", "")
	if err != nil {
		t.Fatalf("resolveCollectSources(auto) unexpected error: %v", err)
	}

	wantAuto := []collectSource{
		{Tool: collectToolCodex, Dir: filepath.Join(homeDir, ".codex", "sessions")},
		{Tool: collectToolClaude, Dir: filepath.Join(homeDir, ".claude", "projects")},
		{Tool: collectToolGemini, Dir: filepath.Join(homeDir, ".gemini", "tmp")},
	}
	if !reflect.DeepEqual(sources, wantAuto) {
		t.Fatalf("resolveCollectSources(auto)=%#v, want=%#v", sources, wantAuto)
	}

	sources, err = resolveCollectSources("codex", "~/workspace/custom")
	if err != nil {
		t.Fatalf("resolveCollectSources(codex, override) unexpected error: %v", err)
	}
	wantOverride := []collectSource{
		{Tool: collectToolCodex, Dir: filepath.Join(homeDir, "workspace", "custom")},
	}
	if !reflect.DeepEqual(sources, wantOverride) {
		t.Fatalf("resolveCollectSources(codex, override)=%#v, want=%#v", sources, wantOverride)
	}

	if _, err := resolveCollectSources("unknown", ""); err == nil {
		t.Fatalf("resolveCollectSources(unknown) expected error, got nil")
	}
}

func TestScanCollectFiles_SupportedAndSorted(t *testing.T) {
	root := t.TempDir()
	paths := []string{
		filepath.Join(root, "b.log"),
		filepath.Join(root, "sub", "a.jsonl"),
		filepath.Join(root, "sub", "c.txt"),
	}
	for _, path := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("os.MkdirAll(%s) error: %v", path, err)
		}
		if err := os.WriteFile(path, []byte("line\n"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(%s) error: %v", path, err)
		}
	}

	unsupported := filepath.Join(root, "ignore.bin")
	if err := os.WriteFile(unsupported, []byte{0, 1, 2, 3}, 0o644); err != nil {
		t.Fatalf("os.WriteFile(%s) error: %v", unsupported, err)
	}

	files, err := scanCollectFiles(root)
	if err != nil {
		t.Fatalf("scanCollectFiles(%s) unexpected error: %v", root, err)
	}

	want := []string{
		filepath.Join(root, "b.log"),
		filepath.Join(root, "sub", "a.jsonl"),
		filepath.Join(root, "sub", "c.txt"),
	}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("scanCollectFiles(%s)=%#v, want=%#v", root, files, want)
	}
}

func TestCollectCommand_OutputAgentEventStructure(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "session.log")
	input := strings.Join([]string{
		`{"session_id":"sess-1","event_type":"message","role":"user","text":"hello","model":"gpt-5","occurred_at":"2026-03-01T01:02:03Z"}`,
		`assistant: hi`,
		`{"message":"third-line"}`,
	}, "\n")
	if err := os.WriteFile(inputPath, []byte(input), 0o644); err != nil {
		t.Fatalf("os.WriteFile(input) error: %v", err)
	}

	outputPath := filepath.Join(root, "output.jsonl")
	exitCode := collectCommand([]string{
		"--tool=codex",
		"--dir=" + root,
		"--output=" + outputPath,
		"--max-events=2",
	})
	if exitCode != 0 {
		t.Fatalf("collectCommand()=%d, want=0", exitCode)
	}

	outputBody, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("os.ReadFile(output) error: %v", err)
	}

	lines := splitNonEmptyLines(string(outputBody))
	if len(lines) != 2 {
		t.Fatalf("output lines=%d, want=2, output=%q", len(lines), string(outputBody))
	}

	var first agentEvent
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("json.Unmarshal(first) error: %v", err)
	}
	if first.EventID == "" {
		t.Fatalf("first.event_id is empty")
	}
	if first.SessionID != "sess-1" {
		t.Fatalf("first.session_id=%q, want=%q", first.SessionID, "sess-1")
	}
	if first.EventType != "message" {
		t.Fatalf("first.event_type=%q, want=message", first.EventType)
	}
	if first.Role != "user" {
		t.Fatalf("first.role=%q, want=user", first.Role)
	}
	if first.Text != "hello" {
		t.Fatalf("first.text=%q, want=hello", first.Text)
	}
	if first.Model != "gpt-5" {
		t.Fatalf("first.model=%q, want=gpt-5", first.Model)
	}
	if first.SourcePath != inputPath {
		t.Fatalf("first.source_path=%q, want=%q", first.SourcePath, inputPath)
	}
	if first.SourceOffset == nil || *first.SourceOffset != 1 {
		t.Fatalf("first.source_offset=%v, want=1", first.SourceOffset)
	}
	if len(first.Payload) == 0 {
		t.Fatalf("first.payload is empty")
	}

	var second agentEvent
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatalf("json.Unmarshal(second) error: %v", err)
	}
	if second.EventID == "" {
		t.Fatalf("second.event_id is empty")
	}
	if second.SessionID == "" {
		t.Fatalf("second.session_id is empty")
	}
	if second.Role != "assistant" {
		t.Fatalf("second.role=%q, want=assistant", second.Role)
	}
	if second.Text != "hi" {
		t.Fatalf("second.text=%q, want=hi", second.Text)
	}
	if second.SourcePath != inputPath {
		t.Fatalf("second.source_path=%q, want=%q", second.SourcePath, inputPath)
	}
	if second.SourceOffset == nil || *second.SourceOffset != 2 {
		t.Fatalf("second.source_offset=%v, want=2", second.SourceOffset)
	}
	if len(second.Payload) == 0 {
		t.Fatalf("second.payload is empty")
	}
}

func TestPrintUsage_IncludesCollect(t *testing.T) {
	stderr := captureStderrOutput(t, func() {
		printUsage()
	})
	if !strings.Contains(stderr, "collect") {
		t.Fatalf("printUsage() missing collect command, output=%q", stderr)
	}
}

func splitNonEmptyLines(raw string) []string {
	scanner := bufio.NewScanner(strings.NewReader(raw))
	lines := make([]string, 0)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

func captureStderrOutput(t *testing.T, fn func()) string {
	t.Helper()

	oldStderr := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error: %v", err)
	}

	os.Stderr = writer
	fn()
	_ = writer.Close()
	os.Stderr = oldStderr

	body, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("io.ReadAll(stderr) error: %v", err)
	}
	return string(body)
}
