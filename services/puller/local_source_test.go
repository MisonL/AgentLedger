package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCollectLocalSourceFiles(t *testing.T) {
	t.Run("single_file", func(t *testing.T) {
		dir := t.TempDir()
		filePath := filepath.Join(dir, "session.jsonl")
		if err := os.WriteFile(filePath, []byte(`{"id":"1"}`), 0o644); err != nil {
			t.Fatalf("write test file failed: %v", err)
		}

		files, err := collectLocalSourceFiles(filePath)
		if err != nil {
			t.Fatalf("collectLocalSourceFiles() unexpected error: %v", err)
		}
		if len(files) != 1 || files[0] != filepath.Clean(filePath) {
			t.Fatalf("collectLocalSourceFiles() = %#v, want [%q]", files, filepath.Clean(filePath))
		}
	})

	t.Run("directory_with_filter", func(t *testing.T) {
		dir := t.TempDir()
		nested := filepath.Join(dir, "nested")
		if err := os.MkdirAll(nested, 0o755); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}

		validOne := filepath.Join(dir, "a.json")
		validTwo := filepath.Join(nested, "b.log")
		invalid := filepath.Join(dir, "c.bin")
		if err := os.WriteFile(validOne, []byte(`{"id":"1"}`), 0o644); err != nil {
			t.Fatalf("write validOne failed: %v", err)
		}
		if err := os.WriteFile(validTwo, []byte("assistant: hi"), 0o644); err != nil {
			t.Fatalf("write validTwo failed: %v", err)
		}
		if err := os.WriteFile(invalid, []byte{0x00, 0x01, 0x02}, 0o644); err != nil {
			t.Fatalf("write invalid failed: %v", err)
		}

		files, err := collectLocalSourceFiles(dir)
		if err != nil {
			t.Fatalf("collectLocalSourceFiles() unexpected error: %v", err)
		}
		if len(files) != 2 {
			t.Fatalf("collectLocalSourceFiles() len = %d, want 2 (%#v)", len(files), files)
		}
		if files[0] != filepath.Clean(validOne) || files[1] != filepath.Clean(validTwo) {
			t.Fatalf("collectLocalSourceFiles() order/content = %#v", files)
		}
	})

	t.Run("relative_path_rejected", func(t *testing.T) {
		_, err := collectLocalSourceFiles("./relative/path")
		if err == nil || !strings.Contains(err.Error(), "absolute path") {
			t.Fatalf("collectLocalSourceFiles(relative) error = %v, want absolute path error", err)
		}
	})

	t.Run("home_path_expanded", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		if runtime.GOOS == "windows" {
			t.Setenv("USERPROFILE", home)
		}

		targetDir := filepath.Join(home, ".codex", "sessions")
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			t.Fatalf("mkdir targetDir failed: %v", err)
		}
		targetFile := filepath.Join(targetDir, "session.jsonl")
		if err := os.WriteFile(targetFile, []byte(`{"ok":true}`), 0o644); err != nil {
			t.Fatalf("write targetFile failed: %v", err)
		}

		homeExpr := filepath.Join("~", ".codex", "sessions")
		files, err := collectLocalSourceFiles(homeExpr)
		if err != nil {
			t.Fatalf("collectLocalSourceFiles(home path) unexpected error: %v", err)
		}
		if len(files) != 1 || files[0] != filepath.Clean(targetFile) {
			t.Fatalf("collectLocalSourceFiles(home path) = %#v, want [%q]", files, filepath.Clean(targetFile))
		}
	})
}

func TestFetchLocalSourceContents(t *testing.T) {
	dir := t.TempDir()
	textFile := filepath.Join(dir, "chat.jsonl")
	binaryFile := filepath.Join(dir, "blob.txt")

	if err := os.WriteFile(textFile, []byte(`{"session_id":"s1","text":"hello"}`), 0o644); err != nil {
		t.Fatalf("write textFile failed: %v", err)
	}
	if err := os.WriteFile(binaryFile, []byte{0x00, 0x01, 0x02}, 0o644); err != nil {
		t.Fatalf("write binaryFile failed: %v", err)
	}

	svc := &pullerService{}
	contents, err := svc.fetchLocalSourceContents(context.Background(), sourceRecord{
		ID:       "source-local-1",
		Type:     "local",
		Location: dir,
	})
	if err != nil {
		t.Fatalf("fetchLocalSourceContents() unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("fetchLocalSourceContents() len = %d, want 1 (%#v)", len(contents), contents)
	}
	if contents[0].SourcePath != filepath.Clean(textFile) {
		t.Fatalf("fetchLocalSourceContents() source_path = %q, want %q", contents[0].SourcePath, filepath.Clean(textFile))
	}
	if gotPrefix := strings.HasPrefix(contents[0].HostKey, "local:"); !gotPrefix {
		t.Fatalf("fetchLocalSourceContents() host_key = %q, want local:*", contents[0].HostKey)
	}
}

func TestBuildLocalHostKey(t *testing.T) {
	if runtime.GOOS == "windows" {
		key := buildLocalHostKey(`C:\work\chat\session.jsonl`)
		if !strings.HasPrefix(key, "local:") || !strings.Contains(key, "/work/chat/session.jsonl") {
			t.Fatalf("buildLocalHostKey(windows) = %q, want normalized slash path", key)
		}
		return
	}

	key := buildLocalHostKey("/tmp/chat/session.jsonl")
	if key != "local:/tmp/chat/session.jsonl" {
		t.Fatalf("buildLocalHostKey(unix) = %q, want %q", key, "local:/tmp/chat/session.jsonl")
	}
}
