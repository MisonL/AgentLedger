package main

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestParseSSHLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    sshLocation
		wantErr bool
	}{
		{
			name: "scp_like",
			raw:  "dev@10.0.0.8:/var/log/app.log",
			want: sshLocation{User: "dev", Host: "10.0.0.8", Port: 22, Path: "/var/log/app.log"},
		},
		{
			name: "ssh_url_with_port",
			raw:  "ssh://ops@example.com:2222/home/ops/events.jsonl",
			want: sshLocation{User: "ops", Host: "example.com", Port: 2222, Path: "/home/ops/events.jsonl"},
		},
		{
			name:    "invalid",
			raw:     "not-a-valid-location",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseSSHLocation(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseSSHLocation() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSSHLocation() unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseSSHLocation() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestShellQuote(t *testing.T) {
	t.Parallel()

	got := shellQuote("/var/log/it's.log")
	want := "'/var/log/it'\"'\"'s.log'"
	if got != want {
		t.Fatalf("shellQuote() = %q, want %q", got, want)
	}
}

func writeEvalRemoteCommandSSH(t *testing.T) string {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("directory ssh tests require POSIX shell")
	}

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "ssh")
	content := `#!/usr/bin/env sh
set -eu
last=""
for arg in "$@"; do
  last="$arg"
done
if [ -z "$last" ]; then
  echo "missing remote command" >&2
  exit 9
fi
sh -c "$last"
`
	if err := os.WriteFile(scriptPath, []byte(content), 0o755); err != nil {
		t.Fatalf("write fake ssh command failed: %v", err)
	}
	return dir
}

func TestFetchSSHSourceContents_DirectoryFiltersAndSort(t *testing.T) {
	fakePathPrefix := writeEvalRemoteCommandSSH(t)
	t.Setenv("PATH", fakePathPrefix+string(os.PathListSeparator)+os.Getenv("PATH"))

	root := t.TempDir()
	remoteDir := filepath.Join(root, "remote dir's files")
	if err := os.MkdirAll(filepath.Join(remoteDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir remote dir failed: %v", err)
	}

	files := map[string]string{
		"z.md":      "# z\n",
		"a.json":    `{"k":"v"}` + "\n",
		"b.jsonl":   `{"id":1}` + "\n",
		"skip.txt":  "skip\n",
		"sub/c.md":  "nested\n",
		"sub/d.log": "skip log\n",
	}
	for relativePath, body := range files {
		filePath := filepath.Join(remoteDir, relativePath)
		if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
			t.Fatalf("mkdir file dir failed: %v", err)
		}
		if err := os.WriteFile(filePath, []byte(body), 0o644); err != nil {
			t.Fatalf("write file failed: %v", err)
		}
	}

	svc := &pullerService{
		runtime: pullerRuntimeConfig{SSHTimeout: 2 * time.Second},
	}
	contents, err := svc.fetchSSHSourceContents(context.Background(), sshLocation{
		User: "dev",
		Host: "127.0.0.1",
		Port: 22,
		Path: remoteDir,
	})
	if err != nil {
		t.Fatalf("fetchSSHSourceContents() unexpected error: %v", err)
	}

	gotPaths := make([]string, 0, len(contents))
	for _, content := range contents {
		gotPaths = append(gotPaths, content.SourcePath)
		if len(content.Content) == 0 {
			t.Fatalf("file %s content should not be empty", content.SourcePath)
		}
	}

	wantPaths := []string{
		filepath.Join(remoteDir, "a.json"),
		filepath.Join(remoteDir, "b.jsonl"),
		filepath.Join(remoteDir, "sub", "c.md"),
		filepath.Join(remoteDir, "z.md"),
	}
	if !reflect.DeepEqual(gotPaths, wantPaths) {
		t.Fatalf("fetched paths = %#v, want %#v", gotPaths, wantPaths)
	}
}

func TestFetchSSHSourceContents_FileSourcePathCompatible(t *testing.T) {
	fakePathPrefix := writeEvalRemoteCommandSSH(t)
	t.Setenv("PATH", fakePathPrefix+string(os.PathListSeparator)+os.Getenv("PATH"))

	root := t.TempDir()
	filePath := filepath.Join(root, "chat.log")
	if err := os.WriteFile(filePath, []byte("line-1\n"), 0o644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}

	svc := &pullerService{
		runtime: pullerRuntimeConfig{SSHTimeout: 2 * time.Second},
	}
	location := sshLocation{
		User: "dev",
		Host: "127.0.0.1",
		Port: 22,
		Path: filePath,
	}
	contents, err := svc.fetchSSHSourceContents(context.Background(), location)
	if err != nil {
		t.Fatalf("fetchSSHSourceContents() unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("fetchSSHSourceContents() len = %d, want 1", len(contents))
	}
	if contents[0].SourcePath != filePath {
		t.Fatalf("source path = %q, want %q", contents[0].SourcePath, filePath)
	}
	if contents[0].HostKey != location.HostKey() {
		t.Fatalf("host key = %q, want %q", contents[0].HostKey, location.HostKey())
	}
	if strings.TrimSpace(string(contents[0].Content)) != "line-1" {
		t.Fatalf("content = %q, want line-1", string(contents[0].Content))
	}
}
