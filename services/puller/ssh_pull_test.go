package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func writeFakeSSHCommand(t *testing.T, success bool) string {
	t.Helper()

	dir := t.TempDir()
	if runtime.GOOS == "windows" {
		path := filepath.Join(dir, "ssh.bat")
		content := "@echo off\r\n"
		if success {
			content += "echo mocked-ssh-output\r\nexit /b 0\r\n"
		} else {
			content += "echo mocked-ssh-error 1>&2\r\nexit /b 7\r\n"
		}
		if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
			t.Fatalf("write fake ssh.bat failed: %v", err)
		}
		return dir
	}

	path := filepath.Join(dir, "ssh")
	content := "#!/usr/bin/env sh\n"
	if success {
		content += "echo mocked-ssh-output\nexit 0\n"
	} else {
		content += "echo mocked-ssh-error 1>&2\nexit 7\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write fake ssh failed: %v", err)
	}
	return dir
}

func writeSilentFailSSHCommand(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	if runtime.GOOS == "windows" {
		path := filepath.Join(dir, "ssh.bat")
		content := "@echo off\r\nexit /b 9\r\n"
		if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
			t.Fatalf("write silent fail ssh.bat failed: %v", err)
		}
		return dir
	}

	path := filepath.Join(dir, "ssh")
	content := "#!/usr/bin/env sh\nexit 9\n"
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write silent fail ssh failed: %v", err)
	}
	return dir
}

func TestPullSSHFile(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fakePath := writeFakeSSHCommand(t, true) + string(os.PathListSeparator) + os.Getenv("PATH")
		t.Setenv("PATH", fakePath)

		svc := &pullerService{
			runtime: pullerRuntimeConfig{SSHTimeout: 2 * time.Second},
		}
		out, err := svc.pullSSHFile(context.Background(), sshLocation{
			User: "dev",
			Host: "127.0.0.1",
			Port: 2222,
			Path: "/tmp/chat.log",
		})
		if err != nil {
			t.Fatalf("pullSSHFile() unexpected error: %v", err)
		}
		if !strings.Contains(string(out), "mocked-ssh-output") {
			t.Fatalf("pullSSHFile() output = %q, want mocked-ssh-output", string(out))
		}
	})

	t.Run("command_failed", func(t *testing.T) {
		fakePath := writeFakeSSHCommand(t, false) + string(os.PathListSeparator) + os.Getenv("PATH")
		t.Setenv("PATH", fakePath)

		svc := &pullerService{
			runtime: pullerRuntimeConfig{SSHTimeout: 2 * time.Second},
		}
		_, err := svc.pullSSHFile(context.Background(), sshLocation{
			User: "dev",
			Host: "127.0.0.1",
			Port: 22,
			Path: "/tmp/chat.log",
		})
		if err == nil || !strings.Contains(err.Error(), "ssh command failed") {
			t.Fatalf("pullSSHFile() error = %v, want ssh command failed", err)
		}
	})

	t.Run("command_failed_without_output_and_default_port", func(t *testing.T) {
		fakePath := writeSilentFailSSHCommand(t) + string(os.PathListSeparator) + os.Getenv("PATH")
		t.Setenv("PATH", fakePath)

		svc := &pullerService{
			runtime: pullerRuntimeConfig{SSHTimeout: 2 * time.Second},
		}
		_, err := svc.pullSSHFile(context.Background(), sshLocation{
			User: "dev",
			Host: "127.0.0.1",
			Port: 0,
			Path: "/tmp/chat.log",
		})
		if err == nil || !strings.Contains(err.Error(), "ssh command failed") {
			t.Fatalf("pullSSHFile() error = %v, want ssh command failed", err)
		}
		if !strings.Contains(err.Error(), "exit") {
			t.Fatalf("pullSSHFile() should fallback to command error detail, got: %v", err)
		}
	})
}
