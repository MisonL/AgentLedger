package main

import (
	"context"
	"fmt"
	"net/url"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var scpLikePattern = regexp.MustCompile(`^([^@\s]+)@([^:\s]+):(.+)$`)

func parseSSHLocation(raw string) (sshLocation, error) {
	location := strings.TrimSpace(raw)
	if location == "" {
		return sshLocation{}, fmt.Errorf("empty ssh location")
	}

	if strings.HasPrefix(strings.ToLower(location), "ssh://") {
		parsed, err := url.Parse(location)
		if err != nil {
			return sshLocation{}, fmt.Errorf("parse ssh url failed: %w", err)
		}
		if !strings.EqualFold(parsed.Scheme, "ssh") {
			return sshLocation{}, fmt.Errorf("unsupported scheme: %s", parsed.Scheme)
		}
		user := ""
		if parsed.User != nil {
			user = strings.TrimSpace(parsed.User.Username())
		}
		host := strings.TrimSpace(parsed.Hostname())
		if user == "" || host == "" {
			return sshLocation{}, fmt.Errorf("ssh url must include user and host")
		}

		path := strings.TrimSpace(parsed.Path)
		if unescaped, err := url.PathUnescape(path); err == nil {
			path = strings.TrimSpace(unescaped)
		}
		if path == "" || path == "/" {
			return sshLocation{}, fmt.Errorf("ssh url must include path")
		}

		port := 22
		if rawPort := strings.TrimSpace(parsed.Port()); rawPort != "" {
			value, err := strconv.Atoi(rawPort)
			if err != nil || value <= 0 {
				return sshLocation{}, fmt.Errorf("invalid ssh port: %s", rawPort)
			}
			port = value
		}

		return sshLocation{
			User: user,
			Host: host,
			Port: port,
			Path: path,
		}, nil
	}

	matches := scpLikePattern.FindStringSubmatch(location)
	if len(matches) != 4 {
		return sshLocation{}, fmt.Errorf("unsupported ssh location format")
	}

	user := strings.TrimSpace(matches[1])
	host := strings.TrimSpace(matches[2])
	path := strings.TrimSpace(matches[3])
	if user == "" || host == "" || path == "" {
		return sshLocation{}, fmt.Errorf("ssh location requires user, host, and path")
	}

	return sshLocation{
		User: user,
		Host: host,
		Port: 22,
		Path: path,
	}, nil
}

func (s *pullerService) pullSSHFile(ctx context.Context, location sshLocation) ([]byte, error) {
	return s.runSSHCommand(ctx, location, buildSSHCatCommand(location.Path))
}

func (s *pullerService) fetchSSHSourceContents(ctx context.Context, location sshLocation) ([]sourceContent, error) {
	paths, err := s.listSSHSourcePaths(ctx, location)
	if err != nil {
		return nil, err
	}

	contents := make([]sourceContent, 0, len(paths))
	for _, sourcePath := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		fileLocation := location
		fileLocation.Path = sourcePath

		content, err := s.pullSSHFile(ctx, fileLocation)
		if err != nil {
			return nil, fmt.Errorf("pull remote file failed (%s): %w", sourcePath, err)
		}

		contents = append(contents, sourceContent{
			SourcePath: sourcePath,
			HostKey:    fileLocation.HostKey(),
			Content:    content,
		})
	}

	if len(contents) == 0 {
		return nil, fmt.Errorf("no readable remote files found under %s", strings.TrimSpace(location.Path))
	}
	return contents, nil
}

func (s *pullerService) listSSHSourcePaths(ctx context.Context, location sshLocation) ([]string, error) {
	output, err := s.runSSHCommand(ctx, location, buildSSHListCommand(location.Path))
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.ReplaceAll(string(output), "\r\n", "\n"), "\n")
	paths := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		paths = append(paths, trimmed)
	}
	sort.Strings(paths)
	return paths, nil
}

func (s *pullerService) runSSHCommand(ctx context.Context, location sshLocation, remoteCommand string) ([]byte, error) {
	sshCtx, cancel := context.WithTimeout(ctx, s.runtime.SSHTimeout)
	defer cancel()

	args := []string{"-o", "BatchMode=yes"}
	if location.Port > 0 {
		args = append(args, "-p", strconv.Itoa(location.Port))
	}
	args = append(args, location.Target(), remoteCommand)

	cmd := exec.CommandContext(sshCtx, "ssh", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		detail := strings.TrimSpace(string(output))
		if detail == "" {
			detail = err.Error()
		}
		return nil, fmt.Errorf("ssh command failed: %s", detail)
	}

	return output, nil
}

func buildSSHCatCommand(path string) string {
	return "cat -- " + shellQuote(path)
}

func buildSSHListCommand(path string) string {
	quotedPath := shellQuote(path)
	return "if [ -f " + quotedPath + " ]; then " +
		"printf '%s\\n' " + quotedPath + "; " +
		"elif [ -d " + quotedPath + " ]; then " +
		"find " + quotedPath + " -type f \\( -name '*.json' -o -name '*.jsonl' -o -name '*.md' \\) -print | LC_ALL=C sort; " +
		"else " +
		"echo 'ssh source path not found or not regular file/directory' >&2; exit 3; " +
		"fi"
}

func shellQuote(raw string) string {
	return "'" + strings.ReplaceAll(raw, "'", `'"'"'`) + "'"
}
