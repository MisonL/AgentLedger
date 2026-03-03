package main

import (
	"context"
	"fmt"
	"net/url"
	"os/exec"
	"regexp"
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
	sshCtx, cancel := context.WithTimeout(ctx, s.runtime.SSHTimeout)
	defer cancel()

	args := []string{"-o", "BatchMode=yes"}
	if location.Port > 0 {
		args = append(args, "-p", strconv.Itoa(location.Port))
	}
	args = append(args, location.Target(), "cat -- "+shellQuote(location.Path))

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

func shellQuote(raw string) string {
	return "'" + strings.ReplaceAll(raw, "'", `'"'"'`) + "'"
}
