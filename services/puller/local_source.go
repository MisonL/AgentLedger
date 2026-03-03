package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const maxLocalFileSizeBytes int64 = 20 * 1024 * 1024

var (
	errLocalLocationInvalid = errors.New("local location invalid")
	errLocalReadFailed      = errors.New("local read failed")
)

type sourceContent struct {
	SourcePath string
	HostKey    string
	Content    []byte
}

func (s *pullerService) fetchLocalSourceContents(ctx context.Context, source sourceRecord) ([]sourceContent, error) {
	files, err := collectLocalSourceFiles(source.Location)
	if err != nil {
		return nil, err
	}

	contents := make([]sourceContent, 0, len(files))
	for _, filePath := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		content, err := os.ReadFile(filePath)
		if err != nil {
			if s != nil && s.log != nil {
				s.log.Warn("skip unreadable local file", "source_id", source.ID, "path", filePath, "error", err)
			}
			continue
		}
		if len(content) == 0 || !isLikelyTextContent(content) {
			continue
		}

		contents = append(contents, sourceContent{
			SourcePath: filePath,
			HostKey:    buildLocalHostKey(filePath),
			Content:    content,
		})
	}

	if len(contents) == 0 {
		return nil, fmt.Errorf("%w: no readable local text files found under %s", errLocalReadFailed, strings.TrimSpace(source.Location))
	}

	return contents, nil
}

func collectLocalSourceFiles(rawLocation string) ([]string, error) {
	location, err := normalizeLocalLocation(rawLocation)
	if err != nil {
		return nil, err
	}

	info, err := os.Stat(location)
	if err != nil {
		return nil, fmt.Errorf("%w: stat local location failed: %w", errLocalLocationInvalid, err)
	}

	cleaned := filepath.Clean(location)
	if info.Mode().IsRegular() {
		if !isSupportedLocalFile(cleaned) {
			return nil, fmt.Errorf("%w: local file is not supported: %s", errLocalLocationInvalid, cleaned)
		}
		if info.Size() > maxLocalFileSizeBytes {
			return nil, fmt.Errorf("%w: local file is too large: %s", errLocalLocationInvalid, cleaned)
		}
		return []string{cleaned}, nil
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: local location must be a file or directory", errLocalLocationInvalid)
	}

	files := make([]string, 0, 16)
	err = filepath.WalkDir(cleaned, func(currentPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return nil
		}

		normalizedPath := filepath.Clean(currentPath)
		if !isSupportedLocalFile(normalizedPath) {
			return nil
		}

		fileInfo, err := entry.Info()
		if err != nil {
			return err
		}
		if fileInfo.Size() > maxLocalFileSizeBytes {
			return nil
		}

		files = append(files, normalizedPath)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: walk local location failed: %w", errLocalLocationInvalid, err)
	}

	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("%w: no supported local files found under %s", errLocalLocationInvalid, cleaned)
	}
	return files, nil
}

func normalizeLocalLocation(rawLocation string) (string, error) {
	location := strings.TrimSpace(rawLocation)
	if location == "" {
		return "", fmt.Errorf("%w: empty local location", errLocalLocationInvalid)
	}

	expanded, err := expandHomePath(location)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errLocalLocationInvalid, err)
	}

	cleaned := filepath.Clean(expanded)
	if !filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("%w: local location must be an absolute path", errLocalLocationInvalid)
	}
	return cleaned, nil
}

func expandHomePath(path string) (string, error) {
	if path == "" || path[0] != '~' {
		return path, nil
	}
	if path != "~" && !strings.HasPrefix(path, "~/") && !strings.HasPrefix(path, `~\`) {
		return "", fmt.Errorf("unsupported home path format: %s", path)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home failed: %w", err)
	}
	if strings.TrimSpace(homeDir) == "" {
		return "", fmt.Errorf("resolve user home failed: empty home directory")
	}

	if path == "~" {
		return homeDir, nil
	}

	suffix := path[2:]
	return filepath.Join(homeDir, suffix), nil
}

func buildLocalHostKey(filePath string) string {
	cleaned := filepath.ToSlash(filepath.Clean(filePath))
	return "local:" + cleaned
}

func isSupportedLocalFile(path string) bool {
	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(path)))
	switch ext {
	case "", ".json", ".jsonl", ".log", ".txt", ".md", ".ndjson":
		return true
	default:
		return false
	}
}

func isLikelyTextContent(content []byte) bool {
	limit := len(content)
	if limit > 4096 {
		limit = 4096
	}

	for i := 0; i < limit; i++ {
		if content[i] == 0x00 {
			return false
		}
	}
	return true
}
