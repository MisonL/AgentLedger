package main

import (
	"strconv"
	"strings"
)

const (
	defaultWatermarkProvider = "unknown"
	legacyWatermarkHostKey   = "__legacy__"
)

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func nullableString(value string) any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func parseWatermark(raw string) (int64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, nil
	}
	if parsed < 0 {
		return 0, nil
	}
	return parsed, nil
}

func int64Ptr(value int64) *int64 {
	v := value
	return &v
}

func normalizeWatermarkParserKey(parserKey string) string {
	return firstNonEmpty(parserKey, defaultWatermarkProvider)
}

func normalizeWatermarkHostKey(hostKey string) string {
	return firstNonEmpty(hostKey, legacyWatermarkHostKey)
}

func watermarkProviderFromParserKey(parserKey string) string {
	return normalizeWatermarkParserKey(parserKey)
}

func watermarkLookupHostKeys(hostKey string) []string {
	normalizedHostKey := normalizeWatermarkHostKey(hostKey)
	if normalizedHostKey == legacyWatermarkHostKey {
		return []string{legacyWatermarkHostKey}
	}
	return []string{normalizedHostKey, legacyWatermarkHostKey}
}
