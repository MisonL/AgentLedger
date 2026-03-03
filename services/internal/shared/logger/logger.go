package logger

import (
	"log/slog"
	"os"
	"strings"
)

// New 创建 JSON 结构化日志实例。
func New(level string) *slog.Logger {
	resolved := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		resolved = slog.LevelDebug
	case "warn", "warning":
		resolved = slog.LevelWarn
	case "error":
		resolved = slog.LevelError
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: resolved,
	})

	return slog.New(handler)
}
