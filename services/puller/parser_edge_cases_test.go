package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseLinesConcurrently_CancelAndError(t *testing.T) {
	makeLines := func(n int) []lineRecord {
		lines := make([]lineRecord, 0, n)
		for i := 1; i <= n; i++ {
			lines = append(lines, lineRecord{
				No:   int64(i),
				Text: `{"session_id":"s-1","event_type":"message","text":"hello"}`,
			})
		}
		return lines
	}

	t.Run("cancelled_by_checker", func(t *testing.T) {
		_, err := parseLinesConcurrently(context.Background(), parseInput{
			Source:      sourceRecord{ID: "source-1"},
			SourcePath:  "/tmp/cancel.jsonl",
			Lines:       makeLines(parserCancelCheckEvery + 1),
			JSONLStart:  0,
			NativeStart: 0,
			CheckCancel: func(ctx context.Context) (bool, error) {
				return true, nil
			},
		})
		if !errors.Is(err, errJobCancelled) {
			t.Fatalf("parseLinesConcurrently() error = %v, want %v", err, errJobCancelled)
		}
	})

	t.Run("checker_returns_error", func(t *testing.T) {
		_, err := parseLinesConcurrently(context.Background(), parseInput{
			Source:      sourceRecord{ID: "source-1"},
			SourcePath:  "/tmp/error.jsonl",
			Lines:       makeLines(parserCancelCheckEvery + 1),
			JSONLStart:  0,
			NativeStart: 0,
			CheckCancel: func(ctx context.Context) (bool, error) {
				return false, errors.New("checker down")
			},
		})
		if err == nil || !strings.Contains(err.Error(), "check cancel_requested failed") {
			t.Fatalf("parseLinesConcurrently() error = %v, want wrapped checker error", err)
		}
	})
}

func TestParseJSONLLines_NonObjectPayload(t *testing.T) {
	out, err := parseJSONLLines(context.Background(), parseInput{
		Source:      sourceRecord{ID: "source-1"},
		SourcePath:  "/tmp/values.log",
		Lines:       []lineRecord{{No: 1, Text: `123`}},
		JSONLStart:  0,
		NativeStart: 0,
	})
	if err != nil {
		t.Fatalf("parseJSONLLines() unexpected error: %v", err)
	}
	if len(out.Events) != 1 {
		t.Fatalf("parseJSONLLines() events = %d, want 1", len(out.Events))
	}
	if string(out.Events[0].Event.Payload) != `{"value":123}` {
		t.Fatalf("parseJSONLLines() payload = %s, want wrapped scalar", string(out.Events[0].Event.Payload))
	}
}

func TestParseLines_CancelledContextEarly(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := parseJSONLLines(cancelledCtx, parseInput{
		Source:     sourceRecord{ID: "source-1"},
		SourcePath: "/tmp/ctx.jsonl",
		Lines:      []lineRecord{{No: 1, Text: `{"text":"a"}`}},
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("parseJSONLLines(cancelled) error = %v, want context.Canceled", err)
	}

	if _, err := parseNativeLines(cancelledCtx, parseInput{
		Source:     sourceRecord{ID: "source-1"},
		SourcePath: "/tmp/ctx.native",
		Lines:      []lineRecord{{No: 1, Text: "assistant: hi"}},
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("parseNativeLines(cancelled) error = %v, want context.Canceled", err)
	}
}

func TestParseNativeLine_EdgeCases(t *testing.T) {
	now := mustTime("2026-03-03T10:00:00Z")

	role, text, occurredAt, dateKey := parseNativeLine("   ", now)
	if role != "assistant" || text != "" || occurredAt == "" || dateKey != "2026-03-03" {
		t.Fatalf("parseNativeLine(blank) got role=%q text=%q occurredAt=%q dateKey=%q", role, text, occurredAt, dateKey)
	}

	role, text, occurredAt, dateKey = parseNativeLine("not-a-time tool: output", now)
	if role != "assistant" || text != "not-a-time tool: output" {
		t.Fatalf("parseNativeLine(invalid-time-prefix) role=%q text=%q", role, text)
	}
	if occurredAt != now.UTC().Format(time.RFC3339Nano) || dateKey != "2026-03-03" {
		t.Fatalf("parseNativeLine(invalid-time-prefix) occurredAt/dateKey mismatch")
	}
}

func TestExtractSessionIDFromJSON_EdgeCases(t *testing.T) {
	if got := extractSessionIDFromJSON(map[string]any{"session": "  s-1  "}); got != "s-1" {
		t.Fatalf("extractSessionIDFromJSON(session:string) = %q, want s-1", got)
	}
	if got := extractSessionIDFromJSON(map[string]any{"session": map[string]any{"id": "nested-1"}}); got != "nested-1" {
		t.Fatalf("extractSessionIDFromJSON(session:map.id) = %q, want nested-1", got)
	}
	if got := extractSessionIDFromJSON(map[string]any{"session": 123}); got != "" {
		t.Fatalf("extractSessionIDFromJSON(session:unsupported) = %q, want empty", got)
	}
}
