package main

import (
	"context"
	"testing"
)

func TestParseLinesConcurrently(t *testing.T) {
	t.Parallel()

	lines := []lineRecord{
		{No: 1, Text: `{"session_id":"s-1","event_type":"message","text":"hello","occurred_at":"2026-03-01T01:02:03Z"}`},
		{No: 2, Text: `assistant: world`},
		{No: 3, Text: `{"event_type":"message","text":"fallback"}`},
		{No: 4, Text: "   "},
		{No: 5, Text: `2026-03-01T02:03:04Z user: hi there`},
	}

	outputs, err := parseLinesConcurrently(context.Background(), parseInput{
		Source:      sourceRecord{ID: "source-1"},
		SourcePath:  "/tmp/test.log",
		Lines:       lines,
		JSONLStart:  0,
		NativeStart: 0,
	})
	if err != nil {
		t.Fatalf("parseLinesConcurrently() unexpected error: %v", err)
	}

	jsonOut, ok := outputs[parserKeyJSONL]
	if !ok {
		t.Fatalf("missing jsonl parser output")
	}
	nativeOut, ok := outputs[parserKeyNative]
	if !ok {
		t.Fatalf("missing native parser output")
	}

	if len(jsonOut.Events) != 2 {
		t.Fatalf("json events = %d, want 2", len(jsonOut.Events))
	}
	if len(jsonOut.Failures) != 2 {
		t.Fatalf("json failures = %d, want 2", len(jsonOut.Failures))
	}
	if len(nativeOut.Events) != 2 {
		t.Fatalf("native events = %d, want 2", len(nativeOut.Events))
	}

	if jsonOut.Events[0].Event.SessionID != "s-1" {
		t.Fatalf("json line1 session_id = %q, want %q", jsonOut.Events[0].Event.SessionID, "s-1")
	}
	if jsonOut.Events[1].Event.SessionID == "" {
		t.Fatalf("json line3 fallback session_id should not be empty")
	}

	if nativeOut.Events[0].Event.EventType != "message" {
		t.Fatalf("native event type = %q, want message", nativeOut.Events[0].Event.EventType)
	}
	if nativeOut.Events[0].Event.SourceOffset == nil || *nativeOut.Events[0].Event.SourceOffset != 2 {
		t.Fatalf("native line2 source_offset invalid: %#v", nativeOut.Events[0].Event.SourceOffset)
	}
	if nativeOut.Events[1].Event.SourceOffset == nil || *nativeOut.Events[1].Event.SourceOffset != 5 {
		t.Fatalf("native line5 source_offset invalid: %#v", nativeOut.Events[1].Event.SourceOffset)
	}

	if jsonOut.Failures[0].SourcePath != "/tmp/test.log" || jsonOut.Failures[0].SourceOffset != 2 {
		t.Fatalf("json failure[0] = %#v, want source_path=/tmp/test.log source_offset=2", jsonOut.Failures[0])
	}
	if jsonOut.Failures[1].SourceOffset != 5 {
		t.Fatalf("json failure[1].source_offset = %d, want 5", jsonOut.Failures[1].SourceOffset)
	}
	if jsonOut.Failures[0].Error == "" || jsonOut.Failures[1].Error == "" {
		t.Fatalf("json failure error should not be empty: %#v", jsonOut.Failures)
	}
}

func TestParseNativeLine(t *testing.T) {
	t.Parallel()

	role, text, occurredAt, dateKey := parseNativeLine("2026-03-01T02:03:04Z user: hello", mustTime("2026-03-02T00:00:00Z"))
	if role != "user" {
		t.Fatalf("role = %q, want user", role)
	}
	if text != "hello" {
		t.Fatalf("text = %q, want hello", text)
	}
	if occurredAt != "2026-03-01T02:03:04Z" {
		t.Fatalf("occurredAt = %q, want 2026-03-01T02:03:04Z", occurredAt)
	}
	if dateKey != "2026-03-01" {
		t.Fatalf("dateKey = %q, want 2026-03-01", dateKey)
	}
}
