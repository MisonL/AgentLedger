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

func TestParseLinesConcurrently_ExtractsJSONTokenAndContentFields(t *testing.T) {
	t.Parallel()

	lines := []lineRecord{
		{
			No:   1,
			Text: `{"session_id":"codex-s-1","event_type":"message","role":"assistant","content":[{"type":"text","text":"codex reply"}],"usage":{"input_tokens":11,"output_tokens":22},"model":"gpt-5-codex","cost_usd":0.12}`,
		},
		{
			No:   2,
			Text: `{"session":"claude-s-1","message":{"content":[{"type":"text","text":"claude reply"}]},"usage":{"prompt_tokens":33,"completion_tokens":44},"model_name":"claude-3-7-sonnet"}`,
		},
		{
			No:   3,
			Text: `{"session":"gemini-s-1","candidates":[{"content":{"parts":[{"text":"gemini reply"}]}}],"usageMetadata":{"promptTokenCount":55,"candidatesTokenCount":66},"model":"gemini-2.5-pro"}`,
		},
	}

	outputs, err := parseLinesConcurrently(context.Background(), parseInput{
		Source:      sourceRecord{ID: "source-json-rich"},
		SourcePath:  "/tmp/rich.jsonl",
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
	if len(jsonOut.Events) != 3 {
		t.Fatalf("json events = %d, want 3", len(jsonOut.Events))
	}

	event1 := jsonOut.Events[0].Event
	if event1.SessionID != "codex-s-1" {
		t.Fatalf("event1.session_id = %q, want codex-s-1", event1.SessionID)
	}
	if event1.Text != "codex reply" {
		t.Fatalf("event1.text = %q, want codex reply", event1.Text)
	}
	if event1.Tokens.InputTokens != 11 || event1.Tokens.OutputTokens != 22 {
		t.Fatalf("event1.tokens = %#v, want input=11 output=22", event1.Tokens)
	}
	if event1.CostUSD == nil || *event1.CostUSD != 0.12 {
		t.Fatalf("event1.cost_usd = %v, want 0.12", event1.CostUSD)
	}

	event2 := jsonOut.Events[1].Event
	if event2.SessionID != "claude-s-1" {
		t.Fatalf("event2.session_id = %q, want claude-s-1", event2.SessionID)
	}
	if event2.Text != "claude reply" {
		t.Fatalf("event2.text = %q, want claude reply", event2.Text)
	}
	if event2.Model != "claude-3-7-sonnet" {
		t.Fatalf("event2.model = %q, want claude-3-7-sonnet", event2.Model)
	}
	if event2.Tokens.InputTokens != 33 || event2.Tokens.OutputTokens != 44 {
		t.Fatalf("event2.tokens = %#v, want input=33 output=44", event2.Tokens)
	}

	event3 := jsonOut.Events[2].Event
	if event3.SessionID != "gemini-s-1" {
		t.Fatalf("event3.session_id = %q, want gemini-s-1", event3.SessionID)
	}
	if event3.Text != "gemini reply" {
		t.Fatalf("event3.text = %q, want gemini reply", event3.Text)
	}
	if event3.Tokens.InputTokens != 55 || event3.Tokens.OutputTokens != 66 {
		t.Fatalf("event3.tokens = %#v, want input=55 output=66", event3.Tokens)
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
