package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/agentledger/agentledger/services/internal/shared/ingest"
)

func TestLoadPullerRuntimeConfig_DefaultsAndOverrides(t *testing.T) {
	t.Setenv("PULLER_POLL_INTERVAL", "")
	t.Setenv("PULLER_JOB_TIMEOUT", "")
	t.Setenv("PULLER_SSH_TIMEOUT", "")
	t.Setenv("PULLER_INGEST_TIMEOUT", "")
	t.Setenv("PULLER_INGEST_ENDPOINT", "")
	t.Setenv("PULLER_INGEST_BEARER_TOKEN", " bearer-token ")
	t.Setenv("PULLER_AGENT_ID", "")

	cfg, err := loadPullerRuntimeConfig()
	if err != nil {
		t.Fatalf("loadPullerRuntimeConfig() unexpected error: %v", err)
	}
	if cfg.AgentID != "puller" {
		t.Fatalf("AgentID = %q, want puller", cfg.AgentID)
	}
	if cfg.IngestEndpoint != defaultIngestEndpoint {
		t.Fatalf("IngestEndpoint = %q, want %q", cfg.IngestEndpoint, defaultIngestEndpoint)
	}
	if cfg.IngestBearer != "bearer-token" {
		t.Fatalf("IngestBearer = %q, want bearer-token", cfg.IngestBearer)
	}

	t.Setenv("PULLER_AGENT_ID", "agent-custom")
	t.Setenv("PULLER_POLL_INTERVAL", "3s")
	t.Setenv("PULLER_JOB_TIMEOUT", "30s")
	t.Setenv("PULLER_SSH_TIMEOUT", "11s")
	t.Setenv("PULLER_INGEST_TIMEOUT", "9s")
	t.Setenv("PULLER_INGEST_ENDPOINT", "http://127.0.0.1:18081/v1/ingest")

	cfg, err = loadPullerRuntimeConfig()
	if err != nil {
		t.Fatalf("loadPullerRuntimeConfig() unexpected error with override: %v", err)
	}
	if cfg.AgentID != "agent-custom" {
		t.Fatalf("AgentID = %q, want agent-custom", cfg.AgentID)
	}
	if cfg.PollInterval != 3*time.Second || cfg.JobTimeout != 30*time.Second || cfg.SSHTimeout != 11*time.Second || cfg.IngestTimeout != 9*time.Second {
		t.Fatalf("duration override not applied: %+v", cfg)
	}
	if cfg.IngestEndpoint != "http://127.0.0.1:18081/v1/ingest" {
		t.Fatalf("IngestEndpoint = %q, want override", cfg.IngestEndpoint)
	}
}

func TestLoadPullerRuntimeConfig_InvalidEnv(t *testing.T) {
	t.Setenv("PULLER_POLL_INTERVAL", "not-duration")
	if _, err := loadPullerRuntimeConfig(); err == nil || !strings.Contains(err.Error(), "invalid PULLER_POLL_INTERVAL") {
		t.Fatalf("loadPullerRuntimeConfig() error = %v, want invalid PULLER_POLL_INTERVAL", err)
	}

	t.Setenv("PULLER_POLL_INTERVAL", "0s")
	t.Setenv("PULLER_JOB_TIMEOUT", "1s")
	t.Setenv("PULLER_SSH_TIMEOUT", "1s")
	t.Setenv("PULLER_INGEST_TIMEOUT", "1s")
	if _, err := loadPullerRuntimeConfig(); err == nil || !strings.Contains(err.Error(), "PULLER_POLL_INTERVAL") {
		t.Fatalf("loadPullerRuntimeConfig() error = %v, want poll interval > 0", err)
	}
}

func TestDurationFromEnv(t *testing.T) {
	t.Setenv("PULLER_TEST_DURATION", "")
	got, err := durationFromEnv("PULLER_TEST_DURATION", 7*time.Second)
	if err != nil {
		t.Fatalf("durationFromEnv() unexpected error: %v", err)
	}
	if got != 7*time.Second {
		t.Fatalf("durationFromEnv() = %v, want 7s", got)
	}

	t.Setenv("PULLER_TEST_DURATION", "250ms")
	got, err = durationFromEnv("PULLER_TEST_DURATION", 0)
	if err != nil {
		t.Fatalf("durationFromEnv() unexpected error: %v", err)
	}
	if got != 250*time.Millisecond {
		t.Fatalf("durationFromEnv() = %v, want 250ms", got)
	}

	t.Setenv("PULLER_TEST_DURATION", "bad")
	if _, err := durationFromEnv("PULLER_TEST_DURATION", 0); err == nil || !strings.Contains(err.Error(), "invalid PULLER_TEST_DURATION") {
		t.Fatalf("durationFromEnv() error = %v, want invalid env", err)
	}
}

func TestUtilsAndTypeHelpers(t *testing.T) {
	if got := nullableString("   "); got != nil {
		t.Fatalf("nullableString(blank) = %#v, want nil", got)
	}
	if got := nullableString(" x "); got != "x" {
		t.Fatalf("nullableString(non-blank) = %#v, want %q", got, "x")
	}

	cases := []struct {
		raw  string
		want int64
	}{
		{"", 0},
		{" 42 ", 42},
		{"-1", 0},
		{"not-number", 0},
	}
	for _, tt := range cases {
		got, err := parseWatermark(tt.raw)
		if err != nil {
			t.Fatalf("parseWatermark(%q) unexpected error: %v", tt.raw, err)
		}
		if got != tt.want {
			t.Fatalf("parseWatermark(%q) = %d, want %d", tt.raw, got, tt.want)
		}
	}

	loc := sshLocation{User: "root", Host: "127.0.0.1", Port: 2222, Path: "/var/log/app.log"}
	if got := loc.Target(); got != "root@127.0.0.1" {
		t.Fatalf("Target() = %q, want %q", got, "root@127.0.0.1")
	}
	if got := loc.HostKey(); got != "root@127.0.0.1:2222:/var/log/app.log" {
		t.Fatalf("HostKey() = %q, want expected", got)
	}
	loc = sshLocation{Host: "example.com", Path: "/a.log"}
	if got := loc.Target(); got != "example.com" {
		t.Fatalf("Target() = %q, want %q", got, "example.com")
	}
	if got := loc.HostKey(); got != "example.com:22:/a.log" {
		t.Fatalf("HostKey() = %q, want default port host key", got)
	}
}

type testStringer struct {
	value string
}

func (s testStringer) String() string {
	return s.value
}

func TestParserPrimitiveHelpers(t *testing.T) {
	lines, err := splitLines([]byte("a\nb\n"))
	if err != nil {
		t.Fatalf("splitLines() unexpected error: %v", err)
	}
	if len(lines) != 2 || lines[0].No != 1 || lines[1].No != 2 {
		t.Fatalf("splitLines() got %#v", lines)
	}

	tooLongLine := strings.Repeat("x", (8*1024*1024)+1)
	if _, err := splitLines([]byte(tooLongLine)); err == nil || !strings.Contains(err.Error(), "token too long") {
		t.Fatalf("splitLines(tooLong) error = %v, want token too long", err)
	}

	mixed := map[string]any{
		"s": " value ",
		"f": float64(12),
		"i": int64(9),
		"u": uint(7),
		"b": true,
		"x": struct{}{},
	}
	if got := anyToString(mixed["s"]); got != "value" {
		t.Fatalf("anyToString(string) = %q", got)
	}
	if got := anyToString(testStringer{value: "str"}); got != "str" {
		t.Fatalf("anyToString(stringer) = %q", got)
	}
	if got := anyToString(mixed["f"]); got != "12" {
		t.Fatalf("anyToString(float64) = %q", got)
	}
	if got := anyToString(mixed["i"]); got != "9" {
		t.Fatalf("anyToString(int64) = %q", got)
	}
	if got := anyToString(mixed["u"]); got != "7" {
		t.Fatalf("anyToString(uint) = %q", got)
	}
	if got := anyToString(mixed["b"]); got != "true" {
		t.Fatalf("anyToString(bool) = %q", got)
	}
	if got := anyToString(mixed["x"]); got != "" {
		t.Fatalf("anyToString(default) = %q, want empty", got)
	}

	if got := valueFromMap(map[string]any{"k1": " ", "k2": "hit"}, "k1", "k2"); got != "hit" {
		t.Fatalf("valueFromMap() = %q, want hit", got)
	}
}

func TestCollectAndSortEventsAndCancelCheck(t *testing.T) {
	outputs := map[string]parserOutput{
		parserKeyJSONL: {
			Events: []rawEventWithLine{
				{LineNo: 3, Event: ingest.RawEvent{EventID: "b"}},
				{LineNo: 1, Event: ingest.RawEvent{EventID: "c"}},
			},
		},
		parserKeyNative: {
			Events: []rawEventWithLine{
				{LineNo: 1, Event: ingest.RawEvent{EventID: "a"}},
			},
		},
	}

	got := collectAndSortEvents(outputs)
	if len(got) != 3 {
		t.Fatalf("collectAndSortEvents() len = %d, want 3", len(got))
	}
	if got[0].EventID != "a" || got[1].EventID != "c" || got[2].EventID != "b" {
		t.Fatalf("collectAndSortEvents() order = %#v", got)
	}

	canceled, err := checkCancelled(context.Background(), nil)
	if err != nil || canceled {
		t.Fatalf("checkCancelled(nil) = (%v, %v), want (false, nil)", canceled, err)
	}
	canceled, err = checkCancelled(context.Background(), func(ctx context.Context) (bool, error) {
		return true, nil
	})
	if err != nil || !canceled {
		t.Fatalf("checkCancelled(true) = (%v, %v), want (true, nil)", canceled, err)
	}
	_, err = checkCancelled(context.Background(), func(ctx context.Context) (bool, error) {
		return false, fmt.Errorf("boom")
	})
	if err == nil || !strings.Contains(err.Error(), "check cancel_requested failed") {
		t.Fatalf("checkCancelled(error) = %v, want wrapped error", err)
	}
}

func TestSyncCronAndConnectorHelpers(t *testing.T) {
	now := mustTime("2026-03-03T10:15:00Z")
	jobID1 := buildScheduledJobID("source-1", now)
	jobID2 := buildScheduledJobID(" source-1 ", now)
	if jobID1 != jobID2 {
		t.Fatalf("buildScheduledJobID() should trim source id: %q != %q", jobID1, jobID2)
	}

	svc := &pullerService{}
	registry := svc.effectiveConnectorRegistry()
	if registry == nil {
		t.Fatalf("effectiveConnectorRegistry() returned nil")
	}
	var nilRegistry *connectorRegistry
	nilRegistry.Register(nil)
	if got := nilRegistry.Select(sourceRecord{}, ""); got != nil {
		t.Fatalf("nil registry Select() = %#v, want nil", got)
	}

	typedConnector, ok := newFeatureConnector(" demo ", []string{" ", "feature"}).(*featureConnector)
	if !ok {
		t.Fatalf("newFeatureConnector() type assertion failed")
	}
	if typedConnector.Name() != "demo" {
		t.Fatalf("featureConnector.Name() = %q, want demo", typedConnector.Name())
	}
	var nilFeature *featureConnector
	if got := nilFeature.Name(); got != "" {
		t.Fatalf("nil featureConnector.Name() = %q, want empty", got)
	}

	custom := newConnectorRegistry()
	custom.Register(nil)
	svc.connectors = custom
	if got := svc.effectiveConnectorRegistry(); got != custom {
		t.Fatalf("effectiveConnectorRegistry() should return custom registry")
	}

	connector := newFeatureConnector("demo", []string{"demo"})
	out, err := connector.Parse(context.Background(), parseInput{
		Source:      sourceRecord{ID: "source-1"},
		SourcePath:  "/tmp/demo.log",
		Lines:       []lineRecord{{No: 1, Text: `{"session_id":"s1","event_type":"message","text":"ok"}`}},
		JSONLStart:  0,
		NativeStart: 0,
	})
	if err != nil {
		t.Fatalf("featureConnector.Parse() unexpected error: %v", err)
	}
	if len(out[parserKeyJSONL].Events) != 1 {
		t.Fatalf("featureConnector.Parse() json output invalid: %#v", out)
	}
}
