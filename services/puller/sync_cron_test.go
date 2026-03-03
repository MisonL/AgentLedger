package main

import "testing"

func TestCronScheduleHitsMinute(t *testing.T) {
	t.Parallel()

	schedule, err := parseSyncCron("*/5 * * * *")
	if err != nil {
		t.Fatalf("parseSyncCron() unexpected error: %v", err)
	}

	if !cronScheduleHitsMinute(schedule, mustTime("2026-03-02T10:15:34Z")) {
		t.Fatalf("cronScheduleHitsMinute() = false, want true for 10:15")
	}
	if cronScheduleHitsMinute(schedule, mustTime("2026-03-02T10:16:34Z")) {
		t.Fatalf("cronScheduleHitsMinute() = true, want false for 10:16")
	}
}

func TestShouldEnqueueScheduledJob_AvoidDuplicate(t *testing.T) {
	t.Parallel()

	schedule, err := parseSyncCron("* * * * *")
	if err != nil {
		t.Fatalf("parseSyncCron() unexpected error: %v", err)
	}

	now := mustTime("2026-03-02T10:20:12Z")
	if !shouldEnqueueScheduledJob(schedule, now, false) {
		t.Fatalf("shouldEnqueueScheduledJob() = false, want true")
	}
	if shouldEnqueueScheduledJob(schedule, now, true) {
		t.Fatalf("shouldEnqueueScheduledJob() = true, want false when already scheduled")
	}
}

func TestParseSyncCron_Invalid(t *testing.T) {
	t.Parallel()

	if _, err := parseSyncCron("invalid cron"); err == nil {
		t.Fatalf("parseSyncCron() expected error, got nil")
	}
}

func TestNormalizeSyncJobMode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
		want string
	}{
		{name: "realtime", raw: "realtime", want: "realtime"},
		{name: "sync_upper", raw: "SYNC", want: "sync"},
		{name: "hybrid_with_space", raw: " hybrid ", want: "hybrid"},
		{name: "empty_default", raw: "", want: "realtime"},
		{name: "invalid_default", raw: "manual", want: "realtime"},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := normalizeSyncJobMode(tt.raw)
			if got != tt.want {
				t.Fatalf("normalizeSyncJobMode(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}
