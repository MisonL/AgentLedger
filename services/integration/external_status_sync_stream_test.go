package main

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestEnsureAlertExternalStatusSyncStreamCreateWhenMissing(t *testing.T) {
	t.Parallel()

	const (
		streamName = "INTEGRATION_ALERT_EXTERNAL_STATUS_SYNC_EVENTS"
		subject    = "integration.alert.external_status_sync"
	)

	manager := &fakeCallbackStreamManager{
		streamInfoFunc: func(context.Context, string) (*jetstream.StreamInfo, error) {
			return nil, jetstream.ErrStreamNotFound
		},
		createStreamFunc: func(_ context.Context, cfg jetstream.StreamConfig) error {
			if cfg.Name != streamName {
				t.Fatalf("create stream name mismatch: got %q want %q", cfg.Name, streamName)
			}
			if cfg.Description != "integration alert external status sync events" {
				t.Fatalf("create stream description mismatch: got %q", cfg.Description)
			}
			if len(cfg.Subjects) != 1 || cfg.Subjects[0] != subject {
				t.Fatalf("create stream subjects mismatch: got %v want [%q]", cfg.Subjects, subject)
			}
			return nil
		},
		updateStreamFunc: func(context.Context, jetstream.StreamConfig) error {
			t.Fatalf("update stream should not be called when stream is created")
			return nil
		},
	}

	if err := ensureAlertExternalStatusSyncStream(context.Background(), manager, streamName, subject, noopCallbackStreamLogger{}); err != nil {
		t.Fatalf("ensureAlertExternalStatusSyncStream returned error: %v", err)
	}
	if manager.createCalls != 1 {
		t.Fatalf("create stream calls mismatch: got %d want %d", manager.createCalls, 1)
	}
	if manager.updateCalls != 0 {
		t.Fatalf("update stream should not be called, got %d", manager.updateCalls)
	}
}

func TestEnsureAlertExternalStatusSyncStreamAppendSubjectWithoutOverwritingConfig(t *testing.T) {
	t.Parallel()

	const (
		streamName = "INTEGRATION_ALERT_EXTERNAL_STATUS_SYNC_EVENTS"
		subject    = "integration.alert.external_status_sync"
	)

	manager := &fakeCallbackStreamManager{
		streamInfoFunc: func(context.Context, string) (*jetstream.StreamInfo, error) {
			return &jetstream.StreamInfo{
				Config: jetstream.StreamConfig{
					Name:      streamName,
					Subjects:  []string{"integration.alert.external_status.legacy"},
					Retention: jetstream.InterestPolicy,
					Storage:   jetstream.MemoryStorage,
				},
			}, nil
		},
		createStreamFunc: func(context.Context, jetstream.StreamConfig) error {
			t.Fatalf("create stream should not be called when stream exists")
			return nil
		},
		updateStreamFunc: func(_ context.Context, cfg jetstream.StreamConfig) error {
			wantSubjects := []string{"integration.alert.external_status.legacy", subject}
			if len(cfg.Subjects) != len(wantSubjects) || cfg.Subjects[0] != wantSubjects[0] || cfg.Subjects[1] != wantSubjects[1] {
				t.Fatalf("update stream subjects mismatch: got %v want %v", cfg.Subjects, wantSubjects)
			}
			if cfg.Retention != jetstream.InterestPolicy {
				t.Fatalf("retention should be preserved: got %v", cfg.Retention)
			}
			if cfg.Storage != jetstream.MemoryStorage {
				t.Fatalf("storage should be preserved: got %v", cfg.Storage)
			}
			return nil
		},
	}

	if err := ensureAlertExternalStatusSyncStream(context.Background(), manager, streamName, subject, noopCallbackStreamLogger{}); err != nil {
		t.Fatalf("ensureAlertExternalStatusSyncStream returned error: %v", err)
	}
	if manager.createCalls != 0 {
		t.Fatalf("create stream should not be called, got %d", manager.createCalls)
	}
	if manager.updateCalls != 1 {
		t.Fatalf("update stream calls mismatch: got %d want %d", manager.updateCalls, 1)
	}
}
