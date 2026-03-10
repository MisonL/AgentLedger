package main

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestEnsureDLQStreamCreateWhenMissing(t *testing.T) {
	t.Parallel()

	manager := &fakeCallbackStreamManager{
		streamInfoFunc: func(context.Context, string) (*jetstream.StreamInfo, error) {
			return nil, jetstream.ErrStreamNotFound
		},
		createStreamFunc: func(_ context.Context, cfg jetstream.StreamConfig) error {
			if cfg.Name != defaultDLQStream {
				t.Fatalf("create stream name mismatch: got %q want %q", cfg.Name, defaultDLQStream)
			}
			if cfg.Description != "integration dispatch dead-letter queue events" {
				t.Fatalf("create stream description mismatch: got %q", cfg.Description)
			}
			if len(cfg.Subjects) != 1 || cfg.Subjects[0] != defaultDLQSubject {
				t.Fatalf("create stream subjects mismatch: got %v want [%q]", cfg.Subjects, defaultDLQSubject)
			}
			return nil
		},
		updateStreamFunc: func(context.Context, jetstream.StreamConfig) error {
			t.Fatalf("update stream should not be called when stream is created")
			return nil
		},
	}

	if err := ensureDLQStream(
		context.Background(),
		manager,
		defaultDLQStream,
		defaultDLQSubject,
		noopCallbackStreamLogger{},
	); err != nil {
		t.Fatalf("ensureDLQStream returned error: %v", err)
	}
	if manager.createCalls != 1 {
		t.Fatalf("create stream calls mismatch: got %d want %d", manager.createCalls, 1)
	}
	if manager.updateCalls != 0 {
		t.Fatalf("update stream should not be called, got %d", manager.updateCalls)
	}
}

func TestEnsureDLQStreamAppendSubjectWithoutOverwritingConfig(t *testing.T) {
	t.Parallel()

	manager := &fakeCallbackStreamManager{
		streamInfoFunc: func(context.Context, string) (*jetstream.StreamInfo, error) {
			return &jetstream.StreamInfo{
				Config: jetstream.StreamConfig{
					Name:      defaultDLQStream,
					Subjects:  []string{"integration.dispatch.legacy"},
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
			wantSubjects := []string{"integration.dispatch.legacy", defaultDLQSubject}
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

	if err := ensureDLQStream(
		context.Background(),
		manager,
		defaultDLQStream,
		defaultDLQSubject,
		noopCallbackStreamLogger{},
	); err != nil {
		t.Fatalf("ensureDLQStream returned error: %v", err)
	}
	if manager.createCalls != 0 {
		t.Fatalf("create stream should not be called, got %d", manager.createCalls)
	}
	if manager.updateCalls != 1 {
		t.Fatalf("update stream calls mismatch: got %d want %d", manager.updateCalls, 1)
	}
}
