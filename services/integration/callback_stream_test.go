package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestEnsureCallbackStreamCreateWhenMissing(t *testing.T) {
	t.Parallel()

	const (
		streamName = "INTEGRATION_CALLBACK_EVENTS"
		subject    = "integration.callback.events"
	)

	manager := &fakeCallbackStreamManager{
		streamInfoFunc: func(context.Context, string) (*jetstream.StreamInfo, error) {
			return nil, jetstream.ErrStreamNotFound
		},
		createStreamFunc: func(_ context.Context, cfg jetstream.StreamConfig) error {
			if cfg.Name != streamName {
				t.Fatalf("create stream name mismatch: got %q want %q", cfg.Name, streamName)
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

	if err := ensureCallbackStream(context.Background(), manager, streamName, subject, noopCallbackStreamLogger{}); err != nil {
		t.Fatalf("ensureCallbackStream returned error: %v", err)
	}
	if manager.createCalls != 1 {
		t.Fatalf("create stream calls mismatch: got %d want %d", manager.createCalls, 1)
	}
	if manager.updateCalls != 0 {
		t.Fatalf("update stream should not be called, got %d", manager.updateCalls)
	}
}

func TestEnsureCallbackStreamNoopWhenSubjectAlreadyBound(t *testing.T) {
	t.Parallel()

	const (
		streamName = "INTEGRATION_CALLBACK_EVENTS"
		subject    = "integration.callback.events"
	)

	manager := &fakeCallbackStreamManager{
		streamInfoFunc: func(context.Context, string) (*jetstream.StreamInfo, error) {
			return &jetstream.StreamInfo{
				Config: jetstream.StreamConfig{
					Name:     streamName,
					Subjects: []string{"integration.callback.events", "integration.callback.other"},
				},
			}, nil
		},
		createStreamFunc: func(context.Context, jetstream.StreamConfig) error {
			t.Fatalf("create stream should not be called when stream already exists")
			return nil
		},
		updateStreamFunc: func(context.Context, jetstream.StreamConfig) error {
			t.Fatalf("update stream should not be called when subject is already bound")
			return nil
		},
	}

	if err := ensureCallbackStream(context.Background(), manager, streamName, subject, noopCallbackStreamLogger{}); err != nil {
		t.Fatalf("ensureCallbackStream returned error: %v", err)
	}
	if manager.createCalls != 0 {
		t.Fatalf("create stream should not be called, got %d", manager.createCalls)
	}
	if manager.updateCalls != 0 {
		t.Fatalf("update stream should not be called, got %d", manager.updateCalls)
	}
}

func TestEnsureCallbackStreamNoopWhenSubjectCoveredByWildcard(t *testing.T) {
	t.Parallel()

	const (
		streamName = "INTEGRATION_CALLBACK_EVENTS"
		subject    = "integration.callback.events"
	)

	manager := &fakeCallbackStreamManager{
		streamInfoFunc: func(context.Context, string) (*jetstream.StreamInfo, error) {
			return &jetstream.StreamInfo{
				Config: jetstream.StreamConfig{
					Name:     streamName,
					Subjects: []string{"integration.>"},
				},
			}, nil
		},
		createStreamFunc: func(context.Context, jetstream.StreamConfig) error {
			t.Fatalf("create stream should not be called when stream already exists")
			return nil
		},
		updateStreamFunc: func(context.Context, jetstream.StreamConfig) error {
			t.Fatalf("update stream should not be called when wildcard subject already covers target")
			return nil
		},
	}

	if err := ensureCallbackStream(context.Background(), manager, streamName, subject, noopCallbackStreamLogger{}); err != nil {
		t.Fatalf("ensureCallbackStream returned error: %v", err)
	}
	if manager.createCalls != 0 {
		t.Fatalf("create stream should not be called, got %d", manager.createCalls)
	}
	if manager.updateCalls != 0 {
		t.Fatalf("update stream should not be called, got %d", manager.updateCalls)
	}
}

func TestEnsureCallbackStreamAppendSubjectWithoutOverwritingConfig(t *testing.T) {
	t.Parallel()

	const (
		streamName = "INTEGRATION_CALLBACK_EVENTS"
		subject    = "integration.callback.events"
	)

	manager := &fakeCallbackStreamManager{
		streamInfoFunc: func(context.Context, string) (*jetstream.StreamInfo, error) {
			return &jetstream.StreamInfo{
				Config: jetstream.StreamConfig{
					Name:      streamName,
					Subjects:  []string{"integration.callback.legacy"},
					Retention: jetstream.InterestPolicy,
					Storage:   jetstream.MemoryStorage,
					MaxMsgs:   2048,
					MaxBytes:  1024 * 1024,
				},
			}, nil
		},
		createStreamFunc: func(context.Context, jetstream.StreamConfig) error {
			t.Fatalf("create stream should not be called when stream exists")
			return nil
		},
		updateStreamFunc: func(_ context.Context, cfg jetstream.StreamConfig) error {
			if cfg.Name != streamName {
				t.Fatalf("update stream name mismatch: got %q want %q", cfg.Name, streamName)
			}
			wantSubjects := []string{"integration.callback.legacy", subject}
			if len(cfg.Subjects) != len(wantSubjects) || cfg.Subjects[0] != wantSubjects[0] || cfg.Subjects[1] != wantSubjects[1] {
				t.Fatalf("update stream subjects mismatch: got %v want %v", cfg.Subjects, wantSubjects)
			}
			if cfg.Retention != jetstream.InterestPolicy {
				t.Fatalf("retention should be preserved: got %v", cfg.Retention)
			}
			if cfg.Storage != jetstream.MemoryStorage {
				t.Fatalf("storage should be preserved: got %v", cfg.Storage)
			}
			if cfg.MaxMsgs != 2048 {
				t.Fatalf("max msgs should be preserved: got %d", cfg.MaxMsgs)
			}
			if cfg.MaxBytes != 1024*1024 {
				t.Fatalf("max bytes should be preserved: got %d", cfg.MaxBytes)
			}
			return nil
		},
	}

	if err := ensureCallbackStream(context.Background(), manager, streamName, subject, noopCallbackStreamLogger{}); err != nil {
		t.Fatalf("ensureCallbackStream returned error: %v", err)
	}
	if manager.createCalls != 0 {
		t.Fatalf("create stream should not be called, got %d", manager.createCalls)
	}
	if manager.updateCalls != 1 {
		t.Fatalf("update stream calls mismatch: got %d want %d", manager.updateCalls, 1)
	}
}

func TestCallbackStreamSubjectMatches(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		filter  string
		subject string
		want    bool
	}{
		{
			name:    "exact match",
			filter:  "integration.callback.events",
			subject: "integration.callback.events",
			want:    true,
		},
		{
			name:    "single wildcard match",
			filter:  "integration.*.events",
			subject: "integration.callback.events",
			want:    true,
		},
		{
			name:    "tail wildcard match",
			filter:  "integration.>",
			subject: "integration.callback.events",
			want:    true,
		},
		{
			name:    "target contains wildcard should be exact only",
			filter:  "integration.>",
			subject: "integration.callback.>",
			want:    false,
		},
		{
			name:    "token count mismatch",
			filter:  "integration.*",
			subject: "integration.callback.events",
			want:    false,
		},
		{
			name:    "literal mismatch",
			filter:  "integration.alert.events",
			subject: "integration.callback.events",
			want:    false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := callbackStreamSubjectMatches(tc.filter, tc.subject); got != tc.want {
				t.Fatalf("callbackStreamSubjectMatches(%q, %q)=%v want %v", tc.filter, tc.subject, got, tc.want)
			}
		})
	}
}

func TestEnsureCallbackStreamRetryAfterConcurrentCreate(t *testing.T) {
	t.Parallel()

	const (
		streamName = "INTEGRATION_CALLBACK_EVENTS"
		subject    = "integration.callback.events"
	)

	streamInfoCalls := 0
	manager := &fakeCallbackStreamManager{
		streamInfoFunc: func(context.Context, string) (*jetstream.StreamInfo, error) {
			streamInfoCalls++
			if streamInfoCalls == 1 {
				return nil, jetstream.ErrStreamNotFound
			}
			return &jetstream.StreamInfo{
				Config: jetstream.StreamConfig{
					Name:     streamName,
					Subjects: []string{"integration.callback.legacy"},
				},
			}, nil
		},
		createStreamFunc: func(context.Context, jetstream.StreamConfig) error {
			return jetstream.ErrStreamNameAlreadyInUse
		},
		updateStreamFunc: func(_ context.Context, cfg jetstream.StreamConfig) error {
			if len(cfg.Subjects) != 2 || cfg.Subjects[1] != subject {
				t.Fatalf("update stream subjects mismatch: got %v", cfg.Subjects)
			}
			return nil
		},
	}

	if err := ensureCallbackStream(context.Background(), manager, streamName, subject, noopCallbackStreamLogger{}); err != nil {
		t.Fatalf("ensureCallbackStream returned error: %v", err)
	}
	if manager.createCalls != 1 {
		t.Fatalf("create stream calls mismatch: got %d want %d", manager.createCalls, 1)
	}
	if manager.updateCalls != 1 {
		t.Fatalf("update stream calls mismatch: got %d want %d", manager.updateCalls, 1)
	}
	if manager.streamInfoCalls != 2 {
		t.Fatalf("stream info calls mismatch: got %d want %d", manager.streamInfoCalls, 2)
	}
}

type fakeCallbackStreamManager struct {
	streamInfoFunc   func(ctx context.Context, stream string) (*jetstream.StreamInfo, error)
	createStreamFunc func(ctx context.Context, cfg jetstream.StreamConfig) error
	updateStreamFunc func(ctx context.Context, cfg jetstream.StreamConfig) error

	streamInfoCalls int
	createCalls     int
	updateCalls     int
}

func (f *fakeCallbackStreamManager) StreamInfo(ctx context.Context, stream string) (*jetstream.StreamInfo, error) {
	f.streamInfoCalls++
	if f.streamInfoFunc == nil {
		return nil, errors.New("unexpected StreamInfo call")
	}
	return f.streamInfoFunc(ctx, stream)
}

func (f *fakeCallbackStreamManager) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) error {
	f.createCalls++
	if f.createStreamFunc == nil {
		return errors.New("unexpected CreateStream call")
	}
	return f.createStreamFunc(ctx, cfg)
}

func (f *fakeCallbackStreamManager) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) error {
	f.updateCalls++
	if f.updateStreamFunc == nil {
		return errors.New("unexpected UpdateStream call")
	}
	return f.updateStreamFunc(ctx, cfg)
}

type noopCallbackStreamLogger struct{}

func (noopCallbackStreamLogger) Info(string, ...any) {}
