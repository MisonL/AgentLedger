package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestDispatchMessageKeyFallbackWithoutMetadata(t *testing.T) {
	t.Parallel()

	msg := &messageKeyFallbackTestMsg{
		data:        []byte(`{"id":"evt-1","severity":"warning"}`),
		metadataErr: errors.New("metadata unavailable"),
		subject:     "governance.alerts",
		reply:       "_INBOX.reply",
		headers: nats.Header{
			"X-Trace-ID": []string{"trace-1"},
		},
	}

	alertKey := dispatchMessageKey(msg, eventTypeAlert)
	if alertKey == "" {
		t.Fatalf("fallback message key should not be empty")
	}
	if !strings.HasPrefix(alertKey, "fallback/alert/") {
		t.Fatalf("fallback message key format mismatch: %q", alertKey)
	}

	alertKeyAgain := dispatchMessageKey(msg, eventTypeAlert)
	if alertKeyAgain != alertKey {
		t.Fatalf("fallback message key should be stable for same message: got %q want %q", alertKeyAgain, alertKey)
	}

	weeklyKey := dispatchMessageKey(msg, eventTypeWeeklyReport)
	if weeklyKey == alertKey {
		t.Fatalf("different event type should generate different fallback key")
	}

	msgWithDifferentPayload := &messageKeyFallbackTestMsg{
		data:        []byte(`{"id":"evt-2","severity":"warning"}`),
		metadataErr: errors.New("metadata unavailable"),
		subject:     "governance.alerts",
		reply:       "_INBOX.reply",
		headers: nats.Header{
			"X-Trace-ID": []string{"trace-1"},
		},
	}
	if got := dispatchMessageKey(msgWithDifferentPayload, eventTypeAlert); got == alertKey {
		t.Fatalf("different payload should generate different fallback key")
	}
}

func TestDispatchMessageKeyPreferMetadata(t *testing.T) {
	t.Parallel()

	msg := &messageKeyFallbackTestMsg{
		data: []byte(`{"id":"evt-1","severity":"warning"}`),
		metadata: &jetstream.MsgMetadata{
			Stream: "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   2048,
				Consumer: 1,
			},
		},
		metadataErr: nil,
	}

	if got := dispatchMessageKey(msg, eventTypeAlert); got != "GOVERNANCE_ALERTS/2048" {
		t.Fatalf("message key should prefer metadata sequence: got %q", got)
	}
}

func TestDispatchProgressCleanupLockedEvictsOverLimit(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	store := &dispatchProgressStore{
		items: map[string]*dispatchProgressEntry{
			"oldest": {updatedAt: base},
			"middle": {updatedAt: base.Add(1 * time.Minute)},
			"newest": {updatedAt: base.Add(2 * time.Minute)},
		},
		ttl:        0,
		maxEntries: 2,
	}

	result := store.cleanupLocked(base.Add(3 * time.Minute))
	if result.evicted != 1 {
		t.Fatalf("evicted mismatch: got %d want %d", result.evicted, 1)
	}
	if len(store.items) != 2 {
		t.Fatalf("remaining items mismatch: got %d want %d", len(store.items), 2)
	}
	if _, exists := store.items["oldest"]; exists {
		t.Fatalf("oldest entry should be evicted when over max entries")
	}
	if _, exists := store.items["middle"]; !exists {
		t.Fatalf("middle entry should be retained")
	}
	if _, exists := store.items["newest"]; !exists {
		t.Fatalf("newest entry should be retained")
	}
}

func TestNewAlertDispatcherInitializesDedupeStore(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	withDedupe := newAlertDispatcher(
		context.Background(),
		logger,
		nil,
		integrationConfig{
			AlertDedupeWindow:     time.Minute,
			AlertDedupeMaxEntries: 64,
		},
		nil,
	)
	if withDedupe == nil {
		t.Fatal("newAlertDispatcher returned nil")
	}
	if withDedupe.dedupe == nil {
		t.Fatal("dedupe store should be initialized when alert dedupe window > 0")
	}
	if withDedupe.dedupe.maxEntries != 64 {
		t.Fatalf("dedupe max entries mismatch: got %d want %d", withDedupe.dedupe.maxEntries, 64)
	}

	withoutDedupe := newAlertDispatcher(
		context.Background(),
		logger,
		nil,
		integrationConfig{},
		nil,
	)
	if withoutDedupe == nil {
		t.Fatal("newAlertDispatcher returned nil")
	}
	if withoutDedupe.dedupe != nil {
		t.Fatal("dedupe store should be nil when alert dedupe window is disabled")
	}
}

func TestHandleAlertMessageDelegatesToAlertEventType(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cfg := testHandleMessageConfig([]integrationChannel{channelWebhook})
	cfg.ChannelURLs[channelWebhook] = server.URL

	dispatcher := newTestDispatcherForHandleMessage(cfg, server.Client(), &fakeDLQPublisher{}, nil)
	msg := &fakeJetStreamMsg{
		data: []byte(`{"id":"evt-wrapper","severity":"critical"}`),
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
			Stream:       "GOVERNANCE_ALERTS",
			Sequence: jetstream.SequencePair{
				Stream:   3001,
				Consumer: 1,
			},
		},
	}

	dispatcher.handleAlertMessage(msg)

	if msg.ackCalls != 1 {
		t.Fatalf("ack calls mismatch: got %d want %d", msg.ackCalls, 1)
	}
}

func TestBuildAlertDedupeFingerprint(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		payload string
		want    string
	}{
		{
			name:    "prefer dedupe key",
			payload: `{"tenant_id":" tenant-a ","dedupe_key":" alert-1 ","severity":" CRITICAL "}`,
			want:    "tenant-a|alert-1|critical",
		},
		{
			name:    "fallback to budget and stage with default tenant",
			payload: `{"budget_id":"budget-a","severity":"Warning","stage":"Escalated"}`,
			want:    "default|budget-a|warning|escalated",
		},
		{
			name:    "missing dedupe and budget returns empty",
			payload: `{"tenant_id":"tenant-a","severity":"warning"}`,
			want:    "",
		},
		{
			name:    "invalid json returns empty",
			payload: `{`,
			want:    "",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := buildAlertDedupeFingerprint([]byte(tc.payload))
			if got != tc.want {
				t.Fatalf("fingerprint mismatch: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestAlertDedupeStoreRememberOrSuppress(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)
	store := newAlertDedupeStore(2*time.Minute, 2)
	if store.maxEntries != 2 {
		t.Fatalf("max entries mismatch: got %d want %d", store.maxEntries, 2)
	}

	if suppressed := store.rememberOrSuppress("alert-a", base); suppressed {
		t.Fatal("first seen key should not be suppressed")
	}
	if suppressed := store.rememberOrSuppress("alert-a", base.Add(30*time.Second)); !suppressed {
		t.Fatal("same key within ttl should be suppressed")
	}
	if suppressed := store.rememberOrSuppress("alert-a", base.Add(3*time.Minute)); suppressed {
		t.Fatal("same key after ttl expiry should not be suppressed")
	}

	// 覆盖 now.IsZero 分支
	if suppressed := store.rememberOrSuppress("alert-zero-time", time.Time{}); suppressed {
		t.Fatal("first seen key with zero time should not be suppressed")
	}

	// 覆盖容量淘汰分支：保留最近两条
	_ = store.rememberOrSuppress("alert-b", base.Add(4*time.Minute))
	_ = store.rememberOrSuppress("alert-c", base.Add(5*time.Minute))
	if len(store.items) != 2 {
		t.Fatalf("store size mismatch after eviction: got %d want %d", len(store.items), 2)
	}
	if _, exists := store.items["alert-a"]; exists {
		t.Fatal("oldest key should be evicted when store exceeds max entries")
	}

	// 覆盖空 key / nil receiver / ttl<=0 分支
	if suppressed := store.rememberOrSuppress("", base); suppressed {
		t.Fatal("empty key should never be suppressed")
	}
	zeroTTLStore := newAlertDedupeStore(0, 2)
	if suppressed := zeroTTLStore.rememberOrSuppress("alert-z", base); suppressed {
		t.Fatal("ttl<=0 should disable suppression")
	}
	var nilStore *alertDedupeStore
	if suppressed := nilStore.rememberOrSuppress("alert-nil", base); suppressed {
		t.Fatal("nil store should never suppress")
	}
	nilStore.cleanupLocked(base)
	nilStore.evictOverflowLocked()
}

func TestNewAlertDedupeStoreUsesDefaultMaxEntries(t *testing.T) {
	t.Parallel()

	store := newAlertDedupeStore(time.Minute, 0)
	if store.maxEntries != defaultAlertDedupeMaxEntries {
		t.Fatalf("default max entries mismatch: got %d want %d", store.maxEntries, defaultAlertDedupeMaxEntries)
	}
}

func TestShouldSuppressAlert(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"tenant_id":"tenant-a","budget_id":"budget-a","severity":"warning","stage":"warning"}`)

	var nilDispatcher *alertDispatcher
	if nilDispatcher.shouldSuppressAlert(payload) {
		t.Fatal("nil dispatcher should not suppress")
	}

	dispatcher := &alertDispatcher{}
	if dispatcher.shouldSuppressAlert(payload) {
		t.Fatal("dispatcher without dedupe store should not suppress")
	}
	if dispatcher.shouldSuppressAlert([]byte(`{`)) {
		t.Fatal("invalid payload should not suppress")
	}

	dispatcher.dedupe = newAlertDedupeStore(time.Minute, 16)
	if dispatcher.shouldSuppressAlert(payload) {
		t.Fatal("first alert should not be suppressed")
	}
	if !dispatcher.shouldSuppressAlert(payload) {
		t.Fatal("duplicate alert in dedupe window should be suppressed")
	}
}

func TestRetryDecision(t *testing.T) {
	t.Parallel()

	retryableErr := &dispatchError{retryable: true, message: "temporary failure"}
	nonRetryableErr := &dispatchError{retryable: false, message: "bad request"}

	testCases := []struct {
		name       string
		err        error
		attempt    int
		maxRetries int
		wantRetry  bool
	}{
		{
			name:       "retryable and within retry budget",
			err:        retryableErr,
			attempt:    3,
			maxRetries: 5,
			wantRetry:  true,
		},
		{
			name:       "retryable and equal retry budget",
			err:        retryableErr,
			attempt:    5,
			maxRetries: 5,
			wantRetry:  true,
		},
		{
			name:       "retryable but exceeded retry budget",
			err:        retryableErr,
			attempt:    6,
			maxRetries: 5,
			wantRetry:  false,
		},
		{
			name:       "non-retryable error",
			err:        nonRetryableErr,
			attempt:    1,
			maxRetries: 5,
			wantRetry:  false,
		},
		{
			name:       "plain error defaults to non-retryable",
			err:        errors.New("plain error"),
			attempt:    1,
			maxRetries: 5,
			wantRetry:  false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := isRetryable(tc.err) && shouldRetry(tc.attempt, tc.maxRetries)
			if got != tc.wantRetry {
				t.Fatalf("retry decision mismatch: got %v, want %v", got, tc.wantRetry)
			}
		})
	}
}

type messageKeyFallbackTestMsg struct {
	data        []byte
	metadata    *jetstream.MsgMetadata
	metadataErr error
	subject     string
	reply       string
	headers     nats.Header
}

func (m *messageKeyFallbackTestMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metadataErr != nil {
		return nil, m.metadataErr
	}
	return m.metadata, nil
}

func (m *messageKeyFallbackTestMsg) Data() []byte {
	return append([]byte(nil), m.data...)
}

func (m *messageKeyFallbackTestMsg) Headers() nats.Header {
	dup := make(nats.Header, len(m.headers))
	for key, values := range m.headers {
		dup[key] = append([]string(nil), values...)
	}
	return dup
}

func (m *messageKeyFallbackTestMsg) Subject() string {
	return m.subject
}

func (m *messageKeyFallbackTestMsg) Reply() string {
	return m.reply
}

func (m *messageKeyFallbackTestMsg) Ack() error {
	return nil
}

func (m *messageKeyFallbackTestMsg) DoubleAck(_ context.Context) error {
	return nil
}

func (m *messageKeyFallbackTestMsg) Nak() error {
	return nil
}

func (m *messageKeyFallbackTestMsg) NakWithDelay(_ time.Duration) error {
	return nil
}

func (m *messageKeyFallbackTestMsg) InProgress() error {
	return nil
}

func (m *messageKeyFallbackTestMsg) Term() error {
	return nil
}

func (m *messageKeyFallbackTestMsg) TermWithReason(_ string) error {
	return nil
}

func TestBackoffDelay(t *testing.T) {
	t.Parallel()

	base := 2 * time.Second
	max := 60 * time.Second

	testCases := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 2 * time.Second},
		{attempt: 2, want: 4 * time.Second},
		{attempt: 3, want: 8 * time.Second},
		{attempt: 5, want: 32 * time.Second},
		{attempt: 6, want: 60 * time.Second},
		{attempt: 10, want: 60 * time.Second},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.want.String(), func(t *testing.T) {
			t.Parallel()

			got := backoffDelay(tc.attempt, base, max)
			if got != tc.want {
				t.Fatalf("backoff mismatch: attempt=%d got=%s want=%s", tc.attempt, got, tc.want)
			}
		})
	}
}

func TestBuildDLQPayload(t *testing.T) {
	t.Parallel()

	event := []byte(`{"alert_id":123,"severity":"critical"}`)
	failedAt := time.Date(2026, 3, 2, 8, 30, 0, 0, time.UTC)
	dispatchErr := errors.New("webhook returned status 400")

	raw, err := buildDLQPayload(event, eventTypeAlert, dispatchErr, 4, failedAt)
	if err != nil {
		t.Fatalf("buildDLQPayload returned error: %v", err)
	}

	var decoded dlqPayload
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal dlq payload failed: %v", err)
	}

	if decoded.Error != dispatchErr.Error() {
		t.Fatalf("error mismatch: got %q want %q", decoded.Error, dispatchErr.Error())
	}
	if decoded.Attempt != 4 {
		t.Fatalf("attempt mismatch: got %d want %d", decoded.Attempt, 4)
	}
	if decoded.EventType != eventTypeAlert {
		t.Fatalf("event_type mismatch: got %q want %q", decoded.EventType, eventTypeAlert)
	}
	if !decoded.FailedAt.Equal(failedAt) {
		t.Fatalf("failed_at mismatch: got %s want %s", decoded.FailedAt, failedAt)
	}
	if string(decoded.Event) != string(event) {
		t.Fatalf("event mismatch: got %s want %s", decoded.Event, event)
	}
	if decoded.EventRaw != "" {
		t.Fatalf("event_raw should be empty for valid json event")
	}
}
