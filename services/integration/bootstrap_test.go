package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	sharedconfig "github.com/agentledger/agentledger/services/internal/shared/config"
	"github.com/nats-io/nats.go/jetstream"
)

func TestChannelsToStrings(t *testing.T) {
	t.Parallel()

	got := channelsToStrings([]integrationChannel{
		channelWebhook,
		channelWeCom,
		channelDingTalk,
	})
	want := []string{"webhook", "wecom", "dingtalk"}
	if len(got) != len(want) {
		t.Fatalf("channels length mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("channels item mismatch at %d: got %q want %q", i, got[i], want[i])
		}
	}

	empty := channelsToStrings(nil)
	if len(empty) != 0 {
		t.Fatalf("nil channels should convert to empty list, got %v", empty)
	}
}

func TestInitJetStreamConnectError(t *testing.T) {
	t.Parallel()

	cfg := integrationConfig{
		Core: sharedconfig.Config{
			ServiceName: "integration-test",
			NATS: sharedconfig.NATSConfig{
				URL:            "://bad-nats-url",
				ConnectTimeout: 10 * time.Millisecond,
				MaxReconnects:  0,
				ReconnectWait:  10 * time.Millisecond,
			},
		},
	}

	nc, js, err := initJetStream(cfg, noopBootstrapLogger{})
	if err == nil {
		t.Fatal("initJetStream should fail when nats url is invalid")
	}
	if !strings.Contains(err.Error(), "connect nats failed") {
		t.Fatalf("initJetStream error mismatch: %v", err)
	}
	if nc != nil {
		t.Fatalf("nats connection should be nil on connect error, got %#v", nc)
	}
	if js != nil {
		t.Fatalf("jetstream should be nil on connect error, got %#v", js)
	}
}

func TestEnsureConsumerCreateOrUpdateError(t *testing.T) {
	t.Parallel()

	js := &fakeBootstrapJetStream{
		createOrUpdateConsumerFn: func(context.Context, string, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
			return nil, errors.New("create failed")
		},
	}

	consumer, err := ensureConsumer(
		context.Background(),
		js,
		"GOVERNANCE_ALERTS",
		"governance.alerts",
		"INTEGRATION_ALERTS_DISPATCHER",
		30*time.Second,
	)
	if err == nil {
		t.Fatal("ensureConsumer should fail when CreateOrUpdateConsumer returns error")
	}
	if !strings.Contains(err.Error(), "create or update consumer failed") {
		t.Fatalf("ensureConsumer create/update error mismatch: %v", err)
	}
	if consumer != nil {
		t.Fatalf("consumer should be nil when CreateOrUpdateConsumer fails, got %#v", consumer)
	}
	if js.createOrUpdateCalls != 1 {
		t.Fatalf("CreateOrUpdateConsumer calls mismatch: got %d want %d", js.createOrUpdateCalls, 1)
	}
	if js.consumerCalls != 0 {
		t.Fatalf("Consumer should not be called when CreateOrUpdateConsumer fails, got %d", js.consumerCalls)
	}
}

func TestEnsureConsumerGetConsumerError(t *testing.T) {
	t.Parallel()

	js := &fakeBootstrapJetStream{
		createOrUpdateConsumerFn: func(context.Context, string, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
			return nil, nil
		},
		consumerFn: func(context.Context, string, string) (jetstream.Consumer, error) {
			return nil, errors.New("lookup failed")
		},
	}

	consumer, err := ensureConsumer(
		context.Background(),
		js,
		"GOVERNANCE_ALERTS",
		"governance.alerts",
		"INTEGRATION_ALERTS_DISPATCHER",
		45*time.Second,
	)
	if err == nil {
		t.Fatal("ensureConsumer should fail when Consumer returns error")
	}
	if !strings.Contains(err.Error(), "get consumer failed") {
		t.Fatalf("ensureConsumer get consumer error mismatch: %v", err)
	}
	if consumer != nil {
		t.Fatalf("consumer should be nil when Consumer fails, got %#v", consumer)
	}
	if js.createOrUpdateCalls != 1 {
		t.Fatalf("CreateOrUpdateConsumer calls mismatch: got %d want %d", js.createOrUpdateCalls, 1)
	}
	if js.consumerCalls != 1 {
		t.Fatalf("Consumer calls mismatch: got %d want %d", js.consumerCalls, 1)
	}
}

func TestEnsureConsumerSuccess(t *testing.T) {
	t.Parallel()

	wantConsumer := &fakeBootstrapConsumer{}
	js := &fakeBootstrapJetStream{
		createOrUpdateConsumerFn: func(context.Context, string, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
			return nil, nil
		},
		consumerFn: func(context.Context, string, string) (jetstream.Consumer, error) {
			return wantConsumer, nil
		},
	}

	ackWait := 90 * time.Second
	gotConsumer, err := ensureConsumer(
		context.Background(),
		js,
		"GOVERNANCE_ALERTS",
		"governance.alerts",
		"INTEGRATION_ALERTS_DISPATCHER",
		ackWait,
	)
	if err != nil {
		t.Fatalf("ensureConsumer returned unexpected error: %v", err)
	}
	if gotConsumer != wantConsumer {
		t.Fatalf("ensureConsumer consumer mismatch: got %#v want %#v", gotConsumer, wantConsumer)
	}
	if js.lastCreateOrUpdateStream != "GOVERNANCE_ALERTS" {
		t.Fatalf("CreateOrUpdateConsumer stream mismatch: got %q", js.lastCreateOrUpdateStream)
	}
	if js.lastCreateOrUpdateConfig.Durable != "INTEGRATION_ALERTS_DISPATCHER" {
		t.Fatalf("durable mismatch: got %q", js.lastCreateOrUpdateConfig.Durable)
	}
	if js.lastCreateOrUpdateConfig.FilterSubject != "governance.alerts" {
		t.Fatalf("filter subject mismatch: got %q", js.lastCreateOrUpdateConfig.FilterSubject)
	}
	if js.lastCreateOrUpdateConfig.AckPolicy != jetstream.AckExplicitPolicy {
		t.Fatalf("ack policy mismatch: got %v want %v", js.lastCreateOrUpdateConfig.AckPolicy, jetstream.AckExplicitPolicy)
	}
	if js.lastCreateOrUpdateConfig.AckWait != ackWait {
		t.Fatalf("ack wait mismatch: got %s want %s", js.lastCreateOrUpdateConfig.AckWait, ackWait)
	}
	if js.lastCreateOrUpdateConfig.MaxDeliver != -1 {
		t.Fatalf("max deliver mismatch: got %d want -1", js.lastCreateOrUpdateConfig.MaxDeliver)
	}
	if js.lastConsumerStream != "GOVERNANCE_ALERTS" {
		t.Fatalf("Consumer stream mismatch: got %q", js.lastConsumerStream)
	}
	if js.lastConsumerDurable != "INTEGRATION_ALERTS_DISPATCHER" {
		t.Fatalf("Consumer durable mismatch: got %q", js.lastConsumerDurable)
	}
}

func TestJetStreamCallbackStreamManagerStreamInfo(t *testing.T) {
	t.Parallel()

	wantInfo := &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{
			Name:     "INTEGRATION_CALLBACK_EVENTS",
			Subjects: []string{"integration.callback.events"},
		},
	}
	fakeStream := &fakeBootstrapStream{
		infoFn: func(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
			return wantInfo, nil
		},
	}
	js := &fakeBootstrapJetStream{
		streamFn: func(context.Context, string) (jetstream.Stream, error) {
			return fakeStream, nil
		},
	}
	manager := jetStreamCallbackStreamManager{js: js}

	gotInfo, err := manager.StreamInfo(context.Background(), "INTEGRATION_CALLBACK_EVENTS")
	if err != nil {
		t.Fatalf("StreamInfo returned unexpected error: %v", err)
	}
	if gotInfo != wantInfo {
		t.Fatalf("StreamInfo result mismatch: got %#v want %#v", gotInfo, wantInfo)
	}
	if js.streamCalls != 1 {
		t.Fatalf("js.Stream calls mismatch: got %d want %d", js.streamCalls, 1)
	}
	if fakeStream.infoCalls != 1 {
		t.Fatalf("stream.Info calls mismatch: got %d want %d", fakeStream.infoCalls, 1)
	}
}

func TestJetStreamCallbackStreamManagerStreamInfoError(t *testing.T) {
	t.Parallel()

	js := &fakeBootstrapJetStream{
		streamFn: func(context.Context, string) (jetstream.Stream, error) {
			return nil, errors.New("stream lookup failed")
		},
	}
	manager := jetStreamCallbackStreamManager{js: js}

	info, err := manager.StreamInfo(context.Background(), "INTEGRATION_CALLBACK_EVENTS")
	if err == nil {
		t.Fatal("StreamInfo should fail when js.Stream returns error")
	}
	if !strings.Contains(err.Error(), "stream lookup failed") {
		t.Fatalf("StreamInfo error mismatch: %v", err)
	}
	if info != nil {
		t.Fatalf("StreamInfo should return nil info on error, got %#v", info)
	}
}

func TestJetStreamCallbackStreamManagerCreateAndUpdateError(t *testing.T) {
	t.Parallel()

	js := &fakeBootstrapJetStream{
		createStreamFn: func(context.Context, jetstream.StreamConfig) (jetstream.Stream, error) {
			return nil, errors.New("create stream failed")
		},
		updateStreamFn: func(context.Context, jetstream.StreamConfig) (jetstream.Stream, error) {
			return nil, errors.New("update stream failed")
		},
	}
	manager := jetStreamCallbackStreamManager{js: js}

	createErr := manager.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     "INTEGRATION_CALLBACK_EVENTS",
		Subjects: []string{"integration.callback.events"},
	})
	if createErr == nil || !strings.Contains(createErr.Error(), "create stream failed") {
		t.Fatalf("CreateStream error mismatch: %v", createErr)
	}

	updateErr := manager.UpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     "INTEGRATION_CALLBACK_EVENTS",
		Subjects: []string{"integration.callback.events", "integration.callback.fallback"},
	})
	if updateErr == nil || !strings.Contains(updateErr.Error(), "update stream failed") {
		t.Fatalf("UpdateStream error mismatch: %v", updateErr)
	}
}

func TestDispatchErrorUnwrap(t *testing.T) {
	t.Parallel()

	var nilErr *dispatchError
	if nilErr.Unwrap() != nil {
		t.Fatalf("nil dispatchError should unwrap to nil")
	}

	rootErr := errors.New("root")
	err := &dispatchError{err: rootErr}
	if !errors.Is(err.Unwrap(), rootErr) {
		t.Fatalf("dispatchError unwrap mismatch: got %v want %v", err.Unwrap(), rootErr)
	}
}

func TestCallbackErrorToStatus(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		err  error
		want int
	}{
		{
			name: "dispatch 4xx maps to 400",
			err: &dispatchError{
				statusCode: 429,
				message:    "too many requests",
			},
			want: 400,
		},
		{
			name: "dispatch 5xx maps to 502",
			err: &dispatchError{
				statusCode: 503,
				message:    "upstream unavailable",
			},
			want: 502,
		},
		{
			name: "deadline exceeded maps to 408",
			err:  context.DeadlineExceeded,
			want: 408,
		},
		{
			name: "default maps to 502",
			err:  errors.New("unknown"),
			want: 502,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := callbackErrorToStatus(tc.err); got != tc.want {
				t.Fatalf("callbackErrorToStatus mismatch: got %d want %d", got, tc.want)
			}
		})
	}
}

func TestWaitWithContext(t *testing.T) {
	t.Parallel()

	if !waitWithContext(nil, 0) {
		t.Fatal("waitWithContext should return true for nil ctx and non-positive delay")
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitWithContext(cancelledCtx, 0) {
		t.Fatal("waitWithContext should return false when context is already canceled")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitWithContext(ctx, 50*time.Millisecond) {
		t.Fatal("waitWithContext should return false when canceled before timer")
	}

	if !waitWithContext(context.Background(), 5*time.Millisecond) {
		t.Fatal("waitWithContext should return true when timer finishes normally")
	}
}

func TestReadBodySnippet(t *testing.T) {
	t.Parallel()

	if got := readBodySnippet(nil, 16); got != "" {
		t.Fatalf("nil reader snippet mismatch: got %q want empty", got)
	}
	if got := readBodySnippet(strings.NewReader("hello"), 0); got != "" {
		t.Fatalf("non-positive limit snippet mismatch: got %q want empty", got)
	}
	if got := readBodySnippet(strings.NewReader("  hello world  "), 32); got != "hello world" {
		t.Fatalf("snippet trim mismatch: got %q want %q", got, "hello world")
	}
	if got := readBodySnippet(errorReader{err: errors.New("read failed")}, 8); got != "" {
		t.Fatalf("error reader snippet mismatch: got %q want empty", got)
	}
}

func TestExtractAttempt(t *testing.T) {
	t.Parallel()

	if got := extractAttempt(&messageKeyFallbackTestMsg{
		metadataErr: errors.New("metadata unavailable"),
	}); got != 1 {
		t.Fatalf("extractAttempt metadata error mismatch: got %d want %d", got, 1)
	}

	if got := extractAttempt(&messageKeyFallbackTestMsg{
		metadata: &jetstream.MsgMetadata{NumDelivered: 0},
	}); got != 1 {
		t.Fatalf("extractAttempt zero delivery mismatch: got %d want %d", got, 1)
	}

	if got := extractAttempt(&messageKeyFallbackTestMsg{
		metadata: &jetstream.MsgMetadata{NumDelivered: 3},
	}); got != 3 {
		t.Fatalf("extractAttempt normal delivery mismatch: got %d want %d", got, 3)
	}

	if got := extractAttempt(&messageKeyFallbackTestMsg{
		metadata: &jetstream.MsgMetadata{NumDelivered: uint64(maxInt) + 1},
	}); got != maxInt {
		t.Fatalf("extractAttempt overflow clamp mismatch: got %d want %d", got, maxInt)
	}
}

type noopBootstrapLogger struct{}

func (noopBootstrapLogger) Info(string, ...any) {}

func (noopBootstrapLogger) Warn(string, ...any) {}

type fakeBootstrapConsumer struct {
	jetstream.Consumer
}

type fakeBootstrapStream struct {
	jetstream.Stream
	infoFn    func(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error)
	infoCalls int
}

func (f *fakeBootstrapStream) Info(ctx context.Context, opts ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	f.infoCalls++
	if f.infoFn == nil {
		return nil, errors.New("unexpected Stream.Info call")
	}
	return f.infoFn(ctx, opts...)
}

type fakeBootstrapJetStream struct {
	jetstream.JetStream

	createOrUpdateConsumerFn func(context.Context, string, jetstream.ConsumerConfig) (jetstream.Consumer, error)
	consumerFn               func(context.Context, string, string) (jetstream.Consumer, error)
	streamFn                 func(context.Context, string) (jetstream.Stream, error)
	createStreamFn           func(context.Context, jetstream.StreamConfig) (jetstream.Stream, error)
	updateStreamFn           func(context.Context, jetstream.StreamConfig) (jetstream.Stream, error)

	createOrUpdateCalls int
	consumerCalls       int
	streamCalls         int
	createStreamCalls   int
	updateStreamCalls   int

	lastCreateOrUpdateStream string
	lastCreateOrUpdateConfig jetstream.ConsumerConfig
	lastConsumerStream       string
	lastConsumerDurable      string
}

func (f *fakeBootstrapJetStream) CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	f.createOrUpdateCalls++
	f.lastCreateOrUpdateStream = stream
	f.lastCreateOrUpdateConfig = cfg
	if f.createOrUpdateConsumerFn == nil {
		return nil, errors.New("unexpected CreateOrUpdateConsumer call")
	}
	return f.createOrUpdateConsumerFn(ctx, stream, cfg)
}

func (f *fakeBootstrapJetStream) Consumer(ctx context.Context, stream string, durable string) (jetstream.Consumer, error) {
	f.consumerCalls++
	f.lastConsumerStream = stream
	f.lastConsumerDurable = durable
	if f.consumerFn == nil {
		return nil, errors.New("unexpected Consumer call")
	}
	return f.consumerFn(ctx, stream, durable)
}

func (f *fakeBootstrapJetStream) Stream(ctx context.Context, stream string) (jetstream.Stream, error) {
	f.streamCalls++
	if f.streamFn == nil {
		return nil, errors.New("unexpected Stream call")
	}
	return f.streamFn(ctx, stream)
}

func (f *fakeBootstrapJetStream) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	f.createStreamCalls++
	if f.createStreamFn == nil {
		return nil, errors.New("unexpected CreateStream call")
	}
	return f.createStreamFn(ctx, cfg)
}

func (f *fakeBootstrapJetStream) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	f.updateStreamCalls++
	if f.updateStreamFn == nil {
		return nil, errors.New("unexpected UpdateStream call")
	}
	return f.updateStreamFn(ctx, cfg)
}

type errorReader struct {
	err error
}

func (r errorReader) Read(_ []byte) (int, error) {
	if r.err == nil {
		return 0, io.EOF
	}
	return 0, r.err
}
