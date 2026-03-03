package main

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestRouteChannels(t *testing.T) {
	t.Parallel()

	cfg := integrationConfig{
		Channels: []integrationChannel{channelWebhook, channelWeCom, channelDingTalk, channelFeishu},
	}
	dispatcher := &alertDispatcher{cfg: cfg}

	dispatcher.cfg.RoutingMode = routingModeBroadcast
	if got := dispatcher.routeChannels(eventTypeAlert, []byte(`{"severity":"warning"}`)); !reflect.DeepEqual(got, cfg.Channels) {
		t.Fatalf("broadcast route mismatch: got %v want %v", got, cfg.Channels)
	}

	dispatcher.cfg.RoutingMode = routingModeSeverity
	if got := dispatcher.routeChannels(eventTypeAlert, []byte(`{"severity":"critical"}`)); !reflect.DeepEqual(got, cfg.Channels) {
		t.Fatalf("critical route mismatch: got %v want %v", got, cfg.Channels)
	}

	wantWarning := []integrationChannel{channelWebhook, channelWeCom}
	if got := dispatcher.routeChannels(eventTypeAlert, []byte(`{"severity":"warning"}`)); !reflect.DeepEqual(got, wantWarning) {
		t.Fatalf("warning route mismatch: got %v want %v", got, wantWarning)
	}

	if got := dispatcher.routeChannels(eventTypeAlert, []byte(`{"severity":"info"}`)); !reflect.DeepEqual(got, cfg.Channels) {
		t.Fatalf("unknown severity route mismatch: got %v want %v", got, cfg.Channels)
	}

	if got := dispatcher.routeChannels(eventTypeWeeklyReport, []byte(`{"severity":"warning"}`)); !reflect.DeepEqual(got, cfg.Channels) {
		t.Fatalf("weekly event route mismatch: got %v want %v", got, cfg.Channels)
	}
}

func TestRouteChannelsWarningNoEnabledTarget(t *testing.T) {
	t.Parallel()

	dispatcher := &alertDispatcher{cfg: integrationConfig{
		Channels:    []integrationChannel{channelDingTalk},
		RoutingMode: routingModeSeverity,
	}}

	got := dispatcher.routeChannels(eventTypeAlert, []byte(`{"severity":"warning"}`))
	want := []integrationChannel{channelDingTalk}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected fallback to enabled channels, got %v want %v", got, want)
	}
}

func TestExtractSeverity(t *testing.T) {
	t.Parallel()

	if got := extractSeverity([]byte(`{"severity":" CRITICAL "}`)); got != "critical" {
		t.Fatalf("severity mismatch: got %q want %q", got, "critical")
	}
	if got := extractSeverity([]byte(`not-json`)); got != "" {
		t.Fatalf("invalid payload severity should be empty, got %q", got)
	}
}

func TestMultiDispatchRetryable(t *testing.T) {
	t.Parallel()

	retryableErr := &dispatchError{retryable: true, message: "temporary"}
	nonRetryableErr := &dispatchError{retryable: false, message: "bad request"}

	err := &multiDispatchError{failures: []channelDispatchFailure{
		{channel: channelWebhook, err: retryableErr},
		{channel: channelWeCom, err: retryableErr},
	}}
	if !isRetryable(err) {
		t.Fatal("expected multi dispatch error to be retryable")
	}

	err = &multiDispatchError{failures: []channelDispatchFailure{
		{channel: channelWebhook, err: retryableErr},
		{channel: channelWeCom, err: nonRetryableErr},
	}}
	if isRetryable(err) {
		t.Fatal("expected mixed multi dispatch error to be non-retryable")
	}
}

func TestDispatchToChannelRetryableByStatusCode(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		statusCode    int
		wantRetryable bool
	}{
		{name: "server error", statusCode: http.StatusInternalServerError, wantRetryable: true},
		{name: "too many requests", statusCode: http.StatusTooManyRequests, wantRetryable: true},
		{name: "bad request", statusCode: http.StatusBadRequest, wantRetryable: false},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.statusCode)
				_, _ = io.WriteString(w, "failed")
			}))
			defer server.Close()

			dispatcher := &alertDispatcher{
				log: slog.New(slog.NewTextHandler(io.Discard, nil)),
				cfg: integrationConfig{
					WebhookTimeout: defaultWebhookTimeout,
					ChannelURLs: map[integrationChannel]string{
						channelWebhook: server.URL,
					},
				},
				httpClient: server.Client(),
			}

			err := dispatcher.dispatchToChannel(channelWebhook, []byte(`{"ok":true}`))
			if err == nil {
				t.Fatal("expected error for non-2xx status")
			}

			if isRetryable(err) != tc.wantRetryable {
				t.Fatalf("retryable mismatch: got %v want %v", isRetryable(err), tc.wantRetryable)
			}
		})
	}
}

func TestDispatchWarningFallbackToEnabledChannels(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dispatcher := &alertDispatcher{
		httpClient: server.Client(),
		cfg: integrationConfig{
			Channels:       []integrationChannel{channelDingTalk},
			ChannelURLs:    map[integrationChannel]string{channelDingTalk: server.URL},
			RoutingMode:    routingModeSeverity,
			WebhookTimeout: defaultWebhookTimeout,
		},
	}

	results, err := dispatcher.dispatch([]byte(`{"severity":"warning"}`), eventTypeAlert)
	if err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected fallback dispatch result, got %d", len(results))
	}
	if results[0].channel != channelDingTalk {
		t.Fatalf("fallback dispatch channel mismatch: got %s want %s", results[0].channel, channelDingTalk)
	}
}

func TestFailedChannelsFromError(t *testing.T) {
	t.Parallel()

	err := &multiDispatchError{failures: []channelDispatchFailure{
		{channel: channelWebhook, err: errors.New("x")},
		{channel: channelWebhook, err: errors.New("y")},
		{channel: channelWeCom, err: errors.New("z")},
	}}

	got := failedChannelsFromError(err)
	want := []integrationChannel{channelWebhook, channelWeCom}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("failed channels mismatch: got %v want %v", got, want)
	}
}
