package main

import (
	"encoding/json"
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
		Channels: []integrationChannel{channelWebhook, channelWeCom, channelDingTalk, channelFeishu, channelEmail, channelEmailWebhook, channelTicket},
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

	wantWarning := []integrationChannel{channelWebhook, channelWeCom, channelEmail, channelEmailWebhook, channelTicket}
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

func TestRouteChannelsWarningIncludesEmailWhenEnabled(t *testing.T) {
	t.Parallel()

	dispatcher := &alertDispatcher{cfg: integrationConfig{
		Channels:    []integrationChannel{channelWebhook, channelEmail},
		RoutingMode: routingModeSeverity,
	}}

	got := dispatcher.routeChannels(eventTypeAlert, []byte(`{"severity":"warning"}`))
	want := []integrationChannel{channelWebhook, channelEmail}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("warning route should include email when enabled: got %v want %v", got, want)
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

func TestRouteChannelsUsesOrchestrationOverride(t *testing.T) {
	t.Parallel()

	dispatcher := &alertDispatcher{cfg: integrationConfig{
		Channels:    []integrationChannel{channelWebhook, channelWeCom, channelEmail},
		RoutingMode: routingModeSeverity,
	}}

	got := dispatcher.routeChannels(eventTypeAlert, []byte(`{"severity":"warning","orchestration":{"channels":["email","wecom"],"fallback":false}}`))
	want := []integrationChannel{channelWeCom, channelEmail}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("orchestration override mismatch: got %v want %v", got, want)
	}
}

func TestBuildTicketWebhookPayloadAcceptsNumericAlertID(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"alert_id":501,"tenant_id":"tenant-numeric","budget_id":"budget-numeric","source_id":"source-numeric","rule_id":"rule-numeric","severity":"critical","status":"open","occurred_at":"2026-03-05T03:04:05Z"}`)
	raw, err := buildTicketWebhookPayload(payload, eventTypeAlert)
	if err != nil {
		t.Fatalf("buildTicketWebhookPayload returned error: %v", err)
	}

	var wrapped ticketWebhookChannelPayload
	if err := json.Unmarshal(raw, &wrapped); err != nil {
		t.Fatalf("unmarshal wrapped ticket payload failed: %v", err)
	}
	if wrapped.Context.AlertID != "501" {
		t.Fatalf("alert id mismatch: got %q want %q", wrapped.Context.AlertID, "501")
	}
	if wrapped.Context.TenantID != "tenant-numeric" {
		t.Fatalf("tenant id mismatch: got %q want %q", wrapped.Context.TenantID, "tenant-numeric")
	}
	if wrapped.Status != "open" {
		t.Fatalf("status mismatch: got %q want %q", wrapped.Status, "open")
	}
	if wrapped.Severity != "critical" {
		t.Fatalf("severity mismatch: got %q want %q", wrapped.Severity, "critical")
	}
}

func TestRouteChannelsSuppressedByOrchestration(t *testing.T) {
	t.Parallel()

	dispatcher := &alertDispatcher{cfg: integrationConfig{
		Channels:    []integrationChannel{channelWebhook, channelWeCom},
		RoutingMode: routingModeBroadcast,
	}}

	got := dispatcher.routeChannels(eventTypeAlert, []byte(`{"orchestration":{"channels":[],"suppressed":true}}`))
	if len(got) != 0 {
		t.Fatalf("suppressed orchestration should route to no channels, got %v", got)
	}
}

func TestRouteChannelsFallbackToLegacyWhenRequested(t *testing.T) {
	t.Parallel()

	dispatcher := &alertDispatcher{cfg: integrationConfig{
		Channels:    []integrationChannel{channelWebhook, channelWeCom, channelEmail},
		RoutingMode: routingModeSeverity,
	}}

	got := dispatcher.routeChannels(eventTypeAlert, []byte(`{"severity":"warning","orchestration":{"channels":[],"fallback":true}}`))
	want := []integrationChannel{channelWebhook, channelWeCom, channelEmail}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback orchestration should reuse legacy routing: got %v want %v", got, want)
	}
}

func TestRouteChannelsWeeklyUsesOrchestrationOverride(t *testing.T) {
	t.Parallel()

	dispatcher := &alertDispatcher{cfg: integrationConfig{
		Channels:    []integrationChannel{channelWebhook, channelWeCom, channelEmail},
		RoutingMode: routingModeBroadcast,
	}}

	got := dispatcher.routeChannels(eventTypeWeeklyReport, []byte(`{"report_id":"weekly-1","orchestration":{"channels":["email"],"fallback":false}}`))
	want := []integrationChannel{channelEmail}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("weekly orchestration override mismatch: got %v want %v", got, want)
	}
}

func TestRouteChannelsNoDispatchWhenOrchestrationEmptyWithoutFlags(t *testing.T) {
	t.Parallel()

	dispatcher := &alertDispatcher{cfg: integrationConfig{
		Channels:    []integrationChannel{channelWebhook, channelWeCom},
		RoutingMode: routingModeBroadcast,
	}}

	got := dispatcher.routeChannels(eventTypeAlert, []byte(`{"severity":"warning","orchestration":{"channels":[],"fallback":false}}`))
	if len(got) != 0 {
		t.Fatalf("empty orchestration without fallback/dedupe/suppressed should route to no channels, got %v", got)
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

			err := dispatcher.dispatchToChannel("", channelWebhook, []byte(`{"ok":true}`), eventTypeAlert)
			if err == nil {
				t.Fatal("expected error for non-2xx status")
			}

			if isRetryable(err) != tc.wantRetryable {
				t.Fatalf("retryable mismatch: got %v want %v", isRetryable(err), tc.wantRetryable)
			}
		})
	}
}

func TestDispatchToChannelPayloadAdaptation(t *testing.T) {
	t.Parallel()

	alertPayload := []byte("{\n  \"id\": \"alert-1\",\n  \"severity\": \"critical\"\n}")
	weeklyPayload := []byte("{\n  \"report_id\": \"weekly-1\",\n  \"tenant_id\": \"tenant-a\",\n  \"week_start\": \"2026-03-02T00:00:00Z\",\n  \"week_end\": \"2026-03-09T00:00:00Z\",\n  \"tokens\": 3100,\n  \"cost\": 6.35,\n  \"peak_day_date\": \"2026-03-04\",\n  \"peak_day_tokens\": 1900,\n  \"peak_day_cost\": 4.245\n}")
	alertText := "[agentledger][alert]\n{\"id\":\"alert-1\",\"severity\":\"critical\"}"
	weeklyText := "[agentledger][weekly_report]\nreport_id=weekly-1 tenant_id=tenant-a week_start=2026-03-02T00:00:00Z week_end=2026-03-09T00:00:00Z tokens=3100 cost=6.35 peak_day_date=2026-03-04 peak_day_tokens=1900 peak_day_cost=4.245"

	testCases := []struct {
		name      string
		channel   integrationChannel
		eventType string
		payload   []byte
		wantText  string
	}{
		{
			name:      "webhook alert keeps raw payload",
			channel:   channelWebhook,
			eventType: eventTypeAlert,
			payload:   alertPayload,
			wantText:  string(alertPayload),
		},
		{
			name:      "webhook weekly keeps raw payload",
			channel:   channelWebhook,
			eventType: eventTypeWeeklyReport,
			payload:   weeklyPayload,
			wantText:  string(weeklyPayload),
		},
		{
			name:      "wecom uses text payload for alert",
			channel:   channelWeCom,
			eventType: eventTypeAlert,
			payload:   alertPayload,
			wantText:  alertText,
		},
		{
			name:      "wecom uses text payload for weekly report",
			channel:   channelWeCom,
			eventType: eventTypeWeeklyReport,
			payload:   weeklyPayload,
			wantText:  weeklyText,
		},
		{
			name:      "dingtalk uses text payload for alert",
			channel:   channelDingTalk,
			eventType: eventTypeAlert,
			payload:   alertPayload,
			wantText:  alertText,
		},
		{
			name:      "dingtalk uses text payload for weekly report",
			channel:   channelDingTalk,
			eventType: eventTypeWeeklyReport,
			payload:   weeklyPayload,
			wantText:  weeklyText,
		},
		{
			name:      "feishu uses text payload for alert",
			channel:   channelFeishu,
			eventType: eventTypeAlert,
			payload:   alertPayload,
			wantText:  alertText,
		},
		{
			name:      "feishu uses text payload for weekly report",
			channel:   channelFeishu,
			eventType: eventTypeWeeklyReport,
			payload:   weeklyPayload,
			wantText:  weeklyText,
		},
		{
			name:      "email webhook uses wrapped payload for alert",
			channel:   channelEmailWebhook,
			eventType: eventTypeAlert,
			payload:   alertPayload,
			wantText:  alertText,
		},
		{
			name:      "ticket uses wrapped payload for alert",
			channel:   channelTicket,
			eventType: eventTypeAlert,
			payload:   alertPayload,
			wantText:  alertText,
		},
		{
			name:      "ticket uses wrapped payload for weekly report",
			channel:   channelTicket,
			eventType: eventTypeWeeklyReport,
			payload:   weeklyPayload,
			wantText:  weeklyText,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var gotContentType string
			var gotBody []byte

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotContentType = r.Header.Get("Content-Type")
				body, _ := io.ReadAll(r.Body)
				gotBody = append([]byte(nil), body...)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			dispatcher := &alertDispatcher{
				log: slog.New(slog.NewTextHandler(io.Discard, nil)),
				cfg: integrationConfig{
					WebhookTimeout: defaultWebhookTimeout,
					EmailFrom:      "alerts@example.com",
					ChannelURLs: map[integrationChannel]string{
						tc.channel: server.URL,
					},
				},
				httpClient: server.Client(),
			}

			if err := dispatcher.dispatchToChannel("", tc.channel, tc.payload, tc.eventType); err != nil {
				t.Fatalf("dispatchToChannel returned error: %v", err)
			}
			if gotContentType != "application/json" {
				t.Fatalf("content type mismatch: got %q want %q", gotContentType, "application/json")
			}

			switch tc.channel {
			case channelWebhook:
				if string(gotBody) != tc.wantText {
					t.Fatalf("webhook payload mismatch:\ngot:  %s\nwant: %s", gotBody, tc.wantText)
				}
			case channelWeCom, channelDingTalk:
				var parsed struct {
					MsgType string `json:"msgtype"`
					Text    struct {
						Content string `json:"content"`
					} `json:"text"`
				}
				if err := json.Unmarshal(gotBody, &parsed); err != nil {
					t.Fatalf("unmarshal text payload failed: %v", err)
				}
				if parsed.MsgType != "text" {
					t.Fatalf("msgtype mismatch: got %q want %q", parsed.MsgType, "text")
				}
				if parsed.Text.Content != tc.wantText {
					t.Fatalf("text content mismatch:\ngot:  %s\nwant: %s", parsed.Text.Content, tc.wantText)
				}
			case channelFeishu:
				var parsed struct {
					MsgType string `json:"msg_type"`
					Content struct {
						Text string `json:"text"`
					} `json:"content"`
				}
				if err := json.Unmarshal(gotBody, &parsed); err != nil {
					t.Fatalf("unmarshal feishu payload failed: %v", err)
				}
				if parsed.MsgType != "text" {
					t.Fatalf("msg_type mismatch: got %q want %q", parsed.MsgType, "text")
				}
				if parsed.Content.Text != tc.wantText {
					t.Fatalf("text content mismatch:\ngot:  %s\nwant: %s", parsed.Content.Text, tc.wantText)
				}
			case channelEmailWebhook:
				var parsed emailWebhookChannelPayload
				if err := json.Unmarshal(gotBody, &parsed); err != nil {
					t.Fatalf("unmarshal email webhook payload failed: %v", err)
				}
				if parsed.EventType != eventTypeAlert {
					t.Fatalf("event type mismatch: got %q want %q", parsed.EventType, eventTypeAlert)
				}
				if parsed.Subject != buildEmailSubject(tc.payload, tc.eventType) {
					t.Fatalf("subject mismatch: got %q want %q", parsed.Subject, buildEmailSubject(tc.payload, tc.eventType))
				}
				if parsed.From != "alerts@example.com" {
					t.Fatalf("from mismatch: got %q want %q", parsed.From, "alerts@example.com")
				}
				if !reflect.DeepEqual(parsed.To, []string{"alerts@example.com"}) {
					t.Fatalf("to mismatch: got %v want %v", parsed.To, []string{"alerts@example.com"})
				}
				if parsed.Body != tc.wantText {
					t.Fatalf("body mismatch:\ngot:  %s\nwant: %s", parsed.Body, tc.wantText)
				}
				if compactJSONPayload(parsed.Event) != compactJSONPayload(tc.payload) {
					t.Fatalf("event payload mismatch: got %s want %s", parsed.Event, tc.payload)
				}
			case channelTicket:
				var parsed ticketWebhookChannelPayload
				if err := json.Unmarshal(gotBody, &parsed); err != nil {
					t.Fatalf("unmarshal ticket webhook payload failed: %v", err)
				}
				if parsed.EventType != normalizeEventTypeLabel(tc.eventType) {
					t.Fatalf("event type mismatch: got %q want %q", parsed.EventType, normalizeEventTypeLabel(tc.eventType))
				}
				if parsed.Title != buildEmailSubject(tc.payload, tc.eventType) {
					t.Fatalf("title mismatch: got %q want %q", parsed.Title, buildEmailSubject(tc.payload, tc.eventType))
				}
				if parsed.Summary != tc.wantText {
					t.Fatalf("summary mismatch:\ngot:  %s\nwant: %s", parsed.Summary, tc.wantText)
				}
				if compactJSONPayload(parsed.Event) != compactJSONPayload(tc.payload) {
					t.Fatalf("event payload mismatch: got %s want %s", parsed.Event, tc.payload)
				}
				if parsed.OccurredAt.IsZero() {
					t.Fatal("occurred_at should not be zero")
				}
				if tc.eventType == eventTypeAlert {
					if parsed.Context.AlertID != "alert-1" {
						t.Fatalf("alert context id mismatch: got %q want %q", parsed.Context.AlertID, "alert-1")
					}
					if parsed.Severity != "critical" {
						t.Fatalf("severity mismatch: got %q want %q", parsed.Severity, "critical")
					}
				}
				if tc.eventType == eventTypeWeeklyReport {
					if parsed.Context.ReportID != "weekly-1" {
						t.Fatalf("report context id mismatch: got %q want %q", parsed.Context.ReportID, "weekly-1")
					}
					if parsed.Context.TenantID != "tenant-a" {
						t.Fatalf("tenant context mismatch: got %q want %q", parsed.Context.TenantID, "tenant-a")
					}
				}
			default:
				t.Fatalf("unexpected channel %s", tc.channel)
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
