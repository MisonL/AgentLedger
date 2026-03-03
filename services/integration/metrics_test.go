package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestIntegrationMetricsObserveAndRender(t *testing.T) {
	t.Parallel()

	metrics := newIntegrationMetrics()
	metrics.observe(outcomeSuccess, string(channelWebhook), eventTypeAlert, 120*time.Millisecond)
	metrics.observe(outcomeRetry, string(channelWeCom), eventTypeWeeklyReport, 220*time.Millisecond)
	metrics.observe(outcomeSuccess, labelChannelControl, eventTypeCallback, 80*time.Millisecond)
	metrics.observe("unexpected", "random", "custom", 10*time.Millisecond)

	rendered := metrics.renderPrometheus()

	assertContains(t, rendered, "# HELP integration_dispatch_events_total")
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"success\",channel=\"webhook\",event_type=\"alert\"} 1")
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"success\",channel=\"control_plane\",event_type=\"callback_alert\"} 1")
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"retry\",channel=\"wecom\",event_type=\"weekly_report\"} 1")
	assertContains(t, rendered, "integration_dispatch_events_total{outcome=\"dlq\",channel=\"unknown\",event_type=\"unknown\"} 1")
	assertContains(t, rendered, "integration_dispatch_latency_seconds_count{outcome=\"success\",channel=\"webhook\",event_type=\"alert\"} 1")
	assertContains(t, rendered, "integration_dispatch_latency_seconds_count{outcome=\"retry\",channel=\"wecom\",event_type=\"weekly_report\"} 1")
}

func TestIntegrationMetricsHandler(t *testing.T) {
	t.Parallel()

	metrics := newIntegrationMetrics()
	metrics.observe(outcomeSuccess, string(channelWebhook), eventTypeAlert, 50*time.Millisecond)

	h := metrics.handler()

	getReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	getResp := httptest.NewRecorder()
	h.ServeHTTP(getResp, getReq)
	if getResp.Code != http.StatusOK {
		t.Fatalf("GET /metrics status mismatch: got %d want %d", getResp.Code, http.StatusOK)
	}
	if ct := getResp.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("content-type mismatch: got %q", ct)
	}
	assertContains(t, getResp.Body.String(), "integration_dispatch_events_total")

	postReq := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	postResp := httptest.NewRecorder()
	h.ServeHTTP(postResp, postReq)
	if postResp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /metrics status mismatch: got %d want %d", postResp.Code, http.StatusMethodNotAllowed)
	}
}

func assertContains(t *testing.T, value, expectedSubstring string) {
	t.Helper()
	if !strings.Contains(value, expectedSubstring) {
		t.Fatalf("expected substring %q not found in output:\n%s", expectedSubstring, value)
	}
}
