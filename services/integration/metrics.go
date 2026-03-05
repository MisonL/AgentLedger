package main

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	outcomeSuccess   = "success"
	outcomeRetry     = "retry"
	outcomeDLQ       = "dlq"
	outcomeDLQFailed = "dlq_failed"

	labelChannelUnknown = "unknown"
	eventTypeUnknown    = "unknown"
)

var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type metricLabelKey struct {
	outcome   string
	channel   string
	eventType string
}

type histogramValue struct {
	buckets []uint64
	sum     float64
	count   uint64
}

type integrationMetrics struct {
	mu       sync.Mutex
	outcomes map[metricLabelKey]uint64
	latency  map[metricLabelKey]*histogramValue
}

func newIntegrationMetrics() *integrationMetrics {
	return &integrationMetrics{
		outcomes: make(map[metricLabelKey]uint64),
		latency:  make(map[metricLabelKey]*histogramValue),
	}
}

func (m *integrationMetrics) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(m.renderPrometheus()))
	})
}

func (m *integrationMetrics) observe(outcome, channel, eventType string, duration time.Duration) {
	if m == nil {
		return
	}

	key := metricLabelKey{
		outcome:   normalizeOutcome(outcome),
		channel:   normalizeChannel(channel),
		eventType: normalizeEventType(eventType),
	}

	seconds := duration.Seconds()
	if seconds < 0 {
		seconds = 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.outcomes[key]++

	value, ok := m.latency[key]
	if !ok {
		value = &histogramValue{buckets: make([]uint64, len(latencyBuckets)+1)}
		m.latency[key] = value
	}
	value.sum += seconds
	value.count++

	bucketIdx := len(latencyBuckets)
	for i, upperBound := range latencyBuckets {
		if seconds <= upperBound {
			bucketIdx = i
			break
		}
	}
	value.buckets[bucketIdx]++
}

func (m *integrationMetrics) renderPrometheus() string {
	if m == nil {
		return ""
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	keys := make([]metricLabelKey, 0, len(m.outcomes)+len(m.latency))
	seen := make(map[metricLabelKey]struct{}, len(m.outcomes)+len(m.latency))
	for key := range m.outcomes {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range m.latency {
		if _, ok := seen[key]; ok {
			continue
		}
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].outcome != keys[j].outcome {
			return keys[i].outcome < keys[j].outcome
		}
		if keys[i].channel != keys[j].channel {
			return keys[i].channel < keys[j].channel
		}
		return keys[i].eventType < keys[j].eventType
	})

	var b strings.Builder
	b.WriteString("# HELP integration_dispatch_events_total Integration dispatch events by outcome, channel, and event type.\n")
	b.WriteString("# TYPE integration_dispatch_events_total counter\n")
	for _, key := range keys {
		value := m.outcomes[key]
		b.WriteString("integration_dispatch_events_total")
		b.WriteString(formatLabels(key))
		b.WriteString(" ")
		b.WriteString(strconv.FormatUint(value, 10))
		b.WriteByte('\n')
	}

	b.WriteString("# HELP integration_dispatch_latency_seconds Integration dispatch latency by outcome, channel, and event type.\n")
	b.WriteString("# TYPE integration_dispatch_latency_seconds histogram\n")
	for _, key := range keys {
		hist, ok := m.latency[key]
		if !ok {
			hist = &histogramValue{buckets: make([]uint64, len(latencyBuckets)+1)}
		}

		cumulative := uint64(0)
		for i, upperBound := range latencyBuckets {
			cumulative += hist.buckets[i]
			b.WriteString("integration_dispatch_latency_seconds_bucket")
			b.WriteString(formatLabelsWithLE(key, trimFloat(upperBound)))
			b.WriteString(" ")
			b.WriteString(strconv.FormatUint(cumulative, 10))
			b.WriteByte('\n')
		}

		cumulative += hist.buckets[len(latencyBuckets)]
		b.WriteString("integration_dispatch_latency_seconds_bucket")
		b.WriteString(formatLabelsWithLE(key, "+Inf"))
		b.WriteString(" ")
		b.WriteString(strconv.FormatUint(cumulative, 10))
		b.WriteByte('\n')

		b.WriteString("integration_dispatch_latency_seconds_sum")
		b.WriteString(formatLabels(key))
		b.WriteString(" ")
		b.WriteString(trimFloat(hist.sum))
		b.WriteByte('\n')

		b.WriteString("integration_dispatch_latency_seconds_count")
		b.WriteString(formatLabels(key))
		b.WriteString(" ")
		b.WriteString(strconv.FormatUint(hist.count, 10))
		b.WriteByte('\n')
	}

	return b.String()
}

func formatLabels(key metricLabelKey) string {
	return fmt.Sprintf("{outcome=%q,channel=%q,event_type=%q}",
		escapeLabelValue(key.outcome),
		escapeLabelValue(key.channel),
		escapeLabelValue(key.eventType),
	)
}

func formatLabelsWithLE(key metricLabelKey, le string) string {
	return fmt.Sprintf("{outcome=%q,channel=%q,event_type=%q,le=%q}",
		escapeLabelValue(key.outcome),
		escapeLabelValue(key.channel),
		escapeLabelValue(key.eventType),
		escapeLabelValue(le),
	)
}

func escapeLabelValue(value string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"")
	return replacer.Replace(value)
}

func trimFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func normalizeOutcome(outcome string) string {
	switch outcome {
	case outcomeSuccess, outcomeRetry, outcomeDLQ, outcomeDLQFailed:
		return outcome
	default:
		return outcomeDLQ
	}
}

func normalizeChannel(channel string) string {
	switch channel {
	case string(channelWebhook), string(channelWeCom), string(channelDingTalk), string(channelFeishu), string(channelEmail), string(channelEmailWebhook), string(channelTicket), labelChannelNone, labelChannelControl:
		return channel
	default:
		return labelChannelUnknown
	}
}

func normalizeEventType(eventType string) string {
	switch eventType {
	case eventTypeAlert, eventTypeWeeklyReport, eventTypeCallback:
		return eventType
	default:
		return eventTypeUnknown
	}
}
