package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/agentledger/agentledger/services/internal/shared/config"
)

const (
	defaultIntegrationHTTPAddr    = ":8085"
	defaultAlertsStream           = "GOVERNANCE_ALERTS"
	defaultWeeklyStream           = "GOVERNANCE_REPORTS_WEEKLY"
	defaultAlertsSubject          = "governance.alerts"
	defaultAlertsDurable          = "INTEGRATION_ALERTS_DISPATCHER"
	defaultCallbackStream         = "INTEGRATION_CALLBACK_EVENTS"
	defaultCallbackSubject        = "integration.callback.events"
	defaultCallbackDurable        = "INTEGRATION_CALLBACK_EVENT_SINK"
	defaultCallbackPath           = "/api/v1/integrations/callbacks/alerts"
	defaultWeeklySubject          = "governance.reports.weekly"
	defaultWeeklyDurable          = "INTEGRATION_WEEKLY_REPORT_DISPATCHER"
	defaultDLQSubject             = "integration.dispatch"
	defaultWebhookTimeout         = 10 * time.Second
	defaultRetryMax               = 5
	defaultRetryBaseDelay         = 2 * time.Second
	defaultRetryMaxDelay          = 60 * time.Second
	defaultConsumerAckWait        = 90 * time.Second
	defaultDLQPublishTimeout      = 5 * time.Second
	defaultIntegrationServiceName = "integration"
	defaultIntegrationChannels    = "webhook"
)

type integrationChannel string

const (
	channelWebhook  integrationChannel = "webhook"
	channelWeCom    integrationChannel = "wecom"
	channelDingTalk integrationChannel = "dingtalk"
	channelFeishu   integrationChannel = "feishu"
)

type routingMode string

const (
	routingModeBroadcast routingMode = "broadcast"
	routingModeSeverity  routingMode = "severity"
)

type integrationConfig struct {
	Core config.Config

	Stream  string
	Subject string
	Durable string

	CallbackStream  string
	CallbackSubject string
	CallbackDurable string

	WeeklyStream  string
	WeeklySubject string
	WeeklyDurable string

	DLQSubject string

	ControlPlaneBaseURL     string
	CallbackPath            string
	CallbackSecret          string
	ControlPlaneCallbackURL string

	Channels       []integrationChannel
	ChannelURLs    map[integrationChannel]string
	RoutingMode    routingMode
	WebhookTimeout time.Duration

	RetryMax       int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	ConsumerAckWait   time.Duration
	DLQPublishTimeout time.Duration
}

func loadIntegrationConfig() (integrationConfig, error) {
	coreCfg, err := config.Load(defaultIntegrationServiceName, defaultIntegrationHTTPAddr)
	if err != nil {
		return integrationConfig{}, err
	}

	callbackSubject := firstNonEmptyEnv("INTEGRATION_CALLBACK_SUBJECT", "INTEGRATION_CALLBACK_TOPIC")
	if callbackSubject == "" {
		callbackSubject = defaultCallbackSubject
	}

	cfg := integrationConfig{
		Core: coreCfg,

		Stream:  getEnv("INTEGRATION_STREAM", defaultAlertsStream),
		Subject: getEnv("INTEGRATION_SUBJECT", defaultAlertsSubject),
		Durable: getEnv("INTEGRATION_DURABLE", defaultAlertsDurable),

		CallbackStream:  getEnv("INTEGRATION_CALLBACK_STREAM", defaultCallbackStream),
		CallbackSubject: callbackSubject,
		CallbackDurable: getEnv("INTEGRATION_CALLBACK_DURABLE", defaultCallbackDurable),

		WeeklyStream:  getEnv("INTEGRATION_WEEKLY_STREAM", defaultWeeklyStream),
		WeeklySubject: getEnv("INTEGRATION_WEEKLY_SUBJECT", defaultWeeklySubject),
		WeeklyDurable: getEnv("INTEGRATION_WEEKLY_DURABLE", defaultWeeklyDurable),

		DLQSubject: getEnv("INTEGRATION_DLQ_SUBJECT", defaultDLQSubject),

		ControlPlaneBaseURL: getEnv("CONTROL_PLANE_BASE_URL", ""),
		CallbackPath:        getEnv("INTEGRATION_CALLBACK_PATH", defaultCallbackPath),
		CallbackSecret:      strings.TrimSpace(os.Getenv("INTEGRATION_CALLBACK_SECRET")),

		ChannelURLs: map[integrationChannel]string{
			channelWebhook:  getEnv("INTEGRATION_WEBHOOK_URL", ""),
			channelWeCom:    getEnv("INTEGRATION_WECOM_WEBHOOK_URL", ""),
			channelDingTalk: getEnv("INTEGRATION_DINGTALK_WEBHOOK_URL", ""),
			channelFeishu:   getEnv("INTEGRATION_FEISHU_WEBHOOK_URL", ""),
		},

		WebhookTimeout:    defaultWebhookTimeout,
		RetryMax:          defaultRetryMax,
		RetryBaseDelay:    defaultRetryBaseDelay,
		RetryMaxDelay:     defaultRetryMaxDelay,
		ConsumerAckWait:   defaultConsumerAckWait,
		DLQPublishTimeout: defaultDLQPublishTimeout,
	}

	cfg.Channels, err = channelsFromEnv("INTEGRATION_CHANNELS", defaultIntegrationChannels)
	if err != nil {
		return integrationConfig{}, err
	}

	cfg.RoutingMode, err = routingModeFromEnv("INTEGRATION_ROUTING_MODE", routingModeBroadcast)
	if err != nil {
		return integrationConfig{}, err
	}

	cfg.WebhookTimeout, err = durationFromEnv("INTEGRATION_WEBHOOK_TIMEOUT", cfg.WebhookTimeout)
	if err != nil {
		return integrationConfig{}, err
	}

	cfg.RetryMax, err = intFromEnv("INTEGRATION_RETRY_MAX", cfg.RetryMax)
	if err != nil {
		return integrationConfig{}, err
	}

	cfg.RetryBaseDelay, err = durationFromEnv("INTEGRATION_RETRY_BASE_DELAY", cfg.RetryBaseDelay)
	if err != nil {
		return integrationConfig{}, err
	}

	cfg.RetryMaxDelay, err = durationFromEnv("INTEGRATION_RETRY_MAX_DELAY", cfg.RetryMaxDelay)
	if err != nil {
		return integrationConfig{}, err
	}

	cfg.ConsumerAckWait, err = durationFromEnv("INTEGRATION_CONSUMER_ACK_WAIT", cfg.ConsumerAckWait)
	if err != nil {
		return integrationConfig{}, err
	}

	cfg.DLQPublishTimeout, err = durationFromEnv("INTEGRATION_DLQ_PUBLISH_TIMEOUT", cfg.DLQPublishTimeout)
	if err != nil {
		return integrationConfig{}, err
	}

	if cfg.Stream == "" {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_STREAM is required")
	}
	if cfg.Subject == "" {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_SUBJECT is required")
	}
	if cfg.Durable == "" {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_DURABLE is required")
	}
	if cfg.CallbackStream == "" {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_CALLBACK_STREAM is required")
	}
	if cfg.CallbackSubject == "" {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_CALLBACK_SUBJECT or INTEGRATION_CALLBACK_TOPIC is required")
	}
	if cfg.CallbackDurable == "" {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_CALLBACK_DURABLE is required")
	}
	if cfg.WeeklyStream == "" {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_WEEKLY_STREAM is required")
	}
	if cfg.WeeklySubject == "" {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_WEEKLY_SUBJECT is required")
	}
	if cfg.WeeklyDurable == "" {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_WEEKLY_DURABLE is required")
	}
	if cfg.DLQSubject == "" {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_DLQ_SUBJECT is required")
	}
	if cfg.ControlPlaneBaseURL == "" {
		return integrationConfig{}, fmt.Errorf("CONTROL_PLANE_BASE_URL is required")
	}
	if err := validateWebhookURL(cfg.ControlPlaneBaseURL); err != nil {
		return integrationConfig{}, fmt.Errorf("invalid CONTROL_PLANE_BASE_URL: %w", err)
	}
	callbackPath, err := normalizeCallbackPath(cfg.CallbackPath)
	if err != nil {
		return integrationConfig{}, fmt.Errorf("invalid INTEGRATION_CALLBACK_PATH: %w", err)
	}
	cfg.CallbackPath = callbackPath
	cfg.ControlPlaneCallbackURL, err = buildControlPlaneCallbackURL(cfg.ControlPlaneBaseURL, cfg.CallbackPath)
	if err != nil {
		return integrationConfig{}, fmt.Errorf("build control-plane callback url failed: %w", err)
	}
	if cfg.CallbackSecret == "" && !isIntegrationTestEnvironment() {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_CALLBACK_SECRET is required outside test environment")
	}

	if len(cfg.Channels) == 0 {
		return integrationConfig{}, fmt.Errorf("INTEGRATION_CHANNELS must contain at least one channel")
	}
	for _, channel := range cfg.Channels {
		rawURL := strings.TrimSpace(cfg.ChannelURLs[channel])
		envKey := channelEnvKey(channel)
		if rawURL == "" {
			return integrationConfig{}, fmt.Errorf("%s is required when channel %q is enabled", envKey, channel)
		}
		if err := validateWebhookURL(rawURL); err != nil {
			return integrationConfig{}, fmt.Errorf("invalid %s: %w", envKey, err)
		}
		cfg.ChannelURLs[channel] = rawURL
	}

	if cfg.RetryMax < 0 {
		return integrationConfig{}, fmt.Errorf("invalid INTEGRATION_RETRY_MAX: must be >= 0")
	}
	if cfg.RetryBaseDelay <= 0 {
		return integrationConfig{}, fmt.Errorf("invalid INTEGRATION_RETRY_BASE_DELAY: must be > 0")
	}
	if cfg.RetryMaxDelay <= 0 {
		return integrationConfig{}, fmt.Errorf("invalid INTEGRATION_RETRY_MAX_DELAY: must be > 0")
	}
	if cfg.RetryMaxDelay < cfg.RetryBaseDelay {
		return integrationConfig{}, fmt.Errorf("invalid retry delay config: INTEGRATION_RETRY_MAX_DELAY(%s) < INTEGRATION_RETRY_BASE_DELAY(%s)", cfg.RetryMaxDelay, cfg.RetryBaseDelay)
	}
	if cfg.WebhookTimeout <= 0 {
		return integrationConfig{}, fmt.Errorf("invalid INTEGRATION_WEBHOOK_TIMEOUT: must be > 0")
	}
	if cfg.ConsumerAckWait <= 0 {
		return integrationConfig{}, fmt.Errorf("invalid INTEGRATION_CONSUMER_ACK_WAIT: must be > 0")
	}
	if cfg.DLQPublishTimeout <= 0 {
		return integrationConfig{}, fmt.Errorf("invalid INTEGRATION_DLQ_PUBLISH_TIMEOUT: must be > 0")
	}

	return cfg, nil
}

func validateWebhookURL(raw string) error {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil {
		return err
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		return nil
	default:
		return fmt.Errorf("scheme must be http or https")
	}
}

func normalizeCallbackPath(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		value = defaultCallbackPath
	}

	parsed, err := url.Parse(value)
	if err != nil {
		return "", err
	}
	if parsed.IsAbs() || parsed.Host != "" {
		return "", fmt.Errorf("must be a relative path")
	}
	if parsed.Path == "" {
		return "", fmt.Errorf("path cannot be empty")
	}
	if !strings.HasPrefix(parsed.Path, "/") {
		parsed.Path = "/" + parsed.Path
	}
	return parsed.String(), nil
}

func buildControlPlaneCallbackURL(baseURL, callbackPath string) (string, error) {
	base := strings.TrimSpace(baseURL)
	if base == "" {
		return "", fmt.Errorf("base url is empty")
	}

	baseParsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	if baseParsed.Scheme == "" || baseParsed.Host == "" {
		return "", fmt.Errorf("base url must include scheme and host")
	}

	path, err := normalizeCallbackPath(callbackPath)
	if err != nil {
		return "", err
	}

	relativePath, err := url.Parse(path)
	if err != nil {
		return "", err
	}

	return baseParsed.ResolveReference(relativePath).String(), nil
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		value := strings.TrimSpace(os.Getenv(key))
		if value != "" {
			return value
		}
	}
	return ""
}

func intFromEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return parsed, nil
}

func durationFromEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return parsed, nil
}

func webhookHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return parsed.Host
}

func channelsFromEnv(key string, fallback string) ([]integrationChannel, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		value = fallback
	}
	return parseChannels(value)
}

func parseChannels(raw string) ([]integrationChannel, error) {
	parts := strings.Split(raw, ",")
	channels := make([]integrationChannel, 0, len(parts))
	seen := make(map[integrationChannel]struct{}, len(parts))

	for _, part := range parts {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" {
			continue
		}

		channel, ok := channelFromString(name)
		if !ok {
			return nil, fmt.Errorf("invalid INTEGRATION_CHANNELS: unsupported channel %q", name)
		}

		if _, exists := seen[channel]; exists {
			continue
		}
		seen[channel] = struct{}{}
		channels = append(channels, channel)
	}

	if len(channels) == 0 {
		return nil, fmt.Errorf("invalid INTEGRATION_CHANNELS: at least one channel is required")
	}

	return channels, nil
}

func channelFromString(raw string) (integrationChannel, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(channelWebhook):
		return channelWebhook, true
	case string(channelWeCom):
		return channelWeCom, true
	case string(channelDingTalk):
		return channelDingTalk, true
	case string(channelFeishu):
		return channelFeishu, true
	default:
		return "", false
	}
}

func routingModeFromEnv(key string, fallback routingMode) (routingMode, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	switch routingMode(strings.ToLower(value)) {
	case routingModeBroadcast:
		return routingModeBroadcast, nil
	case routingModeSeverity:
		return routingModeSeverity, nil
	default:
		return "", fmt.Errorf("invalid %s: %q (supported: broadcast,severity)", key, value)
	}
}

func channelEnvKey(channel integrationChannel) string {
	switch channel {
	case channelWebhook:
		return "INTEGRATION_WEBHOOK_URL"
	case channelWeCom:
		return "INTEGRATION_WECOM_WEBHOOK_URL"
	case channelDingTalk:
		return "INTEGRATION_DINGTALK_WEBHOOK_URL"
	case channelFeishu:
		return "INTEGRATION_FEISHU_WEBHOOK_URL"
	default:
		return "INTEGRATION_WEBHOOK_URL"
	}
}

func isIntegrationTestEnvironment() bool {
	for _, key := range []string{"APP_ENV", "GO_ENV", "ENV"} {
		value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
		if value == "" {
			continue
		}
		return value == "test" || value == "testing"
	}

	if len(os.Args) > 0 {
		bin := strings.ToLower(strings.TrimSpace(os.Args[0]))
		if strings.HasSuffix(bin, ".test") {
			return true
		}
	}
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-test.") {
			return true
		}
	}

	return false
}
