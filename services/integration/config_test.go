package main

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseChannels(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		raw     string
		want    []integrationChannel
		wantErr string
	}{
		{
			name: "all channels",
			raw:  "webhook,wecom,dingtalk,feishu,email,email_webhook",
			want: []integrationChannel{channelWebhook, channelWeCom, channelDingTalk, channelFeishu, channelEmail, channelEmailWebhook},
		},
		{
			name: "trim and dedupe",
			raw:  " webhook , wecom,webhook ",
			want: []integrationChannel{channelWebhook, channelWeCom},
		},
		{
			name:    "unsupported channel",
			raw:     "webhook,slack",
			wantErr: "unsupported channel",
		},
		{
			name:    "empty list",
			raw:     "  ,  ",
			wantErr: "at least one channel",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseChannels(tc.raw)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error mismatch: got %q want substring %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseChannels returned error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("channels mismatch: got %v want %v", got, tc.want)
			}
		})
	}
}

func TestRoutingModeFromEnv(t *testing.T) {

	key := "INTEGRATION_ROUTING_MODE_UNIT_TEST"
	t.Setenv(key, "")

	got, err := routingModeFromEnv(key, routingModeBroadcast)
	if err != nil {
		t.Fatalf("routingModeFromEnv returned error: %v", err)
	}
	if got != routingModeBroadcast {
		t.Fatalf("default mode mismatch: got %q want %q", got, routingModeBroadcast)
	}

	t.Setenv(key, "severity")
	got, err = routingModeFromEnv(key, routingModeBroadcast)
	if err != nil {
		t.Fatalf("routingModeFromEnv returned error: %v", err)
	}
	if got != routingModeSeverity {
		t.Fatalf("severity mode mismatch: got %q want %q", got, routingModeSeverity)
	}

	t.Setenv(key, "invalid-mode")
	_, err = routingModeFromEnv(key, routingModeBroadcast)
	if err == nil || !strings.Contains(err.Error(), "supported: broadcast,severity") {
		t.Fatalf("expected invalid mode error, got: %v", err)
	}
}

func TestLoadIntegrationConfigChannelsValidation(t *testing.T) {
	setBaseIntegrationEnvs(t)

	t.Setenv("INTEGRATION_CHANNELS", "wecom,dingtalk")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "")
	t.Setenv("INTEGRATION_WECOM_WEBHOOK_URL", "https://example.com/wecom")
	t.Setenv("INTEGRATION_DINGTALK_WEBHOOK_URL", "https://example.com/dingtalk")
	t.Setenv("INTEGRATION_FEISHU_WEBHOOK_URL", "")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}

	wantChannels := []integrationChannel{channelWeCom, channelDingTalk}
	if !reflect.DeepEqual(cfg.Channels, wantChannels) {
		t.Fatalf("enabled channels mismatch: got %v want %v", cfg.Channels, wantChannels)
	}
	if cfg.ChannelURLs[channelWeCom] != "https://example.com/wecom" {
		t.Fatalf("wecom url mismatch: got %q", cfg.ChannelURLs[channelWeCom])
	}
	if cfg.ChannelURLs[channelDingTalk] != "https://example.com/dingtalk" {
		t.Fatalf("dingtalk url mismatch: got %q", cfg.ChannelURLs[channelDingTalk])
	}
}

func TestLoadIntegrationConfigMissingEnabledChannelURL(t *testing.T) {
	setBaseIntegrationEnvs(t)

	t.Setenv("INTEGRATION_CHANNELS", "wecom")
	t.Setenv("INTEGRATION_WECOM_WEBHOOK_URL", "")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "")

	_, err := loadIntegrationConfig()
	if err == nil {
		t.Fatal("expected error when enabled channel URL is missing")
	}
	if !strings.Contains(err.Error(), "INTEGRATION_WECOM_WEBHOOK_URL") {
		t.Fatalf("error mismatch: %v", err)
	}
}

func TestLoadIntegrationConfigEmailChannelValidation(t *testing.T) {
	setBaseIntegrationEnvs(t)

	t.Setenv("INTEGRATION_CHANNELS", "email")
	t.Setenv("INTEGRATION_EMAIL_SMTP_HOST", "smtp.example.com")
	t.Setenv("INTEGRATION_EMAIL_SMTP_PORT", "587")
	t.Setenv("INTEGRATION_EMAIL_SMTP_USER", "mailer")
	t.Setenv("INTEGRATION_EMAIL_SMTP_PASS", "super-secret")
	t.Setenv("INTEGRATION_EMAIL_FROM", "alerts@example.com")
	t.Setenv("INTEGRATION_EMAIL_SMTP_TLS_MODE", "starttls")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}

	if !reflect.DeepEqual(cfg.Channels, []integrationChannel{channelEmail}) {
		t.Fatalf("email channels mismatch: got %v", cfg.Channels)
	}
	if cfg.EmailSMTPHost != "smtp.example.com" {
		t.Fatalf("email smtp host mismatch: got %q", cfg.EmailSMTPHost)
	}
	if cfg.EmailSMTPPort != 587 {
		t.Fatalf("email smtp port mismatch: got %d want %d", cfg.EmailSMTPPort, 587)
	}
	if cfg.EmailSMTPUser != "mailer" {
		t.Fatalf("email smtp user mismatch: got %q", cfg.EmailSMTPUser)
	}
	if cfg.EmailSMTPPass != "super-secret" {
		t.Fatalf("email smtp pass mismatch: got %q", cfg.EmailSMTPPass)
	}
	if cfg.EmailFrom != "alerts@example.com" {
		t.Fatalf("email from mismatch: got %q", cfg.EmailFrom)
	}
	if cfg.EmailSMTPTLSMode != smtpTLSModeSTARTTLS {
		t.Fatalf("email tls mode mismatch: got %q want %q", cfg.EmailSMTPTLSMode, smtpTLSModeSTARTTLS)
	}
}

func TestLoadIntegrationConfigMissingEmailChannelEnv(t *testing.T) {
	setBaseIntegrationEnvs(t)

	t.Setenv("INTEGRATION_CHANNELS", "email")
	t.Setenv("INTEGRATION_EMAIL_SMTP_HOST", "")
	t.Setenv("INTEGRATION_EMAIL_SMTP_USER", "mailer")
	t.Setenv("INTEGRATION_EMAIL_SMTP_PASS", "super-secret")
	t.Setenv("INTEGRATION_EMAIL_FROM", "alerts@example.com")
	t.Setenv("INTEGRATION_EMAIL_SMTP_TLS_MODE", "starttls")

	_, err := loadIntegrationConfig()
	if err == nil {
		t.Fatal("expected missing email smtp host to fail")
	}
	if !strings.Contains(err.Error(), "INTEGRATION_EMAIL_SMTP_HOST") {
		t.Fatalf("error mismatch: %v", err)
	}
}

func TestLoadIntegrationConfigEmailWebhookChannelValidation(t *testing.T) {
	setBaseIntegrationEnvs(t)

	t.Setenv("INTEGRATION_CHANNELS", "email_webhook")
	t.Setenv("INTEGRATION_EMAIL_WEBHOOK_URL", "")

	_, err := loadIntegrationConfig()
	if err == nil {
		t.Fatal("expected missing email webhook url to fail")
	}
	if !strings.Contains(err.Error(), "INTEGRATION_EMAIL_WEBHOOK_URL") {
		t.Fatalf("error mismatch: %v", err)
	}
}

func TestLoadIntegrationConfigEmailWebhookChannelRequiresEmailFrom(t *testing.T) {
	setBaseIntegrationEnvs(t)

	t.Setenv("INTEGRATION_CHANNELS", "email_webhook")
	t.Setenv("INTEGRATION_EMAIL_WEBHOOK_URL", "https://example.com/email-webhook")
	t.Setenv("INTEGRATION_EMAIL_FROM", "")

	_, err := loadIntegrationConfig()
	if err == nil {
		t.Fatal("expected missing email from to fail for email_webhook channel")
	}
	if !strings.Contains(err.Error(), "INTEGRATION_EMAIL_FROM") {
		t.Fatalf("error mismatch: %v", err)
	}
}

func TestLoadIntegrationConfigEmailWebhookChannelValid(t *testing.T) {
	setBaseIntegrationEnvs(t)

	t.Setenv("INTEGRATION_CHANNELS", "email_webhook")
	t.Setenv("INTEGRATION_EMAIL_WEBHOOK_URL", "https://example.com/email-webhook")
	t.Setenv("INTEGRATION_EMAIL_FROM", "alerts@example.com")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}
	if !reflect.DeepEqual(cfg.Channels, []integrationChannel{channelEmailWebhook}) {
		t.Fatalf("email webhook channels mismatch: got %v", cfg.Channels)
	}
	if cfg.ChannelURLs[channelEmailWebhook] != "https://example.com/email-webhook" {
		t.Fatalf("email webhook url mismatch: got %q", cfg.ChannelURLs[channelEmailWebhook])
	}
	if cfg.EmailFrom != "alerts@example.com" {
		t.Fatalf("email from mismatch: got %q", cfg.EmailFrom)
	}
}

func TestSMTPTLSModeFromEnv(t *testing.T) {
	const key = "INTEGRATION_EMAIL_SMTP_TLS_MODE_UNIT_TEST"
	t.Setenv(key, "")

	got, err := smtpTLSModeFromEnv(key, smtpTLSModeSTARTTLS)
	if err != nil {
		t.Fatalf("smtpTLSModeFromEnv returned error: %v", err)
	}
	if got != smtpTLSModeSTARTTLS {
		t.Fatalf("default smtp tls mode mismatch: got %q want %q", got, smtpTLSModeSTARTTLS)
	}

	t.Setenv(key, "tls")
	got, err = smtpTLSModeFromEnv(key, smtpTLSModeSTARTTLS)
	if err != nil {
		t.Fatalf("smtpTLSModeFromEnv returned error: %v", err)
	}
	if got != smtpTLSModeTLS {
		t.Fatalf("tls mode mismatch: got %q want %q", got, smtpTLSModeTLS)
	}

	t.Setenv(key, "broken")
	_, err = smtpTLSModeFromEnv(key, smtpTLSModeSTARTTLS)
	if err == nil || !strings.Contains(err.Error(), "supported: none,starttls,tls") {
		t.Fatalf("expected invalid smtp tls mode error, got: %v", err)
	}
}

func TestLoadIntegrationConfigCallbackDefaults(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("INTEGRATION_CHANNELS", "webhook")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}

	if cfg.CallbackStream != defaultCallbackStream {
		t.Fatalf("callback stream mismatch: got %q want %q", cfg.CallbackStream, defaultCallbackStream)
	}
	if cfg.CallbackSubject != defaultCallbackSubject {
		t.Fatalf("callback subject mismatch: got %q want %q", cfg.CallbackSubject, defaultCallbackSubject)
	}
	if cfg.CallbackDurable != defaultCallbackDurable {
		t.Fatalf("callback durable mismatch: got %q want %q", cfg.CallbackDurable, defaultCallbackDurable)
	}
	if cfg.CallbackPath != defaultCallbackPath {
		t.Fatalf("callback path mismatch: got %q want %q", cfg.CallbackPath, defaultCallbackPath)
	}
	if cfg.ControlPlaneCallbackURL != "https://control.example.com/api/v1/integrations/callbacks/alerts" {
		t.Fatalf("control plane callback url mismatch: got %q", cfg.ControlPlaneCallbackURL)
	}
	if cfg.CallbackSignatureTTL != defaultCallbackSignatureTTL {
		t.Fatalf("callback signature ttl mismatch: got %s want %s", cfg.CallbackSignatureTTL, defaultCallbackSignatureTTL)
	}
}

func TestLoadIntegrationConfigCallbackTopicAliasCompatibility(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("INTEGRATION_CHANNELS", "webhook")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
	t.Setenv("INTEGRATION_CALLBACK_SUBJECT", "")
	t.Setenv("INTEGRATION_CALLBACK_TOPIC", "integration.callback.custom")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}

	if cfg.CallbackSubject != "integration.callback.custom" {
		t.Fatalf("callback topic alias mismatch: got %q want %q", cfg.CallbackSubject, "integration.callback.custom")
	}
}

func TestLoadIntegrationConfigCallbackSubjectPrecedenceOverTopic(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("INTEGRATION_CHANNELS", "webhook")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
	t.Setenv("INTEGRATION_CALLBACK_SUBJECT", "integration.callback.from-subject")
	t.Setenv("INTEGRATION_CALLBACK_TOPIC", "integration.callback.from-topic")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}

	if cfg.CallbackSubject != "integration.callback.from-subject" {
		t.Fatalf("callback subject precedence mismatch: got %q want %q", cfg.CallbackSubject, "integration.callback.from-subject")
	}
}

func TestLoadIntegrationConfigRequiresControlPlaneBaseURL(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("CONTROL_PLANE_BASE_URL", "")

	_, err := loadIntegrationConfig()
	if err == nil {
		t.Fatal("expected missing CONTROL_PLANE_BASE_URL to fail")
	}
	if !strings.Contains(err.Error(), "CONTROL_PLANE_BASE_URL") {
		t.Fatalf("error mismatch: %v", err)
	}
}

func TestLoadIntegrationConfigCallbackPathNormalization(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("INTEGRATION_CHANNELS", "webhook")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
	t.Setenv("INTEGRATION_CALLBACK_PATH", "callbacks/custom-alert")
	t.Setenv("CONTROL_PLANE_BASE_URL", "https://control.example.com/base")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}

	if cfg.CallbackPath != "/callbacks/custom-alert" {
		t.Fatalf("callback path mismatch: got %q want %q", cfg.CallbackPath, "/callbacks/custom-alert")
	}
	if cfg.ControlPlaneCallbackURL != "https://control.example.com/callbacks/custom-alert" {
		t.Fatalf("control plane callback url mismatch: got %q", cfg.ControlPlaneCallbackURL)
	}
}

func TestLoadIntegrationConfigCallbackSignatureTTLOverride(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("INTEGRATION_CHANNELS", "webhook")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
	t.Setenv("INTEGRATION_CALLBACK_SIGNATURE_TTL", "90s")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}
	if cfg.CallbackSignatureTTL != 90*time.Second {
		t.Fatalf("callback signature ttl mismatch: got %s want %s", cfg.CallbackSignatureTTL, 90*time.Second)
	}
}

func TestLoadIntegrationConfigCallbackSignatureTTLValidation(t *testing.T) {
	testCases := []struct {
		name  string
		value string
	}{
		{
			name:  "zero ttl",
			value: "0s",
		},
		{
			name:  "negative ttl",
			value: "-1s",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			setBaseIntegrationEnvs(t)
			t.Setenv("INTEGRATION_CHANNELS", "webhook")
			t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
			t.Setenv("INTEGRATION_CALLBACK_SIGNATURE_TTL", tc.value)

			_, err := loadIntegrationConfig()
			if err == nil {
				t.Fatalf("expected callback signature ttl validation to fail for %q", tc.value)
			}
			if !strings.Contains(err.Error(), "INTEGRATION_CALLBACK_SIGNATURE_TTL") {
				t.Fatalf("error mismatch: %v", err)
			}
		})
	}
}

func TestLoadIntegrationConfigAlertDedupeOverride(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("INTEGRATION_CHANNELS", "webhook")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
	t.Setenv("INTEGRATION_ALERT_DEDUPE_WINDOW", "45s")
	t.Setenv("INTEGRATION_ALERT_DEDUPE_MAX_ENTRIES", "256")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}
	if cfg.AlertDedupeWindow != 45*time.Second {
		t.Fatalf("alert dedupe window mismatch: got %s want %s", cfg.AlertDedupeWindow, 45*time.Second)
	}
	if cfg.AlertDedupeMaxEntries != 256 {
		t.Fatalf("alert dedupe max entries mismatch: got %d want %d", cfg.AlertDedupeMaxEntries, 256)
	}
}

func TestLoadIntegrationConfigAlertDedupeValidation(t *testing.T) {
	t.Run("negative window", func(t *testing.T) {
		setBaseIntegrationEnvs(t)
		t.Setenv("INTEGRATION_CHANNELS", "webhook")
		t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
		t.Setenv("INTEGRATION_ALERT_DEDUPE_WINDOW", "-1s")

		_, err := loadIntegrationConfig()
		if err == nil {
			t.Fatal("expected alert dedupe window validation to fail")
		}
		if !strings.Contains(err.Error(), "INTEGRATION_ALERT_DEDUPE_WINDOW") {
			t.Fatalf("error mismatch: %v", err)
		}
	})

	t.Run("positive window with non-positive max entries", func(t *testing.T) {
		setBaseIntegrationEnvs(t)
		t.Setenv("INTEGRATION_CHANNELS", "webhook")
		t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
		t.Setenv("INTEGRATION_ALERT_DEDUPE_WINDOW", "30s")
		t.Setenv("INTEGRATION_ALERT_DEDUPE_MAX_ENTRIES", "0")

		_, err := loadIntegrationConfig()
		if err == nil {
			t.Fatal("expected alert dedupe max entries validation to fail")
		}
		if !strings.Contains(err.Error(), "INTEGRATION_ALERT_DEDUPE_MAX_ENTRIES") {
			t.Fatalf("error mismatch: %v", err)
		}
	})
}

func TestLoadIntegrationConfigRequiresCallbackSecretOutsideTestEnv(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("INTEGRATION_CHANNELS", "webhook")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
	t.Setenv("INTEGRATION_CALLBACK_SECRET", "")

	_, err := loadIntegrationConfig()
	if err == nil {
		t.Fatal("expected missing INTEGRATION_CALLBACK_SECRET to fail in non-test environment")
	}
	if !strings.Contains(err.Error(), "INTEGRATION_CALLBACK_SECRET") {
		t.Fatalf("error mismatch: %v", err)
	}
}

func TestLoadIntegrationConfigAllowsCallbackSecretOutsideTestEnvWhenProvided(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("INTEGRATION_CHANNELS", "webhook")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
	t.Setenv("INTEGRATION_CALLBACK_SECRET", "secret-123")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}
	if cfg.CallbackSecret != "secret-123" {
		t.Fatalf("callback secret mismatch: got %q want %q", cfg.CallbackSecret, "secret-123")
	}
}

func TestLoadIntegrationConfigAllowsEmptyCallbackSecretInTestEnv(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("APP_ENV", "test")
	t.Setenv("INTEGRATION_CHANNELS", "webhook")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
	t.Setenv("INTEGRATION_CALLBACK_SECRET", "")

	if _, err := loadIntegrationConfig(); err != nil {
		t.Fatalf("expected empty callback secret to be allowed in test environment, got: %v", err)
	}
}

func setBaseIntegrationEnvs(t *testing.T) {
	t.Helper()

	t.Setenv("SERVICE_NAME", "integration-test")
	t.Setenv("HTTP_ADDR", ":18085")
	t.Setenv("NATS_URL", "nats://127.0.0.1:4222")

	t.Setenv("INTEGRATION_STREAM", "GOVERNANCE_ALERTS")
	t.Setenv("INTEGRATION_SUBJECT", "governance.alerts")
	t.Setenv("INTEGRATION_DURABLE", "INTEGRATION_ALERTS_DISPATCHER")
	t.Setenv("INTEGRATION_CALLBACK_STREAM", "")
	t.Setenv("INTEGRATION_CALLBACK_SUBJECT", "")
	t.Setenv("INTEGRATION_CALLBACK_TOPIC", "")
	t.Setenv("INTEGRATION_CALLBACK_DURABLE", "")
	t.Setenv("INTEGRATION_WEEKLY_STREAM", "GOVERNANCE_REPORTS_WEEKLY")
	t.Setenv("INTEGRATION_WEEKLY_SUBJECT", "governance.reports.weekly")
	t.Setenv("INTEGRATION_WEEKLY_DURABLE", "INTEGRATION_WEEKLY_REPORT_DISPATCHER")
	t.Setenv("INTEGRATION_DLQ_SUBJECT", "integration.dispatch")
	t.Setenv("INTEGRATION_ROUTING_MODE", "broadcast")
	t.Setenv("CONTROL_PLANE_BASE_URL", "https://control.example.com")
	t.Setenv("INTEGRATION_EMAIL_WEBHOOK_URL", "")
	t.Setenv("INTEGRATION_EMAIL_SMTP_HOST", "")
	t.Setenv("INTEGRATION_EMAIL_SMTP_PORT", "")
	t.Setenv("INTEGRATION_EMAIL_SMTP_USER", "")
	t.Setenv("INTEGRATION_EMAIL_SMTP_PASS", "")
	t.Setenv("INTEGRATION_EMAIL_FROM", "")
	t.Setenv("INTEGRATION_EMAIL_SMTP_TLS_MODE", "")

	t.Setenv("INTEGRATION_WEBHOOK_TIMEOUT", "10s")
	t.Setenv("INTEGRATION_RETRY_MAX", "5")
	t.Setenv("INTEGRATION_RETRY_BASE_DELAY", "2s")
	t.Setenv("INTEGRATION_RETRY_MAX_DELAY", "60s")
	t.Setenv("INTEGRATION_ALERT_DEDUPE_WINDOW", "")
	t.Setenv("INTEGRATION_ALERT_DEDUPE_MAX_ENTRIES", "")
	t.Setenv("INTEGRATION_CONSUMER_ACK_WAIT", "90s")
	t.Setenv("INTEGRATION_DLQ_PUBLISH_TIMEOUT", "5s")
}
