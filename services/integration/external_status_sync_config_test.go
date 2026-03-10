package main

import "testing"

func TestLoadIntegrationConfigAlertExternalStatusSyncDefaults(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("INTEGRATION_CHANNELS", "webhook")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}

	if cfg.AlertExternalStatusSyncStream != defaultAlertExternalStatusSyncStream {
		t.Fatalf(
			"alert external status sync stream mismatch: got %q want %q",
			cfg.AlertExternalStatusSyncStream,
			defaultAlertExternalStatusSyncStream,
		)
	}
	if cfg.AlertExternalStatusSyncSubject != defaultAlertExternalStatusSyncSubject {
		t.Fatalf(
			"alert external status sync subject mismatch: got %q want %q",
			cfg.AlertExternalStatusSyncSubject,
			defaultAlertExternalStatusSyncSubject,
		)
	}
	if cfg.AlertExternalStatusSyncDurable != defaultAlertExternalStatusSyncDurable {
		t.Fatalf(
			"alert external status sync durable mismatch: got %q want %q",
			cfg.AlertExternalStatusSyncDurable,
			defaultAlertExternalStatusSyncDurable,
		)
	}
}

func TestLoadIntegrationConfigAlertExternalStatusSyncOverride(t *testing.T) {
	setBaseIntegrationEnvs(t)
	t.Setenv("INTEGRATION_CHANNELS", "webhook")
	t.Setenv("INTEGRATION_WEBHOOK_URL", "https://example.com/webhook")
	t.Setenv("INTEGRATION_ALERT_EXTERNAL_STATUS_SYNC_STREAM", "INTEGRATION_ALERT_EXTERNAL_STATUS_SYNC_CUSTOM")
	t.Setenv("INTEGRATION_ALERT_EXTERNAL_STATUS_SYNC_SUBJECT", "integration.alert.external_status_sync.custom")
	t.Setenv("INTEGRATION_ALERT_EXTERNAL_STATUS_SYNC_DURABLE", "INTEGRATION_ALERT_EXTERNAL_STATUS_SYNC_CUSTOM_SINK")

	cfg, err := loadIntegrationConfig()
	if err != nil {
		t.Fatalf("loadIntegrationConfig returned error: %v", err)
	}

	if cfg.AlertExternalStatusSyncStream != "INTEGRATION_ALERT_EXTERNAL_STATUS_SYNC_CUSTOM" {
		t.Fatalf("stream override mismatch: got %q", cfg.AlertExternalStatusSyncStream)
	}
	if cfg.AlertExternalStatusSyncSubject != "integration.alert.external_status_sync.custom" {
		t.Fatalf("subject override mismatch: got %q", cfg.AlertExternalStatusSyncSubject)
	}
	if cfg.AlertExternalStatusSyncDurable != "INTEGRATION_ALERT_EXTERNAL_STATUS_SYNC_CUSTOM_SINK" {
		t.Fatalf("durable override mismatch: got %q", cfg.AlertExternalStatusSyncDurable)
	}
}
