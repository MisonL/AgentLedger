package main

import (
	"bytes"
	"net/http"
	"testing"
	"time"
)

func TestIntegrationE2EAlertExternalStatusSyncRoutesByExternalType(t *testing.T) {
	env := newIntegrationE2EEnv(t)
	incidentProbe := newIntegrationE2EProbe(t, http.StatusNoContent)
	ticketProbe := newIntegrationE2EProbe(t, http.StatusNoContent)

	cfg := newIntegrationE2EConfig(channelIncident, channelTicket)
	cfg.ChannelURLs[channelIncident] = incidentProbe.server.URL
	cfg.ChannelURLs[channelTicket] = ticketProbe.server.URL
	cfg.AlertExternalStatusSyncStream = defaultAlertExternalStatusSyncStream
	cfg.AlertExternalStatusSyncSubject = defaultAlertExternalStatusSyncSubject
	cfg.AlertExternalStatusSyncDurable = defaultAlertExternalStatusSyncDurable

	env.ensureStream(
		t,
		cfg.AlertExternalStatusSyncStream,
		cfg.AlertExternalStatusSyncSubject,
	)
	dispatcher := newAlertDispatcher(env.ctx, env.log, env.js, cfg, nil)
	dispatcher.httpClient = &http.Client{Timeout: time.Second}
	env.startConsumer(
		t,
		cfg,
		cfg.AlertExternalStatusSyncStream,
		cfg.AlertExternalStatusSyncSubject,
		cfg.AlertExternalStatusSyncDurable,
		dispatcher.handleAlertExternalStatusSyncMessage,
	)

	payload := []byte(`{"callback_id":"sync-e2e-1","tenant_id":"tenant-e2e","action":"upsert_external_link","alert_id":"alert-e2e","external_type":"ticket","external_id":"ticket-1","external_status":"resolved","metadata":{"source":"control-plane"}}`)
	env.publish(t, cfg.AlertExternalStatusSyncSubject, payload)

	request := ticketProbe.waitForRequest(t)
	if !bytes.Equal(request.Body, payload) {
		t.Fatalf("ticket sync payload mismatch: got %s want %s", string(request.Body), string(payload))
	}
	incidentProbe.assertNoRequest(t)
}
