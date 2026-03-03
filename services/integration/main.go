package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/agentledger/agentledger/services/internal/shared/health"
	"github.com/agentledger/agentledger/services/internal/shared/logger"
)

const callbackAPIPath = "/v1/callbacks/alerts"

func main() {
	cfg, err := loadIntegrationConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config failed: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.Core.LogLevel).With("service", cfg.Core.ServiceName)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, js, err := initJetStream(cfg, log)
	if err != nil {
		log.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := nc.Drain(); err != nil {
			log.Warn("nats drain failed", "error", err)
		}
		nc.Close()
	}()

	if err := ensureCallbackStream(ctx, jetStreamCallbackStreamManager{js: js}, cfg.CallbackStream, cfg.CallbackSubject, log); err != nil {
		log.Error("ensure callback stream failed", "error", err)
		os.Exit(1)
	}

	alertsConsumer, err := ensureConsumer(ctx, js, cfg.Stream, cfg.Subject, cfg.Durable, cfg.ConsumerAckWait)
	if err != nil {
		log.Error("ensure alerts consumer failed", "error", err)
		os.Exit(1)
	}

	weeklyConsumer, err := ensureConsumer(ctx, js, cfg.WeeklyStream, cfg.WeeklySubject, cfg.WeeklyDurable, cfg.ConsumerAckWait)
	if err != nil {
		log.Error("ensure weekly report consumer failed", "error", err)
		os.Exit(1)
	}

	callbackConsumer, err := ensureConsumer(ctx, js, cfg.CallbackStream, cfg.CallbackSubject, cfg.CallbackDurable, cfg.ConsumerAckWait)
	if err != nil {
		log.Error("ensure callback consumer failed", "error", err)
		os.Exit(1)
	}

	metrics := newIntegrationMetrics()
	dispatcher := newAlertDispatcher(ctx, log, js, cfg, metrics)

	alertsConsumeCtx, err := alertsConsumer.Consume(dispatcher.handleAlertMessage)
	if err != nil {
		log.Error("start alerts consumer failed", "error", err, "consumer", cfg.Durable)
		os.Exit(1)
	}
	defer alertsConsumeCtx.Stop()

	weeklyConsumeCtx, err := weeklyConsumer.Consume(dispatcher.handleWeeklyReportMessage)
	if err != nil {
		log.Error("start weekly report consumer failed", "error", err, "consumer", cfg.WeeklyDurable)
		os.Exit(1)
	}
	defer weeklyConsumeCtx.Stop()

	callbackConsumeCtx, err := callbackConsumer.Consume(dispatcher.handleCallbackMessage)
	if err != nil {
		log.Error("start callback consumer failed", "error", err, "consumer", cfg.CallbackDurable)
		os.Exit(1)
	}
	defer callbackConsumeCtx.Stop()

	mux := http.NewServeMux()
	health.Register(mux, cfg.Core.ServiceName)
	mux.Handle("/metrics", metrics.handler())
	mux.Handle(callbackAPIPath, dispatcher.callbackHandler())
	healthErrCh := health.StartWithHandler(ctx, cfg.Core.HTTPAddr, log, mux)

	log.Info("service started",
		"http_addr", cfg.Core.HTTPAddr,
		"nats_url", cfg.Core.NATS.URL,
		"stream", cfg.Stream,
		"alerts_subject", cfg.Subject,
		"alerts_consumer", cfg.Durable,
		"weekly_stream", cfg.WeeklyStream,
		"weekly_subject", cfg.WeeklySubject,
		"weekly_consumer", cfg.WeeklyDurable,
		"callback_stream", cfg.CallbackStream,
		"callback_subject", cfg.CallbackSubject,
		"callback_consumer", cfg.CallbackDurable,
		"callback_api_path", callbackAPIPath,
		"control_plane_callback_url", cfg.ControlPlaneCallbackURL,
		"dlq_subject", cfg.DLQSubject,
		"channels", channelsToStrings(cfg.Channels),
		"routing_mode", cfg.RoutingMode,
		"webhook_host", webhookHost(cfg.ChannelURLs[channelWebhook]),
		"retry_max", cfg.RetryMax,
		"retry_base_delay", cfg.RetryBaseDelay.String(),
		"retry_max_delay", cfg.RetryMaxDelay.String(),
	)

	for {
		select {
		case <-ctx.Done():
			log.Info("service stopping", "reason", ctx.Err())
			return
		case err, ok := <-healthErrCh:
			if ok && err != nil {
				log.Error("health server failed", "error", err)
				os.Exit(1)
			}
		}
	}
}

func channelsToStrings(channels []integrationChannel) []string {
	result := make([]string, 0, len(channels))
	for _, channel := range channels {
		result = append(result, string(channel))
	}
	return result
}
