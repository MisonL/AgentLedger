package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/agentledger/agentledger/services/internal/shared/config"
	"github.com/agentledger/agentledger/services/internal/shared/health"
	"github.com/agentledger/agentledger/services/internal/shared/logger"
)

func main() {
	cfg, err := config.Load("analytics", ":8083")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config failed: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.LogLevel).With("service", cfg.ServiceName)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := initPGPool(ctx, cfg)
	if err != nil {
		log.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	weekStartWeekday, err := loadAnalyticsWeekStartWeekday()
	if err != nil {
		log.Error("load analytics week start weekday failed", "error", err)
		os.Exit(1)
	}

	svc := newAnalyticsService(log, pool, weekStartWeekday)
	mux := http.NewServeMux()
	health.Register(mux, cfg.ServiceName)
	mux.HandleFunc("/v1/usage/heatmap", svc.handleHeatmap)
	mux.HandleFunc("/v1/usage/weekly-summary", svc.handleWeeklySummary)

	httpErrCh := health.StartWithHandler(ctx, cfg.HTTPAddr, log, mux)
	log.Info("service started",
		"http_addr", cfg.HTTPAddr,
		"week_start_weekday", weekStartWeekday.String(),
	)

	for {
		select {
		case <-ctx.Done():
			log.Info("service stopping", "reason", ctx.Err())
			return
		case err, ok := <-httpErrCh:
			if ok && err != nil {
				log.Error("http server failed", "error", err)
				os.Exit(1)
			}
		}
	}
}
