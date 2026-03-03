package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/agentledger/agentledger/services/internal/shared/config"
	"github.com/agentledger/agentledger/services/internal/shared/health"
	"github.com/agentledger/agentledger/services/internal/shared/logger"
)

func main() {
	cfg, err := config.Load("puller", ":8086")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config failed: %v\n", err)
		os.Exit(1)
	}

	runtimeCfg, err := loadPullerRuntimeConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load puller config failed: %v\n", err)
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

	if err := ensurePullerSchema(ctx, pool); err != nil {
		log.Error("ensure puller schema failed", "error", err)
		os.Exit(1)
	}

	hostname, _ := os.Hostname()
	svc := &pullerService{
		log:        log,
		pool:       pool,
		httpClient: newHTTPClient(),
		runtime:    runtimeCfg,
		hostname:   hostname,
		connectors: defaultPullerConnectorRegistry,
	}

	healthErrCh := health.Start(ctx, cfg.HTTPAddr, cfg.ServiceName, log)
	log.Info(
		"service started",
		"http_addr", cfg.HTTPAddr,
		"poll_interval", runtimeCfg.PollInterval.String(),
		"job_timeout", runtimeCfg.JobTimeout.String(),
		"ingest_endpoint", runtimeCfg.IngestEndpoint,
	)

	if err := svc.pollOnce(ctx); err != nil {
		log.Error("initial poll cycle failed", "error", err)
	}

	ticker := time.NewTicker(runtimeCfg.PollInterval)
	defer ticker.Stop()

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
		case <-ticker.C:
			if err := svc.pollOnce(ctx); err != nil {
				log.Error("poll cycle failed", "error", err)
			}
		}
	}
}
