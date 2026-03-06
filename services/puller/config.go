package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func loadPullerRuntimeConfig() (pullerRuntimeConfig, error) {
	cfg := pullerRuntimeConfig{
		PollInterval:          5 * time.Second,
		JobTimeout:            3 * time.Minute,
		JobMaxRetries:         3,
		JobRetryBaseDelay:     5 * time.Second,
		SSHTimeout:            60 * time.Second,
		IngestTimeout:         20 * time.Second,
		IngestEndpoint:        defaultIngestEndpoint,
		IngestBearer:          strings.TrimSpace(os.Getenv("PULLER_INGEST_BEARER_TOKEN")),
		AgentID:               strings.TrimSpace(os.Getenv("PULLER_AGENT_ID")),
		InternalToken:         strings.TrimSpace(os.Getenv("PULLER_INTERNAL_TOKEN")),
		ResidencyTargetRegion: strings.ToLower(strings.TrimSpace(os.Getenv("PULLER_RESIDENCY_TARGET_REGION"))),
	}

	if cfg.AgentID == "" {
		cfg.AgentID = "puller"
	}

	var err error
	cfg.PollInterval, err = durationFromEnv("PULLER_POLL_INTERVAL", cfg.PollInterval)
	if err != nil {
		return pullerRuntimeConfig{}, err
	}
	cfg.JobTimeout, err = durationFromEnv("PULLER_JOB_TIMEOUT", cfg.JobTimeout)
	if err != nil {
		return pullerRuntimeConfig{}, err
	}
	cfg.JobMaxRetries, err = intFromEnv("PULLER_JOB_MAX_RETRIES", cfg.JobMaxRetries)
	if err != nil {
		return pullerRuntimeConfig{}, err
	}
	cfg.JobRetryBaseDelay, err = durationFromEnv("PULLER_JOB_RETRY_BASE_DELAY", cfg.JobRetryBaseDelay)
	if err != nil {
		return pullerRuntimeConfig{}, err
	}
	cfg.SSHTimeout, err = durationFromEnv("PULLER_SSH_TIMEOUT", cfg.SSHTimeout)
	if err != nil {
		return pullerRuntimeConfig{}, err
	}
	cfg.IngestTimeout, err = durationFromEnv("PULLER_INGEST_TIMEOUT", cfg.IngestTimeout)
	if err != nil {
		return pullerRuntimeConfig{}, err
	}

	if endpoint := strings.TrimSpace(os.Getenv("PULLER_INGEST_ENDPOINT")); endpoint != "" {
		cfg.IngestEndpoint = endpoint
	}

	if cfg.PollInterval <= 0 {
		return pullerRuntimeConfig{}, fmt.Errorf("invalid PULLER_POLL_INTERVAL: must be > 0")
	}
	if cfg.JobTimeout <= 0 {
		return pullerRuntimeConfig{}, fmt.Errorf("invalid PULLER_JOB_TIMEOUT: must be > 0")
	}
	if cfg.JobMaxRetries < 0 {
		return pullerRuntimeConfig{}, fmt.Errorf("invalid PULLER_JOB_MAX_RETRIES: must be >= 0")
	}
	if cfg.JobRetryBaseDelay <= 0 {
		return pullerRuntimeConfig{}, fmt.Errorf("invalid PULLER_JOB_RETRY_BASE_DELAY: must be > 0")
	}
	if cfg.SSHTimeout <= 0 {
		return pullerRuntimeConfig{}, fmt.Errorf("invalid PULLER_SSH_TIMEOUT: must be > 0")
	}
	if cfg.IngestTimeout <= 0 {
		return pullerRuntimeConfig{}, fmt.Errorf("invalid PULLER_INGEST_TIMEOUT: must be > 0")
	}

	return cfg, nil
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
