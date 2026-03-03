package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Config 定义了服务统一配置结构。
type Config struct {
	ServiceName string
	HTTPAddr    string
	LogLevel    string
	NATS        NATSConfig
	PG          PGConfig
	OIDC        OIDCConfig
}

// NATSConfig 定义消息总线连接参数。
type NATSConfig struct {
	URL            string
	ConnectTimeout time.Duration
	MaxReconnects  int
	ReconnectWait  time.Duration
}

// PGConfig 定义 PostgreSQL 连接与连接池参数。
type PGConfig struct {
	DatabaseURL       string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

// OIDCConfig 定义 OIDC 设备码代理相关参数。
type OIDCConfig struct {
	Issuer             string
	ClientID           string
	Audience           string
	DeviceAuthEndpoint string
	TokenEndpoint      string
	JWKSURI            string
}

// Load 从环境变量加载统一配置。
func Load(serviceName, defaultHTTPAddr string) (Config, error) {
	cfg := Config{
		ServiceName: getEnv("SERVICE_NAME", serviceName),
		HTTPAddr:    getEnv("HTTP_ADDR", defaultHTTPAddr),
		LogLevel:    strings.ToLower(getEnv("LOG_LEVEL", "info")),
		NATS: NATSConfig{
			URL:            getEnv("NATS_URL", nats.DefaultURL),
			ConnectTimeout: 5 * time.Second,
			MaxReconnects:  -1,
			ReconnectWait:  2 * time.Second,
		},
		PG: PGConfig{
			DatabaseURL:       getEnv("DATABASE_URL", ""),
			MaxConns:          8,
			MinConns:          0,
			MaxConnLifetime:   time.Hour,
			MaxConnIdleTime:   30 * time.Minute,
			HealthCheckPeriod: time.Minute,
		},
		OIDC: OIDCConfig{
			Issuer:             getEnv("OIDC_ISSUER", ""),
			ClientID:           getEnv("OIDC_CLIENT_ID", ""),
			Audience:           getEnv("OIDC_AUDIENCE", ""),
			DeviceAuthEndpoint: getEnv("OIDC_DEVICE_AUTH_ENDPOINT", ""),
			TokenEndpoint:      getEnv("OIDC_TOKEN_ENDPOINT", ""),
			JWKSURI:            getEnv("OIDC_JWKS_URI", ""),
		},
	}

	var err error
	cfg.NATS.ConnectTimeout, err = durationFromEnv("NATS_CONNECT_TIMEOUT", cfg.NATS.ConnectTimeout)
	if err != nil {
		return Config{}, err
	}

	cfg.NATS.MaxReconnects, err = intFromEnv("NATS_MAX_RECONNECTS", cfg.NATS.MaxReconnects)
	if err != nil {
		return Config{}, err
	}

	cfg.NATS.ReconnectWait, err = durationFromEnv("NATS_RECONNECT_WAIT", cfg.NATS.ReconnectWait)
	if err != nil {
		return Config{}, err
	}

	pgMaxConns, err := intFromEnv("PG_MAX_CONNS", int(cfg.PG.MaxConns))
	if err != nil {
		return Config{}, err
	}
	cfg.PG.MaxConns = int32(pgMaxConns)

	pgMinConns, err := intFromEnv("PG_MIN_CONNS", int(cfg.PG.MinConns))
	if err != nil {
		return Config{}, err
	}
	cfg.PG.MinConns = int32(pgMinConns)

	cfg.PG.MaxConnLifetime, err = durationFromEnv("PG_MAX_CONN_LIFETIME", cfg.PG.MaxConnLifetime)
	if err != nil {
		return Config{}, err
	}

	cfg.PG.MaxConnIdleTime, err = durationFromEnv("PG_MAX_CONN_IDLE_TIME", cfg.PG.MaxConnIdleTime)
	if err != nil {
		return Config{}, err
	}

	cfg.PG.HealthCheckPeriod, err = durationFromEnv("PG_HEALTH_CHECK_PERIOD", cfg.PG.HealthCheckPeriod)
	if err != nil {
		return Config{}, err
	}

	if cfg.PG.MinConns < 0 {
		return Config{}, fmt.Errorf("invalid PG_MIN_CONNS: must be >= 0")
	}
	if cfg.PG.MaxConns <= 0 {
		return Config{}, fmt.Errorf("invalid PG_MAX_CONNS: must be > 0")
	}
	if cfg.PG.MinConns > cfg.PG.MaxConns {
		return Config{}, fmt.Errorf("invalid PG pool config: PG_MIN_CONNS(%d) > PG_MAX_CONNS(%d)", cfg.PG.MinConns, cfg.PG.MaxConns)
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
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
