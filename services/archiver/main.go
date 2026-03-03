package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/agentledger/agentledger/services/internal/shared/config"
	"github.com/agentledger/agentledger/services/internal/shared/health"
	"github.com/agentledger/agentledger/services/internal/shared/ingest"
	"github.com/agentledger/agentledger/services/internal/shared/logger"
)

const (
	archiveStreamName      = "ARCHIVE_ENQUEUE"
	archiveSubjectName     = "archive.enqueue"
	archiverConsumerName   = "ARCHIVER_DURABLE"
	defaultArchiveHTTPAddr = ":8086"
	defaultLocalArchiveDir = "/data/agentledger/archive/raw"
	defaultObjectPrefix    = "agentledger/archive/raw"
	objectContentType      = "application/x-ndjson"
)

type archiveMode string

const (
	archiveModeLocal  archiveMode = "local"
	archiveModeObject archiveMode = "object"
	archiveModeHybrid archiveMode = "hybrid"
)

type archiveConfig struct {
	Mode               archiveMode
	LocalRoot          string
	ObjectProvider     string
	ObjectBucket       string
	ObjectPrefix       string
	S3Region           string
	OSSEndpoint        string
	OSSAccessKeyID     string
	OSSAccessKeySecret string
	OSSSecurityToken   string
}

type archiveEnqueueMessage struct {
	Tenant     string          `json:"tenant,omitempty"`
	TenantID   string          `json:"tenant_id,omitempty"`
	Source     string          `json:"source,omitempty"`
	SourceID   string          `json:"source_id,omitempty"`
	Session    string          `json:"session,omitempty"`
	SessionID  string          `json:"session_id,omitempty"`
	Event      string          `json:"event,omitempty"`
	EventID    string          `json:"event_id,omitempty"`
	Time       string          `json:"time,omitempty"`
	Timestamp  string          `json:"timestamp,omitempty"`
	Raw        json.RawMessage `json:"raw,omitempty"`
	RawPayload json.RawMessage `json:"raw_payload,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Metadata   map[string]any  `json:"metadata,omitempty"`
}

type archiveJob struct {
	Tenant     string
	Source     string
	Session    string
	Event      string
	OccurredAt time.Time
	RawPayload []byte
	Payload    json.RawMessage
	Metadata   map[string]any
}

type archiveObjectRecord struct {
	Backend    string
	ObjectKey  string
	Checksum   string
	SizeBytes  int64
	ArchivedAt time.Time
}

type objectArchiveWriter interface {
	PutObject(ctx context.Context, key string, content []byte) error
}

type localArchiveWriter struct {
	rootDir string
}

type s3ArchiveWriter struct {
	bucket string
	client *awss3.Client
}

type ossArchiveWriter struct {
	bucket *oss.Bucket
}

type archiverService struct {
	log           *slog.Logger
	pool          *pgxpool.Pool
	mode          archiveMode
	localWriter   *localArchiveWriter
	objectWriter  objectArchiveWriter
	objectBackend string
	objectPrefix  string
}

var invalidPathCharPattern = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

func main() {
	cfg, err := config.Load("archiver", defaultArchiveHTTPAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config failed: %v\n", err)
		os.Exit(1)
	}

	archiveCfg, err := loadArchiveConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load archive config failed: %v\n", err)
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

	if err := ensureArchiveObjectsTable(ctx, pool); err != nil {
		log.Error("ensure archive_objects table failed", "error", err)
		os.Exit(1)
	}

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

	consumer, err := ensureArchiveConsumer(ctx, js)
	if err != nil {
		log.Error("ensure archive consumer failed", "error", err)
		os.Exit(1)
	}

	svc, err := newArchiverService(ctx, log, pool, archiveCfg)
	if err != nil {
		log.Error("init archiver service failed", "error", err)
		os.Exit(1)
	}

	consumeCtx, err := consumer.Consume(svc.handleMessage)
	if err != nil {
		log.Error("start consumer failed", "error", err, "consumer", archiverConsumerName)
		os.Exit(1)
	}
	defer consumeCtx.Stop()

	healthErrCh := health.Start(ctx, cfg.HTTPAddr, cfg.ServiceName, log)
	log.Info("service started",
		"http_addr", cfg.HTTPAddr,
		"nats_url", cfg.NATS.URL,
		"consumer", archiverConsumerName,
		"subject", archiveSubjectName,
		"mode", archiveCfg.Mode,
		"local_root", archiveCfg.LocalRoot,
		"object_provider", archiveCfg.ObjectProvider,
		"object_bucket", archiveCfg.ObjectBucket,
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

func loadArchiveConfig() (archiveConfig, error) {
	mode, err := parseArchiveMode(os.Getenv("ARCHIVE_MODE"))
	if err != nil {
		return archiveConfig{}, err
	}

	cfg := archiveConfig{
		Mode:               mode,
		LocalRoot:          firstNonEmpty(strings.TrimSpace(os.Getenv("ARCHIVE_LOCAL_ROOT")), defaultLocalArchiveDir),
		ObjectProvider:     strings.ToLower(strings.TrimSpace(os.Getenv("ARCHIVE_OBJECT_PROVIDER"))),
		ObjectBucket:       strings.TrimSpace(os.Getenv("ARCHIVE_OBJECT_BUCKET")),
		ObjectPrefix:       strings.Trim(strings.TrimSpace(firstNonEmpty(os.Getenv("ARCHIVE_OBJECT_PREFIX"), defaultObjectPrefix)), "/"),
		S3Region:           strings.TrimSpace(firstNonEmpty(os.Getenv("ARCHIVE_S3_REGION"), os.Getenv("AWS_REGION"))),
		OSSEndpoint:        strings.TrimSpace(os.Getenv("ARCHIVE_OSS_ENDPOINT")),
		OSSAccessKeyID:     strings.TrimSpace(firstNonEmpty(os.Getenv("ARCHIVE_OSS_ACCESS_KEY_ID"), os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"))),
		OSSAccessKeySecret: strings.TrimSpace(firstNonEmpty(os.Getenv("ARCHIVE_OSS_ACCESS_KEY_SECRET"), os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"))),
		OSSSecurityToken:   strings.TrimSpace(firstNonEmpty(os.Getenv("ARCHIVE_OSS_SECURITY_TOKEN"), os.Getenv("ALIBABA_CLOUD_SECURITY_TOKEN"))),
	}

	if cfg.Mode == archiveModeLocal {
		return cfg, nil
	}
	if cfg.ObjectProvider == "" {
		return archiveConfig{}, fmt.Errorf("ARCHIVE_OBJECT_PROVIDER is required for %s mode", cfg.Mode)
	}
	if cfg.ObjectBucket == "" {
		return archiveConfig{}, fmt.Errorf("ARCHIVE_OBJECT_BUCKET is required for %s mode", cfg.Mode)
	}

	switch cfg.ObjectProvider {
	case "s3":
		if cfg.S3Region == "" {
			return archiveConfig{}, fmt.Errorf("ARCHIVE_S3_REGION (or AWS_REGION) is required for s3 provider")
		}
	case "oss":
		if cfg.OSSEndpoint == "" {
			return archiveConfig{}, fmt.Errorf("ARCHIVE_OSS_ENDPOINT is required for oss provider")
		}
		if cfg.OSSAccessKeyID == "" || cfg.OSSAccessKeySecret == "" {
			return archiveConfig{}, fmt.Errorf("ARCHIVE_OSS_ACCESS_KEY_ID and ARCHIVE_OSS_ACCESS_KEY_SECRET are required for oss provider")
		}
	default:
		return archiveConfig{}, fmt.Errorf("invalid ARCHIVE_OBJECT_PROVIDER: %s", cfg.ObjectProvider)
	}

	return cfg, nil
}

func parseArchiveMode(raw string) (archiveMode, error) {
	mode := archiveMode(strings.ToLower(strings.TrimSpace(raw)))
	if mode == "" {
		return archiveModeLocal, nil
	}
	switch mode {
	case archiveModeLocal, archiveModeObject, archiveModeHybrid:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid ARCHIVE_MODE: %s", raw)
	}
}

func newArchiverService(
	ctx context.Context,
	log *slog.Logger,
	pool *pgxpool.Pool,
	cfg archiveConfig,
) (*archiverService, error) {
	svc := &archiverService{
		log:          log,
		pool:         pool,
		mode:         cfg.Mode,
		objectPrefix: strings.Trim(cfg.ObjectPrefix, "/"),
	}

	if cfg.Mode == archiveModeLocal || cfg.Mode == archiveModeHybrid {
		svc.localWriter = &localArchiveWriter{rootDir: cfg.LocalRoot}
	}

	if cfg.Mode == archiveModeObject || cfg.Mode == archiveModeHybrid {
		switch cfg.ObjectProvider {
		case "s3":
			writer, err := newS3ArchiveWriter(ctx, cfg)
			if err != nil {
				return nil, err
			}
			svc.objectWriter = writer
			svc.objectBackend = "s3"
		case "oss":
			writer, err := newOSSArchiveWriter(cfg)
			if err != nil {
				return nil, err
			}
			svc.objectWriter = writer
			svc.objectBackend = "oss"
		default:
			return nil, fmt.Errorf("unsupported object provider: %s", cfg.ObjectProvider)
		}
	}

	if svc.localWriter == nil && svc.objectWriter == nil {
		return nil, fmt.Errorf("no archive backend configured")
	}

	return svc, nil
}

func newS3ArchiveWriter(ctx context.Context, cfg archiveConfig) (*s3ArchiveWriter, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.S3Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config failed: %w", err)
	}

	return &s3ArchiveWriter{
		bucket: cfg.ObjectBucket,
		client: awss3.NewFromConfig(awsCfg),
	}, nil
}

func newOSSArchiveWriter(cfg archiveConfig) (*ossArchiveWriter, error) {
	options := make([]oss.ClientOption, 0, 1)
	if cfg.OSSSecurityToken != "" {
		options = append(options, oss.SecurityToken(cfg.OSSSecurityToken))
	}

	client, err := oss.New(cfg.OSSEndpoint, cfg.OSSAccessKeyID, cfg.OSSAccessKeySecret, options...)
	if err != nil {
		return nil, fmt.Errorf("create oss client failed: %w", err)
	}

	bucket, err := client.Bucket(cfg.ObjectBucket)
	if err != nil {
		return nil, fmt.Errorf("get oss bucket failed: %w", err)
	}

	return &ossArchiveWriter{bucket: bucket}, nil
}

func (s *archiverService) handleMessage(msg jetstream.Msg) {
	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	job, err := decodeArchiveMessage(msg.Data())
	if err != nil {
		s.log.Error("decode archive enqueue message failed", "error", err, "payload_size", len(msg.Data()))
		if termErr := msg.Term(); termErr != nil {
			s.log.Warn("term message failed", "error", termErr)
		}
		return
	}

	records, err := s.archive(ctx, job)
	if err != nil {
		s.log.Error("archive write failed",
			"error", err,
			"tenant", job.Tenant,
			"source", job.Source,
			"session", job.Session,
			"event", job.Event,
		)
		if nakErr := msg.Nak(); nakErr != nil {
			s.log.Warn("nak message failed", "error", nakErr)
		}
		return
	}

	if err := s.persistArchiveObjects(ctx, job, records); err != nil {
		s.log.Error("persist archive objects failed",
			"error", err,
			"tenant", job.Tenant,
			"source", job.Source,
			"session", job.Session,
			"event", job.Event,
		)
		if nakErr := msg.Nak(); nakErr != nil {
			s.log.Warn("nak message failed", "error", nakErr)
		}
		return
	}

	if ackErr := msg.Ack(); ackErr != nil {
		s.log.Warn("ack message failed", "error", ackErr, "event", job.Event)
		return
	}

	s.log.Info("archive message processed",
		"tenant", job.Tenant,
		"source", job.Source,
		"session", job.Session,
		"event", job.Event,
		"mode", s.mode,
		"backend_count", len(records),
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)
}

func decodeArchiveMessage(raw []byte) (archiveJob, error) {
	var message archiveEnqueueMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return archiveJob{}, fmt.Errorf("decode json failed: %w", err)
	}

	tenant := firstNonEmpty(strings.TrimSpace(message.Tenant), strings.TrimSpace(message.TenantID), "default")
	source := firstNonEmpty(strings.TrimSpace(message.Source), strings.TrimSpace(message.SourceID))
	session := firstNonEmpty(strings.TrimSpace(message.Session), strings.TrimSpace(message.SessionID))
	event := firstNonEmpty(strings.TrimSpace(message.Event), strings.TrimSpace(message.EventID))
	timestamp := firstNonEmpty(strings.TrimSpace(message.Time), strings.TrimSpace(message.Timestamp))

	if source == "" {
		return archiveJob{}, fmt.Errorf("missing source")
	}
	if session == "" {
		return archiveJob{}, fmt.Errorf("missing session")
	}
	if event == "" {
		return archiveJob{}, fmt.Errorf("missing event")
	}
	if timestamp == "" {
		return archiveJob{}, fmt.Errorf("missing time")
	}

	occurredAt, err := ingest.ParseTimestamp(timestamp)
	if err != nil {
		return archiveJob{}, fmt.Errorf("invalid time: %w", err)
	}

	rawPayload := bytes.TrimSpace(message.Raw)
	if len(rawPayload) == 0 {
		rawPayload = bytes.TrimSpace(message.RawPayload)
	}
	if len(rawPayload) == 0 {
		return archiveJob{}, fmt.Errorf("missing raw payload")
	}
	if !json.Valid(rawPayload) {
		return archiveJob{}, fmt.Errorf("raw payload is not valid json")
	}

	payload := bytes.TrimSpace(message.Payload)
	if len(payload) > 0 && !json.Valid(payload) {
		return archiveJob{}, fmt.Errorf("payload is not valid json")
	}

	metadata := message.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}

	return archiveJob{
		Tenant:     tenant,
		Source:     source,
		Session:    session,
		Event:      event,
		OccurredAt: occurredAt.UTC(),
		RawPayload: append([]byte(nil), rawPayload...),
		Payload:    append(json.RawMessage(nil), payload...),
		Metadata:   metadata,
	}, nil
}

func (s *archiverService) archive(ctx context.Context, job archiveJob) ([]archiveObjectRecord, error) {
	checksum := sha256Hex(job.RawPayload)
	relativeKey := buildArchiveRelativeKey(job.OccurredAt, job.Tenant, job.Source, job.Session, job.Event, checksum)
	line := buildJSONLLine(job.RawPayload)
	archivedAt := time.Now().UTC()

	records := make([]archiveObjectRecord, 0, 2)
	if s.localWriter != nil {
		localPath, err := s.localWriter.WriteAtomic(ctx, relativeKey, line)
		if err != nil {
			return nil, fmt.Errorf("write local archive failed: %w", err)
		}
		records = append(records, archiveObjectRecord{
			Backend:    "local",
			ObjectKey:  localPath,
			Checksum:   checksum,
			SizeBytes:  int64(len(line)),
			ArchivedAt: archivedAt,
		})
	}

	if s.objectWriter != nil {
		objectKey := joinObjectKey(s.objectPrefix, relativeKey)
		if err := s.objectWriter.PutObject(ctx, objectKey, line); err != nil {
			return nil, fmt.Errorf("write object archive failed: %w", err)
		}
		records = append(records, archiveObjectRecord{
			Backend:    s.objectBackend,
			ObjectKey:  objectKey,
			Checksum:   checksum,
			SizeBytes:  int64(len(line)),
			ArchivedAt: archivedAt,
		})
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no archive backend configured")
	}
	return records, nil
}

func buildJSONLLine(payload []byte) []byte {
	trimmed := bytes.TrimSpace(payload)
	line := make([]byte, len(trimmed)+1)
	copy(line, trimmed)
	line[len(trimmed)] = '\n'
	return line
}

func (s *archiverService) persistArchiveObjects(
	ctx context.Context,
	job archiveJob,
	records []archiveObjectRecord,
) error {
	metadata := make(map[string]any, len(job.Metadata)+1)
	for key, value := range job.Metadata {
		metadata[key] = value
	}
	metadata["mode"] = s.mode
	if len(job.Payload) > 0 {
		metadata["payload"] = job.Payload
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata failed: %w", err)
	}

	for _, record := range records {
		_, err := s.pool.Exec(ctx, `
INSERT INTO archive_objects (
  tenant_id,
  source_id,
  session_id,
  event_id,
  occurred_at,
  backend,
  object_key,
  checksum,
  size_bytes,
  archived_at,
  metadata,
  created_at,
  updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, NOW(), NOW())
ON CONFLICT (backend, object_key) DO NOTHING
`,
			job.Tenant,
			job.Source,
			job.Session,
			job.Event,
			job.OccurredAt,
			record.Backend,
			record.ObjectKey,
			record.Checksum,
			record.SizeBytes,
			record.ArchivedAt,
			metadataJSON,
		)
		if err != nil {
			return fmt.Errorf("insert archive_objects failed: %w", err)
		}
	}

	return nil
}

func buildArchiveRelativeKey(
	occurredAt time.Time,
	tenant string,
	source string,
	session string,
	event string,
	checksum string,
) string {
	tenantSegment := sanitizePathSegment(firstNonEmpty(tenant, "default"))
	sourceSegment := sanitizePathSegment(source)
	sessionSegment := sanitizePathSegment(session)
	eventSegment := sanitizePathSegment(event)
	shortChecksum := checksum
	if len(shortChecksum) > 16 {
		shortChecksum = shortChecksum[:16]
	}
	filename := fmt.Sprintf("%s_%s.jsonl", eventSegment, shortChecksum)
	return path.Join(
		occurredAt.UTC().Format("2006/01/02"),
		tenantSegment,
		sourceSegment,
		sessionSegment,
		filename,
	)
}

func sanitizePathSegment(input string) string {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "unknown"
	}
	safe := invalidPathCharPattern.ReplaceAllString(trimmed, "_")
	safe = strings.Trim(safe, "._-")
	if safe == "" {
		return "unknown"
	}
	if len(safe) > 128 {
		safe = safe[:128]
	}
	return safe
}

func joinObjectKey(prefix string, relative string) string {
	cleanRelative := strings.TrimLeft(path.Clean("/"+relative), "/")
	cleanPrefix := strings.Trim(strings.TrimSpace(prefix), "/")
	if cleanPrefix == "" {
		return cleanRelative
	}
	return path.Join(cleanPrefix, cleanRelative)
}

func sha256Hex(input []byte) string {
	sum := sha256.Sum256(input)
	return hex.EncodeToString(sum[:])
}

func (w *localArchiveWriter) WriteAtomic(ctx context.Context, relativeKey string, content []byte) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	fullPath := filepath.Join(w.rootDir, filepath.FromSlash(relativeKey))
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create archive directory failed: %w", err)
	}

	tmpFile, err := os.CreateTemp(dir, ".archiver-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create temp archive file failed: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := tmpFile.Write(content); err != nil {
		tmpFile.Close()
		return "", fmt.Errorf("write temp archive file failed: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return "", fmt.Errorf("sync temp archive file failed: %w", err)
	}
	if err := tmpFile.Chmod(0o644); err != nil {
		tmpFile.Close()
		return "", fmt.Errorf("chmod temp archive file failed: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("close temp archive file failed: %w", err)
	}

	if _, statErr := os.Stat(fullPath); statErr == nil {
		return fullPath, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", fmt.Errorf("check archive target failed: %w", statErr)
	}

	if err := os.Rename(tmpPath, fullPath); err != nil {
		if _, statErr := os.Stat(fullPath); statErr == nil {
			return fullPath, nil
		}
		return "", fmt.Errorf("rename archive file failed: %w", err)
	}

	return fullPath, nil
}

func (w *s3ArchiveWriter) PutObject(ctx context.Context, key string, content []byte) error {
	_, err := w.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      awsv2.String(w.bucket),
		Key:         awsv2.String(key),
		Body:        bytes.NewReader(content),
		ContentType: awsv2.String(objectContentType),
	})
	if err != nil {
		return fmt.Errorf("s3 put object failed: %w", err)
	}
	return nil
}

func (w *ossArchiveWriter) PutObject(ctx context.Context, key string, content []byte) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.bucket.PutObject(key, bytes.NewReader(content), oss.ContentType(objectContentType))
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("oss put object failed: %w", err)
		}
		return nil
	}
}

func ensureArchiveObjectsTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS archive_objects (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  backend TEXT NOT NULL,
  object_key TEXT NOT NULL,
  checksum TEXT NOT NULL,
  size_bytes BIGINT NOT NULL,
  archived_at TIMESTAMPTZ NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (backend, object_key)
);

CREATE INDEX IF NOT EXISTS idx_archive_objects_lookup
  ON archive_objects (tenant_id, source_id, session_id, event_id, occurred_at DESC);
`)
	if err != nil {
		return fmt.Errorf("create archive_objects table failed: %w", err)
	}
	return nil
}

func initPGPool(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	if strings.TrimSpace(cfg.PG.DatabaseURL) == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.PG.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL failed: %w", err)
	}

	poolCfg.MaxConns = cfg.PG.MaxConns
	poolCfg.MinConns = cfg.PG.MinConns
	poolCfg.MaxConnLifetime = cfg.PG.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.PG.MaxConnIdleTime
	poolCfg.HealthCheckPeriod = cfg.PG.HealthCheckPeriod
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET TIME ZONE 'UTC'")
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("new pgx pool failed: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres failed: %w", err)
	}

	return pool, nil
}

func initJetStream(cfg config.Config, log interface {
	Info(string, ...any)
	Warn(string, ...any)
}) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(
		cfg.NATS.URL,
		nats.Name(cfg.ServiceName),
		nats.Timeout(cfg.NATS.ConnectTimeout),
		nats.MaxReconnects(cfg.NATS.MaxReconnects),
		nats.ReconnectWait(cfg.NATS.ReconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Warn("nats disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			log.Info("nats reconnected", "connected_url", conn.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			log.Info("nats connection closed")
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connect nats failed: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create jetstream context failed: %w", err)
	}

	return nc, js, nil
}

func ensureArchiveConsumer(ctx context.Context, js jetstream.JetStream) (jetstream.Consumer, error) {
	_, err := js.CreateOrUpdateConsumer(ctx, archiveStreamName, jetstream.ConsumerConfig{
		Durable:       archiverConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       90 * time.Second,
		FilterSubject: archiveSubjectName,
		MaxDeliver:    20,
	})
	if err != nil {
		return nil, fmt.Errorf("create or update consumer failed: %w", err)
	}

	consumer, err := js.Consumer(ctx, archiveStreamName, archiverConsumerName)
	if err != nil {
		return nil, fmt.Errorf("get consumer failed: %w", err)
	}
	return consumer, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
