package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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
	rawStreamName          = "AGENT_EVENTS_RAW"
	rawSubjectName         = "agent.events.raw"
	archiveEnqueueSubject  = "archive.enqueue"
	normalizerConsumerName = "NORMALIZER_DURABLE"
)

func main() {
	cfg, err := config.Load("normalizer", ":8082")
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

	consumer, err := ensureRawConsumer(ctx, js)
	if err != nil {
		log.Error("ensure raw consumer failed", "error", err)
		os.Exit(1)
	}

	processor := &eventProcessor{
		log:  log,
		pool: pool,
		js:   js,
	}
	consumeCtx, err := consumer.Consume(processor.handleMessage)
	if err != nil {
		log.Error("start consumer failed", "error", err, "consumer", normalizerConsumerName)
		os.Exit(1)
	}
	defer consumeCtx.Stop()

	healthErrCh := health.Start(ctx, cfg.HTTPAddr, cfg.ServiceName, log)
	log.Info("service started",
		"http_addr", cfg.HTTPAddr,
		"nats_url", cfg.NATS.URL,
		"consumer", normalizerConsumerName,
		"raw_subject", rawSubjectName,
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

type eventProcessor struct {
	log  *slog.Logger
	pool *pgxpool.Pool
	js   jetstream.JetStream
}

type archiveEnqueueMessage struct {
	Tenant   string          `json:"tenant"`
	Source   string          `json:"source"`
	Session  string          `json:"session"`
	Event    string          `json:"event"`
	Time     string          `json:"time"`
	Raw      json.RawMessage `json:"raw"`
	Payload  json.RawMessage `json:"payload"`
	Metadata map[string]any  `json:"metadata,omitempty"`
}

type normalizedEvent struct {
	BatchID          string
	EnvelopeID       string
	EventID          string
	EventRowID       string
	SessionRowID     string
	NativeSessionID  string
	SourceID         string
	Provider         string
	SourceType       string
	AgentID          string
	TenantID         string
	WorkspaceID      string
	Hostname         string
	EventType        string
	Role             string
	Text             string
	Model            string
	OccurredAt       time.Time
	InputTokens      int64
	OutputTokens     int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	ReasoningTokens  int64
	CostUSD          *float64
	CostMode         string
	SourcePath       string
	SourceOffset     *int64
	RawHash          string
	RawPayload       json.RawMessage
}

func (p *eventProcessor) handleMessage(msg jetstream.Msg) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rawPayload := append([]byte(nil), msg.Data()...)
	var envelope ingest.RawEnvelope
	if err := json.Unmarshal(rawPayload, &envelope); err != nil {
		p.log.Error("decode raw envelope failed", "error", err, "payload_size", len(rawPayload))
		if termErr := msg.Term(); termErr != nil {
			p.log.Warn("term message failed", "error", termErr)
		}
		return
	}

	nEvent, err := normalizeEnvelope(envelope, rawPayload)
	if err != nil {
		p.log.Error("normalize envelope failed",
			"error", err,
			"batch_id", envelope.BatchID,
			"envelope_id", envelope.EnvelopeID,
			"event_id", envelope.Event.EventID,
		)
		if termErr := msg.Term(); termErr != nil {
			p.log.Warn("term message failed", "error", termErr)
		}
		return
	}

	if err := p.persistNormalizedEvent(ctx, nEvent); err != nil {
		p.log.Error("persist normalized event failed",
			"error", err,
			"source_id", nEvent.SourceID,
			"session_id", nEvent.NativeSessionID,
			"event_id", nEvent.EventID,
			"envelope_id", nEvent.EnvelopeID,
		)
		if nakErr := msg.Nak(); nakErr != nil {
			p.log.Warn("nak message failed", "error", nakErr)
		}
		return
	}

	if err := p.publishArchiveEnqueue(ctx, nEvent, envelope, rawPayload); err != nil {
		p.log.Error("publish archive enqueue failed",
			"error", err,
			"subject", archiveEnqueueSubject,
			"source_id", nEvent.SourceID,
			"session_id", nEvent.NativeSessionID,
			"event_id", nEvent.EventID,
			"envelope_id", nEvent.EnvelopeID,
		)
		if nakErr := msg.Nak(); nakErr != nil {
			p.log.Warn("nak message failed", "error", nakErr)
		}
		return
	}

	if ackErr := msg.Ack(); ackErr != nil {
		p.log.Warn("ack message failed", "error", ackErr, "envelope_id", nEvent.EnvelopeID)
		return
	}

	p.log.Info("normalized event persisted",
		"batch_id", nEvent.BatchID,
		"envelope_id", nEvent.EnvelopeID,
		"event_id", nEvent.EventID,
		"source_id", nEvent.SourceID,
		"session_id", nEvent.NativeSessionID,
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

func (p *eventProcessor) publishArchiveEnqueue(
	ctx context.Context,
	event normalizedEvent,
	envelope ingest.RawEnvelope,
	rawPayload []byte,
) error {
	metadata := map[string]any{
		"batch_id":    event.BatchID,
		"envelope_id": event.EnvelopeID,
		"event_type":  event.EventType,
		"provider":    event.Provider,
		"source_type": event.SourceType,
		"raw_hash":    event.RawHash,
	}
	if len(envelope.Event.Metadata) > 0 {
		metadata["event_metadata"] = envelope.Event.Metadata
	}
	if event.AgentID != "" {
		metadata["agent_id"] = event.AgentID
	}
	if event.WorkspaceID != "" {
		metadata["workspace_id"] = event.WorkspaceID
	}

	payload, err := json.Marshal(archiveEnqueueMessage{
		Tenant:   firstNonEmpty(event.TenantID, "default"),
		Source:   event.SourceID,
		Session:  event.NativeSessionID,
		Event:    event.EventID,
		Time:     event.OccurredAt.UTC().Format(time.RFC3339Nano),
		Raw:      json.RawMessage(rawPayload),
		Payload:  event.RawPayload,
		Metadata: metadata,
	})
	if err != nil {
		return fmt.Errorf("marshal archive enqueue payload failed: %w", err)
	}

	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ack, err := p.js.Publish(publishCtx, archiveEnqueueSubject, payload)
	if err != nil {
		return fmt.Errorf("publish to %s failed: %w", archiveEnqueueSubject, err)
	}

	p.log.Info("archive enqueue published",
		"tenant_id", firstNonEmpty(event.TenantID, "default"),
		"source_id", event.SourceID,
		"session_id", event.NativeSessionID,
		"event_id", event.EventID,
		"subject", archiveEnqueueSubject,
		"stream", ack.Stream,
		"sequence", ack.Sequence,
	)
	return nil
}

func normalizeEnvelope(envelope ingest.RawEnvelope, rawPayload []byte) (normalizedEvent, error) {
	sourceID := strings.TrimSpace(envelope.Source.SourceID)
	provider := strings.TrimSpace(envelope.Source.Provider)
	sessionID := strings.TrimSpace(envelope.Event.SessionID)
	eventType := strings.TrimSpace(envelope.Event.EventType)
	if sourceID == "" {
		return normalizedEvent{}, fmt.Errorf("missing source.source_id")
	}
	if provider == "" {
		return normalizedEvent{}, fmt.Errorf("missing source.provider")
	}
	if sessionID == "" {
		return normalizedEvent{}, fmt.Errorf("missing event.session_id")
	}
	if eventType == "" {
		return normalizedEvent{}, fmt.Errorf("missing event.event_type")
	}

	occurredAt, err := ingest.ParseTimestamp(envelope.Event.OccurredAt)
	if err != nil {
		return normalizedEvent{}, fmt.Errorf("invalid event.occurred_at: %w", err)
	}

	eventID := strings.TrimSpace(envelope.Event.EventID)
	if eventID == "" {
		eventID = ingest.NewID("evt")
	}
	envelopeID := strings.TrimSpace(envelope.EnvelopeID)
	if envelopeID == "" {
		envelopeID = stableID("env", sourceID, eventID, occurredAt.Format(time.RFC3339Nano))
	}

	sourceType := strings.TrimSpace(envelope.Source.SourceType)
	if sourceType == "" {
		sourceType = "agent"
	}
	costMode := strings.TrimSpace(envelope.Event.CostMode)
	if costMode == "" {
		costMode = "reported"
	}
	sourcePath := strings.TrimSpace(envelope.Event.SourcePath)
	if sourcePath == "" {
		sourcePath = "agent://push"
	}
	rawHash := strings.TrimSpace(envelope.RawHash)
	if rawHash == "" {
		rawHash = ingest.SHA256Hex(rawPayload)
	}

	payload := envelope.Event.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}

	return normalizedEvent{
		BatchID:          strings.TrimSpace(envelope.BatchID),
		EnvelopeID:       envelopeID,
		EventID:          eventID,
		EventRowID:       stableID("evt", sourceID, eventID),
		SessionRowID:     stableID("ses", sourceID, provider, sessionID),
		NativeSessionID:  sessionID,
		SourceID:         sourceID,
		Provider:         provider,
		SourceType:       sourceType,
		AgentID:          strings.TrimSpace(envelope.Agent.AgentID),
		TenantID:         strings.TrimSpace(envelope.Agent.TenantID),
		WorkspaceID:      strings.TrimSpace(envelope.Agent.WorkspaceID),
		Hostname:         strings.TrimSpace(envelope.Agent.Hostname),
		EventType:        eventType,
		Role:             strings.TrimSpace(envelope.Event.Role),
		Text:             strings.TrimSpace(envelope.Event.Text),
		Model:            strings.TrimSpace(envelope.Event.Model),
		OccurredAt:       occurredAt,
		InputTokens:      envelope.Event.Tokens.InputTokens,
		OutputTokens:     envelope.Event.Tokens.OutputTokens,
		CacheReadTokens:  envelope.Event.Tokens.CacheReadTokens,
		CacheWriteTokens: envelope.Event.Tokens.CacheWriteTokens,
		ReasoningTokens:  envelope.Event.Tokens.ReasoningTokens,
		CostUSD:          envelope.Event.CostUSD,
		CostMode:         costMode,
		SourcePath:       sourcePath,
		SourceOffset:     envelope.Event.SourceOffset,
		RawHash:          rawHash,
		RawPayload:       payload,
	}, nil
}

func (p *eventProcessor) persistNormalizedEvent(ctx context.Context, event normalizedEvent) error {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	sourceMeta, err := json.Marshal(map[string]string{
		"batch_id":    event.BatchID,
		"source_type": event.SourceType,
	})
	if err != nil {
		return fmt.Errorf("marshal source metadata: %w", err)
	}

	sourceName := event.SourceID
	sourceType := "sync-cache"
	sourceLocation := firstNonEmpty(event.Hostname, event.WorkspaceID, event.SourceID)

	_, err = tx.Exec(ctx, `
INSERT INTO sources (
  id, name, type, location, enabled, provider, source_type,
  hostname, agent_id, tenant_id, workspace_id, metadata, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, NOW(), NOW())
ON CONFLICT (id) DO UPDATE
SET provider = EXCLUDED.provider,
    source_type = EXCLUDED.source_type,
    hostname = EXCLUDED.hostname,
    agent_id = EXCLUDED.agent_id,
    tenant_id = EXCLUDED.tenant_id,
    workspace_id = EXCLUDED.workspace_id,
    metadata = EXCLUDED.metadata,
    updated_at = NOW()
`, event.SourceID, sourceName, sourceType, sourceLocation, true, event.Provider, event.SourceType,
		nullableString(event.Hostname), nullableString(event.AgentID), nullableString(event.TenantID),
		nullableString(event.WorkspaceID), sourceMeta,
	)
	if err != nil {
		return fmt.Errorf("upsert sources failed: %w", err)
	}

	err = tx.QueryRow(ctx, `
INSERT INTO sessions (
  id, source_id, tool, model, tokens, cost, provider, native_session_id,
  message_count, workspace, started_at, ended_at, created_at, updated_at
)
VALUES ($1, $2, $3, $4, 0, 0, $5, $6, 0, $7, $8, $8, NOW(), NOW())
ON CONFLICT (source_id, provider, native_session_id) DO UPDATE
SET tool = COALESCE(EXCLUDED.tool, sessions.tool),
    workspace = COALESCE(EXCLUDED.workspace, sessions.workspace),
    model = COALESCE(EXCLUDED.model, sessions.model),
    started_at = COALESCE(LEAST(sessions.started_at, EXCLUDED.started_at), sessions.started_at, EXCLUDED.started_at),
    ended_at = COALESCE(GREATEST(sessions.ended_at, EXCLUDED.ended_at), sessions.ended_at, EXCLUDED.ended_at),
    updated_at = NOW()
RETURNING id
`, event.SessionRowID, event.SourceID, nullableString(deriveSessionTool(event.SourceType, event.Provider)),
		nullableString(event.Model), event.Provider, event.NativeSessionID,
		nullableString(event.WorkspaceID), event.OccurredAt,
	).Scan(&event.SessionRowID)
	if err != nil {
		return fmt.Errorf("upsert sessions failed: %w", err)
	}

	var costValue any
	if event.CostUSD != nil {
		costValue = *event.CostUSD
	}

	commandTag, err := tx.Exec(ctx, `
INSERT INTO events (
  id, source_id, session_id, provider, event_type, role, text, model, timestamp,
  input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, reasoning_tokens,
  cost_usd, cost_mode, source_path, source_offset, raw_hash, raw_payload, created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
        $10, $11, $12, $13, $14,
        $15, $16, $17, $18, $19, $20::jsonb, NOW())
ON CONFLICT (id) DO NOTHING
`, event.EventRowID, event.SourceID, event.SessionRowID, event.Provider, event.EventType,
		nullableString(event.Role), nullableString(event.Text), nullableString(event.Model), event.OccurredAt,
		nullableInt64(event.InputTokens), nullableInt64(event.OutputTokens), nullableInt64(event.CacheReadTokens),
		nullableInt64(event.CacheWriteTokens), nullableInt64(event.ReasoningTokens),
		costValue, event.CostMode, event.SourcePath, event.SourceOffset, event.RawHash, event.RawPayload,
	)
	if err != nil {
		return fmt.Errorf("insert events failed: %w", err)
	}

	if commandTag.RowsAffected() > 0 {
		_, err = tx.Exec(ctx, `
UPDATE sessions
SET message_count = sessions.message_count + 1,
    tokens = sessions.tokens + $2,
    cost = sessions.cost + $3::numeric,
    updated_at = NOW()
WHERE id = $1
`, event.SessionRowID, totalEventTokens(event), sessionCostIncrement(event.CostUSD))
		if err != nil {
			return fmt.Errorf("accumulate sessions usage failed: %w", err)
		}
	}

	action := "normalizer.event_inserted"
	detail := "event normalized and persisted"
	if commandTag.RowsAffected() == 0 {
		action = "normalizer.event_duplicated"
		detail = "duplicate event ignored by idempotent insert"
	}

	auditMeta, err := json.Marshal(map[string]any{
		"batch_id":    event.BatchID,
		"envelope_id": event.EnvelopeID,
		"source_id":   event.SourceID,
		"session_id":  event.NativeSessionID,
		"event_id":    event.EventID,
		"event_type":  event.EventType,
	})
	if err != nil {
		return fmt.Errorf("marshal audit metadata: %w", err)
	}

	_, err = tx.Exec(ctx, `
INSERT INTO audit_logs (id, event_id, action, level, detail, metadata, created_at)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, NOW())
`, ingest.NewID("audit"), event.EventRowID, action, "info", detail, auditMeta)
	if err != nil {
		return fmt.Errorf("insert audit_logs failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx failed: %w", err)
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

func ensureRawConsumer(ctx context.Context, js jetstream.JetStream) (jetstream.Consumer, error) {
	_, err := js.CreateOrUpdateConsumer(ctx, rawStreamName, jetstream.ConsumerConfig{
		Durable:       normalizerConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       45 * time.Second,
		FilterSubject: rawSubjectName,
		MaxDeliver:    10,
	})
	if err != nil {
		return nil, fmt.Errorf("create or update consumer failed: %w", err)
	}

	consumer, err := js.Consumer(ctx, rawStreamName, normalizerConsumerName)
	if err != nil {
		return nil, fmt.Errorf("get consumer failed: %w", err)
	}
	return consumer, nil
}

func nullableString(input string) any {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func nullableInt64(input int64) any {
	if input == 0 {
		return nil
	}
	return input
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

func deriveSessionTool(sourceType string, provider string) string {
	normalizedSourceType := strings.TrimSpace(sourceType)
	if normalizedSourceType != "" && normalizedSourceType != "agent" {
		return normalizedSourceType
	}
	return strings.TrimSpace(provider)
}

func totalEventTokens(event normalizedEvent) int64 {
	return event.InputTokens +
		event.OutputTokens +
		event.CacheReadTokens +
		event.CacheWriteTokens +
		event.ReasoningTokens
}

func sessionCostIncrement(cost *float64) float64 {
	if cost == nil {
		return 0
	}
	return *cost
}

func stableID(prefix string, parts ...string) string {
	joined := strings.Join(parts, "|")
	sum := sha256.Sum256([]byte(joined))
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(sum[:12]))
}
