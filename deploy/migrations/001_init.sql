CREATE TABLE IF NOT EXISTS sources (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  type TEXT NOT NULL DEFAULT 'local',
  location TEXT NOT NULL DEFAULT '',
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  provider TEXT,
  source_type TEXT,
  hostname TEXT,
  agent_id TEXT,
  tenant_id TEXT,
  workspace_id TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES sources(id),
  tool TEXT,
  model TEXT,
  tokens BIGINT NOT NULL DEFAULT 0,
  cost NUMERIC(18, 8) NOT NULL DEFAULT 0,
  provider TEXT NOT NULL,
  native_session_id TEXT NOT NULL,
  workspace TEXT,
  started_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ,
  message_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_uni
ON sessions(source_id, provider, native_session_id);

CREATE INDEX IF NOT EXISTS idx_sessions_source_started_at
ON sessions(source_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_sessions_started_at
ON sessions(started_at DESC);

CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES sources(id),
  session_id TEXT NOT NULL REFERENCES sessions(id),
  provider TEXT NOT NULL,
  event_type TEXT NOT NULL,
  role TEXT,
  text TEXT,
  model TEXT,
  timestamp TIMESTAMPTZ NOT NULL,
  input_tokens BIGINT,
  output_tokens BIGINT,
  cache_read_tokens BIGINT,
  cache_write_tokens BIGINT,
  reasoning_tokens BIGINT,
  cost_usd NUMERIC(18, 8),
  cost_mode TEXT NOT NULL,
  source_path TEXT NOT NULL,
  source_offset BIGINT,
  raw_hash TEXT NOT NULL,
  raw_payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_time ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);

CREATE TABLE IF NOT EXISTS audit_logs (
  id TEXT PRIMARY KEY,
  event_id TEXT,
  action TEXT NOT NULL,
  level TEXT NOT NULL,
  detail TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_event_id ON audit_logs(event_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at);
