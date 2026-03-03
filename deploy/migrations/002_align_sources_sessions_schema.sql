-- 兼容旧版 001 结构，补齐统一后的 sources/sessions 字段与索引。
-- 该迁移可重复执行（幂等），用于线上平滑升级。

ALTER TABLE sources
  ADD COLUMN IF NOT EXISTS name TEXT,
  ADD COLUMN IF NOT EXISTS type TEXT,
  ADD COLUMN IF NOT EXISTS location TEXT,
  ADD COLUMN IF NOT EXISTS enabled BOOLEAN,
  ADD COLUMN IF NOT EXISTS provider TEXT,
  ADD COLUMN IF NOT EXISTS source_type TEXT,
  ADD COLUMN IF NOT EXISTS hostname TEXT,
  ADD COLUMN IF NOT EXISTS agent_id TEXT,
  ADD COLUMN IF NOT EXISTS tenant_id TEXT,
  ADD COLUMN IF NOT EXISTS workspace_id TEXT,
  ADD COLUMN IF NOT EXISTS metadata JSONB,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

UPDATE sources
SET
  provider = COALESCE(NULLIF(provider, ''), 'unknown'),
  source_type = COALESCE(NULLIF(source_type, ''), NULLIF(type, ''), 'local'),
  hostname = COALESCE(NULLIF(hostname, ''), NULLIF(location, '')),
  agent_id = COALESCE(NULLIF(agent_id, ''), NULLIF(name, '')),
  metadata = COALESCE(metadata, '{}'::jsonb),
  name = COALESCE(NULLIF(name, ''), NULLIF(agent_id, ''), NULLIF(hostname, ''), id),
  type = COALESCE(NULLIF(type, ''), NULLIF(source_type, ''), 'local'),
  location = COALESCE(NULLIF(location, ''), NULLIF(hostname, ''), NULLIF(workspace_id, ''), ''),
  enabled = COALESCE(enabled, TRUE),
  updated_at = COALESCE(updated_at, created_at, NOW())
WHERE
  provider IS NULL
  OR provider = ''
  OR source_type IS NULL
  OR source_type = ''
  OR metadata IS NULL
  OR name IS NULL
  OR name = ''
  OR type IS NULL
  OR type = ''
  OR location IS NULL
  OR enabled IS NULL
  OR updated_at IS NULL;

ALTER TABLE sources
  ALTER COLUMN name SET DEFAULT '',
  ALTER COLUMN type SET DEFAULT 'local',
  ALTER COLUMN location SET DEFAULT '',
  ALTER COLUMN enabled SET DEFAULT TRUE,
  ALTER COLUMN metadata SET DEFAULT '{}'::jsonb,
  ALTER COLUMN updated_at SET DEFAULT NOW();

ALTER TABLE sources
  ALTER COLUMN name SET NOT NULL,
  ALTER COLUMN type SET NOT NULL,
  ALTER COLUMN location SET NOT NULL,
  ALTER COLUMN enabled SET NOT NULL,
  ALTER COLUMN metadata SET NOT NULL,
  ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE sessions
  ADD COLUMN IF NOT EXISTS tool TEXT,
  ADD COLUMN IF NOT EXISTS tokens BIGINT,
  ADD COLUMN IF NOT EXISTS cost NUMERIC(18, 8),
  ADD COLUMN IF NOT EXISTS provider TEXT,
  ADD COLUMN IF NOT EXISTS native_session_id TEXT,
  ADD COLUMN IF NOT EXISTS workspace TEXT,
  ADD COLUMN IF NOT EXISTS message_count INTEGER,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

UPDATE sessions
SET
  provider = COALESCE(NULLIF(provider, ''), NULLIF(tool, ''), 'unknown'),
  native_session_id = COALESCE(NULLIF(native_session_id, ''), id),
  tool = COALESCE(NULLIF(tool, ''), NULLIF(provider, '')),
  message_count = COALESCE(message_count, 0),
  tokens = COALESCE(tokens, 0),
  cost = COALESCE(cost, 0),
  updated_at = COALESCE(updated_at, created_at, NOW())
WHERE
  provider IS NULL
  OR provider = ''
  OR native_session_id IS NULL
  OR native_session_id = ''
  OR message_count IS NULL
  OR tokens IS NULL
  OR cost IS NULL
  OR updated_at IS NULL;

ALTER TABLE sessions
  ALTER COLUMN tokens SET DEFAULT 0,
  ALTER COLUMN cost SET DEFAULT 0,
  ALTER COLUMN message_count SET DEFAULT 0,
  ALTER COLUMN updated_at SET DEFAULT NOW();

ALTER TABLE sessions
  ALTER COLUMN provider SET NOT NULL,
  ALTER COLUMN native_session_id SET NOT NULL,
  ALTER COLUMN tokens SET NOT NULL,
  ALTER COLUMN cost SET NOT NULL,
  ALTER COLUMN message_count SET NOT NULL,
  ALTER COLUMN updated_at SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_uni
ON sessions(source_id, provider, native_session_id);

CREATE INDEX IF NOT EXISTS idx_sessions_source_started_at
ON sessions(source_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_sessions_started_at
ON sessions(started_at DESC);
