-- 预算治理闭环与回执事件扩展。
-- 该迁移可重复执行（幂等友好）。

ALTER TABLE IF EXISTS budgets
  ADD COLUMN IF NOT EXISTS governance_mode TEXT,
  ADD COLUMN IF NOT EXISTS release_approval_mode TEXT,
  ADD COLUMN IF NOT EXISTS freeze_ttl_seconds INTEGER,
  ADD COLUMN IF NOT EXISTS control_plane_callback_topic TEXT,
  ADD COLUMN IF NOT EXISTS integration_callback_topic TEXT,
  ADD COLUMN IF NOT EXISTS callback_secret_ref TEXT,
  ADD COLUMN IF NOT EXISTS frozen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS released_at TIMESTAMPTZ;

UPDATE budgets
SET
  governance_mode = CASE
    WHEN governance_mode IN ('advisory', 'enforce') THEN governance_mode
    ELSE 'advisory'
  END,
  release_approval_mode = CASE
    WHEN release_approval_mode IN ('manual', 'auto') THEN release_approval_mode
    ELSE 'manual'
  END,
  freeze_ttl_seconds = GREATEST(COALESCE(freeze_ttl_seconds, 0), 0),
  control_plane_callback_topic = COALESCE(
    NULLIF(control_plane_callback_topic, ''),
    'control-plane.governance.callback'
  ),
  integration_callback_topic = COALESCE(
    NULLIF(integration_callback_topic, ''),
    'integration.callback.events'
  ),
  callback_secret_ref = COALESCE(callback_secret_ref, '')
WHERE
  governance_mode IS NULL
  OR governance_mode NOT IN ('advisory', 'enforce')
  OR release_approval_mode IS NULL
  OR release_approval_mode NOT IN ('manual', 'auto')
  OR freeze_ttl_seconds IS NULL
  OR freeze_ttl_seconds < 0
  OR control_plane_callback_topic IS NULL
  OR control_plane_callback_topic = ''
  OR integration_callback_topic IS NULL
  OR integration_callback_topic = ''
  OR callback_secret_ref IS NULL;

ALTER TABLE IF EXISTS budgets
  ALTER COLUMN governance_mode SET DEFAULT 'advisory',
  ALTER COLUMN release_approval_mode SET DEFAULT 'manual',
  ALTER COLUMN freeze_ttl_seconds SET DEFAULT 0,
  ALTER COLUMN control_plane_callback_topic SET DEFAULT 'control-plane.governance.callback',
  ALTER COLUMN integration_callback_topic SET DEFAULT 'integration.callback.events',
  ALTER COLUMN callback_secret_ref SET DEFAULT '';

ALTER TABLE IF EXISTS budgets
  ALTER COLUMN governance_mode SET NOT NULL,
  ALTER COLUMN release_approval_mode SET NOT NULL,
  ALTER COLUMN freeze_ttl_seconds SET NOT NULL,
  ALTER COLUMN control_plane_callback_topic SET NOT NULL,
  ALTER COLUMN integration_callback_topic SET NOT NULL,
  ALTER COLUMN callback_secret_ref SET NOT NULL;

DO $$
BEGIN
  IF to_regclass('public.budgets') IS NOT NULL THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.budgets'::regclass
        AND conname = 'chk_budgets_governance_mode'
    ) THEN
      ALTER TABLE budgets
        ADD CONSTRAINT chk_budgets_governance_mode
        CHECK (governance_mode IN ('advisory', 'enforce'));
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.budgets'::regclass
        AND conname = 'chk_budgets_release_approval_mode'
    ) THEN
      ALTER TABLE budgets
        ADD CONSTRAINT chk_budgets_release_approval_mode
        CHECK (release_approval_mode IN ('manual', 'auto'));
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.budgets'::regclass
        AND conname = 'chk_budgets_freeze_ttl_seconds'
    ) THEN
      ALTER TABLE budgets
        ADD CONSTRAINT chk_budgets_freeze_ttl_seconds
        CHECK (freeze_ttl_seconds >= 0);
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.budgets'::regclass
        AND conname = 'chk_budgets_control_plane_callback_topic'
    ) THEN
      ALTER TABLE budgets
        ADD CONSTRAINT chk_budgets_control_plane_callback_topic
        CHECK (control_plane_callback_topic <> '');
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.budgets'::regclass
        AND conname = 'chk_budgets_integration_callback_topic'
    ) THEN
      ALTER TABLE budgets
        ADD CONSTRAINT chk_budgets_integration_callback_topic
        CHECK (integration_callback_topic <> '');
    END IF;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_budgets_governance_mode_enabled
ON budgets (governance_mode, enabled);

CREATE INDEX IF NOT EXISTS idx_budgets_callback_topics
ON budgets (control_plane_callback_topic, integration_callback_topic);

ALTER TABLE IF EXISTS governance_alerts
  ADD COLUMN IF NOT EXISTS workflow_status TEXT,
  ADD COLUMN IF NOT EXISTS workflow_updated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS freeze_applied_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS release_requested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS release_resolved_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS release_request_id TEXT,
  ADD COLUMN IF NOT EXISTS control_plane_callback_status TEXT,
  ADD COLUMN IF NOT EXISTS integration_callback_status TEXT,
  ADD COLUMN IF NOT EXISTS callback_retry_count INTEGER,
  ADD COLUMN IF NOT EXISTS callback_last_error TEXT,
  ADD COLUMN IF NOT EXISTS callback_updated_at TIMESTAMPTZ;

UPDATE governance_alerts
SET
  workflow_status = CASE
    WHEN workflow_status IN ('triggered', 'frozen', 'release_pending', 'released', 'closed') THEN workflow_status
    ELSE 'triggered'
  END,
  workflow_updated_at = COALESCE(workflow_updated_at, updated_at, created_at, NOW()),
  control_plane_callback_status = CASE
    WHEN control_plane_callback_status IN ('pending', 'sent', 'acked', 'failed', 'skipped') THEN control_plane_callback_status
    ELSE 'pending'
  END,
  integration_callback_status = CASE
    WHEN integration_callback_status IN ('pending', 'sent', 'acked', 'failed', 'skipped') THEN integration_callback_status
    ELSE 'pending'
  END,
  callback_retry_count = GREATEST(COALESCE(callback_retry_count, 0), 0),
  callback_updated_at = COALESCE(callback_updated_at, updated_at, created_at, NOW())
WHERE
  workflow_status IS NULL
  OR workflow_status NOT IN ('triggered', 'frozen', 'release_pending', 'released', 'closed')
  OR workflow_updated_at IS NULL
  OR control_plane_callback_status IS NULL
  OR control_plane_callback_status NOT IN ('pending', 'sent', 'acked', 'failed', 'skipped')
  OR integration_callback_status IS NULL
  OR integration_callback_status NOT IN ('pending', 'sent', 'acked', 'failed', 'skipped')
  OR callback_retry_count IS NULL
  OR callback_retry_count < 0
  OR callback_updated_at IS NULL;

ALTER TABLE IF EXISTS governance_alerts
  ALTER COLUMN workflow_status SET DEFAULT 'triggered',
  ALTER COLUMN workflow_updated_at SET DEFAULT NOW(),
  ALTER COLUMN control_plane_callback_status SET DEFAULT 'pending',
  ALTER COLUMN integration_callback_status SET DEFAULT 'pending',
  ALTER COLUMN callback_retry_count SET DEFAULT 0,
  ALTER COLUMN callback_updated_at SET DEFAULT NOW();

ALTER TABLE IF EXISTS governance_alerts
  ALTER COLUMN workflow_status SET NOT NULL,
  ALTER COLUMN workflow_updated_at SET NOT NULL,
  ALTER COLUMN control_plane_callback_status SET NOT NULL,
  ALTER COLUMN integration_callback_status SET NOT NULL,
  ALTER COLUMN callback_retry_count SET NOT NULL,
  ALTER COLUMN callback_updated_at SET NOT NULL;

DO $$
BEGIN
  IF to_regclass('public.governance_alerts') IS NOT NULL THEN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.governance_alerts'::regclass
        AND conname = 'chk_governance_alerts_workflow_status'
    ) THEN
      ALTER TABLE governance_alerts
        ADD CONSTRAINT chk_governance_alerts_workflow_status
        CHECK (workflow_status IN ('triggered', 'frozen', 'release_pending', 'released', 'closed'));
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.governance_alerts'::regclass
        AND conname = 'chk_governance_alerts_control_plane_callback_status'
    ) THEN
      ALTER TABLE governance_alerts
        ADD CONSTRAINT chk_governance_alerts_control_plane_callback_status
        CHECK (control_plane_callback_status IN ('pending', 'sent', 'acked', 'failed', 'skipped'));
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.governance_alerts'::regclass
        AND conname = 'chk_governance_alerts_integration_callback_status'
    ) THEN
      ALTER TABLE governance_alerts
        ADD CONSTRAINT chk_governance_alerts_integration_callback_status
        CHECK (integration_callback_status IN ('pending', 'sent', 'acked', 'failed', 'skipped'));
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.governance_alerts'::regclass
        AND conname = 'chk_governance_alerts_callback_retry_count'
    ) THEN
      ALTER TABLE governance_alerts
        ADD CONSTRAINT chk_governance_alerts_callback_retry_count
        CHECK (callback_retry_count >= 0);
    END IF;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_governance_alerts_workflow_status_created_at
ON governance_alerts (workflow_status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_governance_alerts_release_request_id
ON governance_alerts (release_request_id);

CREATE INDEX IF NOT EXISTS idx_governance_alerts_callback_status_updated_at
ON governance_alerts (control_plane_callback_status, integration_callback_status, callback_updated_at DESC);

CREATE TABLE IF NOT EXISTS budget_release_requests (
  id TEXT PRIMARY KEY CHECK (id <> ''),
  tenant_id TEXT NOT NULL CHECK (tenant_id <> ''),
  budget_id TEXT NOT NULL REFERENCES budgets(id) ON DELETE CASCADE CHECK (budget_id <> ''),
  alert_id BIGINT REFERENCES governance_alerts(id) ON DELETE SET NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'approved', 'rejected', 'cancelled', 'expired')),
  reason TEXT NOT NULL DEFAULT '',
  requested_by TEXT NOT NULL DEFAULT 'system' CHECK (requested_by <> ''),
  requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  decision_by TEXT,
  decision_note TEXT,
  decided_at TIMESTAMPTZ,
  control_plane_callback_topic TEXT NOT NULL DEFAULT 'control-plane.governance.callback'
    CHECK (control_plane_callback_topic <> ''),
  integration_callback_topic TEXT NOT NULL DEFAULT 'integration.callback.events'
    CHECK (integration_callback_topic <> ''),
  control_plane_callback_status TEXT NOT NULL DEFAULT 'pending'
    CHECK (control_plane_callback_status IN ('pending', 'sent', 'acked', 'failed', 'skipped')),
  integration_callback_status TEXT NOT NULL DEFAULT 'pending'
    CHECK (integration_callback_status IN ('pending', 'sent', 'acked', 'failed', 'skipped')),
  dedupe_key TEXT NOT NULL CHECK (dedupe_key <> ''),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (
    (status = 'pending' AND decided_at IS NULL)
    OR (status <> 'pending' AND decided_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_budget_release_requests_dedupe_key
ON budget_release_requests (dedupe_key);

CREATE UNIQUE INDEX IF NOT EXISTS idx_budget_release_requests_budget_pending
ON budget_release_requests (budget_id)
WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_budget_release_requests_tenant_status_requested_at
ON budget_release_requests (tenant_id, status, requested_at DESC);

CREATE INDEX IF NOT EXISTS idx_budget_release_requests_budget_status_updated_at
ON budget_release_requests (budget_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_budget_release_requests_alert_id
ON budget_release_requests (alert_id);

CREATE INDEX IF NOT EXISTS idx_budget_release_requests_callback_status
ON budget_release_requests (control_plane_callback_status, integration_callback_status, updated_at DESC);

CREATE TABLE IF NOT EXISTS integration_callback_events (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL CHECK (tenant_id <> ''),
  release_request_id TEXT REFERENCES budget_release_requests(id) ON DELETE SET NULL,
  alert_id BIGINT REFERENCES governance_alerts(id) ON DELETE SET NULL,
  event_source TEXT NOT NULL
    CHECK (event_source IN ('control-plane', 'integration')),
  event_type TEXT NOT NULL
    CHECK (event_type IN ('dispatch', 'delivery', 'receipt', 'verify', 'error')),
  topic TEXT NOT NULL CHECK (topic <> ''),
  callback_id TEXT NOT NULL CHECK (callback_id <> ''),
  delivery_status TEXT NOT NULL DEFAULT 'received'
    CHECK (delivery_status IN ('received', 'validated', 'processed', 'failed', 'duplicate', 'ignored')),
  http_status INTEGER,
  signature_valid BOOLEAN,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  headers JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  event_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_integration_callback_events_source_callback_id
ON integration_callback_events (event_source, callback_id);

CREATE INDEX IF NOT EXISTS idx_integration_callback_events_tenant_status_event_at
ON integration_callback_events (tenant_id, delivery_status, event_at DESC);

CREATE INDEX IF NOT EXISTS idx_integration_callback_events_release_request_event_at
ON integration_callback_events (release_request_id, event_at DESC);

CREATE INDEX IF NOT EXISTS idx_integration_callback_events_alert_event_at
ON integration_callback_events (alert_id, event_at DESC);

CREATE INDEX IF NOT EXISTS idx_integration_callback_events_topic_event_at
ON integration_callback_events (topic, event_at DESC);

DO $$
BEGIN
  IF to_regclass('public.governance_alerts') IS NOT NULL
     AND to_regclass('public.budget_release_requests') IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM pg_constraint
       WHERE conrelid = 'public.governance_alerts'::regclass
         AND conname = 'fk_governance_alerts_release_request'
     ) THEN
    ALTER TABLE governance_alerts
      ADD CONSTRAINT fk_governance_alerts_release_request
      FOREIGN KEY (release_request_id)
      REFERENCES budget_release_requests(id)
      ON DELETE SET NULL;
  END IF;
END $$;
