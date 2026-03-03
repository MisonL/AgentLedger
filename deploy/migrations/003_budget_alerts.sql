CREATE TABLE IF NOT EXISTS budgets (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL CHECK (tenant_id <> ''),
  scope TEXT NOT NULL CHECK (scope IN ('global', 'source')),
  source_id TEXT NOT NULL DEFAULT '',
  period TEXT NOT NULL CHECK (period IN ('daily', 'monthly')),
  token_limit BIGINT NOT NULL DEFAULT 0 CHECK (token_limit >= 0),
  cost_limit NUMERIC(18, 8) NOT NULL DEFAULT 0 CHECK (cost_limit >= 0),
  alert_threshold NUMERIC(5, 4) NOT NULL DEFAULT 0.80
    CHECK (alert_threshold > 0 AND alert_threshold <= 1),
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  last_evaluated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (token_limit > 0 OR cost_limit > 0),
  CHECK (NOT (scope = 'source' AND source_id = ''))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_budgets_tenant_scope_source_period
ON budgets (tenant_id, scope, source_id, period);

CREATE INDEX IF NOT EXISTS idx_budgets_tenant_enabled
ON budgets (tenant_id, enabled);

CREATE INDEX IF NOT EXISTS idx_budgets_source_period
ON budgets (source_id, period);

CREATE INDEX IF NOT EXISTS idx_budgets_last_evaluated_at
ON budgets (last_evaluated_at);

CREATE TABLE IF NOT EXISTS governance_alerts (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL CHECK (tenant_id <> ''),
  budget_id TEXT NOT NULL REFERENCES budgets(id) ON DELETE CASCADE,
  source_id TEXT,
  period TEXT NOT NULL CHECK (period IN ('daily', 'monthly')),
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  tokens_used BIGINT NOT NULL DEFAULT 0 CHECK (tokens_used >= 0),
  cost_used NUMERIC(18, 8) NOT NULL DEFAULT 0 CHECK (cost_used >= 0),
  token_limit BIGINT NOT NULL DEFAULT 0 CHECK (token_limit >= 0),
  cost_limit NUMERIC(18, 8) NOT NULL DEFAULT 0 CHECK (cost_limit >= 0),
  threshold NUMERIC(5, 4) NOT NULL CHECK (threshold > 0 AND threshold <= 1),
  severity TEXT NOT NULL CHECK (severity IN ('warning', 'critical')),
  status TEXT NOT NULL DEFAULT 'open'
    CHECK (status IN ('open', 'acknowledged', 'resolved')),
  dedupe_key TEXT NOT NULL CHECK (dedupe_key <> ''),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (window_end > window_start)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_governance_alerts_dedupe_key
ON governance_alerts (dedupe_key);

CREATE INDEX IF NOT EXISTS idx_governance_alerts_tenant_status_created_at
ON governance_alerts (tenant_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_governance_alerts_budget_created_at
ON governance_alerts (budget_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_governance_alerts_source_window
ON governance_alerts (source_id, window_start DESC, window_end DESC);

CREATE INDEX IF NOT EXISTS idx_governance_alerts_period_window
ON governance_alerts (period, window_start DESC, window_end DESC);
