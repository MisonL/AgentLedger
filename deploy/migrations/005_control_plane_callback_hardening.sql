-- control-plane callback 安全与一致性加固。
-- 该迁移支持重复执行。

UPDATE budgets
SET enabled = CASE
      WHEN governance_state = 'active' THEN TRUE
      ELSE FALSE
    END,
    updated_at = COALESCE(updated_at, NOW())
WHERE (governance_state = 'active' AND enabled IS DISTINCT FROM TRUE)
   OR (governance_state IN ('frozen', 'pending_release') AND enabled IS DISTINCT FROM FALSE);

DO $$
BEGIN
  IF to_regclass('public.budgets') IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.budgets'::regclass
        AND conname = 'chk_budgets_enabled_governance_consistent'
    ) THEN
    ALTER TABLE budgets
      ADD CONSTRAINT chk_budgets_enabled_governance_consistent
      CHECK (
        (governance_state = 'active' AND enabled = TRUE)
        OR (governance_state IN ('frozen', 'pending_release') AND enabled = FALSE)
      );
  END IF;
END
$$;

WITH ranked AS (
  SELECT ctid,
         ROW_NUMBER() OVER (
           PARTITION BY tenant_id, budget_id
           ORDER BY requested_at DESC, updated_at DESC, ctid DESC
         ) AS row_index
  FROM budget_release_requests
  WHERE status = 'pending'
)
UPDATE budget_release_requests AS requests
SET status = 'rejected',
    rejected_reason = COALESCE(
      NULLIF(requests.rejected_reason, ''),
      '系统自动驳回：同 tenant + budget 仅允许一个 pending 申请。'
    ),
    rejected_at = COALESCE(requests.rejected_at, requests.updated_at, NOW()),
    updated_at = COALESCE(requests.updated_at, NOW())
FROM ranked
WHERE requests.ctid = ranked.ctid
  AND ranked.row_index > 1;

UPDATE budgets AS budgets
SET governance_state = 'pending_release',
    enabled = FALSE,
    updated_at = COALESCE(budgets.updated_at, NOW())
WHERE EXISTS (
  SELECT 1
  FROM budget_release_requests AS requests
  WHERE requests.tenant_id = budgets.tenant_id
    AND requests.budget_id = budgets.id
    AND requests.status = 'pending'
)
  AND (
    budgets.governance_state <> 'pending_release'
    OR budgets.enabled IS DISTINCT FROM FALSE
  );

CREATE UNIQUE INDEX IF NOT EXISTS idx_budget_release_requests_tenant_budget_pending_unique
ON budget_release_requests (tenant_id, budget_id)
WHERE status = 'pending';

CREATE OR REPLACE FUNCTION enforce_governance_alert_status_transition()
RETURNS trigger AS $$
BEGIN
  IF NEW.status IS DISTINCT FROM OLD.status THEN
    IF OLD.status = 'open' AND NEW.status IN ('acknowledged', 'resolved') THEN
      RETURN NEW;
    END IF;

    IF OLD.status = 'acknowledged' AND NEW.status = 'resolved' THEN
      RETURN NEW;
    END IF;

    RAISE EXCEPTION
      'invalid governance_alerts status transition from % to %',
      OLD.status,
      NEW.status
      USING ERRCODE = '23514';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
  IF to_regclass('public.governance_alerts') IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM pg_trigger
      WHERE tgrelid = 'public.governance_alerts'::regclass
        AND tgname = 'trg_governance_alert_status_transition'
    ) THEN
    CREATE TRIGGER trg_governance_alert_status_transition
    BEFORE UPDATE OF status ON governance_alerts
    FOR EACH ROW
    EXECUTE FUNCTION enforce_governance_alert_status_transition();
  END IF;
END
$$;
