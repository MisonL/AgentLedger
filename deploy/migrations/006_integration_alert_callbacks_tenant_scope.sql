-- integration_alert_callbacks 幂等键升级为 tenant + callback 维度。
-- 该迁移支持重复执行。

CREATE TABLE IF NOT EXISTS integration_alert_callbacks (
  tenant_id TEXT NOT NULL DEFAULT 'default',
  callback_id TEXT NOT NULL,
  action TEXT NOT NULL DEFAULT 'ack',
  response_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, callback_id)
);

ALTER TABLE IF EXISTS integration_alert_callbacks
  ADD COLUMN IF NOT EXISTS tenant_id TEXT,
  ADD COLUMN IF NOT EXISTS callback_id TEXT,
  ADD COLUMN IF NOT EXISTS action TEXT,
  ADD COLUMN IF NOT EXISTS response_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;

UPDATE integration_alert_callbacks
SET tenant_id = COALESCE(NULLIF(tenant_id, ''), 'default'),
    callback_id = COALESCE(
      NULLIF(callback_id, ''),
      md5(random()::text || clock_timestamp()::text)
    ),
    action = CASE
      WHEN action IN ('ack', 'resolve', 'request_release', 'approve_release', 'reject_release')
        THEN action
      ELSE 'ack'
    END,
    response_payload = COALESCE(response_payload, '{}'::jsonb),
    processed_at = COALESCE(processed_at, NOW())
WHERE tenant_id IS NULL
   OR tenant_id = ''
   OR callback_id IS NULL
   OR callback_id = ''
   OR action IS NULL
   OR action NOT IN ('ack', 'resolve', 'request_release', 'approve_release', 'reject_release')
   OR response_payload IS NULL
   OR processed_at IS NULL;

ALTER TABLE IF EXISTS integration_alert_callbacks
  ALTER COLUMN tenant_id SET DEFAULT 'default',
  ALTER COLUMN tenant_id SET NOT NULL,
  ALTER COLUMN callback_id SET NOT NULL,
  ALTER COLUMN action SET DEFAULT 'ack',
  ALTER COLUMN action SET NOT NULL,
  ALTER COLUMN response_payload SET DEFAULT '{}'::jsonb,
  ALTER COLUMN response_payload SET NOT NULL,
  ALTER COLUMN processed_at SET DEFAULT NOW(),
  ALTER COLUMN processed_at SET NOT NULL;

WITH ranked AS (
  SELECT ctid,
         ROW_NUMBER() OVER (
           PARTITION BY tenant_id, callback_id
           ORDER BY processed_at DESC, ctid DESC
         ) AS row_index
  FROM integration_alert_callbacks
)
DELETE FROM integration_alert_callbacks AS callbacks
USING ranked
WHERE callbacks.ctid = ranked.ctid
  AND ranked.row_index > 1;

DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  IF to_regclass('public.integration_alert_callbacks') IS NULL THEN
    RETURN;
  END IF;

  FOR constraint_name IN
    SELECT constraints.conname
    FROM pg_constraint AS constraints
    WHERE constraints.conrelid = 'public.integration_alert_callbacks'::regclass
      AND constraints.contype IN ('p', 'u')
      AND ARRAY(
        SELECT attrs.attname
        FROM unnest(constraints.conkey) WITH ORDINALITY AS keys(attnum, ord)
        JOIN pg_attribute AS attrs
          ON attrs.attrelid = constraints.conrelid
         AND attrs.attnum = keys.attnum
        ORDER BY keys.ord
      ) = ARRAY['callback_id']
  LOOP
    EXECUTE format(
      'ALTER TABLE integration_alert_callbacks DROP CONSTRAINT %I',
      constraint_name
    );
  END LOOP;
END
$$;

DO $$
DECLARE
  index_name TEXT;
BEGIN
  IF to_regclass('public.integration_alert_callbacks') IS NULL THEN
    RETURN;
  END IF;

  FOR index_name IN
    SELECT index_class.relname
    FROM pg_index AS index_meta
    JOIN pg_class AS table_class
      ON table_class.oid = index_meta.indrelid
    JOIN pg_namespace AS table_namespace
      ON table_namespace.oid = table_class.relnamespace
    JOIN pg_class AS index_class
      ON index_class.oid = index_meta.indexrelid
    LEFT JOIN pg_constraint AS constraints
      ON constraints.conindid = index_meta.indexrelid
    WHERE table_namespace.nspname = 'public'
      AND table_class.relname = 'integration_alert_callbacks'
      AND index_meta.indisunique = TRUE
      AND index_meta.indisprimary = FALSE
      AND constraints.oid IS NULL
      AND index_meta.indnkeyatts = 1
      AND pg_get_indexdef(index_meta.indexrelid) ILIKE '%(callback_id)%'
  LOOP
    EXECUTE format('DROP INDEX IF EXISTS %I.%I', 'public', index_name);
  END LOOP;
END
$$;

DO $$
BEGIN
  IF to_regclass('public.integration_alert_callbacks') IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conrelid = 'public.integration_alert_callbacks'::regclass
        AND contype = 'p'
    ) THEN
    ALTER TABLE integration_alert_callbacks
      ADD CONSTRAINT integration_alert_callbacks_pkey
      PRIMARY KEY (tenant_id, callback_id);
  END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_integration_alert_callbacks_tenant_callback_id
ON integration_alert_callbacks (tenant_id, callback_id);

CREATE INDEX IF NOT EXISTS idx_integration_alert_callbacks_tenant_processed_at
ON integration_alert_callbacks (tenant_id, processed_at DESC);
