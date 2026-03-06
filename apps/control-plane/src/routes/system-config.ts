import { Hono, type Context } from "hono";
import type {
  Budget,
  SystemConfigBackupBudget,
  SystemConfigBackupPayload,
  SystemConfigBackupSource,
  SystemConfigRestoreResult,
} from "../contracts";
import { validateSystemConfigRestoreInput } from "../contracts";
import type { AppendAuditLogInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

const SYSTEM_CONFIG_BACKUP_SCHEMA_VERSION = "system-config-backup.v1";

export const systemConfigRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 system-config 审计日志失败。", error);
  }
}

async function requireAuthContext(c: Context<AppEnv>) {
  const authResult = await authMiddleware(c, async () => {});
  if (authResult instanceof Response) {
    return authResult;
  }

  const auth = c.get("auth");
  if (!auth) {
    return c.json({ message: "未认证：请先登录。" }, 401);
  }
  return auth;
}

function normalizeSourceSignaturePart(value: unknown): string {
  if (typeof value === "string") {
    return value.trim().toLowerCase();
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return "";
}

function buildSourceSignature(
  source: Pick<
    SystemConfigBackupSource,
    | "type"
    | "location"
    | "sourceRegion"
    | "accessMode"
    | "syncCron"
    | "syncRetentionDays"
    | "sshConfig"
    | "enabled"
  >
): string {
  const ssh = source.sshConfig;
  return [
    normalizeSourceSignaturePart(source.type),
    normalizeSourceSignaturePart(source.location),
    normalizeSourceSignaturePart(source.sourceRegion),
    normalizeSourceSignaturePart(source.accessMode),
    normalizeSourceSignaturePart(source.syncCron),
    normalizeSourceSignaturePart(source.syncRetentionDays),
    normalizeSourceSignaturePart(source.enabled),
    normalizeSourceSignaturePart(ssh?.host),
    normalizeSourceSignaturePart(ssh?.port),
    normalizeSourceSignaturePart(ssh?.user),
    normalizeSourceSignaturePart(ssh?.authType),
    normalizeSourceSignaturePart(ssh?.keyPath),
    normalizeSourceSignaturePart(ssh?.knownHostsPath),
  ].join("|");
}

function toSystemConfigBackupSource(
  source: SystemConfigBackupSource & {
    id?: string;
    createdAt?: string;
  }
): SystemConfigBackupSource {
  return {
    name: source.name,
    type: source.type,
    location: source.location,
    sourceRegion: source.sourceRegion,
    sshConfig: source.sshConfig,
    accessMode: source.accessMode,
    syncCron: source.syncCron,
    syncRetentionDays: source.syncRetentionDays,
    enabled: source.enabled,
  };
}

function toSystemConfigBackupBudget(budget: Budget): SystemConfigBackupBudget {
  return {
    scope: budget.scope,
    sourceId: budget.sourceId,
    organizationId: budget.organizationId,
    userId: budget.userId,
    model: budget.model,
    period: budget.period,
    tokenLimit: budget.tokenLimit,
    costLimit: budget.costLimit,
    thresholds: budget.thresholds,
    alertThreshold: budget.thresholds.warning,
  };
}

function buildBackupFileName(tenantId: string): string {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `system-config-${tenantId}-${timestamp}.json`;
}

systemConfigRoutes.get("/system/config/backup", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const tenantId = auth.tenantId;
  const [sources, budgets, pricingCatalog] = await Promise.all([
    repository.listSources(tenantId),
    repository.listBudgets(tenantId),
    repository.getPricingCatalog(tenantId),
  ]);

  const payload: SystemConfigBackupPayload = {
    schemaVersion: SYSTEM_CONFIG_BACKUP_SCHEMA_VERSION,
    tenantId,
    exportedAt: new Date().toISOString(),
    exportedBy: {
      userId: auth.userId,
      email: auth.email,
    },
    sources: sources.map((item) =>
      toSystemConfigBackupSource(item as SystemConfigBackupSource)
    ),
    budgets: budgets.map(toSystemConfigBackupBudget),
    pricingCatalog: pricingCatalog
      ? {
          note: pricingCatalog.version.note,
          entries: pricingCatalog.entries,
        }
      : undefined,
  };

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.system_config_backup_exported",
    level: "info",
    detail: `Exported tenant system config backup for tenant ${tenantId}.`,
    metadata: {
      requestId,
      tenantId,
      userId: auth.userId,
      sourceCount: payload.sources.length,
      budgetCount: payload.budgets.length,
      pricingEntryCount: payload.pricingCatalog?.entries.length ?? 0,
      sourceLocations: payload.sources.slice(0, 5).map((item) => item.location),
      pricingNote: payload.pricingCatalog?.note,
      schemaVersion: payload.schemaVersion,
    },
  });

  c.header(
    "content-disposition",
    `attachment; filename="${buildBackupFileName(tenantId)}"`
  );
  return c.json(payload);
});

systemConfigRoutes.post("/system/config/restore", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateSystemConfigRestoreInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const tenantId = auth.tenantId;
  const backup = result.data.backup;
  if (backup.tenantId !== tenantId) {
    return c.json({ message: "backup.tenantId 与当前租户不一致，禁止跨租户恢复。" }, 403);
  }

  const dryRun = result.data.dryRun ?? false;
  const restoreSources = result.data.restoreSources ?? true;
  const restoreBudgets = result.data.restoreBudgets ?? true;
  const restorePricingCatalog = result.data.restorePricingCatalog ?? true;

  const warnings: string[] = [];
  const summary: SystemConfigRestoreResult["summary"] = {
    sources: {
      total: restoreSources ? backup.sources.length : 0,
      created: 0,
      skipped: 0,
    },
    budgets: {
      total: restoreBudgets ? backup.budgets.length : 0,
      upserted: 0,
      skipped: 0,
    },
    pricingCatalog: {
      included: restorePricingCatalog && Boolean(backup.pricingCatalog),
      restored: false,
      entryCount: backup.pricingCatalog?.entries.length ?? 0,
    },
  };

  if (restoreSources) {
    const existingSources = await repository.listSources(tenantId);
    const signatures = new Set(
      existingSources.map((item) =>
        buildSourceSignature(toSystemConfigBackupSource(item as SystemConfigBackupSource))
      )
    );
    const backupSignatures = new Set<string>();

    for (const source of backup.sources) {
      const signature = buildSourceSignature(source);
      if (signatures.has(signature) || backupSignatures.has(signature)) {
        summary.sources.skipped += 1;
        continue;
      }

      if (!dryRun) {
        await repository.createSource(tenantId, source);
      }
      summary.sources.created += 1;
      signatures.add(signature);
      backupSignatures.add(signature);
    }
  }

  if (restoreBudgets) {
    for (const budget of backup.budgets) {
      const bindingError = await repository.validateBudgetScopeBinding(tenantId, budget);
      if (bindingError) {
        summary.budgets.skipped += 1;
        warnings.push(
          `budget(scope=${budget.scope}) 绑定校验失败：${bindingError.message}`
        );
        continue;
      }

      if (!dryRun) {
        await repository.upsertBudget(tenantId, budget);
      }
      summary.budgets.upserted += 1;
    }
  }

  if (
    restorePricingCatalog &&
    backup.pricingCatalog &&
    backup.pricingCatalog.entries.length > 0
  ) {
    if (!dryRun) {
      await repository.upsertPricingCatalog(tenantId, {
        note:
          backup.pricingCatalog.note ??
          `Restored from backup ${backup.exportedAt}`,
        entries: backup.pricingCatalog.entries,
      });
    }
    summary.pricingCatalog.restored = true;
  }

  const response: SystemConfigRestoreResult = {
    tenantId,
    dryRun,
    restoredAt: new Date().toISOString(),
    summary,
    warnings,
  };

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: dryRun
      ? "control_plane.system_config_restore_dry_run"
      : "control_plane.system_config_restore_applied",
    level: warnings.length > 0 ? "warning" : "info",
    detail: dryRun
      ? `Previewed tenant system config restore for tenant ${tenantId}.`
      : `Applied tenant system config restore for tenant ${tenantId}.`,
    metadata: {
      requestId,
      tenantId,
      userId: auth.userId,
      schemaVersion: backup.schemaVersion,
      backupExportedAt: backup.exportedAt,
      restoreSources,
      restoreBudgets,
      restorePricingCatalog,
      dryRun,
      sourceLocations: backup.sources.slice(0, 5).map((item) => item.location),
      pricingNote: backup.pricingCatalog?.note,
      summary,
      warningCount: warnings.length,
    },
  });

  return c.json(response);
});
