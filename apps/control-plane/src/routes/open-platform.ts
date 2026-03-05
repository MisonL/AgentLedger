import { createHash } from "node:crypto";
import { Hono, type Context } from "hono";
import {
  validateApiKeyCreateInput,
  validateApiKeyListInput,
  validateApiKeyRevokeInput,
  validateWebhookEndpointCreateInput,
  validateWebhookEndpointUpdateInput,
} from "../contracts";
import type { AppendAuditLogInput, ApiKey, WebhookEndpoint } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const openPlatformRoutes = new Hono<AppEnv>();

const repository = getControlPlaneRepository();
const WRITABLE_ROLES = new Set(["owner", "maintainer"]);
const API_KEY_SCOPE_SET = new Set(["read", "write", "admin"]);
const WEBHOOK_STATUS_SET = new Set(["active", "paused", "disabled"]);
const WEBHOOK_REPLAY_LIMIT_DEFAULT = 100;
const WEBHOOK_REPLAY_LIMIT_MAX = 500;
const WEBHOOK_EVENT_TYPE_SET = new Set([
  "api_key.created",
  "api_key.revoked",
  "quality.event.created",
  "quality.scorecard.updated",
  "replay.job.started",
  "replay.job.completed",
  "replay.job.failed",
]);

type ApiKeyScope = "read" | "write" | "admin";
type ApiKeyStatus = "active" | "revoked" | "expired";
type WebhookEndpointStatus = "active" | "paused" | "disabled";
type WebhookReplayInput = {
  eventType?: string;
  from?: string;
  to?: string;
  limit: number;
  dryRun: boolean;
};

function firstNonEmptyString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeStringRecord(input: unknown): Record<string, unknown> {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return {};
  }
  return input as Record<string, unknown>;
}

function toIsoString(value: unknown): string | undefined {
  const normalized = firstNonEmptyString(value);
  if (!normalized) {
    return undefined;
  }
  const timestamp = Date.parse(normalized);
  if (!Number.isFinite(timestamp)) {
    return undefined;
  }
  return new Date(timestamp).toISOString();
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function randomSecret(prefix: string): string {
  const normalizedPrefix = prefix.replace(/[^a-zA-Z0-9_]/g, "");
  return `${normalizedPrefix}_${crypto.randomUUID().replace(/-/g, "")}`;
}

function resolveApiKeyScope(apiKey: ApiKey): ApiKeyScope {
  for (const scope of apiKey.scopes) {
    const lowered = scope.toLowerCase();
    if (API_KEY_SCOPE_SET.has(lowered)) {
      return lowered as ApiKeyScope;
    }
  }
  return "read";
}

function resolveApiKeyStatus(apiKey: ApiKey): ApiKeyStatus {
  if (apiKey.revokedAt) {
    return "revoked";
  }
  return "active";
}

function mapApiKeyItem(apiKey: ApiKey) {
  return {
    id: apiKey.id,
    tenantId: apiKey.tenantId,
    name: apiKey.name,
    scope: resolveApiKeyScope(apiKey),
    status: resolveApiKeyStatus(apiKey),
    keyPrefix: apiKey.keyHash.slice(0, 12),
    createdByUserId: undefined,
    lastUsedAt: apiKey.lastUsedAt,
    expiresAt: undefined,
    revokedAt: apiKey.revokedAt,
    metadata: {},
    createdAt: apiKey.createdAt,
    updatedAt: apiKey.updatedAt,
  };
}

function toWebhookStatus(enabled: boolean): WebhookEndpointStatus {
  return enabled ? "active" : "paused";
}

function mapWebhookEndpoint(endpoint: WebhookEndpoint) {
  return {
    id: endpoint.id,
    tenantId: endpoint.tenantId,
    name: endpoint.name,
    url: endpoint.url,
    events: endpoint.eventTypes,
    status: toWebhookStatus(endpoint.enabled),
    secretHint: endpoint.secretHash ? `${endpoint.secretHash.slice(0, 8)}***` : undefined,
    failureCount: 0,
    lastSuccessAt: undefined,
    lastFailureAt: undefined,
    createdAt: endpoint.createdAt,
    updatedAt: endpoint.updatedAt,
  };
}

function validateWebhookReplayInput(input: unknown): {
  success: true;
  data: WebhookReplayInput;
} | {
  success: false;
  error: string;
} {
  if (
    input !== undefined &&
    (typeof input !== "object" || input === null || Array.isArray(input))
  ) {
    return { success: false, error: "请求体必须是对象。" };
  }
  const body = normalizeStringRecord(input);
  const eventType = firstNonEmptyString(body.eventType);
  if (body.eventType !== undefined && !eventType) {
    return { success: false, error: "eventType 必须为非空字符串。" };
  }
  if (eventType && !WEBHOOK_EVENT_TYPE_SET.has(eventType)) {
    return {
      success: false,
      error:
        "eventType 仅支持 api_key.created/api_key.revoked/quality.event.created/quality.scorecard.updated/replay.job.started/replay.job.completed/replay.job.failed。",
    };
  }

  const from = toIsoString(body.from);
  if (body.from !== undefined && !from) {
    return { success: false, error: "from 必须是 ISO 日期字符串。" };
  }
  const to = toIsoString(body.to);
  if (body.to !== undefined && !to) {
    return { success: false, error: "to 必须是 ISO 日期字符串。" };
  }
  if (from && to && Date.parse(from) > Date.parse(to)) {
    return { success: false, error: "from 不能晚于 to。" };
  }

  let dryRun = true;
  if (body.dryRun !== undefined) {
    if (typeof body.dryRun !== "boolean") {
      return { success: false, error: "dryRun 必须是布尔值。" };
    }
    dryRun = body.dryRun;
  }

  let limit = WEBHOOK_REPLAY_LIMIT_DEFAULT;
  if (body.limit !== undefined) {
    if (typeof body.limit !== "number" || !Number.isFinite(body.limit)) {
      return { success: false, error: "limit 必须是整数。" };
    }
    const normalizedLimit = Math.trunc(body.limit);
    if (
      normalizedLimit <= 0 ||
      normalizedLimit > WEBHOOK_REPLAY_LIMIT_MAX
    ) {
      return {
        success: false,
        error: `limit 必须是 1 到 ${WEBHOOK_REPLAY_LIMIT_MAX} 的整数。`,
      };
    }
    limit = normalizedLimit;
  }

  return {
    success: true,
    data: {
      eventType,
      from: from ?? undefined,
      to: to ?? undefined,
      limit,
      dryRun,
    },
  };
}

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 open-platform 审计日志失败。", error);
  }
}

function unauthorized(c: Context<AppEnv>) {
  return c.json({ message: "未认证：请先登录。" }, 401);
}

function forbidden(c: Context<AppEnv>, mode: "read" | "write") {
  if (mode === "write") {
    return c.json({ message: "无写入权限：仅 owner/maintainer 可执行写操作。" }, 403);
  }
  return c.json({ message: "无权访问该租户资源。" }, 403);
}

async function requireAuthContext(c: Context<AppEnv>) {
  const authResult = await authMiddleware(c, async () => {});
  if (authResult instanceof Response) {
    return authResult;
  }
  const auth = c.get("auth");
  if (!auth) {
    return unauthorized(c);
  }
  return auth;
}

async function requireTenantAccess(c: Context<AppEnv>, mode: "read" | "write") {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }
  const membership = await repository.getTenantMemberByUser(auth.tenantId, auth.userId);
  if (!membership) {
    return forbidden(c, mode);
  }
  if (mode === "write" && !WRITABLE_ROLES.has(membership.tenantRole)) {
    return forbidden(c, mode);
  }
  return auth;
}

function buildOpenApiDocument() {
  return {
    openapi: "3.0.3",
    info: {
      title: "AgentLedger Control Plane API",
      version: "1.0.0",
      description: "最小可用 OpenAPI 文档（含 open-platform / quality / replay 摘要路径）。",
    },
    paths: {
      "/api/v1/openapi.json": {
        get: { summary: "获取最小 OpenAPI 文档" },
      },
      "/api/v1/api-keys": {
        get: { summary: "列出 API Keys" },
        post: { summary: "创建 API Key" },
      },
      "/api/v1/api-keys/{id}/revoke": {
        post: { summary: "吊销 API Key" },
      },
      "/api/v1/webhooks": {
        get: { summary: "列出 Webhook" },
        post: { summary: "创建 Webhook" },
      },
      "/api/v1/webhooks/{id}": {
        put: { summary: "更新 Webhook" },
        delete: { summary: "删除 Webhook" },
      },
      "/api/v1/webhooks/{id}/replay": {
        post: { summary: "重放 Webhook" },
      },
      "/api/v1/quality/events": {
        post: { summary: "上报质量事件" },
      },
      "/api/v1/quality/metrics/daily": {
        get: { summary: "查询质量日报" },
      },
      "/api/v1/quality/scorecards": {
        get: { summary: "列出质量评分卡" },
      },
      "/api/v1/quality/scorecards/{id}": {
        put: { summary: "更新质量评分卡" },
      },
      "/api/v1/replay/baselines": {
        post: { summary: "创建回放基线" },
        get: { summary: "列出回放基线" },
      },
      "/api/v1/replay/jobs": {
        post: { summary: "创建回放任务" },
        get: { summary: "列出回放任务" },
      },
      "/api/v1/replay/jobs/{id}": {
        get: { summary: "查询回放任务" },
      },
      "/api/v1/replay/jobs/{id}/diff": {
        get: { summary: "查询回放差异" },
      },
    },
  };
}

openPlatformRoutes.get("/openapi.json", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  return c.json({
    ...buildOpenApiDocument(),
    tenantId: auth.tenantId,
  });
});

openPlatformRoutes.get("/api-keys", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const validation = validateApiKeyListInput({
    ...c.req.query(),
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const filter = validation.data;
  const fromTimestamp = filter.from ? Date.parse(filter.from) : undefined;
  const toTimestamp = filter.to ? Date.parse(filter.to) : undefined;
  const keyword = firstNonEmptyString(filter.keyword)?.toLowerCase();
  const limit = filter.limit ?? 200;

  const items = (await repository.listApiKeys(auth.tenantId))
    .map(mapApiKeyItem)
    .filter((item) => (filter.scope ? item.scope === filter.scope : true))
    .filter((item) => (filter.status ? item.status === filter.status : true))
    .filter((item) => {
      if (!keyword) {
        return true;
      }
      return (
        item.name.toLowerCase().includes(keyword) ||
        item.keyPrefix.toLowerCase().includes(keyword)
      );
    })
    .filter((item) => {
      if (fromTimestamp === undefined && toTimestamp === undefined) {
        return true;
      }
      const createdAtTimestamp = Date.parse(item.createdAt);
      if (!Number.isFinite(createdAtTimestamp)) {
        return false;
      }
      if (fromTimestamp !== undefined && createdAtTimestamp < fromTimestamp) {
        return false;
      }
      if (toTimestamp !== undefined && createdAtTimestamp > toTimestamp) {
        return false;
      }
      return true;
    })
    .slice(0, limit);

  return c.json({
    items,
    total: items.length,
    filters: filter,
  });
});

openPlatformRoutes.post("/api-keys", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeStringRecord(body);
  const validation = validateApiKeyCreateInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const secret = randomSecret("sk_live");
  const keyHash = sha256(secret);
  const created = await repository.createApiKey(auth.tenantId, {
    name: validation.data.name,
    keyHash,
    scopes: [validation.data.scope],
  });

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.open_platform.api_key_created",
    level: "info",
    detail: `Created API key ${created.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      keyId: created.id,
      scope: validation.data.scope,
    },
  });

  return c.json(
    {
      id: created.id,
      tenantId: auth.tenantId,
      keyId: created.id,
      keyPrefix: secret.slice(0, 12),
      secret,
      createdAt: created.createdAt,
    },
    201
  );
});

openPlatformRoutes.post("/api-keys/:id/revoke", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const apiKeyId = c.req.param("id")?.trim();
  if (!apiKeyId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeStringRecord(body);
  const validation = validateApiKeyRevokeInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
    keyId: apiKeyId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const revoked = await repository.revokeApiKey(auth.tenantId, apiKeyId);
  if (!revoked) {
    return c.json({ message: `未找到 API Key：${apiKeyId}` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.open_platform.api_key_revoked",
    level: "warning",
    detail: `Revoked API key ${revoked.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      keyId: revoked.id,
      reason: validation.data.reason,
    },
  });

  return c.json(mapApiKeyItem(revoked));
});

openPlatformRoutes.get("/webhooks", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const limitQuery = c.req.query("limit");
  const limit = limitQuery ? Number(limitQuery) : 200;
  if (!Number.isInteger(limit) || limit <= 0 || limit > 500) {
    return c.json({ message: "limit 必须是 1 到 500 的整数。" }, 400);
  }
  const statusQuery = firstNonEmptyString(c.req.query("status"));
  if (statusQuery && !WEBHOOK_STATUS_SET.has(statusQuery)) {
    return c.json({ message: "status 必须是 active/paused/disabled 之一。" }, 400);
  }
  const keyword = firstNonEmptyString(c.req.query("keyword"))?.toLowerCase();

  const items = (await repository.listWebhookEndpoints(auth.tenantId, limit))
    .map(mapWebhookEndpoint)
    .filter((item) => (statusQuery ? item.status === statusQuery : true))
    .filter((item) => (keyword ? item.name.toLowerCase().includes(keyword) : true));

  return c.json({
    items,
    total: items.length,
  });
});

openPlatformRoutes.post("/webhooks", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeStringRecord(body);
  const validation = validateWebhookEndpointCreateInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const secret = validation.data.secret ?? randomSecret("whsec");
  const secretHash = sha256(secret);
  try {
    const endpoint = await repository.createWebhookEndpoint(auth.tenantId, {
      name: validation.data.name,
      url: validation.data.url,
      eventTypes: validation.data.events,
      enabled: validation.data.status === "active",
      secretHash,
      headers: {},
    });

    const requestId = c.get("requestId");
    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:${requestId}`,
      action: "control_plane.open_platform.webhook_created",
      level: "info",
      detail: `Created webhook ${endpoint.id}.`,
      metadata: {
        requestId,
        tenantId: auth.tenantId,
        webhookId: endpoint.id,
        status: validation.data.status,
      },
    });

    return c.json(
      {
        ...mapWebhookEndpoint(endpoint),
        secret,
      },
      201
    );
  } catch (error) {
    if (
      error instanceof Error &&
      error.message.startsWith("webhook_endpoint_name_already_exists:")
    ) {
      return c.json({ message: "Webhook 名称已存在，请更换后重试。" }, 409);
    }
    throw error;
  }
});

openPlatformRoutes.put("/webhooks/:id", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const endpointId = c.req.param("id")?.trim();
  if (!endpointId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeStringRecord(body);
  const validation = validateWebhookEndpointUpdateInput({
    ...bodyRecord,
    endpointId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const updated = await repository.updateWebhookEndpoint(auth.tenantId, endpointId, {
    name: validation.data.name,
    url: validation.data.url,
    eventTypes: validation.data.events,
    enabled:
      validation.data.status !== undefined ? validation.data.status === "active" : undefined,
    secretHash: validation.data.secret ? sha256(validation.data.secret) : undefined,
  });
  if (!updated) {
    return c.json({ message: `未找到 Webhook：${endpointId}` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.open_platform.webhook_updated",
    level: "info",
    detail: `Updated webhook ${updated.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      webhookId: updated.id,
    },
  });

  return c.json({
    ...mapWebhookEndpoint(updated),
    secret: validation.data.secret,
  });
});

openPlatformRoutes.delete("/webhooks/:id", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const endpointId = c.req.param("id")?.trim();
  if (!endpointId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const deleted = await repository.deleteWebhookEndpoint(auth.tenantId, endpointId);
  if (!deleted) {
    return c.json({ message: `未找到 Webhook：${endpointId}` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.open_platform.webhook_deleted",
    level: "warning",
    detail: `Deleted webhook ${endpointId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      webhookId: endpointId,
    },
  });

  return c.json({ success: true });
});

openPlatformRoutes.post("/webhooks/:id/replay", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const endpointId = c.req.param("id")?.trim();
  if (!endpointId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const validation = validateWebhookReplayInput(body);
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const endpoint = await repository.getWebhookEndpointById(auth.tenantId, endpointId);
  if (!endpoint) {
    return c.json({ message: `未找到 Webhook：${endpointId}` }, 404);
  }

  const replayId = crypto.randomUUID();
  const requestedAt = new Date().toISOString();
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.open_platform.webhook_replayed",
    level: "info",
    detail: `Replay requested for webhook ${endpoint.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      webhookId: endpoint.id,
      replayId,
      replay: validation.data,
    },
  });

  return c.json(
    {
      id: replayId,
      webhookId: endpoint.id,
      status: "queued",
      dryRun: validation.data.dryRun,
      filters: {
        eventType: validation.data.eventType,
        from: validation.data.from,
        to: validation.data.to,
        limit: validation.data.limit,
      },
      requestedAt,
    },
    202
  );
});
