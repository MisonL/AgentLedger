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

export function buildOpenApiDocument() {
  const standardErrorSchema = {
    $ref: "#/components/schemas/ErrorResponse",
  };

  return {
    openapi: "3.0.3",
    info: {
      title: "AgentLedger Control Plane API",
      version: "1.1.0",
      description: "Open Platform / Quality / Replay 核心路径文档。",
    },
    tags: [
      { name: "open-platform", description: "开放平台与 Webhook 能力" },
      { name: "quality", description: "质量治理能力" },
      { name: "replay", description: "回放评测能力" },
    ],
    security: [{ bearerAuth: [] }],
    paths: {
      "/api/v1/openapi.json": {
        get: {
          summary: "获取 OpenAPI 文档",
          operationId: "getOpenApiDocument",
          tags: ["open-platform"],
          responses: {
            "200": {
              description: "OpenAPI 文档",
              content: {
                "application/json": {
                  schema: {
                    $ref: "#/components/schemas/OpenApiDocument",
                  },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
          },
        },
      },
      "/api/v1/api-keys": {
        get: {
          summary: "列出 API Keys",
          operationId: "listApiKeys",
          tags: ["open-platform"],
          parameters: [
            { $ref: "#/components/parameters/PageLimit" },
            { $ref: "#/components/parameters/PageCursor" },
            {
              name: "scope",
              in: "query",
              description: "按 scope 过滤",
              schema: { type: "string", enum: ["read", "write", "admin"] },
            },
            {
              name: "status",
              in: "query",
              description: "按状态过滤",
              schema: { type: "string", enum: ["active", "revoked", "expired"] },
            },
            {
              name: "keyword",
              in: "query",
              description: "名称/前缀关键字",
              schema: { type: "string" },
            },
            {
              name: "from",
              in: "query",
              description: "创建时间起点（ISO 日期）",
              schema: { type: "string", format: "date-time" },
            },
            {
              name: "to",
              in: "query",
              description: "创建时间终点（ISO 日期）",
              schema: { type: "string", format: "date-time" },
            },
          ],
          responses: {
            "200": {
              description: "API Key 列表",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ApiKeyListResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        post: {
          summary: "创建 API Key",
          operationId: "createApiKey",
          tags: ["open-platform"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ApiKeyCreateRequest" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ApiKeyCreateResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/api-keys/{id}/revoke": {
        post: {
          summary: "吊销 API Key",
          operationId: "revokeApiKey",
          tags: ["open-platform"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: false,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ApiKeyRevokeRequest" },
              },
            },
          },
          responses: {
            "200": {
              description: "吊销成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ApiKey" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/webhooks": {
        get: {
          summary: "列出 Webhook",
          operationId: "listWebhookEndpoints",
          tags: ["open-platform"],
          parameters: [
            { $ref: "#/components/parameters/PageLimit" },
            {
              name: "status",
              in: "query",
              description: "按状态过滤",
              schema: { type: "string", enum: ["active", "paused", "disabled"] },
            },
            {
              name: "keyword",
              in: "query",
              description: "Webhook 名称关键字",
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "Webhook 列表",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/WebhookListResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        post: {
          summary: "创建 Webhook",
          operationId: "createWebhookEndpoint",
          tags: ["open-platform"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/WebhookCreateRequest" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/WebhookCreateResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "409": { $ref: "#/components/responses/ConflictError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/webhooks/{id}": {
        put: {
          summary: "更新 Webhook",
          operationId: "updateWebhookEndpoint",
          tags: ["open-platform"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/WebhookUpdateRequest" },
              },
            },
          },
          responses: {
            "200": {
              description: "更新成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/WebhookCreateResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        delete: {
          summary: "删除 Webhook",
          operationId: "deleteWebhookEndpoint",
          tags: ["open-platform"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "删除结果",
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    required: ["success"],
                    properties: {
                      success: { type: "boolean" },
                    },
                  },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/webhooks/{id}/replay": {
        post: {
          summary: "重放 Webhook",
          operationId: "replayWebhookEndpoint",
          tags: ["open-platform"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: false,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/WebhookReplayRequest" },
              },
            },
          },
          responses: {
            "202": {
              description: "已接收重放请求",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/WebhookReplayResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/quality/events": {
        post: {
          summary: "上报质量事件",
          operationId: "createQualityEvent",
          tags: ["quality"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/QualityEventInput" },
              },
            },
          },
          responses: {
            "202": {
              description: "事件已接收",
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    required: ["accepted"],
                    properties: {
                      accepted: { type: "boolean" },
                    },
                  },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/quality/metrics/daily": {
        get: {
          summary: "查询质量日报",
          operationId: "listQualityDailyMetrics",
          tags: ["quality"],
          parameters: [{ $ref: "#/components/parameters/PageLimit" }],
          responses: {
            "200": {
              description: "质量日报",
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    required: ["items"],
                    properties: {
                      items: {
                        type: "array",
                        items: { $ref: "#/components/schemas/QualityDailyMetric" },
                      },
                    },
                  },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/quality/scorecards": {
        get: {
          summary: "列出质量评分卡",
          operationId: "listQualityScorecards",
          tags: ["quality"],
          parameters: [{ $ref: "#/components/parameters/PageLimit" }],
          responses: {
            "200": {
              description: "评分卡列表",
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    required: ["items"],
                    properties: {
                      items: {
                        type: "array",
                        items: { $ref: "#/components/schemas/QualityScorecard" },
                      },
                    },
                  },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/quality/scorecards/{id}": {
        put: {
          summary: "更新质量评分卡",
          operationId: "updateQualityScorecard",
          tags: ["quality"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/QualityScorecard" },
              },
            },
          },
          responses: {
            "200": {
              description: "更新后的评分卡",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/QualityScorecard" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/replay/baselines": {
        post: {
          summary: "创建回放基线",
          operationId: "createReplayBaseline",
          tags: ["replay"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ReplayBaselineInput" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayBaseline" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        get: {
          summary: "列出回放基线",
          operationId: "listReplayBaselines",
          tags: ["replay"],
          parameters: [{ $ref: "#/components/parameters/PageLimit" }],
          responses: {
            "200": {
              description: "回放基线列表",
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    required: ["items"],
                    properties: {
                      items: {
                        type: "array",
                        items: { $ref: "#/components/schemas/ReplayBaseline" },
                      },
                    },
                  },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/replay/jobs": {
        post: {
          summary: "创建回放任务",
          operationId: "createReplayJob",
          tags: ["replay"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ReplayJobInput" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayJob" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        get: {
          summary: "列出回放任务",
          operationId: "listReplayJobs",
          tags: ["replay"],
          parameters: [{ $ref: "#/components/parameters/PageLimit" }],
          responses: {
            "200": {
              description: "回放任务列表",
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    required: ["items"],
                    properties: {
                      items: {
                        type: "array",
                        items: { $ref: "#/components/schemas/ReplayJob" },
                      },
                    },
                  },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/replay/jobs/{id}": {
        get: {
          summary: "查询回放任务",
          operationId: "getReplayJob",
          tags: ["replay"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "回放任务详情",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayJob" },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/replay/jobs/{id}/diff": {
        get: {
          summary: "查询回放差异",
          operationId: "getReplayJobDiff",
          tags: ["replay"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "回放差异详情",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayDiff" },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
    },
    components: {
      securitySchemes: {
        bearerAuth: {
          type: "http",
          scheme: "bearer",
          bearerFormat: "JWT",
          description: "使用 Authorization: Bearer <access_token> 访问。",
        },
      },
      parameters: {
        PageLimit: {
          name: "limit",
          in: "query",
          description: "分页大小，默认 100，最大 500。",
          schema: {
            type: "integer",
            minimum: 1,
            maximum: 500,
            default: 100,
          },
        },
        PageCursor: {
          name: "cursor",
          in: "query",
          description: "分页游标（预留参数，当前可忽略）。",
          schema: {
            type: "string",
          },
        },
      },
      responses: {
        BadRequestError: {
          description: "请求参数不合法。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
        UnauthorizedError: {
          description: "未认证或 token 无效。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
        ForbiddenError: {
          description: "无权访问当前租户资源。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
        NotFoundError: {
          description: "资源不存在。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
        ConflictError: {
          description: "请求与当前资源状态冲突。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
        InternalServerError: {
          description: "服务端错误。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
      },
      schemas: {
        OpenApiDocument: {
          type: "object",
          required: ["openapi", "info", "paths", "components"],
          properties: {
            openapi: { type: "string" },
            info: { type: "object", additionalProperties: true },
            paths: { type: "object", additionalProperties: true },
            components: { type: "object", additionalProperties: true },
          },
        },
        ErrorResponse: {
          type: "object",
          required: ["message"],
          properties: {
            message: { type: "string" },
            requestId: { type: "string" },
            code: { type: "string" },
            details: {
              type: "object",
              additionalProperties: true,
            },
          },
        },
        PaginationMeta: {
          type: "object",
          required: ["total"],
          properties: {
            total: { type: "integer", minimum: 0 },
            nextCursor: { type: "string", nullable: true },
            limit: { type: "integer", minimum: 1 },
          },
        },
        ApiKey: {
          type: "object",
          required: ["id", "tenantId", "name", "scope", "status", "keyPrefix", "createdAt", "updatedAt"],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            name: { type: "string" },
            scope: { type: "string", enum: ["read", "write", "admin"] },
            status: { type: "string", enum: ["active", "revoked", "expired"] },
            keyPrefix: { type: "string" },
            createdByUserId: { type: "string", nullable: true },
            lastUsedAt: { type: "string", format: "date-time", nullable: true },
            expiresAt: { type: "string", format: "date-time", nullable: true },
            revokedAt: { type: "string", format: "date-time", nullable: true },
            metadata: { type: "object", additionalProperties: true },
            createdAt: { type: "string", format: "date-time" },
            updatedAt: { type: "string", format: "date-time" },
          },
        },
        ApiKeyCreateRequest: {
          type: "object",
          required: ["name"],
          properties: {
            name: { type: "string", minLength: 1, maxLength: 100 },
            scope: { type: "string", enum: ["read", "write", "admin"] },
          },
        },
        ApiKeyCreateResponse: {
          type: "object",
          required: ["id", "tenantId", "keyId", "keyPrefix", "secret", "createdAt"],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            keyId: { type: "string" },
            keyPrefix: { type: "string" },
            secret: { type: "string" },
            createdAt: { type: "string", format: "date-time" },
          },
        },
        ApiKeyRevokeRequest: {
          type: "object",
          properties: {
            reason: { type: "string", maxLength: 500 },
          },
        },
        ApiKeyListResponse: {
          type: "object",
          required: ["items", "total"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/ApiKey" },
            },
            total: { type: "integer", minimum: 0 },
            filters: {
              type: "object",
              additionalProperties: true,
            },
            pagination: { $ref: "#/components/schemas/PaginationMeta" },
          },
        },
        WebhookEndpoint: {
          type: "object",
          required: ["id", "tenantId", "name", "url", "events", "status", "createdAt", "updatedAt"],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            name: { type: "string" },
            url: { type: "string", format: "uri" },
            events: { type: "array", items: { type: "string" } },
            status: { type: "string", enum: ["active", "paused", "disabled"] },
            secretHint: { type: "string", nullable: true },
            failureCount: { type: "integer", minimum: 0 },
            lastSuccessAt: { type: "string", format: "date-time", nullable: true },
            lastFailureAt: { type: "string", format: "date-time", nullable: true },
            createdAt: { type: "string", format: "date-time" },
            updatedAt: { type: "string", format: "date-time" },
          },
        },
        WebhookCreateRequest: {
          type: "object",
          required: ["name", "url", "events"],
          properties: {
            name: { type: "string", minLength: 1, maxLength: 120 },
            url: { type: "string", format: "uri" },
            events: {
              type: "array",
              minItems: 1,
              items: { type: "string" },
            },
            status: { type: "string", enum: ["active", "paused", "disabled"] },
            secret: { type: "string" },
          },
        },
        WebhookUpdateRequest: {
          type: "object",
          minProperties: 1,
          properties: {
            name: { type: "string", minLength: 1, maxLength: 120 },
            url: { type: "string", format: "uri" },
            events: {
              type: "array",
              minItems: 1,
              items: { type: "string" },
            },
            status: { type: "string", enum: ["active", "paused", "disabled"] },
            secret: { type: "string" },
          },
        },
        WebhookCreateResponse: {
          allOf: [
            { $ref: "#/components/schemas/WebhookEndpoint" },
            {
              type: "object",
              properties: {
                secret: { type: "string", nullable: true },
              },
            },
          ],
        },
        WebhookListResponse: {
          type: "object",
          required: ["items", "total"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/WebhookEndpoint" },
            },
            total: { type: "integer", minimum: 0 },
            pagination: { $ref: "#/components/schemas/PaginationMeta" },
          },
        },
        WebhookReplayFilter: {
          type: "object",
          required: ["limit"],
          properties: {
            eventType: { type: "string" },
            from: { type: "string", format: "date-time", nullable: true },
            to: { type: "string", format: "date-time", nullable: true },
            limit: { type: "integer", minimum: 1, maximum: 500 },
          },
        },
        WebhookReplayRequest: {
          type: "object",
          properties: {
            eventType: { type: "string" },
            from: { type: "string", format: "date-time" },
            to: { type: "string", format: "date-time" },
            limit: { type: "integer", minimum: 1, maximum: 500 },
            dryRun: { type: "boolean" },
          },
        },
        WebhookReplayResponse: {
          type: "object",
          required: ["id", "webhookId", "status", "dryRun", "filters", "requestedAt"],
          properties: {
            id: { type: "string" },
            webhookId: { type: "string" },
            status: { type: "string", enum: ["queued", "running", "completed", "failed"] },
            dryRun: { type: "boolean" },
            filters: { $ref: "#/components/schemas/WebhookReplayFilter" },
            requestedAt: { type: "string", format: "date-time" },
          },
        },
        QualityEventInput: {
          type: "object",
          required: ["sessionId", "eventType", "occurredAt"],
          properties: {
            sessionId: { type: "string" },
            eventType: { type: "string" },
            score: { type: "number" },
            occurredAt: { type: "string", format: "date-time" },
            metadata: { type: "object", additionalProperties: true },
          },
        },
        QualityDailyMetric: {
          type: "object",
          required: ["date", "eventCount"],
          properties: {
            date: { type: "string", format: "date" },
            eventCount: { type: "integer", minimum: 0 },
            issueCount: { type: "integer", minimum: 0 },
            scoreAvg: { type: "number" },
          },
        },
        QualityScorecard: {
          type: "object",
          required: ["id", "name", "updatedAt"],
          properties: {
            id: { type: "string" },
            name: { type: "string" },
            description: { type: "string" },
            weight: { type: "number" },
            updatedAt: { type: "string", format: "date-time" },
          },
        },
        ReplayBaselineInput: {
          type: "object",
          required: ["name"],
          properties: {
            name: { type: "string", minLength: 1, maxLength: 120 },
            description: { type: "string" },
            metadata: { type: "object", additionalProperties: true },
          },
        },
        ReplayBaseline: {
          type: "object",
          required: ["id", "name", "createdAt"],
          properties: {
            id: { type: "string" },
            name: { type: "string" },
            description: { type: "string" },
            metadata: { type: "object", additionalProperties: true },
            createdAt: { type: "string", format: "date-time" },
          },
        },
        ReplayJobInput: {
          type: "object",
          required: ["baselineId"],
          properties: {
            baselineId: { type: "string" },
            sourceSessionId: { type: "string" },
            targetSessionId: { type: "string" },
            options: { type: "object", additionalProperties: true },
          },
        },
        ReplayJob: {
          type: "object",
          required: ["id", "baselineId", "status", "createdAt"],
          properties: {
            id: { type: "string" },
            baselineId: { type: "string" },
            status: {
              type: "string",
              enum: ["pending", "running", "succeeded", "failed", "cancelled"],
            },
            score: { type: "number", nullable: true },
            createdAt: { type: "string", format: "date-time" },
            updatedAt: { type: "string", format: "date-time", nullable: true },
          },
        },
        ReplayDiff: {
          type: "object",
          required: ["jobId"],
          properties: {
            jobId: { type: "string" },
            summary: { type: "string" },
            regressions: { type: "integer", minimum: 0 },
            improvements: { type: "integer", minimum: 0 },
            details: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: true,
              },
            },
          },
        },
      },
    },
  };
}

openPlatformRoutes.get("/openapi.json", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  return c.json(buildOpenApiDocument());
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
