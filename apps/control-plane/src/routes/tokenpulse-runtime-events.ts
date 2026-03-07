import { Hono, type Context } from "hono";
import {
  validateTokenPulseRuntimeEventIngestInput,
  validateTokenPulseRuntimeEventListInput,
} from "../contracts";
import type { AppendAuditLogInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import { parseOptionalTimePaginationCursor } from "./pagination-cursor";
import {
  computeTokenPulseRuntimeIdempotencyKey,
  computeTokenPulseRuntimeSignature,
  isTokenPulseRuntimeSignatureValid,
  isTokenPulseRuntimeTimestampWithinWindow,
  parseTokenPulseRuntimeTimestampMs,
  resolveTokenPulseRuntimeKeyId,
  resolveTokenPulseRuntimeSecret,
  TOKENPULSE_RUNTIME_IDEMPOTENCY_KEY_HEADER,
  TOKENPULSE_RUNTIME_KEY_ID_HEADER,
  TOKENPULSE_RUNTIME_SIGNATURE_HEADER,
  TOKENPULSE_RUNTIME_SIGNATURE_WINDOW_MS,
  TOKENPULSE_RUNTIME_SPEC_VERSION,
  TOKENPULSE_RUNTIME_SPEC_VERSION_HEADER,
  TOKENPULSE_RUNTIME_TIMESTAMP_HEADER,
} from "./tokenpulse-runtime-signature";
import type { AppEnv } from "../types";

export const tokenPulseRuntimeEventRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const WRITABLE_ROLES = new Set(["owner", "maintainer"]);

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn(
      "[control-plane] 写入 TokenPulse runtime 审计日志失败。",
      error,
    );
  }
}

function unauthorized(c: Context<AppEnv>) {
  return c.json({ message: "未认证：请先登录。" }, 401);
}

function forbidden(c: Context<AppEnv>, mode: "read" | "write") {
  if (mode === "write") {
    return c.json(
      { message: "无写入权限：仅 owner/maintainer 可执行写操作。" },
      403,
    );
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
  const membership = await repository.getTenantMemberByUser(
    auth.tenantId,
    auth.userId,
  );
  if (!membership) {
    return forbidden(c, mode);
  }
  if (mode === "write" && !WRITABLE_ROLES.has(membership.tenantRole)) {
    return forbidden(c, mode);
  }
  return auth;
}

function getRequiredHeader(c: Context<AppEnv>, name: string): string | null {
  const value = c.req.header(name)?.trim();
  return value && value.length > 0 ? value : null;
}

tokenPulseRuntimeEventRoutes.post(
  "/integrations/tokenpulse/runtime-events",
  async (c) => {
    const configuredSecret = resolveTokenPulseRuntimeSecret();
    if (!configuredSecret) {
      return c.json({ message: "服务未配置 TokenPulse webhook secret。" }, 500);
    }

    const specVersion = getRequiredHeader(c, TOKENPULSE_RUNTIME_SPEC_VERSION_HEADER);
    if (specVersion !== TOKENPULSE_RUNTIME_SPEC_VERSION) {
      return c.json({ message: "spec-version 缺失或不受支持。" }, 400);
    }

    const expectedKeyId = resolveTokenPulseRuntimeKeyId();
    const keyId = getRequiredHeader(c, TOKENPULSE_RUNTIME_KEY_ID_HEADER);
    if (!keyId || keyId !== expectedKeyId) {
      return c.json({ message: "未授权：key-id 无效。" }, 401);
    }

    const timestamp = getRequiredHeader(c, TOKENPULSE_RUNTIME_TIMESTAMP_HEADER);
    const timestampMs = parseTokenPulseRuntimeTimestampMs(timestamp ?? undefined);
    if (
      !timestamp ||
      timestampMs === null ||
      !isTokenPulseRuntimeTimestampWithinWindow(
        timestampMs,
        Date.now(),
        TOKENPULSE_RUNTIME_SIGNATURE_WINDOW_MS,
      )
    ) {
      return c.json({ message: "未授权：timestamp 无效或已过期。" }, 401);
    }

    const idempotencyKey = getRequiredHeader(
      c,
      TOKENPULSE_RUNTIME_IDEMPOTENCY_KEY_HEADER,
    );
    if (!idempotencyKey) {
      return c.json({ message: "idempotency-key 缺失。" }, 400);
    }

    const signature = getRequiredHeader(c, TOKENPULSE_RUNTIME_SIGNATURE_HEADER);
    if (!signature) {
      return c.json({ message: "未授权：signature 无效。" }, 401);
    }

    const rawBody = await c.req.text().catch(() => "");
    const expectedSignature = computeTokenPulseRuntimeSignature(
      configuredSecret,
      {
        specVersion,
        keyId,
        timestamp,
        idempotencyKey,
        rawBody,
      },
    );
    if (!isTokenPulseRuntimeSignatureValid(expectedSignature, signature)) {
      return c.json({ message: "未授权：signature 无效。" }, 401);
    }

    let body: unknown;
    try {
      body = rawBody.length > 0 ? JSON.parse(rawBody) : undefined;
    } catch {
      body = undefined;
    }
    const result = validateTokenPulseRuntimeEventIngestInput(body);
    if (!result.success) {
      return c.json({ message: result.error }, 400);
    }

    const recomputedIdempotencyKey = computeTokenPulseRuntimeIdempotencyKey({
      tenantId: result.data.tenantId,
      traceId: result.data.traceId,
      provider: result.data.provider,
      model: result.data.model,
      startedAt: result.data.startedAt,
    });
    if (recomputedIdempotencyKey !== idempotencyKey) {
      return c.json({ message: "idempotency-key 与请求体不一致。" }, 400);
    }

    const tenant = await repository.getTenantById(result.data.tenantId);
    if (!tenant) {
      return c.json(
        { message: `未找到租户 ${result.data.tenantId}。` },
        404,
      );
    }

    const writeResult = await repository.insertTokenPulseRuntimeEvent({
      ...result.data,
      idempotencyKey,
      specVersion: "v1",
      keyId,
    });

    const requestId = c.get("requestId");
    await appendAuditLogSafely({
      tenantId: result.data.tenantId,
      eventId: `cp:${requestId}`,
      action: writeResult.created
        ? "control_plane.tokenpulse_runtime_event_ingested"
        : "control_plane.tokenpulse_runtime_event_duplicate",
      level: "info",
      detail: writeResult.created
        ? `Ingested TokenPulse runtime event ${writeResult.event.id}.`
        : `Duplicate TokenPulse runtime event ${writeResult.event.id}.`,
      metadata: {
        requestId,
        tenantId: writeResult.event.tenantId,
        traceId: writeResult.event.traceId,
        provider: writeResult.event.provider,
        status: writeResult.event.status,
        routePolicy: writeResult.event.routePolicy,
        idempotencyKey: writeResult.event.idempotencyKey,
        specVersion: writeResult.event.specVersion,
        keyId: writeResult.event.keyId,
        duplicate: !writeResult.created,
      },
    });

    return c.json(
      {
        duplicate: !writeResult.created,
        item: writeResult.event,
      },
      writeResult.created ? 202 : 200,
    );
  },
);

tokenPulseRuntimeEventRoutes.get(
  "/integrations/tokenpulse/runtime-events",
  async (c) => {
    const auth = await requireTenantAccess(c, "read");
    if (auth instanceof Response) {
      return auth;
    }

    const validation = validateTokenPulseRuntimeEventListInput(c.req.query());
    if (!validation.success) {
      return c.json({ message: validation.error }, 400);
    }
    const cursorResult = parseOptionalTimePaginationCursor(validation.data.cursor);
    if (!cursorResult.success) {
      return c.json({ message: cursorResult.error }, 400);
    }

    const filters = {
      ...validation.data,
      cursor: cursorResult.cursor,
    };
    const payload = await repository.listTokenPulseRuntimeEvents(
      auth.tenantId,
      filters,
    );

    const requestId = c.get("requestId");
    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:${requestId}`,
      action: "control_plane.tokenpulse_runtime_event_queried",
      level: "info",
      detail: "Queried TokenPulse runtime events.",
      metadata: {
        requestId,
        tenantId: auth.tenantId,
        traceId: filters.traceId,
        provider: filters.provider,
        status: filters.status,
        limit: filters.limit,
        cursor: filters.cursor,
        resultCount: payload.items.length,
        resultTotal: payload.total,
        nextCursor: payload.nextCursor,
      },
    });

    return c.json({
      items: payload.items,
      total: payload.total,
      filters,
      nextCursor: payload.nextCursor,
    });
  },
);
