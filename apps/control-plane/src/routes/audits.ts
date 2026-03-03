import { Hono, type Context } from "hono";
import { validateAuditListInput } from "../contracts";
import {
  getControlPlaneRepository,
  type AppendAuditLogInput,
} from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const auditRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 audits 访问审计失败。", error);
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

function normalizeOptionalQuery(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

auditRoutes.get("/audits", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const query = c.req.query();
  const result = validateAuditListInput(query);

  if (!result.success) {
    return c.json(
      {
        message: result.error,
      },
      400
    );
  }

  const eventId = normalizeOptionalQuery(query.eventId);
  const actionFilter = normalizeOptionalQuery(query.action);
  const keyword = normalizeOptionalQuery(query.keyword);

  if (query.eventId !== undefined && !eventId) {
    return c.json({ message: "eventId 必须为非空字符串。" }, 400);
  }
  if (query.action !== undefined && !actionFilter) {
    return c.json({ message: "action 必须为非空字符串。" }, 400);
  }
  if (query.keyword !== undefined && !keyword) {
    return c.json({ message: "keyword 必须为非空字符串。" }, 400);
  }

  const filters = {
    ...result.data,
    eventId,
    action: actionFilter,
    keyword,
  };
  const payload = await repository.listAudits(filters, auth.tenantId);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:audit-query`,
    action: "audit.query",
    level: "info",
    detail: "查询审计日志。",
    metadata: {
      tenantId: auth.tenantId,
      userId: auth.userId,
      requestId,
      route: "/api/v1/audits",
      eventId: filters.eventId,
      actionFilter: filters.action,
      levelFilter: filters.level,
      keyword: filters.keyword,
      from: filters.from,
      to: filters.to,
      limit: filters.limit,
      resultCount: payload.items.length,
      resultTotal: payload.total,
    },
  });

  return c.json({
    items: payload.items,
    total: payload.total,
    filters,
    nextCursor: null,
  });
});
