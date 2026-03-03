import { Hono, type Context } from "hono";
import { validateSessionSearchInput } from "../contracts";
import type { SessionDetail } from "../contracts";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const sessionRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const DEFAULT_SESSION_EVENT_LIMIT = 500;
const MAX_SESSION_EVENT_LIMIT = 2_000;

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

function normalizeSessionId(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function parseSessionEventLimit(
  value: string | undefined
): { success: true; limit: number } | { success: false; error: string } {
  if (value === undefined) {
    return { success: true, limit: DEFAULT_SESSION_EVENT_LIMIT };
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return { success: false, error: "limit 必须是 1 到 2000 的整数。" };
  }

  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_SESSION_EVENT_LIMIT) {
    return { success: false, error: "limit 必须是 1 到 2000 的整数。" };
  }

  return { success: true, limit: parsed };
}

function buildSessionDetailResponse(detail: SessionDetail) {
  return {
    ...detail,
    session: detail,
    tokenBreakdown: {
      inputTokens: detail.inputTokens,
      outputTokens: detail.outputTokens,
      cacheReadTokens: detail.cacheReadTokens,
      cacheWriteTokens: detail.cacheWriteTokens,
      reasoningTokens: detail.reasoningTokens,
      totalTokens: detail.tokens,
    },
    sourceTrace: {
      sourceId: detail.sourceId,
      sourceName: detail.sourceName,
      provider: detail.provider ?? "unknown",
      path: detail.sourcePath ?? detail.workspace,
    },
  };
}

sessionRoutes.post("/sessions/search", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateSessionSearchInput(body);

  if (!result.success) {
    return c.json(
      {
        message: result.error,
      },
      400
    );
  }

  const payload = await repository.searchSessions(result.data, auth.tenantId);

  return c.json({
    items: payload.items,
    total: payload.total,
    nextCursor: null,
    filters: result.data,
  });
});

sessionRoutes.get("/sessions/:id", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const sessionId = normalizeSessionId(c.req.param("id"));
  if (!sessionId) {
    return c.json({ message: "sessionId 必须为非空字符串。" }, 400);
  }

  const detail = await repository.getSessionById(auth.tenantId, sessionId);
  if (!detail) {
    return c.json({ message: `未找到会话 ${sessionId}。` }, 404);
  }

  return c.json(buildSessionDetailResponse(detail));
});

sessionRoutes.get("/sessions/:id/events", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const sessionId = normalizeSessionId(c.req.param("id"));
  if (!sessionId) {
    return c.json({ message: "sessionId 必须为非空字符串。" }, 400);
  }

  const sessionDetail = await repository.getSessionById(auth.tenantId, sessionId);
  if (!sessionDetail) {
    return c.json({ message: `未找到会话 ${sessionId}。` }, 404);
  }

  const limitResult = parseSessionEventLimit(c.req.query("limit"));
  if (!limitResult.success) {
    return c.json({ message: limitResult.error }, 400);
  }

  const items = await repository.listSessionEvents(
    auth.tenantId,
    sessionId,
    limitResult.limit
  );
  return c.json({
    items,
    total: items.length,
    limit: limitResult.limit,
  });
});
