import { Hono, type Context } from "hono";
import { validateSessionSearchInput } from "../contracts";
import type { SessionDetail, Source } from "../contracts";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const sessionRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const DEFAULT_SESSION_EVENT_LIMIT = 500;
const MAX_SESSION_EVENT_LIMIT = 2_000;
const DEFAULT_PULLER_BASE_URL = "http://127.0.0.1:8086";
const DEFAULT_PULLER_SYNC_TIMEOUT_MS = 1_200;
const MIN_PULLER_SYNC_TIMEOUT_MS = 100;
const MAX_PULLER_SYNC_TIMEOUT_MS = 30_000;
const DEFAULT_PULLER_SYNC_RETRY_MAX_ATTEMPTS = 3;
const MIN_PULLER_SYNC_RETRY_MAX_ATTEMPTS = 1;
const MAX_PULLER_SYNC_RETRY_MAX_ATTEMPTS = 10;
const DEFAULT_PULLER_SYNC_RETRY_BASE_BACKOFF_MS = 200;
const DEFAULT_PULLER_SYNC_RETRY_MAX_BACKOFF_MS = 2_000;
const MIN_PULLER_SYNC_RETRY_BACKOFF_MS = 1;
const MAX_PULLER_SYNC_RETRY_BACKOFF_MS = 60_000;

type SessionSearchFetchPath = "realtime" | "fallback-cache" | "cache";

interface SourceFreshnessMetadata {
  fetchPath: SessionSearchFetchPath;
  freshnessMinutes: number | null;
  fallbackReason: string | null;
  accessMode: Source["accessMode"] | null;
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

function getPullerBaseUrl(): string {
  const rawValue = (Bun.env.PULLER_BASE_URL ?? "").trim();
  const baseUrl = rawValue.length > 0 ? rawValue : DEFAULT_PULLER_BASE_URL;
  return baseUrl.replace(/\/+$/, "");
}

function getPullerSyncTimeoutMs(): number {
  const rawValue = (Bun.env.PULLER_SYNC_TIMEOUT_MS ?? "").trim();
  if (!rawValue) {
    return DEFAULT_PULLER_SYNC_TIMEOUT_MS;
  }
  if (!/^\d+$/.test(rawValue)) {
    return DEFAULT_PULLER_SYNC_TIMEOUT_MS;
  }

  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_PULLER_SYNC_TIMEOUT_MS;
  }
  if (parsed < MIN_PULLER_SYNC_TIMEOUT_MS) {
    return MIN_PULLER_SYNC_TIMEOUT_MS;
  }
  if (parsed > MAX_PULLER_SYNC_TIMEOUT_MS) {
    return MAX_PULLER_SYNC_TIMEOUT_MS;
  }
  return parsed;
}

function parseBoundedInteger(
  raw: string | undefined,
  fallback: number,
  min: number,
  max: number
): number {
  const trimmed = (raw ?? "").trim();
  if (!trimmed || !/^\d+$/.test(trimmed)) {
    return fallback;
  }

  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  if (parsed < min) {
    return min;
  }
  if (parsed > max) {
    return max;
  }
  return parsed;
}

function getPullerSyncRetryMaxAttempts(): number {
  return parseBoundedInteger(
    Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS,
    DEFAULT_PULLER_SYNC_RETRY_MAX_ATTEMPTS,
    MIN_PULLER_SYNC_RETRY_MAX_ATTEMPTS,
    MAX_PULLER_SYNC_RETRY_MAX_ATTEMPTS
  );
}

function getPullerSyncRetryBaseBackoffMs(): number {
  return parseBoundedInteger(
    Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS,
    DEFAULT_PULLER_SYNC_RETRY_BASE_BACKOFF_MS,
    MIN_PULLER_SYNC_RETRY_BACKOFF_MS,
    MAX_PULLER_SYNC_RETRY_BACKOFF_MS
  );
}

function getPullerSyncRetryMaxBackoffMs(baseBackoffMs: number): number {
  const parsed = parseBoundedInteger(
    Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS,
    DEFAULT_PULLER_SYNC_RETRY_MAX_BACKOFF_MS,
    MIN_PULLER_SYNC_RETRY_BACKOFF_MS,
    MAX_PULLER_SYNC_RETRY_BACKOFF_MS
  );
  return parsed < baseBackoffMs ? baseBackoffMs : parsed;
}

function computeRetryBackoffMs(baseBackoffMs: number, maxBackoffMs: number, attempt: number): number {
  if (attempt <= 1) {
    return Math.min(baseBackoffMs, maxBackoffMs);
  }

  const exponential = baseBackoffMs * Math.pow(2, attempt - 1);
  if (!Number.isFinite(exponential)) {
    return maxBackoffMs;
  }
  return Math.min(Math.trunc(exponential), maxBackoffMs);
}

function shouldRetrySyncNowReason(reason: string): boolean {
  if (reason === "puller_timeout" || reason === "puller_unreachable") {
    return true;
  }
  if (reason.startsWith("puller_http_")) {
    const status = Number(reason.slice("puller_http_".length));
    return Number.isFinite(status) && (status >= 500 || status === 429);
  }
  return false;
}

function sleepMs(ms: number): Promise<void> {
  if (ms <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function shouldTryRealtimeSync(source: Source): boolean {
  return (
    source.type === "ssh" &&
    (source.accessMode === "realtime" || source.accessMode === "hybrid")
  );
}

async function findSourceById(
  tenantId: string,
  sourceId: string
): Promise<Source | undefined> {
  const sources = await repository.listSources(tenantId);
  return sources.find((item) => item.id === sourceId);
}

async function triggerSyncNow(
  sourceId: string,
  tenantId: string
): Promise<{ ok: true; attempts: number } | { ok: false; reason: string; attempts: number }> {
  const syncUrl = `${getPullerBaseUrl()}/v1/sources/${encodeURIComponent(sourceId)}/sync-now`;
  const maxAttempts = getPullerSyncRetryMaxAttempts();
  const baseBackoffMs = getPullerSyncRetryBaseBackoffMs();
  const maxBackoffMs = getPullerSyncRetryMaxBackoffMs(baseBackoffMs);
  let reason = "puller_unreachable";

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const timeoutMs = getPullerSyncTimeoutMs();
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(syncUrl, {
        method: "POST",
        headers: {
          accept: "application/json",
          "x-tenant-id": tenantId,
        },
        signal: controller.signal,
      });
      if (response.ok) {
        return { ok: true, attempts: attempt };
      }
      reason = `puller_http_${response.status}`;
    } catch (error) {
      reason = error instanceof Error && error.name === "AbortError" ? "puller_timeout" : "puller_unreachable";
    } finally {
      clearTimeout(timeoutId);
    }

    if (attempt >= maxAttempts || !shouldRetrySyncNowReason(reason)) {
      return { ok: false, reason, attempts: attempt };
    }

    const backoffMs = computeRetryBackoffMs(baseBackoffMs, maxBackoffMs, attempt);
    await sleepMs(backoffMs);
  }

  return { ok: false, reason, attempts: maxAttempts };
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

  const sourceId = result.data.sourceId?.trim();
  const source =
    sourceId && sourceId.length > 0
      ? await findSourceById(auth.tenantId, sourceId)
      : undefined;

  const sourceFreshness: SourceFreshnessMetadata = {
    fetchPath: "cache",
    freshnessMinutes: null,
    fallbackReason: null,
    accessMode: source?.accessMode ?? null,
  };

  if (source && shouldTryRealtimeSync(source)) {
    const syncResult = await triggerSyncNow(source.id, auth.tenantId);
    if (syncResult.ok) {
      sourceFreshness.fetchPath = "realtime";
    } else {
      sourceFreshness.fetchPath = "fallback-cache";
      sourceFreshness.fallbackReason = syncResult.reason;
      console.warn("[control-plane] sessions/search realtime 拉取失败，回退缓存。", {
        sourceId: source.id,
        tenantId: auth.tenantId,
        reason: syncResult.reason,
      });
    }
  }

  const payload = await repository.searchSessions(result.data, auth.tenantId);

  if (source) {
    const health = await repository.getSourceHealth(auth.tenantId, source.id);
    sourceFreshness.freshnessMinutes = health?.freshnessMinutes ?? null;
    sourceFreshness.accessMode = health?.accessMode ?? source.accessMode;
  }

  return c.json({
    items: payload.items,
    total: payload.total,
    nextCursor: null,
    filters: result.data,
    sourceFreshness,
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
