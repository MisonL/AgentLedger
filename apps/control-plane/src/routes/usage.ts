import { Hono, type Context } from "hono";
import {
  isHeatmapCell,
  type HeatmapCell,
  type UsageAggregateFilters,
  type UsageDailyItem,
  type UsageHeatmapResponse,
  type UsageListResponse,
  type UsageModelItem,
  type UsageMonthlyItem,
  type UsageSessionBreakdownItem,
} from "../contracts";
import {
  getControlPlaneRepository,
  type UsageHeatmapQueryInput,
} from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const usageRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const ANALYTICS_PROXY_PATH = "/v1/usage/heatmap";
const DEFAULT_ANALYTICS_BASE_URL = "http://127.0.0.1:8083";
const DEFAULT_ANALYTICS_PROXY_TIMEOUT_MS = 1_500;
const MIN_ANALYTICS_PROXY_TIMEOUT_MS = 100;
const MAX_ANALYTICS_PROXY_TIMEOUT_MS = 60_000;
const DEFAULT_USAGE_AGGREGATE_LIMIT = 50;
const MAX_USAGE_AGGREGATE_LIMIT = 2_000;

class analyticsProxyClientError extends Error {
  readonly statusCode: number;

  constructor(statusCode: number) {
    super(`analytics 请求参数错误: ${statusCode}`);
    this.name = "analyticsProxyClientError";
    this.statusCode = statusCode;
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

function isAnalyticsProxyEnabled(): boolean {
  const rawValue = Bun.env.ANALYTICS_PROXY_ENABLED;
  if (rawValue === undefined) {
    return true;
  }

  const normalized = rawValue.trim().toLowerCase();
  if (!normalized) {
    return true;
  }

  return !["0", "false", "off", "no"].includes(normalized);
}

function getAnalyticsBaseUrl(): string {
  const rawValue = (Bun.env.ANALYTICS_BASE_URL ?? "").trim();
  const baseUrl = rawValue.length > 0 ? rawValue : DEFAULT_ANALYTICS_BASE_URL;
  return baseUrl.replace(/\/+$/, "");
}

function getAnalyticsProxyTimeoutMs(): number {
  const rawValue = (Bun.env.ANALYTICS_PROXY_TIMEOUT_MS ?? "").trim();
  if (!rawValue) {
    return DEFAULT_ANALYTICS_PROXY_TIMEOUT_MS;
  }

  if (!/^\d+$/.test(rawValue)) {
    return DEFAULT_ANALYTICS_PROXY_TIMEOUT_MS;
  }
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_ANALYTICS_PROXY_TIMEOUT_MS;
  }
  if (parsed < MIN_ANALYTICS_PROXY_TIMEOUT_MS) {
    return MIN_ANALYTICS_PROXY_TIMEOUT_MS;
  }
  if (parsed > MAX_ANALYTICS_PROXY_TIMEOUT_MS) {
    return MAX_ANALYTICS_PROXY_TIMEOUT_MS;
  }

  return parsed;
}

function normalizeAnalyticsHeatmapCell(value: unknown): HeatmapCell | null {
  if (!isHeatmapCell(value)) {
    return null;
  }

  if (!Number.isInteger(value.tokens) || value.tokens < 0) {
    return null;
  }
  if (!Number.isFinite(value.cost) || value.cost < 0) {
    return null;
  }
  if (!Number.isInteger(value.sessions) || value.sessions < 0) {
    return null;
  }

  return value;
}

function parseAnalyticsHeatmap(payload: unknown): HeatmapCell[] | null {
  const candidateCells = Array.isArray(payload)
    ? payload
    : payload && typeof payload === "object" && "cells" in payload
      ? (payload as { cells?: unknown }).cells
      : null;

  if (!Array.isArray(candidateCells)) {
    return null;
  }
  const normalized: HeatmapCell[] = [];
  for (const item of candidateCells) {
    const cell = normalizeAnalyticsHeatmapCell(item);
    if (!cell) {
      return null;
    }
    normalized.push(cell);
  }
  return normalized;
}

function toOptionalQueryString(value: string | null): string | undefined {
  if (!value) {
    return undefined;
  }
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : undefined;
}

function parseUsageAggregateQuery(
  query: Record<string, string | undefined>
):
  | { success: true; data: UsageAggregateFilters }
  | { success: false; error: string } {
  const from = toOptionalQueryString(query.from ?? null);
  const to = toOptionalQueryString(query.to ?? null);
  const rawLimit = query.limit;

  if (query.from !== undefined && !from) {
    return { success: false, error: "from 必须为 ISO 日期字符串。" };
  }
  if (query.to !== undefined && !to) {
    return { success: false, error: "to 必须为 ISO 日期字符串。" };
  }
  if (from && Number.isNaN(Date.parse(from))) {
    return { success: false, error: "from 必须为 ISO 日期字符串。" };
  }
  if (to && Number.isNaN(Date.parse(to))) {
    return { success: false, error: "to 必须为 ISO 日期字符串。" };
  }
  if (from && to && Date.parse(from) > Date.parse(to)) {
    return { success: false, error: "from 不能晚于 to。" };
  }

  let limit = DEFAULT_USAGE_AGGREGATE_LIMIT;
  if (rawLimit !== undefined) {
    const trimmed = rawLimit.trim();
    if (trimmed.length === 0) {
      return { success: false, error: "limit 必须是 1 到 2000 的整数。" };
    }
    const parsed = Number(trimmed);
    if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_USAGE_AGGREGATE_LIMIT) {
      return { success: false, error: "limit 必须是 1 到 2000 的整数。" };
    }
    limit = parsed;
  }

  return {
    success: true,
    data: {
      from,
      to,
      limit,
    },
  };
}

function parseUsageHeatmapQuery(url: URL): UsageHeatmapQueryInput {
  return {
    tenantId: toOptionalQueryString(url.searchParams.get("tenant_id"))
      ?? toOptionalQueryString(url.searchParams.get("tenantId")),
    from: toOptionalQueryString(url.searchParams.get("from")),
    to: toOptionalQueryString(url.searchParams.get("to")),
    timezone: toOptionalQueryString(url.searchParams.get("tz"))
      ?? toOptionalQueryString(url.searchParams.get("timezone")),
    metric: toOptionalQueryString(url.searchParams.get("metric")),
  };
}

function buildAnalyticsQueryString(
  rawSearchParams: URLSearchParams,
  query: UsageHeatmapQueryInput
): string {
  const params = new URLSearchParams(rawSearchParams);

  params.delete("tenant_id");
  params.delete("tenantId");
  if (query.tenantId) {
    params.set("tenant_id", query.tenantId);
  }

  params.delete("tz");
  params.delete("timezone");
  if (query.timezone) {
    params.set("tz", query.timezone);
  }

  params.delete("from");
  if (query.from) {
    params.set("from", query.from);
  }

  params.delete("to");
  if (query.to) {
    params.set("to", query.to);
  }

  params.delete("metric");
  if (query.metric) {
    params.set("metric", query.metric);
  }

  const encoded = params.toString();
  return encoded ? `?${encoded}` : "";
}

async function listUsageHeatmapWithFallback(
  query: UsageHeatmapQueryInput,
  analyticsQueryString: string
): Promise<HeatmapCell[]> {
  if (!isAnalyticsProxyEnabled()) {
    return repository.listUsageHeatmap(query);
  }

  const analyticsBaseUrl = getAnalyticsBaseUrl();
  const analyticsUrl = `${analyticsBaseUrl}${ANALYTICS_PROXY_PATH}${analyticsQueryString}`;
  const timeoutMs = getAnalyticsProxyTimeoutMs();
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(analyticsUrl, {
      method: "GET",
      headers: {
        accept: "application/json",
      },
      signal: controller.signal,
    });
    if (!response.ok) {
      if (response.status >= 400 && response.status < 500) {
        throw new analyticsProxyClientError(response.status);
      }
      throw new Error(`analytics 返回状态异常: ${response.status}`);
    }

    const payload = await response.json();
    const cells = parseAnalyticsHeatmap(payload);
    if (!cells) {
      throw new Error("analytics 返回结构不合法");
    }

    return cells;
  } catch (error) {
    if (error instanceof analyticsProxyClientError) {
      throw error;
    }
    console.warn("[control-plane] analytics 代理失败，回退 repository.listUsageHeatmap()", {
      analyticsUrl,
      timeoutMs,
      error,
    });
    return repository.listUsageHeatmap(query);
  } finally {
    clearTimeout(timeoutId);
  }
}

usageRoutes.get("/usage/heatmap", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const requestUrl = new URL(c.req.url);
  const query = {
    ...parseUsageHeatmapQuery(requestUrl),
    tenantId: auth.tenantId,
  };
  const analyticsQueryString = buildAnalyticsQueryString(requestUrl.searchParams, query);
  let responseCells: HeatmapCell[];
  try {
    responseCells = await listUsageHeatmapWithFallback(query, analyticsQueryString);
  } catch (error) {
    if (error instanceof analyticsProxyClientError) {
      return c.json(
        {
          error: "analytics 请求参数不合法",
          status: error.statusCode,
        },
        400
      );
    }
    throw error;
  }

  const summary = responseCells.reduce(
    (acc, cell) => {
      acc.tokens += cell.tokens;
      acc.cost += cell.cost;
      acc.sessions += cell.sessions;
      return acc;
    },
    { tokens: 0, cost: 0, sessions: 0 }
  );

  const payload: UsageHeatmapResponse = {
    cells: responseCells,
    summary: {
      tokens: summary.tokens,
      cost: Number(summary.cost.toFixed(2)),
      sessions: summary.sessions,
    },
  };

  return c.json(payload);
});

usageRoutes.get("/usage/monthly", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const queryResult = parseUsageAggregateQuery(c.req.query());
  if (!queryResult.success) {
    return c.json({ message: queryResult.error }, 400);
  }

  const filters = queryResult.data;
  const items = await repository.listUsageMonthly({
    tenantId: auth.tenantId,
    from: filters.from,
    to: filters.to,
    limit: filters.limit,
  });
  const normalizedItems =
    items.length > filters.limit ? items.slice(-filters.limit) : items;

  const payload: UsageListResponse<UsageMonthlyItem> = {
    items: normalizedItems,
    total: normalizedItems.length,
    filters,
  };

  return c.json(payload);
});

usageRoutes.get("/usage/daily", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const queryResult = parseUsageAggregateQuery(c.req.query());
  if (!queryResult.success) {
    return c.json({ message: queryResult.error }, 400);
  }

  const filters = queryResult.data;
  const items = await repository.listUsageDaily({
    tenantId: auth.tenantId,
    from: filters.from,
    to: filters.to,
    limit: filters.limit,
  });
  const normalizedItems =
    items.length > filters.limit ? items.slice(-filters.limit) : items;

  const payload: UsageListResponse<UsageDailyItem> = {
    items: normalizedItems,
    total: normalizedItems.length,
    filters,
  };

  return c.json(payload);
});

usageRoutes.get("/usage/models", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const queryResult = parseUsageAggregateQuery(c.req.query());
  if (!queryResult.success) {
    return c.json({ message: queryResult.error }, 400);
  }

  const filters = queryResult.data;
  const items = await repository.listUsageModelRanking({
    tenantId: auth.tenantId,
    from: filters.from,
    to: filters.to,
    limit: filters.limit,
  });

  const payload: UsageListResponse<UsageModelItem> = {
    items,
    total: items.length,
    filters,
  };

  return c.json(payload);
});

usageRoutes.get("/usage/sessions", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const queryResult = parseUsageAggregateQuery(c.req.query());
  if (!queryResult.success) {
    return c.json({ message: queryResult.error }, 400);
  }

  const filters = queryResult.data;
  const items = await repository.listUsageSessionBreakdown({
    tenantId: auth.tenantId,
    from: filters.from,
    to: filters.to,
    limit: filters.limit,
  });

  const payload: UsageListResponse<UsageSessionBreakdownItem> = {
    items,
    total: items.length,
    filters,
  };

  return c.json(payload);
});
