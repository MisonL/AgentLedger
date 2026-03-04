import { Hono, type Context } from "hono";
import {
  isHeatmapCell,
  type HeatmapCell,
  type UsageAggregateFilters,
  type UsageDailyItem,
  type UsageWeekItem,
  type UsageHeatmapDrilldownResponse,
  type UsageHeatmapMetric,
  type UsageHeatmapResponse,
  type UsageWeeklySummaryResponse,
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
const ANALYTICS_HEATMAP_PROXY_PATH = "/v1/usage/heatmap";
const ANALYTICS_WEEKLY_SUMMARY_PROXY_PATH = "/v1/usage/weekly-summary";
const DEFAULT_ANALYTICS_BASE_URL = "http://127.0.0.1:8083";
const DEFAULT_ANALYTICS_PROXY_TIMEOUT_MS = 1_500;
const MIN_ANALYTICS_PROXY_TIMEOUT_MS = 100;
const MAX_ANALYTICS_PROXY_TIMEOUT_MS = 60_000;
const DEFAULT_USAGE_AGGREGATE_LIMIT = 50;
const MAX_USAGE_AGGREGATE_LIMIT = 2_000;
const DEFAULT_USAGE_HEATMAP_DRILLDOWN_LIMIT = 50;
const MAX_USAGE_HEATMAP_DRILLDOWN_LIMIT = 500;

class analyticsProxyClientError extends Error {
  readonly statusCode: number;

  constructor(statusCode: number) {
    super(`analytics 请求参数错误: ${statusCode}`);
    this.name = "analyticsProxyClientError";
    this.statusCode = statusCode;
  }
}

class analyticsProxyUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "analyticsProxyUnavailableError";
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function readStringField(
  value: Record<string, unknown>,
  keys: string[]
): string | undefined {
  for (const key of keys) {
    const candidate = value[key];
    if (typeof candidate === "string") {
      const trimmed = candidate.trim();
      if (trimmed.length > 0) {
        return trimmed;
      }
    }
  }
  return undefined;
}

function normalizeWeekItem(value: unknown): UsageWeekItem | null {
  if (!isRecord(value)) {
    return null;
  }

  const weekStart = readStringField(value, ["weekStart", "week_start"]);
  const weekEnd = readStringField(value, ["weekEnd", "week_end"]);
  const tokensValue = value.tokens;
  const costValue = value.cost;
  const sessionsValue = value.sessions;

  if (!weekStart || !weekEnd) {
    return null;
  }
  if (Number.isNaN(Date.parse(weekStart)) || Number.isNaN(Date.parse(weekEnd))) {
    return null;
  }
  if (
    typeof tokensValue !== "number" ||
    !Number.isInteger(tokensValue) ||
    tokensValue < 0
  ) {
    return null;
  }
  if (
    typeof costValue !== "number" ||
    !Number.isFinite(costValue) ||
    costValue < 0
  ) {
    return null;
  }
  if (
    typeof sessionsValue !== "number" ||
    !Number.isInteger(sessionsValue) ||
    sessionsValue < 0
  ) {
    return null;
  }

  return {
    weekStart,
    weekEnd,
    tokens: tokensValue,
    cost: costValue,
    sessions: sessionsValue,
  };
}

function parseAnalyticsWeeklySummary(
  payload: unknown
): UsageWeeklySummaryResponse | null {
  if (!isRecord(payload)) {
    return null;
  }

  const metric = parseUsageHeatmapMetric(
    readStringField(payload, ["metric"])
  );
  const timezone = readStringField(payload, ["timezone"]);
  if (!metric || !timezone) {
    return null;
  }

  const summary = payload.summary;
  if (!isRecord(summary)) {
    return null;
  }
  const summaryTokens = summary.tokens;
  const summaryCost = summary.cost;
  const summarySessions = summary.sessions;
  if (
    typeof summaryTokens !== "number" ||
    !Number.isInteger(summaryTokens) ||
    summaryTokens < 0 ||
    typeof summaryCost !== "number" ||
    !Number.isFinite(summaryCost) ||
    summaryCost < 0 ||
    typeof summarySessions !== "number" ||
    !Number.isInteger(summarySessions) ||
    summarySessions < 0
  ) {
    return null;
  }

  if (!Array.isArray(payload.weeks)) {
    return null;
  }

  const weeks: UsageWeekItem[] = [];
  for (const item of payload.weeks) {
    const normalized = normalizeWeekItem(item);
    if (!normalized) {
      return null;
    }
    weeks.push(normalized);
  }

  const peakWeekRaw =
    payload.peakWeek !== undefined ? payload.peakWeek : payload.peak_week;
  const peakWeek =
    peakWeekRaw === undefined || peakWeekRaw === null
      ? undefined
      : normalizeWeekItem(peakWeekRaw);
  if (peakWeekRaw !== undefined && peakWeekRaw !== null && !peakWeek) {
    return null;
  }

  return {
    metric,
    timezone,
    weeks,
    summary: {
      tokens: summaryTokens,
      cost: summaryCost,
      sessions: summarySessions,
    },
    ...(peakWeek ? { peakWeek } : {}),
  };
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

function parseUsageHeatmapMetric(value: string | undefined): UsageHeatmapMetric | undefined {
  const normalized = (value ?? "").trim().toLowerCase();
  switch (normalized) {
    case "tokens":
    case "cost":
    case "sessions":
      return normalized;
    default:
      return undefined;
  }
}

function parseUsageHeatmapDrilldownDateRange(
  rawDate: string
): { date: string; from: string; to: string } | null {
  const trimmed = rawDate.trim();
  if (!trimmed) {
    return null;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    const fromDate = new Date(`${trimmed}T00:00:00.000Z`);
    if (Number.isNaN(fromDate.getTime())) {
      return null;
    }
    const toDate = new Date(fromDate.getTime() + (24 * 60 * 60 * 1000) - 1);
    return {
      date: trimmed,
      from: fromDate.toISOString(),
      to: toDate.toISOString(),
    };
  }

  const timestamp = Date.parse(trimmed);
  if (Number.isNaN(timestamp)) {
    return null;
  }
  const date = new Date(timestamp);
  const fromDate = new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 0, 0, 0, 0)
  );
  const toDate = new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 23, 59, 59, 999)
  );
  return {
    date: fromDate.toISOString().slice(0, 10),
    from: fromDate.toISOString(),
    to: toDate.toISOString(),
  };
}

function parseUsageHeatmapDrilldownQuery(
  query: Record<string, string | undefined>
):
  | {
      success: true;
      data: {
        date: string;
        metric: UsageHeatmapMetric;
        from: string;
        to: string;
        limit: number;
      };
    }
  | { success: false; error: string } {
  const rawDate = toOptionalQueryString(query.date ?? null);
  if (!rawDate) {
    return { success: false, error: "date 必填，格式需为 YYYY-MM-DD 或 ISO 日期时间。" };
  }
  const dateRange = parseUsageHeatmapDrilldownDateRange(rawDate);
  if (!dateRange) {
    return { success: false, error: "date 必须为 YYYY-MM-DD 或合法 ISO 日期时间。" };
  }

  const metric = parseUsageHeatmapMetric(query.metric) ?? "tokens";
  if (query.metric !== undefined && !parseUsageHeatmapMetric(query.metric)) {
    return { success: false, error: "metric 仅支持 tokens/cost/sessions。" };
  }

  let limit = DEFAULT_USAGE_HEATMAP_DRILLDOWN_LIMIT;
  if (query.limit !== undefined) {
    const trimmed = query.limit.trim();
    if (!/^\d+$/.test(trimmed)) {
      return {
        success: false,
        error: `limit 必须是 1 到 ${MAX_USAGE_HEATMAP_DRILLDOWN_LIMIT} 的整数。`,
      };
    }
    const parsed = Number(trimmed);
    if (
      !Number.isInteger(parsed) ||
      parsed < 1 ||
      parsed > MAX_USAGE_HEATMAP_DRILLDOWN_LIMIT
    ) {
      return {
        success: false,
        error: `limit 必须是 1 到 ${MAX_USAGE_HEATMAP_DRILLDOWN_LIMIT} 的整数。`,
      };
    }
    limit = parsed;
  }

  return {
    success: true,
    data: {
      date: dateRange.date,
      metric,
      from: dateRange.from,
      to: dateRange.to,
      limit,
    },
  };
}

function sortUsageSessionItemsByMetric(
  items: UsageSessionBreakdownItem[],
  metric: UsageHeatmapMetric
): UsageSessionBreakdownItem[] {
  const sorted = [...items];
  sorted.sort((left, right) => {
    if (metric === "cost") {
      return (
        right.cost - left.cost ||
        right.totalTokens - left.totalTokens ||
        right.startedAt.localeCompare(left.startedAt)
      );
    }
    if (metric === "sessions") {
      return (
        right.startedAt.localeCompare(left.startedAt) ||
        right.totalTokens - left.totalTokens ||
        right.cost - left.cost
      );
    }
    return (
      right.totalTokens - left.totalTokens ||
      right.cost - left.cost ||
      right.startedAt.localeCompare(left.startedAt)
    );
  });
  return sorted;
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
  const analyticsUrl = `${analyticsBaseUrl}${ANALYTICS_HEATMAP_PROXY_PATH}${analyticsQueryString}`;
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

async function fetchUsageWeeklySummary(
  analyticsQueryString: string
): Promise<UsageWeeklySummaryResponse> {
  if (!isAnalyticsProxyEnabled()) {
    throw new analyticsProxyUnavailableError(
      "ANALYTICS_PROXY_ENABLED=false 时无法查询 weekly summary。"
    );
  }

  const analyticsBaseUrl = getAnalyticsBaseUrl();
  const analyticsUrl = `${analyticsBaseUrl}${ANALYTICS_WEEKLY_SUMMARY_PROXY_PATH}${analyticsQueryString}`;
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
    const normalized = parseAnalyticsWeeklySummary(payload);
    if (!normalized) {
      throw new Error("analytics weekly-summary 返回结构不合法");
    }
    return normalized;
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

usageRoutes.get("/usage/weekly-summary", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const requestUrl = new URL(c.req.url);
  const query = {
    ...parseUsageHeatmapQuery(requestUrl),
    tenantId: auth.tenantId,
  };
  const analyticsQueryString = buildAnalyticsQueryString(
    requestUrl.searchParams,
    query
  );

  try {
    const payload = await fetchUsageWeeklySummary(analyticsQueryString);
    return c.json(payload);
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
    if (error instanceof analyticsProxyUnavailableError) {
      return c.json(
        {
          message: error.message,
        },
        503
      );
    }
    console.warn("[control-plane] weekly summary 代理失败。", {
      error,
    });
    return c.json(
      {
        message: "query usage weekly summary failed",
      },
      502
    );
  }
});

usageRoutes.get("/usage/heatmap/drilldown", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const queryResult = parseUsageHeatmapDrilldownQuery(c.req.query());
  if (!queryResult.success) {
    return c.json({ message: queryResult.error }, 400);
  }

  const fullItems = await repository.listUsageSessionBreakdown({
    tenantId: auth.tenantId,
    from: queryResult.data.from,
    to: queryResult.data.to,
    limit: MAX_USAGE_AGGREGATE_LIMIT,
  });
  const items = sortUsageSessionItemsByMetric(fullItems, queryResult.data.metric).slice(
    0,
    queryResult.data.limit
  );

  const summary = fullItems.reduce(
    (acc, item) => {
      acc.tokens += item.totalTokens;
      acc.cost += item.cost;
      return acc;
    },
    { tokens: 0, cost: 0 }
  );

  const payload: UsageHeatmapDrilldownResponse = {
    items,
    total: fullItems.length,
    filters: {
      date: queryResult.data.date,
      metric: queryResult.data.metric,
      from: queryResult.data.from,
      to: queryResult.data.to,
      limit: queryResult.data.limit,
    },
    summary: {
      tokens: summary.tokens,
      cost: Number(summary.cost.toFixed(2)),
      sessions: fullItems.length,
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
