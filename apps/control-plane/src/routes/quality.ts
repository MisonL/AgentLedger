import { Hono, type Context } from "hono";
import {
  validateQualityEventCreateInput,
  validateQualityScorecardUpsertInput,
} from "../contracts";
import type {
  AppendAuditLogInput,
  QualityDailyMetric,
  QualityScorecard,
} from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const qualityRoutes = new Hono<AppEnv>();

const repository = getControlPlaneRepository();
const WRITABLE_ROLES = new Set(["owner", "maintainer"]);
const QUALITY_METRIC_SET = new Set([
  "accuracy",
  "consistency",
  "groundedness",
  "safety",
  "latency",
]);

type QualityMetric = "accuracy" | "consistency" | "groundedness" | "safety" | "latency";

function firstNonEmptyString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
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

function toRepositoryScore(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  if (value > 1) {
    return Math.max(0, Math.min(1, value / 100));
  }
  return Math.max(0, Math.min(1, value));
}

function fromRepositoryScore(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Number((value <= 1 ? value * 100 : value).toFixed(4));
}

function toQualityMetric(value: unknown): QualityMetric {
  const normalized = firstNonEmptyString(value)?.toLowerCase();
  if (normalized && QUALITY_METRIC_SET.has(normalized)) {
    return normalized as QualityMetric;
  }
  return "accuracy";
}

function toPositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function parseIsoDateOrUndefined(value: unknown): string | undefined {
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

function parseRangeBoundary(value: string | undefined, mode: "from" | "to"): string | undefined {
  if (!value) {
    return undefined;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return mode === "from"
      ? `${value}T00:00:00.000Z`
      : `${value}T23:59:59.999Z`;
  }
  return parseIsoDateOrUndefined(value);
}

function parseDateRange(
  fromRaw: string | undefined,
  toRaw: string | undefined
): { from?: string; to?: string; error?: string } {
  const from = parseRangeBoundary(fromRaw, "from");
  if (fromRaw !== undefined && !from) {
    return { error: "from 必须为 ISO 日期字符串。" };
  }
  const to = parseRangeBoundary(toRaw, "to");
  if (toRaw !== undefined && !to) {
    return { error: "to 必须为 ISO 日期字符串。" };
  }
  if (from && to && Date.parse(from) > Date.parse(to)) {
    return { error: "from 不能晚于 to。" };
  }
  return { from, to };
}

function mapDailyMetric(metric: QualityDailyMetric, selectedMetric: QualityMetric) {
  const avg = fromRepositoryScore(metric.averageScore);
  return {
    date: metric.date,
    metric: selectedMetric,
    avgScore: avg,
    p50Score: avg,
    p90Score: avg,
    totalEvents: metric.total,
  };
}

function mapQualityScorecard(scorecard: QualityScorecard) {
  const warningScore = Number(scorecard.dimensions.warningScore ?? scorecard.score);
  const criticalScore = Number(scorecard.dimensions.criticalScore ?? warningScore);
  const weight = Number(scorecard.dimensions.weight ?? 1);
  const metadata = normalizeRecord(scorecard.metadata);

  return {
    id: scorecard.scorecardKey,
    tenantId: scorecard.tenantId,
    metric: toQualityMetric(scorecard.scorecardKey),
    targetScore: fromRepositoryScore(scorecard.score),
    warningScore: fromRepositoryScore(Number.isFinite(warningScore) ? warningScore : scorecard.score),
    criticalScore: fromRepositoryScore(
      Number.isFinite(criticalScore) ? criticalScore : warningScore
    ),
    weight: Number.isFinite(weight) ? Math.max(0, weight) : 1,
    enabled: metadata.enabled !== false,
    updatedByUserId: firstNonEmptyString(metadata.updatedByUserId),
    updatedAt: scorecard.updatedAt,
  };
}

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 quality 审计日志失败。", error);
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

qualityRoutes.post("/quality/events", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateQualityEventCreateInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const metadata = {
    ...(validation.data.metadata ?? {}),
    metric: validation.data.metric,
    sampleCount: validation.data.sampleCount,
    sessionId: validation.data.sessionId,
    replayJobId: validation.data.replayJobId,
    notes: validation.data.notes,
    occurredAt: validation.data.occurredAt,
  };
  const created = await repository.createQualityEvent(auth.tenantId, {
    scorecardKey: validation.data.metric,
    metricKey: validation.data.metric,
    score: toRepositoryScore(validation.data.score),
    passed: toRepositoryScore(validation.data.score) >= 0.8,
    metadata,
    createdAt: validation.data.occurredAt,
  });

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.quality.event_created",
    level: "info",
    detail: `Created quality event ${created.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      eventId: created.id,
      metric: validation.data.metric,
      score: validation.data.score,
    },
  });

  return c.json(
    {
      id: created.id,
      tenantId: auth.tenantId,
      sessionId: validation.data.sessionId,
      replayJobId: validation.data.replayJobId,
      metric: validation.data.metric,
      score: validation.data.score,
      sampleCount: validation.data.sampleCount,
      occurredAt: validation.data.occurredAt,
      notes: validation.data.notes,
      metadata: validation.data.metadata ?? {},
      createdAt: created.createdAt,
    },
    201
  );
});

qualityRoutes.get("/quality/metrics/daily", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const fromRaw = c.req.query("from");
  const toRaw = c.req.query("to");
  const range = parseDateRange(fromRaw, toRaw);
  if (range.error) {
    return c.json({ message: range.error }, 400);
  }

  const metricQuery = firstNonEmptyString(c.req.query("metric"));
  if (metricQuery && !QUALITY_METRIC_SET.has(metricQuery.toLowerCase())) {
    return c.json(
      { message: "metric 必须是 accuracy/consistency/groundedness/safety/latency 之一。" },
      400
    );
  }
  const metric = toQualityMetric(metricQuery);

  const limitRaw = c.req.query("limit");
  const limit = toPositiveInteger(limitRaw, 60);
  if (limit > 365) {
    return c.json({ message: "limit 必须是 1 到 365 的整数。" }, 400);
  }

  const items = await repository.listQualityDailyMetrics(auth.tenantId, {
    from: range.from,
    to: range.to,
    scorecardKey: metricQuery ? metric : undefined,
    limit,
  });

  return c.json({
    items: items.map((item) => mapDailyMetric(item, metric)),
    total: items.length,
  });
});

qualityRoutes.get("/quality/scorecards", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const metricQuery = firstNonEmptyString(c.req.query("metric"));
  if (metricQuery && !QUALITY_METRIC_SET.has(metricQuery.toLowerCase())) {
    return c.json(
      { message: "metric 必须是 accuracy/consistency/groundedness/safety/latency 之一。" },
      400
    );
  }
  const limitRaw = c.req.query("limit");
  const limit = toPositiveInteger(limitRaw, 100);
  if (limit > 500) {
    return c.json({ message: "limit 必须是 1 到 500 的整数。" }, 400);
  }

  const scorecards = await repository.listQualityScorecards(auth.tenantId, {
    scorecardKey: metricQuery?.toLowerCase(),
    limit,
  });
  const items = scorecards.map(mapQualityScorecard);
  return c.json({
    items,
    total: items.length,
  });
});

qualityRoutes.put("/quality/scorecards/:id", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const metricId = c.req.param("id")?.trim();
  if (!metricId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const normalizedMetric = metricId.toLowerCase();
  if (!QUALITY_METRIC_SET.has(normalizedMetric)) {
    return c.json(
      { message: "id 必须是 accuracy/consistency/groundedness/safety/latency 之一。" },
      400
    );
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateQualityScorecardUpsertInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
    metric: normalizedMetric,
    updatedAt: toIsoString(bodyRecord.updatedAt) ?? new Date().toISOString(),
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const saved = await repository.upsertQualityScorecard(auth.tenantId, {
    scorecardKey: validation.data.metric,
    title: `${validation.data.metric} 质量评分卡`,
    description: `由控制台维护，最近更新时间 ${validation.data.updatedAt}`,
    score: toRepositoryScore(validation.data.targetScore),
    dimensions: {
      warningScore: toRepositoryScore(validation.data.warningScore),
      criticalScore: toRepositoryScore(validation.data.criticalScore),
      weight: validation.data.weight ?? 1,
    },
    metadata: {
      enabled: validation.data.enabled,
      updatedByUserId: auth.userId,
      metric: validation.data.metric,
    },
    updatedAt: validation.data.updatedAt,
  });

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.quality.scorecard_upserted",
    level: "info",
    detail: `Upserted quality scorecard ${saved.scorecardKey}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      scorecardKey: saved.scorecardKey,
      enabled: validation.data.enabled,
    },
  });

  return c.json(mapQualityScorecard(saved));
});
