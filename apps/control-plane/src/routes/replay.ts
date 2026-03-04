import { Hono, type Context } from "hono";
import {
  validateReplayBaselineCreateInput,
  validateReplayJobCreateInput,
} from "../contracts";
import type {
  AppendAuditLogInput,
  ReplayBaseline,
  ReplayJob,
  ReplayJobStatus,
} from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const replayRoutes = new Hono<AppEnv>();

const repository = getControlPlaneRepository();
const WRITABLE_ROLES = new Set(["owner", "maintainer"]);
const QUALITY_METRIC_SET = new Set([
  "accuracy",
  "consistency",
  "groundedness",
  "safety",
  "latency",
]);
const REPLAY_STATUS_SET = new Set(["pending", "running", "completed", "failed", "cancelled"]);

type QualityMetric = "accuracy" | "consistency" | "groundedness" | "safety" | "latency";
type ExternalReplayStatus = "pending" | "running" | "completed" | "failed" | "cancelled";

function firstNonEmptyString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function toNumber(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) {
    return fallback;
  }
  return parsed;
}

function normalizeRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

function toQualityMetric(value: unknown): QualityMetric {
  const normalized = firstNonEmptyString(value)?.toLowerCase();
  if (normalized && QUALITY_METRIC_SET.has(normalized)) {
    return normalized as QualityMetric;
  }
  return "accuracy";
}

function toExternalReplayStatus(status: ReplayJobStatus): ExternalReplayStatus {
  if (status === "succeeded") {
    return "completed";
  }
  return status;
}

function toRepositoryReplayStatus(status: string | undefined): ReplayJobStatus | undefined {
  switch (status) {
    case "completed":
      return "succeeded";
    case "pending":
    case "running":
    case "failed":
    case "cancelled":
      return status;
    default:
      return undefined;
  }
}

function buildDiffItems(sampleLimit: number, metric: QualityMetric) {
  const count = Math.max(1, Math.min(sampleLimit, 20));
  const items: Array<{
    caseId: string;
    metric: QualityMetric;
    baselineScore: number;
    candidateScore: number;
    delta: number;
    verdict: "improved" | "regressed" | "unchanged";
    detail: string;
  }> = [];
  for (let index = 0; index < count; index += 1) {
    const seed = index % 3;
    const baselineScore = Number((0.65 + ((index % 7) * 0.03)).toFixed(4));
    const delta = seed === 0 ? 0.08 : seed === 1 ? -0.06 : 0;
    const candidateScore = Number(Math.max(0, Math.min(1, baselineScore + delta)).toFixed(4));
    const verdict = delta > 0 ? "improved" : delta < 0 ? "regressed" : "unchanged";
    items.push({
      caseId: `case-${index + 1}`,
      metric,
      baselineScore: Number((baselineScore * 100).toFixed(2)),
      candidateScore: Number((candidateScore * 100).toFixed(2)),
      delta: Number(((candidateScore - baselineScore) * 100).toFixed(2)),
      verdict,
      detail:
        verdict === "improved"
          ? "候选版本得分更高。"
          : verdict === "regressed"
            ? "候选版本得分下降。"
            : "候选版本与基线一致。",
    });
  }
  return items;
}

function summarizeDiffItems(items: Array<{ verdict: "improved" | "regressed" | "unchanged" }>) {
  let improvedCases = 0;
  let regressedCases = 0;
  let unchangedCases = 0;
  for (const item of items) {
    if (item.verdict === "improved") {
      improvedCases += 1;
      continue;
    }
    if (item.verdict === "regressed") {
      regressedCases += 1;
      continue;
    }
    unchangedCases += 1;
  }
  const totalCases = items.length;
  return {
    totalCases,
    processedCases: totalCases,
    improvedCases,
    regressedCases,
    unchangedCases,
  };
}

function parseDiffItems(diffPayload: Record<string, unknown>) {
  const rawItems = diffPayload.items;
  if (!Array.isArray(rawItems)) {
    return [] as Array<{
      caseId: string;
      metric: QualityMetric;
      baselineScore: number;
      candidateScore: number;
      delta: number;
      verdict: "improved" | "regressed" | "unchanged";
      detail?: string;
    }>;
  }

  const items: Array<{
    caseId: string;
    metric: QualityMetric;
    baselineScore: number;
    candidateScore: number;
    delta: number;
    verdict: "improved" | "regressed" | "unchanged";
    detail?: string;
  }> = [];
  for (const rawItem of rawItems) {
    if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
      continue;
    }
    const row = rawItem as Record<string, unknown>;
    const caseId = firstNonEmptyString(row.caseId);
    if (!caseId) {
      continue;
    }
    const metric = toQualityMetric(row.metric);
    const baselineScore = toNumber(row.baselineScore, 0);
    const candidateScore = toNumber(row.candidateScore, 0);
    const delta = toNumber(row.delta, candidateScore - baselineScore);
    const verdictRaw = firstNonEmptyString(row.verdict)?.toLowerCase();
    const verdict =
      verdictRaw === "improved" || verdictRaw === "regressed" || verdictRaw === "unchanged"
        ? verdictRaw
        : "unchanged";
    items.push({
      caseId,
      metric,
      baselineScore,
      candidateScore,
      delta,
      verdict,
      detail: firstNonEmptyString(row.detail),
    });
  }
  return items;
}

function mapReplayBaseline(baseline: ReplayBaseline) {
  const metadata = normalizeRecord(baseline.metadata);
  return {
    id: baseline.id,
    tenantId: baseline.tenantId,
    name: baseline.name,
    datasetId: firstNonEmptyString(baseline.datasetRef) ?? firstNonEmptyString(metadata.datasetId) ?? "",
    model: firstNonEmptyString(metadata.model) ?? "unknown",
    promptVersion: firstNonEmptyString(metadata.promptVersion),
    sampleCount: baseline.scenarioCount,
    metadata,
    createdAt: baseline.createdAt,
    updatedAt: baseline.updatedAt,
  };
}

function mapReplayJob(job: ReplayJob) {
  const parameters = normalizeRecord(job.parameters);
  const summary = normalizeRecord(job.summary);
  const diff = normalizeRecord(job.diff);
  const diffs = parseDiffItems(diff);
  const fallbackSummary = summarizeDiffItems(diffs);
  const totalCases = Math.max(
    0,
    toInteger(summary.totalCases, fallbackSummary.totalCases)
  );
  const processedCases = Math.max(
    0,
    toInteger(summary.processedCases, fallbackSummary.processedCases)
  );
  const improvedCases = Math.max(
    0,
    toInteger(summary.improvedCases, fallbackSummary.improvedCases)
  );
  const regressedCases = Math.max(
    0,
    toInteger(summary.regressedCases, fallbackSummary.regressedCases)
  );
  const unchangedCases = Math.max(
    0,
    toInteger(summary.unchangedCases, fallbackSummary.unchangedCases)
  );

  return {
    id: job.id,
    tenantId: job.tenantId,
    baselineId: job.baselineId,
    candidateLabel: firstNonEmptyString(parameters.candidateLabel) ?? "candidate",
    status: toExternalReplayStatus(job.status),
    totalCases,
    processedCases,
    improvedCases,
    regressedCases,
    unchangedCases,
    diffs,
    summary,
    error: job.error,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
  };
}

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 replay 审计日志失败。", error);
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

replayRoutes.post("/replay/baselines", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateReplayBaselineCreateInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  try {
    const baseline = await repository.createReplayBaseline(auth.tenantId, {
      name: validation.data.name,
      datasetRef: validation.data.datasetId,
      scenarioCount: validation.data.sampleCount ?? 0,
      metadata: {
        model: validation.data.model,
        promptVersion: validation.data.promptVersion,
        ...(validation.data.metadata ?? {}),
      },
    });

    const requestId = c.get("requestId");
    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:${requestId}`,
      action: "control_plane.replay.baseline_created",
      level: "info",
      detail: `Created replay baseline ${baseline.id}.`,
      metadata: {
        requestId,
        tenantId: auth.tenantId,
        baselineId: baseline.id,
        model: validation.data.model,
      },
    });

    return c.json(mapReplayBaseline(baseline), 201);
  } catch (error) {
    if (
      error instanceof Error &&
      error.message.startsWith("replay_baseline_name_already_exists:")
    ) {
      return c.json({ message: "回放基线名称已存在，请更换后重试。" }, 409);
    }
    throw error;
  }
});

replayRoutes.get("/replay/baselines", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const limitRaw = c.req.query("limit");
  const limit = toInteger(limitRaw, 100);
  if (limit <= 0 || limit > 500) {
    return c.json({ message: "limit 必须是 1 到 500 的整数。" }, 400);
  }
  const keyword = firstNonEmptyString(c.req.query("keyword"));
  const items = await repository.listReplayBaselines(auth.tenantId, {
    keyword,
    limit,
  });
  return c.json({
    items: items.map(mapReplayBaseline),
    total: items.length,
  });
});

replayRoutes.post("/replay/jobs", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateReplayJobCreateInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const sampleLimit = validation.data.sampleLimit ?? 100;
  const metric = toQualityMetric(validation.data.metadata?.metric);
  const diffItems = buildDiffItems(sampleLimit, metric);
  const summary = summarizeDiffItems(diffItems);
  const now = new Date().toISOString();

  try {
    const replayJob = await repository.createReplayJob(auth.tenantId, {
      baselineId: validation.data.baselineId,
      status: "succeeded",
      parameters: {
        candidateLabel: validation.data.candidateLabel,
        from: validation.data.from,
        to: validation.data.to,
        sampleLimit,
        ...(validation.data.metadata ?? {}),
      },
      summary,
      diff: {
        items: diffItems,
      },
      startedAt: now,
      finishedAt: now,
      createdAt: now,
    });

    const requestId = c.get("requestId");
    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:${requestId}`,
      action: "control_plane.replay.job_created",
      level: "info",
      detail: `Created replay job ${replayJob.id}.`,
      metadata: {
        requestId,
        tenantId: auth.tenantId,
        replayJobId: replayJob.id,
        baselineId: replayJob.baselineId,
      },
    });

    return c.json(mapReplayJob(replayJob), 201);
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("replay_baseline_not_found:")) {
      return c.json({ message: `未找到回放基线：${validation.data.baselineId}` }, 404);
    }
    throw error;
  }
});

replayRoutes.get("/replay/jobs", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const statusQuery = firstNonEmptyString(c.req.query("status"))?.toLowerCase();
  if (statusQuery && !REPLAY_STATUS_SET.has(statusQuery)) {
    return c.json(
      { message: "status 必须是 pending/running/completed/failed/cancelled 之一。" },
      400
    );
  }

  const limitRaw = c.req.query("limit");
  const limit = toInteger(limitRaw, 100);
  if (limit <= 0 || limit > 500) {
    return c.json({ message: "limit 必须是 1 到 500 的整数。" }, 400);
  }

  const jobs = await repository.listReplayJobs(auth.tenantId, {
    baselineId: firstNonEmptyString(c.req.query("baselineId")),
    status: toRepositoryReplayStatus(statusQuery),
    limit,
  });

  const items = jobs.map(mapReplayJob);
  return c.json({
    items,
    total: items.length,
  });
});

replayRoutes.get("/replay/jobs/:id", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const replayJobId = c.req.param("id")?.trim();
  if (!replayJobId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const replayJob = await repository.getReplayJobById(auth.tenantId, replayJobId);
  if (!replayJob) {
    return c.json({ message: `未找到回放任务：${replayJobId}` }, 404);
  }
  return c.json(mapReplayJob(replayJob));
});

replayRoutes.get("/replay/jobs/:id/diff", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const replayJobId = c.req.param("id")?.trim();
  if (!replayJobId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const replayJob = await repository.getReplayJobById(auth.tenantId, replayJobId);
  if (!replayJob) {
    return c.json({ message: `未找到回放任务：${replayJobId}` }, 404);
  }
  const diffPayload = (await repository.getReplayJobDiff(auth.tenantId, replayJobId)) ?? {};
  const diffItems = parseDiffItems(diffPayload);
  const replayJobView = mapReplayJob(replayJob);
  return c.json({
    jobId: replayJob.id,
    baselineId: replayJob.baselineId,
    diffs: diffItems,
    summary: replayJobView.summary,
  });
});
