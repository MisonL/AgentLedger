import { Hono, type Context } from "hono";
import {
  validateQualityEventCreateInput,
  validateQualityScorecardUpsertInput,
  validateReplayDatasetCasesReplaceInput,
  validateReplayDatasetCreateInput,
  validateReplayDatasetMaterializeInput,
  validateReplayRunCreateInput,
  validateReplicationJobApproveInput,
  validateReplicationJobCancelInput,
  validateReplicationJobCreateInput,
  validateReplicationJobListInput,
  validateTenantResidencyPolicyUpsertInput,
} from "../contracts";
import type {
  AppendAuditLogInput,
  QualityDailyMetric,
  QualityExternalMetricGroup,
  QualityScorecard,
  ReplayArtifact,
  ReplayDataset,
  ReplayRun,
} from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import { enqueueReplayJobExecution } from "./replay";
import { readReplayArtifactContent } from "./replay-artifact-store";
import type { AppEnv } from "../types";

export const apiV2Routes = new Hono<AppEnv>();

const repository = getControlPlaneRepository();
const WRITABLE_ROLES = new Set(["owner", "maintainer"]);
const QUALITY_METRIC_SET = new Set([
  "accuracy",
  "consistency",
  "groundedness",
  "safety",
  "latency",
]);
const QUALITY_EXTERNAL_GROUP_BY_SET = new Set(["provider", "repo", "workflow", "runid", "run_id"]);
const REPLAY_STATUS_SET = new Set(["pending", "running", "completed", "failed", "cancelled"]);
const REPLAY_CASE_SOURCE_TYPE_SET = new Set(["manual", "session", "import"]);

type QualityMetric = "accuracy" | "consistency" | "groundedness" | "safety" | "latency";
type QualityExternalGroupBy = "provider" | "repo" | "workflow" | "runId";

function firstNonEmptyString(...values: unknown[]): string | undefined {
  for (const value of values) {
    if (typeof value !== "string") {
      continue;
    }
    const trimmed = value.trim();
    if (trimmed.length > 0) {
      return trimmed;
    }
  }
  return undefined;
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

function toPositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function toNumber(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toInteger(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) {
    return fallback;
  }
  return parsed;
}

function toBoolean(value: unknown, fallback = false): boolean {
  const normalized = firstNonEmptyString(value)?.toLowerCase();
  if (!normalized) {
    return fallback;
  }
  if (normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "y" || normalized === "on") {
    return true;
  }
  if (normalized === "0" || normalized === "false" || normalized === "no" || normalized === "n" || normalized === "off") {
    return false;
  }
  return fallback;
}

function toQualityMetric(value: unknown): QualityMetric {
  const normalized = firstNonEmptyString(value)?.toLowerCase();
  if (normalized && QUALITY_METRIC_SET.has(normalized)) {
    return normalized as QualityMetric;
  }
  return "accuracy";
}

function toQualityExternalGroupBy(value: unknown): QualityExternalGroupBy | undefined {
  const normalized = firstNonEmptyString(value)?.toLowerCase();
  if (!normalized || !QUALITY_EXTERNAL_GROUP_BY_SET.has(normalized)) {
    return undefined;
  }
  if (normalized === "runid" || normalized === "run_id") {
    return "runId";
  }
  return normalized as QualityExternalGroupBy;
}

function normalizeQualityExternalFilter(
  value: unknown,
  options: { lowerCase?: boolean } = {}
): string | undefined {
  const normalized = firstNonEmptyString(value);
  if (!normalized) {
    return undefined;
  }
  if (options.lowerCase) {
    return normalized.toLowerCase();
  }
  return normalized;
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

function parseRangeBoundary(value: string | undefined, mode: "from" | "to"): string | undefined {
  if (!value) {
    return undefined;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return mode === "from" ? `${value}T00:00:00.000Z` : `${value}T23:59:59.999Z`;
  }
  return toIsoString(value);
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

function mapQualityDailyMetric(item: QualityDailyMetric, metric: QualityMetric) {
  const avgScore = fromRepositoryScore(item.averageScore);
  const passRate = item.total > 0 ? Number((item.passed / item.total).toFixed(6)) : 0;
  return {
    date: item.date,
    metric,
    totalEvents: item.total,
    passedEvents: item.passed,
    failedEvents: item.failed,
    avgScore,
    passRate,
  };
}

function mapQualityExternalGroup(item: QualityExternalMetricGroup) {
  const avgScore = fromRepositoryScore(item.averageScore);
  const passRate = item.total > 0 ? Number((item.passed / item.total).toFixed(6)) : 0;
  return {
    groupBy: item.groupBy,
    value: item.value,
    totalEvents: item.total,
    passedEvents: item.passed,
    failedEvents: item.failed,
    avgScore,
    passRate,
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
    criticalScore: fromRepositoryScore(Number.isFinite(criticalScore) ? criticalScore : warningScore),
    weight: Number.isFinite(weight) ? Math.max(0, weight) : 1,
    enabled: metadata.enabled !== false,
    updatedByUserId: firstNonEmptyString(metadata.updatedByUserId),
    updatedAt: scorecard.updatedAt,
  };
}

function toRepositoryReplayStatus(status: string | undefined): "pending" | "running" | "completed" | "failed" | "cancelled" | undefined {
  if (!status) {
    return undefined;
  }
  if (status === "success" || status === "succeeded") {
    return "completed";
  }
  if (
    status === "pending" ||
    status === "running" ||
    status === "completed" ||
    status === "failed" ||
    status === "cancelled"
  ) {
    return status;
  }
  return undefined;
}

function mapReplayDataset(dataset: ReplayDataset) {
  const metadata = normalizeRecord(dataset.metadata);
  return {
    id: dataset.id,
    tenantId: dataset.tenantId,
    name: dataset.name,
    datasetId: dataset.id,
    datasetRef:
      firstNonEmptyString(dataset.externalDatasetId) ??
      firstNonEmptyString(metadata.datasetId, metadata.datasetRef) ??
      undefined,
    model: firstNonEmptyString(dataset.model, metadata.model) ?? "unknown",
    promptVersion:
      firstNonEmptyString(dataset.promptVersion, metadata.promptVersion, metadata.prompt_version) ??
      undefined,
    caseCount: dataset.caseCount,
    sampleCount: dataset.caseCount,
    metadata,
    createdAt: dataset.createdAt,
    updatedAt: dataset.updatedAt,
  };
}

function toReplayCaseSourceType(value: unknown): "manual" | "session" | "import" | undefined {
  const normalized = firstNonEmptyString(value)?.toLowerCase();
  if (!normalized || !REPLAY_CASE_SOURCE_TYPE_SET.has(normalized)) {
    return undefined;
  }
  return normalized as "manual" | "session" | "import";
}

function summarizeReplayDatasetCaseSources(
  items: Array<{ metadata?: Record<string, unknown> }>
): Record<string, number> {
  const summary: Record<string, number> = {};
  for (const item of items) {
    const sourceType = toReplayCaseSourceType(normalizeRecord(item.metadata).sourceType) ?? "manual";
    summary[sourceType] = (summary[sourceType] ?? 0) + 1;
  }
  return summary;
}

function trimReplayCaseText(value: unknown): string | undefined {
  const normalized = firstNonEmptyString(value);
  if (!normalized) {
    return undefined;
  }
  return normalized.replace(/\r\n/g, "\n").trim();
}

function pickReplaySessionInputText(
  events: Array<{ role?: string; text?: string }>
): string | undefined {
  for (const event of events) {
    if (firstNonEmptyString(event.role)?.toLowerCase() === "user") {
      const text = trimReplayCaseText(event.text);
      if (text) {
        return text;
      }
    }
  }
  for (const event of events) {
    const text = trimReplayCaseText(event.text);
    if (text) {
      return text;
    }
  }
  return undefined;
}

function pickReplaySessionOutputText(
  events: Array<{ role?: string; text?: string }>,
  inputText: string | undefined
): string | undefined {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    if (firstNonEmptyString(event.role)?.toLowerCase() === "assistant") {
      const text = trimReplayCaseText(event.text);
      if (text && text !== inputText) {
        return text;
      }
    }
  }
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const text = trimReplayCaseText(events[index]?.text);
    if (text && text !== inputText) {
      return text;
    }
  }
  return undefined;
}

async function materializeReplayDatasetCasesFromSessions(input: {
  tenantId: string;
  datasetId: string;
  sessionIds?: string[];
  filters?: Record<string, unknown>;
  sampleLimit: number;
  sanitized: boolean;
  snapshotVersion?: string;
}) {
  const sessions =
    input.sessionIds && input.sessionIds.length > 0
      ? (
          await Promise.all(
            input.sessionIds.slice(0, input.sampleLimit).map(async (sessionId) => {
              const detail = await repository.getSessionById(input.tenantId, sessionId);
              return detail;
            })
          )
        ).filter((item): item is NonNullable<typeof item> => Boolean(item))
      : (
          await repository.searchSessions(
            {
              ...(input.filters ?? {}),
              limit: input.sampleLimit,
            },
            input.tenantId
          )
        ).items;

  const materializedItems = (
    await Promise.all(
      sessions.map(async (session, index) => {
        const sessionDetail = await repository.getSessionById(input.tenantId, session.id);
        if (!sessionDetail) {
          return null;
        }
        const eventPayload = await repository.listSessionEvents(input.tenantId, session.id, 500);
        const inputText = pickReplaySessionInputText(eventPayload.items);
        if (!inputText) {
          return null;
        }
        const expectedOutput = pickReplaySessionOutputText(eventPayload.items, inputText);
        return {
          caseId: `session-${session.id}`,
          sortOrder: index,
          input: inputText,
          expectedOutput,
          baselineOutput: expectedOutput,
          candidateInput: inputText,
          metadata: {
            sourceType: "session",
            sourceRef: session.id,
            snapshotVersion: input.snapshotVersion ?? sessionDetail.startedAt,
            sanitized: input.sanitized,
            session: {
              id: sessionDetail.id,
              sourceId: sessionDetail.sourceId,
              sourceName: sessionDetail.sourceName,
              sourceType: sessionDetail.sourceType,
              provider: sessionDetail.provider,
              tool: sessionDetail.tool,
              model: sessionDetail.model,
              workspace: sessionDetail.workspace,
              startedAt: sessionDetail.startedAt,
              endedAt: sessionDetail.endedAt,
              messageCount: sessionDetail.messageCount,
            },
          },
        };
      })
    )
  ).filter((item): item is NonNullable<typeof item> => Boolean(item));

  const items = await repository.replaceReplayDatasetCases(
    input.tenantId,
    input.datasetId,
    materializedItems
  );
  return {
    items,
    materialized: items.length,
    skipped: Math.max(0, sessions.length - items.length),
    sourceSummary: summarizeReplayDatasetCaseSources(items),
  };
}

function buildReplayArtifactInlinePreview(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  if (Array.isArray(value)) {
    return {
      total: value.length,
      items: value.slice(0, 5),
    };
  }

  const record = value as Record<string, unknown>;
  if (Array.isArray(record.items)) {
    return {
      ...record,
      total: typeof record.total === "number" ? record.total : record.items.length,
      items: record.items.slice(0, 5),
    };
  }
  if (Array.isArray(record.topRegressions)) {
    return {
      ...record,
      topRegressions: record.topRegressions.slice(0, 5),
    };
  }
  return record;
}

function parseReplayDiffItems(diffPayload: Record<string, unknown>) {
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

function filterReplayDiffItems(
  items: Array<{
    caseId: string;
    metric: QualityMetric;
    baselineScore: number;
    candidateScore: number;
    delta: number;
    verdict: "improved" | "regressed" | "unchanged";
    detail?: string;
  }>,
  options: {
    keyword?: string;
    limit?: number;
  }
) {
  const keyword = firstNonEmptyString(options.keyword)?.toLowerCase();
  const filtered = keyword
    ? items.filter((item) =>
        [
          item.caseId,
          item.metric,
          item.verdict,
          item.detail ?? "",
          `${item.delta}`,
        ]
          .join(" ")
          .toLowerCase()
          .includes(keyword)
      )
    : items;
  if (typeof options.limit === "number" && options.limit > 0) {
    return filtered.slice(0, options.limit);
  }
  return filtered;
}

const REPLAY_ARTIFACT_TYPES = ["summary", "diff", "cases"] as const;

type ReplayArtifactType = (typeof REPLAY_ARTIFACT_TYPES)[number];

function isReplayArtifactType(value: string): value is ReplayArtifactType {
  return REPLAY_ARTIFACT_TYPES.some((item) => item === value);
}

function buildReplayArtifactDownloadUrl(runId: string, artifactType: ReplayArtifactType): string {
  return `/api/v2/replay/runs/${encodeURIComponent(runId)}/artifacts/${encodeURIComponent(artifactType)}/download`;
}

function mapReplayArtifact(artifact: ReplayArtifact) {
  return {
    type: artifact.artifactType,
    name: artifact.name,
    description: artifact.description,
    contentType: artifact.contentType,
    downloadName: artifact.name,
    byteSize: artifact.byteSize,
    checksum: artifact.checksum,
    storageBackend: artifact.storageBackend,
    storageKey: artifact.storageKey,
    metadata: normalizeRecord(artifact.metadata),
    createdAt: artifact.createdAt,
    downloadUrl: buildReplayArtifactDownloadUrl(artifact.runId, artifact.artifactType),
  };
}

function mapReplayRun(run: ReplayRun) {
  const parameters = normalizeRecord(run.parameters);
  const summary = normalizeRecord(run.summary);
  const diff = normalizeRecord(run.diff);
  const diffs = parseReplayDiffItems(diff);

  const totalCases = Math.max(0, toInteger(summary.totalCases, 0));
  const processedCases = Math.max(0, toInteger(summary.processedCases, 0));
  const improvedCases = Math.max(0, toInteger(summary.improvedCases, 0));
  const regressedCases = Math.max(0, toInteger(summary.regressedCases, 0));
  const unchangedCases = Math.max(0, toInteger(summary.unchangedCases, 0));

  return {
    id: run.id,
    tenantId: run.tenantId,
    datasetId: run.datasetId,
    baselineId: run.datasetId,
    candidateLabel: firstNonEmptyString(parameters.candidateLabel) ?? "candidate",
    status: run.status,
    totalCases,
    processedCases,
    improvedCases,
    regressedCases,
    unchangedCases,
    summary,
    diffs,
    error: run.error,
    startedAt: run.startedAt,
    finishedAt: run.finishedAt,
    createdAt: run.createdAt,
    updatedAt: run.updatedAt,
  };
}

function computePearsonCorrelationCoefficient(points: Array<{ x: number; y: number }>): number | null {
  if (points.length < 2) {
    return null;
  }
  const xs = points.map((point) => point.x);
  const ys = points.map((point) => point.y);
  const xAvg = xs.reduce((sum, value) => sum + value, 0) / xs.length;
  const yAvg = ys.reduce((sum, value) => sum + value, 0) / ys.length;
  let numerator = 0;
  let xDenominator = 0;
  let yDenominator = 0;
  for (const point of points) {
    const xDelta = point.x - xAvg;
    const yDelta = point.y - yAvg;
    numerator += xDelta * yDelta;
    xDenominator += xDelta ** 2;
    yDenominator += yDelta ** 2;
  }
  if (xDenominator <= 0 || yDenominator <= 0) {
    return null;
  }
  const value = numerator / Math.sqrt(xDenominator * yDenominator);
  if (!Number.isFinite(value)) {
    return null;
  }
  return Number(value.toFixed(6));
}

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 api-v2 审计日志失败。", error);
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

apiV2Routes.post("/quality/evaluations", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateQualityEventCreateInput({
    ...bodyRecord,
    replayJobId:
      firstNonEmptyString(bodyRecord.replayJobId) ?? firstNonEmptyString(bodyRecord.replayRunId),
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
    ...(validation.data.externalSource
      ? {
          externalSource: validation.data.externalSource,
        }
      : {}),
  };

  const normalizedScore = toRepositoryScore(validation.data.score);
  const created = await repository.createQualityEvent(auth.tenantId, {
    scorecardKey: validation.data.metric,
    metricKey: validation.data.metric,
    externalSource: validation.data.externalSource,
    score: normalizedScore,
    passed: normalizedScore >= 0.8,
    metadata,
    createdAt: validation.data.occurredAt,
  });
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.v2.quality.evaluation_created",
    level: "info",
    detail: `Created quality evaluation ${created.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      evaluationId: created.id,
      metric: validation.data.metric,
    },
  });

  return c.json(
    {
      id: created.id,
      tenantId: auth.tenantId,
      metric: validation.data.metric,
      score: validation.data.score,
      sampleCount: validation.data.sampleCount,
      occurredAt: validation.data.occurredAt,
      sessionId: validation.data.sessionId,
      replayRunId: validation.data.replayJobId,
      externalSource: validation.data.externalSource,
      notes: validation.data.notes,
      metadata,
      createdAt: created.createdAt,
    },
    201
  );
});

apiV2Routes.get("/quality/metrics", async (c) => {
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
  const limit = toPositiveInteger(c.req.query("limit"), 60);
  if (limit > 365) {
    return c.json({ message: "limit 必须是 1 到 365 的整数。" }, 400);
  }

  const provider = normalizeQualityExternalFilter(c.req.query("provider"), { lowerCase: true });
  const repo = normalizeQualityExternalFilter(c.req.query("repo"), { lowerCase: true });
  const workflow = normalizeQualityExternalFilter(c.req.query("workflow"));
  const runId = normalizeQualityExternalFilter(c.req.query("runId"));
  const groupByRaw = c.req.query("groupBy");
  const groupBy = toQualityExternalGroupBy(groupByRaw);
  if (groupByRaw !== undefined && !groupBy) {
    return c.json({ message: "groupBy 必须是 provider/repo/workflow/runId 之一。" }, 400);
  }

  const metrics = await repository.listQualityDailyMetrics(auth.tenantId, {
    from: range.from,
    to: range.to,
    scorecardKey: metricQuery ? metric : undefined,
    provider,
    repo,
    workflow,
    runId,
    limit,
  });
  const groups = groupBy
    ? await repository.listQualityExternalMetricGroups(auth.tenantId, {
        from: range.from,
        to: range.to,
        scorecardKey: metricQuery ? metric : undefined,
        provider,
        repo,
        workflow,
        runId,
        groupBy,
        limit: Math.min(limit, 200),
      })
    : [];

  const mappedItems = metrics.map((item) => mapQualityDailyMetric(item, metric));
  const totalEvents = mappedItems.reduce((sum, item) => sum + item.totalEvents, 0);
  const passedEvents = mappedItems.reduce((sum, item) => sum + item.passedEvents, 0);
  const failedEvents = mappedItems.reduce((sum, item) => sum + item.failedEvents, 0);
  const avgScore =
    totalEvents > 0
      ? Number(
          (
            mappedItems.reduce((sum, item) => sum + item.avgScore * item.totalEvents, 0) / totalEvents
          ).toFixed(4)
        )
      : 0;

  return c.json({
    items: mappedItems,
    total: mappedItems.length,
    summary: {
      totalEvents,
      passedEvents,
      failedEvents,
      passRate: totalEvents > 0 ? Number((passedEvents / totalEvents).toFixed(6)) : 0,
      avgScore,
    },
    ...(groupBy
      ? {
          groups: groups.map(mapQualityExternalGroup),
        }
      : {}),
  });
});

apiV2Routes.get("/quality/reports/cost-correlation", async (c) => {
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
  const metric = metricQuery ? toQualityMetric(metricQuery) : undefined;

  const qualityItems = await repository.listQualityDailyMetrics(auth.tenantId, {
    from: range.from,
    to: range.to,
    scorecardKey: metric,
    limit: 366,
  });
  const usageItems = await repository.listUsageDaily({
    tenantId: auth.tenantId,
    from: range.from,
    to: range.to,
    limit: 366,
  });
  const usageByDate = new Map(usageItems.map((item) => [item.date, item]));

  const items = qualityItems.map((qualityItem) => {
    const usageItem = usageByDate.get(qualityItem.date);
    const avgScore = fromRepositoryScore(qualityItem.averageScore);
    const cost = Number((usageItem?.cost ?? 0).toFixed(6));
    return {
      date: qualityItem.date,
      metric: metric ?? "all",
      avgScore,
      totalEvents: qualityItem.total,
      cost,
      tokens: usageItem?.tokens ?? 0,
      sessions: usageItem?.sessions ?? 0,
      costPerQualityPoint: avgScore > 0 ? Number((cost / avgScore).toFixed(6)) : 0,
    };
  });
  const pairedPoints = items
    .filter((item) => item.totalEvents > 0)
    .map((item) => ({
      x: item.avgScore,
      y: item.cost,
    }));
  const correlation = computePearsonCorrelationCoefficient(pairedPoints);

  return c.json({
    items,
    total: items.length,
    summary: {
      metric: metric ?? "all",
      correlationCoefficient: correlation,
      pairs: pairedPoints.length,
      from: range.from,
      to: range.to,
    },
  });
});

apiV2Routes.get("/quality/reports/project-trends", async (c) => {
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
  const metric = metricQuery ? toQualityMetric(metricQuery) : undefined;

  const limit = toPositiveInteger(c.req.query("limit"), 20);
  if (limit > 200) {
    return c.json({ message: "limit 必须是 1 到 200 的整数。" }, 400);
  }

  const includeUnknown = toBoolean(c.req.query("includeUnknown"), false);
  const provider = normalizeQualityExternalFilter(c.req.query("provider"), { lowerCase: true });
  const workflow = normalizeQualityExternalFilter(c.req.query("workflow"));

  const groups = await repository.listQualityExternalMetricGroups(auth.tenantId, {
    from: range.from,
    to: range.to,
    scorecardKey: metric,
    provider,
    workflow,
    groupBy: "repo",
    limit: Math.min(500, Math.max(50, limit * 5)),
  });
  const candidateGroups = groups
    .filter((group) => includeUnknown || group.value !== "unknown")
    .slice(0, limit);

  const items = await Promise.all(
    candidateGroups.map(async (group) => {
      const usageItems = await repository.listUsageDaily({
        tenantId: auth.tenantId,
        from: range.from,
        to: range.to,
        project: group.value,
        limit: 366,
      });
      const totalCost = Number(
        usageItems.reduce((sum, usageItem) => sum + usageItem.cost, 0).toFixed(6)
      );
      const totalTokens = usageItems.reduce((sum, usageItem) => sum + usageItem.tokens, 0);
      const totalSessions = usageItems.reduce((sum, usageItem) => sum + usageItem.sessions, 0);
      const avgScore = fromRepositoryScore(group.averageScore);
      const passRate = group.total > 0 ? Number((group.passed / group.total).toFixed(6)) : 0;
      return {
        project: group.value,
        metric: metric ?? "all",
        totalEvents: group.total,
        passedEvents: group.passed,
        failedEvents: group.failed,
        passRate,
        avgScore,
        totalCost,
        totalTokens,
        totalSessions,
        costPerQualityPoint: avgScore > 0 ? Number((totalCost / avgScore).toFixed(6)) : 0,
      };
    })
  );

  items.sort((left, right) => {
    if (right.totalEvents !== left.totalEvents) {
      return right.totalEvents - left.totalEvents;
    }
    if (right.totalCost !== left.totalCost) {
      return right.totalCost - left.totalCost;
    }
    return right.avgScore - left.avgScore;
  });

  const totalEvents = items.reduce((sum, item) => sum + item.totalEvents, 0);
  const passedEvents = items.reduce((sum, item) => sum + item.passedEvents, 0);
  const failedEvents = items.reduce((sum, item) => sum + item.failedEvents, 0);
  const totalCost = Number(items.reduce((sum, item) => sum + item.totalCost, 0).toFixed(6));
  const totalTokens = items.reduce((sum, item) => sum + item.totalTokens, 0);
  const totalSessions = items.reduce((sum, item) => sum + item.totalSessions, 0);
  const avgScore =
    totalEvents > 0
      ? Number(
          (
            items.reduce((sum, item) => sum + item.avgScore * item.totalEvents, 0) / totalEvents
          ).toFixed(4)
        )
      : 0;

  return c.json({
    items,
    total: items.length,
    summary: {
      metric: metric ?? "all",
      totalEvents,
      passedEvents,
      failedEvents,
      passRate: totalEvents > 0 ? Number((passedEvents / totalEvents).toFixed(6)) : 0,
      avgScore,
      totalCost,
      totalTokens,
      totalSessions,
      from: range.from,
      to: range.to,
    },
    filters: {
      from: range.from ?? null,
      to: range.to ?? null,
      metric: metric ?? "all",
      includeUnknown,
      provider: provider ?? null,
      workflow: workflow ?? null,
      limit,
    },
  });
});

apiV2Routes.get("/quality/scorecards", async (c) => {
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
  const limit = toPositiveInteger(c.req.query("limit"), 100);
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

apiV2Routes.put("/quality/scorecards/:id", async (c) => {
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
    score: toRepositoryScore(validation.data.targetScore),
    dimensions: {
      warningScore: toRepositoryScore(validation.data.warningScore),
      criticalScore: toRepositoryScore(validation.data.criticalScore),
      weight: validation.data.weight ?? 1,
    },
    metadata: {
      enabled: validation.data.enabled,
      updatedByUserId: auth.userId,
    },
    updatedAt: validation.data.updatedAt,
  });
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.v2.quality.scorecard_upserted",
    level: "info",
    detail: `Upserted v2 quality scorecard ${saved.scorecardKey}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      scorecardKey: saved.scorecardKey,
      updatedByUserId: auth.userId,
    },
  });

  return c.json(mapQualityScorecard(saved));
});

apiV2Routes.post("/replay/datasets", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateReplayDatasetCreateInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
    datasetRef:
      firstNonEmptyString(bodyRecord.datasetRef) ??
      firstNonEmptyString(bodyRecord.datasetId),
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  try {
    const dataset = await repository.createReplayDataset(auth.tenantId, {
      name: validation.data.name,
      description: firstNonEmptyString(bodyRecord.description),
      externalDatasetId: validation.data.datasetRef,
      model: validation.data.model,
      promptVersion: validation.data.promptVersion,
      caseCount: validation.data.sampleCount ?? 0,
      metadata: validation.data.metadata,
    });
    const requestId = c.get("requestId");
    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:${requestId}`,
      action: "control_plane.v2.replay.dataset_created",
      level: "info",
      detail: `Created replay dataset ${dataset.id}.`,
      metadata: {
        requestId,
        tenantId: auth.tenantId,
        datasetId: dataset.id,
      },
    });
    return c.json(mapReplayDataset(dataset), 201);
  } catch (error) {
    if (
      error instanceof Error &&
      error.message.startsWith("replay_dataset_name_already_exists:")
    ) {
      return c.json({ message: "回放数据集名称已存在，请更换后重试。" }, 409);
    }
    throw error;
  }
});

apiV2Routes.get("/replay/datasets", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const limit = toPositiveInteger(c.req.query("limit"), 100);
  if (limit > 500) {
    return c.json({ message: "limit 必须是 1 到 500 的整数。" }, 400);
  }
  const keyword = firstNonEmptyString(c.req.query("keyword"));
  const items = (await repository.listReplayDatasets(auth.tenantId, { keyword, limit: 500 }))
    .map(mapReplayDataset);
  return c.json({
    items: items.slice(0, limit),
    total: items.length,
  });
});

apiV2Routes.get("/replay/datasets/:id/cases", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const datasetId = c.req.param("id")?.trim();
  if (!datasetId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const dataset = await repository.getReplayDatasetById(auth.tenantId, datasetId);
  if (!dataset) {
    return c.json({ message: `未找到回放数据集：${datasetId}` }, 404);
  }
  const limit = toPositiveInteger(c.req.query("limit"), 1000);
  if (limit > 5000) {
    return c.json({ message: "limit 必须是 1 到 5000 的整数。" }, 400);
  }
  const items = await repository.listReplayDatasetCases(auth.tenantId, datasetId, { limit });
  return c.json({
    datasetId,
    items,
    total: items.length,
  });
});

apiV2Routes.post("/replay/datasets/:id/cases", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const datasetId = c.req.param("id")?.trim();
  if (!datasetId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateReplayDatasetCasesReplaceInput({
    tenantId: auth.tenantId,
    datasetId,
    items: Array.isArray(body) ? body : bodyRecord.items,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }
  try {
    const items = await repository.replaceReplayDatasetCases(
      auth.tenantId,
      datasetId,
      validation.data.items.map((item) => ({
        caseId: item.caseId,
        sortOrder: item.sortOrder,
        input: item.input,
        expectedOutput: item.expectedOutput,
        baselineOutput: item.baselineOutput,
        candidateInput: item.candidateInput,
        metadata: item.metadata ?? {},
      }))
    );
    return c.json({
      datasetId,
      items,
      total: items.length,
    });
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("replay_dataset_not_found:")) {
      return c.json({ message: `未找到回放数据集：${datasetId}` }, 404);
    }
    if (error instanceof Error && error.message.startsWith("replay_dataset_case_input_required:")) {
      return c.json({ message: "回放样本缺少 input 字段。" }, 400);
    }
    throw error;
  }
});

apiV2Routes.post("/replay/datasets/:id/materialize", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const datasetId = c.req.param("id")?.trim();
  if (!datasetId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const dataset = await repository.getReplayDatasetById(auth.tenantId, datasetId);
  if (!dataset) {
    return c.json({ message: `未找到回放数据集：${datasetId}` }, 404);
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateReplayDatasetMaterializeInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
    datasetId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  try {
    const sampleLimit = validation.data.sampleLimit ?? 20;
    const materialized = await materializeReplayDatasetCasesFromSessions({
      tenantId: auth.tenantId,
      datasetId,
      sessionIds: validation.data.sessionIds,
      filters: validation.data.filters as Record<string, unknown> | undefined,
      sampleLimit,
      sanitized: validation.data.sanitized ?? true,
      snapshotVersion: validation.data.snapshotVersion,
    });
    if (materialized.materialized <= 0) {
      return c.json(
        { message: "未能从所选会话中物化有效样本，请调整筛选条件后重试。" },
        400
      );
    }

    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:replay-materialize:${datasetId}:${Date.now()}`,
      action: "control_plane.v2.replay.dataset_materialized",
      level: "info",
      detail: `Materialized replay dataset ${datasetId} from historical sessions.`,
      metadata: {
        datasetId,
        materialized: materialized.materialized,
        skipped: materialized.skipped,
        sourceType: "session",
        sampleLimit,
        sessionIds: validation.data.sessionIds,
        filters: validation.data.filters ?? {},
        sourceSummary: materialized.sourceSummary,
      },
    });

    return c.json({
      datasetId,
      sourceType: "session",
      materialized: materialized.materialized,
      skipped: materialized.skipped,
      sourceSummary: materialized.sourceSummary,
      items: materialized.items,
      total: materialized.items.length,
      filters: {
        datasetId,
        sessionIds: validation.data.sessionIds,
        filters: validation.data.filters ?? {},
        sampleLimit,
        sanitized: validation.data.sanitized ?? true,
        snapshotVersion: validation.data.snapshotVersion,
      },
    });
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("replay_dataset_not_found:")) {
      return c.json({ message: `未找到回放数据集：${datasetId}` }, 404);
    }
    throw error;
  }
});

apiV2Routes.post("/replay/runs", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateReplayRunCreateInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const sampleLimit = validation.data.sampleLimit ?? 100;
  const dataset = await repository.getReplayDatasetById(auth.tenantId, validation.data.datasetId);
  if (!dataset) {
    return c.json({ message: `未找到回放数据集：${validation.data.datasetId}` }, 404);
  }
  const totalCases = Math.min(sampleLimit, Math.max(0, dataset.caseCount));
  const metric = toQualityMetric(validation.data.metadata?.metric);
  const summary = {
    metric,
    totalCases,
    processedCases: 0,
    improvedCases: 0,
    regressedCases: 0,
    unchangedCases: 0,
    executionSource: "dataset_cases",
    materializedCaseCount: totalCases,
  };
  try {
    const replayRun = await repository.createReplayRun(auth.tenantId, {
      datasetId: validation.data.datasetId,
      status: "pending",
      parameters: {
        candidateLabel: validation.data.candidateLabel,
        from: validation.data.from,
        to: validation.data.to,
        sampleLimit,
        executionSource: "dataset_cases",
        ...(validation.data.metadata ?? {}),
      },
      summary,
      diff: {},
      createdAt: new Date().toISOString(),
    });
    enqueueReplayJobExecution(auth.tenantId, replayRun.id);
    return c.json(mapReplayRun(replayRun), 201);
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("replay_dataset_not_found:")) {
      return c.json({ message: `未找到回放数据集：${validation.data.datasetId}` }, 404);
    }
    throw error;
  }
});

apiV2Routes.get("/replay/runs", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const status = firstNonEmptyString(c.req.query("status"))?.toLowerCase();
  if (status && !REPLAY_STATUS_SET.has(status)) {
    return c.json(
      { message: "status 必须是 pending/running/completed/failed/cancelled 之一。" },
      400
    );
  }
  const limit = toPositiveInteger(c.req.query("limit"), 100);
  if (limit > 500) {
    return c.json({ message: "limit 必须是 1 到 500 的整数。" }, 400);
  }

  const runs = await repository.listReplayRuns(auth.tenantId, {
    datasetId:
      firstNonEmptyString(c.req.query("datasetId")) ??
      firstNonEmptyString(c.req.query("baselineId")),
    status: toRepositoryReplayStatus(status),
    limit,
  });
  const items = runs.map(mapReplayRun);
  return c.json({
    items,
    total: items.length,
  });
});

apiV2Routes.get("/replay/runs/:id", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const runId = c.req.param("id")?.trim();
  if (!runId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const replayRun = await repository.getReplayRunById(auth.tenantId, runId);
  if (!replayRun) {
    return c.json({ message: `未找到回放运行：${runId}` }, 404);
  }
  return c.json(mapReplayRun(replayRun));
});

apiV2Routes.get("/replay/runs/:id/diffs", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const runId = c.req.param("id")?.trim();
  if (!runId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const replayRun = await repository.getReplayRunById(auth.tenantId, runId);
  if (!replayRun) {
    return c.json({ message: `未找到回放运行：${runId}` }, 404);
  }
  const datasetId =
    firstNonEmptyString(c.req.query("datasetId")) ??
    firstNonEmptyString(c.req.query("baselineId"));
  if (datasetId && datasetId !== replayRun.datasetId) {
    return c.json({ message: "datasetId 与运行所属数据集不匹配。" }, 400);
  }
  const keyword = firstNonEmptyString(c.req.query("keyword"));
  const limitRaw = c.req.query("limit");
  const limit =
    limitRaw === undefined ? undefined : toPositiveInteger(limitRaw, Number.NaN);
  if (
    limitRaw !== undefined &&
    (typeof limit !== "number" || !Number.isFinite(limit) || limit <= 0 || limit > 500)
  ) {
    return c.json({ message: "limit 必须是 1 到 500 的整数。" }, 400);
  }
  const diffPayload = (await repository.getReplayRunDiff(auth.tenantId, runId)) ?? {};
  const diffItems = filterReplayDiffItems(parseReplayDiffItems(diffPayload), {
    keyword,
    limit,
  });
  return c.json({
    runId: replayRun.id,
    jobId: replayRun.id,
    datasetId: replayRun.datasetId,
    diffs: diffItems,
    total: diffItems.length,
    summary: normalizeRecord(replayRun.summary),
    filters: {
      datasetId: replayRun.datasetId,
      baselineId: replayRun.datasetId,
      runId: replayRun.id,
      jobId: replayRun.id,
      keyword: keyword ?? null,
      limit: limit ?? null,
    },
  });
});

apiV2Routes.get("/replay/runs/:id/artifacts", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const runId = c.req.param("id")?.trim();
  if (!runId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const replayRun = await repository.getReplayRunById(auth.tenantId, runId);
  if (!replayRun) {
    return c.json({ message: `未找到回放运行：${runId}` }, 404);
  }
  const storedArtifacts = await repository.listReplayArtifacts(auth.tenantId, runId, { limit: 20 });
  const items = await Promise.all(
    storedArtifacts.map(async (artifact) => {
      const mapped = mapReplayArtifact(artifact);
      const content = await readReplayArtifactContent(artifact);
      if (!content) {
        return mapped;
      }
      try {
        const parsed = JSON.parse(Buffer.from(content).toString("utf8"));
        const inline = buildReplayArtifactInlinePreview(parsed);
        return inline ? { ...mapped, inline } : mapped;
      } catch {
        return mapped;
      }
    })
  );
  return c.json({
    runId: replayRun.id,
    jobId: replayRun.id,
    datasetId: replayRun.datasetId,
    items,
    total: items.length,
  });
});

apiV2Routes.get("/replay/runs/:id/artifacts/:artifactType/download", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const runId = c.req.param("id")?.trim();
  if (!runId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const artifactType = c.req.param("artifactType")?.trim().toLowerCase();
  if (!artifactType || !isReplayArtifactType(artifactType)) {
    return c.json({ message: "artifactType 仅支持 summary、diff 或 cases。" }, 400);
  }

  const replayRun = await repository.getReplayRunById(auth.tenantId, runId);
  if (!replayRun) {
    return c.json({ message: `未找到回放运行：${runId}` }, 404);
  }
  const artifact = await repository.getReplayArtifactByType(auth.tenantId, runId, artifactType);
  if (!artifact) {
    return c.json({ message: `未找到回放工件：${artifactType}` }, 404);
  }
  const content = await readReplayArtifactContent(artifact);
  if (!content) {
    return c.json({ message: `回放工件内容不可用：${artifactType}` }, 404);
  }

  c.header("Content-Type", artifact.contentType);
  c.header("Content-Disposition", `attachment; filename="${artifact.name}"`);
  c.header("Cache-Control", "no-store");
  const responseBody = new Blob([new Uint8Array(Array.from(content))], {
    type: artifact.contentType,
  });
  return new Response(responseBody, {
    status: 200,
    headers: c.res.headers,
  });
});

apiV2Routes.get("/residency/policies/current", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const policy = await repository.getTenantResidencyPolicy(auth.tenantId);
  if (!policy) {
    return c.json({ message: "当前租户尚未配置数据主权策略。" }, 404);
  }
  return c.json(policy);
});

apiV2Routes.put("/residency/policies/current", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateTenantResidencyPolicyUpsertInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
    updatedAt: toIsoString(bodyRecord.updatedAt) ?? new Date().toISOString(),
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const policy = await repository.upsertTenantResidencyPolicy(auth.tenantId, validation.data);
  return c.json(policy);
});

apiV2Routes.get("/residency/region-mappings", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const regions = await repository.listResidencyRegions();
  const policy = await repository.getTenantResidencyPolicy(auth.tenantId);

  const replicaSet = new Set(policy?.replicaRegions ?? []);
  const items = regions.map((region) => {
    const role =
      policy?.primaryRegion === region.id
        ? "primary"
        : replicaSet.has(region.id)
          ? "replica"
          : "available";
    return {
      regionId: region.id,
      regionName: region.name,
      active: region.active,
      role,
      writable: role === "primary",
      metadata: {
        description: region.description,
      },
    };
  });
  return c.json({
    items,
    total: items.length,
  });
});

apiV2Routes.get("/residency/replications", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const validation = validateReplicationJobListInput(c.req.query());
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }
  const result = await repository.listReplicationJobs(auth.tenantId, validation.data);
  return c.json({
    items: result.items,
    total: result.total,
  });
});

apiV2Routes.post("/residency/replications", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateReplicationJobCreateInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }
  const job = await repository.createReplicationJob(auth.tenantId, validation.data, {
    createdByUserId: auth.userId,
  });
  return c.json(job, 201);
});

apiV2Routes.post("/residency/replications/:id/approvals", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const replicationId = c.req.param("id")?.trim();
  if (!replicationId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const validation = validateReplicationJobApproveInput(body);
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }
  const current = await repository.getReplicationJobById(auth.tenantId, replicationId);
  if (!current) {
    return c.json({ message: `未找到复制任务 ${replicationId}。` }, 404);
  }
  if (current.status !== "pending") {
    return c.json({ message: `复制任务 ${replicationId} 当前状态为 ${current.status}，无法审批。` }, 409);
  }
  const job = await repository.approveReplicationJob(auth.tenantId, replicationId, validation.data, {
    userId: auth.userId,
  });
  if (!job) {
    return c.json({ message: `未找到复制任务 ${replicationId}。` }, 404);
  }
  return c.json(job);
});

apiV2Routes.post("/residency/replications/:id/cancel", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const replicationId = c.req.param("id")?.trim();
  if (!replicationId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const validation = validateReplicationJobCancelInput(body);
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const current = await repository.getReplicationJobById(auth.tenantId, replicationId);
  if (!current) {
    return c.json({ message: `未找到复制任务 ${replicationId}。` }, 404);
  }
  if (current.status !== "pending" && current.status !== "running") {
    return c.json({ message: `复制任务 ${replicationId} 当前状态为 ${current.status}，无法取消。` }, 409);
  }
  const job = await repository.cancelReplicationJob(auth.tenantId, replicationId, validation.data, {
    userId: auth.userId,
  });
  if (!job) {
    return c.json({ message: `未找到复制任务 ${replicationId}。` }, 404);
  }
  return c.json(job);
});
