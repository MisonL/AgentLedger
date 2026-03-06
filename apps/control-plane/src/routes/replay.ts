import { createHash } from "node:crypto";
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
import { storeReplayArtifact } from "./replay-artifact-store";
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
type ReplayExecutionSource = "synthetic" | "dataset_cases" | "session_materialized";
type ReplayJobTerminalStatus = "completed" | "failed" | "cancelled";
type ReplayJobExecutionTask = {
  tenantId: string;
  replayJobId: string;
};
type ReplayJobExecutionResult = {
  status?: ReplayJobTerminalStatus;
  summary?: Record<string, unknown>;
  diff?: Record<string, unknown>;
  artifacts?: Array<{
    artifactType: "summary" | "diff" | "cases";
    description: string;
    payload: Record<string, unknown>;
  }>;
  error?: string;
};
type ReplayJobExecutionHandler = (input: {
  tenantId: string;
  replayJob: ReplayJob;
}) => Promise<ReplayJobExecutionResult>;

const replayJobExecutionQueue: ReplayJobExecutionTask[] = [];
let replayJobExecutionDrainScheduled = false;
let replayJobExecutionDrainRunning = false;
let replayJobExecutionHandler: ReplayJobExecutionHandler = defaultReplayJobExecutionHandler;

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

function toRepositoryReplayStatus(status: string | undefined): ReplayJobStatus | undefined {
  switch (status) {
    case "succeeded":
    case "success":
    case "completed":
      return "completed";
    case "pending":
    case "running":
    case "failed":
    case "cancelled":
      return status;
    default:
      return undefined;
  }
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
    status: job.status,
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

function resolveReplayTotalCases(job: ReplayJob): number {
  const parameters = normalizeRecord(job.parameters);
  const summary = normalizeRecord(job.summary);
  const sampleLimit = toNumber(parameters.sampleLimit, Number.NaN);
  if (Number.isInteger(sampleLimit) && sampleLimit > 0) {
    return sampleLimit;
  }
  const totalCases = toNumber(summary.totalCases, Number.NaN);
  if (Number.isInteger(totalCases) && totalCases >= 0) {
    return totalCases;
  }
  return 0;
}

function buildReplaySummaryPayload(
  summaryInput: Record<string, unknown>,
  fallback: {
    metric: QualityMetric;
    totalCases: number;
    processedCases: number;
  }
): Record<string, unknown> {
  const totalCases = Math.max(0, toInteger(summaryInput.totalCases, fallback.totalCases));
  const processedCases = Math.max(
    0,
    Math.min(totalCases, toInteger(summaryInput.processedCases, fallback.processedCases))
  );
  const improvedCases = Math.max(0, toInteger(summaryInput.improvedCases, 0));
  const regressedCases = Math.max(0, toInteger(summaryInput.regressedCases, 0));
  const unchangedFallback = Math.max(0, processedCases - improvedCases - regressedCases);
  const unchangedCases = Math.max(0, toInteger(summaryInput.unchangedCases, unchangedFallback));

  return {
    ...summaryInput,
    metric: toQualityMetric(summaryInput.metric ?? fallback.metric),
    totalCases,
    processedCases,
    improvedCases,
    regressedCases,
    unchangedCases,
  };
}

function toReplayExecutionSource(value: unknown): ReplayExecutionSource | undefined {
  const normalized = firstNonEmptyString(value)?.toLowerCase();
  if (!normalized) {
    return undefined;
  }
  if (normalized === "synthetic") {
    return "synthetic";
  }
  if (normalized === "session_materialized" || normalized === "session-materialized") {
    return "session_materialized";
  }
  if (
    normalized === "dataset_cases" ||
    normalized === "dataset-cases" ||
    normalized === "dataset"
  ) {
    return "dataset_cases";
  }
  return undefined;
}

function summarizeReplayCaseSources(
  cases: Array<{ metadata?: Record<string, unknown> }>
): Record<string, number> {
  const summary: Record<string, number> = {};
  for (const item of cases) {
    const metadata = normalizeRecord(item.metadata);
    const sourceType = firstNonEmptyString(metadata.sourceType)?.toLowerCase();
    const key =
      sourceType === "session" ? "session" : sourceType === "import" ? "import" : "manual";
    summary[key] = (summary[key] ?? 0) + 1;
  }
  return summary;
}

function normalizeReplaySourceSummary(value: unknown): Record<string, number> {
  const record = normalizeRecord(value);
  const summary: Record<string, number> = {};
  for (const [key, rawValue] of Object.entries(record)) {
    const normalizedKey = firstNonEmptyString(key);
    if (!normalizedKey) {
      continue;
    }
    summary[normalizedKey] = Math.max(0, toInteger(rawValue, 0));
  }
  return summary;
}

function inferReplayExecutionSource(input: {
  requested?: ReplayExecutionSource;
  selectedCases: Array<{ metadata?: Record<string, unknown> }>;
}): ReplayExecutionSource {
  if (input.requested === "synthetic") {
    return "synthetic";
  }
  for (const item of input.selectedCases) {
    if (firstNonEmptyString(normalizeRecord(item.metadata).sourceType)?.toLowerCase() === "session") {
      return "session_materialized";
    }
  }
  return "dataset_cases";
}

function buildReplayArtifactDownloadUrl(runId: string, artifactType: "summary" | "diff" | "cases"): string {
  return `/api/v2/replay/runs/${encodeURIComponent(runId)}/artifacts/${encodeURIComponent(artifactType)}/download`;
}

function buildReplayResultDigest(input: {
  tenantId: string;
  replayJob: ReplayJob;
  summary: Record<string, unknown>;
  diffItems: Array<{
    caseId: string;
    metric: QualityMetric;
    baselineScore: number;
    candidateScore: number;
    delta: number;
    verdict: "improved" | "regressed" | "unchanged";
    detail?: string;
  }>;
  sourceSummary: Record<string, number>;
  executionSource: ReplayExecutionSource;
  finishedAt: string;
}): Record<string, unknown> {
  const parameters = normalizeRecord(input.replayJob.parameters);
  const topRegressions = input.diffItems
    .filter((item) => item.verdict === "regressed")
    .sort((left, right) => left.delta - right.delta || left.caseId.localeCompare(right.caseId))
    .slice(0, 5)
    .map((item) => ({
      caseId: item.caseId,
      metric: item.metric,
      delta: item.delta,
      baselineScore: item.baselineScore,
      candidateScore: item.candidateScore,
      detail: item.detail,
    }));

  return {
    tenantId: input.tenantId,
    datasetId: input.replayJob.baselineId,
    baselineId: input.replayJob.baselineId,
    runId: input.replayJob.id,
    jobId: input.replayJob.id,
    candidateLabel: firstNonEmptyString(parameters.candidateLabel) ?? "candidate",
    executionSource: input.executionSource,
    materializedCaseCount: Object.values(input.sourceSummary).reduce(
      (sum, value) => sum + Math.max(0, toInteger(value, 0)),
      0
    ),
    totalCases: Math.max(0, toInteger(input.summary.totalCases, 0)),
    processedCases: Math.max(0, toInteger(input.summary.processedCases, 0)),
    improvedCases: Math.max(0, toInteger(input.summary.improvedCases, 0)),
    regressedCases: Math.max(0, toInteger(input.summary.regressedCases, 0)),
    unchangedCases: Math.max(0, toInteger(input.summary.unchangedCases, 0)),
    sourceSummary: input.sourceSummary,
    topRegressions,
    finishedAt: input.finishedAt,
    artifactUrls: {
      summary: buildReplayArtifactDownloadUrl(input.replayJob.id, "summary"),
      diff: buildReplayArtifactDownloadUrl(input.replayJob.id, "diff"),
      cases: buildReplayArtifactDownloadUrl(input.replayJob.id, "cases"),
    },
  };
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function clampScore(value: number): number {
  return Math.max(0, Math.min(100, Math.round(value)));
}

function tokenizeReplayText(value: string): string[] {
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return [];
  }
  return normalized
    .split(/[^a-z0-9\u4e00-\u9fa5]+/u)
    .map((item) => item.trim())
    .filter(Boolean);
}

function computeTokenOverlapScore(reference: string, candidate: string): number {
  const referenceTokens = tokenizeReplayText(reference);
  const candidateTokens = tokenizeReplayText(candidate);
  if (referenceTokens.length === 0 || candidateTokens.length === 0) {
    return 0;
  }
  const referenceSet = new Set(referenceTokens);
  let overlap = 0;
  for (const token of candidateTokens) {
    if (referenceSet.has(token)) {
      overlap += 1;
    }
  }
  return overlap / Math.max(referenceSet.size, candidateTokens.length);
}

function computeStableJitter(seed: string, max = 12): number {
  const digest = createHash("sha256").update(seed).digest();
  return digest[0] % Math.max(1, max);
}

function buildSyntheticReplayCase(job: ReplayJob, index: number) {
  const parameters = normalizeRecord(job.parameters);
  const candidateLabel = firstNonEmptyString(parameters.candidateLabel) ?? "candidate";
  const caseId = `synthetic-${index + 1}`;
  const inputText = `synthetic replay case ${index + 1} for ${candidateLabel}`;
  return {
    caseId,
    input: inputText,
    expectedOutput: undefined,
    baselineOutput: `baseline ${inputText}`,
    candidateInput: `${candidateLabel} ${inputText}`,
    metadata: {
      synthetic: true,
    },
  };
}

function evaluateReplayCase(input: {
  replayJob: ReplayJob;
  caseId: string;
  metric: QualityMetric;
  source: {
    input: string;
    expectedOutput?: string;
    baselineOutput?: string;
    candidateInput?: string;
    metadata?: Record<string, unknown>;
  };
}) {
  const parameters = normalizeRecord(input.replayJob.parameters);
  const candidateLabel = firstNonEmptyString(parameters.candidateLabel) ?? "candidate";
  const referenceText =
    firstNonEmptyString(
      input.source.expectedOutput,
      input.source.baselineOutput,
      input.source.input
    ) ?? input.source.input;
  const baselineText =
    firstNonEmptyString(input.source.baselineOutput, input.source.expectedOutput, input.source.input) ??
    input.source.input;
  const candidateText =
    firstNonEmptyString(input.source.candidateInput, input.source.input, input.source.expectedOutput) ??
    input.source.input;
  const baselineSimilarity = computeTokenOverlapScore(referenceText, baselineText);
  const candidateSimilarity = computeTokenOverlapScore(referenceText, candidateText);
  const baselineScore = clampScore(
    35 + baselineSimilarity * 55 + computeStableJitter(`${input.replayJob.baselineId}:${input.caseId}:baseline:${input.metric}`)
  );
  const candidateScore = clampScore(
    35 +
      candidateSimilarity * 55 +
      computeStableJitter(
        `${input.replayJob.baselineId}:${input.caseId}:${candidateLabel}:${input.metric}`
      )
  );
  const delta = candidateScore - baselineScore;
  const verdict =
    delta >= 3 ? "improved" : delta <= -3 ? "regressed" : "unchanged";
  const detailParts = [
    `metric=${input.metric}`,
    `reference_hash=${sha256Hex(referenceText).slice(0, 10)}`,
    `candidate=${candidateLabel}`,
    firstNonEmptyString(input.source.metadata?.sourceType)
      ? `sourceType=${firstNonEmptyString(input.source.metadata?.sourceType)}`
      : undefined,
    input.source.metadata?.synthetic === true ? "synthetic=true" : undefined,
  ].filter(Boolean);

  return {
    caseId: input.caseId,
    metric: input.metric,
    baselineScore,
    candidateScore,
    delta,
    verdict,
    detail: detailParts.join(" "),
  } as const;
}

async function persistReplayArtifacts(input: {
  tenantId: string;
  replayJob: ReplayJob;
  summary: Record<string, unknown>;
  diff: Record<string, unknown>;
  cases: Record<string, unknown>;
}) {
  const artifacts = [
    {
      artifactType: "summary" as const,
      description: "Replay run summary payload.",
      payload: input.summary,
    },
    {
      artifactType: "diff" as const,
      description: "Replay run diff payload.",
      payload: input.diff,
    },
    {
      artifactType: "cases" as const,
      description: "Replay dataset cases snapshot.",
      payload: input.cases,
    },
  ];

  const items = [];
  for (const artifact of artifacts) {
    const content = new TextEncoder().encode(JSON.stringify(artifact.payload));
    const stored = await storeReplayArtifact({
      tenantId: input.tenantId,
      datasetId: input.replayJob.baselineId,
      runId: input.replayJob.id,
      artifactType: artifact.artifactType,
      content,
    });
    items.push({
      artifactType: artifact.artifactType,
      name: `${artifact.artifactType}.json`,
      description: artifact.description,
      contentType: "application/json",
      byteSize: stored.byteSize,
      checksum: stored.checksum,
      storageBackend: stored.storageBackend,
      storageKey: stored.storageKey,
      metadata: stored.metadata,
    });
  }

  await repository.upsertReplayArtifacts(input.tenantId, input.replayJob.id, items);
}

function toReplayExecutionErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    const message = error.message.trim();
    return message.length > 0 ? message : "回放任务执行失败。";
  }
  if (typeof error === "string") {
    const message = error.trim();
    return message.length > 0 ? message : "回放任务执行失败。";
  }
  return "回放任务执行失败。";
}

async function defaultReplayJobExecutionHandler(input: {
  tenantId: string;
  replayJob: ReplayJob;
}): Promise<ReplayJobExecutionResult> {
  const parameters = normalizeRecord(input.replayJob.parameters);
  const summary = normalizeRecord(input.replayJob.summary);
  const metric = toQualityMetric(parameters.metric ?? summary.metric);
  const requestedExecutionSource = toReplayExecutionSource(parameters.executionSource);
  const sampleLimitRaw = toNumber(parameters.sampleLimit, Number.NaN);
  const sampleLimit =
    Number.isInteger(sampleLimitRaw) && sampleLimitRaw > 0
      ? sampleLimitRaw
      : Number.MAX_SAFE_INTEGER;
  const persistedCases = await repository.listReplayDatasetCases(
    input.tenantId,
    input.replayJob.baselineId,
    { limit: sampleLimit }
  );
  const selectedCases =
    requestedExecutionSource === "synthetic"
      ? Array.from({ length: resolveReplayTotalCases(input.replayJob) }, (_, index) =>
          buildSyntheticReplayCase(input.replayJob, index)
        )
      : persistedCases.slice(0, sampleLimit).map((item) => ({
          caseId: item.caseId,
          input: item.input,
          expectedOutput: item.expectedOutput,
          baselineOutput: item.baselineOutput,
          candidateInput: item.candidateInput,
          metadata: item.metadata,
        }));
  if (selectedCases.length === 0) {
    const emptySummary = buildReplaySummaryPayload(
      {
        ...summary,
        metric,
        totalCases: 0,
        processedCases: 0,
        improvedCases: 0,
        regressedCases: 0,
        unchangedCases: 0,
        executionSource: requestedExecutionSource ?? "dataset_cases",
        materializedCaseCount: 0,
        sourceSummary: {},
      },
      {
        metric,
        totalCases: 0,
        processedCases: 0,
      }
    );
    return {
      status: "failed",
      summary: emptySummary,
      diff: {
        items: [],
        metric,
        datasetId: input.replayJob.baselineId,
        runId: input.replayJob.id,
      },
      artifacts: [
        {
          artifactType: "summary",
          description: "Replay run summary payload.",
          payload: emptySummary,
        },
      ],
      error: "回放数据集缺少可执行样本，请先录入样本或从历史会话物化样本。",
    };
  }
  const diffItems = selectedCases.map((item) =>
    evaluateReplayCase({
      replayJob: input.replayJob,
      caseId: item.caseId,
      metric,
      source: item,
    })
  );
  const summaryCounts = summarizeDiffItems(diffItems);
  const totalCases = selectedCases.length;
  const sourceSummary = summarizeReplayCaseSources(selectedCases);
  const executionSource = inferReplayExecutionSource({
    requested: requestedExecutionSource,
    selectedCases,
  });
  const completedSummary = buildReplaySummaryPayload(
    {
      ...summary,
      metric,
      totalCases,
      processedCases: summaryCounts.processedCases,
      improvedCases: summaryCounts.improvedCases,
      regressedCases: summaryCounts.regressedCases,
      unchangedCases: summaryCounts.unchangedCases,
      executionSource,
      materializedCaseCount: totalCases,
      sourceSummary,
    },
    {
      metric,
      totalCases,
      processedCases: summaryCounts.processedCases,
    }
  );

  return {
    status: "completed",
    summary: completedSummary,
    diff: {
      items: diffItems,
      metric,
      datasetId: input.replayJob.baselineId,
      runId: input.replayJob.id,
    },
    artifacts: [
      {
        artifactType: "summary",
        description: "Replay run summary payload.",
        payload: completedSummary,
      },
      {
        artifactType: "diff",
        description: "Replay run diff payload.",
        payload: {
          items: diffItems,
          metric,
          datasetId: input.replayJob.baselineId,
          runId: input.replayJob.id,
        },
      },
      {
        artifactType: "cases",
        description: "Replay dataset cases snapshot.",
        payload: {
          datasetId: input.replayJob.baselineId,
          runId: input.replayJob.id,
          total: selectedCases.length,
          sourceSummary,
          items: selectedCases,
        },
      },
    ],
  };
}

async function runReplayJobExecutionTask(task: ReplayJobExecutionTask): Promise<void> {
  const pendingJob = await repository.getReplayJobById(task.tenantId, task.replayJobId);
  if (!pendingJob || pendingJob.status !== "pending") {
    return;
  }

  const runningJob = await repository.updateReplayJob(task.tenantId, task.replayJobId, {
    fromStatuses: ["pending"],
    status: "running",
    startedAt: new Date().toISOString(),
    finishedAt: null,
    error: null,
  });
  if (!runningJob) {
    return;
  }

  try {
    const execution = await replayJobExecutionHandler({
      tenantId: task.tenantId,
      replayJob: runningJob,
    });
    const terminalStatus = execution.status ?? "completed";
    const finishedAt = new Date().toISOString();
    if (terminalStatus === "completed") {
      const metric = toQualityMetric(
        normalizeRecord(execution.summary).metric ??
          normalizeRecord(runningJob.summary).metric ??
          normalizeRecord(runningJob.parameters).metric
      );
      const diffPayload = normalizeRecord(execution.diff);
      const diffItems = Array.isArray(diffPayload.items)
        ? (diffPayload.items.filter(
            (item) => item && typeof item === "object" && !Array.isArray(item)
          ) as Array<{
            caseId: string;
            metric: QualityMetric;
            baselineScore: number;
            candidateScore: number;
            delta: number;
            verdict: "improved" | "regressed" | "unchanged";
            detail?: string;
          }>)
        : [];
      const summary = buildReplaySummaryPayload(
        {
          ...normalizeRecord(runningJob.summary),
          ...normalizeRecord(execution.summary),
        },
        {
          metric,
          totalCases: Math.max(0, toInteger(normalizeRecord(execution.summary).totalCases, 0)),
          processedCases: Math.max(0, toInteger(normalizeRecord(execution.summary).processedCases, 0)),
        }
      );
      const casesArtifactPayload =
        execution.artifacts?.find((item) => item.artifactType === "cases")?.payload ?? {
          datasetId: runningJob.baselineId,
          runId: runningJob.id,
          total: 0,
          items: [],
        };
      const executionSource =
        toReplayExecutionSource(summary.executionSource) ??
        inferReplayExecutionSource({
          selectedCases: Array.isArray(casesArtifactPayload.items)
            ? (casesArtifactPayload.items.filter(
                (item) => item && typeof item === "object" && !Array.isArray(item)
              ) as Array<{ metadata?: Record<string, unknown> }>)
            : [],
        });
      const sourceSummary =
        Object.keys(normalizeReplaySourceSummary(summary.sourceSummary)).length > 0
          ? normalizeReplaySourceSummary(summary.sourceSummary)
          : summarizeReplayCaseSources(
              Array.isArray(casesArtifactPayload.items)
                ? (casesArtifactPayload.items.filter(
                    (item) => item && typeof item === "object" && !Array.isArray(item)
                  ) as Array<{ metadata?: Record<string, unknown> }>)
                : []
            );
      const executedCaseCount = Math.max(
        Array.isArray(casesArtifactPayload.items)
          ? casesArtifactPayload.items.filter(
              (item) => item && typeof item === "object" && !Array.isArray(item)
            ).length
          : 0,
        0,
        toInteger(casesArtifactPayload.total, 0),
        diffItems.length,
        toInteger(normalizeRecord(execution.summary).totalCases, 0)
      );
      const processedCaseCount =
        Math.max(
          diffItems.length,
          toInteger(normalizeRecord(execution.summary).processedCases, 0),
          Math.max(0, toInteger(summary.processedCases, 0))
        ) || executedCaseCount;
      const summaryWithDigest = buildReplaySummaryPayload(
        {
          ...summary,
          totalCases: executedCaseCount,
          processedCases: processedCaseCount,
          executionSource,
          materializedCaseCount:
            Math.max(0, toInteger(summary.materializedCaseCount, 0)) ||
            Object.values(sourceSummary).reduce(
              (sum: number, value) => sum + Math.max(0, toInteger(value, 0)),
              0
            ),
          sourceSummary,
        },
        {
          metric,
          totalCases: executedCaseCount,
          processedCases: processedCaseCount,
        }
      );
      const digest = buildReplayResultDigest({
        tenantId: task.tenantId,
        replayJob: runningJob,
        summary: summaryWithDigest,
        diffItems,
        sourceSummary,
        executionSource,
        finishedAt,
      });
      const regressedCases = Math.max(0, toInteger(summaryWithDigest.regressedCases, 0));
      const finalSummary: Record<string, unknown> = {
        ...summaryWithDigest,
        digest,
      };
      await persistReplayArtifacts({
        tenantId: task.tenantId,
        replayJob: runningJob,
        summary: finalSummary,
        diff: diffPayload,
        cases: normalizeRecord(casesArtifactPayload),
      });

      await repository.updateReplayJob(task.tenantId, task.replayJobId, {
        fromStatuses: ["running"],
        status: "completed",
        summary: finalSummary,
        diff: diffPayload,
        error: null,
        finishedAt,
      });
      await appendAuditLogSafely({
        tenantId: task.tenantId,
        eventId: `cp:replay-run-completed:${runningJob.id}:${finishedAt}`,
        action: "control_plane.replay.run_completed",
        level: regressedCases > 0 ? "warning" : "info",
        detail: `Replay run ${runningJob.id} completed for dataset ${runningJob.baselineId}.`,
        metadata: digest,
      });
      if (regressedCases > 0) {
        await appendAuditLogSafely({
          tenantId: task.tenantId,
          eventId: `cp:replay-run-regression:${runningJob.id}:${finishedAt}`,
          action: "control_plane.replay.run_regression_detected",
          level: "warning",
          detail: `Replay run ${runningJob.id} detected regressions.`,
          metadata: digest,
        });
      }
      return;
    }

    const errorMessage =
      firstNonEmptyString(execution.error) ??
      (terminalStatus === "cancelled" ? "回放任务已取消。" : "回放任务执行失败。");
    await repository.updateReplayJob(task.tenantId, task.replayJobId, {
      fromStatuses: ["running"],
      status: terminalStatus,
      error: terminalStatus === "cancelled" ? null : errorMessage,
      finishedAt,
    });
    await appendAuditLogSafely({
      tenantId: task.tenantId,
      eventId: `cp:replay-run-terminal:${runningJob.id}:${finishedAt}`,
      action:
        terminalStatus === "cancelled"
          ? "control_plane.replay.run_cancelled"
          : "control_plane.replay.run_failed",
      level: terminalStatus === "cancelled" ? "info" : "error",
      detail: `Replay run ${runningJob.id} finished with status ${terminalStatus}.`,
      metadata: {
        replayRunId: runningJob.id,
        replayJobId: runningJob.id,
        datasetId: runningJob.baselineId,
        status: terminalStatus,
        error: terminalStatus === "cancelled" ? null : errorMessage,
      },
    });
  } catch (error) {
    await repository.updateReplayJob(task.tenantId, task.replayJobId, {
      fromStatuses: ["running"],
      status: "failed",
      error: toReplayExecutionErrorMessage(error),
      finishedAt: new Date().toISOString(),
    });
    const failedAt = new Date().toISOString();
    await appendAuditLogSafely({
      tenantId: task.tenantId,
      eventId: `cp:replay-run-failed:${task.replayJobId}:${failedAt}`,
      action: "control_plane.replay.run_failed",
      level: "error",
      detail: `Replay run ${task.replayJobId} execution failed.`,
      metadata: {
        replayRunId: task.replayJobId,
        replayJobId: task.replayJobId,
        error: toReplayExecutionErrorMessage(error),
      },
    });
  }
}

async function drainReplayJobExecutionQueue(): Promise<void> {
  if (replayJobExecutionDrainRunning) {
    return;
  }
  replayJobExecutionDrainRunning = true;

  try {
    while (replayJobExecutionQueue.length > 0) {
      const task = replayJobExecutionQueue.shift();
      if (!task) {
        continue;
      }
      try {
        await runReplayJobExecutionTask(task);
      } catch (error) {
        console.warn("[control-plane] replay worker 执行失败。", error);
      }
    }
  } finally {
    replayJobExecutionDrainRunning = false;
    if (replayJobExecutionQueue.length > 0) {
      scheduleReplayJobExecutionDrain();
    }
  }
}

function scheduleReplayJobExecutionDrain(): void {
  if (replayJobExecutionDrainScheduled) {
    return;
  }
  replayJobExecutionDrainScheduled = true;
  setTimeout(() => {
    replayJobExecutionDrainScheduled = false;
    void drainReplayJobExecutionQueue();
  }, 0);
}

export function enqueueReplayJobExecution(tenantId: string, replayJobId: string): void {
  const normalizedTenantId = firstNonEmptyString(tenantId);
  const normalizedReplayJobId = firstNonEmptyString(replayJobId);
  if (!normalizedTenantId || !normalizedReplayJobId) {
    return;
  }
  replayJobExecutionQueue.push({
    tenantId: normalizedTenantId,
    replayJobId: normalizedReplayJobId,
  });
  scheduleReplayJobExecutionDrain();
}

export async function flushReplayJobExecutionQueueForTests(): Promise<void> {
  await drainReplayJobExecutionQueue();
}

export function setReplayJobExecutionHandlerForTests(
  handler?: ReplayJobExecutionHandler
): void {
  replayJobExecutionHandler = handler ?? defaultReplayJobExecutionHandler;
}

export function resetReplayJobExecutionWorkerForTests(): void {
  replayJobExecutionHandler = defaultReplayJobExecutionHandler;
  replayJobExecutionQueue.length = 0;
  replayJobExecutionDrainScheduled = false;
  replayJobExecutionDrainRunning = false;
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
      (error.message.startsWith("replay_baseline_name_already_exists:") ||
        error.message.startsWith("replay_dataset_name_already_exists:"))
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
  const model = firstNonEmptyString(c.req.query("model"))?.toLowerCase();
  const datasetId = firstNonEmptyString(c.req.query("datasetId"));
  const fromRaw = firstNonEmptyString(c.req.query("from"));
  const toRaw = firstNonEmptyString(c.req.query("to"));
  const fromTs = fromRaw ? Date.parse(fromRaw) : Number.NaN;
  const toTs = toRaw ? Date.parse(toRaw) : Number.NaN;
  if (fromRaw && !Number.isFinite(fromTs)) {
    return c.json({ message: "from 必须是 ISO 日期字符串。" }, 400);
  }
  if (toRaw && !Number.isFinite(toTs)) {
    return c.json({ message: "to 必须是 ISO 日期字符串。" }, 400);
  }
  if (Number.isFinite(fromTs) && Number.isFinite(toTs) && fromTs > toTs) {
    return c.json({ message: "from 不能晚于 to。" }, 400);
  }

  const baselines = await repository.listReplayBaselines(auth.tenantId, {
    keyword,
    limit: 500,
  });
  const filtered = baselines
    .map(mapReplayBaseline)
    .filter((item) => {
      if (model) {
        const itemModel = firstNonEmptyString(item.model)?.toLowerCase() ?? "";
        if (itemModel !== model) {
          return false;
        }
      }
      if (datasetId && item.datasetId !== datasetId) {
        return false;
      }
      if (Number.isFinite(fromTs) || Number.isFinite(toTs)) {
        const createdAtTs = Date.parse(item.createdAt);
        if (!Number.isFinite(createdAtTs)) {
          return false;
        }
        if (Number.isFinite(fromTs) && createdAtTs < fromTs) {
          return false;
        }
        if (Number.isFinite(toTs) && createdAtTs > toTs) {
          return false;
        }
      }
      return true;
    });
  const items = filtered.slice(0, limit);

  return c.json({
    items,
    total: filtered.length,
    filters: {
      keyword,
      model,
      datasetId,
      from: fromRaw,
      to: toRaw,
      limit,
    },
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
  const summary = buildReplaySummaryPayload(
    {
      metric,
      totalCases: sampleLimit,
      processedCases: 0,
      improvedCases: 0,
      regressedCases: 0,
      unchangedCases: 0,
      executionSource: "synthetic",
      materializedCaseCount: sampleLimit,
      sourceSummary: {
        synthetic: sampleLimit,
      },
    },
    {
      metric,
      totalCases: sampleLimit,
      processedCases: 0,
    }
  );
  const now = new Date().toISOString();

  try {
    const replayJob = await repository.createReplayJob(auth.tenantId, {
      baselineId: validation.data.baselineId,
      status: "pending",
      parameters: {
        candidateLabel: validation.data.candidateLabel,
        from: validation.data.from,
        to: validation.data.to,
        sampleLimit,
        executionSource: "synthetic",
        ...(validation.data.metadata ?? {}),
      },
      summary,
      diff: {},
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

    enqueueReplayJobExecution(auth.tenantId, replayJob.id);
    return c.json(mapReplayJob(replayJob), 201);
  } catch (error) {
    if (
      error instanceof Error &&
      (error.message.startsWith("replay_baseline_not_found:") ||
        error.message.startsWith("replay_dataset_not_found:"))
    ) {
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
  const candidateLabel = firstNonEmptyString(c.req.query("candidateLabel"))?.toLowerCase();
  const metric = firstNonEmptyString(c.req.query("metric"))?.toLowerCase();
  if (metric && !QUALITY_METRIC_SET.has(metric)) {
    return c.json(
      { message: "metric 必须是 accuracy/consistency/groundedness/safety/latency 之一。" },
      400
    );
  }
  const fromRaw = firstNonEmptyString(c.req.query("from"));
  const toRaw = firstNonEmptyString(c.req.query("to"));
  const fromTs = fromRaw ? Date.parse(fromRaw) : Number.NaN;
  const toTs = toRaw ? Date.parse(toRaw) : Number.NaN;
  if (fromRaw && !Number.isFinite(fromTs)) {
    return c.json({ message: "from 必须是 ISO 日期字符串。" }, 400);
  }
  if (toRaw && !Number.isFinite(toTs)) {
    return c.json({ message: "to 必须是 ISO 日期字符串。" }, 400);
  }
  if (Number.isFinite(fromTs) && Number.isFinite(toTs) && fromTs > toTs) {
    return c.json({ message: "from 不能晚于 to。" }, 400);
  }
  const needExtendedFiltering =
    Boolean(candidateLabel) || Boolean(metric) || Number.isFinite(fromTs) || Number.isFinite(toTs);
  const fetchLimit = needExtendedFiltering ? 500 : limit;

  const jobs = await repository.listReplayJobs(auth.tenantId, {
    baselineId: firstNonEmptyString(c.req.query("baselineId")),
    status: toRepositoryReplayStatus(statusQuery),
    limit: fetchLimit,
  });

  const filtered = jobs
    .map(mapReplayJob)
    .filter((item) => {
      if (candidateLabel) {
        const normalized = firstNonEmptyString(item.candidateLabel)?.toLowerCase() ?? "";
        if (!normalized.includes(candidateLabel)) {
          return false;
        }
      }
      if (metric) {
        const summaryMetric =
          firstNonEmptyString(item.summary?.metric)?.toLowerCase() ??
          firstNonEmptyString(item.diffs[0]?.metric)?.toLowerCase();
        if (summaryMetric !== metric) {
          return false;
        }
      }
      if (Number.isFinite(fromTs) || Number.isFinite(toTs)) {
        const createdAtTs = Date.parse(item.createdAt);
        if (!Number.isFinite(createdAtTs)) {
          return false;
        }
        if (Number.isFinite(fromTs) && createdAtTs < fromTs) {
          return false;
        }
        if (Number.isFinite(toTs) && createdAtTs > toTs) {
          return false;
        }
      }
      return true;
    });
  const items = filtered.slice(0, limit);
  return c.json({
    items,
    total: filtered.length,
    filters: {
      baselineId: firstNonEmptyString(c.req.query("baselineId")),
      status: statusQuery,
      candidateLabel,
      metric,
      from: fromRaw,
      to: toRaw,
      limit,
    },
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
