import { Hono, type Context } from "hono";
import {
  validateMcpToolPolicyUpsertInput,
  validateQualityEventCreateInput,
  validateQualityScorecardUpsertInput,
  validateReplayDatasetCasesReplaceInput,
  validateReplayDatasetCreateInput,
  validateReplayDatasetMaterializeInput,
  validateReplayDatasetVersionCreateInput,
  validateReplayDatasetVersionPromoteInput,
  validateReplayExperimentCreateInput,
  validateReplayExperimentUpdateInput,
  validateReplayRunCreateInput,
  validateReplicationJobApproveInput,
  validateReplicationJobCancelInput,
  validateReplicationJobCreateInput,
  validateReplicationJobListInput,
  validateResidencyArchiveRegionPolicyUpsertInput,
  validateResidencyKmsKeyMappingUpsertInput,
  validateTenantResidencyPolicyUpsertInput,
} from "../contracts";
import type {
  AppendAuditLogInput,
  QualityAdviceActionType,
  QualityAdviceExecution,
  QualityAdviceExecutionStatus,
  QualityAdviceSeverity,
  QualityDailyMetric,
  QualityExternalMetricGroup,
  QualityScorecard,
  ReplayArtifact,
  ReplayBaselineVersion,
  ReplayDataset,
  ReplayExperiment,
  ReplayExperimentStatus,
  ReplayRun,
} from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import { enqueueReplayJobExecution } from "./replay";
import { readReplayArtifactContent } from "./replay-artifact-store";
import { executeQualityAdviceExecution } from "./quality-advice-execution";
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
const QUALITY_AUTOMATION_TOOL_ID = "quality.replay.advice.execute";
const QUALITY_AUTOMATION_DEFAULT_POLICY = {
  riskLevel: "medium" as const,
  decision: "allow" as const,
  reason: "默认允许对失败评估或回放回退自动执行治理建议。",
};
const QUALITY_AUTOMATION_SCORE_THRESHOLD = 0.8;
const QUALITY_AUTOMATION_DEFAULT_ACTION_TYPE = "scorecard_adjustment" as const;
const QUALITY_FORECAST_MODEL_VERSION_SET = new Set<QualityForecastModelVersion>([
  "quality-heuristic-v2",
  "quality-timeseries-v1",
]);

type QualityMetric = "accuracy" | "consistency" | "groundedness" | "safety" | "latency";
type QualityExternalGroupBy = "provider" | "repo" | "workflow" | "runId";
type ReplayExperimentRecord = ReplayExperiment;
type QualityAdviceExecutionRecord = QualityAdviceExecution;
type QualityTrendDirection = "up" | "down" | "flat";
type QualityAdviceAction = "scorecard_adjustment" | "replay_experiment";
type QualityForecastModelVersion = "quality-heuristic-v2" | "quality-timeseries-v1";

const replayExperimentSidecarStore = new Map<
  string,
  {
    baselineVersionId: string | null;
  }
>();

function buildReplayExperimentSidecarKey(tenantId: string, experimentId: string): string {
  return `${tenantId}:${experimentId}`;
}

function rememberReplayExperimentBaselineVersionId(
  tenantId: string,
  experimentId: string,
  baselineVersionId: string | undefined,
): void {
  const key = buildReplayExperimentSidecarKey(tenantId, experimentId);
  if (!baselineVersionId) {
    replayExperimentSidecarStore.delete(key);
    return;
  }
  replayExperimentSidecarStore.set(key, {
    baselineVersionId,
  });
}

function readReplayExperimentBaselineVersionId(
  tenantId: string,
  experimentId: string,
): string | undefined {
  return firstNonEmptyString(
    replayExperimentSidecarStore.get(buildReplayExperimentSidecarKey(tenantId, experimentId))
      ?.baselineVersionId,
  );
}

interface QualityAutomationStrategyRule {
  ruleId: string;
  metric?: QualityMetric;
  severity?: QualityAdviceSeverity;
  trendDirection?: QualityTrendDirection;
  provider?: string;
  workflow?: string;
  projectPattern?: string;
  minSampleCount?: number;
  minPassRate?: number;
  minConfidence?: number;
  regressionProbabilityAtLeast?: number;
  replayRegressionAtLeast?: number;
  actionType: QualityAdviceAction;
  requiresApproval?: boolean;
  cooldownMinutes?: number;
  reason?: string;
}

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

function slugifyIdentifier(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

function buildQualityAdviceId(input: {
  project: string;
  provider?: string;
  workflow?: string;
  from?: string | null;
  to?: string | null;
}): string {
  return [
    "advice",
    slugifyIdentifier(input.project),
    slugifyIdentifier(input.provider ?? "all"),
    slugifyIdentifier(input.workflow ?? "all"),
    slugifyIdentifier(input.from ?? "na"),
    slugifyIdentifier(input.to ?? "na"),
  ].join(":");
}

function deriveUtcDayRange(value: string | undefined): { from?: string; to?: string } {
  if (!value) {
    return {};
  }
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) {
    return {};
  }
  const start = new Date(timestamp);
  start.setUTCHours(0, 0, 0, 0);
  const end = new Date(timestamp);
  end.setUTCHours(23, 59, 59, 999);
  return {
    from: start.toISOString(),
    to: end.toISOString(),
  };
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
  if (typeof value === "boolean") {
    return value;
  }
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

function toQualityForecastModelVersion(value: unknown): QualityForecastModelVersion | undefined {
  const normalized = firstNonEmptyString(value);
  if (!normalized) {
    return undefined;
  }
  if (!QUALITY_FORECAST_MODEL_VERSION_SET.has(normalized as QualityForecastModelVersion)) {
    return undefined;
  }
  return normalized as QualityForecastModelVersion;
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

function resolveReplayDatasetCurrentVersionId(dataset: ReplayDataset): string | undefined {
  const metadata = normalizeRecord(dataset.metadata);
  return firstNonEmptyString(
    metadata.currentVersionId,
    metadata.current_version_id,
  );
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

async function resolveQualityAutomationPolicy(tenantId: string) {
  const policy = await repository.getMcpToolPolicyByToolId(
    tenantId,
    QUALITY_AUTOMATION_TOOL_ID,
  );
  if (policy) {
    return policy;
  }
  return {
    tenantId,
    toolId: QUALITY_AUTOMATION_TOOL_ID,
    ...QUALITY_AUTOMATION_DEFAULT_POLICY,
    updatedAt: new Date().toISOString(),
  };
}

function mapQualityAutomationPolicy(policy: {
  tenantId: string;
  toolId: string;
  riskLevel: "low" | "medium" | "high";
  decision: "allow" | "deny" | "require_approval";
  reason?: string;
  metadata?: Record<string, unknown>;
  updatedAt: string;
}) {
  const metadata = normalizeRecord(policy.metadata);
  const evaluationScoreThreshold = Number(
    Math.max(
      0,
      Math.min(
        100,
        toNumber(
          metadata.evaluationScoreThreshold,
          fromRepositoryScore(QUALITY_AUTOMATION_SCORE_THRESHOLD),
        ),
      ),
    ).toFixed(4),
  );
  const triggerOnEvaluationFailure = toBoolean(
    metadata.triggerOnEvaluationFailure,
    true,
  );
  const triggerOnReplayRegression = toBoolean(
    metadata.triggerOnReplayRegression,
    true,
  );
  const defaultActionType =
    metadata.defaultActionType === "replay_experiment"
      ? "replay_experiment"
      : QUALITY_AUTOMATION_DEFAULT_ACTION_TYPE;
  const strategyMatrix = parseQualityAutomationStrategyMatrix(
    Array.isArray(metadata.strategyMatrix) ? metadata.strategyMatrix : undefined,
  ).map((rule) => ({
    id: rule.ruleId,
    ruleId: rule.ruleId,
    metric: rule.metric,
    severity: rule.severity,
    trendDirection: rule.trendDirection,
    provider: rule.provider,
    workflow: rule.workflow,
    projectPattern: rule.projectPattern,
    minSampleCount: rule.minSampleCount,
    minPassRate: rule.minPassRate,
    minConfidence: rule.minConfidence,
    regressionProbabilityAtLeast: rule.regressionProbabilityAtLeast,
    replayRegressionAtLeast: rule.replayRegressionAtLeast,
    actionType: rule.actionType,
    requiresApproval: rule.requiresApproval ?? false,
    cooldownMinutes: rule.cooldownMinutes,
    reason: rule.reason,
  }));
  return {
    tenantId: policy.tenantId,
    toolId: policy.toolId,
    scope: "quality_replay_advice" as const,
    riskLevel: policy.riskLevel,
    decision: policy.decision,
    reason: policy.reason,
    evaluationScoreThreshold,
    triggerOnEvaluationFailure,
    triggerOnReplayRegression,
    defaultActionType,
    strategyMatrix,
    modelVersion:
      firstNonEmptyString(metadata.modelVersion) ?? "quality-heuristic-v2",
    updatedAt: policy.updatedAt,
  };
}

function normalizeQualityMetric(value: unknown): QualityMetric | undefined {
  const normalized = firstNonEmptyString(value)?.toLowerCase();
  if (!normalized || !QUALITY_METRIC_SET.has(normalized)) {
    return undefined;
  }
  return normalized as QualityMetric;
}

function normalizeQualitySeverity(value: unknown): QualityAdviceSeverity | undefined {
  const normalized = firstNonEmptyString(value)?.toLowerCase();
  if (normalized === "critical" || normalized === "warn" || normalized === "info") {
    return normalized;
  }
  return undefined;
}

function normalizeQualityTrendDirection(value: unknown): QualityTrendDirection | undefined {
  const normalized = firstNonEmptyString(value)?.toLowerCase();
  if (normalized === "up" || normalized === "down" || normalized === "flat") {
    return normalized;
  }
  return undefined;
}

function normalizeQualityAdviceAction(value: unknown): QualityAdviceAction | undefined {
  const normalized = firstNonEmptyString(value);
  if (normalized === "scorecard_adjustment" || normalized === "replay_experiment") {
    return normalized;
  }
  return undefined;
}

function parseQualityAutomationStrategyMatrix(
  value: unknown[] | undefined,
): QualityAutomationStrategyRule[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const rules: QualityAutomationStrategyRule[] = [];
  for (const [index, item] of value.entries()) {
    const record = normalizeRecord(item);
    const actionType = normalizeQualityAdviceAction(record.actionType);
    if (!actionType) {
      continue;
    }
    const minConfidence =
      record.minConfidence === undefined
        ? undefined
        : Number.isFinite(Number(record.minConfidence))
          ? Number(Number(record.minConfidence).toFixed(4))
          : undefined;
    const minSampleCount =
      record.minSampleCount === undefined
        ? undefined
        : Math.max(0, toInteger(record.minSampleCount, 0));
    const minPassRate =
      record.minPassRate === undefined
        ? undefined
        : Number.isFinite(Number(record.minPassRate))
          ? Number(Number(record.minPassRate).toFixed(6))
          : undefined;
    const regressionProbabilityAtLeast =
      record.regressionProbabilityAtLeast === undefined
        ? undefined
        : Number.isFinite(Number(record.regressionProbabilityAtLeast))
          ? Number(Number(record.regressionProbabilityAtLeast).toFixed(4))
          : undefined;
    const replayRegressionAtLeast =
      record.replayRegressionAtLeast === undefined
        ? undefined
        : Math.max(0, toInteger(record.replayRegressionAtLeast, 0));
    const cooldownMinutes =
      record.cooldownMinutes === undefined
        ? undefined
        : Math.max(0, toInteger(record.cooldownMinutes, 0));
    rules.push({
      ruleId: firstNonEmptyString(record.ruleId, record.id) ?? `rule-${index + 1}`,
      metric: normalizeQualityMetric(record.metric),
      severity: normalizeQualitySeverity(record.severity),
      trendDirection: normalizeQualityTrendDirection(record.trendDirection),
      provider: firstNonEmptyString(record.provider)?.toLowerCase(),
      workflow: firstNonEmptyString(record.workflow),
      projectPattern: firstNonEmptyString(record.projectPattern),
      minSampleCount,
      minPassRate,
      minConfidence,
      regressionProbabilityAtLeast,
      replayRegressionAtLeast,
      actionType,
      requiresApproval:
        typeof record.requiresApproval === "boolean" ? record.requiresApproval : undefined,
      cooldownMinutes,
      reason: firstNonEmptyString(record.reason),
    });
  }
  return rules;
}

function validateQualityAutomationStrategyMatrixNumber(
  value: unknown,
  options: {
    field: string;
    integer?: boolean;
    min?: number;
    max?: number;
  },
): string | null {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return `${options.field} 必须是数字。`;
  }
  if (options.integer && !Number.isInteger(numericValue)) {
    return `${options.field} 必须是非负整数。`;
  }
  if (options.min !== undefined && numericValue < options.min) {
    return `${options.field} 必须大于等于 ${options.min}。`;
  }
  if (options.max !== undefined && numericValue > options.max) {
    return `${options.field} 必须小于等于 ${options.max}。`;
  }
  return null;
}

function validateQualityAutomationStrategyMatrix(
  value: unknown,
): { success: true; data: QualityAutomationStrategyRule[] } | { success: false; error: string } {
  if (value === undefined) {
    return { success: true, data: [] };
  }
  if (!Array.isArray(value)) {
    return { success: false, error: "strategyMatrix 必须是数组。" };
  }
  for (const [index, item] of value.entries()) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      return {
        success: false,
        error: `strategyMatrix[${index}] 必须是对象。`,
      };
    }
    const record = normalizeRecord(item);
    if (!normalizeQualityAdviceAction(record.actionType)) {
      return {
        success: false,
        error: `strategyMatrix[${index}] 的 actionType 非法。`,
      };
    }
    if (record.metric !== undefined && !normalizeQualityMetric(record.metric)) {
      return {
        success: false,
        error: `strategyMatrix[${index}] 的 metric 非法。`,
      };
    }
    if (record.severity !== undefined && !normalizeQualitySeverity(record.severity)) {
      return {
        success: false,
        error: `strategyMatrix[${index}] 的 severity 非法。`,
      };
    }
    if (
      record.trendDirection !== undefined &&
      !normalizeQualityTrendDirection(record.trendDirection)
    ) {
      return {
        success: false,
        error: `strategyMatrix[${index}] 的 trendDirection 非法。`,
      };
    }
    if (
      record.requiresApproval !== undefined &&
      typeof record.requiresApproval !== "boolean"
    ) {
      return {
        success: false,
        error: `strategyMatrix[${index}] 的 requiresApproval 必须是布尔值。`,
      };
    }
    for (const field of ["provider", "workflow", "projectPattern", "reason"] as const) {
      const rawValue = record[field];
      if (rawValue !== undefined && !firstNonEmptyString(rawValue)) {
        return {
          success: false,
          error: `strategyMatrix[${index}] 的 ${field} 不能为空字符串。`,
        };
      }
    }
    const minSampleCountError = validateQualityAutomationStrategyMatrixNumber(
      record.minSampleCount,
      {
        field: `strategyMatrix[${index}].minSampleCount`,
        integer: true,
        min: 0,
      },
    );
    if (minSampleCountError) {
      return { success: false, error: minSampleCountError };
    }
    const minPassRateError = validateQualityAutomationStrategyMatrixNumber(
      record.minPassRate,
      {
        field: `strategyMatrix[${index}].minPassRate`,
        min: 0,
        max: 1,
      },
    );
    if (minPassRateError) {
      return { success: false, error: minPassRateError };
    }
    const minConfidenceError = validateQualityAutomationStrategyMatrixNumber(
      record.minConfidence,
      {
        field: `strategyMatrix[${index}].minConfidence`,
        min: 0,
        max: 1,
      },
    );
    if (minConfidenceError) {
      return { success: false, error: minConfidenceError };
    }
    const regressionProbabilityError = validateQualityAutomationStrategyMatrixNumber(
      record.regressionProbabilityAtLeast,
      {
        field: `strategyMatrix[${index}].regressionProbabilityAtLeast`,
        min: 0,
        max: 1,
      },
    );
    if (regressionProbabilityError) {
      return { success: false, error: regressionProbabilityError };
    }
    const replayRegressionError = validateQualityAutomationStrategyMatrixNumber(
      record.replayRegressionAtLeast,
      {
        field: `strategyMatrix[${index}].replayRegressionAtLeast`,
        integer: true,
        min: 0,
      },
    );
    if (replayRegressionError) {
      return { success: false, error: replayRegressionError };
    }
    const cooldownError = validateQualityAutomationStrategyMatrixNumber(
      record.cooldownMinutes,
      {
        field: `strategyMatrix[${index}].cooldownMinutes`,
        integer: true,
        min: 0,
      },
    );
    if (cooldownError) {
      return { success: false, error: cooldownError };
    }
  }
  const rules = parseQualityAutomationStrategyMatrix(value);
  if (rules.length !== value.length) {
    return {
      success: false,
      error:
        "strategyMatrix 项配置非法：actionType 必须合法，metric/severity/trendDirection/minConfidence 等字段格式必须正确。",
    };
  }
  return { success: true, data: rules };
}

function deriveForecastHorizonDays(from?: string | null, to?: string | null): number {
  if (!from || !to) {
    return 7;
  }
  const fromTime = Date.parse(from);
  const toTime = Date.parse(to);
  if (!Number.isFinite(fromTime) || !Number.isFinite(toTime) || toTime < fromTime) {
    return 7;
  }
  const days = Math.max(1, Math.round((toTime - fromTime) / 86_400_000) + 1);
  return Math.min(30, days);
}

function clampProbability(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Number(Math.max(0, Math.min(1, value)).toFixed(6));
}

function computeQualityRegressionProbability(input: {
  avgScore: number;
  passRate: number;
  projectedDelta: number;
  previousAverageScore?: number;
  previousPassRate?: number;
  totalEvents: number;
}): number {
  const scoreRisk = Math.max(0, (85 - input.avgScore) / 35);
  const passRisk = Math.max(0, (0.9 - input.passRate) / 0.9);
  const trendRisk = input.projectedDelta < 0 ? Math.min(1, Math.abs(input.projectedDelta) / 12) : 0;
  const previousScoreRisk =
    input.previousAverageScore !== undefined
      ? Math.max(0, (input.previousAverageScore - input.avgScore) / 20)
      : 0;
  const previousPassRisk =
    input.previousPassRate !== undefined
      ? Math.max(0, input.previousPassRate - input.passRate)
      : 0;
  const samplePenalty = input.totalEvents >= 10 ? 0 : (10 - input.totalEvents) / 25;
  return clampProbability(
    0.28 * scoreRisk +
      0.22 * passRisk +
      0.18 * trendRisk +
      0.18 * previousScoreRisk +
      0.1 * previousPassRisk +
      0.04 * samplePenalty,
  );
}

function buildQualityFeatureContributions(input: {
  avgScore: number;
  passRate: number;
  projectedDelta: number;
  previousAverageScore?: number;
  previousPassRate?: number;
}) {
  const scoreGap = Number(((input.avgScore - 80) / 20).toFixed(4));
  const passRateGap = Number(((input.passRate - 0.8) / 0.2).toFixed(4));
  const trend = Number((input.projectedDelta / 10).toFixed(4));
  const previousScoreGap =
    input.previousAverageScore !== undefined
      ? Number(((input.avgScore - input.previousAverageScore) / 20).toFixed(4))
      : 0;
  const previousPassGap =
    input.previousPassRate !== undefined
      ? Number(((input.passRate - input.previousPassRate) / 0.2).toFixed(4))
      : 0;
  return {
    scoreGap,
    passRateGap,
    trend,
    previousScoreGap,
    previousPassGap,
  };
}

function matchQualityProjectPattern(pattern: string, project: string): boolean {
  const escaped = pattern
    .replace(/[|\{}()[\]^$+?.]/g, "\$&")
    .replace(/\*/g, ".*");
  return new RegExp(`^${escaped}$`, "i").test(project);
}

function selectQualityAutomationStrategyRule(input: {
  policy: ReturnType<typeof mapQualityAutomationPolicy>;
  metric: QualityMetric;
  severity: QualityAdviceSeverity;
  trendDirection: QualityTrendDirection;
  provider?: string;
  workflow?: string;
  project?: string;
  sampleCount: number;
  passRate: number;
  confidence: number;
  regressionProbability: number;
  replayRegressionCount: number;
}) {
  return input.policy.strategyMatrix.find((rule) => {
    if (rule.metric && rule.metric !== input.metric) {
      return false;
    }
    if (rule.severity && rule.severity !== input.severity) {
      return false;
    }
    if (rule.trendDirection && rule.trendDirection !== input.trendDirection) {
      return false;
    }
    if (rule.provider && rule.provider !== (input.provider ?? "").toLowerCase()) {
      return false;
    }
    if (rule.workflow && rule.workflow !== (input.workflow ?? "")) {
      return false;
    }
    if (
      rule.projectPattern &&
      (!input.project || !matchQualityProjectPattern(rule.projectPattern, input.project))
    ) {
      return false;
    }
    if (typeof rule.minSampleCount === "number" && input.sampleCount < rule.minSampleCount) {
      return false;
    }
    if (typeof rule.minPassRate === "number" && input.passRate + Number.EPSILON < rule.minPassRate) {
      return false;
    }
    if (
      typeof rule.minConfidence === "number" &&
      input.confidence + Number.EPSILON < rule.minConfidence
    ) {
      return false;
    }
    if (
      typeof rule.regressionProbabilityAtLeast === "number" &&
      input.regressionProbability + Number.EPSILON < rule.regressionProbabilityAtLeast
    ) {
      return false;
    }
    if (
      typeof rule.replayRegressionAtLeast === "number" &&
      input.replayRegressionCount < rule.replayRegressionAtLeast
    ) {
      return false;
    }
    return true;
  });
}

function buildQualityStrategyMatrixSimulation(input: {
  policy: ReturnType<typeof mapQualityAutomationPolicy>;
  metric: QualityMetric;
  score: number;
  sampleCount?: number;
  provider?: string;
  workflow?: string;
  project?: string;
  confidence: number;
  trendDirection: QualityTrendDirection;
  replayRegressionCount: number;
  regressionProbability: number;
}) {
  const passRate = Math.max(0, Math.min(1, input.score / 100));
  const sampleCount = Math.max(0, toInteger(input.sampleCount, 0));
  const severity = deriveQualityAdviceSeverity(input.score, passRate);
  const matchedRule = selectQualityAutomationStrategyRule({
    policy: input.policy,
    metric: input.metric,
    severity,
    trendDirection: input.trendDirection,
    provider: input.provider,
    workflow: input.workflow,
    project: input.project,
    sampleCount,
    passRate,
    confidence: input.confidence,
    regressionProbability: input.regressionProbability,
    replayRegressionCount: input.replayRegressionCount,
  });
  const resolvedAction = matchedRule?.actionType ?? input.policy.defaultActionType ?? QUALITY_AUTOMATION_DEFAULT_ACTION_TYPE;
  const requiresApproval =
    matchedRule?.requiresApproval ?? (input.policy.decision === "require_approval");
  const triggered = Boolean(matchedRule);
  const reason = triggered ? "matched_strategy_matrix" : "fallback_default_action";
  return {
    triggered,
    reason,
    metric: input.metric,
    severity,
    confidence: input.confidence,
    trendDirection: input.trendDirection,
    regressionProbability: input.regressionProbability,
    replayRegressionCount: input.replayRegressionCount,
    matchedRuleId: matchedRule?.ruleId ?? null,
    matchedRule:
      matchedRule == null
        ? null
        : {
            id: matchedRule.ruleId,
            metric: matchedRule.metric ?? null,
            severity: matchedRule.severity ?? null,
            trendDirection: matchedRule.trendDirection ?? null,
            provider: matchedRule.provider ?? null,
            workflow: matchedRule.workflow ?? null,
            projectPattern: matchedRule.projectPattern ?? null,
            minSampleCount: matchedRule.minSampleCount ?? null,
            minPassRate: matchedRule.minPassRate ?? null,
            minConfidence: matchedRule.minConfidence ?? null,
            regressionProbabilityAtLeast: matchedRule.regressionProbabilityAtLeast ?? null,
            replayRegressionAtLeast: matchedRule.replayRegressionAtLeast ?? null,
            actionType: matchedRule.actionType,
            requiresApproval: matchedRule.requiresApproval ?? false,
            cooldownMinutes: matchedRule.cooldownMinutes ?? null,
            reason:
              matchedRule.actionType === "replay_experiment"
                ? "命中高风险回放策略"
                : "命中评分卡调整策略",
          },
    resolvedAction,
    recommendedActionType: resolvedAction,
    requiresApproval,
    blockingReasons:
      input.policy.decision === "deny" ? ["policy_denied"] : [],
    evaluatedContext: {
      metric: input.metric,
      score: input.score,
      sampleCount,
      passRate,
      severity,
      trendDirection: input.trendDirection,
      confidence: input.confidence,
      regressionProbability: input.regressionProbability,
      replayRegressionCount: input.replayRegressionCount,
      provider: input.provider ?? null,
      workflow: input.workflow ?? null,
      project: input.project ?? null,
    },
  };
}

function buildQualityAutomationAdvice(input: {
  metric: QualityMetric;
  score: number;
  passed: boolean;
  sampleCount?: number;
  replayRunId?: string;
  regressedCases: number;
  externalSource?: unknown;
}) {
  const actions: string[] = [];
  const roundedScore = Number(input.score.toFixed(2));
  const source = normalizeRecord(input.externalSource);
  const provider = firstNonEmptyString(source.provider);
  const repo = firstNonEmptyString(source.repo);

  if (!input.passed) {
    actions.push(
      `优先排查 ${input.metric} 指标，当前评分 ${roundedScore} 低于 ${fromRepositoryScore(
        QUALITY_AUTOMATION_SCORE_THRESHOLD,
      )}。`,
    );
  }
  if (input.regressedCases > 0) {
    actions.push(`检查 Replay 回退样本，共 ${input.regressedCases} 条 regression。`);
  }
  if (input.sampleCount && input.sampleCount > 0) {
    actions.push(`复核本次评估样本，共 ${input.sampleCount} 条。`);
  }
  if (provider || repo) {
    actions.push(`联动外部来源 ${[provider, repo].filter(Boolean).join(" / ")} 复盘。`);
  }
  if (actions.length === 0) {
    actions.push("当前未发现明显异常，继续观察下一轮质量与回放结果。");
  }

  return {
    title:
      input.regressedCases > 0
        ? "检测到 Replay 回退，建议优先处理 regression。"
        : input.passed
          ? "质量评估已通过，建议继续观察趋势。"
          : "检测到质量下滑，建议尽快处置。",
    summary:
      input.regressedCases > 0
        ? `${input.metric} 评估触发自动治理，回放回退 ${input.regressedCases} 条。`
        : input.passed
          ? `${input.metric} 评估通过，未触发强制处置。`
          : `${input.metric} 评估分数 ${roundedScore}，建议立即跟进。`,
    priority:
      input.regressedCases > 0 || roundedScore < 60
        ? "high"
        : input.passed
          ? "low"
          : "medium",
    actions,
    signals: {
      metric: input.metric,
      score: roundedScore,
      passed: input.passed,
      regressedCases: input.regressedCases,
      sampleCount: input.sampleCount ?? 0,
      replayRunId: input.replayRunId ?? null,
      provider: provider ?? null,
      repo: repo ?? null,
    },
  };
}

async function maybeExecuteQualityAutomationAdvice(input: {
  tenantId: string;
  userId: string;
  userEmail?: string;
  metric: QualityMetric;
  score: number;
  sampleCount?: number;
  replayRunId?: string;
  evaluationId: string;
  occurredAt?: string;
  externalSource?: unknown;
}) {
  const replayRun = input.replayRunId
    ? await repository.getReplayRunById(input.tenantId, input.replayRunId)
    : null;
  const replaySummary = normalizeRecord(replayRun?.summary);
  const regressedCases = Math.max(0, toInteger(replaySummary.regressedCases, 0));
  const policy = await resolveQualityAutomationPolicy(input.tenantId);
  const mappedPolicy = mapQualityAutomationPolicy(policy);
  const passed = input.score >= mappedPolicy.evaluationScoreThreshold;
  const sampleCount = Math.max(0, toInteger(input.sampleCount, 0));
  const passRate = Math.max(0, Math.min(1, input.score / 100));
  const confidence = deriveQualityForecastConfidence(sampleCount);
  const trendDirection: QualityTrendDirection = passed ? "flat" : "down";
  const regressionProbability = computeQualityRegressionProbability({
    avgScore: input.score,
    passRate,
    projectedDelta: passed ? 0 : Number((input.score - mappedPolicy.evaluationScoreThreshold).toFixed(4)),
    totalEvents: sampleCount,
  });
  const shouldTrigger =
    (!passed && mappedPolicy.triggerOnEvaluationFailure) ||
    (regressedCases > 0 && mappedPolicy.triggerOnReplayRegression);

  if (!shouldTrigger) {
    return {
      policy: mappedPolicy,
      triggered: false,
      reason:
        !passed && !mappedPolicy.triggerOnEvaluationFailure
          ? "evaluation_failure_automation_disabled"
          : regressedCases > 0 && !mappedPolicy.triggerOnReplayRegression
            ? "replay_regression_automation_disabled"
            : "score_within_threshold",
      execution: null,
    };
  }

  const evaluatedAt = new Date().toISOString();
  const advice = buildQualityAutomationAdvice({
    metric: input.metric,
    score: input.score,
    passed,
    sampleCount: input.sampleCount,
    replayRunId: input.replayRunId,
    regressedCases,
    externalSource: input.externalSource,
  });
  const source = normalizeRecord(input.externalSource);
  const project = firstNonEmptyString(source.repo) ?? "unknown";
  const provider = firstNonEmptyString(source.provider)?.toLowerCase();
  const workflow = firstNonEmptyString(source.workflow);
  const adviceRange = deriveUtcDayRange(input.occurredAt);
  const adviceId = buildQualityAdviceId({
    project,
    provider,
    workflow,
    from: adviceRange.from ?? null,
    to: adviceRange.to ?? null,
  });
  const severity: QualityAdviceSeverity =
    advice.priority === "high"
      ? "critical"
      : advice.priority === "medium"
        ? "warn"
        : "info";
  const matchedRule = selectQualityAutomationStrategyRule({
    policy: mappedPolicy,
    metric: input.metric,
    severity,
    trendDirection,
    provider,
    workflow,
    project,
    sampleCount,
    passRate,
    confidence,
    regressionProbability,
    replayRegressionCount: regressedCases,
  });
  const selectedActionType: QualityAdviceActionType =
    (matchedRule?.actionType ??
      mappedPolicy.defaultActionType ??
      QUALITY_AUTOMATION_DEFAULT_ACTION_TYPE) as QualityAdviceActionType;
  const selectedRequiresApproval =
    matchedRule?.requiresApproval ?? (policy.decision === "require_approval");
  const datasetIdForReplay = replayRun?.datasetId;
  const latestExecution = await repository.getLatestQualityAdviceExecution(input.tenantId, adviceId);
  const cooldownMinutes = matchedRule?.cooldownMinutes;
  const cooldownActive =
    typeof cooldownMinutes === "number" &&
    cooldownMinutes > 0 &&
    latestExecution?.requestedAt &&
    Date.now() - Date.parse(latestExecution.requestedAt) < cooldownMinutes * 60_000;

  let approvalRequestId: string | undefined;
  let result: "allowed" | "blocked";

  if (cooldownActive) {
    result = "blocked";
  } else if (selectedActionType === "replay_experiment" && !datasetIdForReplay) {
    result = "blocked";
  } else if (selectedRequiresApproval) {
    const approval = await repository.createMcpApprovalRequest(
      input.tenantId,
      {
        toolId: QUALITY_AUTOMATION_TOOL_ID,
        reason: `质量评估 ${input.evaluationId} 触发自动治理建议，请审批后执行。`,
      },
      {
        requestedByUserId: input.userId,
        requestedByEmail: input.userEmail,
      },
    );
    approvalRequestId = approval.approval.id;
    result = "blocked";
  } else if (policy.decision === "deny") {
    result = "blocked";
  } else {
    result = "allowed";
  }

  const invocation = await repository.appendMcpInvocationAudit(input.tenantId, {
    toolId: QUALITY_AUTOMATION_TOOL_ID,
    decision: policy.decision,
    result,
    approvalRequestId,
    enforced: true,
    evaluatedDecision: policy.decision,
    metadata: {
      source: "quality.v2.automation",
      executionKind: "advice_execution",
      evaluationId: input.evaluationId,
      metric: input.metric,
      score: input.score,
      sampleCount: input.sampleCount ?? null,
      replayRunId: input.replayRunId ?? null,
      regressedCases,
      matchedRuleId: matchedRule?.ruleId ?? null,
      selectedActionType,
      regressionProbability,
      confidence,
      advice,
    },
    createdAt: evaluatedAt,
  });
  let adviceExecution = await repository.upsertQualityAdviceExecution(input.tenantId, {
    id: crypto.randomUUID(),
    adviceId,
    project,
    severity,
    actionType: selectedActionType,
    triggerSource: "automatic",
    status:
      result === "allowed"
        ? "running"
        : approvalRequestId
          ? "pending"
          : "failed",
    metric: input.metric,
    datasetId: selectedActionType === "replay_experiment" ? datasetIdForReplay : undefined,
    resultSummary: {
      automationInvocationId: invocation.id,
      defaultActionType: mappedPolicy.defaultActionType,
      matchedRuleId: matchedRule?.ruleId ?? null,
      selectedActionType,
      confidence,
      regressionProbability,
      decision: policy.decision,
      result,
      approvalRequestId: approvalRequestId ?? null,
      evaluationId: input.evaluationId,
      replayRunId: input.replayRunId ?? null,
      currentScore: input.score,
      cooldownActive,
      executionPayload: {
        metric: input.metric,
        currentScore: input.score,
        ...(selectedActionType === "replay_experiment"
          ? {
              datasetId: datasetIdForReplay ?? null,
              baselineVersionId:
                firstNonEmptyString(replayRun?.parameters?.baselineVersionId) ??
                firstNonEmptyString(replayRun?.summary?.baselineVersionId) ??
                null,
            }
          : {
              targetScore: Math.max(
                mappedPolicy.evaluationScoreThreshold,
                Math.min(95, Math.ceil(input.score / 5) * 5 + 10),
              ),
            }),
      },
      advice,
    },
    error:
      result === "allowed"
        ? null
        : approvalRequestId
          ? null
          : cooldownActive
            ? "automation_cooldown_active"
            : selectedActionType === "replay_experiment" && !datasetIdForReplay
              ? "automation_replay_dataset_missing"
              : "automation_denied_by_policy",
    requestedAt: evaluatedAt,
    startedAt: evaluatedAt,
    finishedAt: result === "allowed" ? null : !approvalRequestId ? evaluatedAt : null,
    updatedAt: evaluatedAt,
  });

  if (result === "allowed") {
    adviceExecution = await executeQualityAdviceExecution(adviceExecution, {
      actorUserId: input.userId,
      actorEmail: input.userEmail,
      continuationTrigger: "automatic_allowed",
      approvalRequestId,
    });
  }

  return {
    policy: mappedPolicy,
    triggered: true,
    reason:
      regressedCases > 0 ? "replay_regression_detected" : "score_below_threshold",
    execution: {
      executionId: invocation.id,
      toolId: QUALITY_AUTOMATION_TOOL_ID,
      status:
        result === "blocked"
          ? "blocked"
          : adviceExecution.status === "completed"
            ? "executed"
            : "failed",
      decision: policy.decision,
      result,
      adviceExecutionId: adviceExecution.id,
      approvalRequestId,
      advice,
      createdAt: invocation.createdAt,
      metadata: invocation.metadata,
    },
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
  const currentVersionId = firstNonEmptyString(metadata.currentVersionId) ?? null;
  const currentVersionNumber =
    typeof metadata.currentVersionNumber === "number" &&
    Number.isInteger(metadata.currentVersionNumber) &&
    metadata.currentVersionNumber > 0
      ? metadata.currentVersionNumber
      : currentVersionId
        ? toPositiveInteger(metadata.currentVersionNumber, 1)
        : null;
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
    currentVersionId,
    currentVersionNumber,
    caseCount: dataset.caseCount,
    sampleCount: dataset.caseCount,
    metadata,
    createdAt: dataset.createdAt,
    updatedAt: dataset.updatedAt,
  };
}

function mapReplayBaselineVersion(version: ReplayBaselineVersion) {
  return {
    id: version.id,
    tenantId: version.tenantId,
    replayDatasetId: version.baselineId,
    datasetId: version.baselineId,
    baselineId: version.baselineId,
    version: version.version,
    datasetRef: version.datasetRef ?? null,
    model: version.model,
    promptVersion: version.promptVersion ?? null,
    sampleCount: version.scenarioCount,
    metadata: normalizeRecord(version.metadata),
    note: version.note ?? null,
    createdAt: version.createdAt,
    promotedAt: version.promotedAt ?? null,
  };
}

async function loadReplayDatasetVersionCasesSnapshot(input: {
  tenantId: string;
  datasetId: string;
  versionId: string;
  limit: number;
}) {
  let items = await repository.listReplayDatasetVersionCases(
    input.tenantId,
    input.datasetId,
    input.versionId,
    { limit: input.limit },
  );
  if (items.length > 0) {
    return items;
  }
  const dataset = await repository.getReplayDatasetById(input.tenantId, input.datasetId);
  if (!dataset) {
    return [] as Awaited<ReturnType<typeof repository.listReplayDatasetCases>>;
  }
  if (resolveReplayDatasetCurrentVersionId(dataset) !== input.versionId) {
    return items;
  }
  const currentCases = await repository.listReplayDatasetCases(input.tenantId, input.datasetId, {
    limit: input.limit,
  });
  if (currentCases.length === 0) {
    return currentCases;
  }
  await repository.replaceReplayDatasetVersionCases(
    input.tenantId,
    input.datasetId,
    input.versionId,
    currentCases,
  );
  items = await repository.listReplayDatasetVersionCases(
    input.tenantId,
    input.datasetId,
    input.versionId,
    { limit: input.limit },
  );
  return items;
}

function extractReplayExperimentBaselineVersionIdFromRun(
  run: ReplayRun,
): string | undefined {
  const parameters = normalizeRecord(run.parameters);
  const summary = normalizeRecord(run.summary);
  const summaryMetadata = normalizeRecord(summary.metadata);
  return firstNonEmptyString(
    parameters.baselineVersionId,
    parameters.baseline_version_id,
    summary.baselineVersionId,
    summary.baseline_version_id,
    summaryMetadata.baselineVersionId,
    summaryMetadata.baseline_version_id,
  );
}

function resolveReplayExperimentBaselineVersionId(
  record: ReplayExperimentRecord,
  runs: ReplayRun[],
): string | undefined {
  const persisted = firstNonEmptyString(record.baselineVersionId);
  if (persisted) {
    return persisted;
  }
  const remembered = readReplayExperimentBaselineVersionId(record.tenantId, record.id);
  if (remembered) {
    return remembered;
  }
  const derivedFromRuns = runs
    .map((run) => extractReplayExperimentBaselineVersionIdFromRun(run))
    .find((value): value is string => typeof value === "string" && value.trim().length > 0);
  if (derivedFromRuns) {
    rememberReplayExperimentBaselineVersionId(record.tenantId, record.id, derivedFromRuns);
  }
  return derivedFromRuns;
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

function clampScore(score: number): number {
  if (!Number.isFinite(score)) {
    return 0;
  }
  return Number(Math.max(0, Math.min(score, 100)).toFixed(4));
}

function deriveQualityForecastConfidence(totalEvents: number): number {
  if (totalEvents >= 20) {
    return 0.85;
  }
  if (totalEvents >= 10) {
    return 0.65;
  }
  if (totalEvents >= 3) {
    return 0.45;
  }
  return 0.25;
}

function deriveQualityAdviceSeverity(
  avgScore: number,
  passRate: number,
): QualityAdviceSeverity {
  if (avgScore < 75 || passRate < 0.7) {
    return "critical";
  }
  if (avgScore < 85 || passRate < 0.85) {
    return "warn";
  }
  return "info";
}

function deriveConfidenceLabel(confidence: number): "low" | "medium" | "high" {
  if (confidence >= 0.75) {
    return "high";
  }
  if (confidence >= 0.45) {
    return "medium";
  }
  return "low";
}

function buildQualityForecastRationale(input: {
  project: string;
  avgScore: number;
  passRate: number;
  totalEvents: number;
  trendDirection: "up" | "down" | "flat";
  projectedDelta: number;
}) {
  const passRatePercent = Number((input.passRate * 100).toFixed(2));
  const deltaLabel =
    input.trendDirection === "up"
      ? "预计延续上升"
      : input.trendDirection === "down"
        ? "预计继续下滑"
        : "预计保持平稳";
  return `项目 ${input.project} 在最近 ${input.totalEvents} 条样本中均分 ${input.avgScore.toFixed(
    2,
  )}、通过率 ${passRatePercent}% ，${deltaLabel}（delta ${input.projectedDelta.toFixed(2)}）。`;
}

function buildQualityTimeseriesForecastItem(input: {
  project: string;
  metric: string;
  series: QualityDailyMetric[];
  forecastHorizonDays: number;
  windowStart: string;
  windowEnd: string;
}): Record<string, unknown> | null {
  const points = [...(input.series ?? [])]
    .filter((item) => typeof item?.date === "string" && item.date.length >= 10 && item.total > 0)
    .sort((a, b) => a.date.localeCompare(b.date));
  const basisDays = points.length;
  const totalEvents = points.reduce((sum, item) => sum + item.total, 0);
  if (basisDays < 5 || totalEvents < 10) {
    return null;
  }

  const passedEvents = points.reduce((sum, item) => sum + item.passed, 0);
  const passRate = totalEvents > 0 ? Number((passedEvents / totalEvents).toFixed(6)) : 0;
  const scoreSum = points.reduce(
    (sum, item) => sum + fromRepositoryScore(item.averageScore) * item.total,
    0,
  );
  const avgScore = totalEvents > 0 ? Number((scoreSum / totalEvents).toFixed(4)) : 0;

  const sliceSize = Math.max(1, Math.min(3, Math.floor(basisDays / 2)));
  const firstSlice = points.slice(0, sliceSize);
  const lastSlice = points.slice(-sliceSize);
  const firstTotal = firstSlice.reduce((sum, item) => sum + item.total, 0);
  const lastTotal = lastSlice.reduce((sum, item) => sum + item.total, 0);
  const firstPassed = firstSlice.reduce((sum, item) => sum + item.passed, 0);
  const lastPassed = lastSlice.reduce((sum, item) => sum + item.passed, 0);
  const firstScoreSum = firstSlice.reduce(
    (sum, item) => sum + fromRepositoryScore(item.averageScore) * item.total,
    0,
  );
  const lastScoreSum = lastSlice.reduce(
    (sum, item) => sum + fromRepositoryScore(item.averageScore) * item.total,
    0,
  );
  const firstAvgScore =
    firstTotal > 0 ? Number((firstScoreSum / firstTotal).toFixed(4)) : avgScore;
  const lastAvgScore = lastTotal > 0 ? Number((lastScoreSum / lastTotal).toFixed(4)) : avgScore;
  const firstPassRate = firstTotal > 0 ? Number((firstPassed / firstTotal).toFixed(6)) : passRate;
  const lastPassRate = lastTotal > 0 ? Number((lastPassed / lastTotal).toFixed(6)) : passRate;

  const historicalSpan = Math.max(1, basisDays - 1);
  const rawTrendDelta =
    ((lastAvgScore - firstAvgScore) / historicalSpan) * Math.max(1, input.forecastHorizonDays);
  const trendDelta = Math.max(-12, Math.min(12, rawTrendDelta));
  const passRateAdjustment = Math.max(-6, Math.min(6, (passRate - 0.85) * 10));
  const projectedDelta = Number((trendDelta + passRateAdjustment).toFixed(4));
  const trendDirection =
    projectedDelta > 0.5 ? "up" : projectedDelta < -0.5 ? "down" : "flat";

  const dailyScores = points.map((item) => fromRepositoryScore(item.averageScore));
  const meanScore = dailyScores.reduce((sum, value) => sum + value, 0) / dailyScores.length;
  const scoreVariance =
    dailyScores.reduce((sum, value) => sum + (value - meanScore) ** 2, 0) / dailyScores.length;
  const scoreStdDev = Math.sqrt(scoreVariance);

  let confidence = deriveQualityForecastConfidence(totalEvents);
  if (basisDays < 7) {
    confidence = Math.min(confidence, 0.45);
  }
  if (scoreStdDev > 8) {
    confidence = Math.min(confidence, 0.45);
  } else if (scoreStdDev > 5) {
    confidence = Math.min(confidence, 0.65);
  }
  confidence = clampProbability(confidence);

  const previousAverageScore = Number(firstAvgScore.toFixed(4));
  const previousPassRate = clampProbability(firstPassRate);
  const regressionProbability = computeQualityRegressionProbability({
    avgScore,
    passRate,
    projectedDelta,
    previousAverageScore,
    previousPassRate,
    totalEvents,
  });
  const scoreBand = Math.max(
    1,
    Number(((1 - confidence) * 10).toFixed(4)),
    Number((scoreStdDev * 1.2).toFixed(4)),
  );
  const predictedScore = clampScore(avgScore + projectedDelta);

  return {
    project: input.project,
    metric: input.metric,
    modelVersion: "quality-timeseries-v1",
    forecastHorizonDays: input.forecastHorizonDays,
    predictedScore,
    expectedScoreRange: {
      lower: clampScore(predictedScore - scoreBand),
      upper: clampScore(predictedScore + scoreBand),
    },
    confidence,
    confidenceLabel: deriveConfidenceLabel(confidence),
    trendDirection,
    projectedDelta,
    regressionProbability,
    basisWindowCount: totalEvents,
    rationale: buildQualityForecastRationale({
      project: input.project,
      avgScore,
      passRate,
      totalEvents,
      trendDirection,
      projectedDelta,
    }),
    explanation: {
      summary:
        trendDirection === "down"
          ? "近期质量序列呈下行趋势，建议优先复盘低分日期与失败样本。"
          : trendDirection === "up"
            ? "近期质量序列呈上行趋势，可继续观察并巩固有效策略。"
            : "近期质量序列整体平稳，建议继续按窗口观测。",
      confidenceLabel: deriveConfidenceLabel(confidence),
      primaryDriver: passRate < 0.85 ? "pass_rate" : "average_score",
    },
    featureContributions: Object.entries(
      buildQualityFeatureContributions({
        avgScore,
        passRate,
        projectedDelta,
        previousAverageScore,
        previousPassRate,
      }),
    ).map(([feature, impact]) => ({
      feature,
      impact,
      direction: impact > 0 ? "positive" : impact < 0 ? "negative" : "neutral",
    })),
    windowComparisons: {
      currentWindow: {
        averageScore: lastAvgScore,
        passRate: lastPassRate,
        totalEvents: lastTotal,
      },
      previousWindow: {
        averageScore: previousAverageScore,
        passRate: previousPassRate,
        totalEvents: firstTotal,
      },
    },
    riskDrivers: [
      passRate < 0.85 ? "pass_rate" : "average_score",
      totalEvents < 10 ? "low_sample_size" : basisDays < 7 ? "short_timeseries" : "stable_timeseries",
    ],
    recommendedActions:
      trendDirection === "down"
        ? ["review_failed_samples", "run_replay_experiment"]
        : trendDirection === "up"
          ? ["observe_current_policy"]
          : ["monitor_next_window"],
    windowStart: input.windowStart,
    windowEnd: input.windowEnd,
    basis: {
      totalEvents,
      passRate,
      averageScore: avgScore,
      basisDays,
    },
  };
}

function buildQualityAdviceExecutionOptions(input: {
  severity: QualityAdviceSeverity;
  policy: ReturnType<typeof mapQualityAutomationPolicy>;
}) {
  const scorecardAvailability =
    input.policy.decision === "deny"
      ? "disabled"
      : input.policy.decision === "require_approval"
        ? "approval_required"
        : input.severity === "critical"
          ? "recommended"
          : input.severity === "warn"
            ? "available"
            : "available";
  const replayAvailability =
    input.severity === "critical" || input.severity === "warn"
      ? "recommended"
      : "available";

  return [
    {
      actionType: "scorecard_adjustment" as const,
      availability: scorecardAvailability,
      reason:
        input.policy.decision === "deny"
          ? "当前自动治理策略禁止直接执行评分卡调整。"
          : input.policy.decision === "require_approval"
            ? "评分卡调整需先经过 MCP 审批后继续执行。"
            : input.severity === "critical"
              ? "建议先抬高评分卡门槛，尽快阻断明显低质量输出。"
              : "可直接调整评分卡阈值，缩小质量波动窗口。",
    },
    {
      actionType: "replay_experiment" as const,
      availability: replayAvailability,
      reason:
        input.severity === "critical"
          ? "建议立即补一轮 replay experiment，定位回退样本与候选差异。"
          : input.severity === "warn"
            ? "建议补充对比 run，验证是否存在隐性回退。"
            : "当前可选做回放抽检，继续观测趋势。",
    },
  ];
}

function buildQualityAdviceExplanation(input: {
  project: string;
  severity: QualityAdviceSeverity;
  avgScore: number;
  passRate: number;
  totalEvents: number;
  policy: ReturnType<typeof mapQualityAutomationPolicy>;
}) {
  const confidence = deriveQualityForecastConfidence(input.totalEvents);
  const confidenceLabel = deriveConfidenceLabel(confidence);
  const primaryDriver =
    input.avgScore < 75
      ? "average_score"
      : input.passRate < 0.85
        ? "pass_rate"
        : "stability";
  const recommendedActionType =
    input.severity === "critical"
      ? "replay_experiment"
      : input.policy.defaultActionType;

  return {
    summary:
      input.severity === "critical"
        ? `项目 ${input.project} 当前均分 ${input.avgScore.toFixed(
            2,
          )}，且通过率 ${(input.passRate * 100).toFixed(2)}%，已进入优先治理区间。`
        : input.severity === "warn"
          ? `项目 ${input.project} 出现中等强度波动，均分 ${input.avgScore.toFixed(
              2,
            )}，建议补充对比验证。`
          : `项目 ${input.project} 当前质量相对稳定，建议维持观察。`,
    primaryDriver,
    confidence,
    confidenceLabel,
    basisWindowCount: input.totalEvents,
    automationDecision: input.policy.decision,
    recommendedActionType,
  };
}

function summarizeReplayWorkflowStepStatus(
  runs: ReplayRun[],
): {
  completedSteps: number;
  runningSteps: number;
  failedSteps: number;
  cancelledSteps: number;
  queuedSteps: number;
} {
  return runs.reduce(
    (summary, run) => {
      if (run.status === "completed") {
        summary.completedSteps += 1;
      } else if (run.status === "running") {
        summary.runningSteps += 1;
      } else if (run.status === "failed") {
        summary.failedSteps += 1;
      } else if (run.status === "cancelled") {
        summary.cancelledSteps += 1;
      } else {
        summary.queuedSteps += 1;
      }
      return summary;
    },
    {
      completedSteps: 0,
      runningSteps: 0,
      failedSteps: 0,
      cancelledSteps: 0,
      queuedSteps: 0,
    },
  );
}

function buildReplayExperimentComparison(input: {
  experiment: ReplayExperimentRecord;
  runs: ReplayRun[];
}) {
  const items = input.runs
    .map((run) => {
      const mapped = mapReplayRun(run);
      const totalCases = Math.max(mapped.totalCases, 1);
      const processedCases = Math.max(mapped.processedCases, 0);
      const passRate =
        processedCases > 0
          ? Number(((processedCases - mapped.regressedCases) / processedCases).toFixed(6))
          : 0;
      const improvementRate = Number((mapped.improvedCases / totalCases).toFixed(6));
      const regressionRate = Number((mapped.regressedCases / totalCases).toFixed(6));
      const netDelta = mapped.improvedCases - mapped.regressedCases;
      return {
        runId: mapped.id,
        candidateLabel: mapped.candidateLabel,
        status: mapped.status,
        totalCases: mapped.totalCases,
        processedCases,
        improvedCases: mapped.improvedCases,
        regressedCases: mapped.regressedCases,
        unchangedCases: mapped.unchangedCases,
        passRate,
        improvementRate,
        regressionRate,
        netDelta,
        startedAt: mapped.createdAt,
        finishedAt: mapped.finishedAt ?? null,
      };
    })
    .sort((left, right) => {
      if (right.netDelta !== left.netDelta) {
        return right.netDelta - left.netDelta;
      }
      if (left.regressionRate !== right.regressionRate) {
        return left.regressionRate - right.regressionRate;
      }
      return left.candidateLabel.localeCompare(right.candidateLabel);
    });

  const winner = items[0];
  const loser = items.at(-1);
  const runningRuns = items.filter((item) => item.status === "running").length;
  const queuedRuns = items.filter((item) => item.status === "pending").length;
  const cancelledRuns = items.filter((item) => item.status === "cancelled").length;
  return {
    experimentId: input.experiment.id,
    datasetId: input.experiment.datasetId,
    items,
    total: items.length,
    summary: {
      totalRuns: items.length,
      completedRuns: items.filter((item) => item.status === "completed").length,
      failedRuns: items.filter((item) => item.status === "failed").length,
      bestRunId: winner?.runId ?? null,
      worstRunId: loser?.runId ?? null,
      bestNetDelta: winner?.netDelta,
      worstNetDelta: loser?.netDelta,
      runningRuns,
      queuedRuns,
      cancelledRuns,
    },
  };
}

function buildReplayExperimentWorkflow(input: {
  experiment: ReplayExperimentRecord;
  runs: ReplayRun[];
}) {
  const runNodes = input.runs
    .map((run) => {
      const mapped = mapReplayRun(run);
      return {
        id: `run:${mapped.id}`,
        type: "run" as const,
        label: `候选 ${mapped.candidateLabel}`,
        status: mapped.status,
        startedAt: mapped.createdAt,
        finishedAt: mapped.finishedAt ?? null,
        metadata: {
          runId: mapped.id,
          candidateLabel: mapped.candidateLabel,
          processedCases: mapped.processedCases,
          totalCases: mapped.totalCases,
          improvedCases: mapped.improvedCases,
          regressedCases: mapped.regressedCases,
        },
      };
    })
    .sort((left, right) => left.startedAt.localeCompare(right.startedAt));
  const runSummary = summarizeReplayWorkflowStepStatus(input.runs);
  const finalStatus =
    input.experiment.status === "cancelled"
      ? "cancelled"
      : runSummary.failedSteps > 0
        ? "failed"
        : runSummary.runningSteps > 0
          ? "running"
          : runSummary.queuedSteps > 0
          ? "queued"
            : "completed";

  return {
    experimentId: input.experiment.id,
    status: finalStatus,
    nodes: [
      {
        id: `experiment:${input.experiment.id}`,
        type: "experiment" as const,
        label: input.experiment.name,
        status: finalStatus,
        startedAt: input.experiment.createdAt,
        finishedAt: input.experiment.finishedAt ?? null,
        metadata: {
          datasetId: input.experiment.datasetId,
          sourceAdviceId: input.experiment.sourceAdviceId ?? null,
          triggerSource: input.experiment.triggerSource,
        },
      },
      ...runNodes,
    ],
    edges: runNodes.map((node) => ({
      from: `experiment:${input.experiment.id}`,
      to: node.id,
      label: "dispatches",
    })),
    summary: {
      totalNodes: runNodes.length + 1,
      totalRuns: runNodes.length,
      queuedRuns: runSummary.queuedSteps,
      runningRuns: runSummary.runningSteps,
      completedRuns: runSummary.completedSteps,
      failedRuns: runSummary.failedSteps,
      cancelledRuns: runSummary.cancelledSteps,
    },
  };
}

async function listQualityAdviceExecutionsByTenant(
  tenantId: string,
): Promise<QualityAdviceExecutionRecord[]> {
  return repository.listQualityAdviceExecutions(tenantId, { limit: 500 });
}

async function saveQualityAdviceExecution(
  record: QualityAdviceExecutionRecord,
): Promise<QualityAdviceExecutionRecord> {
  const { tenantId, ...input } = record;
  return repository.upsertQualityAdviceExecution(tenantId, input);
}

async function getLatestQualityAdviceExecution(
  tenantId: string,
  adviceId: string,
): Promise<QualityAdviceExecutionRecord | null> {
  return repository.getLatestQualityAdviceExecution(tenantId, adviceId);
}

function deriveReplayExperimentStatus(
  record: ReplayExperimentRecord,
  runs: ReplayRun[],
): ReplayExperimentStatus {
  if (record.status === "cancelled") {
    return "cancelled";
  }
  if (runs.length === 0) {
    return record.status === "draft" ? "draft" : "queued";
  }
  if (runs.some((item) => item.status === "running")) {
    return "running";
  }
  if (runs.some((item) => item.status === "pending")) {
    return "queued";
  }
  if (runs.every((item) => item.status === "completed")) {
    return "completed";
  }
  if (runs.some((item) => item.status === "failed")) {
    return "failed";
  }
  return record.status;
}

async function listReplayExperimentsByTenant(
  tenantId: string,
): Promise<ReplayExperimentRecord[]> {
  return repository.listReplayExperiments(tenantId, { limit: 500 });
}

async function saveReplayExperiment(
  record: ReplayExperimentRecord,
): Promise<ReplayExperimentRecord> {
  const { tenantId, ...input } = record;
  return repository.upsertReplayExperiment(tenantId, input);
}

async function getReplayExperimentById(
  tenantId: string,
  experimentId: string,
): Promise<ReplayExperimentRecord | null> {
  return repository.getReplayExperimentById(tenantId, experimentId);
}

function mapReplayExperiment(
  record: ReplayExperimentRecord,
  runs: ReplayRun[],
) {
  const runItems = runs.map((run) => mapReplayRun(run));
  const status = deriveReplayExperimentStatus(record, runs);
  const totalRuns = runItems.length;
  const completedRuns = runItems.filter((item) => item.status === "completed").length;
  const failedRuns = runItems.filter((item) => item.status === "failed").length;
  const runningRuns = runItems.filter((item) => item.status === "running").length;
  const queuedRuns = runItems.filter((item) => item.status === "pending").length;
  const totalCases = runItems.reduce((sum, item) => sum + item.totalCases, 0);
  const processedCases = runItems.reduce((sum, item) => sum + item.processedCases, 0);
  const improvedCases = runItems.reduce((sum, item) => sum + item.improvedCases, 0);
  const regressedCases = runItems.reduce((sum, item) => sum + item.regressedCases, 0);
  const baselineVersionId = resolveReplayExperimentBaselineVersionId(record, runs) ?? null;
  const aggregateSummary = {
    totalCases,
    processedCases,
    improvedCases,
    regressedCases,
    baselineVersionId,
  };
  const runStatusSummary = {
    totalRuns,
    completedRuns,
    failedRuns,
    runningRuns,
    queuedRuns,
  };

  return {
    id: record.id,
    tenantId: record.tenantId,
    name: record.name,
    datasetId: record.datasetId,
    baselineId: record.baselineId ?? null,
    baselineVersionId,
    status,
    triggerSource: record.triggerSource,
    executionMode: record.executionMode,
    candidateLabels: [...record.candidateLabels],
    sourceAdviceId: record.sourceAdviceId ?? null,
    runIds: [...record.runIds],
    runStatusSummary,
    aggregateSummary,
    summary: {
      status,
      ...runStatusSummary,
      ...aggregateSummary,
      baselineVersionId,
    },
    metadata: {
      baselineVersionId,
    },
    runs: runItems,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
    startedAt: record.startedAt ?? null,
    finishedAt: record.finishedAt ?? null,
    lastError: record.lastError ?? null,
  };
}

function mapReplayExperimentComparisonItem(
  experiment: ReturnType<typeof mapReplayExperiment>,
) {
  const runBreakdown = experiment.runs
    .map((run) => {
      const totalCases = Math.max(0, run.totalCases);
      const processedCases = Math.max(0, run.processedCases);
      const passRate =
        processedCases > 0
          ? Number(((processedCases - run.regressedCases) / processedCases).toFixed(6))
          : 0;
      const improvementRate =
        totalCases > 0 ? Number((run.improvedCases / totalCases).toFixed(6)) : 0;
      const regressionRate =
        totalCases > 0 ? Number((run.regressedCases / totalCases).toFixed(6)) : 0;
      const netDelta = run.improvedCases - run.regressedCases;
      return {
        runId: run.id,
        candidateLabel: run.candidateLabel,
        status: run.status,
        totalCases,
        processedCases,
        improvedCases: run.improvedCases,
        regressedCases: run.regressedCases,
        unchangedCases: run.unchangedCases,
        passRate,
        improvementRate,
        regressionRate,
        netDelta,
        startedAt: run.startedAt ?? run.createdAt,
        finishedAt: run.finishedAt ?? null,
      };
    })
    .sort((left, right) => {
      if (right.netDelta !== left.netDelta) {
        return right.netDelta - left.netDelta;
      }
      if (right.improvementRate !== left.improvementRate) {
        return right.improvementRate - left.improvementRate;
      }
      return left.regressionRate - right.regressionRate;
    });

  const bestRun = runBreakdown[0];
  const worstRun = runBreakdown.at(-1);
  const aggregateSummary = normalizeRecord(experiment.aggregateSummary);
  const runStatusSummary = normalizeRecord(experiment.runStatusSummary);
  const totalCases = Math.max(0, toInteger(aggregateSummary.totalCases, 0));
  const processedCases = Math.max(0, toInteger(aggregateSummary.processedCases, 0));
  const improvedCases = Math.max(0, toInteger(aggregateSummary.improvedCases, 0));
  const regressedCases = Math.max(0, toInteger(aggregateSummary.regressedCases, 0));
  const completedRuns = Math.max(0, toInteger(runStatusSummary.completedRuns, 0));
  const failedRuns = Math.max(0, toInteger(runStatusSummary.failedRuns, 0));
  const runningRuns = Math.max(0, toInteger(runStatusSummary.runningRuns, 0));
  const queuedRuns = Math.max(0, toInteger(runStatusSummary.queuedRuns, 0));
  const totalRuns = Math.max(0, toInteger(runStatusSummary.totalRuns, experiment.runs.length));
  const improvementRate =
    totalCases > 0 ? Number((improvedCases / totalCases).toFixed(6)) : 0;
  const regressionRate =
    totalCases > 0 ? Number((regressedCases / totalCases).toFixed(6)) : 0;
  const workflowStage =
    experiment.status === "completed"
      ? "completed"
      : experiment.status === "failed"
        ? "failed"
        : experiment.status === "cancelled"
          ? "cancelled"
          : experiment.status === "running"
            ? "running"
            : experiment.status === "queued"
              ? "queued"
              : "draft";

  return {
    experimentId: experiment.id,
    name: experiment.name,
    datasetId: experiment.datasetId,
    status: experiment.status ?? "draft",
    workflowStage,
    triggerSource: experiment.triggerSource ?? "manual",
    sourceAdviceId: experiment.sourceAdviceId ?? null,
    candidateLabels: [...(experiment.candidateLabels ?? [])],
    totalRuns,
    completedRuns,
    failedRuns,
    runningRuns,
    queuedRuns,
    totalCases,
    processedCases,
    improvedCases,
    regressedCases,
    improvementRate,
    regressionRate,
    netDelta: improvedCases - regressedCases,
    bestRunId: bestRun?.runId ?? null,
    worstRunId: worstRun?.runId ?? null,
    runs: runBreakdown,
    updatedAt: experiment.updatedAt,
  };
}

function buildReplayExperimentComparisonResponse(input: {
  items: Array<ReturnType<typeof mapReplayExperimentComparisonItem>>;
  experimentIds: string[];
  datasetId?: string;
}) {
  const totalRuns = input.items.reduce((sum, item) => sum + item.totalRuns, 0);
  const completedRuns = input.items.reduce((sum, item) => sum + item.completedRuns, 0);
  const failedRuns = input.items.reduce((sum, item) => sum + item.failedRuns, 0);
  const runningRuns = input.items.reduce((sum, item) => sum + item.runningRuns, 0);
  const queuedRuns = input.items.reduce((sum, item) => sum + item.queuedRuns, 0);
  const totalCases = input.items.reduce((sum, item) => sum + item.totalCases, 0);
  const processedCases = input.items.reduce((sum, item) => sum + item.processedCases, 0);
  const improvedCases = input.items.reduce((sum, item) => sum + item.improvedCases, 0);
  const regressedCases = input.items.reduce((sum, item) => sum + item.regressedCases, 0);
  const rankedItems = [...input.items].sort((left, right) => {
    if (right.netDelta !== left.netDelta) {
      return right.netDelta - left.netDelta;
    }
    if (right.improvementRate !== left.improvementRate) {
      return right.improvementRate - left.improvementRate;
    }
    return left.regressionRate - right.regressionRate;
  });

  return {
    items: input.items,
    total: input.items.length,
    summary: {
      comparedExperimentCount: input.items.length,
      comparedAt: new Date().toISOString(),
      datasets: Array.from(new Set(input.items.map((item) => item.datasetId))),
      totalRuns,
      completedRuns,
      failedRuns,
      runningRuns,
      queuedRuns,
      totalCases,
      processedCases,
      improvedCases,
      regressedCases,
      bestExperimentId: rankedItems[0]?.experimentId ?? null,
      worstExperimentId: rankedItems.at(-1)?.experimentId ?? null,
    },
    filters: {
      experimentIds: input.experimentIds,
      datasetId: input.datasetId ?? null,
    },
  };
}

async function listReplayExperimentRuns(
  tenantId: string,
  record: ReplayExperimentRecord,
): Promise<ReplayRun[]> {
  return (
    await repository.listReplayRuns(tenantId, {
      datasetId: record.datasetId,
      limit: 500,
    })
  ).filter((run) => record.runIds.length === 0 || record.runIds.includes(run.id));
}

async function mapReplayExperimentWithRuns(
  tenantId: string,
  record: ReplayExperimentRecord,
) {
  const runs = await listReplayExperimentRuns(tenantId, record);
  return mapReplayExperiment(record, runs);
}

async function triggerReplayExperimentRuns(input: {
  tenantId: string;
  experiment: ReplayExperimentRecord;
  candidateLabels?: string[];
  skipIfRunning?: boolean;
}): Promise<ReplayExperimentRecord> {
  const skipIfRunning = input.skipIfRunning ?? true;
  const candidateLabelsSource =
    input.candidateLabels && input.candidateLabels.length > 0
      ? input.candidateLabels
      : input.experiment.candidateLabels.length > 0
        ? input.experiment.candidateLabels
        : ["candidate"];
  const candidateLabels = Array.from(
    new Set(
      candidateLabelsSource
        .map((item) => (typeof item === "string" ? item.trim() : ""))
        .filter(Boolean),
    ),
  );
  const existingRunIds = new Set(input.experiment.runIds);
  const linkedRunIds: string[] = [];
  const createdRunIds: string[] = [];
  const dataset = await repository.getReplayDatasetById(
    input.tenantId,
    input.experiment.datasetId,
  );
  const baselineVersionId =
    firstNonEmptyString(input.experiment.baselineVersionId) ??
    readReplayExperimentBaselineVersionId(input.tenantId, input.experiment.id) ??
    (dataset ? resolveReplayDatasetCurrentVersionId(dataset) : undefined);
  const activeRuns = skipIfRunning
    ? (
        await Promise.all([
          repository.listReplayRuns(input.tenantId, {
            datasetId: input.experiment.datasetId,
            status: "pending",
            limit: 500,
          }),
          repository.listReplayRuns(input.tenantId, {
            datasetId: input.experiment.datasetId,
            status: "running",
            limit: 500,
          }),
        ])
      )
        .flat()
        .filter((run) => {
          const experimentId = firstNonEmptyString(
            run.parameters.experimentId,
            run.parameters.experiment_id,
            run.summary.experimentId,
            run.summary.experiment_id,
          );
          return experimentId === input.experiment.id;
        })
        .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
    : [];

  for (const candidateLabel of candidateLabels.length > 0 ? candidateLabels : ["candidate"]) {
    if (skipIfRunning) {
      const existingActiveRun = activeRuns.find((run) => {
        const runCandidateLabel = firstNonEmptyString(
          run.parameters.candidateLabel,
          run.parameters.candidate_label,
          run.summary.candidateLabel,
          run.summary.candidate_label,
        );
        if (runCandidateLabel !== candidateLabel) {
          return false;
        }
        const runBaselineVersionId = firstNonEmptyString(
          run.parameters.baselineVersionId,
          run.parameters.baseline_version_id,
          run.summary.baselineVersionId,
          run.summary.baseline_version_id,
        );
        return runBaselineVersionId === baselineVersionId;
      });
      if (existingActiveRun) {
        if (!existingRunIds.has(existingActiveRun.id)) {
          existingRunIds.add(existingActiveRun.id);
          linkedRunIds.push(existingActiveRun.id);
        }
        continue;
      }
    }

    const replayRun = await repository.createReplayRun(input.tenantId, {
      datasetId: input.experiment.datasetId,
      parameters: {
        experimentId: input.experiment.id,
        candidateLabel,
        triggerSource: input.experiment.triggerSource,
        ...(baselineVersionId
          ? { baselineVersionId }
          : {}),
      },
      summary: {
        totalCases: 12,
        candidateLabel,
        ...(baselineVersionId
          ? { baselineVersionId }
          : {}),
      },
    });
    enqueueReplayJobExecution(input.tenantId, replayRun.id);
    if (!existingRunIds.has(replayRun.id)) {
      existingRunIds.add(replayRun.id);
      createdRunIds.push(replayRun.id);
    }
  }
  return await saveReplayExperiment({
    ...input.experiment,
    runIds: [...input.experiment.runIds, ...linkedRunIds, ...createdRunIds],
    status: "queued",
    startedAt: input.experiment.startedAt ?? new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  });
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
  const automation = await maybeExecuteQualityAutomationAdvice({
    tenantId: auth.tenantId,
    userId: auth.userId,
    userEmail: auth.email,
    metric: validation.data.metric,
    score: validation.data.score,
    sampleCount: validation.data.sampleCount,
    replayRunId: validation.data.replayJobId,
    evaluationId: created.id,
    occurredAt: validation.data.occurredAt,
    externalSource: validation.data.externalSource,
  });
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
      automationTriggered: automation.triggered,
      automationReason: automation.reason,
      automationExecutionId: automation.execution?.executionId ?? null,
    },
  });
  if (automation.execution) {
    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:${requestId}:automation`,
      action: "control_plane.v2.quality.automation_advice_executed",
      level: automation.execution.status === "blocked" ? "warning" : "info",
      detail: `Quality automation handled evaluation ${created.id} with ${automation.execution.status}.`,
      metadata: {
        requestId,
        tenantId: auth.tenantId,
        evaluationId: created.id,
        executionId: automation.execution.executionId,
        adviceExecutionId: automation.execution.adviceExecutionId ?? null,
        result: automation.execution.result,
        decision: automation.execution.decision,
        approvalRequestId: automation.execution.approvalRequestId ?? null,
      },
    });
  }

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
      automation,
      createdAt: created.createdAt,
    },
    201
  );
});

apiV2Routes.get("/quality/automation-policy", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const policy = await resolveQualityAutomationPolicy(auth.tenantId);
  return c.json(mapQualityAutomationPolicy(policy));
});

apiV2Routes.put("/quality/automation-policy", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeRecord(body);
  const validation = validateMcpToolPolicyUpsertInput({
    ...bodyRecord,
    toolId: QUALITY_AUTOMATION_TOOL_ID,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  if (
    bodyRecord.evaluationScoreThreshold !== undefined &&
    (!Number.isFinite(Number(bodyRecord.evaluationScoreThreshold)) ||
      Number(bodyRecord.evaluationScoreThreshold) < 0 ||
      Number(bodyRecord.evaluationScoreThreshold) > 100)
  ) {
    return c.json(
      { message: "evaluationScoreThreshold 必须是 0 到 100 之间的数字。" },
      400,
    );
  }
  if (
    bodyRecord.triggerOnEvaluationFailure !== undefined &&
    typeof bodyRecord.triggerOnEvaluationFailure !== "boolean"
  ) {
    return c.json(
      { message: "triggerOnEvaluationFailure 必须是布尔值。" },
      400,
    );
  }
  if (
    bodyRecord.triggerOnReplayRegression !== undefined &&
    typeof bodyRecord.triggerOnReplayRegression !== "boolean"
  ) {
    return c.json(
      { message: "triggerOnReplayRegression 必须是布尔值。" },
      400,
    );
  }
  const strategyMatrixValidation = validateQualityAutomationStrategyMatrix(
    bodyRecord.strategyMatrix,
  );
  if (!strategyMatrixValidation.success) {
    return c.json({ message: strategyMatrixValidation.error }, 400);
  }

  const currentPolicy = await resolveQualityAutomationPolicy(auth.tenantId);
  const currentMetadata = normalizeRecord(currentPolicy.metadata);
  const metadata = {
    ...currentMetadata,
    evaluationScoreThreshold:
      bodyRecord.evaluationScoreThreshold !== undefined
        ? Number(Number(bodyRecord.evaluationScoreThreshold).toFixed(4))
        : currentMetadata.evaluationScoreThreshold ??
          fromRepositoryScore(QUALITY_AUTOMATION_SCORE_THRESHOLD),
    triggerOnEvaluationFailure:
      bodyRecord.triggerOnEvaluationFailure !== undefined
        ? bodyRecord.triggerOnEvaluationFailure
        : currentMetadata.triggerOnEvaluationFailure ?? true,
    triggerOnReplayRegression:
      bodyRecord.triggerOnReplayRegression !== undefined
        ? bodyRecord.triggerOnReplayRegression
        : currentMetadata.triggerOnReplayRegression ?? true,
    defaultActionType:
      currentMetadata.defaultActionType ?? QUALITY_AUTOMATION_DEFAULT_ACTION_TYPE,
    strategyMatrix:
      bodyRecord.strategyMatrix !== undefined
        ? strategyMatrixValidation.data
        : currentMetadata.strategyMatrix ?? [],
    modelVersion: firstNonEmptyString(bodyRecord.modelVersion) ?? currentMetadata.modelVersion ?? "quality-heuristic-v2",
  };

  const policy = await repository.upsertMcpToolPolicy(auth.tenantId, {
    ...validation.data,
    metadata,
  });
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.v2.quality.automation_policy_upserted",
    level: "info",
    detail: `Updated quality automation policy ${policy.toolId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      toolId: policy.toolId,
      riskLevel: policy.riskLevel,
      decision: policy.decision,
      evaluationScoreThreshold: metadata.evaluationScoreThreshold,
      triggerOnEvaluationFailure: metadata.triggerOnEvaluationFailure,
      triggerOnReplayRegression: metadata.triggerOnReplayRegression,
      strategyMatrixCount: Array.isArray(metadata.strategyMatrix)
        ? metadata.strategyMatrix.length
        : 0,
    },
  });
  return c.json(mapQualityAutomationPolicy(policy));
});

apiV2Routes.post("/quality/automation-policy/simulate", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const body = normalizeRecord(await c.req.json().catch(() => undefined));
  const metric = normalizeQualityMetric(body.metric);
  if (!metric) {
    return c.json(
      { message: "metric 必须是 accuracy/consistency/groundedness/safety/latency 之一。" },
      400,
    );
  }
  const score = toNumber(body.score, Number.NaN);
  if (!Number.isFinite(score) || score < 0 || score > 100) {
    return c.json({ message: "score 必须是 0 到 100 之间的数字。" }, 400);
  }
  const confidence = clampProbability(toNumber(body.confidence, 0.65));
  const trendDirection = normalizeQualityTrendDirection(body.trendDirection) ?? "flat";
  const replayRegressionCount = Math.max(0, toInteger(body.replayRegressionCount, 0));
  const regressionProbability = clampProbability(
    toNumber(
      body.regressionProbability,
      computeQualityRegressionProbability({
        avgScore: score,
        passRate: Math.max(0, Math.min(1, score / 100)),
        projectedDelta: trendDirection === "down" ? -5 : trendDirection === "up" ? 5 : 0,
        totalEvents: Math.max(1, toInteger(body.sampleCount, 1)),
      }),
    ),
  );
  const policy = mapQualityAutomationPolicy(await resolveQualityAutomationPolicy(auth.tenantId));
  const provider = firstNonEmptyString(body.provider)?.toLowerCase();
  const workflow = firstNonEmptyString(body.workflow);
  const project = firstNonEmptyString(body.project);
  return c.json(
    buildQualityStrategyMatrixSimulation({
      policy,
      metric,
      score,
      sampleCount: toInteger(body.sampleCount, 0),
      provider,
      workflow,
      project,
      confidence,
      trendDirection,
      replayRegressionCount,
      regressionProbability,
    }),
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

apiV2Routes.get("/quality/reports/forecast", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const range = parseDateRange(c.req.query("from"), c.req.query("to"));
  if (range.error) {
    return c.json({ message: range.error }, 400);
  }
  const metricQuery = firstNonEmptyString(c.req.query("metric"));
  if (metricQuery && !QUALITY_METRIC_SET.has(metricQuery.toLowerCase())) {
    return c.json(
      { message: "metric 必须是 accuracy/consistency/groundedness/safety/latency 之一。" },
      400,
    );
  }
  const metric = metricQuery ? toQualityMetric(metricQuery) : undefined;
  const limit = toPositiveInteger(c.req.query("limit"), 20);
  if (limit > 100) {
    return c.json({ message: "limit 必须是 1 到 100 的整数。" }, 400);
  }
  const provider = normalizeQualityExternalFilter(c.req.query("provider"), { lowerCase: true });
  const workflow = normalizeQualityExternalFilter(c.req.query("workflow"));
  const requestedModelVersionRaw = firstNonEmptyString(c.req.query("modelVersion"));
  const requestedModelVersion = requestedModelVersionRaw
    ? toQualityForecastModelVersion(requestedModelVersionRaw)
    : undefined;
  if (requestedModelVersionRaw && !requestedModelVersion) {
    return c.json(
      { message: "modelVersion 必须是 quality-heuristic-v2/quality-timeseries-v1 之一。" },
      400,
    );
  }
  const modelVersion: QualityForecastModelVersion = requestedModelVersion ?? "quality-heuristic-v2";

  const groups = await repository.listQualityExternalMetricGroups(auth.tenantId, {
    from: range.from,
    to: range.to,
    scorecardKey: metric,
    provider,
    workflow,
    groupBy: "repo",
    limit,
  });
  const forecastHorizonDays = deriveForecastHorizonDays(range.from, range.to);

  const dailySeriesByProject =
    modelVersion === "quality-timeseries-v1" && range.from && range.to && groups.length > 0
      ? await repository.listQualityDailyMetricsSeriesByRepo(auth.tenantId, {
          from: range.from,
          to: range.to,
          scorecardKey: metric,
          provider,
          workflow,
          repos: Array.from(new Set(groups.map((group) => group.value))),
        })
      : {};

  const items = groups.map((group) => {
    const avgScore = fromRepositoryScore(group.averageScore);
    const passRate =
      group.total > 0 ? Number((group.passed / group.total).toFixed(6)) : 0;

    if (modelVersion === "quality-timeseries-v1" && range.from && range.to) {
      const series = dailySeriesByProject[group.value] ?? [];
      const timeseriesItem = buildQualityTimeseriesForecastItem({
        project: group.value,
        metric: metric ?? "all",
        series,
        forecastHorizonDays,
        windowStart: range.from,
        windowEnd: range.to,
      });
      if (timeseriesItem) {
        return timeseriesItem;
      }
    }

    const trendAdjustment = (passRate - 0.8) * 20;
    const projectedDelta = Number(trendAdjustment.toFixed(4));
    const trendDirection =
      projectedDelta > 0.5 ? "up" : projectedDelta < -0.5 ? "down" : "flat";
    const confidence = deriveQualityForecastConfidence(group.total);
    const previousAverageScore = Number(Math.max(0, avgScore - projectedDelta / 2).toFixed(4));
    const previousPassRate = clampProbability(passRate - projectedDelta / 40);
    const regressionProbability = computeQualityRegressionProbability({
      avgScore,
      passRate,
      projectedDelta,
      previousAverageScore,
      previousPassRate,
      totalEvents: group.total,
    });
    const scoreBand = Math.max(1, Number(((1 - confidence) * 10).toFixed(4)));
    return {
      project: group.value,
      metric: metric ?? "all",
      modelVersion: "quality-heuristic-v2",
      forecastHorizonDays,
      predictedScore: clampScore(avgScore + trendAdjustment),
      expectedScoreRange: {
        lower: clampScore(avgScore + trendAdjustment - scoreBand),
        upper: clampScore(avgScore + trendAdjustment + scoreBand),
      },
      confidence,
      confidenceLabel: deriveConfidenceLabel(confidence),
      trendDirection,
      projectedDelta,
      regressionProbability,
      basisWindowCount: group.total,
      rationale: buildQualityForecastRationale({
        project: group.value,
        avgScore,
        passRate,
        totalEvents: group.total,
        trendDirection,
        projectedDelta,
      }),
      explanation: {
        summary:
          trendDirection === "down"
            ? "近期质量信号偏弱，建议优先查看失败样本与工作流漂移。"
            : trendDirection === "up"
              ? "近期质量趋势向好，可继续观察当前策略是否稳定。"
              : "近期质量信号相对平稳，建议继续按窗口观测。",
        confidenceLabel: deriveConfidenceLabel(confidence),
        primaryDriver: passRate < 0.85 ? "pass_rate" : "average_score",
      },
      featureContributions: Object.entries(
        buildQualityFeatureContributions({
          avgScore,
          passRate,
          projectedDelta,
          previousAverageScore,
          previousPassRate,
        }),
      ).map(([feature, impact]) => ({
        feature,
        impact,
        direction: impact > 0 ? "positive" : impact < 0 ? "negative" : "neutral",
      })),
      windowComparisons: {
        currentWindow: {
          averageScore: avgScore,
          passRate,
          totalEvents: group.total,
        },
        previousWindow: {
          averageScore: previousAverageScore,
          passRate: previousPassRate,
          totalEvents: Math.max(1, group.total - 1),
        },
      },
      riskDrivers: [
        passRate < 0.85 ? "pass_rate" : "average_score",
        group.total < 5 ? "low_sample_size" : "stable_sample_size",
      ],
      recommendedActions:
        trendDirection === "down"
          ? ["review_failed_samples", "run_replay_experiment"]
          : trendDirection === "up"
            ? ["observe_current_policy"]
            : ["monitor_next_window"],
      windowStart: range.from ?? null,
      windowEnd: range.to ?? null,
      basis: {
        totalEvents: group.total,
        passRate,
        averageScore: avgScore,
      },
    };
  });

  return c.json({
    items,
    total: items.length,
    filters: {
      from: range.from ?? null,
      to: range.to ?? null,
      metric: metric ?? "all",
      provider: provider ?? null,
      workflow: workflow ?? null,
      limit,
      modelVersion,
    },
  });
});

apiV2Routes.get("/quality/reports/advice", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const range = parseDateRange(c.req.query("from"), c.req.query("to"));
  if (range.error) {
    return c.json({ message: range.error }, 400);
  }
  const provider = normalizeQualityExternalFilter(c.req.query("provider"), { lowerCase: true });
  const workflow = normalizeQualityExternalFilter(c.req.query("workflow"));

  const groups = await repository.listQualityExternalMetricGroups(auth.tenantId, {
    from: range.from,
    to: range.to,
    provider,
    workflow,
    groupBy: "repo",
    limit: 50,
  });
  const mappedPolicy = mapQualityAutomationPolicy(
    await resolveQualityAutomationPolicy(auth.tenantId),
  );

  const items = await Promise.all(
    groups.map(async (group) => {
      const avgScore = fromRepositoryScore(group.averageScore);
      const passRate =
        group.total > 0 ? Number((group.passed / group.total).toFixed(6)) : 0;
      const severity = deriveQualityAdviceSeverity(avgScore, passRate);
      const adviceId = buildQualityAdviceId({
        project: group.value,
        provider,
        workflow,
        from: range.from ?? null,
        to: range.to ?? null,
      });
      const latestExecution = await getLatestQualityAdviceExecution(auth.tenantId, adviceId);
      const explanation = buildQualityAdviceExplanation({
        project: group.value,
        severity,
        avgScore,
        passRate,
        totalEvents: group.total,
        policy: mappedPolicy,
      });
      const executionOptions = buildQualityAdviceExecutionOptions({
        severity,
        policy: mappedPolicy,
      });
      const regressionProbability = computeQualityRegressionProbability({
        avgScore,
        passRate,
        projectedDelta: avgScore - mappedPolicy.evaluationScoreThreshold,
        totalEvents: group.total,
      });
      const strategyRule = selectQualityAutomationStrategyRule({
        policy: mappedPolicy,
        metric: "accuracy",
        severity,
        trendDirection:
          avgScore < mappedPolicy.evaluationScoreThreshold ? "down" : "flat",
        provider,
        workflow,
        project: group.value,
        sampleCount: group.total,
        passRate,
        confidence: explanation.confidence,
        regressionProbability,
        replayRegressionCount: 0,
      });
      return {
        id: adviceId,
        project: group.value,
        severity,
        title:
          severity === "critical"
            ? "质量风险偏高，建议优先治理"
            : severity === "warn"
              ? "质量波动，建议跟踪趋势"
              : "质量稳定，建议持续观察",
        recommendation:
          severity === "critical"
            ? "优先检查失败样本、工作流配置与提示词回归。"
            : severity === "warn"
              ? "关注最近窗口得分下降趋势，并补充对比 run。"
            : "维持当前策略，并持续观测 scorecards。",
        explanation: explanation.summary,
        confidence: explanation.confidence,
        confidenceLabel: explanation.confidenceLabel,
        why: [
          explanation.primaryDriver,
          `basis_window_${group.total}`,
          `automation_${mappedPolicy.decision}`,
        ],
        automationReadiness:
          latestExecution?.status === "running"
            ? "execution_in_progress"
            : mappedPolicy.decision === "deny"
              ? "monitor_only"
              : mappedPolicy.decision === "require_approval"
                ? "manual_review"
                : "ready_for_execution",
        executionHint: {
          recommendedActionType: explanation.recommendedActionType,
          requiresDataset: explanation.recommendedActionType === "replay_experiment",
          priority:
            severity === "critical" ? "high" : severity === "warn" ? "medium" : "low",
          reason:
            executionOptions.find(
              (option) => option.actionType === explanation.recommendedActionType,
            )?.reason ?? "建议结合当前质量窗口选择治理动作。",
        },
        strategyMatrixMatch: strategyRule?.ruleId ?? null,
        recommendedPlan: {
          actionType: explanation.recommendedActionType,
          requiresApproval:
            strategyRule?.requiresApproval ?? (mappedPolicy.decision === "require_approval"),
          confidence: explanation.confidence,
          regressionProbability,
        },
        autoExecutionDecision:
          mappedPolicy.decision === "deny"
            ? "blocked"
            : strategyRule?.requiresApproval || mappedPolicy.decision === "require_approval"
              ? "approval_required"
              : "eligible",
        blockingReasons:
          mappedPolicy.decision === "deny"
            ? ["policy_denied"]
            : explanation.recommendedActionType === "replay_experiment"
              ? ["dataset_required_for_replay_experiment"]
              : [],
        basis: {
          totalEvents: group.total,
          passRate,
          averageScore: avgScore,
          from: range.from ?? null,
          to: range.to ?? null,
          provider: provider ?? null,
          workflow: workflow ?? null,
        },
        relatedMetrics: ["avgScore", "passRate", "totalEvents"],
        suggestedActions: ["scorecard_adjustment", "replay_experiment"],
        executionOptions,
        latestExecutionId: latestExecution?.id,
        latestExecutionStatus: latestExecution?.status,
      };
    }),
  );

  return c.json({
    items,
    total: items.length,
    filters: {
      from: range.from ?? null,
      to: range.to ?? null,
      provider: provider ?? null,
      workflow: workflow ?? null,
    },
  });
});

apiV2Routes.get("/quality/advice/executions", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const adviceId = firstNonEmptyString(c.req.query("adviceId"));
  const actionType = firstNonEmptyString(c.req.query("actionType"));
  const status = firstNonEmptyString(c.req.query("status"));
  const limit = toPositiveInteger(c.req.query("limit"), 50);
  const items = await repository.listQualityAdviceExecutions(auth.tenantId, {
    adviceId,
    actionType:
      actionType === "scorecard_adjustment" || actionType === "replay_experiment"
        ? actionType
        : undefined,
    status:
      status === "pending" ||
      status === "running" ||
      status === "completed" ||
      status === "failed" ||
      status === "cancelled"
        ? status
        : undefined,
    limit,
  });
  return c.json({
    items,
    total: items.length,
    filters: {
      adviceId: adviceId ?? null,
      actionType: actionType ?? null,
      status: status ?? null,
      limit,
    },
  });
});

apiV2Routes.get("/quality/advice/executions/:id", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const executionId = c.req.param("id")?.trim();
  if (!executionId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const item = await repository.getQualityAdviceExecutionById(auth.tenantId, executionId);
  if (!item) {
    return c.json({ message: `未找到建议执行记录：${executionId}` }, 404);
  }
  return c.json(item);
});

apiV2Routes.post("/quality/advice/:id/execute", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const adviceId = c.req.param("id")?.trim();
  if (!adviceId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const body = normalizeRecord(await c.req.json().catch(() => undefined));
  const actionType = firstNonEmptyString(body.actionType) as QualityAdviceActionType | undefined;
  const project = firstNonEmptyString(body.project);
  if (
    actionType !== "scorecard_adjustment" &&
    actionType !== "replay_experiment"
  ) {
    return c.json({ message: "actionType 必须是 scorecard_adjustment/replay_experiment 之一。" }, 400);
  }
  if (!project) {
    return c.json({ message: "project 必填且必须为非空字符串。" }, 400);
  }
  const now = new Date().toISOString();
  const metric =
    firstNonEmptyString(body.metric) &&
    QUALITY_METRIC_SET.has(firstNonEmptyString(body.metric) as string)
      ? (firstNonEmptyString(body.metric) as QualityMetric)
      : "accuracy";
  const candidateLabels =
    Array.isArray(body.candidateLabels) &&
    body.candidateLabels.every((item) => typeof item === "string" && item.trim().length > 0)
      ? Array.from(new Set(body.candidateLabels.map((item) => item.trim())))
      : undefined;
  const baselineVersionId = firstNonEmptyString(
    body.baselineVersionId,
    body.baseline_version_id,
  );
  if (
    (body.baselineVersionId !== undefined || body.baseline_version_id !== undefined) &&
    !baselineVersionId
  ) {
    return c.json({ message: "baselineVersionId 必须为非空字符串。" }, 400);
  }
  let record: QualityAdviceExecutionRecord = {
    id: crypto.randomUUID(),
    tenantId: auth.tenantId,
    adviceId,
    project,
    severity:
      body.severity === "critical" || body.severity === "warn" || body.severity === "info"
        ? body.severity
        : "warn",
    actionType,
    triggerSource: body.triggerSource === "automatic" ? "automatic" : "manual",
    status: "running",
    metric,
    datasetId: firstNonEmptyString(body.datasetId),
    candidateLabels,
    resultSummary:
      actionType === "scorecard_adjustment"
        ? {
            executionPayload: {
              metric,
              currentScore: toNumber(body.currentScore, 0),
              targetScore: Math.max(0, Math.min(100, toNumber(body.targetScore, 82))),
              warningScore: Math.max(0, Math.min(100, toNumber(body.warningScore, 72))),
              criticalScore: Math.max(0, Math.min(100, toNumber(body.criticalScore, 63))),
            },
          }
        : {
            executionPayload: {
              datasetId: firstNonEmptyString(body.datasetId) ?? null,
              baselineVersionId: baselineVersionId ?? null,
              candidateLabels: candidateLabels ?? [],
            },
          },
    requestedAt: now,
    startedAt: now,
    updatedAt: now,
  };
  record = await saveQualityAdviceExecution(record);
  record = await executeQualityAdviceExecution(record, {
    actorUserId: auth.userId,
    actorEmail: auth.email,
    continuationTrigger: "manual",
  });
  if (record.status === "failed") {
    return c.json({ message: record.error, execution: record }, 400);
  }
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.v2.quality.advice_execution_completed",
    level: "info",
    detail: `Quality advice execution ${record.id} completed.`,
    metadata: {
      requestId,
      adviceId,
      executionId: record.id,
      actionType: record.actionType,
      project,
      experimentId: record.experimentId ?? null,
    },
  });
  return c.json(record, 201);
});

apiV2Routes.post("/quality/advice/executions/:id/cancel", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const executionId = c.req.param("id")?.trim();
  if (!executionId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const existing = await repository.getQualityAdviceExecutionById(auth.tenantId, executionId);
  if (!existing) {
    return c.json({ message: `未找到建议执行记录：${executionId}` }, 404);
  }
  const updated = await saveQualityAdviceExecution({
    ...existing,
    status: "cancelled",
    finishedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  });
  return c.json(updated);
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

apiV2Routes.get("/replay/datasets/:id/versions", async (c) => {
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
  const mappedDataset = mapReplayDataset(dataset);
  const versions = await repository.listReplayBaselineVersions(auth.tenantId, datasetId);
  return c.json({
    datasetId,
    currentVersionId: mappedDataset.currentVersionId,
    currentVersionNumber: mappedDataset.currentVersionNumber,
    items: versions.map(mapReplayBaselineVersion),
    total: versions.length,
  });
});

apiV2Routes.get("/replay/datasets/:id/versions/:versionId/cases", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const datasetId = c.req.param("id")?.trim();
  const versionId = c.req.param("versionId")?.trim();
  if (!datasetId || !versionId) {
    return c.json({ message: "id 与 versionId 必须为非空字符串。" }, 400);
  }
  const dataset = await repository.getReplayDatasetById(auth.tenantId, datasetId);
  if (!dataset) {
    return c.json({ message: `未找到回放数据集：${datasetId}` }, 404);
  }
  const versions = await repository.listReplayBaselineVersions(auth.tenantId, datasetId);
  if (!versions.some((item) => item.id === versionId)) {
    return c.json({ message: `未找到回放数据集版本：${versionId}` }, 404);
  }
  const limit = toPositiveInteger(c.req.query("limit"), 500);
  if (limit > 5000) {
    return c.json({ message: "limit 必须是 1 到 5000 的整数。" }, 400);
  }
  const items = await loadReplayDatasetVersionCasesSnapshot({
    tenantId: auth.tenantId,
    datasetId,
    versionId,
    limit,
  });
  return c.json({
    datasetId,
    versionId,
    items,
    total: items.length,
  });
});

apiV2Routes.post("/replay/datasets/:id/versions", async (c) => {
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
  const validation = validateReplayDatasetVersionCreateInput({
    ...normalizeRecord(body),
    tenantId: auth.tenantId,
    replayDatasetId: datasetId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }
  const version = await repository.createReplayBaselineVersion(auth.tenantId, datasetId, {
    datasetRef: validation.data.versionDatasetId,
    model: validation.data.model,
    promptVersion: validation.data.promptVersion,
    scenarioCount: validation.data.sampleCount,
    metadata: validation.data.metadata,
    note: validation.data.note,
  });
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.v2.replay.dataset_version_created",
    level: "info",
    detail: `Created replay dataset version ${version.id} for dataset ${datasetId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      datasetId,
      versionId: version.id,
      version: version.version,
    },
  });
  return c.json(mapReplayBaselineVersion(version), 201);
});

apiV2Routes.post("/replay/datasets/:id/promote", async (c) => {
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
  const validation = validateReplayDatasetVersionPromoteInput({
    ...normalizeRecord(body),
    tenantId: auth.tenantId,
    replayDatasetId: datasetId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }
  const version = await repository.promoteReplayBaselineVersion(
    auth.tenantId,
    datasetId,
    validation.data.versionId,
  );
  if (!version) {
    return c.json({ message: `未找到回放数据集版本：${validation.data.versionId}` }, 404);
  }
  const refreshedDataset = await repository.getReplayDatasetById(auth.tenantId, datasetId);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.v2.replay.dataset_version_promoted",
    level: "info",
    detail: `Promoted replay dataset version ${version.id} for dataset ${datasetId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      datasetId,
      versionId: version.id,
      version: version.version,
    },
  });
  return c.json({
    dataset: refreshedDataset ? mapReplayDataset(refreshedDataset) : null,
    version: mapReplayBaselineVersion(version),
  });
});

apiV2Routes.post("/replay/experiments", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const validation = validateReplayExperimentCreateInput({
    ...normalizeRecord(await c.req.json().catch(() => undefined)),
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const dataset = await repository.getReplayDatasetById(
    auth.tenantId,
    validation.data.datasetId,
  );
  if (!dataset) {
    return c.json({ message: `未找到回放数据集：${validation.data.datasetId}` }, 404);
  }

  const now = new Date().toISOString();
  const baselineVersionId =
    validation.data.baselineVersionId ?? resolveReplayDatasetCurrentVersionId(dataset);
  const record = await saveReplayExperiment({
    id: crypto.randomUUID(),
    tenantId: auth.tenantId,
    name: validation.data.name,
    datasetId: validation.data.datasetId,
    baselineId: validation.data.baselineId,
    baselineVersionId,
    triggerSource: validation.data.triggerSource ?? "manual",
    executionMode: validation.data.autoRun === true ? "automatic" : "manual",
    status:
      (validation.data.runIds?.length ?? 0) > 0 || validation.data.autoRun === true
        ? "queued"
        : "draft",
    candidateLabels: validation.data.candidateLabels ?? [],
    sourceAdviceId: validation.data.sourceAdviceId,
    runIds: validation.data.runIds ?? [],
    createdAt: now,
    updatedAt: now,
  });
  const resolvedRecord =
    validation.data.autoRun === true
      ? await triggerReplayExperimentRuns({ tenantId: auth.tenantId, experiment: record })
      : record;
  return c.json(await mapReplayExperimentWithRuns(auth.tenantId, resolvedRecord), 201);
});

apiV2Routes.get("/replay/experiments", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const datasetId = firstNonEmptyString(c.req.query("datasetId"));
  const limit = toPositiveInteger(c.req.query("limit"), 50);
  const records = await repository.listReplayExperiments(auth.tenantId, {
    datasetId,
    limit,
  });
  const items = await Promise.all(
    records.map((record) => mapReplayExperimentWithRuns(auth.tenantId, record)),
  );
  return c.json({
    items,
    total: items.length,
  });
});

apiV2Routes.get("/replay/experiments/compare", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const experimentIds = Array.from(
    new Set(
      (c.req.query("experimentIds") ?? "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  );
  const datasetId = firstNonEmptyString(c.req.query("datasetId"));
  if (experimentIds.length < 2) {
    return c.json({ message: "experimentIds 至少需要提供 2 个实验 ID。" }, 400);
  }
  if (experimentIds.length > 10) {
    return c.json({ message: "experimentIds 最多支持 10 个实验 ID。" }, 400);
  }

  const resolved = await Promise.all(
    experimentIds.map(async (experimentId) => {
      const record = await getReplayExperimentById(auth.tenantId, experimentId);
      if (!record) {
        return null;
      }
      if (datasetId && record.datasetId !== datasetId) {
        return null;
      }
      return mapReplayExperimentWithRuns(auth.tenantId, record);
    }),
  );
  const items = resolved
    .filter(
      (item): item is Awaited<ReturnType<typeof mapReplayExperimentWithRuns>> => Boolean(item),
    )
    .map((item) => mapReplayExperimentComparisonItem(item));

  if (items.length < 2) {
    return c.json({ message: "可比较的 experiment 少于 2 个，请检查 ID 或 datasetId。" }, 404);
  }

  return c.json(
    buildReplayExperimentComparisonResponse({
      items,
      experimentIds,
      datasetId,
    }),
  );
});

apiV2Routes.get("/replay/experiments/:id", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const experimentId = c.req.param("id")?.trim();
  if (!experimentId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const record = await getReplayExperimentById(auth.tenantId, experimentId);
  if (!record) {
    return c.json({ message: `未找到回放实验：${experimentId}` }, 404);
  }
  return c.json(await mapReplayExperimentWithRuns(auth.tenantId, record));
});

apiV2Routes.patch("/replay/experiments/:id", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const experimentId = c.req.param("id")?.trim();
  if (!experimentId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const record = await getReplayExperimentById(auth.tenantId, experimentId);
  if (!record) {
    return c.json({ message: `未找到回放实验：${experimentId}` }, 404);
  }

  const validation = validateReplayExperimentUpdateInput({
    ...normalizeRecord(await c.req.json().catch(() => undefined)),
    tenantId: auth.tenantId,
    experimentId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const now = new Date().toISOString();
  const updatedFields: string[] = [];
  let updatedRecord = record;
  if (validation.data.name !== undefined) {
    updatedFields.push("name");
    updatedRecord = {
      ...updatedRecord,
      name: validation.data.name,
    };
  }
  if (validation.data.baselineVersionId !== undefined) {
    updatedFields.push("baselineVersionId");
    updatedRecord = {
      ...updatedRecord,
      baselineVersionId: validation.data.baselineVersionId,
    };
    rememberReplayExperimentBaselineVersionId(auth.tenantId, experimentId, validation.data.baselineVersionId);
  }
  if (validation.data.candidateLabels !== undefined) {
    updatedFields.push("candidateLabels");
    updatedRecord = {
      ...updatedRecord,
      candidateLabels: validation.data.candidateLabels,
    };
  }
  updatedRecord = await saveReplayExperiment({
    ...updatedRecord,
    updatedAt: now,
  });

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.v2.replay.experiment_updated",
    level: "info",
    detail: `Updated replay experiment ${experimentId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      experimentId,
      updatedFields,
    },
  });

  return c.json(await mapReplayExperimentWithRuns(auth.tenantId, updatedRecord));
});

apiV2Routes.post("/replay/experiments/:id/run", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const experimentId = c.req.param("id")?.trim();
  if (!experimentId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const body = normalizeRecord(await c.req.json().catch(() => undefined));
  let skipIfRunning = true;
  if (body.skipIfRunning !== undefined) {
    if (typeof body.skipIfRunning !== "boolean") {
      return c.json({ message: "skipIfRunning 必须为 boolean。" }, 400);
    }
    skipIfRunning = body.skipIfRunning;
  }
  let candidateLabels: string[] | undefined;
  if (body.candidateLabels !== undefined) {
    if (!Array.isArray(body.candidateLabels)) {
      return c.json({ message: "candidateLabels 必须为 string[]。" }, 400);
    }
    if (!body.candidateLabels.every((item) => typeof item === "string")) {
      return c.json({ message: "candidateLabels 必须为 string[]。" }, 400);
    }
    const resolved = Array.from(
      new Set(body.candidateLabels.map((item) => item.trim()).filter(Boolean)),
    );
    candidateLabels = resolved.length > 0 ? resolved : undefined;
  }
  const record = await getReplayExperimentById(auth.tenantId, experimentId);
  if (!record) {
    return c.json({ message: `未找到回放实验：${experimentId}` }, 404);
  }
  const updated = await triggerReplayExperimentRuns({
    tenantId: auth.tenantId,
    experiment: {
      ...record,
      executionMode: "automatic",
      triggerSource: record.triggerSource === "manual" ? "manual" : record.triggerSource,
      updatedAt: new Date().toISOString(),
    },
    candidateLabels,
    skipIfRunning,
  });
  return c.json(await mapReplayExperimentWithRuns(auth.tenantId, updated));
});

apiV2Routes.post("/replay/experiments/:id/cancel", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const experimentId = c.req.param("id")?.trim();
  if (!experimentId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const record = await getReplayExperimentById(auth.tenantId, experimentId);
  if (!record) {
    return c.json({ message: `未找到回放实验：${experimentId}` }, 404);
  }
  for (const runId of record.runIds) {
    const run = await repository.getReplayRunById(auth.tenantId, runId);
    if (run && (run.status === "pending" || run.status === "running")) {
      await repository.updateReplayRun(auth.tenantId, runId, {
        status: "cancelled",
        finishedAt: new Date().toISOString(),
      });
    }
  }
  const updated = await saveReplayExperiment({
    ...record,
    status: "cancelled",
    finishedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  });
  return c.json(await mapReplayExperimentWithRuns(auth.tenantId, updated));
});

apiV2Routes.get("/replay/experiments/:id/results", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const experimentId = c.req.param("id")?.trim();
  if (!experimentId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const record = await getReplayExperimentById(auth.tenantId, experimentId);
  if (!record) {
    return c.json({ message: `未找到回放实验：${experimentId}` }, 404);
  }
  return c.json(await mapReplayExperimentWithRuns(auth.tenantId, record));
});

apiV2Routes.get("/replay/experiments/:id/compare", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const experimentId = c.req.param("id")?.trim();
  if (!experimentId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const record = await getReplayExperimentById(auth.tenantId, experimentId);
  if (!record) {
    return c.json({ message: `未找到回放实验：${experimentId}` }, 404);
  }
  const runs = await listReplayExperimentRuns(auth.tenantId, record);
  return c.json(buildReplayExperimentComparison({ experiment: record, runs }));
});

apiV2Routes.get("/replay/experiments/:id/workflow", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const experimentId = c.req.param("id")?.trim();
  if (!experimentId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const record = await getReplayExperimentById(auth.tenantId, experimentId);
  if (!record) {
    return c.json({ message: `未找到回放实验：${experimentId}` }, 404);
  }
  const runs = await listReplayExperimentRuns(auth.tenantId, record);
  return c.json(buildReplayExperimentWorkflow({ experiment: record, runs }));
});

apiV2Routes.get("/replay/experiments/:id/artifacts", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const experimentId = c.req.param("id")?.trim();
  if (!experimentId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }
  const record = await getReplayExperimentById(auth.tenantId, experimentId);
  if (!record) {
    return c.json({ message: `未找到回放实验：${experimentId}` }, 404);
  }
  const items = (
    await Promise.all(
      record.runIds.map(async (runId) => {
        const artifacts = await repository.listReplayArtifacts(auth.tenantId, runId);
        return artifacts.map((artifact) => ({
          ...artifact,
          runId,
        }));
      }),
    )
  ).flat();
  return c.json({
    experimentId,
    datasetId: record.datasetId,
    items,
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
  const baselineVersionId =
    validation.data.baselineVersionId ?? resolveReplayDatasetCurrentVersionId(dataset);
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
    ...(baselineVersionId ? { baselineVersionId } : {}),
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
        ...(baselineVersionId ? { baselineVersionId } : {}),
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

apiV2Routes.get("/residency/kms-key-mappings", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const result = await repository.listResidencyKmsKeyMappings(auth.tenantId);
  return c.json({
    items: result.items,
    total: result.total,
  });
});

apiV2Routes.put("/residency/kms-key-mappings", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const body = await c.req.json().catch(() => undefined);
  const validation = validateResidencyKmsKeyMappingUpsertInput(body);
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }
  const result = await repository.replaceResidencyKmsKeyMappings(
    auth.tenantId,
    validation.data
  );
  const requestId = c.get("requestId");
  await repository.appendAuditLog({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.residency.kms_key_mappings_upserted",
    level: "info",
    detail: "Updated residency KMS key mappings.",
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      count: result.total,
    },
  });
  return c.json({
    items: result.items,
    total: result.total,
  });
});

apiV2Routes.get("/residency/archive-region-policies", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const result = await repository.listResidencyArchiveRegionPolicies(auth.tenantId);
  return c.json({
    items: result.items,
    total: result.total,
  });
});

apiV2Routes.put("/residency/archive-region-policies", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const body = await c.req.json().catch(() => undefined);
  const validation = validateResidencyArchiveRegionPolicyUpsertInput(body);
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }
  const result = await repository.replaceResidencyArchiveRegionPolicies(
    auth.tenantId,
    validation.data
  );
  const requestId = c.get("requestId");
  await repository.appendAuditLog({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.residency.archive_region_policies_upserted",
    level: "info",
    detail: "Updated residency archive region policies.",
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      count: result.total,
    },
  });
  return c.json({
    items: result.items,
    total: result.total,
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
