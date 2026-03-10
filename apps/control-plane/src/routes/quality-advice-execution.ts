import {
  getControlPlaneRepository,
  type QualityAdviceExecution,
} from "../data/repository";
import { enqueueReplayJobExecution } from "./replay";

const repository = getControlPlaneRepository();

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

function normalizeDistinctStrings(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const items = value
    .map((item) => firstNonEmptyString(item))
    .filter((item): item is string => Boolean(item));
  return Array.from(new Set(items));
}

function toNumber(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function clampScore(score: number): number {
  if (!Number.isFinite(score)) {
    return 0;
  }
  return Math.max(0, Math.min(100, Number(score.toFixed(4))));
}

function resolveReplayDatasetCurrentVersionId(dataset: {
  metadata?: Record<string, unknown>;
}): string | undefined {
  const metadata = normalizeRecord(dataset.metadata);
  return firstNonEmptyString(
    metadata.currentVersionId,
    metadata.current_version_id,
  );
}

function buildScorecardAdjustmentPayload(execution: QualityAdviceExecution) {
  const resultSummary = normalizeRecord(execution.resultSummary);
  const executionPayload = normalizeRecord(resultSummary.executionPayload);
  const metric =
    firstNonEmptyString(
      executionPayload.metric,
      execution.metric,
      execution.scorecardKey,
    ) ?? "accuracy";
  const currentScore = clampScore(
    toNumber(executionPayload.currentScore, toNumber(resultSummary.currentScore, 0)),
  );
  const targetScore = clampScore(
    toNumber(
      executionPayload.targetScore,
      currentScore > 0 ? Math.max(80, currentScore + 10) : 80,
    ),
  );
  const warningScore = clampScore(
    Math.min(
      targetScore,
      toNumber(executionPayload.warningScore, Math.max(0, targetScore - 10)),
    ),
  );
  const criticalScore = clampScore(
    Math.min(
      warningScore,
      toNumber(executionPayload.criticalScore, Math.max(0, warningScore - 10)),
    ),
  );
  return {
    metric,
    currentScore,
    targetScore,
    warningScore,
    criticalScore,
  };
}

async function failQualityAdviceExecution(
  execution: QualityAdviceExecution,
  options: {
    continuationTrigger:
      | "automatic_allowed"
      | "approval_approved"
      | "approval_rejected"
      | "manual";
    approvalRequestId?: string;
    actorUserId?: string;
    actorEmail?: string;
    error: string;
  },
): Promise<QualityAdviceExecution> {
  const now = new Date().toISOString();
  const resultSummary = normalizeRecord(execution.resultSummary);
  return repository.upsertQualityAdviceExecution(execution.tenantId, {
    id: execution.id,
    adviceId: execution.adviceId,
    project: execution.project,
    severity: execution.severity,
    actionType: execution.actionType,
    triggerSource: execution.triggerSource,
    status: "failed",
    metric: execution.metric,
    datasetId: execution.datasetId,
    experimentId: execution.experimentId,
    candidateLabels: execution.candidateLabels,
    scorecardKey: execution.scorecardKey,
    resultSummary: {
      ...resultSummary,
      continuationTrigger: options.continuationTrigger,
      approvalRequestId:
        options.approvalRequestId ??
        firstNonEmptyString(resultSummary.approvalRequestId) ??
        null,
      reviewedByUserId: options.actorUserId ?? null,
      reviewedByEmail: options.actorEmail ?? null,
    },
    error: options.error,
    requestedAt: execution.requestedAt,
    startedAt: execution.startedAt ?? now,
    finishedAt: now,
    updatedAt: now,
  });
}

async function executeReplayExperimentAdviceExecution(
  execution: QualityAdviceExecution,
  options: {
    actorUserId?: string;
    actorEmail?: string;
    continuationTrigger:
      | "automatic_allowed"
      | "approval_approved"
      | "manual";
    approvalRequestId?: string;
  },
): Promise<QualityAdviceExecution> {
  const now = new Date().toISOString();
  const resultSummary = normalizeRecord(execution.resultSummary);
  const executionPayload = normalizeRecord(resultSummary.executionPayload);
  const datasetId = firstNonEmptyString(executionPayload.datasetId, execution.datasetId);
  if (!datasetId) {
    return failQualityAdviceExecution(execution, {
      ...options,
      error: "datasetId 必填。",
    });
  }

  const dataset = await repository.getReplayDatasetById(execution.tenantId, datasetId);
  if (!dataset) {
    return failQualityAdviceExecution(execution, {
      ...options,
      error: `未找到回放数据集：${datasetId}`,
    });
  }

  const baselineVersionId =
    firstNonEmptyString(
      executionPayload.baselineVersionId,
      executionPayload.baseline_version_id,
      resultSummary.baselineVersionId,
      resultSummary.baseline_version_id,
    ) ?? resolveReplayDatasetCurrentVersionId(dataset);
  const candidateLabels = [
    ...(execution.candidateLabels ?? []),
    ...normalizeDistinctStrings(executionPayload.candidateLabels),
  ];
  const resolvedCandidateLabels =
    candidateLabels.length > 0 ? Array.from(new Set(candidateLabels)) : ["candidate"];
  const experimentId = firstNonEmptyString(execution.experimentId) ?? crypto.randomUUID();
  const createdAt = execution.startedAt ?? now;

  let experiment = await repository.upsertReplayExperiment(execution.tenantId, {
    id: experimentId,
    name: `Advice ${execution.project}`,
    datasetId: dataset.id,
    baselineId: dataset.id,
    baselineVersionId,
    triggerSource: "quality_advice",
    executionMode: "automatic",
    status: "queued",
    candidateLabels: resolvedCandidateLabels,
    sourceAdviceId: execution.adviceId,
    runIds: [],
    startedAt: createdAt,
    createdAt,
    updatedAt: now,
  });

  const runIds = new Set(experiment.runIds);
  for (const candidateLabel of resolvedCandidateLabels) {
    const replayRun = await repository.createReplayRun(execution.tenantId, {
      datasetId: dataset.id,
      parameters: {
        experimentId,
        candidateLabel,
        triggerSource: "quality_advice",
        executionSource: "dataset_cases",
        ...(execution.metric ? { metric: execution.metric } : {}),
        ...(baselineVersionId ? { baselineVersionId } : {}),
      },
      summary: {
        totalCases: 12,
        candidateLabel,
        ...(execution.metric ? { metric: execution.metric } : {}),
        ...(baselineVersionId ? { baselineVersionId } : {}),
      },
      startedAt: now,
      createdAt: now,
    });
    enqueueReplayJobExecution(execution.tenantId, replayRun.id);
    runIds.add(replayRun.id);
  }

  experiment = await repository.upsertReplayExperiment(execution.tenantId, {
    id: experiment.id,
    name: experiment.name,
    datasetId: experiment.datasetId,
    baselineId: experiment.baselineId,
    baselineVersionId,
    triggerSource: experiment.triggerSource,
    executionMode: experiment.executionMode,
    status: experiment.status,
    candidateLabels: experiment.candidateLabels,
    sourceAdviceId: experiment.sourceAdviceId,
    runIds: [...runIds],
    startedAt: experiment.startedAt ?? createdAt,
    createdAt: experiment.createdAt,
    updatedAt: now,
  });

  return repository.upsertQualityAdviceExecution(execution.tenantId, {
    id: execution.id,
    adviceId: execution.adviceId,
    project: execution.project,
    severity: execution.severity,
    actionType: execution.actionType,
    triggerSource: execution.triggerSource,
    status: "completed",
    metric: execution.metric,
    datasetId: dataset.id,
    experimentId: experiment.id,
    candidateLabels: resolvedCandidateLabels,
    scorecardKey: execution.scorecardKey,
    resultSummary: {
      ...resultSummary,
      executionPayload: {
        ...executionPayload,
        datasetId: dataset.id,
        baselineVersionId: baselineVersionId ?? null,
        candidateLabels: resolvedCandidateLabels,
      },
      continuationTrigger: options.continuationTrigger,
      approvalRequestId:
        options.approvalRequestId ??
        firstNonEmptyString(resultSummary.approvalRequestId) ??
        null,
      appliedByUserId: options.actorUserId ?? null,
      appliedByEmail: options.actorEmail ?? null,
      experimentId: experiment.id,
      datasetCurrentVersionId: resolveReplayDatasetCurrentVersionId(dataset) ?? null,
      runIds: [...runIds],
      candidateLabels: resolvedCandidateLabels,
      baselineVersionId: baselineVersionId ?? null,
      completedAt: now,
    },
    error: null,
    requestedAt: execution.requestedAt,
    startedAt: execution.startedAt ?? createdAt,
    finishedAt: now,
    updatedAt: now,
  });
}

export async function executeQualityAdviceExecution(
  execution: QualityAdviceExecution,
  options: {
    actorUserId?: string;
    actorEmail?: string;
    continuationTrigger:
      | "automatic_allowed"
      | "approval_approved"
      | "manual";
    approvalRequestId?: string;
  },
): Promise<QualityAdviceExecution> {
  const now = new Date().toISOString();
  const resultSummary = normalizeRecord(execution.resultSummary);
  if (execution.actionType === "replay_experiment") {
    return executeReplayExperimentAdviceExecution(execution, options);
  }
  if (execution.actionType !== "scorecard_adjustment") {
    return failQualityAdviceExecution(execution, {
      ...options,
      error: "quality_advice_action_not_supported",
    });
  }

  const payload = buildScorecardAdjustmentPayload(execution);
  const saved = await repository.upsertQualityScorecard(execution.tenantId, {
    scorecardKey: payload.metric,
    title: `${payload.metric} 质量评分卡`,
    score: Math.max(0, Math.min(1, payload.targetScore / 100)),
    dimensions: {
      warningScore: Math.max(0, Math.min(1, payload.warningScore / 100)),
      criticalScore: Math.max(0, Math.min(1, payload.criticalScore / 100)),
      weight: 1,
    },
    metadata: {
      adviceId: execution.adviceId,
      executionId: execution.id,
      updatedByUserId: options.actorUserId,
      updatedByEmail: options.actorEmail,
      project: execution.project,
      continuationTrigger: options.continuationTrigger,
      approvalRequestId:
        options.approvalRequestId ??
        firstNonEmptyString(resultSummary.approvalRequestId) ??
        null,
    },
    updatedAt: now,
  });

  return repository.upsertQualityAdviceExecution(execution.tenantId, {
    id: execution.id,
    adviceId: execution.adviceId,
    project: execution.project,
    severity: execution.severity,
    actionType: execution.actionType,
    triggerSource: execution.triggerSource,
    status: "completed",
    metric: execution.metric,
    datasetId: execution.datasetId,
    experimentId: execution.experimentId,
    candidateLabels: execution.candidateLabels,
    scorecardKey: saved.scorecardKey,
    resultSummary: {
      ...resultSummary,
      executionPayload: payload,
      continuationTrigger: options.continuationTrigger,
      approvalRequestId:
        options.approvalRequestId ??
        firstNonEmptyString(resultSummary.approvalRequestId) ??
        null,
      appliedByUserId: options.actorUserId ?? null,
      appliedByEmail: options.actorEmail ?? null,
      scorecardKey: saved.scorecardKey,
      targetScore: payload.targetScore,
      warningScore: payload.warningScore,
      criticalScore: payload.criticalScore,
      completedAt: now,
    },
    error: null,
    requestedAt: execution.requestedAt,
    startedAt: execution.startedAt ?? now,
    finishedAt: now,
    updatedAt: now,
  });
}

export async function continueQualityAdviceExecutionFromApproval(input: {
  tenantId: string;
  approvalRequestId: string;
  decision: "approved" | "rejected";
  actorUserId?: string;
  actorEmail?: string;
}): Promise<QualityAdviceExecution | null> {
  const execution = await repository.getQualityAdviceExecutionByApprovalRequestId(
    input.tenantId,
    input.approvalRequestId,
  );
  if (!execution) {
    return null;
  }
  if (execution.status === "completed" || execution.status === "cancelled") {
    return execution;
  }
  if (input.decision === "rejected") {
    return failQualityAdviceExecution(execution, {
      continuationTrigger: "approval_rejected",
      approvalRequestId: input.approvalRequestId,
      actorUserId: input.actorUserId,
      actorEmail: input.actorEmail,
      error: "automation_approval_rejected",
    });
  }
  return executeQualityAdviceExecution(execution, {
    actorUserId: input.actorUserId,
    actorEmail: input.actorEmail,
    continuationTrigger: "approval_approved",
    approvalRequestId: input.approvalRequestId,
  });
}
