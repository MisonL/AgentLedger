import type {
  AlertExternalLinkBatchRetryResponse,
  AlertExternalLinkFailureResponse,
  AlertExternalLinkOpsItem,
  AlertExternalLinkOpsResponse,
  AlertItem,
  AlertListInput,
  AlertListResponse,
  AlertMutableStatus,
  AlertOrchestrationChannel,
  AlertOrchestrationDispatchMode,
  AlertOrchestrationEscalationReason,
  AlertOrchestrationEventType,
  AlertOrchestrationExecutionListInput,
  AlertOrchestrationExecutionListResponse,
  AlertOrchestrationExecutionLog,
  AlertOrchestrationRule,
  AlertOrchestrationRuleListInput,
  AlertOrchestrationRuleListResponse,
  AlertOrchestrationRuleUpsertInput,
  AlertOrchestrationSimulateInput,
  AlertOrchestrationSimulationResponse,
  AuthExternalExchangeInput,
  AuthLoginInput,
  AuthLoginResponse,
  AuthProviderItem,
  AuthProviderListResponse,
  AuthRefreshInput,
  AuthRefreshResponse,
  AuthTokens,
  CreateSourceInput,
  AgentRuntimeConfigResponse,
  AgentRuntimeStatus,
  AgentRuntimeView,
  AgentRuntimeViewListResponse,
  AgentRelease,
  AgentReleaseArtifact,
  AgentReleaseChannel,
  AgentReleaseCheckBatchPreviewInput,
  AgentReleaseCheckBatchPreviewResponse,
  AgentReleaseCheckPreviewInput,
  AgentReleaseCheckPreviewResponse,
  AgentReleaseCheckSelectionReason,
  AgentReleaseListResponse,
  DataResidencyMode,
  DownloadFile,
  ExportFormat,
  AuditDlpMode,
  HeatmapCell,
  IntegrationDlqMessageListResponse,
  IntegrationDlqRecoveryJob,
  IntegrationDlqRecoveryJobListResponse,
  IntegrationAlertFailureReportResponse,
  IntegrationAlertFailureTrendResponse,
  IntegrationDlqReplayResponse,
  McpApprovalListInput,
  McpApprovalMode,
  McpApprovalListResponse,
  McpApprovalCreateInput,
  McpApprovalRequest,
  McpApprovalReviewInput,
  McpApprovalStage,
  McpApprovalWorkflow,
  McpApprovalWorkflowNode,
  McpApprovalWorkflowNodeSnapshot,
  McpApprovalWorkflowTransition,
  McpApprovalWorkflowTransitionPreview,
  McpEvaluateInput,
  McpEvaluateResult,
  McpInvocationCreateInput,
  McpInvocationListInput,
  McpInvocationListResponse,
  McpInvocationAudit,
  McpRiskLevel,
  McpToolDecision,
  McpToolPolicyListInput,
  McpToolPolicyListResponse,
  McpToolPolicyUpsertInput,
  McpToolPolicy,
  OpenPlatformApiKey,
  OpenPlatformApiKeyListInput,
  OpenPlatformApiKeyListResponse,
  OpenPlatformApiKeyStatus,
  OpenPlatformApiKeyUpsertInput,
  OpenPlatformAutomationPolicy,
  OpenPlatformAutomationPolicySimulationInput,
  OpenPlatformAutomationPolicySimulationResponse,
  OpenPlatformAutomationStrategyRule,
  OpenPlatformAutomationPolicyUpsertInput,
  OpenPlatformOpenApiSummary,
  OpenPlatformQualityDailyItem,
  OpenPlatformQualityDailyQueryInput,
  OpenPlatformQualityDailyResponse,
  OpenPlatformQualityForecastResponse,
  OpenPlatformQualityAdviceResponse,
  OpenPlatformQualityAdviceExecution,
  OpenPlatformQualityAdviceExecutionListResponse,
  OpenPlatformQualityDailyStatus,
  OpenPlatformQualityProjectTrendItem,
  OpenPlatformQualityProjectTrendQueryInput,
  OpenPlatformQualityProjectTrendResponse,
  OpenPlatformQualityProjectTrendSummary,
  OpenPlatformQualityScorecard,
  OpenPlatformQualityScorecardListInput,
  OpenPlatformQualityScorecardListResponse,
  OpenPlatformReplayArtifact,
  OpenPlatformReplayArtifactListResponse,
  OpenPlatformReplayDataset,
  OpenPlatformReplayDatasetVersion,
  OpenPlatformReplayDatasetVersionCreateInput,
  OpenPlatformReplayDatasetVersionListResponse,
  OpenPlatformReplayDatasetVersionPromoteInput,
  OpenPlatformReplayDatasetVersionPromoteResponse,
  OpenPlatformReplayDatasetCase,
  OpenPlatformReplayDatasetVersionCaseListResponse,
  OpenPlatformReplayDatasetCaseListResponse,
  OpenPlatformReplayDatasetMaterializeInput,
  OpenPlatformReplayDatasetMaterializeResponse,
  OpenPlatformReplayDatasetCaseReplaceInput,
  OpenPlatformReplayDatasetCreateInput,
  OpenPlatformReplayDatasetListInput,
  OpenPlatformReplayDatasetListResponse,
  OpenPlatformReplayBaseline,
  OpenPlatformReplayBaselineListInput,
  OpenPlatformReplayBaselineListResponse,
  OpenPlatformReplayDiffItem,
  OpenPlatformReplayDiffQueryInput,
  OpenPlatformReplayDiffResponse,
  OpenPlatformReplayDiffVerdict,
  OpenPlatformReplayJob,
  OpenPlatformReplayJobListInput,
  OpenPlatformReplayJobListResponse,
  OpenPlatformReplayJobStatus,
  OpenPlatformReplayRun,
  OpenPlatformReplayRunCreateInput,
  OpenPlatformReplayExperiment,
  OpenPlatformReplayExperimentArtifactListResponse,
  OpenPlatformReplayExperimentBatchCompareResponse,
  OpenPlatformReplayExperimentCompareResponse,
  OpenPlatformReplayExperimentListResponse,
  OpenPlatformReplayExperimentWorkflowResponse,
  OpenPlatformReplayRunListInput,
  OpenPlatformReplayRunListResponse,
  OpenPlatformReplayRunStatus,
  OpenPlatformWebhook,
  OpenPlatformWebhookListInput,
  OpenPlatformWebhookListResponse,
  OpenPlatformWebhookReplayInput,
  OpenPlatformWebhookReplayResult,
  OpenPlatformWebhookUpsertInput,
  AuthUserProfile,
  PricingCatalog,
  PricingCatalogEntry,
  PricingCatalogUpsertInput,
  RegionDescriptor,
  ResidencyArchiveRegionPolicy,
  ResidencyArchiveRegionPolicyListResponse,
  ResidencyArchiveRegionPolicyUpsertInput,
  ResidencyKmsKeyMapping,
  ResidencyKmsKeyMappingListResponse,
  ResidencyKmsKeyMappingUpsertInput,
  ReplicationJobApproveInput,
  ReplicationJobCancelInput,
  ReplicationJobCreateInput,
  ReplicationJobListInput,
  ReplicationJobListResponse,
  ReplicationJobStatus,
  ReplicationJob,
  SystemConfigPackage,
  SystemConfigPackageCreateInput,
  SystemConfigPackageApproval,
  SystemConfigPackageApprovalCreateInput,
  SystemConfigPackageApprovalDecision,
  SystemConfigPackageApprovalListResponse,
  SystemConfigPackageListResponse,
  SystemConfigPackageRequiredApprovals,
  SystemConfigPackageTargetSelectors,
  SystemConfigWatchLatestInput,
  SystemConfigWatchLatestResponse,
  ResidencyRegionListResponse,
  RuleAssetCreateInput,
  RuleAssetListInput,
  RuleAssetListResponse,
  RuleRequiredApprovals,
  RuleAssetVersionDiffResponse,
  RuleApprovalCreateInput,
  RuleApprovalDecision,
  RuleApprovalListInput,
  RuleApprovalListResponse,
  RuleApproval,
  RuleAsset,
  RuleAssetVersionCreateInput,
  RuleAssetVersion,
  RuleLifecycleStatus,
  RulePublishInput,
  RuleRollbackInput,
  TokenPulseRoutePolicy,
  TokenPulseRuntimeEvent,
  TokenPulseRuntimeEventListInput,
  TokenPulseRuntimeEventListResponse,
  TokenPulseRuntimeEventStatus,
  SourceAccessMode,
  SourceConnectionTestResponse,
  SourceHealth,
  SourceMissingRegionListResponse,
  SourceParseFailure,
  SourceParseFailureListResponse,
  SourceParseFailureQueryInput,
  SourceRegionBackfillResult,
  Source,
  SourceListResponse,
  SourceType,
  Session,
  SessionDetailResponse,
  SessionEventListResponse,
  SessionSearchInput,
  SessionSearchResponse,
  SessionSourceFreshness,
  UpdateSourceInput,
  UsageExportDimension,
  UsageExportQueryInput,
  UsageAggregateFilters,
  UsageAggregateResponse,
  UsageDailyItem,
  UsageHeatmapResponse,
  UsageWeekItem,
  UsageModelItem,
  UsageMonthlyItem,
  UsageSessionBreakdownItem,
  UsageWeeklySummaryQueryInput,
  UsageWeeklySummaryResponse,
  TenantResidencyPolicy,
} from "./types";

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "").replace(
  /\/$/,
  ""
);
const AUTH_STORAGE_KEY = "agentledger.web-console.auth";

function shouldUseMockFallback(): boolean {
  return import.meta.env.DEV || import.meta.env.VITE_ENABLE_MOCK_FALLBACK === "true";
}

function toDateKey(iso: string): string {
  return iso.slice(0, 10);
}

const SOURCE_TYPES: SourceType[] = ["local", "ssh", "sync-cache"];
const AGENT_RUNTIME_STATUSES: AgentRuntimeStatus[] = [
  "online",
  "stale",
  "never_seen",
];
const ALERT_SEVERITIES = ["warning", "critical"] as const;
const ALERT_STATUSES = ["open", "acknowledged", "resolved"] as const;
const ALERT_MUTABLE_STATUSES = ["acknowledged", "resolved"] as const;
const ALERT_ORCHESTRATION_EVENT_TYPES = ["alert", "weekly"] as const;
const ALERT_ORCHESTRATION_DISPATCH_MODES = ["rule", "fallback"] as const;
const ALERT_ORCHESTRATION_ESCALATION_REASONS = ["sla_timeout"] as const;
const ALERT_ORCHESTRATION_CHANNELS = [
  "webhook",
  "wecom",
  "dingtalk",
  "feishu",
  "email",
  "email_webhook",
  "incident",
  "ticket",
] as const;
const EXPORT_FORMATS = ["json", "csv"] as const;
const USAGE_EXPORT_DIMENSIONS = [
  "daily",
  "weekly",
  "monthly",
  "models",
  "sessions",
  "heatmap",
] as const;
const USAGE_METRICS = ["tokens", "cost", "sessions"] as const;
const DATA_RESIDENCY_MODES: DataResidencyMode[] = ["single_region", "active_active"];
const REPLICATION_JOB_STATUSES: ReplicationJobStatus[] = [
  "pending",
  "running",
  "succeeded",
  "failed",
  "cancelled",
];
const RULE_LIFECYCLE_STATUSES: RuleLifecycleStatus[] = [
  "draft",
  "published",
  "deprecated",
];
const RULE_APPROVAL_DECISIONS: RuleApprovalDecision[] = ["approved", "rejected"];
const MCP_RISK_LEVELS: McpRiskLevel[] = ["low", "medium", "high"];
const MCP_TOOL_DECISIONS: McpToolDecision[] = ["allow", "deny", "require_approval"];
const MCP_APPROVAL_MODES: McpApprovalMode[] = ["single_stage", "two_stage", "multi_stage"];
const MCP_APPROVAL_STATUSES = ["pending", "approved", "rejected"] as const;
export const OPEN_PLATFORM_QUALITY_AUTOMATION_TOOL_ID = "quality.replay.advice.execute";
const OPEN_PLATFORM_API_KEY_STATUSES: OpenPlatformApiKeyStatus[] = [
  "active",
  "disabled",
];
const OPEN_PLATFORM_QUALITY_DAILY_STATUSES: OpenPlatformQualityDailyStatus[] = [
  "pass",
  "warn",
  "fail",
];
const OPEN_PLATFORM_REPLAY_JOB_STATUSES: OpenPlatformReplayJobStatus[] = [
  "pending",
  "running",
  "completed",
  "failed",
  "cancelled",
];
const OPEN_PLATFORM_REPLAY_DIFF_VERDICTS: OpenPlatformReplayDiffVerdict[] = [
  "improved",
  "regressed",
  "unchanged",
];

function daysAgo(days: number): Date {
  const date = new Date();
  date.setUTCHours(0, 0, 0, 0);
  date.setUTCDate(date.getUTCDate() - days);
  return date;
}

function buildMockHeatmap(): UsageHeatmapResponse {
  const cells: HeatmapCell[] = [];

  for (let i = 83; i >= 0; i -= 1) {
    const date = daysAgo(i);
    const weekday = date.getUTCDay();
    const baseline = 4000 + (83 - i) * 65;
    const wave = Math.round(Math.abs(Math.sin((83 - i) / 5)) * 7000);
    const burst = weekday === 1 ? 2800 : weekday === 5 ? 1800 : 600;
    const tokens = baseline + wave + burst;
    const sessions = Math.max(1, Math.round(tokens / 2200));
    const cost = Number((tokens / 3200).toFixed(2));

    cells.push({
      date: date.toISOString(),
      tokens,
      sessions,
      cost,
    });
  }

  const summary = cells.reduce(
    (acc, cell) => {
      acc.tokens += cell.tokens;
      acc.sessions += cell.sessions;
      acc.cost += cell.cost;
      return acc;
    },
    { tokens: 0, sessions: 0, cost: 0 }
  );

  return {
    cells,
    summary: {
      tokens: summary.tokens,
      sessions: summary.sessions,
      cost: Number(summary.cost.toFixed(2)),
    },
  };
}

function buildMockSessions(input: SessionSearchInput): SessionSearchResponse {
  const date = input.from ? new Date(input.from) : daysAgo(0);
  const dateKey = toDateKey(date.toISOString());

  const items: Session[] = [
    {
      id: `${dateKey}-codex-1`,
      sourceId: "devbox-shanghai",
      tool: "Codex CLI",
      model: "gpt-5-codex",
      startedAt: `${dateKey}T01:20:00.000Z`,
      endedAt: `${dateKey}T01:47:00.000Z`,
      tokens: 8420,
      cost: 2.41,
    },
    {
      id: `${dateKey}-cursor-1`,
      sourceId: "macbook-pro-15",
      tool: "Cursor IDE",
      model: "claude-3.7-sonnet",
      startedAt: `${dateKey}T07:10:00.000Z`,
      endedAt: `${dateKey}T08:02:00.000Z`,
      tokens: 12860,
      cost: 4.76,
    },
    {
      id: `${dateKey}-qwen-1`,
      sourceId: "win-build-02",
      tool: "Qwen Code",
      model: "qwen3-coder",
      startedAt: `${dateKey}T12:15:00.000Z`,
      endedAt: `${dateKey}T12:39:00.000Z`,
      tokens: 5690,
      cost: 1.34,
    },
  ];

  return {
    items,
    total: items.length,
    nextCursor: null,
    sourceFreshness: [
      {
        sourceId: "devbox-shanghai",
        sourceName: "上海开发机",
        accessMode: "hybrid",
        lastSuccessAt: `${dateKey}T02:10:00.000Z`,
        lastFailureAt: null,
        failureCount: 0,
        avgLatencyMs: 118,
        freshnessMinutes: 6,
      },
      {
        sourceId: "macbook-pro-15",
        sourceName: "设计组 Mac",
        accessMode: "realtime",
        lastSuccessAt: `${dateKey}T08:03:00.000Z`,
        lastFailureAt: `${dateKey}T07:58:00.000Z`,
        failureCount: 1,
        avgLatencyMs: 156,
        freshnessMinutes: 11,
      },
    ],
  };
}

function isSourceType(value: unknown): value is SourceType {
  return typeof value === "string" && SOURCE_TYPES.includes(value as SourceType);
}

function isSourceAccessMode(value: unknown): value is SourceAccessMode {
  return typeof value === "string" && value.trim().length > 0;
}

function isSourceSync(value: unknown): boolean {
  return typeof value === "boolean" || (typeof value === "object" && value !== null);
}

function isSource(value: unknown): value is Source {
  if (!value || typeof value !== "object") {
    return false;
  }

  const source = value as Partial<Source>;
  const hasCompatibleAccessMode =
    source.accessMode === undefined || isSourceAccessMode(source.accessMode);
  const hasCompatibleSourceRegion =
    source.sourceRegion === undefined ||
    source.sourceRegion === null ||
    typeof source.sourceRegion === "string";
  const hasCompatibleSync = source.sync === undefined || isSourceSync(source.sync);
  const hasCompatibleSyncCron =
    source.syncCron === undefined || source.syncCron === null || typeof source.syncCron === "string";
  const hasCompatibleSyncRetentionDays =
    source.syncRetentionDays === undefined ||
    source.syncRetentionDays === null ||
    (typeof source.syncRetentionDays === "number" &&
      Number.isInteger(source.syncRetentionDays) &&
      source.syncRetentionDays >= 0);

  return (
    typeof source.id === "string" &&
    typeof source.name === "string" &&
    typeof source.location === "string" &&
    typeof source.enabled === "boolean" &&
    typeof source.createdAt === "string" &&
    isSourceType(source.type) &&
    hasCompatibleSourceRegion &&
    hasCompatibleAccessMode &&
    hasCompatibleSync &&
    hasCompatibleSyncCron &&
    hasCompatibleSyncRetentionDays
  );
}

function isSourceMissingRegionListResponse(value: unknown): value is SourceMissingRegionListResponse {
  if (!isRecord(value)) {
    return false;
  }

  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isSource(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isSourceRegionBackfillResult(value: unknown): value is SourceRegionBackfillResult {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.tenantId === "string" &&
    typeof value.dryRun === "boolean" &&
    typeof value.primaryRegion === "string" &&
    typeof value.totalMissing === "number" &&
    Number.isInteger(value.totalMissing) &&
    value.totalMissing >= 0 &&
    typeof value.updated === "number" &&
    Number.isInteger(value.updated) &&
    value.updated >= 0 &&
    typeof value.skipped === "number" &&
    Number.isInteger(value.skipped) &&
    value.skipped >= 0 &&
    Array.isArray(value.items)
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isISODateString(value: unknown): value is string {
  return typeof value === "string" && !Number.isNaN(Date.parse(value));
}

function isAgentRuntimeStatus(value: unknown): value is AgentRuntimeStatus {
  return (
    typeof value === "string" &&
    AGENT_RUNTIME_STATUSES.includes(value as AgentRuntimeStatus)
  );
}

function isAgentRuntimeView(value: unknown): value is AgentRuntimeView {
  if (!isRecord(value)) {
    return false;
  }
  const deviceIdOk =
    value.deviceId === undefined ||
    value.deviceId === null ||
    typeof value.deviceId === "string";
  const versionOk =
    value.version === undefined ||
    value.version === null ||
    typeof value.version === "string";
  const lastConfigVersionOk =
    value.lastConfigVersion === undefined ||
    value.lastConfigVersion === null ||
    typeof value.lastConfigVersion === "string";
  const lastErrorOk =
    value.lastError === undefined ||
    value.lastError === null ||
    typeof value.lastError === "string";
  const ingestEndpointOk =
    value.ingestEndpoint === undefined ||
    value.ingestEndpoint === null ||
    typeof value.ingestEndpoint === "string";
  return (
    typeof value.id === "string" &&
    typeof value.agentId === "string" &&
    typeof value.tenantId === "string" &&
    deviceIdOk &&
    typeof value.displayName === "string" &&
    typeof value.hostname === "string" &&
    versionOk &&
    typeof value.sourceCount === "number" &&
    Number.isInteger(value.sourceCount) &&
    value.sourceCount >= 0 &&
    Array.isArray(value.sourceIds) &&
    value.sourceIds.every((item) => typeof item === "string") &&
    Array.isArray(value.sourceNames) &&
    value.sourceNames.every((item) => typeof item === "string") &&
    isAgentRuntimeStatus(value.runtimeStatus) &&
    (value.lastHeartbeatAt === null || isISODateString(value.lastHeartbeatAt)) &&
    (value.lastConfigFetchedAt === null || isISODateString(value.lastConfigFetchedAt)) &&
    lastConfigVersionOk &&
    lastErrorOk &&
    (value.lastIngestStatusCode === null ||
      (typeof value.lastIngestStatusCode === "number" &&
        Number.isInteger(value.lastIngestStatusCode) &&
        value.lastIngestStatusCode >= 0)) &&
    typeof value.lastAccepted === "number" &&
    Number.isInteger(value.lastAccepted) &&
    value.lastAccepted >= 0 &&
    typeof value.lastRejected === "number" &&
    Number.isInteger(value.lastRejected) &&
    value.lastRejected >= 0 &&
    typeof value.heartbeatIntervalSeconds === "number" &&
    Number.isInteger(value.heartbeatIntervalSeconds) &&
    value.heartbeatIntervalSeconds > 0 &&
    typeof value.staleAfterSeconds === "number" &&
    Number.isInteger(value.staleAfterSeconds) &&
    value.staleAfterSeconds > 0 &&
    (value.ingestProtocol === "http" || value.ingestProtocol === "grpc") &&
    ingestEndpointOk &&
    isISODateString(value.updatedAt)
  );
}

function isAgentRuntimeViewListResponse(
  value: unknown,
): value is AgentRuntimeViewListResponse {
  return (
    isRecord(value) &&
    Array.isArray(value.items) &&
    value.items.every((item) => isAgentRuntimeView(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isISODateString(value.generatedAt)
  );
}

function isAgentRuntimeConfigResponse(
  value: unknown,
): value is AgentRuntimeConfigResponse {
  if (!isRecord(value) || !isRecord(value.agent) || !isRecord(value.runtime) || !isRecord(value.bindings)) {
    return false;
  }
  const agent = value.agent;
  const runtime = value.runtime;
  const bindings = value.bindings;
  const deviceIdOk =
    agent.deviceId === undefined || agent.deviceId === null || typeof agent.deviceId === "string";
  const versionOk =
    agent.version === undefined || agent.version === null || typeof agent.version === "string";
  const ingestEndpointOk =
    runtime.ingestEndpoint === undefined ||
    runtime.ingestEndpoint === null ||
    typeof runtime.ingestEndpoint === "string";
  return (
    typeof value.tenantId === "string" &&
    typeof agent.agentId === "string" &&
    deviceIdOk &&
    typeof agent.hostname === "string" &&
    versionOk &&
    typeof agent.displayName === "string" &&
    typeof runtime.heartbeatIntervalSeconds === "number" &&
    Number.isInteger(runtime.heartbeatIntervalSeconds) &&
    runtime.heartbeatIntervalSeconds > 0 &&
    typeof runtime.staleAfterSeconds === "number" &&
    Number.isInteger(runtime.staleAfterSeconds) &&
    runtime.staleAfterSeconds > 0 &&
    (runtime.ingestProtocol === "http" || runtime.ingestProtocol === "grpc") &&
    ingestEndpointOk &&
    typeof runtime.sampleGenerateCount === "number" &&
    Number.isInteger(runtime.sampleGenerateCount) &&
    runtime.sampleGenerateCount >= 0 &&
    typeof bindings.sourceCount === "number" &&
    Number.isInteger(bindings.sourceCount) &&
    bindings.sourceCount >= 0 &&
    Array.isArray(bindings.sourceIds) &&
    bindings.sourceIds.every((item) => typeof item === "string") &&
    Array.isArray(bindings.sources) &&
    bindings.sources.every(
      (item) =>
        isRecord(item) &&
        typeof item.sourceId === "string" &&
        typeof item.name === "string" &&
        typeof item.accessMode === "string" &&
        typeof item.enabled === "boolean" &&
        typeof item.location === "string" &&
        (item.sourceRegion === undefined ||
          item.sourceRegion === null ||
          typeof item.sourceRegion === "string")
    ) &&
    typeof value.configVersion === "string" &&
    isISODateString(value.updatedAt)
  );
}

function isDataResidencyMode(value: unknown): value is DataResidencyMode {
  return typeof value === "string" && DATA_RESIDENCY_MODES.includes(value as DataResidencyMode);
}

function isReplicationJobStatus(value: unknown): value is ReplicationJobStatus {
  return (
    typeof value === "string" &&
    REPLICATION_JOB_STATUSES.includes(value as ReplicationJobStatus)
  );
}

function isRuleLifecycleStatus(value: unknown): value is RuleLifecycleStatus {
  return (
    typeof value === "string" &&
    RULE_LIFECYCLE_STATUSES.includes(value as RuleLifecycleStatus)
  );
}

function isRuleApprovalDecision(value: unknown): value is RuleApprovalDecision {
  return (
    typeof value === "string" &&
    RULE_APPROVAL_DECISIONS.includes(value as RuleApprovalDecision)
  );
}

function isMcpRiskLevel(value: unknown): value is McpRiskLevel {
  return typeof value === "string" && MCP_RISK_LEVELS.includes(value as McpRiskLevel);
}

function isMcpToolDecision(value: unknown): value is McpToolDecision {
  return typeof value === "string" && MCP_TOOL_DECISIONS.includes(value as McpToolDecision);
}

function isMcpApprovalMode(value: unknown): value is McpApprovalMode {
  return typeof value === "string" && MCP_APPROVAL_MODES.includes(value as McpApprovalMode);
}

function isMcpApprovalStage(value: unknown): value is McpApprovalStage {
  return typeof value === "string" && /^stage[1-9]\d*$/.test(value);
}

function isMcpApprovalStageConfig(
  value: unknown,
): value is {
  stage?: McpApprovalStage;
  requiredApprovals: number;
  roles: string[];
} {
  if (!isRecord(value)) {
    return false;
  }
  const stageOk =
    value.stage === undefined ||
    value.stage === null ||
    isMcpApprovalStage(value.stage);
  return (
    stageOk &&
    typeof value.requiredApprovals === "number" &&
    Number.isInteger(value.requiredApprovals) &&
    value.requiredApprovals >= 1 &&
    Array.isArray(value.roles) &&
    value.roles.every((item) => typeof item === "string")
  );
}

function isMcpApprovalStageSnapshot(
  value: unknown,
): value is {
  stage: McpApprovalStage;
  requiredApprovals: number;
  roles: string[];
  approvedApprovals: number;
  approvedByUserIds: string[];
} {
  return (
    isRecord(value) &&
    isMcpApprovalStage(value.stage) &&
    typeof value.requiredApprovals === "number" &&
    Number.isInteger(value.requiredApprovals) &&
    value.requiredApprovals >= 1 &&
    Array.isArray(value.roles) &&
    value.roles.every((item) => typeof item === "string") &&
    typeof value.approvedApprovals === "number" &&
    Number.isInteger(value.approvedApprovals) &&
    value.approvedApprovals >= 0 &&
    Array.isArray(value.approvedByUserIds) &&
    value.approvedByUserIds.every((item) => typeof item === "string")
  );
}

function isMcpApprovalWorkflowNode(
  value: unknown,
): value is McpApprovalWorkflowNode {
  if (!isRecord(value) || typeof value.nodeId !== "string") {
    return false;
  }
  const kindOk =
    value.kind === "approval" ||
    value.kind === "terminal_approved" ||
    value.kind === "terminal_rejected";
  const labelOk =
    value.label === undefined || value.label === null || typeof value.label === "string";
  const stageOk =
    value.stage === undefined || value.stage === null || isMcpApprovalStage(value.stage);
  const requiredApprovalsOk =
    value.requiredApprovals === undefined ||
    value.requiredApprovals === null ||
    (typeof value.requiredApprovals === "number" &&
      Number.isInteger(value.requiredApprovals) &&
      value.requiredApprovals >= 1);
  const rolesOk =
    value.roles === undefined ||
    value.roles === null ||
    (Array.isArray(value.roles) && value.roles.every((item) => typeof item === "string"));
  return kindOk && labelOk && stageOk && requiredApprovalsOk && rolesOk;
}

function isMcpApprovalWorkflowTransition(
  value: unknown,
): value is McpApprovalWorkflowTransition {
  if (!isRecord(value)) {
    return false;
  }
  const condition = value.condition;
  const isTimeWindow = (candidate: unknown) =>
    isRecord(candidate) &&
    typeof candidate.timezone === "string" &&
    typeof candidate.startTime === "string" &&
    typeof candidate.endTime === "string" &&
    (candidate.weekdays === undefined ||
      candidate.weekdays === null ||
      (Array.isArray(candidate.weekdays) &&
        candidate.weekdays.every(
          (item) =>
            typeof item === "number" &&
            Number.isInteger(item) &&
            item >= 1 &&
            item <= 7,
        )));
  const conditionOk =
    condition === undefined ||
    condition === null ||
    (isRecord(condition) &&
      (condition.riskLevelAtLeast === undefined ||
        condition.riskLevelAtLeast === null ||
        isMcpRiskLevel(condition.riskLevelAtLeast)) &&
      (condition.toolIds === undefined ||
        condition.toolIds === null ||
        (Array.isArray(condition.toolIds) &&
          condition.toolIds.every((item) => typeof item === "string"))) &&
      (condition.tenantRoles === undefined ||
        condition.tenantRoles === null ||
        (Array.isArray(condition.tenantRoles) &&
          condition.tenantRoles.every((item) => typeof item === "string"))) &&
      (condition.timeWindow === undefined ||
        condition.timeWindow === null ||
        isTimeWindow(condition.timeWindow)) &&
      (condition.default === undefined ||
        condition.default === null ||
        typeof condition.default === "boolean"));
  return (
    typeof value.fromNodeId === "string" &&
    typeof value.toNodeId === "string" &&
    conditionOk
  );
}

function isMcpApprovalWorkflow(
  value: unknown,
): value is McpApprovalWorkflow {
  return (
    isRecord(value) &&
    typeof value.entryNodeId === "string" &&
    Array.isArray(value.nodes) &&
    value.nodes.every((item) => isMcpApprovalWorkflowNode(item)) &&
    Array.isArray(value.transitions) &&
    value.transitions.every((item) => isMcpApprovalWorkflowTransition(item))
  );
}

function isMcpApprovalWorkflowNodeSnapshot(
  value: unknown,
): value is McpApprovalWorkflowNodeSnapshot {
  if (!isRecord(value)) {
    return false;
  }
  return (
    isMcpApprovalWorkflowNode(value) &&
    typeof value.approvedApprovals === "number" &&
    Number.isInteger(value.approvedApprovals) &&
    value.approvedApprovals >= 0 &&
    Array.isArray(value.approvedByUserIds) &&
    value.approvedByUserIds.every((item: unknown) => typeof item === "string") &&
    (value.rejectedByUserId === undefined ||
      value.rejectedByUserId === null ||
      typeof value.rejectedByUserId === "string")
  );
}

function isMcpApprovalWorkflowTransitionPreview(
  value: unknown,
): value is McpApprovalWorkflowTransitionPreview {
  return (
    isRecord(value) &&
    typeof value.fromNodeId === "string" &&
    (value.toNodeId === undefined ||
      value.toNodeId === null ||
      typeof value.toNodeId === "string") &&
    typeof value.matched === "boolean" &&
    (value.matchedBy === undefined ||
      value.matchedBy === null ||
      value.matchedBy === "condition" ||
      value.matchedBy === "default") &&
    (value.condition === undefined ||
      value.condition === null ||
      (isRecord(value.condition) &&
        (value.condition.riskLevelAtLeast === undefined ||
          value.condition.riskLevelAtLeast === null ||
          isMcpRiskLevel(value.condition.riskLevelAtLeast)) &&
        (value.condition.toolIds === undefined ||
          value.condition.toolIds === null ||
          (Array.isArray(value.condition.toolIds) &&
            value.condition.toolIds.every((item) => typeof item === "string"))) &&
        (value.condition.tenantRoles === undefined ||
          value.condition.tenantRoles === null ||
          (Array.isArray(value.condition.tenantRoles) &&
            value.condition.tenantRoles.every((item) => typeof item === "string"))) &&
        (value.condition.timeWindow === undefined ||
          value.condition.timeWindow === null ||
          (isRecord(value.condition.timeWindow) &&
            typeof value.condition.timeWindow.timezone === "string" &&
            typeof value.condition.timeWindow.startTime === "string" &&
            typeof value.condition.timeWindow.endTime === "string" &&
            (value.condition.timeWindow.weekdays === undefined ||
              value.condition.timeWindow.weekdays === null ||
              (Array.isArray(value.condition.timeWindow.weekdays) &&
                value.condition.timeWindow.weekdays.every(
                  (item) =>
                    typeof item === "number" &&
                    Number.isInteger(item) &&
                    item >= 1 &&
                    item <= 7,
                ))))) &&
        (value.condition.default === undefined ||
          value.condition.default === null ||
          typeof value.condition.default === "boolean")))
  );
}

function isMcpApprovalStatus(value: unknown): value is McpApprovalRequest["status"] {
  return (
    typeof value === "string" &&
    MCP_APPROVAL_STATUSES.includes(value as McpApprovalRequest["status"])
  );
}

function isOpenPlatformApiKeyStatus(value: unknown): value is OpenPlatformApiKeyStatus {
  return (
    typeof value === "string" &&
    OPEN_PLATFORM_API_KEY_STATUSES.includes(value as OpenPlatformApiKeyStatus)
  );
}

function isOpenPlatformQualityDailyStatus(
  value: unknown
): value is OpenPlatformQualityDailyStatus {
  return (
    typeof value === "string" &&
    OPEN_PLATFORM_QUALITY_DAILY_STATUSES.includes(value as OpenPlatformQualityDailyStatus)
  );
}

function isOpenPlatformReplayJobStatus(value: unknown): value is OpenPlatformReplayJobStatus {
  return (
    typeof value === "string" &&
    OPEN_PLATFORM_REPLAY_JOB_STATUSES.includes(value as OpenPlatformReplayJobStatus)
  );
}

function isOpenPlatformReplayDiffVerdict(
  value: unknown
): value is OpenPlatformReplayDiffVerdict {
  return (
    typeof value === "string" &&
    OPEN_PLATFORM_REPLAY_DIFF_VERDICTS.includes(value as OpenPlatformReplayDiffVerdict)
  );
}

function isRegionDescriptor(value: unknown): value is RegionDescriptor {
  if (!isRecord(value)) {
    return false;
  }

  const descriptionOk =
    value.description === undefined ||
    value.description === null ||
    typeof value.description === "string";
  return (
    typeof value.id === "string" &&
    typeof value.name === "string" &&
    typeof value.active === "boolean" &&
    descriptionOk
  );
}

function isResidencyRegionListResponse(value: unknown): value is ResidencyRegionListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isRegionDescriptor(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isTenantResidencyPolicy(value: unknown): value is TenantResidencyPolicy {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.tenantId === "string" &&
    isDataResidencyMode(value.mode) &&
    typeof value.primaryRegion === "string" &&
    Array.isArray(value.replicaRegions) &&
    value.replicaRegions.every((region) => typeof region === "string") &&
    typeof value.allowCrossRegionTransfer === "boolean" &&
    typeof value.requireTransferApproval === "boolean" &&
    isISODateString(value.updatedAt)
  );
}

function isResidencyKmsKeyMapping(
  value: unknown,
): value is ResidencyKmsKeyMapping {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.tenantId === "string" &&
    typeof value.regionId === "string" &&
    typeof value.keyProvider === "string" &&
    typeof value.keyRef === "string" &&
    typeof value.enabled === "boolean" &&
    isISODateString(value.updatedAt)
  );
}

function isResidencyKmsKeyMappingListResponse(
  value: unknown,
): value is ResidencyKmsKeyMappingListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isResidencyKmsKeyMapping(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isResidencyArchiveRegionPolicy(
  value: unknown,
): value is ResidencyArchiveRegionPolicy {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.tenantId === "string" &&
    typeof value.sourceRegion === "string" &&
    typeof value.archiveRegion === "string" &&
    typeof value.archiveClass === "string" &&
    typeof value.enabled === "boolean" &&
    isISODateString(value.updatedAt)
  );
}

function isResidencyArchiveRegionPolicyListResponse(
  value: unknown,
): value is ResidencyArchiveRegionPolicyListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isResidencyArchiveRegionPolicy(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isReplicationJob(value: unknown): value is ReplicationJob {
  if (!isRecord(value)) {
    return false;
  }

  const reasonOk = value.reason === undefined || value.reason === null || typeof value.reason === "string";
  const createdByOk =
    value.createdByUserId === undefined ||
    value.createdByUserId === null ||
    typeof value.createdByUserId === "string";
  const approvedByOk =
    value.approvedByUserId === undefined ||
    value.approvedByUserId === null ||
    typeof value.approvedByUserId === "string";
  const startedAtOk =
    value.startedAt === undefined || value.startedAt === null || isISODateString(value.startedAt);
  const finishedAtOk =
    value.finishedAt === undefined || value.finishedAt === null || isISODateString(value.finishedAt);

  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.sourceRegion === "string" &&
    typeof value.targetRegion === "string" &&
    isReplicationJobStatus(value.status) &&
    reasonOk &&
    createdByOk &&
    approvedByOk &&
    isRecord(value.metadata) &&
    isISODateString(value.createdAt) &&
    isISODateString(value.updatedAt) &&
    startedAtOk &&
    finishedAtOk
  );
}

function isReplicationJobListInput(value: unknown): value is ReplicationJobListInput {
  if (!isRecord(value)) {
    return false;
  }
  const statusOk = value.status === undefined || isReplicationJobStatus(value.status);
  const sourceRegionOk = value.sourceRegion === undefined || typeof value.sourceRegion === "string";
  const targetRegionOk = value.targetRegion === undefined || typeof value.targetRegion === "string";
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return statusOk && sourceRegionOk && targetRegionOk && limitOk;
}

function isReplicationJobListResponse(value: unknown): value is ReplicationJobListResponse {
  if (!isRecord(value)) {
    return false;
  }
  const filtersOk = isReplicationJobListInput(value.filters);
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isReplicationJob(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

function isSystemConfigPackageRequiredApprovals(
  value: unknown,
): value is SystemConfigPackageRequiredApprovals {
  return value === 0 || value === 1 || value === 2;
}

function isSystemConfigPackageApprovalDecision(
  value: unknown,
): value is SystemConfigPackageApprovalDecision {
  return value === "approved" || value === "rejected";
}

function isSystemConfigPackageTargetSelectors(
  value: unknown,
): value is SystemConfigPackageTargetSelectors {
  if (!isRecord(value)) {
    return false;
  }
  const arrayOfStrings = (input: unknown) =>
    input === undefined ||
    input === null ||
    (Array.isArray(input) && input.every((item) => typeof item === "string"));
  return (
    arrayOfStrings(value.agentIds) &&
    arrayOfStrings(value.deviceIds) &&
    arrayOfStrings(value.channels) &&
    arrayOfStrings(value.hostnames)
  );
}

function isSystemConfigPackage(value: unknown): value is SystemConfigPackage {
  if (!isRecord(value)) {
    return false;
  }
  const issuedAtOk =
    value.issuedAt === undefined ||
    value.issuedAt === null ||
    isISODateString(value.issuedAt);
  const publishedAtOk =
    value.publishedAt === undefined ||
    value.publishedAt === null ||
    isISODateString(value.publishedAt);
  return (
    typeof value.packageId === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.version === "string" &&
    issuedAtOk &&
    typeof value.signatureStatus === "string" &&
    isRecord(value.payload) &&
    isSystemConfigPackageTargetSelectors(value.targetSelectors) &&
    typeof value.requiresApproval === "boolean" &&
    isSystemConfigPackageRequiredApprovals(value.requiredApprovals) &&
    typeof value.isPublished === "boolean" &&
    publishedAtOk &&
    isISODateString(value.createdAt) &&
    isISODateString(value.updatedAt)
  );
}

function isSystemConfigPackageListResponse(
  value: unknown,
): value is SystemConfigPackageListResponse {
  if (!isRecord(value) || !isRecord(value.filters)) {
    return false;
  }
  const limitOk =
    value.filters.limit === undefined ||
    (typeof value.filters.limit === "number" &&
      Number.isInteger(value.filters.limit) &&
      value.filters.limit >= 1);
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isSystemConfigPackage(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    limitOk
  );
}

function isSystemConfigPackageCreateInput(
  value: unknown,
): value is SystemConfigPackageCreateInput {
  if (!isRecord(value)) {
    return false;
  }
  const issuedAtOk =
    value.issuedAt === undefined ||
    value.issuedAt === null ||
    isISODateString(value.issuedAt);
  const signatureStatusOk =
    value.signatureStatus === undefined ||
    value.signatureStatus === null ||
    typeof value.signatureStatus === "string";
  const selectorsOk =
    value.targetSelectors === undefined ||
    value.targetSelectors === null ||
    isSystemConfigPackageTargetSelectors(value.targetSelectors);
  const payloadOk =
    value.payload === undefined ||
    value.payload === null ||
    isRecord(value.payload);
  return (
    typeof value.version === "string" &&
    issuedAtOk &&
    signatureStatusOk &&
    typeof value.requiresApproval === "boolean" &&
    isSystemConfigPackageRequiredApprovals(value.requiredApprovals) &&
    selectorsOk &&
    payloadOk
  );
}

function isSystemConfigPackageApproval(
  value: unknown,
): value is SystemConfigPackageApproval {
  if (!isRecord(value)) {
    return false;
  }
  const commentOk =
    value.comment === undefined ||
    value.comment === null ||
    typeof value.comment === "string";
  return (
    typeof value.approvalId === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.packageId === "string" &&
    typeof value.version === "string" &&
    typeof value.approverUserId === "string" &&
    isSystemConfigPackageApprovalDecision(value.decision) &&
    commentOk &&
    isISODateString(value.createdAt) &&
    isISODateString(value.updatedAt)
  );
}

function isSystemConfigPackageApprovalListResponse(
  value: unknown,
): value is SystemConfigPackageApprovalListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isSystemConfigPackageApproval(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isAgentReleaseChannel(value: unknown): value is AgentReleaseChannel {
  return value === "stable" || value === "beta" || value === "canary";
}

function isAgentReleaseArtifact(value: unknown): value is AgentReleaseArtifact {
  if (!isRecord(value)) {
    return false;
  }
  const checksumOk =
    value.checksumSha256 === undefined ||
    value.checksumSha256 === null ||
    typeof value.checksumSha256 === "string";
  const signatureOk =
    value.signature === undefined ||
    value.signature === null ||
    typeof value.signature === "string";
  const signatureAlgorithmOk =
    value.signatureAlgorithm === undefined ||
    value.signatureAlgorithm === null ||
    value.signatureAlgorithm === "ed25519";
  const rolloutRingOk =
    value.rolloutRing === undefined ||
    value.rolloutRing === null ||
    typeof value.rolloutRing === "string";
  const rolloutPercentageOk =
    value.rolloutPercentage === undefined ||
    value.rolloutPercentage === null ||
    (typeof value.rolloutPercentage === "number" &&
      Number.isInteger(value.rolloutPercentage) &&
      value.rolloutPercentage >= 0 &&
      value.rolloutPercentage <= 100);
  const minAgentVersionOk =
    value.minAgentVersion === undefined ||
    value.minAgentVersion === null ||
    typeof value.minAgentVersion === "string";
  const fileNameOk =
    value.fileName === undefined ||
    value.fileName === null ||
    typeof value.fileName === "string";
  const installHintOk =
    value.installHint === undefined ||
    value.installHint === null ||
    typeof value.installHint === "string";
  return (
    typeof value.os === "string" &&
    typeof value.arch === "string" &&
    typeof value.downloadUrl === "string" &&
    checksumOk &&
    signatureOk &&
    signatureAlgorithmOk &&
    rolloutRingOk &&
    rolloutPercentageOk &&
    minAgentVersionOk &&
    fileNameOk &&
    installHintOk
  );
}

function isAgentRelease(value: unknown): value is AgentRelease {
  if (!isRecord(value)) {
    return false;
  }
  const notesOk =
    value.notes === undefined ||
    value.notes === null ||
    typeof value.notes === "string";
  return (
    typeof value.releaseId === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.version === "string" &&
    isAgentReleaseChannel(value.channel) &&
    notesOk &&
    isISODateString(value.publishedAt) &&
    Array.isArray(value.artifacts) &&
    value.artifacts.every((item) => isAgentReleaseArtifact(item)) &&
    isISODateString(value.createdAt) &&
    isISODateString(value.updatedAt)
  );
}

function isAgentReleaseListResponse(
  value: unknown,
): value is AgentReleaseListResponse {
  if (!isRecord(value) || !isRecord(value.filters)) {
    return false;
  }
  const limitOk =
    value.filters.limit === undefined ||
    (typeof value.filters.limit === "number" &&
      Number.isInteger(value.filters.limit) &&
      value.filters.limit >= 1);
  const channelOk =
    value.filters.channel === undefined ||
    isAgentReleaseChannel(value.filters.channel);
  const osOk =
    value.filters.os === undefined || typeof value.filters.os === "string";
  const archOk =
    value.filters.arch === undefined || typeof value.filters.arch === "string";
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isAgentRelease(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    limitOk &&
    channelOk &&
    osOk &&
    archOk
  );
}

function isAgentReleaseCheckSelectionReason(
  value: unknown,
): value is AgentReleaseCheckSelectionReason {
  return (
    value === "matched" ||
    value === "no_candidate" ||
    value === "ring_mismatch" ||
    value === "rollout_percentage_blocked" ||
    value === "min_agent_version_blocked"
  );
}

function isAgentReleaseCheckPreviewResponse(
  value: unknown,
): value is AgentReleaseCheckPreviewResponse {
  if (!isRecord(value)) {
    return false;
  }
  const latestReleaseOk =
    value.latestRelease === null ||
    value.latestRelease === undefined ||
    isAgentRelease(value.latestRelease);
  const selectedArtifactOk =
    value.selectedArtifact === null ||
    value.selectedArtifact === undefined ||
    isAgentReleaseArtifact(value.selectedArtifact);
  const evaluatedRingOk =
    value.evaluatedRing === undefined ||
    value.evaluatedRing === null ||
    typeof value.evaluatedRing === "string";
  const rolloutBucketOk =
    value.rolloutBucket === undefined ||
    value.rolloutBucket === null ||
    (typeof value.rolloutBucket === "number" &&
      Number.isInteger(value.rolloutBucket) &&
      value.rolloutBucket >= 0 &&
      value.rolloutBucket <= 99);
  const selectionReasonOk =
    value.selectionReason === undefined ||
    value.selectionReason === null ||
    isAgentReleaseCheckSelectionReason(value.selectionReason);
  return (
    isISODateString(value.checkedAt) &&
    typeof value.currentVersion === "string" &&
    isAgentReleaseChannel(value.channel) &&
    typeof value.os === "string" &&
    typeof value.arch === "string" &&
    typeof value.updateAvailable === "boolean" &&
    (value.comparison === "upgrade_available" ||
      value.comparison === "up_to_date" ||
      value.comparison === "ahead_of_latest" ||
      value.comparison === "no_release") &&
    latestReleaseOk &&
    selectedArtifactOk &&
    typeof value.instructions === "string" &&
    evaluatedRingOk &&
    rolloutBucketOk &&
    selectionReasonOk
  );
}

function isAgentReleaseBatchCheckPreviewResponse(
  value: unknown,
): value is AgentReleaseCheckBatchPreviewResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => {
      if (!isRecord(item) || typeof item.label !== "string") {
        return false;
      }
      return isAgentReleaseCheckPreviewResponse(item);
    }) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isRuleAsset(value: unknown): value is RuleAsset {
  if (!isRecord(value)) {
    return false;
  }
  const descriptionOk =
    value.description === undefined ||
    value.description === null ||
    typeof value.description === "string";
  const publishedVersionOk =
    value.publishedVersion === undefined ||
    value.publishedVersion === null ||
    (typeof value.publishedVersion === "number" &&
      Number.isInteger(value.publishedVersion) &&
      value.publishedVersion >= 0);
  const scopeBindingOk = isRecord(value.scopeBinding);
  const requiredApprovalsOk = isRuleRequiredApprovals(value.requiredApprovals);
  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.name === "string" &&
    descriptionOk &&
    isRuleLifecycleStatus(value.status) &&
    typeof value.latestVersion === "number" &&
    Number.isInteger(value.latestVersion) &&
    value.latestVersion >= 0 &&
    publishedVersionOk &&
    requiredApprovalsOk &&
    scopeBindingOk &&
    isISODateString(value.createdAt) &&
    isISODateString(value.updatedAt)
  );
}

function isRuleRequiredApprovals(
  value: unknown,
): value is RuleRequiredApprovals {
  return value === 1 || value === 2;
}

function isTokenPulseRoutePolicy(
  value: unknown,
): value is TokenPulseRoutePolicy {
  return (
    value === "round_robin" ||
    value === "latest_valid" ||
    value === "sticky_user"
  );
}

function isTokenPulseRuntimeEventStatus(
  value: unknown,
): value is TokenPulseRuntimeEventStatus {
  return (
    value === "success" ||
    value === "failure" ||
    value === "blocked" ||
    value === "timeout"
  );
}

function isRuleAssetListInput(value: unknown): value is RuleAssetListInput {
  if (!isRecord(value)) {
    return false;
  }
  const statusOk = value.status === undefined || isRuleLifecycleStatus(value.status);
  const keywordOk = value.keyword === undefined || typeof value.keyword === "string";
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return statusOk && keywordOk && limitOk;
}

function isRuleAssetListResponse(value: unknown): value is RuleAssetListResponse {
  if (!isRecord(value)) {
    return false;
  }
  const filtersOk = isRuleAssetListInput(value.filters);
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isRuleAsset(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

function isRuleAssetVersion(value: unknown): value is RuleAssetVersion {
  if (!isRecord(value)) {
    return false;
  }
  const changelogOk =
    value.changelog === undefined || value.changelog === null || typeof value.changelog === "string";
  const createdByOk =
    value.createdByUserId === undefined ||
    value.createdByUserId === null ||
    typeof value.createdByUserId === "string";
  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.assetId === "string" &&
    typeof value.version === "number" &&
    Number.isInteger(value.version) &&
    value.version > 0 &&
    typeof value.content === "string" &&
    changelogOk &&
    createdByOk &&
    isISODateString(value.createdAt)
  );
}

function isRuleAssetVersionListResponse(value: unknown): value is {
  items: RuleAssetVersion[];
  total: number;
} {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isRuleAssetVersion(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isRuleAssetVersionDiffResponse(
  value: unknown,
): value is RuleAssetVersionDiffResponse {
  if (!isRecord(value) || !isRecord(value.summary)) {
    return false;
  }
  const summary = value.summary;
  return (
    typeof value.assetId === "string" &&
    typeof value.fromVersion === "number" &&
    Number.isInteger(value.fromVersion) &&
    value.fromVersion > 0 &&
    typeof value.toVersion === "number" &&
    Number.isInteger(value.toVersion) &&
    value.toVersion > 0 &&
    Array.isArray(value.lines) &&
    value.lines.every((line) => {
      if (!isRecord(line)) {
        return false;
      }
      const typeOk =
        line.type === "added" ||
        line.type === "removed" ||
        line.type === "unchanged";
      const oldLineOk =
        line.oldLineNumber === undefined ||
        (typeof line.oldLineNumber === "number" &&
          Number.isInteger(line.oldLineNumber) &&
          line.oldLineNumber > 0);
      const newLineOk =
        line.newLineNumber === undefined ||
        (typeof line.newLineNumber === "number" &&
          Number.isInteger(line.newLineNumber) &&
          line.newLineNumber > 0);
      return (
        typeOk &&
        typeof line.content === "string" &&
        oldLineOk &&
        newLineOk
      );
    }) &&
    typeof summary.added === "number" &&
    Number.isInteger(summary.added) &&
    summary.added >= 0 &&
    typeof summary.removed === "number" &&
    Number.isInteger(summary.removed) &&
    summary.removed >= 0 &&
    typeof summary.unchanged === "number" &&
    Number.isInteger(summary.unchanged) &&
    summary.unchanged >= 0 &&
    typeof summary.changed === "boolean"
  );
}

function isTokenPulseRuntimeEvent(
  value: unknown,
): value is TokenPulseRuntimeEvent {
  if (!isRecord(value)) {
    return false;
  }
  const projectIdOk =
    value.projectId === undefined ||
    value.projectId === null ||
    typeof value.projectId === "string";
  const accountIdOk =
    value.accountId === undefined ||
    value.accountId === null ||
    typeof value.accountId === "string";
  const finishedAtOk =
    value.finishedAt === undefined ||
    value.finishedAt === null ||
    isISODateString(value.finishedAt);
  const errorCodeOk =
    value.errorCode === undefined ||
    value.errorCode === null ||
    typeof value.errorCode === "string";
  const costOk =
    value.cost === undefined ||
    value.cost === null ||
    (typeof value.cost === "string" &&
      /^\d+(?:\.\d{1,6})?$/.test(value.cost));
  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    projectIdOk &&
    typeof value.traceId === "string" &&
    typeof value.provider === "string" &&
    typeof value.model === "string" &&
    typeof value.resolvedModel === "string" &&
    isTokenPulseRoutePolicy(value.routePolicy) &&
    accountIdOk &&
    isTokenPulseRuntimeEventStatus(value.status) &&
    isISODateString(value.startedAt) &&
    finishedAtOk &&
    errorCodeOk &&
    costOk &&
    typeof value.idempotencyKey === "string" &&
    value.specVersion === "v1" &&
    typeof value.keyId === "string" &&
    isISODateString(value.createdAt)
  );
}

function isTokenPulseRuntimeEventListInput(
  value: unknown,
): value is TokenPulseRuntimeEventListInput {
  if (!isRecord(value)) {
    return false;
  }
  const traceIdOk =
    value.traceId === undefined || typeof value.traceId === "string";
  const providerOk =
    value.provider === undefined || typeof value.provider === "string";
  const statusOk =
    value.status === undefined || isTokenPulseRuntimeEventStatus(value.status);
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" &&
      Number.isInteger(value.limit) &&
      value.limit >= 1);
  const cursorOk =
    value.cursor === undefined || typeof value.cursor === "string";
  return traceIdOk && providerOk && statusOk && limitOk && cursorOk;
}

function isTokenPulseRuntimeEventListResponse(
  value: unknown,
): value is TokenPulseRuntimeEventListResponse {
  if (!isRecord(value)) {
    return false;
  }
  const nextCursorOk =
    value.nextCursor === null ||
    value.nextCursor === undefined ||
    typeof value.nextCursor === "string";
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isTokenPulseRuntimeEvent(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isTokenPulseRuntimeEventListInput(value.filters) &&
    nextCursorOk
  );
}

function isRuleApproval(value: unknown): value is RuleApproval {
  if (!isRecord(value)) {
    return false;
  }
  const approverEmailOk =
    value.approverEmail === undefined ||
    value.approverEmail === null ||
    typeof value.approverEmail === "string";
  const reasonOk =
    value.reason === undefined || value.reason === null || typeof value.reason === "string";
  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.assetId === "string" &&
    typeof value.version === "number" &&
    Number.isInteger(value.version) &&
    value.version > 0 &&
    typeof value.approverUserId === "string" &&
    approverEmailOk &&
    isRuleApprovalDecision(value.decision) &&
    reasonOk &&
    isISODateString(value.createdAt)
  );
}

function isRuleApprovalListInput(value: unknown): value is RuleApprovalListInput {
  if (!isRecord(value)) {
    return false;
  }
  const versionOk =
    value.version === undefined ||
    (typeof value.version === "number" && Number.isInteger(value.version) && value.version > 0);
  const decisionOk = value.decision === undefined || isRuleApprovalDecision(value.decision);
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return versionOk && decisionOk && limitOk;
}

function isRuleApprovalListResponse(value: unknown): value is RuleApprovalListResponse {
  if (!isRecord(value)) {
    return false;
  }
  const filtersOk = isRuleApprovalListInput(value.filters);
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isRuleApproval(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

function isMcpToolPolicy(value: unknown): value is McpToolPolicy {
  if (!isRecord(value)) {
    return false;
  }
  const reasonOk = value.reason === undefined || value.reason === null || typeof value.reason === "string";
  const approvalModeOk =
    value.approvalMode === undefined ||
    value.approvalMode === null ||
    isMcpApprovalMode(value.approvalMode);
  const approvalWorkflowOk =
    value.approvalWorkflow === undefined ||
    value.approvalWorkflow === null ||
    isMcpApprovalWorkflow(value.approvalWorkflow);
  const approvalStagesOk =
    value.approvalStages === undefined ||
    value.approvalStages === null ||
    (Array.isArray(value.approvalStages) &&
      value.approvalStages.every((item) => isMcpApprovalStageConfig(item)));
  const stage1RequiredApprovalsOk =
    value.stage1RequiredApprovals === undefined ||
    value.stage1RequiredApprovals === null ||
    (typeof value.stage1RequiredApprovals === "number" &&
      Number.isInteger(value.stage1RequiredApprovals) &&
      value.stage1RequiredApprovals >= 0);
  const stage2RequiredApprovalsOk =
    value.stage2RequiredApprovals === undefined ||
    value.stage2RequiredApprovals === null ||
    (typeof value.stage2RequiredApprovals === "number" &&
      Number.isInteger(value.stage2RequiredApprovals) &&
      value.stage2RequiredApprovals >= 0);
  const stage1RolesOk =
    value.stage1Roles === undefined ||
    value.stage1Roles === null ||
    (Array.isArray(value.stage1Roles) &&
      value.stage1Roles.every((item) => typeof item === "string"));
  const stage2RolesOk =
    value.stage2Roles === undefined ||
    value.stage2Roles === null ||
    (Array.isArray(value.stage2Roles) &&
      value.stage2Roles.every((item) => typeof item === "string"));
  const approvalConditionOk =
    value.approvalCondition === undefined ||
    value.approvalCondition === null ||
    (isRecord(value.approvalCondition) &&
      (value.approvalCondition.riskLevelAtLeast === undefined ||
        value.approvalCondition.riskLevelAtLeast === null ||
        isMcpRiskLevel(value.approvalCondition.riskLevelAtLeast)) &&
      (value.approvalCondition.toolIds === undefined ||
        value.approvalCondition.toolIds === null ||
        (Array.isArray(value.approvalCondition.toolIds) &&
          value.approvalCondition.toolIds.every((item) => typeof item === "string"))) &&
      (value.approvalCondition.tenantRoles === undefined ||
        value.approvalCondition.tenantRoles === null ||
        (Array.isArray(value.approvalCondition.tenantRoles) &&
          value.approvalCondition.tenantRoles.every((item) => typeof item === "string"))));
  const metadataOk =
    value.metadata === undefined ||
    value.metadata === null ||
    isRecord(value.metadata);
  return (
    typeof value.tenantId === "string" &&
    typeof value.toolId === "string" &&
    isMcpRiskLevel(value.riskLevel) &&
    isMcpToolDecision(value.decision) &&
    approvalModeOk &&
    approvalWorkflowOk &&
    approvalStagesOk &&
    stage1RequiredApprovalsOk &&
    stage2RequiredApprovalsOk &&
    stage1RolesOk &&
    stage2RolesOk &&
    approvalConditionOk &&
    metadataOk &&
    reasonOk &&
    isISODateString(value.updatedAt)
  );
}

function isMcpToolPolicyListInput(value: unknown): value is McpToolPolicyListInput {
  if (!isRecord(value)) {
    return false;
  }
  const riskLevelOk = value.riskLevel === undefined || isMcpRiskLevel(value.riskLevel);
  const decisionOk = value.decision === undefined || isMcpToolDecision(value.decision);
  const keywordOk = value.keyword === undefined || typeof value.keyword === "string";
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return riskLevelOk && decisionOk && keywordOk && limitOk;
}

function isMcpToolPolicyListResponse(value: unknown): value is McpToolPolicyListResponse {
  if (!isRecord(value)) {
    return false;
  }
  const filtersOk = isMcpToolPolicyListInput(value.filters);
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isMcpToolPolicy(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

function isMcpApprovalRequest(value: unknown): value is McpApprovalRequest {
  if (!isRecord(value)) {
    return false;
  }
  const requestedByEmailOk =
    value.requestedByEmail === undefined ||
    value.requestedByEmail === null ||
    typeof value.requestedByEmail === "string";
  const reasonOk = value.reason === undefined || value.reason === null || typeof value.reason === "string";
  const reviewedByUserIdOk =
    value.reviewedByUserId === undefined ||
    value.reviewedByUserId === null ||
    typeof value.reviewedByUserId === "string";
  const reviewedByEmailOk =
    value.reviewedByEmail === undefined ||
    value.reviewedByEmail === null ||
    typeof value.reviewedByEmail === "string";
  const reviewReasonOk =
    value.reviewReason === undefined ||
    value.reviewReason === null ||
    typeof value.reviewReason === "string";
  const approvalModeOk =
    value.approvalMode === undefined ||
    value.approvalMode === null ||
    isMcpApprovalMode(value.approvalMode);
  const currentNodeIdOk =
    value.currentNodeId === undefined ||
    value.currentNodeId === null ||
    typeof value.currentNodeId === "string";
  const currentStageOk =
    value.currentStage === undefined ||
    value.currentStage === null ||
    isMcpApprovalStage(value.currentStage);
  const approvalWorkflowOk =
    value.approvalWorkflow === undefined ||
    value.approvalWorkflow === null ||
    isMcpApprovalWorkflow(value.approvalWorkflow);
  const approvalNodesOk =
    value.approvalNodes === undefined ||
    value.approvalNodes === null ||
    (Array.isArray(value.approvalNodes) &&
      value.approvalNodes.every((item) => isMcpApprovalWorkflowNodeSnapshot(item)));
  const pathHistoryOk =
    value.pathHistory === undefined ||
    value.pathHistory === null ||
    (Array.isArray(value.pathHistory) &&
      value.pathHistory.every((item) => typeof item === "string"));
  const nextTransitionPreviewOk =
    value.nextTransitionPreview === undefined ||
    value.nextTransitionPreview === null ||
    isMcpApprovalWorkflowTransitionPreview(value.nextTransitionPreview);
  const approvalStagesOk =
    value.approvalStages === undefined ||
    value.approvalStages === null ||
    (Array.isArray(value.approvalStages) &&
      value.approvalStages.every((item) => isMcpApprovalStageSnapshot(item)));
  const stage1RequiredApprovalsOk =
    value.stage1RequiredApprovals === undefined ||
    value.stage1RequiredApprovals === null ||
    (typeof value.stage1RequiredApprovals === "number" &&
      Number.isInteger(value.stage1RequiredApprovals) &&
      value.stage1RequiredApprovals >= 0);
  const stage2RequiredApprovalsOk =
    value.stage2RequiredApprovals === undefined ||
    value.stage2RequiredApprovals === null ||
    (typeof value.stage2RequiredApprovals === "number" &&
      Number.isInteger(value.stage2RequiredApprovals) &&
      value.stage2RequiredApprovals >= 0);
  const stage1ApprovedCountOk =
    value.stage1ApprovedCount === undefined ||
    value.stage1ApprovedCount === null ||
    (typeof value.stage1ApprovedCount === "number" &&
      Number.isInteger(value.stage1ApprovedCount) &&
      value.stage1ApprovedCount >= 0);
  const stage2ApprovedCountOk =
    value.stage2ApprovedCount === undefined ||
    value.stage2ApprovedCount === null ||
    (typeof value.stage2ApprovedCount === "number" &&
      Number.isInteger(value.stage2ApprovedCount) &&
      value.stage2ApprovedCount >= 0);
  const remainingApprovalsOk =
    value.remainingApprovals === undefined ||
    value.remainingApprovals === null ||
    (typeof value.remainingApprovals === "number" &&
      Number.isInteger(value.remainingApprovals) &&
      value.remainingApprovals >= 0);
  const stage1RolesOk =
    value.stage1Roles === undefined ||
    value.stage1Roles === null ||
    (Array.isArray(value.stage1Roles) &&
      value.stage1Roles.every((item) => typeof item === "string"));
  const stage2RolesOk =
    value.stage2Roles === undefined ||
    value.stage2Roles === null ||
    (Array.isArray(value.stage2Roles) &&
      value.stage2Roles.every((item) => typeof item === "string"));
  const approvalConditionMatchedOk =
    value.approvalConditionMatched === undefined ||
    value.approvalConditionMatched === null ||
    typeof value.approvalConditionMatched === "boolean";
  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.toolId === "string" &&
    isMcpApprovalStatus(value.status) &&
    approvalModeOk &&
    currentNodeIdOk &&
    currentStageOk &&
    approvalWorkflowOk &&
    approvalNodesOk &&
    pathHistoryOk &&
    nextTransitionPreviewOk &&
    approvalStagesOk &&
    stage1RequiredApprovalsOk &&
    stage2RequiredApprovalsOk &&
    stage1ApprovedCountOk &&
    stage2ApprovedCountOk &&
    remainingApprovalsOk &&
    stage1RolesOk &&
    stage2RolesOk &&
    approvalConditionMatchedOk &&
    typeof value.requestedByUserId === "string" &&
    requestedByEmailOk &&
    reasonOk &&
    reviewedByUserIdOk &&
    reviewedByEmailOk &&
    reviewReasonOk &&
    isISODateString(value.createdAt) &&
    isISODateString(value.updatedAt)
  );
}

function isMcpApprovalListInput(value: unknown): value is McpApprovalListInput {
  if (!isRecord(value)) {
    return false;
  }
  const statusOk = value.status === undefined || isMcpApprovalStatus(value.status);
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return statusOk && limitOk;
}

function isMcpApprovalListResponse(value: unknown): value is McpApprovalListResponse {
  if (!isRecord(value)) {
    return false;
  }
  const filtersOk = isMcpApprovalListInput(value.filters);
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isMcpApprovalRequest(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

function isMcpInvocationAudit(value: unknown): value is McpInvocationAudit {
  if (!isRecord(value)) {
    return false;
  }
  const approvalRequestIdOk =
    value.approvalRequestId === undefined ||
    value.approvalRequestId === null ||
    typeof value.approvalRequestId === "string";
  const resultOk =
    value.result === "allowed" || value.result === "blocked" || value.result === "approved";
  const evaluatedDecisionOk =
    value.evaluatedDecision === undefined ||
    value.evaluatedDecision === null ||
    isMcpToolDecision(value.evaluatedDecision);
  const approvalModeOk =
    value.approvalMode === undefined ||
    value.approvalMode === null ||
    isMcpApprovalMode(value.approvalMode);
  const approvalStageOk =
    value.approvalStage === undefined ||
    value.approvalStage === null ||
    isMcpApprovalStage(value.approvalStage);
  const approvalSatisfiedOk =
    value.approvalSatisfied === undefined ||
    value.approvalSatisfied === null ||
    typeof value.approvalSatisfied === "boolean";
  const approvalConditionMatchedOk =
    value.approvalConditionMatched === undefined ||
    value.approvalConditionMatched === null ||
    typeof value.approvalConditionMatched === "boolean";
  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.toolId === "string" &&
    isMcpToolDecision(value.decision) &&
    resultOk &&
    approvalRequestIdOk &&
    typeof value.enforced === "boolean" &&
    evaluatedDecisionOk &&
    approvalModeOk &&
    approvalStageOk &&
    approvalSatisfiedOk &&
    approvalConditionMatchedOk &&
    isRecord(value.metadata) &&
    isISODateString(value.createdAt)
  );
}

function isMcpEvaluateResult(value: unknown): value is McpEvaluateResult {
  if (!isRecord(value)) {
    return false;
  }
  const approvalRequestIdOk =
    value.approvalRequestId === undefined ||
    value.approvalRequestId === null ||
    typeof value.approvalRequestId === "string";
  const resultOk =
    value.result === "allowed" || value.result === "blocked" || value.result === "approved";
  const approvalRequiredOk =
    value.approvalRequired === undefined ||
    value.approvalRequired === null ||
    typeof value.approvalRequired === "boolean";
  const approvalModeOk =
    value.approvalMode === undefined ||
    value.approvalMode === null ||
    isMcpApprovalMode(value.approvalMode);
  const currentNodeIdOk =
    value.currentNodeId === undefined ||
    value.currentNodeId === null ||
    typeof value.currentNodeId === "string";
  const currentStageOk =
    value.currentStage === undefined ||
    value.currentStage === null ||
    isMcpApprovalStage(value.currentStage);
  const approvalWorkflowOk =
    value.approvalWorkflow === undefined ||
    value.approvalWorkflow === null ||
    isMcpApprovalWorkflow(value.approvalWorkflow);
  const approvalNodesOk =
    value.approvalNodes === undefined ||
    value.approvalNodes === null ||
    (Array.isArray(value.approvalNodes) &&
      value.approvalNodes.every((item) => isMcpApprovalWorkflowNodeSnapshot(item)));
  const pathHistoryOk =
    value.pathHistory === undefined ||
    value.pathHistory === null ||
    (Array.isArray(value.pathHistory) &&
      value.pathHistory.every((item) => typeof item === "string"));
  const nextTransitionPreviewOk =
    value.nextTransitionPreview === undefined ||
    value.nextTransitionPreview === null ||
    isMcpApprovalWorkflowTransitionPreview(value.nextTransitionPreview);
  const approvalStagesOk =
    value.approvalStages === undefined ||
    value.approvalStages === null ||
    (Array.isArray(value.approvalStages) &&
      value.approvalStages.every((item) => isMcpApprovalStageSnapshot(item)));
  const remainingApprovalsOk =
    value.remainingApprovals === undefined ||
    value.remainingApprovals === null ||
    (typeof value.remainingApprovals === "number" &&
      Number.isInteger(value.remainingApprovals) &&
      value.remainingApprovals >= 0);
  const approvalConditionMatchedOk =
    value.approvalConditionMatched === undefined ||
    value.approvalConditionMatched === null ||
    typeof value.approvalConditionMatched === "boolean";
  return (
    typeof value.toolId === "string" &&
    isMcpToolDecision(value.decision) &&
    resultOk &&
    approvalRequestIdOk &&
    approvalRequiredOk &&
    approvalModeOk &&
    currentNodeIdOk &&
    currentStageOk &&
    approvalWorkflowOk &&
    approvalNodesOk &&
    pathHistoryOk &&
    nextTransitionPreviewOk &&
    approvalStagesOk &&
    remainingApprovalsOk &&
    approvalConditionMatchedOk &&
    value.enforced === true &&
    isMcpToolDecision(value.evaluatedDecision) &&
    isMcpToolPolicy(value.policy) &&
    isMcpInvocationAudit(value.invocation) &&
    isISODateString(value.evaluatedAt)
  );
}

function isMcpInvocationListInput(value: unknown): value is McpInvocationListInput {
  if (!isRecord(value)) {
    return false;
  }
  const toolIdOk = value.toolId === undefined || typeof value.toolId === "string";
  const decisionOk = value.decision === undefined || isMcpToolDecision(value.decision);
  const fromOk = value.from === undefined || isISODateString(value.from);
  const toOk = value.to === undefined || isISODateString(value.to);
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return toolIdOk && decisionOk && fromOk && toOk && limitOk;
}

function isMcpInvocationListResponse(value: unknown): value is McpInvocationListResponse {
  if (!isRecord(value)) {
    return false;
  }
  const filtersOk = isMcpInvocationListInput(value.filters);
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isMcpInvocationAudit(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

function isHttpUrl(value: unknown): value is string {
  if (typeof value !== "string") {
    return false;
  }
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function isOpenPlatformOpenApiSummary(value: unknown): value is OpenPlatformOpenApiSummary {
  if (!isRecord(value)) {
    return false;
  }
  const tagsOk =
    Array.isArray(value.tags) &&
    value.tags.every((item) => {
      if (!isRecord(item)) {
        return false;
      }
      return (
        typeof item.tag === "string" &&
        typeof item.operations === "number" &&
        Number.isInteger(item.operations) &&
        item.operations >= 0
      );
    });
  return (
    typeof value.version === "string" &&
    value.version.trim().length > 0 &&
    typeof value.totalPaths === "number" &&
    Number.isInteger(value.totalPaths) &&
    value.totalPaths >= 0 &&
    typeof value.totalOperations === "number" &&
    Number.isInteger(value.totalOperations) &&
    value.totalOperations >= 0 &&
    isISODateString(value.generatedAt) &&
    tagsOk
  );
}

function isOpenPlatformApiKey(value: unknown): value is OpenPlatformApiKey {
  if (!isRecord(value)) {
    return false;
  }
  const expiresAtOk =
    value.expiresAt === undefined || value.expiresAt === null || isISODateString(value.expiresAt);
  const lastUsedAtOk =
    value.lastUsedAt === undefined ||
    value.lastUsedAt === null ||
    isISODateString(value.lastUsedAt);
  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.name === "string" &&
    typeof value.maskedKey === "string" &&
    isOpenPlatformApiKeyStatus(value.status) &&
    Array.isArray(value.scopes) &&
    value.scopes.every((scope) => typeof scope === "string" && scope.trim().length > 0) &&
    expiresAtOk &&
    lastUsedAtOk &&
    isISODateString(value.createdAt) &&
    isISODateString(value.updatedAt)
  );
}

function isOpenPlatformApiKeyListInput(value: unknown): value is OpenPlatformApiKeyListInput {
  if (!isRecord(value)) {
    return false;
  }
  const statusOk = value.status === undefined || isOpenPlatformApiKeyStatus(value.status);
  const keywordOk = value.keyword === undefined || typeof value.keyword === "string";
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return statusOk && keywordOk && limitOk;
}

function isOpenPlatformApiKeyListResponse(value: unknown): value is OpenPlatformApiKeyListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformApiKey(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isOpenPlatformApiKeyListInput(value.filters)
  );
}

function isOpenPlatformWebhook(value: unknown): value is OpenPlatformWebhook {
  if (!isRecord(value)) {
    return false;
  }
  const lastDeliveryAtOk =
    value.lastDeliveryAt === undefined ||
    value.lastDeliveryAt === null ||
    isISODateString(value.lastDeliveryAt);
  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.name === "string" &&
    isHttpUrl(value.url) &&
    Array.isArray(value.events) &&
    value.events.every((event) => typeof event === "string" && event.trim().length > 0) &&
    typeof value.enabled === "boolean" &&
    lastDeliveryAtOk &&
    isISODateString(value.createdAt) &&
    isISODateString(value.updatedAt)
  );
}

function isOpenPlatformWebhookListInput(value: unknown): value is OpenPlatformWebhookListInput {
  if (!isRecord(value)) {
    return false;
  }
  const enabledOk = value.enabled === undefined || typeof value.enabled === "boolean";
  const keywordOk = value.keyword === undefined || typeof value.keyword === "string";
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return enabledOk && keywordOk && limitOk;
}

function isOpenPlatformWebhookListResponse(value: unknown): value is OpenPlatformWebhookListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformWebhook(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isOpenPlatformWebhookListInput(value.filters)
  );
}

function isOpenPlatformWebhookReplayResult(
  value: unknown
): value is OpenPlatformWebhookReplayResult {
  if (!isRecord(value) || !isRecord(value.filters)) {
    return false;
  }
  const eventTypeOk =
    value.filters.eventType === undefined || typeof value.filters.eventType === "string";
  const fromOk = value.filters.from === undefined || isISODateString(value.filters.from);
  const toOk = value.filters.to === undefined || isISODateString(value.filters.to);
  const limitOk =
    value.filters.limit === undefined ||
    (typeof value.filters.limit === "number" &&
      Number.isInteger(value.filters.limit) &&
      value.filters.limit > 0);
  return (
    typeof value.id === "string" &&
    typeof value.webhookId === "string" &&
    typeof value.status === "string" &&
    typeof value.dryRun === "boolean" &&
    eventTypeOk &&
    fromOk &&
    toOk &&
    limitOk &&
    isISODateString(value.requestedAt)
  );
}

function isOpenPlatformQualityDailyQueryInput(
  value: unknown
): value is OpenPlatformQualityDailyQueryInput {
  if (!isRecord(value)) {
    return false;
  }
  const dateOk = value.date === undefined || typeof value.date === "string";
  const fromOk = value.from === undefined || isISODateString(value.from);
  const toOk = value.to === undefined || isISODateString(value.to);
  const metricOk = value.metric === undefined || typeof value.metric === "string";
  const providerOk = value.provider === undefined || typeof value.provider === "string";
  const repoOk = value.repo === undefined || typeof value.repo === "string";
  const workflowOk = value.workflow === undefined || typeof value.workflow === "string";
  const runIdOk = value.runId === undefined || typeof value.runId === "string";
  const groupByOk =
    value.groupBy === undefined ||
    value.groupBy === "provider" ||
    value.groupBy === "repo" ||
    value.groupBy === "workflow" ||
    value.groupBy === "runId";
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return dateOk && fromOk && toOk && metricOk && providerOk && repoOk && workflowOk && runIdOk && groupByOk && limitOk;
}

function isOpenPlatformQualityDailyItem(value: unknown): value is OpenPlatformQualityDailyItem {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.date === "string" &&
    !Number.isNaN(Date.parse(value.date)) &&
    typeof value.metric === "string" &&
    typeof value.value === "number" &&
    Number.isFinite(value.value) &&
    typeof value.target === "number" &&
    Number.isFinite(value.target) &&
    typeof value.score === "number" &&
    Number.isFinite(value.score) &&
    isOpenPlatformQualityDailyStatus(value.status)
  );
}

function isOpenPlatformQualityDailyGroup(
  value: unknown
): value is NonNullable<OpenPlatformQualityDailyResponse["groups"]>[number] {
  if (!isRecord(value)) {
    return false;
  }
  return (
    (value.groupBy === "provider" ||
      value.groupBy === "repo" ||
      value.groupBy === "workflow" ||
      value.groupBy === "runId") &&
    typeof value.value === "string" &&
    typeof value.totalEvents === "number" &&
    Number.isFinite(value.totalEvents) &&
    typeof value.passedEvents === "number" &&
    Number.isFinite(value.passedEvents) &&
    typeof value.failedEvents === "number" &&
    Number.isFinite(value.failedEvents) &&
    typeof value.passRate === "number" &&
    Number.isFinite(value.passRate) &&
    typeof value.avgScore === "number" &&
    Number.isFinite(value.avgScore)
  );
}

function isOpenPlatformQualityDailyResponse(
  value: unknown
): value is OpenPlatformQualityDailyResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformQualityDailyItem(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    (value.groups === undefined ||
      (Array.isArray(value.groups) &&
        value.groups.every((item) => isOpenPlatformQualityDailyGroup(item)))) &&
    isOpenPlatformQualityDailyQueryInput(value.filters)
  );
}

function isOpenPlatformQualityScorecardListInput(
  value: unknown
): value is OpenPlatformQualityScorecardListInput {
  if (!isRecord(value)) {
    return false;
  }
  const teamOk = value.team === undefined || typeof value.team === "string";
  const ownerOk = value.owner === undefined || typeof value.owner === "string";
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return teamOk && ownerOk && limitOk;
}

function isOpenPlatformQualityScorecard(value: unknown): value is OpenPlatformQualityScorecard {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.id === "string" &&
    typeof value.team === "string" &&
    typeof value.owner === "string" &&
    typeof value.overallScore === "number" &&
    Number.isFinite(value.overallScore) &&
    isISODateString(value.publishedAt) &&
    Array.isArray(value.highlights) &&
    value.highlights.every((item) => typeof item === "string")
  );
}

function isOpenPlatformQualityScorecardListResponse(
  value: unknown
): value is OpenPlatformQualityScorecardListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformQualityScorecard(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isOpenPlatformQualityScorecardListInput(value.filters)
  );
}

function isOpenPlatformQualityProjectTrendQueryInput(
  value: unknown
): value is OpenPlatformQualityProjectTrendQueryInput {
  if (!isRecord(value)) {
    return false;
  }
  const fromOk = value.from === undefined || typeof value.from === "string";
  const toOk = value.to === undefined || typeof value.to === "string";
  const metricOk = value.metric === undefined || typeof value.metric === "string";
  const providerOk =
    value.provider === undefined || value.provider === null || typeof value.provider === "string";
  const workflowOk =
    value.workflow === undefined || value.workflow === null || typeof value.workflow === "string";
  const includeUnknownOk =
    value.includeUnknown === undefined || typeof value.includeUnknown === "boolean";
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return fromOk && toOk && metricOk && providerOk && workflowOk && includeUnknownOk && limitOk;
}

function isOpenPlatformQualityProjectTrendItem(
  value: unknown
): value is OpenPlatformQualityProjectTrendItem {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.project === "string" &&
    typeof value.metric === "string" &&
    typeof value.totalEvents === "number" &&
    Number.isInteger(value.totalEvents) &&
    value.totalEvents >= 0 &&
    typeof value.passedEvents === "number" &&
    Number.isInteger(value.passedEvents) &&
    value.passedEvents >= 0 &&
    typeof value.failedEvents === "number" &&
    Number.isInteger(value.failedEvents) &&
    value.failedEvents >= 0 &&
    typeof value.passRate === "number" &&
    Number.isFinite(value.passRate) &&
    typeof value.avgScore === "number" &&
    Number.isFinite(value.avgScore) &&
    typeof value.totalCost === "number" &&
    Number.isFinite(value.totalCost) &&
    typeof value.totalTokens === "number" &&
    Number.isInteger(value.totalTokens) &&
    value.totalTokens >= 0 &&
    typeof value.totalSessions === "number" &&
    Number.isInteger(value.totalSessions) &&
    value.totalSessions >= 0 &&
    typeof value.costPerQualityPoint === "number" &&
    Number.isFinite(value.costPerQualityPoint)
  );
}

function isOpenPlatformQualityProjectTrendSummary(
  value: unknown
): value is OpenPlatformQualityProjectTrendSummary {
  if (!isRecord(value)) {
    return false;
  }
  const fromOk = value.from === undefined || value.from === null || isISODateString(value.from);
  const toOk = value.to === undefined || value.to === null || isISODateString(value.to);
  return (
    typeof value.metric === "string" &&
    typeof value.totalEvents === "number" &&
    Number.isInteger(value.totalEvents) &&
    value.totalEvents >= 0 &&
    typeof value.passedEvents === "number" &&
    Number.isInteger(value.passedEvents) &&
    value.passedEvents >= 0 &&
    typeof value.failedEvents === "number" &&
    Number.isInteger(value.failedEvents) &&
    value.failedEvents >= 0 &&
    typeof value.passRate === "number" &&
    Number.isFinite(value.passRate) &&
    typeof value.avgScore === "number" &&
    Number.isFinite(value.avgScore) &&
    typeof value.totalCost === "number" &&
    Number.isFinite(value.totalCost) &&
    typeof value.totalTokens === "number" &&
    Number.isInteger(value.totalTokens) &&
    value.totalTokens >= 0 &&
    typeof value.totalSessions === "number" &&
    Number.isInteger(value.totalSessions) &&
    value.totalSessions >= 0 &&
    fromOk &&
    toOk
  );
}

function isOpenPlatformQualityProjectTrendResponse(
  value: unknown
): value is OpenPlatformQualityProjectTrendResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformQualityProjectTrendItem(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isOpenPlatformQualityProjectTrendSummary(value.summary) &&
    isOpenPlatformQualityProjectTrendQueryInput(value.filters)
  );
}

function isOpenPlatformAutomationPolicy(
  value: unknown,
): value is OpenPlatformAutomationPolicy {
  return (
    isRecord(value) &&
    typeof value.tenantId === "string" &&
    typeof value.toolId === "string" &&
    value.scope === "quality_replay_advice" &&
    isMcpRiskLevel(value.riskLevel) &&
    isMcpToolDecision(value.decision) &&
    (value.reason === undefined ||
      value.reason === null ||
      typeof value.reason === "string") &&
    typeof value.evaluationScoreThreshold === "number" &&
    Number.isFinite(value.evaluationScoreThreshold) &&
    typeof value.triggerOnEvaluationFailure === "boolean" &&
    typeof value.triggerOnReplayRegression === "boolean" &&
    (value.defaultActionType === undefined ||
      value.defaultActionType === null ||
      value.defaultActionType === "scorecard_adjustment" ||
      value.defaultActionType === "replay_experiment") &&
    (value.modelVersion === undefined ||
      value.modelVersion === null ||
      typeof value.modelVersion === "string") &&
    (value.strategyMatrix === undefined ||
      value.strategyMatrix === null ||
      (Array.isArray(value.strategyMatrix) &&
        value.strategyMatrix.every((rule) =>
          isOpenPlatformAutomationStrategyRule(rule),
        ))) &&
    isISODateString(value.updatedAt)
  );
}

function isOpenPlatformAutomationStrategyRule(
  value: unknown,
): value is OpenPlatformAutomationStrategyRule {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    (value.metric === undefined || value.metric === null || typeof value.metric === "string") &&
    (value.severity === undefined ||
      value.severity === null ||
      value.severity === "info" ||
      value.severity === "warn" ||
      value.severity === "critical") &&
    (value.trendDirection === undefined ||
      value.trendDirection === null ||
      value.trendDirection === "up" ||
      value.trendDirection === "down" ||
      value.trendDirection === "flat") &&
    (value.minConfidence === undefined ||
      value.minConfidence === null ||
      (typeof value.minConfidence === "number" && Number.isFinite(value.minConfidence))) &&
    (value.regressionProbabilityAtLeast === undefined ||
      value.regressionProbabilityAtLeast === null ||
      (typeof value.regressionProbabilityAtLeast === "number" &&
        Number.isFinite(value.regressionProbabilityAtLeast))) &&
    (value.replayRegressionAtLeast === undefined ||
      value.replayRegressionAtLeast === null ||
      (typeof value.replayRegressionAtLeast === "number" &&
        Number.isInteger(value.replayRegressionAtLeast) &&
        value.replayRegressionAtLeast >= 0)) &&
    (value.actionType === "scorecard_adjustment" ||
      value.actionType === "replay_experiment") &&
    typeof value.requiresApproval === "boolean" &&
    (value.cooldownMinutes === undefined ||
      value.cooldownMinutes === null ||
      (typeof value.cooldownMinutes === "number" &&
        Number.isInteger(value.cooldownMinutes) &&
        value.cooldownMinutes >= 0)) &&
    (value.reason === undefined || value.reason === null || typeof value.reason === "string")
  );
}

function isOpenPlatformAutomationPolicySimulationResponse(
  value: unknown,
): value is OpenPlatformAutomationPolicySimulationResponse {
  return (
    isRecord(value) &&
    typeof value.metric === "string" &&
    (value.severity === "info" ||
      value.severity === "warn" ||
      value.severity === "critical") &&
    typeof value.confidence === "number" &&
    Number.isFinite(value.confidence) &&
    (value.trendDirection === "up" ||
      value.trendDirection === "down" ||
      value.trendDirection === "flat") &&
    typeof value.regressionProbability === "number" &&
    Number.isFinite(value.regressionProbability) &&
    typeof value.replayRegressionCount === "number" &&
    Number.isInteger(value.replayRegressionCount) &&
    (value.matchedRuleId === undefined ||
      value.matchedRuleId === null ||
      typeof value.matchedRuleId === "string") &&
    (value.resolvedAction === "scorecard_adjustment" ||
      value.resolvedAction === "replay_experiment") &&
    typeof value.requiresApproval === "boolean" &&
    Array.isArray(value.blockingReasons) &&
    value.blockingReasons.every((item) => typeof item === "string")
  );
}

function isOpenPlatformQualityForecastResponse(
  value: unknown,
): value is OpenPlatformQualityForecastResponse {
  return (
    isRecord(value) &&
    Array.isArray(value.items) &&
    value.items.every(
      (item) =>
        isRecord(item) &&
        typeof item.project === "string" &&
        typeof item.metric === "string" &&
        typeof item.predictedScore === "number" &&
        Number.isFinite(item.predictedScore) &&
        typeof item.confidence === "number" &&
        Number.isFinite(item.confidence) &&
        (item.confidenceLabel === undefined ||
          item.confidenceLabel === null ||
          item.confidenceLabel === "low" ||
          item.confidenceLabel === "medium" ||
          item.confidenceLabel === "high") &&
        (item.trendDirection === undefined ||
          item.trendDirection === null ||
          item.trendDirection === "up" ||
          item.trendDirection === "down" ||
          item.trendDirection === "flat") &&
        (item.projectedDelta === undefined ||
          item.projectedDelta === null ||
          (typeof item.projectedDelta === "number" &&
            Number.isFinite(item.projectedDelta))) &&
        (item.basisWindowCount === undefined ||
          item.basisWindowCount === null ||
          (typeof item.basisWindowCount === "number" &&
            Number.isInteger(item.basisWindowCount) &&
            item.basisWindowCount >= 0)) &&
        (item.rationale === undefined ||
          item.rationale === null ||
          typeof item.rationale === "string") &&
        (item.modelVersion === undefined ||
          item.modelVersion === null ||
          typeof item.modelVersion === "string") &&
        (item.forecastHorizonDays === undefined ||
          item.forecastHorizonDays === null ||
          (typeof item.forecastHorizonDays === "number" &&
            Number.isInteger(item.forecastHorizonDays) &&
            item.forecastHorizonDays >= 0)) &&
        (item.expectedScoreRange === undefined ||
          item.expectedScoreRange === null ||
          (isRecord(item.expectedScoreRange) &&
            typeof item.expectedScoreRange.lower === "number" &&
            Number.isFinite(item.expectedScoreRange.lower) &&
            typeof item.expectedScoreRange.upper === "number" &&
            Number.isFinite(item.expectedScoreRange.upper))) &&
        (item.regressionProbability === undefined ||
          item.regressionProbability === null ||
          (typeof item.regressionProbability === "number" &&
            Number.isFinite(item.regressionProbability))) &&
        (item.explanation === undefined ||
          item.explanation === null ||
          (isRecord(item.explanation) &&
            typeof item.explanation.summary === "string" &&
            (item.explanation.confidenceLabel === "low" ||
              item.explanation.confidenceLabel === "medium" ||
              item.explanation.confidenceLabel === "high") &&
            typeof item.explanation.primaryDriver === "string")) &&
        (item.riskDrivers === undefined ||
          item.riskDrivers === null ||
          (Array.isArray(item.riskDrivers) &&
            item.riskDrivers.every((driver) => typeof driver === "string"))) &&
        (item.recommendedActions === undefined ||
          item.recommendedActions === null ||
          (Array.isArray(item.recommendedActions) &&
            item.recommendedActions.every((action) => typeof action === "string"))) &&
        (item.featureContributions === undefined ||
          item.featureContributions === null ||
          (Array.isArray(item.featureContributions) &&
            item.featureContributions.every(
              (entry) =>
                isRecord(entry) &&
                typeof entry.feature === "string" &&
                typeof entry.impact === "number" &&
                Number.isFinite(entry.impact) &&
                (entry.direction === "positive" ||
                  entry.direction === "negative" ||
                  entry.direction === "neutral"),
            ))) &&
        (item.windowComparisons === undefined ||
          item.windowComparisons === null ||
          (isRecord(item.windowComparisons) &&
            isRecord(item.windowComparisons.currentWindow) &&
            isRecord(item.windowComparisons.previousWindow))) &&
        isRecord(item.basis),
    ) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isRecord(value.filters)
  );
}

function isOpenPlatformQualityAdviceResponse(
  value: unknown,
): value is OpenPlatformQualityAdviceResponse {
  return (
    isRecord(value) &&
    Array.isArray(value.items) &&
    value.items.every(
      (item) =>
        isRecord(item) &&
        typeof item.id === "string" &&
        typeof item.project === "string" &&
        (item.severity === "info" ||
          item.severity === "warn" ||
          item.severity === "critical") &&
        typeof item.title === "string" &&
        typeof item.recommendation === "string" &&
        (item.explanation === undefined ||
          item.explanation === null ||
          typeof item.explanation === "string") &&
        (item.confidence === undefined ||
          item.confidence === null ||
          (typeof item.confidence === "number" && Number.isFinite(item.confidence))) &&
        (item.confidenceLabel === undefined ||
          item.confidenceLabel === null ||
          item.confidenceLabel === "low" ||
          item.confidenceLabel === "medium" ||
          item.confidenceLabel === "high") &&
        (item.why === undefined ||
          item.why === null ||
          (Array.isArray(item.why) && item.why.every((reason) => typeof reason === "string"))) &&
        (item.automationReadiness === undefined ||
          item.automationReadiness === null ||
          item.automationReadiness === "monitor_only" ||
          item.automationReadiness === "manual_review" ||
          item.automationReadiness === "ready_for_execution" ||
          item.automationReadiness === "execution_in_progress") &&
        (item.executionHint === undefined ||
          item.executionHint === null ||
          (isRecord(item.executionHint) &&
            (item.executionHint.recommendedActionType === "scorecard_adjustment" ||
              item.executionHint.recommendedActionType === "replay_experiment") &&
            typeof item.executionHint.requiresDataset === "boolean" &&
            (item.executionHint.priority === "low" ||
              item.executionHint.priority === "medium" ||
              item.executionHint.priority === "high") &&
            typeof item.executionHint.reason === "string")) &&
        (item.executionOptions === undefined ||
          item.executionOptions === null ||
          (Array.isArray(item.executionOptions) &&
            item.executionOptions.every(
              (option) =>
                isRecord(option) &&
                (option.actionType === "scorecard_adjustment" ||
                  option.actionType === "replay_experiment") &&
                (option.availability === "recommended" ||
                  option.availability === "available" ||
                  option.availability === "approval_required" ||
                  option.availability === "disabled") &&
                typeof option.reason === "string",
            ))) &&
        (item.strategyMatrixMatch === undefined ||
          item.strategyMatrixMatch === null ||
          typeof item.strategyMatrixMatch === "string") &&
        (item.recommendedPlan === undefined ||
          item.recommendedPlan === null ||
          isRecord(item.recommendedPlan)) &&
        (item.autoExecutionDecision === undefined ||
          item.autoExecutionDecision === null ||
          typeof item.autoExecutionDecision === "string") &&
        (item.blockingReasons === undefined ||
          item.blockingReasons === null ||
          (Array.isArray(item.blockingReasons) &&
            item.blockingReasons.every((reason) => typeof reason === "string"))) &&
        isRecord(item.basis) &&
        Array.isArray(item.relatedMetrics) &&
        item.relatedMetrics.every((metric) => typeof metric === "string") &&
        (item.suggestedActions === undefined ||
          item.suggestedActions === null ||
          (Array.isArray(item.suggestedActions) &&
            item.suggestedActions.every(
              (action) =>
                action === "scorecard_adjustment" || action === "replay_experiment",
            ))) &&
        (item.latestExecutionId === undefined ||
          item.latestExecutionId === null ||
          typeof item.latestExecutionId === "string") &&
        (item.latestExecutionStatus === undefined ||
          item.latestExecutionStatus === null ||
          item.latestExecutionStatus === "pending" ||
          item.latestExecutionStatus === "running" ||
          item.latestExecutionStatus === "completed" ||
          item.latestExecutionStatus === "failed" ||
          item.latestExecutionStatus === "cancelled"),
    ) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isRecord(value.filters)
  );
}

function isOpenPlatformQualityAdviceExecution(
  value: unknown,
): value is OpenPlatformQualityAdviceExecution {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.adviceId === "string" &&
    typeof value.project === "string" &&
    (value.severity === "info" || value.severity === "warn" || value.severity === "critical") &&
    (value.actionType === "scorecard_adjustment" || value.actionType === "replay_experiment") &&
    (value.triggerSource === "manual" || value.triggerSource === "automatic") &&
    (value.status === "pending" ||
      value.status === "running" ||
      value.status === "completed" ||
      value.status === "failed" ||
      value.status === "cancelled") &&
    (value.metric === undefined || value.metric === null || typeof value.metric === "string") &&
    (value.datasetId === undefined || value.datasetId === null || typeof value.datasetId === "string") &&
    (value.experimentId === undefined || value.experimentId === null || typeof value.experimentId === "string") &&
    (value.candidateLabels === undefined ||
      value.candidateLabels === null ||
      (Array.isArray(value.candidateLabels) &&
        value.candidateLabels.every((item) => typeof item === "string"))) &&
    (value.scorecardKey === undefined || value.scorecardKey === null || typeof value.scorecardKey === "string") &&
    (value.resultSummary === undefined || value.resultSummary === null || isRecord(value.resultSummary)) &&
    (value.error === undefined || value.error === null || typeof value.error === "string") &&
    isISODateString(value.requestedAt) &&
    (value.startedAt === undefined || value.startedAt === null || isISODateString(value.startedAt)) &&
    (value.finishedAt === undefined || value.finishedAt === null || isISODateString(value.finishedAt)) &&
    isISODateString(value.updatedAt)
  );
}

function isOpenPlatformQualityAdviceExecutionListResponse(
  value: unknown,
): value is OpenPlatformQualityAdviceExecutionListResponse {
  return (
    isRecord(value) &&
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformQualityAdviceExecution(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isRecord(value.filters)
  );
}

function isOpenPlatformReplayBaseline(value: unknown): value is OpenPlatformReplayBaseline {
  if (!isRecord(value)) {
    return false;
  }
  const descriptionOk =
    value.description === undefined || value.description === null || typeof value.description === "string";
  const datasetRefOk =
    value.datasetRef === undefined || value.datasetRef === null || typeof value.datasetRef === "string";
  const promptVersionOk =
    value.promptVersion === undefined ||
    value.promptVersion === null ||
    typeof value.promptVersion === "string";
  const sampleCountOk =
    value.sampleCount === undefined ||
    value.sampleCount === null ||
    (typeof value.sampleCount === "number" &&
      Number.isInteger(value.sampleCount) &&
      value.sampleCount >= 0);
  const caseCountOk =
    value.caseCount === undefined ||
    value.caseCount === null ||
    (typeof value.caseCount === "number" &&
      Number.isInteger(value.caseCount) &&
      value.caseCount >= 0);
  const currentVersionIdOk =
    value.currentVersionId === undefined ||
    value.currentVersionId === null ||
    typeof value.currentVersionId === "string";
  const currentVersionNumberOk =
    value.currentVersionNumber === undefined ||
    value.currentVersionNumber === null ||
    (typeof value.currentVersionNumber === "number" &&
      Number.isInteger(value.currentVersionNumber) &&
      value.currentVersionNumber >= 1);
  return (
    typeof value.id === "string" &&
    typeof value.name === "string" &&
    typeof value.model === "string" &&
    typeof value.datasetId === "string" &&
    datasetRefOk &&
    promptVersionOk &&
    sampleCountOk &&
    caseCountOk &&
    currentVersionIdOk &&
    currentVersionNumberOk &&
    descriptionOk &&
    isISODateString(value.createdAt) &&
    isISODateString(value.updatedAt)
  );
}

function isOpenPlatformReplayDatasetVersion(
  value: unknown,
): value is OpenPlatformReplayDatasetVersion {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    (value.tenantId === undefined || value.tenantId === null || typeof value.tenantId === "string") &&
    typeof value.datasetId === "string" &&
    typeof value.version === "number" &&
    Number.isInteger(value.version) &&
    value.version >= 1 &&
    (value.datasetRef === undefined || value.datasetRef === null || typeof value.datasetRef === "string") &&
    typeof value.model === "string" &&
    (value.promptVersion === undefined ||
      value.promptVersion === null ||
      typeof value.promptVersion === "string") &&
    typeof value.sampleCount === "number" &&
    Number.isInteger(value.sampleCount) &&
    value.sampleCount >= 0 &&
    (value.metadata === undefined || value.metadata === null || isRecord(value.metadata)) &&
    (value.note === undefined || value.note === null || typeof value.note === "string") &&
    isISODateString(value.createdAt) &&
    (value.promotedAt === undefined || value.promotedAt === null || isISODateString(value.promotedAt))
  );
}

function isOpenPlatformReplayDatasetVersionListResponse(
  value: unknown,
): value is OpenPlatformReplayDatasetVersionListResponse {
  return (
    isRecord(value) &&
    typeof value.datasetId === "string" &&
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformReplayDatasetVersion(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    (value.currentVersionId === undefined ||
      value.currentVersionId === null ||
      typeof value.currentVersionId === "string") &&
    (value.currentVersionNumber === undefined ||
      value.currentVersionNumber === null ||
      (typeof value.currentVersionNumber === "number" &&
        Number.isInteger(value.currentVersionNumber) &&
        value.currentVersionNumber >= 1))
  );
}

function isOpenPlatformReplayDatasetVersionPromoteResponse(
  value: unknown,
): value is OpenPlatformReplayDatasetVersionPromoteResponse {
  return (
    isRecord(value) &&
    isOpenPlatformReplayDatasetVersion(value.version) &&
    (value.dataset === undefined ||
      value.dataset === null ||
      isOpenPlatformReplayBaseline(value.dataset))
  );
}

function isOpenPlatformReplayExperiment(
  value: unknown,
): value is OpenPlatformReplayExperiment {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.name === "string" &&
    typeof value.datasetId === "string" &&
    (value.baselineId === undefined ||
      value.baselineId === null ||
      typeof value.baselineId === "string") &&
    (value.baselineVersionId === undefined ||
      value.baselineVersionId === null ||
      typeof value.baselineVersionId === "string") &&
    (value.metadata === undefined || value.metadata === null || isRecord(value.metadata)) &&
    (value.status === undefined ||
      value.status === null ||
      value.status === "draft" ||
      value.status === "queued" ||
      value.status === "running" ||
      value.status === "completed" ||
      value.status === "failed" ||
      value.status === "cancelled") &&
    (value.triggerSource === undefined ||
      value.triggerSource === null ||
      value.triggerSource === "manual" ||
      value.triggerSource === "quality_advice" ||
      value.triggerSource === "automatic") &&
    (value.executionMode === undefined ||
      value.executionMode === null ||
      value.executionMode === "manual" ||
      value.executionMode === "automatic") &&
    (value.candidateLabels === undefined ||
      value.candidateLabels === null ||
      (Array.isArray(value.candidateLabels) &&
        value.candidateLabels.every((item) => typeof item === "string"))) &&
    (value.sourceAdviceId === undefined ||
      value.sourceAdviceId === null ||
      typeof value.sourceAdviceId === "string") &&
    Array.isArray(value.runIds) &&
    value.runIds.every((item) => typeof item === "string") &&
    (value.runStatusSummary === undefined ||
      value.runStatusSummary === null ||
      isRecord(value.runStatusSummary)) &&
    (value.aggregateSummary === undefined ||
      value.aggregateSummary === null ||
      isRecord(value.aggregateSummary)) &&
    isRecord(value.summary) &&
    Array.isArray(value.runs) &&
    value.runs.every((item) => mapOpenPlatformReplayJob(item) !== null) &&
    isISODateString(value.createdAt) &&
    isISODateString(value.updatedAt) &&
    (value.startedAt === undefined ||
      value.startedAt === null ||
      isISODateString(value.startedAt)) &&
    (value.finishedAt === undefined ||
      value.finishedAt === null ||
      isISODateString(value.finishedAt)) &&
    (value.lastError === undefined ||
      value.lastError === null ||
      typeof value.lastError === "string")
  );
}

function isOpenPlatformReplayExperimentListResponse(
  value: unknown,
): value is OpenPlatformReplayExperimentListResponse {
  return (
    isRecord(value) &&
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformReplayExperiment(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isOpenPlatformReplayExperimentCompareResponse(
  value: unknown,
): value is OpenPlatformReplayExperimentCompareResponse {
  return (
    isRecord(value) &&
    typeof value.experimentId === "string" &&
    typeof value.datasetId === "string" &&
    Array.isArray(value.items) &&
    value.items.every(
      (item) =>
        isRecord(item) &&
        typeof item.runId === "string" &&
        typeof item.candidateLabel === "string" &&
        (item.status === "pending" ||
          item.status === "running" ||
          item.status === "completed" ||
          item.status === "failed" ||
          item.status === "cancelled") &&
        typeof item.totalCases === "number" &&
        Number.isInteger(item.totalCases) &&
        typeof item.processedCases === "number" &&
        Number.isInteger(item.processedCases) &&
        typeof item.improvedCases === "number" &&
        Number.isInteger(item.improvedCases) &&
        typeof item.regressedCases === "number" &&
        Number.isInteger(item.regressedCases) &&
        typeof item.unchangedCases === "number" &&
        Number.isInteger(item.unchangedCases) &&
        typeof item.passRate === "number" &&
        Number.isFinite(item.passRate) &&
        typeof item.improvementRate === "number" &&
        Number.isFinite(item.improvementRate) &&
        typeof item.regressionRate === "number" &&
        Number.isFinite(item.regressionRate) &&
        typeof item.netDelta === "number" &&
        Number.isFinite(item.netDelta) &&
        (item.startedAt === undefined || item.startedAt === null || isISODateString(item.startedAt)) &&
        (item.finishedAt === undefined || item.finishedAt === null || isISODateString(item.finishedAt)),
    ) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isRecord(value.summary) &&
    typeof value.summary.totalRuns === "number" &&
    typeof value.summary.completedRuns === "number" &&
    typeof value.summary.failedRuns === "number" &&
    typeof value.summary.runningRuns === "number" &&
    typeof value.summary.queuedRuns === "number" &&
    typeof value.summary.cancelledRuns === "number" &&
    (value.summary.bestRunId === undefined ||
      value.summary.bestRunId === null ||
      typeof value.summary.bestRunId === "string") &&
    (value.summary.worstRunId === undefined ||
      value.summary.worstRunId === null ||
      typeof value.summary.worstRunId === "string") &&
    (value.summary.bestNetDelta === undefined ||
      value.summary.bestNetDelta === null ||
      typeof value.summary.bestNetDelta === "number") &&
    (value.summary.worstNetDelta === undefined ||
      value.summary.worstNetDelta === null ||
      typeof value.summary.worstNetDelta === "number")
  );
}

function isOpenPlatformReplayExperimentBatchCompareResponse(
  value: unknown,
): value is OpenPlatformReplayExperimentBatchCompareResponse {
  return (
    isRecord(value) &&
    Array.isArray(value.items) &&
    value.items.every(
      (item) =>
        isRecord(item) &&
        typeof item.experimentId === "string" &&
        typeof item.name === "string" &&
        typeof item.datasetId === "string" &&
        (item.status === "draft" ||
          item.status === "queued" ||
          item.status === "running" ||
          item.status === "completed" ||
          item.status === "failed" ||
          item.status === "cancelled") &&
        (item.workflowStage === "draft" ||
          item.workflowStage === "queued" ||
          item.workflowStage === "running" ||
          item.workflowStage === "completed" ||
          item.workflowStage === "failed" ||
          item.workflowStage === "cancelled") &&
        (item.triggerSource === "manual" ||
          item.triggerSource === "quality_advice" ||
          item.triggerSource === "automatic") &&
        (item.sourceAdviceId === undefined ||
          item.sourceAdviceId === null ||
          typeof item.sourceAdviceId === "string") &&
        Array.isArray(item.candidateLabels) &&
        item.candidateLabels.every((candidate) => typeof candidate === "string") &&
        typeof item.totalRuns === "number" &&
        Number.isInteger(item.totalRuns) &&
        typeof item.completedRuns === "number" &&
        Number.isInteger(item.completedRuns) &&
        typeof item.failedRuns === "number" &&
        Number.isInteger(item.failedRuns) &&
        typeof item.runningRuns === "number" &&
        Number.isInteger(item.runningRuns) &&
        typeof item.queuedRuns === "number" &&
        Number.isInteger(item.queuedRuns) &&
        typeof item.totalCases === "number" &&
        Number.isInteger(item.totalCases) &&
        typeof item.processedCases === "number" &&
        Number.isInteger(item.processedCases) &&
        typeof item.improvedCases === "number" &&
        Number.isInteger(item.improvedCases) &&
        typeof item.regressedCases === "number" &&
        Number.isInteger(item.regressedCases) &&
        typeof item.improvementRate === "number" &&
        Number.isFinite(item.improvementRate) &&
        typeof item.regressionRate === "number" &&
        Number.isFinite(item.regressionRate) &&
        typeof item.netDelta === "number" &&
        Number.isFinite(item.netDelta) &&
        (item.bestRunId === undefined ||
          item.bestRunId === null ||
          typeof item.bestRunId === "string") &&
        (item.worstRunId === undefined ||
          item.worstRunId === null ||
          typeof item.worstRunId === "string") &&
        Array.isArray(item.runs) &&
        item.runs.every(
          (run) =>
            isRecord(run) &&
            typeof run.runId === "string" &&
            typeof run.candidateLabel === "string" &&
            (run.status === "pending" ||
              run.status === "running" ||
              run.status === "completed" ||
              run.status === "failed" ||
              run.status === "cancelled") &&
            typeof run.totalCases === "number" &&
            Number.isInteger(run.totalCases) &&
            typeof run.processedCases === "number" &&
            Number.isInteger(run.processedCases) &&
            typeof run.improvedCases === "number" &&
            Number.isInteger(run.improvedCases) &&
            typeof run.regressedCases === "number" &&
            Number.isInteger(run.regressedCases) &&
            typeof run.unchangedCases === "number" &&
            Number.isInteger(run.unchangedCases) &&
            typeof run.passRate === "number" &&
            Number.isFinite(run.passRate) &&
            typeof run.improvementRate === "number" &&
            Number.isFinite(run.improvementRate) &&
            typeof run.regressionRate === "number" &&
            Number.isFinite(run.regressionRate) &&
            typeof run.netDelta === "number" &&
            Number.isFinite(run.netDelta) &&
            (run.startedAt === undefined ||
              run.startedAt === null ||
              isISODateString(run.startedAt)) &&
            (run.finishedAt === undefined ||
              run.finishedAt === null ||
              isISODateString(run.finishedAt)),
        ) &&
        isISODateString(item.updatedAt),
    ) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isRecord(value.summary) &&
    typeof value.summary.comparedExperimentCount === "number" &&
    Number.isInteger(value.summary.comparedExperimentCount) &&
    isISODateString(value.summary.comparedAt) &&
    Array.isArray(value.summary.datasets) &&
    value.summary.datasets.every((dataset) => typeof dataset === "string") &&
    typeof value.summary.totalRuns === "number" &&
    Number.isInteger(value.summary.totalRuns) &&
    typeof value.summary.completedRuns === "number" &&
    Number.isInteger(value.summary.completedRuns) &&
    typeof value.summary.failedRuns === "number" &&
    Number.isInteger(value.summary.failedRuns) &&
    typeof value.summary.runningRuns === "number" &&
    Number.isInteger(value.summary.runningRuns) &&
    typeof value.summary.queuedRuns === "number" &&
    Number.isInteger(value.summary.queuedRuns) &&
    typeof value.summary.totalCases === "number" &&
    Number.isInteger(value.summary.totalCases) &&
    typeof value.summary.processedCases === "number" &&
    Number.isInteger(value.summary.processedCases) &&
    typeof value.summary.improvedCases === "number" &&
    Number.isInteger(value.summary.improvedCases) &&
    typeof value.summary.regressedCases === "number" &&
    Number.isInteger(value.summary.regressedCases) &&
    (value.summary.bestExperimentId === undefined ||
      value.summary.bestExperimentId === null ||
      typeof value.summary.bestExperimentId === "string") &&
    (value.summary.worstExperimentId === undefined ||
      value.summary.worstExperimentId === null ||
      typeof value.summary.worstExperimentId === "string") &&
    isRecord(value.filters) &&
    Array.isArray(value.filters.experimentIds) &&
    value.filters.experimentIds.every((experimentId) => typeof experimentId === "string") &&
    (value.filters.datasetId === undefined ||
      value.filters.datasetId === null ||
      typeof value.filters.datasetId === "string")
  );
}

function isOpenPlatformReplayExperimentWorkflowResponse(
  value: unknown,
): value is OpenPlatformReplayExperimentWorkflowResponse {
  return (
    isRecord(value) &&
    typeof value.experimentId === "string" &&
    typeof value.status === "string" &&
    Array.isArray(value.nodes) &&
    value.nodes.every(
      (node) =>
        isRecord(node) &&
        typeof node.id === "string" &&
        (node.type === "experiment" || node.type === "run") &&
        typeof node.label === "string" &&
        typeof node.status === "string" &&
        (node.startedAt === undefined || node.startedAt === null || isISODateString(node.startedAt)) &&
        (node.finishedAt === undefined || node.finishedAt === null || isISODateString(node.finishedAt)) &&
        (node.metadata === undefined || node.metadata === null || isRecord(node.metadata)),
    ) &&
    Array.isArray(value.edges) &&
    value.edges.every(
      (edge) =>
        isRecord(edge) &&
        typeof edge.from === "string" &&
        typeof edge.to === "string" &&
        typeof edge.label === "string",
    ) &&
    isRecord(value.summary) &&
    typeof value.summary.totalNodes === "number" &&
    typeof value.summary.totalRuns === "number" &&
    typeof value.summary.queuedRuns === "number" &&
    typeof value.summary.runningRuns === "number" &&
    typeof value.summary.completedRuns === "number" &&
    typeof value.summary.failedRuns === "number" &&
    typeof value.summary.cancelledRuns === "number"
  );
}

function isOpenPlatformReplayBaselineListInput(
  value: unknown
): value is OpenPlatformReplayBaselineListInput {
  if (!isRecord(value)) {
    return false;
  }
  const keywordOk = value.keyword === undefined || typeof value.keyword === "string";
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return keywordOk && limitOk;
}

function isOpenPlatformReplayBaselineListResponse(
  value: unknown
): value is OpenPlatformReplayBaselineListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformReplayBaseline(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isOpenPlatformReplayBaselineListInput(value.filters)
  );
}

function isOpenPlatformReplayJob(value: unknown): value is OpenPlatformReplayJob {
  if (!isRecord(value)) {
    return false;
  }
  const baselineIdOk =
    value.baselineId === undefined || value.baselineId === null || typeof value.baselineId === "string";
  const jobIdOk = value.jobId === undefined || value.jobId === null || typeof value.jobId === "string";
  const finishedAtOk =
    value.finishedAt === undefined || value.finishedAt === null || isISODateString(value.finishedAt);
  const updatedAtOk =
    value.updatedAt === undefined || value.updatedAt === null || isISODateString(value.updatedAt);
  const summaryOk =
    value.summary === undefined || value.summary === null || isRecord(value.summary);
  return (
    typeof value.id === "string" &&
    typeof value.runId === "string" &&
    jobIdOk &&
    typeof value.datasetId === "string" &&
    baselineIdOk &&
    typeof value.candidateLabel === "string" &&
    isOpenPlatformReplayJobStatus(value.status) &&
    typeof value.totalCases === "number" &&
    Number.isInteger(value.totalCases) &&
    value.totalCases >= 0 &&
    typeof value.processedCases === "number" &&
    Number.isInteger(value.processedCases) &&
    value.processedCases >= 0 &&
    typeof value.improvedCases === "number" &&
    Number.isInteger(value.improvedCases) &&
    value.improvedCases >= 0 &&
    typeof value.regressedCases === "number" &&
    Number.isInteger(value.regressedCases) &&
    value.regressedCases >= 0 &&
    typeof value.unchangedCases === "number" &&
    Number.isInteger(value.unchangedCases) &&
    value.unchangedCases >= 0 &&
    typeof value.passedCases === "number" &&
    Number.isInteger(value.passedCases) &&
    value.passedCases >= 0 &&
    typeof value.failedCases === "number" &&
    Number.isInteger(value.failedCases) &&
    value.failedCases >= 0 &&
    value.passedCases + value.failedCases <= value.totalCases &&
    value.processedCases <= value.totalCases &&
    value.improvedCases + value.regressedCases + value.unchangedCases <= value.processedCases &&
    summaryOk &&
    isISODateString(value.createdAt) &&
    updatedAtOk &&
    finishedAtOk
  );
}

function isOpenPlatformReplayJobListInput(value: unknown): value is OpenPlatformReplayJobListInput {
  if (!isRecord(value)) {
    return false;
  }
  const datasetIdOk = value.datasetId === undefined || typeof value.datasetId === "string";
  const baselineIdOk = value.baselineId === undefined || typeof value.baselineId === "string";
  const statusOk = value.status === undefined || isOpenPlatformReplayJobStatus(value.status);
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return datasetIdOk && baselineIdOk && statusOk && limitOk;
}

function isOpenPlatformReplayJobListResponse(
  value: unknown
): value is OpenPlatformReplayJobListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformReplayJob(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isOpenPlatformReplayJobListInput(value.filters)
  );
}

function isOpenPlatformReplayDiffQueryInput(
  value: unknown
): value is OpenPlatformReplayDiffQueryInput {
  if (!isRecord(value)) {
    return false;
  }
  const datasetIdOk = value.datasetId === undefined || typeof value.datasetId === "string";
  const baselineIdOk = value.baselineId === undefined || typeof value.baselineId === "string";
  const runIdOk = value.runId === undefined || typeof value.runId === "string";
  const jobIdOk = value.jobId === undefined || typeof value.jobId === "string";
  const hasRunIdentifier = typeof value.runId === "string" || typeof value.jobId === "string";
  const keywordOk = value.keyword === undefined || typeof value.keyword === "string";
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit >= 1);
  return datasetIdOk && baselineIdOk && runIdOk && jobIdOk && hasRunIdentifier && keywordOk && limitOk;
}

function isOpenPlatformReplayDiffItem(value: unknown): value is OpenPlatformReplayDiffItem {
  if (!isRecord(value)) {
    return false;
  }
  const baselineIdOk =
    value.baselineId === undefined || value.baselineId === null || typeof value.baselineId === "string";
  const jobIdOk = value.jobId === undefined || value.jobId === null || typeof value.jobId === "string";
  return (
    typeof value.id === "string" &&
    typeof value.datasetId === "string" &&
    baselineIdOk &&
    typeof value.runId === "string" &&
    jobIdOk &&
    typeof value.caseId === "string" &&
    typeof value.summary === "string" &&
    isOpenPlatformReplayDiffVerdict(value.verdict) &&
    typeof value.deltaScore === "number" &&
    Number.isFinite(value.deltaScore)
  );
}

function isOpenPlatformReplayDiffResponse(value: unknown): value is OpenPlatformReplayDiffResponse {
  if (!isRecord(value)) {
    return false;
  }
  const summaryOk = value.summary === undefined || value.summary === null || isRecord(value.summary);
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformReplayDiffItem(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    summaryOk &&
    isOpenPlatformReplayDiffQueryInput(value.filters)
  );
}

function isOpenPlatformReplayDatasetCase(value: unknown): value is OpenPlatformReplayDatasetCase {
  if (!isRecord(value)) {
    return false;
  }
  const expectedOutputOk =
    value.expectedOutput === undefined ||
    value.expectedOutput === null ||
    typeof value.expectedOutput === "string";
  const baselineOutputOk =
    value.baselineOutput === undefined ||
    value.baselineOutput === null ||
    typeof value.baselineOutput === "string";
  const candidateInputOk =
    value.candidateInput === undefined ||
    value.candidateInput === null ||
    typeof value.candidateInput === "string";
  const checksumOk =
    value.checksum === undefined || value.checksum === null || typeof value.checksum === "string";
  const createdAtOk =
    value.createdAt === undefined || value.createdAt === null || isISODateString(value.createdAt);
  const updatedAtOk =
    value.updatedAt === undefined || value.updatedAt === null || isISODateString(value.updatedAt);
  return (
    typeof value.datasetId === "string" &&
    typeof value.caseId === "string" &&
    typeof value.sortOrder === "number" &&
    Number.isInteger(value.sortOrder) &&
    value.sortOrder >= 0 &&
    typeof value.input === "string" &&
    value.input.length > 0 &&
    expectedOutputOk &&
    baselineOutputOk &&
    candidateInputOk &&
    isRecord(value.metadata) &&
    checksumOk &&
    createdAtOk &&
    updatedAtOk
  );
}

function isOpenPlatformReplayDatasetCaseListResponse(
  value: unknown
): value is OpenPlatformReplayDatasetCaseListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.datasetId === "string" &&
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformReplayDatasetCase(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isOpenPlatformReplayDatasetMaterializeResponse(
  value: unknown
): value is OpenPlatformReplayDatasetMaterializeResponse {
  if (!isRecord(value)) {
    return false;
  }
  const sourceTypeOk = value.sourceType === "session";
  const materializedOk =
    typeof value.materialized === "number" &&
    Number.isInteger(value.materialized) &&
    value.materialized >= 0;
  const skippedOk =
    typeof value.skipped === "number" &&
    Number.isInteger(value.skipped) &&
    value.skipped >= 0;
  const filtersOk = isRecord(value.filters);
  return (
    typeof value.datasetId === "string" &&
    sourceTypeOk &&
    materializedOk &&
    skippedOk &&
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformReplayDatasetCase(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

function isOpenPlatformReplayArtifact(value: unknown): value is OpenPlatformReplayArtifact {
  if (!isRecord(value)) {
    return false;
  }
  const runIdOk = value.runId === undefined || value.runId === null || typeof value.runId === "string";
  const nameOk = value.name === undefined || value.name === null || typeof value.name === "string";
  const descriptionOk =
    value.description === undefined || value.description === null || typeof value.description === "string";
  const byteSizeOk =
    value.byteSize === undefined ||
    value.byteSize === null ||
    (typeof value.byteSize === "number" && Number.isInteger(value.byteSize) && value.byteSize >= 0);
  const downloadNameOk =
    value.downloadName === undefined ||
    value.downloadName === null ||
    typeof value.downloadName === "string";
  const downloadUrlOk =
    value.downloadUrl === undefined ||
    value.downloadUrl === null ||
    typeof value.downloadUrl === "string";
  const checksumOk =
    value.checksum === undefined || value.checksum === null || typeof value.checksum === "string";
  const storageBackendOk =
    value.storageBackend === undefined ||
    value.storageBackend === null ||
    value.storageBackend === "local" ||
    value.storageBackend === "object" ||
    value.storageBackend === "hybrid";
  const storageKeyOk =
    value.storageKey === undefined || value.storageKey === null || typeof value.storageKey === "string";
  const metadataOk = value.metadata === undefined || value.metadata === null || isRecord(value.metadata);
  const createdAtOk =
    value.createdAt === undefined || value.createdAt === null || isISODateString(value.createdAt);
  const inlineOk =
    value.inline === undefined ||
    value.inline === null ||
    isRecord(value.inline);
  return (
    runIdOk &&
    typeof value.type === "string" &&
    typeof value.contentType === "string" &&
    nameOk &&
    descriptionOk &&
    byteSizeOk &&
    downloadNameOk &&
    downloadUrlOk &&
    checksumOk &&
    storageBackendOk &&
    storageKeyOk &&
    metadataOk &&
    createdAtOk &&
    inlineOk
  );
}

function isOpenPlatformReplayArtifactListResponse(
  value: unknown
): value is OpenPlatformReplayArtifactListResponse {
  if (!isRecord(value)) {
    return false;
  }
  const jobIdOk = value.jobId === undefined || value.jobId === null || typeof value.jobId === "string";
  const datasetIdOk = value.datasetId === undefined || value.datasetId === null || typeof value.datasetId === "string";
  return (
    typeof value.runId === "string" &&
    jobIdOk &&
    datasetIdOk &&
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformReplayArtifact(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isOpenPlatformReplayDatasetVersionCaseListResponse(
  value: unknown,
): value is OpenPlatformReplayDatasetVersionCaseListResponse {
  return (
    isRecord(value) &&
    typeof value.datasetId === "string" &&
    typeof value.versionId === "string" &&
    Array.isArray(value.items) &&
    value.items.every((item) => mapOpenPlatformReplayDatasetCase(item) !== null) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isOpenPlatformReplayExperimentArtifactListResponse(
  value: unknown,
): value is OpenPlatformReplayExperimentArtifactListResponse {
  return (
    isRecord(value) &&
    typeof value.experimentId === "string" &&
    typeof value.datasetId === "string" &&
    Array.isArray(value.items) &&
    value.items.every((item) => isOpenPlatformReplayArtifact(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0
  );
}

function isSession(value: unknown): value is Session {
  if (!isRecord(value)) {
    return false;
  }

  const endedAtOk =
    value.endedAt === undefined ||
    value.endedAt === null ||
    (typeof value.endedAt === "string" && value.endedAt.trim().length > 0);

  return (
    typeof value.id === "string" &&
    typeof value.sourceId === "string" &&
    typeof value.tool === "string" &&
    typeof value.model === "string" &&
    isISODateString(value.startedAt) &&
    endedAtOk &&
    typeof value.tokens === "number" &&
    Number.isFinite(value.tokens) &&
    typeof value.cost === "number" &&
    Number.isFinite(value.cost)
  );
}

function isSessionSourceFreshness(value: unknown): value is SessionSourceFreshness {
  if (!isRecord(value)) {
    return false;
  }

  const sourceNameOk =
    value.sourceName === undefined ||
    value.sourceName === null ||
    (typeof value.sourceName === "string" && value.sourceName.trim().length > 0);
  const lastSuccessAtOk = value.lastSuccessAt === null || isISODateString(value.lastSuccessAt);
  const lastFailureAtOk = value.lastFailureAt === null || isISODateString(value.lastFailureAt);
  const avgLatencyOk =
    value.avgLatencyMs === null ||
    (typeof value.avgLatencyMs === "number" &&
      Number.isFinite(value.avgLatencyMs) &&
      value.avgLatencyMs >= 0);
  const freshnessOk =
    value.freshnessMinutes === null ||
    (typeof value.freshnessMinutes === "number" &&
      Number.isInteger(value.freshnessMinutes) &&
      value.freshnessMinutes >= 0);

  return (
    typeof value.sourceId === "string" &&
    sourceNameOk &&
    isSourceAccessMode(value.accessMode) &&
    lastSuccessAtOk &&
    lastFailureAtOk &&
    typeof value.failureCount === "number" &&
    Number.isInteger(value.failureCount) &&
    value.failureCount >= 0 &&
    avgLatencyOk &&
    freshnessOk
  );
}

function isSourceHealth(value: unknown): value is SourceHealth {
  if (!isRecord(value)) {
    return false;
  }

  const lastSuccessAtOk = value.lastSuccessAt === null || isISODateString(value.lastSuccessAt);
  const lastFailureAtOk = value.lastFailureAt === null || isISODateString(value.lastFailureAt);
  const avgLatencyOk =
    value.avgLatencyMs === null ||
    (typeof value.avgLatencyMs === "number" &&
      Number.isFinite(value.avgLatencyMs) &&
      value.avgLatencyMs >= 0);
  const freshnessOk =
    value.freshnessMinutes === null ||
    (typeof value.freshnessMinutes === "number" &&
      Number.isInteger(value.freshnessMinutes) &&
      value.freshnessMinutes >= 0);

  return (
    typeof value.sourceId === "string" &&
    isSourceAccessMode(value.accessMode) &&
    lastSuccessAtOk &&
    lastFailureAtOk &&
    typeof value.failureCount === "number" &&
    Number.isInteger(value.failureCount) &&
    value.failureCount >= 0 &&
    avgLatencyOk &&
    freshnessOk
  );
}

function isSourceParseFailure(value: unknown): value is SourceParseFailure {
  if (!isRecord(value)) {
    return false;
  }

  const sourcePathOk =
    value.sourcePath === undefined ||
    value.sourcePath === null ||
    typeof value.sourcePath === "string";
  const sourceOffsetOk =
    value.sourceOffset === undefined ||
    value.sourceOffset === null ||
    (typeof value.sourceOffset === "number" &&
      Number.isInteger(value.sourceOffset) &&
      value.sourceOffset >= 0);
  const rawHashOk =
    value.rawHash === undefined ||
    value.rawHash === null ||
    typeof value.rawHash === "string";

  return (
    typeof value.id === "string" &&
    typeof value.sourceId === "string" &&
    typeof value.parserKey === "string" &&
    typeof value.errorCode === "string" &&
    typeof value.errorMessage === "string" &&
    sourcePathOk &&
    sourceOffsetOk &&
    rawHashOk &&
    isRecord(value.metadata) &&
    isISODateString(value.failedAt) &&
    isISODateString(value.createdAt)
  );
}

function isSourceParseFailureQueryInput(
  value: unknown
): value is SourceParseFailureQueryInput {
  if (!isRecord(value)) {
    return false;
  }

  const fromOk = value.from === undefined || isISODateString(value.from);
  const toOk = value.to === undefined || isISODateString(value.to);
  const parserKeyOk = value.parserKey === undefined || typeof value.parserKey === "string";
  const errorCodeOk = value.errorCode === undefined || typeof value.errorCode === "string";
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" &&
      Number.isInteger(value.limit) &&
      value.limit > 0);

  return fromOk && toOk && parserKeyOk && errorCodeOk && limitOk;
}

function isSourceParseFailureListResponse(
  value: unknown
): value is SourceParseFailureListResponse {
  if (!isRecord(value)) {
    return false;
  }

  const filtersOk =
    value.filters === undefined || isSourceParseFailureQueryInput(value.filters);

  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isSourceParseFailure(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

function isAlertSeverity(value: unknown): value is AlertItem["severity"] {
  return typeof value === "string" && ALERT_SEVERITIES.includes(value as AlertItem["severity"]);
}

function isAlertStatus(value: unknown): value is AlertItem["status"] {
  return typeof value === "string" && ALERT_STATUSES.includes(value as AlertItem["status"]);
}

function isAlertMutableStatus(value: unknown): value is AlertMutableStatus {
  return typeof value === "string" && ALERT_MUTABLE_STATUSES.includes(value as AlertMutableStatus);
}

function isAlertOrchestrationEventType(value: unknown): value is AlertOrchestrationEventType {
  return (
    typeof value === "string" &&
    ALERT_ORCHESTRATION_EVENT_TYPES.includes(value as AlertOrchestrationEventType)
  );
}

function isAlertOrchestrationChannel(value: unknown): value is AlertOrchestrationChannel {
  return (
    typeof value === "string" &&
    ALERT_ORCHESTRATION_CHANNELS.includes(value as AlertOrchestrationChannel)
  );
}

function isAlertOrchestrationDispatchMode(
  value: unknown
): value is AlertOrchestrationDispatchMode {
  return (
    typeof value === "string" &&
    ALERT_ORCHESTRATION_DISPATCH_MODES.includes(value as AlertOrchestrationDispatchMode)
  );
}

function isAlertOrchestrationEscalationReason(
  value: unknown
): value is AlertOrchestrationEscalationReason {
  return (
    typeof value === "string" &&
    ALERT_ORCHESTRATION_ESCALATION_REASONS.includes(
      value as AlertOrchestrationEscalationReason,
    )
  );
}

function isExportFormat(value: unknown): value is ExportFormat {
  return typeof value === "string" && EXPORT_FORMATS.includes(value as ExportFormat);
}

function isUsageExportDimension(value: unknown): value is UsageExportDimension {
  return (
    typeof value === "string" &&
    USAGE_EXPORT_DIMENSIONS.includes(value as UsageExportDimension)
  );
}

function isUsageMetric(value: unknown): value is "tokens" | "cost" | "sessions" {
  return typeof value === "string" && USAGE_METRICS.includes(value as "tokens" | "cost" | "sessions");
}

function isAlertListInput(value: unknown): value is AlertListInput {
  if (!isRecord(value)) {
    return false;
  }

  const statusOk = value.status === undefined || isAlertStatus(value.status);
  const severityOk = value.severity === undefined || isAlertSeverity(value.severity);
  const scopeOk = value.scope === undefined || typeof value.scope === "string";
  const scopeRefOk = value.scopeRef === undefined || typeof value.scopeRef === "string";
  const budgetIdOk = value.budgetId === undefined || typeof value.budgetId === "string";
  const fromOk = value.from === undefined || isISODateString(value.from);
  const toOk = value.to === undefined || isISODateString(value.to);
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" &&
      Number.isInteger(value.limit) &&
      value.limit > 0);
  const cursorOk = value.cursor === undefined || typeof value.cursor === "string";

  return (
    statusOk &&
    severityOk &&
    scopeOk &&
    scopeRefOk &&
    budgetIdOk &&
    fromOk &&
    toOk &&
    limitOk &&
    cursorOk
  );
}

function isAlertItem(value: unknown): value is AlertItem {
  if (!isRecord(value)) {
    return false;
  }

  const externalLinksOk =
    value.externalLinks === undefined ||
    (Array.isArray(value.externalLinks) &&
      value.externalLinks.every((item) => {
        if (!isRecord(item)) {
          return false;
        }
        const externalStatusOk =
          item.externalStatus === undefined ||
          item.externalStatus === null ||
          typeof item.externalStatus === "string";
        const pendingExternalStatusOk =
          item.pendingExternalStatus === undefined ||
          item.pendingExternalStatus === null ||
          typeof item.pendingExternalStatus === "string";
        const publishStatusOk =
          item.publishStatus === undefined ||
          item.publishStatus === null ||
          item.publishStatus === "success" ||
          item.publishStatus === "failed";
        const publishErrorOk =
          item.publishError === undefined ||
          item.publishError === null ||
          typeof item.publishError === "string";
        const lastSyncResultOk =
          item.lastSyncResult === undefined ||
          item.lastSyncResult === null ||
          item.lastSyncResult === "success" ||
          item.lastSyncResult === "failed";
        const lastSyncErrorOk =
          item.lastSyncError === undefined ||
          item.lastSyncError === null ||
          typeof item.lastSyncError === "string";
        const lastSyncFailureStageOk =
          item.lastSyncFailureStage === undefined ||
          item.lastSyncFailureStage === null ||
          typeof item.lastSyncFailureStage === "string";
        const lastSyncFailureCodeOk =
          item.lastSyncFailureCode === undefined ||
          item.lastSyncFailureCode === null ||
          typeof item.lastSyncFailureCode === "string";
        return (
          typeof item.id === "string" &&
          (item.externalType === "ticket" ||
            item.externalType === "case" ||
            item.externalType === "incident") &&
          typeof item.externalSystem === "string" &&
          typeof item.externalId === "string" &&
          externalStatusOk &&
          pendingExternalStatusOk &&
          isISODateString(item.lastSyncedAt) &&
          publishStatusOk &&
          publishErrorOk &&
          lastSyncResultOk &&
          lastSyncErrorOk &&
          lastSyncFailureStageOk &&
          lastSyncFailureCodeOk
        );
      }));

  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.budgetId === "string" &&
    typeof value.scope === "string" &&
    typeof value.scopeRef === "string" &&
    isAlertSeverity(value.severity) &&
    isAlertStatus(value.status) &&
    typeof value.message === "string" &&
    typeof value.threshold === "number" &&
    Number.isFinite(value.threshold) &&
    typeof value.value === "number" &&
    Number.isFinite(value.value) &&
    isISODateString(value.createdAt) &&
    isISODateString(value.updatedAt) &&
    isRecord(value.metadata) &&
    externalLinksOk
  );
}

function isAlertListResponse(value: unknown): value is AlertListResponse {
  if (!isRecord(value) || !Array.isArray(value.items)) {
    return false;
  }
  const filtersOk = isAlertListInput(value.filters);
  const nextCursorOk = value.nextCursor === null || typeof value.nextCursor === "string";
  return (
    value.items.every((item) => isAlertItem(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk &&
    nextCursorOk
  );
}

function isAlertExternalLinkOpsItem(value: unknown): value is AlertExternalLinkOpsItem {
  if (!isRecord(value)) {
    return false;
  }
  const alertIdOk =
    value.alertId === undefined ||
    value.alertId === null ||
    typeof value.alertId === "string";
  const alertStatusOk =
    value.alertStatus === undefined ||
    value.alertStatus === null ||
    value.alertStatus === "open" ||
    value.alertStatus === "acknowledged" ||
    value.alertStatus === "resolved";
  const externalStatusOk =
    value.externalStatus === undefined ||
    value.externalStatus === null ||
    typeof value.externalStatus === "string";
  const pendingExternalStatusOk =
    value.pendingExternalStatus === undefined ||
    value.pendingExternalStatus === null ||
    typeof value.pendingExternalStatus === "string";
  const publishStatusOk =
    value.publishStatus === undefined ||
    value.publishStatus === null ||
    value.publishStatus === "success" ||
    value.publishStatus === "failed";
  const publishErrorOk =
    value.publishError === undefined ||
    value.publishError === null ||
    typeof value.publishError === "string";
  const lastSyncResultOk =
    value.lastSyncResult === undefined ||
    value.lastSyncResult === null ||
    value.lastSyncResult === "success" ||
    value.lastSyncResult === "failed";
  const lastSyncErrorOk =
    value.lastSyncError === undefined ||
    value.lastSyncError === null ||
    typeof value.lastSyncError === "string";
  const lastSyncFailureStageOk =
    value.lastSyncFailureStage === undefined ||
    value.lastSyncFailureStage === null ||
    typeof value.lastSyncFailureStage === "string";
  const lastSyncFailureCodeOk =
    value.lastSyncFailureCode === undefined ||
    value.lastSyncFailureCode === null ||
    typeof value.lastSyncFailureCode === "string";
  const updatedAtOk =
    value.updatedAt === undefined ||
    value.updatedAt === null ||
    (typeof value.updatedAt === "string" && isISODateString(value.updatedAt));
  return (
    typeof value.id === "string" &&
    alertIdOk &&
    alertStatusOk &&
    (value.externalType === "ticket" ||
      value.externalType === "case" ||
      value.externalType === "incident") &&
    typeof value.externalSystem === "string" &&
    typeof value.externalId === "string" &&
    externalStatusOk &&
    pendingExternalStatusOk &&
    typeof value.lastSyncedAt === "string" &&
    isISODateString(value.lastSyncedAt) &&
    publishStatusOk &&
    publishErrorOk &&
    lastSyncResultOk &&
    lastSyncErrorOk &&
    lastSyncFailureStageOk &&
    lastSyncFailureCodeOk &&
    (value.syncState === "synced" ||
      value.syncState === "pending" ||
      value.syncState === "failed") &&
    typeof value.retryable === "boolean" &&
    updatedAtOk
  );
}

function isAlertExternalLinkOpsResponse(value: unknown): value is AlertExternalLinkOpsResponse {
  if (!isRecord(value) || !isRecord(value.summary) || !isRecord(value.filters)) {
    return false;
  }
  const externalTypeOk =
    value.filters.externalType === undefined ||
    value.filters.externalType === "ticket" ||
    value.filters.externalType === "case" ||
    value.filters.externalType === "incident";
  const onlyFailedOk =
    value.filters.onlyFailed === undefined ||
    typeof value.filters.onlyFailed === "boolean";
  return (
    typeof value.alertId === "string" &&
    typeof value.summary.total === "number" &&
    typeof value.summary.pending === "number" &&
    typeof value.summary.failed === "number" &&
    Array.isArray(value.items) &&
    value.items.every((item) => isAlertExternalLinkOpsItem(item)) &&
    externalTypeOk &&
    onlyFailedOk
  );
}

function isAlertExternalLinkBatchRetryResponse(
  value: unknown,
): value is AlertExternalLinkBatchRetryResponse {
  return (
    isRecord(value) &&
    typeof value.alertId === "string" &&
    typeof value.retriedCount === "number" &&
    typeof value.published === "number" &&
    typeof value.failed === "number" &&
    Array.isArray(value.items) &&
    value.items.every((item) => isAlertExternalLinkOpsItem(item))
  );
}

function isIntegrationDlqMessage(value: unknown): value is IntegrationDlqMessageListResponse["items"][number] {
  return (
    isRecord(value) &&
    typeof value.messageId === "string" &&
    typeof value.stream === "string" &&
    typeof value.subject === "string" &&
    typeof value.eventType === "string" &&
    (value.channel === undefined || value.channel === null || typeof value.channel === "string") &&
    (value.callbackId === undefined || value.callbackId === null || typeof value.callbackId === "string") &&
    (value.tenantId === undefined || value.tenantId === null || typeof value.tenantId === "string") &&
    (value.alertId === undefined || value.alertId === null || typeof value.alertId === "string") &&
    (value.externalType === undefined || value.externalType === null || typeof value.externalType === "string") &&
    (value.externalId === undefined || value.externalId === null || typeof value.externalId === "string") &&
    typeof value.failedAt === "string" &&
    isISODateString(value.failedAt) &&
    typeof value.attempt === "number" &&
    Number.isInteger(value.attempt) &&
    typeof value.error === "string" &&
    typeof value.retryable === "boolean" &&
    isRecord(value.payload)
  );
}

function isIntegrationDlqMessageListResponse(
  value: unknown,
): value is IntegrationDlqMessageListResponse {
  return (
    isRecord(value) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    Array.isArray(value.items) &&
    value.items.every((item) => isIntegrationDlqMessage(item)) &&
    isRecord(value.filters)
  );
}

function isIntegrationDlqReplayResponse(
  value: unknown,
): value is IntegrationDlqReplayResponse {
  return (
    isRecord(value) &&
    typeof value.replayedCount === "number" &&
    typeof value.failedCount === "number" &&
    Array.isArray(value.items) &&
    value.items.every(
      (item) =>
        isRecord(item) &&
        typeof item.messageId === "string" &&
        (item.status === "replayed" || item.status === "failed") &&
        (item.error === undefined || item.error === null || typeof item.error === "string"),
    )
  );
}

function isIntegrationDlqRecoveryJob(
  value: unknown,
): value is IntegrationDlqRecoveryJob {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    (value.status === "queued" ||
      value.status === "running" ||
      value.status === "completed" ||
      value.status === "failed") &&
    typeof value.requestedAt === "string" &&
    isISODateString(value.requestedAt) &&
    (value.startedAt === undefined ||
      value.startedAt === null ||
      (typeof value.startedAt === "string" && isISODateString(value.startedAt))) &&
    (value.finishedAt === undefined ||
      value.finishedAt === null ||
      (typeof value.finishedAt === "string" && isISODateString(value.finishedAt))) &&
    Array.isArray(value.messageIds) &&
    value.messageIds.every((item) => typeof item === "string") &&
    isRecord(value.summary) &&
    typeof value.summary.total === "number" &&
    typeof value.summary.replayed === "number" &&
    typeof value.summary.failed === "number" &&
    Array.isArray(value.items) &&
    value.items.every(
      (item) =>
        isRecord(item) &&
        typeof item.messageId === "string" &&
        (item.status === "replayed" || item.status === "failed") &&
        (item.error === undefined || item.error === null || typeof item.error === "string"),
    ) &&
    (value.error === undefined || value.error === null || typeof value.error === "string")
  );
}

function isIntegrationDlqRecoveryJobListResponse(
  value: unknown,
): value is IntegrationDlqRecoveryJobListResponse {
  return (
    isRecord(value) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    Array.isArray(value.items) &&
    value.items.every((item) => isIntegrationDlqRecoveryJob(item)) &&
    isRecord(value.filters)
  );
}

function isIntegrationAlertFailureReportResponse(
  value: unknown,
): value is IntegrationAlertFailureReportResponse {
  return (
    isRecord(value) &&
    isRecord(value.summary) &&
    Array.isArray(value.items) &&
    value.items.every(
      (item) =>
        isRecord(item) &&
        typeof item.occurredAt === "string" &&
        isISODateString(item.occurredAt) &&
        typeof item.action === "string" &&
        typeof item.actionType === "string" &&
        (item.alertId === undefined || item.alertId === null || typeof item.alertId === "string") &&
        (item.externalSystem === undefined || item.externalSystem === null || typeof item.externalSystem === "string") &&
        (item.stage === undefined || item.stage === null || typeof item.stage === "string") &&
        (item.code === undefined || item.code === null || typeof item.code === "string") &&
        (item.status === "requested" || item.status === "success" || item.status === "failed") &&
        isRecord(item.metadata),
    ) &&
    isRecord(value.filters)
  );
}

function isIntegrationAlertFailureTrendResponse(
  value: unknown,
): value is IntegrationAlertFailureTrendResponse {
  return (
    isRecord(value) &&
    isRecord(value.summary) &&
    typeof value.summary.totalEvents === "number" &&
    typeof value.summary.requestedEvents === "number" &&
    typeof value.summary.successEvents === "number" &&
    typeof value.summary.failedEvents === "number" &&
    typeof value.summary.days === "number" &&
    typeof value.summary.averageEventsPerDay === "number" &&
    (value.summary.peakDate === undefined ||
      value.summary.peakDate === null ||
      typeof value.summary.peakDate === "string") &&
    typeof value.summary.peakCount === "number" &&
    Array.isArray(value.daily) &&
    value.daily.every(
      (item) =>
        isRecord(item) &&
        typeof item.date === "string" &&
        typeof item.totalEvents === "number" &&
        typeof item.requestedEvents === "number" &&
        typeof item.successEvents === "number" &&
        typeof item.failedEvents === "number" &&
        typeof item.uniqueAlerts === "number" &&
        typeof item.retryRequested === "number" &&
        typeof item.retryCompleted === "number" &&
        typeof item.retryFailed === "number" &&
        typeof item.dlqQueried === "number" &&
        typeof item.dlqReplayed === "number" &&
        typeof item.recoveryJobsCreated === "number" &&
        typeof item.recoveryJobsCompleted === "number" &&
        typeof item.recoveryJobsFailed === "number",
    ) &&
    isRecord(value.capacity) &&
    Array.isArray(value.capacity.externalSystems) &&
    value.capacity.externalSystems.every(
      (item) =>
        isRecord(item) &&
        typeof item.name === "string" &&
        typeof item.totalEvents === "number" &&
        typeof item.requestedEvents === "number" &&
        typeof item.successEvents === "number" &&
        typeof item.failedEvents === "number" &&
        typeof item.uniqueAlerts === "number" &&
        (item.lastOccurredAt === undefined ||
          item.lastOccurredAt === null ||
          isISODateString(item.lastOccurredAt)),
    ) &&
    Array.isArray(value.capacity.stages) &&
    value.capacity.stages.every(
      (item) =>
        isRecord(item) &&
        typeof item.name === "string" &&
        typeof item.totalEvents === "number" &&
        typeof item.requestedEvents === "number" &&
        typeof item.successEvents === "number" &&
        typeof item.failedEvents === "number" &&
        typeof item.uniqueAlerts === "number" &&
        (item.lastOccurredAt === undefined ||
          item.lastOccurredAt === null ||
          isISODateString(item.lastOccurredAt)),
    ) &&
    isRecord(value.filters) &&
    typeof value.filters.top === "number"
  );
}

function isAlertExternalLinkFailureResponse(
  value: unknown,
): value is AlertExternalLinkFailureResponse {
  if (!isRecord(value) || !isRecord(value.summary) || !isRecord(value.filters)) {
    return false;
  }
  const externalTypeOk =
    value.filters.externalType === undefined ||
    value.filters.externalType === "ticket" ||
    value.filters.externalType === "case" ||
    value.filters.externalType === "incident";
  const syncStateOk =
    value.filters.syncState === undefined ||
    value.filters.syncState === "synced" ||
    value.filters.syncState === "pending" ||
    value.filters.syncState === "failed";
  const alertIdOk =
    value.filters.alertId === undefined ||
    typeof value.filters.alertId === "string";
  const externalSystemOk =
    value.filters.externalSystem === undefined ||
    typeof value.filters.externalSystem === "string";
  const limitOk =
    value.filters.limit === undefined ||
    (typeof value.filters.limit === "number" &&
      Number.isInteger(value.filters.limit) &&
      value.filters.limit > 0);
  return (
    typeof value.summary.total === "number" &&
    typeof value.summary.pending === "number" &&
    typeof value.summary.failed === "number" &&
    Array.isArray(value.items) &&
    value.items.every((item) => isAlertExternalLinkOpsItem(item)) &&
    alertIdOk &&
    externalTypeOk &&
    externalSystemOk &&
    syncStateOk &&
    limitOk
  );
}

function isAlertOrchestrationRule(value: unknown): value is AlertOrchestrationRule {
  if (!isRecord(value)) {
    return false;
  }

  const severityOk = value.severity === undefined || value.severity === null || isAlertSeverity(value.severity);
  const sourceIdOk = value.sourceId === undefined || value.sourceId === null || typeof value.sourceId === "string";
  const slaOk =
    value.slaMinutes === undefined ||
    value.slaMinutes === null ||
    (typeof value.slaMinutes === "number" && Number.isInteger(value.slaMinutes) && value.slaMinutes >= 0);

  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.name === "string" &&
    typeof value.enabled === "boolean" &&
    isAlertOrchestrationEventType(value.eventType) &&
    severityOk &&
    sourceIdOk &&
    typeof value.dedupeWindowSeconds === "number" &&
    Number.isInteger(value.dedupeWindowSeconds) &&
    value.dedupeWindowSeconds >= 0 &&
    typeof value.suppressionWindowSeconds === "number" &&
    Number.isInteger(value.suppressionWindowSeconds) &&
    value.suppressionWindowSeconds >= 0 &&
    typeof value.mergeWindowSeconds === "number" &&
    Number.isInteger(value.mergeWindowSeconds) &&
    value.mergeWindowSeconds >= 0 &&
    slaOk &&
    Array.isArray(value.channels) &&
    value.channels.every((channel) => isAlertOrchestrationChannel(channel)) &&
    isISODateString(value.updatedAt)
  );
}

function isAlertOrchestrationRuleListInput(
  value: unknown
): value is AlertOrchestrationRuleListInput {
  if (!isRecord(value)) {
    return false;
  }
  const eventTypeOk = value.eventType === undefined || isAlertOrchestrationEventType(value.eventType);
  const enabledOk = value.enabled === undefined || typeof value.enabled === "boolean";
  const severityOk = value.severity === undefined || isAlertSeverity(value.severity);
  const sourceIdOk = value.sourceId === undefined || typeof value.sourceId === "string";
  return eventTypeOk && enabledOk && severityOk && sourceIdOk;
}

function isAlertOrchestrationRuleListResponse(
  value: unknown
): value is AlertOrchestrationRuleListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isAlertOrchestrationRule(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isAlertOrchestrationRuleListInput(value.filters)
  );
}

function isAlertOrchestrationExecutionLog(
  value: unknown
): value is AlertOrchestrationExecutionLog {
  if (!isRecord(value)) {
    return false;
  }

  const alertIdOk = value.alertId === undefined || value.alertId === null || typeof value.alertId === "string";
  const severityOk = value.severity === undefined || value.severity === null || isAlertSeverity(value.severity);
  const sourceIdOk = value.sourceId === undefined || value.sourceId === null || typeof value.sourceId === "string";
  const escalationReasonOk =
    value.escalationReason === undefined ||
    value.escalationReason === null ||
    isAlertOrchestrationEscalationReason(value.escalationReason);
  const escalationTargetChannelsOk =
    value.escalationTargetChannels === undefined ||
    value.escalationTargetChannels === null ||
    (Array.isArray(value.escalationTargetChannels) &&
      value.escalationTargetChannels.every((channel) => isAlertOrchestrationChannel(channel)));
  const slaMinutesOk =
    value.slaMinutes === undefined ||
    value.slaMinutes === null ||
    (typeof value.slaMinutes === "number" &&
      Number.isInteger(value.slaMinutes) &&
      value.slaMinutes >= 0);
  return (
    typeof value.id === "string" &&
    typeof value.tenantId === "string" &&
    typeof value.ruleId === "string" &&
    isAlertOrchestrationEventType(value.eventType) &&
    alertIdOk &&
    severityOk &&
    sourceIdOk &&
    Array.isArray(value.channels) &&
    value.channels.every((channel) => isAlertOrchestrationChannel(channel)) &&
    isAlertOrchestrationDispatchMode(value.dispatchMode) &&
    Array.isArray(value.conflictRuleIds) &&
    value.conflictRuleIds.every((ruleId) => typeof ruleId === "string") &&
    typeof value.dedupeHit === "boolean" &&
    typeof value.suppressed === "boolean" &&
    typeof value.simulated === "boolean" &&
    typeof value.escalated === "boolean" &&
    escalationReasonOk &&
    escalationTargetChannelsOk &&
    slaMinutesOk &&
    isRecord(value.metadata) &&
    isISODateString(value.createdAt)
  );
}

function isAlertOrchestrationExecutionListInput(
  value: unknown
): value is AlertOrchestrationExecutionListInput {
  if (!isRecord(value)) {
    return false;
  }
  const ruleIdOk = value.ruleId === undefined || typeof value.ruleId === "string";
  const eventTypeOk = value.eventType === undefined || isAlertOrchestrationEventType(value.eventType);
  const alertIdOk = value.alertId === undefined || typeof value.alertId === "string";
  const severityOk = value.severity === undefined || isAlertSeverity(value.severity);
  const sourceIdOk = value.sourceId === undefined || typeof value.sourceId === "string";
  const dedupeHitOk = value.dedupeHit === undefined || typeof value.dedupeHit === "boolean";
  const suppressedOk = value.suppressed === undefined || typeof value.suppressed === "boolean";
  const dispatchModeOk =
    value.dispatchMode === undefined || isAlertOrchestrationDispatchMode(value.dispatchMode);
  const hasConflictOk = value.hasConflict === undefined || typeof value.hasConflict === "boolean";
  const simulatedOk = value.simulated === undefined || typeof value.simulated === "boolean";
  const escalatedOk = value.escalated === undefined || typeof value.escalated === "boolean";
  const escalationReasonOk =
    value.escalationReason === undefined ||
    isAlertOrchestrationEscalationReason(value.escalationReason);
  const fromOk = value.from === undefined || isISODateString(value.from);
  const toOk = value.to === undefined || isISODateString(value.to);
  const limitOk =
    value.limit === undefined ||
    (typeof value.limit === "number" && Number.isInteger(value.limit) && value.limit > 0);
  return (
    ruleIdOk &&
    eventTypeOk &&
    alertIdOk &&
    severityOk &&
    sourceIdOk &&
    dedupeHitOk &&
    suppressedOk &&
    dispatchModeOk &&
    hasConflictOk &&
    simulatedOk &&
    escalatedOk &&
    escalationReasonOk &&
    fromOk &&
    toOk &&
    limitOk
  );
}

function isAlertOrchestrationExecutionListResponse(
  value: unknown
): value is AlertOrchestrationExecutionListResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isAlertOrchestrationExecutionLog(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    isAlertOrchestrationExecutionListInput(value.filters)
  );
}

function isAlertOrchestrationSimulationResponse(
  value: unknown
): value is AlertOrchestrationSimulationResponse {
  if (!isRecord(value)) {
    return false;
  }
  return (
    Array.isArray(value.matchedRules) &&
    value.matchedRules.every((rule) => isAlertOrchestrationRule(rule)) &&
    Array.isArray(value.conflictRuleIds) &&
    value.conflictRuleIds.every((ruleId) => typeof ruleId === "string") &&
    Array.isArray(value.executions) &&
    value.executions.every((execution) => isAlertOrchestrationExecutionLog(execution))
  );
}

function isSessionSearchResponse(value: unknown): value is SessionSearchResponse {
  if (!isRecord(value)) {
    return false;
  }

  const nextCursorOk =
    value.nextCursor === null || typeof value.nextCursor === "string";
  const filtersOk = value.filters === undefined || isRecord(value.filters);
  const sourceFreshnessOk =
    value.sourceFreshness === undefined ||
    (Array.isArray(value.sourceFreshness) &&
      value.sourceFreshness.every((item) => isSessionSourceFreshness(item)));

  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isSession(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    nextCursorOk &&
    filtersOk &&
    sourceFreshnessOk
  );
}

function buildUsageAggregateQuery(filters?: UsageAggregateFilters): string {
  if (!filters) {
    return "";
  }

  const params = new URLSearchParams();
  if (typeof filters.from === "string" && filters.from.trim().length > 0) {
    params.set("from", filters.from.trim());
  }
  if (typeof filters.to === "string" && filters.to.trim().length > 0) {
    params.set("to", filters.to.trim());
  }
  if (typeof filters.limit === "number" && Number.isInteger(filters.limit) && filters.limit > 0) {
    params.set("limit", String(filters.limit));
  }

  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildUsageWeeklySummaryQuery(input?: UsageWeeklySummaryQueryInput): string {
  if (!input) {
    return "";
  }

  const params = new URLSearchParams();
  if (typeof input.from === "string" && input.from.trim().length > 0) {
    params.set("from", input.from.trim());
  }
  if (typeof input.to === "string" && input.to.trim().length > 0) {
    params.set("to", input.to.trim());
  }
  if (typeof input.metric === "string" && isUsageMetric(input.metric)) {
    params.set("metric", input.metric);
  }
  if (typeof input.timezone === "string" && input.timezone.trim().length > 0) {
    params.set("timezone", input.timezone.trim());
  }

  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildSourceParseFailureQuery(input?: SourceParseFailureQueryInput): string {
  if (!input) {
    return "";
  }

  const params = new URLSearchParams();
  if (typeof input.from === "string" && input.from.trim().length > 0) {
    params.set("from", input.from.trim());
  }
  if (typeof input.to === "string" && input.to.trim().length > 0) {
    params.set("to", input.to.trim());
  }
  if (typeof input.parserKey === "string" && input.parserKey.trim().length > 0) {
    params.set("parserKey", input.parserKey.trim());
  }
  if (typeof input.errorCode === "string" && input.errorCode.trim().length > 0) {
    params.set("errorCode", input.errorCode.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    params.set("limit", String(input.limit));
  }
  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildAlertListQuery(input?: AlertListInput): string {
  if (!input) {
    return "";
  }

  const params = new URLSearchParams();
  if (input.status) {
    params.set("status", input.status);
  }
  if (input.severity) {
    params.set("severity", input.severity);
  }
  if (typeof input.scope === "string" && input.scope.trim().length > 0) {
    params.set("scope", input.scope.trim());
  }
  if (typeof input.scopeRef === "string" && input.scopeRef.trim().length > 0) {
    params.set("scopeRef", input.scopeRef.trim());
  }
  if (typeof input.budgetId === "string" && input.budgetId.trim().length > 0) {
    params.set("budgetId", input.budgetId.trim());
  }
  if (typeof input.from === "string" && input.from.trim().length > 0) {
    params.set("from", input.from.trim());
  }
  if (typeof input.to === "string" && input.to.trim().length > 0) {
    params.set("to", input.to.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    params.set("limit", String(input.limit));
  }
  if (typeof input.cursor === "string" && input.cursor.trim().length > 0) {
    params.set("cursor", input.cursor.trim());
  }

  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildAlertOrchestrationRuleListQuery(
  input?: AlertOrchestrationRuleListInput
): string {
  if (!input) {
    return "";
  }

  const params = new URLSearchParams();
  if (input.eventType) {
    params.set("eventType", input.eventType);
  }
  if (typeof input.enabled === "boolean") {
    params.set("enabled", String(input.enabled));
  }
  if (input.severity) {
    params.set("severity", input.severity);
  }
  if (typeof input.sourceId === "string" && input.sourceId.trim().length > 0) {
    params.set("sourceId", input.sourceId.trim());
  }

  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildAlertOrchestrationExecutionListQuery(
  input?: AlertOrchestrationExecutionListInput
): string {
  if (!input) {
    return "";
  }

  const params = new URLSearchParams();
  if (typeof input.ruleId === "string" && input.ruleId.trim().length > 0) {
    params.set("ruleId", input.ruleId.trim());
  }
  if (input.eventType) {
    params.set("eventType", input.eventType);
  }
  if (typeof input.alertId === "string" && input.alertId.trim().length > 0) {
    params.set("alertId", input.alertId.trim());
  }
  if (input.severity) {
    params.set("severity", input.severity);
  }
  if (typeof input.sourceId === "string" && input.sourceId.trim().length > 0) {
    params.set("sourceId", input.sourceId.trim());
  }
  if (typeof input.dedupeHit === "boolean") {
    params.set("dedupeHit", String(input.dedupeHit));
  }
  if (typeof input.suppressed === "boolean") {
    params.set("suppressed", String(input.suppressed));
  }
  if (input.dispatchMode) {
    params.set("dispatchMode", input.dispatchMode);
  }
  if (typeof input.hasConflict === "boolean") {
    params.set("hasConflict", String(input.hasConflict));
  }
  if (typeof input.simulated === "boolean") {
    params.set("simulated", String(input.simulated));
  }
  if (typeof input.from === "string" && input.from.trim().length > 0) {
    params.set("from", input.from.trim());
  }
  if (typeof input.to === "string" && input.to.trim().length > 0) {
    params.set("to", input.to.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    params.set("limit", String(input.limit));
  }

  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildReplicationJobListQuery(input?: ReplicationJobListInput): string {
  if (!input) {
    return "";
  }

  const params = new URLSearchParams();
  if (input.status) {
    params.set("status", input.status);
  }
  if (typeof input.sourceRegion === "string" && input.sourceRegion.trim().length > 0) {
    params.set("sourceRegion", input.sourceRegion.trim());
  }
  if (typeof input.targetRegion === "string" && input.targetRegion.trim().length > 0) {
    params.set("targetRegion", input.targetRegion.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    params.set("limit", String(input.limit));
  }

  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildRuleAssetListQuery(input?: RuleAssetListInput): string {
  if (!input) {
    return "";
  }

  const params = new URLSearchParams();
  if (input.status) {
    params.set("status", input.status);
  }
  if (typeof input.keyword === "string" && input.keyword.trim().length > 0) {
    params.set("keyword", input.keyword.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    params.set("limit", String(input.limit));
  }

  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildRuleApprovalListQuery(input?: RuleApprovalListInput): string {
  if (!input) {
    return "";
  }

  const params = new URLSearchParams();
  if (typeof input.version === "number" && Number.isInteger(input.version) && input.version > 0) {
    params.set("version", String(input.version));
  }
  if (input.decision) {
    params.set("decision", input.decision);
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    params.set("limit", String(input.limit));
  }
  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildMcpToolPolicyListQuery(input?: McpToolPolicyListInput): string {
  if (!input) {
    return "";
  }

  const params = new URLSearchParams();
  if (input.riskLevel) {
    params.set("riskLevel", input.riskLevel);
  }
  if (input.decision) {
    params.set("decision", input.decision);
  }
  if (typeof input.keyword === "string" && input.keyword.trim().length > 0) {
    params.set("keyword", input.keyword.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    params.set("limit", String(input.limit));
  }

  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildMcpApprovalListQuery(input?: McpApprovalListInput): string {
  if (!input) {
    return "";
  }

  const params = new URLSearchParams();
  if (input.status) {
    params.set("status", input.status);
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    params.set("limit", String(input.limit));
  }

  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildMcpInvocationListQuery(input?: McpInvocationListInput): string {
  if (!input) {
    return "";
  }

  const params = new URLSearchParams();
  if (typeof input.toolId === "string" && input.toolId.trim().length > 0) {
    params.set("toolId", input.toolId.trim());
  }
  if (input.decision) {
    params.set("decision", input.decision);
  }
  if (typeof input.from === "string" && input.from.trim().length > 0) {
    params.set("from", input.from.trim());
  }
  if (typeof input.to === "string" && input.to.trim().length > 0) {
    params.set("to", input.to.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    params.set("limit", String(input.limit));
  }

  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildOpenPlatformApiKeyListQuery(input?: OpenPlatformApiKeyListInput): string {
  if (!input) {
    return "";
  }
  const params = new URLSearchParams();
  if (input.status) {
    params.set("status", input.status === "disabled" ? "revoked" : input.status);
  }
  if (typeof input.keyword === "string" && input.keyword.trim().length > 0) {
    params.set("keyword", input.keyword.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit >= 1) {
    params.set("limit", String(input.limit));
  }
  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildOpenPlatformWebhookListQuery(input?: OpenPlatformWebhookListInput): string {
  if (!input) {
    return "";
  }
  const params = new URLSearchParams();
  if (typeof input.enabled === "boolean") {
    params.set("status", input.enabled ? "active" : "paused");
  }
  if (typeof input.keyword === "string" && input.keyword.trim().length > 0) {
    params.set("keyword", input.keyword.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit >= 1) {
    params.set("limit", String(input.limit));
  }
  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildOpenPlatformQualityDailyQuery(input?: OpenPlatformQualityDailyQueryInput): string {
  if (!input) {
    return "";
  }
  const params = new URLSearchParams();
  const date = typeof input.date === "string" ? input.date.trim() : "";
  const from = typeof input.from === "string" ? input.from.trim() : "";
  const to = typeof input.to === "string" ? input.to.trim() : "";
  if (date) {
    const normalized = date;
    params.set("from", `${normalized}T00:00:00.000Z`);
    params.set("to", `${normalized}T23:59:59.999Z`);
  } else {
    if (from) {
      params.set("from", from);
    }
    if (to) {
      params.set("to", to);
    }
  }
  if (typeof input.metric === "string" && input.metric.trim().length > 0) {
    params.set("metric", input.metric.trim());
  }
  if (typeof input.provider === "string" && input.provider.trim().length > 0) {
    params.set("provider", input.provider.trim());
  }
  if (typeof input.repo === "string" && input.repo.trim().length > 0) {
    params.set("repo", input.repo.trim());
  }
  if (typeof input.workflow === "string" && input.workflow.trim().length > 0) {
    params.set("workflow", input.workflow.trim());
  }
  if (typeof input.runId === "string" && input.runId.trim().length > 0) {
    params.set("runId", input.runId.trim());
  }
  if (
    input.groupBy === "provider" ||
    input.groupBy === "repo" ||
    input.groupBy === "workflow" ||
    input.groupBy === "runId"
  ) {
    params.set("groupBy", input.groupBy);
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit >= 1) {
    params.set("limit", String(input.limit));
  }
  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildOpenPlatformQualityScorecardListQuery(
  input?: OpenPlatformQualityScorecardListInput
): string {
  if (!input) {
    return "";
  }
  const params = new URLSearchParams();
  if (typeof input.team === "string" && input.team.trim().length > 0) {
    params.set("metric", input.team.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit >= 1) {
    params.set("limit", String(input.limit));
  }
  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildOpenPlatformQualityProjectTrendQuery(
  input?: OpenPlatformQualityProjectTrendQueryInput
): string {
  if (!input) {
    return "";
  }
  const params = new URLSearchParams();
  if (typeof input.from === "string" && input.from.trim().length > 0) {
    params.set("from", input.from.trim());
  }
  if (typeof input.to === "string" && input.to.trim().length > 0) {
    params.set("to", input.to.trim());
  }
  if (typeof input.metric === "string" && input.metric.trim().length > 0) {
    params.set("metric", input.metric.trim());
  }
  if (typeof input.provider === "string" && input.provider.trim().length > 0) {
    params.set("provider", input.provider.trim());
  }
  if (typeof input.workflow === "string" && input.workflow.trim().length > 0) {
    params.set("workflow", input.workflow.trim());
  }
  if (typeof input.includeUnknown === "boolean") {
    params.set("includeUnknown", input.includeUnknown ? "true" : "false");
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit >= 1) {
    params.set("limit", String(input.limit));
  }
  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildOpenPlatformReplayBaselineListQuery(
  input?: OpenPlatformReplayBaselineListInput
): string {
  if (!input) {
    return "";
  }
  const params = new URLSearchParams();
  if (typeof input.keyword === "string" && input.keyword.trim().length > 0) {
    params.set("keyword", input.keyword.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit >= 1) {
    params.set("limit", String(input.limit));
  }
  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildOpenPlatformReplayJobListQuery(input?: OpenPlatformReplayJobListInput): string {
  if (!input) {
    return "";
  }
  const params = new URLSearchParams();
  const datasetId =
    typeof input.datasetId === "string" && input.datasetId.trim().length > 0
      ? input.datasetId.trim()
      : typeof input.baselineId === "string" && input.baselineId.trim().length > 0
        ? input.baselineId.trim()
        : "";
  if (datasetId) {
    params.set("datasetId", datasetId);
  }
  if (input.status) {
    params.set("status", input.status);
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit >= 1) {
    params.set("limit", String(input.limit));
  }
  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
}

function buildOpenPlatformReplayDiffQuery(input: OpenPlatformReplayDiffQueryInput): string {
  const params = new URLSearchParams();
  const datasetId =
    typeof input.datasetId === "string" && input.datasetId.trim().length > 0
      ? input.datasetId.trim()
      : typeof input.baselineId === "string" && input.baselineId.trim().length > 0
        ? input.baselineId.trim()
        : "";
  if (datasetId) {
    params.set("datasetId", datasetId);
  }
  if (typeof input.keyword === "string" && input.keyword.trim().length > 0) {
    params.set("keyword", input.keyword.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit >= 1) {
    params.set("limit", String(input.limit));
  }
  return params.toString().length > 0 ? `?${params.toString()}` : "";
}

function buildSessionExportQuery(format: ExportFormat, input?: SessionSearchInput): string {
  const params = new URLSearchParams();
  params.set("format", format);

  if (input) {
    if (typeof input.sourceId === "string" && input.sourceId.trim().length > 0) {
      params.set("sourceId", input.sourceId.trim());
    }
    if (typeof input.keyword === "string" && input.keyword.trim().length > 0) {
      params.set("keyword", input.keyword.trim());
    }
    if (typeof input.clientType === "string" && input.clientType.trim().length > 0) {
      params.set("clientType", input.clientType.trim());
    }
    if (typeof input.tool === "string" && input.tool.trim().length > 0) {
      params.set("tool", input.tool.trim());
    }
    if (typeof input.host === "string" && input.host.trim().length > 0) {
      params.set("host", input.host.trim());
    }
    if (typeof input.model === "string" && input.model.trim().length > 0) {
      params.set("model", input.model.trim());
    }
    if (typeof input.project === "string" && input.project.trim().length > 0) {
      params.set("project", input.project.trim());
    }
    if (typeof input.from === "string" && input.from.trim().length > 0) {
      params.set("from", input.from.trim());
    }
    if (typeof input.to === "string" && input.to.trim().length > 0) {
      params.set("to", input.to.trim());
    }
    if (
      typeof input.limit === "number" &&
      Number.isInteger(input.limit) &&
      input.limit > 0
    ) {
      params.set("limit", String(input.limit));
    }
    if (typeof input.cursor === "string" && input.cursor.trim().length > 0) {
      params.set("cursor", input.cursor.trim());
    }
  }

  return `?${params.toString()}`;
}

function buildUsageExportQuery(format: ExportFormat, input: UsageExportQueryInput): string {
  const params = new URLSearchParams();
  params.set("format", format);
  params.set("dimension", input.dimension);

  if (typeof input.from === "string" && input.from.trim().length > 0) {
    params.set("from", input.from.trim());
  }
  if (typeof input.to === "string" && input.to.trim().length > 0) {
    params.set("to", input.to.trim());
  }
  if (
    typeof input.limit === "number" &&
    Number.isInteger(input.limit) &&
    input.limit > 0
  ) {
    params.set("limit", String(input.limit));
  }
  if (typeof input.timezone === "string" && input.timezone.trim().length > 0) {
    params.set("timezone", input.timezone.trim());
  }

  return `?${params.toString()}`;
}

function buildTokenPulseRuntimeEventListQuery(
  input: TokenPulseRuntimeEventListInput = {},
): string {
  const params = new URLSearchParams();
  if (typeof input.traceId === "string" && input.traceId.trim().length > 0) {
    params.set("traceId", input.traceId.trim());
  }
  if (typeof input.provider === "string" && input.provider.trim().length > 0) {
    params.set("provider", input.provider.trim().toLowerCase());
  }
  if (typeof input.status === "string" && input.status.trim().length > 0) {
    params.set("status", input.status.trim());
  }
  if (
    typeof input.limit === "number" &&
    Number.isInteger(input.limit) &&
    input.limit > 0
  ) {
    params.set("limit", String(input.limit));
  }
  if (typeof input.cursor === "string" && input.cursor.trim().length > 0) {
    params.set("cursor", input.cursor.trim());
  }
  return `?${params.toString()}`;
}

function isUsageAggregateResponse<TItem>(
  value: unknown
): value is UsageAggregateResponse<TItem> {
  return isRecord(value) && Array.isArray(value.items);
}

function isUsageWeekItem(value: unknown): value is UsageWeekItem {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.weekStart === "string" &&
    !Number.isNaN(Date.parse(value.weekStart)) &&
    typeof value.weekEnd === "string" &&
    !Number.isNaN(Date.parse(value.weekEnd)) &&
    typeof value.tokens === "number" &&
    Number.isInteger(value.tokens) &&
    value.tokens >= 0 &&
    typeof value.cost === "number" &&
    Number.isFinite(value.cost) &&
    value.cost >= 0 &&
    typeof value.sessions === "number" &&
    Number.isInteger(value.sessions) &&
    value.sessions >= 0
  );
}

function isUsageWeeklySummaryResponse(
  value: unknown
): value is UsageWeeklySummaryResponse {
  if (!isRecord(value) || !Array.isArray(value.weeks) || !isRecord(value.summary)) {
    return false;
  }

  const peakWeekOk =
    value.peakWeek === undefined ||
    value.peakWeek === null ||
    isUsageWeekItem(value.peakWeek);
  return (
    isUsageMetric(value.metric) &&
    typeof value.timezone === "string" &&
    value.timezone.trim().length > 0 &&
    value.weeks.every((item) => isUsageWeekItem(item)) &&
    typeof value.summary.tokens === "number" &&
    Number.isInteger(value.summary.tokens) &&
    value.summary.tokens >= 0 &&
    typeof value.summary.cost === "number" &&
    Number.isFinite(value.summary.cost) &&
    value.summary.cost >= 0 &&
    typeof value.summary.sessions === "number" &&
    Number.isInteger(value.summary.sessions) &&
    value.summary.sessions >= 0 &&
    peakWeekOk
  );
}

function isPricingCatalogEntry(value: unknown): value is PricingCatalogEntry {
  if (!isRecord(value)) {
    return false;
  }

  const inputPer1k = value.inputPer1k;
  const outputPer1k = value.outputPer1k;
  return (
    typeof value.model === "string" &&
    typeof inputPer1k === "number" &&
    Number.isFinite(inputPer1k) &&
    typeof outputPer1k === "number" &&
    Number.isFinite(outputPer1k)
  );
}

function isPricingCatalog(value: unknown): value is PricingCatalog {
  if (!isRecord(value)) {
    return false;
  }
  if (!isRecord(value.version) || !Array.isArray(value.entries)) {
    return false;
  }

  return value.entries.every((entry) => isPricingCatalogEntry(entry));
}

function isSessionEventListResponse(value: unknown): value is SessionEventListResponse {
  if (!isRecord(value) || !Array.isArray(value.items) || typeof value.total !== "number") {
    return false;
  }
  const nextCursor = value.nextCursor;
  return (
    nextCursor === undefined ||
    nextCursor === null ||
    typeof nextCursor === "string"
  );
}

function isSessionDetailResponse(value: unknown): value is SessionDetailResponse {
  if (!isRecord(value)) {
    return false;
  }

  const tokenBreakdown = value.tokenBreakdown;
  const sourceTrace = value.sourceTrace;
  return (
    isRecord(tokenBreakdown) &&
    typeof tokenBreakdown.inputTokens === "number" &&
    typeof tokenBreakdown.outputTokens === "number" &&
    typeof tokenBreakdown.cacheReadTokens === "number" &&
    typeof tokenBreakdown.cacheWriteTokens === "number" &&
    typeof tokenBreakdown.reasoningTokens === "number" &&
    typeof tokenBreakdown.totalTokens === "number" &&
    isRecord(sourceTrace) &&
    typeof sourceTrace.sourceId === "string"
  );
}

function isSourceConnectionTestResponse(
  value: unknown
): value is SourceConnectionTestResponse {
  return (
    isRecord(value) &&
    typeof value.sourceId === "string" &&
    typeof value.success === "boolean" &&
    isSourceType(value.mode) &&
    typeof value.latencyMs === "number" &&
    typeof value.detail === "string"
  );
}

export class ApiError extends Error {
  status: number;
  payload: unknown;

  constructor(status: number, message: string, payload?: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.payload = payload;
  }
}

type UnauthorizedHandler = (message: string) => void;

let unauthorizedHandler: UnauthorizedHandler | null = null;
let currentAuthTokens: AuthTokens | null = readAuthTokensFromStorage();
let refreshTokensInFlight: Promise<AuthTokens> | null = null;

function getStorage(): Storage | null {
  if (typeof globalThis === "undefined" || !("localStorage" in globalThis)) {
    return null;
  }
  try {
    return globalThis.localStorage;
  } catch {
    return null;
  }
}

function isAuthTokens(value: unknown): value is AuthTokens {
  if (!value || typeof value !== "object") {
    return false;
  }

  const tokens = value as Partial<AuthTokens>;
  return (
    typeof tokens.accessToken === "string" &&
    tokens.accessToken.trim().length > 0 &&
    typeof tokens.refreshToken === "string" &&
    tokens.refreshToken.trim().length > 0 &&
    typeof tokens.expiresIn === "number" &&
    Number.isFinite(tokens.expiresIn) &&
    tokens.expiresIn > 0 &&
    tokens.tokenType === "Bearer"
  );
}

function isAuthUserProfile(value: unknown): value is AuthUserProfile {
  if (!value || typeof value !== "object") {
    return false;
  }

  const profile = value as Partial<AuthUserProfile>;
  const tenantId =
    profile.tenantId === undefined || typeof profile.tenantId === "string";
  const tenantRole =
    profile.tenantRole === undefined || typeof profile.tenantRole === "string";
  return (
    typeof profile.userId === "string" &&
    typeof profile.email === "string" &&
    typeof profile.displayName === "string" &&
    tenantId &&
    tenantRole
  );
}

function isAuthProviderItem(value: unknown): value is AuthProviderItem {
  if (!isRecord(value)) {
    return false;
  }

  const issuerOk = value.issuer === undefined || typeof value.issuer === "string";
  const authorizationUrlOk =
    value.authorizationUrl === undefined || typeof value.authorizationUrl === "string";
  const requireMfaOk =
    value.requireMfa === undefined || typeof value.requireMfa === "boolean";
  return (
    typeof value.id === "string" &&
    typeof value.type === "string" &&
    ["local", "oauth2", "oidc", "sso", "saml"].includes(value.type) &&
    typeof value.displayName === "string" &&
    typeof value.enabled === "boolean" &&
    issuerOk &&
    authorizationUrlOk &&
    requireMfaOk
  );
}

function isAuthProviderListResponse(value: unknown): value is AuthProviderListResponse {
  if (!isRecord(value) || !Array.isArray(value.items)) {
    return false;
  }

  if (
    value.total !== undefined &&
    (typeof value.total !== "number" || !Number.isFinite(value.total))
  ) {
    return false;
  }

  return value.items.every((item) => isAuthProviderItem(item));
}

function isAuthLoginResponse(value: unknown): value is AuthLoginResponse {
  if (!value || typeof value !== "object") {
    return false;
  }

  const payload = value as { user?: unknown; tokens?: unknown };
  return isAuthUserProfile(payload.user) && isAuthTokens(payload.tokens);
}

function isAuthRefreshResponse(value: unknown): value is AuthRefreshResponse {
  if (!value || typeof value !== "object") {
    return false;
  }

  const payload = value as Partial<AuthRefreshResponse>;
  return isAuthTokens(payload.tokens);
}

function extractAuthTokens(value: unknown): AuthTokens | null {
  if (isAuthTokens(value)) {
    return value;
  }

  if (isAuthRefreshResponse(value)) {
    return value.tokens;
  }

  return null;
}

function readAuthTokensFromStorage(): AuthTokens | null {
  const storage = getStorage();
  if (!storage) {
    return null;
  }

  const raw = storage.getItem(AUTH_STORAGE_KEY);
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!isAuthTokens(parsed)) {
      storage.removeItem(AUTH_STORAGE_KEY);
      return null;
    }
    return parsed;
  } catch {
    storage.removeItem(AUTH_STORAGE_KEY);
    return null;
  }
}

function saveAuthTokensToStorage(tokens: AuthTokens) {
  const storage = getStorage();
  if (!storage) {
    return;
  }
  storage.setItem(AUTH_STORAGE_KEY, JSON.stringify(tokens));
}

function readErrorMessage(payload: unknown, status: number): string {
  if (payload && typeof payload === "object") {
    const message = (payload as { message?: unknown }).message;
    if (typeof message === "string" && message.trim().length > 0) {
      return message;
    }
  }
  if (typeof payload === "string" && payload.trim().length > 0) {
    return payload;
  }
  return `请求失败: ${status}`;
}

async function readErrorPayload(response: Response): Promise<unknown> {
  const contentType = response.headers?.get?.("content-type") ?? "";
  if (contentType.toLowerCase().includes("application/json")) {
    try {
      return await response.json();
    } catch {
      return undefined;
    }
  }

  try {
    const text = await response.text();
    return text.trim().length > 0 ? text : undefined;
  } catch {
    return undefined;
  }
}

function isUnauthorizedError(error: unknown): boolean {
  return error instanceof ApiError && error.status === 401;
}

export function hasAccessToken(): boolean {
  return typeof currentAuthTokens?.accessToken === "string";
}

export function getAccessToken(): string | null {
  return currentAuthTokens?.accessToken ?? null;
}

export function setAuthTokens(tokens: AuthTokens) {
  currentAuthTokens = tokens;
  saveAuthTokensToStorage(tokens);
}

export function clearAuthTokens() {
  currentAuthTokens = null;
  refreshTokensInFlight = null;
  const storage = getStorage();
  storage?.removeItem(AUTH_STORAGE_KEY);
}

export function setUnauthorizedHandler(handler: UnauthorizedHandler | null) {
  unauthorizedHandler = handler;
}

interface RequestOptions {
  skipAuth?: boolean;
  skipUnauthorizedHandling?: boolean;
  skipRefresh?: boolean;
}

async function requestJson<T>(
  path: string,
  init?: RequestInit,
  signal?: AbortSignal,
  options?: RequestOptions
): Promise<T> {
  return requestJsonInternal(path, init, signal, options, false);
}

function parseContentDispositionFilename(headerValue: string | null): string | null {
  if (!headerValue) {
    return null;
  }

  const encodedMatch = headerValue.match(/filename\*=UTF-8''([^;]+)/i);
  if (encodedMatch?.[1]) {
    try {
      const decoded = decodeURIComponent(encodedMatch[1]);
      const sanitized = decoded.replace(/["\r\n]/g, "").trim();
      return sanitized.length > 0 ? sanitized : null;
    } catch {
      // fall through and try plain filename
    }
  }

  const plainMatch = headerValue.match(/filename="?([^"]+)"?/i);
  if (!plainMatch?.[1]) {
    return null;
  }
  const sanitized = plainMatch[1].replace(/["\r\n]/g, "").trim();
  return sanitized.length > 0 ? sanitized : null;
}

async function requestBlob(
  path: string,
  defaultFilename: string,
  init?: RequestInit,
  signal?: AbortSignal,
  options?: RequestOptions
): Promise<DownloadFile> {
  const response = await requestResponseInternal(path, init, signal, options, false);
  const contentType = response.headers.get("content-type") ?? "application/octet-stream";
  const filename =
    parseContentDispositionFilename(response.headers.get("content-disposition")) ??
    defaultFilename;
  const blob = await response.blob();
  return {
    blob,
    filename,
    contentType,
  };
}

function readMessageFromError(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error && error.message.trim().length > 0) {
    return error.message;
  }
  return fallback;
}

async function refreshAuthTokens(): Promise<AuthTokens> {
  if (refreshTokensInFlight) {
    return refreshTokensInFlight;
  }

  const refreshToken = currentAuthTokens?.refreshToken?.trim();
  if (!refreshToken) {
    throw new ApiError(401, "未认证：请先登录。");
  }

  refreshTokensInFlight = (async () => {
    const input: AuthRefreshInput = { refreshToken };
    const result = await requestJsonInternal<unknown>(
      "/api/v1/auth/refresh",
      {
        method: "POST",
        body: JSON.stringify(input),
      },
      undefined,
      { skipAuth: true, skipUnauthorizedHandling: true, skipRefresh: true },
      true
    );

    const tokens = extractAuthTokens(result);
    if (!tokens) {
      throw new Error("auth.refresh 返回结构不合法");
    }

    setAuthTokens(tokens);
    return tokens;
  })().finally(() => {
    refreshTokensInFlight = null;
  });

  return refreshTokensInFlight;
}

async function requestJsonInternal<T>(
  path: string,
  init?: RequestInit,
  signal?: AbortSignal,
  options?: RequestOptions,
  hasRetriedUnauthorized = false
): Promise<T> {
  const response = await requestResponseInternal(
    path,
    init,
    signal,
    options,
    hasRetriedUnauthorized
  );
  return (await response.json()) as T;
}

async function requestResponseInternal(
  path: string,
  init?: RequestInit,
  signal?: AbortSignal,
  options?: RequestOptions,
  hasRetriedUnauthorized = false
): Promise<Response> {
  const headers = new Headers(init?.headers);
  if (!headers.has("content-type")) {
    headers.set("content-type", "application/json");
  }

  const accessToken = getAccessToken();
  if (!options?.skipAuth && accessToken && !headers.has("authorization")) {
    headers.set("authorization", `Bearer ${accessToken}`);
  }

  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    headers,
    signal,
  });

  if (!response.ok) {
    const payload = await readErrorPayload(response);
    const message = readErrorMessage(payload, response.status);

    const canTryRefresh =
      response.status === 401 &&
      !hasRetriedUnauthorized &&
      !options?.skipUnauthorizedHandling &&
      !options?.skipRefresh &&
      !options?.skipAuth &&
      typeof currentAuthTokens?.refreshToken === "string" &&
      currentAuthTokens.refreshToken.trim().length > 0;

    if (canTryRefresh) {
      try {
        await refreshAuthTokens();
        return await requestResponseInternal(path, init, signal, options, true);
      } catch (refreshError) {
        const refreshMessage = readMessageFromError(refreshError, message);
        if (!options?.skipUnauthorizedHandling) {
          clearAuthTokens();
          unauthorizedHandler?.(refreshMessage);
        }
        if (refreshError instanceof ApiError) {
          throw refreshError;
        }
        throw new ApiError(401, refreshMessage, refreshError);
      }
    }

    if (response.status === 401 && !options?.skipUnauthorizedHandling) {
      clearAuthTokens();
      unauthorizedHandler?.(message);
    }
    throw new ApiError(response.status, message, payload);
  }

  return response;
}

export async function login(
  input: AuthLoginInput,
  signal?: AbortSignal
): Promise<AuthLoginResponse> {
  const result = await requestJson<unknown>(
    "/api/v1/auth/login",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal,
    { skipAuth: true, skipUnauthorizedHandling: true }
  );

  if (!isAuthLoginResponse(result)) {
    throw new Error("auth.login 返回结构不合法");
  }

  setAuthTokens(result.tokens);
  return result;
}

export async function fetchAuthProviders(
  signal?: AbortSignal
): Promise<AuthProviderListResponse> {
  const result = await requestJson<unknown>(
    "/api/v1/auth/providers",
    undefined,
    signal,
    { skipAuth: true, skipUnauthorizedHandling: true }
  );

  if (!isAuthProviderListResponse(result)) {
    throw new Error("auth.providers 返回结构不合法");
  }

  return {
    items: result.items,
    total: typeof result.total === "number" ? result.total : result.items.length,
  };
}

export async function exchangeExternalAuthCode(
  input: AuthExternalExchangeInput,
  signal?: AbortSignal
): Promise<AuthLoginResponse> {
  const providerId = input.providerId.trim().toLowerCase();
  const code = input.code.trim();
  const redirectUri = input.redirectUri.trim();
  const codeVerifier = input.codeVerifier?.trim();
  const state = input.state?.trim();

  if (!providerId) {
    throw new Error("providerId 不能为空。");
  }
  if (!code) {
    throw new Error("code 不能为空。");
  }
  if (!redirectUri) {
    throw new Error("redirectUri 不能为空。");
  }

  const payload = {
    providerId,
    code,
    redirectUri,
    ...(codeVerifier ? { codeVerifier } : {}),
    ...(state ? { state } : {}),
  };

  const result = await requestJson<unknown>(
    "/api/v1/auth/external/exchange",
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    signal,
    { skipAuth: true, skipUnauthorizedHandling: true }
  );

  if (!isAuthLoginResponse(result)) {
    throw new Error("auth.external.exchange 返回结构不合法");
  }

  setAuthTokens(result.tokens);
  return result;
}

export async function fetchHeatmap(signal?: AbortSignal): Promise<UsageHeatmapResponse> {
  try {
    const result = await requestJson<UsageHeatmapResponse>(
      "/api/v1/usage/heatmap",
      undefined,
      signal
    );
    if (!Array.isArray(result.cells) || !result.summary) {
      throw new Error("heatmap 返回结构不合法");
    }
    return result;
  } catch (error) {
    if (isUnauthorizedError(error) || !shouldUseMockFallback()) {
      throw error;
    }
    return buildMockHeatmap();
  }
}

export async function fetchSources(signal?: AbortSignal): Promise<SourceListResponse> {
  const result = await requestJson<SourceListResponse>("/api/v1/sources", undefined, signal);

  if (!Array.isArray(result.items)) {
    throw new Error("sources 返回结构不合法");
  }

  for (const item of result.items) {
    if (!isSource(item)) {
      throw new Error("sources 返回结构不合法");
    }
  }

  return {
    items: result.items,
    total: typeof result.total === "number" ? result.total : result.items.length,
  };
}

export async function fetchSourcesMissingRegion(
  signal?: AbortSignal
): Promise<SourceMissingRegionListResponse> {
  const result = await requestJson<unknown>("/api/v1/sources/missing-region", undefined, signal);
  if (!isSourceMissingRegionListResponse(result)) {
    throw new Error("sources.missing-region 返回结构不合法");
  }
  return result;
}

export async function fetchSourceHealth(
  sourceId: string,
  signal?: AbortSignal
): Promise<SourceHealth> {
  const normalizedSourceId = sourceId.trim();
  if (!normalizedSourceId) {
    throw new Error("sourceId 不能为空。");
  }

  const result = await requestJson<unknown>(
    `/api/v1/sources/${encodeURIComponent(normalizedSourceId)}/health`,
    undefined,
    signal
  );
  if (!isSourceHealth(result)) {
    throw new Error("sources.health 返回结构不合法");
  }
  return result;
}

export async function fetchSourceParseFailures(
  sourceId: string,
  input?: SourceParseFailureQueryInput,
  signal?: AbortSignal
): Promise<SourceParseFailureListResponse> {
  const normalizedSourceId = sourceId.trim();
  if (!normalizedSourceId) {
    throw new Error("sourceId 不能为空。");
  }

  const result = await requestJson<unknown>(
    `/api/v1/sources/${encodeURIComponent(normalizedSourceId)}/parse-failures${buildSourceParseFailureQuery(input)}`,
    undefined,
    signal
  );
  if (!isSourceParseFailureListResponse(result)) {
    throw new Error("sources.parse-failures 返回结构不合法");
  }
  return result;
}

export async function createSource(
  input: CreateSourceInput,
  signal?: AbortSignal
): Promise<Source> {
  const result = await requestJson<unknown>(
    "/api/v1/sources",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal
  );

  if (isSource(result)) {
    return result;
  }

  if (result && typeof result === "object") {
    const item = (result as { item?: unknown }).item;
    if (isSource(item)) {
      return item;
    }

    const source = (result as { source?: unknown }).source;
    if (isSource(source)) {
      return source;
    }
  }

  throw new Error("sources.create 返回结构不合法");
}

export async function updateSource(
  sourceId: string,
  input: UpdateSourceInput,
  signal?: AbortSignal
): Promise<Source> {
  const normalizedSourceId = sourceId.trim();
  if (!normalizedSourceId) {
    throw new Error("sourceId 不能为空。");
  }

  const result = await requestJson<unknown>(
    `/api/v1/sources/${encodeURIComponent(normalizedSourceId)}`,
    {
      method: "PATCH",
      body: JSON.stringify(input),
    },
    signal
  );

  if (!isSource(result)) {
    throw new Error("sources.update 返回结构不合法");
  }
  return result;
}

export async function backfillSourceRegions(
  input: {
    dryRun?: boolean;
    sourceIds?: string[];
  } = {},
  signal?: AbortSignal
): Promise<SourceRegionBackfillResult> {
  const payload = {
    ...(input.dryRun !== undefined ? { dryRun: input.dryRun } : {}),
    ...(Array.isArray(input.sourceIds) && input.sourceIds.length > 0
      ? {
          sourceIds: input.sourceIds
            .map((item) => item.trim())
            .filter((item) => item.length > 0),
        }
      : {}),
  };
  const result = await requestJson<unknown>(
    "/api/v1/sources/source-region/backfill",
    {
      method: "POST",
      body: JSON.stringify(payload),
    },
    signal
  );
  if (!isSourceRegionBackfillResult(result)) {
    throw new Error("sources.source-region.backfill 返回结构不合法");
  }
  return result;
}

export async function fetchAlerts(
  input?: AlertListInput,
  signal?: AbortSignal
): Promise<AlertListResponse> {
  const baseInput = input ? { ...input } : {};
  const dedupedItems: AlertItem[] = [];
  const seenIds = new Set<string>();
  let cursor = baseInput.cursor;
  let total = 0;
  let firstPageFilters: AlertListInput | null = null;

  for (let page = 0; page < 200; page += 1) {
    const result = await requestJson<unknown>(
      `/api/v1/alerts${buildAlertListQuery({ ...baseInput, cursor })}`,
      undefined,
      signal
    );
    if (!isAlertListResponse(result)) {
      throw new Error("alerts 返回结构不合法");
    }
    if (page === 0) {
      total = result.total;
      firstPageFilters = result.filters;
    }

    for (const item of result.items) {
      if (seenIds.has(item.id)) {
        continue;
      }
      seenIds.add(item.id);
      dedupedItems.push(item);
    }

    if (!result.nextCursor) {
      return {
        items: dedupedItems,
        total,
        filters: firstPageFilters ?? result.filters,
        nextCursor: null,
      };
    }
    cursor = result.nextCursor;
  }

  throw new Error("alerts 分页超过安全上限，请收窄筛选条件后重试。");
}

export async function fetchTokenPulseRuntimeEvents(
  input: TokenPulseRuntimeEventListInput = {},
  signal?: AbortSignal,
): Promise<TokenPulseRuntimeEventListResponse> {
  const result = await requestJson<unknown>(
    `/api/v1/integrations/tokenpulse/runtime-events${buildTokenPulseRuntimeEventListQuery(input)}`,
    undefined,
    signal,
  );
  if (!isTokenPulseRuntimeEventListResponse(result)) {
    throw new Error("tokenpulse runtime events 返回结构不合法");
  }
  return result;
}

export async function updateAlertStatus(
  alertId: string,
  status: AlertMutableStatus,
  signal?: AbortSignal
): Promise<AlertItem> {
  const normalizedAlertId = alertId.trim();
  if (!normalizedAlertId) {
    throw new Error("alertId 不能为空。");
  }
  if (!isAlertMutableStatus(status)) {
    throw new Error("status 必须是 acknowledged 或 resolved。");
  }

  const result = await requestJson<unknown>(
    `/api/v1/alerts/${encodeURIComponent(normalizedAlertId)}/status`,
    {
      method: "PATCH",
      body: JSON.stringify({ status }),
    },
    signal
  );

  if (!isAlertItem(result)) {
    throw new Error("alerts.status 返回结构不合法");
  }
  return result;
}

export async function retryAlertExternalLinkSync(
  alertId: string,
  input: {
    externalType: "ticket" | "case" | "incident";
    externalId: string;
  },
  signal?: AbortSignal,
): Promise<AlertItem> {
  const normalizedAlertId = alertId.trim();
  const normalizedExternalId = input.externalId.trim();
  if (!normalizedAlertId) {
    throw new Error("alertId 不能为空。");
  }
  if (
    input.externalType !== "ticket" &&
    input.externalType !== "case" &&
    input.externalType !== "incident"
  ) {
    throw new Error("externalType 必须是 ticket/case/incident 之一。");
  }
  if (!normalizedExternalId) {
    throw new Error("externalId 不能为空。");
  }

  const result = await requestJson<unknown>(
    `/api/v1/alerts/${encodeURIComponent(normalizedAlertId)}/external-links/retry-sync`,
    {
      method: "POST",
      body: JSON.stringify({
        externalType: input.externalType,
        externalId: normalizedExternalId,
      }),
    },
    signal,
  );

  if (!isAlertItem(result)) {
    throw new Error("alerts.external-links.retry-sync 返回结构不合法");
  }
  return result;
}

export async function fetchAlertExternalLinkOps(
  alertId: string,
  input: {
    externalType?: "ticket" | "case" | "incident";
    onlyFailed?: boolean;
  } = {},
  signal?: AbortSignal,
): Promise<AlertExternalLinkOpsResponse> {
  const normalizedAlertId = alertId.trim();
  if (!normalizedAlertId) {
    throw new Error("alertId 不能为空。");
  }
  const query = new URLSearchParams();
  if (input.externalType) {
    query.set("externalType", input.externalType);
  }
  if (typeof input.onlyFailed === "boolean") {
    query.set("onlyFailed", input.onlyFailed ? "true" : "false");
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v1/alerts/${encodeURIComponent(normalizedAlertId)}/external-links${suffix}`,
    undefined,
    signal,
  );
  if (!isAlertExternalLinkOpsResponse(result)) {
    throw new Error("alerts.external-links 返回结构不合法");
  }
  return result;
}

export async function fetchAlertExternalLinkFailures(
  input: {
    alertId?: string;
    externalType?: "ticket" | "case" | "incident";
    externalSystem?: string;
    syncState?: "synced" | "pending" | "failed";
    limit?: number;
  } = {},
  signal?: AbortSignal,
): Promise<AlertExternalLinkFailureResponse> {
  const query = new URLSearchParams();
  if (input.alertId?.trim()) {
    query.set("alertId", input.alertId.trim());
  }
  if (input.externalType) {
    query.set("externalType", input.externalType);
  }
  if (input.externalSystem?.trim()) {
    query.set("externalSystem", input.externalSystem.trim());
  }
  if (input.syncState) {
    query.set("syncState", input.syncState);
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    query.set("limit", String(input.limit));
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v1/alerts/external-links/failures${suffix}`,
    undefined,
    signal,
  );
  if (!isAlertExternalLinkFailureResponse(result)) {
    throw new Error("alerts.external-links.failures 返回结构不合法");
  }
  return result;
}

export async function fetchIntegrationDlqMessages(
  input: {
    eventType?: string;
    channel?: string;
    callbackId?: string;
    alertId?: string;
    limit?: number;
  } = {},
  signal?: AbortSignal,
): Promise<IntegrationDlqMessageListResponse> {
  const query = new URLSearchParams();
  if (input.eventType?.trim()) {
    query.set("eventType", input.eventType.trim());
  }
  if (input.channel?.trim()) {
    query.set("channel", input.channel.trim());
  }
  if (input.callbackId?.trim()) {
    query.set("callbackId", input.callbackId.trim());
  }
  if (input.alertId?.trim()) {
    query.set("alertId", input.alertId.trim());
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    query.set("limit", String(input.limit));
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v1/integrations/dlq/messages${suffix}`,
    undefined,
    signal,
  );
  if (!isIntegrationDlqMessageListResponse(result)) {
    throw new Error("integrations.dlq.messages 返回结构不合法");
  }
  return result;
}

export async function replayIntegrationDlqMessages(
  messageIds: string[],
  signal?: AbortSignal,
): Promise<IntegrationDlqReplayResponse> {
  const normalizedMessageIds = messageIds
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  if (normalizedMessageIds.length === 0) {
    throw new Error("messageIds 至少需要 1 条。");
  }
  const result = await requestJson<unknown>(
    "/api/v1/integrations/dlq/messages/replay",
    {
      method: "POST",
      body: JSON.stringify({
        messageIds: normalizedMessageIds,
      }),
    },
    signal,
  );
  if (!isIntegrationDlqReplayResponse(result)) {
    throw new Error("integrations.dlq.messages.replay 返回结构不合法");
  }
  return result;
}

export async function createIntegrationDlqRecoveryJob(
  input: {
    messageIds?: string[];
    filters?: {
      eventType?: string;
      channel?: string;
      callbackId?: string;
      alertId?: string;
      limit?: number;
    };
  },
  signal?: AbortSignal,
): Promise<IntegrationDlqRecoveryJob> {
  const result = await requestJson<unknown>(
    "/api/v1/integrations/dlq/recovery-jobs",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal,
  );
  if (!isIntegrationDlqRecoveryJob(result)) {
    throw new Error("integrations.dlq.recovery-jobs.create 返回结构不合法");
  }
  return result;
}

export async function fetchIntegrationDlqRecoveryJobs(
  input: {
    status?: "queued" | "running" | "completed" | "failed";
    limit?: number;
  } = {},
  signal?: AbortSignal,
): Promise<IntegrationDlqRecoveryJobListResponse> {
  const query = new URLSearchParams();
  if (input.status) {
    query.set("status", input.status);
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    query.set("limit", String(input.limit));
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v1/integrations/dlq/recovery-jobs${suffix}`,
    undefined,
    signal,
  );
  if (!isIntegrationDlqRecoveryJobListResponse(result)) {
    throw new Error("integrations.dlq.recovery-jobs 返回结构不合法");
  }
  return result;
}

export async function fetchIntegrationDlqRecoveryJobDetail(
  jobId: string,
  signal?: AbortSignal,
): Promise<IntegrationDlqRecoveryJob> {
  const normalizedJobId = jobId.trim();
  if (!normalizedJobId) {
    throw new Error("jobId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/integrations/dlq/recovery-jobs/${encodeURIComponent(normalizedJobId)}`,
    undefined,
    signal,
  );
  if (!isIntegrationDlqRecoveryJob(result)) {
    throw new Error("integrations.dlq.recovery-jobs.detail 返回结构不合法");
  }
  return result;
}

export async function fetchIntegrationAlertFailureReport(
  input: {
    from?: string;
    to?: string;
    externalSystem?: string;
    stage?: string;
    actionType?:
      | "retry_requested"
      | "retry_completed"
      | "retry_failed"
      | "dlq_queried"
      | "dlq_replayed"
      | "recovery_job_created"
      | "recovery_job_completed"
      | "recovery_job_failed";
    limit?: number;
  } = {},
  signal?: AbortSignal,
): Promise<IntegrationAlertFailureReportResponse> {
  const query = new URLSearchParams();
  if (input.from?.trim()) {
    query.set("from", input.from.trim());
  }
  if (input.to?.trim()) {
    query.set("to", input.to.trim());
  }
  if (input.externalSystem?.trim()) {
    query.set("externalSystem", input.externalSystem.trim());
  }
  if (input.stage?.trim()) {
    query.set("stage", input.stage.trim());
  }
  if (input.actionType) {
    query.set("actionType", input.actionType);
  }
  if (typeof input.limit === "number" && Number.isInteger(input.limit) && input.limit > 0) {
    query.set("limit", String(input.limit));
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v1/integrations/failure-reports/alerts${suffix}`,
    undefined,
    signal,
  );
  if (!isIntegrationAlertFailureReportResponse(result)) {
    throw new Error("integrations.failure-reports.alerts 返回结构不合法");
  }
  return result;
}

export async function fetchIntegrationAlertFailureTrends(
  input: {
    from?: string;
    to?: string;
    externalSystem?: string;
    stage?: string;
    actionType?:
      | "retry_requested"
      | "retry_completed"
      | "retry_failed"
      | "dlq_queried"
      | "dlq_replayed"
      | "recovery_job_created"
      | "recovery_job_completed"
      | "recovery_job_failed";
    top?: number;
  } = {},
  signal?: AbortSignal,
): Promise<IntegrationAlertFailureTrendResponse> {
  const query = new URLSearchParams();
  if (input.from?.trim()) {
    query.set("from", input.from.trim());
  }
  if (input.to?.trim()) {
    query.set("to", input.to.trim());
  }
  if (input.externalSystem?.trim()) {
    query.set("externalSystem", input.externalSystem.trim());
  }
  if (input.stage?.trim()) {
    query.set("stage", input.stage.trim());
  }
  if (input.actionType) {
    query.set("actionType", input.actionType);
  }
  if (typeof input.top === "number" && Number.isInteger(input.top) && input.top > 0) {
    query.set("top", String(input.top));
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v1/integrations/failure-reports/alerts/trends${suffix}`,
    undefined,
    signal,
  );
  if (!isIntegrationAlertFailureTrendResponse(result)) {
    throw new Error("integrations.failure-reports.alerts.trends 返回结构不合法");
  }
  return result;
}

export async function retryAlertExternalLinkSyncBatch(
  alertId: string,
  input: {
    externalType?: "ticket" | "case" | "incident";
  } = {},
  signal?: AbortSignal,
): Promise<AlertExternalLinkBatchRetryResponse> {
  const normalizedAlertId = alertId.trim();
  if (!normalizedAlertId) {
    throw new Error("alertId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/alerts/${encodeURIComponent(normalizedAlertId)}/external-links/retry-sync-batch`,
    {
      method: "POST",
      body: JSON.stringify(
        input.externalType ? { externalType: input.externalType } : {},
      ),
    },
    signal,
  );
  if (!isAlertExternalLinkBatchRetryResponse(result)) {
    throw new Error("alerts.external-links.retry-sync-batch 返回结构不合法");
  }
  return result;
}

export async function fetchAlertOrchestrationRules(
  input?: AlertOrchestrationRuleListInput,
  signal?: AbortSignal
): Promise<AlertOrchestrationRuleListResponse> {
  const result = await requestJson<unknown>(
    `/api/v1/alerts/orchestration/rules${buildAlertOrchestrationRuleListQuery(input)}`,
    undefined,
    signal
  );
  if (!isAlertOrchestrationRuleListResponse(result)) {
    throw new Error("alerts.orchestration.rules 返回结构不合法");
  }
  return result;
}

export async function upsertAlertOrchestrationRule(
  ruleId: string,
  input: AlertOrchestrationRuleUpsertInput,
  signal?: AbortSignal
): Promise<AlertOrchestrationRule> {
  const normalizedRuleId = ruleId.trim();
  if (!normalizedRuleId) {
    throw new Error("ruleId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/alerts/orchestration/rules/${encodeURIComponent(normalizedRuleId)}`,
    {
      method: "PUT",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isAlertOrchestrationRule(result)) {
    throw new Error("alerts.orchestration.rules.upsert 返回结构不合法");
  }
  return result;
}

export async function simulateAlertOrchestration(
  input: AlertOrchestrationSimulateInput,
  signal?: AbortSignal
): Promise<AlertOrchestrationSimulationResponse> {
  const result = await requestJson<unknown>(
    "/api/v1/alerts/orchestration/simulate",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isAlertOrchestrationSimulationResponse(result)) {
    throw new Error("alerts.orchestration.simulate 返回结构不合法");
  }
  return result;
}

export async function fetchAlertOrchestrationExecutions(
  input?: AlertOrchestrationExecutionListInput,
  signal?: AbortSignal
): Promise<AlertOrchestrationExecutionListResponse> {
  const result = await requestJson<unknown>(
    `/api/v1/alerts/orchestration/executions${buildAlertOrchestrationExecutionListQuery(input)}`,
    undefined,
    signal
  );
  if (!isAlertOrchestrationExecutionListResponse(result)) {
    throw new Error("alerts.orchestration.executions 返回结构不合法");
  }
  return result;
}

export async function exportSessions(
  format: ExportFormat,
  input?: SessionSearchInput,
  signal?: AbortSignal
): Promise<DownloadFile> {
  if (!isExportFormat(format)) {
    throw new Error("format 必须是 json 或 csv。");
  }

  const defaultFilename = `sessions-export.${format}`;
  return requestBlob(
    `/api/v1/exports/sessions${buildSessionExportQuery(format, input)}`,
    defaultFilename,
    undefined,
    signal
  );
}

export async function exportUsage(
  format: ExportFormat,
  input: UsageExportQueryInput,
  signal?: AbortSignal
): Promise<DownloadFile> {
  if (!isExportFormat(format)) {
    throw new Error("format 必须是 json 或 csv。");
  }
  if (!isUsageExportDimension(input.dimension)) {
    throw new Error("dimension 必须是 daily/weekly/monthly/models/sessions/heatmap。");
  }

  const defaultFilename = `usage-${input.dimension}-export.${format}`;
  return requestBlob(
    `/api/v1/exports/usage${buildUsageExportQuery(format, input)}`,
    defaultFilename,
    undefined,
    signal
  );
}

export type DlpDownloadFile = DownloadFile & {
  dlpMode?: AuditDlpMode;
  dlpMatched?: boolean;
};

function parseAuditDlpMode(value: string | null): AuditDlpMode | undefined {
  const normalized = (value || "").trim().toLowerCase();
  if (normalized === "off" || normalized === "redact" || normalized === "block") {
    return normalized as AuditDlpMode;
  }
  return undefined;
}

function parseBooleanHeader(value: string | null): boolean | undefined {
  const normalized = (value || "").trim().toLowerCase();
  if (!normalized) return undefined;
  if (normalized === "true" || normalized === "1" || normalized === "yes" || normalized === "on") {
    return true;
  }
  if (normalized === "false" || normalized === "0" || normalized === "no" || normalized === "off") {
    return false;
  }
  return undefined;
}

function readAuditDlpHeaders(response: Response): Pick<DlpDownloadFile, "dlpMode" | "dlpMatched"> {
  return {
    dlpMode: parseAuditDlpMode(response.headers.get("x-agentledger-dlp-mode")),
    dlpMatched: parseBooleanHeader(response.headers.get("x-agentledger-dlp-matched")),
  };
}

function appendOptionalQueryParam(params: URLSearchParams, key: string, value: unknown) {
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed) {
      params.set(key, trimmed);
    }
    return;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    params.set(key, String(Math.trunc(value)));
  }
}

export async function exportAudits(
  format: ExportFormat,
  input?: {
    dlpMode?: AuditDlpMode;
    level?: string;
    from?: string;
    to?: string;
    limit?: number;
    cursor?: string;
    eventId?: string;
    action?: string;
    keyword?: string;
    resourceId?: string;
  },
  signal?: AbortSignal
): Promise<DlpDownloadFile> {
  if (!isExportFormat(format)) {
    throw new Error("format 必须是 json 或 csv。");
  }

  const params = new URLSearchParams();
  params.set("format", format);
  appendOptionalQueryParam(params, "dlpMode", input?.dlpMode);
  appendOptionalQueryParam(params, "level", input?.level);
  appendOptionalQueryParam(params, "from", input?.from);
  appendOptionalQueryParam(params, "to", input?.to);
  appendOptionalQueryParam(params, "limit", input?.limit);
  appendOptionalQueryParam(params, "cursor", input?.cursor);
  appendOptionalQueryParam(params, "eventId", input?.eventId);
  appendOptionalQueryParam(params, "action", input?.action);
  appendOptionalQueryParam(params, "keyword", input?.keyword);
  appendOptionalQueryParam(params, "resourceId", input?.resourceId);

  const response = await requestResponseInternal(
    `/api/v1/audits/export${params.toString() ? `?${params.toString()}` : ""}`,
    undefined,
    signal,
    undefined,
    false
  );
  const contentType = response.headers.get("content-type") ?? "application/octet-stream";
  const filename =
    parseContentDispositionFilename(response.headers.get("content-disposition")) ??
    `audits-export.${format}`;
  const blob = await response.blob();
  return {
    blob,
    filename,
    contentType,
    ...readAuditDlpHeaders(response),
  };
}

export async function exportAuditEvidenceBundle(
  input?: {
    dlpMode?: AuditDlpMode;
    level?: string;
    from?: string;
    to?: string;
    limit?: number;
    cursor?: string;
    eventId?: string;
    action?: string;
    keyword?: string;
    resourceId?: string;
  },
  signal?: AbortSignal
): Promise<DlpDownloadFile> {
  const params = new URLSearchParams();
  appendOptionalQueryParam(params, "dlpMode", input?.dlpMode);
  appendOptionalQueryParam(params, "level", input?.level);
  appendOptionalQueryParam(params, "from", input?.from);
  appendOptionalQueryParam(params, "to", input?.to);
  appendOptionalQueryParam(params, "limit", input?.limit);
  appendOptionalQueryParam(params, "cursor", input?.cursor);
  appendOptionalQueryParam(params, "eventId", input?.eventId);
  appendOptionalQueryParam(params, "action", input?.action);
  appendOptionalQueryParam(params, "keyword", input?.keyword);
  appendOptionalQueryParam(params, "resourceId", input?.resourceId);

  const response = await requestResponseInternal(
    `/api/v1/audits/evidence-bundle${params.toString() ? `?${params.toString()}` : ""}`,
    undefined,
    signal,
    undefined,
    false
  );
  const contentType = response.headers.get("content-type") ?? "application/octet-stream";
  const filename =
    parseContentDispositionFilename(response.headers.get("content-disposition")) ??
    "audit-evidence-bundle.json";
  const blob = await response.blob();
  return {
    blob,
    filename,
    contentType,
    ...readAuditDlpHeaders(response),
  };
}

export async function searchSessions(
  input: SessionSearchInput,
  signal?: AbortSignal
): Promise<SessionSearchResponse> {
  try {
    const result = await requestJson<unknown>(
      "/api/v1/sessions/search",
      {
        method: "POST",
        body: JSON.stringify(input),
      },
      signal
    );

    if (!isSessionSearchResponse(result)) {
      throw new Error("session 返回结构不合法");
    }

    return result;
  } catch (error) {
    if (isUnauthorizedError(error) || !shouldUseMockFallback()) {
      throw error;
    }
    return buildMockSessions(input);
  }
}

export async function fetchUsageMonthly(
  filters?: UsageAggregateFilters,
  signal?: AbortSignal
): Promise<UsageAggregateResponse<UsageMonthlyItem>> {
  const result = await requestJson<unknown>(
    `/api/v1/usage/monthly${buildUsageAggregateQuery(filters)}`,
    undefined,
    signal
  );
  if (!isUsageAggregateResponse<UsageMonthlyItem>(result)) {
    throw new Error("usage.monthly 返回结构不合法");
  }
  return result;
}

export async function fetchUsageDaily(
  filters?: UsageAggregateFilters,
  signal?: AbortSignal
): Promise<UsageAggregateResponse<UsageDailyItem>> {
  const result = await requestJson<unknown>(
    `/api/v1/usage/daily${buildUsageAggregateQuery(filters)}`,
    undefined,
    signal
  );
  if (!isUsageAggregateResponse<UsageDailyItem>(result)) {
    throw new Error("usage.daily 返回结构不合法");
  }
  return result;
}

export async function fetchUsageModels(
  filters?: UsageAggregateFilters,
  signal?: AbortSignal
): Promise<UsageAggregateResponse<UsageModelItem>> {
  const result = await requestJson<unknown>(
    `/api/v1/usage/models${buildUsageAggregateQuery(filters)}`,
    undefined,
    signal
  );
  if (!isUsageAggregateResponse<UsageModelItem>(result)) {
    throw new Error("usage.models 返回结构不合法");
  }
  return result;
}

export async function fetchUsageSessions(
  filters?: UsageAggregateFilters,
  signal?: AbortSignal
): Promise<UsageAggregateResponse<UsageSessionBreakdownItem>> {
  const result = await requestJson<unknown>(
    `/api/v1/usage/sessions${buildUsageAggregateQuery(filters)}`,
    undefined,
    signal
  );
  if (!isUsageAggregateResponse<UsageSessionBreakdownItem>(result)) {
    throw new Error("usage.sessions 返回结构不合法");
  }
  return result;
}

export async function fetchUsageWeeklySummary(
  input?: UsageWeeklySummaryQueryInput,
  signal?: AbortSignal
): Promise<UsageWeeklySummaryResponse> {
  const result = await requestJson<unknown>(
    `/api/v1/usage/weekly-summary${buildUsageWeeklySummaryQuery(input)}`,
    undefined,
    signal
  );
  if (!isUsageWeeklySummaryResponse(result)) {
    throw new Error("usage.weekly-summary 返回结构不合法");
  }
  return result;
}

export async function fetchSessionEvents(
  sessionId: string,
  limit = 20,
  cursor?: string,
  signal?: AbortSignal
): Promise<SessionEventListResponse> {
  const normalizedSessionId = sessionId.trim();
  if (!normalizedSessionId) {
    throw new Error("sessionId 不能为空。");
  }

  const normalizedLimit =
    Number.isInteger(limit) && limit > 0 ? String(limit) : "20";
  const params = new URLSearchParams({
    limit: normalizedLimit,
  });
  if (typeof cursor === "string" && cursor.trim().length > 0) {
    params.set("cursor", cursor.trim());
  }
  const result = await requestJson<unknown>(
    `/api/v1/sessions/${encodeURIComponent(normalizedSessionId)}/events?${params.toString()}`,
    undefined,
    signal
  );
  if (!isSessionEventListResponse(result)) {
    throw new Error("sessions.events 返回结构不合法");
  }
  return result;
}

export async function fetchSessionDetail(
  sessionId: string,
  signal?: AbortSignal
): Promise<SessionDetailResponse> {
  const normalizedSessionId = sessionId.trim();
  if (!normalizedSessionId) {
    throw new Error("sessionId 不能为空。");
  }

  const result = await requestJson<unknown>(
    `/api/v1/sessions/${encodeURIComponent(normalizedSessionId)}`,
    undefined,
    signal
  );
  if (!isSessionDetailResponse(result)) {
    throw new Error("sessions.detail 返回结构不合法");
  }
  return result;
}

export async function fetchPricingCatalog(signal?: AbortSignal): Promise<PricingCatalog> {
  const result = await requestJson<unknown>("/api/v1/pricing/catalog", undefined, signal);
  if (!isPricingCatalog(result)) {
    throw new Error("pricing.catalog 返回结构不合法");
  }
  return result;
}

export async function upsertPricingCatalog(
  input: PricingCatalogUpsertInput,
  signal?: AbortSignal
): Promise<PricingCatalog> {
  const result = await requestJson<unknown>(
    "/api/v1/pricing/catalog",
    {
      method: "PUT",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isPricingCatalog(result)) {
    throw new Error("pricing.catalog.upsert 返回结构不合法");
  }
  return result;
}

export async function testSourceConnection(
  sourceId: string,
  signal?: AbortSignal
): Promise<SourceConnectionTestResponse> {
  const normalizedSourceId = sourceId.trim();
  if (!normalizedSourceId) {
    throw new Error("sourceId 不能为空。");
  }

  const result = await requestJson<unknown>(
    "/api/v1/sources/test-connection",
    {
      method: "POST",
      body: JSON.stringify({ sourceId: normalizedSourceId }),
    },
    signal
  );
  if (!isSourceConnectionTestResponse(result)) {
    throw new Error("sources.test-connection 返回结构不合法");
  }
  return result;
}

export async function fetchResidencyRegions(
  signal?: AbortSignal
): Promise<ResidencyRegionListResponse> {
  const result = await requestJson<unknown>(
    "/api/v2/residency/region-mappings",
    undefined,
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("residency.region-mappings 返回结构不合法");
  }
  const items: RegionDescriptor[] = result.items
    .map((item) => {
      if (!isRecord(item)) {
        return null;
      }
      const metadata = isRecord(item.metadata) ? item.metadata : undefined;
      const id = asString(item.id) ?? asString(item.regionId);
      const name = asString(item.name) ?? asString(item.regionName) ?? id;
      if (!id || !name) {
        return null;
      }
      const active = typeof item.active === "boolean" ? item.active : true;
      const description =
        asString(item.description) ?? (metadata ? asString(metadata.description) : undefined);
      return {
        id,
        name,
        active,
        ...(description ? { description } : {}),
      } satisfies RegionDescriptor;
    })
    .filter((item): item is RegionDescriptor => Boolean(item));
  const payload: ResidencyRegionListResponse = {
    items,
    total:
      typeof result.total === "number" && Number.isInteger(result.total)
        ? result.total
        : items.length,
  };
  if (!isResidencyRegionListResponse(payload)) {
    throw new Error("residency.region-mappings 解析后结构不合法");
  }
  return payload;
}

export async function fetchResidencyPolicy(
  signal?: AbortSignal
): Promise<TenantResidencyPolicy | null> {
  try {
    const result = await requestJson<unknown>(
      "/api/v2/residency/policies/current",
      undefined,
      signal
    );
    if (!isTenantResidencyPolicy(result)) {
      throw new Error("residency.policy 返回结构不合法");
    }
    return result;
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) {
      return null;
    }
    throw error;
  }
}

export async function upsertResidencyPolicy(
  input: Omit<TenantResidencyPolicy, "tenantId" | "updatedAt"> & {
    updatedAt?: string;
  },
  signal?: AbortSignal
): Promise<TenantResidencyPolicy> {
  const result = await requestJson<unknown>(
    "/api/v2/residency/policies/current",
    {
      method: "PUT",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isTenantResidencyPolicy(result)) {
    throw new Error("residency.policy.upsert 返回结构不合法");
  }
  return result;
}

export async function fetchResidencyKmsKeyMappings(
  signal?: AbortSignal
): Promise<ResidencyKmsKeyMappingListResponse> {
  const result = await requestJson<unknown>(
    "/api/v2/residency/kms-key-mappings",
    undefined,
    signal
  );
  if (!isResidencyKmsKeyMappingListResponse(result)) {
    throw new Error("residency.kms-key-mappings 返回结构不合法");
  }
  return result;
}

export async function upsertResidencyKmsKeyMappings(
  input: ResidencyKmsKeyMappingUpsertInput,
  signal?: AbortSignal
): Promise<ResidencyKmsKeyMappingListResponse> {
  const result = await requestJson<unknown>(
    "/api/v2/residency/kms-key-mappings",
    {
      method: "PUT",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isResidencyKmsKeyMappingListResponse(result)) {
    throw new Error("residency.kms-key-mappings.upsert 返回结构不合法");
  }
  return result;
}

export async function fetchResidencyArchiveRegionPolicies(
  signal?: AbortSignal
): Promise<ResidencyArchiveRegionPolicyListResponse> {
  const result = await requestJson<unknown>(
    "/api/v2/residency/archive-region-policies",
    undefined,
    signal
  );
  if (!isResidencyArchiveRegionPolicyListResponse(result)) {
    throw new Error("residency.archive-region-policies 返回结构不合法");
  }
  return result;
}

export async function upsertResidencyArchiveRegionPolicies(
  input: ResidencyArchiveRegionPolicyUpsertInput,
  signal?: AbortSignal
): Promise<ResidencyArchiveRegionPolicyListResponse> {
  const result = await requestJson<unknown>(
    "/api/v2/residency/archive-region-policies",
    {
      method: "PUT",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isResidencyArchiveRegionPolicyListResponse(result)) {
    throw new Error("residency.archive-region-policies.upsert 返回结构不合法");
  }
  return result;
}

export async function fetchReplicationJobs(
  input?: ReplicationJobListInput,
  signal?: AbortSignal
): Promise<ReplicationJobListResponse> {
  const result = await requestJson<unknown>(
    `/api/v2/residency/replications${buildReplicationJobListQuery(input)}`,
    undefined,
    signal
  );
  if (!isReplicationJobListResponse(result)) {
    throw new Error("residency.replications 返回结构不合法");
  }
  return result;
}

export async function createReplicationJob(
  input: ReplicationJobCreateInput,
  signal?: AbortSignal
): Promise<ReplicationJob> {
  const result = await requestJson<unknown>(
    "/api/v2/residency/replications",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isReplicationJob(result)) {
    throw new Error("residency.replications.create 返回结构不合法");
  }
  return result;
}

export async function cancelReplicationJob(
  jobId: string,
  input?: ReplicationJobCancelInput,
  signal?: AbortSignal
): Promise<ReplicationJob> {
  const normalizedJobId = jobId.trim();
  if (!normalizedJobId) {
    throw new Error("jobId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/residency/replications/${encodeURIComponent(normalizedJobId)}/cancel`,
    {
      method: "POST",
      body: JSON.stringify(input ?? {}),
    },
    signal
  );
  if (!isReplicationJob(result)) {
    throw new Error("residency.replications.cancel 返回结构不合法");
  }
  return result;
}

export async function approveReplicationJob(
  jobId: string,
  input?: ReplicationJobApproveInput,
  signal?: AbortSignal
): Promise<ReplicationJob> {
  const normalizedJobId = jobId.trim();
  if (!normalizedJobId) {
    throw new Error("jobId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/residency/replications/${encodeURIComponent(normalizedJobId)}/approvals`,
    {
      method: "POST",
      body: JSON.stringify(input ?? {}),
    },
    signal
  );
  if (!isReplicationJob(result)) {
    throw new Error("residency.replications.approve 返回结构不合法");
  }
  return result;
}

export async function fetchSystemConfigPackages(
  limit = 50,
  signal?: AbortSignal,
): Promise<SystemConfigPackageListResponse> {
  const normalizedLimit = Number.isInteger(limit) && limit > 0 ? limit : 50;
  const result = await requestJson<unknown>(
    `/api/v1/system/config/packages?limit=${normalizedLimit}`,
    undefined,
    signal,
  );
  if (!isSystemConfigPackageListResponse(result)) {
    throw new Error("system.config.packages 返回结构不合法");
  }
  return result;
}

export async function createSystemConfigPackage(
  input: SystemConfigPackageCreateInput,
  signal?: AbortSignal,
): Promise<SystemConfigPackage> {
  if (!isSystemConfigPackageCreateInput(input)) {
    throw new Error("system.config.packages.create 输入不合法");
  }
  const result = await requestJson<unknown>(
    "/api/v1/system/config/packages",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal,
  );
  if (!isSystemConfigPackage(result)) {
    throw new Error("system.config.packages.create 返回结构不合法");
  }
  return result;
}

export async function fetchSystemConfigPackageApprovals(
  packageId: string,
  signal?: AbortSignal,
): Promise<SystemConfigPackageApprovalListResponse> {
  const normalizedPackageId = packageId.trim();
  if (!normalizedPackageId) {
    throw new Error("packageId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/system/config/packages/${encodeURIComponent(normalizedPackageId)}/approvals`,
    undefined,
    signal,
  );
  if (!isSystemConfigPackageApprovalListResponse(result)) {
    throw new Error("system.config.packages.approvals 返回结构不合法");
  }
  return result;
}

export async function createSystemConfigPackageApproval(
  packageId: string,
  input: SystemConfigPackageApprovalCreateInput,
  signal?: AbortSignal,
): Promise<SystemConfigPackageApproval> {
  const normalizedPackageId = packageId.trim();
  if (!normalizedPackageId) {
    throw new Error("packageId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/system/config/packages/${encodeURIComponent(normalizedPackageId)}/approvals`,
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal,
  );
  if (!isSystemConfigPackageApproval(result)) {
    throw new Error("system.config.packages.approvals.create 返回结构不合法");
  }
  return result;
}

export async function publishSystemConfigPackage(
  packageId: string,
  signal?: AbortSignal,
): Promise<SystemConfigPackage> {
  const normalizedPackageId = packageId.trim();
  if (!normalizedPackageId) {
    throw new Error("packageId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/system/config/packages/${encodeURIComponent(normalizedPackageId)}/publish`,
    {
      method: "POST",
    },
    signal,
  );
  if (!isSystemConfigPackage(result)) {
    throw new Error("system.config.packages.publish 返回结构不合法");
  }
  return result;
}

export async function fetchSystemConfigWatchLatest(
  input: SystemConfigWatchLatestInput,
  signal?: AbortSignal,
): Promise<SystemConfigWatchLatestResponse> {
  const query = new URLSearchParams();
  if (input.agentId?.trim()) {
    query.set("agentId", input.agentId.trim());
  }
  if (input.deviceId?.trim()) {
    query.set("deviceId", input.deviceId.trim());
  }
  if (input.channel?.trim()) {
    query.set("channel", input.channel.trim());
  }
  if (input.hostname?.trim()) {
    query.set("hostname", input.hostname.trim());
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v1/system/config/packages/watch/latest${suffix}`,
    undefined,
    signal,
  );
  if (!isSystemConfigPackage(result)) {
    throw new Error("system.config.packages.watch.latest 返回结构不合法");
  }
  return result;
}

export async function fetchAgentRuntimeViews(
  signal?: AbortSignal,
): Promise<AgentRuntimeViewListResponse> {
  const result = await requestJson<unknown>(
    "/api/v1/system/config/agents/views",
    undefined,
    signal,
  );
  if (!isAgentRuntimeViewListResponse(result)) {
    throw new Error("system.config.agents.views 返回结构不合法");
  }
  return result;
}

export async function fetchAgentRuntimeConfig(
  agentId: string,
  signal?: AbortSignal,
): Promise<AgentRuntimeConfigResponse> {
  const normalizedAgentId = agentId.trim();
  if (!normalizedAgentId) {
    throw new Error("agentId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/system/config/agent-runtime?agentId=${encodeURIComponent(normalizedAgentId)}`,
    undefined,
    signal,
  );
  if (!isAgentRuntimeConfigResponse(result)) {
    throw new Error("system.config.agent-runtime 返回结构不合法");
  }
  return result;
}

export async function fetchAgentReleases(
  input?: {
    limit?: number;
    channel?: AgentReleaseChannel;
    os?: string;
    arch?: string;
  },
  signal?: AbortSignal,
): Promise<AgentReleaseListResponse> {
  const query = new URLSearchParams();
  if (input?.limit && Number.isInteger(input.limit) && input.limit > 0) {
    query.set("limit", String(input.limit));
  }
  if (input?.channel) {
    query.set("channel", input.channel);
  }
  if (input?.os?.trim()) {
    query.set("os", input.os.trim());
  }
  if (input?.arch?.trim()) {
    query.set("arch", input.arch.trim());
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v1/system/agent-releases${suffix}`,
    undefined,
    signal,
  );
  if (!isAgentReleaseListResponse(result)) {
    throw new Error("system.agent-releases 返回结构不合法");
  }
  return result;
}

export async function fetchAgentReleaseCheckPreview(
  input: AgentReleaseCheckPreviewInput,
  signal?: AbortSignal,
): Promise<AgentReleaseCheckPreviewResponse> {
  const query = new URLSearchParams({
    currentVersion: input.currentVersion,
    channel: input.channel,
    os: input.os,
    arch: input.arch,
  });
  if (input.agentId?.trim()) {
    query.set("agentId", input.agentId.trim());
  }
  if (input.deviceId?.trim()) {
    query.set("deviceId", input.deviceId.trim());
  }
  if (input.hostname?.trim()) {
    query.set("hostname", input.hostname.trim());
  }
  if (input.ring?.trim()) {
    query.set("ring", input.ring.trim());
  }
  const result = await requestJson<unknown>(
    `/api/v1/system/agent-releases/check?${query.toString()}`,
    undefined,
    signal,
  );
  if (!isAgentReleaseCheckPreviewResponse(result)) {
    throw new Error("system.agent-releases.check 返回结构不合法");
  }
  return result;
}

export async function fetchAgentReleaseCheckBatchPreview(
  input: AgentReleaseCheckBatchPreviewInput,
  signal?: AbortSignal,
): Promise<AgentReleaseCheckBatchPreviewResponse> {
  const result = await requestJson<unknown>(
    "/api/v1/system/agent-releases/check/batch",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal,
  );
  if (!isAgentReleaseBatchCheckPreviewResponse(result)) {
    throw new Error("system.agent-releases.check.batch 返回结构不合法");
  }
  return result;
}

export async function fetchRuleAssets(
  input?: RuleAssetListInput,
  signal?: AbortSignal
): Promise<RuleAssetListResponse> {
  const result = await requestJson<unknown>(
    `/api/v1/rules/assets${buildRuleAssetListQuery(input)}`,
    undefined,
    signal
  );
  if (!isRuleAssetListResponse(result)) {
    throw new Error("rules.assets 返回结构不合法");
  }
  return result;
}

export async function createRuleAsset(
  input: RuleAssetCreateInput,
  signal?: AbortSignal
): Promise<RuleAsset> {
  if (
    input.requiredApprovals !== undefined &&
    !isRuleRequiredApprovals(input.requiredApprovals)
  ) {
    throw new Error("requiredApprovals 仅支持 1 或 2。");
  }
  const result = await requestJson<unknown>(
    "/api/v1/rules/assets",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isRuleAsset(result)) {
    throw new Error("rules.assets.create 返回结构不合法");
  }
  return result;
}

export async function fetchRuleAssetVersions(
  assetId: string,
  limit?: number,
  signal?: AbortSignal
): Promise<{ items: RuleAssetVersion[]; total: number }> {
  const normalizedAssetId = assetId.trim();
  if (!normalizedAssetId) {
    throw new Error("assetId 不能为空。");
  }
  const query =
    typeof limit === "number" && Number.isInteger(limit) && limit > 0 ? `?limit=${limit}` : "";
  const result = await requestJson<unknown>(
    `/api/v1/rules/assets/${encodeURIComponent(normalizedAssetId)}/versions${query}`,
    undefined,
    signal
  );
  if (!isRuleAssetVersionListResponse(result)) {
    throw new Error("rules.assets.versions 返回结构不合法");
  }
  return result;
}

export async function fetchRuleAssetVersionDiff(
  assetId: string,
  input: {
    fromVersion: number;
    toVersion: number;
  },
  signal?: AbortSignal,
): Promise<RuleAssetVersionDiffResponse> {
  const normalizedAssetId = assetId.trim();
  if (!normalizedAssetId) {
    throw new Error("assetId 不能为空。");
  }
  if (
    !Number.isInteger(input.fromVersion) ||
    input.fromVersion < 1 ||
    !Number.isInteger(input.toVersion) ||
    input.toVersion < 1
  ) {
    throw new Error("fromVersion 和 toVersion 必须是正整数。");
  }
  const query = new URLSearchParams({
    fromVersion: String(input.fromVersion),
    toVersion: String(input.toVersion),
  });
  const result = await requestJson<unknown>(
    `/api/v1/rules/assets/${encodeURIComponent(normalizedAssetId)}/versions/diff?${query.toString()}`,
    undefined,
    signal,
  );
  if (!isRuleAssetVersionDiffResponse(result)) {
    throw new Error("rules.assets.versions.diff 返回结构不合法");
  }
  return result;
}

export async function createRuleAssetVersion(
  assetId: string,
  input: RuleAssetVersionCreateInput,
  signal?: AbortSignal
): Promise<RuleAssetVersion> {
  const normalizedAssetId = assetId.trim();
  if (!normalizedAssetId) {
    throw new Error("assetId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/rules/assets/${encodeURIComponent(normalizedAssetId)}/versions`,
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isRuleAssetVersion(result)) {
    throw new Error("rules.assets.versions.create 返回结构不合法");
  }
  return result;
}

export async function publishRuleAsset(
  assetId: string,
  input: RulePublishInput,
  signal?: AbortSignal
): Promise<RuleAsset> {
  const normalizedAssetId = assetId.trim();
  if (!normalizedAssetId) {
    throw new Error("assetId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/rules/assets/${encodeURIComponent(normalizedAssetId)}/publish`,
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isRuleAsset(result)) {
    throw new Error("rules.assets.publish 返回结构不合法");
  }
  return result;
}

export async function rollbackRuleAsset(
  assetId: string,
  input: RuleRollbackInput,
  signal?: AbortSignal
): Promise<RuleAsset> {
  const normalizedAssetId = assetId.trim();
  if (!normalizedAssetId) {
    throw new Error("assetId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/rules/assets/${encodeURIComponent(normalizedAssetId)}/rollback`,
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isRuleAsset(result)) {
    throw new Error("rules.assets.rollback 返回结构不合法");
  }
  return result;
}

export async function fetchRuleApprovals(
  assetId: string,
  input?: RuleApprovalListInput,
  signal?: AbortSignal
): Promise<RuleApprovalListResponse> {
  const normalizedAssetId = assetId.trim();
  if (!normalizedAssetId) {
    throw new Error("assetId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/rules/assets/${encodeURIComponent(normalizedAssetId)}/approvals${buildRuleApprovalListQuery(input)}`,
    undefined,
    signal
  );
  if (!isRuleApprovalListResponse(result)) {
    throw new Error("rules.assets.approvals 返回结构不合法");
  }
  return result;
}

export async function createRuleApproval(
  assetId: string,
  input: RuleApprovalCreateInput,
  signal?: AbortSignal
): Promise<RuleApproval> {
  const normalizedAssetId = assetId.trim();
  if (!normalizedAssetId) {
    throw new Error("assetId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/rules/assets/${encodeURIComponent(normalizedAssetId)}/approvals`,
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isRuleApproval(result)) {
    throw new Error("rules.assets.approvals.create 返回结构不合法");
  }
  return result;
}

export async function fetchMcpPolicies(
  input?: McpToolPolicyListInput,
  signal?: AbortSignal
): Promise<McpToolPolicyListResponse> {
  const result = await requestJson<unknown>(
    `/api/v1/mcp/policies${buildMcpToolPolicyListQuery(input)}`,
    undefined,
    signal
  );
  if (!isMcpToolPolicyListResponse(result)) {
    throw new Error("mcp.policies 返回结构不合法");
  }
  return result;
}

export async function upsertMcpPolicy(
  toolId: string,
  input: McpToolPolicyUpsertInput,
  signal?: AbortSignal
): Promise<McpToolPolicy> {
  const normalizedToolId = toolId.trim();
  if (!normalizedToolId) {
    throw new Error("toolId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/mcp/policies/${encodeURIComponent(normalizedToolId)}`,
    {
      method: "PUT",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isMcpToolPolicy(result)) {
    throw new Error("mcp.policies.upsert 返回结构不合法");
  }
  return result;
}

export async function fetchMcpApprovals(
  input?: McpApprovalListInput,
  signal?: AbortSignal
): Promise<McpApprovalListResponse> {
  const result = await requestJson<unknown>(
    `/api/v1/mcp/approvals${buildMcpApprovalListQuery(input)}`,
    undefined,
    signal
  );
  if (!isMcpApprovalListResponse(result)) {
    throw new Error("mcp.approvals 返回结构不合法");
  }
  return result;
}

export async function createMcpApproval(
  input: McpApprovalCreateInput,
  signal?: AbortSignal
): Promise<McpApprovalRequest> {
  const result = await requestJson<unknown>(
    "/api/v1/mcp/approvals",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isMcpApprovalRequest(result)) {
    throw new Error("mcp.approvals.create 返回结构不合法");
  }
  return result;
}

async function reviewMcpApproval(
  approvalId: string,
  action: "approve" | "reject",
  input?: McpApprovalReviewInput,
  signal?: AbortSignal
): Promise<McpApprovalRequest> {
  const normalizedApprovalId = approvalId.trim();
  if (!normalizedApprovalId) {
    throw new Error("approvalId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/mcp/approvals/${encodeURIComponent(normalizedApprovalId)}/${action}`,
    {
      method: "POST",
      body: JSON.stringify(input ?? {}),
    },
    signal
  );
  if (!isMcpApprovalRequest(result)) {
    throw new Error(`mcp.approvals.${action} 返回结构不合法`);
  }
  return result;
}

export async function approveMcpApproval(
  approvalId: string,
  input?: McpApprovalReviewInput,
  signal?: AbortSignal
): Promise<McpApprovalRequest> {
  return reviewMcpApproval(approvalId, "approve", input, signal);
}

export async function rejectMcpApproval(
  approvalId: string,
  input?: McpApprovalReviewInput,
  signal?: AbortSignal
): Promise<McpApprovalRequest> {
  return reviewMcpApproval(approvalId, "reject", input, signal);
}

export async function evaluateMcpTool(
  input: McpEvaluateInput,
  signal?: AbortSignal
): Promise<McpEvaluateResult> {
  const result = await requestJson<unknown>(
    "/api/v1/mcp/evaluate",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isMcpEvaluateResult(result)) {
    throw new Error("mcp.evaluate 返回结构不合法");
  }
  return result;
}

export async function fetchMcpInvocations(
  input?: McpInvocationListInput,
  signal?: AbortSignal
): Promise<McpInvocationListResponse> {
  const result = await requestJson<unknown>(
    `/api/v1/mcp/invocations${buildMcpInvocationListQuery(input)}`,
    undefined,
    signal
  );
  if (!isMcpInvocationListResponse(result)) {
    throw new Error("mcp.invocations 返回结构不合法");
  }
  return result;
}

export async function createMcpInvocation(
  input: McpInvocationCreateInput,
  signal?: AbortSignal
): Promise<McpInvocationAudit> {
  const result = await requestJson<unknown>(
    "/api/v1/mcp/invocations",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isMcpInvocationAudit(result)) {
    throw new Error("mcp.invocations.create 返回结构不合法");
  }
  return result;
}

function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function asIsoDateString(value: unknown): string | null {
  return typeof value === "string" && isISODateString(value) ? value : null;
}

function asFiniteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function mapOpenPlatformApiKey(value: unknown): OpenPlatformApiKey | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asString(value.id);
  const tenantId = asString(value.tenantId);
  const name = asString(value.name);
  const scope = asString(value.scope);
  const keyPrefix = asString(value.keyPrefix);
  const createdAt = asIsoDateString(value.createdAt);
  const updatedAt = asIsoDateString(value.updatedAt);
  const status = value.status === "active" ? "active" : value.status === "revoked" ? "disabled" : null;
  if (!id || !tenantId || !name || !scope || !keyPrefix || !createdAt || !updatedAt || !status) {
    return null;
  }
  const lastUsedAt = asIsoDateString(value.lastUsedAt) ?? undefined;
  const expiresAt = asIsoDateString(value.expiresAt) ?? undefined;
  return {
    id,
    tenantId,
    name,
    maskedKey: `${keyPrefix}***`,
    status,
    scopes: [scope],
    expiresAt,
    lastUsedAt,
    createdAt,
    updatedAt,
  };
}

function mapOpenPlatformWebhook(value: unknown): OpenPlatformWebhook | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asString(value.id);
  const tenantId = asString(value.tenantId);
  const name = asString(value.name);
  const url = asString(value.url);
  const createdAt = asIsoDateString(value.createdAt);
  const updatedAt = asIsoDateString(value.updatedAt);
  const events = Array.isArray(value.events)
    ? value.events.map((item) => asString(item)).filter((item): item is string => Boolean(item))
    : [];
  const status = asString(value.status);
  if (!id || !tenantId || !name || !url || !createdAt || !updatedAt || !status || events.length === 0) {
    return null;
  }
  return {
    id,
    tenantId,
    name,
    url,
    events,
    enabled: status === "active",
    lastDeliveryAt: asIsoDateString(value.lastSuccessAt) ?? undefined,
    createdAt,
    updatedAt,
  };
}

function mapReplayStatus(value: unknown): OpenPlatformReplayRunStatus | null {
  if (
    value === "pending" ||
    value === "running" ||
    value === "completed" ||
    value === "failed" ||
    value === "cancelled"
  ) {
    return value;
  }
  return null;
}

function mapOpenPlatformReplayJob(value: unknown): OpenPlatformReplayJob | null {
  if (!isRecord(value)) {
    return null;
  }
  const runId = asString(value.runId) ?? asString(value.id) ?? asString(value.jobId);
  const jobId = asString(value.jobId) ?? runId;
  const datasetId = asString(value.datasetId) ?? asString(value.baselineId);
  const baselineId = asString(value.baselineId) ?? datasetId;
  const candidateLabel = asString(value.candidateLabel);
  const status = mapReplayStatus(value.status);
  const totalCases = asFiniteNumber(value.totalCases);
  const processedCases = asFiniteNumber(value.processedCases);
  const improvedCases = asFiniteNumber(value.improvedCases);
  const regressedCases = asFiniteNumber(value.regressedCases);
  const unchangedCases = asFiniteNumber(value.unchangedCases);
  const createdAt = asIsoDateString(value.createdAt);
  const updatedAt = asIsoDateString(value.updatedAt) ?? undefined;
  const summary = isRecord(value.summary) ? value.summary : undefined;
  const baselineVersionId =
    asString(value.baselineVersionId) ??
    (summary ? asString(summary.baselineVersionId) : undefined) ??
    (summary && isRecord(summary.metadata)
      ? asString(summary.metadata.baselineVersionId)
      : undefined);
  if (
    !runId ||
    !datasetId ||
    !candidateLabel ||
    !status ||
    totalCases === null ||
    processedCases === null ||
    improvedCases === null ||
    regressedCases === null ||
    unchangedCases === null ||
    !createdAt
  ) {
    return null;
  }
  const passedCases = Math.max(0, Math.round(processedCases - regressedCases));
  const failedCases = Math.max(0, Math.round(regressedCases));
  const finishedAt = asIsoDateString(value.finishedAt) ?? undefined;
  return {
    id: asString(value.id) ?? runId,
    runId,
    ...(jobId ? { jobId } : {}),
    datasetId,
    ...(baselineId ? { baselineId } : {}),
    ...(baselineVersionId ? { baselineVersionId } : {}),
    candidateLabel,
    status,
    totalCases: Math.max(0, Math.round(totalCases)),
    processedCases: Math.max(0, Math.round(processedCases)),
    improvedCases: Math.max(0, Math.round(improvedCases)),
    regressedCases: Math.max(0, Math.round(regressedCases)),
    unchangedCases: Math.max(0, Math.round(unchangedCases)),
    passedCases,
    failedCases,
    ...(summary ? { summary } : {}),
    createdAt,
    ...(updatedAt ? { updatedAt } : {}),
    ...(finishedAt ? { finishedAt } : {}),
  };
}

function mapOpenPlatformReplayBaseline(value: unknown): OpenPlatformReplayBaseline | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asString(value.id);
  const name = asString(value.name);
  const model = asString(value.model);
  const rawDatasetId = asString(value.datasetId);
  const datasetId = id ?? rawDatasetId;
  const datasetRef =
    asString(value.datasetRef) ?? (rawDatasetId && rawDatasetId !== datasetId ? rawDatasetId : undefined);
  const promptVersion = asString(value.promptVersion) ?? undefined;
  const sampleCount = asFiniteNumber(value.sampleCount);
  const caseCount = asFiniteNumber(value.caseCount) ?? sampleCount;
  const currentVersionId = asString(value.currentVersionId) ?? undefined;
  const currentVersionNumber = asFiniteNumber(value.currentVersionNumber);
  const createdAt = asIsoDateString(value.createdAt);
  const updatedAt = asIsoDateString(value.updatedAt);
  if (!datasetId || !name || !model || !createdAt || !updatedAt) {
    return null;
  }
  const description = asString(value.description) ?? undefined;
  return {
    id: datasetId,
    name,
    model,
    datasetId,
    ...(datasetRef ? { datasetRef } : {}),
    ...(promptVersion ? { promptVersion } : {}),
    ...(sampleCount !== null ? { sampleCount: Math.max(0, Math.round(sampleCount)) } : {}),
    ...(caseCount !== null ? { caseCount: Math.max(0, Math.round(caseCount)) } : {}),
    ...(currentVersionId ? { currentVersionId } : {}),
    ...(currentVersionNumber !== null ? { currentVersionNumber: Math.max(1, Math.round(currentVersionNumber)) } : {}),
    ...(description ? { description } : {}),
    ...(isRecord(value.metadata) ? { metadata: value.metadata } : {}),
    createdAt,
    updatedAt,
  };
}

function mapOpenPlatformReplayDatasetVersion(
  value: unknown,
  datasetIdFallback?: string,
): OpenPlatformReplayDatasetVersion | null {
  if (!isRecord(value)) {
    return null;
  }
  const rawDatasetId = asString(value.datasetId) ?? asString(value.baselineId);
  const datasetId = datasetIdFallback ?? rawDatasetId;
  const version = asFiniteNumber(value.version);
  const sampleCount =
    asFiniteNumber(value.sampleCount) ?? asFiniteNumber(value.scenarioCount) ?? 0;
  const createdAt = asIsoDateString(value.createdAt);
  const promotedAt =
    value.promotedAt === null ? null : asIsoDateString(value.promotedAt) ?? undefined;
  if (!datasetId || !asString(value.id) || version === null || !asString(value.model) || !createdAt) {
    return null;
  }
  const datasetRef =
    asString(value.datasetRef) ??
    (rawDatasetId && rawDatasetId !== datasetId ? rawDatasetId : undefined);
  return {
    id: asString(value.id) ?? "",
    ...(asString(value.tenantId) ? { tenantId: asString(value.tenantId) ?? undefined } : {}),
    datasetId,
    version: Math.max(1, Math.round(version)),
    ...(datasetRef ? { datasetRef } : {}),
    model: asString(value.model) ?? "",
    ...(asString(value.promptVersion) ? { promptVersion: asString(value.promptVersion) ?? undefined } : {}),
    sampleCount: Math.max(0, Math.round(sampleCount)),
    ...(isRecord(value.metadata) ? { metadata: value.metadata } : {}),
    ...(asString(value.note) ? { note: asString(value.note) ?? undefined } : {}),
    createdAt,
    ...(promotedAt !== undefined ? { promotedAt } : {}),
  };
}

function mapOpenPlatformReplayDatasetCase(value: unknown): OpenPlatformReplayDatasetCase | null {
  if (!isRecord(value)) {
    return null;
  }
  const datasetId = asString(value.datasetId);
  const caseId = asString(value.caseId);
  const sortOrder = asFiniteNumber(value.sortOrder);
  const input = asString(value.input);
  if (!datasetId || !caseId || sortOrder === null || !input) {
    return null;
  }
  return {
    datasetId,
    caseId,
    sortOrder: Math.max(0, Math.round(sortOrder)),
    input,
    expectedOutput: asString(value.expectedOutput) ?? undefined,
    baselineOutput: asString(value.baselineOutput) ?? undefined,
    candidateInput: asString(value.candidateInput) ?? undefined,
    metadata: isRecord(value.metadata) ? value.metadata : {},
    checksum: asString(value.checksum) ?? undefined,
    createdAt: asIsoDateString(value.createdAt) ?? undefined,
    updatedAt: asIsoDateString(value.updatedAt) ?? undefined,
  };
}

function mapOpenPlatformReplayArtifact(value: unknown): OpenPlatformReplayArtifact | null {
  if (!isRecord(value)) {
    return null;
  }
  const type = asString(value.type);
  const contentType = asString(value.contentType);
  if (!type || !contentType) {
    return null;
  }
  return {
    ...(asString(value.runId) ? { runId: asString(value.runId) ?? undefined } : {}),
    type,
    contentType,
    name: asString(value.name) ?? undefined,
    description: asString(value.description) ?? undefined,
    byteSize:
      typeof value.byteSize === "number" && Number.isInteger(value.byteSize) && value.byteSize >= 0
        ? value.byteSize
        : undefined,
    downloadName: asString(value.downloadName) ?? undefined,
    downloadUrl: asString(value.downloadUrl) ?? undefined,
    checksum: asString(value.checksum) ?? undefined,
    storageBackend:
      value.storageBackend === "local" ||
      value.storageBackend === "object" ||
      value.storageBackend === "hybrid"
        ? value.storageBackend
        : undefined,
    storageKey: asString(value.storageKey) ?? undefined,
    metadata: isRecord(value.metadata) ? value.metadata : undefined,
    createdAt: asIsoDateString(value.createdAt) ?? undefined,
    inline: isRecord(value.inline) ? value.inline : undefined,
  };
}

function normalizeReplaySourceSummary(value: unknown): Record<string, number> | undefined {
  if (!isRecord(value)) {
    return undefined;
  }
  const normalized: Record<string, number> = {};
  for (const [key, rawValue] of Object.entries(value)) {
    if (typeof rawValue !== "number" || !Number.isFinite(rawValue)) {
      continue;
    }
    normalized[key] = Math.max(0, Math.round(rawValue));
  }
  return Object.keys(normalized).length > 0 ? normalized : undefined;
}

export async function fetchOpenPlatformOpenApiSummary(
  signal?: AbortSignal
): Promise<OpenPlatformOpenApiSummary> {
  const result = await requestJson<unknown>("/api/v1/openapi.json", undefined, signal);
  if (!isRecord(result)) {
    throw new Error("openapi.json 返回结构不合法");
  }
  const pathsRecord = isRecord(result.paths) ? result.paths : {};
  let totalOperations = 0;
  const tagCounter = new Map<string, number>();
  for (const value of Object.values(pathsRecord)) {
    if (!isRecord(value)) {
      continue;
    }
    for (const [method, operation] of Object.entries(value)) {
      if (!["get", "post", "put", "patch", "delete", "head", "options"].includes(method)) {
        continue;
      }
      totalOperations += 1;
      if (isRecord(operation) && Array.isArray(operation.tags)) {
        for (const rawTag of operation.tags) {
          const tag = asString(rawTag);
          if (!tag) {
            continue;
          }
          tagCounter.set(tag, (tagCounter.get(tag) ?? 0) + 1);
        }
      }
    }
  }
  const summary: OpenPlatformOpenApiSummary = {
    version: asString(result.openapi) ?? "unknown",
    totalPaths: Object.keys(pathsRecord).length,
    totalOperations,
    generatedAt: new Date().toISOString(),
    tags: Array.from(tagCounter.entries())
      .map(([tag, operations]) => ({ tag, operations }))
      .sort((a, b) => a.tag.localeCompare(b.tag)),
  };
  if (!isOpenPlatformOpenApiSummary(summary)) {
    throw new Error("openapi.summary 结构不合法");
  }
  return summary;
}

export async function fetchOpenPlatformApiKeys(
  input?: OpenPlatformApiKeyListInput,
  signal?: AbortSignal
): Promise<OpenPlatformApiKeyListResponse> {
  const result = await requestJson<unknown>(
    `/api/v1/api-keys${buildOpenPlatformApiKeyListQuery(input)}`,
    undefined,
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("api-keys 返回结构不合法");
  }
  const items = result.items
    .map((item) => mapOpenPlatformApiKey(item))
    .filter((item): item is OpenPlatformApiKey => Boolean(item));
  const payload: OpenPlatformApiKeyListResponse = {
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
    filters: {
      status: input?.status,
      keyword: input?.keyword,
      limit: input?.limit,
    },
  };
  if (!isOpenPlatformApiKeyListResponse(payload)) {
    throw new Error("api-keys 解析后结构不合法");
  }
  return payload;
}

export async function upsertOpenPlatformApiKey(
  keyId: string,
  input: OpenPlatformApiKeyUpsertInput,
  signal?: AbortSignal
): Promise<OpenPlatformApiKey> {
  const normalizedKeyId = keyId.trim();
  if (!normalizedKeyId) {
    throw new Error("apiKeyId 不能为空。");
  }
  const scopes = input.scopes.filter((item) => item.trim().length > 0);
  if (scopes.length === 0) {
    throw new Error("至少需要一个 scope。");
  }
  const created = await requestJson<unknown>(
    "/api/v1/api-keys",
    {
      method: "POST",
      body: JSON.stringify({
        name: input.name,
        scope: scopes[0],
      }),
    },
    signal
  );
  if (!isRecord(created)) {
    throw new Error("api-keys.create 返回结构不合法");
  }
  const createdId = asString(created.id) ?? normalizedKeyId;
  if (!input.enabled) {
    await requestJson<unknown>(
      `/api/v1/api-keys/${encodeURIComponent(createdId)}/revoke`,
      {
        method: "POST",
        body: JSON.stringify({ reason: "disabled via web console" }),
      },
      signal
    );
  }
  const latestList = await fetchOpenPlatformApiKeys({ keyword: createdId, limit: 50 }, signal);
  const matched = latestList.items.find((item) => item.id === createdId);
  if (matched) {
    return matched;
  }
  return {
    id: createdId,
    tenantId: asString(created.tenantId) ?? "default",
    name: input.name,
    maskedKey: `${asString(created.keyPrefix) ?? "sk_live"}***`,
    status: input.enabled ? "active" : "disabled",
    scopes,
    expiresAt: input.expiresAt,
    createdAt: asIsoDateString(created.createdAt) ?? new Date().toISOString(),
    updatedAt: asIsoDateString(created.createdAt) ?? new Date().toISOString(),
  };
}

export async function revokeOpenPlatformApiKey(
  keyId: string,
  reason?: string,
  signal?: AbortSignal
): Promise<OpenPlatformApiKey> {
  const normalizedKeyId = keyId.trim();
  if (!normalizedKeyId) {
    throw new Error("apiKeyId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v1/api-keys/${encodeURIComponent(normalizedKeyId)}/revoke`,
    {
      method: "POST",
      body: JSON.stringify(reason?.trim() ? { reason: reason.trim() } : {}),
    },
    signal
  );
  const mapped = mapOpenPlatformApiKey(result);
  if (!mapped) {
    throw new Error("api-keys.revoke 返回结构不合法");
  }
  return mapped;
}

export async function fetchOpenPlatformWebhooks(
  input?: OpenPlatformWebhookListInput,
  signal?: AbortSignal
): Promise<OpenPlatformWebhookListResponse> {
  const result = await requestJson<unknown>(
    `/api/v1/webhooks${buildOpenPlatformWebhookListQuery(input)}`,
    undefined,
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("webhooks 返回结构不合法");
  }
  const items = result.items
    .map((item) => mapOpenPlatformWebhook(item))
    .filter((item): item is OpenPlatformWebhook => Boolean(item));
  const payload: OpenPlatformWebhookListResponse = {
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
    filters: {
      enabled: input?.enabled,
      keyword: input?.keyword,
      limit: input?.limit,
    },
  };
  if (!isOpenPlatformWebhookListResponse(payload)) {
    throw new Error("webhooks 解析后结构不合法");
  }
  return payload;
}

export async function upsertOpenPlatformWebhook(
  webhookId: string,
  input: OpenPlatformWebhookUpsertInput,
  signal?: AbortSignal
): Promise<OpenPlatformWebhook> {
  const normalizedWebhookId = webhookId.trim();
  if (!normalizedWebhookId) {
    throw new Error("webhookId 不能为空。");
  }
  const body = {
    name: input.name,
    url: input.url,
    events: input.events,
    status: input.enabled ? "active" : "paused",
    ...(input.secret ? { secret: input.secret } : {}),
  };
  let result: unknown;
  try {
    result = await requestJson<unknown>(
      `/api/v1/webhooks/${encodeURIComponent(normalizedWebhookId)}`,
      {
        method: "PUT",
        body: JSON.stringify(body),
      },
      signal
    );
  } catch (error) {
    const isNotFound =
      (error instanceof ApiError && error.status === 404) ||
      (error instanceof Error && error.message.includes("404")) ||
      String(error).includes("404");
    if (!isNotFound) {
      throw error;
    }
    result = await requestJson<unknown>(
      "/api/v1/webhooks",
      {
        method: "POST",
        body: JSON.stringify(body),
      },
      signal
    );
  }
  const mapped = mapOpenPlatformWebhook(result);
  if (!mapped) {
    throw new Error("webhooks.upsert 返回结构不合法");
  }
  return mapped;
}

export async function deleteOpenPlatformWebhook(
  webhookId: string,
  signal?: AbortSignal
): Promise<void> {
  const normalizedWebhookId = webhookId.trim();
  if (!normalizedWebhookId) {
    throw new Error("webhookId 不能为空。");
  }
  await requestJson<unknown>(
    `/api/v1/webhooks/${encodeURIComponent(normalizedWebhookId)}`,
    { method: "DELETE" },
    signal
  );
}

export async function replayOpenPlatformWebhook(
  webhookId: string,
  input?: OpenPlatformWebhookReplayInput,
  signal?: AbortSignal
): Promise<OpenPlatformWebhookReplayResult> {
  const normalizedWebhookId = webhookId.trim();
  if (!normalizedWebhookId) {
    throw new Error("webhookId 不能为空。");
  }
  const normalizedInput = {
    ...(input?.eventType?.trim() ? { eventType: input.eventType.trim() } : {}),
    ...(input?.from?.trim() ? { from: input.from.trim() } : {}),
    ...(input?.to?.trim() ? { to: input.to.trim() } : {}),
    ...(typeof input?.limit === "number" && Number.isInteger(input.limit) && input.limit > 0
      ? { limit: input.limit }
      : {}),
    ...(typeof input?.dryRun === "boolean" ? { dryRun: input.dryRun } : {}),
  };
  const result = await requestJson<unknown>(
    `/api/v1/webhooks/${encodeURIComponent(normalizedWebhookId)}/replay`,
    {
      method: "POST",
      body: JSON.stringify(normalizedInput),
    },
    signal
  );
  if (!isOpenPlatformWebhookReplayResult(result)) {
    throw new Error("webhooks.replay 返回结构不合法");
  }
  return result;
}

export async function fetchOpenPlatformQualityDaily(
  input?: OpenPlatformQualityDailyQueryInput,
  signal?: AbortSignal
): Promise<OpenPlatformQualityDailyResponse> {
  const result = await requestJson<unknown>(
    `/api/v2/quality/metrics${buildOpenPlatformQualityDailyQuery(input)}`,
    undefined,
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("quality.daily 返回结构不合法");
  }
  const items: OpenPlatformQualityDailyItem[] = result.items
    .map((item) => {
      if (!isRecord(item)) {
        return null;
      }
      const date = asString(item.date);
      const metric = asString(item.metric);
      const avgScore = asFiniteNumber(item.avgScore);
      if (!date || !metric || avgScore === null) {
        return null;
      }
      const normalizedScore = Math.max(0, Math.min(avgScore, 100));
      return {
        date,
        metric,
        value: normalizedScore,
        target: 80,
        score: normalizedScore,
        status: normalizedScore >= 90 ? "pass" : normalizedScore >= 75 ? "warn" : "fail",
      } satisfies OpenPlatformQualityDailyItem;
    })
    .filter((item): item is OpenPlatformQualityDailyItem => Boolean(item));
  const groups =
    Array.isArray(result.groups) &&
    result.groups.length > 0
      ? result.groups
          .map((group) => {
            if (!isRecord(group)) {
              return null;
            }
            const groupByRaw = asString(group.groupBy);
            if (
              groupByRaw !== "provider" &&
              groupByRaw !== "repo" &&
              groupByRaw !== "workflow" &&
              groupByRaw !== "runId"
            ) {
              return null;
            }
            const value = asString(group.value);
            const totalEvents = asFiniteNumber(group.totalEvents);
            const passedEvents = asFiniteNumber(group.passedEvents);
            const failedEvents = asFiniteNumber(group.failedEvents);
            const passRate = asFiniteNumber(group.passRate);
            const avgScore = asFiniteNumber(group.avgScore);
            if (
              !value ||
              totalEvents === null ||
              passedEvents === null ||
              failedEvents === null ||
              passRate === null ||
              avgScore === null
            ) {
              return null;
            }
            return {
              groupBy: groupByRaw,
              value,
              totalEvents,
              passedEvents,
              failedEvents,
              passRate,
              avgScore: Math.max(0, Math.min(avgScore, 100)),
            } satisfies NonNullable<OpenPlatformQualityDailyResponse["groups"]>[number];
          })
          .filter(
            (
              item
            ): item is NonNullable<OpenPlatformQualityDailyResponse["groups"]>[number] =>
              Boolean(item)
          )
      : undefined;
  const payload: OpenPlatformQualityDailyResponse = {
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
    ...(groups ? { groups } : {}),
    filters: {
      date: input?.date,
      from: input?.from,
      to: input?.to,
      metric: input?.metric,
      provider: input?.provider,
      repo: input?.repo,
      workflow: input?.workflow,
      runId: input?.runId,
      groupBy: input?.groupBy,
      limit: input?.limit,
    },
  };
  if (!isOpenPlatformQualityDailyResponse(payload)) {
    throw new Error("quality.daily 解析后结构不合法");
  }
  return payload;
}

export async function fetchOpenPlatformQualityScorecards(
  input?: OpenPlatformQualityScorecardListInput,
  signal?: AbortSignal
): Promise<OpenPlatformQualityScorecardListResponse> {
  const result = await requestJson<unknown>(
    `/api/v2/quality/scorecards${buildOpenPlatformQualityScorecardListQuery(input)}`,
    undefined,
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("quality.scorecards 返回结构不合法");
  }
  const items: OpenPlatformQualityScorecard[] = result.items
    .map((item) => {
      if (!isRecord(item)) {
        return null;
      }
      const id = asString(item.id);
      const metric = asString(item.metric);
      const updatedAt = asIsoDateString(item.updatedAt);
      const targetScore = asFiniteNumber(item.targetScore);
      if (!id || !metric || !updatedAt || targetScore === null) {
        return null;
      }
      const highlights: string[] = [
        `warning=${Number(asFiniteNumber(item.warningScore) ?? 0).toFixed(2)}`,
        `critical=${Number(asFiniteNumber(item.criticalScore) ?? 0).toFixed(2)}`,
        `enabled=${item.enabled === false ? "false" : "true"}`,
      ];
      return {
        id,
        team: metric,
        owner: asString(item.updatedByUserId) ?? "--",
        overallScore: Number(targetScore.toFixed(2)),
        publishedAt: updatedAt,
        highlights,
      } satisfies OpenPlatformQualityScorecard;
    })
    .filter((item): item is OpenPlatformQualityScorecard => Boolean(item));
  const payload: OpenPlatformQualityScorecardListResponse = {
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
    filters: {
      team: input?.team,
      owner: input?.owner,
      limit: input?.limit,
    },
  };
  if (!isOpenPlatformQualityScorecardListResponse(payload)) {
    throw new Error("quality.scorecards 解析后结构不合法");
  }
  return payload;
}

export async function fetchOpenPlatformQualityProjectTrends(
  input?: OpenPlatformQualityProjectTrendQueryInput,
  signal?: AbortSignal
): Promise<OpenPlatformQualityProjectTrendResponse> {
  const result = await requestJson<unknown>(
    `/api/v2/quality/reports/project-trends${buildOpenPlatformQualityProjectTrendQuery(input)}`,
    undefined,
    signal
  );
  if (
    !isRecord(result) ||
    !Array.isArray(result.items) ||
    !isRecord(result.summary) ||
    !isRecord(result.filters)
  ) {
    throw new Error("quality.project-trends 返回结构不合法");
  }
  const items: OpenPlatformQualityProjectTrendItem[] = result.items
    .map((item) => {
      if (!isRecord(item)) {
        return null;
      }
      const project = asString(item.project);
      const metric = asString(item.metric);
      const totalEvents = asFiniteNumber(item.totalEvents);
      const passedEvents = asFiniteNumber(item.passedEvents);
      const failedEvents = asFiniteNumber(item.failedEvents);
      const passRate = asFiniteNumber(item.passRate);
      const avgScore = asFiniteNumber(item.avgScore);
      const totalCost = asFiniteNumber(item.totalCost);
      const totalTokens = asFiniteNumber(item.totalTokens);
      const totalSessions = asFiniteNumber(item.totalSessions);
      const costPerQualityPoint = asFiniteNumber(item.costPerQualityPoint);
      if (
        !project ||
        !metric ||
        totalEvents === null ||
        passedEvents === null ||
        failedEvents === null ||
        passRate === null ||
        avgScore === null ||
        totalCost === null ||
        totalTokens === null ||
        totalSessions === null ||
        costPerQualityPoint === null
      ) {
        return null;
      }
      return {
        project,
        metric,
        totalEvents: Math.max(0, Math.round(totalEvents)),
        passedEvents: Math.max(0, Math.round(passedEvents)),
        failedEvents: Math.max(0, Math.round(failedEvents)),
        passRate,
        avgScore,
        totalCost,
        totalTokens: Math.max(0, Math.round(totalTokens)),
        totalSessions: Math.max(0, Math.round(totalSessions)),
        costPerQualityPoint,
      } satisfies OpenPlatformQualityProjectTrendItem;
    })
    .filter((item): item is OpenPlatformQualityProjectTrendItem => Boolean(item));
  const summary: OpenPlatformQualityProjectTrendSummary = {
    metric: asString(result.summary.metric) ?? input?.metric ?? "all",
    totalEvents: Math.max(0, Math.round(asFiniteNumber(result.summary.totalEvents) ?? 0)),
    passedEvents: Math.max(0, Math.round(asFiniteNumber(result.summary.passedEvents) ?? 0)),
    failedEvents: Math.max(0, Math.round(asFiniteNumber(result.summary.failedEvents) ?? 0)),
    passRate: asFiniteNumber(result.summary.passRate) ?? 0,
    avgScore: asFiniteNumber(result.summary.avgScore) ?? 0,
    totalCost: asFiniteNumber(result.summary.totalCost) ?? 0,
    totalTokens: Math.max(0, Math.round(asFiniteNumber(result.summary.totalTokens) ?? 0)),
    totalSessions: Math.max(0, Math.round(asFiniteNumber(result.summary.totalSessions) ?? 0)),
    from: asIsoDateString(result.summary.from) ?? undefined,
    to: asIsoDateString(result.summary.to) ?? undefined,
  };
  const filters: OpenPlatformQualityProjectTrendResponse["filters"] = {
    from: asString(result.filters.from) ?? input?.from,
    to: asString(result.filters.to) ?? input?.to,
    metric: asString(result.filters.metric) ?? input?.metric,
    provider:
      result.filters.provider === null
        ? null
        : asString(result.filters.provider) ?? input?.provider,
    workflow:
      result.filters.workflow === null
        ? null
        : asString(result.filters.workflow) ?? input?.workflow,
    includeUnknown:
      typeof result.filters.includeUnknown === "boolean"
        ? result.filters.includeUnknown
        : input?.includeUnknown,
    limit:
      typeof result.filters.limit === "number" && Number.isInteger(result.filters.limit)
        ? result.filters.limit
        : input?.limit,
  };
  const payload: OpenPlatformQualityProjectTrendResponse = {
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
    summary,
    filters,
  };
  if (!isOpenPlatformQualityProjectTrendResponse(payload)) {
    throw new Error("quality.project-trends 解析后结构不合法");
  }
  return payload;
}

export async function fetchOpenPlatformAutomationPolicy(
  signal?: AbortSignal
): Promise<OpenPlatformAutomationPolicy> {
  const result = await requestJson<unknown>(
    "/api/v2/quality/automation-policy",
    undefined,
    signal
  );
  if (!isOpenPlatformAutomationPolicy(result)) {
    throw new Error("quality.automation-policy 返回结构不合法");
  }
  return result;
}

export async function upsertOpenPlatformAutomationPolicy(
  input: OpenPlatformAutomationPolicyUpsertInput,
  signal?: AbortSignal
): Promise<OpenPlatformAutomationPolicy> {
  const result = await requestJson<unknown>(
    "/api/v2/quality/automation-policy",
    {
      method: "PUT",
      body: JSON.stringify(input),
    },
    signal
  );
  if (!isOpenPlatformAutomationPolicy(result)) {
    throw new Error("quality.automation-policy.upsert 返回结构不合法");
  }
  return result;
}

export async function simulateOpenPlatformAutomationPolicy(
  input: OpenPlatformAutomationPolicySimulationInput,
  signal?: AbortSignal,
): Promise<OpenPlatformAutomationPolicySimulationResponse> {
  const result = await requestJson<unknown>(
    "/api/v2/quality/automation-policy/simulate",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal,
  );
  if (!isOpenPlatformAutomationPolicySimulationResponse(result)) {
    throw new Error("quality.automation-policy.simulate 返回结构不合法");
  }
  return result;
}

export async function fetchOpenPlatformQualityForecast(
  input: {
    from?: string;
    to?: string;
    metric?: string;
    provider?: string;
    workflow?: string;
    limit?: number;
  } = {},
  signal?: AbortSignal,
): Promise<OpenPlatformQualityForecastResponse> {
  const query = new URLSearchParams();
  if (input.from) query.set("from", input.from);
  if (input.to) query.set("to", input.to);
  if (input.metric) query.set("metric", input.metric);
  if (input.provider) query.set("provider", input.provider);
  if (input.workflow) query.set("workflow", input.workflow);
  if (typeof input.limit === "number") query.set("limit", String(input.limit));
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v2/quality/reports/forecast${suffix}`,
    undefined,
    signal,
  );
  if (!isOpenPlatformQualityForecastResponse(result)) {
    throw new Error("quality.forecast 返回结构不合法");
  }
  return result;
}

export async function fetchOpenPlatformQualityAdvice(
  input: {
    from?: string;
    to?: string;
    provider?: string;
    workflow?: string;
  } = {},
  signal?: AbortSignal,
): Promise<OpenPlatformQualityAdviceResponse> {
  const query = new URLSearchParams();
  if (input.from) query.set("from", input.from);
  if (input.to) query.set("to", input.to);
  if (input.provider) query.set("provider", input.provider);
  if (input.workflow) query.set("workflow", input.workflow);
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v2/quality/reports/advice${suffix}`,
    undefined,
    signal,
  );
  if (!isOpenPlatformQualityAdviceResponse(result)) {
    throw new Error("quality.advice 返回结构不合法");
  }
  return result;
}

export async function executeOpenPlatformQualityAdvice(
  adviceId: string,
  input: {
    project: string;
    severity: "info" | "warn" | "critical";
    actionType: "scorecard_adjustment" | "replay_experiment";
    metric?: string;
    datasetId?: string;
    candidateLabels?: string[];
    triggerSource?: "manual" | "automatic";
  },
  signal?: AbortSignal,
): Promise<OpenPlatformQualityAdviceExecution> {
  const normalizedAdviceId = adviceId.trim();
  if (!normalizedAdviceId) {
    throw new Error("adviceId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/quality/advice/${encodeURIComponent(normalizedAdviceId)}/execute`,
    {
      method: "POST",
      body: JSON.stringify(input),
    },
    signal,
  );
  if (!isOpenPlatformQualityAdviceExecution(result)) {
    throw new Error("quality.advice.execute 返回结构不合法");
  }
  return result;
}

export async function fetchOpenPlatformQualityAdviceExecutions(
  input: {
    adviceId?: string;
    actionType?: "scorecard_adjustment" | "replay_experiment";
    status?: "pending" | "running" | "completed" | "failed" | "cancelled";
    limit?: number;
  } = {},
  signal?: AbortSignal,
): Promise<OpenPlatformQualityAdviceExecutionListResponse> {
  const query = new URLSearchParams();
  if (input.adviceId) query.set("adviceId", input.adviceId);
  if (input.actionType) query.set("actionType", input.actionType);
  if (input.status) query.set("status", input.status);
  if (typeof input.limit === "number") query.set("limit", String(input.limit));
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v2/quality/advice/executions${suffix}`,
    undefined,
    signal,
  );
  if (!isOpenPlatformQualityAdviceExecutionListResponse(result)) {
    throw new Error("quality.advice.executions 返回结构不合法");
  }
  return result;
}

export async function cancelOpenPlatformQualityAdviceExecution(
  executionId: string,
  signal?: AbortSignal,
): Promise<OpenPlatformQualityAdviceExecution> {
  const normalizedExecutionId = executionId.trim();
  if (!normalizedExecutionId) {
    throw new Error("executionId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/quality/advice/executions/${encodeURIComponent(normalizedExecutionId)}/cancel`,
    {
      method: "POST",
    },
    signal,
  );
  if (!isOpenPlatformQualityAdviceExecution(result)) {
    throw new Error("quality.advice.executions.cancel 返回结构不合法");
  }
  return result;
}

export async function fetchOpenPlatformReplayDatasets(
  input?: OpenPlatformReplayDatasetListInput,
  signal?: AbortSignal
): Promise<OpenPlatformReplayDatasetListResponse> {
  const result = await requestJson<unknown>(
    `/api/v2/replay/datasets${buildOpenPlatformReplayBaselineListQuery(input)}`,
    undefined,
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("replay.datasets 返回结构不合法");
  }
  const items: OpenPlatformReplayDataset[] = result.items
    .map((item) => mapOpenPlatformReplayBaseline(item))
    .filter((item): item is OpenPlatformReplayDataset => Boolean(item));
  const payload: OpenPlatformReplayDatasetListResponse = {
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
    filters: {
      keyword: input?.keyword,
      limit: input?.limit,
    },
  };
  if (!isOpenPlatformReplayBaselineListResponse(payload)) {
    throw new Error("replay.datasets 解析后结构不合法");
  }
  return payload;
}

export const fetchOpenPlatformReplayBaselines = fetchOpenPlatformReplayDatasets;

export async function createOpenPlatformReplayDataset(
  input: OpenPlatformReplayDatasetCreateInput,
  signal?: AbortSignal
): Promise<OpenPlatformReplayDataset> {
  const datasetRef = input.datasetRef?.trim() ?? input.datasetId?.trim();
  if (!datasetRef) {
    throw new Error("datasetRef 不能为空。");
  }
  const { datasetId: _legacyDatasetId, datasetRef: _inputDatasetRef, ...rest } = input;
  const result = await requestJson<unknown>(
    "/api/v2/replay/datasets",
    {
      method: "POST",
      body: JSON.stringify({
        ...rest,
        datasetRef,
      }),
    },
    signal
  );
  const mapped = mapOpenPlatformReplayBaseline(result);
  if (!mapped) {
    throw new Error("replay.datasets.create 返回结构不合法");
  }
  return mapped;
}

export async function fetchOpenPlatformReplayDatasetVersions(
  datasetId: string,
  signal?: AbortSignal,
): Promise<OpenPlatformReplayDatasetVersionListResponse> {
  const normalizedDatasetId = datasetId.trim();
  if (!normalizedDatasetId) {
    throw new Error("datasetId 不能为空。");
  }
  const result = await requestJson<unknown>(
    "/api/v2/replay/datasets/" + encodeURIComponent(normalizedDatasetId) + "/versions",
    undefined,
    signal,
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("replay.dataset-versions 返回结构不合法");
  }
  const items = result.items
    .map((item) => mapOpenPlatformReplayDatasetVersion(item, normalizedDatasetId))
    .filter((item): item is OpenPlatformReplayDatasetVersion => Boolean(item));
  const payload: OpenPlatformReplayDatasetVersionListResponse = {
    datasetId: normalizedDatasetId,
    items,
    total:
      typeof result.total === "number" && Number.isInteger(result.total)
        ? result.total
        : items.length,
    ...(result.currentVersionId === null
      ? { currentVersionId: null }
      : asString(result.currentVersionId)
        ? { currentVersionId: asString(result.currentVersionId) ?? undefined }
        : {}),
    ...(typeof result.currentVersionNumber === "number" &&
    Number.isInteger(result.currentVersionNumber) &&
    result.currentVersionNumber >= 1
      ? { currentVersionNumber: result.currentVersionNumber }
      : result.currentVersionNumber === null
        ? { currentVersionNumber: null }
        : {}),
  };
  if (!isOpenPlatformReplayDatasetVersionListResponse(payload)) {
    throw new Error("replay.dataset-versions 解析后结构不合法");
  }
  return payload;
}

export async function createOpenPlatformReplayDatasetVersion(
  datasetId: string,
  input: OpenPlatformReplayDatasetVersionCreateInput,
  signal?: AbortSignal,
): Promise<OpenPlatformReplayDatasetVersion> {
  const normalizedDatasetId = datasetId.trim();
  if (!normalizedDatasetId) {
    throw new Error("datasetId 不能为空。");
  }
  const datasetRef = input.datasetRef?.trim() ?? input.datasetId?.trim();
  if (!datasetRef) {
    throw new Error("datasetRef 不能为空。");
  }
  const { datasetId: _legacyDatasetId, datasetRef: _inputDatasetRef, ...rest } = input;
  const result = await requestJson<unknown>(
    "/api/v2/replay/datasets/" + encodeURIComponent(normalizedDatasetId) + "/versions",
    {
      method: "POST",
      body: JSON.stringify({
        ...rest,
        datasetRef,
      }),
    },
    signal,
  );
  const mapped = mapOpenPlatformReplayDatasetVersion(result, normalizedDatasetId);
  if (!mapped) {
    throw new Error("replay.dataset-versions.create 返回结构不合法");
  }
  return mapped;
}

export async function promoteOpenPlatformReplayDatasetVersion(
  datasetId: string,
  input: OpenPlatformReplayDatasetVersionPromoteInput,
  signal?: AbortSignal,
): Promise<OpenPlatformReplayDatasetVersionPromoteResponse> {
  const normalizedDatasetId = datasetId.trim();
  if (!normalizedDatasetId) {
    throw new Error("datasetId 不能为空。");
  }
  const versionId = input.versionId.trim();
  if (!versionId) {
    throw new Error("versionId 不能为空。");
  }
  const result = await requestJson<unknown>(
    "/api/v2/replay/datasets/" + encodeURIComponent(normalizedDatasetId) + "/promote",
    {
      method: "POST",
      body: JSON.stringify({ versionId }),
    },
    signal,
  );
  if (!isRecord(result)) {
    throw new Error("replay.dataset-versions.promote 返回结构不合法");
  }
  const mappedVersion = mapOpenPlatformReplayDatasetVersion(result.version, normalizedDatasetId);
  const mappedDataset = mapOpenPlatformReplayBaseline(result.dataset ?? result.baseline);
  if (!mappedVersion) {
    throw new Error("replay.dataset-versions.promote 返回结构不合法");
  }
  const payload: OpenPlatformReplayDatasetVersionPromoteResponse = {
    ...(mappedDataset ? { dataset: mappedDataset } : {}),
    version: mappedVersion,
  };
  if (!isOpenPlatformReplayDatasetVersionPromoteResponse(payload)) {
    throw new Error("replay.dataset-versions.promote 解析后结构不合法");
  }
  return payload;
}

export async function fetchOpenPlatformReplayDatasetCases(
  datasetId: string,
  signal?: AbortSignal
): Promise<OpenPlatformReplayDatasetCaseListResponse> {
  const normalizedDatasetId = datasetId.trim();
  if (!normalizedDatasetId) {
    throw new Error("datasetId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/datasets/${encodeURIComponent(normalizedDatasetId)}/cases`,
    undefined,
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("replay.dataset-cases 返回结构不合法");
  }
  const items: OpenPlatformReplayDatasetCase[] = result.items
    .map((item) => mapOpenPlatformReplayDatasetCase(item))
    .filter((item): item is OpenPlatformReplayDatasetCase => Boolean(item));
  const payload: OpenPlatformReplayDatasetCaseListResponse = {
    datasetId: asString(result.datasetId) ?? normalizedDatasetId,
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
  };
  if (!isOpenPlatformReplayDatasetCaseListResponse(payload)) {
    throw new Error("replay.dataset-cases 解析后结构不合法");
  }
  return payload;
}

export async function replaceOpenPlatformReplayDatasetCases(
  datasetId: string,
  input: OpenPlatformReplayDatasetCaseReplaceInput,
  signal?: AbortSignal
): Promise<OpenPlatformReplayDatasetCaseListResponse> {
  const normalizedDatasetId = datasetId.trim();
  if (!normalizedDatasetId) {
    throw new Error("datasetId 不能为空。");
  }
  if (!Array.isArray(input.items)) {
    throw new Error("items 必须为数组。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/datasets/${encodeURIComponent(normalizedDatasetId)}/cases`,
    {
      method: "POST",
      body: JSON.stringify({ items: input.items }),
    },
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("replay.dataset-cases.replace 返回结构不合法");
  }
  const items: OpenPlatformReplayDatasetCase[] = result.items
    .map((item) => mapOpenPlatformReplayDatasetCase(item))
    .filter((item): item is OpenPlatformReplayDatasetCase => Boolean(item));
  const payload: OpenPlatformReplayDatasetCaseListResponse = {
    datasetId: asString(result.datasetId) ?? normalizedDatasetId,
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
  };
  if (!isOpenPlatformReplayDatasetCaseListResponse(payload)) {
    throw new Error("replay.dataset-cases.replace 解析后结构不合法");
  }
  return payload;
}

export async function materializeOpenPlatformReplayDatasetCases(
  datasetId: string,
  input: OpenPlatformReplayDatasetMaterializeInput,
  signal?: AbortSignal
): Promise<OpenPlatformReplayDatasetMaterializeResponse> {
  const normalizedDatasetId = datasetId.trim();
  if (!normalizedDatasetId) {
    throw new Error("datasetId 不能为空。");
  }
  const sessionIds = input.sessionIds?.map((item) => item.trim()).filter(Boolean);
  const filters =
    input.filters && typeof input.filters === "object"
      ? Object.fromEntries(
          Object.entries(input.filters).filter(([, value]) => typeof value === "string" && value.trim().length > 0)
        )
      : undefined;
  const result = await requestJson<unknown>(
    `/api/v2/replay/datasets/${encodeURIComponent(normalizedDatasetId)}/materialize`,
    {
      method: "POST",
      body: JSON.stringify({
        ...(sessionIds && sessionIds.length > 0 ? { sessionIds } : {}),
        ...(filters && Object.keys(filters).length > 0 ? { filters } : {}),
        ...(typeof input.sampleLimit === "number" ? { sampleLimit: input.sampleLimit } : {}),
        ...(typeof input.sanitized === "boolean" ? { sanitized: input.sanitized } : {}),
        ...(typeof input.snapshotVersion === "string" && input.snapshotVersion.trim().length > 0
          ? { snapshotVersion: input.snapshotVersion.trim() }
          : {}),
      }),
    },
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("replay.dataset-cases.materialize 返回结构不合法");
  }
  const items: OpenPlatformReplayDatasetCase[] = result.items
    .map((item) => mapOpenPlatformReplayDatasetCase(item))
    .filter((item): item is OpenPlatformReplayDatasetCase => Boolean(item));
  const payload: OpenPlatformReplayDatasetMaterializeResponse = {
    datasetId: asString(result.datasetId) ?? normalizedDatasetId,
    sourceType: "session",
    materialized:
      typeof result.materialized === "number" && Number.isInteger(result.materialized)
        ? result.materialized
        : items.length,
    skipped:
      typeof result.skipped === "number" && Number.isInteger(result.skipped) ? result.skipped : 0,
    ...(normalizeReplaySourceSummary(result.sourceSummary)
      ? { sourceSummary: normalizeReplaySourceSummary(result.sourceSummary) }
      : {}),
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
    filters: {
      datasetId: normalizedDatasetId,
      ...(isRecord(result.filters)
        ? {
            sessionIds: Array.isArray(result.filters.sessionIds)
              ? result.filters.sessionIds.filter((item): item is string => typeof item === "string")
              : undefined,
            filters: isRecord(result.filters.filters) ? result.filters.filters : undefined,
            sampleLimit:
              typeof result.filters.sampleLimit === "number" ? result.filters.sampleLimit : input.sampleLimit,
            sanitized:
              typeof result.filters.sanitized === "boolean" ? result.filters.sanitized : input.sanitized,
            snapshotVersion:
              typeof result.filters.snapshotVersion === "string"
                ? result.filters.snapshotVersion
                : input.snapshotVersion,
          }
        : {}),
    },
  };
  if (!isOpenPlatformReplayDatasetMaterializeResponse(payload)) {
    throw new Error("replay.dataset-cases.materialize 解析后结构不合法");
  }
  return payload;
}

export async function fetchOpenPlatformReplayRuns(
  input?: OpenPlatformReplayRunListInput,
  signal?: AbortSignal
): Promise<OpenPlatformReplayRunListResponse> {
  const result = await requestJson<unknown>(
    `/api/v2/replay/runs${buildOpenPlatformReplayJobListQuery(input)}`,
    undefined,
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("replay.runs 返回结构不合法");
  }
  const items: OpenPlatformReplayRun[] = result.items
    .map((item) => mapOpenPlatformReplayJob(item))
    .filter((item): item is OpenPlatformReplayRun => Boolean(item));
  const payload: OpenPlatformReplayRunListResponse = {
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
    filters: {
      datasetId: input?.datasetId ?? input?.baselineId,
      baselineId: input?.baselineId ?? input?.datasetId,
      status: input?.status,
      limit: input?.limit,
    },
  };
  if (!isOpenPlatformReplayJobListResponse(payload)) {
    throw new Error("replay.jobs 解析后结构不合法");
  }
  return payload;
}

export const fetchOpenPlatformReplayJobs = fetchOpenPlatformReplayRuns;

export async function createOpenPlatformReplayExperiment(
  input: {
    name: string;
    datasetId: string;
    baselineId?: string;
    baselineVersionId?: string;
    runIds?: string[];
    candidateLabels?: string[];
    autoRun?: boolean;
    triggerSource?: "manual" | "quality_advice" | "automatic";
    sourceAdviceId?: string;
  },
  signal?: AbortSignal,
): Promise<OpenPlatformReplayExperiment> {
  const { baselineVersionId, ...rest } = input;
  const result = await requestJson<unknown>(
    "/api/v2/replay/experiments",
    {
      method: "POST",
      body: JSON.stringify({
        ...rest,
        ...(baselineVersionId?.trim()
          ? { baselineVersionId: baselineVersionId.trim() }
          : {}),
      }),
    },
    signal,
  );
  if (!isOpenPlatformReplayExperiment(result)) {
    throw new Error("replay.experiments.create 返回结构不合法");
  }
  return result;
}

export async function fetchOpenPlatformReplayExperiments(
  input?: { datasetId?: string; limit?: number },
  signal?: AbortSignal,
): Promise<OpenPlatformReplayExperimentListResponse> {
  const query = new URLSearchParams();
  if (input?.datasetId) query.set("datasetId", input.datasetId);
  if (typeof input?.limit === "number") query.set("limit", String(input.limit));
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  const result = await requestJson<unknown>(
    `/api/v2/replay/experiments${suffix}`,
    undefined,
    signal,
  );
  if (!isOpenPlatformReplayExperimentListResponse(result)) {
    throw new Error("replay.experiments 返回结构不合法");
  }
  return result;
}

export async function fetchOpenPlatformReplayExperimentsBatchCompare(
  input: { experimentIds: string[]; datasetId?: string },
  signal?: AbortSignal,
): Promise<OpenPlatformReplayExperimentBatchCompareResponse> {
  const experimentIds = Array.from(
    new Set(input.experimentIds.map((item) => item.trim()).filter(Boolean)),
  );
  if (experimentIds.length < 2) {
    throw new Error("experimentIds 至少需要 2 个。");
  }
  const query = new URLSearchParams();
  query.set("experimentIds", experimentIds.join(","));
  if (input.datasetId?.trim()) {
    query.set("datasetId", input.datasetId.trim());
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/experiments/compare?${query.toString()}`,
    undefined,
    signal,
  );
  if (!isOpenPlatformReplayExperimentBatchCompareResponse(result)) {
    throw new Error("replay.experiments.compare 返回结构不合法");
  }
  return result;
}

export async function fetchOpenPlatformReplayExperiment(
  experimentId: string,
  signal?: AbortSignal,
): Promise<OpenPlatformReplayExperiment> {
  const normalizedExperimentId = experimentId.trim();
  if (!normalizedExperimentId) {
    throw new Error("experimentId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/experiments/${encodeURIComponent(normalizedExperimentId)}`,
    undefined,
    signal,
  );
  if (!isOpenPlatformReplayExperiment(result)) {
    throw new Error("replay.experiment 返回结构不合法");
  }
  return result;
}

export async function runOpenPlatformReplayExperiment(
  experimentId: string,
  signal?: AbortSignal,
): Promise<OpenPlatformReplayExperiment> {
  const normalizedExperimentId = experimentId.trim();
  if (!normalizedExperimentId) {
    throw new Error("experimentId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/experiments/${encodeURIComponent(normalizedExperimentId)}/run`,
    { method: "POST" },
    signal,
  );
  if (!isOpenPlatformReplayExperiment(result)) {
    throw new Error("replay.experiment.run 返回结构不合法");
  }
  return result;
}

export async function cancelOpenPlatformReplayExperiment(
  experimentId: string,
  signal?: AbortSignal,
): Promise<OpenPlatformReplayExperiment> {
  const normalizedExperimentId = experimentId.trim();
  if (!normalizedExperimentId) {
    throw new Error("experimentId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/experiments/${encodeURIComponent(normalizedExperimentId)}/cancel`,
    { method: "POST" },
    signal,
  );
  if (!isOpenPlatformReplayExperiment(result)) {
    throw new Error("replay.experiment.cancel 返回结构不合法");
  }
  return result;
}

export async function fetchOpenPlatformReplayExperimentResults(
  experimentId: string,
  signal?: AbortSignal,
): Promise<OpenPlatformReplayExperiment> {
  const normalizedExperimentId = experimentId.trim();
  if (!normalizedExperimentId) {
    throw new Error("experimentId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/experiments/${encodeURIComponent(normalizedExperimentId)}/results`,
    undefined,
    signal,
  );
  if (!isOpenPlatformReplayExperiment(result)) {
    throw new Error("replay.experiment.results 返回结构不合法");
  }
  return result;
}

export async function fetchOpenPlatformReplayExperimentCompare(
  experimentId: string,
  signal?: AbortSignal,
): Promise<OpenPlatformReplayExperimentCompareResponse> {
  const normalizedExperimentId = experimentId.trim();
  if (!normalizedExperimentId) {
    throw new Error("experimentId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/experiments/${encodeURIComponent(normalizedExperimentId)}/compare`,
    undefined,
    signal,
  );
  if (!isOpenPlatformReplayExperimentCompareResponse(result)) {
    throw new Error("replay.experiment.compare 返回结构不合法");
  }
  return result;
}

export async function fetchOpenPlatformReplayExperimentWorkflow(
  experimentId: string,
  signal?: AbortSignal,
): Promise<OpenPlatformReplayExperimentWorkflowResponse> {
  const normalizedExperimentId = experimentId.trim();
  if (!normalizedExperimentId) {
    throw new Error("experimentId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/experiments/${encodeURIComponent(normalizedExperimentId)}/workflow`,
    undefined,
    signal,
  );
  if (!isOpenPlatformReplayExperimentWorkflowResponse(result)) {
    throw new Error("replay.experiment.workflow 返回结构不合法");
  }
  return result;
}

export async function createOpenPlatformReplayRun(
  input: OpenPlatformReplayRunCreateInput,
  signal?: AbortSignal
): Promise<OpenPlatformReplayRun> {
  const datasetId = input.datasetId?.trim() ?? input.baselineId?.trim();
  if (!datasetId) {
    throw new Error("datasetId 不能为空。");
  }
  const baselineVersionId = input.baselineVersionId?.trim();
  const { baselineId: _legacyBaselineId, datasetId: _inputDatasetId, baselineVersionId: _inputBaselineVersionId, ...rest } = input;
  const result = await requestJson<unknown>(
    "/api/v2/replay/runs",
    {
      method: "POST",
      body: JSON.stringify({
        ...rest,
        datasetId,
        ...(baselineVersionId ? { baselineVersionId } : {}),
      }),
    },
    signal
  );
  const mapped = mapOpenPlatformReplayJob(result);
  if (!mapped) {
    throw new Error("replay.runs.create 返回结构不合法");
  }
  return mapped;
}

export async function fetchOpenPlatformReplayDiffs(
  input: OpenPlatformReplayDiffQueryInput,
  signal?: AbortSignal
): Promise<OpenPlatformReplayDiffResponse> {
  const datasetId = input.datasetId?.trim() ?? input.baselineId?.trim();
  const requestedRunId = input.runId?.trim() ?? input.jobId?.trim();
  if (!requestedRunId) {
    throw new Error("runId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/runs/${encodeURIComponent(requestedRunId)}/diffs${buildOpenPlatformReplayDiffQuery({
      ...input,
      datasetId,
      runId: requestedRunId,
    })}`,
    undefined,
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.diffs)) {
    throw new Error("replay.diff 返回结构不合法");
  }
  const items = result.diffs
    .map<OpenPlatformReplayDiffItem | null>((item) => {
      if (!isRecord(item)) {
        return null;
      }
      const caseId = asString(item.caseId);
      const verdict = asString(item.verdict);
      const delta = asFiniteNumber(item.delta);
      if (!caseId || !verdict || delta === null) {
        return null;
      }
      const responseDatasetId =
        datasetId ?? asString(result.datasetId) ?? asString(result.baselineId) ?? "unknown";
      const runId = asString(result.runId) ?? asString(result.jobId) ?? requestedRunId;
      return {
        id: `${runId}:${caseId}`,
        datasetId: responseDatasetId,
        baselineId: responseDatasetId,
        runId,
        jobId: runId,
        caseId,
        summary: asString(item.detail) ?? `${asString(item.metric) ?? "metric"} delta`,
        verdict:
          verdict === "improved" || verdict === "regressed" || verdict === "unchanged"
            ? verdict
            : "unchanged",
        deltaScore: delta,
      } satisfies OpenPlatformReplayDiffItem;
    })
    .filter((item): item is OpenPlatformReplayDiffItem => item !== null);
  const payload: OpenPlatformReplayDiffResponse = {
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
    ...(isRecord(result.summary) ? { summary: result.summary } : {}),
    filters: {
      datasetId: datasetId ?? asString(result.datasetId) ?? asString(result.baselineId) ?? undefined,
      baselineId: datasetId ?? asString(result.datasetId) ?? asString(result.baselineId) ?? undefined,
      runId: asString(result.runId) ?? asString(result.jobId) ?? requestedRunId,
      jobId: asString(result.jobId) ?? asString(result.runId) ?? requestedRunId,
      keyword: input.keyword,
      limit: input.limit,
    },
  };
  if (!isOpenPlatformReplayDiffResponse(payload)) {
    throw new Error("replay.diff 解析后结构不合法");
  }
  return payload;
}

export const fetchOpenPlatformReplayDiff = fetchOpenPlatformReplayDiffs;

export async function fetchOpenPlatformReplayArtifacts(
  runId: string,
  signal?: AbortSignal
): Promise<OpenPlatformReplayArtifactListResponse> {
  const normalizedRunId = runId.trim();
  if (!normalizedRunId) {
    throw new Error("runId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/runs/${encodeURIComponent(normalizedRunId)}/artifacts`,
    undefined,
    signal
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("replay.artifacts 返回结构不合法");
  }
  const items = result.items
    .map((item) => mapOpenPlatformReplayArtifact(item))
    .filter((item): item is OpenPlatformReplayArtifact => Boolean(item));
  const responseRunId = asString(result.runId) ?? asString(result.jobId) ?? normalizedRunId;
  const payload: OpenPlatformReplayArtifactListResponse = {
    runId: responseRunId,
    jobId: asString(result.jobId) ?? responseRunId,
    ...(asString(result.datasetId) ? { datasetId: asString(result.datasetId) ?? undefined } : {}),
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
  };
  if (!isOpenPlatformReplayArtifactListResponse(payload)) {
    throw new Error("replay.artifacts 解析后结构不合法");
  }
  return payload;
}

export async function fetchOpenPlatformReplayDatasetVersionCases(
  datasetId: string,
  versionId: string,
  signal?: AbortSignal,
): Promise<OpenPlatformReplayDatasetVersionCaseListResponse> {
  const normalizedDatasetId = datasetId.trim();
  const normalizedVersionId = versionId.trim();
  if (!normalizedDatasetId || !normalizedVersionId) {
    throw new Error("datasetId 与 versionId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/datasets/${encodeURIComponent(normalizedDatasetId)}/versions/${encodeURIComponent(normalizedVersionId)}/cases`,
    undefined,
    signal,
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("replay.dataset-version-cases 返回结构不合法");
  }
  const items = result.items
    .map((item) => mapOpenPlatformReplayDatasetCase(item))
    .filter((item): item is OpenPlatformReplayDatasetCase => Boolean(item));
  const payload: OpenPlatformReplayDatasetVersionCaseListResponse = {
    datasetId: asString(result.datasetId) ?? normalizedDatasetId,
    versionId: asString(result.versionId) ?? normalizedVersionId,
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
  };
  if (!isOpenPlatformReplayDatasetVersionCaseListResponse(payload)) {
    throw new Error("replay.dataset-version-cases 解析后结构不合法");
  }
  return payload;
}

export async function fetchOpenPlatformReplayExperimentArtifacts(
  experimentId: string,
  signal?: AbortSignal,
): Promise<OpenPlatformReplayExperimentArtifactListResponse> {
  const normalizedExperimentId = experimentId.trim();
  if (!normalizedExperimentId) {
    throw new Error("experimentId 不能为空。");
  }
  const result = await requestJson<unknown>(
    `/api/v2/replay/experiments/${encodeURIComponent(normalizedExperimentId)}/artifacts`,
    undefined,
    signal,
  );
  if (!isRecord(result) || !Array.isArray(result.items)) {
    throw new Error("replay.experiment.artifacts 返回结构不合法");
  }
  const items = result.items
    .map((item) => mapOpenPlatformReplayArtifact(item))
    .filter((item): item is OpenPlatformReplayArtifact => Boolean(item));
  const payload: OpenPlatformReplayExperimentArtifactListResponse = {
    experimentId: asString(result.experimentId) ?? normalizedExperimentId,
    datasetId: asString(result.datasetId) ?? "unknown",
    items,
    total: typeof result.total === "number" && Number.isInteger(result.total) ? result.total : items.length,
  };
  if (!isOpenPlatformReplayExperimentArtifactListResponse(payload)) {
    throw new Error("replay.experiment.artifacts 解析后结构不合法");
  }
  return payload;
}

export async function downloadOpenPlatformReplayArtifact(
  runId: string,
  artifactType: string,
  downloadName?: string,
  signal?: AbortSignal
): Promise<DownloadFile> {
  const normalizedRunId = runId.trim();
  if (!normalizedRunId) {
    throw new Error("runId 不能为空。");
  }
  const normalizedArtifactType = artifactType.trim();
  if (!normalizedArtifactType) {
    throw new Error("artifactType 不能为空。");
  }
  return requestBlob(
    `/api/v2/replay/runs/${encodeURIComponent(normalizedRunId)}/artifacts/${encodeURIComponent(
      normalizedArtifactType
    )}/download`,
    downloadName?.trim() || `${normalizedArtifactType}.json`,
    undefined,
    signal
  );
}
