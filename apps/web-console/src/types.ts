export type MetricKey = "tokens" | "cost" | "sessions";
export type SourceType = "local" | "ssh" | "sync-cache";
export type SourceAccessMode = "realtime" | "sync" | "hybrid" | (string & {});
export type AlertOrchestrationDispatchMode = "rule" | "fallback";

export interface SourceSyncPayload {
  enabled?: boolean;
  status?: string;
  cron?: string;
  retentionDays?: number;
  [key: string]: unknown;
}

export type SourceSync = boolean | SourceSyncPayload;

export interface Source {
  id: string;
  name: string;
  type: SourceType;
  location: string;
  sourceRegion?: string;
  enabled: boolean;
  accessMode?: SourceAccessMode;
  sync?: SourceSync;
  syncCron?: string;
  syncRetentionDays?: number;
  createdAt: string;
}

export interface SourceListResponse {
  items: Source[];
  total: number;
}

export type AgentRuntimeStatus = "online" | "stale" | "never_seen";

export interface AgentRuntimeView {
  id: string;
  agentId: string;
  tenantId: string;
  deviceId?: string;
  displayName: string;
  hostname: string;
  version?: string;
  sourceCount: number;
  sourceIds: string[];
  sourceNames: string[];
  runtimeStatus: AgentRuntimeStatus;
  lastHeartbeatAt: string | null;
  lastConfigFetchedAt: string | null;
  lastConfigVersion?: string;
  lastError?: string;
  lastIngestStatusCode: number | null;
  lastAccepted: number;
  lastRejected: number;
  heartbeatIntervalSeconds: number;
  staleAfterSeconds: number;
  ingestProtocol: "http" | "grpc";
  ingestEndpoint?: string;
  updatedAt: string;
}

export interface AgentRuntimeViewListResponse {
  items: AgentRuntimeView[];
  total: number;
  generatedAt: string;
}

export interface AgentRuntimeConfigResponse {
  tenantId: string;
  agent: {
    agentId: string;
    deviceId?: string;
    hostname: string;
    version?: string;
    displayName: string;
  };
  runtime: {
    heartbeatIntervalSeconds: number;
    staleAfterSeconds: number;
    ingestProtocol: "http" | "grpc";
    ingestEndpoint?: string;
    sampleGenerateCount: number;
  };
  bindings: {
    sourceCount: number;
    sourceIds: string[];
    sources: Array<{
      sourceId: string;
      name: string;
      accessMode: string;
      enabled: boolean;
      location: string;
      sourceRegion?: string;
    }>;
  };
  configVersion: string;
  updatedAt: string;
}

export interface CreateSourceInput {
  name: string;
  type: SourceType;
  location: string;
  sourceRegion?: string;
  enabled?: boolean;
  accessMode?: SourceAccessMode;
  sync?: SourceSync;
  syncCron?: string;
  syncRetentionDays?: number;
}

export interface UpdateSourceInput {
  name?: string;
  location?: string;
  sourceRegion?: string;
  enabled?: boolean;
  accessMode?: SourceAccessMode;
  syncCron?: string;
  syncRetentionDays?: number;
}

export interface SourceMissingRegionListResponse {
  items: Source[];
  total: number;
}

export type SourceRegionBackfillItemStatus = "updated" | "would_update" | "skipped";

export interface SourceRegionBackfillResultItem {
  sourceId: string;
  name: string;
  status: SourceRegionBackfillItemStatus;
  appliedRegion?: string;
  reason?: string;
}

export interface SourceRegionBackfillResult {
  tenantId: string;
  dryRun: boolean;
  primaryRegion: string;
  totalMissing: number;
  updated: number;
  skipped: number;
  items: SourceRegionBackfillResultItem[];
}

export interface SourceHealth {
  sourceId: string;
  accessMode: SourceAccessMode;
  lastSuccessAt: string | null;
  lastFailureAt: string | null;
  failureCount: number;
  avgLatencyMs: number | null;
  freshnessMinutes: number | null;
}

export interface SourceParseFailure {
  id: string;
  sourceId: string;
  parserKey: string;
  errorCode: string;
  errorMessage: string;
  sourcePath?: string;
  sourceOffset?: number;
  rawHash?: string;
  metadata: Record<string, unknown>;
  failedAt: string;
  createdAt: string;
}

export interface SourceParseFailureQueryInput {
  from?: string;
  to?: string;
  parserKey?: string;
  errorCode?: string;
  limit?: number;
}

export interface SourceParseFailureListResponse {
  items: SourceParseFailure[];
  total: number;
  filters?: SourceParseFailureQueryInput;
}

export interface HeatmapCell {
  date: string;
  tokens: number;
  cost: number;
  sessions: number;
}

export interface UsageHeatmapResponse {
  cells: HeatmapCell[];
  summary: {
    tokens: number;
    cost: number;
    sessions: number;
  };
}

export interface Session {
  id: string;
  sourceId: string;
  tool: string;
  model: string;
  startedAt: string;
  endedAt?: string | null;
  tokens: number;
  cost: number;
}

export interface SessionSearchInput {
  sourceId?: string;
  keyword?: string;
  clientType?: string;
  tool?: string;
  host?: string;
  model?: string;
  project?: string;
  from?: string;
  to?: string;
  limit?: number;
  cursor?: string;
}

export interface SessionSearchResponse {
  items: Session[];
  total: number;
  nextCursor: string | null;
  filters?: SessionSearchInput;
  sourceFreshness?: SessionSourceFreshness[];
}

export type TokenPulseRuntimeEventStatus =
  | "success"
  | "failure"
  | "blocked"
  | "timeout";

export type TokenPulseRoutePolicy =
  | "round_robin"
  | "latest_valid"
  | "sticky_user";

export interface TokenPulseRuntimeEvent {
  id: string;
  tenantId: string;
  projectId?: string;
  traceId: string;
  provider: string;
  model: string;
  resolvedModel: string;
  routePolicy: TokenPulseRoutePolicy;
  accountId?: string;
  status: TokenPulseRuntimeEventStatus;
  startedAt: string;
  finishedAt?: string;
  errorCode?: string;
  cost?: string;
  idempotencyKey: string;
  specVersion: "v1";
  keyId: string;
  createdAt: string;
}

export interface TokenPulseRuntimeEventListInput {
  traceId?: string;
  provider?: string;
  status?: TokenPulseRuntimeEventStatus;
  limit?: number;
  cursor?: string;
}

export interface TokenPulseRuntimeEventListResponse {
  items: TokenPulseRuntimeEvent[];
  total: number;
  filters: TokenPulseRuntimeEventListInput;
  nextCursor: string | null;
}

export interface SessionDetail extends Session {
  provider?: string;
  sourceName?: string;
  sourceType?: SourceType;
  sourceLocation?: string;
  sourceHost?: string;
  sourcePath?: string;
  workspace?: string;
  messageCount: number;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  reasoningTokens: number;
}

export interface SessionTokenBreakdown {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  reasoningTokens: number;
  totalTokens: number;
}

export interface SessionSourceTrace {
  sourceId: string;
  sourceName?: string;
  provider?: string;
  path?: string;
}

export interface SessionSourceFreshness {
  sourceId: string;
  sourceName?: string;
  accessMode: SourceAccessMode;
  lastSuccessAt: string | null;
  lastFailureAt: string | null;
  failureCount: number;
  avgLatencyMs: number | null;
  freshnessMinutes: number | null;
}

export interface SessionDetailResponse extends SessionDetail {
  session?: SessionDetail;
  tokenBreakdown: SessionTokenBreakdown;
  sourceTrace: SessionSourceTrace;
}

export type AlertSeverity = "warning" | "critical";
export type AlertStatus = "open" | "acknowledged" | "resolved";
export type AlertMutableStatus = "acknowledged" | "resolved";
export type AlertOrchestrationEventType = "alert" | "weekly";
export type AlertOrchestrationEscalationReason = "sla_timeout";
export type AlertOrchestrationChannel =
  | "webhook"
  | "wecom"
  | "dingtalk"
  | "feishu"
  | "email"
  | "email_webhook"
  | "incident"
  | "ticket";

export interface AlertListInput {
  status?: AlertStatus;
  severity?: AlertSeverity;
  scope?: string;
  scopeRef?: string;
  budgetId?: string;
  from?: string;
  to?: string;
  limit?: number;
  cursor?: string;
}

export interface AlertItem {
  id: string;
  tenantId: string;
  budgetId: string;
  scope: string;
  scopeRef: string;
  severity: AlertSeverity;
  status: AlertStatus;
  message: string;
  threshold: number;
  value: number;
  createdAt: string;
  updatedAt: string;
  metadata: Record<string, unknown>;
  externalLinks?: Array<{
    id: string;
    externalType: "ticket" | "case" | "incident";
    externalSystem: string;
    externalId: string;
    externalStatus?: string;
    pendingExternalStatus?: string;
    lastSyncedAt: string;
    publishStatus?: "success" | "failed";
    publishError?: string;
    lastSyncResult?: "success" | "failed";
    lastSyncError?: string;
    lastSyncFailureStage?: string;
    lastSyncFailureCode?: string;
  }>;
}

export interface AlertExternalLinkOpsItem {
  id: string;
  alertId?: string;
  alertStatus?: AlertStatus;
  externalType: "ticket" | "case" | "incident";
  externalSystem: string;
  externalId: string;
  externalStatus?: string;
  pendingExternalStatus?: string;
  lastSyncedAt: string;
  publishStatus?: "success" | "failed";
  publishError?: string;
  lastSyncResult?: "success" | "failed";
  lastSyncError?: string;
  lastSyncFailureStage?: string;
  lastSyncFailureCode?: string;
  syncState: "synced" | "pending" | "failed";
  retryable: boolean;
  updatedAt?: string;
}

export interface AlertExternalLinkOpsResponse {
  alertId: string;
  summary: {
    total: number;
    pending: number;
    failed: number;
  };
  items: AlertExternalLinkOpsItem[];
  filters: {
    externalType?: "ticket" | "case" | "incident";
    onlyFailed?: boolean;
  };
}

export interface AlertExternalLinkBatchRetryResponse {
  alertId: string;
  retriedCount: number;
  published: number;
  failed: number;
  items: AlertExternalLinkOpsItem[];
}

export interface AlertExternalLinkFailureResponse {
  summary: {
    total: number;
    pending: number;
    failed: number;
  };
  items: AlertExternalLinkOpsItem[];
  filters: {
    alertId?: string;
    externalType?: "ticket" | "case" | "incident";
    externalSystem?: string;
    syncState?: "synced" | "pending" | "failed";
    limit?: number;
  };
}

export interface IntegrationDlqMessage {
  messageId: string;
  stream: string;
  subject: string;
  eventType: string;
  channel?: string;
  callbackId?: string;
  tenantId?: string;
  alertId?: string;
  externalType?: string;
  externalId?: string;
  failedAt: string;
  attempt: number;
  error: string;
  retryable: boolean;
  payload: Record<string, unknown>;
}

export interface IntegrationDlqMessageListResponse {
  items: IntegrationDlqMessage[];
  total: number;
  filters: {
    eventType?: string;
    channel?: string;
    callbackId?: string;
    alertId?: string;
    limit?: number;
  };
}

export interface IntegrationDlqReplayResponse {
  replayedCount: number;
  failedCount: number;
  items: Array<{
    messageId: string;
    status: "replayed" | "failed";
    error?: string;
  }>;
}

export type IntegrationDlqRecoveryJobStatus =
  | "queued"
  | "running"
  | "completed"
  | "failed";

export interface IntegrationDlqRecoveryJob {
  id: string;
  tenantId: string;
  status: IntegrationDlqRecoveryJobStatus;
  requestedAt: string;
  startedAt?: string;
  finishedAt?: string;
  filters?: {
    eventType?: string;
    channel?: string;
    callbackId?: string;
    alertId?: string;
    limit?: number;
  };
  messageIds: string[];
  summary: {
    total: number;
    replayed: number;
    failed: number;
  };
  items: Array<{
    messageId: string;
    status: "replayed" | "failed";
    error?: string;
  }>;
  error?: string;
}

export interface IntegrationDlqRecoveryJobListResponse {
  items: IntegrationDlqRecoveryJob[];
  total: number;
  filters: {
    status?: IntegrationDlqRecoveryJobStatus;
    limit?: number;
  };
}

export type IntegrationAlertFailureReportActionType =
  | "retry_requested"
  | "retry_completed"
  | "retry_failed"
  | "dlq_queried"
  | "dlq_replayed"
  | "recovery_job_created"
  | "recovery_job_completed"
  | "recovery_job_failed";

export interface IntegrationAlertFailureReportItem {
  occurredAt: string;
  action: string;
  actionType: IntegrationAlertFailureReportActionType;
  alertId?: string;
  externalSystem?: string;
  externalType?: string;
  externalId?: string;
  stage?: string;
  code?: string;
  status: "requested" | "success" | "failed";
  requestId?: string;
  metadata: Record<string, unknown>;
}

export interface IntegrationAlertFailureReportResponse {
  summary: {
    totalEvents: number;
    retryRequested: number;
    retryCompleted: number;
    retryFailed: number;
    dlqQueried: number;
    dlqReplayed: number;
    recoveryJobsCreated: number;
    recoveryJobsCompleted: number;
    recoveryJobsFailed: number;
  };
  items: IntegrationAlertFailureReportItem[];
  filters: {
    from?: string;
    to?: string;
    externalSystem?: string;
    stage?: string;
    actionType?: IntegrationAlertFailureReportActionType;
    limit?: number;
  };
}

export interface IntegrationAlertFailureTrendPoint {
  date: string;
  totalEvents: number;
  requestedEvents: number;
  successEvents: number;
  failedEvents: number;
  uniqueAlerts: number;
  retryRequested: number;
  retryCompleted: number;
  retryFailed: number;
  dlqQueried: number;
  dlqReplayed: number;
  recoveryJobsCreated: number;
  recoveryJobsCompleted: number;
  recoveryJobsFailed: number;
}

export interface IntegrationAlertFailureTrendCapacityBucket {
  name: string;
  totalEvents: number;
  requestedEvents: number;
  successEvents: number;
  failedEvents: number;
  uniqueAlerts: number;
  lastOccurredAt?: string;
}

export interface IntegrationAlertFailureTrendResponse {
  summary: {
    totalEvents: number;
    requestedEvents: number;
    successEvents: number;
    failedEvents: number;
    days: number;
    averageEventsPerDay: number;
    peakDate?: string;
    peakCount: number;
  };
  daily: IntegrationAlertFailureTrendPoint[];
  capacity: {
    externalSystems: IntegrationAlertFailureTrendCapacityBucket[];
    stages: IntegrationAlertFailureTrendCapacityBucket[];
  };
  filters: {
    from?: string;
    to?: string;
    externalSystem?: string;
    stage?: string;
    actionType?: IntegrationAlertFailureReportActionType;
    top: number;
  };
}

export interface AlertListResponse {
  items: AlertItem[];
  total: number;
  filters: AlertListInput;
  nextCursor: string | null;
}

export interface AlertOrchestrationRule {
  id: string;
  tenantId: string;
  name: string;
  enabled: boolean;
  eventType: AlertOrchestrationEventType;
  severity?: AlertSeverity;
  sourceId?: string;
  dedupeWindowSeconds: number;
  suppressionWindowSeconds: number;
  mergeWindowSeconds: number;
  slaMinutes?: number;
  channels: AlertOrchestrationChannel[];
  updatedAt: string;
}

export interface AlertOrchestrationRuleListInput {
  eventType?: AlertOrchestrationEventType;
  enabled?: boolean;
  severity?: AlertSeverity;
  sourceId?: string;
}

export interface AlertOrchestrationRuleListResponse {
  items: AlertOrchestrationRule[];
  total: number;
  filters: AlertOrchestrationRuleListInput;
}

export interface AlertOrchestrationRuleUpsertInput {
  name: string;
  enabled: boolean;
  eventType: AlertOrchestrationEventType;
  severity?: AlertSeverity;
  sourceId?: string;
  dedupeWindowSeconds: number;
  suppressionWindowSeconds: number;
  mergeWindowSeconds: number;
  slaMinutes?: number;
  channels: AlertOrchestrationChannel[];
  updatedAt?: string;
}

export interface AlertOrchestrationExecutionLog {
  id: string;
  tenantId: string;
  ruleId: string;
  eventType: AlertOrchestrationEventType;
  alertId?: string;
  severity?: AlertSeverity;
  sourceId?: string;
  channels: AlertOrchestrationChannel[];
  dispatchMode: AlertOrchestrationDispatchMode;
  conflictRuleIds: string[];
  dedupeHit: boolean;
  suppressed: boolean;
  simulated: boolean;
  escalated: boolean;
  escalationReason?: AlertOrchestrationEscalationReason;
  escalationTargetChannels?: AlertOrchestrationChannel[];
  slaMinutes?: number;
  metadata: Record<string, unknown>;
  createdAt: string;
}

export interface AlertOrchestrationExecutionListInput {
  ruleId?: string;
  eventType?: AlertOrchestrationEventType;
  alertId?: string;
  severity?: AlertSeverity;
  sourceId?: string;
  dedupeHit?: boolean;
  suppressed?: boolean;
  dispatchMode?: AlertOrchestrationDispatchMode;
  hasConflict?: boolean;
  simulated?: boolean;
  escalated?: boolean;
  escalationReason?: AlertOrchestrationEscalationReason;
  from?: string;
  to?: string;
  limit?: number;
}

export interface AlertOrchestrationExecutionListResponse {
  items: AlertOrchestrationExecutionLog[];
  total: number;
  filters: AlertOrchestrationExecutionListInput;
}

export interface AlertOrchestrationSimulateInput {
  ruleId?: string;
  eventType: AlertOrchestrationEventType;
  alertId?: string;
  severity?: AlertSeverity;
  sourceId?: string;
  channels?: AlertOrchestrationChannel[];
  conflictRuleIds?: string[];
  dedupeHit?: boolean;
  suppressed?: boolean;
  metadata?: Record<string, unknown>;
}

export interface AlertOrchestrationSimulationResponse {
  matchedRules: AlertOrchestrationRule[];
  conflictRuleIds: string[];
  executions: AlertOrchestrationExecutionLog[];
}

export type DataResidencyMode = "single_region" | "active_active";
export type ReplicationJobStatus = "pending" | "running" | "succeeded" | "failed" | "cancelled";
export type RuleLifecycleStatus = "draft" | "published" | "deprecated";
export type RuleApprovalDecision = "approved" | "rejected";
export type McpRiskLevel = "low" | "medium" | "high";
export type McpToolDecision = "allow" | "deny" | "require_approval";
export type McpApprovalMode = "single_stage" | "two_stage" | "multi_stage";
export type McpApprovalStage = `stage${number}`;
export type McpApprovalStatus = "pending" | "approved" | "rejected";

export interface RegionDescriptor {
  id: string;
  name: string;
  active: boolean;
  description?: string;
}

export interface ResidencyRegionListResponse {
  items: RegionDescriptor[];
  total: number;
}

export interface TenantResidencyPolicy {
  tenantId: string;
  mode: DataResidencyMode;
  primaryRegion: string;
  replicaRegions: string[];
  allowCrossRegionTransfer: boolean;
  requireTransferApproval: boolean;
  updatedAt: string;
}

export interface ResidencyKmsKeyMapping {
  tenantId: string;
  regionId: string;
  keyProvider: string;
  keyRef: string;
  enabled: boolean;
  updatedAt: string;
}

export interface ResidencyKmsKeyMappingUpsertInput {
  items: Array<{
    regionId: string;
    keyProvider: string;
    keyRef: string;
    enabled: boolean;
  }>;
  updatedAt?: string;
}

export interface ResidencyKmsKeyMappingListResponse {
  items: ResidencyKmsKeyMapping[];
  total: number;
}

export interface ResidencyArchiveRegionPolicy {
  tenantId: string;
  sourceRegion: string;
  archiveRegion: string;
  archiveClass: string;
  enabled: boolean;
  updatedAt: string;
}

export interface ResidencyArchiveRegionPolicyUpsertInput {
  items: Array<{
    sourceRegion: string;
    archiveRegion: string;
    archiveClass: string;
    enabled: boolean;
  }>;
  updatedAt?: string;
}

export interface ResidencyArchiveRegionPolicyListResponse {
  items: ResidencyArchiveRegionPolicy[];
  total: number;
}

export interface ReplicationJob {
  id: string;
  tenantId: string;
  sourceRegion: string;
  targetRegion: string;
  status: ReplicationJobStatus;
  reason?: string;
  createdByUserId?: string;
  approvedByUserId?: string;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
  startedAt?: string;
  finishedAt?: string;
}

export interface ReplicationJobListInput {
  status?: ReplicationJobStatus;
  sourceRegion?: string;
  targetRegion?: string;
  limit?: number;
}

export interface ReplicationJobListResponse {
  items: ReplicationJob[];
  total: number;
  filters: ReplicationJobListInput;
}

export interface ReplicationJobCreateInput {
  sourceRegion: string;
  targetRegion: string;
  reason?: string;
  metadata?: Record<string, unknown>;
}

export interface ReplicationJobCancelInput {
  reason?: string;
}

export interface ReplicationJobApproveInput {
  reason?: string;
}

export interface SystemConfigPackageTargetSelectors {
  agentIds?: string[];
  deviceIds?: string[];
  channels?: string[];
  hostnames?: string[];
}

export type SystemConfigPackageRequiredApprovals = 0 | 1 | 2;
export type SystemConfigPackageApprovalDecision = "approved" | "rejected";

export interface SystemConfigPackage {
  packageId: string;
  tenantId: string;
  version: string;
  issuedAt?: string;
  signatureStatus: string;
  payload: Record<string, unknown>;
  targetSelectors: SystemConfigPackageTargetSelectors;
  requiresApproval: boolean;
  requiredApprovals: SystemConfigPackageRequiredApprovals;
  isPublished: boolean;
  publishedAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface SystemConfigPackageListResponse {
  items: SystemConfigPackage[];
  total: number;
  filters: {
    limit?: number;
  };
}

export interface SystemConfigPackageCreateInput {
  version: string;
  issuedAt?: string;
  signatureStatus?: string;
  requiresApproval: boolean;
  requiredApprovals: SystemConfigPackageRequiredApprovals;
  targetSelectors?: SystemConfigPackageTargetSelectors;
  payload?: Record<string, unknown>;
}

export interface SystemConfigPackageApproval {
  approvalId: string;
  tenantId: string;
  packageId: string;
  version: string;
  approverUserId: string;
  decision: SystemConfigPackageApprovalDecision;
  comment?: string;
  createdAt: string;
  updatedAt: string;
}

export interface SystemConfigPackageApprovalListResponse {
  items: SystemConfigPackageApproval[];
  total: number;
}

export interface SystemConfigPackageApprovalCreateInput {
  decision: SystemConfigPackageApprovalDecision;
  comment?: string;
}

export interface SystemConfigWatchLatestInput {
  agentId?: string;
  deviceId?: string;
  channel?: string;
  hostname?: string;
}

export type SystemConfigWatchLatestResponse = SystemConfigPackage;

export type AgentReleaseChannel = "stable" | "beta" | "canary";

export interface AgentReleaseArtifact {
  os: string;
  arch: string;
  downloadUrl: string;
  checksumSha256?: string;
  signature?: string;
  signatureAlgorithm?: "ed25519";
  rolloutRing?: string;
  rolloutPercentage?: number;
  minAgentVersion?: string;
  fileName?: string;
  installHint?: string;
}

export interface AgentRelease {
  releaseId: string;
  tenantId: string;
  version: string;
  channel: AgentReleaseChannel;
  notes?: string;
  publishedAt: string;
  artifacts: AgentReleaseArtifact[];
  createdAt: string;
  updatedAt: string;
}

export interface AgentReleaseListResponse {
  items: AgentRelease[];
  total: number;
  filters: {
    limit?: number;
    channel?: AgentReleaseChannel;
    os?: string;
    arch?: string;
  };
}

export type AgentReleaseCheckSelectionReason =
  | "matched"
  | "no_candidate"
  | "ring_mismatch"
  | "rollout_percentage_blocked"
  | "min_agent_version_blocked";

export interface AgentReleaseCheckPreviewInput {
  currentVersion: string;
  channel: AgentReleaseChannel;
  os: string;
  arch: string;
  agentId?: string;
  deviceId?: string;
  hostname?: string;
  ring?: string;
}

export interface AgentReleaseCheckPreviewResponse {
  checkedAt: string;
  currentVersion: string;
  channel: AgentReleaseChannel;
  os: string;
  arch: string;
  updateAvailable: boolean;
  comparison: "upgrade_available" | "up_to_date" | "ahead_of_latest" | "no_release";
  latestRelease: AgentRelease | null;
  selectedArtifact: AgentReleaseArtifact | null;
  instructions: string;
  evaluatedRing?: string;
  rolloutBucket?: number;
  selectionReason?: AgentReleaseCheckSelectionReason;
}

export interface AgentReleaseBatchCheckSampleInput {
  label: string;
  currentVersion: string;
  agentId?: string;
  deviceId?: string;
  hostname?: string;
  ring?: string;
}

export interface AgentReleaseCheckBatchPreviewInput {
  channel: AgentReleaseChannel;
  os: string;
  arch: string;
  samples: AgentReleaseBatchCheckSampleInput[];
}

export interface AgentReleaseCheckBatchPreviewResponse {
  items: Array<AgentReleaseCheckPreviewResponse & { label: string }>;
  total: number;
}

export interface RuleScopeBinding {
  organizations?: string[];
  projects?: string[];
  clients?: string[];
}

export type RuleRequiredApprovals = 1 | 2;

export interface RuleAsset {
  id: string;
  tenantId: string;
  name: string;
  description?: string;
  status: RuleLifecycleStatus;
  latestVersion: number;
  publishedVersion?: number;
  requiredApprovals: RuleRequiredApprovals;
  scopeBinding: RuleScopeBinding;
  createdAt: string;
  updatedAt: string;
}

export interface RuleAssetListInput {
  status?: RuleLifecycleStatus;
  keyword?: string;
  limit?: number;
}

export interface RuleAssetListResponse {
  items: RuleAsset[];
  total: number;
  filters: RuleAssetListInput;
}

export interface RuleAssetCreateInput {
  name: string;
  description?: string;
  requiredApprovals?: RuleRequiredApprovals;
  scopeBinding?: RuleScopeBinding;
}

export interface RuleAssetVersion {
  id: string;
  tenantId: string;
  assetId: string;
  version: number;
  content: string;
  changelog?: string;
  createdByUserId?: string;
  createdAt: string;
}

export interface RuleAssetVersionCreateInput {
  content: string;
  changelog?: string;
}

export type RuleVersionDiffLineType = "added" | "removed" | "unchanged";

export interface RuleAssetVersionDiffLine {
  type: RuleVersionDiffLineType;
  content: string;
  oldLineNumber?: number;
  newLineNumber?: number;
}

export interface RuleAssetVersionDiffSummary {
  added: number;
  removed: number;
  unchanged: number;
  changed: boolean;
}

export interface RuleAssetVersionDiffResponse {
  assetId: string;
  fromVersion: number;
  toVersion: number;
  lines: RuleAssetVersionDiffLine[];
  summary: RuleAssetVersionDiffSummary;
}

export interface RuleApproval {
  id: string;
  tenantId: string;
  assetId: string;
  version: number;
  approverUserId: string;
  approverEmail?: string;
  decision: RuleApprovalDecision;
  reason?: string;
  createdAt: string;
}

export interface RuleApprovalListInput {
  version?: number;
  decision?: RuleApprovalDecision;
  limit?: number;
}

export interface RuleApprovalListResponse {
  items: RuleApproval[];
  total: number;
  filters: RuleApprovalListInput;
}

export interface RuleApprovalCreateInput {
  version: number;
  decision: RuleApprovalDecision;
  reason?: string;
}

export interface RulePublishInput {
  version: number;
}

export interface RuleRollbackInput {
  version: number;
  reason?: string;
}

export interface McpToolPolicy {
  tenantId: string;
  toolId: string;
  riskLevel: McpRiskLevel;
  decision: McpToolDecision;
  approvalMode?: McpApprovalMode;
  approvalWorkflow?: McpApprovalWorkflow;
  approvalStages?: McpApprovalStageConfig[];
  stage1RequiredApprovals?: number;
  stage2RequiredApprovals?: number;
  stage1Roles?: string[];
  stage2Roles?: string[];
  approvalCondition?: {
    riskLevelAtLeast?: McpRiskLevel;
    toolIds?: string[];
    tenantRoles?: string[];
  };
  metadata?: Record<string, unknown>;
  reason?: string;
  updatedAt: string;
}

export interface McpToolPolicyListInput {
  riskLevel?: McpRiskLevel;
  decision?: McpToolDecision;
  keyword?: string;
  limit?: number;
}

export interface McpToolPolicyListResponse {
  items: McpToolPolicy[];
  total: number;
  filters: McpToolPolicyListInput;
}

export interface McpToolPolicyUpsertInput {
  riskLevel: McpRiskLevel;
  decision: McpToolDecision;
  approvalMode?: McpApprovalMode;
  approvalWorkflow?: McpApprovalWorkflow;
  approvalStages?: McpApprovalStageConfig[];
  stage1RequiredApprovals?: number;
  stage2RequiredApprovals?: number;
  stage1Roles?: string[];
  stage2Roles?: string[];
  approvalCondition?: McpApprovalWorkflowCondition;
  metadata?: Record<string, unknown>;
  reason?: string;
}

export interface McpApprovalRequest {
  id: string;
  tenantId: string;
  toolId: string;
  status: McpApprovalStatus;
  approvalMode?: McpApprovalMode;
  currentNodeId?: string;
  currentStage?: McpApprovalStage;
  approvalWorkflow?: McpApprovalWorkflow;
  approvalNodes?: McpApprovalWorkflowNodeSnapshot[];
  pathHistory?: string[];
  nextTransitionPreview?: McpApprovalWorkflowTransitionPreview;
  approvalStages?: McpApprovalStageSnapshot[];
  stage1RequiredApprovals?: number;
  stage2RequiredApprovals?: number;
  stage1ApprovedCount?: number;
  stage2ApprovedCount?: number;
  remainingApprovals?: number;
  stage1Roles?: string[];
  stage2Roles?: string[];
  approvalConditionMatched?: boolean;
  requestedByUserId: string;
  requestedByEmail?: string;
  reason?: string;
  reviewedByUserId?: string;
  reviewedByEmail?: string;
  reviewReason?: string;
  createdAt: string;
  updatedAt: string;
}

export interface McpApprovalStageConfig {
  nodeId?: string;
  stage?: McpApprovalStage;
  label?: string;
  requiredApprovals: number;
  roles: string[];
}

export interface McpApprovalStageSnapshot extends McpApprovalStageConfig {
  nodeId?: string;
  label?: string;
  stage: McpApprovalStage;
  approvedApprovals: number;
  approvedByUserIds: string[];
  rejectedByUserId?: string;
}

export type McpApprovalWorkflowNodeKind =
  | "approval"
  | "terminal_approved"
  | "terminal_rejected";

export interface McpApprovalWorkflowTimeWindow {
  timezone: string;
  weekdays?: number[];
  startTime: string;
  endTime: string;
}

export interface McpApprovalWorkflowCondition {
  riskLevelAtLeast?: McpRiskLevel;
  toolIds?: string[];
  tenantRoles?: string[];
  timeWindow?: McpApprovalWorkflowTimeWindow;
  default?: boolean;
}

export interface McpApprovalWorkflowNode {
  nodeId: string;
  kind: McpApprovalWorkflowNodeKind;
  label?: string;
  stage?: McpApprovalStage;
  requiredApprovals?: number;
  roles?: string[];
}

export interface McpApprovalWorkflowTransition {
  fromNodeId: string;
  toNodeId: string;
  condition?: McpApprovalWorkflowCondition;
}

export interface McpApprovalWorkflow {
  entryNodeId: string;
  nodes: McpApprovalWorkflowNode[];
  transitions: McpApprovalWorkflowTransition[];
}

export interface McpApprovalWorkflowNodeSnapshot
  extends McpApprovalWorkflowNode {
  approvedApprovals: number;
  approvedByUserIds: string[];
  rejectedByUserId?: string;
}

export interface McpApprovalWorkflowTransitionPreview {
  fromNodeId: string;
  toNodeId?: string;
  matched: boolean;
  matchedBy?: "condition" | "default";
  condition?: McpApprovalWorkflowCondition;
}

export interface McpApprovalConfig {
  mode: McpApprovalMode;
  approvalWorkflow?: McpApprovalWorkflow;
  approvalStages?: McpApprovalStageConfig[];
  stage1?: McpApprovalStageConfig;
  stage2?: McpApprovalStageConfig;
}

export interface McpApprovalListInput {
  status?: McpApprovalStatus;
  limit?: number;
}

export interface McpApprovalListResponse {
  items: McpApprovalRequest[];
  total: number;
  filters: McpApprovalListInput;
}

export interface McpApprovalCreateInput {
  toolId: string;
  reason?: string;
  approvalConfig?: McpApprovalConfig;
}

export interface McpApprovalReviewInput {
  reason?: string;
  stage?: McpApprovalStage;
  nodeId?: string;
}

export interface McpInvocationAudit {
  id: string;
  tenantId: string;
  toolId: string;
  decision: McpToolDecision;
  result: "allowed" | "blocked" | "approved";
  approvalRequestId?: string;
  enforced: boolean;
  evaluatedDecision?: McpToolDecision;
  approvalMode?: McpApprovalMode;
  approvalStage?: McpApprovalStage;
  approvalSatisfied?: boolean;
  approvalConditionMatched?: boolean;
  metadata: Record<string, unknown>;
  createdAt: string;
}

export interface McpInvocationListInput {
  toolId?: string;
  decision?: McpToolDecision;
  from?: string;
  to?: string;
  limit?: number;
}

export interface McpInvocationListResponse {
  items: McpInvocationAudit[];
  total: number;
  filters: McpInvocationListInput;
}

export interface McpInvocationCreateInput {
  toolId: string;
  decision?: McpToolDecision;
  result?: "allowed" | "blocked" | "approved";
  approvalRequestId?: string;
  enforced?: boolean;
  evaluatedDecision?: McpToolDecision;
  metadata?: Record<string, unknown>;
}

export interface McpEvaluateInput {
  toolId: string;
  reason?: string;
  approvalRequestId?: string;
  evaluationTimestamp?: string;
  approvalConfig?: McpApprovalConfig;
  metadata?: Record<string, unknown>;
}

export interface McpEvaluateResult {
  toolId: string;
  decision: McpToolDecision;
  result: "allowed" | "blocked" | "approved";
  approvalRequestId?: string;
  approvalRequired?: boolean;
  approvalMode?: McpApprovalMode;
  currentNodeId?: string;
  currentStage?: McpApprovalStage;
  approvalWorkflow?: McpApprovalWorkflow;
  approvalNodes?: McpApprovalWorkflowNodeSnapshot[];
  pathHistory?: string[];
  nextTransitionPreview?: McpApprovalWorkflowTransitionPreview;
  approvalStages?: McpApprovalStageSnapshot[];
  remainingApprovals?: number;
  approvalConditionMatched?: boolean;
  enforced: true;
  evaluatedDecision: McpToolDecision;
  policy: McpToolPolicy;
  invocation: McpInvocationAudit;
  evaluatedAt: string;
}

export type OpenPlatformApiKeyStatus = "active" | "disabled";
export type OpenPlatformReplayRunStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "cancelled";
export type OpenPlatformReplayJobStatus = OpenPlatformReplayRunStatus;
export type OpenPlatformQualityDailyStatus = "pass" | "warn" | "fail";
export type OpenPlatformReplayDiffVerdict = "improved" | "regressed" | "unchanged";

export interface OpenPlatformOpenApiTagSummary {
  tag: string;
  operations: number;
}

export interface OpenPlatformOpenApiSummary {
  version: string;
  totalPaths: number;
  totalOperations: number;
  generatedAt: string;
  tags: OpenPlatformOpenApiTagSummary[];
}

export interface OpenPlatformApiKey {
  id: string;
  tenantId: string;
  name: string;
  maskedKey: string;
  status: OpenPlatformApiKeyStatus;
  scopes: string[];
  expiresAt?: string;
  lastUsedAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface OpenPlatformApiKeyListInput {
  status?: OpenPlatformApiKeyStatus;
  keyword?: string;
  limit?: number;
}

export interface OpenPlatformApiKeyListResponse {
  items: OpenPlatformApiKey[];
  total: number;
  filters: OpenPlatformApiKeyListInput;
}

export interface OpenPlatformApiKeyUpsertInput {
  name: string;
  scopes: string[];
  enabled: boolean;
  expiresAt?: string;
  note?: string;
}

export interface OpenPlatformWebhook {
  id: string;
  tenantId: string;
  name: string;
  url: string;
  events: string[];
  enabled: boolean;
  lastDeliveryAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface OpenPlatformWebhookListInput {
  enabled?: boolean;
  keyword?: string;
  limit?: number;
}

export interface OpenPlatformWebhookListResponse {
  items: OpenPlatformWebhook[];
  total: number;
  filters: OpenPlatformWebhookListInput;
}

export interface OpenPlatformWebhookUpsertInput {
  name: string;
  url: string;
  events: string[];
  enabled: boolean;
  secret?: string;
}

export interface OpenPlatformWebhookReplayInput {
  eventType?: string;
  from?: string;
  to?: string;
  limit?: number;
  dryRun?: boolean;
}

export interface OpenPlatformWebhookReplayResult {
  id: string;
  webhookId: string;
  status: string;
  dryRun: boolean;
  filters: {
    eventType?: string;
    from?: string;
    to?: string;
    limit?: number;
  };
  requestedAt: string;
}

export interface OpenPlatformQualityDailyQueryInput {
  date?: string;
  from?: string;
  to?: string;
  metric?: string;
  provider?: string;
  repo?: string;
  workflow?: string;
  runId?: string;
  groupBy?: "provider" | "repo" | "workflow" | "runId";
  limit?: number;
}

export interface OpenPlatformQualityDailyItem {
  date: string;
  metric: string;
  value: number;
  target: number;
  score: number;
  status: OpenPlatformQualityDailyStatus;
}

export interface OpenPlatformQualityDailyResponse {
  items: OpenPlatformQualityDailyItem[];
  total: number;
  groups?: Array<{
    groupBy: "provider" | "repo" | "workflow" | "runId";
    value: string;
    totalEvents: number;
    passedEvents: number;
    failedEvents: number;
    passRate: number;
    avgScore: number;
  }>;
  filters: OpenPlatformQualityDailyQueryInput;
}

export interface OpenPlatformQualityScorecardListInput {
  team?: string;
  owner?: string;
  limit?: number;
}

export interface OpenPlatformQualityScorecard {
  id: string;
  team: string;
  owner: string;
  overallScore: number;
  publishedAt: string;
  highlights: string[];
}

export interface OpenPlatformQualityScorecardListResponse {
  items: OpenPlatformQualityScorecard[];
  total: number;
  filters: OpenPlatformQualityScorecardListInput;
}

export interface OpenPlatformQualityProjectTrendQueryInput {
  from?: string;
  to?: string;
  metric?: string;
  provider?: string;
  workflow?: string;
  includeUnknown?: boolean;
  limit?: number;
}

export interface OpenPlatformQualityProjectTrendItem {
  project: string;
  metric: string;
  totalEvents: number;
  passedEvents: number;
  failedEvents: number;
  passRate: number;
  avgScore: number;
  totalCost: number;
  totalTokens: number;
  totalSessions: number;
  costPerQualityPoint: number;
}

export interface OpenPlatformQualityProjectTrendSummary {
  metric: string;
  totalEvents: number;
  passedEvents: number;
  failedEvents: number;
  passRate: number;
  avgScore: number;
  totalCost: number;
  totalTokens: number;
  totalSessions: number;
  from?: string;
  to?: string;
}

export interface OpenPlatformQualityProjectTrendResponse {
  items: OpenPlatformQualityProjectTrendItem[];
  total: number;
  summary: OpenPlatformQualityProjectTrendSummary;
  filters: Omit<OpenPlatformQualityProjectTrendQueryInput, "provider" | "workflow"> & {
    metric?: string;
    provider?: string | null;
    workflow?: string | null;
    includeUnknown?: boolean;
  };
}

export interface OpenPlatformAutomationPolicy {
  tenantId: string;
  toolId: string;
  scope: "quality_replay_advice";
  riskLevel: McpRiskLevel;
  decision: McpToolDecision;
  reason?: string;
  evaluationScoreThreshold: number;
  triggerOnEvaluationFailure: boolean;
  triggerOnReplayRegression: boolean;
  defaultActionType?: "scorecard_adjustment" | "replay_experiment";
  modelVersion?: string;
  strategyMatrix?: OpenPlatformAutomationStrategyRule[];
  updatedAt: string;
}

export interface OpenPlatformAutomationPolicyUpsertInput {
  riskLevel: McpRiskLevel;
  decision: McpToolDecision;
  reason?: string;
  evaluationScoreThreshold?: number;
  triggerOnEvaluationFailure?: boolean;
  triggerOnReplayRegression?: boolean;
  modelVersion?: string;
  strategyMatrix?: OpenPlatformAutomationStrategyRule[];
}

export interface OpenPlatformAutomationStrategyRule {
  id: string;
  metric?: string;
  severity?: "info" | "warn" | "critical";
  trendDirection?: "up" | "down" | "flat";
  provider?: string;
  workflow?: string;
  projectPattern?: string;
  minSampleCount?: number;
  minPassRate?: number;
  minConfidence?: number;
  regressionProbabilityAtLeast?: number;
  replayRegressionAtLeast?: number;
  actionType: "scorecard_adjustment" | "replay_experiment";
  requiresApproval: boolean;
  cooldownMinutes?: number;
  reason?: string;
}

export interface OpenPlatformAutomationPolicySimulationInput {
  metric: string;
  score: number;
  sampleCount?: number;
  provider?: string;
  workflow?: string;
  project?: string;
  trendDirection?: "up" | "down" | "flat";
  confidence?: number;
  regressionProbability?: number;
  replayRegressionCount?: number;
}

export interface OpenPlatformAutomationPolicySimulationResponse {
  metric: string;
  severity: "info" | "warn" | "critical";
  confidence: number;
  trendDirection: "up" | "down" | "flat";
  regressionProbability: number;
  replayRegressionCount: number;
  matchedRuleId?: string | null;
  resolvedAction: "scorecard_adjustment" | "replay_experiment";
  requiresApproval: boolean;
  blockingReasons: string[];
}

export interface OpenPlatformQualityForecastItem {
  project: string;
  metric: string;
  predictedScore: number;
  confidence: number;
  confidenceLabel?: "low" | "medium" | "high";
  modelVersion?: string;
  forecastHorizonDays?: number;
  expectedScoreRange?: {
    lower: number;
    upper: number;
  };
  regressionProbability?: number;
  featureContributions?: Array<{
    feature: string;
    impact: number;
    direction: "positive" | "negative" | "neutral";
  }>;
  windowComparisons?: {
    currentWindow: Record<string, unknown>;
    previousWindow: Record<string, unknown>;
  };
  trendDirection?: "up" | "down" | "flat";
  projectedDelta?: number;
  basisWindowCount?: number;
  rationale?: string;
  explanation?: {
    summary: string;
    confidenceLabel: "low" | "medium" | "high";
    primaryDriver: string;
  };
  riskDrivers?: string[];
  recommendedActions?: string[];
  windowStart?: string | null;
  windowEnd?: string | null;
  basis: Record<string, unknown>;
}

export interface OpenPlatformQualityForecastResponse {
  items: OpenPlatformQualityForecastItem[];
  total: number;
  filters: Record<string, unknown>;
}

export interface OpenPlatformQualityAdviceItem {
  id: string;
  project: string;
  severity: "info" | "warn" | "critical";
  title: string;
  recommendation: string;
  explanation?: string;
  confidence?: number;
  confidenceLabel?: "low" | "medium" | "high";
  why?: string[];
  automationReadiness?:
    | "monitor_only"
    | "manual_review"
    | "ready_for_execution"
    | "execution_in_progress";
  executionHint?: {
    recommendedActionType: "scorecard_adjustment" | "replay_experiment";
    requiresDataset: boolean;
    priority: "low" | "medium" | "high";
    reason: string;
  };
  executionOptions?: Array<{
    actionType: "scorecard_adjustment" | "replay_experiment";
    availability: "recommended" | "available" | "approval_required" | "disabled";
    reason: string;
  }>;
  strategyMatrixMatch?: string | null;
  recommendedPlan?: Record<string, unknown>;
  autoExecutionDecision?: string;
  blockingReasons?: string[];
  basis: Record<string, unknown>;
  relatedMetrics: string[];
  suggestedActions?: Array<"scorecard_adjustment" | "replay_experiment">;
  latestExecutionId?: string;
  latestExecutionStatus?: "pending" | "running" | "completed" | "failed" | "cancelled";
}

export interface OpenPlatformQualityAdviceResponse {
  items: OpenPlatformQualityAdviceItem[];
  total: number;
  filters: Record<string, unknown>;
}

export interface OpenPlatformQualityAdviceExecution {
  id: string;
  tenantId: string;
  adviceId: string;
  project: string;
  severity: "info" | "warn" | "critical";
  actionType: "scorecard_adjustment" | "replay_experiment";
  triggerSource: "manual" | "automatic";
  status: "pending" | "running" | "completed" | "failed" | "cancelled";
  metric?: string;
  datasetId?: string;
  experimentId?: string;
  candidateLabels?: string[];
  scorecardKey?: string;
  resultSummary?: Record<string, unknown>;
  error?: string;
  requestedAt: string;
  startedAt?: string;
  finishedAt?: string;
  updatedAt: string;
}

export interface OpenPlatformQualityAdviceExecutionListResponse {
  items: OpenPlatformQualityAdviceExecution[];
  total: number;
  filters: Record<string, unknown>;
}

export interface OpenPlatformReplayDataset {
  id: string;
  name: string;
  model: string;
  datasetId: string;
  datasetRef?: string;
  promptVersion?: string;
  sampleCount?: number;
  caseCount?: number;
  currentVersionId?: string;
  currentVersionNumber?: number;
  description?: string;
  metadata?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}
export type OpenPlatformReplayBaseline = OpenPlatformReplayDataset;

export interface OpenPlatformReplayDatasetListInput {
  keyword?: string;
  limit?: number;
}
export type OpenPlatformReplayBaselineListInput = OpenPlatformReplayDatasetListInput;

export interface OpenPlatformReplayDatasetListResponse {
  items: OpenPlatformReplayDataset[];
  total: number;
  filters: OpenPlatformReplayDatasetListInput;
}
export type OpenPlatformReplayBaselineListResponse = OpenPlatformReplayDatasetListResponse;

export interface OpenPlatformReplayDatasetCreateInput {
  name: string;
  datasetId?: string;
  datasetRef?: string;
  model: string;
  promptVersion?: string;
  sampleCount?: number;
  metadata?: Record<string, unknown>;
}
export type OpenPlatformReplayBaselineCreateInput = OpenPlatformReplayDatasetCreateInput;

export interface OpenPlatformReplayDatasetVersion {
  id: string;
  tenantId?: string;
  datasetId: string;
  version: number;
  datasetRef?: string;
  model: string;
  promptVersion?: string;
  sampleCount: number;
  metadata?: Record<string, unknown>;
  note?: string;
  createdAt: string;
  promotedAt?: string | null;
}

export interface OpenPlatformReplayDatasetVersionListResponse {
  datasetId: string;
  items: OpenPlatformReplayDatasetVersion[];
  total: number;
  currentVersionId?: string | null;
  currentVersionNumber?: number | null;
}

export interface OpenPlatformReplayDatasetVersionCreateInput {
  datasetId?: string;
  datasetRef?: string;
  model: string;
  promptVersion?: string;
  sampleCount?: number;
  metadata?: Record<string, unknown>;
  note?: string;
}

export interface OpenPlatformReplayDatasetVersionPromoteInput {
  versionId: string;
}

export interface OpenPlatformReplayDatasetVersionPromoteResponse {
  dataset?: OpenPlatformReplayDataset | null;
  version: OpenPlatformReplayDatasetVersion;
}

export interface OpenPlatformReplayDatasetCase {
  datasetId: string;
  caseId: string;
  sortOrder: number;
  input: string;
  expectedOutput?: string;
  baselineOutput?: string;
  candidateInput?: string;
  metadata: Record<string, unknown>;
  checksum?: string;
  createdAt?: string;
  updatedAt?: string;
}

export interface OpenPlatformReplayDatasetCaseWriteInput {
  caseId?: string;
  sortOrder?: number;
  input: string;
  expectedOutput?: string;
  baselineOutput?: string;
  candidateInput?: string;
  metadata?: Record<string, unknown>;
}

export interface OpenPlatformReplayDatasetCaseListResponse {
  datasetId: string;
  items: OpenPlatformReplayDatasetCase[];
  total: number;
}

export interface OpenPlatformReplayDatasetVersionCaseListResponse {
  datasetId: string;
  versionId: string;
  items: OpenPlatformReplayDatasetCase[];
  total: number;
}

export interface OpenPlatformReplayDatasetCaseReplaceInput {
  items: OpenPlatformReplayDatasetCaseWriteInput[];
}

export interface OpenPlatformReplayDatasetMaterializeFilters {
  sourceId?: string;
  keyword?: string;
  clientType?: string;
  tool?: string;
  host?: string;
  model?: string;
  project?: string;
  from?: string;
  to?: string;
}

export interface OpenPlatformReplayDatasetMaterializeInput {
  sessionIds?: string[];
  filters?: OpenPlatformReplayDatasetMaterializeFilters;
  sampleLimit?: number;
  sanitized?: boolean;
  snapshotVersion?: string;
}

export interface OpenPlatformReplayDatasetMaterializeResponse {
  datasetId: string;
  sourceType: "session";
  materialized: number;
  skipped: number;
  sourceSummary?: Record<string, number>;
  items: OpenPlatformReplayDatasetCase[];
  total: number;
  filters: {
    datasetId: string;
    sessionIds?: string[];
    filters?: OpenPlatformReplayDatasetMaterializeFilters;
    sampleLimit?: number;
    sanitized?: boolean;
    snapshotVersion?: string;
  };
}

export interface OpenPlatformReplayRun {
  id: string;
  runId: string;
  jobId?: string;
  datasetId: string;
  baselineId?: string;
  baselineVersionId?: string | null;
  candidateLabel: string;
  status: OpenPlatformReplayRunStatus;
  totalCases: number;
  processedCases: number;
  improvedCases: number;
  regressedCases: number;
  unchangedCases: number;
  passedCases: number;
  failedCases: number;
  summary?: Record<string, unknown>;
  createdAt: string;
  updatedAt?: string;
  finishedAt?: string;
}
export type OpenPlatformReplayJob = OpenPlatformReplayRun;

export interface OpenPlatformReplayRunListInput {
  datasetId?: string;
  baselineId?: string;
  status?: OpenPlatformReplayRunStatus;
  limit?: number;
}
export type OpenPlatformReplayJobListInput = OpenPlatformReplayRunListInput;

export interface OpenPlatformReplayRunListResponse {
  items: OpenPlatformReplayRun[];
  total: number;
  filters: OpenPlatformReplayRunListInput;
}
export type OpenPlatformReplayJobListResponse = OpenPlatformReplayRunListResponse;

export interface OpenPlatformReplayExperiment {
  id: string;
  tenantId: string;
  name: string;
  datasetId: string;
  baselineId?: string | null;
  baselineVersionId?: string | null;
  metadata?: Record<string, unknown>;
  status?: "draft" | "queued" | "running" | "completed" | "failed" | "cancelled";
  triggerSource?: "manual" | "quality_advice" | "automatic";
  executionMode?: "manual" | "automatic";
  candidateLabels?: string[];
  sourceAdviceId?: string | null;
  runIds: string[];
  runStatusSummary?: Record<string, unknown>;
  aggregateSummary?: Record<string, unknown>;
  summary: Record<string, unknown>;
  runs: OpenPlatformReplayRun[];
  createdAt: string;
  updatedAt: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  lastError?: string | null;
}

export interface OpenPlatformReplayExperimentListResponse {
  items: OpenPlatformReplayExperiment[];
  total: number;
}

export interface OpenPlatformReplayExperimentComparisonItem {
  runId: string;
  candidateLabel: string;
  status: "pending" | "running" | "completed" | "failed" | "cancelled";
  totalCases: number;
  processedCases: number;
  improvedCases: number;
  regressedCases: number;
  unchangedCases: number;
  improvementRate: number;
  regressionRate: number;
  netDelta: number;
  verdict: "improved" | "regressed" | "mixed";
  rank: number;
  topDiffs: Array<Record<string, unknown>>;
}

export interface OpenPlatformReplayExperimentComparison {
  experimentId: string;
  datasetId: string;
  comparedAt: string;
  summary: {
    totalRuns: number;
    completedRuns: number;
    failedRuns: number;
    totalImprovedCases: number;
    totalRegressedCases: number;
    bestCandidateLabel?: string | null;
    bestRunId?: string | null;
    bestNetDelta?: number | null;
  };
  items: OpenPlatformReplayExperimentComparisonItem[];
}

export interface OpenPlatformReplayExperimentWorkflowStep {
  stepId: string;
  label: string;
  category: "lifecycle" | "run";
  status: "pending" | "queued" | "running" | "completed" | "failed" | "cancelled";
  startedAt: string;
  finishedAt?: string | null;
  detail: string;
}

export interface OpenPlatformReplayExperimentWorkflow {
  experimentId: string;
  datasetId: string;
  status: "queued" | "running" | "completed" | "failed" | "cancelled";
  summary: {
    totalSteps: number;
    completedSteps: number;
    runningSteps: number;
    failedSteps: number;
    cancelledSteps: number;
    queuedSteps: number;
  };
  steps: OpenPlatformReplayExperimentWorkflowStep[];
}

export interface OpenPlatformReplayExperimentCompareItem {
  runId: string;
  candidateLabel: string;
  status: OpenPlatformReplayRunStatus;
  totalCases: number;
  processedCases: number;
  improvedCases: number;
  regressedCases: number;
  unchangedCases: number;
  passRate: number;
  improvementRate: number;
  regressionRate: number;
  netDelta: number;
  startedAt?: string | null;
  finishedAt?: string | null;
}

export interface OpenPlatformReplayExperimentCompareResponse {
  experimentId: string;
  datasetId: string;
  items: OpenPlatformReplayExperimentCompareItem[];
  total: number;
  summary: {
    totalRuns: number;
    completedRuns: number;
    failedRuns: number;
    runningRuns: number;
    queuedRuns: number;
    cancelledRuns: number;
    bestRunId?: string | null;
    worstRunId?: string | null;
    bestNetDelta?: number;
    worstNetDelta?: number;
  };
}

export interface OpenPlatformReplayExperimentBatchCompareItem {
  experimentId: string;
  name: string;
  datasetId: string;
  status: OpenPlatformReplayExperiment["status"];
  workflowStage: "draft" | "queued" | "running" | "completed" | "failed" | "cancelled";
  triggerSource: NonNullable<OpenPlatformReplayExperiment["triggerSource"]>;
  sourceAdviceId?: string | null;
  candidateLabels: string[];
  totalRuns: number;
  completedRuns: number;
  failedRuns: number;
  runningRuns: number;
  queuedRuns: number;
  totalCases: number;
  processedCases: number;
  improvedCases: number;
  regressedCases: number;
  improvementRate: number;
  regressionRate: number;
  netDelta: number;
  bestRunId?: string | null;
  worstRunId?: string | null;
  runs: OpenPlatformReplayExperimentCompareItem[];
  updatedAt: string;
}

export interface OpenPlatformReplayExperimentBatchCompareResponse {
  items: OpenPlatformReplayExperimentBatchCompareItem[];
  total: number;
  summary: {
    comparedExperimentCount: number;
    comparedAt: string;
    datasets: string[];
    totalRuns: number;
    completedRuns: number;
    failedRuns: number;
    runningRuns: number;
    queuedRuns: number;
    totalCases: number;
    processedCases: number;
    improvedCases: number;
    regressedCases: number;
    bestExperimentId?: string | null;
    worstExperimentId?: string | null;
  };
  filters: {
    experimentIds: string[];
    datasetId?: string | null;
  };
}

export interface OpenPlatformReplayExperimentWorkflowNode {
  id: string;
  type: "experiment" | "run";
  label: string;
  status: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  metadata?: Record<string, unknown>;
}

export interface OpenPlatformReplayExperimentWorkflowEdge {
  from: string;
  to: string;
  label: string;
}

export interface OpenPlatformReplayExperimentWorkflowResponse {
  experimentId: string;
  status: string;
  nodes: OpenPlatformReplayExperimentWorkflowNode[];
  edges: OpenPlatformReplayExperimentWorkflowEdge[];
  summary: {
    totalNodes: number;
    totalRuns: number;
    queuedRuns: number;
    runningRuns: number;
    completedRuns: number;
    failedRuns: number;
    cancelledRuns: number;
  };
}

export interface OpenPlatformReplayRunCreateInput {
  datasetId?: string;
  baselineId?: string;
  baselineVersionId?: string;
  candidateLabel: string;
  from?: string;
  to?: string;
  sampleLimit?: number;
  metadata?: Record<string, unknown>;
}
export type OpenPlatformReplayJobCreateInput = OpenPlatformReplayRunCreateInput;

export interface OpenPlatformReplayDiffQueryInput {
  datasetId?: string;
  baselineId?: string;
  runId?: string;
  jobId?: string;
  keyword?: string;
  limit?: number;
}

export interface OpenPlatformReplayDiffItem {
  id: string;
  datasetId: string;
  baselineId?: string;
  runId: string;
  jobId?: string;
  caseId: string;
  summary: string;
  verdict: OpenPlatformReplayDiffVerdict;
  deltaScore: number;
}

export interface OpenPlatformReplayDiffResponse {
  items: OpenPlatformReplayDiffItem[];
  total: number;
  summary?: Record<string, unknown>;
  filters: OpenPlatformReplayDiffQueryInput;
}

export interface OpenPlatformReplayArtifact {
  runId?: string;
  type: string;
  contentType: string;
  name?: string;
  description?: string;
  byteSize?: number;
  downloadName?: string;
  downloadUrl?: string;
  checksum?: string;
  storageBackend?: "local" | "object" | "hybrid";
  storageKey?: string;
  metadata?: Record<string, unknown>;
  createdAt?: string;
  inline?: Record<string, unknown>;
}

export interface OpenPlatformReplayArtifactListResponse {
  runId: string;
  jobId?: string;
  datasetId?: string;
  items: OpenPlatformReplayArtifact[];
  total: number;
}

export interface OpenPlatformReplayExperimentArtifactListResponse {
  experimentId: string;
  datasetId: string;
  items: OpenPlatformReplayArtifact[];
  total: number;
}

export interface SessionEvent {
  id: string;
  sessionId: string;
  sourceId: string;
  eventType: string;
  role?: string;
  text?: string;
  model?: string;
  timestamp: string;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  reasoningTokens: number;
  cost: number;
  sourcePath?: string;
  sourceOffset?: number;
}

export interface SessionEventListResponse {
  items: SessionEvent[];
  total: number;
  limit: number;
  nextCursor?: string | null;
}

export interface UsageAggregateFilters {
  from?: string;
  to?: string;
  limit?: number;
}

export interface UsageAggregateResponse<TItem> {
  items: TItem[];
  total: number;
  filters?: UsageAggregateFilters;
}

export interface UsageWeekItem {
  weekStart: string;
  weekEnd: string;
  tokens: number;
  cost: number;
  sessions: number;
}

export interface UsageWeeklySummaryQueryInput extends UsageAggregateFilters {
  metric?: MetricKey;
  timezone?: string;
}

export interface UsageWeeklySummaryResponse {
  metric: MetricKey;
  timezone: string;
  weeks: UsageWeekItem[];
  summary: {
    tokens: number;
    cost: number;
    sessions: number;
  };
  peakWeek?: UsageWeekItem;
}

export type ExportFormat = "json" | "csv";
export type AuditDlpMode = "off" | "redact" | "block";
export type UsageExportDimension =
  | "daily"
  | "weekly"
  | "monthly"
  | "models"
  | "sessions"
  | "heatmap";

export interface UsageExportQueryInput {
  dimension: UsageExportDimension;
  from?: string;
  to?: string;
  limit?: number;
  timezone?: string;
}

export interface DownloadFile {
  blob: Blob;
  filename: string;
  contentType: string;
}

export type UsageCostMode = "raw" | "estimated" | "reported" | "mixed" | "none";

export interface UsageCostMetrics {
  costRaw?: number;
  costEstimated?: number;
  costMode?: UsageCostMode;
  rawCost?: number;
  estimatedCost?: number;
  totalCost?: number;
  costLabel?: string;
  costBasis?: string;
}

export interface UsageDailyItem extends UsageCostMetrics {
  date: string;
  tokens: number;
  cost: number;
  sessions: number;
}

export interface UsageMonthlyItem extends UsageCostMetrics {
  month: string;
  tokens: number;
  cost: number;
  sessions: number;
}

export interface UsageModelItem extends UsageCostMetrics {
  model: string;
  tokens: number;
  cost: number;
  sessions: number;
}

export interface UsageSessionBreakdownItem extends UsageCostMetrics {
  sessionId: string;
  sourceId: string;
  tool: string;
  model: string;
  startedAt: string;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  reasoningTokens: number;
  totalTokens: number;
  cost: number;
}

export interface PricingCatalogEntry {
  model: string;
  inputPer1k: number;
  outputPer1k: number;
  cacheReadPer1k?: number;
  cacheWritePer1k?: number;
  reasoningPer1k?: number;
  currency?: string;
}

export interface PricingCatalogVersion {
  id: string;
  tenantId: string;
  version: number;
  note?: string;
  createdAt: string;
}

export interface PricingCatalog {
  version: PricingCatalogVersion;
  entries: PricingCatalogEntry[];
}

export interface PricingCatalogUpsertInput {
  note?: string;
  entries: PricingCatalogEntry[];
}

export interface SourceConnectionTestResponse {
  sourceId: string;
  success: boolean;
  mode: SourceType;
  latencyMs: number;
  detail: string;
}

export interface AuthLoginInput {
  email: string;
  password: string;
  otpCode?: string;
}

export type AuthProviderType = "local" | "oauth2" | "oidc" | "sso" | "saml";

export interface AuthProviderItem {
  id: string;
  type: AuthProviderType;
  displayName: string;
  enabled: boolean;
  issuer?: string;
  authorizationUrl?: string;
  requireMfa?: boolean;
}

export interface AuthProviderListResponse {
  items: AuthProviderItem[];
  total: number;
}

export interface AuthExternalExchangeInput {
  providerId: string;
  code: string;
  redirectUri: string;
  codeVerifier?: string;
  state?: string;
  mfaVerified?: boolean;
}

export interface AuthRefreshInput {
  refreshToken: string;
}

export interface AuthUserProfile {
  userId: string;
  email: string;
  displayName: string;
  tenantId?: string;
  tenantRole?: string;
}

export interface AuthSessionInfo {
  sessionId: string;
  issuedAt: string;
  expiresAt: string;
}

export interface AuthTokens {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
  tokenType: "Bearer";
}

export interface AuthLoginResponse {
  user: AuthUserProfile;
  tokens: AuthTokens;
}

export interface AuthRefreshResponse {
  tokens: AuthTokens;
  session?: AuthSessionInfo;
}
