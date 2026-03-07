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
export type AlertOrchestrationChannel =
  | "webhook"
  | "wecom"
  | "dingtalk"
  | "feishu"
  | "email"
  | "email_webhook"
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
  reason?: string;
}

export interface McpApprovalRequest {
  id: string;
  tenantId: string;
  toolId: string;
  status: McpApprovalStatus;
  requestedByUserId: string;
  requestedByEmail?: string;
  reason?: string;
  reviewedByUserId?: string;
  reviewedByEmail?: string;
  reviewReason?: string;
  createdAt: string;
  updatedAt: string;
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
}

export interface McpApprovalReviewInput {
  reason?: string;
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
  metadata?: Record<string, unknown>;
}

export interface McpEvaluateResult {
  toolId: string;
  decision: McpToolDecision;
  result: "allowed" | "blocked" | "approved";
  approvalRequestId?: string;
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

export interface OpenPlatformReplayDataset {
  id: string;
  name: string;
  model: string;
  datasetId: string;
  datasetRef?: string;
  promptVersion?: string;
  sampleCount?: number;
  caseCount?: number;
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

export interface OpenPlatformReplayRunCreateInput {
  datasetId?: string;
  baselineId?: string;
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
  type: string;
  contentType: string;
  name?: string;
  description?: string;
  byteSize?: number;
  downloadName?: string;
  downloadUrl?: string;
  createdAt?: string;
  inline?: Record<string, unknown>;
}

export interface OpenPlatformReplayArtifactListResponse {
  runId: string;
  jobId?: string;
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
}

export type AuthProviderType = "local" | "oauth2" | "oidc" | "sso";

export interface AuthProviderItem {
  id: string;
  type: AuthProviderType;
  displayName: string;
  enabled: boolean;
  issuer?: string;
  authorizationUrl?: string;
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
