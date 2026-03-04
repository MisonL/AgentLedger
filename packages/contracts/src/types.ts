export type SourceType = "local" | "ssh" | "sync-cache";
export type SourceAccessMode = "realtime" | "sync" | "hybrid";
export type SSHAuthType = "key" | "agent";

export interface SSHConfig {
  host: string;
  port: number;
  user: string;
  authType: SSHAuthType;
  keyPath?: string;
  knownHostsPath?: string;
}

export interface Source {
  id: string;
  name: string;
  type: SourceType;
  location: string;
  sshConfig?: SSHConfig;
  accessMode: SourceAccessMode;
  syncCron?: string;
  syncRetentionDays?: number;
  enabled: boolean;
  createdAt: string;
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

export interface SessionDetailResponse {
  session: SessionDetail;
  tokenBreakdown: SessionTokenBreakdown;
  sourceTrace: SessionSourceTrace;
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

export type UsageHeatmapMetric = "tokens" | "cost" | "sessions";

export interface UsageWeekItem {
  weekStart: string;
  weekEnd: string;
  tokens: number;
  cost: number;
  sessions: number;
}

export interface UsageWeeklySummaryResponse {
  metric: UsageHeatmapMetric;
  timezone: string;
  weeks: UsageWeekItem[];
  summary: {
    tokens: number;
    cost: number;
    sessions: number;
  };
  peakWeek?: UsageWeekItem;
}

export type BudgetPeriod = "daily" | "monthly";
export type BudgetScope = "global" | "source" | "org" | "user" | "model";
export type BudgetGovernanceState = "active" | "frozen" | "pending_release";
export type AlertStatus = "open" | "acknowledged" | "resolved";
export type AlertMutableStatus = "acknowledged" | "resolved";
export type AlertSeverity = "warning" | "critical";
export type AlertOrchestrationEventType = "alert" | "weekly";
export type AlertOrchestrationChannel =
  | "webhook"
  | "wecom"
  | "dingtalk"
  | "feishu"
  | "email"
  | "email_webhook";
export type DataResidencyMode = "single_region" | "active_active";
export type ReplicationJobStatus = "pending" | "running" | "succeeded" | "failed" | "cancelled";
export type RuleLifecycleStatus = "draft" | "published" | "deprecated";
export type RuleApprovalDecision = "approved" | "rejected";
export type McpRiskLevel = "low" | "medium" | "high";
export type McpToolDecision = "allow" | "deny" | "require_approval";
export type McpApprovalStatus = "pending" | "approved" | "rejected";
export type AuditLevel = "info" | "warning" | "error" | "critical";

export interface BudgetThresholds {
  warning: number;
  escalated: number;
  critical: number;
}

export interface Budget {
  id: string;
  scope: BudgetScope;
  sourceId?: string;
  organizationId?: string;
  userId?: string;
  model?: string;
  period: BudgetPeriod;
  tokenLimit: number;
  costLimit: number;
  thresholds: BudgetThresholds;
  alertThreshold: number;
  enabled: boolean;
  governanceState: BudgetGovernanceState;
  freezeReason?: string;
  frozenAt?: string;
  frozenByAlertId?: string;
  updatedAt: string;
}

export interface Alert {
  id: string;
  tenantId: string;
  budgetId: string;
  sourceId?: string;
  period: BudgetPeriod;
  windowStart: string;
  windowEnd: string;
  tokensUsed: number;
  costUsed: number;
  tokenLimit: number;
  costLimit: number;
  threshold: number;
  status: AlertStatus;
  severity: AlertSeverity;
  triggeredAt: string;
  updatedAt: string;
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

export interface AlertOrchestrationRuleUpsertInput {
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

export interface RegionDescriptor {
  id: string;
  name: string;
  active: boolean;
  description?: string;
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

export interface TenantResidencyPolicyUpsertInput {
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

export interface ReplicationJobCreateInput {
  tenantId: string;
  sourceRegion: string;
  targetRegion: string;
  reason?: string;
  metadata?: Record<string, unknown>;
}

export interface ReplicationJobListInput {
  status?: ReplicationJobStatus;
  sourceRegion?: string;
  targetRegion?: string;
  limit?: number;
}

export interface ReplicationJobCancelInput {
  reason?: string;
}

export interface RuleScopeBinding {
  organizations?: string[];
  projects?: string[];
  clients?: string[];
}

export interface RuleAsset {
  id: string;
  tenantId: string;
  name: string;
  description?: string;
  status: RuleLifecycleStatus;
  latestVersion: number;
  publishedVersion?: number;
  scopeBinding: RuleScopeBinding;
  createdAt: string;
  updatedAt: string;
}

export interface RuleAssetCreateInput {
  name: string;
  description?: string;
  scopeBinding?: RuleScopeBinding;
}

export interface RuleAssetListInput {
  status?: RuleLifecycleStatus;
  keyword?: string;
  limit?: number;
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

export interface RulePublishInput {
  version: number;
}

export interface RuleRollbackInput {
  version: number;
  reason?: string;
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

export interface RuleApprovalCreateInput {
  version: number;
  decision: RuleApprovalDecision;
  reason?: string;
}

export interface RuleApprovalListInput {
  version?: number;
  decision?: RuleApprovalDecision;
  limit?: number;
}

export interface McpToolPolicy {
  tenantId: string;
  toolId: string;
  riskLevel: McpRiskLevel;
  decision: McpToolDecision;
  reason?: string;
  updatedAt: string;
}

export interface McpToolPolicyUpsertInput {
  toolId: string;
  riskLevel: McpRiskLevel;
  decision: McpToolDecision;
  reason?: string;
}

export interface McpToolPolicyListInput {
  riskLevel?: McpRiskLevel;
  decision?: McpToolDecision;
  keyword?: string;
  limit?: number;
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

export interface AuditItem {
  id: string;
  eventId: string;
  action: string;
  level: AuditLevel;
  detail: string;
  metadata: Record<string, unknown>;
  createdAt: string;
}

export type ApiKeyScope = "read" | "write" | "admin";
export type ApiKeyStatus = "active" | "revoked" | "expired";

export interface ApiKeyItem {
  id: string;
  tenantId: string;
  name: string;
  scope: ApiKeyScope;
  status: ApiKeyStatus;
  keyPrefix: string;
  createdByUserId?: string;
  lastUsedAt?: string;
  expiresAt?: string;
  revokedAt?: string;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface ApiKeySecretView {
  keyId: string;
  keyPrefix: string;
  secret: string;
  createdAt: string;
}

export interface ApiKeyListInput {
  tenantId: string;
  scope?: ApiKeyScope;
  status?: ApiKeyStatus;
  keyword?: string;
  from?: string;
  to?: string;
  limit?: number;
  cursor?: string;
}

export interface ApiKeyCreateInput {
  tenantId: string;
  name: string;
  scope: ApiKeyScope;
  expiresAt?: string;
  metadata?: Record<string, unknown>;
}

export interface ApiKeyRevokeInput {
  tenantId: string;
  keyId: string;
  reason?: string;
}

export type WebhookEventType =
  | "api_key.created"
  | "api_key.revoked"
  | "quality.event.created"
  | "quality.scorecard.updated"
  | "replay.job.started"
  | "replay.job.completed"
  | "replay.job.failed";

export type WebhookEndpointStatus = "active" | "paused" | "disabled";

export interface WebhookEndpoint {
  id: string;
  tenantId: string;
  name: string;
  url: string;
  events: WebhookEventType[];
  status: WebhookEndpointStatus;
  secretHint?: string;
  failureCount: number;
  lastSuccessAt?: string;
  lastFailureAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface WebhookEndpointCreateInput {
  tenantId: string;
  name: string;
  url: string;
  events: WebhookEventType[];
  status?: WebhookEndpointStatus;
  secret?: string;
}

export interface WebhookEndpointUpdateInput {
  endpointId: string;
  name?: string;
  url?: string;
  events?: WebhookEventType[];
  status?: WebhookEndpointStatus;
  secret?: string;
}

export type QualityMetric =
  | "accuracy"
  | "consistency"
  | "groundedness"
  | "safety"
  | "latency";

export interface QualityEvent {
  id: string;
  tenantId: string;
  sessionId?: string;
  replayJobId?: string;
  metric: QualityMetric;
  score: number;
  sampleCount: number;
  occurredAt: string;
  notes?: string;
  metadata: Record<string, unknown>;
  createdAt: string;
}

export interface QualityEventCreateInput {
  tenantId: string;
  sessionId?: string;
  replayJobId?: string;
  metric: QualityMetric;
  score: number;
  sampleCount: number;
  occurredAt: string;
  notes?: string;
  metadata?: Record<string, unknown>;
}

export interface QualityDailyMetric {
  date: string;
  metric: QualityMetric;
  avgScore: number;
  p50Score: number;
  p90Score: number;
  totalEvents: number;
}

export interface QualityScorecard {
  id: string;
  tenantId: string;
  metric: QualityMetric;
  targetScore: number;
  warningScore: number;
  criticalScore: number;
  weight: number;
  enabled: boolean;
  updatedByUserId?: string;
  updatedAt: string;
}

export interface QualityScorecardUpsertInput {
  tenantId: string;
  metric: QualityMetric;
  targetScore: number;
  warningScore: number;
  criticalScore: number;
  weight?: number;
  enabled: boolean;
  updatedAt: string;
}

export type ReplayJobStatus = "pending" | "running" | "completed" | "failed" | "cancelled";

export interface ReplayBaseline {
  id: string;
  tenantId: string;
  name: string;
  datasetId: string;
  model: string;
  promptVersion?: string;
  sampleCount: number;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface ReplayBaselineCreateInput {
  tenantId: string;
  name: string;
  datasetId: string;
  model: string;
  promptVersion?: string;
  sampleCount?: number;
  metadata?: Record<string, unknown>;
}

export interface ReplayJobCreateInput {
  tenantId: string;
  baselineId: string;
  candidateLabel: string;
  from?: string;
  to?: string;
  sampleLimit?: number;
  metadata?: Record<string, unknown>;
}

export interface ReplayJobDiffItem {
  caseId: string;
  metric: QualityMetric;
  baselineScore: number;
  candidateScore: number;
  delta: number;
  verdict: "improved" | "regressed" | "unchanged";
  detail?: string;
}

export interface ReplayJob {
  id: string;
  tenantId: string;
  baselineId: string;
  candidateLabel: string;
  status: ReplayJobStatus;
  totalCases: number;
  processedCases: number;
  improvedCases: number;
  regressedCases: number;
  unchangedCases: number;
  diffs: ReplayJobDiffItem[];
  summary: Record<string, unknown>;
  error?: string;
  createdAt: string;
  updatedAt: string;
  startedAt?: string;
  finishedAt?: string;
}

export interface CreateSourceInput {
  name: string;
  type: SourceType;
  location: string;
  sshConfig?: SSHConfig;
  accessMode?: SourceAccessMode;
  syncCron?: string;
  syncRetentionDays?: number;
  enabled?: boolean;
}

export type SyncJobStatus = "pending" | "running" | "success" | "failed" | "cancelled";

export interface SyncJob {
  id: string;
  sourceId: string;
  mode: SourceAccessMode;
  status: SyncJobStatus;
  error?: string;
  trigger?: string;
  attempt?: number;
  startedAt?: string;
  endedAt?: string;
  nextRunAt?: string;
  durationMs?: number;
  errorCode?: string;
  errorDetail?: string;
  cancelRequested?: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface CreateSyncJobInput {
  sourceId: string;
  mode: SourceAccessMode;
  status: SyncJobStatus;
  error?: string;
  trigger?: string;
  attempt?: number;
  startedAt?: string;
  endedAt?: string;
  nextRunAt?: string;
  durationMs?: number;
  errorCode?: string;
  errorDetail?: string;
  cancelRequested?: boolean;
}

export interface SourceWatermark {
  sourceId: string;
  provider: string;
  watermark: string;
  createdAt: string;
  updatedAt: string;
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
  filters: SourceParseFailureQueryInput;
}

export interface SourceListResponse {
  items: Source[];
  total: number;
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

export type ExportFormat = "json" | "csv";

export interface SessionExportQueryInput extends SessionSearchInput {
  format: ExportFormat;
}

export type UsageExportDimension =
  | "daily"
  | "weekly"
  | "monthly"
  | "models"
  | "sessions"
  | "heatmap";

export interface UsageExportQueryInput extends UsageAggregateFilters {
  format: ExportFormat;
  dimension: UsageExportDimension;
  timezone?: string;
}

export type SessionExportJobStatus = "pending" | "running" | "completed" | "failed";

export type SessionExportJobCreateInput = SessionExportQueryInput;

export interface SessionExportJob {
  id: string;
  status: SessionExportJobStatus;
  format: ExportFormat;
  filters: SessionSearchInput;
  requestedAt: string;
  startedAt?: string;
  completedAt?: string;
  failedAt?: string;
  error?: string;
  total?: number;
  count?: number;
}

export interface SessionSearchResponse {
  items: Session[];
  total: number;
  nextCursor: string | null;
  filters: SessionSearchInput;
  sourceFreshness?: SessionSourceFreshness[];
}

export type UsageCostMode = "raw" | "estimated" | "reported" | "mixed" | "none";

export interface UsageCostSource {
  costRaw: number;
  costEstimated: number;
  costMode: UsageCostMode;
}

export interface UsageAggregateFilters {
  from?: string;
  to?: string;
  limit: number;
}

export interface UsageListResponse<TItem> {
  items: TItem[];
  total: number;
  filters: UsageAggregateFilters;
}

export interface UsageHeatmapDrilldownFilters extends UsageAggregateFilters {
  date: string;
  metric: UsageHeatmapMetric;
}

export interface UsageHeatmapDrilldownSummary {
  tokens: number;
  cost: number;
  sessions: number;
}

export interface UsageHeatmapDrilldownResponse {
  items: UsageSessionBreakdownItem[];
  total: number;
  filters: UsageHeatmapDrilldownFilters;
  summary: UsageHeatmapDrilldownSummary;
}

export interface UsageMonthlyItem extends UsageCostSource {
  month: string;
  tokens: number;
  cost: number;
  sessions: number;
}

export interface UsageDailyItem extends UsageCostSource {
  date: string;
  tokens: number;
  cost: number;
  sessions: number;
  change: {
    tokens: number | null;
    cost: number | null;
    sessions: number | null;
  };
}

export interface UsageModelItem extends UsageCostSource {
  model: string;
  tokens: number;
  cost: number;
  sessions: number;
}

export interface UsageSessionBreakdownItem extends UsageCostSource {
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

export interface BudgetUpsertInput {
  scope: BudgetScope;
  sourceId?: string;
  organizationId?: string;
  userId?: string;
  model?: string;
  period: BudgetPeriod;
  tokenLimit: number;
  costLimit: number;
  thresholds?: BudgetThresholds;
  alertThreshold?: number;
}

export type BudgetReleaseRequestStatus = "pending" | "rejected" | "executed";

export interface BudgetReleaseRequestApproval {
  userId: string;
  email?: string;
  approvedAt: string;
}

export interface BudgetReleaseRequest {
  id: string;
  tenantId: string;
  budgetId: string;
  status: BudgetReleaseRequestStatus;
  requestedByUserId: string;
  requestedByEmail?: string;
  requestedAt: string;
  approvals: BudgetReleaseRequestApproval[];
  rejectedByUserId?: string;
  rejectedByEmail?: string;
  rejectedReason?: string;
  rejectedAt?: string;
  executedAt?: string;
  updatedAt: string;
}

export interface CreateBudgetReleaseRequestInput {
  reason?: string;
}

export interface RejectBudgetReleaseRequestInput {
  reason?: string;
}

export type IntegrationAlertCallbackAction =
  | "ack"
  | "resolve"
  | "request_release"
  | "approve_release"
  | "reject_release";

export interface IntegrationAlertCallbackInput {
  callbackId: string;
  tenantId?: string;
  action: IntegrationAlertCallbackAction;
  alertId?: string;
  budgetId?: string;
  requestId?: string;
  actorUserId?: string;
  actorEmail?: string;
  reason?: string;
}

export interface AlertListInput {
  status?: AlertStatus;
  severity?: AlertSeverity;
  sourceId?: string;
  from?: string;
  to?: string;
  limit?: number;
  cursor?: string;
}

export interface AlertOrchestrationRuleListInput {
  eventType?: AlertOrchestrationEventType;
  enabled?: boolean;
  severity?: AlertSeverity;
  sourceId?: string;
}

export interface AlertStatusUpdateInput {
  status: AlertMutableStatus;
}

export interface AlertListResponse {
  items: Alert[];
  total: number;
  filters: AlertListInput;
  nextCursor: string | null;
}

export interface AlertOrchestrationRuleListResponse {
  items: AlertOrchestrationRule[];
  total: number;
  filters: AlertOrchestrationRuleListInput;
}

export interface ReplicationJobListResponse {
  items: ReplicationJob[];
  total: number;
  filters: ReplicationJobListInput;
}

export interface RuleAssetListResponse {
  items: RuleAsset[];
  total: number;
  filters: RuleAssetListInput;
}

export interface RuleApprovalListResponse {
  items: RuleApproval[];
  total: number;
  filters: RuleApprovalListInput;
}

export interface McpToolPolicyListResponse {
  items: McpToolPolicy[];
  total: number;
  filters: McpToolPolicyListInput;
}

export interface McpInvocationListResponse {
  items: McpInvocationAudit[];
  total: number;
  filters: McpInvocationListInput;
}

export interface AuditListInput {
  level?: AuditLevel;
  from?: string;
  to?: string;
  limit?: number;
  cursor?: string;
}

export interface AuditExportQueryInput extends AuditListInput {
  format: ExportFormat;
  eventId?: string;
  action?: string;
  keyword?: string;
}

export interface AuditListResponse {
  items: AuditItem[];
  total: number;
  filters: AuditListInput;
  nextCursor: string | null;
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

export interface SystemConfigBackupSource {
  name: string;
  type: SourceType;
  location: string;
  sshConfig?: SSHConfig;
  accessMode: SourceAccessMode;
  syncCron?: string;
  syncRetentionDays?: number;
  enabled: boolean;
}

export interface SystemConfigBackupBudget extends BudgetUpsertInput {}

export interface SystemConfigBackupPricingCatalog {
  note?: string;
  entries: PricingCatalogEntry[];
}

export interface SystemConfigBackupPayload {
  schemaVersion: string;
  tenantId: string;
  exportedAt: string;
  exportedBy: {
    userId: string;
    email?: string;
  };
  sources: SystemConfigBackupSource[];
  budgets: SystemConfigBackupBudget[];
  pricingCatalog?: SystemConfigBackupPricingCatalog;
}

export interface SystemConfigRestoreInput {
  backup: SystemConfigBackupPayload;
  dryRun?: boolean;
  restoreSources?: boolean;
  restoreBudgets?: boolean;
  restorePricingCatalog?: boolean;
}

export interface SystemConfigRestoreSummary {
  sources: {
    total: number;
    created: number;
    skipped: number;
  };
  budgets: {
    total: number;
    upserted: number;
    skipped: number;
  };
  pricingCatalog: {
    included: boolean;
    restored: boolean;
    entryCount: number;
  };
}

export interface SystemConfigRestoreResult {
  tenantId: string;
  dryRun: boolean;
  restoredAt: string;
  summary: SystemConfigRestoreSummary;
  warnings: string[];
}

export interface AuthRegisterInput {
  email: string;
  password: string;
  displayName: string;
}

export interface AuthLoginInput {
  email: string;
  password: string;
}

export interface AuthRefreshInput {
  refreshToken: string;
}

export interface AuthLogoutInput {
  refreshToken: string;
}

export interface AuthExternalLoginInput {
  providerId: string;
  externalUserId: string;
  email: string;
  displayName?: string;
  tenantId?: string;
  timestamp: string;
  nonce: string;
  signature: string;
}

export interface AuthExternalExchangeInput {
  providerId: string;
  code: string;
  redirectUri: string;
  codeVerifier?: string;
  state?: string;
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

export interface AuthTokens {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
  tokenType: "Bearer";
}

export type TenantRole = "owner" | "maintainer" | "member" | "readonly";
export type OrgRole = "owner" | "maintainer" | "member" | "readonly";

export interface AuthUserProfile {
  userId: string;
  email: string;
  displayName: string;
  tenantId?: string;
  organizationId?: string;
  tenantRole?: TenantRole;
  orgRole?: OrgRole;
}

export interface AuthSessionInfo {
  sessionId: string;
  issuedAt: string;
  expiresAt: string;
}

export interface AuthMeResponse {
  user: AuthUserProfile;
  session: AuthSessionInfo;
  tenants: TenantItem[];
  organizations: OrganizationItem[];
}

export interface TenantItem {
  id: string;
  name: string;
  slug: string;
  active: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface OrganizationItem {
  id: string;
  tenantId: string;
  name: string;
  slug: string;
  active: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface TenantMemberItem {
  id: string;
  tenantId: string;
  userId: string;
  email: string;
  displayName: string;
  tenantRole: TenantRole;
  organizationId?: string;
  orgRole?: OrgRole;
  createdAt: string;
  updatedAt: string;
}

export interface CreateTenantInput {
  name: string;
  slug?: string;
}

export interface CreateOrganizationInput {
  tenantId: string;
  name: string;
  slug?: string;
}

export interface AddTenantMemberInput {
  tenantId: string;
  userId?: string;
  email?: string;
  tenantRole: TenantRole;
  organizationId?: string;
  orgRole?: OrgRole;
}

export type SourceBindingMethod = "ssh-pull" | "agent-push";

export interface DeviceItem {
  id: string;
  tenantId: string;
  organizationId?: string;
  userId: string;
  hostname: string;
  fingerprint: string;
  platform?: string;
  active: boolean;
  lastSeenAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface AgentItem {
  id: string;
  tenantId: string;
  organizationId?: string;
  userId?: string;
  deviceId: string;
  hostname: string;
  version?: string;
  active: boolean;
  lastSeenAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface SourceBindingItem {
  id: string;
  tenantId: string;
  organizationId?: string;
  userId?: string;
  sourceId: string;
  deviceId?: string;
  agentId?: string;
  method: SourceBindingMethod;
  accessMode: SourceAccessMode;
  active: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface DeviceListInput {
  tenantId: string;
  organizationId?: string;
  userId?: string;
  keyword?: string;
  limit?: number;
  cursor?: string;
}

export interface AgentListInput {
  tenantId: string;
  organizationId?: string;
  userId?: string;
  deviceId?: string;
  keyword?: string;
  limit?: number;
  cursor?: string;
}

export interface SourceBindingListInput {
  tenantId: string;
  organizationId?: string;
  userId?: string;
  sourceId?: string;
  deviceId?: string;
  agentId?: string;
  method?: SourceBindingMethod;
  accessMode?: SourceAccessMode;
  active?: boolean;
  limit?: number;
  cursor?: string;
}

export interface CreateDeviceInput {
  tenantId: string;
  organizationId?: string;
  userId: string;
  hostname: string;
  fingerprint: string;
  platform?: string;
}

export interface CreateAgentInput {
  tenantId: string;
  organizationId?: string;
  userId?: string;
  deviceId: string;
  hostname: string;
  version?: string;
}

export interface CreateSourceBindingInput {
  tenantId: string;
  organizationId?: string;
  userId?: string;
  sourceId: string;
  deviceId?: string;
  agentId?: string;
  method: SourceBindingMethod;
  accessMode?: SourceAccessMode;
}

export interface DeleteDeviceInput {
  tenantId: string;
  deviceId: string;
}

export interface DeleteAgentInput {
  tenantId: string;
  agentId: string;
}

export interface DeleteSourceBindingInput {
  tenantId: string;
  bindingId: string;
}

export interface DeviceListResponse {
  items: DeviceItem[];
  total: number;
  filters: DeviceListInput;
}

export interface AgentListResponse {
  items: AgentItem[];
  total: number;
  filters: AgentListInput;
}

export interface SourceBindingListResponse {
  items: SourceBindingItem[];
  total: number;
  filters: SourceBindingListInput;
}
