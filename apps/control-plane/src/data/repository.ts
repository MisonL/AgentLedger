import type {
  Alert,
  AlertOrchestrationChannel,
  AlertOrchestrationDispatchMode,
  AlertOrchestrationExecutionCreateInput,
  AlertOrchestrationExecutionListInput,
  AlertOrchestrationExecutionLog,
  AlertOrchestrationEventType,
  AlertOrchestrationRule,
  AlertOrchestrationRuleListInput,
  AlertOrchestrationRuleUpsertInput,
  AlertListInput,
  AlertMutableStatus,
  AlertSeverity,
  AlertStatus,
  AuditItem,
  AuditLevel,
  AuditListInput,
  Budget,
  BudgetGovernanceState,
  BudgetPeriod,
  BudgetReleaseRequest,
  BudgetReleaseRequestStatus,
  BudgetScope,
  BudgetThresholds,
  BudgetUpsertInput,
  CreateSourceInput,
  DataResidencyMode,
  HeatmapCell,
  IntegrationAlertCallbackAction,
  McpApprovalCreateInput,
  McpApprovalRequest,
  McpApprovalReviewInput,
  McpApprovalStatus,
  McpInvocationAudit,
  McpInvocationListInput,
  McpRiskLevel,
  McpToolDecision,
  McpToolPolicy,
  McpToolPolicyListInput,
  McpToolPolicyUpsertInput,
  PricingCatalog,
  PricingCatalogEntry,
  RegionDescriptor,
  ReplicationJob,
  ReplicationJobApproveInput,
  ReplicationJobCancelInput,
  ReplicationJobCreateInput,
  ReplicationJobListInput,
  ReplicationJobStatus,
  RuleApproval,
  RuleApprovalCreateInput,
  RuleApprovalDecision,
  RuleApprovalListInput,
  RuleAsset,
  RuleAssetCreateInput,
  RuleAssetListInput,
  RuleAssetVersion,
  RuleAssetVersionCreateInput,
  RuleLifecycleStatus,
  RuleScopeBinding,
  RulePublishInput,
  RuleRollbackInput,
  Session,
  SessionDetail,
  SessionEvent,
  SessionSearchInput,
  Source,
  SourceAccessMode,
  SourceBindingMethod,
  SourceHealth,
  SSHConfig,
  SourceWatermark,
  SourceType,
  SyncJob,
  SyncJobStatus,
  TenantResidencyPolicy,
  TenantResidencyPolicyUpsertInput,
  UsageDailyItem,
  UsageCostMode,
  UsageModelItem,
  UsageMonthlyItem,
  UsageSessionBreakdownItem,
  WebhookEventType,
  OrgRole,
  TenantRole,
} from "../contracts";

export type ReplayJobStatus = "pending" | "running" | "completed" | "failed" | "cancelled";
export type WebhookReplayTaskStatus = "queued" | "running" | "completed" | "failed";

const DEFAULT_SESSION_LIMIT = 50;
const DEFAULT_ALERT_LIMIT = 50;
const DEFAULT_ALERT_ORCHESTRATION_EXECUTION_LIMIT = 50;
const DEFAULT_AUDIT_LIMIT = 50;
const DEFAULT_SYNC_JOB_LIMIT = 50;
const DEFAULT_PARSE_FAILURE_LIMIT = 50;
const MAX_PARSE_FAILURE_LIMIT = 500;
const DEFAULT_WEBHOOK_ENDPOINT_LIMIT = 200;
const DEFAULT_WEBHOOK_REPLAY_TASK_LIMIT = 100;
const DEFAULT_WEBHOOK_REPLAY_EVENT_LIMIT = 200;
const DEFAULT_QUALITY_DAILY_METRIC_LIMIT = 60;
const DEFAULT_QUALITY_SCORECARD_LIMIT = 100;
const DEFAULT_REPLAY_BASELINE_LIMIT = 100;
const DEFAULT_REPLAY_JOB_LIMIT = 100;
const DEFAULT_REPLAY_DATASET_CASE_LIMIT = 1000;
const DEFAULT_REPLAY_ARTIFACT_LIMIT = 20;
const MAX_ALERT_ORCHESTRATION_EXECUTION_LIMIT = 200;
const SOURCE_TYPES: ReadonlyArray<SourceType> = ["local", "ssh", "sync-cache"];
const SOURCE_ACCESS_MODES: ReadonlyArray<SourceAccessMode> = ["realtime", "sync", "hybrid"];
const ALERT_STATUS_SET: ReadonlyArray<AlertStatus> = ["open", "acknowledged", "resolved"];
const ALERT_SEVERITY_SET: ReadonlyArray<AlertSeverity> = [
  "warning",
  "critical",
];
const ALERT_ORCHESTRATION_EVENT_TYPES: ReadonlyArray<AlertOrchestrationEventType> = [
  "alert",
  "weekly",
];
const ALERT_ORCHESTRATION_CHANNELS: ReadonlyArray<AlertOrchestrationChannel> = [
  "webhook",
  "wecom",
  "dingtalk",
  "feishu",
  "email",
  "email_webhook",
  "ticket",
];
const DATA_RESIDENCY_MODES: ReadonlyArray<DataResidencyMode> = ["single_region", "active_active"];
const REPLICATION_JOB_STATUSES: ReadonlyArray<ReplicationJobStatus> = [
  "pending",
  "running",
  "succeeded",
  "failed",
  "cancelled",
];
const RULE_LIFECYCLE_STATUSES: ReadonlyArray<RuleLifecycleStatus> = [
  "draft",
  "published",
  "deprecated",
];
const RULE_APPROVAL_DECISIONS: ReadonlyArray<RuleApprovalDecision> = ["approved", "rejected"];
const MCP_RISK_LEVELS: ReadonlyArray<McpRiskLevel> = ["low", "medium", "high"];
const MCP_TOOL_DECISIONS: ReadonlyArray<McpToolDecision> = ["allow", "deny", "require_approval"];
const MCP_APPROVAL_STATUSES: ReadonlyArray<McpApprovalStatus> = [
  "pending",
  "approved",
  "rejected",
];
const REPLAY_JOB_STATUS_SET: ReadonlyArray<ReplayJobStatus> = [
  "pending",
  "running",
  "completed",
  "failed",
  "cancelled",
];
const WEBHOOK_REPLAY_TASK_STATUS_SET: ReadonlyArray<WebhookReplayTaskStatus> = [
  "queued",
  "running",
  "completed",
  "failed",
];
const WEBHOOK_EVENT_TYPES: ReadonlyArray<WebhookEventType> = [
  "api_key.created",
  "api_key.revoked",
  "quality.event.created",
  "quality.scorecard.updated",
  "replay.job.started",
  "replay.job.completed",
  "replay.job.failed",
  "replay.run.started",
  "replay.run.completed",
  "replay.run.regression_detected",
  "replay.run.failed",
  "replay.run.cancelled",
];
const AUDIT_LEVEL_SET: ReadonlyArray<AuditLevel> = ["info", "warning", "error", "critical"];
const TENANT_ROLE_SET: ReadonlyArray<TenantRole> = [
  "owner",
  "maintainer",
  "member",
  "readonly",
];
const ORG_ROLE_SET: ReadonlyArray<OrgRole> = [
  "owner",
  "maintainer",
  "member",
  "readonly",
];
const SYNC_JOB_STATUSES: ReadonlyArray<SyncJobStatus> = [
  "pending",
  "running",
  "success",
  "failed",
  "cancelled",
];
const BUDGET_GOVERNANCE_STATES: ReadonlyArray<BudgetGovernanceState> = [
  "active",
  "frozen",
  "pending_release",
];
const BUDGET_RELEASE_REQUEST_STATUSES: ReadonlyArray<BudgetReleaseRequestStatus> = [
  "pending",
  "rejected",
  "executed",
];
const CONTROL_PLANE_SOURCE_PROVIDER = "manual";
const DEFAULT_TENANT_ID = "default";
const DEFAULT_TENANT_NAME = "Default Tenant";
const DEFAULT_HEATMAP_TIMEZONE = "UTC";
const DEFAULT_CALLBACK_CLAIM_STALE_AFTER_MS = 2 * 60 * 1000;
const DEFAULT_REGIONS: ReadonlyArray<RegionDescriptor> = [
  {
    id: "cn-hangzhou",
    name: "华东 1（杭州）",
    active: true,
    description: "默认主区域",
  },
  {
    id: "cn-shanghai",
    name: "华东 2（上海）",
    active: true,
    description: "推荐副本区域",
  },
  {
    id: "ap-southeast-1",
    name: "新加坡",
    active: true,
    description: "海外合规区域",
  },
];
const QUALITY_EXTERNAL_GROUP_BY_TO_COLUMN: Readonly<Record<QualityExternalMetricGroupBy, string>> = {
  provider: "provider",
  repo: "repository",
  workflow: "workflow",
  runId: "run_id",
};
const USAGE_AGGREGATE_PROJECT_SQL = `COALESCE(
  NULLIF(to_jsonb(sess)->>'project', ''),
  NULLIF(to_jsonb(sess)->>'project_id', ''),
  NULLIF(to_jsonb(sess)->>'workspace', ''),
  NULLIF(to_jsonb(sess)->>'workspace_id', ''),
  NULLIF(to_jsonb(sess)->>'repo', ''),
  NULLIF(to_jsonb(sess)->>'repository', ''),
  NULLIF(src.workspace_id, ''),
  NULLIF(sess.source_metadata->>'project', ''),
  NULLIF(sess.source_metadata->>'project_id', ''),
  NULLIF(sess.source_metadata->>'workspace', ''),
  NULLIF(sess.source_metadata->>'workspace_id', ''),
  NULLIF(sess.source_metadata->>'repo', ''),
  NULLIF(sess.source_metadata->>'repository', ''),
  ''
)`;

type DbRow = Record<string, unknown>;

type PgQueryResult = {
  rows: DbRow[];
};

type PgQueryable = {
  query: (text: string, values?: readonly unknown[]) => Promise<PgQueryResult>;
};

type PgClient = PgQueryable & {
  release: () => void;
};

type PgPool = {
  query: PgQueryable["query"];
  connect: () => Promise<PgClient>;
  on: (event: "error", listener: (error: unknown) => void) => void;
};

type PgModule = {
  Pool: new (config: { connectionString: string }) => PgPool;
};

type PgErrorLike = {
  code?: unknown;
};

export interface SessionSearchResult {
  items: Session[];
  total: number;
  nextCursor: string | null;
}

export interface SessionEventListResult {
  items: SessionEvent[];
  total: number;
  nextCursor: string | null;
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

export interface SourceParseFailureListResult {
  items: SourceParseFailure[];
  total: number;
}

export interface AuditListResult {
  items: AuditItem[];
  total: number;
  nextCursor: string | null;
}

export interface AlertListResult {
  items: Alert[];
  total: number;
  nextCursor: string | null;
}

export interface AlertOrchestrationRuleListResult {
  items: AlertOrchestrationRule[];
  total: number;
}

export interface AlertOrchestrationExecutionListResult {
  items: AlertOrchestrationExecutionLog[];
  total: number;
}

export interface ReplicationJobListResult {
  items: ReplicationJob[];
  total: number;
}

export interface RuleAssetListResult {
  items: RuleAsset[];
  total: number;
}

export interface RuleApprovalListResult {
  items: RuleApproval[];
  total: number;
}

export interface McpToolPolicyListResult {
  items: McpToolPolicy[];
  total: number;
}

export interface McpApprovalRequestListResult {
  items: McpApprovalRequest[];
  total: number;
}

export interface McpInvocationListResult {
  items: McpInvocationAudit[];
  total: number;
}

export interface AuditListQueryInput extends AuditListInput {
  eventId?: string;
  action?: string;
  keyword?: string;
}

export interface AppendAuditLogInput {
  tenantId?: string;
  eventId?: string;
  action: string;
  level?: AuditLevel;
  detail?: string;
  metadata?: Record<string, unknown>;
  createdAt?: string;
}

export interface CreateSyncJobOptions {
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

export type DeleteSourceResult = "deleted" | "not_found" | "conflict";

export interface UsageHeatmapQueryInput {
  tenantId?: string;
  from?: string;
  to?: string;
  timezone?: string;
  metric?: string;
}

export interface UsageAggregateQueryInput {
  tenantId?: string;
  from?: string;
  to?: string;
  project?: string;
  limit?: number;
}

export interface PricingCatalogUpsertInput {
  note?: string;
  entries: PricingCatalogEntry[];
}

interface UsageDailyBaseItem {
  date: string;
  tokens: number;
  cost: number;
  costRaw: number;
  costEstimated: number;
  costMode: UsageCostMode;
  sessions: number;
}

interface UsageCostComponents {
  cost: number;
  costRaw: number;
  costEstimated: number;
  costMode: UsageCostMode;
}

interface UsageCostModeCounters {
  raw: number;
  reported: number;
  estimated: number;
}

interface SessionUsageCostSnapshot extends UsageCostComponents {
  modeCounters: UsageCostModeCounters;
}

interface MemorySessionEventRecord {
  sessionId: string;
  sourceId: string;
  text: string;
  sourcePath?: string;
}

interface MemorySourceParseFailureRecord {
  tenantId: string;
  failure: SourceParseFailure;
}

interface MemoryApiKeyHashRecord {
  tenantId: string;
  apiKeyId: string;
}

interface MemoryReplayJobDiffRecord {
  tenantId: string;
  replayJobId: string;
  diff: Record<string, unknown>;
}

interface NormalizedSessionSearchInput {
  tenantId?: string;
  sourceId?: string;
  keyword?: string;
  clientType?: string;
  tool?: string;
  host?: string;
  model?: string;
  project?: string;
  from?: string;
  to?: string;
  limit: number;
  cursor?: string;
}

interface NormalizedSourceParseFailureQueryInput {
  from?: string;
  to?: string;
  parserKey?: string;
  errorCode?: string;
  limit: number;
}

interface NormalizedAlertListInput {
  status?: AlertStatus;
  severity?: AlertSeverity;
  sourceId?: string;
  from?: string;
  to?: string;
  limit: number;
  cursor?: string;
}

interface NormalizedAlertOrchestrationRuleListInput {
  eventType?: AlertOrchestrationEventType;
  enabled?: boolean;
  severity?: AlertSeverity;
  sourceId?: string;
}

interface NormalizedAlertOrchestrationExecutionListInput {
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
  limit: number;
}

interface NormalizedReplicationJobListInput {
  status?: ReplicationJobStatus;
  sourceRegion?: string;
  targetRegion?: string;
  limit: number;
}

interface NormalizedRuleAssetListInput {
  status?: RuleLifecycleStatus;
  keyword?: string;
  limit: number;
}

interface NormalizedRuleApprovalListInput {
  version?: number;
  decision?: RuleApprovalDecision;
  limit: number;
}

interface NormalizedMcpToolPolicyListInput {
  riskLevel?: McpRiskLevel;
  decision?: McpToolDecision;
  keyword?: string;
  limit: number;
}

interface NormalizedMcpInvocationListInput {
  toolId?: string;
  decision?: McpToolDecision;
  from?: string;
  to?: string;
  limit: number;
}

interface NormalizedAuditListInput {
  tenantId?: string;
  eventId?: string;
  action?: string;
  level?: AuditLevel;
  keyword?: string;
  from?: string;
  to?: string;
  limit: number;
  cursor?: string;
}

interface NormalizedQualityDailyMetricsInput {
  from?: string;
  to?: string;
  scorecardKey?: string;
  provider?: string;
  repo?: string;
  workflow?: string;
  runId?: string;
  groupBy?: QualityExternalMetricGroupBy;
  limit: number;
}

interface NormalizedQualityExternalMetricGroupsInput {
  from?: string;
  to?: string;
  scorecardKey?: string;
  provider?: string;
  repo?: string;
  workflow?: string;
  runId?: string;
  groupBy: QualityExternalMetricGroupBy;
  limit: number;
}

interface NormalizedQualityScorecardListInput {
  scorecardKey?: string;
  limit: number;
}

interface NormalizedReplayBaselineListInput {
  keyword?: string;
  limit: number;
}

interface NormalizedReplayDatasetListInput {
  keyword?: string;
  limit: number;
}

interface NormalizedReplayDatasetCaseListInput {
  limit: number;
}

interface NormalizedReplayJobListInput {
  baselineId?: string;
  status?: ReplayJobStatus;
  limit: number;
}

interface NormalizedReplayRunListInput {
  datasetId?: string;
  status?: ReplayJobStatus;
  limit: number;
}

interface NormalizedReplayArtifactListInput {
  limit: number;
}

interface NormalizedWebhookReplayTaskListInput {
  webhookId?: string;
  status?: WebhookReplayTaskStatus;
  limit: number;
  cursor?: string;
}

interface NormalizedWebhookReplayEventListInput {
  eventTypes: WebhookEventType[];
  from?: string;
  to?: string;
  limit: number;
}

interface NormalizedUsageHeatmapInput {
  tenantId: string;
  from?: string;
  to?: string;
  timezone: string;
}

interface NormalizedUsageAggregateInput {
  tenantId: string;
  from?: string;
  to?: string;
  project?: string;
  limit: number;
}

interface TenantBudgetRecord {
  tenantId: string;
  budget: Budget;
}

interface TenantBudgetReleaseRequestRecord {
  tenantId: string;
  request: BudgetReleaseRequest;
}

interface IntegrationAlertCallbackRecord {
  callbackId: string;
  tenantId: string;
  action: IntegrationAlertCallbackAction;
  response: Record<string, unknown>;
  processedAt: string;
}

interface PricingCatalogVersionRecord {
  id: string;
  tenantId: string;
  version: number;
  note?: string;
  createdAt: string;
}

interface PricingCatalogEntryRecord extends PricingCatalogEntry {
  versionId: string;
  tenantId: string;
}

interface ClaimIntegrationAlertCallbackResult {
  claimed: boolean;
  record: IntegrationAlertCallbackRecord;
}

export interface ReleaseRequestActor {
  userId: string;
  email?: string;
}

export interface CreateBudgetReleaseRequestOptions {
  reason?: string;
  requestedAt?: string;
}

export interface RejectBudgetReleaseRequestOptions {
  reason?: string;
  rejectedAt?: string;
}

export interface ListBudgetReleaseRequestsInput {
  status?: BudgetReleaseRequestStatus;
  limit?: number;
}

export interface ListMcpApprovalRequestsInput {
  status?: McpApprovalStatus;
  limit?: number;
}

export interface AppendMcpInvocationInput {
  toolId: string;
  decision: McpToolDecision;
  result: "allowed" | "blocked" | "approved";
  approvalRequestId?: string;
  enforced?: boolean;
  evaluatedDecision?: McpToolDecision;
  metadata?: Record<string, unknown>;
  createdAt?: string;
}

export interface LocalUser {
  id: string;
  email: string;
  passwordHash: string;
  displayName: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreateLocalUserInput {
  email: string;
  passwordHash: string;
  displayName?: string;
}

export interface Tenant {
  id: string;
  name: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreateTenantInput {
  id?: string;
  name: string;
}

export interface Organization {
  id: string;
  tenantId: string;
  name: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreateOrganizationInput {
  name: string;
}

export interface TenantMember {
  id: string;
  tenantId: string;
  userId: string;
  tenantRole: TenantRole;
  organizationId?: string;
  orgRole?: OrgRole;
  createdAt: string;
  updatedAt: string;
}

export interface AddTenantMemberInput {
  tenantId: string;
  userId: string;
  tenantRole?: TenantRole;
  organizationId?: string;
  orgRole?: OrgRole;
}

export interface DeviceBinding {
  id: string;
  tenantId: string;
  deviceId: string;
  displayName?: string;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface CreateDeviceBindingInput {
  deviceId: string;
  displayName?: string;
  metadata?: Record<string, unknown>;
}

export interface AgentBinding {
  id: string;
  tenantId: string;
  agentId: string;
  deviceId?: string;
  displayName?: string;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface CreateAgentBindingInput {
  agentId: string;
  deviceId?: string;
  displayName?: string;
  metadata?: Record<string, unknown>;
}

export interface SourceBinding {
  id: string;
  tenantId: string;
  sourceId: string;
  deviceId?: string;
  agentId?: string;
  bindingType: SourceBindingMethod;
  accessMode: SourceAccessMode;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface CreateSourceBindingInput {
  sourceId: string;
  deviceId?: string;
  agentId?: string;
  bindingType?: SourceBindingMethod;
  accessMode?: SourceAccessMode;
  metadata?: Record<string, unknown>;
}

export interface BudgetScopeBindingValidationError {
  field: "organizationId" | "userId";
  message: string;
}

export interface AuthSession {
  id: string;
  userId: string;
  tenantId: string;
  sessionToken: string;
  expiresAt: string;
  revokedAt: string | null;
  replacedBySessionId: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface CreateAuthSessionInput {
  userId: string;
  tenantId?: string;
  sessionToken: string;
  expiresAt: string;
}

export interface RotateAuthSessionInput {
  sessionToken: string;
  expiresAt: string;
}

export interface ApiKey {
  id: string;
  tenantId: string;
  name: string;
  keyHash: string;
  scopes: string[];
  lastUsedAt?: string;
  revokedAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreateApiKeyInput {
  name: string;
  keyHash: string;
  scopes?: string[];
  createdAt?: string;
}

export interface WebhookEndpoint {
  id: string;
  tenantId: string;
  name: string;
  url: string;
  enabled: boolean;
  eventTypes: string[];
  secretHash?: string;
  secretCiphertext?: string;
  headers: Record<string, string>;
  createdAt: string;
  updatedAt: string;
}

export interface CreateWebhookEndpointInput {
  name: string;
  url: string;
  enabled?: boolean;
  eventTypes?: string[];
  secretHash?: string;
  secretCiphertext?: string;
  headers?: Record<string, string>;
}

export interface UpdateWebhookEndpointInput {
  name?: string;
  url?: string;
  enabled?: boolean;
  eventTypes?: string[];
  secretHash?: string | null;
  secretCiphertext?: string | null;
  headers?: Record<string, string>;
}

export interface WebhookReplayFilters {
  eventType?: string;
  from?: string;
  to?: string;
  limit: number;
}

export interface WebhookReplayTask {
  id: string;
  tenantId: string;
  webhookId: string;
  status: WebhookReplayTaskStatus;
  dryRun: boolean;
  filters: WebhookReplayFilters;
  result: Record<string, unknown>;
  error?: string;
  requestedAt: string;
  startedAt?: string;
  finishedAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreateWebhookReplayTaskInput {
  webhookId: string;
  dryRun: boolean;
  filters: WebhookReplayFilters;
  result?: Record<string, unknown>;
  status?: WebhookReplayTaskStatus;
  error?: string;
  requestedAt?: string;
  startedAt?: string;
  finishedAt?: string;
}

export interface UpdateWebhookReplayTaskInput {
  fromStatuses?: WebhookReplayTaskStatus[];
  status?: WebhookReplayTaskStatus;
  result?: Record<string, unknown>;
  error?: string | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  updatedAt?: string;
}

export interface ListWebhookReplayTasksInput {
  webhookId?: string;
  status?: WebhookReplayTaskStatus;
  limit?: number;
  cursor?: string;
}

export interface WebhookReplayTaskListResult {
  items: WebhookReplayTask[];
  total: number;
  nextCursor: string | null;
}

export interface WebhookReplayEvent {
  id: string;
  tenantId: string;
  eventType: WebhookEventType;
  occurredAt: string;
  payload: Record<string, unknown>;
}

export interface ListWebhookReplayEventsInput {
  eventTypes: WebhookEventType[];
  from?: string;
  to?: string;
  limit?: number;
}

export interface QualityEvent {
  id: string;
  tenantId: string;
  scorecardKey: string;
  metricKey?: string;
  externalSource?: QualityExternalSourceMetadata;
  score: number;
  passed: boolean;
  metadata: Record<string, unknown>;
  createdAt: string;
}

export interface QualityExternalSourceMetadata {
  provider: string;
  repo?: string;
  workflow?: string;
  runId?: string;
}

export interface CreateQualityEventInput {
  scorecardKey: string;
  metricKey?: string;
  externalSource?: QualityExternalSourceMetadata;
  score: number;
  passed?: boolean;
  metadata?: Record<string, unknown>;
  createdAt?: string;
}

export interface ListQualityDailyMetricsInput {
  from?: string;
  to?: string;
  scorecardKey?: string;
  provider?: string;
  repo?: string;
  workflow?: string;
  runId?: string;
  limit?: number;
}

export interface QualityDailyMetric {
  date: string;
  total: number;
  passed: number;
  failed: number;
  averageScore: number;
}

export type QualityExternalMetricGroupBy = "provider" | "repo" | "workflow" | "runId";

export interface ListQualityExternalMetricGroupsInput extends ListQualityDailyMetricsInput {
  groupBy: QualityExternalMetricGroupBy;
}

export interface QualityExternalMetricGroup {
  groupBy: QualityExternalMetricGroupBy;
  value: string;
  total: number;
  passed: number;
  failed: number;
  averageScore: number;
}

export interface QualityScorecard {
  tenantId: string;
  scorecardKey: string;
  title: string;
  description?: string;
  score: number;
  dimensions: Record<string, number>;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface ListQualityScorecardsInput {
  scorecardKey?: string;
  limit?: number;
}

export interface QualityScorecardUpsertInput {
  scorecardKey: string;
  title: string;
  description?: string;
  score: number;
  dimensions?: Record<string, number>;
  metadata?: Record<string, unknown>;
  updatedAt?: string;
}

export interface ReplayBaseline {
  id: string;
  tenantId: string;
  name: string;
  description?: string;
  datasetRef?: string;
  scenarioCount: number;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface CreateReplayBaselineInput {
  name: string;
  description?: string;
  datasetRef?: string;
  scenarioCount?: number;
  metadata?: Record<string, unknown>;
  createdAt?: string;
}

export interface ListReplayBaselinesInput {
  keyword?: string;
  limit?: number;
}

export interface ReplayJob {
  id: string;
  tenantId: string;
  baselineId: string;
  status: ReplayJobStatus;
  parameters: Record<string, unknown>;
  summary: Record<string, unknown>;
  diff: Record<string, unknown>;
  error?: string;
  startedAt?: string;
  finishedAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreateReplayJobInput {
  baselineId: string;
  status?: ReplayJobStatus;
  parameters?: Record<string, unknown>;
  summary?: Record<string, unknown>;
  diff?: Record<string, unknown>;
  error?: string;
  startedAt?: string;
  finishedAt?: string;
  createdAt?: string;
}

export interface ListReplayJobsInput {
  baselineId?: string;
  status?: ReplayJobStatus;
  limit?: number;
}

export interface UpdateReplayJobInput {
  status?: ReplayJobStatus;
  fromStatuses?: ReplayJobStatus[];
  summary?: Record<string, unknown>;
  diff?: Record<string, unknown>;
  error?: string | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  updatedAt?: string;
}

export interface ReplayDataset {
  id: string;
  tenantId: string;
  name: string;
  description?: string;
  model: string;
  promptVersion?: string;
  externalDatasetId?: string;
  caseCount: number;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface CreateReplayDatasetInput {
  name: string;
  description?: string;
  model: string;
  promptVersion?: string;
  externalDatasetId?: string;
  caseCount?: number;
  metadata?: Record<string, unknown>;
  createdAt?: string;
}

export interface ListReplayDatasetsInput {
  keyword?: string;
  limit?: number;
}

export interface ReplayDatasetCase {
  id: string;
  tenantId: string;
  datasetId: string;
  caseId: string;
  sortOrder: number;
  input: string;
  expectedOutput?: string;
  baselineOutput?: string;
  candidateInput?: string;
  metadata: Record<string, unknown>;
  checksum?: string;
  createdAt: string;
  updatedAt: string;
}

export interface ReplayDatasetCaseInput {
  caseId?: string;
  sortOrder?: number;
  input: string;
  expectedOutput?: string;
  baselineOutput?: string;
  candidateInput?: string;
  metadata?: Record<string, unknown>;
}

export interface ListReplayDatasetCasesInput {
  limit?: number;
}

export interface ReplayRun {
  id: string;
  tenantId: string;
  datasetId: string;
  status: ReplayJobStatus;
  parameters: Record<string, unknown>;
  summary: Record<string, unknown>;
  diff: Record<string, unknown>;
  error?: string;
  startedAt?: string;
  finishedAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface CreateReplayRunInput {
  datasetId: string;
  status?: ReplayJobStatus;
  parameters?: Record<string, unknown>;
  summary?: Record<string, unknown>;
  diff?: Record<string, unknown>;
  error?: string;
  startedAt?: string;
  finishedAt?: string;
  createdAt?: string;
}

export interface ListReplayRunsInput {
  datasetId?: string;
  status?: ReplayJobStatus;
  limit?: number;
}

export interface UpdateReplayRunInput {
  status?: ReplayJobStatus;
  fromStatuses?: ReplayJobStatus[];
  summary?: Record<string, unknown>;
  diff?: Record<string, unknown>;
  error?: string | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  updatedAt?: string;
}

export type ReplayArtifactStorageBackend = "local" | "object" | "hybrid";
export type ReplayArtifactType = "summary" | "diff" | "cases";

export interface ReplayArtifact {
  id: string;
  tenantId: string;
  runId: string;
  datasetId: string;
  artifactType: ReplayArtifactType;
  name: string;
  description?: string;
  contentType: string;
  byteSize: number;
  checksum?: string;
  storageBackend: ReplayArtifactStorageBackend;
  storageKey: string;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface ReplayArtifactInput {
  artifactType: ReplayArtifactType;
  name: string;
  description?: string;
  contentType: string;
  byteSize: number;
  checksum?: string;
  storageBackend: ReplayArtifactStorageBackend;
  storageKey: string;
  metadata?: Record<string, unknown>;
  createdAt?: string;
}

export interface ListReplayArtifactsInput {
  limit?: number;
}

function toIsoString(value: unknown): string | null {
  if (value === undefined || value === null) {
    return null;
  }
  const date = value instanceof Date ? value : new Date(String(value));
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString();
}

function toNumber(value: unknown, fallback = 0): number {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toBoolean(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    return value === "1" || value.toLowerCase() === "true";
  }
  return fallback;
}

function toSourceType(value: unknown): SourceType {
  if (typeof value === "string" && SOURCE_TYPES.includes(value as SourceType)) {
    return value as SourceType;
  }
  return "local";
}

function toSourceAccessMode(value: unknown): SourceAccessMode {
  if (
    typeof value === "string" &&
    SOURCE_ACCESS_MODES.includes(value as SourceAccessMode)
  ) {
    return value as SourceAccessMode;
  }
  return "realtime";
}

function toSourceBindingMethod(value: unknown): SourceBindingMethod {
  if (value === "agent-push" || value === "ssh-pull") {
    return value;
  }
  return "ssh-pull";
}

function toSyncJobStatus(value: unknown): SyncJobStatus {
  if (typeof value === "string" && SYNC_JOB_STATUSES.includes(value as SyncJobStatus)) {
    return value as SyncJobStatus;
  }
  return "pending";
}

function toSyncJobAttempt(value: unknown): number {
  const candidate = toOptionalNonNegativeInteger(value);
  if (candidate === undefined || candidate < 1) {
    return 1;
  }
  return candidate;
}

function toDbRow(value: unknown): DbRow | null {
  if (typeof value === "object" && value !== null) {
    return value as DbRow;
  }
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (typeof parsed === "object" && parsed !== null) {
        return parsed as DbRow;
      }
    } catch {
      return null;
    }
  }
  return null;
}

function toJsonArray(value: unknown): unknown[] {
  if (Array.isArray(value)) {
    return value;
  }
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

function firstNonEmptyString(...values: unknown[]): string | null {
  for (const value of values) {
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed.length > 0) {
        return trimmed;
      }
      continue;
    }

    if (typeof value === "number" || typeof value === "bigint") {
      return String(value);
    }
  }
  return null;
}

function toOptionalNonNegativeInteger(value: unknown): number | undefined {
  const parsed = toNumber(value, Number.NaN);
  if (!Number.isFinite(parsed)) {
    return undefined;
  }
  const normalized = Math.trunc(parsed);
  return normalized >= 0 ? normalized : undefined;
}

function normalizeEmail(value: string): string {
  return value.trim().toLowerCase();
}

function normalizeTenantId(value: string | undefined, fallbackName: string): string {
  const raw = (value ?? "").trim().toLowerCase();
  if (raw.length > 0) {
    const normalized = raw
      .replace(/[^a-z0-9_-]/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "");
    if (normalized.length > 0) {
      return normalized;
    }
  }

  const fromName = fallbackName
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  if (fromName.length > 0) {
    return fromName;
  }

  return `tenant-${crypto.randomUUID()}`;
}

function createDefaultTenant(now: string): Tenant {
  return {
    id: DEFAULT_TENANT_ID,
    name: DEFAULT_TENANT_NAME,
    createdAt: now,
    updatedAt: now,
  };
}

function mapSSHConfigFromRow(
  rowData: DbRow,
  metadata: DbRow | null | undefined
): SSHConfig | undefined {
  const sourceType = firstNonEmptyString(rowData.type, rowData.source_type, metadata?.type);
  if (sourceType !== "ssh") {
    return undefined;
  }

  const host = firstNonEmptyString(
    metadata?.ssh_host,
    metadata?.sshHost,
    rowData.hostname
  );
  const user = firstNonEmptyString(metadata?.ssh_user, metadata?.sshUser);
  const authTypeRaw = firstNonEmptyString(metadata?.ssh_auth_type, metadata?.sshAuthType, "key");
  const authType = authTypeRaw === "agent" ? "agent" : "key";
  const portRaw = toNumber(
    metadata?.ssh_port ??
      metadata?.sshPort ??
      22,
    22
  );
  const port = Math.min(65535, Math.max(1, Math.trunc(portRaw || 22)));
  const keyPath = firstNonEmptyString(metadata?.ssh_key_path, metadata?.sshKeyPath);
  const knownHostsPath = firstNonEmptyString(
    metadata?.ssh_known_hosts_path,
    metadata?.sshKnownHostsPath
  );

  if (!host || !user) {
    return undefined;
  }

  return {
    host,
    user,
    authType,
    port,
    keyPath: keyPath ?? undefined,
    knownHostsPath: knownHostsPath ?? undefined,
  };
}

function mapSourceRow(row: DbRow): Source {
  const rowData = toDbRow(row.row_data) ?? row;
  const metadata = toDbRow(rowData.metadata);

  const accessMode = toSourceAccessMode(
    firstNonEmptyString(
      rowData.access_mode,
      rowData.accessMode,
      metadata?.access_mode,
      metadata?.accessMode
    )
  );
  const syncCron =
    firstNonEmptyString(
      rowData.sync_cron,
      rowData.syncCron,
      metadata?.sync_cron,
      metadata?.syncCron
    ) ?? undefined;
  const syncRetentionDays = toOptionalNonNegativeInteger(
    rowData.sync_retention_days ??
      rowData.syncRetentionDays ??
      metadata?.sync_retention_days ??
      metadata?.syncRetentionDays
  );

  return {
    id: String(rowData.id ?? row.id ?? ""),
    name:
      firstNonEmptyString(rowData.name, metadata?.name, rowData.agent_id, rowData.hostname, rowData.id) ??
      "",
    type: toSourceType(
      firstNonEmptyString(rowData.type, metadata?.type, rowData.source_type, metadata?.source_type)
    ),
    location:
      firstNonEmptyString(rowData.location, metadata?.location, rowData.hostname, rowData.workspace_id) ??
      "",
    sshConfig: mapSSHConfigFromRow(rowData, metadata),
    accessMode,
    syncCron,
    syncRetentionDays,
    enabled: toBoolean(rowData.enabled ?? metadata?.enabled, true),
    createdAt: toIsoString(rowData.created_at ?? row.created_at) ?? new Date().toISOString(),
  };
}

function mapSyncJobRow(row: DbRow): SyncJob {
  const createdAt = toIsoString(row.created_at) ?? new Date().toISOString();
  const startedAt = toIsoString(row.started_at) ?? undefined;
  const endedAt = toIsoString(row.ended_at) ?? undefined;
  const nextRunAt = toIsoString(row.next_run_at ?? row.nextRunAt) ?? undefined;
  const durationMs = toOptionalNonNegativeInteger(row.duration_ms);
  const error = firstNonEmptyString(row.error, row.error_detail) ?? undefined;

  return {
    id: String(row.id ?? ""),
    sourceId: firstNonEmptyString(row.source_id) ?? "",
    mode: toSourceAccessMode(row.mode),
    status: toSyncJobStatus(row.status),
    error,
    trigger: firstNonEmptyString(row.trigger) ?? undefined,
    attempt: toSyncJobAttempt(row.attempt),
    startedAt,
    endedAt,
    nextRunAt,
    durationMs,
    errorCode: firstNonEmptyString(row.error_code) ?? undefined,
    errorDetail: firstNonEmptyString(row.error_detail) ?? undefined,
    cancelRequested: toBoolean(row.cancel_requested, false),
    createdAt,
    updatedAt: toIsoString(row.updated_at) ?? createdAt,
  };
}

function mapSourceWatermarkRow(row: DbRow): SourceWatermark {
  const createdAt = toIsoString(row.created_at) ?? new Date().toISOString();

  return {
    sourceId: firstNonEmptyString(row.source_id) ?? "",
    provider: firstNonEmptyString(row.provider) ?? "unknown",
    watermark: firstNonEmptyString(row.watermark) ?? "",
    createdAt,
    updatedAt: toIsoString(row.updated_at) ?? createdAt,
  };
}

function mapSourceParseFailureRow(row: DbRow): SourceParseFailure {
  const createdAt = toIsoString(row.created_at) ?? new Date().toISOString();

  return {
    id: firstNonEmptyString(row.id) ?? "",
    sourceId: firstNonEmptyString(row.source_id) ?? "",
    parserKey: firstNonEmptyString(row.parser_key) ?? "unknown",
    errorCode: firstNonEmptyString(row.error_code) ?? "unknown",
    errorMessage: firstNonEmptyString(row.error_message, row.message, row.error_detail, row.error) ?? "",
    sourcePath: firstNonEmptyString(row.source_path) ?? undefined,
    sourceOffset:
      row.source_offset === null || row.source_offset === undefined
        ? undefined
        : Math.max(0, Math.trunc(toNumber(row.source_offset, 0))),
    rawHash: firstNonEmptyString(row.raw_hash) ?? undefined,
    metadata: toDbRow(row.metadata) ?? {},
    failedAt: toIsoString(row.occurred_at) ?? createdAt,
    createdAt,
  };
}

function mapSessionRow(row: DbRow): Session {
  const rowData = toDbRow(row.row_data) ?? row;
  const provider = firstNonEmptyString(rowData.provider) ?? "";

  const endedAt =
    rowData.ended_at === null || rowData.ended_at === undefined
      ? null
      : (toIsoString(rowData.ended_at) ?? null);

  return {
    id: String(rowData.id ?? row.id ?? ""),
    sourceId: String(rowData.source_id ?? row.source_id ?? ""),
    tool: firstNonEmptyString(rowData.tool, provider) ?? "",
    model: firstNonEmptyString(rowData.model) ?? "",
    startedAt: toIsoString(rowData.started_at ?? row.started_at) ?? new Date(0).toISOString(),
    endedAt,
    tokens: Math.max(
      0,
      Math.trunc(toNumber(row.tokens ?? rowData.tokens ?? rowData.total_tokens, 0))
    ),
    cost: toNumber(row.cost ?? rowData.cost ?? rowData.cost_usd ?? rowData.total_cost, 0),
  };
}

function mapSessionDetailRow(row: DbRow): SessionDetail {
  const session = mapSessionRow(row);
  const rowData = toDbRow(row.row_data) ?? row;
  const sourceTypeRaw = firstNonEmptyString(rowData.source_type, rowData.type);
  const sourceType = SOURCE_TYPES.includes(sourceTypeRaw as SourceType)
    ? (sourceTypeRaw as SourceType)
    : undefined;

  return {
    ...session,
    provider: firstNonEmptyString(rowData.provider) ?? undefined,
    sourceName: firstNonEmptyString(rowData.source_name, rowData.name) ?? undefined,
    sourceType,
    sourceLocation: firstNonEmptyString(rowData.source_location, rowData.location) ?? undefined,
    sourceHost: firstNonEmptyString(rowData.source_host, rowData.hostname) ?? undefined,
    sourcePath: firstNonEmptyString(rowData.source_path) ?? undefined,
    workspace: firstNonEmptyString(rowData.workspace) ?? undefined,
    messageCount: Math.max(0, Math.trunc(toNumber(rowData.message_count, 0))),
    inputTokens: Math.max(0, Math.trunc(toNumber(rowData.input_tokens, 0))),
    outputTokens: Math.max(0, Math.trunc(toNumber(rowData.output_tokens, 0))),
    cacheReadTokens: Math.max(0, Math.trunc(toNumber(rowData.cache_read_tokens, 0))),
    cacheWriteTokens: Math.max(0, Math.trunc(toNumber(rowData.cache_write_tokens, 0))),
    reasoningTokens: Math.max(0, Math.trunc(toNumber(rowData.reasoning_tokens, 0))),
  };
}

function mapSessionEventRow(row: DbRow): SessionEvent {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    sessionId: firstNonEmptyString(row.session_id) ?? "",
    sourceId: firstNonEmptyString(row.source_id) ?? "",
    eventType: firstNonEmptyString(row.event_type) ?? "message",
    role: firstNonEmptyString(row.role) ?? undefined,
    text: firstNonEmptyString(row.text) ?? undefined,
    model: firstNonEmptyString(row.model) ?? undefined,
    timestamp: toIsoString(row.timestamp) ?? new Date(0).toISOString(),
    inputTokens: Math.max(0, Math.trunc(toNumber(row.input_tokens, 0))),
    outputTokens: Math.max(0, Math.trunc(toNumber(row.output_tokens, 0))),
    cacheReadTokens: Math.max(0, Math.trunc(toNumber(row.cache_read_tokens, 0))),
    cacheWriteTokens: Math.max(0, Math.trunc(toNumber(row.cache_write_tokens, 0))),
    reasoningTokens: Math.max(0, Math.trunc(toNumber(row.reasoning_tokens, 0))),
    cost: Math.max(0, toNumber(row.cost_usd, 0)),
    sourcePath: firstNonEmptyString(row.source_path) ?? undefined,
    sourceOffset: row.source_offset === null ? undefined : Math.max(0, Math.trunc(toNumber(row.source_offset, 0))),
  };
}

function mapHeatmapRow(row: DbRow): HeatmapCell | null {
  const date = toIsoString(row.day) ?? toIsoString(row.date_bucket) ?? toIsoString(row.date);
  if (!date) {
    return null;
  }

  return {
    date,
    tokens: Math.max(0, Math.trunc(toNumber(row.tokens, 0))),
    cost: Number(toNumber(row.cost, 0).toFixed(6)),
    sessions: Math.max(0, Math.trunc(toNumber(row.sessions, 0))),
  };
}

function computeRelativeChange(current: number, previous: number): number | null {
  if (!Number.isFinite(previous) || previous <= 0) {
    return null;
  }
  return Number((((current - previous) / previous)).toFixed(6));
}

function roundUsageCost(value: number): number {
  return Number(Math.max(0, value).toFixed(6));
}

function createUsageCostModeCounters(): UsageCostModeCounters {
  return {
    raw: 0,
    reported: 0,
    estimated: 0,
  };
}

function resolveUsageCostMode(counters: UsageCostModeCounters): UsageCostMode {
  const hasRawLike = counters.raw > 0 || counters.reported > 0;
  if (hasRawLike && counters.estimated > 0) {
    return "mixed";
  }
  if (counters.raw > 0) {
    return "raw";
  }
  if (counters.reported > 0) {
    return "reported";
  }
  if (counters.estimated > 0) {
    return "estimated";
  }
  return "none";
}

function buildUsageCostComponents(
  costRaw: number,
  costEstimated: number,
  modeCounters: UsageCostModeCounters
): UsageCostComponents {
  const normalizedRaw = roundUsageCost(costRaw);
  const normalizedEstimated = roundUsageCost(costEstimated);

  return {
    cost: roundUsageCost(normalizedRaw + normalizedEstimated),
    costRaw: normalizedRaw,
    costEstimated: normalizedEstimated,
    costMode: resolveUsageCostMode(modeCounters),
  };
}

function readCostValueFromRecord(
  record: DbRow | null | undefined,
  keys: readonly string[]
): number | null {
  if (!record) {
    return null;
  }

  for (const key of keys) {
    if (!(key in record)) {
      continue;
    }
    const rawValue = record[key];
    if (rawValue === undefined || rawValue === null || rawValue === "") {
      continue;
    }
    const parsed = toNumber(rawValue, Number.NaN);
    if (Number.isFinite(parsed)) {
      return Math.max(0, parsed);
    }
  }

  return null;
}

function readUsageCostModeHint(record: DbRow | null | undefined): UsageCostMode | null {
  const mode = firstNonEmptyString(record?.cost_mode, record?.costMode);
  if (mode === "raw" || mode === "estimated" || mode === "reported") {
    return mode;
  }
  return null;
}

function resolveSessionUsageCostSnapshotFromRecord(
  record: DbRow | null | undefined,
  legacyCostInput: number
): SessionUsageCostSnapshot {
  const modeCounters = createUsageCostModeCounters();
  const legacyCost = Math.max(0, toNumber(legacyCostInput, 0));
  const rawCost = readCostValueFromRecord(record, ["cost_raw", "raw_cost", "costRaw", "rawCost"]);
  const estimatedCost = readCostValueFromRecord(record, [
    "cost_estimated",
    "estimated_cost",
    "costEstimated",
    "estimatedCost",
  ]);
  const reportedCost = readCostValueFromRecord(record, [
    "cost_reported",
    "reported_cost",
    "costReported",
    "reportedCost",
  ]);
  const modeHint = readUsageCostModeHint(record);

  let costRaw = 0;
  let costEstimated = 0;
  const hasExplicitCost =
    rawCost !== null || estimatedCost !== null || reportedCost !== null;

  if (rawCost !== null) {
    costRaw += rawCost;
    modeCounters.raw += 1;
  }
  if (reportedCost !== null) {
    costRaw += reportedCost;
    if (rawCost === null) {
      modeCounters.reported += 1;
    }
  }
  if (estimatedCost !== null) {
    costEstimated += estimatedCost;
    modeCounters.estimated += 1;
  }

  if (!hasExplicitCost && legacyCost > 0) {
    if (modeHint === "estimated") {
      costEstimated = legacyCost;
      modeCounters.estimated += 1;
    } else if (modeHint === "raw") {
      costRaw = legacyCost;
      modeCounters.raw += 1;
    } else {
      costRaw = legacyCost;
      modeCounters.reported += 1;
    }
  }

  return {
    ...buildUsageCostComponents(costRaw, costEstimated, modeCounters),
    modeCounters,
  };
}

function resolveSessionUsageCostSnapshotFromAggregateRow(
  row: DbRow
): SessionUsageCostSnapshot {
  const hasAny = toBoolean(row.has_any_cost, false);
  if (!hasAny) {
    const payload = toDbRow(row.session_payload) ?? row;
    return resolveSessionUsageCostSnapshotFromRecord(payload, toNumber(row.session_cost, 0));
  }

  const modeCounters = createUsageCostModeCounters();
  const hasRaw = toBoolean(row.has_raw, false);
  const hasReported = toBoolean(row.has_reported, false);
  const hasEstimated = toBoolean(row.has_estimated, false);

  if (hasRaw) {
    modeCounters.raw += 1;
  } else if (hasReported) {
    modeCounters.reported += 1;
  }
  if (hasEstimated) {
    modeCounters.estimated += 1;
  }

  const costRaw =
    Math.max(0, toNumber(row.cost_raw, 0)) + Math.max(0, toNumber(row.cost_reported, 0));
  const costEstimated = hasEstimated ? Math.max(0, toNumber(row.cost_estimated, 0)) : 0;

  return {
    ...buildUsageCostComponents(costRaw, costEstimated, modeCounters),
    modeCounters,
  };
}

function resolveUsageCostFromAggregateRow(row: DbRow): UsageCostComponents {
  const modeCounters: UsageCostModeCounters = {
    raw: Math.max(0, Math.trunc(toNumber(row.raw_count, 0))),
    reported: Math.max(0, Math.trunc(toNumber(row.reported_count, 0))),
    estimated: Math.max(0, Math.trunc(toNumber(row.estimated_count, 0))),
  };

  return buildUsageCostComponents(
    toNumber(row.cost_raw, 0),
    toNumber(row.cost_estimated, 0),
    modeCounters
  );
}

function buildUsageDailyItems(items: UsageDailyBaseItem[]): UsageDailyItem[] {
  const sortedItems = [...items].sort((a, b) => a.date.localeCompare(b.date));

  return sortedItems.map((item, index) => {
    const previous = index > 0 ? sortedItems[index - 1] : undefined;
    return {
      ...item,
      change: {
        tokens: previous ? computeRelativeChange(item.tokens, previous.tokens) : null,
        cost: previous ? computeRelativeChange(item.cost, previous.cost) : null,
        sessions: previous ? computeRelativeChange(item.sessions, previous.sessions) : null,
      },
    };
  });
}

function resolveLatestIsoTimestamp(...values: unknown[]): string | null {
  let latest: string | null = null;
  for (const value of values) {
    const iso = toIsoString(value);
    if (!iso) {
      continue;
    }
    if (!latest || iso > latest) {
      latest = iso;
    }
  }
  return latest;
}

function computeFreshnessMinutes(lastSuccessAt: string | null): number | null {
  if (!lastSuccessAt) {
    return null;
  }

  const successMs = Date.parse(lastSuccessAt);
  if (!Number.isFinite(successMs)) {
    return null;
  }

  const diffMs = Date.now() - successMs;
  if (!Number.isFinite(diffMs)) {
    return null;
  }

  return Math.max(0, Math.trunc(diffMs / 60_000));
}

function toBudgetScope(value: unknown): BudgetScope {
  if (value === "source") {
    return "source";
  }
  if (value === "org") {
    return "org";
  }
  if (value === "user") {
    return "user";
  }
  if (value === "model") {
    return "model";
  }
  return "global";
}

function toBudgetPeriod(value: unknown): BudgetPeriod {
  return value === "daily" ? "daily" : "monthly";
}

function toBudgetGovernanceState(value: unknown): BudgetGovernanceState {
  if (
    typeof value === "string" &&
    BUDGET_GOVERNANCE_STATES.includes(value as BudgetGovernanceState)
  ) {
    return value as BudgetGovernanceState;
  }
  return "active";
}

function toBudgetReleaseRequestStatus(value: unknown): BudgetReleaseRequestStatus {
  if (
    typeof value === "string" &&
    BUDGET_RELEASE_REQUEST_STATUSES.includes(value as BudgetReleaseRequestStatus)
  ) {
    return value as BudgetReleaseRequestStatus;
  }
  return "pending";
}

function toIntegrationAlertCallbackAction(value: unknown): IntegrationAlertCallbackAction {
  if (value === "resolve") {
    return "resolve";
  }
  if (value === "request_release") {
    return "request_release";
  }
  if (value === "approve_release") {
    return "approve_release";
  }
  if (value === "reject_release") {
    return "reject_release";
  }
  return "ack";
}

function toAlertStatus(value: unknown): AlertStatus {
  if (typeof value === "string" && ALERT_STATUS_SET.includes(value as AlertStatus)) {
    return value as AlertStatus;
  }
  return "open";
}

function toAlertSeverity(value: unknown): AlertSeverity {
  if (
    typeof value === "string" &&
    ALERT_SEVERITY_SET.includes(value as AlertSeverity)
  ) {
    return value as AlertSeverity;
  }
  return "warning";
}

function toAlertOrchestrationEventType(value: unknown): AlertOrchestrationEventType {
  if (
    typeof value === "string" &&
    ALERT_ORCHESTRATION_EVENT_TYPES.includes(value as AlertOrchestrationEventType)
  ) {
    return value as AlertOrchestrationEventType;
  }
  return "alert";
}

function toAlertOrchestrationChannel(value: unknown): AlertOrchestrationChannel | null {
  if (
    typeof value === "string" &&
    ALERT_ORCHESTRATION_CHANNELS.includes(value as AlertOrchestrationChannel)
  ) {
    return value as AlertOrchestrationChannel;
  }
  return null;
}

function toDataResidencyMode(value: unknown): DataResidencyMode {
  if (typeof value === "string" && DATA_RESIDENCY_MODES.includes(value as DataResidencyMode)) {
    return value as DataResidencyMode;
  }
  return "single_region";
}

function toReplicationJobStatus(value: unknown): ReplicationJobStatus {
  if (
    typeof value === "string" &&
    REPLICATION_JOB_STATUSES.includes(value as ReplicationJobStatus)
  ) {
    return value as ReplicationJobStatus;
  }
  return "pending";
}

function toRuleLifecycleStatus(value: unknown): RuleLifecycleStatus {
  if (
    typeof value === "string" &&
    RULE_LIFECYCLE_STATUSES.includes(value as RuleLifecycleStatus)
  ) {
    return value as RuleLifecycleStatus;
  }
  return "draft";
}

function toRuleApprovalDecision(value: unknown): RuleApprovalDecision {
  if (
    typeof value === "string" &&
    RULE_APPROVAL_DECISIONS.includes(value as RuleApprovalDecision)
  ) {
    return value as RuleApprovalDecision;
  }
  return "approved";
}

function toMcpRiskLevel(value: unknown): McpRiskLevel {
  if (typeof value === "string" && MCP_RISK_LEVELS.includes(value as McpRiskLevel)) {
    return value as McpRiskLevel;
  }
  return "medium";
}

function toMcpToolDecision(value: unknown): McpToolDecision {
  if (typeof value === "string" && MCP_TOOL_DECISIONS.includes(value as McpToolDecision)) {
    return value as McpToolDecision;
  }
  return "require_approval";
}

function toMcpApprovalStatus(value: unknown): McpApprovalStatus {
  if (
    typeof value === "string" &&
    MCP_APPROVAL_STATUSES.includes(value as McpApprovalStatus)
  ) {
    return value as McpApprovalStatus;
  }
  return "pending";
}

function toReplayJobStatus(value: unknown): ReplayJobStatus {
  if (value === "succeeded" || value === "success") {
    return "completed";
  }
  if (value === "canceled") {
    return "cancelled";
  }
  if (
    typeof value === "string" &&
    REPLAY_JOB_STATUS_SET.includes(value as ReplayJobStatus)
  ) {
    return value as ReplayJobStatus;
  }
  return "pending";
}

function toWebhookReplayTaskStatus(value: unknown): WebhookReplayTaskStatus {
  if (value === "success" || value === "succeeded") {
    return "completed";
  }
  if (value === "canceled") {
    return "failed";
  }
  if (
    typeof value === "string" &&
    WEBHOOK_REPLAY_TASK_STATUS_SET.includes(value as WebhookReplayTaskStatus)
  ) {
    return value as WebhookReplayTaskStatus;
  }
  return "queued";
}

function toWebhookEventType(value: unknown): WebhookEventType | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return undefined;
  }
  return WEBHOOK_EVENT_TYPES.includes(normalized as WebhookEventType)
    ? (normalized as WebhookEventType)
    : undefined;
}

function toAuditLevel(value: unknown): AuditLevel {
  if (typeof value === "string" && AUDIT_LEVEL_SET.includes(value as AuditLevel)) {
    return value as AuditLevel;
  }
  return "info";
}

function normalizeDistinctStringArray(
  values: unknown,
  options: {
    lowerCase?: boolean;
  } = {}
): string[] {
  const normalized: string[] = [];
  const dedupe = new Set<string>();
  const lowerCase = options.lowerCase === true;
  for (const item of Array.isArray(values) ? values : []) {
    const candidate = firstNonEmptyString(item);
    if (!candidate) {
      continue;
    }
    const value = lowerCase ? candidate.toLowerCase() : candidate;
    if (dedupe.has(value)) {
      continue;
    }
    dedupe.add(value);
    normalized.push(value);
  }
  return normalized;
}

function normalizeStringRecord(value: unknown): Record<string, string> {
  const row = toDbRow(value);
  if (!row) {
    return {};
  }

  const normalized: Record<string, string> = {};
  for (const [rawKey, rawValue] of Object.entries(row)) {
    const key = firstNonEmptyString(rawKey);
    if (!key) {
      continue;
    }
    const mappedValue = firstNonEmptyString(rawValue);
    if (!mappedValue) {
      continue;
    }
    normalized[key] = mappedValue;
  }
  return normalized;
}

function normalizeNumericRecord(value: unknown): Record<string, number> {
  const row = toDbRow(value);
  if (!row) {
    return {};
  }
  const normalized: Record<string, number> = {};
  for (const [rawKey, rawValue] of Object.entries(row)) {
    const key = firstNonEmptyString(rawKey);
    if (!key) {
      continue;
    }
    const mappedValue = toNumber(rawValue, Number.NaN);
    if (!Number.isFinite(mappedValue)) {
      continue;
    }
    normalized[key] = Number(mappedValue.toFixed(6));
  }
  return normalized;
}

function normalizeQualityScore(value: unknown): number {
  const mapped = toNumber(value, 0);
  if (!Number.isFinite(mapped)) {
    return 0;
  }
  return Number(Math.max(0, mapped).toFixed(6));
}

function toTenantRole(value: unknown): TenantRole {
  if (typeof value === "string" && TENANT_ROLE_SET.includes(value as TenantRole)) {
    return value as TenantRole;
  }
  return "member";
}

function toOrgRole(value: unknown): OrgRole {
  if (typeof value === "string" && ORG_ROLE_SET.includes(value as OrgRole)) {
    return value as OrgRole;
  }
  return "member";
}

function toAuditMetadata(value: unknown): Record<string, unknown> {
  return toDbRow(value) ?? {};
}

function safeStringifyJson(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return "{}";
  }
}

function isPgForeignKeyViolation(error: unknown): boolean {
  if (typeof error !== "object" || error === null) {
    return false;
  }
  return (error as PgErrorLike).code === "23503";
}

function isPgUniqueViolation(error: unknown): boolean {
  if (typeof error !== "object" || error === null) {
    return false;
  }
  return (error as PgErrorLike).code === "23505";
}

function isPgUndefinedTable(error: unknown): boolean {
  if (typeof error !== "object" || error === null) {
    return false;
  }
  return (error as PgErrorLike).code === "42P01";
}

function canTransitAlertStatus(current: AlertStatus, next: AlertMutableStatus): boolean {
  if (current === next) {
    return true;
  }
  if (current === "open" && (next === "acknowledged" || next === "resolved")) {
    return true;
  }
  if (current === "acknowledged" && next === "resolved") {
    return true;
  }
  return false;
}

function mapBudgetRow(row: DbRow): Budget {
  const scope = toBudgetScope(row.scope);
  const sourceId = firstNonEmptyString(row.source_id);
  const organizationId = firstNonEmptyString(row.organization_id);
  const userId = firstNonEmptyString(row.user_id);
  const model = firstNonEmptyString(row.model_name, row.model);
  const warningThreshold = Number(
    toNumber(row.warning_threshold ?? row.alert_threshold, 0.8).toFixed(4)
  );
  const escalatedThreshold = Number(
    toNumber(row.escalated_threshold ?? row.warning_threshold ?? row.alert_threshold, 0.9).toFixed(
      4
    )
  );
  const criticalThreshold = Number(
    toNumber(
      row.critical_threshold ??
        row.escalated_threshold ??
        row.warning_threshold ??
        row.alert_threshold,
      1
    ).toFixed(4)
  );
  const freezeReason = firstNonEmptyString(row.freeze_reason);
  const frozenAt = toIsoString(row.frozen_at);
  const frozenByAlertId = firstNonEmptyString(row.frozen_by_alert_id);

  return {
    id: String(row.id ?? ""),
    scope,
    sourceId: scope === "source" ? sourceId ?? undefined : undefined,
    organizationId: scope === "org" ? organizationId ?? undefined : undefined,
    userId: scope === "user" ? userId ?? undefined : undefined,
    model: scope === "model" ? model ?? undefined : undefined,
    period: toBudgetPeriod(row.period),
    tokenLimit: Math.max(0, Math.trunc(toNumber(row.token_limit, 0))),
    costLimit: Number(toNumber(row.cost_limit, 0).toFixed(6)),
    thresholds: {
      warning: warningThreshold,
      escalated: escalatedThreshold,
      critical: criticalThreshold,
    },
    alertThreshold: warningThreshold,
    enabled: toBoolean(row.enabled, true),
    governanceState: toBudgetGovernanceState(row.governance_state),
    freezeReason: freezeReason ?? undefined,
    frozenAt: frozenAt ?? undefined,
    frozenByAlertId: frozenByAlertId ?? undefined,
    updatedAt: toIsoString(row.updated_at) ?? new Date().toISOString(),
  };
}

function mapBudgetReleaseRequestRow(row: DbRow): BudgetReleaseRequest {
  const approvals = toJsonArray(row.approvals).flatMap((item) => {
    const record = toDbRow(item);
    if (!record) {
      return [];
    }
    const userId = firstNonEmptyString(record.user_id, record.userId);
    const approvedAt = toIsoString(record.approved_at ?? record.approvedAt);
    if (!userId || !approvedAt) {
      return [];
    }
    return [
      {
        userId,
        email: firstNonEmptyString(record.email) ?? undefined,
        approvedAt,
      },
    ];
  });

  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    budgetId: firstNonEmptyString(row.budget_id) ?? "",
    status: toBudgetReleaseRequestStatus(row.status),
    requestedByUserId: firstNonEmptyString(row.requested_by_user_id) ?? "",
    requestedByEmail: firstNonEmptyString(row.requested_by_email) ?? undefined,
    requestedAt: toIsoString(row.requested_at) ?? new Date().toISOString(),
    approvals,
    rejectedByUserId: firstNonEmptyString(row.rejected_by_user_id) ?? undefined,
    rejectedByEmail: firstNonEmptyString(row.rejected_by_email) ?? undefined,
    rejectedReason: firstNonEmptyString(row.rejected_reason) ?? undefined,
    rejectedAt: toIsoString(row.rejected_at) ?? undefined,
    executedAt: toIsoString(row.executed_at) ?? undefined,
    updatedAt: toIsoString(row.updated_at ?? row.requested_at) ?? new Date().toISOString(),
  };
}

function mapAlertRow(row: DbRow): Alert {
  const sourceId = firstNonEmptyString(row.source_id);
  const windowStart = toIsoString(row.window_start) ?? toIsoString(row.created_at);
  const triggeredAt = toIsoString(row.created_at) ?? new Date().toISOString();

  return {
    id: String(row.id ?? ""),
    tenantId: firstNonEmptyString(row.tenant_id) ?? "default",
    budgetId: firstNonEmptyString(row.budget_id) ?? "",
    sourceId: sourceId ?? undefined,
    period: toBudgetPeriod(row.period),
    windowStart: windowStart ?? triggeredAt,
    windowEnd: toIsoString(row.window_end) ?? windowStart ?? triggeredAt,
    tokensUsed: Math.max(0, Math.trunc(toNumber(row.tokens_used, 0))),
    costUsed: Number(toNumber(row.cost_used, 0).toFixed(6)),
    tokenLimit: Math.max(0, Math.trunc(toNumber(row.token_limit, 0))),
    costLimit: Number(toNumber(row.cost_limit, 0).toFixed(6)),
    threshold: Number(toNumber(row.threshold, 0).toFixed(4)),
    status: toAlertStatus(row.status),
    severity: toAlertSeverity(row.severity),
    triggeredAt,
    updatedAt: toIsoString(row.updated_at ?? row.created_at) ?? new Date().toISOString(),
  };
}

function mapAlertOrchestrationRuleRow(row: DbRow): AlertOrchestrationRule {
  const channels: AlertOrchestrationChannel[] = [];
  const channelSet = new Set<AlertOrchestrationChannel>();
  for (const channel of toJsonArray(row.channels)) {
    const normalized = firstNonEmptyString(channel);
    if (!normalized) {
      continue;
    }
    const mapped = toAlertOrchestrationChannel(normalized.toLowerCase());
    if (!mapped) {
      continue;
    }
    if (channelSet.has(mapped)) {
      continue;
    }
    channelSet.add(mapped);
    channels.push(mapped);
  }

  const sourceId = firstNonEmptyString(row.source_id);
  const severityRaw = firstNonEmptyString(row.severity);
  const severity = severityRaw ? toAlertSeverity(severityRaw) : undefined;
  const slaMinutes = toOptionalNonNegativeInteger(row.sla_minutes);

  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    name: firstNonEmptyString(row.name) ?? "",
    enabled: toBoolean(row.enabled, true),
    eventType: toAlertOrchestrationEventType(row.event_type),
    severity,
    sourceId: sourceId ?? undefined,
    dedupeWindowSeconds: toOptionalNonNegativeInteger(row.dedupe_window_seconds) ?? 0,
    suppressionWindowSeconds: toOptionalNonNegativeInteger(row.suppression_window_seconds) ?? 0,
    mergeWindowSeconds: toOptionalNonNegativeInteger(row.merge_window_seconds) ?? 0,
    slaMinutes,
    channels,
    updatedAt: toIsoString(row.updated_at) ?? new Date().toISOString(),
  };
}

function mapAlertOrchestrationExecutionRow(row: DbRow): AlertOrchestrationExecutionLog {
  const channels: AlertOrchestrationChannel[] = [];
  const channelSet = new Set<AlertOrchestrationChannel>();
  for (const channel of toJsonArray(row.channels)) {
    const normalized = firstNonEmptyString(channel);
    if (!normalized) {
      continue;
    }
    const mapped = toAlertOrchestrationChannel(normalized.toLowerCase());
    if (!mapped) {
      continue;
    }
    if (channelSet.has(mapped)) {
      continue;
    }
    channelSet.add(mapped);
    channels.push(mapped);
  }

  const conflictRuleIds: string[] = [];
  const conflictRuleIdSet = new Set<string>();
  for (const conflictRuleId of toJsonArray(row.conflict_rule_ids)) {
    const normalizedConflictRuleId = firstNonEmptyString(conflictRuleId);
    if (!normalizedConflictRuleId) {
      continue;
    }
    if (conflictRuleIdSet.has(normalizedConflictRuleId)) {
      continue;
    }
    conflictRuleIdSet.add(normalizedConflictRuleId);
    conflictRuleIds.push(normalizedConflictRuleId);
  }

  const alertId = firstNonEmptyString(row.alert_id);
  const severityRaw = firstNonEmptyString(row.severity);
  const sourceId = firstNonEmptyString(row.source_id);
  const metadata = toDbRow(row.metadata) ?? {};

  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    ruleId: firstNonEmptyString(row.rule_id) ?? "",
    eventType: toAlertOrchestrationEventType(row.event_type),
    alertId: alertId ?? undefined,
    severity: severityRaw ? toAlertSeverity(severityRaw) : undefined,
    sourceId: sourceId ?? undefined,
    channels,
    dispatchMode: resolveAlertOrchestrationDispatchMode(metadata),
    conflictRuleIds,
    dedupeHit: toBoolean(row.dedupe_hit, false),
    suppressed: toBoolean(row.suppressed, false),
    simulated: toBoolean(row.simulated, false),
    metadata,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function resolveAlertOrchestrationDispatchMode(
  metadata: DbRow | null | undefined
): AlertOrchestrationDispatchMode {
  const normalizedDispatchMode = firstNonEmptyString(
    metadata?.dispatchMode,
    metadata?.dispatch_mode
  );
  if (normalizedDispatchMode === "fallback") {
    return "fallback";
  }
  if (normalizedDispatchMode === "rule") {
    return "rule";
  }
  return toBoolean(metadata?.fallback, false) ? "fallback" : "rule";
}

function mapRuleScopeBinding(value: unknown): RuleScopeBinding {
  const row = toDbRow(value) ?? {};
  const readList = (field: string): string[] | undefined => {
    const result: string[] = [];
    for (const item of toJsonArray(row[field])) {
      const normalized = firstNonEmptyString(item);
      if (!normalized) {
        continue;
      }
      if (!result.includes(normalized)) {
        result.push(normalized);
      }
    }
    return result.length > 0 ? result : undefined;
  };

  return {
    organizations: readList("organizations"),
    projects: readList("projects"),
    clients: readList("clients"),
  };
}

function mapTenantResidencyPolicyRow(row: DbRow): TenantResidencyPolicy {
  const replicaRegions = toJsonArray(row.replica_regions).flatMap((value) => {
    const normalized = firstNonEmptyString(value);
    return normalized ? [normalized] : [];
  });

  return {
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    mode: toDataResidencyMode(row.mode),
    primaryRegion: firstNonEmptyString(row.primary_region) ?? "cn-hangzhou",
    replicaRegions,
    allowCrossRegionTransfer: toBoolean(row.allow_cross_region_transfer, false),
    requireTransferApproval: toBoolean(row.require_transfer_approval, false),
    updatedAt: toIsoString(row.updated_at) ?? new Date().toISOString(),
  };
}

function mapReplicationJobRow(row: DbRow): ReplicationJob {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    sourceRegion: firstNonEmptyString(row.source_region) ?? "",
    targetRegion: firstNonEmptyString(row.target_region) ?? "",
    status: toReplicationJobStatus(row.status),
    reason: firstNonEmptyString(row.reason) ?? undefined,
    createdByUserId: firstNonEmptyString(row.created_by_user_id) ?? undefined,
    approvedByUserId: firstNonEmptyString(row.approved_by_user_id) ?? undefined,
    metadata: toDbRow(row.metadata) ?? {},
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
    startedAt: toIsoString(row.started_at) ?? undefined,
    finishedAt: toIsoString(row.finished_at) ?? undefined,
  };
}

function mapRuleAssetRow(row: DbRow): RuleAsset {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    name: firstNonEmptyString(row.name) ?? "",
    description: firstNonEmptyString(row.description) ?? undefined,
    status: toRuleLifecycleStatus(row.status),
    latestVersion: Math.max(0, Math.trunc(toNumber(row.latest_version, 0))),
    publishedVersion: toOptionalNonNegativeInteger(row.published_version),
    scopeBinding: mapRuleScopeBinding(row.scope_binding),
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapRuleAssetVersionRow(row: DbRow): RuleAssetVersion {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    assetId: firstNonEmptyString(row.asset_id) ?? "",
    version: Math.max(1, Math.trunc(toNumber(row.version, 1))),
    content: firstNonEmptyString(row.content) ?? "",
    changelog: firstNonEmptyString(row.changelog) ?? undefined,
    createdByUserId: firstNonEmptyString(row.created_by_user_id) ?? undefined,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapRuleApprovalRow(row: DbRow): RuleApproval {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    assetId: firstNonEmptyString(row.asset_id) ?? "",
    version: Math.max(1, Math.trunc(toNumber(row.version, 1))),
    approverUserId: firstNonEmptyString(row.approver_user_id) ?? "",
    approverEmail: firstNonEmptyString(row.approver_email) ?? undefined,
    decision: toRuleApprovalDecision(row.decision),
    reason: firstNonEmptyString(row.reason) ?? undefined,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapMcpToolPolicyRow(row: DbRow): McpToolPolicy {
  return {
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    toolId: firstNonEmptyString(row.tool_id) ?? "",
    riskLevel: toMcpRiskLevel(row.risk_level),
    decision: toMcpToolDecision(row.decision),
    reason: firstNonEmptyString(row.reason) ?? undefined,
    updatedAt: toIsoString(row.updated_at) ?? new Date().toISOString(),
  };
}

function mapMcpApprovalRequestRow(row: DbRow): McpApprovalRequest {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    toolId: firstNonEmptyString(row.tool_id) ?? "",
    status: toMcpApprovalStatus(row.status),
    requestedByUserId: firstNonEmptyString(row.requested_by_user_id) ?? "",
    requestedByEmail: firstNonEmptyString(row.requested_by_email) ?? undefined,
    reason: firstNonEmptyString(row.reason) ?? undefined,
    reviewedByUserId: firstNonEmptyString(row.reviewed_by_user_id) ?? undefined,
    reviewedByEmail: firstNonEmptyString(row.reviewed_by_email) ?? undefined,
    reviewReason: firstNonEmptyString(row.review_reason) ?? undefined,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapMcpInvocationAuditRow(row: DbRow): McpInvocationAudit {
  const evaluatedDecisionRaw = firstNonEmptyString(row.evaluated_decision);
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    toolId: firstNonEmptyString(row.tool_id) ?? "",
    decision: toMcpToolDecision(row.decision),
    result:
      firstNonEmptyString(row.result) === "blocked"
        ? "blocked"
        : firstNonEmptyString(row.result) === "approved"
          ? "approved"
          : "allowed",
    approvalRequestId: firstNonEmptyString(row.approval_request_id) ?? undefined,
    enforced: toBoolean(row.enforced, false),
    evaluatedDecision: evaluatedDecisionRaw ? toMcpToolDecision(evaluatedDecisionRaw) : undefined,
    metadata: toDbRow(row.metadata) ?? {},
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapApiKeyRow(row: DbRow): ApiKey {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    name: firstNonEmptyString(row.name, row.id) ?? "",
    keyHash: firstNonEmptyString(row.key_hash) ?? "",
    scopes: normalizeDistinctStringArray(toJsonArray(row.scopes)),
    lastUsedAt: toIsoString(row.last_used_at) ?? undefined,
    revokedAt: toIsoString(row.revoked_at) ?? undefined,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapWebhookEndpointRow(row: DbRow): WebhookEndpoint {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    name: firstNonEmptyString(row.name, row.id) ?? "",
    url: firstNonEmptyString(row.url) ?? "",
    enabled: toBoolean(row.enabled, true),
    eventTypes: normalizeDistinctStringArray(toJsonArray(row.event_types), { lowerCase: true }),
    secretHash: firstNonEmptyString(row.secret_hash) ?? undefined,
    secretCiphertext: firstNonEmptyString(row.secret_ciphertext) ?? undefined,
    headers: normalizeStringRecord(row.headers),
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function normalizeWebhookReplayFilters(value: unknown): WebhookReplayFilters {
  const record = toDbRow(value) ?? {};
  const limitRaw = toOptionalNonNegativeInteger(record.limit);
  const limit = Math.min(500, Math.max(1, limitRaw ?? DEFAULT_WEBHOOK_REPLAY_TASK_LIMIT));
  return {
    eventType: firstNonEmptyString(record.eventType) ?? undefined,
    from: toIsoString(record.from) ?? undefined,
    to: toIsoString(record.to) ?? undefined,
    limit,
  };
}

function mapWebhookReplayTaskRow(row: DbRow): WebhookReplayTask {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    webhookId: firstNonEmptyString(row.webhook_id) ?? "",
    status: toWebhookReplayTaskStatus(row.status),
    dryRun: toBoolean(row.dry_run, true),
    filters: normalizeWebhookReplayFilters(row.filters),
    result: toDbRow(row.result) ?? {},
    error: firstNonEmptyString(row.error) ?? undefined,
    requestedAt: toIsoString(row.requested_at) ?? new Date().toISOString(),
    startedAt: toIsoString(row.started_at) ?? undefined,
    finishedAt: toIsoString(row.finished_at) ?? undefined,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function normalizeQualityExternalSourceMetadata(
  value: unknown
): QualityExternalSourceMetadata | undefined {
  const row = toDbRow(value);
  if (!row) {
    return undefined;
  }
  const provider = firstNonEmptyString(row.provider)?.toLowerCase();
  if (!provider) {
    return undefined;
  }
  const repo = firstNonEmptyString(row.repo)?.toLowerCase() ?? undefined;
  const workflow = firstNonEmptyString(row.workflow) ?? undefined;
  const runId = firstNonEmptyString(row.runId) ?? undefined;
  return {
    provider,
    repo,
    workflow,
    runId,
  };
}

function extractQualityExternalSourceFromMetadata(value: unknown): QualityExternalSourceMetadata | undefined {
  const metadata = toDbRow(value);
  if (!metadata) {
    return undefined;
  }
  const nested = toDbRow(metadata.externalSource);
  const merged = {
    provider: nested?.provider ?? metadata.provider,
    repo: nested?.repo ?? metadata.repo,
    workflow: nested?.workflow ?? metadata.workflow,
    runId: nested?.runId ?? metadata.runId ?? metadata.run_id,
  };
  return normalizeQualityExternalSourceMetadata(merged);
}

function mergeQualityExternalSourceIntoMetadata(
  metadata: Record<string, unknown>,
  externalSource: QualityExternalSourceMetadata | undefined
): Record<string, unknown> {
  if (!externalSource) {
    return { ...metadata };
  }
  return {
    ...metadata,
    externalSource: {
      provider: externalSource.provider,
      ...(externalSource.repo ? { repo: externalSource.repo } : {}),
      ...(externalSource.workflow ? { workflow: externalSource.workflow } : {}),
      ...(externalSource.runId ? { runId: externalSource.runId } : {}),
    },
  };
}

function mapQualityEventRow(row: DbRow): QualityEvent {
  const metadata = toDbRow(row.metadata) ?? {};
  const externalSource =
    normalizeQualityExternalSourceMetadata({
      provider: row.provider,
      repo: row.repository,
      workflow: row.workflow,
      runId: row.run_id,
    }) ?? extractQualityExternalSourceFromMetadata(metadata);

  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    scorecardKey: firstNonEmptyString(row.scorecard_key) ?? "unknown",
    metricKey: firstNonEmptyString(row.metric_key) ?? undefined,
    externalSource,
    score: normalizeQualityScore(row.score),
    passed: toBoolean(row.passed, false),
    metadata: mergeQualityExternalSourceIntoMetadata(metadata, externalSource),
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapQualityScorecardRow(row: DbRow): QualityScorecard {
  return {
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    scorecardKey: firstNonEmptyString(row.scorecard_key) ?? "unknown",
    title: firstNonEmptyString(row.title, row.scorecard_key) ?? "unknown",
    description: firstNonEmptyString(row.description) ?? undefined,
    score: normalizeQualityScore(row.score),
    dimensions: normalizeNumericRecord(row.dimensions),
    metadata: toDbRow(row.metadata) ?? {},
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapReplayBaselineRow(row: DbRow): ReplayBaseline {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    name: firstNonEmptyString(row.name, row.id) ?? "",
    description: firstNonEmptyString(row.description) ?? undefined,
    datasetRef: firstNonEmptyString(row.dataset_ref) ?? undefined,
    scenarioCount: Math.max(0, Math.trunc(toNumber(row.scenario_count, 0))),
    metadata: toDbRow(row.metadata) ?? {},
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapReplayDatasetRow(row: DbRow): ReplayDataset {
  const metadata = toDbRow(row.metadata) ?? {};
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    name: firstNonEmptyString(row.name, row.id) ?? "",
    description: firstNonEmptyString(row.description) ?? undefined,
    model: firstNonEmptyString(row.model, metadata.model) ?? "unknown",
    promptVersion:
      firstNonEmptyString(row.prompt_version, metadata.promptVersion, metadata.prompt_version) ??
      undefined,
    externalDatasetId:
      firstNonEmptyString(
        row.external_dataset_id,
        row.dataset_ref,
        metadata.datasetId,
        metadata.datasetRef
      ) ?? undefined,
    caseCount: Math.max(0, Math.trunc(toNumber(row.case_count, toNumber(row.scenario_count, 0)))),
    metadata,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapReplayDatasetCaseRow(row: DbRow): ReplayDatasetCase {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    datasetId: firstNonEmptyString(row.dataset_id) ?? "",
    caseId: firstNonEmptyString(row.case_id, row.id) ?? "",
    sortOrder: Math.max(0, Math.trunc(toNumber(row.sort_order, 0))),
    input: firstNonEmptyString(row.input_text, row.input) ?? "",
    expectedOutput: firstNonEmptyString(row.expected_output) ?? undefined,
    baselineOutput: firstNonEmptyString(row.baseline_output) ?? undefined,
    candidateInput: firstNonEmptyString(row.candidate_input) ?? undefined,
    metadata: toDbRow(row.metadata) ?? {},
    checksum: firstNonEmptyString(row.checksum) ?? undefined,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapReplayJobRow(row: DbRow): ReplayJob {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    baselineId: firstNonEmptyString(row.baseline_id) ?? "",
    status: toReplayJobStatus(row.status),
    parameters: toDbRow(row.parameters) ?? {},
    summary: toDbRow(row.summary_payload ?? row.summary) ?? {},
    diff: toDbRow(row.diff_payload ?? row.diff) ?? {},
    error: firstNonEmptyString(row.error) ?? undefined,
    startedAt: toIsoString(row.started_at) ?? undefined,
    finishedAt: toIsoString(row.finished_at) ?? undefined,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapReplayRunRow(row: DbRow): ReplayRun {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    datasetId: firstNonEmptyString(row.dataset_id, row.baseline_id) ?? "",
    status: toReplayJobStatus(row.status),
    parameters: toDbRow(row.parameters) ?? {},
    summary: toDbRow(row.summary_payload ?? row.summary) ?? {},
    diff: toDbRow(row.diff_payload ?? row.diff) ?? {},
    error: firstNonEmptyString(row.error) ?? undefined,
    startedAt: toIsoString(row.started_at) ?? undefined,
    finishedAt: toIsoString(row.finished_at) ?? undefined,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function toReplayArtifactType(value: unknown): ReplayArtifactType {
  if (value === "summary" || value === "diff" || value === "cases") {
    return value;
  }
  return "summary";
}

function toReplayArtifactStorageBackend(value: unknown): ReplayArtifactStorageBackend {
  if (value === "local" || value === "object" || value === "hybrid") {
    return value;
  }
  return "local";
}

function mapReplayArtifactRow(row: DbRow): ReplayArtifact {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    runId: firstNonEmptyString(row.run_id) ?? "",
    datasetId: firstNonEmptyString(row.dataset_id) ?? "",
    artifactType: toReplayArtifactType(row.artifact_type),
    name: firstNonEmptyString(row.name) ?? "artifact",
    description: firstNonEmptyString(row.description) ?? undefined,
    contentType: firstNonEmptyString(row.content_type) ?? "application/octet-stream",
    byteSize: Math.max(0, Math.trunc(toNumber(row.byte_size, 0))),
    checksum: firstNonEmptyString(row.checksum) ?? undefined,
    storageBackend: toReplayArtifactStorageBackend(row.storage_backend),
    storageKey: firstNonEmptyString(row.storage_key) ?? "",
    metadata: toDbRow(row.metadata) ?? {},
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapReplayDatasetToBaseline(dataset: ReplayDataset): ReplayBaseline {
  return {
    id: dataset.id,
    tenantId: dataset.tenantId,
    name: dataset.name,
    description: dataset.description,
    datasetRef: dataset.externalDatasetId,
    scenarioCount: dataset.caseCount,
    metadata: {
      model: dataset.model,
      ...(dataset.promptVersion ? { promptVersion: dataset.promptVersion } : {}),
      ...dataset.metadata,
    },
    createdAt: dataset.createdAt,
    updatedAt: dataset.updatedAt,
  };
}

function mapReplayBaselineToDataset(input: {
  tenantId: string;
  id: string;
  name: string;
  description?: string;
  datasetRef?: string;
  scenarioCount: number;
  metadata: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}): ReplayDataset {
  return {
    id: input.id,
    tenantId: input.tenantId,
    name: input.name,
    description: input.description,
    model: firstNonEmptyString(input.metadata.model) ?? "unknown",
    promptVersion: firstNonEmptyString(
      input.metadata.promptVersion,
      input.metadata.prompt_version
    ) ?? undefined,
    externalDatasetId: input.datasetRef,
    caseCount: input.scenarioCount,
    metadata: { ...input.metadata },
    createdAt: input.createdAt,
    updatedAt: input.updatedAt,
  };
}

function mapReplayRunToJob(run: ReplayRun): ReplayJob {
  return {
    id: run.id,
    tenantId: run.tenantId,
    baselineId: run.datasetId,
    status: run.status,
    parameters: { ...run.parameters },
    summary: { ...run.summary },
    diff: { ...run.diff },
    error: run.error,
    startedAt: run.startedAt,
    finishedAt: run.finishedAt,
    createdAt: run.createdAt,
    updatedAt: run.updatedAt,
  };
}

function mapReplayJobToRun(input: ReplayJob): ReplayRun {
  return {
    id: input.id,
    tenantId: input.tenantId,
    datasetId: input.baselineId,
    status: input.status,
    parameters: { ...input.parameters },
    summary: { ...input.summary },
    diff: { ...input.diff },
    error: input.error,
    startedAt: input.startedAt,
    finishedAt: input.finishedAt,
    createdAt: input.createdAt,
    updatedAt: input.updatedAt,
  };
}

function mapAuditRow(row: DbRow): AuditItem {
  const rowData = toDbRow(row.row_data);
  const metadata = toDbRow(row.metadata) ?? toDbRow(rowData?.metadata) ?? {};

  return {
    id: String(row.id ?? ""),
    eventId: firstNonEmptyString(row.event_id) ?? "",
    action: firstNonEmptyString(row.action) ?? "",
    level: toAuditLevel(row.level),
    detail: firstNonEmptyString(row.detail) ?? "",
    metadata,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapUserRow(row: DbRow): LocalUser {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    email: normalizeEmail(firstNonEmptyString(row.email) ?? ""),
    passwordHash: firstNonEmptyString(row.password_hash) ?? "",
    displayName: firstNonEmptyString(row.display_name, row.email) ?? "",
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapTenantRow(row: DbRow): Tenant {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    name: firstNonEmptyString(row.name, row.id) ?? "",
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapOrganizationRow(row: DbRow): Organization {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    name: firstNonEmptyString(row.name, row.id) ?? "",
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapTenantMemberRow(row: DbRow): TenantMember {
  const organizationId = firstNonEmptyString(row.organization_id);
  const tenantRole = toTenantRole(firstNonEmptyString(row.role, row.tenant_role));
  const orgRole = organizationId
    ? toOrgRole(firstNonEmptyString(row.org_role, row.orgRole))
    : undefined;

  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    userId: firstNonEmptyString(row.user_id) ?? "",
    tenantRole,
    organizationId: organizationId ?? undefined,
    orgRole,
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapAuthSessionRow(row: DbRow): AuthSession {
  return {
    id: firstNonEmptyString(row.id) ?? "",
    userId: firstNonEmptyString(row.user_id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    sessionToken: firstNonEmptyString(row.session_token) ?? "",
    expiresAt: toIsoString(row.expires_at) ?? new Date().toISOString(),
    revokedAt: toIsoString(row.revoked_at),
    replacedBySessionId: firstNonEmptyString(row.replaced_by_session_id),
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapDeviceBindingRow(row: DbRow): DeviceBinding {
  const displayName = firstNonEmptyString(row.display_name);
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    deviceId: firstNonEmptyString(row.device_id) ?? "",
    displayName: displayName ?? undefined,
    metadata: toDbRow(row.metadata) ?? {},
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapAgentBindingRow(row: DbRow): AgentBinding {
  const deviceId = firstNonEmptyString(row.device_id);
  const displayName = firstNonEmptyString(row.display_name);
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    agentId: firstNonEmptyString(row.agent_id) ?? "",
    deviceId: deviceId ?? undefined,
    displayName: displayName ?? undefined,
    metadata: toDbRow(row.metadata) ?? {},
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function mapSourceBindingRow(row: DbRow): SourceBinding {
  const deviceId = firstNonEmptyString(row.device_id);
  const agentId = firstNonEmptyString(row.agent_id);
  const bindingType = toSourceBindingMethod(firstNonEmptyString(row.binding_type));
  const accessMode = toSourceAccessMode(firstNonEmptyString(row.access_mode));
  return {
    id: firstNonEmptyString(row.id) ?? "",
    tenantId: firstNonEmptyString(row.tenant_id) ?? DEFAULT_TENANT_ID,
    sourceId: firstNonEmptyString(row.source_id) ?? "",
    deviceId: deviceId ?? undefined,
    agentId: agentId ?? undefined,
    bindingType,
    accessMode,
    metadata: toDbRow(row.metadata) ?? {},
    createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
    updatedAt: toIsoString(row.updated_at) ?? toIsoString(row.created_at) ?? new Date().toISOString(),
  };
}

function cloneDeviceBinding(binding: DeviceBinding): DeviceBinding {
  return {
    ...binding,
    metadata: { ...binding.metadata },
  };
}

function cloneAgentBinding(binding: AgentBinding): AgentBinding {
  return {
    ...binding,
    metadata: { ...binding.metadata },
  };
}

function cloneSourceBinding(binding: SourceBinding): SourceBinding {
  return {
    ...binding,
    metadata: { ...binding.metadata },
  };
}

interface TimePaginationCursor {
  timestamp: string;
  id: string;
}

function encodeTimePaginationCursor(input: TimePaginationCursor): string {
  return Buffer.from(JSON.stringify(input), "utf8").toString("base64url");
}

function decodeTimePaginationCursor(value: string | undefined): TimePaginationCursor | null {
  const raw = firstNonEmptyString(value);
  if (!raw) {
    return null;
  }

  let decoded: unknown;
  try {
    decoded = JSON.parse(Buffer.from(raw, "base64url").toString("utf8"));
  } catch {
    return null;
  }

  if (typeof decoded !== "object" || decoded === null) {
    return null;
  }
  const record = decoded as Record<string, unknown>;
  const timestamp = firstNonEmptyString(record.timestamp);
  const id = firstNonEmptyString(record.id);
  if (!timestamp || !id) {
    return null;
  }
  if (!Number.isFinite(Date.parse(timestamp))) {
    return null;
  }
  return { timestamp, id };
}

function normalizeSessionSearchInput(
  input: SessionSearchInput,
  tenantId?: string
): NormalizedSessionSearchInput {
  return {
    tenantId: firstNonEmptyString(tenantId) ?? undefined,
    sourceId: firstNonEmptyString(input.sourceId) ?? undefined,
    keyword: firstNonEmptyString(input.keyword) ?? undefined,
    clientType: firstNonEmptyString(input.clientType) ?? undefined,
    tool: firstNonEmptyString(input.tool) ?? undefined,
    host: firstNonEmptyString(input.host) ?? undefined,
    model: firstNonEmptyString(input.model) ?? undefined,
    project: firstNonEmptyString(input.project) ?? undefined,
    from: input.from,
    to: input.to,
    limit: input.limit ?? DEFAULT_SESSION_LIMIT,
    cursor: firstNonEmptyString(input.cursor) ?? undefined,
  };
}

function normalizeSourceParseFailureQueryInput(
  input: SourceParseFailureQueryInput | undefined
): NormalizedSourceParseFailureQueryInput {
  const normalizedFrom = toIsoString(input?.from);
  const normalizedTo = toIsoString(input?.to);
  const rawLimit =
    typeof input?.limit === "number" && Number.isFinite(input.limit)
      ? Math.trunc(input.limit)
      : DEFAULT_PARSE_FAILURE_LIMIT;
  const limit = Math.max(1, Math.min(rawLimit, MAX_PARSE_FAILURE_LIMIT));

  return {
    from: normalizedFrom ?? undefined,
    to: normalizedTo ?? undefined,
    parserKey: firstNonEmptyString(input?.parserKey) ?? undefined,
    errorCode: firstNonEmptyString(input?.errorCode) ?? undefined,
    limit,
  };
}

function normalizeAlertListInput(input: AlertListInput): NormalizedAlertListInput {
  return {
    status: input.status,
    severity: input.severity,
    sourceId: input.sourceId,
    from: input.from,
    to: input.to,
    limit: input.limit ?? DEFAULT_ALERT_LIMIT,
    cursor: firstNonEmptyString(input.cursor) ?? undefined,
  };
}

function normalizeAlertOrchestrationRuleListInput(
  input: AlertOrchestrationRuleListInput | undefined
): NormalizedAlertOrchestrationRuleListInput {
  return {
    eventType: input?.eventType,
    enabled: typeof input?.enabled === "boolean" ? input.enabled : undefined,
    severity: input?.severity,
    sourceId: firstNonEmptyString(input?.sourceId) ?? undefined,
  };
}

function normalizeAlertOrchestrationExecutionListInput(
  input: AlertOrchestrationExecutionListInput = {}
): NormalizedAlertOrchestrationExecutionListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(
    MAX_ALERT_ORCHESTRATION_EXECUTION_LIMIT,
    Math.max(1, rawLimit ?? DEFAULT_ALERT_ORCHESTRATION_EXECUTION_LIMIT)
  );
  const from = toIsoString(input.from);
  const to = toIsoString(input.to);

  return {
    ruleId: firstNonEmptyString(input.ruleId) ?? undefined,
    eventType: input.eventType,
    alertId: firstNonEmptyString(input.alertId) ?? undefined,
    severity: input.severity,
    sourceId: firstNonEmptyString(input.sourceId) ?? undefined,
    dedupeHit: typeof input.dedupeHit === "boolean" ? input.dedupeHit : undefined,
    suppressed: typeof input.suppressed === "boolean" ? input.suppressed : undefined,
    dispatchMode:
      input.dispatchMode === "rule" || input.dispatchMode === "fallback"
        ? input.dispatchMode
        : undefined,
    hasConflict: typeof input.hasConflict === "boolean" ? input.hasConflict : undefined,
    simulated: typeof input.simulated === "boolean" ? input.simulated : undefined,
    from: from ?? undefined,
    to: to ?? undefined,
    limit,
  };
}

function normalizeReplicationJobListInput(
  input: ReplicationJobListInput = {}
): NormalizedReplicationJobListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(200, Math.max(1, rawLimit ?? 50));
  return {
    status: input.status,
    sourceRegion: firstNonEmptyString(input.sourceRegion) ?? undefined,
    targetRegion: firstNonEmptyString(input.targetRegion) ?? undefined,
    limit,
  };
}

function normalizeRuleAssetListInput(input: RuleAssetListInput = {}): NormalizedRuleAssetListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(200, Math.max(1, rawLimit ?? 50));
  return {
    status: input.status,
    keyword: firstNonEmptyString(input.keyword) ?? undefined,
    limit,
  };
}

function normalizeRuleApprovalListInput(
  input: RuleApprovalListInput = {}
): NormalizedRuleApprovalListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(200, Math.max(1, rawLimit ?? 50));
  return {
    version: toOptionalNonNegativeInteger(input.version),
    decision: input.decision,
    limit,
  };
}

function normalizeMcpToolPolicyListInput(
  input: McpToolPolicyListInput = {}
): NormalizedMcpToolPolicyListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(200, Math.max(1, rawLimit ?? 50));
  return {
    riskLevel: input.riskLevel,
    decision: input.decision,
    keyword: firstNonEmptyString(input.keyword) ?? undefined,
    limit,
  };
}

function normalizeMcpInvocationListInput(
  input: McpInvocationListInput = {}
): NormalizedMcpInvocationListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(200, Math.max(1, rawLimit ?? 50));
  const from = toIsoString(input.from);
  const to = toIsoString(input.to);
  return {
    toolId: firstNonEmptyString(input.toolId) ?? undefined,
    decision: input.decision,
    from: from ?? undefined,
    to: to ?? undefined,
    limit,
  };
}

function normalizeAuditListInput(
  input: AuditListQueryInput,
  tenantId?: string
): NormalizedAuditListInput {
  return {
    tenantId: firstNonEmptyString(tenantId) ?? undefined,
    eventId: input.eventId,
    action: input.action,
    level: input.level,
    keyword: input.keyword,
    from: input.from,
    to: input.to,
    limit: input.limit ?? DEFAULT_AUDIT_LIMIT,
    cursor: firstNonEmptyString(input.cursor) ?? undefined,
  };
}

function normalizeQualityDailyMetricsInput(
  input: ListQualityDailyMetricsInput = {}
): NormalizedQualityDailyMetricsInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(366, Math.max(1, rawLimit ?? DEFAULT_QUALITY_DAILY_METRIC_LIMIT));
  const groupByRaw = firstNonEmptyString((input as { groupBy?: unknown }).groupBy);
  const groupBy =
    groupByRaw &&
    Object.prototype.hasOwnProperty.call(QUALITY_EXTERNAL_GROUP_BY_TO_COLUMN, groupByRaw)
      ? (groupByRaw as QualityExternalMetricGroupBy)
      : undefined;
  return {
    from: toIsoString(input.from) ?? undefined,
    to: toIsoString(input.to) ?? undefined,
    scorecardKey: firstNonEmptyString(input.scorecardKey) ?? undefined,
    provider: normalizeQualityExternalFilterValue(input.provider, { lowerCase: true }),
    repo: normalizeQualityExternalFilterValue(input.repo, { lowerCase: true }),
    workflow: normalizeQualityExternalFilterValue(input.workflow),
    runId: normalizeQualityExternalFilterValue(input.runId),
    groupBy,
    limit,
  };
}

function normalizeQualityExternalMetricGroupsInput(
  input: ListQualityExternalMetricGroupsInput
): NormalizedQualityExternalMetricGroupsInput {
  const normalized = normalizeQualityDailyMetricsInput(input);
  return {
    from: normalized.from,
    to: normalized.to,
    scorecardKey: normalized.scorecardKey,
    provider: normalized.provider,
    repo: normalized.repo,
    workflow: normalized.workflow,
    runId: normalized.runId,
    groupBy: input.groupBy,
    limit: normalized.limit,
  };
}

function normalizeQualityExternalFilterValue(
  value: unknown,
  options: { lowerCase?: boolean } = {}
): string | undefined {
  const normalized = firstNonEmptyString(value) ?? undefined;
  if (!normalized) {
    return undefined;
  }
  if (options.lowerCase) {
    return normalized.toLowerCase();
  }
  return normalized;
}

function normalizeQualityScorecardListInput(
  input: ListQualityScorecardsInput = {}
): NormalizedQualityScorecardListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(200, Math.max(1, rawLimit ?? DEFAULT_QUALITY_SCORECARD_LIMIT));
  return {
    scorecardKey: firstNonEmptyString(input.scorecardKey) ?? undefined,
    limit,
  };
}

function normalizeReplayBaselineListInput(
  input: ListReplayBaselinesInput = {}
): NormalizedReplayBaselineListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(200, Math.max(1, rawLimit ?? DEFAULT_REPLAY_BASELINE_LIMIT));
  return {
    keyword: firstNonEmptyString(input.keyword) ?? undefined,
    limit,
  };
}

function normalizeReplayDatasetListInput(
  input: ListReplayDatasetsInput = {}
): NormalizedReplayDatasetListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(200, Math.max(1, rawLimit ?? DEFAULT_REPLAY_BASELINE_LIMIT));
  return {
    keyword: firstNonEmptyString(input.keyword) ?? undefined,
    limit,
  };
}

function normalizeReplayDatasetCaseListInput(
  input: ListReplayDatasetCasesInput = {}
): NormalizedReplayDatasetCaseListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  return {
    limit: Math.min(5000, Math.max(1, rawLimit ?? DEFAULT_REPLAY_DATASET_CASE_LIMIT)),
  };
}

function normalizeReplayJobListInput(
  input: ListReplayJobsInput = {}
): NormalizedReplayJobListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(200, Math.max(1, rawLimit ?? DEFAULT_REPLAY_JOB_LIMIT));
  const statusRaw = firstNonEmptyString(input.status);
  return {
    baselineId: firstNonEmptyString(input.baselineId) ?? undefined,
    status: statusRaw ? toReplayJobStatus(statusRaw) : undefined,
    limit,
  };
}

function normalizeReplayRunListInput(
  input: ListReplayRunsInput = {}
): NormalizedReplayRunListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(200, Math.max(1, rawLimit ?? DEFAULT_REPLAY_JOB_LIMIT));
  const statusRaw = firstNonEmptyString(input.status);
  return {
    datasetId: firstNonEmptyString(input.datasetId) ?? undefined,
    status: statusRaw ? toReplayJobStatus(statusRaw) : undefined,
    limit,
  };
}

function normalizeReplayArtifactListInput(
  input: ListReplayArtifactsInput = {}
): NormalizedReplayArtifactListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  return {
    limit: Math.min(100, Math.max(1, rawLimit ?? DEFAULT_REPLAY_ARTIFACT_LIMIT)),
  };
}

function normalizeWebhookReplayTaskListInput(
  input: ListWebhookReplayTasksInput = {}
): NormalizedWebhookReplayTaskListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(500, Math.max(1, rawLimit ?? DEFAULT_WEBHOOK_REPLAY_TASK_LIMIT));
  const statusRaw = firstNonEmptyString(input.status);
  return {
    webhookId: firstNonEmptyString(input.webhookId) ?? undefined,
    status:
      statusRaw &&
      WEBHOOK_REPLAY_TASK_STATUS_SET.includes(statusRaw as WebhookReplayTaskStatus)
        ? (statusRaw as WebhookReplayTaskStatus)
        : undefined,
    limit,
    cursor: firstNonEmptyString(input.cursor) ?? undefined,
  };
}

function normalizeWebhookReplayEventListInput(
  input: ListWebhookReplayEventsInput
): NormalizedWebhookReplayEventListInput {
  const rawLimit = toOptionalNonNegativeInteger(input.limit);
  const limit = Math.min(500, Math.max(1, rawLimit ?? DEFAULT_WEBHOOK_REPLAY_EVENT_LIMIT));
  const eventTypes = normalizeDistinctStringArray(input.eventTypes, { lowerCase: true })
    .map((item) => toWebhookEventType(item))
    .filter((item): item is WebhookEventType => Boolean(item));

  return {
    eventTypes,
    from: toIsoString(input.from) ?? undefined,
    to: toIsoString(input.to) ?? undefined,
    limit,
  };
}

function compareWebhookReplayEventDesc(left: WebhookReplayEvent, right: WebhookReplayEvent): number {
  const leftTs = Date.parse(left.occurredAt);
  const rightTs = Date.parse(right.occurredAt);
  if (Number.isFinite(leftTs) && Number.isFinite(rightTs) && leftTs !== rightTs) {
    return rightTs - leftTs;
  }
  if (left.occurredAt !== right.occurredAt) {
    return right.occurredAt.localeCompare(left.occurredAt);
  }
  return right.id.localeCompare(left.id);
}

function normalizeHeatmapTimezone(value: string | undefined): string {
  const candidate = firstNonEmptyString(value);
  if (!candidate) {
    return DEFAULT_HEATMAP_TIMEZONE;
  }

  try {
    new Intl.DateTimeFormat("en-US", { timeZone: candidate }).format(new Date());
    return candidate;
  } catch {
    return DEFAULT_HEATMAP_TIMEZONE;
  }
}

function normalizeUsageHeatmapInput(
  input: UsageHeatmapQueryInput | undefined
): NormalizedUsageHeatmapInput {
  const normalizedFrom = toIsoString(input?.from);
  const normalizedTo = toIsoString(input?.to);

  return {
    tenantId: firstNonEmptyString(input?.tenantId) ?? DEFAULT_TENANT_ID,
    from: normalizedFrom ?? undefined,
    to: normalizedTo ?? undefined,
    timezone: normalizeHeatmapTimezone(input?.timezone),
  };
}

function normalizeUsageAggregateInput(
  input: UsageAggregateQueryInput | undefined
): NormalizedUsageAggregateInput {
  const normalizedFrom = toIsoString(input?.from);
  const normalizedTo = toIsoString(input?.to);
  const rawLimit =
    typeof input?.limit === "number" && Number.isFinite(input.limit)
      ? Math.trunc(input.limit)
      : DEFAULT_SESSION_LIMIT;
  const limit = rawLimit > 0 ? rawLimit : DEFAULT_SESSION_LIMIT;

  return {
    tenantId: firstNonEmptyString(input?.tenantId) ?? DEFAULT_TENANT_ID,
    from: normalizedFrom ?? undefined,
    to: normalizedTo ?? undefined,
    project: firstNonEmptyString(input?.project)?.toLowerCase() ?? undefined,
    limit,
  };
}

function normalizeScopedTenantId(tenantId: string | undefined): string {
  return firstNonEmptyString(tenantId) ?? DEFAULT_TENANT_ID;
}

function buildTenantScopedLookupKey(tenantId: string, scopedValue: string): string {
  return `${tenantId}:${scopedValue}`;
}

function readDatePart(parts: Intl.DateTimeFormatPart[], type: "year" | "month" | "day"): number {
  const matched = parts.find((part) => part.type === type);
  return matched ? Number(matched.value) : Number.NaN;
}

function getLocalDateKey(date: Date, formatter: Intl.DateTimeFormat): string | null {
  const parts = formatter.formatToParts(date);
  const year = readDatePart(parts, "year");
  const month = readDatePart(parts, "month");
  const day = readDatePart(parts, "day");
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return null;
  }
  if (month < 1 || month > 12 || day < 1 || day > 31) {
    return null;
  }
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function parseTimeZoneOffsetMinutes(timeZoneName: string): number | null {
  if (timeZoneName === "GMT" || timeZoneName === "UTC") {
    return 0;
  }

  const matched = /^(?:GMT|UTC)([+-])(\d{1,2})(?::?(\d{2}))?$/.exec(timeZoneName);
  if (!matched) {
    return null;
  }

  const sign = matched[1] === "-" ? -1 : 1;
  const hours = Number(matched[2]);
  const minutes = matched[3] ? Number(matched[3]) : 0;
  if (!Number.isFinite(hours) || !Number.isFinite(minutes)) {
    return null;
  }
  return sign * (hours * 60 + minutes);
}

function getTimeZoneOffsetMinutes(date: Date, formatter: Intl.DateTimeFormat): number {
  const timeZoneName = formatter
    .formatToParts(date)
    .find((part) => part.type === "timeZoneName")?.value;
  if (!timeZoneName) {
    return 0;
  }

  const parsed = parseTimeZoneOffsetMinutes(timeZoneName);
  return parsed ?? 0;
}

function toUtcIsoForLocalDate(localDateKey: string, offsetFormatter: Intl.DateTimeFormat): string {
  const [yearRaw, monthRaw, dayRaw] = localDateKey.split("-");
  const year = Number(yearRaw);
  const month = Number(monthRaw);
  const day = Number(dayRaw);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return new Date(0).toISOString();
  }

  const localMidnightAsUtc = Date.UTC(year, month - 1, day, 0, 0, 0, 0);
  let current = localMidnightAsUtc;
  for (let index = 0; index < 3; index += 1) {
    const offsetMinutes = getTimeZoneOffsetMinutes(new Date(current), offsetFormatter);
    const next = localMidnightAsUtc - offsetMinutes * 60_000;
    if (next === current) {
      break;
    }
    current = next;
  }

  return new Date(current).toISOString();
}

function resolveSourceTenantId(source: Source): string | undefined {
  const sourceRow = source as unknown as DbRow;
  const metadata = toDbRow(sourceRow.metadata);

  return (
    firstNonEmptyString(
      sourceRow.tenant_id,
      sourceRow.tenantId,
      metadata?.tenant_id,
      metadata?.tenantId
    ) ?? undefined
  );
}

function resolveSessionTenantId(
  session: Session,
  sourceTenantById: ReadonlyMap<string, string>
): string {
  const tenantIdFromSource = sourceTenantById.get(session.sourceId);
  if (tenantIdFromSource) {
    return tenantIdFromSource;
  }

  const sessionRow = session as unknown as DbRow;
  return firstNonEmptyString(sessionRow.tenant_id, sessionRow.tenantId) ?? DEFAULT_TENANT_ID;
}

interface SessionFilterDimensions {
  clientType: string | null;
  tool: string | null;
  host: string | null;
  model: string | null;
  project: string | null;
}

function resolveSessionFilterDimensions(
  session: Session,
  source?: Source
): SessionFilterDimensions {
  const sessionRow = session as unknown as DbRow;
  const sourceRow = source ? (source as unknown as DbRow) : undefined;
  const sessionMetadata = toDbRow(sessionRow.metadata);
  const sourceMetadata = toDbRow(sourceRow?.metadata);

  return {
    clientType: firstNonEmptyString(
      sessionRow.clientType,
      sessionRow.client_type,
      sessionMetadata?.clientType,
      sessionMetadata?.client_type,
      sessionRow.client,
      sessionMetadata?.client,
      sessionRow.provider,
      sessionMetadata?.provider
    ),
    tool: firstNonEmptyString(sessionRow.tool, sessionMetadata?.tool, sessionRow.provider),
    host: firstNonEmptyString(
      sessionRow.host,
      sessionRow.hostname,
      sessionMetadata?.host,
      sessionMetadata?.hostname,
      sessionRow.client_host,
      sessionMetadata?.client_host,
      sourceRow?.hostname,
      sourceMetadata?.hostname,
      sourceRow?.location,
      source?.location
    ),
    model: firstNonEmptyString(sessionRow.model, sessionMetadata?.model),
    project: firstNonEmptyString(
      sessionRow.project,
      sessionRow.project_id,
      sessionMetadata?.project,
      sessionMetadata?.project_id,
      sessionRow.workspace,
      sessionRow.workspace_id,
      sessionMetadata?.workspace,
      sessionMetadata?.workspace_id,
      sourceRow?.workspace_id,
      sourceMetadata?.project,
      sourceMetadata?.project_id,
      sourceMetadata?.workspace,
      sourceMetadata?.workspace_id
    ),
  };
}

function matchesCaseInsensitiveFilter(value: string | null, expected: string | undefined): boolean {
  if (!expected) {
    return true;
  }
  return (value ?? "").toLowerCase() === expected.toLowerCase();
}

function aggregateHeatmap(
  sessions: Session[],
  input: NormalizedUsageHeatmapInput,
  sourceTenantById: ReadonlyMap<string, string>
): HeatmapCell[] {
  const bucket = new Map<string, HeatmapCell>();
  const dateCache = new Map<string, string>();
  const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
  const toTimestamp = input.to ? Date.parse(input.to) : undefined;
  const localDateFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone: input.timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const offsetFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone: input.timezone,
    timeZoneName: "shortOffset",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  for (const session of sessions) {
    if (resolveSessionTenantId(session, sourceTenantById) !== input.tenantId) {
      continue;
    }

    const startedAtTimestamp = Date.parse(session.startedAt);
    if (!Number.isFinite(startedAtTimestamp)) {
      continue;
    }
    if (fromTimestamp !== undefined && startedAtTimestamp < fromTimestamp) {
      continue;
    }
    if (toTimestamp !== undefined && startedAtTimestamp > toTimestamp) {
      continue;
    }

    const localDateKey = getLocalDateKey(new Date(startedAtTimestamp), localDateFormatter);
    if (!localDateKey) {
      continue;
    }

    let key = dateCache.get(localDateKey);
    if (!key) {
      key = toUtcIsoForLocalDate(localDateKey, offsetFormatter);
      dateCache.set(localDateKey, key);
    }

    const tokens = Math.max(0, Math.trunc(toNumber(session.tokens, 0)));
    const cost = Math.max(0, toNumber(session.cost, 0));
    const current = bucket.get(key);

    if (!current) {
      bucket.set(key, {
        date: key,
        tokens,
        cost,
        sessions: 1,
      });
      continue;
    }

    current.tokens += tokens;
    current.cost += cost;
    current.sessions += 1;
  }

  return [...bucket.values()]
    .sort((a, b) => a.date.localeCompare(b.date))
    .map((cell) => ({ ...cell, cost: Number(cell.cost.toFixed(6)) }));
}

class ControlPlaneRepository {
  private readonly memorySources: Source[] = [];
  private readonly memorySourceTenantById = new Map<string, string>();
  private readonly memorySyncJobs: SyncJob[] = [];
  private readonly memorySourceWatermarks: SourceWatermark[] = [];
  private readonly memorySourceParseFailures: MemorySourceParseFailureRecord[] = [];
  private readonly memorySessions: Session[] = [];
  private readonly memorySessionEvents: MemorySessionEventRecord[] = [];
  private readonly memoryBudgets: TenantBudgetRecord[] = [];
  private readonly memoryAlerts: Alert[] = [];
  private readonly memoryAlertOrchestrationRules: AlertOrchestrationRule[] = [];
  private readonly memoryAlertOrchestrationExecutions: AlertOrchestrationExecutionLog[] = [];
  private readonly memoryResidencyPolicies: TenantResidencyPolicy[] = [];
  private readonly memoryReplicationJobs: ReplicationJob[] = [];
  private readonly memoryRuleAssets: RuleAsset[] = [];
  private readonly memoryRuleAssetVersions: RuleAssetVersion[] = [];
  private readonly memoryRuleApprovals: RuleApproval[] = [];
  private readonly memoryMcpToolPolicies: McpToolPolicy[] = [];
  private readonly memoryMcpApprovalRequests: McpApprovalRequest[] = [];
  private readonly memoryMcpInvocations: McpInvocationAudit[] = [];
  private readonly memoryBudgetReleaseRequests: TenantBudgetReleaseRequestRecord[] = [];
  private readonly memoryIntegrationAlertCallbacks: IntegrationAlertCallbackRecord[] = [];
  private readonly memoryPricingCatalogVersions: PricingCatalogVersionRecord[] = [];
  private readonly memoryPricingCatalogEntries: PricingCatalogEntryRecord[] = [];
  private readonly memoryAudits: AuditItem[] = [];
  private readonly memoryUsers: LocalUser[] = [];
  private readonly memoryTenants: Tenant[] = [];
  private readonly memoryOrganizations: Organization[] = [];
  private readonly memoryTenantMembers: TenantMember[] = [];
  private readonly memoryDeviceBindings: DeviceBinding[] = [];
  private readonly memoryAgentBindings: AgentBinding[] = [];
  private readonly memorySourceBindings: SourceBinding[] = [];
  private readonly memoryAuthSessions: AuthSession[] = [];
  private readonly memoryApiKeys: ApiKey[] = [];
  private readonly memoryApiKeyByHash = new Map<string, MemoryApiKeyHashRecord>();
  private readonly memoryWebhookEndpoints: WebhookEndpoint[] = [];
  private readonly memoryWebhookReplayTasks: WebhookReplayTask[] = [];
  private readonly memoryQualityEvents: QualityEvent[] = [];
  private readonly memoryQualityScorecards: QualityScorecard[] = [];
  private readonly memoryReplayDatasets: ReplayDataset[] = [];
  private readonly memoryReplayDatasetCases: ReplayDatasetCase[] = [];
  private readonly memoryReplayRuns: ReplayRun[] = [];
  private readonly memoryReplayArtifacts: ReplayArtifact[] = [];
  private readonly memoryReplayBaselines: ReplayBaseline[] = [];
  private readonly memoryReplayJobs: ReplayJob[] = [];
  private readonly memoryReplayJobDiffById = new Map<string, MemoryReplayJobDiffRecord>();
  private pool: PgPool | null = null;
  private initPromise: Promise<void> | null = null;
  private loggedDbFallback = false;

  constructor() {
    this.ensureDefaultTenantInMemory();
  }

  async listSources(tenantId: string): Promise<Source[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const pool = await this.getPool();
    if (!pool) {
      return this.listSourcesFromMemory(normalizedTenantId);
    }

    try {
      const result = await pool.query(
        `SELECT id, created_at, to_jsonb(sources.*) AS row_data
         FROM sources
         WHERE tenant_id = $1
         ORDER BY created_at DESC`,
        [normalizedTenantId]
      );
      return result.rows.map(mapSourceRow);
    } catch (error) {
      this.disableDb(error, "查询 sources 失败");
      return this.listSourcesFromMemory(normalizedTenantId);
    }
  }

  async createSource(tenantId: string, input: CreateSourceInput): Promise<Source> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const syncCron = firstNonEmptyString(input.syncCron) ?? undefined;
    const syncRetentionDays = toOptionalNonNegativeInteger(input.syncRetentionDays);
    const source: Source = {
      id: crypto.randomUUID(),
      name: input.name,
      type: input.type,
      location: input.location,
      sshConfig: input.sshConfig,
      accessMode: toSourceAccessMode(input.accessMode),
      syncCron,
      syncRetentionDays,
      enabled: input.enabled ?? true,
      createdAt: new Date().toISOString(),
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.saveSourceToMemory(source, normalizedTenantId);
    }

    try {
      const metadata = JSON.stringify({
        tenant_id: normalizedTenantId,
        tenantId: normalizedTenantId,
        name: source.name,
        type: source.type,
        location: source.location,
        ssh_host: source.sshConfig?.host,
        ssh_port: source.sshConfig?.port,
        ssh_user: source.sshConfig?.user,
        ssh_auth_type: source.sshConfig?.authType,
        ssh_key_path: source.sshConfig?.keyPath,
        ssh_known_hosts_path: source.sshConfig?.knownHostsPath,
        access_mode: source.accessMode,
        sync_cron: source.syncCron,
        sync_retention_days: source.syncRetentionDays,
        enabled: source.enabled,
        created_by: "control-plane",
      });

      await pool.query(
        `INSERT INTO sources (
           id, provider, source_type, hostname, agent_id, tenant_id, workspace_id, metadata,
           name, type, location, access_mode, sync_cron, sync_retention_days, enabled, created_at, updated_at
         )
         VALUES (
           $1, $2, $3, $4, $5, $6, $7, $8::jsonb,
           $9, $10, $11, $12, $13, $14, $15, $16::timestamptz, $16::timestamptz
         )`,
        [
          source.id,
          CONTROL_PLANE_SOURCE_PROVIDER,
          source.type,
          source.sshConfig?.host ?? source.location,
          source.name,
          normalizedTenantId,
          null,
          metadata,
          source.name,
          source.type,
          source.location,
          source.accessMode,
          source.syncCron ?? null,
          source.syncRetentionDays ?? null,
          source.enabled,
          source.createdAt,
        ]
      );
      return source;
    } catch (error) {
      this.disableDb(error, "写入 sources 失败");
      return this.saveSourceToMemory(source, normalizedTenantId);
    }
  }

  async listSyncJobs(tenantId: string, sourceId: string, limit?: number): Promise<SyncJob[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedLimitCandidate =
      typeof limit === "number" && Number.isFinite(limit)
        ? Math.trunc(limit)
        : DEFAULT_SYNC_JOB_LIMIT;
    const normalizedLimit =
      normalizedLimitCandidate > 0 ? normalizedLimitCandidate : DEFAULT_SYNC_JOB_LIMIT;

    const pool = await this.getPool();
    if (!pool) {
      return this.listSyncJobsFromMemory(normalizedTenantId, sourceId, normalizedLimit);
    }

    try {
      const result = await pool.query(
        `SELECT jobs.id,
                source_id,
                mode,
                status,
                error,
                "trigger" AS trigger,
                attempt,
                started_at,
                ended_at,
                next_run_at,
                duration_ms,
                error_code,
                error_detail,
                cancel_requested,
                created_at,
                updated_at
         FROM sync_jobs AS jobs
         INNER JOIN sources AS src ON src.id = jobs.source_id
         WHERE jobs.source_id = $1
           AND src.tenant_id = $2
         ORDER BY jobs.created_at DESC
         LIMIT $3`,
        [sourceId, normalizedTenantId, normalizedLimit]
      );

      return result.rows.map(mapSyncJobRow);
    } catch (error) {
      this.disableDb(error, "查询 sync_jobs 失败");
      return this.listSyncJobsFromMemory(normalizedTenantId, sourceId, normalizedLimit);
    }
  }

  async createSyncJob(
    tenantId: string,
    sourceId: string,
    mode: SourceAccessMode,
    status: SyncJobStatus,
    error?: string,
    options?: CreateSyncJobOptions
  ): Promise<SyncJob> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const now = new Date().toISOString();
    const startedAt = toIsoString(options?.startedAt) ?? undefined;
    const endedAt = toIsoString(options?.endedAt) ?? undefined;
    const nextRunAt = toIsoString(options?.nextRunAt) ?? undefined;
    const durationMsCandidate = toOptionalNonNegativeInteger(options?.durationMs);
    const durationMs =
      durationMsCandidate ??
      (startedAt && endedAt
        ? Math.max(0, Date.parse(endedAt) - Date.parse(startedAt))
        : undefined);
    const errorDetail = firstNonEmptyString(options?.errorDetail, error) ?? undefined;
    const normalizedError = firstNonEmptyString(error, options?.errorDetail) ?? undefined;
    const syncJob: SyncJob = {
      id: crypto.randomUUID(),
      sourceId,
      mode: toSourceAccessMode(mode),
      status: toSyncJobStatus(status),
      error: normalizedError,
      trigger: firstNonEmptyString(options?.trigger) ?? undefined,
      attempt: toSyncJobAttempt(options?.attempt),
      startedAt,
      endedAt,
      nextRunAt,
      durationMs,
      errorCode: firstNonEmptyString(options?.errorCode) ?? undefined,
      errorDetail,
      cancelRequested: options?.cancelRequested === true,
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.saveSyncJobToMemory(syncJob, normalizedTenantId);
    }

    try {
      const result = await pool.query(
        `INSERT INTO sync_jobs (
           id,
           source_id,
           mode,
           status,
           error,
           "trigger",
           attempt,
           started_at,
           ended_at,
           next_run_at,
           duration_ms,
           error_code,
           error_detail,
           cancel_requested,
           created_at,
           updated_at
         )
         SELECT
           $1,
           src.id,
           $3,
           $4,
           $5,
           $6,
           $7,
           $8::timestamptz,
           $9::timestamptz,
           $10::timestamptz,
           $11,
           $12,
           $13,
           $14,
           $15::timestamptz,
           $15::timestamptz
         FROM sources AS src
         WHERE src.id = $2
           AND src.tenant_id = $16
         RETURNING id,
                   source_id,
                   mode,
                   status,
                   error,
                   "trigger" AS trigger,
                   attempt,
                   started_at,
                   ended_at,
                   next_run_at,
                   duration_ms,
                   error_code,
                   error_detail,
                   cancel_requested,
                   created_at,
                   updated_at`,
        [
          syncJob.id,
          syncJob.sourceId,
          syncJob.mode,
          syncJob.status,
          syncJob.error ?? null,
          syncJob.trigger ?? null,
          syncJob.attempt ?? 1,
          syncJob.startedAt ?? null,
          syncJob.endedAt ?? null,
          syncJob.nextRunAt ?? null,
          syncJob.durationMs ?? null,
          syncJob.errorCode ?? null,
          syncJob.errorDetail ?? null,
          syncJob.cancelRequested ?? false,
          syncJob.createdAt,
          normalizedTenantId,
        ]
      );

      const row = result.rows[0];
      if (!row) {
        throw new Error("sync_job_source_not_found");
      }
      return mapSyncJobRow(row);
    } catch (dbError) {
      if (
        isPgForeignKeyViolation(dbError) ||
        (dbError instanceof Error && dbError.message === "sync_job_source_not_found")
      ) {
        throw dbError;
      }
      this.disableDb(dbError, "写入 sync_jobs 失败");
      return this.saveSyncJobToMemory(syncJob, normalizedTenantId);
    }
  }

  async requestCancelSyncJob(tenantId: string, jobId: string): Promise<SyncJob | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedJobId = firstNonEmptyString(jobId);
    if (!normalizedJobId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.requestCancelSyncJobFromMemory(normalizedJobId, normalizedTenantId);
    }

    const now = new Date().toISOString();

    try {
      const result = await pool.query(
        `UPDATE sync_jobs AS jobs
         SET status = CASE
               WHEN jobs.status = 'pending' THEN 'cancelled'
               ELSE jobs.status
             END,
             cancel_requested = CASE
               WHEN jobs.status IN ('pending', 'running') THEN TRUE
               ELSE jobs.cancel_requested
             END,
             "trigger" = CASE
               WHEN jobs.status IN ('pending', 'running')
                 THEN COALESCE(NULLIF("trigger", ''), 'manual')
               ELSE "trigger"
             END,
             started_at = CASE
               WHEN jobs.status = 'pending'
                 THEN COALESCE(jobs.started_at, jobs.created_at, $2::timestamptz)
               ELSE jobs.started_at
             END,
             ended_at = CASE
               WHEN jobs.status = 'pending'
                 THEN COALESCE(jobs.ended_at, $2::timestamptz)
               ELSE jobs.ended_at
             END,
             duration_ms = CASE
               WHEN jobs.status = 'pending'
                 THEN COALESCE(
                   jobs.duration_ms,
                   GREATEST(
                     0,
                     FLOOR(
                       EXTRACT(
                         EPOCH FROM (
                           COALESCE(jobs.ended_at, $2::timestamptz) -
                           COALESCE(jobs.started_at, jobs.created_at, $2::timestamptz)
                         )
                       ) * 1000
                     )::INTEGER
                   )
                 )
               ELSE jobs.duration_ms
             END,
             updated_at = CASE
               WHEN jobs.status IN ('pending', 'running')
                 THEN $2::timestamptz
               ELSE jobs.updated_at
             END
         FROM sources AS src
         WHERE jobs.id = $1
           AND src.id = jobs.source_id
           AND src.tenant_id = $3
         RETURNING jobs.id,
                   jobs.source_id,
                   jobs.mode,
                   jobs.status,
                   jobs.error,
                   jobs."trigger" AS trigger,
                   jobs.attempt,
                   jobs.started_at,
                   jobs.ended_at,
                   jobs.next_run_at,
                   jobs.duration_ms,
                   jobs.error_code,
                   jobs.error_detail,
                   jobs.cancel_requested,
                   jobs.created_at,
                   jobs.updated_at`,
        [normalizedJobId, now, normalizedTenantId]
      );
      const row = result.rows[0];
      return row ? mapSyncJobRow(row) : null;
    } catch (error) {
      this.disableDb(error, "更新 sync_jobs 取消状态失败");
      return this.requestCancelSyncJobFromMemory(normalizedJobId, normalizedTenantId);
    }
  }

  async listSourceWatermarks(
    tenantId: string,
    sourceId: string
  ): Promise<SourceWatermark[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const pool = await this.getPool();
    if (!pool) {
      return this.listSourceWatermarksFromMemory(normalizedTenantId, sourceId);
    }

    try {
      const result = await pool.query(
        `SELECT sw.source_id,
                provider,
                watermark,
                created_at,
                updated_at
         FROM source_watermarks AS sw
         INNER JOIN sources AS src ON src.id = sw.source_id
         WHERE sw.source_id = $1
           AND src.tenant_id = $2
         ORDER BY sw.updated_at DESC, sw.created_at DESC`,
        [sourceId, normalizedTenantId]
      );
      return result.rows.map(mapSourceWatermarkRow);
    } catch (error) {
      this.disableDb(error, "查询 source_watermarks 失败");
      return this.listSourceWatermarksFromMemory(normalizedTenantId, sourceId);
    }
  }

  async getSourceHealth(tenantId: string, sourceId: string): Promise<SourceHealth | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedSourceId = firstNonEmptyString(sourceId);
    if (!normalizedSourceId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getSourceHealthFromMemory(normalizedTenantId, normalizedSourceId);
    }

    try {
      const result = await pool.query(
        `SELECT
           src.id AS source_id,
           src.access_mode,
           MAX(
             CASE
               WHEN jobs.status = 'success'
                 THEN COALESCE(jobs.ended_at, jobs.updated_at, jobs.started_at, jobs.created_at)
               ELSE NULL
             END
           ) AS last_success_at,
           MAX(
             CASE
               WHEN jobs.status = 'failed'
                 THEN COALESCE(jobs.ended_at, jobs.updated_at, jobs.started_at, jobs.created_at)
               ELSE NULL
             END
           ) AS last_failure_at,
           COALESCE(COUNT(*) FILTER (WHERE jobs.status = 'failed'), 0)::bigint AS failure_count,
           AVG(
             CASE
               WHEN jobs.duration_ms IS NOT NULL
                 AND jobs.duration_ms >= 0
                 AND jobs.status IN ('success', 'failed')
                 THEN jobs.duration_ms::double precision
               ELSE NULL
             END
           ) AS avg_latency_ms,
           MAX(sw.updated_at) AS watermark_updated_at
         FROM sources AS src
         LEFT JOIN sync_jobs AS jobs ON jobs.source_id = src.id
         LEFT JOIN source_watermarks AS sw ON sw.source_id = src.id
         WHERE src.id = $1
           AND src.tenant_id = $2
         GROUP BY src.id, src.access_mode
         LIMIT 1`,
        [normalizedSourceId, normalizedTenantId]
      );

      const row = result.rows[0];
      if (!row) {
        return null;
      }

      const lastSuccessAt = resolveLatestIsoTimestamp(
        row.last_success_at,
        row.watermark_updated_at
      );
      const avgLatencyMs =
        row.avg_latency_ms === null || row.avg_latency_ms === undefined
          ? null
          : Math.max(0, Math.round(toNumber(row.avg_latency_ms, 0)));

      return {
        sourceId: firstNonEmptyString(row.source_id) ?? normalizedSourceId,
        accessMode: toSourceAccessMode(row.access_mode),
        lastSuccessAt,
        lastFailureAt: toIsoString(row.last_failure_at),
        failureCount: Math.max(0, Math.trunc(toNumber(row.failure_count, 0))),
        avgLatencyMs,
        freshnessMinutes: computeFreshnessMinutes(lastSuccessAt),
      };
    } catch (error) {
      this.disableDb(error, "查询 source health 失败");
      return this.getSourceHealthFromMemory(normalizedTenantId, normalizedSourceId);
    }
  }

  async listSourceParseFailures(
    tenantId: string,
    sourceId: string,
    input?: SourceParseFailureQueryInput
  ): Promise<SourceParseFailureListResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedSourceId = firstNonEmptyString(sourceId);
    if (!normalizedSourceId) {
      return {
        items: [],
        total: 0,
      };
    }

    const normalizedInput = normalizeSourceParseFailureQueryInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listSourceParseFailuresFromMemory(
        normalizedTenantId,
        normalizedSourceId,
        normalizedInput
      );
    }

    try {
      const params: unknown[] = [normalizedTenantId, normalizedSourceId];
      const whereClauses = ["tenant_id = $1", "source_id = $2"];

      if (normalizedInput.from) {
        params.push(normalizedInput.from);
        whereClauses.push(`occurred_at >= $${params.length}::timestamptz`);
      }
      if (normalizedInput.to) {
        params.push(normalizedInput.to);
        whereClauses.push(`occurred_at <= $${params.length}::timestamptz`);
      }
      if (normalizedInput.parserKey) {
        params.push(normalizedInput.parserKey);
        whereClauses.push(`LOWER(parser_key) = LOWER($${params.length})`);
      }
      if (normalizedInput.errorCode) {
        params.push(normalizedInput.errorCode);
        whereClauses.push(`LOWER(error_code) = LOWER($${params.length})`);
      }

      const whereSql = whereClauses.join(" AND ");
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total
         FROM parse_failures
         WHERE ${whereSql}`,
        params
      );

      const listParams = [...params, normalizedInput.limit];
      const listResult = await pool.query(
        `SELECT id,
                tenant_id,
                source_id,
                parser_key,
                error_code,
                error_message,
                source_path,
                source_offset,
                raw_hash,
                metadata,
                occurred_at,
                created_at
         FROM parse_failures
         WHERE ${whereSql}
         ORDER BY occurred_at DESC, created_at DESC
         LIMIT $${listParams.length}`,
        listParams
      );

      return {
        items: listResult.rows.map(mapSourceParseFailureRow),
        total: Math.max(0, Math.trunc(toNumber(countResult.rows[0]?.total, 0))),
      };
    } catch (error) {
      this.disableDb(error, "查询 source parse failures 失败");
      return this.listSourceParseFailuresFromMemory(
        normalizedTenantId,
        normalizedSourceId,
        normalizedInput
      );
    }
  }

  async deleteSourceById(tenantId: string, id: string): Promise<DeleteSourceResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const pool = await this.getPool();
    if (!pool) {
      return this.deleteSourceByIdFromMemory(normalizedTenantId, id);
    }

    try {
      const result = await pool.query(
        `DELETE FROM sources
         WHERE id = $1
           AND tenant_id = $2
         RETURNING id`,
        [id, normalizedTenantId]
      );
      return result.rows[0] ? "deleted" : "not_found";
    } catch (error) {
      if (isPgForeignKeyViolation(error)) {
        return "conflict";
      }
      this.disableDb(error, "删除 sources 失败");
      return this.deleteSourceByIdFromMemory(normalizedTenantId, id);
    }
  }

  async searchSessions(input: SessionSearchInput, tenantId?: string): Promise<SessionSearchResult> {
    const normalized = normalizeSessionSearchInput(input, tenantId);
    const pool = await this.getPool();

    if (!pool) {
      return this.searchSessionsFromMemory(normalized);
    }

    const runSearchQuery = async (preferFtsOnEventText: boolean): Promise<SessionSearchResult> => {
      const params: unknown[] = [];
      const whereClauses: string[] = [];
      const sessionClientTypeSql = `COALESCE(
        NULLIF(s.session_json->>'client_type', ''),
        NULLIF(s.session_json->>'clientType', ''),
        NULLIF(s.session_json->>'client', ''),
        NULLIF(s.session_json->>'provider', ''),
        ''
      )`;
      const sessionToolSql = `COALESCE(
        NULLIF(s.session_json->>'tool', ''),
        NULLIF(s.session_json->>'provider', ''),
        ''
      )`;
      const sessionHostSql = `COALESCE(
        NULLIF(s.session_json->>'host', ''),
        NULLIF(s.session_json->>'hostname', ''),
        NULLIF(s.source_hostname, ''),
        NULLIF(s.source_location, ''),
        ''
      )`;
      const sessionModelSql = `COALESCE(NULLIF(s.session_json->>'model', ''), '')`;
      const sessionProjectSql = `COALESCE(
        NULLIF(s.session_json->>'project', ''),
        NULLIF(s.session_json->>'project_id', ''),
        NULLIF(s.session_json->>'workspace', ''),
        NULLIF(s.session_json->>'workspace_id', ''),
        NULLIF(s.source_workspace_id, ''),
        NULLIF(s.source_metadata->>'project', ''),
        NULLIF(s.source_metadata->>'project_id', ''),
        NULLIF(s.source_metadata->>'workspace', ''),
        NULLIF(s.source_metadata->>'workspace_id', ''),
        ''
      )`;
      const cursor = decodeTimePaginationCursor(normalized.cursor);
      const sessionsFromSql = `FROM (
        SELECT
          sess.id,
          sess.source_id,
          sess.started_at,
          sess.ended_at,
          to_jsonb(sess.*) AS session_json,
          NULLIF(src.hostname, '') AS source_hostname,
          NULLIF(src.location, '') AS source_location,
          NULLIF(src.workspace_id, '') AS source_workspace_id,
          COALESCE(src.metadata, '{}'::jsonb) AS source_metadata,
          COALESCE(NULLIF(src.tenant_id, ''), '${DEFAULT_TENANT_ID}') AS source_tenant_id
        FROM sessions AS sess
        LEFT JOIN sources AS src ON src.id = sess.source_id
      ) AS s`;

      if (normalized.tenantId) {
        params.push(normalized.tenantId);
        whereClauses.push(`s.source_tenant_id = $${params.length}`);
      }

      if (normalized.sourceId) {
        params.push(normalized.sourceId);
        whereClauses.push(`s.source_id = $${params.length}`);
      }

      if (normalized.clientType) {
        params.push(normalized.clientType);
        whereClauses.push(`LOWER(${sessionClientTypeSql}) = LOWER($${params.length})`);
      }

      if (normalized.tool) {
        params.push(normalized.tool);
        whereClauses.push(`LOWER(${sessionToolSql}) = LOWER($${params.length})`);
      }

      if (normalized.host) {
        params.push(normalized.host);
        whereClauses.push(`LOWER(${sessionHostSql}) = LOWER($${params.length})`);
      }

      if (normalized.model) {
        params.push(normalized.model);
        whereClauses.push(`LOWER(${sessionModelSql}) = LOWER($${params.length})`);
      }

      if (normalized.project) {
        params.push(normalized.project);
        whereClauses.push(`LOWER(${sessionProjectSql}) = LOWER($${params.length})`);
      }

      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`s.started_at >= $${params.length}::timestamptz`);
      }

      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`s.started_at <= $${params.length}::timestamptz`);
      }

      if (normalized.keyword) {
        params.push(`%${normalized.keyword}%`);
        const ilikeToken = `$${params.length}`;
        const sessionKeywordSql = `(s.id ILIKE ${ilikeToken}
          OR COALESCE(s.session_json->>'native_session_id', '') ILIKE ${ilikeToken}
          OR COALESCE(s.session_json->>'provider', '') ILIKE ${ilikeToken}
          OR ${sessionToolSql} ILIKE ${ilikeToken}
          OR ${sessionModelSql} ILIKE ${ilikeToken})`;

        if (preferFtsOnEventText) {
          params.push(normalized.keyword);
          const queryToken = `$${params.length}`;
          whereClauses.push(
            `(${sessionKeywordSql}
              OR (
                CASE
                  WHEN numnode(plainto_tsquery('simple', ${queryToken})) > 0
                    THEN EXISTS (
                      SELECT 1
                      FROM events AS evt
                      WHERE evt.session_id = s.id
                        AND evt.source_id = s.source_id
                        AND to_tsvector('simple', COALESCE(evt.text, '')) @@ plainto_tsquery('simple', ${queryToken})
                    )
                  ELSE EXISTS (
                    SELECT 1
                    FROM events AS evt
                    WHERE evt.session_id = s.id
                      AND evt.source_id = s.source_id
                      AND COALESCE(evt.text, '') ILIKE ${ilikeToken}
                  )
                END
              ))`
          );
        } else {
          whereClauses.push(
            `(${sessionKeywordSql}
              OR EXISTS (
                SELECT 1
                FROM events AS evt
                WHERE evt.session_id = s.id
                  AND evt.source_id = s.source_id
                  AND COALESCE(evt.text, '') ILIKE ${ilikeToken}
              ))`
          );
        }
      }

      const whereSql = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total
         ${sessionsFromSql}
         ${whereSql}`,
        params
      );

      const listParams = [...params];
      const listWhereClauses = [...whereClauses];
      if (cursor) {
        listParams.push(cursor.timestamp);
        const timestampToken = `$${listParams.length}`;
        listParams.push(cursor.id);
        const idToken = `$${listParams.length}`;
        listWhereClauses.push(
          `(s.started_at < ${timestampToken}::timestamptz
            OR (s.started_at = ${timestampToken}::timestamptz AND s.id < ${idToken}))`
        );
      }

      const listWhereSql =
        listWhereClauses.length > 0 ? `WHERE ${listWhereClauses.join(" AND ")}` : "";
      listParams.push(normalized.limit + 1);
      const listResult = await pool.query(
        `SELECT s.id,
                s.source_id,
                s.started_at,
                to_char(s.started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS started_at_cursor,
                s.ended_at,
                s.session_json AS row_data,
                COALESCE(NULLIF(s.session_json->>'tokens', ''), NULLIF(s.session_json->>'total_tokens', ''), '0') AS tokens,
                COALESCE(
                  NULLIF(s.session_json->>'cost', ''),
                  NULLIF(s.session_json->>'cost_usd', ''),
                  NULLIF(s.session_json->>'total_cost', ''),
                 '0'
                ) AS cost
         ${sessionsFromSql}
         ${listWhereSql}
         ORDER BY s.started_at DESC NULLS LAST, s.id DESC
         LIMIT $${listParams.length}`,
        listParams
      );

      const mappedItems = listResult.rows.map(mapSessionRow);
      const hasMore = mappedItems.length > normalized.limit;
      const items = hasMore ? mappedItems.slice(0, normalized.limit) : mappedItems;
      const cursorRows = hasMore ? listResult.rows.slice(0, normalized.limit) : listResult.rows;
      const lastItem = items[items.length - 1];
      const lastCursorRow = cursorRows[cursorRows.length - 1];
      const cursorTimestamp =
        firstNonEmptyString((lastCursorRow as DbRow | undefined)?.started_at_cursor) ??
        toIsoString((lastCursorRow as DbRow | undefined)?.started_at);
      const nextCursor =
        hasMore &&
        lastItem &&
        typeof cursorTimestamp === "string" &&
        Number.isFinite(Date.parse(cursorTimestamp)) &&
        lastItem.id.trim().length > 0
          ? encodeTimePaginationCursor({
              timestamp: cursorTimestamp,
              id: lastItem.id,
            })
          : null;

      return {
        items,
        total: Math.max(0, Math.trunc(toNumber(countResult.rows[0]?.total, 0))),
        nextCursor,
      };
    };

    try {
      try {
        return await runSearchQuery(true);
      } catch (ftsError) {
        if (!normalized.keyword) {
          throw ftsError;
        }
        console.warn("[control-plane] sessions keyword FTS 查询失败，回退 ILIKE EXISTS。", {
          keyword: normalized.keyword,
          error: ftsError,
        });
        return await runSearchQuery(false);
      }
    } catch (error) {
      this.disableDb(error, "查询 sessions 失败");
      return this.searchSessionsFromMemory(normalized);
    }
  }

  async getSessionById(tenantId: string, sessionId: string): Promise<SessionDetail | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedSessionId = firstNonEmptyString(sessionId);
    if (!normalizedSessionId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getSessionByIdFromMemory(normalizedTenantId, normalizedSessionId);
    }

    try {
      const result = await pool.query(
        `SELECT
           sess.id,
           sess.source_id,
           sess.provider,
           sess.native_session_id,
           sess.tool,
           sess.workspace,
           sess.model,
           sess.started_at,
           sess.ended_at,
           sess.message_count,
           sess.tokens,
           sess.cost,
           (
             SELECT evt.source_path
             FROM events AS evt
             WHERE evt.session_id = sess.id
               AND evt.source_path IS NOT NULL
               AND evt.source_path <> ''
             ORDER BY evt."timestamp" DESC, evt.created_at DESC
             LIMIT 1
           ) AS source_path,
           COALESCE(src.name, src.agent_id, src.id) AS source_name,
           src.type AS source_type,
           src.location AS source_location,
           src.hostname AS source_host
         FROM sessions AS sess
         LEFT JOIN sources AS src ON src.id = sess.source_id
         WHERE sess.id = $1
           AND COALESCE(NULLIF(src.tenant_id, ''), $3) = $2
         LIMIT 1`,
        [normalizedSessionId, normalizedTenantId, DEFAULT_TENANT_ID]
      );

      const row = result.rows[0];
      return row ? mapSessionDetailRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询 session 详情失败");
      return this.getSessionByIdFromMemory(normalizedTenantId, normalizedSessionId);
    }
  }

  async listSessionEvents(
    tenantId: string,
    sessionId: string,
    limit = 500,
    cursor?: string
  ): Promise<SessionEventListResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedSessionId = firstNonEmptyString(sessionId);
    if (!normalizedSessionId) {
      return {
        items: [],
        total: 0,
        nextCursor: null,
      };
    }
    const normalizedLimit =
      Number.isFinite(limit) && Number.isInteger(limit) && limit > 0
        ? Math.min(limit, 2000)
        : 500;
    const parsedCursor = decodeTimePaginationCursor(cursor);

    const pool = await this.getPool();
    if (!pool) {
      return this.listSessionEventsFromMemory(
        normalizedTenantId,
        normalizedSessionId,
        normalizedLimit,
        parsedCursor
      );
    }

    try {
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total
         FROM events AS evt
         INNER JOIN sources AS src ON src.id = evt.source_id
         WHERE evt.session_id = $1
           AND COALESCE(NULLIF(src.tenant_id, ''), $2) = $2`,
        [normalizedSessionId, normalizedTenantId]
      );

      const listParams: unknown[] = [normalizedSessionId, normalizedTenantId];
      let cursorSql = "";
      if (parsedCursor) {
        listParams.push(parsedCursor.timestamp);
        const timestampToken = `$${listParams.length}`;
        listParams.push(parsedCursor.id);
        const idToken = `$${listParams.length}`;
        cursorSql = `
           AND (
             evt.timestamp > ${timestampToken}::timestamptz
             OR (evt.timestamp = ${timestampToken}::timestamptz AND evt.id > ${idToken})
           )`;
      }
      listParams.push(normalizedLimit + 1);

      const result = await pool.query(
        `SELECT
           evt.id,
           evt.session_id,
           evt.source_id,
           evt.event_type,
           evt.role,
           evt.text,
           evt.model,
           evt.timestamp,
           to_char(evt.timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS timestamp_cursor,
           evt.input_tokens,
           evt.output_tokens,
           evt.cache_read_tokens,
           evt.cache_write_tokens,
           evt.reasoning_tokens,
           evt.cost_usd,
           evt.source_path,
           evt.source_offset
         FROM events AS evt
         INNER JOIN sources AS src ON src.id = evt.source_id
         WHERE evt.session_id = $1
           AND COALESCE(NULLIF(src.tenant_id, ''), $2) = $2
           ${cursorSql}
         ORDER BY evt.timestamp ASC, evt.id ASC
         LIMIT $${listParams.length}`,
        listParams
      );
      const mappedItems = result.rows.map(mapSessionEventRow);
      const hasMore = mappedItems.length > normalizedLimit;
      const items = hasMore ? mappedItems.slice(0, normalizedLimit) : mappedItems;
      const cursorRows = hasMore ? result.rows.slice(0, normalizedLimit) : result.rows;
      const lastItem = items[items.length - 1];
      const lastCursorRow = cursorRows[cursorRows.length - 1];
      const cursorTimestamp =
        firstNonEmptyString((lastCursorRow as DbRow | undefined)?.timestamp_cursor) ??
        toIsoString((lastCursorRow as DbRow | undefined)?.timestamp);
      const nextCursor =
        hasMore &&
        lastItem &&
        typeof cursorTimestamp === "string" &&
        Number.isFinite(Date.parse(cursorTimestamp)) &&
        lastItem.id.trim().length > 0
          ? encodeTimePaginationCursor({
              timestamp: cursorTimestamp,
              id: lastItem.id,
            })
          : null;

      return {
        items,
        total: Math.max(0, Math.trunc(toNumber(countResult.rows[0]?.total, 0))),
        nextCursor,
      };
    } catch (error) {
      if (isPgUndefinedTable(error)) {
        return {
          items: [],
          total: 0,
          nextCursor: null,
        };
      }
      this.disableDb(error, "查询 session events 失败");
      return this.listSessionEventsFromMemory(
        normalizedTenantId,
        normalizedSessionId,
        normalizedLimit,
        parsedCursor
      );
    }
  }

  async listUsageDaily(input?: UsageAggregateQueryInput): Promise<UsageDailyItem[]> {
    const normalized = normalizeUsageAggregateInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listUsageDailyFromMemory(normalized);
    }

    try {
      const params: unknown[] = [normalized.tenantId, DEFAULT_TENANT_ID];
      const whereClauses: string[] = [
        "COALESCE(NULLIF(src.tenant_id, ''), $2) = $1",
        "sess.started_at IS NOT NULL",
      ];
      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`sess.started_at >= $${params.length}::timestamptz`);
      }
      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`sess.started_at <= $${params.length}::timestamptz`);
      }
      if (normalized.project) {
        params.push(normalized.project);
        whereClauses.push(`LOWER(${USAGE_AGGREGATE_PROJECT_SQL}) = $${params.length}`);
      }

      const result = await pool.query(
        `WITH event_cost AS (
           SELECT
             session_id,
             COALESCE(SUM(CASE WHEN cost_mode = 'raw' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_raw,
             COALESCE(SUM(CASE WHEN cost_mode = 'estimated' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_estimated,
             COALESCE(SUM(CASE WHEN cost_mode = 'reported' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_reported,
             BOOL_OR(cost_mode = 'raw' AND cost_usd IS NOT NULL) AS has_raw,
             BOOL_OR(cost_mode = 'estimated' AND cost_usd IS NOT NULL) AS has_estimated,
             BOOL_OR(cost_mode = 'reported' AND cost_usd IS NOT NULL) AS has_reported,
             BOOL_OR(cost_usd IS NOT NULL) AS has_any_cost
           FROM events
           GROUP BY session_id
         ),
         session_cost AS (
           SELECT
             sess.id,
             sess.started_at,
             COALESCE(sess.tokens, 0)::bigint AS tokens,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE)
                 THEN COALESCE(ec.cost_raw, 0) + COALESCE(ec.cost_reported, 0)
               WHEN LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'estimated'
                 THEN 0
               ELSE COALESCE(sess.cost, 0)::double precision
             END AS cost_raw,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE)
                 THEN COALESCE(ec.cost_estimated, 0)
               WHEN LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'estimated'
                 THEN COALESCE(sess.cost, 0)::double precision
               ELSE 0
             END AS cost_estimated,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE) AND COALESCE(ec.has_raw, FALSE)
                 THEN 1
               WHEN NOT COALESCE(ec.has_any_cost, FALSE)
                 AND COALESCE(sess.cost, 0) > 0
                 AND LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'raw'
                 THEN 1
               ELSE 0
             END AS raw_count,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE)
                 AND NOT COALESCE(ec.has_raw, FALSE)
                 AND COALESCE(ec.has_reported, FALSE)
                 THEN 1
               WHEN NOT COALESCE(ec.has_any_cost, FALSE)
                 AND COALESCE(sess.cost, 0) > 0
                 AND LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) NOT IN ('estimated', 'raw')
                 THEN 1
               ELSE 0
             END AS reported_count,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE) AND COALESCE(ec.has_estimated, FALSE)
                 THEN 1
               WHEN NOT COALESCE(ec.has_any_cost, FALSE)
                 AND COALESCE(sess.cost, 0) > 0
                 AND LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'estimated'
                 THEN 1
               ELSE 0
             END AS estimated_count
           FROM sessions AS sess
           INNER JOIN sources AS src ON src.id = sess.source_id
           LEFT JOIN event_cost AS ec ON ec.session_id = sess.id
           WHERE ${whereClauses.join(" AND ")}
         )
         SELECT
           date_trunc('day', started_at) AS day,
           COALESCE(SUM(tokens), 0)::bigint AS tokens,
           COUNT(*)::bigint AS sessions,
           COALESCE(SUM(cost_raw), 0)::double precision AS cost_raw,
           COALESCE(SUM(cost_estimated), 0)::double precision AS cost_estimated,
           COALESCE(SUM(raw_count), 0)::bigint AS raw_count,
           COALESCE(SUM(reported_count), 0)::bigint AS reported_count,
           COALESCE(SUM(estimated_count), 0)::bigint AS estimated_count
         FROM session_cost
         GROUP BY 1
         ORDER BY 1 ASC`,
        params
      );

      const dailyItems: UsageDailyBaseItem[] = result.rows.map((row) => {
        const cost = resolveUsageCostFromAggregateRow(row);
        return {
          date: toIsoString(row.day) ?? new Date(0).toISOString(),
          tokens: Math.max(0, Math.trunc(toNumber(row.tokens, 0))),
          sessions: Math.max(0, Math.trunc(toNumber(row.sessions, 0))),
          ...cost,
        };
      });

      return buildUsageDailyItems(dailyItems);
    } catch (error) {
      this.disableDb(error, "聚合 daily usage 失败");
      return this.listUsageDailyFromMemory(normalized);
    }
  }

  async listUsageMonthly(input?: UsageAggregateQueryInput): Promise<UsageMonthlyItem[]> {
    const normalized = normalizeUsageAggregateInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listUsageMonthlyFromMemory(normalized);
    }

    try {
      const params: unknown[] = [normalized.tenantId, DEFAULT_TENANT_ID];
      const whereClauses: string[] = [
        "COALESCE(NULLIF(src.tenant_id, ''), $2) = $1",
        "sess.started_at IS NOT NULL",
      ];
      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`sess.started_at >= $${params.length}::timestamptz`);
      }
      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`sess.started_at <= $${params.length}::timestamptz`);
      }
      if (normalized.project) {
        params.push(normalized.project);
        whereClauses.push(`LOWER(${USAGE_AGGREGATE_PROJECT_SQL}) = $${params.length}`);
      }

      const result = await pool.query(
        `WITH event_cost AS (
           SELECT
             session_id,
             COALESCE(SUM(CASE WHEN cost_mode = 'raw' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_raw,
             COALESCE(SUM(CASE WHEN cost_mode = 'estimated' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_estimated,
             COALESCE(SUM(CASE WHEN cost_mode = 'reported' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_reported,
             BOOL_OR(cost_mode = 'raw' AND cost_usd IS NOT NULL) AS has_raw,
             BOOL_OR(cost_mode = 'estimated' AND cost_usd IS NOT NULL) AS has_estimated,
             BOOL_OR(cost_mode = 'reported' AND cost_usd IS NOT NULL) AS has_reported,
             BOOL_OR(cost_usd IS NOT NULL) AS has_any_cost
           FROM events
           GROUP BY session_id
         ),
         session_cost AS (
           SELECT
             sess.id,
             sess.started_at,
             COALESCE(sess.tokens, 0)::bigint AS tokens,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE)
                 THEN COALESCE(ec.cost_raw, 0) + COALESCE(ec.cost_reported, 0)
               WHEN LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'estimated'
                 THEN 0
               ELSE COALESCE(sess.cost, 0)::double precision
             END AS cost_raw,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE)
                 THEN COALESCE(ec.cost_estimated, 0)
               WHEN LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'estimated'
                 THEN COALESCE(sess.cost, 0)::double precision
               ELSE 0
             END AS cost_estimated,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE) AND COALESCE(ec.has_raw, FALSE)
                 THEN 1
               WHEN NOT COALESCE(ec.has_any_cost, FALSE)
                 AND COALESCE(sess.cost, 0) > 0
                 AND LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'raw'
                 THEN 1
               ELSE 0
             END AS raw_count,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE)
                 AND NOT COALESCE(ec.has_raw, FALSE)
                 AND COALESCE(ec.has_reported, FALSE)
                 THEN 1
               WHEN NOT COALESCE(ec.has_any_cost, FALSE)
                 AND COALESCE(sess.cost, 0) > 0
                 AND LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) NOT IN ('estimated', 'raw')
                 THEN 1
               ELSE 0
             END AS reported_count,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE) AND COALESCE(ec.has_estimated, FALSE)
                 THEN 1
               WHEN NOT COALESCE(ec.has_any_cost, FALSE)
                 AND COALESCE(sess.cost, 0) > 0
                 AND LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'estimated'
                 THEN 1
               ELSE 0
             END AS estimated_count
           FROM sessions AS sess
           INNER JOIN sources AS src ON src.id = sess.source_id
           LEFT JOIN event_cost AS ec ON ec.session_id = sess.id
           WHERE ${whereClauses.join(" AND ")}
         )
         SELECT
           to_char(date_trunc('month', started_at), 'YYYY-MM-01T00:00:00.000Z') AS month,
           COALESCE(SUM(tokens), 0)::bigint AS tokens,
           COUNT(*)::bigint AS sessions,
           COALESCE(SUM(cost_raw), 0)::double precision AS cost_raw,
           COALESCE(SUM(cost_estimated), 0)::double precision AS cost_estimated,
           COALESCE(SUM(raw_count), 0)::bigint AS raw_count,
           COALESCE(SUM(reported_count), 0)::bigint AS reported_count,
           COALESCE(SUM(estimated_count), 0)::bigint AS estimated_count
         FROM session_cost
         GROUP BY 1
         ORDER BY 1 ASC`,
        params
      );

      return result.rows.map((row) => ({
        month: firstNonEmptyString(row.month) ?? new Date(0).toISOString(),
        tokens: Math.max(0, Math.trunc(toNumber(row.tokens, 0))),
        sessions: Math.max(0, Math.trunc(toNumber(row.sessions, 0))),
        ...resolveUsageCostFromAggregateRow(row),
      }));
    } catch (error) {
      this.disableDb(error, "聚合 monthly usage 失败");
      return this.listUsageMonthlyFromMemory(normalized);
    }
  }

  async listUsageModelRanking(input?: UsageAggregateQueryInput): Promise<UsageModelItem[]> {
    const normalized = normalizeUsageAggregateInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listUsageModelRankingFromMemory(normalized);
    }

    try {
      const params: unknown[] = [normalized.tenantId, DEFAULT_TENANT_ID];
      const whereClauses: string[] = [
        "COALESCE(NULLIF(src.tenant_id, ''), $2) = $1",
      ];
      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`sess.started_at >= $${params.length}::timestamptz`);
      }
      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`sess.started_at <= $${params.length}::timestamptz`);
      }
      if (normalized.project) {
        params.push(normalized.project);
        whereClauses.push(`LOWER(${USAGE_AGGREGATE_PROJECT_SQL}) = $${params.length}`);
      }
      params.push(normalized.limit);

      const result = await pool.query(
        `WITH event_cost AS (
           SELECT
             session_id,
             COALESCE(SUM(CASE WHEN cost_mode = 'raw' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_raw,
             COALESCE(SUM(CASE WHEN cost_mode = 'estimated' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_estimated,
             COALESCE(SUM(CASE WHEN cost_mode = 'reported' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_reported,
             BOOL_OR(cost_mode = 'raw' AND cost_usd IS NOT NULL) AS has_raw,
             BOOL_OR(cost_mode = 'estimated' AND cost_usd IS NOT NULL) AS has_estimated,
             BOOL_OR(cost_mode = 'reported' AND cost_usd IS NOT NULL) AS has_reported,
             BOOL_OR(cost_usd IS NOT NULL) AS has_any_cost
           FROM events
           GROUP BY session_id
         ),
         session_cost AS (
           SELECT
             sess.id,
             COALESCE(NULLIF(sess.model, ''), 'unknown') AS model,
             COALESCE(sess.tokens, 0)::bigint AS tokens,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE)
                 THEN COALESCE(ec.cost_raw, 0) + COALESCE(ec.cost_reported, 0)
               WHEN LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'estimated'
                 THEN 0
               ELSE COALESCE(sess.cost, 0)::double precision
             END AS cost_raw,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE)
                 THEN COALESCE(ec.cost_estimated, 0)
               WHEN LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'estimated'
                 THEN COALESCE(sess.cost, 0)::double precision
               ELSE 0
             END AS cost_estimated,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE) AND COALESCE(ec.has_raw, FALSE)
                 THEN 1
               WHEN NOT COALESCE(ec.has_any_cost, FALSE)
                 AND COALESCE(sess.cost, 0) > 0
                 AND LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'raw'
                 THEN 1
               ELSE 0
             END AS raw_count,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE)
                 AND NOT COALESCE(ec.has_raw, FALSE)
                 AND COALESCE(ec.has_reported, FALSE)
                 THEN 1
               WHEN NOT COALESCE(ec.has_any_cost, FALSE)
                 AND COALESCE(sess.cost, 0) > 0
                 AND LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) NOT IN ('estimated', 'raw')
                 THEN 1
               ELSE 0
             END AS reported_count,
             CASE
               WHEN COALESCE(ec.has_any_cost, FALSE) AND COALESCE(ec.has_estimated, FALSE)
                 THEN 1
               WHEN NOT COALESCE(ec.has_any_cost, FALSE)
                 AND COALESCE(sess.cost, 0) > 0
                 AND LOWER(COALESCE(NULLIF(to_jsonb(sess)->>'cost_mode', ''), NULLIF(to_jsonb(sess)->>'costMode', ''), 'reported')) = 'estimated'
                 THEN 1
               ELSE 0
             END AS estimated_count
           FROM sessions AS sess
           INNER JOIN sources AS src ON src.id = sess.source_id
           LEFT JOIN event_cost AS ec ON ec.session_id = sess.id
           WHERE ${whereClauses.join(" AND ")}
         )
         SELECT
           model,
           COALESCE(SUM(tokens), 0)::bigint AS tokens,
           COUNT(*)::bigint AS sessions,
           COALESCE(SUM(cost_raw), 0)::double precision AS cost_raw,
           COALESCE(SUM(cost_estimated), 0)::double precision AS cost_estimated,
           COALESCE(SUM(raw_count), 0)::bigint AS raw_count,
           COALESCE(SUM(reported_count), 0)::bigint AS reported_count,
           COALESCE(SUM(estimated_count), 0)::bigint AS estimated_count
         FROM session_cost
         GROUP BY model
         ORDER BY (COALESCE(SUM(cost_raw), 0) + COALESCE(SUM(cost_estimated), 0)) DESC, COALESCE(SUM(tokens), 0) DESC
         LIMIT $${params.length}`,
        params
      );

      return result.rows.map((row) => ({
        model: firstNonEmptyString(row.model) ?? "unknown",
        tokens: Math.max(0, Math.trunc(toNumber(row.tokens, 0))),
        sessions: Math.max(0, Math.trunc(toNumber(row.sessions, 0))),
        ...resolveUsageCostFromAggregateRow(row),
      }));
    } catch (error) {
      this.disableDb(error, "聚合 model ranking 失败");
      return this.listUsageModelRankingFromMemory(normalized);
    }
  }

  async listUsageSessionBreakdown(
    input?: UsageAggregateQueryInput
  ): Promise<UsageSessionBreakdownItem[]> {
    const normalized = normalizeUsageAggregateInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listUsageSessionBreakdownFromMemory(normalized);
    }

    try {
      const params: unknown[] = [normalized.tenantId, DEFAULT_TENANT_ID];
      const whereClauses: string[] = [
        "COALESCE(NULLIF(src.tenant_id, ''), $2) = $1",
      ];
      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`sess.started_at >= $${params.length}::timestamptz`);
      }
      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`sess.started_at <= $${params.length}::timestamptz`);
      }
      if (normalized.project) {
        params.push(normalized.project);
        whereClauses.push(`LOWER(${USAGE_AGGREGATE_PROJECT_SQL}) = $${params.length}`);
      }
      params.push(normalized.limit);

      const result = await pool.query(
        `WITH event_agg AS (
           SELECT
             session_id,
             COALESCE(SUM(input_tokens), 0)::bigint AS input_tokens,
             COALESCE(SUM(output_tokens), 0)::bigint AS output_tokens,
             COALESCE(SUM(cache_read_tokens), 0)::bigint AS cache_read_tokens,
             COALESCE(SUM(cache_write_tokens), 0)::bigint AS cache_write_tokens,
             COALESCE(SUM(reasoning_tokens), 0)::bigint AS reasoning_tokens,
             COALESCE(SUM(CASE WHEN cost_mode = 'raw' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_raw,
             COALESCE(SUM(CASE WHEN cost_mode = 'estimated' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_estimated,
             COALESCE(SUM(CASE WHEN cost_mode = 'reported' THEN COALESCE(cost_usd, 0) ELSE 0 END), 0)::double precision AS cost_reported,
             BOOL_OR(cost_mode = 'raw' AND cost_usd IS NOT NULL) AS has_raw,
             BOOL_OR(cost_mode = 'estimated' AND cost_usd IS NOT NULL) AS has_estimated,
             BOOL_OR(cost_mode = 'reported' AND cost_usd IS NOT NULL) AS has_reported,
             BOOL_OR(cost_usd IS NOT NULL) AS has_any_cost
           FROM events
           GROUP BY session_id
         )
         SELECT
           sess.id AS session_id,
           sess.source_id,
           COALESCE(NULLIF(sess.tool, ''), sess.provider, 'unknown') AS tool,
           COALESCE(NULLIF(sess.model, ''), 'unknown') AS model,
           sess.started_at,
           to_jsonb(sess) AS session_payload,
           COALESCE(evt.input_tokens, 0)::bigint AS input_tokens,
           COALESCE(evt.output_tokens, 0)::bigint AS output_tokens,
           COALESCE(evt.cache_read_tokens, 0)::bigint AS cache_read_tokens,
           COALESCE(evt.cache_write_tokens, 0)::bigint AS cache_write_tokens,
           COALESCE(evt.reasoning_tokens, 0)::bigint AS reasoning_tokens,
           COALESCE(sess.tokens, 0)::bigint AS total_tokens,
           COALESCE(sess.cost, 0)::double precision AS session_cost,
           COALESCE(evt.cost_raw, 0)::double precision AS cost_raw,
           COALESCE(evt.cost_estimated, 0)::double precision AS cost_estimated,
           COALESCE(evt.cost_reported, 0)::double precision AS cost_reported,
           COALESCE(evt.has_raw, FALSE) AS has_raw,
           COALESCE(evt.has_estimated, FALSE) AS has_estimated,
           COALESCE(evt.has_reported, FALSE) AS has_reported,
           COALESCE(evt.has_any_cost, FALSE) AS has_any_cost
         FROM sessions AS sess
         INNER JOIN sources AS src ON src.id = sess.source_id
         LEFT JOIN event_agg AS evt ON evt.session_id = sess.id
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY sess.started_at DESC NULLS LAST
         LIMIT $${params.length}`,
        params
      );

      return result.rows.map((row) => {
        const costSnapshot = resolveSessionUsageCostSnapshotFromAggregateRow(row);
        const { modeCounters: _modeCounters, ...cost } = costSnapshot;
        return {
          sessionId: firstNonEmptyString(row.session_id) ?? "",
          sourceId: firstNonEmptyString(row.source_id) ?? "",
          tool: firstNonEmptyString(row.tool) ?? "unknown",
          model: firstNonEmptyString(row.model) ?? "unknown",
          startedAt: toIsoString(row.started_at) ?? new Date(0).toISOString(),
          inputTokens: Math.max(0, Math.trunc(toNumber(row.input_tokens, 0))),
          outputTokens: Math.max(0, Math.trunc(toNumber(row.output_tokens, 0))),
          cacheReadTokens: Math.max(0, Math.trunc(toNumber(row.cache_read_tokens, 0))),
          cacheWriteTokens: Math.max(0, Math.trunc(toNumber(row.cache_write_tokens, 0))),
          reasoningTokens: Math.max(0, Math.trunc(toNumber(row.reasoning_tokens, 0))),
          totalTokens: Math.max(0, Math.trunc(toNumber(row.total_tokens, 0))),
          ...cost,
        };
      });
    } catch (error) {
      if (isPgUndefinedTable(error)) {
        return this.listUsageSessionBreakdownFromMemory(normalized);
      }
      this.disableDb(error, "聚合 session breakdown 失败");
      return this.listUsageSessionBreakdownFromMemory(normalized);
    }
  }

  async listUsageHeatmap(input?: UsageHeatmapQueryInput): Promise<HeatmapCell[]> {
    const normalized = normalizeUsageHeatmapInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listUsageHeatmapFromMemory(normalized);
    }

    try {
      const params: unknown[] = [normalized.tenantId, normalized.timezone, DEFAULT_TENANT_ID];
      const whereClauses: string[] = [
        "sessions.started_at IS NOT NULL",
        "COALESCE(NULLIF(sources.tenant_id, ''), $3) = $1",
      ];

      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`sessions.started_at >= $${params.length}::timestamptz`);
      }

      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`sessions.started_at <= $${params.length}::timestamptz`);
      }

      const result = await pool.query(
        `SELECT (date_trunc('day', sessions.started_at AT TIME ZONE $2) AT TIME ZONE $2) AS day,
                COALESCE(SUM(sessions.tokens), 0) AS tokens,
                COALESCE(SUM(sessions.cost), 0) AS cost,
                COUNT(*)::int AS sessions
         FROM sessions
         LEFT JOIN sources
           ON sources.id = sessions.source_id
         WHERE ${whereClauses.join(" AND ")}
         GROUP BY 1
         ORDER BY 1 ASC`,
        params
      );

      return result.rows
        .map(mapHeatmapRow)
        .filter((cell): cell is HeatmapCell => cell !== null);
    } catch (error) {
      this.disableDb(error, "聚合 usage heatmap 失败");
      return this.listUsageHeatmapFromMemory(normalized);
    }
  }

  async listPricingCatalogVersions(tenantId: string, limit = 20): Promise<PricingCatalog["version"][]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedLimit =
      Number.isFinite(limit) && Number.isInteger(limit) && limit > 0
        ? Math.min(limit, 200)
        : 20;
    const pool = await this.getPool();
    if (!pool) {
      return this.listPricingCatalogVersionsFromMemory(normalizedTenantId, normalizedLimit);
    }

    try {
      const result = await pool.query(
        `SELECT id, tenant_id, version, note, created_at
         FROM pricing_catalog_versions
         WHERE tenant_id = $1
         ORDER BY version DESC
         LIMIT $2`,
        [normalizedTenantId, normalizedLimit]
      );

      return result.rows.map((row) => ({
        id: firstNonEmptyString(row.id) ?? "",
        tenantId: firstNonEmptyString(row.tenant_id) ?? normalizedTenantId,
        version: Math.max(1, Math.trunc(toNumber(row.version, 1))),
        note: firstNonEmptyString(row.note) ?? undefined,
        createdAt: toIsoString(row.created_at) ?? new Date().toISOString(),
      }));
    } catch (error) {
      if (isPgUndefinedTable(error)) {
        return this.listPricingCatalogVersionsFromMemory(normalizedTenantId, normalizedLimit);
      }
      this.disableDb(error, "查询 pricing catalog versions 失败");
      return this.listPricingCatalogVersionsFromMemory(normalizedTenantId, normalizedLimit);
    }
  }

  async getPricingCatalog(tenantId: string): Promise<PricingCatalog | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const pool = await this.getPool();
    if (!pool) {
      return this.getPricingCatalogFromMemory(normalizedTenantId);
    }

    try {
      const versionResult = await pool.query(
        `SELECT id, tenant_id, version, note, created_at
         FROM pricing_catalog_versions
         WHERE tenant_id = $1
         ORDER BY version DESC
         LIMIT 1`,
        [normalizedTenantId]
      );
      const versionRow = versionResult.rows[0];
      if (!versionRow) {
        return null;
      }

      const versionId = firstNonEmptyString(versionRow.id);
      if (!versionId) {
        return null;
      }
      const entriesResult = await pool.query(
        `SELECT model_name, input_per_1k, output_per_1k, cache_read_per_1k, cache_write_per_1k, reasoning_per_1k, currency
         FROM pricing_catalog_entries
         WHERE tenant_id = $1
           AND version_id = $2
         ORDER BY model_name ASC`,
        [normalizedTenantId, versionId]
      );

      return {
        version: {
          id: versionId,
          tenantId: firstNonEmptyString(versionRow.tenant_id) ?? normalizedTenantId,
          version: Math.max(1, Math.trunc(toNumber(versionRow.version, 1))),
          note: firstNonEmptyString(versionRow.note) ?? undefined,
          createdAt: toIsoString(versionRow.created_at) ?? new Date().toISOString(),
        },
        entries: entriesResult.rows.map((row) => ({
          model: firstNonEmptyString(row.model_name) ?? "",
          inputPer1k: Math.max(0, toNumber(row.input_per_1k, 0)),
          outputPer1k: Math.max(0, toNumber(row.output_per_1k, 0)),
          cacheReadPer1k: row.cache_read_per_1k === null ? undefined : Math.max(0, toNumber(row.cache_read_per_1k, 0)),
          cacheWritePer1k: row.cache_write_per_1k === null ? undefined : Math.max(0, toNumber(row.cache_write_per_1k, 0)),
          reasoningPer1k: row.reasoning_per_1k === null ? undefined : Math.max(0, toNumber(row.reasoning_per_1k, 0)),
          currency: firstNonEmptyString(row.currency, "USD") ?? "USD",
        })),
      };
    } catch (error) {
      if (isPgUndefinedTable(error)) {
        return this.getPricingCatalogFromMemory(normalizedTenantId);
      }
      this.disableDb(error, "查询 pricing catalog 失败");
      return this.getPricingCatalogFromMemory(normalizedTenantId);
    }
  }

  async upsertPricingCatalog(
    tenantId: string,
    input: PricingCatalogUpsertInput
  ): Promise<PricingCatalog> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const now = new Date().toISOString();
    const pool = await this.getPool();
    if (!pool) {
      return this.upsertPricingCatalogToMemory(normalizedTenantId, input, now);
    }

    try {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const nextVersionResult = await client.query(
          `SELECT COALESCE(MAX(version), 0)::int + 1 AS next_version
           FROM pricing_catalog_versions
           WHERE tenant_id = $1`,
          [normalizedTenantId]
        );
        const nextVersion = Math.max(1, Math.trunc(toNumber(nextVersionResult.rows[0]?.next_version, 1)));
        const versionId = crypto.randomUUID();

        await client.query(
          `INSERT INTO pricing_catalog_versions (id, tenant_id, version, note, created_at)
           VALUES ($1, $2, $3, $4, $5::timestamptz)`,
          [versionId, normalizedTenantId, nextVersion, input.note ?? null, now]
        );

        for (const entry of input.entries) {
          await client.query(
            `INSERT INTO pricing_catalog_entries (
               id, tenant_id, version_id, model_name,
               input_per_1k, output_per_1k, cache_read_per_1k, cache_write_per_1k, reasoning_per_1k, currency,
               created_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::timestamptz)`,
            [
              crypto.randomUUID(),
              normalizedTenantId,
              versionId,
              entry.model,
              entry.inputPer1k,
              entry.outputPer1k,
              entry.cacheReadPer1k ?? null,
              entry.cacheWritePer1k ?? null,
              entry.reasoningPer1k ?? null,
              entry.currency ?? "USD",
              now,
            ]
          );
        }
        await client.query("COMMIT");

        return {
          version: {
            id: versionId,
            tenantId: normalizedTenantId,
            version: nextVersion,
            note: input.note,
            createdAt: now,
          },
          entries: input.entries.map((entry) => ({ ...entry })),
        };
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      } finally {
        client.release();
      }
    } catch (error) {
      if (isPgUndefinedTable(error)) {
        return this.upsertPricingCatalogToMemory(normalizedTenantId, input, now);
      }
      this.disableDb(error, "写入 pricing catalog 失败");
      return this.upsertPricingCatalogToMemory(normalizedTenantId, input, now);
    }
  }

  async listBudgets(tenantId: string): Promise<Budget[]> {
    const pool = await this.getPool();
    if (!pool) {
      return this.listBudgetsFromMemory(tenantId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                scope,
                source_id,
                organization_id,
                user_id,
                model_name,
                period,
                token_limit,
                cost_limit,
                alert_threshold,
                warning_threshold,
                escalated_threshold,
                critical_threshold,
                enabled,
                governance_state,
                freeze_reason,
                frozen_at,
                frozen_by_alert_id,
                updated_at
         FROM budgets
         WHERE tenant_id = $1
         ORDER BY updated_at DESC`,
        [tenantId]
      );

      return result.rows.map(mapBudgetRow);
    } catch (error) {
      this.disableDb(error, "查询 budgets 失败");
      return this.listBudgetsFromMemory(tenantId);
    }
  }

  async getBudgetById(tenantId: string, budgetId: string): Promise<Budget | null> {
    const normalizedBudgetId = firstNonEmptyString(budgetId);
    if (!normalizedBudgetId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getBudgetByIdFromMemory(tenantId, normalizedBudgetId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                scope,
                source_id,
                organization_id,
                user_id,
                model_name,
                period,
                token_limit,
                cost_limit,
                alert_threshold,
                warning_threshold,
                escalated_threshold,
                critical_threshold,
                enabled,
                governance_state,
                freeze_reason,
                frozen_at,
                frozen_by_alert_id,
                updated_at
         FROM budgets
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [tenantId, normalizedBudgetId]
      );
      const row = result.rows[0];
      return row ? mapBudgetRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询单条 budget 失败");
      return this.getBudgetByIdFromMemory(tenantId, normalizedBudgetId);
    }
  }

  async validateBudgetScopeBinding(
    tenantId: string,
    input: Pick<BudgetUpsertInput, "scope" | "organizationId" | "userId">
  ): Promise<BudgetScopeBindingValidationError | null> {
    const normalizedTenantId = firstNonEmptyString(tenantId) ?? DEFAULT_TENANT_ID;

    if (input.scope === "org") {
      const organizationId = firstNonEmptyString(input.organizationId);
      if (!organizationId) {
        return {
          field: "organizationId",
          message: "organizationId 必须为非空字符串。",
        };
      }

      const organization = await this.getOrganizationById(normalizedTenantId, organizationId);
      if (!organization) {
        return {
          field: "organizationId",
          message: "organizationId 不存在或不属于当前租户。",
        };
      }
    }

    if (input.scope === "user") {
      const userId = firstNonEmptyString(input.userId);
      if (!userId) {
        return {
          field: "userId",
          message: "userId 必须为非空字符串。",
        };
      }

      const [user, membership] = await Promise.all([
        this.getUserById(userId),
        this.getTenantMemberByUser(normalizedTenantId, userId),
      ]);
      if (!user || !membership) {
        return {
          field: "userId",
          message: "userId 不存在或不属于当前租户。",
        };
      }
    }

    return null;
  }

  async upsertBudget(tenantId: string, input: BudgetUpsertInput): Promise<Budget> {
    const sourceId = input.scope === "source" ? firstNonEmptyString(input.sourceId) ?? "" : "";
    const organizationId =
      input.scope === "org" ? firstNonEmptyString(input.organizationId) ?? "" : "";
    const userId = input.scope === "user" ? firstNonEmptyString(input.userId) ?? "" : "";
    const modelName = input.scope === "model" ? firstNonEmptyString(input.model) ?? "" : "";
    const resolvedThresholds = input.thresholds ?? {
      warning: toNumber(input.alertThreshold, 0.8),
      escalated: toNumber(input.alertThreshold, 0.8),
      critical: toNumber(input.alertThreshold, 0.8),
    };
    const warningThreshold = Number(Math.max(0, Math.min(resolvedThresholds.warning, 1)).toFixed(4));
    const escalatedThreshold = Number(
      Math.max(warningThreshold, Math.min(resolvedThresholds.escalated, 1)).toFixed(4)
    );
    const criticalThreshold = Number(
      Math.max(escalatedThreshold, Math.min(resolvedThresholds.critical, 1)).toFixed(4)
    );
    const normalizedInput: BudgetUpsertInput = {
      scope: input.scope,
      sourceId: input.scope === "source" ? sourceId : undefined,
      organizationId: input.scope === "org" ? organizationId : undefined,
      userId: input.scope === "user" ? userId : undefined,
      model: input.scope === "model" ? modelName : undefined,
      period: input.period,
      tokenLimit: Math.max(0, Math.trunc(input.tokenLimit)),
      costLimit: Number(Math.max(0, input.costLimit).toFixed(6)),
      thresholds: {
        warning: warningThreshold,
        escalated: escalatedThreshold,
        critical: criticalThreshold,
      },
      alertThreshold: warningThreshold,
    };
    const now = new Date().toISOString();

    const pool = await this.getPool();
    if (!pool) {
      return this.upsertBudgetToMemory(tenantId, normalizedInput, now);
    }

    try {
      const result = await pool.query(
        `INSERT INTO budgets (
           id,
           tenant_id,
           scope,
           source_id,
           organization_id,
           user_id,
           model_name,
           period,
           token_limit,
           cost_limit,
           alert_threshold,
           warning_threshold,
           escalated_threshold,
           critical_threshold,
           enabled,
           governance_state,
           freeze_reason,
           frozen_at,
           frozen_by_alert_id,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6,
           $7,
           $8,
           $9,
           $10,
           $11,
           $12,
           $13,
           $14,
           TRUE,
           'active',
           NULL,
           NULL,
           NULL,
           $15::timestamptz,
           $15::timestamptz
         )
         ON CONFLICT (tenant_id, scope, source_id, organization_id, user_id, model_name, period)
         DO UPDATE
           SET token_limit = EXCLUDED.token_limit,
               cost_limit = EXCLUDED.cost_limit,
               alert_threshold = EXCLUDED.alert_threshold,
               warning_threshold = EXCLUDED.warning_threshold,
               escalated_threshold = EXCLUDED.escalated_threshold,
               critical_threshold = EXCLUDED.critical_threshold,
               updated_at = EXCLUDED.updated_at
         RETURNING id,
                   scope,
                   source_id,
                   organization_id,
                   user_id,
                   model_name,
                   period,
                   token_limit,
                   cost_limit,
                   alert_threshold,
                   warning_threshold,
                   escalated_threshold,
                   critical_threshold,
                   enabled,
                   governance_state,
                   freeze_reason,
                   frozen_at,
                   frozen_by_alert_id,
                   updated_at`,
        [
          crypto.randomUUID(),
          tenantId,
          normalizedInput.scope,
          normalizedInput.scope === "source" ? sourceId : "",
          normalizedInput.scope === "org" ? organizationId : "",
          normalizedInput.scope === "user" ? userId : "",
          normalizedInput.scope === "model" ? modelName : "",
          normalizedInput.period,
          normalizedInput.tokenLimit,
          normalizedInput.costLimit,
          normalizedInput.alertThreshold ?? warningThreshold,
          normalizedInput.thresholds?.warning ?? warningThreshold,
          normalizedInput.thresholds?.escalated ?? escalatedThreshold,
          normalizedInput.thresholds?.critical ?? criticalThreshold,
          now,
        ]
      );

      const row = result.rows[0];
      if (!row) {
        return this.upsertBudgetToMemory(tenantId, normalizedInput, now);
      }
      return mapBudgetRow(row);
    } catch (error) {
      this.disableDb(error, "写入 budgets 失败");
      return this.upsertBudgetToMemory(tenantId, normalizedInput, now);
    }
  }

  async freezeBudget(
    tenantId: string,
    budgetId: string,
    input: {
      reason: string;
      alertId?: string;
      frozenAt?: string;
    }
  ): Promise<Budget | null> {
    const normalizedBudgetId = firstNonEmptyString(budgetId);
    if (!normalizedBudgetId) {
      return null;
    }

    const freezeReason = firstNonEmptyString(input.reason) ?? "触发治理冻结。";
    const frozenAt = toIsoString(input.frozenAt) ?? new Date().toISOString();
    const alertId = firstNonEmptyString(input.alertId) ?? undefined;
    const pool = await this.getPool();
    if (!pool) {
      return this.freezeBudgetInMemory(tenantId, normalizedBudgetId, {
        reason: freezeReason,
        alertId,
        frozenAt,
      });
    }

    try {
      const result = await pool.query(
        `UPDATE budgets
         SET governance_state = 'frozen',
             enabled = FALSE,
             freeze_reason = $3,
             frozen_at = $4::timestamptz,
             frozen_by_alert_id = $5,
             updated_at = $4::timestamptz
         WHERE tenant_id = $1
           AND id = $2
         RETURNING id,
                   scope,
                   source_id,
                   organization_id,
                   user_id,
                   model_name,
                   period,
                   token_limit,
                   cost_limit,
                   alert_threshold,
                   warning_threshold,
                   escalated_threshold,
                   critical_threshold,
                   enabled,
                   governance_state,
                   freeze_reason,
                   frozen_at,
                   frozen_by_alert_id,
                   updated_at`,
        [tenantId, normalizedBudgetId, freezeReason, frozenAt, alertId ?? null]
      );
      const row = result.rows[0];
      return row ? mapBudgetRow(row) : null;
    } catch (error) {
      this.disableDb(error, "冻结 budget 失败");
      return this.freezeBudgetInMemory(tenantId, normalizedBudgetId, {
        reason: freezeReason,
        alertId,
        frozenAt,
      });
    }
  }

  async markBudgetPendingRelease(tenantId: string, budgetId: string): Promise<Budget | null> {
    const normalizedBudgetId = firstNonEmptyString(budgetId);
    if (!normalizedBudgetId) {
      return null;
    }

    const updatedAt = new Date().toISOString();
    const pool = await this.getPool();
    if (!pool) {
      return this.markBudgetPendingReleaseInMemory(tenantId, normalizedBudgetId, updatedAt);
    }

    try {
      const result = await pool.query(
        `UPDATE budgets
         SET governance_state = 'pending_release',
             enabled = FALSE,
             updated_at = $3::timestamptz
         WHERE tenant_id = $1
           AND id = $2
         RETURNING id,
                   scope,
                   source_id,
                   organization_id,
                   user_id,
                   model_name,
                   period,
                   token_limit,
                   cost_limit,
                   alert_threshold,
                   warning_threshold,
                   escalated_threshold,
                   critical_threshold,
                   enabled,
                   governance_state,
                   freeze_reason,
                   frozen_at,
                   frozen_by_alert_id,
                   updated_at`,
        [tenantId, normalizedBudgetId, updatedAt]
      );
      const row = result.rows[0];
      return row ? mapBudgetRow(row) : null;
    } catch (error) {
      this.disableDb(error, "更新 budget pending_release 失败");
      return this.markBudgetPendingReleaseInMemory(tenantId, normalizedBudgetId, updatedAt);
    }
  }

  async restoreBudgetFrozenState(tenantId: string, budgetId: string): Promise<Budget | null> {
    const normalizedBudgetId = firstNonEmptyString(budgetId);
    if (!normalizedBudgetId) {
      return null;
    }

    const updatedAt = new Date().toISOString();
    const pool = await this.getPool();
    if (!pool) {
      return this.restoreBudgetFrozenStateInMemory(tenantId, normalizedBudgetId, updatedAt);
    }

    try {
      const result = await pool.query(
        `UPDATE budgets
         SET governance_state = 'frozen',
             enabled = FALSE,
             updated_at = $3::timestamptz
         WHERE tenant_id = $1
           AND id = $2
         RETURNING id,
                   scope,
                   source_id,
                   organization_id,
                   user_id,
                   model_name,
                   period,
                   token_limit,
                   cost_limit,
                   alert_threshold,
                   warning_threshold,
                   escalated_threshold,
                   critical_threshold,
                   enabled,
                   governance_state,
                   freeze_reason,
                   frozen_at,
                   frozen_by_alert_id,
                   updated_at`,
        [tenantId, normalizedBudgetId, updatedAt]
      );
      const row = result.rows[0];
      return row ? mapBudgetRow(row) : null;
    } catch (error) {
      this.disableDb(error, "恢复 budget 冻结状态失败");
      return this.restoreBudgetFrozenStateInMemory(tenantId, normalizedBudgetId, updatedAt);
    }
  }

  async activateBudget(tenantId: string, budgetId: string): Promise<Budget | null> {
    const normalizedBudgetId = firstNonEmptyString(budgetId);
    if (!normalizedBudgetId) {
      return null;
    }

    const updatedAt = new Date().toISOString();
    const pool = await this.getPool();
    if (!pool) {
      return this.activateBudgetInMemory(tenantId, normalizedBudgetId, updatedAt);
    }

    try {
      const result = await pool.query(
        `UPDATE budgets
         SET governance_state = 'active',
             enabled = TRUE,
             freeze_reason = NULL,
             frozen_at = NULL,
             frozen_by_alert_id = NULL,
             updated_at = $3::timestamptz
         WHERE tenant_id = $1
           AND id = $2
         RETURNING id,
                   scope,
                   source_id,
                   organization_id,
                   user_id,
                   model_name,
                   period,
                   token_limit,
                   cost_limit,
                   alert_threshold,
                   warning_threshold,
                   escalated_threshold,
                   critical_threshold,
                   enabled,
                   governance_state,
                   freeze_reason,
                   frozen_at,
                   frozen_by_alert_id,
                   updated_at`,
        [tenantId, normalizedBudgetId, updatedAt]
      );
      const row = result.rows[0];
      return row ? mapBudgetRow(row) : null;
    } catch (error) {
      this.disableDb(error, "解冻 budget 失败");
      return this.activateBudgetInMemory(tenantId, normalizedBudgetId, updatedAt);
    }
  }

  async createBudgetReleaseRequest(
    tenantId: string,
    budgetId: string,
    actor: ReleaseRequestActor,
    options?: CreateBudgetReleaseRequestOptions
  ): Promise<BudgetReleaseRequest | null> {
    const normalizedBudgetId = firstNonEmptyString(budgetId);
    const actorUserId = firstNonEmptyString(actor.userId);
    if (!normalizedBudgetId || !actorUserId) {
      return null;
    }

    const now = toIsoString(options?.requestedAt) ?? new Date().toISOString();
    const created: BudgetReleaseRequest = {
      id: crypto.randomUUID(),
      tenantId,
      budgetId: normalizedBudgetId,
      status: "pending",
      requestedByUserId: actorUserId,
      requestedByEmail: firstNonEmptyString(actor.email) ?? undefined,
      requestedAt: now,
      approvals: [],
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      const budget = await this.getBudgetById(tenantId, normalizedBudgetId);
      if (!budget || budget.governanceState === "active") {
        return null;
      }
      const existingPending = this.getPendingBudgetReleaseRequestFromMemory(
        tenantId,
        normalizedBudgetId
      );
      if (existingPending) {
        await this.markBudgetPendingRelease(tenantId, normalizedBudgetId);
        return existingPending;
      }
      const request = this.createBudgetReleaseRequestToMemory(created);
      await this.markBudgetPendingRelease(tenantId, normalizedBudgetId);
      return request;
    }

    try {
      return await this.withTransaction(pool, async (client) => {
        const budgetResult = await client.query(
          `SELECT id,
                  scope,
                  source_id,
                  organization_id,
                  user_id,
                  model_name,
                  period,
                  token_limit,
                  cost_limit,
                  alert_threshold,
                  warning_threshold,
                  escalated_threshold,
                  critical_threshold,
                  enabled,
                  governance_state,
                  freeze_reason,
                  frozen_at,
                  frozen_by_alert_id,
                  updated_at
           FROM budgets
           WHERE tenant_id = $1
             AND id = $2
           FOR UPDATE`,
          [tenantId, normalizedBudgetId]
        );
        const budgetRow = budgetResult.rows[0];
        if (!budgetRow) {
          return null;
        }

        const lockedBudget = mapBudgetRow(budgetRow);
        if (lockedBudget.governanceState === "active") {
          return null;
        }

        const pendingResult = await client.query(
          `SELECT id,
                  tenant_id,
                  budget_id,
                  status,
                  requested_by_user_id,
                  requested_by_email,
                  requested_at,
                  approvals,
                  rejected_by_user_id,
                  rejected_by_email,
                  rejected_reason,
                  rejected_at,
                  executed_at,
                  updated_at
           FROM budget_release_requests
           WHERE tenant_id = $1
             AND budget_id = $2
             AND status = 'pending'
           ORDER BY requested_at DESC, updated_at DESC
           LIMIT 1
           FOR UPDATE`,
          [tenantId, normalizedBudgetId]
        );

        const pendingRow = pendingResult.rows[0];
        if (pendingRow) {
          await client.query(
            `UPDATE budgets
             SET governance_state = 'pending_release',
                 enabled = FALSE,
                 updated_at = $3::timestamptz
             WHERE tenant_id = $1
               AND id = $2`,
            [tenantId, normalizedBudgetId, now]
          );
          return mapBudgetReleaseRequestRow(pendingRow);
        }

        const insertResult = await client.query(
          `INSERT INTO budget_release_requests (
             id,
             tenant_id,
             budget_id,
             status,
             requested_by_user_id,
             requested_by_email,
             requested_at,
             approvals,
             rejected_by_user_id,
             rejected_by_email,
             rejected_reason,
             rejected_at,
             executed_at,
             updated_at
           )
           VALUES (
             $1,
             $2,
             $3,
             $4,
             $5,
             $6,
             $7::timestamptz,
             '[]'::jsonb,
             NULL,
             NULL,
             NULL,
             NULL,
             NULL,
             $7::timestamptz
           )
           RETURNING id,
                     tenant_id,
                     budget_id,
                     status,
                     requested_by_user_id,
                     requested_by_email,
                     requested_at,
                     approvals,
                     rejected_by_user_id,
                     rejected_by_email,
                     rejected_reason,
                     rejected_at,
                     executed_at,
                     updated_at`,
          [
            created.id,
            created.tenantId,
            created.budgetId,
            created.status,
            created.requestedByUserId,
            created.requestedByEmail ?? null,
            created.requestedAt,
          ]
        );
        await client.query(
          `UPDATE budgets
           SET governance_state = 'pending_release',
               enabled = FALSE,
               updated_at = $3::timestamptz
           WHERE tenant_id = $1
             AND id = $2`,
          [tenantId, normalizedBudgetId, now]
        );

        const row = insertResult.rows[0];
        return row ? mapBudgetReleaseRequestRow(row) : created;
      });
    } catch (error) {
      if (isPgUniqueViolation(error)) {
        try {
          const existingResult = await pool.query(
            `SELECT id,
                    tenant_id,
                    budget_id,
                    status,
                    requested_by_user_id,
                    requested_by_email,
                    requested_at,
                    approvals,
                    rejected_by_user_id,
                    rejected_by_email,
                    rejected_reason,
                    rejected_at,
                    executed_at,
                    updated_at
             FROM budget_release_requests
             WHERE tenant_id = $1
               AND budget_id = $2
               AND status = 'pending'
             ORDER BY requested_at DESC, updated_at DESC
             LIMIT 1`,
            [tenantId, normalizedBudgetId]
          );
          const existingRow = existingResult.rows[0];
          if (existingRow) {
            return mapBudgetReleaseRequestRow(existingRow);
          }
        } catch {
          // noop: fallback to memory below
        }
      }

      this.disableDb(error, "创建预算释放申请失败");
      const budget = await this.getBudgetById(tenantId, normalizedBudgetId);
      if (!budget || budget.governanceState === "active") {
        return null;
      }
      const existingPending = this.getPendingBudgetReleaseRequestFromMemory(
        tenantId,
        normalizedBudgetId
      );
      if (existingPending) {
        await this.markBudgetPendingRelease(tenantId, normalizedBudgetId);
        return existingPending;
      }
      const request = this.createBudgetReleaseRequestToMemory(created);
      await this.markBudgetPendingRelease(tenantId, normalizedBudgetId);
      return request;
    }
  }

  async getBudgetReleaseRequestById(
    tenantId: string,
    budgetId: string,
    requestId: string
  ): Promise<BudgetReleaseRequest | null> {
    const normalizedBudgetId = firstNonEmptyString(budgetId);
    const normalizedRequestId = firstNonEmptyString(requestId);
    if (!normalizedBudgetId || !normalizedRequestId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getBudgetReleaseRequestByIdFromMemory(
        tenantId,
        normalizedBudgetId,
        normalizedRequestId
      );
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                budget_id,
                status,
                requested_by_user_id,
                requested_by_email,
                requested_at,
                approvals,
                rejected_by_user_id,
                rejected_by_email,
                rejected_reason,
                rejected_at,
                executed_at,
                updated_at
         FROM budget_release_requests
         WHERE tenant_id = $1
           AND budget_id = $2
           AND id = $3
         LIMIT 1`,
        [tenantId, normalizedBudgetId, normalizedRequestId]
      );
      const row = result.rows[0];
      return row ? mapBudgetReleaseRequestRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询预算释放申请失败");
      return this.getBudgetReleaseRequestByIdFromMemory(
        tenantId,
        normalizedBudgetId,
        normalizedRequestId
      );
    }
  }

  async listBudgetReleaseRequests(
    tenantId: string,
    budgetId: string,
    input?: ListBudgetReleaseRequestsInput
  ): Promise<BudgetReleaseRequest[]> {
    const normalizedBudgetId = firstNonEmptyString(budgetId);
    if (!normalizedBudgetId) {
      return [];
    }

    const rawStatus = firstNonEmptyString(input?.status);
    const status =
      rawStatus && BUDGET_RELEASE_REQUEST_STATUSES.includes(rawStatus as BudgetReleaseRequestStatus)
        ? (rawStatus as BudgetReleaseRequestStatus)
        : undefined;
    const limit = Math.min(
      200,
      Math.max(1, Number.isInteger(input?.limit) ? Number(input?.limit) : 50)
    );

    const pool = await this.getPool();
    if (!pool) {
      return this.listBudgetReleaseRequestsFromMemory(tenantId, normalizedBudgetId, {
        status,
        limit,
      });
    }

    try {
      const params: unknown[] = [tenantId, normalizedBudgetId];
      let statusFilterSQL = "";
      if (status) {
        params.push(status);
        statusFilterSQL = ` AND status = $${params.length}`;
      }
      params.push(limit);
      const limitPlaceHolder = `$${params.length}`;

      const result = await pool.query(
        `SELECT id,
                tenant_id,
                budget_id,
                status,
                requested_by_user_id,
                requested_by_email,
                requested_at,
                approvals,
                rejected_by_user_id,
                rejected_by_email,
                rejected_reason,
                rejected_at,
                executed_at,
                updated_at
         FROM budget_release_requests
         WHERE tenant_id = $1
           AND budget_id = $2${statusFilterSQL}
         ORDER BY requested_at DESC, updated_at DESC
         LIMIT ${limitPlaceHolder}`,
        params
      );
      return result.rows.map(mapBudgetReleaseRequestRow);
    } catch (error) {
      this.disableDb(error, "查询预算释放申请列表失败");
      return this.listBudgetReleaseRequestsFromMemory(tenantId, normalizedBudgetId, {
        status,
        limit,
      });
    }
  }

  async approveBudgetReleaseRequest(
    tenantId: string,
    budgetId: string,
    requestId: string,
    actor: ReleaseRequestActor
  ): Promise<BudgetReleaseRequest | null> {
    const normalizedBudgetId = firstNonEmptyString(budgetId);
    const normalizedRequestId = firstNonEmptyString(requestId);
    const actorUserId = firstNonEmptyString(actor.userId);
    if (!normalizedBudgetId || !normalizedRequestId || !actorUserId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      const current = await this.getBudgetReleaseRequestById(
        tenantId,
        normalizedBudgetId,
        normalizedRequestId
      );
      if (!current || current.status !== "pending") {
        return current;
      }
      if (current.requestedByUserId === actorUserId) {
        return current;
      }

      if (current.approvals.some((approval) => approval.userId === actorUserId)) {
        return current;
      }

      const now = new Date().toISOString();
      const approvals = [
        ...current.approvals,
        {
          userId: actorUserId,
          email: firstNonEmptyString(actor.email) ?? undefined,
          approvedAt: now,
        },
      ];
      const executed = approvals.length >= 2;
      const nextStatus: BudgetReleaseRequestStatus = executed ? "executed" : "pending";
      const updated = this.approveBudgetReleaseRequestInMemory(
        tenantId,
        normalizedBudgetId,
        normalizedRequestId,
        approvals,
        nextStatus,
        now
      );
      if (updated?.status === "executed") {
        await this.activateBudget(tenantId, normalizedBudgetId);
      }
      return updated;
    }

    try {
      return await this.withTransaction(pool, async (client) => {
        const currentResult = await client.query(
          `SELECT id,
                  tenant_id,
                  budget_id,
                  status,
                  requested_by_user_id,
                  requested_by_email,
                  requested_at,
                  approvals,
                  rejected_by_user_id,
                  rejected_by_email,
                  rejected_reason,
                  rejected_at,
                  executed_at,
                  updated_at
           FROM budget_release_requests
           WHERE tenant_id = $1
             AND budget_id = $2
             AND id = $3
           LIMIT 1
           FOR UPDATE`,
          [tenantId, normalizedBudgetId, normalizedRequestId]
        );
        const currentRow = currentResult.rows[0];
        if (!currentRow) {
          return null;
        }
        const current = mapBudgetReleaseRequestRow(currentRow);
        if (current.status !== "pending") {
          return current;
        }
        if (current.requestedByUserId === actorUserId) {
          return current;
        }
        if (current.approvals.some((approval) => approval.userId === actorUserId)) {
          return current;
        }

        const now = new Date().toISOString();
        const approvals = [
          ...current.approvals,
          {
            userId: actorUserId,
            email: firstNonEmptyString(actor.email) ?? undefined,
            approvedAt: now,
          },
        ];
        const nextStatus: BudgetReleaseRequestStatus =
          approvals.length >= 2 ? "executed" : "pending";
        const approvalsPayload = JSON.stringify(
          approvals.map((approval) => ({
            user_id: approval.userId,
            email: approval.email ?? null,
            approved_at: approval.approvedAt,
          }))
        );

        await client.query(
          `SELECT id
           FROM budgets
           WHERE tenant_id = $1
             AND id = $2
           FOR UPDATE`,
          [tenantId, normalizedBudgetId]
        );

        const updatedResult = await client.query(
          `UPDATE budget_release_requests
           SET status = $4,
               approvals = $5::jsonb,
               executed_at = CASE
                 WHEN $4 = 'executed'
                   THEN $6::timestamptz
                 ELSE executed_at
               END,
               updated_at = $6::timestamptz
           WHERE tenant_id = $1
             AND budget_id = $2
             AND id = $3
           RETURNING id,
                     tenant_id,
                     budget_id,
                     status,
                     requested_by_user_id,
                     requested_by_email,
                     requested_at,
                     approvals,
                     rejected_by_user_id,
                     rejected_by_email,
                     rejected_reason,
                     rejected_at,
                     executed_at,
                     updated_at`,
          [
            tenantId,
            normalizedBudgetId,
            normalizedRequestId,
            nextStatus,
            approvalsPayload,
            now,
          ]
        );
        const updatedRow = updatedResult.rows[0];
        if (!updatedRow) {
          return current;
        }
        const updated = mapBudgetReleaseRequestRow(updatedRow);

        if (updated.status === "executed") {
          await client.query(
            `UPDATE budgets
             SET governance_state = 'active',
                 enabled = TRUE,
                 freeze_reason = NULL,
                 frozen_at = NULL,
                 frozen_by_alert_id = NULL,
                 updated_at = $3::timestamptz
             WHERE tenant_id = $1
               AND id = $2`,
            [tenantId, normalizedBudgetId, now]
          );
        } else {
          await client.query(
            `UPDATE budgets
             SET governance_state = 'pending_release',
                 enabled = FALSE,
                 updated_at = $3::timestamptz
             WHERE tenant_id = $1
               AND id = $2`,
            [tenantId, normalizedBudgetId, now]
          );
        }

        return updated;
      });
    } catch (error) {
      this.disableDb(error, "审批预算释放申请失败");
      const current = await this.getBudgetReleaseRequestById(
        tenantId,
        normalizedBudgetId,
        normalizedRequestId
      );
      if (!current || current.status !== "pending") {
        return current;
      }
      if (current.requestedByUserId === actorUserId) {
        return current;
      }
      if (current.approvals.some((approval) => approval.userId === actorUserId)) {
        return current;
      }
      const now = new Date().toISOString();
      const approvals = [
        ...current.approvals,
        {
          userId: actorUserId,
          email: firstNonEmptyString(actor.email) ?? undefined,
          approvedAt: now,
        },
      ];
      const executed = approvals.length >= 2;
      const nextStatus: BudgetReleaseRequestStatus = executed ? "executed" : "pending";
      const updated = this.approveBudgetReleaseRequestInMemory(
        tenantId,
        normalizedBudgetId,
        normalizedRequestId,
        approvals,
        nextStatus,
        now
      );
      if (updated?.status === "executed") {
        await this.activateBudget(tenantId, normalizedBudgetId);
      }
      return updated;
    }
  }

  async rejectBudgetReleaseRequest(
    tenantId: string,
    budgetId: string,
    requestId: string,
    actor: ReleaseRequestActor,
    options?: RejectBudgetReleaseRequestOptions
  ): Promise<BudgetReleaseRequest | null> {
    const normalizedBudgetId = firstNonEmptyString(budgetId);
    const normalizedRequestId = firstNonEmptyString(requestId);
    const actorUserId = firstNonEmptyString(actor.userId);
    if (!normalizedBudgetId || !normalizedRequestId || !actorUserId) {
      return null;
    }

    const rejectedAt = toIsoString(options?.rejectedAt) ?? new Date().toISOString();
    const rejectedReason = firstNonEmptyString(options?.reason) ?? undefined;
    const rejectedByEmail = firstNonEmptyString(actor.email) ?? undefined;
    const pool = await this.getPool();
    if (!pool) {
      const current = await this.getBudgetReleaseRequestById(
        tenantId,
        normalizedBudgetId,
        normalizedRequestId
      );
      if (!current || current.status !== "pending") {
        return current;
      }
      const updated = this.rejectBudgetReleaseRequestInMemory(
        tenantId,
        normalizedBudgetId,
        normalizedRequestId,
        actorUserId,
        rejectedByEmail,
        rejectedReason,
        rejectedAt
      );
      if (updated) {
        await this.restoreBudgetFrozenState(tenantId, normalizedBudgetId);
      }
      return updated;
    }

    try {
      return await this.withTransaction(pool, async (client) => {
        const currentResult = await client.query(
          `SELECT id,
                  tenant_id,
                  budget_id,
                  status,
                  requested_by_user_id,
                  requested_by_email,
                  requested_at,
                  approvals,
                  rejected_by_user_id,
                  rejected_by_email,
                  rejected_reason,
                  rejected_at,
                  executed_at,
                  updated_at
           FROM budget_release_requests
           WHERE tenant_id = $1
             AND budget_id = $2
             AND id = $3
           LIMIT 1
           FOR UPDATE`,
          [tenantId, normalizedBudgetId, normalizedRequestId]
        );
        const currentRow = currentResult.rows[0];
        if (!currentRow) {
          return null;
        }
        const current = mapBudgetReleaseRequestRow(currentRow);
        if (current.status !== "pending") {
          return current;
        }

        await client.query(
          `SELECT id
           FROM budgets
           WHERE tenant_id = $1
             AND id = $2
           FOR UPDATE`,
          [tenantId, normalizedBudgetId]
        );

        const updatedResult = await client.query(
          `UPDATE budget_release_requests
           SET status = 'rejected',
               rejected_by_user_id = $4,
               rejected_by_email = $5,
               rejected_reason = $6,
               rejected_at = $7::timestamptz,
               updated_at = $7::timestamptz
           WHERE tenant_id = $1
             AND budget_id = $2
             AND id = $3
           RETURNING id,
                     tenant_id,
                     budget_id,
                     status,
                     requested_by_user_id,
                     requested_by_email,
                     requested_at,
                     approvals,
                     rejected_by_user_id,
                     rejected_by_email,
                     rejected_reason,
                     rejected_at,
                     executed_at,
                     updated_at`,
          [
            tenantId,
            normalizedBudgetId,
            normalizedRequestId,
            actorUserId,
            rejectedByEmail ?? null,
            rejectedReason ?? null,
            rejectedAt,
          ]
        );
        const updatedRow = updatedResult.rows[0];
        if (!updatedRow) {
          return current;
        }

        await client.query(
          `UPDATE budgets
           SET governance_state = 'frozen',
               enabled = FALSE,
               updated_at = $3::timestamptz
           WHERE tenant_id = $1
             AND id = $2`,
          [tenantId, normalizedBudgetId, rejectedAt]
        );
        return mapBudgetReleaseRequestRow(updatedRow);
      });
    } catch (error) {
      this.disableDb(error, "驳回预算释放申请失败");
      const current = await this.getBudgetReleaseRequestById(
        tenantId,
        normalizedBudgetId,
        normalizedRequestId
      );
      if (!current || current.status !== "pending") {
        return current;
      }
      const updated = this.rejectBudgetReleaseRequestInMemory(
        tenantId,
        normalizedBudgetId,
        normalizedRequestId,
        actorUserId,
        rejectedByEmail,
        rejectedReason,
        rejectedAt
      );
      if (updated) {
        await this.restoreBudgetFrozenState(tenantId, normalizedBudgetId);
      }
      return updated;
    }
  }

  async claimIntegrationAlertCallback(
    input: Pick<IntegrationAlertCallbackRecord, "callbackId" | "tenantId" | "action"> & {
      processedAt?: string;
      staleAfterMs?: number;
    }
  ): Promise<ClaimIntegrationAlertCallbackResult> {
    const staleAfterMs =
      typeof input.staleAfterMs === "number" &&
      Number.isFinite(input.staleAfterMs) &&
      input.staleAfterMs > 0
        ? Math.trunc(input.staleAfterMs)
        : DEFAULT_CALLBACK_CLAIM_STALE_AFTER_MS;
    const normalizedRecord: IntegrationAlertCallbackRecord = {
      callbackId: firstNonEmptyString(input.callbackId) ?? crypto.randomUUID(),
      tenantId: firstNonEmptyString(input.tenantId) ?? DEFAULT_TENANT_ID,
      action: input.action,
      response: {
        state: "processing",
      },
      processedAt: toIsoString(input.processedAt) ?? new Date().toISOString(),
    };
    const staleBefore = new Date(
      Date.parse(normalizedRecord.processedAt) - staleAfterMs
    ).toISOString();

    const pool = await this.getPool();
    if (!pool) {
      return this.claimIntegrationAlertCallbackToMemory(normalizedRecord);
    }

    try {
      const claimResult = await pool.query(
        `INSERT INTO integration_alert_callbacks (
           callback_id,
           tenant_id,
           action,
           response_payload,
           processed_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4::jsonb,
           $5::timestamptz
         )
         ON CONFLICT (tenant_id, callback_id)
         DO UPDATE
           SET action = EXCLUDED.action,
               response_payload = EXCLUDED.response_payload,
               processed_at = EXCLUDED.processed_at
         WHERE COALESCE(integration_alert_callbacks.response_payload ->> 'state', '') = 'processing'
           AND integration_alert_callbacks.processed_at <= $6::timestamptz
         RETURNING callback_id,
                   tenant_id,
                   action,
                   response_payload,
                   processed_at`,
        [
          normalizedRecord.callbackId,
          normalizedRecord.tenantId,
          normalizedRecord.action,
          safeStringifyJson(normalizedRecord.response),
          normalizedRecord.processedAt,
          staleBefore,
        ]
      );
      const claimedRow = claimResult.rows[0];
      if (claimedRow) {
        return {
          claimed: true,
          record: {
            callbackId:
              firstNonEmptyString(claimedRow.callback_id) ?? normalizedRecord.callbackId,
            tenantId: firstNonEmptyString(claimedRow.tenant_id) ?? normalizedRecord.tenantId,
            action: toIntegrationAlertCallbackAction(firstNonEmptyString(claimedRow.action)),
            response: toDbRow(claimedRow.response_payload) ?? { ...normalizedRecord.response },
            processedAt: toIsoString(claimedRow.processed_at) ?? normalizedRecord.processedAt,
          },
        };
      }

      const existing = await this.getIntegrationAlertCallbackById(
        normalizedRecord.callbackId,
        normalizedRecord.tenantId
      );
      if (existing) {
        return {
          claimed: false,
          record: existing,
        };
      }

      return {
        claimed: false,
        record: normalizedRecord,
      };
    } catch (error) {
      this.disableDb(error, "claim integration callback 幂等记录失败");
      return this.claimIntegrationAlertCallbackToMemory(normalizedRecord, staleAfterMs);
    }
  }

  async getIntegrationAlertCallbackById(
    callbackId: string,
    tenantId?: string
  ): Promise<IntegrationAlertCallbackRecord | null> {
    const normalizedCallbackId = firstNonEmptyString(callbackId);
    const normalizedTenantId = firstNonEmptyString(tenantId) ?? DEFAULT_TENANT_ID;
    if (!normalizedCallbackId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getIntegrationAlertCallbackByIdFromMemory(
        normalizedTenantId,
        normalizedCallbackId
      );
    }

    try {
      const result = await pool.query(
        `SELECT callback_id,
                tenant_id,
                action,
                response_payload,
                processed_at
         FROM integration_alert_callbacks
         WHERE tenant_id = $1
           AND callback_id = $2
         LIMIT 1`,
        [normalizedTenantId, normalizedCallbackId]
      );
      const row = result.rows[0];
      if (!row) {
        return null;
      }
      return {
        callbackId: firstNonEmptyString(row.callback_id) ?? normalizedCallbackId,
        tenantId: firstNonEmptyString(row.tenant_id) ?? normalizedTenantId,
        action: toIntegrationAlertCallbackAction(firstNonEmptyString(row.action)),
        response: toDbRow(row.response_payload) ?? {},
        processedAt: toIsoString(row.processed_at) ?? new Date().toISOString(),
      };
    } catch (error) {
      this.disableDb(error, "查询 integration callback 幂等记录失败");
      return this.getIntegrationAlertCallbackByIdFromMemory(
        normalizedTenantId,
        normalizedCallbackId
      );
    }
  }

  async saveIntegrationAlertCallback(
    record: IntegrationAlertCallbackRecord
  ): Promise<IntegrationAlertCallbackRecord> {
    const normalizedRecord: IntegrationAlertCallbackRecord = {
      callbackId: firstNonEmptyString(record.callbackId) ?? crypto.randomUUID(),
      tenantId: firstNonEmptyString(record.tenantId) ?? DEFAULT_TENANT_ID,
      action: record.action,
      response: toDbRow(record.response) ?? {},
      processedAt: toIsoString(record.processedAt) ?? new Date().toISOString(),
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.saveIntegrationAlertCallbackToMemory(normalizedRecord);
    }

    try {
      const result = await pool.query(
        `INSERT INTO integration_alert_callbacks (
           callback_id,
           tenant_id,
           action,
           response_payload,
           processed_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4::jsonb,
           $5::timestamptz
         )
         ON CONFLICT (tenant_id, callback_id)
         DO UPDATE
           SET action = EXCLUDED.action,
               response_payload = EXCLUDED.response_payload,
               processed_at = EXCLUDED.processed_at
         RETURNING callback_id,
                   tenant_id,
                   action,
                   response_payload,
                   processed_at`,
        [
          normalizedRecord.callbackId,
          normalizedRecord.tenantId,
          normalizedRecord.action,
          safeStringifyJson(normalizedRecord.response),
          normalizedRecord.processedAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.saveIntegrationAlertCallbackToMemory(normalizedRecord);
      }
      return {
        callbackId: firstNonEmptyString(row.callback_id) ?? normalizedRecord.callbackId,
        tenantId: firstNonEmptyString(row.tenant_id) ?? normalizedRecord.tenantId,
        action: toIntegrationAlertCallbackAction(firstNonEmptyString(row.action)),
        response: toDbRow(row.response_payload) ?? {},
        processedAt: toIsoString(row.processed_at) ?? normalizedRecord.processedAt,
      };
    } catch (error) {
      this.disableDb(error, "写入 integration callback 幂等记录失败");
      return this.saveIntegrationAlertCallbackToMemory(normalizedRecord);
    }
  }

  async listAlerts(tenantId: string, input: AlertListInput): Promise<AlertListResult> {
    const normalized = normalizeAlertListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listAlertsFromMemory(tenantId, normalized);
    }

    try {
      const params: unknown[] = [tenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      const cursor = decodeTimePaginationCursor(normalized.cursor);

      if (normalized.status) {
        params.push(normalized.status);
        whereClauses.push(`status = $${params.length}`);
      }

      if (normalized.severity) {
        params.push(normalized.severity);
        whereClauses.push(`severity = $${params.length}`);
      }

      if (normalized.sourceId) {
        params.push(normalized.sourceId);
        whereClauses.push(`source_id = $${params.length}`);
      }

      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`created_at >= $${params.length}::timestamptz`);
      }

      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`created_at <= $${params.length}::timestamptz`);
      }

      const whereSql = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total
         FROM governance_alerts
         ${whereSql}`,
        params
      );

      const listParams = [...params];
      const listWhereClauses = [...whereClauses];
      if (cursor) {
        listParams.push(cursor.timestamp);
        const timestampToken = `$${listParams.length}`;
        listParams.push(cursor.id);
        const idToken = `$${listParams.length}`;
        listWhereClauses.push(
          `(created_at < ${timestampToken}::timestamptz
            OR (created_at = ${timestampToken}::timestamptz AND id::text < ${idToken}))`
        );
      }
      const listWhereSql =
        listWhereClauses.length > 0 ? `WHERE ${listWhereClauses.join(" AND ")}` : "";
      listParams.push(normalized.limit + 1);
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                budget_id,
                source_id,
                period,
                window_start,
                window_end,
                tokens_used,
                cost_used,
                token_limit,
                cost_limit,
                threshold,
                status,
                severity,
                updated_at,
                created_at,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
                  AS created_at_cursor
         FROM governance_alerts
         ${listWhereSql}
         ORDER BY created_at DESC, id::text DESC
         LIMIT $${listParams.length}`,
        listParams
      );

      const mappedItems = result.rows.map(mapAlertRow);
      const hasMore = mappedItems.length > normalized.limit;
      const items = hasMore ? mappedItems.slice(0, normalized.limit) : mappedItems;
      const cursorRows = hasMore ? result.rows.slice(0, normalized.limit) : result.rows;
      const lastItem = items[items.length - 1];
      const lastCursorRow = cursorRows[cursorRows.length - 1];
      const cursorTimestamp =
        firstNonEmptyString((lastCursorRow as DbRow | undefined)?.created_at_cursor) ??
        toIsoString((lastCursorRow as DbRow | undefined)?.created_at);
      const nextCursor =
        hasMore &&
        lastItem &&
        typeof cursorTimestamp === "string" &&
        Number.isFinite(Date.parse(cursorTimestamp)) &&
        lastItem.id.trim().length > 0
          ? encodeTimePaginationCursor({
              timestamp: cursorTimestamp,
              id: lastItem.id,
            })
          : null;

      return {
        items,
        total: Math.max(0, Math.trunc(toNumber(countResult.rows[0]?.total, 0))),
        nextCursor,
      };
    } catch (error) {
      this.disableDb(error, "查询 alerts 失败");
      return this.listAlertsFromMemory(tenantId, normalized);
    }
  }

  async getAlertById(tenantId: string, alertId: string): Promise<Alert | null> {
    const normalizedAlertId = firstNonEmptyString(alertId);
    if (!normalizedAlertId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getAlertByIdFromMemory(tenantId, normalizedAlertId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                budget_id,
                source_id,
                period,
                window_start,
                window_end,
                tokens_used,
                cost_used,
                token_limit,
                cost_limit,
                threshold,
                status,
                severity,
                updated_at,
                created_at
         FROM governance_alerts
         WHERE tenant_id = $1
           AND id::text = $2
         LIMIT 1`,
        [tenantId, normalizedAlertId]
      );

      const row = result.rows[0];
      return row ? mapAlertRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询单条 alert 失败");
      return this.getAlertByIdFromMemory(tenantId, normalizedAlertId);
    }
  }

  async updateAlertStatus(
    tenantId: string,
    alertId: string,
    status: AlertMutableStatus
  ): Promise<Alert | null> {
    const normalizedAlertId = firstNonEmptyString(alertId);
    if (!normalizedAlertId) {
      return null;
    }

    const updatedAt = new Date().toISOString();
    const pool = await this.getPool();
    if (!pool) {
      return this.updateAlertStatusInMemory(tenantId, normalizedAlertId, status, updatedAt);
    }

    try {
      const result = await pool.query(
        `UPDATE governance_alerts
         SET status = $3,
             updated_at = $4::timestamptz
         WHERE tenant_id = $1
           AND id::text = $2
           AND (
             status = $3
             OR (status = 'open' AND $3 IN ('acknowledged', 'resolved'))
             OR (status = 'acknowledged' AND $3 = 'resolved')
           )
         RETURNING id,
                   tenant_id,
                   budget_id,
                   source_id,
                   period,
                   window_start,
                   window_end,
                   tokens_used,
                   cost_used,
                   token_limit,
                   cost_limit,
                   threshold,
                   status,
                   severity,
                   updated_at,
                   created_at`,
        [tenantId, normalizedAlertId, status, updatedAt]
      );

      const row = result.rows[0];
      if (row) {
        return mapAlertRow(row);
      }

      const currentResult = await pool.query(
        `SELECT id,
                tenant_id,
                budget_id,
                source_id,
                period,
                window_start,
                window_end,
                tokens_used,
                cost_used,
                token_limit,
                cost_limit,
                threshold,
                status,
                severity,
                updated_at,
                created_at
         FROM governance_alerts
         WHERE tenant_id = $1
           AND id::text = $2
         LIMIT 1`,
        [tenantId, normalizedAlertId]
      );
      const currentRow = currentResult.rows[0];
      return currentRow ? mapAlertRow(currentRow) : null;
    } catch (error) {
      this.disableDb(error, "更新 alerts 状态失败");
      return this.updateAlertStatusInMemory(tenantId, normalizedAlertId, status, updatedAt);
    }
  }

  async listAlertOrchestrationRules(
    tenantId: string,
    input: AlertOrchestrationRuleListInput = {}
  ): Promise<AlertOrchestrationRuleListResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeAlertOrchestrationRuleListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listAlertOrchestrationRulesFromMemory(normalizedTenantId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];

      if (normalized.eventType) {
        params.push(normalized.eventType);
        whereClauses.push(`event_type = $${params.length}`);
      }
      if (normalized.enabled !== undefined) {
        params.push(normalized.enabled);
        whereClauses.push(`enabled = $${params.length}`);
      }
      if (normalized.severity) {
        params.push(normalized.severity);
        whereClauses.push(`severity = $${params.length}`);
      }
      if (normalized.sourceId) {
        params.push(normalized.sourceId);
        whereClauses.push(`source_id = $${params.length}`);
      }

      const whereSql = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";
      const result = await pool.query(
        `SELECT tenant_id,
                id,
                name,
                enabled,
                event_type,
                severity,
                source_id,
                dedupe_window_seconds,
                suppression_window_seconds,
                merge_window_seconds,
                sla_minutes,
                channels,
                updated_at
         FROM alert_orchestration_rules
         ${whereSql}
         ORDER BY updated_at DESC, id ASC`,
        params
      );
      const items = result.rows.map(mapAlertOrchestrationRuleRow);
      return {
        items,
        total: items.length,
      };
    } catch (error) {
      this.disableDb(error, "查询 alert orchestration rules 失败");
      return this.listAlertOrchestrationRulesFromMemory(normalizedTenantId, normalized);
    }
  }

  async getAlertOrchestrationRuleById(
    tenantId: string,
    ruleId: string
  ): Promise<AlertOrchestrationRule | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedRuleId = firstNonEmptyString(ruleId);
    if (!normalizedRuleId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getAlertOrchestrationRuleByIdFromMemory(normalizedTenantId, normalizedRuleId);
    }

    try {
      const result = await pool.query(
        `SELECT tenant_id,
                id,
                name,
                enabled,
                event_type,
                severity,
                source_id,
                dedupe_window_seconds,
                suppression_window_seconds,
                merge_window_seconds,
                sla_minutes,
                channels,
                updated_at
         FROM alert_orchestration_rules
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [normalizedTenantId, normalizedRuleId]
      );
      const row = result.rows[0];
      return row ? mapAlertOrchestrationRuleRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询单条 alert orchestration rule 失败");
      return this.getAlertOrchestrationRuleByIdFromMemory(normalizedTenantId, normalizedRuleId);
    }
  }

  async upsertAlertOrchestrationRule(
    tenantId: string,
    input: AlertOrchestrationRuleUpsertInput
  ): Promise<AlertOrchestrationRule> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedRuleId = firstNonEmptyString(input.id) ?? crypto.randomUUID();
    const normalizedName = firstNonEmptyString(input.name) ?? normalizedRuleId;
    const normalizedEventType = toAlertOrchestrationEventType(input.eventType);
    const severityRaw = firstNonEmptyString(input.severity);
    const sourceId = firstNonEmptyString(input.sourceId) ?? undefined;
    const updatedAt = toIsoString(input.updatedAt) ?? new Date().toISOString();
    const channels: AlertOrchestrationChannel[] = [];
    const channelSet = new Set<AlertOrchestrationChannel>();
    for (const channel of Array.isArray(input.channels) ? input.channels : []) {
      const normalizedChannel = firstNonEmptyString(channel);
      if (!normalizedChannel) {
        continue;
      }
      const mappedChannel = toAlertOrchestrationChannel(normalizedChannel.toLowerCase());
      if (!mappedChannel) {
        continue;
      }
      if (channelSet.has(mappedChannel)) {
        continue;
      }
      channelSet.add(mappedChannel);
      channels.push(mappedChannel);
    }

    const normalizedInput: AlertOrchestrationRuleUpsertInput = {
      id: normalizedRuleId,
      tenantId: normalizedTenantId,
      name: normalizedName,
      enabled: input.enabled === true,
      eventType: normalizedEventType,
      severity: severityRaw ? toAlertSeverity(severityRaw) : undefined,
      sourceId,
      dedupeWindowSeconds: toOptionalNonNegativeInteger(input.dedupeWindowSeconds) ?? 0,
      suppressionWindowSeconds: toOptionalNonNegativeInteger(input.suppressionWindowSeconds) ?? 0,
      mergeWindowSeconds: toOptionalNonNegativeInteger(input.mergeWindowSeconds) ?? 0,
      slaMinutes: toOptionalNonNegativeInteger(input.slaMinutes),
      channels,
      updatedAt,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.upsertAlertOrchestrationRuleToMemory(normalizedTenantId, normalizedInput);
    }

    try {
      const result = await pool.query(
        `INSERT INTO alert_orchestration_rules (
           tenant_id,
           id,
           name,
           enabled,
           event_type,
           severity,
           source_id,
           dedupe_window_seconds,
           suppression_window_seconds,
           merge_window_seconds,
           sla_minutes,
           channels,
           updated_at,
           created_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6,
           $7,
           $8,
           $9,
           $10,
           $11,
           $12::jsonb,
           $13::timestamptz,
           $13::timestamptz
         )
         ON CONFLICT (tenant_id, id)
         DO UPDATE
           SET name = EXCLUDED.name,
               enabled = EXCLUDED.enabled,
               event_type = EXCLUDED.event_type,
               severity = EXCLUDED.severity,
               source_id = EXCLUDED.source_id,
               dedupe_window_seconds = EXCLUDED.dedupe_window_seconds,
               suppression_window_seconds = EXCLUDED.suppression_window_seconds,
               merge_window_seconds = EXCLUDED.merge_window_seconds,
               sla_minutes = EXCLUDED.sla_minutes,
               channels = EXCLUDED.channels,
               updated_at = EXCLUDED.updated_at
         RETURNING tenant_id,
                   id,
                   name,
                   enabled,
                   event_type,
                   severity,
                   source_id,
                   dedupe_window_seconds,
                   suppression_window_seconds,
                   merge_window_seconds,
                   sla_minutes,
                   channels,
                   updated_at`,
        [
          normalizedTenantId,
          normalizedInput.id,
          normalizedInput.name,
          normalizedInput.enabled,
          normalizedInput.eventType,
          normalizedInput.severity ?? null,
          normalizedInput.sourceId ?? null,
          normalizedInput.dedupeWindowSeconds,
          normalizedInput.suppressionWindowSeconds,
          normalizedInput.mergeWindowSeconds,
          normalizedInput.slaMinutes ?? null,
          safeStringifyJson(normalizedInput.channels),
          normalizedInput.updatedAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.upsertAlertOrchestrationRuleToMemory(normalizedTenantId, normalizedInput);
      }
      return mapAlertOrchestrationRuleRow(row);
    } catch (error) {
      this.disableDb(error, "写入 alert orchestration rule 失败");
      return this.upsertAlertOrchestrationRuleToMemory(normalizedTenantId, normalizedInput);
    }
  }

  async listAlertOrchestrationExecutionLogs(
    tenantId: string,
    input: AlertOrchestrationExecutionListInput = {}
  ): Promise<AlertOrchestrationExecutionListResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeAlertOrchestrationExecutionListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listAlertOrchestrationExecutionLogsFromMemory(normalizedTenantId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      const fallbackDispatchWhereSql =
        "(COALESCE(metadata ->> 'dispatchMode', '') = 'fallback' OR COALESCE(metadata ->> 'fallback', '') = 'true')";

      if (normalized.ruleId) {
        params.push(normalized.ruleId);
        whereClauses.push(`rule_id = $${params.length}`);
      }
      if (normalized.eventType) {
        params.push(normalized.eventType);
        whereClauses.push(`event_type = $${params.length}`);
      }
      if (normalized.alertId) {
        params.push(normalized.alertId);
        whereClauses.push(`alert_id = $${params.length}`);
      }
      if (normalized.severity) {
        params.push(normalized.severity);
        whereClauses.push(`severity = $${params.length}`);
      }
      if (normalized.sourceId) {
        params.push(normalized.sourceId);
        whereClauses.push(`source_id = $${params.length}`);
      }
      if (normalized.dedupeHit !== undefined) {
        params.push(normalized.dedupeHit);
        whereClauses.push(`dedupe_hit = $${params.length}`);
      }
      if (normalized.suppressed !== undefined) {
        params.push(normalized.suppressed);
        whereClauses.push(`suppressed = $${params.length}`);
      }
      if (normalized.dispatchMode) {
        whereClauses.push(
          normalized.dispatchMode === "fallback"
            ? fallbackDispatchWhereSql
            : `NOT ${fallbackDispatchWhereSql}`
        );
      }
      if (normalized.hasConflict !== undefined) {
        whereClauses.push(
          normalized.hasConflict
            ? "jsonb_array_length(COALESCE(conflict_rule_ids, '[]'::jsonb)) > 0"
            : "jsonb_array_length(COALESCE(conflict_rule_ids, '[]'::jsonb)) = 0"
        );
      }
      if (normalized.simulated !== undefined) {
        params.push(normalized.simulated);
        whereClauses.push(`simulated = $${params.length}`);
      }
      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`created_at >= $${params.length}::timestamptz`);
      }
      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`created_at <= $${params.length}::timestamptz`);
      }

      const whereSql = `WHERE ${whereClauses.join(" AND ")}`;
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total
         FROM alert_orchestration_executions
         ${whereSql}`,
        params
      );

      const listParams = [...params, normalized.limit];
      const listResult = await pool.query(
        `SELECT id,
                tenant_id,
                rule_id,
                event_type,
                alert_id,
                severity,
                source_id,
                channels,
                conflict_rule_ids,
                dedupe_hit,
                suppressed,
                simulated,
                metadata,
                created_at
         FROM alert_orchestration_executions
         ${whereSql}
         ORDER BY created_at DESC, id DESC
         LIMIT $${listParams.length}`,
        listParams
      );

      return {
        items: listResult.rows.map(mapAlertOrchestrationExecutionRow),
        total: Math.max(0, Math.trunc(toNumber(countResult.rows[0]?.total, 0))),
      };
    } catch (error) {
      this.disableDb(error, "查询 alert orchestration execution logs 失败");
      return this.listAlertOrchestrationExecutionLogsFromMemory(normalizedTenantId, normalized);
    }
  }

  async createAlertOrchestrationExecutionLog(
    tenantId: string,
    input: AlertOrchestrationExecutionCreateInput
  ): Promise<AlertOrchestrationExecutionLog> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedRuleId = firstNonEmptyString(input.ruleId);
    if (!normalizedRuleId) {
      throw new Error("alert_orchestration_execution_rule_id_required");
    }

    const rule = await this.getAlertOrchestrationRuleById(normalizedTenantId, normalizedRuleId);
    const rawChannels = Array.isArray(input.channels) ? input.channels : rule?.channels ?? [];
    const channels: AlertOrchestrationChannel[] = [];
    const channelSet = new Set<AlertOrchestrationChannel>();
    for (const channel of rawChannels) {
      const normalizedChannel = firstNonEmptyString(channel);
      if (!normalizedChannel) {
        continue;
      }
      const mappedChannel = toAlertOrchestrationChannel(normalizedChannel.toLowerCase());
      if (!mappedChannel) {
        continue;
      }
      if (channelSet.has(mappedChannel)) {
        continue;
      }
      channelSet.add(mappedChannel);
      channels.push(mappedChannel);
    }

    const conflictRuleIds = normalizeDistinctStringArray(input.conflictRuleIds);
    const severityRaw = firstNonEmptyString(input.severity);
    const createdAt = toIsoString(input.createdAt) ?? new Date().toISOString();
    const metadata = toDbRow(input.metadata) ?? {};
    const execution: AlertOrchestrationExecutionLog = {
      id: firstNonEmptyString(input.id) ?? crypto.randomUUID(),
      tenantId: normalizedTenantId,
      ruleId: normalizedRuleId,
      eventType: toAlertOrchestrationEventType(input.eventType),
      alertId: firstNonEmptyString(input.alertId) ?? undefined,
      severity: severityRaw ? toAlertSeverity(severityRaw) : undefined,
      sourceId: firstNonEmptyString(input.sourceId) ?? undefined,
      channels,
      dispatchMode:
        input.dispatchMode === "rule" || input.dispatchMode === "fallback"
          ? input.dispatchMode
          : resolveAlertOrchestrationDispatchMode(metadata),
      conflictRuleIds,
      dedupeHit: input.dedupeHit === true,
      suppressed: input.suppressed === true,
      simulated: input.simulated === true,
      metadata,
      createdAt,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createAlertOrchestrationExecutionLogToMemory(execution);
    }

    try {
      const result = await pool.query(
        `INSERT INTO alert_orchestration_executions (
           id,
           tenant_id,
           rule_id,
           event_type,
           alert_id,
           severity,
           source_id,
           channels,
           conflict_rule_ids,
           dedupe_hit,
           suppressed,
           simulated,
           metadata,
           created_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6,
           $7,
           $8::jsonb,
           $9::jsonb,
           $10,
           $11,
           $12,
           $13::jsonb,
           $14::timestamptz
         )
         ON CONFLICT (tenant_id, id)
         DO UPDATE
           SET rule_id = EXCLUDED.rule_id,
               event_type = EXCLUDED.event_type,
               alert_id = EXCLUDED.alert_id,
               severity = EXCLUDED.severity,
               source_id = EXCLUDED.source_id,
               channels = EXCLUDED.channels,
               conflict_rule_ids = EXCLUDED.conflict_rule_ids,
               dedupe_hit = EXCLUDED.dedupe_hit,
               suppressed = EXCLUDED.suppressed,
               simulated = EXCLUDED.simulated,
               metadata = EXCLUDED.metadata,
               created_at = EXCLUDED.created_at
         RETURNING id,
                   tenant_id,
                   rule_id,
                   event_type,
                   alert_id,
                   severity,
                   source_id,
                   channels,
                   conflict_rule_ids,
                   dedupe_hit,
                   suppressed,
                   simulated,
                   metadata,
                   created_at`,
        [
          execution.id,
          execution.tenantId,
          execution.ruleId,
          execution.eventType,
          execution.alertId ?? null,
          execution.severity ?? null,
          execution.sourceId ?? null,
          safeStringifyJson(execution.channels),
          safeStringifyJson(execution.conflictRuleIds),
          execution.dedupeHit,
          execution.suppressed,
          execution.simulated,
          safeStringifyJson(execution.metadata),
          execution.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.createAlertOrchestrationExecutionLogToMemory(execution);
      }
      return mapAlertOrchestrationExecutionRow(row);
    } catch (error) {
      this.disableDb(error, "写入 alert orchestration execution log 失败");
      return this.createAlertOrchestrationExecutionLogToMemory(execution);
    }
  }

  async listResidencyRegions(): Promise<RegionDescriptor[]> {
    return DEFAULT_REGIONS.map((region) => ({ ...region }));
  }

  async getTenantResidencyPolicy(tenantId: string): Promise<TenantResidencyPolicy | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const pool = await this.getPool();
    if (!pool) {
      return this.getTenantResidencyPolicyFromMemory(normalizedTenantId);
    }

    try {
      const result = await pool.query(
        `SELECT tenant_id,
                mode,
                primary_region,
                replica_regions,
                allow_cross_region_transfer,
                require_transfer_approval,
                updated_at
         FROM tenant_residency_policies
         WHERE tenant_id = $1
         LIMIT 1`,
        [normalizedTenantId]
      );
      const row = result.rows[0];
      return row ? mapTenantResidencyPolicyRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询租户数据主权策略失败");
      return this.getTenantResidencyPolicyFromMemory(normalizedTenantId);
    }
  }

  async upsertTenantResidencyPolicy(
    tenantId: string,
    input: TenantResidencyPolicyUpsertInput
  ): Promise<TenantResidencyPolicy> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const primaryRegion = firstNonEmptyString(input.primaryRegion) ?? "cn-hangzhou";
    const mode = toDataResidencyMode(input.mode);
    const replicaRegionSet = new Set<string>();
    const replicaRegions: string[] = [];
    for (const region of Array.isArray(input.replicaRegions) ? input.replicaRegions : []) {
      const normalizedRegion = firstNonEmptyString(region);
      if (!normalizedRegion || normalizedRegion === primaryRegion || replicaRegionSet.has(normalizedRegion)) {
        continue;
      }
      replicaRegionSet.add(normalizedRegion);
      replicaRegions.push(normalizedRegion);
    }
    const updatedAt = toIsoString(input.updatedAt) ?? new Date().toISOString();
    const normalized: TenantResidencyPolicy = {
      tenantId: normalizedTenantId,
      mode,
      primaryRegion,
      replicaRegions: mode === "single_region" ? [] : replicaRegions,
      allowCrossRegionTransfer: input.allowCrossRegionTransfer === true,
      requireTransferApproval: input.requireTransferApproval === true,
      updatedAt,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.upsertTenantResidencyPolicyToMemory(normalized);
    }

    try {
      const result = await pool.query(
        `INSERT INTO tenant_residency_policies (
           tenant_id,
           mode,
           primary_region,
           replica_regions,
           allow_cross_region_transfer,
           require_transfer_approval,
           updated_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::timestamptz)
         ON CONFLICT (tenant_id)
         DO UPDATE
           SET mode = EXCLUDED.mode,
               primary_region = EXCLUDED.primary_region,
               replica_regions = EXCLUDED.replica_regions,
               allow_cross_region_transfer = EXCLUDED.allow_cross_region_transfer,
               require_transfer_approval = EXCLUDED.require_transfer_approval,
               updated_at = EXCLUDED.updated_at
         RETURNING tenant_id,
                   mode,
                   primary_region,
                   replica_regions,
                   allow_cross_region_transfer,
                   require_transfer_approval,
                   updated_at`,
        [
          normalized.tenantId,
          normalized.mode,
          normalized.primaryRegion,
          safeStringifyJson(normalized.replicaRegions),
          normalized.allowCrossRegionTransfer,
          normalized.requireTransferApproval,
          normalized.updatedAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.upsertTenantResidencyPolicyToMemory(normalized);
      }
      return mapTenantResidencyPolicyRow(row);
    } catch (error) {
      this.disableDb(error, "写入租户数据主权策略失败");
      return this.upsertTenantResidencyPolicyToMemory(normalized);
    }
  }

  async createReplicationJob(
    tenantId: string,
    input: ReplicationJobCreateInput,
    options: {
      createdByUserId?: string;
      approvedByUserId?: string;
    } = {}
  ): Promise<ReplicationJob> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const now = new Date().toISOString();
    const sourceRegion = firstNonEmptyString(input.sourceRegion) ?? "cn-hangzhou";
    const targetRegion = firstNonEmptyString(input.targetRegion) ?? "cn-shanghai";
    const metadata = toDbRow(input.metadata) ?? {};
    const job: ReplicationJob = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      sourceRegion,
      targetRegion,
      status: "pending",
      reason: firstNonEmptyString(input.reason) ?? undefined,
      createdByUserId: firstNonEmptyString(options.createdByUserId) ?? undefined,
      approvedByUserId: firstNonEmptyString(options.approvedByUserId) ?? undefined,
      metadata,
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.saveReplicationJobToMemory(job);
    }

    try {
      const result = await pool.query(
        `INSERT INTO residency_replication_jobs (
           id,
           tenant_id,
           source_region,
           target_region,
           status,
           reason,
           created_by_user_id,
           approved_by_user_id,
           metadata,
           created_at,
           updated_at,
           started_at,
           finished_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6,
           $7,
           $8,
           $9::jsonb,
           $10::timestamptz,
           $10::timestamptz,
           NULL,
           NULL
         )
         RETURNING id,
                   tenant_id,
                   source_region,
                   target_region,
                   status,
                   reason,
                   created_by_user_id,
                   approved_by_user_id,
                   metadata,
                   created_at,
                   updated_at,
                   started_at,
                   finished_at`,
        [
          job.id,
          job.tenantId,
          job.sourceRegion,
          job.targetRegion,
          job.status,
          job.reason ?? null,
          job.createdByUserId ?? null,
          job.approvedByUserId ?? null,
          safeStringifyJson(job.metadata),
          job.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.saveReplicationJobToMemory(job);
      }
      return mapReplicationJobRow(row);
    } catch (error) {
      this.disableDb(error, "创建区域复制任务失败");
      return this.saveReplicationJobToMemory(job);
    }
  }

  async listReplicationJobs(
    tenantId: string,
    input: ReplicationJobListInput = {}
  ): Promise<ReplicationJobListResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeReplicationJobListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listReplicationJobsFromMemory(normalizedTenantId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      if (normalized.status) {
        params.push(normalized.status);
        whereClauses.push(`status = $${params.length}`);
      }
      if (normalized.sourceRegion) {
        params.push(normalized.sourceRegion);
        whereClauses.push(`source_region = $${params.length}`);
      }
      if (normalized.targetRegion) {
        params.push(normalized.targetRegion);
        whereClauses.push(`target_region = $${params.length}`);
      }
      params.push(normalized.limit);

      const whereSql = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                source_region,
                target_region,
                status,
                reason,
                created_by_user_id,
                approved_by_user_id,
                metadata,
                created_at,
                updated_at,
                started_at,
                finished_at
         FROM residency_replication_jobs
         ${whereSql}
         ORDER BY created_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      const items = result.rows.map(mapReplicationJobRow);
      return {
        items,
        total: items.length,
      };
    } catch (error) {
      this.disableDb(error, "查询区域复制任务失败");
      return this.listReplicationJobsFromMemory(normalizedTenantId, normalized);
    }
  }

  async getReplicationJobById(tenantId: string, jobId: string): Promise<ReplicationJob | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedJobId = firstNonEmptyString(jobId);
    if (!normalizedJobId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getReplicationJobByIdFromMemory(normalizedTenantId, normalizedJobId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                source_region,
                target_region,
                status,
                reason,
                created_by_user_id,
                approved_by_user_id,
                metadata,
                created_at,
                updated_at,
                started_at,
                finished_at
         FROM residency_replication_jobs
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [normalizedTenantId, normalizedJobId]
      );
      const row = result.rows[0];
      return row ? mapReplicationJobRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询区域复制任务失败");
      return this.getReplicationJobByIdFromMemory(normalizedTenantId, normalizedJobId);
    }
  }

  async cancelReplicationJob(
    tenantId: string,
    jobId: string,
    input: ReplicationJobCancelInput = {},
    options: {
      userId?: string;
    } = {}
  ): Promise<ReplicationJob | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedJobId = firstNonEmptyString(jobId);
    if (!normalizedJobId) {
      return null;
    }
    const updatedAt = new Date().toISOString();
    const reason = firstNonEmptyString(input.reason) ?? undefined;

    const pool = await this.getPool();
    if (!pool) {
      return this.cancelReplicationJobInMemory(
        normalizedTenantId,
        normalizedJobId,
        reason,
        firstNonEmptyString(options.userId) ?? undefined,
        updatedAt
      );
    }

    try {
      const result = await pool.query(
        `UPDATE residency_replication_jobs
         SET status = 'cancelled',
             reason = COALESCE($3, reason),
             approved_by_user_id = COALESCE($4, approved_by_user_id),
             finished_at = COALESCE(finished_at, $5::timestamptz),
             updated_at = $5::timestamptz
         WHERE tenant_id = $1
           AND id = $2
           AND status IN ('pending', 'running')
         RETURNING id,
                   tenant_id,
                   source_region,
                   target_region,
                   status,
                   reason,
                   created_by_user_id,
                   approved_by_user_id,
                   metadata,
                   created_at,
                   updated_at,
                   started_at,
                   finished_at`,
        [
          normalizedTenantId,
          normalizedJobId,
          reason ?? null,
          firstNonEmptyString(options.userId) ?? null,
          updatedAt,
        ]
      );
      const row = result.rows[0];
      if (row) {
        return mapReplicationJobRow(row);
      }
      return await this.getReplicationJobById(normalizedTenantId, normalizedJobId);
    } catch (error) {
      this.disableDb(error, "取消区域复制任务失败");
      return this.cancelReplicationJobInMemory(
        normalizedTenantId,
        normalizedJobId,
        reason,
        firstNonEmptyString(options.userId) ?? undefined,
        updatedAt
      );
    }
  }

  async approveReplicationJob(
    tenantId: string,
    jobId: string,
    input: ReplicationJobApproveInput = {},
    options: {
      userId?: string;
    } = {}
  ): Promise<ReplicationJob | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedJobId = firstNonEmptyString(jobId);
    if (!normalizedJobId) {
      return null;
    }
    const updatedAt = new Date().toISOString();
    const reason = firstNonEmptyString(input.reason) ?? undefined;
    const approverUserId = firstNonEmptyString(options.userId) ?? undefined;

    const pool = await this.getPool();
    if (!pool) {
      return this.approveReplicationJobInMemory(
        normalizedTenantId,
        normalizedJobId,
        reason,
        approverUserId,
        updatedAt
      );
    }

    try {
      const result = await pool.query(
        `UPDATE residency_replication_jobs
         SET status = 'running',
             reason = COALESCE($3, reason),
             approved_by_user_id = COALESCE($4, approved_by_user_id),
             started_at = COALESCE(started_at, $5::timestamptz),
             updated_at = $5::timestamptz
         WHERE tenant_id = $1
           AND id = $2
           AND status = 'pending'
         RETURNING id,
                   tenant_id,
                   source_region,
                   target_region,
                   status,
                   reason,
                   created_by_user_id,
                   approved_by_user_id,
                   metadata,
                   created_at,
                   updated_at,
                   started_at,
                   finished_at`,
        [normalizedTenantId, normalizedJobId, reason ?? null, approverUserId ?? null, updatedAt]
      );
      const row = result.rows[0];
      if (row) {
        return mapReplicationJobRow(row);
      }
      return await this.getReplicationJobById(normalizedTenantId, normalizedJobId);
    } catch (error) {
      this.disableDb(error, "审批区域复制任务失败");
      return this.approveReplicationJobInMemory(
        normalizedTenantId,
        normalizedJobId,
        reason,
        approverUserId,
        updatedAt
      );
    }
  }

  async listRuleAssets(tenantId: string, input: RuleAssetListInput = {}): Promise<RuleAssetListResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeRuleAssetListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listRuleAssetsFromMemory(normalizedTenantId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      if (normalized.status) {
        params.push(normalized.status);
        whereClauses.push(`status = $${params.length}`);
      }
      if (normalized.keyword) {
        params.push(`%${normalized.keyword}%`);
        whereClauses.push(`(name ILIKE $${params.length} OR COALESCE(description, '') ILIKE $${params.length})`);
      }
      params.push(normalized.limit);
      const whereSql = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                description,
                status,
                latest_version,
                published_version,
                scope_binding,
                created_at,
                updated_at
         FROM rule_assets
         ${whereSql}
         ORDER BY updated_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      const items = result.rows.map(mapRuleAssetRow);
      return {
        items,
        total: items.length,
      };
    } catch (error) {
      this.disableDb(error, "查询规则资产失败");
      return this.listRuleAssetsFromMemory(normalizedTenantId, normalized);
    }
  }

  async getRuleAssetById(tenantId: string, assetId: string): Promise<RuleAsset | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedAssetId = firstNonEmptyString(assetId);
    if (!normalizedAssetId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getRuleAssetByIdFromMemory(normalizedTenantId, normalizedAssetId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                description,
                status,
                latest_version,
                published_version,
                scope_binding,
                created_at,
                updated_at
         FROM rule_assets
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [normalizedTenantId, normalizedAssetId]
      );
      const row = result.rows[0];
      return row ? mapRuleAssetRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询规则资产失败");
      return this.getRuleAssetByIdFromMemory(normalizedTenantId, normalizedAssetId);
    }
  }

  async createRuleAsset(tenantId: string, input: RuleAssetCreateInput): Promise<RuleAsset> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const now = new Date().toISOString();
    const scopeBinding = mapRuleScopeBinding(input.scopeBinding);
    const asset: RuleAsset = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      name: firstNonEmptyString(input.name) ?? "untitled-rule",
      description: firstNonEmptyString(input.description) ?? undefined,
      status: "draft",
      latestVersion: 0,
      publishedVersion: undefined,
      scopeBinding,
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.saveRuleAssetToMemory(asset);
    }

    try {
      const result = await pool.query(
        `INSERT INTO rule_assets (
           id,
           tenant_id,
           name,
           description,
           status,
           latest_version,
           published_version,
           scope_binding,
           created_at,
           updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, NULL, $7::jsonb, $8::timestamptz, $8::timestamptz)
         RETURNING id,
                   tenant_id,
                   name,
                   description,
                   status,
                   latest_version,
                   published_version,
                   scope_binding,
                   created_at,
                   updated_at`,
        [
          asset.id,
          asset.tenantId,
          asset.name,
          asset.description ?? null,
          asset.status,
          asset.latestVersion,
          safeStringifyJson(asset.scopeBinding),
          asset.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.saveRuleAssetToMemory(asset);
      }
      return mapRuleAssetRow(row);
    } catch (error) {
      this.disableDb(error, "创建规则资产失败");
      return this.saveRuleAssetToMemory(asset);
    }
  }

  async listRuleAssetVersions(
    tenantId: string,
    assetId: string,
    limit = 50
  ): Promise<RuleAssetVersion[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedAssetId = firstNonEmptyString(assetId);
    if (!normalizedAssetId) {
      return [];
    }
    const normalizedLimit = Math.min(200, Math.max(1, Math.trunc(limit)));

    const pool = await this.getPool();
    if (!pool) {
      return this.listRuleAssetVersionsFromMemory(normalizedTenantId, normalizedAssetId, normalizedLimit);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                asset_id,
                version,
                content,
                changelog,
                created_by_user_id,
                created_at
         FROM rule_asset_versions
         WHERE tenant_id = $1
           AND asset_id = $2
         ORDER BY version DESC, created_at DESC
         LIMIT $3`,
        [normalizedTenantId, normalizedAssetId, normalizedLimit]
      );
      return result.rows.map(mapRuleAssetVersionRow);
    } catch (error) {
      this.disableDb(error, "查询规则版本失败");
      return this.listRuleAssetVersionsFromMemory(normalizedTenantId, normalizedAssetId, normalizedLimit);
    }
  }

  async createRuleAssetVersion(
    tenantId: string,
    assetId: string,
    input: RuleAssetVersionCreateInput,
    options: {
      createdByUserId?: string;
    } = {}
  ): Promise<RuleAssetVersion | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedAssetId = firstNonEmptyString(assetId);
    if (!normalizedAssetId) {
      return null;
    }

    const content = firstNonEmptyString(input.content);
    if (!content) {
      return null;
    }
    const createdAt = new Date().toISOString();
    const createdByUserId = firstNonEmptyString(options.createdByUserId) ?? undefined;

    const pool = await this.getPool();
    if (!pool) {
      return this.createRuleAssetVersionToMemory(
        normalizedTenantId,
        normalizedAssetId,
        content,
        firstNonEmptyString(input.changelog) ?? undefined,
        createdByUserId,
        createdAt
      );
    }

    try {
      const asset = await this.getRuleAssetById(normalizedTenantId, normalizedAssetId);
      if (!asset) {
        return null;
      }
      const currentResult = await pool.query(
        `SELECT COALESCE(MAX(version), 0) AS max_version
         FROM rule_asset_versions
         WHERE tenant_id = $1
           AND asset_id = $2`,
        [normalizedTenantId, normalizedAssetId]
      );
      const currentVersion = Math.max(0, Math.trunc(toNumber(currentResult.rows[0]?.max_version, 0)));
      const nextVersion = currentVersion + 1;
      const versionId = crypto.randomUUID();

      const versionResult = await pool.query(
        `INSERT INTO rule_asset_versions (
           id,
           tenant_id,
           asset_id,
           version,
           content,
           changelog,
           created_by_user_id,
           created_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8::timestamptz)
         RETURNING id,
                   tenant_id,
                   asset_id,
                   version,
                   content,
                   changelog,
                   created_by_user_id,
                   created_at`,
        [
          versionId,
          normalizedTenantId,
          normalizedAssetId,
          nextVersion,
          content,
          firstNonEmptyString(input.changelog) ?? null,
          createdByUserId ?? null,
          createdAt,
        ]
      );
      await pool.query(
        `UPDATE rule_assets
         SET latest_version = GREATEST(latest_version, $3),
             status = CASE
               WHEN status = 'deprecated' THEN 'draft'
               ELSE status
             END,
             updated_at = $4::timestamptz
         WHERE tenant_id = $1
           AND id = $2`,
        [normalizedTenantId, normalizedAssetId, nextVersion, createdAt]
      );
      const row = versionResult.rows[0];
      return row ? mapRuleAssetVersionRow(row) : null;
    } catch (error) {
      this.disableDb(error, "创建规则版本失败");
      return this.createRuleAssetVersionToMemory(
        normalizedTenantId,
        normalizedAssetId,
        content,
        firstNonEmptyString(input.changelog) ?? undefined,
        createdByUserId,
        createdAt
      );
    }
  }

  async publishRuleAssetVersion(
    tenantId: string,
    assetId: string,
    input: RulePublishInput
  ): Promise<RuleAsset | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedAssetId = firstNonEmptyString(assetId);
    if (!normalizedAssetId) {
      return null;
    }
    const version = Math.max(1, Math.trunc(toNumber(input.version, 1)));
    const updatedAt = new Date().toISOString();

    const pool = await this.getPool();
    if (!pool) {
      return this.publishRuleAssetVersionFromMemory(
        normalizedTenantId,
        normalizedAssetId,
        version,
        updatedAt
      );
    }

    try {
      const result = await pool.query(
        `UPDATE rule_assets AS assets
         SET published_version = $3,
             status = 'published',
             updated_at = $4::timestamptz
         WHERE assets.tenant_id = $1
           AND assets.id = $2
           AND EXISTS (
             SELECT 1
             FROM rule_asset_versions AS versions
             WHERE versions.tenant_id = $1
               AND versions.asset_id = $2
               AND versions.version = $3
           )
         RETURNING id,
                   tenant_id,
                   name,
                   description,
                   status,
                   latest_version,
                   published_version,
                   scope_binding,
                   created_at,
                   updated_at`,
        [normalizedTenantId, normalizedAssetId, version, updatedAt]
      );
      const row = result.rows[0];
      if (row) {
        return mapRuleAssetRow(row);
      }
      return this.getRuleAssetById(normalizedTenantId, normalizedAssetId);
    } catch (error) {
      this.disableDb(error, "发布规则版本失败");
      return this.publishRuleAssetVersionFromMemory(
        normalizedTenantId,
        normalizedAssetId,
        version,
        updatedAt
      );
    }
  }

  async rollbackRuleAssetVersion(
    tenantId: string,
    assetId: string,
    input: RuleRollbackInput
  ): Promise<RuleAsset | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedAssetId = firstNonEmptyString(assetId);
    if (!normalizedAssetId) {
      return null;
    }
    const version = Math.max(1, Math.trunc(toNumber(input.version, 1)));
    const updatedAt = new Date().toISOString();

    const pool = await this.getPool();
    if (!pool) {
      return this.publishRuleAssetVersionFromMemory(
        normalizedTenantId,
        normalizedAssetId,
        version,
        updatedAt
      );
    }

    try {
      const result = await pool.query(
        `UPDATE rule_assets AS assets
         SET published_version = $3,
             status = 'published',
             updated_at = $4::timestamptz
         WHERE assets.tenant_id = $1
           AND assets.id = $2
           AND EXISTS (
             SELECT 1
             FROM rule_asset_versions AS versions
             WHERE versions.tenant_id = $1
               AND versions.asset_id = $2
               AND versions.version = $3
           )
         RETURNING id,
                   tenant_id,
                   name,
                   description,
                   status,
                   latest_version,
                   published_version,
                   scope_binding,
                   created_at,
                   updated_at`,
        [normalizedTenantId, normalizedAssetId, version, updatedAt]
      );
      const row = result.rows[0];
      if (row) {
        return mapRuleAssetRow(row);
      }
      return this.getRuleAssetById(normalizedTenantId, normalizedAssetId);
    } catch (error) {
      this.disableDb(error, "回滚规则版本失败");
      return this.publishRuleAssetVersionFromMemory(
        normalizedTenantId,
        normalizedAssetId,
        version,
        updatedAt
      );
    }
  }

  async listRuleApprovals(
    tenantId: string,
    assetId: string,
    input: RuleApprovalListInput = {}
  ): Promise<RuleApprovalListResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedAssetId = firstNonEmptyString(assetId);
    if (!normalizedAssetId) {
      return { items: [], total: 0 };
    }
    const normalized = normalizeRuleApprovalListInput(input);

    const pool = await this.getPool();
    if (!pool) {
      return this.listRuleApprovalsFromMemory(normalizedTenantId, normalizedAssetId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId, normalizedAssetId];
      const whereClauses: string[] = ["tenant_id = $1", "asset_id = $2"];
      if (normalized.version !== undefined) {
        params.push(normalized.version);
        whereClauses.push(`version = $${params.length}`);
      }
      if (normalized.decision) {
        params.push(normalized.decision);
        whereClauses.push(`decision = $${params.length}`);
      }
      params.push(normalized.limit);
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                asset_id,
                version,
                approver_user_id,
                approver_email,
                decision,
                reason,
                created_at
         FROM rule_approvals
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY created_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      const items = result.rows.map(mapRuleApprovalRow);
      return {
        items,
        total: items.length,
      };
    } catch (error) {
      this.disableDb(error, "查询规则审批记录失败");
      return this.listRuleApprovalsFromMemory(normalizedTenantId, normalizedAssetId, normalized);
    }
  }

  async createRuleApproval(
    tenantId: string,
    assetId: string,
    input: RuleApprovalCreateInput,
    options: {
      approverUserId: string;
      approverEmail?: string;
    }
  ): Promise<RuleApproval | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedAssetId = firstNonEmptyString(assetId);
    const approverUserId = firstNonEmptyString(options.approverUserId);
    if (!normalizedAssetId || !approverUserId) {
      return null;
    }
    const version = Math.max(1, Math.trunc(toNumber(input.version, 1)));
    const createdAt = new Date().toISOString();
    const approval: RuleApproval = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      assetId: normalizedAssetId,
      version,
      approverUserId,
      approverEmail: firstNonEmptyString(options.approverEmail) ?? undefined,
      decision: toRuleApprovalDecision(input.decision),
      reason: firstNonEmptyString(input.reason) ?? undefined,
      createdAt,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createRuleApprovalToMemory(approval);
    }

    try {
      const versionExists = await pool.query(
        `SELECT 1
         FROM rule_asset_versions
         WHERE tenant_id = $1
           AND asset_id = $2
           AND version = $3
         LIMIT 1`,
        [normalizedTenantId, normalizedAssetId, version]
      );
      if (versionExists.rows.length === 0) {
        return null;
      }

      const result = await pool.query(
        `INSERT INTO rule_approvals (
           id,
           tenant_id,
           asset_id,
           version,
           approver_user_id,
           approver_email,
           decision,
           reason,
           created_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::timestamptz)
         ON CONFLICT (tenant_id, asset_id, version, approver_user_id)
         DO UPDATE
           SET approver_email = EXCLUDED.approver_email,
               decision = EXCLUDED.decision,
               reason = EXCLUDED.reason,
               created_at = EXCLUDED.created_at
         RETURNING id,
                   tenant_id,
                   asset_id,
                   version,
                   approver_user_id,
                   approver_email,
                   decision,
                   reason,
                   created_at`,
        [
          approval.id,
          approval.tenantId,
          approval.assetId,
          approval.version,
          approval.approverUserId,
          approval.approverEmail ?? null,
          approval.decision,
          approval.reason ?? null,
          approval.createdAt,
        ]
      );
      const row = result.rows[0];
      return row ? mapRuleApprovalRow(row) : null;
    } catch (error) {
      this.disableDb(error, "创建规则审批记录失败");
      return this.createRuleApprovalToMemory(approval);
    }
  }

  async listMcpToolPolicies(
    tenantId: string,
    input: McpToolPolicyListInput = {}
  ): Promise<McpToolPolicyListResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeMcpToolPolicyListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listMcpToolPoliciesFromMemory(normalizedTenantId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      if (normalized.riskLevel) {
        params.push(normalized.riskLevel);
        whereClauses.push(`risk_level = $${params.length}`);
      }
      if (normalized.decision) {
        params.push(normalized.decision);
        whereClauses.push(`decision = $${params.length}`);
      }
      if (normalized.keyword) {
        params.push(`%${normalized.keyword}%`);
        whereClauses.push(`(tool_id ILIKE $${params.length} OR COALESCE(reason, '') ILIKE $${params.length})`);
      }
      params.push(normalized.limit);

      const result = await pool.query(
        `SELECT tenant_id,
                tool_id,
                risk_level,
                decision,
                reason,
                updated_at
         FROM mcp_tool_policies
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY updated_at DESC, tool_id ASC
         LIMIT $${params.length}`,
        params
      );
      const items = result.rows.map(mapMcpToolPolicyRow);
      return {
        items,
        total: items.length,
      };
    } catch (error) {
      this.disableDb(error, "查询 MCP 工具策略失败");
      return this.listMcpToolPoliciesFromMemory(normalizedTenantId, normalized);
    }
  }

  async getMcpToolPolicyByToolId(
    tenantId: string,
    toolId: string
  ): Promise<McpToolPolicy | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedToolId = firstNonEmptyString(toolId);
    if (!normalizedToolId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getMcpToolPolicyByToolIdFromMemory(normalizedTenantId, normalizedToolId);
    }

    try {
      const result = await pool.query(
        `SELECT tenant_id,
                tool_id,
                risk_level,
                decision,
                reason,
                updated_at
         FROM mcp_tool_policies
         WHERE tenant_id = $1
           AND tool_id = $2
         LIMIT 1`,
        [normalizedTenantId, normalizedToolId]
      );
      const row = result.rows[0];
      return row ? mapMcpToolPolicyRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询 MCP 工具策略失败");
      return this.getMcpToolPolicyByToolIdFromMemory(normalizedTenantId, normalizedToolId);
    }
  }

  async upsertMcpToolPolicy(
    tenantId: string,
    input: McpToolPolicyUpsertInput
  ): Promise<McpToolPolicy> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const updatedAt = new Date().toISOString();
    const policy: McpToolPolicy = {
      tenantId: normalizedTenantId,
      toolId: firstNonEmptyString(input.toolId) ?? "unknown",
      riskLevel: toMcpRiskLevel(input.riskLevel),
      decision: toMcpToolDecision(input.decision),
      reason: firstNonEmptyString(input.reason) ?? undefined,
      updatedAt,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.upsertMcpToolPolicyToMemory(policy);
    }

    try {
      const result = await pool.query(
        `INSERT INTO mcp_tool_policies (
           tenant_id,
           tool_id,
           risk_level,
           decision,
           reason,
           updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6::timestamptz)
         ON CONFLICT (tenant_id, tool_id)
         DO UPDATE
           SET risk_level = EXCLUDED.risk_level,
               decision = EXCLUDED.decision,
               reason = EXCLUDED.reason,
               updated_at = EXCLUDED.updated_at
         RETURNING tenant_id,
                   tool_id,
                   risk_level,
                   decision,
                   reason,
                   updated_at`,
        [
          policy.tenantId,
          policy.toolId,
          policy.riskLevel,
          policy.decision,
          policy.reason ?? null,
          policy.updatedAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.upsertMcpToolPolicyToMemory(policy);
      }
      return mapMcpToolPolicyRow(row);
    } catch (error) {
      this.disableDb(error, "写入 MCP 工具策略失败");
      return this.upsertMcpToolPolicyToMemory(policy);
    }
  }

  async listMcpApprovalRequests(
    tenantId: string,
    input: ListMcpApprovalRequestsInput = {}
  ): Promise<McpApprovalRequestListResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const limit = Math.min(200, Math.max(1, toOptionalNonNegativeInteger(input.limit) ?? 50));
    const status = input.status;

    const pool = await this.getPool();
    if (!pool) {
      return this.listMcpApprovalRequestsFromMemory(normalizedTenantId, status, limit);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      if (status) {
        params.push(status);
        whereClauses.push(`status = $${params.length}`);
      }
      params.push(limit);
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                tool_id,
                status,
                requested_by_user_id,
                requested_by_email,
                reason,
                reviewed_by_user_id,
                reviewed_by_email,
                review_reason,
                created_at,
                updated_at
         FROM mcp_approval_requests
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY updated_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      const items = result.rows.map(mapMcpApprovalRequestRow);
      return {
        items,
        total: items.length,
      };
    } catch (error) {
      this.disableDb(error, "查询 MCP 审批请求失败");
      return this.listMcpApprovalRequestsFromMemory(normalizedTenantId, status, limit);
    }
  }

  async getMcpApprovalRequestById(
    tenantId: string,
    approvalRequestId: string
  ): Promise<McpApprovalRequest | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const requestId = firstNonEmptyString(approvalRequestId);
    if (!requestId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getMcpApprovalRequestByIdFromMemory(normalizedTenantId, requestId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                tool_id,
                status,
                requested_by_user_id,
                requested_by_email,
                reason,
                reviewed_by_user_id,
                reviewed_by_email,
                review_reason,
                created_at,
                updated_at
         FROM mcp_approval_requests
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [normalizedTenantId, requestId]
      );
      const row = result.rows[0];
      return row ? mapMcpApprovalRequestRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询 MCP 审批请求详情失败");
      return this.getMcpApprovalRequestByIdFromMemory(normalizedTenantId, requestId);
    }
  }

  async createMcpApprovalRequest(
    tenantId: string,
    input: McpApprovalCreateInput,
    options: {
      requestedByUserId: string;
      requestedByEmail?: string;
    }
  ): Promise<McpApprovalRequest> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const now = new Date().toISOString();
    const record: McpApprovalRequest = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      toolId: firstNonEmptyString(input.toolId) ?? "unknown",
      status: "pending",
      requestedByUserId: firstNonEmptyString(options.requestedByUserId) ?? "unknown",
      requestedByEmail: firstNonEmptyString(options.requestedByEmail) ?? undefined,
      reason: firstNonEmptyString(input.reason) ?? undefined,
      reviewedByUserId: undefined,
      reviewedByEmail: undefined,
      reviewReason: undefined,
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.saveMcpApprovalRequestToMemory(record);
    }

    try {
      const result = await pool.query(
        `INSERT INTO mcp_approval_requests (
           id,
           tenant_id,
           tool_id,
           status,
           requested_by_user_id,
           requested_by_email,
           reason,
           reviewed_by_user_id,
           reviewed_by_email,
           review_reason,
           created_at,
           updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, NULL, $8::timestamptz, $8::timestamptz)
         RETURNING id,
                   tenant_id,
                   tool_id,
                   status,
                   requested_by_user_id,
                   requested_by_email,
                   reason,
                   reviewed_by_user_id,
                   reviewed_by_email,
                   review_reason,
                   created_at,
                   updated_at`,
        [
          record.id,
          record.tenantId,
          record.toolId,
          record.status,
          record.requestedByUserId,
          record.requestedByEmail ?? null,
          record.reason ?? null,
          record.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.saveMcpApprovalRequestToMemory(record);
      }
      return mapMcpApprovalRequestRow(row);
    } catch (error) {
      this.disableDb(error, "创建 MCP 审批请求失败");
      return this.saveMcpApprovalRequestToMemory(record);
    }
  }

  async reviewMcpApprovalRequest(
    tenantId: string,
    approvalRequestId: string,
    nextStatus: "approved" | "rejected",
    input: McpApprovalReviewInput,
    options: {
      reviewedByUserId: string;
      reviewedByEmail?: string;
    }
  ): Promise<McpApprovalRequest | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const requestId = firstNonEmptyString(approvalRequestId);
    if (!requestId) {
      return null;
    }
    const reviewedByUserId = firstNonEmptyString(options.reviewedByUserId);
    if (!reviewedByUserId) {
      return null;
    }
    const updatedAt = new Date().toISOString();
    const reviewReason = firstNonEmptyString(input.reason) ?? undefined;

    const pool = await this.getPool();
    if (!pool) {
      return this.reviewMcpApprovalRequestInMemory(
        normalizedTenantId,
        requestId,
        nextStatus,
        reviewedByUserId,
        firstNonEmptyString(options.reviewedByEmail) ?? undefined,
        reviewReason,
        updatedAt
      );
    }

    try {
      const result = await pool.query(
        `UPDATE mcp_approval_requests
         SET status = $3,
             reviewed_by_user_id = $4,
             reviewed_by_email = $5,
             review_reason = $6,
             updated_at = $7::timestamptz
         WHERE tenant_id = $1
           AND id = $2
           AND status = 'pending'
         RETURNING id,
                   tenant_id,
                   tool_id,
                   status,
                   requested_by_user_id,
                   requested_by_email,
                   reason,
                   reviewed_by_user_id,
                   reviewed_by_email,
                   review_reason,
                   created_at,
                   updated_at`,
        [
          normalizedTenantId,
          requestId,
          nextStatus,
          reviewedByUserId,
          firstNonEmptyString(options.reviewedByEmail) ?? null,
          reviewReason ?? null,
          updatedAt,
        ]
      );
      const row = result.rows[0];
      if (row) {
        return mapMcpApprovalRequestRow(row);
      }
      const currentResult = await pool.query(
        `SELECT id,
                tenant_id,
                tool_id,
                status,
                requested_by_user_id,
                requested_by_email,
                reason,
                reviewed_by_user_id,
                reviewed_by_email,
                review_reason,
                created_at,
                updated_at
         FROM mcp_approval_requests
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [normalizedTenantId, requestId]
      );
      const currentRow = currentResult.rows[0];
      return currentRow ? mapMcpApprovalRequestRow(currentRow) : null;
    } catch (error) {
      this.disableDb(error, "审批 MCP 请求失败");
      return this.reviewMcpApprovalRequestInMemory(
        normalizedTenantId,
        requestId,
        nextStatus,
        reviewedByUserId,
        firstNonEmptyString(options.reviewedByEmail) ?? undefined,
        reviewReason,
        updatedAt
      );
    }
  }

  async appendMcpInvocationAudit(
    tenantId: string,
    input: AppendMcpInvocationInput
  ): Promise<McpInvocationAudit> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const enforced = input.enforced === true;
    const evaluatedDecision =
      firstNonEmptyString(input.evaluatedDecision) ?? (enforced ? input.decision : undefined);
    const invocation: McpInvocationAudit = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      toolId: firstNonEmptyString(input.toolId) ?? "unknown",
      decision: toMcpToolDecision(input.decision),
      result:
        input.result === "blocked" || input.result === "approved" ? input.result : "allowed",
      approvalRequestId: firstNonEmptyString(input.approvalRequestId) ?? undefined,
      enforced,
      evaluatedDecision: evaluatedDecision ? toMcpToolDecision(evaluatedDecision) : undefined,
      metadata: toDbRow(input.metadata) ?? {},
      createdAt: toIsoString(input.createdAt) ?? new Date().toISOString(),
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.saveMcpInvocationAuditToMemory(invocation);
    }

    try {
      const result = await pool.query(
        `INSERT INTO mcp_invocation_audits (
           id,
           tenant_id,
           tool_id,
           decision,
           result,
           approval_request_id,
           enforced,
           evaluated_decision,
           metadata,
           created_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::timestamptz)
         RETURNING id,
                   tenant_id,
                   tool_id,
                   decision,
                   result,
                   approval_request_id,
                   enforced,
                   evaluated_decision,
                   metadata,
                   created_at`,
        [
          invocation.id,
          invocation.tenantId,
          invocation.toolId,
          invocation.decision,
          invocation.result,
          invocation.approvalRequestId ?? null,
          invocation.enforced,
          invocation.evaluatedDecision ?? null,
          safeStringifyJson(invocation.metadata),
          invocation.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.saveMcpInvocationAuditToMemory(invocation);
      }
      return mapMcpInvocationAuditRow(row);
    } catch (error) {
      this.disableDb(error, "写入 MCP 调用审计失败");
      return this.saveMcpInvocationAuditToMemory(invocation);
    }
  }

  async listMcpInvocationAudits(
    tenantId: string,
    input: McpInvocationListInput = {}
  ): Promise<McpInvocationListResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeMcpInvocationListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listMcpInvocationAuditsFromMemory(normalizedTenantId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      if (normalized.toolId) {
        params.push(normalized.toolId);
        whereClauses.push(`tool_id = $${params.length}`);
      }
      if (normalized.decision) {
        params.push(normalized.decision);
        whereClauses.push(`decision = $${params.length}`);
      }
      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`created_at >= $${params.length}::timestamptz`);
      }
      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`created_at <= $${params.length}::timestamptz`);
      }
      params.push(normalized.limit);

      const result = await pool.query(
        `SELECT id,
                tenant_id,
                tool_id,
                decision,
                result,
                approval_request_id,
                enforced,
                evaluated_decision,
                metadata,
                created_at
         FROM mcp_invocation_audits
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY created_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      const items = result.rows.map(mapMcpInvocationAuditRow);
      return {
        items,
        total: items.length,
      };
    } catch (error) {
      this.disableDb(error, "查询 MCP 调用审计失败");
      return this.listMcpInvocationAuditsFromMemory(normalizedTenantId, normalized);
    }
  }

  async appendAuditLog(input: AppendAuditLogInput): Promise<void> {
    const tenantId = firstNonEmptyString(input.tenantId);
    const normalizedTenantId = tenantId ?? DEFAULT_TENANT_ID;
    const metadata = toAuditMetadata(input.metadata);
    metadata.tenant_id = normalizedTenantId;
    metadata.tenantId = normalizedTenantId;

    const audit: AuditItem = {
      id: crypto.randomUUID(),
      eventId: firstNonEmptyString(input.eventId) ?? "",
      action: firstNonEmptyString(input.action) ?? "unknown",
      level: toAuditLevel(input.level),
      detail: typeof input.detail === "string" ? input.detail : "",
      metadata,
      createdAt: toIsoString(input.createdAt) ?? new Date().toISOString(),
    };

    const pool = await this.getPool();
    if (!pool) {
      this.saveAuditToMemory(audit);
      return;
    }

    try {
      await pool.query(
        `INSERT INTO audit_logs (
           id,
           event_id,
           action,
           level,
           detail,
           tenant_id,
           metadata,
           created_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6,
           $7::jsonb,
           $8::timestamptz
         )`,
        [
          audit.id,
          audit.eventId.length > 0 ? audit.eventId : null,
          audit.action,
          audit.level,
          audit.detail,
          normalizedTenantId,
          safeStringifyJson(audit.metadata),
          audit.createdAt,
        ]
      );
      return;
    } catch (error) {
      this.disableDb(error, "写入 audit_logs 失败");
      this.saveAuditToMemory(audit);
    }
  }

  async listAudits(input: AuditListQueryInput, tenantId?: string): Promise<AuditListResult> {
    const normalized = normalizeAuditListInput(input, tenantId);
    const pool = await this.getPool();
    if (!pool) {
      return this.listAuditsFromMemory(normalized);
    }

    try {
      const params: unknown[] = [];
      const whereClauses: string[] = [];
      const cursor = decodeTimePaginationCursor(normalized.cursor);

      if (normalized.tenantId) {
        params.push(normalized.tenantId);
        whereClauses.push(`tenant_id = $${params.length}`);
      }

      if (normalized.eventId) {
        params.push(normalized.eventId);
        whereClauses.push(`event_id = $${params.length}`);
      }

      if (normalized.action) {
        params.push(normalized.action);
        whereClauses.push(`action = $${params.length}`);
      }

      if (normalized.level) {
        params.push(normalized.level);
        whereClauses.push(`level = $${params.length}`);
      }

      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`created_at >= $${params.length}::timestamptz`);
      }

      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`created_at <= $${params.length}::timestamptz`);
      }

      if (normalized.keyword) {
        params.push(`%${normalized.keyword}%`);
        const token = `$${params.length}`;
        whereClauses.push(
          `(COALESCE(event_id, '') ILIKE ${token}
            OR COALESCE(action, '') ILIKE ${token}
            OR COALESCE(level, '') ILIKE ${token}
            OR COALESCE(detail, '') ILIKE ${token}
            OR COALESCE(metadata::text, '') ILIKE ${token})`
        );
      }

      const whereSql = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total
         FROM audit_logs
         ${whereSql}`,
        params
      );

      const listParams = [...params];
      const listWhereClauses = [...whereClauses];
      if (cursor) {
        listParams.push(cursor.timestamp);
        const timestampToken = `$${listParams.length}`;
        listParams.push(cursor.id);
        const idToken = `$${listParams.length}`;
        listWhereClauses.push(
          `(created_at < ${timestampToken}::timestamptz
            OR (created_at = ${timestampToken}::timestamptz AND id::text < ${idToken}))`
        );
      }
      const listWhereSql =
        listWhereClauses.length > 0 ? `WHERE ${listWhereClauses.join(" AND ")}` : "";
      listParams.push(normalized.limit + 1);
      const result = await pool.query(
        `SELECT id,
                event_id,
                action,
                level,
                detail,
                metadata,
                created_at,
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
                  AS created_at_cursor
         FROM audit_logs
         ${listWhereSql}
         ORDER BY created_at DESC, id::text DESC
         LIMIT $${listParams.length}`,
        listParams
      );

      const mappedItems = result.rows.map(mapAuditRow);
      const hasMore = mappedItems.length > normalized.limit;
      const items = hasMore ? mappedItems.slice(0, normalized.limit) : mappedItems;
      const cursorRows = hasMore ? result.rows.slice(0, normalized.limit) : result.rows;
      const lastItem = items[items.length - 1];
      const lastCursorRow = cursorRows[cursorRows.length - 1];
      const cursorTimestamp =
        firstNonEmptyString((lastCursorRow as DbRow | undefined)?.created_at_cursor) ??
        toIsoString((lastCursorRow as DbRow | undefined)?.created_at);
      const nextCursor =
        hasMore &&
        lastItem &&
        typeof cursorTimestamp === "string" &&
        Number.isFinite(Date.parse(cursorTimestamp)) &&
        lastItem.id.trim().length > 0
          ? encodeTimePaginationCursor({
              timestamp: cursorTimestamp,
              id: lastItem.id,
            })
          : null;

      return {
        items,
        total: Math.max(0, Math.trunc(toNumber(countResult.rows[0]?.total, 0))),
        nextCursor,
      };
    } catch (error) {
      this.disableDb(error, "查询 audit_logs 失败");
      return this.listAuditsFromMemory(normalized);
    }
  }

  async createLocalUser(input: CreateLocalUserInput): Promise<LocalUser> {
    const now = new Date().toISOString();
    const email = normalizeEmail(input.email);
    const displayName = firstNonEmptyString(input.displayName, email) ?? email;
    const user: LocalUser = {
      id: crypto.randomUUID(),
      email,
      passwordHash: input.passwordHash,
      displayName,
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createLocalUserInMemory(user);
    }

    try {
      const result = await pool.query(
        `INSERT INTO users (
           id,
           email,
           password_hash,
           display_name,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5::timestamptz,
           $5::timestamptz
         )
         ON CONFLICT (email)
         DO UPDATE
           SET password_hash = EXCLUDED.password_hash,
               display_name = EXCLUDED.display_name,
               updated_at = EXCLUDED.updated_at
         RETURNING id,
                   email,
                   password_hash,
                   display_name,
                   created_at,
                   updated_at`,
        [user.id, user.email, user.passwordHash, user.displayName, user.createdAt]
      );
      const row = result.rows[0];
      if (!row) {
        return this.createLocalUserInMemory(user);
      }
      return mapUserRow(row);
    } catch (error) {
      this.disableDb(error, "写入 users 失败");
      return this.createLocalUserInMemory(user);
    }
  }

  async getLocalUserByEmail(email: string): Promise<LocalUser | null> {
    const normalizedEmail = normalizeEmail(email);
    const pool = await this.getPool();
    if (!pool) {
      return this.getLocalUserByEmailFromMemory(normalizedEmail);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                email,
                password_hash,
                display_name,
                created_at,
                updated_at
         FROM users
         WHERE email = $1
         LIMIT 1`,
        [normalizedEmail]
      );
      const row = result.rows[0];
      return row ? mapUserRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询 users 失败");
      return this.getLocalUserByEmailFromMemory(normalizedEmail);
    }
  }

  async getUserById(id: string): Promise<LocalUser | null> {
    const pool = await this.getPool();
    if (!pool) {
      return this.getUserByIdFromMemory(id);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                email,
                password_hash,
                display_name,
                created_at,
                updated_at
         FROM users
         WHERE id = $1
         LIMIT 1`,
        [id]
      );
      const row = result.rows[0];
      return row ? mapUserRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询 users 失败");
      return this.getUserByIdFromMemory(id);
    }
  }

  async createAuthSession(input: CreateAuthSessionInput): Promise<AuthSession> {
    const now = new Date().toISOString();
    const expiresAt =
      toIsoString(input.expiresAt) ?? new Date(Date.now() + 1000 * 60 * 60 * 24).toISOString();
    const authSession: AuthSession = {
      id: crypto.randomUUID(),
      userId: input.userId,
      tenantId: firstNonEmptyString(input.tenantId) ?? DEFAULT_TENANT_ID,
      sessionToken: input.sessionToken,
      expiresAt,
      revokedAt: null,
      replacedBySessionId: null,
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createAuthSessionInMemory(authSession);
    }

    try {
      const result = await pool.query(
        `INSERT INTO auth_sessions (
           id,
           user_id,
           tenant_id,
           session_token,
           expires_at,
           revoked_at,
           replaced_by_session_id,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5::timestamptz,
           NULL,
           NULL,
           $6::timestamptz,
           $6::timestamptz
         )
         RETURNING id,
                   user_id,
                   tenant_id,
                   session_token,
                   expires_at,
                   revoked_at,
                   replaced_by_session_id,
                   created_at,
                   updated_at`,
        [
          authSession.id,
          authSession.userId,
          authSession.tenantId,
          authSession.sessionToken,
          authSession.expiresAt,
          authSession.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.createAuthSessionInMemory(authSession);
      }
      return mapAuthSessionRow(row);
    } catch (error) {
      if (isPgForeignKeyViolation(error)) {
        throw error;
      }
      this.disableDb(error, "写入 auth_sessions 失败");
      return this.createAuthSessionInMemory(authSession);
    }
  }

  async getAuthSessionById(id: string): Promise<AuthSession | null> {
    const pool = await this.getPool();
    if (!pool) {
      return this.getAuthSessionByIdFromMemory(id);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                user_id,
                tenant_id,
                session_token,
                expires_at,
                revoked_at,
                replaced_by_session_id,
                created_at,
                updated_at
         FROM auth_sessions
         WHERE id = $1
         LIMIT 1`,
        [id]
      );
      const row = result.rows[0];
      return row ? mapAuthSessionRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询 auth_sessions 失败");
      return this.getAuthSessionByIdFromMemory(id);
    }
  }

  async rotateAuthSession(
    sessionId: string,
    input: RotateAuthSessionInput
  ): Promise<AuthSession | null> {
    const now = new Date().toISOString();
    const expiresAt =
      toIsoString(input.expiresAt) ?? new Date(Date.now() + 1000 * 60 * 60 * 24).toISOString();
    const nextSessionId = crypto.randomUUID();

    const pool = await this.getPool();
    if (!pool) {
      return this.rotateAuthSessionFromMemory(sessionId, nextSessionId, input.sessionToken, expiresAt, now);
    }

    try {
      const result = await pool.query(
        `WITH current_session AS (
           SELECT user_id, tenant_id
           FROM auth_sessions
           WHERE id = $1
             AND revoked_at IS NULL
           LIMIT 1
         ),
         inserted AS (
           INSERT INTO auth_sessions (
             id,
             user_id,
             tenant_id,
             session_token,
             expires_at,
             revoked_at,
             replaced_by_session_id,
             created_at,
             updated_at
           )
           SELECT
             $2,
             current_session.user_id,
             current_session.tenant_id,
             $3,
             $4::timestamptz,
             NULL,
             NULL,
             $5::timestamptz,
             $5::timestamptz
           FROM current_session
           RETURNING id,
                     user_id,
                     tenant_id,
                     session_token,
                     expires_at,
                     revoked_at,
                     replaced_by_session_id,
                     created_at,
                     updated_at
         )
         UPDATE auth_sessions AS current
         SET revoked_at = $5::timestamptz,
             replaced_by_session_id = inserted.id,
             updated_at = $5::timestamptz
         FROM inserted
         WHERE current.id = $1
         RETURNING inserted.id,
                   inserted.user_id,
                   inserted.tenant_id,
                   inserted.session_token,
                   inserted.expires_at,
                   inserted.revoked_at,
                   inserted.replaced_by_session_id,
                   inserted.created_at,
                   inserted.updated_at`,
        [sessionId, nextSessionId, input.sessionToken, expiresAt, now]
      );
      const row = result.rows[0];
      return row ? mapAuthSessionRow(row) : null;
    } catch (error) {
      this.disableDb(error, "轮转 auth_sessions 失败");
      return this.rotateAuthSessionFromMemory(
        sessionId,
        nextSessionId,
        input.sessionToken,
        expiresAt,
        now
      );
    }
  }

  async revokeAuthSession(id: string): Promise<boolean> {
    const now = new Date().toISOString();
    const pool = await this.getPool();
    if (!pool) {
      return this.revokeAuthSessionFromMemory(id, now);
    }

    try {
      const result = await pool.query(
        `UPDATE auth_sessions
         SET revoked_at = $2::timestamptz,
             updated_at = $2::timestamptz
         WHERE id = $1
           AND revoked_at IS NULL
         RETURNING id`,
        [id, now]
      );
      return Boolean(result.rows[0]);
    } catch (error) {
      this.disableDb(error, "撤销 auth_sessions 失败");
      return this.revokeAuthSessionFromMemory(id, now);
    }
  }

  async listTenants(): Promise<Tenant[]> {
    const pool = await this.getPool();
    if (!pool) {
      return this.listTenantsFromMemory();
    }

    try {
      const result = await pool.query(
        `SELECT id,
                name,
                created_at,
                updated_at
         FROM tenants
         ORDER BY created_at ASC, id ASC`
      );
      return result.rows.map(mapTenantRow);
    } catch (error) {
      this.disableDb(error, "查询 tenants 失败");
      return this.listTenantsFromMemory();
    }
  }

  async createTenant(input: CreateTenantInput): Promise<Tenant> {
    const now = new Date().toISOString();
    const name = firstNonEmptyString(input.name) ?? "Tenant";
    const tenant: Tenant = {
      id: normalizeTenantId(input.id, name),
      name,
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createTenantInMemory(tenant);
    }

    try {
      const result = await pool.query(
        `INSERT INTO tenants (
           id,
           name,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3::timestamptz,
           $3::timestamptz
         )
         ON CONFLICT (id)
         DO NOTHING
         RETURNING id,
                   name,
                   created_at,
                   updated_at`,
        [tenant.id, tenant.name, tenant.createdAt]
      );
      const row = result.rows[0];
      if (!row) {
        throw new Error(`tenant_already_exists:${tenant.id}`);
      }
      return mapTenantRow(row);
    } catch (error) {
      if (error instanceof Error && error.message.startsWith("tenant_already_exists:")) {
        throw error;
      }
      this.disableDb(error, "写入 tenants 失败");
      return this.createTenantInMemory(tenant);
    }
  }

  async listOrganizations(tenantId: string): Promise<Organization[]> {
    const normalizedTenantId = firstNonEmptyString(tenantId) ?? DEFAULT_TENANT_ID;
    const pool = await this.getPool();
    if (!pool) {
      return this.listOrganizationsFromMemory(normalizedTenantId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                created_at,
                updated_at
         FROM organizations
         WHERE tenant_id = $1
         ORDER BY created_at ASC, id ASC`,
        [normalizedTenantId]
      );
      return result.rows.map(mapOrganizationRow);
    } catch (error) {
      this.disableDb(error, "查询 organizations 失败");
      return this.listOrganizationsFromMemory(normalizedTenantId);
    }
  }

  async getOrganizationById(
    tenantId: string,
    organizationId: string
  ): Promise<Organization | null> {
    const normalizedTenantId = firstNonEmptyString(tenantId) ?? DEFAULT_TENANT_ID;
    const normalizedOrganizationId = firstNonEmptyString(organizationId);
    if (!normalizedOrganizationId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getOrganizationByIdFromMemory(
        normalizedTenantId,
        normalizedOrganizationId
      );
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                created_at,
                updated_at
         FROM organizations
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [normalizedTenantId, normalizedOrganizationId]
      );
      const row = result.rows[0];
      return row ? mapOrganizationRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询 organizations 失败");
      return this.getOrganizationByIdFromMemory(
        normalizedTenantId,
        normalizedOrganizationId
      );
    }
  }

  async createOrganization(
    tenantId: string,
    input: CreateOrganizationInput
  ): Promise<Organization> {
    const now = new Date().toISOString();
    const normalizedTenantId = firstNonEmptyString(tenantId) ?? DEFAULT_TENANT_ID;
    const organization: Organization = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      name: firstNonEmptyString(input.name) ?? "Organization",
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createOrganizationInMemory(organization);
    }

    try {
      const result = await pool.query(
        `INSERT INTO organizations (
           id,
           tenant_id,
           name,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4::timestamptz,
           $4::timestamptz
         )
         RETURNING id,
                   tenant_id,
                   name,
                   created_at,
                   updated_at`,
        [
          organization.id,
          organization.tenantId,
          organization.name,
          organization.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.createOrganizationInMemory(organization);
      }
      return mapOrganizationRow(row);
    } catch (error) {
      if (isPgForeignKeyViolation(error)) {
        throw error;
      }
      this.disableDb(error, "写入 organizations 失败");
      return this.createOrganizationInMemory(organization);
    }
  }

  async listTenantMembers(tenantId: string): Promise<TenantMember[]> {
    const normalizedTenantId = firstNonEmptyString(tenantId) ?? DEFAULT_TENANT_ID;
    const pool = await this.getPool();
    if (!pool) {
      return this.listTenantMembersFromMemory(normalizedTenantId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                user_id,
                role,
                organization_id,
                org_role,
                created_at,
                updated_at
         FROM tenant_memberships
         WHERE tenant_id = $1
         ORDER BY created_at ASC, id ASC`,
        [normalizedTenantId]
      );
      return result.rows.map(mapTenantMemberRow);
    } catch (error) {
      this.disableDb(error, "查询 tenant_memberships 失败");
      return this.listTenantMembersFromMemory(normalizedTenantId);
    }
  }

  async addTenantMember(input: AddTenantMemberInput): Promise<TenantMember> {
    const now = new Date().toISOString();
    const tenantId = firstNonEmptyString(input.tenantId) ?? DEFAULT_TENANT_ID;
    const tenantRole = toTenantRole(input.tenantRole);
    const organizationId = firstNonEmptyString(input.organizationId);
    const orgRole = organizationId ? toOrgRole(input.orgRole) : undefined;
    const membership: TenantMember = {
      id: crypto.randomUUID(),
      tenantId,
      userId: input.userId,
      tenantRole,
      organizationId: organizationId ?? undefined,
      orgRole,
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.addTenantMemberToMemory(membership);
    }

    try {
      const result = await pool.query(
        `INSERT INTO tenant_memberships (
           id,
           tenant_id,
           user_id,
           role,
           organization_id,
           org_role,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6,
           $7::timestamptz,
           $7::timestamptz
         )
         ON CONFLICT (tenant_id, user_id)
         DO UPDATE
           SET role = EXCLUDED.role,
               organization_id = EXCLUDED.organization_id,
               org_role = EXCLUDED.org_role,
               updated_at = EXCLUDED.updated_at
         RETURNING id,
                   tenant_id,
                   user_id,
                   role,
                   organization_id,
                   org_role,
                   created_at,
                   updated_at`,
        [
          membership.id,
          membership.tenantId,
          membership.userId,
          membership.tenantRole,
          membership.organizationId ?? null,
          membership.orgRole ?? "member",
          membership.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.addTenantMemberToMemory(membership);
      }
      return mapTenantMemberRow(row);
    } catch (error) {
      if (isPgForeignKeyViolation(error)) {
        throw error;
      }
      this.disableDb(error, "写入 tenant_memberships 失败");
      return this.addTenantMemberToMemory(membership);
    }
  }

  async getTenantMemberByUser(tenantId: string, userId: string): Promise<TenantMember | null> {
    const normalizedTenantId = firstNonEmptyString(tenantId) ?? DEFAULT_TENANT_ID;
    const pool = await this.getPool();
    if (!pool) {
      return this.getTenantMemberByUserFromMemory(normalizedTenantId, userId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                user_id,
                role,
                organization_id,
                org_role,
                created_at,
                updated_at
         FROM tenant_memberships
         WHERE tenant_id = $1
           AND user_id = $2
         LIMIT 1`,
        [normalizedTenantId, userId]
      );
      const row = result.rows[0];
      return row ? mapTenantMemberRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询 tenant_memberships 失败");
      return this.getTenantMemberByUserFromMemory(normalizedTenantId, userId);
    }
  }

  async listDeviceBindings(tenantId: string): Promise<DeviceBinding[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const pool = await this.getPool();
    if (!pool) {
      return this.listDeviceBindingsFromMemory(normalizedTenantId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                device_id,
                display_name,
                metadata,
                created_at,
                updated_at
         FROM identity_device_bindings
         WHERE tenant_id = $1
         ORDER BY created_at ASC, id ASC`,
        [normalizedTenantId]
      );
      return result.rows.map(mapDeviceBindingRow);
    } catch (error) {
      this.disableDb(error, "查询 identity_device_bindings 失败");
      return this.listDeviceBindingsFromMemory(normalizedTenantId);
    }
  }

  async createDeviceBinding(
    tenantId: string,
    input: CreateDeviceBindingInput
  ): Promise<DeviceBinding> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedDeviceId = firstNonEmptyString(input.deviceId);
    if (!normalizedDeviceId) {
      throw new Error("device_binding_device_id_required");
    }

    const now = new Date().toISOString();
    const binding: DeviceBinding = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      deviceId: normalizedDeviceId,
      displayName: firstNonEmptyString(input.displayName) ?? undefined,
      metadata: toDbRow(input.metadata) ?? {},
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createDeviceBindingInMemory(binding);
    }

    try {
      const result = await pool.query(
        `INSERT INTO identity_device_bindings (
           id,
           tenant_id,
           device_id,
           display_name,
           metadata,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5::jsonb,
           $6::timestamptz,
           $6::timestamptz
         )
         ON CONFLICT (tenant_id, device_id)
         DO NOTHING
         RETURNING id,
                   tenant_id,
                   device_id,
                   display_name,
                   metadata,
                   created_at,
                   updated_at`,
        [
          binding.id,
          binding.tenantId,
          binding.deviceId,
          binding.displayName ?? "",
          JSON.stringify(binding.metadata),
          binding.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        throw new Error(
          `device_binding_already_exists:${binding.tenantId}:${binding.deviceId}`
        );
      }
      return mapDeviceBindingRow(row);
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.startsWith("device_binding_already_exists:")
      ) {
        throw error;
      }
      if (isPgUniqueViolation(error)) {
        throw new Error(
          `device_binding_already_exists:${binding.tenantId}:${binding.deviceId}`
        );
      }
      if (isPgForeignKeyViolation(error)) {
        throw error;
      }
      this.disableDb(error, "写入 identity_device_bindings 失败");
      return this.createDeviceBindingInMemory(binding);
    }
  }

  async deleteDeviceBinding(tenantId: string, deviceId: string): Promise<boolean> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedDeviceId = firstNonEmptyString(deviceId);
    if (!normalizedDeviceId) {
      return false;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.deleteDeviceBindingFromMemory(normalizedTenantId, normalizedDeviceId);
    }

    try {
      const result = await pool.query(
        `DELETE FROM identity_device_bindings
         WHERE tenant_id = $1
           AND device_id = $2
         RETURNING id`,
        [normalizedTenantId, normalizedDeviceId]
      );
      return Boolean(result.rows[0]);
    } catch (error) {
      this.disableDb(error, "删除 identity_device_bindings 失败");
      return this.deleteDeviceBindingFromMemory(normalizedTenantId, normalizedDeviceId);
    }
  }

  async listAgentBindings(tenantId: string): Promise<AgentBinding[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const pool = await this.getPool();
    if (!pool) {
      return this.listAgentBindingsFromMemory(normalizedTenantId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                agent_id,
                device_id,
                display_name,
                metadata,
                created_at,
                updated_at
         FROM identity_agent_bindings
         WHERE tenant_id = $1
         ORDER BY created_at ASC, id ASC`,
        [normalizedTenantId]
      );
      return result.rows.map(mapAgentBindingRow);
    } catch (error) {
      this.disableDb(error, "查询 identity_agent_bindings 失败");
      return this.listAgentBindingsFromMemory(normalizedTenantId);
    }
  }

  async createAgentBinding(
    tenantId: string,
    input: CreateAgentBindingInput
  ): Promise<AgentBinding> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedAgentId = firstNonEmptyString(input.agentId);
    if (!normalizedAgentId) {
      throw new Error("agent_binding_agent_id_required");
    }

    const normalizedDeviceId = firstNonEmptyString(input.deviceId) ?? undefined;
    const now = new Date().toISOString();
    const binding: AgentBinding = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      agentId: normalizedAgentId,
      deviceId: normalizedDeviceId,
      displayName: firstNonEmptyString(input.displayName) ?? undefined,
      metadata: toDbRow(input.metadata) ?? {},
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createAgentBindingInMemory(binding);
    }

    try {
      if (binding.deviceId) {
        const deviceResult = await pool.query(
          `SELECT 1
           FROM identity_device_bindings
           WHERE tenant_id = $1
             AND device_id = $2
           LIMIT 1`,
          [binding.tenantId, binding.deviceId]
        );
        if (!deviceResult.rows[0]) {
          throw new Error(
            `agent_binding_device_not_found:${binding.tenantId}:${binding.deviceId}`
          );
        }
      }

      const result = await pool.query(
        `INSERT INTO identity_agent_bindings (
           id,
           tenant_id,
           agent_id,
           device_id,
           display_name,
           metadata,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6::jsonb,
           $7::timestamptz,
           $7::timestamptz
         )
         ON CONFLICT (tenant_id, agent_id)
         DO NOTHING
         RETURNING id,
                   tenant_id,
                   agent_id,
                   device_id,
                   display_name,
                   metadata,
                   created_at,
                   updated_at`,
        [
          binding.id,
          binding.tenantId,
          binding.agentId,
          binding.deviceId ?? null,
          binding.displayName ?? "",
          JSON.stringify(binding.metadata),
          binding.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        throw new Error(
          `agent_binding_already_exists:${binding.tenantId}:${binding.agentId}`
        );
      }
      return mapAgentBindingRow(row);
    } catch (error) {
      if (
        error instanceof Error &&
        (error.message.startsWith("agent_binding_already_exists:") ||
          error.message.startsWith("agent_binding_device_not_found:"))
      ) {
        throw error;
      }
      if (isPgUniqueViolation(error)) {
        throw new Error(
          `agent_binding_already_exists:${binding.tenantId}:${binding.agentId}`
        );
      }
      if (isPgForeignKeyViolation(error)) {
        throw error;
      }
      this.disableDb(error, "写入 identity_agent_bindings 失败");
      return this.createAgentBindingInMemory(binding);
    }
  }

  async deleteAgentBinding(tenantId: string, agentId: string): Promise<boolean> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedAgentId = firstNonEmptyString(agentId);
    if (!normalizedAgentId) {
      return false;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.deleteAgentBindingFromMemory(normalizedTenantId, normalizedAgentId);
    }

    try {
      const result = await pool.query(
        `DELETE FROM identity_agent_bindings
         WHERE tenant_id = $1
           AND agent_id = $2
         RETURNING id`,
        [normalizedTenantId, normalizedAgentId]
      );
      return Boolean(result.rows[0]);
    } catch (error) {
      this.disableDb(error, "删除 identity_agent_bindings 失败");
      return this.deleteAgentBindingFromMemory(normalizedTenantId, normalizedAgentId);
    }
  }

  async listSourceBindings(tenantId: string): Promise<SourceBinding[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const pool = await this.getPool();
    if (!pool) {
      return this.listSourceBindingsFromMemory(normalizedTenantId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                source_id,
                device_id,
                agent_id,
                binding_type,
                access_mode,
                metadata,
                created_at,
                updated_at
         FROM identity_source_bindings
         WHERE tenant_id = $1
         ORDER BY created_at ASC, id ASC`,
        [normalizedTenantId]
      );
      return result.rows.map(mapSourceBindingRow);
    } catch (error) {
      this.disableDb(error, "查询 identity_source_bindings 失败");
      return this.listSourceBindingsFromMemory(normalizedTenantId);
    }
  }

  async createSourceBinding(
    tenantId: string,
    input: CreateSourceBindingInput
  ): Promise<SourceBinding> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedSourceId = firstNonEmptyString(input.sourceId);
    if (!normalizedSourceId) {
      throw new Error("source_binding_source_id_required");
    }

    const normalizedDeviceId = firstNonEmptyString(input.deviceId) ?? undefined;
    const normalizedAgentId = firstNonEmptyString(input.agentId) ?? undefined;
    const now = new Date().toISOString();
    const binding: SourceBinding = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      sourceId: normalizedSourceId,
      deviceId: normalizedDeviceId,
      agentId: normalizedAgentId,
      bindingType: toSourceBindingMethod(input.bindingType),
      accessMode: toSourceAccessMode(input.accessMode),
      metadata: toDbRow(input.metadata) ?? {},
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createSourceBindingInMemory(binding);
    }

    try {
      const sourceResult = await pool.query(
        `SELECT 1
         FROM sources
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [binding.tenantId, binding.sourceId]
      );
      if (!sourceResult.rows[0]) {
        throw new Error(
          `source_binding_source_not_found:${binding.tenantId}:${binding.sourceId}`
        );
      }

      if (binding.deviceId) {
        const deviceResult = await pool.query(
          `SELECT 1
           FROM identity_device_bindings
           WHERE tenant_id = $1
             AND device_id = $2
           LIMIT 1`,
          [binding.tenantId, binding.deviceId]
        );
        if (!deviceResult.rows[0]) {
          throw new Error(
            `source_binding_device_not_found:${binding.tenantId}:${binding.deviceId}`
          );
        }
      }

      if (binding.agentId) {
        const agentResult = await pool.query(
          `SELECT 1
           FROM identity_agent_bindings
           WHERE tenant_id = $1
             AND agent_id = $2
           LIMIT 1`,
          [binding.tenantId, binding.agentId]
        );
        if (!agentResult.rows[0]) {
          throw new Error(
            `source_binding_agent_not_found:${binding.tenantId}:${binding.agentId}`
          );
        }
      }

      const result = await pool.query(
        `INSERT INTO identity_source_bindings (
           id,
           tenant_id,
           source_id,
           device_id,
           agent_id,
           binding_type,
           access_mode,
           metadata,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6,
           $7,
           $8::jsonb,
           $9::timestamptz,
           $9::timestamptz
         )
         ON CONFLICT (tenant_id, source_id)
         DO NOTHING
         RETURNING id,
                   tenant_id,
                   source_id,
                   device_id,
                   agent_id,
                   binding_type,
                   access_mode,
                   metadata,
                   created_at,
                   updated_at`,
        [
          binding.id,
          binding.tenantId,
          binding.sourceId,
          binding.deviceId ?? null,
          binding.agentId ?? null,
          binding.bindingType,
          binding.accessMode,
          JSON.stringify(binding.metadata),
          binding.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        throw new Error(
          `source_binding_already_exists:${binding.tenantId}:${binding.sourceId}`
        );
      }
      return mapSourceBindingRow(row);
    } catch (error) {
      if (
        error instanceof Error &&
        (error.message.startsWith("source_binding_already_exists:") ||
          error.message.startsWith("source_binding_source_not_found:") ||
          error.message.startsWith("source_binding_device_not_found:") ||
          error.message.startsWith("source_binding_agent_not_found:"))
      ) {
        throw error;
      }
      if (isPgUniqueViolation(error)) {
        throw new Error(
          `source_binding_already_exists:${binding.tenantId}:${binding.sourceId}`
        );
      }
      if (isPgForeignKeyViolation(error)) {
        throw error;
      }
      this.disableDb(error, "写入 identity_source_bindings 失败");
      return this.createSourceBindingInMemory(binding);
    }
  }

  async deleteSourceBinding(tenantId: string, bindingId: string): Promise<boolean> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedBindingId = firstNonEmptyString(bindingId);
    if (!normalizedBindingId) {
      return false;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.deleteSourceBindingFromMemory(normalizedTenantId, normalizedBindingId);
    }

    try {
      const result = await pool.query(
        `DELETE FROM identity_source_bindings
         WHERE tenant_id = $1
           AND id = $2
         RETURNING id`,
        [normalizedTenantId, normalizedBindingId]
      );
      return Boolean(result.rows[0]);
    } catch (error) {
      this.disableDb(error, "删除 identity_source_bindings 失败");
      return this.deleteSourceBindingFromMemory(normalizedTenantId, normalizedBindingId);
    }
  }

  async listApiKeys(tenantId: string): Promise<ApiKey[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const pool = await this.getPool();
    if (!pool) {
      return this.listApiKeysFromMemory(normalizedTenantId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                key_hash,
                scopes,
                last_used_at,
                revoked_at,
                created_at,
                updated_at
         FROM api_keys
         WHERE tenant_id = $1
         ORDER BY created_at DESC, id DESC`,
        [normalizedTenantId]
      );
      return result.rows.map(mapApiKeyRow);
    } catch (error) {
      this.disableDb(error, "查询 api_keys 失败");
      return this.listApiKeysFromMemory(normalizedTenantId);
    }
  }

  async createApiKey(tenantId: string, input: CreateApiKeyInput): Promise<ApiKey> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const keyHash = firstNonEmptyString(input.keyHash);
    if (!keyHash) {
      throw new Error("api_key_hash_required");
    }

    const createdAt = toIsoString(input.createdAt) ?? new Date().toISOString();
    const apiKey: ApiKey = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      name: firstNonEmptyString(input.name) ?? "unnamed-key",
      keyHash,
      scopes: normalizeDistinctStringArray(input.scopes, { lowerCase: true }),
      lastUsedAt: undefined,
      revokedAt: undefined,
      createdAt,
      updatedAt: createdAt,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createApiKeyToMemory(apiKey);
    }

    try {
      const result = await pool.query(
        `INSERT INTO api_keys (
           id,
           tenant_id,
           name,
           key_hash,
           scopes,
           last_used_at,
           revoked_at,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5::jsonb,
           NULL,
           NULL,
           $6::timestamptz,
           $6::timestamptz
         )
         ON CONFLICT (tenant_id, key_hash)
         DO NOTHING
         RETURNING id,
                   tenant_id,
                   name,
                   key_hash,
                   scopes,
                   last_used_at,
                   revoked_at,
                   created_at,
                   updated_at`,
        [
          apiKey.id,
          apiKey.tenantId,
          apiKey.name,
          apiKey.keyHash,
          safeStringifyJson(apiKey.scopes),
          apiKey.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        throw new Error(`api_key_hash_already_exists:${apiKey.tenantId}:${apiKey.keyHash}`);
      }
      return mapApiKeyRow(row);
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.startsWith("api_key_hash_already_exists:")
      ) {
        throw error;
      }
      if (isPgUniqueViolation(error)) {
        throw new Error(`api_key_hash_already_exists:${apiKey.tenantId}:${apiKey.keyHash}`);
      }
      this.disableDb(error, "写入 api_keys 失败");
      return this.createApiKeyToMemory(apiKey);
    }
  }

  async revokeApiKey(
    tenantId: string,
    apiKeyId: string,
    revokedAt?: string
  ): Promise<ApiKey | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedApiKeyId = firstNonEmptyString(apiKeyId);
    if (!normalizedApiKeyId) {
      return null;
    }
    const updatedAt = toIsoString(revokedAt) ?? new Date().toISOString();

    const pool = await this.getPool();
    if (!pool) {
      return this.revokeApiKeyFromMemory(normalizedTenantId, normalizedApiKeyId, updatedAt);
    }

    try {
      const result = await pool.query(
        `UPDATE api_keys
         SET revoked_at = COALESCE(revoked_at, $3::timestamptz),
             updated_at = CASE
               WHEN revoked_at IS NULL THEN $3::timestamptz
               ELSE updated_at
             END
         WHERE tenant_id = $1
           AND id = $2
         RETURNING id,
                   tenant_id,
                   name,
                   key_hash,
                   scopes,
                   last_used_at,
                   revoked_at,
                   created_at,
                   updated_at`,
        [normalizedTenantId, normalizedApiKeyId, updatedAt]
      );
      const row = result.rows[0];
      return row ? mapApiKeyRow(row) : null;
    } catch (error) {
      this.disableDb(error, "更新 api_keys 吊销状态失败");
      return this.revokeApiKeyFromMemory(normalizedTenantId, normalizedApiKeyId, updatedAt);
    }
  }

  async touchApiKeyUsage(
    tenantId: string,
    apiKeyId: string,
    usedAt?: string
  ): Promise<ApiKey | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedApiKeyId = firstNonEmptyString(apiKeyId);
    if (!normalizedApiKeyId) {
      return null;
    }
    const touchedAt = toIsoString(usedAt) ?? new Date().toISOString();

    const pool = await this.getPool();
    if (!pool) {
      return this.touchApiKeyUsageFromMemory(normalizedTenantId, normalizedApiKeyId, touchedAt);
    }

    try {
      const result = await pool.query(
        `UPDATE api_keys
         SET last_used_at = CASE
               WHEN revoked_at IS NULL
                 THEN COALESCE(
                   GREATEST(last_used_at, $3::timestamptz),
                   $3::timestamptz
                 )
               ELSE last_used_at
             END,
             updated_at = CASE
               WHEN revoked_at IS NULL THEN $3::timestamptz
               ELSE updated_at
             END
         WHERE tenant_id = $1
           AND id = $2
         RETURNING id,
                   tenant_id,
                   name,
                   key_hash,
                   scopes,
                   last_used_at,
                   revoked_at,
                   created_at,
                   updated_at`,
        [normalizedTenantId, normalizedApiKeyId, touchedAt]
      );
      const row = result.rows[0];
      return row ? mapApiKeyRow(row) : null;
    } catch (error) {
      this.disableDb(error, "更新 api_keys 使用时间失败");
      return this.touchApiKeyUsageFromMemory(normalizedTenantId, normalizedApiKeyId, touchedAt);
    }
  }

  async findApiKeyByHash(tenantId: string, keyHash: string): Promise<ApiKey | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedKeyHash = firstNonEmptyString(keyHash);
    if (!normalizedKeyHash) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.findApiKeyByHashFromMemory(normalizedTenantId, normalizedKeyHash);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                key_hash,
                scopes,
                last_used_at,
                revoked_at,
                created_at,
                updated_at
         FROM api_keys
         WHERE tenant_id = $1
           AND key_hash = $2
         LIMIT 1`,
        [normalizedTenantId, normalizedKeyHash]
      );
      const row = result.rows[0];
      return row ? mapApiKeyRow(row) : null;
    } catch (error) {
      this.disableDb(error, "按 hash 查询 api_keys 失败");
      return this.findApiKeyByHashFromMemory(normalizedTenantId, normalizedKeyHash);
    }
  }

  async listWebhookEndpoints(
    tenantId: string,
    limit: number = DEFAULT_WEBHOOK_ENDPOINT_LIMIT
  ): Promise<WebhookEndpoint[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedLimit = Math.min(
      500,
      Math.max(1, toOptionalNonNegativeInteger(limit) ?? DEFAULT_WEBHOOK_ENDPOINT_LIMIT)
    );
    const pool = await this.getPool();
    if (!pool) {
      return this.listWebhookEndpointsFromMemory(normalizedTenantId, normalizedLimit);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                url,
                enabled,
                event_types,
                secret_hash,
                secret_ciphertext,
                headers,
                created_at,
                updated_at
         FROM webhook_endpoints
         WHERE tenant_id = $1
         ORDER BY updated_at DESC, id DESC
         LIMIT $2`,
        [normalizedTenantId, normalizedLimit]
      );
      return result.rows.map(mapWebhookEndpointRow);
    } catch (error) {
      this.disableDb(error, "查询 webhook_endpoints 失败");
      return this.listWebhookEndpointsFromMemory(normalizedTenantId, normalizedLimit);
    }
  }

  async getWebhookEndpointById(
    tenantId: string,
    endpointId: string
  ): Promise<WebhookEndpoint | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedEndpointId = firstNonEmptyString(endpointId);
    if (!normalizedEndpointId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getWebhookEndpointByIdFromMemory(normalizedTenantId, normalizedEndpointId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                url,
                enabled,
                event_types,
                secret_hash,
                secret_ciphertext,
                headers,
                created_at,
                updated_at
         FROM webhook_endpoints
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [normalizedTenantId, normalizedEndpointId]
      );
      const row = result.rows[0];
      return row ? mapWebhookEndpointRow(row) : null;
    } catch (error) {
      this.disableDb(error, "按 id 查询 webhook_endpoints 失败");
      return this.getWebhookEndpointByIdFromMemory(normalizedTenantId, normalizedEndpointId);
    }
  }

  async createWebhookEndpoint(
    tenantId: string,
    input: CreateWebhookEndpointInput
  ): Promise<WebhookEndpoint> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const url = firstNonEmptyString(input.url);
    if (!url) {
      throw new Error("webhook_endpoint_url_required");
    }

    const now = new Date().toISOString();
    const endpoint: WebhookEndpoint = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      name: firstNonEmptyString(input.name) ?? "unnamed-webhook",
      url,
      enabled: input.enabled !== false,
      eventTypes: normalizeDistinctStringArray(input.eventTypes, { lowerCase: true }),
      secretHash: firstNonEmptyString(input.secretHash) ?? undefined,
      secretCiphertext: firstNonEmptyString(input.secretCiphertext) ?? undefined,
      headers: normalizeStringRecord(input.headers),
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createWebhookEndpointToMemory(endpoint);
    }

    try {
      const result = await pool.query(
        `INSERT INTO webhook_endpoints (
           id,
           tenant_id,
           name,
           url,
           enabled,
           event_types,
           secret_hash,
           secret_ciphertext,
           headers,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6::jsonb,
           $7,
           $8,
           $9::jsonb,
           $10::timestamptz,
           $10::timestamptz
         )
         ON CONFLICT (tenant_id, name)
         DO NOTHING
         RETURNING id,
                   tenant_id,
                   name,
                   url,
                   enabled,
                   event_types,
                   secret_hash,
                   secret_ciphertext,
                   headers,
                   created_at,
                   updated_at`,
        [
          endpoint.id,
          endpoint.tenantId,
          endpoint.name,
          endpoint.url,
          endpoint.enabled,
          safeStringifyJson(endpoint.eventTypes),
          endpoint.secretHash ?? null,
          endpoint.secretCiphertext ?? null,
          safeStringifyJson(endpoint.headers),
          endpoint.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        throw new Error(
          `webhook_endpoint_name_already_exists:${endpoint.tenantId}:${endpoint.name}`
        );
      }
      return mapWebhookEndpointRow(row);
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.startsWith("webhook_endpoint_name_already_exists:")
      ) {
        throw error;
      }
      if (isPgUniqueViolation(error)) {
        throw new Error(
          `webhook_endpoint_name_already_exists:${endpoint.tenantId}:${endpoint.name}`
        );
      }
      this.disableDb(error, "写入 webhook_endpoints 失败");
      return this.createWebhookEndpointToMemory(endpoint);
    }
  }

  async updateWebhookEndpoint(
    tenantId: string,
    endpointId: string,
    input: UpdateWebhookEndpointInput
  ): Promise<WebhookEndpoint | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedEndpointId = firstNonEmptyString(endpointId);
    if (!normalizedEndpointId) {
      return null;
    }

    const hasSecretHash = Object.prototype.hasOwnProperty.call(input, "secretHash");
    const hasSecretCiphertext = Object.prototype.hasOwnProperty.call(input, "secretCiphertext");
    const normalizedInput: UpdateWebhookEndpointInput = {
      name: firstNonEmptyString(input.name) ?? undefined,
      url: firstNonEmptyString(input.url) ?? undefined,
      enabled: typeof input.enabled === "boolean" ? input.enabled : undefined,
      eventTypes: Array.isArray(input.eventTypes)
        ? normalizeDistinctStringArray(input.eventTypes, { lowerCase: true })
        : undefined,
      secretHash: hasSecretHash ? firstNonEmptyString(input.secretHash ?? undefined) ?? null : undefined,
      secretCiphertext: hasSecretCiphertext
        ? firstNonEmptyString(input.secretCiphertext ?? undefined) ?? null
        : undefined,
      headers: input.headers !== undefined ? normalizeStringRecord(input.headers) : undefined,
    };
    const hasEventTypes = Array.isArray(normalizedInput.eventTypes);
    const hasHeaders = normalizedInput.headers !== undefined;
    const updatedAt = new Date().toISOString();

    const pool = await this.getPool();
    if (!pool) {
      return this.updateWebhookEndpointInMemory(
        normalizedTenantId,
        normalizedEndpointId,
        normalizedInput,
        hasSecretHash,
        hasSecretCiphertext,
        updatedAt
      );
    }

    try {
      const result = await pool.query(
        `UPDATE webhook_endpoints
         SET name = COALESCE($3, name),
             url = COALESCE($4, url),
             enabled = COALESCE($5, enabled),
             event_types = CASE
               WHEN $6::boolean THEN $7::jsonb
               ELSE event_types
             END,
             secret_hash = CASE
               WHEN $8::boolean THEN $9
               ELSE secret_hash
             END,
             secret_ciphertext = CASE
               WHEN $10::boolean THEN $11
               ELSE secret_ciphertext
             END,
             headers = CASE
               WHEN $12::boolean THEN $13::jsonb
               ELSE headers
             END,
             updated_at = $14::timestamptz
         WHERE tenant_id = $1
           AND id = $2
         RETURNING id,
                   tenant_id,
                   name,
                   url,
                   enabled,
                   event_types,
                   secret_hash,
                   secret_ciphertext,
                   headers,
                   created_at,
                   updated_at`,
        [
          normalizedTenantId,
          normalizedEndpointId,
          normalizedInput.name ?? null,
          normalizedInput.url ?? null,
          normalizedInput.enabled ?? null,
          hasEventTypes,
          safeStringifyJson(normalizedInput.eventTypes ?? []),
          hasSecretHash,
          hasSecretHash ? normalizedInput.secretHash ?? null : null,
          hasSecretCiphertext,
          hasSecretCiphertext ? normalizedInput.secretCiphertext ?? null : null,
          hasHeaders,
          safeStringifyJson(normalizedInput.headers ?? {}),
          updatedAt,
        ]
      );
      const row = result.rows[0];
      return row ? mapWebhookEndpointRow(row) : null;
    } catch (error) {
      this.disableDb(error, "更新 webhook_endpoints 失败");
      return this.updateWebhookEndpointInMemory(
        normalizedTenantId,
        normalizedEndpointId,
        normalizedInput,
        hasSecretHash,
        hasSecretCiphertext,
        updatedAt
      );
    }
  }

  async deleteWebhookEndpoint(tenantId: string, endpointId: string): Promise<boolean> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedEndpointId = firstNonEmptyString(endpointId);
    if (!normalizedEndpointId) {
      return false;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.deleteWebhookEndpointFromMemory(normalizedTenantId, normalizedEndpointId);
    }

    try {
      const result = await pool.query(
        `DELETE FROM webhook_endpoints
         WHERE tenant_id = $1
           AND id = $2
         RETURNING id`,
        [normalizedTenantId, normalizedEndpointId]
      );
      return Boolean(result.rows[0]);
    } catch (error) {
      this.disableDb(error, "删除 webhook_endpoints 失败");
      return this.deleteWebhookEndpointFromMemory(normalizedTenantId, normalizedEndpointId);
    }
  }

  async createWebhookReplayTask(
    tenantId: string,
    input: CreateWebhookReplayTaskInput
  ): Promise<WebhookReplayTask> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const webhookId = firstNonEmptyString(input.webhookId);
    if (!webhookId) {
      throw new Error("webhook_replay_task_webhook_id_required");
    }

    const now = new Date().toISOString();
    const replayTask: WebhookReplayTask = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      webhookId,
      status: input.status ? toWebhookReplayTaskStatus(input.status) : "queued",
      dryRun: input.dryRun !== false,
      filters: normalizeWebhookReplayFilters(input.filters),
      result: toDbRow(input.result) ?? {},
      error: firstNonEmptyString(input.error) ?? undefined,
      requestedAt: toIsoString(input.requestedAt) ?? now,
      startedAt: toIsoString(input.startedAt) ?? undefined,
      finishedAt: toIsoString(input.finishedAt) ?? undefined,
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createWebhookReplayTaskToMemory(replayTask);
    }

    try {
      const result = await pool.query(
        `INSERT INTO webhook_replay_tasks (
           id,
           tenant_id,
           webhook_id,
           status,
           dry_run,
           filters,
           result,
           error,
           requested_at,
           started_at,
           finished_at,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6::jsonb,
           $7::jsonb,
           $8,
           $9::timestamptz,
           $10::timestamptz,
           $11::timestamptz,
           $12::timestamptz,
           $12::timestamptz
         )
         RETURNING id,
                   tenant_id,
                   webhook_id,
                   status,
                   dry_run,
                   filters,
                   result,
                   error,
                   requested_at,
                   started_at,
                   finished_at,
                   created_at,
                   updated_at`,
        [
          replayTask.id,
          replayTask.tenantId,
          replayTask.webhookId,
          replayTask.status,
          replayTask.dryRun,
          safeStringifyJson(replayTask.filters),
          safeStringifyJson(replayTask.result),
          replayTask.error ?? null,
          replayTask.requestedAt,
          replayTask.startedAt ?? null,
          replayTask.finishedAt ?? null,
          replayTask.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.createWebhookReplayTaskToMemory(replayTask);
      }
      return mapWebhookReplayTaskRow(row);
    } catch (error) {
      this.disableDb(error, "写入 webhook_replay_tasks 失败");
      return this.createWebhookReplayTaskToMemory(replayTask);
    }
  }

  async listWebhookReplayTasks(
    tenantId: string,
    input: ListWebhookReplayTasksInput = {}
  ): Promise<WebhookReplayTaskListResult> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeWebhookReplayTaskListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listWebhookReplayTasksFromMemory(normalizedTenantId, normalized);
    }

    try {
      const cursor = decodeTimePaginationCursor(normalized.cursor);
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];

      if (normalized.webhookId) {
        params.push(normalized.webhookId);
        whereClauses.push(`webhook_id = $${params.length}`);
      }
      if (normalized.status) {
        params.push(normalized.status);
        whereClauses.push(`status = $${params.length}`);
      }
      if (cursor) {
        params.push(cursor.timestamp);
        params.push(cursor.id);
        whereClauses.push(
          `(requested_at < $${params.length - 1}::timestamptz OR (requested_at = $${params.length - 1}::timestamptz AND id < $${params.length}))`
        );
      }
      params.push(normalized.limit + 1);

      const result = await pool.query(
        `SELECT id,
                tenant_id,
                webhook_id,
                status,
                dry_run,
                filters,
                result,
                error,
                requested_at,
                started_at,
                finished_at,
                created_at,
                updated_at,
                to_char(requested_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
                  AS requested_at_cursor
         FROM webhook_replay_tasks
         WHERE ${whereClauses.join("\n           AND ")}
         ORDER BY requested_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );

      const hasMore = result.rows.length > normalized.limit;
      const cursorRows = hasMore ? result.rows.slice(0, normalized.limit) : result.rows;
      const items = cursorRows.map((row) => mapWebhookReplayTaskRow(row));

      const lastCursorRow = cursorRows[cursorRows.length - 1];
      const cursorTimestamp =
        firstNonEmptyString((lastCursorRow as DbRow | undefined)?.requested_at_cursor) ??
        items[items.length - 1]?.requestedAt;
      const nextCursor =
        hasMore &&
        typeof cursorTimestamp === "string" &&
        Number.isFinite(Date.parse(cursorTimestamp)) &&
        items.length > 0
          ? encodeTimePaginationCursor({
              timestamp: cursorTimestamp,
              id: items[items.length - 1]?.id ?? "",
            })
          : null;

      return {
        items,
        total: items.length,
        nextCursor,
      };
    } catch (error) {
      this.disableDb(error, "查询 webhook_replay_tasks 失败");
      return this.listWebhookReplayTasksFromMemory(normalizedTenantId, normalized);
    }
  }

  async getWebhookReplayTaskById(
    tenantId: string,
    taskId: string
  ): Promise<WebhookReplayTask | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedTaskId = firstNonEmptyString(taskId);
    if (!normalizedTaskId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getWebhookReplayTaskByIdFromMemory(normalizedTenantId, normalizedTaskId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                webhook_id,
                status,
                dry_run,
                filters,
                result,
                error,
                requested_at,
                started_at,
                finished_at,
                created_at,
                updated_at
         FROM webhook_replay_tasks
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [normalizedTenantId, normalizedTaskId]
      );
      const row = result.rows[0];
      return row ? mapWebhookReplayTaskRow(row) : null;
    } catch (error) {
      this.disableDb(error, "按 id 查询 webhook_replay_tasks 失败");
      return this.getWebhookReplayTaskByIdFromMemory(normalizedTenantId, normalizedTaskId);
    }
  }

  async updateWebhookReplayTask(
    tenantId: string,
    taskId: string,
    input: UpdateWebhookReplayTaskInput
  ): Promise<WebhookReplayTask | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedTaskId = firstNonEmptyString(taskId);
    if (!normalizedTaskId) {
      return null;
    }

    const hasStatus = Object.prototype.hasOwnProperty.call(input, "status");
    const hasResult = Object.prototype.hasOwnProperty.call(input, "result");
    const hasError = Object.prototype.hasOwnProperty.call(input, "error");
    const hasStartedAt = Object.prototype.hasOwnProperty.call(input, "startedAt");
    const hasFinishedAt = Object.prototype.hasOwnProperty.call(input, "finishedAt");
    const normalizedStatus =
      hasStatus && firstNonEmptyString(input.status)
        ? toWebhookReplayTaskStatus(input.status)
        : undefined;
    const normalizedResult = hasResult ? toDbRow(input.result) ?? {} : undefined;
    const normalizedError = hasError ? firstNonEmptyString(input.error ?? undefined) ?? null : undefined;
    const normalizedStartedAt = hasStartedAt ? toIsoString(input.startedAt ?? undefined) ?? null : undefined;
    const normalizedFinishedAt = hasFinishedAt
      ? toIsoString(input.finishedAt ?? undefined) ?? null
      : undefined;
    const updatedAt = toIsoString(input.updatedAt) ?? new Date().toISOString();
    const normalizedFromStatuses = (input.fromStatuses ?? [])
      .map((status) => firstNonEmptyString(status))
      .filter((status): status is string => Boolean(status))
      .map((status) => toWebhookReplayTaskStatus(status));
    const hasFromStatuses = normalizedFromStatuses.length > 0;

    if (!hasStatus && !hasResult && !hasError && !hasStartedAt && !hasFinishedAt) {
      return this.getWebhookReplayTaskById(normalizedTenantId, normalizedTaskId);
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.updateWebhookReplayTaskInMemory(
        normalizedTenantId,
        normalizedTaskId,
        {
          status: normalizedStatus,
          result: normalizedResult,
          error: normalizedError,
          startedAt: normalizedStartedAt,
          finishedAt: normalizedFinishedAt,
          updatedAt,
          fromStatuses: hasFromStatuses ? normalizedFromStatuses : undefined,
        },
        {
          hasStatus,
          hasResult,
          hasError,
          hasStartedAt,
          hasFinishedAt,
        }
      );
    }

    try {
      const result = await pool.query(
        `UPDATE webhook_replay_tasks
         SET status = CASE
               WHEN $3::boolean THEN $4
               ELSE status
             END,
             result = CASE
               WHEN $5::boolean THEN $6::jsonb
               ELSE result
             END,
             error = CASE
               WHEN $7::boolean THEN $8
               ELSE error
             END,
             started_at = CASE
               WHEN $9::boolean THEN $10::timestamptz
               ELSE started_at
             END,
             finished_at = CASE
               WHEN $11::boolean THEN $12::timestamptz
               ELSE finished_at
             END,
             updated_at = $13::timestamptz
         WHERE tenant_id = $1
           AND id = $2
           AND (
             NOT $14::boolean
             OR status = ANY($15::text[])
           )
         RETURNING id,
                   tenant_id,
                   webhook_id,
                   status,
                   dry_run,
                   filters,
                   result,
                   error,
                   requested_at,
                   started_at,
                   finished_at,
                   created_at,
                   updated_at`,
        [
          normalizedTenantId,
          normalizedTaskId,
          hasStatus,
          normalizedStatus ?? null,
          hasResult,
          safeStringifyJson(normalizedResult ?? {}),
          hasError,
          hasError ? normalizedError : null,
          hasStartedAt,
          hasStartedAt ? normalizedStartedAt : null,
          hasFinishedAt,
          hasFinishedAt ? normalizedFinishedAt : null,
          updatedAt,
          hasFromStatuses,
          normalizedFromStatuses,
        ]
      );
      const row = result.rows[0];
      return row ? mapWebhookReplayTaskRow(row) : null;
    } catch (error) {
      this.disableDb(error, "更新 webhook_replay_tasks 失败");
      return this.updateWebhookReplayTaskInMemory(
        normalizedTenantId,
        normalizedTaskId,
        {
          status: normalizedStatus,
          result: normalizedResult,
          error: normalizedError,
          startedAt: normalizedStartedAt,
          finishedAt: normalizedFinishedAt,
          updatedAt,
          fromStatuses: hasFromStatuses ? normalizedFromStatuses : undefined,
        },
        {
          hasStatus,
          hasResult,
          hasError,
          hasStartedAt,
          hasFinishedAt,
        }
      );
    }
  }

  async listWebhookReplayEvents(
    tenantId: string,
    input: ListWebhookReplayEventsInput
  ): Promise<WebhookReplayEvent[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeWebhookReplayEventListInput(input);
    if (normalized.eventTypes.length === 0) {
      return [];
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.listWebhookReplayEventsFromMemory(normalizedTenantId, normalized);
    }

    try {
      const items: WebhookReplayEvent[] = [];
      for (const eventType of normalized.eventTypes) {
        const currentItems = await this.queryWebhookReplayEventsByType(
          pool,
          normalizedTenantId,
          eventType,
          normalized.from,
          normalized.to,
          normalized.limit
        );
        items.push(...currentItems);
      }

      return items.sort(compareWebhookReplayEventDesc).slice(0, normalized.limit);
    } catch (error) {
      this.disableDb(error, "查询 webhook replay events 失败");
      return this.listWebhookReplayEventsFromMemory(normalizedTenantId, normalized);
    }
  }

  async createQualityEvent(tenantId: string, input: CreateQualityEventInput): Promise<QualityEvent> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const scorecardKey = firstNonEmptyString(input.scorecardKey);
    if (!scorecardKey) {
      throw new Error("quality_event_scorecard_key_required");
    }
    const createdAt = toIsoString(input.createdAt) ?? new Date().toISOString();
    const score = normalizeQualityScore(input.score);
    const metadata = toDbRow(input.metadata) ?? {};
    const externalSource =
      normalizeQualityExternalSourceMetadata(input.externalSource) ??
      extractQualityExternalSourceFromMetadata(metadata);
    const qualityEvent: QualityEvent = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      scorecardKey,
      metricKey: firstNonEmptyString(input.metricKey) ?? undefined,
      externalSource,
      score,
      passed: typeof input.passed === "boolean" ? input.passed : score >= 0.8,
      metadata: mergeQualityExternalSourceIntoMetadata(metadata, externalSource),
      createdAt,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createQualityEventToMemory(qualityEvent);
    }

    try {
      const result = await pool.query(
        `INSERT INTO quality_events (
           id,
           tenant_id,
           scorecard_key,
           metric_key,
           provider,
           repository,
           workflow,
           run_id,
           score,
           passed,
           metadata,
           created_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6,
           $7,
           $8,
           $9,
           $10,
           $11::jsonb,
           $12::timestamptz
         )
         RETURNING id,
                   tenant_id,
                   scorecard_key,
                   metric_key,
                   provider,
                   repository,
                   workflow,
                   run_id,
                   score,
                   passed,
                   metadata,
                   created_at`,
        [
          qualityEvent.id,
          qualityEvent.tenantId,
          qualityEvent.scorecardKey,
          qualityEvent.metricKey ?? null,
          qualityEvent.externalSource?.provider ?? null,
          qualityEvent.externalSource?.repo ?? null,
          qualityEvent.externalSource?.workflow ?? null,
          qualityEvent.externalSource?.runId ?? null,
          qualityEvent.score,
          qualityEvent.passed,
          safeStringifyJson(qualityEvent.metadata),
          qualityEvent.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.createQualityEventToMemory(qualityEvent);
      }
      return mapQualityEventRow(row);
    } catch (error) {
      this.disableDb(error, "写入 quality_events 失败");
      return this.createQualityEventToMemory(qualityEvent);
    }
  }

  async listQualityDailyMetrics(
    tenantId: string,
    input: ListQualityDailyMetricsInput = {}
  ): Promise<QualityDailyMetric[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeQualityDailyMetricsInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listQualityDailyMetricsFromMemory(normalizedTenantId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`created_at >= $${params.length}::timestamptz`);
      }
      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`created_at <= $${params.length}::timestamptz`);
      }
      if (normalized.scorecardKey) {
        params.push(normalized.scorecardKey);
        whereClauses.push(`scorecard_key = $${params.length}`);
      }
      if (normalized.provider) {
        params.push(normalized.provider);
        whereClauses.push(`LOWER(COALESCE(provider, '')) = $${params.length}`);
      }
      if (normalized.repo) {
        params.push(normalized.repo);
        whereClauses.push(`LOWER(COALESCE(repository, '')) = $${params.length}`);
      }
      if (normalized.workflow) {
        params.push(normalized.workflow);
        whereClauses.push(`workflow = $${params.length}`);
      }
      if (normalized.runId) {
        params.push(normalized.runId);
        whereClauses.push(`run_id = $${params.length}`);
      }
      params.push(normalized.limit);
      const result = await pool.query(
        `SELECT to_char((created_at AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD') AS metric_date,
                COUNT(*)::int AS total,
                COUNT(*) FILTER (WHERE passed)::int AS passed,
                COUNT(*) FILTER (WHERE NOT passed)::int AS failed,
                AVG(score)::double precision AS average_score
         FROM quality_events
         WHERE ${whereClauses.join(" AND ")}
         GROUP BY metric_date
         ORDER BY metric_date DESC
         LIMIT $${params.length}`,
        params
      );
      return result.rows.map((row) => ({
        date: firstNonEmptyString(row.metric_date) ?? "1970-01-01",
        total: Math.max(0, Math.trunc(toNumber(row.total, 0))),
        passed: Math.max(0, Math.trunc(toNumber(row.passed, 0))),
        failed: Math.max(0, Math.trunc(toNumber(row.failed, 0))),
        averageScore: Number(normalizeQualityScore(row.average_score).toFixed(6)),
      }));
    } catch (error) {
      this.disableDb(error, "聚合 quality_events 日指标失败");
      return this.listQualityDailyMetricsFromMemory(normalizedTenantId, normalized);
    }
  }

  async listQualityExternalMetricGroups(
    tenantId: string,
    input: ListQualityExternalMetricGroupsInput
  ): Promise<QualityExternalMetricGroup[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeQualityExternalMetricGroupsInput(input);
    const groupColumn = QUALITY_EXTERNAL_GROUP_BY_TO_COLUMN[normalized.groupBy];
    const pool = await this.getPool();
    if (!pool) {
      return this.listQualityExternalMetricGroupsFromMemory(normalizedTenantId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      if (normalized.from) {
        params.push(normalized.from);
        whereClauses.push(`created_at >= $${params.length}::timestamptz`);
      }
      if (normalized.to) {
        params.push(normalized.to);
        whereClauses.push(`created_at <= $${params.length}::timestamptz`);
      }
      if (normalized.scorecardKey) {
        params.push(normalized.scorecardKey);
        whereClauses.push(`scorecard_key = $${params.length}`);
      }
      if (normalized.provider) {
        params.push(normalized.provider);
        whereClauses.push(`LOWER(COALESCE(provider, '')) = $${params.length}`);
      }
      if (normalized.repo) {
        params.push(normalized.repo);
        whereClauses.push(`LOWER(COALESCE(repository, '')) = $${params.length}`);
      }
      if (normalized.workflow) {
        params.push(normalized.workflow);
        whereClauses.push(`workflow = $${params.length}`);
      }
      if (normalized.runId) {
        params.push(normalized.runId);
        whereClauses.push(`run_id = $${params.length}`);
      }

      params.push(normalized.limit);
      const result = await pool.query(
        `SELECT COALESCE(NULLIF(${groupColumn}, ''), 'unknown') AS group_value,
                COUNT(*)::int AS total,
                COUNT(*) FILTER (WHERE passed)::int AS passed,
                COUNT(*) FILTER (WHERE NOT passed)::int AS failed,
                AVG(score)::double precision AS average_score
         FROM quality_events
         WHERE ${whereClauses.join(" AND ")}
         GROUP BY group_value
         ORDER BY total DESC, group_value ASC
         LIMIT $${params.length}`,
        params
      );
      return result.rows.map((row) => ({
        groupBy: normalized.groupBy,
        value: firstNonEmptyString(row.group_value) ?? "unknown",
        total: Math.max(0, Math.trunc(toNumber(row.total, 0))),
        passed: Math.max(0, Math.trunc(toNumber(row.passed, 0))),
        failed: Math.max(0, Math.trunc(toNumber(row.failed, 0))),
        averageScore: Number(normalizeQualityScore(row.average_score).toFixed(6)),
      }));
    } catch (error) {
      this.disableDb(error, "聚合 quality_events 外部维度失败");
      return this.listQualityExternalMetricGroupsFromMemory(normalizedTenantId, normalized);
    }
  }

  async listQualityScorecards(
    tenantId: string,
    input: ListQualityScorecardsInput = {}
  ): Promise<QualityScorecard[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeQualityScorecardListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listQualityScorecardsFromMemory(normalizedTenantId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      if (normalized.scorecardKey) {
        params.push(normalized.scorecardKey);
        whereClauses.push(`scorecard_key = $${params.length}`);
      }
      params.push(normalized.limit);
      const result = await pool.query(
        `SELECT tenant_id,
                scorecard_key,
                title,
                description,
                score,
                dimensions,
                metadata,
                created_at,
                updated_at
         FROM quality_scorecards
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY updated_at DESC, scorecard_key ASC
         LIMIT $${params.length}`,
        params
      );
      return result.rows.map(mapQualityScorecardRow);
    } catch (error) {
      this.disableDb(error, "查询 quality_scorecards 失败");
      return this.listQualityScorecardsFromMemory(normalizedTenantId, normalized);
    }
  }

  async upsertQualityScorecard(
    tenantId: string,
    input: QualityScorecardUpsertInput
  ): Promise<QualityScorecard> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const scorecardKey = firstNonEmptyString(input.scorecardKey);
    if (!scorecardKey) {
      throw new Error("quality_scorecard_key_required");
    }
    const updatedAt = toIsoString(input.updatedAt) ?? new Date().toISOString();
    const qualityScorecard: QualityScorecard = {
      tenantId: normalizedTenantId,
      scorecardKey,
      title: firstNonEmptyString(input.title) ?? scorecardKey,
      description: firstNonEmptyString(input.description) ?? undefined,
      score: normalizeQualityScore(input.score),
      dimensions: normalizeNumericRecord(input.dimensions),
      metadata: toDbRow(input.metadata) ?? {},
      createdAt: updatedAt,
      updatedAt,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.upsertQualityScorecardToMemory(qualityScorecard);
    }

    try {
      const result = await pool.query(
        `INSERT INTO quality_scorecards (
           tenant_id,
           scorecard_key,
           title,
           description,
           score,
           dimensions,
           metadata,
           created_at,
           updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::timestamptz, $8::timestamptz)
         ON CONFLICT (tenant_id, scorecard_key)
         DO UPDATE
           SET title = EXCLUDED.title,
               description = EXCLUDED.description,
               score = EXCLUDED.score,
               dimensions = EXCLUDED.dimensions,
               metadata = EXCLUDED.metadata,
               updated_at = EXCLUDED.updated_at
         RETURNING tenant_id,
                   scorecard_key,
                   title,
                   description,
                   score,
                   dimensions,
                   metadata,
                   created_at,
                   updated_at`,
        [
          qualityScorecard.tenantId,
          qualityScorecard.scorecardKey,
          qualityScorecard.title,
          qualityScorecard.description ?? null,
          qualityScorecard.score,
          safeStringifyJson(qualityScorecard.dimensions),
          safeStringifyJson(qualityScorecard.metadata),
          qualityScorecard.updatedAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.upsertQualityScorecardToMemory(qualityScorecard);
      }
      return mapQualityScorecardRow(row);
    } catch (error) {
      this.disableDb(error, "写入 quality_scorecards 失败");
      return this.upsertQualityScorecardToMemory(qualityScorecard);
    }
  }

  async createReplayDataset(
    tenantId: string,
    input: CreateReplayDatasetInput
  ): Promise<ReplayDataset> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const name = firstNonEmptyString(input.name);
    const model = firstNonEmptyString(input.model);
    if (!name) {
      throw new Error("replay_dataset_name_required");
    }
    if (!model) {
      throw new Error("replay_dataset_model_required");
    }
    const now = toIsoString(input.createdAt) ?? new Date().toISOString();
    const dataset: ReplayDataset = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      name,
      description: firstNonEmptyString(input.description) ?? undefined,
      model,
      promptVersion: firstNonEmptyString(input.promptVersion) ?? undefined,
      externalDatasetId: firstNonEmptyString(input.externalDatasetId) ?? undefined,
      caseCount: Math.max(0, Math.trunc(toNumber(input.caseCount, 0))),
      metadata: toDbRow(input.metadata) ?? {},
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createReplayDatasetToMemory(dataset);
    }

    try {
      const result = await pool.query(
        `INSERT INTO replay_datasets (
           id,
           tenant_id,
           name,
           description,
           model,
           prompt_version,
           external_dataset_id,
           case_count,
           metadata,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5,
           $6,
           $7,
           $8,
           $9::jsonb,
           $10::timestamptz,
           $10::timestamptz
         )
         ON CONFLICT (tenant_id, name)
         DO NOTHING
         RETURNING id,
                   tenant_id,
                   name,
                   description,
                   model,
                   prompt_version,
                   external_dataset_id,
                   case_count,
                   metadata,
                   created_at,
                   updated_at`,
        [
          dataset.id,
          dataset.tenantId,
          dataset.name,
          dataset.description ?? null,
          dataset.model,
          dataset.promptVersion ?? null,
          dataset.externalDatasetId ?? null,
          dataset.caseCount,
          safeStringifyJson(dataset.metadata),
          dataset.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        throw new Error(`replay_dataset_name_already_exists:${dataset.tenantId}:${dataset.name}`);
      }
      return mapReplayDatasetRow(row);
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.startsWith("replay_dataset_name_already_exists:")
      ) {
        throw error;
      }
      if (isPgUniqueViolation(error)) {
        throw new Error(`replay_dataset_name_already_exists:${dataset.tenantId}:${dataset.name}`);
      }
      this.disableDb(error, "写入 replay_datasets 失败");
      return this.createReplayDatasetToMemory(dataset);
    }
  }

  async listReplayDatasets(
    tenantId: string,
    input: ListReplayDatasetsInput = {}
  ): Promise<ReplayDataset[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeReplayDatasetListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listReplayDatasetsFromMemory(normalizedTenantId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      if (normalized.keyword) {
        params.push(`%${normalized.keyword.toLowerCase()}%`);
        whereClauses.push(
          `(LOWER(name) LIKE $${params.length} OR LOWER(COALESCE(description, '')) LIKE $${params.length})`
        );
      }
      params.push(normalized.limit);
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                description,
                model,
                prompt_version,
                external_dataset_id,
                case_count,
                metadata,
                created_at,
                updated_at
         FROM replay_datasets
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY updated_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      return result.rows.map(mapReplayDatasetRow);
    } catch (error) {
      this.disableDb(error, "查询 replay_datasets 失败");
      return this.listReplayDatasetsFromMemory(normalizedTenantId, normalized);
    }
  }

  async getReplayDatasetById(tenantId: string, datasetId: string): Promise<ReplayDataset | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedDatasetId = firstNonEmptyString(datasetId);
    if (!normalizedDatasetId) {
      return null;
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.getReplayDatasetByIdFromMemory(normalizedTenantId, normalizedDatasetId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                description,
                model,
                prompt_version,
                external_dataset_id,
                case_count,
                metadata,
                created_at,
                updated_at
         FROM replay_datasets
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [normalizedTenantId, normalizedDatasetId]
      );
      const row = result.rows[0];
      return row ? mapReplayDatasetRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询 replay_datasets 单条记录失败");
      return this.getReplayDatasetByIdFromMemory(normalizedTenantId, normalizedDatasetId);
    }
  }

  async replaceReplayDatasetCases(
    tenantId: string,
    datasetId: string,
    cases: ReplayDatasetCaseInput[]
  ): Promise<ReplayDatasetCase[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedDatasetId = firstNonEmptyString(datasetId);
    if (!normalizedDatasetId) {
      throw new Error("replay_dataset_case_dataset_id_required");
    }

    const normalizedCases = cases
      .map((item, index) => {
        const inputText = firstNonEmptyString(item.input);
        if (!inputText) {
          throw new Error(`replay_dataset_case_input_required:${index}`);
        }
        const now = new Date().toISOString();
        return {
          id: crypto.randomUUID(),
          tenantId: normalizedTenantId,
          datasetId: normalizedDatasetId,
          caseId: firstNonEmptyString(item.caseId) ?? `case-${index + 1}`,
          sortOrder: Math.max(0, Math.trunc(toNumber(item.sortOrder, index))),
          input: inputText,
          expectedOutput: firstNonEmptyString(item.expectedOutput) ?? undefined,
          baselineOutput: firstNonEmptyString(item.baselineOutput) ?? undefined,
          candidateInput: firstNonEmptyString(item.candidateInput) ?? undefined,
          metadata: toDbRow(item.metadata) ?? {},
          checksum: undefined,
          createdAt: now,
          updatedAt: now,
        } satisfies ReplayDatasetCase;
      })
      .sort((a, b) => a.sortOrder - b.sortOrder || a.caseId.localeCompare(b.caseId));

    const pool = await this.getPool();
    if (!pool) {
      return this.replaceReplayDatasetCasesInMemory(
        normalizedTenantId,
        normalizedDatasetId,
        normalizedCases
      );
    }

    try {
      return await this.withTransaction(pool, async (client) => {
        const datasetResult = await client.query(
          `SELECT 1
           FROM replay_datasets
           WHERE tenant_id = $1
             AND id = $2
           LIMIT 1`,
          [normalizedTenantId, normalizedDatasetId]
        );
        if (!datasetResult.rows[0]) {
          throw new Error(`replay_dataset_not_found:${normalizedTenantId}:${normalizedDatasetId}`);
        }

        await client.query(
          `DELETE FROM replay_dataset_cases
           WHERE tenant_id = $1
             AND dataset_id = $2`,
          [normalizedTenantId, normalizedDatasetId]
        );

        for (const item of normalizedCases) {
          await client.query(
            `INSERT INTO replay_dataset_cases (
               id,
               tenant_id,
               dataset_id,
               case_id,
               sort_order,
               input_text,
               expected_output,
               baseline_output,
               candidate_input,
               metadata,
               checksum,
               created_at,
               updated_at
             )
             VALUES (
               $1,
               $2,
               $3,
               $4,
               $5,
               $6,
               $7,
               $8,
               $9,
               $10::jsonb,
               $11,
               $12::timestamptz,
               $13::timestamptz
             )`,
            [
              item.id,
              item.tenantId,
              item.datasetId,
              item.caseId,
              item.sortOrder,
              item.input,
              item.expectedOutput ?? null,
              item.baselineOutput ?? null,
              item.candidateInput ?? null,
              safeStringifyJson(item.metadata),
              item.checksum ?? null,
              item.createdAt,
              item.updatedAt,
            ]
          );
        }

        await client.query(
          `UPDATE replay_datasets
           SET case_count = $3,
               updated_at = $4::timestamptz
           WHERE tenant_id = $1
             AND id = $2`,
          [normalizedTenantId, normalizedDatasetId, normalizedCases.length, new Date().toISOString()]
        );

        const refreshed = await client.query(
          `SELECT id,
                  tenant_id,
                  dataset_id,
                  case_id,
                  sort_order,
                  input_text,
                  expected_output,
                  baseline_output,
                  candidate_input,
                  metadata,
                  checksum,
                  created_at,
                  updated_at
           FROM replay_dataset_cases
           WHERE tenant_id = $1
             AND dataset_id = $2
           ORDER BY sort_order ASC, case_id ASC, id ASC`,
          [normalizedTenantId, normalizedDatasetId]
        );
        return refreshed.rows.map(mapReplayDatasetCaseRow);
      });
    } catch (error) {
      if (error instanceof Error && error.message.startsWith("replay_dataset_not_found:")) {
        throw error;
      }
      this.disableDb(error, "写入 replay_dataset_cases 失败");
      return this.replaceReplayDatasetCasesInMemory(
        normalizedTenantId,
        normalizedDatasetId,
        normalizedCases
      );
    }
  }

  async listReplayDatasetCases(
    tenantId: string,
    datasetId: string,
    input: ListReplayDatasetCasesInput = {}
  ): Promise<ReplayDatasetCase[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedDatasetId = firstNonEmptyString(datasetId);
    if (!normalizedDatasetId) {
      return [];
    }
    const normalized = normalizeReplayDatasetCaseListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listReplayDatasetCasesFromMemory(
        normalizedTenantId,
        normalizedDatasetId,
        normalized
      );
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                dataset_id,
                case_id,
                sort_order,
                input_text,
                expected_output,
                baseline_output,
                candidate_input,
                metadata,
                checksum,
                created_at,
                updated_at
         FROM replay_dataset_cases
         WHERE tenant_id = $1
           AND dataset_id = $2
         ORDER BY sort_order ASC, case_id ASC, id ASC
         LIMIT $3`,
        [normalizedTenantId, normalizedDatasetId, normalized.limit]
      );
      return result.rows.map(mapReplayDatasetCaseRow);
    } catch (error) {
      this.disableDb(error, "查询 replay_dataset_cases 失败");
      return this.listReplayDatasetCasesFromMemory(
        normalizedTenantId,
        normalizedDatasetId,
        normalized
      );
    }
  }

  async createReplayRun(tenantId: string, input: CreateReplayRunInput): Promise<ReplayRun> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const datasetId = firstNonEmptyString(input.datasetId);
    if (!datasetId) {
      throw new Error("replay_run_dataset_id_required");
    }
    const now = toIsoString(input.createdAt) ?? new Date().toISOString();
    const replayRun: ReplayRun = {
      id: crypto.randomUUID(),
      tenantId: normalizedTenantId,
      datasetId,
      status: toReplayJobStatus(input.status),
      parameters: toDbRow(input.parameters) ?? {},
      summary: toDbRow(input.summary) ?? {},
      diff: toDbRow(input.diff) ?? {},
      error: firstNonEmptyString(input.error) ?? undefined,
      startedAt: toIsoString(input.startedAt) ?? undefined,
      finishedAt: toIsoString(input.finishedAt) ?? undefined,
      createdAt: now,
      updatedAt: now,
    };

    const pool = await this.getPool();
    if (!pool) {
      return this.createReplayRunToMemory(replayRun);
    }

    try {
      const datasetResult = await pool.query(
        `SELECT 1
         FROM replay_datasets
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [replayRun.tenantId, replayRun.datasetId]
      );
      if (!datasetResult.rows[0]) {
        throw new Error(`replay_dataset_not_found:${replayRun.tenantId}:${replayRun.datasetId}`);
      }

      const result = await pool.query(
        `INSERT INTO replay_runs (
           id,
           tenant_id,
           dataset_id,
           status,
           parameters,
           summary_payload,
           diff_payload,
           error,
           started_at,
           finished_at,
           created_at,
           updated_at
         )
         VALUES (
           $1,
           $2,
           $3,
           $4,
           $5::jsonb,
           $6::jsonb,
           $7::jsonb,
           $8,
           $9::timestamptz,
           $10::timestamptz,
           $11::timestamptz,
           $11::timestamptz
         )
         RETURNING id,
                   tenant_id,
                   dataset_id,
                   status,
                   parameters,
                   summary_payload,
                   diff_payload,
                   error,
                   started_at,
                   finished_at,
                   created_at,
                   updated_at`,
        [
          replayRun.id,
          replayRun.tenantId,
          replayRun.datasetId,
          replayRun.status,
          safeStringifyJson(replayRun.parameters),
          safeStringifyJson(replayRun.summary),
          safeStringifyJson(replayRun.diff),
          replayRun.error ?? null,
          replayRun.startedAt ?? null,
          replayRun.finishedAt ?? null,
          replayRun.createdAt,
        ]
      );
      const row = result.rows[0];
      if (!row) {
        return this.createReplayRunToMemory(replayRun);
      }
      return mapReplayRunRow(row);
    } catch (error) {
      if (error instanceof Error && error.message.startsWith("replay_dataset_not_found:")) {
        throw error;
      }
      this.disableDb(error, "写入 replay_runs 失败");
      return this.createReplayRunToMemory(replayRun);
    }
  }

  async listReplayRuns(
    tenantId: string,
    input: ListReplayRunsInput = {}
  ): Promise<ReplayRun[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalized = normalizeReplayRunListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listReplayRunsFromMemory(normalizedTenantId, normalized);
    }

    try {
      const params: unknown[] = [normalizedTenantId];
      const whereClauses: string[] = ["tenant_id = $1"];
      if (normalized.datasetId) {
        params.push(normalized.datasetId);
        whereClauses.push(`dataset_id = $${params.length}`);
      }
      if (normalized.status) {
        params.push(normalized.status);
        whereClauses.push(`status = $${params.length}`);
      }
      params.push(normalized.limit);
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                dataset_id,
                status,
                parameters,
                summary_payload,
                diff_payload,
                error,
                started_at,
                finished_at,
                created_at,
                updated_at
         FROM replay_runs
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY created_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      return result.rows.map(mapReplayRunRow);
    } catch (error) {
      this.disableDb(error, "查询 replay_runs 失败");
      return this.listReplayRunsFromMemory(normalizedTenantId, normalized);
    }
  }

  async getReplayRunById(tenantId: string, runId: string): Promise<ReplayRun | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedRunId = firstNonEmptyString(runId);
    if (!normalizedRunId) {
      return null;
    }
    const pool = await this.getPool();
    if (!pool) {
      return this.getReplayRunByIdFromMemory(normalizedTenantId, normalizedRunId);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                dataset_id,
                status,
                parameters,
                summary_payload,
                diff_payload,
                error,
                started_at,
                finished_at,
                created_at,
                updated_at
         FROM replay_runs
         WHERE tenant_id = $1
           AND id = $2
         LIMIT 1`,
        [normalizedTenantId, normalizedRunId]
      );
      const row = result.rows[0];
      return row ? mapReplayRunRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询 replay_runs 单条记录失败");
      return this.getReplayRunByIdFromMemory(normalizedTenantId, normalizedRunId);
    }
  }

  async getReplayRunDiff(
    tenantId: string,
    runId: string
  ): Promise<Record<string, unknown> | null> {
    const replayRun = await this.getReplayRunById(tenantId, runId);
    if (!replayRun) {
      return null;
    }
    return { ...replayRun.diff };
  }

  async updateReplayRun(
    tenantId: string,
    runId: string,
    input: UpdateReplayRunInput
  ): Promise<ReplayRun | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedRunId = firstNonEmptyString(runId);
    if (!normalizedRunId) {
      return null;
    }

    const hasStatus = input.status !== undefined;
    const hasSummary = Object.prototype.hasOwnProperty.call(input, "summary");
    const hasDiff = Object.prototype.hasOwnProperty.call(input, "diff");
    const hasError = Object.prototype.hasOwnProperty.call(input, "error");
    const hasStartedAt = Object.prototype.hasOwnProperty.call(input, "startedAt");
    const hasFinishedAt = Object.prototype.hasOwnProperty.call(input, "finishedAt");
    const normalizedStatus = hasStatus ? toReplayJobStatus(input.status) : undefined;
    const normalizedSummary = hasSummary ? toDbRow(input.summary) ?? {} : undefined;
    const normalizedDiff = hasDiff ? toDbRow(input.diff) ?? {} : undefined;
    const normalizedError = hasError ? firstNonEmptyString(input.error) ?? null : null;
    const normalizedStartedAt = hasStartedAt
      ? input.startedAt === null
        ? null
        : toIsoString(input.startedAt)
      : null;
    const normalizedFinishedAt = hasFinishedAt
      ? input.finishedAt === null
        ? null
        : toIsoString(input.finishedAt)
      : null;
    const normalizedUpdatedAt = toIsoString(input.updatedAt) ?? new Date().toISOString();
    const fromStatuses = normalizeDistinctStringArray(input.fromStatuses)
      .map((status) => toReplayJobStatus(status))
      .filter((status, index, collection) => collection.indexOf(status) === index);

    if (!hasStatus && !hasSummary && !hasDiff && !hasError && !hasStartedAt && !hasFinishedAt) {
      return this.getReplayRunById(normalizedTenantId, normalizedRunId);
    }

    const pool = await this.getPool();
    if (!pool) {
      return this.updateReplayRunInMemory(normalizedTenantId, normalizedRunId, {
        status: normalizedStatus,
        fromStatuses,
        summary: normalizedSummary,
        diff: normalizedDiff,
        error: hasError ? normalizedError : undefined,
        startedAt: hasStartedAt ? normalizedStartedAt : undefined,
        finishedAt: hasFinishedAt ? normalizedFinishedAt : undefined,
        updatedAt: normalizedUpdatedAt,
      });
    }

    try {
      const params: unknown[] = [normalizedTenantId, normalizedRunId];
      const setClauses: string[] = [];
      if (hasStatus) {
        params.push(normalizedStatus);
        setClauses.push(`status = $${params.length}`);
      }
      if (hasSummary) {
        params.push(safeStringifyJson(normalizedSummary));
        setClauses.push(`summary_payload = $${params.length}::jsonb`);
      }
      if (hasDiff) {
        params.push(safeStringifyJson(normalizedDiff));
        setClauses.push(`diff_payload = $${params.length}::jsonb`);
      }
      if (hasError) {
        params.push(normalizedError);
        setClauses.push(`error = $${params.length}`);
      }
      if (hasStartedAt) {
        params.push(normalizedStartedAt);
        setClauses.push(`started_at = $${params.length}::timestamptz`);
      }
      if (hasFinishedAt) {
        params.push(normalizedFinishedAt);
        setClauses.push(`finished_at = $${params.length}::timestamptz`);
      }
      params.push(normalizedUpdatedAt);
      setClauses.push(`updated_at = $${params.length}::timestamptz`);

      const whereClauses = [`tenant_id = $1`, `id = $2`];
      if (fromStatuses.length > 0) {
        params.push(fromStatuses);
        whereClauses.push(`status = ANY($${params.length}::text[])`);
      }

      const result = await pool.query(
        `UPDATE replay_runs
         SET ${setClauses.join(", ")}
         WHERE ${whereClauses.join(" AND ")}
         RETURNING id,
                   tenant_id,
                   dataset_id,
                   status,
                   parameters,
                   summary_payload,
                   diff_payload,
                   error,
                   started_at,
                   finished_at,
                   created_at,
                   updated_at`,
        params
      );
      const row = result.rows[0];
      return row ? mapReplayRunRow(row) : null;
    } catch (error) {
      this.disableDb(error, "更新 replay_runs 失败");
      return this.updateReplayRunInMemory(normalizedTenantId, normalizedRunId, {
        status: normalizedStatus,
        fromStatuses,
        summary: normalizedSummary,
        diff: normalizedDiff,
        error: hasError ? normalizedError : undefined,
        startedAt: hasStartedAt ? normalizedStartedAt : undefined,
        finishedAt: hasFinishedAt ? normalizedFinishedAt : undefined,
        updatedAt: normalizedUpdatedAt,
      });
    }
  }

  async upsertReplayArtifacts(
    tenantId: string,
    runId: string,
    items: ReplayArtifactInput[]
  ): Promise<ReplayArtifact[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedRunId = firstNonEmptyString(runId);
    if (!normalizedRunId) {
      throw new Error("replay_artifact_run_id_required");
    }

    const replayRun = await this.getReplayRunById(normalizedTenantId, normalizedRunId);
    if (!replayRun) {
      throw new Error(`replay_run_not_found:${normalizedTenantId}:${normalizedRunId}`);
    }

    const normalizedItems = items.map((item) => {
      const now = toIsoString(item.createdAt) ?? new Date().toISOString();
      return {
        id: crypto.randomUUID(),
        tenantId: normalizedTenantId,
        runId: normalizedRunId,
        datasetId: replayRun.datasetId,
        artifactType: toReplayArtifactType(item.artifactType),
        name: firstNonEmptyString(item.name) ?? `${item.artifactType}.json`,
        description: firstNonEmptyString(item.description) ?? undefined,
        contentType: firstNonEmptyString(item.contentType) ?? "application/octet-stream",
        byteSize: Math.max(0, Math.trunc(toNumber(item.byteSize, 0))),
        checksum: firstNonEmptyString(item.checksum) ?? undefined,
        storageBackend: toReplayArtifactStorageBackend(item.storageBackend),
        storageKey: firstNonEmptyString(item.storageKey) ?? "",
        metadata: toDbRow(item.metadata) ?? {},
        createdAt: now,
        updatedAt: now,
      } satisfies ReplayArtifact;
    });

    const pool = await this.getPool();
    if (!pool) {
      return this.upsertReplayArtifactsInMemory(
        normalizedTenantId,
        normalizedRunId,
        normalizedItems
      );
    }

    try {
      return await this.withTransaction(pool, async (client) => {
        for (const item of normalizedItems) {
          await client.query(
            `INSERT INTO replay_artifacts (
               id,
               tenant_id,
               run_id,
               dataset_id,
               artifact_type,
               name,
               description,
               content_type,
               byte_size,
               checksum,
               storage_backend,
               storage_key,
               metadata,
               created_at,
               updated_at
             )
             VALUES (
               $1,
               $2,
               $3,
               $4,
               $5,
               $6,
               $7,
               $8,
               $9,
               $10,
               $11,
               $12,
               $13::jsonb,
               $14::timestamptz,
               $15::timestamptz
             )
             ON CONFLICT (tenant_id, run_id, artifact_type)
             DO UPDATE SET
               dataset_id = EXCLUDED.dataset_id,
               name = EXCLUDED.name,
               description = EXCLUDED.description,
               content_type = EXCLUDED.content_type,
               byte_size = EXCLUDED.byte_size,
               checksum = EXCLUDED.checksum,
               storage_backend = EXCLUDED.storage_backend,
               storage_key = EXCLUDED.storage_key,
               metadata = EXCLUDED.metadata,
               updated_at = EXCLUDED.updated_at`,
            [
              item.id,
              item.tenantId,
              item.runId,
              item.datasetId,
              item.artifactType,
              item.name,
              item.description ?? null,
              item.contentType,
              item.byteSize,
              item.checksum ?? null,
              item.storageBackend,
              item.storageKey,
              safeStringifyJson(item.metadata),
              item.createdAt,
              item.updatedAt,
            ]
          );
        }

        const refreshed = await client.query(
          `SELECT id,
                  tenant_id,
                  run_id,
                  dataset_id,
                  artifact_type,
                  name,
                  description,
                  content_type,
                  byte_size,
                  checksum,
                  storage_backend,
                  storage_key,
                  metadata,
                  created_at,
                  updated_at
           FROM replay_artifacts
           WHERE tenant_id = $1
             AND run_id = $2
           ORDER BY created_at ASC, artifact_type ASC`,
          [normalizedTenantId, normalizedRunId]
        );
        return refreshed.rows.map(mapReplayArtifactRow);
      });
    } catch (error) {
      if (error instanceof Error && error.message.startsWith("replay_run_not_found:")) {
        throw error;
      }
      this.disableDb(error, "写入 replay_artifacts 失败");
      return this.upsertReplayArtifactsInMemory(
        normalizedTenantId,
        normalizedRunId,
        normalizedItems
      );
    }
  }

  async listReplayArtifacts(
    tenantId: string,
    runId: string,
    input: ListReplayArtifactsInput = {}
  ): Promise<ReplayArtifact[]> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedRunId = firstNonEmptyString(runId);
    if (!normalizedRunId) {
      return [];
    }
    const normalized = normalizeReplayArtifactListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listReplayArtifactsFromMemory(normalizedTenantId, normalizedRunId, normalized);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                run_id,
                dataset_id,
                artifact_type,
                name,
                description,
                content_type,
                byte_size,
                checksum,
                storage_backend,
                storage_key,
                metadata,
                created_at,
                updated_at
         FROM replay_artifacts
         WHERE tenant_id = $1
           AND run_id = $2
         ORDER BY created_at ASC, artifact_type ASC
         LIMIT $3`,
        [normalizedTenantId, normalizedRunId, normalized.limit]
      );
      return result.rows.map(mapReplayArtifactRow);
    } catch (error) {
      this.disableDb(error, "查询 replay_artifacts 失败");
      return this.listReplayArtifactsFromMemory(normalizedTenantId, normalizedRunId, normalized);
    }
  }

  async getReplayArtifactByType(
    tenantId: string,
    runId: string,
    artifactType: ReplayArtifactType
  ): Promise<ReplayArtifact | null> {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedRunId = firstNonEmptyString(runId);
    if (!normalizedRunId) {
      return null;
    }
    const normalizedType = toReplayArtifactType(artifactType);
    const pool = await this.getPool();
    if (!pool) {
      return this.getReplayArtifactByTypeFromMemory(normalizedTenantId, normalizedRunId, normalizedType);
    }

    try {
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                run_id,
                dataset_id,
                artifact_type,
                name,
                description,
                content_type,
                byte_size,
                checksum,
                storage_backend,
                storage_key,
                metadata,
                created_at,
                updated_at
         FROM replay_artifacts
         WHERE tenant_id = $1
           AND run_id = $2
           AND artifact_type = $3
         LIMIT 1`,
        [normalizedTenantId, normalizedRunId, normalizedType]
      );
      const row = result.rows[0];
      return row ? mapReplayArtifactRow(row) : null;
    } catch (error) {
      this.disableDb(error, "查询 replay_artifacts 单条记录失败");
      return this.getReplayArtifactByTypeFromMemory(
        normalizedTenantId,
        normalizedRunId,
        normalizedType
      );
    }
  }

  async createReplayBaseline(
    tenantId: string,
    input: CreateReplayBaselineInput
  ): Promise<ReplayBaseline> {
    const dataset = await this.createReplayDataset(tenantId, {
      name: input.name,
      description: input.description,
      model: firstNonEmptyString(toDbRow(input.metadata)?.model) ?? "unknown",
      promptVersion: firstNonEmptyString(
        toDbRow(input.metadata)?.promptVersion,
        toDbRow(input.metadata)?.prompt_version
      ) ?? undefined,
      externalDatasetId: input.datasetRef,
      caseCount: input.scenarioCount,
      metadata: input.metadata,
      createdAt: input.createdAt,
    });
    return mapReplayDatasetToBaseline(dataset);
  }

  async listReplayBaselines(
    tenantId: string,
    input: ListReplayBaselinesInput = {}
  ): Promise<ReplayBaseline[]> {
    const items = await this.listReplayDatasets(tenantId, {
      keyword: input.keyword,
      limit: input.limit,
    });
    return items.map(mapReplayDatasetToBaseline);
  }

  async getReplayBaselineById(
    tenantId: string,
    baselineId: string
  ): Promise<ReplayBaseline | null> {
    const dataset = await this.getReplayDatasetById(tenantId, baselineId);
    return dataset ? mapReplayDatasetToBaseline(dataset) : null;
  }

  async createReplayJob(tenantId: string, input: CreateReplayJobInput): Promise<ReplayJob> {
    const replayRun = await this.createReplayRun(tenantId, {
      datasetId: input.baselineId,
      status: input.status,
      parameters: input.parameters,
      summary: input.summary,
      diff: input.diff,
      error: input.error,
      startedAt: input.startedAt,
      finishedAt: input.finishedAt,
      createdAt: input.createdAt,
    });
    return mapReplayRunToJob(replayRun);
  }

  async listReplayJobs(
    tenantId: string,
    input: ListReplayJobsInput = {}
  ): Promise<ReplayJob[]> {
    const items = await this.listReplayRuns(tenantId, {
      datasetId: input.baselineId,
      status: input.status,
      limit: input.limit,
    });
    return items.map(mapReplayRunToJob);
  }

  async getReplayJobById(tenantId: string, replayJobId: string): Promise<ReplayJob | null> {
    const replayRun = await this.getReplayRunById(tenantId, replayJobId);
    return replayRun ? mapReplayRunToJob(replayRun) : null;
  }

  async getReplayJobDiff(
    tenantId: string,
    replayJobId: string
  ): Promise<Record<string, unknown> | null> {
    return this.getReplayRunDiff(tenantId, replayJobId);
  }

  async updateReplayJob(
    tenantId: string,
    replayJobId: string,
    input: UpdateReplayJobInput
  ): Promise<ReplayJob | null> {
    const replayRun = await this.updateReplayRun(tenantId, replayJobId, input);
    return replayRun ? mapReplayRunToJob(replayRun) : null;
  }

  private async getPool(): Promise<PgPool | null> {
    await this.ensureInitialized();
    return this.pool;
  }

  private async withTransaction<T>(
    pool: PgPool,
    runner: (client: PgClient) => Promise<T>
  ): Promise<T> {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await runner(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      try {
        await client.query("ROLLBACK");
      } catch {
        // noop: rollback failure should not hide original error
      }
      throw error;
    } finally {
      client.release();
    }
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initPromise) {
      await this.initPromise;
      return;
    }

    this.initPromise = this.initializePool();
    await this.initPromise;
  }

  private async initializePool(): Promise<void> {
    const connectionString = (Bun.env.DATABASE_URL ?? "").trim();
    if (!connectionString) {
      return;
    }

    try {
      const pg = (await import("pg")) as unknown as PgModule;
      const pool = new pg.Pool({ connectionString });

      pool.on("error", (error) => {
        this.disableDb(error, "连接池异常");
      });

      await pool.query("SELECT 1");
      await this.ensureSchema(pool);
      this.pool = pool;
      console.info("[control-plane] PostgreSQL 已启用。", { mode: "postgres" });
    } catch (error) {
      this.disableDb(error, "初始化失败");
    }
  }

  private async ensureSchema(pool: PgPool): Promise<void> {
    await pool.query(
      `CREATE TABLE IF NOT EXISTS sources (
         id TEXT PRIMARY KEY,
         provider TEXT NOT NULL DEFAULT 'unknown',
         source_type TEXT NOT NULL DEFAULT 'local',
         hostname TEXT,
         agent_id TEXT,
         tenant_id TEXT NOT NULL DEFAULT 'default',
         workspace_id TEXT,
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         name TEXT,
         type TEXT,
         location TEXT,
         access_mode TEXT NOT NULL DEFAULT 'realtime',
         sync_cron TEXT,
         sync_retention_days INTEGER,
         enabled BOOLEAN NOT NULL DEFAULT TRUE,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS sessions (
         id TEXT PRIMARY KEY,
         source_id TEXT NOT NULL REFERENCES sources(id),
         provider TEXT NOT NULL DEFAULT 'unknown',
         native_session_id TEXT NOT NULL,
         tool TEXT,
         workspace TEXT,
         model TEXT,
         started_at TIMESTAMPTZ,
         ended_at TIMESTAMPTZ,
         message_count INTEGER NOT NULL DEFAULT 0,
         tokens BIGINT NOT NULL DEFAULT 0,
         cost NUMERIC(18, 6) NOT NULL DEFAULT 0,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE sources
         ADD COLUMN IF NOT EXISTS provider TEXT,
         ADD COLUMN IF NOT EXISTS source_type TEXT,
         ADD COLUMN IF NOT EXISTS hostname TEXT,
         ADD COLUMN IF NOT EXISTS agent_id TEXT,
         ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default',
         ADD COLUMN IF NOT EXISTS workspace_id TEXT,
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS name TEXT,
         ADD COLUMN IF NOT EXISTS type TEXT,
         ADD COLUMN IF NOT EXISTS location TEXT,
         ADD COLUMN IF NOT EXISTS access_mode TEXT NOT NULL DEFAULT 'realtime',
         ADD COLUMN IF NOT EXISTS sync_cron TEXT,
         ADD COLUMN IF NOT EXISTS sync_retention_days INTEGER,
         ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE,
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE sources
       SET provider = COALESCE(NULLIF(provider, ''), 'unknown'),
           source_type = COALESCE(NULLIF(source_type, ''), NULLIF(type, ''), 'local'),
           hostname = COALESCE(NULLIF(hostname, ''), NULLIF(location, '')),
           agent_id = COALESCE(NULLIF(agent_id, ''), NULLIF(name, '')),
           tenant_id = COALESCE(
             NULLIF(tenant_id, ''),
             NULLIF((COALESCE(metadata, '{}'::jsonb)->>'tenant_id'), ''),
             NULLIF((COALESCE(metadata, '{}'::jsonb)->>'tenantId'), ''),
             '${DEFAULT_TENANT_ID}'
           ),
           metadata = COALESCE(metadata, '{}'::jsonb),
           name = COALESCE(NULLIF(name, ''), NULLIF(agent_id, ''), NULLIF(hostname, ''), id),
           type = COALESCE(NULLIF(type, ''), NULLIF(source_type, ''), 'local'),
           location = COALESCE(NULLIF(location, ''), NULLIF(hostname, ''), NULLIF(workspace_id, ''), ''),
           access_mode = CASE
             WHEN access_mode IN ('realtime', 'sync', 'hybrid') THEN access_mode
             WHEN COALESCE(
               NULLIF((COALESCE(metadata, '{}'::jsonb)->>'access_mode'), ''),
               NULLIF((COALESCE(metadata, '{}'::jsonb)->>'accessMode'), '')
             ) IN ('realtime', 'sync', 'hybrid')
               THEN COALESCE(
                 NULLIF((COALESCE(metadata, '{}'::jsonb)->>'access_mode'), ''),
                 NULLIF((COALESCE(metadata, '{}'::jsonb)->>'accessMode'), '')
               )
             WHEN COALESCE(
               NULLIF(sync_cron, ''),
               NULLIF((COALESCE(metadata, '{}'::jsonb)->>'sync_cron'), ''),
               NULLIF((COALESCE(metadata, '{}'::jsonb)->>'syncCron'), '')
             ) IS NOT NULL
               THEN 'sync'
             ELSE 'realtime'
           END,
           sync_cron = COALESCE(
             NULLIF(sync_cron, ''),
             NULLIF((COALESCE(metadata, '{}'::jsonb)->>'sync_cron'), ''),
             NULLIF((COALESCE(metadata, '{}'::jsonb)->>'syncCron'), '')
           ),
           sync_retention_days = CASE
             WHEN sync_retention_days IS NOT NULL AND sync_retention_days >= 0
               THEN sync_retention_days
             WHEN COALESCE(
               NULLIF((COALESCE(metadata, '{}'::jsonb)->>'sync_retention_days'), ''),
               NULLIF((COALESCE(metadata, '{}'::jsonb)->>'syncRetentionDays'), '')
             ) ~ '^[0-9]+$'
               THEN (
                 COALESCE(
                   NULLIF((COALESCE(metadata, '{}'::jsonb)->>'sync_retention_days'), ''),
                   NULLIF((COALESCE(metadata, '{}'::jsonb)->>'syncRetentionDays'), '')
                 )
               )::integer
             ELSE NULL
           END,
           enabled = COALESCE(enabled, true),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE provider IS NULL
          OR provider = ''
          OR source_type IS NULL
          OR source_type = ''
          OR tenant_id IS NULL
          OR tenant_id = ''
          OR metadata IS NULL
          OR name IS NULL
          OR name = ''
          OR type IS NULL
          OR type = ''
          OR location IS NULL
          OR access_mode IS NULL
          OR access_mode NOT IN ('realtime', 'sync', 'hybrid')
          OR sync_cron = ''
          OR sync_retention_days < 0
          OR (
            sync_cron IS NULL
            AND COALESCE(
              NULLIF((COALESCE(metadata, '{}'::jsonb)->>'sync_cron'), ''),
              NULLIF((COALESCE(metadata, '{}'::jsonb)->>'syncCron'), '')
            ) IS NOT NULL
          )
          OR (
            sync_retention_days IS NULL
            AND COALESCE(
              NULLIF((COALESCE(metadata, '{}'::jsonb)->>'sync_retention_days'), ''),
              NULLIF((COALESCE(metadata, '{}'::jsonb)->>'syncRetentionDays'), '')
            ) ~ '^[0-9]+$'
          )
          OR enabled IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `ALTER TABLE sessions
         ADD COLUMN IF NOT EXISTS provider TEXT,
         ADD COLUMN IF NOT EXISTS native_session_id TEXT,
         ADD COLUMN IF NOT EXISTS tool TEXT,
         ADD COLUMN IF NOT EXISTS workspace TEXT,
         ADD COLUMN IF NOT EXISTS message_count INTEGER NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS tokens BIGINT NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS cost NUMERIC(18, 6) NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE sessions
       SET provider = COALESCE(NULLIF(provider, ''), NULLIF(tool, ''), 'unknown'),
           native_session_id = COALESCE(NULLIF(native_session_id, ''), id),
           tool = COALESCE(NULLIF(tool, ''), NULLIF(provider, '')),
           message_count = COALESCE(message_count, 0),
           tokens = COALESCE(tokens, 0),
           cost = COALESCE(cost, 0),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE provider IS NULL
          OR provider = ''
          OR native_session_id IS NULL
          OR native_session_id = ''
          OR message_count IS NULL
          OR tokens IS NULL
          OR cost IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_uni
       ON sessions (source_id, provider, native_session_id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_sessions_source_started_at
       ON sessions (source_id, started_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_sessions_started_at
       ON sessions (started_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS events (
         id TEXT PRIMARY KEY,
         session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
         source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
         event_type TEXT NOT NULL DEFAULT 'message',
         role TEXT,
         text TEXT,
         model TEXT,
         "timestamp" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         input_tokens BIGINT NOT NULL DEFAULT 0,
         output_tokens BIGINT NOT NULL DEFAULT 0,
         cache_read_tokens BIGINT NOT NULL DEFAULT 0,
         cache_write_tokens BIGINT NOT NULL DEFAULT 0,
         reasoning_tokens BIGINT NOT NULL DEFAULT 0,
         cost_usd NUMERIC(18, 6) NOT NULL DEFAULT 0,
         source_path TEXT,
         source_offset INTEGER,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE events
         ADD COLUMN IF NOT EXISTS id TEXT,
         ADD COLUMN IF NOT EXISTS session_id TEXT,
         ADD COLUMN IF NOT EXISTS source_id TEXT,
         ADD COLUMN IF NOT EXISTS event_type TEXT,
         ADD COLUMN IF NOT EXISTS role TEXT,
         ADD COLUMN IF NOT EXISTS text TEXT,
         ADD COLUMN IF NOT EXISTS model TEXT,
         ADD COLUMN IF NOT EXISTS "timestamp" TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS input_tokens BIGINT NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS output_tokens BIGINT NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS cache_read_tokens BIGINT NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS cache_write_tokens BIGINT NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS reasoning_tokens BIGINT NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS cost_usd NUMERIC(18, 6) NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS source_path TEXT,
         ADD COLUMN IF NOT EXISTS source_offset INTEGER,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE events AS evt
       SET id = COALESCE(NULLIF(evt.id, ''), md5(random()::text || clock_timestamp()::text)),
           session_id = COALESCE(
             NULLIF(evt.session_id, ''),
             (
               SELECT sess.id
               FROM sessions AS sess
               WHERE sess.id = evt.id
               LIMIT 1
             )
           ),
           source_id = COALESCE(
             NULLIF(evt.source_id, ''),
             (
               SELECT sess.source_id
               FROM sessions AS sess
               WHERE sess.id = COALESCE(
                 NULLIF(evt.session_id, ''),
                 (
                   SELECT fallback.id
                   FROM sessions AS fallback
                   WHERE fallback.id = evt.id
                   LIMIT 1
                 )
               )
               LIMIT 1
             )
           ),
           event_type = COALESCE(NULLIF(evt.event_type, ''), 'message'),
           role = NULLIF(evt.role, ''),
           text = NULLIF(evt.text, ''),
           model = NULLIF(evt.model, ''),
           "timestamp" = COALESCE(evt."timestamp", evt.created_at, NOW()),
           input_tokens = GREATEST(COALESCE(evt.input_tokens, 0), 0),
           output_tokens = GREATEST(COALESCE(evt.output_tokens, 0), 0),
           cache_read_tokens = GREATEST(COALESCE(evt.cache_read_tokens, 0), 0),
           cache_write_tokens = GREATEST(COALESCE(evt.cache_write_tokens, 0), 0),
           reasoning_tokens = GREATEST(COALESCE(evt.reasoning_tokens, 0), 0),
           cost_usd = GREATEST(COALESCE(evt.cost_usd, 0), 0),
           source_path = NULLIF(evt.source_path, ''),
           source_offset = CASE
             WHEN evt.source_offset IS NULL THEN NULL
             WHEN evt.source_offset < 0 THEN 0
             ELSE evt.source_offset
           END,
           created_at = COALESCE(evt.created_at, NOW()),
           updated_at = COALESCE(evt.updated_at, evt.created_at, NOW())
       WHERE evt.id IS NULL
          OR evt.id = ''
          OR evt.session_id IS NULL
          OR evt.session_id = ''
          OR evt.source_id IS NULL
          OR evt.source_id = ''
          OR evt.event_type IS NULL
          OR evt.event_type = ''
          OR evt.role = ''
          OR evt.text = ''
          OR evt.model = ''
          OR evt."timestamp" IS NULL
          OR evt.input_tokens IS NULL
          OR evt.input_tokens < 0
          OR evt.output_tokens IS NULL
          OR evt.output_tokens < 0
          OR evt.cache_read_tokens IS NULL
          OR evt.cache_read_tokens < 0
          OR evt.cache_write_tokens IS NULL
          OR evt.cache_write_tokens < 0
          OR evt.reasoning_tokens IS NULL
          OR evt.reasoning_tokens < 0
          OR evt.cost_usd IS NULL
          OR evt.cost_usd < 0
          OR evt.source_path = ''
          OR evt.source_offset < 0
          OR evt.created_at IS NULL
          OR evt.updated_at IS NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_events_session_timestamp_created_at
       ON events (session_id, "timestamp" ASC, created_at ASC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_events_source_timestamp
       ON events (source_id, "timestamp" DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_events_text_fts
       ON events
       USING GIN (to_tsvector('simple', COALESCE(text, '')))`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS pricing_catalog_versions (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         version INTEGER NOT NULL DEFAULT 1,
         note TEXT,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE pricing_catalog_versions
         ADD COLUMN IF NOT EXISTS id TEXT,
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS version INTEGER,
         ADD COLUMN IF NOT EXISTS note TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE pricing_catalog_versions AS versions
       SET id = COALESCE(
             NULLIF(versions.id, ''),
             md5(random()::text || clock_timestamp()::text)
           ),
           tenant_id = COALESCE(NULLIF(versions.tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           version = GREATEST(COALESCE(versions.version, 1), 1),
           note = NULLIF(versions.note, ''),
           created_at = COALESCE(versions.created_at, NOW())
       WHERE versions.id IS NULL
          OR versions.id = ''
          OR versions.tenant_id IS NULL
          OR versions.tenant_id = ''
          OR versions.version IS NULL
          OR versions.version < 1
          OR versions.note = ''
          OR versions.created_at IS NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_pricing_catalog_versions_tenant_version
       ON pricing_catalog_versions (tenant_id, version DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS pricing_catalog_entries (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         version_id TEXT NOT NULL REFERENCES pricing_catalog_versions(id) ON DELETE CASCADE,
         model_name TEXT NOT NULL,
         input_per_1k NUMERIC(18, 6) NOT NULL DEFAULT 0,
         output_per_1k NUMERIC(18, 6) NOT NULL DEFAULT 0,
         cache_read_per_1k NUMERIC(18, 6),
         cache_write_per_1k NUMERIC(18, 6),
         reasoning_per_1k NUMERIC(18, 6),
         currency TEXT NOT NULL DEFAULT 'USD',
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE pricing_catalog_entries
         ADD COLUMN IF NOT EXISTS id TEXT,
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS version_id TEXT,
         ADD COLUMN IF NOT EXISTS model_name TEXT,
         ADD COLUMN IF NOT EXISTS input_per_1k NUMERIC(18, 6),
         ADD COLUMN IF NOT EXISTS output_per_1k NUMERIC(18, 6),
         ADD COLUMN IF NOT EXISTS cache_read_per_1k NUMERIC(18, 6),
         ADD COLUMN IF NOT EXISTS cache_write_per_1k NUMERIC(18, 6),
         ADD COLUMN IF NOT EXISTS reasoning_per_1k NUMERIC(18, 6),
         ADD COLUMN IF NOT EXISTS currency TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE pricing_catalog_entries AS entries
       SET id = COALESCE(
             NULLIF(entries.id, ''),
             md5(random()::text || clock_timestamp()::text)
           ),
           tenant_id = COALESCE(NULLIF(entries.tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           version_id = COALESCE(
             NULLIF(entries.version_id, ''),
             (
               SELECT versions.id
               FROM pricing_catalog_versions AS versions
               WHERE versions.tenant_id = COALESCE(
                 NULLIF(entries.tenant_id, ''),
                 '${DEFAULT_TENANT_ID}'
               )
               ORDER BY versions.version DESC, versions.created_at DESC
               LIMIT 1
             )
           ),
           model_name = COALESCE(NULLIF(entries.model_name, ''), 'unknown'),
           input_per_1k = GREATEST(COALESCE(entries.input_per_1k, 0), 0),
           output_per_1k = GREATEST(COALESCE(entries.output_per_1k, 0), 0),
           cache_read_per_1k = CASE
             WHEN entries.cache_read_per_1k IS NULL THEN NULL
             ELSE GREATEST(entries.cache_read_per_1k, 0)
           END,
           cache_write_per_1k = CASE
             WHEN entries.cache_write_per_1k IS NULL THEN NULL
             ELSE GREATEST(entries.cache_write_per_1k, 0)
           END,
           reasoning_per_1k = CASE
             WHEN entries.reasoning_per_1k IS NULL THEN NULL
             ELSE GREATEST(entries.reasoning_per_1k, 0)
           END,
           currency = UPPER(COALESCE(NULLIF(entries.currency, ''), 'USD')),
           created_at = COALESCE(entries.created_at, NOW())
       WHERE entries.id IS NULL
          OR entries.id = ''
          OR entries.tenant_id IS NULL
          OR entries.tenant_id = ''
          OR entries.version_id IS NULL
          OR entries.version_id = ''
          OR entries.model_name IS NULL
          OR entries.model_name = ''
          OR entries.input_per_1k IS NULL
          OR entries.input_per_1k < 0
          OR entries.output_per_1k IS NULL
          OR entries.output_per_1k < 0
          OR entries.cache_read_per_1k < 0
          OR entries.cache_write_per_1k < 0
          OR entries.reasoning_per_1k < 0
          OR entries.currency IS NULL
          OR entries.currency = ''
          OR entries.created_at IS NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_pricing_catalog_entries_tenant_version_model
       ON pricing_catalog_entries (tenant_id, version_id, model_name)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS sync_jobs (
         id TEXT PRIMARY KEY,
         source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
         mode TEXT NOT NULL DEFAULT 'realtime',
         status TEXT NOT NULL DEFAULT 'pending',
         error TEXT,
         "trigger" TEXT,
         attempt INTEGER NOT NULL DEFAULT 1,
         started_at TIMESTAMPTZ,
         ended_at TIMESTAMPTZ,
         next_run_at TIMESTAMPTZ,
         duration_ms INTEGER,
         error_code TEXT,
         error_detail TEXT,
         cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE sync_jobs
         ADD COLUMN IF NOT EXISTS source_id TEXT,
         ADD COLUMN IF NOT EXISTS mode TEXT,
         ADD COLUMN IF NOT EXISTS status TEXT,
         ADD COLUMN IF NOT EXISTS error TEXT,
         ADD COLUMN IF NOT EXISTS "trigger" TEXT,
         ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 1,
         ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS ended_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS duration_ms INTEGER,
         ADD COLUMN IF NOT EXISTS error_code TEXT,
         ADD COLUMN IF NOT EXISTS error_detail TEXT,
         ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE sync_jobs
       SET mode = CASE
             WHEN mode IN ('realtime', 'sync', 'hybrid') THEN mode
             ELSE 'realtime'
           END,
           status = CASE
             WHEN status = 'canceled' THEN 'cancelled'
             WHEN status IN ('pending', 'running', 'success', 'failed', 'cancelled') THEN status
             ELSE 'pending'
           END,
           "trigger" = COALESCE(NULLIF("trigger", ''), 'manual'),
           attempt = GREATEST(COALESCE(attempt, 1), 1),
           started_at = CASE
             WHEN started_at IS NOT NULL THEN started_at
             WHEN status IN ('running', 'success', 'failed', 'cancelled', 'canceled')
               THEN COALESCE(created_at, NOW())
             ELSE NULL
           END,
           ended_at = CASE
             WHEN ended_at IS NOT NULL THEN ended_at
             WHEN status IN ('success', 'failed', 'cancelled', 'canceled')
               THEN COALESCE(updated_at, created_at, NOW())
             ELSE NULL
           END,
           duration_ms = CASE
             WHEN duration_ms IS NOT NULL AND duration_ms >= 0 THEN duration_ms
             WHEN COALESCE(
               ended_at,
               CASE
                 WHEN status IN ('success', 'failed', 'cancelled', 'canceled')
                   THEN COALESCE(updated_at, created_at, NOW())
                 ELSE NULL
               END
             ) IS NOT NULL
               THEN GREATEST(
                 0,
                 FLOOR(
                   EXTRACT(
                     EPOCH FROM (
                       COALESCE(
                         ended_at,
                         CASE
                           WHEN status IN ('success', 'failed', 'cancelled', 'canceled')
                             THEN COALESCE(updated_at, created_at, NOW())
                           ELSE NOW()
                         END
                       ) -
                       COALESCE(
                         started_at,
                         CASE
                           WHEN status IN ('running', 'success', 'failed', 'cancelled', 'canceled')
                             THEN COALESCE(created_at, NOW())
                           ELSE NOW()
                         END
                       )
                     )
                   ) * 1000
                 )::INTEGER
               )
             ELSE NULL
           END,
           error = NULLIF(error, ''),
           error_code = NULLIF(error_code, ''),
           error_detail = COALESCE(NULLIF(error_detail, ''), NULLIF(error, '')),
           cancel_requested = COALESCE(cancel_requested, FALSE),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE mode IS NULL
          OR mode NOT IN ('realtime', 'sync', 'hybrid')
          OR status IS NULL
          OR status NOT IN ('pending', 'running', 'success', 'failed', 'cancelled')
          OR "trigger" IS NULL
          OR "trigger" = ''
          OR attempt IS NULL
          OR attempt < 1
          OR duration_ms < 0
          OR error = ''
          OR error_code = ''
          OR error_detail = ''
          OR cancel_requested IS NULL
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_sync_jobs_source_created_at
       ON sync_jobs (source_id, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_sync_jobs_source_status_created_at
       ON sync_jobs (source_id, status, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_sync_jobs_source_status_ended_updated
       ON sync_jobs (source_id, status, ended_at DESC, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS source_watermarks (
         source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
         provider TEXT NOT NULL DEFAULT 'unknown',
         watermark TEXT NOT NULL DEFAULT '',
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE source_watermarks
         ADD COLUMN IF NOT EXISTS source_id TEXT,
         ADD COLUMN IF NOT EXISTS provider TEXT,
         ADD COLUMN IF NOT EXISTS watermark TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE source_watermarks
       SET provider = COALESCE(NULLIF(provider, ''), 'unknown'),
           watermark = COALESCE(watermark, ''),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE provider IS NULL
          OR provider = ''
          OR watermark IS NULL
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `DELETE FROM source_watermarks
       WHERE ctid IN (
         SELECT ctid
         FROM (
           SELECT ctid,
                  ROW_NUMBER() OVER (
                    PARTITION BY source_id, provider
                    ORDER BY updated_at DESC, created_at DESC, ctid DESC
                  ) AS row_index
           FROM source_watermarks
         ) AS ranked
         WHERE ranked.row_index > 1
       )`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_source_watermarks_source_provider
       ON source_watermarks (source_id, provider)`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_source_watermarks_source_provider_unique
       ON source_watermarks (source_id, provider)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_source_watermarks_source_updated_at
       ON source_watermarks (source_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS parse_failures (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
         parser_key TEXT NOT NULL DEFAULT 'unknown',
         error_code TEXT NOT NULL DEFAULT 'unknown',
         error_message TEXT NOT NULL DEFAULT '',
         source_path TEXT,
         source_offset INTEGER,
         raw_hash TEXT,
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE parse_failures
         ADD COLUMN IF NOT EXISTS id TEXT,
         ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         ADD COLUMN IF NOT EXISTS source_id TEXT,
         ADD COLUMN IF NOT EXISTS parser_key TEXT,
         ADD COLUMN IF NOT EXISTS error_code TEXT,
         ADD COLUMN IF NOT EXISTS error_message TEXT,
         ADD COLUMN IF NOT EXISTS source_path TEXT,
         ADD COLUMN IF NOT EXISTS source_offset INTEGER,
         ADD COLUMN IF NOT EXISTS raw_hash TEXT,
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE parse_failures AS failures
       SET id = COALESCE(
             NULLIF(failures.id, ''),
             md5(random()::text || clock_timestamp()::text)
           ),
           tenant_id = COALESCE(
             NULLIF(failures.tenant_id, ''),
             (
               SELECT COALESCE(NULLIF(src.tenant_id, ''), '${DEFAULT_TENANT_ID}')
               FROM sources AS src
               WHERE src.id = failures.source_id
               LIMIT 1
             ),
             '${DEFAULT_TENANT_ID}'
           ),
           parser_key = COALESCE(NULLIF(failures.parser_key, ''), 'unknown'),
           error_code = COALESCE(NULLIF(failures.error_code, ''), 'unknown'),
           error_message = COALESCE(
             NULLIF(failures.error_message, ''),
             NULLIF(failures.error_code, ''),
             'unknown'
           ),
           source_path = NULLIF(failures.source_path, ''),
           source_offset = CASE
             WHEN failures.source_offset IS NULL THEN NULL
             WHEN failures.source_offset < 0 THEN 0
             ELSE failures.source_offset
           END,
           raw_hash = NULLIF(failures.raw_hash, ''),
           metadata = COALESCE(failures.metadata, '{}'::jsonb),
           occurred_at = COALESCE(failures.occurred_at, failures.created_at, NOW()),
           created_at = COALESCE(failures.created_at, failures.occurred_at, NOW())
       WHERE failures.id IS NULL
          OR failures.id = ''
          OR failures.tenant_id IS NULL
          OR failures.tenant_id = ''
          OR failures.parser_key IS NULL
          OR failures.parser_key = ''
          OR failures.error_code IS NULL
          OR failures.error_code = ''
          OR failures.error_message IS NULL
          OR failures.error_message = ''
          OR failures.source_path = ''
          OR failures.source_offset < 0
          OR failures.raw_hash = ''
          OR failures.metadata IS NULL
          OR failures.occurred_at IS NULL
          OR failures.created_at IS NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_parse_failures_tenant_source_occurred_at
       ON parse_failures (tenant_id, source_id, occurred_at DESC, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_parse_failures_source_parser_error
       ON parse_failures (source_id, parser_key, error_code, occurred_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS budgets (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT 'default',
         scope TEXT NOT NULL DEFAULT 'global',
         source_id TEXT NOT NULL DEFAULT '',
         organization_id TEXT NOT NULL DEFAULT '',
         user_id TEXT NOT NULL DEFAULT '',
         model_name TEXT NOT NULL DEFAULT '',
         period TEXT NOT NULL DEFAULT 'monthly',
         token_limit BIGINT NOT NULL DEFAULT 0,
         cost_limit NUMERIC(18, 6) NOT NULL DEFAULT 0,
         alert_threshold NUMERIC(10, 6) NOT NULL DEFAULT 0.8,
         warning_threshold NUMERIC(10, 6) NOT NULL DEFAULT 0.8,
         escalated_threshold NUMERIC(10, 6) NOT NULL DEFAULT 0.9,
         critical_threshold NUMERIC(10, 6) NOT NULL DEFAULT 1,
         enabled BOOLEAN NOT NULL DEFAULT TRUE,
         governance_state TEXT NOT NULL DEFAULT 'active',
         freeze_reason TEXT,
         frozen_at TIMESTAMPTZ,
         frozen_by_alert_id TEXT,
         last_evaluated_at TIMESTAMPTZ,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE budgets
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS scope TEXT,
         ADD COLUMN IF NOT EXISTS source_id TEXT,
         ADD COLUMN IF NOT EXISTS organization_id TEXT,
         ADD COLUMN IF NOT EXISTS user_id TEXT,
         ADD COLUMN IF NOT EXISTS model_name TEXT,
         ADD COLUMN IF NOT EXISTS period TEXT,
         ADD COLUMN IF NOT EXISTS token_limit BIGINT,
         ADD COLUMN IF NOT EXISTS cost_limit NUMERIC(18, 6),
         ADD COLUMN IF NOT EXISTS alert_threshold NUMERIC(10, 6),
         ADD COLUMN IF NOT EXISTS warning_threshold NUMERIC(10, 6),
         ADD COLUMN IF NOT EXISTS escalated_threshold NUMERIC(10, 6),
         ADD COLUMN IF NOT EXISTS critical_threshold NUMERIC(10, 6),
         ADD COLUMN IF NOT EXISTS enabled BOOLEAN,
         ADD COLUMN IF NOT EXISTS governance_state TEXT,
         ADD COLUMN IF NOT EXISTS freeze_reason TEXT,
         ADD COLUMN IF NOT EXISTS frozen_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS frozen_by_alert_id TEXT,
         ADD COLUMN IF NOT EXISTS last_evaluated_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE budgets
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), 'default'),
           scope = CASE
             WHEN scope IN ('global', 'source', 'org', 'user', 'model') THEN scope
             ELSE 'global'
           END,
           source_id = COALESCE(source_id, ''),
           organization_id = COALESCE(organization_id, ''),
           user_id = COALESCE(user_id, ''),
           model_name = COALESCE(model_name, ''),
           period = COALESCE(NULLIF(period, ''), 'monthly'),
           token_limit = COALESCE(token_limit, 0),
           cost_limit = COALESCE(cost_limit, 0),
           alert_threshold = COALESCE(alert_threshold, 0.8),
           warning_threshold = COALESCE(warning_threshold, alert_threshold, 0.8),
           escalated_threshold = GREATEST(
             COALESCE(escalated_threshold, warning_threshold, alert_threshold, 0.9),
             COALESCE(warning_threshold, alert_threshold, 0.8)
           ),
           critical_threshold = GREATEST(
             COALESCE(critical_threshold, escalated_threshold, warning_threshold, alert_threshold, 1),
             GREATEST(
               COALESCE(escalated_threshold, warning_threshold, alert_threshold, 0.9),
               COALESCE(warning_threshold, alert_threshold, 0.8)
             )
           ),
           enabled = COALESCE(enabled, TRUE),
           governance_state = CASE
             WHEN governance_state IN ('active', 'frozen', 'pending_release') THEN governance_state
             ELSE 'active'
           END,
           freeze_reason = NULLIF(freeze_reason, ''),
           frozen_by_alert_id = NULLIF(frozen_by_alert_id, ''),
           last_evaluated_at = last_evaluated_at,
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR scope IS NULL
          OR source_id IS NULL
          OR organization_id IS NULL
          OR user_id IS NULL
          OR model_name IS NULL
          OR period IS NULL
          OR period = ''
          OR token_limit IS NULL
          OR cost_limit IS NULL
          OR alert_threshold IS NULL
          OR warning_threshold IS NULL
          OR escalated_threshold IS NULL
          OR critical_threshold IS NULL
          OR enabled IS NULL
          OR governance_state IS NULL
          OR governance_state NOT IN ('active', 'frozen', 'pending_release')
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `UPDATE budgets
       SET enabled = CASE
         WHEN governance_state = 'active' THEN TRUE
         ELSE FALSE
       END,
           updated_at = COALESCE(updated_at, NOW())
       WHERE (governance_state = 'active' AND enabled IS DISTINCT FROM TRUE)
          OR (governance_state IN ('frozen', 'pending_release') AND enabled IS DISTINCT FROM FALSE)`
    );

    await pool.query(
      `DO $$
       BEGIN
         IF NOT EXISTS (
           SELECT 1
           FROM pg_constraint
           WHERE conname = 'chk_budgets_enabled_governance_consistent'
             AND conrelid = 'budgets'::regclass
         ) THEN
           ALTER TABLE budgets
             ADD CONSTRAINT chk_budgets_enabled_governance_consistent
             CHECK (
               (governance_state = 'active' AND enabled = TRUE)
               OR (governance_state IN ('frozen', 'pending_release') AND enabled = FALSE)
             );
         END IF;
       END
       $$`
    );

    await pool.query(
      `DROP INDEX IF EXISTS idx_budgets_uni`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_budgets_uni
       ON budgets (tenant_id, scope, source_id, organization_id, user_id, model_name, period)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_budgets_tenant_updated_at
       ON budgets (tenant_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS governance_alerts (
         id BIGSERIAL PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT 'default',
         budget_id TEXT NOT NULL,
         source_id TEXT,
         period TEXT NOT NULL DEFAULT 'monthly',
         window_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         window_end TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         tokens_used BIGINT NOT NULL DEFAULT 0,
         cost_used NUMERIC(18, 8) NOT NULL DEFAULT 0,
         token_limit BIGINT NOT NULL DEFAULT 0,
         cost_limit NUMERIC(18, 8) NOT NULL DEFAULT 0,
         threshold NUMERIC(10, 6) NOT NULL DEFAULT 0.8,
         status TEXT NOT NULL DEFAULT 'open',
         severity TEXT NOT NULL DEFAULT 'warning',
         dedupe_key TEXT NOT NULL DEFAULT '',
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE governance_alerts
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS budget_id TEXT,
         ADD COLUMN IF NOT EXISTS source_id TEXT,
         ADD COLUMN IF NOT EXISTS period TEXT,
         ADD COLUMN IF NOT EXISTS window_start TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS window_end TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS tokens_used BIGINT,
         ADD COLUMN IF NOT EXISTS cost_used NUMERIC(18, 8),
         ADD COLUMN IF NOT EXISTS token_limit BIGINT,
         ADD COLUMN IF NOT EXISTS cost_limit NUMERIC(18, 8),
         ADD COLUMN IF NOT EXISTS threshold NUMERIC(10, 6),
         ADD COLUMN IF NOT EXISTS status TEXT,
         ADD COLUMN IF NOT EXISTS severity TEXT,
         ADD COLUMN IF NOT EXISTS dedupe_key TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE governance_alerts
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), 'default'),
           budget_id = COALESCE(NULLIF(budget_id, ''), 'unknown'),
           source_id = source_id,
           period = COALESCE(NULLIF(period, ''), 'monthly'),
           window_start = COALESCE(window_start, created_at, NOW()),
           window_end = COALESCE(window_end, created_at, NOW()),
           tokens_used = COALESCE(tokens_used, 0),
           cost_used = COALESCE(cost_used, 0),
           token_limit = COALESCE(token_limit, 0),
           cost_limit = COALESCE(cost_limit, 0),
           threshold = COALESCE(threshold, 0.8),
           status = CASE
             WHEN status IN ('open', 'acknowledged', 'resolved') THEN status
             ELSE 'open'
           END,
           severity = CASE
             WHEN severity IN ('warning', 'critical') THEN severity
             ELSE 'warning'
           END,
           dedupe_key = COALESCE(NULLIF(dedupe_key, ''), 'legacy:' || id::text),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR budget_id IS NULL
          OR budget_id = ''
          OR status IS NULL
          OR status NOT IN ('open', 'acknowledged', 'resolved')
          OR severity IS NULL
          OR severity NOT IN ('warning', 'critical')
          OR dedupe_key IS NULL
          OR dedupe_key = ''
          OR period IS NULL
          OR period = ''
          OR window_start IS NULL
          OR window_end IS NULL
          OR tokens_used IS NULL
          OR cost_used IS NULL
          OR token_limit IS NULL
          OR cost_limit IS NULL
          OR threshold IS NULL
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_governance_alerts_dedupe_key
       ON governance_alerts (dedupe_key)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_governance_alerts_tenant_created_at
       ON governance_alerts (tenant_id, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_governance_alerts_filters
       ON governance_alerts (tenant_id, status, severity, source_id, created_at DESC)`
    );

    await pool.query(
      `CREATE OR REPLACE FUNCTION enforce_governance_alert_status_transition()
       RETURNS trigger AS $$
       BEGIN
         IF NEW.status IS DISTINCT FROM OLD.status THEN
           IF OLD.status = 'open' AND NEW.status IN ('acknowledged', 'resolved') THEN
             RETURN NEW;
           END IF;
           IF OLD.status = 'acknowledged' AND NEW.status = 'resolved' THEN
             RETURN NEW;
           END IF;
           RAISE EXCEPTION
             'invalid governance_alerts status transition from % to %',
             OLD.status,
             NEW.status
             USING ERRCODE = '23514';
         END IF;
         RETURN NEW;
       END;
       $$ LANGUAGE plpgsql`
    );

    await pool.query(
      `DO $$
       BEGIN
         IF NOT EXISTS (
           SELECT 1
           FROM pg_trigger
           WHERE tgname = 'trg_governance_alert_status_transition'
             AND tgrelid = 'governance_alerts'::regclass
         ) THEN
           CREATE TRIGGER trg_governance_alert_status_transition
           BEFORE UPDATE OF status ON governance_alerts
           FOR EACH ROW
           EXECUTE FUNCTION enforce_governance_alert_status_transition();
         END IF;
       END
       $$`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS alert_orchestration_rules (
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         id TEXT NOT NULL,
         name TEXT NOT NULL,
         enabled BOOLEAN NOT NULL DEFAULT TRUE,
         event_type TEXT NOT NULL DEFAULT 'alert',
         severity TEXT,
         source_id TEXT,
         dedupe_window_seconds INTEGER NOT NULL DEFAULT 0,
         suppression_window_seconds INTEGER NOT NULL DEFAULT 0,
         merge_window_seconds INTEGER NOT NULL DEFAULT 0,
         sla_minutes INTEGER,
         channels JSONB NOT NULL DEFAULT '[]'::jsonb,
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         PRIMARY KEY (tenant_id, id)
       )`
    );

    await pool.query(
      `ALTER TABLE alert_orchestration_rules
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS id TEXT,
         ADD COLUMN IF NOT EXISTS name TEXT,
         ADD COLUMN IF NOT EXISTS enabled BOOLEAN,
         ADD COLUMN IF NOT EXISTS event_type TEXT,
         ADD COLUMN IF NOT EXISTS severity TEXT,
         ADD COLUMN IF NOT EXISTS source_id TEXT,
         ADD COLUMN IF NOT EXISTS dedupe_window_seconds INTEGER,
         ADD COLUMN IF NOT EXISTS suppression_window_seconds INTEGER,
         ADD COLUMN IF NOT EXISTS merge_window_seconds INTEGER,
         ADD COLUMN IF NOT EXISTS sla_minutes INTEGER,
         ADD COLUMN IF NOT EXISTS channels JSONB,
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE alert_orchestration_rules
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           id = COALESCE(
             NULLIF(id, ''),
             md5(random()::text || clock_timestamp()::text)
           ),
           name = COALESCE(NULLIF(name, ''), 'unnamed-rule'),
           enabled = COALESCE(enabled, TRUE),
           event_type = CASE
             WHEN event_type IN ('alert', 'weekly') THEN event_type
             ELSE 'alert'
           END,
           severity = CASE
             WHEN severity IN ('warning', 'critical') THEN severity
             WHEN NULLIF(severity, '') IS NULL THEN NULL
             ELSE NULL
           END,
           source_id = NULLIF(source_id, ''),
           dedupe_window_seconds = GREATEST(COALESCE(dedupe_window_seconds, 0), 0),
           suppression_window_seconds = GREATEST(COALESCE(suppression_window_seconds, 0), 0),
           merge_window_seconds = GREATEST(COALESCE(merge_window_seconds, 0), 0),
           sla_minutes = CASE
             WHEN sla_minutes IS NULL THEN NULL
             WHEN sla_minutes < 0 THEN 0
             ELSE sla_minutes
           END,
           channels = CASE
             WHEN jsonb_typeof(channels) = 'array' THEN channels
             ELSE '[]'::jsonb
           END,
           updated_at = COALESCE(updated_at, created_at, NOW()),
           created_at = COALESCE(created_at, updated_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR id IS NULL
          OR id = ''
          OR name IS NULL
          OR name = ''
          OR enabled IS NULL
          OR event_type IS NULL
          OR event_type NOT IN ('alert', 'weekly')
          OR severity = ''
          OR source_id = ''
          OR dedupe_window_seconds IS NULL
          OR dedupe_window_seconds < 0
          OR suppression_window_seconds IS NULL
          OR suppression_window_seconds < 0
          OR merge_window_seconds IS NULL
          OR merge_window_seconds < 0
          OR sla_minutes < 0
          OR channels IS NULL
          OR jsonb_typeof(channels) <> 'array'
          OR updated_at IS NULL
          OR created_at IS NULL`
    );

    await pool.query(
      `WITH ranked AS (
         SELECT ctid,
                ROW_NUMBER() OVER (
                  PARTITION BY tenant_id, id
                  ORDER BY updated_at DESC, created_at DESC, ctid DESC
                ) AS row_index
         FROM alert_orchestration_rules
       )
       DELETE FROM alert_orchestration_rules AS rules
       USING ranked
       WHERE rules.ctid = ranked.ctid
         AND ranked.row_index > 1`
    );

    await pool.query(
      `ALTER TABLE alert_orchestration_rules
         ALTER COLUMN tenant_id SET DEFAULT '${DEFAULT_TENANT_ID}',
         ALTER COLUMN tenant_id SET NOT NULL,
         ALTER COLUMN id SET NOT NULL,
         ALTER COLUMN name SET NOT NULL,
         ALTER COLUMN enabled SET DEFAULT TRUE,
         ALTER COLUMN enabled SET NOT NULL,
         ALTER COLUMN event_type SET DEFAULT 'alert',
         ALTER COLUMN event_type SET NOT NULL,
         ALTER COLUMN dedupe_window_seconds SET DEFAULT 0,
         ALTER COLUMN dedupe_window_seconds SET NOT NULL,
         ALTER COLUMN suppression_window_seconds SET DEFAULT 0,
         ALTER COLUMN suppression_window_seconds SET NOT NULL,
         ALTER COLUMN merge_window_seconds SET DEFAULT 0,
         ALTER COLUMN merge_window_seconds SET NOT NULL,
         ALTER COLUMN channels SET DEFAULT '[]'::jsonb,
         ALTER COLUMN channels SET NOT NULL,
         ALTER COLUMN updated_at SET DEFAULT NOW(),
         ALTER COLUMN updated_at SET NOT NULL,
         ALTER COLUMN created_at SET DEFAULT NOW(),
         ALTER COLUMN created_at SET NOT NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_orchestration_rules_tenant_rule
       ON alert_orchestration_rules (tenant_id, id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_alert_orchestration_rules_tenant_updated_at
       ON alert_orchestration_rules (tenant_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_alert_orchestration_rules_filters
       ON alert_orchestration_rules (
         tenant_id,
         event_type,
         enabled,
         severity,
         source_id,
         updated_at DESC
       )`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS alert_orchestration_executions (
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         id TEXT NOT NULL,
         rule_id TEXT NOT NULL,
         event_type TEXT NOT NULL DEFAULT 'alert',
         alert_id TEXT,
         severity TEXT,
         source_id TEXT,
         channels JSONB NOT NULL DEFAULT '[]'::jsonb,
         conflict_rule_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
         dedupe_hit BOOLEAN NOT NULL DEFAULT FALSE,
         suppressed BOOLEAN NOT NULL DEFAULT FALSE,
         simulated BOOLEAN NOT NULL DEFAULT FALSE,
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         PRIMARY KEY (tenant_id, id)
       )`
    );

    await pool.query(
      `ALTER TABLE alert_orchestration_executions
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS id TEXT,
         ADD COLUMN IF NOT EXISTS rule_id TEXT,
         ADD COLUMN IF NOT EXISTS event_type TEXT,
         ADD COLUMN IF NOT EXISTS alert_id TEXT,
         ADD COLUMN IF NOT EXISTS severity TEXT,
         ADD COLUMN IF NOT EXISTS source_id TEXT,
         ADD COLUMN IF NOT EXISTS channels JSONB,
         ADD COLUMN IF NOT EXISTS conflict_rule_ids JSONB,
         ADD COLUMN IF NOT EXISTS dedupe_hit BOOLEAN,
         ADD COLUMN IF NOT EXISTS suppressed BOOLEAN,
         ADD COLUMN IF NOT EXISTS simulated BOOLEAN,
         ADD COLUMN IF NOT EXISTS metadata JSONB,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE alert_orchestration_executions
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           id = COALESCE(
             NULLIF(id, ''),
             md5(random()::text || clock_timestamp()::text)
           ),
           rule_id = COALESCE(NULLIF(rule_id, ''), 'unknown-rule'),
           event_type = CASE
             WHEN event_type IN ('alert', 'weekly') THEN event_type
             ELSE 'alert'
           END,
           alert_id = NULLIF(alert_id, ''),
           severity = CASE
             WHEN severity IN ('warning', 'critical') THEN severity
             WHEN NULLIF(severity, '') IS NULL THEN NULL
             ELSE NULL
           END,
           source_id = NULLIF(source_id, ''),
           channels = CASE
             WHEN jsonb_typeof(channels) = 'array' THEN channels
             ELSE '[]'::jsonb
           END,
           conflict_rule_ids = CASE
             WHEN jsonb_typeof(conflict_rule_ids) = 'array' THEN conflict_rule_ids
             ELSE '[]'::jsonb
           END,
           dedupe_hit = COALESCE(dedupe_hit, FALSE),
           suppressed = COALESCE(suppressed, FALSE),
           simulated = COALESCE(simulated, FALSE),
           metadata = CASE
             WHEN jsonb_typeof(metadata) = 'object' THEN metadata
             ELSE '{}'::jsonb
           END,
           created_at = COALESCE(created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR id IS NULL
          OR id = ''
          OR rule_id IS NULL
          OR rule_id = ''
          OR event_type IS NULL
          OR event_type NOT IN ('alert', 'weekly')
          OR severity = ''
          OR source_id = ''
          OR alert_id = ''
          OR channels IS NULL
          OR jsonb_typeof(channels) <> 'array'
          OR conflict_rule_ids IS NULL
          OR jsonb_typeof(conflict_rule_ids) <> 'array'
          OR dedupe_hit IS NULL
          OR suppressed IS NULL
          OR simulated IS NULL
          OR metadata IS NULL
          OR jsonb_typeof(metadata) <> 'object'
          OR created_at IS NULL`
    );

    await pool.query(
      `WITH ranked AS (
         SELECT ctid,
                ROW_NUMBER() OVER (
                  PARTITION BY tenant_id, id
                  ORDER BY created_at DESC, ctid DESC
                ) AS row_index
         FROM alert_orchestration_executions
       )
       DELETE FROM alert_orchestration_executions AS executions
       USING ranked
       WHERE executions.ctid = ranked.ctid
         AND ranked.row_index > 1`
    );

    await pool.query(
      `ALTER TABLE alert_orchestration_executions
         ALTER COLUMN tenant_id SET DEFAULT '${DEFAULT_TENANT_ID}',
         ALTER COLUMN tenant_id SET NOT NULL,
         ALTER COLUMN id SET NOT NULL,
         ALTER COLUMN rule_id SET NOT NULL,
         ALTER COLUMN event_type SET DEFAULT 'alert',
         ALTER COLUMN event_type SET NOT NULL,
         ALTER COLUMN channels SET DEFAULT '[]'::jsonb,
         ALTER COLUMN channels SET NOT NULL,
         ALTER COLUMN conflict_rule_ids SET DEFAULT '[]'::jsonb,
         ALTER COLUMN conflict_rule_ids SET NOT NULL,
         ALTER COLUMN dedupe_hit SET DEFAULT FALSE,
         ALTER COLUMN dedupe_hit SET NOT NULL,
         ALTER COLUMN suppressed SET DEFAULT FALSE,
         ALTER COLUMN suppressed SET NOT NULL,
         ALTER COLUMN simulated SET DEFAULT FALSE,
         ALTER COLUMN simulated SET NOT NULL,
         ALTER COLUMN metadata SET DEFAULT '{}'::jsonb,
         ALTER COLUMN metadata SET NOT NULL,
         ALTER COLUMN created_at SET DEFAULT NOW(),
         ALTER COLUMN created_at SET NOT NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_orchestration_executions_tenant_id
       ON alert_orchestration_executions (tenant_id, id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_alert_orchestration_executions_tenant_created_at
       ON alert_orchestration_executions (tenant_id, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_alert_orchestration_executions_filters
       ON alert_orchestration_executions (
         tenant_id,
         rule_id,
         event_type,
         alert_id,
         severity,
         source_id,
         created_at DESC
       )`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS tenant_residency_policies (
         tenant_id TEXT PRIMARY KEY,
         mode TEXT NOT NULL DEFAULT 'single_region',
         primary_region TEXT NOT NULL DEFAULT 'cn-hangzhou',
         replica_regions JSONB NOT NULL DEFAULT '[]'::jsonb,
         allow_cross_region_transfer BOOLEAN NOT NULL DEFAULT FALSE,
         require_transfer_approval BOOLEAN NOT NULL DEFAULT FALSE,
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE tenant_residency_policies
         ADD COLUMN IF NOT EXISTS mode TEXT,
         ADD COLUMN IF NOT EXISTS primary_region TEXT,
         ADD COLUMN IF NOT EXISTS replica_regions JSONB,
         ADD COLUMN IF NOT EXISTS allow_cross_region_transfer BOOLEAN,
         ADD COLUMN IF NOT EXISTS require_transfer_approval BOOLEAN,
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE tenant_residency_policies
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           mode = CASE
             WHEN mode IN ('single_region', 'active_active') THEN mode
             ELSE 'single_region'
           END,
           primary_region = COALESCE(NULLIF(primary_region, ''), 'cn-hangzhou'),
           replica_regions = CASE
             WHEN jsonb_typeof(replica_regions) = 'array' THEN replica_regions
             ELSE '[]'::jsonb
           END,
           allow_cross_region_transfer = COALESCE(allow_cross_region_transfer, FALSE),
           require_transfer_approval = COALESCE(require_transfer_approval, FALSE),
           updated_at = COALESCE(updated_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR mode IS NULL
          OR mode NOT IN ('single_region', 'active_active')
          OR primary_region IS NULL
          OR primary_region = ''
          OR replica_regions IS NULL
          OR jsonb_typeof(replica_regions) <> 'array'
          OR allow_cross_region_transfer IS NULL
          OR require_transfer_approval IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS residency_replication_jobs (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         source_region TEXT NOT NULL DEFAULT 'cn-hangzhou',
         target_region TEXT NOT NULL DEFAULT 'cn-shanghai',
         status TEXT NOT NULL DEFAULT 'pending',
         reason TEXT,
         created_by_user_id TEXT,
         approved_by_user_id TEXT,
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         started_at TIMESTAMPTZ,
         finished_at TIMESTAMPTZ
       )`
    );

    await pool.query(
      `ALTER TABLE residency_replication_jobs
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS source_region TEXT,
         ADD COLUMN IF NOT EXISTS target_region TEXT,
         ADD COLUMN IF NOT EXISTS status TEXT,
         ADD COLUMN IF NOT EXISTS reason TEXT,
         ADD COLUMN IF NOT EXISTS created_by_user_id TEXT,
         ADD COLUMN IF NOT EXISTS approved_by_user_id TEXT,
         ADD COLUMN IF NOT EXISTS metadata JSONB,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE residency_replication_jobs
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           source_region = COALESCE(NULLIF(source_region, ''), 'cn-hangzhou'),
           target_region = COALESCE(NULLIF(target_region, ''), 'cn-shanghai'),
           status = CASE
             WHEN status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled') THEN status
             ELSE 'pending'
           END,
           reason = NULLIF(reason, ''),
           created_by_user_id = NULLIF(created_by_user_id, ''),
           approved_by_user_id = NULLIF(approved_by_user_id, ''),
           metadata = COALESCE(metadata, '{}'::jsonb),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW()),
           started_at = started_at,
           finished_at = finished_at
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR source_region IS NULL
          OR source_region = ''
          OR target_region IS NULL
          OR target_region = ''
          OR status IS NULL
          OR status NOT IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')
          OR metadata IS NULL
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_residency_replication_jobs_tenant_created_at
       ON residency_replication_jobs (tenant_id, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_residency_replication_jobs_tenant_status_updated_at
       ON residency_replication_jobs (tenant_id, status, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS rule_assets (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         name TEXT NOT NULL,
         description TEXT,
         status TEXT NOT NULL DEFAULT 'draft',
         latest_version INTEGER NOT NULL DEFAULT 0,
         published_version INTEGER,
         scope_binding JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE rule_assets
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS name TEXT,
         ADD COLUMN IF NOT EXISTS description TEXT,
         ADD COLUMN IF NOT EXISTS status TEXT,
         ADD COLUMN IF NOT EXISTS latest_version INTEGER,
         ADD COLUMN IF NOT EXISTS published_version INTEGER,
         ADD COLUMN IF NOT EXISTS scope_binding JSONB,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE rule_assets
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           name = COALESCE(NULLIF(name, ''), id),
           description = NULLIF(description, ''),
           status = CASE
             WHEN status IN ('draft', 'published', 'deprecated') THEN status
             ELSE 'draft'
           END,
           latest_version = GREATEST(COALESCE(latest_version, 0), 0),
           published_version = CASE
             WHEN published_version IS NULL THEN NULL
             ELSE GREATEST(published_version, 0)
           END,
           scope_binding = CASE
             WHEN jsonb_typeof(scope_binding) = 'object' THEN scope_binding
             ELSE '{}'::jsonb
           END,
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR name IS NULL
          OR name = ''
          OR status IS NULL
          OR status NOT IN ('draft', 'published', 'deprecated')
          OR latest_version IS NULL
          OR latest_version < 0
          OR published_version < 0
          OR scope_binding IS NULL
          OR jsonb_typeof(scope_binding) <> 'object'
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_rule_assets_tenant_updated_at
       ON rule_assets (tenant_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_rule_assets_tenant_status_updated_at
       ON rule_assets (tenant_id, status, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS rule_asset_versions (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         asset_id TEXT NOT NULL,
         version INTEGER NOT NULL,
         content TEXT NOT NULL,
         changelog TEXT,
         created_by_user_id TEXT,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE rule_asset_versions
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS asset_id TEXT,
         ADD COLUMN IF NOT EXISTS version INTEGER,
         ADD COLUMN IF NOT EXISTS content TEXT,
         ADD COLUMN IF NOT EXISTS changelog TEXT,
         ADD COLUMN IF NOT EXISTS created_by_user_id TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE rule_asset_versions
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           asset_id = COALESCE(NULLIF(asset_id, ''), 'unknown'),
           version = GREATEST(COALESCE(version, 1), 1),
           content = COALESCE(content, ''),
           changelog = NULLIF(changelog, ''),
           created_by_user_id = NULLIF(created_by_user_id, ''),
           created_at = COALESCE(created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR asset_id IS NULL
          OR asset_id = ''
          OR version IS NULL
          OR version < 1
          OR content IS NULL
          OR created_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_rule_asset_versions_tenant_asset_version
       ON rule_asset_versions (tenant_id, asset_id, version)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_rule_asset_versions_tenant_asset_created_at
       ON rule_asset_versions (tenant_id, asset_id, created_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS rule_approvals (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         asset_id TEXT NOT NULL,
         version INTEGER NOT NULL,
         approver_user_id TEXT NOT NULL,
         approver_email TEXT,
         decision TEXT NOT NULL DEFAULT 'approved',
         reason TEXT,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE rule_approvals
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS asset_id TEXT,
         ADD COLUMN IF NOT EXISTS version INTEGER,
         ADD COLUMN IF NOT EXISTS approver_user_id TEXT,
         ADD COLUMN IF NOT EXISTS approver_email TEXT,
         ADD COLUMN IF NOT EXISTS decision TEXT,
         ADD COLUMN IF NOT EXISTS reason TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE rule_approvals
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           asset_id = COALESCE(NULLIF(asset_id, ''), 'unknown'),
           version = GREATEST(COALESCE(version, 1), 1),
           approver_user_id = COALESCE(NULLIF(approver_user_id, ''), 'unknown'),
           approver_email = NULLIF(approver_email, ''),
           decision = CASE
             WHEN decision IN ('approved', 'rejected') THEN decision
             ELSE 'approved'
           END,
           reason = NULLIF(reason, ''),
           created_at = COALESCE(created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR asset_id IS NULL
          OR asset_id = ''
          OR version IS NULL
          OR version < 1
          OR approver_user_id IS NULL
          OR approver_user_id = ''
          OR decision IS NULL
          OR decision NOT IN ('approved', 'rejected')
          OR created_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_rule_approvals_tenant_asset_version_approver
       ON rule_approvals (tenant_id, asset_id, version, approver_user_id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_rule_approvals_tenant_asset_created_at
       ON rule_approvals (tenant_id, asset_id, created_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS mcp_tool_policies (
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         tool_id TEXT NOT NULL,
         risk_level TEXT NOT NULL DEFAULT 'medium',
         decision TEXT NOT NULL DEFAULT 'require_approval',
         reason TEXT,
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         PRIMARY KEY (tenant_id, tool_id)
       )`
    );

    await pool.query(
      `ALTER TABLE mcp_tool_policies
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS tool_id TEXT,
         ADD COLUMN IF NOT EXISTS risk_level TEXT,
         ADD COLUMN IF NOT EXISTS decision TEXT,
         ADD COLUMN IF NOT EXISTS reason TEXT,
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE mcp_tool_policies
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           tool_id = COALESCE(NULLIF(tool_id, ''), 'unknown'),
           risk_level = CASE
             WHEN risk_level IN ('low', 'medium', 'high') THEN risk_level
             ELSE 'medium'
           END,
           decision = CASE
             WHEN decision IN ('allow', 'deny', 'require_approval') THEN decision
             ELSE 'require_approval'
           END,
           reason = NULLIF(reason, ''),
           updated_at = COALESCE(updated_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR tool_id IS NULL
          OR tool_id = ''
          OR risk_level IS NULL
          OR risk_level NOT IN ('low', 'medium', 'high')
          OR decision IS NULL
          OR decision NOT IN ('allow', 'deny', 'require_approval')
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_mcp_tool_policies_tenant_updated_at
       ON mcp_tool_policies (tenant_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS mcp_approval_requests (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         tool_id TEXT NOT NULL,
         status TEXT NOT NULL DEFAULT 'pending',
         requested_by_user_id TEXT NOT NULL,
         requested_by_email TEXT,
         reason TEXT,
         reviewed_by_user_id TEXT,
         reviewed_by_email TEXT,
         review_reason TEXT,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE mcp_approval_requests
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS tool_id TEXT,
         ADD COLUMN IF NOT EXISTS status TEXT,
         ADD COLUMN IF NOT EXISTS requested_by_user_id TEXT,
         ADD COLUMN IF NOT EXISTS requested_by_email TEXT,
         ADD COLUMN IF NOT EXISTS reason TEXT,
         ADD COLUMN IF NOT EXISTS reviewed_by_user_id TEXT,
         ADD COLUMN IF NOT EXISTS reviewed_by_email TEXT,
         ADD COLUMN IF NOT EXISTS review_reason TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE mcp_approval_requests
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           tool_id = COALESCE(NULLIF(tool_id, ''), 'unknown'),
           status = CASE
             WHEN status IN ('pending', 'approved', 'rejected') THEN status
             ELSE 'pending'
           END,
           requested_by_user_id = COALESCE(NULLIF(requested_by_user_id, ''), 'unknown'),
           requested_by_email = NULLIF(requested_by_email, ''),
           reason = NULLIF(reason, ''),
           reviewed_by_user_id = NULLIF(reviewed_by_user_id, ''),
           reviewed_by_email = NULLIF(reviewed_by_email, ''),
           review_reason = NULLIF(review_reason, ''),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR tool_id IS NULL
          OR tool_id = ''
          OR status IS NULL
          OR status NOT IN ('pending', 'approved', 'rejected')
          OR requested_by_user_id IS NULL
          OR requested_by_user_id = ''
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_mcp_approval_requests_tenant_status_updated_at
       ON mcp_approval_requests (tenant_id, status, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS mcp_invocation_audits (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         tool_id TEXT NOT NULL,
         decision TEXT NOT NULL DEFAULT 'require_approval',
         result TEXT NOT NULL DEFAULT 'allowed',
         approval_request_id TEXT,
         enforced BOOLEAN NOT NULL DEFAULT FALSE,
         evaluated_decision TEXT,
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE mcp_invocation_audits
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS tool_id TEXT,
         ADD COLUMN IF NOT EXISTS decision TEXT,
         ADD COLUMN IF NOT EXISTS result TEXT,
         ADD COLUMN IF NOT EXISTS approval_request_id TEXT,
         ADD COLUMN IF NOT EXISTS enforced BOOLEAN,
         ADD COLUMN IF NOT EXISTS evaluated_decision TEXT,
         ADD COLUMN IF NOT EXISTS metadata JSONB,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE mcp_invocation_audits
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           tool_id = COALESCE(NULLIF(tool_id, ''), 'unknown'),
           decision = CASE
             WHEN decision IN ('allow', 'deny', 'require_approval') THEN decision
             ELSE 'require_approval'
           END,
           result = CASE
             WHEN result IN ('allowed', 'blocked', 'approved') THEN result
             ELSE 'allowed'
           END,
           approval_request_id = NULLIF(approval_request_id, ''),
           enforced = COALESCE(enforced, FALSE),
           evaluated_decision = CASE
             WHEN evaluated_decision IN ('allow', 'deny', 'require_approval')
               THEN evaluated_decision
             ELSE NULL
           END,
           metadata = COALESCE(metadata, '{}'::jsonb),
           created_at = COALESCE(created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR tool_id IS NULL
          OR tool_id = ''
          OR decision IS NULL
          OR decision NOT IN ('allow', 'deny', 'require_approval')
          OR result IS NULL
          OR result NOT IN ('allowed', 'blocked', 'approved')
          OR enforced IS NULL
          OR (evaluated_decision IS NOT NULL AND evaluated_decision NOT IN ('allow', 'deny', 'require_approval'))
          OR metadata IS NULL
          OR created_at IS NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_mcp_invocation_audits_tenant_created_at
       ON mcp_invocation_audits (tenant_id, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_mcp_invocation_audits_tenant_tool_created_at
       ON mcp_invocation_audits (tenant_id, tool_id, created_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS budget_release_requests (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT 'default',
         budget_id TEXT NOT NULL,
         status TEXT NOT NULL DEFAULT 'pending',
         requested_by_user_id TEXT NOT NULL DEFAULT 'system',
         requested_by_email TEXT,
         requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         approvals JSONB NOT NULL DEFAULT '[]'::jsonb,
         rejected_by_user_id TEXT,
         rejected_by_email TEXT,
         rejected_reason TEXT,
         rejected_at TIMESTAMPTZ,
         executed_at TIMESTAMPTZ,
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE budget_release_requests
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS budget_id TEXT,
         ADD COLUMN IF NOT EXISTS status TEXT,
         ADD COLUMN IF NOT EXISTS requested_by_user_id TEXT,
         ADD COLUMN IF NOT EXISTS requested_by_email TEXT,
         ADD COLUMN IF NOT EXISTS requested_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS approvals JSONB NOT NULL DEFAULT '[]'::jsonb,
         ADD COLUMN IF NOT EXISTS rejected_by_user_id TEXT,
         ADD COLUMN IF NOT EXISTS rejected_by_email TEXT,
         ADD COLUMN IF NOT EXISTS rejected_reason TEXT,
         ADD COLUMN IF NOT EXISTS rejected_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS executed_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE budget_release_requests
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), 'default'),
           budget_id = COALESCE(NULLIF(budget_id, ''), 'unknown'),
           status = CASE
             WHEN status IN ('pending', 'rejected', 'executed') THEN status
             ELSE 'pending'
           END,
           requested_by_user_id = COALESCE(NULLIF(requested_by_user_id, ''), 'system'),
           requested_by_email = NULLIF(requested_by_email, ''),
           requested_at = COALESCE(requested_at, NOW()),
           approvals = CASE
             WHEN jsonb_typeof(approvals) = 'array' THEN approvals
             ELSE '[]'::jsonb
           END,
           rejected_by_user_id = NULLIF(rejected_by_user_id, ''),
           rejected_by_email = NULLIF(rejected_by_email, ''),
           rejected_reason = NULLIF(rejected_reason, ''),
           updated_at = COALESCE(updated_at, requested_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR budget_id IS NULL
          OR budget_id = ''
          OR status IS NULL
          OR status NOT IN ('pending', 'rejected', 'executed')
          OR requested_by_user_id IS NULL
          OR requested_by_user_id = ''
          OR requested_at IS NULL
          OR approvals IS NULL
          OR jsonb_typeof(approvals) <> 'array'
          OR updated_at IS NULL`
    );

    await pool.query(
      `WITH ranked AS (
         SELECT ctid,
                ROW_NUMBER() OVER (
                  PARTITION BY tenant_id, budget_id
                  ORDER BY requested_at DESC, updated_at DESC, ctid DESC
                ) AS row_index
         FROM budget_release_requests
         WHERE status = 'pending'
       )
       UPDATE budget_release_requests AS requests
       SET status = 'rejected',
           rejected_reason = COALESCE(
             NULLIF(requests.rejected_reason, ''),
             '系统自动驳回：同 tenant + budget 仅允许一个 pending 申请。'
           ),
           rejected_at = COALESCE(requests.rejected_at, requests.updated_at, NOW()),
           updated_at = COALESCE(requests.updated_at, NOW())
       FROM ranked
       WHERE requests.ctid = ranked.ctid
         AND ranked.row_index > 1`
    );

    await pool.query(
      `UPDATE budgets AS budgets
       SET governance_state = 'pending_release',
           enabled = FALSE,
           updated_at = COALESCE(budgets.updated_at, NOW())
       WHERE EXISTS (
         SELECT 1
         FROM budget_release_requests AS requests
         WHERE requests.tenant_id = budgets.tenant_id
           AND requests.budget_id = budgets.id
           AND requests.status = 'pending'
       )
         AND (
           budgets.governance_state <> 'pending_release'
           OR budgets.enabled IS DISTINCT FROM FALSE
         )`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_budget_release_requests_tenant_budget_updated_at
       ON budget_release_requests (tenant_id, budget_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_budget_release_requests_tenant_status_updated_at
       ON budget_release_requests (tenant_id, status, updated_at DESC)`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_budget_release_requests_tenant_budget_pending_unique
       ON budget_release_requests (tenant_id, budget_id)
       WHERE status = 'pending'`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS integration_alert_callbacks (
         tenant_id TEXT NOT NULL DEFAULT 'default',
         callback_id TEXT NOT NULL,
         action TEXT NOT NULL DEFAULT 'ack',
         response_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
         processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         PRIMARY KEY (tenant_id, callback_id)
       )`
    );

    await pool.query(
      `ALTER TABLE integration_alert_callbacks
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS callback_id TEXT,
         ADD COLUMN IF NOT EXISTS action TEXT,
         ADD COLUMN IF NOT EXISTS response_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ`
    );

    await pool.query(
      `UPDATE integration_alert_callbacks
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), 'default'),
           callback_id = COALESCE(
             NULLIF(callback_id, ''),
             md5(random()::text || clock_timestamp()::text)
           ),
           action = CASE
             WHEN action IN ('ack', 'resolve', 'request_release', 'approve_release', 'reject_release')
               THEN action
             ELSE 'ack'
           END,
           response_payload = CASE
             WHEN response_payload IS NULL THEN '{}'::jsonb
             ELSE response_payload
           END,
           processed_at = COALESCE(processed_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR callback_id IS NULL
          OR callback_id = ''
          OR action IS NULL
          OR action NOT IN ('ack', 'resolve', 'request_release', 'approve_release', 'reject_release')
          OR response_payload IS NULL
          OR processed_at IS NULL`
    );

    await pool.query(
      `ALTER TABLE integration_alert_callbacks
         ALTER COLUMN tenant_id SET DEFAULT 'default',
         ALTER COLUMN tenant_id SET NOT NULL,
         ALTER COLUMN callback_id SET NOT NULL,
         ALTER COLUMN action SET DEFAULT 'ack',
         ALTER COLUMN action SET NOT NULL,
         ALTER COLUMN response_payload SET DEFAULT '{}'::jsonb,
         ALTER COLUMN response_payload SET NOT NULL,
         ALTER COLUMN processed_at SET DEFAULT NOW(),
         ALTER COLUMN processed_at SET NOT NULL`
    );

    await pool.query(
      `WITH ranked AS (
         SELECT ctid,
                ROW_NUMBER() OVER (
                  PARTITION BY tenant_id, callback_id
                  ORDER BY processed_at DESC, ctid DESC
                ) AS row_index
         FROM integration_alert_callbacks
       )
       DELETE FROM integration_alert_callbacks AS callbacks
       USING ranked
       WHERE callbacks.ctid = ranked.ctid
         AND ranked.row_index > 1`
    );

    await pool.query(
      `DO $$
       DECLARE
         constraint_name TEXT;
       BEGIN
         IF to_regclass('public.integration_alert_callbacks') IS NULL THEN
           RETURN;
         END IF;

         FOR constraint_name IN
           SELECT constraints.conname
           FROM pg_constraint AS constraints
           WHERE constraints.conrelid = 'public.integration_alert_callbacks'::regclass
             AND constraints.contype IN ('p', 'u')
             AND ARRAY(
               SELECT attrs.attname
               FROM unnest(constraints.conkey) WITH ORDINALITY AS keys(attnum, ord)
               JOIN pg_attribute AS attrs
                 ON attrs.attrelid = constraints.conrelid
                AND attrs.attnum = keys.attnum
               ORDER BY keys.ord
             ) = ARRAY['callback_id']
         LOOP
           EXECUTE format(
             'ALTER TABLE integration_alert_callbacks DROP CONSTRAINT %I',
             constraint_name
           );
         END LOOP;
       END
       $$`
    );

    await pool.query(
      `DO $$
       DECLARE
         index_name TEXT;
       BEGIN
         IF to_regclass('public.integration_alert_callbacks') IS NULL THEN
           RETURN;
         END IF;

         FOR index_name IN
           SELECT index_class.relname
           FROM pg_index AS index_meta
           JOIN pg_class AS table_class
             ON table_class.oid = index_meta.indrelid
           JOIN pg_namespace AS table_namespace
             ON table_namespace.oid = table_class.relnamespace
           JOIN pg_class AS index_class
             ON index_class.oid = index_meta.indexrelid
           LEFT JOIN pg_constraint AS constraints
             ON constraints.conindid = index_meta.indexrelid
           WHERE table_namespace.nspname = 'public'
             AND table_class.relname = 'integration_alert_callbacks'
             AND index_meta.indisunique = TRUE
             AND index_meta.indisprimary = FALSE
             AND constraints.oid IS NULL
             AND index_meta.indnkeyatts = 1
             AND pg_get_indexdef(index_meta.indexrelid) ILIKE '%(callback_id)%'
         LOOP
           EXECUTE format('DROP INDEX IF EXISTS %I.%I', 'public', index_name);
         END LOOP;
       END
       $$`
    );

    await pool.query(
      `DO $$
       BEGIN
         IF to_regclass('public.integration_alert_callbacks') IS NOT NULL
           AND NOT EXISTS (
             SELECT 1
             FROM pg_constraint
             WHERE conrelid = 'public.integration_alert_callbacks'::regclass
               AND contype = 'p'
           ) THEN
           ALTER TABLE integration_alert_callbacks
             ADD CONSTRAINT integration_alert_callbacks_pkey
             PRIMARY KEY (tenant_id, callback_id);
         END IF;
       END
       $$`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_integration_alert_callbacks_tenant_callback_id
       ON integration_alert_callbacks (tenant_id, callback_id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_integration_alert_callbacks_tenant_processed_at
       ON integration_alert_callbacks (tenant_id, processed_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS audit_logs (
         id TEXT PRIMARY KEY,
         event_id TEXT,
         action TEXT NOT NULL,
         level TEXT NOT NULL,
         detail TEXT,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE audit_logs
         ADD COLUMN IF NOT EXISTS event_id TEXT,
         ADD COLUMN IF NOT EXISTS action TEXT,
         ADD COLUMN IF NOT EXISTS level TEXT,
         ADD COLUMN IF NOT EXISTS detail TEXT,
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE audit_logs
       SET action = COALESCE(NULLIF(action, ''), 'unknown'),
           level = CASE
             WHEN level IN ('info', 'warning', 'error', 'critical') THEN level
             ELSE 'info'
           END,
           detail = COALESCE(detail, ''),
           tenant_id = COALESCE(
             NULLIF(tenant_id, ''),
             NULLIF((COALESCE(metadata, '{}'::jsonb)->>'tenant_id'), ''),
             NULLIF((COALESCE(metadata, '{}'::jsonb)->>'tenantId'), ''),
             '${DEFAULT_TENANT_ID}'
           ),
           metadata = COALESCE(metadata, '{}'::jsonb),
           created_at = COALESCE(created_at, NOW())
       WHERE action IS NULL
          OR action = ''
          OR level IS NULL
         OR level NOT IN ('info', 'warning', 'error', 'critical')
          OR detail IS NULL
          OR tenant_id IS NULL
          OR tenant_id = ''
          OR metadata IS NULL
          OR created_at IS NULL`
    );

    await pool.query(
      `ALTER TABLE audit_logs
         ALTER COLUMN tenant_id SET DEFAULT '${DEFAULT_TENANT_ID}'`
    );

    await pool.query(
      `ALTER TABLE audit_logs
         ALTER COLUMN tenant_id SET NOT NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at
       ON audit_logs (created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_audit_logs_event_id
       ON audit_logs (event_id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_audit_logs_action_level_created_at
       ON audit_logs (action, level, created_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS tenants (
         id TEXT PRIMARY KEY,
         name TEXT NOT NULL,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE tenants
         ADD COLUMN IF NOT EXISTS name TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE tenants
       SET name = COALESCE(NULLIF(name, ''), id),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE name IS NULL
          OR name = ''
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `INSERT INTO tenants (id, name, created_at, updated_at)
       VALUES ($1, $2, NOW(), NOW())
       ON CONFLICT (id) DO NOTHING`,
      [DEFAULT_TENANT_ID, DEFAULT_TENANT_NAME]
    );

    await pool.query(
      `UPDATE sources
       SET tenant_id = COALESCE(
             NULLIF(tenant_id, ''),
             NULLIF((COALESCE(metadata, '{}'::jsonb)->>'tenant_id'), ''),
             NULLIF((COALESCE(metadata, '{}'::jsonb)->>'tenantId'), ''),
             $1
           ),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR updated_at IS NULL`,
      [DEFAULT_TENANT_ID]
    );

    await pool.query(
      `INSERT INTO tenants (id, name, created_at, updated_at)
       SELECT DISTINCT src.tenant_id, src.tenant_id, NOW(), NOW()
       FROM sources AS src
       LEFT JOIN tenants AS t ON t.id = src.tenant_id
       WHERE src.tenant_id IS NOT NULL
         AND src.tenant_id <> ''
         AND t.id IS NULL
       ON CONFLICT (id) DO NOTHING`
    );

    await pool.query(
      `INSERT INTO tenants (id, name, created_at, updated_at)
       SELECT DISTINCT audit.tenant_id, audit.tenant_id, NOW(), NOW()
       FROM audit_logs AS audit
       LEFT JOIN tenants AS t ON t.id = audit.tenant_id
       WHERE audit.tenant_id IS NOT NULL
         AND audit.tenant_id <> ''
         AND t.id IS NULL
       ON CONFLICT (id) DO NOTHING`
    );

    await pool.query(
      `UPDATE sources
       SET metadata = jsonb_set(
             jsonb_set(COALESCE(metadata, '{}'::jsonb), '{tenant_id}', to_jsonb(tenant_id), true),
             '{tenantId}', to_jsonb(tenant_id), true
           ),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE metadata IS NULL
          OR COALESCE(metadata->>'tenant_id', '') <> tenant_id
          OR COALESCE(metadata->>'tenantId', '') <> tenant_id`
    );

    await pool.query(
      `ALTER TABLE sources
         ALTER COLUMN tenant_id SET DEFAULT '${DEFAULT_TENANT_ID}'`
    );

    await pool.query(
      `ALTER TABLE sources
         ALTER COLUMN tenant_id SET NOT NULL`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_sources_tenant_created_at
       ON sources (tenant_id, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_audit_logs_tenant_created_at
       ON audit_logs (tenant_id, created_at DESC)`
    );

    await pool.query(
      `DO $$
       BEGIN
         IF NOT EXISTS (
           SELECT 1
           FROM pg_constraint
           WHERE conname = 'fk_sources_tenant'
             AND conrelid = 'sources'::regclass
         ) THEN
           ALTER TABLE sources
             ADD CONSTRAINT fk_sources_tenant
             FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE RESTRICT;
         END IF;
       END
       $$`
    );

    await pool.query(
      `DO $$
       BEGIN
         IF NOT EXISTS (
           SELECT 1
           FROM pg_constraint
           WHERE conname = 'fk_audit_logs_tenant'
             AND conrelid = 'audit_logs'::regclass
         ) THEN
           ALTER TABLE audit_logs
             ADD CONSTRAINT fk_audit_logs_tenant
             FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE RESTRICT;
         END IF;
       END
       $$`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_tenants_updated_at
       ON tenants (updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS users (
         id TEXT PRIMARY KEY,
         email TEXT NOT NULL,
         password_hash TEXT NOT NULL,
         display_name TEXT NOT NULL DEFAULT '',
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE users
         ADD COLUMN IF NOT EXISTS email TEXT,
         ADD COLUMN IF NOT EXISTS password_hash TEXT,
         ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT '',
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE users
       SET email = LOWER(COALESCE(NULLIF(email, ''), id || '@local.invalid')),
           password_hash = COALESCE(NULLIF(password_hash, ''), '*'),
           display_name = COALESCE(NULLIF(display_name, ''), COALESCE(NULLIF(email, ''), id)),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE email IS NULL
          OR email = ''
          OR password_hash IS NULL
          OR password_hash = ''
          OR display_name IS NULL
          OR display_name = ''
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique
       ON users (email)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_users_updated_at
       ON users (updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS organizations (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
         name TEXT NOT NULL,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE organizations
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS name TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE organizations
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), $1),
           name = COALESCE(NULLIF(name, ''), id),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR name IS NULL
          OR name = ''
          OR created_at IS NULL
          OR updated_at IS NULL`,
      [DEFAULT_TENANT_ID]
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_organizations_tenant_name
       ON organizations (tenant_id, name)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_organizations_tenant_updated_at
       ON organizations (tenant_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS tenant_memberships (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
         user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
         role TEXT NOT NULL DEFAULT 'member',
         organization_id TEXT REFERENCES organizations(id) ON DELETE SET NULL,
         org_role TEXT NOT NULL DEFAULT 'member',
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE tenant_memberships
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS user_id TEXT,
         ADD COLUMN IF NOT EXISTS role TEXT,
         ADD COLUMN IF NOT EXISTS organization_id TEXT,
         ADD COLUMN IF NOT EXISTS org_role TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `ALTER TABLE tenant_memberships
         ALTER COLUMN role SET DEFAULT 'member',
         ALTER COLUMN org_role SET DEFAULT 'member'`
    );

    await pool.query(
      `UPDATE tenant_memberships
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), $1),
           role = COALESCE(NULLIF(role, ''), 'member'),
           org_role = COALESCE(NULLIF(org_role, ''), 'member'),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR role IS NULL
          OR role = ''
          OR org_role IS NULL
          OR org_role = ''
          OR created_at IS NULL
          OR updated_at IS NULL`,
      [DEFAULT_TENANT_ID]
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_tenant_memberships_uni
       ON tenant_memberships (tenant_id, user_id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_tenant_memberships_user_updated_at
       ON tenant_memberships (user_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS auth_sessions (
         id TEXT PRIMARY KEY,
         user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
         tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
         session_token TEXT NOT NULL,
         expires_at TIMESTAMPTZ NOT NULL,
         revoked_at TIMESTAMPTZ,
         replaced_by_session_id TEXT REFERENCES auth_sessions(id) ON DELETE SET NULL,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE auth_sessions
         ADD COLUMN IF NOT EXISTS user_id TEXT,
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS session_token TEXT,
         ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS replaced_by_session_id TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE auth_sessions
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), $1),
           expires_at = COALESCE(expires_at, created_at, NOW()),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR expires_at IS NULL
          OR created_at IS NULL
          OR updated_at IS NULL`,
      [DEFAULT_TENANT_ID]
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_sessions_token_unique
       ON auth_sessions (session_token)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_auth_sessions_user_tenant_created_at
       ON auth_sessions (user_id, tenant_id, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_auth_sessions_tenant_updated_at
       ON auth_sessions (tenant_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS identity_device_bindings (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
         device_id TEXT NOT NULL,
         display_name TEXT NOT NULL DEFAULT '',
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE identity_device_bindings
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS device_id TEXT,
         ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT '',
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE identity_device_bindings
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), $1),
           device_id = COALESCE(NULLIF(device_id, ''), id),
           display_name = COALESCE(display_name, ''),
           metadata = COALESCE(metadata, '{}'::jsonb),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR device_id IS NULL
          OR device_id = ''
          OR display_name IS NULL
          OR metadata IS NULL
          OR created_at IS NULL
          OR updated_at IS NULL`,
      [DEFAULT_TENANT_ID]
    );

    await pool.query(
      `ALTER TABLE identity_device_bindings
         ALTER COLUMN tenant_id SET DEFAULT '${DEFAULT_TENANT_ID}',
         ALTER COLUMN tenant_id SET NOT NULL,
         ALTER COLUMN device_id SET NOT NULL,
         ALTER COLUMN display_name SET DEFAULT '',
         ALTER COLUMN display_name SET NOT NULL,
         ALTER COLUMN metadata SET DEFAULT '{}'::jsonb,
         ALTER COLUMN metadata SET NOT NULL,
         ALTER COLUMN created_at SET DEFAULT NOW(),
         ALTER COLUMN created_at SET NOT NULL,
         ALTER COLUMN updated_at SET DEFAULT NOW(),
         ALTER COLUMN updated_at SET NOT NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_identity_device_bindings_tenant_device_uni
       ON identity_device_bindings (tenant_id, device_id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_identity_device_bindings_tenant_updated_at
       ON identity_device_bindings (tenant_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS identity_agent_bindings (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
         agent_id TEXT NOT NULL,
         device_id TEXT,
         display_name TEXT NOT NULL DEFAULT '',
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE identity_agent_bindings
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS agent_id TEXT,
         ADD COLUMN IF NOT EXISTS device_id TEXT,
         ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT '',
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE identity_agent_bindings
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), $1),
           agent_id = COALESCE(NULLIF(agent_id, ''), id),
           device_id = NULLIF(device_id, ''),
           display_name = COALESCE(display_name, ''),
           metadata = COALESCE(metadata, '{}'::jsonb),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR agent_id IS NULL
          OR agent_id = ''
          OR display_name IS NULL
          OR metadata IS NULL
          OR created_at IS NULL
          OR updated_at IS NULL`,
      [DEFAULT_TENANT_ID]
    );

    await pool.query(
      `ALTER TABLE identity_agent_bindings
         ALTER COLUMN tenant_id SET DEFAULT '${DEFAULT_TENANT_ID}',
         ALTER COLUMN tenant_id SET NOT NULL,
         ALTER COLUMN agent_id SET NOT NULL,
         ALTER COLUMN display_name SET DEFAULT '',
         ALTER COLUMN display_name SET NOT NULL,
         ALTER COLUMN metadata SET DEFAULT '{}'::jsonb,
         ALTER COLUMN metadata SET NOT NULL,
         ALTER COLUMN created_at SET DEFAULT NOW(),
         ALTER COLUMN created_at SET NOT NULL,
         ALTER COLUMN updated_at SET DEFAULT NOW(),
         ALTER COLUMN updated_at SET NOT NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_identity_agent_bindings_tenant_agent_uni
       ON identity_agent_bindings (tenant_id, agent_id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_identity_agent_bindings_tenant_device_updated_at
       ON identity_agent_bindings (tenant_id, device_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS identity_source_bindings (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
         source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
         device_id TEXT,
         agent_id TEXT,
         binding_type TEXT NOT NULL DEFAULT 'ssh-pull',
         access_mode TEXT NOT NULL DEFAULT 'realtime',
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE identity_source_bindings
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS source_id TEXT,
         ADD COLUMN IF NOT EXISTS device_id TEXT,
         ADD COLUMN IF NOT EXISTS agent_id TEXT,
         ADD COLUMN IF NOT EXISTS binding_type TEXT NOT NULL DEFAULT 'ssh-pull',
         ADD COLUMN IF NOT EXISTS access_mode TEXT NOT NULL DEFAULT 'realtime',
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE identity_source_bindings
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), $1),
           source_id = COALESCE(NULLIF(source_id, ''), id),
           device_id = NULLIF(device_id, ''),
           agent_id = NULLIF(agent_id, ''),
           binding_type = CASE
             WHEN NULLIF(binding_type, '') IS NULL THEN 'ssh-pull'
             WHEN binding_type = 'manual' THEN 'ssh-pull'
             ELSE binding_type
           END,
           access_mode = COALESCE(NULLIF(access_mode, ''), 'realtime'),
           metadata = COALESCE(metadata, '{}'::jsonb),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR source_id IS NULL
          OR source_id = ''
          OR binding_type IS NULL
          OR binding_type = ''
          OR access_mode IS NULL
          OR access_mode = ''
          OR metadata IS NULL
          OR created_at IS NULL
          OR updated_at IS NULL`,
      [DEFAULT_TENANT_ID]
    );

    await pool.query(
      `ALTER TABLE identity_source_bindings
         ALTER COLUMN tenant_id SET DEFAULT '${DEFAULT_TENANT_ID}',
         ALTER COLUMN tenant_id SET NOT NULL,
         ALTER COLUMN source_id SET NOT NULL,
         ALTER COLUMN binding_type SET DEFAULT 'ssh-pull',
         ALTER COLUMN binding_type SET NOT NULL,
         ALTER COLUMN access_mode SET DEFAULT 'realtime',
         ALTER COLUMN access_mode SET NOT NULL,
         ALTER COLUMN metadata SET DEFAULT '{}'::jsonb,
         ALTER COLUMN metadata SET NOT NULL,
         ALTER COLUMN created_at SET DEFAULT NOW(),
         ALTER COLUMN created_at SET NOT NULL,
         ALTER COLUMN updated_at SET DEFAULT NOW(),
         ALTER COLUMN updated_at SET NOT NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_identity_source_bindings_tenant_source_uni
       ON identity_source_bindings (tenant_id, source_id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_identity_source_bindings_tenant_agent_updated_at
       ON identity_source_bindings (tenant_id, agent_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_identity_source_bindings_tenant_device_updated_at
       ON identity_source_bindings (tenant_id, device_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS api_keys (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         name TEXT NOT NULL DEFAULT '',
         key_hash TEXT NOT NULL,
         scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
         last_used_at TIMESTAMPTZ,
         revoked_at TIMESTAMPTZ,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE api_keys
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS name TEXT,
         ADD COLUMN IF NOT EXISTS key_hash TEXT,
         ADD COLUMN IF NOT EXISTS scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
         ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE api_keys
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           name = COALESCE(NULLIF(name, ''), id),
           key_hash = COALESCE(
             NULLIF(key_hash, ''),
             md5(COALESCE(NULLIF(id, ''), random()::text) || ':' || COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'))
           ),
           scopes = CASE
             WHEN jsonb_typeof(scopes) = 'array' THEN scopes
             ELSE '[]'::jsonb
           END,
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR name IS NULL
          OR name = ''
          OR key_hash IS NULL
          OR key_hash = ''
          OR scopes IS NULL
          OR jsonb_typeof(scopes) <> 'array'
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_tenant_id_unique
       ON api_keys (tenant_id, id)`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_tenant_hash_unique
       ON api_keys (tenant_id, key_hash)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_created_at
       ON api_keys (tenant_id, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_revoked_at_updated_at
       ON api_keys (tenant_id, revoked_at, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS webhook_endpoints (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         name TEXT NOT NULL DEFAULT '',
         url TEXT NOT NULL DEFAULT '',
         enabled BOOLEAN NOT NULL DEFAULT TRUE,
         event_types JSONB NOT NULL DEFAULT '[]'::jsonb,
         secret_hash TEXT,
         secret_ciphertext TEXT,
         headers JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE webhook_endpoints
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS name TEXT,
         ADD COLUMN IF NOT EXISTS url TEXT,
         ADD COLUMN IF NOT EXISTS enabled BOOLEAN,
         ADD COLUMN IF NOT EXISTS event_types JSONB NOT NULL DEFAULT '[]'::jsonb,
         ADD COLUMN IF NOT EXISTS secret_hash TEXT,
         ADD COLUMN IF NOT EXISTS secret_ciphertext TEXT,
         ADD COLUMN IF NOT EXISTS headers JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE webhook_endpoints
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           name = COALESCE(NULLIF(name, ''), id),
           url = COALESCE(NULLIF(url, ''), 'http://localhost'),
           enabled = COALESCE(enabled, TRUE),
           event_types = CASE
             WHEN jsonb_typeof(event_types) = 'array' THEN event_types
             ELSE '[]'::jsonb
           END,
           secret_hash = NULLIF(secret_hash, ''),
           secret_ciphertext = NULLIF(secret_ciphertext, ''),
           headers = CASE
             WHEN jsonb_typeof(headers) = 'object' THEN headers
             ELSE '{}'::jsonb
           END,
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR name IS NULL
          OR name = ''
          OR url IS NULL
          OR url = ''
          OR enabled IS NULL
          OR event_types IS NULL
          OR jsonb_typeof(event_types) <> 'array'
          OR secret_hash = ''
          OR secret_ciphertext = ''
          OR headers IS NULL
          OR jsonb_typeof(headers) <> 'object'
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_endpoints_tenant_id_unique
       ON webhook_endpoints (tenant_id, id)`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_endpoints_tenant_name_unique
       ON webhook_endpoints (tenant_id, name)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_webhook_endpoints_tenant_enabled_updated_at
       ON webhook_endpoints (tenant_id, enabled, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS webhook_replay_tasks (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         webhook_id TEXT NOT NULL DEFAULT '',
         status TEXT NOT NULL DEFAULT 'queued',
         dry_run BOOLEAN NOT NULL DEFAULT TRUE,
         filters JSONB NOT NULL DEFAULT '{}'::jsonb,
         result JSONB NOT NULL DEFAULT '{}'::jsonb,
         error TEXT,
         requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         started_at TIMESTAMPTZ,
         finished_at TIMESTAMPTZ,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE webhook_replay_tasks
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS webhook_id TEXT,
         ADD COLUMN IF NOT EXISTS status TEXT,
         ADD COLUMN IF NOT EXISTS dry_run BOOLEAN,
         ADD COLUMN IF NOT EXISTS filters JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS result JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS error TEXT,
         ADD COLUMN IF NOT EXISTS requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE webhook_replay_tasks
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           webhook_id = COALESCE(NULLIF(webhook_id, ''), 'unknown-webhook'),
           status = CASE
             WHEN status IN ('queued', 'running', 'completed', 'failed') THEN status
             WHEN status IN ('success', 'succeeded') THEN 'completed'
             WHEN status IN ('canceled', 'cancelled') THEN 'failed'
             ELSE 'queued'
           END,
           dry_run = COALESCE(dry_run, TRUE),
           filters = CASE
             WHEN jsonb_typeof(filters) = 'object' THEN filters
             ELSE '{}'::jsonb
           END,
           result = CASE
             WHEN jsonb_typeof(result) = 'object' THEN result
             ELSE '{}'::jsonb
           END,
           error = NULLIF(error, ''),
           requested_at = COALESCE(requested_at, created_at, NOW()),
           started_at = started_at,
           finished_at = finished_at,
           created_at = COALESCE(created_at, requested_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, requested_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR webhook_id IS NULL
          OR webhook_id = ''
          OR status IS NULL
          OR status NOT IN ('queued', 'running', 'completed', 'failed', 'success', 'succeeded', 'canceled', 'cancelled')
          OR dry_run IS NULL
          OR filters IS NULL
          OR jsonb_typeof(filters) <> 'object'
          OR result IS NULL
          OR jsonb_typeof(result) <> 'object'
          OR error = ''
          OR requested_at IS NULL
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_replay_tasks_tenant_id_unique
       ON webhook_replay_tasks (tenant_id, id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_webhook_replay_tasks_tenant_webhook_requested_at
       ON webhook_replay_tasks (tenant_id, webhook_id, requested_at DESC, id DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_webhook_replay_tasks_tenant_status_updated_at
       ON webhook_replay_tasks (tenant_id, status, updated_at DESC, id DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS quality_events (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         scorecard_key TEXT NOT NULL DEFAULT 'default',
         metric_key TEXT,
         provider TEXT,
         repository TEXT,
         workflow TEXT,
         run_id TEXT,
         score NUMERIC(10, 6) NOT NULL DEFAULT 0,
         passed BOOLEAN NOT NULL DEFAULT FALSE,
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE quality_events
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS scorecard_key TEXT,
         ADD COLUMN IF NOT EXISTS metric_key TEXT,
         ADD COLUMN IF NOT EXISTS provider TEXT,
         ADD COLUMN IF NOT EXISTS repository TEXT,
         ADD COLUMN IF NOT EXISTS workflow TEXT,
         ADD COLUMN IF NOT EXISTS run_id TEXT,
         ADD COLUMN IF NOT EXISTS score NUMERIC(10, 6),
         ADD COLUMN IF NOT EXISTS passed BOOLEAN,
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE quality_events
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           scorecard_key = COALESCE(NULLIF(scorecard_key, ''), 'default'),
           metric_key = NULLIF(metric_key, ''),
           provider = NULLIF(
             LOWER(
               COALESCE(
                 NULLIF(provider, ''),
                 NULLIF(metadata->'externalSource'->>'provider', ''),
                 NULLIF(metadata->>'provider', '')
               )
             ),
             ''
           ),
           repository = NULLIF(
             LOWER(
               COALESCE(
                 NULLIF(repository, ''),
                 NULLIF(metadata->'externalSource'->>'repo', ''),
                 NULLIF(metadata->>'repo', '')
               )
             ),
             ''
           ),
           workflow = NULLIF(
             COALESCE(
               NULLIF(workflow, ''),
               NULLIF(metadata->'externalSource'->>'workflow', ''),
               NULLIF(metadata->>'workflow', '')
             ),
             ''
           ),
           run_id = NULLIF(
             COALESCE(
               NULLIF(run_id, ''),
               NULLIF(metadata->'externalSource'->>'runId', ''),
               NULLIF(metadata->>'runId', ''),
               NULLIF(metadata->>'run_id', '')
             ),
             ''
           ),
           score = GREATEST(COALESCE(score, 0), 0),
           passed = COALESCE(passed, FALSE),
           metadata = COALESCE(metadata, '{}'::jsonb),
           created_at = COALESCE(created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR scorecard_key IS NULL
          OR scorecard_key = ''
          OR metric_key = ''
          OR provider = ''
          OR repository = ''
          OR workflow = ''
          OR run_id = ''
          OR (
            provider IS NULL
            AND (
              NULLIF(metadata->'externalSource'->>'provider', '') IS NOT NULL
              OR NULLIF(metadata->>'provider', '') IS NOT NULL
            )
          )
          OR (
            repository IS NULL
            AND (
              NULLIF(metadata->'externalSource'->>'repo', '') IS NOT NULL
              OR NULLIF(metadata->>'repo', '') IS NOT NULL
            )
          )
          OR (
            workflow IS NULL
            AND (
              NULLIF(metadata->'externalSource'->>'workflow', '') IS NOT NULL
              OR NULLIF(metadata->>'workflow', '') IS NOT NULL
            )
          )
          OR (
            run_id IS NULL
            AND (
              NULLIF(metadata->'externalSource'->>'runId', '') IS NOT NULL
              OR NULLIF(metadata->>'runId', '') IS NOT NULL
              OR NULLIF(metadata->>'run_id', '') IS NOT NULL
            )
          )
          OR score IS NULL
          OR score < 0
          OR passed IS NULL
          OR metadata IS NULL
          OR created_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_quality_events_tenant_id_unique
       ON quality_events (tenant_id, id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_quality_events_tenant_created_at
       ON quality_events (tenant_id, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_quality_events_tenant_scorecard_created_at
       ON quality_events (tenant_id, scorecard_key, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_quality_events_tenant_external_created_at
       ON quality_events (
         tenant_id,
         provider,
         repository,
         workflow,
         run_id,
         created_at DESC
       )`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS quality_scorecards (
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         scorecard_key TEXT NOT NULL,
         title TEXT NOT NULL DEFAULT '',
         description TEXT,
         score NUMERIC(10, 6) NOT NULL DEFAULT 0,
         dimensions JSONB NOT NULL DEFAULT '{}'::jsonb,
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         PRIMARY KEY (tenant_id, scorecard_key)
       )`
    );

    await pool.query(
      `ALTER TABLE quality_scorecards
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS scorecard_key TEXT,
         ADD COLUMN IF NOT EXISTS title TEXT,
         ADD COLUMN IF NOT EXISTS description TEXT,
         ADD COLUMN IF NOT EXISTS score NUMERIC(10, 6),
         ADD COLUMN IF NOT EXISTS dimensions JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE quality_scorecards
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           scorecard_key = COALESCE(NULLIF(scorecard_key, ''), 'default'),
           title = COALESCE(NULLIF(title, ''), scorecard_key),
           description = NULLIF(description, ''),
           score = GREATEST(COALESCE(score, 0), 0),
           dimensions = CASE
             WHEN jsonb_typeof(dimensions) = 'object' THEN dimensions
             ELSE '{}'::jsonb
           END,
           metadata = CASE
             WHEN jsonb_typeof(metadata) = 'object' THEN metadata
             ELSE '{}'::jsonb
           END,
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR scorecard_key IS NULL
          OR scorecard_key = ''
          OR title IS NULL
          OR title = ''
          OR description = ''
          OR score IS NULL
          OR score < 0
          OR dimensions IS NULL
          OR jsonb_typeof(dimensions) <> 'object'
          OR metadata IS NULL
          OR jsonb_typeof(metadata) <> 'object'
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_quality_scorecards_tenant_key_unique
       ON quality_scorecards (tenant_id, scorecard_key)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_quality_scorecards_tenant_updated_at
       ON quality_scorecards (tenant_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS replay_baselines (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         name TEXT NOT NULL DEFAULT '',
         description TEXT,
         dataset_ref TEXT,
         scenario_count INTEGER NOT NULL DEFAULT 0,
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE replay_baselines
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS name TEXT,
         ADD COLUMN IF NOT EXISTS description TEXT,
         ADD COLUMN IF NOT EXISTS dataset_ref TEXT,
         ADD COLUMN IF NOT EXISTS scenario_count INTEGER,
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE replay_baselines
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           name = COALESCE(NULLIF(name, ''), id),
           description = NULLIF(description, ''),
           dataset_ref = NULLIF(dataset_ref, ''),
           scenario_count = GREATEST(COALESCE(scenario_count, 0), 0),
           metadata = CASE
             WHEN jsonb_typeof(metadata) = 'object' THEN metadata
             ELSE '{}'::jsonb
           END,
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR name IS NULL
          OR name = ''
          OR description = ''
          OR dataset_ref = ''
          OR scenario_count IS NULL
          OR scenario_count < 0
          OR metadata IS NULL
          OR jsonb_typeof(metadata) <> 'object'
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_baselines_tenant_id_unique
       ON replay_baselines (tenant_id, id)`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_baselines_tenant_name_unique
       ON replay_baselines (tenant_id, name)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_replay_baselines_tenant_updated_at
       ON replay_baselines (tenant_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS replay_jobs (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         baseline_id TEXT NOT NULL,
         status TEXT NOT NULL DEFAULT 'pending',
         parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
         summary_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
         diff_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
         error TEXT,
         started_at TIMESTAMPTZ,
         finished_at TIMESTAMPTZ,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE replay_jobs
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS baseline_id TEXT,
         ADD COLUMN IF NOT EXISTS status TEXT,
         ADD COLUMN IF NOT EXISTS parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS summary_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS diff_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS error TEXT,
         ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE replay_jobs
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           baseline_id = COALESCE(NULLIF(baseline_id, ''), id),
           status = CASE
             WHEN status IN ('pending', 'running', 'completed', 'failed', 'cancelled') THEN status
             WHEN status IN ('succeeded', 'success') THEN 'completed'
             WHEN status = 'canceled' THEN 'cancelled'
             ELSE 'pending'
           END,
           parameters = CASE
             WHEN jsonb_typeof(parameters) = 'object' THEN parameters
             ELSE '{}'::jsonb
           END,
           summary_payload = CASE
             WHEN jsonb_typeof(summary_payload) = 'object' THEN summary_payload
             ELSE '{}'::jsonb
           END,
           diff_payload = CASE
             WHEN jsonb_typeof(diff_payload) = 'object' THEN diff_payload
             ELSE '{}'::jsonb
           END,
           error = NULLIF(error, ''),
           started_at = started_at,
           finished_at = finished_at,
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR baseline_id IS NULL
          OR baseline_id = ''
          OR status IS NULL
          OR status NOT IN ('pending', 'running', 'completed', 'failed', 'cancelled')
          OR parameters IS NULL
          OR jsonb_typeof(parameters) <> 'object'
          OR summary_payload IS NULL
          OR jsonb_typeof(summary_payload) <> 'object'
          OR diff_payload IS NULL
          OR jsonb_typeof(diff_payload) <> 'object'
          OR error = ''
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_jobs_tenant_id_unique
       ON replay_jobs (tenant_id, id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_replay_jobs_tenant_baseline_created_at
       ON replay_jobs (tenant_id, baseline_id, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_replay_jobs_tenant_status_updated_at
       ON replay_jobs (tenant_id, status, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS replay_datasets (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         name TEXT NOT NULL DEFAULT '',
         description TEXT,
         model TEXT NOT NULL DEFAULT 'unknown',
         prompt_version TEXT,
         external_dataset_id TEXT,
         case_count INTEGER NOT NULL DEFAULT 0,
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE replay_datasets
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS name TEXT,
         ADD COLUMN IF NOT EXISTS description TEXT,
         ADD COLUMN IF NOT EXISTS model TEXT NOT NULL DEFAULT 'unknown',
         ADD COLUMN IF NOT EXISTS prompt_version TEXT,
         ADD COLUMN IF NOT EXISTS external_dataset_id TEXT,
         ADD COLUMN IF NOT EXISTS case_count INTEGER NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE replay_datasets
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           name = COALESCE(NULLIF(name, ''), id),
           description = NULLIF(description, ''),
           model = COALESCE(NULLIF(model, ''), 'unknown'),
           prompt_version = NULLIF(prompt_version, ''),
           external_dataset_id = NULLIF(external_dataset_id, ''),
           case_count = GREATEST(COALESCE(case_count, 0), 0),
           metadata = CASE
             WHEN jsonb_typeof(metadata) = 'object' THEN metadata
             ELSE '{}'::jsonb
           END,
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR name IS NULL
          OR name = ''
          OR model IS NULL
          OR model = ''
          OR case_count IS NULL
          OR case_count < 0
          OR metadata IS NULL
          OR jsonb_typeof(metadata) <> 'object'
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `INSERT INTO replay_datasets (
         id,
         tenant_id,
         name,
         description,
         model,
         prompt_version,
         external_dataset_id,
         case_count,
         metadata,
         created_at,
         updated_at
       )
       SELECT id,
              tenant_id,
              name,
              description,
              COALESCE(NULLIF(metadata->>'model', ''), 'unknown'),
              NULLIF(metadata->>'promptVersion', ''),
              dataset_ref,
              scenario_count,
              metadata,
              created_at,
              updated_at
       FROM replay_baselines
       ON CONFLICT (id) DO NOTHING`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_datasets_tenant_id_unique
       ON replay_datasets (tenant_id, id)`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_datasets_tenant_name_unique
       ON replay_datasets (tenant_id, name)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_replay_datasets_tenant_updated_at
       ON replay_datasets (tenant_id, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS replay_dataset_cases (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         dataset_id TEXT NOT NULL,
         case_id TEXT NOT NULL,
         sort_order INTEGER NOT NULL DEFAULT 0,
         input_text TEXT NOT NULL DEFAULT '',
         expected_output TEXT,
         baseline_output TEXT,
         candidate_input TEXT,
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         checksum TEXT,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE replay_dataset_cases
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS dataset_id TEXT,
         ADD COLUMN IF NOT EXISTS case_id TEXT,
         ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS input_text TEXT NOT NULL DEFAULT '',
         ADD COLUMN IF NOT EXISTS expected_output TEXT,
         ADD COLUMN IF NOT EXISTS baseline_output TEXT,
         ADD COLUMN IF NOT EXISTS candidate_input TEXT,
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS checksum TEXT,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_dataset_cases_tenant_case_unique
       ON replay_dataset_cases (tenant_id, dataset_id, case_id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_replay_dataset_cases_tenant_sort
       ON replay_dataset_cases (tenant_id, dataset_id, sort_order ASC, case_id ASC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS replay_runs (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         dataset_id TEXT NOT NULL,
         status TEXT NOT NULL DEFAULT 'pending',
         parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
         summary_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
         diff_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
         error TEXT,
         started_at TIMESTAMPTZ,
         finished_at TIMESTAMPTZ,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE replay_runs
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS dataset_id TEXT,
         ADD COLUMN IF NOT EXISTS status TEXT,
         ADD COLUMN IF NOT EXISTS parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS summary_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS diff_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS error TEXT,
         ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `UPDATE replay_runs
       SET tenant_id = COALESCE(NULLIF(tenant_id, ''), '${DEFAULT_TENANT_ID}'),
           dataset_id = COALESCE(NULLIF(dataset_id, ''), id),
           status = CASE
             WHEN status IN ('pending', 'running', 'completed', 'failed', 'cancelled') THEN status
             WHEN status IN ('succeeded', 'success') THEN 'completed'
             WHEN status = 'canceled' THEN 'cancelled'
             ELSE 'pending'
           END,
           parameters = CASE
             WHEN jsonb_typeof(parameters) = 'object' THEN parameters
             ELSE '{}'::jsonb
           END,
           summary_payload = CASE
             WHEN jsonb_typeof(summary_payload) = 'object' THEN summary_payload
             ELSE '{}'::jsonb
           END,
           diff_payload = CASE
             WHEN jsonb_typeof(diff_payload) = 'object' THEN diff_payload
             ELSE '{}'::jsonb
           END,
           error = NULLIF(error, ''),
           created_at = COALESCE(created_at, NOW()),
           updated_at = COALESCE(updated_at, created_at, NOW())
       WHERE tenant_id IS NULL
          OR tenant_id = ''
          OR dataset_id IS NULL
          OR dataset_id = ''
          OR status IS NULL
          OR status NOT IN ('pending', 'running', 'completed', 'failed', 'cancelled')
          OR parameters IS NULL
          OR jsonb_typeof(parameters) <> 'object'
          OR summary_payload IS NULL
          OR jsonb_typeof(summary_payload) <> 'object'
          OR diff_payload IS NULL
          OR jsonb_typeof(diff_payload) <> 'object'
          OR created_at IS NULL
          OR updated_at IS NULL`
    );

    await pool.query(
      `INSERT INTO replay_runs (
         id,
         tenant_id,
         dataset_id,
         status,
         parameters,
         summary_payload,
         diff_payload,
         error,
         started_at,
         finished_at,
         created_at,
         updated_at
       )
       SELECT id,
              tenant_id,
              baseline_id,
              status,
              parameters,
              summary_payload,
              diff_payload,
              error,
              started_at,
              finished_at,
              created_at,
              updated_at
       FROM replay_jobs
       ON CONFLICT (id) DO NOTHING`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_runs_tenant_id_unique
       ON replay_runs (tenant_id, id)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_replay_runs_tenant_dataset_created_at
       ON replay_runs (tenant_id, dataset_id, created_at DESC)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_replay_runs_tenant_status_updated_at
       ON replay_runs (tenant_id, status, updated_at DESC)`
    );

    await pool.query(
      `CREATE TABLE IF NOT EXISTS replay_artifacts (
         id TEXT PRIMARY KEY,
         tenant_id TEXT NOT NULL DEFAULT '${DEFAULT_TENANT_ID}',
         run_id TEXT NOT NULL,
         dataset_id TEXT NOT NULL,
         artifact_type TEXT NOT NULL,
         name TEXT NOT NULL DEFAULT '',
         description TEXT,
         content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
         byte_size BIGINT NOT NULL DEFAULT 0,
         checksum TEXT,
         storage_backend TEXT NOT NULL DEFAULT 'local',
         storage_key TEXT NOT NULL DEFAULT '',
         metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )`
    );

    await pool.query(
      `ALTER TABLE replay_artifacts
         ADD COLUMN IF NOT EXISTS tenant_id TEXT,
         ADD COLUMN IF NOT EXISTS run_id TEXT,
         ADD COLUMN IF NOT EXISTS dataset_id TEXT,
         ADD COLUMN IF NOT EXISTS artifact_type TEXT,
         ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT '',
         ADD COLUMN IF NOT EXISTS description TEXT,
         ADD COLUMN IF NOT EXISTS content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
         ADD COLUMN IF NOT EXISTS byte_size BIGINT NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS checksum TEXT,
         ADD COLUMN IF NOT EXISTS storage_backend TEXT NOT NULL DEFAULT 'local',
         ADD COLUMN IF NOT EXISTS storage_key TEXT NOT NULL DEFAULT '',
         ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
         ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
    );

    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_artifacts_tenant_run_type_unique
       ON replay_artifacts (tenant_id, run_id, artifact_type)`
    );

    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_replay_artifacts_tenant_dataset_created_at
       ON replay_artifacts (tenant_id, dataset_id, created_at DESC)`
    );
  }

  private disableDb(error: unknown, reason: string): void {
    this.pool = null;

    if (this.loggedDbFallback) {
      return;
    }

    this.loggedDbFallback = true;
    console.warn(`[control-plane] PostgreSQL ${reason}，已降级到内存模式。`, error);
  }

  private resolveSourceTenantIdFromMemory(source: Source): string {
    return (
      firstNonEmptyString(this.memorySourceTenantById.get(source.id), resolveSourceTenantId(source)) ??
      DEFAULT_TENANT_ID
    );
  }

  private resolveSourceTenantIdBySourceIdFromMemory(sourceId: string): string {
    return (
      firstNonEmptyString(this.memorySourceTenantById.get(sourceId)) ?? DEFAULT_TENANT_ID
    );
  }

  private cloneApiKey(apiKey: ApiKey): ApiKey {
    return {
      ...apiKey,
      scopes: [...apiKey.scopes],
    };
  }

  private listApiKeysFromMemory(tenantId: string): ApiKey[] {
    return this.memoryApiKeys
      .filter((apiKey) => apiKey.tenantId === tenantId)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt) || b.id.localeCompare(a.id))
      .map((apiKey) => this.cloneApiKey(apiKey));
  }

  private createApiKeyToMemory(apiKey: ApiKey): ApiKey {
    const lookupKey = buildTenantScopedLookupKey(apiKey.tenantId, apiKey.keyHash);
    const existingByHash = this.memoryApiKeyByHash.get(lookupKey);
    if (existingByHash && existingByHash.apiKeyId !== apiKey.id) {
      throw new Error(`api_key_hash_already_exists:${apiKey.tenantId}:${apiKey.keyHash}`);
    }

    const existingByIdIndex = this.memoryApiKeys.findIndex(
      (item) => item.tenantId === apiKey.tenantId && item.id === apiKey.id
    );
    if (existingByIdIndex >= 0) {
      const existingById = this.memoryApiKeys[existingByIdIndex];
      if (existingById) {
        this.memoryApiKeyByHash.delete(
          buildTenantScopedLookupKey(existingById.tenantId, existingById.keyHash)
        );
      }
      this.memoryApiKeys.splice(existingByIdIndex, 1);
    }

    this.memoryApiKeys.unshift(this.cloneApiKey(apiKey));
    this.memoryApiKeyByHash.set(lookupKey, {
      tenantId: apiKey.tenantId,
      apiKeyId: apiKey.id,
    });
    return this.cloneApiKey(apiKey);
  }

  private revokeApiKeyFromMemory(
    tenantId: string,
    apiKeyId: string,
    revokedAt: string
  ): ApiKey | null {
    const index = this.memoryApiKeys.findIndex(
      (item) => item.tenantId === tenantId && item.id === apiKeyId
    );
    if (index < 0) {
      return null;
    }
    const current = this.memoryApiKeys[index];
    if (!current) {
      return null;
    }
    if (current.revokedAt) {
      return this.cloneApiKey(current);
    }
    const updated: ApiKey = {
      ...current,
      revokedAt,
      updatedAt: revokedAt,
      scopes: [...current.scopes],
    };
    this.memoryApiKeys[index] = updated;
    return this.cloneApiKey(updated);
  }

  private touchApiKeyUsageFromMemory(
    tenantId: string,
    apiKeyId: string,
    touchedAt: string
  ): ApiKey | null {
    const index = this.memoryApiKeys.findIndex(
      (item) => item.tenantId === tenantId && item.id === apiKeyId
    );
    if (index < 0) {
      return null;
    }
    const current = this.memoryApiKeys[index];
    if (!current) {
      return null;
    }
    if (current.revokedAt) {
      return this.cloneApiKey(current);
    }
    const currentLastUsedAtTimestamp = current.lastUsedAt
      ? Date.parse(current.lastUsedAt)
      : Number.NaN;
    const touchedAtTimestamp = Date.parse(touchedAt);
    const nextLastUsedAt =
      Number.isFinite(currentLastUsedAtTimestamp) &&
      Number.isFinite(touchedAtTimestamp) &&
      currentLastUsedAtTimestamp > touchedAtTimestamp
        ? current.lastUsedAt
        : touchedAt;
    const updated: ApiKey = {
      ...current,
      lastUsedAt: nextLastUsedAt,
      updatedAt: touchedAt,
      scopes: [...current.scopes],
    };
    this.memoryApiKeys[index] = updated;
    return this.cloneApiKey(updated);
  }

  private findApiKeyByHashFromMemory(tenantId: string, keyHash: string): ApiKey | null {
    const lookupKey = buildTenantScopedLookupKey(tenantId, keyHash);
    const indexed = this.memoryApiKeyByHash.get(lookupKey);
    if (indexed) {
      const found = this.memoryApiKeys.find(
        (item) => item.tenantId === indexed.tenantId && item.id === indexed.apiKeyId
      );
      if (found) {
        return this.cloneApiKey(found);
      }
      this.memoryApiKeyByHash.delete(lookupKey);
    }

    const matched = this.memoryApiKeys.find(
      (item) => item.tenantId === tenantId && item.keyHash === keyHash
    );
    if (!matched) {
      return null;
    }
    this.memoryApiKeyByHash.set(lookupKey, {
      tenantId,
      apiKeyId: matched.id,
    });
    return this.cloneApiKey(matched);
  }

  private cloneWebhookEndpoint(endpoint: WebhookEndpoint): WebhookEndpoint {
    return {
      ...endpoint,
      eventTypes: [...endpoint.eventTypes],
      headers: { ...endpoint.headers },
    };
  }

  private listWebhookEndpointsFromMemory(tenantId: string, limit: number): WebhookEndpoint[] {
    return this.memoryWebhookEndpoints
      .filter((endpoint) => endpoint.tenantId === tenantId)
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || b.id.localeCompare(a.id))
      .slice(0, limit)
      .map((endpoint) => this.cloneWebhookEndpoint(endpoint));
  }

  private getWebhookEndpointByIdFromMemory(
    tenantId: string,
    endpointId: string
  ): WebhookEndpoint | null {
    const matched = this.memoryWebhookEndpoints.find(
      (endpoint) => endpoint.tenantId === tenantId && endpoint.id === endpointId
    );
    return matched ? this.cloneWebhookEndpoint(matched) : null;
  }

  private createWebhookEndpointToMemory(endpoint: WebhookEndpoint): WebhookEndpoint {
    const existing = this.memoryWebhookEndpoints.find(
      (item) => item.tenantId === endpoint.tenantId && item.name === endpoint.name
    );
    if (existing) {
      throw new Error(`webhook_endpoint_name_already_exists:${endpoint.tenantId}:${endpoint.name}`);
    }
    this.memoryWebhookEndpoints.unshift(this.cloneWebhookEndpoint(endpoint));
    return this.cloneWebhookEndpoint(endpoint);
  }

  private updateWebhookEndpointInMemory(
    tenantId: string,
    endpointId: string,
    input: UpdateWebhookEndpointInput,
    hasSecretHash: boolean,
    hasSecretCiphertext: boolean,
    updatedAt: string
  ): WebhookEndpoint | null {
    const index = this.memoryWebhookEndpoints.findIndex(
      (endpoint) => endpoint.tenantId === tenantId && endpoint.id === endpointId
    );
    if (index < 0) {
      return null;
    }
    const current = this.memoryWebhookEndpoints[index];
    if (!current) {
      return null;
    }

    const nextName = input.name ?? current.name;
    const duplicated = this.memoryWebhookEndpoints.find(
      (endpoint) =>
        endpoint.tenantId === tenantId &&
        endpoint.name === nextName &&
        endpoint.id !== endpointId
    );
    if (duplicated) {
      throw new Error(`webhook_endpoint_name_already_exists:${tenantId}:${nextName}`);
    }

    const next: WebhookEndpoint = {
      ...current,
      name: nextName,
      url: input.url ?? current.url,
      enabled: input.enabled ?? current.enabled,
      eventTypes: input.eventTypes ? [...input.eventTypes] : [...current.eventTypes],
      secretHash: hasSecretHash
        ? firstNonEmptyString(input.secretHash ?? undefined) ?? undefined
        : current.secretHash,
      secretCiphertext: hasSecretCiphertext
        ? firstNonEmptyString(input.secretCiphertext ?? undefined) ?? undefined
        : current.secretCiphertext,
      headers: input.headers ? { ...input.headers } : { ...current.headers },
      updatedAt,
    };
    this.memoryWebhookEndpoints[index] = next;
    return this.cloneWebhookEndpoint(next);
  }

  private deleteWebhookEndpointFromMemory(tenantId: string, endpointId: string): boolean {
    const index = this.memoryWebhookEndpoints.findIndex(
      (endpoint) => endpoint.tenantId === tenantId && endpoint.id === endpointId
    );
    if (index < 0) {
      return false;
    }
    this.memoryWebhookEndpoints.splice(index, 1);
    return true;
  }

  private cloneWebhookReplayTask(task: WebhookReplayTask): WebhookReplayTask {
    return {
      ...task,
      filters: { ...task.filters },
      result: { ...task.result },
    };
  }

  private createWebhookReplayTaskToMemory(task: WebhookReplayTask): WebhookReplayTask {
    const cloned = this.cloneWebhookReplayTask(task);
    this.memoryWebhookReplayTasks.unshift(cloned);
    return this.cloneWebhookReplayTask(cloned);
  }

  private getWebhookReplayTaskByIdFromMemory(
    tenantId: string,
    taskId: string
  ): WebhookReplayTask | null {
    const matched = this.memoryWebhookReplayTasks.find(
      (task) => task.tenantId === tenantId && task.id === taskId
    );
    return matched ? this.cloneWebhookReplayTask(matched) : null;
  }

  private listWebhookReplayTasksFromMemory(
    tenantId: string,
    input: NormalizedWebhookReplayTaskListInput
  ): WebhookReplayTaskListResult {
    const cursor = decodeTimePaginationCursor(input.cursor);
    const items = this.memoryWebhookReplayTasks.filter((task) => {
      if (task.tenantId !== tenantId) {
        return false;
      }
      if (input.webhookId && task.webhookId !== input.webhookId) {
        return false;
      }
      if (input.status && task.status !== input.status) {
        return false;
      }
      if (cursor) {
        const requestedAtTimestamp = Date.parse(task.requestedAt);
        const cursorTimestamp = Date.parse(cursor.timestamp);
        if (Number.isFinite(requestedAtTimestamp) && Number.isFinite(cursorTimestamp)) {
          if (requestedAtTimestamp < cursorTimestamp) {
            return true;
          }
          if (requestedAtTimestamp > cursorTimestamp) {
            return false;
          }
          return task.id < cursor.id;
        }
        const requestedAtCompare = task.requestedAt.localeCompare(cursor.timestamp);
        if (requestedAtCompare < 0) {
          return true;
        }
        if (requestedAtCompare > 0) {
          return false;
        }
        return task.id < cursor.id;
      }
      return true;
    });

    const sorted = items.sort(
      (a, b) => b.requestedAt.localeCompare(a.requestedAt) || b.id.localeCompare(a.id)
    );
    const sliced = sorted.slice(0, input.limit);
    const nextCursor =
      sorted.length > input.limit && sliced.length > 0
        ? encodeTimePaginationCursor({
            timestamp: sliced[sliced.length - 1]?.requestedAt ?? new Date().toISOString(),
            id: sliced[sliced.length - 1]?.id ?? "",
          })
        : null;

    return {
      items: sliced.map((task) => this.cloneWebhookReplayTask(task)),
      total: sliced.length,
      nextCursor,
    };
  }

  private updateWebhookReplayTaskInMemory(
    tenantId: string,
    taskId: string,
    input: {
      fromStatuses?: WebhookReplayTaskStatus[];
      status?: WebhookReplayTaskStatus;
      result?: Record<string, unknown>;
      error?: string | null;
      startedAt?: string | null;
      finishedAt?: string | null;
      updatedAt: string;
    },
    flags: {
      hasStatus: boolean;
      hasResult: boolean;
      hasError: boolean;
      hasStartedAt: boolean;
      hasFinishedAt: boolean;
    }
  ): WebhookReplayTask | null {
    const index = this.memoryWebhookReplayTasks.findIndex(
      (task) => task.tenantId === tenantId && task.id === taskId
    );
    if (index < 0) {
      return null;
    }
    const current = this.memoryWebhookReplayTasks[index];
    if (!current) {
      return null;
    }

    if (
      Array.isArray(input.fromStatuses) &&
      input.fromStatuses.length > 0 &&
      !input.fromStatuses.includes(current.status)
    ) {
      return null;
    }

    const updated: WebhookReplayTask = {
      ...current,
      status: flags.hasStatus ? input.status ?? current.status : current.status,
      result: flags.hasResult ? { ...(input.result ?? {}) } : { ...current.result },
      error: flags.hasError ? firstNonEmptyString(input.error ?? undefined) ?? undefined : current.error,
      startedAt: flags.hasStartedAt
        ? toIsoString(input.startedAt ?? undefined) ?? undefined
        : current.startedAt,
      finishedAt: flags.hasFinishedAt
        ? toIsoString(input.finishedAt ?? undefined) ?? undefined
        : current.finishedAt,
      updatedAt: input.updatedAt,
      filters: { ...current.filters },
    };

    this.memoryWebhookReplayTasks[index] = updated;
    return this.cloneWebhookReplayTask(updated);
  }

  private buildWebhookReplayEventForApiKeyCreated(apiKey: ApiKey): WebhookReplayEvent {
    return {
      id: `api_key.created:${apiKey.id}`,
      tenantId: apiKey.tenantId,
      eventType: "api_key.created",
      occurredAt: apiKey.createdAt,
      payload: {
        eventType: "api_key.created",
        apiKeyId: apiKey.id,
        name: apiKey.name,
        scopes: [...apiKey.scopes],
        createdAt: apiKey.createdAt,
        updatedAt: apiKey.updatedAt,
      },
    };
  }

  private buildWebhookReplayEventForApiKeyRevoked(apiKey: ApiKey): WebhookReplayEvent | null {
    if (!apiKey.revokedAt) {
      return null;
    }
    return {
      id: `api_key.revoked:${apiKey.id}`,
      tenantId: apiKey.tenantId,
      eventType: "api_key.revoked",
      occurredAt: apiKey.revokedAt,
      payload: {
        eventType: "api_key.revoked",
        apiKeyId: apiKey.id,
        name: apiKey.name,
        scopes: [...apiKey.scopes],
        revokedAt: apiKey.revokedAt,
        updatedAt: apiKey.updatedAt,
      },
    };
  }

  private buildWebhookReplayEventForQualityEvent(
    qualityEvent: QualityEvent
  ): WebhookReplayEvent {
    return {
      id: `quality.event.created:${qualityEvent.id}`,
      tenantId: qualityEvent.tenantId,
      eventType: "quality.event.created",
      occurredAt: qualityEvent.createdAt,
      payload: {
        eventType: "quality.event.created",
        qualityEventId: qualityEvent.id,
        metric: qualityEvent.metricKey ?? qualityEvent.scorecardKey,
        scorecardKey: qualityEvent.scorecardKey,
        score: qualityEvent.score,
        passed: qualityEvent.passed,
        externalSource: qualityEvent.externalSource
          ? { ...qualityEvent.externalSource }
          : undefined,
        metadata: { ...qualityEvent.metadata },
        createdAt: qualityEvent.createdAt,
      },
    };
  }

  private buildWebhookReplayEventForQualityScorecard(
    scorecard: QualityScorecard
  ): WebhookReplayEvent {
    return {
      id: `quality.scorecard.updated:${scorecard.scorecardKey}:${scorecard.updatedAt}`,
      tenantId: scorecard.tenantId,
      eventType: "quality.scorecard.updated",
      occurredAt: scorecard.updatedAt,
      payload: {
        eventType: "quality.scorecard.updated",
        scorecardKey: scorecard.scorecardKey,
        title: scorecard.title,
        description: scorecard.description,
        score: scorecard.score,
        dimensions: { ...scorecard.dimensions },
        metadata: { ...scorecard.metadata },
        createdAt: scorecard.createdAt,
        updatedAt: scorecard.updatedAt,
      },
    };
  }

  private buildWebhookReplayEventForReplayJobStarted(
    replayJob: ReplayJob
  ): WebhookReplayEvent | null {
    if (!replayJob.startedAt) {
      return null;
    }
    return {
      id: `replay.job.started:${replayJob.id}`,
      tenantId: replayJob.tenantId,
      eventType: "replay.job.started",
      occurredAt: replayJob.startedAt,
      payload: {
        eventType: "replay.job.started",
        replayJobId: replayJob.id,
        baselineId: replayJob.baselineId,
        status: replayJob.status,
        parameters: { ...replayJob.parameters },
        summary: { ...replayJob.summary },
        startedAt: replayJob.startedAt,
        createdAt: replayJob.createdAt,
        updatedAt: replayJob.updatedAt,
      },
    };
  }

  private buildReplayRunWebhookPayloadBase(
    replayJob: ReplayJob
  ): Record<string, unknown> {
    return {
      runId: replayJob.id,
      jobId: replayJob.id,
      replayJobId: replayJob.id,
      datasetId: replayJob.baselineId,
      baselineId: replayJob.baselineId,
      status: replayJob.status,
      parameters: { ...replayJob.parameters },
      summary: { ...replayJob.summary },
      createdAt: replayJob.createdAt,
      updatedAt: replayJob.updatedAt,
      startedAt: replayJob.startedAt,
      finishedAt: replayJob.finishedAt,
    };
  }

  private hasReplayRegressions(replayJob: ReplayJob): boolean {
    return Math.max(
      0,
      Math.trunc(
        toNumber(
          (replayJob.summary as Record<string, unknown> | undefined)?.regressedCases,
          0
        )
      )
    ) > 0;
  }

  private buildWebhookReplayEventForReplayJobCompleted(
    replayJob: ReplayJob
  ): WebhookReplayEvent | null {
    if (replayJob.status !== "completed") {
      return null;
    }
    const occurredAt = replayJob.finishedAt ?? replayJob.updatedAt;
    if (!occurredAt) {
      return null;
    }
    return {
      id: `replay.job.completed:${replayJob.id}`,
      tenantId: replayJob.tenantId,
      eventType: "replay.job.completed",
      occurredAt,
      payload: {
        eventType: "replay.job.completed",
        replayJobId: replayJob.id,
        baselineId: replayJob.baselineId,
        status: replayJob.status,
        parameters: { ...replayJob.parameters },
        summary: { ...replayJob.summary },
        diff: { ...replayJob.diff },
        finishedAt: replayJob.finishedAt,
        createdAt: replayJob.createdAt,
        updatedAt: replayJob.updatedAt,
      },
    };
  }

  private buildWebhookReplayEventForReplayRunCompleted(
    replayJob: ReplayJob
  ): WebhookReplayEvent | null {
    if (replayJob.status !== "completed") {
      return null;
    }
    const occurredAt = replayJob.finishedAt ?? replayJob.updatedAt;
    if (!occurredAt) {
      return null;
    }
    return {
      id: `replay.run.completed:${replayJob.id}`,
      tenantId: replayJob.tenantId,
      eventType: "replay.run.completed",
      occurredAt,
      payload: {
        eventType: "replay.run.completed",
        ...this.buildReplayRunWebhookPayloadBase(replayJob),
        diff: { ...replayJob.diff },
      },
    };
  }

  private buildWebhookReplayEventForReplayRunStarted(
    replayJob: ReplayJob
  ): WebhookReplayEvent | null {
    if (!replayJob.startedAt) {
      return null;
    }
    return {
      id: `replay.run.started:${replayJob.id}`,
      tenantId: replayJob.tenantId,
      eventType: "replay.run.started",
      occurredAt: replayJob.startedAt,
      payload: {
        eventType: "replay.run.started",
        ...this.buildReplayRunWebhookPayloadBase(replayJob),
      },
    };
  }

  private buildWebhookReplayEventForReplayRunRegressionDetected(
    replayJob: ReplayJob
  ): WebhookReplayEvent | null {
    if (replayJob.status !== "completed" || !this.hasReplayRegressions(replayJob)) {
      return null;
    }
    const occurredAt = replayJob.finishedAt ?? replayJob.updatedAt;
    if (!occurredAt) {
      return null;
    }
    return {
      id: `replay.run.regression_detected:${replayJob.id}`,
      tenantId: replayJob.tenantId,
      eventType: "replay.run.regression_detected",
      occurredAt,
      payload: {
        eventType: "replay.run.regression_detected",
        ...this.buildReplayRunWebhookPayloadBase(replayJob),
        diff: { ...replayJob.diff },
      },
    };
  }

  private buildWebhookReplayEventForReplayJobFailed(
    replayJob: ReplayJob
  ): WebhookReplayEvent | null {
    if (replayJob.status !== "failed") {
      return null;
    }
    const occurredAt = replayJob.finishedAt ?? replayJob.updatedAt;
    if (!occurredAt) {
      return null;
    }
    return {
      id: `replay.job.failed:${replayJob.id}`,
      tenantId: replayJob.tenantId,
      eventType: "replay.job.failed",
      occurredAt,
      payload: {
        eventType: "replay.job.failed",
        replayJobId: replayJob.id,
        baselineId: replayJob.baselineId,
        status: replayJob.status,
        error: replayJob.error,
        parameters: { ...replayJob.parameters },
        summary: { ...replayJob.summary },
        finishedAt: replayJob.finishedAt,
        createdAt: replayJob.createdAt,
        updatedAt: replayJob.updatedAt,
      },
    };
  }

  private buildWebhookReplayEventForReplayRunFailed(
    replayJob: ReplayJob
  ): WebhookReplayEvent | null {
    if (replayJob.status !== "failed") {
      return null;
    }
    const occurredAt = replayJob.finishedAt ?? replayJob.updatedAt;
    if (!occurredAt) {
      return null;
    }
    return {
      id: `replay.run.failed:${replayJob.id}`,
      tenantId: replayJob.tenantId,
      eventType: "replay.run.failed",
      occurredAt,
      payload: {
        eventType: "replay.run.failed",
        ...this.buildReplayRunWebhookPayloadBase(replayJob),
        error: replayJob.error,
      },
    };
  }

  private buildWebhookReplayEventForReplayRunCancelled(
    replayJob: ReplayJob
  ): WebhookReplayEvent | null {
    if (replayJob.status !== "cancelled") {
      return null;
    }
    const occurredAt = replayJob.finishedAt ?? replayJob.updatedAt;
    if (!occurredAt) {
      return null;
    }
    return {
      id: `replay.run.cancelled:${replayJob.id}`,
      tenantId: replayJob.tenantId,
      eventType: "replay.run.cancelled",
      occurredAt,
      payload: {
        eventType: "replay.run.cancelled",
        ...this.buildReplayRunWebhookPayloadBase(replayJob),
        error: replayJob.error,
      },
    };
  }

  private matchesWebhookReplayEventTimeRange(
    event: WebhookReplayEvent,
    from?: string,
    to?: string
  ): boolean {
    const occurredAtTs = Date.parse(event.occurredAt);
    const fromTs = from ? Date.parse(from) : Number.NaN;
    const toTs = to ? Date.parse(to) : Number.NaN;
    if (Number.isFinite(fromTs) && Number.isFinite(occurredAtTs) && occurredAtTs < fromTs) {
      return false;
    }
    if (Number.isFinite(toTs) && Number.isFinite(occurredAtTs) && occurredAtTs > toTs) {
      return false;
    }
    if (!Number.isFinite(occurredAtTs)) {
      if (from && event.occurredAt < from) {
        return false;
      }
      if (to && event.occurredAt > to) {
        return false;
      }
    }
    return true;
  }

  private async queryWebhookReplayEventsByType(
    pool: PgQueryable,
    tenantId: string,
    eventType: WebhookEventType,
    from: string | undefined,
    to: string | undefined,
    limit: number
  ): Promise<WebhookReplayEvent[]> {
    if (limit <= 0) {
      return [];
    }

    if (eventType === "api_key.created") {
      const params: unknown[] = [tenantId];
      const whereClauses = ["tenant_id = $1"];
      if (from) {
        params.push(from);
        whereClauses.push(`created_at >= $${params.length}::timestamptz`);
      }
      if (to) {
        params.push(to);
        whereClauses.push(`created_at <= $${params.length}::timestamptz`);
      }
      params.push(limit);
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                key_hash,
                scopes,
                last_used_at,
                revoked_at,
                created_at,
                updated_at
         FROM api_keys
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY created_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      return result.rows.map((row) => this.buildWebhookReplayEventForApiKeyCreated(mapApiKeyRow(row)));
    }

    if (eventType === "api_key.revoked") {
      const params: unknown[] = [tenantId];
      const whereClauses = ["tenant_id = $1", "revoked_at IS NOT NULL"];
      if (from) {
        params.push(from);
        whereClauses.push(`revoked_at >= $${params.length}::timestamptz`);
      }
      if (to) {
        params.push(to);
        whereClauses.push(`revoked_at <= $${params.length}::timestamptz`);
      }
      params.push(limit);
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                name,
                key_hash,
                scopes,
                last_used_at,
                revoked_at,
                created_at,
                updated_at
         FROM api_keys
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY revoked_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      return result.rows
        .map((row) => this.buildWebhookReplayEventForApiKeyRevoked(mapApiKeyRow(row)))
        .filter((row): row is WebhookReplayEvent => Boolean(row));
    }

    if (eventType === "quality.event.created") {
      const params: unknown[] = [tenantId];
      const whereClauses = ["tenant_id = $1"];
      if (from) {
        params.push(from);
        whereClauses.push(`created_at >= $${params.length}::timestamptz`);
      }
      if (to) {
        params.push(to);
        whereClauses.push(`created_at <= $${params.length}::timestamptz`);
      }
      params.push(limit);
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                scorecard_key,
                metric_key,
                provider,
                repository,
                workflow,
                run_id,
                score,
                passed,
                metadata,
                created_at
         FROM quality_events
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY created_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      return result.rows.map((row) =>
        this.buildWebhookReplayEventForQualityEvent(mapQualityEventRow(row))
      );
    }

    if (eventType === "quality.scorecard.updated") {
      const params: unknown[] = [tenantId];
      const whereClauses = ["tenant_id = $1"];
      if (from) {
        params.push(from);
        whereClauses.push(`updated_at >= $${params.length}::timestamptz`);
      }
      if (to) {
        params.push(to);
        whereClauses.push(`updated_at <= $${params.length}::timestamptz`);
      }
      params.push(limit);
      const result = await pool.query(
        `SELECT tenant_id,
                scorecard_key,
                title,
                description,
                score,
                dimensions,
                metadata,
                created_at,
                updated_at
         FROM quality_scorecards
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY updated_at DESC, scorecard_key DESC
         LIMIT $${params.length}`,
        params
      );
      return result.rows.map((row) =>
        this.buildWebhookReplayEventForQualityScorecard(mapQualityScorecardRow(row))
      );
    }

    if (eventType === "replay.job.started") {
      const params: unknown[] = [tenantId];
      const whereClauses = ["tenant_id = $1", "started_at IS NOT NULL"];
      if (from) {
        params.push(from);
        whereClauses.push(`started_at >= $${params.length}::timestamptz`);
      }
      if (to) {
        params.push(to);
        whereClauses.push(`started_at <= $${params.length}::timestamptz`);
      }
      params.push(limit);
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                dataset_id AS baseline_id,
                status,
                parameters,
                summary_payload,
                diff_payload,
                error,
                started_at,
                finished_at,
                created_at,
                updated_at
         FROM replay_runs
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY started_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      return result.rows
        .map((row) => this.buildWebhookReplayEventForReplayJobStarted(mapReplayJobRow(row)))
        .filter((row): row is WebhookReplayEvent => Boolean(row));
    }

    if (eventType === "replay.run.started") {
      const params: unknown[] = [tenantId];
      const whereClauses = ["tenant_id = $1", "started_at IS NOT NULL"];
      if (from) {
        params.push(from);
        whereClauses.push(`started_at >= $${params.length}::timestamptz`);
      }
      if (to) {
        params.push(to);
        whereClauses.push(`started_at <= $${params.length}::timestamptz`);
      }
      params.push(limit);
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                dataset_id AS baseline_id,
                status,
                parameters,
                summary_payload,
                diff_payload,
                error,
                started_at,
                finished_at,
                created_at,
                updated_at
         FROM replay_runs
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY started_at DESC, id DESC
         LIMIT $${params.length}`,
        params
      );
      return result.rows
        .map((row) => this.buildWebhookReplayEventForReplayRunStarted(mapReplayJobRow(row)))
        .filter((row): row is WebhookReplayEvent => Boolean(row));
    }

    if (
      eventType === "replay.job.completed" ||
      eventType === "replay.job.failed" ||
      eventType === "replay.run.cancelled" ||
      eventType === "replay.run.completed" ||
      eventType === "replay.run.failed" ||
      eventType === "replay.run.regression_detected"
    ) {
      const targetStatus =
        eventType === "replay.job.failed" || eventType === "replay.run.failed"
          ? "failed"
          : eventType === "replay.run.cancelled"
            ? "cancelled"
            : "completed";
      const occurredAtSql = "COALESCE(finished_at, updated_at)";
      const params: unknown[] = [tenantId, targetStatus];
      const whereClauses = ["tenant_id = $1", "status = $2", `${occurredAtSql} IS NOT NULL`];
      if (eventType === "replay.run.regression_detected") {
        whereClauses.push(
          "COALESCE(NULLIF(summary_payload ->> 'regressedCases', ''), '0')::int > 0"
        );
      }
      if (from) {
        params.push(from);
        whereClauses.push(`${occurredAtSql} >= $${params.length}::timestamptz`);
      }
      if (to) {
        params.push(to);
        whereClauses.push(`${occurredAtSql} <= $${params.length}::timestamptz`);
      }
      params.push(limit);
      const result = await pool.query(
        `SELECT id,
                tenant_id,
                dataset_id AS baseline_id,
                status,
                parameters,
                summary_payload,
                diff_payload,
                error,
                started_at,
                finished_at,
                created_at,
                updated_at
         FROM replay_runs
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY ${occurredAtSql} DESC, id DESC
         LIMIT $${params.length}`,
        params
      );

      if (eventType === "replay.job.completed") {
        return result.rows
          .map((row) => this.buildWebhookReplayEventForReplayJobCompleted(mapReplayJobRow(row)))
          .filter((row): row is WebhookReplayEvent => Boolean(row));
      }
      if (eventType === "replay.run.completed") {
        return result.rows
          .map((row) => this.buildWebhookReplayEventForReplayRunCompleted(mapReplayJobRow(row)))
          .filter((row): row is WebhookReplayEvent => Boolean(row));
      }
      if (eventType === "replay.run.regression_detected") {
        return result.rows
          .map((row) =>
            this.buildWebhookReplayEventForReplayRunRegressionDetected(mapReplayJobRow(row))
          )
          .filter((row): row is WebhookReplayEvent => Boolean(row));
      }
      if (eventType === "replay.run.failed") {
        return result.rows
          .map((row) => this.buildWebhookReplayEventForReplayRunFailed(mapReplayJobRow(row)))
          .filter((row): row is WebhookReplayEvent => Boolean(row));
      }
      if (eventType === "replay.run.cancelled") {
        return result.rows
          .map((row) => this.buildWebhookReplayEventForReplayRunCancelled(mapReplayJobRow(row)))
          .filter((row): row is WebhookReplayEvent => Boolean(row));
      }
      return result.rows
        .map((row) => this.buildWebhookReplayEventForReplayJobFailed(mapReplayJobRow(row)))
        .filter((row): row is WebhookReplayEvent => Boolean(row));
    }

    return [];
  }

  private listWebhookReplayEventsFromMemory(
    tenantId: string,
    input: NormalizedWebhookReplayEventListInput
  ): WebhookReplayEvent[] {
    const items: WebhookReplayEvent[] = [];
    for (const eventType of input.eventTypes) {
      if (eventType === "api_key.created") {
        for (const apiKey of this.memoryApiKeys) {
          if (apiKey.tenantId !== tenantId) {
            continue;
          }
          items.push(this.buildWebhookReplayEventForApiKeyCreated(this.cloneApiKey(apiKey)));
        }
        continue;
      }

      if (eventType === "api_key.revoked") {
        for (const apiKey of this.memoryApiKeys) {
          if (apiKey.tenantId !== tenantId) {
            continue;
          }
          const replayEvent = this.buildWebhookReplayEventForApiKeyRevoked(
            this.cloneApiKey(apiKey)
          );
          if (replayEvent) {
            items.push(replayEvent);
          }
        }
        continue;
      }

      if (eventType === "quality.event.created") {
        for (const qualityEvent of this.memoryQualityEvents) {
          if (qualityEvent.tenantId !== tenantId) {
            continue;
          }
          items.push(
            this.buildWebhookReplayEventForQualityEvent(this.cloneQualityEvent(qualityEvent))
          );
        }
        continue;
      }

      if (eventType === "quality.scorecard.updated") {
        for (const scorecard of this.memoryQualityScorecards) {
          if (scorecard.tenantId !== tenantId) {
            continue;
          }
          items.push(
            this.buildWebhookReplayEventForQualityScorecard(
              this.cloneQualityScorecard(scorecard)
            )
          );
        }
        continue;
      }

      if (eventType === "replay.job.started") {
        for (const replayRun of this.memoryReplayRuns) {
          if (replayRun.tenantId !== tenantId) {
            continue;
          }
          const replayEvent = this.buildWebhookReplayEventForReplayJobStarted(
            mapReplayRunToJob(this.cloneReplayRun(replayRun))
          );
          if (replayEvent) {
            items.push(replayEvent);
          }
        }
        continue;
      }

      if (eventType === "replay.run.started") {
        for (const replayRun of this.memoryReplayRuns) {
          if (replayRun.tenantId !== tenantId) {
            continue;
          }
          const replayEvent = this.buildWebhookReplayEventForReplayRunStarted(
            mapReplayRunToJob(this.cloneReplayRun(replayRun))
          );
          if (replayEvent) {
            items.push(replayEvent);
          }
        }
        continue;
      }

      if (eventType === "replay.job.completed") {
        for (const replayRun of this.memoryReplayRuns) {
          if (replayRun.tenantId !== tenantId) {
            continue;
          }
          const replayEvent = this.buildWebhookReplayEventForReplayJobCompleted(
            mapReplayRunToJob(this.cloneReplayRun(replayRun))
          );
          if (replayEvent) {
            items.push(replayEvent);
          }
        }
        continue;
      }

      if (eventType === "replay.job.failed") {
        for (const replayRun of this.memoryReplayRuns) {
          if (replayRun.tenantId !== tenantId) {
            continue;
          }
          const replayEvent = this.buildWebhookReplayEventForReplayJobFailed(
            mapReplayRunToJob(this.cloneReplayRun(replayRun))
          );
          if (replayEvent) {
            items.push(replayEvent);
          }
        }
        continue;
      }

      if (eventType === "replay.run.completed") {
        for (const replayRun of this.memoryReplayRuns) {
          if (replayRun.tenantId !== tenantId) {
            continue;
          }
          const replayEvent = this.buildWebhookReplayEventForReplayRunCompleted(
            mapReplayRunToJob(this.cloneReplayRun(replayRun))
          );
          if (replayEvent) {
            items.push(replayEvent);
          }
        }
        continue;
      }

      if (eventType === "replay.run.regression_detected") {
        for (const replayRun of this.memoryReplayRuns) {
          if (replayRun.tenantId !== tenantId) {
            continue;
          }
          const replayEvent = this.buildWebhookReplayEventForReplayRunRegressionDetected(
            mapReplayRunToJob(this.cloneReplayRun(replayRun))
          );
          if (replayEvent) {
            items.push(replayEvent);
          }
        }
        continue;
      }

      if (eventType === "replay.run.failed") {
        for (const replayRun of this.memoryReplayRuns) {
          if (replayRun.tenantId !== tenantId) {
            continue;
          }
          const replayEvent = this.buildWebhookReplayEventForReplayRunFailed(
            mapReplayRunToJob(this.cloneReplayRun(replayRun))
          );
          if (replayEvent) {
            items.push(replayEvent);
          }
        }
        continue;
      }

      if (eventType === "replay.run.cancelled") {
        for (const replayRun of this.memoryReplayRuns) {
          if (replayRun.tenantId !== tenantId) {
            continue;
          }
          const replayEvent = this.buildWebhookReplayEventForReplayRunCancelled(
            mapReplayRunToJob(this.cloneReplayRun(replayRun))
          );
          if (replayEvent) {
            items.push(replayEvent);
          }
        }
      }
    }

    return items
      .filter((item) => this.matchesWebhookReplayEventTimeRange(item, input.from, input.to))
      .sort(compareWebhookReplayEventDesc)
      .slice(0, input.limit);
  }

  private cloneQualityEvent(qualityEvent: QualityEvent): QualityEvent {
    return {
      ...qualityEvent,
      externalSource: qualityEvent.externalSource
        ? { ...qualityEvent.externalSource }
        : undefined,
      metadata: { ...qualityEvent.metadata },
    };
  }

  private createQualityEventToMemory(qualityEvent: QualityEvent): QualityEvent {
    this.memoryQualityEvents.unshift(this.cloneQualityEvent(qualityEvent));
    return this.cloneQualityEvent(qualityEvent);
  }

  private listQualityDailyMetricsFromMemory(
    tenantId: string,
    input: NormalizedQualityDailyMetricsInput
  ): QualityDailyMetric[] {
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;
    const buckets = new Map<
      string,
      {
        total: number;
        passed: number;
        failed: number;
        scoreSum: number;
      }
    >();

    for (const qualityEvent of this.memoryQualityEvents) {
      if (qualityEvent.tenantId !== tenantId) {
        continue;
      }
      if (input.scorecardKey && qualityEvent.scorecardKey !== input.scorecardKey) {
        continue;
      }
      const externalSource =
        qualityEvent.externalSource ??
        extractQualityExternalSourceFromMetadata(qualityEvent.metadata);
      if (input.provider && externalSource?.provider !== input.provider) {
        continue;
      }
      if (input.repo && externalSource?.repo !== input.repo) {
        continue;
      }
      if (input.workflow && externalSource?.workflow !== input.workflow) {
        continue;
      }
      if (input.runId && externalSource?.runId !== input.runId) {
        continue;
      }
      const createdAtTimestamp = Date.parse(qualityEvent.createdAt);
      if (fromTimestamp !== undefined && createdAtTimestamp < fromTimestamp) {
        continue;
      }
      if (toTimestamp !== undefined && createdAtTimestamp > toTimestamp) {
        continue;
      }

      const dateKey = qualityEvent.createdAt.slice(0, 10);
      const bucket = buckets.get(dateKey) ?? {
        total: 0,
        passed: 0,
        failed: 0,
        scoreSum: 0,
      };
      bucket.total += 1;
      if (qualityEvent.passed) {
        bucket.passed += 1;
      } else {
        bucket.failed += 1;
      }
      bucket.scoreSum += normalizeQualityScore(qualityEvent.score);
      buckets.set(dateKey, bucket);
    }

    return [...buckets.entries()]
      .map(([date, bucket]) => ({
        date,
        total: bucket.total,
        passed: bucket.passed,
        failed: bucket.failed,
        averageScore:
          bucket.total > 0 ? Number((bucket.scoreSum / bucket.total).toFixed(6)) : 0,
      }))
      .sort((a, b) => b.date.localeCompare(a.date))
      .slice(0, input.limit);
  }

  private listQualityExternalMetricGroupsFromMemory(
    tenantId: string,
    input: NormalizedQualityExternalMetricGroupsInput
  ): QualityExternalMetricGroup[] {
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;
    const buckets = new Map<
      string,
      {
        total: number;
        passed: number;
        failed: number;
        scoreSum: number;
      }
    >();

    for (const qualityEvent of this.memoryQualityEvents) {
      if (qualityEvent.tenantId !== tenantId) {
        continue;
      }
      if (input.scorecardKey && qualityEvent.scorecardKey !== input.scorecardKey) {
        continue;
      }
      const externalSource =
        qualityEvent.externalSource ??
        extractQualityExternalSourceFromMetadata(qualityEvent.metadata);
      if (input.provider && externalSource?.provider !== input.provider) {
        continue;
      }
      if (input.repo && externalSource?.repo !== input.repo) {
        continue;
      }
      if (input.workflow && externalSource?.workflow !== input.workflow) {
        continue;
      }
      if (input.runId && externalSource?.runId !== input.runId) {
        continue;
      }

      const createdAtTimestamp = Date.parse(qualityEvent.createdAt);
      if (fromTimestamp !== undefined && createdAtTimestamp < fromTimestamp) {
        continue;
      }
      if (toTimestamp !== undefined && createdAtTimestamp > toTimestamp) {
        continue;
      }

      const groupValue = this.getQualityExternalGroupValue(externalSource, input.groupBy);
      const bucket = buckets.get(groupValue) ?? {
        total: 0,
        passed: 0,
        failed: 0,
        scoreSum: 0,
      };
      bucket.total += 1;
      if (qualityEvent.passed) {
        bucket.passed += 1;
      } else {
        bucket.failed += 1;
      }
      bucket.scoreSum += normalizeQualityScore(qualityEvent.score);
      buckets.set(groupValue, bucket);
    }

    return [...buckets.entries()]
      .map(([value, bucket]) => ({
        groupBy: input.groupBy,
        value,
        total: bucket.total,
        passed: bucket.passed,
        failed: bucket.failed,
        averageScore:
          bucket.total > 0 ? Number((bucket.scoreSum / bucket.total).toFixed(6)) : 0,
      }))
      .sort((a, b) => b.total - a.total || a.value.localeCompare(b.value))
      .slice(0, input.limit);
  }

  private getQualityExternalGroupValue(
    externalSource: QualityExternalSourceMetadata | undefined,
    groupBy: QualityExternalMetricGroupBy
  ): string {
    if (!externalSource) {
      return "unknown";
    }
    if (groupBy === "provider") {
      return externalSource.provider ?? "unknown";
    }
    if (groupBy === "repo") {
      return externalSource.repo ?? "unknown";
    }
    if (groupBy === "workflow") {
      return externalSource.workflow ?? "unknown";
    }
    return externalSource.runId ?? "unknown";
  }

  private cloneQualityScorecard(qualityScorecard: QualityScorecard): QualityScorecard {
    return {
      ...qualityScorecard,
      dimensions: { ...qualityScorecard.dimensions },
      metadata: { ...qualityScorecard.metadata },
    };
  }

  private listQualityScorecardsFromMemory(
    tenantId: string,
    input: NormalizedQualityScorecardListInput
  ): QualityScorecard[] {
    return this.memoryQualityScorecards
      .filter((qualityScorecard) => {
        if (qualityScorecard.tenantId !== tenantId) {
          return false;
        }
        if (input.scorecardKey && qualityScorecard.scorecardKey !== input.scorecardKey) {
          return false;
        }
        return true;
      })
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || a.scorecardKey.localeCompare(b.scorecardKey))
      .slice(0, input.limit)
      .map((qualityScorecard) => this.cloneQualityScorecard(qualityScorecard));
  }

  private upsertQualityScorecardToMemory(qualityScorecard: QualityScorecard): QualityScorecard {
    const index = this.memoryQualityScorecards.findIndex(
      (item) =>
        item.tenantId === qualityScorecard.tenantId &&
        item.scorecardKey === qualityScorecard.scorecardKey
    );
    if (index >= 0) {
      const current = this.memoryQualityScorecards[index];
      if (current) {
        qualityScorecard.createdAt = current.createdAt;
      }
      this.memoryQualityScorecards.splice(index, 1);
    }
    this.memoryQualityScorecards.unshift(this.cloneQualityScorecard(qualityScorecard));
    return this.cloneQualityScorecard(qualityScorecard);
  }

  private cloneReplayDataset(replayDataset: ReplayDataset): ReplayDataset {
    return {
      ...replayDataset,
      metadata: { ...replayDataset.metadata },
    };
  }

  private createReplayDatasetToMemory(replayDataset: ReplayDataset): ReplayDataset {
    const existing = this.memoryReplayDatasets.find(
      (item) => item.tenantId === replayDataset.tenantId && item.name === replayDataset.name
    );
    if (existing) {
      throw new Error(
        `replay_dataset_name_already_exists:${replayDataset.tenantId}:${replayDataset.name}`
      );
    }
    this.memoryReplayDatasets.unshift(this.cloneReplayDataset(replayDataset));
    return this.cloneReplayDataset(replayDataset);
  }

  private listReplayDatasetsFromMemory(
    tenantId: string,
    input: NormalizedReplayDatasetListInput
  ): ReplayDataset[] {
    const keyword = input.keyword?.toLowerCase();
    return this.memoryReplayDatasets
      .filter((replayDataset) => {
        if (replayDataset.tenantId !== tenantId) {
          return false;
        }
        if (!keyword) {
          return true;
        }
        return (
          replayDataset.name.toLowerCase().includes(keyword) ||
          (replayDataset.description ?? "").toLowerCase().includes(keyword)
        );
      })
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || b.id.localeCompare(a.id))
      .slice(0, input.limit)
      .map((replayDataset) => this.cloneReplayDataset(replayDataset));
  }

  private getReplayDatasetByIdFromMemory(tenantId: string, datasetId: string): ReplayDataset | null {
    const matched = this.memoryReplayDatasets.find(
      (replayDataset) => replayDataset.tenantId === tenantId && replayDataset.id === datasetId
    );
    return matched ? this.cloneReplayDataset(matched) : null;
  }

  private cloneReplayDatasetCase(replayDatasetCase: ReplayDatasetCase): ReplayDatasetCase {
    return {
      ...replayDatasetCase,
      metadata: { ...replayDatasetCase.metadata },
    };
  }

  private replaceReplayDatasetCasesInMemory(
    tenantId: string,
    datasetId: string,
    cases: ReplayDatasetCase[]
  ): ReplayDatasetCase[] {
    const datasetIndex = this.memoryReplayDatasets.findIndex(
      (item) => item.tenantId === tenantId && item.id === datasetId
    );
    if (datasetIndex < 0) {
      throw new Error(`replay_dataset_not_found:${tenantId}:${datasetId}`);
    }
    this.memoryReplayDatasetCases.splice(
      0,
      this.memoryReplayDatasetCases.length,
      ...this.memoryReplayDatasetCases.filter(
        (item) => item.tenantId !== tenantId || item.datasetId !== datasetId
      ),
      ...cases.map((item) => this.cloneReplayDatasetCase(item))
    );
    const currentDataset = this.memoryReplayDatasets[datasetIndex];
    if (currentDataset) {
      this.memoryReplayDatasets[datasetIndex] = {
        ...currentDataset,
        caseCount: cases.length,
        updatedAt: new Date().toISOString(),
      };
    }
    return cases.map((item) => this.cloneReplayDatasetCase(item));
  }

  private listReplayDatasetCasesFromMemory(
    tenantId: string,
    datasetId: string,
    input: NormalizedReplayDatasetCaseListInput
  ): ReplayDatasetCase[] {
    return this.memoryReplayDatasetCases
      .filter((item) => item.tenantId === tenantId && item.datasetId === datasetId)
      .sort((a, b) => a.sortOrder - b.sortOrder || a.caseId.localeCompare(b.caseId))
      .slice(0, input.limit)
      .map((item) => this.cloneReplayDatasetCase(item));
  }

  private cloneReplayRun(replayRun: ReplayRun): ReplayRun {
    return {
      ...replayRun,
      parameters: { ...replayRun.parameters },
      summary: { ...replayRun.summary },
      diff: { ...replayRun.diff },
    };
  }

  private createReplayRunToMemory(replayRun: ReplayRun): ReplayRun {
    const datasetExists = this.memoryReplayDatasets.some(
      (item) => item.tenantId === replayRun.tenantId && item.id === replayRun.datasetId
    );
    if (!datasetExists) {
      throw new Error(`replay_dataset_not_found:${replayRun.tenantId}:${replayRun.datasetId}`);
    }
    this.memoryReplayRuns.unshift(this.cloneReplayRun(replayRun));
    return this.cloneReplayRun(replayRun);
  }

  private listReplayRunsFromMemory(
    tenantId: string,
    input: NormalizedReplayRunListInput
  ): ReplayRun[] {
    return this.memoryReplayRuns
      .filter((replayRun) => {
        if (replayRun.tenantId !== tenantId) {
          return false;
        }
        if (input.datasetId && replayRun.datasetId !== input.datasetId) {
          return false;
        }
        if (input.status && replayRun.status !== input.status) {
          return false;
        }
        return true;
      })
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt) || b.id.localeCompare(a.id))
      .slice(0, input.limit)
      .map((replayRun) => this.cloneReplayRun(replayRun));
  }

  private getReplayRunByIdFromMemory(tenantId: string, runId: string): ReplayRun | null {
    const matched = this.memoryReplayRuns.find(
      (replayRun) => replayRun.tenantId === tenantId && replayRun.id === runId
    );
    return matched ? this.cloneReplayRun(matched) : null;
  }

  private updateReplayRunInMemory(
    tenantId: string,
    runId: string,
    input: {
      status?: ReplayJobStatus;
      fromStatuses?: ReplayJobStatus[];
      summary?: Record<string, unknown>;
      diff?: Record<string, unknown>;
      error?: string | null;
      startedAt?: string | null;
      finishedAt?: string | null;
      updatedAt: string;
    }
  ): ReplayRun | null {
    const index = this.memoryReplayRuns.findIndex(
      (replayRun) => replayRun.tenantId === tenantId && replayRun.id === runId
    );
    if (index < 0) {
      return null;
    }

    const current = this.memoryReplayRuns[index];
    if (!current) {
      return null;
    }
    if (
      input.fromStatuses &&
      input.fromStatuses.length > 0 &&
      !input.fromStatuses.includes(current.status)
    ) {
      return null;
    }

    const updated: ReplayRun = {
      ...current,
      updatedAt: input.updatedAt,
      parameters: { ...current.parameters },
      summary: input.summary ? { ...input.summary } : { ...current.summary },
      diff: input.diff ? { ...input.diff } : { ...current.diff },
    };

    if (input.status) {
      updated.status = input.status;
    }
    if (input.error !== undefined) {
      updated.error = input.error ?? undefined;
    }
    if (input.startedAt !== undefined) {
      updated.startedAt = input.startedAt ?? undefined;
    }
    if (input.finishedAt !== undefined) {
      updated.finishedAt = input.finishedAt ?? undefined;
    }

    this.memoryReplayRuns[index] = updated;
    return this.cloneReplayRun(updated);
  }

  private cloneReplayArtifact(replayArtifact: ReplayArtifact): ReplayArtifact {
    return {
      ...replayArtifact,
      metadata: { ...replayArtifact.metadata },
    };
  }

  private upsertReplayArtifactsInMemory(
    tenantId: string,
    runId: string,
    items: ReplayArtifact[]
  ): ReplayArtifact[] {
    this.memoryReplayArtifacts.splice(
      0,
      this.memoryReplayArtifacts.length,
      ...this.memoryReplayArtifacts.filter(
        (item) =>
          item.tenantId !== tenantId ||
          item.runId !== runId ||
          !items.some((candidate) => candidate.artifactType === item.artifactType)
      ),
      ...items.map((item) => this.cloneReplayArtifact(item))
    );
    return items.map((item) => this.cloneReplayArtifact(item));
  }

  private listReplayArtifactsFromMemory(
    tenantId: string,
    runId: string,
    input: NormalizedReplayArtifactListInput
  ): ReplayArtifact[] {
    return this.memoryReplayArtifacts
      .filter((item) => item.tenantId === tenantId && item.runId === runId)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.artifactType.localeCompare(b.artifactType))
      .slice(0, input.limit)
      .map((item) => this.cloneReplayArtifact(item));
  }

  private getReplayArtifactByTypeFromMemory(
    tenantId: string,
    runId: string,
    artifactType: ReplayArtifactType
  ): ReplayArtifact | null {
    const matched = this.memoryReplayArtifacts.find(
      (item) =>
        item.tenantId === tenantId && item.runId === runId && item.artifactType === artifactType
    );
    return matched ? this.cloneReplayArtifact(matched) : null;
  }

  private cloneReplayBaseline(replayBaseline: ReplayBaseline): ReplayBaseline {
    return {
      ...replayBaseline,
      metadata: { ...replayBaseline.metadata },
    };
  }

  private createReplayBaselineToMemory(replayBaseline: ReplayBaseline): ReplayBaseline {
    return mapReplayDatasetToBaseline(
      this.createReplayDatasetToMemory(mapReplayBaselineToDataset(replayBaseline))
    );
  }

  private listReplayBaselinesFromMemory(
    tenantId: string,
    input: NormalizedReplayBaselineListInput
  ): ReplayBaseline[] {
    return this.listReplayDatasetsFromMemory(tenantId, {
      keyword: input.keyword,
      limit: input.limit,
    }).map(mapReplayDatasetToBaseline);
  }

  private cloneReplayJob(replayJob: ReplayJob): ReplayJob {
    return {
      ...replayJob,
      parameters: { ...replayJob.parameters },
      summary: { ...replayJob.summary },
      diff: { ...replayJob.diff },
    };
  }

  private createReplayJobToMemory(replayJob: ReplayJob): ReplayJob {
    return mapReplayRunToJob(this.createReplayRunToMemory(mapReplayJobToRun(replayJob)));
  }

  private listReplayJobsFromMemory(
    tenantId: string,
    input: NormalizedReplayJobListInput
  ): ReplayJob[] {
    return this.listReplayRunsFromMemory(tenantId, {
      datasetId: input.baselineId,
      status: input.status,
      limit: input.limit,
    }).map(mapReplayRunToJob);
  }

  private getReplayJobByIdFromMemory(tenantId: string, replayJobId: string): ReplayJob | null {
    const run = this.getReplayRunByIdFromMemory(tenantId, replayJobId);
    return run ? mapReplayRunToJob(run) : null;
  }

  private getReplayJobDiffFromMemory(
    tenantId: string,
    replayJobId: string
  ): Record<string, unknown> | null {
    const run = this.getReplayRunByIdFromMemory(tenantId, replayJobId);
    return run ? { ...run.diff } : null;
  }

  private updateReplayJobInMemory(
    tenantId: string,
    replayJobId: string,
    input: {
      status?: ReplayJobStatus;
      fromStatuses?: ReplayJobStatus[];
      summary?: Record<string, unknown>;
      diff?: Record<string, unknown>;
      error?: string | null;
      startedAt?: string | null;
      finishedAt?: string | null;
      updatedAt: string;
    }
  ): ReplayJob | null {
    const run = this.updateReplayRunInMemory(tenantId, replayJobId, input);
    return run ? mapReplayRunToJob(run) : null;
  }

  private listSourcesFromMemory(tenantId: string): Source[] {
    return this.memorySources
      .filter((source) => this.resolveSourceTenantIdFromMemory(source) === tenantId)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
      .map((source) => ({ ...source }));
  }

  private saveSourceToMemory(source: Source, tenantId: string): Source {
    this.memorySources.unshift({ ...source });
    this.memorySourceTenantById.set(source.id, tenantId);
    return { ...source };
  }

  private listSyncJobsFromMemory(tenantId: string, sourceId: string, limit: number): SyncJob[] {
    return this.memorySyncJobs
      .filter(
        (job) =>
          job.sourceId === sourceId &&
          this.resolveSourceTenantIdBySourceIdFromMemory(job.sourceId) === tenantId
      )
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
      .slice(0, limit)
      .map((job) => ({ ...job }));
  }

  private saveSyncJobToMemory(syncJob: SyncJob, tenantId: string): SyncJob {
    const sourceTenantId = this.resolveSourceTenantIdBySourceIdFromMemory(syncJob.sourceId);
    if (sourceTenantId !== tenantId) {
      throw new Error("sync_job_source_not_found");
    }
    this.memorySyncJobs.unshift({ ...syncJob });
    return { ...syncJob };
  }

  private requestCancelSyncJobFromMemory(jobId: string, tenantId: string): SyncJob | null {
    const index = this.memorySyncJobs.findIndex((job) => job.id === jobId);
    if (index < 0) {
      return null;
    }

    const current = this.memorySyncJobs[index];
    if (!current) {
      return null;
    }
    if (this.resolveSourceTenantIdBySourceIdFromMemory(current.sourceId) !== tenantId) {
      return null;
    }

    if (current.status !== "pending" && current.status !== "running") {
      return { ...current };
    }

    const now = new Date().toISOString();
    if (current.status === "running") {
      const updated: SyncJob = {
        ...current,
        cancelRequested: true,
        updatedAt: now,
      };
      this.memorySyncJobs[index] = updated;
      return { ...updated };
    }

    const startedAt = current.startedAt ?? current.createdAt;
    const endedAt = current.endedAt ?? now;
    const durationMs =
      current.durationMs ?? Math.max(0, Date.parse(endedAt) - Date.parse(startedAt));
    const updated: SyncJob = {
      ...current,
      status: "cancelled",
      cancelRequested: true,
      startedAt,
      endedAt,
      durationMs: Number.isFinite(durationMs) ? durationMs : 0,
      updatedAt: now,
    };
    this.memorySyncJobs[index] = updated;
    return { ...updated };
  }

  private listSourceWatermarksFromMemory(
    tenantId: string,
    sourceId: string
  ): SourceWatermark[] {
    return this.memorySourceWatermarks
      .filter(
        (item) =>
          item.sourceId === sourceId &&
          this.resolveSourceTenantIdBySourceIdFromMemory(item.sourceId) === tenantId
      )
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt))
      .map((item) => ({ ...item }));
  }

  private listSourceParseFailuresFromMemory(
    tenantId: string,
    sourceId: string,
    input: NormalizedSourceParseFailureQueryInput
  ): SourceParseFailureListResult {
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;
    const parserKey = input.parserKey?.toLowerCase();
    const errorCode = input.errorCode?.toLowerCase();

    const filtered = this.memorySourceParseFailures.filter((record) => {
      if (record.tenantId !== tenantId) {
        return false;
      }
      if (record.failure.sourceId !== sourceId) {
        return false;
      }
      if (
        parserKey &&
        record.failure.parserKey.toLowerCase() !== parserKey
      ) {
        return false;
      }
      if (
        errorCode &&
        record.failure.errorCode.toLowerCase() !== errorCode
      ) {
        return false;
      }

      const occurredAtTimestamp = Date.parse(record.failure.failedAt);
      if (fromTimestamp !== undefined && occurredAtTimestamp < fromTimestamp) {
        return false;
      }
      if (toTimestamp !== undefined && occurredAtTimestamp > toTimestamp) {
        return false;
      }

      return true;
    });

    const items = [...filtered]
      .sort((a, b) => {
        const failedAtDiff = b.failure.failedAt.localeCompare(a.failure.failedAt);
        if (failedAtDiff !== 0) {
          return failedAtDiff;
        }
        return b.failure.createdAt.localeCompare(a.failure.createdAt);
      })
      .slice(0, input.limit)
      .map((record) => ({
        ...record.failure,
        metadata: { ...record.failure.metadata },
      }));

    return {
      items,
      total: filtered.length,
    };
  }

  private getSourceHealthFromMemory(tenantId: string, sourceId: string): SourceHealth | null {
    const source = this.memorySources.find(
      (item) =>
        item.id === sourceId && this.resolveSourceTenantIdFromMemory(item) === tenantId
    );
    if (!source) {
      return null;
    }

    let lastSuccessAt: string | null = null;
    let lastFailureAt: string | null = null;
    let failureCount = 0;
    let latencyTotal = 0;
    let latencyCount = 0;

    for (const job of this.memorySyncJobs) {
      if (job.sourceId !== sourceId) {
        continue;
      }
      if (this.resolveSourceTenantIdBySourceIdFromMemory(job.sourceId) !== tenantId) {
        continue;
      }

      const jobTimestamp = resolveLatestIsoTimestamp(
        job.endedAt,
        job.updatedAt,
        job.startedAt,
        job.createdAt
      );
      if (job.status === "success" && jobTimestamp && (!lastSuccessAt || jobTimestamp > lastSuccessAt)) {
        lastSuccessAt = jobTimestamp;
      }
      if (job.status === "failed") {
        failureCount += 1;
        if (jobTimestamp && (!lastFailureAt || jobTimestamp > lastFailureAt)) {
          lastFailureAt = jobTimestamp;
        }
      }

      if (
        (job.status === "success" || job.status === "failed") &&
        typeof job.durationMs === "number" &&
        Number.isFinite(job.durationMs) &&
        job.durationMs >= 0
      ) {
        latencyTotal += job.durationMs;
        latencyCount += 1;
      }
    }

    let latestWatermarkAt: string | null = null;
    for (const watermark of this.memorySourceWatermarks) {
      if (watermark.sourceId !== sourceId) {
        continue;
      }
      if (this.resolveSourceTenantIdBySourceIdFromMemory(watermark.sourceId) !== tenantId) {
        continue;
      }
      const updatedAt = toIsoString(watermark.updatedAt);
      if (!updatedAt) {
        continue;
      }
      if (!latestWatermarkAt || updatedAt > latestWatermarkAt) {
        latestWatermarkAt = updatedAt;
      }
    }

    const resolvedLastSuccessAt = resolveLatestIsoTimestamp(lastSuccessAt, latestWatermarkAt);

    return {
      sourceId,
      accessMode: source.accessMode,
      lastSuccessAt: resolvedLastSuccessAt,
      lastFailureAt,
      failureCount,
      avgLatencyMs:
        latencyCount > 0 ? Math.max(0, Math.round(latencyTotal / latencyCount)) : null,
      freshnessMinutes: computeFreshnessMinutes(resolvedLastSuccessAt),
    };
  }

  private deleteSourceByIdFromMemory(tenantId: string, id: string): DeleteSourceResult {
    const index = this.memorySources.findIndex(
      (source) => source.id === id && this.resolveSourceTenantIdFromMemory(source) === tenantId
    );
    if (index < 0) {
      return "not_found";
    }
    if (this.memorySessions.some((session) => session.sourceId === id)) {
      return "conflict";
    }
    this.memorySources.splice(index, 1);
    this.memorySourceTenantById.delete(id);
    for (let i = this.memorySyncJobs.length - 1; i >= 0; i -= 1) {
      if (this.memorySyncJobs[i]?.sourceId === id) {
        this.memorySyncJobs.splice(i, 1);
      }
    }
    for (let i = this.memorySessionEvents.length - 1; i >= 0; i -= 1) {
      if (this.memorySessionEvents[i]?.sourceId === id) {
        this.memorySessionEvents.splice(i, 1);
      }
    }
    for (let i = this.memorySourceWatermarks.length - 1; i >= 0; i -= 1) {
      if (this.memorySourceWatermarks[i]?.sourceId === id) {
        this.memorySourceWatermarks.splice(i, 1);
      }
    }
    for (let i = this.memorySourceParseFailures.length - 1; i >= 0; i -= 1) {
      if (this.memorySourceParseFailures[i]?.failure.sourceId === id) {
        this.memorySourceParseFailures.splice(i, 1);
      }
    }
    return "deleted";
  }

  private searchSessionsFromMemory(
    input: NormalizedSessionSearchInput
  ): SessionSearchResult {
    const keyword = input.keyword?.toLowerCase();
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;
    const sourceTenantById = new Map<string, string>();
    const sourceById = new Map<string, Source>();
    const sessionEventTextBySessionId = new Map<string, string>();
    for (const source of this.memorySources) {
      sourceTenantById.set(source.id, this.resolveSourceTenantIdFromMemory(source));
      sourceById.set(source.id, source);
    }
    if (keyword) {
      for (const event of this.memorySessionEvents) {
        if (input.tenantId) {
          const eventTenantId =
            sourceTenantById.get(event.sourceId) ?? DEFAULT_TENANT_ID;
          if (eventTenantId !== input.tenantId) {
            continue;
          }
        }
        const text = firstNonEmptyString(event.text);
        if (!text) {
          continue;
        }
        const current = sessionEventTextBySessionId.get(event.sessionId);
        sessionEventTextBySessionId.set(
          event.sessionId,
          current ? `${current}\n${text.toLowerCase()}` : text.toLowerCase()
        );
      }
    }

    let items = this.memorySessions.filter((session) => {
      if (input.tenantId && resolveSessionTenantId(session, sourceTenantById) !== input.tenantId) {
        return false;
      }
      if (input.sourceId && session.sourceId !== input.sourceId) {
        return false;
      }

      const dimensions = resolveSessionFilterDimensions(session, sourceById.get(session.sourceId));
      if (!matchesCaseInsensitiveFilter(dimensions.clientType, input.clientType)) {
        return false;
      }
      if (!matchesCaseInsensitiveFilter(dimensions.tool, input.tool)) {
        return false;
      }
      if (!matchesCaseInsensitiveFilter(dimensions.host, input.host)) {
        return false;
      }
      if (!matchesCaseInsensitiveFilter(dimensions.model, input.model)) {
        return false;
      }
      if (!matchesCaseInsensitiveFilter(dimensions.project, input.project)) {
        return false;
      }

      const startedAtTimestamp = Date.parse(session.startedAt);
      if (fromTimestamp !== undefined && startedAtTimestamp < fromTimestamp) {
        return false;
      }
      if (toTimestamp !== undefined && startedAtTimestamp > toTimestamp) {
        return false;
      }

      if (keyword) {
        const sessionRow = session as unknown as DbRow;
        const sessionKeywordTarget = [
          session.id,
          session.tool,
          session.model,
          firstNonEmptyString(sessionRow.native_session_id),
          firstNonEmptyString(sessionRow.provider),
        ]
          .filter((value): value is string => typeof value === "string")
          .join(" ")
          .toLowerCase();
        const eventKeywordTarget = sessionEventTextBySessionId.get(session.id) ?? "";
        if (
          !sessionKeywordTarget.includes(keyword) &&
          !eventKeywordTarget.includes(keyword)
        ) {
          return false;
        }
      }

      return true;
    });

    items = items.sort((a, b) => {
      const startedAtCompare = b.startedAt.localeCompare(a.startedAt);
      if (startedAtCompare !== 0) {
        return startedAtCompare;
      }
      return b.id.localeCompare(a.id);
    });
    const total = items.length;
    const cursor = decodeTimePaginationCursor(input.cursor);
    if (cursor) {
      const cursorTimestamp = Date.parse(cursor.timestamp);
      items = items.filter((session) => {
        const startedAtTimestamp = Date.parse(session.startedAt);
        if (Number.isFinite(startedAtTimestamp) && Number.isFinite(cursorTimestamp)) {
          if (startedAtTimestamp < cursorTimestamp) {
            return true;
          }
          if (startedAtTimestamp > cursorTimestamp) {
            return false;
          }
          return session.id < cursor.id;
        }
        const startedAtCompare = session.startedAt.localeCompare(cursor.timestamp);
        if (startedAtCompare < 0) {
          return true;
        }
        if (startedAtCompare > 0) {
          return false;
        }
        return session.id < cursor.id;
      });
    }

    const visibleItems = items.slice(0, input.limit + 1);
    const hasMore = visibleItems.length > input.limit;
    const pagedItems = hasMore ? visibleItems.slice(0, input.limit) : visibleItems;
    const lastItem = pagedItems[pagedItems.length - 1];
    const nextCursor =
      hasMore &&
      lastItem &&
      Number.isFinite(Date.parse(lastItem.startedAt)) &&
      lastItem.id.trim().length > 0
        ? encodeTimePaginationCursor({
            timestamp: lastItem.startedAt,
            id: lastItem.id,
          })
        : null;

    return {
      items: pagedItems,
      total,
      nextCursor,
    };
  }

  private getSessionByIdFromMemory(tenantId: string, sessionId: string): SessionDetail | null {
    const sourceById = new Map<string, Source>();
    for (const source of this.memorySources) {
      sourceById.set(source.id, source);
    }

    const matched = this.memorySessions.find((session) => {
      if (session.id !== sessionId) {
        return false;
      }
      const source = sourceById.get(session.sourceId);
      return (source ? this.resolveSourceTenantIdFromMemory(source) : DEFAULT_TENANT_ID) === tenantId;
    });
    if (!matched) {
      return null;
    }
    const matchedRow = matched as Session & {
      provider?: string;
      workspace?: string;
    };
    const tracePath = this.memorySessionEvents.find(
      (event) => event.sessionId === matched.id && typeof event.sourcePath === "string"
    )?.sourcePath;
    const source = sourceById.get(matched.sourceId);
    return {
      ...matched,
      provider: firstNonEmptyString(matchedRow.provider) ?? undefined,
      sourceName: source?.name,
      sourceType: source?.type,
      sourceLocation: source?.location,
      sourceHost: source?.sshConfig?.host,
      sourcePath: firstNonEmptyString(tracePath) ?? undefined,
      workspace: firstNonEmptyString(matchedRow.workspace) ?? undefined,
      messageCount: 0,
      inputTokens: 0,
      outputTokens: 0,
      cacheReadTokens: 0,
      cacheWriteTokens: 0,
      reasoningTokens: 0,
    };
  }

  private listSessionEventsFromMemory(
    tenantId: string,
    sessionId: string,
    limit: number,
    cursor: TimePaginationCursor | null
  ): SessionEventListResult {
    const sourceTenantById = new Map<string, string>();
    for (const source of this.memorySources) {
      sourceTenantById.set(source.id, this.resolveSourceTenantIdFromMemory(source));
    }

    const items = this.memorySessionEvents
      .map((event, index) => ({
        event,
        index,
      }))
      .filter(({ event }) => {
        if (event.sessionId !== sessionId) {
          return false;
        }
        const resolvedTenant = sourceTenantById.get(event.sourceId) ?? DEFAULT_TENANT_ID;
        return resolvedTenant === tenantId;
      })
      .map(({ event, index }) => {
        const timestamp = new Date(index * 1000).toISOString();
        return {
          id: `${event.sessionId}-${index}`,
          sessionId: event.sessionId,
          sourceId: event.sourceId,
          eventType: "message",
          role: "user",
          text: event.text,
          model: undefined,
          timestamp,
          inputTokens: 0,
          outputTokens: 0,
          cacheReadTokens: 0,
          cacheWriteTokens: 0,
          reasoningTokens: 0,
          cost: 0,
          sourcePath: firstNonEmptyString(event.sourcePath) ?? undefined,
          sourceOffset: undefined,
        } satisfies SessionEvent;
      })
      .sort((left, right) => {
        const timestampCompare = left.timestamp.localeCompare(right.timestamp);
        if (timestampCompare !== 0) {
          return timestampCompare;
        }
        return left.id.localeCompare(right.id);
      });

    const total = items.length;
    let pagedCandidates = items;
    if (cursor) {
      const cursorTimestamp = Date.parse(cursor.timestamp);
      pagedCandidates = items.filter((item) => {
        const itemTimestamp = Date.parse(item.timestamp);
        if (Number.isFinite(itemTimestamp) && Number.isFinite(cursorTimestamp)) {
          if (itemTimestamp > cursorTimestamp) {
            return true;
          }
          if (itemTimestamp < cursorTimestamp) {
            return false;
          }
          return item.id > cursor.id;
        }

        const timestampCompare = item.timestamp.localeCompare(cursor.timestamp);
        if (timestampCompare > 0) {
          return true;
        }
        if (timestampCompare < 0) {
          return false;
        }
        return item.id > cursor.id;
      });
    }

    const visibleItems = pagedCandidates.slice(0, limit + 1);
    const hasMore = visibleItems.length > limit;
    const outputItems = hasMore ? visibleItems.slice(0, limit) : visibleItems;
    const lastItem = outputItems[outputItems.length - 1];
    const nextCursor =
      hasMore &&
      lastItem &&
      Number.isFinite(Date.parse(lastItem.timestamp)) &&
      lastItem.id.trim().length > 0
        ? encodeTimePaginationCursor({
            timestamp: lastItem.timestamp,
            id: lastItem.id,
          })
        : null;

    return {
      items: outputItems,
      total,
      nextCursor,
    };
  }

  private listUsageDailyFromMemory(input: NormalizedUsageAggregateInput): UsageDailyItem[] {
    const sourceTenantById = new Map<string, string>();
    const sourceById = new Map<string, Source>();
    for (const source of this.memorySources) {
      sourceTenantById.set(source.id, this.resolveSourceTenantIdFromMemory(source));
      sourceById.set(source.id, source);
    }
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;
    const bucket = new Map<
      string,
      {
        date: string;
        tokens: number;
        sessions: number;
        costRaw: number;
        costEstimated: number;
        rawCount: number;
        reportedCount: number;
        estimatedCount: number;
      }
    >();

    for (const session of this.memorySessions) {
      if (resolveSessionTenantId(session, sourceTenantById) !== input.tenantId) {
        continue;
      }
      const startedAt = Date.parse(session.startedAt);
      if (!Number.isFinite(startedAt)) {
        continue;
      }
      if (fromTimestamp !== undefined && startedAt < fromTimestamp) {
        continue;
      }
      if (toTimestamp !== undefined && startedAt > toTimestamp) {
        continue;
      }
      if (input.project) {
        const dimensions = resolveSessionFilterDimensions(session, sourceById.get(session.sourceId));
        if (!matchesCaseInsensitiveFilter(dimensions.project, input.project)) {
          continue;
        }
      }

      const day = new Date(startedAt).toISOString().slice(0, 10);
      const dayKey = `${day}T00:00:00.000Z`;
      const sessionCost = resolveSessionUsageCostSnapshotFromRecord(
        session as unknown as DbRow,
        session.cost
      );
      const current = bucket.get(dayKey) ?? {
        date: dayKey,
        tokens: 0,
        sessions: 0,
        costRaw: 0,
        costEstimated: 0,
        rawCount: 0,
        reportedCount: 0,
        estimatedCount: 0,
      };
      current.tokens += Math.max(0, Math.trunc(session.tokens));
      current.sessions += 1;
      current.costRaw += sessionCost.costRaw;
      current.costEstimated += sessionCost.costEstimated;
      current.rawCount += sessionCost.modeCounters.raw;
      current.reportedCount += sessionCost.modeCounters.reported;
      current.estimatedCount += sessionCost.modeCounters.estimated;
      bucket.set(dayKey, current);
    }

    const dailyItems = [...bucket.values()]
      .sort((a, b) => a.date.localeCompare(b.date))
      .map((item) => {
        const cost = buildUsageCostComponents(item.costRaw, item.costEstimated, {
          raw: item.rawCount,
          reported: item.reportedCount,
          estimated: item.estimatedCount,
        });
        return {
          date: item.date,
          tokens: item.tokens,
          sessions: item.sessions,
          ...cost,
        };
      });

    return buildUsageDailyItems(dailyItems);
  }

  private listUsageMonthlyFromMemory(input: NormalizedUsageAggregateInput): UsageMonthlyItem[] {
    const sourceTenantById = new Map<string, string>();
    const sourceById = new Map<string, Source>();
    for (const source of this.memorySources) {
      sourceTenantById.set(source.id, this.resolveSourceTenantIdFromMemory(source));
      sourceById.set(source.id, source);
    }
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;
    const bucket = new Map<
      string,
      {
        month: string;
        tokens: number;
        sessions: number;
        costRaw: number;
        costEstimated: number;
        rawCount: number;
        reportedCount: number;
        estimatedCount: number;
      }
    >();

    for (const session of this.memorySessions) {
      if (resolveSessionTenantId(session, sourceTenantById) !== input.tenantId) {
        continue;
      }
      const startedAt = Date.parse(session.startedAt);
      if (!Number.isFinite(startedAt)) {
        continue;
      }
      if (fromTimestamp !== undefined && startedAt < fromTimestamp) {
        continue;
      }
      if (toTimestamp !== undefined && startedAt > toTimestamp) {
        continue;
      }
      if (input.project) {
        const dimensions = resolveSessionFilterDimensions(session, sourceById.get(session.sourceId));
        if (!matchesCaseInsensitiveFilter(dimensions.project, input.project)) {
          continue;
        }
      }
      const date = new Date(startedAt);
      const month = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}-01T00:00:00.000Z`;
      const sessionCost = resolveSessionUsageCostSnapshotFromRecord(
        session as unknown as DbRow,
        session.cost
      );
      const current = bucket.get(month) ?? {
        month,
        tokens: 0,
        sessions: 0,
        costRaw: 0,
        costEstimated: 0,
        rawCount: 0,
        reportedCount: 0,
        estimatedCount: 0,
      };
      current.tokens += Math.max(0, Math.trunc(session.tokens));
      current.sessions += 1;
      current.costRaw += sessionCost.costRaw;
      current.costEstimated += sessionCost.costEstimated;
      current.rawCount += sessionCost.modeCounters.raw;
      current.reportedCount += sessionCost.modeCounters.reported;
      current.estimatedCount += sessionCost.modeCounters.estimated;
      bucket.set(month, current);
    }

    return [...bucket.values()]
      .sort((a, b) => a.month.localeCompare(b.month))
      .map((item) => ({
        month: item.month,
        tokens: item.tokens,
        sessions: item.sessions,
        ...buildUsageCostComponents(item.costRaw, item.costEstimated, {
          raw: item.rawCount,
          reported: item.reportedCount,
          estimated: item.estimatedCount,
        }),
      }));
  }

  private listUsageModelRankingFromMemory(input: NormalizedUsageAggregateInput): UsageModelItem[] {
    const sourceTenantById = new Map<string, string>();
    const sourceById = new Map<string, Source>();
    for (const source of this.memorySources) {
      sourceTenantById.set(source.id, this.resolveSourceTenantIdFromMemory(source));
      sourceById.set(source.id, source);
    }
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;
    const bucket = new Map<
      string,
      {
        model: string;
        tokens: number;
        sessions: number;
        costRaw: number;
        costEstimated: number;
        rawCount: number;
        reportedCount: number;
        estimatedCount: number;
      }
    >();

    for (const session of this.memorySessions) {
      if (resolveSessionTenantId(session, sourceTenantById) !== input.tenantId) {
        continue;
      }
      const startedAt = Date.parse(session.startedAt);
      if (!Number.isFinite(startedAt)) {
        continue;
      }
      if (fromTimestamp !== undefined && startedAt < fromTimestamp) {
        continue;
      }
      if (toTimestamp !== undefined && startedAt > toTimestamp) {
        continue;
      }
      if (input.project) {
        const dimensions = resolveSessionFilterDimensions(session, sourceById.get(session.sourceId));
        if (!matchesCaseInsensitiveFilter(dimensions.project, input.project)) {
          continue;
        }
      }
      const model = firstNonEmptyString(session.model, "unknown") ?? "unknown";
      const sessionCost = resolveSessionUsageCostSnapshotFromRecord(
        session as unknown as DbRow,
        session.cost
      );
      const current = bucket.get(model) ?? {
        model,
        tokens: 0,
        sessions: 0,
        costRaw: 0,
        costEstimated: 0,
        rawCount: 0,
        reportedCount: 0,
        estimatedCount: 0,
      };
      current.tokens += Math.max(0, Math.trunc(session.tokens));
      current.sessions += 1;
      current.costRaw += sessionCost.costRaw;
      current.costEstimated += sessionCost.costEstimated;
      current.rawCount += sessionCost.modeCounters.raw;
      current.reportedCount += sessionCost.modeCounters.reported;
      current.estimatedCount += sessionCost.modeCounters.estimated;
      bucket.set(model, current);
    }

    return [...bucket.values()]
      .map((item) => ({
        model: item.model,
        tokens: item.tokens,
        sessions: item.sessions,
        ...buildUsageCostComponents(item.costRaw, item.costEstimated, {
          raw: item.rawCount,
          reported: item.reportedCount,
          estimated: item.estimatedCount,
        }),
      }))
      .sort((a, b) => b.cost - a.cost || b.tokens - a.tokens)
      .slice(0, input.limit)
      .map((item) => ({ ...item, cost: roundUsageCost(item.cost) }));
  }

  private listUsageSessionBreakdownFromMemory(
    input: NormalizedUsageAggregateInput
  ): UsageSessionBreakdownItem[] {
    const sourceTenantById = new Map<string, string>();
    const sourceById = new Map<string, Source>();
    for (const source of this.memorySources) {
      sourceTenantById.set(source.id, this.resolveSourceTenantIdFromMemory(source));
      sourceById.set(source.id, source);
    }
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;
    const items: UsageSessionBreakdownItem[] = [];

    for (const session of this.memorySessions) {
      if (resolveSessionTenantId(session, sourceTenantById) !== input.tenantId) {
        continue;
      }
      const startedAt = Date.parse(session.startedAt);
      if (!Number.isFinite(startedAt)) {
        continue;
      }
      if (fromTimestamp !== undefined && startedAt < fromTimestamp) {
        continue;
      }
      if (toTimestamp !== undefined && startedAt > toTimestamp) {
        continue;
      }
      if (input.project) {
        const dimensions = resolveSessionFilterDimensions(session, sourceById.get(session.sourceId));
        if (!matchesCaseInsensitiveFilter(dimensions.project, input.project)) {
          continue;
        }
      }

      const sessionCost = resolveSessionUsageCostSnapshotFromRecord(
        session as unknown as DbRow,
        session.cost
      );
      const { modeCounters: _modeCounters, ...cost } = sessionCost;

      items.push({
        sessionId: session.id,
        sourceId: session.sourceId,
        tool: session.tool,
        model: session.model,
        startedAt: session.startedAt,
        inputTokens: 0,
        outputTokens: 0,
        cacheReadTokens: 0,
        cacheWriteTokens: 0,
        reasoningTokens: 0,
        totalTokens: Math.max(0, Math.trunc(session.tokens)),
        ...cost,
      });
    }

    return items
      .sort((a, b) => b.startedAt.localeCompare(a.startedAt))
      .slice(0, input.limit);
  }

  private listPricingCatalogVersionsFromMemory(
    tenantId: string,
    limit: number
  ): PricingCatalog["version"][] {
    return this.memoryPricingCatalogVersions
      .filter((item) => item.tenantId === tenantId)
      .sort((a, b) => b.version - a.version)
      .slice(0, limit)
      .map((item) => ({
        id: item.id,
        tenantId: item.tenantId,
        version: item.version,
        note: item.note,
        createdAt: item.createdAt,
      }));
  }

  private getPricingCatalogFromMemory(tenantId: string): PricingCatalog | null {
    const version = this.listPricingCatalogVersionsFromMemory(tenantId, 1)[0];
    if (!version) {
      return null;
    }
    const entries = this.memoryPricingCatalogEntries
      .filter((item) => item.tenantId === tenantId && item.versionId === version.id)
      .sort((a, b) => a.model.localeCompare(b.model))
      .map((item) => ({
        model: item.model,
        inputPer1k: item.inputPer1k,
        outputPer1k: item.outputPer1k,
        cacheReadPer1k: item.cacheReadPer1k,
        cacheWritePer1k: item.cacheWritePer1k,
        reasoningPer1k: item.reasoningPer1k,
        currency: item.currency,
      }));
    return {
      version,
      entries,
    };
  }

  private upsertPricingCatalogToMemory(
    tenantId: string,
    input: PricingCatalogUpsertInput,
    now: string
  ): PricingCatalog {
    const current = this.listPricingCatalogVersionsFromMemory(tenantId, 1)[0];
    const nextVersion = (current?.version ?? 0) + 1;
    const versionId = crypto.randomUUID();
    const version: PricingCatalog["version"] = {
      id: versionId,
      tenantId,
      version: nextVersion,
      note: input.note,
      createdAt: now,
    };
    this.memoryPricingCatalogVersions.push({ ...version });

    for (const entry of input.entries) {
      this.memoryPricingCatalogEntries.push({
        tenantId,
        versionId,
        ...entry,
      });
    }

    return {
      version,
      entries: input.entries.map((entry) => ({ ...entry })),
    };
  }

  private listBudgetsFromMemory(tenantId: string): Budget[] {
    return this.memoryBudgets
      .filter((record) => record.tenantId === tenantId)
      .map((record) => ({
        ...record.budget,
        thresholds: { ...record.budget.thresholds },
      }))
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
  }

  private getBudgetByIdFromMemory(tenantId: string, budgetId: string): Budget | null {
    const target = this.memoryBudgets.find(
      (record) => record.tenantId === tenantId && record.budget.id === budgetId
    );
    if (!target) {
      return null;
    }
    return {
      ...target.budget,
      thresholds: { ...target.budget.thresholds },
    };
  }

  private upsertBudgetToMemory(
    tenantId: string,
    input: BudgetUpsertInput,
    updatedAt: string
  ): Budget {
    const sourceId = input.scope === "source" ? input.sourceId ?? "" : "";
    const organizationId = input.scope === "org" ? input.organizationId ?? "" : "";
    const userId = input.scope === "user" ? input.userId ?? "" : "";
    const model = input.scope === "model" ? input.model ?? "" : "";
    const thresholds = input.thresholds ?? {
      warning: toNumber(input.alertThreshold, 0.8),
      escalated: toNumber(input.alertThreshold, 0.8),
      critical: toNumber(input.alertThreshold, 0.8),
    };
    const existingRecord = this.memoryBudgets.find(
      (record) =>
        record.tenantId === tenantId &&
        record.budget.scope === input.scope &&
        record.budget.period === input.period &&
        (record.budget.scope === "source" ? record.budget.sourceId ?? "" : "") === sourceId &&
        (record.budget.scope === "org"
          ? record.budget.organizationId ?? ""
          : "") === organizationId &&
        (record.budget.scope === "user" ? record.budget.userId ?? "" : "") === userId &&
        (record.budget.scope === "model" ? record.budget.model ?? "" : "") === model
    );

    if (existingRecord) {
      existingRecord.budget = {
        ...existingRecord.budget,
        scope: input.scope,
        sourceId: input.scope === "source" ? sourceId : undefined,
        organizationId: input.scope === "org" ? organizationId : undefined,
        userId: input.scope === "user" ? userId : undefined,
        model: input.scope === "model" ? model : undefined,
        period: input.period,
        tokenLimit: input.tokenLimit,
        costLimit: input.costLimit,
        thresholds: {
          warning: thresholds.warning,
          escalated: thresholds.escalated,
          critical: thresholds.critical,
        },
        alertThreshold: thresholds.warning,
        enabled: existingRecord.budget.governanceState === "active",
        updatedAt,
      };
      return {
        ...existingRecord.budget,
        thresholds: { ...existingRecord.budget.thresholds },
      };
    }

    const created: Budget = {
      id: crypto.randomUUID(),
      scope: input.scope,
      sourceId: input.scope === "source" ? sourceId : undefined,
      organizationId: input.scope === "org" ? organizationId : undefined,
      userId: input.scope === "user" ? userId : undefined,
      model: input.scope === "model" ? model : undefined,
      period: input.period,
      tokenLimit: input.tokenLimit,
      costLimit: input.costLimit,
      thresholds: {
        warning: thresholds.warning,
        escalated: thresholds.escalated,
        critical: thresholds.critical,
      },
      alertThreshold: thresholds.warning,
      enabled: true,
      governanceState: "active",
      updatedAt,
    };

    this.memoryBudgets.push({
      tenantId,
      budget: created,
    });

    return {
      ...created,
      thresholds: { ...created.thresholds },
    };
  }

  private freezeBudgetInMemory(
    tenantId: string,
    budgetId: string,
    input: {
      reason: string;
      alertId?: string;
      frozenAt: string;
    }
  ): Budget | null {
    const target = this.memoryBudgets.find(
      (record) => record.tenantId === tenantId && record.budget.id === budgetId
    );
    if (!target) {
      return null;
    }

    target.budget = {
      ...target.budget,
      enabled: false,
      governanceState: "frozen",
      freezeReason: input.reason,
      frozenAt: input.frozenAt,
      frozenByAlertId: input.alertId,
      updatedAt: input.frozenAt,
    };

    return {
      ...target.budget,
      thresholds: { ...target.budget.thresholds },
    };
  }

  private markBudgetPendingReleaseInMemory(
    tenantId: string,
    budgetId: string,
    updatedAt: string
  ): Budget | null {
    const target = this.memoryBudgets.find(
      (record) => record.tenantId === tenantId && record.budget.id === budgetId
    );
    if (!target) {
      return null;
    }

    target.budget = {
      ...target.budget,
      enabled: false,
      governanceState: "pending_release",
      updatedAt,
    };
    return {
      ...target.budget,
      thresholds: { ...target.budget.thresholds },
    };
  }

  private restoreBudgetFrozenStateInMemory(
    tenantId: string,
    budgetId: string,
    updatedAt: string
  ): Budget | null {
    const target = this.memoryBudgets.find(
      (record) => record.tenantId === tenantId && record.budget.id === budgetId
    );
    if (!target) {
      return null;
    }

    target.budget = {
      ...target.budget,
      enabled: false,
      governanceState: "frozen",
      updatedAt,
    };
    return {
      ...target.budget,
      thresholds: { ...target.budget.thresholds },
    };
  }

  private activateBudgetInMemory(
    tenantId: string,
    budgetId: string,
    updatedAt: string
  ): Budget | null {
    const target = this.memoryBudgets.find(
      (record) => record.tenantId === tenantId && record.budget.id === budgetId
    );
    if (!target) {
      return null;
    }

    target.budget = {
      ...target.budget,
      enabled: true,
      governanceState: "active",
      freezeReason: undefined,
      frozenAt: undefined,
      frozenByAlertId: undefined,
      updatedAt,
    };
    return {
      ...target.budget,
      thresholds: { ...target.budget.thresholds },
    };
  }

  private cloneReleaseRequest(request: BudgetReleaseRequest): BudgetReleaseRequest {
    return {
      ...request,
      approvals: request.approvals.map((approval) => ({ ...approval })),
    };
  }

  private createBudgetReleaseRequestToMemory(
    request: BudgetReleaseRequest
  ): BudgetReleaseRequest {
    const existingPending = this.getPendingBudgetReleaseRequestFromMemory(
      request.tenantId,
      request.budgetId
    );
    if (existingPending) {
      return existingPending;
    }
    this.memoryBudgetReleaseRequests.unshift({
      tenantId: request.tenantId,
      request: this.cloneReleaseRequest(request),
    });
    return this.cloneReleaseRequest(request);
  }

  private getBudgetReleaseRequestByIdFromMemory(
    tenantId: string,
    budgetId: string,
    requestId: string
  ): BudgetReleaseRequest | null {
    const matched = this.memoryBudgetReleaseRequests.find(
      (record) =>
        record.tenantId === tenantId &&
        record.request.budgetId === budgetId &&
        record.request.id === requestId
    );
    return matched ? this.cloneReleaseRequest(matched.request) : null;
  }

  private listBudgetReleaseRequestsFromMemory(
    tenantId: string,
    budgetId: string,
    input?: ListBudgetReleaseRequestsInput
  ): BudgetReleaseRequest[] {
    const limit = Math.min(
      200,
      Math.max(1, Number.isInteger(input?.limit) ? Number(input?.limit) : 50)
    );
    const status = input?.status;

    const filtered = this.memoryBudgetReleaseRequests
      .filter((record) => {
        if (record.tenantId !== tenantId || record.request.budgetId !== budgetId) {
          return false;
        }
        if (status && record.request.status !== status) {
          return false;
        }
        return true;
      })
      .map((record) => this.cloneReleaseRequest(record.request))
      .sort((left, right) => {
        const requestedAtDelta = Date.parse(right.requestedAt) - Date.parse(left.requestedAt);
        if (requestedAtDelta !== 0) {
          return requestedAtDelta;
        }
        return Date.parse(right.updatedAt) - Date.parse(left.updatedAt);
      });

    return filtered.slice(0, limit);
  }

  private getPendingBudgetReleaseRequestFromMemory(
    tenantId: string,
    budgetId: string
  ): BudgetReleaseRequest | null {
    const matched = this.memoryBudgetReleaseRequests.find(
      (record) =>
        record.tenantId === tenantId &&
        record.request.budgetId === budgetId &&
        record.request.status === "pending"
    );
    return matched ? this.cloneReleaseRequest(matched.request) : null;
  }

  private approveBudgetReleaseRequestInMemory(
    tenantId: string,
    budgetId: string,
    requestId: string,
    approvals: BudgetReleaseRequest["approvals"],
    nextStatus: BudgetReleaseRequestStatus,
    updatedAt: string
  ): BudgetReleaseRequest | null {
    const matched = this.memoryBudgetReleaseRequests.find(
      (record) =>
        record.tenantId === tenantId &&
        record.request.budgetId === budgetId &&
        record.request.id === requestId
    );
    if (!matched || matched.request.status !== "pending") {
      return matched ? this.cloneReleaseRequest(matched.request) : null;
    }

    matched.request = {
      ...matched.request,
      status: nextStatus,
      approvals: approvals.map((approval) => ({ ...approval })),
      executedAt: nextStatus === "executed" ? updatedAt : matched.request.executedAt,
      updatedAt,
    };
    return this.cloneReleaseRequest(matched.request);
  }

  private rejectBudgetReleaseRequestInMemory(
    tenantId: string,
    budgetId: string,
    requestId: string,
    actorUserId: string,
    actorEmail: string | undefined,
    reason: string | undefined,
    rejectedAt: string
  ): BudgetReleaseRequest | null {
    const matched = this.memoryBudgetReleaseRequests.find(
      (record) =>
        record.tenantId === tenantId &&
        record.request.budgetId === budgetId &&
        record.request.id === requestId
    );
    if (!matched || matched.request.status !== "pending") {
      return matched ? this.cloneReleaseRequest(matched.request) : null;
    }

    matched.request = {
      ...matched.request,
      status: "rejected",
      rejectedByUserId: actorUserId,
      rejectedByEmail: actorEmail,
      rejectedReason: reason,
      rejectedAt,
      updatedAt: rejectedAt,
    };
    return this.cloneReleaseRequest(matched.request);
  }

  private getIntegrationAlertCallbackByIdFromMemory(
    tenantId: string,
    callbackId: string
  ): IntegrationAlertCallbackRecord | null {
    const matched = this.memoryIntegrationAlertCallbacks.find(
      (item) => item.tenantId === tenantId && item.callbackId === callbackId
    );
    return matched
      ? {
          ...matched,
          response: { ...matched.response },
        }
      : null;
  }

  private claimIntegrationAlertCallbackToMemory(
    record: IntegrationAlertCallbackRecord,
    staleAfterMs: number = DEFAULT_CALLBACK_CLAIM_STALE_AFTER_MS
  ): ClaimIntegrationAlertCallbackResult {
    const existing = this.getIntegrationAlertCallbackByIdFromMemory(
      record.tenantId,
      record.callbackId
    );
    if (existing) {
      const existingState = String(existing.response.state ?? "");
      const existingTs = Date.parse(existing.processedAt);
      const cutoffTs = Date.parse(record.processedAt) - staleAfterMs;
      const isStaleProcessing =
        existingState === "processing" &&
        Number.isFinite(existingTs) &&
        Number.isFinite(cutoffTs) &&
        existingTs <= cutoffTs;
      if (isStaleProcessing) {
        return {
          claimed: true,
          record: this.saveIntegrationAlertCallbackToMemory(record),
        };
      }
      return {
        claimed: false,
        record: existing,
      };
    }
    return {
      claimed: true,
      record: this.saveIntegrationAlertCallbackToMemory(record),
    };
  }

  private saveIntegrationAlertCallbackToMemory(
    record: IntegrationAlertCallbackRecord
  ): IntegrationAlertCallbackRecord {
    const existingIndex = this.memoryIntegrationAlertCallbacks.findIndex(
      (item) => item.tenantId === record.tenantId && item.callbackId === record.callbackId
    );
    if (existingIndex >= 0) {
      this.memoryIntegrationAlertCallbacks.splice(existingIndex, 1);
    }
    const stored: IntegrationAlertCallbackRecord = {
      ...record,
      response: { ...record.response },
    };
    this.memoryIntegrationAlertCallbacks.unshift(stored);
    return {
      ...stored,
      response: { ...stored.response },
    };
  }

  private cloneAlertOrchestrationRule(rule: AlertOrchestrationRule): AlertOrchestrationRule {
    return {
      ...rule,
      channels: [...rule.channels],
    };
  }

  private listAlertOrchestrationRulesFromMemory(
    tenantId: string,
    input: NormalizedAlertOrchestrationRuleListInput
  ): AlertOrchestrationRuleListResult {
    const items = this.memoryAlertOrchestrationRules
      .filter((rule) => {
        if (rule.tenantId !== tenantId) {
          return false;
        }
        if (input.eventType && rule.eventType !== input.eventType) {
          return false;
        }
        if (input.enabled !== undefined && rule.enabled !== input.enabled) {
          return false;
        }
        if (input.severity && rule.severity !== input.severity) {
          return false;
        }
        if (input.sourceId && rule.sourceId !== input.sourceId) {
          return false;
        }
        return true;
      })
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || a.id.localeCompare(b.id))
      .map((rule) => this.cloneAlertOrchestrationRule(rule));

    return {
      items,
      total: items.length,
    };
  }

  private getAlertOrchestrationRuleByIdFromMemory(
    tenantId: string,
    ruleId: string
  ): AlertOrchestrationRule | null {
    const matched = this.memoryAlertOrchestrationRules.find(
      (rule) => rule.tenantId === tenantId && rule.id === ruleId
    );
    return matched ? this.cloneAlertOrchestrationRule(matched) : null;
  }

  private upsertAlertOrchestrationRuleToMemory(
    tenantId: string,
    input: AlertOrchestrationRuleUpsertInput
  ): AlertOrchestrationRule {
    const normalizedTenantId = normalizeScopedTenantId(tenantId);
    const normalizedId = firstNonEmptyString(input.id) ?? crypto.randomUUID();
    const channels: AlertOrchestrationChannel[] = [];
    const channelSet = new Set<AlertOrchestrationChannel>();
    for (const channel of Array.isArray(input.channels) ? input.channels : []) {
      const normalizedChannel = firstNonEmptyString(channel);
      if (!normalizedChannel) {
        continue;
      }
      const mappedChannel = toAlertOrchestrationChannel(normalizedChannel.toLowerCase());
      if (!mappedChannel) {
        continue;
      }
      if (channelSet.has(mappedChannel)) {
        continue;
      }
      channelSet.add(mappedChannel);
      channels.push(mappedChannel);
    }

    const stored: AlertOrchestrationRule = {
      id: normalizedId,
      tenantId: normalizedTenantId,
      name: firstNonEmptyString(input.name) ?? normalizedId,
      enabled: input.enabled === true,
      eventType: toAlertOrchestrationEventType(input.eventType),
      severity: firstNonEmptyString(input.severity)
        ? toAlertSeverity(input.severity)
        : undefined,
      sourceId: firstNonEmptyString(input.sourceId) ?? undefined,
      dedupeWindowSeconds: toOptionalNonNegativeInteger(input.dedupeWindowSeconds) ?? 0,
      suppressionWindowSeconds: toOptionalNonNegativeInteger(input.suppressionWindowSeconds) ?? 0,
      mergeWindowSeconds: toOptionalNonNegativeInteger(input.mergeWindowSeconds) ?? 0,
      slaMinutes: toOptionalNonNegativeInteger(input.slaMinutes),
      channels,
      updatedAt: toIsoString(input.updatedAt) ?? new Date().toISOString(),
    };

    const existingIndex = this.memoryAlertOrchestrationRules.findIndex(
      (rule) => rule.tenantId === normalizedTenantId && rule.id === normalizedId
    );
    if (existingIndex >= 0) {
      this.memoryAlertOrchestrationRules.splice(existingIndex, 1);
    }
    this.memoryAlertOrchestrationRules.unshift(stored);
    return this.cloneAlertOrchestrationRule(stored);
  }

  private cloneAlertOrchestrationExecutionLog(
    execution: AlertOrchestrationExecutionLog
  ): AlertOrchestrationExecutionLog {
    return {
      ...execution,
      channels: [...execution.channels],
      conflictRuleIds: [...execution.conflictRuleIds],
      metadata: { ...execution.metadata },
    };
  }

  private listAlertOrchestrationExecutionLogsFromMemory(
    tenantId: string,
    input: NormalizedAlertOrchestrationExecutionListInput
  ): AlertOrchestrationExecutionListResult {
    const fromTimestamp = input.from ? Date.parse(input.from) : Number.NaN;
    const toTimestamp = input.to ? Date.parse(input.to) : Number.NaN;

    const filtered = this.memoryAlertOrchestrationExecutions
      .filter((execution) => {
        if (execution.tenantId !== tenantId) {
          return false;
        }
        if (input.ruleId && execution.ruleId !== input.ruleId) {
          return false;
        }
        if (input.eventType && execution.eventType !== input.eventType) {
          return false;
        }
        if (input.alertId && execution.alertId !== input.alertId) {
          return false;
        }
        if (input.severity && execution.severity !== input.severity) {
          return false;
        }
        if (input.sourceId && execution.sourceId !== input.sourceId) {
          return false;
        }
        if (input.dedupeHit !== undefined && execution.dedupeHit !== input.dedupeHit) {
          return false;
        }
        if (input.suppressed !== undefined && execution.suppressed !== input.suppressed) {
          return false;
        }
        if (input.dispatchMode && execution.dispatchMode !== input.dispatchMode) {
          return false;
        }
        if (
          input.hasConflict !== undefined &&
          (execution.conflictRuleIds.length > 0) !== input.hasConflict
        ) {
          return false;
        }
        if (input.simulated !== undefined && execution.simulated !== input.simulated) {
          return false;
        }
        if (Number.isFinite(fromTimestamp) || Number.isFinite(toTimestamp)) {
          const createdAtTs = Date.parse(execution.createdAt);
          if (!Number.isFinite(createdAtTs)) {
            return false;
          }
          if (Number.isFinite(fromTimestamp) && createdAtTs < fromTimestamp) {
            return false;
          }
          if (Number.isFinite(toTimestamp) && createdAtTs > toTimestamp) {
            return false;
          }
        }
        return true;
      })
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt) || b.id.localeCompare(a.id));

    return {
      items: filtered.slice(0, input.limit).map((item) => this.cloneAlertOrchestrationExecutionLog(item)),
      total: filtered.length,
    };
  }

  private createAlertOrchestrationExecutionLogToMemory(
    execution: AlertOrchestrationExecutionLog
  ): AlertOrchestrationExecutionLog {
    const stored = this.cloneAlertOrchestrationExecutionLog(execution);
    const existingIndex = this.memoryAlertOrchestrationExecutions.findIndex(
      (item) => item.tenantId === stored.tenantId && item.id === stored.id
    );
    if (existingIndex >= 0) {
      this.memoryAlertOrchestrationExecutions.splice(existingIndex, 1);
    }
    this.memoryAlertOrchestrationExecutions.unshift(stored);
    return this.cloneAlertOrchestrationExecutionLog(stored);
  }

  private cloneRuleScopeBinding(scopeBinding: RuleScopeBinding): RuleScopeBinding {
    return {
      organizations: scopeBinding.organizations ? [...scopeBinding.organizations] : undefined,
      projects: scopeBinding.projects ? [...scopeBinding.projects] : undefined,
      clients: scopeBinding.clients ? [...scopeBinding.clients] : undefined,
    };
  }

  private cloneTenantResidencyPolicy(policy: TenantResidencyPolicy): TenantResidencyPolicy {
    return {
      ...policy,
      replicaRegions: [...policy.replicaRegions],
    };
  }

  private getTenantResidencyPolicyFromMemory(tenantId: string): TenantResidencyPolicy | null {
    const matched = this.memoryResidencyPolicies.find((item) => item.tenantId === tenantId);
    return matched ? this.cloneTenantResidencyPolicy(matched) : null;
  }

  private upsertTenantResidencyPolicyToMemory(policy: TenantResidencyPolicy): TenantResidencyPolicy {
    const index = this.memoryResidencyPolicies.findIndex((item) => item.tenantId === policy.tenantId);
    if (index >= 0) {
      this.memoryResidencyPolicies.splice(index, 1);
    }
    this.memoryResidencyPolicies.unshift(this.cloneTenantResidencyPolicy(policy));
    return this.cloneTenantResidencyPolicy(policy);
  }

  private cloneReplicationJob(job: ReplicationJob): ReplicationJob {
    return {
      ...job,
      metadata: { ...job.metadata },
    };
  }

  private saveReplicationJobToMemory(job: ReplicationJob): ReplicationJob {
    const index = this.memoryReplicationJobs.findIndex(
      (item) => item.tenantId === job.tenantId && item.id === job.id
    );
    if (index >= 0) {
      this.memoryReplicationJobs.splice(index, 1);
    }
    this.memoryReplicationJobs.unshift(this.cloneReplicationJob(job));
    return this.cloneReplicationJob(job);
  }

  private listReplicationJobsFromMemory(
    tenantId: string,
    input: NormalizedReplicationJobListInput
  ): ReplicationJobListResult {
    const items = this.memoryReplicationJobs
      .filter((job) => {
        if (job.tenantId !== tenantId) {
          return false;
        }
        if (input.status && job.status !== input.status) {
          return false;
        }
        if (input.sourceRegion && job.sourceRegion !== input.sourceRegion) {
          return false;
        }
        if (input.targetRegion && job.targetRegion !== input.targetRegion) {
          return false;
        }
        return true;
      })
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt) || b.id.localeCompare(a.id))
      .slice(0, input.limit)
      .map((job) => this.cloneReplicationJob(job));
    return {
      items,
      total: items.length,
    };
  }

  private getReplicationJobByIdFromMemory(tenantId: string, jobId: string): ReplicationJob | null {
    const matched = this.memoryReplicationJobs.find(
      (job) => job.tenantId === tenantId && job.id === jobId
    );
    return matched ? this.cloneReplicationJob(matched) : null;
  }

  private cancelReplicationJobInMemory(
    tenantId: string,
    jobId: string,
    reason: string | undefined,
    userId: string | undefined,
    updatedAt: string
  ): ReplicationJob | null {
    const index = this.memoryReplicationJobs.findIndex(
      (job) => job.tenantId === tenantId && job.id === jobId
    );
    if (index < 0) {
      return null;
    }
    const current = this.memoryReplicationJobs[index];
    if (!current) {
      return null;
    }
    if (current.status !== "pending" && current.status !== "running") {
      return this.cloneReplicationJob(current);
    }
    const updated: ReplicationJob = {
      ...current,
      status: "cancelled",
      reason: reason ?? current.reason,
      approvedByUserId: userId ?? current.approvedByUserId,
      finishedAt: current.finishedAt ?? updatedAt,
      updatedAt,
      metadata: { ...current.metadata },
    };
    this.memoryReplicationJobs[index] = updated;
    return this.cloneReplicationJob(updated);
  }

  private approveReplicationJobInMemory(
    tenantId: string,
    jobId: string,
    reason: string | undefined,
    userId: string | undefined,
    updatedAt: string
  ): ReplicationJob | null {
    const index = this.memoryReplicationJobs.findIndex(
      (job) => job.tenantId === tenantId && job.id === jobId
    );
    if (index < 0) {
      return null;
    }
    const current = this.memoryReplicationJobs[index];
    if (!current) {
      return null;
    }
    if (current.status !== "pending") {
      return this.cloneReplicationJob(current);
    }
    const updated: ReplicationJob = {
      ...current,
      status: "running",
      reason: reason ?? current.reason,
      approvedByUserId: userId ?? current.approvedByUserId,
      startedAt: current.startedAt ?? updatedAt,
      updatedAt,
      metadata: { ...current.metadata },
    };
    this.memoryReplicationJobs[index] = updated;
    return this.cloneReplicationJob(updated);
  }

  private cloneRuleAsset(asset: RuleAsset): RuleAsset {
    return {
      ...asset,
      scopeBinding: this.cloneRuleScopeBinding(asset.scopeBinding),
    };
  }

  private saveRuleAssetToMemory(asset: RuleAsset): RuleAsset {
    const index = this.memoryRuleAssets.findIndex(
      (item) => item.tenantId === asset.tenantId && item.id === asset.id
    );
    if (index >= 0) {
      this.memoryRuleAssets.splice(index, 1);
    }
    this.memoryRuleAssets.unshift(this.cloneRuleAsset(asset));
    return this.cloneRuleAsset(asset);
  }

  private listRuleAssetsFromMemory(
    tenantId: string,
    input: NormalizedRuleAssetListInput
  ): RuleAssetListResult {
    const keyword = input.keyword?.toLowerCase();
    const items = this.memoryRuleAssets
      .filter((asset) => {
        if (asset.tenantId !== tenantId) {
          return false;
        }
        if (input.status && asset.status !== input.status) {
          return false;
        }
        if (keyword) {
          const name = asset.name.toLowerCase();
          const description = (asset.description ?? "").toLowerCase();
          if (!name.includes(keyword) && !description.includes(keyword)) {
            return false;
          }
        }
        return true;
      })
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || b.id.localeCompare(a.id))
      .slice(0, input.limit)
      .map((asset) => this.cloneRuleAsset(asset));
    return {
      items,
      total: items.length,
    };
  }

  private getRuleAssetByIdFromMemory(tenantId: string, assetId: string): RuleAsset | null {
    const matched = this.memoryRuleAssets.find(
      (asset) => asset.tenantId === tenantId && asset.id === assetId
    );
    return matched ? this.cloneRuleAsset(matched) : null;
  }

  private cloneRuleAssetVersion(version: RuleAssetVersion): RuleAssetVersion {
    return { ...version };
  }

  private listRuleAssetVersionsFromMemory(
    tenantId: string,
    assetId: string,
    limit: number
  ): RuleAssetVersion[] {
    return this.memoryRuleAssetVersions
      .filter((item) => item.tenantId === tenantId && item.assetId === assetId)
      .sort((a, b) => b.version - a.version || b.createdAt.localeCompare(a.createdAt))
      .slice(0, limit)
      .map((item) => this.cloneRuleAssetVersion(item));
  }

  private createRuleAssetVersionToMemory(
    tenantId: string,
    assetId: string,
    content: string,
    changelog: string | undefined,
    createdByUserId: string | undefined,
    createdAt: string
  ): RuleAssetVersion | null {
    const assetIndex = this.memoryRuleAssets.findIndex(
      (asset) => asset.tenantId === tenantId && asset.id === assetId
    );
    if (assetIndex < 0) {
      return null;
    }
    const currentMaxVersion = this.memoryRuleAssetVersions
      .filter((item) => item.tenantId === tenantId && item.assetId === assetId)
      .reduce((maxVersion, item) => Math.max(maxVersion, item.version), 0);
    const version: RuleAssetVersion = {
      id: crypto.randomUUID(),
      tenantId,
      assetId,
      version: currentMaxVersion + 1,
      content,
      changelog,
      createdByUserId,
      createdAt,
    };
    this.memoryRuleAssetVersions.unshift(this.cloneRuleAssetVersion(version));
    const currentAsset = this.memoryRuleAssets[assetIndex];
    if (currentAsset) {
      this.memoryRuleAssets[assetIndex] = {
        ...currentAsset,
        latestVersion: version.version,
        status: currentAsset.status === "deprecated" ? "draft" : currentAsset.status,
        updatedAt: createdAt,
        scopeBinding: this.cloneRuleScopeBinding(currentAsset.scopeBinding),
      };
    }
    return this.cloneRuleAssetVersion(version);
  }

  private publishRuleAssetVersionFromMemory(
    tenantId: string,
    assetId: string,
    version: number,
    updatedAt: string
  ): RuleAsset | null {
    const hasVersion = this.memoryRuleAssetVersions.some(
      (item) => item.tenantId === tenantId && item.assetId === assetId && item.version === version
    );
    const assetIndex = this.memoryRuleAssets.findIndex(
      (asset) => asset.tenantId === tenantId && asset.id === assetId
    );
    if (assetIndex < 0) {
      return null;
    }
    const asset = this.memoryRuleAssets[assetIndex];
    if (!asset) {
      return null;
    }
    if (!hasVersion) {
      return this.cloneRuleAsset(asset);
    }
    const next: RuleAsset = {
      ...asset,
      publishedVersion: version,
      status: "published",
      updatedAt,
      scopeBinding: this.cloneRuleScopeBinding(asset.scopeBinding),
    };
    this.memoryRuleAssets[assetIndex] = next;
    return this.cloneRuleAsset(next);
  }

  private cloneRuleApproval(approval: RuleApproval): RuleApproval {
    return { ...approval };
  }

  private listRuleApprovalsFromMemory(
    tenantId: string,
    assetId: string,
    input: NormalizedRuleApprovalListInput
  ): RuleApprovalListResult {
    const items = this.memoryRuleApprovals
      .filter((approval) => {
        if (approval.tenantId !== tenantId || approval.assetId !== assetId) {
          return false;
        }
        if (input.version !== undefined && approval.version !== input.version) {
          return false;
        }
        if (input.decision && approval.decision !== input.decision) {
          return false;
        }
        return true;
      })
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt) || b.id.localeCompare(a.id))
      .slice(0, input.limit)
      .map((approval) => this.cloneRuleApproval(approval));
    return {
      items,
      total: items.length,
    };
  }

  private createRuleApprovalToMemory(approval: RuleApproval): RuleApproval | null {
    const versionExists = this.memoryRuleAssetVersions.some(
      (item) =>
        item.tenantId === approval.tenantId &&
        item.assetId === approval.assetId &&
        item.version === approval.version
    );
    if (!versionExists) {
      return null;
    }
    const index = this.memoryRuleApprovals.findIndex(
      (item) =>
        item.tenantId === approval.tenantId &&
        item.assetId === approval.assetId &&
        item.version === approval.version &&
        item.approverUserId === approval.approverUserId
    );
    if (index >= 0) {
      this.memoryRuleApprovals.splice(index, 1);
    }
    this.memoryRuleApprovals.unshift(this.cloneRuleApproval(approval));
    return this.cloneRuleApproval(approval);
  }

  private cloneMcpToolPolicy(policy: McpToolPolicy): McpToolPolicy {
    return { ...policy };
  }

  private listMcpToolPoliciesFromMemory(
    tenantId: string,
    input: NormalizedMcpToolPolicyListInput
  ): McpToolPolicyListResult {
    const keyword = input.keyword?.toLowerCase();
    const items = this.memoryMcpToolPolicies
      .filter((policy) => {
        if (policy.tenantId !== tenantId) {
          return false;
        }
        if (input.riskLevel && policy.riskLevel !== input.riskLevel) {
          return false;
        }
        if (input.decision && policy.decision !== input.decision) {
          return false;
        }
        if (keyword) {
          const toolId = policy.toolId.toLowerCase();
          const reason = (policy.reason ?? "").toLowerCase();
          if (!toolId.includes(keyword) && !reason.includes(keyword)) {
            return false;
          }
        }
        return true;
      })
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || a.toolId.localeCompare(b.toolId))
      .slice(0, input.limit)
      .map((policy) => this.cloneMcpToolPolicy(policy));
    return {
      items,
      total: items.length,
    };
  }

  private getMcpToolPolicyByToolIdFromMemory(
    tenantId: string,
    toolId: string
  ): McpToolPolicy | null {
    const matched = this.memoryMcpToolPolicies.find(
      (policy) => policy.tenantId === tenantId && policy.toolId === toolId
    );
    return matched ? this.cloneMcpToolPolicy(matched) : null;
  }

  private upsertMcpToolPolicyToMemory(policy: McpToolPolicy): McpToolPolicy {
    const index = this.memoryMcpToolPolicies.findIndex(
      (item) => item.tenantId === policy.tenantId && item.toolId === policy.toolId
    );
    if (index >= 0) {
      this.memoryMcpToolPolicies.splice(index, 1);
    }
    this.memoryMcpToolPolicies.unshift(this.cloneMcpToolPolicy(policy));
    return this.cloneMcpToolPolicy(policy);
  }

  private cloneMcpApprovalRequest(request: McpApprovalRequest): McpApprovalRequest {
    return { ...request };
  }

  private saveMcpApprovalRequestToMemory(request: McpApprovalRequest): McpApprovalRequest {
    const index = this.memoryMcpApprovalRequests.findIndex(
      (item) => item.tenantId === request.tenantId && item.id === request.id
    );
    if (index >= 0) {
      this.memoryMcpApprovalRequests.splice(index, 1);
    }
    this.memoryMcpApprovalRequests.unshift(this.cloneMcpApprovalRequest(request));
    return this.cloneMcpApprovalRequest(request);
  }

  private getMcpApprovalRequestByIdFromMemory(
    tenantId: string,
    requestId: string
  ): McpApprovalRequest | null {
    const matched = this.memoryMcpApprovalRequests.find(
      (request) => request.tenantId === tenantId && request.id === requestId
    );
    return matched ? this.cloneMcpApprovalRequest(matched) : null;
  }

  private listMcpApprovalRequestsFromMemory(
    tenantId: string,
    status: McpApprovalStatus | undefined,
    limit: number
  ): McpApprovalRequestListResult {
    const items = this.memoryMcpApprovalRequests
      .filter((request) => {
        if (request.tenantId !== tenantId) {
          return false;
        }
        if (status && request.status !== status) {
          return false;
        }
        return true;
      })
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || b.id.localeCompare(a.id))
      .slice(0, limit)
      .map((request) => this.cloneMcpApprovalRequest(request));
    return {
      items,
      total: items.length,
    };
  }

  private reviewMcpApprovalRequestInMemory(
    tenantId: string,
    requestId: string,
    nextStatus: "approved" | "rejected",
    reviewedByUserId: string,
    reviewedByEmail: string | undefined,
    reviewReason: string | undefined,
    updatedAt: string
  ): McpApprovalRequest | null {
    const index = this.memoryMcpApprovalRequests.findIndex(
      (request) => request.tenantId === tenantId && request.id === requestId
    );
    if (index < 0) {
      return null;
    }
    const current = this.memoryMcpApprovalRequests[index];
    if (!current) {
      return null;
    }
    if (current.status !== "pending") {
      return this.cloneMcpApprovalRequest(current);
    }
    const next: McpApprovalRequest = {
      ...current,
      status: nextStatus,
      reviewedByUserId,
      reviewedByEmail,
      reviewReason,
      updatedAt,
    };
    this.memoryMcpApprovalRequests[index] = next;
    return this.cloneMcpApprovalRequest(next);
  }

  private cloneMcpInvocationAudit(invocation: McpInvocationAudit): McpInvocationAudit {
    return {
      ...invocation,
      metadata: { ...invocation.metadata },
    };
  }

  private saveMcpInvocationAuditToMemory(invocation: McpInvocationAudit): McpInvocationAudit {
    const index = this.memoryMcpInvocations.findIndex(
      (item) => item.tenantId === invocation.tenantId && item.id === invocation.id
    );
    if (index >= 0) {
      this.memoryMcpInvocations.splice(index, 1);
    }
    this.memoryMcpInvocations.unshift(this.cloneMcpInvocationAudit(invocation));
    return this.cloneMcpInvocationAudit(invocation);
  }

  private listMcpInvocationAuditsFromMemory(
    tenantId: string,
    input: NormalizedMcpInvocationListInput
  ): McpInvocationListResult {
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;
    const items = this.memoryMcpInvocations
      .filter((invocation) => {
        if (invocation.tenantId !== tenantId) {
          return false;
        }
        if (input.toolId && invocation.toolId !== input.toolId) {
          return false;
        }
        if (input.decision && invocation.decision !== input.decision) {
          return false;
        }
        const createdAtTimestamp = Date.parse(invocation.createdAt);
        if (fromTimestamp !== undefined && createdAtTimestamp < fromTimestamp) {
          return false;
        }
        if (toTimestamp !== undefined && createdAtTimestamp > toTimestamp) {
          return false;
        }
        return true;
      })
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt) || b.id.localeCompare(a.id))
      .slice(0, input.limit)
      .map((invocation) => this.cloneMcpInvocationAudit(invocation));
    return {
      items,
      total: items.length,
    };
  }

  private listAlertsFromMemory(
    tenantId: string,
    input: NormalizedAlertListInput
  ): AlertListResult {
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;
    const cursor = decodeTimePaginationCursor(input.cursor);

    let items = this.memoryAlerts.filter((alert) => {
      if (alert.tenantId !== tenantId) {
        return false;
      }
      if (input.status && alert.status !== input.status) {
        return false;
      }
      if (input.severity && alert.severity !== input.severity) {
        return false;
      }
      if (input.sourceId && alert.sourceId !== input.sourceId) {
        return false;
      }

      const triggeredAtTimestamp = Date.parse(alert.triggeredAt);
      if (fromTimestamp !== undefined && triggeredAtTimestamp < fromTimestamp) {
        return false;
      }
      if (toTimestamp !== undefined && triggeredAtTimestamp > toTimestamp) {
        return false;
      }
      return true;
    });

    items = items.sort(
      (a, b) => b.triggeredAt.localeCompare(a.triggeredAt) || b.id.localeCompare(a.id)
    );
    const total = items.length;
    if (cursor) {
      const cursorTimestamp = Date.parse(cursor.timestamp);
      items = items.filter((alert) => {
        const triggeredAtTimestamp = Date.parse(alert.triggeredAt);
        if (Number.isFinite(triggeredAtTimestamp) && Number.isFinite(cursorTimestamp)) {
          if (triggeredAtTimestamp < cursorTimestamp) {
            return true;
          }
          if (triggeredAtTimestamp > cursorTimestamp) {
            return false;
          }
          return alert.id < cursor.id;
        }

        const triggeredAtCompare = alert.triggeredAt.localeCompare(cursor.timestamp);
        if (triggeredAtCompare < 0) {
          return true;
        }
        if (triggeredAtCompare > 0) {
          return false;
        }
        return alert.id < cursor.id;
      });
    }

    const hasMore = items.length > input.limit;
    const pagedItems = (hasMore ? items.slice(0, input.limit) : items).map((alert) => ({ ...alert }));
    const lastItem = pagedItems[pagedItems.length - 1];
    const nextCursor =
      hasMore && lastItem
        ? encodeTimePaginationCursor({
            timestamp: lastItem.triggeredAt,
            id: lastItem.id,
          })
        : null;

    return {
      items: pagedItems,
      total,
      nextCursor,
    };
  }

  private getAlertByIdFromMemory(tenantId: string, alertId: string): Alert | null {
    const target = this.memoryAlerts.find(
      (alert) => alert.tenantId === tenantId && alert.id === alertId
    );
    return target ? { ...target } : null;
  }

  private updateAlertStatusInMemory(
    tenantId: string,
    alertId: string,
    status: AlertMutableStatus,
    updatedAt: string
  ): Alert | null {
    const target = this.memoryAlerts.find(
      (alert) => alert.tenantId === tenantId && alert.id === alertId
    );
    if (!target) {
      return null;
    }

    if (!canTransitAlertStatus(target.status, status)) {
      return { ...target };
    }

    target.status = status;
    target.updatedAt = updatedAt;
    return { ...target };
  }

  private saveAuditToMemory(audit: AuditItem): AuditItem {
    const stored: AuditItem = {
      ...audit,
      metadata: { ...audit.metadata },
    };
    this.memoryAudits.unshift(stored);
    return {
      ...stored,
      metadata: { ...stored.metadata },
    };
  }

  private listAuditsFromMemory(input: NormalizedAuditListInput): AuditListResult {
    const keyword = input.keyword?.toLowerCase();
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;
    const cursor = decodeTimePaginationCursor(input.cursor);

    let items = this.memoryAudits.filter((audit) => {
      if (input.tenantId) {
        const metadataTenantId = firstNonEmptyString(
          audit.metadata.tenant_id,
          audit.metadata.tenantId
        );
        if (metadataTenantId) {
          if (metadataTenantId !== input.tenantId) {
            return false;
          }
        } else if (input.tenantId !== DEFAULT_TENANT_ID) {
          return false;
        }
      }
      if (input.eventId && audit.eventId !== input.eventId) {
        return false;
      }
      if (input.action && audit.action !== input.action) {
        return false;
      }
      if (input.level && audit.level !== input.level) {
        return false;
      }

      const createdAtTimestamp = Date.parse(audit.createdAt);
      if (fromTimestamp !== undefined && createdAtTimestamp < fromTimestamp) {
        return false;
      }
      if (toTimestamp !== undefined && createdAtTimestamp > toTimestamp) {
        return false;
      }

      if (keyword) {
        const metadataText = safeStringifyJson(audit.metadata).toLowerCase();
        const target = `${audit.eventId} ${audit.action} ${audit.level} ${audit.detail}`.toLowerCase();
        if (!target.includes(keyword) && !metadataText.includes(keyword)) {
          return false;
        }
      }
      return true;
    });

    items = items.sort(
      (a, b) => b.createdAt.localeCompare(a.createdAt) || b.id.localeCompare(a.id)
    );
    const total = items.length;
    if (cursor) {
      const cursorTimestamp = Date.parse(cursor.timestamp);
      items = items.filter((audit) => {
        const createdAtTimestamp = Date.parse(audit.createdAt);
        if (Number.isFinite(createdAtTimestamp) && Number.isFinite(cursorTimestamp)) {
          if (createdAtTimestamp < cursorTimestamp) {
            return true;
          }
          if (createdAtTimestamp > cursorTimestamp) {
            return false;
          }
          return audit.id < cursor.id;
        }

        const createdAtCompare = audit.createdAt.localeCompare(cursor.timestamp);
        if (createdAtCompare < 0) {
          return true;
        }
        if (createdAtCompare > 0) {
          return false;
        }
        return audit.id < cursor.id;
      });
    }
    const hasMore = items.length > input.limit;
    const pagedItems = (hasMore ? items.slice(0, input.limit) : items).map((audit) => ({
      ...audit,
      metadata: { ...audit.metadata },
    }));
    const lastItem = pagedItems[pagedItems.length - 1];
    const nextCursor =
      hasMore && lastItem
        ? encodeTimePaginationCursor({
            timestamp: lastItem.createdAt,
            id: lastItem.id,
          })
        : null;

    return {
      items: pagedItems,
      total,
      nextCursor,
    };
  }

  private ensureDefaultTenantInMemory(): void {
    if (this.memoryTenants.some((tenant) => tenant.id === DEFAULT_TENANT_ID)) {
      return;
    }
    const now = new Date().toISOString();
    this.memoryTenants.unshift(createDefaultTenant(now));
  }

  private createLocalUserInMemory(user: LocalUser): LocalUser {
    const existing = this.memoryUsers.find(
      (item) => normalizeEmail(item.email) === normalizeEmail(user.email)
    );
    if (existing) {
      existing.passwordHash = user.passwordHash;
      existing.displayName = user.displayName;
      existing.updatedAt = user.updatedAt;
      return { ...existing };
    }

    this.memoryUsers.unshift({ ...user });
    return { ...user };
  }

  private getLocalUserByEmailFromMemory(email: string): LocalUser | null {
    const matched = this.memoryUsers.find(
      (user) => normalizeEmail(user.email) === normalizeEmail(email)
    );
    return matched ? { ...matched } : null;
  }

  private getUserByIdFromMemory(id: string): LocalUser | null {
    const matched = this.memoryUsers.find((user) => user.id === id);
    return matched ? { ...matched } : null;
  }

  private createAuthSessionInMemory(session: AuthSession): AuthSession {
    const index = this.memoryAuthSessions.findIndex(
      (item) => item.sessionToken === session.sessionToken
    );
    if (index >= 0) {
      this.memoryAuthSessions.splice(index, 1);
    }
    this.memoryAuthSessions.unshift({ ...session });
    return { ...session };
  }

  private getAuthSessionByIdFromMemory(id: string): AuthSession | null {
    const matched = this.memoryAuthSessions.find((session) => session.id === id);
    return matched ? { ...matched } : null;
  }

  private rotateAuthSessionFromMemory(
    currentSessionId: string,
    nextSessionId: string,
    nextSessionToken: string,
    nextExpiresAt: string,
    rotatedAt: string
  ): AuthSession | null {
    const currentSession = this.memoryAuthSessions.find(
      (session) => session.id === currentSessionId && session.revokedAt === null
    );
    if (!currentSession) {
      return null;
    }

    currentSession.revokedAt = rotatedAt;
    currentSession.replacedBySessionId = nextSessionId;
    currentSession.updatedAt = rotatedAt;

    const nextSession: AuthSession = {
      id: nextSessionId,
      userId: currentSession.userId,
      tenantId: currentSession.tenantId,
      sessionToken: nextSessionToken,
      expiresAt: nextExpiresAt,
      revokedAt: null,
      replacedBySessionId: null,
      createdAt: rotatedAt,
      updatedAt: rotatedAt,
    };

    const sameTokenIndex = this.memoryAuthSessions.findIndex(
      (session) =>
        session.sessionToken === nextSessionToken && session.id !== currentSessionId
    );
    if (sameTokenIndex >= 0) {
      this.memoryAuthSessions.splice(sameTokenIndex, 1);
    }
    this.memoryAuthSessions.unshift(nextSession);
    return { ...nextSession };
  }

  private revokeAuthSessionFromMemory(id: string, revokedAt: string): boolean {
    const current = this.memoryAuthSessions.find((session) => session.id === id);
    if (!current || current.revokedAt !== null) {
      return false;
    }

    current.revokedAt = revokedAt;
    current.updatedAt = revokedAt;
    return true;
  }

  private listTenantsFromMemory(): Tenant[] {
    this.ensureDefaultTenantInMemory();
    return [...this.memoryTenants]
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id))
      .map((tenant) => ({ ...tenant }));
  }

  private createTenantInMemory(tenant: Tenant): Tenant {
    this.ensureDefaultTenantInMemory();
    const existing = this.memoryTenants.find((item) => item.id === tenant.id);
    if (existing) {
      throw new Error(`tenant_already_exists:${tenant.id}`);
    }

    this.memoryTenants.push({ ...tenant });
    return { ...tenant };
  }

  private listOrganizationsFromMemory(tenantId: string): Organization[] {
    return this.memoryOrganizations
      .filter((organization) => organization.tenantId === tenantId)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id))
      .map((organization) => ({ ...organization }));
  }

  private getOrganizationByIdFromMemory(
    tenantId: string,
    organizationId: string
  ): Organization | null {
    const matched = this.memoryOrganizations.find(
      (organization) =>
        organization.tenantId === tenantId && organization.id === organizationId
    );
    return matched ? { ...matched } : null;
  }

  private createOrganizationInMemory(organization: Organization): Organization {
    this.ensureDefaultTenantInMemory();
    if (!this.memoryTenants.some((tenant) => tenant.id === organization.tenantId)) {
      this.memoryTenants.push({
        id: organization.tenantId,
        name: organization.tenantId,
        createdAt: organization.createdAt,
        updatedAt: organization.updatedAt,
      });
    }
    this.memoryOrganizations.push({ ...organization });
    return { ...organization };
  }

  private listTenantMembersFromMemory(tenantId: string): TenantMember[] {
    return this.memoryTenantMembers
      .filter((membership) => membership.tenantId === tenantId)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id))
      .map((membership) => ({ ...membership }));
  }

  private addTenantMemberToMemory(membership: TenantMember): TenantMember {
    this.ensureDefaultTenantInMemory();
    const existing = this.memoryTenantMembers.find(
      (item) => item.tenantId === membership.tenantId && item.userId === membership.userId
    );
    if (existing) {
      existing.tenantRole = membership.tenantRole;
      existing.organizationId = membership.organizationId;
      existing.orgRole = membership.orgRole;
      existing.updatedAt = membership.updatedAt;
      return { ...existing };
    }

    this.memoryTenantMembers.push({ ...membership });
    return { ...membership };
  }

  private getTenantMemberByUserFromMemory(
    tenantId: string,
    userId: string
  ): TenantMember | null {
    const matched = this.memoryTenantMembers.find(
      (membership) => membership.tenantId === tenantId && membership.userId === userId
    );
    return matched ? { ...matched } : null;
  }

  private listDeviceBindingsFromMemory(tenantId: string): DeviceBinding[] {
    return this.memoryDeviceBindings
      .filter((binding) => binding.tenantId === tenantId)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id))
      .map(cloneDeviceBinding);
  }

  private createDeviceBindingInMemory(binding: DeviceBinding): DeviceBinding {
    const existing = this.memoryDeviceBindings.find(
      (item) => item.tenantId === binding.tenantId && item.deviceId === binding.deviceId
    );
    if (existing) {
      throw new Error(
        `device_binding_already_exists:${binding.tenantId}:${binding.deviceId}`
      );
    }

    this.memoryDeviceBindings.push(cloneDeviceBinding(binding));
    return cloneDeviceBinding(binding);
  }

  private deleteDeviceBindingFromMemory(tenantId: string, deviceId: string): boolean {
    const index = this.memoryDeviceBindings.findIndex(
      (binding) => binding.tenantId === tenantId && binding.deviceId === deviceId
    );
    if (index < 0) {
      return false;
    }
    this.memoryDeviceBindings.splice(index, 1);
    return true;
  }

  private listAgentBindingsFromMemory(tenantId: string): AgentBinding[] {
    return this.memoryAgentBindings
      .filter((binding) => binding.tenantId === tenantId)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id))
      .map(cloneAgentBinding);
  }

  private createAgentBindingInMemory(binding: AgentBinding): AgentBinding {
    if (
      binding.deviceId &&
      !this.memoryDeviceBindings.some(
        (item) => item.tenantId === binding.tenantId && item.deviceId === binding.deviceId
      )
    ) {
      throw new Error(`agent_binding_device_not_found:${binding.tenantId}:${binding.deviceId}`);
    }

    const existing = this.memoryAgentBindings.find(
      (item) => item.tenantId === binding.tenantId && item.agentId === binding.agentId
    );
    if (existing) {
      throw new Error(`agent_binding_already_exists:${binding.tenantId}:${binding.agentId}`);
    }

    this.memoryAgentBindings.push(cloneAgentBinding(binding));
    return cloneAgentBinding(binding);
  }

  private deleteAgentBindingFromMemory(tenantId: string, agentId: string): boolean {
    const index = this.memoryAgentBindings.findIndex(
      (binding) => binding.tenantId === tenantId && binding.agentId === agentId
    );
    if (index < 0) {
      return false;
    }
    this.memoryAgentBindings.splice(index, 1);
    return true;
  }

  private hasSourceInTenantFromMemory(tenantId: string, sourceId: string): boolean {
    return this.memorySources.some(
      (source) =>
        source.id === sourceId && this.resolveSourceTenantIdFromMemory(source) === tenantId
    );
  }

  private listSourceBindingsFromMemory(tenantId: string): SourceBinding[] {
    return this.memorySourceBindings
      .filter((binding) => binding.tenantId === tenantId)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id))
      .map(cloneSourceBinding);
  }

  private createSourceBindingInMemory(binding: SourceBinding): SourceBinding {
    if (!this.hasSourceInTenantFromMemory(binding.tenantId, binding.sourceId)) {
      throw new Error(
        `source_binding_source_not_found:${binding.tenantId}:${binding.sourceId}`
      );
    }

    if (
      binding.deviceId &&
      !this.memoryDeviceBindings.some(
        (item) => item.tenantId === binding.tenantId && item.deviceId === binding.deviceId
      )
    ) {
      throw new Error(`source_binding_device_not_found:${binding.tenantId}:${binding.deviceId}`);
    }

    if (
      binding.agentId &&
      !this.memoryAgentBindings.some(
        (item) => item.tenantId === binding.tenantId && item.agentId === binding.agentId
      )
    ) {
      throw new Error(`source_binding_agent_not_found:${binding.tenantId}:${binding.agentId}`);
    }

    const existing = this.memorySourceBindings.find(
      (item) => item.tenantId === binding.tenantId && item.sourceId === binding.sourceId
    );
    if (existing) {
      throw new Error(
        `source_binding_already_exists:${binding.tenantId}:${binding.sourceId}`
      );
    }

    this.memorySourceBindings.push(cloneSourceBinding(binding));
    return cloneSourceBinding(binding);
  }

  private deleteSourceBindingFromMemory(tenantId: string, bindingId: string): boolean {
    const index = this.memorySourceBindings.findIndex(
      (binding) => binding.tenantId === tenantId && binding.id === bindingId
    );
    if (index < 0) {
      return false;
    }
    this.memorySourceBindings.splice(index, 1);
    return true;
  }

  private listUsageHeatmapFromMemory(input: NormalizedUsageHeatmapInput): HeatmapCell[] {
    const sourceTenantById = new Map<string, string>();
    for (const source of this.memorySources) {
      sourceTenantById.set(source.id, this.resolveSourceTenantIdFromMemory(source));
    }

    return aggregateHeatmap(this.memorySessions, input, sourceTenantById);
  }
}

const controlPlaneRepository = new ControlPlaneRepository();

export function getControlPlaneRepository(): ControlPlaneRepository {
  return controlPlaneRepository;
}
