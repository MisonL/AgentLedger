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

export type BudgetPeriod = "daily" | "monthly";
export type BudgetScope = "global" | "source" | "org" | "user" | "model";
export type BudgetGovernanceState = "active" | "frozen" | "pending_release";
export type AlertStatus = "open" | "acknowledged" | "resolved";
export type AlertMutableStatus = "acknowledged" | "resolved";
export type AlertSeverity = "warning" | "critical";
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

export interface AuditItem {
  id: string;
  eventId: string;
  action: string;
  level: AuditLevel;
  detail: string;
  metadata: Record<string, unknown>;
  createdAt: string;
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
}

export type ExportFormat = "json" | "csv";

export interface SessionExportQueryInput extends SessionSearchInput {
  format: ExportFormat;
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
}

export interface AlertStatusUpdateInput {
  status: AlertMutableStatus;
}

export interface AuditListInput {
  level?: AuditLevel;
  from?: string;
  to?: string;
  limit?: number;
}

export interface AuditListResponse {
  items: AuditItem[];
  total: number;
  filters: AuditListInput;
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
