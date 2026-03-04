import type {
  Alert,
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
  HeatmapCell,
  IntegrationAlertCallbackAction,
  PricingCatalog,
  PricingCatalogEntry,
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
  UsageDailyItem,
  UsageCostMode,
  UsageModelItem,
  UsageMonthlyItem,
  UsageSessionBreakdownItem,
  OrgRole,
  TenantRole,
} from "../contracts";

const DEFAULT_SESSION_LIMIT = 50;
const DEFAULT_ALERT_LIMIT = 50;
const DEFAULT_AUDIT_LIMIT = 50;
const DEFAULT_SYNC_JOB_LIMIT = 50;
const DEFAULT_PARSE_FAILURE_LIMIT = 50;
const MAX_PARSE_FAILURE_LIMIT = 500;
const SOURCE_TYPES: ReadonlyArray<SourceType> = ["local", "ssh", "sync-cache"];
const SOURCE_ACCESS_MODES: ReadonlyArray<SourceAccessMode> = ["realtime", "sync", "hybrid"];
const ALERT_STATUS_SET: ReadonlyArray<AlertStatus> = ["open", "acknowledged", "resolved"];
const ALERT_SEVERITY_SET: ReadonlyArray<AlertSeverity> = [
  "warning",
  "critical",
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

function toAuditLevel(value: unknown): AuditLevel {
  if (typeof value === "string" && AUDIT_LEVEL_SET.includes(value as AuditLevel)) {
    return value as AuditLevel;
  }
  return "info";
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
  };
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
    limit,
  };
}

function normalizeScopedTenantId(tenantId: string | undefined): string {
  return firstNonEmptyString(tenantId) ?? DEFAULT_TENANT_ID;
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

  async listAlerts(tenantId: string, input: AlertListInput): Promise<Alert[]> {
    const normalized = normalizeAlertListInput(input);
    const pool = await this.getPool();
    if (!pool) {
      return this.listAlertsFromMemory(tenantId, normalized);
    }

    try {
      const params: unknown[] = [tenantId];
      const whereClauses: string[] = ["tenant_id = $1"];

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

      const listParams = [...params, normalized.limit];
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
         WHERE ${whereClauses.join(" AND ")}
         ORDER BY created_at DESC, updated_at DESC
         LIMIT $${listParams.length}`,
        listParams
      );

      return result.rows.map(mapAlertRow);
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

      const listParams = [...params, normalized.limit];
      const result = await pool.query(
        `SELECT id,
                event_id,
                action,
                level,
                detail,
                metadata,
                created_at
         FROM audit_logs
         ${whereSql}
         ORDER BY created_at DESC, id DESC
         LIMIT $${listParams.length}`,
        listParams
      );

      return {
        items: result.rows.map(mapAuditRow),
        total: Math.max(0, Math.trunc(toNumber(countResult.rows[0]?.total, 0))),
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
    for (const source of this.memorySources) {
      sourceTenantById.set(source.id, this.resolveSourceTenantIdFromMemory(source));
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
    for (const source of this.memorySources) {
      sourceTenantById.set(source.id, this.resolveSourceTenantIdFromMemory(source));
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
    for (const source of this.memorySources) {
      sourceTenantById.set(source.id, this.resolveSourceTenantIdFromMemory(source));
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
    for (const source of this.memorySources) {
      sourceTenantById.set(source.id, this.resolveSourceTenantIdFromMemory(source));
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

  private listAlertsFromMemory(
    tenantId: string,
    input: NormalizedAlertListInput
  ): Alert[] {
    const fromTimestamp = input.from ? Date.parse(input.from) : undefined;
    const toTimestamp = input.to ? Date.parse(input.to) : undefined;

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

    items = items.sort((a, b) => b.triggeredAt.localeCompare(a.triggeredAt));
    return items.slice(0, input.limit).map((alert) => ({ ...alert }));
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

    const filtered = this.memoryAudits.filter((audit) => {
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

    const items = [...filtered]
      .sort((a, b) => {
        const createdAtDiff = b.createdAt.localeCompare(a.createdAt);
        if (createdAtDiff !== 0) {
          return createdAtDiff;
        }
        return b.id.localeCompare(a.id);
      })
      .slice(0, input.limit)
      .map((audit) => ({
        ...audit,
        metadata: { ...audit.metadata },
      }));

    return {
      items,
      total: filtered.length,
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
