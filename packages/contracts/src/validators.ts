import type {
  AgentItem,
  AgentListInput,
  AgentListResponse,
  AddTenantMemberInput,
  Alert,
  AlertMutableStatus,
  AlertListInput,
  AlertStatusUpdateInput,
  AuthLoginInput,
  AuthLogoutInput,
  AuthRefreshInput,
  AuthRegisterInput,
  AuditExportQueryInput,
  AuditItem,
  AuditLevel,
  AuditListInput,
  AlertSeverity,
  AlertStatus,
  Budget,
  BudgetGovernanceState,
  BudgetReleaseRequest,
  BudgetReleaseRequestStatus,
  BudgetThresholds,
  BudgetUpsertInput,
  CreateAgentInput,
  CreateBudgetReleaseRequestInput,
  CreateDeviceInput,
  CreateOrganizationInput,
  CreateSourceInput,
  CreateSourceBindingInput,
  CreateTenantInput,
  DeleteAgentInput,
  DeleteDeviceInput,
  DeleteSourceBindingInput,
  DeviceItem,
  DeviceListInput,
  DeviceListResponse,
  ExportFormat,
  HeatmapCell,
  IntegrationAlertCallbackAction,
  IntegrationAlertCallbackInput,
  OrgRole,
  RejectBudgetReleaseRequestInput,
  SSHAuthType,
  SSHConfig,
  Session,
  SessionDetail,
  SessionDetailResponse,
  SessionEvent,
  SessionSearchResponse,
  SessionSourceFreshness,
  SessionSourceTrace,
  SessionTokenBreakdown,
  SessionExportJobCreateInput,
  SessionExportJobStatus,
  SessionExportQueryInput,
  SessionSearchInput,
  SourceBindingItem,
  SourceBindingListInput,
  SourceBindingListResponse,
  SourceBindingMethod,
  SystemConfigBackupPayload,
  SystemConfigBackupSource,
  SystemConfigRestoreInput,
  Source,
  SourceHealth,
  SourceAccessMode,
  SourceParseFailure,
  SourceParseFailureListResponse,
  SourceParseFailureQueryInput,
  SourceType,
  SyncJob,
  SyncJobStatus,
  TenantRole,
  PricingCatalog,
  PricingCatalogEntry,
  UsageDailyItem,
  UsageModelItem,
  UsageMonthlyItem,
  UsageSessionBreakdownItem,
  UsageCostMode,
} from "./types";

export type ValidationResult<T> =
  | { success: true; data: T }
  | { success: false; error: string };

const SOURCE_TYPE_SET = new Set<SourceType>(["local", "ssh", "sync-cache"]);
const SOURCE_ACCESS_MODE_SET = new Set<SourceAccessMode>(["realtime", "sync", "hybrid"]);
const SSH_AUTH_TYPE_SET = new Set<SSHAuthType>(["key", "agent"]);
const SOURCE_BINDING_METHOD_SET = new Set<SourceBindingMethod>(["ssh-pull", "agent-push"]);
const BUDGET_SCOPE_SET = new Set(["global", "source", "org", "user", "model"]);
const BUDGET_GOVERNANCE_STATE_SET = new Set<BudgetGovernanceState>([
  "active",
  "frozen",
  "pending_release",
]);
const BUDGET_PERIOD_SET = new Set(["daily", "monthly"]);
const BUDGET_RELEASE_REQUEST_STATUS_SET = new Set<BudgetReleaseRequestStatus>([
  "pending",
  "rejected",
  "executed",
]);
const INTEGRATION_ALERT_CALLBACK_ACTION_SET = new Set<IntegrationAlertCallbackAction>([
  "ack",
  "resolve",
  "request_release",
  "approve_release",
  "reject_release",
]);
const ALERT_STATUS_SET = new Set<AlertStatus>(["open", "acknowledged", "resolved"]);
const ALERT_MUTABLE_STATUS_SET = new Set<AlertMutableStatus>([
  "acknowledged",
  "resolved",
]);
const ALERT_SEVERITY_SET = new Set<AlertSeverity>(["warning", "critical"]);
const AUDIT_LEVEL_SET = new Set<AuditLevel>(["info", "warning", "error", "critical"]);
const EXPORT_FORMAT_SET = new Set<ExportFormat>(["json", "csv"]);
const SESSION_EXPORT_JOB_STATUS_SET = new Set<SessionExportJobStatus>([
  "pending",
  "running",
  "completed",
  "failed",
]);
const SYNC_JOB_STATUS_SET = new Set<SyncJobStatus>([
  "pending",
  "running",
  "success",
  "failed",
  "cancelled",
]);
const TENANT_ROLE_SET = new Set<TenantRole>(["owner", "maintainer", "member", "readonly"]);
const ORG_ROLE_SET = new Set<OrgRole>(["owner", "maintainer", "member", "readonly"]);
const USAGE_COST_MODE_SET = new Set<UsageCostMode>([
  "raw",
  "estimated",
  "reported",
  "mixed",
  "none",
]);
const SESSION_LIMIT_MAX = 200;
const SOURCE_PARSE_FAILURE_LIMIT_DEFAULT = 50;
const SOURCE_PARSE_FAILURE_LIMIT_MAX = 500;
const ALERT_LIMIT_MAX = 200;
const AUDIT_LIMIT_MAX = 200;
const IDENTITY_BINDING_LIST_LIMIT_MAX = 200;
const PASSWORD_MIN_LENGTH = 8;
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const SLUG_PATTERN = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
const LOCAL_SOURCE_WHITELIST = [
  "~/.codex/sessions",
  "~/.claude/projects",
  "~/.gemini/tmp",
  "~/.aider/sessions",
  "~/.opencode/sessions",
  "~/.qwen-code/sessions",
  "~/.kimi-cli/sessions",
  "~/.trae-cli/sessions",
  "~/.codebuddy-cli/sessions",
  "~/.cursor/sessions",
  "~/.vscode/sessions",
  "~/.trae-ide/sessions",
  "~/.windsurf/sessions",
  "~/.lingma/sessions",
  "~/.codebuddy-ide/sessions",
  "~/.zed/sessions",
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function isNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function normalizeString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeSourcePath(value: string): string {
  return value.trim().replace(/\\/g, "/").replace(/\/+$/, "");
}

function matchesLocalWhitelist(path: string): boolean {
  const normalized = normalizeSourcePath(path).toLowerCase();
  for (const allowed of LOCAL_SOURCE_WHITELIST) {
    const allowedPath = normalizeSourcePath(allowed).toLowerCase();
    if (
      normalized === allowedPath ||
      normalized.startsWith(`${allowedPath}/`)
    ) {
      return true;
    }
  }
  return false;
}

function isISODate(value: unknown): value is string {
  return typeof value === "string" && !Number.isNaN(Date.parse(value));
}

function isEmail(value: string): boolean {
  return EMAIL_PATTERN.test(value);
}

function isSlug(value: string): boolean {
  return SLUG_PATTERN.test(value);
}

function toOptionalInteger(value: unknown): number | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : undefined;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.length === 0) {
      return undefined;
    }
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function normalizeThresholdNumber(value: unknown): number | undefined {
  if (!isNumber(value)) {
    return undefined;
  }
  if (value < 0 || value > 1) {
    return undefined;
  }
  return Number(value.toFixed(4));
}

function normalizeBudgetThresholds(
  input: unknown
): BudgetThresholds | undefined | "invalid" {
  if (input === undefined || input === null) {
    return undefined;
  }
  if (!isRecord(input)) {
    return "invalid";
  }

  const warning = normalizeThresholdNumber(input.warning);
  const escalated = normalizeThresholdNumber(input.escalated);
  const critical = normalizeThresholdNumber(input.critical);

  if (warning === undefined || escalated === undefined || critical === undefined) {
    return "invalid";
  }
  if (warning > escalated || escalated > critical) {
    return "invalid";
  }

  return {
    warning,
    escalated,
    critical,
  };
}

export function isAlertStatus(value: unknown): value is AlertStatus {
  return typeof value === "string" && ALERT_STATUS_SET.has(value as AlertStatus);
}

export function isAlertMutableStatus(value: unknown): value is AlertMutableStatus {
  return (
    typeof value === "string" &&
    ALERT_MUTABLE_STATUS_SET.has(value as AlertMutableStatus)
  );
}

export function isAlertSeverity(value: unknown): value is AlertSeverity {
  return typeof value === "string" && ALERT_SEVERITY_SET.has(value as AlertSeverity);
}

export function isAuditLevel(value: unknown): value is AuditLevel {
  return typeof value === "string" && AUDIT_LEVEL_SET.has(value as AuditLevel);
}

export function isExportFormat(value: unknown): value is ExportFormat {
  return typeof value === "string" && EXPORT_FORMAT_SET.has(value as ExportFormat);
}

export function isSessionExportJobStatus(value: unknown): value is SessionExportJobStatus {
  return (
    typeof value === "string" &&
    SESSION_EXPORT_JOB_STATUS_SET.has(value as SessionExportJobStatus)
  );
}

export function isSourceType(value: unknown): value is SourceType {
  return typeof value === "string" && SOURCE_TYPE_SET.has(value as SourceType);
}

export function isSourceAccessMode(value: unknown): value is SourceAccessMode {
  return (
    typeof value === "string" &&
    SOURCE_ACCESS_MODE_SET.has(value as SourceAccessMode)
  );
}

export function isSourceBindingMethod(value: unknown): value is SourceBindingMethod {
  return (
    typeof value === "string" &&
    SOURCE_BINDING_METHOD_SET.has(value as SourceBindingMethod)
  );
}

export function isSSHAuthType(value: unknown): value is SSHAuthType {
  return typeof value === "string" && SSH_AUTH_TYPE_SET.has(value as SSHAuthType);
}

export function isSyncJobStatus(value: unknown): value is SyncJobStatus {
  return typeof value === "string" && SYNC_JOB_STATUS_SET.has(value as SyncJobStatus);
}

export function isTenantRole(value: unknown): value is TenantRole {
  return typeof value === "string" && TENANT_ROLE_SET.has(value as TenantRole);
}

export function isOrgRole(value: unknown): value is OrgRole {
  return typeof value === "string" && ORG_ROLE_SET.has(value as OrgRole);
}

export function isBudgetGovernanceState(value: unknown): value is BudgetGovernanceState {
  return (
    typeof value === "string" &&
    BUDGET_GOVERNANCE_STATE_SET.has(value as BudgetGovernanceState)
  );
}

export function isBudgetReleaseRequestStatus(
  value: unknown
): value is BudgetReleaseRequestStatus {
  return (
    typeof value === "string" &&
    BUDGET_RELEASE_REQUEST_STATUS_SET.has(value as BudgetReleaseRequestStatus)
  );
}

export function isIntegrationAlertCallbackAction(
  value: unknown
): value is IntegrationAlertCallbackAction {
  return (
    typeof value === "string" &&
    INTEGRATION_ALERT_CALLBACK_ACTION_SET.has(value as IntegrationAlertCallbackAction)
  );
}

function normalizeSSHConfig(input: unknown): SSHConfig | undefined | "invalid" {
  if (input === undefined || input === null) {
    return undefined;
  }
  if (!isRecord(input)) {
    return "invalid";
  }

  const host = normalizeString(input.host);
  const user = normalizeString(input.user);
  const authType = normalizeString(input.authType) ?? "key";
  const keyPath = normalizeString(input.keyPath);
  const knownHostsPath = normalizeString(input.knownHostsPath);
  const portRaw = toOptionalInteger(input.port);
  const port = portRaw ?? 22;

  if (!host || !user) {
    return "invalid";
  }
  if (!isSSHAuthType(authType)) {
    return "invalid";
  }
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    return "invalid";
  }
  if (authType === "key" && !keyPath) {
    return "invalid";
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

export function isSource(value: unknown): value is Source {
  if (!isRecord(value)) {
    return false;
  }
  const syncCronOk =
    value.syncCron === undefined || value.syncCron === null || isString(value.syncCron);
  const syncRetentionDaysOk =
    value.syncRetentionDays === undefined ||
    value.syncRetentionDays === null ||
    (isNumber(value.syncRetentionDays) &&
      Number.isInteger(value.syncRetentionDays) &&
      value.syncRetentionDays >= 0);
  const sshConfig = normalizeSSHConfig(value.sshConfig);
  const sshConfigOk = sshConfig !== "invalid";

  return (
    isString(value.id) &&
    isString(value.name) &&
    isSourceType(value.type) &&
    isString(value.location) &&
    isSourceAccessMode(value.accessMode) &&
    syncCronOk &&
    syncRetentionDaysOk &&
    sshConfigOk &&
    typeof value.enabled === "boolean" &&
    isISODate(value.createdAt)
  );
}

export function isSourceHealth(value: unknown): value is SourceHealth {
  if (!isRecord(value)) {
    return false;
  }

  const lastSuccessAtOk =
    value.lastSuccessAt === null ||
    value.lastSuccessAt === undefined ||
    isISODate(value.lastSuccessAt);
  const lastFailureAtOk =
    value.lastFailureAt === null ||
    value.lastFailureAt === undefined ||
    isISODate(value.lastFailureAt);
  const avgLatencyOk =
    value.avgLatencyMs === null ||
    value.avgLatencyMs === undefined ||
    (isNumber(value.avgLatencyMs) && value.avgLatencyMs >= 0);
  const freshnessOk =
    value.freshnessMinutes === null ||
    value.freshnessMinutes === undefined ||
    (isNumber(value.freshnessMinutes) &&
      Number.isInteger(value.freshnessMinutes) &&
      value.freshnessMinutes >= 0);

  return (
    isString(value.sourceId) &&
    isSourceAccessMode(value.accessMode) &&
    lastSuccessAtOk &&
    lastFailureAtOk &&
    isNumber(value.failureCount) &&
    Number.isInteger(value.failureCount) &&
    value.failureCount >= 0 &&
    avgLatencyOk &&
    freshnessOk
  );
}

export function isSourceParseFailure(value: unknown): value is SourceParseFailure {
  if (!isRecord(value)) {
    return false;
  }

  const sourcePathOk =
    value.sourcePath === undefined || value.sourcePath === null || isString(value.sourcePath);
  const sourceOffsetOk =
    value.sourceOffset === undefined ||
    value.sourceOffset === null ||
    (isNumber(value.sourceOffset) && Number.isInteger(value.sourceOffset) && value.sourceOffset >= 0);
  const rawHashOk = value.rawHash === undefined || value.rawHash === null || isString(value.rawHash);

  return (
    isString(value.id) &&
    isString(value.sourceId) &&
    isString(value.parserKey) &&
    isString(value.errorCode) &&
    isString(value.errorMessage) &&
    sourcePathOk &&
    sourceOffsetOk &&
    rawHashOk &&
    isRecord(value.metadata) &&
    isISODate(value.failedAt) &&
    isISODate(value.createdAt)
  );
}

export function isSourceParseFailureListResponse(
  value: unknown
): value is SourceParseFailureListResponse {
  if (!isRecord(value)) {
    return false;
  }

  const filtersOk =
    isRecord(value.filters) && validateSourceParseFailureQueryInput(value.filters).success;

  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isSourceParseFailure(item)) &&
    isNumber(value.total) &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

export function isSyncJob(value: unknown): value is SyncJob {
  if (!isRecord(value)) {
    return false;
  }

  const errorOk = value.error === undefined || value.error === null || isString(value.error);
  const triggerOk = value.trigger === undefined || value.trigger === null || isString(value.trigger);
  const attemptOk =
    value.attempt === undefined ||
    value.attempt === null ||
    (isNumber(value.attempt) && Number.isInteger(value.attempt) && value.attempt >= 0);
  const startedAtOk =
    value.startedAt === undefined || value.startedAt === null || isISODate(value.startedAt);
  const endedAtOk =
    value.endedAt === undefined || value.endedAt === null || isISODate(value.endedAt);
  const nextRunAtOk =
    value.nextRunAt === undefined || value.nextRunAt === null || isISODate(value.nextRunAt);
  const durationMsOk =
    value.durationMs === undefined ||
    value.durationMs === null ||
    (isNumber(value.durationMs) && Number.isInteger(value.durationMs) && value.durationMs >= 0);
  const errorCodeOk =
    value.errorCode === undefined || value.errorCode === null || isString(value.errorCode);
  const errorDetailOk =
    value.errorDetail === undefined || value.errorDetail === null || isString(value.errorDetail);
  const cancelRequestedOk =
    value.cancelRequested === undefined ||
    value.cancelRequested === null ||
    typeof value.cancelRequested === "boolean";

  return (
    isString(value.id) &&
    isString(value.sourceId) &&
    isSourceAccessMode(value.mode) &&
    isSyncJobStatus(value.status) &&
    errorOk &&
    triggerOk &&
    attemptOk &&
    startedAtOk &&
    endedAtOk &&
    nextRunAtOk &&
    durationMsOk &&
    errorCodeOk &&
    errorDetailOk &&
    cancelRequestedOk &&
    isISODate(value.createdAt) &&
    isISODate(value.updatedAt)
  );
}

export function isSession(value: unknown): value is Session {
  if (!isRecord(value)) {
    return false;
  }

  const endedAtValid =
    value.endedAt === undefined ||
    value.endedAt === null ||
    isISODate(value.endedAt);

  return (
    isString(value.id) &&
    isString(value.sourceId) &&
    isString(value.tool) &&
    isString(value.model) &&
    isISODate(value.startedAt) &&
    endedAtValid &&
    isNumber(value.tokens) &&
    isNumber(value.cost)
  );
}

export function isSessionEvent(value: unknown): value is SessionEvent {
  if (!isRecord(value)) {
    return false;
  }
  const roleOk = value.role === undefined || value.role === null || isString(value.role);
  const textOk = value.text === undefined || value.text === null || typeof value.text === "string";
  const modelOk = value.model === undefined || value.model === null || typeof value.model === "string";
  const sourcePathOk =
    value.sourcePath === undefined || value.sourcePath === null || typeof value.sourcePath === "string";
  const sourceOffsetOk =
    value.sourceOffset === undefined ||
    value.sourceOffset === null ||
    (isNumber(value.sourceOffset) && Number.isInteger(value.sourceOffset) && value.sourceOffset >= 0);

  return (
    isString(value.id) &&
    isString(value.sessionId) &&
    isString(value.sourceId) &&
    isString(value.eventType) &&
    roleOk &&
    textOk &&
    modelOk &&
    isISODate(value.timestamp) &&
    isNumber(value.inputTokens) &&
    isNumber(value.outputTokens) &&
    isNumber(value.cacheReadTokens) &&
    isNumber(value.cacheWriteTokens) &&
    isNumber(value.reasoningTokens) &&
    isNumber(value.cost) &&
    sourcePathOk &&
    sourceOffsetOk
  );
}

export function isSessionDetail(value: unknown): value is SessionDetail {
  if (!isRecord(value) || !isSession(value)) {
    return false;
  }
  const providerOk = value.provider === undefined || value.provider === null || isString(value.provider);
  const sourceNameOk = value.sourceName === undefined || value.sourceName === null || isString(value.sourceName);
  const sourceTypeOk = value.sourceType === undefined || value.sourceType === null || isSourceType(value.sourceType);
  const sourceLocationOk =
    value.sourceLocation === undefined || value.sourceLocation === null || isString(value.sourceLocation);
  const sourceHostOk = value.sourceHost === undefined || value.sourceHost === null || isString(value.sourceHost);
  const sourcePathOk = value.sourcePath === undefined || value.sourcePath === null || isString(value.sourcePath);
  const workspaceOk = value.workspace === undefined || value.workspace === null || typeof value.workspace === "string";

  return (
    providerOk &&
    sourceNameOk &&
    sourceTypeOk &&
    sourceLocationOk &&
    sourceHostOk &&
    sourcePathOk &&
    workspaceOk &&
    isNumber(value.messageCount) &&
    isNumber(value.inputTokens) &&
    isNumber(value.outputTokens) &&
    isNumber(value.cacheReadTokens) &&
    isNumber(value.cacheWriteTokens) &&
    isNumber(value.reasoningTokens)
  );
}

export function isSessionTokenBreakdown(value: unknown): value is SessionTokenBreakdown {
  if (!isRecord(value)) {
    return false;
  }

  return (
    isNumber(value.inputTokens) &&
    isNumber(value.outputTokens) &&
    isNumber(value.cacheReadTokens) &&
    isNumber(value.cacheWriteTokens) &&
    isNumber(value.reasoningTokens) &&
    isNumber(value.totalTokens)
  );
}

export function isSessionSourceTrace(value: unknown): value is SessionSourceTrace {
  if (!isRecord(value)) {
    return false;
  }

  const sourceNameOk = value.sourceName === undefined || value.sourceName === null || isString(value.sourceName);
  const providerOk = value.provider === undefined || value.provider === null || isString(value.provider);
  const pathOk = value.path === undefined || value.path === null || isString(value.path);

  return isString(value.sourceId) && sourceNameOk && providerOk && pathOk;
}

export function isSessionSourceFreshness(value: unknown): value is SessionSourceFreshness {
  if (!isRecord(value)) {
    return false;
  }

  const sourceNameOk = value.sourceName === undefined || value.sourceName === null || isString(value.sourceName);
  const lastSuccessAtOk = value.lastSuccessAt === null || isISODate(value.lastSuccessAt);
  const lastFailureAtOk = value.lastFailureAt === null || isISODate(value.lastFailureAt);
  const avgLatencyOk =
    value.avgLatencyMs === null ||
    (isNumber(value.avgLatencyMs) && value.avgLatencyMs >= 0);
  const freshnessOk =
    value.freshnessMinutes === null ||
    (isNumber(value.freshnessMinutes) &&
      Number.isInteger(value.freshnessMinutes) &&
      value.freshnessMinutes >= 0);

  return (
    isString(value.sourceId) &&
    sourceNameOk &&
    isSourceAccessMode(value.accessMode) &&
    lastSuccessAtOk &&
    lastFailureAtOk &&
    isNumber(value.failureCount) &&
    Number.isInteger(value.failureCount) &&
    value.failureCount >= 0 &&
    avgLatencyOk &&
    freshnessOk
  );
}

export function isSessionSearchResponse(value: unknown): value is SessionSearchResponse {
  if (!isRecord(value)) {
    return false;
  }

  const nextCursorOk = value.nextCursor === null || isString(value.nextCursor);
  const filtersOk = isRecord(value.filters) && validateSessionSearchInput(value.filters).success;
  const sourceFreshnessOk =
    value.sourceFreshness === undefined ||
    (Array.isArray(value.sourceFreshness) &&
      value.sourceFreshness.every((item) => isSessionSourceFreshness(item)));

  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isSession(item)) &&
    isNumber(value.total) &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    nextCursorOk &&
    filtersOk &&
    sourceFreshnessOk
  );
}

export function isSessionDetailResponse(value: unknown): value is SessionDetailResponse {
  if (!isRecord(value)) {
    return false;
  }

  return (
    isSessionDetail(value.session) &&
    isSessionTokenBreakdown(value.tokenBreakdown) &&
    isSessionSourceTrace(value.sourceTrace)
  );
}

export function isHeatmapCell(value: unknown): value is HeatmapCell {
  if (!isRecord(value)) {
    return false;
  }
  return (
    isISODate(value.date) &&
    isNumber(value.tokens) &&
    isNumber(value.cost) &&
    isNumber(value.sessions)
  );
}

export function isUsageCostMode(value: unknown): value is UsageCostMode {
  return typeof value === "string" && USAGE_COST_MODE_SET.has(value as UsageCostMode);
}

function isUsageCostSourceRecord(value: Record<string, unknown>): boolean {
  return (
    isNumber(value.costRaw) &&
    value.costRaw >= 0 &&
    isNumber(value.costEstimated) &&
    value.costEstimated >= 0 &&
    isUsageCostMode(value.costMode)
  );
}

export function isUsageDailyItem(value: unknown): value is UsageDailyItem {
  if (!isRecord(value) || !isRecord(value.change)) {
    return false;
  }

  const changeTokensOk = value.change.tokens === null || isNumber(value.change.tokens);
  const changeCostOk = value.change.cost === null || isNumber(value.change.cost);
  const changeSessionsOk = value.change.sessions === null || isNumber(value.change.sessions);

  return (
    isISODate(value.date) &&
    isNumber(value.tokens) &&
    isNumber(value.cost) &&
    value.cost >= 0 &&
    isNumber(value.sessions) &&
    isUsageCostSourceRecord(value) &&
    changeTokensOk &&
    changeCostOk &&
    changeSessionsOk
  );
}

export function isUsageMonthlyItem(value: unknown): value is UsageMonthlyItem {
  if (!isRecord(value)) {
    return false;
  }

  return (
    isISODate(value.month) &&
    isNumber(value.tokens) &&
    isNumber(value.cost) &&
    value.cost >= 0 &&
    isNumber(value.sessions) &&
    isUsageCostSourceRecord(value)
  );
}

export function isUsageModelItem(value: unknown): value is UsageModelItem {
  if (!isRecord(value)) {
    return false;
  }

  return (
    isString(value.model) &&
    isNumber(value.tokens) &&
    isNumber(value.cost) &&
    value.cost >= 0 &&
    isNumber(value.sessions) &&
    isUsageCostSourceRecord(value)
  );
}

export function isUsageSessionBreakdownItem(
  value: unknown
): value is UsageSessionBreakdownItem {
  if (!isRecord(value)) {
    return false;
  }

  return (
    isString(value.sessionId) &&
    isString(value.sourceId) &&
    isString(value.tool) &&
    isString(value.model) &&
    isISODate(value.startedAt) &&
    isNumber(value.inputTokens) &&
    isNumber(value.outputTokens) &&
    isNumber(value.cacheReadTokens) &&
    isNumber(value.cacheWriteTokens) &&
    isNumber(value.reasoningTokens) &&
    isNumber(value.totalTokens) &&
    isNumber(value.cost) &&
    value.cost >= 0 &&
    isUsageCostSourceRecord(value)
  );
}

export function isBudget(value: unknown): value is Budget {
  if (!isRecord(value)) {
    return false;
  }

  const scope = value.scope;
  const period = value.period;
  const sourceId = normalizeString(value.sourceId);
  const organizationId = normalizeString(value.organizationId);
  const userId = normalizeString(value.userId);
  const model = normalizeString(value.model);
  const sourceIdOk =
    scope === "source"
      ? Boolean(sourceId)
      : value.sourceId === undefined || value.sourceId === null || isString(value.sourceId);
  const organizationIdOk =
    scope === "org"
      ? Boolean(organizationId)
      : value.organizationId === undefined ||
        value.organizationId === null ||
        isString(value.organizationId);
  const userIdOk =
    scope === "user"
      ? Boolean(userId)
      : value.userId === undefined || value.userId === null || isString(value.userId);
  const modelOk =
    scope === "model"
      ? Boolean(model)
      : value.model === undefined || value.model === null || isString(value.model);
  const legacyAlertThreshold = normalizeThresholdNumber(value.alertThreshold);
  const thresholds = normalizeBudgetThresholds(value.thresholds);
  const resolvedThresholds =
    thresholds === "invalid"
      ? undefined
      : thresholds ??
        (legacyAlertThreshold === undefined
          ? undefined
          : {
              warning: legacyAlertThreshold,
              escalated: legacyAlertThreshold,
              critical: legacyAlertThreshold,
            });
  const governanceState = value.governanceState;
  const freezeReason = normalizeString(value.freezeReason);
  const frozenAt = normalizeString(value.frozenAt);
  const frozenByAlertId = normalizeString(value.frozenByAlertId);
  const freezeReasonOk =
    value.freezeReason === undefined || value.freezeReason === null || Boolean(freezeReason);
  const frozenAtOk =
    value.frozenAt === undefined || value.frozenAt === null || isISODate(value.frozenAt);
  const frozenByAlertIdOk =
    value.frozenByAlertId === undefined ||
    value.frozenByAlertId === null ||
    Boolean(frozenByAlertId);
  const frozenStateFieldsOk =
    governanceState === "active"
      ? true
      : Boolean(freezeReason) && Boolean(frozenAt) && isISODate(frozenAt);

  return (
    isString(value.id) &&
    typeof scope === "string" &&
    BUDGET_SCOPE_SET.has(scope) &&
    sourceIdOk &&
    organizationIdOk &&
    userIdOk &&
    modelOk &&
    typeof period === "string" &&
    BUDGET_PERIOD_SET.has(period) &&
    isNumber(value.tokenLimit) &&
    isNumber(value.costLimit) &&
    resolvedThresholds !== undefined &&
    isNumber(resolvedThresholds.warning) &&
    isNumber(resolvedThresholds.escalated) &&
    isNumber(resolvedThresholds.critical) &&
    isNumber(value.alertThreshold) &&
    typeof value.enabled === "boolean" &&
    typeof governanceState === "string" &&
    isBudgetGovernanceState(governanceState) &&
    freezeReasonOk &&
    frozenAtOk &&
    frozenByAlertIdOk &&
    frozenStateFieldsOk &&
    isISODate(value.updatedAt)
  );
}

export function isBudgetReleaseRequest(value: unknown): value is BudgetReleaseRequest {
  if (!isRecord(value)) {
    return false;
  }

  const approvalsOk =
    Array.isArray(value.approvals) &&
    value.approvals.every((approval) => {
      if (!isRecord(approval)) {
        return false;
      }
      const email = normalizeString(approval.email);
      return (
        isString(approval.userId) &&
        isISODate(approval.approvedAt) &&
        (approval.email === undefined || approval.email === null || Boolean(email))
      );
    });

  const rejectedReasonOk =
    value.rejectedReason === undefined ||
    value.rejectedReason === null ||
    isString(value.rejectedReason);
  const rejectedAtOk =
    value.rejectedAt === undefined || value.rejectedAt === null || isISODate(value.rejectedAt);
  const executedAtOk =
    value.executedAt === undefined || value.executedAt === null || isISODate(value.executedAt);

  return (
    isString(value.id) &&
    isString(value.tenantId) &&
    isString(value.budgetId) &&
    isBudgetReleaseRequestStatus(value.status) &&
    isString(value.requestedByUserId) &&
    (value.requestedByEmail === undefined ||
      value.requestedByEmail === null ||
      isString(value.requestedByEmail)) &&
    isISODate(value.requestedAt) &&
    approvalsOk &&
    (value.rejectedByUserId === undefined ||
      value.rejectedByUserId === null ||
      isString(value.rejectedByUserId)) &&
    (value.rejectedByEmail === undefined ||
      value.rejectedByEmail === null ||
      isString(value.rejectedByEmail)) &&
    rejectedReasonOk &&
    rejectedAtOk &&
    executedAtOk &&
    isISODate(value.updatedAt)
  );
}

export function isAlert(value: unknown): value is Alert {
  if (!isRecord(value)) {
    return false;
  }

  const sourceIdOk =
    value.sourceId === undefined || value.sourceId === null || isString(value.sourceId);

  return (
    isString(value.id) &&
    isString(value.tenantId) &&
    isString(value.budgetId) &&
    sourceIdOk &&
    typeof value.period === "string" &&
    BUDGET_PERIOD_SET.has(value.period) &&
    isISODate(value.windowStart) &&
    isISODate(value.windowEnd) &&
    isNumber(value.tokensUsed) &&
    isNumber(value.costUsed) &&
    isNumber(value.tokenLimit) &&
    isNumber(value.costLimit) &&
    isNumber(value.threshold) &&
    isAlertStatus(value.status) &&
    isAlertSeverity(value.severity) &&
    isISODate(value.triggeredAt) &&
    isISODate(value.updatedAt)
  );
}

export function isAuditItem(value: unknown): value is AuditItem {
  if (!isRecord(value)) {
    return false;
  }

  return (
    isString(value.id) &&
    isString(value.eventId) &&
    isString(value.action) &&
    isAuditLevel(value.level) &&
    isString(value.detail) &&
    isRecord(value.metadata) &&
    isISODate(value.createdAt)
  );
}

export function isDeviceItem(value: unknown): value is DeviceItem {
  if (!isRecord(value)) {
    return false;
  }

  const organizationIdOk =
    value.organizationId === undefined ||
    value.organizationId === null ||
    isString(value.organizationId);
  const platformOk =
    value.platform === undefined || value.platform === null || isString(value.platform);
  const lastSeenAtOk =
    value.lastSeenAt === undefined || value.lastSeenAt === null || isISODate(value.lastSeenAt);

  return (
    isString(value.id) &&
    isString(value.tenantId) &&
    organizationIdOk &&
    isString(value.userId) &&
    isString(value.hostname) &&
    isString(value.fingerprint) &&
    platformOk &&
    typeof value.active === "boolean" &&
    lastSeenAtOk &&
    isISODate(value.createdAt) &&
    isISODate(value.updatedAt)
  );
}

export function isAgentItem(value: unknown): value is AgentItem {
  if (!isRecord(value)) {
    return false;
  }

  const organizationIdOk =
    value.organizationId === undefined ||
    value.organizationId === null ||
    isString(value.organizationId);
  const userIdOk = value.userId === undefined || value.userId === null || isString(value.userId);
  const versionOk =
    value.version === undefined || value.version === null || isString(value.version);
  const lastSeenAtOk =
    value.lastSeenAt === undefined || value.lastSeenAt === null || isISODate(value.lastSeenAt);

  return (
    isString(value.id) &&
    isString(value.tenantId) &&
    organizationIdOk &&
    userIdOk &&
    isString(value.deviceId) &&
    isString(value.hostname) &&
    versionOk &&
    typeof value.active === "boolean" &&
    lastSeenAtOk &&
    isISODate(value.createdAt) &&
    isISODate(value.updatedAt)
  );
}

export function isSourceBindingItem(value: unknown): value is SourceBindingItem {
  if (!isRecord(value)) {
    return false;
  }

  const organizationIdOk =
    value.organizationId === undefined ||
    value.organizationId === null ||
    isString(value.organizationId);
  const userIdOk = value.userId === undefined || value.userId === null || isString(value.userId);
  const deviceId = normalizeString(value.deviceId);
  const agentId = normalizeString(value.agentId);
  const identityRefOk = Boolean(deviceId) || Boolean(agentId);

  return (
    isString(value.id) &&
    isString(value.tenantId) &&
    organizationIdOk &&
    userIdOk &&
    isString(value.sourceId) &&
    (value.deviceId === undefined || value.deviceId === null || Boolean(deviceId)) &&
    (value.agentId === undefined || value.agentId === null || Boolean(agentId)) &&
    identityRefOk &&
    isSourceBindingMethod(value.method) &&
    isSourceAccessMode(value.accessMode) &&
    typeof value.active === "boolean" &&
    isISODate(value.createdAt) &&
    isISODate(value.updatedAt)
  );
}

export function isDeviceListResponse(value: unknown): value is DeviceListResponse {
  if (!isRecord(value)) {
    return false;
  }

  const filtersOk = isRecord(value.filters) && validateDeviceListInput(value.filters).success;

  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isDeviceItem(item)) &&
    isNumber(value.total) &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

export function isAgentListResponse(value: unknown): value is AgentListResponse {
  if (!isRecord(value)) {
    return false;
  }

  const filtersOk = isRecord(value.filters) && validateAgentListInput(value.filters).success;

  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isAgentItem(item)) &&
    isNumber(value.total) &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

export function isSourceBindingListResponse(
  value: unknown
): value is SourceBindingListResponse {
  if (!isRecord(value)) {
    return false;
  }

  const filtersOk =
    isRecord(value.filters) && validateSourceBindingListInput(value.filters).success;

  return (
    Array.isArray(value.items) &&
    value.items.every((item) => isSourceBindingItem(item)) &&
    isNumber(value.total) &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
  );
}

export function validateCreateSourceInput(input: unknown): ValidationResult<CreateSourceInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const name = normalizeString(input.name);
  const location = normalizeString(input.location);
  const accessMode = normalizeString(input.accessMode) ?? "realtime";
  const syncCron = normalizeString(input.syncCron);
  const syncRetentionDays = toOptionalInteger(input.syncRetentionDays);
  const sshConfig = normalizeSSHConfig(input.sshConfig);

  if (!name) {
    return { success: false, error: "name 必填且必须为非空字符串。" };
  }
  if (!isSourceType(input.type)) {
    return { success: false, error: "type 必须是 local/ssh/sync-cache 之一。" };
  }
  if (!location) {
    return { success: false, error: "location 必填且必须为非空字符串。" };
  }
  if (input.type === "local" && !matchesLocalWhitelist(location)) {
    return {
      success: false,
      error: `local 源路径不在白名单内：${LOCAL_SOURCE_WHITELIST.join(", ")}`,
    };
  }
  if (input.type === "ssh" && sshConfig === "invalid") {
    return {
      success: false,
      error:
        "sshConfig 非法：必须包含 host/user/authType/port；authType=key 时 keyPath 必填。",
    };
  }
  if (input.type !== "ssh" && input.sshConfig !== undefined && sshConfig === "invalid") {
    return { success: false, error: "sshConfig 格式非法。" };
  }
  if (input.accessMode !== undefined && !normalizeString(input.accessMode)) {
    return { success: false, error: "accessMode 必须是 realtime/sync/hybrid 之一。" };
  }
  if (!isSourceAccessMode(accessMode)) {
    return { success: false, error: "accessMode 必须是 realtime/sync/hybrid 之一。" };
  }
  if (input.syncCron !== undefined && !syncCron) {
    return { success: false, error: "syncCron 必须为非空字符串。" };
  }
  if (
    input.syncRetentionDays !== undefined &&
    (syncRetentionDays === undefined ||
      !Number.isInteger(syncRetentionDays) ||
      syncRetentionDays < 0)
  ) {
    return { success: false, error: "syncRetentionDays 必须是大于等于 0 的整数。" };
  }
  if (input.enabled !== undefined && typeof input.enabled !== "boolean") {
    return { success: false, error: "enabled 必须为布尔值。" };
  }

  return {
    success: true,
    data: {
      name,
      type: input.type,
      location,
      sshConfig: sshConfig === "invalid" ? undefined : sshConfig,
      accessMode,
      syncCron,
      syncRetentionDays,
      enabled: input.enabled,
    },
  };
}

export function validateAuthRegisterInput(input: unknown): ValidationResult<AuthRegisterInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const email = normalizeString(input.email);
  const displayName = normalizeString(input.displayName);
  const password = typeof input.password === "string" ? input.password : undefined;

  if (!email || !isEmail(email)) {
    return { success: false, error: "email 必填且必须为合法邮箱地址。" };
  }
  if (password === undefined || password.trim().length === 0) {
    return { success: false, error: "password 必填且必须为非空字符串。" };
  }
  if (password.length < PASSWORD_MIN_LENGTH) {
    return { success: false, error: `password 长度不能少于 ${PASSWORD_MIN_LENGTH} 位。` };
  }
  if (!displayName) {
    return { success: false, error: "displayName 必填且必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      email,
      password,
      displayName,
    },
  };
}

export function validateAuthLoginInput(input: unknown): ValidationResult<AuthLoginInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const email = normalizeString(input.email);
  const password = typeof input.password === "string" ? input.password : undefined;

  if (!email || !isEmail(email)) {
    return { success: false, error: "email 必填且必须为合法邮箱地址。" };
  }
  if (password === undefined || password.trim().length === 0) {
    return { success: false, error: "password 必填且必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      email,
      password,
    },
  };
}

export function validateAuthRefreshInput(input: unknown): ValidationResult<AuthRefreshInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const refreshToken = normalizeString(input.refreshToken);
  if (!refreshToken) {
    return { success: false, error: "refreshToken 必填且必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      refreshToken,
    },
  };
}

export function validateAuthLogoutInput(input: unknown): ValidationResult<AuthLogoutInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const refreshToken = normalizeString(input.refreshToken);
  if (!refreshToken) {
    return { success: false, error: "refreshToken 必填且必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      refreshToken,
    },
  };
}

export function validateCreateTenantInput(input: unknown): ValidationResult<CreateTenantInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const name = normalizeString(input.name);
  const slug = normalizeString(input.slug);

  if (!name) {
    return { success: false, error: "name 必填且必须为非空字符串。" };
  }
  if (input.slug !== undefined && !slug) {
    return { success: false, error: "slug 必须为非空字符串。" };
  }
  if (slug && !isSlug(slug)) {
    return {
      success: false,
      error: "slug 仅支持小写字母、数字和连字符，且不能以连字符开头或结尾。",
    };
  }

  return {
    success: true,
    data: {
      name,
      slug,
    },
  };
}

export function validateCreateOrganizationInput(
  input: unknown
): ValidationResult<CreateOrganizationInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const tenantId = normalizeString(input.tenantId);
  const name = normalizeString(input.name);
  const slug = normalizeString(input.slug);

  if (!tenantId) {
    return { success: false, error: "tenantId 必填且必须为非空字符串。" };
  }
  if (!name) {
    return { success: false, error: "name 必填且必须为非空字符串。" };
  }
  if (input.slug !== undefined && !slug) {
    return { success: false, error: "slug 必须为非空字符串。" };
  }
  if (slug && !isSlug(slug)) {
    return {
      success: false,
      error: "slug 仅支持小写字母、数字和连字符，且不能以连字符开头或结尾。",
    };
  }

  return {
    success: true,
    data: {
      tenantId,
      name,
      slug,
    },
  };
}

export function validateAddTenantMemberInput(
  input: unknown
): ValidationResult<AddTenantMemberInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const tenantId = normalizeString(input.tenantId);
  const userId = normalizeString(input.userId);
  const email = normalizeString(input.email);
  const tenantRole = normalizeString(input.tenantRole);
  const organizationId = normalizeString(input.organizationId);
  const orgRole = normalizeString(input.orgRole);

  if (!tenantId) {
    return { success: false, error: "tenantId 必填且必须为非空字符串。" };
  }
  if (input.userId !== undefined && !userId) {
    return { success: false, error: "userId 必须为非空字符串。" };
  }
  if (input.email !== undefined && !email) {
    return { success: false, error: "email 必须为非空字符串。" };
  }
  if (email && !isEmail(email)) {
    return { success: false, error: "email 必须为合法邮箱地址。" };
  }
  if (!userId && !email) {
    return { success: false, error: "userId 与 email 不能同时为空，至少提供一个。" };
  }
  if (!tenantRole || !isTenantRole(tenantRole)) {
    return {
      success: false,
      error: "tenantRole 必须是 owner/maintainer/member/readonly 之一。",
    };
  }
  if (input.organizationId !== undefined && !organizationId) {
    return { success: false, error: "organizationId 必须为非空字符串。" };
  }
  if (input.orgRole !== undefined && (!orgRole || !isOrgRole(orgRole))) {
    return { success: false, error: "orgRole 必须是 owner/maintainer/member/readonly 之一。" };
  }
  if (organizationId && !orgRole) {
    return { success: false, error: "提供 organizationId 时，orgRole 也必须提供。" };
  }
  if (!organizationId && orgRole) {
    return { success: false, error: "提供 orgRole 时，organizationId 也必须提供。" };
  }

  return {
    success: true,
    data: {
      tenantId,
      userId,
      email,
      tenantRole: tenantRole as TenantRole,
      organizationId,
      orgRole: orgRole as OrgRole | undefined,
    },
  };
}

export function validateDeviceListInput(input: unknown): ValidationResult<DeviceListInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const tenantId = normalizeString(input.tenantId);
  const organizationId = normalizeString(input.organizationId);
  const userId = normalizeString(input.userId);
  const keyword = normalizeString(input.keyword);
  const cursor = normalizeString(input.cursor);
  const limit = toOptionalInteger(input.limit);

  if (!tenantId) {
    return { success: false, error: "tenantId 必填且必须为非空字符串。" };
  }
  if (input.organizationId !== undefined && !organizationId) {
    return { success: false, error: "organizationId 必须为非空字符串。" };
  }
  if (input.userId !== undefined && !userId) {
    return { success: false, error: "userId 必须为非空字符串。" };
  }
  if (input.keyword !== undefined && !keyword) {
    return { success: false, error: "keyword 必须为非空字符串。" };
  }
  if (input.cursor !== undefined && !cursor) {
    return { success: false, error: "cursor 必须为非空字符串。" };
  }
  if (
    input.limit !== undefined &&
    (limit === undefined ||
      !Number.isInteger(limit) ||
      limit <= 0 ||
      limit > IDENTITY_BINDING_LIST_LIMIT_MAX)
  ) {
    return {
      success: false,
      error: `limit 必须是 1 到 ${IDENTITY_BINDING_LIST_LIMIT_MAX} 的整数。`,
    };
  }

  return {
    success: true,
    data: {
      tenantId,
      organizationId,
      userId,
      keyword,
      limit,
      cursor,
    },
  };
}

export function validateAgentListInput(input: unknown): ValidationResult<AgentListInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const tenantId = normalizeString(input.tenantId);
  const organizationId = normalizeString(input.organizationId);
  const userId = normalizeString(input.userId);
  const deviceId = normalizeString(input.deviceId);
  const keyword = normalizeString(input.keyword);
  const cursor = normalizeString(input.cursor);
  const limit = toOptionalInteger(input.limit);

  if (!tenantId) {
    return { success: false, error: "tenantId 必填且必须为非空字符串。" };
  }
  if (input.organizationId !== undefined && !organizationId) {
    return { success: false, error: "organizationId 必须为非空字符串。" };
  }
  if (input.userId !== undefined && !userId) {
    return { success: false, error: "userId 必须为非空字符串。" };
  }
  if (input.deviceId !== undefined && !deviceId) {
    return { success: false, error: "deviceId 必须为非空字符串。" };
  }
  if (input.keyword !== undefined && !keyword) {
    return { success: false, error: "keyword 必须为非空字符串。" };
  }
  if (input.cursor !== undefined && !cursor) {
    return { success: false, error: "cursor 必须为非空字符串。" };
  }
  if (
    input.limit !== undefined &&
    (limit === undefined ||
      !Number.isInteger(limit) ||
      limit <= 0 ||
      limit > IDENTITY_BINDING_LIST_LIMIT_MAX)
  ) {
    return {
      success: false,
      error: `limit 必须是 1 到 ${IDENTITY_BINDING_LIST_LIMIT_MAX} 的整数。`,
    };
  }

  return {
    success: true,
    data: {
      tenantId,
      organizationId,
      userId,
      deviceId,
      keyword,
      limit,
      cursor,
    },
  };
}

export function validateSourceBindingListInput(
  input: unknown
): ValidationResult<SourceBindingListInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const tenantId = normalizeString(input.tenantId);
  const organizationId = normalizeString(input.organizationId);
  const userId = normalizeString(input.userId);
  const sourceId = normalizeString(input.sourceId);
  const deviceId = normalizeString(input.deviceId);
  const agentId = normalizeString(input.agentId);
  const method = normalizeString(input.method);
  const accessMode = normalizeString(input.accessMode);
  const cursor = normalizeString(input.cursor);
  const limit = toOptionalInteger(input.limit);

  if (!tenantId) {
    return { success: false, error: "tenantId 必填且必须为非空字符串。" };
  }
  if (input.organizationId !== undefined && !organizationId) {
    return { success: false, error: "organizationId 必须为非空字符串。" };
  }
  if (input.userId !== undefined && !userId) {
    return { success: false, error: "userId 必须为非空字符串。" };
  }
  if (input.sourceId !== undefined && !sourceId) {
    return { success: false, error: "sourceId 必须为非空字符串。" };
  }
  if (input.deviceId !== undefined && !deviceId) {
    return { success: false, error: "deviceId 必须为非空字符串。" };
  }
  if (input.agentId !== undefined && !agentId) {
    return { success: false, error: "agentId 必须为非空字符串。" };
  }
  if (input.method !== undefined && !method) {
    return { success: false, error: "method 必须是 ssh-pull/agent-push 之一。" };
  }
  if (method && !isSourceBindingMethod(method)) {
    return { success: false, error: "method 必须是 ssh-pull/agent-push 之一。" };
  }
  if (input.accessMode !== undefined && !accessMode) {
    return { success: false, error: "accessMode 必须是 realtime/sync/hybrid 之一。" };
  }
  if (accessMode && !isSourceAccessMode(accessMode)) {
    return { success: false, error: "accessMode 必须是 realtime/sync/hybrid 之一。" };
  }
  if (input.active !== undefined && typeof input.active !== "boolean") {
    return { success: false, error: "active 必须为布尔值。" };
  }
  if (input.cursor !== undefined && !cursor) {
    return { success: false, error: "cursor 必须为非空字符串。" };
  }
  if (
    input.limit !== undefined &&
    (limit === undefined ||
      !Number.isInteger(limit) ||
      limit <= 0 ||
      limit > IDENTITY_BINDING_LIST_LIMIT_MAX)
  ) {
    return {
      success: false,
      error: `limit 必须是 1 到 ${IDENTITY_BINDING_LIST_LIMIT_MAX} 的整数。`,
    };
  }

  return {
    success: true,
    data: {
      tenantId,
      organizationId,
      userId,
      sourceId,
      deviceId,
      agentId,
      method: method as SourceBindingMethod | undefined,
      accessMode: accessMode as SourceAccessMode | undefined,
      active: input.active as boolean | undefined,
      limit,
      cursor,
    },
  };
}

export function validateCreateDeviceInput(input: unknown): ValidationResult<CreateDeviceInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const tenantId = normalizeString(input.tenantId);
  const organizationId = normalizeString(input.organizationId);
  const userId = normalizeString(input.userId);
  const hostname = normalizeString(input.hostname);
  const fingerprint = normalizeString(input.fingerprint);
  const platform = normalizeString(input.platform);

  if (!tenantId) {
    return { success: false, error: "tenantId 必填且必须为非空字符串。" };
  }
  if (input.organizationId !== undefined && !organizationId) {
    return { success: false, error: "organizationId 必须为非空字符串。" };
  }
  if (!userId) {
    return { success: false, error: "userId 必填且必须为非空字符串。" };
  }
  if (!hostname) {
    return { success: false, error: "hostname 必填且必须为非空字符串。" };
  }
  if (!fingerprint) {
    return { success: false, error: "fingerprint 必填且必须为非空字符串。" };
  }
  if (input.platform !== undefined && !platform) {
    return { success: false, error: "platform 必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      tenantId,
      organizationId,
      userId,
      hostname,
      fingerprint,
      platform,
    },
  };
}

export function validateCreateAgentInput(input: unknown): ValidationResult<CreateAgentInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const tenantId = normalizeString(input.tenantId);
  const organizationId = normalizeString(input.organizationId);
  const userId = normalizeString(input.userId);
  const deviceId = normalizeString(input.deviceId);
  const hostname = normalizeString(input.hostname);
  const version = normalizeString(input.version);

  if (!tenantId) {
    return { success: false, error: "tenantId 必填且必须为非空字符串。" };
  }
  if (input.organizationId !== undefined && !organizationId) {
    return { success: false, error: "organizationId 必须为非空字符串。" };
  }
  if (input.userId !== undefined && !userId) {
    return { success: false, error: "userId 必须为非空字符串。" };
  }
  if (!deviceId) {
    return { success: false, error: "deviceId 必填且必须为非空字符串。" };
  }
  if (!hostname) {
    return { success: false, error: "hostname 必填且必须为非空字符串。" };
  }
  if (input.version !== undefined && !version) {
    return { success: false, error: "version 必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      tenantId,
      organizationId,
      userId,
      deviceId,
      hostname,
      version,
    },
  };
}

export function validateCreateSourceBindingInput(
  input: unknown
): ValidationResult<CreateSourceBindingInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const tenantId = normalizeString(input.tenantId);
  const organizationId = normalizeString(input.organizationId);
  const userId = normalizeString(input.userId);
  const sourceId = normalizeString(input.sourceId);
  const deviceId = normalizeString(input.deviceId);
  const agentId = normalizeString(input.agentId);
  const method = normalizeString(input.method);
  const accessMode = normalizeString(input.accessMode) ?? "realtime";

  if (!tenantId) {
    return { success: false, error: "tenantId 必填且必须为非空字符串。" };
  }
  if (input.organizationId !== undefined && !organizationId) {
    return { success: false, error: "organizationId 必须为非空字符串。" };
  }
  if (input.userId !== undefined && !userId) {
    return { success: false, error: "userId 必须为非空字符串。" };
  }
  if (!sourceId) {
    return { success: false, error: "sourceId 必填且必须为非空字符串。" };
  }
  if (input.deviceId !== undefined && !deviceId) {
    return { success: false, error: "deviceId 必须为非空字符串。" };
  }
  if (input.agentId !== undefined && !agentId) {
    return { success: false, error: "agentId 必须为非空字符串。" };
  }
  if (!deviceId && !agentId) {
    return { success: false, error: "deviceId 与 agentId 不能同时为空，至少提供一个。" };
  }
  if (!method || !isSourceBindingMethod(method)) {
    return { success: false, error: "method 必须是 ssh-pull/agent-push 之一。" };
  }
  if (input.accessMode !== undefined && !normalizeString(input.accessMode)) {
    return { success: false, error: "accessMode 必须是 realtime/sync/hybrid 之一。" };
  }
  if (!isSourceAccessMode(accessMode)) {
    return { success: false, error: "accessMode 必须是 realtime/sync/hybrid 之一。" };
  }

  return {
    success: true,
    data: {
      tenantId,
      organizationId,
      userId,
      sourceId,
      deviceId,
      agentId,
      method: method as SourceBindingMethod,
      accessMode,
    },
  };
}

export function validateDeleteDeviceInput(input: unknown): ValidationResult<DeleteDeviceInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const tenantId = normalizeString(input.tenantId);
  const deviceId = normalizeString(input.deviceId);

  if (!tenantId) {
    return { success: false, error: "tenantId 必填且必须为非空字符串。" };
  }
  if (!deviceId) {
    return { success: false, error: "deviceId 必填且必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      tenantId,
      deviceId,
    },
  };
}

export function validateDeleteAgentInput(input: unknown): ValidationResult<DeleteAgentInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const tenantId = normalizeString(input.tenantId);
  const agentId = normalizeString(input.agentId);

  if (!tenantId) {
    return { success: false, error: "tenantId 必填且必须为非空字符串。" };
  }
  if (!agentId) {
    return { success: false, error: "agentId 必填且必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      tenantId,
      agentId,
    },
  };
}

export function validateDeleteSourceBindingInput(
  input: unknown
): ValidationResult<DeleteSourceBindingInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const tenantId = normalizeString(input.tenantId);
  const bindingId = normalizeString(input.bindingId);

  if (!tenantId) {
    return { success: false, error: "tenantId 必填且必须为非空字符串。" };
  }
  if (!bindingId) {
    return { success: false, error: "bindingId 必填且必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      tenantId,
      bindingId,
    },
  };
}

export function validateSessionSearchInput(input: unknown): ValidationResult<SessionSearchInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const { from, to, limit } = input;
  const sourceId = normalizeString(input.sourceId);
  const keyword = normalizeString(input.keyword);
  const clientType = normalizeString(input.clientType);
  const tool = normalizeString(input.tool);
  const host = normalizeString(input.host);
  const model = normalizeString(input.model);
  const project = normalizeString(input.project);
  const cursor = normalizeString(input.cursor);

  if (input.sourceId !== undefined && !sourceId) {
    return { success: false, error: "sourceId 必须为非空字符串。" };
  }
  if (input.keyword !== undefined && !keyword) {
    return { success: false, error: "keyword 必须为非空字符串。" };
  }
  if (input.clientType !== undefined && !clientType) {
    return { success: false, error: "clientType 必须为非空字符串。" };
  }
  if (input.tool !== undefined && !tool) {
    return { success: false, error: "tool 必须为非空字符串。" };
  }
  if (input.host !== undefined && !host) {
    return { success: false, error: "host 必须为非空字符串。" };
  }
  if (input.model !== undefined && !model) {
    return { success: false, error: "model 必须为非空字符串。" };
  }
  if (input.project !== undefined && !project) {
    return { success: false, error: "project 必须为非空字符串。" };
  }
  if (input.cursor !== undefined && !cursor) {
    return { success: false, error: "cursor 必须为非空字符串。" };
  }
  if (from !== undefined && !isISODate(from)) {
    return { success: false, error: "from 必须为 ISO 日期字符串。" };
  }
  if (to !== undefined && !isISODate(to)) {
    return { success: false, error: "to 必须为 ISO 日期字符串。" };
  }
  if (
    limit !== undefined &&
    (!isNumber(limit) || !Number.isInteger(limit) || limit <= 0 || limit > SESSION_LIMIT_MAX)
  ) {
    return { success: false, error: `limit 必须是 1 到 ${SESSION_LIMIT_MAX} 的整数。` };
  }

  if (from !== undefined && to !== undefined) {
    const fromTimestamp = Date.parse(from);
    const toTimestamp = Date.parse(to);
    if (fromTimestamp > toTimestamp) {
      return { success: false, error: "from 不能晚于 to。" };
    }
  }

  return {
    success: true,
    data: {
      sourceId,
      keyword,
      clientType,
      tool,
      host,
      model,
      project,
      from: from as string | undefined,
      to: to as string | undefined,
      limit: limit as number | undefined,
      cursor,
    },
  };
}

export function validateSourceParseFailureQueryInput(
  input: unknown
): ValidationResult<SourceParseFailureQueryInput> {
  if (!isRecord(input)) {
    return { success: false, error: "查询参数必须是对象。" };
  }

  const from = normalizeString(input.from);
  const to = normalizeString(input.to);
  const parserKey = normalizeString(input.parserKey);
  const errorCode = normalizeString(input.errorCode);
  const parsedLimit = toOptionalInteger(input.limit);

  if (input.from !== undefined && !from) {
    return { success: false, error: "from 必须为 ISO 日期字符串。" };
  }
  if (input.to !== undefined && !to) {
    return { success: false, error: "to 必须为 ISO 日期字符串。" };
  }
  if (from && !isISODate(from)) {
    return { success: false, error: "from 必须为 ISO 日期字符串。" };
  }
  if (to && !isISODate(to)) {
    return { success: false, error: "to 必须为 ISO 日期字符串。" };
  }
  if (from && to && Date.parse(from) > Date.parse(to)) {
    return { success: false, error: "from 不能晚于 to。" };
  }
  if (
    input.limit !== undefined &&
    (parsedLimit === undefined ||
      !Number.isInteger(parsedLimit) ||
      parsedLimit < 1 ||
      parsedLimit > SOURCE_PARSE_FAILURE_LIMIT_MAX)
  ) {
    return {
      success: false,
      error: `limit 必须是 1 到 ${SOURCE_PARSE_FAILURE_LIMIT_MAX} 的整数。`,
    };
  }

  return {
    success: true,
    data: {
      from,
      to,
      parserKey,
      errorCode,
      limit: parsedLimit ?? SOURCE_PARSE_FAILURE_LIMIT_DEFAULT,
    },
  };
}

export function validateSessionExportQueryInput(
  input: unknown
): ValidationResult<SessionExportQueryInput> {
  if (!isRecord(input)) {
    return { success: false, error: "查询参数必须是对象。" };
  }

  const format = normalizeString(input.format) ?? "json";
  if (input.format !== undefined && !normalizeString(input.format)) {
    return { success: false, error: "format 必须是 json/csv 之一。" };
  }
  if (!isExportFormat(format)) {
    return { success: false, error: "format 必须是 json/csv 之一。" };
  }

  const parsedLimit = toOptionalInteger(input.limit);
  if (
    input.limit !== undefined &&
    (parsedLimit === undefined || !Number.isInteger(parsedLimit))
  ) {
    return { success: false, error: `limit 必须是 1 到 ${SESSION_LIMIT_MAX} 的整数。` };
  }

  const searchInputResult = validateSessionSearchInput({
    ...input,
    limit: parsedLimit,
  });
  if (!searchInputResult.success) {
    return searchInputResult;
  }

  return {
    success: true,
    data: {
      format,
      ...searchInputResult.data,
    },
  };
}

export function validateSessionExportJobCreateInput(
  input: unknown
): ValidationResult<SessionExportJobCreateInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }
  return validateSessionExportQueryInput(input);
}

export function validateExportJobId(input: unknown): ValidationResult<string> {
  const id = normalizeString(input);
  if (!id) {
    return { success: false, error: "id 必须为非空字符串。" };
  }
  return {
    success: true,
    data: id,
  };
}

function normalizePricingCatalogEntry(
  input: unknown
): PricingCatalogEntry | undefined | "invalid" {
  if (!isRecord(input)) {
    return "invalid";
  }
  const model = normalizeString(input.model);
  if (!model) {
    return "invalid";
  }

  const inputPer1k = toNumber(input.inputPer1k);
  const outputPer1k = toNumber(input.outputPer1k);
  const cacheReadPer1k = input.cacheReadPer1k === undefined ? undefined : toNumber(input.cacheReadPer1k);
  const cacheWritePer1k =
    input.cacheWritePer1k === undefined ? undefined : toNumber(input.cacheWritePer1k);
  const reasoningPer1k =
    input.reasoningPer1k === undefined ? undefined : toNumber(input.reasoningPer1k);
  const currency = normalizeString(input.currency) ?? "USD";

  if (!Number.isFinite(inputPer1k) || inputPer1k < 0) {
    return "invalid";
  }
  if (!Number.isFinite(outputPer1k) || outputPer1k < 0) {
    return "invalid";
  }
  if (cacheReadPer1k !== undefined && (!Number.isFinite(cacheReadPer1k) || cacheReadPer1k < 0)) {
    return "invalid";
  }
  if (cacheWritePer1k !== undefined && (!Number.isFinite(cacheWritePer1k) || cacheWritePer1k < 0)) {
    return "invalid";
  }
  if (reasoningPer1k !== undefined && (!Number.isFinite(reasoningPer1k) || reasoningPer1k < 0)) {
    return "invalid";
  }

  return {
    model,
    inputPer1k,
    outputPer1k,
    cacheReadPer1k,
    cacheWritePer1k,
    reasoningPer1k,
    currency,
  };
}

function toNumber(value: unknown): number {
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value.trim());
    return Number.isFinite(parsed) ? parsed : Number.NaN;
  }
  return Number.NaN;
}

export function validatePricingCatalogUpsertInput(
  input: unknown
): ValidationResult<{ note?: string; entries: PricingCatalogEntry[] }> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const entriesRaw = input.entries;
  if (!Array.isArray(entriesRaw) || entriesRaw.length === 0) {
    return { success: false, error: "entries 必须是非空数组。" };
  }
  const note = normalizeString(input.note);

  const entries: PricingCatalogEntry[] = [];
  const modelSet = new Set<string>();
  for (const entry of entriesRaw) {
    const normalized = normalizePricingCatalogEntry(entry);
    if (!normalized || normalized === "invalid") {
      return { success: false, error: "pricing entry 非法：model 与单价字段必须有效。" };
    }
    const modelKey = normalized.model.toLowerCase();
    if (modelSet.has(modelKey)) {
      return { success: false, error: `entries 存在重复 model：${normalized.model}` };
    }
    modelSet.add(modelKey);
    entries.push(normalized);
  }

  return {
    success: true,
    data: {
      note,
      entries,
    },
  };
}

export function isPricingCatalog(value: unknown): value is PricingCatalog {
  if (!isRecord(value)) {
    return false;
  }
  const version = value.version;
  const entries = value.entries;
  if (!isRecord(version) || !Array.isArray(entries)) {
    return false;
  }
  return (
    isString(version.id) &&
    isString(version.tenantId) &&
    isNumber(version.version) &&
    isISODate(version.createdAt) &&
    entries.every((entry) => normalizePricingCatalogEntry(entry) !== "invalid")
  );
}

export function validateBudgetUpsertInput(input: unknown): ValidationResult<BudgetUpsertInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const { scope, sourceId, organizationId, userId, model, period, tokenLimit, costLimit } = input;
  const normalizedSourceId = normalizeString(sourceId);
  const normalizedOrganizationId = normalizeString(organizationId);
  const normalizedUserId = normalizeString(userId);
  const normalizedModel = normalizeString(model);
  const thresholds = normalizeBudgetThresholds(input.thresholds);
  const legacyAlertThreshold = normalizeThresholdNumber(input.alertThreshold);

  if (typeof scope !== "string" || !BUDGET_SCOPE_SET.has(scope)) {
    return { success: false, error: "scope 必须是 global/source/org/user/model 之一。" };
  }
  if (sourceId !== undefined && !normalizedSourceId) {
    return { success: false, error: "sourceId 必须为非空字符串。" };
  }
  if (organizationId !== undefined && !normalizedOrganizationId) {
    return { success: false, error: "organizationId 必须为非空字符串。" };
  }
  if (userId !== undefined && !normalizedUserId) {
    return { success: false, error: "userId 必须为非空字符串。" };
  }
  if (model !== undefined && !normalizedModel) {
    return { success: false, error: "model 必须为非空字符串。" };
  }
  if (scope === "source" && !normalizedSourceId) {
    return { success: false, error: "scope=source 时 sourceId 必填且必须为非空字符串。" };
  }
  if (scope === "org" && !normalizedOrganizationId) {
    return {
      success: false,
      error: "scope=org 时 organizationId 必填且必须为非空字符串。",
    };
  }
  if (scope === "user" && !normalizedUserId) {
    return {
      success: false,
      error: "scope=user 时 userId 必填且必须为非空字符串。",
    };
  }
  if (scope === "model" && !normalizedModel) {
    return {
      success: false,
      error: "scope=model 时 model 必填且必须为非空字符串。",
    };
  }
  if (typeof period !== "string" || !BUDGET_PERIOD_SET.has(period)) {
    return { success: false, error: "period 必须是 daily/monthly 之一。" };
  }
  if (!isNumber(tokenLimit) || tokenLimit < 0) {
    return { success: false, error: "tokenLimit 必须是大于等于 0 的数字。" };
  }
  if (!isNumber(costLimit) || costLimit < 0) {
    return { success: false, error: "costLimit 必须是大于等于 0 的数字。" };
  }
  if (thresholds === "invalid") {
    return {
      success: false,
      error: "thresholds 必须包含 warning/escalated/critical，且都在 0 到 1 之间并满足 warning <= escalated <= critical。",
    };
  }
  if (input.alertThreshold !== undefined && legacyAlertThreshold === undefined) {
    return { success: false, error: "alertThreshold 必须在 0 到 1 之间。" };
  }
  if (!thresholds && legacyAlertThreshold === undefined) {
    return {
      success: false,
      error: "thresholds 与 alertThreshold 不能同时为空，至少提供一种阈值配置。",
    };
  }
  if (tokenLimit === 0 && costLimit === 0) {
    return { success: false, error: "tokenLimit 与 costLimit 不能同时为 0。" };
  }

  const resolvedThresholds: BudgetThresholds =
    thresholds ??
    ({
      warning: legacyAlertThreshold as number,
      escalated: legacyAlertThreshold as number,
      critical: legacyAlertThreshold as number,
    } as const);

  return {
    success: true,
    data: {
      scope: scope as BudgetUpsertInput["scope"],
      sourceId: normalizedSourceId,
      organizationId: normalizedOrganizationId,
      userId: normalizedUserId,
      model: normalizedModel,
      period: period as BudgetUpsertInput["period"],
      tokenLimit,
      costLimit,
      thresholds: resolvedThresholds,
      alertThreshold: resolvedThresholds.warning,
    },
  };
}

export function validateAlertListInput(input: unknown): ValidationResult<AlertListInput> {
  if (!isRecord(input)) {
    return { success: false, error: "查询参数必须是对象。" };
  }

  const status = normalizeString(input.status);
  const severity = normalizeString(input.severity);
  const sourceId = normalizeString(input.sourceId);
  const from = normalizeString(input.from);
  const to = normalizeString(input.to);
  const limit = toOptionalInteger(input.limit);

  if (input.status !== undefined && (!status || !isAlertStatus(status))) {
    return { success: false, error: "status 必须是 open/acknowledged/resolved 之一。" };
  }
  if (input.severity !== undefined && (!severity || !isAlertSeverity(severity))) {
    return { success: false, error: "severity 必须是 warning/critical 之一。" };
  }
  if (input.sourceId !== undefined && !sourceId) {
    return { success: false, error: "sourceId 必须为非空字符串。" };
  }
  if (from !== undefined && !isISODate(from)) {
    return { success: false, error: "from 必须为 ISO 日期字符串。" };
  }
  if (to !== undefined && !isISODate(to)) {
    return { success: false, error: "to 必须为 ISO 日期字符串。" };
  }
  if (
    limit !== undefined &&
    (!Number.isInteger(limit) || limit <= 0 || limit > ALERT_LIMIT_MAX)
  ) {
    return { success: false, error: `limit 必须是 1 到 ${ALERT_LIMIT_MAX} 的整数。` };
  }
  if (from && to && Date.parse(from) > Date.parse(to)) {
    return { success: false, error: "from 不能晚于 to。" };
  }

  return {
    success: true,
    data: {
      status: status as AlertStatus | undefined,
      severity: severity as AlertSeverity | undefined,
      sourceId,
      from,
      to,
      limit,
    },
  };
}

export function validateAlertStatusUpdateInput(
  input: unknown
): ValidationResult<AlertStatusUpdateInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const status = normalizeString(input.status);
  if (!status || !isAlertMutableStatus(status)) {
    return {
      success: false,
      error: "status 必填且必须是 acknowledged/resolved 之一。",
    };
  }

  return {
    success: true,
    data: {
      status,
    },
  };
}

export function validateAuditListInput(input: unknown): ValidationResult<AuditListInput> {
  if (!isRecord(input)) {
    return { success: false, error: "查询参数必须是对象。" };
  }

  const level = normalizeString(input.level);
  const from = normalizeString(input.from);
  const to = normalizeString(input.to);
  const limit = toOptionalInteger(input.limit);

  if (input.level !== undefined && (!level || !isAuditLevel(level))) {
    return { success: false, error: "level 必须是 info/warning/error/critical 之一。" };
  }
  if (from !== undefined && !isISODate(from)) {
    return { success: false, error: "from 必须为 ISO 日期字符串。" };
  }
  if (to !== undefined && !isISODate(to)) {
    return { success: false, error: "to 必须为 ISO 日期字符串。" };
  }
  if (
    limit !== undefined &&
    (!Number.isInteger(limit) || limit <= 0 || limit > AUDIT_LIMIT_MAX)
  ) {
    return { success: false, error: `limit 必须是 1 到 ${AUDIT_LIMIT_MAX} 的整数。` };
  }
  if (from && to && Date.parse(from) > Date.parse(to)) {
    return { success: false, error: "from 不能晚于 to。" };
  }

  return {
    success: true,
    data: {
      level: level as AuditLevel | undefined,
      from,
      to,
      limit,
    },
  };
}

export function validateAuditExportQueryInput(
  input: unknown
): ValidationResult<AuditExportQueryInput> {
  if (!isRecord(input)) {
    return { success: false, error: "查询参数必须是对象。" };
  }

  const format = normalizeString(input.format);
  const eventId = normalizeString(input.eventId);
  const action = normalizeString(input.action);
  const keyword = normalizeString(input.keyword);
  const base = validateAuditListInput(input);

  if (!base.success) {
    return base;
  }
  if (!format || !isExportFormat(format)) {
    return { success: false, error: "format 必填且必须是 json/csv 之一。" };
  }
  if (input.eventId !== undefined && !eventId) {
    return { success: false, error: "eventId 必须为非空字符串。" };
  }
  if (input.action !== undefined && !action) {
    return { success: false, error: "action 必须为非空字符串。" };
  }
  if (input.keyword !== undefined && !keyword) {
    return { success: false, error: "keyword 必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      ...base.data,
      format,
      eventId,
      action,
      keyword,
    },
  };
}

function validateSystemConfigBackupSource(
  input: unknown
): ValidationResult<SystemConfigBackupSource> {
  const sourceResult = validateCreateSourceInput(input);
  if (!sourceResult.success) {
    return sourceResult;
  }

  if (!isRecord(input)) {
    return { success: false, error: "source 必须是对象。" };
  }

  const enabled = input.enabled ?? true;
  if (typeof enabled !== "boolean") {
    return { success: false, error: "source.enabled 必须为布尔值。" };
  }

  return {
    success: true,
    data: {
      name: sourceResult.data.name,
      type: sourceResult.data.type,
      location: sourceResult.data.location,
      sshConfig: sourceResult.data.sshConfig,
      accessMode: sourceResult.data.accessMode ?? "realtime",
      syncCron: sourceResult.data.syncCron,
      syncRetentionDays: sourceResult.data.syncRetentionDays,
      enabled,
    },
  };
}

function validateSystemConfigBackupPayload(
  input: unknown
): ValidationResult<SystemConfigBackupPayload> {
  if (!isRecord(input)) {
    return { success: false, error: "backup 必须是对象。" };
  }

  const schemaVersion = normalizeString(input.schemaVersion);
  const tenantId = normalizeString(input.tenantId);
  const exportedAt = normalizeString(input.exportedAt);
  const exportedBy = input.exportedBy;
  const sources = input.sources;
  const budgets = input.budgets;
  const pricingCatalog = input.pricingCatalog;

  if (!schemaVersion) {
    return { success: false, error: "backup.schemaVersion 必填且必须为非空字符串。" };
  }
  if (!tenantId) {
    return { success: false, error: "backup.tenantId 必填且必须为非空字符串。" };
  }
  if (!exportedAt || !isISODate(exportedAt)) {
    return { success: false, error: "backup.exportedAt 必填且必须为 ISO 日期字符串。" };
  }
  if (!isRecord(exportedBy)) {
    return { success: false, error: "backup.exportedBy 必填且必须是对象。" };
  }

  const exportedByUserId = normalizeString(exportedBy.userId);
  const exportedByEmail = normalizeString(exportedBy.email);
  if (!exportedByUserId) {
    return {
      success: false,
      error: "backup.exportedBy.userId 必填且必须为非空字符串。",
    };
  }
  if (
    exportedBy.email !== undefined &&
    (!exportedByEmail || !isEmail(exportedByEmail))
  ) {
    return { success: false, error: "backup.exportedBy.email 必须是合法邮箱地址。" };
  }

  if (!Array.isArray(sources)) {
    return { success: false, error: "backup.sources 必须是数组。" };
  }
  const normalizedSources: SystemConfigBackupSource[] = [];
  for (let index = 0; index < sources.length; index += 1) {
    const sourceResult = validateSystemConfigBackupSource(sources[index]);
    if (!sourceResult.success) {
      return {
        success: false,
        error: `backup.sources[${index}] 非法：${sourceResult.error}`,
      };
    }
    normalizedSources.push(sourceResult.data);
  }

  if (!Array.isArray(budgets)) {
    return { success: false, error: "backup.budgets 必须是数组。" };
  }
  const normalizedBudgets: SystemConfigBackupPayload["budgets"] = [];
  for (let index = 0; index < budgets.length; index += 1) {
    const budgetResult = validateBudgetUpsertInput(budgets[index]);
    if (!budgetResult.success) {
      return {
        success: false,
        error: `backup.budgets[${index}] 非法：${budgetResult.error}`,
      };
    }
    normalizedBudgets.push(budgetResult.data);
  }

  let normalizedPricingCatalog: SystemConfigBackupPayload["pricingCatalog"] | undefined;
  if (pricingCatalog !== undefined) {
    if (pricingCatalog === null || !isRecord(pricingCatalog)) {
      return { success: false, error: "backup.pricingCatalog 必须是对象。" };
    }
    const pricingResult = validatePricingCatalogUpsertInput(pricingCatalog);
    if (!pricingResult.success) {
      return {
        success: false,
        error: `backup.pricingCatalog 非法：${pricingResult.error}`,
      };
    }
    normalizedPricingCatalog = {
      note: pricingResult.data.note,
      entries: pricingResult.data.entries,
    };
  }

  return {
    success: true,
    data: {
      schemaVersion,
      tenantId,
      exportedAt,
      exportedBy: {
        userId: exportedByUserId,
        email: exportedByEmail,
      },
      sources: normalizedSources,
      budgets: normalizedBudgets,
      pricingCatalog: normalizedPricingCatalog,
    },
  };
}

export function validateSystemConfigRestoreInput(
  input: unknown
): ValidationResult<SystemConfigRestoreInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const backupResult = validateSystemConfigBackupPayload(input.backup);
  if (!backupResult.success) {
    return { success: false, error: backupResult.error };
  }

  const dryRun = input.dryRun;
  const restoreSources = input.restoreSources;
  const restoreBudgets = input.restoreBudgets;
  const restorePricingCatalog = input.restorePricingCatalog;

  if (dryRun !== undefined && typeof dryRun !== "boolean") {
    return { success: false, error: "dryRun 必须为布尔值。" };
  }
  if (restoreSources !== undefined && typeof restoreSources !== "boolean") {
    return { success: false, error: "restoreSources 必须为布尔值。" };
  }
  if (restoreBudgets !== undefined && typeof restoreBudgets !== "boolean") {
    return { success: false, error: "restoreBudgets 必须为布尔值。" };
  }
  if (
    restorePricingCatalog !== undefined &&
    typeof restorePricingCatalog !== "boolean"
  ) {
    return { success: false, error: "restorePricingCatalog 必须为布尔值。" };
  }

  return {
    success: true,
    data: {
      backup: backupResult.data,
      dryRun,
      restoreSources,
      restoreBudgets,
      restorePricingCatalog,
    },
  };
}

export function validateCreateBudgetReleaseRequestInput(
  input: unknown
): ValidationResult<CreateBudgetReleaseRequestInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const reason = normalizeString(input.reason);
  if (input.reason !== undefined && !reason) {
    return { success: false, error: "reason 必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      reason,
    },
  };
}

export function validateRejectBudgetReleaseRequestInput(
  input: unknown
): ValidationResult<RejectBudgetReleaseRequestInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const reason = normalizeString(input.reason);
  if (input.reason !== undefined && !reason) {
    return { success: false, error: "reason 必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      reason,
    },
  };
}

export function validateIntegrationAlertCallbackInput(
  input: unknown
): ValidationResult<IntegrationAlertCallbackInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }

  const callbackId = normalizeString(input.callbackId ?? input.callback_id);
  const tenantId = normalizeString(input.tenantId ?? input.tenant_id);
  const action = normalizeString(input.action);
  const alertId = normalizeString(input.alertId ?? input.alert_id);
  const budgetId = normalizeString(input.budgetId ?? input.budget_id);
  const requestId = normalizeString(input.requestId ?? input.request_id);
  const actorUserId = normalizeString(
    input.actorUserId ?? input.actor_user_id ?? input.userId ?? input.user_id
  );
  const actorEmail = normalizeString(input.actorEmail ?? input.actor_email ?? input.email);
  const reason = normalizeString(input.reason);

  if (!callbackId) {
    return { success: false, error: "callback_id 必填且必须为非空字符串。" };
  }
  if (!action || !isIntegrationAlertCallbackAction(action)) {
    return {
      success: false,
      error: "action 必须是 ack/resolve/request_release/approve_release/reject_release 之一。",
    };
  }
  if (input.tenantId !== undefined || input.tenant_id !== undefined) {
    if (!tenantId) {
      return { success: false, error: "tenant_id 必须为非空字符串。" };
    }
  }
  if (
    (input.actorEmail !== undefined ||
      input.actor_email !== undefined ||
      input.email !== undefined) &&
    (!actorEmail || !isEmail(actorEmail))
  ) {
    return { success: false, error: "actorEmail 必须是合法邮箱地址。" };
  }
  if (input.reason !== undefined && !reason) {
    return { success: false, error: "reason 必须为非空字符串。" };
  }

  if ((action === "ack" || action === "resolve") && !alertId) {
    return { success: false, error: `action=${action} 时 alert_id 必填且必须为非空字符串。` };
  }
  if (action === "request_release") {
    if (!budgetId) {
      return {
        success: false,
        error: "action=request_release 时 budget_id 必填且必须为非空字符串。",
      };
    }
    if (!actorUserId) {
      return {
        success: false,
        error: "action=request_release 时 actor_user_id 必填且必须为非空字符串。",
      };
    }
  }
  if (action === "approve_release" || action === "reject_release") {
    if (!budgetId) {
      return {
        success: false,
        error: `action=${action} 时 budget_id 必填且必须为非空字符串。`,
      };
    }
    if (!requestId) {
      return {
        success: false,
        error: `action=${action} 时 request_id 必填且必须为非空字符串。`,
      };
    }
    if (!actorUserId) {
      return {
        success: false,
        error: `action=${action} 时 actor_user_id 必填且必须为非空字符串。`,
      };
    }
  }

  return {
    success: true,
    data: {
      callbackId,
      tenantId,
      action,
      alertId,
      budgetId,
      requestId,
      actorUserId,
      actorEmail,
      reason,
    },
  };
}
