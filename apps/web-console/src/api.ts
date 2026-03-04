import type {
  AlertItem,
  AlertListInput,
  AlertListResponse,
  AlertMutableStatus,
  AuthExternalExchangeInput,
  AuthLoginInput,
  AuthLoginResponse,
  AuthProviderItem,
  AuthProviderListResponse,
  AuthRefreshInput,
  AuthRefreshResponse,
  AuthTokens,
  CreateSourceInput,
  DownloadFile,
  ExportFormat,
  HeatmapCell,
  AuthUserProfile,
  PricingCatalog,
  PricingCatalogEntry,
  PricingCatalogUpsertInput,
  SourceAccessMode,
  SourceConnectionTestResponse,
  SourceHealth,
  SourceParseFailure,
  SourceParseFailureListResponse,
  SourceParseFailureQueryInput,
  Source,
  SourceListResponse,
  SourceType,
  Session,
  SessionDetailResponse,
  SessionEventListResponse,
  SessionSearchInput,
  SessionSearchResponse,
  SessionSourceFreshness,
  UsageExportDimension,
  UsageExportQueryInput,
  UsageAggregateFilters,
  UsageAggregateResponse,
  UsageDailyItem,
  UsageHeatmapResponse,
  UsageModelItem,
  UsageMonthlyItem,
  UsageSessionBreakdownItem,
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
const ALERT_SEVERITIES = ["warning", "critical"] as const;
const ALERT_STATUSES = ["open", "acknowledged", "resolved"] as const;
const ALERT_MUTABLE_STATUSES = ["acknowledged", "resolved"] as const;
const EXPORT_FORMATS = ["json", "csv"] as const;
const USAGE_EXPORT_DIMENSIONS = [
  "daily",
  "monthly",
  "models",
  "sessions",
  "heatmap",
] as const;

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
    hasCompatibleAccessMode &&
    hasCompatibleSync &&
    hasCompatibleSyncCron &&
    hasCompatibleSyncRetentionDays
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isISODateString(value: unknown): value is string {
  return typeof value === "string" && !Number.isNaN(Date.parse(value));
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

function isExportFormat(value: unknown): value is ExportFormat {
  return typeof value === "string" && EXPORT_FORMATS.includes(value as ExportFormat);
}

function isUsageExportDimension(value: unknown): value is UsageExportDimension {
  return (
    typeof value === "string" &&
    USAGE_EXPORT_DIMENSIONS.includes(value as UsageExportDimension)
  );
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

  return (
    statusOk &&
    severityOk &&
    scopeOk &&
    scopeRefOk &&
    budgetIdOk &&
    fromOk &&
    toOk &&
    limitOk
  );
}

function isAlertItem(value: unknown): value is AlertItem {
  if (!isRecord(value)) {
    return false;
  }

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
    isRecord(value.metadata)
  );
}

function isAlertListResponse(value: unknown): value is AlertListResponse {
  if (!isRecord(value) || !Array.isArray(value.items)) {
    return false;
  }
  const filtersOk = value.filters === undefined || isAlertListInput(value.filters);
  return (
    value.items.every((item) => isAlertItem(item)) &&
    typeof value.total === "number" &&
    Number.isInteger(value.total) &&
    value.total >= 0 &&
    filtersOk
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

  const query = params.toString();
  return query.length > 0 ? `?${query}` : "";
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

function isUsageAggregateResponse<TItem>(
  value: unknown
): value is UsageAggregateResponse<TItem> {
  return isRecord(value) && Array.isArray(value.items);
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
  return (
    typeof value.id === "string" &&
    typeof value.type === "string" &&
    typeof value.displayName === "string" &&
    typeof value.enabled === "boolean" &&
    issuerOk &&
    authorizationUrlOk
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

export async function fetchAlerts(
  input?: AlertListInput,
  signal?: AbortSignal
): Promise<AlertListResponse> {
  const result = await requestJson<unknown>(
    `/api/v1/alerts${buildAlertListQuery(input)}`,
    undefined,
    signal
  );

  if (!isAlertListResponse(result)) {
    throw new Error("alerts 返回结构不合法");
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
    throw new Error("dimension 必须是 daily/monthly/models/sessions/heatmap。");
  }

  const defaultFilename = `usage-${input.dimension}-export.${format}`;
  return requestBlob(
    `/api/v1/exports/usage${buildUsageExportQuery(format, input)}`,
    defaultFilename,
    undefined,
    signal
  );
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
