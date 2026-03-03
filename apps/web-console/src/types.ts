export type MetricKey = "tokens" | "cost" | "sessions";
export type SourceType = "local" | "ssh" | "sync-cache";
export type SourceAccessMode = "realtime" | "sync" | "hybrid" | (string & {});

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
  enabled?: boolean;
  accessMode?: SourceAccessMode;
  sync?: SourceSync;
  syncCron?: string;
  syncRetentionDays?: number;
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
}

export interface SessionSearchResponse {
  items: Session[];
  total: number;
  nextCursor?: string | null;
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
