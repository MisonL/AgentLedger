import { type FormEvent, useEffect, useMemo, useRef, useState } from "react";
import {
  useInfiniteQuery,
  QueryClient,
  QueryClientProvider,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import {
  approveMcpApproval,
  ApiError,
  cancelReplicationJob,
  createMcpApproval,
  createReplicationJob,
  createRuleApproval,
  createRuleAsset,
  createRuleAssetVersion,
  createSource,
  exportSessions,
  exportUsage,
  exchangeExternalAuthCode,
  fetchAlerts,
  fetchAuthProviders,
  fetchMcpApprovals,
  fetchMcpInvocations,
  fetchMcpPolicies,
  fetchReplicationJobs,
  fetchResidencyPolicy,
  fetchResidencyRegions,
  fetchRuleApprovals,
  fetchRuleAssetVersions,
  fetchRuleAssets,
  fetchUsageWeeklySummary,
  fetchSourceHealth,
  fetchSourceParseFailures,
  fetchHeatmap,
  fetchPricingCatalog,
  fetchSessionDetail,
  fetchSessionEvents,
  fetchSources,
  fetchUsageDaily,
  fetchUsageModels,
  fetchUsageMonthly,
  fetchUsageSessions,
  hasAccessToken,
  login,
  publishRuleAsset,
  rejectMcpApproval,
  rollbackRuleAsset,
  searchSessions,
  setUnauthorizedHandler,
  testSourceConnection,
  upsertMcpPolicy,
  upsertResidencyPolicy,
  updateAlertStatus,
  upsertPricingCatalog,
} from "./api";
import type {
  AlertItem,
  AlertMutableStatus,
  AlertSeverity,
  AlertStatus,
  AuthProviderItem,
  AuthLoginInput,
  CreateSourceInput,
  DataResidencyMode,
  ExportFormat,
  HeatmapCell,
  McpApprovalRequest,
  McpInvocationAudit,
  McpRiskLevel,
  McpToolDecision,
  McpToolPolicy,
  MetricKey,
  PricingCatalogEntry,
  PricingCatalogUpsertInput,
  RegionDescriptor,
  ReplicationJob,
  ReplicationJobStatus,
  RuleApproval,
  RuleApprovalDecision,
  RuleAsset,
  RuleAssetVersion,
  RuleLifecycleStatus,
  Session,
  SessionDetailResponse,
  SessionSearchInput,
  SessionSourceFreshness,
  SourceConnectionTestResponse,
  SourceHealth,
  TenantResidencyPolicy,
  SourceType,
  UsageAggregateFilters,
  UsageCostMode,
  UsageExportDimension,
} from "./types";
import "./App.css";

const SOURCE_TYPE_OPTIONS: Array<{ value: SourceType; label: string }> = [
  { value: "local", label: "本地（local）" },
  { value: "ssh", label: "远程 SSH（ssh）" },
  { value: "sync-cache", label: "同步缓存（sync-cache）" },
];

type ConsoleRoute =
  | "dashboard"
  | "sessions"
  | "analytics"
  | "governance"
  | "sources"
  | "pricing";

const ROUTE_ITEMS: Array<{
  key: ConsoleRoute;
  label: string;
  title: string;
  subtitle: string;
}> = [
  {
    key: "dashboard",
    label: "Dashboard",
    title: "AI 使用热力图",
    subtitle: "看总览与时间分布。",
  },
  {
    key: "sessions",
    label: "Sessions",
    title: "会话中心",
    subtitle: "按日检索会话并下钻事件流。",
  },
  {
    key: "analytics",
    label: "Analytics",
    title: "聚合分析",
    subtitle: "接入 daily/monthly/models/sessions 聚合接口。",
  },
  {
    key: "governance",
    label: "Governance",
    title: "治理中心",
    subtitle: "告警工作台与导出入口。",
  },
  {
    key: "sources",
    label: "Sources",
    title: "Sources 管理",
    subtitle: "管理来源并执行 test-connection。",
  },
  {
    key: "pricing",
    label: "Pricing",
    title: "Pricing Catalog",
    subtitle: "读取并保存模型单价目录。",
  },
];

const DEFAULT_ROUTE: ConsoleRoute = "dashboard";
const AUTH_CALLBACK_HASH_ROUTE = "/auth/callback";
const AUTH_EXTERNAL_PENDING_STORAGE_KEY =
  "agentledger.web-console.auth.external.pending";
const FALLBACK_LOCAL_PROVIDER: AuthProviderItem = {
  id: "local",
  type: "local",
  displayName: "邮箱密码登录",
  enabled: true,
};

interface ExternalAuthPendingState {
  providerId: string;
  state: string;
  redirectUri: string;
  codeVerifier?: string;
  createdAt: number;
}

interface SourceFormState {
  name: string;
  type: SourceType;
  location: string;
  enabled: boolean;
}

const INITIAL_SOURCE_FORM: SourceFormState = {
  name: "",
  type: "local",
  location: "",
  enabled: true,
};

interface LoginFormState {
  email: string;
  password: string;
}

const INITIAL_LOGIN_FORM: LoginFormState = {
  email: "",
  password: "",
};

interface PricingEntryFormState {
  model: string;
  inputPer1k: string;
  outputPer1k: string;
  currency: string;
}

interface SessionSearchFilters {
  keyword: string;
  clientType: string;
  tool: string;
  host: string;
  model: string;
  project: string;
}

const EMPTY_SESSION_SEARCH_FILTERS: SessionSearchFilters = {
  keyword: "",
  clientType: "",
  tool: "",
  host: "",
  model: "",
  project: "",
};

const ALERT_STATUS_FILTER_OPTIONS: Array<{ value: ""; label: string } | { value: AlertStatus; label: string }> = [
  { value: "", label: "全部状态" },
  { value: "open", label: "open" },
  { value: "acknowledged", label: "acknowledged" },
  { value: "resolved", label: "resolved" },
];

const ALERT_SEVERITY_FILTER_OPTIONS: Array<
  { value: ""; label: string } | { value: AlertSeverity; label: string }
> = [
  { value: "", label: "全部级别" },
  { value: "warning", label: "warning" },
  { value: "critical", label: "critical" },
];

const EXPORT_FORMAT_OPTIONS: Array<{ value: ExportFormat; label: string }> = [
  { value: "json", label: "JSON" },
  { value: "csv", label: "CSV" },
];

const USAGE_EXPORT_DIMENSION_OPTIONS: Array<{ value: UsageExportDimension; label: string }> = [
  { value: "daily", label: "daily" },
  { value: "weekly", label: "weekly" },
  { value: "monthly", label: "monthly" },
  { value: "models", label: "models" },
  { value: "sessions", label: "sessions" },
  { value: "heatmap", label: "heatmap" },
];

const WEEKLY_SUMMARY_METRIC_OPTIONS: Array<{ value: MetricKey; label: string }> = [
  { value: "tokens", label: "tokens" },
  { value: "cost", label: "cost" },
  { value: "sessions", label: "sessions" },
];

const WEEKLY_SUMMARY_TIMEZONE_OPTIONS: Array<{ value: string; label: string }> = [
  { value: "UTC", label: "UTC" },
  { value: "Asia/Shanghai", label: "Asia/Shanghai" },
  { value: "America/Los_Angeles", label: "America/Los_Angeles" },
];

const DATA_RESIDENCY_MODE_OPTIONS: Array<{ value: DataResidencyMode; label: string }> = [
  { value: "single_region", label: "single_region" },
  { value: "active_active", label: "active_active" },
];

const REPLICATION_STATUS_FILTER_OPTIONS: Array<
  { value: ""; label: string } | { value: ReplicationJobStatus; label: string }
> = [
  { value: "", label: "全部状态" },
  { value: "pending", label: "pending" },
  { value: "running", label: "running" },
  { value: "succeeded", label: "succeeded" },
  { value: "failed", label: "failed" },
  { value: "cancelled", label: "cancelled" },
];

const RULE_STATUS_FILTER_OPTIONS: Array<
  { value: ""; label: string } | { value: RuleLifecycleStatus; label: string }
> = [
  { value: "", label: "全部状态" },
  { value: "draft", label: "draft" },
  { value: "published", label: "published" },
  { value: "deprecated", label: "deprecated" },
];

const RULE_APPROVAL_DECISION_OPTIONS: Array<{
  value: RuleApprovalDecision;
  label: string;
}> = [
  { value: "approved", label: "approved" },
  { value: "rejected", label: "rejected" },
];

const MCP_RISK_LEVEL_OPTIONS: Array<{ value: McpRiskLevel; label: string }> = [
  { value: "low", label: "low" },
  { value: "medium", label: "medium" },
  { value: "high", label: "high" },
];

const MCP_DECISION_OPTIONS: Array<{ value: McpToolDecision; label: string }> = [
  { value: "allow", label: "allow" },
  { value: "deny", label: "deny" },
  { value: "require_approval", label: "require_approval" },
];

const MCP_APPROVAL_STATUS_FILTER_OPTIONS: Array<
  { value: ""; label: string } | { value: McpApprovalRequest["status"]; label: string }
> = [
  { value: "", label: "全部状态" },
  { value: "pending", label: "pending" },
  { value: "approved", label: "approved" },
  { value: "rejected", label: "rejected" },
];

function createEmptyPricingEntry(): PricingEntryFormState {
  return {
    model: "",
    inputPer1k: "",
    outputPer1k: "",
    currency: "USD",
  };
}

function isConsoleRoute(value: string): value is ConsoleRoute {
  return ROUTE_ITEMS.some((item) => item.key === value);
}

function readRouteFromHash(): ConsoleRoute {
  if (typeof window === "undefined") {
    return DEFAULT_ROUTE;
  }

  const normalized = window.location.hash.replace(/^#\/?/, "").trim().toLowerCase();
  return isConsoleRoute(normalized) ? normalized : DEFAULT_ROUTE;
}

function writeRouteToHash(route: ConsoleRoute) {
  if (typeof window === "undefined") {
    return;
  }
  const nextHash = `#/${route}`;
  if (window.location.hash !== nextHash) {
    window.location.hash = nextHash;
  }
}

function buildExternalAuthRedirectUri(): string {
  if (typeof window === "undefined") {
    return "";
  }
  return `${window.location.origin}${window.location.pathname}#${AUTH_CALLBACK_HASH_ROUTE}`;
}

function createExternalAuthState(providerId: string): string {
  const nonce =
    typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
      ? crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(36).slice(2, 12)}`;
  return `${providerId}:${nonce}`;
}

function bytesToBase64Url(bytes: Uint8Array): string {
  let binary = "";
  for (let index = 0; index < bytes.length; index += 1) {
    binary += String.fromCharCode(bytes[index]);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function createCodeVerifier(): string {
  if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
    const random = new Uint8Array(48);
    crypto.getRandomValues(random);
    return bytesToBase64Url(random);
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2, 18)}`;
}

async function createCodeChallenge(codeVerifier: string): Promise<string> {
  if (
    typeof crypto !== "undefined" &&
    typeof crypto.subtle !== "undefined" &&
    typeof crypto.subtle.digest === "function"
  ) {
    const digest = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(codeVerifier)
    );
    return bytesToBase64Url(new Uint8Array(digest));
  }
  return codeVerifier;
}

function saveExternalAuthPendingState(state: ExternalAuthPendingState) {
  if (typeof window === "undefined") {
    return;
  }

  try {
    window.sessionStorage.setItem(
      AUTH_EXTERNAL_PENDING_STORAGE_KEY,
      JSON.stringify(state)
    );
  } catch {
    // ignore storage failures to keep login flow available
  }
}

function readExternalAuthPendingState(): ExternalAuthPendingState | null {
  if (typeof window === "undefined") {
    return null;
  }

  try {
    const raw = window.sessionStorage.getItem(AUTH_EXTERNAL_PENDING_STORAGE_KEY);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw) as Partial<ExternalAuthPendingState>;
    if (
      typeof parsed.providerId !== "string" ||
      typeof parsed.state !== "string" ||
      typeof parsed.redirectUri !== "string" ||
      (parsed.codeVerifier !== undefined && typeof parsed.codeVerifier !== "string") ||
      typeof parsed.createdAt !== "number" ||
      !Number.isFinite(parsed.createdAt)
    ) {
      return null;
    }
    return {
      providerId: parsed.providerId,
      state: parsed.state,
      redirectUri: parsed.redirectUri,
      codeVerifier: parsed.codeVerifier,
      createdAt: parsed.createdAt,
    };
  } catch {
    return null;
  }
}

function clearExternalAuthPendingState() {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.sessionStorage.removeItem(AUTH_EXTERNAL_PENDING_STORAGE_KEY);
  } catch {
    // ignore storage failures
  }
}

interface AuthCallbackPayload {
  code: string;
  state?: string;
  providerId?: string;
  error?: string;
  errorDescription?: string;
}

function parseAuthCallbackPayload(
  hash: string,
  search: string
): AuthCallbackPayload | null {
  const normalized = hash.replace(/^#/, "");
  const [path, query = ""] = normalized.split("?", 2);
  if (path !== AUTH_CALLBACK_HASH_ROUTE) {
    return null;
  }

  const hashParams = new URLSearchParams(query);
  const searchParams = new URLSearchParams(search.replace(/^\?/, ""));
  const readParam = (...keys: string[]): string | undefined => {
    for (const key of keys) {
      const hashValue = hashParams.get(key)?.trim();
      if (hashValue) {
        return hashValue;
      }
      const searchValue = searchParams.get(key)?.trim();
      if (searchValue) {
        return searchValue;
      }
    }
    return undefined;
  };

  const code = readParam("code") ?? "";
  const state = readParam("state");
  const providerId =
    readParam("providerId", "provider");
  const error = readParam("error");
  const errorDescription = readParam("error_description", "errorDescription");

  return {
    code,
    state,
    providerId,
    error,
    errorDescription,
  };
}

function buildExternalAuthAuthorizeUrl(
  provider: AuthProviderItem,
  redirectUri: string,
  state: string,
  codeChallenge?: string
): string {
  if (!provider.authorizationUrl) {
    throw new Error("该登录提供方未配置 authorizationUrl。");
  }

  let url: URL;
  try {
    url = new URL(provider.authorizationUrl, window.location.origin);
  } catch {
    throw new Error("登录提供方 authorizationUrl 非法。");
  }

  if (!url.searchParams.has("response_type")) {
    url.searchParams.set("response_type", "code");
  }
  if (!url.searchParams.has("redirect_uri")) {
    url.searchParams.set("redirect_uri", redirectUri);
  }
  if (!url.searchParams.has("state")) {
    url.searchParams.set("state", state);
  }
  if (codeChallenge && !url.searchParams.has("code_challenge")) {
    url.searchParams.set("code_challenge", codeChallenge);
  }
  if (codeChallenge && !url.searchParams.has("code_challenge_method")) {
    url.searchParams.set("code_challenge_method", "S256");
  }
  if (!url.searchParams.has("provider")) {
    url.searchParams.set("provider", provider.id);
  }

  return url.toString();
}

function formatDateTime(isoDate: string): string {
  const date = new Date(isoDate);
  if (Number.isNaN(date.getTime())) {
    return isoDate;
  }

  return date.toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatOptionalDateTime(isoDate: string | null): string {
  return isoDate ? formatDateTime(isoDate) : "--";
}

function formatSourceFreshness(item: SessionSourceFreshness): string {
  const sourceLabel = item.sourceName ?? item.sourceId;
  const freshnessLabel =
    item.freshnessMinutes === null ? "--" : `${item.freshnessMinutes.toLocaleString("zh-CN")} 分钟`;
  const latencyLabel =
    item.avgLatencyMs === null ? "--" : `${Math.round(item.avgLatencyMs).toLocaleString("zh-CN")} ms`;

  return [
    `${sourceLabel}（${item.accessMode}）`,
    `新鲜度 ${freshnessLabel}`,
    `最近成功 ${formatOptionalDateTime(item.lastSuccessAt)}`,
    `最近失败 ${formatOptionalDateTime(item.lastFailureAt)}`,
    `失败 ${item.failureCount.toLocaleString("zh-CN")} 次`,
    `平均延迟 ${latencyLabel}`,
  ].join(" | ");
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "未知错误";
}

function triggerBrowserDownload(file: { blob: Blob; filename: string }) {
  if (typeof window === "undefined" || typeof document === "undefined") {
    return;
  }
  if (typeof URL.createObjectURL !== "function") {
    return;
  }

  const objectUrl = URL.createObjectURL(file.blob);
  const anchor = document.createElement("a");
  anchor.href = objectUrl;
  anchor.download = file.filename;
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(objectUrl);
}

function toTimeMs(value: string | null): number | null {
  if (!value) {
    return null;
  }
  const timestamp = Date.parse(value);
  return Number.isNaN(timestamp) ? null : timestamp;
}

function getSourceHealthStatus(health: SourceHealth): { label: string; className: string } {
  const successTime = toTimeMs(health.lastSuccessAt);
  const failureTime = toTimeMs(health.lastFailureAt);
  const latestFailed =
    failureTime !== null && (successTime === null || failureTime > successTime);

  if (latestFailed || (successTime === null && health.failureCount > 0)) {
    return { label: "异常", className: "is-error" };
  }
  if (successTime !== null) {
    return { label: "健康", className: "is-success" };
  }
  return { label: "未知", className: "is-unknown" };
}

function toDateKey(isoDate: string): string {
  return isoDate.slice(0, 10);
}

function nextDateKey(dateKey: string): string {
  const date = new Date(`${dateKey}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + 1);
  return date.toISOString().slice(0, 10);
}

function isDateKey(value: string | null | undefined): value is string {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function createDateSeries(days: number): string[] {
  const end = new Date();
  end.setUTCHours(0, 0, 0, 0);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - (days - 1));

  const result: string[] = [];
  for (let i = 0; i < days; i += 1) {
    const date = new Date(start);
    date.setUTCDate(start.getUTCDate() + i);
    result.push(date.toISOString().slice(0, 10));
  }

  return result;
}

function getMetricValue(cell: HeatmapCell | undefined, metric: MetricKey): number {
  if (!cell) {
    return 0;
  }

  if (metric === "tokens") {
    return cell.tokens;
  }
  if (metric === "cost") {
    return cell.cost;
  }
  return cell.sessions;
}

function getIntensityLevel(value: number, max: number): number {
  if (value <= 0 || max <= 0) {
    return 0;
  }

  const ratio = value / max;
  if (ratio <= 0.25) {
    return 1;
  }
  if (ratio <= 0.5) {
    return 2;
  }
  if (ratio <= 0.75) {
    return 3;
  }
  return 4;
}

function formatMetric(value: number, metric: MetricKey): string {
  if (metric === "tokens") {
    return `${value.toLocaleString("zh-CN")} tokens`;
  }
  if (metric === "cost") {
    return `$${value.toFixed(2)}`;
  }
  return `${value.toLocaleString("zh-CN")} sessions`;
}

function todayDateKey(): string {
  return new Date().toISOString().slice(0, 10);
}

function daysAgoDateKey(days: number): string {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - days);
  return date.toISOString().slice(0, 10);
}

function parsePriceNumber(raw: string): number | null {
  const normalized = raw.trim();
  if (!normalized) {
    return null;
  }
  const value = Number(normalized);
  if (!Number.isFinite(value) || value < 0) {
    return null;
  }
  return value;
}

interface UsageCostPresentation {
  rawCost: number | null;
  estimatedCost: number | null;
  totalCost: number;
  label: string;
}

interface UsageCostCandidate {
  cost?: number;
  costRaw?: number;
  costEstimated?: number;
  costMode?: UsageCostMode;
  rawCost?: number;
  estimatedCost?: number;
  totalCost?: number;
  costLabel?: string;
  costBasis?: string;
}

function toFiniteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function normalizeUsageCostMode(value: unknown): UsageCostMode | null {
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  if (
    normalized === "raw" ||
    normalized === "estimated" ||
    normalized === "reported" ||
    normalized === "mixed" ||
    normalized === "none"
  ) {
    return normalized;
  }
  return null;
}

function formatUsageCostModeLabel(mode: UsageCostMode | null): string | null {
  if (mode === "raw") {
    return "raw";
  }
  if (mode === "estimated") {
    return "estimated";
  }
  if (mode === "reported" || mode === "none") {
    return "total";
  }
  if (mode === "mixed") {
    return "raw + estimated";
  }
  return null;
}

function resolveUsageCost(candidate: UsageCostCandidate): UsageCostPresentation {
  const contractRawCost = toFiniteNumber(candidate.costRaw);
  const contractEstimatedCost = toFiniteNumber(candidate.costEstimated);
  const legacyRawCost = toFiniteNumber(candidate.rawCost);
  const legacyEstimatedCost = toFiniteNumber(candidate.estimatedCost);
  const providedCost = toFiniteNumber(candidate.cost);
  const legacyTotal = toFiniteNumber(candidate.totalCost);
  const costMode = normalizeUsageCostMode(candidate.costMode);

  let rawCost = contractRawCost ?? legacyRawCost;
  let estimatedCost = contractEstimatedCost ?? legacyEstimatedCost;

  const hasContractFields =
    costMode !== null || contractRawCost !== null || contractEstimatedCost !== null;

  if (hasContractFields && providedCost !== null) {
    if ((costMode === "raw" || costMode === "reported") && rawCost === null) {
      rawCost = providedCost;
    }
    if (costMode === "estimated" && estimatedCost === null) {
      estimatedCost = providedCost;
    }
  }

  const hasSplitCost = rawCost !== null || estimatedCost !== null;
  const totalCost = hasContractFields
    ? providedCost ?? (rawCost ?? 0) + (estimatedCost ?? 0)
    : legacyTotal ?? (hasSplitCost ? (rawCost ?? 0) + (estimatedCost ?? 0) : providedCost ?? 0);

  return {
    rawCost: rawCost ?? (hasContractFields || hasSplitCost ? null : providedCost ?? 0),
    estimatedCost,
    totalCost,
    label:
      formatUsageCostModeLabel(costMode) ??
      normalizeOptionalText(candidate.costLabel ?? candidate.costBasis ?? "") ??
      (hasSplitCost ? "raw + estimated" : "raw"),
  };
}

function calculateChainRatio(current: number, previous: number): number | null {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || Math.abs(previous) < 0.000001) {
    return null;
  }
  return (current - previous) / Math.abs(previous);
}

function formatChainRatio(value: number | null): string {
  if (value === null) {
    return "--";
  }
  const signedPrefix = value > 0 ? "+" : "";
  return `${signedPrefix}${(value * 100).toFixed(1)}%`;
}

function chainRatioClass(value: number | null): string {
  if (value === null || Math.abs(value) < 0.000001) {
    return "is-flat";
  }
  return value > 0 ? "is-up" : "is-down";
}

function buildPolylinePath(points: Array<{ x: number; y: number }>): string {
  return points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");
}

function buildAreaPath(points: Array<{ x: number; y: number }>, baseY: number): string {
  if (points.length === 0) {
    return "";
  }
  const first = points[0];
  const last = points[points.length - 1];
  return `${buildPolylinePath(points)} L ${last.x} ${baseY} L ${first.x} ${baseY} Z`;
}

function normalizeOptionalText(value: string): string | undefined {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function mapPricingEntryToForm(entry: PricingCatalogEntry): PricingEntryFormState {
  return {
    model: entry.model,
    inputPer1k: String(entry.inputPer1k),
    outputPer1k: String(entry.outputPer1k),
    currency: entry.currency ?? "USD",
  };
}

function normalizePricingForm(
  rows: PricingEntryFormState[]
): { success: true; entries: PricingCatalogEntry[] } | { success: false; message: string } {
  const entries: PricingCatalogEntry[] = [];

  for (const row of rows) {
    const model = row.model.trim();
    const inputPer1k = parsePriceNumber(row.inputPer1k);
    const outputPer1k = parsePriceNumber(row.outputPer1k);

    if (!model && inputPer1k === null && outputPer1k === null) {
      continue;
    }

    if (!model) {
      return {
        success: false,
        message: "pricing 条目缺少 model。",
      };
    }
    if (inputPer1k === null || outputPer1k === null) {
      return {
        success: false,
        message: `pricing 条目 ${model} 的 input/output 单价必须是 >= 0 的数字。`,
      };
    }

    const currency = row.currency.trim();
    entries.push({
      model,
      inputPer1k,
      outputPer1k,
      currency: currency.length > 0 ? currency : undefined,
    });
  }

  if (entries.length === 0) {
    return {
      success: false,
      message: "至少保留一个 pricing 条目。",
    };
  }

  return {
    success: true,
    entries,
  };
}

interface LoginPageProps {
  authMessage: string | null;
  onLoggedIn: () => void;
}

function LoginPage({ authMessage, onLoggedIn }: LoginPageProps) {
  const [loginForm, setLoginForm] = useState<LoginFormState>(INITIAL_LOGIN_FORM);
  const [formError, setFormError] = useState<string | null>(null);
  const callbackHandledRef = useRef<string | null>(null);
  const [authCallback, setAuthCallback] = useState<AuthCallbackPayload | null>(() => {
    if (typeof window === "undefined") {
      return null;
    }
    return parseAuthCallbackPayload(window.location.hash, window.location.search);
  });

  const providersQuery = useQuery({
    queryKey: ["auth-providers"],
    queryFn: ({ signal }) => fetchAuthProviders(signal),
    staleTime: 60_000,
    retry: 1,
  });

  const providers = useMemo(() => {
    const items = providersQuery.data?.items ?? [];
    return items.length > 0 ? items : [FALLBACK_LOCAL_PROVIDER];
  }, [providersQuery.data?.items]);

  const localProviderEnabled = providers.some(
    (provider) => provider.id === "local" && provider.enabled
  );
  const externalProviders = providers.filter(
    (provider) =>
      provider.id !== "local" &&
      provider.enabled &&
      typeof provider.authorizationUrl === "string" &&
      provider.authorizationUrl.trim().length > 0
  );

  const loginMutation = useMutation({
    mutationFn: (input: AuthLoginInput) => login(input),
    onSuccess: () => {
      setLoginForm(INITIAL_LOGIN_FORM);
      setFormError(null);
      onLoggedIn();
    },
  });

  const externalExchangeMutation = useMutation({
    mutationFn: ({
      providerId,
      code,
      redirectUri,
      state,
      codeVerifier,
    }: {
      providerId: string;
      code: string;
      redirectUri: string;
      state?: string;
      codeVerifier?: string;
    }) =>
      exchangeExternalAuthCode({
        providerId,
        code,
        redirectUri,
        state,
        codeVerifier,
      }),
    onSuccess: () => {
      clearExternalAuthPendingState();
      setFormError(null);
      onLoggedIn();
    },
  });

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const handleHashChange = () => {
      setAuthCallback(
        parseAuthCallbackPayload(window.location.hash, window.location.search)
      );
    };

    window.addEventListener("hashchange", handleHashChange);
    handleHashChange();

    return () => {
      window.removeEventListener("hashchange", handleHashChange);
    };
  }, []);

  useEffect(() => {
    if (!authCallback) {
      return;
    }

    const callbackKey = [
      authCallback.code,
      authCallback.state ?? "",
      authCallback.providerId ?? "",
      authCallback.error ?? "",
    ].join("|");
    if (callbackHandledRef.current === callbackKey) {
      return;
    }
    callbackHandledRef.current = callbackKey;

    if (authCallback.error) {
      const message = authCallback.errorDescription ?? authCallback.error;
      setFormError(`外部登录失败：${message}`);
      clearExternalAuthPendingState();
      return;
    }

    if (!authCallback.code) {
      setFormError("外部登录回调缺少授权码，请重试。");
      clearExternalAuthPendingState();
      return;
    }

    const pending = readExternalAuthPendingState();
    const providerId = authCallback.providerId ?? pending?.providerId;
    if (!providerId) {
      setFormError("无法识别外部登录提供方，请重新发起登录。");
      clearExternalAuthPendingState();
      return;
    }

    if (pending?.state && authCallback.state && pending.state !== authCallback.state) {
      setFormError("外部登录 state 校验失败，请重新发起登录。");
      clearExternalAuthPendingState();
      return;
    }

    externalExchangeMutation.mutate({
      providerId,
      code: authCallback.code,
      redirectUri: pending?.redirectUri ?? buildExternalAuthRedirectUri(),
      state: authCallback.state ?? pending?.state,
      codeVerifier: pending?.codeVerifier,
    });
  }, [authCallback, externalExchangeMutation]);

  async function handleExternalLoginStart(provider: AuthProviderItem) {
    setFormError(null);

    try {
      const redirectUri = buildExternalAuthRedirectUri();
      const state = createExternalAuthState(provider.id);
      const codeVerifier = createCodeVerifier();
      const codeChallenge = await createCodeChallenge(codeVerifier);
      saveExternalAuthPendingState({
        providerId: provider.id,
        state,
        redirectUri,
        codeVerifier,
        createdAt: Date.now(),
      });

      const authorizeUrl = buildExternalAuthAuthorizeUrl(
        provider,
        redirectUri,
        state,
        codeChallenge
      );
      window.location.assign(authorizeUrl);
    } catch (error) {
      setFormError(`发起外部登录失败：${toErrorMessage(error)}`);
      clearExternalAuthPendingState();
    }
  }

  function handleLoginSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!localProviderEnabled) {
      setFormError("当前环境未启用本地账号登录。");
      return;
    }

    const email = loginForm.email.trim();
    const password = loginForm.password.trim();
    if (!email || !password) {
      setFormError("邮箱和密码不能为空。");
      return;
    }

    setFormError(null);
    loginMutation.mutate({
      email,
      password,
    });
  }

  return (
    <main className="page-shell auth-shell">
      <section className="panel auth-panel">
        <header>
          <div>
            <p className="eyebrow">AgentLedger 企业治理台</p>
            <h1>登录控制台</h1>
            <p className="subtitle">请先登录后再访问各业务页面。</p>
          </div>
        </header>

        {authMessage ? <p className="feedback error">{authMessage}</p> : null}

        {providersQuery.isLoading ? <p className="feedback info">正在加载登录方式...</p> : null}
        {providersQuery.isError ? (
          <p className="feedback error">
            登录方式加载失败：{toErrorMessage(providersQuery.error)}
          </p>
        ) : null}

        {localProviderEnabled ? (
          <form className="login-form" onSubmit={handleLoginSubmit}>
            <label htmlFor="login-email">邮箱</label>
            <input
              id="login-email"
              type="email"
              autoComplete="email"
              placeholder="例如：owner@example.com"
              value={loginForm.email}
              onChange={(event) =>
                setLoginForm((prev) => ({
                  ...prev,
                  email: event.target.value,
                }))
              }
            />

            <label htmlFor="login-password">密码</label>
            <input
              id="login-password"
              type="password"
              autoComplete="current-password"
              placeholder="请输入密码"
              value={loginForm.password}
              onChange={(event) =>
                setLoginForm((prev) => ({
                  ...prev,
                  password: event.target.value,
                }))
              }
            />

            <button
              type="submit"
              className="submit-button"
              disabled={loginMutation.isPending || externalExchangeMutation.isPending}
            >
              {loginMutation.isPending ? "登录中..." : "登录"}
            </button>
          </form>
        ) : (
          <p className="feedback info">当前环境未启用本地账号登录，请使用企业登录。</p>
        )}

        {externalProviders.length > 0 ? (
          <section className="external-login">
            <p className="external-login-title">或使用企业身份提供方</p>
            <div className="external-provider-list">
              {externalProviders.map((provider) => (
                <button
                  key={provider.id}
                  type="button"
                  className="external-provider-button"
                  onClick={() => handleExternalLoginStart(provider)}
                  disabled={
                    loginMutation.isPending ||
                    externalExchangeMutation.isPending ||
                    providersQuery.isLoading
                  }
                >
                  {provider.displayName}
                </button>
              ))}
            </div>
          </section>
        ) : null}

        {externalExchangeMutation.isPending ? (
          <p className="feedback info">正在完成外部登录回调，请稍候...</p>
        ) : null}
        {formError ? <p className="feedback error">{formError}</p> : null}
        {loginMutation.isError ? (
          <p className="feedback error">登录失败：{toErrorMessage(loginMutation.error)}</p>
        ) : null}
        {externalExchangeMutation.isError ? (
          <p className="feedback error">
            外部登录失败：{toErrorMessage(externalExchangeMutation.error)}
          </p>
        ) : null}
      </section>
    </main>
  );
}

interface DashboardPageProps {
  onDrilldownDate?: (dateKey: string) => void;
}

function DashboardPage({ onDrilldownDate }: DashboardPageProps) {
  const [metric, setMetric] = useState<MetricKey>("tokens");
  const [selectedDate, setSelectedDate] = useState<string | null>(null);

  const heatmapQuery = useQuery({
    queryKey: ["usage-heatmap"],
    queryFn: ({ signal }) => fetchHeatmap(signal),
    staleTime: 20_000,
  });

  const heatmapCells = heatmapQuery.data?.cells ?? [];
  const cellMap = useMemo(() => {
    const map = new Map<string, HeatmapCell>();
    for (const cell of heatmapCells) {
      map.set(toDateKey(cell.date), cell);
    }
    return map;
  }, [heatmapCells]);

  const defaultDate = useMemo(() => {
    if (selectedDate) {
      return selectedDate;
    }
    if (heatmapCells.length > 0) {
      return toDateKey(heatmapCells[heatmapCells.length - 1].date);
    }
    return createDateSeries(84).at(-1) ?? null;
  }, [heatmapCells, selectedDate]);

  const series = useMemo(() => createDateSeries(84), []);
  const maxValue = useMemo(() => {
    let max = 0;
    for (const dateKey of series) {
      const value = getMetricValue(cellMap.get(dateKey), metric);
      if (value > max) {
        max = value;
      }
    }
    return max;
  }, [cellMap, metric, series]);

  const summary = heatmapQuery.data?.summary;

  return (
    <>
      <section className="kpi-grid" aria-label="KPI 概览">
        <article className="kpi-card">
          <h2>总 Tokens</h2>
          <strong>{summary?.tokens.toLocaleString("zh-CN") ?? "--"}</strong>
        </article>
        <article className="kpi-card">
          <h2>总 Cost</h2>
          <strong>{summary ? `$${summary.cost.toFixed(2)}` : "--"}</strong>
        </article>
        <article className="kpi-card">
          <h2>总 Sessions</h2>
          <strong>{summary?.sessions.toLocaleString("zh-CN") ?? "--"}</strong>
        </article>
      </section>

      <section className="panel heatmap-panel">
        <header>
          <h2>GitHub 风格热力图</h2>
          <div className="metric-switch" role="tablist" aria-label="指标切换">
            {(["tokens", "cost", "sessions"] as MetricKey[]).map((item) => (
              <button
                key={item}
                type="button"
                role="tab"
                aria-selected={metric === item}
                className={metric === item ? "is-active" : ""}
                onClick={() => setMetric(item)}
              >
                {item}
              </button>
            ))}
          </div>
        </header>

        <p>
          当前指标：<span>{metric}</span>
          {defaultDate ? ` | 当前下钻日期：${defaultDate}` : ""}
        </p>

        {heatmapQuery.isLoading ? <p className="feedback info">热力图加载中...</p> : null}
        {heatmapQuery.isError ? <p className="feedback error">热力图加载失败，请稍后重试。</p> : null}

        <div className="heatmap-grid" role="grid" aria-label="使用热力图">
          {series.map((dateKey) => {
            const cell = cellMap.get(dateKey);
            const value = getMetricValue(cell, metric);
            const level = getIntensityLevel(value, maxValue);
            const isSelected = defaultDate === dateKey;

            return (
              <button
                key={dateKey}
                role="gridcell"
                type="button"
                className={`heatmap-cell level-${level} ${isSelected ? "is-selected" : ""}`}
                onClick={() => {
                  setSelectedDate(dateKey);
                  onDrilldownDate?.(dateKey);
                }}
                title={`${dateKey} | ${formatMetric(value, metric)}`}
                aria-label={`${dateKey} ${formatMetric(value, metric)}`}
              />
            );
          })}
        </div>

        <div className="legend">
          <span>低</span>
          <div className="legend-steps">
            <i className="level-0" />
            <i className="level-1" />
            <i className="level-2" />
            <i className="level-3" />
            <i className="level-4" />
          </div>
          <span>高</span>
        </div>
      </section>
    </>
  );
}

interface SessionsPageProps {
  initialDateKey?: string | null;
}

function SessionsPage({ initialDateKey }: SessionsPageProps) {
  const [dateKey, setDateKey] = useState(() =>
    isDateKey(initialDateKey) ? initialDateKey : todayDateKey()
  );
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null);
  const [filterForm, setFilterForm] = useState<SessionSearchFilters>(EMPTY_SESSION_SEARCH_FILTERS);
  const [appliedFilters, setAppliedFilters] = useState<SessionSearchFilters>(
    EMPTY_SESSION_SEARCH_FILTERS
  );

  useEffect(() => {
    if (isDateKey(initialDateKey) && initialDateKey !== dateKey) {
      setDateKey(initialDateKey);
    }
  }, [dateKey, initialDateKey]);

  const normalizedFilters = useMemo<Partial<SessionSearchInput>>(() => {
    const normalized: Partial<SessionSearchInput> = {};

    const keyword = normalizeOptionalText(appliedFilters.keyword);
    if (keyword) {
      normalized.keyword = keyword;
    }

    const clientType = normalizeOptionalText(appliedFilters.clientType);
    if (clientType) {
      normalized.clientType = clientType;
    }

    const tool = normalizeOptionalText(appliedFilters.tool);
    if (tool) {
      normalized.tool = tool;
    }

    const host = normalizeOptionalText(appliedFilters.host);
    if (host) {
      normalized.host = host;
    }

    const model = normalizeOptionalText(appliedFilters.model);
    if (model) {
      normalized.model = model;
    }

    const project = normalizeOptionalText(appliedFilters.project);
    if (project) {
      normalized.project = project;
    }

    return normalized;
  }, [appliedFilters]);

  const sessionSearchInput = useMemo<SessionSearchInput>(
    () => ({
      from: `${dateKey}T00:00:00.000Z`,
      to: `${nextDateKey(dateKey)}T00:00:00.000Z`,
      limit: 50,
      ...normalizedFilters,
    }),
    [dateKey, normalizedFilters]
  );

  const hasAppliedFilters = Object.keys(normalizedFilters).length > 0;

  const sessionsQuery = useInfiniteQuery({
    queryKey: ["sessions-search", sessionSearchInput],
    queryFn: ({ pageParam, signal }) =>
      searchSessions(
        {
          ...sessionSearchInput,
          cursor: typeof pageParam === "string" ? pageParam : undefined,
        },
        signal
      ),
    initialPageParam: null as string | null,
    getNextPageParam: (lastPage) => lastPage.nextCursor ?? undefined,
    staleTime: 20_000,
  });

  const sessions = useMemo(
    () => sessionsQuery.data?.pages.flatMap((page) => page.items) ?? [],
    [sessionsQuery.data?.pages]
  );
  const sourceFreshness = sessionsQuery.data?.pages[0]?.sourceFreshness ?? [];
  const sourceFreshnessText =
    sourceFreshness.length > 0
      ? `来源新鲜度：${sourceFreshness.map((item) => formatSourceFreshness(item)).join("；")}`
      : "来源新鲜度：暂无数据";

  useEffect(() => {
    if (sessions.length === 0) {
      setSelectedSessionId(null);
      return;
    }

    if (!selectedSessionId || !sessions.some((item) => item.id === selectedSessionId)) {
      setSelectedSessionId(sessions[0].id);
    }
  }, [selectedSessionId, sessions]);

  const eventsQuery = useInfiniteQuery({
    queryKey: ["session-events", selectedSessionId],
    enabled: Boolean(selectedSessionId),
    queryFn: ({ pageParam, signal }) =>
      fetchSessionEvents(
        selectedSessionId!,
        50,
        typeof pageParam === "string" ? pageParam : undefined,
        signal
      ),
    initialPageParam: null as string | null,
    getNextPageParam: (lastPage) => lastPage.nextCursor ?? undefined,
    staleTime: 20_000,
  });

  const detailQuery = useQuery({
    queryKey: ["session-detail", selectedSessionId],
    enabled: Boolean(selectedSessionId),
    queryFn: ({ signal }) => fetchSessionDetail(selectedSessionId!, signal),
    staleTime: 20_000,
  });

  const eventItems = useMemo(
    () => eventsQuery.data?.pages.flatMap((page) => page.items) ?? [],
    [eventsQuery.data?.pages]
  );
  const sessionDetail = detailQuery.data as SessionDetailResponse | undefined;

  function updateFilterField(field: keyof SessionSearchFilters, value: string) {
    setFilterForm((prev) => ({
      ...prev,
      [field]: value,
    }));
  }

  function handleFilterSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setAppliedFilters({
      keyword: filterForm.keyword,
      clientType: filterForm.clientType,
      tool: filterForm.tool,
      host: filterForm.host,
      model: filterForm.model,
      project: filterForm.project,
    });
    setSelectedSessionId(null);
  }

  function handleFilterReset() {
    setFilterForm(EMPTY_SESSION_SEARCH_FILTERS);
    setAppliedFilters(EMPTY_SESSION_SEARCH_FILTERS);
    setSelectedSessionId(null);
  }

  return (
    <>
      <section className="panel">
        <header>
          <h2>会话列表</h2>
          <p>
            共 {sessionsQuery.data?.pages[0]?.total ?? 0} 条
            {hasAppliedFilters ? "（已应用筛选）" : ""}
          </p>
          <p aria-label="来源新鲜度">{sourceFreshnessText}</p>
        </header>

        <form className="session-filter-form" onSubmit={handleFilterSubmit}>
          <div className="filters-row">
            <label className="inline-field" htmlFor="session-date">
              日期
              <input
                id="session-date"
                type="date"
                value={dateKey}
                onChange={(event) => setDateKey(event.target.value)}
              />
            </label>

            <label className="inline-field" htmlFor="session-keyword">
              关键词
              <input
                id="session-keyword"
                type="text"
                placeholder="例如：deploy failed"
                value={filterForm.keyword}
                onChange={(event) => updateFilterField("keyword", event.target.value)}
              />
            </label>

            <label className="inline-field" htmlFor="session-client-type">
              客户端类型
              <select
                id="session-client-type"
                value={filterForm.clientType}
                onChange={(event) => updateFilterField("clientType", event.target.value)}
              >
                <option value="">全部</option>
                <option value="cli">CLI</option>
                <option value="ide">IDE</option>
              </select>
            </label>

            <label className="inline-field" htmlFor="session-tool">
              工具
              <input
                id="session-tool"
                type="text"
                placeholder="例如：Codex CLI"
                value={filterForm.tool}
                onChange={(event) => updateFilterField("tool", event.target.value)}
              />
            </label>

            <label className="inline-field" htmlFor="session-host">
              主机
              <input
                id="session-host"
                type="text"
                placeholder="例如：devbox-01"
                value={filterForm.host}
                onChange={(event) => updateFilterField("host", event.target.value)}
              />
            </label>

            <label className="inline-field" htmlFor="session-model">
              模型
              <input
                id="session-model"
                type="text"
                placeholder="例如：gpt-5-codex"
                value={filterForm.model}
                onChange={(event) => updateFilterField("model", event.target.value)}
              />
            </label>

            <label className="inline-field" htmlFor="session-project">
              项目
              <input
                id="session-project"
                type="text"
                placeholder="例如：agentledger"
                value={filterForm.project}
                onChange={(event) => updateFilterField("project", event.target.value)}
              />
            </label>
          </div>

          <div className="button-row">
            <button type="submit" className="submit-button">
              应用筛选
            </button>
            <button
              type="button"
              className="submit-button secondary-button"
              onClick={handleFilterReset}
            >
              重置
            </button>
          </div>
        </form>

        {sessionsQuery.isLoading ? <p className="feedback info">会话加载中...</p> : null}
        {sessionsQuery.isError ? (
          <p className="feedback error">会话加载失败：{toErrorMessage(sessionsQuery.error)}</p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>开始时间</th>
                <th>工具</th>
                <th>模型</th>
                <th>来源主机</th>
                <th>Tokens</th>
                <th>Cost</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {sessions.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={7}>
                    暂无会话数据
                  </td>
                </tr>
              ) : (
                sessions.map((session: Session) => (
                  <tr
                    key={session.id}
                    className={selectedSessionId === session.id ? "is-selected-row" : ""}
                  >
                    <td>{formatDateTime(session.startedAt)}</td>
                    <td>{session.tool}</td>
                    <td>{session.model}</td>
                    <td>{session.sourceId}</td>
                    <td>{session.tokens.toLocaleString("zh-CN")}</td>
                    <td>${session.cost.toFixed(2)}</td>
                    <td>
                      <button
                        type="button"
                        className="table-action"
                        onClick={() => setSelectedSessionId(session.id)}
                      >
                        查看事件
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        {sessionsQuery.hasNextPage ? (
          <div className="button-row">
            <button
              type="button"
              className="submit-button secondary-button"
              onClick={() => void sessionsQuery.fetchNextPage()}
              disabled={sessionsQuery.isFetchingNextPage}
            >
              {sessionsQuery.isFetchingNextPage ? "加载中..." : "加载更多会话"}
            </button>
          </div>
        ) : null}
      </section>

      <section className="panel">
        <header>
          <h2>会话详情</h2>
          <p>{selectedSessionId ? `sessionId: ${selectedSessionId}` : "请选择会话"}</p>
        </header>

        {detailQuery.isLoading ? <p className="feedback info">会话详情加载中...</p> : null}
        {detailQuery.isError ? (
          <p className="feedback error">会话详情加载失败：{toErrorMessage(detailQuery.error)}</p>
        ) : null}

        {sessionDetail ? (
          <div className="session-detail-grid">
            <div className="session-detail-card">
              <h3>基础信息</h3>
              <p>工具：{sessionDetail.session?.tool ?? sessionDetail.tool}</p>
              <p>模型：{sessionDetail.session?.model ?? sessionDetail.model}</p>
              <p>开始：{formatDateTime(sessionDetail.session?.startedAt ?? sessionDetail.startedAt)}</p>
              <p>
                结束：
                {sessionDetail.session?.endedAt ?? sessionDetail.endedAt
                  ? formatDateTime(sessionDetail.session?.endedAt ?? sessionDetail.endedAt ?? "")
                  : "--"}
              </p>
              <p>消息数：{sessionDetail.session?.messageCount ?? sessionDetail.messageCount}</p>
            </div>

            <div className="session-detail-card">
              <h3>Token 分解</h3>
              <p>input：{sessionDetail.tokenBreakdown.inputTokens.toLocaleString("zh-CN")}</p>
              <p>output：{sessionDetail.tokenBreakdown.outputTokens.toLocaleString("zh-CN")}</p>
              <p>cache read：{sessionDetail.tokenBreakdown.cacheReadTokens.toLocaleString("zh-CN")}</p>
              <p>
                cache write：
                {sessionDetail.tokenBreakdown.cacheWriteTokens.toLocaleString("zh-CN")}
              </p>
              <p>
                reasoning：
                {sessionDetail.tokenBreakdown.reasoningTokens.toLocaleString("zh-CN")}
              </p>
              <p>
                total：<strong>{sessionDetail.tokenBreakdown.totalTokens.toLocaleString("zh-CN")}</strong>
              </p>
            </div>

            <div className="session-detail-card">
              <h3>来源追溯</h3>
              <p>sourceId：{sessionDetail.sourceTrace.sourceId}</p>
              <p>sourceName：{sessionDetail.sourceTrace.sourceName ?? "--"}</p>
              <p>provider：{sessionDetail.sourceTrace.provider ?? "--"}</p>
              <p>path：{sessionDetail.sourceTrace.path ?? "--"}</p>
            </div>
          </div>
        ) : null}
      </section>

      <section className="panel">
        <header>
          <h2>Session 事件流</h2>
          <p>{selectedSessionId ? `sessionId: ${selectedSessionId}` : "请选择会话"}</p>
        </header>

        {eventsQuery.isLoading ? <p className="feedback info">事件加载中...</p> : null}
        {eventsQuery.isError ? (
          <p className="feedback error">事件加载失败：{toErrorMessage(eventsQuery.error)}</p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>时间</th>
                <th>类型</th>
                <th>角色</th>
                <th>模型</th>
                <th>文本</th>
                <th>Cost</th>
              </tr>
            </thead>
            <tbody>
              {eventItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={6}>
                    暂无事件
                  </td>
                </tr>
              ) : (
                eventItems.map((event) => (
                  <tr key={event.id}>
                    <td>{formatDateTime(event.timestamp)}</td>
                    <td>{event.eventType}</td>
                    <td>{event.role ?? "--"}</td>
                    <td>{event.model ?? "--"}</td>
                    <td className="event-text-cell">{event.text ?? "--"}</td>
                    <td>${event.cost.toFixed(4)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        {eventsQuery.hasNextPage ? (
          <div className="button-row">
            <button
              type="button"
              className="submit-button secondary-button"
              onClick={() => void eventsQuery.fetchNextPage()}
              disabled={eventsQuery.isFetchingNextPage}
            >
              {eventsQuery.isFetchingNextPage ? "加载中..." : "加载更多事件"}
            </button>
          </div>
        ) : null}
      </section>
    </>
  );
}

function AnalyticsPage() {
  const [fromDate, setFromDate] = useState(() => daysAgoDateKey(29));
  const [toDate, setToDate] = useState(() => todayDateKey());

  const rangeValid = fromDate <= toDate;

  const filters = useMemo<UsageAggregateFilters>(
    () => ({
      from: `${fromDate}T00:00:00.000Z`,
      to: `${toDate}T23:59:59.999Z`,
      limit: 50,
    }),
    [fromDate, toDate]
  );

  const dailyQuery = useQuery({
    queryKey: ["usage-daily", filters],
    enabled: rangeValid,
    queryFn: ({ signal }) => fetchUsageDaily(filters, signal),
    staleTime: 20_000,
  });

  const monthlyQuery = useQuery({
    queryKey: ["usage-monthly", filters],
    enabled: rangeValid,
    queryFn: ({ signal }) => fetchUsageMonthly(filters, signal),
    staleTime: 20_000,
  });

  const modelsQuery = useQuery({
    queryKey: ["usage-models", filters],
    enabled: rangeValid,
    queryFn: ({ signal }) => fetchUsageModels(filters, signal),
    staleTime: 20_000,
  });

  const sessionBreakdownQuery = useQuery({
    queryKey: ["usage-sessions", filters],
    enabled: rangeValid,
    queryFn: ({ signal }) => fetchUsageSessions(filters, signal),
    staleTime: 20_000,
  });

  const dailyRows = useMemo(() => {
    const sorted = [...(dailyQuery.data?.items ?? [])].sort((left, right) =>
      left.date.localeCompare(right.date)
    );

    return sorted.map((item, index) => {
      const previous = sorted[index - 1];
      const currentCost = resolveUsageCost(item);
      const previousCost = previous ? resolveUsageCost(previous) : null;

      return {
        item,
        cost: currentCost,
        tokensRatio: previous
          ? calculateChainRatio(item.tokens, previous.tokens)
          : null,
        sessionsRatio: previous
          ? calculateChainRatio(item.sessions, previous.sessions)
          : null,
        totalCostRatio:
          previousCost === null
            ? null
            : calculateChainRatio(currentCost.totalCost, previousCost.totalCost),
      };
    });
  }, [dailyQuery.data?.items]);

  const latestDaily = dailyRows.at(-1) ?? null;

  const monthlyRows = useMemo(
    () =>
      [...(monthlyQuery.data?.items ?? [])]
        .sort((left, right) => left.month.localeCompare(right.month))
        .map((item) => ({
          item,
          cost: resolveUsageCost(item),
        })),
    [monthlyQuery.data?.items]
  );

  const monthlyTrend = useMemo(() => {
    if (monthlyRows.length === 0) {
      return null;
    }

    const width = 720;
    const height = 220;
    const paddingX = 34;
    const paddingY = 26;
    const innerWidth = width - paddingX * 2;
    const innerHeight = height - paddingY * 2;
    const bottomY = height - paddingY;

    const values = monthlyRows.map((row) => row.cost.totalCost);
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);
    const span = maxValue - minValue;
    const isFlatLine = span < 0.000001;

    const points = monthlyRows.map((row, index) => {
      const ratio = monthlyRows.length === 1 ? 0.5 : index / (monthlyRows.length - 1);
      const x = paddingX + ratio * innerWidth;
      const normalized = isFlatLine ? 0.5 : (row.cost.totalCost - minValue) / span;
      const y = paddingY + (1 - normalized) * innerHeight;
      return {
        x,
        y,
        label: row.item.month,
        value: row.cost.totalCost,
      };
    });

    return {
      width,
      height,
      bottomY,
      minValue,
      maxValue,
      points,
      linePath: buildPolylinePath(points),
      areaPath: buildAreaPath(points, bottomY),
    };
  }, [monthlyRows]);

  const modelRows = useMemo(
    () =>
      (modelsQuery.data?.items ?? []).map((item) => ({
        item,
        cost: resolveUsageCost(item),
      })),
    [modelsQuery.data?.items]
  );

  const modelTotalCost = useMemo(
    () => modelRows.reduce((sum, row) => sum + row.cost.totalCost, 0),
    [modelRows]
  );

  const sessionRows = useMemo(
    () =>
      (sessionBreakdownQuery.data?.items ?? []).map((item) => ({
        item,
        cost: resolveUsageCost(item),
      })),
    [sessionBreakdownQuery.data?.items]
  );

  return (
    <>
      <section className="panel">
        <header>
          <h2>筛选条件</h2>
          <p>当前 limit=50</p>
        </header>

        <div className="filters-row">
          <label className="inline-field" htmlFor="analytics-from">
            开始
            <input
              id="analytics-from"
              type="date"
              value={fromDate}
              onChange={(event) => setFromDate(event.target.value)}
            />
          </label>
          <label className="inline-field" htmlFor="analytics-to">
            结束
            <input
              id="analytics-to"
              type="date"
              value={toDate}
              onChange={(event) => setToDate(event.target.value)}
            />
          </label>
        </div>

        {!rangeValid ? <p className="feedback error">开始日期不能晚于结束日期。</p> : null}
      </section>

      <section className="panel">
        <header>
          <h2>日聚合（usage/daily）</h2>
          <p>共 {dailyQuery.data?.total ?? 0} 条</p>
        </header>
        {dailyQuery.isLoading ? <p className="feedback info">daily 加载中...</p> : null}
        {dailyQuery.isError ? (
          <p className="feedback error">daily 加载失败：{toErrorMessage(dailyQuery.error)}</p>
        ) : null}

        {latestDaily ? (
          <section className="analytics-kpi-grid" aria-label="daily 环比概览">
            <article className="analytics-kpi-card">
              <h3>最新日 Tokens</h3>
              <strong>{latestDaily.item.tokens.toLocaleString("zh-CN")}</strong>
              <span className={`chain-badge ${chainRatioClass(latestDaily.tokensRatio)}`}>
                环比 {formatChainRatio(latestDaily.tokensRatio)}
              </span>
            </article>
            <article className="analytics-kpi-card">
              <h3>最新日总成本</h3>
              <strong>${latestDaily.cost.totalCost.toFixed(4)}</strong>
              <span className={`chain-badge ${chainRatioClass(latestDaily.totalCostRatio)}`}>
                环比 {formatChainRatio(latestDaily.totalCostRatio)}
              </span>
            </article>
            <article className="analytics-kpi-card">
              <h3>最新日 Sessions</h3>
              <strong>{latestDaily.item.sessions.toLocaleString("zh-CN")}</strong>
              <span className={`chain-badge ${chainRatioClass(latestDaily.sessionsRatio)}`}>
                环比 {formatChainRatio(latestDaily.sessionsRatio)}
              </span>
            </article>
          </section>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>日期</th>
                <th>Tokens</th>
                <th>Raw</th>
                <th>Estimated</th>
                <th>总成本</th>
                <th>总成本环比</th>
                <th>Sessions</th>
                <th>口径</th>
              </tr>
            </thead>
            <tbody>
              {dailyRows.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={8}>
                    暂无数据
                  </td>
                </tr>
              ) : (
                dailyRows.map((row) => (
                  <tr key={row.item.date}>
                    <td>{toDateKey(row.item.date)}</td>
                    <td>{row.item.tokens.toLocaleString("zh-CN")}</td>
                    <td>{row.cost.rawCost === null ? "--" : `$${row.cost.rawCost.toFixed(4)}`}</td>
                    <td>
                      {row.cost.estimatedCost === null
                        ? "--"
                        : `$${row.cost.estimatedCost.toFixed(4)}`}
                    </td>
                    <td>${row.cost.totalCost.toFixed(4)}</td>
                    <td>
                      <span className={`chain-badge ${chainRatioClass(row.totalCostRatio)}`}>
                        {formatChainRatio(row.totalCostRatio)}
                      </span>
                    </td>
                    <td>{row.item.sessions.toLocaleString("zh-CN")}</td>
                    <td>{row.cost.label}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>月度聚合（usage/monthly）</h2>
          <p>共 {monthlyQuery.data?.total ?? 0} 条</p>
        </header>
        {monthlyQuery.isLoading ? <p className="feedback info">monthly 加载中...</p> : null}
        {monthlyQuery.isError ? (
          <p className="feedback error">monthly 加载失败：{toErrorMessage(monthlyQuery.error)}</p>
        ) : null}

        {monthlyTrend ? (
          <figure className="trend-chart-shell">
            <figcaption>总成本趋势（monthly）</figcaption>
            <svg
              className="trend-chart"
              role="img"
              aria-label="monthly 总成本趋势图"
              viewBox={`0 0 ${monthlyTrend.width} ${monthlyTrend.height}`}
            >
              <path className="trend-area" d={monthlyTrend.areaPath} />
              <path className="trend-line" d={monthlyTrend.linePath} />
              {monthlyTrend.points.map((point) => (
                <circle
                  key={point.label}
                  className="trend-point"
                  cx={point.x}
                  cy={point.y}
                  r={4}
                >
                  <title>
                    {point.label}: ${point.value.toFixed(4)}
                  </title>
                </circle>
              ))}
            </svg>
            <p className="trend-chart-meta">
              区间最小 ${monthlyTrend.minValue.toFixed(4)} / 最大 ${monthlyTrend.maxValue.toFixed(4)}
            </p>
          </figure>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>月份</th>
                <th>Tokens</th>
                <th>Raw</th>
                <th>Estimated</th>
                <th>总成本</th>
                <th>Sessions</th>
                <th>口径</th>
              </tr>
            </thead>
            <tbody>
              {monthlyRows.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={7}>
                    暂无数据
                  </td>
                </tr>
              ) : (
                monthlyRows.map((row) => (
                  <tr key={row.item.month}>
                    <td>{row.item.month}</td>
                    <td>{row.item.tokens.toLocaleString("zh-CN")}</td>
                    <td>{row.cost.rawCost === null ? "--" : `$${row.cost.rawCost.toFixed(4)}`}</td>
                    <td>
                      {row.cost.estimatedCost === null
                        ? "--"
                        : `$${row.cost.estimatedCost.toFixed(4)}`}
                    </td>
                    <td>${row.cost.totalCost.toFixed(4)}</td>
                    <td>{row.item.sessions.toLocaleString("zh-CN")}</td>
                    <td>{row.cost.label}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>模型排行（usage/models）</h2>
          <p>共 {modelsQuery.data?.total ?? 0} 条</p>
        </header>
        {modelsQuery.isLoading ? <p className="feedback info">models 加载中...</p> : null}
        {modelsQuery.isError ? (
          <p className="feedback error">models 加载失败：{toErrorMessage(modelsQuery.error)}</p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Model</th>
                <th>Tokens</th>
                <th>Raw</th>
                <th>Estimated</th>
                <th>总成本</th>
                <th>成本占比</th>
                <th>Sessions</th>
                <th>口径</th>
              </tr>
            </thead>
            <tbody>
              {modelRows.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={8}>
                    暂无数据
                  </td>
                </tr>
              ) : (
                modelRows.map((row) => (
                  <tr key={row.item.model}>
                    <td>{row.item.model}</td>
                    <td>{row.item.tokens.toLocaleString("zh-CN")}</td>
                    <td>{row.cost.rawCost === null ? "--" : `$${row.cost.rawCost.toFixed(4)}`}</td>
                    <td>
                      {row.cost.estimatedCost === null
                        ? "--"
                        : `$${row.cost.estimatedCost.toFixed(4)}`}
                    </td>
                    <td>${row.cost.totalCost.toFixed(4)}</td>
                    <td>
                      {modelTotalCost > 0
                        ? `${((row.cost.totalCost / modelTotalCost) * 100).toFixed(1)}%`
                        : "0.0%"}
                    </td>
                    <td>{row.item.sessions.toLocaleString("zh-CN")}</td>
                    <td>{row.cost.label}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>会话拆解（usage/sessions）</h2>
          <p>共 {sessionBreakdownQuery.data?.total ?? 0} 条</p>
        </header>
        {sessionBreakdownQuery.isLoading ? <p className="feedback info">sessions 加载中...</p> : null}
        {sessionBreakdownQuery.isError ? (
          <p className="feedback error">
            sessions 加载失败：{toErrorMessage(sessionBreakdownQuery.error)}
          </p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Session</th>
                <th>Source</th>
                <th>工具</th>
                <th>模型</th>
                <th>开始时间</th>
                <th>Input</th>
                <th>Output</th>
                <th>Cache Read</th>
                <th>Cache Write</th>
                <th>Reasoning</th>
                <th>Total Tokens</th>
                <th>总成本</th>
                <th>口径</th>
              </tr>
            </thead>
            <tbody>
              {sessionRows.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={13}>
                    暂无数据
                  </td>
                </tr>
              ) : (
                sessionRows.map((row) => (
                  <tr key={`${row.item.sessionId}:${row.item.startedAt}`}>
                    <td>{row.item.sessionId}</td>
                    <td>{row.item.sourceId}</td>
                    <td>{row.item.tool}</td>
                    <td>{row.item.model}</td>
                    <td>{formatDateTime(row.item.startedAt)}</td>
                    <td>{row.item.inputTokens.toLocaleString("zh-CN")}</td>
                    <td>{row.item.outputTokens.toLocaleString("zh-CN")}</td>
                    <td>{row.item.cacheReadTokens.toLocaleString("zh-CN")}</td>
                    <td>{row.item.cacheWriteTokens.toLocaleString("zh-CN")}</td>
                    <td>{row.item.reasoningTokens.toLocaleString("zh-CN")}</td>
                    <td>{row.item.totalTokens.toLocaleString("zh-CN")}</td>
                    <td>${row.cost.totalCost.toFixed(4)}</td>
                    <td>{row.cost.label}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

function GovernancePage() {
  const queryClient = useQueryClient();
  const [weeklyMetric, setWeeklyMetric] = useState<MetricKey>("tokens");
  const [weeklyTimezone, setWeeklyTimezone] = useState<string>("UTC");
  const [statusFilter, setStatusFilter] = useState<AlertStatus | "">("");
  const [severityFilter, setSeverityFilter] = useState<AlertSeverity | "">("");
  const [alertFeedback, setAlertFeedback] = useState<string | null>(null);

  const [residencyMode, setResidencyMode] = useState<DataResidencyMode>("single_region");
  const [primaryRegion, setPrimaryRegion] = useState("");
  const [replicaRegionsInput, setReplicaRegionsInput] = useState("");
  const [allowCrossRegionTransfer, setAllowCrossRegionTransfer] = useState(false);
  const [requireTransferApproval, setRequireTransferApproval] = useState(false);
  const [replicationStatusFilter, setReplicationStatusFilter] =
    useState<ReplicationJobStatus | "">("");
  const [replicationSourceRegion, setReplicationSourceRegion] = useState("");
  const [replicationTargetRegion, setReplicationTargetRegion] = useState("");
  const [replicationReason, setReplicationReason] = useState("");
  const [residencyFeedback, setResidencyFeedback] = useState<string | null>(null);
  const [residencyError, setResidencyError] = useState<string | null>(null);

  const [ruleStatusFilter, setRuleStatusFilter] = useState<RuleLifecycleStatus | "">("");
  const [ruleKeyword, setRuleKeyword] = useState("");
  const [ruleName, setRuleName] = useState("");
  const [ruleDescription, setRuleDescription] = useState("");
  const [selectedRuleAssetId, setSelectedRuleAssetId] = useState<string | null>(null);
  const [ruleVersionContent, setRuleVersionContent] = useState("");
  const [ruleVersionChangelog, setRuleVersionChangelog] = useState("");
  const [rulePublishVersion, setRulePublishVersion] = useState("");
  const [ruleRollbackVersion, setRuleRollbackVersion] = useState("");
  const [ruleRollbackReason, setRuleRollbackReason] = useState("");
  const [ruleApprovalVersion, setRuleApprovalVersion] = useState("");
  const [ruleApprovalDecision, setRuleApprovalDecision] =
    useState<RuleApprovalDecision>("approved");
  const [ruleApprovalReason, setRuleApprovalReason] = useState("");
  const [ruleFeedback, setRuleFeedback] = useState<string | null>(null);
  const [ruleError, setRuleError] = useState<string | null>(null);

  const [mcpPolicyKeyword, setMcpPolicyKeyword] = useState("");
  const [mcpPolicyToolId, setMcpPolicyToolId] = useState("");
  const [mcpPolicyRiskLevel, setMcpPolicyRiskLevel] = useState<McpRiskLevel>("medium");
  const [mcpPolicyDecision, setMcpPolicyDecision] = useState<McpToolDecision>("require_approval");
  const [mcpPolicyReason, setMcpPolicyReason] = useState("");
  const [mcpApprovalStatusFilter, setMcpApprovalStatusFilter] =
    useState<McpApprovalRequest["status"] | "">("");
  const [mcpApprovalToolId, setMcpApprovalToolId] = useState("");
  const [mcpApprovalReason, setMcpApprovalReason] = useState("");
  const [mcpReviewReason, setMcpReviewReason] = useState("");
  const [mcpInvocationToolId, setMcpInvocationToolId] = useState("");
  const [mcpFeedback, setMcpFeedback] = useState<string | null>(null);
  const [mcpError, setMcpError] = useState<string | null>(null);

  const [sessionExportFormat, setSessionExportFormat] = useState<ExportFormat>("csv");
  const [usageExportFormat, setUsageExportFormat] = useState<ExportFormat>("csv");
  const [usageExportDimension, setUsageExportDimension] =
    useState<UsageExportDimension>("daily");
  const [exportFeedback, setExportFeedback] = useState<string | null>(null);
  const [exportError, setExportError] = useState<string | null>(null);
  const hasInitializedResidencyForm = useRef(false);
  const previousRuleAssetIdRef = useRef<string | null>(null);

  const alertQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(statusFilter ? { status: statusFilter } : {}),
      ...(severityFilter ? { severity: severityFilter } : {}),
    }),
    [severityFilter, statusFilter]
  );

  const replicationJobQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(replicationStatusFilter ? { status: replicationStatusFilter } : {}),
    }),
    [replicationStatusFilter]
  );

  const ruleAssetQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(ruleStatusFilter ? { status: ruleStatusFilter } : {}),
      ...(ruleKeyword.trim().length > 0 ? { keyword: ruleKeyword.trim() } : {}),
    }),
    [ruleKeyword, ruleStatusFilter]
  );

  const mcpPolicyQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(mcpPolicyKeyword.trim().length > 0 ? { keyword: mcpPolicyKeyword.trim() } : {}),
    }),
    [mcpPolicyKeyword]
  );

  const mcpApprovalQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(mcpApprovalStatusFilter ? { status: mcpApprovalStatusFilter } : {}),
    }),
    [mcpApprovalStatusFilter]
  );

  const mcpInvocationQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(mcpInvocationToolId.trim().length > 0 ? { toolId: mcpInvocationToolId.trim() } : {}),
    }),
    [mcpInvocationToolId]
  );

  const alertsQuery = useQuery({
    queryKey: ["alerts", alertQueryInput],
    queryFn: ({ signal }) => fetchAlerts(alertQueryInput, signal),
    staleTime: 20_000,
  });

  const weeklySummaryQuery = useQuery({
    queryKey: ["usage", "weekly-summary", weeklyMetric, weeklyTimezone],
    queryFn: ({ signal }) =>
      fetchUsageWeeklySummary(
        {
          metric: weeklyMetric,
          timezone: weeklyTimezone,
        },
        signal
      ),
    staleTime: 60_000,
  });

  const residencyRegionsQuery = useQuery({
    queryKey: ["residency", "regions"],
    queryFn: ({ signal }) => fetchResidencyRegions(signal),
    staleTime: 60_000,
  });

  const residencyPolicyQuery = useQuery({
    queryKey: ["residency", "policy"],
    queryFn: ({ signal }) => fetchResidencyPolicy(signal),
    staleTime: 20_000,
  });

  const replicationJobsQuery = useQuery({
    queryKey: ["residency", "jobs", replicationJobQueryInput],
    queryFn: ({ signal }) => fetchReplicationJobs(replicationJobQueryInput, signal),
    staleTime: 20_000,
  });

  const ruleAssetsQuery = useQuery({
    queryKey: ["rules", "assets", ruleAssetQueryInput],
    queryFn: ({ signal }) => fetchRuleAssets(ruleAssetQueryInput, signal),
    staleTime: 20_000,
  });

  const ruleVersionsQuery = useQuery({
    queryKey: ["rules", "assets", selectedRuleAssetId, "versions"],
    enabled: Boolean(selectedRuleAssetId),
    queryFn: ({ signal }) => fetchRuleAssetVersions(selectedRuleAssetId!, 50, signal),
    staleTime: 20_000,
  });

  const ruleApprovalsQuery = useQuery({
    queryKey: ["rules", "assets", selectedRuleAssetId, "approvals"],
    enabled: Boolean(selectedRuleAssetId),
    queryFn: ({ signal }) => fetchRuleApprovals(selectedRuleAssetId!, { limit: 50 }, signal),
    staleTime: 20_000,
  });

  const mcpPoliciesQuery = useQuery({
    queryKey: ["mcp", "policies", mcpPolicyQueryInput],
    queryFn: ({ signal }) => fetchMcpPolicies(mcpPolicyQueryInput, signal),
    staleTime: 20_000,
  });

  const mcpApprovalsQuery = useQuery({
    queryKey: ["mcp", "approvals", mcpApprovalQueryInput],
    queryFn: ({ signal }) => fetchMcpApprovals(mcpApprovalQueryInput, signal),
    staleTime: 20_000,
  });

  const mcpInvocationsQuery = useQuery({
    queryKey: ["mcp", "invocations", mcpInvocationQueryInput],
    queryFn: ({ signal }) => fetchMcpInvocations(mcpInvocationQueryInput, signal),
    staleTime: 20_000,
  });

  useEffect(() => {
    if (hasInitializedResidencyForm.current) {
      return;
    }
    if (residencyRegionsQuery.isLoading || residencyPolicyQuery.isLoading) {
      return;
    }
    if (residencyRegionsQuery.isError || residencyPolicyQuery.isError) {
      return;
    }
    const regions = residencyRegionsQuery.data?.items ?? [];
    const policy = residencyPolicyQuery.data;

    if (policy) {
      setResidencyMode(policy.mode);
      setPrimaryRegion(policy.primaryRegion);
      setReplicaRegionsInput(policy.replicaRegions.join(", "));
      setAllowCrossRegionTransfer(policy.allowCrossRegionTransfer);
      setRequireTransferApproval(policy.requireTransferApproval);
      setReplicationSourceRegion(policy.primaryRegion);
      setReplicationTargetRegion(policy.replicaRegions[0] ?? "");
      hasInitializedResidencyForm.current = true;
      return;
    }

    if (regions.length > 0) {
      setPrimaryRegion(regions[0].id);
      setReplicationSourceRegion(regions[0].id);
      setReplicationTargetRegion(regions[1]?.id ?? regions[0].id);
    }
    hasInitializedResidencyForm.current = true;
  }, [
    residencyPolicyQuery.data,
    residencyPolicyQuery.isLoading,
    residencyRegionsQuery.data,
    residencyRegionsQuery.isLoading,
  ]);

  useEffect(() => {
    const assets = ruleAssetsQuery.data?.items ?? [];
    if (assets.length === 0) {
      previousRuleAssetIdRef.current = null;
      setSelectedRuleAssetId(null);
      setRulePublishVersion("");
      setRuleRollbackVersion("");
      setRuleApprovalVersion("");
      return;
    }
    if (!selectedRuleAssetId || !assets.some((asset) => asset.id === selectedRuleAssetId)) {
      setSelectedRuleAssetId(assets[0].id);
    }
  }, [ruleAssetsQuery.data, selectedRuleAssetId]);

  useEffect(() => {
    if (!selectedRuleAssetId) {
      previousRuleAssetIdRef.current = null;
      return;
    }
    const assets = ruleAssetsQuery.data?.items ?? [];
    const selected = assets.find((asset) => asset.id === selectedRuleAssetId);
    if (!selected || selected.latestVersion < 1) {
      return;
    }
    const latestVersionText = String(selected.latestVersion);
    const switchedAsset = previousRuleAssetIdRef.current !== selectedRuleAssetId;
    previousRuleAssetIdRef.current = selectedRuleAssetId;
    if (switchedAsset) {
      setRulePublishVersion(latestVersionText);
      setRuleRollbackVersion(latestVersionText);
      setRuleApprovalVersion(latestVersionText);
      return;
    }
    setRulePublishVersion((prev) => (prev.trim().length > 0 ? prev : latestVersionText));
    setRuleRollbackVersion((prev) => (prev.trim().length > 0 ? prev : latestVersionText));
    setRuleApprovalVersion((prev) => (prev.trim().length > 0 ? prev : latestVersionText));
  }, [ruleAssetsQuery.data, selectedRuleAssetId]);

  const updateAlertStatusMutation = useMutation({
    mutationFn: ({ alertId, status }: { alertId: string; status: AlertMutableStatus }) =>
      updateAlertStatus(alertId, status),
    onSuccess: async (alert) => {
      setAlertFeedback(`告警 ${alert.id} 已更新为 ${alert.status}。`);
      await queryClient.invalidateQueries({ queryKey: ["alerts"] });
    },
  });

  const saveResidencyPolicyMutation = useMutation({
    mutationFn: (
      input: Omit<TenantResidencyPolicy, "tenantId" | "updatedAt"> & {
        updatedAt?: string;
      }
    ) => upsertResidencyPolicy(input),
    onSuccess: async (policy) => {
      setResidencyError(null);
      setResidencyFeedback(`数据主权策略已保存，主地域：${policy.primaryRegion}`);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["residency", "policy"] }),
        queryClient.invalidateQueries({ queryKey: ["residency", "jobs"] }),
      ]);
    },
    onError: (error) => {
      setResidencyFeedback(null);
      setResidencyError(`保存策略失败：${toErrorMessage(error)}`);
    },
  });

  const createReplicationJobMutation = useMutation({
    mutationFn: ({
      sourceRegion,
      targetRegion,
      reason,
    }: {
      sourceRegion: string;
      targetRegion: string;
      reason?: string;
    }) => createReplicationJob({ sourceRegion, targetRegion, reason }),
    onSuccess: async (job) => {
      setResidencyError(null);
      setResidencyFeedback(`复制任务 ${job.id} 已创建（${job.sourceRegion} -> ${job.targetRegion}）。`);
      setReplicationReason("");
      await queryClient.invalidateQueries({ queryKey: ["residency", "jobs"] });
    },
    onError: (error) => {
      setResidencyFeedback(null);
      setResidencyError(`创建复制任务失败：${toErrorMessage(error)}`);
    },
  });

  const cancelReplicationJobMutation = useMutation({
    mutationFn: ({ jobId, reason }: { jobId: string; reason?: string }) =>
      cancelReplicationJob(jobId, reason ? { reason } : undefined),
    onSuccess: async (job) => {
      setResidencyError(null);
      setResidencyFeedback(`复制任务 ${job.id} 已取消。`);
      await queryClient.invalidateQueries({ queryKey: ["residency", "jobs"] });
    },
    onError: (error) => {
      setResidencyFeedback(null);
      setResidencyError(`取消复制任务失败：${toErrorMessage(error)}`);
    },
  });

  const createRuleAssetMutation = useMutation({
    mutationFn: ({ name, description }: { name: string; description?: string }) =>
      createRuleAsset({ name, description }),
    onSuccess: async (asset) => {
      setRuleError(null);
      setRuleFeedback(`规则资产 ${asset.name} 已创建。`);
      setRuleName("");
      setRuleDescription("");
      setSelectedRuleAssetId(asset.id);
      const latestVersionText =
        asset.latestVersion > 0 ? String(asset.latestVersion) : "";
      setRulePublishVersion(latestVersionText);
      setRuleRollbackVersion(latestVersionText);
      setRuleApprovalVersion(latestVersionText);
      await queryClient.invalidateQueries({ queryKey: ["rules", "assets"] });
    },
    onError: (error) => {
      setRuleFeedback(null);
      setRuleError(`创建规则资产失败：${toErrorMessage(error)}`);
    },
  });

  const createRuleAssetVersionMutation = useMutation({
    mutationFn: ({
      assetId,
      content,
      changelog,
    }: {
      assetId: string;
      content: string;
      changelog?: string;
    }) => createRuleAssetVersion(assetId, { content, changelog }),
    onSuccess: async (version) => {
      setRuleError(null);
      setRuleFeedback(`规则版本 v${version.version} 已创建。`);
      setRuleVersionContent("");
      setRuleVersionChangelog("");
      setRulePublishVersion(String(version.version));
      setRuleRollbackVersion(String(version.version));
      setRuleApprovalVersion(String(version.version));
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["rules", "assets"] }),
        queryClient.invalidateQueries({
          queryKey: ["rules", "assets", version.assetId, "versions"],
        }),
      ]);
    },
    onError: (error) => {
      setRuleFeedback(null);
      setRuleError(`创建规则版本失败：${toErrorMessage(error)}`);
    },
  });

  const publishRuleAssetMutation = useMutation({
    mutationFn: ({ assetId, version }: { assetId: string; version: number }) =>
      publishRuleAsset(assetId, { version }),
    onSuccess: async (asset) => {
      setRuleError(null);
      setRuleFeedback(`规则资产 ${asset.name} 已发布 v${asset.publishedVersion ?? "-"}.`);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["rules", "assets"] }),
        queryClient.invalidateQueries({ queryKey: ["rules", "assets", asset.id, "versions"] }),
      ]);
    },
    onError: (error) => {
      setRuleFeedback(null);
      setRuleError(`发布规则版本失败：${toErrorMessage(error)}`);
    },
  });

  const rollbackRuleAssetMutation = useMutation({
    mutationFn: ({
      assetId,
      version,
      reason,
    }: {
      assetId: string;
      version: number;
      reason?: string;
    }) => rollbackRuleAsset(assetId, { version, reason }),
    onSuccess: async (asset) => {
      setRuleError(null);
      setRuleFeedback(`规则资产 ${asset.name} 已回滚到 v${asset.publishedVersion ?? "-"}.`);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["rules", "assets"] }),
        queryClient.invalidateQueries({ queryKey: ["rules", "assets", asset.id, "versions"] }),
      ]);
    },
    onError: (error) => {
      setRuleFeedback(null);
      setRuleError(`回滚规则版本失败：${toErrorMessage(error)}`);
    },
  });

  const createRuleApprovalMutation = useMutation({
    mutationFn: ({
      assetId,
      version,
      decision,
      reason,
    }: {
      assetId: string;
      version: number;
      decision: RuleApprovalDecision;
      reason?: string;
    }) => createRuleApproval(assetId, { version, decision, reason }),
    onSuccess: async (approval) => {
      setRuleError(null);
      setRuleFeedback(`已提交审批：v${approval.version} -> ${approval.decision}。`);
      setRuleApprovalReason("");
      await queryClient.invalidateQueries({
        queryKey: ["rules", "assets", approval.assetId, "approvals"],
      });
    },
    onError: (error) => {
      setRuleFeedback(null);
      setRuleError(`提交审批失败：${toErrorMessage(error)}`);
    },
  });

  const upsertMcpPolicyMutation = useMutation({
    mutationFn: ({
      toolId,
      riskLevel,
      decision,
      reason,
    }: {
      toolId: string;
      riskLevel: McpRiskLevel;
      decision: McpToolDecision;
      reason?: string;
    }) => upsertMcpPolicy(toolId, { riskLevel, decision, reason }),
    onSuccess: async (policy) => {
      setMcpError(null);
      setMcpFeedback(`策略 ${policy.toolId} 已更新为 ${policy.decision}。`);
      await queryClient.invalidateQueries({ queryKey: ["mcp", "policies"] });
    },
    onError: (error) => {
      setMcpFeedback(null);
      setMcpError(`更新策略失败：${toErrorMessage(error)}`);
    },
  });

  const createMcpApprovalMutation = useMutation({
    mutationFn: ({ toolId, reason }: { toolId: string; reason?: string }) =>
      createMcpApproval({ toolId, reason }),
    onSuccess: async (approval) => {
      setMcpError(null);
      setMcpFeedback(`审批请求 ${approval.id} 已创建。`);
      setMcpApprovalToolId("");
      setMcpApprovalReason("");
      await queryClient.invalidateQueries({ queryKey: ["mcp", "approvals"] });
    },
    onError: (error) => {
      setMcpFeedback(null);
      setMcpError(`创建审批请求失败：${toErrorMessage(error)}`);
    },
  });

  const reviewMcpApprovalMutation = useMutation({
    mutationFn: ({
      approvalId,
      status,
      reason,
    }: {
      approvalId: string;
      status: "approved" | "rejected";
      reason?: string;
    }) =>
      status === "approved"
        ? approveMcpApproval(approvalId, reason ? { reason } : undefined)
        : rejectMcpApproval(approvalId, reason ? { reason } : undefined),
    onSuccess: async (approval) => {
      setMcpError(null);
      setMcpFeedback(`审批请求 ${approval.id} 已更新为 ${approval.status}。`);
      await queryClient.invalidateQueries({ queryKey: ["mcp", "approvals"] });
    },
    onError: (error) => {
      setMcpFeedback(null);
      setMcpError(`审批操作失败：${toErrorMessage(error)}`);
    },
  });

  const exportSessionsMutation = useMutation({
    mutationFn: (format: ExportFormat) => exportSessions(format, { limit: 200 }),
    onSuccess: (file) => {
      setExportError(null);
      setExportFeedback(`Sessions 导出成功：${file.filename}`);
      triggerBrowserDownload(file);
    },
    onError: (error) => {
      setExportFeedback(null);
      setExportError(`Sessions 导出失败：${toErrorMessage(error)}`);
    },
  });

  const exportUsageMutation = useMutation({
    mutationFn: (input: { format: ExportFormat; dimension: UsageExportDimension }) =>
      exportUsage(input.format, {
        dimension: input.dimension,
        limit: 200,
      }),
    onSuccess: (file) => {
      setExportError(null);
      setExportFeedback(`Usage 导出成功：${file.filename}`);
      triggerBrowserDownload(file);
    },
    onError: (error) => {
      setExportFeedback(null);
      setExportError(`Usage 导出失败：${toErrorMessage(error)}`);
    },
  });

  const alertItems = alertsQuery.data?.items ?? [];
  const weeklyItems = weeklySummaryQuery.data?.weeks ?? [];
  const weeklyPeak = weeklySummaryQuery.data?.peakWeek;
  const regionItems: RegionDescriptor[] = residencyRegionsQuery.data?.items ?? [];
  const replicationItems: ReplicationJob[] = replicationJobsQuery.data?.items ?? [];
  const ruleItems: RuleAsset[] = ruleAssetsQuery.data?.items ?? [];
  const selectedRuleAsset =
    ruleItems.find((asset) => asset.id === selectedRuleAssetId) ?? null;
  const ruleVersionItems: RuleAssetVersion[] = ruleVersionsQuery.data?.items ?? [];
  const ruleApprovalItems: RuleApproval[] = ruleApprovalsQuery.data?.items ?? [];
  const mcpPolicyItems: McpToolPolicy[] = mcpPoliciesQuery.data?.items ?? [];
  const mcpApprovalItems: McpApprovalRequest[] = mcpApprovalsQuery.data?.items ?? [];
  const mcpInvocationItems: McpInvocationAudit[] = mcpInvocationsQuery.data?.items ?? [];

  return (
    <>
      <section className="panel">
        <header>
          <h2>周报摘要</h2>
          <p>展示最近周级用量统计与峰值周。</p>
        </header>

        <div className="filters-row">
          <label className="inline-field" htmlFor="weekly-summary-metric">
            指标
            <select
              id="weekly-summary-metric"
              value={weeklyMetric}
              onChange={(event) => setWeeklyMetric(event.target.value as MetricKey)}
            >
              {WEEKLY_SUMMARY_METRIC_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="inline-field" htmlFor="weekly-summary-timezone">
            时区
            <select
              id="weekly-summary-timezone"
              value={weeklyTimezone}
              onChange={(event) => setWeeklyTimezone(event.target.value)}
            >
              {WEEKLY_SUMMARY_TIMEZONE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
        </div>

        {weeklySummaryQuery.isLoading ? <p className="feedback info">周报摘要加载中...</p> : null}
        {weeklySummaryQuery.isError ? (
          <p className="feedback error">
            周报摘要加载失败：{toErrorMessage(weeklySummaryQuery.error)}
          </p>
        ) : null}

        {weeklySummaryQuery.data ? (
          <div className="governance-weekly-overview">
            <p>
              统计区间内总计：
              <strong>
                {" "}
                {weeklySummaryQuery.data.summary.tokens.toLocaleString("zh-CN")} tokens / $
                {weeklySummaryQuery.data.summary.cost.toFixed(2)} /{" "}
                {weeklySummaryQuery.data.summary.sessions.toLocaleString("zh-CN")} sessions
              </strong>
            </p>
            <p>
              峰值周：
              <strong>
                {" "}
                {weeklyPeak
                  ? `${weeklyPeak.weekStart} ~ ${weeklyPeak.weekEnd}（${weeklyMetric}: ${
                      weeklyMetric === "cost"
                        ? `$${weeklyPeak.cost.toFixed(2)}`
                        : weeklyMetric === "sessions"
                          ? weeklyPeak.sessions.toLocaleString("zh-CN")
                          : weeklyPeak.tokens.toLocaleString("zh-CN")
                    }）`
                  : "暂无峰值"}
              </strong>
            </p>
          </div>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Week Start</th>
                <th>Week End</th>
                <th>Tokens</th>
                <th>Cost</th>
                <th>Sessions</th>
              </tr>
            </thead>
            <tbody>
              {weeklyItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={5}>
                    暂无周报数据
                  </td>
                </tr>
              ) : (
                weeklyItems.map((item) => (
                  <tr key={`${item.weekStart}:${item.weekEnd}`}>
                    <td>{item.weekStart}</td>
                    <td>{item.weekEnd}</td>
                    <td>{item.tokens.toLocaleString("zh-CN")}</td>
                    <td>${item.cost.toFixed(2)}</td>
                    <td>{item.sessions.toLocaleString("zh-CN")}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>告警工作台</h2>
          <p>共 {alertsQuery.data?.total ?? alertItems.length} 条</p>
        </header>

        <div className="filters-row">
          <label className="inline-field" htmlFor="alerts-severity-filter">
            级别
            <select
              id="alerts-severity-filter"
              value={severityFilter}
              onChange={(event) => {
                setSeverityFilter(event.target.value as AlertSeverity | "");
                setAlertFeedback(null);
              }}
            >
              {ALERT_SEVERITY_FILTER_OPTIONS.map((option) => (
                <option key={option.value || "all"} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="inline-field" htmlFor="alerts-status-filter">
            状态
            <select
              id="alerts-status-filter"
              value={statusFilter}
              onChange={(event) => {
                setStatusFilter(event.target.value as AlertStatus | "");
                setAlertFeedback(null);
              }}
            >
              {ALERT_STATUS_FILTER_OPTIONS.map((option) => (
                <option key={option.value || "all"} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
        </div>

        {alertsQuery.isLoading ? <p className="feedback info">告警加载中...</p> : null}
        {alertsQuery.isError ? (
          <p className="feedback error">告警加载失败：{toErrorMessage(alertsQuery.error)}</p>
        ) : null}
        {alertFeedback ? <p className="feedback success">{alertFeedback}</p> : null}
        {updateAlertStatusMutation.isError ? (
          <p className="feedback error">
            告警状态更新失败：{toErrorMessage(updateAlertStatusMutation.error)}
          </p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>级别</th>
                <th>状态</th>
                <th>消息</th>
                <th>更新时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {alertItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={6}>
                    暂无告警
                  </td>
                </tr>
              ) : (
                alertItems.map((alert: AlertItem) => {
                  const isUpdating =
                    updateAlertStatusMutation.isPending &&
                    updateAlertStatusMutation.variables?.alertId === alert.id;

                  return (
                    <tr key={alert.id}>
                      <td>{alert.id}</td>
                      <td>{alert.severity}</td>
                      <td>{alert.status}</td>
                      <td>{alert.message}</td>
                      <td>{formatDateTime(alert.updatedAt)}</td>
                      <td>
                        <div className="governance-action-row">
                          {alert.status === "open" ? (
                            <button
                              type="button"
                              className="table-action"
                              disabled={isUpdating}
                              onClick={() =>
                                updateAlertStatusMutation.mutate({
                                  alertId: alert.id,
                                  status: "acknowledged",
                                })
                              }
                            >
                              ACK
                            </button>
                          ) : null}
                          {alert.status !== "resolved" ? (
                            <button
                              type="button"
                              className="table-action"
                              disabled={isUpdating}
                              onClick={() =>
                                updateAlertStatusMutation.mutate({
                                  alertId: alert.id,
                                  status: "resolved",
                                })
                              }
                            >
                              Resolve
                            </button>
                          ) : (
                            <span className="tiny-feedback tiny-feedback-success">已完成</span>
                          )}
                        </div>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>数据主权与复制</h2>
          <p>主权策略 + 跨地域复制任务。</p>
        </header>

        <div className="filters-row">
          <label className="inline-field" htmlFor="residency-mode">
            模式
            <select
              id="residency-mode"
              value={residencyMode}
              onChange={(event) => setResidencyMode(event.target.value as DataResidencyMode)}
            >
              {DATA_RESIDENCY_MODE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="inline-field" htmlFor="residency-primary-region">
            主地域
            <select
              id="residency-primary-region"
              value={primaryRegion}
              onChange={(event) => setPrimaryRegion(event.target.value)}
            >
              <option value="">请选择</option>
              {regionItems.map((region) => (
                <option key={region.id} value={region.id}>
                  {region.id}
                </option>
              ))}
            </select>
          </label>
        </div>

        <div className="filters-row">
          <label className="inline-field governance-wide-field" htmlFor="residency-replica-regions">
            副本地域（逗号分隔）
            <input
              id="residency-replica-regions"
              type="text"
              value={replicaRegionsInput}
              onChange={(event) => setReplicaRegionsInput(event.target.value)}
              placeholder="例如：cn-shanghai, ap-southeast-1"
            />
          </label>
        </div>

        <div className="filters-row">
          <label className="checkbox-field" htmlFor="residency-cross-transfer">
            <input
              id="residency-cross-transfer"
              type="checkbox"
              checked={allowCrossRegionTransfer}
              onChange={(event) => setAllowCrossRegionTransfer(event.target.checked)}
            />
            允许跨地域传输
          </label>

          <label className="checkbox-field" htmlFor="residency-transfer-approval">
            <input
              id="residency-transfer-approval"
              type="checkbox"
              checked={requireTransferApproval}
              onChange={(event) => setRequireTransferApproval(event.target.checked)}
            />
            传输必须审批
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={saveResidencyPolicyMutation.isPending}
            onClick={() => {
              const normalizedPrimaryRegion = primaryRegion.trim();
              if (!normalizedPrimaryRegion) {
                setResidencyFeedback(null);
                setResidencyError("主地域不能为空。");
                return;
              }
              const replicaRegions = replicaRegionsInput
                .split(",")
                .map((region) => region.trim())
                .filter((region, index, list) => region.length > 0 && list.indexOf(region) === index)
                .filter((region) => region !== normalizedPrimaryRegion);
              if (residencyMode === "active_active" && replicaRegions.length === 0) {
                setResidencyFeedback(null);
                setResidencyError("active_active 模式至少需要一个副本地域。");
                return;
              }
              if (residencyMode === "single_region" && replicaRegions.length > 0) {
                setResidencyFeedback(null);
                setResidencyError("single_region 模式不允许配置副本地域。");
                return;
              }
              setResidencyFeedback(null);
              setResidencyError(null);
              saveResidencyPolicyMutation.mutate({
                mode: residencyMode,
                primaryRegion: normalizedPrimaryRegion,
                replicaRegions,
                allowCrossRegionTransfer,
                requireTransferApproval,
              });
            }}
          >
            {saveResidencyPolicyMutation.isPending ? "保存中..." : "保存策略"}
          </button>
        </div>

        {residencyRegionsQuery.isLoading || residencyPolicyQuery.isLoading ? (
          <p className="feedback info">数据主权配置加载中...</p>
        ) : null}
        {residencyRegionsQuery.isError ? (
          <p className="feedback error">地域列表加载失败：{toErrorMessage(residencyRegionsQuery.error)}</p>
        ) : null}
        {residencyPolicyQuery.isError ? (
          <p className="feedback error">主权策略加载失败：{toErrorMessage(residencyPolicyQuery.error)}</p>
        ) : null}
        {residencyFeedback ? <p className="feedback success">{residencyFeedback}</p> : null}
        {residencyError ? <p className="feedback error">{residencyError}</p> : null}

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="replication-source-region">
            复制源地域
            <select
              id="replication-source-region"
              value={replicationSourceRegion}
              onChange={(event) => setReplicationSourceRegion(event.target.value)}
            >
              <option value="">请选择</option>
              {regionItems.map((region) => (
                <option key={`src-${region.id}`} value={region.id}>
                  {region.id}
                </option>
              ))}
            </select>
          </label>

          <label className="inline-field" htmlFor="replication-target-region">
            复制目标地域
            <select
              id="replication-target-region"
              value={replicationTargetRegion}
              onChange={(event) => setReplicationTargetRegion(event.target.value)}
            >
              <option value="">请选择</option>
              {regionItems.map((region) => (
                <option key={`target-${region.id}`} value={region.id}>
                  {region.id}
                </option>
              ))}
            </select>
          </label>

          <label className="inline-field" htmlFor="replication-reason">
            原因
            <input
              id="replication-reason"
              type="text"
              value={replicationReason}
              onChange={(event) => setReplicationReason(event.target.value)}
              placeholder="可选"
            />
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={createReplicationJobMutation.isPending}
            onClick={() => {
              const sourceRegion = replicationSourceRegion.trim();
              const targetRegion = replicationTargetRegion.trim();
              if (!sourceRegion || !targetRegion) {
                setResidencyFeedback(null);
                setResidencyError("复制任务的源地域与目标地域不能为空。");
                return;
              }
              if (sourceRegion === targetRegion) {
                setResidencyFeedback(null);
                setResidencyError("源地域和目标地域不能相同。");
                return;
              }
              setResidencyFeedback(null);
              setResidencyError(null);
              createReplicationJobMutation.mutate({
                sourceRegion,
                targetRegion,
                reason: replicationReason.trim() || undefined,
              });
            }}
          >
            {createReplicationJobMutation.isPending ? "创建中..." : "创建复制任务"}
          </button>
        </div>

        <div className="filters-row">
          <label className="inline-field" htmlFor="replication-status-filter">
            任务状态
            <select
              id="replication-status-filter"
              value={replicationStatusFilter}
              onChange={(event) =>
                setReplicationStatusFilter(event.target.value as ReplicationJobStatus | "")
              }
            >
              {REPLICATION_STATUS_FILTER_OPTIONS.map((option) => (
                <option key={option.value || "all"} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
        </div>

        {replicationJobsQuery.isLoading ? <p className="feedback info">复制任务加载中...</p> : null}
        {replicationJobsQuery.isError ? (
          <p className="feedback error">
            复制任务加载失败：{toErrorMessage(replicationJobsQuery.error)}
          </p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>源地域</th>
                <th>目标地域</th>
                <th>状态</th>
                <th>创建时间</th>
                <th>原因</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {replicationItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={7}>
                    暂无复制任务
                  </td>
                </tr>
              ) : (
                replicationItems.map((job) => {
                  const canCancel = job.status === "pending" || job.status === "running";
                  const isCancelling =
                    cancelReplicationJobMutation.isPending &&
                    cancelReplicationJobMutation.variables?.jobId === job.id;
                  return (
                    <tr key={job.id}>
                      <td>{job.id}</td>
                      <td>{job.sourceRegion}</td>
                      <td>{job.targetRegion}</td>
                      <td>{job.status}</td>
                      <td>{formatDateTime(job.createdAt)}</td>
                      <td>{job.reason ?? "--"}</td>
                      <td>
                        {canCancel ? (
                          <button
                            type="button"
                            className="table-action"
                            disabled={isCancelling}
                            onClick={() => {
                              const reason =
                                typeof window !== "undefined"
                                  ? window.prompt("取消原因（可选）", "") ?? ""
                                  : "";
                              cancelReplicationJobMutation.mutate({
                                jobId: job.id,
                                reason: reason.trim() || undefined,
                              });
                            }}
                          >
                            {isCancelling ? "取消中..." : "取消"}
                          </button>
                        ) : (
                          <span className="tiny-feedback tiny-feedback-success">不可取消</span>
                        )}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>Rule Hub 规则资产</h2>
          <p>规则资产、版本发布与审批闭环。</p>
        </header>

        <div className="filters-row">
          <label className="inline-field" htmlFor="rule-status-filter">
            规则状态
            <select
              id="rule-status-filter"
              value={ruleStatusFilter}
              onChange={(event) => setRuleStatusFilter(event.target.value as RuleLifecycleStatus | "")}
            >
              {RULE_STATUS_FILTER_OPTIONS.map((option) => (
                <option key={option.value || "all"} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="inline-field governance-wide-field" htmlFor="rule-keyword">
            关键字
            <input
              id="rule-keyword"
              type="text"
              value={ruleKeyword}
              onChange={(event) => setRuleKeyword(event.target.value)}
              placeholder="按名称或描述检索"
            />
          </label>
        </div>

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="rule-name">
            资产名称
            <input
              id="rule-name"
              type="text"
              value={ruleName}
              onChange={(event) => setRuleName(event.target.value)}
              placeholder="例如：Prompt 审计规则"
            />
          </label>

          <label className="inline-field governance-wide-field" htmlFor="rule-description">
            说明
            <input
              id="rule-description"
              type="text"
              value={ruleDescription}
              onChange={(event) => setRuleDescription(event.target.value)}
              placeholder="可选"
            />
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={createRuleAssetMutation.isPending}
            onClick={() => {
              const name = ruleName.trim();
              if (!name) {
                setRuleFeedback(null);
                setRuleError("资产名称不能为空。");
                return;
              }
              setRuleFeedback(null);
              setRuleError(null);
              createRuleAssetMutation.mutate({
                name,
                description: ruleDescription.trim() || undefined,
              });
            }}
          >
            {createRuleAssetMutation.isPending ? "创建中..." : "创建规则资产"}
          </button>
        </div>

        {ruleAssetsQuery.isLoading ? <p className="feedback info">规则资产加载中...</p> : null}
        {ruleAssetsQuery.isError ? (
          <p className="feedback error">规则资产加载失败：{toErrorMessage(ruleAssetsQuery.error)}</p>
        ) : null}
        {ruleFeedback ? <p className="feedback success">{ruleFeedback}</p> : null}
        {ruleError ? <p className="feedback error">{ruleError}</p> : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>名称</th>
                <th>状态</th>
                <th>最新版本</th>
                <th>发布版本</th>
                <th>更新时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {ruleItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={7}>
                    暂无规则资产
                  </td>
                </tr>
              ) : (
                ruleItems.map((asset) => {
                  const isSelected = selectedRuleAssetId === asset.id;
                  return (
                    <tr key={asset.id} className={isSelected ? "is-selected-row" : ""}>
                      <td>{asset.id}</td>
                      <td>{asset.name}</td>
                      <td>{asset.status}</td>
                      <td>{asset.latestVersion}</td>
                      <td>{asset.publishedVersion ?? "--"}</td>
                      <td>{formatDateTime(asset.updatedAt)}</td>
                      <td>
                        <button
                          type="button"
                          className="table-action"
                          onClick={() => {
                            setSelectedRuleAssetId(asset.id);
                            if (asset.latestVersion > 0) {
                              const latestVersionText = String(asset.latestVersion);
                              setRulePublishVersion(latestVersionText);
                              setRuleRollbackVersion(latestVersionText);
                              setRuleApprovalVersion(latestVersionText);
                            }
                          }}
                        >
                          {isSelected ? "已选中" : "选中"}
                        </button>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        {selectedRuleAsset ? (
          <>
            <div className="filters-row governance-inline-grid">
              <label className="inline-field governance-wide-field" htmlFor="rule-version-content">
                新版本内容
                <input
                  id="rule-version-content"
                  type="text"
                  value={ruleVersionContent}
                  onChange={(event) => setRuleVersionContent(event.target.value)}
                  placeholder="例如：deny tool=github.delete_repo when risk=high"
                />
              </label>

              <label className="inline-field governance-wide-field" htmlFor="rule-version-changelog">
                变更说明
                <input
                  id="rule-version-changelog"
                  type="text"
                  value={ruleVersionChangelog}
                  onChange={(event) => setRuleVersionChangelog(event.target.value)}
                  placeholder="可选"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={createRuleAssetVersionMutation.isPending}
                onClick={() => {
                  const content = ruleVersionContent.trim();
                  if (!content) {
                    setRuleFeedback(null);
                    setRuleError("版本内容不能为空。");
                    return;
                  }
                  setRuleFeedback(null);
                  setRuleError(null);
                  createRuleAssetVersionMutation.mutate({
                    assetId: selectedRuleAsset.id,
                    content,
                    changelog: ruleVersionChangelog.trim() || undefined,
                  });
                }}
              >
                {createRuleAssetVersionMutation.isPending ? "创建中..." : "创建版本"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label className="inline-field" htmlFor="rule-publish-version">
                发布版本
                <input
                  id="rule-publish-version"
                  type="number"
                  min={1}
                  step={1}
                  value={rulePublishVersion}
                  onChange={(event) => setRulePublishVersion(event.target.value)}
                  placeholder="例如：1"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={publishRuleAssetMutation.isPending}
                onClick={() => {
                  const version = Number(rulePublishVersion);
                  if (!Number.isInteger(version) || version < 1) {
                    setRuleFeedback(null);
                    setRuleError("发布版本必须是正整数。");
                    return;
                  }
                  setRuleFeedback(null);
                  setRuleError(null);
                  publishRuleAssetMutation.mutate({ assetId: selectedRuleAsset.id, version });
                }}
              >
                {publishRuleAssetMutation.isPending ? "发布中..." : "发布版本"}
              </button>

              <label className="inline-field" htmlFor="rule-rollback-version">
                回滚版本
                <input
                  id="rule-rollback-version"
                  type="number"
                  min={1}
                  step={1}
                  value={ruleRollbackVersion}
                  onChange={(event) => setRuleRollbackVersion(event.target.value)}
                  placeholder="例如：1"
                />
              </label>

              <label className="inline-field governance-wide-field" htmlFor="rule-rollback-reason">
                回滚原因
                <input
                  id="rule-rollback-reason"
                  type="text"
                  value={ruleRollbackReason}
                  onChange={(event) => setRuleRollbackReason(event.target.value)}
                  placeholder="可选"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={rollbackRuleAssetMutation.isPending}
                onClick={() => {
                  const version = Number(ruleRollbackVersion);
                  if (!Number.isInteger(version) || version < 1) {
                    setRuleFeedback(null);
                    setRuleError("回滚版本必须是正整数。");
                    return;
                  }
                  setRuleFeedback(null);
                  setRuleError(null);
                  rollbackRuleAssetMutation.mutate({
                    assetId: selectedRuleAsset.id,
                    version,
                    reason: ruleRollbackReason.trim() || undefined,
                  });
                }}
              >
                {rollbackRuleAssetMutation.isPending ? "回滚中..." : "执行回滚"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label className="inline-field" htmlFor="rule-approval-version">
                审批版本
                <input
                  id="rule-approval-version"
                  type="number"
                  min={1}
                  step={1}
                  value={ruleApprovalVersion}
                  onChange={(event) => setRuleApprovalVersion(event.target.value)}
                  placeholder="例如：1"
                />
              </label>

              <label className="inline-field" htmlFor="rule-approval-decision">
                审批决策
                <select
                  id="rule-approval-decision"
                  value={ruleApprovalDecision}
                  onChange={(event) =>
                    setRuleApprovalDecision(event.target.value as RuleApprovalDecision)
                  }
                >
                  {RULE_APPROVAL_DECISION_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label className="inline-field governance-wide-field" htmlFor="rule-approval-reason">
                审批意见
                <input
                  id="rule-approval-reason"
                  type="text"
                  value={ruleApprovalReason}
                  onChange={(event) => setRuleApprovalReason(event.target.value)}
                  placeholder="可选"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={createRuleApprovalMutation.isPending}
                onClick={() => {
                  const version = Number(ruleApprovalVersion);
                  if (!Number.isInteger(version) || version < 1) {
                    setRuleFeedback(null);
                    setRuleError("审批版本必须是正整数。");
                    return;
                  }
                  setRuleFeedback(null);
                  setRuleError(null);
                  createRuleApprovalMutation.mutate({
                    assetId: selectedRuleAsset.id,
                    version,
                    decision: ruleApprovalDecision,
                    reason: ruleApprovalReason.trim() || undefined,
                  });
                }}
              >
                {createRuleApprovalMutation.isPending ? "提交中..." : "提交审批"}
              </button>
            </div>

            {ruleVersionsQuery.isLoading ? <p className="feedback info">版本列表加载中...</p> : null}
            {ruleVersionsQuery.isError ? (
              <p className="feedback error">版本列表加载失败：{toErrorMessage(ruleVersionsQuery.error)}</p>
            ) : null}
            {ruleApprovalsQuery.isLoading ? <p className="feedback info">审批记录加载中...</p> : null}
            {ruleApprovalsQuery.isError ? (
              <p className="feedback error">审批记录加载失败：{toErrorMessage(ruleApprovalsQuery.error)}</p>
            ) : null}

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>Version</th>
                    <th>Content</th>
                    <th>Changelog</th>
                    <th>Created At</th>
                  </tr>
                </thead>
                <tbody>
                  {ruleVersionItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={4}>
                        暂无版本记录
                      </td>
                    </tr>
                  ) : (
                    ruleVersionItems.map((item) => (
                      <tr key={item.id}>
                        <td>{item.version}</td>
                        <td>{item.content}</td>
                        <td>{item.changelog ?? "--"}</td>
                        <td>{formatDateTime(item.createdAt)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Version</th>
                    <th>Decision</th>
                    <th>Approver</th>
                    <th>Reason</th>
                    <th>Created At</th>
                  </tr>
                </thead>
                <tbody>
                  {ruleApprovalItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={6}>
                        暂无审批记录
                      </td>
                    </tr>
                  ) : (
                    ruleApprovalItems.map((item) => (
                      <tr key={item.id}>
                        <td>{item.id}</td>
                        <td>{item.version}</td>
                        <td>{item.decision}</td>
                        <td>{item.approverEmail ?? item.approverUserId}</td>
                        <td>{item.reason ?? "--"}</td>
                        <td>{formatDateTime(item.createdAt)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </>
        ) : (
          <p className="feedback empty">请选择一个规则资产查看版本与审批详情。</p>
        )}
      </section>

      <section className="panel">
        <header>
          <h2>MCP 治理</h2>
          <p>工具策略、审批请求和调用审计。</p>
        </header>

        <div className="filters-row">
          <label className="inline-field governance-wide-field" htmlFor="mcp-policy-keyword">
            策略检索
            <input
              id="mcp-policy-keyword"
              type="text"
              value={mcpPolicyKeyword}
              onChange={(event) => setMcpPolicyKeyword(event.target.value)}
              placeholder="按 toolId 过滤"
            />
          </label>
        </div>

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="mcp-policy-tool-id">
            Tool ID
            <input
              id="mcp-policy-tool-id"
              type="text"
              value={mcpPolicyToolId}
              onChange={(event) => setMcpPolicyToolId(event.target.value)}
              placeholder="例如：github.delete_repo"
            />
          </label>

          <label className="inline-field" htmlFor="mcp-policy-risk-level">
            风险等级
            <select
              id="mcp-policy-risk-level"
              value={mcpPolicyRiskLevel}
              onChange={(event) => setMcpPolicyRiskLevel(event.target.value as McpRiskLevel)}
            >
              {MCP_RISK_LEVEL_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="inline-field" htmlFor="mcp-policy-decision">
            策略决策
            <select
              id="mcp-policy-decision"
              value={mcpPolicyDecision}
              onChange={(event) => setMcpPolicyDecision(event.target.value as McpToolDecision)}
            >
              {MCP_DECISION_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="inline-field governance-wide-field" htmlFor="mcp-policy-reason">
            策略说明
            <input
              id="mcp-policy-reason"
              type="text"
              value={mcpPolicyReason}
              onChange={(event) => setMcpPolicyReason(event.target.value)}
              placeholder="可选"
            />
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={upsertMcpPolicyMutation.isPending}
            onClick={() => {
              const toolId = mcpPolicyToolId.trim();
              if (!toolId) {
                setMcpFeedback(null);
                setMcpError("Tool ID 不能为空。");
                return;
              }
              setMcpFeedback(null);
              setMcpError(null);
              upsertMcpPolicyMutation.mutate({
                toolId,
                riskLevel: mcpPolicyRiskLevel,
                decision: mcpPolicyDecision,
                reason: mcpPolicyReason.trim() || undefined,
              });
            }}
          >
            {upsertMcpPolicyMutation.isPending ? "保存中..." : "保存策略"}
          </button>
        </div>

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Tool ID</th>
                <th>Risk</th>
                <th>Decision</th>
                <th>Reason</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {mcpPolicyItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={5}>
                    暂无 MCP 策略
                  </td>
                </tr>
              ) : (
                mcpPolicyItems.map((policy) => (
                  <tr key={policy.toolId}>
                    <td>{policy.toolId}</td>
                    <td>{policy.riskLevel}</td>
                    <td>{policy.decision}</td>
                    <td>{policy.reason ?? "--"}</td>
                    <td>{formatDateTime(policy.updatedAt)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="mcp-approval-status-filter">
            审批状态
            <select
              id="mcp-approval-status-filter"
              value={mcpApprovalStatusFilter}
              onChange={(event) =>
                setMcpApprovalStatusFilter(event.target.value as McpApprovalRequest["status"] | "")
              }
            >
              {MCP_APPROVAL_STATUS_FILTER_OPTIONS.map((option) => (
                <option key={option.value || "all"} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="inline-field" htmlFor="mcp-approval-tool-id">
            新建审批 Tool ID
            <input
              id="mcp-approval-tool-id"
              type="text"
              value={mcpApprovalToolId}
              onChange={(event) => setMcpApprovalToolId(event.target.value)}
              placeholder="例如：github.delete_repo"
            />
          </label>

          <label className="inline-field governance-wide-field" htmlFor="mcp-approval-reason">
            申请原因
            <input
              id="mcp-approval-reason"
              type="text"
              value={mcpApprovalReason}
              onChange={(event) => setMcpApprovalReason(event.target.value)}
              placeholder="可选"
            />
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={createMcpApprovalMutation.isPending}
            onClick={() => {
              const toolId = mcpApprovalToolId.trim();
              if (!toolId) {
                setMcpFeedback(null);
                setMcpError("审批请求的 Tool ID 不能为空。");
                return;
              }
              setMcpFeedback(null);
              setMcpError(null);
              createMcpApprovalMutation.mutate({
                toolId,
                reason: mcpApprovalReason.trim() || undefined,
              });
            }}
          >
            {createMcpApprovalMutation.isPending ? "提交中..." : "提交审批请求"}
          </button>
        </div>

        <div className="filters-row">
          <label className="inline-field governance-wide-field" htmlFor="mcp-review-reason">
            审批操作说明（通过/拒绝时可选）
            <input
              id="mcp-review-reason"
              type="text"
              value={mcpReviewReason}
              onChange={(event) => setMcpReviewReason(event.target.value)}
              placeholder="可选"
            />
          </label>
        </div>

        {mcpPoliciesQuery.isLoading || mcpApprovalsQuery.isLoading || mcpInvocationsQuery.isLoading ? (
          <p className="feedback info">MCP 数据加载中...</p>
        ) : null}
        {mcpPoliciesQuery.isError ? (
          <p className="feedback error">MCP 策略加载失败：{toErrorMessage(mcpPoliciesQuery.error)}</p>
        ) : null}
        {mcpApprovalsQuery.isError ? (
          <p className="feedback error">审批列表加载失败：{toErrorMessage(mcpApprovalsQuery.error)}</p>
        ) : null}
        {mcpInvocationsQuery.isError ? (
          <p className="feedback error">调用审计加载失败：{toErrorMessage(mcpInvocationsQuery.error)}</p>
        ) : null}
        {mcpFeedback ? <p className="feedback success">{mcpFeedback}</p> : null}
        {mcpError ? <p className="feedback error">{mcpError}</p> : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>审批 ID</th>
                <th>Tool ID</th>
                <th>状态</th>
                <th>申请人</th>
                <th>创建时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {mcpApprovalItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={6}>
                    暂无审批请求
                  </td>
                </tr>
              ) : (
                mcpApprovalItems.map((approval) => {
                  const isMutating =
                    reviewMcpApprovalMutation.isPending &&
                    reviewMcpApprovalMutation.variables?.approvalId === approval.id;
                  return (
                    <tr key={approval.id}>
                      <td>{approval.id}</td>
                      <td>{approval.toolId}</td>
                      <td>{approval.status}</td>
                      <td>{approval.requestedByEmail ?? approval.requestedByUserId}</td>
                      <td>{formatDateTime(approval.createdAt)}</td>
                      <td>
                        {approval.status === "pending" ? (
                          <div className="governance-action-row">
                            <button
                              type="button"
                              className="table-action"
                              disabled={isMutating}
                              onClick={() =>
                                reviewMcpApprovalMutation.mutate({
                                  approvalId: approval.id,
                                  status: "approved",
                                  reason: mcpReviewReason.trim() || undefined,
                                })
                              }
                            >
                              通过
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              disabled={isMutating}
                              onClick={() =>
                                reviewMcpApprovalMutation.mutate({
                                  approvalId: approval.id,
                                  status: "rejected",
                                  reason: mcpReviewReason.trim() || undefined,
                                })
                              }
                            >
                              拒绝
                            </button>
                          </div>
                        ) : (
                          <span className="tiny-feedback tiny-feedback-success">已处理</span>
                        )}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        <div className="filters-row">
          <label className="inline-field governance-wide-field" htmlFor="mcp-invocation-tool-id">
            调用审计 Tool ID
            <input
              id="mcp-invocation-tool-id"
              type="text"
              value={mcpInvocationToolId}
              onChange={(event) => setMcpInvocationToolId(event.target.value)}
              placeholder="留空查看全部"
            />
          </label>
        </div>

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Tool ID</th>
                <th>Decision</th>
                <th>Result</th>
                <th>审批请求</th>
                <th>时间</th>
              </tr>
            </thead>
            <tbody>
              {mcpInvocationItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={6}>
                    暂无调用审计
                  </td>
                </tr>
              ) : (
                mcpInvocationItems.map((item) => (
                  <tr key={item.id}>
                    <td>{item.id}</td>
                    <td>{item.toolId}</td>
                    <td>{item.decision}</td>
                    <td>{item.result}</td>
                    <td>{item.approvalRequestId ?? "--"}</td>
                    <td>{formatDateTime(item.createdAt)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>导出中心</h2>
          <p>支持 sessions/usage 一键下载 JSON 或 CSV。</p>
        </header>

        <div className="governance-export-grid">
          <article className="governance-export-card">
            <h3>Sessions 导出</h3>
            <label className="inline-field" htmlFor="sessions-export-format">
              格式
              <select
                id="sessions-export-format"
                value={sessionExportFormat}
                onChange={(event) => setSessionExportFormat(event.target.value as ExportFormat)}
              >
                {EXPORT_FORMAT_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <button
              type="button"
              className="submit-button"
              onClick={() => {
                setExportFeedback(null);
                setExportError(null);
                exportSessionsMutation.mutate(sessionExportFormat);
              }}
              disabled={exportSessionsMutation.isPending}
            >
              {exportSessionsMutation.isPending ? "导出中..." : "导出 Sessions"}
            </button>
          </article>

          <article className="governance-export-card">
            <h3>Usage 导出</h3>
            <div className="filters-row">
              <label className="inline-field" htmlFor="usage-export-dimension">
                维度
                <select
                  id="usage-export-dimension"
                  value={usageExportDimension}
                  onChange={(event) =>
                    setUsageExportDimension(event.target.value as UsageExportDimension)
                  }
                >
                  {USAGE_EXPORT_DIMENSION_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label className="inline-field" htmlFor="usage-export-format">
                格式
                <select
                  id="usage-export-format"
                  value={usageExportFormat}
                  onChange={(event) => setUsageExportFormat(event.target.value as ExportFormat)}
                >
                  {EXPORT_FORMAT_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <button
              type="button"
              className="submit-button"
              onClick={() => {
                setExportFeedback(null);
                setExportError(null);
                exportUsageMutation.mutate({
                  format: usageExportFormat,
                  dimension: usageExportDimension,
                });
              }}
              disabled={exportUsageMutation.isPending}
            >
              {exportUsageMutation.isPending ? "导出中..." : "导出 Usage"}
            </button>
          </article>
        </div>

        {exportFeedback ? <p className="feedback success">{exportFeedback}</p> : null}
        {exportError ? <p className="feedback error">{exportError}</p> : null}
      </section>
    </>
  );
}

function SourcesPage() {
  const [sourceForm, setSourceForm] = useState<SourceFormState>(INITIAL_SOURCE_FORM);
  const [sourceFormError, setSourceFormError] = useState<string | null>(null);
  const [selectedSourceId, setSelectedSourceId] = useState<string | null>(null);
  const [connectionResults, setConnectionResults] = useState<
    Record<string, { success: boolean; message: string }>
  >({});
  const queryClient = useQueryClient();

  const sourcesQuery = useQuery({
    queryKey: ["sources"],
    queryFn: ({ signal }) => fetchSources(signal),
    staleTime: 20_000,
  });

  const createSourceMutation = useMutation({
    mutationFn: (input: CreateSourceInput) => createSource(input),
    onSuccess: async () => {
      setSourceForm(INITIAL_SOURCE_FORM);
      setSourceFormError(null);
      await queryClient.invalidateQueries({ queryKey: ["sources"] });
    },
  });

  const testConnectionMutation = useMutation({
    mutationFn: (sourceId: string) => testSourceConnection(sourceId),
    onSuccess: (result: SourceConnectionTestResponse) => {
      setConnectionResults((prev) => ({
        ...prev,
        [result.sourceId]: {
          success: result.success,
          message: `${result.success ? "成功" : "失败"} (${result.latencyMs}ms)：${result.detail}`,
        },
      }));
    },
    onError: (error, sourceId) => {
      setConnectionResults((prev) => ({
        ...prev,
        [sourceId]: {
          success: false,
          message: toErrorMessage(error),
        },
      }));
    },
  });

  const sourceItems = sourcesQuery.data?.items ?? [];
  const selectedSource = sourceItems.find((item) => item.id === selectedSourceId) ?? null;

  useEffect(() => {
    if (sourceItems.length === 0) {
      setSelectedSourceId(null);
      return;
    }
    if (!selectedSourceId || !sourceItems.some((item) => item.id === selectedSourceId)) {
      setSelectedSourceId(sourceItems[0].id);
    }
  }, [selectedSourceId, sourceItems]);

  const sourceHealthQuery = useQuery({
    queryKey: ["source-health", selectedSourceId],
    enabled: Boolean(selectedSourceId),
    queryFn: ({ signal }) => fetchSourceHealth(selectedSourceId!, signal),
    staleTime: 20_000,
    retry: false,
  });

  const parseFailureQuery = useQuery({
    queryKey: ["source-parse-failures", selectedSourceId],
    enabled: Boolean(selectedSourceId),
    queryFn: ({ signal }) =>
      fetchSourceParseFailures(selectedSourceId!, { limit: 5 }, signal),
    staleTime: 20_000,
    retry: false,
  });

  const sourceHealthStatus = sourceHealthQuery.data
    ? getSourceHealthStatus(sourceHealthQuery.data)
    : null;
  const sourceParseFailureItems = parseFailureQuery.data?.items ?? [];

  function handleSourceSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const name = sourceForm.name.trim();
    const location = sourceForm.location.trim();
    if (!name || !location) {
      setSourceFormError("名称和位置不能为空。");
      return;
    }

    setSourceFormError(null);
    createSourceMutation.mutate({
      name,
      type: sourceForm.type,
      location,
      enabled: sourceForm.enabled,
    });
  }

  return (
    <section className="panel source-panel">
      <header>
        <h2>Sources 管理</h2>
        <p>来源总数：{sourcesQuery.data?.total ?? sourceItems.length}</p>
      </header>

      <div className="source-layout">
        <div className="source-list-block">
          <h3>来源列表</h3>
          {sourcesQuery.isLoading ? <p className="feedback info">Sources 加载中...</p> : null}
          {sourcesQuery.isFetching && !sourcesQuery.isLoading ? (
            <p className="feedback info">Sources 刷新中...</p>
          ) : null}
          {sourcesQuery.isError ? (
            <p className="feedback error">Sources 加载失败：{toErrorMessage(sourcesQuery.error)}</p>
          ) : null}

          {!sourcesQuery.isLoading && !sourcesQuery.isError && sourceItems.length === 0 ? (
            <p className="feedback empty">暂无 Source，请先新增。</p>
          ) : null}

          {!sourcesQuery.isError && sourceItems.length > 0 ? (
            <div className="source-table-wrapper">
              <table className="source-table">
                <thead>
                  <tr>
                    <th>名称</th>
                    <th>类型</th>
                    <th>位置</th>
                    <th>状态</th>
                    <th>创建时间</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {sourceItems.map((source) => {
                    const latestResult = connectionResults[source.id];
                    const isSelected = source.id === selectedSourceId;
                    const isTesting =
                      testConnectionMutation.isPending &&
                      testConnectionMutation.variables === source.id;

                    return (
                      <tr key={source.id} className={isSelected ? "is-selected-row" : ""}>
                        <td>{source.name}</td>
                        <td>{source.type}</td>
                        <td>{source.location}</td>
                        <td>{source.enabled ? "启用" : "停用"}</td>
                        <td>{formatDateTime(source.createdAt)}</td>
                        <td>
                          <div className="source-action-row">
                            <button
                              type="button"
                              className="table-action"
                              onClick={() => setSelectedSourceId(source.id)}
                              aria-pressed={isSelected}
                            >
                              {isSelected ? "已选中" : "选中查看"}
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              disabled={isTesting}
                              onClick={() => testConnectionMutation.mutate(source.id)}
                            >
                              {isTesting ? "测试中..." : "测试连接"}
                            </button>
                          </div>
                          {latestResult ? (
                            <p
                              className={`tiny-feedback ${
                                latestResult.success ? "tiny-feedback-success" : "tiny-feedback-error"
                              }`}
                            >
                              {latestResult.message}
                            </p>
                          ) : null}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}

          {!sourcesQuery.isLoading && !sourcesQuery.isError ? (
            <section className="source-insight-panel" aria-label="Source 健康状态与解析失败">
              <header className="source-insight-header">
                <h3>健康状态与最近解析失败</h3>
                <p>{selectedSource ? `当前 Source：${selectedSource.name}` : "请先选择 Source"}</p>
              </header>

              <div className="source-insight-grid">
                <article className="source-insight-card">
                  <h4>健康状态</h4>
                  {!selectedSourceId ? <p className="feedback empty">请先选中一个 Source。</p> : null}
                  {selectedSourceId && sourceHealthQuery.isLoading ? (
                    <p className="feedback info">健康状态加载中...</p>
                  ) : null}
                  {selectedSourceId && sourceHealthQuery.isError ? (
                    <p className="feedback error">
                      健康状态加载失败：{toErrorMessage(sourceHealthQuery.error)}
                    </p>
                  ) : null}
                  {selectedSourceId && sourceHealthQuery.data ? (
                    <dl className="source-health-list">
                      <div className="source-health-row">
                        <dt>健康状态</dt>
                        <dd>
                          <span className={`source-health-status ${sourceHealthStatus?.className ?? ""}`}>
                            {sourceHealthStatus?.label ?? "--"}
                          </span>
                        </dd>
                      </div>
                      <div className="source-health-row">
                        <dt>接入模式</dt>
                        <dd>{sourceHealthQuery.data.accessMode}</dd>
                      </div>
                      <div className="source-health-row">
                        <dt>最近成功</dt>
                        <dd>{formatOptionalDateTime(sourceHealthQuery.data.lastSuccessAt)}</dd>
                      </div>
                      <div className="source-health-row">
                        <dt>最近失败</dt>
                        <dd>{formatOptionalDateTime(sourceHealthQuery.data.lastFailureAt)}</dd>
                      </div>
                      <div className="source-health-row">
                        <dt>失败次数</dt>
                        <dd>{sourceHealthQuery.data.failureCount.toLocaleString("zh-CN")}</dd>
                      </div>
                      <div className="source-health-row">
                        <dt>平均延迟</dt>
                        <dd>
                          {sourceHealthQuery.data.avgLatencyMs === null
                            ? "--"
                            : `${Math.round(sourceHealthQuery.data.avgLatencyMs).toLocaleString(
                                "zh-CN"
                              )} ms`}
                        </dd>
                      </div>
                      <div className="source-health-row">
                        <dt>新鲜度</dt>
                        <dd>
                          {sourceHealthQuery.data.freshnessMinutes === null
                            ? "--"
                            : `${sourceHealthQuery.data.freshnessMinutes.toLocaleString("zh-CN")} 分钟`}
                        </dd>
                      </div>
                    </dl>
                  ) : null}
                </article>

                <article className="source-insight-card">
                  <h4>最近解析失败</h4>
                  {!selectedSourceId ? <p className="feedback empty">请先选中一个 Source。</p> : null}
                  {selectedSourceId && parseFailureQuery.isLoading ? (
                    <p className="feedback info">解析失败列表加载中...</p>
                  ) : null}
                  {selectedSourceId && parseFailureQuery.isError ? (
                    <p className="feedback error">
                      解析失败列表加载失败：{toErrorMessage(parseFailureQuery.error)}
                    </p>
                  ) : null}
                  {selectedSourceId &&
                  !parseFailureQuery.isLoading &&
                  !parseFailureQuery.isError &&
                  sourceParseFailureItems.length === 0 ? (
                    <p className="feedback empty">最近暂无解析失败记录。</p>
                  ) : null}
                  {selectedSourceId && sourceParseFailureItems.length > 0 ? (
                    <ul className="source-failure-list">
                      {sourceParseFailureItems.map((item) => (
                        <li key={item.id} className="source-failure-item">
                          <header>
                            <strong>{item.errorCode}</strong>
                            <time dateTime={item.failedAt}>{formatDateTime(item.failedAt)}</time>
                          </header>
                          <p>{item.errorMessage}</p>
                          <p>
                            parser={item.parserKey}
                            {item.sourcePath ? ` | path=${item.sourcePath}` : ""}
                            {item.sourceOffset !== undefined ? ` | offset=${item.sourceOffset}` : ""}
                          </p>
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </article>
              </div>
            </section>
          ) : null}
        </div>

        <form className="source-form" onSubmit={handleSourceSubmit}>
          <h3>新增 Source</h3>
          <label htmlFor="source-name">名称</label>
          <input
            id="source-name"
            type="text"
            placeholder="例如：devbox-shanghai"
            value={sourceForm.name}
            onChange={(event) =>
              setSourceForm((prev) => ({
                ...prev,
                name: event.target.value,
              }))
            }
          />

          <label htmlFor="source-type">类型</label>
          <select
            id="source-type"
            value={sourceForm.type}
            onChange={(event) =>
              setSourceForm((prev) => ({
                ...prev,
                type: event.target.value as SourceType,
              }))
            }
          >
            {SOURCE_TYPE_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>

          <label htmlFor="source-location">位置</label>
          <input
            id="source-location"
            type="text"
            placeholder="例如：cn-shanghai / 10.0.0.8"
            value={sourceForm.location}
            onChange={(event) =>
              setSourceForm((prev) => ({
                ...prev,
                location: event.target.value,
              }))
            }
          />

          <label className="checkbox-field" htmlFor="source-enabled">
            <input
              id="source-enabled"
              type="checkbox"
              checked={sourceForm.enabled}
              onChange={(event) =>
                setSourceForm((prev) => ({
                  ...prev,
                  enabled: event.target.checked,
                }))
              }
            />
            启用该 Source
          </label>

          <button type="submit" className="submit-button" disabled={createSourceMutation.isPending}>
            {createSourceMutation.isPending ? "提交中..." : "新增 Source"}
          </button>

          {sourceFormError ? <p className="feedback error">{sourceFormError}</p> : null}
          {createSourceMutation.isError ? (
            <p className="feedback error">新增失败：{toErrorMessage(createSourceMutation.error)}</p>
          ) : null}
          {createSourceMutation.isSuccess ? (
            <p className="feedback success">新增成功，列表已刷新。</p>
          ) : null}
        </form>
      </div>
    </section>
  );
}

function PricingPage() {
  const queryClient = useQueryClient();
  const [note, setNote] = useState("");
  const [entries, setEntries] = useState<PricingEntryFormState[]>([createEmptyPricingEntry()]);
  const [formError, setFormError] = useState<string | null>(null);
  const [loadedVersionId, setLoadedVersionId] = useState<string>("");

  const catalogQuery = useQuery({
    queryKey: ["pricing-catalog"],
    queryFn: async ({ signal }) => {
      try {
        return await fetchPricingCatalog(signal);
      } catch (error) {
        if (error instanceof ApiError && error.status === 404) {
          return null;
        }
        throw error;
      }
    },
    staleTime: 20_000,
  });

  useEffect(() => {
    if (catalogQuery.data === undefined) {
      return;
    }

    const nextVersionId = catalogQuery.data?.version.id ?? "__empty__";
    if (loadedVersionId === nextVersionId) {
      return;
    }

    setLoadedVersionId(nextVersionId);
    setFormError(null);

    if (catalogQuery.data) {
      setNote(catalogQuery.data.version.note ?? "");
      setEntries(
        catalogQuery.data.entries.length > 0
          ? catalogQuery.data.entries.map((entry) => mapPricingEntryToForm(entry))
          : [createEmptyPricingEntry()]
      );
      return;
    }

    setNote("");
    setEntries([createEmptyPricingEntry()]);
  }, [catalogQuery.data, loadedVersionId]);

  const saveMutation = useMutation({
    mutationFn: (input: PricingCatalogUpsertInput) => upsertPricingCatalog(input),
    onSuccess: async () => {
      setFormError(null);
      await queryClient.invalidateQueries({ queryKey: ["pricing-catalog"] });
    },
  });

  function updateEntry(index: number, key: keyof PricingEntryFormState, value: string) {
    setEntries((prev) => prev.map((entry, idx) => (idx === index ? { ...entry, [key]: value } : entry)));
  }

  function handleSave(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const normalized = normalizePricingForm(entries);
    if (!normalized.success) {
      setFormError(normalized.message);
      return;
    }

    setFormError(null);
    saveMutation.mutate({
      note: note.trim().length > 0 ? note.trim() : undefined,
      entries: normalized.entries,
    });
  }

  return (
    <section className="panel">
      <header>
        <h2>Pricing Catalog</h2>
        <p>
          {catalogQuery.data
            ? `当前版本 v${catalogQuery.data.version.version} (${formatDateTime(
                catalogQuery.data.version.createdAt
              )})`
            : "当前租户尚未配置 catalog，可直接新建。"}
        </p>
      </header>

      {catalogQuery.isLoading ? <p className="feedback info">pricing 加载中...</p> : null}
      {catalogQuery.isError ? (
        <p className="feedback error">pricing 加载失败：{toErrorMessage(catalogQuery.error)}</p>
      ) : null}

      <form className="pricing-form" onSubmit={handleSave}>
        <label htmlFor="pricing-note">版本备注</label>
        <input
          id="pricing-note"
          type="text"
          placeholder="例如：2026Q1 基线价格"
          value={note}
          onChange={(event) => setNote(event.target.value)}
        />

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Model</th>
                <th>Input /1k</th>
                <th>Output /1k</th>
                <th>Currency</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((entry, index) => (
                <tr key={`pricing-entry-${index}`}> 
                  <td>
                    <input
                      type="text"
                      value={entry.model}
                      onChange={(event) => updateEntry(index, "model", event.target.value)}
                      placeholder="gpt-5"
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={entry.inputPer1k}
                      onChange={(event) => updateEntry(index, "inputPer1k", event.target.value)}
                      placeholder="0.003"
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={entry.outputPer1k}
                      onChange={(event) => updateEntry(index, "outputPer1k", event.target.value)}
                      placeholder="0.012"
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={entry.currency}
                      onChange={(event) => updateEntry(index, "currency", event.target.value)}
                      placeholder="USD"
                    />
                  </td>
                  <td>
                    <button
                      type="button"
                      className="table-action"
                      onClick={() =>
                        setEntries((prev) =>
                          prev.length > 1
                            ? prev.filter((_, rowIndex) => rowIndex !== index)
                            : [createEmptyPricingEntry()]
                        )
                      }
                    >
                      删除
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="button-row">
          <button
            type="button"
            className="submit-button secondary-button"
            onClick={() => setEntries((prev) => [...prev, createEmptyPricingEntry()])}
          >
            新增条目
          </button>
          <button type="submit" className="submit-button" disabled={saveMutation.isPending}>
            {saveMutation.isPending ? "保存中..." : "保存 Catalog"}
          </button>
        </div>

        {formError ? <p className="feedback error">{formError}</p> : null}
        {saveMutation.isError ? (
          <p className="feedback error">保存失败：{toErrorMessage(saveMutation.error)}</p>
        ) : null}
        {saveMutation.isSuccess ? <p className="feedback success">保存成功。</p> : null}
      </form>
    </section>
  );
}

interface WorkspaceProps {
  route: ConsoleRoute;
  onRouteChange: (route: ConsoleRoute) => void;
  sessionsDateKey: string | null;
  onDashboardDrilldownDate: (dateKey: string) => void;
}

function Workspace({
  route,
  onRouteChange,
  sessionsDateKey,
  onDashboardDrilldownDate,
}: WorkspaceProps) {
  const activeRoute = ROUTE_ITEMS.find((item) => item.key === route) ?? ROUTE_ITEMS[0];

  return (
    <main className="page-shell">
      <section className="header-band">
        <div>
          <p className="eyebrow">AgentLedger 企业治理台</p>
          <h1>{activeRoute.title}</h1>
          <p className="subtitle">{activeRoute.subtitle}</p>
        </div>
        <nav className="route-nav" aria-label="页面切换">
          {ROUTE_ITEMS.map((item) => (
            <button
              key={item.key}
              type="button"
              className={route === item.key ? "is-active" : ""}
              onClick={() => onRouteChange(item.key)}
            >
              {item.label}
            </button>
          ))}
        </nav>
      </section>

      {route === "dashboard" ? (
        <DashboardPage onDrilldownDate={onDashboardDrilldownDate} />
      ) : null}
      {route === "sessions" ? <SessionsPage initialDateKey={sessionsDateKey} /> : null}
      {route === "analytics" ? <AnalyticsPage /> : null}
      {route === "governance" ? <GovernancePage /> : null}
      {route === "sources" ? <SourcesPage /> : null}
      {route === "pricing" ? <PricingPage /> : null}
    </main>
  );
}

export default function App() {
  const [queryClient] = useState(() => new QueryClient());
  const [isAuthenticated, setIsAuthenticated] = useState(() => hasAccessToken());
  const [authMessage, setAuthMessage] = useState<string | null>(null);
  const [route, setRoute] = useState<ConsoleRoute>(() => readRouteFromHash());
  const [sessionsDateKey, setSessionsDateKey] = useState<string | null>(null);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const handleHashChange = () => {
      setRoute(readRouteFromHash());
    };

    window.addEventListener("hashchange", handleHashChange);
    if (!window.location.hash) {
      writeRouteToHash(DEFAULT_ROUTE);
    }
    setRoute(readRouteFromHash());

    return () => {
      window.removeEventListener("hashchange", handleHashChange);
    };
  }, []);

  useEffect(() => {
    setUnauthorizedHandler((message) => {
      queryClient.clear();
      setIsAuthenticated(false);
      setAuthMessage(message);
    });
    return () => {
      setUnauthorizedHandler(null);
    };
  }, [queryClient]);

  function handleLoggedIn() {
    queryClient.clear();
    setAuthMessage(null);
    setIsAuthenticated(true);
    setRoute(DEFAULT_ROUTE);
    writeRouteToHash(DEFAULT_ROUTE);
  }

  function handleRouteChange(nextRoute: ConsoleRoute) {
    setRoute(nextRoute);
    writeRouteToHash(nextRoute);
  }

  function handleDashboardDrilldownDate(dateKey: string) {
    setSessionsDateKey(dateKey);
    setRoute("sessions");
    writeRouteToHash("sessions");
  }

  return (
    <QueryClientProvider client={queryClient}>
      {isAuthenticated ? (
        <Workspace
          route={route}
          onRouteChange={handleRouteChange}
          sessionsDateKey={sessionsDateKey}
          onDashboardDrilldownDate={handleDashboardDrilldownDate}
        />
      ) : (
        <LoginPage authMessage={authMessage} onLoggedIn={handleLoggedIn} />
      )}
    </QueryClientProvider>
  );
}
