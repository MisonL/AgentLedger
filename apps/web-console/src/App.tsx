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
  approveReplicationJob,
  approveMcpApproval,
  ApiError,
  cancelReplicationJob,
  backfillSourceRegions,
  fetchAlertExternalLinkFailures,
  fetchAlertExternalLinkOps,
  fetchAlertOrchestrationExecutions,
  fetchAlertOrchestrationRules,
  fetchAgentRuntimeConfig,
  fetchAgentRuntimeViews,
  fetchAgentReleaseCheckBatchPreview,
  fetchAgentReleaseCheckPreview,
  fetchAgentReleases,
  createMcpApproval,
  createReplicationJob,
  createRuleApproval,
  createSystemConfigPackage,
  createSystemConfigPackageApproval,
  createRuleAsset,
  createRuleAssetVersion,
  createSource,
  createOpenPlatformReplayDataset,
  createOpenPlatformReplayDatasetVersion,
  createOpenPlatformReplayExperiment,
  createOpenPlatformReplayRun,
  executeOpenPlatformQualityAdvice,
  createIntegrationDlqRecoveryJob,
  downloadOpenPlatformReplayArtifact,
  evaluateMcpTool,
  exportSessions,
  exportUsage,
  exchangeExternalAuthCode,
  fetchAlerts,
  fetchAuthProviders,
  fetchIntegrationAlertFailureReport,
  fetchIntegrationAlertFailureTrends,
  fetchIntegrationDlqRecoveryJobDetail,
  fetchIntegrationDlqRecoveryJobs,
  fetchIntegrationDlqMessages,
  fetchMcpApprovals,
  fetchMcpInvocations,
  fetchMcpPolicies,
  fetchOpenPlatformAutomationPolicy,
  fetchOpenPlatformApiKeys,
  fetchOpenPlatformOpenApiSummary,
  fetchOpenPlatformQualityDaily,
  fetchOpenPlatformQualityForecast,
  fetchOpenPlatformQualityAdvice,
  fetchOpenPlatformQualityAdviceExecutions,
  fetchOpenPlatformQualityProjectTrends,
  fetchOpenPlatformQualityScorecards,
  fetchOpenPlatformReplayArtifacts,
  fetchOpenPlatformReplayDatasetVersionCases,
  fetchOpenPlatformReplayExperimentArtifacts,
  fetchOpenPlatformReplayExperimentCompare,
  fetchOpenPlatformReplayExperimentsBatchCompare,
  fetchOpenPlatformReplayExperimentResults,
  fetchOpenPlatformReplayExperiments,
  fetchOpenPlatformReplayExperimentWorkflow,
  fetchOpenPlatformReplayDatasetCases,
  fetchOpenPlatformReplayDatasetVersions,
  fetchOpenPlatformReplayDatasets,
  fetchOpenPlatformReplayDiffs,
  fetchOpenPlatformReplayRuns,
  materializeOpenPlatformReplayDatasetCases,
  fetchOpenPlatformWebhooks,
  fetchReplicationJobs,
  fetchResidencyPolicy,
  fetchResidencyKmsKeyMappings,
  fetchResidencyRegions,
  fetchResidencyArchiveRegionPolicies,
  fetchRuleApprovals,
  fetchRuleAssetVersions,
  fetchRuleAssetVersionDiff,
  fetchRuleAssets,
  fetchSystemConfigPackageApprovals,
  fetchSystemConfigPackages,
  fetchSystemConfigWatchLatest,
  fetchTokenPulseRuntimeEvents,
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
  runOpenPlatformReplayExperiment,
  retryAlertExternalLinkSync,
  retryAlertExternalLinkSyncBatch,
  revokeOpenPlatformApiKey,
  replaceOpenPlatformReplayDatasetCases,
  promoteOpenPlatformReplayDatasetVersion,
  cancelOpenPlatformQualityAdviceExecution,
  cancelOpenPlatformReplayExperiment,
  searchSessions,
  setUnauthorizedHandler,
  testSourceConnection,
  simulateAlertOrchestration,
  replayOpenPlatformWebhook,
  deleteOpenPlatformWebhook,
  OPEN_PLATFORM_QUALITY_AUTOMATION_TOOL_ID,
  publishSystemConfigPackage,
  simulateOpenPlatformAutomationPolicy,
  upsertOpenPlatformAutomationPolicy,
  upsertOpenPlatformApiKey,
  upsertOpenPlatformWebhook,
  upsertMcpPolicy,
  upsertAlertOrchestrationRule,
  upsertResidencyPolicy,
  upsertResidencyKmsKeyMappings,
  upsertResidencyArchiveRegionPolicies,
  updateAlertStatus,
  updateSource,
  upsertPricingCatalog,
} from "./api";
import type {
  AlertExternalLinkBatchRetryResponse,
  AlertExternalLinkFailureResponse,
  AlertExternalLinkOpsResponse,
  IntegrationAlertFailureReportResponse,
  IntegrationAlertFailureTrendResponse,
  IntegrationDlqMessageListResponse,
  IntegrationDlqRecoveryJob,
  IntegrationDlqRecoveryJobListResponse,
  AlertOrchestrationChannel,
  AlertOrchestrationDispatchMode,
  AlertOrchestrationEscalationReason,
  AlertOrchestrationEventType,
  AlertOrchestrationExecutionListInput,
  AlertOrchestrationExecutionLog,
  AlertOrchestrationRule,
  AlertOrchestrationRuleListInput,
  AlertOrchestrationSimulateInput,
  AlertOrchestrationSimulationResponse,
  AlertItem,
  AlertMutableStatus,
  AlertSeverity,
  AlertStatus,
  AgentRelease,
  AgentRuntimeConfigResponse,
  AgentRuntimeView,
  AgentReleaseArtifact,
  AgentReleaseCheckBatchPreviewResponse,
  AgentReleaseBatchCheckSampleInput,
  AgentReleaseChannel,
  AgentReleaseCheckPreviewResponse,
  AuthProviderItem,
  AuthLoginInput,
  CreateSourceInput,
  DataResidencyMode,
  ExportFormat,
  HeatmapCell,
  McpApprovalRequest,
  McpApprovalConfig,
  McpApprovalMode,
  McpApprovalStage,
  McpApprovalWorkflowCondition,
  McpApprovalWorkflow,
  McpApprovalWorkflowNode,
  McpApprovalWorkflowNodeSnapshot,
  McpApprovalWorkflowTransition,
  McpApprovalWorkflowTransitionPreview,
  McpInvocationAudit,
  McpRiskLevel,
  McpToolDecision,
  McpToolPolicy,
  MetricKey,
  PricingCatalogEntry,
  PricingCatalogUpsertInput,
  OpenPlatformApiKey,
  OpenPlatformApiKeyStatus,
  OpenPlatformAutomationPolicy,
  OpenPlatformAutomationPolicySimulationResponse,
  OpenPlatformAutomationStrategyRule,
  OpenPlatformOpenApiSummary,
  OpenPlatformQualityAdviceItem,
  OpenPlatformQualityAdviceExecution,
  OpenPlatformQualityDailyItem,
  OpenPlatformQualityForecastItem,
  OpenPlatformQualityProjectTrendItem,
  OpenPlatformQualityProjectTrendResponse,
  OpenPlatformQualityDailyResponse,
  OpenPlatformQualityScorecard,
  OpenPlatformReplayArtifact,
  OpenPlatformReplayDataset,
  OpenPlatformReplayDatasetCase,
  OpenPlatformReplayDatasetVersion,
  OpenPlatformReplayDiffItem,
  OpenPlatformReplayDatasetMaterializeResponse,
  OpenPlatformReplayExperiment,
  OpenPlatformReplayExperimentBatchCompareResponse,
  OpenPlatformReplayExperimentCompareResponse,
  OpenPlatformReplayRun,
  OpenPlatformReplayJobStatus,
  OpenPlatformReplayExperimentWorkflowResponse,
  OpenPlatformWebhook,
  RegionDescriptor,
  ResidencyArchiveRegionPolicy,
  ResidencyKmsKeyMapping,
  ReplicationJob,
  ReplicationJobStatus,
  RuleApproval,
  RuleApprovalDecision,
  RuleAsset,
  RuleAssetVersionDiffResponse,
  RuleAssetVersion,
  RuleLifecycleStatus,
  SystemConfigPackage,
  SystemConfigPackageCreateInput,
  SystemConfigPackageApproval,
  SystemConfigPackageApprovalDecision,
  TokenPulseRuntimeEvent,
  TokenPulseRuntimeEventStatus,
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
  | "agents"
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
    key: "agents",
    label: "Agents",
    title: "Agent 守护视图",
    subtitle: "查看守护状态、最近心跳与运行时配置。",
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
  sourceRegion: string;
  enabled: boolean;
}

const INITIAL_SOURCE_FORM: SourceFormState = {
  name: "",
  type: "local",
  location: "",
  sourceRegion: "",
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

const ALERT_STATUS_FILTER_OPTIONS: Array<
  { value: ""; label: string } | { value: AlertStatus; label: string }
> = [
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

const ALERT_ORCHESTRATION_EVENT_TYPE_OPTIONS: Array<{
  value: AlertOrchestrationEventType;
  label: string;
}> = [
  { value: "alert", label: "alert" },
  { value: "weekly", label: "weekly" },
];

const ALERT_ORCHESTRATION_CHANNEL_OPTIONS: Array<{
  value: AlertOrchestrationChannel;
  label: string;
}> = [
  { value: "webhook", label: "webhook" },
  { value: "wecom", label: "wecom" },
  { value: "dingtalk", label: "dingtalk" },
  { value: "feishu", label: "feishu" },
  { value: "email", label: "email" },
  { value: "email_webhook", label: "email_webhook" },
  { value: "incident", label: "incident" },
  { value: "ticket", label: "ticket" },
];

const ALERT_ORCHESTRATION_DISPATCH_MODE_OPTIONS: Array<
  | {
      value: "";
      label: string;
    }
  | {
      value: AlertOrchestrationDispatchMode;
      label: string;
    }
> = [
  { value: "", label: "全部模式" },
  { value: "rule", label: "rule" },
  { value: "fallback", label: "fallback" },
];

const ALERT_ORCHESTRATION_ESCALATION_REASON_OPTIONS: Array<
  { value: ""; label: string } | { value: AlertOrchestrationEscalationReason; label: string }
> = [{ value: "", label: "全部升级原因" }, { value: "sla_timeout", label: "sla_timeout" }];

const ALERT_ORCHESTRATION_ENABLED_FILTER_OPTIONS: Array<
  | {
      value: "";
      label: string;
    }
  | {
      value: "true" | "false";
      label: string;
    }
> = [
  { value: "", label: "全部启用状态" },
  { value: "true", label: "enabled=true" },
  { value: "false", label: "enabled=false" },
];

const BOOLEAN_FILTER_OPTIONS: Array<
  | {
      value: "";
      label: string;
    }
  | {
      value: "true" | "false";
      label: string;
    }
> = [
  { value: "", label: "全部" },
  { value: "true", label: "true" },
  { value: "false", label: "false" },
];

const CONFLICT_FILTER_OPTIONS: Array<
  | {
      value: "";
      label: string;
    }
  | {
      value: "true" | "false";
      label: string;
    }
> = [
  { value: "", label: "全部冲突状态" },
  { value: "true", label: "仅冲突" },
  { value: "false", label: "仅无冲突" },
];

const EXPORT_FORMAT_OPTIONS: Array<{ value: ExportFormat; label: string }> = [
  { value: "json", label: "JSON" },
  { value: "csv", label: "CSV" },
];

const USAGE_EXPORT_DIMENSION_OPTIONS: Array<{
  value: UsageExportDimension;
  label: string;
}> = [
  { value: "daily", label: "daily" },
  { value: "weekly", label: "weekly" },
  { value: "monthly", label: "monthly" },
  { value: "models", label: "models" },
  { value: "sessions", label: "sessions" },
  { value: "heatmap", label: "heatmap" },
];

const WEEKLY_SUMMARY_METRIC_OPTIONS: Array<{
  value: MetricKey;
  label: string;
}> = [
  { value: "tokens", label: "tokens" },
  { value: "cost", label: "cost" },
  { value: "sessions", label: "sessions" },
];

const WEEKLY_SUMMARY_TIMEZONE_OPTIONS: Array<{ value: string; label: string }> =
  [
    { value: "UTC", label: "UTC" },
    { value: "Asia/Shanghai", label: "Asia/Shanghai" },
    { value: "America/Los_Angeles", label: "America/Los_Angeles" },
  ];

const DATA_RESIDENCY_MODE_OPTIONS: Array<{
  value: DataResidencyMode;
  label: string;
}> = [
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

const TOKENPULSE_RUNTIME_STATUS_FILTER_OPTIONS: Array<
  { value: ""; label: string } | { value: TokenPulseRuntimeEventStatus; label: string }
> = [
  { value: "", label: "全部状态" },
  { value: "success", label: "success" },
  { value: "failure", label: "failure" },
  { value: "blocked", label: "blocked" },
  { value: "timeout", label: "timeout" },
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

const MCP_APPROVAL_MODE_OPTIONS: Array<{ value: McpApprovalMode; label: string }> = [
  { value: "single_stage", label: "single_stage" },
  { value: "two_stage", label: "two_stage" },
  { value: "multi_stage", label: "multi_stage" },
];

const MCP_APPROVAL_STATUS_FILTER_OPTIONS: Array<
  | { value: ""; label: string }
  | { value: McpApprovalRequest["status"]; label: string }
> = [
  { value: "", label: "全部状态" },
  { value: "pending", label: "pending" },
  { value: "approved", label: "approved" },
  { value: "rejected", label: "rejected" },
];

const OPEN_PLATFORM_API_KEY_STATUS_FILTER_OPTIONS: Array<
  | { value: ""; label: string }
  | { value: OpenPlatformApiKeyStatus; label: string }
> = [
  { value: "", label: "全部状态" },
  { value: "active", label: "active" },
  { value: "disabled", label: "disabled" },
];

const OPEN_PLATFORM_WEBHOOK_ENABLED_FILTER_OPTIONS: Array<
  { value: ""; label: string } | { value: "true" | "false"; label: string }
> = [
  { value: "", label: "全部 webhook" },
  { value: "true", label: "enabled=true" },
  { value: "false", label: "enabled=false" },
];

const OPEN_PLATFORM_QUALITY_METRIC_OPTIONS: Array<{
  value: string;
  label: string;
}> = [
  { value: "", label: "全部指标" },
  { value: "accuracy", label: "accuracy" },
  { value: "consistency", label: "consistency" },
  { value: "groundedness", label: "groundedness" },
  { value: "safety", label: "safety" },
  { value: "latency", label: "latency" },
];

const OPEN_PLATFORM_WEBHOOK_EVENT_OPTIONS = [
  "api_key.created",
  "api_key.revoked",
  "quality.event.created",
  "quality.scorecard.updated",
  "replay.run.started",
  "replay.run.completed",
  "replay.run.regression_detected",
  "replay.run.failed",
  "replay.run.cancelled",
  "replay.job.started",
  "replay.job.completed",
  "replay.job.failed",
] as const;
const OPEN_PLATFORM_WEBHOOK_EVENT_OPTION_SET = new Set<string>(
  OPEN_PLATFORM_WEBHOOK_EVENT_OPTIONS,
);

const OPEN_PLATFORM_REPLAY_JOB_STATUS_FILTER_OPTIONS: Array<
  | { value: ""; label: string }
  | { value: OpenPlatformReplayJobStatus; label: string }
> = [
  { value: "", label: "全部任务状态" },
  { value: "pending", label: "pending" },
  { value: "running", label: "running" },
  { value: "completed", label: "completed" },
  { value: "failed", label: "failed" },
  { value: "cancelled", label: "cancelled" },
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

  const normalized = window.location.hash
    .replace(/^#\/?/, "")
    .trim()
    .toLowerCase();
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
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function createCodeVerifier(): string {
  if (
    typeof crypto !== "undefined" &&
    typeof crypto.getRandomValues === "function"
  ) {
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
      new TextEncoder().encode(codeVerifier),
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
      JSON.stringify(state),
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
    const raw = window.sessionStorage.getItem(
      AUTH_EXTERNAL_PENDING_STORAGE_KEY,
    );
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw) as Partial<ExternalAuthPendingState>;
    if (
      typeof parsed.providerId !== "string" ||
      typeof parsed.state !== "string" ||
      typeof parsed.redirectUri !== "string" ||
      (parsed.codeVerifier !== undefined &&
        typeof parsed.codeVerifier !== "string") ||
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
  search: string,
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
  const providerId = readParam("providerId", "provider");
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
  codeChallenge?: string,
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

function formatAgentRuntimeStatus(
  status: AgentRuntimeView["runtimeStatus"],
): string {
  switch (status) {
    case "online":
      return "在线";
    case "stale":
      return "陈旧";
    case "never_seen":
      return "未上报";
    default:
      return status;
  }
}

function formatSourceFreshness(item: SessionSourceFreshness): string {
  const sourceLabel = item.sourceName ?? item.sourceId;
  const freshnessLabel =
    item.freshnessMinutes === null
      ? "--"
      : `${item.freshnessMinutes.toLocaleString("zh-CN")} 分钟`;
  const latencyLabel =
    item.avgLatencyMs === null
      ? "--"
      : `${Math.round(item.avgLatencyMs).toLocaleString("zh-CN")} ms`;

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

function formatCompactJson(value: Record<string, unknown> | undefined): string {
  if (!value || Object.keys(value).length === 0) {
    return "--";
  }
  const serialized = JSON.stringify(value);
  if (!serialized) {
    return "--";
  }
  return serialized.length > 120
    ? `${serialized.slice(0, 117)}...`
    : serialized;
}

function formatPrettyJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return "";
  }
}

function parseBooleanSelect(value: "" | "true" | "false"): boolean | undefined {
  if (value === "true") {
    return true;
  }
  if (value === "false") {
    return false;
  }
  return undefined;
}

function buildResidencyPolicyHydrationSignature(policy: {
  mode: DataResidencyMode;
  primaryRegion: string;
  replicaRegions: string[];
  allowCrossRegionTransfer: boolean;
  requireTransferApproval: boolean;
  updatedAt: string;
}): string {
  return JSON.stringify({
    mode: policy.mode,
    primaryRegion: policy.primaryRegion,
    replicaRegions: [...policy.replicaRegions].sort(),
    allowCrossRegionTransfer: policy.allowCrossRegionTransfer,
    requireTransferApproval: policy.requireTransferApproval,
    updatedAt: policy.updatedAt,
  });
}

function parseDistinctCommaSeparatedList(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter((item, index, items) => item.length > 0 && items.indexOf(item) === index);
}

function formatAlertExternalLinks(
  externalLinks: AlertItem["externalLinks"],
): string {
  if (!Array.isArray(externalLinks) || externalLinks.length === 0) {
    return "--";
  }
  return externalLinks
    .map((item) => {
      const parts = [`${item.externalType}:${item.externalId}`];
      if (item.externalStatus) {
        parts.push(`status=${item.externalStatus}`);
      }
      if (item.pendingExternalStatus) {
        parts.push(`pending=${item.pendingExternalStatus}`);
      }
      if (item.publishStatus) {
        parts.push(`publish=${item.publishStatus}`);
      }
      if (item.publishError) {
        parts.push(`publishError=${item.publishError}`);
      }
      if (item.lastSyncResult) {
        parts.push(`sync=${item.lastSyncResult}`);
      }
      if (item.lastSyncError) {
        parts.push(`error=${item.lastSyncError}`);
      }
      if (item.lastSyncFailureStage) {
        parts.push(`stage=${item.lastSyncFailureStage}`);
      }
      if (item.lastSyncFailureCode) {
        parts.push(`code=${item.lastSyncFailureCode}`);
      }
      return parts.join(" ");
    })
    .join(" | ");
}

type AlertExternalLinkItem = NonNullable<AlertItem["externalLinks"]>[number];

function resolveAlertExternalLinkDesiredStatus(
  alertStatus: AlertStatus,
  link: AlertExternalLinkItem,
): string {
  const pending = link.pendingExternalStatus?.trim();
  if (pending) {
    return pending;
  }
  return alertStatus;
}

function isAlertExternalLinkRetryable(
  alertStatus: AlertStatus,
  link: AlertExternalLinkItem,
): boolean {
  if (link.publishStatus === "failed" || link.lastSyncResult === "failed") {
    return true;
  }
  return resolveAlertExternalLinkDesiredStatus(alertStatus, link) !==
    (link.externalStatus ?? "");
}

function parseOptionalNonNegativeInteger(value: string): number | undefined {
  if (value.trim().length === 0) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    return undefined;
  }
  return parsed;
}

function parseCommaSeparatedValues(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(
      (item, index, list) => item.length > 0 && list.indexOf(item) === index,
    );
}

function buildRuleScopeBindingInput(input: {
  organizations: string;
  projects: string;
  clients: string;
}): RuleAsset["scopeBinding"] | undefined {
  const organizations = parseCommaSeparatedValues(input.organizations);
  const projects = parseCommaSeparatedValues(input.projects);
  const clients = parseCommaSeparatedValues(input.clients);

  if (
    organizations.length === 0 &&
    projects.length === 0 &&
    clients.length === 0
  ) {
    return undefined;
  }

  return {
    ...(organizations.length > 0 ? { organizations } : {}),
    ...(projects.length > 0 ? { projects } : {}),
    ...(clients.length > 0 ? { clients } : {}),
  };
}

type McpWorkflowNodeDraft = {
  nodeId: string;
  label: string;
  stage: string;
  requiredApprovals: string;
  roles: string;
};

type McpWorkflowTransitionDraft = {
  fromNodeId: string;
  toNodeId: string;
  mode: "default" | "conditional";
  riskLevelAtLeast: "" | McpRiskLevel;
  toolIds: string;
  tenantRoles: string;
  timeWindowTimezone: string;
  timeWindowWeekdays: string;
  timeWindowStartTime: string;
  timeWindowEndTime: string;
};

function createDefaultMcpWorkflowNodeDraft(index: number): McpWorkflowNodeDraft {
  return {
    nodeId: `stage${index + 1}-node`,
    label: `Stage ${index + 1}`,
    stage: `stage${index + 1}`,
    requiredApprovals: "1",
    roles: index === 0 ? "owner,maintainer" : "owner",
  };
}

function createDefaultMcpWorkflowDraftState(): {
  entryNodeId: string;
  nodes: McpWorkflowNodeDraft[];
  transitions: McpWorkflowTransitionDraft[];
} {
  const firstNode = createDefaultMcpWorkflowNodeDraft(0);
  return {
    entryNodeId: firstNode.nodeId,
    nodes: [firstNode],
    transitions: [
      {
        fromNodeId: firstNode.nodeId,
        toNodeId: "approved",
        mode: "default",
        riskLevelAtLeast: "",
        toolIds: "",
        tenantRoles: "",
        timeWindowTimezone: "",
        timeWindowWeekdays: "",
        timeWindowStartTime: "",
        timeWindowEndTime: "",
      },
    ],
  };
}

function buildMcpWorkflowFromDraft(input: {
  entryNodeId: string;
  nodes: McpWorkflowNodeDraft[];
  transitions: McpWorkflowTransitionDraft[];
}): { success: true; data: McpApprovalWorkflow } | { success: false; message: string } {
  if (input.nodes.length === 0) {
    return { success: false, message: "至少需要一个审批节点。" };
  }
  const nodes: McpApprovalWorkflowNode[] = [];
  const nodeIds = new Set<string>();
  for (const [index, node] of input.nodes.entries()) {
    const nodeId = node.nodeId.trim();
    const stage = node.stage.trim();
    const requiredApprovals = Number(node.requiredApprovals);
    const roles = parseDistinctCommaSeparatedList(node.roles);
    if (
      !nodeId ||
      !stage ||
      !/^stage[1-9]\d*$/.test(stage) ||
      nodeIds.has(nodeId) ||
      !Number.isInteger(requiredApprovals) ||
      requiredApprovals < 1 ||
      roles.length === 0
    ) {
      return { success: false, message: `审批节点 ${index + 1} 配置非法。` };
    }
    nodeIds.add(nodeId);
    nodes.push({
      nodeId,
      kind: "approval",
      label: node.label.trim() || undefined,
      stage: stage as McpApprovalStage,
      requiredApprovals,
      roles,
    });
  }
  if (!nodeIds.has(input.entryNodeId)) {
    return { success: false, message: "入口节点必须命中一个审批节点。" };
  }
  const terminalNodeIds = new Set(["approved", "rejected"]);
  const transitions: McpApprovalWorkflowTransition[] = [];
  for (const [index, transition] of input.transitions.entries()) {
    const fromNodeId = transition.fromNodeId.trim();
    const toNodeId = transition.toNodeId.trim();
    if (
      !fromNodeId ||
      !toNodeId ||
      !nodeIds.has(fromNodeId) ||
      (!nodeIds.has(toNodeId) && !terminalNodeIds.has(toNodeId))
    ) {
      return { success: false, message: `转移 ${index + 1} 非法。` };
    }
    const condition =
      transition.mode === "default"
        ? { default: true }
        : (() => {
            const timeWindowTimezone = transition.timeWindowTimezone.trim();
            const timeWindowStartTime = transition.timeWindowStartTime.trim();
            const timeWindowEndTime = transition.timeWindowEndTime.trim();
            const timeWindowWeekdays = transition.timeWindowWeekdays
              .split(",")
              .map((item) => item.trim())
              .filter((item) => item.length > 0)
              .map((item) => Number(item));
            if (
              [timeWindowTimezone, timeWindowStartTime, timeWindowEndTime].some(
                (item) => item.length > 0,
              ) &&
              (!timeWindowTimezone ||
                !timeWindowStartTime ||
                !timeWindowEndTime ||
                !/^\d{2}:\d{2}$/.test(timeWindowStartTime) ||
                !/^\d{2}:\d{2}$/.test(timeWindowEndTime) ||
                timeWindowWeekdays.some(
                  (item) =>
                    !Number.isInteger(item) || item < 1 || item > 7,
                ))
            ) {
              return "invalid" as const;
            }
            return {
              ...(transition.riskLevelAtLeast
                ? { riskLevelAtLeast: transition.riskLevelAtLeast }
                : {}),
              ...(parseDistinctCommaSeparatedList(transition.toolIds).length > 0
                ? { toolIds: parseDistinctCommaSeparatedList(transition.toolIds) }
                : {}),
              ...(parseDistinctCommaSeparatedList(transition.tenantRoles).length > 0
                ? { tenantRoles: parseDistinctCommaSeparatedList(transition.tenantRoles) }
                : {}),
              ...(timeWindowTimezone &&
              timeWindowStartTime &&
              timeWindowEndTime
                ? {
                    timeWindow: {
                      timezone: timeWindowTimezone,
                      ...(timeWindowWeekdays.length > 0
                        ? {
                            weekdays: Array.from(new Set(timeWindowWeekdays)),
                          }
                        : {}),
                      startTime: timeWindowStartTime,
                      endTime: timeWindowEndTime,
                    },
                  }
                : {}),
            };
          })();
    if (condition === "invalid") {
      return { success: false, message: `转移 ${index + 1} 的 timeWindow 配置非法。` };
    }
    transitions.push({
      fromNodeId,
      toNodeId,
      condition,
    });
  }
  for (const node of nodes) {
    const outgoing = transitions.filter((item) => item.fromNodeId === node.nodeId);
    if (outgoing.length === 0) {
      return { success: false, message: `审批节点 ${node.nodeId} 缺少转移。` };
    }
    if (outgoing.filter((item) => item.condition?.default === true).length !== 1) {
      return { success: false, message: `审批节点 ${node.nodeId} 必须且仅能有一条 default 转移。` };
    }
  }
  return {
    success: true,
    data: {
      entryNodeId: input.entryNodeId,
      nodes: [
        ...nodes,
        { nodeId: "approved", kind: "terminal_approved", label: "Approved" },
        { nodeId: "rejected", kind: "terminal_rejected", label: "Rejected" },
      ],
      transitions,
    },
  };
}

function buildMcpWorkflowDraftFromWorkflow(
  workflow?: McpApprovalWorkflow,
): {
  entryNodeId: string;
  nodes: McpWorkflowNodeDraft[];
  transitions: McpWorkflowTransitionDraft[];
} {
  if (!workflow) {
    return createDefaultMcpWorkflowDraftState();
  }
  const nodes = workflow.nodes
    .filter((node) => node.kind === "approval")
    .map((node, index) => ({
      nodeId: node.nodeId,
      label: node.label ?? "",
      stage: node.stage ?? `stage${index + 1}`,
      requiredApprovals: String(node.requiredApprovals ?? 1),
      roles: (node.roles ?? []).join(","),
    }));
  const transitions = workflow.transitions
    .filter((item) => item.fromNodeId !== "approved" && item.fromNodeId !== "rejected")
    .map((transition) => ({
      fromNodeId: transition.fromNodeId,
      toNodeId: transition.toNodeId,
      mode: (transition.condition?.default === true ? "default" : "conditional") as
        | "default"
        | "conditional",
      riskLevelAtLeast: (transition.condition?.riskLevelAtLeast ?? "") as "" | McpRiskLevel,
      toolIds: (transition.condition?.toolIds ?? []).join(","),
      tenantRoles: (transition.condition?.tenantRoles ?? []).join(","),
      timeWindowTimezone: transition.condition?.timeWindow?.timezone ?? "",
      timeWindowWeekdays: (transition.condition?.timeWindow?.weekdays ?? []).join(","),
      timeWindowStartTime: transition.condition?.timeWindow?.startTime ?? "",
      timeWindowEndTime: transition.condition?.timeWindow?.endTime ?? "",
    }));
  return {
    entryNodeId: workflow.entryNodeId,
    nodes: nodes.length > 0 ? nodes : createDefaultMcpWorkflowDraftState().nodes,
    transitions:
      transitions.length > 0 ? transitions : createDefaultMcpWorkflowDraftState().transitions,
  };
}

function buildStaticMcpApprovalStages(
  mode: McpApprovalMode,
  stage1RequiredApprovals: number | undefined,
  stage2RequiredApprovals: number | undefined,
  stage1Roles: string[],
  stage2Roles: string[],
): Array<{
  stage: McpApprovalStage;
  requiredApprovals: number;
  roles: string[];
}> {
  return [
    {
      stage: "stage1",
      requiredApprovals: stage1RequiredApprovals ?? 1,
      roles:
        stage1Roles.length > 0 ? stage1Roles : ["owner", "maintainer"],
    },
    ...(mode === "two_stage"
      ? [
          {
            stage: "stage2" as McpApprovalStage,
            requiredApprovals: stage2RequiredApprovals ?? 1,
            roles: stage2Roles.length > 0 ? stage2Roles : ["owner"],
          },
        ]
      : []),
  ];
}

function stringifyStaticMcpApprovalStages(
  mode: McpApprovalMode,
  stage1RequiredApprovals: number | undefined,
  stage2RequiredApprovals: number | undefined,
  stage1Roles: string[],
  stage2Roles: string[],
): string {
  return JSON.stringify(
    buildStaticMcpApprovalStages(
      mode,
      stage1RequiredApprovals,
      stage2RequiredApprovals,
      stage1Roles,
      stage2Roles,
    ),
    null,
    2,
  );
}

function parseMcpApprovalStagesJson(
  rawValue: string,
): { success: true; data: Array<{
  nodeId?: string;
  stage: McpApprovalStage;
  label?: string;
  requiredApprovals: number;
  roles: string[];
}> } | { success: false; message: string } {
  const trimmed = rawValue.trim();
  if (!trimmed) {
    return { success: false, message: "approvalStages JSON 不能为空。" };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return { success: false, message: "approvalStages JSON 不是合法 JSON。" };
  }
  if (!Array.isArray(parsed) || parsed.length === 0) {
    return { success: false, message: "approvalStages JSON 必须是非空数组。" };
  }
  const stages: Array<{
    nodeId?: string;
    stage: McpApprovalStage;
    label?: string;
    requiredApprovals: number;
    roles: string[];
  }> = [];
  const seenStages = new Set<string>();
  for (const [index, item] of parsed.entries()) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      return { success: false, message: `approvalStages[${index}] 必须是对象。` };
    }
    const record = item as Record<string, unknown>;
    const stageValue =
      typeof record.stage === "string" && record.stage.trim().length > 0
        ? record.stage.trim()
        : `stage${index + 1}`;
    if (!/^stage[1-9]\d*$/.test(stageValue) || seenStages.has(stageValue)) {
      return { success: false, message: `approvalStages[${index}].stage 非法或重复。` };
    }
    const requiredApprovals = Number(record.requiredApprovals);
    const roles =
      Array.isArray(record.roles) &&
      record.roles.every((role) => typeof role === "string" && role.trim().length > 0)
        ? Array.from(
            new Set(record.roles.map((role) => String(role).trim())),
          )
        : null;
    if (!Number.isInteger(requiredApprovals) || requiredApprovals < 1 || !roles || roles.length === 0) {
      return {
        success: false,
        message: `approvalStages[${index}] 的 requiredApprovals/roles 非法。`,
      };
    }
    seenStages.add(stageValue);
    const nodeId =
      typeof record.nodeId === "string" && record.nodeId.trim().length > 0
        ? record.nodeId.trim()
        : undefined;
    const label =
      typeof record.label === "string" && record.label.trim().length > 0
        ? record.label.trim()
        : undefined;
    stages.push({
      nodeId,
      stage: stageValue as McpApprovalStage,
      label,
      requiredApprovals,
      roles,
    });
  }
  return { success: true, data: stages };
}

function parseQualityAutomationStrategyMatrixJson(
  rawValue: string,
): { success: true; data: OpenPlatformAutomationStrategyRule[] } | { success: false; message: string } {
  const trimmed = rawValue.trim();
  if (!trimmed) {
    return { success: true, data: [] };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return { success: false, message: "strategyMatrix JSON 不是合法 JSON。" };
  }
  if (!Array.isArray(parsed)) {
    return { success: false, message: "strategyMatrix JSON 必须是数组。" };
  }
  const metricSet = new Set(["accuracy", "consistency", "groundedness", "safety", "latency"]);
  const severitySet = new Set(["info", "warn", "critical"]);
  const trendDirectionSet = new Set(["up", "down", "flat"]);
  const rules: OpenPlatformAutomationStrategyRule[] = [];
  for (const [index, item] of parsed.entries()) {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      return { success: false, message: `strategyMatrix[${index}] 必须是对象。` };
    }
    const record = item as Record<string, unknown>;
    const id =
      typeof record.id === "string" && record.id.trim().length > 0
        ? record.id.trim()
        : `rule-${index + 1}`;
    const actionType =
      record.actionType === "scorecard_adjustment" ||
      record.actionType === "replay_experiment"
        ? record.actionType
        : null;
    const requiresApproval =
      typeof record.requiresApproval === "boolean" ? record.requiresApproval : null;
    const reason =
      typeof record.reason === "string" && record.reason.trim().length > 0
        ? record.reason.trim()
        : null;
    if (!actionType || requiresApproval === null || !reason) {
      return {
        success: false,
        message: `strategyMatrix[${index}] 的 actionType/requiresApproval/reason 非法。`,
      };
    }
    const metric =
      record.metric === undefined
        ? undefined
        : typeof record.metric === "string" && metricSet.has(record.metric.trim())
          ? record.metric.trim()
          : null;
    if (metric === null) {
      return {
        success: false,
        message: `strategyMatrix[${index}] 的 metric 非法。`,
      };
    }
    const severity =
      record.severity === undefined
        ? undefined
        : typeof record.severity === "string" && severitySet.has(record.severity)
          ? (record.severity as "info" | "warn" | "critical")
          : null;
    if (severity === null) {
      return {
        success: false,
        message: `strategyMatrix[${index}] 的 severity 非法。`,
      };
    }
    const trendDirection =
      record.trendDirection === undefined
        ? undefined
        : typeof record.trendDirection === "string" && trendDirectionSet.has(record.trendDirection)
          ? (record.trendDirection as "up" | "down" | "flat")
          : null;
    if (trendDirection === null) {
      return {
        success: false,
        message: `strategyMatrix[${index}] 的 trendDirection 非法。`,
      };
    }
    const stringFieldOrUndefined = (value: unknown, field: string) => {
      if (value === undefined || value === null) {
        return { ok: true as const, value: undefined };
      }
      if (typeof value !== "string" || value.trim().length === 0) {
        return { ok: false as const, message: `strategyMatrix[${index}] 的 ${field} 不能为空字符串。` };
      }
      return { ok: true as const, value: value.trim() };
    };
    const providerResult = stringFieldOrUndefined(record.provider, "provider");
    if (!providerResult.ok) {
      return { success: false, message: providerResult.message };
    }
    const workflowResult = stringFieldOrUndefined(record.workflow, "workflow");
    if (!workflowResult.ok) {
      return { success: false, message: workflowResult.message };
    }
    const projectPatternResult = stringFieldOrUndefined(record.projectPattern, "projectPattern");
    if (!projectPatternResult.ok) {
      return { success: false, message: projectPatternResult.message };
    }
    const numericOrUndefined = (
      value: unknown,
      options: { field: string; integer?: boolean; min?: number; max?: number },
    ) => {
      if (value === undefined || value === null || value === "") {
        return { ok: true as const, value: undefined };
      }
      const parsedNumber = Number(value);
      if (!Number.isFinite(parsedNumber)) {
        return { ok: false as const, message: `strategyMatrix[${index}].${options.field} 必须是数字。` };
      }
      if (options.integer && !Number.isInteger(parsedNumber)) {
        return { ok: false as const, message: `strategyMatrix[${index}].${options.field} 必须是非负整数。` };
      }
      if (options.min !== undefined && parsedNumber < options.min) {
        return { ok: false as const, message: `strategyMatrix[${index}].${options.field} 必须大于等于 ${options.min}。` };
      }
      if (options.max !== undefined && parsedNumber > options.max) {
        return { ok: false as const, message: `strategyMatrix[${index}].${options.field} 必须小于等于 ${options.max}。` };
      }
      return { ok: true as const, value: parsedNumber };
    };
    const minConfidenceResult = numericOrUndefined(record.minConfidence, {
      field: "minConfidence",
      min: 0,
      max: 1,
    });
    if (!minConfidenceResult.ok) {
      return { success: false, message: minConfidenceResult.message };
    }
    const regressionProbabilityResult = numericOrUndefined(
      record.regressionProbabilityAtLeast,
      {
        field: "regressionProbabilityAtLeast",
        min: 0,
        max: 1,
      },
    );
    if (!regressionProbabilityResult.ok) {
      return { success: false, message: regressionProbabilityResult.message };
    }
    const replayRegressionResult = numericOrUndefined(record.replayRegressionAtLeast, {
      field: "replayRegressionAtLeast",
      integer: true,
      min: 0,
    });
    if (!replayRegressionResult.ok) {
      return { success: false, message: replayRegressionResult.message };
    }
    const cooldownResult = numericOrUndefined(record.cooldownMinutes, {
      field: "cooldownMinutes",
      integer: true,
      min: 0,
    });
    if (!cooldownResult.ok) {
      return { success: false, message: cooldownResult.message };
    }
    rules.push({
      id,
      metric,
      severity,
      trendDirection,
      provider: providerResult.value,
      workflow: workflowResult.value,
      projectPattern: projectPatternResult.value,
      minConfidence: minConfidenceResult.value,
      regressionProbabilityAtLeast: regressionProbabilityResult.value,
      replayRegressionAtLeast: replayRegressionResult.value,
      actionType,
      requiresApproval,
      cooldownMinutes: cooldownResult.value,
      reason,
    });
  }
  return { success: true, data: rules };
}

function resolveMcpStaticApprovalConfig(input: {
  mode: McpApprovalMode;
  approvalStagesJson: string;
  approvalStagesJsonTouched: boolean;
  stage1RequiredApprovals: number | undefined;
  stage2RequiredApprovals: number | undefined;
  stage1Roles: string[];
  stage2Roles: string[];
}): { success: true; approvalStages: Array<{
  nodeId?: string;
  stage: McpApprovalStage;
  label?: string;
  requiredApprovals: number;
  roles: string[];
}> } | { success: false; message: string } {
  if (!input.approvalStagesJsonTouched) {
    return {
      success: true,
      approvalStages: buildStaticMcpApprovalStages(
        input.mode,
        input.stage1RequiredApprovals,
        input.stage2RequiredApprovals,
        input.stage1Roles,
        input.stage2Roles,
      ),
    };
  }
  const parsed = parseMcpApprovalStagesJson(input.approvalStagesJson);
  if (!parsed.success) {
    return parsed;
  }
  const expectedStageCount = input.mode === "two_stage" ? 2 : 1;
  if (parsed.data.length !== expectedStageCount) {
    return {
      success: false,
      message: `approvalStages JSON 与 ${input.mode} 阶段数不一致。`,
    };
  }
  return {
    success: true,
    approvalStages: parsed.data,
  };
}

function formatMcpApprovalWorkflowSummary(workflow?: McpApprovalWorkflow): string {
  if (!workflow) {
    return "--";
  }
  const nodes = workflow.nodes
    .filter((node) => node.kind === "approval")
    .map((node) => `${node.stage ?? node.nodeId}:${node.requiredApprovals ?? 1}`);
  return `${workflow.entryNodeId} / ${nodes.join(" -> ")}`;
}

function formatMcpPathHistory(pathHistory?: string[]): string {
  return Array.isArray(pathHistory) && pathHistory.length > 0
    ? pathHistory.join(" -> ")
    : "--";
}

function formatMcpWorkflowCondition(
  condition?: McpApprovalWorkflowCondition,
): string {
  if (!condition) {
    return "--";
  }
  if (condition.default === true) {
    return "default";
  }
  const parts: string[] = [];
  if (condition.riskLevelAtLeast) {
    parts.push(`risk>=${condition.riskLevelAtLeast}`);
  }
  if (condition.toolIds && condition.toolIds.length > 0) {
    parts.push(`tools=${condition.toolIds.join(",")}`);
  }
  if (condition.tenantRoles && condition.tenantRoles.length > 0) {
    parts.push(`roles=${condition.tenantRoles.join(",")}`);
  }
  if (condition.timeWindow) {
    parts.push(
      `time=${condition.timeWindow.timezone} ${condition.timeWindow.startTime}-${condition.timeWindow.endTime}${
        condition.timeWindow.weekdays && condition.timeWindow.weekdays.length > 0
          ? ` [${condition.timeWindow.weekdays.join(",")}]`
          : ""
      }`,
    );
  }
  return parts.length > 0 ? parts.join(" | ") : "--";
}

function formatMcpTransitionPreview(
  preview?: McpApprovalWorkflowTransitionPreview,
): string {
  if (!preview) {
    return "--";
  }
  return `${preview.fromNodeId} -> ${preview.toNodeId ?? "--"} / ${
    preview.matched ? preview.matchedBy ?? "condition" : "unmatched"
  } / ${formatMcpWorkflowCondition(preview.condition)}`;
}

function formatMcpApprovalStages(
  stages:
    | Array<{
        stage?: string;
        requiredApprovals: number;
        roles: string[];
      }>
    | undefined,
): string {
  if (!Array.isArray(stages) || stages.length === 0) {
    return "--";
  }
  return stages
    .map((stage, index) =>
      `${stage.stage ?? `stage${index + 1}`}:${stage.requiredApprovals}(${stage.roles.join(",")})`,
    )
    .join(" | ");
}

function formatRuleScopeBinding(
  scopeBinding: RuleAsset["scopeBinding"],
): string {
  const segments: string[] = [];

  if (
    Array.isArray(scopeBinding.organizations) &&
    scopeBinding.organizations.length > 0
  ) {
    segments.push(`organizations: ${scopeBinding.organizations.join(", ")}`);
  }
  if (
    Array.isArray(scopeBinding.projects) &&
    scopeBinding.projects.length > 0
  ) {
    segments.push(`projects: ${scopeBinding.projects.join(", ")}`);
  }
  if (Array.isArray(scopeBinding.clients) && scopeBinding.clients.length > 0) {
    segments.push(`clients: ${scopeBinding.clients.join(", ")}`);
  }

  return segments.length > 0 ? segments.join(" | ") : "全局";
}

function formatSystemConfigTargetSelectors(
  selectors: SystemConfigPackage["targetSelectors"],
): string {
  const segments: string[] = [];
  if (Array.isArray(selectors.agentIds) && selectors.agentIds.length > 0) {
    segments.push(`agentIds: ${selectors.agentIds.join(", ")}`);
  }
  if (Array.isArray(selectors.deviceIds) && selectors.deviceIds.length > 0) {
    segments.push(`deviceIds: ${selectors.deviceIds.join(", ")}`);
  }
  if (Array.isArray(selectors.channels) && selectors.channels.length > 0) {
    segments.push(`channels: ${selectors.channels.join(", ")}`);
  }
  if (Array.isArray(selectors.hostnames) && selectors.hostnames.length > 0) {
    segments.push(`hostnames: ${selectors.hostnames.join(", ")}`);
  }
  return segments.length > 0 ? segments.join(" | ") : "全量";
}

function formatAgentReleaseArtifactSummary(
  artifact: AgentRelease["artifacts"][number],
): string {
  const segments = [`${artifact.os}/${artifact.arch}`];
  if (artifact.rolloutRing) {
    segments.push(`ring=${artifact.rolloutRing}`);
  }
  if (typeof artifact.rolloutPercentage === "number") {
    segments.push(`pct=${artifact.rolloutPercentage}`);
  }
  if (artifact.minAgentVersion) {
    segments.push(`min=${artifact.minAgentVersion}`);
  }
  if (artifact.signatureAlgorithm) {
    segments.push(`sig=${artifact.signatureAlgorithm}`);
  }
  return segments.join(" | ");
}

type SystemConfigCreateFormSource = {
  packageId: string;
  version: string;
  mode: "loaded" | "clone";
};

function formatRuleRequiredApprovals(
  requiredApprovals: RuleAsset["requiredApprovals"],
): string {
  return `${requiredApprovals === 2 ? 2 : 1} 人`;
}

function formatRuleDiffLineType(type: RuleAssetVersionDiffResponse["lines"][number]["type"]): string {
  switch (type) {
    case "added":
      return "新增";
    case "removed":
      return "删除";
    default:
      return "未变";
  }
}

function isValidHttpUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
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

function getSourceHealthStatus(health: SourceHealth): {
  label: string;
  className: string;
} {
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

function getMetricValue(
  cell: HeatmapCell | undefined,
  metric: MetricKey,
): number {
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

function resolveUsageCost(
  candidate: UsageCostCandidate,
): UsageCostPresentation {
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
    costMode !== null ||
    contractRawCost !== null ||
    contractEstimatedCost !== null;

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
    ? (providedCost ?? (rawCost ?? 0) + (estimatedCost ?? 0))
    : (legacyTotal ??
      (hasSplitCost
        ? (rawCost ?? 0) + (estimatedCost ?? 0)
        : (providedCost ?? 0)));

  return {
    rawCost:
      rawCost ??
      (hasContractFields || hasSplitCost ? null : (providedCost ?? 0)),
    estimatedCost,
    totalCost,
    label:
      formatUsageCostModeLabel(costMode) ??
      normalizeOptionalText(candidate.costLabel ?? candidate.costBasis ?? "") ??
      (hasSplitCost ? "raw + estimated" : "raw"),
  };
}

function calculateChainRatio(current: number, previous: number): number | null {
  if (
    !Number.isFinite(current) ||
    !Number.isFinite(previous) ||
    Math.abs(previous) < 0.000001
  ) {
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

function buildAreaPath(
  points: Array<{ x: number; y: number }>,
  baseY: number,
): string {
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

function mapPricingEntryToForm(
  entry: PricingCatalogEntry,
): PricingEntryFormState {
  return {
    model: entry.model,
    inputPer1k: String(entry.inputPer1k),
    outputPer1k: String(entry.outputPer1k),
    currency: entry.currency ?? "USD",
  };
}

function normalizePricingForm(
  rows: PricingEntryFormState[],
):
  | { success: true; entries: PricingCatalogEntry[] }
  | { success: false; message: string } {
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
  const [loginForm, setLoginForm] =
    useState<LoginFormState>(INITIAL_LOGIN_FORM);
  const [formError, setFormError] = useState<string | null>(null);
  const callbackHandledRef = useRef<string | null>(null);
  const [authCallback, setAuthCallback] = useState<AuthCallbackPayload | null>(
    () => {
      if (typeof window === "undefined") {
        return null;
      }
      return parseAuthCallbackPayload(
        window.location.hash,
        window.location.search,
      );
    },
  );

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
    (provider) => provider.id === "local" && provider.enabled,
  );
  const externalProviders = providers.filter(
    (provider) =>
      provider.id !== "local" &&
      provider.enabled &&
      typeof provider.authorizationUrl === "string" &&
      provider.authorizationUrl.trim().length > 0,
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
        parseAuthCallbackPayload(window.location.hash, window.location.search),
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

    if (
      pending?.state &&
      authCallback.state &&
      pending.state !== authCallback.state
    ) {
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
        codeChallenge,
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

        {providersQuery.isLoading ? (
          <p className="feedback info">正在加载登录方式...</p>
        ) : null}
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
              disabled={
                loginMutation.isPending || externalExchangeMutation.isPending
              }
            >
              {loginMutation.isPending ? "登录中..." : "登录"}
            </button>
          </form>
        ) : (
          <p className="feedback info">
            当前环境未启用本地账号登录，请使用企业登录。
          </p>
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
          <p className="feedback error">
            登录失败：{toErrorMessage(loginMutation.error)}
          </p>
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

        {heatmapQuery.isLoading ? (
          <p className="feedback info">热力图加载中...</p>
        ) : null}
        {heatmapQuery.isError ? (
          <p className="feedback error">热力图加载失败，请稍后重试。</p>
        ) : null}

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
    isDateKey(initialDateKey) ? initialDateKey : todayDateKey(),
  );
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(
    null,
  );
  const [filterForm, setFilterForm] = useState<SessionSearchFilters>(
    EMPTY_SESSION_SEARCH_FILTERS,
  );
  const [appliedFilters, setAppliedFilters] = useState<SessionSearchFilters>(
    EMPTY_SESSION_SEARCH_FILTERS,
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
    [dateKey, normalizedFilters],
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
        signal,
      ),
    initialPageParam: null as string | null,
    getNextPageParam: (lastPage) => lastPage.nextCursor ?? undefined,
    staleTime: 20_000,
  });

  const sessions = useMemo(
    () => sessionsQuery.data?.pages.flatMap((page) => page.items) ?? [],
    [sessionsQuery.data?.pages],
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

    if (
      !selectedSessionId ||
      !sessions.some((item) => item.id === selectedSessionId)
    ) {
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
        signal,
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
    [eventsQuery.data?.pages],
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
                onChange={(event) =>
                  updateFilterField("keyword", event.target.value)
                }
              />
            </label>

            <label className="inline-field" htmlFor="session-client-type">
              客户端类型
              <select
                id="session-client-type"
                value={filterForm.clientType}
                onChange={(event) =>
                  updateFilterField("clientType", event.target.value)
                }
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
                onChange={(event) =>
                  updateFilterField("tool", event.target.value)
                }
              />
            </label>

            <label className="inline-field" htmlFor="session-host">
              主机
              <input
                id="session-host"
                type="text"
                placeholder="例如：devbox-01"
                value={filterForm.host}
                onChange={(event) =>
                  updateFilterField("host", event.target.value)
                }
              />
            </label>

            <label className="inline-field" htmlFor="session-model">
              模型
              <input
                id="session-model"
                type="text"
                placeholder="例如：gpt-5-codex"
                value={filterForm.model}
                onChange={(event) =>
                  updateFilterField("model", event.target.value)
                }
              />
            </label>

            <label className="inline-field" htmlFor="session-project">
              项目
              <input
                id="session-project"
                type="text"
                placeholder="例如：agentledger"
                value={filterForm.project}
                onChange={(event) =>
                  updateFilterField("project", event.target.value)
                }
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

        {sessionsQuery.isLoading ? (
          <p className="feedback info">会话加载中...</p>
        ) : null}
        {sessionsQuery.isError ? (
          <p className="feedback error">
            会话加载失败：{toErrorMessage(sessionsQuery.error)}
          </p>
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
                    className={
                      selectedSessionId === session.id ? "is-selected-row" : ""
                    }
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
          <p>
            {selectedSessionId
              ? `sessionId: ${selectedSessionId}`
              : "请选择会话"}
          </p>
        </header>

        {detailQuery.isLoading ? (
          <p className="feedback info">会话详情加载中...</p>
        ) : null}
        {detailQuery.isError ? (
          <p className="feedback error">
            会话详情加载失败：{toErrorMessage(detailQuery.error)}
          </p>
        ) : null}

        {sessionDetail ? (
          <div className="session-detail-grid">
            <div className="session-detail-card">
              <h3>基础信息</h3>
              <p>工具：{sessionDetail.session?.tool ?? sessionDetail.tool}</p>
              <p>模型：{sessionDetail.session?.model ?? sessionDetail.model}</p>
              <p>
                开始：
                {formatDateTime(
                  sessionDetail.session?.startedAt ?? sessionDetail.startedAt,
                )}
              </p>
              <p>
                结束：
                {(sessionDetail.session?.endedAt ?? sessionDetail.endedAt)
                  ? formatDateTime(
                      sessionDetail.session?.endedAt ??
                        sessionDetail.endedAt ??
                        "",
                    )
                  : "--"}
              </p>
              <p>
                消息数：
                {sessionDetail.session?.messageCount ??
                  sessionDetail.messageCount}
              </p>
            </div>

            <div className="session-detail-card">
              <h3>Token 分解</h3>
              <p>
                input：
                {sessionDetail.tokenBreakdown.inputTokens.toLocaleString(
                  "zh-CN",
                )}
              </p>
              <p>
                output：
                {sessionDetail.tokenBreakdown.outputTokens.toLocaleString(
                  "zh-CN",
                )}
              </p>
              <p>
                cache read：
                {sessionDetail.tokenBreakdown.cacheReadTokens.toLocaleString(
                  "zh-CN",
                )}
              </p>
              <p>
                cache write：
                {sessionDetail.tokenBreakdown.cacheWriteTokens.toLocaleString(
                  "zh-CN",
                )}
              </p>
              <p>
                reasoning：
                {sessionDetail.tokenBreakdown.reasoningTokens.toLocaleString(
                  "zh-CN",
                )}
              </p>
              <p>
                total：
                <strong>
                  {sessionDetail.tokenBreakdown.totalTokens.toLocaleString(
                    "zh-CN",
                  )}
                </strong>
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
          <p>
            {selectedSessionId
              ? `sessionId: ${selectedSessionId}`
              : "请选择会话"}
          </p>
        </header>

        {eventsQuery.isLoading ? (
          <p className="feedback info">事件加载中...</p>
        ) : null}
        {eventsQuery.isError ? (
          <p className="feedback error">
            事件加载失败：{toErrorMessage(eventsQuery.error)}
          </p>
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
    [fromDate, toDate],
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
      left.date.localeCompare(right.date),
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
            : calculateChainRatio(
                currentCost.totalCost,
                previousCost.totalCost,
              ),
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
    [monthlyQuery.data?.items],
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
      const ratio =
        monthlyRows.length === 1 ? 0.5 : index / (monthlyRows.length - 1);
      const x = paddingX + ratio * innerWidth;
      const normalized = isFlatLine
        ? 0.5
        : (row.cost.totalCost - minValue) / span;
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
    [modelsQuery.data?.items],
  );

  const modelTotalCost = useMemo(
    () => modelRows.reduce((sum, row) => sum + row.cost.totalCost, 0),
    [modelRows],
  );

  const sessionRows = useMemo(
    () =>
      (sessionBreakdownQuery.data?.items ?? []).map((item) => ({
        item,
        cost: resolveUsageCost(item),
      })),
    [sessionBreakdownQuery.data?.items],
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

        {!rangeValid ? (
          <p className="feedback error">开始日期不能晚于结束日期。</p>
        ) : null}
      </section>

      <section className="panel">
        <header>
          <h2>日聚合（usage/daily）</h2>
          <p>共 {dailyQuery.data?.total ?? 0} 条</p>
        </header>
        {dailyQuery.isLoading ? (
          <p className="feedback info">daily 加载中...</p>
        ) : null}
        {dailyQuery.isError ? (
          <p className="feedback error">
            daily 加载失败：{toErrorMessage(dailyQuery.error)}
          </p>
        ) : null}

        {latestDaily ? (
          <section className="analytics-kpi-grid" aria-label="daily 环比概览">
            <article className="analytics-kpi-card">
              <h3>最新日 Tokens</h3>
              <strong>{latestDaily.item.tokens.toLocaleString("zh-CN")}</strong>
              <span
                className={`chain-badge ${chainRatioClass(latestDaily.tokensRatio)}`}
              >
                环比 {formatChainRatio(latestDaily.tokensRatio)}
              </span>
            </article>
            <article className="analytics-kpi-card">
              <h3>最新日总成本</h3>
              <strong>${latestDaily.cost.totalCost.toFixed(4)}</strong>
              <span
                className={`chain-badge ${chainRatioClass(latestDaily.totalCostRatio)}`}
              >
                环比 {formatChainRatio(latestDaily.totalCostRatio)}
              </span>
            </article>
            <article className="analytics-kpi-card">
              <h3>最新日 Sessions</h3>
              <strong>
                {latestDaily.item.sessions.toLocaleString("zh-CN")}
              </strong>
              <span
                className={`chain-badge ${chainRatioClass(latestDaily.sessionsRatio)}`}
              >
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
                    <td>
                      {row.cost.rawCost === null
                        ? "--"
                        : `$${row.cost.rawCost.toFixed(4)}`}
                    </td>
                    <td>
                      {row.cost.estimatedCost === null
                        ? "--"
                        : `$${row.cost.estimatedCost.toFixed(4)}`}
                    </td>
                    <td>${row.cost.totalCost.toFixed(4)}</td>
                    <td>
                      <span
                        className={`chain-badge ${chainRatioClass(row.totalCostRatio)}`}
                      >
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
        {monthlyQuery.isLoading ? (
          <p className="feedback info">monthly 加载中...</p>
        ) : null}
        {monthlyQuery.isError ? (
          <p className="feedback error">
            monthly 加载失败：{toErrorMessage(monthlyQuery.error)}
          </p>
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
              区间最小 ${monthlyTrend.minValue.toFixed(4)} / 最大 $
              {monthlyTrend.maxValue.toFixed(4)}
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
                    <td>
                      {row.cost.rawCost === null
                        ? "--"
                        : `$${row.cost.rawCost.toFixed(4)}`}
                    </td>
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
        {modelsQuery.isLoading ? (
          <p className="feedback info">models 加载中...</p>
        ) : null}
        {modelsQuery.isError ? (
          <p className="feedback error">
            models 加载失败：{toErrorMessage(modelsQuery.error)}
          </p>
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
                    <td>
                      {row.cost.rawCost === null
                        ? "--"
                        : `$${row.cost.rawCost.toFixed(4)}`}
                    </td>
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
        {sessionBreakdownQuery.isLoading ? (
          <p className="feedback info">sessions 加载中...</p>
        ) : null}
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
  const [selectedAlertOpsAlertId, setSelectedAlertOpsAlertId] = useState<string | null>(
    null,
  );
  const [alertOpsOnlyFailed, setAlertOpsOnlyFailed] = useState(true);
  const [alertOpsExternalType, setAlertOpsExternalType] = useState<
    "" | "ticket" | "case" | "incident"
  >("");
  const [alertOpsPayload, setAlertOpsPayload] =
    useState<AlertExternalLinkOpsResponse | null>(null);
  const [alertOpsError, setAlertOpsError] = useState<string | null>(null);
  const [alertFailureAlertIdFilter, setAlertFailureAlertIdFilter] = useState("");
  const [alertFailureExternalSystemFilter, setAlertFailureExternalSystemFilter] =
    useState("");
  const [alertFailureSyncStateFilter, setAlertFailureSyncStateFilter] = useState<
    "" | "synced" | "pending" | "failed"
  >("failed");
  const [alertFailureLimit, setAlertFailureLimit] = useState("20");
  const [alertFailurePayload, setAlertFailurePayload] =
    useState<AlertExternalLinkFailureResponse | null>(null);
  const [alertFailureError, setAlertFailureError] = useState<string | null>(null);
  const [failureReportFrom, setFailureReportFrom] = useState("");
  const [failureReportTo, setFailureReportTo] = useState("");
  const [failureReportExternalSystem, setFailureReportExternalSystem] = useState("");
  const [failureReportStage, setFailureReportStage] = useState("");
  const [failureReportActionType, setFailureReportActionType] = useState<
    | ""
    | "retry_requested"
    | "retry_completed"
    | "retry_failed"
    | "dlq_queried"
    | "dlq_replayed"
    | "recovery_job_created"
    | "recovery_job_completed"
    | "recovery_job_failed"
  >("");
  const [failureReportLimit, setFailureReportLimit] = useState("50");
  const [failureReportPayload, setFailureReportPayload] =
    useState<IntegrationAlertFailureReportResponse | null>(null);
  const [failureReportError, setFailureReportError] = useState<string | null>(null);
  const [failureTrendFrom, setFailureTrendFrom] = useState("");
  const [failureTrendTo, setFailureTrendTo] = useState("");
  const [failureTrendExternalSystem, setFailureTrendExternalSystem] = useState("");
  const [failureTrendStage, setFailureTrendStage] = useState("");
  const [failureTrendActionType, setFailureTrendActionType] = useState<
    | ""
    | "retry_requested"
    | "retry_completed"
    | "retry_failed"
    | "dlq_queried"
    | "dlq_replayed"
    | "recovery_job_created"
    | "recovery_job_completed"
    | "recovery_job_failed"
  >("");
  const [failureTrendTop, setFailureTrendTop] = useState("5");
  const [failureTrendPayload, setFailureTrendPayload] =
    useState<IntegrationAlertFailureTrendResponse | null>(null);
  const [failureTrendError, setFailureTrendError] = useState<string | null>(null);
  const [integrationDlqEventTypeFilter, setIntegrationDlqEventTypeFilter] =
    useState("");
  const [integrationDlqChannelFilter, setIntegrationDlqChannelFilter] =
    useState("");
  const [integrationDlqAlertIdFilter, setIntegrationDlqAlertIdFilter] =
    useState("");
  const [integrationDlqCallbackIdFilter, setIntegrationDlqCallbackIdFilter] =
    useState("");
  const [integrationDlqLimit, setIntegrationDlqLimit] = useState("20");
  const [integrationDlqPayload, setIntegrationDlqPayload] =
    useState<IntegrationDlqMessageListResponse | null>(null);
  const [integrationDlqError, setIntegrationDlqError] = useState<string | null>(
    null,
  );
  const [selectedIntegrationDlqMessageIds, setSelectedIntegrationDlqMessageIds] =
    useState<string[]>([]);
  const [integrationDlqRecoveryJobsPayload, setIntegrationDlqRecoveryJobsPayload] =
    useState<IntegrationDlqRecoveryJobListResponse | null>(null);
  const [integrationDlqRecoveryJobDetail, setIntegrationDlqRecoveryJobDetail] =
    useState<IntegrationDlqRecoveryJob | null>(null);
  const [integrationDlqRecoveryJobId, setIntegrationDlqRecoveryJobId] =
    useState("");
  const [integrationDlqRecoveryError, setIntegrationDlqRecoveryError] =
    useState<string | null>(null);
  const [
    orchestrationRuleEventTypeFilter,
    setOrchestrationRuleEventTypeFilter,
  ] = useState<AlertOrchestrationEventType | "">("");
  const [orchestrationRuleEnabledFilter, setOrchestrationRuleEnabledFilter] =
    useState<"" | "true" | "false">("");
  const [orchestrationRuleSeverityFilter, setOrchestrationRuleSeverityFilter] =
    useState<AlertSeverity | "">("");
  const [orchestrationRuleSourceIdFilter, setOrchestrationRuleSourceIdFilter] =
    useState("");
  const [orchestrationRuleId, setOrchestrationRuleId] = useState("");
  const [orchestrationRuleName, setOrchestrationRuleName] = useState("");
  const [orchestrationRuleEnabled, setOrchestrationRuleEnabled] =
    useState(true);
  const [orchestrationRuleEventType, setOrchestrationRuleEventType] =
    useState<AlertOrchestrationEventType>("alert");
  const [orchestrationRuleSeverity, setOrchestrationRuleSeverity] = useState<
    AlertSeverity | ""
  >("");
  const [orchestrationRuleSourceId, setOrchestrationRuleSourceId] =
    useState("");
  const [
    orchestrationRuleDedupeWindowSeconds,
    setOrchestrationRuleDedupeWindowSeconds,
  ] = useState("0");
  const [
    orchestrationRuleSuppressionWindowSeconds,
    setOrchestrationRuleSuppressionWindowSeconds,
  ] = useState("0");
  const [
    orchestrationRuleMergeWindowSeconds,
    setOrchestrationRuleMergeWindowSeconds,
  ] = useState("0");
  const [orchestrationRuleSlaMinutes, setOrchestrationRuleSlaMinutes] =
    useState("");
  const [orchestrationRuleChannelsInput, setOrchestrationRuleChannelsInput] =
    useState("webhook,wecom");
  const [orchestrationSimulateRuleId, setOrchestrationSimulateRuleId] =
    useState("");
  const [orchestrationSimulateEventType, setOrchestrationSimulateEventType] =
    useState<AlertOrchestrationEventType>("alert");
  const [orchestrationSimulateAlertId, setOrchestrationSimulateAlertId] =
    useState("");
  const [orchestrationSimulateSeverity, setOrchestrationSimulateSeverity] =
    useState<AlertSeverity | "">("");
  const [orchestrationSimulateSourceId, setOrchestrationSimulateSourceId] =
    useState("");
  const [orchestrationSimulateDedupeHit, setOrchestrationSimulateDedupeHit] =
    useState(false);
  const [orchestrationSimulateSuppressed, setOrchestrationSimulateSuppressed] =
    useState(false);
  const [
    orchestrationExecutionRuleIdFilter,
    setOrchestrationExecutionRuleIdFilter,
  ] = useState("");
  const [
    orchestrationExecutionEventTypeFilter,
    setOrchestrationExecutionEventTypeFilter,
  ] = useState<AlertOrchestrationEventType | "">("");
  const [
    orchestrationExecutionSeverityFilter,
    setOrchestrationExecutionSeverityFilter,
  ] = useState<AlertSeverity | "">("");
  const [
    orchestrationExecutionSourceIdFilter,
    setOrchestrationExecutionSourceIdFilter,
  ] = useState("");
  const [
    orchestrationExecutionDedupeHitFilter,
    setOrchestrationExecutionDedupeHitFilter,
  ] = useState<"" | "true" | "false">("");
  const [
    orchestrationExecutionSuppressedFilter,
    setOrchestrationExecutionSuppressedFilter,
  ] = useState<"" | "true" | "false">("");
  const [
    orchestrationExecutionDispatchModeFilter,
    setOrchestrationExecutionDispatchModeFilter,
  ] = useState<AlertOrchestrationDispatchMode | "">("");
  const [
    orchestrationExecutionConflictFilter,
    setOrchestrationExecutionConflictFilter,
  ] = useState<"" | "true" | "false">("");
  const [
    orchestrationExecutionSimulatedFilter,
    setOrchestrationExecutionSimulatedFilter,
  ] = useState<"" | "true" | "false">("");
  const [
    orchestrationExecutionEscalatedFilter,
    setOrchestrationExecutionEscalatedFilter,
  ] = useState<"" | "true" | "false">("");
  const [
    orchestrationExecutionEscalationReasonFilter,
    setOrchestrationExecutionEscalationReasonFilter,
  ] = useState<AlertOrchestrationEscalationReason | "">("");
  const [orchestrationExecutionFrom, setOrchestrationExecutionFrom] =
    useState("");
  const [orchestrationExecutionTo, setOrchestrationExecutionTo] = useState("");
  const [orchestrationExecutionLimit, setOrchestrationExecutionLimit] =
    useState("50");
  const [orchestrationRulesPayload, setOrchestrationRulesPayload] = useState<{
    items: AlertOrchestrationRule[];
    total: number;
  } | null>(null);
  const [orchestrationExecutionsPayload, setOrchestrationExecutionsPayload] =
    useState<{ items: AlertOrchestrationExecutionLog[]; total: number } | null>(
      null,
    );
  const [orchestrationSimulationResult, setOrchestrationSimulationResult] =
    useState<AlertOrchestrationSimulationResponse | null>(null);
  const [orchestrationFeedback, setOrchestrationFeedback] = useState<
    string | null
  >(null);
  const [orchestrationError, setOrchestrationError] = useState<string | null>(
    null,
  );
  const [hasLoadedOrchestrationRules, setHasLoadedOrchestrationRules] =
    useState(false);
  const [
    hasLoadedOrchestrationExecutions,
    setHasLoadedOrchestrationExecutions,
  ] = useState(false);

  const [residencyMode, setResidencyMode] =
    useState<DataResidencyMode>("single_region");
  const [primaryRegion, setPrimaryRegion] = useState("");
  const [replicaRegionsInput, setReplicaRegionsInput] = useState("");
  const [allowCrossRegionTransfer, setAllowCrossRegionTransfer] =
    useState(false);
  const [requireTransferApproval, setRequireTransferApproval] = useState(false);
  const [replicationStatusFilter, setReplicationStatusFilter] = useState<
    ReplicationJobStatus | ""
  >("");
  const [replicationSourceRegion, setReplicationSourceRegion] = useState("");
  const [replicationTargetRegion, setReplicationTargetRegion] = useState("");
  const [replicationReason, setReplicationReason] = useState("");
  const [residencyFeedback, setResidencyFeedback] = useState<string | null>(
    null,
  );
  const [residencyError, setResidencyError] = useState<string | null>(null);
  const [residencyKmsDrafts, setResidencyKmsDrafts] = useState<
    ResidencyKmsKeyMapping[]
  >([]);
  const [residencyKmsRegionId, setResidencyKmsRegionId] = useState("");
  const [residencyKmsProvider, setResidencyKmsProvider] = useState("");
  const [residencyKmsKeyRef, setResidencyKmsKeyRef] = useState("");
  const [residencyKmsEnabled, setResidencyKmsEnabled] = useState(true);
  const [residencyKmsFeedback, setResidencyKmsFeedback] = useState<string | null>(
    null,
  );
  const [residencyKmsError, setResidencyKmsError] = useState<string | null>(null);
  const [residencyArchiveDrafts, setResidencyArchiveDrafts] = useState<
    ResidencyArchiveRegionPolicy[]
  >([]);
  const [residencyArchiveSourceRegion, setResidencyArchiveSourceRegion] =
    useState("");
  const [residencyArchiveTargetRegion, setResidencyArchiveTargetRegion] =
    useState("");
  const [residencyArchiveClass, setResidencyArchiveClass] = useState("");
  const [residencyArchiveEnabled, setResidencyArchiveEnabled] = useState(true);
  const [residencyArchiveFeedback, setResidencyArchiveFeedback] = useState<
    string | null
  >(null);
  const [residencyArchiveError, setResidencyArchiveError] = useState<string | null>(
    null,
  );

  const [selectedSystemConfigPackageId, setSelectedSystemConfigPackageId] =
    useState<string | null>(null);
  const [systemConfigCreateVersion, setSystemConfigCreateVersion] = useState("");
  const [systemConfigCreateIssuedAt, setSystemConfigCreateIssuedAt] = useState("");
  const [systemConfigCreateSignatureStatus, setSystemConfigCreateSignatureStatus] =
    useState("unknown");
  const [systemConfigCreateRequiresApproval, setSystemConfigCreateRequiresApproval] =
    useState(false);
  const [systemConfigCreateRequiredApprovals, setSystemConfigCreateRequiredApprovals] =
    useState<SystemConfigPackage["requiredApprovals"]>(0);
  const [systemConfigCreateAgentIds, setSystemConfigCreateAgentIds] = useState("");
  const [systemConfigCreateDeviceIds, setSystemConfigCreateDeviceIds] = useState("");
  const [systemConfigCreateChannels, setSystemConfigCreateChannels] = useState("");
  const [systemConfigCreateHostnames, setSystemConfigCreateHostnames] = useState("");
  const [systemConfigCreatePayloadJson, setSystemConfigCreatePayloadJson] =
    useState("{}");
  const [systemConfigCreateFormSource, setSystemConfigCreateFormSource] =
    useState<SystemConfigCreateFormSource | null>(null);
  const [systemConfigApprovalDecision, setSystemConfigApprovalDecision] =
    useState<SystemConfigPackageApprovalDecision>("approved");
  const [systemConfigApprovalComment, setSystemConfigApprovalComment] =
    useState("");
  const [systemConfigWatchAgentId, setSystemConfigWatchAgentId] = useState("");
  const [systemConfigWatchDeviceId, setSystemConfigWatchDeviceId] = useState("");
  const [systemConfigWatchChannel, setSystemConfigWatchChannel] = useState("");
  const [systemConfigWatchHostname, setSystemConfigWatchHostname] = useState("");
  const [systemConfigWatchResult, setSystemConfigWatchResult] =
    useState<SystemConfigPackage | null>(null);
  const [systemConfigFeedback, setSystemConfigFeedback] = useState<string | null>(
    null,
  );
  const [systemConfigError, setSystemConfigError] = useState<string | null>(null);
  const [hasLoadedSystemConfigWatch, setHasLoadedSystemConfigWatch] =
    useState(false);

  const [agentReleaseChannelFilter, setAgentReleaseChannelFilter] =
    useState<AgentReleaseChannel>("stable");
  const [selectedAgentReleaseId, setSelectedAgentReleaseId] =
    useState<string | null>(null);
  const [agentReleasePreviewCurrentVersion, setAgentReleasePreviewCurrentVersion] =
    useState("");
  const [agentReleasePreviewOs, setAgentReleasePreviewOs] = useState("darwin");
  const [agentReleasePreviewArch, setAgentReleasePreviewArch] = useState("amd64");
  const [agentReleasePreviewAgentId, setAgentReleasePreviewAgentId] = useState("");
  const [agentReleasePreviewDeviceId, setAgentReleasePreviewDeviceId] =
    useState("");
  const [agentReleasePreviewHostname, setAgentReleasePreviewHostname] =
    useState("");
  const [agentReleasePreviewRing, setAgentReleasePreviewRing] =
    useState("stable");
  const [agentReleasePreviewPayload, setAgentReleasePreviewPayload] =
    useState<AgentReleaseCheckPreviewResponse | null>(null);
  const [agentReleaseBatchOs, setAgentReleaseBatchOs] = useState("darwin");
  const [agentReleaseBatchArch, setAgentReleaseBatchArch] = useState("amd64");
  const [agentReleaseBatchSamplesJson, setAgentReleaseBatchSamplesJson] = useState(
    JSON.stringify(
      [
        {
          label: "stable-default",
          currentVersion: "1.0.0",
          agentId: "agent-stable-1",
          deviceId: "device-stable-1",
          hostname: "host-stable-1",
          ring: "stable",
        },
        {
          label: "beta-ring-1",
          currentVersion: "1.0.0",
          agentId: "agent-beta-1",
          deviceId: "device-beta-1",
          hostname: "host-beta-1",
          ring: "beta-ring",
        },
      ],
      null,
      2,
    ),
  );
  const [agentReleaseBatchPreviewPayload, setAgentReleaseBatchPreviewPayload] =
    useState<AgentReleaseCheckBatchPreviewResponse | null>(null);
  const [agentReleaseFeedback, setAgentReleaseFeedback] = useState<string | null>(
    null,
  );
  const [agentReleaseError, setAgentReleaseError] = useState<string | null>(null);
  const [hasLoadedAgentReleasePreview, setHasLoadedAgentReleasePreview] =
    useState(false);

  const [ruleStatusFilter, setRuleStatusFilter] = useState<
    RuleLifecycleStatus | ""
  >("");
  const [ruleKeyword, setRuleKeyword] = useState("");
  const [ruleName, setRuleName] = useState("");
  const [ruleDescription, setRuleDescription] = useState("");
  const [ruleRequiredApprovals, setRuleRequiredApprovals] = useState<1 | 2>(1);
  const [ruleScopeOrganizations, setRuleScopeOrganizations] = useState("");
  const [ruleScopeProjects, setRuleScopeProjects] = useState("");
  const [ruleScopeClients, setRuleScopeClients] = useState("");
  const [selectedRuleAssetId, setSelectedRuleAssetId] = useState<string | null>(
    null,
  );
  const [ruleVersionContent, setRuleVersionContent] = useState("");
  const [ruleVersionChangelog, setRuleVersionChangelog] = useState("");
  const [rulePublishVersion, setRulePublishVersion] = useState("");
  const [ruleRollbackVersion, setRuleRollbackVersion] = useState("");
  const [ruleRollbackReason, setRuleRollbackReason] = useState("");
  const [ruleApprovalVersion, setRuleApprovalVersion] = useState("");
  const [ruleApprovalDecision, setRuleApprovalDecision] =
    useState<RuleApprovalDecision>("approved");
  const [ruleApprovalReason, setRuleApprovalReason] = useState("");
  const [ruleDiffFromVersion, setRuleDiffFromVersion] = useState("");
  const [ruleDiffToVersion, setRuleDiffToVersion] = useState("");
  const [ruleDiffPayload, setRuleDiffPayload] =
    useState<RuleAssetVersionDiffResponse | null>(null);
  const [ruleFeedback, setRuleFeedback] = useState<string | null>(null);
  const [ruleError, setRuleError] = useState<string | null>(null);

  const [tokenPulseRuntimeTraceId, setTokenPulseRuntimeTraceId] = useState("");
  const [tokenPulseRuntimeProviderFilter, setTokenPulseRuntimeProviderFilter] =
    useState("");
  const [tokenPulseRuntimeStatusFilter, setTokenPulseRuntimeStatusFilter] =
    useState<TokenPulseRuntimeEventStatus | "">("");
  const [tokenPulseRuntimePayload, setTokenPulseRuntimePayload] = useState<{
    items: TokenPulseRuntimeEvent[];
    total: number;
  } | null>(null);
  const [tokenPulseRuntimeFeedback, setTokenPulseRuntimeFeedback] =
    useState<string | null>(null);
  const [tokenPulseRuntimeError, setTokenPulseRuntimeError] =
    useState<string | null>(null);
  const [hasLoadedTokenPulseRuntimeEvents, setHasLoadedTokenPulseRuntimeEvents] =
    useState(false);

  const [mcpPolicyKeyword, setMcpPolicyKeyword] = useState("");
  const [mcpPolicyToolId, setMcpPolicyToolId] = useState("");
  const [mcpPolicyRiskLevel, setMcpPolicyRiskLevel] =
    useState<McpRiskLevel>("medium");
  const [mcpPolicyDecision, setMcpPolicyDecision] =
    useState<McpToolDecision>("require_approval");
  const [mcpPolicyApprovalMode, setMcpPolicyApprovalMode] =
    useState<McpApprovalMode>("single_stage");
  const [mcpPolicyStage1RequiredApprovals, setMcpPolicyStage1RequiredApprovals] =
    useState("1");
  const [mcpPolicyStage2RequiredApprovals, setMcpPolicyStage2RequiredApprovals] =
    useState("1");
  const [mcpPolicyStage1Roles, setMcpPolicyStage1Roles] = useState("");
  const [mcpPolicyStage2Roles, setMcpPolicyStage2Roles] = useState("");
  const [mcpPolicyApprovalStagesJson, setMcpPolicyApprovalStagesJson] =
    useState('[{"stage":"stage1","requiredApprovals":1,"roles":["owner","maintainer"]}]');
  const [mcpPolicyApprovalStagesJsonTouched, setMcpPolicyApprovalStagesJsonTouched] =
    useState(false);
  const [mcpWorkflowEntryNodeId, setMcpWorkflowEntryNodeId] = useState(
    createDefaultMcpWorkflowDraftState().entryNodeId,
  );
  const [mcpWorkflowNodes, setMcpWorkflowNodes] = useState<McpWorkflowNodeDraft[]>(
    createDefaultMcpWorkflowDraftState().nodes,
  );
  const [mcpWorkflowTransitions, setMcpWorkflowTransitions] = useState<
    McpWorkflowTransitionDraft[]
  >(createDefaultMcpWorkflowDraftState().transitions);
  const [mcpPolicyConditionRiskLevel, setMcpPolicyConditionRiskLevel] =
    useState<McpRiskLevel | "">("");
  const [mcpPolicyConditionToolIds, setMcpPolicyConditionToolIds] = useState("");
  const [mcpPolicyConditionTenantRoles, setMcpPolicyConditionTenantRoles] =
    useState("");
  const [mcpPolicyReason, setMcpPolicyReason] = useState("");
  const [mcpApprovalStatusFilter, setMcpApprovalStatusFilter] = useState<
    McpApprovalRequest["status"] | ""
  >("");
  const [mcpApprovalToolId, setMcpApprovalToolId] = useState("");
  const [mcpApprovalReason, setMcpApprovalReason] = useState("");
  const [mcpReviewReason, setMcpReviewReason] = useState("");
  const [mcpEvaluateToolId, setMcpEvaluateToolId] = useState("");
  const [mcpEvaluateApprovalRequestId, setMcpEvaluateApprovalRequestId] =
    useState("");
  const [mcpEvaluateReason, setMcpEvaluateReason] = useState("");
  const [mcpEvaluateTimestamp, setMcpEvaluateTimestamp] = useState("");
  const [mcpEvaluateResultPayload, setMcpEvaluateResultPayload] = useState<{
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
    approvalStages?: Array<{
      stage: McpApprovalStage;
      requiredApprovals: number;
      roles: string[];
      approvedApprovals: number;
      approvedByUserIds: string[];
    }>;
    remainingApprovals?: number;
    approvalConditionMatched?: boolean;
  } | null>(null);
  const [mcpInvocationToolId, setMcpInvocationToolId] = useState("");
  const [mcpFeedback, setMcpFeedback] = useState<string | null>(null);
  const [mcpError, setMcpError] = useState<string | null>(null);

  useEffect(() => {
    if (mcpPolicyApprovalMode === "multi_stage" || mcpPolicyApprovalStagesJsonTouched) {
      return;
    }
    setMcpPolicyApprovalStagesJson(
      stringifyStaticMcpApprovalStages(
        mcpPolicyApprovalMode,
        parseOptionalNonNegativeInteger(mcpPolicyStage1RequiredApprovals) ?? 1,
        parseOptionalNonNegativeInteger(mcpPolicyStage2RequiredApprovals) ?? 1,
        parseDistinctCommaSeparatedList(mcpPolicyStage1Roles),
        parseDistinctCommaSeparatedList(mcpPolicyStage2Roles),
      ),
    );
  }, [
    mcpPolicyApprovalMode,
    mcpPolicyApprovalStagesJsonTouched,
    mcpPolicyStage1RequiredApprovals,
    mcpPolicyStage2RequiredApprovals,
    mcpPolicyStage1Roles,
    mcpPolicyStage2Roles,
  ]);

  const replaceMcpWorkflowNodeId = (
    targetNodeId: string,
    nextNodeId: string,
  ) => {
    setMcpWorkflowTransitions((prev) =>
      prev.map((transition) => ({
        ...transition,
        fromNodeId:
          transition.fromNodeId === targetNodeId
            ? nextNodeId
            : transition.fromNodeId,
        toNodeId:
          transition.toNodeId === targetNodeId
            ? nextNodeId
            : transition.toNodeId,
      })),
    );
    setMcpWorkflowEntryNodeId((prev) =>
      prev === targetNodeId ? nextNodeId : prev,
    );
  };

  const removeMcpWorkflowNode = (index: number) => {
    const targetNode = mcpWorkflowNodes[index];
    if (!targetNode) {
      return;
    }
    const remainingNodes = mcpWorkflowNodes.filter((_, itemIndex) => itemIndex !== index);
    setMcpWorkflowNodes(remainingNodes);
    setMcpWorkflowTransitions((prev) =>
      prev.filter(
        (transition) =>
          transition.fromNodeId !== targetNode.nodeId &&
          transition.toNodeId !== targetNode.nodeId,
      ),
    );
    if (mcpWorkflowEntryNodeId === targetNode.nodeId) {
      setMcpWorkflowEntryNodeId(
        remainingNodes[0]?.nodeId ?? createDefaultMcpWorkflowDraftState().entryNodeId,
      );
    }
  };

  const [openApiSummaryPayload, setOpenApiSummaryPayload] =
    useState<OpenPlatformOpenApiSummary | null>(null);
  const [openApiFeedback, setOpenApiFeedback] = useState<string | null>(null);
  const [openApiError, setOpenApiError] = useState<string | null>(null);
  const [hasLoadedOpenApiSummary, setHasLoadedOpenApiSummary] = useState(false);

  const [apiKeyStatusFilter, setApiKeyStatusFilter] = useState<
    OpenPlatformApiKeyStatus | ""
  >("");
  const [apiKeyKeyword, setApiKeyKeyword] = useState("");
  const [apiKeyId, setApiKeyId] = useState("");
  const [apiKeyName, setApiKeyName] = useState("");
  const [apiKeyScopesInput, setApiKeyScopesInput] = useState("");
  const [apiKeyExpiresAt, setApiKeyExpiresAt] = useState("");
  const [apiKeyEnabled, setApiKeyEnabled] = useState(true);
  const [apiKeyRevokeReason, setApiKeyRevokeReason] = useState("");
  const [apiKeyPayload, setApiKeyPayload] = useState<{
    items: OpenPlatformApiKey[];
    total: number;
  } | null>(null);
  const [apiKeyFeedback, setApiKeyFeedback] = useState<string | null>(null);
  const [apiKeyError, setApiKeyError] = useState<string | null>(null);
  const [hasLoadedApiKeys, setHasLoadedApiKeys] = useState(false);

  const [webhookEnabledFilter, setWebhookEnabledFilter] = useState<
    "" | "true" | "false"
  >("");
  const [webhookKeyword, setWebhookKeyword] = useState("");
  const [webhookId, setWebhookId] = useState("");
  const [webhookName, setWebhookName] = useState("");
  const [webhookUrl, setWebhookUrl] = useState("");
  const [webhookEventsInput, setWebhookEventsInput] = useState("");
  const [webhookEnabled, setWebhookEnabled] = useState(true);
  const [webhookReplayEventType, setWebhookReplayEventType] = useState("");
  const [webhookReplayFrom, setWebhookReplayFrom] = useState("");
  const [webhookReplayTo, setWebhookReplayTo] = useState("");
  const [webhookReplayLimit, setWebhookReplayLimit] = useState("100");
  const [webhookReplayDryRun, setWebhookReplayDryRun] = useState(true);
  const [webhookPayload, setWebhookPayload] = useState<{
    items: OpenPlatformWebhook[];
    total: number;
  } | null>(null);
  const [webhookFeedback, setWebhookFeedback] = useState<string | null>(null);
  const [webhookError, setWebhookError] = useState<string | null>(null);
  const [hasLoadedWebhooks, setHasLoadedWebhooks] = useState(false);

  const [qualityDailyDate, setQualityDailyDate] = useState("");
  const [qualityDailyMetric, setQualityDailyMetric] = useState("");
  const [qualityDailyProvider, setQualityDailyProvider] = useState("");
  const [qualityDailyRepo, setQualityDailyRepo] = useState("");
  const [qualityDailyWorkflow, setQualityDailyWorkflow] = useState("");
  const [qualityDailyRunId, setQualityDailyRunId] = useState("");
  const [qualityDailyGroupBy, setQualityDailyGroupBy] = useState<
    "" | "provider" | "repo" | "workflow" | "runId"
  >("");
  const [qualityProjectTrendsFrom, setQualityProjectTrendsFrom] = useState("");
  const [qualityProjectTrendsTo, setQualityProjectTrendsTo] = useState("");
  const [qualityProjectTrendsMetric, setQualityProjectTrendsMetric] =
    useState("");
  const [qualityProjectTrendsProvider, setQualityProjectTrendsProvider] =
    useState("");
  const [qualityProjectTrendsWorkflow, setQualityProjectTrendsWorkflow] =
    useState("");
  const [
    qualityProjectTrendsIncludeUnknown,
    setQualityProjectTrendsIncludeUnknown,
  ] = useState(false);
  const [qualityScorecardTeam, setQualityScorecardTeam] = useState("");
  const [qualityAutomationRiskLevel, setQualityAutomationRiskLevel] =
    useState<McpRiskLevel>("medium");
  const [qualityAutomationDecision, setQualityAutomationDecision] =
    useState<McpToolDecision>("allow");
  const [qualityAutomationReason, setQualityAutomationReason] = useState("");
  const [
    qualityAutomationEvaluationScoreThreshold,
    setQualityAutomationEvaluationScoreThreshold,
  ] = useState("80");
  const [
    qualityAutomationTriggerOnEvaluationFailure,
    setQualityAutomationTriggerOnEvaluationFailure,
  ] = useState(true);
  const [
    qualityAutomationTriggerOnReplayRegression,
    setQualityAutomationTriggerOnReplayRegression,
  ] = useState(true);
  const [qualityAutomationStrategyMatrixJson, setQualityAutomationStrategyMatrixJson] =
    useState("[]");
  const [qualityAutomationSimulationMetric, setQualityAutomationSimulationMetric] =
    useState("accuracy");
  const [qualityAutomationSimulationScore, setQualityAutomationSimulationScore] =
    useState("72");
  const [
    qualityAutomationSimulationTrendDirection,
    setQualityAutomationSimulationTrendDirection,
  ] = useState<"up" | "down" | "flat">("down");
  const [qualityAutomationSimulationConfidence, setQualityAutomationSimulationConfidence] =
    useState("0.65");
  const [
    qualityAutomationSimulationRegressionProbability,
    setQualityAutomationSimulationRegressionProbability,
  ] = useState("0.55");
  const [qualityAutomationSimulationReplayRegressionCount, setQualityAutomationSimulationReplayRegressionCount] =
    useState("2");
  const [qualityAutomationSimulationPayload, setQualityAutomationSimulationPayload] =
    useState<OpenPlatformAutomationPolicySimulationResponse | null>(null);
  const [qualityAdviceActionType, setQualityAdviceActionType] = useState<
    "scorecard_adjustment" | "replay_experiment"
  >("scorecard_adjustment");
  const [qualityAdviceSelectedId, setQualityAdviceSelectedId] = useState("");
  const [qualityAdviceExecuteProject, setQualityAdviceExecuteProject] = useState("");
  const [qualityAdviceExecuteSeverity, setQualityAdviceExecuteSeverity] = useState<
    "info" | "warn" | "critical"
  >("warn");
  const [qualityAdviceExecuteMetric, setQualityAdviceExecuteMetric] = useState("accuracy");
  const [qualityAdviceExecuteDatasetId, setQualityAdviceExecuteDatasetId] = useState("");
  const [qualityAdviceExecuteCandidateLabels, setQualityAdviceExecuteCandidateLabels] =
    useState("");
  const [qualityDailyPayload, setQualityDailyPayload] = useState<{
    items: OpenPlatformQualityDailyItem[];
    total: number;
    groups?: NonNullable<OpenPlatformQualityDailyResponse["groups"]>;
  } | null>(null);
  const [qualityProjectTrendsPayload, setQualityProjectTrendsPayload] =
    useState<{
      items: OpenPlatformQualityProjectTrendItem[];
      total: number;
      summary: OpenPlatformQualityProjectTrendResponse["summary"];
    } | null>(null);
  const [qualityForecastPayload, setQualityForecastPayload] = useState<{
    items: OpenPlatformQualityForecastItem[];
    total: number;
  } | null>(null);
  const [qualityAdvicePayload, setQualityAdvicePayload] = useState<{
    items: OpenPlatformQualityAdviceItem[];
    total: number;
  } | null>(null);
  const [qualityScorecardPayload, setQualityScorecardPayload] = useState<{
    items: OpenPlatformQualityScorecard[];
    total: number;
  } | null>(null);
  const [qualityAdviceExecutionPayload, setQualityAdviceExecutionPayload] = useState<{
    items: OpenPlatformQualityAdviceExecution[];
    total: number;
  } | null>(null);
  const [qualityFeedback, setQualityFeedback] = useState<string | null>(null);
  const [qualityError, setQualityError] = useState<string | null>(null);
  const [hasLoadedQualityDaily, setHasLoadedQualityDaily] = useState(false);
  const [hasLoadedQualityProjectTrends, setHasLoadedQualityProjectTrends] =
    useState(false);
  const [hasLoadedQualityForecast, setHasLoadedQualityForecast] =
    useState(false);
  const [hasLoadedQualityAdvice, setHasLoadedQualityAdvice] = useState(false);
  const [hasLoadedQualityScorecards, setHasLoadedQualityScorecards] =
    useState(false);
  const [hasLoadedQualityAdviceExecutions, setHasLoadedQualityAdviceExecutions] =
    useState(false);

  const [replayCreateDatasetName, setReplayCreateDatasetName] = useState("");
  const [replayCreateDatasetRef, setReplayCreateDatasetRef] = useState("");
  const [replayCreateDatasetModel, setReplayCreateDatasetModel] = useState("");
  const [
    replayCreateDatasetPromptVersion,
    setReplayCreateDatasetPromptVersion,
  ] = useState("");
  const [replayCreateDatasetSampleCount, setReplayCreateDatasetSampleCount] =
    useState("50");
  const [replayDatasetKeyword, setReplayDatasetKeyword] = useState("");
  const [replayVersionDatasetId, setReplayVersionDatasetId] = useState("");
  const [replayCreateVersionDatasetRef, setReplayCreateVersionDatasetRef] =
    useState("");
  const [replayCreateVersionModel, setReplayCreateVersionModel] = useState("");
  const [replayCreateVersionPromptVersion, setReplayCreateVersionPromptVersion] =
    useState("");
  const [replayCreateVersionSampleCount, setReplayCreateVersionSampleCount] =
    useState("50");
  const [replayCreateVersionNote, setReplayCreateVersionNote] = useState("");
  const [replayCreateRunDatasetId, setReplayCreateRunDatasetId] = useState("");
  const [replayCreateRunCandidateLabel, setReplayCreateRunCandidateLabel] =
    useState("");
  const [replayCreateRunSampleLimit, setReplayCreateRunSampleLimit] =
    useState("50");
  const [replayCreateRunBaselineVersionId, setReplayCreateRunBaselineVersionId] =
    useState("");
  const [replayDatasetCasesDatasetId, setReplayDatasetCasesDatasetId] =
    useState("");
  const [replayDatasetCasesEditor, setReplayDatasetCasesEditor] = useState("");
  const [replayMaterializeSessionIds, setReplayMaterializeSessionIds] =
    useState("");
  const [replayMaterializeKeyword, setReplayMaterializeKeyword] = useState("");
  const [replayMaterializeTool, setReplayMaterializeTool] = useState("");
  const [replayMaterializeModel, setReplayMaterializeModel] = useState("");
  const [replayMaterializeFrom, setReplayMaterializeFrom] = useState("");
  const [replayMaterializeTo, setReplayMaterializeTo] = useState("");
  const [replayMaterializeSampleLimit, setReplayMaterializeSampleLimit] =
    useState("20");
  const [replayMaterializeSanitized, setReplayMaterializeSanitized] =
    useState(true);
  const [replayRunsDatasetIdFilter, setReplayRunsDatasetIdFilter] =
    useState("");
  const [replayRunsStatusFilter, setReplayRunsStatusFilter] = useState<
    OpenPlatformReplayJobStatus | ""
  >("");
  const [replayDiffDatasetId, setReplayDiffDatasetId] = useState("");
  const [replayDiffRunId, setReplayDiffRunId] = useState("");
  const [replayDiffKeyword, setReplayDiffKeyword] = useState("");
  const [replayArtifactRunId, setReplayArtifactRunId] = useState("");
  const [replayDatasetPayload, setReplayDatasetPayload] = useState<{
    items: OpenPlatformReplayDataset[];
    total: number;
  } | null>(null);
  const [replayDatasetVersionPayload, setReplayDatasetVersionPayload] = useState<{
    datasetId: string;
    items: OpenPlatformReplayDatasetVersion[];
    total: number;
    currentVersionId?: string | null;
    currentVersionNumber?: number | null;
  } | null>(null);
  const [replayDatasetCasesPayload, setReplayDatasetCasesPayload] = useState<{
    datasetId: string;
    items: OpenPlatformReplayDatasetCase[];
    total: number;
  } | null>(null);
  const [replayDatasetVersionCasesPayload, setReplayDatasetVersionCasesPayload] =
    useState<{
      datasetId: string;
      versionId: string;
      items: OpenPlatformReplayDatasetCase[];
      total: number;
    } | null>(null);
  const [replayRunPayload, setReplayRunPayload] = useState<{
    items: OpenPlatformReplayRun[];
    total: number;
  } | null>(null);
  const [replayDiffPayload, setReplayDiffPayload] = useState<{
    items: OpenPlatformReplayDiffItem[];
    total: number;
    summary?: Record<string, unknown>;
  } | null>(null);
  const [replayArtifactPayload, setReplayArtifactPayload] = useState<{
    runId: string;
    items: OpenPlatformReplayArtifact[];
    total: number;
  } | null>(null);
  const [replayExperimentArtifactPayload, setReplayExperimentArtifactPayload] =
    useState<{
      experimentId: string;
      datasetId: string;
      items: OpenPlatformReplayArtifact[];
      total: number;
    } | null>(null);
  const [replayExperimentName, setReplayExperimentName] = useState("");
  const [replayExperimentDatasetId, setReplayExperimentDatasetId] = useState("");
  const [replayExperimentBaselineVersionId, setReplayExperimentBaselineVersionId] =
    useState("");
  const [replayExperimentRunIds, setReplayExperimentRunIds] = useState("");
  const [replayExperimentCandidateLabels, setReplayExperimentCandidateLabels] =
    useState("");
  const [replayExperimentAutoRun, setReplayExperimentAutoRun] = useState(true);
  const [replayExperimentCompareIds, setReplayExperimentCompareIds] = useState("");
  const [replayExperimentPayload, setReplayExperimentPayload] = useState<{
    items: OpenPlatformReplayExperiment[];
    total: number;
  } | null>(null);
  const [replayExperimentDetailPayload, setReplayExperimentDetailPayload] =
    useState<OpenPlatformReplayExperiment | null>(null);
  const [replayExperimentComparePayload, setReplayExperimentComparePayload] =
    useState<OpenPlatformReplayExperimentCompareResponse | null>(null);
  const [replayExperimentBatchComparePayload, setReplayExperimentBatchComparePayload] =
    useState<OpenPlatformReplayExperimentBatchCompareResponse | null>(null);
  const [replayExperimentWorkflowPayload, setReplayExperimentWorkflowPayload] =
    useState<OpenPlatformReplayExperimentWorkflowResponse | null>(null);
  const [hasLoadedReplayExperiments, setHasLoadedReplayExperiments] =
    useState(false);
  const [replayMaterializePayload, setReplayMaterializePayload] =
    useState<OpenPlatformReplayDatasetMaterializeResponse | null>(null);
  const [replayFeedback, setReplayFeedback] = useState<string | null>(null);
  const [replayError, setReplayError] = useState<string | null>(null);
  const [hasLoadedReplayDatasets, setHasLoadedReplayDatasets] = useState(false);
  const [hasLoadedReplayDatasetVersions, setHasLoadedReplayDatasetVersions] =
    useState(false);
  const [hasLoadedReplayDatasetCases, setHasLoadedReplayDatasetCases] =
    useState(false);
  const [hasLoadedReplayDatasetVersionCases, setHasLoadedReplayDatasetVersionCases] =
    useState(false);
  const [hasLoadedReplayJobs, setHasLoadedReplayJobs] = useState(false);
  const [hasLoadedReplayDiff, setHasLoadedReplayDiff] = useState(false);
  const [hasLoadedReplayArtifacts, setHasLoadedReplayArtifacts] =
    useState(false);
  const [hasLoadedReplayExperimentArtifacts, setHasLoadedReplayExperimentArtifacts] =
    useState(false);

  const [sessionExportFormat, setSessionExportFormat] =
    useState<ExportFormat>("csv");
  const [usageExportFormat, setUsageExportFormat] =
    useState<ExportFormat>("csv");
  const [usageExportDimension, setUsageExportDimension] =
    useState<UsageExportDimension>("daily");
  const [exportFeedback, setExportFeedback] = useState<string | null>(null);
  const [exportError, setExportError] = useState<string | null>(null);
  const hasInitializedResidencyForm = useRef(false);
  const lastHydratedResidencyPolicySignature = useRef<string | null>(null);
  const hasRetriedResidencyPolicyAfterError = useRef(false);
  const hasInitializedResidencyKmsDrafts = useRef(false);
  const hasInitializedResidencyArchiveDrafts = useRef(false);
  const previousRuleAssetIdRef = useRef<string | null>(null);

  const alertQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(statusFilter ? { status: statusFilter } : {}),
      ...(severityFilter ? { severity: severityFilter } : {}),
    }),
    [severityFilter, statusFilter],
  );

  const orchestrationRuleQueryInput = useMemo<AlertOrchestrationRuleListInput>(
    () => ({
      ...(orchestrationRuleEventTypeFilter
        ? { eventType: orchestrationRuleEventTypeFilter }
        : {}),
      ...(typeof parseBooleanSelect(orchestrationRuleEnabledFilter) ===
      "boolean"
        ? { enabled: parseBooleanSelect(orchestrationRuleEnabledFilter) }
        : {}),
      ...(orchestrationRuleSeverityFilter
        ? { severity: orchestrationRuleSeverityFilter }
        : {}),
      ...(orchestrationRuleSourceIdFilter.trim().length > 0
        ? { sourceId: orchestrationRuleSourceIdFilter.trim() }
        : {}),
    }),
    [
      orchestrationRuleEnabledFilter,
      orchestrationRuleEventTypeFilter,
      orchestrationRuleSeverityFilter,
      orchestrationRuleSourceIdFilter,
    ],
  );

  const orchestrationExecutionQueryInput =
    useMemo<AlertOrchestrationExecutionListInput>(
      () => ({
        ...(orchestrationExecutionRuleIdFilter.trim().length > 0
          ? { ruleId: orchestrationExecutionRuleIdFilter.trim() }
          : {}),
        ...(orchestrationExecutionEventTypeFilter
          ? { eventType: orchestrationExecutionEventTypeFilter }
          : {}),
        ...(orchestrationExecutionSeverityFilter
          ? { severity: orchestrationExecutionSeverityFilter }
          : {}),
        ...(orchestrationExecutionSourceIdFilter.trim().length > 0
          ? { sourceId: orchestrationExecutionSourceIdFilter.trim() }
          : {}),
        ...(typeof parseBooleanSelect(orchestrationExecutionDedupeHitFilter) ===
        "boolean"
          ? {
              dedupeHit: parseBooleanSelect(
                orchestrationExecutionDedupeHitFilter,
              ),
            }
          : {}),
        ...(typeof parseBooleanSelect(
          orchestrationExecutionSuppressedFilter,
        ) === "boolean"
          ? {
              suppressed: parseBooleanSelect(
                orchestrationExecutionSuppressedFilter,
              ),
            }
          : {}),
        ...(orchestrationExecutionDispatchModeFilter
          ? { dispatchMode: orchestrationExecutionDispatchModeFilter }
          : {}),
        ...(typeof parseBooleanSelect(orchestrationExecutionConflictFilter) ===
        "boolean"
          ? {
              hasConflict: parseBooleanSelect(
                orchestrationExecutionConflictFilter,
              ),
            }
          : {}),
        ...(typeof parseBooleanSelect(orchestrationExecutionSimulatedFilter) ===
        "boolean"
          ? {
              simulated: parseBooleanSelect(
                orchestrationExecutionSimulatedFilter,
              ),
            }
          : {}),
        ...(typeof parseBooleanSelect(orchestrationExecutionEscalatedFilter) ===
        "boolean"
          ? {
              escalated: parseBooleanSelect(
                orchestrationExecutionEscalatedFilter,
              ),
            }
          : {}),
        ...(orchestrationExecutionEscalationReasonFilter
          ? {
              escalationReason: orchestrationExecutionEscalationReasonFilter,
            }
          : {}),
        ...(orchestrationExecutionFrom.trim().length > 0
          ? { from: orchestrationExecutionFrom.trim() }
          : {}),
        ...(orchestrationExecutionTo.trim().length > 0
          ? { to: orchestrationExecutionTo.trim() }
          : {}),
        ...(typeof parseOptionalNonNegativeInteger(
          orchestrationExecutionLimit,
        ) === "number"
          ? {
              limit: parseOptionalNonNegativeInteger(
                orchestrationExecutionLimit,
              ),
            }
          : {}),
      }),
      [
        orchestrationExecutionConflictFilter,
        orchestrationExecutionDispatchModeFilter,
        orchestrationExecutionDedupeHitFilter,
        orchestrationExecutionEscalatedFilter,
        orchestrationExecutionEscalationReasonFilter,
        orchestrationExecutionEventTypeFilter,
        orchestrationExecutionFrom,
        orchestrationExecutionLimit,
        orchestrationExecutionRuleIdFilter,
        orchestrationExecutionSeverityFilter,
        orchestrationExecutionSimulatedFilter,
        orchestrationExecutionSourceIdFilter,
        orchestrationExecutionSuppressedFilter,
        orchestrationExecutionTo,
      ],
    );

  const replicationJobQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(replicationStatusFilter ? { status: replicationStatusFilter } : {}),
    }),
    [replicationStatusFilter],
  );

  const ruleAssetQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(ruleStatusFilter ? { status: ruleStatusFilter } : {}),
      ...(ruleKeyword.trim().length > 0 ? { keyword: ruleKeyword.trim() } : {}),
    }),
    [ruleKeyword, ruleStatusFilter],
  );

  const mcpPolicyQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(mcpPolicyKeyword.trim().length > 0
        ? { keyword: mcpPolicyKeyword.trim() }
        : {}),
    }),
    [mcpPolicyKeyword],
  );

  const mcpApprovalQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(mcpApprovalStatusFilter ? { status: mcpApprovalStatusFilter } : {}),
    }),
    [mcpApprovalStatusFilter],
  );

  const mcpInvocationQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(mcpInvocationToolId.trim().length > 0
        ? { toolId: mcpInvocationToolId.trim() }
        : {}),
    }),
    [mcpInvocationToolId],
  );

  const apiKeyQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(apiKeyStatusFilter ? { status: apiKeyStatusFilter } : {}),
      ...(apiKeyKeyword.trim().length > 0
        ? { keyword: apiKeyKeyword.trim() }
        : {}),
    }),
    [apiKeyKeyword, apiKeyStatusFilter],
  );

  const webhookQueryInput = useMemo(
    () => ({
      limit: 50,
      ...(typeof parseBooleanSelect(webhookEnabledFilter) === "boolean"
        ? { enabled: parseBooleanSelect(webhookEnabledFilter) }
        : {}),
      ...(webhookKeyword.trim().length > 0
        ? { keyword: webhookKeyword.trim() }
        : {}),
    }),
    [webhookEnabledFilter, webhookKeyword],
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
        signal,
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
    retry: 1,
    retryDelay: 200,
  });

  const residencyKmsQuery = useQuery({
    queryKey: ["residency", "kms-key-mappings"],
    queryFn: ({ signal }) => fetchResidencyKmsKeyMappings(signal),
    staleTime: 20_000,
  });

  const residencyArchiveQuery = useQuery({
    queryKey: ["residency", "archive-region-policies"],
    queryFn: ({ signal }) => fetchResidencyArchiveRegionPolicies(signal),
    staleTime: 20_000,
  });

  const replicationJobsQuery = useQuery({
    queryKey: ["residency", "jobs", replicationJobQueryInput],
    queryFn: ({ signal }) =>
      fetchReplicationJobs(replicationJobQueryInput, signal),
    staleTime: 20_000,
  });

  const systemConfigPackagesQuery = useQuery({
    queryKey: ["system-config", "packages"],
    queryFn: ({ signal }) => fetchSystemConfigPackages(50, signal),
    staleTime: 20_000,
  });

  const systemConfigPackageApprovalsQuery = useQuery({
    queryKey: ["system-config", "packages", selectedSystemConfigPackageId, "approvals"],
    enabled: Boolean(selectedSystemConfigPackageId),
    queryFn: ({ signal }) =>
      fetchSystemConfigPackageApprovals(selectedSystemConfigPackageId!, signal),
    staleTime: 20_000,
  });

  const agentReleasesQuery = useQuery({
    queryKey: ["agent-releases", agentReleaseChannelFilter],
    queryFn: ({ signal }) =>
      fetchAgentReleases({ channel: agentReleaseChannelFilter, limit: 50 }, signal),
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
    queryFn: ({ signal }) =>
      fetchRuleAssetVersions(selectedRuleAssetId!, 50, signal),
    staleTime: 20_000,
  });

  const ruleApprovalsQuery = useQuery({
    queryKey: ["rules", "assets", selectedRuleAssetId, "approvals"],
    enabled: Boolean(selectedRuleAssetId),
    queryFn: ({ signal }) =>
      fetchRuleApprovals(selectedRuleAssetId!, { limit: 50 }, signal),
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
    queryFn: ({ signal }) =>
      fetchMcpInvocations(mcpInvocationQueryInput, signal),
    staleTime: 20_000,
  });

  const qualityAutomationPolicyQuery = useQuery({
    queryKey: ["quality", "automation-policy"],
    queryFn: ({ signal }) => fetchOpenPlatformAutomationPolicy(signal),
    staleTime: 20_000,
  });

  const qualityAutomationExecutionsQuery = useQuery({
    queryKey: ["quality", "automation-executions"],
    queryFn: ({ signal }) =>
      fetchMcpInvocations(
        {
          toolId: OPEN_PLATFORM_QUALITY_AUTOMATION_TOOL_ID,
          limit: 20,
        },
        signal,
      ),
    staleTime: 20_000,
  });

  useEffect(() => {
    const policy = qualityAutomationPolicyQuery.data;
    if (!policy) {
      return;
    }
    setQualityAutomationRiskLevel(policy.riskLevel);
    setQualityAutomationDecision(policy.decision);
    setQualityAutomationReason(policy.reason ?? "");
    setQualityAutomationEvaluationScoreThreshold(
      String(policy.evaluationScoreThreshold),
    );
    setQualityAutomationTriggerOnEvaluationFailure(
      policy.triggerOnEvaluationFailure,
    );
    setQualityAutomationTriggerOnReplayRegression(
      policy.triggerOnReplayRegression,
    );
    setQualityAutomationStrategyMatrixJson(
      JSON.stringify(policy.strategyMatrix ?? [], null, 2),
    );
  }, [qualityAutomationPolicyQuery.data]);

  useEffect(() => {
    if (
      residencyPolicyQuery.isError &&
      !residencyPolicyQuery.isLoading &&
      !hasRetriedResidencyPolicyAfterError.current
    ) {
      hasRetriedResidencyPolicyAfterError.current = true;
      void residencyPolicyQuery.refetch();
      return;
    }
    if (!residencyPolicyQuery.isError) {
      hasRetriedResidencyPolicyAfterError.current = false;
    }
  }, [
    residencyPolicyQuery.isError,
    residencyPolicyQuery.isLoading,
    residencyPolicyQuery.refetch,
  ]);

  useEffect(() => {
    if (residencyRegionsQuery.isLoading || residencyPolicyQuery.isLoading) {
      return;
    }
    const regions = residencyRegionsQuery.data?.items ?? [];
    const policy = residencyPolicyQuery.data;

    if (policy) {
      const signature = buildResidencyPolicyHydrationSignature(policy);
      if (lastHydratedResidencyPolicySignature.current === signature) {
        return;
      }
      setResidencyMode(policy.mode);
      setPrimaryRegion(policy.primaryRegion);
      setReplicaRegionsInput(policy.replicaRegions.join(", "));
      setAllowCrossRegionTransfer(policy.allowCrossRegionTransfer);
      setRequireTransferApproval(policy.requireTransferApproval);
      setReplicationSourceRegion(policy.primaryRegion);
      setReplicationTargetRegion(policy.replicaRegions[0] ?? "");
      lastHydratedResidencyPolicySignature.current = signature;
      hasInitializedResidencyForm.current = true;
      return;
    }

    if (hasInitializedResidencyForm.current) {
      return;
    }
    if (residencyRegionsQuery.isError || residencyPolicyQuery.isError) {
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
    if (hasInitializedResidencyKmsDrafts.current) {
      return;
    }
    if (residencyKmsQuery.isLoading || residencyKmsQuery.isError) {
      return;
    }
    setResidencyKmsDrafts(residencyKmsQuery.data?.items ?? []);
    hasInitializedResidencyKmsDrafts.current = true;
  }, [residencyKmsQuery.data, residencyKmsQuery.isError, residencyKmsQuery.isLoading]);

  useEffect(() => {
    if (hasInitializedResidencyArchiveDrafts.current) {
      return;
    }
    if (residencyArchiveQuery.isLoading || residencyArchiveQuery.isError) {
      return;
    }
    setResidencyArchiveDrafts(residencyArchiveQuery.data?.items ?? []);
    hasInitializedResidencyArchiveDrafts.current = true;
  }, [
    residencyArchiveQuery.data,
    residencyArchiveQuery.isError,
    residencyArchiveQuery.isLoading,
  ]);

  useEffect(() => {
    const items = systemConfigPackagesQuery.data?.items ?? [];
    if (items.length === 0) {
      setSelectedSystemConfigPackageId(null);
      return;
    }
    if (
      !selectedSystemConfigPackageId ||
      !items.some((item) => item.packageId === selectedSystemConfigPackageId)
    ) {
      setSelectedSystemConfigPackageId(items[0]?.packageId ?? null);
    }
  }, [systemConfigPackagesQuery.data, selectedSystemConfigPackageId]);

  useEffect(() => {
    const items = agentReleasesQuery.data?.items ?? [];
    if (items.length === 0) {
      setSelectedAgentReleaseId(null);
      return;
    }
    if (
      !selectedAgentReleaseId ||
      !items.some((item) => item.releaseId === selectedAgentReleaseId)
    ) {
      setSelectedAgentReleaseId(items[0]?.releaseId ?? null);
    }
  }, [agentReleasesQuery.data, selectedAgentReleaseId]);

  useEffect(() => {
    const assets = ruleAssetsQuery.data?.items ?? [];
    if (assets.length === 0) {
      previousRuleAssetIdRef.current = null;
      setSelectedRuleAssetId(null);
      setRulePublishVersion("");
      setRuleRollbackVersion("");
      setRuleApprovalVersion("");
      setRuleDiffFromVersion("");
      setRuleDiffToVersion("");
      setRuleDiffPayload(null);
      return;
    }
    if (
      !selectedRuleAssetId ||
      !assets.some((asset) => asset.id === selectedRuleAssetId)
    ) {
      const nextSelectedAsset = assets[0];
      if (!nextSelectedAsset) {
        return;
      }
      const latestVersionText =
        nextSelectedAsset.latestVersion > 0
          ? String(nextSelectedAsset.latestVersion)
          : "";
      const defaultDiffFromVersion =
        nextSelectedAsset.latestVersion > 1
          ? String(nextSelectedAsset.latestVersion - 1)
          : "";
      const defaultDiffToVersion =
        nextSelectedAsset.latestVersion > 1 ? latestVersionText : "";
      previousRuleAssetIdRef.current = nextSelectedAsset.id;
      setSelectedRuleAssetId(nextSelectedAsset.id);
      setRulePublishVersion(latestVersionText);
      setRuleRollbackVersion(latestVersionText);
      setRuleApprovalVersion(latestVersionText);
      setRuleDiffFromVersion(defaultDiffFromVersion);
      setRuleDiffToVersion(defaultDiffToVersion);
      setRuleDiffPayload(null);
    }
  }, [ruleAssetsQuery.data, selectedRuleAssetId]);

  useEffect(() => {
    if (!selectedRuleAssetId) {
      previousRuleAssetIdRef.current = null;
      setRulePublishVersion("");
      setRuleRollbackVersion("");
      setRuleApprovalVersion("");
      setRuleDiffFromVersion("");
      setRuleDiffToVersion("");
      setRuleDiffPayload(null);
      return;
    }
    const assets = ruleAssetsQuery.data?.items ?? [];
    const selected = assets.find((asset) => asset.id === selectedRuleAssetId);
    if (!selected) {
      previousRuleAssetIdRef.current = null;
      setRulePublishVersion("");
      setRuleRollbackVersion("");
      setRuleApprovalVersion("");
      setRuleDiffFromVersion("");
      setRuleDiffToVersion("");
      setRuleDiffPayload(null);
      return;
    }
    if (selected.latestVersion < 1) {
      previousRuleAssetIdRef.current = selectedRuleAssetId;
      setRulePublishVersion("");
      setRuleRollbackVersion("");
      setRuleApprovalVersion("");
      setRuleDiffFromVersion("");
      setRuleDiffToVersion("");
      setRuleDiffPayload(null);
      return;
    }
    const latestVersionText = String(selected.latestVersion);
    const defaultDiffFromVersion =
      selected.latestVersion > 1 ? String(selected.latestVersion - 1) : "";
    const defaultDiffToVersion =
      selected.latestVersion > 1 ? latestVersionText : "";
    const switchedAsset =
      previousRuleAssetIdRef.current !== selectedRuleAssetId;
    previousRuleAssetIdRef.current = selectedRuleAssetId;
    if (switchedAsset) {
      setRulePublishVersion(latestVersionText);
      setRuleRollbackVersion(latestVersionText);
      setRuleApprovalVersion(latestVersionText);
      setRuleDiffFromVersion(defaultDiffFromVersion);
      setRuleDiffToVersion(defaultDiffToVersion);
      setRuleDiffPayload(null);
      return;
    }
    setRulePublishVersion((prev) =>
      prev.trim().length > 0 ? prev : latestVersionText,
    );
    setRuleRollbackVersion((prev) =>
      prev.trim().length > 0 ? prev : latestVersionText,
    );
    setRuleApprovalVersion((prev) =>
      prev.trim().length > 0 ? prev : latestVersionText,
    );
    setRuleDiffFromVersion((prev) =>
      prev.trim().length > 0 ? prev : defaultDiffFromVersion,
    );
    setRuleDiffToVersion((prev) =>
      prev.trim().length > 0 ? prev : defaultDiffToVersion,
    );
  }, [ruleAssetsQuery.data, selectedRuleAssetId]);

  const updateAlertStatusMutation = useMutation({
    mutationFn: ({
      alertId,
      status,
    }: {
      alertId: string;
      status: AlertMutableStatus;
    }) => updateAlertStatus(alertId, status),
    onSuccess: async (alert) => {
      setAlertFeedback(`告警 ${alert.id} 已更新为 ${alert.status}。`);
      await queryClient.invalidateQueries({ queryKey: ["alerts"] });
    },
  });

  const retryAlertExternalLinkSyncMutation = useMutation({
    mutationFn: ({
      alertId,
      externalType,
      externalId,
    }: {
      alertId: string;
      externalType: "ticket" | "case" | "incident";
      externalId: string;
    }) =>
      retryAlertExternalLinkSync(alertId, {
        externalType,
        externalId,
      }),
    onSuccess: async (alert, variables) => {
      setAlertFeedback(
        `告警 ${alert.id} 的外部联动 ${variables.externalType}:${variables.externalId} 已重新触发同步。`,
      );
      await queryClient.invalidateQueries({ queryKey: ["alerts"] });
      if (selectedAlertOpsAlertId === alert.id) {
        void loadAlertExternalLinkOpsMutation.mutateAsync({
          alertId: alert.id,
          externalType: alertOpsExternalType || undefined,
          onlyFailed: alertOpsOnlyFailed,
        });
      }
      if (alertFailurePayload) {
        void loadAlertExternalLinkFailuresMutation.mutateAsync({
          alertId: alertFailureAlertIdFilter.trim() || undefined,
          externalSystem: alertFailureExternalSystemFilter.trim() || undefined,
          syncState: alertFailureSyncStateFilter || undefined,
          limit:
            alertFailureLimit.trim().length > 0
              ? Number(alertFailureLimit)
              : undefined,
        });
      }
      if (integrationDlqPayload) {
        void loadIntegrationDlqMessagesMutation.mutateAsync({
          eventType: integrationDlqEventTypeFilter.trim() || undefined,
          channel: integrationDlqChannelFilter.trim() || undefined,
          alertId: integrationDlqAlertIdFilter.trim() || undefined,
          callbackId: integrationDlqCallbackIdFilter.trim() || undefined,
          limit:
            integrationDlqLimit.trim().length > 0
              ? Number(integrationDlqLimit)
              : undefined,
        });
      }
    },
  });

  const loadAlertExternalLinkOpsMutation = useMutation({
    mutationFn: ({
      alertId,
      externalType,
      onlyFailed,
    }: {
      alertId: string;
      externalType?: "ticket" | "case" | "incident";
      onlyFailed?: boolean;
    }) =>
      fetchAlertExternalLinkOps(alertId, {
        externalType,
        onlyFailed,
      }),
    onSuccess: (payload) => {
      setAlertOpsError(null);
      setAlertOpsPayload(payload);
    },
    onError: (error) => {
      setAlertOpsPayload(null);
      setAlertOpsError(`加载外部联动运维视图失败：${toErrorMessage(error)}`);
    },
  });

  const loadAlertExternalLinkFailuresMutation = useMutation({
    mutationFn: ({
      alertId,
      externalSystem,
      syncState,
      limit,
    }: {
      alertId?: string;
      externalSystem?: string;
      syncState?: "synced" | "pending" | "failed";
      limit?: number;
    }) =>
      fetchAlertExternalLinkFailures({
        alertId,
        externalSystem,
        syncState,
        limit,
      }),
    onSuccess: (payload) => {
      setAlertFailureError(null);
      setAlertFailurePayload(payload);
    },
    onError: (error) => {
      setAlertFailurePayload(null);
      setAlertFailureError(`加载失败外部联动视图失败：${toErrorMessage(error)}`);
    },
  });

  const loadIntegrationAlertFailureReportMutation = useMutation({
    mutationFn: ({
      from,
      to,
      externalSystem,
      stage,
      actionType,
      limit,
    }: {
      from?: string;
      to?: string;
      externalSystem?: string;
      stage?: string;
      actionType?:
        | "retry_requested"
        | "retry_completed"
        | "retry_failed"
        | "dlq_queried"
        | "dlq_replayed"
        | "recovery_job_created"
        | "recovery_job_completed"
        | "recovery_job_failed";
      limit?: number;
    }) =>
      fetchIntegrationAlertFailureReport({
        from,
        to,
        externalSystem,
        stage,
        actionType,
        limit,
      }),
    onSuccess: (payload) => {
      setFailureReportError(null);
      setFailureReportPayload(payload);
    },
    onError: (error) => {
      setFailureReportPayload(null);
      setFailureReportError(`加载失败审计报表失败：${toErrorMessage(error)}`);
    },
  });

  const loadIntegrationAlertFailureTrendMutation = useMutation({
    mutationFn: ({
      from,
      to,
      externalSystem,
      stage,
      actionType,
      top,
    }: {
      from?: string;
      to?: string;
      externalSystem?: string;
      stage?: string;
      actionType?:
        | "retry_requested"
        | "retry_completed"
        | "retry_failed"
        | "dlq_queried"
        | "dlq_replayed"
        | "recovery_job_created"
        | "recovery_job_completed"
        | "recovery_job_failed";
      top?: number;
    }) =>
      fetchIntegrationAlertFailureTrends({
        from,
        to,
        externalSystem,
        stage,
        actionType,
        top,
      }),
    onSuccess: (payload) => {
      setFailureTrendError(null);
      setFailureTrendPayload(payload);
    },
    onError: (error) => {
      setFailureTrendPayload(null);
      setFailureTrendError(`加载长期趋势/容量运维视图失败：${toErrorMessage(error)}`);
    },
  });

  const loadIntegrationDlqMessagesMutation = useMutation({
    mutationFn: ({
      eventType,
      channel,
      alertId,
      callbackId,
      limit,
    }: {
      eventType?: string;
      channel?: string;
      alertId?: string;
      callbackId?: string;
      limit?: number;
    }) =>
      fetchIntegrationDlqMessages({
        eventType,
        channel,
        alertId,
        callbackId,
        limit,
      }),
    onSuccess: (payload) => {
      setIntegrationDlqError(null);
      setIntegrationDlqPayload(payload);
      setSelectedIntegrationDlqMessageIds([]);
    },
    onError: (error) => {
      setIntegrationDlqPayload(null);
      setSelectedIntegrationDlqMessageIds([]);
      setIntegrationDlqError(`加载 Integration DLQ 失败：${toErrorMessage(error)}`);
    },
  });

  const createIntegrationDlqRecoveryJobMutation = useMutation({
    mutationFn: (input: {
      messageIds?: string[];
      filters?: {
        eventType?: string;
        channel?: string;
        callbackId?: string;
        alertId?: string;
        limit?: number;
      };
    }) => createIntegrationDlqRecoveryJob(input),
    onSuccess: async (job) => {
      setAlertFeedback(`已创建恢复批次 ${job.id}。`);
      setIntegrationDlqRecoveryError(null);
      setIntegrationDlqRecoveryJobId(job.id);
      setIntegrationDlqRecoveryJobDetail(job);
      setSelectedIntegrationDlqMessageIds([]);
      if (integrationDlqPayload) {
        void loadIntegrationDlqMessagesMutation.mutateAsync({
          eventType: integrationDlqEventTypeFilter.trim() || undefined,
          channel: integrationDlqChannelFilter.trim() || undefined,
          alertId: integrationDlqAlertIdFilter.trim() || undefined,
          callbackId: integrationDlqCallbackIdFilter.trim() || undefined,
          limit:
            integrationDlqLimit.trim().length > 0
              ? Number(integrationDlqLimit)
              : undefined,
        });
      }
      void loadIntegrationDlqRecoveryJobsMutation.mutateAsync({
        limit: 20,
      });
    },
    onError: (error) => {
      setAlertFeedback(null);
      setIntegrationDlqRecoveryError(
        `创建 Integration DLQ 恢复批次失败：${toErrorMessage(error)}`,
      );
    },
  });

  const loadIntegrationDlqRecoveryJobsMutation = useMutation({
    mutationFn: (input: { status?: "queued" | "running" | "completed" | "failed"; limit?: number }) =>
      fetchIntegrationDlqRecoveryJobs(input),
    onSuccess: (payload) => {
      setIntegrationDlqRecoveryError(null);
      setIntegrationDlqRecoveryJobsPayload(payload);
    },
    onError: (error) => {
      setIntegrationDlqRecoveryError(
        `加载 Integration DLQ 恢复批次失败：${toErrorMessage(error)}`,
      );
    },
  });

  const loadIntegrationDlqRecoveryJobDetailMutation = useMutation({
    mutationFn: (jobId: string) => fetchIntegrationDlqRecoveryJobDetail(jobId),
    onSuccess: (payload) => {
      setIntegrationDlqRecoveryError(null);
      setIntegrationDlqRecoveryJobDetail(payload);
      setIntegrationDlqRecoveryJobId(payload.id);
    },
    onError: (error) => {
      setIntegrationDlqRecoveryJobDetail(null);
      setIntegrationDlqRecoveryError(
        `加载 Integration DLQ 恢复批次详情失败：${toErrorMessage(error)}`,
      );
    },
  });

  const retryAlertExternalLinkSyncBatchMutation = useMutation({
    mutationFn: ({
      alertId,
      externalType,
    }: {
      alertId: string;
      externalType?: "ticket" | "case" | "incident";
    }) =>
      retryAlertExternalLinkSyncBatch(alertId, {
        externalType,
      }),
    onSuccess: async (
      payload: AlertExternalLinkBatchRetryResponse,
      variables,
    ) => {
      setAlertFeedback(
        `告警 ${payload.alertId} 已批量重试 ${payload.retriedCount} 条外部联动。`,
      );
      await queryClient.invalidateQueries({ queryKey: ["alerts"] });
      if (selectedAlertOpsAlertId === payload.alertId) {
        void loadAlertExternalLinkOpsMutation.mutateAsync({
          alertId: payload.alertId,
          externalType: variables.externalType,
          onlyFailed: alertOpsOnlyFailed,
        });
      }
      if (alertFailurePayload) {
        void loadAlertExternalLinkFailuresMutation.mutateAsync({
          alertId: alertFailureAlertIdFilter.trim() || undefined,
          externalSystem: alertFailureExternalSystemFilter.trim() || undefined,
          syncState: alertFailureSyncStateFilter || undefined,
          limit:
            alertFailureLimit.trim().length > 0
              ? Number(alertFailureLimit)
              : undefined,
        });
      }
      if (integrationDlqPayload) {
        void loadIntegrationDlqMessagesMutation.mutateAsync({
          eventType: integrationDlqEventTypeFilter.trim() || undefined,
          channel: integrationDlqChannelFilter.trim() || undefined,
          alertId: integrationDlqAlertIdFilter.trim() || undefined,
          callbackId: integrationDlqCallbackIdFilter.trim() || undefined,
          limit:
            integrationDlqLimit.trim().length > 0
              ? Number(integrationDlqLimit)
              : undefined,
        });
      }
    },
    onError: (error) => {
      setAlertFeedback(null);
      setAlertOpsError(`批量重试失败：${toErrorMessage(error)}`);
    },
  });

  const loadOrchestrationRulesMutation = useMutation({
    mutationFn: (input: AlertOrchestrationRuleListInput) =>
      fetchAlertOrchestrationRules(input),
    onSuccess: (payload) => {
      setOrchestrationError(null);
      setHasLoadedOrchestrationRules(true);
      setOrchestrationRulesPayload({
        items: payload.items,
        total: payload.total,
      });
      setOrchestrationFeedback(`编排规则已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setOrchestrationFeedback(null);
      setHasLoadedOrchestrationRules(true);
      setOrchestrationError(`加载编排规则失败：${toErrorMessage(error)}`);
    },
  });

  const upsertOrchestrationRuleMutation = useMutation({
    mutationFn: ({
      ruleId,
      input,
    }: {
      ruleId: string;
      input: {
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
      };
    }) => upsertAlertOrchestrationRule(ruleId, input),
    onSuccess: async (rule) => {
      setOrchestrationError(null);
      setOrchestrationFeedback(`编排规则 ${rule.id} 已保存。`);
      setOrchestrationRuleId(rule.id);
      setOrchestrationRuleName(rule.name);
      setOrchestrationRuleEnabled(rule.enabled);
      setOrchestrationRuleEventType(rule.eventType);
      setOrchestrationRuleSeverity(rule.severity ?? "");
      setOrchestrationRuleSourceId(rule.sourceId ?? "");
      setOrchestrationRuleDedupeWindowSeconds(String(rule.dedupeWindowSeconds));
      setOrchestrationRuleSuppressionWindowSeconds(
        String(rule.suppressionWindowSeconds),
      );
      setOrchestrationRuleMergeWindowSeconds(String(rule.mergeWindowSeconds));
      setOrchestrationRuleSlaMinutes(
        typeof rule.slaMinutes === "number" ? String(rule.slaMinutes) : "",
      );
      setOrchestrationRuleChannelsInput(rule.channels.join(","));
      try {
        const payload = await fetchAlertOrchestrationRules(
          orchestrationRuleQueryInput,
        );
        setHasLoadedOrchestrationRules(true);
        setOrchestrationRulesPayload({
          items: payload.items,
          total: payload.total,
        });
      } catch (error) {
        setOrchestrationFeedback(
          `编排规则 ${rule.id} 已保存，但规则列表刷新失败：${toErrorMessage(error)}`,
        );
      }
    },
    onError: (error) => {
      setOrchestrationFeedback(null);
      setOrchestrationError(`保存编排规则失败：${toErrorMessage(error)}`);
    },
  });

  const simulateOrchestrationMutation = useMutation({
    mutationFn: (input: AlertOrchestrationSimulateInput) =>
      simulateAlertOrchestration(input),
    onSuccess: (payload) => {
      setOrchestrationError(null);
      setOrchestrationSimulationResult(payload);
      setOrchestrationFeedback(
        `模拟完成：命中 ${payload.matchedRules.length} 条规则，冲突 ${payload.conflictRuleIds.length} 条。`,
      );
    },
    onError: (error) => {
      setOrchestrationFeedback(null);
      setOrchestrationError(`模拟失败：${toErrorMessage(error)}`);
    },
  });

  const loadOrchestrationExecutionsMutation = useMutation({
    mutationFn: (input: AlertOrchestrationExecutionListInput) =>
      fetchAlertOrchestrationExecutions(input),
    onSuccess: (payload) => {
      setOrchestrationError(null);
      setHasLoadedOrchestrationExecutions(true);
      setOrchestrationExecutionsPayload({
        items: payload.items,
        total: payload.total,
      });
      setOrchestrationFeedback(`执行日志已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setOrchestrationFeedback(null);
      setHasLoadedOrchestrationExecutions(true);
      setOrchestrationError(`加载执行日志失败：${toErrorMessage(error)}`);
    },
  });

  const saveResidencyPolicyMutation = useMutation({
    mutationFn: (
      input: Omit<TenantResidencyPolicy, "tenantId" | "updatedAt"> & {
        updatedAt?: string;
      },
    ) => upsertResidencyPolicy(input),
    onSuccess: async (policy) => {
      setResidencyError(null);
      setResidencyFeedback(
        `数据主权策略已保存，主地域：${policy.primaryRegion}`,
      );
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

  const saveResidencyKmsMappingsMutation = useMutation({
    mutationFn: (items: ResidencyKmsKeyMapping[]) =>
      upsertResidencyKmsKeyMappings({
        items: items.map((item) => ({
          regionId: item.regionId,
          keyProvider: item.keyProvider,
          keyRef: item.keyRef,
          enabled: item.enabled,
        })),
      }),
    onSuccess: async (payload) => {
      setResidencyKmsError(null);
      setResidencyKmsFeedback(`KMS 映射已保存，共 ${payload.total} 条。`);
      setResidencyKmsDrafts(payload.items);
      await queryClient.invalidateQueries({
        queryKey: ["residency", "kms-key-mappings"],
      });
    },
    onError: (error) => {
      setResidencyKmsFeedback(null);
      setResidencyKmsError(`保存 KMS 映射失败：${toErrorMessage(error)}`);
    },
  });

  const saveResidencyArchivePoliciesMutation = useMutation({
    mutationFn: (items: ResidencyArchiveRegionPolicy[]) =>
      upsertResidencyArchiveRegionPolicies({
        items: items.map((item) => ({
          sourceRegion: item.sourceRegion,
          archiveRegion: item.archiveRegion,
          archiveClass: item.archiveClass,
          enabled: item.enabled,
        })),
      }),
    onSuccess: async (payload) => {
      setResidencyArchiveError(null);
      setResidencyArchiveFeedback(`区域归档策略已保存，共 ${payload.total} 条。`);
      setResidencyArchiveDrafts(payload.items);
      await queryClient.invalidateQueries({
        queryKey: ["residency", "archive-region-policies"],
      });
    },
    onError: (error) => {
      setResidencyArchiveFeedback(null);
      setResidencyArchiveError(`保存区域归档策略失败：${toErrorMessage(error)}`);
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
      setResidencyFeedback(
        `复制任务 ${job.id} 已创建（${job.sourceRegion} -> ${job.targetRegion}）。`,
      );
      setReplicationReason("");
      await queryClient.invalidateQueries({ queryKey: ["residency", "jobs"] });
    },
    onError: (error) => {
      setResidencyFeedback(null);
      setResidencyError(`创建复制任务失败：${toErrorMessage(error)}`);
    },
  });

  const approveReplicationJobMutation = useMutation({
    mutationFn: ({ jobId, reason }: { jobId: string; reason?: string }) =>
      approveReplicationJob(jobId, reason ? { reason } : undefined),
    onSuccess: async (job) => {
      setResidencyError(null);
      setResidencyFeedback(
        `复制任务 ${job.id} 已审批，当前状态 ${job.status}。`,
      );
      await queryClient.invalidateQueries({ queryKey: ["residency", "jobs"] });
    },
    onError: (error) => {
      setResidencyFeedback(null);
      setResidencyError(`审批复制任务失败：${toErrorMessage(error)}`);
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

  const createSystemConfigPackageMutation = useMutation({
    mutationFn: (input: SystemConfigPackageCreateInput) =>
      createSystemConfigPackage(input),
    onSuccess: async (pkg) => {
      setSystemConfigError(null);
      setSystemConfigFeedback(`配置包 ${pkg.packageId} 已创建。`);
      setSelectedSystemConfigPackageId(pkg.packageId);
      resetSystemConfigCreateForm();
      await queryClient.invalidateQueries({ queryKey: ["system-config", "packages"] });
    },
    onError: (error) => {
      setSystemConfigFeedback(null);
      setSystemConfigError(`创建配置包失败：${toErrorMessage(error)}`);
    },
  });

  const createSystemConfigPackageApprovalMutation = useMutation({
    mutationFn: ({
      packageId,
      decision,
      comment,
    }: {
      packageId: string;
      decision: SystemConfigPackageApprovalDecision;
      comment?: string;
    }) =>
      createSystemConfigPackageApproval(
        packageId,
        { decision, comment },
      ),
    onSuccess: async (approval) => {
      setSystemConfigError(null);
      setSystemConfigFeedback(
        `配置包审批已提交：${approval.version} -> ${approval.decision}。`,
      );
      setSystemConfigApprovalComment("");
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["system-config", "packages"] }),
        queryClient.invalidateQueries({
          queryKey: ["system-config", "packages", approval.packageId, "approvals"],
        }),
      ]);
    },
    onError: (error) => {
      setSystemConfigFeedback(null);
      setSystemConfigError(`提交配置包审批失败：${toErrorMessage(error)}`);
    },
  });

  const publishSystemConfigPackageMutation = useMutation({
    mutationFn: ({ packageId }: { packageId: string }) =>
      publishSystemConfigPackage(packageId),
    onSuccess: async (pkg) => {
      setSystemConfigError(null);
      setSystemConfigFeedback(`配置包 ${pkg.packageId} 已发布。`);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["system-config", "packages"] }),
        queryClient.invalidateQueries({
          queryKey: ["system-config", "packages", pkg.packageId, "approvals"],
        }),
      ]);
    },
    onError: (error) => {
      setSystemConfigFeedback(null);
      setSystemConfigError(`发布配置包失败：${toErrorMessage(error)}`);
    },
  });

  const fetchSystemConfigWatchLatestMutation = useMutation({
    mutationFn: () =>
      fetchSystemConfigWatchLatest({
        agentId: systemConfigWatchAgentId.trim() || undefined,
        deviceId: systemConfigWatchDeviceId.trim() || undefined,
        channel: systemConfigWatchChannel.trim() || undefined,
        hostname: systemConfigWatchHostname.trim() || undefined,
      }),
    onSuccess: (pkg) => {
      setSystemConfigError(null);
      setHasLoadedSystemConfigWatch(true);
      setSystemConfigWatchResult(pkg);
      setSystemConfigFeedback(`watch/latest 已命中 ${pkg.packageId}。`);
    },
    onError: (error) => {
      setSystemConfigFeedback(null);
      setHasLoadedSystemConfigWatch(true);
      setSystemConfigWatchResult(null);
      setSystemConfigError(`watch/latest 查询失败：${toErrorMessage(error)}`);
    },
  });

  const fetchAgentReleaseCheckPreviewMutation = useMutation({
    mutationFn: () =>
      fetchAgentReleaseCheckPreview({
        currentVersion: agentReleasePreviewCurrentVersion.trim(),
        channel: agentReleaseChannelFilter,
        os: agentReleasePreviewOs.trim(),
        arch: agentReleasePreviewArch.trim(),
        agentId: agentReleasePreviewAgentId.trim() || undefined,
        deviceId: agentReleasePreviewDeviceId.trim() || undefined,
        hostname: agentReleasePreviewHostname.trim() || undefined,
        ring: agentReleasePreviewRing.trim() || undefined,
      }),
    onSuccess: (payload) => {
      setAgentReleaseError(null);
      setHasLoadedAgentReleasePreview(true);
      setAgentReleasePreviewPayload(payload);
      setAgentReleaseFeedback(
        payload.selectedArtifact
          ? `升级预览已命中 ${payload.selectedArtifact.fileName ?? payload.latestRelease?.version ?? "--"}。`
          : `升级预览已完成：${payload.selectionReason ?? payload.comparison}。`,
      );
    },
    onError: (error) => {
      setHasLoadedAgentReleasePreview(true);
      setAgentReleasePreviewPayload(null);
      setAgentReleaseFeedback(null);
      setAgentReleaseError(`升级预览失败：${toErrorMessage(error)}`);
    },
  });

  const fetchAgentReleaseCheckBatchPreviewMutation = useMutation({
    mutationFn: (samples: AgentReleaseBatchCheckSampleInput[]) =>
      fetchAgentReleaseCheckBatchPreview({
        channel: agentReleaseChannelFilter,
        os: agentReleaseBatchOs.trim(),
        arch: agentReleaseBatchArch.trim(),
        samples,
      }),
    onSuccess: (payload) => {
      setAgentReleaseError(null);
      setAgentReleaseFeedback(`批量升级模拟已完成，共 ${payload.total} 条。`);
      setAgentReleaseBatchPreviewPayload(payload);
    },
    onError: (error) => {
      setAgentReleaseFeedback(null);
      setAgentReleaseBatchPreviewPayload(null);
      setAgentReleaseError(`批量升级模拟失败：${toErrorMessage(error)}`);
    },
  });

  const createRuleAssetMutation = useMutation({
    mutationFn: ({
      name,
      description,
      requiredApprovals,
      scopeBinding,
    }: {
      name: string;
      description?: string;
      requiredApprovals?: RuleAsset["requiredApprovals"];
      scopeBinding?: RuleAsset["scopeBinding"];
    }) => createRuleAsset({ name, description, requiredApprovals, scopeBinding }),
    onSuccess: async (asset) => {
      setRuleError(null);
      setRuleFeedback(`规则资产 ${asset.name} 已创建。`);
      setRuleName("");
      setRuleDescription("");
      setRuleRequiredApprovals(1);
      setRuleScopeOrganizations("");
      setRuleScopeProjects("");
      setRuleScopeClients("");
      setSelectedRuleAssetId(asset.id);
      const latestVersionText =
        asset.latestVersion > 0 ? String(asset.latestVersion) : "";
      setRulePublishVersion(latestVersionText);
      setRuleRollbackVersion(latestVersionText);
      setRuleApprovalVersion(latestVersionText);
      setRuleDiffFromVersion("");
      setRuleDiffToVersion("");
      setRuleDiffPayload(null);
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
      setRuleDiffFromVersion(version.version > 1 ? String(version.version - 1) : "");
      setRuleDiffToVersion(version.version > 1 ? String(version.version) : "");
      setRuleDiffPayload(null);
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
      setRuleFeedback(
        `规则资产 ${asset.name} 已发布 v${asset.publishedVersion ?? "-"}.`,
      );
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["rules", "assets"] }),
        queryClient.invalidateQueries({
          queryKey: ["rules", "assets", asset.id, "versions"],
        }),
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
      setRuleFeedback(
        `规则资产 ${asset.name} 已回滚到 v${asset.publishedVersion ?? "-"}.`,
      );
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["rules", "assets"] }),
        queryClient.invalidateQueries({
          queryKey: ["rules", "assets", asset.id, "versions"],
        }),
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
      setRuleFeedback(
        `已提交审批：v${approval.version} -> ${approval.decision}。`,
      );
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

  const fetchRuleVersionDiffMutation = useMutation({
    mutationFn: ({
      assetId,
      fromVersion,
      toVersion,
    }: {
      assetId: string;
      fromVersion: number;
      toVersion: number;
    }) => fetchRuleAssetVersionDiff(assetId, { fromVersion, toVersion }),
    onSuccess: (diff) => {
      setRuleError(null);
      setRuleDiffPayload(diff);
    },
    onError: (error) => {
      setRuleFeedback(null);
      setRuleDiffPayload(null);
      setRuleError(`加载版本 diff 失败：${toErrorMessage(error)}`);
    },
  });

  const loadTokenPulseRuntimeEventsMutation = useMutation({
    mutationFn: ({
      traceId,
      provider,
      status,
    }: {
      traceId: string;
      provider?: string;
      status?: TokenPulseRuntimeEventStatus;
    }) =>
      fetchTokenPulseRuntimeEvents({
        traceId,
        provider,
        status,
        limit: 50,
      }),
    onSuccess: (payload) => {
      setTokenPulseRuntimeError(null);
      setHasLoadedTokenPulseRuntimeEvents(true);
      setTokenPulseRuntimePayload({
        items: payload.items,
        total: payload.total,
      });
      setTokenPulseRuntimeFeedback(`TokenPulse 运行时摘要已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setTokenPulseRuntimeFeedback(null);
      setHasLoadedTokenPulseRuntimeEvents(true);
      setTokenPulseRuntimePayload(null);
      setTokenPulseRuntimeError(
        `TokenPulse 运行时摘要加载失败：${toErrorMessage(error)}`,
      );
    },
  });

  const upsertMcpPolicyMutation = useMutation({
    mutationFn: ({
      toolId,
      riskLevel,
      decision,
      approvalMode,
      approvalWorkflow,
      approvalStages,
      stage1RequiredApprovals,
      stage2RequiredApprovals,
      stage1Roles,
      stage2Roles,
      approvalCondition,
      reason,
    }: {
      toolId: string;
      riskLevel: McpRiskLevel;
      decision: McpToolDecision;
      approvalMode?: McpApprovalMode;
      approvalWorkflow?: McpApprovalWorkflow;
      approvalStages?: McpToolPolicy["approvalStages"];
      stage1RequiredApprovals?: number;
      stage2RequiredApprovals?: number;
      stage1Roles?: string[];
      stage2Roles?: string[];
      approvalCondition?: McpToolPolicy["approvalCondition"];
      reason?: string;
    }) =>
      upsertMcpPolicy(toolId, {
        riskLevel,
        decision,
        approvalMode,
        approvalWorkflow,
        approvalStages,
        stage1RequiredApprovals,
        stage2RequiredApprovals,
        stage1Roles,
        stage2Roles,
        approvalCondition,
        reason,
      }),
    onSuccess: async (policy) => {
      setMcpError(null);
      setMcpFeedback(`策略 ${policy.toolId} 已更新为 ${policy.decision}。`);
      setMcpPolicyToolId(policy.toolId);
      setMcpPolicyRiskLevel(policy.riskLevel);
      setMcpPolicyDecision(policy.decision);
      setMcpPolicyApprovalMode(policy.approvalMode ?? "single_stage");
      setMcpPolicyApprovalStagesJson(
        policy.approvalStages && policy.approvalStages.length > 0
          ? JSON.stringify(policy.approvalStages, null, 2)
          : '[{"stage":"stage1","requiredApprovals":1,"roles":["owner","maintainer"]}]',
      );
      const workflowDraft = buildMcpWorkflowDraftFromWorkflow(policy.approvalWorkflow);
      setMcpWorkflowEntryNodeId(workflowDraft.entryNodeId);
      setMcpWorkflowNodes(workflowDraft.nodes);
      setMcpWorkflowTransitions(workflowDraft.transitions);
      setMcpPolicyStage1RequiredApprovals(
        typeof policy.stage1RequiredApprovals === "number"
          ? String(policy.stage1RequiredApprovals)
          : "1",
      );
      setMcpPolicyStage2RequiredApprovals(
        typeof policy.stage2RequiredApprovals === "number"
          ? String(policy.stage2RequiredApprovals)
          : "1",
      );
      setMcpPolicyStage1Roles((policy.stage1Roles ?? []).join(","));
      setMcpPolicyStage2Roles((policy.stage2Roles ?? []).join(","));
      setMcpPolicyConditionRiskLevel(
        policy.approvalCondition?.riskLevelAtLeast ?? "",
      );
      setMcpPolicyConditionToolIds(
        (policy.approvalCondition?.toolIds ?? []).join(","),
      );
      setMcpPolicyConditionTenantRoles(
        (policy.approvalCondition?.tenantRoles ?? []).join(","),
      );
      setMcpPolicyReason(policy.reason ?? "");
      await queryClient.invalidateQueries({ queryKey: ["mcp", "policies"] });
    },
    onError: (error) => {
      setMcpFeedback(null);
      setMcpError(`更新策略失败：${toErrorMessage(error)}`);
    },
  });

  const createMcpApprovalMutation = useMutation({
    mutationFn: ({
      toolId,
      reason,
      approvalConfig,
    }: {
      toolId: string;
      reason?: string;
      approvalConfig?: McpApprovalConfig;
    }) => createMcpApproval({ toolId, reason, approvalConfig }),
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
      nodeId,
    }: {
      approvalId: string;
      status: "approved" | "rejected";
      reason?: string;
      nodeId?: string;
    }) =>
      status === "approved"
        ? approveMcpApproval(
            approvalId,
            reason || nodeId ? { ...(reason ? { reason } : {}), ...(nodeId ? { nodeId } : {}) } : undefined,
          )
        : rejectMcpApproval(
            approvalId,
            reason || nodeId ? { ...(reason ? { reason } : {}), ...(nodeId ? { nodeId } : {}) } : undefined,
          ),
    onSuccess: async (approval) => {
      setMcpError(null);
      setMcpFeedback(`审批请求 ${approval.id} 已更新为 ${approval.status}。`);
      await queryClient.invalidateQueries({ queryKey: ["mcp", "approvals"] });
      await queryClient.invalidateQueries({ queryKey: ["mcp", "invocations"] });
    },
    onError: (error) => {
      setMcpFeedback(null);
      setMcpError(`审批操作失败：${toErrorMessage(error)}`);
    },
  });

  const evaluateMcpToolMutation = useMutation({
    mutationFn: ({
      toolId,
      approvalRequestId,
      reason,
      evaluationTimestamp,
      approvalConfig,
    }: {
      toolId: string;
      approvalRequestId?: string;
      reason?: string;
      evaluationTimestamp?: string;
      approvalConfig?: McpApprovalConfig;
    }) =>
      evaluateMcpTool({
        toolId,
        approvalRequestId,
        reason,
        evaluationTimestamp,
        approvalConfig,
      }),
    onSuccess: async (result) => {
      setMcpError(null);
      setMcpEvaluateResultPayload({
        toolId: result.toolId,
        decision: result.decision,
        result: result.result,
        approvalRequestId: result.approvalRequestId,
        approvalRequired: result.approvalRequired,
        approvalMode: result.approvalMode,
        currentNodeId: result.currentNodeId,
        currentStage: result.currentStage,
        approvalWorkflow: result.approvalWorkflow,
        approvalNodes: result.approvalNodes,
        pathHistory: result.pathHistory,
        nextTransitionPreview: result.nextTransitionPreview,
        approvalStages: result.approvalStages,
        remainingApprovals: result.remainingApprovals,
        approvalConditionMatched: result.approvalConditionMatched,
      });
      setMcpFeedback(
        `MCP 评估完成：${result.toolId} -> ${result.result}${
          result.currentStage ? ` (${result.currentStage})` : ""
        }。`,
      );
      await queryClient.invalidateQueries({ queryKey: ["mcp", "approvals"] });
      await queryClient.invalidateQueries({ queryKey: ["mcp", "invocations"] });
    },
    onError: (error) => {
      setMcpEvaluateResultPayload(null);
      setMcpFeedback(null);
      setMcpError(`MCP 评估失败：${toErrorMessage(error)}`);
    },
  });

  const loadOpenApiSummaryMutation = useMutation({
    mutationFn: () => fetchOpenPlatformOpenApiSummary(),
    onSuccess: (summary) => {
      setOpenApiError(null);
      setHasLoadedOpenApiSummary(true);
      setOpenApiSummaryPayload(summary);
      setOpenApiFeedback(`OpenAPI 摘要已加载，版本 ${summary.version}。`);
    },
    onError: (error) => {
      setOpenApiFeedback(null);
      setHasLoadedOpenApiSummary(true);
      setOpenApiError(`OpenAPI 摘要加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadApiKeysMutation = useMutation({
    mutationFn: (input: {
      status?: OpenPlatformApiKeyStatus;
      keyword?: string;
      limit: number;
    }) => fetchOpenPlatformApiKeys(input),
    onSuccess: (payload) => {
      setApiKeyError(null);
      setHasLoadedApiKeys(true);
      setApiKeyPayload({ items: payload.items, total: payload.total });
      setApiKeyFeedback(`API Key 列表已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setApiKeyFeedback(null);
      setHasLoadedApiKeys(true);
      setApiKeyError(`API Key 列表加载失败：${toErrorMessage(error)}`);
    },
  });

  const upsertApiKeyMutation = useMutation({
    mutationFn: ({
      keyId,
      input,
    }: {
      keyId: string;
      input: {
        name: string;
        scopes: string[];
        enabled: boolean;
        expiresAt?: string;
      };
    }) => upsertOpenPlatformApiKey(keyId, input),
    onSuccess: async (apiKey) => {
      setApiKeyError(null);
      setApiKeyFeedback(`API Key ${apiKey.id} 已保存。`);
      setApiKeyId(apiKey.id);
      setApiKeyName(apiKey.name);
      setApiKeyScopesInput(apiKey.scopes.join(","));
      setApiKeyEnabled(apiKey.status === "active");
      setApiKeyExpiresAt(apiKey.expiresAt ? apiKey.expiresAt.slice(0, 10) : "");
      try {
        const payload = await fetchOpenPlatformApiKeys(apiKeyQueryInput);
        setHasLoadedApiKeys(true);
        setApiKeyPayload({ items: payload.items, total: payload.total });
      } catch (error) {
        setApiKeyFeedback(
          `API Key ${apiKey.id} 已保存，但列表刷新失败：${toErrorMessage(error)}`,
        );
      }
    },
    onError: (error) => {
      setApiKeyFeedback(null);
      setApiKeyError(`保存 API Key 失败：${toErrorMessage(error)}`);
    },
  });

  const revokeApiKeyMutation = useMutation({
    mutationFn: ({ keyId, reason }: { keyId: string; reason?: string }) =>
      revokeOpenPlatformApiKey(keyId, reason),
    onSuccess: async (apiKey) => {
      setApiKeyError(null);
      setApiKeyFeedback(`API Key ${apiKey.id} 已吊销。`);
      if (apiKey.id === apiKeyId.trim()) {
        setApiKeyEnabled(false);
      }
      try {
        const payload = await fetchOpenPlatformApiKeys(apiKeyQueryInput);
        setHasLoadedApiKeys(true);
        setApiKeyPayload({ items: payload.items, total: payload.total });
      } catch (error) {
        setApiKeyFeedback(
          `API Key ${apiKey.id} 已吊销，但列表刷新失败：${toErrorMessage(error)}`,
        );
      }
    },
    onError: (error) => {
      setApiKeyFeedback(null);
      setApiKeyError(`吊销 API Key 失败：${toErrorMessage(error)}`);
    },
  });

  const loadWebhooksMutation = useMutation({
    mutationFn: (input: {
      enabled?: boolean;
      keyword?: string;
      limit: number;
    }) => fetchOpenPlatformWebhooks(input),
    onSuccess: (payload) => {
      setWebhookError(null);
      setHasLoadedWebhooks(true);
      setWebhookPayload({ items: payload.items, total: payload.total });
      setWebhookFeedback(`Webhook 列表已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setWebhookFeedback(null);
      setHasLoadedWebhooks(true);
      setWebhookError(`Webhook 列表加载失败：${toErrorMessage(error)}`);
    },
  });

  const upsertWebhookMutation = useMutation({
    mutationFn: ({
      webhookId,
      input,
    }: {
      webhookId: string;
      input: {
        name: string;
        url: string;
        events: string[];
        enabled: boolean;
      };
    }) => upsertOpenPlatformWebhook(webhookId, input),
    onSuccess: async (webhook) => {
      setWebhookError(null);
      setWebhookFeedback(`Webhook ${webhook.id} 已保存。`);
      setWebhookId(webhook.id);
      setWebhookName(webhook.name);
      setWebhookUrl(webhook.url);
      setWebhookEventsInput(webhook.events.join(","));
      setWebhookEnabled(webhook.enabled);
      try {
        const payload = await fetchOpenPlatformWebhooks(webhookQueryInput);
        setHasLoadedWebhooks(true);
        setWebhookPayload({ items: payload.items, total: payload.total });
      } catch (error) {
        setWebhookFeedback(
          `Webhook ${webhook.id} 已保存，但列表刷新失败：${toErrorMessage(error)}`,
        );
      }
    },
    onError: (error) => {
      setWebhookFeedback(null);
      setWebhookError(`保存 Webhook 失败：${toErrorMessage(error)}`);
    },
  });

  const deleteWebhookMutation = useMutation({
    mutationFn: ({ webhookId }: { webhookId: string }) =>
      deleteOpenPlatformWebhook(webhookId),
    onSuccess: async (_result, variables) => {
      setWebhookError(null);
      setWebhookFeedback(`Webhook ${variables.webhookId} 已删除。`);
      if (variables.webhookId === webhookId.trim()) {
        setWebhookId("");
        setWebhookName("");
        setWebhookUrl("");
        setWebhookEventsInput("");
        setWebhookEnabled(true);
      }
      try {
        const payload = await fetchOpenPlatformWebhooks(webhookQueryInput);
        setHasLoadedWebhooks(true);
        setWebhookPayload({ items: payload.items, total: payload.total });
      } catch (error) {
        setWebhookFeedback(
          `Webhook ${variables.webhookId} 已删除，但列表刷新失败：${toErrorMessage(error)}`,
        );
      }
    },
    onError: (error) => {
      setWebhookFeedback(null);
      setWebhookError(`删除 Webhook 失败：${toErrorMessage(error)}`);
    },
  });

  const replayWebhookMutation = useMutation({
    mutationFn: ({
      webhookId,
      input,
    }: {
      webhookId: string;
      input: {
        eventType?: string;
        from?: string;
        to?: string;
        limit?: number;
        dryRun?: boolean;
      };
    }) => replayOpenPlatformWebhook(webhookId, input),
    onSuccess: (result) => {
      setWebhookError(null);
      setWebhookFeedback(
        `Webhook ${result.webhookId} 回放任务 ${result.id} 已排队（dryRun=${result.dryRun ? "true" : "false"}）。`,
      );
    },
    onError: (error) => {
      setWebhookFeedback(null);
      setWebhookError(`回放 Webhook 失败：${toErrorMessage(error)}`);
    },
  });

  const upsertQualityAutomationPolicyMutation = useMutation({
    mutationFn: (input: {
      riskLevel: McpRiskLevel;
      decision: McpToolDecision;
      reason?: string;
      evaluationScoreThreshold?: number;
      triggerOnEvaluationFailure?: boolean;
      triggerOnReplayRegression?: boolean;
      strategyMatrix?: OpenPlatformAutomationStrategyRule[];
    }) => upsertOpenPlatformAutomationPolicy(input),
    onSuccess: async (payload) => {
      setQualityError(null);
      setQualityFeedback(`Quality automation policy 已更新为 ${payload.decision}。`);
      setQualityAutomationRiskLevel(payload.riskLevel);
      setQualityAutomationDecision(payload.decision);
      setQualityAutomationReason(payload.reason ?? "");
      setQualityAutomationEvaluationScoreThreshold(
        String(payload.evaluationScoreThreshold),
      );
      setQualityAutomationTriggerOnEvaluationFailure(
        payload.triggerOnEvaluationFailure,
      );
      setQualityAutomationTriggerOnReplayRegression(
        payload.triggerOnReplayRegression,
      );
      setQualityAutomationStrategyMatrixJson(
        JSON.stringify(payload.strategyMatrix ?? [], null, 2),
      );
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["quality", "automation-policy"] }),
        queryClient.invalidateQueries({ queryKey: ["quality", "automation-executions"] }),
      ]);
    },
    onError: (error) => {
      setQualityFeedback(null);
      setQualityError(`保存 Quality automation policy 失败：${toErrorMessage(error)}`);
    },
  });

  const simulateQualityAutomationPolicyMutation = useMutation({
    mutationFn: (input: {
      metric: string;
      score: number;
      sampleCount?: number;
      trendDirection?: "up" | "down" | "flat";
      confidence?: number;
      regressionProbability?: number;
      replayRegressionCount?: number;
    }) => simulateOpenPlatformAutomationPolicy(input),
    onSuccess: (payload) => {
      setQualityError(null);
      setQualityAutomationSimulationPayload(payload);
      setQualityFeedback(
        `Quality automation simulate 已完成：${payload.matchedRuleId ?? "default"} -> ${payload.resolvedAction}。`,
      );
    },
    onError: (error) => {
      setQualityFeedback(null);
      setQualityError(`模拟 Quality automation policy 失败：${toErrorMessage(error)}`);
    },
  });

  const loadQualityDailyMutation = useMutation({
    mutationFn: (input: {
      date?: string;
      metric?: string;
      provider?: string;
      repo?: string;
      workflow?: string;
      runId?: string;
      groupBy?: "provider" | "repo" | "workflow" | "runId";
      limit: number;
    }) => fetchOpenPlatformQualityDaily(input),
    onSuccess: (payload) => {
      setQualityError(null);
      setHasLoadedQualityDaily(true);
      setQualityDailyPayload({
        items: payload.items,
        total: payload.total,
        groups: payload.groups,
      });
      setQualityFeedback(`Quality daily 已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setQualityFeedback(null);
      setHasLoadedQualityDaily(true);
      setQualityError(`Quality daily 加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadQualityProjectTrendsMutation = useMutation({
    mutationFn: (input: {
      from?: string;
      to?: string;
      metric?: string;
      provider?: string;
      workflow?: string;
      includeUnknown?: boolean;
      limit: number;
    }) => fetchOpenPlatformQualityProjectTrends(input),
    onSuccess: (payload) => {
      setQualityError(null);
      setHasLoadedQualityProjectTrends(true);
      setQualityProjectTrendsPayload({
        items: payload.items,
        total: payload.total,
        summary: payload.summary,
      });
      setQualityFeedback(
        `Quality project-trends 已加载，共 ${payload.total} 个项目。`,
      );
    },
    onError: (error) => {
      setQualityFeedback(null);
      setHasLoadedQualityProjectTrends(true);
      setQualityError(
        `Quality project-trends 加载失败：${toErrorMessage(error)}`,
      );
    },
  });

  const loadQualityForecastMutation = useMutation({
    mutationFn: (input: {
      from?: string;
      to?: string;
      metric?: string;
      provider?: string;
      workflow?: string;
      limit?: number;
    }) => fetchOpenPlatformQualityForecast(input),
    onSuccess: (payload) => {
      setQualityError(null);
      setHasLoadedQualityForecast(true);
      setQualityForecastPayload({
        items: payload.items,
        total: payload.total,
      });
      setQualityFeedback(`Quality forecast 已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setQualityFeedback(null);
      setHasLoadedQualityForecast(true);
      setQualityError(`Quality forecast 加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadQualityAdviceMutation = useMutation({
    mutationFn: (input: {
      from?: string;
      to?: string;
      provider?: string;
      workflow?: string;
    }) => fetchOpenPlatformQualityAdvice(input),
    onSuccess: (payload) => {
      setQualityError(null);
      setHasLoadedQualityAdvice(true);
      setQualityAdvicePayload({
        items: payload.items,
        total: payload.total,
      });
      setQualityFeedback(`Quality advice 已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setQualityFeedback(null);
      setHasLoadedQualityAdvice(true);
      setQualityError(`Quality advice 加载失败：${toErrorMessage(error)}`);
    },
  });

  const executeQualityAdviceMutation = useMutation({
    mutationFn: (input: {
      adviceId: string;
      project: string;
      severity: "info" | "warn" | "critical";
      actionType: "scorecard_adjustment" | "replay_experiment";
      metric?: string;
      datasetId?: string;
      candidateLabels?: string[];
    }) =>
      executeOpenPlatformQualityAdvice(input.adviceId, {
        project: input.project,
        severity: input.severity,
        actionType: input.actionType,
        metric: input.metric,
        datasetId: input.datasetId,
        candidateLabels: input.candidateLabels,
      }),
    onSuccess: async (payload) => {
      setQualityError(null);
      setQualityFeedback(`Quality advice 执行已创建：${payload.id} (${payload.status})。`);
      await queryClient.invalidateQueries({ queryKey: ["quality", "advice"] });
      if (qualityAdviceExecutionPayload) {
        const next = await fetchOpenPlatformQualityAdviceExecutions({ limit: 50 });
        setHasLoadedQualityAdviceExecutions(true);
        setQualityAdviceExecutionPayload({ items: next.items, total: next.total });
      }
      if (payload.experimentId) {
        const experiment = await fetchOpenPlatformReplayExperimentResults(payload.experimentId);
        setReplayExperimentDetailPayload(experiment);
        setReplayExperimentComparePayload(null);
        setReplayExperimentWorkflowPayload(null);
      }
    },
    onError: (error) => {
      setQualityFeedback(null);
      setQualityError(`Quality advice 执行失败：${toErrorMessage(error)}`);
    },
  });

  const loadQualityAdviceExecutionsMutation = useMutation({
    mutationFn: (input: {
      adviceId?: string;
      actionType?: "scorecard_adjustment" | "replay_experiment";
      status?: "pending" | "running" | "completed" | "failed" | "cancelled";
      limit?: number;
    }) => fetchOpenPlatformQualityAdviceExecutions(input),
    onSuccess: (payload) => {
      setQualityError(null);
      setHasLoadedQualityAdviceExecutions(true);
      setQualityAdviceExecutionPayload({ items: payload.items, total: payload.total });
      setQualityFeedback(`Quality advice executions 已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setQualityFeedback(null);
      setHasLoadedQualityAdviceExecutions(true);
      setQualityError(`Quality advice executions 加载失败：${toErrorMessage(error)}`);
    },
  });

  const cancelQualityAdviceExecutionMutation = useMutation({
    mutationFn: (executionId: string) => cancelOpenPlatformQualityAdviceExecution(executionId),
    onSuccess: async (payload) => {
      setQualityError(null);
      setQualityFeedback(`Quality advice execution ${payload.id} 已取消。`);
      if (qualityAdviceExecutionPayload) {
        const next = await fetchOpenPlatformQualityAdviceExecutions({ limit: 50 });
        setHasLoadedQualityAdviceExecutions(true);
        setQualityAdviceExecutionPayload({ items: next.items, total: next.total });
      }
    },
    onError: (error) => {
      setQualityFeedback(null);
      setQualityError(`取消 Quality advice execution 失败：${toErrorMessage(error)}`);
    },
  });

  const loadQualityScorecardsMutation = useMutation({
    mutationFn: (input: { team?: string; limit: number }) =>
      fetchOpenPlatformQualityScorecards(input),
    onSuccess: (payload) => {
      setQualityError(null);
      setHasLoadedQualityScorecards(true);
      setQualityScorecardPayload({
        items: payload.items,
        total: payload.total,
      });
      setQualityFeedback(`Quality scorecards 已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setQualityFeedback(null);
      setHasLoadedQualityScorecards(true);
      setQualityError(`Quality scorecards 加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayDatasetsMutation = useMutation({
    mutationFn: (input: { keyword?: string; limit: number }) =>
      fetchOpenPlatformReplayDatasets(input),
    onSuccess: (payload) => {
      setReplayError(null);
      setHasLoadedReplayDatasets(true);
      setReplayDatasetPayload({ items: payload.items, total: payload.total });
      setReplayFeedback(`回放数据集已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setHasLoadedReplayDatasets(true);
      setReplayError(`回放数据集加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayDatasetVersionsMutation = useMutation({
    mutationFn: (datasetId: string) =>
      fetchOpenPlatformReplayDatasetVersions(datasetId),
    onSuccess: (payload) => {
      setReplayError(null);
      setHasLoadedReplayDatasetVersions(true);
      setReplayVersionDatasetId(payload.datasetId);
      setReplayDatasetVersionPayload({
        datasetId: payload.datasetId,
        items: payload.items,
        total: payload.total,
        currentVersionId: payload.currentVersionId,
        currentVersionNumber: payload.currentVersionNumber,
      });
      if (
        replayExperimentDatasetId.trim() === payload.datasetId &&
        payload.currentVersionId
      ) {
        setReplayExperimentBaselineVersionId(payload.currentVersionId);
      }
      if (
        replayCreateRunDatasetId.trim() === payload.datasetId &&
        payload.currentVersionId
      ) {
        setReplayCreateRunBaselineVersionId(payload.currentVersionId);
      }
      setReplayFeedback(`回放数据集版本已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setHasLoadedReplayDatasetVersions(true);
      setReplayError(`回放数据集版本加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayDatasetVersionCasesMutation = useMutation({
    mutationFn: ({ datasetId, versionId }: { datasetId: string; versionId: string }) =>
      fetchOpenPlatformReplayDatasetVersionCases(datasetId, versionId),
    onSuccess: (payload) => {
      setReplayError(null);
      setHasLoadedReplayDatasetVersionCases(true);
      setReplayDatasetVersionCasesPayload({
        datasetId: payload.datasetId,
        versionId: payload.versionId,
        items: payload.items,
        total: payload.total,
      });
      setReplayFeedback(`回放版本样本已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setHasLoadedReplayDatasetVersionCases(true);
      setReplayError(`回放版本样本加载失败：${toErrorMessage(error)}`);
    },
  });

  const createReplayExperimentMutation = useMutation({
    mutationFn: (input: {
      name: string;
      datasetId: string;
      baselineVersionId?: string;
      runIds?: string[];
      candidateLabels?: string[];
      autoRun?: boolean;
      triggerSource?: "manual" | "quality_advice" | "automatic";
      sourceAdviceId?: string;
    }) => createOpenPlatformReplayExperiment(input),
    onSuccess: async (payload) => {
      setReplayError(null);
      setReplayFeedback(`回放实验 ${payload.name} 已创建。`);
      setReplayExperimentName("");
      setReplayExperimentRunIds("");
      setReplayExperimentCandidateLabels("");
      setReplayExperimentBaselineVersionId(
        payload.baselineVersionId ?? replayExperimentBaselineVersionId,
      );
      setReplayExperimentDetailPayload(payload);
      setReplayExperimentComparePayload(null);
      setReplayExperimentBatchComparePayload(null);
      setReplayExperimentWorkflowPayload(null);
      await queryClient.invalidateQueries({ queryKey: ["replay", "experiments"] });
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`创建回放实验失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayExperimentsMutation = useMutation({
    mutationFn: (input: { datasetId?: string; limit?: number }) =>
      fetchOpenPlatformReplayExperiments(input),
    onSuccess: (payload) => {
      setReplayError(null);
      setHasLoadedReplayExperiments(true);
      setReplayExperimentPayload({ items: payload.items, total: payload.total });
      setReplayFeedback(`回放实验已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setHasLoadedReplayExperiments(true);
      setReplayError(`回放实验加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayExperimentDetailMutation = useMutation({
    mutationFn: (experimentId: string) => fetchOpenPlatformReplayExperimentResults(experimentId),
    onSuccess: (payload) => {
      setReplayError(null);
      setReplayExperimentDetailPayload(payload);
      setReplayExperimentComparePayload(null);
      setReplayExperimentBatchComparePayload(null);
      setReplayExperimentWorkflowPayload(null);
      setReplayFeedback(`回放实验 ${payload.name} 已加载。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`回放实验详情加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayExperimentCompareMutation = useMutation({
    mutationFn: (experimentId: string) => fetchOpenPlatformReplayExperimentCompare(experimentId),
    onSuccess: (payload) => {
      setReplayError(null);
      setReplayExperimentComparePayload(payload);
      setReplayExperimentBatchComparePayload(null);
      setReplayFeedback(`回放实验对比已加载，共 ${payload.total} 个 run。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`回放实验对比加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayExperimentWorkflowMutation = useMutation({
    mutationFn: (experimentId: string) => fetchOpenPlatformReplayExperimentWorkflow(experimentId),
    onSuccess: (payload) => {
      setReplayError(null);
      setReplayExperimentWorkflowPayload(payload);
      setReplayFeedback(`回放实验工作流已加载，共 ${payload.summary.totalNodes} 个节点。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`回放实验工作流加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayExperimentBatchCompareMutation = useMutation({
    mutationFn: (input: { experimentIds: string[]; datasetId?: string }) =>
      fetchOpenPlatformReplayExperimentsBatchCompare(input),
    onSuccess: (payload) => {
      setReplayError(null);
      setReplayExperimentBatchComparePayload(payload);
      setReplayFeedback(
        `回放实验批量对比已加载，共 ${payload.summary.comparedExperimentCount} 个实验。`,
      );
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`回放实验批量对比加载失败：${toErrorMessage(error)}`);
    },
  });

  const runReplayExperimentMutation = useMutation({
    mutationFn: (experimentId: string) => runOpenPlatformReplayExperiment(experimentId),
    onSuccess: async (payload) => {
      setReplayError(null);
      setReplayFeedback(`回放实验 ${payload.name} 已启动。`);
      setReplayExperimentDetailPayload(payload);
      setReplayExperimentComparePayload(null);
      setReplayExperimentBatchComparePayload(null);
      setReplayExperimentWorkflowPayload(null);
      await queryClient.invalidateQueries({ queryKey: ["replay", "experiments"] });
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`启动回放实验失败：${toErrorMessage(error)}`);
    },
  });

  const cancelReplayExperimentMutation = useMutation({
    mutationFn: (experimentId: string) => cancelOpenPlatformReplayExperiment(experimentId),
    onSuccess: async (payload) => {
      setReplayError(null);
      setReplayFeedback(`回放实验 ${payload.name} 已取消。`);
      setReplayExperimentDetailPayload(payload);
      setReplayExperimentComparePayload(null);
      setReplayExperimentBatchComparePayload(null);
      setReplayExperimentWorkflowPayload(null);
      await queryClient.invalidateQueries({ queryKey: ["replay", "experiments"] });
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`取消回放实验失败：${toErrorMessage(error)}`);
    },
  });

  const createReplayDatasetMutation = useMutation({
    mutationFn: (input: {
      name: string;
      datasetRef: string;
      model: string;
      promptVersion?: string;
      sampleCount?: number;
    }) => createOpenPlatformReplayDataset(input),
    onSuccess: async (payload) => {
      setReplayError(null);
      setReplayCreateDatasetName("");
      setReplayCreateDatasetRef("");
      setReplayCreateDatasetModel("");
      setReplayCreateDatasetPromptVersion("");
      setReplayCreateDatasetSampleCount("50");
      setReplayDatasetCasesDatasetId(payload.datasetId);
      setReplayVersionDatasetId(payload.datasetId);
      setReplayCreateRunDatasetId(payload.datasetId);
      setReplayCreateRunBaselineVersionId(payload.currentVersionId ?? "");
      setReplayExperimentDatasetId(payload.datasetId);
      setReplayExperimentBaselineVersionId(payload.currentVersionId ?? "");
      setReplayDiffDatasetId(payload.datasetId);
      setReplayRunsDatasetIdFilter(payload.datasetId);
      setReplayFeedback(`回放数据集 ${payload.datasetId} 已创建。`);
      try {
        const refreshed = await fetchOpenPlatformReplayDatasets({
          keyword: replayDatasetKeyword.trim() || undefined,
          limit: 50,
        });
        setHasLoadedReplayDatasets(true);
        setReplayDatasetPayload({
          items: refreshed.items,
          total: refreshed.total,
        });
      } catch (error) {
        setReplayFeedback(
          `回放数据集 ${payload.datasetId} 已创建，但列表刷新失败：${toErrorMessage(error)}`,
        );
      }
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`创建回放数据集失败：${toErrorMessage(error)}`);
    },
  });

  const createReplayDatasetVersionMutation = useMutation({
    mutationFn: ({
      datasetId,
      input,
    }: {
      datasetId: string;
      input: {
        datasetRef: string;
        model: string;
        promptVersion?: string;
        sampleCount?: number;
        note?: string;
      };
    }) => createOpenPlatformReplayDatasetVersion(datasetId, input),
    onSuccess: async (payload) => {
      setReplayError(null);
      setReplayCreateVersionDatasetRef("");
      setReplayCreateVersionModel("");
      setReplayCreateVersionPromptVersion("");
      setReplayCreateVersionSampleCount("50");
      setReplayCreateVersionNote("");
      setReplayFeedback(
        `回放数据集版本 v${payload.version} 已创建，versionId=${payload.id}。`,
      );
      try {
        const [versionPayload, datasetPayload] = await Promise.all([
          fetchOpenPlatformReplayDatasetVersions(payload.datasetId),
          fetchOpenPlatformReplayDatasets({
            keyword: replayDatasetKeyword.trim() || undefined,
            limit: 50,
          }),
        ]);
        setHasLoadedReplayDatasetVersions(true);
        setReplayDatasetVersionPayload({
          datasetId: versionPayload.datasetId,
          items: versionPayload.items,
          total: versionPayload.total,
          currentVersionId: versionPayload.currentVersionId,
          currentVersionNumber: versionPayload.currentVersionNumber,
        });
        setHasLoadedReplayDatasets(true);
        setReplayDatasetPayload({
          items: datasetPayload.items,
          total: datasetPayload.total,
        });
      } catch (error) {
        setReplayFeedback(
          `回放数据集版本 v${payload.version} 已创建，但列表刷新失败：${toErrorMessage(error)}`,
        );
      }
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`创建回放数据集版本失败：${toErrorMessage(error)}`);
    },
  });

  const promoteReplayDatasetVersionMutation = useMutation({
    mutationFn: ({ datasetId, versionId }: { datasetId: string; versionId: string }) =>
      promoteOpenPlatformReplayDatasetVersion(datasetId, { versionId }),
    onSuccess: async (payload) => {
      setReplayError(null);
      if (payload.dataset) {
        setReplayDatasetPayload((current) =>
          current
            ? {
                ...current,
                items: current.items.map((item) =>
                  item.datasetId === payload.dataset?.datasetId ? payload.dataset : item,
                ),
              }
            : current,
        );
        if (replayExperimentDatasetId.trim() === payload.dataset.datasetId) {
          setReplayExperimentBaselineVersionId(
            payload.dataset.currentVersionId ?? payload.version.id,
          );
        }
        if (replayCreateRunDatasetId.trim() === payload.dataset.datasetId) {
          setReplayCreateRunBaselineVersionId(
            payload.dataset.currentVersionId ?? payload.version.id,
          );
        }
      }
      setReplayFeedback(
        `回放数据集版本 v${payload.version.version} 已提升为当前版本。`,
      );
      try {
        const versionPayload = await fetchOpenPlatformReplayDatasetVersions(
          payload.dataset?.datasetId ?? replayVersionDatasetId,
        );
        setHasLoadedReplayDatasetVersions(true);
        setReplayVersionDatasetId(versionPayload.datasetId);
        setReplayDatasetVersionPayload({
          datasetId: versionPayload.datasetId,
          items: versionPayload.items,
          total: versionPayload.total,
          currentVersionId: versionPayload.currentVersionId,
          currentVersionNumber: versionPayload.currentVersionNumber,
        });
      } catch (error) {
        setReplayFeedback(
          `回放数据集版本已提升，但版本列表刷新失败：${toErrorMessage(error)}`,
        );
      }
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`提升回放数据集版本失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayDatasetCasesMutation = useMutation({
    mutationFn: (datasetId: string) =>
      fetchOpenPlatformReplayDatasetCases(datasetId),
    onSuccess: (payload) => {
      setReplayError(null);
      setHasLoadedReplayDatasetCases(true);
      setReplayDatasetCasesDatasetId(payload.datasetId);
      setReplayDatasetCasesPayload({
        datasetId: payload.datasetId,
        items: payload.items,
        total: payload.total,
      });
      setReplayDatasetCasesEditor(
        formatPrettyJson(
          payload.items.map((item) => ({
            caseId: item.caseId,
            sortOrder: item.sortOrder,
            input: item.input,
            expectedOutput: item.expectedOutput,
            baselineOutput: item.baselineOutput,
            candidateInput: item.candidateInput,
            metadata: item.metadata,
          })),
        ),
      );
      setReplayFeedback(`回放样本已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setHasLoadedReplayDatasetCases(true);
      setReplayError(`加载回放样本失败：${toErrorMessage(error)}`);
    },
  });

  const saveReplayDatasetCasesMutation = useMutation({
    mutationFn: ({
      datasetId,
      items,
    }: {
      datasetId: string;
      items: Array<{
        caseId?: string;
        sortOrder?: number;
        input: string;
        expectedOutput?: string;
        baselineOutput?: string;
        candidateInput?: string;
        metadata?: Record<string, unknown>;
      }>;
    }) => replaceOpenPlatformReplayDatasetCases(datasetId, { items }),
    onSuccess: (payload) => {
      setReplayError(null);
      setHasLoadedReplayDatasetCases(true);
      setReplayDatasetCasesDatasetId(payload.datasetId);
      setReplayDatasetCasesPayload({
        datasetId: payload.datasetId,
        items: payload.items,
        total: payload.total,
      });
      setReplayDatasetCasesEditor(
        formatPrettyJson(
          payload.items.map((item) => ({
            caseId: item.caseId,
            sortOrder: item.sortOrder,
            input: item.input,
            expectedOutput: item.expectedOutput,
            baselineOutput: item.baselineOutput,
            candidateInput: item.candidateInput,
            metadata: item.metadata,
          })),
        ),
      );
      setReplayFeedback(`回放样本已保存，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`保存回放样本失败：${toErrorMessage(error)}`);
    },
  });

  const materializeReplayDatasetCasesMutation = useMutation({
    mutationFn: ({
      datasetId,
      sessionIds,
      filters,
      sampleLimit,
      sanitized,
    }: {
      datasetId: string;
      sessionIds?: string[];
      filters?: {
        keyword?: string;
        tool?: string;
        model?: string;
        from?: string;
        to?: string;
      };
      sampleLimit?: number;
      sanitized?: boolean;
    }) =>
      materializeOpenPlatformReplayDatasetCases(datasetId, {
        sessionIds,
        filters,
        sampleLimit,
        sanitized,
      }),
    onSuccess: (payload) => {
      setReplayError(null);
      setHasLoadedReplayDatasetCases(true);
      setReplayMaterializePayload(payload);
      setReplayDatasetCasesDatasetId(payload.datasetId);
      setReplayDatasetCasesPayload({
        datasetId: payload.datasetId,
        items: payload.items,
        total: payload.total,
      });
      setReplayDatasetCasesEditor(
        formatPrettyJson(
          payload.items.map((item) => ({
            caseId: item.caseId,
            sortOrder: item.sortOrder,
            input: item.input,
            expectedOutput: item.expectedOutput,
            baselineOutput: item.baselineOutput,
            candidateInput: item.candidateInput,
            metadata: item.metadata,
          })),
        ),
      );
      setReplayFeedback(
        `已从历史会话物化 ${payload.materialized} 条样本，跳过 ${payload.skipped} 条。`,
      );
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`历史会话物化失败：${toErrorMessage(error)}`);
    },
  });

  const createReplayRunMutation = useMutation({
    mutationFn: (input: {
      datasetId: string;
      baselineVersionId?: string;
      candidateLabel: string;
      sampleLimit?: number;
    }) => createOpenPlatformReplayRun(input),
    onSuccess: async (payload) => {
      setReplayError(null);
      setReplayCreateRunCandidateLabel("");
      setReplayCreateRunSampleLimit("50");
      setReplayCreateRunBaselineVersionId(
        payload.baselineVersionId ?? replayCreateRunBaselineVersionId,
      );
      setReplayDiffRunId(payload.runId);
      setReplayArtifactRunId(payload.runId);
      setReplayFeedback(
        `回放运行 ${payload.runId} 已创建，当前状态 ${payload.status}。`,
      );
      try {
        const refreshed = await fetchOpenPlatformReplayRuns({
          datasetId: replayRunsDatasetIdFilter.trim() || undefined,
          status: replayRunsStatusFilter || undefined,
          limit: 50,
        });
        setHasLoadedReplayJobs(true);
        setReplayRunPayload({ items: refreshed.items, total: refreshed.total });
      } catch (error) {
        setReplayFeedback(
          `回放运行 ${payload.runId} 已创建，但运行列表刷新失败：${toErrorMessage(error)}`,
        );
      }
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`创建回放运行失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayRunsMutation = useMutation({
    mutationFn: (input: {
      datasetId?: string;
      status?: OpenPlatformReplayJobStatus;
      limit: number;
    }) => fetchOpenPlatformReplayRuns(input),
    onSuccess: (payload) => {
      setReplayError(null);
      setHasLoadedReplayJobs(true);
      setReplayRunPayload({ items: payload.items, total: payload.total });
      setReplayFeedback(`回放运行已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setHasLoadedReplayJobs(true);
      setReplayError(`回放运行加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayDiffMutation = useMutation({
    mutationFn: (input: {
      datasetId?: string;
      runId: string;
      keyword?: string;
      limit: number;
    }) => fetchOpenPlatformReplayDiffs(input),
    onSuccess: (payload) => {
      setReplayError(null);
      setHasLoadedReplayDiff(true);
      setReplayDiffPayload({
        items: payload.items,
        total: payload.total,
        summary: payload.summary,
      });
      setReplayFeedback(`回放差异已加载，共 ${payload.total} 条。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setHasLoadedReplayDiff(true);
      setReplayError(`回放差异加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayArtifactsMutation = useMutation({
    mutationFn: (runId: string) => fetchOpenPlatformReplayArtifacts(runId),
    onSuccess: (payload) => {
      setReplayError(null);
      setHasLoadedReplayArtifacts(true);
      setReplayArtifactPayload({
        runId: payload.runId,
        items: payload.items,
        total: payload.total,
      });
      setReplayFeedback(`回放工件已加载，共 ${payload.total} 个工件。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setHasLoadedReplayArtifacts(true);
      setReplayError(`回放工件加载失败：${toErrorMessage(error)}`);
    },
  });

  const loadReplayExperimentArtifactsMutation = useMutation({
    mutationFn: (experimentId: string) =>
      fetchOpenPlatformReplayExperimentArtifacts(experimentId),
    onSuccess: (payload) => {
      setReplayError(null);
      setHasLoadedReplayExperimentArtifacts(true);
      setReplayExperimentArtifactPayload({
        experimentId: payload.experimentId,
        datasetId: payload.datasetId,
        items: payload.items,
        total: payload.total,
      });
      setReplayFeedback(`回放实验工件已加载，共 ${payload.total} 个工件。`);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setHasLoadedReplayExperimentArtifacts(true);
      setReplayError(`回放实验工件加载失败：${toErrorMessage(error)}`);
    },
  });

  const downloadReplayArtifactMutation = useMutation({
    mutationFn: (input: {
      runId: string;
      artifactType: string;
      downloadName?: string;
    }) =>
      downloadOpenPlatformReplayArtifact(
        input.runId,
        input.artifactType,
        input.downloadName,
      ),
    onSuccess: (file) => {
      setReplayError(null);
      setReplayFeedback(`回放工件下载成功：${file.filename}`);
      triggerBrowserDownload(file);
    },
    onError: (error) => {
      setReplayFeedback(null);
      setReplayError(`回放工件下载失败：${toErrorMessage(error)}`);
    },
  });

  const exportSessionsMutation = useMutation({
    mutationFn: (format: ExportFormat) =>
      exportSessions(format, { limit: 200 }),
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
    mutationFn: (input: {
      format: ExportFormat;
      dimension: UsageExportDimension;
    }) =>
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
  const orchestrationRuleItems = orchestrationRulesPayload?.items ?? [];
  const orchestrationExecutionItems =
    orchestrationExecutionsPayload?.items ?? [];
  const orchestrationSimulationExecutions =
    orchestrationSimulationResult?.executions ?? [];
  const orchestrationExecutionSummary = useMemo(
    () => ({
      total: orchestrationExecutionItems.length,
      ruleDispatches: orchestrationExecutionItems.filter(
        (item) => item.dispatchMode === "rule",
      ).length,
      fallbackDispatches: orchestrationExecutionItems.filter(
        (item) => item.dispatchMode === "fallback",
      ).length,
      conflictExecutions: orchestrationExecutionItems.filter(
        (item) => item.conflictRuleIds.length > 0,
      ).length,
      dedupeHits: orchestrationExecutionItems.filter((item) => item.dedupeHit)
        .length,
      suppressedExecutions: orchestrationExecutionItems.filter(
        (item) => item.suppressed,
      ).length,
      simulatedExecutions: orchestrationExecutionItems.filter(
        (item) => item.simulated,
      ).length,
      escalatedExecutions: orchestrationExecutionItems.filter(
        (item) => item.escalated,
      ).length,
    }),
    [orchestrationExecutionItems],
  );
  const knownOrchestrationRuleIds = useMemo(
    () =>
      Array.from(
        new Set([
          ...orchestrationRuleItems.map((rule) => rule.id),
          ...orchestrationExecutionItems.map((execution) => execution.ruleId),
        ]),
      ).sort((left, right) => left.localeCompare(right)),
    [orchestrationExecutionItems, orchestrationRuleItems],
  );
  const knownOrchestrationSourceIds = useMemo(
    () =>
      Array.from(
        new Set(
          [
            ...orchestrationRuleItems.map((rule) => rule.sourceId ?? ""),
            ...orchestrationExecutionItems.map(
              (execution) => execution.sourceId ?? "",
            ),
          ].filter((item) => item.trim().length > 0),
        ),
      ).sort((left, right) => left.localeCompare(right)),
    [orchestrationExecutionItems, orchestrationRuleItems],
  );
  const simulationConflictRuleSet = useMemo(
    () => new Set(orchestrationSimulationResult?.conflictRuleIds ?? []),
    [orchestrationSimulationResult],
  );
  const simulationConflictRules = useMemo(() => {
    if (!orchestrationSimulationResult) {
      return [];
    }
    const candidateRules = [
      ...orchestrationSimulationResult.matchedRules,
      ...orchestrationRuleItems,
    ];
    const mergedRuleById = new Map<string, AlertOrchestrationRule>();
    for (const candidate of candidateRules) {
      if (!mergedRuleById.has(candidate.id)) {
        mergedRuleById.set(candidate.id, candidate);
      }
    }
    return orchestrationSimulationResult.conflictRuleIds.map((ruleId) => ({
      ruleId,
      rule: mergedRuleById.get(ruleId) ?? null,
    }));
  }, [orchestrationRuleItems, orchestrationSimulationResult]);
  const regionItems: RegionDescriptor[] =
    residencyRegionsQuery.data?.items ?? [];
  const replicationItems: ReplicationJob[] =
    replicationJobsQuery.data?.items ?? [];
  const systemConfigPackageItems: SystemConfigPackage[] =
    systemConfigPackagesQuery.data?.items ?? [];
  const selectedSystemConfigPackage =
    systemConfigPackageItems.find(
      (item) => item.packageId === selectedSystemConfigPackageId,
    ) ?? null;
  const systemConfigApprovalItems: SystemConfigPackageApproval[] =
    systemConfigPackageApprovalsQuery.data?.items ?? [];
  const agentReleaseItems: AgentRelease[] = agentReleasesQuery.data?.items ?? [];
  const selectedAgentRelease =
    agentReleaseItems.find((item) => item.releaseId === selectedAgentReleaseId) ?? null;
  const ruleItems: RuleAsset[] = ruleAssetsQuery.data?.items ?? [];
  const selectedRuleAsset =
    ruleItems.find((asset) => asset.id === selectedRuleAssetId) ?? null;
  const ruleVersionItems: RuleAssetVersion[] =
    ruleVersionsQuery.data?.items ?? [];
  const ruleApprovalItems: RuleApproval[] =
    ruleApprovalsQuery.data?.items ?? [];
  const ruleApprovalSummaryByVersion = useMemo(() => {
    const summary = new Map<number, { approved: number; rejected: number }>();
    for (const item of ruleApprovalItems) {
      const current = summary.get(item.version) ?? {
        approved: 0,
        rejected: 0,
      };
      if (item.decision === "approved") {
        current.approved += 1;
      } else if (item.decision === "rejected") {
        current.rejected += 1;
      }
      summary.set(item.version, current);
    }
    return summary;
  }, [ruleApprovalItems]);

  function resetSystemConfigCreateForm() {
    setSystemConfigCreateVersion("");
    setSystemConfigCreateIssuedAt("");
    setSystemConfigCreateSignatureStatus("unknown");
    setSystemConfigCreateRequiresApproval(false);
    setSystemConfigCreateRequiredApprovals(0);
    setSystemConfigCreateAgentIds("");
    setSystemConfigCreateDeviceIds("");
    setSystemConfigCreateChannels("");
    setSystemConfigCreateHostnames("");
    setSystemConfigCreatePayloadJson("{}");
    setSystemConfigCreateFormSource(null);
  }

  function hydrateSystemConfigCreateForm(
    pkg: SystemConfigPackage,
    mode: SystemConfigCreateFormSource["mode"],
  ) {
    setSystemConfigCreateVersion(pkg.version);
    setSystemConfigCreateIssuedAt(pkg.issuedAt ?? "");
    setSystemConfigCreateSignatureStatus(pkg.signatureStatus);
    setSystemConfigCreateRequiresApproval(pkg.requiresApproval);
    setSystemConfigCreateRequiredApprovals(
      pkg.requiresApproval ? pkg.requiredApprovals : 0,
    );
    setSystemConfigCreateAgentIds((pkg.targetSelectors.agentIds ?? []).join(","));
    setSystemConfigCreateDeviceIds((pkg.targetSelectors.deviceIds ?? []).join(","));
    setSystemConfigCreateChannels((pkg.targetSelectors.channels ?? []).join(","));
    setSystemConfigCreateHostnames((pkg.targetSelectors.hostnames ?? []).join(","));
    setSystemConfigCreatePayloadJson(JSON.stringify(pkg.payload ?? {}, null, 2));
    setSystemConfigCreateFormSource({
      packageId: pkg.packageId,
      version: pkg.version,
      mode,
    });
  }

  function hydrateAgentReleasePreviewFromArtifact(
    artifact: AgentReleaseArtifact,
  ) {
    setAgentReleasePreviewOs(artifact.os);
    setAgentReleasePreviewArch(artifact.arch);
    setAgentReleasePreviewRing(artifact.rolloutRing ?? "");
  }
  const currentPublishVersion = Number(rulePublishVersion);
  const currentPublishApprovalSummary =
    Number.isInteger(currentPublishVersion) && currentPublishVersion > 0
      ? ruleApprovalSummaryByVersion.get(currentPublishVersion) ?? {
          approved: 0,
          rejected: 0,
        }
      : null;
  const tokenPulseRuntimeItems: TokenPulseRuntimeEvent[] =
    tokenPulseRuntimePayload?.items ?? [];
  const mcpPolicyItems: McpToolPolicy[] = mcpPoliciesQuery.data?.items ?? [];
  const mcpApprovalItems: McpApprovalRequest[] =
    mcpApprovalsQuery.data?.items ?? [];
  const mcpInvocationItems: McpInvocationAudit[] =
    mcpInvocationsQuery.data?.items ?? [];
  const qualityAutomationPolicy: OpenPlatformAutomationPolicy | null =
    qualityAutomationPolicyQuery.data ?? null;
  const qualityAutomationExecutionItems: McpInvocationAudit[] =
    qualityAutomationExecutionsQuery.data?.items ?? [];
  const apiKeyItems: OpenPlatformApiKey[] = apiKeyPayload?.items ?? [];
  const webhookItems: OpenPlatformWebhook[] = webhookPayload?.items ?? [];
  const qualityDailyItems: OpenPlatformQualityDailyItem[] =
    qualityDailyPayload?.items ?? [];
  const qualityDailyGroups = qualityDailyPayload?.groups ?? [];
  const qualityForecastItems: OpenPlatformQualityForecastItem[] =
    qualityForecastPayload?.items ?? [];
  const qualityAdviceItems: OpenPlatformQualityAdviceItem[] =
    qualityAdvicePayload?.items ?? [];
  const qualityProjectTrendItems: OpenPlatformQualityProjectTrendItem[] =
    qualityProjectTrendsPayload?.items ?? [];
  const qualityProjectTrendSummary =
    qualityProjectTrendsPayload?.summary ?? null;
  const qualityScorecardItems: OpenPlatformQualityScorecard[] =
    qualityScorecardPayload?.items ?? [];
  const replayDatasetItems: OpenPlatformReplayDataset[] =
    replayDatasetPayload?.items ?? [];
  const replayDatasetVersionItems: OpenPlatformReplayDatasetVersion[] =
    replayDatasetVersionPayload?.items ?? [];
  const replayDatasetCaseItems: OpenPlatformReplayDatasetCase[] =
    replayDatasetCasesPayload?.items ?? [];
  const replayDatasetVersionCaseItems: OpenPlatformReplayDatasetCase[] =
    replayDatasetVersionCasesPayload?.items ?? [];
  const replayRunItems: OpenPlatformReplayRun[] = replayRunPayload?.items ?? [];
  const replayExperimentItems: OpenPlatformReplayExperiment[] =
    replayExperimentPayload?.items ?? [];
  const replayExperimentBatchCompareItems =
    replayExperimentBatchComparePayload?.items ?? [];
  const replayDiffItems: OpenPlatformReplayDiffItem[] =
    replayDiffPayload?.items ?? [];
  const replayArtifactItems: OpenPlatformReplayArtifact[] =
    replayArtifactPayload?.items ?? [];
  const replayExperimentArtifactItems: OpenPlatformReplayArtifact[] =
    replayExperimentArtifactPayload?.items ?? [];
  const replaySelectedRunSummary = useMemo(() => {
    const diffSummary = replayDiffPayload?.summary;
    if (
      diffSummary &&
      typeof diffSummary === "object" &&
      !Array.isArray(diffSummary)
    ) {
      return diffSummary;
    }
    const activeRunId = replayDiffRunId.trim() || replayArtifactRunId.trim();
    if (!activeRunId) {
      return null;
    }
    const matched = replayRunItems.find((item) => item.runId === activeRunId);
    return matched?.summary ?? null;
  }, [
    replayArtifactRunId,
    replayDiffPayload?.summary,
    replayDiffRunId,
    replayRunItems,
  ]);
  const replaySelectedRunDigest =
    replaySelectedRunSummary &&
    typeof replaySelectedRunSummary.digest === "object" &&
    replaySelectedRunSummary.digest !== null &&
    !Array.isArray(replaySelectedRunSummary.digest)
      ? (replaySelectedRunSummary.digest as Record<string, unknown>)
      : null;
  const knownReplayDatasetIds = useMemo(
    () =>
      Array.from(
        new Set([
          ...replayDatasetItems.map((item) => item.datasetId),
          ...replayDatasetCaseItems.map((item) => item.datasetId),
          ...replayRunItems.map((item) => item.datasetId),
        ]),
      ).sort((left, right) => left.localeCompare(right)),
    [replayDatasetItems, replayDatasetCaseItems, replayRunItems],
  );
  const knownReplayVersionIds = useMemo(
    () =>
      Array.from(
        new Set([
          ...replayDatasetVersionItems.map((item) => item.id),
          ...replayDatasetItems
            .map((item) => item.currentVersionId)
            .filter((item): item is string => Boolean(item)),
        ]),
      ).sort((left, right) => left.localeCompare(right)),
    [replayDatasetItems, replayDatasetVersionItems],
  );
  const replaySelectedExperimentDataset = useMemo(
    () =>
      replayDatasetItems.find(
        (item) => item.datasetId === replayExperimentDatasetId.trim(),
      ) ?? null,
    [replayDatasetItems, replayExperimentDatasetId],
  );
  const replaySelectedExperimentCurrentVersionId =
    replayDatasetVersionPayload?.datasetId === replayExperimentDatasetId.trim()
      ? replayDatasetVersionPayload.currentVersionId ??
        replaySelectedExperimentDataset?.currentVersionId ??
        null
      : replaySelectedExperimentDataset?.currentVersionId ?? null;
  const replaySelectedExperimentCurrentVersionNumber =
    replayDatasetVersionPayload?.datasetId === replayExperimentDatasetId.trim()
      ? replayDatasetVersionPayload.currentVersionNumber ??
        replaySelectedExperimentDataset?.currentVersionNumber ??
        null
      : replaySelectedExperimentDataset?.currentVersionNumber ?? null;
  const knownReplayRunIds = useMemo(
    () =>
      Array.from(new Set(replayRunItems.map((item) => item.runId))).sort(
        (left, right) => left.localeCompare(right),
      ),
    [replayRunItems],
  );
  const residencyExtensionPanel = (
    <div className="governance-stack">
      <h3>KMS Key Mapping</h3>
      <div className="filters-row governance-inline-grid">
        <label className="inline-field" htmlFor="residency-kms-region-id-inline">
          Region
          <select
            id="residency-kms-region-id-inline"
            value={residencyKmsRegionId}
            onChange={(event) => setResidencyKmsRegionId(event.target.value)}
          >
            <option value="">请选择</option>
            {regionItems.map((region) => (
              <option key={`kms-inline-${region.id}`} value={region.id}>
                {region.id}
              </option>
            ))}
          </select>
        </label>
        <label className="inline-field" htmlFor="residency-kms-provider-inline">
          Key Provider
          <input
            id="residency-kms-provider-inline"
            type="text"
            value={residencyKmsProvider}
            onChange={(event) => setResidencyKmsProvider(event.target.value)}
            placeholder="例如：kms"
          />
        </label>
        <label className="inline-field governance-wide-field" htmlFor="residency-kms-key-ref-inline">
          Key Ref
          <input
            id="residency-kms-key-ref-inline"
            type="text"
            value={residencyKmsKeyRef}
            onChange={(event) => setResidencyKmsKeyRef(event.target.value)}
            placeholder="例如：kms://primary-cn-hangzhou"
          />
        </label>
        <label className="checkbox-field" htmlFor="residency-kms-enabled-inline">
          <input
            id="residency-kms-enabled-inline"
            type="checkbox"
            checked={residencyKmsEnabled}
            onChange={(event) => setResidencyKmsEnabled(event.target.checked)}
          />
          启用
        </label>
        <button
          type="button"
          className="submit-button"
          onClick={() => {
            const regionId = residencyKmsRegionId.trim();
            const keyProvider = residencyKmsProvider.trim();
            const keyRef = residencyKmsKeyRef.trim();
            if (!regionId || !keyProvider || !keyRef) {
              setResidencyKmsFeedback(null);
              setResidencyKmsError("Region、Key Provider、Key Ref 不能为空。");
              return;
            }
            setResidencyKmsFeedback(null);
            setResidencyKmsError(null);
            setResidencyKmsDrafts((prev) => {
              const nextItem: ResidencyKmsKeyMapping = {
                tenantId: residencyPolicyQuery.data?.tenantId ?? "default",
                regionId,
                keyProvider,
                keyRef,
                enabled: residencyKmsEnabled,
                updatedAt: new Date().toISOString(),
              };
              return [...prev.filter((item) => item.regionId !== regionId), nextItem].sort(
                (a, b) => a.regionId.localeCompare(b.regionId),
              );
            });
            setResidencyKmsRegionId("");
            setResidencyKmsProvider("");
            setResidencyKmsKeyRef("");
            setResidencyKmsEnabled(true);
          }}
        >
          添加 / 覆盖 KMS 映射
        </button>
        <button
          type="button"
          className="submit-button"
          disabled={saveResidencyKmsMappingsMutation.isPending}
          onClick={() => {
            setResidencyKmsFeedback(null);
            setResidencyKmsError(null);
            saveResidencyKmsMappingsMutation.mutate(residencyKmsDrafts);
          }}
        >
          {saveResidencyKmsMappingsMutation.isPending ? "保存中..." : "保存 KMS 映射"}
        </button>
      </div>
      {residencyKmsQuery.isLoading ? <p className="feedback info">KMS 映射加载中...</p> : null}
      {residencyKmsQuery.isError ? (
        <p className="feedback error">KMS 映射加载失败：{toErrorMessage(residencyKmsQuery.error)}</p>
      ) : null}
      {residencyKmsFeedback ? <p className="feedback success">{residencyKmsFeedback}</p> : null}
      {residencyKmsError ? <p className="feedback error">{residencyKmsError}</p> : null}
      <div className="table-wrapper">
        <table className="session-table">
          <thead>
            <tr>
              <th>Region</th>
              <th>Provider</th>
              <th>Key Ref</th>
              <th>Enabled</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {residencyKmsDrafts.length === 0 ? (
              <tr>
                <td className="table-empty-cell" colSpan={5}>
                  暂无 KMS 映射
                </td>
              </tr>
            ) : (
              residencyKmsDrafts.map((item) => (
                <tr key={`kms-inline-row-${item.regionId}`}>
                  <td>{item.regionId}</td>
                  <td>{item.keyProvider}</td>
                  <td>{item.keyRef}</td>
                  <td>{item.enabled ? "true" : "false"}</td>
                  <td>
                    <button
                      type="button"
                      className="table-action"
                      onClick={() =>
                        setResidencyKmsDrafts((prev) =>
                          prev.filter((current) => current.regionId !== item.regionId),
                        )
                      }
                    >
                      删除
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <h3>Archive Region Policy</h3>
      <div className="filters-row governance-inline-grid">
        <label className="inline-field" htmlFor="residency-archive-source-region-inline">
          Source Region
          <select
            id="residency-archive-source-region-inline"
            value={residencyArchiveSourceRegion}
            onChange={(event) => setResidencyArchiveSourceRegion(event.target.value)}
          >
            <option value="">请选择</option>
            {regionItems.map((region) => (
              <option key={`archive-source-inline-${region.id}`} value={region.id}>
                {region.id}
              </option>
            ))}
          </select>
        </label>
        <label className="inline-field" htmlFor="residency-archive-target-region-inline">
          Archive Region
          <select
            id="residency-archive-target-region-inline"
            value={residencyArchiveTargetRegion}
            onChange={(event) => setResidencyArchiveTargetRegion(event.target.value)}
          >
            <option value="">请选择</option>
            {regionItems.map((region) => (
              <option key={`archive-target-inline-${region.id}`} value={region.id}>
                {region.id}
              </option>
            ))}
          </select>
        </label>
        <label className="inline-field" htmlFor="residency-archive-class-inline">
          Archive Class
          <input
            id="residency-archive-class-inline"
            type="text"
            value={residencyArchiveClass}
            onChange={(event) => setResidencyArchiveClass(event.target.value)}
            placeholder="例如：cold"
          />
        </label>
        <label className="checkbox-field" htmlFor="residency-archive-enabled-inline">
          <input
            id="residency-archive-enabled-inline"
            type="checkbox"
            checked={residencyArchiveEnabled}
            onChange={(event) => setResidencyArchiveEnabled(event.target.checked)}
          />
          启用
        </label>
        <button
          type="button"
          className="submit-button"
          onClick={() => {
            const sourceRegion = residencyArchiveSourceRegion.trim();
            const archiveRegion = residencyArchiveTargetRegion.trim();
            const archiveClass = residencyArchiveClass.trim();
            if (!sourceRegion || !archiveRegion || !archiveClass) {
              setResidencyArchiveFeedback(null);
              setResidencyArchiveError("Source Region、Archive Region、Archive Class 不能为空。");
              return;
            }
            if (sourceRegion === archiveRegion) {
              setResidencyArchiveFeedback(null);
              setResidencyArchiveError("Source Region 与 Archive Region 不能相同。");
              return;
            }
            setResidencyArchiveFeedback(null);
            setResidencyArchiveError(null);
            setResidencyArchiveDrafts((prev) => {
              const nextItem: ResidencyArchiveRegionPolicy = {
                tenantId: residencyPolicyQuery.data?.tenantId ?? "default",
                sourceRegion,
                archiveRegion,
                archiveClass,
                enabled: residencyArchiveEnabled,
                updatedAt: new Date().toISOString(),
              };
              return [...prev.filter((item) => item.sourceRegion !== sourceRegion), nextItem].sort(
                (a, b) => a.sourceRegion.localeCompare(b.sourceRegion),
              );
            });
            setResidencyArchiveSourceRegion("");
            setResidencyArchiveTargetRegion("");
            setResidencyArchiveClass("");
            setResidencyArchiveEnabled(true);
          }}
        >
          添加 / 覆盖归档策略
        </button>
        <button
          type="button"
          className="submit-button"
          disabled={saveResidencyArchivePoliciesMutation.isPending}
          onClick={() => {
            setResidencyArchiveFeedback(null);
            setResidencyArchiveError(null);
            saveResidencyArchivePoliciesMutation.mutate(residencyArchiveDrafts);
          }}
        >
          {saveResidencyArchivePoliciesMutation.isPending ? "保存中..." : "保存归档策略"}
        </button>
      </div>
      {residencyArchiveQuery.isLoading ? <p className="feedback info">归档策略加载中...</p> : null}
      {residencyArchiveQuery.isError ? (
        <p className="feedback error">归档策略加载失败：{toErrorMessage(residencyArchiveQuery.error)}</p>
      ) : null}
      {residencyArchiveFeedback ? <p className="feedback success">{residencyArchiveFeedback}</p> : null}
      {residencyArchiveError ? <p className="feedback error">{residencyArchiveError}</p> : null}
      <div className="table-wrapper">
        <table className="session-table">
          <thead>
            <tr>
              <th>Source Region</th>
              <th>Archive Region</th>
              <th>Archive Class</th>
              <th>Enabled</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {residencyArchiveDrafts.length === 0 ? (
              <tr>
                <td className="table-empty-cell" colSpan={5}>
                  暂无归档策略
                </td>
              </tr>
            ) : (
              residencyArchiveDrafts.map((item) => (
                <tr key={`archive-inline-row-${item.sourceRegion}`}>
                  <td>{item.sourceRegion}</td>
                  <td>{item.archiveRegion}</td>
                  <td>{item.archiveClass}</td>
                  <td>{item.enabled ? "true" : "false"}</td>
                  <td>
                    <button
                      type="button"
                      className="table-action"
                      onClick={() =>
                        setResidencyArchiveDrafts((prev) =>
                          prev.filter((current) => current.sourceRegion !== item.sourceRegion),
                        )
                      }
                    >
                      删除
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );

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
              onChange={(event) =>
                setWeeklyMetric(event.target.value as MetricKey)
              }
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

        {weeklySummaryQuery.isLoading ? (
          <p className="feedback info">周报摘要加载中...</p>
        ) : null}
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
                {weeklySummaryQuery.data.summary.tokens.toLocaleString(
                  "zh-CN",
                )}{" "}
                tokens / ${weeklySummaryQuery.data.summary.cost.toFixed(2)} /{" "}
                {weeklySummaryQuery.data.summary.sessions.toLocaleString(
                  "zh-CN",
                )}{" "}
                sessions
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

        {alertsQuery.isLoading ? (
          <p className="feedback info">告警加载中...</p>
        ) : null}
        {alertsQuery.isError ? (
          <p className="feedback error">
            告警加载失败：{toErrorMessage(alertsQuery.error)}
          </p>
        ) : null}
        {alertFeedback ? (
          <p className="feedback success">{alertFeedback}</p>
        ) : null}
        {updateAlertStatusMutation.isError ? (
          <p className="feedback error">
            告警状态更新失败：{toErrorMessage(updateAlertStatusMutation.error)}
          </p>
        ) : null}
        {retryAlertExternalLinkSyncMutation.isError ? (
          <p className="feedback error">
            外部联动重试失败：
            {toErrorMessage(retryAlertExternalLinkSyncMutation.error)}
          </p>
        ) : null}
        {alertOpsError ? <p className="feedback error">{alertOpsError}</p> : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>级别</th>
                <th>状态</th>
                <th>消息</th>
                <th>外部联动</th>
                <th>更新时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {alertItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={7}>
                    暂无告警
                  </td>
                </tr>
              ) : (
                alertItems.map((alert: AlertItem) => {
                  const isUpdating =
                    updateAlertStatusMutation.isPending &&
                    updateAlertStatusMutation.variables?.alertId === alert.id;
                  const retryableExternalLinks =
                    alert.externalLinks?.filter((link) =>
                      isAlertExternalLinkRetryable(alert.status, link),
                    ) ?? [];

                  return (
                    <tr key={alert.id}>
                      <td>{alert.id}</td>
                      <td>{alert.severity}</td>
                      <td>{alert.status}</td>
                      <td>{alert.message}</td>
	                      <td>
	                        {formatAlertExternalLinks(alert.externalLinks)}
	                      </td>
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
                            <span className="tiny-feedback tiny-feedback-success">
                              已完成
                            </span>
                          )}
                          {retryableExternalLinks.map((link) => {
                            const isRetrying =
                              retryAlertExternalLinkSyncMutation.isPending &&
                              retryAlertExternalLinkSyncMutation.variables
                                ?.alertId === alert.id &&
                              retryAlertExternalLinkSyncMutation.variables
                                ?.externalType === link.externalType &&
                              retryAlertExternalLinkSyncMutation.variables
                                ?.externalId === link.externalId;
                            return (
                              <button
                                key={`${alert.id}:${link.externalType}:${link.externalId}:retry`}
                                type="button"
                                className="table-action"
                                disabled={isUpdating || isRetrying}
                                onClick={() =>
                                  retryAlertExternalLinkSyncMutation.mutate({
                                    alertId: alert.id,
                                    externalType: link.externalType,
                                    externalId: link.externalId,
                                  })
                                }
                              >
                                重试同步 {link.externalType}:{link.externalId}
                              </button>
                            );
                          })}
                          {Array.isArray(alert.externalLinks) &&
                          alert.externalLinks.length > 0 ? (
                            <button
                              type="button"
                              className="table-action"
                              onClick={() => {
                                const nextAlertId =
                                  selectedAlertOpsAlertId === alert.id ? null : alert.id;
                                setSelectedAlertOpsAlertId(nextAlertId);
                                setAlertOpsError(null);
                                if (!nextAlertId) {
                                  setAlertOpsPayload(null);
                                  return;
                                }
                                void loadAlertExternalLinkOpsMutation.mutateAsync({
                                  alertId: alert.id,
                                  externalType: alertOpsExternalType || undefined,
                                  onlyFailed: alertOpsOnlyFailed,
                                });
                              }}
                            >
                              {selectedAlertOpsAlertId === alert.id
                                ? "收起联动运维"
                                : "查看联动运维"}
                            </button>
                          ) : null}
                        </div>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        <div className="governance-stack">
          <h3>失败外部联动视图</h3>
          <div className="filters-row">
            <label className="inline-field" htmlFor="alert-failure-alert-id-filter">
              alertId
              <input
                id="alert-failure-alert-id-filter"
                type="text"
                value={alertFailureAlertIdFilter}
                onChange={(event) => setAlertFailureAlertIdFilter(event.target.value)}
                placeholder="可选"
              />
            </label>
            <label
              className="inline-field"
              htmlFor="alert-failure-external-system-filter"
            >
              externalSystem
              <input
                id="alert-failure-external-system-filter"
                type="text"
                value={alertFailureExternalSystemFilter}
                onChange={(event) =>
                  setAlertFailureExternalSystemFilter(event.target.value)
                }
                placeholder="可选"
              />
            </label>
            <label className="inline-field" htmlFor="alert-failure-sync-state-filter">
              syncState
              <select
                id="alert-failure-sync-state-filter"
                value={alertFailureSyncStateFilter}
                onChange={(event) =>
                  setAlertFailureSyncStateFilter(
                    event.target.value as "" | "synced" | "pending" | "failed",
                  )
                }
              >
                <option value="">全部</option>
                <option value="failed">failed</option>
                <option value="pending">pending</option>
                <option value="synced">synced</option>
              </select>
            </label>
            <label className="inline-field" htmlFor="alert-failure-limit">
              limit
              <input
                id="alert-failure-limit"
                type="number"
                min={1}
                max={200}
                step={1}
                value={alertFailureLimit}
                onChange={(event) => setAlertFailureLimit(event.target.value)}
              />
            </label>
            <button
              type="button"
              className="table-action"
              disabled={loadAlertExternalLinkFailuresMutation.isPending}
              onClick={() => {
                const numericLimit = Number(alertFailureLimit);
                if (
                  alertFailureLimit.trim().length > 0 &&
                  (!Number.isInteger(numericLimit) ||
                    numericLimit < 1 ||
                    numericLimit > 200)
                ) {
                  setAlertFailurePayload(null);
                  setAlertFailureError("失败外部联动 limit 必须是 1 到 200 的整数。");
                  return;
                }
                setAlertFailureError(null);
                void loadAlertExternalLinkFailuresMutation.mutateAsync({
                  alertId: alertFailureAlertIdFilter.trim() || undefined,
                  externalSystem: alertFailureExternalSystemFilter.trim() || undefined,
                  syncState: alertFailureSyncStateFilter || undefined,
                  limit:
                    alertFailureLimit.trim().length > 0 ? numericLimit : undefined,
                });
              }}
            >
              {loadAlertExternalLinkFailuresMutation.isPending
                ? "加载中..."
                : "加载失败联动"}
            </button>
          </div>

          {alertFailureError ? (
            <p className="feedback error">{alertFailureError}</p>
          ) : null}

          {alertFailurePayload ? (
            <>
              <p className="feedback info">
                聚合：total={alertFailurePayload.summary.total} / pending=
                {alertFailurePayload.summary.pending} / failed=
                {alertFailurePayload.summary.failed}
              </p>
              <div className="table-wrapper">
                <table className="session-table">
                  <thead>
                    <tr>
                      <th>alertId</th>
                      <th>externalSystem</th>
                      <th>external object</th>
                      <th>syncState</th>
                      <th>publishStatus</th>
                      <th>failureStage</th>
                      <th>failureCode</th>
                      <th>retryable</th>
                      <th>updatedAt</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {alertFailurePayload.items.length === 0 ? (
                      <tr>
                        <td className="table-empty-cell" colSpan={11}>
                          暂无失败外部联动项
                        </td>
                      </tr>
                    ) : (
                      alertFailurePayload.items.map((item) => {
                        const isRetryingSingle =
                          retryAlertExternalLinkSyncMutation.isPending &&
                          retryAlertExternalLinkSyncMutation.variables?.alertId ===
                            item.alertId &&
                          retryAlertExternalLinkSyncMutation.variables?.externalType ===
                            item.externalType &&
                          retryAlertExternalLinkSyncMutation.variables?.externalId ===
                            item.externalId;
                        const isRetryingBatch =
                          retryAlertExternalLinkSyncBatchMutation.isPending &&
                          retryAlertExternalLinkSyncBatchMutation.variables?.alertId ===
                            item.alertId;
                        return (
                          <tr
                            key={`alert-failure-${item.alertId}-${item.id}`}
                          >
                            <td>{item.alertId ?? "--"}</td>
                            <td>{item.externalSystem}</td>
                            <td>
                              {item.externalType}:{item.externalId}
                            </td>
                            <td>{item.syncState}</td>
                            <td>{item.publishStatus ?? "--"}</td>
                            <td>{item.lastSyncFailureStage ?? "--"}</td>
                            <td>{item.lastSyncFailureCode ?? "--"}</td>
                            <td>{item.retryable ? "true" : "false"}</td>
                            <td>
                              {formatDateTime(item.updatedAt ?? item.lastSyncedAt)}
                            </td>
                            <td>
                              <div className="governance-action-row">
                                <button
                                  type="button"
                                  className="table-action"
                                  disabled={!item.retryable || isRetryingSingle}
                                  onClick={() => {
                                    if (!item.alertId) {
                                      return;
                                    }
                                    setAlertFailureError(null);
                                    retryAlertExternalLinkSyncMutation.mutate({
                                      alertId: item.alertId,
                                      externalType: item.externalType,
                                      externalId: item.externalId,
                                    });
                                  }}
                                >
                                  单条重试
                                </button>
                                <button
                                  type="button"
                                  className="table-action"
                                  disabled={!item.alertId || isRetryingBatch}
                                  onClick={() => {
                                    if (!item.alertId) {
                                      return;
                                    }
                                    setAlertFailureError(null);
                                    retryAlertExternalLinkSyncBatchMutation.mutate({
                                      alertId: item.alertId,
                                    });
                                  }}
                                >
                                  按告警批量重试
                                </button>
                              </div>
                            </td>
                          </tr>
                        );
                      })
                    )}
                  </tbody>
                </table>
              </div>
            </>
          ) : (
            <p className="feedback empty">尚未加载失败外部联动列表。</p>
          )}
        </div>

        <div className="governance-stack">
          <h3>失败审计报表</h3>
          <div className="filters-row">
            <label className="inline-field" htmlFor="failure-report-from">
              from
              <input
                id="failure-report-from"
                type="datetime-local"
                value={failureReportFrom}
                onChange={(event) => setFailureReportFrom(event.target.value)}
              />
            </label>
            <label className="inline-field" htmlFor="failure-report-to">
              to
              <input
                id="failure-report-to"
                type="datetime-local"
                value={failureReportTo}
                onChange={(event) => setFailureReportTo(event.target.value)}
              />
            </label>
            <label className="inline-field" htmlFor="failure-report-external-system">
              externalSystem
              <input
                id="failure-report-external-system"
                type="text"
                value={failureReportExternalSystem}
                onChange={(event) => setFailureReportExternalSystem(event.target.value)}
                placeholder="可选"
              />
            </label>
            <label className="inline-field" htmlFor="failure-report-stage">
              stage
              <input
                id="failure-report-stage"
                type="text"
                value={failureReportStage}
                onChange={(event) => setFailureReportStage(event.target.value)}
                placeholder="可选"
              />
            </label>
            <label className="inline-field" htmlFor="failure-report-action-type">
              actionType
              <select
                id="failure-report-action-type"
                value={failureReportActionType}
                onChange={(event) =>
                  setFailureReportActionType(
                    event.target.value as typeof failureReportActionType,
                  )
                }
              >
                <option value="">全部</option>
                <option value="retry_requested">retry_requested</option>
                <option value="retry_completed">retry_completed</option>
                <option value="retry_failed">retry_failed</option>
                <option value="dlq_queried">dlq_queried</option>
                <option value="dlq_replayed">dlq_replayed</option>
                <option value="recovery_job_created">recovery_job_created</option>
                <option value="recovery_job_completed">recovery_job_completed</option>
                <option value="recovery_job_failed">recovery_job_failed</option>
              </select>
            </label>
            <label className="inline-field" htmlFor="failure-report-limit">
              limit
              <input
                id="failure-report-limit"
                type="number"
                min={1}
                max={500}
                step={1}
                value={failureReportLimit}
                onChange={(event) => setFailureReportLimit(event.target.value)}
              />
            </label>
            <button
              type="button"
              className="table-action"
              disabled={loadIntegrationAlertFailureReportMutation.isPending}
              onClick={() => {
                const numericLimit = Number(failureReportLimit);
                if (
                  failureReportLimit.trim().length > 0 &&
                  (!Number.isInteger(numericLimit) ||
                    numericLimit < 1 ||
                    numericLimit > 500)
                ) {
                  setFailureReportPayload(null);
                  setFailureReportError("失败审计报表 limit 必须是 1 到 500 的整数。");
                  return;
                }
                setFailureReportError(null);
                void loadIntegrationAlertFailureReportMutation.mutateAsync({
                  from: failureReportFrom.trim() || undefined,
                  to: failureReportTo.trim() || undefined,
                  externalSystem: failureReportExternalSystem.trim() || undefined,
                  stage: failureReportStage.trim() || undefined,
                  actionType: failureReportActionType || undefined,
                  limit:
                    failureReportLimit.trim().length > 0 ? numericLimit : undefined,
                });
              }}
            >
              {loadIntegrationAlertFailureReportMutation.isPending
                ? "加载中..."
                : "加载失败审计报表"}
            </button>
          </div>

          {failureReportError ? (
            <p className="feedback error">{failureReportError}</p>
          ) : null}

          {failureReportPayload ? (
            <>
              <p className="feedback info">
                total={failureReportPayload.summary.totalEvents} / retryRequested=
                {failureReportPayload.summary.retryRequested} / retryFailed=
                {failureReportPayload.summary.retryFailed} / dlqReplayed=
                {failureReportPayload.summary.dlqReplayed} / recoveryJobsFailed=
                {failureReportPayload.summary.recoveryJobsFailed}
              </p>
              <div className="table-wrapper">
                <table className="session-table">
                  <thead>
                    <tr>
                      <th>occurredAt</th>
                      <th>action</th>
                      <th>alertId</th>
                      <th>externalSystem</th>
                      <th>stage</th>
                      <th>code</th>
                      <th>status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {failureReportPayload.items.length === 0 ? (
                      <tr>
                        <td className="table-empty-cell" colSpan={7}>
                          暂无失败审计事件
                        </td>
                      </tr>
                    ) : (
                      failureReportPayload.items.map((item) => (
                        <tr key={`${item.occurredAt}:${item.action}:${item.alertId ?? "--"}`}>
                          <td>{formatDateTime(item.occurredAt)}</td>
                          <td>{item.actionType}</td>
                          <td>{item.alertId ?? "--"}</td>
                          <td>{item.externalSystem ?? "--"}</td>
                          <td>{item.stage ?? "--"}</td>
                          <td>{item.code ?? "--"}</td>
                          <td>{item.status}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </>
          ) : (
            <p className="feedback empty">尚未加载失败审计报表。</p>
          )}
        </div>

        <div className="governance-stack">
          <h3>长期趋势/容量运维视图</h3>
          <div className="filters-row">
            <label className="inline-field" htmlFor="failure-trend-from">
              from
              <input
                id="failure-trend-from"
                type="datetime-local"
                value={failureTrendFrom}
                onChange={(event) => setFailureTrendFrom(event.target.value)}
              />
            </label>
            <label className="inline-field" htmlFor="failure-trend-to">
              to
              <input
                id="failure-trend-to"
                type="datetime-local"
                value={failureTrendTo}
                onChange={(event) => setFailureTrendTo(event.target.value)}
              />
            </label>
            <label className="inline-field" htmlFor="failure-trend-external-system">
              externalSystem
              <input
                id="failure-trend-external-system"
                type="text"
                value={failureTrendExternalSystem}
                onChange={(event) => setFailureTrendExternalSystem(event.target.value)}
                placeholder="可选"
              />
            </label>
            <label className="inline-field" htmlFor="failure-trend-stage">
              stage
              <input
                id="failure-trend-stage"
                type="text"
                value={failureTrendStage}
                onChange={(event) => setFailureTrendStage(event.target.value)}
                placeholder="可选"
              />
            </label>
            <label className="inline-field" htmlFor="failure-trend-action-type">
              actionType
              <select
                id="failure-trend-action-type"
                value={failureTrendActionType}
                onChange={(event) =>
                  setFailureTrendActionType(event.target.value as typeof failureTrendActionType)
                }
              >
                <option value="">全部</option>
                <option value="retry_requested">retry_requested</option>
                <option value="retry_completed">retry_completed</option>
                <option value="retry_failed">retry_failed</option>
                <option value="dlq_queried">dlq_queried</option>
                <option value="dlq_replayed">dlq_replayed</option>
                <option value="recovery_job_created">recovery_job_created</option>
                <option value="recovery_job_completed">recovery_job_completed</option>
                <option value="recovery_job_failed">recovery_job_failed</option>
              </select>
            </label>
            <label className="inline-field" htmlFor="failure-trend-top">
              top
              <input
                id="failure-trend-top"
                type="number"
                min={1}
                max={20}
                step={1}
                value={failureTrendTop}
                onChange={(event) => setFailureTrendTop(event.target.value)}
              />
            </label>
            <button
              type="button"
              className="table-action"
              disabled={loadIntegrationAlertFailureTrendMutation.isPending}
              onClick={() => {
                const numericTop = Number(failureTrendTop);
                if (
                  failureTrendTop.trim().length > 0 &&
                  (!Number.isInteger(numericTop) || numericTop < 1 || numericTop > 20)
                ) {
                  setFailureTrendPayload(null);
                  setFailureTrendError("长期趋势/容量运维视图 top 必须是 1 到 20 的整数。");
                  return;
                }
                setFailureTrendError(null);
                void loadIntegrationAlertFailureTrendMutation.mutateAsync({
                  from: failureTrendFrom.trim() || undefined,
                  to: failureTrendTo.trim() || undefined,
                  externalSystem: failureTrendExternalSystem.trim() || undefined,
                  stage: failureTrendStage.trim() || undefined,
                  actionType: failureTrendActionType || undefined,
                  top: failureTrendTop.trim().length > 0 ? numericTop : undefined,
                });
              }}
            >
              {loadIntegrationAlertFailureTrendMutation.isPending
                ? "加载中..."
                : "加载长期趋势/容量运维视图"}
            </button>
          </div>

          {failureTrendError ? (
            <p className="feedback error">{failureTrendError}</p>
          ) : null}

          {failureTrendPayload ? (
            <>
              <p className="feedback info">
                total={failureTrendPayload.summary.totalEvents} / failed=
                {failureTrendPayload.summary.failedEvents} / requested=
                {failureTrendPayload.summary.requestedEvents} / avg/day=
                {failureTrendPayload.summary.averageEventsPerDay} / peak=
                {failureTrendPayload.summary.peakDate ?? "--"}(
                {failureTrendPayload.summary.peakCount})
              </p>
              <div className="table-wrapper">
                <table className="session-table">
                  <thead>
                    <tr>
                      <th>date</th>
                      <th>total</th>
                      <th>failed</th>
                      <th>success</th>
                      <th>requested</th>
                      <th>alerts</th>
                      <th>retryFailed</th>
                      <th>recoveryCompleted</th>
                    </tr>
                  </thead>
                  <tbody>
                    {failureTrendPayload.daily.length === 0 ? (
                      <tr>
                        <td className="table-empty-cell" colSpan={8}>
                          暂无长期趋势数据
                        </td>
                      </tr>
                    ) : (
                      failureTrendPayload.daily.map((item) => (
                        <tr key={item.date}>
                          <td>{item.date}</td>
                          <td>{item.totalEvents}</td>
                          <td>{item.failedEvents}</td>
                          <td>{item.successEvents}</td>
                          <td>{item.requestedEvents}</td>
                          <td>{item.uniqueAlerts}</td>
                          <td>{item.retryFailed}</td>
                          <td>{item.recoveryJobsCompleted}</td>
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
                      <th>externalSystem</th>
                      <th>total</th>
                      <th>failed</th>
                      <th>success</th>
                      <th>requested</th>
                      <th>alerts</th>
                      <th>lastOccurredAt</th>
                    </tr>
                  </thead>
                  <tbody>
                    {failureTrendPayload.capacity.externalSystems.length === 0 ? (
                      <tr>
                        <td className="table-empty-cell" colSpan={7}>
                          暂无 externalSystem 容量桶
                        </td>
                      </tr>
                    ) : (
                      failureTrendPayload.capacity.externalSystems.map((item) => (
                        <tr key={`external-system-${item.name}`}>
                          <td>{item.name}</td>
                          <td>{item.totalEvents}</td>
                          <td>{item.failedEvents}</td>
                          <td>{item.successEvents}</td>
                          <td>{item.requestedEvents}</td>
                          <td>{item.uniqueAlerts}</td>
                          <td>{formatOptionalDateTime(item.lastOccurredAt ?? null)}</td>
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
                      <th>stage</th>
                      <th>total</th>
                      <th>failed</th>
                      <th>success</th>
                      <th>requested</th>
                      <th>alerts</th>
                      <th>lastOccurredAt</th>
                    </tr>
                  </thead>
                  <tbody>
                    {failureTrendPayload.capacity.stages.length === 0 ? (
                      <tr>
                        <td className="table-empty-cell" colSpan={7}>
                          暂无 stage 容量桶
                        </td>
                      </tr>
                    ) : (
                      failureTrendPayload.capacity.stages.map((item) => (
                        <tr key={`stage-${item.name}`}>
                          <td>{item.name}</td>
                          <td>{item.totalEvents}</td>
                          <td>{item.failedEvents}</td>
                          <td>{item.successEvents}</td>
                          <td>{item.requestedEvents}</td>
                          <td>{item.uniqueAlerts}</td>
                          <td>{formatOptionalDateTime(item.lastOccurredAt ?? null)}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </>
          ) : (
            <p className="feedback empty">尚未加载长期趋势/容量运维视图。</p>
          )}
        </div>

        <div className="governance-stack">
          <h3>Integration DLQ</h3>
          <div className="filters-row">
            <label className="inline-field" htmlFor="integration-dlq-event-type-filter">
              eventType
              <input
                id="integration-dlq-event-type-filter"
                type="text"
                value={integrationDlqEventTypeFilter}
                onChange={(event) => setIntegrationDlqEventTypeFilter(event.target.value)}
                placeholder="可选"
              />
            </label>
            <label className="inline-field" htmlFor="integration-dlq-channel-filter">
              channel
              <input
                id="integration-dlq-channel-filter"
                type="text"
                value={integrationDlqChannelFilter}
                onChange={(event) => setIntegrationDlqChannelFilter(event.target.value)}
                placeholder="可选"
              />
            </label>
            <label className="inline-field" htmlFor="integration-dlq-alert-id-filter">
              alertId
              <input
                id="integration-dlq-alert-id-filter"
                type="text"
                value={integrationDlqAlertIdFilter}
                onChange={(event) => setIntegrationDlqAlertIdFilter(event.target.value)}
                placeholder="可选"
              />
            </label>
            <label className="inline-field" htmlFor="integration-dlq-callback-id-filter">
              callbackId
              <input
                id="integration-dlq-callback-id-filter"
                type="text"
                value={integrationDlqCallbackIdFilter}
                onChange={(event) => setIntegrationDlqCallbackIdFilter(event.target.value)}
                placeholder="可选"
              />
            </label>
            <label className="inline-field" htmlFor="integration-dlq-limit">
              limit
              <input
                id="integration-dlq-limit"
                type="number"
                min={1}
                max={200}
                step={1}
                value={integrationDlqLimit}
                onChange={(event) => setIntegrationDlqLimit(event.target.value)}
              />
            </label>
            <button
              type="button"
              className="table-action"
              disabled={loadIntegrationDlqMessagesMutation.isPending}
              onClick={() => {
                const numericLimit = Number(integrationDlqLimit);
                if (
                  integrationDlqLimit.trim().length > 0 &&
                  (!Number.isInteger(numericLimit) ||
                    numericLimit < 1 ||
                    numericLimit > 200)
                ) {
                  setIntegrationDlqPayload(null);
                  setSelectedIntegrationDlqMessageIds([]);
                  setIntegrationDlqError("Integration DLQ limit 必须是 1 到 200 的整数。");
                  return;
                }
                setIntegrationDlqError(null);
                void loadIntegrationDlqMessagesMutation.mutateAsync({
                  eventType: integrationDlqEventTypeFilter.trim() || undefined,
                  channel: integrationDlqChannelFilter.trim() || undefined,
                  alertId: integrationDlqAlertIdFilter.trim() || undefined,
                  callbackId: integrationDlqCallbackIdFilter.trim() || undefined,
                  limit:
                    integrationDlqLimit.trim().length > 0
                      ? numericLimit
                      : undefined,
                });
              }}
            >
              {loadIntegrationDlqMessagesMutation.isPending
                ? "加载中..."
                : "加载 DLQ"}
            </button>
            <button
              type="button"
              className="table-action"
              disabled={
                createIntegrationDlqRecoveryJobMutation.isPending ||
                selectedIntegrationDlqMessageIds.length === 0
              }
              onClick={() => {
                setIntegrationDlqRecoveryError(null);
                void createIntegrationDlqRecoveryJobMutation.mutateAsync({
                  messageIds: selectedIntegrationDlqMessageIds,
                });
              }}
            >
              创建恢复批次
            </button>
            <button
              type="button"
              className="table-action"
              disabled={loadIntegrationDlqRecoveryJobsMutation.isPending}
              onClick={() => {
                setIntegrationDlqRecoveryError(null);
                void loadIntegrationDlqRecoveryJobsMutation.mutateAsync({
                  limit: 20,
                });
              }}
            >
              加载恢复批次
            </button>
          </div>

          {integrationDlqError ? (
            <p className="feedback error">{integrationDlqError}</p>
          ) : null}
          {integrationDlqRecoveryError ? (
            <p className="feedback error">{integrationDlqRecoveryError}</p>
          ) : null}

          {integrationDlqPayload ? (
            <>
              <p className="feedback info">
                共 {integrationDlqPayload.total} 条 DLQ 消息，当前展示{" "}
                {integrationDlqPayload.items.length} 条。
              </p>
              <div className="table-wrapper">
                <table className="session-table">
                  <thead>
                    <tr>
                      <th>选择</th>
                      <th>eventType</th>
                      <th>channel</th>
                      <th>alertId</th>
                      <th>callbackId</th>
                      <th>error</th>
                      <th>retryable</th>
                      <th>attempt</th>
                      <th>failedAt</th>
                    </tr>
                  </thead>
                  <tbody>
                    {integrationDlqPayload.items.length === 0 ? (
                      <tr>
                        <td className="table-empty-cell" colSpan={9}>
                          暂无 DLQ 消息
                        </td>
                      </tr>
                    ) : (
                      integrationDlqPayload.items.map((item) => (
                        <tr key={item.messageId}>
                          <td>
                            <input
                              type="checkbox"
                              checked={selectedIntegrationDlqMessageIds.includes(
                                item.messageId,
                              )}
                              onChange={(event) => {
                                setSelectedIntegrationDlqMessageIds((prev) =>
                                  event.target.checked
                                    ? [...prev, item.messageId]
                                    : prev.filter((value) => value !== item.messageId),
                                );
                              }}
                            />
                          </td>
                          <td>{item.eventType}</td>
                          <td>{item.channel ?? "--"}</td>
                          <td>{item.alertId ?? "--"}</td>
                          <td>{item.callbackId ?? "--"}</td>
                          <td>{item.error}</td>
                          <td>{item.retryable ? "true" : "false"}</td>
                          <td>{item.attempt}</td>
                          <td>{formatDateTime(item.failedAt)}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </>
          ) : (
            <p className="feedback empty">尚未加载 Integration DLQ 列表。</p>
          )}

          <div className="governance-stack">
            <h4>恢复批次</h4>
            <div className="filters-row">
              <label
                className="inline-field"
                htmlFor="integration-dlq-recovery-job-id"
              >
                jobId
                <input
                  id="integration-dlq-recovery-job-id"
                  type="text"
                  value={integrationDlqRecoveryJobId}
                  onChange={(event) =>
                    setIntegrationDlqRecoveryJobId(event.target.value)
                  }
                  placeholder="可选"
                />
              </label>
              <button
                type="button"
                className="table-action"
                disabled={loadIntegrationDlqRecoveryJobDetailMutation.isPending}
                onClick={() => {
                  const jobId = integrationDlqRecoveryJobId.trim();
                  if (!jobId) {
                    setIntegrationDlqRecoveryJobDetail(null);
                    setIntegrationDlqRecoveryError("恢复批次 jobId 不能为空。");
                    return;
                  }
                  setIntegrationDlqRecoveryError(null);
                  void loadIntegrationDlqRecoveryJobDetailMutation.mutateAsync(jobId);
                }}
              >
                查看恢复详情
              </button>
            </div>

            {integrationDlqRecoveryJobsPayload ? (
              <div className="table-wrapper">
                <table className="session-table">
                  <thead>
                    <tr>
                      <th>jobId</th>
                      <th>status</th>
                      <th>requestedAt</th>
                      <th>startedAt</th>
                      <th>finishedAt</th>
                      <th>total</th>
                      <th>replayed</th>
                      <th>failed</th>
                    </tr>
                  </thead>
                  <tbody>
                    {integrationDlqRecoveryJobsPayload.items.length === 0 ? (
                      <tr>
                        <td className="table-empty-cell" colSpan={8}>
                          暂无恢复批次
                        </td>
                      </tr>
                    ) : (
                      integrationDlqRecoveryJobsPayload.items.map((item) => (
                        <tr key={item.id}>
                          <td>{item.id}</td>
                          <td>{item.status}</td>
                          <td>{formatDateTime(item.requestedAt)}</td>
                          <td>{item.startedAt ? formatDateTime(item.startedAt) : "--"}</td>
                          <td>{item.finishedAt ? formatDateTime(item.finishedAt) : "--"}</td>
                          <td>{item.summary.total}</td>
                          <td>{item.summary.replayed}</td>
                          <td>{item.summary.failed}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="feedback empty">尚未加载恢复批次列表。</p>
            )}

            {integrationDlqRecoveryJobDetail ? (
              <>
                <p className="feedback info">
                  恢复批次 {integrationDlqRecoveryJobDetail.id}：
                  total={integrationDlqRecoveryJobDetail.summary.total} / replayed=
                  {integrationDlqRecoveryJobDetail.summary.replayed} / failed=
                  {integrationDlqRecoveryJobDetail.summary.failed}
                </p>
                <div className="table-wrapper">
                  <table className="session-table">
                    <thead>
                      <tr>
                        <th>messageId</th>
                        <th>status</th>
                        <th>error</th>
                      </tr>
                    </thead>
                    <tbody>
                      {integrationDlqRecoveryJobDetail.items.length === 0 ? (
                        <tr>
                          <td className="table-empty-cell" colSpan={3}>
                            暂无恢复明细
                          </td>
                        </tr>
                      ) : (
                        integrationDlqRecoveryJobDetail.items.map((item) => (
                          <tr key={`${integrationDlqRecoveryJobDetail.id}:${item.messageId}`}>
                            <td>{item.messageId}</td>
                            <td>{item.status}</td>
                            <td>{item.error ?? "--"}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </>
            ) : null}
          </div>
        </div>

        {selectedAlertOpsAlertId ? (
          <div className="governance-stack">
            <h3>外部联动运维视图</h3>
            <div className="filters-row">
              <label className="inline-field" htmlFor="alert-ops-external-type-filter">
                externalType
                <select
                  id="alert-ops-external-type-filter"
                  value={alertOpsExternalType}
                  onChange={(event) =>
                    setAlertOpsExternalType(
                      event.target.value as "" | "ticket" | "case" | "incident",
                    )
                  }
                >
                  <option value="">全部</option>
                  <option value="ticket">ticket</option>
                  <option value="case">case</option>
                  <option value="incident">incident</option>
                </select>
              </label>
              <label className="checkbox-field" htmlFor="alert-ops-only-failed">
                <input
                  id="alert-ops-only-failed"
                  type="checkbox"
                  checked={alertOpsOnlyFailed}
                  onChange={(event) => setAlertOpsOnlyFailed(event.target.checked)}
                />
                仅失败项
              </label>
              <button
                type="button"
                className="table-action"
                disabled={loadAlertExternalLinkOpsMutation.isPending}
                onClick={() => {
                  setAlertOpsError(null);
                  void loadAlertExternalLinkOpsMutation.mutateAsync({
                    alertId: selectedAlertOpsAlertId,
                    externalType: alertOpsExternalType || undefined,
                    onlyFailed: alertOpsOnlyFailed,
                  });
                }}
              >
                刷新联动运维
              </button>
              <button
                type="button"
                className="table-action"
                disabled={
                  retryAlertExternalLinkSyncBatchMutation.isPending ||
                  !alertOpsPayload ||
                  alertOpsPayload.summary.pending + alertOpsPayload.summary.failed === 0
                }
                onClick={() => {
                  setAlertOpsError(null);
                  void retryAlertExternalLinkSyncBatchMutation.mutateAsync({
                    alertId: selectedAlertOpsAlertId,
                    externalType: alertOpsExternalType || undefined,
                  });
                }}
              >
                批量重试当前告警
              </button>
            </div>

            {loadAlertExternalLinkOpsMutation.isPending ? (
              <p className="feedback info">外部联动运维视图加载中...</p>
            ) : null}

            {alertOpsPayload ? (
              <>
                <p className="feedback info">
                  聚合：total={alertOpsPayload.summary.total} / pending=
                  {alertOpsPayload.summary.pending} / failed=
                  {alertOpsPayload.summary.failed}
                </p>
                <div className="table-wrapper">
                  <table className="session-table">
                    <thead>
                      <tr>
                        <th>外部对象</th>
                        <th>syncState</th>
                        <th>当前状态</th>
                        <th>目标状态</th>
                        <th>失败归因</th>
                        <th>最近同步</th>
                      </tr>
                    </thead>
                    <tbody>
                      {alertOpsPayload.items.length === 0 ? (
                        <tr>
                          <td className="table-empty-cell" colSpan={7}>
                            暂无外部联动运维项
                          </td>
                        </tr>
                      ) : (
                        alertOpsPayload.items.map((item) => (
                          <tr key={`alert-ops-${item.id}`}>
                            <td>
                              {item.externalType}:{item.externalId}
                            </td>
                            <td>{item.syncState}</td>
                            <td>{item.externalStatus ?? "--"}</td>
                            <td>{item.pendingExternalStatus ?? "--"}</td>
                            <td>
                              {item.lastSyncFailureStage || item.lastSyncFailureCode
                                ? `${item.lastSyncFailureStage ?? "--"} / ${item.lastSyncFailureCode ?? "--"}`
                                : item.lastSyncError ?? item.publishError ?? "--"}
                            </td>
                            <td>{formatDateTime(item.lastSyncedAt)}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </>
            ) : null}
          </div>
        ) : null}
      </section>

      <section className="panel">
        <header>
          <h2>告警编排中心</h2>
          <p>规则编排、模拟与执行日志。</p>
        </header>
        <datalist id="orchestration-rule-id-options">
          {knownOrchestrationRuleIds.map((ruleId) => (
            <option key={ruleId} value={ruleId} />
          ))}
        </datalist>
        <datalist id="orchestration-source-id-options">
          {knownOrchestrationSourceIds.map((sourceId) => (
            <option key={sourceId} value={sourceId} />
          ))}
        </datalist>

        <div className="filters-row governance-inline-grid">
          <label
            className="inline-field"
            htmlFor="orchestration-rule-event-type-filter"
          >
            事件类型
            <select
              id="orchestration-rule-event-type-filter"
              value={orchestrationRuleEventTypeFilter}
              onChange={(event) =>
                setOrchestrationRuleEventTypeFilter(
                  event.target.value as AlertOrchestrationEventType | "",
                )
              }
            >
              <option value="">全部</option>
              {ALERT_ORCHESTRATION_EVENT_TYPE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-rule-enabled-filter"
          >
            enabled
            <select
              id="orchestration-rule-enabled-filter"
              value={orchestrationRuleEnabledFilter}
              onChange={(event) =>
                setOrchestrationRuleEnabledFilter(
                  event.target.value as "" | "true" | "false",
                )
              }
            >
              {ALERT_ORCHESTRATION_ENABLED_FILTER_OPTIONS.map((option) => (
                <option key={option.label} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-rule-severity-filter"
          >
            级别（规则筛选）
            <select
              id="orchestration-rule-severity-filter"
              value={orchestrationRuleSeverityFilter}
              onChange={(event) =>
                setOrchestrationRuleSeverityFilter(
                  event.target.value as AlertSeverity | "",
                )
              }
            >
              <option value="">全部</option>
              {ALERT_SEVERITY_FILTER_OPTIONS.filter(
                (option) => option.value !== "",
              ).map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="orchestration-rule-source-filter"
          >
            Source ID
            <input
              id="orchestration-rule-source-filter"
              type="text"
              list="orchestration-source-id-options"
              value={orchestrationRuleSourceIdFilter}
              onChange={(event) =>
                setOrchestrationRuleSourceIdFilter(event.target.value)
              }
              placeholder="可选"
            />
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={loadOrchestrationRulesMutation.isPending}
            onClick={() => {
              setOrchestrationFeedback(null);
              setOrchestrationError(null);
              loadOrchestrationRulesMutation.mutate(
                orchestrationRuleQueryInput,
              );
            }}
          >
            {loadOrchestrationRulesMutation.isPending
              ? "加载中..."
              : "加载编排规则"}
          </button>
        </div>

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="orchestration-rule-id">
            Rule ID（规则）
            <input
              id="orchestration-rule-id"
              type="text"
              list="orchestration-rule-id-options"
              value={orchestrationRuleId}
              onChange={(event) => setOrchestrationRuleId(event.target.value)}
              placeholder="例如：rule-alert-critical"
            />
          </label>

          <label className="inline-field" htmlFor="orchestration-rule-name">
            名称
            <input
              id="orchestration-rule-name"
              type="text"
              value={orchestrationRuleName}
              onChange={(event) => setOrchestrationRuleName(event.target.value)}
              placeholder="例如：critical 高频抑制"
            />
          </label>

          <label
            className="checkbox-field"
            htmlFor="orchestration-rule-enabled"
          >
            <input
              id="orchestration-rule-enabled"
              type="checkbox"
              checked={orchestrationRuleEnabled}
              onChange={(event) =>
                setOrchestrationRuleEnabled(event.target.checked)
              }
            />
            enabled
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-rule-event-type"
          >
            事件类型
            <select
              id="orchestration-rule-event-type"
              value={orchestrationRuleEventType}
              onChange={(event) =>
                setOrchestrationRuleEventType(
                  event.target.value as AlertOrchestrationEventType,
                )
              }
            >
              {ALERT_ORCHESTRATION_EVENT_TYPE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="inline-field" htmlFor="orchestration-rule-severity">
            级别（规则）
            <select
              id="orchestration-rule-severity"
              value={orchestrationRuleSeverity}
              onChange={(event) =>
                setOrchestrationRuleSeverity(
                  event.target.value as AlertSeverity | "",
                )
              }
            >
              <option value="">全部</option>
              {ALERT_SEVERITY_FILTER_OPTIONS.filter(
                (option) => option.value !== "",
              ).map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-rule-source-id"
          >
            Source ID
            <input
              id="orchestration-rule-source-id"
              type="text"
              list="orchestration-source-id-options"
              value={orchestrationRuleSourceId}
              onChange={(event) =>
                setOrchestrationRuleSourceId(event.target.value)
              }
              placeholder="可选"
            />
          </label>
        </div>

        <div className="filters-row governance-inline-grid">
          <label
            className="inline-field"
            htmlFor="orchestration-rule-dedupe-window"
          >
            去重窗口(s)
            <input
              id="orchestration-rule-dedupe-window"
              type="number"
              min={0}
              step={1}
              value={orchestrationRuleDedupeWindowSeconds}
              onChange={(event) =>
                setOrchestrationRuleDedupeWindowSeconds(event.target.value)
              }
            />
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-rule-suppression-window"
          >
            抑制窗口(s)
            <input
              id="orchestration-rule-suppression-window"
              type="number"
              min={0}
              step={1}
              value={orchestrationRuleSuppressionWindowSeconds}
              onChange={(event) =>
                setOrchestrationRuleSuppressionWindowSeconds(event.target.value)
              }
            />
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-rule-merge-window"
          >
            合并窗口(s)
            <input
              id="orchestration-rule-merge-window"
              type="number"
              min={0}
              step={1}
              value={orchestrationRuleMergeWindowSeconds}
              onChange={(event) =>
                setOrchestrationRuleMergeWindowSeconds(event.target.value)
              }
            />
          </label>

          <label className="inline-field" htmlFor="orchestration-rule-sla">
            SLA(分钟)
            <input
              id="orchestration-rule-sla"
              type="number"
              min={0}
              step={1}
              value={orchestrationRuleSlaMinutes}
              onChange={(event) =>
                setOrchestrationRuleSlaMinutes(event.target.value)
              }
              placeholder="可选"
            />
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="orchestration-rule-channels"
          >
            Channels（逗号分隔）
            <input
              id="orchestration-rule-channels"
              type="text"
              value={orchestrationRuleChannelsInput}
              onChange={(event) =>
                setOrchestrationRuleChannelsInput(event.target.value)
              }
              placeholder={ALERT_ORCHESTRATION_CHANNEL_OPTIONS.map(
                (option) => option.value,
              ).join(",")}
            />
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={upsertOrchestrationRuleMutation.isPending}
            onClick={() => {
              const normalizedRuleId = orchestrationRuleId.trim();
              const normalizedName = orchestrationRuleName.trim();
              const dedupeWindowSeconds = parseOptionalNonNegativeInteger(
                orchestrationRuleDedupeWindowSeconds,
              );
              const suppressionWindowSeconds = parseOptionalNonNegativeInteger(
                orchestrationRuleSuppressionWindowSeconds,
              );
              const mergeWindowSeconds = parseOptionalNonNegativeInteger(
                orchestrationRuleMergeWindowSeconds,
              );
              const slaMinutes = parseOptionalNonNegativeInteger(
                orchestrationRuleSlaMinutes,
              );
              if (!normalizedRuleId) {
                setOrchestrationFeedback(null);
                setOrchestrationError("Rule ID 不能为空。");
                return;
              }
              if (!normalizedName) {
                setOrchestrationFeedback(null);
                setOrchestrationError("规则名称不能为空。");
                return;
              }
              if (
                typeof dedupeWindowSeconds !== "number" ||
                typeof suppressionWindowSeconds !== "number" ||
                typeof mergeWindowSeconds !== "number"
              ) {
                setOrchestrationFeedback(null);
                setOrchestrationError("去重/抑制/合并窗口必须是非负整数。");
                return;
              }
              const rawChannels = orchestrationRuleChannelsInput
                .split(",")
                .map((item) => item.trim().toLowerCase())
                .filter(
                  (item, index, array) =>
                    item.length > 0 && array.indexOf(item) === index,
                );
              const invalidChannels = rawChannels.filter(
                (item) =>
                  !ALERT_ORCHESTRATION_CHANNEL_OPTIONS.some(
                    (option) => option.value === item,
                  ),
              );
              const channels = rawChannels.filter(
                (item): item is AlertOrchestrationChannel =>
                  ALERT_ORCHESTRATION_CHANNEL_OPTIONS.some(
                    (option) => option.value === item,
                  ),
              );
              if (invalidChannels.length > 0) {
                setOrchestrationFeedback(null);
                setOrchestrationError(
                  `存在不支持的 channels：${invalidChannels.join(",")}。可选值：${ALERT_ORCHESTRATION_CHANNEL_OPTIONS.map(
                    (option) => option.value,
                  ).join(",")}`,
                );
                return;
              }
              if (channels.length === 0) {
                setOrchestrationFeedback(null);
                setOrchestrationError("至少选择一个合法 channel。");
                return;
              }
              setOrchestrationFeedback(null);
              setOrchestrationError(null);
              upsertOrchestrationRuleMutation.mutate({
                ruleId: normalizedRuleId,
                input: {
                  name: normalizedName,
                  enabled: orchestrationRuleEnabled,
                  eventType: orchestrationRuleEventType,
                  severity: orchestrationRuleSeverity || undefined,
                  sourceId: orchestrationRuleSourceId.trim() || undefined,
                  dedupeWindowSeconds,
                  suppressionWindowSeconds,
                  mergeWindowSeconds,
                  slaMinutes,
                  channels,
                },
              });
            }}
          >
            {upsertOrchestrationRuleMutation.isPending
              ? "保存中..."
              : "保存编排规则"}
          </button>
        </div>

        <div className="filters-row governance-inline-grid">
          <label
            className="inline-field"
            htmlFor="orchestration-simulate-rule-id"
          >
            指定 Rule ID（可选）
            <input
              id="orchestration-simulate-rule-id"
              type="text"
              list="orchestration-rule-id-options"
              value={orchestrationSimulateRuleId}
              onChange={(event) =>
                setOrchestrationSimulateRuleId(event.target.value)
              }
              placeholder="可选"
            />
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-simulate-event-type"
          >
            事件类型
            <select
              id="orchestration-simulate-event-type"
              value={orchestrationSimulateEventType}
              onChange={(event) =>
                setOrchestrationSimulateEventType(
                  event.target.value as AlertOrchestrationEventType,
                )
              }
            >
              {ALERT_ORCHESTRATION_EVENT_TYPE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-simulate-alert-id"
          >
            Alert ID
            <input
              id="orchestration-simulate-alert-id"
              type="text"
              value={orchestrationSimulateAlertId}
              onChange={(event) =>
                setOrchestrationSimulateAlertId(event.target.value)
              }
              placeholder="可选"
            />
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-simulate-severity"
          >
            级别（模拟）
            <select
              id="orchestration-simulate-severity"
              value={orchestrationSimulateSeverity}
              onChange={(event) =>
                setOrchestrationSimulateSeverity(
                  event.target.value as AlertSeverity | "",
                )
              }
            >
              <option value="">全部</option>
              {ALERT_SEVERITY_FILTER_OPTIONS.filter(
                (option) => option.value !== "",
              ).map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-simulate-source-id"
          >
            Source ID
            <input
              id="orchestration-simulate-source-id"
              type="text"
              list="orchestration-source-id-options"
              value={orchestrationSimulateSourceId}
              onChange={(event) =>
                setOrchestrationSimulateSourceId(event.target.value)
              }
              placeholder="可选"
            />
          </label>

          <label
            className="checkbox-field"
            htmlFor="orchestration-simulate-dedupe-hit"
          >
            <input
              id="orchestration-simulate-dedupe-hit"
              type="checkbox"
              checked={orchestrationSimulateDedupeHit}
              onChange={(event) =>
                setOrchestrationSimulateDedupeHit(event.target.checked)
              }
            />
            dedupeHit
          </label>

          <label
            className="checkbox-field"
            htmlFor="orchestration-simulate-suppressed"
          >
            <input
              id="orchestration-simulate-suppressed"
              type="checkbox"
              checked={orchestrationSimulateSuppressed}
              onChange={(event) =>
                setOrchestrationSimulateSuppressed(event.target.checked)
              }
            />
            suppressed
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={simulateOrchestrationMutation.isPending}
            onClick={() => {
              setOrchestrationFeedback(null);
              setOrchestrationError(null);
              setOrchestrationSimulationResult(null);
              simulateOrchestrationMutation.mutate({
                ruleId: orchestrationSimulateRuleId.trim() || undefined,
                eventType: orchestrationSimulateEventType,
                alertId: orchestrationSimulateAlertId.trim() || undefined,
                severity: orchestrationSimulateSeverity || undefined,
                sourceId: orchestrationSimulateSourceId.trim() || undefined,
                dedupeHit: orchestrationSimulateDedupeHit,
                suppressed: orchestrationSimulateSuppressed,
              });
            }}
          >
            {simulateOrchestrationMutation.isPending ? "模拟中..." : "执行模拟"}
          </button>
        </div>

        <div className="filters-row governance-inline-grid">
          <label
            className="inline-field"
            htmlFor="orchestration-execution-rule-id-filter"
          >
            Rule ID（日志）
            <input
              id="orchestration-execution-rule-id-filter"
              type="text"
              list="orchestration-rule-id-options"
              value={orchestrationExecutionRuleIdFilter}
              onChange={(event) =>
                setOrchestrationExecutionRuleIdFilter(event.target.value)
              }
              placeholder="可选"
            />
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-event-type-filter"
          >
            事件类型
            <select
              id="orchestration-execution-event-type-filter"
              value={orchestrationExecutionEventTypeFilter}
              onChange={(event) =>
                setOrchestrationExecutionEventTypeFilter(
                  event.target.value as AlertOrchestrationEventType | "",
                )
              }
            >
              <option value="">全部</option>
              {ALERT_ORCHESTRATION_EVENT_TYPE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-severity-filter"
          >
            级别（日志）
            <select
              id="orchestration-execution-severity-filter"
              value={orchestrationExecutionSeverityFilter}
              onChange={(event) =>
                setOrchestrationExecutionSeverityFilter(
                  event.target.value as AlertSeverity | "",
                )
              }
            >
              <option value="">全部</option>
              {ALERT_SEVERITY_FILTER_OPTIONS.filter(
                (option) => option.value !== "",
              ).map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-source-id-filter"
          >
            Source ID
            <input
              id="orchestration-execution-source-id-filter"
              type="text"
              list="orchestration-source-id-options"
              value={orchestrationExecutionSourceIdFilter}
              onChange={(event) =>
                setOrchestrationExecutionSourceIdFilter(event.target.value)
              }
              placeholder="可选"
            />
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-dedupe-hit-filter"
          >
            dedupeHit
            <select
              id="orchestration-execution-dedupe-hit-filter"
              value={orchestrationExecutionDedupeHitFilter}
              onChange={(event) =>
                setOrchestrationExecutionDedupeHitFilter(
                  event.target.value as "" | "true" | "false",
                )
              }
            >
              {BOOLEAN_FILTER_OPTIONS.map((option) => (
                <option key={`dedupe-${option.label}`} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-suppressed-filter"
          >
            suppressed
            <select
              id="orchestration-execution-suppressed-filter"
              value={orchestrationExecutionSuppressedFilter}
              onChange={(event) =>
                setOrchestrationExecutionSuppressedFilter(
                  event.target.value as "" | "true" | "false",
                )
              }
            >
              {BOOLEAN_FILTER_OPTIONS.map((option) => (
                <option key={`suppressed-${option.label}`} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-dispatch-mode-filter"
          >
            mode
            <select
              id="orchestration-execution-dispatch-mode-filter"
              value={orchestrationExecutionDispatchModeFilter}
              onChange={(event) =>
                setOrchestrationExecutionDispatchModeFilter(
                  event.target.value as AlertOrchestrationDispatchMode | "",
                )
              }
            >
              {ALERT_ORCHESTRATION_DISPATCH_MODE_OPTIONS.map((option) => (
                <option
                  key={`dispatch-mode-${option.label}`}
                  value={option.value}
                >
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-conflict-filter"
          >
            conflict
            <select
              id="orchestration-execution-conflict-filter"
              value={orchestrationExecutionConflictFilter}
              onChange={(event) =>
                setOrchestrationExecutionConflictFilter(
                  event.target.value as "" | "true" | "false",
                )
              }
            >
              {CONFLICT_FILTER_OPTIONS.map((option) => (
                <option key={`conflict-${option.label}`} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-simulated-filter"
          >
            simulated
            <select
              id="orchestration-execution-simulated-filter"
              value={orchestrationExecutionSimulatedFilter}
              onChange={(event) =>
                setOrchestrationExecutionSimulatedFilter(
                  event.target.value as "" | "true" | "false",
                )
              }
            >
              {BOOLEAN_FILTER_OPTIONS.map((option) => (
                <option key={`simulated-${option.label}`} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-escalated-filter"
          >
            escalated
            <select
              id="orchestration-execution-escalated-filter"
              value={orchestrationExecutionEscalatedFilter}
              onChange={(event) =>
                setOrchestrationExecutionEscalatedFilter(
                  event.target.value as "" | "true" | "false",
                )
              }
            >
              {BOOLEAN_FILTER_OPTIONS.map((option) => (
                <option key={`escalated-${option.label}`} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-escalation-reason-filter"
          >
            escalationReason
            <select
              id="orchestration-execution-escalation-reason-filter"
              value={orchestrationExecutionEscalationReasonFilter}
              onChange={(event) =>
                setOrchestrationExecutionEscalationReasonFilter(
                  event.target.value as AlertOrchestrationEscalationReason | "",
                )
              }
            >
              {ALERT_ORCHESTRATION_ESCALATION_REASON_OPTIONS.map((option) => (
                <option
                  key={`escalation-reason-${option.label}`}
                  value={option.value}
                >
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-from"
          >
            from
            <input
              id="orchestration-execution-from"
              type="datetime-local"
              value={orchestrationExecutionFrom}
              onChange={(event) =>
                setOrchestrationExecutionFrom(event.target.value)
              }
            />
          </label>

          <label className="inline-field" htmlFor="orchestration-execution-to">
            to
            <input
              id="orchestration-execution-to"
              type="datetime-local"
              value={orchestrationExecutionTo}
              onChange={(event) =>
                setOrchestrationExecutionTo(event.target.value)
              }
            />
          </label>

          <label
            className="inline-field"
            htmlFor="orchestration-execution-limit"
          >
            limit
            <input
              id="orchestration-execution-limit"
              type="number"
              min={1}
              step={1}
              value={orchestrationExecutionLimit}
              onChange={(event) =>
                setOrchestrationExecutionLimit(event.target.value)
              }
            />
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={loadOrchestrationExecutionsMutation.isPending}
            onClick={() => {
              const fromTimestamp = orchestrationExecutionFrom
                ? Date.parse(orchestrationExecutionFrom)
                : null;
              const toTimestamp = orchestrationExecutionTo
                ? Date.parse(orchestrationExecutionTo)
                : null;
              if (
                fromTimestamp !== null &&
                toTimestamp !== null &&
                !Number.isNaN(fromTimestamp) &&
                !Number.isNaN(toTimestamp) &&
                fromTimestamp > toTimestamp
              ) {
                setOrchestrationFeedback(null);
                setOrchestrationError(
                  "执行日志筛选时间范围非法：from 不能晚于 to。",
                );
                return;
              }
              setOrchestrationFeedback(null);
              setOrchestrationError(null);
              loadOrchestrationExecutionsMutation.mutate(
                orchestrationExecutionQueryInput,
              );
            }}
          >
            {loadOrchestrationExecutionsMutation.isPending
              ? "加载中..."
              : "加载执行日志"}
          </button>
        </div>

        {orchestrationFeedback ? (
          <p className="feedback success">{orchestrationFeedback}</p>
        ) : null}
        {orchestrationError ? (
          <p className="feedback error">{orchestrationError}</p>
        ) : null}
        {hasLoadedOrchestrationExecutions ? (
          <section
            className="analytics-kpi-grid"
            aria-label="执行日志当前结果统计"
          >
            <article className="analytics-kpi-card">
              <h3>当前结果</h3>
              <strong>{orchestrationExecutionSummary.total}</strong>
            </article>
            <article className="analytics-kpi-card">
              <h3>rule</h3>
              <strong>{orchestrationExecutionSummary.ruleDispatches}</strong>
            </article>
            <article className="analytics-kpi-card">
              <h3>fallback</h3>
              <strong>
                {orchestrationExecutionSummary.fallbackDispatches}
              </strong>
            </article>
            <article className="analytics-kpi-card">
              <h3>冲突</h3>
              <strong>
                {orchestrationExecutionSummary.conflictExecutions}
              </strong>
            </article>
            <article className="analytics-kpi-card">
              <h3>dedupe</h3>
              <strong>{orchestrationExecutionSummary.dedupeHits}</strong>
            </article>
            <article className="analytics-kpi-card">
              <h3>suppressed</h3>
              <strong>
                {orchestrationExecutionSummary.suppressedExecutions}
              </strong>
            </article>
            <article className="analytics-kpi-card">
              <h3>simulated</h3>
              <strong>
                {orchestrationExecutionSummary.simulatedExecutions}
              </strong>
            </article>
            <article className="analytics-kpi-card">
              <h3>escalated</h3>
              <strong>
                {orchestrationExecutionSummary.escalatedExecutions}
              </strong>
            </article>
          </section>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>名称</th>
                <th>eventType</th>
                <th>severity</th>
                <th>sourceId</th>
                <th>enabled</th>
                <th>channels</th>
                <th>窗口配置</th>
                <th>更新时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {orchestrationRuleItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={10}>
                    {hasLoadedOrchestrationRules
                      ? "无匹配规则。"
                      : "尚未加载规则，请点击“加载编排规则”。"}
                  </td>
                </tr>
              ) : (
                orchestrationRuleItems.map((rule) => (
                  <tr key={rule.id}>
                    <td>{rule.id}</td>
                    <td>{rule.name}</td>
                    <td>{rule.eventType}</td>
                    <td>{rule.severity ?? "--"}</td>
                    <td>{rule.sourceId ?? "--"}</td>
                    <td>{rule.enabled ? "true" : "false"}</td>
                    <td>{rule.channels.join(",")}</td>
                    <td>
                      d={rule.dedupeWindowSeconds}s / s=
                      {rule.suppressionWindowSeconds}s / m=
                      {rule.mergeWindowSeconds}s / sla=
                      {typeof rule.slaMinutes === "number"
                        ? `${rule.slaMinutes}m`
                        : "--"}
                    </td>
                    <td>{formatDateTime(rule.updatedAt)}</td>
                    <td>
                      <button
                        type="button"
                        className="table-action"
                        onClick={() => {
                          setOrchestrationRuleId(rule.id);
                          setOrchestrationSimulateRuleId(rule.id);
                          setOrchestrationRuleName(rule.name);
                          setOrchestrationRuleEnabled(rule.enabled);
                          setOrchestrationRuleEventType(rule.eventType);
                          setOrchestrationRuleSeverity(rule.severity ?? "");
                          setOrchestrationRuleSourceId(rule.sourceId ?? "");
                          setOrchestrationRuleDedupeWindowSeconds(
                            String(rule.dedupeWindowSeconds),
                          );
                          setOrchestrationRuleSuppressionWindowSeconds(
                            String(rule.suppressionWindowSeconds),
                          );
                          setOrchestrationRuleMergeWindowSeconds(
                            String(rule.mergeWindowSeconds),
                          );
                          setOrchestrationRuleSlaMinutes(
                            typeof rule.slaMinutes === "number"
                              ? String(rule.slaMinutes)
                              : "",
                          );
                          setOrchestrationRuleChannelsInput(
                            rule.channels.join(","),
                          );
                        }}
                      >
                        载入
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {orchestrationSimulationResult ? (
          <div className="governance-stack">
            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>模拟结果</th>
                    <th>值</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>命中规则</td>
                    <td>{orchestrationSimulationResult.matchedRules.length}</td>
                  </tr>
                  <tr>
                    <td>冲突规则 ID</td>
                    <td>
                      {orchestrationSimulationResult.conflictRuleIds.length > 0
                        ? orchestrationSimulationResult.conflictRuleIds.join(
                            ",",
                          )
                        : "--"}
                    </td>
                  </tr>
                  <tr>
                    <td>执行日志条数</td>
                    <td>{orchestrationSimulationExecutions.length}</td>
                  </tr>
                </tbody>
              </table>
            </div>

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>命中规则 ID</th>
                    <th>名称</th>
                    <th>eventType</th>
                    <th>severity</th>
                    <th>sourceId</th>
                    <th>channels</th>
                    <th>冲突</th>
                  </tr>
                </thead>
                <tbody>
                  {orchestrationSimulationResult.matchedRules.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={7}>
                        本次模拟未命中规则
                      </td>
                    </tr>
                  ) : (
                    orchestrationSimulationResult.matchedRules.map((rule) => (
                      <tr key={`simulation-match-${rule.id}`}>
                        <td>{rule.id}</td>
                        <td>{rule.name}</td>
                        <td>{rule.eventType}</td>
                        <td>{rule.severity ?? "--"}</td>
                        <td>{rule.sourceId ?? "--"}</td>
                        <td>{rule.channels.join(",")}</td>
                        <td>
                          {simulationConflictRuleSet.has(rule.id) ? "是" : "否"}
                        </td>
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
                    <th>冲突规则 ID</th>
                    <th>名称</th>
                    <th>eventType</th>
                    <th>severity</th>
                    <th>sourceId</th>
                    <th>channels</th>
                  </tr>
                </thead>
                <tbody>
                  {simulationConflictRules.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={6}>
                        本次模拟无冲突
                      </td>
                    </tr>
                  ) : (
                    simulationConflictRules.map((item) => (
                      <tr key={`simulation-conflict-${item.ruleId}`}>
                        <td>{item.ruleId}</td>
                        <td>{item.rule?.name ?? "--"}</td>
                        <td>{item.rule?.eventType ?? "--"}</td>
                        <td>{item.rule?.severity ?? "--"}</td>
                        <td>{item.rule?.sourceId ?? "--"}</td>
                        <td>
                          {item.rule ? item.rule.channels.join(",") : "--"}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>groupBy</th>
                <th>value</th>
                <th>totalEvents</th>
                <th>passedEvents</th>
                <th>failedEvents</th>
                <th>passRate</th>
                <th>avgScore</th>
              </tr>
            </thead>
            <tbody>
              {qualityDailyGroups.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={7}>
                    暂无 externalSource 分组数据。
                  </td>
                </tr>
              ) : (
                qualityDailyGroups.map((item) => (
                  <tr key={`${item.groupBy}:${item.value}`}>
                    <td>{item.groupBy}</td>
                    <td>{item.value}</td>
                    <td>{item.totalEvents}</td>
                    <td>{item.passedEvents}</td>
                    <td>{item.failedEvents}</td>
                    <td>{item.passRate.toFixed(4)}</td>
                    <td>{item.avgScore.toFixed(2)}</td>
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
                <th>ruleId</th>
                <th>eventType</th>
                <th>mode</th>
                <th>escalated</th>
                <th>reason</th>
                <th>severity</th>
                <th>sourceId</th>
                <th>dedupeHit</th>
                <th>suppressed</th>
                <th>simulated</th>
                <th>conflicts</th>
                <th>channels</th>
                <th>targetChannels</th>
                <th>sla</th>
                <th>metadata</th>
                <th>创建时间</th>
              </tr>
            </thead>
            <tbody>
              {orchestrationExecutionItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={17}>
                    {hasLoadedOrchestrationExecutions
                      ? "无匹配执行日志。"
                      : "尚未加载执行日志，请点击“加载执行日志”。"}
                  </td>
                </tr>
              ) : (
                orchestrationExecutionItems.map((item) => (
                  <tr key={item.id}>
                    <td>{item.id}</td>
                    <td>{item.ruleId}</td>
                    <td>{item.eventType}</td>
                    <td>{item.dispatchMode}</td>
                    <td>{item.escalated ? "true" : "false"}</td>
                    <td>{item.escalationReason ?? "--"}</td>
                    <td>{item.severity ?? "--"}</td>
                    <td>{item.sourceId ?? "--"}</td>
                    <td>{item.dedupeHit ? "true" : "false"}</td>
                    <td>{item.suppressed ? "true" : "false"}</td>
                    <td>{item.simulated ? "true" : "false"}</td>
                    <td>
                      {item.conflictRuleIds.length > 0
                        ? item.conflictRuleIds.join(",")
                        : "--"}
                    </td>
                    <td>{item.channels.join(",")}</td>
                    <td>
                      {Array.isArray(item.escalationTargetChannels) &&
                      item.escalationTargetChannels.length > 0
                        ? item.escalationTargetChannels.join(",")
                        : "--"}
                    </td>
                    <td>
                      {typeof item.slaMinutes === "number"
                        ? `${item.slaMinutes}m`
                        : "--"}
                    </td>
                    <td>{formatCompactJson(item.metadata)}</td>
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
          <h2>数据主权与复制</h2>
          <p>主权策略 + 跨地域复制任务。</p>
        </header>

        <div className="filters-row">
          <label className="inline-field" htmlFor="residency-mode">
            模式
            <select
              id="residency-mode"
              value={residencyMode}
              onChange={(event) =>
                setResidencyMode(event.target.value as DataResidencyMode)
              }
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
          <label
            className="inline-field governance-wide-field"
            htmlFor="residency-replica-regions"
          >
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
              onChange={(event) =>
                setAllowCrossRegionTransfer(event.target.checked)
              }
            />
            允许跨地域传输
          </label>

          <label
            className="checkbox-field"
            htmlFor="residency-transfer-approval"
          >
            <input
              id="residency-transfer-approval"
              type="checkbox"
              checked={requireTransferApproval}
              onChange={(event) =>
                setRequireTransferApproval(event.target.checked)
              }
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
                .filter(
                  (region, index, list) =>
                    region.length > 0 && list.indexOf(region) === index,
                )
                .filter((region) => region !== normalizedPrimaryRegion);
              if (
                residencyMode === "active_active" &&
                replicaRegions.length === 0
              ) {
                setResidencyFeedback(null);
                setResidencyError("active_active 模式至少需要一个副本地域。");
                return;
              }
              if (
                residencyMode === "single_region" &&
                replicaRegions.length > 0
              ) {
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
          <p className="feedback error">
            地域列表加载失败：{toErrorMessage(residencyRegionsQuery.error)}
          </p>
        ) : null}
        {residencyPolicyQuery.isError ? (
          <p className="feedback error">
            主权策略加载失败：{toErrorMessage(residencyPolicyQuery.error)}
          </p>
        ) : null}
        {residencyFeedback ? (
          <p className="feedback success">{residencyFeedback}</p>
        ) : null}
        {residencyError ? (
          <p className="feedback error">{residencyError}</p>
        ) : null}

        {residencyExtensionPanel}

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="replication-source-region">
            复制源地域
            <select
              id="replication-source-region"
              value={replicationSourceRegion}
              onChange={(event) =>
                setReplicationSourceRegion(event.target.value)
              }
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
              onChange={(event) =>
                setReplicationTargetRegion(event.target.value)
              }
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
            {createReplicationJobMutation.isPending
              ? "创建中..."
              : "创建复制任务"}
          </button>
        </div>

        <div className="filters-row">
          <label className="inline-field" htmlFor="replication-status-filter">
            任务状态
            <select
              id="replication-status-filter"
              value={replicationStatusFilter}
              onChange={(event) =>
                setReplicationStatusFilter(
                  event.target.value as ReplicationJobStatus | "",
                )
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

        {replicationJobsQuery.isLoading ? (
          <p className="feedback info">复制任务加载中...</p>
        ) : null}
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
                  const canApprove = job.status === "pending";
                  const canCancel =
                    job.status === "pending" || job.status === "running";
                  const isApproving =
                    approveReplicationJobMutation.isPending &&
                    approveReplicationJobMutation.variables?.jobId === job.id;
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
                        {canApprove || canCancel ? (
                          <>
                            {canApprove ? (
                              <button
                                type="button"
                                className="table-action"
                                disabled={isApproving}
                                onClick={() => {
                                  const reason =
                                    typeof window !== "undefined"
                                      ? (window.prompt(
                                          "审批原因（可选）",
                                          "",
                                        ) ?? "")
                                      : "";
                                  approveReplicationJobMutation.mutate({
                                    jobId: job.id,
                                    reason: reason.trim() || undefined,
                                  });
                                }}
                              >
                                {isApproving ? "审批中..." : "审批"}
                              </button>
                            ) : null}
                            {canCancel ? (
                              <button
                                type="button"
                                className="table-action"
                                disabled={isCancelling}
                                onClick={() => {
                                  const reason =
                                    typeof window !== "undefined"
                                      ? (window.prompt(
                                          "取消原因（可选）",
                                          "",
                                        ) ?? "")
                                      : "";
                                  cancelReplicationJobMutation.mutate({
                                    jobId: job.id,
                                    reason: reason.trim() || undefined,
                                  });
                                }}
                              >
                                {isCancelling ? "取消中..." : "取消"}
                              </button>
                            ) : null}
                          </>
                        ) : (
                          <span className="tiny-feedback tiny-feedback-success">
                            不可操作
                          </span>
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
          <h2>Config Packages</h2>
          <p>配置包审批、发布与 watch/latest 预览。</p>
        </header>

        {systemConfigCreateFormSource ? (
          <p className="feedback info">
            {systemConfigCreateFormSource.mode === "clone"
              ? `当前基于配置包 ${systemConfigCreateFormSource.packageId} / v${systemConfigCreateFormSource.version} 克隆新包，请修改 Version 后再创建。`
              : `创建表单已载入配置包 ${systemConfigCreateFormSource.packageId} / v${systemConfigCreateFormSource.version}。`}
          </p>
        ) : null}

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="system-config-create-version">
            Version
            <input
              id="system-config-create-version"
              type="text"
              value={systemConfigCreateVersion}
              onChange={(event) => setSystemConfigCreateVersion(event.target.value)}
              placeholder="例如：config-v2"
            />
          </label>
          <label className="inline-field" htmlFor="system-config-create-issued-at">
            Issued At
            <input
              id="system-config-create-issued-at"
              type="text"
              value={systemConfigCreateIssuedAt}
              onChange={(event) => setSystemConfigCreateIssuedAt(event.target.value)}
              placeholder="可选，ISO 时间"
            />
          </label>
          <label className="inline-field" htmlFor="system-config-create-signature-status">
            Signature Status
            <input
              id="system-config-create-signature-status"
              type="text"
              value={systemConfigCreateSignatureStatus}
              onChange={(event) =>
                setSystemConfigCreateSignatureStatus(event.target.value)
              }
              placeholder="unknown / verified"
            />
          </label>
          <label className="checkbox-field" htmlFor="system-config-create-requires-approval">
            <input
              id="system-config-create-requires-approval"
              type="checkbox"
              checked={systemConfigCreateRequiresApproval}
              onChange={(event) => {
                const checked = event.target.checked;
                setSystemConfigCreateRequiresApproval(checked);
                setSystemConfigCreateRequiredApprovals(checked ? 1 : 0);
              }}
            />
            Requires Approval
          </label>
          <label className="inline-field" htmlFor="system-config-create-required-approvals">
            Required Approvals
            <select
              id="system-config-create-required-approvals"
              value={String(systemConfigCreateRequiredApprovals)}
              onChange={(event) =>
                setSystemConfigCreateRequiredApprovals(
                  Number(event.target.value) === 2
                    ? 2
                    : Number(event.target.value) === 1
                      ? 1
                      : 0,
                )
              }
              disabled={!systemConfigCreateRequiresApproval}
            >
              <option value="0">0</option>
              <option value="1">1</option>
              <option value="2">2</option>
            </select>
          </label>
          <label className="inline-field" htmlFor="system-config-create-agent-ids">
            Agent IDs
            <input
              id="system-config-create-agent-ids"
              type="text"
              value={systemConfigCreateAgentIds}
              onChange={(event) => setSystemConfigCreateAgentIds(event.target.value)}
              placeholder="逗号分隔"
            />
          </label>
          <label className="inline-field" htmlFor="system-config-create-device-ids">
            Device IDs
            <input
              id="system-config-create-device-ids"
              type="text"
              value={systemConfigCreateDeviceIds}
              onChange={(event) => setSystemConfigCreateDeviceIds(event.target.value)}
              placeholder="逗号分隔"
            />
          </label>
          <label className="inline-field" htmlFor="system-config-create-channels">
            Channels
            <input
              id="system-config-create-channels"
              type="text"
              value={systemConfigCreateChannels}
              onChange={(event) => setSystemConfigCreateChannels(event.target.value)}
              placeholder="逗号分隔"
            />
          </label>
          <label className="inline-field" htmlFor="system-config-create-hostnames">
            Hostnames
            <input
              id="system-config-create-hostnames"
              type="text"
              value={systemConfigCreateHostnames}
              onChange={(event) => setSystemConfigCreateHostnames(event.target.value)}
              placeholder="逗号分隔"
            />
          </label>
        </div>

        <div className="filters-row">
          <label
            className="inline-field governance-wide-field"
            htmlFor="system-config-create-payload-json"
          >
            Payload JSON
            <textarea
              id="system-config-create-payload-json"
              value={systemConfigCreatePayloadJson}
              onChange={(event) =>
                setSystemConfigCreatePayloadJson(event.target.value)
              }
              rows={6}
              placeholder='{"mode":"observe"}'
            />
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={createSystemConfigPackageMutation.isPending}
            onClick={() => {
              const version = systemConfigCreateVersion.trim();
              if (!version) {
                setSystemConfigFeedback(null);
                setSystemConfigError("Version 不能为空。");
                return;
              }
              if (
                systemConfigCreateFormSource?.mode === "clone" &&
                version === systemConfigCreateFormSource.version
              ) {
                setSystemConfigFeedback(null);
                setSystemConfigError("克隆为新包时 Version 必须不同。");
                return;
              }
              let payload: Record<string, unknown>;
              try {
                const parsed = JSON.parse(systemConfigCreatePayloadJson);
                if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
                  setSystemConfigFeedback(null);
                  setSystemConfigError("Payload JSON 必须是对象。");
                  return;
                }
                payload = parsed as Record<string, unknown>;
              } catch (error) {
                setSystemConfigFeedback(null);
                setSystemConfigError(`Payload JSON 非法：${toErrorMessage(error)}`);
                return;
              }
              const targetSelectors = {
                ...(parseDistinctCommaSeparatedList(systemConfigCreateAgentIds).length > 0
                  ? { agentIds: parseDistinctCommaSeparatedList(systemConfigCreateAgentIds) }
                  : {}),
                ...(parseDistinctCommaSeparatedList(systemConfigCreateDeviceIds).length > 0
                  ? {
                      deviceIds: parseDistinctCommaSeparatedList(
                        systemConfigCreateDeviceIds,
                      ),
                    }
                  : {}),
                ...(parseDistinctCommaSeparatedList(systemConfigCreateChannels).length > 0
                  ? {
                      channels: parseDistinctCommaSeparatedList(
                        systemConfigCreateChannels,
                      ),
                    }
                  : {}),
                ...(parseDistinctCommaSeparatedList(systemConfigCreateHostnames).length > 0
                  ? {
                      hostnames: parseDistinctCommaSeparatedList(
                        systemConfigCreateHostnames,
                      ),
                    }
                  : {}),
              };
              setSystemConfigFeedback(null);
              setSystemConfigError(null);
              createSystemConfigPackageMutation.mutate({
                version,
                issuedAt: systemConfigCreateIssuedAt.trim() || undefined,
                signatureStatus: systemConfigCreateSignatureStatus.trim() || undefined,
                requiresApproval: systemConfigCreateRequiresApproval,
                requiredApprovals: systemConfigCreateRequiresApproval
                  ? systemConfigCreateRequiredApprovals === 2
                    ? 2
                    : 1
                  : 0,
                targetSelectors,
                payload,
              });
            }}
          >
            {createSystemConfigPackageMutation.isPending ? "创建中..." : "创建配置包"}
          </button>
        </div>

        {systemConfigPackagesQuery.isLoading ? (
          <p className="feedback info">配置包加载中...</p>
        ) : null}
        {systemConfigPackagesQuery.isError ? (
          <p className="feedback error">
            配置包加载失败：{toErrorMessage(systemConfigPackagesQuery.error)}
          </p>
        ) : null}
        {systemConfigFeedback ? (
          <p className="feedback success">{systemConfigFeedback}</p>
        ) : null}
        {systemConfigError ? (
          <p className="feedback error">{systemConfigError}</p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Package ID</th>
                <th>Version</th>
                <th>审批开关</th>
                <th>审批门槛</th>
                <th>已发布</th>
                <th>发布时间</th>
                <th>签名状态</th>
                <th>Target Selectors</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {systemConfigPackageItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={9}>
                    暂无配置包
                  </td>
                </tr>
              ) : (
                systemConfigPackageItems.map((item) => (
                  <tr
                    key={item.packageId}
                    className={
                      selectedSystemConfigPackageId === item.packageId
                        ? "is-selected-row"
                        : ""
                    }
                  >
                    <td>{item.packageId}</td>
                    <td>{item.version}</td>
                    <td>{item.requiresApproval ? "是" : "否"}</td>
                    <td>{item.requiredApprovals}</td>
                    <td>{item.isPublished ? "是" : "否"}</td>
                    <td>{item.publishedAt ? formatDateTime(item.publishedAt) : "--"}</td>
                    <td>{item.signatureStatus}</td>
                    <td>{formatSystemConfigTargetSelectors(item.targetSelectors)}</td>
                    <td>
                      <button
                        type="button"
                        className="table-action"
                        onClick={() => setSelectedSystemConfigPackageId(item.packageId)}
                      >
                        {selectedSystemConfigPackageId === item.packageId ? "已选中" : "选中"}
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {selectedSystemConfigPackage ? (
          <>
            <p className="feedback info">
              当前配置包：{selectedSystemConfigPackage.packageId} / v
              {selectedSystemConfigPackage.version} / selectors：
              {formatSystemConfigTargetSelectors(
                selectedSystemConfigPackage.targetSelectors,
              )}
            </p>

            <div className="governance-action-row">
              <button
                type="button"
                className="table-action"
                onClick={() => {
                  hydrateSystemConfigCreateForm(selectedSystemConfigPackage, "loaded");
                  setSystemConfigFeedback(
                    `已将配置包 ${selectedSystemConfigPackage.packageId} 载入创建表单。`,
                  );
                  setSystemConfigError(null);
                }}
              >
                载入到表单
              </button>
              <button
                type="button"
                className="table-action"
                onClick={() => {
                  hydrateSystemConfigCreateForm(selectedSystemConfigPackage, "clone");
                  setSystemConfigFeedback(
                    `已基于配置包 ${selectedSystemConfigPackage.packageId} 回填表单，请修改 Version 后创建新包。`,
                  );
                  setSystemConfigError(null);
                }}
              >
                克隆为新包
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="system-config-approval-decision"
              >
                审批决策
                <select
                  id="system-config-approval-decision"
                  value={systemConfigApprovalDecision}
                  onChange={(event) =>
                    setSystemConfigApprovalDecision(
                      event.target.value as SystemConfigPackageApprovalDecision,
                    )
                  }
                >
                  <option value="approved">approved</option>
                  <option value="rejected">rejected</option>
                </select>
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="system-config-approval-comment"
              >
                审批意见
                <input
                  id="system-config-approval-comment"
                  type="text"
                  value={systemConfigApprovalComment}
                  onChange={(event) =>
                    setSystemConfigApprovalComment(event.target.value)
                  }
                  placeholder="可选"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={createSystemConfigPackageApprovalMutation.isPending}
                onClick={() => {
                  setSystemConfigFeedback(null);
                  setSystemConfigError(null);
                  createSystemConfigPackageApprovalMutation.mutate({
                    packageId: selectedSystemConfigPackage.packageId,
                    decision: systemConfigApprovalDecision,
                    comment: systemConfigApprovalComment.trim() || undefined,
                  });
                }}
              >
                {createSystemConfigPackageApprovalMutation.isPending
                  ? "提交中..."
                  : "提交配置审批"}
              </button>

              <button
                type="button"
                className="submit-button"
                disabled={publishSystemConfigPackageMutation.isPending}
                onClick={() => {
                  setSystemConfigFeedback(null);
                  setSystemConfigError(null);
                  publishSystemConfigPackageMutation.mutate({
                    packageId: selectedSystemConfigPackage.packageId,
                  });
                }}
              >
                {publishSystemConfigPackageMutation.isPending ? "发布中..." : "发布配置包"}
              </button>
            </div>

            {systemConfigPackageApprovalsQuery.isLoading ? (
              <p className="feedback info">配置审批记录加载中...</p>
            ) : null}
            {systemConfigPackageApprovalsQuery.isError ? (
              <p className="feedback error">
                配置审批记录加载失败：
                {toErrorMessage(systemConfigPackageApprovalsQuery.error)}
              </p>
            ) : null}

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>Approval ID</th>
                    <th>Version</th>
                    <th>Decision</th>
                    <th>Approver</th>
                    <th>Comment</th>
                    <th>Updated At</th>
                  </tr>
                </thead>
                <tbody>
                  {systemConfigApprovalItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={6}>
                        暂无审批记录
                      </td>
                    </tr>
                  ) : (
                    systemConfigApprovalItems.map((item) => (
                      <tr key={item.approvalId}>
                        <td>{item.approvalId}</td>
                        <td>{item.version}</td>
                        <td>{item.decision}</td>
                        <td>{item.approverUserId}</td>
                        <td>{item.comment ?? "--"}</td>
                        <td>{formatDateTime(item.updatedAt)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="system-config-watch-agent-id"
              >
                Watch Agent ID
                <input
                  id="system-config-watch-agent-id"
                  type="text"
                  value={systemConfigWatchAgentId}
                  onChange={(event) => setSystemConfigWatchAgentId(event.target.value)}
                  placeholder="可选"
                />
              </label>
              <label
                className="inline-field"
                htmlFor="system-config-watch-device-id"
              >
                Watch Device ID
                <input
                  id="system-config-watch-device-id"
                  type="text"
                  value={systemConfigWatchDeviceId}
                  onChange={(event) => setSystemConfigWatchDeviceId(event.target.value)}
                  placeholder="可选"
                />
              </label>
              <label
                className="inline-field"
                htmlFor="system-config-watch-channel"
              >
                Watch Channel
                <input
                  id="system-config-watch-channel"
                  type="text"
                  value={systemConfigWatchChannel}
                  onChange={(event) => setSystemConfigWatchChannel(event.target.value)}
                  placeholder="例如：stable"
                />
              </label>
              <label
                className="inline-field governance-wide-field"
                htmlFor="system-config-watch-hostname"
              >
                Watch Hostname
                <input
                  id="system-config-watch-hostname"
                  type="text"
                  value={systemConfigWatchHostname}
                  onChange={(event) => setSystemConfigWatchHostname(event.target.value)}
                  placeholder="可选"
                />
              </label>
              <button
                type="button"
                className="submit-button"
                disabled={fetchSystemConfigWatchLatestMutation.isPending}
                onClick={() => {
                  setSystemConfigFeedback(null);
                  setSystemConfigError(null);
                  fetchSystemConfigWatchLatestMutation.mutate();
                }}
              >
                {fetchSystemConfigWatchLatestMutation.isPending
                  ? "查询中..."
                  : "查询 watch/latest"}
              </button>
            </div>

            <p className="feedback info">
              {hasLoadedSystemConfigWatch
                ? systemConfigWatchResult
                  ? `watch/latest 命中：${systemConfigWatchResult.packageId} / v${systemConfigWatchResult.version}`
                  : "watch/latest 未命中任何已发布配置包。"
                : "可按 agent/device/channel/hostname 预览 watch/latest 命中结果。"}
            </p>
          </>
        ) : (
          <p className="feedback empty">请选择一个配置包查看审批和发布详情。</p>
        )}
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
              onChange={(event) =>
                setRuleStatusFilter(
                  event.target.value as RuleLifecycleStatus | "",
                )
              }
            >
              {RULE_STATUS_FILTER_OPTIONS.map((option) => (
                <option key={option.value || "all"} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="rule-keyword"
          >
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

          <label
            className="inline-field governance-wide-field"
            htmlFor="rule-description"
          >
            说明
            <input
              id="rule-description"
              type="text"
              value={ruleDescription}
              onChange={(event) => setRuleDescription(event.target.value)}
              placeholder="可选"
            />
          </label>

          <label className="inline-field" htmlFor="rule-required-approvals">
            审批要求
            <select
              id="rule-required-approvals"
              value={String(ruleRequiredApprovals)}
              onChange={(event) =>
                setRuleRequiredApprovals(
                  event.target.value === "2" ? 2 : 1,
                )
              }
            >
              <option value="1">单人审批</option>
              <option value="2">双人审批</option>
            </select>
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="rule-scope-organizations"
          >
            Organizations（逗号分隔）
            <input
              id="rule-scope-organizations"
              type="text"
              value={ruleScopeOrganizations}
              onChange={(event) =>
                setRuleScopeOrganizations(event.target.value)
              }
              placeholder="可选"
            />
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="rule-scope-projects"
          >
            Projects（逗号分隔）
            <input
              id="rule-scope-projects"
              type="text"
              value={ruleScopeProjects}
              onChange={(event) => setRuleScopeProjects(event.target.value)}
              placeholder="可选"
            />
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="rule-scope-clients"
          >
            Clients（逗号分隔）
            <input
              id="rule-scope-clients"
              type="text"
              value={ruleScopeClients}
              onChange={(event) => setRuleScopeClients(event.target.value)}
              placeholder="可选"
            />
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={createRuleAssetMutation.isPending}
            onClick={() => {
              const name = ruleName.trim();
              const scopeBinding = buildRuleScopeBindingInput({
                organizations: ruleScopeOrganizations,
                projects: ruleScopeProjects,
                clients: ruleScopeClients,
              });
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
                requiredApprovals: ruleRequiredApprovals,
                scopeBinding,
              });
            }}
          >
            {createRuleAssetMutation.isPending ? "创建中..." : "创建规则资产"}
          </button>
        </div>

        {ruleAssetsQuery.isLoading ? (
          <p className="feedback info">规则资产加载中...</p>
        ) : null}
        {ruleAssetsQuery.isError ? (
          <p className="feedback error">
            规则资产加载失败：{toErrorMessage(ruleAssetsQuery.error)}
          </p>
        ) : null}
        {ruleFeedback ? (
          <p className="feedback success">{ruleFeedback}</p>
        ) : null}
        {ruleError ? <p className="feedback error">{ruleError}</p> : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>名称</th>
                <th>状态</th>
                <th>审批要求</th>
                <th>Scope Binding</th>
                <th>最新版本</th>
                <th>发布版本</th>
                <th>更新时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {ruleItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={9}>
                    暂无规则资产
                  </td>
                </tr>
              ) : (
                ruleItems.map((asset) => {
                  const isSelected = selectedRuleAssetId === asset.id;
                  return (
                    <tr
                      key={asset.id}
                      className={isSelected ? "is-selected-row" : ""}
                    >
                      <td>{asset.id}</td>
                      <td>{asset.name}</td>
                      <td>{asset.status}</td>
                      <td>{formatRuleRequiredApprovals(asset.requiredApprovals)}</td>
                      <td>{formatRuleScopeBinding(asset.scopeBinding)}</td>
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
                              const latestVersionText = String(
                                asset.latestVersion,
                              );
                              setRulePublishVersion(latestVersionText);
                              setRuleRollbackVersion(latestVersionText);
                              setRuleApprovalVersion(latestVersionText);
                              return;
                            }
                            setRulePublishVersion("");
                            setRuleRollbackVersion("");
                            setRuleApprovalVersion("");
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
            <p className="feedback info">
              当前 Scope Binding：
              {formatRuleScopeBinding(selectedRuleAsset.scopeBinding)}
            </p>
            <p className="feedback info">
              当前审批要求：
              {formatRuleRequiredApprovals(selectedRuleAsset.requiredApprovals)}
              {currentPublishApprovalSummary
                ? `；发布版本 v${rulePublishVersion || "--"} 已批准 ${currentPublishApprovalSummary.approved} 人`
                : ""}
            </p>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field governance-wide-field"
                htmlFor="rule-version-content"
              >
                新版本内容
                <input
                  id="rule-version-content"
                  type="text"
                  value={ruleVersionContent}
                  onChange={(event) =>
                    setRuleVersionContent(event.target.value)
                  }
                  placeholder="例如：deny tool=github.delete_repo when risk=high"
                />
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="rule-version-changelog"
              >
                变更说明
                <input
                  id="rule-version-changelog"
                  type="text"
                  value={ruleVersionChangelog}
                  onChange={(event) =>
                    setRuleVersionChangelog(event.target.value)
                  }
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
                {createRuleAssetVersionMutation.isPending
                  ? "创建中..."
                  : "创建版本"}
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
                  onChange={(event) =>
                    setRulePublishVersion(event.target.value)
                  }
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
                  publishRuleAssetMutation.mutate({
                    assetId: selectedRuleAsset.id,
                    version,
                  });
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
                  onChange={(event) =>
                    setRuleRollbackVersion(event.target.value)
                  }
                  placeholder="例如：1"
                />
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="rule-rollback-reason"
              >
                回滚原因
                <input
                  id="rule-rollback-reason"
                  type="text"
                  value={ruleRollbackReason}
                  onChange={(event) =>
                    setRuleRollbackReason(event.target.value)
                  }
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
                  onChange={(event) =>
                    setRuleApprovalVersion(event.target.value)
                  }
                  placeholder="例如：1"
                />
              </label>

              <label className="inline-field" htmlFor="rule-approval-decision">
                审批决策
                <select
                  id="rule-approval-decision"
                  value={ruleApprovalDecision}
                  onChange={(event) =>
                    setRuleApprovalDecision(
                      event.target.value as RuleApprovalDecision,
                    )
                  }
                >
                  {RULE_APPROVAL_DECISION_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="rule-approval-reason"
              >
                审批意见
                <input
                  id="rule-approval-reason"
                  type="text"
                  value={ruleApprovalReason}
                  onChange={(event) =>
                    setRuleApprovalReason(event.target.value)
                  }
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
                {createRuleApprovalMutation.isPending
                  ? "提交中..."
                  : "提交审批"}
              </button>
            </div>

            {ruleVersionsQuery.isLoading ? (
              <p className="feedback info">版本列表加载中...</p>
            ) : null}
            {ruleVersionsQuery.isError ? (
              <p className="feedback error">
                版本列表加载失败：{toErrorMessage(ruleVersionsQuery.error)}
              </p>
            ) : null}
            {ruleApprovalsQuery.isLoading ? (
              <p className="feedback info">审批记录加载中...</p>
            ) : null}
            {ruleApprovalsQuery.isError ? (
              <p className="feedback error">
                审批记录加载失败：{toErrorMessage(ruleApprovalsQuery.error)}
              </p>
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

            <div className="filters-row governance-inline-grid">
              <label className="inline-field" htmlFor="rule-diff-from-version">
                Diff 起始版本
                <input
                  id="rule-diff-from-version"
                  type="number"
                  min={1}
                  step={1}
                  value={ruleDiffFromVersion}
                  onChange={(event) => setRuleDiffFromVersion(event.target.value)}
                  placeholder="例如：1"
                />
              </label>

              <label className="inline-field" htmlFor="rule-diff-to-version">
                Diff 目标版本
                <input
                  id="rule-diff-to-version"
                  type="number"
                  min={1}
                  step={1}
                  value={ruleDiffToVersion}
                  onChange={(event) => setRuleDiffToVersion(event.target.value)}
                  placeholder="例如：2"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={
                  fetchRuleVersionDiffMutation.isPending ||
                  selectedRuleAsset.latestVersion < 2
                }
                onClick={() => {
                  const fromVersion = Number(ruleDiffFromVersion);
                  const toVersion = Number(ruleDiffToVersion);
                  if (
                    !Number.isInteger(fromVersion) ||
                    fromVersion < 1 ||
                    !Number.isInteger(toVersion) ||
                    toVersion < 1
                  ) {
                    setRuleFeedback(null);
                    setRuleError("Diff 起始版本和目标版本必须是正整数。");
                    return;
                  }
                  if (fromVersion === toVersion) {
                    setRuleFeedback(null);
                    setRuleError("Diff 起始版本和目标版本不能相同。");
                    return;
                  }
                  setRuleFeedback(null);
                  setRuleError(null);
                  fetchRuleVersionDiffMutation.mutate({
                    assetId: selectedRuleAsset.id,
                    fromVersion,
                    toVersion,
                  });
                }}
              >
                {fetchRuleVersionDiffMutation.isPending ? "比较中..." : "比较版本"}
              </button>
            </div>

            {fetchRuleVersionDiffMutation.isPending ? (
              <p className="feedback info">版本 diff 加载中...</p>
            ) : null}
            {selectedRuleAsset.latestVersion < 2 ? (
              <p className="feedback info">
                当前资产至少需要两个版本才可比较 diff。
              </p>
            ) : null}
            {ruleDiffPayload ? (
              <>
                <p className="feedback info">
                  {`版本 diff：v${ruleDiffPayload.fromVersion} -> v${ruleDiffPayload.toVersion}（+${ruleDiffPayload.summary.added} / -${ruleDiffPayload.summary.removed} / =${ruleDiffPayload.summary.unchanged}）`}
                </p>
                <div className="table-wrapper">
                  <table className="session-table">
                    <thead>
                      <tr>
                        <th>旧行号</th>
                        <th>新行号</th>
                        <th>类型</th>
                        <th>内容</th>
                      </tr>
                    </thead>
                    <tbody>
                      {ruleDiffPayload.lines.length === 0 ? (
                        <tr>
                          <td className="table-empty-cell" colSpan={4}>
                            两个版本内容一致
                          </td>
                        </tr>
                      ) : (
                        ruleDiffPayload.lines.map((line, index) => (
                          <tr key={`${line.type}-${index}-${line.oldLineNumber ?? "old"}-${line.newLineNumber ?? "new"}`}>
                            <td>{line.oldLineNumber ?? "--"}</td>
                            <td>{line.newLineNumber ?? "--"}</td>
                            <td>{formatRuleDiffLineType(line.type)}</td>
                            <td style={{ whiteSpace: "pre-wrap" }}>{line.content}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </>
            ) : null}

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
          <p className="feedback empty">
            请选择一个规则资产查看版本与审批详情。
          </p>
        )}
      </section>

      <section className="panel">
        <header>
          <h2>Agent Releases / Rollout</h2>
          <p>查看 release 与服务端 rollout 预览结果。</p>
        </header>

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="agent-release-channel-filter">
            Release Channel
            <select
              id="agent-release-channel-filter"
              value={agentReleaseChannelFilter}
              onChange={(event) =>
                setAgentReleaseChannelFilter(
                  event.target.value as AgentReleaseChannel,
                )
              }
            >
              <option value="stable">stable</option>
              <option value="beta">beta</option>
              <option value="canary">canary</option>
            </select>
          </label>
        </div>

        {agentReleasesQuery.isLoading ? (
          <p className="feedback info">Agent Releases 加载中...</p>
        ) : null}
        {agentReleasesQuery.isError ? (
          <p className="feedback error">
            Agent Releases 加载失败：{toErrorMessage(agentReleasesQuery.error)}
          </p>
        ) : null}
        {agentReleaseFeedback ? (
          <p className="feedback success">{agentReleaseFeedback}</p>
        ) : null}
        {agentReleaseError ? (
          <p className="feedback error">{agentReleaseError}</p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Release ID</th>
                <th>Version</th>
                <th>Channel</th>
                <th>Published At</th>
                <th>Artifacts</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {agentReleaseItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={6}>
                    暂无 Agent Release
                  </td>
                </tr>
              ) : (
                agentReleaseItems.map((item) => (
                  <tr
                    key={item.releaseId}
                    className={
                      selectedAgentReleaseId === item.releaseId ? "is-selected-row" : ""
                    }
                  >
                    <td>{item.releaseId}</td>
                    <td>{item.version}</td>
                    <td>{item.channel}</td>
                    <td>{formatDateTime(item.publishedAt)}</td>
                    <td>
                      {item.artifacts.length > 0
                        ? item.artifacts
                            .map((artifact) => formatAgentReleaseArtifactSummary(artifact))
                            .join(" || ")
                        : "--"}
                    </td>
                    <td>
                      <button
                        type="button"
                        className="table-action"
                        onClick={() => setSelectedAgentReleaseId(item.releaseId)}
                      >
                        {selectedAgentReleaseId === item.releaseId ? "已选中" : "选中"}
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {selectedAgentRelease ? (
          <>
            <p className="feedback info">
              当前 Release：{selectedAgentRelease.releaseId} / v
              {selectedAgentRelease.version} / channel={selectedAgentRelease.channel}
            </p>

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>OS</th>
                    <th>Arch</th>
                    <th>File Name</th>
                    <th>Rollout Ring</th>
                    <th>Rollout Percentage</th>
                    <th>Min Agent Version</th>
                    <th>Signature</th>
                    <th>Install Hint</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {selectedAgentRelease.artifacts.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={9}>
                        当前 release 暂无 artifact
                      </td>
                    </tr>
                  ) : (
                    selectedAgentRelease.artifacts.map((artifact, index) => (
                      <tr key={`${selectedAgentRelease.releaseId}-${artifact.os}-${artifact.arch}-${index}`}>
                        <td>{artifact.os}</td>
                        <td>{artifact.arch}</td>
                        <td>{artifact.fileName ?? "--"}</td>
                        <td>{artifact.rolloutRing ?? "--"}</td>
                        <td>
                          {typeof artifact.rolloutPercentage === "number"
                            ? artifact.rolloutPercentage
                            : "--"}
                        </td>
                        <td>{artifact.minAgentVersion ?? "--"}</td>
                        <td>{artifact.signatureAlgorithm ?? "--"}</td>
                        <td>{artifact.installHint ?? "--"}</td>
                        <td>
                          <button
                            type="button"
                            className="table-action"
                            onClick={() => {
                              hydrateAgentReleasePreviewFromArtifact(artifact);
                              setAgentReleaseError(null);
                              setAgentReleaseFeedback(
                                agentReleasePreviewCurrentVersion.trim().length > 0
                                  ? artifact.fileName
                                    ? `已将 ${artifact.fileName} 回填到升级预览。`
                                    : "已将 artifact 回填到升级预览。"
                                  : artifact.fileName
                                    ? `已将 ${artifact.fileName} 回填到升级预览，请补 Current Version 后执行。`
                                    : "已将 artifact 回填到升级预览，请补 Current Version 后执行。",
                              );
                            }}
                          >
                            回填到预览
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </>
        ) : (
          <p className="feedback empty">请选择一个 release 查看 artifact 编排视图。</p>
        )}

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="agent-release-preview-current-version">
            Current Version
            <input
              id="agent-release-preview-current-version"
              type="text"
              value={agentReleasePreviewCurrentVersion}
              onChange={(event) =>
                setAgentReleasePreviewCurrentVersion(event.target.value)
              }
              placeholder="例如：1.0.0"
            />
          </label>
          <label className="inline-field" htmlFor="agent-release-preview-os">
            OS
            <input
              id="agent-release-preview-os"
              type="text"
              value={agentReleasePreviewOs}
              onChange={(event) => setAgentReleasePreviewOs(event.target.value)}
              placeholder="例如：darwin"
            />
          </label>
          <label className="inline-field" htmlFor="agent-release-preview-arch">
            Arch
            <input
              id="agent-release-preview-arch"
              type="text"
              value={agentReleasePreviewArch}
              onChange={(event) => setAgentReleasePreviewArch(event.target.value)}
              placeholder="例如：amd64"
            />
          </label>
          <label className="inline-field" htmlFor="agent-release-preview-agent-id">
            Agent ID
            <input
              id="agent-release-preview-agent-id"
              type="text"
              value={agentReleasePreviewAgentId}
              onChange={(event) =>
                setAgentReleasePreviewAgentId(event.target.value)
              }
              placeholder="可选"
            />
          </label>
          <label className="inline-field" htmlFor="agent-release-preview-device-id">
            Device ID
            <input
              id="agent-release-preview-device-id"
              type="text"
              value={agentReleasePreviewDeviceId}
              onChange={(event) =>
                setAgentReleasePreviewDeviceId(event.target.value)
              }
              placeholder="可选"
            />
          </label>
          <label className="inline-field" htmlFor="agent-release-preview-hostname">
            Hostname
            <input
              id="agent-release-preview-hostname"
              type="text"
              value={agentReleasePreviewHostname}
              onChange={(event) =>
                setAgentReleasePreviewHostname(event.target.value)
              }
              placeholder="可选"
            />
          </label>
          <label className="inline-field" htmlFor="agent-release-preview-ring">
            Rollout Ring
            <input
              id="agent-release-preview-ring"
              type="text"
              value={agentReleasePreviewRing}
              onChange={(event) => setAgentReleasePreviewRing(event.target.value)}
              placeholder="例如：stable"
            />
          </label>
          <button
            type="button"
            className="submit-button"
            disabled={fetchAgentReleaseCheckPreviewMutation.isPending}
            onClick={() => {
              if (!agentReleasePreviewCurrentVersion.trim()) {
                setAgentReleaseFeedback(null);
                setAgentReleaseError("Current Version 不能为空。");
                return;
              }
              if (!agentReleasePreviewOs.trim() || !agentReleasePreviewArch.trim()) {
                setAgentReleaseFeedback(null);
                setAgentReleaseError("OS 和 Arch 不能为空。");
                return;
              }
              setAgentReleaseFeedback(null);
              setAgentReleaseError(null);
              fetchAgentReleaseCheckPreviewMutation.mutate();
            }}
          >
            {fetchAgentReleaseCheckPreviewMutation.isPending ? "预览中..." : "执行升级预览"}
          </button>
        </div>

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Comparison</th>
                <th>Update Available</th>
                <th>Latest Release</th>
                <th>Selected Artifact</th>
                <th>Evaluated Ring</th>
                <th>Rollout Bucket</th>
                <th>Selection Reason</th>
              </tr>
            </thead>
            <tbody>
              {!agentReleasePreviewPayload ? (
                <tr>
                  <td className="table-empty-cell" colSpan={7}>
                    {hasLoadedAgentReleasePreview
                      ? "未查询到 rollout 预览结果。"
                      : "请输入版本与环境信息后执行升级预览。"}
                  </td>
                </tr>
              ) : (
                <tr>
                  <td>{agentReleasePreviewPayload.comparison}</td>
                  <td>{agentReleasePreviewPayload.updateAvailable ? "true" : "false"}</td>
                  <td>{agentReleasePreviewPayload.latestRelease?.version ?? "--"}</td>
                  <td>
                    {agentReleasePreviewPayload.selectedArtifact
                      ? formatAgentReleaseArtifactSummary(
                          agentReleasePreviewPayload.selectedArtifact,
                        )
                      : "--"}
                  </td>
                  <td>{agentReleasePreviewPayload.evaluatedRing ?? "--"}</td>
                  <td>
                    {typeof agentReleasePreviewPayload.rolloutBucket === "number"
                      ? agentReleasePreviewPayload.rolloutBucket
                      : "--"}
                  </td>
                  <td>{agentReleasePreviewPayload.selectionReason ?? "--"}</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="agent-release-batch-os">
            Batch OS
            <input
              id="agent-release-batch-os"
              type="text"
              value={agentReleaseBatchOs}
              onChange={(event) => setAgentReleaseBatchOs(event.target.value)}
              placeholder="例如：darwin"
            />
          </label>
          <label className="inline-field" htmlFor="agent-release-batch-arch">
            Batch Arch
            <input
              id="agent-release-batch-arch"
              type="text"
              value={agentReleaseBatchArch}
              onChange={(event) => setAgentReleaseBatchArch(event.target.value)}
              placeholder="例如：amd64"
            />
          </label>
        </div>

        <div className="filters-row">
          <label
            className="inline-field governance-wide-field"
            htmlFor="agent-release-batch-samples-json"
          >
            Batch Samples JSON
            <textarea
              id="agent-release-batch-samples-json"
              value={agentReleaseBatchSamplesJson}
              onChange={(event) =>
                setAgentReleaseBatchSamplesJson(event.target.value)
              }
              rows={8}
              placeholder='[{"label":"stable-default","currentVersion":"1.0.0","ring":"stable"}]'
            />
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={fetchAgentReleaseCheckBatchPreviewMutation.isPending}
            onClick={() => {
              if (!agentReleaseBatchOs.trim() || !agentReleaseBatchArch.trim()) {
                setAgentReleaseFeedback(null);
                setAgentReleaseError("Batch OS 和 Batch Arch 不能为空。");
                return;
              }
              let samples: AgentReleaseBatchCheckSampleInput[];
              try {
                const parsed = JSON.parse(agentReleaseBatchSamplesJson);
                if (!Array.isArray(parsed) || parsed.length === 0) {
                  setAgentReleaseFeedback(null);
                  setAgentReleaseError("Batch Samples JSON 必须是非空数组。");
                  return;
                }
                samples = parsed as AgentReleaseBatchCheckSampleInput[];
              } catch (error) {
                setAgentReleaseFeedback(null);
                setAgentReleaseError(`Batch Samples JSON 非法：${toErrorMessage(error)}`);
                return;
              }
              setAgentReleaseFeedback(null);
              setAgentReleaseError(null);
              fetchAgentReleaseCheckBatchPreviewMutation.mutate(samples);
            }}
          >
            {fetchAgentReleaseCheckBatchPreviewMutation.isPending
              ? "模拟中..."
              : "执行批量模拟"}
          </button>
        </div>

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Label</th>
                <th>Comparison</th>
                <th>Update Available</th>
                <th>Latest Release</th>
                <th>Selected Artifact</th>
                <th>Evaluated Ring</th>
                <th>Rollout Bucket</th>
                <th>Selection Reason</th>
              </tr>
            </thead>
            <tbody>
              {!agentReleaseBatchPreviewPayload ? (
                <tr>
                  <td className="table-empty-cell" colSpan={8}>
                    暂无批量模拟结果
                  </td>
                </tr>
              ) : (
                agentReleaseBatchPreviewPayload.items.map((item) => (
                  <tr key={item.label}>
                    <td>{item.label}</td>
                    <td>{item.comparison}</td>
                    <td>{item.updateAvailable ? "true" : "false"}</td>
                    <td>{item.latestRelease?.version ?? "--"}</td>
                    <td>
                      {item.selectedArtifact
                        ? formatAgentReleaseArtifactSummary(item.selectedArtifact)
                        : "--"}
                    </td>
                    <td>{item.evaluatedRing ?? "--"}</td>
                    <td>
                      {typeof item.rolloutBucket === "number"
                        ? item.rolloutBucket
                        : "--"}
                    </td>
                    <td>{item.selectionReason ?? "--"}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>TokenPulse Runtime Events</h2>
          <p>按 traceId 联查 TokenPulse 运行时摘要事件。</p>
        </header>

        <div className="filters-row governance-inline-grid">
          <label
            className="inline-field governance-wide-field"
            htmlFor="tokenpulse-runtime-trace-id"
          >
            Trace ID
            <input
              id="tokenpulse-runtime-trace-id"
              type="text"
              value={tokenPulseRuntimeTraceId}
              onChange={(event) => setTokenPulseRuntimeTraceId(event.target.value)}
              placeholder="例如：trace-oauth-runtime-20260308-0001"
            />
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="tokenpulse-runtime-provider"
          >
            Provider
            <input
              id="tokenpulse-runtime-provider"
              type="text"
              value={tokenPulseRuntimeProviderFilter}
              onChange={(event) =>
                setTokenPulseRuntimeProviderFilter(event.target.value)
              }
              placeholder="可选，例如：claude"
            />
          </label>

          <label
            className="inline-field"
            htmlFor="tokenpulse-runtime-status-filter"
          >
            运行时状态
            <select
              id="tokenpulse-runtime-status-filter"
              value={tokenPulseRuntimeStatusFilter}
              onChange={(event) =>
                setTokenPulseRuntimeStatusFilter(
                  event.target.value as TokenPulseRuntimeEventStatus | "",
                )
              }
            >
              {TOKENPULSE_RUNTIME_STATUS_FILTER_OPTIONS.map((option) => (
                <option key={option.value || "all"} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={loadTokenPulseRuntimeEventsMutation.isPending}
            onClick={() => {
              const traceId = tokenPulseRuntimeTraceId.trim();
              if (!traceId) {
                setTokenPulseRuntimeFeedback(null);
                setTokenPulseRuntimeError("Trace ID 不能为空。");
                return;
              }
              setTokenPulseRuntimeFeedback(null);
              setTokenPulseRuntimeError(null);
              loadTokenPulseRuntimeEventsMutation.mutate({
                traceId,
                provider: tokenPulseRuntimeProviderFilter.trim() || undefined,
                status: tokenPulseRuntimeStatusFilter || undefined,
              });
            }}
          >
            {loadTokenPulseRuntimeEventsMutation.isPending ? "查询中..." : "查询运行时摘要"}
          </button>
        </div>

        {loadTokenPulseRuntimeEventsMutation.isPending ? (
          <p className="feedback info">TokenPulse 运行时摘要加载中...</p>
        ) : null}
        {tokenPulseRuntimeFeedback ? (
          <p className="feedback success">{tokenPulseRuntimeFeedback}</p>
        ) : null}
        {tokenPulseRuntimeError ? (
          <p className="feedback error">{tokenPulseRuntimeError}</p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Trace ID</th>
                <th>Project</th>
                <th>Provider</th>
                <th>Model</th>
                <th>Resolved Model</th>
                <th>Route Policy</th>
                <th>Status</th>
                <th>Error Code</th>
                <th>Cost</th>
                <th>Started At</th>
                <th>Finished At</th>
              </tr>
            </thead>
            <tbody>
              {tokenPulseRuntimeItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={11}>
                    {hasLoadedTokenPulseRuntimeEvents
                      ? "未查询到 TokenPulse 运行时摘要。"
                      : "请输入 Trace ID 后查询 TokenPulse 运行时摘要。"}
                  </td>
                </tr>
              ) : (
                tokenPulseRuntimeItems.map((item) => (
                  <tr key={item.id}>
                    <td>{item.traceId}</td>
                    <td>{item.projectId ?? "--"}</td>
                    <td>{item.provider}</td>
                    <td>{item.model}</td>
                    <td>{item.resolvedModel}</td>
                    <td>{item.routePolicy}</td>
                    <td>{item.status}</td>
                    <td>{item.errorCode ?? "--"}</td>
                    <td>{item.cost ?? "--"}</td>
                    <td>{formatDateTime(item.startedAt)}</td>
                    <td>{item.finishedAt ? formatDateTime(item.finishedAt) : "--"}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>MCP 治理</h2>
          <p>工具策略、审批请求和调用审计。</p>
        </header>

        <div className="filters-row">
          <label
            className="inline-field governance-wide-field"
            htmlFor="mcp-policy-keyword"
          >
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
              onChange={(event) =>
                setMcpPolicyRiskLevel(event.target.value as McpRiskLevel)
              }
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
              onChange={(event) =>
                setMcpPolicyDecision(event.target.value as McpToolDecision)
              }
            >
              {MCP_DECISION_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="mcp-policy-reason"
          >
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
              const stage1RequiredApprovals = parseOptionalNonNegativeInteger(
                mcpPolicyStage1RequiredApprovals,
              );
              const stage2RequiredApprovals = parseOptionalNonNegativeInteger(
                mcpPolicyStage2RequiredApprovals,
              );
              const stage1Roles = parseDistinctCommaSeparatedList(mcpPolicyStage1Roles);
              const stage2Roles = parseDistinctCommaSeparatedList(mcpPolicyStage2Roles);
              const approvalWorkflowResult =
                mcpPolicyApprovalMode === "multi_stage"
                  ? buildMcpWorkflowFromDraft({
                      entryNodeId: mcpWorkflowEntryNodeId,
                      nodes: mcpWorkflowNodes,
                      transitions: mcpWorkflowTransitions,
                    })
                  : null;
              const staticApprovalConfigResult =
                mcpPolicyApprovalMode === "multi_stage"
                  ? null
                  : resolveMcpStaticApprovalConfig({
                      mode: mcpPolicyApprovalMode,
                      approvalStagesJson: mcpPolicyApprovalStagesJson,
                      approvalStagesJsonTouched: mcpPolicyApprovalStagesJsonTouched,
                      stage1RequiredApprovals,
                      stage2RequiredApprovals,
                      stage1Roles,
                      stage2Roles,
                    });
              const approvalCondition = {
                ...(mcpPolicyConditionRiskLevel
                  ? { riskLevelAtLeast: mcpPolicyConditionRiskLevel }
                  : {}),
                ...(parseDistinctCommaSeparatedList(mcpPolicyConditionToolIds).length > 0
                  ? {
                      toolIds: parseDistinctCommaSeparatedList(
                        mcpPolicyConditionToolIds,
                      ),
                    }
                  : {}),
                ...(parseDistinctCommaSeparatedList(mcpPolicyConditionTenantRoles)
                  .length > 0
                  ? {
                      tenantRoles: parseDistinctCommaSeparatedList(
                        mcpPolicyConditionTenantRoles,
                      ),
                    }
                  : {}),
              };
              if (!toolId) {
                setMcpFeedback(null);
                setMcpError("Tool ID 不能为空。");
                return;
              }
              if (
                mcpPolicyDecision === "require_approval" &&
                mcpPolicyApprovalMode === "multi_stage" &&
                approvalWorkflowResult &&
                !approvalWorkflowResult.success
              ) {
                setMcpFeedback(null);
                setMcpError(approvalWorkflowResult.message);
                return;
              }
              if (
                mcpPolicyDecision === "require_approval" &&
                staticApprovalConfigResult &&
                !staticApprovalConfigResult.success
              ) {
                setMcpFeedback(null);
                setMcpError(staticApprovalConfigResult.message);
                return;
              }
              if (
                mcpPolicyDecision === "require_approval" &&
                (typeof stage1RequiredApprovals !== "number" ||
                  stage1RequiredApprovals < 1)
                &&
                mcpPolicyApprovalMode !== "multi_stage"
              ) {
                setMcpFeedback(null);
                setMcpError("stage1 审批人数必须是大于等于 1 的整数。");
                return;
              }
              if (
                mcpPolicyDecision === "require_approval" &&
                mcpPolicyApprovalMode === "two_stage" &&
                (typeof stage2RequiredApprovals !== "number" ||
                  stage2RequiredApprovals < 1)
              ) {
                setMcpFeedback(null);
                setMcpError("two_stage 模式下 stage2 审批人数必须是大于等于 1 的整数。");
                return;
              }
              setMcpFeedback(null);
              setMcpError(null);
              upsertMcpPolicyMutation.mutate({
                toolId,
                riskLevel: mcpPolicyRiskLevel,
                decision: mcpPolicyDecision,
                approvalMode:
                  mcpPolicyDecision === "require_approval"
                    ? mcpPolicyApprovalMode
                    : undefined,
                approvalWorkflow:
                  mcpPolicyDecision === "require_approval" &&
                  mcpPolicyApprovalMode === "multi_stage" &&
                  approvalWorkflowResult &&
                  approvalWorkflowResult.success
                    ? approvalWorkflowResult.data
                    : undefined,
                approvalStages:
                  mcpPolicyDecision === "require_approval" &&
                  mcpPolicyApprovalMode !== "multi_stage" &&
                  staticApprovalConfigResult &&
                  staticApprovalConfigResult.success
                    ? staticApprovalConfigResult.approvalStages
                    : undefined,
                stage1RequiredApprovals:
                  mcpPolicyDecision === "require_approval" &&
                  mcpPolicyApprovalMode !== "multi_stage" &&
                  staticApprovalConfigResult &&
                  staticApprovalConfigResult.success
                    ? staticApprovalConfigResult.approvalStages[0]?.requiredApprovals
                    : undefined,
                stage2RequiredApprovals:
                  mcpPolicyDecision === "require_approval" &&
                  mcpPolicyApprovalMode !== "multi_stage" &&
                  staticApprovalConfigResult &&
                  staticApprovalConfigResult.success &&
                  mcpPolicyApprovalMode === "two_stage"
                    ? staticApprovalConfigResult.approvalStages[1]?.requiredApprovals
                    : undefined,
                stage1Roles:
                  mcpPolicyDecision === "require_approval" &&
                  mcpPolicyApprovalMode !== "multi_stage" &&
                  staticApprovalConfigResult &&
                  staticApprovalConfigResult.success &&
                  (staticApprovalConfigResult.approvalStages[0]?.roles.length ?? 0) > 0
                    ? staticApprovalConfigResult.approvalStages[0]?.roles
                    : undefined,
                stage2Roles:
                  mcpPolicyDecision === "require_approval" &&
                  mcpPolicyApprovalMode !== "multi_stage" &&
                  staticApprovalConfigResult &&
                  staticApprovalConfigResult.success &&
                  mcpPolicyApprovalMode === "two_stage" &&
                  (staticApprovalConfigResult.approvalStages[1]?.roles.length ?? 0) > 0
                    ? staticApprovalConfigResult.approvalStages[1]?.roles
                    : undefined,
                approvalCondition:
                  mcpPolicyDecision === "require_approval" &&
                  Object.keys(approvalCondition).length > 0
                    ? approvalCondition
                    : undefined,
                reason: mcpPolicyReason.trim() || undefined,
              });
            }}
          >
            {upsertMcpPolicyMutation.isPending ? "保存中..." : "保存策略"}
          </button>
        </div>

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="mcp-policy-approval-mode">
            审批模式
            <select
              id="mcp-policy-approval-mode"
              value={mcpPolicyApprovalMode}
              onChange={(event) =>
                setMcpPolicyApprovalMode(
                  event.target.value as McpApprovalMode,
                )
              }
            >
              {MCP_APPROVAL_MODE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field"
            htmlFor="mcp-policy-stage1-required-approvals"
          >
            stage1 人数
            <input
              id="mcp-policy-stage1-required-approvals"
              type="number"
              min={1}
              step={1}
              value={mcpPolicyStage1RequiredApprovals}
              onChange={(event) =>
                setMcpPolicyStage1RequiredApprovals(event.target.value)
              }
              disabled={mcpPolicyApprovalMode === "multi_stage"}
            />
          </label>

          <label
            className="inline-field"
            htmlFor="mcp-policy-stage2-required-approvals"
          >
            stage2 人数
            <input
              id="mcp-policy-stage2-required-approvals"
              type="number"
              min={1}
              step={1}
              value={mcpPolicyStage2RequiredApprovals}
              onChange={(event) =>
                setMcpPolicyStage2RequiredApprovals(event.target.value)
              }
              disabled={mcpPolicyApprovalMode !== "two_stage"}
            />
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="mcp-policy-stage1-roles"
          >
            stage1 角色（逗号分隔）
            <input
              id="mcp-policy-stage1-roles"
              type="text"
              value={mcpPolicyStage1Roles}
              onChange={(event) => setMcpPolicyStage1Roles(event.target.value)}
              placeholder="例如：owner,maintainer"
              disabled={mcpPolicyApprovalMode === "multi_stage"}
            />
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="mcp-policy-stage2-roles"
          >
            stage2 角色（逗号分隔）
            <input
              id="mcp-policy-stage2-roles"
              type="text"
              value={mcpPolicyStage2Roles}
              onChange={(event) => setMcpPolicyStage2Roles(event.target.value)}
              placeholder="例如：owner"
              disabled={mcpPolicyApprovalMode !== "two_stage"}
            />
          </label>

          {mcpPolicyApprovalMode === "multi_stage" ? (
            <>
              <label className="inline-field" htmlFor="mcp-workflow-entry-node">
                入口节点
                <select
                  id="mcp-workflow-entry-node"
                  value={mcpWorkflowEntryNodeId}
                  onChange={(event) => setMcpWorkflowEntryNodeId(event.target.value)}
                >
                  {mcpWorkflowNodes.map((node) => (
                    <option key={`workflow-entry-${node.nodeId}`} value={node.nodeId}>
                      {node.label || node.nodeId}
                    </option>
                  ))}
                </select>
              </label>

              <div className="inline-field governance-wide-field">
                审批节点编排器
                <div className="governance-action-row">
                  <button
                    type="button"
                    className="table-action"
                    onClick={() =>
                      setMcpWorkflowNodes((prev) => [
                        ...prev,
                        createDefaultMcpWorkflowNodeDraft(prev.length),
                      ])
                    }
                  >
                    添加审批节点
                  </button>
                </div>
                <div className="table-wrapper">
                  <table className="session-table">
                    <thead>
                      <tr>
                        <th>nodeId</th>
                        <th>label</th>
                        <th>stage</th>
                        <th>requiredApprovals</th>
                        <th>roles</th>
                        <th>操作</th>
                      </tr>
                    </thead>
                    <tbody>
                      {mcpWorkflowNodes.map((node, index) => (
                        <tr key={`workflow-node-${index}-${node.nodeId}`}>
                          <td>
                            <input
                              aria-label={`workflow-node-${index}-nodeId`}
                              value={node.nodeId}
                              onChange={(event) => {
                                const nextNodeId = event.target.value;
                                replaceMcpWorkflowNodeId(node.nodeId, nextNodeId);
                                setMcpWorkflowNodes((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? { ...item, nodeId: nextNodeId }
                                      : item,
                                  ),
                                );
                              }}
                            />
                          </td>
                          <td>
                            <input
                              aria-label={`workflow-node-${index}-label`}
                              value={node.label}
                              onChange={(event) =>
                                setMcpWorkflowNodes((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? { ...item, label: event.target.value }
                                      : item,
                                  ),
                                )
                              }
                            />
                          </td>
                          <td>
                            <input
                              aria-label={`workflow-node-${index}-stage`}
                              value={node.stage}
                              onChange={(event) =>
                                setMcpWorkflowNodes((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? { ...item, stage: event.target.value }
                                      : item,
                                  ),
                                )
                              }
                            />
                          </td>
                          <td>
                            <input
                              aria-label={`workflow-node-${index}-requiredApprovals`}
                              type="number"
                              min={1}
                              step={1}
                              value={node.requiredApprovals}
                              onChange={(event) =>
                                setMcpWorkflowNodes((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? { ...item, requiredApprovals: event.target.value }
                                      : item,
                                  ),
                                )
                              }
                            />
                          </td>
                          <td>
                            <input
                              aria-label={`workflow-node-${index}-roles`}
                              value={node.roles}
                              onChange={(event) =>
                                setMcpWorkflowNodes((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? { ...item, roles: event.target.value }
                                      : item,
                                  ),
                                )
                              }
                            />
                          </td>
                          <td>
                            <button
                              type="button"
                              className="table-action"
                              disabled={mcpWorkflowNodes.length <= 1}
                              onClick={() => removeMcpWorkflowNode(index)}
                            >
                              删除
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="inline-field governance-wide-field">
                转移编排器
                <div className="governance-action-row">
                  <button
                    type="button"
                    className="table-action"
                    onClick={() =>
                      setMcpWorkflowTransitions((prev) => [
                        ...prev,
                        {
                          fromNodeId: mcpWorkflowNodes[0]?.nodeId ?? "stage1-node",
                          toNodeId: "approved",
                          mode: "default",
                          riskLevelAtLeast: "",
                          toolIds: "",
                          tenantRoles: "",
                          timeWindowTimezone: "",
                          timeWindowWeekdays: "",
                          timeWindowStartTime: "",
                          timeWindowEndTime: "",
                        },
                      ])
                    }
                  >
                    添加转移
                  </button>
                </div>
                <div className="table-wrapper">
                  <table className="session-table">
                    <thead>
                      <tr>
                        <th>from</th>
                        <th>to</th>
                        <th>mode</th>
                        <th>risk</th>
                        <th>toolIds</th>
                        <th>tenantRoles</th>
                        <th>timezone</th>
                        <th>weekdays</th>
                        <th>start</th>
                        <th>end</th>
                        <th>操作</th>
                      </tr>
                    </thead>
                    <tbody>
                      {mcpWorkflowTransitions.map((transition, index) => (
                        <tr key={`workflow-transition-${index}`}>
                          <td>
                            <select
                              aria-label={`workflow-transition-${index}-from`}
                              value={transition.fromNodeId}
                              onChange={(event) =>
                                setMcpWorkflowTransitions((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? { ...item, fromNodeId: event.target.value }
                                      : item,
                                  ),
                                )
                              }
                            >
                              {mcpWorkflowNodes.map((node) => (
                                <option key={`from-${node.nodeId}`} value={node.nodeId}>
                                  {node.label || node.nodeId}
                                </option>
                              ))}
                            </select>
                          </td>
                          <td>
                            <select
                              aria-label={`workflow-transition-${index}-to`}
                              value={transition.toNodeId}
                              onChange={(event) =>
                                setMcpWorkflowTransitions((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? { ...item, toNodeId: event.target.value }
                                      : item,
                                  ),
                                )
                              }
                            >
                              {[...mcpWorkflowNodes.map((node) => node.nodeId), "approved", "rejected"].map((nodeId) => (
                                <option key={`to-${nodeId}`} value={nodeId}>
                                  {nodeId}
                                </option>
                              ))}
                            </select>
                          </td>
                          <td>
                            <select
                              aria-label={`workflow-transition-${index}-mode`}
                              value={transition.mode}
                              onChange={(event) =>
                                setMcpWorkflowTransitions((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? {
                                          ...item,
                                          mode: event.target.value as "default" | "conditional",
                                        }
                                      : item,
                                  ),
                                )
                              }
                            >
                              <option value="default">default</option>
                              <option value="conditional">conditional</option>
                            </select>
                          </td>
                          <td>
                            <select
                              aria-label={`workflow-transition-${index}-risk`}
                              value={transition.riskLevelAtLeast}
                              disabled={transition.mode !== "conditional"}
                              onChange={(event) =>
                                setMcpWorkflowTransitions((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? {
                                          ...item,
                                          riskLevelAtLeast: event.target.value as "" | McpRiskLevel,
                                        }
                                      : item,
                                  ),
                                )
                              }
                            >
                              <option value="">--</option>
                              {MCP_RISK_LEVEL_OPTIONS.map((option) => (
                                <option key={`workflow-risk-${option.value}`} value={option.value}>
                                  {option.label}
                                </option>
                              ))}
                            </select>
                          </td>
                          <td>
                            <input
                              aria-label={`workflow-transition-${index}-toolIds`}
                              value={transition.toolIds}
                              disabled={transition.mode !== "conditional"}
                              onChange={(event) =>
                                setMcpWorkflowTransitions((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? { ...item, toolIds: event.target.value }
                                      : item,
                                  ),
                                )
                              }
                            />
                          </td>
                          <td>
                            <input
                              aria-label={`workflow-transition-${index}-tenantRoles`}
                              value={transition.tenantRoles}
                              disabled={transition.mode !== "conditional"}
                              onChange={(event) =>
                                setMcpWorkflowTransitions((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? { ...item, tenantRoles: event.target.value }
                                      : item,
                                  ),
                                )
                              }
                            />
                          </td>
                          <td>
                            <input
                              aria-label={`workflow-transition-${index}-timezone`}
                              value={transition.timeWindowTimezone}
                              disabled={transition.mode !== "conditional"}
                              placeholder="Asia/Shanghai"
                              onChange={(event) =>
                                setMcpWorkflowTransitions((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? {
                                          ...item,
                                          timeWindowTimezone: event.target.value,
                                        }
                                      : item,
                                  ),
                                )
                              }
                            />
                          </td>
                          <td>
                            <input
                              aria-label={`workflow-transition-${index}-weekdays`}
                              value={transition.timeWindowWeekdays}
                              disabled={transition.mode !== "conditional"}
                              placeholder="1,2,3,4,5"
                              onChange={(event) =>
                                setMcpWorkflowTransitions((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? {
                                          ...item,
                                          timeWindowWeekdays: event.target.value,
                                        }
                                      : item,
                                  ),
                                )
                              }
                            />
                          </td>
                          <td>
                            <input
                              aria-label={`workflow-transition-${index}-startTime`}
                              type="time"
                              value={transition.timeWindowStartTime}
                              disabled={transition.mode !== "conditional"}
                              onChange={(event) =>
                                setMcpWorkflowTransitions((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? {
                                          ...item,
                                          timeWindowStartTime: event.target.value,
                                        }
                                      : item,
                                  ),
                                )
                              }
                            />
                          </td>
                          <td>
                            <input
                              aria-label={`workflow-transition-${index}-endTime`}
                              type="time"
                              value={transition.timeWindowEndTime}
                              disabled={transition.mode !== "conditional"}
                              onChange={(event) =>
                                setMcpWorkflowTransitions((prev) =>
                                  prev.map((item, itemIndex) =>
                                    itemIndex === index
                                      ? {
                                          ...item,
                                          timeWindowEndTime: event.target.value,
                                        }
                                      : item,
                                  ),
                                )
                              }
                            />
                          </td>
                          <td>
                            <button
                              type="button"
                              className="table-action"
                              onClick={() =>
                                setMcpWorkflowTransitions((prev) =>
                                  prev.filter((_, itemIndex) => itemIndex !== index),
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
              </div>

              <label
                className="inline-field governance-wide-field"
                htmlFor="mcp-policy-approval-stages-json"
              >
                approvalWorkflow 预览
                <textarea
                  id="mcp-policy-approval-stages-json"
                  value={(() => {
                    const preview = buildMcpWorkflowFromDraft({
                      entryNodeId: mcpWorkflowEntryNodeId,
                      nodes: mcpWorkflowNodes,
                      transitions: mcpWorkflowTransitions,
                    });
                    return preview.success
                      ? JSON.stringify(preview.data, null, 2)
                      : preview.message;
                  })()}
                  rows={10}
                  readOnly
                />
              </label>
            </>
          ) : (
            <label
              className="inline-field governance-wide-field"
              htmlFor="mcp-policy-approval-stages-json"
            >
              approvalStages JSON
              <textarea
                id="mcp-policy-approval-stages-json"
                value={mcpPolicyApprovalStagesJson}
                onChange={(event) => {
                  setMcpPolicyApprovalStagesJsonTouched(true);
                  setMcpPolicyApprovalStagesJson(event.target.value);
                }}
                rows={6}
                placeholder='[{"stage":"stage1","requiredApprovals":1,"roles":["owner"]}]'
              />
            </label>
          )}
        </div>

        <div className="filters-row governance-inline-grid">
          <label
            className="inline-field"
            htmlFor="mcp-policy-condition-risk-level"
          >
            条件风险下限
            <select
              id="mcp-policy-condition-risk-level"
              value={mcpPolicyConditionRiskLevel}
              onChange={(event) =>
                setMcpPolicyConditionRiskLevel(
                  event.target.value as McpRiskLevel | "",
                )
              }
            >
              <option value="">全部风险</option>
              {MCP_RISK_LEVEL_OPTIONS.map((option) => (
                <option key={`condition-risk-${option.value}`} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="mcp-policy-condition-tool-ids"
          >
            条件 Tool IDs（逗号分隔）
            <input
              id="mcp-policy-condition-tool-ids"
              type="text"
              value={mcpPolicyConditionToolIds}
              onChange={(event) =>
                setMcpPolicyConditionToolIds(event.target.value)
              }
              placeholder="可选"
            />
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="mcp-policy-condition-tenant-roles"
          >
            条件租户角色（逗号分隔）
            <input
              id="mcp-policy-condition-tenant-roles"
              type="text"
              value={mcpPolicyConditionTenantRoles}
              onChange={(event) =>
                setMcpPolicyConditionTenantRoles(event.target.value)
              }
              placeholder="例如：owner,maintainer"
            />
          </label>
        </div>

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Tool ID</th>
                <th>Risk</th>
                <th>Decision</th>
                <th>Approval</th>
                <th>Condition</th>
                <th>Reason</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {mcpPolicyItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={7}>
                    暂无 MCP 策略
                  </td>
                </tr>
              ) : (
                mcpPolicyItems.map((policy) => (
                  <tr key={policy.toolId}>
                    <td>{policy.toolId}</td>
                    <td>{policy.riskLevel}</td>
                    <td>{policy.decision}</td>
                    <td>
                      {policy.decision === "require_approval"
                        ? `${policy.approvalMode ?? "single_stage"} / ${
                            policy.approvalWorkflow
                              ? formatMcpApprovalWorkflowSummary(policy.approvalWorkflow)
                              : formatMcpApprovalStages(
                                  policy.approvalStages && policy.approvalStages.length > 0
                                    ? policy.approvalStages
                                    : [
                                        {
                                          stage: "stage1",
                                          requiredApprovals:
                                            policy.stage1RequiredApprovals ?? 1,
                                          roles: policy.stage1Roles ?? [
                                            "owner",
                                            "maintainer",
                                          ],
                                        },
                                        ...(typeof policy.stage2RequiredApprovals === "number"
                                          ? [
                                              {
                                                stage: "stage2",
                                                requiredApprovals:
                                                  policy.stage2RequiredApprovals,
                                                roles: policy.stage2Roles ?? ["owner"],
                                              },
                                            ]
                                          : []),
                                      ],
                                )
                          }`
                        : "--"}
                    </td>
                    <td>
                      {policy.approvalCondition
                        ? formatCompactJson(policy.approvalCondition)
                        : "--"}
                    </td>
                    <td>{policy.reason ?? "--"}</td>
                    <td>{formatDateTime(policy.updatedAt)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="mcp-evaluate-tool-id">
            评估 Tool ID
            <input
              id="mcp-evaluate-tool-id"
              type="text"
              value={mcpEvaluateToolId}
              onChange={(event) => setMcpEvaluateToolId(event.target.value)}
              placeholder="例如：github.delete_repo"
            />
          </label>

          <label
            className="inline-field"
            htmlFor="mcp-evaluate-approval-request-id"
          >
            审批请求 ID
            <input
              id="mcp-evaluate-approval-request-id"
              type="text"
              value={mcpEvaluateApprovalRequestId}
              onChange={(event) =>
                setMcpEvaluateApprovalRequestId(event.target.value)
              }
              placeholder="可选"
            />
          </label>

          <label
            className="inline-field governance-wide-field"
            htmlFor="mcp-evaluate-reason"
          >
            评估说明
            <input
              id="mcp-evaluate-reason"
              type="text"
              value={mcpEvaluateReason}
              onChange={(event) => setMcpEvaluateReason(event.target.value)}
              placeholder="可选"
            />
          </label>

          <label className="inline-field" htmlFor="mcp-evaluate-timestamp">
            评估时间（可选）
            <input
              id="mcp-evaluate-timestamp"
              type="datetime-local"
              value={mcpEvaluateTimestamp}
              onChange={(event) => setMcpEvaluateTimestamp(event.target.value)}
            />
          </label>

          <button
            type="button"
            className="submit-button"
            disabled={evaluateMcpToolMutation.isPending}
            onClick={() => {
              const toolId = mcpEvaluateToolId.trim();
              const normalizedEvaluationTimestamp = mcpEvaluateTimestamp.trim();
              const stage1Roles = parseDistinctCommaSeparatedList(
                mcpPolicyStage1Roles,
              );
              const stage2Roles = parseDistinctCommaSeparatedList(
                mcpPolicyStage2Roles,
              );
              const stage1RequiredApprovals = parseOptionalNonNegativeInteger(
                mcpPolicyStage1RequiredApprovals,
              );
              const stage2RequiredApprovals = parseOptionalNonNegativeInteger(
                mcpPolicyStage2RequiredApprovals,
              );
              const approvalWorkflowResult =
                mcpPolicyApprovalMode === "multi_stage"
                  ? buildMcpWorkflowFromDraft({
                      entryNodeId: mcpWorkflowEntryNodeId,
                      nodes: mcpWorkflowNodes,
                      transitions: mcpWorkflowTransitions,
                    })
                  : null;
              const staticApprovalConfigResult =
                mcpPolicyApprovalMode === "multi_stage"
                  ? null
                  : resolveMcpStaticApprovalConfig({
                      mode: mcpPolicyApprovalMode,
                      approvalStagesJson: mcpPolicyApprovalStagesJson,
                      approvalStagesJsonTouched: mcpPolicyApprovalStagesJsonTouched,
                      stage1RequiredApprovals,
                      stage2RequiredApprovals,
                      stage1Roles,
                      stage2Roles,
                    });
              if (!toolId) {
                setMcpFeedback(null);
                setMcpError("评估 Tool ID 不能为空。");
                return;
              }
              if (
                normalizedEvaluationTimestamp &&
                Number.isNaN(Date.parse(normalizedEvaluationTimestamp))
              ) {
                setMcpFeedback(null);
                setMcpError("评估时间格式不合法。");
                return;
              }
              if (
                !mcpEvaluateApprovalRequestId.trim() &&
                mcpPolicyDecision === "require_approval" &&
                mcpPolicyApprovalMode === "multi_stage" &&
                approvalWorkflowResult &&
                !approvalWorkflowResult.success
              ) {
                setMcpFeedback(null);
                setMcpError(approvalWorkflowResult.message);
                return;
              }
              if (
                !mcpEvaluateApprovalRequestId.trim() &&
                mcpPolicyDecision === "require_approval" &&
                staticApprovalConfigResult &&
                !staticApprovalConfigResult.success
              ) {
                setMcpFeedback(null);
                setMcpError(staticApprovalConfigResult.message);
                return;
              }
              setMcpFeedback(null);
              setMcpError(null);
              evaluateMcpToolMutation.mutate({
                toolId,
                approvalRequestId:
                  mcpEvaluateApprovalRequestId.trim() || undefined,
                reason: mcpEvaluateReason.trim() || undefined,
                evaluationTimestamp: normalizedEvaluationTimestamp
                  ? new Date(normalizedEvaluationTimestamp).toISOString()
                  : undefined,
                approvalConfig:
                  !mcpEvaluateApprovalRequestId.trim() &&
                  mcpPolicyDecision === "require_approval" &&
                  (mcpPolicyApprovalMode === "multi_stage"
                    ? Boolean(approvalWorkflowResult && approvalWorkflowResult.success)
                    : typeof stage1RequiredApprovals === "number")
                    ? {
                        mode: mcpPolicyApprovalMode,
                        ...(mcpPolicyApprovalMode === "multi_stage" &&
                        approvalWorkflowResult &&
                        approvalWorkflowResult.success
                          ? {
                              approvalWorkflow: approvalWorkflowResult.data,
                            }
                          : {}),
                        ...(mcpPolicyApprovalMode !== "multi_stage" &&
                        staticApprovalConfigResult &&
                        staticApprovalConfigResult.success
                          ? {
                              approvalStages: staticApprovalConfigResult.approvalStages,
                              stage1: {
                                nodeId: staticApprovalConfigResult.approvalStages[0]?.nodeId,
                                stage: staticApprovalConfigResult.approvalStages[0]?.stage,
                                label: staticApprovalConfigResult.approvalStages[0]?.label,
                                requiredApprovals:
                                  staticApprovalConfigResult.approvalStages[0]
                                    ?.requiredApprovals ?? 1,
                                roles:
                                  staticApprovalConfigResult.approvalStages[0]?.roles ?? [
                                    "owner",
                                    "maintainer",
                                  ],
                              },
                              ...(mcpPolicyApprovalMode === "two_stage"
                                ? {
                                    stage2: {
                                      nodeId:
                                        staticApprovalConfigResult.approvalStages[1]?.nodeId,
                                      stage:
                                        staticApprovalConfigResult.approvalStages[1]?.stage,
                                      label:
                                        staticApprovalConfigResult.approvalStages[1]?.label,
                                      requiredApprovals:
                                        staticApprovalConfigResult.approvalStages[1]
                                          ?.requiredApprovals ?? 1,
                                      roles:
                                        staticApprovalConfigResult.approvalStages[1]?.roles ?? [
                                          "owner",
                                        ],
                                    },
                                  }
                                : {}),
                            }
                          : {}),
                      }
                    : undefined,
              });
            }}
          >
            {evaluateMcpToolMutation.isPending ? "评估中..." : "执行 MCP 评估"}
          </button>
        </div>

        {mcpEvaluateResultPayload ? (
          <div className="table-wrapper">
            <table className="session-table">
              <thead>
                <tr>
                  <th>评估结果</th>
                  <th>值</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>toolId</td>
                  <td>{mcpEvaluateResultPayload.toolId}</td>
                </tr>
                <tr>
                  <td>decision</td>
                  <td>{mcpEvaluateResultPayload.decision}</td>
                </tr>
                <tr>
                  <td>result</td>
                  <td>{mcpEvaluateResultPayload.result}</td>
                </tr>
                <tr>
                  <td>approvalMode</td>
                  <td>{mcpEvaluateResultPayload.approvalMode ?? "--"}</td>
                </tr>
                <tr>
                  <td>currentNodeId</td>
                  <td>{mcpEvaluateResultPayload.currentNodeId ?? "--"}</td>
                </tr>
                <tr>
                  <td>currentStage</td>
                  <td>{mcpEvaluateResultPayload.currentStage ?? "--"}</td>
                </tr>
                <tr>
                  <td>pathHistory</td>
                  <td>{formatMcpPathHistory(mcpEvaluateResultPayload.pathHistory)}</td>
                </tr>
                <tr>
                  <td>nextTransitionPreview</td>
                  <td>
                    {formatMcpTransitionPreview(
                      mcpEvaluateResultPayload.nextTransitionPreview,
                    )}
                  </td>
                </tr>
                <tr>
                  <td>approvalStages</td>
                  <td>
                    {formatMcpApprovalStages(
                      mcpEvaluateResultPayload.approvalStages,
                    )}
                  </td>
                </tr>
                <tr>
                  <td>approvalWorkflow</td>
                  <td>
                    {formatMcpApprovalWorkflowSummary(
                      mcpEvaluateResultPayload.approvalWorkflow,
                    )}
                  </td>
                </tr>
                <tr>
                  <td>remainingApprovals</td>
                  <td>
                    {typeof mcpEvaluateResultPayload.remainingApprovals === "number"
                      ? mcpEvaluateResultPayload.remainingApprovals
                      : "--"}
                  </td>
                </tr>
                <tr>
                  <td>approvalConditionMatched</td>
                  <td>
                    {typeof mcpEvaluateResultPayload.approvalConditionMatched ===
                    "boolean"
                      ? mcpEvaluateResultPayload.approvalConditionMatched
                        ? "true"
                        : "false"
                      : "--"}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        ) : null}

        <div className="filters-row governance-inline-grid">
          <label className="inline-field" htmlFor="mcp-approval-status-filter">
            审批状态
            <select
              id="mcp-approval-status-filter"
              value={mcpApprovalStatusFilter}
              onChange={(event) =>
                setMcpApprovalStatusFilter(
                  event.target.value as McpApprovalRequest["status"] | "",
                )
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

          <label
            className="inline-field governance-wide-field"
            htmlFor="mcp-approval-reason"
          >
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
              const stage1Roles = parseDistinctCommaSeparatedList(
                mcpPolicyStage1Roles,
              );
              const stage2Roles = parseDistinctCommaSeparatedList(
                mcpPolicyStage2Roles,
              );
              const stage1RequiredApprovals = parseOptionalNonNegativeInteger(
                mcpPolicyStage1RequiredApprovals,
              );
              const stage2RequiredApprovals = parseOptionalNonNegativeInteger(
                mcpPolicyStage2RequiredApprovals,
              );
              const approvalWorkflowResult =
                mcpPolicyApprovalMode === "multi_stage"
                  ? buildMcpWorkflowFromDraft({
                      entryNodeId: mcpWorkflowEntryNodeId,
                      nodes: mcpWorkflowNodes,
                      transitions: mcpWorkflowTransitions,
                    })
                  : null;
              const staticApprovalConfigResult =
                mcpPolicyApprovalMode === "multi_stage"
                  ? null
                  : resolveMcpStaticApprovalConfig({
                      mode: mcpPolicyApprovalMode,
                      approvalStagesJson: mcpPolicyApprovalStagesJson,
                      approvalStagesJsonTouched: mcpPolicyApprovalStagesJsonTouched,
                      stage1RequiredApprovals,
                      stage2RequiredApprovals,
                      stage1Roles,
                      stage2Roles,
                    });
              if (!toolId) {
                setMcpFeedback(null);
                setMcpError("审批请求的 Tool ID 不能为空。");
                return;
              }
              if (
                mcpPolicyDecision === "require_approval" &&
                mcpPolicyApprovalMode === "multi_stage" &&
                approvalWorkflowResult &&
                !approvalWorkflowResult.success
              ) {
                setMcpFeedback(null);
                setMcpError(approvalWorkflowResult.message);
                return;
              }
              if (
                mcpPolicyDecision === "require_approval" &&
                staticApprovalConfigResult &&
                !staticApprovalConfigResult.success
              ) {
                setMcpFeedback(null);
                setMcpError(staticApprovalConfigResult.message);
                return;
              }
              setMcpFeedback(null);
              setMcpError(null);
              createMcpApprovalMutation.mutate({
                toolId,
                reason: mcpApprovalReason.trim() || undefined,
                approvalConfig:
                  mcpPolicyDecision === "require_approval" &&
                  (mcpPolicyApprovalMode === "multi_stage"
                    ? Boolean(approvalWorkflowResult && approvalWorkflowResult.success)
                    : typeof stage1RequiredApprovals === "number")
                    ? {
                        mode: mcpPolicyApprovalMode,
                        ...(mcpPolicyApprovalMode === "multi_stage" &&
                        approvalWorkflowResult &&
                        approvalWorkflowResult.success
                          ? {
                              approvalWorkflow: approvalWorkflowResult.data,
                            }
                          : {}),
                        ...(mcpPolicyApprovalMode !== "multi_stage" &&
                        staticApprovalConfigResult &&
                        staticApprovalConfigResult.success
                          ? {
                              approvalStages: staticApprovalConfigResult.approvalStages,
                              stage1: {
                                nodeId: staticApprovalConfigResult.approvalStages[0]?.nodeId,
                                stage: staticApprovalConfigResult.approvalStages[0]?.stage,
                                label: staticApprovalConfigResult.approvalStages[0]?.label,
                                requiredApprovals:
                                  staticApprovalConfigResult.approvalStages[0]
                                    ?.requiredApprovals ?? 1,
                                roles:
                                  staticApprovalConfigResult.approvalStages[0]?.roles ?? [
                                    "owner",
                                    "maintainer",
                                  ],
                              },
                              ...(mcpPolicyApprovalMode === "two_stage"
                                ? {
                                    stage2: {
                                      nodeId:
                                        staticApprovalConfigResult.approvalStages[1]?.nodeId,
                                      stage:
                                        staticApprovalConfigResult.approvalStages[1]?.stage,
                                      label:
                                        staticApprovalConfigResult.approvalStages[1]?.label,
                                      requiredApprovals:
                                        staticApprovalConfigResult.approvalStages[1]
                                          ?.requiredApprovals ?? 1,
                                      roles:
                                        staticApprovalConfigResult.approvalStages[1]?.roles ?? [
                                          "owner",
                                        ],
                                    },
                                  }
                                : {}),
                            }
                          : {}),
                      }
                    : undefined,
              });
            }}
          >
            {createMcpApprovalMutation.isPending ? "提交中..." : "提交审批请求"}
          </button>
        </div>

        <div className="filters-row">
          <label
            className="inline-field governance-wide-field"
            htmlFor="mcp-review-reason"
          >
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

        {mcpPoliciesQuery.isLoading ||
        mcpApprovalsQuery.isLoading ||
        mcpInvocationsQuery.isLoading ? (
          <p className="feedback info">MCP 数据加载中...</p>
        ) : null}
        {mcpPoliciesQuery.isError ? (
          <p className="feedback error">
            MCP 策略加载失败：{toErrorMessage(mcpPoliciesQuery.error)}
          </p>
        ) : null}
        {mcpApprovalsQuery.isError ? (
          <p className="feedback error">
            审批列表加载失败：{toErrorMessage(mcpApprovalsQuery.error)}
          </p>
        ) : null}
        {mcpInvocationsQuery.isError ? (
          <p className="feedback error">
            调用审计加载失败：{toErrorMessage(mcpInvocationsQuery.error)}
          </p>
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
                <th>模式/阶段</th>
                <th>剩余</th>
                <th>申请人</th>
                <th>创建时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {mcpApprovalItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={8}>
                    暂无审批请求
                  </td>
                </tr>
              ) : (
                mcpApprovalItems.map((approval) => {
                  const isMutating =
                    reviewMcpApprovalMutation.isPending &&
                    reviewMcpApprovalMutation.variables?.approvalId ===
                      approval.id;
                  return (
                    <tr key={approval.id}>
                      <td>{approval.id}</td>
                      <td>{approval.toolId}</td>
                      <td>{approval.status}</td>
                      <td>
                        {approval.approvalMode
                          ? `${approval.approvalMode} / ${
                              approval.currentNodeId ?? approval.currentStage ?? "--"
                            } / ${
                              approval.approvalWorkflow
                                ? formatMcpApprovalWorkflowSummary(approval.approvalWorkflow)
                                : formatMcpApprovalStages(approval.approvalStages)
                            }`
                          : "--"}
                      </td>
                      <td>
                        {typeof approval.remainingApprovals === "number"
                          ? approval.remainingApprovals
                          : "--"}
                      </td>
                      <td>
                        {approval.requestedByEmail ??
                          approval.requestedByUserId}
                        <div className="secondary-text">
                          {formatMcpPathHistory(approval.pathHistory)}
                        </div>
                      </td>
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
                                  nodeId: approval.currentNodeId,
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
                                  nodeId: approval.currentNodeId,
                                })
                              }
                            >
                              拒绝
                            </button>
                          </div>
                        ) : (
                          <span className="tiny-feedback tiny-feedback-success">
                            已处理
                          </span>
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
          <label
            className="inline-field governance-wide-field"
            htmlFor="mcp-invocation-tool-id"
          >
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
                <th>Approval</th>
                <th>审批请求</th>
                <th>时间</th>
              </tr>
            </thead>
            <tbody>
              {mcpInvocationItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={7}>
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
                    <td>
                      {item.approvalMode
                        ? `${item.approvalMode} / ${item.approvalStage ?? "--"} / ${
                            item.approvalSatisfied === true ? "done" : "pending"
                          }`
                        : "--"}
                    </td>
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
          <h2>开放平台工作台</h2>
          <p>OpenAPI、API Key、Webhook、Quality、Replay 一站式治理。</p>
        </header>

        <div className="open-platform-grid">
          <article className="open-platform-card">
            <h3>OpenAPI 摘要</h3>
            <p>查询当前租户开放接口规模与标签分布。</p>
            <button
              type="button"
              className="submit-button"
              disabled={loadOpenApiSummaryMutation.isPending}
              onClick={() => {
                setOpenApiFeedback(null);
                setOpenApiError(null);
                loadOpenApiSummaryMutation.mutate();
              }}
            >
              {loadOpenApiSummaryMutation.isPending
                ? "加载中..."
                : "加载 OpenAPI 摘要"}
            </button>
            {openApiFeedback ? (
              <p className="feedback success">{openApiFeedback}</p>
            ) : null}
            {openApiError ? (
              <p className="feedback error">{openApiError}</p>
            ) : null}
            {openApiSummaryPayload ? (
              <div className="open-platform-summary-list">
                <p>
                  version: <strong>{openApiSummaryPayload.version}</strong>
                </p>
                <p>
                  paths:{" "}
                  <strong>
                    {openApiSummaryPayload.totalPaths.toLocaleString("zh-CN")}
                  </strong>
                </p>
                <p>
                  operations:{" "}
                  <strong>
                    {openApiSummaryPayload.totalOperations.toLocaleString(
                      "zh-CN",
                    )}
                  </strong>
                </p>
                <p>
                  generatedAt:{" "}
                  <strong>
                    {formatDateTime(openApiSummaryPayload.generatedAt)}
                  </strong>
                </p>
                <div className="table-wrapper">
                  <table className="session-table">
                    <thead>
                      <tr>
                        <th>tag</th>
                        <th>operations</th>
                      </tr>
                    </thead>
                    <tbody>
                      {openApiSummaryPayload.tags.length === 0 ? (
                        <tr>
                          <td className="table-empty-cell" colSpan={2}>
                            当前无 tags
                          </td>
                        </tr>
                      ) : (
                        openApiSummaryPayload.tags.map((tag) => (
                          <tr key={tag.tag}>
                            <td>{tag.tag}</td>
                            <td>{tag.operations.toLocaleString("zh-CN")}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : hasLoadedOpenApiSummary ? (
              <p className="feedback empty">已加载，但未返回摘要数据。</p>
            ) : (
              <p className="feedback empty">尚未加载 OpenAPI 摘要。</p>
            )}
          </article>

          <article className="open-platform-card">
            <h3>API Key 管理</h3>
            <p>支持列表查询与 API Key 更新。</p>
            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-api-key-status-filter"
              >
                状态（API Key）
                <select
                  id="open-platform-api-key-status-filter"
                  value={apiKeyStatusFilter}
                  onChange={(event) =>
                    setApiKeyStatusFilter(
                      event.target.value as OpenPlatformApiKeyStatus | "",
                    )
                  }
                >
                  {OPEN_PLATFORM_API_KEY_STATUS_FILTER_OPTIONS.map((option) => (
                    <option key={option.value || "all"} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-api-key-keyword"
              >
                关键字（API Key）
                <input
                  id="open-platform-api-key-keyword"
                  type="text"
                  value={apiKeyKeyword}
                  onChange={(event) => setApiKeyKeyword(event.target.value)}
                  placeholder="按 ID 或名称筛选"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadApiKeysMutation.isPending}
                onClick={() => {
                  setApiKeyFeedback(null);
                  setApiKeyError(null);
                  loadApiKeysMutation.mutate(apiKeyQueryInput);
                }}
              >
                {loadApiKeysMutation.isPending
                  ? "加载中..."
                  : "加载 API Key 列表"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-api-key-id"
              >
                API Key ID
                <input
                  id="open-platform-api-key-id"
                  type="text"
                  value={apiKeyId}
                  onChange={(event) => setApiKeyId(event.target.value)}
                  placeholder="例如：key-agent-build"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-api-key-name"
              >
                API Key 名称
                <input
                  id="open-platform-api-key-name"
                  type="text"
                  value={apiKeyName}
                  onChange={(event) => setApiKeyName(event.target.value)}
                  placeholder="例如：CI 机器人"
                />
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-api-key-scopes"
              >
                scopes（逗号分隔）
                <input
                  id="open-platform-api-key-scopes"
                  type="text"
                  value={apiKeyScopesInput}
                  onChange={(event) => setApiKeyScopesInput(event.target.value)}
                  placeholder="read,write,admin"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-api-key-expires-at"
              >
                过期日期（可选）
                <input
                  id="open-platform-api-key-expires-at"
                  type="date"
                  value={apiKeyExpiresAt}
                  onChange={(event) => setApiKeyExpiresAt(event.target.value)}
                />
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-api-key-revoke-reason"
              >
                吊销原因（可选）
                <input
                  id="open-platform-api-key-revoke-reason"
                  type="text"
                  value={apiKeyRevokeReason}
                  onChange={(event) =>
                    setApiKeyRevokeReason(event.target.value)
                  }
                  placeholder="例如：密钥轮换"
                />
              </label>

              <label
                className="checkbox-field"
                htmlFor="open-platform-api-key-enabled"
              >
                <input
                  id="open-platform-api-key-enabled"
                  type="checkbox"
                  checked={apiKeyEnabled}
                  onChange={(event) => setApiKeyEnabled(event.target.checked)}
                />
                启用 API Key
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={upsertApiKeyMutation.isPending}
                onClick={() => {
                  const normalizedApiKeyId = apiKeyId.trim();
                  const normalizedName = apiKeyName.trim();
                  const rawScopes = parseCommaSeparatedValues(
                    apiKeyScopesInput,
                  ).map((item) => item.toLowerCase());
                  const invalidScopes = rawScopes.filter(
                    (item) => !["read", "write", "admin"].includes(item),
                  );
                  const scopes = rawScopes.filter((item) =>
                    ["read", "write", "admin"].includes(item),
                  );
                  const expiresAt = apiKeyExpiresAt.trim();
                  if (!normalizedApiKeyId) {
                    setApiKeyFeedback(null);
                    setApiKeyError("API Key ID 不能为空。");
                    return;
                  }
                  if (!normalizedName) {
                    setApiKeyFeedback(null);
                    setApiKeyError("API Key 名称不能为空。");
                    return;
                  }
                  if (scopes.length === 0) {
                    setApiKeyFeedback(null);
                    setApiKeyError("至少填写一个 scope。");
                    return;
                  }
                  if (invalidScopes.length > 0) {
                    setApiKeyFeedback(null);
                    setApiKeyError(
                      `存在不支持的 scope：${invalidScopes.join(",")}。可选值：read,write,admin`,
                    );
                    return;
                  }
                  if (expiresAt && Number.isNaN(Date.parse(expiresAt))) {
                    setApiKeyFeedback(null);
                    setApiKeyError("过期日期格式不合法。");
                    return;
                  }
                  setApiKeyFeedback(null);
                  setApiKeyError(null);
                  upsertApiKeyMutation.mutate({
                    keyId: normalizedApiKeyId,
                    input: {
                      name: normalizedName,
                      scopes,
                      enabled: apiKeyEnabled,
                      expiresAt: expiresAt || undefined,
                    },
                  });
                }}
              >
                {upsertApiKeyMutation.isPending ? "保存中..." : "保存 API Key"}
              </button>
            </div>

            {apiKeyFeedback ? (
              <p className="feedback success">{apiKeyFeedback}</p>
            ) : null}
            {apiKeyError ? (
              <p className="feedback error">{apiKeyError}</p>
            ) : null}

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>名称</th>
                    <th>状态</th>
                    <th>Scopes</th>
                    <th>过期时间</th>
                    <th>最近使用</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {apiKeyItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={7}>
                        {hasLoadedApiKeys
                          ? "无匹配 API Key。"
                          : "尚未加载 API Key，请点击“加载 API Key 列表”。"}
                      </td>
                    </tr>
                  ) : (
                    apiKeyItems.map((item) => (
                      <tr key={item.id}>
                        <td>{item.id}</td>
                        <td>{item.name}</td>
                        <td>{item.status}</td>
                        <td>{item.scopes.join(",")}</td>
                        <td>
                          {item.expiresAt
                            ? formatDateTime(item.expiresAt)
                            : "--"}
                        </td>
                        <td>
                          {item.lastUsedAt
                            ? formatDateTime(item.lastUsedAt)
                            : "--"}
                        </td>
                        <td>
                          <div className="governance-action-row">
                            <button
                              type="button"
                              className="table-action"
                              onClick={() => {
                                setApiKeyId(item.id);
                                setApiKeyName(item.name);
                                setApiKeyScopesInput(item.scopes.join(","));
                                setApiKeyEnabled(item.status === "active");
                                setApiKeyExpiresAt(
                                  item.expiresAt
                                    ? item.expiresAt.slice(0, 10)
                                    : "",
                                );
                              }}
                            >
                              载入
                            </button>
                            {item.status === "active" ? (
                              <button
                                type="button"
                                className="table-action"
                                disabled={
                                  revokeApiKeyMutation.isPending &&
                                  revokeApiKeyMutation.variables?.keyId ===
                                    item.id
                                }
                                onClick={() => {
                                  setApiKeyFeedback(null);
                                  setApiKeyError(null);
                                  revokeApiKeyMutation.mutate({
                                    keyId: item.id,
                                    reason:
                                      apiKeyRevokeReason.trim() || undefined,
                                  });
                                }}
                              >
                                {revokeApiKeyMutation.isPending &&
                                revokeApiKeyMutation.variables?.keyId ===
                                  item.id
                                  ? "吊销中..."
                                  : "吊销"}
                              </button>
                            ) : (
                              <span className="tiny-feedback tiny-feedback-success">
                                已吊销
                              </span>
                            )}
                          </div>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </article>

          <article className="open-platform-card">
            <h3>Webhook 管理</h3>
            <p>支持 webhook 列表查询和配置更新。</p>
            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-webhook-enabled-filter"
              >
                启用状态（Webhook）
                <select
                  id="open-platform-webhook-enabled-filter"
                  value={webhookEnabledFilter}
                  onChange={(event) =>
                    setWebhookEnabledFilter(
                      event.target.value as "" | "true" | "false",
                    )
                  }
                >
                  {OPEN_PLATFORM_WEBHOOK_ENABLED_FILTER_OPTIONS.map(
                    (option) => (
                      <option key={option.value || "all"} value={option.value}>
                        {option.label}
                      </option>
                    ),
                  )}
                </select>
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-webhook-keyword"
              >
                关键字（Webhook）
                <input
                  id="open-platform-webhook-keyword"
                  type="text"
                  value={webhookKeyword}
                  onChange={(event) => setWebhookKeyword(event.target.value)}
                  placeholder="按 ID、名称或 URL 过滤"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadWebhooksMutation.isPending}
                onClick={() => {
                  setWebhookFeedback(null);
                  setWebhookError(null);
                  loadWebhooksMutation.mutate(webhookQueryInput);
                }}
              >
                {loadWebhooksMutation.isPending
                  ? "加载中..."
                  : "加载 Webhook 列表"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-webhook-id"
              >
                Webhook ID
                <input
                  id="open-platform-webhook-id"
                  type="text"
                  value={webhookId}
                  onChange={(event) => setWebhookId(event.target.value)}
                  placeholder="例如：webhook-alert-1"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-webhook-name"
              >
                Webhook 名称
                <input
                  id="open-platform-webhook-name"
                  type="text"
                  value={webhookName}
                  onChange={(event) => setWebhookName(event.target.value)}
                  placeholder="例如：告警通知"
                />
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-webhook-url"
              >
                回调 URL
                <input
                  id="open-platform-webhook-url"
                  type="url"
                  value={webhookUrl}
                  onChange={(event) => setWebhookUrl(event.target.value)}
                  placeholder="https://hooks.example.com/alert"
                />
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-webhook-events"
              >
                events（逗号分隔）
                <input
                  id="open-platform-webhook-events"
                  type="text"
                  list="open-platform-webhook-event-options"
                  value={webhookEventsInput}
                  onChange={(event) =>
                    setWebhookEventsInput(event.target.value)
                  }
                  placeholder="replay.run.started,replay.run.completed"
                />
              </label>

              <label
                className="checkbox-field"
                htmlFor="open-platform-webhook-enabled"
              >
                <input
                  id="open-platform-webhook-enabled"
                  type="checkbox"
                  checked={webhookEnabled}
                  onChange={(event) => setWebhookEnabled(event.target.checked)}
                />
                启用 Webhook
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={upsertWebhookMutation.isPending}
                onClick={() => {
                  const normalizedWebhookId = webhookId.trim();
                  const normalizedName = webhookName.trim();
                  const normalizedUrl = webhookUrl.trim();
                  const events = Array.from(
                    new Set(
                      parseCommaSeparatedValues(webhookEventsInput).map(
                        (item) => item.toLowerCase(),
                      ),
                    ),
                  );
                  if (!normalizedWebhookId) {
                    setWebhookFeedback(null);
                    setWebhookError("Webhook ID 不能为空。");
                    return;
                  }
                  if (!normalizedName) {
                    setWebhookFeedback(null);
                    setWebhookError("Webhook 名称不能为空。");
                    return;
                  }
                  if (!isValidHttpUrl(normalizedUrl)) {
                    setWebhookFeedback(null);
                    setWebhookError("Webhook URL 必须是 http/https 地址。");
                    return;
                  }
                  if (events.length === 0) {
                    setWebhookFeedback(null);
                    setWebhookError("至少填写一个事件名。");
                    return;
                  }
                  const invalidEvents = events.filter(
                    (item) => !OPEN_PLATFORM_WEBHOOK_EVENT_OPTION_SET.has(item),
                  );
                  if (invalidEvents.length > 0) {
                    setWebhookFeedback(null);
                    setWebhookError(
                      `事件名不合法：${invalidEvents.join(",")}。推荐优先使用 replay.run.*；可选值：${OPEN_PLATFORM_WEBHOOK_EVENT_OPTIONS.join(",")}`,
                    );
                    return;
                  }
                  setWebhookFeedback(null);
                  setWebhookError(null);
                  upsertWebhookMutation.mutate({
                    webhookId: normalizedWebhookId,
                    input: {
                      name: normalizedName,
                      url: normalizedUrl,
                      events,
                      enabled: webhookEnabled,
                    },
                  });
                }}
              >
                {upsertWebhookMutation.isPending ? "保存中..." : "保存 Webhook"}
              </button>
            </div>

            <datalist id="open-platform-webhook-event-options">
              {OPEN_PLATFORM_WEBHOOK_EVENT_OPTIONS.map((eventType) => (
                <option key={eventType} value={eventType} />
              ))}
            </datalist>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-webhook-replay-event-type"
              >
                回放事件类型（可选）
                <input
                  id="open-platform-webhook-replay-event-type"
                  type="text"
                  list="open-platform-webhook-event-options"
                  value={webhookReplayEventType}
                  onChange={(event) =>
                    setWebhookReplayEventType(event.target.value)
                  }
                  placeholder="例如：replay.run.completed"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-webhook-replay-from"
              >
                回放起始时间（可选）
                <input
                  id="open-platform-webhook-replay-from"
                  type="datetime-local"
                  value={webhookReplayFrom}
                  onChange={(event) => setWebhookReplayFrom(event.target.value)}
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-webhook-replay-to"
              >
                回放结束时间（可选）
                <input
                  id="open-platform-webhook-replay-to"
                  type="datetime-local"
                  value={webhookReplayTo}
                  onChange={(event) => setWebhookReplayTo(event.target.value)}
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-webhook-replay-limit"
              >
                回放条数上限
                <input
                  id="open-platform-webhook-replay-limit"
                  type="number"
                  min={1}
                  step={1}
                  value={webhookReplayLimit}
                  onChange={(event) =>
                    setWebhookReplayLimit(event.target.value)
                  }
                />
              </label>

              <label
                className="checkbox-field"
                htmlFor="open-platform-webhook-replay-dry-run"
              >
                <input
                  id="open-platform-webhook-replay-dry-run"
                  type="checkbox"
                  checked={webhookReplayDryRun}
                  onChange={(event) =>
                    setWebhookReplayDryRun(event.target.checked)
                  }
                />
                Dry Run
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={replayWebhookMutation.isPending}
                onClick={() => {
                  const normalizedWebhookId = webhookId.trim();
                  const normalizedEventType = webhookReplayEventType.trim();
                  const normalizedFrom = webhookReplayFrom.trim();
                  const normalizedTo = webhookReplayTo.trim();
                  const replayLimit = Number(webhookReplayLimit);
                  if (!normalizedWebhookId) {
                    setWebhookFeedback(null);
                    setWebhookError("回放前请先填写或载入 Webhook ID。");
                    return;
                  }
                  if (
                    normalizedEventType &&
                    !OPEN_PLATFORM_WEBHOOK_EVENT_OPTION_SET.has(
                      normalizedEventType,
                    )
                  ) {
                    setWebhookFeedback(null);
                    setWebhookError(
                      `事件类型不合法：${normalizedEventType}。推荐优先使用 replay.run.*；可选值：${OPEN_PLATFORM_WEBHOOK_EVENT_OPTIONS.join(",")}`,
                    );
                    return;
                  }
                  if (!Number.isInteger(replayLimit) || replayLimit <= 0) {
                    setWebhookFeedback(null);
                    setWebhookError("回放条数上限必须是正整数。");
                    return;
                  }
                  const fromTs = normalizedFrom
                    ? Date.parse(normalizedFrom)
                    : NaN;
                  const toTs = normalizedTo ? Date.parse(normalizedTo) : NaN;
                  if (normalizedFrom && Number.isNaN(fromTs)) {
                    setWebhookFeedback(null);
                    setWebhookError("回放起始时间格式不合法。");
                    return;
                  }
                  if (normalizedTo && Number.isNaN(toTs)) {
                    setWebhookFeedback(null);
                    setWebhookError("回放结束时间格式不合法。");
                    return;
                  }
                  if (
                    !Number.isNaN(fromTs) &&
                    !Number.isNaN(toTs) &&
                    fromTs > toTs
                  ) {
                    setWebhookFeedback(null);
                    setWebhookError(
                      "回放时间范围非法：起始时间不能晚于结束时间。",
                    );
                    return;
                  }
                  setWebhookFeedback(null);
                  setWebhookError(null);
                  replayWebhookMutation.mutate({
                    webhookId: normalizedWebhookId,
                    input: {
                      eventType: normalizedEventType || undefined,
                      from: normalizedFrom
                        ? new Date(fromTs).toISOString()
                        : undefined,
                      to: normalizedTo
                        ? new Date(toTs).toISOString()
                        : undefined,
                      limit: replayLimit,
                      dryRun: webhookReplayDryRun,
                    },
                  });
                }}
              >
                {replayWebhookMutation.isPending ? "回放中..." : "回放 Webhook"}
              </button>
            </div>

            {webhookFeedback ? (
              <p className="feedback success">{webhookFeedback}</p>
            ) : null}
            {webhookError ? (
              <p className="feedback error">{webhookError}</p>
            ) : null}

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>名称</th>
                    <th>URL</th>
                    <th>events</th>
                    <th>enabled</th>
                    <th>最近投递</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {webhookItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={7}>
                        {hasLoadedWebhooks
                          ? "无匹配 Webhook。"
                          : "尚未加载 Webhook，请点击“加载 Webhook 列表”。"}
                      </td>
                    </tr>
                  ) : (
                    webhookItems.map((item) => (
                      <tr key={item.id}>
                        <td>{item.id}</td>
                        <td>{item.name}</td>
                        <td>{item.url}</td>
                        <td>{item.events.join(",")}</td>
                        <td>{item.enabled ? "true" : "false"}</td>
                        <td>
                          {item.lastDeliveryAt
                            ? formatDateTime(item.lastDeliveryAt)
                            : "--"}
                        </td>
                        <td>
                          <div className="governance-action-row">
                            <button
                              type="button"
                              className="table-action"
                              onClick={() => {
                                setWebhookId(item.id);
                                setWebhookName(item.name);
                                setWebhookUrl(item.url);
                                setWebhookEventsInput(item.events.join(","));
                                setWebhookEnabled(item.enabled);
                              }}
                            >
                              载入
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              disabled={
                                deleteWebhookMutation.isPending &&
                                deleteWebhookMutation.variables?.webhookId ===
                                  item.id
                              }
                              onClick={() => {
                                setWebhookFeedback(null);
                                setWebhookError(null);
                                deleteWebhookMutation.mutate({
                                  webhookId: item.id,
                                });
                              }}
                            >
                              {deleteWebhookMutation.isPending &&
                              deleteWebhookMutation.variables?.webhookId ===
                                item.id
                                ? "删除中..."
                                : "删除"}
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </article>

          <article className="open-platform-card">
            <h3>Quality（daily/project-trends/scorecards）</h3>
            <p>
              按日查看质量指标，并拉通项目级质量、成本、tokens、sessions 趋势。
            </p>
            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-quality-daily-date"
              >
                日期（Quality daily）
                <input
                  id="open-platform-quality-daily-date"
                  type="date"
                  value={qualityDailyDate}
                  onChange={(event) => setQualityDailyDate(event.target.value)}
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-daily-metric"
              >
                指标（Quality daily）
                <select
                  id="open-platform-quality-daily-metric"
                  value={qualityDailyMetric}
                  onChange={(event) =>
                    setQualityDailyMetric(event.target.value)
                  }
                >
                  {OPEN_PLATFORM_QUALITY_METRIC_OPTIONS.map((option) => (
                    <option key={option.label} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-daily-provider"
              >
                provider
                <input
                  id="open-platform-quality-daily-provider"
                  type="text"
                  value={qualityDailyProvider}
                  onChange={(event) =>
                    setQualityDailyProvider(event.target.value)
                  }
                  placeholder="可选，例如：github"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-daily-repo"
              >
                repo
                <input
                  id="open-platform-quality-daily-repo"
                  type="text"
                  value={qualityDailyRepo}
                  onChange={(event) => setQualityDailyRepo(event.target.value)}
                  placeholder="可选，例如：agentledger/main"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-daily-workflow"
              >
                workflow
                <input
                  id="open-platform-quality-daily-workflow"
                  type="text"
                  value={qualityDailyWorkflow}
                  onChange={(event) =>
                    setQualityDailyWorkflow(event.target.value)
                  }
                  placeholder="可选"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-daily-run-id"
              >
                runId
                <input
                  id="open-platform-quality-daily-run-id"
                  type="text"
                  value={qualityDailyRunId}
                  onChange={(event) => setQualityDailyRunId(event.target.value)}
                  placeholder="可选"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-daily-group-by"
              >
                groupBy
                <select
                  id="open-platform-quality-daily-group-by"
                  value={qualityDailyGroupBy}
                  onChange={(event) =>
                    setQualityDailyGroupBy(
                      event.target.value as
                        | ""
                        | "provider"
                        | "repo"
                        | "workflow"
                        | "runId",
                    )
                  }
                >
                  <option value="">不分组</option>
                  <option value="provider">provider</option>
                  <option value="repo">repo</option>
                  <option value="workflow">workflow</option>
                  <option value="runId">runId</option>
                </select>
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadQualityDailyMutation.isPending}
                onClick={() => {
                  if (
                    qualityDailyDate.trim() &&
                    Number.isNaN(Date.parse(qualityDailyDate))
                  ) {
                    setQualityFeedback(null);
                    setQualityError("Quality daily 日期格式不合法。");
                    return;
                  }
                  setQualityFeedback(null);
                  setQualityError(null);
                  loadQualityDailyMutation.mutate({
                    date: qualityDailyDate.trim() || undefined,
                    metric: qualityDailyMetric.trim() || undefined,
                    provider: qualityDailyProvider.trim() || undefined,
                    repo: qualityDailyRepo.trim() || undefined,
                    workflow: qualityDailyWorkflow.trim() || undefined,
                    runId: qualityDailyRunId.trim() || undefined,
                    groupBy: qualityDailyGroupBy || undefined,
                    limit: 50,
                  });
                }}
              >
                {loadQualityDailyMutation.isPending
                  ? "查询中..."
                  : "加载 Quality daily"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-quality-project-trends-from"
              >
                开始日期（project-trends）
                <input
                  id="open-platform-quality-project-trends-from"
                  type="date"
                  value={qualityProjectTrendsFrom}
                  onChange={(event) =>
                    setQualityProjectTrendsFrom(event.target.value)
                  }
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-project-trends-to"
              >
                结束日期（project-trends）
                <input
                  id="open-platform-quality-project-trends-to"
                  type="date"
                  value={qualityProjectTrendsTo}
                  onChange={(event) =>
                    setQualityProjectTrendsTo(event.target.value)
                  }
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-project-trends-metric"
              >
                指标（project-trends）
                <select
                  id="open-platform-quality-project-trends-metric"
                  value={qualityProjectTrendsMetric}
                  onChange={(event) =>
                    setQualityProjectTrendsMetric(event.target.value)
                  }
                >
                  {OPEN_PLATFORM_QUALITY_METRIC_OPTIONS.map((option) => (
                    <option
                      key={`project-trend-${option.label}`}
                      value={option.value}
                    >
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-project-trends-provider"
              >
                provider
                <input
                  id="open-platform-quality-project-trends-provider"
                  type="text"
                  value={qualityProjectTrendsProvider}
                  onChange={(event) =>
                    setQualityProjectTrendsProvider(event.target.value)
                  }
                  placeholder="可选"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-project-trends-workflow"
              >
                workflow
                <input
                  id="open-platform-quality-project-trends-workflow"
                  type="text"
                  value={qualityProjectTrendsWorkflow}
                  onChange={(event) =>
                    setQualityProjectTrendsWorkflow(event.target.value)
                  }
                  placeholder="可选"
                />
              </label>

              <label
                className="checkbox-field"
                htmlFor="open-platform-quality-project-trends-include-unknown"
              >
                <input
                  id="open-platform-quality-project-trends-include-unknown"
                  type="checkbox"
                  checked={qualityProjectTrendsIncludeUnknown}
                  onChange={(event) =>
                    setQualityProjectTrendsIncludeUnknown(event.target.checked)
                  }
                />
                包含 unknown 项目
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadQualityProjectTrendsMutation.isPending}
                onClick={() => {
                  const from = qualityProjectTrendsFrom.trim();
                  const to = qualityProjectTrendsTo.trim();
                  if (from && Number.isNaN(Date.parse(from))) {
                    setQualityFeedback(null);
                    setQualityError(
                      "Quality project-trends 开始日期格式不合法。",
                    );
                    return;
                  }
                  if (to && Number.isNaN(Date.parse(to))) {
                    setQualityFeedback(null);
                    setQualityError(
                      "Quality project-trends 结束日期格式不合法。",
                    );
                    return;
                  }
                  if (from && to && Date.parse(from) > Date.parse(to)) {
                    setQualityFeedback(null);
                    setQualityError(
                      "Quality project-trends 时间范围非法：开始日期不能晚于结束日期。",
                    );
                    return;
                  }
                  setQualityFeedback(null);
                  setQualityError(null);
                  loadQualityProjectTrendsMutation.mutate({
                    from: from || undefined,
                    to: to || undefined,
                    metric: qualityProjectTrendsMetric.trim() || undefined,
                    provider: qualityProjectTrendsProvider.trim() || undefined,
                    workflow: qualityProjectTrendsWorkflow.trim() || undefined,
                    includeUnknown: qualityProjectTrendsIncludeUnknown,
                    limit: 20,
                  });
                }}
              >
                {loadQualityProjectTrendsMutation.isPending
                  ? "查询中..."
                  : "加载 Quality project-trends"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-quality-automation-risk-level"
              >
                风险等级（automation）
                <select
                  id="open-platform-quality-automation-risk-level"
                  value={qualityAutomationRiskLevel}
                  onChange={(event) =>
                    setQualityAutomationRiskLevel(
                      event.target.value as McpRiskLevel,
                    )
                  }
                >
                  {MCP_RISK_LEVEL_OPTIONS.map((option) => (
                    <option
                      key={`quality-automation-${option.value}`}
                      value={option.value}
                    >
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-automation-decision"
              >
                决策（automation）
                <select
                  id="open-platform-quality-automation-decision"
                  value={qualityAutomationDecision}
                  onChange={(event) =>
                    setQualityAutomationDecision(
                      event.target.value as McpToolDecision,
                    )
                  }
                >
                  {MCP_DECISION_OPTIONS.map((option) => (
                    <option
                      key={`quality-automation-decision-${option.value}`}
                      value={option.value}
                    >
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-quality-automation-threshold"
              >
                阈值（automation）
                <input
                  id="open-platform-quality-automation-threshold"
                  type="number"
                  min="0"
                  max="100"
                  step="0.1"
                  value={qualityAutomationEvaluationScoreThreshold}
                  onChange={(event) =>
                    setQualityAutomationEvaluationScoreThreshold(
                      event.target.value,
                    )
                  }
                />
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-quality-automation-reason"
              >
                策略说明（automation）
                <input
                  id="open-platform-quality-automation-reason"
                  type="text"
                  value={qualityAutomationReason}
                  onChange={(event) =>
                    setQualityAutomationReason(event.target.value)
                  }
                  placeholder="例如：失败评估默认自动升级为建议执行"
                />
              </label>

              <label className="inline-field checkbox-field">
                <input
                  type="checkbox"
                  checked={qualityAutomationTriggerOnEvaluationFailure}
                  onChange={(event) =>
                    setQualityAutomationTriggerOnEvaluationFailure(
                      event.target.checked,
                    )
                  }
                />
                评估失败时自动触发
              </label>

              <label className="inline-field checkbox-field">
                <input
                  type="checkbox"
                  checked={qualityAutomationTriggerOnReplayRegression}
                  onChange={(event) =>
                    setQualityAutomationTriggerOnReplayRegression(
                      event.target.checked,
                    )
                  }
                />
                回放回退时自动触发
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-quality-automation-strategy-matrix"
              >
                策略矩阵（strategyMatrix JSON）
                <textarea
                  id="open-platform-quality-automation-strategy-matrix"
                  rows={8}
                  value={qualityAutomationStrategyMatrixJson}
                  onChange={(event) =>
                    setQualityAutomationStrategyMatrixJson(event.target.value)
                  }
                  placeholder='[{"id":"critical-replay","severity":"critical","actionType":"replay_experiment","requiresApproval":true,"reason":"高风险先做回放"}]'
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={
                  upsertQualityAutomationPolicyMutation.isPending ||
                  qualityAutomationPolicyQuery.isLoading
                }
                onClick={() => {
                  setQualityFeedback(null);
                  setQualityError(null);
                  const parsedThreshold = Number(
                    qualityAutomationEvaluationScoreThreshold,
                  );
                  if (!Number.isFinite(parsedThreshold)) {
                    setQualityError("automation threshold 必须是数字。");
                    return;
                  }
                  if (parsedThreshold < 0 || parsedThreshold > 100) {
                    setQualityError(
                      "automation threshold 必须在 0 到 100 之间。",
                    );
                    return;
                  }
                  const strategyMatrixResult =
                    parseQualityAutomationStrategyMatrixJson(
                      qualityAutomationStrategyMatrixJson,
                    );
                  if (!strategyMatrixResult.success) {
                    setQualityError(strategyMatrixResult.message);
                    return;
                  }
                  upsertQualityAutomationPolicyMutation.mutate({
                    riskLevel: qualityAutomationRiskLevel,
                    decision: qualityAutomationDecision,
                    reason: qualityAutomationReason.trim() || undefined,
                    evaluationScoreThreshold: parsedThreshold,
                    triggerOnEvaluationFailure:
                      qualityAutomationTriggerOnEvaluationFailure,
                    triggerOnReplayRegression:
                      qualityAutomationTriggerOnReplayRegression,
                    strategyMatrix: strategyMatrixResult.data,
                  });
                }}
              >
                {upsertQualityAutomationPolicyMutation.isPending
                  ? "保存中..."
                  : "保存 automation policy"}
              </button>
            </div>

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>automation tool</th>
                    <th>scope</th>
                    <th>threshold</th>
                    <th>defaultAction</th>
                    <th>decision</th>
                    <th>reason</th>
                    <th>updatedAt</th>
                  </tr>
                </thead>
                <tbody>
                  {!qualityAutomationPolicy ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={7}>
                        {qualityAutomationPolicyQuery.isLoading
                          ? "正在加载 automation policy。"
                          : "尚未加载 automation policy。"}
                      </td>
                    </tr>
                  ) : (
                    <tr>
                      <td>{qualityAutomationPolicy.toolId}</td>
                      <td>{qualityAutomationPolicy.scope}</td>
                      <td>{qualityAutomationPolicy.evaluationScoreThreshold}</td>
                      <td>{qualityAutomationPolicy.defaultActionType ?? "--"}</td>
                      <td>{qualityAutomationPolicy.decision}</td>
                      <td>{qualityAutomationPolicy.reason ?? "--"}</td>
                      <td>{formatDateTime(qualityAutomationPolicy.updatedAt)}</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>ruleId</th>
                    <th>metric</th>
                    <th>severity</th>
                    <th>trend</th>
                    <th>minConfidence</th>
                    <th>regressionProbability</th>
                    <th>replayRegression</th>
                    <th>action</th>
                    <th>approval</th>
                    <th>cooldown</th>
                    <th>reason</th>
                  </tr>
                </thead>
                <tbody>
                  {(qualityAutomationPolicy?.strategyMatrix ?? []).length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={11}>
                        当前未配置 strategyMatrix，默认沿用基础 automation policy。
                      </td>
                    </tr>
                  ) : (
                    (qualityAutomationPolicy?.strategyMatrix ?? []).map((rule) => (
                      <tr key={rule.id}>
                        <td>{rule.id}</td>
                        <td>{rule.metric ?? "--"}</td>
                        <td>{rule.severity ?? "--"}</td>
                        <td>{rule.trendDirection ?? "--"}</td>
                        <td>{rule.minConfidence ?? "--"}</td>
                        <td>{rule.regressionProbabilityAtLeast ?? "--"}</td>
                        <td>{rule.replayRegressionAtLeast ?? "--"}</td>
                        <td>{rule.actionType}</td>
                        <td>{rule.requiresApproval ? "yes" : "no"}</td>
                        <td>{rule.cooldownMinutes ?? "--"}</td>
                        <td>{rule.reason}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-quality-automation-simulate-metric"
              >
                metric（simulate）
                <input
                  id="open-platform-quality-automation-simulate-metric"
                  type="text"
                  value={qualityAutomationSimulationMetric}
                  onChange={(event) =>
                    setQualityAutomationSimulationMetric(event.target.value)
                  }
                />
              </label>
              <label
                className="inline-field"
                htmlFor="open-platform-quality-automation-simulate-score"
              >
                score（simulate）
                <input
                  id="open-platform-quality-automation-simulate-score"
                  type="number"
                  min="0"
                  max="100"
                  step="0.1"
                  value={qualityAutomationSimulationScore}
                  onChange={(event) =>
                    setQualityAutomationSimulationScore(event.target.value)
                  }
                />
              </label>
              <label
                className="inline-field"
                htmlFor="open-platform-quality-automation-simulate-trend"
              >
                trend（simulate）
                <select
                  id="open-platform-quality-automation-simulate-trend"
                  value={qualityAutomationSimulationTrendDirection}
                  onChange={(event) =>
                    setQualityAutomationSimulationTrendDirection(
                      event.target.value as "up" | "down" | "flat",
                    )
                  }
                >
                  <option value="up">up</option>
                  <option value="down">down</option>
                  <option value="flat">flat</option>
                </select>
              </label>
              <label
                className="inline-field"
                htmlFor="open-platform-quality-automation-simulate-confidence"
              >
                confidence（simulate）
                <input
                  id="open-platform-quality-automation-simulate-confidence"
                  type="number"
                  min="0"
                  max="1"
                  step="0.01"
                  value={qualityAutomationSimulationConfidence}
                  onChange={(event) =>
                    setQualityAutomationSimulationConfidence(event.target.value)
                  }
                />
              </label>
              <label
                className="inline-field"
                htmlFor="open-platform-quality-automation-simulate-regression-probability"
              >
                regressionProbability（simulate）
                <input
                  id="open-platform-quality-automation-simulate-regression-probability"
                  type="number"
                  min="0"
                  max="1"
                  step="0.01"
                  value={qualityAutomationSimulationRegressionProbability}
                  onChange={(event) =>
                    setQualityAutomationSimulationRegressionProbability(
                      event.target.value,
                    )
                  }
                />
              </label>
              <label
                className="inline-field"
                htmlFor="open-platform-quality-automation-simulate-replay-regression-count"
              >
                replayRegressionCount（simulate）
                <input
                  id="open-platform-quality-automation-simulate-replay-regression-count"
                  type="number"
                  min="0"
                  step="1"
                  value={qualityAutomationSimulationReplayRegressionCount}
                  onChange={(event) =>
                    setQualityAutomationSimulationReplayRegressionCount(
                      event.target.value,
                    )
                  }
                />
              </label>
              <button
                type="button"
                className="submit-button"
                disabled={simulateQualityAutomationPolicyMutation.isPending}
                onClick={() => {
                  const score = Number(qualityAutomationSimulationScore);
                  const confidence = Number(qualityAutomationSimulationConfidence);
                  const regressionProbability = Number(
                    qualityAutomationSimulationRegressionProbability,
                  );
                  const replayRegressionCount = Number(
                    qualityAutomationSimulationReplayRegressionCount,
                  );
                  if (
                    !qualityAutomationSimulationMetric.trim() ||
                    !Number.isFinite(score) ||
                    !Number.isFinite(confidence) ||
                    !Number.isFinite(regressionProbability) ||
                    !Number.isInteger(replayRegressionCount) ||
                    replayRegressionCount < 0
                  ) {
                    setQualityFeedback(null);
                    setQualityError("automation simulate 输入非法。");
                    return;
                  }
                  setQualityFeedback(null);
                  setQualityError(null);
                  simulateQualityAutomationPolicyMutation.mutate({
                    metric: qualityAutomationSimulationMetric.trim(),
                    score,
                    trendDirection: qualityAutomationSimulationTrendDirection,
                    confidence,
                    regressionProbability,
                    replayRegressionCount,
                  });
                }}
              >
                {simulateQualityAutomationPolicyMutation.isPending
                  ? "模拟中..."
                  : "模拟 automation policy"}
              </button>
            </div>

            {qualityAutomationSimulationPayload ? (
              <div className="table-wrapper">
                <table className="session-table">
                  <tbody>
                    <tr>
                      <th>simulate metric</th>
                      <td>{qualityAutomationSimulationPayload.metric}</td>
                      <th>severity</th>
                      <td>{qualityAutomationSimulationPayload.severity}</td>
                    </tr>
                    <tr>
                      <th>matchedRule</th>
                      <td>{qualityAutomationSimulationPayload.matchedRuleId ?? "--"}</td>
                      <th>resolvedAction</th>
                      <td>{qualityAutomationSimulationPayload.resolvedAction ?? "--"}</td>
                    </tr>
                    <tr>
                      <th>requiresApproval</th>
                      <td>{qualityAutomationSimulationPayload.requiresApproval ? "yes" : "no"}</td>
                      <th>blockingReasons</th>
                      <td>{qualityAutomationSimulationPayload.blockingReasons.join(", ") || "--"}</td>
                    </tr>
                    <tr>
                      <th>simulation context</th>
                      <td colSpan={3}>
                        {formatCompactJson(
                          {
                            confidence:
                              qualityAutomationSimulationPayload.confidence,
                            trendDirection:
                              qualityAutomationSimulationPayload.trendDirection,
                            regressionProbability:
                              qualityAutomationSimulationPayload.regressionProbability,
                            replayRegressionCount:
                              qualityAutomationSimulationPayload.replayRegressionCount,
                          },
                        )}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            ) : null}

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>automation execution</th>
                    <th>decision</th>
                    <th>result</th>
                    <th>approvalRequest</th>
                    <th>source</th>
                    <th>createdAt</th>
                  </tr>
                </thead>
                <tbody>
                  {qualityAutomationExecutionItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={6}>
                        {qualityAutomationExecutionsQuery.isLoading
                          ? "正在加载 automation executions。"
                          : "尚未触发 automation executions。"}
                      </td>
                    </tr>
                  ) : (
                    qualityAutomationExecutionItems.map((item) => (
                      <tr key={item.id}>
                        <td>{item.toolId}</td>
                        <td>{item.decision}</td>
                        <td>{item.result}</td>
                        <td>{item.approvalRequestId ?? "--"}</td>
                        <td>{String(item.metadata.source ?? "--")}</td>
                        <td>{formatDateTime(item.createdAt)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-quality-scorecard-team"
              >
                指标（scorecards）
                <input
                  id="open-platform-quality-scorecard-team"
                  type="text"
                  value={qualityScorecardTeam}
                  onChange={(event) =>
                    setQualityScorecardTeam(event.target.value)
                  }
                  placeholder="可选，例如：accuracy"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadQualityScorecardsMutation.isPending}
                onClick={() => {
                  setQualityFeedback(null);
                  setQualityError(null);
                  loadQualityScorecardsMutation.mutate({
                    team: qualityScorecardTeam.trim() || undefined,
                    limit: 50,
                  });
                }}
              >
                {loadQualityScorecardsMutation.isPending
                  ? "查询中..."
                  : "加载 Quality scorecards"}
              </button>

              <button
                type="button"
                className="submit-button"
                disabled={loadQualityForecastMutation.isPending}
                onClick={() => {
                  setQualityFeedback(null);
                  setQualityError(null);
                  loadQualityForecastMutation.mutate({
                    from: qualityProjectTrendsFrom.trim() || undefined,
                    to: qualityProjectTrendsTo.trim() || undefined,
                    metric: qualityProjectTrendsMetric.trim() || undefined,
                    provider: qualityProjectTrendsProvider.trim() || undefined,
                    workflow: qualityProjectTrendsWorkflow.trim() || undefined,
                    limit: 20,
                  });
                }}
              >
                {loadQualityForecastMutation.isPending
                  ? "查询中..."
                  : "加载 Quality forecast"}
              </button>

              <button
                type="button"
                className="submit-button"
                disabled={loadQualityAdviceMutation.isPending}
                onClick={() => {
                  setQualityFeedback(null);
                  setQualityError(null);
                  loadQualityAdviceMutation.mutate({
                    from: qualityProjectTrendsFrom.trim() || undefined,
                    to: qualityProjectTrendsTo.trim() || undefined,
                    provider: qualityProjectTrendsProvider.trim() || undefined,
                    workflow: qualityProjectTrendsWorkflow.trim() || undefined,
                  });
                }}
              >
                {loadQualityAdviceMutation.isPending
                  ? "查询中..."
                  : "加载 Quality advice"}
              </button>
            </div>

            {qualityFeedback ? (
              <p className="feedback success">{qualityFeedback}</p>
            ) : null}
            {qualityError ? (
              <p className="feedback error">{qualityError}</p>
            ) : null}

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>date</th>
                    <th>metric</th>
                    <th>value</th>
                    <th>target</th>
                    <th>score</th>
                    <th>status</th>
                  </tr>
                </thead>
                <tbody>
                  {qualityDailyItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={6}>
                        {hasLoadedQualityDaily
                          ? "无匹配 daily 数据。"
                          : "尚未加载 daily 数据。"}
                      </td>
                    </tr>
                  ) : (
                    qualityDailyItems.map((item) => (
                      <tr key={`${item.date}:${item.metric}`}>
                        <td>{item.date}</td>
                        <td>{item.metric}</td>
                        <td>{item.value}</td>
                        <td>{item.target}</td>
                        <td>{item.score.toFixed(2)}</td>
                        <td>{item.status}</td>
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
                    <th>project-trends summary</th>
                    <th>metric</th>
                    <th>events</th>
                    <th>passRate</th>
                    <th>avgScore</th>
                    <th>cost</th>
                    <th>tokens</th>
                    <th>sessions</th>
                    <th>window</th>
                  </tr>
                </thead>
                <tbody>
                  {!qualityProjectTrendSummary ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={9}>
                        {hasLoadedQualityProjectTrends
                          ? "无 project-trends 汇总。"
                          : "尚未加载 project-trends。"}
                      </td>
                    </tr>
                  ) : (
                    <tr>
                      <td>summary</td>
                      <td>{qualityProjectTrendSummary.metric}</td>
                      <td>{qualityProjectTrendSummary.totalEvents}</td>
                      <td>{qualityProjectTrendSummary.passRate.toFixed(4)}</td>
                      <td>{qualityProjectTrendSummary.avgScore.toFixed(2)}</td>
                      <td>{qualityProjectTrendSummary.totalCost.toFixed(4)}</td>
                      <td>{qualityProjectTrendSummary.totalTokens}</td>
                      <td>{qualityProjectTrendSummary.totalSessions}</td>
                      <td>
                        {qualityProjectTrendSummary.from
                          ? formatDateTime(qualityProjectTrendSummary.from)
                          : "--"}
                        {" ~ "}
                        {qualityProjectTrendSummary.to
                          ? formatDateTime(qualityProjectTrendSummary.to)
                          : "--"}
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>project</th>
                    <th>metric</th>
                    <th>events</th>
                    <th>passRate</th>
                    <th>avgScore</th>
                    <th>cost</th>
                    <th>tokens</th>
                    <th>sessions</th>
                    <th>cost/quality</th>
                  </tr>
                </thead>
                <tbody>
                  {qualityProjectTrendItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={9}>
                        {hasLoadedQualityProjectTrends
                          ? "无匹配 project-trends 数据。"
                          : "尚未加载 project-trends 数据。"}
                      </td>
                    </tr>
                  ) : (
                    qualityProjectTrendItems.map((item) => (
                      <tr key={`${item.project}:${item.metric}`}>
                        <td>{item.project}</td>
                        <td>{item.metric}</td>
                        <td>{item.totalEvents}</td>
                        <td>{item.passRate.toFixed(4)}</td>
                        <td>{item.avgScore.toFixed(2)}</td>
                        <td>{item.totalCost.toFixed(4)}</td>
                        <td>{item.totalTokens}</td>
                        <td>{item.totalSessions}</td>
                        <td>{item.costPerQualityPoint.toFixed(4)}</td>
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
                    <th>metric</th>
                    <th>updatedBy</th>
                    <th>targetScore(%)</th>
                    <th>updatedAt</th>
                    <th>highlights</th>
                  </tr>
                </thead>
                <tbody>
                  {qualityScorecardItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={6}>
                        {hasLoadedQualityScorecards
                          ? "无匹配 scorecards。"
                          : "尚未加载 scorecards。"}
                      </td>
                    </tr>
                  ) : (
                    qualityScorecardItems.map((item) => (
                      <tr key={item.id}>
                        <td>{item.id}</td>
                        <td>{item.team}</td>
                        <td>{item.owner}</td>
                        <td>{item.overallScore.toFixed(1)}</td>
                        <td>{formatDateTime(item.publishedAt)}</td>
                        <td>{item.highlights.join(" | ") || "--"}</td>
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
                    <th>forecast project</th>
                    <th>metric</th>
                    <th>modelVersion</th>
                    <th>predictedScore</th>
                    <th>confidence</th>
                    <th>regressionProbability</th>
                    <th>delta</th>
                    <th>windowCount</th>
                    <th>trend</th>
                    <th>rationale</th>
                    <th>window</th>
                  </tr>
                </thead>
                <tbody>
                  {qualityForecastItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={11}>
                        {hasLoadedQualityForecast
                          ? "无 forecast 数据。"
                          : "尚未加载 forecast。"}
                      </td>
                    </tr>
                  ) : (
                    qualityForecastItems.map((item) => (
                      <tr key={`${item.project}:${item.metric}:forecast`}>
                        <td>{item.project}</td>
                        <td>{item.metric}</td>
                        <td>{item.modelVersion ?? "--"}</td>
                        <td>{item.predictedScore.toFixed(2)}</td>
                        <td>{item.confidence.toFixed(2)}</td>
                        <td>
                          {typeof item.regressionProbability === "number"
                            ? item.regressionProbability.toFixed(4)
                            : "--"}
                        </td>
                        <td>
                          {typeof item.projectedDelta === "number"
                            ? item.projectedDelta.toFixed(2)
                            : "--"}
                        </td>
                        <td>{item.basisWindowCount ?? "--"}</td>
                        <td>{item.trendDirection ?? "--"}</td>
                        <td>{item.rationale ?? formatCompactJson(item.explanation)}</td>
                        <td>
                          {(item.windowStart ? formatDateTime(item.windowStart) : "--")}
                          {" ~ "}
                          {(item.windowEnd ? formatDateTime(item.windowEnd) : "--")}
                        </td>
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
                    <th>adviceId</th>
                    <th>advice project</th>
                    <th>severity</th>
                    <th>title</th>
                    <th>recommendation</th>
                    <th>explanation</th>
                    <th>strategyRule</th>
                    <th>autoDecision</th>
                    <th>execution hint</th>
                    <th>latestExecution</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {qualityAdviceItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={11}>
                        {hasLoadedQualityAdvice
                          ? "无 advice 数据。"
                          : "尚未加载 advice。"}
                      </td>
                    </tr>
                  ) : (
                    qualityAdviceItems.map((item) => (
                      <tr key={`${item.project}:${item.title}:advice`}>
                        <td>{item.id}</td>
                        <td>{item.project}</td>
                        <td>{item.severity}</td>
                        <td>{item.title}</td>
                        <td>{item.recommendation}</td>
                        <td>{item.explanation ?? "--"}</td>
                        <td>{item.strategyMatrixMatch ?? "--"}</td>
                        <td>{item.autoExecutionDecision ?? "--"}</td>
                        <td>
                          {item.executionOptions?.map((option) =>
                            `${option.actionType}:${option.availability}`,
                          ).join(" | ") ||
                            formatCompactJson(item.executionHint)}
                        </td>
                        <td>{item.latestExecutionStatus ?? "--"}</td>
                        <td>
                          <button
                            type="button"
                            className="table-action"
                            onClick={() => {
                              setQualityAdviceSelectedId(item.id);
                              setQualityAdviceExecuteProject(item.project);
                              setQualityAdviceExecuteSeverity(item.severity);
                              setQualityAdviceActionType(
                                item.executionHint?.recommendedActionType ??
                                  item.suggestedActions?.[0] ??
                                  "scorecard_adjustment",
                              );
                              setQualityFeedback(`已将 advice ${item.id} 带入执行表单。`);
                              setQualityError(null);
                            }}
                          >
                            带入执行
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            <div className="filters-row governance-inline-grid">
              <label className="inline-field" htmlFor="quality-advice-id">
                adviceId
                <input
                  id="quality-advice-id"
                  type="text"
                  value={qualityAdviceSelectedId}
                  onChange={(event) => setQualityAdviceSelectedId(event.target.value)}
                  placeholder="必填"
                />
              </label>
              <label className="inline-field" htmlFor="quality-advice-project">
                project
                <input
                  id="quality-advice-project"
                  type="text"
                  value={qualityAdviceExecuteProject}
                  onChange={(event) => setQualityAdviceExecuteProject(event.target.value)}
                  placeholder="必填"
                />
              </label>
              <label className="inline-field" htmlFor="quality-advice-severity">
                severity
                <select
                  id="quality-advice-severity"
                  value={qualityAdviceExecuteSeverity}
                  onChange={(event) =>
                    setQualityAdviceExecuteSeverity(
                      event.target.value as "info" | "warn" | "critical",
                    )
                  }
                >
                  <option value="info">info</option>
                  <option value="warn">warn</option>
                  <option value="critical">critical</option>
                </select>
              </label>
              <label className="inline-field" htmlFor="quality-advice-action-type">
                actionType
                <select
                  id="quality-advice-action-type"
                  value={qualityAdviceActionType}
                  onChange={(event) =>
                    setQualityAdviceActionType(
                      event.target.value as "scorecard_adjustment" | "replay_experiment",
                    )
                  }
                >
                  <option value="scorecard_adjustment">scorecard_adjustment</option>
                  <option value="replay_experiment">replay_experiment</option>
                </select>
              </label>
              <label className="inline-field" htmlFor="quality-advice-metric">
                metric
                <input
                  id="quality-advice-metric"
                  type="text"
                  value={qualityAdviceExecuteMetric}
                  onChange={(event) => setQualityAdviceExecuteMetric(event.target.value)}
                  placeholder="默认 accuracy"
                />
              </label>
              <label className="inline-field" htmlFor="quality-advice-dataset-id">
                datasetId
                <input
                  id="quality-advice-dataset-id"
                  type="text"
                  value={qualityAdviceExecuteDatasetId}
                  onChange={(event) => setQualityAdviceExecuteDatasetId(event.target.value)}
                  placeholder="replay_experiment 时必填"
                />
              </label>
              <label className="inline-field governance-wide-field" htmlFor="quality-advice-candidate-labels">
                candidateLabels（逗号分隔）
                <input
                  id="quality-advice-candidate-labels"
                  type="text"
                  value={qualityAdviceExecuteCandidateLabels}
                  onChange={(event) => setQualityAdviceExecuteCandidateLabels(event.target.value)}
                  placeholder="如 candidate-a,candidate-b"
                />
              </label>
              <button
                type="button"
                className="submit-button"
                disabled={executeQualityAdviceMutation.isPending}
                onClick={() => {
                  const adviceId = qualityAdviceSelectedId.trim();
                  const project = qualityAdviceExecuteProject.trim();
                  if (!adviceId || !project) {
                    setQualityFeedback(null);
                    setQualityError("执行 Quality advice 前请填写 adviceId 和 project。");
                    return;
                  }
                  if (
                    qualityAdviceActionType === "replay_experiment" &&
                    !qualityAdviceExecuteDatasetId.trim()
                  ) {
                    setQualityFeedback(null);
                    setQualityError("replay_experiment 执行时 datasetId 必填。");
                    return;
                  }
                  setQualityFeedback(null);
                  setQualityError(null);
                  executeQualityAdviceMutation.mutate({
                    adviceId,
                    project,
                    severity: qualityAdviceExecuteSeverity,
                    actionType: qualityAdviceActionType,
                    metric: qualityAdviceExecuteMetric.trim() || undefined,
                    datasetId: qualityAdviceExecuteDatasetId.trim() || undefined,
                    candidateLabels: parseDistinctCommaSeparatedList(
                      qualityAdviceExecuteCandidateLabels,
                    ),
                  });
                }}
              >
                {executeQualityAdviceMutation.isPending ? "执行中..." : "执行 Quality advice"}
              </button>
              <button
                type="button"
                className="submit-button"
                disabled={loadQualityAdviceExecutionsMutation.isPending}
                onClick={() => {
                  setQualityFeedback(null);
                  setQualityError(null);
                  loadQualityAdviceExecutionsMutation.mutate({
                    adviceId: qualityAdviceSelectedId.trim() || undefined,
                    actionType: qualityAdviceActionType,
                    limit: 50,
                  });
                }}
              >
                {loadQualityAdviceExecutionsMutation.isPending
                  ? "查询中..."
                  : "加载 advice executions"}
              </button>
            </div>

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>executionId</th>
                    <th>adviceId</th>
                    <th>actionType</th>
                    <th>status</th>
                    <th>result</th>
                    <th>updatedAt</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {qualityAdviceExecutionPayload?.items?.length ? (
                    qualityAdviceExecutionPayload.items.map((item) => (
                      <tr key={item.id}>
                        <td>{item.id}</td>
                        <td>{item.adviceId}</td>
                        <td>{item.actionType}</td>
                        <td>{item.status}</td>
                        <td>{formatCompactJson(item.resultSummary)}</td>
                        <td>{formatDateTime(item.updatedAt)}</td>
                        <td>
                          <button
                            type="button"
                            className="table-action"
                            disabled={
                              cancelQualityAdviceExecutionMutation.isPending ||
                              item.status === "completed" ||
                              item.status === "cancelled"
                            }
                            onClick={() =>
                              cancelQualityAdviceExecutionMutation.mutate(item.id)
                            }
                          >
                            取消
                          </button>
                        </td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td className="table-empty-cell" colSpan={7}>
                        {hasLoadedQualityAdviceExecutions
                          ? "无 advice executions。"
                          : "尚未加载 advice executions。"}
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </article>

          <article className="open-platform-card">
            <h3>Replay（datasets/cases/runs/diff/artifacts）</h3>
            <p>
              发起回放数据集与运行，维护样本集，查看差异结果并拉取
              summary/diff/cases 工件。
            </p>
            <datalist id="open-platform-replay-baseline-options">
              {knownReplayDatasetIds.map((datasetId) => (
                <option key={datasetId} value={datasetId} />
              ))}
            </datalist>
            <datalist id="open-platform-replay-version-options">
              {knownReplayVersionIds.map((versionId) => (
                <option key={versionId} value={versionId} />
              ))}
            </datalist>
            <datalist id="open-platform-replay-run-options">
              {knownReplayRunIds.map((runId) => (
                <option key={runId} value={runId} />
              ))}
            </datalist>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-dataset-name"
              >
                dataset 名称
                <input
                  id="open-platform-replay-create-dataset-name"
                  type="text"
                  value={replayCreateDatasetName}
                  onChange={(event) =>
                    setReplayCreateDatasetName(event.target.value)
                  }
                  placeholder="必填"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-dataset-id"
              >
                datasetRef
                <input
                  id="open-platform-replay-create-dataset-id"
                  type="text"
                  value={replayCreateDatasetRef}
                  onChange={(event) =>
                    setReplayCreateDatasetRef(event.target.value)
                  }
                  placeholder="必填"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-dataset-model"
              >
                model
                <input
                  id="open-platform-replay-create-dataset-model"
                  type="text"
                  value={replayCreateDatasetModel}
                  onChange={(event) =>
                    setReplayCreateDatasetModel(event.target.value)
                  }
                  placeholder="必填"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-dataset-prompt-version"
              >
                promptVersion
                <input
                  id="open-platform-replay-create-dataset-prompt-version"
                  type="text"
                  value={replayCreateDatasetPromptVersion}
                  onChange={(event) =>
                    setReplayCreateDatasetPromptVersion(event.target.value)
                  }
                  placeholder="可选"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-dataset-sample-count"
              >
                sampleCount
                <input
                  id="open-platform-replay-create-dataset-sample-count"
                  type="number"
                  min={0}
                  step={1}
                  value={replayCreateDatasetSampleCount}
                  onChange={(event) =>
                    setReplayCreateDatasetSampleCount(event.target.value)
                  }
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={createReplayDatasetMutation.isPending}
                onClick={() => {
                  const name = replayCreateDatasetName.trim();
                  const datasetRef = replayCreateDatasetRef.trim();
                  const model = replayCreateDatasetModel.trim();
                  const sampleCount = parseOptionalNonNegativeInteger(
                    replayCreateDatasetSampleCount,
                  );
                  if (!name || !datasetRef || !model) {
                    setReplayFeedback(null);
                    setReplayError(
                      "创建回放数据集前请填写 name、datasetRef、model。",
                    );
                    return;
                  }
                  if (
                    replayCreateDatasetSampleCount.trim().length > 0 &&
                    sampleCount === undefined
                  ) {
                    setReplayFeedback(null);
                    setReplayError(
                      "回放数据集的 sampleCount 必须是大于等于 0 的整数。",
                    );
                    return;
                  }
                  setReplayFeedback(null);
                  setReplayError(null);
                  createReplayDatasetMutation.mutate({
                    name,
                    datasetRef,
                    model,
                    promptVersion:
                      replayCreateDatasetPromptVersion.trim() || undefined,
                    sampleCount,
                  });
                }}
              >
                {createReplayDatasetMutation.isPending
                  ? "创建中..."
                  : "创建回放数据集"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-replay-baseline-keyword"
              >
                关键字（dataset）
                <input
                  id="open-platform-replay-baseline-keyword"
                  type="text"
                  value={replayDatasetKeyword}
                  onChange={(event) =>
                    setReplayDatasetKeyword(event.target.value)
                  }
                  placeholder="按 dataset 名称检索"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadReplayDatasetsMutation.isPending}
                onClick={() => {
                  setReplayFeedback(null);
                  setReplayError(null);
                  loadReplayDatasetsMutation.mutate({
                    keyword: replayDatasetKeyword.trim() || undefined,
                    limit: 50,
                  });
                }}
              >
                {loadReplayDatasetsMutation.isPending
                  ? "查询中..."
                  : "加载回放数据集"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-replay-dataset-version-dataset-id"
              >
                datasetId（versions）
                <input
                  id="open-platform-replay-dataset-version-dataset-id"
                  type="text"
                  list="open-platform-replay-baseline-options"
                  value={replayVersionDatasetId}
                  onChange={(event) => setReplayVersionDatasetId(event.target.value)}
                  placeholder="必填"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadReplayDatasetVersionsMutation.isPending}
                onClick={() => {
                  const datasetId = replayVersionDatasetId.trim();
                  if (!datasetId) {
                    setReplayFeedback(null);
                    setReplayError("回放数据集版本的 datasetId 不能为空。");
                    return;
                  }
                  setReplayFeedback(null);
                  setReplayError(null);
                  loadReplayDatasetVersionsMutation.mutate(datasetId);
                }}
              >
                {loadReplayDatasetVersionsMutation.isPending
                  ? "查询中..."
                  : "加载回放数据集版本"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-version-dataset-ref"
              >
                datasetRef（version）
                <input
                  id="open-platform-replay-create-version-dataset-ref"
                  type="text"
                  value={replayCreateVersionDatasetRef}
                  onChange={(event) =>
                    setReplayCreateVersionDatasetRef(event.target.value)
                  }
                  placeholder="必填"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-version-model"
              >
                model（version）
                <input
                  id="open-platform-replay-create-version-model"
                  type="text"
                  value={replayCreateVersionModel}
                  onChange={(event) =>
                    setReplayCreateVersionModel(event.target.value)
                  }
                  placeholder="必填"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-version-prompt-version"
              >
                promptVersion（version）
                <input
                  id="open-platform-replay-create-version-prompt-version"
                  type="text"
                  value={replayCreateVersionPromptVersion}
                  onChange={(event) =>
                    setReplayCreateVersionPromptVersion(event.target.value)
                  }
                  placeholder="可选"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-version-sample-count"
              >
                sampleCount（version）
                <input
                  id="open-platform-replay-create-version-sample-count"
                  type="number"
                  min={0}
                  step={1}
                  value={replayCreateVersionSampleCount}
                  onChange={(event) =>
                    setReplayCreateVersionSampleCount(event.target.value)
                  }
                />
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-replay-create-version-note"
              >
                note（version）
                <input
                  id="open-platform-replay-create-version-note"
                  type="text"
                  value={replayCreateVersionNote}
                  onChange={(event) => setReplayCreateVersionNote(event.target.value)}
                  placeholder="可选"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={createReplayDatasetVersionMutation.isPending}
                onClick={() => {
                  const datasetId = replayVersionDatasetId.trim();
                  const datasetRef = replayCreateVersionDatasetRef.trim();
                  const model = replayCreateVersionModel.trim();
                  const sampleCount = parseOptionalNonNegativeInteger(
                    replayCreateVersionSampleCount,
                  );
                  if (!datasetId || !datasetRef || !model) {
                    setReplayFeedback(null);
                    setReplayError(
                      "创建回放数据集版本前请填写 datasetId、datasetRef、model。",
                    );
                    return;
                  }
                  if (
                    replayCreateVersionSampleCount.trim().length > 0 &&
                    sampleCount === undefined
                  ) {
                    setReplayFeedback(null);
                    setReplayError(
                      "回放数据集版本的 sampleCount 必须是大于等于 0 的整数。",
                    );
                    return;
                  }
                  setReplayFeedback(null);
                  setReplayError(null);
                  createReplayDatasetVersionMutation.mutate({
                    datasetId,
                    input: {
                      datasetRef,
                      model,
                      promptVersion:
                        replayCreateVersionPromptVersion.trim() || undefined,
                      sampleCount,
                      note: replayCreateVersionNote.trim() || undefined,
                    },
                  });
                }}
              >
                {createReplayDatasetVersionMutation.isPending
                  ? "创建中..."
                  : "创建回放数据集版本"}
              </button>
            </div>

            {replayDatasetVersionPayload ? (
              <p>
                当前版本：
                {replayDatasetVersionPayload.currentVersionNumber ?? "--"}
                {" / "}
                {replayDatasetVersionPayload.currentVersionId ?? "--"}
              </p>
            ) : null}

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-replay-experiment-name"
              >
                experiment 名称
                <input
                  id="open-platform-replay-experiment-name"
                  type="text"
                  value={replayExperimentName}
                  onChange={(event) => setReplayExperimentName(event.target.value)}
                  placeholder="必填"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-experiment-dataset-id"
              >
                datasetId（experiment）
                <input
                  id="open-platform-replay-experiment-dataset-id"
                  type="text"
                  list="open-platform-replay-baseline-options"
                  value={replayExperimentDatasetId}
                  onChange={(event) => setReplayExperimentDatasetId(event.target.value)}
                  placeholder="必填"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-experiment-baseline-version-id"
              >
                baselineVersionId（experiment）
                <input
                  id="open-platform-replay-experiment-baseline-version-id"
                  type="text"
                  list="open-platform-replay-version-options"
                  value={replayExperimentBaselineVersionId}
                  onChange={(event) =>
                    setReplayExperimentBaselineVersionId(event.target.value)
                  }
                  placeholder="可选"
                />
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-replay-experiment-run-ids"
              >
                runIds（逗号分隔）
                <input
                  id="open-platform-replay-experiment-run-ids"
                  type="text"
                  value={replayExperimentRunIds}
                  onChange={(event) => setReplayExperimentRunIds(event.target.value)}
                  placeholder="可选"
                />
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-replay-experiment-candidate-labels"
              >
                candidateLabels（逗号分隔）
                <input
                  id="open-platform-replay-experiment-candidate-labels"
                  type="text"
                  value={replayExperimentCandidateLabels}
                  onChange={(event) =>
                    setReplayExperimentCandidateLabels(event.target.value)
                  }
                  placeholder="如 candidate-a,candidate-b"
                />
              </label>

              {replaySelectedExperimentCurrentVersionId ? (
                <p>
                  当前 dataset 版本：v
                  {replaySelectedExperimentCurrentVersionNumber ?? "--"}
                  {" / "}
                  {replaySelectedExperimentCurrentVersionId}
                </p>
              ) : null}

              <label
                className="checkbox-field"
                htmlFor="open-platform-replay-experiment-auto-run"
              >
                <input
                  id="open-platform-replay-experiment-auto-run"
                  type="checkbox"
                  checked={replayExperimentAutoRun}
                  onChange={(event) =>
                    setReplayExperimentAutoRun(event.target.checked)
                  }
                />
                自动启动实验
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={createReplayExperimentMutation.isPending}
                onClick={() => {
                  const name = replayExperimentName.trim();
                  const datasetId = replayExperimentDatasetId.trim();
                  if (!name || !datasetId) {
                    setReplayFeedback(null);
                    setReplayError("创建回放实验前请填写 name 和 datasetId。");
                    return;
                  }
                  setReplayFeedback(null);
                  setReplayError(null);
                  createReplayExperimentMutation.mutate({
                    name,
                    datasetId,
                    baselineVersionId:
                      replayExperimentBaselineVersionId.trim() || undefined,
                    runIds: parseDistinctCommaSeparatedList(replayExperimentRunIds),
                    candidateLabels: parseDistinctCommaSeparatedList(
                      replayExperimentCandidateLabels,
                    ),
                    autoRun: replayExperimentAutoRun,
                  });
                }}
              >
                {createReplayExperimentMutation.isPending
                  ? "创建中..."
                  : "创建回放实验"}
              </button>

              <button
                type="button"
                className="submit-button"
                disabled={loadReplayExperimentsMutation.isPending}
                onClick={() => {
                  setReplayFeedback(null);
                  setReplayError(null);
                  loadReplayExperimentsMutation.mutate({
                    datasetId: replayExperimentDatasetId.trim() || undefined,
                    limit: 50,
                  });
                }}
              >
                {loadReplayExperimentsMutation.isPending
                  ? "查询中..."
                  : "加载回放实验"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-replay-experiment-compare-ids"
              >
                experimentIds（批量对比，逗号分隔）
                <input
                  id="open-platform-replay-experiment-compare-ids"
                  type="text"
                  value={replayExperimentCompareIds}
                  onChange={(event) => setReplayExperimentCompareIds(event.target.value)}
                  placeholder="至少两个 experimentId"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadReplayExperimentBatchCompareMutation.isPending}
                onClick={() => {
                  const experimentIds = parseDistinctCommaSeparatedList(
                    replayExperimentCompareIds,
                  );
                  if (experimentIds.length < 2) {
                    setReplayFeedback(null);
                    setReplayError("批量对比至少需要 2 个 experimentId。");
                    return;
                  }
                  setReplayFeedback(null);
                  setReplayError(null);
                  loadReplayExperimentBatchCompareMutation.mutate({
                    experimentIds,
                    datasetId: replayExperimentDatasetId.trim() || undefined,
                  });
                }}
              >
                {loadReplayExperimentBatchCompareMutation.isPending
                  ? "对比中..."
                  : "批量对比实验"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-replay-dataset-cases-dataset-id"
              >
                datasetId（cases）
                <input
                  id="open-platform-replay-dataset-cases-dataset-id"
                  type="text"
                  list="open-platform-replay-baseline-options"
                  value={replayDatasetCasesDatasetId}
                  onChange={(event) =>
                    setReplayDatasetCasesDatasetId(event.target.value)
                  }
                  placeholder="必填"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadReplayDatasetCasesMutation.isPending}
                onClick={() => {
                  const datasetId = replayDatasetCasesDatasetId.trim();
                  if (!datasetId) {
                    setReplayFeedback(null);
                    setReplayError("回放样本的 datasetId 不能为空。");
                    return;
                  }
                  setReplayFeedback(null);
                  setReplayError(null);
                  loadReplayDatasetCasesMutation.mutate(datasetId);
                }}
              >
                {loadReplayDatasetCasesMutation.isPending
                  ? "查询中..."
                  : "加载回放样本"}
              </button>

              <button
                type="button"
                className="submit-button"
                disabled={saveReplayDatasetCasesMutation.isPending}
                onClick={() => {
                  const datasetId = replayDatasetCasesDatasetId.trim();
                  if (!datasetId) {
                    setReplayFeedback(null);
                    setReplayError("保存回放样本前请先填写 datasetId。");
                    return;
                  }
                  let parsed: unknown;
                  try {
                    parsed = JSON.parse(replayDatasetCasesEditor);
                  } catch {
                    setReplayFeedback(null);
                    setReplayError("回放样本编辑器内容必须是合法 JSON。");
                    return;
                  }
                  const rawItems = Array.isArray(parsed)
                    ? parsed
                    : parsed &&
                        typeof parsed === "object" &&
                        Array.isArray((parsed as { items?: unknown }).items)
                      ? (parsed as { items: unknown[] }).items
                      : null;
                  if (!rawItems) {
                    setReplayFeedback(null);
                    setReplayError(
                      "回放样本必须是数组，或包含 items 数组的对象。",
                    );
                    return;
                  }
                  const normalizedItems: Array<{
                    caseId?: string;
                    sortOrder?: number;
                    input: string;
                    expectedOutput?: string;
                    baselineOutput?: string;
                    candidateInput?: string;
                    metadata?: Record<string, unknown>;
                  }> = [];
                  for (const item of rawItems) {
                    if (!item || typeof item !== "object") {
                      setReplayFeedback(null);
                      setReplayError("回放样本数组中的每一项都必须是对象。");
                      return;
                    }
                    const record = item as Record<string, unknown>;
                    const input =
                      typeof record.input === "string"
                        ? record.input.trim()
                        : "";
                    if (!input) {
                      setReplayFeedback(null);
                      setReplayError("每条回放样本都必须包含非空 input。");
                      return;
                    }
                    normalizedItems.push({
                      caseId:
                        typeof record.caseId === "string" &&
                        record.caseId.trim().length > 0
                          ? record.caseId.trim()
                          : undefined,
                      sortOrder:
                        typeof record.sortOrder === "number" &&
                        Number.isInteger(record.sortOrder)
                          ? record.sortOrder
                          : undefined,
                      input,
                      expectedOutput:
                        typeof record.expectedOutput === "string" &&
                        record.expectedOutput.trim().length > 0
                          ? record.expectedOutput.trim()
                          : undefined,
                      baselineOutput:
                        typeof record.baselineOutput === "string" &&
                        record.baselineOutput.trim().length > 0
                          ? record.baselineOutput.trim()
                          : undefined,
                      candidateInput:
                        typeof record.candidateInput === "string" &&
                        record.candidateInput.trim().length > 0
                          ? record.candidateInput.trim()
                          : undefined,
                      metadata:
                        record.metadata &&
                        typeof record.metadata === "object" &&
                        !Array.isArray(record.metadata)
                          ? (record.metadata as Record<string, unknown>)
                          : undefined,
                    });
                  }
                  setReplayFeedback(null);
                  setReplayError(null);
                  saveReplayDatasetCasesMutation.mutate({
                    datasetId,
                    items: normalizedItems,
                  });
                }}
              >
                {saveReplayDatasetCasesMutation.isPending
                  ? "保存中..."
                  : "保存回放样本"}
              </button>
            </div>

            <label
              className="inline-field governance-wide-field"
              htmlFor="open-platform-replay-dataset-cases-editor"
            >
              样本编辑器（JSON）
              <textarea
                id="open-platform-replay-dataset-cases-editor"
                value={replayDatasetCasesEditor}
                onChange={(event) =>
                  setReplayDatasetCasesEditor(event.target.value)
                }
                placeholder='[{"caseId":"case-1","sortOrder":0,"input":"示例问题","expectedOutput":"示例答案"}]'
                rows={10}
              />
            </label>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-replay-materialize-session-ids"
              >
                sessionIds（可选，逗号分隔）
                <input
                  id="open-platform-replay-materialize-session-ids"
                  type="text"
                  value={replayMaterializeSessionIds}
                  onChange={(event) =>
                    setReplayMaterializeSessionIds(event.target.value)
                  }
                  placeholder="优先按指定 sessionId 物化"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-materialize-keyword"
              >
                keyword
                <input
                  id="open-platform-replay-materialize-keyword"
                  type="text"
                  value={replayMaterializeKeyword}
                  onChange={(event) =>
                    setReplayMaterializeKeyword(event.target.value)
                  }
                  placeholder="按会话关键词过滤"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-materialize-tool"
              >
                tool
                <input
                  id="open-platform-replay-materialize-tool"
                  type="text"
                  value={replayMaterializeTool}
                  onChange={(event) =>
                    setReplayMaterializeTool(event.target.value)
                  }
                  placeholder="如 Codex CLI"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-materialize-model"
              >
                model
                <input
                  id="open-platform-replay-materialize-model"
                  type="text"
                  value={replayMaterializeModel}
                  onChange={(event) =>
                    setReplayMaterializeModel(event.target.value)
                  }
                  placeholder="如 gpt-5-codex"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-materialize-from"
              >
                from
                <input
                  id="open-platform-replay-materialize-from"
                  type="datetime-local"
                  value={replayMaterializeFrom}
                  onChange={(event) =>
                    setReplayMaterializeFrom(event.target.value)
                  }
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-materialize-to"
              >
                to
                <input
                  id="open-platform-replay-materialize-to"
                  type="datetime-local"
                  value={replayMaterializeTo}
                  onChange={(event) =>
                    setReplayMaterializeTo(event.target.value)
                  }
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-materialize-sample-limit"
              >
                sampleLimit（materialize）
                <input
                  id="open-platform-replay-materialize-sample-limit"
                  type="number"
                  min={1}
                  step={1}
                  value={replayMaterializeSampleLimit}
                  onChange={(event) =>
                    setReplayMaterializeSampleLimit(event.target.value)
                  }
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-materialize-sanitized"
              >
                sanitized
                <select
                  id="open-platform-replay-materialize-sanitized"
                  value={replayMaterializeSanitized ? "true" : "false"}
                  onChange={(event) =>
                    setReplayMaterializeSanitized(event.target.value === "true")
                  }
                >
                  <option value="true">true</option>
                  <option value="false">false</option>
                </select>
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={materializeReplayDatasetCasesMutation.isPending}
                onClick={() => {
                  const datasetId = replayDatasetCasesDatasetId.trim();
                  const sampleLimit = Number(replayMaterializeSampleLimit);
                  if (!datasetId) {
                    setReplayFeedback(null);
                    setReplayError("历史会话物化前请先填写 datasetId。");
                    return;
                  }
                  if (
                    replayMaterializeSampleLimit.trim().length > 0 &&
                    (!Number.isInteger(sampleLimit) || sampleLimit <= 0)
                  ) {
                    setReplayFeedback(null);
                    setReplayError("materialize 的 sampleLimit 必须是正整数。");
                    return;
                  }
                  const sessionIds = replayMaterializeSessionIds
                    .split(",")
                    .map((item) => item.trim())
                    .filter(Boolean);
                  const filters = {
                    keyword: replayMaterializeKeyword.trim() || undefined,
                    tool: replayMaterializeTool.trim() || undefined,
                    model: replayMaterializeModel.trim() || undefined,
                    from: replayMaterializeFrom
                      ? new Date(replayMaterializeFrom).toISOString()
                      : undefined,
                    to: replayMaterializeTo
                      ? new Date(replayMaterializeTo).toISOString()
                      : undefined,
                  };
                  if (
                    sessionIds.length === 0 &&
                    !filters.keyword &&
                    !filters.tool &&
                    !filters.model &&
                    !filters.from &&
                    !filters.to
                  ) {
                    setReplayFeedback(null);
                    setReplayError(
                      "请提供 sessionIds，或至少一个会话筛选条件。",
                    );
                    return;
                  }
                  setReplayFeedback(null);
                  setReplayError(null);
                  materializeReplayDatasetCasesMutation.mutate({
                    datasetId,
                    sessionIds: sessionIds.length > 0 ? sessionIds : undefined,
                    filters,
                    sampleLimit:
                      replayMaterializeSampleLimit.trim().length > 0
                        ? sampleLimit
                        : undefined,
                    sanitized: replayMaterializeSanitized,
                  });
                }}
              >
                {materializeReplayDatasetCasesMutation.isPending
                  ? "物化中..."
                  : "从历史会话物化样本"}
              </button>
            </div>

            {replayMaterializePayload ? (
              <div className="table-wrapper">
                <table className="session-table">
                  <tbody>
                    <tr>
                      <th>最近物化</th>
                      <td>
                        {replayMaterializePayload.sourceType} / materialized{" "}
                        {replayMaterializePayload.materialized} / skipped{" "}
                        {replayMaterializePayload.skipped}
                      </td>
                      <th>来源分布</th>
                      <td>
                        {formatCompactJson(
                          replayMaterializePayload.sourceSummary,
                        )}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            ) : null}

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-run-baseline-id"
              >
                datasetId（create run）
                <input
                  id="open-platform-replay-create-run-baseline-id"
                  type="text"
                  list="open-platform-replay-baseline-options"
                  value={replayCreateRunDatasetId}
                  onChange={(event) =>
                    setReplayCreateRunDatasetId(event.target.value)
                  }
                  placeholder="必填"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-run-baseline-version-id"
              >
                baselineVersionId（run）
                <input
                  id="open-platform-replay-create-run-baseline-version-id"
                  type="text"
                  list="open-platform-replay-version-options"
                  value={replayCreateRunBaselineVersionId}
                  onChange={(event) =>
                    setReplayCreateRunBaselineVersionId(event.target.value)
                  }
                  placeholder="可选，默认当前版本"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-run-candidate-label"
              >
                candidateLabel
                <input
                  id="open-platform-replay-create-run-candidate-label"
                  type="text"
                  value={replayCreateRunCandidateLabel}
                  onChange={(event) =>
                    setReplayCreateRunCandidateLabel(event.target.value)
                  }
                  placeholder="必填"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-create-run-sample-limit"
              >
                sampleLimit
                <input
                  id="open-platform-replay-create-run-sample-limit"
                  type="number"
                  min={1}
                  step={1}
                  value={replayCreateRunSampleLimit}
                  onChange={(event) =>
                    setReplayCreateRunSampleLimit(event.target.value)
                  }
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={createReplayRunMutation.isPending}
                onClick={() => {
                  const datasetId = replayCreateRunDatasetId.trim();
                  const candidateLabel = replayCreateRunCandidateLabel.trim();
                  const sampleLimit = Number(replayCreateRunSampleLimit);
                  if (!datasetId || !candidateLabel) {
                    setReplayFeedback(null);
                    setReplayError(
                      "创建回放运行前请填写 datasetId 与 candidateLabel。",
                    );
                    return;
                  }
                  if (
                    replayCreateRunSampleLimit.trim().length > 0 &&
                    (!Number.isInteger(sampleLimit) || sampleLimit <= 0)
                  ) {
                    setReplayFeedback(null);
                    setReplayError("回放运行的 sampleLimit 必须是正整数。");
                    return;
                  }
                  setReplayFeedback(null);
                  setReplayError(null);
                  createReplayRunMutation.mutate({
                    datasetId,
                    baselineVersionId:
                      replayCreateRunBaselineVersionId.trim() || undefined,
                    candidateLabel,
                    sampleLimit:
                      replayCreateRunSampleLimit.trim().length > 0
                        ? sampleLimit
                        : undefined,
                  });
                }}
              >
                {createReplayRunMutation.isPending
                  ? "创建中..."
                  : "创建回放运行"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-replay-jobs-baseline-id"
              >
                datasetId（runs）
                <input
                  id="open-platform-replay-jobs-baseline-id"
                  type="text"
                  list="open-platform-replay-baseline-options"
                  value={replayRunsDatasetIdFilter}
                  onChange={(event) =>
                    setReplayRunsDatasetIdFilter(event.target.value)
                  }
                  placeholder="可选"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-jobs-status"
              >
                状态（runs）
                <select
                  id="open-platform-replay-jobs-status"
                  value={replayRunsStatusFilter}
                  onChange={(event) =>
                    setReplayRunsStatusFilter(
                      event.target.value as OpenPlatformReplayJobStatus | "",
                    )
                  }
                >
                  {OPEN_PLATFORM_REPLAY_JOB_STATUS_FILTER_OPTIONS.map(
                    (option) => (
                      <option key={option.value || "all"} value={option.value}>
                        {option.label}
                      </option>
                    ),
                  )}
                </select>
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadReplayRunsMutation.isPending}
                onClick={() => {
                  setReplayFeedback(null);
                  setReplayError(null);
                  loadReplayRunsMutation.mutate({
                    datasetId: replayRunsDatasetIdFilter.trim() || undefined,
                    status: replayRunsStatusFilter || undefined,
                    limit: 50,
                  });
                }}
              >
                {loadReplayRunsMutation.isPending
                  ? "查询中..."
                  : "加载回放运行"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-replay-diff-baseline-id"
              >
                datasetId（diff，可选校验）
                <input
                  id="open-platform-replay-diff-baseline-id"
                  type="text"
                  list="open-platform-replay-baseline-options"
                  value={replayDiffDatasetId}
                  onChange={(event) =>
                    setReplayDiffDatasetId(event.target.value)
                  }
                  placeholder="可选"
                />
              </label>

              <label
                className="inline-field"
                htmlFor="open-platform-replay-diff-job-id"
              >
                runId（diff）
                <input
                  id="open-platform-replay-diff-job-id"
                  type="text"
                  list="open-platform-replay-run-options"
                  value={replayDiffRunId}
                  onChange={(event) => setReplayDiffRunId(event.target.value)}
                  placeholder="必填"
                />
              </label>

              <label
                className="inline-field governance-wide-field"
                htmlFor="open-platform-replay-diff-keyword"
              >
                关键字（diff）
                <input
                  id="open-platform-replay-diff-keyword"
                  type="text"
                  value={replayDiffKeyword}
                  onChange={(event) => setReplayDiffKeyword(event.target.value)}
                  placeholder="可选，按 caseId 或 summary"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadReplayDiffMutation.isPending}
                onClick={() => {
                  const datasetId = replayDiffDatasetId.trim();
                  const runId = replayDiffRunId.trim();
                  if (!runId) {
                    setReplayFeedback(null);
                    setReplayError("回放差异的 runId 不能为空。");
                    return;
                  }
                  setReplayFeedback(null);
                  setReplayError(null);
                  loadReplayDiffMutation.mutate({
                    datasetId: datasetId || undefined,
                    runId,
                    keyword: replayDiffKeyword.trim() || undefined,
                    limit: 50,
                  });
                }}
              >
                {loadReplayDiffMutation.isPending
                  ? "查询中..."
                  : "加载回放差异"}
              </button>
            </div>

            <div className="filters-row governance-inline-grid">
              <label
                className="inline-field"
                htmlFor="open-platform-replay-artifact-job-id"
              >
                runId（artifacts）
                <input
                  id="open-platform-replay-artifact-job-id"
                  type="text"
                  list="open-platform-replay-run-options"
                  value={replayArtifactRunId}
                  onChange={(event) =>
                    setReplayArtifactRunId(event.target.value)
                  }
                  placeholder="必填"
                />
              </label>

              <button
                type="button"
                className="submit-button"
                disabled={loadReplayArtifactsMutation.isPending}
                onClick={() => {
                  const runId = replayArtifactRunId.trim();
                  if (!runId) {
                    setReplayFeedback(null);
                    setReplayError("回放工件的 runId 不能为空。");
                    return;
                  }
                  setReplayFeedback(null);
                  setReplayError(null);
                  loadReplayArtifactsMutation.mutate(runId);
                }}
              >
                {loadReplayArtifactsMutation.isPending
                  ? "查询中..."
                  : "加载回放工件"}
              </button>
            </div>

            {replayFeedback ? (
              <p className="feedback success">{replayFeedback}</p>
            ) : null}
            {replayError ? (
              <p className="feedback error">{replayError}</p>
            ) : null}

            {replaySelectedRunSummary ? (
              <div className="table-wrapper">
                <table className="session-table">
                  <tbody>
                    <tr>
                      <th>运行摘要</th>
                      <td>
                        total{" "}
                        {String(replaySelectedRunSummary.totalCases ?? "--")} /
                        processed{" "}
                        {String(
                          replaySelectedRunSummary.processedCases ?? "--",
                        )}{" "}
                        / improved{" "}
                        {String(replaySelectedRunSummary.improvedCases ?? "--")}{" "}
                        / regressed{" "}
                        {String(
                          replaySelectedRunSummary.regressedCases ?? "--",
                        )}
                      </td>
                      <th>执行来源</th>
                      <td>
                        {String(
                          replaySelectedRunSummary.executionSource ?? "--",
                        )}
                      </td>
                    </tr>
                    <tr>
                      <th>baselineVersionId</th>
                      <td>
                        {String(
                          replaySelectedRunSummary.baselineVersionId ?? "--",
                        )}
                      </td>
                      <th>digest</th>
                      <td>
                        {formatCompactJson(
                          replaySelectedRunDigest ?? undefined,
                        )}
                      </td>
                    </tr>
                    <tr>
                      <th>样本来源</th>
                      <td colSpan={3}>
                        {formatCompactJson(
                          replaySelectedRunSummary.sourceSummary &&
                            typeof replaySelectedRunSummary.sourceSummary ===
                              "object" &&
                            !Array.isArray(
                              replaySelectedRunSummary.sourceSummary,
                            )
                            ? (replaySelectedRunSummary.sourceSummary as Record<
                                string,
                                unknown
                              >)
                            : undefined,
                        )}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            ) : null}

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>experiment</th>
                    <th>datasetId</th>
                    <th>baselineVersionId</th>
                    <th>status</th>
                    <th>trigger</th>
                    <th>sourceAdvice</th>
                    <th>candidates</th>
                    <th>runIds</th>
                    <th>runSummary</th>
                    <th>aggregate</th>
                    <th>updatedAt</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {replayExperimentItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={12}>
                        {hasLoadedReplayExperiments
                          ? "无匹配 experiment。"
                          : "尚未加载 experiment。"}
                      </td>
                    </tr>
                  ) : (
                    replayExperimentItems.map((item) => (
                      <tr key={item.id}>
                        <td>{item.name}</td>
                        <td>{item.datasetId}</td>
                        <td>{item.baselineVersionId ?? "--"}</td>
                        <td>{item.status ?? "--"}</td>
                        <td>{item.triggerSource ?? "--"}</td>
                        <td>{item.sourceAdviceId ?? "--"}</td>
                        <td>{item.candidateLabels?.join(", ") || "--"}</td>
                        <td>{item.runIds.join(", ") || "--"}</td>
                        <td>{formatCompactJson(item.runStatusSummary ?? item.summary)}</td>
                        <td>{formatCompactJson(item.aggregateSummary ?? item.summary)}</td>
                        <td>{formatDateTime(item.updatedAt)}</td>
                        <td>
                          <div className="governance-action-row">
                            <button
                              type="button"
                              className="table-action"
                              onClick={() =>
                                loadReplayExperimentDetailMutation.mutate(item.id)
                              }
                            >
                              详情
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              onClick={() =>
                                loadReplayExperimentCompareMutation.mutate(item.id)
                              }
                            >
                              对比
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              onClick={() =>
                                loadReplayExperimentWorkflowMutation.mutate(item.id)
                              }
                            >
                              工作流
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              onClick={() =>
                                loadReplayExperimentArtifactsMutation.mutate(item.id)
                              }
                            >
                              工件
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              onClick={() => runReplayExperimentMutation.mutate(item.id)}
                            >
                              启动
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              onClick={() =>
                                cancelReplayExperimentMutation.mutate(item.id)
                              }
                            >
                              取消
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            {replayExperimentDetailPayload ? (
              <div className="table-wrapper">
                <table className="session-table">
                  <tbody>
                    <tr>
                      <th>实验详情</th>
                      <td>{replayExperimentDetailPayload.name}</td>
                      <th>状态</th>
                      <td>{replayExperimentDetailPayload.status ?? "--"}</td>
                    </tr>
                    <tr>
                      <th>trigger</th>
                      <td>{replayExperimentDetailPayload.triggerSource ?? "--"}</td>
                      <th>candidates</th>
                      <td>{replayExperimentDetailPayload.candidateLabels?.join(", ") || "--"}</td>
                    </tr>
                    <tr>
                      <th>runIds</th>
                      <td>{replayExperimentDetailPayload.runIds.join(", ") || "--"}</td>
                      <th>sourceAdvice</th>
                      <td>{replayExperimentDetailPayload.sourceAdviceId ?? "--"}</td>
                    </tr>
                    <tr>
                      <th>baselineVersionId</th>
                      <td>{replayExperimentDetailPayload.baselineVersionId ?? "--"}</td>
                      <th>datasetId</th>
                      <td>{replayExperimentDetailPayload.datasetId}</td>
                    </tr>
                    <tr>
                      <th>runSummary</th>
                      <td>{formatCompactJson(replayExperimentDetailPayload.runStatusSummary ?? replayExperimentDetailPayload.summary)}</td>
                      <th>aggregate</th>
                      <td>{formatCompactJson(replayExperimentDetailPayload.aggregateSummary ?? replayExperimentDetailPayload.summary)}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            ) : null}

            {replayExperimentComparePayload ? (
              <div className="table-wrapper">
                <table className="session-table">
                  <tbody>
                    <tr>
                      <th>实验对比</th>
                      <td>{replayExperimentComparePayload.experimentId}</td>
                      <th>最佳 run</th>
                      <td>{replayExperimentComparePayload.summary.bestRunId ?? "--"}</td>
                    </tr>
                    <tr>
                      <th>completed</th>
                      <td>{replayExperimentComparePayload.summary.completedRuns}</td>
                      <th>queued/running/failed</th>
                      <td>
                        {replayExperimentComparePayload.summary.queuedRuns} /{" "}
                        {replayExperimentComparePayload.summary.runningRuns} /{" "}
                        {replayExperimentComparePayload.summary.failedRuns}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            ) : null}

            {replayExperimentBatchComparePayload ? (
              <div className="table-wrapper">
                <table className="session-table">
                  <tbody>
                    <tr>
                      <th>批量对比</th>
                      <td>{replayExperimentBatchComparePayload.summary.comparedExperimentCount}</td>
                      <th>datasets</th>
                      <td>
                        {replayExperimentBatchComparePayload.summary.datasets.join(", ") || "--"}
                      </td>
                    </tr>
                    <tr>
                      <th>best/worst</th>
                      <td>
                        {replayExperimentBatchComparePayload.summary.bestExperimentId ?? "--"}
                        {" / "}
                        {replayExperimentBatchComparePayload.summary.worstExperimentId ?? "--"}
                      </td>
                      <th>runs</th>
                      <td>
                        {replayExperimentBatchComparePayload.summary.completedRuns}
                        {" completed / "}
                        {replayExperimentBatchComparePayload.summary.runningRuns}
                        {" running / "}
                        {replayExperimentBatchComparePayload.summary.failedRuns}
                        {" failed"}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            ) : null}

            {replayExperimentComparePayload ? (
              <div className="table-wrapper">
                <table className="session-table">
                  <thead>
                    <tr>
                      <th>runId</th>
                      <th>candidate</th>
                      <th>status</th>
                      <th>processed/total</th>
                      <th>improved</th>
                      <th>regressed</th>
                      <th>passRate</th>
                      <th>netDelta</th>
                    </tr>
                  </thead>
                  <tbody>
                    {replayExperimentComparePayload.items.map((item) => (
                      <tr key={`${item.runId}:compare`}>
                        <td>{item.runId}</td>
                        <td>{item.candidateLabel}</td>
                        <td>{item.status}</td>
                        <td>
                          {item.processedCases}/{item.totalCases}
                        </td>
                        <td>{item.improvedCases}</td>
                        <td>{item.regressedCases}</td>
                        <td>{item.passRate.toFixed(4)}</td>
                        <td>{item.netDelta}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}

            {replayExperimentBatchComparePayload ? (
              <div className="table-wrapper">
                <table className="session-table">
                  <thead>
                    <tr>
                      <th>experimentId</th>
                      <th>name</th>
                      <th>datasetId</th>
                      <th>workflow</th>
                      <th>bestRun</th>
                      <th>netDelta</th>
                      <th>rates</th>
                      <th>run overview</th>
                    </tr>
                  </thead>
                  <tbody>
                    {replayExperimentBatchCompareItems.map((item) => (
                      <tr key={`${item.experimentId}:batch-compare`}>
                        <td>{item.experimentId}</td>
                        <td>{item.name}</td>
                        <td>{item.datasetId}</td>
                        <td>{item.workflowStage}</td>
                        <td>{item.bestRunId ?? "--"}</td>
                        <td>{item.netDelta}</td>
                        <td>
                          improve {item.improvementRate.toFixed(4)} / regress{" "}
                          {item.regressionRate.toFixed(4)}
                        </td>
                        <td>
                          {item.runs
                            .map((run) => `${run.candidateLabel}:${run.netDelta}`)
                            .join(" | ") || "--"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}

            {replayExperimentWorkflowPayload ? (
              <div className="table-wrapper">
                <table className="session-table">
                  <tbody>
                    <tr>
                      <th>实验工作流</th>
                      <td>{replayExperimentWorkflowPayload.experimentId}</td>
                      <th>状态</th>
                      <td>{replayExperimentWorkflowPayload.status}</td>
                    </tr>
                    <tr>
                      <th>nodes</th>
                      <td>{replayExperimentWorkflowPayload.summary.totalNodes}</td>
                      <th>runs</th>
                      <td>{replayExperimentWorkflowPayload.summary.totalRuns}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            ) : null}

            {replayExperimentWorkflowPayload ? (
              <div className="table-wrapper">
                <table className="session-table">
                  <thead>
                    <tr>
                      <th>node</th>
                      <th>type</th>
                      <th>status</th>
                      <th>startedAt</th>
                      <th>finishedAt</th>
                      <th>metadata</th>
                    </tr>
                  </thead>
                  <tbody>
                    {replayExperimentWorkflowPayload.nodes.map((node) => (
                      <tr key={node.id}>
                        <td>{node.label}</td>
                        <td>{node.type}</td>
                        <td>{node.status}</td>
                        <td>{node.startedAt ? formatDateTime(node.startedAt) : "--"}</td>
                        <td>{node.finishedAt ? formatDateTime(node.finishedAt) : "--"}</td>
                        <td>{formatCompactJson(node.metadata)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>name</th>
                    <th>model</th>
                    <th>dataset</th>
                    <th>currentVersion</th>
                    <th>来源</th>
                    <th>updatedAt</th>
                  </tr>
                </thead>
                <tbody>
                  {replayDatasetItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={7}>
                        {hasLoadedReplayDatasets
                          ? "无匹配 dataset。"
                          : "尚未加载 dataset。"}
                      </td>
                    </tr>
                  ) : (
                    replayDatasetItems.map((item) => (
                      <tr key={item.datasetId}>
                        <td>{item.datasetId}</td>
                        <td>{item.name}</td>
                        <td>{item.model}</td>
                        <td>{item.datasetRef ?? item.datasetId}</td>
                        <td>
                          {item.currentVersionNumber ?? "--"}
                          {" / "}
                          {item.currentVersionId ?? "--"}
                        </td>
                        <td>{formatCompactJson(item.metadata)}</td>
                        <td>
                          <div className="governance-action-row">
                            <span>{formatDateTime(item.updatedAt)}</span>
                            <button
                              type="button"
                              className="table-action"
                              onClick={() => {
                                setReplayDatasetCasesDatasetId(item.datasetId);
                                setReplayVersionDatasetId(item.datasetId);
                                setReplayCreateRunDatasetId(item.datasetId);
                                setReplayCreateRunBaselineVersionId(
                                  item.currentVersionId ?? "",
                                );
                                setReplayRunsDatasetIdFilter(item.datasetId);
                                setReplayExperimentDatasetId(item.datasetId);
                                setReplayExperimentBaselineVersionId(
                                  item.currentVersionId ?? "",
                                );
                                setReplayDiffDatasetId(item.datasetId);
                              }}
                            >
                              使用此数据集
                            </button>
                          </div>
                        </td>
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
                    <th>version</th>
                    <th>versionId</th>
                    <th>datasetRef</th>
                    <th>model</th>
                    <th>sampleCount</th>
                    <th>promotedAt</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {replayDatasetVersionItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={7}>
                        {hasLoadedReplayDatasetVersions
                          ? "无匹配 dataset version。"
                          : "尚未加载 dataset version。"}
                      </td>
                    </tr>
                  ) : (
                    replayDatasetVersionItems.map((item) => (
                      <tr key={item.id}>
                        <td>v{item.version}</td>
                        <td>{item.id}</td>
                        <td>{item.datasetRef ?? item.datasetId}</td>
                        <td>{item.model}</td>
                        <td>{item.sampleCount}</td>
                        <td>
                          {item.promotedAt ? formatDateTime(item.promotedAt) : "--"}
                        </td>
                        <td>
                          <div className="governance-action-row">
                            <button
                              type="button"
                              className="table-action"
                              onClick={() => {
                                setReplayCreateRunDatasetId(item.datasetId);
                                setReplayCreateRunBaselineVersionId(item.id);
                                setReplayExperimentDatasetId(item.datasetId);
                                setReplayExperimentBaselineVersionId(item.id);
                              }}
                            >
                              带入实验版本
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              disabled={loadReplayDatasetVersionCasesMutation.isPending}
                              onClick={() => {
                                setReplayFeedback(null);
                                setReplayError(null);
                                loadReplayDatasetVersionCasesMutation.mutate({
                                  datasetId: item.datasetId,
                                  versionId: item.id,
                                });
                              }}
                            >
                              版本样本
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              disabled={promoteReplayDatasetVersionMutation.isPending}
                              onClick={() =>
                                promoteReplayDatasetVersionMutation.mutate({
                                  datasetId: item.datasetId,
                                  versionId: item.id,
                                })
                              }
                            >
                              提升为当前版本
                            </button>
                          </div>
                        </td>
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
                    <th>datasetId</th>
                    <th>caseId</th>
                    <th>sortOrder</th>
                    <th>input</th>
                    <th>metadata</th>
                  </tr>
                </thead>
                <tbody>
                  {replayDatasetCaseItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={5}>
                        {hasLoadedReplayDatasetCases
                          ? "无匹配样本。"
                          : "尚未加载样本。"}
                      </td>
                    </tr>
                  ) : (
                    replayDatasetCaseItems.map((item) => (
                      <tr key={`${item.datasetId}:${item.caseId}`}>
                        <td>{item.datasetId}</td>
                        <td>{item.caseId}</td>
                        <td>{item.sortOrder}</td>
                        <td>{item.input}</td>
                        <td>{formatCompactJson(item.metadata)}</td>
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
                    <th>datasetId</th>
                    <th>versionId</th>
                    <th>caseId</th>
                    <th>sortOrder</th>
                    <th>input</th>
                    <th>metadata</th>
                  </tr>
                </thead>
                <tbody>
                  {replayDatasetVersionCaseItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={6}>
                        {hasLoadedReplayDatasetVersionCases
                          ? "无匹配版本样本。"
                          : "尚未加载版本样本。"}
                      </td>
                    </tr>
                  ) : (
                    replayDatasetVersionCaseItems.map((item) => (
                      <tr
                        key={`${replayDatasetVersionCasesPayload?.versionId ?? "version"}:${item.caseId}`}
                      >
                        <td>{item.datasetId}</td>
                        <td>{replayDatasetVersionCasesPayload?.versionId ?? "--"}</td>
                        <td>{item.caseId}</td>
                        <td>{item.sortOrder}</td>
                        <td>{item.input}</td>
                        <td>{formatCompactJson(item.metadata)}</td>
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
                    <th>datasetId</th>
                    <th>status</th>
                    <th>cases</th>
                    <th>createdAt</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {replayRunItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={6}>
                        {hasLoadedReplayJobs
                          ? "无匹配 runs。"
                          : "尚未加载 runs。"}
                      </td>
                    </tr>
                  ) : (
                    replayRunItems.map((item) => (
                      <tr key={item.runId}>
                        <td>{item.runId}</td>
                        <td>{item.datasetId}</td>
                        <td>{item.status}</td>
                        <td>
                          {item.passedCases}/{item.totalCases}（failed:{" "}
                          {item.failedCases}）
                        </td>
                        <td>{formatDateTime(item.createdAt)}</td>
                        <td>
                          <div className="governance-action-row">
                            <span>
                              {item.finishedAt
                                ? formatDateTime(item.finishedAt)
                                : "--"}
                            </span>
                            <button
                              type="button"
                              className="table-action"
                              onClick={() => {
                                setReplayDiffDatasetId(item.datasetId);
                                setReplayDiffRunId(item.runId);
                                setReplayArtifactRunId(item.runId);
                              }}
                            >
                              载入
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              onClick={() => {
                                setReplayFeedback(null);
                                setReplayError(null);
                                setReplayDiffDatasetId(item.datasetId);
                                setReplayDiffRunId(item.runId);
                                loadReplayDiffMutation.mutate({
                                  datasetId: item.datasetId,
                                  runId: item.runId,
                                  keyword:
                                    replayDiffKeyword.trim() || undefined,
                                  limit: 50,
                                });
                              }}
                            >
                              查 diff
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              onClick={() => {
                                setReplayFeedback(null);
                                setReplayError(null);
                                setReplayArtifactRunId(item.runId);
                                loadReplayArtifactsMutation.mutate(item.runId);
                              }}
                            >
                              查工件
                            </button>
                          </div>
                        </td>
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
                    <th>caseId</th>
                    <th>summary</th>
                    <th>verdict</th>
                    <th>deltaScore</th>
                  </tr>
                </thead>
                <tbody>
                  {replayDiffItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={5}>
                        {hasLoadedReplayDiff
                          ? "无匹配 diff。"
                          : "尚未加载 diff。"}
                      </td>
                    </tr>
                  ) : (
                    replayDiffItems.map((item) => (
                      <tr key={item.id}>
                        <td>{item.id}</td>
                        <td>{item.caseId}</td>
                        <td>{item.summary}</td>
                        <td>{item.verdict}</td>
                        <td>{item.deltaScore.toFixed(3)}</td>
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
                    <th>experimentId</th>
                    <th>runId</th>
                    <th>type</th>
                    <th>storage</th>
                    <th>byteSize</th>
                    <th>preview</th>
                    <th>downloadUrl</th>
                  </tr>
                </thead>
                <tbody>
                  {replayExperimentArtifactItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={7}>
                        {hasLoadedReplayExperimentArtifacts
                          ? "无匹配 experiment artifacts。"
                          : "尚未加载 experiment artifacts。"}
                      </td>
                    </tr>
                  ) : (
                    replayExperimentArtifactItems.map((item, index) => (
                      <tr
                        key={`${replayExperimentArtifactPayload?.experimentId ?? "experiment"}:${item.runId ?? index}:${item.type}`}
                      >
                        <td>{replayExperimentArtifactPayload?.experimentId ?? "--"}</td>
                        <td>{item.runId ?? "--"}</td>
                        <td>{item.type}</td>
                        <td>{item.storageBackend ?? "--"}</td>
                        <td>
                          {typeof item.byteSize === "number" ? item.byteSize : "--"}
                        </td>
                        <td>{formatCompactJson(item.inline ?? item.metadata)}</td>
                        <td>{item.downloadUrl ?? "--"}</td>
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
                    <th>type</th>
                    <th>name</th>
                    <th>contentType</th>
                    <th>byteSize</th>
                    <th>createdAt</th>
                    <th>preview</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {replayArtifactItems.length === 0 ? (
                    <tr>
                      <td className="table-empty-cell" colSpan={7}>
                        {hasLoadedReplayArtifacts
                          ? "无匹配 artifacts。"
                          : "尚未加载 artifacts。"}
                      </td>
                    </tr>
                  ) : (
                    replayArtifactItems.map((item) => (
                      <tr
                        key={`${replayArtifactPayload?.runId ?? "run"}:${item.type}`}
                      >
                        <td>{item.type}</td>
                        <td>{item.name ?? item.downloadName ?? "--"}</td>
                        <td>{item.contentType}</td>
                        <td>
                          {typeof item.byteSize === "number"
                            ? item.byteSize
                            : "--"}
                        </td>
                        <td>
                          {item.createdAt
                            ? formatDateTime(item.createdAt)
                            : "--"}
                        </td>
                        <td>{formatCompactJson(item.inline)}</td>
                        <td>
                          <button
                            type="button"
                            className="table-action"
                            disabled={downloadReplayArtifactMutation.isPending}
                            onClick={() => {
                              const runId =
                                replayArtifactPayload?.runId?.trim();
                              if (!runId) {
                                setReplayFeedback(null);
                                setReplayError(
                                  "下载回放工件前请先加载工件列表。",
                                );
                                return;
                              }
                              downloadReplayArtifactMutation.mutate({
                                runId,
                                artifactType: item.type,
                                downloadName: item.downloadName ?? item.name,
                              });
                            }}
                          >
                            {downloadReplayArtifactMutation.isPending
                              ? "下载中..."
                              : "下载"}
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </article>
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
                onChange={(event) =>
                  setSessionExportFormat(event.target.value as ExportFormat)
                }
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
                    setUsageExportDimension(
                      event.target.value as UsageExportDimension,
                    )
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
                  onChange={(event) =>
                    setUsageExportFormat(event.target.value as ExportFormat)
                  }
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

        {exportFeedback ? (
          <p className="feedback success">{exportFeedback}</p>
        ) : null}
        {exportError ? <p className="feedback error">{exportError}</p> : null}
      </section>
    </>
  );
}

function AgentsPage() {
  const [selectedAgentId, setSelectedAgentId] = useState<string | null>(null);

  const agentRuntimeViewsQuery = useQuery({
    queryKey: ["agent-runtime-views"],
    queryFn: ({ signal }) => fetchAgentRuntimeViews(signal),
    staleTime: 20_000,
  });

  const runtimeViews = agentRuntimeViewsQuery.data?.items ?? [];

  useEffect(() => {
    if (runtimeViews.length === 0) {
      setSelectedAgentId(null);
      return;
    }
    if (!selectedAgentId || !runtimeViews.some((item) => item.agentId === selectedAgentId)) {
      setSelectedAgentId(runtimeViews[0]?.agentId ?? null);
    }
  }, [runtimeViews, selectedAgentId]);

  const agentRuntimeConfigQuery = useQuery({
    queryKey: ["agent-runtime-config", selectedAgentId],
    enabled: Boolean(selectedAgentId),
    queryFn: ({ signal }) => fetchAgentRuntimeConfig(selectedAgentId!, signal),
    staleTime: 20_000,
  });

  const overview = useMemo(
    () => ({
      total: runtimeViews.length,
      online: runtimeViews.filter((item) => item.runtimeStatus === "online").length,
      stale: runtimeViews.filter((item) => item.runtimeStatus === "stale").length,
      neverSeen: runtimeViews.filter((item) => item.runtimeStatus === "never_seen").length,
    }),
    [runtimeViews],
  );

  const selectedRuntimeView =
    runtimeViews.find((item) => item.agentId === selectedAgentId) ?? null;
  const runtimeConfig: AgentRuntimeConfigResponse | null =
    agentRuntimeConfigQuery.data ?? null;

  return (
    <>
      <section className="panel">
        <header>
          <h2>Agent 守护状态</h2>
          <p>按租户查看 agent 最近心跳、采集状态与运行时配置快照。</p>
        </header>

        <section className="kpi-grid" aria-label="Agent 守护概览">
          <article className="kpi-card">
            <span>总 Agent</span>
            <strong>{overview.total}</strong>
          </article>
          <article className="kpi-card">
            <span>在线</span>
            <strong>{overview.online}</strong>
          </article>
          <article className="kpi-card">
            <span>陈旧</span>
            <strong>{overview.stale}</strong>
          </article>
          <article className="kpi-card">
            <span>未上报</span>
            <strong>{overview.neverSeen}</strong>
          </article>
        </section>

        {agentRuntimeViewsQuery.isLoading ? (
          <p className="feedback info">Agent 守护视图加载中...</p>
        ) : null}
        {agentRuntimeViewsQuery.isError ? (
          <p className="feedback error">
            加载 Agent 守护视图失败：{toErrorMessage(agentRuntimeViewsQuery.error)}
          </p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Agent</th>
                <th>主机</th>
                <th>版本</th>
                <th>状态</th>
                <th>Sources</th>
                <th>最近心跳</th>
                <th>上次写入</th>
                <th>最近结果</th>
              </tr>
            </thead>
            <tbody>
              {runtimeViews.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={8}>
                    暂无 agent 守护记录
                  </td>
                </tr>
              ) : (
                runtimeViews.map((item) => (
                  <tr
                    key={item.agentId}
                    onClick={() => setSelectedAgentId(item.agentId)}
                    style={{ cursor: "pointer" }}
                    aria-selected={selectedAgentId === item.agentId}
                  >
                    <td>{item.displayName}</td>
                    <td>{item.hostname}</td>
                    <td>{item.version ?? "--"}</td>
                    <td>{formatAgentRuntimeStatus(item.runtimeStatus)}</td>
                    <td>{item.sourceCount}</td>
                    <td>{formatOptionalDateTime(item.lastHeartbeatAt)}</td>
                    <td>{formatDateTime(item.updatedAt)}</td>
                    <td>
                      {item.lastIngestStatusCode === null
                        ? "--"
                        : `${item.lastIngestStatusCode} / ${item.lastAccepted}:${item.lastRejected}`}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>运行时配置</h2>
          <p>展示当前选中 agent 的服务端配置快照与 source 绑定。</p>
        </header>

        {!selectedRuntimeView ? (
          <p className="feedback empty">请选择一个 agent 查看运行时配置。</p>
        ) : null}
        {agentRuntimeConfigQuery.isLoading ? (
          <p className="feedback info">运行时配置加载中...</p>
        ) : null}
        {agentRuntimeConfigQuery.isError ? (
          <p className="feedback error">
            运行时配置加载失败：{toErrorMessage(agentRuntimeConfigQuery.error)}
          </p>
        ) : null}

        {selectedRuntimeView ? (
          <>
            <div className="detail-grid">
              <article className="detail-card">
                <h3>{selectedRuntimeView.displayName}</h3>
                <p>状态：{formatAgentRuntimeStatus(selectedRuntimeView.runtimeStatus)}</p>
                <p>配置版本：{selectedRuntimeView.lastConfigVersion ?? "--"}</p>
                <p>最近取配：{formatOptionalDateTime(selectedRuntimeView.lastConfigFetchedAt)}</p>
                <p>错误：{selectedRuntimeView.lastError ?? "--"}</p>
              </article>
              <article className="detail-card">
                <h3>服务端默认值</h3>
                <p>
                  心跳间隔：
                  {runtimeConfig?.runtime.heartbeatIntervalSeconds ??
                    selectedRuntimeView.heartbeatIntervalSeconds}
                  s
                </p>
                <p>
                  陈旧阈值：
                  {runtimeConfig?.runtime.staleAfterSeconds ??
                    selectedRuntimeView.staleAfterSeconds}
                  s
                </p>
                <p>
                  上报协议：
                  {runtimeConfig?.runtime.ingestProtocol ??
                    selectedRuntimeView.ingestProtocol}
                </p>
                <p>
                  Endpoint：
                  {runtimeConfig?.runtime.ingestEndpoint ??
                    selectedRuntimeView.ingestEndpoint ??
                    "--"}
                </p>
              </article>
            </div>

            <div className="table-wrapper">
              <table className="session-table">
                <thead>
                  <tr>
                    <th>Source ID</th>
                    <th>Name</th>
                    <th>Access Mode</th>
                    <th>Enabled</th>
                    <th>Location</th>
                    <th>Region</th>
                  </tr>
                </thead>
                <tbody>
                  {runtimeConfig?.bindings.sources.length ? (
                    runtimeConfig.bindings.sources.map((item) => (
                      <tr key={item.sourceId}>
                        <td>{item.sourceId}</td>
                        <td>{item.name}</td>
                        <td>{item.accessMode}</td>
                        <td>{item.enabled ? "true" : "false"}</td>
                        <td>{item.location}</td>
                        <td>{item.sourceRegion ?? "--"}</td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td className="table-empty-cell" colSpan={6}>
                        当前 agent 没有 source 绑定
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </>
        ) : null}
      </section>
    </>
  );
}

function SourcesPage() {
  const [sourceForm, setSourceForm] =
    useState<SourceFormState>(INITIAL_SOURCE_FORM);
  const [sourceFormError, setSourceFormError] = useState<string | null>(null);
  const [sourceFeedback, setSourceFeedback] = useState<string | null>(null);
  const [selectedSourceId, setSelectedSourceId] = useState<string | null>(null);
  const [editingSourceId, setEditingSourceId] = useState<string | null>(null);
  const [showMissingRegionOnly, setShowMissingRegionOnly] = useState(false);
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
      setSourceFeedback("新增成功，列表已刷新。");
      await queryClient.invalidateQueries({ queryKey: ["sources"] });
    },
  });

  const updateSourceMutation = useMutation({
    mutationFn: ({
      sourceId,
      input,
    }: {
      sourceId: string;
      input: {
        name: string;
        location: string;
        sourceRegion?: string;
        enabled: boolean;
      };
    }) => updateSource(sourceId, input),
    onSuccess: async (source) => {
      setEditingSourceId(null);
      setSourceForm(INITIAL_SOURCE_FORM);
      setSourceFormError(null);
      setSourceFeedback(`Source ${source.name} 已更新。`);
      await queryClient.invalidateQueries({ queryKey: ["sources"] });
    },
  });

  const sourceRegionBackfillMutation = useMutation({
    mutationFn: (input: { sourceIds?: string[] }) =>
      backfillSourceRegions(input),
    onSuccess: async (result) => {
      setSourceFeedback(
        result.updated > 0
          ? `已按主区域回填 ${result.updated} 个 Source（${result.primaryRegion}）。`
          : "当前没有可回填的缺失 sourceRegion。",
      );
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
  const missingRegionCount = sourceItems.filter(
    (item) => !item.sourceRegion?.trim(),
  ).length;
  const displayedSourceItems = showMissingRegionOnly
    ? sourceItems.filter((item) => !item.sourceRegion?.trim())
    : sourceItems;
  const selectedSource =
    sourceItems.find((item) => item.id === selectedSourceId) ?? null;

  useEffect(() => {
    if (displayedSourceItems.length === 0) {
      if (sourceItems.length === 0) {
        setSelectedSourceId(null);
      }
      return;
    }
    if (
      !selectedSourceId ||
      !displayedSourceItems.some((item) => item.id === selectedSourceId)
    ) {
      setSelectedSourceId(displayedSourceItems[0]?.id ?? null);
    }
  }, [displayedSourceItems, selectedSourceId, sourceItems.length]);

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
  const isSubmittingSource =
    createSourceMutation.isPending || updateSourceMutation.isPending;

  function resetSourceFormState() {
    setEditingSourceId(null);
    setSourceForm(INITIAL_SOURCE_FORM);
    setSourceFormError(null);
  }

  function beginEditSource(source: {
    id: string;
    name: string;
    type: SourceType;
    location: string;
    sourceRegion?: string;
    enabled: boolean;
  }) {
    setEditingSourceId(source.id);
    setSelectedSourceId(source.id);
    setSourceForm({
      name: source.name,
      type: source.type,
      location: source.location,
      sourceRegion: source.sourceRegion ?? "",
      enabled: source.enabled,
    });
    setSourceFormError(null);
    setSourceFeedback(null);
  }

  function handleBackfill(sourceIds?: string[]) {
    setSourceFeedback(null);
    sourceRegionBackfillMutation.mutate({ sourceIds });
  }

  function handleSourceSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const name = sourceForm.name.trim();
    const location = sourceForm.location.trim();
    const sourceRegion = sourceForm.sourceRegion.trim();
    if (!name || !location) {
      setSourceFormError("名称和位置不能为空。");
      return;
    }

    setSourceFormError(null);
    setSourceFeedback(null);
    if (editingSourceId) {
      updateSourceMutation.mutate({
        sourceId: editingSourceId,
        input: {
          name,
          location,
          sourceRegion: sourceRegion || undefined,
          enabled: sourceForm.enabled,
        },
      });
      return;
    }

    createSourceMutation.mutate({
      name,
      type: sourceForm.type,
      location,
      sourceRegion: sourceRegion || undefined,
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
          <div className="filters-row">
            <label
              className="checkbox-field"
              htmlFor="source-filter-missing-region"
            >
              <input
                id="source-filter-missing-region"
                type="checkbox"
                checked={showMissingRegionOnly}
                onChange={(event) =>
                  setShowMissingRegionOnly(event.target.checked)
                }
              />
              仅看缺失 Region
            </label>
            <span className="tiny-feedback">
              缺失 region：{missingRegionCount}
            </span>
            <button
              type="button"
              className="table-action"
              disabled={
                missingRegionCount === 0 ||
                sourceRegionBackfillMutation.isPending
              }
              onClick={() => handleBackfill()}
            >
              {sourceRegionBackfillMutation.isPending
                ? "回填中..."
                : "按主区域批量回填"}
            </button>
          </div>

          {sourceFeedback ? (
            <p className="feedback success">{sourceFeedback}</p>
          ) : null}
          {sourceRegionBackfillMutation.isError ? (
            <p className="feedback error">
              回填失败：{toErrorMessage(sourceRegionBackfillMutation.error)}
            </p>
          ) : null}

          <h3>来源列表</h3>
          {sourcesQuery.isLoading ? (
            <p className="feedback info">Sources 加载中...</p>
          ) : null}
          {sourcesQuery.isFetching && !sourcesQuery.isLoading ? (
            <p className="feedback info">Sources 刷新中...</p>
          ) : null}
          {sourcesQuery.isError ? (
            <p className="feedback error">
              Sources 加载失败：{toErrorMessage(sourcesQuery.error)}
            </p>
          ) : null}

          {!sourcesQuery.isLoading &&
          !sourcesQuery.isError &&
          displayedSourceItems.length === 0 &&
          sourceItems.length === 0 ? (
            <p className="feedback empty">暂无 Source，请先新增。</p>
          ) : null}

          {!sourcesQuery.isLoading &&
          !sourcesQuery.isError &&
          displayedSourceItems.length === 0 &&
          sourceItems.length > 0 ? (
            <p className="feedback empty">
              当前筛选下暂无缺失 region 的 Source。
            </p>
          ) : null}

          {!sourcesQuery.isError && displayedSourceItems.length > 0 ? (
            <div className="source-table-wrapper">
              <table className="source-table">
                <thead>
                  <tr>
                    <th>名称</th>
                    <th>类型</th>
                    <th>位置</th>
                    <th>Region</th>
                    <th>状态</th>
                    <th>创建时间</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {displayedSourceItems.map((source) => {
                    const latestResult = connectionResults[source.id];
                    const isSelected = source.id === selectedSourceId;
                    const isTesting =
                      testConnectionMutation.isPending &&
                      testConnectionMutation.variables === source.id;
                    const isBackfilling =
                      sourceRegionBackfillMutation.isPending &&
                      sourceRegionBackfillMutation.variables?.sourceIds?.[0] ===
                        source.id;

                    return (
                      <tr
                        key={source.id}
                        className={isSelected ? "is-selected-row" : ""}
                      >
                        <td>{source.name}</td>
                        <td>{source.type}</td>
                        <td>{source.location}</td>
                        <td>{source.sourceRegion?.trim() || "未设置"}</td>
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
                              onClick={() => beginEditSource(source)}
                            >
                              编辑
                            </button>
                            <button
                              type="button"
                              className="table-action"
                              disabled={isTesting}
                              onClick={() =>
                                testConnectionMutation.mutate(source.id)
                              }
                            >
                              {isTesting ? "测试中..." : "测试连接"}
                            </button>
                            {!source.sourceRegion?.trim() ? (
                              <button
                                type="button"
                                className="table-action"
                                disabled={isBackfilling}
                                onClick={() => handleBackfill([source.id])}
                              >
                                {isBackfilling ? "回填中..." : "按主区域回填"}
                              </button>
                            ) : null}
                          </div>
                          {latestResult ? (
                            <p
                              className={`tiny-feedback ${
                                latestResult.success
                                  ? "tiny-feedback-success"
                                  : "tiny-feedback-error"
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
            <section
              className="source-insight-panel"
              aria-label="Source 健康状态与解析失败"
            >
              <header className="source-insight-header">
                <h3>健康状态与最近解析失败</h3>
                <p>
                  {selectedSource
                    ? `当前 Source：${selectedSource.name}`
                    : "请先选择 Source"}
                </p>
              </header>

              <div className="source-insight-grid">
                <article className="source-insight-card">
                  <h4>健康状态</h4>
                  {!selectedSourceId ? (
                    <p className="feedback empty">请先选中一个 Source。</p>
                  ) : null}
                  {selectedSourceId && sourceHealthQuery.isLoading ? (
                    <p className="feedback info">健康状态加载中...</p>
                  ) : null}
                  {selectedSourceId && sourceHealthQuery.isError ? (
                    <p className="feedback error">
                      健康状态加载失败：
                      {toErrorMessage(sourceHealthQuery.error)}
                    </p>
                  ) : null}
                  {selectedSourceId && sourceHealthQuery.data ? (
                    <dl className="source-health-list">
                      <div className="source-health-row">
                        <dt>健康状态</dt>
                        <dd>
                          <span
                            className={`source-health-status ${sourceHealthStatus?.className ?? ""}`}
                          >
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
                        <dd>
                          {formatOptionalDateTime(
                            sourceHealthQuery.data.lastSuccessAt,
                          )}
                        </dd>
                      </div>
                      <div className="source-health-row">
                        <dt>最近失败</dt>
                        <dd>
                          {formatOptionalDateTime(
                            sourceHealthQuery.data.lastFailureAt,
                          )}
                        </dd>
                      </div>
                      <div className="source-health-row">
                        <dt>失败次数</dt>
                        <dd>
                          {sourceHealthQuery.data.failureCount.toLocaleString(
                            "zh-CN",
                          )}
                        </dd>
                      </div>
                      <div className="source-health-row">
                        <dt>平均延迟</dt>
                        <dd>
                          {sourceHealthQuery.data.avgLatencyMs === null
                            ? "--"
                            : `${Math.round(
                                sourceHealthQuery.data.avgLatencyMs,
                              ).toLocaleString("zh-CN")} ms`}
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
                  {!selectedSourceId ? (
                    <p className="feedback empty">请先选中一个 Source。</p>
                  ) : null}
                  {selectedSourceId && parseFailureQuery.isLoading ? (
                    <p className="feedback info">解析失败列表加载中...</p>
                  ) : null}
                  {selectedSourceId && parseFailureQuery.isError ? (
                    <p className="feedback error">
                      解析失败列表加载失败：
                      {toErrorMessage(parseFailureQuery.error)}
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
                            <time dateTime={item.failedAt}>
                              {formatDateTime(item.failedAt)}
                            </time>
                          </header>
                          <p>{item.errorMessage}</p>
                          <p>
                            parser={item.parserKey}
                            {item.sourcePath
                              ? ` | path=${item.sourcePath}`
                              : ""}
                            {item.sourceOffset !== undefined
                              ? ` | offset=${item.sourceOffset}`
                              : ""}
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
          <h3>{editingSourceId ? "编辑 Source" : "新增 Source"}</h3>
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
            disabled={Boolean(editingSourceId)}
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

          <label htmlFor="source-region">Region</label>
          <input
            id="source-region"
            type="text"
            placeholder="例如：cn-shanghai"
            value={sourceForm.sourceRegion}
            onChange={(event) =>
              setSourceForm((prev) => ({
                ...prev,
                sourceRegion: event.target.value,
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

          <div className="source-action-row">
            <button
              type="submit"
              className="submit-button"
              disabled={isSubmittingSource}
            >
              {isSubmittingSource
                ? "提交中..."
                : editingSourceId
                  ? "保存 Source"
                  : "新增 Source"}
            </button>
            {editingSourceId ? (
              <button
                type="button"
                className="table-action"
                onClick={resetSourceFormState}
                disabled={isSubmittingSource}
              >
                取消编辑
              </button>
            ) : null}
          </div>

          {sourceFormError ? (
            <p className="feedback error">{sourceFormError}</p>
          ) : null}
          {!editingSourceId && createSourceMutation.isError ? (
            <p className="feedback error">
              新增失败：{toErrorMessage(createSourceMutation.error)}
            </p>
          ) : null}
          {editingSourceId && updateSourceMutation.isError ? (
            <p className="feedback error">
              更新失败：{toErrorMessage(updateSourceMutation.error)}
            </p>
          ) : null}
        </form>
      </div>
    </section>
  );
}

function PricingPage() {
  const queryClient = useQueryClient();
  const [note, setNote] = useState("");
  const [entries, setEntries] = useState<PricingEntryFormState[]>([
    createEmptyPricingEntry(),
  ]);
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
          ? catalogQuery.data.entries.map((entry) =>
              mapPricingEntryToForm(entry),
            )
          : [createEmptyPricingEntry()],
      );
      return;
    }

    setNote("");
    setEntries([createEmptyPricingEntry()]);
  }, [catalogQuery.data, loadedVersionId]);

  const saveMutation = useMutation({
    mutationFn: (input: PricingCatalogUpsertInput) =>
      upsertPricingCatalog(input),
    onSuccess: async () => {
      setFormError(null);
      await queryClient.invalidateQueries({ queryKey: ["pricing-catalog"] });
    },
  });

  function updateEntry(
    index: number,
    key: keyof PricingEntryFormState,
    value: string,
  ) {
    setEntries((prev) =>
      prev.map((entry, idx) =>
        idx === index ? { ...entry, [key]: value } : entry,
      ),
    );
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
                catalogQuery.data.version.createdAt,
              )})`
            : "当前租户尚未配置 catalog，可直接新建。"}
        </p>
      </header>

      {catalogQuery.isLoading ? (
        <p className="feedback info">pricing 加载中...</p>
      ) : null}
      {catalogQuery.isError ? (
        <p className="feedback error">
          pricing 加载失败：{toErrorMessage(catalogQuery.error)}
        </p>
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
                      onChange={(event) =>
                        updateEntry(index, "model", event.target.value)
                      }
                      placeholder="gpt-5"
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={entry.inputPer1k}
                      onChange={(event) =>
                        updateEntry(index, "inputPer1k", event.target.value)
                      }
                      placeholder="0.003"
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={entry.outputPer1k}
                      onChange={(event) =>
                        updateEntry(index, "outputPer1k", event.target.value)
                      }
                      placeholder="0.012"
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={entry.currency}
                      onChange={(event) =>
                        updateEntry(index, "currency", event.target.value)
                      }
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
                            : [createEmptyPricingEntry()],
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
            onClick={() =>
              setEntries((prev) => [...prev, createEmptyPricingEntry()])
            }
          >
            新增条目
          </button>
          <button
            type="submit"
            className="submit-button"
            disabled={saveMutation.isPending}
          >
            {saveMutation.isPending ? "保存中..." : "保存 Catalog"}
          </button>
        </div>

        {formError ? <p className="feedback error">{formError}</p> : null}
        {saveMutation.isError ? (
          <p className="feedback error">
            保存失败：{toErrorMessage(saveMutation.error)}
          </p>
        ) : null}
        {saveMutation.isSuccess ? (
          <p className="feedback success">保存成功。</p>
        ) : null}
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
  const activeRoute =
    ROUTE_ITEMS.find((item) => item.key === route) ?? ROUTE_ITEMS[0];

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
      {route === "sessions" ? (
        <SessionsPage initialDateKey={sessionsDateKey} />
      ) : null}
      {route === "analytics" ? <AnalyticsPage /> : null}
      {route === "governance" ? <GovernancePage /> : null}
      {route === "agents" ? <AgentsPage /> : null}
      {route === "sources" ? <SourcesPage /> : null}
      {route === "pricing" ? <PricingPage /> : null}
    </main>
  );
}

export default function App() {
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            retry: import.meta.env.MODE === "test" ? false : 3,
            gcTime: import.meta.env.MODE === "test" ? 0 : 5 * 60 * 1000,
          },
          mutations: {
            retry: false,
          },
        },
      }),
  );
  const [isAuthenticated, setIsAuthenticated] = useState(() =>
    hasAccessToken(),
  );
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

  useEffect(() => {
    return () => {
      queryClient.clear();
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
