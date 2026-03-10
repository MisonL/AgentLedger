import { Hono, type Context } from "hono";
import * as contracts from "../contracts";
import {
  type AlertExternalLinkType,
  type AlertOrchestrationChannel,
  type AlertOrchestrationDispatchMode,
  type AlertOrchestrationEventType,
  type AlertOrchestrationRule,
  type AlertSeverity,
  validateAlertExternalLinkBatchRetryInput,
  validateAlertExternalLinkRetryInput,
  validateAlertListInput,
  validateAlertOrchestrationRuleListInput,
  validateAlertOrchestrationRuleUpsertInput,
  validateAlertStatusUpdateInput,
} from "../contracts";
import type { AppendAuditLogInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import { parseOptionalTimePaginationCursor } from "./pagination-cursor";
import type { AppEnv } from "../types";
import {
  publishAlertExternalStatusSyncEvents,
} from "./integration-event-publisher";

export const alertRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const ALERT_ORCHESTRATION_EVENT_TYPES = new Set<AlertOrchestrationEventType>([
  "alert",
  "weekly",
]);
const ALERT_ORCHESTRATION_DISPATCH_MODES = new Set<AlertOrchestrationDispatchMode>([
  "rule",
  "fallback",
]);
const ALERT_SEVERITY_TYPES = new Set<AlertSeverity>(["warning", "critical"]);

type ValidationResult<T> = { success: true; data: T } | { success: false; error: string };

type AlertOrchestrationSimulateInput = {
  eventType: AlertOrchestrationEventType;
  alertId?: string;
  severity?: AlertSeverity;
  sourceId?: string;
  dedupeHit?: boolean;
  suppressed?: boolean;
  ruleId?: string;
};

type AlertOrchestrationExecutionListInput = {
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
};

type ValidateAlertOrchestrationSimulateInput = (
  input: unknown
) => ValidationResult<AlertOrchestrationSimulateInput>;
type ValidateAlertOrchestrationExecutionListInput = (
  input: unknown
) => ValidationResult<AlertOrchestrationExecutionListInput>;

const validateAlertOrchestrationSimulateInput: ValidateAlertOrchestrationSimulateInput =
  typeof (
    contracts as {
      validateAlertOrchestrationSimulateInput?: ValidateAlertOrchestrationSimulateInput;
    }
  ).validateAlertOrchestrationSimulateInput === "function"
    ? (
        contracts as {
          validateAlertOrchestrationSimulateInput: ValidateAlertOrchestrationSimulateInput;
        }
      ).validateAlertOrchestrationSimulateInput
    : validateAlertOrchestrationSimulateInputFallback;

const validateAlertOrchestrationExecutionListInput: ValidateAlertOrchestrationExecutionListInput =
  typeof (
    contracts as {
      validateAlertOrchestrationExecutionListInput?: ValidateAlertOrchestrationExecutionListInput;
    }
  ).validateAlertOrchestrationExecutionListInput === "function"
    ? (
        contracts as {
          validateAlertOrchestrationExecutionListInput: ValidateAlertOrchestrationExecutionListInput;
        }
      ).validateAlertOrchestrationExecutionListInput
    : validateAlertOrchestrationExecutionListInputFallback;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function normalizeString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : undefined;
}

function toOptionalInteger(value: unknown): number | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? Math.trunc(value) : undefined;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return undefined;
    }
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? Math.trunc(parsed) : undefined;
  }
  return undefined;
}

function toOptionalBoolean(value: unknown): boolean | "invalid" | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (value === 1) {
      return true;
    }
    if (value === 0) {
      return false;
    }
    return "invalid";
  }
  if (typeof value !== "string") {
    return "invalid";
  }
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return "invalid";
  }
  if (normalized === "true" || normalized === "1") {
    return true;
  }
  if (normalized === "false" || normalized === "0") {
    return false;
  }
  return "invalid";
}

function isIsoDate(value: string): boolean {
  return !Number.isNaN(Date.parse(value));
}

function matchesSeverityWithWildcard(
  expected: AlertSeverity | undefined,
  actual: AlertSeverity | undefined
): boolean {
  if (!expected || !actual) {
    return true;
  }
  return expected === actual;
}

function matchesSourceWithWildcard(expected?: string, actual?: string): boolean {
  if (!expected || !actual) {
    return true;
  }
  return expected === actual;
}

function ruleMatchesSimulateInput(
  rule: AlertOrchestrationRule,
  input: AlertOrchestrationSimulateInput
): boolean {
  if (rule.eventType !== input.eventType) {
    return false;
  }
  if (input.ruleId && rule.id !== input.ruleId) {
    return false;
  }
  if (!matchesSeverityWithWildcard(input.severity, rule.severity)) {
    return false;
  }
  if (!matchesSourceWithWildcard(input.sourceId, rule.sourceId)) {
    return false;
  }
  return true;
}

function hasChannelOverlap(
  left: AlertOrchestrationChannel[],
  right: AlertOrchestrationChannel[]
): boolean {
  if (left.length === 0 || right.length === 0) {
    return false;
  }
  const leftSet = new Set(left);
  return right.some((channel) => leftSet.has(channel));
}

function detectRuleConflicts(
  rules: AlertOrchestrationRule[]
): Map<string, Set<string>> {
  const conflicts = new Map<string, Set<string>>();
  for (const rule of rules) {
    conflicts.set(rule.id, new Set<string>());
  }

  for (let i = 0; i < rules.length; i += 1) {
    const left = rules[i];
    for (let j = i + 1; j < rules.length; j += 1) {
      const right = rules[j];
      if (left.eventType !== right.eventType) {
        continue;
      }
      if (!hasChannelOverlap(left.channels, right.channels)) {
        continue;
      }
      if (!matchesSeverityWithWildcard(left.severity, right.severity)) {
        continue;
      }
      if (!matchesSourceWithWildcard(left.sourceId, right.sourceId)) {
        continue;
      }

      conflicts.get(left.id)?.add(right.id);
      conflicts.get(right.id)?.add(left.id);
    }
  }
  return conflicts;
}

function validateAlertOrchestrationSimulateInputFallback(
  input: unknown
): ValidationResult<AlertOrchestrationSimulateInput> {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }
  const eventType = normalizeString(input.eventType);
  const alertId = normalizeString(input.alertId);
  const severity = normalizeString(input.severity);
  const sourceId = normalizeString(input.sourceId);
  const dedupeHit = toOptionalBoolean(input.dedupeHit);
  const suppressed = toOptionalBoolean(input.suppressed);
  const ruleId = normalizeString(input.ruleId);

  if (!eventType || !ALERT_ORCHESTRATION_EVENT_TYPES.has(eventType as AlertOrchestrationEventType)) {
    return { success: false, error: "eventType 必填且必须是 alert/weekly 之一。" };
  }
  if (input.severity !== undefined && (!severity || !ALERT_SEVERITY_TYPES.has(severity as AlertSeverity))) {
    return { success: false, error: "severity 必须是 warning/critical 之一。" };
  }
  if (input.sourceId !== undefined && !sourceId) {
    return { success: false, error: "sourceId 必须为非空字符串。" };
  }
  if (input.alertId !== undefined && !alertId) {
    return { success: false, error: "alertId 必须为非空字符串。" };
  }
  if (dedupeHit === "invalid") {
    return { success: false, error: "dedupeHit 必须是 true/false 或 1/0。" };
  }
  if (suppressed === "invalid") {
    return { success: false, error: "suppressed 必须是 true/false 或 1/0。" };
  }
  if (input.ruleId !== undefined && !ruleId) {
    return { success: false, error: "ruleId 必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      eventType: eventType as AlertOrchestrationEventType,
      alertId,
      severity: severity as AlertSeverity | undefined,
      sourceId,
      dedupeHit: typeof dedupeHit === "boolean" ? dedupeHit : undefined,
      suppressed: typeof suppressed === "boolean" ? suppressed : undefined,
      ruleId,
    },
  };
}

function validateAlertOrchestrationExecutionListInputFallback(
  input: unknown
): ValidationResult<AlertOrchestrationExecutionListInput> {
  if (!isRecord(input)) {
    return { success: false, error: "查询参数必须是对象。" };
  }
  const ruleId = normalizeString(input.ruleId);
  const eventType = normalizeString(input.eventType);
  const alertId = normalizeString(input.alertId);
  const severity = normalizeString(input.severity);
  const sourceId = normalizeString(input.sourceId);
  const dedupeHit = toOptionalBoolean(input.dedupeHit);
  const suppressed = toOptionalBoolean(input.suppressed);
  const dispatchMode = normalizeString(input.dispatchMode);
  const hasConflict = toOptionalBoolean(input.hasConflict);
  const simulated = toOptionalBoolean(input.simulated);
  const from = normalizeString(input.from);
  const to = normalizeString(input.to);
  const limit = toOptionalInteger(input.limit);

  if (input.ruleId !== undefined && !ruleId) {
    return { success: false, error: "ruleId 必须为非空字符串。" };
  }
  if (
    input.eventType !== undefined &&
    (!eventType || !ALERT_ORCHESTRATION_EVENT_TYPES.has(eventType as AlertOrchestrationEventType))
  ) {
    return { success: false, error: "eventType 必须是 alert/weekly 之一。" };
  }
  if (input.alertId !== undefined && !alertId) {
    return { success: false, error: "alertId 必须为非空字符串。" };
  }
  if (input.severity !== undefined && (!severity || !ALERT_SEVERITY_TYPES.has(severity as AlertSeverity))) {
    return { success: false, error: "severity 必须是 warning/critical 之一。" };
  }
  if (input.sourceId !== undefined && !sourceId) {
    return { success: false, error: "sourceId 必须为非空字符串。" };
  }
  if (dedupeHit === "invalid") {
    return { success: false, error: "dedupeHit 必须是 true/false 或 1/0。" };
  }
  if (suppressed === "invalid") {
    return { success: false, error: "suppressed 必须是 true/false 或 1/0。" };
  }
  if (
    input.dispatchMode !== undefined &&
    (!dispatchMode ||
      !ALERT_ORCHESTRATION_DISPATCH_MODES.has(dispatchMode as AlertOrchestrationDispatchMode))
  ) {
    return { success: false, error: "dispatchMode 必须是 rule/fallback 之一。" };
  }
  if (hasConflict === "invalid") {
    return { success: false, error: "hasConflict 必须是 true/false 或 1/0。" };
  }
  if (simulated === "invalid") {
    return { success: false, error: "simulated 必须是 true/false 或 1/0。" };
  }
  if (from !== undefined && (!from || !isIsoDate(from))) {
    return { success: false, error: "from 必须是 ISO 日期字符串。" };
  }
  if (to !== undefined && (!to || !isIsoDate(to))) {
    return { success: false, error: "to 必须是 ISO 日期字符串。" };
  }
  if (from && to && Date.parse(from) > Date.parse(to)) {
    return { success: false, error: "from 必须早于或等于 to。" };
  }
  if (
    input.limit !== undefined &&
    (limit === undefined || !Number.isInteger(limit) || limit <= 0)
  ) {
    return { success: false, error: "limit 必须是大于 0 的整数。" };
  }

  return {
    success: true,
    data: {
      ruleId,
      eventType: eventType as AlertOrchestrationEventType | undefined,
      alertId,
      severity: severity as AlertSeverity | undefined,
      sourceId,
      dedupeHit: typeof dedupeHit === "boolean" ? dedupeHit : undefined,
      suppressed: typeof suppressed === "boolean" ? suppressed : undefined,
      dispatchMode: dispatchMode as AlertOrchestrationDispatchMode | undefined,
      hasConflict: typeof hasConflict === "boolean" ? hasConflict : undefined,
      simulated: typeof simulated === "boolean" ? simulated : undefined,
      from,
      to,
      limit: limit === undefined ? 50 : Math.min(limit, 200),
    },
  };
}

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 alert 审计日志失败。", error);
  }
}

async function requireAuthContext(c: Context<AppEnv>) {
  const authResult = await authMiddleware(c, async () => {});
  if (authResult instanceof Response) {
    return authResult;
  }

  const auth = c.get("auth");
  if (!auth) {
    return c.json({ message: "未认证：请先登录。" }, 401);
  }
  return auth;
}

type AlertExternalLinkRecord = {
  externalType: AlertExternalLinkType;
  externalSystem: string;
  externalId: string;
  externalStatus?: string;
  pendingExternalStatus?: string;
  metadata: Record<string, unknown>;
};

type AlertExternalLinkOpsItem = AlertExternalLinkRecord & {
  id: string;
  lastSyncedAt: string;
  publishStatus?: "success" | "failed";
  publishError?: string;
  lastSyncResult?: "success" | "failed";
  lastSyncError?: string;
  lastSyncFailureStage?: string;
  lastSyncFailureCode?: string;
  createdAt: string;
  updatedAt: string;
  syncState: "synced" | "pending" | "failed";
  retryable: boolean;
};

type AlertExternalLinkFailureItem = AlertExternalLinkOpsItem & {
  alertId: string;
  alertStatus: "open" | "acknowledged" | "resolved";
};

const ALERT_EXTERNAL_LINK_FAILURE_QUERY_LIMIT_DEFAULT = 20;
const ALERT_EXTERNAL_LINK_FAILURE_QUERY_LIMIT_MAX = 200;
const ALERT_EXTERNAL_LINK_FAILURE_FETCH_ALERT_PAGE_LIMIT = 200;
const ALERT_EXTERNAL_LINK_FAILURE_FETCH_MAX_PAGES = 1000;

function buildAlertExternalStatusSyncCallbackId(
  alertId: string,
  link: Pick<AlertExternalLinkRecord, "externalType" | "externalId">,
  externalStatus: string,
): string {
  return `alert-status-sync:${alertId}:${link.externalType}:${link.externalId}:${externalStatus}`;
}

function resolvePendingExternalStatus(
  link: AlertExternalLinkRecord,
  fallbackStatus: string,
): string {
  return (
    normalizeString(link.pendingExternalStatus) ??
    normalizeString(link.metadata.pendingExternalStatus) ??
    normalizeString(link.metadata.pending_external_status) ??
    fallbackStatus
  );
}

function isAlertExternalLinkRetryableRecord(
  link: AlertExternalLinkRecord,
  alertStatus: string,
): boolean {
  const desiredStatus = resolvePendingExternalStatus(link, alertStatus);
  return (
    link.metadata.publishStatus === "failed" ||
    link.metadata.publish_status === "failed" ||
    link.metadata.lastSyncResult === "failed" ||
    link.metadata.last_sync_result === "failed" ||
    desiredStatus !== (normalizeString(link.externalStatus) ?? "")
  );
}

function resolveAlertExternalLinkSyncState(
  link: AlertExternalLinkRecord,
  alertStatus: string,
): "synced" | "pending" | "failed" {
  const desiredStatus = resolvePendingExternalStatus(link, alertStatus);
  if (
    link.metadata.publishStatus === "failed" ||
    link.metadata.publish_status === "failed" ||
    link.metadata.lastSyncResult === "failed" ||
    link.metadata.last_sync_result === "failed"
  ) {
    return "failed";
  }
  if (desiredStatus !== (normalizeString(link.externalStatus) ?? "")) {
    return "pending";
  }
  return "synced";
}

function buildAlertExternalLinkOpsItem(
  link: AlertExternalLinkRecord & {
    id: string;
    lastSyncedAt: string;
    publishStatus?: "success" | "failed";
    publishError?: string;
    lastSyncResult?: "success" | "failed";
    lastSyncError?: string;
    lastSyncFailureStage?: string;
    lastSyncFailureCode?: string;
    createdAt: string;
    updatedAt: string;
  },
  alertStatus: string,
): AlertExternalLinkOpsItem {
  const syncState = resolveAlertExternalLinkSyncState(link, alertStatus);
  return {
    ...link,
    syncState,
    retryable: syncState !== "synced",
  };
}

function parseAlertExternalLinkOpsQueryInput(
  query: Record<string, string>,
): { success: true; data: { externalType?: AlertExternalLinkType; onlyFailed?: boolean } } | {
  success: false;
  error: string;
} {
  const externalType = normalizeString(query.externalType);
  if (
    query.externalType !== undefined &&
    (!externalType ||
      (externalType !== "ticket" &&
        externalType !== "case" &&
        externalType !== "incident"))
  ) {
    return {
      success: false,
      error: "externalType 必须是 ticket/case/incident 之一。",
    };
  }
  const onlyFailedRaw = normalizeString(query.onlyFailed);
  let onlyFailed: boolean | undefined;
  if (query.onlyFailed !== undefined) {
    if (onlyFailedRaw === "true") {
      onlyFailed = true;
    } else if (onlyFailedRaw === "false") {
      onlyFailed = false;
    } else {
      return {
        success: false,
        error: "onlyFailed 必须是 true/false。",
      };
    }
  }

  return {
    success: true,
    data: {
      externalType: externalType as AlertExternalLinkType | undefined,
      onlyFailed,
    },
  };
}

function parseAlertExternalLinkFailureQueryInput(
  query: Record<string, string>,
): {
  success: true;
  data: {
    alertId?: string;
    externalType?: AlertExternalLinkType;
    externalSystem?: string;
    syncState?: "synced" | "pending" | "failed";
    limit: number;
  };
} | {
  success: false;
  error: string;
} {
  const alertId = normalizeString(query.alertId);
  if (query.alertId !== undefined && !alertId) {
    return { success: false, error: "alertId 必须为非空字符串。" };
  }

  const externalType = normalizeString(query.externalType);
  if (
    query.externalType !== undefined &&
    (!externalType ||
      (externalType !== "ticket" &&
        externalType !== "case" &&
        externalType !== "incident"))
  ) {
    return {
      success: false,
      error: "externalType 必须是 ticket/case/incident 之一。",
    };
  }

  const externalSystem = normalizeString(query.externalSystem);
  if (query.externalSystem !== undefined && !externalSystem) {
    return { success: false, error: "externalSystem 必须为非空字符串。" };
  }

  const syncState = normalizeString(query.syncState);
  if (
    query.syncState !== undefined &&
    (!syncState ||
      (syncState !== "synced" &&
        syncState !== "pending" &&
        syncState !== "failed"))
  ) {
    return {
      success: false,
      error: "syncState 必须是 synced/pending/failed 之一。",
    };
  }

  const limit = toOptionalInteger(query.limit);
  if (
    query.limit !== undefined &&
    (limit === undefined ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > ALERT_EXTERNAL_LINK_FAILURE_QUERY_LIMIT_MAX)
  ) {
    return {
      success: false,
      error: `limit 必须是 1 到 ${ALERT_EXTERNAL_LINK_FAILURE_QUERY_LIMIT_MAX} 的整数。`,
    };
  }

  return {
    success: true,
    data: {
      alertId,
      externalType: externalType as AlertExternalLinkType | undefined,
      externalSystem,
      syncState: syncState as "synced" | "pending" | "failed" | undefined,
      limit: limit ?? ALERT_EXTERNAL_LINK_FAILURE_QUERY_LIMIT_DEFAULT,
    },
  };
}

async function listAllAlertsForExternalLinkFailures(tenantId: string) {
  const items: Awaited<ReturnType<typeof repository.listAlerts>>["items"] = [];
  let cursor: string | undefined;
  let total = 0;

  for (
    let page = 0;
    page < ALERT_EXTERNAL_LINK_FAILURE_FETCH_MAX_PAGES;
    page += 1
  ) {
    const payload = await repository.listAlerts(tenantId, {
      limit: ALERT_EXTERNAL_LINK_FAILURE_FETCH_ALERT_PAGE_LIMIT,
      cursor,
    });
    if (page === 0) {
      total = payload.total;
    }
    items.push(...payload.items);
    if (!payload.nextCursor) {
      return { items, total };
    }
    cursor = payload.nextCursor;
  }

  throw new Error(
    `告警外部联动失败查询超过分页上限（${ALERT_EXTERNAL_LINK_FAILURE_FETCH_MAX_PAGES} 页）。`,
  );
}

function buildAlertExternalLinkFailureItem(
  alert: {
    id: string;
    status: "open" | "acknowledged" | "resolved";
  },
  link: AlertExternalLinkOpsItem,
): AlertExternalLinkFailureItem {
  return {
    ...link,
    alertId: alert.id,
    alertStatus: alert.status,
  };
}

function buildAlertExternalStatusSyncEvent(
  tenantId: string,
  alertId: string,
  link: AlertExternalLinkRecord,
  fromStatus: string,
  toStatus: string,
  syncedAt: string,
) {
  return {
    callback_id: buildAlertExternalStatusSyncCallbackId(alertId, link, toStatus),
    tenant_id: tenantId,
    action: "upsert_external_link" as const,
    alert_id: alertId,
    external_type: link.externalType,
    external_system: link.externalSystem,
    external_id: link.externalId,
    external_status: toStatus,
    from_status: fromStatus,
    to_status: toStatus,
    updated_at: syncedAt,
    metadata: {
      source: "control_plane_alert_status",
      fromStatus,
      toStatus,
      syncedAt,
    },
  };
}

function buildAlertExternalStatusSyncMetadata(
  link: AlertExternalLinkRecord,
  options: {
    alertId: string;
    fromStatus: string;
    toStatus: string;
    syncedAt: string;
    syncSource: "control_plane_alert_status" | "control_plane_alert_retry";
    publishError?: string;
  },
): Record<string, unknown> {
  return {
    ...link.metadata,
    syncSource: options.syncSource,
    fromStatus: options.fromStatus,
    toStatus: options.toStatus,
    pendingExternalStatus: options.toStatus,
    publishStatus: options.publishError ? "failed" : "success",
    publishError: options.publishError ?? null,
    lastSyncResult: null,
    lastSyncError: null,
    lastSyncFailureStage: null,
    lastSyncFailureCode: null,
    retryRequestedAt:
      options.syncSource === "control_plane_alert_retry" ? options.syncedAt : null,
    callbackId: buildAlertExternalStatusSyncCallbackId(
      options.alertId,
      link,
      options.toStatus,
    ),
  };
}

alertRoutes.get("/alerts", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateAlertListInput(c.req.query());
  if (!result.success) {
    return c.json(
      {
        message: result.error,
      },
      400
    );
  }
  const cursorResult = parseOptionalTimePaginationCursor(result.data.cursor);
  if (!cursorResult.success) {
    return c.json({ message: cursorResult.error }, 400);
  }

  const tenantId = auth.tenantId;
  const payload = await repository.listAlerts(tenantId, {
    ...result.data,
    cursor: cursorResult.cursor,
  });

  return c.json({
    items: payload.items,
    total: payload.total,
    filters: {
      ...result.data,
      cursor: cursorResult.cursor,
    },
    nextCursor: payload.nextCursor,
  });
});

alertRoutes.get("/alerts/orchestration/rules", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateAlertOrchestrationRuleListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const tenantId = auth.tenantId;
  const payload = await repository.listAlertOrchestrationRules(tenantId, result.data);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.alert_orchestration.query",
    level: "info",
    detail: "Queried alert orchestration rules.",
    metadata: {
      requestId,
      tenantId,
      filters: result.data,
      total: payload.total,
    },
  });

  return c.json({
    items: payload.items,
    total: payload.total,
    filters: result.data,
  });
});

alertRoutes.put("/alerts/orchestration/rules/:id", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const ruleId = c.req.param("id")?.trim();
  if (!ruleId) {
    return c.json({ message: "ruleId 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = typeof body === "object" && body !== null ? body : {};
  const tenantId = auth.tenantId;
  const result = validateAlertOrchestrationRuleUpsertInput({
    ...bodyRecord,
    id: ruleId,
    tenantId,
    updatedAt:
      typeof (bodyRecord as Record<string, unknown>).updatedAt === "string"
        ? (bodyRecord as Record<string, unknown>).updatedAt
        : new Date().toISOString(),
  });
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const rule = await repository.upsertAlertOrchestrationRule(tenantId, result.data);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.alert_orchestration.upsert",
    level: "info",
    detail: `Upserted alert orchestration rule ${rule.id}.`,
    metadata: {
      requestId,
      tenantId,
      ruleId: rule.id,
      resourceId: rule.id,
      eventType: rule.eventType,
      enabled: rule.enabled,
      channels: rule.channels,
    },
  });

  return c.json(rule);
});

alertRoutes.post("/alerts/orchestration/simulate", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = isRecord(body) ? body : {};
  const result = validateAlertOrchestrationSimulateInput(bodyRecord);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const simulateInput: AlertOrchestrationSimulateInput = result.data;

  const tenantId = auth.tenantId;
  const matchedCandidatePayload = await repository.listAlertOrchestrationRules(tenantId, {
    eventType: simulateInput.eventType,
    enabled: true,
  });
  const matchedRules = matchedCandidatePayload.items.filter((rule) =>
    ruleMatchesSimulateInput(rule, simulateInput)
  );
  const conflicts = detectRuleConflicts(matchedRules);
  const conflictRuleIds = Array.from(conflicts.entries())
    .filter(([, value]) => value.size > 0)
    .map(([ruleId]) => ruleId)
    .sort((left, right) => left.localeCompare(right));

  const requestId = c.get("requestId");
  const createdAt = new Date().toISOString();
  const executions = await Promise.all(
    matchedRules.map(async (rule) => {
      const ruleConflictRuleIds = Array.from(conflicts.get(rule.id) ?? [])
        .filter((conflictRuleId) => conflictRuleId !== rule.id)
        .sort((left, right) => left.localeCompare(right));
      return repository.createAlertOrchestrationExecutionLog(tenantId, {
        ruleId: rule.id,
        eventType: simulateInput.eventType,
        alertId: simulateInput.alertId,
        severity: simulateInput.severity,
        sourceId: simulateInput.sourceId,
        channels: rule.channels,
        conflictRuleIds: ruleConflictRuleIds,
        dedupeHit: simulateInput.dedupeHit,
        suppressed: simulateInput.suppressed,
        simulated: true,
        metadata: {
          requestId,
          ruleName: rule.name,
          simulateInput,
        },
        createdAt,
      });
    })
  );

  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.alert_orchestration.simulate",
    level: "info",
    detail: `Simulated alert orchestration with ${matchedRules.length} matched rules.`,
    metadata: {
      requestId,
      tenantId,
      input: simulateInput,
      matchedRuleIds: matchedRules.map((rule) => rule.id),
      conflictRuleIds,
      executionCount: executions.length,
    },
  });

  return c.json({
    matchedRules,
    conflictRuleIds,
    executions,
  });
});

alertRoutes.get("/alerts/orchestration/executions", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateAlertOrchestrationExecutionListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const tenantId = auth.tenantId;
  const payload = await repository.listAlertOrchestrationExecutionLogs(tenantId, result.data);
  const items = payload.items.map((item) => ({
    ...item,
    channels: [...item.channels],
    conflictRuleIds: [...item.conflictRuleIds],
    metadata: { ...item.metadata },
  }));

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.alert_orchestration.executions.query",
    level: "info",
    detail: `Queried alert orchestration execution logs (${items.length}/${payload.total}).`,
    metadata: {
      requestId,
      tenantId,
      filters: result.data,
      total: payload.total,
      returned: items.length,
    },
  });

  return c.json({
    items,
    total: payload.total,
    filters: result.data,
  });
});

alertRoutes.patch("/alerts/:id/status", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const alertId = c.req.param("id")?.trim();
  if (!alertId) {
    return c.json(
      {
        message: "alertId 必须为非空字符串。",
      },
      400
    );
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateAlertStatusUpdateInput(body);
  if (!result.success) {
    return c.json(
      {
        message: result.error,
      },
      400
    );
  }

  const tenantId = auth.tenantId;
  const currentAlert = await repository.getAlertById(tenantId, alertId);
  if (!currentAlert) {
    return c.json(
      {
        message: `未找到告警 ${alertId}。`,
      },
      404
    );
  }

  const updatedAlert = await repository.updateAlertStatus(
    tenantId,
    alertId,
    result.data.status
  );
  if (!updatedAlert) {
    return c.json(
      {
        message: `未找到告警 ${alertId}。`,
      },
      404
    );
  }

  let updatedBudgetGovernanceState: string | undefined;
  let syncedExternalLinks: unknown[] = [];
  let externalStatusSyncPublished = 0;
  let externalStatusSyncFailed = 0;
  if (updatedAlert.status === "acknowledged" && updatedAlert.severity === "critical") {
    const frozenBudget = await repository.freezeBudget(tenantId, updatedAlert.budgetId, {
      reason: "critical 告警已确认，预算已冻结。",
      alertId: alertId,
    });
    updatedBudgetGovernanceState = frozenBudget?.governanceState;
  }
  if (Array.isArray(currentAlert.externalLinks) && currentAlert.externalLinks.length > 0) {
    const syncedAt = new Date().toISOString();
    const events = currentAlert.externalLinks.map((link) =>
      buildAlertExternalStatusSyncEvent(
        tenantId,
        alertId,
        link,
        currentAlert.status,
        updatedAlert.status,
        syncedAt,
      ),
    );
    const publishResult = await publishAlertExternalStatusSyncEvents(events);
    externalStatusSyncPublished = publishResult.published;
    externalStatusSyncFailed = publishResult.failed;
    const errorByCallbackId = new Map(
      publishResult.errors.map((item) => [item.callbackId, item.message]),
    );
    syncedExternalLinks = (
      await Promise.all(
        currentAlert.externalLinks.map((link) =>
          repository.upsertAlertExternalLink(tenantId, {
            alertId,
            externalType: link.externalType,
            externalSystem: link.externalSystem,
            externalId: link.externalId,
            externalStatus: link.externalStatus,
            metadata: buildAlertExternalStatusSyncMetadata(link, {
              alertId,
              fromStatus: currentAlert.status,
              toStatus: updatedAlert.status,
              syncedAt,
              syncSource: "control_plane_alert_status",
              publishError: errorByCallbackId.get(
                buildAlertExternalStatusSyncCallbackId(
                  alertId,
                  link,
                  updatedAlert.status,
                ),
              ),
            }),
            syncedAt,
          }),
        ),
      )
    ).filter(Boolean);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.alert_status_updated",
    level: "info",
    detail: `Updated alert ${alertId} status from ${currentAlert.status} to ${updatedAlert.status}.`,
    metadata: {
      requestId,
      tenantId,
      alertId,
      resourceId: alertId,
      budgetId: updatedAlert.budgetId,
      fromStatus: currentAlert.status,
      toStatus: updatedAlert.status,
      budgetGovernanceState: updatedBudgetGovernanceState,
      syncedExternalLinkCount: Array.isArray(syncedExternalLinks)
        ? syncedExternalLinks.length
        : 0,
      externalStatusSyncPublished,
      externalStatusSyncFailed,
    },
  });

  const refreshedAlert = await repository.getAlertById(tenantId, alertId);
  return c.json(refreshedAlert ?? updatedAlert);
});

alertRoutes.get("/alerts/external-links/failures", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const queryResult = parseAlertExternalLinkFailureQueryInput(c.req.query());
  if (!queryResult.success) {
    return c.json({ message: queryResult.error }, 400);
  }

  let alertsPayload: Awaited<ReturnType<typeof listAllAlertsForExternalLinkFailures>>;
  try {
    alertsPayload = await listAllAlertsForExternalLinkFailures(auth.tenantId);
  } catch (error) {
    const message =
      error instanceof Error && error.message.trim().length > 0
        ? error.message
        : "查询外部联动失败项失败。";
    return c.json({ message }, 422);
  }

  const scopedItems = alertsPayload.items.flatMap((alert) =>
    (alert.externalLinks ?? [])
      .map((item) => buildAlertExternalLinkOpsItem(item, alert.status))
      .map((item) => buildAlertExternalLinkFailureItem(alert, item))
      .filter((item) =>
        queryResult.data.alertId ? item.alertId === queryResult.data.alertId : true,
      )
      .filter((item) =>
        queryResult.data.externalType
          ? item.externalType === queryResult.data.externalType
          : true,
      )
      .filter((item) =>
        queryResult.data.externalSystem
          ? item.externalSystem === queryResult.data.externalSystem
          : true,
      )
      .filter((item) =>
        queryResult.data.syncState ? item.syncState === queryResult.data.syncState : true,
      ),
  );

  const sortedItems = scopedItems.slice().sort((left, right) => {
    const updatedCompare = right.updatedAt.localeCompare(left.updatedAt);
    if (updatedCompare !== 0) {
      return updatedCompare;
    }
    return right.id.localeCompare(left.id);
  });
  const items = sortedItems.slice(0, queryResult.data.limit);
  const summary = scopedItems.reduce(
    (acc, item) => {
      acc.total += 1;
      if (item.syncState === "pending") {
        acc.pending += 1;
      }
      if (item.syncState === "failed") {
        acc.failed += 1;
      }
      return acc;
    },
    { total: 0, pending: 0, failed: 0 },
  );

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.alert_external_link_failures_queried",
    level: "info",
    detail: "Queried alert external link failures.",
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      filters: queryResult.data,
      returned: items.length,
      total: summary.total,
      pending: summary.pending,
      failed: summary.failed,
      scannedAlerts: alertsPayload.items.length,
      alertTotal: alertsPayload.total,
    },
  });

  return c.json({
    summary,
    items,
    filters: queryResult.data,
  });
});

alertRoutes.get("/alerts/:id/external-links", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const alertId = c.req.param("id")?.trim();
  if (!alertId) {
    return c.json({ message: "alertId 必须为非空字符串。" }, 400);
  }

  const queryResult = parseAlertExternalLinkOpsQueryInput(c.req.query());
  if (!queryResult.success) {
    return c.json({ message: queryResult.error }, 400);
  }

  const alert = await repository.getAlertById(auth.tenantId, alertId);
  if (!alert) {
    return c.json({ message: `未找到告警 ${alertId}。` }, 404);
  }

  const scopedItems = (alert.externalLinks ?? [])
    .filter((item) =>
      queryResult.data.externalType
        ? item.externalType === queryResult.data.externalType
        : true,
    )
    .map((item) => buildAlertExternalLinkOpsItem(item, alert.status));
  const summary = scopedItems.reduce(
    (acc, item) => {
      acc.total += 1;
      if (item.syncState === "pending") {
        acc.pending += 1;
      }
      if (item.syncState === "failed") {
        acc.failed += 1;
      }
      return acc;
    },
    { total: 0, pending: 0, failed: 0 },
  );
  const items =
    queryResult.data.onlyFailed === true
      ? scopedItems.filter((item) => item.syncState === "failed")
      : scopedItems;

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.alert_external_links_queried",
    level: "info",
    detail: `Queried external links for alert ${alertId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      alertId,
      resourceId: alertId,
      filters: queryResult.data,
      returned: items.length,
      total: summary.total,
      pending: summary.pending,
      failed: summary.failed,
    },
  });

  return c.json({
    alertId,
    summary,
    items,
    filters: queryResult.data,
  });
});

alertRoutes.post("/alerts/:id/external-links/retry-sync", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const alertId = c.req.param("id")?.trim();
  if (!alertId) {
    return c.json({ message: "alertId 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateAlertExternalLinkRetryInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const alert = await repository.getAlertById(auth.tenantId, alertId);
  if (!alert) {
    return c.json({ message: `未找到告警 ${alertId}。` }, 404);
  }
  const matchedLink =
    alert.externalLinks?.find(
      (item) =>
        item.externalType === result.data.externalType &&
        item.externalId === result.data.externalId,
    ) ?? null;
  if (!matchedLink) {
    return c.json(
      {
        message: `未找到告警 ${alertId} 对应的外部联动 ${result.data.externalType}:${result.data.externalId}。`,
      },
      404,
    );
  }

  const desiredExternalStatus = resolvePendingExternalStatus(
    matchedLink,
    alert.status,
  );
  const hasRetryableState =
    matchedLink.publishStatus === "failed" ||
    matchedLink.lastSyncResult === "failed" ||
    desiredExternalStatus !== (matchedLink.externalStatus ?? "");
  if (!hasRetryableState) {
    return c.json(
      {
        message: `告警 ${alertId} 的外部联动 ${matchedLink.externalType}:${matchedLink.externalId} 当前无待重试同步状态。`,
      },
      409,
    );
  }

  const syncedAt = new Date().toISOString();
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:alert-external-link-retry-requested`,
    action: "control_plane.alert_external_link_retry_requested",
    level: "info",
    detail: `Requested external link retry for alert ${alertId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      alertId,
      resourceId: alertId,
      scope: "single",
      externalType: matchedLink.externalType,
      externalId: matchedLink.externalId,
      externalSystem: matchedLink.externalSystem,
      fromStatus: matchedLink.externalStatus ?? alert.status,
      toStatus: desiredExternalStatus,
    },
  });
  const event = buildAlertExternalStatusSyncEvent(
    auth.tenantId,
    alertId,
    matchedLink,
    matchedLink.externalStatus ?? alert.status,
    desiredExternalStatus,
    syncedAt,
  );
  const publishResult = await publishAlertExternalStatusSyncEvents([event]);
  const publishError = publishResult.errors[0]?.message;

  await repository.upsertAlertExternalLink(auth.tenantId, {
    alertId,
    externalType: matchedLink.externalType,
    externalSystem: matchedLink.externalSystem,
    externalId: matchedLink.externalId,
    externalStatus: matchedLink.externalStatus,
    metadata: buildAlertExternalStatusSyncMetadata(matchedLink, {
      alertId,
      fromStatus: matchedLink.externalStatus ?? alert.status,
      toStatus: desiredExternalStatus,
      syncedAt,
      syncSource: "control_plane_alert_retry",
      publishError,
    }),
    syncedAt,
  });

  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: publishError
      ? "control_plane.alert_external_link_retry_failed"
      : "control_plane.alert_external_link_retry_completed",
    level: publishError ? "warning" : "info",
    detail: publishError
      ? `Retried external link sync for alert ${alertId}, but publish failed.`
      : `Retried external link sync for alert ${alertId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      alertId,
      resourceId: alertId,
      scope: "single",
      externalType: matchedLink.externalType,
      externalId: matchedLink.externalId,
      externalSystem: matchedLink.externalSystem,
      fromStatus: matchedLink.externalStatus ?? alert.status,
      toStatus: desiredExternalStatus,
      publishStatus: publishError ? "failed" : "success",
      publishError: publishError ?? null,
      published: publishResult.published,
      failed: publishResult.failed,
    },
  });

  const refreshedAlert = await repository.getAlertById(auth.tenantId, alertId);
  if (publishError) {
    return c.json(
      {
        message: `外部联动重试发布失败：${publishError}`,
        ...(refreshedAlert ?? alert),
      },
      502,
    );
  }
  return c.json(refreshedAlert ?? alert);
});

alertRoutes.post("/alerts/:id/external-links/retry-sync-batch", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const alertId = c.req.param("id")?.trim();
  if (!alertId) {
    return c.json({ message: "alertId 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateAlertExternalLinkBatchRetryInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const alert = await repository.getAlertById(auth.tenantId, alertId);
  if (!alert) {
    return c.json({ message: `未找到告警 ${alertId}。` }, 404);
  }

  const retryableLinks =
    alert.externalLinks?.filter((item) => {
      if (
        result.data.externalType &&
        item.externalType !== result.data.externalType
      ) {
        return false;
      }
      return isAlertExternalLinkRetryableRecord(item, alert.status);
    }) ?? [];

  if (retryableLinks.length === 0) {
    return c.json(
      {
        message: `告警 ${alertId} 当前没有可批量重试的外部联动。`,
      },
      409,
    );
  }

  const syncedAt = new Date().toISOString();
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:alert-external-link-retry-requested`,
    action: "control_plane.alert_external_link_retry_requested",
    level: "info",
    detail: `Requested batch external link retry for alert ${alertId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      alertId,
      resourceId: alertId,
      scope: "batch",
      externalType: result.data.externalType ?? null,
      retriedCount: retryableLinks.length,
    },
  });
  const events = retryableLinks.map((link) =>
    buildAlertExternalStatusSyncEvent(
      auth.tenantId,
      alertId,
      link,
      link.externalStatus ?? alert.status,
      resolvePendingExternalStatus(link, alert.status),
      syncedAt,
    ),
  );
  const publishResult = await publishAlertExternalStatusSyncEvents(events);
  const errorByCallbackId = new Map(
    publishResult.errors.map((item) => [item.callbackId, item.message]),
  );

  await Promise.all(
    retryableLinks.map((link) =>
      repository.upsertAlertExternalLink(auth.tenantId, {
        alertId,
        externalType: link.externalType,
        externalSystem: link.externalSystem,
        externalId: link.externalId,
        externalStatus: link.externalStatus,
        metadata: buildAlertExternalStatusSyncMetadata(link, {
          alertId,
          fromStatus: link.externalStatus ?? alert.status,
          toStatus: resolvePendingExternalStatus(link, alert.status),
          syncedAt,
          syncSource: "control_plane_alert_retry",
          publishError: errorByCallbackId.get(
            buildAlertExternalStatusSyncCallbackId(
              alertId,
              link,
              resolvePendingExternalStatus(link, alert.status),
            ),
          ),
        }),
        syncedAt,
      }),
    ),
  );

  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action:
      publishResult.failed > 0
        ? "control_plane.alert_external_link_retry_failed"
        : "control_plane.alert_external_link_retry_completed",
    level: publishResult.failed > 0 ? "warning" : "info",
    detail:
      publishResult.failed > 0
        ? `Retried ${retryableLinks.length} external links for alert ${alertId}, with ${publishResult.failed} publish failures.`
        : `Retried ${retryableLinks.length} external links for alert ${alertId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      alertId,
      resourceId: alertId,
      scope: "batch",
      externalType: result.data.externalType ?? null,
      retriedCount: retryableLinks.length,
      published: publishResult.published,
      failed: publishResult.failed,
    },
  });

  const refreshedAlert = await repository.getAlertById(auth.tenantId, alertId);
  const resultAlert = refreshedAlert ?? alert;
  const resultItems = (resultAlert.externalLinks ?? [])
    .filter((item) =>
      result.data.externalType ? item.externalType === result.data.externalType : true,
    )
    .map((item) => buildAlertExternalLinkOpsItem(item, resultAlert.status));
  return c.json({
    alertId,
    retriedCount: retryableLinks.length,
    published: publishResult.published,
    failed: publishResult.failed,
    items: resultItems,
  });
});
