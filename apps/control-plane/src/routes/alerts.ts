import { Hono, type Context } from "hono";
import * as contracts from "../contracts";
import {
  type AlertOrchestrationChannel,
  type AlertOrchestrationDispatchMode,
  type AlertOrchestrationEventType,
  type AlertOrchestrationRule,
  type AlertSeverity,
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
  if (updatedAlert.status === "acknowledged" && updatedAlert.severity === "critical") {
    const frozenBudget = await repository.freezeBudget(tenantId, updatedAlert.budgetId, {
      reason: "critical 告警已确认，预算已冻结。",
      alertId: alertId,
    });
    updatedBudgetGovernanceState = frozenBudget?.governanceState;
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
    },
  });

  return c.json(updatedAlert);
});
