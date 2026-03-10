import { Hono, type Context } from "hono";
import {
  type TenantRole,
  validateMcpApprovalCreateInput,
  validateMcpEvaluateInput,
  validateMcpInvocationCreateInput,
  validateMcpApprovalReviewInput,
  validateMcpInvocationListInput,
  validateMcpToolPolicyListInput,
  validateMcpToolPolicyUpsertInput,
} from "../contracts";
import type {
  AppendAuditLogInput,
  ListMcpApprovalRequestsInput,
  LocalMcpApprovalConfig,
  LocalMcpApprovalStage,
  LocalMcpApprovalStageSnapshot,
  LocalMcpApprovalWorkflowSnapshot,
} from "../data/repository";
import type {
  McpApprovalWorkflow,
  McpApprovalWorkflowCondition,
  McpApprovalWorkflowTimeWindow,
  McpApprovalWorkflowNode,
  McpApprovalWorkflowNodeKind,
  McpApprovalWorkflowTransition,
} from "../contracts";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import { continueQualityAdviceExecutionFromApproval } from "./quality-advice-execution";
import type { AppEnv } from "../types";

export const mcpRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const WRITABLE_ROLES = new Set(["owner", "maintainer"]);
const TENANT_ROLE_VALUES: TenantRole[] = [
  "owner",
  "maintainer",
  "member",
  "readonly",
];
const MCP_RISK_LEVEL_VALUES = ["low", "medium", "high"] as const;

type ParsedApprovalStageConfig = {
  nodeId: string;
  stage: LocalMcpApprovalStage;
  label?: string;
  requiredApprovals: number;
  roles: TenantRole[];
};

type ParsedApprovalWorkflowNode = McpApprovalWorkflowNode;
type ParsedApprovalWorkflowTransition = McpApprovalWorkflowTransition;

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 MCP 审计日志失败。", error);
  }
}

function unauthorized(c: Context<AppEnv>) {
  return c.json({ message: "未认证：请先登录。" }, 401);
}

function forbidden(c: Context<AppEnv>, mode: "read" | "write") {
  if (mode === "write") {
    return c.json({ message: "无写入权限：仅 owner/maintainer 可执行写操作。" }, 403);
  }
  return c.json({ message: "无权访问该租户资源。" }, 403);
}

async function requireAuthContext(c: Context<AppEnv>) {
  const authResult = await authMiddleware(c, async () => {});
  if (authResult instanceof Response) {
    return authResult;
  }
  const auth = c.get("auth");
  if (!auth) {
    return unauthorized(c);
  }
  return auth;
}

async function requireTenantAccess(c: Context<AppEnv>, mode: "read" | "write") {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }
  const membership = await repository.getTenantMemberByUser(auth.tenantId, auth.userId);
  if (!membership) {
    return forbidden(c, mode);
  }
  if (mode === "write" && !WRITABLE_ROLES.has(membership.tenantRole)) {
    return forbidden(c, mode);
  }
  return auth;
}

async function requireTenantWriteMembership(c: Context<AppEnv>) {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }
  const membership = await repository.getTenantMemberByUser(auth.tenantId, auth.userId);
  if (!membership) {
    return forbidden(c, "write");
  }
  if (!WRITABLE_ROLES.has(membership.tenantRole)) {
    return forbidden(c, "write");
  }
  return {
    auth,
    membership,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function matchesMcpApprovalCondition(
  condition: Record<string, unknown> | undefined,
  context: {
    toolId: string;
    riskLevel: "low" | "medium" | "high";
    tenantRole?: TenantRole;
  },
): boolean {
  if (!condition) {
    return true;
  }
  const riskLevelAtLeast =
    typeof condition.riskLevelAtLeast === "string"
      ? condition.riskLevelAtLeast.trim()
      : "";
  if (
    riskLevelAtLeast &&
    MCP_RISK_LEVEL_VALUES.includes(riskLevelAtLeast as (typeof MCP_RISK_LEVEL_VALUES)[number]) &&
    MCP_RISK_LEVEL_VALUES.indexOf(context.riskLevel) <
      MCP_RISK_LEVEL_VALUES.indexOf(
        riskLevelAtLeast as (typeof MCP_RISK_LEVEL_VALUES)[number],
      )
  ) {
    return false;
  }

  if (Array.isArray(condition.toolIds) && condition.toolIds.length > 0) {
    const toolIds = condition.toolIds.filter(
      (item): item is string => typeof item === "string" && item.trim().length > 0,
    );
    if (toolIds.length > 0 && !toolIds.includes(context.toolId)) {
      return false;
    }
  }

  if (Array.isArray(condition.tenantRoles) && condition.tenantRoles.length > 0) {
    const tenantRoles = condition.tenantRoles.filter(
      (item): item is TenantRole =>
        typeof item === "string" && TENANT_ROLE_VALUES.includes(item as TenantRole),
    );
    if (
      tenantRoles.length > 0 &&
      (!context.tenantRole || !tenantRoles.includes(context.tenantRole))
    ) {
      return false;
    }
  }

  return true;
}

function isApprovalStage(value: unknown): value is LocalMcpApprovalStage {
  return typeof value === "string" && /^stage[1-9]\d*$/.test(value.trim());
}

function normalizeApprovalStageName(
  value: unknown,
  index: number,
): LocalMcpApprovalStage | undefined {
  if (isApprovalStage(value)) {
    return value.trim() as LocalMcpApprovalStage;
  }
  if (value === undefined || value === null || value === "") {
    return `stage${index + 1}` as LocalMcpApprovalStage;
  }
  return undefined;
}

function defaultApprovalStageRoles(stage: LocalMcpApprovalStage): TenantRole[] {
  return stage === "stage1" ? ["owner", "maintainer"] : ["owner"];
}

function parseApprovalStageConfig(
  value: unknown,
  stage: LocalMcpApprovalStage
): {
  success: true;
  data: ParsedApprovalStageConfig;
} | {
  success: false;
  error: string;
} {
  const defaultRoles = defaultApprovalStageRoles(stage);
  const defaultNodeId = stage;
  if (value === undefined) {
    return {
      success: true,
      data: {
        nodeId: defaultNodeId,
        stage,
        requiredApprovals: 1,
        roles: defaultRoles,
      },
    };
  }
  if (!isRecord(value)) {
    return {
      success: false,
      error: `${stage} 必须是对象。`,
    };
  }
  const nodeId = typeof value.nodeId === "string" && value.nodeId.trim().length > 0
    ? value.nodeId.trim()
    : defaultNodeId;
  const label = typeof value.label === "string" && value.label.trim().length > 0
    ? value.label.trim()
    : undefined;
  const requiredApprovalsRaw = value.requiredApprovals;
  let requiredApprovals = 1;
  if (requiredApprovalsRaw !== undefined) {
    const parsed = Number(requiredApprovalsRaw);
    if (!Number.isInteger(parsed) || parsed < 1 || parsed > 5) {
      return {
        success: false,
        error: `${stage}.requiredApprovals 必须是 1 到 5 的整数。`,
      };
    }
    requiredApprovals = parsed;
  }
  let roles = defaultRoles;
  if (value.roles !== undefined) {
    if (!Array.isArray(value.roles) || value.roles.length === 0) {
      return {
        success: false,
        error: `${stage}.roles 必须是非空数组。`,
      };
    }
    const normalizedRoles = Array.from(
      new Set(
        value.roles
          .filter((item): item is string => typeof item === "string")
          .map((item) => item.trim())
          .filter((item): item is TenantRole =>
            TENANT_ROLE_VALUES.includes(item as TenantRole)
          )
      )
    );
    if (normalizedRoles.length === 0) {
      return {
        success: false,
        error: `${stage}.roles 仅支持 owner/maintainer/member/readonly。`,
      };
    }
    roles = normalizedRoles;
  }
  return {
    success: true,
    data: {
      nodeId,
      stage,
      label,
      requiredApprovals,
      roles,
    },
  };
}

function parseApprovalStagesArray(
  value: unknown
): {
  success: true;
  data?: ParsedApprovalStageConfig[];
} | {
  success: false;
  error: string;
} {
  if (value === undefined) {
    return {
      success: true,
      data: undefined,
    };
  }
  if (!Array.isArray(value) || value.length === 0) {
    return {
      success: false,
      error: "approvalStages 必须是非空数组。",
    };
  }

  const seen = new Set<string>();
  const stages: ParsedApprovalStageConfig[] = [];
  for (const [index, item] of value.entries()) {
    if (!isRecord(item)) {
      return {
        success: false,
        error: `approvalStages[${index}] 必须是对象。`,
      };
    }
    const stage =
      typeof item.stage === "string" && item.stage.trim().length > 0
        ? item.stage.trim()
        : `stage${index + 1}`;
    if (!isApprovalStage(stage)) {
      return {
        success: false,
        error: `approvalStages[${index}].stage 必须是 stageN 格式。`,
      };
    }
    if (seen.has(stage)) {
      return {
        success: false,
        error: `approvalStages[${index}].stage ${stage} 重复。`,
      };
    }
    seen.add(stage);
    const stageResult = parseApprovalStageConfig(item, stage);
    if (!stageResult.success) {
      return {
        success: false,
        error: `approvalStages[${index}] ${stageResult.error}`,
      };
    }
    stages.push(stageResult.data);
  }
  return {
    success: true,
    data: stages,
  };
}

function isWorkflowNodeKind(value: unknown): value is McpApprovalWorkflowNodeKind {
  return value === "approval" || value === "terminal_approved" || value === "terminal_rejected";
}

function isSupportedTimeZone(value: string): boolean {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: value }).format(new Date());
    return true;
  } catch {
    return false;
  }
}

function parseWorkflowTimeWindow(
  value: unknown,
): { success: true; data?: McpApprovalWorkflowTimeWindow } | { success: false; error: string } {
  if (value === undefined || value === null) {
    return { success: true, data: undefined };
  }
  if (!isRecord(value)) {
    return { success: false, error: "condition.timeWindow 必须是对象。" };
  }
  const timezone =
    typeof value.timezone === "string" && value.timezone.trim().length > 0
      ? value.timezone.trim()
      : null;
  const startTime =
    typeof value.startTime === "string" && value.startTime.trim().length > 0
      ? value.startTime.trim()
      : null;
  const endTime =
    typeof value.endTime === "string" && value.endTime.trim().length > 0
      ? value.endTime.trim()
      : null;
  if (!timezone || !isSupportedTimeZone(timezone)) {
    return { success: false, error: "condition.timeWindow.timezone 必须是合法的 IANA 时区。" };
  }
  if (!startTime || !/^\d{2}:\d{2}$/.test(startTime)) {
    return { success: false, error: "condition.timeWindow.startTime 必须是 HH:mm 格式。" };
  }
  if (!endTime || !/^\d{2}:\d{2}$/.test(endTime)) {
    return { success: false, error: "condition.timeWindow.endTime 必须是 HH:mm 格式。" };
  }
  const [startHour, startMinute] = startTime.split(":").map(Number);
  const [endHour, endMinute] = endTime.split(":").map(Number);
  if (
    !Number.isInteger(startHour) ||
    !Number.isInteger(startMinute) ||
    startHour < 0 ||
    startHour > 23 ||
    startMinute < 0 ||
    startMinute > 59 ||
    !Number.isInteger(endHour) ||
    !Number.isInteger(endMinute) ||
    endHour < 0 ||
    endHour > 23 ||
    endMinute < 0 ||
    endMinute > 59
  ) {
    return { success: false, error: "condition.timeWindow 的时间范围不合法。" };
  }
  if (value.weekdays !== undefined) {
    if (!Array.isArray(value.weekdays)) {
      return { success: false, error: "condition.timeWindow.weekdays 必须是数组。" };
    }
    const weekdays = Array.from(
      new Set(
        value.weekdays
          .map((item) => (typeof item === "number" ? item : Number(item)))
          .filter((item) => Number.isInteger(item) && item >= 1 && item <= 7),
      ),
    );
    if (weekdays.length !== value.weekdays.length) {
      return { success: false, error: "condition.timeWindow.weekdays 仅支持 1-7 的整数且不能重复。" };
    }
    return {
      success: true,
      data: {
        timezone,
        weekdays,
        startTime,
        endTime,
      },
    };
  }
  return {
    success: true,
    data: {
      timezone,
      startTime,
      endTime,
    },
  };
}

function parseWorkflowCondition(
  value: unknown,
): { success: true; data: McpApprovalWorkflowCondition } | { success: false; error: string } {
  if (value === undefined || value === null) {
    return { success: true, data: { default: true } };
  }
  if (!isRecord(value)) {
    return { success: false, error: "condition 必须是对象。" };
  }
  const riskLevelAtLeast =
    typeof value.riskLevelAtLeast === "string" && value.riskLevelAtLeast.trim().length > 0
      ? value.riskLevelAtLeast.trim()
      : undefined;
  const toolIds = Array.isArray(value.toolIds)
    ? Array.from(
        new Set(
          value.toolIds
            .filter((item): item is string => typeof item === "string")
            .map((item) => item.trim())
            .filter((item) => item.length > 0),
        ),
      )
    : undefined;
  const tenantRoles = Array.isArray(value.tenantRoles)
    ? Array.from(
        new Set(
          value.tenantRoles
            .filter((item): item is string => typeof item === "string")
            .map((item) => item.trim())
            .filter((item): item is TenantRole =>
              TENANT_ROLE_VALUES.includes(item as TenantRole),
            ),
        ),
      )
    : undefined;
  const timeWindowResult = parseWorkflowTimeWindow(value.timeWindow);
  const defaultFlag =
    value.default === undefined
      ? false
      : typeof value.default === "boolean"
        ? value.default
        : null;
  if (
    riskLevelAtLeast &&
    riskLevelAtLeast !== "low" &&
    riskLevelAtLeast !== "medium" &&
    riskLevelAtLeast !== "high"
  ) {
    return { success: false, error: "condition.riskLevelAtLeast 必须是 low/medium/high。" };
  }
  if (defaultFlag === null) {
    return { success: false, error: "condition.default 必须是布尔值。" };
  }
  if (!timeWindowResult.success) {
    return timeWindowResult;
  }
  if (
    defaultFlag === true &&
    (riskLevelAtLeast ||
      (toolIds?.length ?? 0) > 0 ||
      (tenantRoles?.length ?? 0) > 0 ||
      timeWindowResult.data !== undefined)
  ) {
    return { success: false, error: "default 条件不能与其他过滤条件同时出现。" };
  }
  if (
    defaultFlag !== true &&
    !riskLevelAtLeast &&
    (toolIds?.length ?? 0) === 0 &&
    (tenantRoles?.length ?? 0) === 0 &&
    timeWindowResult.data === undefined
  ) {
    return { success: true, data: { default: true } };
  }
  return {
    success: true,
    data: {
      ...(riskLevelAtLeast ? { riskLevelAtLeast: riskLevelAtLeast as "low" | "medium" | "high" } : {}),
      ...(toolIds && toolIds.length > 0 ? { toolIds } : {}),
      ...(tenantRoles && tenantRoles.length > 0 ? { tenantRoles } : {}),
      ...(timeWindowResult.data ? { timeWindow: timeWindowResult.data } : {}),
      ...(defaultFlag === true ? { default: true } : {}),
    },
  };
}

function parseApprovalWorkflow(
  value: unknown,
): { success: true; data?: McpApprovalWorkflow } | { success: false; error: string } {
  if (value === undefined || value === null) {
    return { success: true, data: undefined };
  }
  if (!isRecord(value)) {
    return { success: false, error: "approvalWorkflow 必须是对象。" };
  }
  const entryNodeId =
    typeof value.entryNodeId === "string" && value.entryNodeId.trim().length > 0
      ? value.entryNodeId.trim()
      : null;
  if (!entryNodeId || !Array.isArray(value.nodes) || value.nodes.length === 0) {
    return { success: false, error: "approvalWorkflow.entryNodeId 和 nodes 必填。" };
  }
  if (!Array.isArray(value.transitions) || value.transitions.length === 0) {
    return { success: false, error: "approvalWorkflow.transitions 必填且不能为空。" };
  }
  const nodes: ParsedApprovalWorkflowNode[] = [];
  const nodeIds = new Set<string>();
  const stageNames = new Set<string>();
  let approvalIndex = 0;
  let terminalApprovedCount = 0;
  let terminalRejectedCount = 0;
  for (const [index, item] of value.nodes.entries()) {
    if (!isRecord(item)) {
      return { success: false, error: `approvalWorkflow.nodes[${index}] 必须是对象。` };
    }
    const nodeId =
      typeof item.nodeId === "string" && item.nodeId.trim().length > 0
        ? item.nodeId.trim()
        : null;
    const kind =
      typeof item.kind === "string" && item.kind.trim().length > 0
        ? item.kind.trim()
        : null;
    if (!nodeId || !kind || !isWorkflowNodeKind(kind) || nodeIds.has(nodeId)) {
      return { success: false, error: `approvalWorkflow.nodes[${index}] 非法。` };
    }
    nodeIds.add(nodeId);
    const label =
      typeof item.label === "string" && item.label.trim().length > 0
        ? item.label.trim()
        : undefined;
    if (kind === "approval") {
      const stage = normalizeApprovalStageName(item.stage, approvalIndex);
      const requiredApprovals =
        typeof item.requiredApprovals === "number"
          ? item.requiredApprovals
          : Number(item.requiredApprovals);
      const roles = Array.isArray(item.roles)
        ? Array.from(
            new Set(
              item.roles
                .filter((role): role is string => typeof role === "string")
                .map((role) => role.trim())
                .filter((role): role is TenantRole =>
                  TENANT_ROLE_VALUES.includes(role as TenantRole),
                ),
            ),
          )
        : [];
      if (
        !stage ||
        stageNames.has(stage) ||
        !Number.isInteger(requiredApprovals) ||
        requiredApprovals < 1 ||
        roles.length === 0
      ) {
        return { success: false, error: `approvalWorkflow.nodes[${index}] 审批节点非法。` };
      }
      stageNames.add(stage);
      approvalIndex += 1;
      nodes.push({
        nodeId,
        kind,
        label,
        stage,
        requiredApprovals,
        roles,
      });
      continue;
    }
    if (kind === "terminal_approved") {
      terminalApprovedCount += 1;
    } else {
      terminalRejectedCount += 1;
    }
    nodes.push({ nodeId, kind, label });
  }
  if (terminalApprovedCount !== 1 || terminalRejectedCount !== 1) {
    return { success: false, error: "approvalWorkflow 必须且只能包含一个通过终态和一个拒绝终态。" };
  }
  if (!nodeIds.has(entryNodeId)) {
    return { success: false, error: "approvalWorkflow.entryNodeId 未命中任何节点。" };
  }
  const entryNode = nodes.find((node) => node.nodeId === entryNodeId);
  if (!entryNode || entryNode.kind !== "approval") {
    return { success: false, error: "approvalWorkflow.entryNodeId 必须指向 approval 节点。" };
  }
  const transitions: ParsedApprovalWorkflowTransition[] = [];
  const outgoing = new Map<string, ParsedApprovalWorkflowTransition[]>();
  for (const [index, item] of value.transitions.entries()) {
    if (!isRecord(item)) {
      return { success: false, error: `approvalWorkflow.transitions[${index}] 必须是对象。` };
    }
    const fromNodeId =
      typeof item.fromNodeId === "string" && item.fromNodeId.trim().length > 0
        ? item.fromNodeId.trim()
        : null;
    const toNodeId =
      typeof item.toNodeId === "string" && item.toNodeId.trim().length > 0
        ? item.toNodeId.trim()
        : null;
    const conditionResult = parseWorkflowCondition(item.condition);
    if (!fromNodeId || !toNodeId || !nodeIds.has(fromNodeId) || !nodeIds.has(toNodeId) || !conditionResult.success) {
      return { success: false, error: `approvalWorkflow.transitions[${index}] 非法。` };
    }
    const fromNode = nodes.find((node) => node.nodeId === fromNodeId);
    if (!fromNode || fromNode.kind !== "approval") {
      return { success: false, error: `approvalWorkflow.transitions[${index}].fromNodeId 必须指向 approval 节点。` };
    }
    const transition = {
      fromNodeId,
      toNodeId,
      condition: conditionResult.data,
    };
    transitions.push(transition);
    outgoing.set(fromNodeId, [...(outgoing.get(fromNodeId) ?? []), transition]);
  }
  for (const node of nodes.filter((item) => item.kind === "approval")) {
    const items = outgoing.get(node.nodeId) ?? [];
    const defaultCount = items.filter((item) => item.condition?.default === true).length;
    if (items.length === 0 || defaultCount !== 1) {
      return { success: false, error: `approvalWorkflow 节点 ${node.nodeId} 必须存在且仅存在一条 default 转移。` };
    }
  }
  const visited = new Set<string>();
  const visiting = new Set<string>();
  const hasCycle = (nodeId: string): boolean => {
    if (visiting.has(nodeId)) {
      return true;
    }
    if (visited.has(nodeId)) {
      return false;
    }
    visiting.add(nodeId);
    for (const transition of outgoing.get(nodeId) ?? []) {
      const nextNode = nodes.find((node) => node.nodeId === transition.toNodeId);
      if (nextNode?.kind === "approval" && hasCycle(nextNode.nodeId)) {
        return true;
      }
    }
    visiting.delete(nodeId);
    visited.add(nodeId);
    return false;
  };
  if (hasCycle(entryNodeId)) {
    return { success: false, error: "approvalWorkflow 不支持循环分支。" };
  }
  return {
    success: true,
    data: {
      entryNodeId,
      nodes,
      transitions,
    },
  };
}

function parseApprovalConfig(body: unknown): {
  success: true;
  data: LocalMcpApprovalConfig;
} | {
  success: false;
  error: string;
} {
  if (!isRecord(body)) {
    return {
      success: true,
      data: {
        mode: "single_stage",
        approvalStages: [
          {
            nodeId: "stage1",
            stage: "stage1",
            label: "Stage 1",
            requiredApprovals: 1,
            roles: ["owner", "maintainer"],
          },
        ],
      },
    };
  }
  const configSource = isRecord(body.approvalConfig) ? body.approvalConfig : body;
  const rawMode = typeof configSource.mode === "string"
    ? configSource.mode.trim()
    : typeof configSource.approvalMode === "string"
      ? configSource.approvalMode.trim()
      : "";
  const approvalWorkflowResult = parseApprovalWorkflow(configSource.approvalWorkflow);
  if (!approvalWorkflowResult.success) {
    return approvalWorkflowResult;
  }
  if (approvalWorkflowResult.data) {
    const approvalNodes = approvalWorkflowResult.data.nodes.filter(
      (item): item is McpApprovalWorkflowNode & {
        kind: "approval";
        stage: LocalMcpApprovalStage;
        requiredApprovals: number;
        roles: TenantRole[];
      } =>
        item.kind === "approval" &&
        isApprovalStage(item.stage) &&
        typeof item.requiredApprovals === "number" &&
        Array.isArray(item.roles),
    );
    const inferredMode =
      approvalNodes.length <= 1
        ? "single_stage"
        : approvalNodes.length === 2
          ? "two_stage"
          : "multi_stage";
    if (
      rawMode &&
      rawMode !== inferredMode
    ) {
      return {
        success: false,
        error: "approvalMode 与 approvalWorkflow 中的审批节点数量不一致。",
      };
    }
    return {
      success: true,
      data: {
        mode: inferredMode,
        approvalStages: approvalNodes.map((item) => ({
          nodeId: item.nodeId,
          stage: item.stage,
          label: item.label,
          requiredApprovals: item.requiredApprovals,
          roles: item.roles,
        })),
        approvalWorkflow: approvalWorkflowResult.data,
      },
    };
  }
  const approvalStagesResult = parseApprovalStagesArray(configSource.approvalStages);
  if (!approvalStagesResult.success) {
    return approvalStagesResult;
  }
  if (approvalStagesResult.data) {
    const inferredMode =
      approvalStagesResult.data.length === 1
        ? "single_stage"
        : approvalStagesResult.data.length === 2
          ? "two_stage"
          : "multi_stage";
    const mode =
      rawMode.length === 0
        ? inferredMode
        : rawMode === "single_stage" ||
            rawMode === "two_stage" ||
            rawMode === "multi_stage"
          ? rawMode
          : null;
    if (!mode) {
      return {
        success: false,
        error: "approvalMode 仅支持 single_stage/two_stage/multi_stage。",
      };
    }
    if (mode !== inferredMode) {
      return {
        success: false,
        error: "approvalMode 与 approvalStages 阶段数量不一致。",
      };
    }
    return {
      success: true,
      data: {
        mode,
        approvalStages: approvalStagesResult.data,
        approvalWorkflow: undefined,
      },
    };
  }

  const mode =
    rawMode.length === 0
      ? "single_stage"
      : rawMode === "single_stage" || rawMode === "two_stage"
        ? rawMode
        : null;
  if (!mode) {
    return {
      success: false,
      error: "approvalMode 仅支持 single_stage/two_stage/multi_stage。",
    };
  }
  const stage1Result = parseApprovalStageConfig(configSource.stage1, "stage1");
  if (!stage1Result.success) {
    return stage1Result;
  }
  if (mode === "two_stage") {
    const stage2Result = parseApprovalStageConfig(configSource.stage2, "stage2");
    if (!stage2Result.success) {
      return stage2Result;
    }
    return {
      success: true,
      data: {
        mode,
        approvalStages: [stage1Result.data, stage2Result.data],
        approvalWorkflow: undefined,
      },
    };
  }
  return {
    success: true,
    data: {
      mode,
      approvalStages: [stage1Result.data],
      approvalWorkflow: undefined,
    },
  };
}

function parseRequestedApprovalStage(body: unknown): {
  success: true;
  data?: LocalMcpApprovalStage;
} | {
  success: false;
  error: string;
} {
  if (!isRecord(body) || body.stage === undefined) {
    return {
      success: true,
      data: undefined,
    };
  }
  if (!isApprovalStage(body.stage)) {
    return {
      success: false,
      error: "stage 仅支持 stageN。",
    };
  }
  return {
    success: true,
    data: body.stage.trim() as LocalMcpApprovalStage,
  };
}

function parseRequestedApprovalNodeId(body: unknown): {
  success: true;
  data?: string;
} | {
  success: false;
  error: string;
} {
  if (!isRecord(body) || body.nodeId === undefined) {
    return {
      success: true,
      data: undefined,
    };
  }
  if (typeof body.nodeId !== "string" || body.nodeId.trim().length === 0) {
    return {
      success: false,
      error: "nodeId 必须为非空字符串。",
    };
  }
  return {
    success: true,
    data: body.nodeId.trim(),
  };
}

function formatApprovalStageSnapshot(
  snapshot: LocalMcpApprovalStageSnapshot
) {
  return {
    nodeId: snapshot.nodeId,
    stage: snapshot.stage,
    label: snapshot.label,
    requiredApprovals: snapshot.requiredApprovals,
    roles: snapshot.roles,
    approvedApprovals: snapshot.approvedApprovals,
    approvedByUserIds: snapshot.approvedByUserIds,
    rejectedByUserId: snapshot.rejectedByUserId,
  };
}

function findApprovalStageSnapshot(
  workflow: LocalMcpApprovalWorkflowSnapshot,
  stage: LocalMcpApprovalStage | null | undefined
) {
  if (!stage) {
    return workflow.approvalStages[0];
  }
  return (
    workflow.approvalStages.find((item) => item.stage === stage) ??
    workflow.approvalStages[0]
  );
}

function withApprovalWorkflow<T>(
  payload: T,
  workflow: LocalMcpApprovalWorkflowSnapshot | null | undefined
): T & Record<string, unknown> {
  if (!workflow) {
    return payload as T & Record<string, unknown>;
  }
  const payloadRecord = payload as T & {
    approvalConditionMatched?: boolean;
  } & Record<string, unknown>;
  const approvalConditionMatched =
    typeof payloadRecord.approvalConditionMatched === "boolean"
      ? payloadRecord.approvalConditionMatched
      : workflow.approvalConditionMatched;
  const approvalStages = workflow.approvalStages.map((item) =>
    formatApprovalStageSnapshot(item)
  );
  const stage1 = approvalStages.find((item) => item.stage === "stage1");
  const stage2 = approvalStages.find((item) => item.stage === "stage2");
  return {
    ...payloadRecord,
    approvalMode: workflow.approvalMode,
    currentNodeId: workflow.currentNodeId,
    currentStage: workflow.currentStage,
    remainingApprovals: workflow.remainingApprovals,
    approvalConditionMatched,
    approvalWorkflow: workflow.approvalWorkflow,
    approvalNodes: workflow.approvalNodes,
    pathHistory: workflow.pathHistory,
    nextTransitionPreview: workflow.nextTransitionPreview,
    approvalStages,
    stage1RequiredApprovals: stage1?.requiredApprovals,
    stage2RequiredApprovals: stage2?.requiredApprovals,
    stage1ApprovedCount: stage1?.approvedApprovals,
    stage2ApprovedCount: stage2?.approvedApprovals,
    stage1Roles: stage1?.roles,
    stage2Roles: stage2?.roles,
  } as T & Record<string, unknown>;
}

function parseApprovalRequestListInput(query: Record<string, string>): {
  success: true;
  data: ListMcpApprovalRequestsInput;
} | {
  success: false;
  error: string;
} {
  const statusRaw = query.status?.trim();
  let status: ListMcpApprovalRequestsInput["status"];
  if (statusRaw) {
    if (statusRaw === "pending" || statusRaw === "approved" || statusRaw === "rejected") {
      status = statusRaw;
    } else {
      return { success: false, error: "status 必须是 pending/approved/rejected 之一。" };
    }
  }
  const limitRaw = query.limit?.trim();
  let limit: number | undefined;
  if (limitRaw) {
    const parsed = Number(limitRaw);
    if (!Number.isInteger(parsed) || parsed < 1 || parsed > 200) {
      return { success: false, error: "limit 必须是 1 到 200 的整数。" };
    }
    limit = parsed;
  }
  return {
    success: true,
    data: {
      status,
      limit,
    },
  };
}

async function resolveToolPolicy(tenantId: string, toolId: string) {
  const policy = await repository.getMcpToolPolicyByToolId(tenantId, toolId);
  if (policy) {
    return policy;
  }
  return {
    tenantId,
    toolId,
    riskLevel: "medium" as const,
    decision: "require_approval" as const,
    updatedAt: new Date().toISOString(),
  };
}

mcpRoutes.get("/mcp/policies", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const result = validateMcpToolPolicyListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const payload = await repository.listMcpToolPolicies(auth.tenantId, result.data);
  return c.json({
    items: payload.items,
    total: payload.total,
    filters: result.data,
  });
});

mcpRoutes.put("/mcp/policies/:toolId", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const toolId = c.req.param("toolId")?.trim();
  if (!toolId) {
    return c.json({ message: "toolId 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = typeof body === "object" && body !== null ? body : {};
  const result = validateMcpToolPolicyUpsertInput({
    ...bodyRecord,
    toolId,
  });
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const policy = await repository.upsertMcpToolPolicy(auth.tenantId, result.data);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.mcp.policy_upsert",
    level: "info",
    detail: `Updated MCP tool policy ${policy.toolId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      resourceId: policy.toolId,
      riskLevel: policy.riskLevel,
      decision: policy.decision,
    },
  });
  return c.json(policy);
});

mcpRoutes.get("/mcp/approvals", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const result = parseApprovalRequestListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const payload = await repository.listMcpApprovalRequests(auth.tenantId, result.data);
  const items = await Promise.all(
    payload.items.map(async (item) =>
      withApprovalWorkflow(
        item,
        await repository.getMcpApprovalWorkflowState(auth.tenantId, item.id, item)
      )
    )
  );
  return c.json({
    items,
    total: payload.total,
    filters: result.data,
  });
});

mcpRoutes.post("/mcp/approvals", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const body = await c.req.json().catch(() => undefined);
  const result = validateMcpApprovalCreateInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const approvalConfigResult = parseApprovalConfig(body);
  if (!approvalConfigResult.success) {
    return c.json({ message: approvalConfigResult.error }, 400);
  }
  const membership = await repository.getTenantMemberByUser(auth.tenantId, auth.userId);
  const policy = await resolveToolPolicy(auth.tenantId, result.data.toolId);
  const approvalConditionMatched = matchesMcpApprovalCondition(
    isRecord(policy.approvalCondition) ? policy.approvalCondition : undefined,
    {
      toolId: result.data.toolId,
      riskLevel: policy.riskLevel,
      tenantRole: membership?.tenantRole,
    },
  );
  const approval = await repository.createMcpApprovalRequest(auth.tenantId, result.data, {
    requestedByUserId: auth.userId,
    requestedByEmail: auth.email,
    requestedByTenantRole: membership?.tenantRole,
    approvalConfig: approvalConfigResult.data,
    approvalConditionMatched,
    riskLevel: policy.riskLevel,
  });
  return c.json(withApprovalWorkflow(approval.approval, approval.workflow), 201);
});

mcpRoutes.post("/mcp/approvals/:id/approve", async (c) => {
  const access = await requireTenantWriteMembership(c);
  if (access instanceof Response) {
    return access;
  }
  const { auth, membership } = access;
  const approvalId = c.req.param("id")?.trim();
  if (!approvalId) {
    return c.json({ message: "approvalId 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const result = validateMcpApprovalReviewInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const requestedStageResult = parseRequestedApprovalStage(body);
  if (!requestedStageResult.success) {
    return c.json({ message: requestedStageResult.error }, 400);
  }
  const requestedNodeIdResult = parseRequestedApprovalNodeId(body);
  if (!requestedNodeIdResult.success) {
    return c.json({ message: requestedNodeIdResult.error }, 400);
  }
  const current = await repository.getMcpApprovalRequestById(auth.tenantId, approvalId);
  if (!current) {
    return c.json({ message: `未找到审批请求 ${approvalId}。` }, 404);
  }
  if (current.status !== "pending") {
    return c.json({ message: `审批请求 ${approvalId} 当前状态为 ${current.status}，无法重复审批。` }, 409);
  }
  const workflow = await repository.getMcpApprovalWorkflowState(
    auth.tenantId,
    approvalId,
    current
  );
  if (!workflow || !workflow.currentStage || !workflow.currentNodeId) {
    return c.json({ message: `审批请求 ${approvalId} 当前无可审批阶段。` }, 409);
  }
  if (
    requestedNodeIdResult.data &&
    requestedNodeIdResult.data !== workflow.currentNodeId
  ) {
    return c.json(
      {
        message: `审批请求 ${approvalId} 当前节点为 ${workflow.currentNodeId}，不允许越节点审批到 ${requestedNodeIdResult.data}。`,
      },
      409,
    );
  }
  if (
    requestedStageResult.data &&
    requestedStageResult.data !== workflow.currentStage
  ) {
    return c.json(
      {
        message: `审批请求 ${approvalId} 当前处于 ${workflow.currentStage}，不允许越阶段审批到 ${requestedStageResult.data}。`,
      },
      409
    );
  }
  const activeStage = findApprovalStageSnapshot(workflow, workflow.currentStage);
  if (!activeStage) {
    return c.json({ message: `审批请求 ${approvalId} 当前无可审批阶段。` }, 409);
  }
  if (!activeStage.roles.includes(membership.tenantRole)) {
    return c.json(
      {
        message: `审批请求 ${approvalId} 当前阶段 ${workflow.currentStage} 仅允许角色 ${activeStage.roles.join(", ")} 审批。`,
      },
      403
    );
  }
  if (activeStage.approvedByUserIds.includes(auth.userId)) {
    return c.json(
      {
        message: `审批请求 ${approvalId} 当前阶段 ${workflow.currentStage} 已记录过当前审批人，无法重复审批。`,
      },
      409
    );
  }
  const approval = await repository.reviewMcpApprovalRequest(
    auth.tenantId,
    approvalId,
    "approved",
    result.data,
    {
      reviewedByUserId: auth.userId,
      reviewedByEmail: auth.email,
      reviewedByTenantRole: membership.tenantRole,
      stage: workflow.currentStage,
      nodeId: workflow.currentNodeId,
    }
  );
  if (!approval) {
    return c.json({ message: `未找到审批请求 ${approvalId}。` }, 404);
  }
  const continuedExecution =
    approval.approval.status === "approved"
      ? await continueQualityAdviceExecutionFromApproval({
          tenantId: auth.tenantId,
          approvalRequestId: approvalId,
          decision: "approved",
          actorUserId: auth.userId,
          actorEmail: auth.email,
        })
      : null;
  return c.json({
    ...withApprovalWorkflow(approval.approval, approval.workflow),
    continuedExecution,
  });
});

mcpRoutes.post("/mcp/approvals/:id/reject", async (c) => {
  const access = await requireTenantWriteMembership(c);
  if (access instanceof Response) {
    return access;
  }
  const { auth, membership } = access;
  const approvalId = c.req.param("id")?.trim();
  if (!approvalId) {
    return c.json({ message: "approvalId 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const result = validateMcpApprovalReviewInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const requestedStageResult = parseRequestedApprovalStage(body);
  if (!requestedStageResult.success) {
    return c.json({ message: requestedStageResult.error }, 400);
  }
  const requestedNodeIdResult = parseRequestedApprovalNodeId(body);
  if (!requestedNodeIdResult.success) {
    return c.json({ message: requestedNodeIdResult.error }, 400);
  }
  const current = await repository.getMcpApprovalRequestById(auth.tenantId, approvalId);
  if (!current) {
    return c.json({ message: `未找到审批请求 ${approvalId}。` }, 404);
  }
  if (current.status !== "pending") {
    return c.json({ message: `审批请求 ${approvalId} 当前状态为 ${current.status}，无法驳回。` }, 409);
  }
  const workflow = await repository.getMcpApprovalWorkflowState(
    auth.tenantId,
    approvalId,
    current
  );
  if (!workflow || !workflow.currentStage || !workflow.currentNodeId) {
    return c.json({ message: `审批请求 ${approvalId} 当前无可驳回阶段。` }, 409);
  }
  if (
    requestedNodeIdResult.data &&
    requestedNodeIdResult.data !== workflow.currentNodeId
  ) {
    return c.json(
      {
        message: `审批请求 ${approvalId} 当前节点为 ${workflow.currentNodeId}，不允许越节点驳回到 ${requestedNodeIdResult.data}。`,
      },
      409,
    );
  }
  if (
    requestedStageResult.data &&
    requestedStageResult.data !== workflow.currentStage
  ) {
    return c.json(
      {
        message: `审批请求 ${approvalId} 当前处于 ${workflow.currentStage}，不允许越阶段驳回到 ${requestedStageResult.data}。`,
      },
      409
    );
  }
  const activeStage = findApprovalStageSnapshot(workflow, workflow.currentStage);
  if (!activeStage) {
    return c.json({ message: `审批请求 ${approvalId} 当前无可驳回阶段。` }, 409);
  }
  if (!activeStage.roles.includes(membership.tenantRole)) {
    return c.json(
      {
        message: `审批请求 ${approvalId} 当前阶段 ${workflow.currentStage} 仅允许角色 ${activeStage.roles.join(", ")} 驳回。`,
      },
      403
    );
  }
  if (activeStage.approvedByUserIds.includes(auth.userId)) {
    return c.json(
      {
        message: `审批请求 ${approvalId} 当前阶段 ${workflow.currentStage} 已记录过当前审批人，无法再执行驳回。`,
      },
      409
    );
  }
  const approval = await repository.reviewMcpApprovalRequest(
    auth.tenantId,
    approvalId,
    "rejected",
    result.data,
    {
      reviewedByUserId: auth.userId,
      reviewedByEmail: auth.email,
      reviewedByTenantRole: membership.tenantRole,
      stage: workflow.currentStage,
      nodeId: workflow.currentNodeId,
    }
  );
  if (!approval) {
    return c.json({ message: `未找到审批请求 ${approvalId}。` }, 404);
  }
  const continuedExecution = await continueQualityAdviceExecutionFromApproval({
    tenantId: auth.tenantId,
    approvalRequestId: approvalId,
    decision: "rejected",
    actorUserId: auth.userId,
    actorEmail: auth.email,
  });
  return c.json({
    ...withApprovalWorkflow(approval.approval, approval.workflow),
    continuedExecution,
  });
});

mcpRoutes.post("/mcp/evaluate", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const body = await c.req.json().catch(() => undefined);
  const validation = validateMcpEvaluateInput(body);
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const evaluatedAt = validation.data.evaluationTimestamp ?? new Date().toISOString();
  const { toolId, reason, approvalRequestId: inputApprovalRequestId } = validation.data;
  const policy = await resolveToolPolicy(auth.tenantId, toolId);
  const membership = await repository.getTenantMemberByUser(auth.tenantId, auth.userId);
  const decision = policy.decision;
  const approvalConditionMatched =
    decision === "require_approval" &&
    matchesMcpApprovalCondition(
      isRecord(policy.approvalCondition) ? policy.approvalCondition : undefined,
      {
        toolId,
        riskLevel: policy.riskLevel,
        tenantRole: membership?.tenantRole,
      },
    );

  let approvalRequestId: string | undefined;
  let result: "allowed" | "blocked" | "approved";
  let approvalWorkflow: LocalMcpApprovalWorkflowSnapshot | null = null;

  switch (decision) {
    case "allow":
      result = "allowed";
      break;
    case "deny":
      result = "blocked";
      break;
    default: {
      if (!approvalConditionMatched) {
        result = "allowed";
        break;
      }
      if (inputApprovalRequestId) {
        const current = await repository.getMcpApprovalRequestById(auth.tenantId, inputApprovalRequestId);
        if (!current) {
          return c.json({ message: `未找到审批请求 ${inputApprovalRequestId}。` }, 404);
        }
        if (current.toolId !== toolId) {
          return c.json(
            {
              message: `审批请求 ${inputApprovalRequestId} 与工具 ${toolId} 不匹配。`,
            },
            409
          );
        }
        approvalRequestId = current.id;
        approvalWorkflow = await repository.getMcpApprovalWorkflowState(
          auth.tenantId,
          current.id,
          current
        );
        result = current.status === "approved" ? "approved" : "blocked";
        break;
      }
      const approvalConfigSource =
        isRecord(body) &&
        (body.approvalConfig !== undefined ||
          body.approvalWorkflow !== undefined ||
          body.approvalStages !== undefined ||
          body.stage1 !== undefined ||
          body.stage2 !== undefined ||
          body.approvalMode !== undefined ||
          body.mode !== undefined)
          ? body
          : {
              approvalConfig: {
                approvalMode: policy.approvalMode,
                approvalWorkflow: policy.approvalWorkflow,
                approvalStages: policy.approvalStages,
                stage1: policy.approvalStages?.[0],
                stage2: policy.approvalStages?.[1],
              },
            };
      const approvalConfigResult = parseApprovalConfig(approvalConfigSource);
      if (!approvalConfigResult.success) {
        return c.json({ message: approvalConfigResult.error }, 400);
      }
      const created = await repository.createMcpApprovalRequest(
        auth.tenantId,
        { toolId, reason },
        {
          requestedByUserId: auth.userId,
          requestedByEmail: auth.email,
          requestedByTenantRole: membership?.tenantRole,
          approvalConfig: approvalConfigResult.data,
          approvalConditionMatched,
          riskLevel: policy.riskLevel,
          evaluationTimestamp: evaluatedAt,
        }
      );
      approvalRequestId = created.approval.id;
      approvalWorkflow = created.workflow;
      result = "blocked";
      break;
    }
  }

  const invocation = await repository.appendMcpInvocationAudit(auth.tenantId, {
    toolId,
    decision,
    result,
    approvalRequestId,
    enforced: true,
    evaluatedDecision: decision,
    metadata: {
      ...(validation.data.metadata ?? {}),
      source: "mcp.evaluate",
      ...(reason ? { evaluateReason: reason } : {}),
      approvalMode: approvalWorkflow?.approvalMode ?? null,
      currentNodeId: approvalWorkflow?.currentNodeId ?? null,
      currentStage: approvalWorkflow?.currentStage ?? null,
      remainingApprovals: approvalWorkflow?.remainingApprovals ?? 0,
      approvalConditionMatched,
    },
    createdAt: evaluatedAt,
  });

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.mcp.evaluate",
    level: result === "blocked" ? "warning" : "info",
    detail: `Evaluated MCP tool ${toolId} with decision ${decision} and result ${result}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      toolId,
      decision,
      result,
      approvalRequestId: approvalRequestId ?? null,
      enforced: true,
      evaluatedAt,
      approvalMode: approvalWorkflow?.approvalMode ?? null,
      currentNodeId: approvalWorkflow?.currentNodeId ?? null,
      currentStage: approvalWorkflow?.currentStage ?? null,
      remainingApprovals: approvalWorkflow?.remainingApprovals ?? 0,
      approvalConditionMatched,
    },
  });

  return c.json(withApprovalWorkflow({
    toolId,
    decision,
    result,
    approvalRequestId,
    enforced: true,
    evaluatedDecision: decision,
    approvalConditionMatched,
    policy,
    invocation,
    evaluatedAt,
  }, approvalWorkflow));
});

mcpRoutes.get("/mcp/invocations", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const result = validateMcpInvocationListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const payload = await repository.listMcpInvocationAudits(auth.tenantId, result.data);
  return c.json({
    items: payload.items,
    total: payload.total,
    filters: result.data,
  });
});

mcpRoutes.post("/mcp/invocations", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const body = await c.req.json().catch(() => undefined);
  const validation = validateMcpInvocationCreateInput(body);
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const {
    toolId,
    approvalRequestId,
    result: rawResult,
    enforced,
    decision: rawDecision,
    evaluatedDecision,
    metadata,
  } = validation.data;
  const decision = rawDecision ?? "require_approval";
  const result = rawResult ?? "allowed";

  if (approvalRequestId) {
    const approval = await repository.getMcpApprovalRequestById(auth.tenantId, approvalRequestId);
    if (!approval) {
      return c.json({ message: `未找到审批请求 ${approvalRequestId}。` }, 404);
    }
    if (approval.toolId !== toolId) {
      return c.json(
        {
          message: `审批请求 ${approvalRequestId} 与工具 ${toolId} 不匹配。`,
        },
        409
      );
    }
    if (result === "approved" && approval.status !== "approved") {
      return c.json(
        {
          message: `审批请求 ${approvalRequestId} 当前状态为 ${approval.status}，无法记录 approved 调用。`,
        },
        409
      );
    }
  } else if (result === "approved") {
    return c.json({ message: "result=approved 时必须提供 approvalRequestId。" }, 400);
  }

  const invocation = await repository.appendMcpInvocationAudit(auth.tenantId, {
    toolId,
    decision,
    result,
    approvalRequestId,
    enforced,
    evaluatedDecision,
    metadata,
  });

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.mcp.invocation_append",
    level: invocation.result === "blocked" ? "warning" : "info",
    detail: `Recorded MCP invocation ${invocation.id} for ${invocation.toolId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      invocationId: invocation.id,
      toolId: invocation.toolId,
      decision: invocation.decision,
      result: invocation.result,
      approvalRequestId: invocation.approvalRequestId ?? null,
      enforced: invocation.enforced,
      evaluatedDecision: invocation.evaluatedDecision ?? null,
    },
  });

  return c.json(invocation, 201);
});
