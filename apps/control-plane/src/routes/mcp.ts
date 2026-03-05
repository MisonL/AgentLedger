import { Hono, type Context } from "hono";
import {
  validateMcpApprovalCreateInput,
  validateMcpApprovalReviewInput,
  validateMcpInvocationListInput,
  validateMcpToolPolicyListInput,
  validateMcpToolPolicyUpsertInput,
} from "../contracts";
import type { AppendAuditLogInput, ListMcpApprovalRequestsInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const mcpRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const WRITABLE_ROLES = new Set(["owner", "maintainer"]);

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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseInvocationCreateBody(body: unknown): {
  success: true;
  data: {
    toolId: string;
    decision: "allow" | "deny" | "require_approval";
    result: "allowed" | "blocked" | "approved";
    approvalRequestId?: string;
    metadata?: Record<string, unknown>;
  };
} | {
  success: false;
  error: string;
} {
  if (!isRecord(body)) {
    return { success: false, error: "请求体必须是对象。" };
  }
  const toolId =
    typeof body.toolId === "string" && body.toolId.trim().length > 0
      ? body.toolId.trim()
      : undefined;
  if (!toolId) {
    return { success: false, error: "toolId 必须为非空字符串。" };
  }
  const decisionRaw =
    typeof body.decision === "string" ? body.decision.trim() : "require_approval";
  const decision =
    decisionRaw === "allow" || decisionRaw === "deny" || decisionRaw === "require_approval"
      ? decisionRaw
      : undefined;
  if (!decision) {
    return { success: false, error: "decision 必须是 allow/deny/require_approval 之一。" };
  }
  const resultRaw = typeof body.result === "string" ? body.result.trim() : "allowed";
  const result =
    resultRaw === "allowed" || resultRaw === "blocked" || resultRaw === "approved"
      ? resultRaw
      : undefined;
  if (!result) {
    return { success: false, error: "result 必须是 allowed/blocked/approved 之一。" };
  }
  const approvalRequestId =
    typeof body.approvalRequestId === "string" && body.approvalRequestId.trim().length > 0
      ? body.approvalRequestId.trim()
      : undefined;
  const metadata = isRecord(body.metadata) ? body.metadata : undefined;
  return {
    success: true,
    data: {
      toolId,
      decision,
      result,
      approvalRequestId,
      metadata,
    },
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
  return c.json({
    items: payload.items,
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
  const approval = await repository.createMcpApprovalRequest(auth.tenantId, result.data, {
    requestedByUserId: auth.userId,
    requestedByEmail: auth.email,
  });
  return c.json(approval, 201);
});

mcpRoutes.post("/mcp/approvals/:id/approve", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const approvalId = c.req.param("id")?.trim();
  if (!approvalId) {
    return c.json({ message: "approvalId 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const result = validateMcpApprovalReviewInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const current = await repository.getMcpApprovalRequestById(auth.tenantId, approvalId);
  if (!current) {
    return c.json({ message: `未找到审批请求 ${approvalId}。` }, 404);
  }
  if (current.status !== "pending") {
    return c.json({ message: `审批请求 ${approvalId} 当前状态为 ${current.status}，无法重复审批。` }, 409);
  }
  const approval = await repository.reviewMcpApprovalRequest(
    auth.tenantId,
    approvalId,
    "approved",
    result.data,
    {
      reviewedByUserId: auth.userId,
      reviewedByEmail: auth.email,
    }
  );
  if (!approval) {
    return c.json({ message: `未找到审批请求 ${approvalId}。` }, 404);
  }
  if (approval.status !== "approved") {
    return c.json({ message: `审批请求 ${approvalId} 当前状态为 ${approval.status}，无法重复审批。` }, 409);
  }
  return c.json(approval);
});

mcpRoutes.post("/mcp/approvals/:id/reject", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const approvalId = c.req.param("id")?.trim();
  if (!approvalId) {
    return c.json({ message: "approvalId 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const result = validateMcpApprovalReviewInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const current = await repository.getMcpApprovalRequestById(auth.tenantId, approvalId);
  if (!current) {
    return c.json({ message: `未找到审批请求 ${approvalId}。` }, 404);
  }
  if (current.status !== "pending") {
    return c.json({ message: `审批请求 ${approvalId} 当前状态为 ${current.status}，无法驳回。` }, 409);
  }
  const approval = await repository.reviewMcpApprovalRequest(
    auth.tenantId,
    approvalId,
    "rejected",
    result.data,
    {
      reviewedByUserId: auth.userId,
      reviewedByEmail: auth.email,
    }
  );
  if (!approval) {
    return c.json({ message: `未找到审批请求 ${approvalId}。` }, 404);
  }
  if (approval.status !== "rejected") {
    return c.json({ message: `审批请求 ${approvalId} 当前状态为 ${approval.status}，无法驳回。` }, 409);
  }
  return c.json(approval);
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
  const parsed = parseInvocationCreateBody(body);
  if (!parsed.success) {
    return c.json({ message: parsed.error }, 400);
  }
  const invocation = await repository.appendMcpInvocationAudit(auth.tenantId, parsed.data);
  return c.json(invocation, 201);
});
