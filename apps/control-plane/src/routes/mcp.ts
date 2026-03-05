import { Hono, type Context } from "hono";
import {
  validateMcpApprovalCreateInput,
  validateMcpEvaluateInput,
  validateMcpInvocationCreateInput,
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

  const evaluatedAt = new Date().toISOString();
  const { toolId, reason, approvalRequestId: inputApprovalRequestId } = validation.data;
  const policy = await resolveToolPolicy(auth.tenantId, toolId);
  const decision = policy.decision;

  let approvalRequestId: string | undefined;
  let result: "allowed" | "blocked" | "approved";

  switch (decision) {
    case "allow":
      result = "allowed";
      break;
    case "deny":
      result = "blocked";
      break;
    default: {
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
        result = current.status === "approved" ? "approved" : "blocked";
        break;
      }
      const created = await repository.createMcpApprovalRequest(
        auth.tenantId,
        { toolId, reason },
        {
          requestedByUserId: auth.userId,
          requestedByEmail: auth.email,
        }
      );
      approvalRequestId = created.id;
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
    },
  });

  return c.json({
    toolId,
    decision,
    result,
    approvalRequestId,
    enforced: true,
    evaluatedDecision: decision,
    policy,
    invocation,
    evaluatedAt,
  });
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
