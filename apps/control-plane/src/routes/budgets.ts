import { Hono, type Context } from "hono";
import {
  type BudgetReleaseRequestStatus,
  validateBudgetUpsertInput,
  validateCreateBudgetReleaseRequestInput,
  validateRejectBudgetReleaseRequestInput,
} from "../contracts";
import type { AppendAuditLogInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const budgetRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const DEFAULT_RELEASE_REQUEST_LIMIT = 50;
const MAX_RELEASE_REQUEST_LIMIT = 200;

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 budget 审计日志失败。", error);
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

function parseBooleanQuery(value: string | undefined): boolean | undefined | "invalid" {
  if (value === undefined) {
    return undefined;
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === "true" || normalized === "1") {
    return true;
  }
  if (normalized === "false" || normalized === "0") {
    return false;
  }
  return "invalid";
}

function parseReleaseRequestStatusQuery(
  value: string | undefined
): BudgetReleaseRequestStatus | undefined | "invalid" {
  if (value === undefined) {
    return undefined;
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === "pending" || normalized === "rejected" || normalized === "executed") {
    return normalized;
  }
  return "invalid";
}

function parseReleaseRequestLimitQuery(
  value: string | undefined
): { success: true; limit: number } | { success: false; error: string } {
  if (value === undefined) {
    return { success: true, limit: DEFAULT_RELEASE_REQUEST_LIMIT };
  }
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return { success: false, error: "limit 必须是 1 到 200 的整数。" };
  }
  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_RELEASE_REQUEST_LIMIT) {
    return { success: false, error: "limit 必须是 1 到 200 的整数。" };
  }
  return { success: true, limit: parsed };
}

budgetRoutes.get("/budgets", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const tenantId = auth.tenantId;
  const scope = c.req.query("scope");
  const sourceId = c.req.query("sourceId")?.trim();
  const organizationId = c.req.query("organizationId")?.trim();
  const userId = c.req.query("userId")?.trim();
  const model = c.req.query("model")?.trim();
  const period = c.req.query("period");
  const enabled = parseBooleanQuery(c.req.query("enabled"));
  const governanceState = c.req.query("governanceState");

  if (
    scope !== undefined &&
    scope !== "global" &&
    scope !== "source" &&
    scope !== "org" &&
    scope !== "user" &&
    scope !== "model"
  ) {
    return c.json({ message: "scope 必须是 global/source/org/user/model 之一。" }, 400);
  }
  if (period !== undefined && period !== "daily" && period !== "monthly") {
    return c.json({ message: "period 必须是 daily/monthly 之一。" }, 400);
  }
  if (
    governanceState !== undefined &&
    governanceState !== "active" &&
    governanceState !== "frozen" &&
    governanceState !== "pending_release"
  ) {
    return c.json({ message: "governanceState 必须是 active/frozen/pending_release 之一。" }, 400);
  }
  if (enabled === "invalid") {
    return c.json({ message: "enabled 必须是 true/false 或 1/0。" }, 400);
  }

  const items = (await repository.listBudgets(tenantId)).filter((item) => {
    if (scope && item.scope !== scope) {
      return false;
    }
    if (sourceId !== undefined && sourceId.length > 0 && (item.sourceId ?? "") !== sourceId) {
      return false;
    }
    if (
      organizationId !== undefined &&
      organizationId.length > 0 &&
      (item.organizationId ?? "") !== organizationId
    ) {
      return false;
    }
    if (userId !== undefined && userId.length > 0 && (item.userId ?? "") !== userId) {
      return false;
    }
    if (model !== undefined && model.length > 0 && (item.model ?? "") !== model) {
      return false;
    }
    if (period && item.period !== period) {
      return false;
    }
    if (governanceState && item.governanceState !== governanceState) {
      return false;
    }
    if (enabled !== undefined) {
      if (item.enabled !== enabled) {
        return false;
      }
    }
    return true;
  });

  return c.json({
    items,
    total: items.length,
  });
});

budgetRoutes.put("/budgets", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateBudgetUpsertInput(body);

  if (!result.success) {
    return c.json(
      {
        message: result.error,
      },
      400
    );
  }

  const tenantId = auth.tenantId;
  const bindingError = await repository.validateBudgetScopeBinding(tenantId, result.data);
  if (bindingError) {
    return c.json({ message: bindingError.message }, 400);
  }
  const budget = await repository.upsertBudget(tenantId, result.data);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.budget_upserted",
    level: "info",
    detail: `Upserted budget ${budget.id}.`,
    metadata: {
      requestId,
      tenantId,
      resourceId: budget.id,
      scope: budget.scope,
      sourceId: budget.sourceId,
      organizationId: budget.organizationId,
      userId: budget.userId,
      model: budget.model,
      period: budget.period,
      tokenLimit: budget.tokenLimit,
      costLimit: budget.costLimit,
      alertThreshold: budget.alertThreshold,
      thresholds: budget.thresholds,
      enabled: budget.enabled,
      governanceState: budget.governanceState,
      freezeReason: budget.freezeReason,
      frozenAt: budget.frozenAt,
      frozenByAlertId: budget.frozenByAlertId,
    },
  });

  return c.json(budget);
});

budgetRoutes.get("/budgets/:id/release-requests", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const budgetId = c.req.param("id")?.trim();
  if (!budgetId) {
    return c.json({ message: "budgetId 必须为非空字符串。" }, 400);
  }

  const status = parseReleaseRequestStatusQuery(c.req.query("status"));
  if (status === "invalid") {
    return c.json({ message: "status 必须是 pending/rejected/executed 之一。" }, 400);
  }
  const limitResult = parseReleaseRequestLimitQuery(c.req.query("limit"));
  if (!limitResult.success) {
    return c.json({ message: limitResult.error }, 400);
  }

  const tenantId = auth.tenantId;
  const budget = await repository.getBudgetById(tenantId, budgetId);
  if (!budget) {
    return c.json({ message: `未找到预算 ${budgetId}。` }, 404);
  }

  const items = await repository.listBudgetReleaseRequests(tenantId, budgetId, {
    status,
    limit: limitResult.limit,
  });
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.budget_release_requests_listed",
    level: "info",
    detail: `Listed budget release requests for budget ${budgetId}.`,
    metadata: {
      requestId,
      tenantId,
      budgetId,
      status: status ?? null,
      limit: limitResult.limit,
      total: items.length,
    },
  });

  return c.json({
    items,
    total: items.length,
    status: status ?? null,
    limit: limitResult.limit,
  });
});

budgetRoutes.post("/budgets/:id/release-requests", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const budgetId = c.req.param("id")?.trim();
  if (!budgetId) {
    return c.json({ message: "budgetId 必须为非空字符串。" }, 400);
  }

  const body = (await c.req.json().catch(() => ({}))) ?? {};
  const bodyResult = validateCreateBudgetReleaseRequestInput(body);
  if (!bodyResult.success) {
    return c.json({ message: bodyResult.error }, 400);
  }

  const tenantId = auth.tenantId;
  const budget = await repository.getBudgetById(tenantId, budgetId);
  if (!budget) {
    return c.json({ message: `未找到预算 ${budgetId}。` }, 404);
  }
  if (budget.governanceState === "active") {
    return c.json({ message: "预算当前未冻结，不能发起解冻申请。" }, 409);
  }
  if (budget.governanceState === "pending_release") {
    return c.json({ message: "预算已有待处理解冻申请，请勿重复发起。" }, 409);
  }

  const releaseRequest = await repository.createBudgetReleaseRequest(
    tenantId,
    budgetId,
    {
      userId: auth.userId,
      email: auth.email,
    },
    {
      reason: bodyResult.data.reason,
    }
  );
  if (!releaseRequest) {
    return c.json({ message: `未找到预算 ${budgetId}。` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.budget_release_requested",
    level: "warning",
    detail: `Created budget release request ${releaseRequest.id} for budget ${budgetId}.`,
    metadata: {
      requestId,
      tenantId,
      budgetId,
      releaseRequestId: releaseRequest.id,
      requestedByUserId: auth.userId,
      requestedByEmail: auth.email,
      reason: bodyResult.data.reason,
    },
  });

  return c.json(releaseRequest, 201);
});

budgetRoutes.post("/budgets/:id/release-requests/:requestId/approve", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const budgetId = c.req.param("id")?.trim();
  const releaseRequestId = c.req.param("requestId")?.trim();
  if (!budgetId) {
    return c.json({ message: "budgetId 必须为非空字符串。" }, 400);
  }
  if (!releaseRequestId) {
    return c.json({ message: "requestId 必须为非空字符串。" }, 400);
  }

  const tenantId = auth.tenantId;
  const currentRequest = await repository.getBudgetReleaseRequestById(
    tenantId,
    budgetId,
    releaseRequestId
  );
  if (!currentRequest) {
    return c.json({ message: `未找到释放申请 ${releaseRequestId}。` }, 404);
  }
  if (currentRequest.status === "rejected") {
    return c.json({ message: "该释放申请已驳回，不能继续审批。" }, 409);
  }
  if (currentRequest.status === "executed") {
    return c.json({ message: "该释放申请已执行完成，请勿重复审批。" }, 409);
  }
  if (currentRequest.requestedByUserId === auth.userId) {
    return c.json({ message: "申请人与审批人必须为不同用户。" }, 400);
  }
  if (currentRequest.approvals.some((approval) => approval.userId === auth.userId)) {
    return c.json({ message: "同一用户不能完成两次审批。" }, 400);
  }

  const updatedRequest = await repository.approveBudgetReleaseRequest(
    tenantId,
    budgetId,
    releaseRequestId,
    {
      userId: auth.userId,
      email: auth.email,
    }
  );
  if (!updatedRequest) {
    return c.json({ message: `未找到释放申请 ${releaseRequestId}。` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.budget_release_approved",
    level: updatedRequest.status === "executed" ? "warning" : "info",
    detail: `Approved release request ${releaseRequestId} for budget ${budgetId}.`,
    metadata: {
      requestId,
      tenantId,
      budgetId,
      releaseRequestId,
      approvedByUserId: auth.userId,
      approvedByEmail: auth.email,
      approvalCount: updatedRequest.approvals.length,
      status: updatedRequest.status,
    },
  });

  return c.json(updatedRequest);
});

budgetRoutes.post("/budgets/:id/release-requests/:requestId/reject", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const budgetId = c.req.param("id")?.trim();
  const releaseRequestId = c.req.param("requestId")?.trim();
  if (!budgetId) {
    return c.json({ message: "budgetId 必须为非空字符串。" }, 400);
  }
  if (!releaseRequestId) {
    return c.json({ message: "requestId 必须为非空字符串。" }, 400);
  }

  const body = (await c.req.json().catch(() => ({}))) ?? {};
  const bodyResult = validateRejectBudgetReleaseRequestInput(body);
  if (!bodyResult.success) {
    return c.json({ message: bodyResult.error }, 400);
  }

  const tenantId = auth.tenantId;
  const currentRequest = await repository.getBudgetReleaseRequestById(
    tenantId,
    budgetId,
    releaseRequestId
  );
  if (!currentRequest) {
    return c.json({ message: `未找到释放申请 ${releaseRequestId}。` }, 404);
  }
  if (currentRequest.status === "rejected") {
    return c.json({ message: "该释放申请已驳回，请勿重复操作。" }, 409);
  }
  if (currentRequest.status === "executed") {
    return c.json({ message: "该释放申请已执行完成，不能再驳回。" }, 409);
  }

  const updatedRequest = await repository.rejectBudgetReleaseRequest(
    tenantId,
    budgetId,
    releaseRequestId,
    {
      userId: auth.userId,
      email: auth.email,
    },
    {
      reason: bodyResult.data.reason,
    }
  );
  if (!updatedRequest) {
    return c.json({ message: `未找到释放申请 ${releaseRequestId}。` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.budget_release_rejected",
    level: "warning",
    detail: `Rejected release request ${releaseRequestId} for budget ${budgetId}.`,
    metadata: {
      requestId,
      tenantId,
      budgetId,
      releaseRequestId,
      rejectedByUserId: auth.userId,
      rejectedByEmail: auth.email,
      reason: bodyResult.data.reason,
    },
  });

  return c.json(updatedRequest);
});
