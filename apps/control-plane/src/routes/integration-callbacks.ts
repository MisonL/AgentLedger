import { Hono } from "hono";
import { createHash, timingSafeEqual } from "node:crypto";
import { validateIntegrationAlertCallbackInput } from "../contracts";
import type { AppendAuditLogInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import type { AppEnv } from "../types";

export const integrationCallbackRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const CALLBACK_SECRET_HEADER = "x-integration-callback-secret";
type CallbackResponseStatus = 200 | 400 | 401 | 404 | 409 | 500;

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 integration callback 审计日志失败。", error);
  }
}

function resolveTenantId(value: string | undefined): string {
  const normalized = value?.trim();
  return normalized && normalized.length > 0 ? normalized : "default";
}

function resolveIntegrationCallbackSecret(): string | null {
  const configured = Bun.env.INTEGRATION_CALLBACK_SECRET?.trim();
  return configured && configured.length > 0 ? configured : null;
}

function toSecretDigest(value: string): Buffer {
  return createHash("sha256").update(value).digest();
}

function isIntegrationCallbackSecretValid(expected: string, received: string | undefined): boolean {
  const provided = received?.trim() ?? "";
  return timingSafeEqual(toSecretDigest(expected), toSecretDigest(provided));
}

integrationCallbackRoutes.post("/integrations/callbacks/alerts", async (c) => {
  const configuredSecret = resolveIntegrationCallbackSecret();
  if (!configuredSecret) {
    console.error("[control-plane] integration callback secret 未配置，拒绝请求。");
    return c.json({ message: "服务未配置 integration callback secret。" }, 500);
  }

  if (
    !isIntegrationCallbackSecretValid(
      configuredSecret,
      c.req.header(CALLBACK_SECRET_HEADER)
    )
  ) {
    return c.json({ message: "未授权：callback secret 无效。" }, 401);
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateIntegrationAlertCallbackInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const tenantId = resolveTenantId(result.data.tenantId);
  const callbackId = result.data.callbackId;
  const action = result.data.action;

  const claimResult = await repository.claimIntegrationAlertCallback({
    callbackId,
    tenantId,
    action,
  });
  if (!claimResult.claimed) {
    return c.json({
      callbackId,
      action: claimResult.record.action,
      duplicate: true,
      result: claimResult.record.response,
    });
  }

  const persistAndRespond = async (
    status: CallbackResponseStatus,
    payload: Record<string, unknown>,
    callbackResult: Record<string, unknown>,
    options?: {
      writeAudit?: boolean;
    }
  ) => {
    await repository.saveIntegrationAlertCallback({
      callbackId,
      tenantId,
      action,
      response: callbackResult,
      processedAt: new Date().toISOString(),
    });

    if (options?.writeAudit) {
      const requestId = c.get("requestId");
      await appendAuditLogSafely({
        tenantId,
        eventId: `cp:${requestId}`,
        action: "control_plane.integration_alert_callback_handled",
        level: "info",
        detail: `Handled integration callback ${callbackId} with action ${action}.`,
        metadata: {
          requestId,
          tenantId,
          callbackId,
          action,
          duplicate: false,
        },
      });
    }

    return c.json(payload, status);
  };

  const respondError = async (
    status: Exclude<CallbackResponseStatus, 200>,
    message: string
  ) => {
    return persistAndRespond(
      status,
      { message },
      {
        error: {
          message,
        },
        status,
      }
    );
  };

  try {
    let callbackResult: Record<string, unknown>;

    if (action === "ack") {
      const alertId = result.data.alertId as string;
      const alert = await repository.getAlertById(tenantId, alertId);
      if (!alert) {
        return respondError(404, `未找到告警 ${alertId}。`);
      }
      const updatedAlert = await repository.updateAlertStatus(tenantId, alertId, "acknowledged");
      if (!updatedAlert) {
        return respondError(404, `未找到告警 ${alertId}。`);
      }
      callbackResult = {
        alert: updatedAlert,
      };
      if (updatedAlert.status === "acknowledged") {
        const budget = await repository.freezeBudget(tenantId, updatedAlert.budgetId, {
          reason: result.data.reason ?? "集成回调确认告警，预算已冻结。",
          alertId,
        });
        callbackResult.budget = budget;
      }
    } else if (action === "resolve") {
      const alertId = result.data.alertId as string;
      const alert = await repository.getAlertById(tenantId, alertId);
      if (!alert) {
        return respondError(404, `未找到告警 ${alertId}。`);
      }
      const updatedAlert = await repository.updateAlertStatus(tenantId, alertId, "resolved");
      if (!updatedAlert) {
        return respondError(404, `未找到告警 ${alertId}。`);
      }
      callbackResult = {
        alert: updatedAlert,
      };
    } else if (action === "request_release") {
      const budgetId = result.data.budgetId as string;
      const actorUserId = result.data.actorUserId as string;
      const budget = await repository.getBudgetById(tenantId, budgetId);
      if (!budget) {
        return respondError(404, `未找到预算 ${budgetId}。`);
      }
      if (budget.governanceState === "active") {
        return respondError(409, "预算当前未冻结，不能发起解冻申请。");
      }
      if (budget.governanceState === "pending_release") {
        return respondError(409, "预算已有待处理解冻申请，请勿重复发起。");
      }

      const releaseRequest = await repository.createBudgetReleaseRequest(
        tenantId,
        budgetId,
        {
          userId: actorUserId,
          email: result.data.actorEmail,
        },
        {
          reason: result.data.reason,
        }
      );
      if (!releaseRequest) {
        const latestBudget = await repository.getBudgetById(tenantId, budgetId);
        if (!latestBudget) {
          return respondError(404, `未找到预算 ${budgetId}。`);
        }
        if (latestBudget.governanceState === "active") {
          return respondError(409, "预算当前未冻结，不能发起解冻申请。");
        }
        return respondError(409, "预算已有待处理解冻申请，请勿重复发起。");
      }

      callbackResult = {
        releaseRequest,
      };
    } else if (action === "approve_release") {
      const budgetId = result.data.budgetId as string;
      const requestId = result.data.requestId as string;
      const actorUserId = result.data.actorUserId as string;

      const currentRequest = await repository.getBudgetReleaseRequestById(
        tenantId,
        budgetId,
        requestId
      );
      if (!currentRequest) {
        return respondError(404, `未找到释放申请 ${requestId}。`);
      }
      if (currentRequest.status === "rejected") {
        return respondError(409, "该释放申请已驳回，不能继续审批。");
      }
      if (currentRequest.status === "executed") {
        return respondError(409, "该释放申请已执行完成，请勿重复审批。");
      }
      if (currentRequest.approvals.some((approval) => approval.userId === actorUserId)) {
        return respondError(400, "同一用户不能完成两次审批。");
      }

      const releaseRequest = await repository.approveBudgetReleaseRequest(
        tenantId,
        budgetId,
        requestId,
        {
          userId: actorUserId,
          email: result.data.actorEmail,
        }
      );
      if (!releaseRequest) {
        return respondError(404, `未找到释放申请 ${requestId}。`);
      }

      callbackResult = {
        releaseRequest,
      };
    } else {
      const budgetId = result.data.budgetId as string;
      const requestId = result.data.requestId as string;
      const actorUserId = result.data.actorUserId as string;

      const currentRequest = await repository.getBudgetReleaseRequestById(
        tenantId,
        budgetId,
        requestId
      );
      if (!currentRequest) {
        return respondError(404, `未找到释放申请 ${requestId}。`);
      }
      if (currentRequest.status === "rejected") {
        return respondError(409, "该释放申请已驳回，请勿重复操作。");
      }
      if (currentRequest.status === "executed") {
        return respondError(409, "该释放申请已执行完成，不能再驳回。");
      }

      const releaseRequest = await repository.rejectBudgetReleaseRequest(
        tenantId,
        budgetId,
        requestId,
        {
          userId: actorUserId,
          email: result.data.actorEmail,
        },
        {
          reason: result.data.reason,
        }
      );
      if (!releaseRequest) {
        return respondError(404, `未找到释放申请 ${requestId}。`);
      }

      callbackResult = {
        releaseRequest,
      };
    }

    return persistAndRespond(
      200,
      {
        callbackId,
        action,
        duplicate: false,
        result: callbackResult,
      },
      callbackResult,
      {
        writeAudit: true,
      }
    );
  } catch (error) {
    console.error("[control-plane] integration callback 处理异常。", {
      callbackId,
      action,
      tenantId,
      error,
    });
    return persistAndRespond(
      500,
      { message: "integration callback 处理失败。" },
      {
        error: {
          message: "integration callback 处理失败。",
        },
        status: 500,
      }
    );
  }
});
