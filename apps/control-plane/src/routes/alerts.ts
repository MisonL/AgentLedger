import { Hono, type Context } from "hono";
import { validateAlertListInput, validateAlertStatusUpdateInput } from "../contracts";
import type { AppendAuditLogInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const alertRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();

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

  const tenantId = auth.tenantId;
  const items = await repository.listAlerts(tenantId, result.data);

  return c.json({
    items,
    total: items.length,
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
