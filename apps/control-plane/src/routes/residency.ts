import { Hono, type Context } from "hono";
import {
  validateReplicationJobCancelInput,
  validateReplicationJobCreateInput,
  validateReplicationJobListInput,
  validateTenantResidencyPolicyUpsertInput,
} from "../contracts";
import type { AppendAuditLogInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const residencyRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const WRITABLE_ROLES = new Set(["owner", "maintainer"]);

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 residency 审计日志失败。", error);
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

residencyRoutes.get("/residency/regions", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const items = await repository.listResidencyRegions();
  return c.json({
    items,
    total: items.length,
  });
});

residencyRoutes.get("/residency/policy", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const policy = await repository.getTenantResidencyPolicy(auth.tenantId);
  if (!policy) {
    return c.json({ message: "当前租户尚未配置数据主权策略。" }, 404);
  }
  return c.json(policy);
});

residencyRoutes.put("/residency/policy", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = typeof body === "object" && body !== null ? body : {};
  const result = validateTenantResidencyPolicyUpsertInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
    updatedAt:
      typeof (bodyRecord as Record<string, unknown>).updatedAt === "string"
        ? (bodyRecord as Record<string, unknown>).updatedAt
        : new Date().toISOString(),
  });
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const policy = await repository.upsertTenantResidencyPolicy(auth.tenantId, result.data);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.residency.policy_upsert",
    level: "info",
    detail: "Updated tenant residency policy.",
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      mode: policy.mode,
      primaryRegion: policy.primaryRegion,
      replicaRegionCount: policy.replicaRegions.length,
      allowCrossRegionTransfer: policy.allowCrossRegionTransfer,
      requireTransferApproval: policy.requireTransferApproval,
    },
  });
  return c.json(policy);
});

residencyRoutes.get("/residency/replication-jobs", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateReplicationJobListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const payload = await repository.listReplicationJobs(auth.tenantId, result.data);
  return c.json({
    items: payload.items,
    total: payload.total,
    filters: result.data,
  });
});

residencyRoutes.post("/residency/replication-jobs", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = typeof body === "object" && body !== null ? body : {};
  const result = validateReplicationJobCreateInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
  });
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const job = await repository.createReplicationJob(auth.tenantId, result.data, {
    createdByUserId: auth.userId,
  });
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.residency.replication_job_created",
    level: "info",
    detail: `Created replication job ${job.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      resourceId: job.id,
      sourceRegion: job.sourceRegion,
      targetRegion: job.targetRegion,
      status: job.status,
    },
  });
  return c.json(job, 201);
});

residencyRoutes.post("/residency/replication-jobs/:id/cancel", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const jobId = c.req.param("id")?.trim();
  if (!jobId) {
    return c.json({ message: "jobId 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateReplicationJobCancelInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const current = await repository.getReplicationJobById(auth.tenantId, jobId);
  if (!current) {
    return c.json({ message: `未找到复制任务 ${jobId}。` }, 404);
  }
  if (current.status !== "pending" && current.status !== "running") {
    return c.json({ message: `复制任务 ${jobId} 当前状态为 ${current.status}，无法取消。` }, 409);
  }

  const job = await repository.cancelReplicationJob(auth.tenantId, jobId, result.data, {
    userId: auth.userId,
  });
  if (!job) {
    return c.json({ message: `未找到复制任务 ${jobId}。` }, 404);
  }
  if (job.status !== "cancelled") {
    return c.json({ message: `复制任务 ${jobId} 当前状态为 ${job.status}，无法取消。` }, 409);
  }
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.residency.replication_job_cancelled",
    level: "warning",
    detail: `Cancelled replication job ${job.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      resourceId: job.id,
      status: job.status,
      reason: job.reason,
    },
  });
  return c.json(job);
});
