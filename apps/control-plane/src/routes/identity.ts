import { Hono, type Context } from "hono";
import {
  validateAddTenantMemberInput,
  validateCreateOrganizationInput,
  validateCreateTenantInput,
} from "../contracts";
import { getControlPlaneRepository, type TenantMember } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const identityRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const WRITABLE_ROLES = new Set(["owner", "maintainer"]);

interface TenantMemberView extends TenantMember {
  email: string;
  displayName: string;
}

function resolveTenantId(rawTenantId: string | undefined): string | null {
  const tenantId = rawTenantId?.trim();
  return tenantId && tenantId.length > 0 ? tenantId : null;
}

function unauthorized(c: Context<AppEnv>) {
  return c.json({ message: "未认证：请先登录。" }, 401);
}

function forbiddenCrossTenant(c: Context<AppEnv>) {
  return c.json({ message: "无权访问该租户。" }, 403);
}

function forbiddenWrite(c: Context<AppEnv>) {
  return c.json({ message: "无写入权限：仅 owner/maintainer 可执行写操作。" }, 403);
}

function getActorUserId(c: Context<AppEnv>): string | Response {
  const auth = c.get("auth");
  if (!auth) {
    return unauthorized(c);
  }
  return auth.userId;
}

async function requireTenantAccess(
  c: Context<AppEnv>,
  tenantId: string,
  actorUserId: string,
  mode: "read" | "write"
): Promise<TenantMember | Response> {
  const membership = await repository.getTenantMemberByUser(tenantId, actorUserId);
  if (!membership) {
    return forbiddenCrossTenant(c);
  }
  if (mode === "write" && !WRITABLE_ROLES.has(membership.tenantRole)) {
    return forbiddenWrite(c);
  }
  return membership;
}

async function enrichTenantMember(member: TenantMember): Promise<TenantMemberView> {
  const user = await repository.getUserById(member.userId);
  return {
    ...member,
    email: user?.email ?? "",
    displayName: user?.displayName ?? member.userId,
  };
}

async function listOrganizationsHandler(c: Context<AppEnv>) {
  const actorUserId = getActorUserId(c);
  if (actorUserId instanceof Response) {
    return actorUserId;
  }

  const tenantId = resolveTenantId(c.req.param("tenantId"));
  if (!tenantId) {
    return c.json({ message: "tenantId 必须为非空字符串。" }, 400);
  }

  const access = await requireTenantAccess(c, tenantId, actorUserId, "read");
  if (access instanceof Response) {
    return access;
  }

  const items = await repository.listOrganizations(tenantId);
  return c.json({
    items,
    total: items.length,
  });
}

async function createOrganizationHandler(c: Context<AppEnv>) {
  const actorUserId = getActorUserId(c);
  if (actorUserId instanceof Response) {
    return actorUserId;
  }

  const tenantId = resolveTenantId(c.req.param("tenantId"));
  if (!tenantId) {
    return c.json({ message: "tenantId 必须为非空字符串。" }, 400);
  }

  const access = await requireTenantAccess(c, tenantId, actorUserId, "write");
  if (access instanceof Response) {
    return access;
  }

  const body = await c.req.json().catch(() => undefined);
  if (
    body &&
    typeof body === "object" &&
    "tenantId" in body &&
    typeof body.tenantId === "string" &&
    body.tenantId.trim().length > 0 &&
    body.tenantId.trim() !== tenantId
  ) {
    return c.json({ message: "请求体 tenantId 与路径 tenantId 不一致。" }, 400);
  }

  const result = validateCreateOrganizationInput({
    ...(typeof body === "object" && body !== null ? body : {}),
    tenantId,
  });
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  try {
    const organization = await repository.createOrganization(tenantId, {
      name: result.data.name,
    });
    return c.json(organization, 201);
  } catch {
    return c.json({ message: "组织创建失败：租户或参数无效。" }, 400);
  }
}

identityRoutes.use("*", authMiddleware);

identityRoutes.get("/tenants", async (c) => {
  const actorUserId = getActorUserId(c);
  if (actorUserId instanceof Response) {
    return actorUserId;
  }

  const tenants = await repository.listTenants();
  const memberships = await Promise.all(
    tenants.map((tenant) => repository.getTenantMemberByUser(tenant.id, actorUserId))
  );
  const items = tenants.filter((_, index) => memberships[index] !== null);

  return c.json({
    items,
    total: items.length,
  });
});

identityRoutes.post("/tenants", async (c) => {
  const actorUserId = getActorUserId(c);
  if (actorUserId instanceof Response) {
    return actorUserId;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateCreateTenantInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const tenantId = result.data.slug;
  const existingTenants = await repository.listTenants();
  if (existingTenants.some((item) => item.id === tenantId)) {
    return c.json({ message: `租户 slug 已存在：${tenantId}` }, 409);
  }

  let tenant;
  try {
    tenant = await repository.createTenant({
      id: tenantId,
      name: result.data.name,
    });
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("tenant_already_exists:")) {
      return c.json({ message: `租户 slug 已存在：${tenantId}` }, 409);
    }
    throw error;
  }

  try {
    await repository.addTenantMember({
      tenantId: tenant.id,
      userId: actorUserId,
      tenantRole: "owner",
    });
  } catch (error) {
    console.warn("[control-plane] 自动写入租户创建者成员关系失败。", error);
  }

  return c.json(tenant, 201);
});

identityRoutes.get("/tenants/:tenantId/orgs", listOrganizationsHandler);
identityRoutes.get("/tenants/:tenantId/organizations", listOrganizationsHandler);

identityRoutes.post("/tenants/:tenantId/orgs", createOrganizationHandler);
identityRoutes.post("/tenants/:tenantId/organizations", createOrganizationHandler);

identityRoutes.get("/tenants/:tenantId/members", async (c) => {
  const actorUserId = getActorUserId(c);
  if (actorUserId instanceof Response) {
    return actorUserId;
  }

  const tenantId = resolveTenantId(c.req.param("tenantId"));
  if (!tenantId) {
    return c.json({ message: "tenantId 必须为非空字符串。" }, 400);
  }

  const access = await requireTenantAccess(c, tenantId, actorUserId, "read");
  if (access instanceof Response) {
    return access;
  }

  const members = await repository.listTenantMembers(tenantId);
  const items = await Promise.all(members.map((member) => enrichTenantMember(member)));
  return c.json({
    items,
    total: items.length,
  });
});

identityRoutes.post("/tenants/:tenantId/members", async (c) => {
  const actorUserId = getActorUserId(c);
  if (actorUserId instanceof Response) {
    return actorUserId;
  }

  const tenantId = resolveTenantId(c.req.param("tenantId"));
  if (!tenantId) {
    return c.json({ message: "tenantId 必须为非空字符串。" }, 400);
  }

  const access = await requireTenantAccess(c, tenantId, actorUserId, "write");
  if (access instanceof Response) {
    return access;
  }

  const body = await c.req.json().catch(() => undefined);
  if (
    body &&
    typeof body === "object" &&
    "tenantId" in body &&
    typeof body.tenantId === "string" &&
    body.tenantId.trim().length > 0 &&
    body.tenantId.trim() !== tenantId
  ) {
    return c.json({ message: "请求体 tenantId 与路径 tenantId 不一致。" }, 400);
  }

  const result = validateAddTenantMemberInput({
    ...(typeof body === "object" && body !== null ? body : {}),
    tenantId,
  });
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  let targetUserId = result.data.userId;
  if (!targetUserId && result.data.email) {
    const user = await repository.getLocalUserByEmail(result.data.email);
    if (!user) {
      return c.json({ message: `用户 ${result.data.email} 不存在。` }, 404);
    }
    targetUserId = user.id;
  }
  if (!targetUserId) {
    return c.json({ message: "userId 与 email 不能同时为空，至少提供一个。" }, 400);
  }

  try {
    const member = await repository.addTenantMember({
      tenantId,
      userId: targetUserId,
      tenantRole: result.data.tenantRole,
      organizationId: result.data.organizationId,
      orgRole: result.data.orgRole,
    });
    const enriched = await enrichTenantMember(member);
    return c.json(enriched, 201);
  } catch {
    return c.json({ message: "成员添加失败：用户、租户或组织不存在。" }, 400);
  }
});
