import { Hono, type Context } from "hono";
import {
  validateAddTenantMemberInput,
  validateCreateAgentInput,
  validateCreateDeviceInput,
  validateCreateOrganizationInput,
  validateCreateSourceBindingInput,
  validateCreateTenantInput,
  validateDeleteAgentInput,
  validateDeleteDeviceInput,
  validateDeleteSourceBindingInput,
} from "../contracts";
import {
  getControlPlaneRepository,
  type AgentBinding,
  type DeviceBinding,
  type SourceBinding,
  type TenantMember,
} from "../data/repository";
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function hasMismatchedBodyTenant(body: unknown, tenantId: string): boolean {
  return (
    isRecord(body) &&
    typeof body.tenantId === "string" &&
    body.tenantId.trim().length > 0 &&
    body.tenantId.trim() !== tenantId
  );
}

function resolveBodyTenantFieldError(c: Context<AppEnv>, tenantId: string, body: unknown): Response | null {
  if (hasMismatchedBodyTenant(body, tenantId)) {
    return c.json({ message: "请求体 tenantId 与路径 tenantId 不一致。" }, 400);
  }
  return null;
}

function toBooleanWithDefault(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  return fallback;
}

function buildIdentityMetadata(base: Record<string, unknown>): Record<string, unknown> {
  const metadata: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(base)) {
    if (value !== undefined && value !== null && value !== "") {
      metadata[key] = value;
    }
  }
  return metadata;
}

function toDeviceItem(binding: DeviceBinding) {
  const metadata = isRecord(binding.metadata) ? binding.metadata : {};
  const hostname = normalizeString(metadata.hostname) ?? binding.displayName ?? binding.deviceId;
  const fingerprint = normalizeString(metadata.fingerprint) ?? binding.deviceId;
  return {
    id: binding.deviceId,
    deviceId: binding.deviceId,
    tenantId: binding.tenantId,
    organizationId: normalizeString(metadata.organizationId),
    userId: normalizeString(metadata.userId) ?? "",
    hostname,
    fingerprint,
    platform: normalizeString(metadata.platform),
    active: toBooleanWithDefault(metadata.active, true),
    createdAt: binding.createdAt,
    updatedAt: binding.updatedAt,
  };
}

function toAgentItem(binding: AgentBinding) {
  const metadata = isRecord(binding.metadata) ? binding.metadata : {};
  const hostname = normalizeString(metadata.hostname) ?? binding.displayName ?? binding.agentId;
  return {
    id: binding.agentId,
    agentId: binding.agentId,
    tenantId: binding.tenantId,
    organizationId: normalizeString(metadata.organizationId),
    userId: normalizeString(metadata.userId),
    deviceId: binding.deviceId ?? "",
    hostname,
    version: normalizeString(metadata.version),
    active: toBooleanWithDefault(metadata.active, true),
    createdAt: binding.createdAt,
    updatedAt: binding.updatedAt,
  };
}

function toSourceBindingItem(binding: SourceBinding) {
  const metadata = isRecord(binding.metadata) ? binding.metadata : {};
  return {
    id: binding.id,
    bindingId: binding.id,
    tenantId: binding.tenantId,
    organizationId: normalizeString(metadata.organizationId),
    userId: normalizeString(metadata.userId),
    sourceId: binding.sourceId,
    deviceId: binding.deviceId ?? undefined,
    agentId: binding.agentId ?? undefined,
    method: binding.bindingType,
    accessMode: binding.accessMode,
    active: toBooleanWithDefault(metadata.active, true),
    createdAt: binding.createdAt,
    updatedAt: binding.updatedAt,
  };
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

identityRoutes.get("/tenants/:tenantId/devices", async (c) => {
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

  const bindings = await repository.listDeviceBindings(tenantId);
  const items = bindings.map((binding) => toDeviceItem(binding));
  return c.json({
    items,
    total: items.length,
  });
});

identityRoutes.post("/tenants/:tenantId/devices", async (c) => {
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
  const tenantFieldError = resolveBodyTenantFieldError(c, tenantId, body);
  if (tenantFieldError) {
    return tenantFieldError;
  }

  const payload = isRecord(body) ? body : {};
  const validateResult = validateCreateDeviceInput({
    tenantId,
    organizationId: normalizeString(payload.organizationId),
    userId: normalizeString(payload.userId) ?? actorUserId,
    hostname:
      normalizeString(payload.hostname) ??
      normalizeString(payload.name) ??
      normalizeString(payload.deviceId) ??
      normalizeString(payload.id),
    fingerprint:
      normalizeString(payload.fingerprint) ??
      normalizeString(payload.deviceId) ??
      normalizeString(payload.id) ??
      normalizeString(payload.slug) ??
      normalizeString(payload.hostname) ??
      normalizeString(payload.name),
    platform: normalizeString(payload.platform),
  });
  if (!validateResult.success) {
    return c.json({ message: validateResult.error }, 400);
  }

  const deviceId =
    normalizeString(payload.deviceId) ??
    normalizeString(payload.id) ??
    validateResult.data.fingerprint;
  const displayName =
    normalizeString(payload.name) ?? validateResult.data.hostname ?? validateResult.data.fingerprint;
  const metadata = buildIdentityMetadata({
    organizationId: validateResult.data.organizationId,
    userId: validateResult.data.userId,
    hostname: validateResult.data.hostname,
    fingerprint: validateResult.data.fingerprint,
    platform: validateResult.data.platform,
    slug: normalizeString(payload.slug),
    active: true,
  });

  try {
    const binding = await repository.createDeviceBinding(tenantId, {
      deviceId,
      displayName,
      metadata,
    });
    return c.json(toDeviceItem(binding), 201);
  } catch (error) {
    if (
      error instanceof Error &&
      error.message.startsWith("device_binding_already_exists:")
    ) {
      return c.json({ message: `设备已存在：${deviceId}` }, 409);
    }
    return c.json({ message: "设备绑定创建失败：参数非法或租户不存在。" }, 400);
  }
});

identityRoutes.delete("/tenants/:tenantId/devices/:deviceId", async (c) => {
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

  const validateResult = validateDeleteDeviceInput({
    tenantId,
    deviceId: c.req.param("deviceId"),
  });
  if (!validateResult.success) {
    return c.json({ message: validateResult.error }, 400);
  }

  const deviceId = validateResult.data.deviceId;
  const [agentBindings, sourceBindings] = await Promise.all([
    repository.listAgentBindings(tenantId),
    repository.listSourceBindings(tenantId),
  ]);
  if (agentBindings.some((binding) => binding.deviceId === deviceId)) {
    return c.json({ message: `设备 ${deviceId} 正在被 agent 引用，无法删除。` }, 409);
  }
  if (sourceBindings.some((binding) => binding.deviceId === deviceId)) {
    return c.json({ message: `设备 ${deviceId} 正在被 source-binding 引用，无法删除。` }, 409);
  }

  const deleted = await repository.deleteDeviceBinding(tenantId, deviceId);
  if (!deleted) {
    return c.json({ message: `未找到设备 ${deviceId}。` }, 404);
  }
  return c.body(null, 204);
});

identityRoutes.get("/tenants/:tenantId/agents", async (c) => {
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

  const bindings = await repository.listAgentBindings(tenantId);
  const items = bindings.map((binding) => toAgentItem(binding));
  return c.json({
    items,
    total: items.length,
  });
});

identityRoutes.post("/tenants/:tenantId/agents", async (c) => {
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
  const tenantFieldError = resolveBodyTenantFieldError(c, tenantId, body);
  if (tenantFieldError) {
    return tenantFieldError;
  }

  const payload = isRecord(body) ? body : {};
  const validateResult = validateCreateAgentInput({
    tenantId,
    organizationId: normalizeString(payload.organizationId),
    userId: normalizeString(payload.userId),
    deviceId: normalizeString(payload.deviceId),
    hostname:
      normalizeString(payload.hostname) ??
      normalizeString(payload.name) ??
      normalizeString(payload.agentId) ??
      normalizeString(payload.id),
    version: normalizeString(payload.version),
  });
  if (!validateResult.success) {
    return c.json({ message: validateResult.error }, 400);
  }

  const agentId =
    normalizeString(payload.agentId) ??
    normalizeString(payload.id) ??
    normalizeString(payload.slug) ??
    validateResult.data.hostname;
  const displayName =
    normalizeString(payload.name) ?? validateResult.data.hostname ?? validateResult.data.deviceId;
  const metadata = buildIdentityMetadata({
    organizationId: validateResult.data.organizationId,
    userId: validateResult.data.userId,
    hostname: validateResult.data.hostname,
    version: validateResult.data.version,
    slug: normalizeString(payload.slug),
    active: true,
  });

  try {
    const binding = await repository.createAgentBinding(tenantId, {
      agentId,
      deviceId: validateResult.data.deviceId,
      displayName,
      metadata,
    });
    return c.json(toAgentItem(binding), 201);
  } catch (error) {
    if (
      error instanceof Error &&
      error.message.startsWith("agent_binding_already_exists:")
    ) {
      return c.json({ message: `agent 已存在：${agentId}` }, 409);
    }
    if (
      error instanceof Error &&
      error.message.startsWith("agent_binding_device_not_found:")
    ) {
      return c.json({ message: `设备 ${validateResult.data.deviceId} 不存在。` }, 400);
    }
    return c.json({ message: "agent 绑定创建失败：参数非法或租户不存在。" }, 400);
  }
});

identityRoutes.delete("/tenants/:tenantId/agents/:agentId", async (c) => {
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

  const validateResult = validateDeleteAgentInput({
    tenantId,
    agentId: c.req.param("agentId"),
  });
  if (!validateResult.success) {
    return c.json({ message: validateResult.error }, 400);
  }

  const agentId = validateResult.data.agentId;
  const sourceBindings = await repository.listSourceBindings(tenantId);
  if (sourceBindings.some((binding) => binding.agentId === agentId)) {
    return c.json({ message: `agent ${agentId} 正在被 source-binding 引用，无法删除。` }, 409);
  }

  const deleted = await repository.deleteAgentBinding(tenantId, agentId);
  if (!deleted) {
    return c.json({ message: `未找到 agent ${agentId}。` }, 404);
  }
  return c.body(null, 204);
});

identityRoutes.get("/tenants/:tenantId/source-bindings", async (c) => {
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

  const bindings = await repository.listSourceBindings(tenantId);
  const items = bindings.map((binding) => toSourceBindingItem(binding));
  return c.json({
    items,
    total: items.length,
  });
});

identityRoutes.post("/tenants/:tenantId/source-bindings", async (c) => {
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
  const tenantFieldError = resolveBodyTenantFieldError(c, tenantId, body);
  if (tenantFieldError) {
    return tenantFieldError;
  }

  const payload = isRecord(body) ? body : {};
  const sourceId = normalizeString(payload.sourceId) ?? normalizeString(payload.source);
  const deviceId = normalizeString(payload.deviceId);
  const agentId = normalizeString(payload.agentId);
  const method =
    normalizeString(payload.method) ??
    (agentId ? "agent-push" : "ssh-pull");
  const accessMode = normalizeString(payload.accessMode) ?? "realtime";
  const validateResult = validateCreateSourceBindingInput({
    tenantId,
    organizationId: normalizeString(payload.organizationId),
    userId: normalizeString(payload.userId),
    sourceId,
    deviceId,
    agentId,
    method,
    accessMode,
  });
  if (!validateResult.success) {
    return c.json({ message: validateResult.error }, 400);
  }

  const metadata = buildIdentityMetadata({
    organizationId: validateResult.data.organizationId,
    userId: validateResult.data.userId,
    active: true,
  });
  try {
    const binding = await repository.createSourceBinding(tenantId, {
      sourceId: validateResult.data.sourceId,
      deviceId: validateResult.data.deviceId,
      agentId: validateResult.data.agentId,
      bindingType: validateResult.data.method,
      accessMode: validateResult.data.accessMode,
      metadata,
    });
    return c.json(toSourceBindingItem(binding), 201);
  } catch (error) {
    if (
      error instanceof Error &&
      error.message.startsWith("source_binding_already_exists:")
    ) {
      return c.json({ message: `source-binding 已存在：${validateResult.data.sourceId}` }, 409);
    }
    if (
      error instanceof Error &&
      error.message.startsWith("source_binding_source_not_found:")
    ) {
      return c.json({ message: `source ${validateResult.data.sourceId} 不存在。` }, 400);
    }
    if (
      error instanceof Error &&
      error.message.startsWith("source_binding_device_not_found:")
    ) {
      return c.json({ message: `设备 ${validateResult.data.deviceId} 不存在。` }, 400);
    }
    if (
      error instanceof Error &&
      error.message.startsWith("source_binding_agent_not_found:")
    ) {
      return c.json({ message: `agent ${validateResult.data.agentId} 不存在。` }, 400);
    }
    return c.json({ message: "source-binding 创建失败：参数非法或租户不存在。" }, 400);
  }
});

identityRoutes.delete("/tenants/:tenantId/source-bindings/:bindingId", async (c) => {
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

  const validateResult = validateDeleteSourceBindingInput({
    tenantId,
    bindingId: c.req.param("bindingId"),
  });
  if (!validateResult.success) {
    return c.json({ message: validateResult.error }, 400);
  }

  const bindingId = validateResult.data.bindingId;
  const deleted = await repository.deleteSourceBinding(tenantId, bindingId);
  if (!deleted) {
    return c.json({ message: `未找到 source-binding ${bindingId}。` }, 404);
  }
  return c.body(null, 204);
});
