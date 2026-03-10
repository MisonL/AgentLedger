import { Hono, type Context } from "hono";
import {
  type AgentLifecycleEventItem,
  validateAgentLifecycleEventCreateInput,
  validateAgentLifecycleEventListInput,
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
  type AppendAuditLogInput,
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
const AGENT_LIFECYCLE_EVENT_DEFAULT_LIMIT = 50;
const SCIM_BEARER_TOKEN_ENV = "SCIM_BEARER_TOKEN";
const memoryAgentLifecycleEvents = new Map<string, AgentLifecycleEventItem[]>();
const SCIM_ROLE_SET = new Set(["owner", "maintainer", "member", "readonly"]);

type ScimRole = "owner" | "maintainer" | "member" | "readonly";
type ScimPaginationParseResult =
  | { kind: "ok"; startIndex: number; count?: number }
  | { kind: "error"; error: string };
type ScimEqFilterParseResult =
  | { kind: "none" }
  | { kind: "eq"; attribute: string; value: string }
  | { kind: "error"; error: string };

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

function parsePositiveInt(value: string, options: { min: number }): number | null {
  const normalized = value.trim();
  if (!/^\d+$/.test(normalized)) {
    return null;
  }
  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isFinite(parsed) || parsed < options.min) {
    return null;
  }
  return parsed;
}

function parseScimPagination(query: Record<string, string | undefined>): ScimPaginationParseResult {
  const rawStartIndex = query.startIndex;
  const rawCount = query.count;
  let startIndex = 1;
  if (rawStartIndex !== undefined) {
    const parsed = parsePositiveInt(rawStartIndex, { min: 1 });
    if (parsed === null) {
      return { kind: "error", error: "startIndex 必须为 >= 1 的整数。" };
    }
    startIndex = parsed;
  }
  let count: number | undefined;
  if (rawCount !== undefined) {
    const parsed = parsePositiveInt(rawCount, { min: 0 });
    if (parsed === null) {
      return { kind: "error", error: "count 必须为 >= 0 的整数。" };
    }
    count = parsed;
  }
  return { kind: "ok", startIndex, count };
}

function buildScimEqFilterHelp(allowedAttributes: string[]): string {
  if (allowedAttributes.length === 0) {
    return 'filter 仅支持形如 attribute eq "..." 的表达式。';
  }
  if (allowedAttributes.length === 1) {
    return `filter 仅支持 ${allowedAttributes[0]} eq "..."。`;
  }
  return `filter 仅支持 ${allowedAttributes
    .map((attribute) => `${attribute} eq "..."`)
    .join(" 或 ")}。`;
}

function parseScimEqFilter(
  rawFilter: string | undefined,
  options: { allowedAttributes: string[] },
): ScimEqFilterParseResult {
  const filter = normalizeString(rawFilter);
  if (!filter) {
    return { kind: "none" };
  }
  const match = filter.match(/^\s*([a-zA-Z][a-zA-Z0-9_-]*)\s+eq\s+"([^"]+)"\s*$/i);
  if (!match) {
    return {
      kind: "error",
      error: buildScimEqFilterHelp(options.allowedAttributes),
    };
  }
  const rawAttribute = match[1] ?? "";
  const rawValue = match[2] ?? "";
  const value = rawValue.trim();
  if (!value) {
    return {
      kind: "error",
      error: buildScimEqFilterHelp(options.allowedAttributes),
    };
  }
  const allowed = new Map(
    options.allowedAttributes.map((attribute) => [attribute.toLowerCase(), attribute] as const),
  );
  const attribute = allowed.get(rawAttribute.toLowerCase());
  if (!attribute) {
    return {
      kind: "error",
      error: buildScimEqFilterHelp(options.allowedAttributes),
    };
  }
  return {
    kind: "eq",
    attribute,
    value,
  };
}

function parseScimRole(value: unknown, field: string): { role: ScimRole } | { error: string } {
  const normalized = normalizeString(value);
  if (!normalized) {
    return { error: `${field} 必须为非空字符串。` };
  }
  if (!SCIM_ROLE_SET.has(normalized)) {
    return { error: `${field} 仅支持 owner/maintainer/member/readonly。` };
  }
  return { role: normalized as ScimRole };
}

function resolveScimBearerToken(): string | undefined {
  return normalizeString(Bun.env[SCIM_BEARER_TOKEN_ENV]);
}

function unauthorizedScim(c: Context<AppEnv>) {
  return c.json({ message: "SCIM 未认证：缺少或无效的 Bearer Token。" }, 401);
}

function requireScimBearer(c: Context<AppEnv>) {
  const configured = resolveScimBearerToken();
  if (!configured) {
    return c.json({ message: "服务端未配置 SCIM_BEARER_TOKEN。" }, 503);
  }
  const header = c.req.header("authorization");
  if (!header?.startsWith("Bearer ")) {
    return unauthorizedScim(c);
  }
  const token = header.slice("Bearer ".length).trim();
  if (token !== configured) {
    return unauthorizedScim(c);
  }
  return null;
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

async function appendIdentityAuditLogSafely(
  input: AppendAuditLogInput
): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 identity 审计日志失败。", error);
  }
}

function cloneAgentLifecycleEvent(
  item: AgentLifecycleEventItem
): AgentLifecycleEventItem {
  return {
    ...item,
    metadata: JSON.parse(JSON.stringify(item.metadata)) as Record<string, unknown>,
  };
}

function createAgentLifecycleEventRecord(
  tenantId: string,
  input: {
    agentId: string;
    deviceId?: string;
    hostname?: string;
    version?: string;
    action: AgentLifecycleEventItem["action"];
    result: AgentLifecycleEventItem["result"];
    occurredAt?: string;
    metadata?: Record<string, unknown>;
  }
): AgentLifecycleEventItem {
  const now = new Date().toISOString();
  return {
    id: crypto.randomUUID(),
    tenantId,
    agentId: input.agentId,
    deviceId: input.deviceId,
    hostname: input.hostname,
    version: input.version,
    action: input.action,
    result: input.result,
    occurredAt: input.occurredAt ?? now,
    createdAt: now,
    metadata: input.metadata
      ? (JSON.parse(JSON.stringify(input.metadata)) as Record<string, unknown>)
      : {},
  };
}

function saveAgentLifecycleEvent(
  tenantId: string,
  input: {
    agentId: string;
    deviceId?: string;
    hostname?: string;
    version?: string;
    action: AgentLifecycleEventItem["action"];
    result: AgentLifecycleEventItem["result"];
    occurredAt?: string;
    metadata?: Record<string, unknown>;
  }
): AgentLifecycleEventItem {
  const currentItems = memoryAgentLifecycleEvents.get(tenantId) ?? [];
  const record = createAgentLifecycleEventRecord(tenantId, input);
  memoryAgentLifecycleEvents.set(tenantId, [record, ...currentItems]);
  return cloneAgentLifecycleEvent(record);
}

function listAgentLifecycleEvents(
  tenantId: string,
  input: {
    agentId?: string;
    action?: AgentLifecycleEventItem["action"];
    result?: AgentLifecycleEventItem["result"];
    limit?: number;
  }
) {
  const limit = input.limit ?? AGENT_LIFECYCLE_EVENT_DEFAULT_LIMIT;
  const filtered = (memoryAgentLifecycleEvents.get(tenantId) ?? [])
    .filter((item) => (input.agentId ? item.agentId === input.agentId : true))
    .filter((item) => (input.action ? item.action === input.action : true))
    .filter((item) => (input.result ? item.result === input.result : true))
    .slice()
    .sort(
      (a, b) =>
        b.occurredAt.localeCompare(a.occurredAt) ||
        b.createdAt.localeCompare(a.createdAt) ||
        b.id.localeCompare(a.id)
    );
  return {
    items: filtered.slice(0, limit).map(cloneAgentLifecycleEvent),
    total: filtered.length,
    filters: {
      agentId: input.agentId,
      action: input.action,
      result: input.result,
      limit,
    },
  };
}

function toDeviceItem(binding: DeviceBinding, lastSeenAt?: string) {
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
    lastSeenAt,
    createdAt: binding.createdAt,
    updatedAt: binding.updatedAt,
  };
}

function toAgentItem(binding: AgentBinding, lastSeenAt?: string) {
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
    lastSeenAt,
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

function toScimUserResource(input: {
  userId: string;
  email: string;
  displayName: string;
  tenantRole?: string;
  active?: boolean;
  organizationId?: string;
}) {
  return {
    schemas: ["urn:ietf:params:scim:schemas:core:2.0:User"],
    id: input.userId,
    userName: input.email,
    displayName: input.displayName,
    active: input.active ?? true,
    emails: [
      {
        value: input.email,
        primary: true,
      },
    ],
    roles: input.tenantRole ? [{ value: input.tenantRole }] : [],
    meta: {
      resourceType: "User",
      ...(input.organizationId ? { organizationId: input.organizationId } : {}),
    },
  };
}

function toScimGroupResource(input: {
  organizationId: string;
  name: string;
  members: Array<{ userId: string; email: string }>;
}) {
  return {
    schemas: ["urn:ietf:params:scim:schemas:core:2.0:Group"],
    id: input.organizationId,
    displayName: input.name,
    members: input.members.map((member) => ({
      value: member.userId,
      display: member.email,
    })),
    meta: {
      resourceType: "Group",
    },
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

identityRoutes.get("/tenants/:tenantId/scim/users", async (c) => {
  const scimAuth = requireScimBearer(c);
  if (scimAuth) {
    return scimAuth;
  }
  const tenantId = resolveTenantId(c.req.param("tenantId"));
  if (!tenantId) {
    return c.json({ message: "tenantId 必须为非空字符串。" }, 400);
  }
  const pagination = parseScimPagination(c.req.query());
  if (pagination.kind === "error") {
    return c.json({ message: pagination.error }, 400);
  }
  const filter = parseScimEqFilter(c.req.query().filter, {
    allowedAttributes: ["userName", "email"],
  });
  if (filter.kind === "error") {
    return c.json({ message: filter.error }, 400);
  }
  const normalizedFilterValue = filter.kind === "eq" ? filter.value.toLowerCase() : undefined;
  const memberships = await repository.listTenantMembers(tenantId);
  const resources = await Promise.all(
    memberships.map(async (membership) => {
      const user = await repository.getUserById(membership.userId);
      if (!user) {
        return null;
      }
      if (normalizedFilterValue) {
        const candidate = user.email.toLowerCase();
        if (candidate !== normalizedFilterValue) {
          return null;
        }
      }
      return toScimUserResource({
        userId: user.id,
        email: user.email,
        displayName: user.displayName,
        tenantRole: membership.tenantRole,
        organizationId: membership.organizationId,
      });
    }),
  );
  const items = resources.filter(Boolean);
  const start = Math.max(0, pagination.startIndex - 1);
  const end = pagination.count === undefined ? undefined : start + pagination.count;
  const pageItems = items.slice(start, end);
  return c.json({
    schemas: ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
    totalResults: items.length,
    startIndex: pagination.startIndex,
    itemsPerPage: pageItems.length,
    Resources: pageItems,
  });
});

identityRoutes.post("/tenants/:tenantId/scim/users", async (c) => {
  const scimAuth = requireScimBearer(c);
  if (scimAuth) {
    return scimAuth;
  }
  const tenantId = resolveTenantId(c.req.param("tenantId"));
  if (!tenantId) {
    return c.json({ message: "tenantId 必须为非空字符串。" }, 400);
  }
  const body = (await c.req.json().catch(() => undefined)) as Record<string, unknown> | undefined;
  const userName = normalizeString(body?.userName) ?? normalizeString(body?.email);
  const displayName = normalizeString(body?.displayName) ?? userName;
  const tenantRole = normalizeString(body?.tenantRole) as
    | "owner"
    | "maintainer"
    | "member"
    | "readonly"
    | undefined;
  const organizationId = normalizeString(body?.organizationId);
  const orgRole = normalizeString(body?.orgRole) as
    | "owner"
    | "maintainer"
    | "member"
    | "readonly"
    | undefined;
  if (!userName || !displayName) {
    return c.json({ message: "SCIM userName/email 与 displayName 必填。" }, 400);
  }
  const existingUser = await repository.getLocalUserByEmail(userName);
  const user =
    existingUser ??
    (await repository.createLocalUser({
      email: userName,
      passwordHash: await Bun.password.hash(`scim:${tenantId}:${userName}`),
      displayName,
    }));
  const membership = await repository.addTenantMember({
    tenantId,
    userId: user.id,
    tenantRole: tenantRole ?? "member",
    organizationId,
    orgRole: organizationId ? orgRole ?? "member" : undefined,
  });
  await appendIdentityAuditLogSafely({
    tenantId,
    eventId: `cp:scim:${tenantId}:${user.id}`,
    action: "identity.scim.user_upserted",
    level: "info",
    detail: "SCIM 用户同步完成。",
    metadata: {
      tenantId,
      userId: user.id,
      email: user.email,
      tenantRole: membership.tenantRole,
      organizationId: membership.organizationId,
    },
  });
  return c.json(
    toScimUserResource({
      userId: user.id,
      email: user.email,
      displayName: user.displayName,
      tenantRole: membership.tenantRole,
      organizationId: membership.organizationId,
    }),
    201,
  );
});

identityRoutes.put("/tenants/:tenantId/scim/users/:userId", async (c) => {
  const scimAuth = requireScimBearer(c);
  if (scimAuth) {
    return scimAuth;
  }
  const tenantId = resolveTenantId(c.req.param("tenantId"));
  if (!tenantId) {
    return c.json({ message: "tenantId 必须为非空字符串。" }, 400);
  }
  const userId = normalizeString(c.req.param("userId"));
  if (!userId) {
    return c.json({ message: "userId 必须为非空字符串。" }, 400);
  }
  const body = (await c.req.json().catch(() => undefined)) as Record<string, unknown> | undefined;
  if (!isRecord(body)) {
    return c.json({ message: "请求体必须为 JSON 对象。" }, 400);
  }

  const [user, membership] = await Promise.all([
    repository.getUserById(userId),
    repository.getTenantMemberByUser(tenantId, userId),
  ]);
  if (!user) {
    return c.json({ message: "未找到要更新的 SCIM 用户。" }, 404);
  }
  if (!membership) {
    return c.json({ message: "未找到要更新的租户成员关系。" }, 404);
  }

  const previous = {
    displayName: user.displayName,
    tenantRole: membership.tenantRole,
    organizationId: membership.organizationId,
    orgRole: membership.orgRole,
  };

  let nextDisplayName = user.displayName;
  if ("displayName" in body) {
    const normalized = normalizeString(body.displayName);
    if (!normalized) {
      return c.json({ message: "displayName 必须为非空字符串。" }, 400);
    }
    nextDisplayName = normalized;
  }

  let nextTenantRole: ScimRole = membership.tenantRole as ScimRole;
  if ("tenantRole" in body) {
    const parsed = parseScimRole(body.tenantRole, "tenantRole");
    if ("error" in parsed) {
      return c.json({ message: parsed.error }, 400);
    }
    nextTenantRole = parsed.role;
  }

  let nextOrganizationId = membership.organizationId;
  if ("organizationId" in body) {
    const rawOrganizationId = body.organizationId;
    if (rawOrganizationId === null || rawOrganizationId === undefined) {
      nextOrganizationId = undefined;
    } else if (typeof rawOrganizationId === "string") {
      nextOrganizationId = normalizeString(rawOrganizationId);
    } else {
      return c.json({ message: "organizationId 必须为字符串或 null。" }, 400);
    }
  }

  let nextOrgRole = membership.orgRole as ScimRole | undefined;
  if ("orgRole" in body) {
    if (!nextOrganizationId) {
      return c.json({ message: "organizationId 为空时不允许设置 orgRole。" }, 400);
    }
    const parsed = parseScimRole(body.orgRole, "orgRole");
    if ("error" in parsed) {
      return c.json({ message: parsed.error }, 400);
    }
    nextOrgRole = parsed.role;
  }
  if (!nextOrganizationId) {
    nextOrgRole = undefined;
  }

  const updatedUser =
    nextDisplayName === user.displayName
      ? user
      : await repository.createLocalUser({
          email: user.email,
          passwordHash: user.passwordHash,
          displayName: nextDisplayName,
        });

  let updatedMembership = membership;
  if (
    nextTenantRole !== membership.tenantRole ||
    nextOrganizationId !== membership.organizationId ||
    nextOrgRole !== membership.orgRole
  ) {
    try {
      updatedMembership = await repository.addTenantMember({
        tenantId,
        userId: user.id,
        tenantRole: nextTenantRole,
        organizationId: nextOrganizationId,
        orgRole: nextOrganizationId ? nextOrgRole : undefined,
      });
    } catch {
      return c.json({ message: "SCIM 用户更新失败：租户或参数无效。" }, 400);
    }
  }

  const requestId = c.get("requestId");
  await appendIdentityAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}:scim-user-updated`,
    action: "identity.scim.user_updated",
    level: "info",
    detail: "SCIM 用户更新完成。",
    metadata: {
      tenantId,
      userId: updatedUser.id,
      email: updatedUser.email,
      displayName: updatedUser.displayName,
      tenantRole: updatedMembership.tenantRole,
      organizationId: updatedMembership.organizationId,
      orgRole: updatedMembership.orgRole,
      previous,
    },
  });

  return c.json(
    toScimUserResource({
      userId: updatedUser.id,
      email: updatedUser.email,
      displayName: updatedUser.displayName,
      tenantRole: updatedMembership.tenantRole,
      organizationId: updatedMembership.organizationId,
    }),
    200,
  );
});

identityRoutes.get("/tenants/:tenantId/scim/groups", async (c) => {
  const scimAuth = requireScimBearer(c);
  if (scimAuth) {
    return scimAuth;
  }
  const tenantId = resolveTenantId(c.req.param("tenantId"));
  if (!tenantId) {
    return c.json({ message: "tenantId 必须为非空字符串。" }, 400);
  }
  const pagination = parseScimPagination(c.req.query());
  if (pagination.kind === "error") {
    return c.json({ message: pagination.error }, 400);
  }
  const filter = parseScimEqFilter(c.req.query().filter, {
    allowedAttributes: ["displayName"],
  });
  if (filter.kind === "error") {
    return c.json({ message: filter.error }, 400);
  }
  const organizations = await repository.listOrganizations(tenantId);
  const filteredOrganizations =
    filter.kind === "eq"
      ? organizations.filter((organization) => organization.name.toLowerCase() === filter.value.toLowerCase())
      : organizations;
  const totalResults = filteredOrganizations.length;
  const start = Math.max(0, pagination.startIndex - 1);
  const end = pagination.count === undefined ? undefined : start + pagination.count;
  const pageOrganizations = filteredOrganizations.slice(start, end);
  const memberships = await repository.listTenantMembers(tenantId);
  const resources = await Promise.all(
    pageOrganizations.map(async (organization) => {
      const members = await Promise.all(
        memberships
          .filter((membership) => membership.organizationId === organization.id)
          .map(async (membership) => {
            const user = await repository.getUserById(membership.userId);
            if (!user) {
              return null;
            }
            return {
              userId: user.id,
              email: user.email,
            };
          }),
      );
      return toScimGroupResource({
        organizationId: organization.id,
        name: organization.name,
        members: members.filter(Boolean) as Array<{ userId: string; email: string }>,
      });
    }),
  );
  return c.json({
    schemas: ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
    totalResults,
    startIndex: pagination.startIndex,
    itemsPerPage: resources.length,
    Resources: resources,
  });
});

identityRoutes.use("*", authMiddleware);

identityRoutes.post("/agents/lifecycle-events", async (c) => {
  const actorUserId = getActorUserId(c);
  if (actorUserId instanceof Response) {
    return actorUserId;
  }

  const auth = c.get("auth");
  if (!auth) {
    return unauthorized(c);
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateAgentLifecycleEventCreateInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  if (result.data.tenantId && result.data.tenantId !== auth.tenantId) {
    return c.json({ message: "请求体 tenantId 与当前租户不一致。" }, 403);
  }

  const membership = await repository.getTenantMemberByUser(auth.tenantId, actorUserId);
  if (!membership) {
    return forbiddenCrossTenant(c);
  }

  const item = saveAgentLifecycleEvent(auth.tenantId, result.data);
  const requestId = c.get("requestId");
  await appendIdentityAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:agent-lifecycle-create`,
    action: "identity.agent_lifecycle_event.created",
    level: "info",
    detail: `记录 agent 生命周期事件：${item.agentId}/${item.action}/${item.result}`,
    metadata: {
      tenantId: auth.tenantId,
      userId: actorUserId,
      requestId,
      route: "/api/v1/agents/lifecycle-events",
      agentId: item.agentId,
      deviceId: item.deviceId,
      hostname: item.hostname,
      version: item.version,
      actionName: item.action,
      result: item.result,
      occurredAt: item.occurredAt,
    },
  });

  return c.json(item, 201);
});

identityRoutes.get("/agents/lifecycle-events", async (c) => {
  const actorUserId = getActorUserId(c);
  if (actorUserId instanceof Response) {
    return actorUserId;
  }

  const auth = c.get("auth");
  if (!auth) {
    return unauthorized(c);
  }

  const membership = await repository.getTenantMemberByUser(auth.tenantId, actorUserId);
  if (!membership) {
    return forbiddenCrossTenant(c);
  }

  const result = validateAgentLifecycleEventListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  return c.json(listAgentLifecycleEvents(auth.tenantId, result.data));
});

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

  const [bindings, agentBindings, heartbeats] = await Promise.all([
    repository.listDeviceBindings(tenantId),
    repository.listAgentBindings(tenantId),
    repository.listAgentRuntimeHeartbeats(tenantId),
  ]);
  const deviceIdByAgentId = new Map(
    agentBindings
      .filter((binding) => binding.deviceId)
      .map((binding) => [binding.agentId, binding.deviceId as string] as const)
  );
  const lastSeenAtByDeviceId = new Map<string, string>();
  for (const heartbeat of heartbeats) {
    const deviceId = deviceIdByAgentId.get(heartbeat.agentId);
    if (!deviceId) {
      continue;
    }
    const currentLastSeenAt = lastSeenAtByDeviceId.get(deviceId);
    if (!currentLastSeenAt || heartbeat.occurredAt > currentLastSeenAt) {
      lastSeenAtByDeviceId.set(deviceId, heartbeat.occurredAt);
    }
  }
  const items = bindings.map((binding) =>
    toDeviceItem(binding, lastSeenAtByDeviceId.get(binding.deviceId))
  );
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

  const [bindings, heartbeats] = await Promise.all([
    repository.listAgentBindings(tenantId),
    repository.listAgentRuntimeHeartbeats(tenantId),
  ]);
  const lastSeenAtByAgentId = new Map(
    heartbeats.map((heartbeat) => [heartbeat.agentId, heartbeat.occurredAt] as const)
  );
  const items = bindings.map((binding) =>
    toAgentItem(binding, lastSeenAtByAgentId.get(binding.agentId))
  );
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
