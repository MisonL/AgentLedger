import { Hono, type Context } from "hono";
import {
  validateAuthLoginInput,
  validateAuthLogoutInput,
  validateAuthRefreshInput,
  validateAuthRegisterInput,
} from "../contracts";
import type { AuthProviderItem, AuthProviderType } from "../contracts";
import {
  getControlPlaneRepository,
  type AppendAuditLogInput,
  type AuthSession,
} from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import {
  TOKEN_TYPE,
  createAuthSessionToken,
  getRefreshSessionExpiresAt,
  issueAccessToken,
  issueRefreshToken,
  verifyRefreshToken,
  type AuthTokenPayload,
} from "../security/tokens";
import type { AppEnv } from "../types";

export const authRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const DEFAULT_TENANT_ID = "default";
const AUTH_DISABLE_LOCAL_LOGIN_ENV = "AUTH_DISABLE_LOCAL_LOGIN";
const AUTH_EXTERNAL_PROVIDERS_JSON_ENV = "AUTH_EXTERNAL_PROVIDERS_JSON";

interface PrimaryTenantAccess {
  tenantId: string;
  tenantRole: string;
}

interface VerifiedRefreshSession {
  payload: AuthTokenPayload;
  session: AuthSession;
}

interface RefreshFailureAuditContext {
  tenantId?: string;
  userId?: string;
  sessionId?: string;
  error: string;
}

interface VerifyRefreshSessionOptions {
  onFailure?: (context: RefreshFailureAuditContext) => Promise<void> | void;
}

function isExpired(expiresAt: string): boolean {
  const expiresAtMs = Date.parse(expiresAt);
  if (!Number.isFinite(expiresAtMs)) {
    return true;
  }
  return expiresAtMs <= Date.now();
}

function badRequest(c: Context<AppEnv>, message: string) {
  return c.json(
    {
      message,
      requestId: c.get("requestId"),
    },
    400
  );
}

function unauthorized(c: Context<AppEnv>, message: string) {
  return c.json(
    {
      message,
      requestId: c.get("requestId"),
    },
    401
  );
}

function conflict(c: Context<AppEnv>, message: string) {
  return c.json(
    {
      message,
      requestId: c.get("requestId"),
    },
    409
  );
}

function parseBooleanEnv(raw: string | undefined): boolean {
  const normalized = (raw ?? "").trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeProviderId(value: unknown): string | undefined {
  const normalized = normalizeOptionalString(value)?.toLowerCase();
  if (!normalized) {
    return undefined;
  }
  if (!/^[a-z0-9][a-z0-9_-]{1,63}$/.test(normalized)) {
    return undefined;
  }
  return normalized;
}

function normalizeProviderType(value: unknown): AuthProviderType | undefined {
  const normalized = normalizeOptionalString(value)?.toLowerCase();
  switch (normalized) {
    case "local":
    case "oauth2":
    case "oidc":
    case "sso":
      return normalized;
    default:
      return undefined;
  }
}

function normalizeExternalAuthProvider(value: unknown): AuthProviderItem | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return undefined;
  }

  const record = value as Record<string, unknown>;
  const id = normalizeProviderId(record.id);
  const type = normalizeProviderType(record.type);
  const displayName = normalizeOptionalString(record.displayName);
  if (!id || !type || !displayName) {
    return undefined;
  }

  const issuer = normalizeOptionalString(record.issuer);
  const authorizationUrl =
    normalizeOptionalString(record.authorizationUrl) ?? normalizeOptionalString(record.authUrl);
  const enabled = typeof record.enabled === "boolean" ? record.enabled : true;

  return {
    id,
    type,
    displayName,
    enabled,
    issuer,
    authorizationUrl,
  };
}

function resolveExternalAuthProvidersFromEnv(): AuthProviderItem[] {
  const raw = (Bun.env[AUTH_EXTERNAL_PROVIDERS_JSON_ENV] ?? "").trim();
  if (!raw) {
    return [];
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    console.warn(
      `[control-plane] ${AUTH_EXTERNAL_PROVIDERS_JSON_ENV} 不是合法 JSON，已忽略。`,
      error
    );
    return [];
  }

  if (!Array.isArray(parsed)) {
    console.warn(
      `[control-plane] ${AUTH_EXTERNAL_PROVIDERS_JSON_ENV} 必须是数组，当前值已忽略。`
    );
    return [];
  }

  const providers: AuthProviderItem[] = [];
  const seen = new Set<string>();
  for (const item of parsed) {
    const normalized = normalizeExternalAuthProvider(item);
    if (!normalized) {
      continue;
    }
    if (seen.has(normalized.id)) {
      continue;
    }
    seen.add(normalized.id);
    providers.push(normalized);
  }

  return providers;
}

function resolveAuthProviders(): AuthProviderItem[] {
  const providers: AuthProviderItem[] = [];
  if (!parseBooleanEnv(Bun.env[AUTH_DISABLE_LOCAL_LOGIN_ENV])) {
    providers.push({
      id: "local",
      type: "local",
      displayName: "邮箱密码",
      enabled: true,
    });
  }
  providers.push(...resolveExternalAuthProvidersFromEnv());
  return providers;
}

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 auth 审计日志失败。", error);
  }
}

async function appendRefreshFailureAuditLog(
  c: Context<AppEnv>,
  context: RefreshFailureAuditContext
): Promise<void> {
  const requestId = c.get("requestId");
  const tenantId =
    typeof context.tenantId === "string" && context.tenantId.trim().length > 0
      ? context.tenantId
      : DEFAULT_TENANT_ID;
  const userId =
    typeof context.userId === "string" && context.userId.trim().length > 0
      ? context.userId
      : "unknown";

  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}:auth-refresh`,
    action: "auth.refresh_failed",
    level: "warning",
    detail: `Refresh 失败：${context.error}`,
    metadata: {
      tenant: tenantId,
      tenantId,
      user: userId,
      userId,
      requestId,
      "request-id": requestId,
      result: "failed",
      error: context.error,
      sessionId: context.sessionId,
      route: "/api/v1/auth/refresh",
    },
  });
}

async function reportRefreshVerificationFailure(
  options: VerifyRefreshSessionOptions | undefined,
  context: RefreshFailureAuditContext
): Promise<void> {
  if (!options?.onFailure) {
    return;
  }

  try {
    await options.onFailure(context);
  } catch (error) {
    console.warn("[control-plane] 处理 refresh 失败审计回调时发生异常。", error);
  }
}

function issueAuthTokens(userId: string, tenantId: string, session: AuthSession) {
  const accessToken = issueAccessToken({
    userId,
    tenantId,
    sessionId: session.id,
  });
  const refreshToken = issueRefreshToken({
    userId,
    tenantId,
    sessionId: session.id,
    sessionToken: session.sessionToken,
  });

  return {
    accessToken: accessToken.token,
    refreshToken: refreshToken.token,
    expiresIn: accessToken.expiresIn,
    tokenType: TOKEN_TYPE,
  } as const;
}

async function resolvePrimaryTenantAccess(userId: string): Promise<PrimaryTenantAccess> {
  const tenants = await repository.listTenants();

  for (const tenant of tenants) {
    const membership = await repository.getTenantMemberByUser(tenant.id, userId);
    if (membership) {
      return {
        tenantId: tenant.id,
        tenantRole: membership.tenantRole,
      };
    }
  }

  try {
    const membership = await repository.addTenantMember({
      tenantId: DEFAULT_TENANT_ID,
      userId,
      tenantRole: "owner",
    });
    return {
      tenantId: membership.tenantId,
      tenantRole: membership.tenantRole,
    };
  } catch (error) {
    console.warn("[control-plane] 自动补齐默认租户成员关系失败。", error);
    return {
      tenantId: DEFAULT_TENANT_ID,
      tenantRole: "member",
    };
  }
}

async function createSessionForUser(userId: string, tenantId: string): Promise<AuthSession> {
  return repository.createAuthSession({
    userId,
    tenantId,
    sessionToken: createAuthSessionToken(),
    expiresAt: getRefreshSessionExpiresAt(),
  });
}

async function verifyRefreshSession(
  c: Context<AppEnv>,
  refreshToken: string,
  options?: VerifyRefreshSessionOptions
): Promise<VerifiedRefreshSession | Response> {
  const verifyResult = verifyRefreshToken(refreshToken);
  if (!verifyResult.success) {
    const errorMessage = "刷新令牌无效或已过期。请重新登录。";
    await reportRefreshVerificationFailure(options, {
      error: errorMessage,
    });
    return unauthorized(c, errorMessage);
  }

  const tokenTenantId = verifyResult.payload.tid;
  const tokenUserId = verifyResult.payload.sub;
  const sessionId = verifyResult.payload.sid;
  const sessionToken = verifyResult.payload.st;
  if (!sessionId || !sessionToken) {
    const errorMessage = "刷新令牌缺少会话信息。请重新登录。";
    await reportRefreshVerificationFailure(options, {
      tenantId: tokenTenantId,
      userId: tokenUserId,
      error: errorMessage,
    });
    return unauthorized(c, errorMessage);
  }

  const session = await repository.getAuthSessionById(sessionId);
  if (!session) {
    const errorMessage = "登录会话不存在。请重新登录。";
    await reportRefreshVerificationFailure(options, {
      tenantId: tokenTenantId,
      userId: tokenUserId,
      sessionId,
      error: errorMessage,
    });
    return unauthorized(c, errorMessage);
  }

  if (session.revokedAt) {
    const errorMessage = "登录会话已失效。请重新登录。";
    await reportRefreshVerificationFailure(options, {
      tenantId: session.tenantId,
      userId: session.userId,
      sessionId,
      error: errorMessage,
    });
    return unauthorized(c, errorMessage);
  }

  if (isExpired(session.expiresAt)) {
    const errorMessage = "登录会话已过期。请重新登录。";
    await reportRefreshVerificationFailure(options, {
      tenantId: session.tenantId,
      userId: session.userId,
      sessionId,
      error: errorMessage,
    });
    return unauthorized(c, errorMessage);
  }

  if (
    session.sessionToken !== sessionToken ||
    session.userId !== verifyResult.payload.sub ||
    session.tenantId !== verifyResult.payload.tid
  ) {
    const errorMessage = "刷新令牌与会话不匹配。请重新登录。";
    await reportRefreshVerificationFailure(options, {
      tenantId: session.tenantId,
      userId: session.userId,
      sessionId,
      error: errorMessage,
    });
    return unauthorized(c, errorMessage);
  }

  return {
    payload: verifyResult.payload,
    session,
  };
}

authRoutes.get("/providers", (c) => {
  const items = resolveAuthProviders();
  return c.json({
    items,
    total: items.length,
  });
});

authRoutes.post("/register", async (c) => {
  const body = await c.req.json().catch(() => undefined);
  const result = validateAuthRegisterInput(body);
  if (!result.success) {
    return badRequest(c, result.error);
  }

  const existing = await repository.getLocalUserByEmail(result.data.email);
  if (existing) {
    return conflict(c, "该邮箱已注册，请直接登录。");
  }

  const passwordHash = await Bun.password.hash(result.data.password);
  const user = await repository.createLocalUser({
    email: result.data.email,
    passwordHash,
    displayName: result.data.displayName,
  });

  const tenantAccess = await resolvePrimaryTenantAccess(user.id);
  const session = await createSessionForUser(user.id, tenantAccess.tenantId);
  const tokens = issueAuthTokens(user.id, tenantAccess.tenantId, session);

  return c.json(
    {
      user: {
        userId: user.id,
        email: user.email,
        displayName: user.displayName,
        tenantId: tenantAccess.tenantId,
        tenantRole: tenantAccess.tenantRole,
      },
      tokens,
    },
    201
  );
});

authRoutes.post("/login", async (c) => {
  const body = await c.req.json().catch(() => undefined);
  const result = validateAuthLoginInput(body);
  if (!result.success) {
    return badRequest(c, result.error);
  }

  const user = await repository.getLocalUserByEmail(result.data.email);
  if (!user) {
    return unauthorized(c, "邮箱或密码错误。");
  }

  const passwordMatched = await Bun.password.verify(result.data.password, user.passwordHash);
  if (!passwordMatched) {
    return unauthorized(c, "邮箱或密码错误。");
  }

  const tenantAccess = await resolvePrimaryTenantAccess(user.id);
  const session = await createSessionForUser(user.id, tenantAccess.tenantId);
  const tokens = issueAuthTokens(user.id, tenantAccess.tenantId, session);

  return c.json({
    user: {
      userId: user.id,
      email: user.email,
      displayName: user.displayName,
      tenantId: tenantAccess.tenantId,
      tenantRole: tenantAccess.tenantRole,
    },
    tokens,
  });
});

authRoutes.post("/refresh", async (c) => {
  const body = await c.req.json().catch(() => undefined);
  const result = validateAuthRefreshInput(body);
  if (!result.success) {
    await appendRefreshFailureAuditLog(c, {
      error: result.error,
    });
    return badRequest(c, result.error);
  }

  const verification = await verifyRefreshSession(c, result.data.refreshToken, {
    onFailure: (context) => appendRefreshFailureAuditLog(c, context),
  });
  if (verification instanceof Response) {
    return verification;
  }

  const rotatedSession = await repository.rotateAuthSession(verification.session.id, {
    sessionToken: createAuthSessionToken(),
    expiresAt: getRefreshSessionExpiresAt(),
  });
  if (!rotatedSession) {
    const errorMessage = "登录会话已失效。请重新登录。";
    await appendRefreshFailureAuditLog(c, {
      tenantId: verification.payload.tid,
      userId: verification.payload.sub,
      sessionId: verification.session.id,
      error: errorMessage,
    });
    return unauthorized(c, errorMessage);
  }

  const tokens = issueAuthTokens(
    verification.payload.sub,
    verification.payload.tid,
    rotatedSession
  );

  return c.json({
    tokens,
    session: {
      sessionId: rotatedSession.id,
      issuedAt: rotatedSession.createdAt,
      expiresAt: rotatedSession.expiresAt,
    },
  });
});

authRoutes.post("/logout", async (c) => {
  const body = await c.req.json().catch(() => undefined);
  const result = validateAuthLogoutInput(body);
  if (!result.success) {
    return badRequest(c, result.error);
  }

  const verification = await verifyRefreshSession(c, result.data.refreshToken);
  if (verification instanceof Response) {
    return verification;
  }

  await repository.revokeAuthSession(verification.session.id);
  return c.json({
    message: "已退出登录。",
  });
});

authRoutes.get("/me", authMiddleware, async (c) => {
  const auth = c.get("auth");
  if (!auth) {
    return unauthorized(c, "未认证：请先登录。");
  }

  if (!auth.sessionId) {
    return unauthorized(c, "访问令牌缺少会话信息。请重新登录。");
  }

  const user = await repository.getUserById(auth.userId);
  if (!user) {
    return unauthorized(c, "用户不存在或已失效。");
  }

  const session = await repository.getAuthSessionById(auth.sessionId);
  if (!session || session.revokedAt || isExpired(session.expiresAt)) {
    return unauthorized(c, "登录会话已失效。请重新登录。");
  }

  const tenants = await repository.listTenants();
  const memberships = await Promise.all(
    tenants.map((tenant) => repository.getTenantMemberByUser(tenant.id, user.id))
  );

  const tenantItems = tenants.flatMap((tenant, index) => {
    if (!memberships[index]) {
      return [];
    }
    return [
      {
        id: tenant.id,
        name: tenant.name,
        slug: tenant.id,
        active: true,
        createdAt: tenant.createdAt,
        updatedAt: tenant.updatedAt,
      },
    ];
  });

  const activeMembership =
    memberships.find((membership, index) => membership && tenants[index]?.id === auth.tenantId) ??
    null;

  const organizations = activeMembership
    ? await repository.listOrganizations(auth.tenantId)
    : [];

  return c.json({
    user: {
      userId: user.id,
      email: user.email,
      displayName: user.displayName,
      tenantId: auth.tenantId,
      tenantRole: activeMembership?.tenantRole,
    },
    session: {
      sessionId: session.id,
      issuedAt: session.createdAt,
      expiresAt: session.expiresAt,
    },
    tenants: tenantItems,
    organizations: organizations.map((organization) => ({
      id: organization.id,
      tenantId: organization.tenantId,
      name: organization.name,
      slug: organization.id,
      active: true,
      createdAt: organization.createdAt,
      updatedAt: organization.updatedAt,
    })),
  });
});
