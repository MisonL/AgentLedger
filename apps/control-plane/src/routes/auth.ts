import { Hono, type Context } from "hono";
import { createHmac, timingSafeEqual } from "node:crypto";
import {
  validateAuthExternalExchangeInput,
  validateAuthExternalLoginInput,
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
const AUTH_EXTERNAL_ASSERTION_SECRET_ENV = "AUTH_EXTERNAL_ASSERTION_SECRET";
const AUTH_EXTERNAL_ASSERTION_TTL_SECONDS_ENV = "AUTH_EXTERNAL_ASSERTION_TTL_SECONDS";
const DEFAULT_AUTH_EXTERNAL_ASSERTION_TTL_SECONDS = 300;
const MAX_AUTH_EXTERNAL_ASSERTION_TTL_SECONDS = 3600;
const MAX_EXTERNAL_ASSERTION_NONCE_CACHE_SIZE = 20_000;
const externalAssertionNonceCache = new Map<string, number>();
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

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

interface ExternalLoginFailureAuditContext {
  providerId?: string;
  externalUserId?: string;
  email?: string;
  tenantId?: string;
  reason: string;
}

interface VerifyRefreshSessionOptions {
  onFailure?: (context: RefreshFailureAuditContext) => Promise<void> | void;
}

interface ExternalAuthProviderConfig extends AuthProviderItem {
  tokenUrl?: string;
  userInfoUrl?: string;
  clientId?: string;
  clientSecret?: string;
  scopes?: string[];
}

interface ExternalUserInfoProfile {
  externalUserId: string;
  email: string;
  displayName?: string;
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

function badGateway(c: Context<AppEnv>, message: string) {
  return c.json(
    {
      message,
      requestId: c.get("requestId"),
    },
    502
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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

function normalizeProviderScopes(value: unknown): string[] | undefined {
  const scopes: string[] = [];

  if (typeof value === "string") {
    for (const item of value.split(/[,\s]+/)) {
      const scope = normalizeOptionalString(item);
      if (scope) {
        scopes.push(scope);
      }
    }
  } else if (Array.isArray(value)) {
    for (const item of value) {
      const scope = normalizeOptionalString(item);
      if (scope) {
        scopes.push(scope);
      }
    }
  } else {
    return undefined;
  }

  if (scopes.length === 0) {
    return undefined;
  }

  return Array.from(new Set(scopes));
}

function normalizeExternalAuthProvider(value: unknown): ExternalAuthProviderConfig | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const record = value;
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
  const tokenUrl =
    normalizeOptionalString(record.tokenUrl) ?? normalizeOptionalString(record.tokenEndpoint);
  const userInfoUrl =
    normalizeOptionalString(record.userInfoUrl) ??
    normalizeOptionalString(record.userinfoUrl) ??
    normalizeOptionalString(record.userinfoEndpoint);
  const clientId = normalizeOptionalString(record.clientId);
  const clientSecret = normalizeOptionalString(record.clientSecret);
  const scopes = normalizeProviderScopes(record.scopes);

  return {
    id,
    type,
    displayName,
    enabled,
    issuer,
    authorizationUrl,
    tokenUrl,
    userInfoUrl,
    clientId,
    clientSecret,
    scopes,
  };
}

function toPublicAuthProviderItem(provider: ExternalAuthProviderConfig): AuthProviderItem {
  return {
    id: provider.id,
    type: provider.type,
    displayName: provider.displayName,
    enabled: provider.enabled,
    issuer: provider.issuer,
    authorizationUrl: provider.authorizationUrl,
  };
}

function resolveExternalAuthProvidersFromEnv(): ExternalAuthProviderConfig[] {
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

  const providers: ExternalAuthProviderConfig[] = [];
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
  providers.push(...resolveExternalAuthProvidersFromEnv().map((item) => toPublicAuthProviderItem(item)));
  return providers;
}

function resolveExternalAssertionTTLSeconds(): number {
  const raw = normalizeOptionalString(Bun.env[AUTH_EXTERNAL_ASSERTION_TTL_SECONDS_ENV]);
  if (!raw || !/^\d+$/.test(raw)) {
    return DEFAULT_AUTH_EXTERNAL_ASSERTION_TTL_SECONDS;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return DEFAULT_AUTH_EXTERNAL_ASSERTION_TTL_SECONDS;
  }
  return Math.min(MAX_AUTH_EXTERNAL_ASSERTION_TTL_SECONDS, parsed);
}

function resolveExternalAssertionSecret(): string | undefined {
  return normalizeOptionalString(Bun.env[AUTH_EXTERNAL_ASSERTION_SECRET_ENV]);
}

function findEnabledExternalProvider(providerId: string): AuthProviderItem | undefined {
  const providers = resolveAuthProviders();
  return providers.find(
    (item) => item.id === providerId && item.enabled && item.type !== "local"
  );
}

function isExternalExchangeProviderType(type: AuthProviderType): boolean {
  return type === "oauth2" || type === "oidc" || type === "sso";
}

function findEnabledExternalExchangeProvider(
  providerId: string
): ExternalAuthProviderConfig | undefined {
  const providers = resolveExternalAuthProvidersFromEnv();
  return providers.find(
    (item) =>
      item.id === providerId &&
      item.enabled &&
      isExternalExchangeProviderType(item.type) &&
      typeof item.tokenUrl === "string" &&
      typeof item.userInfoUrl === "string" &&
      typeof item.clientId === "string"
  );
}

async function parseJSONRecordSafely(
  response: Response
): Promise<Record<string, unknown> | undefined> {
  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    return undefined;
  }
  return isRecord(payload) ? payload : undefined;
}

function buildUpstreamFailureDetail(
  status: number,
  payload: Record<string, unknown> | undefined
): string {
  const upstreamErrorCode = normalizeOptionalString(payload?.error);
  const upstreamErrorMessage =
    normalizeOptionalString(payload?.error_description) ??
    normalizeOptionalString(payload?.message);

  if (upstreamErrorCode && upstreamErrorMessage) {
    return `HTTP ${status} ${upstreamErrorCode}: ${upstreamErrorMessage}`;
  }
  if (upstreamErrorCode) {
    return `HTTP ${status} ${upstreamErrorCode}`;
  }
  if (upstreamErrorMessage) {
    return `HTTP ${status}: ${upstreamErrorMessage}`;
  }
  return `HTTP ${status}`;
}

async function exchangeExternalAuthorizationCode(
  provider: ExternalAuthProviderConfig,
  input: {
    code: string;
    redirectUri: string;
    codeVerifier?: string;
  }
): Promise<{ success: true; accessToken: string } | { success: false; reason: string }> {
  if (!provider.tokenUrl || !provider.clientId) {
    return {
      success: false,
      reason: "provider token 配置缺失。",
    };
  }

  const form = new URLSearchParams();
  form.set("grant_type", "authorization_code");
  form.set("code", input.code);
  form.set("redirect_uri", input.redirectUri);
  form.set("client_id", provider.clientId);
  if (provider.clientSecret) {
    form.set("client_secret", provider.clientSecret);
  }
  if (input.codeVerifier) {
    form.set("code_verifier", input.codeVerifier);
  }
  if (provider.scopes && provider.scopes.length > 0) {
    form.set("scope", provider.scopes.join(" "));
  }

  let response: Response;
  try {
    response = await fetch(provider.tokenUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: form.toString(),
    });
  } catch (error) {
    return {
      success: false,
      reason: `调用 token endpoint 失败：${error instanceof Error ? error.message : String(error)}`,
    };
  }

  const payload = await parseJSONRecordSafely(response);
  if (!response.ok) {
    return {
      success: false,
      reason: `token endpoint 响应异常：${buildUpstreamFailureDetail(response.status, payload)}`,
    };
  }

  const accessToken = normalizeOptionalString(payload?.access_token);
  if (!accessToken) {
    return {
      success: false,
      reason: "token endpoint 响应缺少 access_token。",
    };
  }

  return {
    success: true,
    accessToken,
  };
}

function normalizeExternalUserDisplayName(payload: Record<string, unknown>): string | undefined {
  return (
    normalizeOptionalString(payload.name) ??
    normalizeOptionalString(payload.preferred_username) ??
    normalizeOptionalString(payload.nickname)
  );
}

async function fetchExternalUserInfo(
  provider: ExternalAuthProviderConfig,
  accessToken: string
): Promise<{ success: true; profile: ExternalUserInfoProfile } | { success: false; reason: string }> {
  if (!provider.userInfoUrl) {
    return {
      success: false,
      reason: "provider userInfo 配置缺失。",
    };
  }

  let response: Response;
  try {
    response = await fetch(provider.userInfoUrl, {
      method: "GET",
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${accessToken}`,
      },
    });
  } catch (error) {
    return {
      success: false,
      reason: `调用 userinfo endpoint 失败：${error instanceof Error ? error.message : String(error)}`,
    };
  }

  const payload = await parseJSONRecordSafely(response);
  if (!response.ok) {
    return {
      success: false,
      reason: `userinfo endpoint 响应异常：${buildUpstreamFailureDetail(response.status, payload)}`,
    };
  }
  if (!payload) {
    return {
      success: false,
      reason: "userinfo endpoint 响应不是合法 JSON 对象。",
    };
  }

  const externalUserId = normalizeOptionalString(payload.sub);
  const email = normalizeOptionalString(payload.email)?.toLowerCase();
  if (!externalUserId) {
    return {
      success: false,
      reason: "userinfo 响应缺少 sub。",
    };
  }
  if (!email || !EMAIL_PATTERN.test(email)) {
    return {
      success: false,
      reason: "userinfo 响应缺少合法 email。",
    };
  }

  return {
    success: true,
    profile: {
      externalUserId,
      email,
      displayName: normalizeExternalUserDisplayName(payload),
    },
  };
}

function buildExternalAssertionCanonicalPayload(input: {
  providerId: string;
  externalUserId: string;
  email: string;
  tenantId?: string;
  timestamp: string;
  nonce: string;
}): string {
  return [
    input.providerId,
    input.externalUserId,
    input.email,
    input.tenantId ?? "",
    input.timestamp,
    input.nonce,
  ].join("\n");
}

function verifyExternalAssertionSignature(canonicalPayload: string, signature: string, secret: string): boolean {
  const expectedHex = createHmac("sha256", secret).update(canonicalPayload).digest("hex");
  if (expectedHex.length !== signature.length) {
    return false;
  }
  const expectedBuffer = Buffer.from(expectedHex, "hex");
  const providedBuffer = Buffer.from(signature, "hex");
  if (expectedBuffer.length !== providedBuffer.length) {
    return false;
  }
  return timingSafeEqual(expectedBuffer, providedBuffer);
}

function cleanupExpiredExternalAssertionNonces(nowMs: number): void {
  for (const [key, expiresAtMs] of externalAssertionNonceCache.entries()) {
    if (!Number.isFinite(expiresAtMs) || expiresAtMs <= nowMs) {
      externalAssertionNonceCache.delete(key);
    }
  }
}

function enforceExternalAssertionNonceCacheLimit(): void {
  if (externalAssertionNonceCache.size <= MAX_EXTERNAL_ASSERTION_NONCE_CACHE_SIZE) {
    return;
  }
  const overSize = externalAssertionNonceCache.size - MAX_EXTERNAL_ASSERTION_NONCE_CACHE_SIZE;
  let removed = 0;
  for (const key of externalAssertionNonceCache.keys()) {
    externalAssertionNonceCache.delete(key);
    removed += 1;
    if (removed >= overSize) {
      return;
    }
  }
}

function claimExternalAssertionNonce(providerId: string, nonce: string, nowMs: number, ttlMs: number): boolean {
  cleanupExpiredExternalAssertionNonces(nowMs);
  const cacheKey = `${providerId}:${nonce}`;
  const existing = externalAssertionNonceCache.get(cacheKey);
  if (typeof existing === "number" && existing > nowMs) {
    return false;
  }
  externalAssertionNonceCache.set(cacheKey, nowMs + ttlMs);
  enforceExternalAssertionNonceCacheLimit();
  return true;
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

async function appendExternalLoginFailureAuditLog(
  c: Context<AppEnv>,
  context: ExternalLoginFailureAuditContext
): Promise<void> {
  const requestId = c.get("requestId");
  const tenantId =
    typeof context.tenantId === "string" && context.tenantId.trim().length > 0
      ? context.tenantId
      : DEFAULT_TENANT_ID;

  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}:auth-external-login`,
    action: "auth.external_login_failed",
    level: "warning",
    detail: `外部登录失败：${context.reason}`,
    metadata: {
      requestId,
      reason: context.reason,
      providerId: context.providerId,
      externalUserId: context.externalUserId,
      email: context.email,
      tenantId,
      route: "/api/v1/auth/external/login",
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

async function resolveTenantAccessForLogin(userId: string, tenantId?: string): Promise<PrimaryTenantAccess | null> {
  const normalizedTenantId = normalizeOptionalString(tenantId);
  if (!normalizedTenantId) {
    return resolvePrimaryTenantAccess(userId);
  }

  const membership = await repository.getTenantMemberByUser(normalizedTenantId, userId);
  if (!membership) {
    return null;
  }

  return {
    tenantId: membership.tenantId,
    tenantRole: membership.tenantRole,
  };
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

authRoutes.post("/external/exchange", async (c) => {
  const body = await c.req.json().catch(() => undefined);
  const result = validateAuthExternalExchangeInput(body);
  if (!result.success) {
    return badRequest(c, result.error);
  }

  const provider = findEnabledExternalExchangeProvider(result.data.providerId);
  if (!provider) {
    return unauthorized(c, "外部登录提供方不可用或未启用授权码交换。");
  }

  const tokenResult = await exchangeExternalAuthorizationCode(provider, {
    code: result.data.code,
    redirectUri: result.data.redirectUri,
    codeVerifier: result.data.codeVerifier,
  });
  if (!tokenResult.success) {
    console.warn(
      `[control-plane] 外部授权码交换 token 失败(provider=${provider.id})：${tokenResult.reason}`
    );
    return badGateway(c, "外部身份提供方响应异常，请稍后重试。");
  }

  const userInfoResult = await fetchExternalUserInfo(provider, tokenResult.accessToken);
  if (!userInfoResult.success) {
    console.warn(
      `[control-plane] 外部授权码交换 userinfo 失败(provider=${provider.id})：${userInfoResult.reason}`
    );
    return badGateway(c, "外部身份提供方响应异常，请稍后重试。");
  }

  let user = await repository.getLocalUserByEmail(userInfoResult.profile.email);
  if (!user) {
    const passwordHash = await Bun.password.hash(
      `external:${provider.id}:${userInfoResult.profile.externalUserId}:${crypto.randomUUID()}`
    );
    user = await repository.createLocalUser({
      email: userInfoResult.profile.email,
      passwordHash,
      displayName:
        userInfoResult.profile.displayName ??
        `${provider.id}:${userInfoResult.profile.externalUserId}`,
    });
  }

  const tenantAccess = await resolvePrimaryTenantAccess(user.id);
  const session = await createSessionForUser(user.id, tenantAccess.tenantId);
  const tokens = issueAuthTokens(user.id, tenantAccess.tenantId, session);
  const requestId = c.get("requestId");

  await appendAuditLogSafely({
    tenantId: tenantAccess.tenantId,
    eventId: `cp:${requestId}:auth-external-exchange`,
    action: "auth.external_exchange",
    level: "info",
    detail: `外部授权码登录成功(provider=${provider.id}, user=${user.id})`,
    metadata: {
      requestId,
      providerId: provider.id,
      providerType: provider.type,
      externalUserId: userInfoResult.profile.externalUserId,
      userId: user.id,
      tenantId: tenantAccess.tenantId,
      state: result.data.state,
    },
  });

  return c.json({
    user: {
      userId: user.id,
      email: user.email,
      displayName: user.displayName,
      tenantId: tenantAccess.tenantId,
      tenantRole: tenantAccess.tenantRole,
    },
    provider: {
      id: provider.id,
      type: provider.type,
      displayName: provider.displayName,
    },
    tokens,
  });
});

authRoutes.post("/external/login", async (c) => {
  const body = await c.req.json().catch(() => undefined);
  const result = validateAuthExternalLoginInput(body);
  if (!result.success) {
    await appendExternalLoginFailureAuditLog(c, {
      reason: result.error,
    });
    return badRequest(c, result.error);
  }

  const provider = findEnabledExternalProvider(result.data.providerId);
  if (!provider) {
    await appendExternalLoginFailureAuditLog(c, {
      providerId: result.data.providerId,
      externalUserId: result.data.externalUserId,
      email: result.data.email,
      tenantId: result.data.tenantId,
      reason: "外部登录提供方未启用或不存在。",
    });
    return unauthorized(c, "外部登录提供方未启用或不存在。");
  }

  const assertionSecret = resolveExternalAssertionSecret();
  if (!assertionSecret) {
    console.warn(
      `[control-plane] ${AUTH_EXTERNAL_ASSERTION_SECRET_ENV} 未配置，拒绝外部登录请求。`
    );
    await appendExternalLoginFailureAuditLog(c, {
      providerId: result.data.providerId,
      externalUserId: result.data.externalUserId,
      email: result.data.email,
      tenantId: result.data.tenantId,
      reason: "外部登录签名密钥未配置。",
    });
    return c.json(
      {
        message: "外部登录暂不可用，请联系管理员配置签名密钥。",
        requestId: c.get("requestId"),
      },
      503
    );
  }

  const timestampMs = Date.parse(result.data.timestamp);
  if (!Number.isFinite(timestampMs)) {
    await appendExternalLoginFailureAuditLog(c, {
      providerId: result.data.providerId,
      externalUserId: result.data.externalUserId,
      email: result.data.email,
      tenantId: result.data.tenantId,
      reason: "外部登录断言时间戳非法。",
    });
    return unauthorized(c, "外部登录断言时间戳非法。");
  }

  const ttlSeconds = resolveExternalAssertionTTLSeconds();
  const ttlMs = ttlSeconds * 1000;
  const nowMs = Date.now();
  if (Math.abs(nowMs - timestampMs) > ttlMs) {
    await appendExternalLoginFailureAuditLog(c, {
      providerId: result.data.providerId,
      externalUserId: result.data.externalUserId,
      email: result.data.email,
      tenantId: result.data.tenantId,
      reason: "外部登录断言已过期。",
    });
    return unauthorized(c, "外部登录断言已过期。");
  }

  const canonicalPayload = buildExternalAssertionCanonicalPayload({
    providerId: result.data.providerId,
    externalUserId: result.data.externalUserId,
    email: result.data.email,
    tenantId: result.data.tenantId,
    timestamp: result.data.timestamp,
    nonce: result.data.nonce,
  });
  if (
    !verifyExternalAssertionSignature(
      canonicalPayload,
      result.data.signature,
      assertionSecret
    )
  ) {
    await appendExternalLoginFailureAuditLog(c, {
      providerId: result.data.providerId,
      externalUserId: result.data.externalUserId,
      email: result.data.email,
      tenantId: result.data.tenantId,
      reason: "外部登录签名校验失败。",
    });
    return unauthorized(c, "外部登录签名校验失败。");
  }

  if (!claimExternalAssertionNonce(result.data.providerId, result.data.nonce, nowMs, ttlMs)) {
    await appendExternalLoginFailureAuditLog(c, {
      providerId: result.data.providerId,
      externalUserId: result.data.externalUserId,
      email: result.data.email,
      tenantId: result.data.tenantId,
      reason: "外部登录请求疑似重放。",
    });
    return unauthorized(c, "外部登录请求疑似重放，已拒绝。");
  }

  let user = await repository.getLocalUserByEmail(result.data.email);
  if (!user) {
    const passwordHash = await Bun.password.hash(
      `external:${result.data.providerId}:${result.data.externalUserId}:${crypto.randomUUID()}`
    );
    user = await repository.createLocalUser({
      email: result.data.email,
      passwordHash,
      displayName:
        result.data.displayName ?? `${result.data.providerId}:${result.data.externalUserId}`,
    });
  }

  const tenantAccess = await resolveTenantAccessForLogin(user.id, result.data.tenantId);
  if (!tenantAccess) {
    await appendExternalLoginFailureAuditLog(c, {
      providerId: result.data.providerId,
      externalUserId: result.data.externalUserId,
      email: result.data.email,
      tenantId: result.data.tenantId,
      reason: "外部账号未绑定到指定租户。",
    });
    return c.json(
      {
        message: "外部账号未绑定到指定租户，无法登录。",
        requestId: c.get("requestId"),
      },
      403
    );
  }

  const session = await createSessionForUser(user.id, tenantAccess.tenantId);
  const tokens = issueAuthTokens(user.id, tenantAccess.tenantId, session);
  const requestId = c.get("requestId");

  await appendAuditLogSafely({
    tenantId: tenantAccess.tenantId,
    eventId: `cp:${requestId}:auth-external-login`,
    action: "auth.external_login",
    level: "info",
    detail: `外部登录成功(provider=${provider.id}, user=${user.id})`,
    metadata: {
      requestId,
      providerId: provider.id,
      providerType: provider.type,
      externalUserId: result.data.externalUserId,
      userId: user.id,
      tenantId: tenantAccess.tenantId,
    },
  });

  return c.json({
    user: {
      userId: user.id,
      email: user.email,
      displayName: user.displayName,
      tenantId: tenantAccess.tenantId,
      tenantRole: tenantAccess.tenantRole,
    },
    provider: {
      id: provider.id,
      type: provider.type,
      displayName: provider.displayName,
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
