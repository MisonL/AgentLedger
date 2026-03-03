import type { MiddlewareHandler } from "hono";
import { getControlPlaneRepository } from "../data/repository";
import { verifyAccessToken } from "../security/tokens";
import type { AppEnv } from "../types";

const repository = getControlPlaneRepository();

function extractBearerToken(authorization: string): string | null {
  const [scheme, token] = authorization.trim().split(/\s+/, 2);
  if (!scheme || !token) {
    return null;
  }
  if (scheme.toLowerCase() !== "bearer") {
    return null;
  }
  const trimmed = token.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function resolveTenantId(tenantId: string): string {
  const trimmed = tenantId.trim();
  return trimmed.length > 0 ? trimmed : "default";
}

function isExpired(expiresAt: string): boolean {
  const expiresAtMs = Date.parse(expiresAt);
  if (!Number.isFinite(expiresAtMs)) {
    return true;
  }
  return expiresAtMs <= Date.now();
}

export const authMiddleware: MiddlewareHandler<AppEnv> = async (c, next) => {
  const authorization = c.req.header("authorization");
  const requestId = c.get("requestId");
  if (!authorization) {
    return c.json(
      {
        message: "未提供认证凭证。",
        requestId,
      },
      401
    );
  }

  const token = extractBearerToken(authorization);
  if (!token) {
    return c.json(
      {
        message: "认证凭证格式无效。",
        requestId,
      },
      401
    );
  }

  const verifyResult = verifyAccessToken(token);
  if (!verifyResult.success) {
    return c.json(
      {
        message: "访问令牌无效或已过期。",
        requestId,
      },
      401
    );
  }

  const tenantId = resolveTenantId(verifyResult.payload.tid);
  const sessionId = verifyResult.payload.sid?.trim();
  if (!sessionId) {
    return c.json(
      {
        message: "访问令牌缺少会话信息。",
        requestId,
      },
      401
    );
  }

  const session = await repository.getAuthSessionById(sessionId);
  if (!session) {
    return c.json(
      {
        message: "登录会话不存在或已失效。",
        requestId,
      },
      401
    );
  }

  if (session.revokedAt || isExpired(session.expiresAt)) {
    return c.json(
      {
        message: "登录会话已失效，请重新登录。",
        requestId,
      },
      401
    );
  }

  if (session.userId !== verifyResult.payload.sub || session.tenantId !== tenantId) {
    return c.json(
      {
        message: "访问令牌与登录会话不匹配。",
        requestId,
      },
      401
    );
  }

  const user = await repository.getUserById(verifyResult.payload.sub);
  if (!user) {
    return c.json(
      {
        message: "用户不存在或已失效。",
        requestId,
      },
      401
    );
  }

  const membership = await repository.getTenantMemberByUser(tenantId, user.id);
  c.set("auth", {
    userId: user.id,
    email: user.email,
    displayName: user.displayName,
    tenantId,
    role: membership?.tenantRole ?? "member",
    sessionId: session.id,
  });

  await next();
};
