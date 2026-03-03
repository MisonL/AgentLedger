import { describe, expect, test } from "bun:test";
import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import { getControlPlaneRepository } from "../src/data/repository";
import { authMiddleware } from "../src/middleware/auth";
import { errorHandlerMiddleware } from "../src/middleware/error-handler";
import { requestIdMiddleware } from "../src/middleware/request-id";
import { issueAccessToken } from "../src/security/tokens";
import type { AppEnv } from "../src/types";

function createAuthTestApp() {
  const app = new Hono<AppEnv>();
  app.use("*", requestIdMiddleware);
  app.use("*", authMiddleware);
  app.get("/protected", (c) => c.json({ ok: true, auth: c.get("auth") }));
  return app;
}

function createMiddlewareContext(requestId: string) {
  return {
    get: (key: string) => (key === "requestId" ? requestId : undefined),
    json: (payload: unknown, status: number) =>
      new Response(JSON.stringify(payload), {
        status,
        headers: {
          "content-type": "application/json",
        },
      }),
  } as unknown as Parameters<typeof errorHandlerMiddleware>[0];
}

describe("Control Plane Middleware", () => {
  const repository = getControlPlaneRepository() as unknown as {
    getAuthSessionById: (id: string) => Promise<{
      id: string;
      userId: string;
      tenantId: string;
      sessionToken: string;
      expiresAt: string;
      revokedAt: string | null;
      replacedBySessionId: string | null;
      createdAt: string;
      updatedAt: string;
    } | null>;
    getUserById: (id: string) => Promise<{
      id: string;
      email: string;
      passwordHash: string;
      displayName: string;
      createdAt: string;
      updatedAt: string;
    } | null>;
  };

  test("authMiddleware：Bearer 头缺少 token 时返回 401", async () => {
    const app = createAuthTestApp();
    const response = await app.request("/protected", {
      headers: {
        authorization: "Bearer",
      },
    });

    expect(response.status).toBe(401);
    const payload = (await response.json()) as { message?: unknown };
    expect(payload.message).toBe("认证凭证格式无效。");
  });

  test("authMiddleware：会话 expiresAt 非法时按失效处理", async () => {
    const app = createAuthTestApp();
    const token = issueAccessToken({
      userId: "test-user-invalid-exp",
      tenantId: "default",
      sessionId: "test-session-invalid-exp",
    }).token;

    const originalGetAuthSessionById = repository.getAuthSessionById;

    try {
      repository.getAuthSessionById = async (id: string) => {
        if (id !== "test-session-invalid-exp") {
          return null;
        }
        const now = new Date().toISOString();
        return {
          id,
          userId: "test-user-invalid-exp",
          tenantId: "default",
          sessionToken: "session-token",
          expiresAt: "not-a-date",
          revokedAt: null,
          replacedBySessionId: null,
          createdAt: now,
          updatedAt: now,
        };
      };

      const response = await app.request("/protected", {
        headers: {
          authorization: `Bearer ${token}`,
        },
      });

      expect(response.status).toBe(401);
      const payload = (await response.json()) as { message?: unknown };
      expect(payload.message).toBe("登录会话已失效，请重新登录。");
    } finally {
      repository.getAuthSessionById = originalGetAuthSessionById;
    }
  });

  test("authMiddleware：会话存在但用户不存在时返回 401", async () => {
    const app = createAuthTestApp();
    const token = issueAccessToken({
      userId: "test-user-missing",
      tenantId: "default",
      sessionId: "test-session-user-missing",
    }).token;

    const originalGetAuthSessionById = repository.getAuthSessionById;
    const originalGetUserById = repository.getUserById;

    try {
      repository.getAuthSessionById = async (id: string) => {
        if (id !== "test-session-user-missing") {
          return null;
        }
        const now = new Date().toISOString();
        return {
          id,
          userId: "test-user-missing",
          tenantId: "default",
          sessionToken: "session-token",
          expiresAt: new Date(Date.now() + 10 * 60 * 1000).toISOString(),
          revokedAt: null,
          replacedBySessionId: null,
          createdAt: now,
          updatedAt: now,
        };
      };
      repository.getUserById = async () => null;

      const response = await app.request("/protected", {
        headers: {
          authorization: `Bearer ${token}`,
        },
      });

      expect(response.status).toBe(401);
      const payload = (await response.json()) as { message?: unknown };
      expect(payload.message).toBe("用户不存在或已失效。");
    } finally {
      repository.getAuthSessionById = originalGetAuthSessionById;
      repository.getUserById = originalGetUserById;
    }
  });

  test("errorHandlerMiddleware：捕获 HTTPException 并透传状态码", async () => {
    const response = (await errorHandlerMiddleware(
      createMiddlewareContext("rid-http-exception"),
      async () => {
        throw new HTTPException(429, { message: "请求过于频繁。" });
      },
    )) as Response;

    expect(response.status).toBe(429);
    const payload = (await response.json()) as {
      message?: unknown;
      requestId?: unknown;
    };
    expect(payload.message).toBe("请求过于频繁。");
    expect(typeof payload.requestId).toBe("string");
  });

  test("errorHandlerMiddleware：捕获未知异常并返回 500", async () => {
    const originalConsoleError = console.error;

    try {
      console.error = () => undefined;
      const response = (await errorHandlerMiddleware(
        createMiddlewareContext("rid-unexpected"),
        async () => {
          throw new Error("unexpected");
        },
      )) as Response;

      expect(response.status).toBe(500);
      const payload = (await response.json()) as {
        message?: unknown;
        requestId?: unknown;
      };
      expect(payload.message).toBe("服务器内部错误。");
      expect(typeof payload.requestId).toBe("string");
    } finally {
      console.error = originalConsoleError;
    }
  });
});
