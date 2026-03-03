import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import {
  clearAuthTokens,
  fetchPricingCatalog,
  fetchSessionDetail,
  fetchSessionEvents,
  fetchHeatmap,
  fetchSources,
  fetchUsageDaily,
  fetchUsageModels,
  fetchUsageMonthly,
  fetchUsageSessions,
  getAccessToken,
  hasAccessToken,
  searchSessions,
  setAuthTokens,
  setUnauthorizedHandler,
  testSourceConnection,
  upsertPricingCatalog,
} from "../src/api";
import type { SessionSearchInput } from "../src/types";

interface MutableImportMetaEnv {
  DEV: boolean;
  VITE_ENABLE_MOCK_FALLBACK?: string;
  [key: string]: unknown;
}

const env = import.meta.env as unknown as MutableImportMetaEnv;

function mockNetworkError() {
  vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("network down"));
}

function toUrl(input: Parameters<typeof fetch>[0]): string {
  if (typeof input === "string") {
    return input;
  }
  if (input instanceof URL) {
    return input.toString();
  }
  return input.url;
}

function createDeferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;

  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return { promise, resolve, reject };
}

function createSessionSearchInput(): SessionSearchInput {
  return {
    from: "2026-03-02T00:00:00.000Z",
    to: "2026-03-03T00:00:00.000Z",
    limit: 50,
  };
}

function mockJsonResponse(data: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: {
      get: (name: string) => (name.toLowerCase() === "content-type" ? "application/json" : null),
    },
    json: async () => data,
    text: async () => JSON.stringify(data),
  } as Response;
}

describe("api mock fallback gate", () => {
  let originalDev: boolean;
  let originalMockFallbackFlag: string | undefined;

  beforeEach(() => {
    originalDev = env.DEV;
    originalMockFallbackFlag = env.VITE_ENABLE_MOCK_FALLBACK;
  });

  afterEach(() => {
    env.DEV = originalDev;
    if (originalMockFallbackFlag === undefined) {
      delete env.VITE_ENABLE_MOCK_FALLBACK;
    } else {
      env.VITE_ENABLE_MOCK_FALLBACK = originalMockFallbackFlag;
    }
    clearAuthTokens();
    setUnauthorizedHandler(null);
    vi.restoreAllMocks();
  });

  test("非开发且未开启开关时，请求失败应抛错", async () => {
    env.DEV = false;
    delete env.VITE_ENABLE_MOCK_FALLBACK;
    mockNetworkError();

    await expect(fetchHeatmap()).rejects.toThrow("network down");
    await expect(searchSessions(createSessionSearchInput())).rejects.toThrow("network down");
  });

  test("开发环境请求失败时回退到本地 mock", async () => {
    env.DEV = true;
    delete env.VITE_ENABLE_MOCK_FALLBACK;
    mockNetworkError();

    await expect(fetchHeatmap()).resolves.toEqual(
      expect.objectContaining({
        cells: expect.any(Array),
        summary: expect.any(Object),
      })
    );
    await expect(searchSessions(createSessionSearchInput())).resolves.toEqual(
      expect.objectContaining({
        items: expect.any(Array),
        total: expect.any(Number),
      })
    );
  });

  test("非开发但显式开启 VITE_ENABLE_MOCK_FALLBACK=true 时回退到本地 mock", async () => {
    env.DEV = false;
    env.VITE_ENABLE_MOCK_FALLBACK = "true";
    mockNetworkError();

    await expect(fetchHeatmap()).resolves.toEqual(
      expect.objectContaining({
        cells: expect.any(Array),
        summary: expect.any(Object),
      })
    );
    await expect(searchSessions(createSessionSearchInput())).resolves.toEqual(
      expect.objectContaining({
        items: expect.any(Array),
        total: expect.any(Number),
      })
    );
  });

  test("请求层会自动注入 Bearer token", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-001",
      refreshToken: "refresh-token-001",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(
        mockJsonResponse({
          items: [],
          total: 0,
        })
      );

    await expect(fetchSources()).resolves.toEqual({ items: [], total: 0 });

    const [, init] = fetchSpy.mock.calls[0] ?? [];
    const headers = new Headers((init as RequestInit | undefined)?.headers);
    expect(headers.get("authorization")).toBe("Bearer access-token-001");
  });

  test("请求返回 401 时会先 refresh 再重放原请求", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "expired-access-token",
      refreshToken: "refresh-token-old",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const onUnauthorized = vi.fn();
    setUnauthorizedHandler(onUnauthorized);

    let sourceGetCount = 0;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      const headers = new Headers(init?.headers);

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        sourceGetCount += 1;
        if (sourceGetCount === 1) {
          expect(headers.get("authorization")).toBe("Bearer expired-access-token");
          return mockJsonResponse(
            {
              message: "access token 已过期。",
            },
            401
          );
        }

        expect(headers.get("authorization")).toBe("Bearer refreshed-access-token");
        return mockJsonResponse({
          items: [],
          total: 0,
        });
      }

      if (url.endsWith("/api/v1/auth/refresh") && method === "POST") {
        expect(headers.get("authorization")).toBeNull();
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          refreshToken: "refresh-token-old",
        });
        return mockJsonResponse({
          tokens: {
            accessToken: "refreshed-access-token",
            refreshToken: "refresh-token-new",
            expiresIn: 1800,
            tokenType: "Bearer",
          },
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(fetchSources()).resolves.toEqual({ items: [], total: 0 });
    expect(fetchSpy).toHaveBeenCalledTimes(3);
    expect(sourceGetCount).toBe(2);
    expect(onUnauthorized).not.toHaveBeenCalled();
    expect(getAccessToken()).toBe("refreshed-access-token");
  });

  test("并发 401 仅触发一次 refresh 请求（single-flight）", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "expired-access-token",
      refreshToken: "refresh-token-concurrent",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const onUnauthorized = vi.fn();
    setUnauthorizedHandler(onUnauthorized);

    const refreshDeferred = createDeferred<Response>();
    let refreshCallCount = 0;
    let expiredSourceCallCount = 0;
    let refreshedSourceCallCount = 0;

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      const headers = new Headers(init?.headers);

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        const authorization = headers.get("authorization");
        if (authorization === "Bearer expired-access-token") {
          expiredSourceCallCount += 1;
          return mockJsonResponse(
            {
              message: "token 已过期。",
            },
            401
          );
        }
        if (authorization === "Bearer refreshed-access-token") {
          refreshedSourceCallCount += 1;
          return mockJsonResponse({ items: [], total: 0 });
        }
        throw new Error(`unexpected authorization: ${authorization}`);
      }

      if (url.endsWith("/api/v1/auth/refresh") && method === "POST") {
        refreshCallCount += 1;
        return refreshDeferred.promise;
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    const firstRequest = fetchSources();
    const secondRequest = fetchSources();

    await vi.waitFor(() => {
      expect(expiredSourceCallCount).toBe(2);
      expect(refreshCallCount).toBe(1);
    });

    refreshDeferred.resolve(
      mockJsonResponse({
        tokens: {
          accessToken: "refreshed-access-token",
          refreshToken: "refresh-token-rotated",
          expiresIn: 1800,
          tokenType: "Bearer",
        },
      })
    );

    await expect(Promise.all([firstRequest, secondRequest])).resolves.toEqual([
      { items: [], total: 0 },
      { items: [], total: 0 },
    ]);
    expect(refreshCallCount).toBe(1);
    expect(refreshedSourceCallCount).toBe(2);
    expect(onUnauthorized).not.toHaveBeenCalled();
  });

  test("refresh 失败时会清空 token 并触发未登录回调", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "expired-token",
      refreshToken: "refresh-token-expired",
      expiresIn: 1800,
      tokenType: "Bearer",
    });
    const onUnauthorized = vi.fn();
    setUnauthorizedHandler(onUnauthorized);

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        return mockJsonResponse(
          {
            message: "access token 已过期。",
          },
          401
        );
      }

      if (url.endsWith("/api/v1/auth/refresh") && method === "POST") {
        return mockJsonResponse(
          {
            message: "登录会话已失效。请重新登录。",
          },
          401
        );
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(fetchSources()).rejects.toThrow("登录会话已失效。请重新登录。");
    expect(fetchSpy).toHaveBeenCalledTimes(2);
    expect(onUnauthorized).toHaveBeenCalledTimes(1);
    expect(onUnauthorized).toHaveBeenCalledWith("登录会话已失效。请重新登录。");
    expect(hasAccessToken()).toBe(false);
  });

  test("usage 聚合接口（daily/monthly/models/sessions）会拼接 query 并返回列表结构", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-usage",
      refreshToken: "refresh-token-usage",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.includes("/api/v1/usage/daily") && method === "GET") {
        return mockJsonResponse({
          items: [{ date: "2026-02-01", tokens: 12, cost: 0.012, sessions: 2 }],
          total: 1,
        });
      }
      if (url.includes("/api/v1/usage/monthly") && method === "GET") {
        return mockJsonResponse({
          items: [{ month: "2026-02", tokens: 10, cost: 0.01, sessions: 1 }],
          total: 1,
        });
      }
      if (url.includes("/api/v1/usage/models") && method === "GET") {
        return mockJsonResponse({
          items: [{ model: "gpt-5", tokens: 10, cost: 0.01, sessions: 1 }],
          total: 1,
        });
      }
      if (url.includes("/api/v1/usage/sessions") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              sessionId: "s-1",
              sourceId: "source-1",
              tool: "Codex CLI",
              model: "gpt-5",
              startedAt: "2026-02-01T10:00:00.000Z",
              inputTokens: 1,
              outputTokens: 2,
              cacheReadTokens: 0,
              cacheWriteTokens: 0,
              reasoningTokens: 0,
              totalTokens: 3,
              cost: 0.001,
            },
          ],
          total: 1,
        });
      }
      throw new Error(`unexpected call: ${method} ${url}`);
    });

    const filters = {
      from: "2026-02-01T00:00:00.000Z",
      to: "2026-02-28T23:59:59.999Z",
      limit: 20,
    };
    await expect(fetchUsageDaily(filters)).resolves.toEqual(
      expect.objectContaining({ total: 1 })
    );
    await expect(fetchUsageMonthly(filters)).resolves.toEqual(
      expect.objectContaining({ total: 1 })
    );
    await expect(fetchUsageModels(filters)).resolves.toEqual(
      expect.objectContaining({ total: 1 })
    );
    await expect(fetchUsageSessions(filters)).resolves.toEqual(
      expect.objectContaining({ total: 1 })
    );

    expect(fetchSpy.mock.calls.some(([url]) => toUrl(url).includes("/api/v1/usage/daily?"))).toBe(
      true
    );
    expect(fetchSpy.mock.calls.some(([url]) => toUrl(url).includes("/api/v1/usage/monthly?"))).toBe(
      true
    );
    expect(fetchSpy.mock.calls.some(([url]) => toUrl(url).includes("/api/v1/usage/models?"))).toBe(
      true
    );
    expect(fetchSpy.mock.calls.some(([url]) => toUrl(url).includes("/api/v1/usage/sessions?"))).toBe(
      true
    );
  });

  test("session detail/events/pricing/sources test-connection 接口请求方式正确", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-feature",
      refreshToken: "refresh-token-feature",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.includes("/api/v1/sessions/session-1/events?limit=25") && method === "GET") {
        return mockJsonResponse({
          items: [],
          total: 0,
          limit: 25,
        });
      }
      if (url.endsWith("/api/v1/sessions/session-1") && method === "GET") {
        return mockJsonResponse({
          id: "session-1",
          sourceId: "source-1",
          tool: "Codex CLI",
          model: "gpt-5",
          startedAt: "2026-03-02T09:00:00.000Z",
          endedAt: "2026-03-02T09:03:00.000Z",
          tokens: 100,
          cost: 0.1,
          messageCount: 2,
          inputTokens: 40,
          outputTokens: 60,
          cacheReadTokens: 0,
          cacheWriteTokens: 0,
          reasoningTokens: 0,
          tokenBreakdown: {
            inputTokens: 40,
            outputTokens: 60,
            cacheReadTokens: 0,
            cacheWriteTokens: 0,
            reasoningTokens: 0,
            totalTokens: 100,
          },
          sourceTrace: {
            sourceId: "source-1",
            sourceName: "devbox",
            provider: "codex",
            path: "/workspace/a.ts",
          },
        });
      }
      if (url.endsWith("/api/v1/pricing/catalog") && method === "GET") {
        return mockJsonResponse({
          version: {
            id: "ver-1",
            tenantId: "tenant-1",
            version: 1,
            note: "init",
            createdAt: "2026-03-01T00:00:00.000Z",
          },
          entries: [{ model: "gpt-5", inputPer1k: 0.003, outputPer1k: 0.012, currency: "USD" }],
        });
      }
      if (url.endsWith("/api/v1/pricing/catalog") && method === "PUT") {
        return mockJsonResponse({
          version: {
            id: "ver-2",
            tenantId: "tenant-1",
            version: 2,
            note: "updated",
            createdAt: "2026-03-02T00:00:00.000Z",
          },
          entries: [{ model: "gpt-5", inputPer1k: 0.004, outputPer1k: 0.013, currency: "USD" }],
        });
      }
      if (url.endsWith("/api/v1/sources/test-connection") && method === "POST") {
        return mockJsonResponse({
          sourceId: "source-1",
          success: true,
          mode: "ssh",
          latencyMs: 8,
          detail: "ok",
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(fetchSessionEvents("session-1", 25)).resolves.toEqual(
      expect.objectContaining({ total: 0, limit: 25 })
    );
    await expect(fetchSessionDetail("session-1")).resolves.toEqual(
      expect.objectContaining({
        id: "session-1",
        tokenBreakdown: expect.objectContaining({ totalTokens: 100 }),
      })
    );
    await expect(fetchPricingCatalog()).resolves.toEqual(
      expect.objectContaining({
        version: expect.objectContaining({ id: "ver-1" }),
      })
    );
    await expect(
      upsertPricingCatalog({
        note: "updated",
        entries: [{ model: "gpt-5", inputPer1k: 0.004, outputPer1k: 0.013, currency: "USD" }],
      })
    ).resolves.toEqual(
      expect.objectContaining({
        version: expect.objectContaining({ id: "ver-2" }),
      })
    );
    await expect(testSourceConnection("source-1")).resolves.toEqual(
      expect.objectContaining({ success: true })
    );

    expect(
      fetchSpy.mock.calls.some(([url]) =>
        toUrl(url).includes("/api/v1/sessions/session-1/events?limit=25")
      )
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) => toUrl(url).endsWith("/api/v1/sessions/session-1"))
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url, init]) => {
        const requestInit = init as RequestInit | undefined;
        return toUrl(url).endsWith("/api/v1/pricing/catalog") && requestInit?.method === "PUT";
      })
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url, init]) => {
        const requestInit = init as RequestInit | undefined;
        return (
          toUrl(url).endsWith("/api/v1/sources/test-connection") &&
          requestInit?.method === "POST"
        );
      })
    ).toBe(true);
  });
});
