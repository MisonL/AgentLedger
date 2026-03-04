import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import {
  clearAuthTokens,
  exportSessions,
  exportUsage,
  exchangeExternalAuthCode,
  fetchAlerts,
  fetchAuthProviders,
  fetchPricingCatalog,
  fetchSourceHealth,
  fetchSourceParseFailures,
  fetchSessionDetail,
  fetchSessionEvents,
  fetchHeatmap,
  fetchSources,
  fetchUsageDaily,
  fetchUsageModels,
  fetchUsageMonthly,
  fetchUsageSessions,
  fetchUsageWeeklySummary,
  getAccessToken,
  hasAccessToken,
  searchSessions,
  setAuthTokens,
  setUnauthorizedHandler,
  testSourceConnection,
  updateAlertStatus,
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

function mockFileResponse(
  content: string,
  options?: {
    status?: number;
    contentType?: string;
    contentDisposition?: string;
  }
): Response {
  const status = options?.status ?? 200;
  const contentType = options?.contentType ?? "text/csv; charset=utf-8";
  const blob = new Blob([content], { type: contentType });
  const contentDisposition =
    options?.contentDisposition ?? 'attachment; filename="export.csv"';

  return {
    ok: status >= 200 && status < 300,
    status,
    headers: {
      get: (name: string) => {
        const normalized = name.toLowerCase();
        if (normalized === "content-type") {
          return contentType;
        }
        if (normalized === "content-disposition") {
          return contentDisposition;
        }
        return null;
      },
    },
    blob: async () => blob,
    text: async () => content,
    json: async () => {
      throw new Error("not json");
    },
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

  test("searchSessions 响应缺失 nextCursor 时抛错", async () => {
    env.DEV = false;
    delete env.VITE_ENABLE_MOCK_FALLBACK;
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      mockJsonResponse({
        items: [],
        total: 0,
        filters: createSessionSearchInput(),
      }),
    );

    await expect(searchSessions(createSessionSearchInput())).rejects.toThrow(
      "session 返回结构不合法",
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

  test("usage 聚合接口（daily/monthly/models/sessions/weekly-summary）会拼接 query 并返回列表结构", async () => {
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
      if (url.includes("/api/v1/usage/weekly-summary") && method === "GET") {
        return mockJsonResponse({
          metric: "tokens",
          timezone: "Asia/Shanghai",
          weeks: [
            {
              weekStart: "2026-02-24",
              weekEnd: "2026-03-02",
              tokens: 3200,
              cost: 1.23,
              sessions: 4,
            },
          ],
          summary: {
            tokens: 3200,
            cost: 1.23,
            sessions: 4,
          },
          peakWeek: {
            weekStart: "2026-02-24",
            weekEnd: "2026-03-02",
            tokens: 3200,
            cost: 1.23,
            sessions: 4,
          },
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
    await expect(
      fetchUsageWeeklySummary({
        metric: "tokens",
        timezone: "Asia/Shanghai",
        from: filters.from,
        to: filters.to,
      })
    ).resolves.toEqual(
      expect.objectContaining({
        metric: "tokens",
        timezone: "Asia/Shanghai",
      })
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
    expect(
      fetchSpy.mock.calls.some(([url]) => {
        const value = toUrl(url);
        return (
          value.includes("/api/v1/usage/weekly-summary?") &&
          value.includes("metric=tokens") &&
          value.includes("timezone=Asia%2FShanghai")
        );
      })
    ).toBe(true);
  });

  test("session detail/events/pricing/source health/parse-failures/test-connection 接口请求方式正确", async () => {
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
      if (url.endsWith("/api/v1/sources/source-1/health") && method === "GET") {
        return mockJsonResponse({
          sourceId: "source-1",
          accessMode: "sync",
          lastSuccessAt: "2026-03-02T09:10:00.000Z",
          lastFailureAt: "2026-03-02T09:05:00.000Z",
          failureCount: 1,
          avgLatencyMs: 108,
          freshnessMinutes: 4,
        });
      }
      if (url.includes("/api/v1/sources/source-1/parse-failures?") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "pf-1",
              sourceId: "source-1",
              parserKey: "jsonl",
              errorCode: "parse_error",
              errorMessage: "json line parse failed",
              sourcePath: "/tmp/a.jsonl",
              sourceOffset: 12,
              rawHash: "hash-1",
              metadata: {
                parser: "jsonl",
              },
              failedAt: "2026-03-02T09:06:00.000Z",
              createdAt: "2026-03-02T09:06:00.000Z",
            },
          ],
          total: 1,
          filters: {
            from: "2026-03-02T00:00:00.000Z",
            to: "2026-03-03T00:00:00.000Z",
            parserKey: "jsonl",
            errorCode: "parse_error",
            limit: 10,
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
    await expect(fetchSourceHealth("source-1")).resolves.toEqual(
      expect.objectContaining({
        sourceId: "source-1",
        accessMode: "sync",
        failureCount: 1,
      })
    );
    await expect(
      fetchSourceParseFailures("source-1", {
        from: "2026-03-02T00:00:00.000Z",
        to: "2026-03-03T00:00:00.000Z",
        parserKey: "jsonl",
        errorCode: "parse_error",
        limit: 10,
      })
    ).resolves.toEqual(
      expect.objectContaining({
        total: 1,
        items: expect.any(Array),
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
      fetchSpy.mock.calls.some(([url]) => toUrl(url).endsWith("/api/v1/sources/source-1/health"))
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) =>
        toUrl(url).includes("/api/v1/sources/source-1/parse-failures?")
      )
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) =>
        toUrl(url).includes("parserKey=jsonl")
      )
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) =>
        toUrl(url).includes("errorCode=parse_error")
      )
    ).toBe(true);
    expect(fetchSpy.mock.calls.some(([url]) => toUrl(url).includes("limit=10"))).toBe(true);
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

  test("source health 与 parse-failures 返回非法结构时抛错", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-invalid-payload",
      refreshToken: "refresh-token-invalid-payload",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/sources/source-invalid/health") && method === "GET") {
        return mockJsonResponse({
          sourceId: "source-invalid",
          accessMode: "sync",
        });
      }
      if (url.endsWith("/api/v1/sources/source-invalid/parse-failures") && method === "GET") {
        return mockJsonResponse({
          items: [{ id: "pf-invalid" }],
          total: 1,
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(fetchSourceHealth("source-invalid")).rejects.toThrow(
      "sources.health 返回结构不合法"
    );
    await expect(fetchSourceParseFailures("source-invalid")).rejects.toThrow(
      "sources.parse-failures 返回结构不合法"
    );
  });

  test("fetchAuthProviders 返回登录提供方列表", async () => {
    env.DEV = false;
    clearAuthTokens();

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/auth/providers") && method === "GET") {
        const headers = new Headers(init?.headers);
        expect(headers.get("authorization")).toBeNull();
        return mockJsonResponse({
          items: [
            {
              id: "local",
              type: "local",
              displayName: "邮箱密码登录",
              enabled: true,
            },
            {
              id: "corp-oidc",
              type: "oidc",
              displayName: "企业 OIDC",
              enabled: true,
              authorizationUrl: "https://idp.example.com/oauth/authorize",
            },
          ],
          total: 2,
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(fetchAuthProviders()).resolves.toEqual({
      items: [
        {
          id: "local",
          type: "local",
          displayName: "邮箱密码登录",
          enabled: true,
        },
        {
          id: "corp-oidc",
          type: "oidc",
          displayName: "企业 OIDC",
          enabled: true,
          authorizationUrl: "https://idp.example.com/oauth/authorize",
        },
      ],
      total: 2,
    });

    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  test("exchangeExternalAuthCode 成功后写入本地 token", async () => {
    env.DEV = false;
    clearAuthTokens();

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/auth/external/exchange") && method === "POST") {
        const payload = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
        expect(payload.providerId).toBe("corp-oidc");
        expect(payload.code).toBe("authorization-code-1");
        expect(payload.redirectUri).toContain("#/auth/callback");
        expect(payload.state).toBe("corp-oidc:nonce-1");
        return mockJsonResponse({
          user: {
            userId: "user-ext-1",
            email: "owner@example.com",
            displayName: "Owner",
            tenantId: "default",
            tenantRole: "owner",
          },
          tokens: {
            accessToken: "access-token-external",
            refreshToken: "refresh-token-external",
            expiresIn: 1800,
            tokenType: "Bearer",
          },
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(
      exchangeExternalAuthCode({
        providerId: "corp-oidc",
        code: "authorization-code-1",
        redirectUri: "http://localhost:5173/#/auth/callback",
        state: "corp-oidc:nonce-1",
      })
    ).resolves.toEqual(
      expect.objectContaining({
        user: expect.objectContaining({
          userId: "user-ext-1",
        }),
      })
    );

    expect(hasAccessToken()).toBe(true);
    expect(getAccessToken()).toBe("access-token-external");
  });

  test("fetchAlerts 成功解析列表结果", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-alerts",
      refreshToken: "refresh-token-alerts",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      if (url.includes("/api/v1/alerts") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "alert-1",
              tenantId: "default",
              budgetId: "budget-1",
              scope: "tenant",
              scopeRef: "default",
              severity: "critical",
              status: "open",
              message: "cost exceeded",
              threshold: 0.8,
              value: 0.91,
              createdAt: "2026-03-01T10:00:00.000Z",
              updatedAt: "2026-03-01T10:05:00.000Z",
              metadata: {},
            },
          ],
          total: 1,
          filters: {
            severity: "critical",
          },
          nextCursor: null,
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(fetchAlerts({ severity: "critical", limit: 10 })).resolves.toEqual(
      expect.objectContaining({
        total: 1,
        items: expect.arrayContaining([
          expect.objectContaining({
            id: "alert-1",
            severity: "critical",
          }),
        ]),
      })
    );
  });

  test("updateAlertStatus 会发起 PATCH 并提交 status", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-alert-update",
      refreshToken: "refresh-token-alert-update",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      if (url.endsWith("/api/v1/alerts/alert-2/status") && method === "PATCH") {
        return mockJsonResponse({
          id: "alert-2",
          tenantId: "default",
          budgetId: "budget-2",
          scope: "tenant",
          scopeRef: "default",
          severity: "warning",
          status: "acknowledged",
          message: "near threshold",
          threshold: 0.7,
          value: 0.72,
          createdAt: "2026-03-01T10:00:00.000Z",
          updatedAt: "2026-03-01T10:05:00.000Z",
          metadata: {},
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(updateAlertStatus("alert-2", "acknowledged")).resolves.toEqual(
      expect.objectContaining({
        id: "alert-2",
        status: "acknowledged",
      })
    );

    const [, init] = fetchSpy.mock.calls[0] ?? [];
    expect((init as RequestInit | undefined)?.method).toBe("PATCH");
    expect(JSON.parse(String((init as RequestInit | undefined)?.body ?? "{}"))).toEqual({
      status: "acknowledged",
    });
  });

  test("exportSessions 与 exportUsage 支持 csv 下载", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-export",
      refreshToken: "refresh-token-export",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.includes("/api/v1/exports/sessions") && method === "GET") {
        return mockFileResponse("id,tool\ns1,codex\n", {
          contentType: "text/csv; charset=utf-8",
          contentDisposition: 'attachment; filename="sessions-2026.csv"',
        });
      }

      if (url.includes("/api/v1/exports/usage") && method === "GET") {
        return mockFileResponse("date,tokens\n2026-03-01,1000\n", {
          contentType: "text/csv; charset=utf-8",
          contentDisposition: 'attachment; filename="usage-weekly-2026.csv"',
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(exportSessions("csv", { limit: 20 })).resolves.toEqual(
      expect.objectContaining({
        filename: "sessions-2026.csv",
        contentType: expect.stringContaining("text/csv"),
        blob: expect.any(Blob),
      })
    );

    await expect(
      exportUsage("csv", {
        dimension: "weekly",
        limit: 10,
      })
    ).resolves.toEqual(
      expect.objectContaining({
        filename: "usage-weekly-2026.csv",
        contentType: expect.stringContaining("text/csv"),
        blob: expect.any(Blob),
      })
    );
  });

  test("exportUsage 缺少合法 dimension 时抛错", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-export-invalid",
      refreshToken: "refresh-token-export-invalid",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    await expect(
      exportUsage("csv", {
        dimension: "invalid" as never,
      })
    ).rejects.toThrow("dimension 必须是 daily/weekly/monthly/models/sessions/heatmap。");
  });
});
