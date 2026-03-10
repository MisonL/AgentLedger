import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import {
  approveMcpApproval,
  createIntegrationDlqRecoveryJob,
  createMcpApproval,
  backfillSourceRegions,
  cancelOpenPlatformQualityAdviceExecution,
  cancelOpenPlatformReplayExperiment,
  clearAuthTokens,
  createOpenPlatformReplayDataset,
  createOpenPlatformReplayDatasetVersion,
  createOpenPlatformReplayExperiment,
  createOpenPlatformReplayRun,
  deleteOpenPlatformWebhook,
  downloadOpenPlatformReplayArtifact,
  evaluateMcpTool,
  executeOpenPlatformQualityAdvice,
  exportSessions,
  exportUsage,
  exportAudits,
  exportAuditEvidenceBundle,
  exchangeExternalAuthCode,
  fetchAgentRuntimeConfig,
  fetchAgentRuntimeViews,
  fetchAlertExternalLinkFailures,
  fetchAlerts,
  fetchAuthProviders,
  fetchIntegrationAlertFailureReport,
  fetchIntegrationAlertFailureTrends,
  fetchIntegrationDlqRecoveryJobDetail,
  fetchIntegrationDlqRecoveryJobs,
  fetchIntegrationDlqMessages,
  fetchMcpApprovals,
  fetchMcpInvocations,
  fetchMcpPolicies,
  fetchPricingCatalog,
  fetchOpenPlatformAutomationPolicy,
  fetchOpenPlatformQualityAdvice,
  fetchOpenPlatformQualityAdviceExecutions,
  fetchOpenPlatformQualityForecast,
  fetchOpenPlatformQualityProjectTrends,
  fetchOpenPlatformReplayExperimentsBatchCompare,
  fetchOpenPlatformReplayExperimentCompare,
  fetchOpenPlatformReplayExperimentResults,
  fetchOpenPlatformReplayExperimentArtifacts,
  fetchOpenPlatformReplayExperiments,
  fetchOpenPlatformReplayExperimentWorkflow,
  fetchOpenPlatformReplayArtifacts,
  fetchOpenPlatformReplayDatasetCases,
  fetchOpenPlatformReplayDatasetVersionCases,
  fetchOpenPlatformReplayDatasetVersions,
  fetchOpenPlatformReplayDatasets,
  fetchOpenPlatformReplayDiffs,
  fetchOpenPlatformReplayRuns,
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
  patchOpenPlatformReplayExperiment,
  replayOpenPlatformWebhook,
  runOpenPlatformReplayExperiment,
  replaceOpenPlatformReplayDatasetCases,
  promoteOpenPlatformReplayDatasetVersion,
  revokeOpenPlatformApiKey,
  searchSessions,
  setAuthTokens,
  setUnauthorizedHandler,
  simulateOpenPlatformAutomationPolicy,
  testSourceConnection,
  updateSource,
  updateAlertStatus,
  upsertMcpPolicy,
  upsertOpenPlatformAutomationPolicy,
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
    headers?: Record<string, string>;
  }
): Response {
  const status = options?.status ?? 200;
  const contentType = options?.contentType ?? "text/csv; charset=utf-8";
  const blob = new Blob([content], { type: contentType });
  const contentDisposition =
    options?.contentDisposition ?? 'attachment; filename="export.csv"';
  const extraHeaders: Record<string, string> = {};
  for (const [key, value] of Object.entries(options?.headers ?? {})) {
    extraHeaders[key.toLowerCase()] = value;
  }

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
        if (Object.prototype.hasOwnProperty.call(extraHeaders, normalized)) {
          return extraHeaders[normalized] ?? null;
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

  test("fetchSources 会保留 sourceRegion 字段", async () => {
    env.DEV = false;
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      mockJsonResponse({
        items: [
          {
            id: "source-region-1",
            name: "source-region-1",
            type: "local",
            location: "/workspace/source-region-1",
            sourceRegion: "cn-shanghai",
            enabled: true,
            createdAt: "2026-03-06T00:00:00.000Z",
          },
        ],
        total: 1,
      })
    );

    await expect(fetchSources()).resolves.toEqual({
      items: [
        {
          id: "source-region-1",
          name: "source-region-1",
          type: "local",
          location: "/workspace/source-region-1",
          sourceRegion: "cn-shanghai",
          enabled: true,
          createdAt: "2026-03-06T00:00:00.000Z",
        },
      ],
      total: 1,
    });
  });

  test("updateSource 与 backfillSourceRegions 走正确接口", async () => {
    env.DEV = false;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/sources/source-1") && method === "PATCH") {
        return mockJsonResponse({
          id: "source-1",
          name: "source-1",
          type: "local",
          location: "/workspace/source-1",
          sourceRegion: "cn-hangzhou",
          enabled: true,
          createdAt: "2026-03-06T00:00:00.000Z",
        });
      }

      if (url.endsWith("/api/v1/sources/source-region/backfill") && method === "POST") {
        return mockJsonResponse({
          tenantId: "default",
          dryRun: true,
          primaryRegion: "cn-shanghai",
          totalMissing: 1,
          updated: 0,
          skipped: 0,
          items: [
            {
              sourceId: "source-1",
              name: "source-1",
              status: "would_update",
              appliedRegion: "cn-shanghai",
            },
          ],
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(updateSource("source-1", { sourceRegion: "cn-hangzhou" })).resolves.toMatchObject({
      sourceRegion: "cn-hangzhou",
    });
    await expect(
      backfillSourceRegions({ dryRun: true, sourceIds: ["source-1"] })
    ).resolves.toMatchObject({
      dryRun: true,
      primaryRegion: "cn-shanghai",
    });

    expect(
      fetchSpy.mock.calls.some(
        ([url, init]) =>
          toUrl(url).endsWith("/api/v1/sources/source-1") &&
          (init as RequestInit | undefined)?.method === "PATCH"
      )
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(
        ([url, init]) =>
          toUrl(url).endsWith("/api/v1/sources/source-region/backfill") &&
          (init as RequestInit | undefined)?.method === "POST"
      )
    ).toBe(true);
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

    for (let attempt = 0; attempt < 20; attempt += 1) {
      if (expiredSourceCallCount === 2 && refreshCallCount === 1) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect(expiredSourceCallCount).toBe(2);
    expect(refreshCallCount).toBe(1);

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

  test("fetchAlertExternalLinkFailures 会携带查询参数并解析结果", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-alert-failures",
      refreshToken: "refresh-token-alert-failures",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      if (
        url.includes("/api/v1/alerts/external-links/failures") &&
        method === "GET"
      ) {
        return mockJsonResponse({
          summary: {
            total: 1,
            pending: 0,
            failed: 1,
          },
          items: [
            {
              id: "link-failure-1",
              alertId: "alert-3",
              alertStatus: "resolved",
              externalType: "ticket",
              externalSystem: "jira",
              externalId: "INC-1001",
              externalStatus: "acknowledged",
              pendingExternalStatus: "resolved",
              lastSyncedAt: "2026-03-01T11:00:00.000Z",
              publishStatus: "success",
              lastSyncResult: "failed",
              lastSyncError: "downstream timeout",
              lastSyncFailureStage: "dispatch_http",
              lastSyncFailureCode: "downstream_http_5xx",
              syncState: "failed",
              retryable: true,
              updatedAt: "2026-03-01T11:01:00.000Z",
            },
          ],
          filters: {
            alertId: "alert-3",
            externalSystem: "jira",
            syncState: "failed",
            limit: 10,
          },
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(
      fetchAlertExternalLinkFailures({
        alertId: "alert-3",
        externalSystem: "jira",
        syncState: "failed",
        limit: 10,
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        summary: expect.objectContaining({
          failed: 1,
        }),
        items: expect.arrayContaining([
          expect.objectContaining({
            alertId: "alert-3",
            externalSystem: "jira",
            syncState: "failed",
          }),
        ]),
      }),
    );

    const [requestUrl] = fetchSpy.mock.calls[0] ?? [];
    const parsedUrl = new URL(toUrl(requestUrl), "http://localhost");
    expect(parsedUrl.searchParams.get("alertId")).toBe("alert-3");
    expect(parsedUrl.searchParams.get("externalSystem")).toBe("jira");
    expect(parsedUrl.searchParams.get("syncState")).toBe("failed");
    expect(parsedUrl.searchParams.get("limit")).toBe("10");
  });

  test("Integration DLQ recovery job helper 支持查询、创建与详情加载", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-integration-dlq",
      refreshToken: "refresh-token-integration-dlq",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      if (url.includes("/api/v1/integrations/dlq/messages?") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              messageId: "INTEGRATION_DISPATCH_DLQ:101",
              stream: "INTEGRATION_DISPATCH_DLQ",
              subject: "integration.alert.external_status_sync",
              eventType: "alert_external_status_sync",
              channel: "ticket",
              callbackId: "sync-result:1",
              tenantId: "default",
              alertId: "alert-3",
              externalType: "ticket",
              externalId: "INC-1001",
              failedAt: "2026-03-01T11:00:00.000Z",
              attempt: 4,
              error: "downstream timeout",
              retryable: true,
              payload: {
                event_type: "alert_external_status_sync",
              },
            },
          ],
          total: 1,
          filters: {
            eventType: "alert_external_status_sync",
            channel: "ticket",
            alertId: "alert-3",
            callbackId: "sync-result:1",
            limit: 10,
          },
        });
      }
      if (url.endsWith("/api/v1/integrations/dlq/recovery-jobs") && method === "POST") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          messageIds: ["INTEGRATION_DISPATCH_DLQ:101"],
        });
        return mockJsonResponse({
          id: "dlq-job-1",
          tenantId: "default",
          status: "queued",
          requestedAt: "2026-03-01T11:10:00.000Z",
          messageIds: ["INTEGRATION_DISPATCH_DLQ:101"],
          summary: {
            total: 1,
            replayed: 0,
            failed: 0,
          },
          items: [],
        }, 202);
      }
      if (url.includes("/api/v1/integrations/dlq/recovery-jobs?") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "dlq-job-1",
              tenantId: "default",
              status: "completed",
              requestedAt: "2026-03-01T11:10:00.000Z",
              startedAt: "2026-03-01T11:10:01.000Z",
              finishedAt: "2026-03-01T11:10:02.000Z",
              messageIds: ["INTEGRATION_DISPATCH_DLQ:101"],
              summary: {
                total: 1,
                replayed: 1,
                failed: 0,
              },
              items: [
                {
                  messageId: "INTEGRATION_DISPATCH_DLQ:101",
                  status: "replayed",
                },
              ],
            },
          ],
          total: 1,
          filters: {
            limit: 10,
          },
        });
      }
      if (url.endsWith("/api/v1/integrations/dlq/recovery-jobs/dlq-job-1") && method === "GET") {
        return mockJsonResponse({
          id: "dlq-job-1",
          tenantId: "default",
          status: "completed",
          requestedAt: "2026-03-01T11:10:00.000Z",
          startedAt: "2026-03-01T11:10:01.000Z",
          finishedAt: "2026-03-01T11:10:02.000Z",
          messageIds: ["INTEGRATION_DISPATCH_DLQ:101"],
          summary: {
            total: 1,
            replayed: 1,
            failed: 0,
          },
          items: [
            {
              messageId: "INTEGRATION_DISPATCH_DLQ:101",
              status: "replayed",
            },
          ],
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(
      fetchIntegrationDlqMessages({
        eventType: "alert_external_status_sync",
        channel: "ticket",
        alertId: "alert-3",
        callbackId: "sync-result:1",
        limit: 10,
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        total: 1,
        items: expect.arrayContaining([
          expect.objectContaining({
            messageId: "INTEGRATION_DISPATCH_DLQ:101",
            alertId: "alert-3",
          }),
        ]),
      }),
    );

    await expect(
      createIntegrationDlqRecoveryJob({
        messageIds: ["INTEGRATION_DISPATCH_DLQ:101"],
      }),
    ).resolves.toEqual({
      id: "dlq-job-1",
      tenantId: "default",
      status: "queued",
      requestedAt: "2026-03-01T11:10:00.000Z",
      messageIds: ["INTEGRATION_DISPATCH_DLQ:101"],
      summary: {
        total: 1,
        replayed: 0,
        failed: 0,
      },
      items: [],
    });

    await expect(
      fetchIntegrationDlqRecoveryJobs({ limit: 10 }),
    ).resolves.toEqual(
      expect.objectContaining({
        total: 1,
        items: expect.arrayContaining([
          expect.objectContaining({
            id: "dlq-job-1",
            status: "completed",
          }),
        ]),
      }),
    );

    await expect(
      fetchIntegrationDlqRecoveryJobDetail("dlq-job-1"),
    ).resolves.toEqual({
      id: "dlq-job-1",
      tenantId: "default",
      status: "completed",
      requestedAt: "2026-03-01T11:10:00.000Z",
      startedAt: "2026-03-01T11:10:01.000Z",
      finishedAt: "2026-03-01T11:10:02.000Z",
      messageIds: ["INTEGRATION_DISPATCH_DLQ:101"],
      summary: {
        total: 1,
        replayed: 1,
        failed: 0,
      },
      items: [
        {
          messageId: "INTEGRATION_DISPATCH_DLQ:101",
          status: "replayed",
        },
      ],
    });

    expect(fetchSpy).toHaveBeenCalledTimes(4);
  });

  test("fetchIntegrationAlertFailureReport 支持查询与解析结果", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-failure-report",
      refreshToken: "refresh-token-failure-report",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      if (
        url.includes("/api/v1/integrations/failure-reports/alerts?") &&
        method === "GET"
      ) {
        return mockJsonResponse({
          summary: {
            totalEvents: 2,
            retryRequested: 1,
            retryCompleted: 0,
            retryFailed: 1,
            dlqQueried: 0,
            dlqReplayed: 0,
            recoveryJobsCreated: 0,
            recoveryJobsCompleted: 0,
            recoveryJobsFailed: 0,
          },
          items: [
            {
              occurredAt: "2026-03-01T11:00:00.000Z",
              action: "control_plane.alert_external_link_retry_failed",
              actionType: "retry_failed",
              alertId: "alert-1",
              externalSystem: "ticket",
              stage: "dispatch_http",
              code: "downstream_http_5xx",
              status: "failed",
              metadata: {},
            },
          ],
          filters: {
            externalSystem: "ticket",
            limit: 10,
          },
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(
      fetchIntegrationAlertFailureReport({
        externalSystem: "ticket",
        limit: 10,
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        summary: expect.objectContaining({
          retryFailed: 1,
        }),
        items: expect.arrayContaining([
          expect.objectContaining({
            actionType: "retry_failed",
            externalSystem: "ticket",
          }),
        ]),
      }),
    );
  });

  test("fetchIntegrationAlertFailureTrends 支持查询与解析结果", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-failure-trends",
      refreshToken: "refresh-token-failure-trends",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      if (
        url.includes("/api/v1/integrations/failure-reports/alerts/trends?") &&
        method === "GET"
      ) {
        return mockJsonResponse({
          summary: {
            totalEvents: 3,
            requestedEvents: 1,
            successEvents: 1,
            failedEvents: 1,
            days: 2,
            averageEventsPerDay: 1.5,
            peakDate: "2026-03-01",
            peakCount: 2,
          },
          daily: [
            {
              date: "2026-03-01",
              totalEvents: 2,
              requestedEvents: 0,
              successEvents: 1,
              failedEvents: 1,
              uniqueAlerts: 1,
              retryRequested: 0,
              retryCompleted: 0,
              retryFailed: 1,
              dlqQueried: 0,
              dlqReplayed: 0,
              recoveryJobsCreated: 0,
              recoveryJobsCompleted: 1,
              recoveryJobsFailed: 0,
            },
          ],
          capacity: {
            externalSystems: [
              {
                name: "ticket",
                totalEvents: 2,
                requestedEvents: 0,
                successEvents: 1,
                failedEvents: 1,
                uniqueAlerts: 1,
                lastOccurredAt: "2026-03-01T11:00:00.000Z",
              },
            ],
            stages: [
              {
                name: "dispatch_http",
                totalEvents: 1,
                requestedEvents: 0,
                successEvents: 0,
                failedEvents: 1,
                uniqueAlerts: 1,
                lastOccurredAt: "2026-03-01T10:00:00.000Z",
              },
            ],
          },
          filters: {
            externalSystem: "ticket",
            top: 5,
          },
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(
      fetchIntegrationAlertFailureTrends({
        externalSystem: "ticket",
        top: 5,
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        summary: expect.objectContaining({
          peakDate: "2026-03-01",
          peakCount: 2,
        }),
        capacity: expect.objectContaining({
          externalSystems: expect.arrayContaining([
            expect.objectContaining({
              name: "ticket",
              totalEvents: 2,
            }),
          ]),
        }),
      }),
    );
  });

  test("open platform 支持吊销 API Key、删除 Webhook 与回放 Webhook", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-open-platform-actions",
      refreshToken: "refresh-token-open-platform-actions",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/api-keys/ak-op-1/revoke") && method === "POST") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({ reason: "rotate-2026" });
        return mockJsonResponse({
          id: "ak-op-1",
          tenantId: "default",
          name: "release-bot",
          scope: "read",
          status: "revoked",
          keyPrefix: "sk_live_revoke",
          createdAt: "2026-03-01T00:00:00.000Z",
          updatedAt: "2026-03-02T00:00:00.000Z",
        });
      }

      if (url.endsWith("/api/v1/webhooks/wh-op-1") && method === "DELETE") {
        return mockJsonResponse({ success: true });
      }

      if (url.endsWith("/api/v1/webhooks/wh-op-2/replay") && method === "POST") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          eventType: "replay.run.cancelled",
          limit: 20,
          dryRun: true,
        });
        return mockJsonResponse({
          id: "replay-wh-op-2",
          webhookId: "wh-op-2",
          status: "queued",
          dryRun: true,
          filters: {
            eventType: "replay.run.cancelled",
            limit: 20,
          },
          requestedAt: "2026-03-03T12:30:00.000Z",
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(revokeOpenPlatformApiKey("ak-op-1", "rotate-2026")).resolves.toEqual(
      expect.objectContaining({
        id: "ak-op-1",
        status: "disabled",
      })
    );
    await expect(deleteOpenPlatformWebhook("wh-op-1")).resolves.toBeUndefined();
    await expect(
      replayOpenPlatformWebhook("wh-op-2", {
        eventType: "replay.run.cancelled",
        limit: 20,
        dryRun: true,
      })
    ).resolves.toEqual(
      expect.objectContaining({
        id: "replay-wh-op-2",
        webhookId: "wh-op-2",
        status: "queued",
        dryRun: true,
      })
    );

    expect(
      fetchSpy.mock.calls.some(([url]) =>
        toUrl(url).endsWith("/api/v1/api-keys/ak-op-1/revoke")
      )
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) => toUrl(url).endsWith("/api/v1/webhooks/wh-op-1"))
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) =>
        toUrl(url).endsWith("/api/v1/webhooks/wh-op-2/replay")
      )
    ).toBe(true);
  });

  test("open platform 支持 quality project-trends 与 replay v2 create/artifacts/download", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-open-platform-v2",
      refreshToken: "refresh-token-open-platform-v2",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = new URL(toUrl(input), "http://localhost");
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.pathname === "/api/v2/quality/reports/project-trends" && method === "GET") {
        expect(url.searchParams.get("from")).toBe("2026-03-01");
        expect(url.searchParams.get("to")).toBe("2026-03-03");
        expect(url.searchParams.get("metric")).toBe("accuracy");
        expect(url.searchParams.get("provider")).toBe("github");
        expect(url.searchParams.get("workflow")).toBe("nightly");
        expect(url.searchParams.get("includeUnknown")).toBe("true");
        expect(url.searchParams.get("limit")).toBe("10");
        return mockJsonResponse({
          items: [
            {
              project: "agentledger/main",
              metric: "accuracy",
              totalEvents: 8,
              passedEvents: 7,
              failedEvents: 1,
              passRate: 0.875,
              avgScore: 92.4,
              totalCost: 12.8,
              totalTokens: 42000,
              totalSessions: 11,
              costPerQualityPoint: 0.1385,
            },
          ],
          total: 1,
          summary: {
            metric: "accuracy",
            totalEvents: 8,
            passedEvents: 7,
            failedEvents: 1,
            passRate: 0.875,
            avgScore: 92.4,
            totalCost: 12.8,
            totalTokens: 42000,
            totalSessions: 11,
            from: "2026-03-01T00:00:00.000Z",
            to: "2026-03-03T23:59:59.999Z",
          },
          filters: {
            from: "2026-03-01",
            to: "2026-03-03",
            metric: "accuracy",
            provider: "github",
            workflow: "nightly",
            includeUnknown: true,
            limit: 10,
          },
        });
      }

      if (url.pathname === "/api/v2/replay/datasets" && method === "POST") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          name: "baseline smoke",
          datasetRef: "dataset-op-1",
          model: "gpt-5-codex",
          promptVersion: "v3",
          sampleCount: 20,
        });
        return mockJsonResponse(
          {
            id: "baseline-op-1",
            tenantId: "default",
            name: "baseline smoke",
            datasetId: "dataset-op-1",
            model: "gpt-5-codex",
            promptVersion: "v3",
            caseCount: 20,
            sampleCount: 20,
            metadata: {},
            createdAt: "2026-03-03T12:10:00.000Z",
            updatedAt: "2026-03-03T12:10:00.000Z",
          },
          201
        );
      }

      if (url.pathname === "/api/v2/replay/datasets" && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "baseline-op-1",
              tenantId: "default",
              name: "baseline smoke",
              datasetId: "dataset-op-1",
              model: "gpt-5-codex",
              promptVersion: "v3",
              caseCount: 20,
              sampleCount: 20,
              metadata: {},
              createdAt: "2026-03-03T12:10:00.000Z",
              updatedAt: "2026-03-03T12:10:00.000Z",
            },
          ],
          total: 1,
          filters: {},
        });
      }

      if (url.pathname === "/api/v2/replay/datasets/baseline-op-1/versions" && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "baseline-op-1:v1",
              tenantId: "default",
              datasetId: "dataset-op-1",
              version: 1,
              model: "gpt-5-codex",
              promptVersion: "v3",
              sampleCount: 20,
              metadata: {},
              createdAt: "2026-03-03T12:10:00.000Z",
              promotedAt: "2026-03-03T12:10:00.000Z",
            },
          ],
          total: 1,
          currentVersionId: "baseline-op-1:v1",
          currentVersionNumber: 1,
        });
      }

      if (url.pathname === "/api/v2/replay/datasets/baseline-op-1/versions" && method === "POST") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          datasetRef: "dataset-op-2",
          model: "gpt-5-codex-mini",
          promptVersion: "v4",
          sampleCount: 12,
          note: "candidate rollout",
        });
        return mockJsonResponse(
          {
            id: "baseline-op-1:v2",
            tenantId: "default",
            datasetId: "dataset-op-2",
            baselineId: "baseline-op-1",
            version: 2,
            model: "gpt-5-codex-mini",
            promptVersion: "v4",
            sampleCount: 12,
            metadata: { rollout: "candidate" },
            note: "candidate rollout",
            createdAt: "2026-03-03T12:11:00.000Z",
            promotedAt: null,
          },
          201,
        );
      }

      if (url.pathname === "/api/v2/replay/datasets/baseline-op-1/promote" && method === "POST") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          versionId: "baseline-op-1:v2",
        });
        return mockJsonResponse({
          dataset: {
            id: "baseline-op-1",
            tenantId: "default",
            name: "baseline smoke",
            datasetId: "dataset-op-2",
            model: "gpt-5-codex-mini",
            promptVersion: "v4",
            caseCount: 12,
            sampleCount: 12,
            currentVersionId: "baseline-op-1:v2",
            currentVersionNumber: 2,
            metadata: { rollout: "candidate" },
            createdAt: "2026-03-03T12:10:00.000Z",
            updatedAt: "2026-03-03T12:12:00.000Z",
          },
          version: {
            id: "baseline-op-1:v2",
            tenantId: "default",
            datasetId: "dataset-op-2",
            version: 2,
            model: "gpt-5-codex-mini",
            promptVersion: "v4",
            sampleCount: 12,
            metadata: { rollout: "candidate" },
            note: "candidate rollout",
            createdAt: "2026-03-03T12:11:00.000Z",
            promotedAt: "2026-03-03T12:12:00.000Z",
          },
        });
      }

      if (url.pathname === "/api/v2/replay/datasets/baseline-op-1/cases" && method === "GET") {
        return mockJsonResponse({
          datasetId: "baseline-op-1",
          items: [
            {
              datasetId: "baseline-op-1",
              caseId: "case-1",
              sortOrder: 0,
              input: "What changed?",
              expectedOutput: "A concise summary",
              metadata: { priority: "p0" },
              createdAt: "2026-03-03T12:12:00.000Z",
              updatedAt: "2026-03-03T12:12:00.000Z",
            },
          ],
          total: 1,
        });
      }

      if (url.pathname === "/api/v2/replay/datasets/baseline-op-1/cases" && method === "POST") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          items: [
            {
              caseId: "case-1",
              sortOrder: 0,
              input: "What changed?",
              expectedOutput: "A concise summary",
              metadata: { priority: "p0" },
            },
          ],
        });
        return mockJsonResponse({
          datasetId: "baseline-op-1",
          items: [
            {
              datasetId: "baseline-op-1",
              caseId: "case-1",
              sortOrder: 0,
              input: "What changed?",
              expectedOutput: "A concise summary",
              metadata: { priority: "p0" },
              createdAt: "2026-03-03T12:12:00.000Z",
              updatedAt: "2026-03-03T12:12:00.000Z",
            },
          ],
          total: 1,
        });
      }

      if (url.pathname === "/api/v2/replay/runs" && method === "POST") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          datasetId: "baseline-op-1",
          baselineVersionId: "baseline-op-1:v2",
          candidateLabel: "candidate-v3",
          sampleLimit: 20,
        });
        return mockJsonResponse(
          {
            id: "run-op-1",
            runId: "run-op-1",
            jobId: "run-op-1",
            tenantId: "default",
            datasetId: "baseline-op-1",
            baselineId: "baseline-op-1",
            baselineVersionId: "baseline-op-1:v2",
            candidateLabel: "candidate-v3",
            status: "pending",
            totalCases: 20,
            processedCases: 0,
            improvedCases: 0,
            regressedCases: 0,
            unchangedCases: 0,
            summary: {
              baselineVersionId: "baseline-op-1:v2",
            },
            diffs: [],
            createdAt: "2026-03-03T12:20:00.000Z",
            updatedAt: "2026-03-03T12:20:00.000Z",
          },
          201
        );
      }

      if (url.pathname === "/api/v2/replay/runs" && method === "GET") {
        expect(url.searchParams.get("status")).toBe("cancelled");
        expect(url.searchParams.get("datasetId")).toBe("baseline-op-1");
        expect(url.searchParams.get("baselineId")).toBeNull();
        return mockJsonResponse({
          items: [
            {
              id: "run-op-cancelled",
              runId: "run-op-cancelled",
              jobId: "run-op-cancelled",
              tenantId: "default",
              datasetId: "baseline-op-1",
              baselineId: "baseline-op-1",
              candidateLabel: "candidate-v3",
              status: "cancelled",
              totalCases: 10,
              processedCases: 6,
              improvedCases: 3,
              regressedCases: 2,
              unchangedCases: 1,
              createdAt: "2026-03-03T12:30:00.000Z",
              updatedAt: "2026-03-03T12:35:00.000Z",
              finishedAt: "2026-03-03T12:35:00.000Z",
            },
          ],
          total: 1,
          filters: {
            status: "cancelled",
          },
        });
      }

      if (url.pathname === "/api/v2/replay/runs/run-op-1/diffs" && method === "GET") {
        expect(url.searchParams.get("datasetId")).toBe("baseline-op-1");
        expect(url.searchParams.get("baselineId")).toBeNull();
        expect(url.searchParams.get("keyword")).toBe("case-1");
        expect(url.searchParams.get("limit")).toBe("10");
        return mockJsonResponse({
          runId: "run-op-1",
          jobId: "run-op-1",
          datasetId: "baseline-op-1",
          diffs: [
            {
              caseId: "case-1",
              metric: "accuracy",
              baselineScore: 0.72,
              candidateScore: 0.9,
              delta: 0.18,
              verdict: "improved",
              detail: "answer quality improved",
            },
          ],
          total: 1,
          summary: {
            totalCases: 20,
          },
          filters: {
            datasetId: "baseline-op-1",
            baselineId: "baseline-op-1",
            runId: "run-op-1",
            jobId: "run-op-1",
            keyword: "case-1",
            limit: 10,
          },
        });
      }

      if (url.pathname === "/api/v2/replay/datasets/baseline-op-1/versions/baseline-op-1%3Av2/cases" && method === "GET") {
        return mockJsonResponse({
          datasetId: "baseline-op-1",
          versionId: "baseline-op-1:v2",
          items: [
            {
              datasetId: "baseline-op-1",
              caseId: "case-v2-1",
              sortOrder: 0,
              input: "What changed in v2?",
              expectedOutput: "A concise delta summary",
              metadata: { version: "v2" },
              createdAt: "2026-03-03T12:11:00.000Z",
              updatedAt: "2026-03-03T12:11:00.000Z",
            },
          ],
          total: 1,
        });
      }

      if (url.pathname === "/api/v2/replay/runs/run-op-1/artifacts" && method === "GET") {
        return mockJsonResponse({
          runId: "run-op-1",
          datasetId: "baseline-op-1",
          items: [
            {
              type: "summary",
              name: "summary.json",
              contentType: "application/json",
              downloadName: "summary.json",
              downloadUrl: "/api/v2/replay/runs/run-op-1/artifacts/summary/download",
              byteSize: 128,
              checksum: "sha256:summary",
              storageBackend: "local",
              storageKey: "/tmp/run-op-1/summary.json",
              metadata: { source: "run" },
              createdAt: "2026-03-03T12:25:00.000Z",
              inline: { totalCases: 20 },
            },
            {
              type: "diff",
              name: "diff.json",
              contentType: "application/json",
              downloadName: "diff.json",
              downloadUrl: "/api/v2/replay/runs/run-op-1/artifacts/diff/download",
              byteSize: 256,
              createdAt: "2026-03-03T12:25:00.000Z",
              inline: { items: [] },
            },
            {
              type: "cases",
              name: "cases.json",
              contentType: "application/json",
              downloadName: "cases.json",
              downloadUrl: "/api/v2/replay/runs/run-op-1/artifacts/cases/download",
              byteSize: 320,
              createdAt: "2026-03-03T12:25:00.000Z",
              inline: { items: [{ caseId: "case-1" }] },
            },
          ],
          total: 3,
        });
      }

      if (url.pathname === "/api/v2/replay/experiments/exp-1/artifacts" && method === "GET") {
        return mockJsonResponse({
          experimentId: "exp-1",
          datasetId: "baseline-op-1",
          items: [
            {
              runId: "run-op-1",
              type: "summary",
              name: "summary.json",
              contentType: "application/json",
              downloadName: "summary.json",
              downloadUrl: "/api/v2/replay/runs/run-op-1/artifacts/summary/download",
              byteSize: 128,
              checksum: "sha256:summary",
              storageBackend: "local",
              storageKey: "/tmp/run-op-1/summary.json",
              metadata: { source: "experiment" },
              createdAt: "2026-03-03T12:25:00.000Z",
              inline: { totalCases: 20 },
            },
          ],
          total: 1,
        });
      }

      if (url.pathname === "/api/v2/replay/runs/run-op-1/artifacts/summary/download" && method === "GET") {
        return mockFileResponse('{"totalCases":20}', {
          contentType: "application/json",
          contentDisposition: 'attachment; filename="summary.json"',
        });
      }

      throw new Error(`unexpected call: ${method} ${url.pathname}`);
    });

    await expect(
      fetchOpenPlatformQualityProjectTrends({
        from: "2026-03-01",
        to: "2026-03-03",
        metric: "accuracy",
        provider: "github",
        workflow: "nightly",
        includeUnknown: true,
        limit: 10,
      })
    ).resolves.toEqual(
      expect.objectContaining({
        total: 1,
        items: [expect.objectContaining({ project: "agentledger/main", totalSessions: 11 })],
        summary: expect.objectContaining({ totalTokens: 42000 }),
      })
    );

    await expect(
      fetchOpenPlatformReplayDatasets({
        keyword: "baseline",
        limit: 20,
      })
    ).resolves.toEqual(
      expect.objectContaining({
        total: 1,
        items: [
          expect.objectContaining({
            id: "baseline-op-1",
            datasetId: "baseline-op-1",
            datasetRef: "dataset-op-1",
            promptVersion: "v3",
            sampleCount: 20,
            caseCount: 20,
          }),
        ],
      })
    );

    await expect(
      createOpenPlatformReplayDataset({
        name: "baseline smoke",
        datasetRef: "dataset-op-1",
        model: "gpt-5-codex",
        promptVersion: "v3",
        sampleCount: 20,
      })
    ).resolves.toEqual(
      expect.objectContaining({
        id: "baseline-op-1",
        datasetId: "baseline-op-1",
        datasetRef: "dataset-op-1",
        promptVersion: "v3",
        sampleCount: 20,
      })
    );

    await expect(fetchOpenPlatformReplayDatasetVersions("baseline-op-1")).resolves.toEqual(
      expect.objectContaining({
        datasetId: "baseline-op-1",
        total: 1,
        currentVersionId: "baseline-op-1:v1",
        currentVersionNumber: 1,
        items: [
          expect.objectContaining({
            id: "baseline-op-1:v1",
            version: 1,
            datasetId: "baseline-op-1",
            model: "gpt-5-codex",
          }),
        ],
      }),
    );

    await expect(
      createOpenPlatformReplayDatasetVersion("baseline-op-1", {
        datasetRef: "dataset-op-2",
        model: "gpt-5-codex-mini",
        promptVersion: "v4",
        sampleCount: 12,
        note: "candidate rollout",
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        id: "baseline-op-1:v2",
        version: 2,
        datasetId: "baseline-op-1",
        datasetRef: "dataset-op-2",
        model: "gpt-5-codex-mini",
        note: "candidate rollout",
      }),
    );

    await expect(
      promoteOpenPlatformReplayDatasetVersion("baseline-op-1", {
        versionId: "baseline-op-1:v2",
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        dataset: expect.objectContaining({
          datasetId: "baseline-op-1",
          currentVersionId: "baseline-op-1:v2",
          currentVersionNumber: 2,
        }),
        version: expect.objectContaining({
          id: "baseline-op-1:v2",
          version: 2,
          promotedAt: "2026-03-03T12:12:00.000Z",
        }),
      }),
    );

    await expect(
      createOpenPlatformReplayRun({
        datasetId: "baseline-op-1",
        baselineVersionId: "baseline-op-1:v2",
        candidateLabel: "candidate-v3",
        sampleLimit: 20,
      })
    ).resolves.toEqual(
      expect.objectContaining({
        id: "run-op-1",
        runId: "run-op-1",
        jobId: "run-op-1",
        datasetId: "baseline-op-1",
        baselineId: "baseline-op-1",
        baselineVersionId: "baseline-op-1:v2",
        status: "pending",
        totalCases: 20,
      })
    );

    await expect(
      fetchOpenPlatformReplayDatasetVersionCases("baseline-op-1", "baseline-op-1:v2")
    ).resolves.toEqual(
      expect.objectContaining({
        datasetId: "baseline-op-1",
        versionId: "baseline-op-1:v2",
        total: 1,
        items: [expect.objectContaining({ caseId: "case-v2-1" })],
      })
    );

    await expect(
      fetchOpenPlatformReplayDatasetCases("baseline-op-1")
    ).resolves.toEqual(
      expect.objectContaining({
        datasetId: "baseline-op-1",
        total: 1,
        items: [expect.objectContaining({ caseId: "case-1", sortOrder: 0 })],
      })
    );

    await expect(
      replaceOpenPlatformReplayDatasetCases("baseline-op-1", {
        items: [
          {
            caseId: "case-1",
            sortOrder: 0,
            input: "What changed?",
            expectedOutput: "A concise summary",
            metadata: { priority: "p0" },
          },
        ],
      })
    ).resolves.toEqual(
      expect.objectContaining({
        datasetId: "baseline-op-1",
        total: 1,
        items: [expect.objectContaining({ caseId: "case-1", expectedOutput: "A concise summary" })],
      })
    );

    await expect(
      fetchOpenPlatformReplayRuns({
        baselineId: "baseline-op-1",
        status: "cancelled",
        limit: 20,
      })
    ).resolves.toEqual(
      expect.objectContaining({
        items: [
          expect.objectContaining({
            id: "run-op-cancelled",
            runId: "run-op-cancelled",
            jobId: "run-op-cancelled",
            datasetId: "baseline-op-1",
            baselineId: "baseline-op-1",
            status: "cancelled",
          }),
        ],
      })
    );

    await expect(
      fetchOpenPlatformReplayDiffs({
        baselineId: "baseline-op-1",
        jobId: "run-op-1",
        keyword: "case-1",
        limit: 10,
      })
    ).resolves.toEqual(
      expect.objectContaining({
        total: 1,
        items: [
          expect.objectContaining({
            caseId: "case-1",
            datasetId: "baseline-op-1",
            baselineId: "baseline-op-1",
            runId: "run-op-1",
            jobId: "run-op-1",
            verdict: "improved",
          }),
        ],
      })
    );

    await expect(fetchOpenPlatformReplayArtifacts("run-op-1")).resolves.toEqual(
      expect.objectContaining({
        runId: "run-op-1",
        jobId: "run-op-1",
        datasetId: "baseline-op-1",
        total: 3,
        items: expect.arrayContaining([
          expect.objectContaining({
            type: "summary",
            byteSize: 128,
            checksum: "sha256:summary",
            storageBackend: "local",
            downloadUrl: "/api/v2/replay/runs/run-op-1/artifacts/summary/download",
          }),
          expect.objectContaining({
            type: "cases",
            byteSize: 320,
            downloadUrl: "/api/v2/replay/runs/run-op-1/artifacts/cases/download",
          }),
        ]),
      })
    );

    await expect(
      fetchOpenPlatformReplayExperimentArtifacts("exp-1")
    ).resolves.toEqual(
      expect.objectContaining({
        experimentId: "exp-1",
        datasetId: "baseline-op-1",
        total: 1,
        items: [expect.objectContaining({ runId: "run-op-1", type: "summary" })],
      })
    );

    const replayArtifactFile = await downloadOpenPlatformReplayArtifact("run-op-1", "summary");
    expect(replayArtifactFile.filename).toBe("summary.json");
    expect(replayArtifactFile.contentType).toBe("application/json");
    expect(replayArtifactFile.blob).toBeInstanceOf(Blob);
    expect(replayArtifactFile.blob.size).toBeGreaterThan(0);

    expect(
      fetchSpy.mock.calls.some(([url]) =>
        new URL(toUrl(url), "http://localhost").pathname === "/api/v2/quality/reports/project-trends"
      )
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) =>
        new URL(toUrl(url), "http://localhost").pathname ===
        "/api/v2/replay/runs/run-op-1/artifacts/summary/download"
      )
    ).toBe(true);
  });

  test("open platform 支持 quality advice execution 与 replay experiment workflow", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-open-platform-advanced",
      refreshToken: "refresh-token-open-platform-advanced",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = new URL(toUrl(input), "http://localhost");
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.pathname === "/api/v2/quality/automation-policy" && method === "GET") {
        return mockJsonResponse({
          tenantId: "default",
          toolId: "quality.replay.advice.execute",
          scope: "quality_replay_advice",
          riskLevel: "high",
          decision: "require_approval",
          reason: "质量高风险命中矩阵时需要审批",
          evaluationScoreThreshold: 78,
          triggerOnEvaluationFailure: true,
          triggerOnReplayRegression: true,
          defaultActionType: "scorecard_adjustment",
          strategyMatrix: [
            {
              id: "critical-replay",
              metric: "accuracy",
              severity: "critical",
              trendDirection: "down",
              provider: "github",
              workflow: "ci-main",
              projectPattern: "agentledger/*",
              minConfidence: 0.6,
              regressionProbabilityAtLeast: 0.5,
              replayRegressionAtLeast: 1,
              actionType: "replay_experiment",
              requiresApproval: true,
              cooldownMinutes: 30,
              reason: "高风险优先回放",
            },
          ],
          updatedAt: "2026-03-08T09:59:00.000Z",
        });
      }
      if (url.pathname === "/api/v2/quality/automation-policy" && method === "PUT") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          riskLevel: "high",
          decision: "require_approval",
          reason: "质量高风险命中矩阵时需要审批",
          evaluationScoreThreshold: 78,
          triggerOnEvaluationFailure: true,
          triggerOnReplayRegression: true,
          strategyMatrix: [
            {
              id: "critical-replay",
              metric: "accuracy",
              severity: "critical",
              trendDirection: "down",
              provider: "github",
              workflow: "ci-main",
              projectPattern: "agentledger/*",
              minConfidence: 0.6,
              regressionProbabilityAtLeast: 0.5,
              replayRegressionAtLeast: 1,
              actionType: "replay_experiment",
              requiresApproval: true,
              cooldownMinutes: 30,
              reason: "高风险优先回放",
            },
          ],
        });
        return mockJsonResponse({
          tenantId: "default",
          toolId: "quality.replay.advice.execute",
          scope: "quality_replay_advice",
          riskLevel: "high",
          decision: "require_approval",
          reason: "质量高风险命中矩阵时需要审批",
          evaluationScoreThreshold: 78,
          triggerOnEvaluationFailure: true,
          triggerOnReplayRegression: true,
          defaultActionType: "scorecard_adjustment",
          strategyMatrix: [
            {
              id: "critical-replay",
              metric: "accuracy",
              severity: "critical",
              trendDirection: "down",
              provider: "github",
              workflow: "ci-main",
              projectPattern: "agentledger/*",
              minConfidence: 0.6,
              regressionProbabilityAtLeast: 0.5,
              replayRegressionAtLeast: 1,
              actionType: "replay_experiment",
              requiresApproval: true,
              cooldownMinutes: 30,
              reason: "高风险优先回放",
            },
          ],
          updatedAt: "2026-03-08T10:00:00.000Z",
        });
      }
      if (
        url.pathname === "/api/v2/quality/automation-policy/simulate" &&
        method === "POST"
      ) {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          metric: "accuracy",
          score: 63,
          trendDirection: "down",
          confidence: 0.82,
          regressionProbability: 0.73,
          replayRegressionCount: 2,
        });
        return mockJsonResponse({
          metric: "accuracy",
          severity: "critical",
          confidence: 0.82,
          trendDirection: "down",
          regressionProbability: 0.73,
          replayRegressionCount: 2,
          matchedRuleId: "critical-replay",
          resolvedAction: "replay_experiment",
          requiresApproval: true,
          blockingReasons: [],
        });
      }
      if (url.pathname === "/api/v2/quality/reports/forecast" && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              project: "agentledger/main",
              metric: "accuracy",
              modelVersion: "quality-heuristic-v2",
              forecastHorizonDays: 7,
              predictedScore: 88.5,
              expectedScoreRange: {
                lower: 86.3,
                upper: 90.7,
              },
              confidence: 0.82,
              trendDirection: "down",
              regressionProbability: 0.61,
              rationale: "passRate 回落",
              featureContributions: [
                {
                  feature: "passRateGap",
                  impact: -0.52,
                  direction: "negative",
                },
              ],
              windowComparisons: {
                currentWindow: { averageScore: 88.5 },
                previousWindow: { averageScore: 91.1 },
              },
              basis: {},
            },
          ],
          total: 1,
          filters: {},
        });
      }
      if (url.pathname === "/api/v2/quality/reports/advice" && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "advice-1",
              project: "agentledger/main",
              severity: "warn",
              title: "质量波动",
              recommendation: "建议执行回放实验",
              explanation: "质量开始回落，建议按矩阵先补回放。",
              strategyMatrixMatch: "critical-replay",
              autoExecutionDecision: "approval_required",
              blockingReasons: ["dataset_required_for_replay_experiment"],
              basis: {},
              relatedMetrics: ["avgScore"],
              suggestedActions: ["replay_experiment"],
              latestExecutionStatus: "completed",
            },
          ],
          total: 1,
          filters: {},
        });
      }
      if (url.pathname === "/api/v2/quality/advice/advice-1/execute" && method === "POST") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          project: "agentledger/main",
          severity: "warn",
          actionType: "replay_experiment",
          datasetId: "dataset-op-1",
          candidateLabels: ["candidate-a", "candidate-b"],
        });
        return mockJsonResponse(
          {
            id: "advice-exec-1",
            tenantId: "default",
            adviceId: "advice-1",
            project: "agentledger/main",
            severity: "warn",
            actionType: "replay_experiment",
            triggerSource: "manual",
            status: "completed",
            datasetId: "dataset-op-1",
            experimentId: "exp-1",
            candidateLabels: ["candidate-a", "candidate-b"],
            resultSummary: { experimentId: "exp-1" },
            requestedAt: "2026-03-08T10:00:00.000Z",
            startedAt: "2026-03-08T10:00:01.000Z",
            finishedAt: "2026-03-08T10:00:02.000Z",
            updatedAt: "2026-03-08T10:00:02.000Z",
          },
          201,
        );
      }
      if (url.pathname === "/api/v2/quality/advice/executions" && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "advice-exec-1",
              tenantId: "default",
              adviceId: "advice-1",
              project: "agentledger/main",
              severity: "warn",
              actionType: "replay_experiment",
              triggerSource: "manual",
              status: "completed",
              experimentId: "exp-1",
              requestedAt: "2026-03-08T10:00:00.000Z",
              updatedAt: "2026-03-08T10:00:02.000Z",
            },
          ],
          total: 1,
          filters: {},
        });
      }
      if (url.pathname === "/api/v2/quality/advice/executions/advice-exec-1/cancel" && method === "POST") {
        return mockJsonResponse({
          id: "advice-exec-1",
          tenantId: "default",
          adviceId: "advice-1",
          project: "agentledger/main",
          severity: "warn",
          actionType: "replay_experiment",
          triggerSource: "manual",
          status: "cancelled",
          requestedAt: "2026-03-08T10:00:00.000Z",
          updatedAt: "2026-03-08T10:00:03.000Z",
        });
      }
      if (url.pathname === "/api/v2/replay/experiments" && method === "POST") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          name: "Experiment",
          datasetId: "dataset-op-1",
          baselineVersionId: "dataset-op-1:v2",
          candidateLabels: ["candidate-a", "candidate-b"],
          autoRun: true,
          triggerSource: "quality_advice",
        });
        return mockJsonResponse(
          {
            id: "exp-1",
            tenantId: "default",
            name: "Experiment",
            datasetId: "dataset-op-1",
            baselineVersionId: "dataset-op-1:v2",
            status: "queued",
            triggerSource: "quality_advice",
            executionMode: "automatic",
            candidateLabels: ["candidate-a", "candidate-b"],
            runIds: ["run-1", "run-2"],
            summary: {},
            runs: [],
            createdAt: "2026-03-08T10:00:00.000Z",
            updatedAt: "2026-03-08T10:00:00.000Z",
          },
          201,
        );
      }
      if (url.pathname === "/api/v2/replay/experiments" && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "exp-1",
              tenantId: "default",
              name: "Experiment",
              datasetId: "dataset-op-1",
              baselineVersionId: "dataset-op-1:v2",
              status: "queued",
              triggerSource: "quality_advice",
              executionMode: "automatic",
              candidateLabels: ["candidate-a", "candidate-b"],
              runIds: ["run-1", "run-2"],
              summary: {},
              runs: [],
              createdAt: "2026-03-08T10:00:00.000Z",
              updatedAt: "2026-03-08T10:00:00.000Z",
            },
          ],
          total: 1,
        });
      }
      if (url.pathname === "/api/v2/replay/experiments/exp-1" && method === "PATCH") {
        expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
          name: "Experiment Renamed",
          candidateLabels: ["candidate-a", "candidate-b", "candidate-c"],
        });
        return mockJsonResponse({
          id: "exp-1",
          tenantId: "default",
          name: "Experiment Renamed",
          datasetId: "dataset-op-1",
          baselineVersionId: "dataset-op-1:v2",
          status: "queued",
          triggerSource: "quality_advice",
          executionMode: "automatic",
          candidateLabels: ["candidate-a", "candidate-b", "candidate-c"],
          runIds: ["run-1", "run-2"],
          summary: {},
          runs: [],
          createdAt: "2026-03-08T10:00:00.000Z",
          updatedAt: "2026-03-08T10:00:03.000Z",
        });
      }
      if (url.pathname === "/api/v2/replay/experiments/compare" && method === "GET") {
        expect(url.searchParams.get("experimentIds")).toBe("exp-1,exp-2");
        expect(url.searchParams.get("datasetId")).toBe("dataset-op-1");
        return mockJsonResponse({
          items: [
            {
              experimentId: "exp-1",
              name: "Experiment",
              datasetId: "dataset-op-1",
              status: "completed",
              workflowStage: "completed",
              triggerSource: "quality_advice",
              sourceAdviceId: "advice-1",
              candidateLabels: ["candidate-a", "candidate-b"],
              totalRuns: 2,
              completedRuns: 2,
              failedRuns: 0,
              runningRuns: 0,
              queuedRuns: 0,
              totalCases: 24,
              processedCases: 24,
              improvedCases: 8,
              regressedCases: 2,
              improvementRate: 0.3333,
              regressionRate: 0.0833,
              netDelta: 6,
              bestRunId: "run-1",
              worstRunId: "run-2",
              runs: [],
              updatedAt: "2026-03-08T10:10:00.000Z",
            },
            {
              experimentId: "exp-2",
              name: "Experiment 2",
              datasetId: "dataset-op-1",
              status: "completed",
              workflowStage: "completed",
              triggerSource: "manual",
              sourceAdviceId: null,
              candidateLabels: ["candidate-c"],
              totalRuns: 1,
              completedRuns: 1,
              failedRuns: 0,
              runningRuns: 0,
              queuedRuns: 0,
              totalCases: 12,
              processedCases: 12,
              improvedCases: 2,
              regressedCases: 4,
              improvementRate: 0.1667,
              regressionRate: 0.3333,
              netDelta: -2,
              bestRunId: "run-3",
              worstRunId: "run-3",
              runs: [],
              updatedAt: "2026-03-08T10:12:00.000Z",
            },
          ],
          total: 2,
          summary: {
            comparedExperimentCount: 2,
            comparedAt: "2026-03-08T10:15:00.000Z",
            datasets: ["dataset-op-1"],
            totalRuns: 3,
            completedRuns: 3,
            failedRuns: 0,
            runningRuns: 0,
            queuedRuns: 0,
            totalCases: 36,
            processedCases: 36,
            improvedCases: 10,
            regressedCases: 6,
            bestExperimentId: "exp-1",
            worstExperimentId: "exp-2",
          },
          filters: {
            experimentIds: ["exp-1", "exp-2"],
            datasetId: "dataset-op-1",
          },
        });
      }
      if (url.pathname === "/api/v2/replay/experiments/exp-1/results" && method === "GET") {
        return mockJsonResponse({
          id: "exp-1",
          tenantId: "default",
          name: "Experiment",
          datasetId: "dataset-op-1",
          status: "completed",
          triggerSource: "quality_advice",
          executionMode: "automatic",
          candidateLabels: ["candidate-a", "candidate-b"],
          runIds: ["run-1", "run-2"],
          summary: { totalRuns: 2 },
          runs: [],
          createdAt: "2026-03-08T10:00:00.000Z",
          updatedAt: "2026-03-08T10:10:00.000Z",
        });
      }
      if (url.pathname === "/api/v2/replay/experiments/exp-1/compare" && method === "GET") {
        return mockJsonResponse({
          experimentId: "exp-1",
          datasetId: "dataset-op-1",
          items: [
            {
              runId: "run-1",
              candidateLabel: "candidate-a",
              status: "completed",
              totalCases: 12,
              processedCases: 12,
              improvedCases: 4,
              regressedCases: 1,
              unchangedCases: 7,
              passRate: 0.9167,
              improvementRate: 0.3333,
              regressionRate: 0.0833,
              netDelta: 3,
              startedAt: "2026-03-08T10:00:00.000Z",
              finishedAt: "2026-03-08T10:02:00.000Z",
            },
          ],
          total: 1,
          summary: {
            totalRuns: 1,
            completedRuns: 1,
            failedRuns: 0,
            runningRuns: 0,
            queuedRuns: 0,
            cancelledRuns: 0,
            bestRunId: "run-1",
            worstRunId: "run-1",
            bestNetDelta: 3,
            worstNetDelta: 3,
          },
        });
      }
      if (url.pathname === "/api/v2/replay/experiments/exp-1/workflow" && method === "GET") {
        return mockJsonResponse({
          experimentId: "exp-1",
          status: "completed",
          nodes: [
            {
              id: "experiment:exp-1",
              type: "experiment",
              label: "Experiment",
              status: "completed",
              startedAt: "2026-03-08T10:00:00.000Z",
              finishedAt: "2026-03-08T10:10:00.000Z",
              metadata: {
                datasetId: "dataset-op-1",
              },
            },
            {
              id: "run:run-1",
              type: "run",
              label: "候选 candidate-a",
              status: "completed",
              startedAt: "2026-03-08T10:00:00.000Z",
              finishedAt: "2026-03-08T10:02:00.000Z",
              metadata: {
                runId: "run-1",
              },
            },
          ],
          edges: [
            {
              from: "experiment:exp-1",
              to: "run:run-1",
              label: "dispatches",
            },
          ],
          summary: {
            totalNodes: 2,
            totalRuns: 1,
            queuedRuns: 0,
            runningRuns: 0,
            completedRuns: 1,
            failedRuns: 0,
            cancelledRuns: 0,
          },
        });
      }
      if (url.pathname === "/api/v2/replay/experiments/exp-1/run" && method === "POST") {
        return mockJsonResponse({
          id: "exp-1",
          tenantId: "default",
          name: "Experiment",
          datasetId: "dataset-op-1",
          status: "queued",
          triggerSource: "quality_advice",
          executionMode: "automatic",
          candidateLabels: ["candidate-a", "candidate-b"],
          runIds: ["run-1", "run-2", "run-3"],
          summary: {},
          runs: [],
          createdAt: "2026-03-08T10:00:00.000Z",
          updatedAt: "2026-03-08T10:11:00.000Z",
        });
      }
      if (url.pathname === "/api/v2/replay/experiments/exp-1/cancel" && method === "POST") {
        return mockJsonResponse({
          id: "exp-1",
          tenantId: "default",
          name: "Experiment",
          datasetId: "dataset-op-1",
          status: "cancelled",
          triggerSource: "quality_advice",
          executionMode: "automatic",
          candidateLabels: ["candidate-a", "candidate-b"],
          runIds: ["run-1", "run-2"],
          summary: {},
          runs: [],
          createdAt: "2026-03-08T10:00:00.000Z",
          updatedAt: "2026-03-08T10:12:00.000Z",
        });
      }

      throw new Error(`unexpected call: ${method} ${url.pathname}`);
    });

    await expect(fetchOpenPlatformAutomationPolicy()).resolves.toEqual(
      expect.objectContaining({
        strategyMatrix: [expect.objectContaining({ id: "critical-replay" })],
      }),
    );
    await expect(
      upsertOpenPlatformAutomationPolicy({
        riskLevel: "high",
        decision: "require_approval",
        reason: "质量高风险命中矩阵时需要审批",
        evaluationScoreThreshold: 78,
        triggerOnEvaluationFailure: true,
        triggerOnReplayRegression: true,
        strategyMatrix: [
          {
            id: "critical-replay",
            metric: "accuracy",
            severity: "critical",
            trendDirection: "down",
            provider: "github",
            workflow: "ci-main",
            projectPattern: "agentledger/*",
            minConfidence: 0.6,
            regressionProbabilityAtLeast: 0.5,
            replayRegressionAtLeast: 1,
            actionType: "replay_experiment",
            requiresApproval: true,
            cooldownMinutes: 30,
            reason: "高风险优先回放",
          },
        ],
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        strategyMatrix: [expect.objectContaining({ id: "critical-replay" })],
      }),
    );
    await expect(
      simulateOpenPlatformAutomationPolicy({
        metric: "accuracy",
        score: 63,
        trendDirection: "down",
        confidence: 0.82,
        regressionProbability: 0.73,
        replayRegressionCount: 2,
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        matchedRuleId: "critical-replay",
        resolvedAction: "replay_experiment",
        requiresApproval: true,
      }),
    );
    await expect(fetchOpenPlatformQualityForecast()).resolves.toEqual(
      expect.objectContaining({
        items: [
          expect.objectContaining({
            trendDirection: "down",
            modelVersion: "quality-heuristic-v2",
            regressionProbability: 0.61,
          }),
        ],
      }),
    );
    await expect(fetchOpenPlatformQualityAdvice()).resolves.toEqual(
      expect.objectContaining({
        items: [
          expect.objectContaining({
            id: "advice-1",
            strategyMatrixMatch: "critical-replay",
            autoExecutionDecision: "approval_required",
          }),
        ],
      }),
    );
    await expect(
      executeOpenPlatformQualityAdvice("advice-1", {
        project: "agentledger/main",
        severity: "warn",
        actionType: "replay_experiment",
        datasetId: "dataset-op-1",
        candidateLabels: ["candidate-a", "candidate-b"],
      }),
    ).resolves.toEqual(expect.objectContaining({ experimentId: "exp-1" }));
    await expect(fetchOpenPlatformQualityAdviceExecutions()).resolves.toEqual(
      expect.objectContaining({ total: 1 }),
    );
    await expect(cancelOpenPlatformQualityAdviceExecution("advice-exec-1")).resolves.toEqual(
      expect.objectContaining({ status: "cancelled" }),
    );
    await expect(
      createOpenPlatformReplayExperiment({
        name: "Experiment",
        datasetId: "dataset-op-1",
        baselineVersionId: "dataset-op-1:v2",
        candidateLabels: ["candidate-a", "candidate-b"],
        autoRun: true,
        triggerSource: "quality_advice",
      }),
    ).resolves.toEqual(expect.objectContaining({ status: "queued" }));
    await expect(
      patchOpenPlatformReplayExperiment("exp-1", {
        name: "Experiment Renamed",
        baselineVersionId: "",
        candidateLabels: ["candidate-a", "candidate-b", "candidate-c"],
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        id: "exp-1",
        name: "Experiment Renamed",
        candidateLabels: ["candidate-a", "candidate-b", "candidate-c"],
      }),
    );
    await expect(fetchOpenPlatformReplayExperiments()).resolves.toEqual(
      expect.objectContaining({ total: 1 }),
    );
    await expect(
      fetchOpenPlatformReplayExperimentsBatchCompare({
        experimentIds: ["exp-1", "exp-2"],
        datasetId: "dataset-op-1",
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        total: 2,
        summary: expect.objectContaining({ bestExperimentId: "exp-1" }),
      }),
    );
    await expect(fetchOpenPlatformReplayExperimentResults("exp-1")).resolves.toEqual(
      expect.objectContaining({ status: "completed" }),
    );
    await expect(fetchOpenPlatformReplayExperimentCompare("exp-1")).resolves.toEqual(
      expect.objectContaining({
        summary: expect.objectContaining({ bestRunId: "run-1" }),
      }),
    );
    await expect(fetchOpenPlatformReplayExperimentWorkflow("exp-1")).resolves.toEqual(
      expect.objectContaining({
        summary: expect.objectContaining({ totalNodes: 2 }),
      }),
    );
    await expect(runOpenPlatformReplayExperiment("exp-1")).resolves.toEqual(
      expect.objectContaining({ runIds: ["run-1", "run-2", "run-3"] }),
    );
    await expect(cancelOpenPlatformReplayExperiment("exp-1")).resolves.toEqual(
      expect.objectContaining({ status: "cancelled" }),
    );
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

  test("exportAudits 与 exportAuditEvidenceBundle 支持解析 DLP 响应头", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-export-audit",
      refreshToken: "refresh-token-export-audit",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.includes("/api/v1/audits/export") && method === "GET") {
        expect(url).toContain("format=csv");
        expect(url).toContain("dlpMode=redact");
        return mockFileResponse("id,action\n1,audit.export\n", {
          contentType: "text/csv; charset=utf-8",
          contentDisposition: 'attachment; filename="audits-2026.csv"',
          headers: {
            "x-agentledger-dlp-mode": "redact",
            "x-agentledger-dlp-matched": "true",
          },
        });
      }

      if (url.includes("/api/v1/audits/evidence-bundle") && method === "GET") {
        expect(url).toContain("dlpMode=block");
        return mockFileResponse(JSON.stringify({ version: "v1", records: [] }), {
          contentType: "application/json; charset=utf-8",
          contentDisposition: 'attachment; filename="evidence-2026.json"',
          headers: {
            "x-agentledger-dlp-mode": "block",
            "x-agentledger-dlp-matched": "false",
          },
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(
      exportAudits("csv", { limit: 20, dlpMode: "redact" }),
    ).resolves.toEqual(
      expect.objectContaining({
        filename: "audits-2026.csv",
        dlpMode: "redact",
        dlpMatched: true,
        blob: expect.any(Blob),
      }),
    );

    await expect(
      exportAuditEvidenceBundle({ limit: 200, dlpMode: "block" }),
    ).resolves.toEqual(
      expect.objectContaining({
        filename: "evidence-2026.json",
        dlpMode: "block",
        dlpMatched: false,
        blob: expect.any(Blob),
      }),
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

  test("fetchAgentRuntimeViews 与 fetchAgentRuntimeConfig 命中 system-config runtime 接口", async () => {
    env.DEV = false;
    setAuthTokens({
      accessToken: "access-token-agent-runtime",
      refreshToken: "refresh-token-agent-runtime",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
        const url = toUrl(input);
        const method = (init?.method ?? "GET").toUpperCase();

        if (url.endsWith("/api/v1/system/config/agents/views") && method === "GET") {
          return mockJsonResponse({
            items: [
              {
                id: "agent-1",
                agentId: "agent-1",
                tenantId: "default",
                displayName: "Agent One",
                hostname: "host-1",
                version: "0.1.0",
                sourceCount: 1,
                sourceIds: ["source-1"],
                sourceNames: ["Source One"],
                runtimeStatus: "online",
                lastHeartbeatAt: "2026-03-09T01:00:00.000Z",
                lastConfigFetchedAt: "2026-03-09T00:59:30.000Z",
                lastConfigVersion: "cfg:test-001",
                lastIngestStatusCode: 202,
                lastAccepted: 5,
                lastRejected: 0,
                heartbeatIntervalSeconds: 30,
                staleAfterSeconds: 90,
                ingestProtocol: "http",
                ingestEndpoint: "http://127.0.0.1:8081/v1/ingest",
                updatedAt: "2026-03-09T01:00:00.000Z",
              },
            ],
            total: 1,
            generatedAt: "2026-03-09T01:00:01.000Z",
          });
        }

        if (
          url.includes("/api/v1/system/config/agent-runtime?agentId=agent-1") &&
          method === "GET"
        ) {
          return mockJsonResponse({
            tenantId: "default",
            agent: {
              agentId: "agent-1",
              hostname: "host-1",
              version: "0.1.0",
              displayName: "Agent One",
            },
            runtime: {
              heartbeatIntervalSeconds: 30,
              staleAfterSeconds: 90,
              ingestProtocol: "http",
              ingestEndpoint: "http://127.0.0.1:8081/v1/ingest",
              sampleGenerateCount: 5,
            },
            bindings: {
              sourceCount: 1,
              sourceIds: ["source-1"],
              sources: [
                {
                  sourceId: "source-1",
                  name: "Source One",
                  accessMode: "realtime",
                  enabled: true,
                  location: "/var/log/agent",
                  sourceRegion: "cn-shanghai",
                },
              ],
            },
            configVersion: "cfg:test-001",
            updatedAt: "2026-03-09T01:00:01.000Z",
          });
        }

        throw new Error(`unexpected call: ${method} ${url}`);
      });

    await expect(fetchAgentRuntimeViews()).resolves.toEqual(
      expect.objectContaining({
        items: [expect.objectContaining({ agentId: "agent-1" })],
        total: 1,
      }),
    );
    await expect(fetchAgentRuntimeConfig("agent-1")).resolves.toEqual(
      expect.objectContaining({
        configVersion: "cfg:test-001",
        bindings: expect.objectContaining({
          sources: [expect.objectContaining({ sourceId: "source-1" })],
        }),
      }),
    );

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining("/api/v1/system/config/agents/views"),
      expect.any(Object),
    );
    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining("/api/v1/system/config/agent-runtime?agentId=agent-1"),
      expect.any(Object),
    );
  });

  test("fetchAgentRuntimeConfig 缺少 agentId 时抛错", async () => {
    await expect(fetchAgentRuntimeConfig("")).rejects.toThrow("agentId 不能为空。");
  });
});

describe("mcp api helpers", () => {
  beforeEach(() => {
    env.DEV = false;
    delete env.VITE_ENABLE_MOCK_FALLBACK;
    clearAuthTokens();
    setUnauthorizedHandler(null);
    vi.restoreAllMocks();
  });

  afterEach(() => {
    clearAuthTokens();
    setUnauthorizedHandler(null);
    vi.restoreAllMocks();
  });

  test("fetchMcpPolicies 与 upsertMcpPolicy 保留 approvalStages 的 nodeId/label", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      if (method === "GET" && url.includes("/api/v1/mcp/policies")) {
        expect(url).toContain("riskLevel=high");
        expect(url).toContain("decision=require_approval");
        expect(url).toContain("keyword=github");
        expect(url).toContain("limit=10");
        return mockJsonResponse({
          items: [
            {
              tenantId: "tenant-a",
              toolId: "github.rotate_key",
              riskLevel: "high",
              decision: "require_approval",
              approvalMode: "two_stage",
              approvalStages: [
                {
                  nodeId: "maintainer-review",
                  stage: "stage1",
                  label: "Maintainer Review",
                  requiredApprovals: 1,
                  roles: ["maintainer"],
                },
                {
                  nodeId: "owner-review",
                  stage: "stage2",
                  label: "Owner Review",
                  requiredApprovals: 1,
                  roles: ["owner"],
                },
              ],
              updatedAt: "2026-03-09T10:00:00.000Z",
            },
          ],
          total: 1,
          filters: {
            riskLevel: "high",
            decision: "require_approval",
            keyword: "github",
            limit: 10,
          },
        });
      }
      if (method === "PUT" && url.endsWith("/api/v1/mcp/policies/github.rotate_key")) {
        const payload = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
        expect(payload).toEqual(
          expect.objectContaining({
            approvalMode: "two_stage",
            approvalStages: [
              {
                nodeId: "maintainer-review",
                stage: "stage1",
                label: "Maintainer Review",
                requiredApprovals: 1,
                roles: ["maintainer"],
              },
              {
                nodeId: "owner-review",
                stage: "stage2",
                label: "Owner Review",
                requiredApprovals: 1,
                roles: ["owner"],
              },
            ],
          }),
        );
        return mockJsonResponse({
          tenantId: "tenant-a",
          toolId: "github.rotate_key",
          riskLevel: "high",
          decision: "require_approval",
          approvalMode: "two_stage",
          approvalStages: payload.approvalStages,
          updatedAt: "2026-03-09T10:01:00.000Z",
        });
      }
      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(
      fetchMcpPolicies({
        riskLevel: "high",
        decision: "require_approval",
        keyword: "github",
        limit: 10,
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        total: 1,
        items: expect.arrayContaining([
          expect.objectContaining({
            toolId: "github.rotate_key",
            approvalStages: expect.arrayContaining([
              expect.objectContaining({
                nodeId: "maintainer-review",
                label: "Maintainer Review",
              }),
            ]),
          }),
        ]),
      }),
    );

    await expect(
      upsertMcpPolicy("github.rotate_key", {
        toolId: "github.rotate_key",
        riskLevel: "high",
        decision: "require_approval",
        approvalMode: "two_stage",
        approvalStages: [
          {
            nodeId: "maintainer-review",
            stage: "stage1",
            label: "Maintainer Review",
            requiredApprovals: 1,
            roles: ["maintainer"],
          },
          {
            nodeId: "owner-review",
            stage: "stage2",
            label: "Owner Review",
            requiredApprovals: 1,
            roles: ["owner"],
          },
        ],
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        approvalStages: [
          expect.objectContaining({
            nodeId: "maintainer-review",
            label: "Maintainer Review",
          }),
          expect.objectContaining({
            nodeId: "owner-review",
            label: "Owner Review",
          }),
        ],
      }),
    );

    expect(fetchSpy).toHaveBeenCalledTimes(2);
  });

  test("create/approve/evaluate/fetch MCP helpers 支持多阶段节点与路径字段", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (method === "POST" && url.endsWith("/api/v1/mcp/approvals")) {
        const payload = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
        expect(payload).toEqual(
          expect.objectContaining({
            toolId: "github.freeze_window",
            approvalConfig: expect.objectContaining({
              mode: "multi_stage",
              approvalWorkflow: expect.objectContaining({
                entryNodeId: "stage1-node",
              }),
            }),
          }),
        );
        return mockJsonResponse({
          id: "mcp-approval-1",
          tenantId: "tenant-a",
          toolId: "github.freeze_window",
          status: "pending",
          approvalMode: "multi_stage",
          currentNodeId: "stage1-node",
          currentStage: "stage1",
          pathHistory: ["stage1-node"],
          remainingApprovals: 1,
          approvalStages: [
            {
              nodeId: "stage1-node",
              stage: "stage1",
              requiredApprovals: 1,
              roles: ["owner"],
              approvedApprovals: 0,
              approvedByUserIds: [],
            },
          ],
          requestedByUserId: "user-a",
          requestedByEmail: "user@example.com",
          createdAt: "2026-03-09T10:10:00.000Z",
          updatedAt: "2026-03-09T10:10:00.000Z",
        }, 201);
      }

      if (method === "POST" && url.endsWith("/api/v1/mcp/approvals/mcp-approval-1/approve")) {
        return mockJsonResponse({
          id: "mcp-approval-1",
          tenantId: "tenant-a",
          toolId: "github.freeze_window",
          status: "approved",
          approvalMode: "multi_stage",
          currentNodeId: "approved",
          currentStage: null,
          pathHistory: ["stage1-node", "approved"],
          remainingApprovals: 0,
          approvalStages: [
            {
              nodeId: "stage1-node",
              stage: "stage1",
              requiredApprovals: 1,
              roles: ["owner"],
              approvedApprovals: 1,
              approvedByUserIds: ["user-a"],
            },
          ],
          requestedByUserId: "user-a",
          requestedByEmail: "user@example.com",
          reviewedByUserId: "owner-a",
          reviewReason: "approved",
          createdAt: "2026-03-09T10:10:00.000Z",
          updatedAt: "2026-03-09T10:11:00.000Z",
        });
      }

      if (method === "POST" && url.endsWith("/api/v1/mcp/evaluate")) {
        const payload = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
        expect(payload).toEqual(
          expect.objectContaining({
            toolId: "github.freeze_window",
            evaluationTimestamp: "2026-03-09T10:12:00.000Z",
          }),
        );
        return mockJsonResponse({
          toolId: "github.freeze_window",
          decision: "require_approval",
          result: "blocked",
          approvalRequestId: "mcp-approval-1",
          approvalRequired: true,
          approvalMode: "multi_stage",
          currentNodeId: "stage1-node",
          currentStage: "stage1",
          pathHistory: ["stage1-node"],
          nextTransitionPreview: {
            fromNodeId: "stage1-node",
            toNodeId: "stage2-node",
            matched: true,
            matchedBy: "condition",
            condition: {
              timeWindow: {
                timezone: "Asia/Shanghai",
                weekdays: [1, 2, 3, 4, 5],
                startTime: "09:00",
                endTime: "18:00",
              },
            },
          },
          approvalStages: [
            {
              nodeId: "stage1-node",
              stage: "stage1",
              requiredApprovals: 1,
              roles: ["owner"],
              approvedApprovals: 0,
              approvedByUserIds: [],
            },
          ],
          remainingApprovals: 1,
          approvalConditionMatched: true,
          enforced: true,
          evaluatedDecision: "require_approval",
          policy: {
            tenantId: "tenant-a",
            toolId: "github.freeze_window",
            riskLevel: "high",
            decision: "require_approval",
            approvalMode: "multi_stage",
            updatedAt: "2026-03-09T10:00:00.000Z",
          },
          invocation: {
            id: "mcp-invocation-1",
            tenantId: "tenant-a",
            toolId: "github.freeze_window",
            decision: "require_approval",
            result: "blocked",
            approvalRequestId: "mcp-approval-1",
            enforced: true,
            evaluatedDecision: "require_approval",
            approvalMode: "multi_stage",
            approvalStage: "stage1",
            approvalSatisfied: false,
            approvalConditionMatched: true,
            metadata: { source: "mcp.evaluate" },
            createdAt: "2026-03-09T10:12:00.000Z",
          },
          evaluatedAt: "2026-03-09T10:12:00.000Z",
        });
      }

      if (method === "GET" && url.includes("/api/v1/mcp/approvals")) {
        return mockJsonResponse({
          items: [
            {
              id: "mcp-approval-1",
              tenantId: "tenant-a",
              toolId: "github.freeze_window",
              status: "pending",
              approvalMode: "multi_stage",
              currentNodeId: "stage1-node",
              currentStage: "stage1",
              pathHistory: ["stage1-node"],
              remainingApprovals: 1,
              approvalStages: [
                {
                  nodeId: "stage1-node",
                  stage: "stage1",
                  requiredApprovals: 1,
                  roles: ["owner"],
                  approvedApprovals: 0,
                  approvedByUserIds: [],
                },
              ],
              requestedByUserId: "user-a",
              createdAt: "2026-03-09T10:10:00.000Z",
              updatedAt: "2026-03-09T10:10:00.000Z",
            },
          ],
          total: 1,
          filters: { status: "pending", limit: 20 },
        });
      }

      if (method === "GET" && url.includes("/api/v1/mcp/invocations")) {
        return mockJsonResponse({
          items: [
            {
              id: "mcp-invocation-1",
              tenantId: "tenant-a",
              toolId: "github.freeze_window",
              decision: "require_approval",
              result: "blocked",
              approvalRequestId: "mcp-approval-1",
              enforced: true,
              evaluatedDecision: "require_approval",
              approvalMode: "multi_stage",
              approvalStage: "stage1",
              approvalSatisfied: false,
              approvalConditionMatched: true,
              metadata: { source: "mcp.evaluate" },
              createdAt: "2026-03-09T10:12:00.000Z",
            },
          ],
          total: 1,
          filters: { toolId: "github.freeze_window", limit: 20 },
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    await expect(
      createMcpApproval({
        toolId: "github.freeze_window",
        approvalConfig: {
          mode: "multi_stage",
          approvalWorkflow: {
            entryNodeId: "stage1-node",
            nodes: [
              {
                nodeId: "stage1-node",
                kind: "approval",
                stage: "stage1",
                requiredApprovals: 1,
                roles: ["owner"],
              },
              {
                nodeId: "approved",
                kind: "terminal_approved",
              },
              {
                nodeId: "rejected",
                kind: "terminal_rejected",
              },
            ],
            transitions: [
              {
                fromNodeId: "stage1-node",
                toNodeId: "approved",
                condition: { default: true },
              },
            ],
          },
        },
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        id: "mcp-approval-1",
        currentNodeId: "stage1-node",
        pathHistory: ["stage1-node"],
      }),
    );

    await expect(
      approveMcpApproval("mcp-approval-1", { nodeId: "stage1-node", reason: "approved" }),
    ).resolves.toEqual(
      expect.objectContaining({
        status: "approved",
        currentNodeId: "approved",
        pathHistory: ["stage1-node", "approved"],
      }),
    );

    await expect(
      evaluateMcpTool({
        toolId: "github.freeze_window",
        evaluationTimestamp: "2026-03-09T10:12:00.000Z",
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        currentNodeId: "stage1-node",
        nextTransitionPreview: expect.objectContaining({
          toNodeId: "stage2-node",
        }),
      }),
    );

    await expect(fetchMcpApprovals({ status: "pending", limit: 20 })).resolves.toEqual(
      expect.objectContaining({
        total: 1,
      }),
    );
    await expect(
      fetchMcpInvocations({ toolId: "github.freeze_window", limit: 20 }),
    ).resolves.toEqual(
      expect.objectContaining({
        total: 1,
      }),
    );
  });
});
