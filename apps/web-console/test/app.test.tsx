import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, test, vi } from "vitest";
import { clearAuthTokens, setAuthTokens } from "../src/api";
import App from "../src/App";

function toUrl(input: Parameters<typeof fetch>[0]): string {
  if (typeof input === "string") {
    return input;
  }
  if (input instanceof URL) {
    return input.toString();
  }
  return input.url;
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

function mockAuthProvidersResponse() {
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

describe("Web Console", () => {
  afterEach(() => {
    window.location.hash = "";
    window.sessionStorage.removeItem("agentledger.web-console.auth.external.pending");
    clearAuthTokens();
    vi.restoreAllMocks();
  });

  test("渲染热力图页面并支持指标切换", async () => {
    setAuthTokens({
      accessToken: "access-token-test-1",
      refreshToken: "refresh-token-test-1",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        return mockJsonResponse({ items: [], total: 0 });
      }

      throw new Error("network down");
    });

    render(<App />);

    expect(await screen.findByRole("heading", { name: "AI 使用热力图" })).toBeInTheDocument();

    const costButton = screen.getByRole("tab", { name: "cost" });
    fireEvent.click(costButton);

    expect(costButton).toHaveAttribute("aria-selected", "true");
    expect(screen.getByRole("grid", { name: "使用热力图" })).toBeInTheDocument();
  });

  test("Sources 新增后会刷新列表", async () => {
    setAuthTokens({
      accessToken: "access-token-test-2",
      refreshToken: "refresh-token-test-2",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const sources = [
      {
        id: "source-1",
        name: "devbox-shanghai",
        type: "local",
        location: "cn-shanghai",
        enabled: true,
        createdAt: "2026-03-01T10:00:00.000Z",
      },
    ];

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        return mockJsonResponse({
          items: sources,
          total: sources.length,
        });
      }

      if (url.endsWith("/api/v1/sources/source-1/health") && method === "GET") {
        return mockJsonResponse({
          sourceId: "source-1",
          accessMode: "hybrid",
          lastSuccessAt: "2026-03-02T09:30:00.000Z",
          lastFailureAt: "2026-03-02T09:00:00.000Z",
          failureCount: 0,
          avgLatencyMs: 122,
          freshnessMinutes: 6,
        });
      }

      if (url.includes("/api/v1/sources/source-1/parse-failures") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "failure-1",
              sourceId: "source-1",
              parserKey: "jsonl",
              errorCode: "parse_error",
              errorMessage: "json line parse failed",
              sourcePath: "/var/log/codex.log",
              sourceOffset: 128,
              metadata: {
                parser: "jsonl",
              },
              failedAt: "2026-03-02T08:59:00.000Z",
              createdAt: "2026-03-02T08:59:00.000Z",
            },
          ],
          total: 1,
          filters: {
            limit: 5,
          },
        });
      }

      if (url.endsWith("/api/v1/sources") && method === "POST") {
        const payload = JSON.parse(String(init?.body ?? "{}")) as {
          name: string;
          type: "local" | "ssh" | "sync-cache";
          location: string;
          enabled?: boolean;
        };

        sources.push({
          id: `source-${sources.length + 1}`,
          name: payload.name,
          type: payload.type,
          location: payload.location,
          enabled: payload.enabled ?? true,
          createdAt: "2026-03-02T10:00:00.000Z",
        });

        return mockJsonResponse(sources[sources.length - 1], 201);
      }

      throw new Error("network down");
    });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Sources" }));

    expect(
      await screen.findByRole("heading", { name: "Sources 管理", level: 1 })
    ).toBeInTheDocument();
    expect(await screen.findByText("devbox-shanghai")).toBeInTheDocument();
    expect(await screen.findByText("健康状态与最近解析失败")).toBeInTheDocument();
    expect(await screen.findByText(/^健康$/)).toBeInTheDocument();
    expect(await screen.findByText("json line parse failed")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("名称"), { target: { value: "build-node-02" } });
    fireEvent.change(screen.getByLabelText("类型"), { target: { value: "ssh" } });
    fireEvent.change(screen.getByLabelText("位置"), { target: { value: "10.0.0.12" } });
    fireEvent.click(screen.getByRole("button", { name: "新增 Source" }));

    expect(await screen.findByText("新增成功，列表已刷新。")).toBeInTheDocument();
    expect(await screen.findByText("build-node-02")).toBeInTheDocument();

    await waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/sources"),
        expect.objectContaining({ method: "POST" })
      );
    });
  });

  test("Sources 状态区块展示 loading 与 empty 态", async () => {
    setAuthTokens({
      accessToken: "access-token-test-source-loading",
      refreshToken: "refresh-token-test-source-loading",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const sources = [
      {
        id: "source-loading",
        name: "build-linux-01",
        type: "ssh",
        location: "10.0.0.31",
        enabled: true,
        createdAt: "2026-03-01T12:00:00.000Z",
      },
    ];

    let resolveHealthRequest: ((value: Response) => void) | null = null;
    const healthRequest = new Promise<Response>((resolve) => {
      resolveHealthRequest = resolve;
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        return mockJsonResponse({
          items: sources,
          total: sources.length,
        });
      }

      if (url.endsWith("/api/v1/sources/source-loading/health") && method === "GET") {
        return healthRequest;
      }

      if (url.includes("/api/v1/sources/source-loading/parse-failures") && method === "GET") {
        return mockJsonResponse({
          items: [],
          total: 0,
          filters: {
            limit: 5,
          },
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Sources" }));
    expect(await screen.findByRole("heading", { name: "Sources 管理", level: 1 })).toBeInTheDocument();
    expect(await screen.findByText("健康状态加载中...")).toBeInTheDocument();

    resolveHealthRequest?.(
      mockJsonResponse({
        sourceId: "source-loading",
        accessMode: "hybrid",
        lastSuccessAt: "2026-03-02T10:00:00.000Z",
        lastFailureAt: null,
        failureCount: 0,
        avgLatencyMs: 110,
        freshnessMinutes: 4,
      })
    );

    expect(await screen.findByText(/^健康$/)).toBeInTheDocument();
    expect(await screen.findByText("最近暂无解析失败记录。")).toBeInTheDocument();
  });

  test("Sources 状态区块展示 error 态", async () => {
    setAuthTokens({
      accessToken: "access-token-test-source-error",
      refreshToken: "refresh-token-test-source-error",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "source-error",
              name: "broken-source",
              type: "local",
              location: "/workspace/.codex/sessions",
              enabled: true,
              createdAt: "2026-03-01T10:00:00.000Z",
            },
          ],
          total: 1,
        });
      }

      if (url.endsWith("/api/v1/sources/source-error/health") && method === "GET") {
        return mockJsonResponse(
          {
            message: "health probe timeout",
          },
          500
        );
      }

      if (url.includes("/api/v1/sources/source-error/parse-failures") && method === "GET") {
        return mockJsonResponse(
          {
            message: "parse failures service unavailable",
          },
          500
        );
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Sources" }));
    expect(await screen.findByRole("heading", { name: "Sources 管理", level: 1 })).toBeInTheDocument();
    expect(await screen.findByText("健康状态加载失败：health probe timeout")).toBeInTheDocument();
    expect(
      await screen.findByText("解析失败列表加载失败：parse failures service unavailable")
    ).toBeInTheDocument();
  });

  test("Sources 页面支持测试连接并展示结果反馈", async () => {
    window.location.hash = "#/sources";
    setAuthTokens({
      accessToken: "access-token-test-source-connection",
      refreshToken: "refresh-token-test-source-connection",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "source-connect-1",
              name: "qa-ssh-node",
              type: "ssh",
              location: "10.0.0.56",
              enabled: true,
              createdAt: "2026-03-02T10:00:00.000Z",
            },
          ],
          total: 1,
        });
      }

      if (url.endsWith("/api/v1/sources/source-connect-1/health") && method === "GET") {
        return mockJsonResponse({
          sourceId: "source-connect-1",
          accessMode: "hybrid",
          lastSuccessAt: "2026-03-02T10:05:00.000Z",
          lastFailureAt: null,
          failureCount: 0,
          avgLatencyMs: 100,
          freshnessMinutes: 3,
        });
      }

      if (url.includes("/api/v1/sources/source-connect-1/parse-failures") && method === "GET") {
        return mockJsonResponse({
          items: [],
          total: 0,
          filters: {
            limit: 5,
          },
        });
      }

      if (url.endsWith("/api/v1/sources/test-connection") && method === "POST") {
        const payload = JSON.parse(String(init?.body ?? "{}")) as { sourceId?: string };
        expect(payload.sourceId).toBe("source-connect-1");
        return mockJsonResponse({
          sourceId: "source-connect-1",
          success: true,
          mode: "ssh",
          latencyMs: 87,
          detail: "ssh handshake ok",
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    expect(await screen.findByRole("heading", { name: "Sources 管理", level: 1 })).toBeInTheDocument();

    fireEvent.click(await screen.findByRole("button", { name: "测试连接" }));

    expect(await screen.findByText("成功 (87ms)：ssh handshake ok")).toBeInTheDocument();

    const postCall = fetchSpy.mock.calls.find(
      ([url, init]) =>
        toUrl(url).endsWith("/api/v1/sources/test-connection") &&
        (init as RequestInit | undefined)?.method === "POST"
    );
    expect(postCall).toBeTruthy();
  });

  test("Analytics 页面会请求 usage daily/monthly/models/sessions 并展示环比与趋势图", async () => {
    setAuthTokens({
      accessToken: "access-token-test-analytics",
      refreshToken: "refresh-token-test-analytics",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.includes("/api/v1/usage/daily") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              date: "2026-02-10",
              tokens: 100,
              cost: 1.2,
              costRaw: 1.0,
              costEstimated: 0.2,
              costMode: "mixed",
              rawCost: 9,
              estimatedCost: 9,
              totalCost: 18,
              costLabel: "legacy should not win",
              sessions: 2,
            },
            {
              date: "2026-02-11",
              tokens: 150,
              cost: 1.8,
              costRaw: 0,
              costEstimated: 1.8,
              costMode: "estimated",
              rawCost: 7,
              totalCost: 7,
              costBasis: "legacy should not win",
              sessions: 3,
            },
          ],
          total: 2,
        });
      }

      if (url.includes("/api/v1/usage/monthly") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              month: "2026-01",
              tokens: 700,
              cost: 0.9,
              costRaw: 0.9,
              costEstimated: 0,
              costMode: "raw",
              sessions: 2,
            },
            {
              month: "2026-02",
              tokens: 1000,
              cost: 1.25,
              costRaw: 1.25,
              costEstimated: 0,
              costMode: "reported",
              sessions: 3,
            },
          ],
          total: 2,
        });
      }

      if (url.includes("/api/v1/usage/models") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              model: "gpt-5",
              tokens: 900,
              cost: 1.1,
              costRaw: 1.0,
              costEstimated: 0.1,
              costMode: "mixed",
              rawCost: 8,
              estimatedCost: 8,
              totalCost: 16,
              costLabel: "legacy should not win",
              sessions: 2,
            },
            {
              model: "legacy-model",
              tokens: 200,
              cost: 0.45,
              rawCost: 0.3,
              estimatedCost: 0.15,
              totalCost: 0.45,
              costBasis: "raw + estimated",
              sessions: 1,
            },
          ],
          total: 2,
        });
      }

      if (url.includes("/api/v1/usage/sessions") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              sessionId: "session-1",
              sourceId: "source-1",
              tool: "Codex CLI",
              model: "gpt-5",
              startedAt: "2026-02-11T09:00:00.000Z",
              inputTokens: 200,
              outputTokens: 300,
              cacheReadTokens: 0,
              cacheWriteTokens: 0,
              reasoningTokens: 0,
              totalTokens: 500,
              cost: 0.5,
              costRaw: 0.5,
              costEstimated: 0,
              costMode: "reported",
            },
          ],
          total: 1,
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Analytics" }));
    expect(await screen.findByRole("heading", { name: "聚合分析" })).toBeInTheDocument();
    expect(await screen.findByText("2026-02-11")).toBeInTheDocument();
    expect(await screen.findByText("2026-02")).toBeInTheDocument();
    expect((await screen.findAllByText("gpt-5")).length).toBeGreaterThan(0);
    expect(await screen.findByText("session-1")).toBeInTheDocument();
    expect(await screen.findByText("总成本趋势（monthly）")).toBeInTheDocument();
    expect(await screen.findByRole("img", { name: "monthly 总成本趋势图" })).toBeInTheDocument();
    expect((await screen.findAllByText("环比 +50.0%")).length).toBeGreaterThan(0);
    expect(await screen.findByText("raw")).toBeInTheDocument();
    expect(await screen.findByText("estimated")).toBeInTheDocument();
    expect((await screen.findAllByText("total")).length).toBeGreaterThan(0);
    expect((await screen.findAllByText("raw + estimated")).length).toBeGreaterThan(0);
    expect(await screen.findByText("$1.1000")).toBeInTheDocument();
    expect(screen.queryByText("$16.0000")).not.toBeInTheDocument();
    expect(screen.queryByText("legacy should not win")).not.toBeInTheDocument();

    expect(
      fetchSpy.mock.calls.some(([url]) => toUrl(url).includes("/api/v1/usage/daily"))
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) => toUrl(url).includes("/api/v1/usage/monthly"))
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) => toUrl(url).includes("/api/v1/usage/models"))
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) => toUrl(url).includes("/api/v1/usage/sessions"))
    ).toBe(true);
  });

  test("Sessions 页面展示会话详情（token 分解 + 来源追溯）", async () => {
    setAuthTokens({
      accessToken: "access-token-test-sessions",
      refreshToken: "refresh-token-test-sessions",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/sessions/search") && method === "POST") {
        return mockJsonResponse({
          items: [
            {
              id: "session-1",
              sourceId: "source-1",
              tool: "Codex CLI",
              model: "gpt-5",
              startedAt: "2026-03-02T09:00:00.000Z",
              endedAt: "2026-03-02T09:03:00.000Z",
              tokens: 100,
              cost: 0.1,
            },
          ],
          total: 1,
          nextCursor: null,
          sourceFreshness: [
            {
              sourceId: "source-1",
              sourceName: "devbox-shanghai",
              accessMode: "hybrid",
              lastSuccessAt: "2026-03-02T09:04:00.000Z",
              lastFailureAt: null,
              failureCount: 1,
              avgLatencyMs: 120,
              freshnessMinutes: 7,
            },
          ],
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
            sourceName: "devbox-shanghai",
            provider: "session-detail-test",
            path: "/workspace/main.ts",
          },
        });
      }

      if (url.includes("/api/v1/sessions/session-1/events?limit=50") && method === "GET") {
        return mockJsonResponse({ items: [], total: 0, limit: 50 });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Sessions" }));
    expect(await screen.findByRole("heading", { name: "会话中心" })).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "会话详情" })).toBeInTheDocument();
    const freshnessSummary = await screen.findByLabelText("来源新鲜度");
    expect(freshnessSummary).toHaveTextContent("devbox-shanghai（hybrid）");
    expect(freshnessSummary).toHaveTextContent("新鲜度 7 分钟");
    expect(await screen.findByText("Token 分解")).toBeInTheDocument();
    expect(await screen.findByText("来源追溯")).toBeInTheDocument();
    expect(await screen.findByText("provider：session-detail-test")).toBeInTheDocument();
    expect(await screen.findByText("path：/workspace/main.ts")).toBeInTheDocument();
    expect(await screen.findByText("input：40")).toBeInTheDocument();
  });

  test("Sessions 页面支持关键词与多维过滤参数下发", async () => {
    setAuthTokens({
      accessToken: "access-token-test-session-filters",
      refreshToken: "refresh-token-test-session-filters",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const searchPayloads: Array<Record<string, unknown>> = [];
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/sessions/search") && method === "POST") {
        searchPayloads.push(JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>);
        return mockJsonResponse({
          items: [],
          total: 0,
          nextCursor: null,
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Sessions" }));
    expect(await screen.findByRole("heading", { name: "会话中心" })).toBeInTheDocument();
    await waitFor(() => {
      expect(searchPayloads.length).toBeGreaterThanOrEqual(1);
    });

    const dateKey = (screen.getByLabelText("日期") as HTMLInputElement).value;
    const nextDate = new Date(`${dateKey}T00:00:00.000Z`);
    nextDate.setUTCDate(nextDate.getUTCDate() + 1);
    const nextDateKey = nextDate.toISOString().slice(0, 10);

    fireEvent.change(screen.getByLabelText("关键词"), { target: { value: "deploy failed" } });
    fireEvent.change(screen.getByLabelText("客户端类型"), { target: { value: "cli" } });
    fireEvent.change(screen.getByLabelText("工具"), { target: { value: "Codex CLI" } });
    fireEvent.change(screen.getByLabelText("主机"), { target: { value: "devbox-01" } });
    fireEvent.change(screen.getByLabelText("模型"), { target: { value: "gpt-5-codex" } });
    fireEvent.change(screen.getByLabelText("项目"), { target: { value: "agentledger" } });
    fireEvent.click(screen.getByRole("button", { name: "应用筛选" }));

    await waitFor(() => {
      expect(searchPayloads.length).toBeGreaterThanOrEqual(2);
    });

    expect(searchPayloads.at(-1)).toMatchObject({
      from: `${dateKey}T00:00:00.000Z`,
      to: `${nextDateKey}T00:00:00.000Z`,
      limit: 50,
      keyword: "deploy failed",
      clientType: "cli",
      tool: "Codex CLI",
      host: "devbox-01",
      model: "gpt-5-codex",
      project: "agentledger",
    });
  });

  test("Sessions 页面支持会话与事件的 cursor 加载更多", async () => {
    setAuthTokens({
      accessToken: "access-token-test-session-cursor",
      refreshToken: "refresh-token-test-session-cursor",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const searchPayloads: Array<Record<string, unknown>> = [];
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/sessions/search") && method === "POST") {
        const payload = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
        searchPayloads.push(payload);
        if (payload.cursor === "session-cursor-2") {
          return mockJsonResponse({
            items: [
              {
                id: "session-2",
                sourceId: "source-2",
                tool: "Cursor IDE",
                model: "claude-3.7",
                startedAt: "2026-03-02T08:00:00.000Z",
                endedAt: "2026-03-02T08:05:00.000Z",
                tokens: 90,
                cost: 0.09,
              },
            ],
            total: 2,
            nextCursor: null,
          });
        }
        return mockJsonResponse({
          items: [
            {
              id: "session-1",
              sourceId: "source-1",
              tool: "Codex CLI",
              model: "gpt-5",
              startedAt: "2026-03-02T09:00:00.000Z",
              endedAt: "2026-03-02T09:05:00.000Z",
              tokens: 100,
              cost: 0.1,
            },
          ],
          total: 2,
          nextCursor: "session-cursor-2",
          sourceFreshness: [],
        });
      }

      if (url.endsWith("/api/v1/sessions/session-1") && method === "GET") {
        return mockJsonResponse({
          id: "session-1",
          sourceId: "source-1",
          tool: "Codex CLI",
          model: "gpt-5",
          startedAt: "2026-03-02T09:00:00.000Z",
          endedAt: "2026-03-02T09:05:00.000Z",
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
            sourceName: "devbox-shanghai",
            provider: "cursor-test",
            path: "/workspace/main.ts",
          },
        });
      }

      if (url.includes("/api/v1/sessions/session-1/events?limit=50&cursor=event-cursor-2")) {
        return mockJsonResponse({
          items: [
            {
              id: "event-2",
              sessionId: "session-1",
              sourceId: "source-1",
              eventType: "message",
              role: "assistant",
              text: "第二页事件",
              timestamp: "2026-03-02T09:02:00.000Z",
              inputTokens: 0,
              outputTokens: 20,
              cacheReadTokens: 0,
              cacheWriteTokens: 0,
              reasoningTokens: 0,
              cost: 0.02,
            },
          ],
          total: 2,
          limit: 50,
          nextCursor: null,
        });
      }

      if (url.includes("/api/v1/sessions/session-1/events?limit=50")) {
        return mockJsonResponse({
          items: [
            {
              id: "event-1",
              sessionId: "session-1",
              sourceId: "source-1",
              eventType: "message",
              role: "user",
              text: "第一页事件",
              timestamp: "2026-03-02T09:01:00.000Z",
              inputTokens: 10,
              outputTokens: 0,
              cacheReadTokens: 0,
              cacheWriteTokens: 0,
              reasoningTokens: 0,
              cost: 0.01,
            },
          ],
          total: 2,
          limit: 50,
          nextCursor: "event-cursor-2",
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Sessions" }));
    expect(await screen.findByRole("heading", { name: "会话中心" })).toBeInTheDocument();
    expect(await screen.findByText("source-1")).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "加载更多会话" })).toBeInTheDocument();
    expect(await screen.findByText("第一页事件")).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "加载更多事件" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "加载更多会话" }));
    expect(await screen.findByText("source-2")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "加载更多事件" }));
    expect(await screen.findByText("第二页事件")).toBeInTheDocument();

    await waitFor(() => {
      const cursors = searchPayloads.map((item) => item.cursor ?? null);
      expect(cursors).toContain("session-cursor-2");
    });
  });

  test("Dashboard 热力图点击后可下钻到 Sessions 并按日期查询", async () => {
    setAuthTokens({
      accessToken: "access-token-test-drilldown",
      refreshToken: "refresh-token-test-drilldown",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const drillDate = new Date();
    drillDate.setUTCDate(drillDate.getUTCDate() - 1);
    const drillDateKey = drillDate.toISOString().slice(0, 10);
    const searchPayloads: Array<Record<string, unknown>> = [];

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/usage/heatmap") && method === "GET") {
        return mockJsonResponse({
          cells: [
            {
              date: `${drillDateKey}T00:00:00.000Z`,
              tokens: 2048,
              cost: 1.28,
              sessions: 4,
            },
          ],
          summary: {
            tokens: 2048,
            cost: 1.28,
            sessions: 4,
          },
        });
      }

      if (url.endsWith("/api/v1/sessions/search") && method === "POST") {
        searchPayloads.push(JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>);
        return mockJsonResponse({
          items: [],
          total: 0,
          nextCursor: null,
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);
    expect(await screen.findByRole("heading", { name: "AI 使用热力图" })).toBeInTheDocument();

    fireEvent.click(await screen.findByRole("gridcell", { name: new RegExp(drillDateKey) }));
    expect(await screen.findByRole("heading", { name: "会话中心" })).toBeInTheDocument();

    await waitFor(() => {
      expect(searchPayloads.length).toBeGreaterThanOrEqual(1);
    });

    const nextDate = new Date(`${drillDateKey}T00:00:00.000Z`);
    nextDate.setUTCDate(nextDate.getUTCDate() + 1);
    const nextDateKey = nextDate.toISOString().slice(0, 10);

    expect(searchPayloads.at(-1)).toMatchObject({
      from: `${drillDateKey}T00:00:00.000Z`,
      to: `${nextDateKey}T00:00:00.000Z`,
      limit: 50,
    });
  });

  test("未登录时显示登录页，登录成功后进入控制台", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/auth/providers") && method === "GET") {
        return mockAuthProvidersResponse();
      }

      if (url.endsWith("/api/v1/auth/login") && method === "POST") {
        return mockJsonResponse({
          user: {
            userId: "user-1",
            email: "owner@example.com",
            displayName: "Owner",
            tenantId: "default",
            tenantRole: "owner",
          },
          tokens: {
            accessToken: "access-token-login",
            refreshToken: "refresh-token-login",
            expiresIn: 1800,
            tokenType: "Bearer",
          },
        });
      }

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        return mockJsonResponse({ items: [], total: 0 });
      }

      throw new Error("network down");
    });

    render(<App />);

    expect(await screen.findByRole("heading", { name: "登录控制台" })).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("邮箱"), { target: { value: "owner@example.com" } });
    fireEvent.change(screen.getByLabelText("密码"), { target: { value: "pwd-123456" } });
    fireEvent.click(screen.getByRole("button", { name: "登录" }));

    expect(await screen.findByRole("heading", { name: "AI 使用热力图" })).toBeInTheDocument();
    await waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/auth/login"),
        expect.objectContaining({ method: "POST" })
      );
    });
  });

  test("登录页支持发起外部登录并携带 PKCE 参数", async () => {
    const assignSpy = vi.fn();
    vi.stubGlobal("location", {
      ...window.location,
      assign: assignSpy,
    } as unknown as Location);

    try {
      vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
        const url = toUrl(input);
        const method = (init?.method ?? "GET").toUpperCase();

        if (url.endsWith("/api/v1/auth/providers") && method === "GET") {
          return mockAuthProvidersResponse();
        }

        throw new Error(`unexpected call: ${method} ${url}`);
      });

      render(<App />);

      expect(await screen.findByRole("heading", { name: "登录控制台" })).toBeInTheDocument();
      fireEvent.click(await screen.findByRole("button", { name: "企业 OIDC" }));

      await waitFor(() => {
        expect(assignSpy).toHaveBeenCalledTimes(1);
      });

      const assignedUrl = new URL(String(assignSpy.mock.calls[0]?.[0]));
      const redirectUri = assignedUrl.searchParams.get("redirect_uri");
      const state = assignedUrl.searchParams.get("state");
      const codeChallenge = assignedUrl.searchParams.get("code_challenge");
      const codeChallengeMethod = assignedUrl.searchParams.get("code_challenge_method");

      expect(`${assignedUrl.origin}${assignedUrl.pathname}`).toBe(
        "https://idp.example.com/oauth/authorize"
      );
      expect(assignedUrl.searchParams.get("response_type")).toBe("code");
      expect(assignedUrl.searchParams.get("provider")).toBe("corp-oidc");
      expect(redirectUri).toContain("#/auth/callback");
      expect(state).toMatch(/^corp-oidc:/);
      expect(codeChallenge).toBeTruthy();
      expect(codeChallengeMethod).toBe("S256");

      const pendingRaw = window.sessionStorage.getItem(
        "agentledger.web-console.auth.external.pending"
      );
      expect(pendingRaw).toBeTruthy();
      const pending = JSON.parse(String(pendingRaw)) as {
        providerId: string;
        state: string;
        redirectUri: string;
        codeVerifier: string;
        createdAt: number;
      };
      expect(pending.providerId).toBe("corp-oidc");
      expect(pending.state).toBe(state);
      expect(pending.redirectUri).toContain("#/auth/callback");
      expect(pending.codeVerifier.length).toBeGreaterThan(10);
      expect(pending.createdAt).toBeGreaterThan(0);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  test("token 失效返回 401 时会提示重新登录，且可再次登录", async () => {
    setAuthTokens({
      accessToken: "expired-token",
      refreshToken: "refresh-token-expired",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    let loggedIn = false;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/auth/providers") && method === "GET") {
        return mockAuthProvidersResponse();
      }

      if (url.endsWith("/api/v1/auth/login") && method === "POST") {
        loggedIn = true;
        return mockJsonResponse({
          user: {
            userId: "user-2",
            email: "owner@example.com",
            displayName: "Owner",
            tenantId: "default",
            tenantRole: "owner",
          },
          tokens: {
            accessToken: "access-token-new",
            refreshToken: "refresh-token-new",
            expiresIn: 1800,
            tokenType: "Bearer",
          },
        });
      }

      if (!loggedIn) {
        return mockJsonResponse(
          {
            message: "登录会话已失效，请重新登录。",
          },
          401
        );
      }

      if (url.endsWith("/api/v1/usage/heatmap") && method === "GET") {
        return mockJsonResponse({
          cells: [
            {
              date: "2026-03-02T00:00:00.000Z",
              tokens: 1000,
              cost: 0.31,
              sessions: 2,
            },
          ],
          summary: {
            tokens: 1000,
            cost: 0.31,
            sessions: 2,
          },
        });
      }

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        return mockJsonResponse({ items: [], total: 0 });
      }

      if (url.endsWith("/api/v1/sessions/search") && method === "POST") {
        return mockJsonResponse({ items: [], total: 0, nextCursor: null });
      }

      throw new Error("unexpected call");
    });

    render(<App />);

    expect(await screen.findByRole("heading", { name: "登录控制台" })).toBeInTheDocument();
    expect(await screen.findByText("登录会话已失效，请重新登录。")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("邮箱"), { target: { value: "owner@example.com" } });
    fireEvent.change(screen.getByLabelText("密码"), { target: { value: "pwd-654321" } });
    fireEvent.click(screen.getByRole("button", { name: "登录" }));

    expect(await screen.findByRole("heading", { name: "AI 使用热力图" })).toBeInTheDocument();
  });

  test("外部登录回调可自动 exchange 并进入控制台", async () => {
    const originalHash = window.location.hash;
    window.location.hash = "#/auth/callback?code=authorization-code-1&state=corp-oidc:nonce-1";
    window.sessionStorage.setItem(
      "agentledger.web-console.auth.external.pending",
      JSON.stringify({
        providerId: "corp-oidc",
        state: "corp-oidc:nonce-1",
        redirectUri: "http://localhost:5173/#/auth/callback",
        codeVerifier: "pkce-code-verifier-1",
        createdAt: Date.now(),
      })
    );

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/auth/providers") && method === "GET") {
        return mockAuthProvidersResponse();
      }

      if (url.endsWith("/api/v1/auth/external/exchange") && method === "POST") {
        const payload = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
        expect(payload.providerId).toBe("corp-oidc");
        expect(payload.code).toBe("authorization-code-1");
        expect(payload.state).toBe("corp-oidc:nonce-1");
        expect(payload.codeVerifier).toBe("pkce-code-verifier-1");
        return mockJsonResponse({
          user: {
            userId: "user-external-1",
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

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        return mockJsonResponse({ items: [], total: 0 });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    expect(await screen.findByRole("heading", { name: "AI 使用热力图" })).toBeInTheDocument();
    await waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/auth/external/exchange"),
        expect.objectContaining({ method: "POST" })
      );
    });

    window.location.hash = originalHash;
  });

  test("外部登录回调兼容 ?code=...#/auth/callback 形态", async () => {
    const originalHref = window.location.href;
    const originalHash = window.location.hash;
    window.history.replaceState(
      {},
      "",
      "/?code=authorization-code-search-1&state=corp-oidc:nonce-search-1#/auth/callback"
    );
    window.sessionStorage.setItem(
      "agentledger.web-console.auth.external.pending",
      JSON.stringify({
        providerId: "corp-oidc",
        state: "corp-oidc:nonce-search-1",
        redirectUri: "http://localhost:5173/#/auth/callback",
        codeVerifier: "pkce-code-verifier-search-1",
        createdAt: Date.now(),
      })
    );

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.endsWith("/api/v1/auth/providers") && method === "GET") {
        return mockAuthProvidersResponse();
      }

      if (url.endsWith("/api/v1/auth/external/exchange") && method === "POST") {
        const payload = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
        expect(payload.providerId).toBe("corp-oidc");
        expect(payload.code).toBe("authorization-code-search-1");
        expect(payload.state).toBe("corp-oidc:nonce-search-1");
        expect(payload.codeVerifier).toBe("pkce-code-verifier-search-1");
        return mockJsonResponse({
          user: {
            userId: "user-external-search-1",
            email: "owner@example.com",
            displayName: "Owner",
            tenantId: "default",
            tenantRole: "owner",
          },
          tokens: {
            accessToken: "access-token-external-search",
            refreshToken: "refresh-token-external-search",
            expiresIn: 1800,
            tokenType: "Bearer",
          },
        });
      }

      if (url.endsWith("/api/v1/sources") && method === "GET") {
        return mockJsonResponse({ items: [], total: 0 });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    expect(await screen.findByRole("heading", { name: "AI 使用热力图" })).toBeInTheDocument();
    await waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/auth/external/exchange"),
        expect.objectContaining({ method: "POST" })
      );
    });

    window.history.replaceState({}, "", originalHref);
    window.location.hash = originalHash;
  });

  test("治理页展示告警工作台与导出中心", async () => {
    window.location.hash = "#/governance";
    setAuthTokens({
      accessToken: "access-token-governance-overview",
      refreshToken: "refresh-token-governance-overview",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.includes("/api/v1/alerts") && method === "GET") {
        return mockJsonResponse({
          items: [],
          total: 0,
          filters: {},
          nextCursor: null,
        });
      }
      if (url.includes("/api/v1/usage/weekly-summary") && method === "GET") {
        return mockJsonResponse({
          metric: "tokens",
          timezone: "UTC",
          weeks: [],
          summary: {
            tokens: 0,
            cost: 0,
            sessions: 0,
          },
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    expect(await screen.findByRole("heading", { name: "治理中心" })).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "周报摘要", level: 2 })).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "告警工作台", level: 2 })).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "导出中心", level: 2 })).toBeInTheDocument();
  });

  test("治理页支持告警级别与状态筛选", async () => {
    window.location.hash = "#/governance";
    setAuthTokens({
      accessToken: "access-token-governance-filters",
      refreshToken: "refresh-token-governance-filters",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const alerts = [
      {
        id: "alert-filter-warning-open",
        tenantId: "default",
        budgetId: "budget-filter-1",
        scope: "tenant",
        scopeRef: "default",
        severity: "warning",
        status: "open",
        message: "warning budget approaching",
        threshold: 0.8,
        value: 0.82,
        createdAt: "2026-03-01T09:00:00.000Z",
        updatedAt: "2026-03-01T09:05:00.000Z",
        metadata: {},
      },
      {
        id: "alert-filter-critical-open",
        tenantId: "default",
        budgetId: "budget-filter-2",
        scope: "tenant",
        scopeRef: "default",
        severity: "critical",
        status: "open",
        message: "critical unresolved incident",
        threshold: 0.9,
        value: 0.95,
        createdAt: "2026-03-01T10:00:00.000Z",
        updatedAt: "2026-03-01T10:02:00.000Z",
        metadata: {},
      },
      {
        id: "alert-filter-critical-resolved",
        tenantId: "default",
        budgetId: "budget-filter-3",
        scope: "tenant",
        scopeRef: "default",
        severity: "critical",
        status: "resolved",
        message: "critical already resolved",
        threshold: 0.9,
        value: 0.93,
        createdAt: "2026-03-01T11:00:00.000Z",
        updatedAt: "2026-03-01T11:20:00.000Z",
        metadata: {},
      },
    ];

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.includes("/api/v1/alerts") && method === "GET") {
        const parsedUrl = new URL(url, "http://localhost");
        const severity = parsedUrl.searchParams.get("severity");
        const status = parsedUrl.searchParams.get("status");
        const filteredItems = alerts.filter((item) => {
          if (severity && item.severity !== severity) {
            return false;
          }
          if (status && item.status !== status) {
            return false;
          }
          return true;
        });

        return mockJsonResponse({
          items: filteredItems,
          total: filteredItems.length,
          filters: {
            limit: 50,
            ...(severity ? { severity } : {}),
            ...(status ? { status } : {}),
          },
          nextCursor: null,
        });
      }
      if (url.includes("/api/v1/usage/weekly-summary") && method === "GET") {
        return mockJsonResponse({
          metric: "tokens",
          timezone: "UTC",
          weeks: [],
          summary: {
            tokens: 0,
            cost: 0,
            sessions: 0,
          },
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    expect(await screen.findByText("warning budget approaching")).toBeInTheDocument();
    expect(await screen.findByText("critical unresolved incident")).toBeInTheDocument();
    expect(await screen.findByText("critical already resolved")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("级别"), { target: { value: "critical" } });

    expect(await screen.findByText("critical unresolved incident")).toBeInTheDocument();
    expect(await screen.findByText("critical already resolved")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.queryByText("warning budget approaching")).not.toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText("状态"), { target: { value: "resolved" } });

    expect(await screen.findByText("critical already resolved")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.queryByText("critical unresolved incident")).not.toBeInTheDocument();
    });

    expect(
      fetchSpy.mock.calls.some(([url]) => {
        const parsedUrl = new URL(toUrl(url), "http://localhost");
        return (
          parsedUrl.pathname === "/api/v1/alerts" &&
          parsedUrl.searchParams.get("severity") === "critical" &&
          parsedUrl.searchParams.get("status") === "resolved"
        );
      })
    ).toBe(true);
  });

  test("治理页支持 ACK 告警状态", async () => {
    window.location.hash = "#/governance";
    setAuthTokens({
      accessToken: "access-token-governance-alert",
      refreshToken: "refresh-token-governance-alert",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.includes("/api/v1/alerts") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              id: "alert-ui-1",
              tenantId: "default",
              budgetId: "budget-ui-1",
              scope: "tenant",
              scopeRef: "default",
              severity: "critical",
              status: "open",
              message: "critical threshold reached",
              threshold: 0.8,
              value: 0.92,
              createdAt: "2026-03-01T10:00:00.000Z",
              updatedAt: "2026-03-01T10:05:00.000Z",
              metadata: {},
            },
          ],
          total: 1,
          filters: {
            limit: 50,
          },
          nextCursor: null,
        });
      }
      if (url.includes("/api/v1/usage/weekly-summary") && method === "GET") {
        return mockJsonResponse({
          metric: "tokens",
          timezone: "UTC",
          weeks: [
            {
              weekStart: "2026-02-24",
              weekEnd: "2026-03-02",
              tokens: 100,
              cost: 1.2,
              sessions: 3,
            },
          ],
          summary: {
            tokens: 100,
            cost: 1.2,
            sessions: 3,
          },
          peakWeek: {
            weekStart: "2026-02-24",
            weekEnd: "2026-03-02",
            tokens: 100,
            cost: 1.2,
            sessions: 3,
          },
        });
      }

      if (url.endsWith("/api/v1/alerts/alert-ui-1/status") && method === "PATCH") {
        return mockJsonResponse({
          id: "alert-ui-1",
          tenantId: "default",
          budgetId: "budget-ui-1",
          scope: "tenant",
          scopeRef: "default",
          severity: "critical",
          status: "acknowledged",
          message: "critical threshold reached",
          threshold: 0.8,
          value: 0.92,
          createdAt: "2026-03-01T10:00:00.000Z",
          updatedAt: "2026-03-01T10:06:00.000Z",
          metadata: {},
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "ACK" }));
    expect(await screen.findByText("告警 alert-ui-1 已更新为 acknowledged。")).toBeInTheDocument();

    const patchCall = fetchSpy.mock.calls.find(
      ([url, init]) =>
        toUrl(url).endsWith("/api/v1/alerts/alert-ui-1/status") &&
        (init as RequestInit | undefined)?.method === "PATCH"
    );
    expect(patchCall).toBeTruthy();
  });

  test("治理页支持 Resolve 告警状态并展示完成反馈", async () => {
    window.location.hash = "#/governance";
    setAuthTokens({
      accessToken: "access-token-governance-resolve",
      refreshToken: "refresh-token-governance-resolve",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const alerts = [
      {
        id: "alert-ui-resolve",
        tenantId: "default",
        budgetId: "budget-ui-resolve",
        scope: "tenant",
        scopeRef: "default",
        severity: "warning",
        status: "open",
        message: "resolve me",
        threshold: 0.7,
        value: 0.75,
        createdAt: "2026-03-01T12:00:00.000Z",
        updatedAt: "2026-03-01T12:10:00.000Z",
        metadata: {},
      },
    ];

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.includes("/api/v1/alerts") && method === "GET") {
        return mockJsonResponse({
          items: alerts,
          total: alerts.length,
          filters: {
            limit: 50,
          },
          nextCursor: null,
        });
      }
      if (url.includes("/api/v1/usage/weekly-summary") && method === "GET") {
        return mockJsonResponse({
          metric: "tokens",
          timezone: "UTC",
          weeks: [],
          summary: {
            tokens: 0,
            cost: 0,
            sessions: 0,
          },
        });
      }

      if (url.endsWith("/api/v1/alerts/alert-ui-resolve/status") && method === "PATCH") {
        const payload = JSON.parse(String(init?.body ?? "{}")) as { status?: string };
        expect(payload.status).toBe("resolved");
        alerts[0] = {
          ...alerts[0],
          status: "resolved",
          updatedAt: "2026-03-01T12:20:00.000Z",
        };
        return mockJsonResponse(alerts[0]);
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Resolve" }));

    expect(await screen.findByText("告警 alert-ui-resolve 已更新为 resolved。")).toBeInTheDocument();
    expect(await screen.findByText("已完成")).toBeInTheDocument();

    await waitFor(() => {
      expect(screen.queryByRole("button", { name: "Resolve" })).not.toBeInTheDocument();
    });

    const patchCall = fetchSpy.mock.calls.find(
      ([url, init]) =>
        toUrl(url).endsWith("/api/v1/alerts/alert-ui-resolve/status") &&
        (init as RequestInit | undefined)?.method === "PATCH"
    );
    expect(patchCall).toBeTruthy();
  });

  test("治理页支持 Sessions/Usage 导出", async () => {
    window.location.hash = "#/governance";
    setAuthTokens({
      accessToken: "access-token-governance-export",
      refreshToken: "refresh-token-governance-export",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const originalCreateObjectURL = URL.createObjectURL;
    const originalRevokeObjectURL = URL.revokeObjectURL;
    const createObjectURLSpy = vi.fn(() => "blob:mock-download-url");
    const revokeObjectURLSpy = vi.fn();
    Object.defineProperty(URL, "createObjectURL", {
      configurable: true,
      writable: true,
      value: createObjectURLSpy,
    });
    Object.defineProperty(URL, "revokeObjectURL", {
      configurable: true,
      writable: true,
      value: revokeObjectURLSpy,
    });
    const anchorClickSpy = vi
      .spyOn(HTMLAnchorElement.prototype, "click")
      .mockImplementation(() => {});

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.includes("/api/v1/alerts") && method === "GET") {
        return mockJsonResponse({
          items: [],
          total: 0,
          filters: {
            limit: 50,
          },
          nextCursor: null,
        });
      }
      if (url.includes("/api/v1/usage/weekly-summary") && method === "GET") {
        return mockJsonResponse({
          metric: "tokens",
          timezone: "UTC",
          weeks: [],
          summary: {
            tokens: 0,
            cost: 0,
            sessions: 0,
          },
        });
      }

      if (url.includes("/api/v1/exports/sessions") && method === "GET") {
        return {
          ok: true,
          status: 200,
          headers: {
            get: (name: string) => {
              const normalized = name.toLowerCase();
              if (normalized === "content-type") {
                return "text/csv; charset=utf-8";
              }
              if (normalized === "content-disposition") {
                return 'attachment; filename="sessions-ui.csv"';
              }
              return null;
            },
          },
          blob: async () => new Blob(["id,tool\ns1,codex\n"], { type: "text/csv" }),
          json: async () => ({}),
          text: async () => "id,tool\ns1,codex\n",
        } as Response;
      }

      if (url.includes("/api/v1/exports/usage") && method === "GET") {
        return {
          ok: true,
          status: 200,
          headers: {
            get: (name: string) => {
              const normalized = name.toLowerCase();
              if (normalized === "content-type") {
                return "text/csv; charset=utf-8";
              }
              if (normalized === "content-disposition") {
                return 'attachment; filename="usage-ui.csv"';
              }
              return null;
            },
          },
          blob: async () => new Blob(["date,tokens\n2026-03-01,100\n"], { type: "text/csv" }),
          json: async () => ({}),
          text: async () => "date,tokens\n2026-03-01,100\n",
        } as Response;
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    try {
      render(<App />);

      fireEvent.click(await screen.findByRole("button", { name: "导出 Sessions" }));
      expect(await screen.findByText("Sessions 导出成功：sessions-ui.csv")).toBeInTheDocument();

      fireEvent.change(screen.getByLabelText("维度"), { target: { value: "weekly" } });
      fireEvent.click(screen.getByRole("button", { name: "导出 Usage" }));
      expect(await screen.findByText("Usage 导出成功：usage-ui.csv")).toBeInTheDocument();

      expect(
        fetchSpy.mock.calls.some(([url, init]) => {
          const requestInit = init as RequestInit | undefined;
          return (
            toUrl(url).includes("/api/v1/exports/sessions?format=csv") &&
            (requestInit?.method ?? "GET").toUpperCase() === "GET"
          );
        })
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(([url, init]) => {
          const requestInit = init as RequestInit | undefined;
          return (
            toUrl(url).includes("/api/v1/exports/usage?format=csv&dimension=weekly") &&
            (requestInit?.method ?? "GET").toUpperCase() === "GET"
          );
        })
      ).toBe(true);
      expect(createObjectURLSpy).toHaveBeenCalledTimes(2);
      expect(revokeObjectURLSpy).toHaveBeenCalledTimes(2);
      expect(anchorClickSpy).toHaveBeenCalledTimes(2);
    } finally {
      anchorClickSpy.mockRestore();
      Object.defineProperty(URL, "createObjectURL", {
        configurable: true,
        writable: true,
        value: originalCreateObjectURL,
      });
      Object.defineProperty(URL, "revokeObjectURL", {
        configurable: true,
        writable: true,
        value: originalRevokeObjectURL,
      });
    }
  });
});
