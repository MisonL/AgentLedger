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

describe("Web Console", () => {
  afterEach(() => {
    window.location.hash = "";
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
});
