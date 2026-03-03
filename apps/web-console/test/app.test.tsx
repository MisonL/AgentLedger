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

  test("Analytics 页面会请求 usage monthly/models/sessions", async () => {
    setAuthTokens({
      accessToken: "access-token-test-analytics",
      refreshToken: "refresh-token-test-analytics",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (url.includes("/api/v1/usage/monthly") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              month: "2026-02",
              tokens: 1000,
              cost: 1.25,
              sessions: 3,
            },
          ],
          total: 1,
        });
      }

      if (url.includes("/api/v1/usage/models") && method === "GET") {
        return mockJsonResponse({
          items: [
            {
              model: "gpt-5",
              tokens: 900,
              cost: 1.1,
              sessions: 2,
            },
          ],
          total: 1,
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
    expect(await screen.findByText("2026-02")).toBeInTheDocument();
    expect((await screen.findAllByText("gpt-5")).length).toBeGreaterThan(0);
    expect(await screen.findByText("session-1")).toBeInTheDocument();

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
    expect(await screen.findByText("Token 分解")).toBeInTheDocument();
    expect(await screen.findByText("来源追溯")).toBeInTheDocument();
    expect(await screen.findByText("provider：session-detail-test")).toBeInTheDocument();
    expect(await screen.findByText("path：/workspace/main.ts")).toBeInTheDocument();
    expect(await screen.findByText("input：40")).toBeInTheDocument();
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
