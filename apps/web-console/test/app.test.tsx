import {
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, describe, expect, test, vi } from "vitest";
import { clearAuthTokens, setAuthTokens } from "../src/api";
import App from "../src/App";

const GOVERNANCE_HEAVY_TEST_TIMEOUT_MS = 25_000;

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
      get: (name: string) =>
        name.toLowerCase() === "content-type" ? "application/json" : null,
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

type GovernanceRuleAssetFixture = {
  id: string;
  name: string;
  status: "draft" | "published" | "deprecated";
  latestVersion: number;
  publishedVersion: number | null;
  requiredApprovals?: number;
  scopeBinding?: {
    organizations?: string[];
    projects?: string[];
    clients?: string[];
  };
};

type GovernanceResidencyPolicyFixture = {
  tenantId: string;
  mode: "single_region" | "active_active";
  primaryRegion: string;
  replicaRegions: string[];
  allowCrossRegionTransfer: boolean;
  requireTransferApproval: boolean;
  updatedAt: string;
};

type GovernanceResidencyPolicyResponseFixture = {
  status?: number;
  body: GovernanceResidencyPolicyFixture | { message: string };
};

function mockGovernancePageFetch({
  residencyPolicy = {
    tenantId: "default",
    mode: "single_region" as const,
    primaryRegion: "cn-shanghai",
    replicaRegions: [] as string[],
    allowCrossRegionTransfer: false,
    requireTransferApproval: false,
    updatedAt: "2026-03-01T00:00:00.000Z",
  },
  residencyPolicyResponses = [] as GovernanceResidencyPolicyResponseFixture[],
  ruleAssets = [] as GovernanceRuleAssetFixture[],
  extraHandler,
}: {
  residencyPolicy?: GovernanceResidencyPolicyFixture;
  residencyPolicyResponses?: GovernanceResidencyPolicyResponseFixture[];
  ruleAssets?: GovernanceRuleAssetFixture[];
  extraHandler?: (
    input: Parameters<typeof fetch>[0],
    init: Parameters<typeof fetch>[1],
    context: { url: string; method: string; pathname: string },
  ) => Response | Promise<Response> | undefined;
} = {}) {
  const buildRuleAssetItems = () =>
    ruleAssets.map((asset, index) => ({
      id: asset.id,
      tenantId: "default",
      name: asset.name,
      description: `fixture-${index + 1}`,
      status: asset.status,
      latestVersion: asset.latestVersion,
      publishedVersion: asset.publishedVersion,
      requiredApprovals: asset.requiredApprovals ?? 1,
      scopeBinding: asset.scopeBinding ?? {},
      createdAt: `2026-03-${String(index + 1).padStart(2, "0")}T08:00:00.000Z`,
      updatedAt: `2026-03-${String(index + 1).padStart(2, "0")}T09:00:00.000Z`,
    }));
  let residencyPolicyResponseCursor = 0;

  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation(async (input, init) => {
      const url = toUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      const pathname = new URL(url, "http://localhost").pathname;

      if (extraHandler) {
        const extraResponse = await extraHandler(input, init, {
          url,
          method,
          pathname,
        });
        if (extraResponse) {
          return extraResponse;
        }
      }

      if (pathname === "/api/v1/alerts" && method === "GET") {
        return mockJsonResponse({
          items: [],
          total: 0,
          filters: {
            limit: 50,
          },
          nextCursor: null,
        });
      }

      if (pathname === "/api/v1/usage/weekly-summary" && method === "GET") {
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

      if (
        pathname === "/api/v2/residency/region-mappings" &&
        method === "GET"
      ) {
        return mockJsonResponse({
          items: [
            {
              regionId: "cn-shanghai",
              regionName: "华东 1",
              active: true,
              role: "primary",
              writable: true,
              metadata: {
                description: "主地域",
              },
            },
            {
              regionId: "ap-southeast-1",
              regionName: "新加坡",
              active: true,
              role: "available",
              writable: false,
              metadata: {
                description: "副本候选地域",
              },
            },
          ],
          total: 2,
        });
      }

      if (
        pathname === "/api/v2/residency/policies/current" &&
        method === "GET"
      ) {
        if (residencyPolicyResponses.length > 0) {
          const response =
            residencyPolicyResponses[
              Math.min(
                residencyPolicyResponseCursor,
                residencyPolicyResponses.length - 1,
              )
            ];
          residencyPolicyResponseCursor += 1;
          return mockJsonResponse(response.body, response.status ?? 200);
        }
        return mockJsonResponse(residencyPolicy);
      }

      if (
        pathname === "/api/v2/residency/policies/current" &&
        method === "PUT"
      ) {
        const payload = JSON.parse(String(init?.body ?? "{}")) as {
          mode?: "single_region" | "active_active";
          primaryRegion?: string;
          replicaRegions?: string[];
          allowCrossRegionTransfer?: boolean;
          requireTransferApproval?: boolean;
        };
        return mockJsonResponse({
          tenantId: "default",
          mode: payload.mode ?? "single_region",
          primaryRegion: payload.primaryRegion ?? "cn-shanghai",
          replicaRegions: payload.replicaRegions ?? [],
          allowCrossRegionTransfer: payload.allowCrossRegionTransfer ?? false,
          requireTransferApproval: payload.requireTransferApproval ?? false,
          updatedAt: "2026-03-02T00:00:00.000Z",
        });
      }

      if (pathname === "/api/v2/residency/replications" && method === "GET") {
        return mockJsonResponse({
          items: [],
          total: 0,
          filters: {
            limit: 50,
          },
        });
      }

      if (
        pathname.match(/^\/api\/v1\/rules\/assets\/[^/]+\/versions$/) &&
        method === "GET"
      ) {
        return mockJsonResponse({
          items: [],
          total: 0,
        });
      }

      if (
        pathname.match(/^\/api\/v1\/rules\/assets\/[^/]+\/approvals$/) &&
        method === "GET"
      ) {
        return mockJsonResponse({
          items: [],
          total: 0,
          filters: {
            limit: 50,
          },
        });
      }

      if (pathname === "/api/v1/rules/assets" && method === "GET") {
        const ruleAssetItems = buildRuleAssetItems();
        return mockJsonResponse({
          items: ruleAssetItems,
          total: ruleAssetItems.length,
          filters: {
            limit: 50,
          },
        });
      }

      if (pathname === "/api/v1/mcp/policies" && method === "GET") {
        return mockJsonResponse({
          items: [],
          total: 0,
          filters: {
            limit: 50,
          },
        });
      }

      if (pathname === "/api/v1/mcp/approvals" && method === "GET") {
        return mockJsonResponse({
          items: [],
          total: 0,
          filters: {
            limit: 50,
          },
        });
      }

      if (pathname === "/api/v1/mcp/invocations" && method === "GET") {
        return mockJsonResponse({
          items: [],
          total: 0,
          filters: {
            limit: 50,
          },
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });
}

describe("Web Console", () => {
  afterEach(() => {
    window.location.hash = "";
    window.sessionStorage.removeItem(
      "agentledger.web-console.auth.external.pending",
    );
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

    expect(
      await screen.findByRole("heading", { name: "AI 使用热力图" }),
    ).toBeInTheDocument();

    const costButton = screen.getByRole("tab", { name: "cost" });
    fireEvent.click(costButton);

    expect(costButton).toHaveAttribute("aria-selected", "true");
    expect(
      screen.getByRole("grid", { name: "使用热力图" }),
    ).toBeInTheDocument();
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
        sourceRegion: "cn-shanghai",
        enabled: true,
        createdAt: "2026-03-01T10:00:00.000Z",
      },
    ];

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
        const url = toUrl(input);
        const method = (init?.method ?? "GET").toUpperCase();

        if (url.endsWith("/api/v1/sources") && method === "GET") {
          return mockJsonResponse({
            items: sources,
            total: sources.length,
          });
        }

        if (
          url.endsWith("/api/v1/sources/source-1/health") &&
          method === "GET"
        ) {
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

        if (
          url.includes("/api/v1/sources/source-1/parse-failures") &&
          method === "GET"
        ) {
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
            sourceRegion?: string;
            enabled?: boolean;
          };

          sources.push({
            id: `source-${sources.length + 1}`,
            name: payload.name,
            type: payload.type,
            location: payload.location,
            sourceRegion: payload.sourceRegion,
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
      await screen.findByRole("heading", { name: "Sources 管理", level: 1 }),
    ).toBeInTheDocument();
    expect(await screen.findByText("devbox-shanghai")).toBeInTheDocument();
    expect(
      await screen.findByText("健康状态与最近解析失败"),
    ).toBeInTheDocument();
    expect(await screen.findByText(/^健康$/)).toBeInTheDocument();
    expect(
      await screen.findByText("json line parse failed"),
    ).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("名称"), {
      target: { value: "build-node-02" },
    });
    fireEvent.change(screen.getByLabelText("类型"), {
      target: { value: "ssh" },
    });
    fireEvent.change(screen.getByLabelText("位置"), {
      target: { value: "10.0.0.12" },
    });
    fireEvent.change(screen.getByLabelText("Region"), {
      target: { value: "ap-southeast-1" },
    });
    fireEvent.click(screen.getByRole("button", { name: "新增 Source" }));

    expect(
      await screen.findByText("新增成功，列表已刷新。"),
    ).toBeInTheDocument();
    expect(await screen.findByText("build-node-02")).toBeInTheDocument();
    expect(await screen.findByText("ap-southeast-1")).toBeInTheDocument();

    await waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/sources"),
        expect.objectContaining({ method: "POST" }),
      );
    });
    const postCall = fetchSpy.mock.calls.find(
      ([url, init]) =>
        toUrl(url).endsWith("/api/v1/sources") &&
        (init as RequestInit | undefined)?.method === "POST",
    );
    expect(postCall).toBeTruthy();
    expect(
      JSON.parse(
        String((postCall?.[1] as RequestInit | undefined)?.body ?? "{}"),
      ),
    ).toMatchObject({
      sourceRegion: "ap-southeast-1",
    });
  });

  test("Sources 页面支持缺失 region 过滤、编辑补录与按主区域回填", async () => {
    setAuthTokens({
      accessToken: "access-token-test-source-region-actions",
      refreshToken: "refresh-token-test-source-region-actions",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const sources = [
      {
        id: "source-missing-1",
        name: "missing-region-source",
        type: "local",
        location: "/workspace/missing-region",
        enabled: true,
        createdAt: "2026-03-02T10:00:00.000Z",
      },
      {
        id: "source-ready-1",
        name: "ready-region-source",
        type: "ssh",
        location: "10.0.0.20",
        sourceRegion: "cn-shanghai",
        enabled: true,
        createdAt: "2026-03-02T11:00:00.000Z",
      },
    ];

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
        const url = toUrl(input);
        const method = (init?.method ?? "GET").toUpperCase();

        if (url.endsWith("/api/v1/sources") && method === "GET") {
          return mockJsonResponse({
            items: sources,
            total: sources.length,
          });
        }

        if (
          url.endsWith("/api/v1/sources/source-missing-1/health") &&
          method === "GET"
        ) {
          return mockJsonResponse({
            sourceId: "source-missing-1",
            accessMode: "realtime",
            lastSuccessAt: null,
            lastFailureAt: null,
            failureCount: 0,
            avgLatencyMs: null,
            freshnessMinutes: null,
          });
        }

        if (
          url.includes("/api/v1/sources/source-missing-1/parse-failures") &&
          method === "GET"
        ) {
          return mockJsonResponse({
            items: [],
            total: 0,
            filters: {
              limit: 5,
            },
          });
        }

        if (
          url.endsWith("/api/v1/sources/source-ready-1/health") &&
          method === "GET"
        ) {
          return mockJsonResponse({
            sourceId: "source-ready-1",
            accessMode: "hybrid",
            lastSuccessAt: "2026-03-02T12:00:00.000Z",
            lastFailureAt: null,
            failureCount: 0,
            avgLatencyMs: 90,
            freshnessMinutes: 2,
          });
        }

        if (
          url.includes("/api/v1/sources/source-ready-1/parse-failures") &&
          method === "GET"
        ) {
          return mockJsonResponse({
            items: [],
            total: 0,
            filters: {
              limit: 5,
            },
          });
        }

        if (
          url.endsWith("/api/v1/sources/source-missing-1") &&
          method === "PATCH"
        ) {
          const payload = JSON.parse(String(init?.body ?? "{}")) as {
            sourceRegion?: string;
          };
          sources[0] = {
            ...sources[0],
            sourceRegion: payload.sourceRegion,
          };
          return mockJsonResponse(sources[0]);
        }

        if (
          url.endsWith("/api/v1/sources/source-region/backfill") &&
          method === "POST"
        ) {
          const payload = JSON.parse(String(init?.body ?? "{}")) as {
            sourceIds?: string[];
          };
          const targetIds =
            payload.sourceIds ??
            sources.filter((item) => !item.sourceRegion).map((item) => item.id);
          for (const source of sources) {
            if (targetIds.includes(source.id) && !source.sourceRegion) {
              source.sourceRegion = "cn-shanghai";
            }
          }
          return mockJsonResponse({
            tenantId: "default",
            dryRun: false,
            primaryRegion: "cn-shanghai",
            totalMissing: targetIds.length,
            updated: targetIds.length,
            skipped: 0,
            items: targetIds.map((sourceId) => ({
              sourceId,
              name: sourceId,
              status: "updated",
              appliedRegion: "cn-shanghai",
            })),
          });
        }

        throw new Error(`unexpected call: ${method} ${url}`);
      });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Sources" }));
    expect(
      await screen.findByRole("heading", { name: "Sources 管理", level: 1 }),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByLabelText("仅看缺失 Region"));
    expect(
      await screen.findByText("missing-region-source"),
    ).toBeInTheDocument();
    expect(screen.queryByText("ready-region-source")).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "编辑" }));
    fireEvent.change(screen.getByLabelText("Region"), {
      target: { value: "cn-hangzhou" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存 Source" }));

    expect(
      await screen.findByText("Source missing-region-source 已更新。"),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText("仅看缺失 Region"));
    expect(await screen.findByText("cn-hangzhou")).toBeInTheDocument();

    sources[0] = {
      ...sources[0],
      sourceRegion: undefined,
    };

    fireEvent.click(screen.getByLabelText("仅看缺失 Region"));
    fireEvent.click(screen.getByLabelText("仅看缺失 Region"));
    expect(await screen.findByText("ready-region-source")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "按主区域批量回填" }));
    expect(
      await screen.findByText("已按主区域回填 1 个 Source（cn-shanghai）。"),
    ).toBeInTheDocument();

    const backfillCall = fetchSpy.mock.calls.find(
      ([url, init]) =>
        toUrl(url).endsWith("/api/v1/sources/source-region/backfill") &&
        (init as RequestInit | undefined)?.method === "POST",
    );
    expect(backfillCall).toBeTruthy();
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

      if (
        url.endsWith("/api/v1/sources/source-loading/health") &&
        method === "GET"
      ) {
        return healthRequest;
      }

      if (
        url.includes("/api/v1/sources/source-loading/parse-failures") &&
        method === "GET"
      ) {
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
    expect(
      await screen.findByRole("heading", { name: "Sources 管理", level: 1 }),
    ).toBeInTheDocument();
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
      }),
    );

    expect(await screen.findByText(/^健康$/)).toBeInTheDocument();
    expect(
      await screen.findByText("最近暂无解析失败记录。"),
    ).toBeInTheDocument();
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

      if (
        url.endsWith("/api/v1/sources/source-error/health") &&
        method === "GET"
      ) {
        return mockJsonResponse(
          {
            message: "health probe timeout",
          },
          500,
        );
      }

      if (
        url.includes("/api/v1/sources/source-error/parse-failures") &&
        method === "GET"
      ) {
        return mockJsonResponse(
          {
            message: "parse failures service unavailable",
          },
          500,
        );
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Sources" }));
    expect(
      await screen.findByRole("heading", { name: "Sources 管理", level: 1 }),
    ).toBeInTheDocument();
    expect(
      await screen.findByText("健康状态加载失败：health probe timeout"),
    ).toBeInTheDocument();
    expect(
      await screen.findByText(
        "解析失败列表加载失败：parse failures service unavailable",
      ),
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

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
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

        if (
          url.endsWith("/api/v1/sources/source-connect-1/health") &&
          method === "GET"
        ) {
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

        if (
          url.includes("/api/v1/sources/source-connect-1/parse-failures") &&
          method === "GET"
        ) {
          return mockJsonResponse({
            items: [],
            total: 0,
            filters: {
              limit: 5,
            },
          });
        }

        if (
          url.endsWith("/api/v1/sources/test-connection") &&
          method === "POST"
        ) {
          const payload = JSON.parse(String(init?.body ?? "{}")) as {
            sourceId?: string;
          };
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

    expect(
      await screen.findByRole("heading", { name: "Sources 管理", level: 1 }),
    ).toBeInTheDocument();

    fireEvent.click(await screen.findByRole("button", { name: "测试连接" }));

    expect(
      await screen.findByText("成功 (87ms)：ssh handshake ok"),
    ).toBeInTheDocument();

    const postCall = fetchSpy.mock.calls.find(
      ([url, init]) =>
        toUrl(url).endsWith("/api/v1/sources/test-connection") &&
        (init as RequestInit | undefined)?.method === "POST",
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

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
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
    expect(
      await screen.findByRole("heading", { name: "聚合分析" }),
    ).toBeInTheDocument();
    expect(await screen.findByText("2026-02-11")).toBeInTheDocument();
    expect(await screen.findByText("2026-02")).toBeInTheDocument();
    expect((await screen.findAllByText("gpt-5")).length).toBeGreaterThan(0);
    expect(await screen.findByText("session-1")).toBeInTheDocument();
    expect(
      await screen.findByText("总成本趋势（monthly）"),
    ).toBeInTheDocument();
    expect(
      await screen.findByRole("img", { name: "monthly 总成本趋势图" }),
    ).toBeInTheDocument();
    expect((await screen.findAllByText("环比 +50.0%")).length).toBeGreaterThan(
      0,
    );
    expect(await screen.findByText("raw")).toBeInTheDocument();
    expect(await screen.findByText("estimated")).toBeInTheDocument();
    expect((await screen.findAllByText("total")).length).toBeGreaterThan(0);
    expect(
      (await screen.findAllByText("raw + estimated")).length,
    ).toBeGreaterThan(0);
    expect(await screen.findByText("$1.1000")).toBeInTheDocument();
    expect(screen.queryByText("$16.0000")).not.toBeInTheDocument();
    expect(screen.queryByText("legacy should not win")).not.toBeInTheDocument();

    expect(
      fetchSpy.mock.calls.some(([url]) =>
        toUrl(url).includes("/api/v1/usage/daily"),
      ),
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) =>
        toUrl(url).includes("/api/v1/usage/monthly"),
      ),
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) =>
        toUrl(url).includes("/api/v1/usage/models"),
      ),
    ).toBe(true);
    expect(
      fetchSpy.mock.calls.some(([url]) =>
        toUrl(url).includes("/api/v1/usage/sessions"),
      ),
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

      if (
        url.includes("/api/v1/sessions/session-1/events?limit=50") &&
        method === "GET"
      ) {
        return mockJsonResponse({ items: [], total: 0, limit: 50 });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Sessions" }));
    expect(
      await screen.findByRole("heading", { name: "会话中心" }),
    ).toBeInTheDocument();
    expect(
      await screen.findByRole("heading", { name: "会话详情" }),
    ).toBeInTheDocument();
    const freshnessSummary = await screen.findByLabelText("来源新鲜度");
    expect(freshnessSummary).toHaveTextContent("devbox-shanghai（hybrid）");
    expect(freshnessSummary).toHaveTextContent("新鲜度 7 分钟");
    expect(await screen.findByText("Token 分解")).toBeInTheDocument();
    expect(await screen.findByText("来源追溯")).toBeInTheDocument();
    expect(
      await screen.findByText("provider：session-detail-test"),
    ).toBeInTheDocument();
    expect(
      await screen.findByText("path：/workspace/main.ts"),
    ).toBeInTheDocument();
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
        searchPayloads.push(
          JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>,
        );
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
    expect(
      await screen.findByRole("heading", { name: "会话中心" }),
    ).toBeInTheDocument();
    await waitFor(() => {
      expect(searchPayloads.length).toBeGreaterThanOrEqual(1);
    });

    const dateKey = (screen.getByLabelText("日期") as HTMLInputElement).value;
    const nextDate = new Date(`${dateKey}T00:00:00.000Z`);
    nextDate.setUTCDate(nextDate.getUTCDate() + 1);
    const nextDateKey = nextDate.toISOString().slice(0, 10);

    fireEvent.change(screen.getByLabelText("关键词"), {
      target: { value: "deploy failed" },
    });
    fireEvent.change(screen.getByLabelText("客户端类型"), {
      target: { value: "cli" },
    });
    fireEvent.change(screen.getByLabelText("工具"), {
      target: { value: "Codex CLI" },
    });
    fireEvent.change(screen.getByLabelText("主机"), {
      target: { value: "devbox-01" },
    });
    fireEvent.change(screen.getByLabelText("模型"), {
      target: { value: "gpt-5-codex" },
    });
    fireEvent.change(screen.getByLabelText("项目"), {
      target: { value: "agentledger" },
    });
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
        const payload = JSON.parse(String(init?.body ?? "{}")) as Record<
          string,
          unknown
        >;
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

      if (
        url.includes(
          "/api/v1/sessions/session-1/events?limit=50&cursor=event-cursor-2",
        )
      ) {
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
    expect(
      await screen.findByRole("heading", { name: "会话中心" }),
    ).toBeInTheDocument();
    expect(await screen.findByText("source-1")).toBeInTheDocument();
    expect(
      await screen.findByRole("button", { name: "加载更多会话" }),
    ).toBeInTheDocument();
    expect(await screen.findByText("第一页事件")).toBeInTheDocument();
    expect(
      await screen.findByRole("button", { name: "加载更多事件" }),
    ).toBeInTheDocument();

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
        searchPayloads.push(
          JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>,
        );
        return mockJsonResponse({
          items: [],
          total: 0,
          nextCursor: null,
        });
      }

      throw new Error(`unexpected call: ${method} ${url}`);
    });

    render(<App />);
    expect(
      await screen.findByRole("heading", { name: "AI 使用热力图" }),
    ).toBeInTheDocument();

    fireEvent.click(
      await screen.findByRole("gridcell", { name: new RegExp(drillDateKey) }),
    );
    expect(
      await screen.findByRole("heading", { name: "会话中心" }),
    ).toBeInTheDocument();

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
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
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

    expect(
      await screen.findByRole("heading", { name: "登录控制台" }),
    ).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("邮箱"), {
      target: { value: "owner@example.com" },
    });
    fireEvent.change(screen.getByLabelText("密码"), {
      target: { value: "pwd-123456" },
    });
    fireEvent.click(screen.getByRole("button", { name: "登录" }));

    expect(
      await screen.findByRole("heading", { name: "AI 使用热力图" }),
    ).toBeInTheDocument();
    await waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/auth/login"),
        expect.objectContaining({ method: "POST" }),
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

      expect(
        await screen.findByRole("heading", { name: "登录控制台" }),
      ).toBeInTheDocument();
      fireEvent.click(await screen.findByRole("button", { name: "企业 OIDC" }));

      await waitFor(() => {
        expect(assignSpy).toHaveBeenCalledTimes(1);
      });

      const assignedUrl = new URL(String(assignSpy.mock.calls[0]?.[0]));
      const redirectUri = assignedUrl.searchParams.get("redirect_uri");
      const state = assignedUrl.searchParams.get("state");
      const codeChallenge = assignedUrl.searchParams.get("code_challenge");
      const codeChallengeMethod = assignedUrl.searchParams.get(
        "code_challenge_method",
      );

      expect(`${assignedUrl.origin}${assignedUrl.pathname}`).toBe(
        "https://idp.example.com/oauth/authorize",
      );
      expect(assignedUrl.searchParams.get("response_type")).toBe("code");
      expect(assignedUrl.searchParams.get("provider")).toBe("corp-oidc");
      expect(redirectUri).toContain("#/auth/callback");
      expect(state).toMatch(/^corp-oidc:/);
      expect(codeChallenge).toBeTruthy();
      expect(codeChallengeMethod).toBe("S256");

      const pendingRaw = window.sessionStorage.getItem(
        "agentledger.web-console.auth.external.pending",
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
          401,
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

    expect(
      await screen.findByRole("heading", { name: "登录控制台" }),
    ).toBeInTheDocument();
    expect(
      await screen.findByText("登录会话已失效，请重新登录。"),
    ).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("邮箱"), {
      target: { value: "owner@example.com" },
    });
    fireEvent.change(screen.getByLabelText("密码"), {
      target: { value: "pwd-654321" },
    });
    fireEvent.click(screen.getByRole("button", { name: "登录" }));

    expect(
      await screen.findByRole("heading", { name: "AI 使用热力图" }),
    ).toBeInTheDocument();
  });

  test("外部登录回调可自动 exchange 并进入控制台", async () => {
    const originalHash = window.location.hash;
    window.location.hash =
      "#/auth/callback?code=authorization-code-1&state=corp-oidc:nonce-1";
    window.sessionStorage.setItem(
      "agentledger.web-console.auth.external.pending",
      JSON.stringify({
        providerId: "corp-oidc",
        state: "corp-oidc:nonce-1",
        redirectUri: "http://localhost:5173/#/auth/callback",
        codeVerifier: "pkce-code-verifier-1",
        createdAt: Date.now(),
      }),
    );

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
        const url = toUrl(input);
        const method = (init?.method ?? "GET").toUpperCase();

        if (url.endsWith("/api/v1/auth/providers") && method === "GET") {
          return mockAuthProvidersResponse();
        }

        if (
          url.endsWith("/api/v1/auth/external/exchange") &&
          method === "POST"
        ) {
          const payload = JSON.parse(String(init?.body ?? "{}")) as Record<
            string,
            unknown
          >;
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

    expect(
      await screen.findByRole("heading", { name: "AI 使用热力图" }),
    ).toBeInTheDocument();
    await waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/auth/external/exchange"),
        expect.objectContaining({ method: "POST" }),
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
      "/?code=authorization-code-search-1&state=corp-oidc:nonce-search-1#/auth/callback",
    );
    window.sessionStorage.setItem(
      "agentledger.web-console.auth.external.pending",
      JSON.stringify({
        providerId: "corp-oidc",
        state: "corp-oidc:nonce-search-1",
        redirectUri: "http://localhost:5173/#/auth/callback",
        codeVerifier: "pkce-code-verifier-search-1",
        createdAt: Date.now(),
      }),
    );

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
        const url = toUrl(input);
        const method = (init?.method ?? "GET").toUpperCase();

        if (url.endsWith("/api/v1/auth/providers") && method === "GET") {
          return mockAuthProvidersResponse();
        }

        if (
          url.endsWith("/api/v1/auth/external/exchange") &&
          method === "POST"
        ) {
          const payload = JSON.parse(String(init?.body ?? "{}")) as Record<
            string,
            unknown
          >;
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

    expect(
      await screen.findByRole("heading", { name: "AI 使用热力图" }),
    ).toBeInTheDocument();
    await waitFor(() => {
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining("/api/v1/auth/external/exchange"),
        expect.objectContaining({ method: "POST" }),
      );
    });

    window.history.replaceState({}, "", originalHref);
    window.location.hash = originalHash;
  });

  test(
    "治理页展示告警工作台与导出中心",
    async () => {
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

    expect(
      await screen.findByRole("heading", { name: "治理中心" }),
    ).toBeInTheDocument();
    expect(
      await screen.findByRole("heading", { name: "周报摘要", level: 2 }),
    ).toBeInTheDocument();
    expect(
      await screen.findByRole("heading", { name: "告警工作台", level: 2 }),
    ).toBeInTheDocument();
    expect(
      await screen.findByRole("heading", { name: "导出中心", level: 2 }),
    ).toBeInTheDocument();
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test(
    "治理页支持告警级别与状态筛选",
    { timeout: GOVERNANCE_HEAVY_TEST_TIMEOUT_MS },
    async () => {
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

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
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

    expect(
      await screen.findByRole("heading", { name: "告警工作台", level: 2 }),
    ).toBeInTheDocument();
    expect(
      await screen.findByText("warning budget approaching"),
    ).toBeInTheDocument();
    expect(
      await screen.findByText("critical unresolved incident"),
    ).toBeInTheDocument();
    expect(
      await screen.findByText("critical already resolved"),
    ).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("级别"), {
      target: { value: "critical" },
    });
    await waitFor(
      () => {
        expect(
          fetchSpy.mock.calls.some(([url]) => {
            const parsedUrl = new URL(toUrl(url), "http://localhost");
            return (
              parsedUrl.pathname === "/api/v1/alerts" &&
              parsedUrl.searchParams.get("severity") === "critical"
            );
          }),
        ).toBe(true);
      },
      { timeout: 15000 },
    );

    expect(
      await screen.findByText("critical unresolved incident"),
    ).toBeInTheDocument();
    expect(
      await screen.findByText("critical already resolved"),
    ).toBeInTheDocument();
    await waitFor(() => {
      expect(
        screen.queryByText("warning budget approaching"),
      ).not.toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText("状态"), {
      target: { value: "resolved" },
    });

    await waitFor(
      () => {
        expect(
          fetchSpy.mock.calls.some(([url]) => {
            const parsedUrl = new URL(toUrl(url), "http://localhost");
            return (
              parsedUrl.pathname === "/api/v1/alerts" &&
              parsedUrl.searchParams.get("severity") === "critical" &&
              parsedUrl.searchParams.get("status") === "resolved"
            );
          }),
        ).toBe(true);
      },
      { timeout: 15000 },
    );

    expect(
      await screen.findByText("critical already resolved"),
    ).toBeInTheDocument();
    await waitFor(() => {
      expect(
        screen.queryByText("critical unresolved incident"),
      ).not.toBeInTheDocument();
    });

    expect(
      fetchSpy.mock.calls.some(([url]) => {
        const parsedUrl = new URL(toUrl(url), "http://localhost");
        return (
          parsedUrl.pathname === "/api/v1/alerts" &&
          parsedUrl.searchParams.get("severity") === "critical" &&
          parsedUrl.searchParams.get("status") === "resolved"
        );
      }),
    ).toBe(true);
    },
  );

  test(
    "治理页支持 ACK 告警状态",
    async () => {
    window.location.hash = "#/governance";
    setAuthTokens({
      accessToken: "access-token-governance-alert",
      refreshToken: "refresh-token-governance-alert",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
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

        if (
          url.endsWith("/api/v1/alerts/alert-ui-1/status") &&
          method === "PATCH"
        ) {
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
    expect(
      await screen.findByText("告警 alert-ui-1 已更新为 acknowledged。"),
    ).toBeInTheDocument();

    const patchCall = fetchSpy.mock.calls.find(
      ([url, init]) =>
        toUrl(url).endsWith("/api/v1/alerts/alert-ui-1/status") &&
        (init as RequestInit | undefined)?.method === "PATCH",
    );
    expect(patchCall).toBeTruthy();
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test(
    "治理页支持 Resolve 告警状态并展示完成反馈",
    async () => {
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

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
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

        if (
          url.endsWith("/api/v1/alerts/alert-ui-resolve/status") &&
          method === "PATCH"
        ) {
          const payload = JSON.parse(String(init?.body ?? "{}")) as {
            status?: string;
          };
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

    expect(await screen.findByText("resolve me")).toBeInTheDocument();
    fireEvent.click(await screen.findByRole("button", { name: "Resolve" }));

    await waitFor(
      () => {
        expect(
          screen.getByText("告警 alert-ui-resolve 已更新为 resolved。"),
        ).toBeInTheDocument();
        expect(screen.getByText("已完成")).toBeInTheDocument();
      },
      { timeout: 15000 },
    );

    await waitFor(() => {
      expect(
        screen.queryByRole("button", { name: "Resolve" }),
      ).not.toBeInTheDocument();
    });

    const patchCall = fetchSpy.mock.calls.find(
      ([url, init]) =>
        toUrl(url).endsWith("/api/v1/alerts/alert-ui-resolve/status") &&
        (init as RequestInit | undefined)?.method === "PATCH",
    );
    expect(patchCall).toBeTruthy();
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test(
    "治理页支持告警编排规则筛选、保存规范化、模拟与执行日志加载",
    async () => {
      window.location.hash = "#/governance";
      setAuthTokens({
        accessToken: "access-token-governance-orchestration",
        refreshToken: "refresh-token-governance-orchestration",
        expiresIn: 1800,
        tokenType: "Bearer",
      });

      const orchestrationRules = [
        {
          id: "rule-ui-1",
          tenantId: "default",
          name: "critical dedupe",
          enabled: true,
          eventType: "alert",
          severity: "critical",
          sourceId: "source-1",
          dedupeWindowSeconds: 300,
          suppressionWindowSeconds: 120,
          mergeWindowSeconds: 60,
          slaMinutes: 15,
          channels: ["webhook", "wecom"],
          updatedAt: "2026-03-03T10:00:00.000Z",
        },
      ];
      const orchestrationExecutions: Array<{
        id: string;
        tenantId: string;
        ruleId: string;
        eventType: "alert" | "weekly";
        alertId?: string;
        severity?: "warning" | "critical";
        sourceId?: string;
        channels: string[];
        dispatchMode: "rule" | "fallback";
        conflictRuleIds: string[];
        dedupeHit: boolean;
        suppressed: boolean;
        simulated: boolean;
        metadata: Record<string, unknown>;
        createdAt: string;
      }> = [];
      const orchestrationRuleQuerySnapshots: Array<Record<string, string>> = [];
      const orchestrationExecutionQuerySnapshots: Array<
        Record<string, string>
      > = [];
      const orchestrationRuleUpsertPayloads: Array<{
        ruleId: string;
        payload: {
          name?: string;
          enabled?: boolean;
          eventType?: "alert" | "weekly";
          severity?: "warning" | "critical";
          sourceId?: string;
          dedupeWindowSeconds?: number;
          suppressionWindowSeconds?: number;
          mergeWindowSeconds?: number;
          slaMinutes?: number;
          channels?: string[];
        };
      }> = [];
      const orchestrationSimulationPayloads: Array<{
        ruleId?: string;
        eventType?: "alert" | "weekly";
        alertId?: string;
        severity?: "warning" | "critical";
        sourceId?: string;
        dedupeHit?: boolean;
        suppressed?: boolean;
      }> = [];

      const fetchSpy = mockGovernancePageFetch({
        extraHandler: async (_input, init, { method, pathname, url }) => {
          if (
            pathname === "/api/v1/alerts/orchestration/rules" &&
            method === "GET"
          ) {
            const parsedUrl = new URL(url, "http://localhost");
            orchestrationRuleQuerySnapshots.push(
              Object.fromEntries(parsedUrl.searchParams.entries()),
            );
            const eventType = parsedUrl.searchParams.get("eventType");
            const severity = parsedUrl.searchParams.get("severity");
            const sourceId = parsedUrl.searchParams.get("sourceId");
            const enabled = parsedUrl.searchParams.get("enabled");
            const filtered = orchestrationRules.filter((item) => {
              if (eventType && item.eventType !== eventType) {
                return false;
              }
              if (severity && item.severity !== severity) {
                return false;
              }
              if (sourceId && item.sourceId !== sourceId) {
                return false;
              }
              if (enabled === "true" && item.enabled !== true) {
                return false;
              }
              if (enabled === "false" && item.enabled !== false) {
                return false;
              }
              return true;
            });
            return mockJsonResponse({
              items: filtered,
              total: filtered.length,
              filters: {
                ...(eventType ? { eventType } : {}),
                ...(severity ? { severity } : {}),
                ...(sourceId ? { sourceId } : {}),
                ...(enabled ? { enabled: enabled === "true" } : {}),
              },
            });
          }

          if (
            pathname.startsWith("/api/v1/alerts/orchestration/rules/") &&
            method === "PUT"
          ) {
            const ruleId = decodeURIComponent(pathname.split("/").pop() ?? "");
            const payload = JSON.parse(String(init?.body ?? "{}")) as {
              name?: string;
              enabled?: boolean;
              eventType?: "alert" | "weekly";
              severity?: "warning" | "critical";
              sourceId?: string;
              dedupeWindowSeconds?: number;
              suppressionWindowSeconds?: number;
              mergeWindowSeconds?: number;
              slaMinutes?: number;
              channels?: string[];
            };
            orchestrationRuleUpsertPayloads.push({ ruleId, payload });
            const nextRule = {
              id: ruleId,
              tenantId: "default",
              name: payload.name ?? ruleId,
              enabled: payload.enabled === true,
              eventType: payload.eventType ?? "alert",
              severity: payload.severity,
              sourceId: payload.sourceId,
              dedupeWindowSeconds: payload.dedupeWindowSeconds ?? 0,
              suppressionWindowSeconds: payload.suppressionWindowSeconds ?? 0,
              mergeWindowSeconds: payload.mergeWindowSeconds ?? 0,
              slaMinutes: payload.slaMinutes,
              channels: payload.channels ?? ["webhook"],
              updatedAt: "2026-03-03T11:00:00.000Z",
            };
            const existingIndex = orchestrationRules.findIndex(
              (item) => item.id === ruleId,
            );
            if (existingIndex >= 0) {
              orchestrationRules[existingIndex] = nextRule;
            } else {
              orchestrationRules.unshift(nextRule);
            }
            return mockJsonResponse(nextRule);
          }

          if (
            pathname === "/api/v1/alerts/orchestration/simulate" &&
            method === "POST"
          ) {
            const payload = JSON.parse(String(init?.body ?? "{}")) as {
              ruleId?: string;
              eventType?: "alert" | "weekly";
              alertId?: string;
              severity?: "warning" | "critical";
              sourceId?: string;
              dedupeHit?: boolean;
              suppressed?: boolean;
            };
            orchestrationSimulationPayloads.push(payload);
            const matchedRules = orchestrationRules.filter((item) => {
              if (payload.ruleId && item.id !== payload.ruleId) {
                return false;
              }
              if (payload.eventType && item.eventType !== payload.eventType) {
                return false;
              }
              return item.enabled;
            });
            const executions = matchedRules.map((item, index) => {
              const execution = {
                id: `exec-ui-${orchestrationExecutions.length + index + 1}`,
                tenantId: "default",
                ruleId: item.id,
                eventType: payload.eventType ?? "alert",
                alertId: payload.alertId,
                severity: payload.severity,
                sourceId: payload.sourceId,
                channels: item.channels,
                dispatchMode: "rule",
                conflictRuleIds: [],
                dedupeHit: payload.dedupeHit === true,
                suppressed: payload.suppressed === true,
                simulated: true,
                metadata: {
                  dispatchMode: "rule",
                  attempt: 2,
                },
                createdAt: "2026-03-03T11:30:00.000Z",
              };
              orchestrationExecutions.unshift(execution);
              return execution;
            });
            return mockJsonResponse({
              matchedRules,
              conflictRuleIds: [],
              executions,
            });
          }

          if (
            pathname === "/api/v1/alerts/orchestration/executions" &&
            method === "GET"
          ) {
            const parsedUrl = new URL(url, "http://localhost");
            orchestrationExecutionQuerySnapshots.push(
              Object.fromEntries(parsedUrl.searchParams.entries()),
            );
            const ruleId = parsedUrl.searchParams.get("ruleId");
            const eventType = parsedUrl.searchParams.get("eventType");
            const severity = parsedUrl.searchParams.get("severity");
            const sourceId = parsedUrl.searchParams.get("sourceId");
            const dedupeHit = parsedUrl.searchParams.get("dedupeHit");
            const suppressed = parsedUrl.searchParams.get("suppressed");
            const dispatchMode = parsedUrl.searchParams.get("dispatchMode");
            const hasConflict = parsedUrl.searchParams.get("hasConflict");
            const simulated = parsedUrl.searchParams.get("simulated");
            const from = parsedUrl.searchParams.get("from");
            const to = parsedUrl.searchParams.get("to");
            const filtered = orchestrationExecutions.filter((item) => {
              if (ruleId && item.ruleId !== ruleId) {
                return false;
              }
              if (eventType && item.eventType !== eventType) {
                return false;
              }
              if (severity && item.severity !== severity) {
                return false;
              }
              if (sourceId && item.sourceId !== sourceId) {
                return false;
              }
              if (dedupeHit === "true" && item.dedupeHit !== true) {
                return false;
              }
              if (dedupeHit === "false" && item.dedupeHit !== false) {
                return false;
              }
              if (suppressed === "true" && item.suppressed !== true) {
                return false;
              }
              if (suppressed === "false" && item.suppressed !== false) {
                return false;
              }
              if (dispatchMode && item.dispatchMode !== dispatchMode) {
                return false;
              }
              if (hasConflict === "true" && item.conflictRuleIds.length === 0) {
                return false;
              }
              if (hasConflict === "false" && item.conflictRuleIds.length > 0) {
                return false;
              }
              if (simulated === "true" && item.simulated !== true) {
                return false;
              }
              if (simulated === "false" && item.simulated !== false) {
                return false;
              }
              if (from && Date.parse(item.createdAt) < Date.parse(from)) {
                return false;
              }
              if (to && Date.parse(item.createdAt) > Date.parse(to)) {
                return false;
              }
              return true;
            });
            return mockJsonResponse({
              items: filtered,
              total: filtered.length,
              filters: {
                ...(ruleId ? { ruleId } : {}),
                ...(eventType ? { eventType } : {}),
                ...(severity ? { severity } : {}),
                ...(sourceId ? { sourceId } : {}),
                ...(dedupeHit ? { dedupeHit: dedupeHit === "true" } : {}),
                ...(suppressed ? { suppressed: suppressed === "true" } : {}),
                ...(dispatchMode ? { dispatchMode } : {}),
                ...(hasConflict ? { hasConflict: hasConflict === "true" } : {}),
                ...(simulated ? { simulated: simulated === "true" } : {}),
                ...(from ? { from } : {}),
                ...(to ? { to } : {}),
              },
            });
          }

          return undefined;
        },
      });

      render(<App />);

      const section = (
        await screen.findByRole("heading", { name: "告警编排中心", level: 2 })
      ).closest("section");
      expect(section).not.toBeNull();
      const sectionScreen = within(section as HTMLElement);
      const byId = <T extends HTMLElement>(id: string) => {
        const element = (section as HTMLElement).querySelector(`#${id}`);
        expect(element).not.toBeNull();
        return element as T;
      };

      expect(
        sectionScreen.getByText("尚未加载规则，请点击“加载编排规则”。"),
      ).toBeInTheDocument();
      expect(
        sectionScreen.getByText("尚未加载执行日志，请点击“加载执行日志”。"),
      ).toBeInTheDocument();

      fireEvent.change(
        byId<HTMLInputElement>("orchestration-rule-source-filter"),
        {
          target: { value: "missing-source" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载编排规则" }),
      );
      expect(
        await sectionScreen.findByText("无匹配规则。"),
      ).toBeInTheDocument();

      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-rule-event-type-filter"),
        {
          target: { value: "alert" },
        },
      );
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-rule-enabled-filter"),
        {
          target: { value: "true" },
        },
      );
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-rule-severity-filter"),
        {
          target: { value: "critical" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>("orchestration-rule-source-filter"),
        {
          target: { value: " source-1 " },
        },
      );

      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载编排规则" }),
      );
      expect(await sectionScreen.findByText("rule-ui-1")).toBeInTheDocument();
      const latestRuleQuerySnapshot =
        orchestrationRuleQuerySnapshots[
          orchestrationRuleQuerySnapshots.length - 1
        ];
      expect(latestRuleQuerySnapshot).toEqual(
        expect.objectContaining({
          eventType: "alert",
          enabled: "true",
          severity: "critical",
          sourceId: "source-1",
        }),
      );

      fireEvent.click(sectionScreen.getByRole("button", { name: "载入" }));
      expect(
        byId<HTMLInputElement>("orchestration-simulate-rule-id").value,
      ).toBe("rule-ui-1");

      fireEvent.change(sectionScreen.getByLabelText("Rule ID（规则）"), {
        target: { value: " rule-ui-2 " },
      });
      fireEvent.change(sectionScreen.getByLabelText("名称"), {
        target: { value: " weekly fanout " },
      });
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-rule-event-type"),
        {
          target: { value: "weekly" },
        },
      );
      fireEvent.change(byId<HTMLSelectElement>("orchestration-rule-severity"), {
        target: { value: "warning" },
      });
      fireEvent.change(byId<HTMLInputElement>("orchestration-rule-source-id"), {
        target: { value: " source-2 " },
      });
      fireEvent.change(
        byId<HTMLInputElement>("orchestration-rule-dedupe-window"),
        {
          target: { value: "301" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>("orchestration-rule-suppression-window"),
        {
          target: { value: "121" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>("orchestration-rule-merge-window"),
        {
          target: { value: "61" },
        },
      );
      fireEvent.change(byId<HTMLInputElement>("orchestration-rule-sla"), {
        target: { value: "30" },
      });
      fireEvent.change(sectionScreen.getByLabelText("Channels（逗号分隔）"), {
        target: { value: " WEBHOOK,email,webhook " },
      });
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "保存编排规则" }),
      );
      expect(
        await sectionScreen.findByText("编排规则 rule-ui-2 已保存。"),
      ).toBeInTheDocument();
      const latestUpsertPayload =
        orchestrationRuleUpsertPayloads[
          orchestrationRuleUpsertPayloads.length - 1
        ];
      expect(latestUpsertPayload).toEqual(
        expect.objectContaining({
          ruleId: "rule-ui-2",
        }),
      );
      expect(latestUpsertPayload?.payload).toEqual(
        expect.objectContaining({
          name: "weekly fanout",
          enabled: true,
          eventType: "weekly",
          severity: "warning",
          sourceId: "source-2",
          dedupeWindowSeconds: 301,
          suppressionWindowSeconds: 121,
          mergeWindowSeconds: 61,
          slaMinutes: 30,
          channels: ["webhook", "email"],
        }),
      );

      fireEvent.change(sectionScreen.getByLabelText("指定 Rule ID（可选）"), {
        target: { value: " rule-ui-2 " },
      });
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-simulate-event-type"),
        {
          target: { value: "weekly" },
        },
      );
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-simulate-severity"),
        {
          target: { value: "warning" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>("orchestration-simulate-source-id"),
        {
          target: { value: " source-2 " },
        },
      );
      fireEvent.click(sectionScreen.getByRole("button", { name: "执行模拟" }));
      expect(
        await sectionScreen.findByText("模拟完成：命中 1 条规则，冲突 0 条。"),
      ).toBeInTheDocument();
      expect(sectionScreen.getByText("命中规则 ID")).toBeInTheDocument();
      const latestSimulationPayload =
        orchestrationSimulationPayloads[
          orchestrationSimulationPayloads.length - 1
        ];
      expect(latestSimulationPayload).toEqual(
        expect.objectContaining({
          ruleId: "rule-ui-2",
          eventType: "weekly",
          severity: "warning",
          sourceId: "source-2",
        }),
      );
      orchestrationExecutions.push({
        id: "exec-ui-simulated",
        tenantId: "default",
        ruleId: "rule-ui-2",
        eventType: "weekly",
        severity: "warning",
        sourceId: "source-2",
        channels: ["webhook"],
        dispatchMode: "fallback",
        conflictRuleIds: ["rule-ui-1"],
        dedupeHit: false,
        suppressed: false,
        simulated: true,
        metadata: {},
        createdAt: "2026-03-03T11:10:00.000Z",
      });

      fireEvent.change(
        byId<HTMLInputElement>("orchestration-execution-rule-id-filter"),
        {
          target: { value: "rule-not-exist" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载执行日志" }),
      );
      expect(
        await sectionScreen.findByText("无匹配执行日志。"),
      ).toBeInTheDocument();

      fireEvent.change(sectionScreen.getByLabelText("Rule ID（日志）"), {
        target: { value: " rule-ui-2 " },
      });
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-execution-event-type-filter"),
        {
          target: { value: "weekly" },
        },
      );
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-execution-severity-filter"),
        {
          target: { value: "warning" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>("orchestration-execution-source-id-filter"),
        {
          target: { value: " source-2 " },
        },
      );
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-execution-dedupe-hit-filter"),
        {
          target: { value: "false" },
        },
      );
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-execution-suppressed-filter"),
        {
          target: { value: "false" },
        },
      );
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-execution-dispatch-mode-filter"),
        {
          target: { value: "rule" },
        },
      );
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-execution-conflict-filter"),
        {
          target: { value: "false" },
        },
      );
      fireEvent.change(
        byId<HTMLSelectElement>("orchestration-execution-simulated-filter"),
        {
          target: { value: "true" },
        },
      );
      fireEvent.change(byId<HTMLInputElement>("orchestration-execution-from"), {
        target: { value: "2026-03-03T00:00" },
      });
      fireEvent.change(byId<HTMLInputElement>("orchestration-execution-to"), {
        target: { value: "2026-03-03T23:59" },
      });
      fireEvent.change(
        byId<HTMLInputElement>("orchestration-execution-limit"),
        {
          target: { value: "25" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载执行日志" }),
      );
      expect(await sectionScreen.findByText("exec-ui-1")).toBeInTheDocument();
      expect(
        sectionScreen.queryByText("exec-ui-simulated"),
      ).not.toBeInTheDocument();
      const executionMetadataCell = await sectionScreen.findByText(
        '{"dispatchMode":"rule","attempt":2}',
      );
      const executionRow = executionMetadataCell.closest("tr");
      expect(executionRow).not.toBeNull();
      const executionCells = within(executionRow as HTMLElement).getAllByRole(
        "cell",
      );
      expect(executionCells[3]?.textContent).toBe("rule");
      expect(executionCells[9]?.textContent).toBe("--");
      expect(executionCells[11]?.textContent).toBe(
        '{"dispatchMode":"rule","attempt":2}',
      );
      const latestExecutionQuerySnapshot =
        orchestrationExecutionQuerySnapshots[
          orchestrationExecutionQuerySnapshots.length - 1
        ];
      expect(latestExecutionQuerySnapshot).toEqual(
        expect.objectContaining({
          ruleId: "rule-ui-2",
          eventType: "weekly",
          severity: "warning",
          sourceId: "source-2",
          dedupeHit: "false",
          suppressed: "false",
          dispatchMode: "rule",
          hasConflict: "false",
          simulated: "true",
          from: "2026-03-03T00:00",
          to: "2026-03-03T23:59",
          limit: "25",
        }),
      );
      expect(
        (section as HTMLElement).querySelector(
          "#orchestration-rule-id-options option[value='rule-ui-1']",
        ),
      ).not.toBeNull();
      expect(
        (section as HTMLElement).querySelector(
          "#orchestration-source-id-options option[value='source-1']",
        ),
      ).not.toBeNull();

      expect(
        fetchSpy.mock.calls.some(
          ([url, init]) =>
            toUrl(url).includes(
              "/api/v1/alerts/orchestration/rules/rule-ui-2",
            ) &&
            (
              (init as RequestInit | undefined)?.method ?? "GET"
            ).toUpperCase() === "PUT",
        ),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(
          ([url, init]) =>
            toUrl(url).includes("/api/v1/alerts/orchestration/simulate") &&
            (
              (init as RequestInit | undefined)?.method ?? "GET"
            ).toUpperCase() === "POST",
        ),
      ).toBe(true);
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test("治理页告警编排非法输入时会阻止请求", async () => {
    window.location.hash = "#/governance";
    setAuthTokens({
      accessToken: "access-token-governance-orchestration-invalid",
      refreshToken: "refresh-token-governance-orchestration-invalid",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const orchestrationExecutionQuerySnapshots: Array<Record<string, string>> =
      [];

    const fetchSpy = mockGovernancePageFetch({
      extraHandler: async (_input, init, { method, pathname, url }) => {
        if (
          pathname === "/api/v1/alerts/orchestration/rules" &&
          method === "GET"
        ) {
          return mockJsonResponse({
            items: [],
            total: 0,
            filters: {},
          });
        }
        if (
          pathname.startsWith("/api/v1/alerts/orchestration/rules/") &&
          method === "PUT"
        ) {
          const ruleId = decodeURIComponent(pathname.split("/").pop() ?? "");
          const payload = JSON.parse(String(init?.body ?? "{}")) as {
            name?: string;
            channels?: string[];
          };
          return mockJsonResponse({
            id: ruleId,
            tenantId: "default",
            name: payload.name ?? ruleId,
            enabled: true,
            eventType: "alert",
            dedupeWindowSeconds: 0,
            suppressionWindowSeconds: 0,
            mergeWindowSeconds: 0,
            channels: payload.channels ?? ["webhook"],
            updatedAt: "2026-03-03T12:00:00.000Z",
          });
        }
        if (
          pathname === "/api/v1/alerts/orchestration/simulate" &&
          method === "POST"
        ) {
          return mockJsonResponse({
            matchedRules: [],
            conflictRuleIds: [],
            executions: [],
          });
        }
        if (
          pathname === "/api/v1/alerts/orchestration/executions" &&
          method === "GET"
        ) {
          const parsedUrl = new URL(url, "http://localhost");
          orchestrationExecutionQuerySnapshots.push(
            Object.fromEntries(parsedUrl.searchParams.entries()),
          );
          return mockJsonResponse({
            items: [],
            total: 0,
            filters: {},
          });
        }
        return undefined;
      },
    });

    render(<App />);

    const section = (
      await screen.findByRole("heading", { name: "告警编排中心", level: 2 })
    ).closest("section");
    expect(section).not.toBeNull();
    const sectionScreen = within(section as HTMLElement);
    const byId = <T extends HTMLElement>(id: string) => {
      const element = (section as HTMLElement).querySelector(`#${id}`);
      expect(element).not.toBeNull();
      return element as T;
    };
    const rulePutCalls = () =>
      fetchSpy.mock.calls.filter(([url, init]) => {
        const requestInit = init as RequestInit | undefined;
        return (
          new URL(toUrl(url), "http://localhost").pathname.startsWith(
            "/api/v1/alerts/orchestration/rules/",
          ) && (requestInit?.method ?? "GET").toUpperCase() === "PUT"
        );
      });

    fireEvent.change(byId<HTMLInputElement>("orchestration-rule-id"), {
      target: { value: "   " },
    });
    fireEvent.change(byId<HTMLInputElement>("orchestration-rule-name"), {
      target: { value: "rule name" },
    });
    fireEvent.change(byId<HTMLInputElement>("orchestration-rule-channels"), {
      target: { value: "webhook" },
    });
    fireEvent.click(
      sectionScreen.getByRole("button", { name: "保存编排规则" }),
    );
    expect(
      await sectionScreen.findByText("Rule ID 不能为空。"),
    ).toBeInTheDocument();
    expect(rulePutCalls()).toHaveLength(0);

    fireEvent.change(byId<HTMLInputElement>("orchestration-rule-id"), {
      target: { value: "rule-invalid-ui" },
    });
    fireEvent.change(byId<HTMLInputElement>("orchestration-rule-name"), {
      target: { value: "   " },
    });
    fireEvent.click(
      sectionScreen.getByRole("button", { name: "保存编排规则" }),
    );
    expect(
      await sectionScreen.findByText("规则名称不能为空。"),
    ).toBeInTheDocument();
    expect(rulePutCalls()).toHaveLength(0);

    fireEvent.change(byId<HTMLInputElement>("orchestration-rule-name"), {
      target: { value: "rule-invalid-ui" },
    });
    fireEvent.change(
      byId<HTMLInputElement>("orchestration-rule-dedupe-window"),
      {
        target: { value: "-1" },
      },
    );
    fireEvent.click(
      sectionScreen.getByRole("button", { name: "保存编排规则" }),
    );
    expect(
      await sectionScreen.findByText("去重/抑制/合并窗口必须是非负整数。"),
    ).toBeInTheDocument();
    expect(rulePutCalls()).toHaveLength(0);

    fireEvent.change(
      byId<HTMLInputElement>("orchestration-rule-dedupe-window"),
      {
        target: { value: "0" },
      },
    );
    fireEvent.change(byId<HTMLInputElement>("orchestration-rule-channels"), {
      target: { value: "webhook,unknown" },
    });
    fireEvent.click(
      sectionScreen.getByRole("button", { name: "保存编排规则" }),
    );
    expect(
      await sectionScreen.findByText((content) =>
        content.includes("存在不支持的 channels：unknown"),
      ),
    ).toBeInTheDocument();
    expect(rulePutCalls()).toHaveLength(0);

    fireEvent.change(byId<HTMLInputElement>("orchestration-rule-channels"), {
      target: { value: " , " },
    });
    fireEvent.click(
      sectionScreen.getByRole("button", { name: "保存编排规则" }),
    );
    expect(
      await sectionScreen.findByText("至少选择一个合法 channel。"),
    ).toBeInTheDocument();
    expect(rulePutCalls()).toHaveLength(0);

    fireEvent.change(byId<HTMLInputElement>("orchestration-rule-channels"), {
      target: { value: "webhook" },
    });
    fireEvent.click(
      sectionScreen.getByRole("button", { name: "保存编排规则" }),
    );
    expect(
      await sectionScreen.findByText("编排规则 rule-invalid-ui 已保存。"),
    ).toBeInTheDocument();
    expect(rulePutCalls()).toHaveLength(1);

    const beforeInvalidTimeRange = orchestrationExecutionQuerySnapshots.length;
    fireEvent.change(byId<HTMLInputElement>("orchestration-execution-from"), {
      target: { value: "2026-03-04T12:00" },
    });
    fireEvent.change(byId<HTMLInputElement>("orchestration-execution-to"), {
      target: { value: "2026-03-04T11:00" },
    });
    fireEvent.click(
      sectionScreen.getByRole("button", { name: "加载执行日志" }),
    );
    expect(
      await sectionScreen.findByText(
        "执行日志筛选时间范围非法：from 不能晚于 to。",
      ),
    ).toBeInTheDocument();
    expect(orchestrationExecutionQuerySnapshots).toHaveLength(
      beforeInvalidTimeRange,
    );
  });

  test(
    "治理页开放平台工作台支持 OpenAPI/API Key/Webhook/Quality/Replay 基础联调",
    async () => {
      window.location.hash = "#/governance";
      setAuthTokens({
        accessToken: "access-token-governance-open-platform",
        refreshToken: "refresh-token-governance-open-platform",
        expiresIn: 1800,
        tokenType: "Bearer",
      });

      const apiKeys: Array<{
        id: string;
        tenantId: string;
        name: string;
        scope: "read" | "write" | "admin";
        status: "active" | "revoked";
        keyPrefix: string;
        createdAt: string;
        updatedAt: string;
        lastUsedAt?: string;
      }> = [
        {
          id: "ak-ui-1",
          tenantId: "default",
          name: "ci-agent",
          scope: "read",
          status: "active",
          keyPrefix: "sk_live_abc123",
          createdAt: "2026-03-03T10:00:00.000Z",
          updatedAt: "2026-03-03T10:00:00.000Z",
        },
      ];
      const webhooks: Array<{
        id: string;
        tenantId: string;
        name: string;
        url: string;
        events: string[];
        status: "active" | "paused" | "disabled";
        createdAt: string;
        updatedAt: string;
      }> = [
        {
          id: "wh-ui-1",
          tenantId: "default",
          name: "alerts-primary",
          url: "https://hooks.example.com/alerts",
          events: ["api_key.created", "api_key.revoked"],
          status: "active",
          createdAt: "2026-03-03T10:00:00.000Z",
          updatedAt: "2026-03-03T10:00:00.000Z",
        },
      ];
      const replayDatasets: Array<{
        id: string;
        tenantId: string;
        name: string;
        datasetId: string;
        model: string;
        promptVersion?: string;
        sampleCount: number;
        metadata: Record<string, unknown>;
        createdAt: string;
        updatedAt: string;
      }> = [
        {
          id: "baseline-ui-1",
          tenantId: "default",
          name: "baseline smoke",
          datasetId: "dataset-1",
          model: "gpt-5-codex",
          promptVersion: "v1",
          caseCount: 50,
          sampleCount: 50,
          metadata: {},
          createdAt: "2026-03-03T12:10:00.000Z",
          updatedAt: "2026-03-03T12:10:00.000Z",
        },
      ];
      const replayDatasetCases = [
        {
          datasetId: "baseline-ui-1",
          caseId: "case-1",
          sortOrder: 0,
          input: "Summarize the change",
          expectedOutput: "A concise summary",
          metadata: { priority: "p0" },
          createdAt: "2026-03-03T12:12:00.000Z",
          updatedAt: "2026-03-03T12:12:00.000Z",
        },
      ];
      const replayRuns: Array<{
        id: string;
        runId?: string;
        jobId?: string;
        tenantId: string;
        datasetId?: string;
        baselineId: string;
        candidateLabel: string;
        status: "pending" | "running" | "completed" | "failed" | "cancelled";
        totalCases: number;
        processedCases: number;
        improvedCases: number;
        regressedCases: number;
        unchangedCases: number;
        diffs: unknown[];
        summary: Record<string, unknown>;
        createdAt: string;
        updatedAt: string;
        startedAt?: string;
        finishedAt?: string;
      }> = [
        {
          id: "job-ui-1",
          runId: "job-ui-1",
          jobId: "job-ui-1",
          tenantId: "default",
          datasetId: "baseline-ui-1",
          baselineId: "baseline-ui-1",
          candidateLabel: "candidate-v2",
          status: "completed",
          totalCases: 10,
          processedCases: 10,
          improvedCases: 6,
          regressedCases: 2,
          unchangedCases: 2,
          diffs: [],
          summary: {},
          createdAt: "2026-03-03T12:20:00.000Z",
          updatedAt: "2026-03-03T12:20:00.000Z",
          startedAt: "2026-03-03T12:20:00.000Z",
          finishedAt: "2026-03-03T12:22:00.000Z",
        },
      ];

      const fetchSpy = mockGovernancePageFetch({
        extraHandler: async (_input, init, { method, pathname, url }) => {
          if (pathname === "/api/v1/openapi.json" && method === "GET") {
            return mockJsonResponse({
              openapi: "3.0.3",
              info: {
                title: "AgentLedger Control Plane API",
                version: "1.1.0",
              },
              paths: {
                "/api/v1/api-keys": {
                  get: { tags: ["open-platform"] },
                  post: { tags: ["open-platform"] },
                },
                "/api/v2/replay/runs/{id}/diffs": {
                  get: { tags: ["replay"] },
                },
              },
            });
          }

          if (pathname === "/api/v1/api-keys" && method === "GET") {
            const parsedUrl = new URL(url, "http://localhost");
            const status = parsedUrl.searchParams.get("status");
            const keyword = parsedUrl.searchParams.get("keyword");
            const limitRaw = parsedUrl.searchParams.get("limit");
            const limit = limitRaw ? Number(limitRaw) : 50;
            const filtered = apiKeys
              .filter((item) => (status ? item.status === status : true))
              .filter((item) =>
                keyword
                  ? item.id.includes(keyword) ||
                    item.name.includes(keyword) ||
                    item.keyPrefix.includes(keyword)
                  : true,
              )
              .slice(0, Number.isFinite(limit) ? limit : 50);
            return mockJsonResponse({
              items: filtered,
              total: filtered.length,
              filters: {
                ...(status ? { status } : {}),
                ...(keyword ? { keyword } : {}),
                ...(limitRaw ? { limit } : {}),
              },
            });
          }

          if (pathname === "/api/v1/api-keys" && method === "POST") {
            const payload = JSON.parse(String(init?.body ?? "{}")) as {
              name?: string;
              scope?: string;
            };
            const created = {
              id: `ak-ui-${apiKeys.length + 1}`,
              tenantId: "default",
              name: payload.name ?? "unnamed",
              scope: (payload.scope === "write" || payload.scope === "admin"
                ? payload.scope
                : "read") as "read" | "write" | "admin",
              status: "active" as const,
              keyPrefix: `sk_live_${apiKeys.length + 1}`,
              createdAt: "2026-03-03T11:00:00.000Z",
              updatedAt: "2026-03-03T11:00:00.000Z",
            };
            apiKeys.unshift(created);
            return mockJsonResponse(
              {
                id: created.id,
                tenantId: created.tenantId,
                keyId: created.id,
                keyPrefix: created.keyPrefix,
                secret: `${created.keyPrefix}_secret`,
                createdAt: created.createdAt,
              },
              201,
            );
          }

          if (
            pathname.match(/^\/api\/v1\/api-keys\/[^/]+\/revoke$/) &&
            method === "POST"
          ) {
            const id = decodeURIComponent(
              pathname.split("/").slice(-2)[0] ?? "",
            );
            const found = apiKeys.find((item) => item.id === id);
            if (!found) {
              return mockJsonResponse(
                { message: `未找到 API Key：${id}` },
                404,
              );
            }
            found.status = "revoked";
            found.updatedAt = "2026-03-03T11:30:00.000Z";
            return mockJsonResponse(found);
          }

          if (pathname === "/api/v1/webhooks" && method === "GET") {
            const parsedUrl = new URL(url, "http://localhost");
            const status = parsedUrl.searchParams.get("status");
            const keyword = parsedUrl.searchParams.get("keyword");
            const filtered = webhooks
              .filter((item) => (status ? item.status === status : true))
              .filter((item) =>
                keyword
                  ? item.id.includes(keyword) || item.name.includes(keyword)
                  : true,
              );
            return mockJsonResponse({
              items: filtered,
              total: filtered.length,
            });
          }

          if (
            pathname.match(/^\/api\/v1\/webhooks\/[^/]+$/) &&
            method === "PUT"
          ) {
            const id = decodeURIComponent(pathname.split("/").pop() ?? "");
            const target = webhooks.find((item) => item.id === id);
            if (!target) {
              return mockJsonResponse(
                { message: `未找到 Webhook：${id}` },
                404,
              );
            }
            const payload = JSON.parse(String(init?.body ?? "{}")) as {
              name?: string;
              url?: string;
              events?: string[];
              status?: "active" | "paused";
            };
            target.name = payload.name ?? target.name;
            target.url = payload.url ?? target.url;
            target.events = payload.events ?? target.events;
            target.status = payload.status ?? target.status;
            target.updatedAt = "2026-03-03T11:40:00.000Z";
            return mockJsonResponse(target);
          }

          if (pathname === "/api/v1/webhooks" && method === "POST") {
            const payload = JSON.parse(String(init?.body ?? "{}")) as {
              name?: string;
              url?: string;
              events?: string[];
              status?: "active" | "paused";
              secret?: string;
            };
            const created = {
              id: `wh-ui-${webhooks.length + 1}`,
              tenantId: "default",
              name: payload.name ?? "unnamed-webhook",
              url: payload.url ?? "https://hooks.example.com/default",
              events: payload.events ?? ["api_key.created"],
              status: payload.status ?? "active",
              createdAt: "2026-03-03T11:20:00.000Z",
              updatedAt: "2026-03-03T11:20:00.000Z",
            };
            webhooks.unshift(created);
            return mockJsonResponse(
              {
                ...created,
                secret: payload.secret ?? "whsec_xxx",
              },
              201,
            );
          }

          if (
            pathname.match(/^\/api\/v1\/webhooks\/[^/]+$/) &&
            method === "DELETE"
          ) {
            const id = decodeURIComponent(pathname.split("/").pop() ?? "");
            const existingIndex = webhooks.findIndex((item) => item.id === id);
            if (existingIndex < 0) {
              return mockJsonResponse(
                { message: `未找到 Webhook：${id}` },
                404,
              );
            }
            webhooks.splice(existingIndex, 1);
            return mockJsonResponse({ success: true });
          }

          if (
            pathname.match(/^\/api\/v1\/webhooks\/[^/]+\/replay$/) &&
            method === "POST"
          ) {
            const id = decodeURIComponent(
              pathname.split("/").slice(-2)[0] ?? "",
            );
            const payload = JSON.parse(String(init?.body ?? "{}")) as {
              eventType?: string;
              from?: string;
              to?: string;
              limit?: number;
              dryRun?: boolean;
            };
            return mockJsonResponse({
              id: `replay-${id}`,
              webhookId: id,
              status: "queued",
              dryRun: payload.dryRun ?? false,
              filters: {
                eventType: payload.eventType,
                from: payload.from,
                to: payload.to,
                limit: payload.limit ?? 100,
              },
              requestedAt: "2026-03-03T12:30:00.000Z",
            });
          }

          if (pathname === "/api/v2/quality/metrics" && method === "GET") {
            return mockJsonResponse({
              items: [
                {
                  date: "2026-03-03",
                  metric: "accuracy",
                  avgScore: 92,
                  totalEvents: 14,
                  passedEvents: 13,
                  failedEvents: 1,
                  passRate: 0.9286,
                },
              ],
              total: 1,
            });
          }

          if (pathname === "/api/v2/quality/scorecards" && method === "GET") {
            return mockJsonResponse({
              items: [
                {
                  id: "accuracy",
                  tenantId: "default",
                  metric: "accuracy",
                  targetScore: 92,
                  warningScore: 85,
                  criticalScore: 75,
                  weight: 1,
                  enabled: true,
                  updatedByUserId: "user-1",
                  updatedAt: "2026-03-03T12:00:00.000Z",
                },
              ],
              total: 1,
            });
          }

          if (
            pathname === "/api/v2/quality/reports/project-trends" &&
            method === "GET"
          ) {
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
                from: "2026-03-03T00:00:00.000Z",
                to: "2026-03-03T23:59:59.999Z",
              },
              filters: {
                from: "2026-03-03",
                to: "2026-03-03",
                metric: "accuracy",
                provider: null,
                workflow: null,
                includeUnknown: false,
                limit: 20,
              },
            });
          }

          if (pathname === "/api/v2/replay/datasets" && method === "POST") {
            const payload = JSON.parse(String(init?.body ?? "{}")) as {
              name?: string;
              datasetId?: string;
              datasetRef?: string;
              model?: string;
              promptVersion?: string;
              sampleCount?: number;
            };
            const created = {
              id: `baseline-ui-${replayDatasets.length + 1}`,
              tenantId: "default",
              name: payload.name ?? "baseline-created",
              datasetId:
                payload.datasetRef ??
                payload.datasetId ??
                `dataset-${replayDatasets.length + 1}`,
              model: payload.model ?? "gpt-5-codex",
              promptVersion: payload.promptVersion,
              caseCount: payload.sampleCount ?? 0,
              sampleCount: payload.sampleCount ?? 0,
              metadata: {},
              createdAt: "2026-03-03T12:12:00.000Z",
              updatedAt: "2026-03-03T12:12:00.000Z",
            };
            replayDatasets.unshift(created);
            return mockJsonResponse(created, 201);
          }

          if (pathname === "/api/v2/replay/datasets" && method === "GET") {
            return mockJsonResponse({
              items: replayDatasets,
              total: replayDatasets.length,
              filters: {},
            });
          }

          if (
            pathname === "/api/v2/replay/datasets/baseline-ui-1/cases" &&
            method === "GET"
          ) {
            return mockJsonResponse({
              datasetId: "baseline-ui-1",
              items: replayDatasetCases,
              total: replayDatasetCases.length,
            });
          }

          if (
            pathname === "/api/v2/replay/datasets/baseline-ui-1/cases" &&
            method === "POST"
          ) {
            const payload = JSON.parse(String(init?.body ?? "{}")) as {
              items?: Array<Record<string, unknown>>;
            };
            const nextCases = (payload.items ?? []).map((item, index) => ({
              datasetId: "baseline-ui-1",
              caseId:
                typeof item.caseId === "string" && item.caseId.trim().length > 0
                  ? item.caseId.trim()
                  : `case-${index + 1}`,
              sortOrder:
                typeof item.sortOrder === "number" &&
                Number.isInteger(item.sortOrder)
                  ? item.sortOrder
                  : index,
              input: typeof item.input === "string" ? item.input : "",
              expectedOutput:
                typeof item.expectedOutput === "string"
                  ? item.expectedOutput
                  : undefined,
              baselineOutput:
                typeof item.baselineOutput === "string"
                  ? item.baselineOutput
                  : undefined,
              candidateInput:
                typeof item.candidateInput === "string"
                  ? item.candidateInput
                  : undefined,
              metadata:
                item.metadata &&
                typeof item.metadata === "object" &&
                !Array.isArray(item.metadata)
                  ? (item.metadata as Record<string, unknown>)
                  : {},
              createdAt: "2026-03-03T12:12:00.000Z",
              updatedAt: "2026-03-03T12:13:00.000Z",
            }));
            replayDatasetCases.splice(
              0,
              replayDatasetCases.length,
              ...nextCases,
            );
            return mockJsonResponse({
              datasetId: "baseline-ui-1",
              items: replayDatasetCases,
              total: replayDatasetCases.length,
            });
          }

          if (pathname === "/api/v2/replay/runs" && method === "POST") {
            const payload = JSON.parse(String(init?.body ?? "{}")) as {
              datasetId?: string;
              baselineId?: string;
              candidateLabel?: string;
              sampleLimit?: number;
            };
            const created = {
              id: `job-ui-${replayRuns.length + 1}`,
              runId: `job-ui-${replayRuns.length + 1}`,
              jobId: `job-ui-${replayRuns.length + 1}`,
              tenantId: "default",
              datasetId:
                payload.datasetId ?? payload.baselineId ?? "baseline-ui-1",
              baselineId:
                payload.datasetId ?? payload.baselineId ?? "baseline-ui-1",
              candidateLabel: payload.candidateLabel ?? "candidate-created",
              status: "pending" as const,
              totalCases: payload.sampleLimit ?? 20,
              processedCases: 0,
              improvedCases: 0,
              regressedCases: 0,
              unchangedCases: 0,
              diffs: [],
              summary: {},
              createdAt: "2026-03-03T12:21:00.000Z",
              updatedAt: "2026-03-03T12:21:00.000Z",
            };
            replayRuns.unshift(created);
            return mockJsonResponse(created, 201);
          }

          if (pathname === "/api/v2/replay/runs" && method === "GET") {
            const parsedUrl = new URL(url, "http://localhost");
            const datasetId = parsedUrl.searchParams.get("datasetId");
            const status = parsedUrl.searchParams.get("status");
            const filteredRuns = replayRuns.filter(
              (item) =>
                (datasetId
                  ? (item.datasetId ?? item.baselineId) === datasetId
                  : true) && (status ? item.status === status : true),
            );
            return mockJsonResponse({
              items: filteredRuns,
              total: filteredRuns.length,
              filters: {},
            });
          }

          if (
            pathname === "/api/v2/replay/runs/job-ui-1/diffs" &&
            method === "GET"
          ) {
            return mockJsonResponse({
              runId: "job-ui-1",
              jobId: "job-ui-1",
              datasetId: "baseline-ui-1",
              diffs: [
                {
                  caseId: "case-1",
                  metric: "accuracy",
                  baselineScore: 0.7,
                  candidateScore: 0.9,
                  delta: 0.2,
                  verdict: "improved",
                  detail: "answer quality improved",
                },
              ],
              total: 1,
              summary: {},
              filters: {
                datasetId: "baseline-ui-1",
                baselineId: "baseline-ui-1",
                runId: "job-ui-1",
                jobId: "job-ui-1",
                keyword: null,
                limit: 50,
              },
            });
          }

          if (
            pathname === "/api/v2/replay/runs/job-ui-1/artifacts" &&
            method === "GET"
          ) {
            return mockJsonResponse({
              runId: "job-ui-1",
              items: [
                {
                  type: "summary",
                  name: "summary.json",
                  contentType: "application/json",
                  downloadName: "summary.json",
                  byteSize: 128,
                  createdAt: "2026-03-03T12:22:00.000Z",
                  inline: {
                    totalCases: 10,
                  },
                },
                {
                  type: "diff",
                  name: "diff.json",
                  contentType: "application/json",
                  downloadName: "diff.json",
                  byteSize: 256,
                  createdAt: "2026-03-03T12:22:00.000Z",
                  inline: {
                    items: [],
                  },
                },
                {
                  type: "cases",
                  name: "cases.json",
                  contentType: "application/json",
                  downloadName: "cases.json",
                  byteSize: 196,
                  createdAt: "2026-03-03T12:22:00.000Z",
                  inline: {
                    items: replayDatasetCases,
                  },
                },
              ],
              total: 3,
            });
          }

          return undefined;
        },
      });

      render(<App />);

      const section = (
        await screen.findByRole("heading", { name: "开放平台工作台", level: 2 })
      ).closest("section");
      expect(section).not.toBeNull();
      const sectionScreen = within(section as HTMLElement);
      const byId = <T extends HTMLElement>(id: string) => {
        const element = (section as HTMLElement).querySelector(`#${id}`);
        expect(element).not.toBeNull();
        return element as T;
      };

      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载 OpenAPI 摘要" }),
      );
      expect(await sectionScreen.findByText("version:")).toBeInTheDocument();
      expect(sectionScreen.getByText("3.0.3")).toBeInTheDocument();

      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载 API Key 列表" }),
      );
      expect(await sectionScreen.findByText("ak-ui-1")).toBeInTheDocument();

      fireEvent.change(byId<HTMLInputElement>("open-platform-api-key-id"), {
        target: { value: "ak-ui-custom-1" },
      });
      fireEvent.change(byId<HTMLInputElement>("open-platform-api-key-name"), {
        target: { value: "release-bot" },
      });
      fireEvent.change(byId<HTMLInputElement>("open-platform-api-key-scopes"), {
        target: { value: "read" },
      });
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "保存 API Key" }),
      );
      expect(
        await sectionScreen.findByText(/API Key .* 已保存。/),
      ).toBeInTheDocument();
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-api-key-revoke-reason"),
        {
          target: { value: "rotation-2026" },
        },
      );
      const apiKeyRow = sectionScreen.getByText("ak-ui-1").closest("tr");
      expect(apiKeyRow).not.toBeNull();
      fireEvent.click(
        within(apiKeyRow as HTMLElement).getByRole("button", { name: "吊销" }),
      );
      expect(
        await sectionScreen.findByText("API Key ak-ui-1 已吊销。"),
      ).toBeInTheDocument();
      expect(await sectionScreen.findByText("已吊销")).toBeInTheDocument();

      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载 Webhook 列表" }),
      );
      expect(await sectionScreen.findByText("wh-ui-1")).toBeInTheDocument();
      expect(
        byId<HTMLInputElement>("open-platform-webhook-events"),
      ).toHaveAttribute(
        "placeholder",
        "replay.run.started,replay.run.completed",
      );
      expect(
        byId<HTMLInputElement>("open-platform-webhook-replay-event-type"),
      ).toHaveAttribute("placeholder", "例如：replay.run.completed");

      fireEvent.change(byId<HTMLInputElement>("open-platform-webhook-id"), {
        target: { value: "wh-ui-custom-2" },
      });
      fireEvent.change(byId<HTMLInputElement>("open-platform-webhook-name"), {
        target: { value: "pipeline-alert" },
      });
      fireEvent.change(byId<HTMLInputElement>("open-platform-webhook-url"), {
        target: { value: "https://hooks.example.com/pipeline-alert" },
      });
      fireEvent.change(byId<HTMLInputElement>("open-platform-webhook-events"), {
        target: { value: "alert.open" },
      });
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "保存 Webhook" }),
      );
      expect(
        await sectionScreen.findByText(/事件名不合法：alert\.open/),
      ).toBeInTheDocument();
      fireEvent.change(byId<HTMLInputElement>("open-platform-webhook-events"), {
        target: { value: "replay.run.started,replay.run.completed" },
      });
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "保存 Webhook" }),
      );
      expect(
        await sectionScreen.findByText(
          (content) =>
            content.includes("Webhook") && content.includes("已保存"),
        ),
      ).toBeInTheDocument();
      expect(
        (section as HTMLElement).querySelector(
          "#open-platform-webhook-event-options option[value='replay.run.started']",
        ),
      ).not.toBeNull();
      expect(
        (section as HTMLElement).querySelector(
          "#open-platform-webhook-event-options option[value='replay.run.cancelled']",
        ),
      ).not.toBeNull();
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-webhook-replay-event-type"),
        {
          target: { value: "replay.run.cancelled" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-webhook-replay-limit"),
        {
          target: { value: "20" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "回放 Webhook" }),
      );
      expect(
        await sectionScreen.findByText(
          (content) =>
            content.includes("回放任务") && content.includes("已排队"),
        ),
      ).toBeInTheDocument();
      const webhookRow = sectionScreen.getByText("wh-ui-1").closest("tr");
      expect(webhookRow).not.toBeNull();
      fireEvent.click(
        within(webhookRow as HTMLElement).getByRole("button", { name: "删除" }),
      );
      expect(
        await sectionScreen.findByText("Webhook wh-ui-1 已删除。"),
      ).toBeInTheDocument();
      await waitFor(() => {
        expect(sectionScreen.queryByText("wh-ui-1")).not.toBeInTheDocument();
      });

      fireEvent.change(
        byId<HTMLSelectElement>("open-platform-quality-daily-metric"),
        {
          target: { value: "accuracy" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载 Quality daily" }),
      );
      expect(
        await sectionScreen.findByRole("cell", { name: "accuracy" }),
      ).toBeInTheDocument();
      expect(
        await screen.findByText("暂无 externalSource 分组数据。"),
      ).toBeInTheDocument();
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-quality-project-trends-from"),
        {
          target: { value: "2026-03-03" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-quality-project-trends-to"),
        {
          target: { value: "2026-03-03" },
        },
      );
      fireEvent.change(
        byId<HTMLSelectElement>("open-platform-quality-project-trends-metric"),
        {
          target: { value: "accuracy" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", {
          name: "加载 Quality project-trends",
        }),
      );
      expect(
        await sectionScreen.findByText("agentledger/main"),
      ).toBeInTheDocument();
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-quality-scorecard-team"),
        {
          target: { value: "accuracy" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载 Quality scorecards" }),
      );
      expect(await sectionScreen.findByText("user-1")).toBeInTheDocument();

      fireEvent.change(
        byId<HTMLInputElement>("open-platform-replay-create-dataset-name"),
        {
          target: { value: "baseline created" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-replay-create-dataset-id"),
        {
          target: { value: "dataset-created-1" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-replay-create-dataset-model"),
        {
          target: { value: "gpt-5-codex" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>(
          "open-platform-replay-create-dataset-prompt-version",
        ),
        {
          target: { value: "v2" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>(
          "open-platform-replay-create-dataset-sample-count",
        ),
        {
          target: { value: "20" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "创建回放数据集" }),
      );
      expect(
        await sectionScreen.findByText(/回放数据集 .* 已创建。/),
      ).toBeInTheDocument();

      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载回放数据集" }),
      );
      expect(
        await sectionScreen.findByText("baseline-ui-1"),
      ).toBeInTheDocument();
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-replay-dataset-cases-dataset-id"),
        {
          target: { value: "baseline-ui-1" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载回放样本" }),
      );
      expect(
        await sectionScreen.findByText("Summarize the change"),
      ).toBeInTheDocument();
      fireEvent.change(
        byId<HTMLTextAreaElement>("open-platform-replay-dataset-cases-editor"),
        {
          target: {
            value:
              '[{"caseId":"case-1","sortOrder":0,"input":"Summarize the change","expectedOutput":"A concise summary","metadata":{"priority":"p0"}}]',
          },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "保存回放样本" }),
      );
      expect(
        await sectionScreen.findByText("回放样本已保存，共 1 条。"),
      ).toBeInTheDocument();
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-replay-create-run-baseline-id"),
        {
          target: { value: "baseline-ui-1" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>(
          "open-platform-replay-create-run-candidate-label",
        ),
        {
          target: { value: "candidate-created" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-replay-create-run-sample-limit"),
        {
          target: { value: "12" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "创建回放运行" }),
      );
      expect(
        await sectionScreen.findByText(/回放运行 .* 已创建/),
      ).toBeInTheDocument();
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-replay-jobs-baseline-id"),
        {
          target: { value: "baseline-ui-1" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载回放运行" }),
      );
      expect(await sectionScreen.findByText("job-ui-1")).toBeInTheDocument();
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-replay-diff-baseline-id"),
        {
          target: { value: "baseline-ui-1" },
        },
      );
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-replay-diff-job-id"),
        {
          target: { value: "job-ui-1" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载回放差异" }),
      );
      expect(await sectionScreen.findByText("case-1")).toBeInTheDocument();
      fireEvent.change(
        byId<HTMLInputElement>("open-platform-replay-artifact-job-id"),
        {
          target: { value: "job-ui-1" },
        },
      );
      fireEvent.click(
        sectionScreen.getByRole("button", { name: "加载回放工件" }),
      );
      expect(
        await sectionScreen.findByText("summary.json"),
      ).toBeInTheDocument();

      expect(
        fetchSpy.mock.calls.some(
          ([url]) =>
            new URL(toUrl(url), "http://localhost").pathname ===
            "/api/v1/openapi.json",
        ),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(
          ([url]) =>
            new URL(toUrl(url), "http://localhost").pathname ===
            "/api/v1/api-keys",
        ),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(([url, init]) => {
          const requestInit = init as RequestInit | undefined;
          return (
            new URL(toUrl(url), "http://localhost").pathname ===
              "/api/v1/api-keys/ak-ui-1/revoke" &&
            (requestInit?.method ?? "GET").toUpperCase() === "POST"
          );
        }),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(
          ([url]) =>
            new URL(toUrl(url), "http://localhost").pathname ===
            "/api/v1/webhooks",
        ),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(([url, init]) => {
          const requestInit = init as RequestInit | undefined;
          return (
            new URL(toUrl(url), "http://localhost").pathname ===
              "/api/v1/webhooks" &&
            (requestInit?.method ?? "GET").toUpperCase() === "POST" &&
            JSON.parse(String(requestInit?.body ?? "{}")).events?.includes(
              "replay.run.started",
            )
          );
        }),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(([url, init]) => {
          const requestInit = init as RequestInit | undefined;
          return (
            new URL(toUrl(url), "http://localhost").pathname ===
              "/api/v1/webhooks/wh-ui-2/replay" &&
            (requestInit?.method ?? "GET").toUpperCase() === "POST" &&
            JSON.parse(String(requestInit?.body ?? "{}")).eventType ===
              "replay.run.cancelled"
          );
        }),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(([url, init]) => {
          const requestInit = init as RequestInit | undefined;
          return (
            new URL(toUrl(url), "http://localhost").pathname ===
              "/api/v1/webhooks/wh-ui-1" &&
            (requestInit?.method ?? "GET").toUpperCase() === "DELETE"
          );
        }),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(
          ([url]) =>
            new URL(toUrl(url), "http://localhost").pathname ===
            "/api/v2/quality/reports/project-trends",
        ),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(([url, init]) => {
          const requestInit = init as RequestInit | undefined;
          return (
            new URL(toUrl(url), "http://localhost").pathname ===
              "/api/v2/replay/datasets" &&
            (requestInit?.method ?? "GET").toUpperCase() === "POST"
          );
        }),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(([url, init]) => {
          const requestInit = init as RequestInit | undefined;
          return (
            new URL(toUrl(url), "http://localhost").pathname ===
              "/api/v2/replay/runs" &&
            (requestInit?.method ?? "GET").toUpperCase() === "POST"
          );
        }),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(
          ([url]) =>
            new URL(toUrl(url), "http://localhost").pathname ===
            "/api/v2/replay/runs/job-ui-1/artifacts",
        ),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(([url]) =>
          new URL(toUrl(url), "http://localhost").pathname.startsWith(
            "/api/v2/replay/runs",
          ),
        ),
      ).toBe(true);
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test("治理页在 single_region 模式配置副本地域时会阻止保存且不发起 PUT", async () => {
    window.location.hash = "#/governance";
    setAuthTokens({
      accessToken: "access-token-governance-residency-single-region",
      refreshToken: "refresh-token-governance-residency-single-region",
      expiresIn: 1800,
      tokenType: "Bearer",
    });

    const fetchSpy = mockGovernancePageFetch({
      residencyPolicy: {
        tenantId: "default",
        mode: "single_region",
        primaryRegion: "cn-shanghai",
        replicaRegions: [],
        allowCrossRegionTransfer: false,
        requireTransferApproval: false,
        updatedAt: "2026-03-01T00:00:00.000Z",
      },
    });

    render(<App />);

    expect(
      await screen.findByRole("heading", { name: "治理中心" }),
    ).toBeInTheDocument();

    const replicaRegionsInput =
      await screen.findByLabelText("副本地域（逗号分隔）");
    const residencySection = replicaRegionsInput.closest("section");
    expect(residencySection).not.toBeNull();

    fireEvent.change(replicaRegionsInput, {
      target: { value: "ap-southeast-1" },
    });
    fireEvent.click(
      within(residencySection as HTMLElement).getByRole("button", {
        name: "保存策略",
      }),
    );

    expect(
      await screen.findByText("single_region 模式不允许配置副本地域。"),
    ).toBeInTheDocument();
    expect(
      fetchSpy.mock.calls.some(([url, init]) => {
        const requestInit = init as RequestInit | undefined;
        return (
          new URL(toUrl(url), "http://localhost").pathname ===
            "/api/v2/residency/policies/current" &&
          (requestInit?.method ?? "GET").toUpperCase() === "PUT"
        );
      }),
    ).toBe(false);
  });

  test(
    "治理页主权策略首次失败后恢复成功时会回填表单",
    async () => {
      window.location.hash = "#/governance";
      setAuthTokens({
        accessToken: "access-token-governance-residency-retry",
        refreshToken: "refresh-token-governance-residency-retry",
        expiresIn: 1800,
        tokenType: "Bearer",
      });

      const fetchSpy = mockGovernancePageFetch({
        residencyPolicyResponses: [
          {
            status: 503,
            body: {
              message: "temporary outage",
            },
          },
          {
            status: 200,
            body: {
              tenantId: "default",
              mode: "active_active",
              primaryRegion: "cn-shanghai",
              replicaRegions: ["ap-southeast-1"],
              allowCrossRegionTransfer: true,
              requireTransferApproval: true,
              updatedAt: "2026-03-02T00:00:00.000Z",
            },
          },
        ],
      });

      render(<App />);

      expect(
        await screen.findByRole("heading", { name: "治理中心" }),
      ).toBeInTheDocument();
      await waitFor(
        () => {
          expect(
            (screen.getByLabelText("模式") as HTMLSelectElement).value,
          ).toBe("active_active");
          expect(
            (screen.getByLabelText("主地域") as HTMLSelectElement).value,
          ).toBe("cn-shanghai");
          expect(
            (screen.getByLabelText("副本地域（逗号分隔）") as HTMLInputElement)
              .value,
          ).toBe("ap-southeast-1");
        },
        { timeout: 8_000 },
      );

      expect(
        fetchSpy.mock.calls.filter(
          ([url, init]) =>
            new URL(toUrl(url), "http://localhost").pathname ===
              "/api/v2/residency/policies/current" &&
            (
              (init as RequestInit | undefined)?.method ?? "GET"
            ).toUpperCase() === "GET",
        ).length,
      ).toBeGreaterThanOrEqual(2);
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test(
    "治理页支持审批复制任务并刷新状态",
    async () => {
      window.location.hash = "#/governance";
      setAuthTokens({
        accessToken: "access-token-governance-residency-approve",
        refreshToken: "refresh-token-governance-residency-approve",
        expiresIn: 1800,
        tokenType: "Bearer",
      });

      let approved = false;
      const promptSpy = vi.spyOn(window, "prompt").mockReturnValue("审批通过");
      const fetchSpy = mockGovernancePageFetch({
        extraHandler: (_input, init, context) => {
          if (
            context.pathname === "/api/v2/residency/replications" &&
            context.method === "GET"
          ) {
            return mockJsonResponse({
              items: [
                {
                  id: "rep-job-approve-1",
                  tenantId: "default",
                  sourceRegion: "cn-shanghai",
                  targetRegion: "ap-southeast-1",
                  status: approved ? "running" : "pending",
                  reason: approved ? "审批通过" : "待审批跨区复制",
                  metadata: {},
                  createdAt: "2026-03-03T10:00:00.000Z",
                  updatedAt: "2026-03-03T10:05:00.000Z",
                  startedAt: approved ? "2026-03-03T10:06:00.000Z" : null,
                  finishedAt: null,
                },
              ],
              total: 1,
              filters: {
                limit: 50,
              },
            });
          }

          if (
            context.pathname ===
              "/api/v2/residency/replications/rep-job-approve-1/approvals" &&
            context.method === "POST"
          ) {
            expect(JSON.parse(String(init?.body ?? "{}"))).toEqual({
              reason: "审批通过",
            });
            approved = true;
            return mockJsonResponse({
              id: "rep-job-approve-1",
              tenantId: "default",
              sourceRegion: "cn-shanghai",
              targetRegion: "ap-southeast-1",
              status: "running",
              reason: "审批通过",
              metadata: {},
              createdAt: "2026-03-03T10:00:00.000Z",
              updatedAt: "2026-03-03T10:06:00.000Z",
              startedAt: "2026-03-03T10:06:00.000Z",
              finishedAt: null,
            });
          }

          return undefined;
        },
      });

      try {
        render(<App />);

        const section = (
          await screen.findByRole("heading", {
            name: "数据主权与复制",
            level: 2,
          })
        ).closest("section");
        expect(section).not.toBeNull();
        const sectionScreen = within(section as HTMLElement);

        const replicationRow = (
          await sectionScreen.findByText("rep-job-approve-1")
        ).closest("tr");
        expect(replicationRow).not.toBeNull();
        fireEvent.click(
          within(replicationRow as HTMLElement).getByRole("button", {
            name: "审批",
          }),
        );

        expect(
          await sectionScreen.findByText(
            "复制任务 rep-job-approve-1 已审批，当前状态 running。",
          ),
        ).toBeInTheDocument();
        await waitFor(() => {
          const refreshedRow = sectionScreen
            .getByText("rep-job-approve-1")
            .closest("tr");
          expect(refreshedRow).not.toBeNull();
          expect(
            within(refreshedRow as HTMLElement).getByText("running"),
          ).toBeInTheDocument();
        });
        expect(promptSpy).toHaveBeenCalledWith("审批原因（可选）", "");
        expect(
          fetchSpy.mock.calls.some(([url, init]) => {
            const requestInit = init as RequestInit | undefined;
            return (
              new URL(toUrl(url), "http://localhost").pathname ===
                "/api/v2/residency/replications/rep-job-approve-1/approvals" &&
              (requestInit?.method ?? "GET").toUpperCase() === "POST"
            );
          }),
        ).toBe(true);
      } finally {
        promptSpy.mockRestore();
      }
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test(
    "治理页开放平台支持下载 Replay artifact",
    async () => {
      window.location.hash = "#/governance";
      setAuthTokens({
        accessToken: "access-token-governance-replay-artifact-download",
        refreshToken: "refresh-token-governance-replay-artifact-download",
        expiresIn: 1800,
        tokenType: "Bearer",
      });

      const originalCreateObjectURL = URL.createObjectURL;
      const originalRevokeObjectURL = URL.revokeObjectURL;
      const createObjectURLSpy = vi.fn(
        () => "blob:mock-replay-artifact-download",
      );
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

      const fetchSpy = mockGovernancePageFetch({
        extraHandler: (_input, _init, context) => {
          if (
            context.pathname ===
              "/api/v2/replay/runs/job-ui-download/artifacts" &&
            context.method === "GET"
          ) {
            return mockJsonResponse({
              runId: "job-ui-download",
              items: [
                {
                  type: "summary",
                  name: "summary.json",
                  contentType: "application/json",
                  downloadName: "summary.json",
                  downloadUrl:
                    "/api/v2/replay/runs/job-ui-download/artifacts/summary/download",
                  byteSize: 18,
                  createdAt: "2026-03-03T12:22:00.000Z",
                  inline: {
                    totalCases: 10,
                  },
                },
              ],
              total: 1,
            });
          }

          if (
            context.pathname ===
              "/api/v2/replay/runs/job-ui-download/artifacts/summary/download" &&
            context.method === "GET"
          ) {
            return {
              ok: true,
              status: 200,
              headers: {
                get: (name: string) => {
                  const normalized = name.toLowerCase();
                  if (normalized === "content-type") {
                    return "application/json";
                  }
                  if (normalized === "content-disposition") {
                    return 'attachment; filename="summary.json"';
                  }
                  return null;
                },
              },
              blob: async () =>
                new Blob(['{"totalCases":10}'], { type: "application/json" }),
              json: async () => ({ totalCases: 10 }),
              text: async () => '{"totalCases":10}',
            } as Response;
          }

          return undefined;
        },
      });

      try {
        render(<App />);

        const section = (
          await screen.findByRole("heading", {
            name: "开放平台工作台",
            level: 2,
          })
        ).closest("section");
        expect(section).not.toBeNull();
        const sectionScreen = within(section as HTMLElement);
        const byId = <T extends HTMLElement>(id: string) => {
          const element = (section as HTMLElement).querySelector(`#${id}`);
          expect(element).not.toBeNull();
          return element as T;
        };

        fireEvent.change(
          byId<HTMLInputElement>("open-platform-replay-artifact-job-id"),
          {
            target: { value: "job-ui-download" },
          },
        );
        fireEvent.click(
          sectionScreen.getByRole("button", { name: "加载回放工件" }),
        );

        const artifactRow = (
          await sectionScreen.findByText("summary.json")
        ).closest("tr");
        expect(artifactRow).not.toBeNull();
        fireEvent.click(
          within(artifactRow as HTMLElement).getByRole("button", {
            name: "下载",
          }),
        );

        expect(
          await sectionScreen.findByText("回放工件下载成功：summary.json"),
        ).toBeInTheDocument();
        expect(
          fetchSpy.mock.calls.some(
            ([url]) =>
              new URL(toUrl(url), "http://localhost").pathname ===
              "/api/v2/replay/runs/job-ui-download/artifacts/summary/download",
          ),
        ).toBe(true);
        expect(createObjectURLSpy).toHaveBeenCalledTimes(1);
        expect(revokeObjectURLSpy).toHaveBeenCalledTimes(1);
        expect(anchorClickSpy).toHaveBeenCalledTimes(1);
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
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test(
    "治理页 Rule Hub 支持创建并展示 scopeBinding",
    async () => {
      window.location.hash = "#/governance";
      setAuthTokens({
        accessToken: "access-token-governance-rule-hub-scope-binding",
        refreshToken: "refresh-token-governance-rule-hub-scope-binding",
        expiresIn: 1800,
        tokenType: "Bearer",
      });

      const ruleAssets: GovernanceRuleAssetFixture[] = [
        {
          id: "asset-scoped-existing",
          name: "asset-scoped-existing",
          status: "draft",
          latestVersion: 1,
          publishedVersion: null,
          scopeBinding: {
            organizations: ["org-existing"],
            projects: ["project-existing"],
          },
        },
      ];

      const fetchSpy = mockGovernancePageFetch({
        ruleAssets,
        extraHandler: (_input, init, context) => {
          if (
            context.pathname === "/api/v1/rules/assets" &&
            context.method === "POST"
          ) {
            const payload = JSON.parse(String(init?.body ?? "{}")) as {
              name?: string;
              description?: string;
              requiredApprovals?: number;
              scopeBinding?: {
                organizations?: string[];
                projects?: string[];
                clients?: string[];
              };
            };

            const created = {
              id: "asset-scoped-created",
              tenantId: "default",
              name: payload.name ?? "asset-scoped-created",
              description: payload.description ?? "",
              status: "draft" as const,
              latestVersion: 0,
              publishedVersion: null,
              requiredApprovals:
                payload.requiredApprovals === 2 ? 2 : 1,
              scopeBinding: payload.scopeBinding ?? {},
              createdAt: "2026-03-05T08:00:00.000Z",
              updatedAt: "2026-03-05T09:00:00.000Z",
            };

            ruleAssets.push({
              id: created.id,
              name: created.name,
              status: created.status,
              latestVersion: created.latestVersion,
              publishedVersion: created.publishedVersion,
              requiredApprovals: created.requiredApprovals,
              scopeBinding: created.scopeBinding,
            });

            return mockJsonResponse(created, 201);
          }

          return undefined;
        },
      });

      render(<App />);

      const ruleHubHeading = await screen.findByRole("heading", {
        name: "Rule Hub 规则资产",
        level: 2,
      });
      expect(ruleHubHeading).toBeInTheDocument();
      const ruleHubSection = ruleHubHeading.closest("section");
      if (!(ruleHubSection instanceof HTMLElement)) {
        throw new Error("未找到 Rule Hub 所在 section。");
      }
      const ruleHubScreen = within(ruleHubSection);

      expect(
        await ruleHubScreen.findByText(
          "organizations: org-existing | projects: project-existing",
        ),
      ).toBeInTheDocument();

      fireEvent.change(ruleHubScreen.getByLabelText("资产名称"), {
        target: { value: "scope-binding-rule" },
      });
      fireEvent.change(ruleHubScreen.getByLabelText("说明"), {
        target: { value: "验证 Rule Hub scopeBinding" },
      });
      fireEvent.change(ruleHubScreen.getByLabelText("审批要求"), {
        target: { value: "2" },
      });
      fireEvent.change(
        ruleHubScreen.getByLabelText("Organizations（逗号分隔）"),
        {
          target: { value: "org-alpha, org-beta, org-alpha" },
        },
      );
      fireEvent.change(ruleHubScreen.getByLabelText("Projects（逗号分隔）"), {
        target: { value: "project-1" },
      });
      fireEvent.change(ruleHubScreen.getByLabelText("Clients（逗号分隔）"), {
        target: { value: "client-web, client-admin" },
      });
      fireEvent.click(
        ruleHubScreen.getByRole("button", { name: "创建规则资产" }),
      );

      expect(
        await ruleHubScreen.findByText("规则资产 scope-binding-rule 已创建。"),
      ).toBeInTheDocument();

      let createdRow: HTMLElement | undefined;
      await waitFor(() => {
        createdRow = ruleHubScreen
          .getAllByRole("row")
          .find((candidate) =>
            candidate.textContent?.includes("scope-binding-rule"),
          ) as HTMLElement | undefined;
        expect(createdRow).toBeDefined();
      });
      expect(createdRow).toHaveTextContent(
        "2 人",
      );
      expect(createdRow).toHaveTextContent(
        "organizations: org-alpha, org-beta | projects: project-1 | clients: client-web, client-admin",
      );
      expect(
        await ruleHubScreen.findByText("当前审批要求：2 人"),
      ).toBeInTheDocument();
      expect(
        await ruleHubScreen.findByText(
          "当前 Scope Binding：organizations: org-alpha, org-beta | projects: project-1 | clients: client-web, client-admin",
        ),
      ).toBeInTheDocument();

      const postCall = fetchSpy.mock.calls.find(
        ([url, init]) =>
          new URL(toUrl(url), "http://localhost").pathname ===
            "/api/v1/rules/assets" &&
          (init as RequestInit | undefined)?.method === "POST",
      );
      expect(postCall).toBeTruthy();
      expect(
        JSON.parse(
          String((postCall?.[1] as RequestInit | undefined)?.body ?? "{}"),
        ),
      ).toMatchObject({
        requiredApprovals: 2,
        scopeBinding: {
          organizations: ["org-alpha", "org-beta"],
          projects: ["project-1"],
          clients: ["client-web", "client-admin"],
        },
      });
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test(
    "治理页 Rule Hub 支持创建双人审批资产并展示审批要求",
    async () => {
      window.location.hash = "#/governance";
      setAuthTokens({
        accessToken: "access-token-governance-rule-hub-dual-approval",
        refreshToken: "refresh-token-governance-rule-hub-dual-approval",
        expiresIn: 1800,
        tokenType: "Bearer",
      });

      const ruleAssets: GovernanceRuleAssetFixture[] = [];
      const fetchSpy = mockGovernancePageFetch({
        ruleAssets,
        extraHandler: (_input, init, context) => {
          if (
            context.pathname === "/api/v1/rules/assets" &&
            context.method === "POST"
          ) {
            const payload = JSON.parse(String(init?.body ?? "{}")) as {
              name?: string;
              description?: string;
              requiredApprovals?: number;
            };
            const created = {
              id: "asset-dual-created",
              tenantId: "default",
              name: payload.name ?? "asset-dual-created",
              description: payload.description ?? "",
              status: "draft" as const,
              latestVersion: 0,
              publishedVersion: null,
              requiredApprovals:
                payload.requiredApprovals === 2 ? 2 : 1,
              scopeBinding: {},
              createdAt: "2026-03-05T10:00:00.000Z",
              updatedAt: "2026-03-05T10:30:00.000Z",
            };
            ruleAssets.push({
              id: created.id,
              name: created.name,
              status: created.status,
              latestVersion: created.latestVersion,
              publishedVersion: created.publishedVersion,
              requiredApprovals: created.requiredApprovals,
            });
            return mockJsonResponse(created, 201);
          }
          return undefined;
        },
      });

      render(<App />);

      const ruleHubHeading = await screen.findByRole("heading", {
        name: "Rule Hub 规则资产",
        level: 2,
      });
      const ruleHubSection = ruleHubHeading.closest("section");
      if (!(ruleHubSection instanceof HTMLElement)) {
        throw new Error("未找到 Rule Hub 所在 section。");
      }
      const ruleHubScreen = within(ruleHubSection);

      fireEvent.change(ruleHubScreen.getByLabelText("资产名称"), {
        target: { value: "dual-approval-rule" },
      });
      fireEvent.change(ruleHubScreen.getByLabelText("审批要求"), {
        target: { value: "2" },
      });
      fireEvent.click(
        ruleHubScreen.getByRole("button", { name: "创建规则资产" }),
      );

      expect(
        await ruleHubScreen.findByText("规则资产 dual-approval-rule 已创建。"),
      ).toBeInTheDocument();
      expect(ruleHubScreen.getByText("2 人")).toBeInTheDocument();
      expect(
        await ruleHubScreen.findByText("当前审批要求：2 人"),
      ).toBeInTheDocument();

      const postCall = fetchSpy.mock.calls.find(
        ([url, init]) =>
          new URL(toUrl(url), "http://localhost").pathname ===
            "/api/v1/rules/assets" &&
          (init as RequestInit | undefined)?.method === "POST",
      );
      expect(postCall).toBeTruthy();
      expect(
        JSON.parse(
          String((postCall?.[1] as RequestInit | undefined)?.body ?? "{}"),
        ),
      ).toMatchObject({
        requiredApprovals: 2,
      });
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test(
    "治理页 Rule Hub 默认填充版本 diff 输入并支持加载",
    async () => {
      window.location.hash = "#/governance";
      setAuthTokens({
        accessToken: "access-token-governance-rule-hub-diff",
        refreshToken: "refresh-token-governance-rule-hub-diff",
        expiresIn: 1800,
        tokenType: "Bearer",
      });

      const fetchSpy = mockGovernancePageFetch({
        ruleAssets: [
          {
            id: "asset-diff",
            name: "asset-diff",
            status: "published",
            latestVersion: 2,
            publishedVersion: 2,
            requiredApprovals: 1,
          },
        ],
        extraHandler: (_input, _init, context) => {
          if (
            context.pathname === "/api/v1/rules/assets/asset-diff/versions" &&
            context.method === "GET"
          ) {
            return mockJsonResponse({
              items: [
                {
                  id: "rule-version-2",
                  tenantId: "default",
                  assetId: "asset-diff",
                  version: 2,
                  content: "deny tool=github.delete_repo\nrequire tag=verified",
                  changelog: "v2",
                  createdAt: "2026-03-05T11:00:00.000Z",
                },
                {
                  id: "rule-version-1",
                  tenantId: "default",
                  assetId: "asset-diff",
                  version: 1,
                  content: "allow tool=github.read_repo\nrequire tag=verified",
                  changelog: "v1",
                  createdAt: "2026-03-05T10:00:00.000Z",
                },
              ],
              total: 2,
            });
          }
          if (
            context.pathname ===
              "/api/v1/rules/assets/asset-diff/versions/diff" &&
            context.method === "GET"
          ) {
            return mockJsonResponse({
              assetId: "asset-diff",
              fromVersion: 1,
              toVersion: 2,
              summary: {
                added: 1,
                removed: 1,
                unchanged: 1,
                changed: true,
              },
              lines: [
                {
                  type: "removed",
                  content: "allow tool=github.read_repo",
                  oldLineNumber: 1,
                },
                {
                  type: "added",
                  content: "deny tool=github.delete_repo",
                  newLineNumber: 1,
                },
                {
                  type: "unchanged",
                  content: "require tag=verified",
                  oldLineNumber: 2,
                  newLineNumber: 2,
                },
              ],
            });
          }
          return undefined;
        },
      });

      render(<App />);

      const ruleHubHeading = await screen.findByRole("heading", {
        name: "Rule Hub 规则资产",
        level: 2,
      });
      const ruleHubSection = ruleHubHeading.closest("section");
      if (!(ruleHubSection instanceof HTMLElement)) {
        throw new Error("未找到 Rule Hub 所在 section。");
      }
      const ruleHubScreen = within(ruleHubSection);

      const fromVersionInput = (await ruleHubScreen.findByLabelText(
        "Diff 起始版本",
      )) as HTMLInputElement;
      const toVersionInput = (await ruleHubScreen.findByLabelText(
        "Diff 目标版本",
      )) as HTMLInputElement;
      expect(fromVersionInput.value).toBe("1");
      expect(toVersionInput.value).toBe("2");

      fireEvent.click(
        ruleHubScreen.getByRole("button", { name: "比较版本" }),
      );

      await waitFor(() => {
        expect((ruleHubSection as HTMLElement).textContent).toContain(
          "版本 diff：v1 -> v2（+1 / -1 / =1）",
        );
      });
      expect(
        ruleHubScreen.getByText("allow tool=github.read_repo"),
      ).toBeInTheDocument();
      expect(
        ruleHubScreen.getByText("deny tool=github.delete_repo"),
      ).toBeInTheDocument();
      expect(
        ruleHubScreen.getByText("require tag=verified"),
      ).toBeInTheDocument();

      expect(
        fetchSpy.mock.calls.some(
          ([url]) =>
            new URL(toUrl(url), "http://localhost").pathname ===
            "/api/v1/rules/assets/asset-diff/versions/diff",
        ),
      ).toBe(true);
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test(
    "治理页 Rule Hub 切换资产后会刷新发布/回滚/审批版本输入框",
    async () => {
      window.location.hash = "#/governance";
      setAuthTokens({
        accessToken: "access-token-governance-rule-hub-switch",
        refreshToken: "refresh-token-governance-rule-hub-switch",
        expiresIn: 1800,
        tokenType: "Bearer",
      });

      mockGovernancePageFetch({
        ruleAssets: [
          {
            id: "asset-alpha",
            name: "asset-alpha",
            status: "draft",
            latestVersion: 3,
            publishedVersion: 2,
          },
          {
            id: "asset-beta",
            name: "asset-beta",
            status: "published",
            latestVersion: 9,
            publishedVersion: 8,
          },
        ],
      });

      render(<App />);

      const ruleHubHeading = await screen.findByRole("heading", {
        name: "Rule Hub 规则资产",
        level: 2,
      });
      expect(ruleHubHeading).toBeInTheDocument();
      const ruleHubSection = ruleHubHeading.closest("section");
      if (!(ruleHubSection instanceof HTMLElement)) {
        throw new Error("未找到 Rule Hub 所在 section。");
      }
      const ruleHubScreen = within(ruleHubSection);

      const publishVersionInput = (await ruleHubScreen.findByLabelText(
        "发布版本",
      )) as HTMLInputElement;
      const rollbackVersionInput = (await ruleHubScreen.findByLabelText(
        "回滚版本",
      )) as HTMLInputElement;
      const approvalVersionInput = (await ruleHubScreen.findByLabelText(
        "审批版本",
      )) as HTMLInputElement;
      const diffFromVersionInput = (await ruleHubScreen.findByLabelText(
        "Diff 起始版本",
      )) as HTMLInputElement;
      const diffToVersionInput = (await ruleHubScreen.findByLabelText(
        "Diff 目标版本",
      )) as HTMLInputElement;

      await waitFor(() => {
        expect(publishVersionInput.value).toBe("3");
        expect(rollbackVersionInput.value).toBe("3");
        expect(approvalVersionInput.value).toBe("3");
        expect(diffFromVersionInput.value).toBe("2");
        expect(diffToVersionInput.value).toBe("3");
      });

      fireEvent.change(publishVersionInput, { target: { value: "1" } });
      fireEvent.change(rollbackVersionInput, { target: { value: "1" } });
      fireEvent.change(approvalVersionInput, { target: { value: "1" } });
      expect(publishVersionInput.value).toBe("1");
      expect(rollbackVersionInput.value).toBe("1");
      expect(approvalVersionInput.value).toBe("1");

      const betaRow = (await ruleHubScreen.findAllByRole("row")).find((row) =>
        row.textContent?.includes("asset-beta"),
      );
      if (!(betaRow instanceof HTMLElement)) {
        throw new Error("未找到 asset-beta 行。");
      }
      fireEvent.click(within(betaRow).getByRole("button", { name: "选中" }));

      await waitFor(() => {
        expect(publishVersionInput.value).toBe("9");
        expect(rollbackVersionInput.value).toBe("9");
        expect(approvalVersionInput.value).toBe("9");
        expect(diffFromVersionInput.value).toBe("8");
        expect(diffToVersionInput.value).toBe("9");
      });
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

  test(
    "治理页 Rule Hub 切换到无版本资产时会清空发布/回滚/审批版本输入框",
    async () => {
      window.location.hash = "#/governance";
      setAuthTokens({
        accessToken: "access-token-governance-rule-hub-empty-version",
        refreshToken: "refresh-token-governance-rule-hub-empty-version",
        expiresIn: 1800,
        tokenType: "Bearer",
      });

      mockGovernancePageFetch({
        ruleAssets: [
          {
            id: "asset-with-version",
            name: "asset-with-version",
            status: "published",
            latestVersion: 4,
            publishedVersion: 4,
          },
          {
            id: "asset-empty",
            name: "asset-empty",
            status: "draft",
            latestVersion: 0,
            publishedVersion: null,
          },
        ],
      });

      render(<App />);

      const ruleHubHeading = await screen.findByRole("heading", {
        name: "Rule Hub 规则资产",
        level: 2,
      });
      expect(ruleHubHeading).toBeInTheDocument();
      const ruleHubSection = ruleHubHeading.closest("section");
      if (!(ruleHubSection instanceof HTMLElement)) {
        throw new Error("未找到 Rule Hub 所在 section。");
      }
      const ruleHubScreen = within(ruleHubSection);

      const publishVersionInput = (await ruleHubScreen.findByLabelText(
        "发布版本",
      )) as HTMLInputElement;
      const rollbackVersionInput = (await ruleHubScreen.findByLabelText(
        "回滚版本",
      )) as HTMLInputElement;
      const approvalVersionInput = (await ruleHubScreen.findByLabelText(
        "审批版本",
      )) as HTMLInputElement;
      const diffFromVersionInput = (await ruleHubScreen.findByLabelText(
        "Diff 起始版本",
      )) as HTMLInputElement;
      const diffToVersionInput = (await ruleHubScreen.findByLabelText(
        "Diff 目标版本",
      )) as HTMLInputElement;

      await waitFor(() => {
        expect(publishVersionInput.value).toBe("4");
        expect(rollbackVersionInput.value).toBe("4");
        expect(approvalVersionInput.value).toBe("4");
        expect(diffFromVersionInput.value).toBe("3");
        expect(diffToVersionInput.value).toBe("4");
      });

      const emptyRow = (await ruleHubScreen.findAllByRole("row")).find((row) =>
        row.textContent?.includes("asset-empty"),
      );
      if (!(emptyRow instanceof HTMLElement)) {
        throw new Error("未找到 asset-empty 行。");
      }
      fireEvent.click(within(emptyRow).getByRole("button", { name: "选中" }));

      await waitFor(() => {
        expect(publishVersionInput.value).toBe("");
        expect(rollbackVersionInput.value).toBe("");
        expect(approvalVersionInput.value).toBe("");
        expect(diffFromVersionInput.value).toBe("");
        expect(diffToVersionInput.value).toBe("");
      });
    },
    GOVERNANCE_HEAVY_TEST_TIMEOUT_MS,
  );

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

    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (input, init) => {
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
            blob: async () =>
              new Blob(["id,tool\ns1,codex\n"], { type: "text/csv" }),
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
            blob: async () =>
              new Blob(["date,tokens\n2026-03-01,100\n"], { type: "text/csv" }),
            json: async () => ({}),
            text: async () => "date,tokens\n2026-03-01,100\n",
          } as Response;
        }

        throw new Error(`unexpected call: ${method} ${url}`);
      });

    try {
      render(<App />);

      fireEvent.click(
        await screen.findByRole("button", { name: "导出 Sessions" }),
      );
      expect(
        await screen.findByText("Sessions 导出成功：sessions-ui.csv"),
      ).toBeInTheDocument();

      fireEvent.change(screen.getByLabelText("维度"), {
        target: { value: "weekly" },
      });
      fireEvent.click(screen.getByRole("button", { name: "导出 Usage" }));
      expect(
        await screen.findByText("Usage 导出成功：usage-ui.csv"),
      ).toBeInTheDocument();

      expect(
        fetchSpy.mock.calls.some(([url, init]) => {
          const requestInit = init as RequestInit | undefined;
          return (
            toUrl(url).includes("/api/v1/exports/sessions?format=csv") &&
            (requestInit?.method ?? "GET").toUpperCase() === "GET"
          );
        }),
      ).toBe(true);
      expect(
        fetchSpy.mock.calls.some(([url, init]) => {
          const requestInit = init as RequestInit | undefined;
          return (
            toUrl(url).includes(
              "/api/v1/exports/usage?format=csv&dimension=weekly",
            ) && (requestInit?.method ?? "GET").toUpperCase() === "GET"
          );
        }),
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
