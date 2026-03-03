import { type FormEvent, useEffect, useMemo, useState } from "react";
import {
  QueryClient,
  QueryClientProvider,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import {
  ApiError,
  createSource,
  fetchHeatmap,
  fetchPricingCatalog,
  fetchSessionDetail,
  fetchSessionEvents,
  fetchSources,
  fetchUsageModels,
  fetchUsageMonthly,
  fetchUsageSessions,
  hasAccessToken,
  login,
  searchSessions,
  setUnauthorizedHandler,
  testSourceConnection,
  upsertPricingCatalog,
} from "./api";
import type {
  AuthLoginInput,
  CreateSourceInput,
  HeatmapCell,
  MetricKey,
  PricingCatalogEntry,
  PricingCatalogUpsertInput,
  Session,
  SessionDetailResponse,
  SourceConnectionTestResponse,
  SourceType,
  UsageAggregateFilters,
} from "./types";
import "./App.css";

const SOURCE_TYPE_OPTIONS: Array<{ value: SourceType; label: string }> = [
  { value: "local", label: "本地（local）" },
  { value: "ssh", label: "远程 SSH（ssh）" },
  { value: "sync-cache", label: "同步缓存（sync-cache）" },
];

type ConsoleRoute = "dashboard" | "sessions" | "analytics" | "sources" | "pricing";

const ROUTE_ITEMS: Array<{
  key: ConsoleRoute;
  label: string;
  title: string;
  subtitle: string;
}> = [
  {
    key: "dashboard",
    label: "Dashboard",
    title: "AI 使用热力图",
    subtitle: "看总览与时间分布。",
  },
  {
    key: "sessions",
    label: "Sessions",
    title: "会话中心",
    subtitle: "按日检索会话并下钻事件流。",
  },
  {
    key: "analytics",
    label: "Analytics",
    title: "聚合分析",
    subtitle: "接入 monthly/models/sessions 聚合接口。",
  },
  {
    key: "sources",
    label: "Sources",
    title: "Sources 管理",
    subtitle: "管理来源并执行 test-connection。",
  },
  {
    key: "pricing",
    label: "Pricing",
    title: "Pricing Catalog",
    subtitle: "读取并保存模型单价目录。",
  },
];

const DEFAULT_ROUTE: ConsoleRoute = "dashboard";

interface SourceFormState {
  name: string;
  type: SourceType;
  location: string;
  enabled: boolean;
}

const INITIAL_SOURCE_FORM: SourceFormState = {
  name: "",
  type: "local",
  location: "",
  enabled: true,
};

interface LoginFormState {
  email: string;
  password: string;
}

const INITIAL_LOGIN_FORM: LoginFormState = {
  email: "",
  password: "",
};

interface PricingEntryFormState {
  model: string;
  inputPer1k: string;
  outputPer1k: string;
  currency: string;
}

function createEmptyPricingEntry(): PricingEntryFormState {
  return {
    model: "",
    inputPer1k: "",
    outputPer1k: "",
    currency: "USD",
  };
}

function isConsoleRoute(value: string): value is ConsoleRoute {
  return ROUTE_ITEMS.some((item) => item.key === value);
}

function readRouteFromHash(): ConsoleRoute {
  if (typeof window === "undefined") {
    return DEFAULT_ROUTE;
  }

  const normalized = window.location.hash.replace(/^#\/?/, "").trim().toLowerCase();
  return isConsoleRoute(normalized) ? normalized : DEFAULT_ROUTE;
}

function writeRouteToHash(route: ConsoleRoute) {
  if (typeof window === "undefined") {
    return;
  }
  const nextHash = `#/${route}`;
  if (window.location.hash !== nextHash) {
    window.location.hash = nextHash;
  }
}

function formatDateTime(isoDate: string): string {
  const date = new Date(isoDate);
  if (Number.isNaN(date.getTime())) {
    return isoDate;
  }

  return date.toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "未知错误";
}

function toDateKey(isoDate: string): string {
  return isoDate.slice(0, 10);
}

function nextDateKey(dateKey: string): string {
  const date = new Date(`${dateKey}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + 1);
  return date.toISOString().slice(0, 10);
}

function createDateSeries(days: number): string[] {
  const end = new Date();
  end.setUTCHours(0, 0, 0, 0);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - (days - 1));

  const result: string[] = [];
  for (let i = 0; i < days; i += 1) {
    const date = new Date(start);
    date.setUTCDate(start.getUTCDate() + i);
    result.push(date.toISOString().slice(0, 10));
  }

  return result;
}

function getMetricValue(cell: HeatmapCell | undefined, metric: MetricKey): number {
  if (!cell) {
    return 0;
  }

  if (metric === "tokens") {
    return cell.tokens;
  }
  if (metric === "cost") {
    return cell.cost;
  }
  return cell.sessions;
}

function getIntensityLevel(value: number, max: number): number {
  if (value <= 0 || max <= 0) {
    return 0;
  }

  const ratio = value / max;
  if (ratio <= 0.25) {
    return 1;
  }
  if (ratio <= 0.5) {
    return 2;
  }
  if (ratio <= 0.75) {
    return 3;
  }
  return 4;
}

function formatMetric(value: number, metric: MetricKey): string {
  if (metric === "tokens") {
    return `${value.toLocaleString("zh-CN")} tokens`;
  }
  if (metric === "cost") {
    return `$${value.toFixed(2)}`;
  }
  return `${value.toLocaleString("zh-CN")} sessions`;
}

function todayDateKey(): string {
  return new Date().toISOString().slice(0, 10);
}

function daysAgoDateKey(days: number): string {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - days);
  return date.toISOString().slice(0, 10);
}

function parsePriceNumber(raw: string): number | null {
  const normalized = raw.trim();
  if (!normalized) {
    return null;
  }
  const value = Number(normalized);
  if (!Number.isFinite(value) || value < 0) {
    return null;
  }
  return value;
}

function mapPricingEntryToForm(entry: PricingCatalogEntry): PricingEntryFormState {
  return {
    model: entry.model,
    inputPer1k: String(entry.inputPer1k),
    outputPer1k: String(entry.outputPer1k),
    currency: entry.currency ?? "USD",
  };
}

function normalizePricingForm(
  rows: PricingEntryFormState[]
): { success: true; entries: PricingCatalogEntry[] } | { success: false; message: string } {
  const entries: PricingCatalogEntry[] = [];

  for (const row of rows) {
    const model = row.model.trim();
    const inputPer1k = parsePriceNumber(row.inputPer1k);
    const outputPer1k = parsePriceNumber(row.outputPer1k);

    if (!model && inputPer1k === null && outputPer1k === null) {
      continue;
    }

    if (!model) {
      return {
        success: false,
        message: "pricing 条目缺少 model。",
      };
    }
    if (inputPer1k === null || outputPer1k === null) {
      return {
        success: false,
        message: `pricing 条目 ${model} 的 input/output 单价必须是 >= 0 的数字。`,
      };
    }

    const currency = row.currency.trim();
    entries.push({
      model,
      inputPer1k,
      outputPer1k,
      currency: currency.length > 0 ? currency : undefined,
    });
  }

  if (entries.length === 0) {
    return {
      success: false,
      message: "至少保留一个 pricing 条目。",
    };
  }

  return {
    success: true,
    entries,
  };
}

interface LoginPageProps {
  authMessage: string | null;
  onLoggedIn: () => void;
}

function LoginPage({ authMessage, onLoggedIn }: LoginPageProps) {
  const [loginForm, setLoginForm] = useState<LoginFormState>(INITIAL_LOGIN_FORM);
  const [formError, setFormError] = useState<string | null>(null);

  const loginMutation = useMutation({
    mutationFn: (input: AuthLoginInput) => login(input),
    onSuccess: () => {
      setLoginForm(INITIAL_LOGIN_FORM);
      setFormError(null);
      onLoggedIn();
    },
  });

  function handleLoginSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const email = loginForm.email.trim();
    const password = loginForm.password.trim();
    if (!email || !password) {
      setFormError("邮箱和密码不能为空。");
      return;
    }

    setFormError(null);
    loginMutation.mutate({
      email,
      password,
    });
  }

  return (
    <main className="page-shell auth-shell">
      <section className="panel auth-panel">
        <header>
          <div>
            <p className="eyebrow">AgentLedger 企业治理台</p>
            <h1>登录控制台</h1>
            <p className="subtitle">请先登录后再访问各业务页面。</p>
          </div>
        </header>

        {authMessage ? <p className="feedback error">{authMessage}</p> : null}

        <form className="login-form" onSubmit={handleLoginSubmit}>
          <label htmlFor="login-email">邮箱</label>
          <input
            id="login-email"
            type="email"
            autoComplete="email"
            placeholder="例如：owner@example.com"
            value={loginForm.email}
            onChange={(event) =>
              setLoginForm((prev) => ({
                ...prev,
                email: event.target.value,
              }))
            }
          />

          <label htmlFor="login-password">密码</label>
          <input
            id="login-password"
            type="password"
            autoComplete="current-password"
            placeholder="请输入密码"
            value={loginForm.password}
            onChange={(event) =>
              setLoginForm((prev) => ({
                ...prev,
                password: event.target.value,
              }))
            }
          />

          <button type="submit" className="submit-button" disabled={loginMutation.isPending}>
            {loginMutation.isPending ? "登录中..." : "登录"}
          </button>

          {formError ? <p className="feedback error">{formError}</p> : null}
          {loginMutation.isError ? (
            <p className="feedback error">登录失败：{toErrorMessage(loginMutation.error)}</p>
          ) : null}
        </form>
      </section>
    </main>
  );
}

function DashboardPage() {
  const [metric, setMetric] = useState<MetricKey>("tokens");
  const [selectedDate, setSelectedDate] = useState<string | null>(null);

  const heatmapQuery = useQuery({
    queryKey: ["usage-heatmap"],
    queryFn: ({ signal }) => fetchHeatmap(signal),
    staleTime: 20_000,
  });

  const heatmapCells = heatmapQuery.data?.cells ?? [];
  const cellMap = useMemo(() => {
    const map = new Map<string, HeatmapCell>();
    for (const cell of heatmapCells) {
      map.set(toDateKey(cell.date), cell);
    }
    return map;
  }, [heatmapCells]);

  const defaultDate = useMemo(() => {
    if (selectedDate) {
      return selectedDate;
    }
    if (heatmapCells.length > 0) {
      return toDateKey(heatmapCells[heatmapCells.length - 1].date);
    }
    return createDateSeries(84).at(-1) ?? null;
  }, [heatmapCells, selectedDate]);

  const series = useMemo(() => createDateSeries(84), []);
  const maxValue = useMemo(() => {
    let max = 0;
    for (const dateKey of series) {
      const value = getMetricValue(cellMap.get(dateKey), metric);
      if (value > max) {
        max = value;
      }
    }
    return max;
  }, [cellMap, metric, series]);

  const summary = heatmapQuery.data?.summary;

  return (
    <>
      <section className="kpi-grid" aria-label="KPI 概览">
        <article className="kpi-card">
          <h2>总 Tokens</h2>
          <strong>{summary?.tokens.toLocaleString("zh-CN") ?? "--"}</strong>
        </article>
        <article className="kpi-card">
          <h2>总 Cost</h2>
          <strong>{summary ? `$${summary.cost.toFixed(2)}` : "--"}</strong>
        </article>
        <article className="kpi-card">
          <h2>总 Sessions</h2>
          <strong>{summary?.sessions.toLocaleString("zh-CN") ?? "--"}</strong>
        </article>
      </section>

      <section className="panel heatmap-panel">
        <header>
          <h2>GitHub 风格热力图</h2>
          <div className="metric-switch" role="tablist" aria-label="指标切换">
            {(["tokens", "cost", "sessions"] as MetricKey[]).map((item) => (
              <button
                key={item}
                type="button"
                role="tab"
                aria-selected={metric === item}
                className={metric === item ? "is-active" : ""}
                onClick={() => setMetric(item)}
              >
                {item}
              </button>
            ))}
          </div>
        </header>

        <p>
          当前指标：<span>{metric}</span>
          {defaultDate ? ` | 当前下钻日期：${defaultDate}` : ""}
        </p>

        {heatmapQuery.isLoading ? <p className="feedback info">热力图加载中...</p> : null}
        {heatmapQuery.isError ? <p className="feedback error">热力图加载失败，请稍后重试。</p> : null}

        <div className="heatmap-grid" role="grid" aria-label="使用热力图">
          {series.map((dateKey) => {
            const cell = cellMap.get(dateKey);
            const value = getMetricValue(cell, metric);
            const level = getIntensityLevel(value, maxValue);
            const isSelected = defaultDate === dateKey;

            return (
              <button
                key={dateKey}
                role="gridcell"
                type="button"
                className={`heatmap-cell level-${level} ${isSelected ? "is-selected" : ""}`}
                onClick={() => setSelectedDate(dateKey)}
                title={`${dateKey} | ${formatMetric(value, metric)}`}
                aria-label={`${dateKey} ${formatMetric(value, metric)}`}
              />
            );
          })}
        </div>

        <div className="legend">
          <span>低</span>
          <div className="legend-steps">
            <i className="level-0" />
            <i className="level-1" />
            <i className="level-2" />
            <i className="level-3" />
            <i className="level-4" />
          </div>
          <span>高</span>
        </div>
      </section>
    </>
  );
}

function SessionsPage() {
  const [dateKey, setDateKey] = useState(() => todayDateKey());
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null);

  const sessionsQuery = useQuery({
    queryKey: ["sessions-search", dateKey],
    queryFn: ({ signal }) =>
      searchSessions(
        {
          from: `${dateKey}T00:00:00.000Z`,
          to: `${nextDateKey(dateKey)}T00:00:00.000Z`,
          limit: 50,
        },
        signal
      ),
    staleTime: 20_000,
  });

  const sessions = sessionsQuery.data?.items ?? [];

  useEffect(() => {
    if (sessions.length === 0) {
      setSelectedSessionId(null);
      return;
    }

    if (!selectedSessionId || !sessions.some((item) => item.id === selectedSessionId)) {
      setSelectedSessionId(sessions[0].id);
    }
  }, [selectedSessionId, sessions]);

  const eventsQuery = useQuery({
    queryKey: ["session-events", selectedSessionId],
    enabled: Boolean(selectedSessionId),
    queryFn: ({ signal }) => fetchSessionEvents(selectedSessionId!, 50, signal),
    staleTime: 20_000,
  });

  const detailQuery = useQuery({
    queryKey: ["session-detail", selectedSessionId],
    enabled: Boolean(selectedSessionId),
    queryFn: ({ signal }) => fetchSessionDetail(selectedSessionId!, signal),
    staleTime: 20_000,
  });

  const eventItems = eventsQuery.data?.items ?? [];
  const sessionDetail = detailQuery.data as SessionDetailResponse | undefined;

  return (
    <>
      <section className="panel">
        <header>
          <h2>会话列表</h2>
          <label className="inline-field" htmlFor="session-date">
            日期
            <input
              id="session-date"
              type="date"
              value={dateKey}
              onChange={(event) => setDateKey(event.target.value)}
            />
          </label>
        </header>

        {sessionsQuery.isLoading ? <p className="feedback info">会话加载中...</p> : null}
        {sessionsQuery.isError ? (
          <p className="feedback error">会话加载失败：{toErrorMessage(sessionsQuery.error)}</p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>开始时间</th>
                <th>工具</th>
                <th>模型</th>
                <th>来源主机</th>
                <th>Tokens</th>
                <th>Cost</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {sessions.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={7}>
                    暂无会话数据
                  </td>
                </tr>
              ) : (
                sessions.map((session: Session) => (
                  <tr
                    key={session.id}
                    className={selectedSessionId === session.id ? "is-selected-row" : ""}
                  >
                    <td>{formatDateTime(session.startedAt)}</td>
                    <td>{session.tool}</td>
                    <td>{session.model}</td>
                    <td>{session.sourceId}</td>
                    <td>{session.tokens.toLocaleString("zh-CN")}</td>
                    <td>${session.cost.toFixed(2)}</td>
                    <td>
                      <button
                        type="button"
                        className="table-action"
                        onClick={() => setSelectedSessionId(session.id)}
                      >
                        查看事件
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>会话详情</h2>
          <p>{selectedSessionId ? `sessionId: ${selectedSessionId}` : "请选择会话"}</p>
        </header>

        {detailQuery.isLoading ? <p className="feedback info">会话详情加载中...</p> : null}
        {detailQuery.isError ? (
          <p className="feedback error">会话详情加载失败：{toErrorMessage(detailQuery.error)}</p>
        ) : null}

        {sessionDetail ? (
          <div className="session-detail-grid">
            <div className="session-detail-card">
              <h3>基础信息</h3>
              <p>工具：{sessionDetail.session?.tool ?? sessionDetail.tool}</p>
              <p>模型：{sessionDetail.session?.model ?? sessionDetail.model}</p>
              <p>开始：{formatDateTime(sessionDetail.session?.startedAt ?? sessionDetail.startedAt)}</p>
              <p>
                结束：
                {sessionDetail.session?.endedAt ?? sessionDetail.endedAt
                  ? formatDateTime(sessionDetail.session?.endedAt ?? sessionDetail.endedAt ?? "")
                  : "--"}
              </p>
              <p>消息数：{sessionDetail.session?.messageCount ?? sessionDetail.messageCount}</p>
            </div>

            <div className="session-detail-card">
              <h3>Token 分解</h3>
              <p>input：{sessionDetail.tokenBreakdown.inputTokens.toLocaleString("zh-CN")}</p>
              <p>output：{sessionDetail.tokenBreakdown.outputTokens.toLocaleString("zh-CN")}</p>
              <p>cache read：{sessionDetail.tokenBreakdown.cacheReadTokens.toLocaleString("zh-CN")}</p>
              <p>
                cache write：
                {sessionDetail.tokenBreakdown.cacheWriteTokens.toLocaleString("zh-CN")}
              </p>
              <p>
                reasoning：
                {sessionDetail.tokenBreakdown.reasoningTokens.toLocaleString("zh-CN")}
              </p>
              <p>
                total：<strong>{sessionDetail.tokenBreakdown.totalTokens.toLocaleString("zh-CN")}</strong>
              </p>
            </div>

            <div className="session-detail-card">
              <h3>来源追溯</h3>
              <p>sourceId：{sessionDetail.sourceTrace.sourceId}</p>
              <p>sourceName：{sessionDetail.sourceTrace.sourceName ?? "--"}</p>
              <p>provider：{sessionDetail.sourceTrace.provider ?? "--"}</p>
              <p>path：{sessionDetail.sourceTrace.path ?? "--"}</p>
            </div>
          </div>
        ) : null}
      </section>

      <section className="panel">
        <header>
          <h2>Session 事件流</h2>
          <p>{selectedSessionId ? `sessionId: ${selectedSessionId}` : "请选择会话"}</p>
        </header>

        {eventsQuery.isLoading ? <p className="feedback info">事件加载中...</p> : null}
        {eventsQuery.isError ? (
          <p className="feedback error">事件加载失败：{toErrorMessage(eventsQuery.error)}</p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>时间</th>
                <th>类型</th>
                <th>角色</th>
                <th>模型</th>
                <th>文本</th>
                <th>Cost</th>
              </tr>
            </thead>
            <tbody>
              {eventItems.length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={6}>
                    暂无事件
                  </td>
                </tr>
              ) : (
                eventItems.map((event) => (
                  <tr key={event.id}>
                    <td>{formatDateTime(event.timestamp)}</td>
                    <td>{event.eventType}</td>
                    <td>{event.role ?? "--"}</td>
                    <td>{event.model ?? "--"}</td>
                    <td className="event-text-cell">{event.text ?? "--"}</td>
                    <td>${event.cost.toFixed(4)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

function AnalyticsPage() {
  const [fromDate, setFromDate] = useState(() => daysAgoDateKey(29));
  const [toDate, setToDate] = useState(() => todayDateKey());

  const rangeValid = fromDate <= toDate;

  const filters = useMemo<UsageAggregateFilters>(
    () => ({
      from: `${fromDate}T00:00:00.000Z`,
      to: `${toDate}T23:59:59.999Z`,
      limit: 50,
    }),
    [fromDate, toDate]
  );

  const monthlyQuery = useQuery({
    queryKey: ["usage-monthly", filters],
    enabled: rangeValid,
    queryFn: ({ signal }) => fetchUsageMonthly(filters, signal),
    staleTime: 20_000,
  });

  const modelsQuery = useQuery({
    queryKey: ["usage-models", filters],
    enabled: rangeValid,
    queryFn: ({ signal }) => fetchUsageModels(filters, signal),
    staleTime: 20_000,
  });

  const sessionBreakdownQuery = useQuery({
    queryKey: ["usage-sessions", filters],
    enabled: rangeValid,
    queryFn: ({ signal }) => fetchUsageSessions(filters, signal),
    staleTime: 20_000,
  });

  return (
    <>
      <section className="panel">
        <header>
          <h2>筛选条件</h2>
          <p>当前 limit=50</p>
        </header>

        <div className="filters-row">
          <label className="inline-field" htmlFor="analytics-from">
            开始
            <input
              id="analytics-from"
              type="date"
              value={fromDate}
              onChange={(event) => setFromDate(event.target.value)}
            />
          </label>
          <label className="inline-field" htmlFor="analytics-to">
            结束
            <input
              id="analytics-to"
              type="date"
              value={toDate}
              onChange={(event) => setToDate(event.target.value)}
            />
          </label>
        </div>

        {!rangeValid ? <p className="feedback error">开始日期不能晚于结束日期。</p> : null}
      </section>

      <section className="panel">
        <header>
          <h2>月度聚合（usage/monthly）</h2>
          <p>共 {monthlyQuery.data?.total ?? 0} 条</p>
        </header>
        {monthlyQuery.isLoading ? <p className="feedback info">monthly 加载中...</p> : null}
        {monthlyQuery.isError ? (
          <p className="feedback error">monthly 加载失败：{toErrorMessage(monthlyQuery.error)}</p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>月份</th>
                <th>Tokens</th>
                <th>Cost</th>
                <th>Sessions</th>
              </tr>
            </thead>
            <tbody>
              {(monthlyQuery.data?.items ?? []).length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={4}>
                    暂无数据
                  </td>
                </tr>
              ) : (
                (monthlyQuery.data?.items ?? []).map((item) => (
                  <tr key={item.month}>
                    <td>{item.month}</td>
                    <td>{item.tokens.toLocaleString("zh-CN")}</td>
                    <td>${item.cost.toFixed(4)}</td>
                    <td>{item.sessions.toLocaleString("zh-CN")}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>模型排行（usage/models）</h2>
          <p>共 {modelsQuery.data?.total ?? 0} 条</p>
        </header>
        {modelsQuery.isLoading ? <p className="feedback info">models 加载中...</p> : null}
        {modelsQuery.isError ? (
          <p className="feedback error">models 加载失败：{toErrorMessage(modelsQuery.error)}</p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Model</th>
                <th>Tokens</th>
                <th>Cost</th>
                <th>Sessions</th>
              </tr>
            </thead>
            <tbody>
              {(modelsQuery.data?.items ?? []).length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={4}>
                    暂无数据
                  </td>
                </tr>
              ) : (
                (modelsQuery.data?.items ?? []).map((item) => (
                  <tr key={item.model}>
                    <td>{item.model}</td>
                    <td>{item.tokens.toLocaleString("zh-CN")}</td>
                    <td>${item.cost.toFixed(4)}</td>
                    <td>{item.sessions.toLocaleString("zh-CN")}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <header>
          <h2>会话拆解（usage/sessions）</h2>
          <p>共 {sessionBreakdownQuery.data?.total ?? 0} 条</p>
        </header>
        {sessionBreakdownQuery.isLoading ? <p className="feedback info">sessions 加载中...</p> : null}
        {sessionBreakdownQuery.isError ? (
          <p className="feedback error">
            sessions 加载失败：{toErrorMessage(sessionBreakdownQuery.error)}
          </p>
        ) : null}

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Session</th>
                <th>Source</th>
                <th>工具</th>
                <th>模型</th>
                <th>开始时间</th>
                <th>Total Tokens</th>
                <th>Cost</th>
              </tr>
            </thead>
            <tbody>
              {(sessionBreakdownQuery.data?.items ?? []).length === 0 ? (
                <tr>
                  <td className="table-empty-cell" colSpan={7}>
                    暂无数据
                  </td>
                </tr>
              ) : (
                (sessionBreakdownQuery.data?.items ?? []).map((item) => (
                  <tr key={`${item.sessionId}:${item.startedAt}`}>
                    <td>{item.sessionId}</td>
                    <td>{item.sourceId}</td>
                    <td>{item.tool}</td>
                    <td>{item.model}</td>
                    <td>{formatDateTime(item.startedAt)}</td>
                    <td>{item.totalTokens.toLocaleString("zh-CN")}</td>
                    <td>${item.cost.toFixed(4)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

function SourcesPage() {
  const [sourceForm, setSourceForm] = useState<SourceFormState>(INITIAL_SOURCE_FORM);
  const [sourceFormError, setSourceFormError] = useState<string | null>(null);
  const [connectionResults, setConnectionResults] = useState<
    Record<string, { success: boolean; message: string }>
  >({});
  const queryClient = useQueryClient();

  const sourcesQuery = useQuery({
    queryKey: ["sources"],
    queryFn: ({ signal }) => fetchSources(signal),
    staleTime: 20_000,
  });

  const createSourceMutation = useMutation({
    mutationFn: (input: CreateSourceInput) => createSource(input),
    onSuccess: async () => {
      setSourceForm(INITIAL_SOURCE_FORM);
      setSourceFormError(null);
      await queryClient.invalidateQueries({ queryKey: ["sources"] });
    },
  });

  const testConnectionMutation = useMutation({
    mutationFn: (sourceId: string) => testSourceConnection(sourceId),
    onSuccess: (result: SourceConnectionTestResponse) => {
      setConnectionResults((prev) => ({
        ...prev,
        [result.sourceId]: {
          success: result.success,
          message: `${result.success ? "成功" : "失败"} (${result.latencyMs}ms)：${result.detail}`,
        },
      }));
    },
    onError: (error, sourceId) => {
      setConnectionResults((prev) => ({
        ...prev,
        [sourceId]: {
          success: false,
          message: toErrorMessage(error),
        },
      }));
    },
  });

  const sourceItems = sourcesQuery.data?.items ?? [];

  function handleSourceSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const name = sourceForm.name.trim();
    const location = sourceForm.location.trim();
    if (!name || !location) {
      setSourceFormError("名称和位置不能为空。");
      return;
    }

    setSourceFormError(null);
    createSourceMutation.mutate({
      name,
      type: sourceForm.type,
      location,
      enabled: sourceForm.enabled,
    });
  }

  return (
    <section className="panel source-panel">
      <header>
        <h2>Sources 管理</h2>
        <p>来源总数：{sourcesQuery.data?.total ?? sourceItems.length}</p>
      </header>

      <div className="source-layout">
        <div className="source-list-block">
          <h3>来源列表</h3>
          {sourcesQuery.isLoading ? <p className="feedback info">Sources 加载中...</p> : null}
          {sourcesQuery.isFetching && !sourcesQuery.isLoading ? (
            <p className="feedback info">Sources 刷新中...</p>
          ) : null}
          {sourcesQuery.isError ? (
            <p className="feedback error">Sources 加载失败：{toErrorMessage(sourcesQuery.error)}</p>
          ) : null}

          {!sourcesQuery.isLoading && !sourcesQuery.isError && sourceItems.length === 0 ? (
            <p className="feedback empty">暂无 Source，请先新增。</p>
          ) : null}

          {!sourcesQuery.isError && sourceItems.length > 0 ? (
            <div className="source-table-wrapper">
              <table className="source-table">
                <thead>
                  <tr>
                    <th>名称</th>
                    <th>类型</th>
                    <th>位置</th>
                    <th>状态</th>
                    <th>创建时间</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {sourceItems.map((source) => {
                    const latestResult = connectionResults[source.id];
                    const isTesting =
                      testConnectionMutation.isPending &&
                      testConnectionMutation.variables === source.id;

                    return (
                      <tr key={source.id}>
                        <td>{source.name}</td>
                        <td>{source.type}</td>
                        <td>{source.location}</td>
                        <td>{source.enabled ? "启用" : "停用"}</td>
                        <td>{formatDateTime(source.createdAt)}</td>
                        <td>
                          <button
                            type="button"
                            className="table-action"
                            disabled={isTesting}
                            onClick={() => testConnectionMutation.mutate(source.id)}
                          >
                            {isTesting ? "测试中..." : "测试连接"}
                          </button>
                          {latestResult ? (
                            <p
                              className={`tiny-feedback ${
                                latestResult.success ? "tiny-feedback-success" : "tiny-feedback-error"
                              }`}
                            >
                              {latestResult.message}
                            </p>
                          ) : null}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>

        <form className="source-form" onSubmit={handleSourceSubmit}>
          <h3>新增 Source</h3>
          <label htmlFor="source-name">名称</label>
          <input
            id="source-name"
            type="text"
            placeholder="例如：devbox-shanghai"
            value={sourceForm.name}
            onChange={(event) =>
              setSourceForm((prev) => ({
                ...prev,
                name: event.target.value,
              }))
            }
          />

          <label htmlFor="source-type">类型</label>
          <select
            id="source-type"
            value={sourceForm.type}
            onChange={(event) =>
              setSourceForm((prev) => ({
                ...prev,
                type: event.target.value as SourceType,
              }))
            }
          >
            {SOURCE_TYPE_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>

          <label htmlFor="source-location">位置</label>
          <input
            id="source-location"
            type="text"
            placeholder="例如：cn-shanghai / 10.0.0.8"
            value={sourceForm.location}
            onChange={(event) =>
              setSourceForm((prev) => ({
                ...prev,
                location: event.target.value,
              }))
            }
          />

          <label className="checkbox-field" htmlFor="source-enabled">
            <input
              id="source-enabled"
              type="checkbox"
              checked={sourceForm.enabled}
              onChange={(event) =>
                setSourceForm((prev) => ({
                  ...prev,
                  enabled: event.target.checked,
                }))
              }
            />
            启用该 Source
          </label>

          <button type="submit" className="submit-button" disabled={createSourceMutation.isPending}>
            {createSourceMutation.isPending ? "提交中..." : "新增 Source"}
          </button>

          {sourceFormError ? <p className="feedback error">{sourceFormError}</p> : null}
          {createSourceMutation.isError ? (
            <p className="feedback error">新增失败：{toErrorMessage(createSourceMutation.error)}</p>
          ) : null}
          {createSourceMutation.isSuccess ? (
            <p className="feedback success">新增成功，列表已刷新。</p>
          ) : null}
        </form>
      </div>
    </section>
  );
}

function PricingPage() {
  const queryClient = useQueryClient();
  const [note, setNote] = useState("");
  const [entries, setEntries] = useState<PricingEntryFormState[]>([createEmptyPricingEntry()]);
  const [formError, setFormError] = useState<string | null>(null);
  const [loadedVersionId, setLoadedVersionId] = useState<string>("");

  const catalogQuery = useQuery({
    queryKey: ["pricing-catalog"],
    queryFn: async ({ signal }) => {
      try {
        return await fetchPricingCatalog(signal);
      } catch (error) {
        if (error instanceof ApiError && error.status === 404) {
          return null;
        }
        throw error;
      }
    },
    staleTime: 20_000,
  });

  useEffect(() => {
    if (catalogQuery.data === undefined) {
      return;
    }

    const nextVersionId = catalogQuery.data?.version.id ?? "__empty__";
    if (loadedVersionId === nextVersionId) {
      return;
    }

    setLoadedVersionId(nextVersionId);
    setFormError(null);

    if (catalogQuery.data) {
      setNote(catalogQuery.data.version.note ?? "");
      setEntries(
        catalogQuery.data.entries.length > 0
          ? catalogQuery.data.entries.map((entry) => mapPricingEntryToForm(entry))
          : [createEmptyPricingEntry()]
      );
      return;
    }

    setNote("");
    setEntries([createEmptyPricingEntry()]);
  }, [catalogQuery.data, loadedVersionId]);

  const saveMutation = useMutation({
    mutationFn: (input: PricingCatalogUpsertInput) => upsertPricingCatalog(input),
    onSuccess: async () => {
      setFormError(null);
      await queryClient.invalidateQueries({ queryKey: ["pricing-catalog"] });
    },
  });

  function updateEntry(index: number, key: keyof PricingEntryFormState, value: string) {
    setEntries((prev) => prev.map((entry, idx) => (idx === index ? { ...entry, [key]: value } : entry)));
  }

  function handleSave(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    const normalized = normalizePricingForm(entries);
    if (!normalized.success) {
      setFormError(normalized.message);
      return;
    }

    setFormError(null);
    saveMutation.mutate({
      note: note.trim().length > 0 ? note.trim() : undefined,
      entries: normalized.entries,
    });
  }

  return (
    <section className="panel">
      <header>
        <h2>Pricing Catalog</h2>
        <p>
          {catalogQuery.data
            ? `当前版本 v${catalogQuery.data.version.version} (${formatDateTime(
                catalogQuery.data.version.createdAt
              )})`
            : "当前租户尚未配置 catalog，可直接新建。"}
        </p>
      </header>

      {catalogQuery.isLoading ? <p className="feedback info">pricing 加载中...</p> : null}
      {catalogQuery.isError ? (
        <p className="feedback error">pricing 加载失败：{toErrorMessage(catalogQuery.error)}</p>
      ) : null}

      <form className="pricing-form" onSubmit={handleSave}>
        <label htmlFor="pricing-note">版本备注</label>
        <input
          id="pricing-note"
          type="text"
          placeholder="例如：2026Q1 基线价格"
          value={note}
          onChange={(event) => setNote(event.target.value)}
        />

        <div className="table-wrapper">
          <table className="session-table">
            <thead>
              <tr>
                <th>Model</th>
                <th>Input /1k</th>
                <th>Output /1k</th>
                <th>Currency</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((entry, index) => (
                <tr key={`pricing-entry-${index}`}> 
                  <td>
                    <input
                      type="text"
                      value={entry.model}
                      onChange={(event) => updateEntry(index, "model", event.target.value)}
                      placeholder="gpt-5"
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={entry.inputPer1k}
                      onChange={(event) => updateEntry(index, "inputPer1k", event.target.value)}
                      placeholder="0.003"
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={entry.outputPer1k}
                      onChange={(event) => updateEntry(index, "outputPer1k", event.target.value)}
                      placeholder="0.012"
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      value={entry.currency}
                      onChange={(event) => updateEntry(index, "currency", event.target.value)}
                      placeholder="USD"
                    />
                  </td>
                  <td>
                    <button
                      type="button"
                      className="table-action"
                      onClick={() =>
                        setEntries((prev) =>
                          prev.length > 1
                            ? prev.filter((_, rowIndex) => rowIndex !== index)
                            : [createEmptyPricingEntry()]
                        )
                      }
                    >
                      删除
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="button-row">
          <button
            type="button"
            className="submit-button secondary-button"
            onClick={() => setEntries((prev) => [...prev, createEmptyPricingEntry()])}
          >
            新增条目
          </button>
          <button type="submit" className="submit-button" disabled={saveMutation.isPending}>
            {saveMutation.isPending ? "保存中..." : "保存 Catalog"}
          </button>
        </div>

        {formError ? <p className="feedback error">{formError}</p> : null}
        {saveMutation.isError ? (
          <p className="feedback error">保存失败：{toErrorMessage(saveMutation.error)}</p>
        ) : null}
        {saveMutation.isSuccess ? <p className="feedback success">保存成功。</p> : null}
      </form>
    </section>
  );
}

interface WorkspaceProps {
  route: ConsoleRoute;
  onRouteChange: (route: ConsoleRoute) => void;
}

function Workspace({ route, onRouteChange }: WorkspaceProps) {
  const activeRoute = ROUTE_ITEMS.find((item) => item.key === route) ?? ROUTE_ITEMS[0];

  return (
    <main className="page-shell">
      <section className="header-band">
        <div>
          <p className="eyebrow">AgentLedger 企业治理台</p>
          <h1>{activeRoute.title}</h1>
          <p className="subtitle">{activeRoute.subtitle}</p>
        </div>
        <nav className="route-nav" aria-label="页面切换">
          {ROUTE_ITEMS.map((item) => (
            <button
              key={item.key}
              type="button"
              className={route === item.key ? "is-active" : ""}
              onClick={() => onRouteChange(item.key)}
            >
              {item.label}
            </button>
          ))}
        </nav>
      </section>

      {route === "dashboard" ? <DashboardPage /> : null}
      {route === "sessions" ? <SessionsPage /> : null}
      {route === "analytics" ? <AnalyticsPage /> : null}
      {route === "sources" ? <SourcesPage /> : null}
      {route === "pricing" ? <PricingPage /> : null}
    </main>
  );
}

export default function App() {
  const [queryClient] = useState(() => new QueryClient());
  const [isAuthenticated, setIsAuthenticated] = useState(() => hasAccessToken());
  const [authMessage, setAuthMessage] = useState<string | null>(null);
  const [route, setRoute] = useState<ConsoleRoute>(() => readRouteFromHash());

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const handleHashChange = () => {
      setRoute(readRouteFromHash());
    };

    window.addEventListener("hashchange", handleHashChange);
    if (!window.location.hash) {
      writeRouteToHash(DEFAULT_ROUTE);
    }
    setRoute(readRouteFromHash());

    return () => {
      window.removeEventListener("hashchange", handleHashChange);
    };
  }, []);

  useEffect(() => {
    setUnauthorizedHandler((message) => {
      queryClient.clear();
      setIsAuthenticated(false);
      setAuthMessage(message);
    });
    return () => {
      setUnauthorizedHandler(null);
    };
  }, [queryClient]);

  function handleLoggedIn() {
    queryClient.clear();
    setAuthMessage(null);
    setIsAuthenticated(true);
    setRoute(DEFAULT_ROUTE);
    writeRouteToHash(DEFAULT_ROUTE);
  }

  function handleRouteChange(nextRoute: ConsoleRoute) {
    setRoute(nextRoute);
    writeRouteToHash(nextRoute);
  }

  return (
    <QueryClientProvider client={queryClient}>
      {isAuthenticated ? (
        <Workspace route={route} onRouteChange={handleRouteChange} />
      ) : (
        <LoginPage authMessage={authMessage} onLoggedIn={handleLoggedIn} />
      )}
    </QueryClientProvider>
  );
}
