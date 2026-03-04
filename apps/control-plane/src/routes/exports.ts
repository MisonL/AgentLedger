import { Hono, type Context } from "hono";
import type {
  HeatmapCell,
  Session,
  SessionExportJob,
  SessionSearchInput,
  UsageDailyItem,
  UsageExportDimension,
  UsageModelItem,
  UsageMonthlyItem,
  UsageSessionBreakdownItem,
  UsageWeekItem,
} from "../contracts";
import {
  validateExportJobId,
  validateSessionExportJobCreateInput,
  validateSessionExportQueryInput,
  validateUsageExportQueryInput,
} from "../contracts";
import type { AppendAuditLogInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import { parseOptionalTimePaginationCursor } from "./pagination-cursor";
import type { AppEnv } from "../types";

export const exportRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const exportJobStore = new Map<string, SessionExportJobRecord>();

const SESSION_EXPORT_CSV_HEADERS = [
  "id",
  "sourceId",
  "tool",
  "model",
  "startedAt",
  "endedAt",
  "tokens",
  "cost",
];
const USAGE_DAILY_EXPORT_CSV_HEADERS = [
  "date",
  "tokens",
  "cost",
  "sessions",
  "costRaw",
  "costEstimated",
  "costMode",
];
const USAGE_WEEKLY_EXPORT_CSV_HEADERS = [
  "weekStart",
  "weekEnd",
  "tokens",
  "cost",
  "sessions",
];
const USAGE_MONTHLY_EXPORT_CSV_HEADERS = [
  "month",
  "tokens",
  "cost",
  "sessions",
  "costRaw",
  "costEstimated",
  "costMode",
];
const USAGE_MODEL_EXPORT_CSV_HEADERS = [
  "model",
  "tokens",
  "cost",
  "sessions",
  "costRaw",
  "costEstimated",
  "costMode",
];
const USAGE_SESSION_EXPORT_CSV_HEADERS = [
  "sessionId",
  "sourceId",
  "tool",
  "model",
  "startedAt",
  "inputTokens",
  "outputTokens",
  "cacheReadTokens",
  "cacheWriteTokens",
  "reasoningTokens",
  "totalTokens",
  "cost",
  "costRaw",
  "costEstimated",
  "costMode",
];
const USAGE_HEATMAP_EXPORT_CSV_HEADERS = ["date", "tokens", "cost", "sessions"];

interface SessionExportJobResult {
  items: Session[];
  total: number;
  nextCursor: string | null;
  filters: SessionSearchInput;
}

interface SessionExportJobRecord {
  job: SessionExportJob;
  requestId: string;
  tenantId: string;
  result?: SessionExportJobResult;
  csvContent?: string;
}

type UsageExportItems =
  | UsageDailyItem[]
  | UsageWeekItem[]
  | UsageMonthlyItem[]
  | UsageModelItem[]
  | UsageSessionBreakdownItem[]
  | HeatmapCell[];

async function requireAuthContext(c: Context<AppEnv>) {
  const authResult = await authMiddleware(c, async () => {});
  if (authResult instanceof Response) {
    return authResult;
  }

  const auth = c.get("auth");
  if (!auth) {
    return c.json({ message: "未认证：请先登录。" }, 401);
  }
  return auth;
}

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 export 审计日志失败。", error);
  }
}

function escapeCsvCell(value: unknown): string {
  const cell = value === null || value === undefined ? "" : String(value);
  if (!/[",\n\r]/.test(cell)) {
    return cell;
  }
  return `"${cell.replace(/"/g, "\"\"")}"`;
}

function buildSessionsCsv(items: Session[]): string {
  const rows = items.map((item) =>
    [
      item.id,
      item.sourceId,
      item.tool,
      item.model,
      item.startedAt,
      item.endedAt ?? "",
      item.tokens,
      item.cost,
    ]
      .map(escapeCsvCell)
      .join(",")
  );

  return [SESSION_EXPORT_CSV_HEADERS.join(","), ...rows].join("\n");
}

function buildCsvRows(headers: string[], rows: Array<Array<unknown>>): string {
  const bodyRows = rows.map((row) => row.map(escapeCsvCell).join(","));
  return [headers.join(","), ...bodyRows].join("\n");
}

function toUtcWeekStart(value: string): Date | null {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  const date = new Date(
    Date.UTC(parsed.getUTCFullYear(), parsed.getUTCMonth(), parsed.getUTCDate())
  );
  const weekday = date.getUTCDay();
  const diffToMonday = weekday === 0 ? -6 : 1 - weekday;
  date.setUTCDate(date.getUTCDate() + diffToMonday);
  return date;
}

function aggregateUsageWeeklyItems(
  items: UsageDailyItem[],
  limit?: number
): UsageWeekItem[] {
  const bucket = new Map<
    string,
    {
      weekStart: Date;
      tokens: number;
      cost: number;
      sessions: number;
    }
  >();

  for (const item of items) {
    const weekStart = toUtcWeekStart(item.date);
    if (!weekStart) {
      continue;
    }

    const key = weekStart.toISOString().slice(0, 10);
    const current = bucket.get(key);
    if (current) {
      current.tokens += item.tokens;
      current.cost += item.cost;
      current.sessions += item.sessions;
      continue;
    }

    bucket.set(key, {
      weekStart,
      tokens: item.tokens,
      cost: item.cost,
      sessions: item.sessions,
    });
  }

  const weeklyItems = Array.from(bucket.values())
    .sort((left, right) => left.weekStart.getTime() - right.weekStart.getTime())
    .map((item) => {
      const weekEnd = new Date(item.weekStart);
      weekEnd.setUTCDate(weekEnd.getUTCDate() + 6);
      weekEnd.setUTCHours(23, 59, 59, 999);
      return {
        weekStart: item.weekStart.toISOString(),
        weekEnd: weekEnd.toISOString(),
        tokens: item.tokens,
        cost: Number(item.cost.toFixed(6)),
        sessions: item.sessions,
      };
    });

  if (typeof limit === "number" && limit > 0 && weeklyItems.length > limit) {
    return weeklyItems.slice(weeklyItems.length - limit);
  }
  return weeklyItems;
}

function buildUsageCsv(
  dimension: UsageExportDimension,
  items: UsageExportItems
): string {
  switch (dimension) {
    case "daily":
      return buildCsvRows(
        USAGE_DAILY_EXPORT_CSV_HEADERS,
        (items as UsageDailyItem[]).map((item) => [
          item.date,
          item.tokens,
          item.cost,
          item.sessions,
          item.costRaw,
          item.costEstimated,
          item.costMode,
        ])
      );
    case "weekly":
      return buildCsvRows(
        USAGE_WEEKLY_EXPORT_CSV_HEADERS,
        (items as UsageWeekItem[]).map((item) => [
          item.weekStart,
          item.weekEnd,
          item.tokens,
          item.cost,
          item.sessions,
        ])
      );
    case "monthly":
      return buildCsvRows(
        USAGE_MONTHLY_EXPORT_CSV_HEADERS,
        (items as UsageMonthlyItem[]).map((item) => [
          item.month,
          item.tokens,
          item.cost,
          item.sessions,
          item.costRaw,
          item.costEstimated,
          item.costMode,
        ])
      );
    case "models":
      return buildCsvRows(
        USAGE_MODEL_EXPORT_CSV_HEADERS,
        (items as UsageModelItem[]).map((item) => [
          item.model,
          item.tokens,
          item.cost,
          item.sessions,
          item.costRaw,
          item.costEstimated,
          item.costMode,
        ])
      );
    case "sessions":
      return buildCsvRows(
        USAGE_SESSION_EXPORT_CSV_HEADERS,
        (items as UsageSessionBreakdownItem[]).map((item) => [
          item.sessionId,
          item.sourceId,
          item.tool,
          item.model,
          item.startedAt,
          item.inputTokens,
          item.outputTokens,
          item.cacheReadTokens,
          item.cacheWriteTokens,
          item.reasoningTokens,
          item.totalTokens,
          item.cost,
          item.costRaw,
          item.costEstimated,
          item.costMode,
        ])
      );
    case "heatmap":
      return buildCsvRows(
        USAGE_HEATMAP_EXPORT_CSV_HEADERS,
        (items as HeatmapCell[]).map((item) => [
          item.date,
          item.tokens,
          item.cost,
          item.sessions,
        ])
      );
  }
}

function buildUsageCsvFileName(dimension: UsageExportDimension): string {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `usage-${dimension}-${timestamp}.csv`;
}

function toSessionSearchInput(input: {
  sourceId?: string;
  keyword?: string;
  clientType?: string;
  tool?: string;
  host?: string;
  model?: string;
  project?: string;
  from?: string;
  to?: string;
  limit?: number;
  cursor?: string;
}): SessionSearchInput {
  return {
    sourceId: input.sourceId,
    keyword: input.keyword,
    clientType: input.clientType,
    tool: input.tool,
    host: input.host,
    model: input.model,
    project: input.project,
    from: input.from,
    to: input.to,
    limit: input.limit,
    cursor: input.cursor,
  };
}

async function listUsageExportItems(input: {
  tenantId: string;
  dimension: UsageExportDimension;
  from?: string;
  to?: string;
  limit?: number;
  timezone?: string;
}): Promise<UsageExportItems> {
  const baseQuery = {
    tenantId: input.tenantId,
    from: input.from,
    to: input.to,
    limit: input.limit,
  };

  switch (input.dimension) {
    case "daily":
      return repository.listUsageDaily(baseQuery);
    case "weekly":
      {
        const dailyItems = await repository.listUsageDaily({
          tenantId: input.tenantId,
          from: input.from,
          to: input.to,
        });
        return aggregateUsageWeeklyItems(dailyItems, input.limit);
      }
    case "monthly":
      return repository.listUsageMonthly(baseQuery);
    case "models":
      return repository.listUsageModelRanking(baseQuery);
    case "sessions":
      return repository.listUsageSessionBreakdown(baseQuery);
    case "heatmap":
      {
        const cells = await repository.listUsageHeatmap({
          tenantId: input.tenantId,
          from: input.from,
          to: input.to,
          timezone: input.timezone,
        });
        if (typeof input.limit === "number" && input.limit > 0 && cells.length > input.limit) {
          return cells.slice(cells.length - input.limit);
        }
        return cells;
      }
  }
}

function buildCsvFileName(): string {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `sessions-${timestamp}.csv`;
}

function buildJobCsvFileName(jobId: string): string {
  return `sessions-${jobId}.csv`;
}

function buildJobJsonFileName(jobId: string): string {
  return `sessions-${jobId}.json`;
}

function buildJobDownloadUrl(jobId: string): string {
  return `/api/v1/exports/sessions/jobs/${jobId}/download`;
}

function toSessionExportJobResponse(job: SessionExportJob): SessionExportJob & {
  downloadUrl: string | null;
} {
  return {
    ...job,
    downloadUrl: job.status === "completed" ? buildJobDownloadUrl(job.id) : null,
  };
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    const message = error.message.trim();
    return message.length > 0 ? message : "导出任务执行失败。";
  }
  if (typeof error === "string") {
    const message = error.trim();
    return message.length > 0 ? message : "导出任务执行失败。";
  }
  return "导出任务执行失败。";
}

async function runSessionExportJob(jobId: string): Promise<void> {
  const jobRecord = exportJobStore.get(jobId);
  if (!jobRecord) {
    return;
  }

  jobRecord.job.status = "running";
  jobRecord.job.startedAt = new Date().toISOString();

  try {
    const payload = await repository.searchSessions(jobRecord.job.filters, jobRecord.tenantId);

    jobRecord.result = {
      items: payload.items,
      total: payload.total,
      nextCursor: payload.nextCursor,
      filters: jobRecord.job.filters,
    };
    if (jobRecord.job.format === "csv") {
      jobRecord.csvContent = buildSessionsCsv(payload.items);
    }

    jobRecord.job.status = "completed";
    jobRecord.job.completedAt = new Date().toISOString();
    jobRecord.job.total = payload.total;
    jobRecord.job.count = payload.items.length;

    await appendAuditLogSafely({
      eventId: `cp:${jobRecord.requestId}:${jobId}`,
      action: "control_plane.export_completed",
      level: "info",
      detail: `Export job ${jobId} completed as ${jobRecord.job.format}.`,
      tenantId: jobRecord.tenantId,
      metadata: {
        requestId: jobRecord.requestId,
        jobId,
        format: jobRecord.job.format,
        total: payload.total,
        count: payload.items.length,
        filters: jobRecord.job.filters,
      },
    });
  } catch (error) {
    const errorMessage = toErrorMessage(error);
    jobRecord.job.status = "failed";
    jobRecord.job.failedAt = new Date().toISOString();
    jobRecord.job.error = errorMessage;

    await appendAuditLogSafely({
      eventId: `cp:${jobRecord.requestId}:${jobId}`,
      action: "control_plane.export_failed",
      level: "error",
      detail: `Export job ${jobId} failed: ${errorMessage}`,
      tenantId: jobRecord.tenantId,
      metadata: {
        requestId: jobRecord.requestId,
        jobId,
        format: jobRecord.job.format,
        filters: jobRecord.job.filters,
        error: errorMessage,
      },
    });
  }
}

exportRoutes.post("/exports/sessions/jobs", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateSessionExportJobCreateInput(body);
  if (!result.success) {
    return c.json(
      {
        message: result.error,
      },
      400
    );
  }

  const requestId = c.get("requestId");
  const jobId = crypto.randomUUID();
  const filters = toSessionSearchInput(result.data);
  const cursorResult = parseOptionalTimePaginationCursor(filters.cursor);
  if (!cursorResult.success) {
    return c.json({ message: cursorResult.error }, 400);
  }
  filters.cursor = cursorResult.cursor;
  const job: SessionExportJob = {
    id: jobId,
    status: "pending",
    format: result.data.format,
    filters,
    requestedAt: new Date().toISOString(),
  };

  exportJobStore.set(jobId, {
    job,
    requestId,
    tenantId: auth.tenantId,
  });

  await appendAuditLogSafely({
    eventId: `cp:${requestId}:${jobId}`,
    action: "control_plane.export_requested",
    level: "info",
    detail: `Requested export job ${jobId} as ${job.format}.`,
    tenantId: auth.tenantId,
    metadata: {
      requestId,
      jobId,
      format: job.format,
      filters,
    },
  });

  void runSessionExportJob(jobId);

  return c.json(toSessionExportJobResponse(job), 202);
});

exportRoutes.get("/exports/sessions/jobs/:id", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const idResult = validateExportJobId(c.req.param("id"));
  if (!idResult.success) {
    return c.json({ message: idResult.error }, 400);
  }

  const jobRecord = exportJobStore.get(idResult.data);
  if (!jobRecord) {
    return c.json({ message: "导出任务不存在。" }, 404);
  }
  if (jobRecord.tenantId !== auth.tenantId) {
    return c.json({ message: "无权访问该导出任务。" }, 403);
  }

  return c.json(toSessionExportJobResponse(jobRecord.job));
});

exportRoutes.get("/exports/sessions/jobs/:id/download", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const idResult = validateExportJobId(c.req.param("id"));
  if (!idResult.success) {
    return c.json({ message: idResult.error }, 400);
  }

  const jobRecord = exportJobStore.get(idResult.data);
  if (!jobRecord) {
    return c.json({ message: "导出任务不存在。" }, 404);
  }
  if (jobRecord.tenantId !== auth.tenantId) {
    return c.json({ message: "无权访问该导出任务。" }, 403);
  }

  if (jobRecord.job.status === "failed") {
    return c.json(
      {
        message: jobRecord.job.error ?? "导出任务失败，暂无可下载结果。",
      },
      409
    );
  }

  if (jobRecord.job.status !== "completed" || !jobRecord.result) {
    return c.json(
      {
        message: "导出任务尚未完成，请稍后重试。",
        status: jobRecord.job.status,
      },
      409
    );
  }

  if (jobRecord.job.format === "csv") {
    c.header("Content-Type", "text/csv; charset=utf-8");
    c.header("Content-Disposition", `attachment; filename="${buildJobCsvFileName(jobRecord.job.id)}"`);
    return c.body(jobRecord.csvContent ?? buildSessionsCsv(jobRecord.result.items));
  }

  c.header("Content-Type", "application/json; charset=utf-8");
  c.header(
    "Content-Disposition",
    `attachment; filename="${buildJobJsonFileName(jobRecord.job.id)}"`
  );
  return c.json(jobRecord.result);
});

exportRoutes.get("/exports/sessions", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateSessionExportQueryInput(c.req.query());
  if (!result.success) {
    return c.json(
      {
        message: result.error,
      },
      400
    );
  }

  const filters = toSessionSearchInput(result.data);
  const cursorResult = parseOptionalTimePaginationCursor(filters.cursor);
  if (!cursorResult.success) {
    return c.json({ message: cursorResult.error }, 400);
  }
  filters.cursor = cursorResult.cursor;
  const payload = await repository.searchSessions(filters, auth.tenantId);
  const requestId = c.get("requestId");

  await appendAuditLogSafely({
    eventId: `cp:${requestId}`,
    action: "control_plane.export_requested",
    level: "info",
    detail: `Exported sessions as ${result.data.format}.`,
    tenantId: auth.tenantId,
    metadata: {
      requestId,
      format: result.data.format,
      total: payload.total,
      count: payload.items.length,
      filters,
    },
  });

  if (result.data.format === "csv") {
    c.header("Content-Type", "text/csv; charset=utf-8");
    c.header("Content-Disposition", `attachment; filename="${buildCsvFileName()}"`);
    return c.body(buildSessionsCsv(payload.items));
  }

  return c.json({
    items: payload.items,
    total: payload.total,
    nextCursor: payload.nextCursor,
    filters,
  });
});

exportRoutes.get("/exports/usage", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateUsageExportQueryInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const filters = {
    dimension: result.data.dimension,
    from: result.data.from,
    to: result.data.to,
    limit: result.data.limit,
    timezone: result.data.timezone,
  };
  const items = await listUsageExportItems({
    tenantId: auth.tenantId,
    ...filters,
  });
  const requestId = c.get("requestId");

  await appendAuditLogSafely({
    eventId: `cp:${requestId}`,
    action: "control_plane.export_requested",
    level: "info",
    detail: `Exported usage(${filters.dimension}) as ${result.data.format}.`,
    tenantId: auth.tenantId,
    metadata: {
      requestId,
      target: "usage",
      format: result.data.format,
      total: items.length,
      dimension: filters.dimension,
      filters,
    },
  });

  if (result.data.format === "csv") {
    c.header("Content-Type", "text/csv; charset=utf-8");
    c.header(
      "Content-Disposition",
      `attachment; filename="${buildUsageCsvFileName(filters.dimension)}"`
    );
    return c.body(buildUsageCsv(filters.dimension, items));
  }

  return c.json({
    items,
    total: items.length,
    filters,
  });
});
