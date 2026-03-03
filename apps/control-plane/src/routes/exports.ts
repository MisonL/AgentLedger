import { Hono, type Context } from "hono";
import type {
  Session,
  SessionExportJob,
  SessionSearchInput,
} from "../contracts";
import {
  validateExportJobId,
  validateSessionExportJobCreateInput,
  validateSessionExportQueryInput,
} from "../contracts";
import type { AppendAuditLogInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
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

interface SessionExportJobResult {
  items: Session[];
  total: number;
  nextCursor: null;
  filters: SessionSearchInput;
}

interface SessionExportJobRecord {
  job: SessionExportJob;
  requestId: string;
  tenantId: string;
  result?: SessionExportJobResult;
  csvContent?: string;
}

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
  };
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
      nextCursor: null,
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
    nextCursor: null,
    filters,
  });
});
