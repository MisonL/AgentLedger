import { Hono, type Context } from "hono";
import type { AuditItem, AuditListInput } from "../contracts";
import { validateAuditExportQueryInput, validateAuditListInput } from "../contracts";
import {
  getControlPlaneRepository,
  type AppendAuditLogInput,
} from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import { buildEvidenceBundle } from "../security/evidence-bundle";
import { parseOptionalTimePaginationCursor } from "./pagination-cursor";
import type { AppEnv } from "../types";

export const auditRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const EVIDENCE_EXPORT_MAX_PAGES = 1000;
const EVIDENCE_EXPORT_MAX_RECORDS = 50_000;

interface AuditQueryFilters extends AuditListInput {
  eventId?: string;
  action?: string;
  keyword?: string;
}

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 audits 访问审计失败。", error);
  }
}

async function appendAuditLogStrict(
  c: Context<AppEnv>,
  input: AppendAuditLogInput,
): Promise<Response | null> {
  try {
    await repository.appendAuditLog(input);
    return null;
  } catch (error) {
    console.error("[control-plane] 写入 audits 访问审计失败（严格模式）。", error);
    return c.json({ message: "审计写入失败，请稍后重试。" }, 500);
  }
}

async function listAllAuditsForEvidence(
  tenantId: string,
  filters: AuditQueryFilters,
): Promise<{ items: AuditItem[]; total: number; pageCount: number }> {
  const pageLimit = Math.max(1, Math.trunc(filters.limit ?? 200));
  const items: AuditItem[] = [];
  let total = 0;
  let pageCount = 0;
  let cursor = filters.cursor;

  while (true) {
    if (pageCount >= EVIDENCE_EXPORT_MAX_PAGES) {
      throw new Error(`审计导出分页超过上限（${EVIDENCE_EXPORT_MAX_PAGES} 页）。`);
    }
    const payload = await repository.listAudits(
      {
        ...filters,
        limit: pageLimit,
        cursor,
      },
      tenantId
    );
    if (pageCount === 0) {
      total = payload.total;
    }
    items.push(...payload.items);
    pageCount += 1;

    if (items.length > EVIDENCE_EXPORT_MAX_RECORDS) {
      throw new Error(`审计导出记录数超过上限（${EVIDENCE_EXPORT_MAX_RECORDS} 条）。`);
    }
    if (!payload.nextCursor) {
      break;
    }
    cursor = payload.nextCursor;
  }

  return {
    items,
    total,
    pageCount,
  };
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

function normalizeOptionalQuery(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function escapeCsvCell(value: unknown): string {
  const cell = value === undefined || value === null ? "" : String(value);
  if (!/[",\n\r]/.test(cell)) {
    return cell;
  }
  return `"${cell.replace(/"/g, "\"\"")}"`;
}

function buildAuditsCsv(items: AuditItem[]): string {
  const headers = [
    "id",
    "eventId",
    "action",
    "level",
    "detail",
    "createdAt",
    "metadata",
  ];
  const rows = items.map((item) =>
    [
      item.id,
      item.eventId,
      item.action,
      item.level,
      item.detail,
      item.createdAt,
      JSON.stringify(item.metadata),
    ]
      .map(escapeCsvCell)
      .join(",")
  );
  return [headers.join(","), ...rows].join("\n");
}

function buildAuditExportFileName(format: "json" | "csv"): string {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `audits-${timestamp}.${format}`;
}

function buildAuditEvidenceBundleFileName(): string {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `audit-evidence-bundle-${timestamp}.json`;
}

auditRoutes.get("/audits", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const query = c.req.query();
  const result = validateAuditListInput(query);

  if (!result.success) {
    return c.json(
      {
        message: result.error,
      },
      400
    );
  }

  const eventId = normalizeOptionalQuery(query.eventId);
  const actionFilter = normalizeOptionalQuery(query.action);
  const keyword = normalizeOptionalQuery(query.keyword);

  if (query.eventId !== undefined && !eventId) {
    return c.json({ message: "eventId 必须为非空字符串。" }, 400);
  }
  if (query.action !== undefined && !actionFilter) {
    return c.json({ message: "action 必须为非空字符串。" }, 400);
  }
  if (query.keyword !== undefined && !keyword) {
    return c.json({ message: "keyword 必须为非空字符串。" }, 400);
  }
  const cursorResult = parseOptionalTimePaginationCursor(result.data.cursor);
  if (!cursorResult.success) {
    return c.json({ message: cursorResult.error }, 400);
  }

  const filters: AuditQueryFilters = {
    ...result.data,
    eventId,
    action: actionFilter,
    keyword,
    cursor: cursorResult.cursor,
  };
  const payload = await repository.listAudits(filters, auth.tenantId);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:audit-query`,
    action: "audit.query",
    level: "info",
    detail: "查询审计日志。",
    metadata: {
      tenantId: auth.tenantId,
      userId: auth.userId,
      requestId,
      route: "/api/v1/audits",
      eventId: filters.eventId,
      actionFilter: filters.action,
      levelFilter: filters.level,
      keyword: filters.keyword,
      from: filters.from,
      to: filters.to,
      limit: filters.limit,
      cursor: filters.cursor,
      resultCount: payload.items.length,
      resultTotal: payload.total,
      nextCursor: payload.nextCursor,
    },
  });

  return c.json({
    items: payload.items,
    total: payload.total,
    filters,
    nextCursor: payload.nextCursor,
  });
});

auditRoutes.get("/audits/export", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const query = c.req.query();
  const result = validateAuditExportQueryInput(query);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const cursorResult = parseOptionalTimePaginationCursor(result.data.cursor);
  if (!cursorResult.success) {
    return c.json({ message: cursorResult.error }, 400);
  }

  const filters: AuditQueryFilters = {
    level: result.data.level,
    from: result.data.from,
    to: result.data.to,
    limit: result.data.limit,
    cursor: cursorResult.cursor,
    eventId: result.data.eventId,
    action: result.data.action,
    keyword: result.data.keyword,
  };
  const payload = await repository.listAudits(filters, auth.tenantId);
  const requestId = c.get("requestId");

  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:audit-export`,
    action: "audit.export",
    level: "info",
    detail: `导出审计日志（${result.data.format}）。`,
    metadata: {
      tenantId: auth.tenantId,
      userId: auth.userId,
      requestId,
      route: "/api/v1/audits/export",
      format: result.data.format,
      eventId: filters.eventId,
      actionFilter: filters.action,
      levelFilter: filters.level,
      keyword: filters.keyword,
      from: filters.from,
      to: filters.to,
      limit: filters.limit,
      cursor: filters.cursor,
      resultCount: payload.items.length,
      resultTotal: payload.total,
      nextCursor: payload.nextCursor,
    },
  });

  if (result.data.format === "csv") {
    c.header("content-type", "text/csv; charset=utf-8");
    c.header(
      "content-disposition",
      `attachment; filename="${buildAuditExportFileName("csv")}"`
    );
    return c.body(buildAuditsCsv(payload.items));
  }

  c.header(
    "content-disposition",
    `attachment; filename="${buildAuditExportFileName("json")}"`
  );
  return c.json({
    format: "json",
    exportedAt: new Date().toISOString(),
    items: payload.items,
    total: payload.total,
    filters,
    nextCursor: payload.nextCursor,
  });
});

auditRoutes.get("/audits/evidence-bundle", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const signingKey = Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY?.trim();
  if (!signingKey) {
    return c.json(
      {
        message: "服务端未配置 EVIDENCE_BUNDLE_SIGNING_KEY。",
      },
      500
    );
  }

  const query = c.req.query();
  const result = validateAuditListInput(query);
  if (!result.success) {
    return c.json(
      {
        message: result.error,
      },
      400
    );
  }

  const eventId = normalizeOptionalQuery(query.eventId);
  const actionFilter = normalizeOptionalQuery(query.action);
  const keyword = normalizeOptionalQuery(query.keyword);

  if (query.eventId !== undefined && !eventId) {
    return c.json({ message: "eventId 必须为非空字符串。" }, 400);
  }
  if (query.action !== undefined && !actionFilter) {
    return c.json({ message: "action 必须为非空字符串。" }, 400);
  }
  if (query.keyword !== undefined && !keyword) {
    return c.json({ message: "keyword 必须为非空字符串。" }, 400);
  }
  const cursorResult = parseOptionalTimePaginationCursor(result.data.cursor);
  if (!cursorResult.success) {
    return c.json({ message: cursorResult.error }, 400);
  }

  const filters: AuditQueryFilters = {
    ...result.data,
    eventId,
    action: actionFilter,
    keyword,
    cursor: cursorResult.cursor,
  };
  let payload: { items: AuditItem[]; total: number; pageCount: number };
  try {
    payload = await listAllAuditsForEvidence(auth.tenantId, filters);
  } catch (error) {
    const message =
      error instanceof Error && error.message.trim().length > 0
        ? error.message
        : "导出审计取证包失败。";
    return c.json({ message }, 422);
  }
  const exportedAt = new Date().toISOString();
  const bundle = buildEvidenceBundle({
    tenantId: auth.tenantId,
    generatedBy: {
      userId: auth.userId,
      email: auth.email,
    },
    filters,
    audits: payload.items,
    signingKey,
    generatedAt: exportedAt,
  });

  const requestId = c.get("requestId");
  const strictAuditResult = await appendAuditLogStrict(c, {
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:audit-evidence-bundle`,
    action: "audit.evidence_bundle.export",
    level: "info",
    detail: "导出审计取证包。",
    metadata: {
      tenantId: auth.tenantId,
      userId: auth.userId,
      requestId,
      route: "/api/v1/audits/evidence-bundle",
      exportedAt,
      levelFilter: filters.level,
      from: filters.from,
      to: filters.to,
      requestedLimit: filters.limit,
      cursor: filters.cursor,
      eventId: filters.eventId,
      actionFilter: filters.action,
      keyword: filters.keyword,
      pageCount: payload.pageCount,
      resultTotal: payload.total,
      recordCount: bundle.records.length,
      rootHash: bundle.rootHash,
    },
  });
  if (strictAuditResult) {
    return strictAuditResult;
  }

  c.header(
    "content-disposition",
    `attachment; filename="${buildAuditEvidenceBundleFileName()}"`
  );
  return c.json(bundle);
});
