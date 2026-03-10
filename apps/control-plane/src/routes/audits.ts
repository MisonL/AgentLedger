import { Hono, type Context } from "hono";
import type {
  AuditItem,
  AuditListInput,
  LegalHoldItem,
  LegalHoldListInput,
  LegalHoldResourceType,
} from "../contracts";
import {
  validateAuditExportQueryInput,
  validateAuditListInput,
  validateLegalHoldCreateInput,
  validateLegalHoldListInput,
  validateLegalHoldReleaseInput,
} from "../contracts";
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
const AUDIT_DLP_MODE_ENV = "AUDIT_DLP_MODE";
const AUDIT_DLP_EXTRA_PATTERNS_JSON_ENV = "AUDIT_DLP_EXTRA_PATTERNS_JSON";

const AUDIT_DLP_EMAIL_PATTERN = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
const AUDIT_DLP_BEARER_PATTERN = /Bearer\s+[A-Za-z0-9._\-+/=]+/gi;
const AUDIT_DLP_BASIC_PATTERN = /\bBasic\s+[A-Za-z0-9+/=]{8,}\b/gi;
const AUDIT_DLP_JWT_PATTERN =
  /\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g;
const AUDIT_DLP_KEY_PATTERN = /\b(?:sk|pk|ak)_[A-Za-z0-9]{6,}\b/g;
const AUDIT_DLP_OPENAI_KEY_PATTERN =
  /\bsk-(?:proj|svcacct)-[A-Za-z0-9_-]{10,}\b/g;
const AUDIT_DLP_OPENAI_LEGACY_KEY_PATTERN = /\bsk-[A-Za-z0-9]{20,}\b/g;
const AUDIT_DLP_ANTHROPIC_KEY_PATTERN =
  /\bsk-ant-(?:api\d{2}-)?[A-Za-z0-9_-]{10,}\b/g;
const AUDIT_DLP_AWS_ACCESS_KEY_ID_PATTERN =
  /\bA(?:KIA|SIA|GPA|IDA|ROA|NPA)[0-9A-Z]{16}\b/g;
const AUDIT_DLP_ALIYUN_ACCESS_KEY_ID_PATTERN = /\bLTAI[A-Za-z0-9]{12,}\b/g;
const AUDIT_DLP_TENCENT_SECRET_ID_PATTERN = /\bAKID[A-Za-z0-9]{13,64}\b/g;
const AUDIT_DLP_SECRET_KV_PATTERN =
  /\b(?:secret|token|password|api[-_]?key|accesskeysecret|secretaccesskey|secretkey|access_key_secret|secret_access_key|secret_id|secretid)\b\s*[:=]\s*["']?[^"'\s,}]+["']?/gi;
const AUDIT_DLP_EXTRA_REDACTED_PLACEHOLDER = "[REDACTED_DLP]";

let cachedExtraDlpPatterns: { raw: string | undefined; patterns: RegExp[] } | null =
  null;

interface AuditQueryFilters extends AuditListInput {
  eventId?: string;
  action?: string;
  keyword?: string;
}

type AuditDlpMode = "off" | "redact" | "block";

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

function resolveAuditDlpMode(raw: string | undefined): AuditDlpMode {
  const normalized = normalizeOptionalQuery(raw)?.toLowerCase();
  if (normalized === "redact" || normalized === "block" || normalized === "off") {
    return normalized;
  }
  const envMode = normalizeOptionalQuery(Bun.env[AUDIT_DLP_MODE_ENV])?.toLowerCase();
  return envMode === "redact" || envMode === "block" ? envMode : "off";
}

function compileExtraDlpPattern(source: string): RegExp | null {
  const trimmed = source.trim();
  if (!trimmed) {
    return null;
  }

  try {
    if (trimmed.startsWith("/") && trimmed.length > 2) {
      const lastSlashIndex = trimmed.lastIndexOf("/");
      if (lastSlashIndex > 0) {
        const body = trimmed.slice(1, lastSlashIndex);
        const rawFlags = trimmed.slice(lastSlashIndex + 1);
        const flags = new Set<string>();
        for (const flag of rawFlags) {
          if (flag === "g" || flag === "i" || flag === "m" || flag === "s" || flag === "u" || flag === "y") {
            flags.add(flag);
          }
        }
        flags.add("g");
        return new RegExp(body, Array.from(flags).join(""));
      }
    }
    return new RegExp(trimmed, "gi");
  } catch (error) {
    console.warn(
      "[control-plane] AUDIT_DLP_EXTRA_PATTERNS_JSON 存在非法正则，已忽略。",
      error,
    );
    return null;
  }
}

function loadAuditDlpExtraPatterns(): RegExp[] {
  const raw = normalizeOptionalQuery(Bun.env[AUDIT_DLP_EXTRA_PATTERNS_JSON_ENV]);
  if (cachedExtraDlpPatterns && cachedExtraDlpPatterns.raw === raw) {
    return cachedExtraDlpPatterns.patterns;
  }

  const patterns: RegExp[] = [];
  if (!raw) {
    cachedExtraDlpPatterns = { raw, patterns };
    return patterns;
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) {
      console.warn(
        "[control-plane] AUDIT_DLP_EXTRA_PATTERNS_JSON 必须是 JSON 数组，已忽略。",
      );
      cachedExtraDlpPatterns = { raw, patterns };
      return patterns;
    }

    for (const item of parsed) {
      if (typeof item !== "string") {
        continue;
      }
      const compiled = compileExtraDlpPattern(item);
      if (compiled) {
        patterns.push(compiled);
      }
    }
  } catch (error) {
    console.warn(
      "[control-plane] 解析 AUDIT_DLP_EXTRA_PATTERNS_JSON 失败，已忽略。",
      error,
    );
  }

  cachedExtraDlpPatterns = { raw, patterns };
  return patterns;
}

function redactSensitiveText(value: string, extraPatterns: RegExp[]): string {
  let redacted = value
    .replace(AUDIT_DLP_EMAIL_PATTERN, "[REDACTED_EMAIL]")
    .replace(AUDIT_DLP_BEARER_PATTERN, "Bearer [REDACTED_TOKEN]")
    .replace(AUDIT_DLP_BASIC_PATTERN, "Basic [REDACTED_TOKEN]")
    .replace(AUDIT_DLP_JWT_PATTERN, "[REDACTED_TOKEN]")
    .replace(AUDIT_DLP_OPENAI_KEY_PATTERN, "[REDACTED_KEY]")
    .replace(AUDIT_DLP_OPENAI_LEGACY_KEY_PATTERN, "[REDACTED_KEY]")
    .replace(AUDIT_DLP_ANTHROPIC_KEY_PATTERN, "[REDACTED_KEY]")
    .replace(AUDIT_DLP_AWS_ACCESS_KEY_ID_PATTERN, "[REDACTED_KEY]")
    .replace(AUDIT_DLP_ALIYUN_ACCESS_KEY_ID_PATTERN, "[REDACTED_KEY]")
    .replace(AUDIT_DLP_TENCENT_SECRET_ID_PATTERN, "[REDACTED_KEY]")
    .replace(AUDIT_DLP_KEY_PATTERN, "[REDACTED_KEY]")
    .replace(AUDIT_DLP_SECRET_KV_PATTERN, "[REDACTED_SECRET]");

  for (const pattern of extraPatterns) {
    redacted = redacted.replace(pattern, AUDIT_DLP_EXTRA_REDACTED_PLACEHOLDER);
  }

  return redacted;
}

function containsSensitiveText(value: string, extraPatterns: RegExp[]): boolean {
  return redactSensitiveText(value, extraPatterns) !== value;
}

function applyAuditDlp(items: AuditItem[], mode: AuditDlpMode, extraPatterns: RegExp[]): {
  items: AuditItem[];
  matched: boolean;
  redacted: number;
} {
  let matched = false;
  let redacted = 0;
  const sanitized = items.map((item) => {
    const detail = containsSensitiveText(item.detail, extraPatterns)
      ? redactSensitiveText(item.detail, extraPatterns)
      : item.detail;
    const metadataJson = JSON.stringify(item.metadata);
    const nextMetadataJson = containsSensitiveText(metadataJson, extraPatterns)
      ? redactSensitiveText(metadataJson, extraPatterns)
      : metadataJson;
    const nextMetadata =
      nextMetadataJson === metadataJson
        ? item.metadata
        : (JSON.parse(nextMetadataJson) as Record<string, unknown>);
    const itemMatched = detail !== item.detail || nextMetadataJson !== metadataJson;
    if (itemMatched) {
      matched = true;
      redacted += 1;
    }
    if (mode !== "redact" || !itemMatched) {
      return item;
    }
    return {
      ...item,
      detail,
      metadata: nextMetadata,
    };
  });
  return {
    items: sanitized,
    matched,
    redacted,
  };
}

function buildAuditEvidenceBundleFileName(): string {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `audit-evidence-bundle-${timestamp}.json`;
}

function toLegalHoldMetadata(hold: LegalHoldItem | null) {
  if (!hold) {
    return null;
  }
  return {
    id: hold.id,
    resourceType: hold.resourceType,
    resourceId: hold.resourceId,
    reason: hold.reason,
    createdAt: hold.createdAt,
    createdByUserId: hold.createdByUserId,
    createdByEmail: hold.createdByEmail,
    releasedAt: hold.releasedAt,
    releasedByUserId: hold.releasedByUserId,
    releasedByEmail: hold.releasedByEmail,
    releaseReason: hold.releaseReason,
  };
}

function buildProtectedResourceMetadata(
  resourceType: LegalHoldResourceType,
  resourceId: string | undefined,
  hold: LegalHoldItem | null,
) {
  return {
    resourceType,
    resourceId: resourceId ?? null,
    legalHold: toLegalHoldMetadata(hold),
  };
}

function applyProtectedResourceHeaders(
  c: Context<AppEnv>,
  metadata: ReturnType<typeof buildProtectedResourceMetadata>,
) {
  c.header("x-agentledger-resource-type", metadata.resourceType);
  c.header("x-agentledger-legal-hold-status", metadata.legalHold ? "active" : "none");
  if (metadata.resourceId) {
    c.header("x-agentledger-resource-id", metadata.resourceId);
  }
  if (metadata.legalHold?.id) {
    c.header("x-agentledger-legal-hold-id", metadata.legalHold.id);
  }
}

async function respondHeldMutationConflict(
  c: Context<AppEnv>,
  auth: Awaited<ReturnType<typeof requireAuthContext>>,
  input: {
    resourceType: LegalHoldResourceType;
    resourceId: string;
    action: string;
    detail: string;
    message: string;
    route: string;
    metadata?: Record<string, unknown>;
  },
): Promise<Response | null> {
  if (auth instanceof Response) {
    return auth;
  }

  const hold = await repository.getActiveLegalHoldByResource(
    auth.tenantId,
    input.resourceType,
    input.resourceId,
  );
  if (!hold) {
    return null;
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:${input.action}`,
    action: input.action,
    level: "warning",
    detail: input.detail,
    metadata: {
      tenantId: auth.tenantId,
      userId: auth.userId,
      requestId,
      route: input.route,
      resourceType: input.resourceType,
      resourceId: input.resourceId,
      legalHoldId: hold.id,
      legalHoldReason: hold.reason,
      ...input.metadata,
    },
  });

  return c.json(
    {
      message: input.message,
      legalHold: toLegalHoldMetadata(hold),
    },
    409,
  );
}

auditRoutes.post("/audits/legal-holds", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateLegalHoldCreateInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  if (result.data.resourceType === "audit") {
    const audit = await repository.getAuditById(auth.tenantId, result.data.resourceId);
    if (!audit) {
      return c.json({ message: `审计记录不存在：${result.data.resourceId}` }, 404);
    }
  }

  const existingHold = await repository.getActiveLegalHoldByResource(
    auth.tenantId,
    result.data.resourceType,
    result.data.resourceId,
  );
  if (existingHold) {
    const requestId = c.get("requestId");
    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:${requestId}:audit-legal-hold-create-blocked`,
      action: "audit.legal_hold.create_blocked",
      level: "warning",
      detail: "Legal Hold 已存在，拒绝重复创建。",
      metadata: {
        tenantId: auth.tenantId,
        userId: auth.userId,
        requestId,
        route: "/api/v1/audits/legal-holds",
        resourceType: result.data.resourceType,
        resourceId: result.data.resourceId,
        legalHoldId: existingHold.id,
      },
    });
    return c.json(
      {
        message: "该资源已有生效中的 Legal Hold。",
        legalHold: toLegalHoldMetadata(existingHold),
      },
      409,
    );
  }

  const hold = await repository.createLegalHold(auth.tenantId, result.data, {
    createdByUserId: auth.userId,
    createdByEmail: auth.email,
  });
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:audit-legal-hold-create`,
    action: "audit.legal_hold.create",
    level: "info",
    detail: "创建 Legal Hold。",
    metadata: {
      tenantId: auth.tenantId,
      userId: auth.userId,
      requestId,
      route: "/api/v1/audits/legal-holds",
      legalHoldId: hold.id,
      resourceType: hold.resourceType,
      resourceId: hold.resourceId,
      reason: hold.reason,
    },
  });

  return c.json(hold, 201);
});

auditRoutes.get("/audits/legal-holds", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const query = c.req.query();
  const result = validateLegalHoldListInput(query);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const payload = await repository.listLegalHolds(auth.tenantId, result.data);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:audit-legal-hold-query`,
    action: "audit.legal_hold.query",
    level: "info",
    detail: "查询 Legal Hold 列表。",
    metadata: {
      tenantId: auth.tenantId,
      userId: auth.userId,
      requestId,
      route: "/api/v1/audits/legal-holds",
      resourceType: result.data.resourceType,
      resourceId: result.data.resourceId,
      active: result.data.active,
      limit: result.data.limit,
      resultCount: payload.items.length,
      resultTotal: payload.total,
    },
  });

  const filters: LegalHoldListInput = {
    resourceType: result.data.resourceType,
    resourceId: result.data.resourceId,
    active: result.data.active,
    limit: result.data.limit,
  };

  return c.json({
    items: payload.items,
    total: payload.total,
    filters,
  });
});

auditRoutes.post("/audits/legal-holds/:id/release", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const holdId = normalizeOptionalQuery(c.req.param("id"));
  if (!holdId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateLegalHoldReleaseInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const current = await repository.getLegalHoldById(auth.tenantId, holdId);
  if (!current) {
    return c.json({ message: `Legal Hold 不存在：${holdId}` }, 404);
  }
  if (current.releasedAt) {
    return c.json(
      {
        message: `Legal Hold ${holdId} 已释放，不能重复操作。`,
        legalHold: toLegalHoldMetadata(current),
      },
      409,
    );
  }

  const released = await repository.releaseLegalHold(auth.tenantId, holdId, result.data, {
    releasedByUserId: auth.userId,
    releasedByEmail: auth.email,
  });
  if (!released) {
    return c.json({ message: `Legal Hold 不存在：${holdId}` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:audit-legal-hold-release`,
    action: "audit.legal_hold.release",
    level: "info",
    detail: "释放 Legal Hold。",
    metadata: {
      tenantId: auth.tenantId,
      userId: auth.userId,
      requestId,
      route: `/api/v1/audits/legal-holds/${holdId}/release`,
      legalHoldId: released.id,
      resourceType: released.resourceType,
      resourceId: released.resourceId,
      releaseReason: released.releaseReason,
    },
  });

  return c.json(released);
});

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
  const resourceId = result.data.resourceId;
  const heldConflict = await respondHeldMutationConflict(c, auth, {
    resourceType: "audit_export",
    resourceId: resourceId ?? "",
    action: "audit.export_blocked",
    detail: "审计导出目标资源存在 Legal Hold，拒绝覆盖。",
    message: "该 audit export 资源处于 Legal Hold 状态，禁止覆盖。",
    route: "/api/v1/audits/export",
    metadata: {
      format: result.data.format,
    },
  });
  if (resourceId && heldConflict) {
    return heldConflict;
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
  const dlpMode = resolveAuditDlpMode(query.dlpMode);
  const dlpResult = applyAuditDlp(payload.items, dlpMode, loadAuditDlpExtraPatterns());
  if (dlpMode === "block" && dlpResult.matched) {
    return c.json({ message: "审计导出命中 DLP 规则，已阻止导出。" }, 422);
  }
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
      resourceId,
      dlpMode,
      dlpMatched: dlpResult.matched,
      dlpRedacted: dlpResult.redacted,
      resultCount: payload.items.length,
      resultTotal: payload.total,
      nextCursor: payload.nextCursor,
    },
  });

  const hold = resourceId
    ? await repository.getActiveLegalHoldByResource(
        auth.tenantId,
        "audit_export",
        resourceId,
      )
    : null;
  const protectedResource = buildProtectedResourceMetadata(
    "audit_export",
    resourceId,
    hold,
  );
  applyProtectedResourceHeaders(c, protectedResource);
  c.header("x-agentledger-dlp-mode", dlpMode);
  c.header("x-agentledger-dlp-matched", dlpResult.matched ? "true" : "false");

  if (result.data.format === "csv") {
    c.header("content-type", "text/csv; charset=utf-8");
    c.header(
      "content-disposition",
      `attachment; filename="${buildAuditExportFileName("csv")}"`
    );
    return c.body(buildAuditsCsv(dlpResult.items));
  }

  c.header(
    "content-disposition",
    `attachment; filename="${buildAuditExportFileName("json")}"`
  );
  return c.json({
    format: "json",
    exportedAt: new Date().toISOString(),
    items: dlpResult.items,
    total: payload.total,
    filters,
    nextCursor: payload.nextCursor,
    targetResource: protectedResource,
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
  const resourceId = normalizeOptionalQuery(query.resourceId);

  if (query.eventId !== undefined && !eventId) {
    return c.json({ message: "eventId 必须为非空字符串。" }, 400);
  }
  if (query.action !== undefined && !actionFilter) {
    return c.json({ message: "action 必须为非空字符串。" }, 400);
  }
  if (query.keyword !== undefined && !keyword) {
    return c.json({ message: "keyword 必须为非空字符串。" }, 400);
  }
  if (query.resourceId !== undefined && !resourceId) {
    return c.json({ message: "resourceId 必须为非空字符串。" }, 400);
  }
  const cursorResult = parseOptionalTimePaginationCursor(result.data.cursor);
  if (!cursorResult.success) {
    return c.json({ message: cursorResult.error }, 400);
  }
  const heldConflict = await respondHeldMutationConflict(c, auth, {
    resourceType: "evidence_bundle",
    resourceId: resourceId ?? "",
    action: "audit.evidence_bundle.export_blocked",
    detail: "审计取证包目标资源存在 Legal Hold，拒绝覆盖。",
    message: "该 evidence bundle 资源处于 Legal Hold 状态，禁止覆盖。",
    route: "/api/v1/audits/evidence-bundle",
  });
  if (resourceId && heldConflict) {
    return heldConflict;
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
  const dlpMode = resolveAuditDlpMode(query.dlpMode);
  const dlpResult = applyAuditDlp(payload.items, dlpMode, loadAuditDlpExtraPatterns());
  if (dlpMode === "block" && dlpResult.matched) {
    return c.json({ message: "审计取证包命中 DLP 规则，已阻止导出。" }, 422);
  }
  const bundle = buildEvidenceBundle({
    tenantId: auth.tenantId,
    generatedBy: {
      userId: auth.userId,
      email: auth.email,
    },
    filters,
    audits: dlpResult.items,
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
      resourceId,
      dlpMode,
      dlpMatched: dlpResult.matched,
      dlpRedacted: dlpResult.redacted,
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
  const protectedResource = buildProtectedResourceMetadata(
    "evidence_bundle",
    resourceId,
    resourceId
      ? await repository.getActiveLegalHoldByResource(
          auth.tenantId,
          "evidence_bundle",
          resourceId,
        )
      : null,
  );
  applyProtectedResourceHeaders(c, protectedResource);
  c.header("x-agentledger-dlp-mode", dlpMode);
  c.header("x-agentledger-dlp-matched", dlpResult.matched ? "true" : "false");
  return c.json({
    ...bundle,
    targetResource: protectedResource,
  });
});

auditRoutes.delete("/audits/:id", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const auditId = normalizeOptionalQuery(c.req.param("id"));
  if (!auditId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const heldConflict = await respondHeldMutationConflict(c, auth, {
    resourceType: "audit",
    resourceId: auditId,
    action: "audit.delete_blocked",
    detail: "审计记录存在 Legal Hold，拒绝删除。",
    message: "该审计记录处于 Legal Hold 状态，禁止删除。",
    route: `/api/v1/audits/${auditId}`,
    metadata: {
      auditId,
    },
  });
  if (heldConflict) {
    return heldConflict;
  }

  const deleted = await repository.deleteAudit(auth.tenantId, auditId);
  if (!deleted) {
    return c.json({ message: `审计记录不存在：${auditId}` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:audit-delete`,
    action: "audit.delete",
    level: "warning",
    detail: "删除审计记录。",
    metadata: {
      tenantId: auth.tenantId,
      userId: auth.userId,
      requestId,
      route: `/api/v1/audits/${auditId}`,
      auditId,
    },
  });

  return c.body(null, 204);
});
