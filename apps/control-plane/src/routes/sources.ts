import { Hono, type Context } from "hono";
import { access, constants as fsConstants } from "node:fs/promises";
import { Socket } from "node:net";
import { isAbsolute } from "node:path";
import type {
  CreateSourceInput,
  Source,
  SourceAccessMode,
  SourceHealth,
  SyncJob,
} from "../contracts";
import {
  validateCreateSourceInput,
  validateSourceRegionBackfillInput,
  validateUpdateSourceInput,
} from "../contracts";
import type { AppendAuditLogInput, SourceParseFailureQueryInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const sourceRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const SOURCE_SYNC_JOB_ACTION = "control_plane.source_sync_job_created";
const SOURCE_SYNC_JOB_CANCEL_ACTION = "control_plane.source_sync_job_cancel_requested";
const SOURCE_CONNECTION_TESTED_ACTION = "control_plane.source_connection_tested";
const SOURCE_UPDATED_ACTION = "control_plane.source_updated";
const SOURCE_REGION_BACKFILL_ACTION = "control_plane.source_region_backfill_executed";
const DEFAULT_SYNC_JOB_LIMIT = 20;
const MAX_SYNC_JOB_LIMIT = 200;
const DEFAULT_PARSE_FAILURE_LIMIT = 50;
const MAX_PARSE_FAILURE_LIMIT = 500;
const DEFAULT_SOURCE_TEST_CONNECTION_TIMEOUT_MS = 1200;
const MIN_SOURCE_TEST_CONNECTION_TIMEOUT_MS = 100;
const MAX_SOURCE_TEST_CONNECTION_TIMEOUT_MS = 30000;

interface SourceConnectionTestResponse {
  sourceId: string;
  success: boolean;
  mode: Source["type"];
  latencyMs: number;
  detail: string;
  errorCode?: string;
}

interface SourceConnectionCheckResult {
  success: boolean;
  detail: string;
  errorCode?: string;
}

interface SshConnectionEndpoint {
  host: string;
  port: number;
}

type ResolveSshEndpointResult =
  | { success: true; endpoint: SshConnectionEndpoint }
  | { success: false; detail: string; errorCode: string };

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 source 审计日志失败。", error);
  }
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

function normalizeSourceId(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeSyncJobId(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

async function findSourceById(
  tenantId: string,
  sourceId: string
): Promise<Source | undefined> {
  const items = await repository.listSources(tenantId);
  return items.find((item) => item.id === sourceId);
}

function parseSyncJobLimit(value: string | undefined): { success: true; limit: number } | { success: false; error: string } {
  if (value === undefined) {
    return { success: true, limit: DEFAULT_SYNC_JOB_LIMIT };
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return { success: false, error: "limit 必须是 1 到 200 的整数。" };
  }

  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_SYNC_JOB_LIMIT) {
    return { success: false, error: "limit 必须是 1 到 200 的整数。" };
  }

  return { success: true, limit: parsed };
}

function parseSyncJobMode(value: unknown): SourceAccessMode | undefined {
  if (value === "realtime" || value === "sync" || value === "hybrid") {
    return value;
  }
  return undefined;
}

function parseSyncJobNextRunAt(
  value: unknown
): { success: true; value: string | undefined } | { success: false; error: string } {
  if (value === undefined || value === null) {
    return { success: true, value: undefined };
  }
  if (typeof value !== "string") {
    return { success: false, error: "nextRunAt 必须为 ISO 日期字符串。" };
  }

  const trimmed = value.trim();
  if (!trimmed || Number.isNaN(Date.parse(trimmed))) {
    return { success: false, error: "nextRunAt 必须为 ISO 日期字符串。" };
  }

  return { success: true, value: new Date(trimmed).toISOString() };
}

function parseSourceParseFailureQuery(
  query: Record<string, string | undefined>
): { success: true; data: SourceParseFailureQueryInput } | { success: false; error: string } {
  const normalizeOptional = (value: string | undefined): string | undefined => {
    if (value === undefined) {
      return undefined;
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : undefined;
  };

  const from = normalizeOptional(query.from);
  const to = normalizeOptional(query.to);
  const parserKey = normalizeOptional(query.parserKey);
  const errorCode = normalizeOptional(query.errorCode);
  const rawLimit = query.limit;

  if (query.from !== undefined && !from) {
    return { success: false, error: "from 必须为 ISO 日期字符串。" };
  }
  if (query.to !== undefined && !to) {
    return { success: false, error: "to 必须为 ISO 日期字符串。" };
  }
  if (from && Number.isNaN(Date.parse(from))) {
    return { success: false, error: "from 必须为 ISO 日期字符串。" };
  }
  if (to && Number.isNaN(Date.parse(to))) {
    return { success: false, error: "to 必须为 ISO 日期字符串。" };
  }
  if (from && to && Date.parse(from) > Date.parse(to)) {
    return { success: false, error: "from 不能晚于 to。" };
  }

  let limit = DEFAULT_PARSE_FAILURE_LIMIT;
  if (rawLimit !== undefined) {
    const trimmed = rawLimit.trim();
    if (!trimmed) {
      return { success: false, error: "limit 必须是 1 到 500 的整数。" };
    }
    const parsed = Number(trimmed);
    if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_PARSE_FAILURE_LIMIT) {
      return { success: false, error: "limit 必须是 1 到 500 的整数。" };
    }
    limit = parsed;
  }

  return {
    success: true,
    data: {
      from,
      to,
      parserKey,
      errorCode,
      limit,
    },
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isValidSshLocation(location: string): boolean {
  const scpLikePattern = /^([a-zA-Z0-9._-]+@)?([a-zA-Z0-9.-]+):\/[^\s]+$/;
  const uriLikePattern = /^ssh:\/\/([a-zA-Z0-9._-]+@)?([a-zA-Z0-9.-]+)(:\d{1,5})?\/[^\s]+$/;
  return scpLikePattern.test(location) || uriLikePattern.test(location);
}

function resolveSourceTestConnectionTimeoutMs(): number {
  const raw = Bun.env.SOURCE_TEST_CONNECTION_TIMEOUT_MS?.trim();
  if (!raw) {
    return DEFAULT_SOURCE_TEST_CONNECTION_TIMEOUT_MS;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed)) {
    return DEFAULT_SOURCE_TEST_CONNECTION_TIMEOUT_MS;
  }
  return Math.min(
    MAX_SOURCE_TEST_CONNECTION_TIMEOUT_MS,
    Math.max(MIN_SOURCE_TEST_CONNECTION_TIMEOUT_MS, parsed)
  );
}

function resolveSshConnectionEndpoint(source: Source): ResolveSshEndpointResult {
  const hostFromConfig = source.sshConfig?.host?.trim();
  if (hostFromConfig) {
    const port =
      source.sshConfig?.port && Number.isInteger(source.sshConfig.port)
        ? source.sshConfig.port
        : 22;
    if (port < 1 || port > 65535) {
      return {
        success: false,
        errorCode: "ssh_location_invalid",
        detail: `SSH 配置端口不合法（error_code=ssh_location_invalid, port=${port}）。`,
      };
    }
    return {
      success: true,
      endpoint: {
        host: hostFromConfig,
        port,
      },
    };
  }

  const location = source.location.trim();
  if (!location) {
    return {
      success: false,
      errorCode: "ssh_location_invalid",
      detail: "SSH location 为空（error_code=ssh_location_invalid）。",
    };
  }

  if (location.toLowerCase().startsWith("ssh://")) {
    try {
      const parsed = new URL(location);
      const host = parsed.hostname.trim();
      const rawPort = parsed.port.trim();
      const port = rawPort ? Number(rawPort) : 22;
      if (!host || !Number.isInteger(port) || port < 1 || port > 65535) {
        return {
          success: false,
          errorCode: "ssh_location_invalid",
          detail:
            "SSH location 格式不合法，应为 user@host:/path 或 ssh://user@host[:port]/path（error_code=ssh_location_invalid）。",
        };
      }
      return {
        success: true,
        endpoint: {
          host,
          port,
        },
      };
    } catch {
      return {
        success: false,
        errorCode: "ssh_location_invalid",
        detail:
          "SSH location 格式不合法，应为 user@host:/path 或 ssh://user@host[:port]/path（error_code=ssh_location_invalid）。",
      };
    }
  }

  const scpLikePattern = /^([a-zA-Z0-9._-]+@)?([a-zA-Z0-9.-]+):\/[^\s]+$/;
  const matched = scpLikePattern.exec(location);
  if (!matched || !matched[2]) {
    return {
      success: false,
      errorCode: "ssh_location_invalid",
      detail:
        "SSH location 格式不合法，应为 user@host:/path 或 ssh://user@host[:port]/path（error_code=ssh_location_invalid）。",
    };
  }

  return {
    success: true,
    endpoint: {
      host: matched[2],
      port: 22,
    },
  };
}

function mapSshConnectErrorCode(error: NodeJS.ErrnoException): string {
  const code = (error.code ?? "").toString();
  switch (code) {
    case "ENOTFOUND":
      return "ssh_dns_error";
    case "ECONNREFUSED":
      return "ssh_connection_refused";
    case "EHOSTUNREACH":
    case "ENETUNREACH":
      return "ssh_unreachable";
    case "ETIMEDOUT":
      return "ssh_timeout";
    default:
      return "ssh_connect_error";
  }
}

async function checkSshConnectivity(
  endpoint: SshConnectionEndpoint,
  timeoutMs: number
): Promise<SourceConnectionCheckResult> {
  return await new Promise((resolve) => {
    const socket = new Socket();
    let settled = false;
    let connected = false;
    let buffer = "";

    const finish = (result: SourceConnectionCheckResult) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.removeAllListeners();
      socket.destroy();
      resolve(result);
    };

    socket.setTimeout(timeoutMs);

    socket.once("connect", () => {
      connected = true;
      socket.write("SSH-2.0-AgentLedger-SourceTest\r\n");
    });

    socket.on("timeout", () => {
      const errorCode = connected ? "ssh_handshake_timeout" : "ssh_timeout";
      const action = connected ? "握手超时" : "连接超时";
      finish({
        success: false,
        errorCode,
        detail: `SSH ${action}（error_code=${errorCode}, host=${endpoint.host}, port=${endpoint.port}, timeout_ms=${timeoutMs}）。`,
      });
    });

    socket.on("error", (error: NodeJS.ErrnoException) => {
      const errorCode = mapSshConnectErrorCode(error);
      finish({
        success: false,
        errorCode,
        detail: `SSH 连通失败（error_code=${errorCode}, host=${endpoint.host}, port=${endpoint.port}, message=${error.message || "unknown"}）。`,
      });
    });

    socket.on("close", () => {
      if (settled) {
        return;
      }
      const errorCode = connected ? "ssh_handshake_failed" : "ssh_connect_closed";
      finish({
        success: false,
        errorCode,
        detail: `SSH 连接异常关闭（error_code=${errorCode}, host=${endpoint.host}, port=${endpoint.port}）。`,
      });
    });

    socket.on("data", (chunk) => {
      buffer += chunk.toString("utf8");
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() ?? "";

      for (const rawLine of lines) {
        const line = rawLine.trim();
        if (!line) {
          continue;
        }
        if (line.startsWith("SSH-")) {
          finish({
            success: true,
            errorCode: "ok",
            detail: `SSH 连通成功（error_code=ok, host=${endpoint.host}, port=${endpoint.port}, banner=${line}）。`,
          });
          return;
        }
      }
    });

    socket.connect(endpoint.port, endpoint.host);
  });
}

async function runSourceConnectionTest(source: Source): Promise<SourceConnectionCheckResult> {
  if (source.type === "local") {
    if (!isAbsolute(source.location)) {
      return {
        success: false,
        errorCode: "local_location_invalid",
        detail: "local 模式下 location 必须是绝对路径。",
      };
    }

    try {
      await access(source.location, fsConstants.R_OK);
      return {
        success: true,
        errorCode: "ok",
        detail: `本地路径可访问：${source.location}`,
      };
    } catch {
      return {
        success: false,
        errorCode: "local_path_unreadable",
        detail: `本地路径不可访问或不存在：${source.location}`,
      };
    }
  }

  if (source.type === "ssh") {
    if (!isValidSshLocation(source.location) && !source.sshConfig?.host?.trim()) {
      return {
        success: false,
        errorCode: "ssh_location_invalid",
        detail:
          "SSH location 格式不合法，应为 user@host:/path 或 ssh://user@host[:port]/path（error_code=ssh_location_invalid）。",
      };
    }

    const endpointResult = resolveSshConnectionEndpoint(source);
    if (!endpointResult.success) {
      return endpointResult;
    }
    return await checkSshConnectivity(
      endpointResult.endpoint,
      resolveSourceTestConnectionTimeoutMs()
    );
  }

  return {
    success: true,
    errorCode: "skipped",
    detail: "sync-cache 模式无需连接测试，已直接通过。",
  };
}

function buildEphemeralSourceId(requestId: string | undefined): string {
  const normalized = requestId?.trim();
  if (normalized && normalized.length > 0) {
    return `temporary:${normalized}`;
  }
  return `temporary:${Date.now().toString(36)}`;
}

function buildEphemeralSourceForTest(sourceId: string, input: CreateSourceInput): Source {
  return {
    id: sourceId,
    name: input.name,
    type: input.type,
    location: input.location,
    sourceRegion: input.sourceRegion,
    sshConfig: input.sshConfig,
    accessMode: input.accessMode ?? "realtime",
    syncCron: input.syncCron,
    syncRetentionDays: input.syncRetentionDays,
    enabled: input.enabled ?? true,
    createdAt: new Date().toISOString(),
  };
}

async function runSourceConnectionTestWithAudit(
  c: Context<AppEnv>,
  tenantId: string,
  sourceId: string,
  source: Source,
  temporary: boolean
): Promise<SourceConnectionTestResponse> {
  const startedAt = Date.now();
  const testResult = await runSourceConnectionTest(source);
  const latencyMs = Math.max(0, Date.now() - startedAt);
  const responseBody: SourceConnectionTestResponse = {
    sourceId,
    success: testResult.success,
    mode: source.type,
    latencyMs,
    detail: testResult.detail,
    errorCode: testResult.errorCode,
  };

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}:${sourceId}:test-connection`,
    action: SOURCE_CONNECTION_TESTED_ACTION,
    level: testResult.success ? "info" : "warning",
    detail: `Tested source ${sourceId} connection (${source.type}).`,
    metadata: {
      requestId,
      resourceId: sourceId,
      sourceId,
      mode: source.type,
      success: testResult.success,
      latencyMs,
      detail: testResult.detail,
      errorCode: testResult.errorCode,
      location: source.location,
      temporary,
    },
  });

  return responseBody;
}

sourceRoutes.get("/sources", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const items = await repository.listSources(auth.tenantId);
  return c.json({
    items,
    total: items.length,
  });
});

sourceRoutes.get("/sources/missing-region", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const items = await repository.listSourcesMissingRegion(auth.tenantId);
  return c.json({
    items,
    total: items.length,
  });
});

sourceRoutes.post("/sources", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateCreateSourceInput(body);

  if (!result.success) {
    return c.json(
      {
        message: result.error,
      },
      400
    );
  }

  const source = await repository.createSource(auth.tenantId, result.data);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.source_created",
    level: "info",
    detail: `Created source ${source.id}.`,
    metadata: {
      requestId,
      resourceId: source.id,
      name: source.name,
      type: source.type,
      location: source.location,
      enabled: source.enabled,
    },
  });

  return c.json(source, 201);
});

async function handleUpdateSource(c: Context<AppEnv>) {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const sourceId = c.req.param("id")?.trim();
  if (!sourceId) {
    return c.json({ message: "sourceId 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateUpdateSourceInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const current = await findSourceById(auth.tenantId, sourceId);
  if (!current) {
    return c.json({ message: `未找到数据源 ${sourceId}。` }, 404);
  }

  const mergedCandidate: CreateSourceInput = {
    name: result.data.name ?? current.name,
    type: current.type,
    location: result.data.location ?? current.location,
    sourceRegion: result.data.sourceRegion ?? current.sourceRegion,
    sshConfig: result.data.sshConfig ?? current.sshConfig,
    accessMode: result.data.accessMode ?? current.accessMode,
    syncCron: result.data.syncCron ?? current.syncCron,
    syncRetentionDays: result.data.syncRetentionDays ?? current.syncRetentionDays,
    enabled: result.data.enabled ?? current.enabled,
  };
  const mergedValidation = validateCreateSourceInput(mergedCandidate);
  if (!mergedValidation.success) {
    return c.json({ message: mergedValidation.error }, 400);
  }

  const source = await repository.updateSource(auth.tenantId, sourceId, result.data);
  if (!source) {
    return c.json({ message: `未找到数据源 ${sourceId}。` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:${source.id}:update`,
    action: SOURCE_UPDATED_ACTION,
    level: "info",
    detail: `Updated source ${source.id}.`,
    metadata: {
      requestId,
      resourceId: source.id,
      name: source.name,
      location: source.location,
      sourceRegion: source.sourceRegion,
      accessMode: source.accessMode,
      enabled: source.enabled,
    },
  });

  return c.json(source);
}

sourceRoutes.put("/sources/:id", handleUpdateSource);
sourceRoutes.patch("/sources/:id", handleUpdateSource);

async function handleBackfillSourceRegion(c: Context<AppEnv>) {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateSourceRegionBackfillInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const backfillResult = await repository.backfillSourceRegionsFromTenantPrimaryRegion(
    auth.tenantId,
    result.data
  );
  if (!backfillResult) {
    return c.json(
      {
        message: "当前租户未配置主区域，无法自动回填 sourceRegion。",
      },
      409
    );
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:source-region-backfill`,
    action: SOURCE_REGION_BACKFILL_ACTION,
    level: "info",
    detail: backfillResult.dryRun
      ? "Previewed source region backfill by tenant primary region."
      : "Backfilled source region by tenant primary region.",
    metadata: {
      requestId,
      primaryRegion: backfillResult.primaryRegion,
      dryRun: backfillResult.dryRun,
      totalMissing: backfillResult.totalMissing,
      updated: backfillResult.updated,
      skipped: backfillResult.skipped,
      sourceIds: backfillResult.items.map((item) => item.sourceId),
    },
  });

  return c.json(backfillResult);
}

sourceRoutes.post("/sources/backfill-region", handleBackfillSourceRegion);
sourceRoutes.post("/sources/source-region/backfill", handleBackfillSourceRegion);

sourceRoutes.delete("/sources/:id", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const sourceId = c.req.param("id")?.trim();
  if (!sourceId) {
    return c.json(
      {
        message: "sourceId 必须为非空字符串。",
      },
      400
    );
  }

  const result = await repository.deleteSourceById(auth.tenantId, sourceId);
  if (result === "not_found") {
    return c.json(
      {
        message: `未找到数据源 ${sourceId}。`,
      },
      404
    );
  }
  if (result === "conflict") {
    const requestId = c.get("requestId");
    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:${requestId}`,
      action: "control_plane.source_delete_blocked",
      level: "warning",
      detail: `Blocked source deletion for ${sourceId} because active references exist.`,
      metadata: {
        requestId,
        resourceId: sourceId,
        reason: "session_referenced",
      },
    });

    return c.json(
      {
        message: `数据源 ${sourceId} 正在被引用，无法删除。`,
      },
      409
    );
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.source_deleted",
    level: "info",
    detail: `Deleted source ${sourceId}.`,
    metadata: {
      requestId,
      resourceId: sourceId,
    },
  });

  return c.body(null, 204);
});

sourceRoutes.post("/sources/test-connection", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  if (!isRecord(body)) {
    return c.json(
      {
        message: "请求体必须是对象。",
      },
      400
    );
  }

  const rawSourceId = body.sourceId;
  const rawSource = body.source;
  const hasSourceId = rawSourceId !== undefined;
  const hasSource = rawSource !== undefined;

  if (!hasSourceId && !hasSource) {
    return c.json(
      {
        message: "请求体必须包含 sourceId 或 source。",
      },
      400
    );
  }

  if (hasSourceId && hasSource) {
    return c.json(
      {
        message: "sourceId 与 source 不能同时提供。",
      },
      400
    );
  }

  if (hasSourceId) {
    if (typeof rawSourceId !== "string") {
      return c.json(
        {
          message: "sourceId 必须为非空字符串。",
        },
        400
      );
    }

    const sourceId = normalizeSourceId(rawSourceId);
    if (!sourceId) {
      return c.json(
        {
          message: "sourceId 必须为非空字符串。",
        },
        400
      );
    }

    const source = await findSourceById(auth.tenantId, sourceId);
    if (!source) {
      return c.json(
        {
          message: `未找到数据源 ${sourceId}。`,
        },
        404
      );
    }

    const responseBody = await runSourceConnectionTestWithAudit(
      c,
      auth.tenantId,
      sourceId,
      source,
      false
    );

    return c.json(responseBody);
  }

  const validationResult = validateCreateSourceInput(rawSource);
  if (!validationResult.success) {
    return c.json(
      {
        message: validationResult.error,
      },
      400
    );
  }

  const ephemeralSourceId = buildEphemeralSourceId(c.get("requestId"));
  const source = buildEphemeralSourceForTest(ephemeralSourceId, validationResult.data);
  const responseBody = await runSourceConnectionTestWithAudit(
    c,
    auth.tenantId,
    ephemeralSourceId,
    source,
    true
  );

  return c.json(responseBody);
});

sourceRoutes.post("/sources/:id/test-connection", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const sourceId = normalizeSourceId(c.req.param("id"));
  if (!sourceId) {
    return c.json(
      {
        message: "sourceId 必须为非空字符串。",
      },
      400
    );
  }

  const source = await findSourceById(auth.tenantId, sourceId);
  if (!source) {
    return c.json(
      {
        message: `未找到数据源 ${sourceId}。`,
      },
      404
    );
  }

  const responseBody = await runSourceConnectionTestWithAudit(
    c,
    auth.tenantId,
    sourceId,
    source,
    false
  );

  return c.json(responseBody);
});

sourceRoutes.get("/sources/:id/sync-jobs", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const sourceId = normalizeSourceId(c.req.param("id"));
  if (!sourceId) {
    return c.json(
      {
        message: "sourceId 必须为非空字符串。",
      },
      400
    );
  }

  const source = await findSourceById(auth.tenantId, sourceId);
  if (!source) {
    return c.json(
      {
        message: `未找到数据源 ${sourceId}。`,
      },
      404
    );
  }

  const limitResult = parseSyncJobLimit(c.req.query("limit"));
  if (!limitResult.success) {
    return c.json(
      {
        message: limitResult.error,
      },
      400
    );
  }

  const items = await repository.listSyncJobs(auth.tenantId, sourceId, limitResult.limit);

  return c.json({
    items,
    total: items.length,
    limit: limitResult.limit,
  });
});

sourceRoutes.post("/sources/:id/sync-jobs", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const sourceId = normalizeSourceId(c.req.param("id"));
  if (!sourceId) {
    return c.json(
      {
        message: "sourceId 必须为非空字符串。",
      },
      400
    );
  }

  const source = await findSourceById(auth.tenantId, sourceId);
  if (!source) {
    return c.json(
      {
        message: `未找到数据源 ${sourceId}。`,
      },
      404
    );
  }

  const body = await c.req.json().catch(() => undefined);
  if (body !== undefined && !isRecord(body)) {
    return c.json(
      {
        message: "请求体必须是对象。",
      },
      400
    );
  }

  const mode =
    body?.mode === undefined ? source.accessMode : parseSyncJobMode(body.mode);
  if (!mode) {
    return c.json(
      {
        message: "mode 仅支持 realtime/sync/hybrid。",
      },
      400
    );
  }

  const nextRunAtResult = parseSyncJobNextRunAt(body?.nextRunAt ?? body?.next_run_at);
  if (!nextRunAtResult.success) {
    return c.json(
      {
        message: nextRunAtResult.error,
      },
      400
    );
  }

  const detail = "同步任务已创建，等待执行。";
  let job: SyncJob;
  try {
    job = await repository.createSyncJob(auth.tenantId, sourceId, mode, "pending", undefined, {
      trigger: "manual",
      attempt: 1,
      cancelRequested: false,
      nextRunAt: nextRunAtResult.value,
    });
  } catch (error) {
    if (error instanceof Error && error.message === "sync_job_source_not_found") {
      return c.json({ message: `未找到数据源 ${sourceId}。` }, 404);
    }
    throw error;
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:${job.id}`,
    action: SOURCE_SYNC_JOB_ACTION,
    level: "info",
    detail: `Created source sync job ${job.id} for ${sourceId}.`,
    metadata: {
      requestId,
      resourceId: sourceId,
      sourceId,
      sourceType: source.type,
      jobId: job.id,
      mode,
      status: "pending",
      trigger: "manual",
      attempt: 1,
      detail,
    },
  });

  return c.json(job, 202);
});

sourceRoutes.get("/sources/:id/watermarks", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const sourceId = normalizeSourceId(c.req.param("id"));
  if (!sourceId) {
    return c.json(
      {
        message: "sourceId 必须为非空字符串。",
      },
      400
    );
  }

  const source = await findSourceById(auth.tenantId, sourceId);
  if (!source) {
    return c.json(
      {
        message: `未找到数据源 ${sourceId}。`,
      },
      404
    );
  }

  const items = await repository.listSourceWatermarks(auth.tenantId, sourceId);
  return c.json({
    items,
    total: items.length,
  });
});

sourceRoutes.get("/sources/:id/parse-failures", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const sourceId = normalizeSourceId(c.req.param("id"));
  if (!sourceId) {
    return c.json(
      {
        message: "sourceId 必须为非空字符串。",
      },
      400
    );
  }

  const source = await findSourceById(auth.tenantId, sourceId);
  if (!source) {
    return c.json(
      {
        message: `未找到数据源 ${sourceId}。`,
      },
      404
    );
  }

  const queryResult = parseSourceParseFailureQuery(c.req.query());
  if (!queryResult.success) {
    return c.json(
      {
        message: queryResult.error,
      },
      400
    );
  }

  const result = await repository.listSourceParseFailures(
    auth.tenantId,
    sourceId,
    queryResult.data
  );

  return c.json({
    items: result.items,
    total: result.total,
    filters: queryResult.data,
  });
});

sourceRoutes.get("/sources/:id/health", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const sourceId = normalizeSourceId(c.req.param("id"));
  if (!sourceId) {
    return c.json(
      {
        message: "sourceId 必须为非空字符串。",
      },
      400
    );
  }

  const source = await findSourceById(auth.tenantId, sourceId);
  if (!source) {
    return c.json(
      {
        message: `未找到数据源 ${sourceId}。`,
      },
      404
    );
  }

  const health: SourceHealth | null = await repository.getSourceHealth(
    auth.tenantId,
    sourceId
  );
  if (!health) {
    const fallbackHealth: SourceHealth = {
      sourceId,
      accessMode: source.accessMode,
      lastSuccessAt: null,
      lastFailureAt: null,
      failureCount: 0,
      avgLatencyMs: null,
      freshnessMinutes: null,
    };
    return c.json(fallbackHealth);
  }

  return c.json(health);
});

sourceRoutes.patch("/sync-jobs/:id/cancel", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const syncJobId = normalizeSyncJobId(c.req.param("id"));
  if (!syncJobId) {
    return c.json(
      {
        message: "syncJobId 必须为非空字符串。",
      },
      400
    );
  }

  const job = await repository.requestCancelSyncJob(auth.tenantId, syncJobId);
  if (!job) {
    return c.json(
      {
        message: `未找到同步任务 ${syncJobId}。`,
      },
      404
    );
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}:${job.id}:cancel`,
    action: SOURCE_SYNC_JOB_CANCEL_ACTION,
    level: "info",
    detail: `Requested cancel for source sync job ${job.id}.`,
    metadata: {
      requestId,
      resourceId: job.sourceId,
      sourceId: job.sourceId,
      jobId: job.id,
      status: job.status,
      cancelRequested: job.cancelRequested ?? false,
    },
  });

  return c.json(job, 202);
});
