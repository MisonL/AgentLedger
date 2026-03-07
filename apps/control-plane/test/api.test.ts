import { afterAll, describe, expect, test } from "bun:test";
import { createHmac } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  validateAuthLoginInput,
  validateAuthLogoutInput,
  validateAuthRefreshInput,
  validateAuthRegisterInput,
} from "../src/contracts";
import type {
  Alert,
  AlertOrchestrationRule,
  AlertOrchestrationRuleListInput,
  AlertListInput,
  AuditListInput,
  AuditListResponse,
  Budget,
  HeatmapCell,
  Session,
  SessionSearchResponse,
  Source,
  SourceHealth,
  SourceListResponse,
  UsageDailyItem,
  UsageHeatmapDrilldownResponse,
  UsageHeatmapResponse,
  UsageWeeklySummaryResponse,
} from "../src/contracts";
import { createApp } from "../src/app";
import { getControlPlaneRepository } from "../src/data/repository";
import {
  flushReplayJobExecutionQueueForTests,
  resetReplayJobExecutionWorkerForTests,
  setReplayJobExecutionHandlerForTests,
} from "../src/routes/replay";
import {
  flushWebhookReplayExecutionQueueForTests,
  resetWebhookReplayExecutionWorkerForTests,
} from "../src/routes/open-platform";
import type {
  SourceParseFailure,
  UsageHeatmapQueryInput,
} from "../src/data/repository";
import {
  createAuthSessionToken,
  getRefreshSessionExpiresAt,
  issueAccessToken,
  issueRefreshToken,
  verifyAccessToken,
  verifyRefreshToken,
} from "../src/security/tokens";
import { verifyEvidenceBundle } from "../src/security/evidence-bundle";

const replayArtifactTestRoot = await mkdtemp(
  join(tmpdir(), "agentledger-control-plane-replay-artifacts-"),
);
const originalReplayStorageLocalRoot = Bun.env.REPLAY_STORAGE_LOCAL_ROOT;
Bun.env.REPLAY_STORAGE_LOCAL_ROOT = replayArtifactTestRoot;

afterAll(async () => {
  if (originalReplayStorageLocalRoot === undefined) {
    delete Bun.env.REPLAY_STORAGE_LOCAL_ROOT;
  } else {
    Bun.env.REPLAY_STORAGE_LOCAL_ROOT = originalReplayStorageLocalRoot;
  }
  await rm(replayArtifactTestRoot, { recursive: true, force: true });
});

describe("Control Plane API", () => {
  const app = createApp();
  let defaultAuthContextPromise: Promise<{
    accessToken: string;
    userId?: string;
  }> | null = null;
  const repository = getControlPlaneRepository() as unknown as {
    getPool?: () => Promise<{
      query: (
        text: string,
        values?: readonly unknown[],
      ) => Promise<{ rows: unknown[] }>;
    } | null>;
    memorySessions?: Session[];
    memorySyncJobs?: Array<{
      id: string;
      sourceId: string;
      mode: "realtime" | "sync" | "hybrid";
      status: "pending" | "running" | "success" | "failed" | "cancelled";
      durationMs?: number;
      startedAt?: string;
      endedAt?: string;
      nextRunAt?: string;
      createdAt: string;
      updatedAt: string;
    }>;
    memorySessionEvents?: Array<{
      sessionId: string;
      sourceId: string;
      text: string;
      sourcePath?: string;
    }>;
    memorySourceParseFailures?: Array<{
      tenantId: string;
      failure: SourceParseFailure;
    }>;
    memoryAlerts?: Alert[];
    claimIntegrationAlertCallback?: (input: {
      callbackId: string;
      tenantId: string;
      action:
        | "ack"
        | "resolve"
        | "request_release"
        | "approve_release"
        | "reject_release";
      processedAt?: string;
      staleAfterMs?: number;
    }) => Promise<{
      claimed: boolean;
      record: {
        callbackId: string;
        tenantId: string;
        action: string;
        response: Record<string, unknown>;
        processedAt: string;
      };
    }>;
    createLocalUser?: (input: {
      email: string;
      passwordHash: string;
      displayName?: string;
    }) => Promise<{
      id: string;
      email: string;
      passwordHash: string;
      displayName: string;
      createdAt: string;
      updatedAt: string;
    }>;
    getLocalUserByEmail?: (email: string) => Promise<{
      id: string;
      email: string;
      passwordHash: string;
      displayName: string;
      createdAt: string;
      updatedAt: string;
    } | null>;
    getUserById?: (id: string) => Promise<{
      id: string;
      email: string;
      passwordHash: string;
      displayName: string;
      createdAt: string;
      updatedAt: string;
    } | null>;
    upsertTenantResidencyPolicy?: (
      tenantId: string,
      input: {
        tenantId: string;
        mode: "single_region" | "active_active";
        primaryRegion: string;
        replicaRegions: string[];
        allowCrossRegionTransfer: boolean;
        requireTransferApproval: boolean;
        updatedAt: string;
      },
    ) => Promise<{
      tenantId: string;
      mode: "single_region" | "active_active";
      primaryRegion: string;
      replicaRegions: string[];
      allowCrossRegionTransfer: boolean;
      requireTransferApproval: boolean;
      updatedAt: string;
    }>;
    createAuthSession?: (input: {
      userId: string;
      tenantId?: string;
      sessionToken: string;
      expiresAt: string;
    }) => Promise<{
      id: string;
      userId: string;
      tenantId: string;
      sessionToken: string;
      expiresAt: string;
      revokedAt: string | null;
      replacedBySessionId: string | null;
      createdAt: string;
      updatedAt: string;
    }>;
    getAuthSessionById?: (id: string) => Promise<{
      id: string;
      userId: string;
      tenantId: string;
      sessionToken: string;
      expiresAt: string;
      revokedAt: string | null;
      replacedBySessionId: string | null;
      createdAt: string;
      updatedAt: string;
    } | null>;
    rotateAuthSession?: (
      sessionId: string,
      input: {
        sessionToken: string;
        expiresAt: string;
      },
    ) => Promise<{
      id: string;
      userId: string;
      tenantId: string;
      sessionToken: string;
      expiresAt: string;
      revokedAt: string | null;
      replacedBySessionId: string | null;
      createdAt: string;
      updatedAt: string;
    } | null>;
    revokeAuthSession?: (id: string) => Promise<boolean>;
    createSyncJob?: (
      tenantId: string,
      sourceId: string,
      mode: "realtime" | "sync" | "hybrid",
      status: "pending" | "running" | "success" | "failed" | "cancelled",
      error?: string,
      options?: {
        trigger?: string;
        attempt?: number;
        startedAt?: string;
        endedAt?: string;
        nextRunAt?: string;
        durationMs?: number;
        errorCode?: string;
        errorDetail?: string;
        cancelRequested?: boolean;
      },
    ) => Promise<{
      id: string;
      sourceId: string;
      mode: "realtime" | "sync" | "hybrid";
      status: "pending" | "running" | "success" | "failed" | "cancelled";
      durationMs?: number;
      startedAt?: string;
      endedAt?: string;
      nextRunAt?: string;
      createdAt: string;
      updatedAt: string;
    }>;
    listTenants?: () => Promise<Array<{ id: string; name: string }>>;
    listOrganizations?: (
      tenantId: string,
    ) => Promise<Array<{ id: string; tenantId: string; name: string }>>;
    getSourceHealth?: (
      tenantId: string,
      sourceId: string,
    ) => Promise<SourceHealth | null>;
    listUsageDaily?: (input?: {
      tenantId?: string;
      from?: string;
      to?: string;
      limit?: number;
    }) => Promise<UsageDailyItem[]>;
    listUsageHeatmap?: (
      input?: UsageHeatmapQueryInput,
    ) => Promise<HeatmapCell[]>;
  };

  function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null;
  }

  function pickString(value: unknown, keys: string[]): string | undefined {
    if (!isRecord(value)) {
      return undefined;
    }

    for (const key of keys) {
      const target = value[key];
      if (typeof target === "string" && target.trim().length > 0) {
        return target;
      }
    }

    return undefined;
  }

  function normalizePath(path: string): string {
    if (path.startsWith("http://") || path.startsWith("https://")) {
      const parsed = new URL(path);
      return `${parsed.pathname}${parsed.search}`;
    }
    if (path.startsWith("/")) {
      return path;
    }
    return `/${path}`;
  }

  async function readResponseAsUnknown(response: Response): Promise<unknown> {
    const text = await response.text();
    if (!text) {
      return {};
    }

    try {
      return JSON.parse(text) as unknown;
    } catch {
      return text;
    }
  }

  function extractJobId(payload: unknown): string | undefined {
    const candidates = [payload];
    if (isRecord(payload)) {
      candidates.push(payload.job, payload.data, payload.result);
    }

    for (const candidate of candidates) {
      const jobId = pickString(candidate, ["jobId", "id", "exportJobId"]);
      if (jobId) {
        return jobId;
      }
    }

    return undefined;
  }

  function extractJobStatus(payload: unknown): string | undefined {
    const candidates = [payload];
    if (isRecord(payload)) {
      candidates.push(payload.job, payload.data, payload.result);
    }

    for (const candidate of candidates) {
      const status = pickString(candidate, ["status", "state", "phase"]);
      if (status) {
        return status.toLowerCase();
      }
    }

    return undefined;
  }

  function extractDownloadPath(payload: unknown): string | undefined {
    const candidates = [payload];
    if (isRecord(payload)) {
      candidates.push(payload.job, payload.data, payload.result);
    }

    for (const candidate of candidates) {
      const downloadPath = pickString(candidate, [
        "downloadUrl",
        "downloadPath",
        "downloadUri",
        "fileUrl",
        "url",
      ]);
      if (downloadPath) {
        return normalizePath(downloadPath);
      }
    }

    return undefined;
  }

  function extractStatusPath(payload: unknown): string | undefined {
    const candidates = [payload];
    if (isRecord(payload)) {
      candidates.push(payload.job, payload.data, payload.result);
    }

    for (const candidate of candidates) {
      const statusPath = pickString(candidate, [
        "statusUrl",
        "statusPath",
        "jobUrl",
      ]);
      if (statusPath) {
        return normalizePath(statusPath);
      }
    }

    return undefined;
  }

  async function ensureSourceReferencedBySession(
    sourceId: string,
  ): Promise<() => Promise<void>> {
    const nonce = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const sessionId = `test-source-conflict-${nonce}`;
    const now = new Date().toISOString();

    if (typeof repository.getPool === "function") {
      const pool = await repository.getPool();
      if (pool) {
        await pool.query(
          `INSERT INTO sessions (
             id,
             source_id,
             provider,
             native_session_id,
             tool,
             model,
             started_at,
             ended_at,
             tokens,
             cost,
             created_at,
             updated_at
           )
           VALUES (
             $1, $2, $3, $4, $5, $6,
             $7::timestamptz, $8::timestamptz, $9, $10,
             $11::timestamptz, $11::timestamptz
           )`,
          [
            sessionId,
            sourceId,
            "control-plane-test",
            `native-${nonce}`,
            "Codex CLI",
            "gpt-5-codex",
            now,
            now,
            42,
            0.01,
            now,
          ],
        );

        return async () => {
          await pool.query(`DELETE FROM sessions WHERE id = $1`, [sessionId]);
        };
      }
    }

    if (!Array.isArray(repository.memorySessions)) {
      throw new Error("无法注入冲突会话：memorySessions 不可用。");
    }

    repository.memorySessions.push({
      id: sessionId,
      sourceId,
      tool: "Codex CLI",
      model: "gpt-5-codex",
      startedAt: now,
      endedAt: now,
      tokens: 42,
      cost: 0.01,
    });

    return async () => {
      if (!Array.isArray(repository.memorySessions)) {
        return;
      }
      const index = repository.memorySessions.findIndex(
        (item) => item.id === sessionId,
      );
      if (index >= 0) {
        repository.memorySessions.splice(index, 1);
      }
    };
  }

  async function insertSessionForSearch(
    sourceId: string,
    input: {
      provider: string;
      tool: string;
      model: string;
      project?: string;
      sourcePath?: string;
      startedAt?: string;
      endedAt?: string;
      tokens?: number;
      cost?: number;
      eventTexts?: string[];
    },
  ): Promise<{ id: string; cleanup: () => Promise<void> }> {
    const nonce = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const sessionId = `search-session-${nonce}`;
    const startedAt = input.startedAt ?? new Date().toISOString();
    const endedAt = input.endedAt ?? startedAt;
    const tokens = input.tokens ?? 1;
    const cost = input.cost ?? 0;

    if (typeof repository.getPool === "function") {
      const pool = await repository.getPool();
      if (pool) {
        await pool.query(
          `INSERT INTO sessions (
             id,
             source_id,
             provider,
             native_session_id,
             tool,
             workspace,
             model,
             started_at,
             ended_at,
             tokens,
             cost,
             created_at,
             updated_at
           )
           VALUES (
             $1, $2, $3, $4, $5, $6, $7,
             $8::timestamptz, $9::timestamptz, $10, $11,
             $12::timestamptz, $12::timestamptz
           )`,
          [
            sessionId,
            sourceId,
            input.provider,
            `native-${nonce}`,
            input.tool,
            input.project ?? null,
            input.model,
            startedAt,
            endedAt,
            tokens,
            cost,
            startedAt,
          ],
        );

        if (Array.isArray(input.eventTexts) && input.eventTexts.length > 0) {
          for (const [index, eventText] of input.eventTexts.entries()) {
            const text = eventText.trim();
            if (!text) {
              continue;
            }
            const timestamp = new Date(
              Date.parse(startedAt) + index * 1000,
            ).toISOString();
            await pool.query(
              `INSERT INTO events (
                 id,
                 session_id,
                 source_id,
                 event_type,
                role,
                text,
                "timestamp",
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
                reasoning_tokens,
                cost_usd,
                source_path,
                created_at,
                updated_at
               )
               VALUES (
                 $1, $2, $3, 'message', 'user', $4,
                 $5::timestamptz, 0, 0, 0, 0, 0, 0, $6,
                 $5::timestamptz, $5::timestamptz
               )`,
              [
                `event-${nonce}-${index}`,
                sessionId,
                sourceId,
                text,
                timestamp,
                input.sourcePath ?? null,
              ],
            );
          }
        }

        return {
          id: sessionId,
          cleanup: async () => {
            await pool.query(`DELETE FROM sessions WHERE id = $1`, [sessionId]);
          },
        };
      }
    }

    if (!Array.isArray(repository.memorySessions)) {
      throw new Error("无法注入会话数据：memorySessions 不可用。");
    }

    repository.memorySessions.push({
      id: sessionId,
      sourceId,
      tool: input.tool,
      model: input.model,
      startedAt,
      endedAt,
      tokens,
      cost,
      provider: input.provider,
      workspace: input.project,
    } as Session & {
      provider?: string;
      workspace?: string;
    });
    if (
      Array.isArray(repository.memorySessionEvents) &&
      Array.isArray(input.eventTexts)
    ) {
      for (const eventText of input.eventTexts) {
        const text = eventText.trim();
        if (!text) {
          continue;
        }
        repository.memorySessionEvents.push({
          sessionId,
          sourceId,
          text,
          sourcePath: input.sourcePath,
        });
      }
    }

    return {
      id: sessionId,
      cleanup: async () => {
        if (!Array.isArray(repository.memorySessions)) {
          return;
        }
        const index = repository.memorySessions.findIndex(
          (item) => item.id === sessionId,
        );
        if (index >= 0) {
          repository.memorySessions.splice(index, 1);
        }
        if (Array.isArray(repository.memorySessionEvents)) {
          for (
            let i = repository.memorySessionEvents.length - 1;
            i >= 0;
            i -= 1
          ) {
            if (repository.memorySessionEvents[i]?.sessionId === sessionId) {
              repository.memorySessionEvents.splice(i, 1);
            }
          }
        }
      },
    };
  }

  async function createTestAlert(
    tenantId: string,
    status: Alert["status"] = "open",
    options?: {
      budgetId?: string;
      sourceId?: string;
      severity?: Alert["severity"];
      createdAt?: string;
    },
  ): Promise<{ alert: Alert; cleanup: () => Promise<void> }> {
    const nonce = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const now = options?.createdAt ?? new Date().toISOString();
    const budgetId = options?.budgetId ?? `budget-${nonce}`;
    const sourceId = options?.sourceId ?? `source-${nonce}`;
    const severity = options?.severity ?? "warning";
    const dedupeKey = `test-alert-${nonce}`;

    if (typeof repository.getPool === "function") {
      const pool = await repository.getPool();
      if (pool) {
        const result = await pool.query(
          `INSERT INTO governance_alerts (
             tenant_id,
             budget_id,
             source_id,
             period,
             window_start,
             window_end,
             tokens_used,
             cost_used,
             token_limit,
             cost_limit,
             threshold,
             status,
             severity,
             dedupe_key,
             created_at,
             updated_at
           )
           VALUES (
             $1,
             $2,
             $3,
             $4,
             $5::timestamptz,
             $6::timestamptz,
             $7,
             $8,
             $9,
             $10,
             $11,
             $12,
             $13,
             $14,
             $15::timestamptz,
             $15::timestamptz
           )
           RETURNING id::text AS id`,
          [
            tenantId,
            budgetId,
            sourceId,
            "monthly",
            now,
            now,
            1200,
            0.12,
            1000,
            0.1,
            0.8,
            status,
            severity,
            dedupeKey,
            now,
          ],
        );
        const insertedId = String(
          (result.rows[0] as { id?: unknown } | undefined)?.id ?? "",
        );
        const insertedAlert: Alert = {
          id: insertedId,
          tenantId,
          budgetId,
          sourceId,
          period: "monthly",
          windowStart: now,
          windowEnd: now,
          tokensUsed: 1200,
          costUsed: 0.12,
          tokenLimit: 1000,
          costLimit: 0.1,
          threshold: 0.8,
          status,
          severity,
          triggeredAt: now,
          updatedAt: now,
        };

        return {
          alert: insertedAlert,
          cleanup: async () => {
            await pool.query(
              `DELETE FROM governance_alerts
               WHERE tenant_id = $1
                 AND id::text = $2`,
              [tenantId, insertedId],
            );
          },
        };
      }
    }

    if (!Array.isArray(repository.memoryAlerts)) {
      throw new Error("无法注入告警数据：memoryAlerts 不可用。");
    }

    const alert: Alert = {
      id: `test-alert-${nonce}`,
      tenantId,
      budgetId,
      sourceId,
      period: "monthly",
      windowStart: now,
      windowEnd: now,
      tokensUsed: 1200,
      costUsed: 0.12,
      tokenLimit: 1000,
      costLimit: 0.1,
      threshold: 0.8,
      status,
      severity,
      triggeredAt: now,
      updatedAt: now,
    };
    repository.memoryAlerts.push(alert);

    return {
      alert: { ...alert },
      cleanup: async () => {
        if (!Array.isArray(repository.memoryAlerts)) {
          return;
        }
        const index = repository.memoryAlerts.findIndex(
          (item) => item.id === alert.id,
        );
        if (index >= 0) {
          repository.memoryAlerts.splice(index, 1);
        }
      },
    };
  }

  async function createAsyncExportJob(
    format: "json" | "csv",
    keyword: string,
    accessToken?: string,
    userId?: string,
  ): Promise<{
    createPath: string;
    jobId: string;
    statusPath?: string;
    downloadPath?: string;
  }> {
    const authHeaders = await resolveAuthHeaders(accessToken, userId);
    const encodedKeyword = encodeURIComponent(keyword);
    const createCandidates: Array<{
      path: string;
      init?: RequestInit;
    }> = [
      {
        path: "/api/v1/exports/sessions/jobs",
        init: {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({
            format,
            keyword,
            limit: 20,
          }),
        },
      },
      {
        path: "/api/v1/exports/jobs/sessions",
        init: {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({
            format,
            keyword,
            limit: 20,
          }),
        },
      },
      {
        path: "/api/v1/exports/jobs",
        init: {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({
            resource: "sessions",
            format,
            filters: {
              keyword,
              limit: 20,
            },
          }),
        },
      },
      {
        path: `/api/v1/exports/sessions?async=true&format=${format}&keyword=${encodedKeyword}&limit=20`,
        init:
          Object.keys(authHeaders).length > 0
            ? { headers: authHeaders }
            : undefined,
      },
    ];

    for (const candidate of createCandidates) {
      const response = await app.request(candidate.path, candidate.init);
      if (response.status === 404 || response.status === 405) {
        continue;
      }

      const payload = await readResponseAsUnknown(response);
      if (response.status < 200 || response.status >= 300) {
        throw new Error(
          `创建导出任务失败(${candidate.path})，status=${response.status}，payload=${JSON.stringify(payload)}`,
        );
      }

      const jobId = extractJobId(payload);
      if (!jobId) {
        throw new Error(
          `创建导出任务返回缺少 jobId(${candidate.path})，payload=${JSON.stringify(payload)}`,
        );
      }

      const statusHeader = response.headers.get("location");
      return {
        createPath: candidate.path,
        jobId,
        statusPath: statusHeader
          ? normalizePath(statusHeader)
          : extractStatusPath(payload),
        downloadPath: extractDownloadPath(payload),
      };
    }

    throw new Error("未发现可用的异步导出 job 创建接口。");
  }

  async function pollExportJobUntilDone(
    jobId: string,
    statusPath?: string,
    accessToken?: string,
    userId?: string,
  ): Promise<{ payload: unknown; downloadPath?: string }> {
    const authHeaders = await resolveAuthHeaders(accessToken, userId);
    const statusCandidates = new Set<string>();
    if (statusPath) {
      statusCandidates.add(normalizePath(statusPath));
    }
    statusCandidates.add(`/api/v1/exports/sessions/jobs/${jobId}`);
    statusCandidates.add(`/api/v1/exports/jobs/${jobId}`);

    const doneStatus = new Set([
      "completed",
      "succeeded",
      "success",
      "done",
      "finished",
      "ready",
    ]);
    const failedStatus = new Set(["failed", "error", "cancelled", "canceled"]);

    for (let attempt = 0; attempt < 30; attempt += 1) {
      for (const candidate of statusCandidates) {
        const response = await app.request(
          candidate,
          Object.keys(authHeaders).length > 0
            ? { headers: authHeaders }
            : undefined,
        );
        if (response.status === 404 || response.status === 405) {
          continue;
        }

        const payload = await readResponseAsUnknown(response);
        if (response.status < 200 || response.status >= 300) {
          throw new Error(
            `查询导出任务失败(${candidate})，status=${response.status}，payload=${JSON.stringify(payload)}`,
          );
        }

        const status = extractJobStatus(payload);
        const downloadPath = extractDownloadPath(payload);
        if (downloadPath && (!status || doneStatus.has(status))) {
          return {
            payload,
            downloadPath,
          };
        }
        if (status && doneStatus.has(status)) {
          return {
            payload,
            downloadPath,
          };
        }
        if (status && failedStatus.has(status)) {
          throw new Error(
            `导出任务进入失败状态(${candidate})，jobId=${jobId}，payload=${JSON.stringify(payload)}`,
          );
        }
      }

      await Bun.sleep(100);
    }

    throw new Error(`导出任务在轮询窗口内未完成，jobId=${jobId}`);
  }

  async function downloadExportResult(
    jobId: string,
    downloadPath?: string,
    accessToken?: string,
    userId?: string,
  ): Promise<Response> {
    const authHeaders = await resolveAuthHeaders(accessToken, userId);
    const downloadCandidates = new Set<string>();
    if (downloadPath) {
      downloadCandidates.add(normalizePath(downloadPath));
    }
    downloadCandidates.add(`/api/v1/exports/sessions/jobs/${jobId}/download`);
    downloadCandidates.add(`/api/v1/exports/jobs/${jobId}/download`);
    downloadCandidates.add(`/api/v1/exports/jobs/${jobId}/file`);

    for (const candidate of downloadCandidates) {
      const response = await app.request(
        candidate,
        Object.keys(authHeaders).length > 0
          ? { headers: authHeaders }
          : undefined,
      );
      if (response.status === 404 || response.status === 405) {
        continue;
      }
      if (response.status >= 200 && response.status < 300) {
        return response;
      }

      const payload = await readResponseAsUnknown(response);
      throw new Error(
        `下载导出文件失败(${candidate})，status=${response.status}，payload=${JSON.stringify(payload)}`,
      );
    }

    throw new Error(`未发现可用的导出下载接口，jobId=${jobId}`);
  }

  async function queryAuditByAction(
    action: string,
    keyword: string,
    accessToken?: string,
    userId?: string,
  ): Promise<{
    items: Array<{
      action: string;
      level: string;
      detail: string;
      metadata: Record<string, unknown>;
    }>;
    total: number;
    filters: AuditListInput & {
      action?: string;
      keyword?: string;
      limit?: number;
    };
  }> {
    const query = new URLSearchParams({
      action,
      keyword,
      limit: "200",
    });
    const authHeaders = await resolveAuthHeaders(accessToken, userId);
    const auditResponse = await app.request(
      `/api/v1/audits?${query.toString()}`,
      Object.keys(authHeaders).length > 0
        ? { headers: authHeaders }
        : undefined,
    );
    const audits = (await auditResponse.json()) as {
      items: Array<{
        action: string;
        level: string;
        detail: string;
        metadata: Record<string, unknown>;
      }>;
      total: number;
      filters: AuditListInput & {
        action?: string;
        keyword?: string;
        limit?: number;
      };
    };

    expect(auditResponse.status).toBe(200);
    expect(Array.isArray(audits.items)).toBe(true);
    expect(typeof audits.total).toBe("number");
    expect(audits.filters.action).toBe(action);
    expect(audits.filters.keyword).toBe(keyword);
    return audits;
  }

  async function queryAuditByActionWithHeaders(
    action: string,
    keyword: string,
    headers: Record<string, string>,
  ): Promise<{
    items: Array<{
      action: string;
      level: string;
      detail: string;
      metadata: Record<string, unknown>;
    }>;
    total: number;
    filters: AuditListInput & {
      action?: string;
      keyword?: string;
      limit?: number;
    };
  }> {
    const query = new URLSearchParams({
      action,
      keyword,
      limit: "200",
    });
    const auditResponse = await app.request(
      `/api/v1/audits?${query.toString()}`,
      {
        headers,
      },
    );
    const audits = (await auditResponse.json()) as {
      items: Array<{
        action: string;
        level: string;
        detail: string;
        metadata: Record<string, unknown>;
      }>;
      total: number;
      filters: AuditListInput & {
        action?: string;
        keyword?: string;
        limit?: number;
      };
    };

    expect(auditResponse.status).toBe(200);
    expect(Array.isArray(audits.items)).toBe(true);
    expect(typeof audits.total).toBe("number");
    expect(audits.filters.action).toBe(action);
    expect(audits.filters.keyword).toBe(keyword);
    return audits;
  }

  function auditMatchesKeyword(
    item: {
      action: string;
      detail: string;
      metadata: Record<string, unknown>;
    },
    action: string,
    keyword: string,
  ): boolean {
    if (item.action !== action) {
      return false;
    }
    const normalizedKeyword = keyword.toLowerCase();
    const detailMatched = item.detail.toLowerCase().includes(normalizedKeyword);
    const metadataMatched = JSON.stringify(item.metadata)
      .toLowerCase()
      .includes(normalizedKeyword);
    return detailMatched || metadataMatched;
  }

  type ApiCandidate = {
    path: string;
    init?: RequestInit;
  };

  type ApiCallResult = {
    path: string;
    response: Response;
    payload: unknown;
  };

  function createNonce(prefix: string): string {
    return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  }

  async function startMockSshServer(options?: {
    sendBanner?: boolean;
    banner?: string;
  }): Promise<{
    host: string;
    port: number;
    stop: () => Promise<void>;
  }> {
    const sendBanner = options?.sendBanner ?? true;
    const banner = options?.banner ?? "SSH-2.0-OpenSSH_9.0";
    const sockets = new Set<Socket>();
    const server: Server = createServer((socket) => {
      sockets.add(socket);
      socket.on("close", () => {
        sockets.delete(socket);
      });
      if (sendBanner) {
        socket.write(`${banner}\r\n`);
      }
    });

    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => {
        server.off("error", reject);
        resolve();
      });
    });

    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("mock ssh server address unavailable");
    }

    return {
      host: "127.0.0.1",
      port: address.port,
      stop: async () => {
        for (const socket of sockets) {
          socket.destroy();
        }
        await new Promise<void>((resolve, reject) => {
          server.close((error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve();
          });
        });
      },
    };
  }

  function buildIntegrationCallbackSignedRequest(
    secret: string,
    payload: Record<string, unknown>,
    options: {
      timestamp?: string;
      nonce?: string;
      signature?: string;
    } = {},
  ): {
    init: RequestInit;
    timestamp: string;
    nonce: string;
    signature: string;
  } {
    const timestamp = options.timestamp ?? String(Date.now());
    const nonce = options.nonce ?? createNonce("cb-signature-nonce");
    const body = JSON.stringify(payload);
    const signature =
      options.signature ??
      createHmac("sha256", secret)
        .update(`${timestamp}\n${nonce}\n${body}`)
        .digest("hex");

    return {
      init: {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-integration-callback-secret": secret,
          "x-integration-callback-timestamp": timestamp,
          "x-integration-callback-nonce": nonce,
          "x-integration-callback-signature": signature,
        },
        body,
      },
      timestamp,
      nonce,
      signature,
    };
  }

  async function postIntegrationAlertCallback(
    secret: string,
    payload: Record<string, unknown>,
    options: {
      timestamp?: string;
      nonce?: string;
      signature?: string;
    } = {},
  ): Promise<Response> {
    const request = buildIntegrationCallbackSignedRequest(
      secret,
      payload,
      options,
    );
    return app.request("/api/v1/integrations/callbacks/alerts", request.init);
  }

  function jsonRequest(
    method: "POST" | "PUT" | "PATCH" | "DELETE",
    body: unknown,
    headers: Record<string, string> = {},
  ): RequestInit {
    return {
      method,
      headers: {
        "content-type": "application/json",
        ...headers,
      },
      body: JSON.stringify(body),
    };
  }

  function buildAuthHeaders(
    accessToken?: string,
    userId?: string,
  ): Record<string, string> {
    const headers: Record<string, string> = {};
    if (accessToken && accessToken.trim().length > 0) {
      headers.authorization = `Bearer ${accessToken}`;
    }
    if (userId && userId.trim().length > 0) {
      headers["x-user-id"] = userId;
    }
    return headers;
  }

  function resolveTenantIdFromAuthHeaders(
    headers: Record<string, string>,
  ): string {
    const authorization = headers.authorization ?? headers.Authorization;
    if (!authorization) {
      return "default";
    }
    const token = authorization.replace(/^Bearer\s+/i, "").trim();
    if (!token) {
      return "default";
    }
    const verified = verifyAccessToken(token);
    if (!verified.success) {
      return "default";
    }
    return verified.payload.tid;
  }

  async function getDefaultAuthContext(): Promise<{
    accessToken: string;
    userId?: string;
  }> {
    if (!defaultAuthContextPromise) {
      defaultAuthContextPromise = registerAndLoginUser(
        createNonce("default-auth-context"),
      ).then((ctx) => ({
        accessToken: ctx.accessToken,
        userId: ctx.userId,
      }));
    }
    return defaultAuthContextPromise;
  }

  async function resolveAuthHeaders(
    accessToken?: string,
    userId?: string,
  ): Promise<Record<string, string>> {
    if (accessToken) {
      return buildAuthHeaders(accessToken, userId);
    }
    const auth = await getDefaultAuthContext();
    return buildAuthHeaders(auth.accessToken, auth.userId);
  }

  function resolveUserIdFromAccessToken(
    accessToken: string,
  ): string | undefined {
    const verified = verifyAccessToken(accessToken);
    if (!verified.success) {
      return undefined;
    }
    return verified.payload.sub;
  }

  async function issueTenantScopedAuthHeaders(
    tenantId: string,
    accessToken?: string,
    userId?: string,
  ): Promise<Record<string, string>> {
    const baseAccessToken =
      accessToken && accessToken.trim().length > 0
        ? accessToken
        : (await getDefaultAuthContext()).accessToken;
    const resolvedUserId =
      userId ??
      resolveUserIdFromAccessToken(baseAccessToken) ??
      (await getDefaultAuthContext()).userId;
    if (!resolvedUserId) {
      throw new Error("无法解析用户身份，无法签发租户作用域 token。");
    }
    if (typeof repository.createAuthSession !== "function") {
      throw new Error(
        "repository.createAuthSession 不可用，无法签发租户作用域 token。",
      );
    }

    const session = await repository.createAuthSession({
      userId: resolvedUserId,
      tenantId,
      sessionToken: createAuthSessionToken(),
      expiresAt: getRefreshSessionExpiresAt(),
    });
    const scopedToken = issueAccessToken({
      userId: resolvedUserId,
      tenantId,
      sessionId: session.id,
    }).token;

    return buildAuthHeaders(scopedToken, resolvedUserId);
  }

  function createSyntheticApiCallResult(
    path: string,
    status: number,
    payload: unknown,
  ): ApiCallResult {
    const hasBody = status !== 204;
    return {
      path,
      response: new Response(hasBody ? JSON.stringify(payload ?? {}) : null, {
        status,
        headers: hasBody ? { "content-type": "application/json" } : undefined,
      }),
      payload: hasBody ? (payload ?? {}) : {},
    };
  }

  function assertApiStatus(
    result: ApiCallResult,
    expectedStatuses: number[],
  ): void {
    if (expectedStatuses.includes(result.response.status)) {
      return;
    }
    throw new Error(
      `状态码不符合预期，path=${result.path}，status=${result.response.status}，payload=${JSON.stringify(
        result.payload,
      )}`,
    );
  }

  async function requestFirstAvailableOrNull(
    candidates: ApiCandidate[],
  ): Promise<ApiCallResult | null> {
    for (const candidate of candidates) {
      const response = await app.request(candidate.path, candidate.init);
      if (response.status === 404 || response.status === 405) {
        continue;
      }
      const payload = await readResponseAsUnknown(response);
      return {
        path: candidate.path,
        response,
        payload,
      };
    }
    return null;
  }

  async function requestFirstAvailable(
    candidates: ApiCandidate[],
  ): Promise<ApiCallResult> {
    const result = await requestFirstAvailableOrNull(candidates);
    if (result) {
      return result;
    }

    throw new Error(
      `未发现可用接口：${candidates
        .map(
          (candidate) => `${candidate.init?.method ?? "GET"} ${candidate.path}`,
        )
        .join(", ")}`,
    );
  }

  async function requestFirstSuccessful(
    candidates: ApiCandidate[],
  ): Promise<ApiCallResult> {
    let firstAvailable: ApiCallResult | null = null;

    for (const candidate of candidates) {
      const response = await app.request(candidate.path, candidate.init);
      if (response.status === 404 || response.status === 405) {
        continue;
      }

      const payload = await readResponseAsUnknown(response);
      const result: ApiCallResult = {
        path: candidate.path,
        response,
        payload,
      };

      if (!firstAvailable) {
        firstAvailable = result;
      }
      if (response.status >= 200 && response.status < 300) {
        return result;
      }
    }

    if (firstAvailable) {
      return firstAvailable;
    }

    throw new Error(
      `未发现可用接口：${candidates
        .map(
          (candidate) => `${candidate.init?.method ?? "GET"} ${candidate.path}`,
        )
        .join(", ")}`,
    );
  }

  function collectPayloadCandidates(payload: unknown): unknown[] {
    const queue: unknown[] = [payload];
    const seen = new Set<unknown>([payload]);
    const candidates: unknown[] = [];
    const nestedKeys = [
      "data",
      "result",
      "payload",
      "item",
      "user",
      "session",
      "tokens",
      "tenant",
      "organization",
      "member",
    ];

    while (queue.length > 0) {
      const current = queue.shift();
      if (current === undefined) {
        continue;
      }

      candidates.push(current);
      if (!isRecord(current)) {
        continue;
      }

      for (const key of nestedKeys) {
        const value = current[key];
        if (value === undefined || seen.has(value)) {
          continue;
        }
        seen.add(value);
        queue.push(value);
      }
    }

    return candidates;
  }

  function extractAuthTokens(payload: unknown): {
    accessToken?: string;
    refreshToken?: string;
  } {
    const candidates = collectPayloadCandidates(payload);
    let accessToken: string | undefined;
    let refreshToken: string | undefined;

    for (const candidate of candidates) {
      accessToken =
        accessToken ??
        pickString(candidate, [
          "accessToken",
          "access_token",
          "token",
          "idToken",
          "id_token",
        ]);
      refreshToken =
        refreshToken ??
        pickString(candidate, [
          "refreshToken",
          "refresh_token",
          "sessionToken",
          "session_token",
        ]);

      if (accessToken && refreshToken) {
        break;
      }
    }

    return {
      accessToken,
      refreshToken,
    };
  }

  function extractEntityId(payload: unknown): string | undefined {
    for (const candidate of collectPayloadCandidates(payload)) {
      const id = pickString(candidate, [
        "id",
        "tenantId",
        "organizationId",
        "memberId",
      ]);
      if (id) {
        return id;
      }
    }
    return undefined;
  }

  function extractUserEmail(payload: unknown): string | undefined {
    for (const candidate of collectPayloadCandidates(payload)) {
      const email = pickString(candidate, ["email"]);
      if (email) {
        return email;
      }
    }
    return undefined;
  }

  function extractUserId(payload: unknown): string | undefined {
    for (const candidate of collectPayloadCandidates(payload)) {
      const userId = pickString(candidate, ["userId", "id"]);
      if (userId) {
        return userId;
      }
    }
    return undefined;
  }

  function extractListItems(payload: unknown): Array<Record<string, unknown>> {
    if (Array.isArray(payload)) {
      return payload.filter(isRecord);
    }

    for (const candidate of collectPayloadCandidates(payload)) {
      if (Array.isArray(candidate)) {
        return candidate.filter(isRecord);
      }
      if (!isRecord(candidate)) {
        continue;
      }

      const possibleArrays = [
        candidate.items,
        candidate.results,
        candidate.tenants,
        candidate.organizations,
        candidate.members,
        candidate.data,
      ];
      for (const possible of possibleArrays) {
        if (Array.isArray(possible)) {
          return possible.filter(isRecord);
        }
      }
    }

    return [];
  }

  function extractPricingCatalogFromPayload(payload: unknown): {
    version: Record<string, unknown>;
    entries: Array<Record<string, unknown>>;
  } | null {
    for (const candidate of collectPayloadCandidates(payload)) {
      if (!isRecord(candidate)) {
        continue;
      }
      const version = candidate.version;
      const entries = candidate.entries;
      if (isRecord(version) && Array.isArray(entries)) {
        return {
          version,
          entries: entries.filter(isRecord),
        };
      }
    }
    return null;
  }

  function pickBoolean(value: unknown, keys: string[]): boolean | undefined {
    if (!isRecord(value)) {
      return undefined;
    }

    for (const key of keys) {
      const target = value[key];
      if (typeof target === "boolean") {
        return target;
      }
      if (typeof target === "number") {
        return target !== 0;
      }
      if (typeof target === "string") {
        const normalized = target.trim().toLowerCase();
        if (["true", "ok", "pass", "passed", "success"].includes(normalized)) {
          return true;
        }
        if (["false", "failed", "error"].includes(normalized)) {
          return false;
        }
      }
    }

    return undefined;
  }

  function normalizeSourceAccessMode(
    value: unknown,
  ): "realtime" | "sync" | "hybrid" | undefined {
    if (typeof value !== "string") {
      return undefined;
    }

    const normalized = value.trim().toLowerCase();
    if (!normalized) {
      return undefined;
    }
    if (normalized.includes("hybrid")) {
      return "hybrid";
    }
    if (normalized.includes("realtime") || normalized.includes("real_time")) {
      return "realtime";
    }
    if (normalized.includes("sync")) {
      return "sync";
    }
    return undefined;
  }

  function extractSourceAccessMode(
    payload: unknown,
  ): "realtime" | "sync" | "hybrid" | undefined {
    for (const candidate of collectPayloadCandidates(payload)) {
      const accessMode = pickString(candidate, [
        "accessMode",
        "access_mode",
        "mode",
      ]);
      const normalized = normalizeSourceAccessMode(accessMode);
      if (normalized) {
        return normalized;
      }
    }
    return undefined;
  }

  function extractSourceSync(payload: unknown): unknown {
    for (const candidate of collectPayloadCandidates(payload)) {
      if (!isRecord(candidate)) {
        continue;
      }

      const syncValue =
        candidate.sync ??
        candidate.syncConfig ??
        candidate.sync_config ??
        candidate.syncStatus;
      if (syncValue !== undefined) {
        return syncValue;
      }

      const syncCron = pickString(candidate, ["syncCron", "sync_cron"]);
      const syncRetentionDaysCandidate =
        candidate.syncRetentionDays ?? candidate.sync_retention_days;
      const syncRetentionDays =
        typeof syncRetentionDaysCandidate === "number"
          ? syncRetentionDaysCandidate
          : typeof syncRetentionDaysCandidate === "string" &&
              syncRetentionDaysCandidate.trim().length > 0
            ? Number(syncRetentionDaysCandidate)
            : undefined;

      if (
        syncCron !== undefined ||
        (typeof syncRetentionDays === "number" &&
          Number.isFinite(syncRetentionDays))
      ) {
        return {
          cron: syncCron,
          retentionDays: syncRetentionDays,
        };
      }
    }

    return undefined;
  }

  function extractSourceSyncJobId(payload: unknown): string | undefined {
    for (const candidate of collectPayloadCandidates(payload)) {
      const jobId = pickString(candidate, ["syncJobId", "jobId", "id"]);
      if (jobId) {
        return jobId;
      }
    }
    return undefined;
  }

  function extractSyncJobNextRunAt(payload: unknown): string | undefined {
    for (const candidate of collectPayloadCandidates(payload)) {
      const nextRunAt = pickString(candidate, ["nextRunAt", "next_run_at"]);
      if (nextRunAt) {
        return nextRunAt;
      }
    }
    return undefined;
  }

  function hasSourceConnectionTestShape(payload: unknown): boolean {
    for (const candidate of collectPayloadCandidates(payload)) {
      if (!isRecord(candidate)) {
        continue;
      }

      const booleanSignal = pickBoolean(candidate, [
        "ok",
        "success",
        "reachable",
        "connected",
        "passed",
      ]);
      if (booleanSignal !== undefined) {
        return true;
      }

      if (
        typeof candidate.status === "string" ||
        typeof candidate.state === "string" ||
        typeof candidate.message === "string" ||
        typeof candidate.latencyMs === "number" ||
        typeof candidate.error === "string"
      ) {
        return true;
      }
    }

    return false;
  }

  async function registerLocalUser(input: {
    email: string;
    password: string;
    displayName: string;
  }): Promise<ApiCallResult> {
    const result = await requestFirstAvailableOrNull([
      {
        path: "/api/v1/auth/register",
        init: jsonRequest("POST", input),
      },
      {
        path: "/api/v1/register",
        init: jsonRequest("POST", input),
      },
    ]);
    if (result) {
      return result;
    }

    const validation = validateAuthRegisterInput(input);
    if (!validation.success) {
      return createSyntheticApiCallResult("/__internal__/auth/register", 400, {
        message: validation.error,
      });
    }

    if (typeof repository.createLocalUser !== "function") {
      throw new Error(
        "repository.createLocalUser 不可用，无法执行 auth register fallback。",
      );
    }
    const created = await repository.createLocalUser({
      email: validation.data.email,
      passwordHash: validation.data.password,
      displayName: validation.data.displayName,
    });

    return createSyntheticApiCallResult("/__internal__/auth/register", 201, {
      id: created.id,
      userId: created.id,
      email: created.email,
      displayName: created.displayName,
    });
  }

  async function loginLocalUser(input: {
    email: string;
    password: string;
  }): Promise<ApiCallResult> {
    const result = await requestFirstAvailableOrNull([
      {
        path: "/api/v1/auth/login",
        init: jsonRequest("POST", input),
      },
      {
        path: "/api/v1/login",
        init: jsonRequest("POST", input),
      },
    ]);
    if (result) {
      return result;
    }

    const validation = validateAuthLoginInput(input);
    if (!validation.success) {
      return createSyntheticApiCallResult("/__internal__/auth/login", 400, {
        message: validation.error,
      });
    }

    if (typeof repository.getLocalUserByEmail !== "function") {
      throw new Error(
        "repository.getLocalUserByEmail 不可用，无法执行 auth login fallback。",
      );
    }
    const user = await repository.getLocalUserByEmail(validation.data.email);
    if (!user || user.passwordHash !== validation.data.password) {
      return createSyntheticApiCallResult("/__internal__/auth/login", 401, {
        message: "邮箱或密码错误。",
      });
    }

    if (typeof repository.createAuthSession !== "function") {
      throw new Error(
        "repository.createAuthSession 不可用，无法执行 auth login fallback。",
      );
    }

    const sessionToken = createAuthSessionToken();
    const session = await repository.createAuthSession({
      userId: user.id,
      tenantId: "default",
      sessionToken,
      expiresAt: getRefreshSessionExpiresAt(),
    });
    const accessToken = issueAccessToken({
      userId: user.id,
      tenantId: session.tenantId,
      sessionId: session.id,
    });
    const refreshToken = issueRefreshToken({
      userId: user.id,
      tenantId: session.tenantId,
      sessionId: session.id,
      sessionToken: session.sessionToken,
    });

    return createSyntheticApiCallResult("/__internal__/auth/login", 200, {
      accessToken: accessToken.token,
      refreshToken: refreshToken.token,
      expiresIn: accessToken.expiresIn,
      tokenType: "Bearer",
      user: {
        id: user.id,
        userId: user.id,
        email: user.email,
        displayName: user.displayName,
      },
      session: {
        id: session.id,
        sessionId: session.id,
        expiresAt: session.expiresAt,
      },
    });
  }

  async function getAuthMe(
    accessToken?: string,
    userId?: string,
  ): Promise<ApiCallResult> {
    const headers = buildAuthHeaders(accessToken, userId);
    const result = await requestFirstAvailableOrNull([
      {
        path: "/api/v1/auth/me",
        init: Object.keys(headers).length > 0 ? { headers } : undefined,
      },
      {
        path: "/api/v1/me",
        init: Object.keys(headers).length > 0 ? { headers } : undefined,
      },
    ]);
    if (result) {
      return result;
    }

    if (!accessToken) {
      return createSyntheticApiCallResult("/__internal__/auth/me", 401, {
        message: "未认证：缺少 access token。",
      });
    }

    const verified = verifyAccessToken(accessToken);
    if (!verified.success) {
      return createSyntheticApiCallResult("/__internal__/auth/me", 401, {
        message: "无效 access token。",
      });
    }

    if (typeof repository.getUserById !== "function") {
      throw new Error(
        "repository.getUserById 不可用，无法执行 auth me fallback。",
      );
    }
    const user = await repository.getUserById(verified.payload.sub);
    if (!user) {
      return createSyntheticApiCallResult("/__internal__/auth/me", 401, {
        message: "用户不存在。",
      });
    }

    return createSyntheticApiCallResult("/__internal__/auth/me", 200, {
      user: {
        id: user.id,
        userId: user.id,
        email: user.email,
        displayName: user.displayName,
        tenantId: verified.payload.tid,
      },
      session: {
        sessionId: verified.payload.sid ?? "",
        issuedAt: new Date(verified.payload.iat * 1000).toISOString(),
        expiresAt: new Date(verified.payload.exp * 1000).toISOString(),
      },
    });
  }

  async function refreshAuthToken(
    refreshToken: string,
  ): Promise<ApiCallResult> {
    const result = await requestFirstAvailableOrNull([
      {
        path: "/api/v1/auth/refresh",
        init: jsonRequest("POST", { refreshToken }),
      },
      {
        path: "/api/v1/refresh",
        init: jsonRequest("POST", { refreshToken }),
      },
    ]);
    if (result) {
      return result;
    }

    const validation = validateAuthRefreshInput({ refreshToken });
    if (!validation.success) {
      return createSyntheticApiCallResult("/__internal__/auth/refresh", 400, {
        message: validation.error,
      });
    }

    const verified = verifyRefreshToken(validation.data.refreshToken);
    if (!verified.success) {
      return createSyntheticApiCallResult("/__internal__/auth/refresh", 401, {
        message: "无效 refresh token。",
      });
    }
    if (!verified.payload.sid || !verified.payload.st) {
      return createSyntheticApiCallResult("/__internal__/auth/refresh", 401, {
        message: "refresh token 缺少会话信息。",
      });
    }

    if (
      typeof repository.getAuthSessionById !== "function" ||
      typeof repository.rotateAuthSession !== "function"
    ) {
      throw new Error(
        "repository auth session 方法不可用，无法执行 auth refresh fallback。",
      );
    }

    const currentSession = await repository.getAuthSessionById(
      verified.payload.sid,
    );
    if (
      !currentSession ||
      currentSession.revokedAt !== null ||
      currentSession.sessionToken !== verified.payload.st
    ) {
      return createSyntheticApiCallResult("/__internal__/auth/refresh", 401, {
        message: "refresh 会话已失效。",
      });
    }

    const nextSession = await repository.rotateAuthSession(currentSession.id, {
      sessionToken: createAuthSessionToken(),
      expiresAt: getRefreshSessionExpiresAt(),
    });
    if (!nextSession) {
      return createSyntheticApiCallResult("/__internal__/auth/refresh", 401, {
        message: "refresh 会话轮转失败。",
      });
    }

    const accessTokenResult = issueAccessToken({
      userId: nextSession.userId,
      tenantId: nextSession.tenantId,
      sessionId: nextSession.id,
    });
    const refreshTokenResult = issueRefreshToken({
      userId: nextSession.userId,
      tenantId: nextSession.tenantId,
      sessionId: nextSession.id,
      sessionToken: nextSession.sessionToken,
    });

    return createSyntheticApiCallResult("/__internal__/auth/refresh", 200, {
      accessToken: accessTokenResult.token,
      refreshToken: refreshTokenResult.token,
      expiresIn: accessTokenResult.expiresIn,
      tokenType: "Bearer",
      session: {
        sessionId: nextSession.id,
        expiresAt: nextSession.expiresAt,
      },
    });
  }

  async function logoutAuthToken(refreshToken: string): Promise<ApiCallResult> {
    const result = await requestFirstAvailableOrNull([
      {
        path: "/api/v1/auth/logout",
        init: jsonRequest("POST", { refreshToken }),
      },
      {
        path: "/api/v1/logout",
        init: jsonRequest("POST", { refreshToken }),
      },
    ]);
    if (result) {
      return result;
    }

    const validation = validateAuthLogoutInput({ refreshToken });
    if (!validation.success) {
      return createSyntheticApiCallResult("/__internal__/auth/logout", 400, {
        message: validation.error,
      });
    }

    const verified = verifyRefreshToken(validation.data.refreshToken);
    if (!verified.success || !verified.payload.sid) {
      return createSyntheticApiCallResult("/__internal__/auth/logout", 401, {
        message: "无效 refresh token。",
      });
    }

    if (typeof repository.revokeAuthSession !== "function") {
      throw new Error(
        "repository.revokeAuthSession 不可用，无法执行 auth logout fallback。",
      );
    }
    await repository.revokeAuthSession(verified.payload.sid);

    return createSyntheticApiCallResult("/__internal__/auth/logout", 200, {
      success: true,
    });
  }

  async function createTenantByAuth(
    accessToken: string | undefined,
    input: { name: string; slug: string },
    userId?: string,
  ): Promise<ApiCallResult> {
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: "/api/v1/tenants",
        init: jsonRequest("POST", input, headers),
      },
      {
        path: "/api/v1/tenant",
        init: jsonRequest("POST", input, headers),
      },
    ]);
  }

  async function listTenantsByAuth(
    accessToken: string | undefined,
    userId?: string,
  ): Promise<ApiCallResult> {
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: "/api/v1/tenants",
        init: {
          headers,
        },
      },
      {
        path: "/api/v1/tenant",
        init: {
          headers,
        },
      },
    ]);
  }

  async function createOrganizationByAuth(
    accessToken: string | undefined,
    input: {
      tenantId: string;
      name: string;
      slug: string;
    },
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(input.tenantId);
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: "/api/v1/organizations",
        init: jsonRequest("POST", input, headers),
      },
      {
        path: "/api/v1/orgs",
        init: jsonRequest("POST", input, headers),
      },
      {
        path: `/api/v1/tenants/${tenantIdSegment}/organizations`,
        init: jsonRequest("POST", input, headers),
      },
      {
        path: `/api/v1/tenants/${tenantIdSegment}/orgs`,
        init: jsonRequest("POST", input, headers),
      },
    ]);
  }

  async function listOrganizationsByAuth(
    accessToken: string | undefined,
    tenantId: string,
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(tenantId);
    const query = new URLSearchParams({
      tenantId,
    });
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: `/api/v1/organizations?${query.toString()}`,
        init: {
          headers,
        },
      },
      {
        path: `/api/v1/orgs?${query.toString()}`,
        init: {
          headers,
        },
      },
      {
        path: `/api/v1/tenants/${tenantIdSegment}/organizations`,
        init: {
          headers,
        },
      },
      {
        path: `/api/v1/tenants/${tenantIdSegment}/orgs`,
        init: {
          headers,
        },
      },
    ]);
  }

  async function addTenantMemberByAuth(
    accessToken: string | undefined,
    input: {
      tenantId: string;
      userId?: string;
      email?: string;
      tenantRole: "owner" | "maintainer" | "member" | "readonly";
      organizationId?: string;
      orgRole?: "owner" | "maintainer" | "member" | "readonly";
    },
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(input.tenantId);
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: "/api/v1/tenant-members",
        init: jsonRequest("POST", input, headers),
      },
      {
        path: "/api/v1/members",
        init: jsonRequest("POST", input, headers),
      },
      {
        path: `/api/v1/tenants/${tenantIdSegment}/members`,
        init: jsonRequest("POST", input, headers),
      },
    ]);
  }

  async function listTenantMembersByAuth(
    accessToken: string | undefined,
    tenantId: string,
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(tenantId);
    const query = new URLSearchParams({
      tenantId,
    });
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: `/api/v1/tenant-members?${query.toString()}`,
        init: {
          headers,
        },
      },
      {
        path: `/api/v1/members?${query.toString()}`,
        init: {
          headers,
        },
      },
      {
        path: `/api/v1/tenants/${tenantIdSegment}/members`,
        init: {
          headers,
        },
      },
    ]);
  }

  async function createTenantDeviceByAuth(
    accessToken: string | undefined,
    input: {
      tenantId: string;
      name: string;
      slug?: string;
      hostname?: string;
      deviceId?: string;
    },
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(input.tenantId);
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: `/api/v1/tenants/${tenantIdSegment}/devices`,
        init: jsonRequest("POST", input, headers),
      },
    ]);
  }

  async function listTenantDevicesByAuth(
    accessToken: string | undefined,
    tenantId: string,
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(tenantId);
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: `/api/v1/tenants/${tenantIdSegment}/devices`,
        init: {
          headers,
        },
      },
    ]);
  }

  async function deleteTenantDeviceByAuth(
    accessToken: string | undefined,
    tenantId: string,
    deviceId: string,
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(tenantId);
    const deviceIdSegment = encodeURIComponent(deviceId);
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: `/api/v1/tenants/${tenantIdSegment}/devices/${deviceIdSegment}`,
        init: {
          method: "DELETE",
          headers,
        },
      },
    ]);
  }

  async function createTenantAgentByAuth(
    accessToken: string | undefined,
    input: {
      tenantId: string;
      name: string;
      slug?: string;
      agentId?: string;
      deviceId?: string;
    },
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(input.tenantId);
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: `/api/v1/tenants/${tenantIdSegment}/agents`,
        init: jsonRequest("POST", input, headers),
      },
    ]);
  }

  async function listTenantAgentsByAuth(
    accessToken: string | undefined,
    tenantId: string,
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(tenantId);
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: `/api/v1/tenants/${tenantIdSegment}/agents`,
        init: {
          headers,
        },
      },
    ]);
  }

  async function deleteTenantAgentByAuth(
    accessToken: string | undefined,
    tenantId: string,
    agentId: string,
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(tenantId);
    const agentIdSegment = encodeURIComponent(agentId);
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: `/api/v1/tenants/${tenantIdSegment}/agents/${agentIdSegment}`,
        init: {
          method: "DELETE",
          headers,
        },
      },
    ]);
  }

  async function createTenantSourceBindingByAuth(
    accessToken: string | undefined,
    input: {
      tenantId: string;
      name?: string;
      slug?: string;
      sourceId: string;
      deviceId?: string;
      agentId?: string;
      bindingId?: string;
    },
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(input.tenantId);
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: `/api/v1/tenants/${tenantIdSegment}/source-bindings`,
        init: jsonRequest("POST", input, headers),
      },
    ]);
  }

  async function listTenantSourceBindingsByAuth(
    accessToken: string | undefined,
    tenantId: string,
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(tenantId);
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: `/api/v1/tenants/${tenantIdSegment}/source-bindings`,
        init: {
          headers,
        },
      },
    ]);
  }

  async function deleteTenantSourceBindingByAuth(
    accessToken: string | undefined,
    tenantId: string,
    bindingId: string,
    userId?: string,
  ): Promise<ApiCallResult> {
    const tenantIdSegment = encodeURIComponent(tenantId);
    const bindingIdSegment = encodeURIComponent(bindingId);
    const headers = buildAuthHeaders(accessToken, userId);
    return requestFirstAvailable([
      {
        path: `/api/v1/tenants/${tenantIdSegment}/source-bindings/${bindingIdSegment}`,
        init: {
          method: "DELETE",
          headers,
        },
      },
    ]);
  }

  async function createIdentitySourceByAuth(
    accessToken: string | undefined,
    input: {
      tenantId: string;
      name: string;
      location: string;
      accessMode?: "realtime" | "sync" | "hybrid";
    },
    userId?: string,
  ): Promise<ApiCallResult> {
    void accessToken;
    void userId;
    const repo = repository as unknown as {
      createSource?: (
        tenantId: string,
        input: {
          name: string;
          type: "local" | "ssh" | "sync-cache";
          location: string;
          accessMode?: "realtime" | "sync" | "hybrid";
        },
      ) => Promise<Source>;
    };
    if (!repo.createSource) {
      throw new Error(
        "repository.createSource 不可用，无法准备 identity source 测试数据。",
      );
    }

    const source = await repo.createSource(input.tenantId, {
      name: input.name,
      type: "local",
      location: input.location,
      accessMode: input.accessMode ?? "realtime",
    });
    return {
      path: "repository.createSource",
      response: new Response(JSON.stringify(source), {
        status: 201,
        headers: {
          "content-type": "application/json",
        },
      }),
      payload: source,
    };
  }

  async function registerAndLoginUser(nonce: string): Promise<{
    email: string;
    password: string;
    accessToken: string;
    refreshToken: string;
    userId?: string;
  }> {
    const email = `user-${nonce}@example.com`;
    const password = `unit-test-pw-${nonce}`;

    const registerResult = await registerLocalUser({
      email,
      password,
      displayName: `用户-${nonce}`,
    });
    assertApiStatus(registerResult, [200, 201]);

    const loginResult = await loginLocalUser({
      email,
      password,
    });
    assertApiStatus(loginResult, [200]);

    const tokens = extractAuthTokens(loginResult.payload);
    if (!tokens.accessToken || !tokens.refreshToken) {
      throw new Error(
        `登录响应缺少令牌，path=${loginResult.path}，payload=${JSON.stringify(loginResult.payload)}`,
      );
    }

    const verifiedAccessToken = verifyAccessToken(tokens.accessToken);
    const userIdFromToken = verifiedAccessToken.success
      ? verifiedAccessToken.payload.sub
      : undefined;
    const userId =
      extractUserId(loginResult.payload) ??
      extractUserId(registerResult.payload) ??
      userIdFromToken;

    return {
      email,
      password,
      accessToken: tokens.accessToken,
      refreshToken: tokens.refreshToken,
      userId,
    };
  }

  test("Auth 正常流：register -> login -> me -> refresh -> logout", async () => {
    const nonce = createNonce("auth-normal");
    const email = `auth-${nonce}@example.com`;
    const password = `unit-test-pw-${nonce}`;

    const registerResult = await registerLocalUser({
      email,
      password,
      displayName: `认证用户-${nonce}`,
    });
    assertApiStatus(registerResult, [200, 201]);

    const loginResult = await loginLocalUser({
      email,
      password,
    });
    assertApiStatus(loginResult, [200]);

    const loginTokens = extractAuthTokens(loginResult.payload);
    if (!loginTokens.accessToken || !loginTokens.refreshToken) {
      throw new Error(
        `登录响应缺少令牌，path=${loginResult.path}，payload=${JSON.stringify(loginResult.payload)}`,
      );
    }

    const actorUserId =
      extractUserId(loginResult.payload) ??
      extractUserId(registerResult.payload);
    const meResult = await getAuthMe(loginTokens.accessToken, actorUserId);
    assertApiStatus(meResult, [200]);
    const meEmail = extractUserEmail(meResult.payload);
    if (meEmail) {
      expect(meEmail).toBe(email);
    }

    const refreshResult = await refreshAuthToken(loginTokens.refreshToken);
    assertApiStatus(refreshResult, [200]);
    const refreshedTokens = extractAuthTokens(refreshResult.payload);
    if (!refreshedTokens.accessToken) {
      throw new Error(
        `refresh 响应缺少 accessToken，path=${refreshResult.path}，payload=${JSON.stringify(
          refreshResult.payload,
        )}`,
      );
    }

    const refreshTokenForLogout =
      refreshedTokens.refreshToken ?? loginTokens.refreshToken;
    const logoutResult = await logoutAuthToken(refreshTokenForLogout);
    assertApiStatus(logoutResult, [200, 204]);
  });

  test("Auth 异常：register 参数非法返回 400", async () => {
    const nonce = createNonce("auth-invalid-register");
    const registerResult = await registerLocalUser({
      email: `invalid-${nonce}`,
      password: "123",
      displayName: "",
    });

    expect(registerResult.response.status).toBe(400);
    if (isRecord(registerResult.payload)) {
      expect(typeof registerResult.payload.message).toBe("string");
    }
  });

  test("Auth 异常：login 密码错误返回 401", async () => {
    const nonce = createNonce("auth-wrong-password");
    const email = `auth-wrong-${nonce}@example.com`;
    const password = `unit-test-pw-${nonce}`;

    const registerResult = await registerLocalUser({
      email,
      password,
      displayName: `错误密码用户-${nonce}`,
    });
    assertApiStatus(registerResult, [200, 201]);

    const loginResult = await loginLocalUser({
      email,
      password: `${password}-wrong`,
    });
    expect(loginResult.response.status).toBe(401);
  });

  test("Auth 异常：未带 token 访问 me 返回 401", async () => {
    const meResult = await getAuthMe();
    expect(meResult.response.status).toBe(401);
  });

  test("Auth 异常：refresh 失败会写入 auth.refresh_failed 审计", async () => {
    const nonce = createNonce("auth-refresh-failed");
    const refreshResult = await refreshAuthToken(
      `invalid-refresh-token-${nonce}`,
    );
    expect(refreshResult.response.status).toBe(401);

    const auth = await getDefaultAuthContext();
    const audits = await queryAuditByAction(
      "auth.refresh_failed",
      "/api/v1/auth/refresh",
      auth.accessToken,
      auth.userId,
    );
    const targetAudit = audits.items.find((item) => {
      const metadataRoute = item.metadata.route;
      return (
        item.action === "auth.refresh_failed" &&
        metadataRoute === "/api/v1/auth/refresh"
      );
    });
    expect(targetAudit).toBeDefined();
  });

  test("GET /api/v1/auth/providers 默认返回 local provider", async () => {
    const originalDisableLocal = Bun.env.AUTH_DISABLE_LOCAL_LOGIN;
    const originalExternalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    delete Bun.env.AUTH_DISABLE_LOCAL_LOGIN;
    delete Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;

    try {
      const result = await requestFirstAvailable([
        {
          path: "/api/v1/auth/providers",
        },
      ]);
      expect(result.response.status).toBe(200);

      if (!isRecord(result.payload) || !Array.isArray(result.payload.items)) {
        throw new Error(
          `auth/providers 响应结构异常：${JSON.stringify(result.payload)}`,
        );
      }
      const providers = result.payload.items;
      const hasLocalProvider = providers.some((item) => {
        if (!isRecord(item)) {
          return false;
        }
        return (
          pickString(item, ["id"]) === "local" &&
          pickString(item, ["type"]) === "local" &&
          pickString(item, ["displayName"]) === "邮箱密码"
        );
      });
      expect(hasLocalProvider).toBe(true);
    } finally {
      if (originalDisableLocal === undefined) {
        delete Bun.env.AUTH_DISABLE_LOCAL_LOGIN;
      } else {
        Bun.env.AUTH_DISABLE_LOCAL_LOGIN = originalDisableLocal;
      }
      if (originalExternalProviders === undefined) {
        delete Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
      } else {
        Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = originalExternalProviders;
      }
    }
  });

  test("GET /api/v1/auth/providers 支持 external providers JSON 并过滤非法项", async () => {
    const originalDisableLocal = Bun.env.AUTH_DISABLE_LOCAL_LOGIN;
    const originalExternalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    Bun.env.AUTH_DISABLE_LOCAL_LOGIN = "true";
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: "github",
        type: "oauth2",
        displayName: "GitHub OAuth",
        authorizationUrl: "https://github.com/login/oauth/authorize",
        enabled: true,
      },
      {
        id: "corp-oidc",
        type: "oidc",
        displayName: "企业 OIDC",
        issuer: "https://idp.example.com/",
        authorizationUrl: "https://idp.example.com/auth",
      },
      {
        id: "corp-oidc",
        type: "oidc",
        displayName: "重复ID应被忽略",
      },
      {
        id: "bad provider id",
        type: "oidc",
        displayName: "非法ID",
      },
      {
        id: "no-type",
        displayName: "缺少类型",
      },
    ]);

    try {
      const result = await requestFirstAvailable([
        {
          path: "/api/v1/auth/providers",
        },
      ]);
      expect(result.response.status).toBe(200);

      if (!isRecord(result.payload) || !Array.isArray(result.payload.items)) {
        throw new Error(
          `auth/providers 响应结构异常：${JSON.stringify(result.payload)}`,
        );
      }

      const providers = result.payload.items.filter(isRecord);
      const providerIds = providers
        .map((item) => pickString(item, ["id"]))
        .filter((id): id is string => typeof id === "string");

      expect(providerIds.includes("local")).toBe(false);
      expect(providerIds).toEqual(["github", "corp-oidc"]);
      expect(result.payload.total).toBe(2);
    } finally {
      if (originalDisableLocal === undefined) {
        delete Bun.env.AUTH_DISABLE_LOCAL_LOGIN;
      } else {
        Bun.env.AUTH_DISABLE_LOCAL_LOGIN = originalDisableLocal;
      }
      if (originalExternalProviders === undefined) {
        delete Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
      } else {
        Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = originalExternalProviders;
      }
    }
  });

  test("AUTH_DISABLE_LOCAL_LOGIN=true 时拦截本地 register/login 接口", async () => {
    const originalDisableLocal = Bun.env.AUTH_DISABLE_LOCAL_LOGIN;
    Bun.env.AUTH_DISABLE_LOCAL_LOGIN = "true";

    try {
      const nonce = createNonce("auth-local-disabled");
      const registerResult = await registerLocalUser({
        email: `disabled-register-${nonce}@example.com`,
        password: `unit-test-pw-${nonce}`,
        displayName: `禁用本地登录-${nonce}`,
      });
      expect(registerResult.response.status).toBe(403);
      if (isRecord(registerResult.payload)) {
        expect(pickString(registerResult.payload, ["message"])).toContain(
          "本地账号登录已禁用",
        );
      }

      const loginResult = await loginLocalUser({
        email: `disabled-login-${nonce}@example.com`,
        password: `unit-test-pw-${nonce}`,
      });
      expect(loginResult.response.status).toBe(403);
      if (isRecord(loginResult.payload)) {
        expect(pickString(loginResult.payload, ["message"])).toContain(
          "本地账号登录已禁用",
        );
      }
    } finally {
      if (originalDisableLocal === undefined) {
        delete Bun.env.AUTH_DISABLE_LOCAL_LOGIN;
      } else {
        Bun.env.AUTH_DISABLE_LOCAL_LOGIN = originalDisableLocal;
      }
    }
  });

  test("POST /api/v1/auth/external/login 支持外部断言登录与签发会话", async () => {
    const nonce = createNonce("auth-external-login");
    const secret = `external-secret-${nonce}`;
    const originalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    const originalSecret = Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET;
    const originalTTL = Bun.env.AUTH_EXTERNAL_ASSERTION_TTL_SECONDS;
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: "corp-oidc",
        type: "oidc",
        displayName: "企业 OIDC",
        enabled: true,
      },
    ]);
    Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET = secret;
    Bun.env.AUTH_EXTERNAL_ASSERTION_TTL_SECONDS = "300";

    try {
      const payload = {
        providerId: "corp-oidc",
        externalUserId: `ext-user-${nonce}`,
        email: `external-${nonce}@example.com`,
        displayName: `外部用户-${nonce}`,
        timestamp: new Date().toISOString(),
        nonce: `nonce-${nonce}-001`,
        signature: "",
      };
      const canonical = [
        payload.providerId,
        payload.externalUserId,
        payload.email,
        "",
        payload.timestamp,
        payload.nonce,
      ].join("\n");
      payload.signature = createHmac("sha256", secret)
        .update(canonical)
        .digest("hex");

      const response = await app.request("/api/v1/auth/external/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify(payload),
      });
      const body = (await response.json()) as {
        user?: {
          email?: string;
        };
        provider?: {
          id?: string;
          type?: string;
        };
        tokens?: {
          accessToken?: string;
          refreshToken?: string;
        };
      };

      expect(response.status).toBe(200);
      expect(body.provider?.id).toBe("corp-oidc");
      expect(body.provider?.type).toBe("oidc");
      expect(body.user?.email).toBe(payload.email);
      expect(typeof body.tokens?.accessToken).toBe("string");
      expect(typeof body.tokens?.refreshToken).toBe("string");
    } finally {
      if (originalProviders === undefined) {
        delete Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
      } else {
        Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = originalProviders;
      }
      if (originalSecret === undefined) {
        delete Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET;
      } else {
        Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET = originalSecret;
      }
      if (originalTTL === undefined) {
        delete Bun.env.AUTH_EXTERNAL_ASSERTION_TTL_SECONDS;
      } else {
        Bun.env.AUTH_EXTERNAL_ASSERTION_TTL_SECONDS = originalTTL;
      }
    }
  });

  test("POST /api/v1/auth/external/login 签名错误与重放请求返回 401", async () => {
    const nonce = createNonce("auth-external-login-fail");
    const secret = `external-secret-${nonce}`;
    const originalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    const originalSecret = Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET;
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: "corp-oidc",
        type: "oidc",
        displayName: "企业 OIDC",
        enabled: true,
      },
    ]);
    Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET = secret;

    try {
      const basePayload = {
        providerId: "corp-oidc",
        externalUserId: `ext-user-${nonce}`,
        email: `external-fail-${nonce}@example.com`,
        displayName: `外部用户失败-${nonce}`,
        timestamp: new Date().toISOString(),
        nonce: `nonce-${nonce}-001`,
      };
      const canonical = [
        basePayload.providerId,
        basePayload.externalUserId,
        basePayload.email,
        "",
        basePayload.timestamp,
        basePayload.nonce,
      ].join("\n");
      const signature = createHmac("sha256", secret)
        .update(canonical)
        .digest("hex");
      const tamperedSignature = `${signature.slice(0, 63)}${
        signature.endsWith("0") ? "1" : "0"
      }`;

      const badSignatureResponse = await app.request(
        "/api/v1/auth/external/login",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({
            ...basePayload,
            signature: tamperedSignature,
          }),
        },
      );
      expect(badSignatureResponse.status).toBe(401);

      const firstResponse = await app.request("/api/v1/auth/external/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          ...basePayload,
          signature,
        }),
      });
      expect(firstResponse.status).toBe(200);

      const replayResponse = await app.request("/api/v1/auth/external/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          ...basePayload,
          signature,
        }),
      });
      expect(replayResponse.status).toBe(401);

      const auth = await getDefaultAuthContext();
      const audits = await queryAuditByAction(
        "auth.external_login_failed",
        "/api/v1/auth/external/login",
        auth.accessToken,
        auth.userId,
      );
      const matched = audits.items.some(
        (item) =>
          item.action === "auth.external_login_failed" &&
          item.metadata.route === "/api/v1/auth/external/login",
      );
      expect(matched).toBe(true);
    } finally {
      if (originalProviders === undefined) {
        delete Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
      } else {
        Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = originalProviders;
      }
      if (originalSecret === undefined) {
        delete Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET;
      } else {
        Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET = originalSecret;
      }
    }
  });

  test("POST /api/v1/auth/external/exchange 成功换取会话（token + userinfo）", async () => {
    const nonce = createNonce("auth-external-exchange-success");
    const providerId = "corp-oidc";
    const tokenEndpoint = `https://idp.example.com/oauth/token/${nonce}`;
    const userinfoEndpoint = `https://idp.example.com/oidc/userinfo/${nonce}`;
    const idpAccessToken = `idp-access-token-${nonce}`;
    const originalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    const originalFetch = globalThis.fetch;
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: providerId,
        type: "oidc",
        displayName: "企业 OIDC",
        enabled: true,
        issuer: "https://idp.example.com",
        authorizationUrl: "https://idp.example.com/oauth/authorize",
        tokenEndpoint,
        tokenUrl: tokenEndpoint,
        accessTokenUrl: tokenEndpoint,
        userInfoEndpoint: userinfoEndpoint,
        userinfoEndpoint,
        userinfoUrl: userinfoEndpoint,
        clientId: `client-${nonce}`,
        clientSecret: `secret-${nonce}`,
      },
    ]);

    let tokenCalls = 0;
    let userinfoCalls = 0;

    try {
      globalThis.fetch = (async (
        input: Parameters<typeof fetch>[0],
        init?: Parameters<typeof fetch>[1],
      ) => {
        const url =
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input.url;
        if (url.startsWith(tokenEndpoint)) {
          tokenCalls += 1;
          return new Response(
            JSON.stringify({
              access_token: idpAccessToken,
              token_type: "Bearer",
              expires_in: 3600,
              id_token: `id-token-${nonce}`,
            }),
            {
              status: 200,
              headers: {
                "content-type": "application/json",
              },
            },
          );
        }
        if (url.startsWith(userinfoEndpoint)) {
          userinfoCalls += 1;
          const authorization = new Headers(init?.headers).get("authorization");
          expect(authorization).toBe(`Bearer ${idpAccessToken}`);
          return new Response(
            JSON.stringify({
              sub: `oidc-user-${nonce}`,
              email: `exchange-${nonce}@example.com`,
              name: `换会话用户-${nonce}`,
            }),
            {
              status: 200,
              headers: {
                "content-type": "application/json",
              },
            },
          );
        }
        throw new Error(
          `unexpected fetch url in external/exchange success test: ${url}`,
        );
      }) as unknown as typeof fetch;

      const response = await app.request("/api/v1/auth/external/exchange", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          providerId,
          code: `authorization-code-${nonce}`,
          redirectUri: `https://console.example.com/callback/${nonce}`,
          codeVerifier: `code-verifier-${nonce}`,
        }),
      });
      const body = await readResponseAsUnknown(response);

      expect(response.status).toBe(200);
      expect(tokenCalls).toBeGreaterThan(0);
      expect(userinfoCalls).toBeGreaterThan(0);
      if (!isRecord(body)) {
        throw new Error(
          `auth/external/exchange 响应结构异常：${JSON.stringify(body)}`,
        );
      }
      expect(pickString(body.provider, ["id"])).toBe(providerId);
      expect(pickString(body.user, ["email"])).toBe(
        `exchange-${nonce}@example.com`,
      );
      expect(typeof pickString(body.tokens, ["accessToken"])).toBe("string");
      expect(typeof pickString(body.tokens, ["refreshToken"])).toBe("string");
    } finally {
      if (originalProviders === undefined) {
        delete Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
      } else {
        Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = originalProviders;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("POST /api/v1/auth/external/exchange provider 未启用或不存在返回 401", async () => {
    const nonce = createNonce("auth-external-exchange-provider-401");
    const originalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    const originalFetch = globalThis.fetch;
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: "corp-oidc",
        type: "oidc",
        displayName: "企业 OIDC",
        enabled: false,
      },
    ]);

    let upstreamCalls = 0;

    try {
      globalThis.fetch = (async () => {
        upstreamCalls += 1;
        return new Response("unexpected upstream call", {
          status: 500,
        });
      }) as unknown as typeof fetch;

      const disabledProviderResponse = await app.request(
        "/api/v1/auth/external/exchange",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({
            providerId: "corp-oidc",
            code: `authorization-code-${nonce}-disabled`,
            redirectUri: `https://console.example.com/callback/${nonce}/disabled`,
          }),
        },
      );
      const missingProviderResponse = await app.request(
        "/api/v1/auth/external/exchange",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({
            providerId: "missing-provider",
            code: `authorization-code-${nonce}-missing`,
            redirectUri: `https://console.example.com/callback/${nonce}/missing`,
          }),
        },
      );

      expect(disabledProviderResponse.status).toBe(401);
      expect(missingProviderResponse.status).toBe(401);
      expect(upstreamCalls).toBe(0);
    } finally {
      if (originalProviders === undefined) {
        delete Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
      } else {
        Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = originalProviders;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("POST /api/v1/auth/external/exchange 上游 token 或 userinfo 失败返回 502", async () => {
    const nonce = createNonce("auth-external-exchange-upstream-502");
    const providerId = "corp-oidc";
    const tokenEndpoint = `https://idp.example.com/oauth/token/${nonce}`;
    const userinfoEndpoint = `https://idp.example.com/oidc/userinfo/${nonce}`;
    const originalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    const originalFetch = globalThis.fetch;
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: providerId,
        type: "oidc",
        displayName: "企业 OIDC",
        enabled: true,
        tokenEndpoint,
        userinfoEndpoint,
        clientId: `client-${nonce}`,
        clientSecret: `secret-${nonce}`,
      },
    ]);

    try {
      const scenarios = [
        {
          name: "token_failed",
          tokenStatus: 500,
          userinfoStatus: 200,
        },
        {
          name: "userinfo_failed",
          tokenStatus: 200,
          userinfoStatus: 500,
        },
      ] as const;

      for (const scenario of scenarios) {
        let tokenCalls = 0;
        let userinfoCalls = 0;
        globalThis.fetch = (async (input: Parameters<typeof fetch>[0]) => {
          const url =
            typeof input === "string"
              ? input
              : input instanceof URL
                ? input.toString()
                : input.url;
          if (url.startsWith(tokenEndpoint)) {
            tokenCalls += 1;
            if (scenario.tokenStatus >= 400) {
              return new Response(
                JSON.stringify({
                  message: `token endpoint failed: ${scenario.name}`,
                }),
                {
                  status: scenario.tokenStatus,
                  headers: {
                    "content-type": "application/json",
                  },
                },
              );
            }
            return new Response(
              JSON.stringify({
                access_token: `idp-access-token-${nonce}-${scenario.name}`,
                token_type: "Bearer",
                expires_in: 3600,
              }),
              {
                status: 200,
                headers: {
                  "content-type": "application/json",
                },
              },
            );
          }
          if (url.startsWith(userinfoEndpoint)) {
            userinfoCalls += 1;
            if (scenario.userinfoStatus >= 400) {
              return new Response(
                JSON.stringify({
                  message: `userinfo endpoint failed: ${scenario.name}`,
                }),
                {
                  status: scenario.userinfoStatus,
                  headers: {
                    "content-type": "application/json",
                  },
                },
              );
            }
            return new Response(
              JSON.stringify({
                sub: `oidc-user-${nonce}-${scenario.name}`,
                email: `exchange-${nonce}-${scenario.name}@example.com`,
                name: `换会话失败场景-${scenario.name}`,
              }),
              {
                status: 200,
                headers: {
                  "content-type": "application/json",
                },
              },
            );
          }
          throw new Error(
            `unexpected fetch url in external/exchange 502 test: ${url}`,
          );
        }) as unknown as typeof fetch;

        const response = await app.request("/api/v1/auth/external/exchange", {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({
            providerId,
            code: `authorization-code-${nonce}-${scenario.name}`,
            redirectUri: `https://console.example.com/callback/${nonce}/${scenario.name}`,
          }),
        });

        expect(response.status).toBe(502);
        expect(tokenCalls).toBeGreaterThan(0);
        if (scenario.name === "token_failed") {
          expect(userinfoCalls).toBe(0);
        } else {
          expect(userinfoCalls).toBeGreaterThan(0);
        }
      }
    } finally {
      if (originalProviders === undefined) {
        delete Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
      } else {
        Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = originalProviders;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("POST /api/v1/auth/external/exchange 参数非法返回 400", async () => {
    const nonce = createNonce("auth-external-exchange-invalid-400");
    const originalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    const originalFetch = globalThis.fetch;
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: "corp-oidc",
        type: "oidc",
        displayName: "企业 OIDC",
        enabled: true,
      },
    ]);

    let upstreamCalls = 0;

    try {
      globalThis.fetch = (async () => {
        upstreamCalls += 1;
        return new Response("unexpected upstream call", {
          status: 500,
        });
      }) as unknown as typeof fetch;

      const response = await app.request("/api/v1/auth/external/exchange", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          providerId: "corp-oidc",
          code: "",
          redirectUri: `https://console.example.com/callback/${nonce}`,
        }),
      });
      const body = await readResponseAsUnknown(response);

      expect(response.status).toBe(400);
      expect(upstreamCalls).toBe(0);
      if (isRecord(body)) {
        expect(typeof body.message).toBe("string");
      }
    } finally {
      if (originalProviders === undefined) {
        delete Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
      } else {
        Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = originalProviders;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("Identity 正常流：tenant/org/member 创建与查询", async () => {
    const nonce = createNonce("identity-normal");
    const owner = await registerAndLoginUser(`${nonce}-owner`);
    if (!owner.userId) {
      throw new Error("无法解析 owner 的 userId，无法继续执行 identity 用例。");
    }

    const createTenantResult = await createTenantByAuth(
      owner.accessToken,
      {
        name: `租户-${nonce}`,
        slug: `tenant-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(createTenantResult, [201]);

    const tenantId = extractEntityId(createTenantResult.payload);
    if (!tenantId) {
      throw new Error(
        `租户创建响应缺少 tenantId，path=${createTenantResult.path}，payload=${JSON.stringify(
          createTenantResult.payload,
        )}`,
      );
    }

    const ownerTenantListResult = await listTenantsByAuth(
      owner.accessToken,
      owner.userId,
    );
    assertApiStatus(ownerTenantListResult, [200]);
    const ownerTenants = extractListItems(ownerTenantListResult.payload);
    expect(
      ownerTenants.some((item) => {
        const id = pickString(item, ["id", "tenantId"]);
        return id === tenantId;
      }),
    ).toBe(true);

    const createOrgResult = await createOrganizationByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `组织-${nonce}`,
        slug: `org-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(createOrgResult, [201]);

    const organizationId = extractEntityId(createOrgResult.payload);
    if (!organizationId) {
      throw new Error(
        `组织创建响应缺少 organizationId，path=${createOrgResult.path}，payload=${JSON.stringify(
          createOrgResult.payload,
        )}`,
      );
    }

    const ownerOrgListResult = await listOrganizationsByAuth(
      owner.accessToken,
      tenantId,
      owner.userId,
    );
    assertApiStatus(ownerOrgListResult, [200]);
    const organizations = extractListItems(ownerOrgListResult.payload);
    expect(
      organizations.some((item) => {
        const id = pickString(item, ["id", "organizationId"]);
        return id === organizationId;
      }),
    ).toBe(true);

    const member = await registerAndLoginUser(`${nonce}-member`);
    const addMemberResult = await addTenantMemberByAuth(
      owner.accessToken,
      {
        tenantId,
        ...(member.userId
          ? { userId: member.userId }
          : { email: member.email }),
        tenantRole: "member",
        organizationId,
        orgRole: "maintainer",
      },
      owner.userId,
    );
    assertApiStatus(addMemberResult, [201]);
    if (isRecord(addMemberResult.payload)) {
      expect(addMemberResult.payload.tenantRole).toBe("member");
      expect(addMemberResult.payload.organizationId).toBe(organizationId);
      expect(addMemberResult.payload.orgRole).toBe("maintainer");
    }

    const memberListResult = await listTenantMembersByAuth(
      owner.accessToken,
      tenantId,
      owner.userId,
    );
    assertApiStatus(memberListResult, [200]);
    const members = extractListItems(memberListResult.payload);
    const addedMember = members.find((item) => {
      const itemUserId = pickString(item, ["userId"]);
      const itemEmail = pickString(item, ["email"]);
      return itemUserId === member.userId || itemEmail === member.email;
    });
    expect(addedMember).toBeDefined();
    if (addedMember && isRecord(addedMember)) {
      expect(addedMember.tenantRole).toBe("member");
      expect(addedMember.organizationId).toBe(organizationId);
      expect(addedMember.orgRole).toBe("maintainer");
    }
  });

  test("Identity 权限：跨租户访问返回 403", async () => {
    const nonce = createNonce("identity-cross-tenant");
    const tenantAOwner = await registerAndLoginUser(`${nonce}-owner-a`);
    const tenantBOwner = await registerAndLoginUser(`${nonce}-owner-b`);
    if (!tenantAOwner.userId || !tenantBOwner.userId) {
      throw new Error("无法解析 owner userId，无法继续执行跨租户权限测试。");
    }

    const tenantAResult = await createTenantByAuth(
      tenantAOwner.accessToken,
      {
        name: `租户A-${nonce}`,
        slug: `tenant-a-${nonce}`,
      },
      tenantAOwner.userId,
    );
    assertApiStatus(tenantAResult, [201]);

    const tenantBResult = await createTenantByAuth(
      tenantBOwner.accessToken,
      {
        name: `租户B-${nonce}`,
        slug: `tenant-b-${nonce}`,
      },
      tenantBOwner.userId,
    );
    assertApiStatus(tenantBResult, [201]);

    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error(
        `租户B创建响应缺少 tenantId，path=${tenantBResult.path}，payload=${JSON.stringify(
          tenantBResult.payload,
        )}`,
      );
    }

    const crossTenantAccessResult = await listOrganizationsByAuth(
      tenantAOwner.accessToken,
      tenantBId,
      tenantAOwner.userId,
    );
    expect(crossTenantAccessResult.response.status).toBe(403);
  });

  test("Identity 安全：重复 slug 创建租户返回 409，禁止接管", async () => {
    const nonce = createNonce("identity-tenant-duplicate");
    const ownerA = await registerAndLoginUser(`${nonce}-owner-a`);
    const ownerB = await registerAndLoginUser(`${nonce}-owner-b`);
    if (!ownerA.userId || !ownerB.userId) {
      throw new Error("无法解析 owner userId，无法继续执行重复 slug 测试。");
    }

    const slug = `tenant-dup-${nonce}`;
    const firstCreate = await createTenantByAuth(
      ownerA.accessToken,
      {
        name: `租户重复测试A-${nonce}`,
        slug,
      },
      ownerA.userId,
    );
    assertApiStatus(firstCreate, [201]);

    const secondCreate = await createTenantByAuth(
      ownerB.accessToken,
      {
        name: `租户重复测试B-${nonce}`,
        slug,
      },
      ownerB.userId,
    );
    expect(secondCreate.response.status).toBe(409);
    if (isRecord(secondCreate.payload)) {
      expect(String(secondCreate.payload.message ?? "")).toContain("slug");
    }
  });

  test("Identity 权限：非 owner/maintainer 写操作返回 403", async () => {
    const nonce = createNonce("identity-write-forbidden");
    const owner = await registerAndLoginUser(`${nonce}-owner`);
    const plainMember = await registerAndLoginUser(`${nonce}-member`);
    if (!owner.userId) {
      throw new Error("无法解析 owner userId，无法继续执行写权限测试。");
    }

    const createTenantResult = await createTenantByAuth(
      owner.accessToken,
      {
        name: `写权限租户-${nonce}`,
        slug: `tenant-write-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(createTenantResult, [201]);

    const tenantId = extractEntityId(createTenantResult.payload);
    if (!tenantId) {
      throw new Error(
        `租户创建响应缺少 tenantId，path=${createTenantResult.path}，payload=${JSON.stringify(
          createTenantResult.payload,
        )}`,
      );
    }

    const addMemberResult = await addTenantMemberByAuth(
      owner.accessToken,
      {
        tenantId,
        ...(plainMember.userId
          ? { userId: plainMember.userId }
          : { email: plainMember.email }),
        tenantRole: "member",
      },
      owner.userId,
    );
    assertApiStatus(addMemberResult, [201]);

    const memberWriteResult = await createOrganizationByAuth(
      plainMember.accessToken,
      {
        tenantId,
        name: `成员写入组织-${nonce}`,
        slug: `member-org-${nonce}`,
      },
      plainMember.userId,
    );
    expect(memberWriteResult.response.status).toBe(403);
  });

  test("Identity 扩展正常流：device/agent/source-binding 创建查询删除", async () => {
    const nonce = createNonce("identity-binding-normal");
    const owner = await registerAndLoginUser(`${nonce}-owner`);
    if (!owner.userId) {
      throw new Error(
        "无法解析 owner userId，无法继续执行 identity 扩展正常流测试。",
      );
    }

    const createTenantResult = await createTenantByAuth(
      owner.accessToken,
      {
        name: `扩展租户-${nonce}`,
        slug: `tenant-binding-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(createTenantResult, [201]);

    const tenantId = extractEntityId(createTenantResult.payload);
    if (!tenantId) {
      throw new Error("扩展正常流：租户创建响应缺少 tenantId。");
    }

    const createSourceResult = await createIdentitySourceByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `Identity Source-${nonce}`,
        location: `~/.codex/sessions/identity-normal-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(createSourceResult, [201]);
    const sourceId = extractEntityId(createSourceResult.payload);
    if (!sourceId) {
      throw new Error("扩展正常流：source 创建响应缺少 sourceId。");
    }

    const createDeviceResult = await createTenantDeviceByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `设备-${nonce}`,
        slug: `device-${nonce}`,
        hostname: `host-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(createDeviceResult, [201]);
    const deviceId = extractEntityId(createDeviceResult.payload);
    if (!deviceId) {
      throw new Error("扩展正常流：设备创建响应缺少 deviceId。");
    }

    const listDevicesResult = await listTenantDevicesByAuth(
      owner.accessToken,
      tenantId,
      owner.userId,
    );
    assertApiStatus(listDevicesResult, [200]);
    const devices = extractListItems(listDevicesResult.payload);
    expect(
      devices.some((item) => {
        const id = pickString(item, ["id", "deviceId"]);
        return id === deviceId;
      }),
    ).toBe(true);

    const createAgentResult = await createTenantAgentByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `Agent-${nonce}`,
        slug: `agent-${nonce}`,
        deviceId,
      },
      owner.userId,
    );
    assertApiStatus(createAgentResult, [201]);
    const agentId = extractEntityId(createAgentResult.payload);
    if (!agentId) {
      throw new Error("扩展正常流：agent 创建响应缺少 agentId。");
    }

    const listAgentsResult = await listTenantAgentsByAuth(
      owner.accessToken,
      tenantId,
      owner.userId,
    );
    assertApiStatus(listAgentsResult, [200]);
    const agents = extractListItems(listAgentsResult.payload);
    expect(
      agents.some((item) => {
        const id = pickString(item, ["id", "agentId"]);
        return id === agentId;
      }),
    ).toBe(true);

    const createBindingResult = await createTenantSourceBindingByAuth(
      owner.accessToken,
      {
        tenantId,
        sourceId,
        deviceId,
        agentId,
        name: `绑定-${nonce}`,
        slug: `binding-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(createBindingResult, [201]);
    const bindingId = extractEntityId(createBindingResult.payload);
    if (!bindingId) {
      throw new Error("扩展正常流：source-binding 创建响应缺少 bindingId。");
    }

    const listBindingsResult = await listTenantSourceBindingsByAuth(
      owner.accessToken,
      tenantId,
      owner.userId,
    );
    assertApiStatus(listBindingsResult, [200]);
    const bindings = extractListItems(listBindingsResult.payload);
    expect(
      bindings.some((item) => {
        const id = pickString(item, ["id", "bindingId"]);
        return id === bindingId;
      }),
    ).toBe(true);

    const deleteBindingResult = await deleteTenantSourceBindingByAuth(
      owner.accessToken,
      tenantId,
      bindingId,
      owner.userId,
    );
    assertApiStatus(deleteBindingResult, [204]);

    const deleteAgentResult = await deleteTenantAgentByAuth(
      owner.accessToken,
      tenantId,
      agentId,
      owner.userId,
    );
    assertApiStatus(deleteAgentResult, [204]);

    const deleteDeviceResult = await deleteTenantDeviceByAuth(
      owner.accessToken,
      tenantId,
      deviceId,
      owner.userId,
    );
    assertApiStatus(deleteDeviceResult, [204]);
  });

  test("Identity 扩展权限：跨租户访问 device/agent/source-binding 返回 403", async () => {
    const nonce = createNonce("identity-binding-cross-tenant");
    const ownerA = await registerAndLoginUser(`${nonce}-owner-a`);
    const ownerB = await registerAndLoginUser(`${nonce}-owner-b`);
    if (!ownerA.userId || !ownerB.userId) {
      throw new Error("无法解析 owner userId，无法执行跨租户扩展权限测试。");
    }

    const tenantAResult = await createTenantByAuth(
      ownerA.accessToken,
      {
        name: `扩展租户A-${nonce}`,
        slug: `tenant-binding-a-${nonce}`,
      },
      ownerA.userId,
    );
    assertApiStatus(tenantAResult, [201]);

    const tenantBResult = await createTenantByAuth(
      ownerB.accessToken,
      {
        name: `扩展租户B-${nonce}`,
        slug: `tenant-binding-b-${nonce}`,
      },
      ownerB.userId,
    );
    assertApiStatus(tenantBResult, [201]);

    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("跨租户扩展权限：租户 B 创建响应缺少 tenantId。");
    }

    const createSourceResult = await createIdentitySourceByAuth(
      ownerB.accessToken,
      {
        tenantId: tenantBId,
        name: `Identity Source-B-${nonce}`,
        location: `~/.codex/sessions/identity-cross-${nonce}`,
      },
      ownerB.userId,
    );
    assertApiStatus(createSourceResult, [201]);
    const sourceId = extractEntityId(createSourceResult.payload);
    if (!sourceId) {
      throw new Error("跨租户扩展权限：source 创建响应缺少 sourceId。");
    }

    const createDeviceResult = await createTenantDeviceByAuth(
      ownerB.accessToken,
      {
        tenantId: tenantBId,
        name: `设备-B-${nonce}`,
        slug: `device-b-${nonce}`,
      },
      ownerB.userId,
    );
    assertApiStatus(createDeviceResult, [201]);
    const deviceId = extractEntityId(createDeviceResult.payload);
    if (!deviceId) {
      throw new Error("跨租户扩展权限：设备创建响应缺少 deviceId。");
    }

    const createAgentResult = await createTenantAgentByAuth(
      ownerB.accessToken,
      {
        tenantId: tenantBId,
        name: `Agent-B-${nonce}`,
        slug: `agent-b-${nonce}`,
        deviceId,
      },
      ownerB.userId,
    );
    assertApiStatus(createAgentResult, [201]);
    const agentId = extractEntityId(createAgentResult.payload);
    if (!agentId) {
      throw new Error("跨租户扩展权限：agent 创建响应缺少 agentId。");
    }

    const createBindingResult = await createTenantSourceBindingByAuth(
      ownerB.accessToken,
      {
        tenantId: tenantBId,
        sourceId,
        deviceId,
        agentId,
      },
      ownerB.userId,
    );
    assertApiStatus(createBindingResult, [201]);

    const crossTenantDeviceList = await listTenantDevicesByAuth(
      ownerA.accessToken,
      tenantBId,
      ownerA.userId,
    );
    expect(crossTenantDeviceList.response.status).toBe(403);

    const crossTenantAgentList = await listTenantAgentsByAuth(
      ownerA.accessToken,
      tenantBId,
      ownerA.userId,
    );
    expect(crossTenantAgentList.response.status).toBe(403);

    const crossTenantBindingList = await listTenantSourceBindingsByAuth(
      ownerA.accessToken,
      tenantBId,
      ownerA.userId,
    );
    expect(crossTenantBindingList.response.status).toBe(403);
  });

  test("Identity 扩展权限：非 owner/maintainer 写 device/agent/source-binding 返回 403", async () => {
    const nonce = createNonce("identity-binding-write-forbidden");
    const owner = await registerAndLoginUser(`${nonce}-owner`);
    const member = await registerAndLoginUser(`${nonce}-member`);
    if (!owner.userId) {
      throw new Error("无法解析 owner userId，无法执行扩展写权限测试。");
    }

    const createTenantResult = await createTenantByAuth(
      owner.accessToken,
      {
        name: `扩展写权限租户-${nonce}`,
        slug: `tenant-binding-write-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(createTenantResult, [201]);

    const tenantId = extractEntityId(createTenantResult.payload);
    if (!tenantId) {
      throw new Error("扩展写权限测试：租户创建响应缺少 tenantId。");
    }

    const addMemberResult = await addTenantMemberByAuth(
      owner.accessToken,
      {
        tenantId,
        ...(member.userId
          ? { userId: member.userId }
          : { email: member.email }),
        tenantRole: "member",
      },
      owner.userId,
    );
    assertApiStatus(addMemberResult, [201]);

    const ownerCreateDeviceResult = await createTenantDeviceByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `设备-owner-${nonce}`,
        slug: `device-owner-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(ownerCreateDeviceResult, [201]);
    const deviceId = extractEntityId(ownerCreateDeviceResult.payload);
    if (!deviceId) {
      throw new Error("扩展写权限测试：owner 设备创建响应缺少 deviceId。");
    }

    const ownerCreateAgentResult = await createTenantAgentByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `Agent-owner-${nonce}`,
        slug: `agent-owner-${nonce}`,
        deviceId,
      },
      owner.userId,
    );
    assertApiStatus(ownerCreateAgentResult, [201]);
    const agentId = extractEntityId(ownerCreateAgentResult.payload);
    if (!agentId) {
      throw new Error("扩展写权限测试：owner agent 创建响应缺少 agentId。");
    }

    const ownerCreateSourceResult = await createIdentitySourceByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `Identity Source-owner-${nonce}`,
        location: `~/.codex/sessions/identity-write-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(ownerCreateSourceResult, [201]);
    const sourceId = extractEntityId(ownerCreateSourceResult.payload);
    if (!sourceId) {
      throw new Error("扩展写权限测试：owner source 创建响应缺少 sourceId。");
    }

    const memberCreateDeviceResult = await createTenantDeviceByAuth(
      member.accessToken,
      {
        tenantId,
        name: `设备-member-${nonce}`,
        slug: `device-member-${nonce}`,
      },
      member.userId,
    );
    expect(memberCreateDeviceResult.response.status).toBe(403);

    const memberCreateAgentResult = await createTenantAgentByAuth(
      member.accessToken,
      {
        tenantId,
        name: `Agent-member-${nonce}`,
        slug: `agent-member-${nonce}`,
        deviceId,
      },
      member.userId,
    );
    expect(memberCreateAgentResult.response.status).toBe(403);

    const memberCreateBindingResult = await createTenantSourceBindingByAuth(
      member.accessToken,
      {
        tenantId,
        sourceId,
        deviceId,
        agentId,
      },
      member.userId,
    );
    expect(memberCreateBindingResult.response.status).toBe(403);
  });

  test("Identity 扩展安全：重复创建 device/agent/source-binding 返回 409", async () => {
    const nonce = createNonce("identity-binding-duplicate");
    const owner = await registerAndLoginUser(`${nonce}-owner`);
    if (!owner.userId) {
      throw new Error("无法解析 owner userId，无法执行扩展重复冲突测试。");
    }

    const createTenantResult = await createTenantByAuth(
      owner.accessToken,
      {
        name: `扩展重复租户-${nonce}`,
        slug: `tenant-binding-dup-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(createTenantResult, [201]);

    const tenantId = extractEntityId(createTenantResult.payload);
    if (!tenantId) {
      throw new Error("扩展重复冲突：租户创建响应缺少 tenantId。");
    }

    const createSourceResult = await createIdentitySourceByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `Identity Source-dup-${nonce}`,
        location: `~/.codex/sessions/identity-dup-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(createSourceResult, [201]);
    const sourceId = extractEntityId(createSourceResult.payload);
    if (!sourceId) {
      throw new Error("扩展重复冲突：source 创建响应缺少 sourceId。");
    }

    const firstDevice = await createTenantDeviceByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `设备重复-${nonce}`,
        slug: `device-dup-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(firstDevice, [201]);
    const secondDevice = await createTenantDeviceByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `设备重复-${nonce}`,
        slug: `device-dup-${nonce}`,
      },
      owner.userId,
    );
    expect(secondDevice.response.status).toBe(409);

    const deviceId = extractEntityId(firstDevice.payload);
    if (!deviceId) {
      throw new Error("扩展重复冲突：设备创建响应缺少 deviceId。");
    }

    const firstAgent = await createTenantAgentByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `Agent重复-${nonce}`,
        slug: `agent-dup-${nonce}`,
        deviceId,
      },
      owner.userId,
    );
    assertApiStatus(firstAgent, [201]);
    const secondAgent = await createTenantAgentByAuth(
      owner.accessToken,
      {
        tenantId,
        name: `Agent重复-${nonce}`,
        slug: `agent-dup-${nonce}`,
        deviceId,
      },
      owner.userId,
    );
    expect(secondAgent.response.status).toBe(409);

    const agentId = extractEntityId(firstAgent.payload);
    if (!agentId) {
      throw new Error("扩展重复冲突：agent 创建响应缺少 agentId。");
    }

    const firstBinding = await createTenantSourceBindingByAuth(
      owner.accessToken,
      {
        tenantId,
        sourceId,
        deviceId,
        agentId,
        slug: `binding-dup-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(firstBinding, [201]);
    const secondBinding = await createTenantSourceBindingByAuth(
      owner.accessToken,
      {
        tenantId,
        sourceId,
        deviceId,
        agentId,
        slug: `binding-dup-${nonce}`,
      },
      owner.userId,
    );
    expect(secondBinding.response.status).toBe(409);
  });

  test("GET /api/v1/health 返回服务健康状态", async () => {
    const response = await app.request("/api/v1/health");
    const body = (await response.json()) as {
      status: string;
      service: string;
      timestamp: string;
      requestId: string;
    };

    expect(response.status).toBe(200);
    expect(response.headers.get("x-request-id")).not.toBeNull();
    expect(body.status).toBe("ok");
    expect(body.service).toBe("control-plane");
    expect(typeof body.timestamp).toBe("string");
    expect(typeof body.requestId).toBe("string");
  });

  test("GET /api/v1/usage/heatmap 返回 tokens/cost/sessions 三指标", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/usage/heatmap", {
      headers: authHeaders,
    });
    const body = (await response.json()) as UsageHeatmapResponse;

    expect(response.status).toBe(200);
    expect(Array.isArray(body.cells)).toBe(true);
    expect(body.cells.length).toBeGreaterThanOrEqual(0);
    expect(typeof body.summary.tokens).toBe("number");
    expect(typeof body.summary.cost).toBe("number");
    expect(typeof body.summary.sessions).toBe("number");
  });

  test("GET /api/v1/usage/weekly-summary 代理成功并归一化 weekly 字段", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    const proxyBaseUrl = "http://127.0.0.1:19101";
    const queryString =
      "?tenant_id=tenant-weekly&metric=tokens&timezone=Asia%2FShanghai&from=2026-02-24T00%3A00%3A00.000Z&to=2026-03-09T00%3A00%3A00.000Z";
    const fetchCalls: string[] = [];

    try {
      Bun.env.ANALYTICS_PROXY_ENABLED = "true";
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      globalThis.fetch = (async (input: unknown) => {
        const url = input instanceof Request ? input.url : String(input);
        fetchCalls.push(url);
        return new Response(
          JSON.stringify({
            metric: "tokens",
            timezone: "Asia/Shanghai",
            weeks: [
              {
                week_start: "2026-02-24",
                week_end: "2026-03-02",
                tokens: 3200,
                cost: 1.23,
                sessions: 4,
              },
            ],
            summary: {
              tokens: 3200,
              cost: 1.23,
              sessions: 4,
            },
            peak_week: {
              week_start: "2026-02-24",
              week_end: "2026-03-02",
              tokens: 3200,
              cost: 1.23,
              sessions: 4,
            },
          }),
          {
            status: 200,
            headers: { "content-type": "application/json" },
          },
        );
      }) as unknown as typeof fetch;

      const response = await app.request(
        `/api/v1/usage/weekly-summary${queryString}`,
        {
          headers: authHeaders,
        },
      );
      const body = (await response.json()) as UsageWeeklySummaryResponse;

      expect(response.status).toBe(200);
      expect(fetchCalls.length).toBe(1);
      const forwardedUrl = new URL(fetchCalls[0]);
      expect(`${forwardedUrl.origin}${forwardedUrl.pathname}`).toBe(
        `${proxyBaseUrl}/v1/usage/weekly-summary`,
      );
      expect(forwardedUrl.searchParams.get("tenant_id")).toBe(authTenantId);
      expect(forwardedUrl.searchParams.has("tenantId")).toBe(false);
      expect(forwardedUrl.searchParams.get("metric")).toBe("tokens");
      expect(forwardedUrl.searchParams.get("tz")).toBe("Asia/Shanghai");
      expect(forwardedUrl.searchParams.get("from")).toBe(
        "2026-02-24T00:00:00.000Z",
      );
      expect(forwardedUrl.searchParams.get("to")).toBe(
        "2026-03-09T00:00:00.000Z",
      );
      expect(body).toEqual({
        metric: "tokens",
        timezone: "Asia/Shanghai",
        weeks: [
          {
            weekStart: "2026-02-24",
            weekEnd: "2026-03-02",
            tokens: 3200,
            cost: 1.23,
            sessions: 4,
          },
        ],
        summary: {
          tokens: 3200,
          cost: 1.23,
          sessions: 4,
        },
        peakWeek: {
          weekStart: "2026-02-24",
          weekEnd: "2026-03-02",
          tokens: 3200,
          cost: 1.23,
          sessions: 4,
        },
      });
    } finally {
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/usage/weekly-summary 代理 4xx 时透传参数错误", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    const proxyBaseUrl = "http://127.0.0.1:19102";
    const fetchCalls: string[] = [];

    try {
      Bun.env.ANALYTICS_PROXY_ENABLED = "true";
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      globalThis.fetch = (async (input: unknown) => {
        const url = input instanceof Request ? input.url : String(input);
        fetchCalls.push(url);
        return new Response(
          JSON.stringify({ message: "invalid weekly query" }),
          {
            status: 400,
            headers: { "content-type": "application/json" },
          },
        );
      }) as unknown as typeof fetch;

      const response = await app.request(
        "/api/v1/usage/weekly-summary?tenant_id=tenant-4xx&from=bad-date",
        {
          headers: authHeaders,
        },
      );
      const body = (await response.json()) as {
        error?: string;
        status?: number;
      };

      expect(fetchCalls).toEqual([
        `${proxyBaseUrl}/v1/usage/weekly-summary?tenant_id=${encodeURIComponent(authTenantId)}&from=bad-date`,
      ]);
      expect(response.status).toBe(400);
      expect(body).toEqual({
        error: "analytics 请求参数不合法",
        status: 400,
      });
    } finally {
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/usage/heatmap/drilldown 支持按日期与指标下钻", async () => {
    const authHeaders = await resolveAuthHeaders();
    const date = new Date().toISOString().slice(0, 10);
    const response = await app.request(
      `/api/v1/usage/heatmap/drilldown?date=${encodeURIComponent(date)}&metric=tokens&limit=20`,
      {
        headers: authHeaders,
      },
    );
    const body = (await response.json()) as UsageHeatmapDrilldownResponse;

    expect(response.status).toBe(200);
    expect(Array.isArray(body.items)).toBe(true);
    expect(typeof body.total).toBe("number");
    expect(body.filters.date).toBe(date);
    expect(body.filters.metric).toBe("tokens");
    expect(body.filters.limit).toBe(20);
    expect(typeof body.summary.tokens).toBe("number");
    expect(typeof body.summary.cost).toBe("number");
    expect(typeof body.summary.sessions).toBe("number");
  });

  test("GET /api/v1/usage/heatmap/drilldown 参数非法返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request(
      "/api/v1/usage/heatmap/drilldown?metric=invalid",
      {
        headers: authHeaders,
      },
    );
    const payload = (await response.json()) as { message?: string };

    expect(response.status).toBe(400);
    expect(typeof payload.message).toBe("string");
  });

  test("GET /api/v1/usage/heatmap 代理成功时返回 analytics 数据", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    const proxyBaseUrl = "http://127.0.0.1:19083";
    const proxyCells: HeatmapCell[] = [
      {
        date: "2026-03-01T00:00:00.000Z",
        tokens: 2100,
        cost: 0.7,
        sessions: 3,
      },
      {
        date: "2026-03-02T00:00:00.000Z",
        tokens: 3200,
        cost: 0.9,
        sessions: 4,
      },
    ];
    const queryString =
      "?tenantId=tenant-proxy&from=2026-03-01T00%3A00%3A00.000Z&to=2026-03-31T23%3A59%3A59.999Z&timezone=Asia%2FShanghai&metric=tokens";
    const fetchCalls: string[] = [];

    try {
      Bun.env.ANALYTICS_PROXY_ENABLED = "true";
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      globalThis.fetch = (async (input: unknown) => {
        const url = input instanceof Request ? input.url : String(input);
        fetchCalls.push(url);
        return new Response(JSON.stringify({ cells: proxyCells }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }) as unknown as typeof fetch;

      const response = await app.request(
        `/api/v1/usage/heatmap${queryString}`,
        {
          headers: authHeaders,
        },
      );
      const body = (await response.json()) as UsageHeatmapResponse;

      expect(response.status).toBe(200);
      expect(fetchCalls.length).toBe(1);
      const forwardedUrl = new URL(fetchCalls[0]);
      expect(`${forwardedUrl.origin}${forwardedUrl.pathname}`).toBe(
        `${proxyBaseUrl}/v1/usage/heatmap`,
      );
      expect(forwardedUrl.searchParams.get("tenant_id")).toBe(authTenantId);
      expect(forwardedUrl.searchParams.has("tenantId")).toBe(false);
      expect(forwardedUrl.searchParams.get("from")).toBe(
        "2026-03-01T00:00:00.000Z",
      );
      expect(forwardedUrl.searchParams.get("to")).toBe(
        "2026-03-31T23:59:59.999Z",
      );
      expect(forwardedUrl.searchParams.get("tz")).toBe("Asia/Shanghai");
      expect(forwardedUrl.searchParams.has("timezone")).toBe(false);
      expect(forwardedUrl.searchParams.get("metric")).toBe("tokens");
      expect(body).toEqual({
        cells: proxyCells,
        summary: {
          tokens: 5300,
          cost: 1.6,
          sessions: 7,
        },
      });
    } finally {
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/usage/heatmap 代理成功返回空 cells 时不注入 SAMPLE", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    const proxyBaseUrl = "http://127.0.0.1:19089";
    const fetchCalls: string[] = [];

    try {
      Bun.env.ANALYTICS_PROXY_ENABLED = "true";
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      globalThis.fetch = (async (input: unknown) => {
        const url = input instanceof Request ? input.url : String(input);
        fetchCalls.push(url);
        return new Response(JSON.stringify({ cells: [] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }) as unknown as typeof fetch;

      const response = await app.request("/api/v1/usage/heatmap", {
        headers: authHeaders,
      });
      const body = (await response.json()) as UsageHeatmapResponse;
      expect(response.status).toBe(200);
      expect(fetchCalls).toEqual([
        `${proxyBaseUrl}/v1/usage/heatmap?tenant_id=${encodeURIComponent(authTenantId)}`,
      ]);
      expect(body).toEqual({
        cells: [],
        summary: {
          tokens: 0,
          cost: 0,
          sessions: 0,
        },
      });
    } finally {
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/usage/heatmap 代理失败时自动回退 repository", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    const originalListUsageHeatmap = repository.listUsageHeatmap;
    const proxyBaseUrl = "http://127.0.0.1:19084";
    const queryString =
      "?tenant_id=tenant-fallback&from=2026-03-01T00%3A00%3A00.000Z&to=2026-03-09T23%3A59%3A59.999Z&tz=Asia%2FShanghai&metric=sessions";
    const fetchCalls: string[] = [];
    const repoQueryCalls: Array<UsageHeatmapQueryInput | undefined> = [];
    const repoCells: HeatmapCell[] = [
      { date: "2026-03-02T00:00:00.000Z", tokens: 400, cost: 0.2, sessions: 1 },
      { date: "2026-03-03T00:00:00.000Z", tokens: 600, cost: 0.3, sessions: 2 },
    ];
    const expectedQuery: UsageHeatmapQueryInput = {
      tenantId: authTenantId,
      from: "2026-03-01T00:00:00.000Z",
      to: "2026-03-09T23:59:59.999Z",
      timezone: "Asia/Shanghai",
      metric: "sessions",
    };

    try {
      if (typeof originalListUsageHeatmap !== "function") {
        throw new Error(
          "repository.listUsageHeatmap 不可用，无法验证 usage fallback。",
        );
      }

      repository.listUsageHeatmap = async (input?: UsageHeatmapQueryInput) => {
        repoQueryCalls.push(input);
        return repoCells;
      };
      Bun.env.ANALYTICS_PROXY_ENABLED = "true";
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      globalThis.fetch = (async (input: unknown) => {
        const url = input instanceof Request ? input.url : String(input);
        fetchCalls.push(url);
        return new Response(
          JSON.stringify({ message: "upstream unavailable" }),
          {
            status: 502,
            headers: { "content-type": "application/json" },
          },
        );
      }) as unknown as typeof fetch;

      const fallbackResponse = await app.request(
        `/api/v1/usage/heatmap${queryString}`,
        {
          headers: authHeaders,
        },
      );
      const fallbackBody =
        (await fallbackResponse.json()) as UsageHeatmapResponse;
      expect(fallbackResponse.status).toBe(200);
      expect(fetchCalls.length).toBe(1);
      const forwardedUrl = new URL(fetchCalls[0]);
      expect(`${forwardedUrl.origin}${forwardedUrl.pathname}`).toBe(
        `${proxyBaseUrl}/v1/usage/heatmap`,
      );
      expect(forwardedUrl.searchParams.get("tenant_id")).toBe(authTenantId);
      expect(forwardedUrl.searchParams.get("from")).toBe(
        "2026-03-01T00:00:00.000Z",
      );
      expect(forwardedUrl.searchParams.get("to")).toBe(
        "2026-03-09T23:59:59.999Z",
      );
      expect(forwardedUrl.searchParams.get("tz")).toBe("Asia/Shanghai");
      expect(forwardedUrl.searchParams.get("metric")).toBe("sessions");
      expect(repoQueryCalls).toEqual([expectedQuery]);
      expect(fallbackBody).toEqual({
        cells: repoCells,
        summary: {
          tokens: 1000,
          cost: 0.5,
          sessions: 3,
        },
      });

      Bun.env.ANALYTICS_PROXY_ENABLED = "false";
      globalThis.fetch = (async () => {
        throw new Error("代理关闭时不应调用 fetch");
      }) as unknown as typeof fetch;

      const directRepoResponse = await app.request(
        `/api/v1/usage/heatmap${queryString}`,
        {
          headers: authHeaders,
        },
      );
      const directRepoBody =
        (await directRepoResponse.json()) as UsageHeatmapResponse;
      expect(directRepoResponse.status).toBe(200);
      expect(repoQueryCalls).toEqual([expectedQuery, expectedQuery]);
      expect(fallbackBody).toEqual(directRepoBody);
    } finally {
      repository.listUsageHeatmap = originalListUsageHeatmap;
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/usage/heatmap 代理 4xx 时透传错误且不回退 repository", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    const originalListUsageHeatmap = repository.listUsageHeatmap;
    const proxyBaseUrl = "http://127.0.0.1:19091";
    const fetchCalls: string[] = [];
    let repoCalls = 0;

    try {
      if (typeof originalListUsageHeatmap !== "function") {
        throw new Error(
          "repository.listUsageHeatmap 不可用，无法验证 4xx 透传。",
        );
      }

      repository.listUsageHeatmap = async () => {
        repoCalls += 1;
        return [];
      };
      Bun.env.ANALYTICS_PROXY_ENABLED = "true";
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      globalThis.fetch = (async (input: unknown) => {
        const url = input instanceof Request ? input.url : String(input);
        fetchCalls.push(url);
        return new Response(JSON.stringify({ message: "invalid query" }), {
          status: 400,
          headers: { "content-type": "application/json" },
        });
      }) as unknown as typeof fetch;

      const response = await app.request(
        "/api/v1/usage/heatmap?tenant_id=tenant-4xx&from=bad-date",
        {
          headers: authHeaders,
        },
      );
      const body = (await response.json()) as {
        error?: string;
        status?: number;
      };

      expect(fetchCalls).toEqual([
        `${proxyBaseUrl}/v1/usage/heatmap?tenant_id=${encodeURIComponent(authTenantId)}&from=bad-date`,
      ]);
      expect(repoCalls).toBe(0);
      expect(response.status).toBe(400);
      expect(body).toEqual({
        error: "analytics 请求参数不合法",
        status: 400,
      });
    } finally {
      repository.listUsageHeatmap = originalListUsageHeatmap;
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/usage/heatmap 代理超时回退时不注入 SAMPLE 假数据", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalTimeoutMs = Bun.env.ANALYTICS_PROXY_TIMEOUT_MS;
    const originalFetch = globalThis.fetch;
    const originalListUsageHeatmap = repository.listUsageHeatmap;
    const proxyBaseUrl = "http://127.0.0.1:19086";
    const fetchCalls: string[] = [];
    let abortTriggered = false;

    try {
      if (typeof originalListUsageHeatmap !== "function") {
        throw new Error(
          "repository.listUsageHeatmap 不可用，无法验证 usage timeout fallback。",
        );
      }

      repository.listUsageHeatmap = async () => [];
      Bun.env.ANALYTICS_PROXY_ENABLED = "true";
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      Bun.env.ANALYTICS_PROXY_TIMEOUT_MS = "20";

      globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
        const url = input instanceof Request ? input.url : String(input);
        fetchCalls.push(url);
        return new Promise<Response>((_resolve, reject) => {
          const signal = init?.signal;
          let settled = false;
          const onAbort = () => {
            if (settled) {
              return;
            }
            settled = true;
            abortTriggered = true;
            const error = new Error("aborted");
            error.name = "AbortError";
            reject(error);
          };
          if (signal?.aborted) {
            onAbort();
            return;
          }
          signal?.addEventListener("abort", onAbort, { once: true });
        });
      }) as unknown as typeof fetch;

      const response = await app.request("/api/v1/usage/heatmap", {
        headers: authHeaders,
      });
      const body = (await response.json()) as UsageHeatmapResponse;
      expect(response.status).toBe(200);
      expect(fetchCalls).toEqual([
        `${proxyBaseUrl}/v1/usage/heatmap?tenant_id=${encodeURIComponent(authTenantId)}`,
      ]);
      expect(abortTriggered).toBe(true);
      expect(body).toEqual({
        cells: [],
        summary: {
          tokens: 0,
          cost: 0,
          sessions: 0,
        },
      });
    } finally {
      repository.listUsageHeatmap = originalListUsageHeatmap;
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      if (originalTimeoutMs === undefined) {
        delete Bun.env.ANALYTICS_PROXY_TIMEOUT_MS;
      } else {
        Bun.env.ANALYTICS_PROXY_TIMEOUT_MS = originalTimeoutMs;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test('GET /api/v1/usage/heatmap ANALYTICS_PROXY_TIMEOUT_MS="1e3" 走默认逻辑，不会在 <100ms 被 abort', async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalTimeoutMs = Bun.env.ANALYTICS_PROXY_TIMEOUT_MS;
    const originalFetch = globalThis.fetch;
    const originalListUsageHeatmap = repository.listUsageHeatmap;
    const proxyBaseUrl = "http://127.0.0.1:19090";
    const fetchCalls: string[] = [];
    const proxyCells: HeatmapCell[] = [
      {
        date: "2026-03-05T00:00:00.000Z",
        tokens: 900,
        cost: 0.45,
        sessions: 2,
      },
    ];
    let abortElapsedMs: number | null = null;

    try {
      if (typeof originalListUsageHeatmap !== "function") {
        throw new Error(
          "repository.listUsageHeatmap 不可用，无法验证 timeout env 解析。",
        );
      }

      repository.listUsageHeatmap = async () => [];
      Bun.env.ANALYTICS_PROXY_ENABLED = "true";
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      Bun.env.ANALYTICS_PROXY_TIMEOUT_MS = "1e3";
      globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
        const url = input instanceof Request ? input.url : String(input);
        fetchCalls.push(url);
        const startedAt = Date.now();
        return new Promise<Response>((resolve, reject) => {
          const signal = init?.signal;
          const onAbort = () => {
            abortElapsedMs = Date.now() - startedAt;
            const error = new Error("aborted");
            error.name = "AbortError";
            reject(error);
          };
          if (signal?.aborted) {
            onAbort();
            return;
          }
          signal?.addEventListener("abort", onAbort, { once: true });
          setTimeout(() => {
            if (abortElapsedMs !== null) {
              return;
            }
            resolve(
              new Response(JSON.stringify({ cells: proxyCells }), {
                status: 200,
                headers: { "content-type": "application/json" },
              }),
            );
          }, 130);
        });
      }) as unknown as typeof fetch;

      const response = await app.request("/api/v1/usage/heatmap", {
        headers: authHeaders,
      });
      const body = (await response.json()) as UsageHeatmapResponse;
      expect(response.status).toBe(200);
      expect(fetchCalls).toEqual([
        `${proxyBaseUrl}/v1/usage/heatmap?tenant_id=${encodeURIComponent(authTenantId)}`,
      ]);
      expect(abortElapsedMs === null || abortElapsedMs >= 100).toBe(true);
      expect(body).toEqual({
        cells: proxyCells,
        summary: {
          tokens: 900,
          cost: 0.45,
          sessions: 2,
        },
      });
    } finally {
      repository.listUsageHeatmap = originalListUsageHeatmap;
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      if (originalTimeoutMs === undefined) {
        delete Bun.env.ANALYTICS_PROXY_TIMEOUT_MS;
      } else {
        Bun.env.ANALYTICS_PROXY_TIMEOUT_MS = originalTimeoutMs;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/usage/heatmap 代理 200 但 payload 数值非法时回退不注入 SAMPLE", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    const originalListUsageHeatmap = repository.listUsageHeatmap;
    const proxyBaseUrl = "http://127.0.0.1:19087";
    const fetchCalls: string[] = [];
    let repoCalls = 0;

    try {
      if (typeof originalListUsageHeatmap !== "function") {
        throw new Error(
          "repository.listUsageHeatmap 不可用，无法验证 payload fallback。",
        );
      }

      repository.listUsageHeatmap = async () => {
        repoCalls += 1;
        return [];
      };
      Bun.env.ANALYTICS_PROXY_ENABLED = "true";
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      globalThis.fetch = (async (input: unknown) => {
        const url = input instanceof Request ? input.url : String(input);
        fetchCalls.push(url);
        return new Response(
          JSON.stringify({
            cells: [
              {
                date: "2026-03-06T00:00:00.000Z",
                tokens: -1,
                cost: 0.2,
                sessions: 1,
              },
              {
                date: "2026-03-07T00:00:00.000Z",
                tokens: 10,
                cost: -0.1,
                sessions: 2,
              },
              {
                date: "2026-03-08T00:00:00.000Z",
                tokens: 12,
                cost: 0.3,
                sessions: 1.5,
              },
            ],
          }),
          {
            status: 200,
            headers: { "content-type": "application/json" },
          },
        );
      }) as unknown as typeof fetch;

      const response = await app.request("/api/v1/usage/heatmap", {
        headers: authHeaders,
      });
      const body = (await response.json()) as UsageHeatmapResponse;
      expect(response.status).toBe(200);
      expect(fetchCalls).toEqual([
        `${proxyBaseUrl}/v1/usage/heatmap?tenant_id=${encodeURIComponent(authTenantId)}`,
      ]);
      expect(repoCalls).toBe(1);
      expect(body).toEqual({
        cells: [],
        summary: {
          tokens: 0,
          cost: 0,
          sessions: 0,
        },
      });
    } finally {
      repository.listUsageHeatmap = originalListUsageHeatmap;
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/usage/heatmap ANALYTICS_PROXY_ENABLED 归一化分支", async () => {
    const authHeaders = await resolveAuthHeaders();
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    const proxyBaseUrl = "http://127.0.0.1:19088";
    const proxyCells: HeatmapCell[] = [
      { date: "2026-03-04T00:00:00.000Z", tokens: 100, cost: 0.1, sessions: 1 },
    ];
    const cases: Array<{
      value: string | undefined;
      shouldCallProxy: boolean;
    }> = [
      { value: undefined, shouldCallProxy: true },
      { value: "   ", shouldCallProxy: true },
      { value: " TRUE ", shouldCallProxy: true },
      { value: " false ", shouldCallProxy: false },
      { value: " OFF ", shouldCallProxy: false },
      { value: " no ", shouldCallProxy: false },
      { value: "0", shouldCallProxy: false },
    ];
    let fetchCount = 0;

    try {
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      globalThis.fetch = (async () => {
        fetchCount += 1;
        return new Response(JSON.stringify({ cells: proxyCells }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }) as unknown as typeof fetch;

      for (const testCase of cases) {
        if (testCase.value === undefined) {
          delete Bun.env.ANALYTICS_PROXY_ENABLED;
        } else {
          Bun.env.ANALYTICS_PROXY_ENABLED = testCase.value;
        }

        const beforeCount = fetchCount;
        const response = await app.request("/api/v1/usage/heatmap", {
          headers: authHeaders,
        });
        expect(response.status).toBe(200);
        expect(fetchCount - beforeCount).toBe(testCase.shouldCallProxy ? 1 : 0);
      }
    } finally {
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/usage/heatmap 代理关闭时不调用 analytics", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    const originalListUsageHeatmap = repository.listUsageHeatmap;
    const queryString =
      "?tenant_id=tenant-direct&from=2026-03-10T00%3A00%3A00.000Z&to=2026-03-12T23%3A59%3A59.999Z&tz=America%2FLos_Angeles&metric=cost";
    const expectedQuery: UsageHeatmapQueryInput = {
      tenantId: authTenantId,
      from: "2026-03-10T00:00:00.000Z",
      to: "2026-03-12T23:59:59.999Z",
      timezone: "America/Los_Angeles",
      metric: "cost",
    };
    const repoQueryCalls: Array<UsageHeatmapQueryInput | undefined> = [];
    let fetchCount = 0;

    try {
      if (typeof originalListUsageHeatmap !== "function") {
        throw new Error(
          "repository.listUsageHeatmap 不可用，无法验证代理关闭分支。",
        );
      }

      repository.listUsageHeatmap = async (input?: UsageHeatmapQueryInput) => {
        repoQueryCalls.push(input);
        return [];
      };
      Bun.env.ANALYTICS_PROXY_ENABLED = "false";
      Bun.env.ANALYTICS_BASE_URL = "http://127.0.0.1:19085";
      globalThis.fetch = (async () => {
        fetchCount += 1;
        return new Response(JSON.stringify({ cells: [] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }) as unknown as typeof fetch;

      const response = await app.request(
        `/api/v1/usage/heatmap${queryString}`,
        {
          headers: authHeaders,
        },
      );
      const body = (await response.json()) as UsageHeatmapResponse;

      expect(response.status).toBe(200);
      expect(fetchCount).toBe(0);
      expect(repoQueryCalls).toEqual([expectedQuery]);
      expect(body).toEqual({
        cells: [],
        summary: {
          tokens: 0,
          cost: 0,
          sessions: 0,
        },
      });
    } finally {
      repository.listUsageHeatmap = originalListUsageHeatmap;
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/usage 四个聚合接口返回基础结构（含 daily 环比）", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("usage-aggregates");
    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `Usage 聚合数据源-${nonce}`,
        type: "ssh",
        location: `10.30.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
      }),
    });
    const source = (await createSourceResponse.json()) as Source;
    expect(createSourceResponse.status).toBe(201);

    const monthlySession = await insertSessionForSearch(source.id, {
      provider: "usage-test",
      tool: "Codex CLI",
      model: `usage-model-a-${nonce}`,
      startedAt: "2026-01-15T08:00:00.000Z",
      endedAt: "2026-01-15T08:05:00.000Z",
      tokens: 120,
      cost: 0.12,
    });
    const breakdownSession = await insertSessionForSearch(source.id, {
      provider: "usage-test",
      tool: "Codex CLI",
      model: `usage-model-b-${nonce}`,
      startedAt: "2026-01-16T09:00:00.000Z",
      endedAt: "2026-01-16T09:07:00.000Z",
      tokens: 80,
      cost: 0.08,
    });

    try {
      const baseQuery = new URLSearchParams({
        from: "2026-01-01T00:00:00.000Z",
        to: "2026-01-31T23:59:59.999Z",
        limit: "20",
      }).toString();

      const dailyResult = await requestFirstSuccessful([
        {
          path: `/api/v1/usage/daily?${baseQuery}`,
          init: { headers: authHeaders },
        },
      ]);
      assertApiStatus(dailyResult, [200]);
      const dailyItems = extractListItems(dailyResult.payload);
      expect(Array.isArray(dailyItems)).toBe(true);
      if (dailyItems.length > 0) {
        const first = dailyItems[0];
        expect(typeof pickString(first, ["date"])).toBe("string");
        expect(typeof first.tokens).toBe("number");
        expect(typeof first.cost).toBe("number");
        expect(typeof first.costRaw).toBe("number");
        expect(typeof first.costEstimated).toBe("number");
        expect(["raw", "estimated", "reported", "mixed", "none"]).toContain(
          String(first.costMode),
        );
        expect(typeof first.sessions).toBe("number");
        expect(isRecord(first.change)).toBe(true);
        if (isRecord(first.change)) {
          const changeTokens = first.change.tokens;
          const changeCost = first.change.cost;
          const changeSessions = first.change.sessions;
          expect(
            changeTokens === null || typeof changeTokens === "number",
          ).toBe(true);
          expect(changeCost === null || typeof changeCost === "number").toBe(
            true,
          );
          expect(
            changeSessions === null || typeof changeSessions === "number",
          ).toBe(true);
        }
      }

      const monthlyResult = await requestFirstSuccessful([
        {
          path: `/api/v1/usage/monthly?${baseQuery}`,
          init: { headers: authHeaders },
        },
        {
          path: `/api/v1/usage/aggregates/monthly?${baseQuery}`,
          init: { headers: authHeaders },
        },
      ]);
      assertApiStatus(monthlyResult, [200]);
      const monthlyItems = extractListItems(monthlyResult.payload);
      expect(Array.isArray(monthlyItems)).toBe(true);
      if (monthlyItems.length > 0) {
        const first = monthlyItems[0];
        expect(typeof pickString(first, ["month"])).toBe("string");
        expect(typeof first.tokens).toBe("number");
        expect(typeof first.cost).toBe("number");
        expect(typeof first.costRaw).toBe("number");
        expect(typeof first.costEstimated).toBe("number");
        expect(["raw", "estimated", "reported", "mixed", "none"]).toContain(
          String(first.costMode),
        );
        expect(typeof first.sessions).toBe("number");
      }

      const modelRankingResult = await requestFirstSuccessful([
        {
          path: `/api/v1/usage/models?${baseQuery}`,
          init: { headers: authHeaders },
        },
        {
          path: `/api/v1/usage/model-ranking?${baseQuery}`,
          init: { headers: authHeaders },
        },
        {
          path: `/api/v1/usage/models/ranking?${baseQuery}`,
          init: { headers: authHeaders },
        },
      ]);
      assertApiStatus(modelRankingResult, [200]);
      const modelItems = extractListItems(modelRankingResult.payload);
      expect(Array.isArray(modelItems)).toBe(true);
      if (modelItems.length > 0) {
        const first = modelItems[0];
        expect(typeof pickString(first, ["model"])).toBe("string");
        expect(typeof first.tokens).toBe("number");
        expect(typeof first.cost).toBe("number");
        expect(typeof first.costRaw).toBe("number");
        expect(typeof first.costEstimated).toBe("number");
        expect(["raw", "estimated", "reported", "mixed", "none"]).toContain(
          String(first.costMode),
        );
        expect(typeof first.sessions).toBe("number");
      }

      const sessionBreakdownResult = await requestFirstSuccessful([
        {
          path: `/api/v1/usage/sessions?${baseQuery}`,
          init: { headers: authHeaders },
        },
        {
          path: `/api/v1/usage/session-breakdown?${baseQuery}`,
          init: { headers: authHeaders },
        },
        {
          path: `/api/v1/usage/sessions/breakdown?${baseQuery}`,
          init: { headers: authHeaders },
        },
      ]);
      assertApiStatus(sessionBreakdownResult, [200]);
      const breakdownItems = extractListItems(sessionBreakdownResult.payload);
      expect(Array.isArray(breakdownItems)).toBe(true);
      if (breakdownItems.length > 0) {
        const first = breakdownItems[0];
        expect(typeof pickString(first, ["sessionId", "session_id"])).toBe(
          "string",
        );
        expect(typeof pickString(first, ["sourceId", "source_id"])).toBe(
          "string",
        );
        expect(typeof pickString(first, ["tool"])).toBe("string");
        expect(typeof pickString(first, ["model"])).toBe("string");
        expect(typeof pickString(first, ["startedAt", "started_at"])).toBe(
          "string",
        );
        expect(typeof first.totalTokens).toBe("number");
        expect(typeof first.cost).toBe("number");
        expect(typeof first.costRaw).toBe("number");
        expect(typeof first.costEstimated).toBe("number");
        expect(["raw", "estimated", "reported", "mixed", "none"]).toContain(
          String(first.costMode),
        );
      }
    } finally {
      await monthlySession.cleanup();
      await breakdownSession.cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
    }
  });

  test("GET /api/v1/usage 成本双轨：raw 优先并按 estimated 补齐，兼容 legacy reported", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("usage-cost-dual-track");
    const originalGetPool = repository.getPool;
    const insertedSessionIds: string[] = [];
    let sourceId: string | undefined;

    try {
      repository.getPool = async () => null;
      if (!Array.isArray(repository.memorySessions)) {
        throw new Error(
          "repository.memorySessions 不可用，无法注入 usage 双轨数据。",
        );
      }

      const createSourceResponse = await app.request("/api/v1/sources", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          name: `Usage 双轨数据源-${nonce}`,
          type: "ssh",
          location: `10.40.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
        }),
      });
      const source = (await createSourceResponse.json()) as Source;
      expect(createSourceResponse.status).toBe(201);
      sourceId = source.id;

      const baseStartedAt = "2026-02-10T09:00:00.000Z";
      const rows: Array<Session & Record<string, unknown>> = [
        {
          id: `usage-cost-raw-${nonce}`,
          sourceId: source.id,
          tool: "Codex CLI",
          model: `usage-raw-${nonce}`,
          startedAt: baseStartedAt,
          endedAt: "2026-02-10T09:03:00.000Z",
          tokens: 120,
          cost: 0.3,
          costRaw: 0.3,
          costMode: "raw",
        },
        {
          id: `usage-cost-estimated-${nonce}`,
          sourceId: source.id,
          tool: "Codex CLI",
          model: `usage-estimated-${nonce}`,
          startedAt: "2026-02-10T09:10:00.000Z",
          endedAt: "2026-02-10T09:13:00.000Z",
          tokens: 90,
          cost: 0.2,
          costEstimated: 0.2,
          costMode: "estimated",
        },
        {
          id: `usage-cost-reported-${nonce}`,
          sourceId: source.id,
          tool: "Codex CLI",
          model: `usage-reported-${nonce}`,
          startedAt: "2026-02-10T09:20:00.000Z",
          endedAt: "2026-02-10T09:22:00.000Z",
          tokens: 60,
          cost: 0.1,
          costMode: "reported",
        },
        {
          id: `usage-cost-mixed-${nonce}`,
          sourceId: source.id,
          tool: "Codex CLI",
          model: `usage-mixed-${nonce}`,
          startedAt: "2026-02-10T09:30:00.000Z",
          endedAt: "2026-02-10T09:35:00.000Z",
          tokens: 150,
          cost: 0.45,
          costRaw: 0.4,
          costEstimated: 0.05,
        },
      ];
      repository.memorySessions.push(...rows);
      insertedSessionIds.push(...rows.map((row) => row.id));

      const baseQuery = new URLSearchParams({
        from: "2026-02-01T00:00:00.000Z",
        to: "2026-02-28T23:59:59.999Z",
        limit: "20",
      }).toString();

      const dailyResponse = await app.request(
        `/api/v1/usage/daily?${baseQuery}`,
        {
          headers: authHeaders,
        },
      );
      const dailyPayload = await readResponseAsUnknown(dailyResponse);
      const dailyItems = extractListItems(dailyPayload);
      expect(dailyResponse.status).toBe(200);
      const dailyTarget = dailyItems.find((item) =>
        (pickString(item, ["date"]) ?? "").startsWith("2026-02-10"),
      );
      expect(dailyTarget).toBeDefined();
      if (dailyTarget) {
        expect(Number(dailyTarget.costRaw)).toBeCloseTo(0.8, 6);
        expect(Number(dailyTarget.costEstimated)).toBeCloseTo(0.25, 6);
        expect(Number(dailyTarget.cost)).toBeCloseTo(1.05, 6);
        expect(String(dailyTarget.costMode)).toBe("mixed");
      }

      const monthlyResponse = await app.request(
        `/api/v1/usage/monthly?${baseQuery}`,
        {
          headers: authHeaders,
        },
      );
      const monthlyPayload = await readResponseAsUnknown(monthlyResponse);
      const monthlyItems = extractListItems(monthlyPayload);
      expect(monthlyResponse.status).toBe(200);
      const monthlyTarget = monthlyItems.find((item) =>
        (pickString(item, ["month"]) ?? "").startsWith("2026-02-01"),
      );
      expect(monthlyTarget).toBeDefined();
      if (monthlyTarget) {
        expect(Number(monthlyTarget.costRaw)).toBeCloseTo(0.8, 6);
        expect(Number(monthlyTarget.costEstimated)).toBeCloseTo(0.25, 6);
        expect(Number(monthlyTarget.cost)).toBeCloseTo(1.05, 6);
        expect(String(monthlyTarget.costMode)).toBe("mixed");
      }

      const modelResponse = await app.request(
        `/api/v1/usage/models?${baseQuery}`,
        {
          headers: authHeaders,
        },
      );
      const modelPayload = await readResponseAsUnknown(modelResponse);
      const modelItems = extractListItems(modelPayload);
      expect(modelResponse.status).toBe(200);
      const rawModel = modelItems.find(
        (item) => pickString(item, ["model"]) === `usage-raw-${nonce}`,
      );
      const estimatedModel = modelItems.find(
        (item) => pickString(item, ["model"]) === `usage-estimated-${nonce}`,
      );
      const reportedModel = modelItems.find(
        (item) => pickString(item, ["model"]) === `usage-reported-${nonce}`,
      );
      const mixedModel = modelItems.find(
        (item) => pickString(item, ["model"]) === `usage-mixed-${nonce}`,
      );
      expect(rawModel).toBeDefined();
      expect(estimatedModel).toBeDefined();
      expect(reportedModel).toBeDefined();
      expect(mixedModel).toBeDefined();
      if (rawModel) {
        expect(Number(rawModel.costRaw)).toBeCloseTo(0.3, 6);
        expect(Number(rawModel.costEstimated)).toBeCloseTo(0, 6);
        expect(String(rawModel.costMode)).toBe("raw");
      }
      if (estimatedModel) {
        expect(Number(estimatedModel.costRaw)).toBeCloseTo(0, 6);
        expect(Number(estimatedModel.costEstimated)).toBeCloseTo(0.2, 6);
        expect(String(estimatedModel.costMode)).toBe("estimated");
      }
      if (reportedModel) {
        expect(Number(reportedModel.costRaw)).toBeCloseTo(0.1, 6);
        expect(Number(reportedModel.costEstimated)).toBeCloseTo(0, 6);
        expect(String(reportedModel.costMode)).toBe("reported");
      }
      if (mixedModel) {
        expect(Number(mixedModel.costRaw)).toBeCloseTo(0.4, 6);
        expect(Number(mixedModel.costEstimated)).toBeCloseTo(0.05, 6);
        expect(String(mixedModel.costMode)).toBe("mixed");
      }

      const sessionResponse = await app.request(
        `/api/v1/usage/sessions?${baseQuery}`,
        {
          headers: authHeaders,
        },
      );
      const sessionPayload = await readResponseAsUnknown(sessionResponse);
      const sessionItems = extractListItems(sessionPayload);
      expect(sessionResponse.status).toBe(200);
      const sessionModeByModel = new Map<string, string>();
      for (const item of sessionItems) {
        const model = pickString(item, ["model"]);
        if (!model) {
          continue;
        }
        sessionModeByModel.set(model, String(item.costMode));
      }
      expect(sessionModeByModel.get(`usage-raw-${nonce}`)).toBe("raw");
      expect(sessionModeByModel.get(`usage-estimated-${nonce}`)).toBe(
        "estimated",
      );
      expect(sessionModeByModel.get(`usage-reported-${nonce}`)).toBe(
        "reported",
      );
      expect(sessionModeByModel.get(`usage-mixed-${nonce}`)).toBe("mixed");
    } finally {
      if (
        Array.isArray(repository.memorySessions) &&
        insertedSessionIds.length > 0
      ) {
        for (
          let index = repository.memorySessions.length - 1;
          index >= 0;
          index -= 1
        ) {
          if (
            insertedSessionIds.includes(repository.memorySessions[index]?.id)
          ) {
            repository.memorySessions.splice(index, 1);
          }
        }
      }
      if (sourceId) {
        await app.request(`/api/v1/sources/${sourceId}`, {
          method: "DELETE",
          headers: authHeaders,
        });
      }
      repository.getPool = originalGetPool;
    }
  });

  test("GET /api/v1/usage/daily 租户隔离：忽略 query tenant 参数并强制使用 auth tenant", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalListUsageDaily = repository.listUsageDaily;
    const calls: Array<{
      tenantId?: string;
      from?: string;
      to?: string;
      limit?: number;
    }> = [];

    try {
      if (typeof originalListUsageDaily !== "function") {
        throw new Error(
          "repository.listUsageDaily 不可用，无法验证 tenant 隔离。",
        );
      }

      repository.listUsageDaily = async (input: unknown) => {
        calls.push(input ?? {});
        return [];
      };

      const query = new URLSearchParams({
        tenantId: "tenant-from-query",
        from: "2026-02-01T00:00:00.000Z",
        to: "2026-02-28T23:59:59.999Z",
        limit: "5",
      });
      const response = await app.request(
        `/api/v1/usage/daily?${query.toString()}&tenant_id=tenant-from-query-2`,
        {
          headers: authHeaders,
        },
      );

      expect(response.status).toBe(200);
      expect(calls.length).toBe(1);
      expect(calls[0]?.tenantId).toBe(authTenantId);
      expect(calls[0]?.from).toBe("2026-02-01T00:00:00.000Z");
      expect(calls[0]?.to).toBe("2026-02-28T23:59:59.999Z");
      expect(calls[0]?.limit).toBe(5);
    } finally {
      repository.listUsageDaily = originalListUsageDaily;
    }
  });

  test("POST /api/v1/sources 可创建，GET /api/v1/sources 可查询到新记录", async () => {
    const authHeaders = await resolveAuthHeaders();
    const name = `测试数据源-${Date.now().toString(36)}`;

    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name,
        type: "local",
        location: "~/.codex/sessions/agentledger",
      }),
    });
    const created = (await createResponse.json()) as Source;

    expect(createResponse.status).toBe(201);
    expect(typeof created.id).toBe("string");
    expect(created.name).toBe(name);
    expect(created.type).toBe("local");
    expect(created.location).toBe("~/.codex/sessions/agentledger");
    expect(created.enabled).toBe(true);
    expect(typeof created.createdAt).toBe("string");

    const listResponse = await app.request("/api/v1/sources", {
      headers: authHeaders,
    });
    const listed = (await listResponse.json()) as SourceListResponse;

    expect(listResponse.status).toBe(200);
    expect(Array.isArray(listed.items)).toBe(true);
    expect(typeof listed.total).toBe("number");
    expect(listed.items.some((item) => item.id === created.id)).toBe(true);
  });

  test("POST /api/v1/sources 带 accessMode/sync 字段可创建并回读", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("source-access-sync");

    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `兼容数据源-${nonce}`,
        type: "ssh",
        location: `10.0.0.${Math.floor(Math.random() * 200) + 10}`,
        accessMode: "sync",
        sync: {
          enabled: true,
          cron: "*/15 * * * *",
          retentionDays: 7,
        },
        syncCron: "*/15 * * * *",
        syncRetentionDays: 7,
      }),
    });
    const createdPayload = await readResponseAsUnknown(createResponse);
    const createdId = pickString(createdPayload, ["id"]);

    expect(createResponse.status).toBe(201);
    expect(typeof createdId).toBe("string");
    expect(extractSourceAccessMode(createdPayload)).toBe("sync");
    expect(extractSourceSync(createdPayload)).toBeDefined();

    const listResponse = await app.request("/api/v1/sources", {
      headers: authHeaders,
    });
    const listed = (await listResponse.json()) as SourceListResponse;

    expect(listResponse.status).toBe(200);
    expect(Array.isArray(listed.items)).toBe(true);
    expect(typeof listed.total).toBe("number");

    const listedItem = listed.items.find((item) => item.id === createdId);
    expect(listedItem).toBeDefined();
    expect(extractSourceAccessMode(listedItem)).toBe("sync");
    expect(extractSourceSync(listedItem)).toBeDefined();
  });

  test("POST /api/v1/sources/:id/test-connection 返回结构正确并写入审计（action+sourceId）", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("source-test-connection");
    const mockSsh = await startMockSshServer();
    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `连通性数据源-${nonce}`,
        type: "ssh",
        location: `ssh://tester@${mockSsh.host}:${mockSsh.port}/tmp/repo`,
      }),
    });
    const created = (await createResponse.json()) as Source;

    expect(createResponse.status).toBe(201);
    expect(typeof created.id).toBe("string");

    try {
      const testConnectionResponse = await app.request(
        `/api/v1/sources/${created.id}/test-connection`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({}),
        },
      );
      const testConnectionPayload = await readResponseAsUnknown(
        testConnectionResponse,
      );

      expect(testConnectionResponse.status).toBe(200);
      expect(hasSourceConnectionTestShape(testConnectionPayload)).toBe(true);
      expect(
        collectPayloadCandidates(testConnectionPayload).some((candidate) => {
          const detail = pickString(candidate, ["detail", "message"]) ?? "";
          const success = pickBoolean(candidate, ["success", "ok"]);
          return success === true && detail.includes("error_code=ok");
        }),
      ).toBe(true);

      const audits = await queryAuditByAction(
        "control_plane.source_connection_tested",
        created.id,
      );
      const targetAudit = audits.items.find((item) => {
        const resourceId = item.metadata.resourceId;
        return (
          item.action === "control_plane.source_connection_tested" &&
          (resourceId === created.id ||
            item.detail.includes(created.id) ||
            JSON.stringify(item.metadata).includes(created.id))
        );
      });
      expect(targetAudit).toBeDefined();
    } finally {
      await mockSsh.stop();
    }
  });

  test("POST /api/v1/sources/test-connection 支持 sourceId 模式", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("source-test-connection-entry-sourceid");
    const mockSsh = await startMockSshServer();
    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `新入口 sourceId 数据源-${nonce}`,
        type: "ssh",
        location: `ssh://tester@${mockSsh.host}:${mockSsh.port}/tmp/repo`,
      }),
    });
    const source = (await createResponse.json()) as Source;
    expect(createResponse.status).toBe(201);

    try {
      const testConnectionResult = await requestFirstSuccessful([
        {
          path: "/api/v1/sources/test-connection",
          init: jsonRequest("POST", { sourceId: source.id }, authHeaders),
        },
        {
          path: "/api/v1/sources/test-connection",
          init: jsonRequest("POST", { id: source.id }, authHeaders),
        },
        {
          path: "/api/v1/source/test-connection",
          init: jsonRequest("POST", { sourceId: source.id }, authHeaders),
        },
      ]);
      assertApiStatus(testConnectionResult, [200]);
      expect(hasSourceConnectionTestShape(testConnectionResult.payload)).toBe(
        true,
      );
      expect(
        collectPayloadCandidates(testConnectionResult.payload).some(
          (candidate) => {
            const success = pickBoolean(candidate, ["success", "ok"]);
            const detail = pickString(candidate, ["detail", "message"]) ?? "";
            return success === true && detail.includes("error_code=ok");
          },
        ),
      ).toBe(true);

      const payloadHasSourceId = collectPayloadCandidates(
        testConnectionResult.payload,
      ).some(
        (candidate) =>
          pickString(candidate, ["sourceId", "source_id", "id"]) === source.id,
      );
      expect(payloadHasSourceId).toBe(true);
    } finally {
      await mockSsh.stop();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
    }
  });

  test("POST /api/v1/sources/test-connection 支持临时 source 模式", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("source-test-connection-entry-temp");
    const mockSsh = await startMockSshServer();
    const location = `ssh://tester@${mockSsh.host}:${mockSsh.port}/tmp/repo`;

    try {
      const testConnectionResult = await requestFirstSuccessful([
        {
          path: "/api/v1/sources/test-connection",
          init: jsonRequest(
            "POST",
            {
              source: {
                name: `临时数据源-${nonce}`,
                type: "ssh",
                location,
              },
            },
            authHeaders,
          ),
        },
        {
          path: "/api/v1/sources/test-connection",
          init: jsonRequest(
            "POST",
            {
              name: `临时数据源-${nonce}`,
              type: "ssh",
              location,
            },
            authHeaders,
          ),
        },
        {
          path: "/api/v1/source/test-connection",
          init: jsonRequest(
            "POST",
            {
              source: {
                name: `临时数据源-${nonce}`,
                type: "ssh",
                location,
              },
            },
            authHeaders,
          ),
        },
      ]);
      assertApiStatus(testConnectionResult, [200]);
      expect(hasSourceConnectionTestShape(testConnectionResult.payload)).toBe(
        true,
      );
      expect(
        collectPayloadCandidates(testConnectionResult.payload).some(
          (candidate) => {
            const success = pickBoolean(candidate, ["success", "ok"]);
            const detail = pickString(candidate, ["detail", "message"]) ?? "";
            return success === true && detail.includes("error_code=ok");
          },
        ),
      ).toBe(true);
    } finally {
      await mockSsh.stop();
    }
  });

  test("POST /api/v1/sources/test-connection SSH 握手超时时返回明确 error_code", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("source-test-connection-timeout");
    const mockSsh = await startMockSshServer({ sendBanner: false });
    const location = `ssh://tester@${mockSsh.host}:${mockSsh.port}/tmp/repo`;
    const originalTimeout = Bun.env.SOURCE_TEST_CONNECTION_TIMEOUT_MS;
    Bun.env.SOURCE_TEST_CONNECTION_TIMEOUT_MS = "120";

    try {
      const response = await app.request("/api/v1/sources/test-connection", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          source: {
            name: `超时数据源-${nonce}`,
            type: "ssh",
            location,
          },
        }),
      });
      const payload = await readResponseAsUnknown(response);

      expect(response.status).toBe(200);
      expect(hasSourceConnectionTestShape(payload)).toBe(true);
      expect(
        collectPayloadCandidates(payload).some((candidate) => {
          const success = pickBoolean(candidate, ["success", "ok"]);
          const errorCode = pickString(candidate, ["errorCode", "error_code"]);
          const detail = pickString(candidate, ["detail", "message"]) ?? "";
          return (
            success === false &&
            (errorCode === "ssh_handshake_timeout" ||
              detail.includes("error_code=ssh_handshake_timeout"))
          );
        }),
      ).toBe(true);
    } finally {
      if (originalTimeout === undefined) {
        delete Bun.env.SOURCE_TEST_CONNECTION_TIMEOUT_MS;
      } else {
        Bun.env.SOURCE_TEST_CONNECTION_TIMEOUT_MS = originalTimeout;
      }
      await mockSsh.stop();
    }
  });

  test("POST /api/v1/sources/:id/sync-jobs 创建成功，GET /api/v1/sources/:id/sync-jobs 可查询到", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("source-sync-job");
    const expectedNextRunAt = new Date(Date.now() + 15 * 60_000).toISOString();
    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `同步任务数据源-${nonce}`,
        type: "ssh",
        location: `172.16.10.${Math.floor(Math.random() * 200) + 10}`,
      }),
    });
    const created = (await createResponse.json()) as Source;

    expect(createResponse.status).toBe(201);
    expect(typeof created.id).toBe("string");

    const createSyncJobResponse = await app.request(
      `/api/v1/sources/${created.id}/sync-jobs`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          nextRunAt: expectedNextRunAt,
        }),
      },
    );
    const createSyncJobPayload = await readResponseAsUnknown(
      createSyncJobResponse,
    );
    const syncJobId = extractSourceSyncJobId(createSyncJobPayload);

    expect(createSyncJobResponse.status).toBe(202);
    expect(typeof syncJobId).toBe("string");
    expect(extractJobStatus(createSyncJobPayload)).toBe("pending");
    expect(extractSyncJobNextRunAt(createSyncJobPayload)).toBe(
      expectedNextRunAt,
    );

    const listSyncJobsResponse = await app.request(
      `/api/v1/sources/${created.id}/sync-jobs`,
      {
        headers: authHeaders,
      },
    );
    const listSyncJobsPayload =
      await readResponseAsUnknown(listSyncJobsResponse);
    const items = extractListItems(listSyncJobsPayload);

    expect(listSyncJobsResponse.status).toBe(200);
    expect(Array.isArray(items)).toBe(true);
    expect(items.length).toBeGreaterThan(0);
    const createdItem = items.find((item) => {
      const jobId = pickString(item, ["syncJobId", "jobId", "id"]);
      return jobId === syncJobId;
    });
    expect(createdItem).toBeDefined();
    expect(extractSyncJobNextRunAt(createdItem)).toBe(expectedNextRunAt);
  });

  test("PATCH /api/v1/sync-jobs/:id/cancel 可取消 pending 同步任务", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("source-sync-job-cancel");
    const expectedNextRunAt = new Date(Date.now() + 30 * 60_000).toISOString();
    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `取消同步任务数据源-${nonce}`,
        type: "ssh",
        location: `172.16.20.${Math.floor(Math.random() * 200) + 10}`,
      }),
    });
    const created = (await createResponse.json()) as Source;

    expect(createResponse.status).toBe(201);
    expect(typeof created.id).toBe("string");

    const createSyncJobResponse = await app.request(
      `/api/v1/sources/${created.id}/sync-jobs`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          nextRunAt: expectedNextRunAt,
        }),
      },
    );
    const createSyncJobPayload = await readResponseAsUnknown(
      createSyncJobResponse,
    );
    const syncJobId = extractSourceSyncJobId(createSyncJobPayload);

    expect(createSyncJobResponse.status).toBe(202);
    expect(typeof syncJobId).toBe("string");
    expect(extractSyncJobNextRunAt(createSyncJobPayload)).toBe(
      expectedNextRunAt,
    );

    const cancelResponse = await app.request(
      `/api/v1/sync-jobs/${syncJobId}/cancel`,
      {
        method: "PATCH",
        headers: authHeaders,
      },
    );
    const cancelPayload = await readResponseAsUnknown(cancelResponse);

    expect(cancelResponse.status).toBe(202);
    expect(extractJobStatus(cancelPayload)).toBe("cancelled");
    expect(extractSyncJobNextRunAt(cancelPayload)).toBe(expectedNextRunAt);

    let cancelRequested: boolean | undefined;
    for (const candidate of collectPayloadCandidates(cancelPayload)) {
      cancelRequested = pickBoolean(candidate, [
        "cancelRequested",
        "cancel_requested",
      ]);
      if (cancelRequested !== undefined) {
        break;
      }
    }
    expect(cancelRequested).toBe(true);
  });

  test("GET /api/v1/sources/:id/health 返回 source health 结构与聚合字段", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const nonce = createNonce("source-health");
    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `健康数据源-${nonce}`,
        type: "ssh",
        location: `172.16.30.${Math.floor(Math.random() * 200) + 10}`,
        accessMode: "sync",
      }),
    });
    const source = (await createResponse.json()) as Source;
    expect(createResponse.status).toBe(201);

    try {
      if (typeof repository.createSyncJob !== "function") {
        throw new Error(
          "repository.createSyncJob 不可用，无法注入 health 测试数据。",
        );
      }

      const now = Date.now();
      await repository.createSyncJob(
        authTenantId,
        source.id,
        "sync",
        "failed",
        "network timeout",
        {
          startedAt: new Date(now - 4 * 60_000).toISOString(),
          endedAt: new Date(now - 3 * 60_000).toISOString(),
          durationMs: 60000,
          errorDetail: "network timeout",
        },
      );
      await repository.createSyncJob(
        authTenantId,
        source.id,
        "sync",
        "success",
        undefined,
        {
          startedAt: new Date(now - 2 * 60_000).toISOString(),
          endedAt: new Date(now - 60_000).toISOString(),
          durationMs: 60000,
        },
      );

      const healthResponse = await app.request(
        `/api/v1/sources/${source.id}/health`,
        {
          headers: authHeaders,
        },
      );
      const payload = await readResponseAsUnknown(healthResponse);

      expect(healthResponse.status).toBe(200);
      expect(isRecord(payload)).toBe(true);
      if (isRecord(payload)) {
        expect(pickString(payload, ["sourceId", "source_id"])).toBe(source.id);
        expect(pickString(payload, ["accessMode", "access_mode"])).toBe("sync");
        expect(
          typeof pickString(payload, ["lastSuccessAt", "last_success_at"]),
        ).toBe("string");
        expect(
          typeof pickString(payload, ["lastFailureAt", "last_failure_at"]),
        ).toBe("string");
        expect(typeof payload.failureCount).toBe("number");
        expect((payload.failureCount as number) >= 1).toBe(true);
        expect(
          payload.avgLatencyMs === null ||
            typeof payload.avgLatencyMs === "number",
        ).toBe(true);
        expect(
          payload.freshnessMinutes === null ||
            typeof payload.freshnessMinutes === "number",
        ).toBe(true);
      }
    } finally {
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
    }
  });

  test("GET /api/v1/sources/:id/watermarks 可返回列表结构", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("source-watermarks");
    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `水位线数据源-${nonce}`,
        type: "local",
        location: `~/.codex/sessions/agentledger-watermarks-${nonce}`,
      }),
    });
    const created = (await createResponse.json()) as Source;

    expect(createResponse.status).toBe(201);
    expect(typeof created.id).toBe("string");

    const watermarksResponse = await app.request(
      `/api/v1/sources/${created.id}/watermarks`,
      {
        headers: authHeaders,
      },
    );
    const watermarksPayload = await readResponseAsUnknown(watermarksResponse);
    const items = extractListItems(watermarksPayload);

    expect(watermarksResponse.status).toBe(200);
    expect(Array.isArray(items)).toBe(true);
    expect(items.length).toBeGreaterThanOrEqual(0);
  });

  test("GET /api/v1/sources/:id/parse-failures 支持过滤条件与 limit", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const nonce = createNonce("source-parse-failures");
    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `解析失败数据源-${nonce}`,
        type: "ssh",
        location: `10.46.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
      }),
    });
    const source = (await createResponse.json()) as Source;
    expect(createResponse.status).toBe(201);

    const insertedIds: string[] = [];
    const insertedMemoryIds: string[] = [];
    const now = Date.now();
    const firstFailedAt = new Date(now - 2 * 60_000).toISOString();
    const secondFailedAt = new Date(now - 60_000).toISOString();

    try {
      if (typeof repository.getPool === "function") {
        const pool = await repository.getPool();
        if (pool) {
          const rows = [
            {
              id: createNonce("pf-match"),
              parserKey: "jsonl",
              errorCode: "parse_error",
              errorMessage: "json line parse failed",
              sourcePath: `/tmp/${nonce}/a.jsonl`,
              sourceOffset: 12,
              failedAt: firstFailedAt,
            },
            {
              id: createNonce("pf-non-match"),
              parserKey: "native",
              errorCode: "unsupported_format",
              errorMessage: "native payload unknown",
              sourcePath: `/tmp/${nonce}/b.log`,
              sourceOffset: 24,
              failedAt: secondFailedAt,
            },
          ];
          for (const row of rows) {
            await pool.query(
              `INSERT INTO parse_failures (
                 id,
                 tenant_id,
                 source_id,
                 parser_key,
                 error_code,
                 error_message,
                 source_path,
                 source_offset,
                 raw_hash,
                 metadata,
                 occurred_at,
                 created_at
               )
               VALUES (
                 $1,
                 $2,
                 $3,
                 $4,
                 $5,
                 $6,
                 $7,
                 $8,
                 $9,
                 $10::jsonb,
                 $11::timestamptz,
                 $11::timestamptz
               )`,
              [
                row.id,
                tenantId,
                source.id,
                row.parserKey,
                row.errorCode,
                row.errorMessage,
                row.sourcePath,
                row.sourceOffset,
                `hash-${row.id}`,
                JSON.stringify({ parser: row.parserKey }),
                row.failedAt,
              ],
            );
            insertedIds.push(row.id);
          }
        } else if (Array.isArray(repository.memorySourceParseFailures)) {
          const records: SourceParseFailure[] = [
            {
              id: createNonce("pf-memory-match"),
              sourceId: source.id,
              parserKey: "jsonl",
              errorCode: "parse_error",
              errorMessage: "json line parse failed",
              sourcePath: `/tmp/${nonce}/a.jsonl`,
              sourceOffset: 12,
              rawHash: `hash-${nonce}-1`,
              metadata: { parser: "jsonl" },
              failedAt: firstFailedAt,
              createdAt: firstFailedAt,
            },
            {
              id: createNonce("pf-memory-non-match"),
              sourceId: source.id,
              parserKey: "native",
              errorCode: "unsupported_format",
              errorMessage: "native payload unknown",
              sourcePath: `/tmp/${nonce}/b.log`,
              sourceOffset: 24,
              rawHash: `hash-${nonce}-2`,
              metadata: { parser: "native" },
              failedAt: secondFailedAt,
              createdAt: secondFailedAt,
            },
          ];
          for (const failure of records) {
            repository.memorySourceParseFailures.push({
              tenantId,
              failure,
            });
            insertedMemoryIds.push(failure.id);
          }
        }
      }

      const query = new URLSearchParams({
        from: new Date(now - 5 * 60_000).toISOString(),
        to: new Date(now + 5 * 60_000).toISOString(),
        parserKey: "jsonl",
        errorCode: "parse_error",
        limit: "1",
      });
      const response = await app.request(
        `/api/v1/sources/${source.id}/parse-failures?${query.toString()}`,
        {
          headers: authHeaders,
        },
      );
      const body = (await response.json()) as {
        items: Array<{
          sourceId: string;
          parserKey: string;
          errorCode: string;
          failedAt: string;
        }>;
        total: number;
        filters: {
          from?: string;
          to?: string;
          parserKey?: string;
          errorCode?: string;
          limit?: number;
        };
      };

      expect(response.status).toBe(200);
      expect(Array.isArray(body.items)).toBe(true);
      expect(body.items.length).toBe(1);
      expect(body.total).toBeGreaterThanOrEqual(1);
      expect(body.filters.parserKey).toBe("jsonl");
      expect(body.filters.errorCode).toBe("parse_error");
      expect(body.filters.limit).toBe(1);
      expect(body.items[0]?.sourceId).toBe(source.id);
      expect(body.items[0]?.parserKey).toBe("jsonl");
      expect(body.items[0]?.errorCode).toBe("parse_error");
      expect(typeof body.items[0]?.failedAt).toBe("string");

      const invalidLimitResponse = await app.request(
        `/api/v1/sources/${source.id}/parse-failures?limit=0`,
        {
          headers: authHeaders,
        },
      );
      const invalidLimitBody = (await invalidLimitResponse.json()) as {
        message: string;
      };
      expect(invalidLimitResponse.status).toBe(400);
      expect(invalidLimitBody.message).toContain("limit");
    } finally {
      if (insertedIds.length > 0 && typeof repository.getPool === "function") {
        const pool = await repository.getPool();
        if (pool) {
          await pool.query(
            `DELETE FROM parse_failures
             WHERE source_id = $1
               AND id = ANY($2::text[])`,
            [source.id, insertedIds],
          );
        }
      }
      if (
        insertedMemoryIds.length > 0 &&
        Array.isArray(repository.memorySourceParseFailures)
      ) {
        for (
          let i = repository.memorySourceParseFailures.length - 1;
          i >= 0;
          i -= 1
        ) {
          const current = repository.memorySourceParseFailures[i];
          if (current && insertedMemoryIds.includes(current.failure.id)) {
            repository.memorySourceParseFailures.splice(i, 1);
          }
        }
      }
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
    }
  });

  test("POST /api/v1/sources 会写入 source_created 审计且可按 sourceId 查询", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const name = `审计数据源-${nonce}`;

    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name,
        type: "local",
        location: `~/.codex/sessions/agentledger-audit-source-${nonce}`,
      }),
    });
    const created = (await createResponse.json()) as Source;

    expect(createResponse.status).toBe(201);
    expect(typeof created.id).toBe("string");

    const query = new URLSearchParams({
      action: "control_plane.source_created",
      keyword: created.id,
      limit: "200",
    });
    const auditResponse = await app.request(
      `/api/v1/audits?${query.toString()}`,
      {
        headers: authHeaders,
      },
    );
    const audits = (await auditResponse.json()) as {
      items: Array<{
        action: string;
        metadata: Record<string, unknown>;
      }>;
      total: number;
      filters: AuditListInput & {
        action?: string;
        keyword?: string;
        limit?: number;
      };
    };

    expect(auditResponse.status).toBe(200);
    expect(Array.isArray(audits.items)).toBe(true);
    expect(typeof audits.total).toBe("number");
    expect(audits.filters.action).toBe("control_plane.source_created");
    expect(audits.filters.keyword).toBe(created.id);

    const targetAudit = audits.items.find((item) => {
      const resourceId = item.metadata.resourceId;
      return (
        item.action === "control_plane.source_created" &&
        resourceId === created.id
      );
    });
    expect(targetAudit).toBeDefined();
  });

  test("POST /api/v1/sources 支持写入并读回 sourceRegion", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("source-region-create");

    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `区域数据源-${nonce}`,
        type: "local",
        location: `~/.codex/sessions/source-region-${nonce}`,
        sourceRegion: "cn-shanghai",
      }),
    });
    const created = (await createResponse.json()) as Source & {
      sourceRegion?: string;
    };

    expect(createResponse.status).toBe(201);
    expect(created.sourceRegion).toBe("cn-shanghai");

    const listResponse = await app.request("/api/v1/sources", {
      headers: authHeaders,
    });
    const listed = (await listResponse.json()) as SourceListResponse & {
      items: Array<Source & { sourceRegion?: string }>;
    };
    expect(listResponse.status).toBe(200);
    expect(
      listed.items.some(
        (item) => item.id === created.id && item.sourceRegion === "cn-shanghai",
      ),
    ).toBe(true);
  });

  test("PATCH /api/v1/sources/:id 支持更新 sourceRegion 并写入 source_updated 审计", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("source-region-update");

    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `待更新区域数据源-${nonce}`,
        type: "local",
        location: `~/.codex/sessions/source-region-update-${nonce}`,
      }),
    });
    const created = (await createResponse.json()) as Source;
    expect(createResponse.status).toBe(201);

    const updateResponse = await app.request(`/api/v1/sources/${created.id}`, {
      method: "PATCH",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        sourceRegion: "cn-hangzhou",
      }),
    });
    const updated = (await updateResponse.json()) as Source & {
      sourceRegion?: string;
    };
    expect(updateResponse.status).toBe(200);
    expect(updated.sourceRegion).toBe("cn-hangzhou");

    const audits = await queryAuditByAction(
      "control_plane.source_updated",
      created.id,
    );
    const targetAudit = audits.items.find(
      (item) =>
        item.action === "control_plane.source_updated" &&
        item.metadata.resourceId === created.id &&
        item.metadata.sourceRegion === "cn-hangzhou",
    );
    expect(targetAudit).toBeDefined();
  });

  test("GET /api/v1/sources/missing-region 与 POST /api/v1/sources/source-region/backfill 支持 dryRun + apply", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const nonce = createNonce("source-region-backfill");

    if (typeof repository.upsertTenantResidencyPolicy !== "function") {
      throw new Error(
        "repository.upsertTenantResidencyPolicy 不可用，无法验证 sourceRegion backfill。",
      );
    }

    await repository.upsertTenantResidencyPolicy(tenantId, {
      tenantId,
      mode: "single_region",
      primaryRegion: "cn-shanghai",
      replicaRegions: [],
      allowCrossRegionTransfer: false,
      requireTransferApproval: false,
      updatedAt: new Date().toISOString(),
    });

    const missingResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `缺失区域数据源-${nonce}`,
        type: "local",
        location: `~/.codex/sessions/source-missing-region-${nonce}`,
      }),
    });
    const missingSource = (await missingResponse.json()) as Source;
    expect(missingResponse.status).toBe(201);

    const presetResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `已有区域数据源-${nonce}`,
        type: "local",
        location: `~/.codex/sessions/source-present-region-${nonce}`,
        sourceRegion: "ap-southeast-1",
      }),
    });
    const presetSource = (await presetResponse.json()) as Source;
    expect(presetResponse.status).toBe(201);

    const missingListResponse = await app.request(
      "/api/v1/sources/missing-region",
      {
        headers: authHeaders,
      },
    );
    const missingList =
      (await missingListResponse.json()) as SourceListResponse;
    expect(missingListResponse.status).toBe(200);
    expect(missingList.items.some((item) => item.id === missingSource.id)).toBe(
      true,
    );
    expect(missingList.items.some((item) => item.id === presetSource.id)).toBe(
      false,
    );

    const dryRunResponse = await app.request(
      "/api/v1/sources/source-region/backfill",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          dryRun: true,
          sourceIds: [missingSource.id],
        }),
      },
    );
    const dryRunBody = (await dryRunResponse.json()) as {
      dryRun: boolean;
      updated: number;
      items: Array<{
        sourceId: string;
        status: string;
        appliedRegion?: string;
      }>;
    };
    expect(dryRunResponse.status).toBe(200);
    expect(dryRunBody.dryRun).toBe(true);
    expect(dryRunBody.updated).toBe(0);
    expect(dryRunBody.items).toHaveLength(1);
    expect(dryRunBody.items[0]).toMatchObject({
      sourceId: missingSource.id,
      status: "would_update",
      appliedRegion: "cn-shanghai",
    });

    const afterDryRunListResponse = await app.request(
      "/api/v1/sources/missing-region",
      {
        headers: authHeaders,
      },
    );
    const afterDryRunList =
      (await afterDryRunListResponse.json()) as SourceListResponse;
    expect(
      afterDryRunList.items.some((item) => item.id === missingSource.id),
    ).toBe(true);

    const applyResponse = await app.request(
      "/api/v1/sources/source-region/backfill",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          sourceIds: [missingSource.id],
        }),
      },
    );
    const applyBody = (await applyResponse.json()) as {
      dryRun: boolean;
      updated: number;
      primaryRegion: string;
    };
    expect(applyResponse.status).toBe(200);
    expect(applyBody.dryRun).toBe(false);
    expect(applyBody.updated).toBe(1);
    expect(applyBody.primaryRegion).toBe("cn-shanghai");

    const listResponse = await app.request("/api/v1/sources", {
      headers: authHeaders,
    });
    const listed = (await listResponse.json()) as SourceListResponse & {
      items: Array<Source & { sourceRegion?: string }>;
    };
    const updatedSource = listed.items.find(
      (item) => item.id === missingSource.id,
    );
    expect(updatedSource?.sourceRegion).toBe("cn-shanghai");

    const backfillAudits = await queryAuditByAction(
      "control_plane.source_region_backfill_executed",
      missingSource.id,
    );
    const targetAudit = backfillAudits.items.find(
      (item) =>
        item.action === "control_plane.source_region_backfill_executed" &&
        Array.isArray(item.metadata.sourceIds) &&
        item.metadata.sourceIds.includes(missingSource.id),
    );
    expect(targetAudit).toBeDefined();
  });

  test("POST /api/v1/sources/source-region/backfill 在未配置主区域时返回 409", async () => {
    const nonce = createNonce("source-region-backfill-no-policy");
    const owner = await registerAndLoginUser(`${nonce}-owner`);
    if (!owner.userId) {
      throw new Error("无法解析用户身份，无法执行无主区域回填测试。");
    }

    const tenantResult = await createTenantByAuth(
      owner.accessToken,
      {
        name: `无主区域租户-${nonce}`,
        slug: `source-backfill-no-policy-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("租户创建响应缺少 tenantId。");
    }
    const authHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      owner.accessToken,
      owner.userId,
    );

    const response = await app.request(
      "/api/v1/sources/source-region/backfill",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({ dryRun: true }),
      },
    );
    const body = (await response.json()) as {
      message: string;
    };
    expect(response.status).toBe(409);
    expect(body.message).toContain("主区域");
  });

  test("DELETE /api/v1/sources/:id 删除不存在的数据源返回 404", async () => {
    const authHeaders = await resolveAuthHeaders();
    const sourceId = `source-not-exists-${Date.now().toString(36)}`;
    const response = await app.request(`/api/v1/sources/${sourceId}`, {
      method: "DELETE",
      headers: authHeaders,
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(404);
    expect(body.message).toContain(sourceId);
  });

  test("DELETE /api/v1/sources/:id 删除成功并写入 source_deleted 审计", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `待删除数据源-${nonce}`,
        type: "local",
        location: `~/.codex/sessions/agentledger-delete-source-${nonce}`,
      }),
    });
    const created = (await createResponse.json()) as Source;

    expect(createResponse.status).toBe(201);
    expect(typeof created.id).toBe("string");

    const deleteResponse = await app.request(`/api/v1/sources/${created.id}`, {
      method: "DELETE",
      headers: authHeaders,
    });
    expect(deleteResponse.status).toBe(204);
    expect(await deleteResponse.text()).toBe("");

    const listResponse = await app.request("/api/v1/sources", {
      headers: authHeaders,
    });
    const listed = (await listResponse.json()) as SourceListResponse;
    expect(listResponse.status).toBe(200);
    expect(listed.items.some((item) => item.id === created.id)).toBe(false);

    const query = new URLSearchParams({
      action: "control_plane.source_deleted",
      keyword: created.id,
      limit: "200",
    });
    const auditResponse = await app.request(
      `/api/v1/audits?${query.toString()}`,
      {
        headers: authHeaders,
      },
    );
    const audits = (await auditResponse.json()) as {
      items: Array<{
        action: string;
        metadata: Record<string, unknown>;
      }>;
      total: number;
      filters: AuditListInput & {
        action?: string;
        keyword?: string;
        limit?: number;
      };
    };

    expect(auditResponse.status).toBe(200);
    expect(Array.isArray(audits.items)).toBe(true);
    expect(typeof audits.total).toBe("number");
    expect(audits.filters.action).toBe("control_plane.source_deleted");
    expect(audits.filters.keyword).toBe(created.id);

    const targetAudit = audits.items.find((item) => {
      const resourceId = item.metadata.resourceId;
      return (
        item.action === "control_plane.source_deleted" &&
        resourceId === created.id
      );
    });
    expect(targetAudit).toBeDefined();
  });

  test("DELETE /api/v1/sources/:id 删除冲突返回 409 且写入 source_delete_blocked 审计", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `冲突数据源-${nonce}`,
        type: "local",
        location: `~/.codex/sessions/agentledger-conflict-source-${nonce}`,
      }),
    });
    const created = (await createResponse.json()) as Source;

    expect(createResponse.status).toBe(201);
    expect(typeof created.id).toBe("string");

    const cleanupSessionReference = await ensureSourceReferencedBySession(
      created.id,
    );

    try {
      const deleteResponse = await app.request(
        `/api/v1/sources/${created.id}`,
        {
          method: "DELETE",
          headers: authHeaders,
        },
      );
      const body = (await deleteResponse.json()) as {
        message: string;
      };

      expect(deleteResponse.status).toBe(409);
      expect(body.message).toContain(created.id);

      const audits = await queryAuditByAction(
        "control_plane.source_delete_blocked",
        created.id,
      );
      const targetAudit = audits.items.find((item) => {
        const resourceId = item.metadata.resourceId;
        return (
          item.action === "control_plane.source_delete_blocked" &&
          (resourceId === created.id ||
            item.detail.includes(created.id) ||
            JSON.stringify(item.metadata).includes(created.id))
        );
      });
      expect(targetAudit).toBeDefined();
    } finally {
      await cleanupSessionReference();
    }
  });

  test("Sources 多租户隔离：跨租户不可见、不可删、审计不可见", async () => {
    const nonce = createNonce("source-tenant-isolation");
    const ownerA = await registerAndLoginUser(`${nonce}-owner-a`);
    const ownerB = await registerAndLoginUser(`${nonce}-owner-b`);
    if (!ownerA.userId || !ownerB.userId) {
      throw new Error("无法解析用户身份，无法执行 sources 多租户隔离测试。");
    }

    const tenantAResult = await createTenantByAuth(
      ownerA.accessToken,
      {
        name: `数据源租户A-${nonce}`,
        slug: `source-tenant-a-${nonce}`,
      },
      ownerA.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建响应缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      ownerB.accessToken,
      {
        name: `数据源租户B-${nonce}`,
        slug: `source-tenant-b-${nonce}`,
      },
      ownerB.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建响应缺少 tenantId。");
    }

    const authHeadersA = await issueTenantScopedAuthHeaders(
      tenantAId,
      ownerA.accessToken,
      ownerA.userId,
    );
    const authHeadersB = await issueTenantScopedAuthHeaders(
      tenantBId,
      ownerB.accessToken,
      ownerB.userId,
    );

    const createResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeadersA,
      },
      body: JSON.stringify({
        name: `租户隔离数据源-${nonce}`,
        type: "local",
        location: `~/.codex/sessions/agentledger-source-tenant-${nonce}`,
      }),
    });
    const created = (await createResponse.json()) as Source;
    expect(createResponse.status).toBe(201);
    expect(typeof created.id).toBe("string");

    const listAResponse = await app.request("/api/v1/sources", {
      headers: authHeadersA,
    });
    const listA = (await listAResponse.json()) as SourceListResponse;
    expect(listAResponse.status).toBe(200);
    expect(listA.items.some((item) => item.id === created.id)).toBe(true);

    const listBResponse = await app.request("/api/v1/sources", {
      headers: authHeadersB,
    });
    const listB = (await listBResponse.json()) as SourceListResponse;
    expect(listBResponse.status).toBe(200);
    expect(listB.items.some((item) => item.id === created.id)).toBe(false);

    const crossDeleteResponse = await app.request(
      `/api/v1/sources/${created.id}`,
      {
        method: "DELETE",
        headers: authHeadersB,
      },
    );
    expect(crossDeleteResponse.status).toBe(404);

    const auditQuery = new URLSearchParams({
      action: "control_plane.source_created",
      keyword: created.id,
      limit: "200",
    });
    const auditAResponse = await app.request(
      `/api/v1/audits?${auditQuery.toString()}`,
      {
        headers: authHeadersA,
      },
    );
    const auditsA = (await auditAResponse.json()) as {
      items: Array<{
        action: string;
        metadata: Record<string, unknown>;
      }>;
    };
    expect(auditAResponse.status).toBe(200);
    expect(
      auditsA.items.some((item) => {
        const resourceId = item.metadata.resourceId;
        const metadataTenantId =
          item.metadata.tenantId ?? item.metadata.tenant_id;
        return (
          item.action === "control_plane.source_created" &&
          resourceId === created.id &&
          metadataTenantId === tenantAId
        );
      }),
    ).toBe(true);

    const auditBResponse = await app.request(
      `/api/v1/audits?${auditQuery.toString()}`,
      {
        headers: authHeadersB,
      },
    );
    const auditsB = (await auditBResponse.json()) as {
      items: Array<{
        action: string;
        metadata: Record<string, unknown>;
      }>;
    };
    expect(auditBResponse.status).toBe(200);
    expect(
      auditsB.items.some((item) => {
        const resourceId = item.metadata.resourceId;
        return (
          item.action === "control_plane.source_created" &&
          resourceId === created.id
        );
      }),
    ).toBe(false);
  });

  test("Sources 多租户隔离：跨租户取消 sync-job 返回 404", async () => {
    const nonce = createNonce("source-sync-job-tenant-cancel");
    const ownerA = await registerAndLoginUser(`${nonce}-owner-a`);
    const ownerB = await registerAndLoginUser(`${nonce}-owner-b`);
    if (!ownerA.userId || !ownerB.userId) {
      throw new Error("无法解析用户身份，无法执行 sync-job 跨租户取消测试。");
    }

    const tenantAResult = await createTenantByAuth(
      ownerA.accessToken,
      {
        name: `同步租户A-${nonce}`,
        slug: `sync-tenant-a-${nonce}`,
      },
      ownerA.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建响应缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      ownerB.accessToken,
      {
        name: `同步租户B-${nonce}`,
        slug: `sync-tenant-b-${nonce}`,
      },
      ownerB.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建响应缺少 tenantId。");
    }

    const authHeadersA = await issueTenantScopedAuthHeaders(
      tenantAId,
      ownerA.accessToken,
      ownerA.userId,
    );
    const authHeadersB = await issueTenantScopedAuthHeaders(
      tenantBId,
      ownerB.accessToken,
      ownerB.userId,
    );

    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeadersA,
      },
      body: JSON.stringify({
        name: `跨租户取消数据源-${nonce}`,
        type: "ssh",
        location: `10.10.1.${Math.floor(Math.random() * 200) + 10}`,
      }),
    });
    const source = (await createSourceResponse.json()) as Source;
    expect(createSourceResponse.status).toBe(201);

    const createSyncJobResponse = await app.request(
      `/api/v1/sources/${source.id}/sync-jobs`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeadersA,
        },
        body: JSON.stringify({
          mode: "sync",
        }),
      },
    );
    const syncJobPayload = await readResponseAsUnknown(createSyncJobResponse);
    const syncJobId = extractSourceSyncJobId(syncJobPayload);
    expect(createSyncJobResponse.status).toBe(202);
    expect(typeof syncJobId).toBe("string");

    const cancelByBResponse = await app.request(
      `/api/v1/sync-jobs/${syncJobId}/cancel`,
      {
        method: "PATCH",
        headers: authHeadersB,
      },
    );
    expect(cancelByBResponse.status).toBe(404);

    const listAResponse = await app.request(
      `/api/v1/sources/${source.id}/sync-jobs`,
      {
        headers: authHeadersA,
      },
    );
    const listAPayload = await readResponseAsUnknown(listAResponse);
    const listAItems = extractListItems(listAPayload);
    expect(listAResponse.status).toBe(200);
    expect(
      listAItems.some((item) => {
        const jobId = pickString(item, ["id", "jobId", "syncJobId"]);
        const status = pickString(item, ["status"]);
        const cancelRequested = pickBoolean(item, [
          "cancelRequested",
          "cancel_requested",
        ]);
        return (
          jobId === syncJobId &&
          status === "pending" &&
          cancelRequested !== true
        );
      }),
    ).toBe(true);
  });

  test("GET /api/v1/sessions/:id 与 /events 返回结构，events limit 非法返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("session-detail-events");
    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `会话详情数据源-${nonce}`,
        type: "ssh",
        location: `10.32.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
      }),
    });
    const source = (await createSourceResponse.json()) as Source;
    expect(createSourceResponse.status).toBe(201);

    const inserted = await insertSessionForSearch(source.id, {
      provider: "session-detail-test",
      tool: "Codex CLI",
      model: `gpt-5-session-${nonce}`,
      project: `workspace-${nonce}`,
      sourcePath: `/workspace/${nonce}/src/main.ts`,
      startedAt: "2026-02-01T10:00:00.000Z",
      endedAt: "2026-02-01T10:02:00.000Z",
      tokens: 64,
      cost: 0.064,
      eventTexts: ["hello detail"],
    });

    try {
      const detailResult = await requestFirstSuccessful([
        {
          path: `/api/v1/sessions/${encodeURIComponent(inserted.id)}`,
          init: { headers: authHeaders },
        },
        {
          path: `/api/v1/sessions/${encodeURIComponent(inserted.id)}/detail`,
          init: { headers: authHeaders },
        },
      ]);
      assertApiStatus(detailResult, [200]);

      const detail = collectPayloadCandidates(detailResult.payload).find(
        (candidate) =>
          pickString(candidate, ["id", "sessionId", "session_id"]) ===
          inserted.id,
      );
      expect(detail).toBeDefined();
      if (detail) {
        expect(pickString(detail, ["sourceId", "source_id"])).toBe(source.id);
        expect(pickString(detail, ["tool"])).toBe("Codex CLI");
        expect(pickString(detail, ["model"])).toBe(`gpt-5-session-${nonce}`);
        if (isRecord(detail)) {
          const tokenBreakdown = detail.tokenBreakdown;
          const sourceTrace = detail.sourceTrace;
          const sessionPayload = detail.session;

          expect(isRecord(tokenBreakdown)).toBe(true);
          if (isRecord(tokenBreakdown)) {
            expect(tokenBreakdown.inputTokens).toBe(0);
            expect(tokenBreakdown.outputTokens).toBe(0);
            expect(tokenBreakdown.cacheReadTokens).toBe(0);
            expect(tokenBreakdown.cacheWriteTokens).toBe(0);
            expect(tokenBreakdown.reasoningTokens).toBe(0);
            expect(tokenBreakdown.totalTokens).toBe(64);
          }

          expect(isRecord(sourceTrace)).toBe(true);
          if (isRecord(sourceTrace)) {
            expect(pickString(sourceTrace, ["sourceId", "source_id"])).toBe(
              source.id,
            );
            expect(pickString(sourceTrace, ["provider"])).toBe(
              "session-detail-test",
            );
            expect(pickString(sourceTrace, ["path"])).toBe(
              `/workspace/${nonce}/src/main.ts`,
            );
          }

          expect(isRecord(sessionPayload)).toBe(true);
          if (isRecord(sessionPayload)) {
            expect(
              pickString(sessionPayload, ["id", "sessionId", "session_id"]),
            ).toBe(inserted.id);
            expect(pickString(sessionPayload, ["provider"])).toBe(
              "session-detail-test",
            );
          }
        }
      }

      const eventsResult = await requestFirstSuccessful([
        {
          path: `/api/v1/sessions/${encodeURIComponent(inserted.id)}/events?limit=20`,
          init: { headers: authHeaders },
        },
        {
          path: `/api/v1/session-events?sessionId=${encodeURIComponent(inserted.id)}&limit=20`,
          init: { headers: authHeaders },
        },
      ]);
      assertApiStatus(eventsResult, [200]);
      const eventItems = extractListItems(eventsResult.payload);
      expect(Array.isArray(eventItems)).toBe(true);
      if (eventItems.length > 0) {
        const first = eventItems[0];
        expect(typeof pickString(first, ["sessionId", "session_id"])).toBe(
          "string",
        );
        expect(typeof pickString(first, ["eventType", "event_type"])).toBe(
          "string",
        );
      }

      const invalidLimitResult = await requestFirstAvailable([
        {
          path: `/api/v1/sessions/${encodeURIComponent(inserted.id)}/events?limit=0`,
          init: { headers: authHeaders },
        },
        {
          path: `/api/v1/session-events?sessionId=${encodeURIComponent(inserted.id)}&limit=0`,
          init: { headers: authHeaders },
        },
      ]);
      expect(invalidLimitResult.response.status).toBe(400);
      if (isRecord(invalidLimitResult.payload)) {
        const message = pickString(invalidLimitResult.payload, [
          "message",
          "error",
        ]);
        expect(typeof message).toBe("string");
        expect(message?.toLowerCase()).toContain("limit");
      }
    } finally {
      await inserted.cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
    }
  });

  test("GET /api/v1/sessions/:id 与 /events 缺少认证时返回 401", async () => {
    const detailResponse = await app.request(
      "/api/v1/sessions/session-without-auth",
    );
    expect(detailResponse.status).toBe(401);

    const eventsResponse = await app.request(
      "/api/v1/sessions/session-without-auth/events",
    );
    expect(eventsResponse.status).toBe(401);
  });

  test("GET /api/v1/sessions/:id 与 /events 参数非法或不存在时返回 400/404", async () => {
    const authHeaders = await resolveAuthHeaders();

    const invalidDetailResponse = await app.request("/api/v1/sessions/%20", {
      headers: authHeaders,
    });
    expect(invalidDetailResponse.status).toBe(400);

    const missingDetailResponse = await app.request(
      "/api/v1/sessions/session-not-found",
      {
        headers: authHeaders,
      },
    );
    expect(missingDetailResponse.status).toBe(404);

    const invalidEventsResponse = await app.request(
      "/api/v1/sessions/%20/events",
      {
        headers: authHeaders,
      },
    );
    expect(invalidEventsResponse.status).toBe(400);

    const missingEventsResponse = await app.request(
      "/api/v1/sessions/session-not-found/events",
      {
        headers: authHeaders,
      },
    );
    expect(missingEventsResponse.status).toBe(404);
  });

  test("POST /api/v1/sessions/search 参数非法时返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/sessions/search", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        from: "2026-03-02T00:00:00.000Z",
        to: "2026-03-01T00:00:00.000Z",
        limit: 0,
      }),
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(400);
    expect(typeof body.message).toBe("string");
    expect(body.message.length).toBeGreaterThan(0);
  });

  test("POST /api/v1/sessions/search 新增过滤字段非法时返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/sessions/search", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        clientType: "   ",
      }),
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(400);
    expect(body.message).toContain("clientType");
  });

  test("POST /api/v1/sessions/search 返回结构包含 filters/items/total", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/sessions/search", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        sourceId: "source-for-structure-check",
        keyword: "gpt",
        clientType: "codex",
        tool: "Codex CLI",
        host: "127.0.0.1",
        model: "gpt-5-codex",
        project: "agent-ledger",
        from: "2026-01-01T00:00:00.000Z",
        to: "2026-12-31T23:59:59.999Z",
        limit: 10,
      }),
    });
    const body = (await response.json()) as SessionSearchResponse;

    expect(response.status).toBe(200);
    expect(Array.isArray(body.items)).toBe(true);
    expect(typeof body.total).toBe("number");
    expect(body.nextCursor).toBeNull();
    expect(body.filters.sourceId).toBe("source-for-structure-check");
    expect(body.filters.keyword).toBe("gpt");
    expect(body.filters.clientType).toBe("codex");
    expect(body.filters.tool).toBe("Codex CLI");
    expect(body.filters.host).toBe("127.0.0.1");
    expect(body.filters.model).toBe("gpt-5-codex");
    expect(body.filters.project).toBe("agent-ledger");
    expect(body.filters.from).toBe("2026-01-01T00:00:00.000Z");
    expect(body.filters.to).toBe("2026-12-31T23:59:59.999Z");
    expect(body.filters.limit).toBe(10);
    expect(Array.isArray(body.sourceFreshness)).toBe(true);
  });

  test("POST /api/v1/sessions/search cursor 非法时返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/sessions/search", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        cursor: "not-a-valid-cursor",
      }),
    });
    const body = (await response.json()) as { message: string };

    expect(response.status).toBe(400);
    expect(body.message).toContain("cursor");
  });

  test("POST /api/v1/sessions/search 支持 cursor 翻页", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("sessions-search-cursor");
    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `cursor 检索源-${nonce}`,
        type: "ssh",
        location: `10.48.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
        accessMode: "sync",
      }),
    });
    expect(createSourceResponse.status).toBe(201);
    const source = (await createSourceResponse.json()) as Source;

    const first = await insertSessionForSearch(source.id, {
      provider: `provider-${nonce}`,
      tool: "Codex CLI",
      model: "gpt-5-codex",
      startedAt: "2026-03-02T12:00:00.000Z",
      endedAt: "2026-03-02T12:05:00.000Z",
      tokens: 30,
      cost: 0.03,
    });
    const second = await insertSessionForSearch(source.id, {
      provider: `provider-${nonce}`,
      tool: "Codex CLI",
      model: "gpt-5-codex",
      startedAt: "2026-03-02T11:00:00.000Z",
      endedAt: "2026-03-02T11:05:00.000Z",
      tokens: 31,
      cost: 0.031,
    });
    const third = await insertSessionForSearch(source.id, {
      provider: `provider-${nonce}`,
      tool: "Codex CLI",
      model: "gpt-5-codex",
      startedAt: "2026-03-02T10:00:00.000Z",
      endedAt: "2026-03-02T10:05:00.000Z",
      tokens: 32,
      cost: 0.032,
    });

    try {
      const firstPageResponse = await app.request("/api/v1/sessions/search", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          sourceId: source.id,
          limit: 2,
        }),
      });
      const firstPage =
        (await firstPageResponse.json()) as SessionSearchResponse;

      expect(firstPageResponse.status).toBe(200);
      expect(firstPage.total).toBe(3);
      expect(firstPage.items.map((item) => item.id)).toEqual([
        first.id,
        second.id,
      ]);
      expect(typeof firstPage.nextCursor).toBe("string");

      const secondPageResponse = await app.request("/api/v1/sessions/search", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          sourceId: source.id,
          limit: 2,
          cursor: firstPage.nextCursor,
        }),
      });
      const secondPage =
        (await secondPageResponse.json()) as SessionSearchResponse;

      expect(secondPageResponse.status).toBe(200);
      expect(secondPage.total).toBe(3);
      expect(secondPage.items.map((item) => item.id)).toEqual([third.id]);
      expect(secondPage.nextCursor).toBeNull();
    } finally {
      await first.cleanup();
      await second.cleanup();
      await third.cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
    }
  });

  test("GET /api/v1/sessions/:id/events 支持 cursor 翻页", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("session-events-cursor");
    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `cursor 事件源-${nonce}`,
        type: "ssh",
        location: `10.49.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
        accessMode: "sync",
      }),
    });
    expect(createSourceResponse.status).toBe(201);
    const source = (await createSourceResponse.json()) as Source;
    const inserted = await insertSessionForSearch(source.id, {
      provider: `provider-${nonce}`,
      tool: "Codex CLI",
      model: "gpt-5-codex",
      startedAt: "2026-03-02T09:00:00.000Z",
      endedAt: "2026-03-02T09:10:00.000Z",
      eventTexts: ["event-1", "event-2", "event-3"],
    });

    try {
      const firstPageResponse = await app.request(
        `/api/v1/sessions/${encodeURIComponent(inserted.id)}/events?limit=2`,
        {
          headers: authHeaders,
        },
      );
      const firstPage = (await firstPageResponse.json()) as {
        items: Array<{ id: string }>;
        total: number;
        nextCursor: string | null;
      };

      expect(firstPageResponse.status).toBe(200);
      expect(firstPage.total).toBe(3);
      expect(firstPage.items).toHaveLength(2);
      expect(typeof firstPage.nextCursor).toBe("string");

      const secondPageResponse = await app.request(
        `/api/v1/sessions/${encodeURIComponent(inserted.id)}/events?limit=2&cursor=${encodeURIComponent(
          String(firstPage.nextCursor),
        )}`,
        {
          headers: authHeaders,
        },
      );
      const secondPage = (await secondPageResponse.json()) as {
        items: Array<{ id: string }>;
        total: number;
        nextCursor: string | null;
      };

      expect(secondPageResponse.status).toBe(200);
      expect(secondPage.total).toBe(3);
      expect(secondPage.items).toHaveLength(1);
      expect(secondPage.nextCursor).toBeNull();
    } finally {
      await inserted.cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
    }
  });

  test("GET /api/v1/sessions/:id/events cursor 非法时返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("session-events-invalid-cursor");
    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `cursor 非法事件源-${nonce}`,
        type: "ssh",
        location: `10.50.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
        accessMode: "sync",
      }),
    });
    expect(createSourceResponse.status).toBe(201);
    const source = (await createSourceResponse.json()) as Source;
    const inserted = await insertSessionForSearch(source.id, {
      provider: `provider-${nonce}`,
      tool: "Codex CLI",
      model: "gpt-5-codex",
      startedAt: "2026-03-02T09:00:00.000Z",
      endedAt: "2026-03-02T09:10:00.000Z",
      eventTexts: ["event-invalid-cursor"],
    });

    try {
      const response = await app.request(
        `/api/v1/sessions/${encodeURIComponent(inserted.id)}/events?cursor=not-a-valid-cursor`,
        {
          headers: authHeaders,
        },
      );
      const body = (await response.json()) as { message: string };

      expect(response.status).toBe(400);
      expect(body.message).toContain("cursor");
    } finally {
      await inserted.cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
    }
  });

  test("POST /api/v1/sessions/search 对 ssh realtime source 走 puller realtime 并返回 sourceFreshness", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("sessions-search-realtime-ok");
    const originalFetch = globalThis.fetch;
    const originalPullerBaseUrl = Bun.env.PULLER_BASE_URL;
    const originalPullerSyncTimeout = Bun.env.PULLER_SYNC_TIMEOUT_MS;

    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `实时检索源-${nonce}`,
        type: "ssh",
        location: `10.44.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
        accessMode: "realtime",
      }),
    });
    expect(createSourceResponse.status).toBe(201);
    const source = (await createSourceResponse.json()) as Source;
    const inserted = await insertSessionForSearch(source.id, {
      provider: `provider-${nonce}`,
      tool: "Codex CLI",
      model: "gpt-5-codex",
      tokens: 42,
      cost: 0.042,
    });

    let pullerCalls = 0;
    try {
      Bun.env.PULLER_BASE_URL = "http://puller.mock";
      Bun.env.PULLER_SYNC_TIMEOUT_MS = "not-a-number";
      globalThis.fetch = (async (input: RequestInfo | URL) => {
        const url =
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input instanceof Request
                ? input.url
                : String(input);
        if (
          url ===
          `http://puller.mock/v1/sources/${encodeURIComponent(source.id)}/sync-now`
        ) {
          pullerCalls += 1;
          return new Response(JSON.stringify({ accepted: true }), {
            status: 202,
            headers: {
              "content-type": "application/json",
            },
          });
        }
        throw new Error(`unexpected fetch url in realtime test: ${url}`);
      }) as unknown as typeof fetch;

      const response = await app.request("/api/v1/sessions/search", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          sourceId: source.id,
          limit: 20,
        }),
      });
      const body = (await response.json()) as SessionSearchResponse & {
        sourceFreshness?: Array<{
          fetchPath?: string;
          freshnessMinutes?: number | null;
          fallbackReason?: string | null;
          accessMode?: string | null;
          sourceId?: string;
        }>;
      };
      const sourceFreshness = body.sourceFreshness?.[0];

      expect(response.status).toBe(200);
      expect(pullerCalls).toBe(1);
      expect(body.total).toBe(1);
      expect(body.items.map((item) => item.id)).toEqual([inserted.id]);
      expect(Array.isArray(body.sourceFreshness)).toBe(true);
      expect(sourceFreshness?.sourceId).toBe(source.id);
      expect(sourceFreshness?.fetchPath).toBe("realtime");
      expect(sourceFreshness?.fallbackReason).toBeNull();
      expect(sourceFreshness?.accessMode).toBe("realtime");
      expect(
        sourceFreshness?.freshnessMinutes === null ||
          typeof sourceFreshness?.freshnessMinutes === "number",
      ).toBe(true);
    } finally {
      await inserted.cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
      if (originalPullerBaseUrl === undefined) {
        delete Bun.env.PULLER_BASE_URL;
      } else {
        Bun.env.PULLER_BASE_URL = originalPullerBaseUrl;
      }
      if (originalPullerSyncTimeout === undefined) {
        delete Bun.env.PULLER_SYNC_TIMEOUT_MS;
      } else {
        Bun.env.PULLER_SYNC_TIMEOUT_MS = originalPullerSyncTimeout;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("POST /api/v1/sessions/search puller 失败时回退缓存并标注 sourceFreshness", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("sessions-search-realtime-fallback");
    const originalFetch = globalThis.fetch;
    const originalPullerBaseUrl = Bun.env.PULLER_BASE_URL;
    const originalRetryMaxAttempts = Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS;
    const originalRetryBaseBackoffMs =
      Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS;
    const originalRetryMaxBackoffMs = Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS;

    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `回退检索源-${nonce}`,
        type: "ssh",
        location: `10.45.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
        accessMode: "hybrid",
      }),
    });
    expect(createSourceResponse.status).toBe(201);
    const source = (await createSourceResponse.json()) as Source;
    const inserted = await insertSessionForSearch(source.id, {
      provider: `provider-${nonce}`,
      tool: "Codex CLI",
      model: "gpt-5-codex",
      tokens: 51,
      cost: 0.051,
    });

    let pullerCalls = 0;
    try {
      Bun.env.PULLER_BASE_URL = "http://puller.mock";
      Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS = "2";
      Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS = "1";
      Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS = "1";
      globalThis.fetch = (async () => {
        pullerCalls += 1;
        return new Response(
          JSON.stringify({ message: "upstream unavailable" }),
          {
            status: 503,
            headers: {
              "content-type": "application/json",
            },
          },
        );
      }) as unknown as typeof fetch;

      const response = await app.request("/api/v1/sessions/search", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          sourceId: source.id,
          limit: 20,
        }),
      });
      const body = (await response.json()) as SessionSearchResponse & {
        sourceFreshness?: Array<{
          fetchPath?: string;
          freshnessMinutes?: number | null;
          fallbackReason?: string | null;
          accessMode?: string | null;
        }>;
      };
      const sourceFreshness = body.sourceFreshness?.[0];

      expect(response.status).toBe(200);
      expect(body.total).toBe(1);
      expect(body.items.map((item) => item.id)).toEqual([inserted.id]);
      expect(Array.isArray(body.sourceFreshness)).toBe(true);
      expect(sourceFreshness?.fetchPath).toBe("fallback-cache");
      expect(sourceFreshness?.fallbackReason).toBe("puller_http_503");
      expect(sourceFreshness?.accessMode).toBe("hybrid");
      expect(pullerCalls).toBe(2);
    } finally {
      await inserted.cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
      if (originalPullerBaseUrl === undefined) {
        delete Bun.env.PULLER_BASE_URL;
      } else {
        Bun.env.PULLER_BASE_URL = originalPullerBaseUrl;
      }
      if (originalRetryMaxAttempts === undefined) {
        delete Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS;
      } else {
        Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS = originalRetryMaxAttempts;
      }
      if (originalRetryBaseBackoffMs === undefined) {
        delete Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS;
      } else {
        Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS = originalRetryBaseBackoffMs;
      }
      if (originalRetryMaxBackoffMs === undefined) {
        delete Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS;
      } else {
        Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS = originalRetryMaxBackoffMs;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("POST /api/v1/sessions/search puller 短暂失败后重试成功", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("sessions-search-retry-success");
    const originalFetch = globalThis.fetch;
    const originalPullerBaseUrl = Bun.env.PULLER_BASE_URL;
    const originalRetryMaxAttempts = Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS;
    const originalRetryBaseBackoffMs =
      Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS;
    const originalRetryMaxBackoffMs = Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS;

    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `重试成功检索源-${nonce}`,
        type: "ssh",
        location: `10.46.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
        accessMode: "realtime",
      }),
    });
    expect(createSourceResponse.status).toBe(201);
    const source = (await createSourceResponse.json()) as Source;
    const inserted = await insertSessionForSearch(source.id, {
      provider: `provider-${nonce}`,
      tool: "Codex CLI",
      model: "gpt-5-codex",
      tokens: 58,
      cost: 0.058,
    });

    let pullerCalls = 0;
    try {
      Bun.env.PULLER_BASE_URL = "http://puller.mock";
      Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS = "3";
      Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS = "1";
      Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS = "1";
      globalThis.fetch = (async () => {
        pullerCalls += 1;
        if (pullerCalls < 3) {
          return new Response(
            JSON.stringify({ message: "temporary unavailable" }),
            {
              status: 503,
              headers: {
                "content-type": "application/json",
              },
            },
          );
        }
        return new Response(JSON.stringify({ accepted: true }), {
          status: 202,
          headers: {
            "content-type": "application/json",
          },
        });
      }) as unknown as typeof fetch;

      const response = await app.request("/api/v1/sessions/search", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          sourceId: source.id,
          limit: 20,
        }),
      });
      const body = (await response.json()) as SessionSearchResponse & {
        sourceFreshness?: Array<{
          fetchPath?: string;
          freshnessMinutes?: number | null;
          fallbackReason?: string | null;
          accessMode?: string | null;
        }>;
      };
      const sourceFreshness = body.sourceFreshness?.[0];

      expect(response.status).toBe(200);
      expect(pullerCalls).toBe(3);
      expect(body.total).toBe(1);
      expect(body.items.map((item) => item.id)).toEqual([inserted.id]);
      expect(Array.isArray(body.sourceFreshness)).toBe(true);
      expect(sourceFreshness?.fetchPath).toBe("realtime");
      expect(sourceFreshness?.fallbackReason).toBeNull();
      expect(sourceFreshness?.accessMode).toBe("realtime");
    } finally {
      await inserted.cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
      if (originalPullerBaseUrl === undefined) {
        delete Bun.env.PULLER_BASE_URL;
      } else {
        Bun.env.PULLER_BASE_URL = originalPullerBaseUrl;
      }
      if (originalRetryMaxAttempts === undefined) {
        delete Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS;
      } else {
        Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS = originalRetryMaxAttempts;
      }
      if (originalRetryBaseBackoffMs === undefined) {
        delete Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS;
      } else {
        Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS = originalRetryBaseBackoffMs;
      }
      if (originalRetryMaxBackoffMs === undefined) {
        delete Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS;
      } else {
        Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS = originalRetryMaxBackoffMs;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("POST /api/v1/sessions/search puller 4xx 不重试且重试参数按边界夹取", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("sessions-search-retry-clamp");
    const originalFetch = globalThis.fetch;
    const originalPullerBaseUrl = Bun.env.PULLER_BASE_URL;
    const originalPullerSyncTimeout = Bun.env.PULLER_SYNC_TIMEOUT_MS;
    const originalRetryMaxAttempts = Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS;
    const originalRetryBaseBackoffMs =
      Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS;
    const originalRetryMaxBackoffMs = Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS;

    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `重试边界检索源-${nonce}`,
        type: "ssh",
        location: `10.47.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
        accessMode: "realtime",
      }),
    });
    expect(createSourceResponse.status).toBe(201);
    const source = (await createSourceResponse.json()) as Source;
    const inserted = await insertSessionForSearch(source.id, {
      provider: `provider-${nonce}`,
      tool: "Codex CLI",
      model: "gpt-5-codex",
      tokens: 61,
      cost: 0.061,
    });

    let pullerCalls = 0;
    try {
      Bun.env.PULLER_BASE_URL = "http://puller.mock";
      Bun.env.PULLER_SYNC_TIMEOUT_MS = "999999";
      Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS = "999";
      Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS = "500";
      Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS = "1";
      globalThis.fetch = (async () => {
        pullerCalls += 1;
        return new Response(JSON.stringify({ message: "bad request" }), {
          status: 400,
          headers: {
            "content-type": "application/json",
          },
        });
      }) as unknown as typeof fetch;

      const response = await app.request("/api/v1/sessions/search", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          sourceId: source.id,
          limit: 20,
        }),
      });
      const body = (await response.json()) as SessionSearchResponse & {
        sourceFreshness?: Array<{
          fetchPath?: string;
          fallbackReason?: string | null;
        }>;
      };
      const sourceFreshness = body.sourceFreshness?.[0];

      expect(response.status).toBe(200);
      expect(pullerCalls).toBe(1);
      expect(body.total).toBe(1);
      expect(body.items.map((item) => item.id)).toEqual([inserted.id]);
      expect(Array.isArray(body.sourceFreshness)).toBe(true);
      expect(sourceFreshness?.fetchPath).toBe("fallback-cache");
      expect(sourceFreshness?.fallbackReason).toBe("puller_http_400");
    } finally {
      await inserted.cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
      if (originalPullerBaseUrl === undefined) {
        delete Bun.env.PULLER_BASE_URL;
      } else {
        Bun.env.PULLER_BASE_URL = originalPullerBaseUrl;
      }
      if (originalPullerSyncTimeout === undefined) {
        delete Bun.env.PULLER_SYNC_TIMEOUT_MS;
      } else {
        Bun.env.PULLER_SYNC_TIMEOUT_MS = originalPullerSyncTimeout;
      }
      if (originalRetryMaxAttempts === undefined) {
        delete Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS;
      } else {
        Bun.env.PULLER_SYNC_RETRY_MAX_ATTEMPTS = originalRetryMaxAttempts;
      }
      if (originalRetryBaseBackoffMs === undefined) {
        delete Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS;
      } else {
        Bun.env.PULLER_SYNC_RETRY_BASE_BACKOFF_MS = originalRetryBaseBackoffMs;
      }
      if (originalRetryMaxBackoffMs === undefined) {
        delete Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS;
      } else {
        Bun.env.PULLER_SYNC_RETRY_MAX_BACKOFF_MS = originalRetryMaxBackoffMs;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("sessions/search 与 exports/sessions 支持 clientType/tool/host/model/project 过滤", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const host = `host-${nonce}.internal`;
    const clientType = `client-${nonce}`;
    const tool = `tool-${nonce}`;
    const model = `model-${nonce}`;
    const project = `project-${nonce}`;

    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `会话过滤源-${nonce}`,
        type: "ssh",
        location: host,
      }),
    });
    expect(createSourceResponse.status).toBe(201);
    const source = (await createSourceResponse.json()) as Source;

    const matched = await insertSessionForSearch(source.id, {
      provider: clientType,
      tool,
      model,
      project,
      tokens: 120,
      cost: 0.12,
    });
    const unmatched = await insertSessionForSearch(source.id, {
      provider: `other-${nonce}`,
      tool,
      model,
      project,
      tokens: 90,
      cost: 0.09,
    });

    try {
      const searchResponse = await app.request("/api/v1/sessions/search", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          sourceId: source.id,
          clientType,
          tool,
          host,
          model,
          project,
          limit: 20,
        }),
      });
      const searchBody = (await searchResponse.json()) as SessionSearchResponse;
      expect(searchResponse.status).toBe(200);
      expect(searchBody.total).toBe(1);
      expect(searchBody.items.map((item) => item.id)).toEqual([matched.id]);

      const exportQuery = new URLSearchParams({
        format: "json",
        sourceId: source.id,
        clientType,
        tool,
        host,
        model,
        project,
        limit: "20",
      });
      const exportResponse = await app.request(
        `/api/v1/exports/sessions?${exportQuery.toString()}`,
        {
          headers: authHeaders,
        },
      );
      const exportBody = (await exportResponse.json()) as SessionSearchResponse;

      expect(exportResponse.status).toBe(200);
      expect(exportBody.total).toBe(1);
      expect(exportBody.items.map((item) => item.id)).toEqual([matched.id]);
      expect(exportBody.filters.sourceId).toBe(source.id);
      expect(exportBody.filters.clientType).toBe(clientType);
      expect(exportBody.filters.tool).toBe(tool);
      expect(exportBody.filters.host).toBe(host);
      expect(exportBody.filters.model).toBe(model);
      expect(exportBody.filters.project).toBe(project);
    } finally {
      await matched.cleanup();
      await unmatched.cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
    }
  });

  test("POST /api/v1/sessions/search keyword 可命中 events 正文", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("sessions-event-keyword");
    const keyword = `event-body-${nonce}`;
    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `正文检索源-${nonce}`,
        type: "ssh",
        location: `10.33.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
      }),
    });
    expect(createSourceResponse.status).toBe(201);
    const source = (await createSourceResponse.json()) as Source;

    const matched = await insertSessionForSearch(source.id, {
      provider: "event-body-provider",
      tool: "Codex CLI",
      model: "gpt-5-codex",
      tokens: 30,
      cost: 0.03,
      eventTexts: [`这是正文关键词：${keyword}`],
    });
    const unmatched = await insertSessionForSearch(source.id, {
      provider: "event-body-provider",
      tool: "Codex CLI",
      model: "gpt-5-codex",
      tokens: 28,
      cost: 0.028,
      eventTexts: ["这条正文不包含目标关键词。"],
    });

    try {
      const searchResponse = await app.request("/api/v1/sessions/search", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          sourceId: source.id,
          keyword,
          limit: 20,
        }),
      });
      const searchBody = (await searchResponse.json()) as SessionSearchResponse;

      expect(searchResponse.status).toBe(200);
      expect(searchBody.total).toBe(1);
      expect(searchBody.items.map((item) => item.id)).toEqual([matched.id]);
    } finally {
      await matched.cleanup();
      await unmatched.cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
    }
  });

  test("GET /api/v1/exports/sessions 支持 json/csv，并写入 export_requested 审计", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const jsonKeyword = `export-json-${nonce}`;
    const csvKeyword = `export-csv-${nonce}`;

    const jsonResponse = await app.request(
      `/api/v1/exports/sessions?format=json&keyword=${encodeURIComponent(jsonKeyword)}`,
      {
        headers: authHeaders,
      },
    );
    const jsonBody = (await jsonResponse.json()) as SessionSearchResponse;

    expect(jsonResponse.status).toBe(200);
    expect(Array.isArray(jsonBody.items)).toBe(true);
    expect(typeof jsonBody.total).toBe("number");
    expect(jsonBody.nextCursor).toBeNull();
    expect(jsonBody.filters.keyword).toBe(jsonKeyword);

    const csvResponse = await app.request(
      `/api/v1/exports/sessions?format=csv&keyword=${encodeURIComponent(csvKeyword)}`,
      {
        headers: authHeaders,
      },
    );
    const csvBody = await csvResponse.text();

    expect(csvResponse.status).toBe(200);
    expect(csvResponse.headers.get("content-type")).toContain("text/csv");
    expect(csvResponse.headers.get("content-disposition")).toContain(
      'attachment; filename="sessions-',
    );
    expect(csvBody.split("\n")[0]).toBe(
      "id,sourceId,tool,model,startedAt,endedAt,tokens,cost",
    );

    const jsonAuditQuery = new URLSearchParams({
      action: "control_plane.export_requested",
      keyword: jsonKeyword,
      limit: "200",
    });
    const jsonAuditResponse = await app.request(
      `/api/v1/audits?${jsonAuditQuery.toString()}`,
      {
        headers: authHeaders,
      },
    );
    const jsonAudits = (await jsonAuditResponse.json()) as {
      items: Array<{
        action: string;
        metadata: Record<string, unknown>;
      }>;
      total: number;
      filters: AuditListInput & {
        action?: string;
        keyword?: string;
        limit?: number;
      };
    };

    expect(jsonAuditResponse.status).toBe(200);
    expect(Array.isArray(jsonAudits.items)).toBe(true);
    expect(typeof jsonAudits.total).toBe("number");
    expect(jsonAudits.filters.action).toBe("control_plane.export_requested");
    expect(jsonAudits.filters.keyword).toBe(jsonKeyword);
    expect(
      jsonAudits.items.some((item) => {
        const format = item.metadata.format;
        return (
          item.action === "control_plane.export_requested" && format === "json"
        );
      }),
    ).toBe(true);

    const csvAuditQuery = new URLSearchParams({
      action: "control_plane.export_requested",
      keyword: csvKeyword,
      limit: "200",
    });
    const csvAuditResponse = await app.request(
      `/api/v1/audits?${csvAuditQuery.toString()}`,
      {
        headers: authHeaders,
      },
    );
    const csvAudits = (await csvAuditResponse.json()) as {
      items: Array<{
        action: string;
        metadata: Record<string, unknown>;
      }>;
      total: number;
      filters: AuditListInput & {
        action?: string;
        keyword?: string;
        limit?: number;
      };
    };

    expect(csvAuditResponse.status).toBe(200);
    expect(Array.isArray(csvAudits.items)).toBe(true);
    expect(typeof csvAudits.total).toBe("number");
    expect(csvAudits.filters.action).toBe("control_plane.export_requested");
    expect(csvAudits.filters.keyword).toBe(csvKeyword);
    expect(
      csvAudits.items.some((item) => {
        const format = item.metadata.format;
        return (
          item.action === "control_plane.export_requested" && format === "csv"
        );
      }),
    ).toBe(true);
  });

  test("GET /api/v1/exports/usage 支持 daily/weekly 的 json/csv 导出", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const from = "2026-02-24T00:00:00.000Z";
    const to = "2026-03-09T00:00:00.000Z";
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    const proxyBaseUrl = "http://127.0.0.1:19120";
    const fetchCalls: string[] = [];

    try {
      Bun.env.ANALYTICS_PROXY_ENABLED = "true";
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      globalThis.fetch = (async (input: unknown) => {
        const url = input instanceof Request ? input.url : String(input);
        fetchCalls.push(url);
        return new Response(
          JSON.stringify({
            metric: "tokens",
            timezone: "Asia/Shanghai",
            weeks: [
              {
                week_start: "2026-02-24",
                week_end: "2026-03-02",
                tokens: 3200,
                cost: 1.23,
                sessions: 4,
              },
              {
                weekStart: "2026-03-03",
                weekEnd: "2026-03-09",
                tokens: 1800,
                cost: 0.88,
                sessions: 3,
              },
            ],
            summary: {
              tokens: 5000,
              cost: 2.11,
              sessions: 7,
            },
          }),
          {
            status: 200,
            headers: { "content-type": "application/json" },
          },
        );
      }) as unknown as typeof fetch;

      const jsonResponse = await app.request(
        `/api/v1/exports/usage?format=json&dimension=daily&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&limit=30`,
        {
          headers: authHeaders,
        },
      );
      const jsonBody = (await jsonResponse.json()) as {
        items: UsageDailyItem[];
        total: number;
        filters: {
          dimension: string;
          from?: string;
          to?: string;
          limit?: number;
        };
      };

      expect(jsonResponse.status).toBe(200);
      expect(Array.isArray(jsonBody.items)).toBe(true);
      expect(typeof jsonBody.total).toBe("number");
      expect(jsonBody.filters.dimension).toBe("daily");
      expect(jsonBody.filters.limit).toBe(30);

      const csvResponse = await app.request(
        `/api/v1/exports/usage?format=csv&dimension=weekly&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&timezone=${encodeURIComponent("Asia/Shanghai")}&limit=1`,
        {
          headers: authHeaders,
        },
      );
      const csvBody = await csvResponse.text();

      expect(fetchCalls.length).toBe(1);
      const forwardedUrl = new URL(fetchCalls[0]);
      expect(`${forwardedUrl.origin}${forwardedUrl.pathname}`).toBe(
        `${proxyBaseUrl}/v1/usage/weekly-summary`,
      );
      expect(forwardedUrl.searchParams.get("tenant_id")).toBe(authTenantId);
      expect(forwardedUrl.searchParams.get("from")).toBe(from);
      expect(forwardedUrl.searchParams.get("to")).toBe(to);
      expect(forwardedUrl.searchParams.get("tz")).toBe("Asia/Shanghai");
      expect(forwardedUrl.searchParams.has("limit")).toBe(false);

      expect(csvResponse.status).toBe(200);
      expect(csvResponse.headers.get("content-type")).toContain("text/csv");
      expect(csvResponse.headers.get("content-disposition")).toContain(
        'attachment; filename="usage-weekly-',
      );
      expect(csvBody.split("\n")[0]).toBe(
        "weekStart,weekEnd,tokens,cost,sessions",
      );
      expect(csvBody.split("\n")[1]).toBe("2026-03-03,2026-03-09,1800,0.88,3");

      const auditResponse = await app.request(
        "/api/v1/audits?action=control_plane.export_requested&limit=200",
        {
          headers: authHeaders,
        },
      );
      const audits = (await auditResponse.json()) as {
        items: Array<{
          action: string;
          metadata: Record<string, unknown>;
        }>;
      };
      expect(auditResponse.status).toBe(200);
      expect(
        audits.items.some(
          (item) =>
            item.action === "control_plane.export_requested" &&
            item.metadata.target === "usage" &&
            item.metadata.dimension === "daily" &&
            item.metadata.format === "json",
        ),
      ).toBe(true);
      expect(
        audits.items.some(
          (item) =>
            item.action === "control_plane.export_requested" &&
            item.metadata.target === "usage" &&
            item.metadata.dimension === "weekly" &&
            item.metadata.format === "csv",
        ),
      ).toBe(true);
    } finally {
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/exports/usage weekly 代理关闭时返回 503", async () => {
    const authHeaders = await resolveAuthHeaders();
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    let fetchCount = 0;

    try {
      Bun.env.ANALYTICS_PROXY_ENABLED = "false";
      Bun.env.ANALYTICS_BASE_URL = "http://127.0.0.1:19121";
      globalThis.fetch = (async () => {
        fetchCount += 1;
        return new Response(JSON.stringify({}), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }) as unknown as typeof fetch;

      const response = await app.request(
        "/api/v1/exports/usage?format=json&dimension=weekly&from=2026-02-24T00%3A00%3A00.000Z&to=2026-03-09T00%3A00%3A00.000Z",
        {
          headers: authHeaders,
        },
      );
      const body = (await response.json()) as {
        message?: string;
      };

      expect(fetchCount).toBe(0);
      expect(response.status).toBe(503);
      expect(body.message).toBe(
        "ANALYTICS_PROXY_ENABLED=false 时无法查询 weekly summary。",
      );
    } finally {
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/exports/usage weekly 代理失败时返回 502", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalProxyEnabled = Bun.env.ANALYTICS_PROXY_ENABLED;
    const originalBaseUrl = Bun.env.ANALYTICS_BASE_URL;
    const originalFetch = globalThis.fetch;
    const proxyBaseUrl = "http://127.0.0.1:19122";
    const fetchCalls: string[] = [];

    try {
      Bun.env.ANALYTICS_PROXY_ENABLED = "true";
      Bun.env.ANALYTICS_BASE_URL = proxyBaseUrl;
      globalThis.fetch = (async (input: unknown) => {
        const url = input instanceof Request ? input.url : String(input);
        fetchCalls.push(url);
        return new Response(JSON.stringify({ message: "upstream down" }), {
          status: 502,
          headers: { "content-type": "application/json" },
        });
      }) as unknown as typeof fetch;

      const response = await app.request(
        "/api/v1/exports/usage?format=json&dimension=weekly&from=2026-02-24T00%3A00%3A00.000Z&to=2026-03-09T00%3A00%3A00.000Z&timezone=Asia%2FShanghai",
        {
          headers: authHeaders,
        },
      );
      const body = (await response.json()) as {
        message?: string;
      };

      expect(fetchCalls).toEqual([
        `${proxyBaseUrl}/v1/usage/weekly-summary?tenant_id=${encodeURIComponent(authTenantId)}&from=2026-02-24T00%3A00%3A00.000Z&to=2026-03-09T00%3A00%3A00.000Z&tz=Asia%2FShanghai`,
      ]);
      expect(response.status).toBe(502);
      expect(body.message).toBe("query usage weekly summary failed");
    } finally {
      if (originalProxyEnabled === undefined) {
        delete Bun.env.ANALYTICS_PROXY_ENABLED;
      } else {
        Bun.env.ANALYTICS_PROXY_ENABLED = originalProxyEnabled;
      }
      if (originalBaseUrl === undefined) {
        delete Bun.env.ANALYTICS_BASE_URL;
      } else {
        Bun.env.ANALYTICS_BASE_URL = originalBaseUrl;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("GET /api/v1/exports/usage 参数非法返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request(
      "/api/v1/exports/usage?format=csv&dimension=unknown",
      {
        headers: authHeaders,
      },
    );
    const payload = (await response.json()) as { message?: string };

    expect(response.status).toBe(400);
    expect(typeof payload.message).toBe("string");
  });

  test("异步导出 job 支持 创建->完成->下载链路（json）并写入 export_requested/export_completed 审计", async () => {
    const nonce = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const keyword = `async-export-${nonce}`;

    const createdJob = await createAsyncExportJob("json", keyword);
    expect(typeof createdJob.jobId).toBe("string");
    expect(createdJob.jobId.length).toBeGreaterThan(0);

    const completed = await pollExportJobUntilDone(
      createdJob.jobId,
      createdJob.statusPath,
    );
    const downloadResponse = await downloadExportResult(
      createdJob.jobId,
      completed.downloadPath ?? createdJob.downloadPath,
    );
    expect(downloadResponse.status).toBe(200);

    const contentType = (
      downloadResponse.headers.get("content-type") ?? ""
    ).toLowerCase();
    if (contentType.includes("application/json")) {
      const payload = (await downloadResponse.json()) as
        | SessionSearchResponse
        | {
            items?: unknown[];
          };
      if ("items" in payload) {
        expect(Array.isArray(payload.items)).toBe(true);
      } else {
        expect(payload).toBeDefined();
      }
    } else {
      const text = await downloadResponse.text();
      expect(text.length).toBeGreaterThan(0);
    }

    const requestedAudits = await queryAuditByAction(
      "control_plane.export_requested",
      keyword,
    );
    expect(
      requestedAudits.items.some((item) =>
        auditMatchesKeyword(item, "control_plane.export_requested", keyword),
      ),
    ).toBe(true);

    const completedAudits = await queryAuditByAction(
      "control_plane.export_completed",
      keyword,
    );
    expect(
      completedAudits.items.some((item) =>
        auditMatchesKeyword(item, "control_plane.export_completed", keyword),
      ),
    ).toBe(true);
  }, 15_000);

  test("Pricing catalog 读写与 versions 列表返回结构正确", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("pricing-catalog");
    const pricingInput = {
      note: `pricing-note-${nonce}`,
      entries: [
        {
          model: `gpt-5-input-${nonce}`,
          inputPer1k: 0.003,
          outputPer1k: 0.012,
          cacheReadPer1k: 0.0005,
          cacheWritePer1k: 0.001,
          reasoningPer1k: 0.002,
          currency: "USD",
        },
        {
          model: `gpt-5-lite-${nonce}`,
          inputPer1k: 0.001,
          outputPer1k: 0.004,
          currency: "USD",
        },
      ],
    };

    const upsertResult = await requestFirstSuccessful([
      {
        path: "/api/v1/pricing/catalog",
        init: jsonRequest("PUT", pricingInput, authHeaders),
      },
      {
        path: "/api/v1/pricing/catalog",
        init: jsonRequest("POST", pricingInput, authHeaders),
      },
      {
        path: "/api/v1/pricing-catalog",
        init: jsonRequest("PUT", pricingInput, authHeaders),
      },
      {
        path: "/api/v1/pricing-catalog",
        init: jsonRequest("POST", pricingInput, authHeaders),
      },
    ]);
    assertApiStatus(upsertResult, [200, 201]);

    const upsertedCatalog = extractPricingCatalogFromPayload(
      upsertResult.payload,
    );
    expect(upsertedCatalog).not.toBeNull();
    if (!upsertedCatalog) {
      throw new Error(
        `pricing upsert 返回结构缺少 version/entries: ${JSON.stringify(upsertResult.payload)}`,
      );
    }
    expect(Array.isArray(upsertedCatalog.entries)).toBe(true);
    expect(upsertedCatalog.entries.length).toBeGreaterThanOrEqual(1);

    const readResult = await requestFirstSuccessful([
      {
        path: "/api/v1/pricing/catalog",
        init: { headers: authHeaders },
      },
      {
        path: "/api/v1/pricing-catalog",
        init: { headers: authHeaders },
      },
    ]);
    assertApiStatus(readResult, [200]);

    const currentCatalog = extractPricingCatalogFromPayload(readResult.payload);
    expect(currentCatalog).not.toBeNull();
    if (!currentCatalog) {
      throw new Error(
        `pricing get 返回结构缺少 version/entries: ${JSON.stringify(readResult.payload)}`,
      );
    }
    expect(Array.isArray(currentCatalog.entries)).toBe(true);
    expect(currentCatalog.entries.length).toBeGreaterThanOrEqual(1);

    const readModels = new Set(
      currentCatalog.entries
        .map((entry) => pickString(entry, ["model"]))
        .filter((model): model is string => typeof model === "string"),
    );
    expect(readModels.has(pricingInput.entries[0].model)).toBe(true);

    const versionsResult = await requestFirstSuccessful([
      {
        path: "/api/v1/pricing/catalog/versions?limit=20",
        init: { headers: authHeaders },
      },
      {
        path: "/api/v1/pricing-catalog/versions?limit=20",
        init: { headers: authHeaders },
      },
      {
        path: "/api/v1/pricing/versions?limit=20",
        init: { headers: authHeaders },
      },
    ]);
    assertApiStatus(versionsResult, [200]);

    const versions = extractListItems(versionsResult.payload);
    expect(Array.isArray(versions)).toBe(true);
    expect(versions.length).toBeGreaterThanOrEqual(1);
    if (versions.length > 0) {
      const first = versions[0];
      expect(typeof pickString(first, ["id", "versionId", "version_id"])).toBe(
        "string",
      );
      expect(typeof first.version).toBe("number");
      expect(typeof pickString(first, ["createdAt", "created_at"])).toBe(
        "string",
      );
    }
  });

  test("PUT/GET /api/v1/budgets 使用 auth tenant，忽略伪造 x-tenant-id", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authTenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const spoofTenantId = `tenant-spoof-${Date.now().toString(36)}`;

    const putResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        "x-tenant-id": spoofTenantId,
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "global",
        period: "monthly",
        tokenLimit: 200000,
        costLimit: 0,
        alertThreshold: 0.8,
      }),
    });
    const budget = (await putResponse.json()) as Budget;

    expect(putResponse.status).toBe(200);
    expect(budget.scope).toBe("global");
    expect(budget.period).toBe("monthly");
    expect(budget.tokenLimit).toBe(200000);
    expect(budget.costLimit).toBe(0);

    const listResponse = await app.request("/api/v1/budgets", {
      headers: {
        "x-tenant-id": spoofTenantId,
        ...authHeaders,
      },
    });
    const listed = (await listResponse.json()) as {
      items: Budget[];
      total: number;
    };

    expect(listResponse.status).toBe(200);
    expect(listed.items.some((item) => item.id === budget.id)).toBe(true);
    expect(listed.total).toBe(listed.items.length);

    const query = new URLSearchParams({
      action: "control_plane.budget_upserted",
      keyword: budget.id,
      limit: "200",
    });
    const auditResponse = await app.request(
      `/api/v1/audits?${query.toString()}`,
      {
        headers: authHeaders,
      },
    );
    const audits = (await auditResponse.json()) as {
      items: Array<{
        action: string;
        metadata: Record<string, unknown>;
      }>;
    };
    expect(auditResponse.status).toBe(200);
    expect(
      audits.items.some((item) => {
        const metadataTenantId =
          item.metadata.tenantId ?? item.metadata.tenant_id;
        return (
          item.action === "control_plane.budget_upserted" &&
          metadataTenantId === authTenantId
        );
      }),
    ).toBe(true);
  });

  test("PUT /api/v1/budgets 会写入 budget_upserted 审计且 tenantId 正确", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);

    const putResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "global",
        period: "monthly",
        tokenLimit: 12345,
        costLimit: 0,
        alertThreshold: 0.75,
      }),
    });
    const budget = (await putResponse.json()) as Budget;

    expect(putResponse.status).toBe(200);
    expect(typeof budget.id).toBe("string");

    const query = new URLSearchParams({
      action: "control_plane.budget_upserted",
      keyword: budget.id,
      limit: "200",
    });
    const auditResponse = await app.request(
      `/api/v1/audits?${query.toString()}`,
      {
        headers: authHeaders,
      },
    );
    const audits = (await auditResponse.json()) as {
      items: Array<{
        action: string;
        metadata: Record<string, unknown>;
      }>;
      total: number;
      filters: AuditListInput & {
        action?: string;
        keyword?: string;
        limit?: number;
      };
    };

    expect(auditResponse.status).toBe(200);
    expect(Array.isArray(audits.items)).toBe(true);
    expect(typeof audits.total).toBe("number");
    expect(audits.filters.action).toBe("control_plane.budget_upserted");
    expect(audits.filters.keyword).toBe(budget.id);

    const targetAudit = audits.items.find((item) => {
      const resourceId = item.metadata.resourceId;
      const metadataTenantId = item.metadata.tenantId;
      return (
        item.action === "control_plane.budget_upserted" &&
        resourceId === budget.id &&
        metadataTenantId === tenantId
      );
    });
    expect(targetAudit).toBeDefined();
  });

  test("PUT /api/v1/budgets 严格校验（scope=source 必须 sourceId）", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "source",
        period: "monthly",
        tokenLimit: 1,
        costLimit: 0,
        alertThreshold: 0.8,
      }),
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(400);
    expect(body.message).toContain("sourceId");
  });

  test("PUT /api/v1/budgets 严格校验（tokenLimit/costLimit 不能同时为 0）", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "global",
        period: "daily",
        tokenLimit: 0,
        costLimit: 0,
        alertThreshold: 0.8,
      }),
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(400);
    expect(body.message).toContain("不能同时为 0");
  });

  test("PUT/GET /api/v1/budgets 支持 thresholds 三段和 org/user/model 范围查询", async () => {
    const authHeaders = await resolveAuthHeaders();
    const authContext = await getDefaultAuthContext();
    const scopedUserId =
      authContext.userId ??
      resolveUserIdFromAccessToken(authContext.accessToken);
    if (!scopedUserId) {
      throw new Error(
        "无法解析当前登录用户 userId，无法执行 scope=user 预算测试。",
      );
    }
    const nonce = createNonce("budget-thresholds-scope");
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const createOrgResult = await createOrganizationByAuth(
      authContext.accessToken,
      {
        tenantId,
        name: `预算组织-${nonce}`,
        slug: `budget-org-${nonce}`,
      },
      scopedUserId,
    );
    assertApiStatus(createOrgResult, [201]);
    const organizationId = extractEntityId(createOrgResult.payload);
    if (!organizationId) {
      throw new Error(
        `预算组织创建响应缺少 organizationId，path=${createOrgResult.path}，payload=${JSON.stringify(
          createOrgResult.payload,
        )}`,
      );
    }
    const model = `gpt-5-${nonce}`;

    const putOrgResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "org",
        organizationId,
        period: "monthly",
        tokenLimit: 1000,
        costLimit: 0,
        thresholds: {
          warning: 0.6,
          escalated: 0.75,
          critical: 0.9,
        },
      }),
    });
    const orgBudget = (await putOrgResponse.json()) as Budget;
    expect(putOrgResponse.status).toBe(200);
    expect(orgBudget.scope).toBe("org");
    expect(orgBudget.organizationId).toBe(organizationId);
    expect(orgBudget.thresholds.warning).toBe(0.6);
    expect(orgBudget.thresholds.escalated).toBe(0.75);
    expect(orgBudget.thresholds.critical).toBe(0.9);

    const putUserResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "user",
        userId: scopedUserId,
        period: "monthly",
        tokenLimit: 900,
        costLimit: 0,
        thresholds: {
          warning: 0.5,
          escalated: 0.7,
          critical: 0.88,
        },
      }),
    });
    const userBudget = (await putUserResponse.json()) as Budget;
    expect(putUserResponse.status).toBe(200);
    expect(userBudget.scope).toBe("user");
    expect(userBudget.userId).toBe(scopedUserId);

    const putModelResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "model",
        model,
        period: "monthly",
        tokenLimit: 800,
        costLimit: 0,
        thresholds: {
          warning: 0.55,
          escalated: 0.72,
          critical: 0.9,
        },
      }),
    });
    const modelBudget = (await putModelResponse.json()) as Budget;
    expect(putModelResponse.status).toBe(200);
    expect(modelBudget.scope).toBe("model");
    expect(modelBudget.model).toBe(model);

    const orgListResponse = await app.request(
      `/api/v1/budgets?scope=org&organizationId=${encodeURIComponent(organizationId)}`,
      { headers: authHeaders },
    );
    const orgList = (await orgListResponse.json()) as {
      items: Budget[];
    };
    expect(orgListResponse.status).toBe(200);
    expect(orgList.items.some((item) => item.id === orgBudget.id)).toBe(true);

    const userListResponse = await app.request(
      `/api/v1/budgets?scope=user&userId=${encodeURIComponent(scopedUserId)}`,
      { headers: authHeaders },
    );
    const userList = (await userListResponse.json()) as {
      items: Budget[];
    };
    expect(userListResponse.status).toBe(200);
    expect(userList.items.some((item) => item.id === userBudget.id)).toBe(true);

    const modelListResponse = await app.request(
      `/api/v1/budgets?scope=model&model=${encodeURIComponent(model)}`,
      { headers: authHeaders },
    );
    const modelList = (await modelListResponse.json()) as {
      items: Budget[];
    };
    expect(modelListResponse.status).toBe(200);
    expect(modelList.items.some((item) => item.id === modelBudget.id)).toBe(
      true,
    );
  });

  test("PUT /api/v1/budgets 严格校验（scope=model 必须 model）", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "model",
        period: "monthly",
        tokenLimit: 1000,
        costLimit: 0,
        thresholds: {
          warning: 0.6,
          escalated: 0.75,
          critical: 0.9,
        },
      }),
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(400);
    expect(body.message).toContain("scope=model");
  });

  test("PUT /api/v1/budgets 绑定校验（scope=org 组织需存在且属于当前租户）", async () => {
    const nonce = createNonce("budget-org-binding");
    const ownerA = await registerAndLoginUser(`${nonce}-owner-a`);
    const ownerB = await registerAndLoginUser(`${nonce}-owner-b`);
    if (!ownerA.userId || !ownerB.userId) {
      throw new Error(
        "无法解析 owner userId，无法执行 scope=org 绑定校验测试。",
      );
    }

    const tenantAResult = await createTenantByAuth(
      ownerA.accessToken,
      {
        name: `预算租户A-${nonce}`,
        slug: `budget-tenant-a-${nonce}`,
      },
      ownerA.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error(
        `预算租户A创建响应缺少 tenantId，path=${tenantAResult.path}，payload=${JSON.stringify(
          tenantAResult.payload,
        )}`,
      );
    }

    const tenantBResult = await createTenantByAuth(
      ownerB.accessToken,
      {
        name: `预算租户B-${nonce}`,
        slug: `budget-tenant-b-${nonce}`,
      },
      ownerB.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error(
        `预算租户B创建响应缺少 tenantId，path=${tenantBResult.path}，payload=${JSON.stringify(
          tenantBResult.payload,
        )}`,
      );
    }

    const createOrgResult = await createOrganizationByAuth(
      ownerB.accessToken,
      {
        tenantId: tenantBId,
        name: `预算组织B-${nonce}`,
        slug: `budget-org-b-${nonce}`,
      },
      ownerB.userId,
    );
    assertApiStatus(createOrgResult, [201]);
    const crossTenantOrganizationId = extractEntityId(createOrgResult.payload);
    if (!crossTenantOrganizationId) {
      throw new Error(
        `预算组织B创建响应缺少 organizationId，path=${createOrgResult.path}，payload=${JSON.stringify(
          createOrgResult.payload,
        )}`,
      );
    }

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      ownerA.accessToken,
      ownerA.userId,
    );

    const crossTenantOrgResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        scope: "org",
        organizationId: crossTenantOrganizationId,
        period: "monthly",
        tokenLimit: 500,
        costLimit: 0,
        alertThreshold: 0.8,
      }),
    });
    const crossTenantOrgBody = (await crossTenantOrgResponse.json()) as {
      message: string;
    };
    expect(crossTenantOrgResponse.status).toBe(400);
    expect(crossTenantOrgBody.message).toContain("organizationId");

    const missingOrgResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        scope: "org",
        organizationId: `missing-org-${nonce}`,
        period: "monthly",
        tokenLimit: 500,
        costLimit: 0,
        alertThreshold: 0.8,
      }),
    });
    const missingOrgBody = (await missingOrgResponse.json()) as {
      message: string;
    };
    expect(missingOrgResponse.status).toBe(400);
    expect(missingOrgBody.message).toContain("organizationId");
  });

  test("PUT /api/v1/budgets 绑定校验（scope=user 用户需存在且属于当前租户）", async () => {
    const nonce = createNonce("budget-user-binding");
    const owner = await registerAndLoginUser(`${nonce}-owner`);
    const outsider = await registerAndLoginUser(`${nonce}-outsider`);
    const ownerUserId =
      owner.userId ?? resolveUserIdFromAccessToken(owner.accessToken);
    const outsiderUserId =
      outsider.userId ?? resolveUserIdFromAccessToken(outsider.accessToken);
    if (!ownerUserId || !outsiderUserId) {
      throw new Error(
        "无法解析用户 userId，无法执行 scope=user 绑定校验测试。",
      );
    }

    const tenantResult = await createTenantByAuth(
      owner.accessToken,
      {
        name: `预算用户租户-${nonce}`,
        slug: `budget-user-tenant-${nonce}`,
      },
      ownerUserId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error(
        `预算用户租户创建响应缺少 tenantId，path=${tenantResult.path}，payload=${JSON.stringify(
          tenantResult.payload,
        )}`,
      );
    }

    const tenantHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      owner.accessToken,
      ownerUserId,
    );

    const crossTenantUserResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...tenantHeaders,
      },
      body: JSON.stringify({
        scope: "user",
        userId: outsiderUserId,
        period: "monthly",
        tokenLimit: 500,
        costLimit: 0,
        alertThreshold: 0.8,
      }),
    });
    const crossTenantUserBody = (await crossTenantUserResponse.json()) as {
      message: string;
    };
    expect(crossTenantUserResponse.status).toBe(400);
    expect(crossTenantUserBody.message).toContain("userId");

    const missingUserResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...tenantHeaders,
      },
      body: JSON.stringify({
        scope: "user",
        userId: `missing-user-${nonce}`,
        period: "monthly",
        tokenLimit: 500,
        costLimit: 0,
        alertThreshold: 0.8,
      }),
    });
    const missingUserBody = (await missingUserResponse.json()) as {
      message: string;
    };
    expect(missingUserResponse.status).toBe(400);
    expect(missingUserBody.message).toContain("userId");
  });

  test("POST /api/v1/budgets/:id/release-requests 双人审批通过后执行解冻", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const reviewer = await registerAndLoginUser(
      createNonce("budget-release-reviewer"),
    );
    const reviewerHeaders = await resolveAuthHeaders(
      reviewer.accessToken,
      reviewer.userId,
    );
    const secondReviewer = await registerAndLoginUser(
      createNonce("budget-release-reviewer-2"),
    );
    const secondReviewerHeaders = await resolveAuthHeaders(
      secondReviewer.accessToken,
      secondReviewer.userId,
    );

    const putBudgetResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "global",
        period: "monthly",
        tokenLimit: 1200,
        costLimit: 0,
        thresholds: {
          warning: 0.6,
          escalated: 0.8,
          critical: 0.95,
        },
      }),
    });
    const budget = (await putBudgetResponse.json()) as Budget;
    expect(putBudgetResponse.status).toBe(200);

    const { alert, cleanup } = await createTestAlert(tenantId, "open", {
      budgetId: budget.id,
      severity: "critical",
    });
    try {
      const ackResponse = await app.request(
        `/api/v1/alerts/${alert.id}/status`,
        {
          method: "PATCH",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({
            status: "acknowledged",
          }),
        },
      );
      expect(ackResponse.status).toBe(200);

      const frozenListResponse = await app.request("/api/v1/budgets", {
        headers: authHeaders,
      });
      const frozenList = (await frozenListResponse.json()) as {
        items: Budget[];
      };
      const frozenBudget = frozenList.items.find(
        (item) => item.id === budget.id,
      );
      expect(frozenBudget?.governanceState).toBe("frozen");
      expect(frozenBudget?.frozenByAlertId).toBe(alert.id);

      const createReleaseResponse = await app.request(
        `/api/v1/budgets/${budget.id}/release-requests`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({
            reason: "人工确认后申请释放。",
          }),
        },
      );
      const createdRelease = (await createReleaseResponse.json()) as {
        id: string;
        status: string;
        approvals: Array<{ userId: string }>;
      };
      expect(createReleaseResponse.status).toBe(201);
      expect(createdRelease.status).toBe("pending");
      expect(Array.isArray(createdRelease.approvals)).toBe(true);
      expect(createdRelease.approvals.length).toBe(0);

      const requesterApproveResponse = await app.request(
        `/api/v1/budgets/${budget.id}/release-requests/${createdRelease.id}/approve`,
        {
          method: "POST",
          headers: authHeaders,
        },
      );
      const requesterApproveBody = (await requesterApproveResponse.json()) as {
        message: string;
      };
      expect(requesterApproveResponse.status).toBe(400);
      expect(requesterApproveBody.message).toContain("申请人");

      const firstApproveResponse = await app.request(
        `/api/v1/budgets/${budget.id}/release-requests/${createdRelease.id}/approve`,
        {
          method: "POST",
          headers: reviewerHeaders,
        },
      );
      const firstApprove = (await firstApproveResponse.json()) as {
        status: string;
        approvals: Array<{ userId: string }>;
      };
      expect(firstApproveResponse.status).toBe(200);
      expect(firstApprove.status).toBe("pending");
      expect(firstApprove.approvals.length).toBe(1);

      const duplicateApproveResponse = await app.request(
        `/api/v1/budgets/${budget.id}/release-requests/${createdRelease.id}/approve`,
        {
          method: "POST",
          headers: reviewerHeaders,
        },
      );
      const duplicateApproveBody = (await duplicateApproveResponse.json()) as {
        message: string;
      };
      expect(duplicateApproveResponse.status).toBe(400);
      expect(duplicateApproveBody.message).toContain("同一用户");

      const secondApproveResponse = await app.request(
        `/api/v1/budgets/${budget.id}/release-requests/${createdRelease.id}/approve`,
        {
          method: "POST",
          headers: secondReviewerHeaders,
        },
      );
      const secondApprove = (await secondApproveResponse.json()) as {
        status: string;
        approvals: Array<{ userId: string }>;
      };
      expect(secondApproveResponse.status).toBe(200);
      expect(secondApprove.status).toBe("executed");
      expect(secondApprove.approvals.length).toBe(2);

      const activeListResponse = await app.request("/api/v1/budgets", {
        headers: authHeaders,
      });
      const activeList = (await activeListResponse.json()) as {
        items: Budget[];
      };
      const activeBudget = activeList.items.find(
        (item) => item.id === budget.id,
      );
      expect(activeBudget?.governanceState).toBe("active");
      expect(activeBudget?.freezeReason).toBeUndefined();
      expect(activeBudget?.frozenAt).toBeUndefined();
      expect(activeBudget?.frozenByAlertId).toBeUndefined();
    } finally {
      await cleanup();
    }
  });

  test("GET /api/v1/budgets/:id/release-requests 支持 status/limit 过滤", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const reviewer = await registerAndLoginUser(
      createNonce("budget-release-list-reviewer"),
    );
    const reviewerHeaders = await resolveAuthHeaders(
      reviewer.accessToken,
      reviewer.userId,
    );
    const secondReviewer = await registerAndLoginUser(
      createNonce("budget-release-list-reviewer-2"),
    );
    const secondReviewerHeaders = await resolveAuthHeaders(
      secondReviewer.accessToken,
      secondReviewer.userId,
    );

    const putBudgetResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "global",
        period: "monthly",
        tokenLimit: 1400,
        costLimit: 0,
        thresholds: {
          warning: 0.6,
          escalated: 0.8,
          critical: 0.95,
        },
      }),
    });
    const budget = (await putBudgetResponse.json()) as Budget;
    expect(putBudgetResponse.status).toBe(200);

    const { alert, cleanup } = await createTestAlert(tenantId, "open", {
      budgetId: budget.id,
      severity: "critical",
    });

    try {
      const ackResponse = await app.request(
        `/api/v1/alerts/${alert.id}/status`,
        {
          method: "PATCH",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({
            status: "acknowledged",
          }),
        },
      );
      expect(ackResponse.status).toBe(200);

      const createReleaseResponse = await app.request(
        `/api/v1/budgets/${budget.id}/release-requests`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({
            reason: "用于列表过滤测试。",
          }),
        },
      );
      const createdRelease = (await createReleaseResponse.json()) as {
        id: string;
        status: string;
      };
      expect(createReleaseResponse.status).toBe(201);
      expect(createdRelease.status).toBe("pending");

      const pendingListResponse = await app.request(
        `/api/v1/budgets/${budget.id}/release-requests?status=pending&limit=1`,
        {
          headers: authHeaders,
        },
      );
      const pendingListPayload =
        await readResponseAsUnknown(pendingListResponse);
      expect(pendingListResponse.status).toBe(200);
      const pendingItems = extractListItems(pendingListPayload);
      expect(pendingItems.length).toBe(1);
      expect(pickString(pendingItems[0], ["id"])).toBe(createdRelease.id);
      expect(pickString(pendingItems[0], ["status"])).toBe("pending");

      const firstApproveResponse = await app.request(
        `/api/v1/budgets/${budget.id}/release-requests/${createdRelease.id}/approve`,
        {
          method: "POST",
          headers: reviewerHeaders,
        },
      );
      expect(firstApproveResponse.status).toBe(200);

      const secondApproveResponse = await app.request(
        `/api/v1/budgets/${budget.id}/release-requests/${createdRelease.id}/approve`,
        {
          method: "POST",
          headers: secondReviewerHeaders,
        },
      );
      expect(secondApproveResponse.status).toBe(200);

      const executedListResponse = await app.request(
        `/api/v1/budgets/${budget.id}/release-requests?status=executed&limit=10`,
        {
          headers: authHeaders,
        },
      );
      const executedListPayload =
        await readResponseAsUnknown(executedListResponse);
      expect(executedListResponse.status).toBe(200);
      const executedItems = extractListItems(executedListPayload);
      expect(
        executedItems.some(
          (item) =>
            pickString(item, ["id"]) === createdRelease.id &&
            pickString(item, ["status"]) === "executed",
        ),
      ).toBe(true);

      const invalidStatusResponse = await app.request(
        `/api/v1/budgets/${budget.id}/release-requests?status=processing`,
        {
          headers: authHeaders,
        },
      );
      const invalidStatusPayload = (await invalidStatusResponse.json()) as {
        message: string;
      };
      expect(invalidStatusResponse.status).toBe(400);
      expect(invalidStatusPayload.message).toContain("status");
    } finally {
      await cleanup();
    }
  });

  test("GET /api/v1/budgets 鉴权中间件边界：token/session 异常统一返回 401", async () => {
    const authContext = await getDefaultAuthContext();
    const userId =
      authContext.userId ??
      resolveUserIdFromAccessToken(authContext.accessToken);
    if (!userId) {
      throw new Error("无法解析默认用户 userId，无法覆盖鉴权中间件分支。");
    }
    if (typeof repository.createAuthSession !== "function") {
      throw new Error(
        "repository.createAuthSession 不可用，无法构造会话分支。",
      );
    }

    const expectUnauthorized = async (
      headers: Record<string, string>,
      expectedMessageFragment: string,
    ) => {
      const response = await app.request("/api/v1/budgets", { headers });
      const body = (await response.json()) as { message?: string };
      expect(response.status).toBe(401);
      expect(String(body.message ?? "")).toContain(expectedMessageFragment);
    };

    await expectUnauthorized(
      {
        authorization: "Token not-bearer",
      },
      "认证凭证格式无效",
    );

    await expectUnauthorized(
      {
        authorization: "Bearer invalid-token",
      },
      "访问令牌无效或已过期",
    );

    const tokenWithoutSessionId = issueAccessToken({
      userId,
      tenantId: "default",
    }).token;
    await expectUnauthorized(
      buildAuthHeaders(tokenWithoutSessionId, userId),
      "访问令牌缺少会话信息",
    );

    const tokenWithMissingSession = issueAccessToken({
      userId,
      tenantId: "default",
      sessionId: createNonce("missing-auth-session"),
    }).token;
    await expectUnauthorized(
      buildAuthHeaders(tokenWithMissingSession, userId),
      "登录会话不存在或已失效",
    );

    const expiredSession = await repository.createAuthSession({
      userId,
      tenantId: "default",
      sessionToken: createAuthSessionToken(),
      expiresAt: new Date(Date.now() - 60_000).toISOString(),
    });
    const tokenWithExpiredSession = issueAccessToken({
      userId,
      tenantId: "default",
      sessionId: expiredSession.id,
    }).token;
    await expectUnauthorized(
      buildAuthHeaders(tokenWithExpiredSession, userId),
      "登录会话已失效",
    );

    const activeSession = await repository.createAuthSession({
      userId,
      tenantId: "default",
      sessionToken: createAuthSessionToken(),
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    const tokenWithTenantMismatch = issueAccessToken({
      userId,
      tenantId: createNonce("tenant-mismatch"),
      sessionId: activeSession.id,
    }).token;
    await expectUnauthorized(
      buildAuthHeaders(tokenWithTenantMismatch, userId),
      "访问令牌与登录会话不匹配",
    );
  });

  test("POST /api/v1/integrations/callbacks/alerts 未配置 secret 返回 500", async () => {
    const originalCallbackSecret = Bun.env.INTEGRATION_CALLBACK_SECRET;
    delete Bun.env.INTEGRATION_CALLBACK_SECRET;
    try {
      const response = await app.request(
        "/api/v1/integrations/callbacks/alerts",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({
            callback_id: createNonce("cb-secret-not-configured"),
            tenant_id: "default",
            action: "resolve",
            alert_id: "alert-not-important",
          }),
        },
      );
      const body = (await response.json()) as {
        message?: string;
      };
      expect(response.status).toBe(500);
      expect(String(body.message ?? "")).toContain("未配置");
    } finally {
      if (originalCallbackSecret === undefined) {
        delete Bun.env.INTEGRATION_CALLBACK_SECRET;
      } else {
        Bun.env.INTEGRATION_CALLBACK_SECRET = originalCallbackSecret;
      }
    }
  });

  test("POST /api/v1/integrations/callbacks/alerts 参数非法返回 400", async () => {
    const originalCallbackSecret = Bun.env.INTEGRATION_CALLBACK_SECRET;
    const callbackSecret = `integration-secret-${createNonce("cb-invalid-payload-secret")}`;
    try {
      Bun.env.INTEGRATION_CALLBACK_SECRET = callbackSecret;
      const response = await postIntegrationAlertCallback(callbackSecret, {
        callback_id: createNonce("cb-invalid-payload"),
        tenant_id: "default",
        action: "resolve",
      });
      const body = (await response.json()) as {
        message?: string;
      };
      expect(response.status).toBe(400);
      expect(typeof body.message).toBe("string");
      expect((body.message ?? "").length).toBeGreaterThan(0);
    } finally {
      if (originalCallbackSecret === undefined) {
        delete Bun.env.INTEGRATION_CALLBACK_SECRET;
      } else {
        Bun.env.INTEGRATION_CALLBACK_SECRET = originalCallbackSecret;
      }
    }
  });

  test("POST /api/v1/integrations/callbacks/alerts 未携带 secret 返回 401", async () => {
    const originalCallbackSecret = Bun.env.INTEGRATION_CALLBACK_SECRET;
    Bun.env.INTEGRATION_CALLBACK_SECRET = `integration-secret-${createNonce("cb-no-secret")}`;
    try {
      const response = await app.request(
        "/api/v1/integrations/callbacks/alerts",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({
            callback_id: createNonce("cb-missing-secret"),
            tenant_id: "default",
            action: "resolve",
            alert_id: "not-important",
          }),
        },
      );
      const body = (await response.json()) as {
        message?: string;
      };
      expect(response.status).toBe(401);
      expect(String(body.message ?? "")).toContain("未授权");
    } finally {
      if (originalCallbackSecret === undefined) {
        delete Bun.env.INTEGRATION_CALLBACK_SECRET;
      } else {
        Bun.env.INTEGRATION_CALLBACK_SECRET = originalCallbackSecret;
      }
    }
  });

  test("POST /api/v1/integrations/callbacks/alerts 签名鉴权成功", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalCallbackSecret = Bun.env.INTEGRATION_CALLBACK_SECRET;
    const callbackSecret = `integration-secret-${createNonce("cb-signature-success")}`;
    const { alert, cleanup } = await createTestAlert(tenantId, "open");

    try {
      Bun.env.INTEGRATION_CALLBACK_SECRET = callbackSecret;
      const response = await postIntegrationAlertCallback(callbackSecret, {
        callback_id: createNonce("cb-signature-ok"),
        tenant_id: tenantId,
        action: "resolve",
        alert_id: alert.id,
      });
      const body = (await response.json()) as {
        duplicate: boolean;
        result: {
          alert?: Alert;
        };
      };

      expect(response.status).toBe(200);
      expect(body.duplicate).toBe(false);
      expect(body.result.alert?.id).toBe(alert.id);
      expect(body.result.alert?.status).toBe("resolved");
    } finally {
      if (originalCallbackSecret === undefined) {
        delete Bun.env.INTEGRATION_CALLBACK_SECRET;
      } else {
        Bun.env.INTEGRATION_CALLBACK_SECRET = originalCallbackSecret;
      }
      await cleanup();
    }
  });

  test("POST /api/v1/integrations/callbacks/alerts 签名错误返回 401", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalCallbackSecret = Bun.env.INTEGRATION_CALLBACK_SECRET;
    const callbackSecret = `integration-secret-${createNonce("cb-signature-invalid")}`;

    try {
      Bun.env.INTEGRATION_CALLBACK_SECRET = callbackSecret;
      const response = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-signature-invalid"),
          tenant_id: tenantId,
          action: "resolve",
          alert_id: "alert-not-important",
        },
        {
          signature: "invalid-signature",
        },
      );
      const body = (await response.json()) as {
        message?: string;
      };

      expect(response.status).toBe(401);
      expect(String(body.message ?? "")).toContain("signature");
    } finally {
      if (originalCallbackSecret === undefined) {
        delete Bun.env.INTEGRATION_CALLBACK_SECRET;
      } else {
        Bun.env.INTEGRATION_CALLBACK_SECRET = originalCallbackSecret;
      }
    }
  });

  test("POST /api/v1/integrations/callbacks/alerts 过期 timestamp 返回 401", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalCallbackSecret = Bun.env.INTEGRATION_CALLBACK_SECRET;
    const callbackSecret = `integration-secret-${createNonce("cb-timestamp-expired")}`;

    try {
      Bun.env.INTEGRATION_CALLBACK_SECRET = callbackSecret;
      const response = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-expired-timestamp"),
          tenant_id: tenantId,
          action: "resolve",
          alert_id: "alert-not-important",
        },
        {
          timestamp: String(Date.now() - 6 * 60 * 1000),
        },
      );
      const body = (await response.json()) as {
        message?: string;
      };

      expect(response.status).toBe(401);
      expect(String(body.message ?? "")).toContain("timestamp");
    } finally {
      if (originalCallbackSecret === undefined) {
        delete Bun.env.INTEGRATION_CALLBACK_SECRET;
      } else {
        Bun.env.INTEGRATION_CALLBACK_SECRET = originalCallbackSecret;
      }
    }
  });

  test("POST /api/v1/integrations/callbacks/alerts nonce 重放返回 401", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalCallbackSecret = Bun.env.INTEGRATION_CALLBACK_SECRET;
    const callbackSecret = `integration-secret-${createNonce("cb-replay")}`;
    const { alert, cleanup } = await createTestAlert(tenantId, "open");
    const replayNonce = createNonce("cb-replay-nonce");
    const replayTimestamp = String(Date.now());
    const replayPayload: Record<string, unknown> = {
      callback_id: createNonce("cb-replay-callback"),
      tenant_id: tenantId,
      action: "resolve",
      alert_id: alert.id,
    };

    try {
      Bun.env.INTEGRATION_CALLBACK_SECRET = callbackSecret;
      const firstResponse = await postIntegrationAlertCallback(
        callbackSecret,
        replayPayload,
        {
          timestamp: replayTimestamp,
          nonce: replayNonce,
        },
      );
      expect(firstResponse.status).toBe(200);

      const secondResponse = await postIntegrationAlertCallback(
        callbackSecret,
        replayPayload,
        {
          timestamp: replayTimestamp,
          nonce: replayNonce,
        },
      );
      const secondBody = (await secondResponse.json()) as {
        message?: string;
      };

      expect(secondResponse.status).toBe(401);
      expect(String(secondBody.message ?? "")).toContain("nonce");
    } finally {
      if (originalCallbackSecret === undefined) {
        delete Bun.env.INTEGRATION_CALLBACK_SECRET;
      } else {
        Bun.env.INTEGRATION_CALLBACK_SECRET = originalCallbackSecret;
      }
      await cleanup();
    }
  });

  test("POST /api/v1/integrations/callbacks/alerts 同 callback_id 在不同 tenant 不冲突", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantAId = resolveTenantIdFromAuthHeaders(authHeaders);
    const tenantBOwner = await registerAndLoginUser(
      createNonce("cb-tenant-owner-b"),
    );
    if (!tenantBOwner.userId) {
      throw new Error("无法解析租户 B owner userId。");
    }

    const tenantBResult = await createTenantByAuth(
      tenantBOwner.accessToken,
      {
        name: `callback-tenant-b-${createNonce("cb-tenant-name-b")}`,
        slug: `callback-tenant-b-${createNonce("cb-tenant-slug-b")}`,
      },
      tenantBOwner.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error(
        `租户 B 创建响应缺少 tenantId，path=${tenantBResult.path}，payload=${JSON.stringify(
          tenantBResult.payload,
        )}`,
      );
    }

    const alertA = await createTestAlert(tenantAId, "open");
    const alertB = await createTestAlert(tenantBId, "open");
    const originalCallbackSecret = Bun.env.INTEGRATION_CALLBACK_SECRET;
    const callbackSecret = `integration-secret-${createNonce("cb-cross-tenant-secret")}`;

    try {
      Bun.env.INTEGRATION_CALLBACK_SECRET = callbackSecret;
      const sharedCallbackId = createNonce("cb-cross-tenant-shared");

      const tenantAResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: sharedCallbackId,
          tenant_id: tenantAId,
          action: "resolve",
          alert_id: alertA.alert.id,
        },
      );
      const tenantABody = (await tenantAResponse.json()) as {
        duplicate: boolean;
        result: {
          alert?: Alert;
        };
      };
      expect(tenantAResponse.status).toBe(200);
      expect(tenantABody.duplicate).toBe(false);
      expect(tenantABody.result.alert?.id).toBe(alertA.alert.id);
      expect(tenantABody.result.alert?.tenantId).toBe(tenantAId);

      const tenantBResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: sharedCallbackId,
          tenant_id: tenantBId,
          action: "resolve",
          alert_id: alertB.alert.id,
        },
      );
      const tenantBBody = (await tenantBResponse.json()) as {
        duplicate: boolean;
        result: {
          alert?: Alert;
        };
      };
      expect(tenantBResponse.status).toBe(200);
      expect(tenantBBody.duplicate).toBe(false);
      expect(tenantBBody.result.alert?.id).toBe(alertB.alert.id);
      expect(tenantBBody.result.alert?.tenantId).toBe(tenantBId);

      const tenantADuplicateResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: sharedCallbackId,
          tenant_id: tenantAId,
          action: "resolve",
          alert_id: alertA.alert.id,
        },
      );
      const tenantADuplicateBody = (await tenantADuplicateResponse.json()) as {
        duplicate: boolean;
        result: {
          alert?: Alert;
        };
      };
      expect(tenantADuplicateResponse.status).toBe(200);
      expect(tenantADuplicateBody.duplicate).toBe(true);
      expect(tenantADuplicateBody.result.alert?.tenantId).toBe(tenantAId);
    } finally {
      if (originalCallbackSecret === undefined) {
        delete Bun.env.INTEGRATION_CALLBACK_SECRET;
      } else {
        Bun.env.INTEGRATION_CALLBACK_SECRET = originalCallbackSecret;
      }
      await alertA.cleanup();
      await alertB.cleanup();
    }
  });

  test("integration callback claim 在 processing 超时后允许重试接管", async () => {
    if (typeof repository.claimIntegrationAlertCallback !== "function") {
      throw new Error("repository.claimIntegrationAlertCallback 不可用。");
    }

    const tenantId = "default";
    const callbackId = createNonce("cb-stale-reclaim");
    const staleBefore = new Date(Date.now() - 5 * 60 * 1000).toISOString();

    const first = await repository.claimIntegrationAlertCallback({
      callbackId,
      tenantId,
      action: "resolve",
      processedAt: staleBefore,
      staleAfterMs: 60_000,
    });
    expect(first.claimed).toBe(true);

    const second = await repository.claimIntegrationAlertCallback({
      callbackId,
      tenantId,
      action: "resolve",
      staleAfterMs: 60_000,
    });
    expect(second.claimed).toBe(true);
    expect(second.record.callbackId).toBe(callbackId);

    const third = await repository.claimIntegrationAlertCallback({
      callbackId,
      tenantId,
      action: "resolve",
      staleAfterMs: 60_000,
    });
    expect(third.claimed).toBe(false);
    expect(third.record.callbackId).toBe(callbackId);
  });

  test("POST /api/v1/integrations/callbacks/alerts warning 告警 ack 不冻结预算", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalCallbackSecret = Bun.env.INTEGRATION_CALLBACK_SECRET;
    const callbackSecret = `integration-secret-${createNonce("cb-warning-no-freeze")}`;

    const putBudgetResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "global",
        period: "monthly",
        tokenLimit: 1300,
        costLimit: 0,
        thresholds: {
          warning: 0.6,
          escalated: 0.8,
          critical: 0.95,
        },
      }),
    });
    expect(putBudgetResponse.status).toBe(200);
    const budget = (await putBudgetResponse.json()) as Budget;

    const warningAlert = await createTestAlert(tenantId, "open", {
      budgetId: budget.id,
      severity: "warning",
    });

    try {
      Bun.env.INTEGRATION_CALLBACK_SECRET = callbackSecret;

      const ackResponse = await postIntegrationAlertCallback(callbackSecret, {
        callback_id: createNonce("cb-warning-ack"),
        tenant_id: tenantId,
        action: "ack",
        alert_id: warningAlert.alert.id,
      });
      const ackBody = (await ackResponse.json()) as {
        duplicate: boolean;
        result: {
          alert?: Alert;
          budget?: Budget;
        };
      };

      expect(ackResponse.status).toBe(200);
      expect(ackBody.duplicate).toBe(false);
      expect(ackBody.result.alert?.status).toBe("acknowledged");
      expect(ackBody.result.budget).toBeUndefined();

      const listResponse = await app.request("/api/v1/budgets", {
        headers: authHeaders,
      });
      const listBody = (await listResponse.json()) as { items: Budget[] };
      const targetBudget = listBody.items.find((item) => item.id === budget.id);
      expect(targetBudget?.governanceState).toBe("active");
      expect(targetBudget?.frozenByAlertId).toBeUndefined();
    } finally {
      if (originalCallbackSecret === undefined) {
        delete Bun.env.INTEGRATION_CALLBACK_SECRET;
      } else {
        Bun.env.INTEGRATION_CALLBACK_SECRET = originalCallbackSecret;
      }
      await warningAlert.cleanup();
    }
  });

  test("POST /api/v1/integrations/callbacks/alerts 支持全 action 与 callback_id 幂等", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const authContext = await getDefaultAuthContext();
    const actorUserId =
      authContext.userId ??
      resolveUserIdFromAccessToken(authContext.accessToken);
    if (!actorUserId) {
      throw new Error("无法解析 callback 主审批人 userId。");
    }

    const reviewer = await registerAndLoginUser(
      createNonce("callback-reviewer"),
    );
    const reviewerUserId =
      reviewer.userId ?? resolveUserIdFromAccessToken(reviewer.accessToken);
    if (!reviewerUserId) {
      throw new Error("无法解析 callback 次审批人 userId。");
    }
    const secondReviewer = await registerAndLoginUser(
      createNonce("callback-reviewer-2"),
    );
    const secondReviewerUserId =
      secondReviewer.userId ??
      resolveUserIdFromAccessToken(secondReviewer.accessToken);
    if (!secondReviewerUserId) {
      throw new Error("无法解析 callback 第三审批人 userId。");
    }
    const originalCallbackSecret = Bun.env.INTEGRATION_CALLBACK_SECRET;
    const callbackSecret = `integration-secret-${createNonce("cb-secret")}`;

    const putBudgetResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "global",
        period: "monthly",
        tokenLimit: 1400,
        costLimit: 0,
        thresholds: {
          warning: 0.6,
          escalated: 0.8,
          critical: 0.95,
        },
      }),
    });
    const budget = (await putBudgetResponse.json()) as Budget;
    expect(putBudgetResponse.status).toBe(200);

    const alertOne = await createTestAlert(tenantId, "open", {
      budgetId: budget.id,
      severity: "critical",
    });
    let alertTwoCleanup: (() => Promise<void>) | null = null;

    try {
      Bun.env.INTEGRATION_CALLBACK_SECRET = callbackSecret;

      const ackCallbackId = createNonce("cb-ack");
      const ackResponse = await postIntegrationAlertCallback(callbackSecret, {
        callback_id: ackCallbackId,
        tenant_id: tenantId,
        action: "ack",
        alert_id: alertOne.alert.id,
      });
      const ackBody = (await ackResponse.json()) as {
        duplicate: boolean;
        result: {
          alert?: Alert;
          budget?: Budget;
        };
      };
      expect(ackResponse.status).toBe(200);
      expect(ackBody.duplicate).toBe(false);
      expect(ackBody.result.alert?.status).toBe("acknowledged");
      expect(ackBody.result.budget?.governanceState).toBe("frozen");

      const ackDuplicateResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: ackCallbackId,
          tenant_id: tenantId,
          action: "ack",
          alert_id: alertOne.alert.id,
        },
      );
      const ackDuplicateBody = (await ackDuplicateResponse.json()) as {
        duplicate: boolean;
      };
      expect(ackDuplicateResponse.status).toBe(200);
      expect(ackDuplicateBody.duplicate).toBe(true);

      const resolveResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-resolve"),
          tenant_id: tenantId,
          action: "resolve",
          alert_id: alertOne.alert.id,
        },
      );
      const resolveBody = (await resolveResponse.json()) as {
        duplicate: boolean;
        result: {
          alert?: Alert;
        };
      };
      expect(resolveResponse.status).toBe(200);
      expect(resolveBody.duplicate).toBe(false);
      expect(resolveBody.result.alert?.status).toBe("resolved");

      const lateAckResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-ack-after-resolve"),
          tenant_id: tenantId,
          action: "ack",
          alert_id: alertOne.alert.id,
        },
      );
      const lateAckBody = (await lateAckResponse.json()) as {
        duplicate: boolean;
        result: {
          alert?: Alert;
          budget?: Budget;
        };
      };
      expect(lateAckResponse.status).toBe(200);
      expect(lateAckBody.duplicate).toBe(false);
      expect(lateAckBody.result.alert?.status).toBe("resolved");
      expect(lateAckBody.result.budget).toBeUndefined();

      const requestReleaseResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-request-release-1"),
          tenant_id: tenantId,
          action: "request_release",
          budget_id: budget.id,
          actor_user_id: actorUserId,
          actor_email: "callback-owner@example.com",
          reason: "告警确认后申请释放。",
        },
      );
      const requestReleaseBody = (await requestReleaseResponse.json()) as {
        result: {
          releaseRequest?: {
            id: string;
            status: string;
          };
        };
      };
      expect(requestReleaseResponse.status).toBe(200);
      expect(requestReleaseBody.result.releaseRequest?.status).toBe("pending");
      const requestIdOne = requestReleaseBody.result.releaseRequest?.id;
      expect(typeof requestIdOne).toBe("string");

      const requesterApproveResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-approve-release-requester"),
          tenant_id: tenantId,
          action: "approve_release",
          budget_id: budget.id,
          request_id: requestIdOne,
          actor_user_id: actorUserId,
          actor_email: "callback-owner@example.com",
        },
      );
      const requesterApproveBody = (await requesterApproveResponse.json()) as {
        message?: string;
      };
      expect(requesterApproveResponse.status).toBe(400);
      expect(requesterApproveBody.message).toContain("申请人");

      const approveOneResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-approve-release-1"),
          tenant_id: tenantId,
          action: "approve_release",
          budget_id: budget.id,
          request_id: requestIdOne,
          actor_user_id: reviewerUserId,
          actor_email: reviewer.email,
        },
      );
      const approveOneBody = (await approveOneResponse.json()) as {
        result: {
          releaseRequest?: {
            status: string;
            approvals: Array<{ userId: string }>;
          };
        };
      };
      expect(approveOneResponse.status).toBe(200);
      expect(approveOneBody.result.releaseRequest?.status).toBe("pending");
      expect(approveOneBody.result.releaseRequest?.approvals.length).toBe(1);

      const approveTwoResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-approve-release-2"),
          tenant_id: tenantId,
          action: "approve_release",
          budget_id: budget.id,
          request_id: requestIdOne,
          actor_user_id: secondReviewerUserId,
          actor_email: secondReviewer.email,
        },
      );
      const approveTwoBody = (await approveTwoResponse.json()) as {
        result: {
          releaseRequest?: {
            status: string;
            approvals: Array<{ userId: string }>;
          };
        };
      };
      expect(approveTwoResponse.status).toBe(200);
      expect(approveTwoBody.result.releaseRequest?.status).toBe("executed");
      expect(approveTwoBody.result.releaseRequest?.approvals.length).toBe(2);
      const postApproveBudgetListResponse = await app.request(
        "/api/v1/budgets",
        {
          headers: authHeaders,
        },
      );
      const postApproveBudgetList =
        (await postApproveBudgetListResponse.json()) as {
          items: Budget[];
        };
      const postApproveBudget = postApproveBudgetList.items.find(
        (item) => item.id === budget.id,
      );
      expect(postApproveBudget?.governanceState).toBe("active");
      expect(postApproveBudget?.freezeReason).toBeUndefined();
      expect(postApproveBudget?.frozenAt).toBeUndefined();
      expect(postApproveBudget?.frozenByAlertId).toBeUndefined();

      const alertTwo = await createTestAlert(tenantId, "open", {
        budgetId: budget.id,
        severity: "critical",
      });
      alertTwoCleanup = alertTwo.cleanup;

      const concurrentAckCallbackId = createNonce("cb-ack-2");
      const [ackTwoResponseA, ackTwoResponseB] = await Promise.all([
        postIntegrationAlertCallback(callbackSecret, {
          callback_id: concurrentAckCallbackId,
          tenant_id: tenantId,
          action: "ack",
          alert_id: alertTwo.alert.id,
        }),
        postIntegrationAlertCallback(callbackSecret, {
          callback_id: concurrentAckCallbackId,
          tenant_id: tenantId,
          action: "ack",
          alert_id: alertTwo.alert.id,
        }),
      ]);
      const ackTwoBodyA = (await ackTwoResponseA.json()) as {
        duplicate: boolean;
        result: {
          alert?: Alert;
          budget?: Budget;
        };
      };
      const ackTwoBodyB = (await ackTwoResponseB.json()) as {
        duplicate: boolean;
        result: {
          alert?: Alert;
          budget?: Budget;
        };
      };
      expect(ackTwoResponseA.status).toBe(200);
      expect(ackTwoResponseB.status).toBe(200);
      expect(ackTwoBodyA.duplicate).not.toBe(ackTwoBodyB.duplicate);
      const appliedAckBody = ackTwoBodyA.duplicate ? ackTwoBodyB : ackTwoBodyA;
      expect(appliedAckBody.result.alert?.status).toBe("acknowledged");
      expect(appliedAckBody.result.budget?.governanceState).toBe("frozen");

      const requestReleaseTwoResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-request-release-2"),
          tenant_id: tenantId,
          action: "request_release",
          budget_id: budget.id,
          actor_user_id: actorUserId,
          actor_email: "callback-owner@example.com",
        },
      );
      const requestReleaseTwoBody =
        (await requestReleaseTwoResponse.json()) as {
          result: {
            releaseRequest?: {
              id: string;
              status: string;
            };
          };
        };
      expect(requestReleaseTwoResponse.status).toBe(200);
      expect(requestReleaseTwoBody.result.releaseRequest?.status).toBe(
        "pending",
      );
      const requestIdTwo = requestReleaseTwoBody.result.releaseRequest?.id;
      expect(typeof requestIdTwo).toBe("string");

      const rejectResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-reject-release-1"),
          tenant_id: tenantId,
          action: "reject_release",
          budget_id: budget.id,
          request_id: requestIdTwo,
          actor_user_id: reviewerUserId,
          actor_email: reviewer.email,
          reason: "二审驳回，待人工复核。",
        },
      );
      const rejectBody = (await rejectResponse.json()) as {
        result: {
          releaseRequest?: {
            status: string;
          };
        };
      };
      expect(rejectResponse.status).toBe(200);
      expect(rejectBody.result.releaseRequest?.status).toBe("rejected");
    } finally {
      if (originalCallbackSecret === undefined) {
        delete Bun.env.INTEGRATION_CALLBACK_SECRET;
      } else {
        Bun.env.INTEGRATION_CALLBACK_SECRET = originalCallbackSecret;
      }
      await alertOne.cleanup();
      if (alertTwoCleanup) {
        await alertTwoCleanup();
      }
    }
  });

  test("GET /api/v1/alerts 支持查询参数并返回结构化结果", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request(
      "/api/v1/alerts?status=open&severity=warning&sourceId=source-default-budget&from=2026-01-01T00:00:00.000Z&to=2026-12-31T23:59:59.999Z&limit=20",
      {
        headers: authHeaders,
      },
    );
    const body = (await response.json()) as {
      items: Alert[];
      total: number;
      filters: AlertListInput;
      nextCursor: string | null;
    };

    expect(response.status).toBe(200);
    expect(Array.isArray(body.items)).toBe(true);
    expect(typeof body.total).toBe("number");
    expect(body.total).toBeGreaterThanOrEqual(body.items.length);
    expect(body.filters.status).toBe("open");
    expect(body.filters.severity).toBe("warning");
    expect(body.filters.sourceId).toBe("source-default-budget");
    expect(body.filters.from).toBe("2026-01-01T00:00:00.000Z");
    expect(body.filters.to).toBe("2026-12-31T23:59:59.999Z");
    expect(body.filters.limit).toBe(20);
    expect(
      body.nextCursor === null || typeof body.nextCursor === "string",
    ).toBe(true);
  });

  test("GET /api/v1/alerts 参数非法时返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/alerts?limit=0", {
      headers: authHeaders,
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(400);
    expect(body.message).toContain("limit");
  });

  test("GET /api/v1/alerts cursor 非法时返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/alerts?cursor=invalid-cursor", {
      headers: authHeaders,
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(400);
    expect(body.message).toContain("cursor");
  });

  test("GET /api/v1/alerts 支持 cursor 分页并返回 nextCursor", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const nonce = createNonce("alerts-pagination");
    const sourceId = `source-pagination-${nonce}`;

    const alertOne = await createTestAlert(tenantId, "open", {
      sourceId,
      severity: "warning",
      createdAt: "2026-03-01T00:00:00.000Z",
    });
    const alertTwo = await createTestAlert(tenantId, "open", {
      sourceId,
      severity: "warning",
      createdAt: "2026-03-02T00:00:00.000Z",
    });
    const alertThree = await createTestAlert(tenantId, "open", {
      sourceId,
      severity: "warning",
      createdAt: "2026-03-03T00:00:00.000Z",
    });

    try {
      const firstResponse = await app.request(
        `/api/v1/alerts?status=open&severity=warning&sourceId=${encodeURIComponent(
          sourceId,
        )}&limit=2`,
        {
          headers: authHeaders,
        },
      );
      const firstBody = (await firstResponse.json()) as {
        items: Alert[];
        total: number;
        nextCursor: string | null;
      };
      expect(firstResponse.status).toBe(200);
      expect(firstBody.total).toBe(3);
      expect(firstBody.items).toHaveLength(2);
      expect(typeof firstBody.nextCursor).toBe("string");

      const secondResponse = await app.request(
        `/api/v1/alerts?status=open&severity=warning&sourceId=${encodeURIComponent(
          sourceId,
        )}&limit=2&cursor=${encodeURIComponent(firstBody.nextCursor as string)}`,
        {
          headers: authHeaders,
        },
      );
      const secondBody = (await secondResponse.json()) as {
        items: Alert[];
        total: number;
        nextCursor: string | null;
      };
      expect(secondResponse.status).toBe(200);
      expect(secondBody.total).toBe(3);
      expect(secondBody.items).toHaveLength(1);
      expect(secondBody.nextCursor).toBeNull();

      const firstIds = new Set(firstBody.items.map((item) => item.id));
      const duplicated = secondBody.items.some((item) => firstIds.has(item.id));
      expect(duplicated).toBe(false);
    } finally {
      await alertOne.cleanup();
      await alertTwo.cleanup();
      await alertThree.cleanup();
    }
  });

  test("PATCH /api/v1/alerts/:id/status 更新成功并返回最新告警", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const { alert, cleanup } = await createTestAlert(tenantId, "open");

    try {
      const response = await app.request(`/api/v1/alerts/${alert.id}/status`, {
        method: "PATCH",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          status: "acknowledged",
        }),
      });
      const body = (await response.json()) as Alert;

      expect(response.status).toBe(200);
      expect(body.id).toBe(alert.id);
      expect(body.tenantId).toBe(tenantId);
      expect(body.status).toBe("acknowledged");

      const audits = await queryAuditByAction(
        "control_plane.alert_status_updated",
        alert.id,
      );
      const targetAudit = audits.items.find((item) => {
        const metadataAlertId = item.metadata.alertId;
        const metadataTenantId = item.metadata.tenantId;
        const metadataFromStatus = item.metadata.fromStatus;
        const metadataToStatus = item.metadata.toStatus;
        return (
          item.action === "control_plane.alert_status_updated" &&
          metadataAlertId === alert.id &&
          metadataTenantId === tenantId &&
          metadataFromStatus === "open" &&
          metadataToStatus === "acknowledged"
        );
      });
      expect(targetAudit).toBeDefined();
    } finally {
      await cleanup();
    }
  });

  test("PATCH /api/v1/alerts/:id/status warning 告警 ack 不会冻结预算", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const nonce = createNonce("warning-alert-no-freeze");

    const createSourceResponse = await app.request("/api/v1/sources", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        name: `warning-ack-no-freeze-${nonce}`,
        type: "ssh",
        location: `10.47.${Math.floor(Math.random() * 200) + 10}.${Math.floor(Math.random() * 200) + 10}`,
      }),
    });
    expect(createSourceResponse.status).toBe(201);
    const source = (await createSourceResponse.json()) as Source;

    const putBudgetResponse = await app.request("/api/v1/budgets", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        scope: "source",
        sourceId: source.id,
        period: "monthly",
        tokenLimit: 1600,
        costLimit: 0,
        thresholds: {
          warning: 0.6,
          escalated: 0.8,
          critical: 0.95,
        },
      }),
    });
    expect(putBudgetResponse.status).toBe(200);
    const budget = (await putBudgetResponse.json()) as Budget;

    const { alert, cleanup } = await createTestAlert(tenantId, "open", {
      budgetId: budget.id,
      sourceId: source.id,
      severity: "warning",
    });

    try {
      const response = await app.request(`/api/v1/alerts/${alert.id}/status`, {
        method: "PATCH",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          status: "acknowledged",
        }),
      });
      const body = (await response.json()) as Alert;

      expect(response.status).toBe(200);
      expect(body.status).toBe("acknowledged");

      const listResponse = await app.request("/api/v1/budgets", {
        headers: authHeaders,
      });
      const listBody = (await listResponse.json()) as { items: Budget[] };
      const targetBudget = listBody.items.find((item) => item.id === budget.id);
      expect(targetBudget?.governanceState).toBe("active");
      expect(targetBudget?.frozenByAlertId).toBeUndefined();
    } finally {
      await cleanup();
      await app.request(`/api/v1/sources/${source.id}`, {
        method: "DELETE",
        headers: authHeaders,
      });
    }
  });

  test("PATCH /api/v1/alerts/:id/status 乱序状态更新不会回退", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const { alert, cleanup } = await createTestAlert(tenantId, "resolved");

    try {
      const response = await app.request(`/api/v1/alerts/${alert.id}/status`, {
        method: "PATCH",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          status: "acknowledged",
        }),
      });
      const body = (await response.json()) as Alert;

      expect(response.status).toBe(200);
      expect(body.id).toBe(alert.id);
      expect(body.status).toBe("resolved");
    } finally {
      await cleanup();
    }
  });

  test("PATCH /api/v1/alerts/:id/status 非法状态返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/alerts/non-existent/status", {
      method: "PATCH",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        status: "open",
      }),
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(400);
    expect(body.message).toContain("acknowledged/resolved");
  });

  test("PATCH /api/v1/alerts/:id/status 告警不存在时返回 404", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/alerts/non-existent/status", {
      method: "PATCH",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        status: "acknowledged",
      }),
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(404);
    expect(body.message).toContain("未找到告警");
  });

  test("alerts/orchestration GET /api/v1/alerts/orchestration/rules 未认证返回 401", async () => {
    const response = await app.request("/api/v1/alerts/orchestration/rules");
    const body = (await response.json()) as { message: string };

    expect(response.status).toBe(401);
    expect(body.message).toContain("认证");
  });

  test("alerts/orchestration GET /api/v1/alerts/orchestration/rules 参数非法返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request(
      "/api/v1/alerts/orchestration/rules?enabled=invalid",
      {
        headers: authHeaders,
      },
    );
    const body = (await response.json()) as { message: string };

    expect(response.status).toBe(400);
    expect(body.message).toContain("enabled");
  });

  test("alerts/orchestration PUT /api/v1/alerts/orchestration/rules/:id channels 非法返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("alerts-orchestration-invalid-channel");
    const ruleId = `rule-invalid-channel-${nonce}`;

    const response = await app.request(
      `/api/v1/alerts/orchestration/rules/${encodeURIComponent(ruleId)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          name: `编排规则-${nonce}`,
          enabled: true,
          eventType: "alert",
          dedupeWindowSeconds: 60,
          suppressionWindowSeconds: 120,
          mergeWindowSeconds: 180,
          channels: ["slack"],
        }),
      },
    );
    const body = (await response.json()) as { message: string };

    expect(response.status).toBe(400);
    expect(body.message).toContain("channels");
  });

  test("alerts/orchestration PUT 后 GET list 可见", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const nonce = createNonce("alerts-orchestration-list-visible");
    const ruleId = `rule-${nonce}`;
    const sourceId = `source-${nonce}`;

    const upsertResponse = await app.request(
      `/api/v1/alerts/orchestration/rules/${encodeURIComponent(ruleId)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          name: `编排规则-${nonce}`,
          enabled: true,
          eventType: "alert",
          severity: "critical",
          sourceId,
          dedupeWindowSeconds: 60,
          suppressionWindowSeconds: 120,
          mergeWindowSeconds: 180,
          slaMinutes: 30,
          channels: ["wecom", "webhook"],
        }),
      },
    );
    const upsertBody = (await upsertResponse.json()) as AlertOrchestrationRule;
    expect(upsertResponse.status).toBe(200);
    expect(upsertBody.id).toBe(ruleId);
    expect(upsertBody.tenantId).toBe(tenantId);

    const listResponse = await app.request(
      `/api/v1/alerts/orchestration/rules?eventType=alert&enabled=true&severity=critical&sourceId=${encodeURIComponent(
        sourceId,
      )}`,
      {
        headers: authHeaders,
      },
    );
    const listBody = (await listResponse.json()) as {
      items: AlertOrchestrationRule[];
      total: number;
      filters: AlertOrchestrationRuleListInput;
    };

    expect(listResponse.status).toBe(200);
    expect(listBody.filters.eventType).toBe("alert");
    expect(listBody.filters.enabled).toBe(true);
    expect(listBody.filters.severity).toBe("critical");
    expect(listBody.filters.sourceId).toBe(sourceId);
    const target = listBody.items.find((item) => item.id === ruleId);
    expect(target).toBeDefined();
    expect(target?.channels).toEqual(["wecom", "webhook"]);
  });

  test("alerts/orchestration 租户隔离：tenant A upsert 的规则 tenant B 不可见", async () => {
    const nonce = createNonce("alerts-orchestration-cross-tenant");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Alerts Orchestration Tenant A ${nonce}`,
        slug: `alerts-orch-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建响应缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Alerts Orchestration Tenant B ${nonce}`,
        slug: `alerts-orch-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建响应缺少 tenantId。");
    }

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      auth.accessToken,
      auth.userId,
    );
    const tenantBHeaders = await issueTenantScopedAuthHeaders(
      tenantBId,
      auth.accessToken,
      auth.userId,
    );

    const ruleId = `cross-tenant-rule-${nonce}`;
    const upsertResponse = await app.request(
      `/api/v1/alerts/orchestration/rules/${encodeURIComponent(ruleId)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          name: `跨租户隔离规则-${nonce}`,
          enabled: true,
          eventType: "weekly",
          dedupeWindowSeconds: 3600,
          suppressionWindowSeconds: 7200,
          mergeWindowSeconds: 1800,
          channels: ["email"],
        }),
      },
    );
    expect(upsertResponse.status).toBe(200);
    const upsertBody = (await upsertResponse.json()) as AlertOrchestrationRule;
    expect(upsertBody.tenantId).toBe(tenantAId);

    const listAResponse = await app.request(
      "/api/v1/alerts/orchestration/rules",
      {
        headers: tenantAHeaders,
      },
    );
    expect(listAResponse.status).toBe(200);
    const listABody = (await listAResponse.json()) as {
      items: AlertOrchestrationRule[];
    };
    expect(listABody.items.some((item) => item.id === ruleId)).toBe(true);

    const listBResponse = await app.request(
      "/api/v1/alerts/orchestration/rules",
      {
        headers: tenantBHeaders,
      },
    );
    expect(listBResponse.status).toBe(200);
    const listBBody = (await listBResponse.json()) as {
      items: AlertOrchestrationRule[];
    };
    expect(listBBody.items.some((item) => item.id === ruleId)).toBe(false);
  });

  test("alerts/orchestration POST /api/v1/alerts/orchestration/simulate 未认证返回 401", async () => {
    const response = await app.request(
      "/api/v1/alerts/orchestration/simulate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          eventType: "alert",
        }),
      },
    );
    const body = (await response.json()) as { message: string };

    expect(response.status).toBe(401);
    expect(body.message).toContain("认证");
  });

  test("alerts/orchestration POST /api/v1/alerts/orchestration/simulate 参数非法返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request(
      "/api/v1/alerts/orchestration/simulate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          eventType: "unknown",
        }),
      },
    );
    const body = (await response.json()) as { message: string };

    expect(response.status).toBe(400);
    expect(body.message).toContain("eventType");
  });

  test("alerts/orchestration simulate 两条规则冲突时返回冲突信息且写入 execution logs", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("alerts-orchestration-simulate-conflict");
    const sourceId = `source-${nonce}`;
    const ruleAId = `rule-a-${nonce}`;
    const ruleBId = `rule-b-${nonce}`;

    const upsertRuleAResponse = await app.request(
      `/api/v1/alerts/orchestration/rules/${encodeURIComponent(ruleAId)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          name: `冲突规则-A-${nonce}`,
          enabled: true,
          eventType: "alert",
          severity: "critical",
          sourceId,
          dedupeWindowSeconds: 30,
          suppressionWindowSeconds: 60,
          mergeWindowSeconds: 120,
          channels: ["wecom", "email"],
        }),
      },
    );
    expect(upsertRuleAResponse.status).toBe(200);

    const upsertRuleBResponse = await app.request(
      `/api/v1/alerts/orchestration/rules/${encodeURIComponent(ruleBId)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          name: `冲突规则-B-${nonce}`,
          enabled: true,
          eventType: "alert",
          dedupeWindowSeconds: 45,
          suppressionWindowSeconds: 90,
          mergeWindowSeconds: 180,
          channels: ["email", "webhook"],
        }),
      },
    );
    expect(upsertRuleBResponse.status).toBe(200);

    const simulateResponse = await app.request(
      "/api/v1/alerts/orchestration/simulate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          eventType: "alert",
          severity: "critical",
          sourceId,
        }),
      },
    );
    const simulateBody = (await simulateResponse.json()) as {
      matchedRules: AlertOrchestrationRule[];
      conflictRuleIds: string[];
      executions: Array<{
        id: string;
        ruleId: string;
        dispatchMode: string;
        simulated: boolean;
        conflictRuleIds: string[];
      }>;
    };
    expect(simulateResponse.status).toBe(200);
    expect(simulateBody.matchedRules.some((item) => item.id === ruleAId)).toBe(
      true,
    );
    expect(simulateBody.matchedRules.some((item) => item.id === ruleBId)).toBe(
      true,
    );
    expect(new Set(simulateBody.conflictRuleIds)).toEqual(
      new Set([ruleAId, ruleBId]),
    );
    expect(simulateBody.executions).toHaveLength(2);
    expect(simulateBody.executions.every((item) => item.simulated)).toBe(true);
    expect(
      simulateBody.executions.every((item) => item.dispatchMode === "rule"),
    ).toBe(true);
    const executionForRuleA = simulateBody.executions.find(
      (item) => item.ruleId === ruleAId,
    );
    const executionForRuleB = simulateBody.executions.find(
      (item) => item.ruleId === ruleBId,
    );
    expect(executionForRuleA?.conflictRuleIds).toContain(ruleBId);
    expect(executionForRuleB?.conflictRuleIds).toContain(ruleAId);

    const executionListResponse = await app.request(
      `/api/v1/alerts/orchestration/executions?eventType=alert&sourceId=${encodeURIComponent(
        sourceId,
      )}&dispatchMode=rule&hasConflict=true&simulated=true&limit=20`,
      {
        headers: authHeaders,
      },
    );
    const executionListBody = (await executionListResponse.json()) as {
      items: Array<{
        id: string;
        ruleId: string;
        dispatchMode: string;
        sourceId?: string;
        simulated: boolean;
        conflictRuleIds: string[];
      }>;
      total: number;
      filters: {
        eventType?: string;
        dispatchMode?: string;
        hasConflict?: boolean;
        sourceId?: string;
        simulated?: boolean;
        limit?: number;
      };
    };
    expect(executionListResponse.status).toBe(200);
    expect(executionListBody.filters.eventType).toBe("alert");
    expect(executionListBody.filters.dispatchMode).toBe("rule");
    expect(executionListBody.filters.hasConflict).toBe(true);
    expect(executionListBody.filters.sourceId).toBe(sourceId);
    expect(executionListBody.filters.simulated).toBe(true);
    expect(executionListBody.filters.limit).toBe(20);
    const matchedExecutionLogs = executionListBody.items.filter((item) =>
      [ruleAId, ruleBId].includes(item.ruleId),
    );
    expect(matchedExecutionLogs.length).toBe(2);
    expect(matchedExecutionLogs.every((item) => item.simulated)).toBe(true);
    expect(
      matchedExecutionLogs.every((item) => item.dispatchMode === "rule"),
    ).toBe(true);
    expect(
      matchedExecutionLogs.every((item) => item.conflictRuleIds.length > 0),
    ).toBe(true);
    expect(
      matchedExecutionLogs.every((item) => item.sourceId === sourceId),
    ).toBe(true);
    expect(executionListBody.total).toBeGreaterThanOrEqual(
      matchedExecutionLogs.length,
    );
  });

  test("alerts/orchestration executions 列表支持过滤并保证租户隔离", async () => {
    const nonce = createNonce("alerts-orchestration-executions-tenant");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Alerts Orchestration Execution Tenant A ${nonce}`,
        slug: `alerts-orch-exec-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建响应缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Alerts Orchestration Execution Tenant B ${nonce}`,
        slug: `alerts-orch-exec-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建响应缺少 tenantId。");
    }

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      auth.accessToken,
      auth.userId,
    );
    const tenantBHeaders = await issueTenantScopedAuthHeaders(
      tenantBId,
      auth.accessToken,
      auth.userId,
    );

    const tenantARuleId = `tenant-a-rule-${nonce}`;
    const tenantBRuleId = `tenant-b-rule-${nonce}`;
    const tenantASourceId = `tenant-a-source-${nonce}`;
    const tenantBSourceId = `tenant-b-source-${nonce}`;

    const upsertTenantARuleResponse = await app.request(
      `/api/v1/alerts/orchestration/rules/${encodeURIComponent(tenantARuleId)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          name: `执行日志过滤规则-A-${nonce}`,
          enabled: true,
          eventType: "alert",
          sourceId: tenantASourceId,
          dedupeWindowSeconds: 10,
          suppressionWindowSeconds: 20,
          mergeWindowSeconds: 30,
          channels: ["wecom"],
        }),
      },
    );
    expect(upsertTenantARuleResponse.status).toBe(200);

    const upsertTenantBRuleResponse = await app.request(
      `/api/v1/alerts/orchestration/rules/${encodeURIComponent(tenantBRuleId)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantBHeaders,
        },
        body: JSON.stringify({
          name: `执行日志过滤规则-B-${nonce}`,
          enabled: true,
          eventType: "alert",
          sourceId: tenantBSourceId,
          dedupeWindowSeconds: 10,
          suppressionWindowSeconds: 20,
          mergeWindowSeconds: 30,
          channels: ["email"],
        }),
      },
    );
    expect(upsertTenantBRuleResponse.status).toBe(200);

    const simulateTenantAResponse = await app.request(
      "/api/v1/alerts/orchestration/simulate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          eventType: "alert",
          sourceId: tenantASourceId,
          ruleId: tenantARuleId,
        }),
      },
    );
    expect(simulateTenantAResponse.status).toBe(200);

    const simulateTenantBResponse = await app.request(
      "/api/v1/alerts/orchestration/simulate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantBHeaders,
        },
        body: JSON.stringify({
          eventType: "alert",
          sourceId: tenantBSourceId,
          ruleId: tenantBRuleId,
        }),
      },
    );
    expect(simulateTenantBResponse.status).toBe(200);

    const listTenantAResponse = await app.request(
      `/api/v1/alerts/orchestration/executions?ruleId=${encodeURIComponent(
        tenantARuleId,
      )}&eventType=alert&sourceId=${encodeURIComponent(
        tenantASourceId,
      )}&simulated=true&limit=10`,
      {
        headers: tenantAHeaders,
      },
    );
    const listTenantABody = (await listTenantAResponse.json()) as {
      items: Array<{
        tenantId: string;
        ruleId: string;
        sourceId?: string;
        simulated: boolean;
      }>;
      total: number;
      filters: {
        ruleId?: string;
        eventType?: string;
        sourceId?: string;
        simulated?: boolean;
        limit?: number;
      };
    };
    expect(listTenantAResponse.status).toBe(200);
    expect(listTenantABody.filters.ruleId).toBe(tenantARuleId);
    expect(listTenantABody.filters.eventType).toBe("alert");
    expect(listTenantABody.filters.sourceId).toBe(tenantASourceId);
    expect(listTenantABody.filters.simulated).toBe(true);
    expect(listTenantABody.filters.limit).toBe(10);
    expect(listTenantABody.total).toBeGreaterThanOrEqual(1);
    expect(listTenantABody.items.length).toBeGreaterThanOrEqual(1);
    expect(
      listTenantABody.items.every((item) => item.tenantId === tenantAId),
    ).toBe(true);
    expect(
      listTenantABody.items.every((item) => item.ruleId === tenantARuleId),
    ).toBe(true);
    expect(
      listTenantABody.items.every((item) => item.sourceId === tenantASourceId),
    ).toBe(true);
    expect(listTenantABody.items.every((item) => item.simulated)).toBe(true);
    expect(
      listTenantABody.items.some((item) => item.ruleId === tenantBRuleId),
    ).toBe(false);

    const listTenantBResponse = await app.request(
      `/api/v1/alerts/orchestration/executions?ruleId=${encodeURIComponent(
        tenantBRuleId,
      )}&eventType=alert&sourceId=${encodeURIComponent(
        tenantBSourceId,
      )}&simulated=true&limit=10`,
      {
        headers: tenantBHeaders,
      },
    );
    const listTenantBBody = (await listTenantBResponse.json()) as {
      items: Array<{
        tenantId: string;
        ruleId: string;
      }>;
      total: number;
    };
    expect(listTenantBResponse.status).toBe(200);
    expect(listTenantBBody.total).toBeGreaterThanOrEqual(1);
    expect(listTenantBBody.items.length).toBeGreaterThanOrEqual(1);
    expect(
      listTenantBBody.items.every((item) => item.tenantId === tenantBId),
    ).toBe(true);
    expect(
      listTenantBBody.items.every((item) => item.ruleId === tenantBRuleId),
    ).toBe(true);
    expect(
      listTenantBBody.items.some((item) => item.ruleId === tenantARuleId),
    ).toBe(false);
  });

  test("GET /api/v1/audits 返回结构包含 items/total/filters", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/audits", {
      headers: authHeaders,
    });
    const body = (await response.json()) as AuditListResponse;

    expect(response.status).toBe(200);
    expect(Array.isArray(body.items)).toBe(true);
    expect(typeof body.total).toBe("number");
    expect(body.total).toBeGreaterThanOrEqual(body.items.length);
    expect(typeof body.filters).toBe("object");
    expect(body.filters).not.toBeNull();
    expect(
      body.nextCursor === null || typeof body.nextCursor === "string",
    ).toBe(true);
  });

  test("GET /api/v1/audits 查询成功会写入 audit.query 审计", async () => {
    const nonce = createNonce("audit-query");
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request(
      `/api/v1/audits?keyword=${encodeURIComponent(nonce)}&limit=20`,
      {
        headers: authHeaders,
      },
    );

    expect(response.status).toBe(200);

    const auth = await getDefaultAuthContext();
    const audits = await queryAuditByAction(
      "audit.query",
      nonce,
      auth.accessToken,
      auth.userId,
    );
    const targetAudit = audits.items.find((item) => {
      const metadataRoute = item.metadata.route;
      const metadataKeyword = item.metadata.keyword;
      return (
        item.action === "audit.query" &&
        metadataRoute === "/api/v1/audits" &&
        metadataKeyword === nonce
      );
    });
    expect(targetAudit).toBeDefined();
  });

  test("GET /api/v1/audits 不应把 critical 审计级别降级", async () => {
    const nonce = createNonce("audit-critical-level");
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const repositoryWithAudit = repository as {
      appendAuditLog?: (input: {
        tenantId: string;
        eventId: string;
        action: string;
        level: string;
        detail: string;
        metadata: Record<string, unknown>;
      }) => Promise<unknown>;
    };
    if (typeof repositoryWithAudit.appendAuditLog !== "function") {
      throw new Error(
        "repository.appendAuditLog 不可用，无法验证审计级别映射。",
      );
    }

    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:audit-critical:${nonce}`,
      action: "test.audit.critical",
      level: "critical",
      detail: `critical audit level validation ${nonce}`,
      metadata: {
        route: "/api/v1/audits",
        nonce,
      },
    });

    const auth = await getDefaultAuthContext();
    const audits = await queryAuditByAction(
      "test.audit.critical",
      nonce,
      auth.accessToken,
      auth.userId,
    );
    const targetAudit = audits.items.find((item) => {
      const metadataNonce = item.metadata.nonce;
      return (
        item.action === "test.audit.critical" &&
        item.level === "critical" &&
        metadataNonce === nonce
      );
    });
    expect(targetAudit).toBeDefined();
  });

  test("GET /api/v1/audits 支持 level/from/to/limit 查询参数", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request(
      "/api/v1/audits?level=warning&from=2026-01-01T00:00:00.000Z&to=2026-12-31T23:59:59.999Z&limit=20",
      {
        headers: authHeaders,
      },
    );
    const body = (await response.json()) as {
      items: unknown[];
      total: number;
      filters: AuditListInput;
      nextCursor: string | null;
    };

    expect(response.status).toBe(200);
    expect(Array.isArray(body.items)).toBe(true);
    expect(typeof body.total).toBe("number");
    expect(body.filters.level).toBe("warning");
    expect(body.filters.from).toBe("2026-01-01T00:00:00.000Z");
    expect(body.filters.to).toBe("2026-12-31T23:59:59.999Z");
    expect(body.filters.limit).toBe(20);
    expect(
      body.nextCursor === null || typeof body.nextCursor === "string",
    ).toBe(true);
  });

  test("GET /api/v1/audits 参数非法（from 晚于 to）时返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request(
      "/api/v1/audits?from=2026-03-02T00:00:00.000Z&to=2026-03-01T00:00:00.000Z",
      {
        headers: authHeaders,
      },
    );
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(400);
    expect(body.message).toContain("from");
  });

  test("GET /api/v1/audits 参数非法（limit=0）时返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/audits?limit=0", {
      headers: authHeaders,
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(400);
    expect(body.message).toContain("limit");
  });

  test("GET /api/v1/audits cursor 非法时返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    const response = await app.request("/api/v1/audits?cursor=invalid-cursor", {
      headers: authHeaders,
    });
    const body = (await response.json()) as {
      message: string;
    };

    expect(response.status).toBe(400);
    expect(body.message).toContain("cursor");
  });

  test("GET /api/v1/audits 支持 cursor 分页并返回 nextCursor", async () => {
    const nonce = createNonce("audit-cursor-pagination");
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const repositoryWithAudit = repository as {
      appendAuditLog?: (input: {
        tenantId: string;
        eventId: string;
        action: string;
        level: string;
        detail: string;
        metadata: Record<string, unknown>;
      }) => Promise<unknown>;
    };
    if (typeof repositoryWithAudit.appendAuditLog !== "function") {
      throw new Error(
        "repository.appendAuditLog 不可用，无法验证 cursor 分页。",
      );
    }

    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:audit-cursor:${nonce}:1`,
      action: "test.audit.cursor",
      level: "info",
      detail: `cursor page one ${nonce}`,
      metadata: { nonce, order: 1 },
    });
    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:audit-cursor:${nonce}:2`,
      action: "test.audit.cursor",
      level: "info",
      detail: `cursor page two ${nonce}`,
      metadata: { nonce, order: 2 },
    });
    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:audit-cursor:${nonce}:3`,
      action: "test.audit.cursor",
      level: "info",
      detail: `cursor page three ${nonce}`,
      metadata: { nonce, order: 3 },
    });

    const firstResponse = await app.request(
      `/api/v1/audits?action=test.audit.cursor&keyword=${encodeURIComponent(
        nonce,
      )}&limit=2`,
      {
        headers: authHeaders,
      },
    );
    const firstBody = (await firstResponse.json()) as {
      items: Array<{ id: string }>;
      total: number;
      nextCursor: string | null;
    };
    expect(firstResponse.status).toBe(200);
    expect(firstBody.total).toBe(3);
    expect(firstBody.items).toHaveLength(2);
    expect(typeof firstBody.nextCursor).toBe("string");

    const secondResponse = await app.request(
      `/api/v1/audits?action=test.audit.cursor&keyword=${encodeURIComponent(
        nonce,
      )}&limit=2&cursor=${encodeURIComponent(firstBody.nextCursor as string)}`,
      {
        headers: authHeaders,
      },
    );
    const secondBody = (await secondResponse.json()) as {
      items: Array<{ id: string }>;
      total: number;
      nextCursor: string | null;
    };
    expect(secondResponse.status).toBe(200);
    expect(secondBody.total).toBe(3);
    expect(secondBody.items).toHaveLength(1);
    expect(secondBody.nextCursor).toBeNull();

    const firstIds = new Set(firstBody.items.map((item) => item.id));
    const duplicated = secondBody.items.some((item) => firstIds.has(item.id));
    expect(duplicated).toBe(false);
  });

  test("GET /api/v1/system/config/backup 返回租户配置快照并写入审计", async () => {
    const nonce = createNonce("system-config-backup");
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);

    const sourceResponse = await app.request(
      "/api/v1/sources",
      jsonRequest(
        "POST",
        {
          name: `备份源-${nonce}`,
          type: "local",
          location: `~/.codex/sessions/${nonce}`,
          sourceRegion: "cn-shanghai",
          accessMode: "sync",
          syncCron: "0 */6 * * *",
          syncRetentionDays: 14,
          enabled: true,
        },
        authHeaders,
      ),
    );
    expect(sourceResponse.status).toBe(201);

    const budgetResponse = await app.request(
      "/api/v1/budgets",
      jsonRequest(
        "PUT",
        {
          scope: "global",
          period: "monthly",
          tokenLimit: 120000,
          costLimit: 120,
          thresholds: {
            warning: 0.5,
            escalated: 0.8,
            critical: 1,
          },
        },
        authHeaders,
      ),
    );
    expect(budgetResponse.status).toBe(200);

    const pricingResponse = await app.request(
      "/api/v1/pricing/catalog",
      jsonRequest(
        "PUT",
        {
          note: `backup-note-${nonce}`,
          entries: [
            {
              model: `backup-model-${nonce}`,
              inputPer1k: 0.1,
              outputPer1k: 0.2,
              cacheReadPer1k: 0.01,
              cacheWritePer1k: 0.03,
              reasoningPer1k: 0.05,
              currency: "USD",
            },
          ],
        },
        authHeaders,
      ),
    );
    expect(pricingResponse.status).toBe(200);

    const backupResponse = await app.request("/api/v1/system/config/backup", {
      headers: authHeaders,
    });
    const payload = (await backupResponse.json()) as {
      schemaVersion: string;
      tenantId: string;
      exportedAt: string;
      sources: Array<{ name: string; location: string; sourceRegion?: string }>;
      budgets: Array<{ scope: string; tokenLimit: number; costLimit: number }>;
      pricingCatalog?: {
        note?: string;
        entries: Array<{ model: string }>;
      };
    };

    expect(backupResponse.status).toBe(200);
    expect(payload.schemaVersion).toBe("system-config-backup.v1");
    expect(payload.tenantId).toBe(tenantId);
    expect(typeof payload.exportedAt).toBe("string");
    expect(
      payload.sources.some(
        (item) =>
          item.location.includes(nonce) && item.sourceRegion === "cn-shanghai",
      ),
    ).toBe(true);
    expect(
      payload.budgets.some(
        (item) =>
          item.scope === "global" &&
          item.tokenLimit === 120000 &&
          item.costLimit === 120,
      ),
    ).toBe(true);
    expect(payload.pricingCatalog?.note).toBe(`backup-note-${nonce}`);
    expect(
      payload.pricingCatalog?.entries.some(
        (item) => item.model === `backup-model-${nonce}`,
      ),
    ).toBe(true);

    const audits = await queryAuditByAction(
      "control_plane.system_config_backup_exported",
      nonce,
    );
    const targetAudit = audits.items.find(
      (item) =>
        item.action === "control_plane.system_config_backup_exported" &&
        item.metadata.tenantId === tenantId,
    );
    expect(targetAudit).toBeDefined();
  });

  test("POST /api/v1/system/config/restore 支持 dryRun + apply，并校验 tenant 边界", async () => {
    const nonce = createNonce("system-config-restore");
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);

    const backupPayload = {
      schemaVersion: "system-config-backup.v1",
      tenantId,
      exportedAt: new Date().toISOString(),
      exportedBy: {
        userId: "backup-user",
        email: "backup-user@example.com",
      },
      sources: [
        {
          name: `恢复源-${nonce}`,
          type: "local" as const,
          location: `~/.codex/sessions/${nonce}`,
          sourceRegion: "cn-shanghai",
          accessMode: "hybrid" as const,
          syncCron: "*/20 * * * *",
          syncRetentionDays: 21,
          enabled: true,
        },
        {
          name: `恢复源异区域-${nonce}`,
          type: "local" as const,
          location: `~/.codex/sessions/${nonce}`,
          sourceRegion: "ap-southeast-1",
          accessMode: "hybrid" as const,
          syncCron: "*/20 * * * *",
          syncRetentionDays: 21,
          enabled: true,
        },
      ],
      budgets: [
        {
          scope: "global" as const,
          period: "monthly" as const,
          tokenLimit: 4096,
          costLimit: 4.2,
          thresholds: {
            warning: 0.6,
            escalated: 0.85,
            critical: 1,
          },
        },
      ],
      pricingCatalog: {
        note: `restore-note-${nonce}`,
        entries: [
          {
            model: `restore-model-${nonce}`,
            inputPer1k: 0.4,
            outputPer1k: 0.7,
            cacheReadPer1k: 0.02,
            cacheWritePer1k: 0.04,
            reasoningPer1k: 0.08,
            currency: "USD",
          },
        ],
      },
    };

    const dryRunResponse = await app.request(
      "/api/v1/system/config/restore",
      jsonRequest(
        "POST",
        {
          backup: backupPayload,
          dryRun: true,
        },
        authHeaders,
      ),
    );
    const dryRunBody = (await dryRunResponse.json()) as {
      tenantId: string;
      dryRun: boolean;
      summary: {
        sources: { created: number };
        budgets: { upserted: number };
        pricingCatalog: { restored: boolean };
      };
      warnings: string[];
    };

    expect(dryRunResponse.status).toBe(200);
    expect(dryRunBody.tenantId).toBe(tenantId);
    expect(dryRunBody.dryRun).toBe(true);
    expect(dryRunBody.summary.sources.created).toBe(2);
    expect(dryRunBody.summary.budgets.upserted).toBe(1);
    expect(dryRunBody.summary.pricingCatalog.restored).toBe(true);
    expect(Array.isArray(dryRunBody.warnings)).toBe(true);

    const sourceListAfterDryRun = await app.request("/api/v1/sources", {
      headers: authHeaders,
    });
    const sourceListAfterDryRunBody =
      (await sourceListAfterDryRun.json()) as SourceListResponse;
    expect(
      sourceListAfterDryRunBody.items.some((item) =>
        item.location.includes(nonce),
      ),
    ).toBe(false);

    const applyResponse = await app.request(
      "/api/v1/system/config/restore",
      jsonRequest(
        "POST",
        {
          backup: backupPayload,
        },
        authHeaders,
      ),
    );
    const applyBody = (await applyResponse.json()) as {
      tenantId: string;
      dryRun: boolean;
      summary: {
        sources: { created: number };
        budgets: { upserted: number };
        pricingCatalog: { restored: boolean };
      };
    };
    expect(applyResponse.status).toBe(200);
    expect(applyBody.tenantId).toBe(tenantId);
    expect(applyBody.dryRun).toBe(false);
    expect(applyBody.summary.sources.created).toBe(2);
    expect(applyBody.summary.budgets.upserted).toBe(1);
    expect(applyBody.summary.pricingCatalog.restored).toBe(true);

    const sourceListAfterApply = await app.request("/api/v1/sources", {
      headers: authHeaders,
    });
    const sourceListAfterApplyBody =
      (await sourceListAfterApply.json()) as SourceListResponse & {
        items: Array<Source & { sourceRegion?: string }>;
      };
    expect(
      sourceListAfterApplyBody.items.filter((item) =>
        item.location.includes(nonce),
      ).length,
    ).toBe(2);
    expect(
      sourceListAfterApplyBody.items.some(
        (item) =>
          item.location.includes(nonce) && item.sourceRegion === "cn-shanghai",
      ),
    ).toBe(true);
    expect(
      sourceListAfterApplyBody.items.some(
        (item) =>
          item.location.includes(nonce) &&
          item.sourceRegion === "ap-southeast-1",
      ),
    ).toBe(true);

    const restoreAudits = await queryAuditByAction(
      "control_plane.system_config_restore_applied",
      nonce,
    );
    const restoreAudit = restoreAudits.items.find(
      (item) =>
        item.action === "control_plane.system_config_restore_applied" &&
        item.metadata.tenantId === tenantId,
    );
    expect(restoreAudit).toBeDefined();

    const crossTenantResponse = await app.request(
      "/api/v1/system/config/restore",
      jsonRequest(
        "POST",
        {
          backup: {
            ...backupPayload,
            tenantId: `${tenantId}-other`,
          },
        },
        authHeaders,
      ),
    );
    expect(crossTenantResponse.status).toBe(403);
  });

  test("GET /api/v1/audits/export 支持 CSV 导出并写入 audit.export", async () => {
    const nonce = createNonce("audit-export");
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const repositoryWithAudit = repository as {
      appendAuditLog?: (input: {
        tenantId: string;
        eventId: string;
        action: string;
        level: string;
        detail: string;
        metadata: Record<string, unknown>;
      }) => Promise<unknown>;
    };

    if (typeof repositoryWithAudit.appendAuditLog !== "function") {
      throw new Error("repository.appendAuditLog 不可用，无法验证审计导出。");
    }

    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:audit-export-seed:${nonce}`,
      action: "test.audit.exportable",
      level: "warning",
      detail: `audit export seed ${nonce}`,
      metadata: {
        nonce,
        route: "/api/v1/audits/export",
      },
    });

    const response = await app.request(
      `/api/v1/audits/export?format=csv&action=test.audit.exportable&keyword=${encodeURIComponent(
        nonce,
      )}&limit=20`,
      {
        headers: authHeaders,
      },
    );
    const body = await response.text();

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")?.includes("text/csv")).toBe(
      true,
    );
    expect(body).toContain("id,eventId,action,level,detail,createdAt,metadata");
    expect(body).toContain("test.audit.exportable");
    expect(body).toContain(nonce);

    const exportAudits = await queryAuditByAction("audit.export", nonce);
    const targetAudit = exportAudits.items.find(
      (item) =>
        item.action === "audit.export" &&
        item.metadata.route === "/api/v1/audits/export",
    );
    expect(targetAudit).toBeDefined();
  });

  test("GET /api/v1/audits/evidence-bundle 未配置签名密钥返回 500", async () => {
    const authHeaders = await resolveAuthHeaders();
    const originalSigningKey = Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY;
    delete Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY;

    try {
      const response = await app.request("/api/v1/audits/evidence-bundle", {
        headers: authHeaders,
      });
      const body = (await response.json()) as {
        message: string;
      };
      expect(response.status).toBe(500);
      expect(body.message).toContain("EVIDENCE_BUNDLE_SIGNING_KEY");
    } finally {
      if (originalSigningKey === undefined) {
        delete Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY;
      } else {
        Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY = originalSigningKey;
      }
    }
  });

  test("GET /api/v1/audits/evidence-bundle 返回可验证取证包并写入审计", async () => {
    const nonce = createNonce("audit-evidence-bundle");
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const signingKey = `evidence-signing-key-${nonce}`;
    const originalSigningKey = Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY;
    Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY = signingKey;

    const repositoryWithAudit = repository as {
      appendAuditLog?: (input: {
        tenantId: string;
        eventId: string;
        action: string;
        level: string;
        detail: string;
        metadata: Record<string, unknown>;
      }) => Promise<unknown>;
    };
    if (typeof repositoryWithAudit.appendAuditLog !== "function") {
      throw new Error("repository.appendAuditLog 不可用，无法验证取证包导出。");
    }

    try {
      await repositoryWithAudit.appendAuditLog({
        tenantId,
        eventId: `cp:audit-evidence-seed:${nonce}:1`,
        action: "test.audit.evidence_seed",
        level: "info",
        detail: `audit evidence seed 1 ${nonce}`,
        metadata: {
          nonce,
          route: "/api/v1/audits/evidence-bundle",
          sequence: 1,
        },
      });
      await repositoryWithAudit.appendAuditLog({
        tenantId,
        eventId: `cp:audit-evidence-seed:${nonce}:2`,
        action: "test.audit.evidence_seed",
        level: "info",
        detail: `audit evidence seed 2 ${nonce}`,
        metadata: {
          nonce,
          route: "/api/v1/audits/evidence-bundle",
          sequence: 2,
        },
      });
      await repositoryWithAudit.appendAuditLog({
        tenantId,
        eventId: `cp:audit-evidence-seed:${nonce}:3`,
        action: "test.audit.evidence_seed",
        level: "info",
        detail: `audit evidence seed 3 ${nonce}`,
        metadata: {
          nonce,
          route: "/api/v1/audits/evidence-bundle",
          sequence: 3,
        },
      });

      const response = await app.request(
        `/api/v1/audits/evidence-bundle?action=test.audit.evidence_seed&keyword=${encodeURIComponent(
          nonce,
        )}&limit=2`,
        {
          headers: authHeaders,
        },
      );
      const body = (await response.json()) as {
        manifest: {
          schemaVersion: string;
          tenantId: string;
          recordCount: number;
        };
        records: Array<{
          index: number;
          recordHash: string;
          chainHash: string;
        }>;
        rootHash: string;
        signature: string;
      };

      expect(response.status).toBe(200);
      expect(
        response.headers
          .get("content-disposition")
          ?.includes("audit-evidence-bundle-"),
      ).toBe(true);
      expect(body.manifest.schemaVersion).toBe("evidence-bundle.v1");
      expect(body.manifest.tenantId).toBe(tenantId);
      expect(body.manifest.recordCount).toBe(body.records.length);
      expect(body.records.length).toBe(3);
      expect(typeof body.rootHash).toBe("string");
      expect(typeof body.signature).toBe("string");

      const verifyResult = verifyEvidenceBundle(body, signingKey);
      expect(verifyResult.success).toBe(true);

      const tampered = structuredClone(body);
      tampered.records[0]!.chainHash = "deadbeef";
      const tamperedResult = verifyEvidenceBundle(tampered, signingKey);
      expect(tamperedResult.success).toBe(false);

      const exportAudits = await queryAuditByAction(
        "audit.evidence_bundle.export",
        nonce,
      );
      const targetAudit = exportAudits.items.find(
        (item) =>
          item.action === "audit.evidence_bundle.export" &&
          item.metadata.route === "/api/v1/audits/evidence-bundle" &&
          item.metadata.tenantId === tenantId,
      );
      expect(targetAudit).toBeDefined();
    } finally {
      if (originalSigningKey === undefined) {
        delete Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY;
      } else {
        Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY = originalSigningKey;
      }
    }
  });

  test("GET /api/v1/audits/evidence-bundle 审计写入失败时返回 500", async () => {
    const nonce = createNonce("audit-evidence-bundle-audit-fail");
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const signingKey = `evidence-signing-key-${nonce}`;
    const originalSigningKey = Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY;
    Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY = signingKey;

    const repositoryWithAudit = repository as {
      appendAuditLog?: (input: {
        tenantId: string;
        eventId: string;
        action: string;
        level: string;
        detail: string;
        metadata: Record<string, unknown>;
      }) => Promise<unknown>;
    };
    if (typeof repositoryWithAudit.appendAuditLog !== "function") {
      throw new Error(
        "repository.appendAuditLog 不可用，无法验证审计写入失败场景。",
      );
    }

    const rawAppendAuditLog = repositoryWithAudit.appendAuditLog;
    const originalAppendAuditLog = rawAppendAuditLog.bind(repositoryWithAudit);
    try {
      await originalAppendAuditLog({
        tenantId,
        eventId: `cp:audit-evidence-seed:${nonce}`,
        action: "test.audit.evidence_seed",
        level: "info",
        detail: `audit evidence seed ${nonce}`,
        metadata: {
          nonce,
          route: "/api/v1/audits/evidence-bundle",
        },
      });

      repositoryWithAudit.appendAuditLog = async (input) => {
        if (input.action === "audit.evidence_bundle.export") {
          throw new Error("forced audit write failure");
        }
        return originalAppendAuditLog(input);
      };

      const response = await app.request(
        `/api/v1/audits/evidence-bundle?action=test.audit.evidence_seed&keyword=${encodeURIComponent(
          nonce,
        )}&limit=20`,
        {
          headers: authHeaders,
        },
      );
      const body = (await response.json()) as {
        message: string;
      };
      expect(response.status).toBe(500);
      expect(body.message).toContain("审计写入失败");
    } finally {
      repositoryWithAudit.appendAuditLog = rawAppendAuditLog;
      if (originalSigningKey === undefined) {
        delete Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY;
      } else {
        Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY = originalSigningKey;
      }
    }
  });

  test("residency 路由：401/400/创建后可查询并按租户隔离", async () => {
    const unauthorizedResponse = await app.request("/api/v1/residency/policy");
    expect(unauthorizedResponse.status).toBe(401);

    const nonce = createNonce("residency-routes");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Residency Tenant A ${nonce}`,
        slug: `residency-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建失败，缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Residency Tenant B ${nonce}`,
        slug: `residency-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建失败，缺少 tenantId。");
    }

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      auth.accessToken,
      auth.userId,
    );
    const tenantBHeaders = await issueTenantScopedAuthHeaders(
      tenantBId,
      auth.accessToken,
      auth.userId,
    );

    const badPolicyResponse = await app.request("/api/v1/residency/policy", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        mode: "invalid",
      }),
    });
    expect(badPolicyResponse.status).toBe(400);

    const policyResponse = await app.request("/api/v1/residency/policy", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        mode: "active_active",
        primaryRegion: "cn-hangzhou",
        replicaRegions: ["cn-shanghai", "ap-southeast-1"],
        allowCrossRegionTransfer: true,
        requireTransferApproval: true,
      }),
    });
    expect(policyResponse.status).toBe(200);
    const policyBody = (await policyResponse.json()) as {
      tenantId: string;
      mode: string;
      replicaRegions: string[];
    };
    expect(policyBody.tenantId).toBe(tenantAId);
    expect(policyBody.mode).toBe("active_active");
    expect(policyBody.replicaRegions).toContain("cn-shanghai");

    const createJobResponse = await app.request(
      "/api/v1/residency/replication-jobs",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          sourceRegion: "cn-hangzhou",
          targetRegion: "cn-shanghai",
          reason: "验证租户隔离",
        }),
      },
    );
    expect(createJobResponse.status).toBe(201);
    const jobBody = (await createJobResponse.json()) as {
      id: string;
      tenantId: string;
      status: string;
    };
    expect(jobBody.tenantId).toBe(tenantAId);
    expect(jobBody.status).toBe("pending");

    const approveJobResponse = await app.request(
      `/api/v1/residency/replication-jobs/${encodeURIComponent(jobBody.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          reason: "审批通过用于跨区同步",
        }),
      },
    );
    expect(approveJobResponse.status).toBe(200);
    const approvedJobBody = (await approveJobResponse.json()) as {
      status: string;
      approvedByUserId?: string;
    };
    expect(approvedJobBody.status).toBe("running");
    expect(typeof approvedJobBody.approvedByUserId).toBe("string");

    const approveAgainResponse = await app.request(
      `/api/v1/residency/replication-jobs/${encodeURIComponent(jobBody.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          reason: "重复审批",
        }),
      },
    );
    expect(approveAgainResponse.status).toBe(409);

    const cancelJobResponse = await app.request(
      `/api/v1/residency/replication-jobs/${encodeURIComponent(jobBody.id)}/cancel`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          reason: "停止测试任务",
        }),
      },
    );
    expect(cancelJobResponse.status).toBe(200);
    const cancelledJobBody = (await cancelJobResponse.json()) as {
      status: string;
    };
    expect(cancelledJobBody.status).toBe("cancelled");

    const cancelAgainResponse = await app.request(
      `/api/v1/residency/replication-jobs/${encodeURIComponent(jobBody.id)}/cancel`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          reason: "重复取消",
        }),
      },
    );
    expect(cancelAgainResponse.status).toBe(409);

    const listAResponse = await app.request(
      "/api/v1/residency/replication-jobs",
      {
        headers: tenantAHeaders,
      },
    );
    expect(listAResponse.status).toBe(200);
    const listABody = (await listAResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(listABody.items.some((item) => item.id === jobBody.id)).toBe(true);

    const listBResponse = await app.request(
      "/api/v1/residency/replication-jobs",
      {
        headers: tenantBHeaders,
      },
    );
    expect(listBResponse.status).toBe(200);
    const listBBody = (await listBResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(listBBody.items.some((item) => item.id === jobBody.id)).toBe(false);
  });

  test("rule hub 路由：400/创建资产版本发布回滚审批审计与租户隔离", async () => {
    const nonce = createNonce("rule-hub-routes");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `RuleHub Tenant A ${nonce}`,
        slug: `rulehub-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建失败，缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `RuleHub Tenant B ${nonce}`,
        slug: `rulehub-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建失败，缺少 tenantId。");
    }

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      auth.accessToken,
      auth.userId,
    );
    const tenantBHeaders = await issueTenantScopedAuthHeaders(
      tenantBId,
      auth.accessToken,
      auth.userId,
    );

    const badCreateResponse = await app.request("/api/v1/rules/assets", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({}),
    });
    expect(badCreateResponse.status).toBe(400);

    const createAssetResponse = await app.request("/api/v1/rules/assets", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        name: `Prompt 审计规则 ${nonce}`,
        description: "用于验证规则资产闭环",
        scopeBinding: {
          organizations: [`org-${nonce}`],
          projects: [`project-${nonce}`],
          clients: [`client-${nonce}`],
        },
      }),
    });
    expect(createAssetResponse.status).toBe(201);
    const asset = (await createAssetResponse.json()) as {
      id: string;
      tenantId: string;
      status: string;
      scopeBinding: {
        organizations?: string[];
        projects?: string[];
        clients?: string[];
      };
    };
    expect(asset.tenantId).toBe(tenantAId);
    expect(asset.status).toBe("draft");
    expect(asset.scopeBinding).toEqual({
      organizations: [`org-${nonce}`],
      projects: [`project-${nonce}`],
      clients: [`client-${nonce}`],
    });

    const listAResponse = await app.request("/api/v1/rules/assets", {
      headers: tenantAHeaders,
    });
    expect(listAResponse.status).toBe(200);
    const listABody = (await listAResponse.json()) as {
      items: Array<{
        id: string;
        scopeBinding: {
          organizations?: string[];
          projects?: string[];
          clients?: string[];
        };
      }>;
    };
    expect(
      listABody.items.some(
        (item) =>
          item.id === asset.id &&
          item.scopeBinding.organizations?.includes(`org-${nonce}`) &&
          item.scopeBinding.projects?.includes(`project-${nonce}`) &&
          item.scopeBinding.clients?.includes(`client-${nonce}`),
      ),
    ).toBe(true);

    const badVersionLimitResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/versions?limit=0`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(badVersionLimitResponse.status).toBe(400);

    const createVersionResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/versions`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          content: "deny tool=github.delete_repo when risk=high",
          changelog: "init version",
        }),
      },
    );
    expect(createVersionResponse.status).toBe(201);
    const version = (await createVersionResponse.json()) as { version: number };
    expect(version.version).toBe(1);

    const createSecondVersionResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/versions`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          content: "allow tool=github.read_repo when risk=low",
          changelog: "second version",
        }),
      },
    );
    expect(createSecondVersionResponse.status).toBe(201);
    const secondVersion = (await createSecondVersionResponse.json()) as {
      version: number;
    };
    expect(secondVersion.version).toBe(2);

    const publishResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/publish`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          version: 2,
        }),
      },
    );
    expect(publishResponse.status).toBe(200);
    const publishedAsset = (await publishResponse.json()) as {
      publishedVersion?: number;
      status: string;
    };
    expect(publishedAsset.publishedVersion).toBe(2);
    expect(publishedAsset.status).toBe("published");

    const publishAudits = await queryAuditByActionWithHeaders(
      "control_plane.rule_asset_published",
      asset.id,
      tenantAHeaders,
    );
    const publishedAudit = publishAudits.items.find(
      (item) =>
        item.action === "control_plane.rule_asset_published" &&
        item.metadata.resourceId === asset.id &&
        item.metadata.version === 2 &&
        item.metadata.publishedVersion === 2 &&
        item.metadata.publishedByUserId === auth.userId,
    );
    expect(publishedAudit).toBeDefined();

    const publishMissingVersionResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/publish`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          version: 99,
        }),
      },
    );
    expect(publishMissingVersionResponse.status).toBe(409);

    const rollbackResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/rollback`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          version: 1,
          reason: "回滚到已验证版本",
        }),
      },
    );
    expect(rollbackResponse.status).toBe(200);
    const rolledBackAsset = (await rollbackResponse.json()) as {
      publishedVersion?: number;
      status: string;
    };
    expect(rolledBackAsset.publishedVersion).toBe(1);
    expect(rolledBackAsset.status).toBe("published");

    const rollbackAudits = await queryAuditByActionWithHeaders(
      "control_plane.rule_asset_rolled_back",
      asset.id,
      tenantAHeaders,
    );
    const rolledBackAudit = rollbackAudits.items.find(
      (item) =>
        item.action === "control_plane.rule_asset_rolled_back" &&
        item.metadata.resourceId === asset.id &&
        item.metadata.version === 1 &&
        item.metadata.publishedVersion === 1 &&
        item.metadata.rolledBackByUserId === auth.userId &&
        item.metadata.reason === "回滚到已验证版本",
    );
    expect(rolledBackAudit).toBeDefined();

    const createApprovalResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/approvals`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          version: 1,
          decision: "approved",
          reason: "通过验证",
        }),
      },
    );
    expect(createApprovalResponse.status).toBe(201);
    const approval = (await createApprovalResponse.json()) as {
      id: string;
      version: number;
      decision: string;
    };
    expect(approval.version).toBe(1);
    expect(approval.decision).toBe("approved");

    const approvalAudits = await queryAuditByActionWithHeaders(
      "control_plane.rule_approval_created",
      approval.id,
      tenantAHeaders,
    );
    const approvalAudit = approvalAudits.items.find(
      (item) =>
        item.action === "control_plane.rule_approval_created" &&
        item.metadata.resourceId === approval.id &&
        item.metadata.assetId === asset.id &&
        item.metadata.version === 1 &&
        item.metadata.decision === "approved" &&
        item.metadata.approverUserId === auth.userId,
    );
    expect(approvalAudit).toBeDefined();

    const listApprovalsResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/approvals?version=1`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(listApprovalsResponse.status).toBe(200);
    const listApprovalsBody = (await listApprovalsResponse.json()) as {
      items: Array<{ version: number; decision: string }>;
    };
    expect(
      listApprovalsBody.items.some(
        (item) => item.version === 1 && item.decision === "approved",
      ),
    ).toBe(true);

    const listBResponse = await app.request("/api/v1/rules/assets", {
      headers: tenantBHeaders,
    });
    expect(listBResponse.status).toBe(200);
    const listBBody = (await listBResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(listBBody.items.some((item) => item.id === asset.id)).toBe(false);
  });

  test("mcp 路由：401/400/策略审批审计链路与租户隔离", async () => {
    const unauthorizedResponse = await app.request("/api/v1/mcp/policies");
    expect(unauthorizedResponse.status).toBe(401);

    const nonce = createNonce("mcp-routes");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `MCP Tenant A ${nonce}`,
        slug: `mcp-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建失败，缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `MCP Tenant B ${nonce}`,
        slug: `mcp-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建失败，缺少 tenantId。");
    }

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      auth.accessToken,
      auth.userId,
    );
    const tenantBHeaders = await issueTenantScopedAuthHeaders(
      tenantBId,
      auth.accessToken,
      auth.userId,
    );

    const badPolicyResponse = await app.request(
      `/api/v1/mcp/policies/${encodeURIComponent(`tool-${nonce}`)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          riskLevel: "invalid",
          decision: "invalid",
        }),
      },
    );
    expect(badPolicyResponse.status).toBe(400);

    const policyResponse = await app.request(
      `/api/v1/mcp/policies/${encodeURIComponent(`tool-${nonce}`)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          riskLevel: "high",
          decision: "require_approval",
          reason: "高风险工具需要审批",
        }),
      },
    );
    expect(policyResponse.status).toBe(200);

    const evaluateBlockedResponse = await app.request("/api/v1/mcp/evaluate", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        reason: "首次评估，预期触发审批",
        metadata: {
          scenario: "evaluate-blocked",
        },
      }),
    });
    expect(evaluateBlockedResponse.status).toBe(200);
    const evaluateBlocked = (await evaluateBlockedResponse.json()) as {
      decision: string;
      result: string;
      approvalRequestId?: string;
      enforced: boolean;
      evaluatedDecision: string;
      invocation: {
        id: string;
        enforced: boolean;
        evaluatedDecision?: string;
      };
    };
    expect(evaluateBlocked.decision).toBe("require_approval");
    expect(evaluateBlocked.result).toBe("blocked");
    expect(typeof evaluateBlocked.approvalRequestId).toBe("string");
    expect(evaluateBlocked.enforced).toBe(true);
    expect(evaluateBlocked.evaluatedDecision).toBe("require_approval");
    expect(evaluateBlocked.invocation.enforced).toBe(true);
    expect(evaluateBlocked.invocation.evaluatedDecision).toBe(
      "require_approval",
    );

    const approveEvaluateRequestResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(evaluateBlocked.approvalRequestId as string)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          reason: "评估流审批通过",
        }),
      },
    );
    expect(approveEvaluateRequestResponse.status).toBe(200);

    const evaluateApprovedResponse = await app.request("/api/v1/mcp/evaluate", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        approvalRequestId: evaluateBlocked.approvalRequestId,
        reason: "复评通过",
      }),
    });
    expect(evaluateApprovedResponse.status).toBe(200);
    const evaluateApproved = (await evaluateApprovedResponse.json()) as {
      result: string;
      approvalRequestId?: string;
      invocation: { result: string };
    };
    expect(evaluateApproved.result).toBe("approved");
    expect(evaluateApproved.approvalRequestId).toBe(
      evaluateBlocked.approvalRequestId,
    );
    expect(evaluateApproved.invocation.result).toBe("approved");

    const createApprovalResponse = await app.request("/api/v1/mcp/approvals", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        reason: "临时申请",
      }),
    });
    expect(createApprovalResponse.status).toBe(201);
    const approval = (await createApprovalResponse.json()) as {
      id: string;
      status: string;
    };
    expect(approval.status).toBe("pending");

    const approveResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(approval.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          reason: "审核通过",
        }),
      },
    );
    expect(approveResponse.status).toBe(200);
    const approved = (await approveResponse.json()) as { status: string };
    expect(approved.status).toBe("approved");

    const approveAgainResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(approval.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          reason: "重复审批应冲突",
        }),
      },
    );
    expect(approveAgainResponse.status).toBe(409);

    const badEnforcedInvocationResponse = await app.request(
      "/api/v1/mcp/invocations",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          toolId: `tool-${nonce}`,
          decision: "require_approval",
          result: "blocked",
          enforced: true,
        }),
      },
    );
    expect(badEnforcedInvocationResponse.status).toBe(400);

    const invocationResponse = await app.request("/api/v1/mcp/invocations", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        decision: "require_approval",
        result: "approved",
        approvalRequestId: approval.id,
        enforced: true,
        evaluatedDecision: "require_approval",
        metadata: {
          scenario: "unit-test",
        },
      }),
    });
    expect(invocationResponse.status).toBe(201);
    const invocation = (await invocationResponse.json()) as {
      id: string;
      enforced: boolean;
      evaluatedDecision?: string;
    };
    expect(invocation.enforced).toBe(true);
    expect(invocation.evaluatedDecision).toBe("require_approval");

    const listAResponse = await app.request("/api/v1/mcp/invocations", {
      headers: tenantAHeaders,
    });
    expect(listAResponse.status).toBe(200);
    const listABody = (await listAResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(listABody.items.some((item) => item.id === invocation.id)).toBe(
      true,
    );

    const listBResponse = await app.request("/api/v1/mcp/invocations", {
      headers: tenantBHeaders,
    });
    expect(listBResponse.status).toBe(200);
    const listBBody = (await listBResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(listBBody.items.some((item) => item.id === invocation.id)).toBe(
      false,
    );
  });

  test("open-platform 路由：401/403/400/主流程与租户隔离", async () => {
    const unauthorizedResponse = await app.request("/api/v1/api-keys");
    expect(unauthorizedResponse.status).toBe(401);
    resetWebhookReplayExecutionWorkerForTests();

    const nonce = createNonce("open-platform-routes");
    const auth = await getDefaultAuthContext();
    const member = await registerAndLoginUser(`${nonce}-member`);
    if (!member.userId) {
      throw new Error("open-platform 测试用户缺少 userId。");
    }

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `OpenPlatform Tenant A ${nonce}`,
        slug: `open-platform-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建失败，缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `OpenPlatform Tenant B ${nonce}`,
        slug: `open-platform-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建失败，缺少 tenantId。");
    }

    const addMemberResult = await addTenantMemberByAuth(
      auth.accessToken,
      {
        tenantId: tenantAId,
        userId: member.userId,
        tenantRole: "member",
      },
      auth.userId,
    );
    assertApiStatus(addMemberResult, [201]);

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      auth.accessToken,
      auth.userId,
    );
    const tenantAMemberHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      member.accessToken,
      member.userId,
    );
    const tenantBHeaders = await issueTenantScopedAuthHeaders(
      tenantBId,
      auth.accessToken,
      auth.userId,
    );
    const replayRepository = repository as unknown as {
      createReplayBaseline?: (
        inputTenantId: string,
        input: {
          name: string;
          datasetRef: string;
          scenarioCount: number;
          metadata?: Record<string, unknown>;
        },
      ) => Promise<{ id: string }>;
      createReplayJob?: (
        inputTenantId: string,
        input: {
          baselineId: string;
          status: string;
          parameters?: Record<string, unknown>;
          summary?: Record<string, unknown>;
          diff?: Record<string, unknown>;
          error?: string | null;
          startedAt?: string;
          finishedAt?: string;
          createdAt?: string;
        },
      ) => Promise<{ id: string }>;
      updateReplayJob?: (
        inputTenantId: string,
        replayJobId: string,
        input: {
          fromStatuses?: string[];
          status?: string;
          summary?: Record<string, unknown>;
          diff?: Record<string, unknown>;
          error?: string | null;
          startedAt?: string | null;
          finishedAt?: string | null;
          updatedAt?: string;
        },
      ) => Promise<{ id: string } | null>;
    };
    if (
      typeof replayRepository.createReplayBaseline !== "function" ||
      typeof replayRepository.createReplayJob !== "function" ||
      typeof replayRepository.updateReplayJob !== "function"
    ) {
      throw new Error(
        "repository replay 方法不可用，无法准备 open-platform replay 测试数据。",
      );
    }

    const replayBaseline = await replayRepository.createReplayBaseline(
      tenantAId,
      {
        name: `open-platform-replay-${nonce}`,
        datasetRef: `dataset-open-platform-${nonce}`,
        scenarioCount: 2,
        metadata: {
          model: "gpt-4.1",
        },
      },
    );
    const replayStartedAt = new Date().toISOString();
    const replayCompletedAt = new Date(Date.now() + 1_000).toISOString();
    const startedReplayJob = await replayRepository.createReplayJob(tenantAId, {
      baselineId: replayBaseline.id,
      status: "pending",
      parameters: {
        candidateLabel: `candidate-started-${nonce}`,
      },
      summary: {
        totalCases: 2,
        processedCases: 0,
      },
    });
    await replayRepository.updateReplayJob(tenantAId, startedReplayJob.id, {
      fromStatuses: ["pending"],
      status: "running",
      startedAt: replayStartedAt,
      error: null,
    });
    const cancelledReplayJob = await replayRepository.createReplayJob(
      tenantAId,
      {
        baselineId: replayBaseline.id,
        status: "pending",
        parameters: {
          candidateLabel: `candidate-cancelled-${nonce}`,
        },
        summary: {
          totalCases: 2,
          processedCases: 0,
        },
      },
    );
    await replayRepository.updateReplayJob(tenantAId, cancelledReplayJob.id, {
      fromStatuses: ["pending"],
      status: "cancelled",
      startedAt: replayStartedAt,
      finishedAt: replayCompletedAt,
      error: "cancelled by test",
    });

    const openapiResponse = await app.request("/api/v1/openapi.json", {
      headers: tenantAHeaders,
    });
    expect(openapiResponse.status).toBe(200);
    const openapiBody = (await openapiResponse.json()) as {
      paths?: Record<string, unknown>;
      components?: {
        schemas?: Record<string, unknown>;
      };
    };
    expect(Boolean(openapiBody.paths?.["/api/v1/replay/jobs"])).toBe(true);
    expect(Boolean(openapiBody.paths?.["/api/v1/webhooks/{id}/replay"])).toBe(
      true,
    );
    expect(Boolean(openapiBody.paths?.["/api/v1/webhooks/replay-tasks"])).toBe(
      true,
    );
    expect(
      Boolean(openapiBody.paths?.["/api/v1/webhooks/replay-tasks/{id}"]),
    ).toBe(true);
    const qualityEventsPath = openapiBody.paths?.["/api/v1/quality/events"] as
      | { post?: { deprecated?: boolean } }
      | undefined;
    const qualityMetricsDailyPath = openapiBody.paths?.[
      "/api/v1/quality/metrics/daily"
    ] as { get?: { deprecated?: boolean } } | undefined;
    const qualityScorecardsPath = openapiBody.paths?.[
      "/api/v1/quality/scorecards"
    ] as { get?: { deprecated?: boolean } } | undefined;
    const qualityScorecardByIdPath = openapiBody.paths?.[
      "/api/v1/quality/scorecards/{id}"
    ] as { put?: { deprecated?: boolean } } | undefined;
    const replayBaselinesPath = openapiBody.paths?.[
      "/api/v1/replay/baselines"
    ] as
      | { get?: { deprecated?: boolean }; post?: { deprecated?: boolean } }
      | undefined;
    const replayJobsPath = openapiBody.paths?.["/api/v1/replay/jobs"] as
      | { get?: { deprecated?: boolean }; post?: { deprecated?: boolean } }
      | undefined;
    const replayJobByIdPath = openapiBody.paths?.[
      "/api/v1/replay/jobs/{id}"
    ] as { get?: { deprecated?: boolean } } | undefined;
    const replayDiffPath = openapiBody.paths?.[
      "/api/v1/replay/jobs/{id}/diff"
    ] as { get?: { deprecated?: boolean } } | undefined;
    expect(qualityEventsPath?.post?.deprecated).toBe(true);
    expect(qualityMetricsDailyPath?.get?.deprecated).toBe(true);
    expect(qualityScorecardsPath?.get?.deprecated).toBe(true);
    expect(qualityScorecardByIdPath?.put?.deprecated).toBe(true);
    expect(replayBaselinesPath?.get?.deprecated).toBe(true);
    expect(replayBaselinesPath?.post?.deprecated).toBe(true);
    expect(replayJobsPath?.get?.deprecated).toBe(true);
    expect(replayJobsPath?.post?.deprecated).toBe(true);
    expect(replayJobByIdPath?.get?.deprecated).toBe(true);
    expect(replayDiffPath?.get?.deprecated).toBe(true);
    expect(
      Boolean(openapiBody.components?.schemas?.["WebhookReplayTask"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.components?.schemas?.["QualityDailyMetricsResponse"]),
    ).toBe(true);
    expect(Boolean(openapiBody.paths?.["/api/v2/quality/evaluations"])).toBe(
      true,
    );
    expect(Boolean(openapiBody.paths?.["/api/v2/quality/metrics"])).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/quality/reports/cost-correlation"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/quality/reports/project-trends"]),
    ).toBe(true);
    expect(Boolean(openapiBody.paths?.["/api/v2/quality/scorecards"])).toBe(
      true,
    );
    expect(
      Boolean(openapiBody.paths?.["/api/v2/quality/scorecards/{id}"]),
    ).toBe(true);
    expect(Boolean(openapiBody.paths?.["/api/v2/replay/datasets"])).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/datasets/{id}/cases"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/datasets/{id}/materialize"]),
    ).toBe(true);
    expect(Boolean(openapiBody.paths?.["/api/v2/replay/runs"])).toBe(true);
    expect(Boolean(openapiBody.paths?.["/api/v2/replay/runs/{id}"])).toBe(true);
    expect(Boolean(openapiBody.paths?.["/api/v2/replay/runs/{id}/diffs"])).toBe(
      true,
    );
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/runs/{id}/artifacts"]),
    ).toBe(true);
    expect(
      Boolean(
        openapiBody.paths?.[
          "/api/v2/replay/runs/{id}/artifacts/{artifactType}/download"
        ],
      ),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/residency/policies/current"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/residency/region-mappings"]),
    ).toBe(true);
    expect(Boolean(openapiBody.paths?.["/api/v2/residency/replications"])).toBe(
      true,
    );
    expect(
      Boolean(
        openapiBody.paths?.["/api/v2/residency/replications/{id}/approvals"],
      ),
    ).toBe(true);
    expect(
      Boolean(
        openapiBody.paths?.["/api/v2/residency/replications/{id}/cancel"],
      ),
    ).toBe(true);
    expect(
      Boolean(openapiBody.components?.schemas?.["QualityEvaluationInputV2"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.components?.schemas?.["QualityMetricsResponseV2"]),
    ).toBe(true);
    expect(
      Boolean(
        openapiBody.components?.schemas?.["QualityCostCorrelationResponseV2"],
      ),
    ).toBe(true);
    expect(
      Boolean(
        openapiBody.components?.schemas?.["QualityProjectTrendResponseV2"],
      ),
    ).toBe(true);
    const qualityProjectTrendsPath = openapiBody.paths?.[
      "/api/v2/quality/reports/project-trends"
    ] as
      | {
          get?: {
            responses?: {
              [status: string]: {
                content?: {
                  [contentType: string]: {
                    schema?: {
                      $ref?: string;
                    };
                  };
                };
              };
            };
          };
        }
      | undefined;
    const replayRunDiffsPath = openapiBody.paths?.[
      "/api/v2/replay/runs/{id}/diffs"
    ] as
      | {
          get?: {
            parameters?: Array<{ name?: string; in?: string }>;
          };
        }
      | undefined;
    const replayMaterializePath = openapiBody.paths?.[
      "/api/v2/replay/datasets/{id}/materialize"
    ] as
      | {
          post?: {
            requestBody?: {
              content?: {
                [contentType: string]: {
                  schema?: {
                    $ref?: string;
                  };
                };
              };
            };
            responses?: {
              [status: string]: {
                content?: {
                  [contentType: string]: {
                    schema?: {
                      $ref?: string;
                    };
                  };
                };
              };
            };
          };
        }
      | undefined;
    const replayDatasetInputV2 = openapiBody.components?.schemas?.[
      "ReplayDatasetInputV2"
    ] as
      | {
          required?: string[];
          properties?: Record<string, { description?: string }>;
        }
      | undefined;
    const replayDatasetMaterializeInputV2 = openapiBody.components?.schemas?.[
      "ReplayDatasetMaterializeInputV2"
    ] as
      | {
          anyOf?: Array<{ required?: string[] }>;
          properties?: Record<string, { description?: string }>;
        }
      | undefined;
    const replayDatasetMaterializeResponseV2 = openapiBody.components
      ?.schemas?.["ReplayDatasetMaterializeResponseV2"] as
      | {
          required?: string[];
          properties?: Record<string, { $ref?: string }>;
        }
      | undefined;
    const replayRunInputV2 = openapiBody.components?.schemas?.[
      "ReplayRunInputV2"
    ] as
      | {
          required?: string[];
          properties?: Record<string, { description?: string }>;
        }
      | undefined;
    const replayRunV2 = openapiBody.components?.schemas?.["ReplayRunV2"] as
      | {
          required?: string[];
          properties?: Record<string, { description?: string; $ref?: string }>;
        }
      | undefined;
    const replayRunDiffsSchemaV2 = openapiBody.components?.schemas?.[
      "ReplayRunDiffsResponseV2"
    ] as
      | {
          properties?: Record<
            string,
            | { description?: string; $ref?: string }
            | {
                required?: string[];
                properties?: Record<string, { description?: string }>;
              }
          >;
        }
      | undefined;
    const replayRunSummaryV2 = openapiBody.components?.schemas?.[
      "ReplayRunSummaryV2"
    ] as
      | {
          properties?: Record<string, { $ref?: string }>;
        }
      | undefined;
    const replayRunSummaryDigestV2 = openapiBody.components?.schemas?.[
      "ReplayRunSummaryDigestV2"
    ] as
      | {
          properties?: Record<string, { $ref?: string }>;
        }
      | undefined;
    const replayArtifactItemV2 = openapiBody.components?.schemas?.[
      "ReplayArtifactItemV2"
    ] as
      | {
          properties?: Record<string, { $ref?: string }>;
        }
      | undefined;
    const replayRunArtifactsSchemaV2 = openapiBody.components?.schemas?.[
      "ReplayRunArtifactsResponseV2"
    ] as
      | {
          required?: string[];
          properties?: Record<string, { description?: string }>;
        }
      | undefined;
    const replayRunDiffsProperties = replayRunDiffsSchemaV2?.properties as
      | Record<string, { description?: string; $ref?: string }>
      | undefined;
    const replayRunDiffFilters = replayRunDiffsProperties?.filters as
      | {
          required?: string[];
          properties?: Record<string, { description?: string }>;
        }
      | undefined;
    expect(
      qualityProjectTrendsPath?.get?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/QualityProjectTrendResponseV2");
    expect(
      replayRunDiffsPath?.get?.parameters?.some(
        (parameter: { name?: string; in?: string }) =>
          parameter.name === "keyword" && parameter.in === "query",
      ),
    ).toBe(true);
    expect(
      replayRunDiffsPath?.get?.parameters?.some(
        (parameter: { name?: string; in?: string }) =>
          parameter.name === "datasetId" && parameter.in === "query",
      ),
    ).toBe(true);
    expect(
      replayRunDiffsPath?.get?.parameters?.some(
        (parameter: { name?: string; in?: string }) =>
          parameter.name === "baselineId" && parameter.in === "query",
      ),
    ).toBe(true);
    expect(
      replayMaterializePath?.post?.requestBody?.content?.["application/json"]
        ?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayDatasetMaterializeInputV2");
    expect(
      replayMaterializePath?.post?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayDatasetMaterializeResponseV2");
    expect(
      Boolean(
        openapiBody.components?.schemas?.["ReplayDatasetCaseWriteInputV2"],
      ),
    ).toBe(true);
    expect(
      Boolean(
        openapiBody.components?.schemas?.["ReplayDatasetCasesReplaceInputV2"],
      ),
    ).toBe(true);
    expect(
      replayDatasetMaterializeInputV2?.anyOf?.some((item) =>
        item.required?.includes("sessionIds"),
      ),
    ).toBe(true);
    expect(
      replayDatasetMaterializeInputV2?.anyOf?.some((item) =>
        item.required?.includes("filters"),
      ),
    ).toBe(true);
    expect(
      replayDatasetMaterializeInputV2?.properties?.sanitized?.description,
    ).toContain("默认 true");
    expect(replayDatasetMaterializeResponseV2?.required).toContain("filters");
    expect(
      replayDatasetMaterializeResponseV2?.properties?.sourceSummary?.$ref,
    ).toBe("#/components/schemas/ReplaySourceSummaryV2");
    expect(replayDatasetInputV2?.required).toContain("datasetRef");
    expect(replayDatasetInputV2?.required?.includes("datasetId")).toBe(false);
    expect(replayDatasetInputV2?.properties?.datasetId?.description).toContain(
      "兼容别名",
    );
    expect(replayRunInputV2?.required).toContain("datasetId");
    expect(replayRunInputV2?.required?.includes("baselineId")).toBe(false);
    expect(replayRunInputV2?.properties?.baselineId?.description).toContain(
      "兼容别名",
    );
    expect(replayRunV2?.required?.includes("baselineId")).toBe(false);
    expect(replayRunV2?.properties?.baselineId?.description).toContain(
      "兼容别名",
    );
    expect(replayRunV2?.properties?.summary?.$ref).toBe(
      "#/components/schemas/ReplayRunSummaryV2",
    );
    expect(replayRunDiffFilters?.required).toContain("datasetId");
    expect(replayRunDiffFilters?.required?.includes("baselineId")).toBe(false);
    expect(replayRunDiffFilters?.properties?.runId?.description).toContain(
      "ReplayRunV2.id",
    );
    expect(replayRunDiffFilters?.properties?.jobId?.description).toContain(
      "兼容别名",
    );
    expect(replayRunDiffsProperties?.jobId?.description).toContain("兼容别名");
    expect(replayRunDiffsProperties?.runId?.description).toContain(
      "ReplayRunV2.id",
    );
    expect(replayRunDiffsProperties?.summary?.$ref).toBe(
      "#/components/schemas/ReplayRunSummaryV2",
    );
    expect(replayRunSummaryV2?.properties?.digest?.$ref).toBe(
      "#/components/schemas/ReplayRunSummaryDigestV2",
    );
    expect(replayRunSummaryDigestV2?.properties?.sourceSummary?.$ref).toBe(
      "#/components/schemas/ReplaySourceSummaryV2",
    );
    expect(replayRunArtifactsSchemaV2?.required).toContain("datasetId");
    expect(
      replayRunArtifactsSchemaV2?.properties?.runId?.description,
    ).toContain("ReplayRunV2.id");
    expect(
      replayRunArtifactsSchemaV2?.properties?.jobId?.description,
    ).toContain("兼容别名");
    expect(replayArtifactItemV2?.properties?.inline?.$ref).toBe(
      "#/components/schemas/ReplayArtifactInlinePreviewV2",
    );
    expect(Boolean(openapiBody.components?.schemas?.["ReplayRunV2"])).toBe(
      true,
    );
    expect(
      Boolean(openapiBody.components?.schemas?.["TenantResidencyPolicyV2"]),
    ).toBe(true);
    expect(Boolean(openapiBody.components?.schemas?.["ReplicationJobV2"])).toBe(
      true,
    );

    const forbiddenCreateKeyResponse = await app.request("/api/v1/api-keys", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAMemberHeaders,
      },
      body: JSON.stringify({
        name: `forbidden-member-key-${nonce}`,
      }),
    });
    expect(forbiddenCreateKeyResponse.status).toBe(403);

    const badCreateKeyResponse = await app.request("/api/v1/api-keys", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({}),
    });
    expect(badCreateKeyResponse.status).toBe(400);

    const createKeyResponse = await app.request("/api/v1/api-keys", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        name: `open-key-${nonce}`,
        scope: "write",
      }),
    });
    expect(createKeyResponse.status).toBe(201);
    const createdKey = (await createKeyResponse.json()) as {
      id: string;
      tenantId: string;
    };
    expect(createdKey.tenantId).toBe(tenantAId);

    const listKeysResponse = await app.request("/api/v1/api-keys", {
      headers: tenantAHeaders,
    });
    expect(listKeysResponse.status).toBe(200);
    const listKeysBody = (await listKeysResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(listKeysBody.items.some((item) => item.id === createdKey.id)).toBe(
      true,
    );

    const revokeResponse = await app.request(
      `/api/v1/api-keys/${encodeURIComponent(createdKey.id)}/revoke`,
      {
        method: "POST",
        headers: tenantAHeaders,
      },
    );
    expect(revokeResponse.status).toBe(200);

    const badWebhookResponse = await app.request("/api/v1/webhooks", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        name: "bad-webhook",
        url: "not-a-url",
        events: ["quality.event.created"],
      }),
    });
    expect(badWebhookResponse.status).toBe(400);

    const createWebhookResponse = await app.request("/api/v1/webhooks", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        name: `Open Webhook ${nonce}`,
        url: "https://example.com/webhook",
        events: [
          "api_key.created",
          "quality.event.created",
          "replay.run.started",
          "replay.job.completed",
          "replay.run.cancelled",
        ],
      }),
    });
    expect(createWebhookResponse.status).toBe(201);
    const createdWebhook = (await createWebhookResponse.json()) as {
      id: string;
    };

    const forbiddenReplayResponse = await app.request(
      `/api/v1/webhooks/${encodeURIComponent(createdWebhook.id)}/replay`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAMemberHeaders,
        },
        body: JSON.stringify({ dryRun: true }),
      },
    );
    expect(forbiddenReplayResponse.status).toBe(403);

    const badReplayResponse = await app.request(
      `/api/v1/webhooks/${encodeURIComponent(createdWebhook.id)}/replay`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          from: "2026-03-10T00:00:00.000Z",
          to: "2026-03-01T00:00:00.000Z",
        }),
      },
    );
    expect(badReplayResponse.status).toBe(400);

    const replayResponse = await app.request(
      `/api/v1/webhooks/${encodeURIComponent(createdWebhook.id)}/replay`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          eventType: "replay.run.started",
          limit: 50,
          dryRun: true,
        }),
      },
    );
    expect(replayResponse.status).toBe(202);
    const replayBody = (await replayResponse.json()) as {
      id: string;
      webhookId: string;
      status: string;
      dryRun: boolean;
      filters: {
        eventType?: string;
        limit?: number;
      };
    };
    expect(typeof replayBody.id).toBe("string");
    expect(replayBody.webhookId).toBe(createdWebhook.id);
    expect(replayBody.status).toBe("queued");
    expect(replayBody.dryRun).toBe(true);
    expect(replayBody.filters.eventType).toBe("replay.run.started");
    expect(replayBody.filters.limit).toBe(50);

    const cancelledReplayResponse = await app.request(
      `/api/v1/webhooks/${encodeURIComponent(createdWebhook.id)}/replay`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          eventType: "replay.run.cancelled",
          limit: 50,
          dryRun: true,
        }),
      },
    );
    expect(cancelledReplayResponse.status).toBe(202);
    const cancelledReplayBody = (await cancelledReplayResponse.json()) as {
      filters: {
        eventType?: string;
      };
    };
    expect(cancelledReplayBody.filters.eventType).toBe("replay.run.cancelled");

    await flushWebhookReplayExecutionQueueForTests();

    const replayTaskDetailResponse = await app.request(
      `/api/v1/webhooks/replay-tasks/${encodeURIComponent(replayBody.id)}`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(replayTaskDetailResponse.status).toBe(200);
    const replayTaskDetailBody = (await replayTaskDetailResponse.json()) as {
      id: string;
      webhookId: string;
      status: string;
      result?: Record<string, unknown>;
    };
    expect(replayTaskDetailBody.id).toBe(replayBody.id);
    expect(replayTaskDetailBody.webhookId).toBe(createdWebhook.id);
    expect(replayTaskDetailBody.status).toBe("completed");
    expect(replayTaskDetailBody.result?.["executor"]).toBe(
      "builtin-webhook-replay",
    );
    expect(
      Number(replayTaskDetailBody.result?.["scannedEvents"] ?? 0),
    ).toBeGreaterThanOrEqual(1);
    expect(replayTaskDetailBody.result?.["dispatchedEvents"]).toBe(0);
    expect(replayTaskDetailBody.result?.["failedEvents"]).toBe(0);

    const replayTaskListResponse = await app.request(
      `/api/v1/webhooks/replay-tasks?webhookId=${encodeURIComponent(createdWebhook.id)}&status=completed&limit=50`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(replayTaskListResponse.status).toBe(200);
    const replayTaskListBody = (await replayTaskListResponse.json()) as {
      items: Array<{ id: string }>;
      total: number;
    };
    expect(
      replayTaskListBody.items.some((item) => item.id === replayBody.id),
    ).toBe(true);
    expect(replayTaskListBody.total).toBeGreaterThanOrEqual(1);

    const crossTenantReplayResponse = await app.request(
      `/api/v1/webhooks/${encodeURIComponent(createdWebhook.id)}/replay`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantBHeaders,
        },
        body: JSON.stringify({ dryRun: true }),
      },
    );
    expect(crossTenantReplayResponse.status).toBe(404);

    const crossTenantReplayTaskResponse = await app.request(
      `/api/v1/webhooks/replay-tasks/${encodeURIComponent(replayBody.id)}`,
      {
        headers: tenantBHeaders,
      },
    );
    expect(crossTenantReplayTaskResponse.status).toBe(404);

    const replayAuditResponse = await app.request(
      `/api/v1/audits?action=control_plane.open_platform.webhook_replayed&keyword=${encodeURIComponent(
        createdWebhook.id,
      )}&limit=200`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(replayAuditResponse.status).toBe(200);
    const replayAudits = (await replayAuditResponse.json()) as {
      items: Array<{ metadata: Record<string, unknown> }>;
    };
    expect(
      replayAudits.items.some(
        (item) => item.metadata["webhookId"] === createdWebhook.id,
      ),
    ).toBe(true);

    const listWebhookTenantBResponse = await app.request("/api/v1/webhooks", {
      headers: tenantBHeaders,
    });
    expect(listWebhookTenantBResponse.status).toBe(200);
    const listWebhookTenantBBody =
      (await listWebhookTenantBResponse.json()) as {
        items: Array<{ id: string }>;
      };
    expect(
      listWebhookTenantBBody.items.some(
        (item) => item.id === createdWebhook.id,
      ),
    ).toBe(false);
    resetWebhookReplayExecutionWorkerForTests();
  });

  test("open-platform webhook replay：签名头生效并在 5xx 后重试成功", async () => {
    resetWebhookReplayExecutionWorkerForTests();
    const nonce = createNonce("open-platform-replay-signature");
    const auth = await getDefaultAuthContext();
    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `OpenPlatform Replay Tenant ${nonce}`,
        slug: `open-platform-replay-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("replay 签名测试：租户创建响应缺少 tenantId。");
    }
    const tenantHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      auth.accessToken,
      auth.userId,
    );

    const createKeyResponse = await app.request("/api/v1/api-keys", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantHeaders,
      },
      body: JSON.stringify({
        name: `open-platform-replay-key-${nonce}`,
        scope: "write",
      }),
    });
    expect(createKeyResponse.status).toBe(201);

    const replaySecret = `whsec-replay-${nonce}`;
    const createWebhookResponse = await app.request("/api/v1/webhooks", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantHeaders,
      },
      body: JSON.stringify({
        name: `Open Replay Webhook ${nonce}`,
        url: "https://example.com/webhook-replay",
        events: ["api_key.created"],
        secret: replaySecret,
      }),
    });
    expect(createWebhookResponse.status).toBe(201);
    const createdWebhook = (await createWebhookResponse.json()) as {
      id: string;
    };

    const originalFetch = globalThis.fetch;
    const originalRetryMax = Bun.env.WEBHOOK_REPLAY_MAX_RETRIES;
    const originalRetryBase = Bun.env.WEBHOOK_REPLAY_RETRY_BASE_DELAY_MS;
    const originalRetryMaxDelay = Bun.env.WEBHOOK_REPLAY_RETRY_MAX_DELAY_MS;
    const observedRequests: Array<{
      body: string;
      signature: string;
      timestamp: string;
      algorithm: string;
    }> = [];
    let dispatchAttemptCount = 0;

    try {
      Bun.env.WEBHOOK_REPLAY_MAX_RETRIES = "2";
      Bun.env.WEBHOOK_REPLAY_RETRY_BASE_DELAY_MS = "1";
      Bun.env.WEBHOOK_REPLAY_RETRY_MAX_DELAY_MS = "5";

      globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
        const request =
          input instanceof Request ? input : new Request(String(input), init);
        const body = await request.text();
        observedRequests.push({
          body,
          signature: request.headers.get("x-agentledger-signature") ?? "",
          timestamp:
            request.headers.get("x-agentledger-signature-timestamp") ?? "",
          algorithm:
            request.headers.get("x-agentledger-signature-algorithm") ?? "",
        });
        dispatchAttemptCount += 1;
        if (dispatchAttemptCount === 1) {
          return new Response("temporary unavailable", { status: 503 });
        }
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        });
      }) as typeof fetch;

      const now = Date.now();
      const replayResponse = await app.request(
        `/api/v1/webhooks/${encodeURIComponent(createdWebhook.id)}/replay`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...tenantHeaders,
          },
          body: JSON.stringify({
            eventType: "api_key.created",
            from: new Date(now - 60_000).toISOString(),
            to: new Date(now + 60_000).toISOString(),
            limit: 20,
            dryRun: false,
          }),
        },
      );
      expect(replayResponse.status).toBe(202);
      const replayTask = (await replayResponse.json()) as { id: string };

      await flushWebhookReplayExecutionQueueForTests();

      const replayTaskDetailResponse = await app.request(
        `/api/v1/webhooks/replay-tasks/${encodeURIComponent(replayTask.id)}`,
        {
          headers: tenantHeaders,
        },
      );
      expect(replayTaskDetailResponse.status).toBe(200);
      const replayTaskDetailBody = (await replayTaskDetailResponse.json()) as {
        status: string;
        result?: Record<string, unknown>;
      };
      expect(replayTaskDetailBody.status).toBe("completed");
      expect(
        Number(replayTaskDetailBody.result?.["dispatchedEvents"] ?? 0),
      ).toBeGreaterThanOrEqual(1);
      expect(replayTaskDetailBody.result?.["failedEvents"]).toBe(0);
      expect(
        Number(replayTaskDetailBody.result?.["retryCount"] ?? 0),
      ).toBeGreaterThanOrEqual(1);
      expect(
        Number(replayTaskDetailBody.result?.["retriedEvents"] ?? 0),
      ).toBeGreaterThanOrEqual(1);
      expect(replayTaskDetailBody.result?.["maxRetries"]).toBe(2);

      expect(dispatchAttemptCount).toBeGreaterThanOrEqual(2);
      expect(observedRequests.length).toBeGreaterThanOrEqual(2);
      for (const observed of observedRequests) {
        expect(observed.timestamp.length).toBeGreaterThan(0);
        expect(observed.algorithm).toBe("hmac-sha256");
        const expectedSignature = `v1=${createHmac("sha256", replaySecret)
          .update(`${observed.timestamp}.${observed.body}`)
          .digest("hex")}`;
        expect(observed.signature).toBe(expectedSignature);
      }
    } finally {
      globalThis.fetch = originalFetch;
      if (originalRetryMax === undefined) {
        delete Bun.env.WEBHOOK_REPLAY_MAX_RETRIES;
      } else {
        Bun.env.WEBHOOK_REPLAY_MAX_RETRIES = originalRetryMax;
      }
      if (originalRetryBase === undefined) {
        delete Bun.env.WEBHOOK_REPLAY_RETRY_BASE_DELAY_MS;
      } else {
        Bun.env.WEBHOOK_REPLAY_RETRY_BASE_DELAY_MS = originalRetryBase;
      }
      if (originalRetryMaxDelay === undefined) {
        delete Bun.env.WEBHOOK_REPLAY_RETRY_MAX_DELAY_MS;
      } else {
        Bun.env.WEBHOOK_REPLAY_RETRY_MAX_DELAY_MS = originalRetryMaxDelay;
      }
      resetWebhookReplayExecutionWorkerForTests();
    }
  });

  test("quality 路由：400/201/200 与租户隔离", async () => {
    const nonce = createNonce("quality-routes");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Quality Tenant A ${nonce}`,
        slug: `quality-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建失败，缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Quality Tenant B ${nonce}`,
        slug: `quality-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建失败，缺少 tenantId。");
    }

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      auth.accessToken,
      auth.userId,
    );
    const tenantBHeaders = await issueTenantScopedAuthHeaders(
      tenantBId,
      auth.accessToken,
      auth.userId,
    );

    const badEventResponse = await app.request("/api/v1/quality/events", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        eventType: "answer.correctness",
        score: 120,
      }),
    });
    expect(badEventResponse.status).toBe(400);

    const createEventResponse = await app.request("/api/v1/quality/events", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        sessionId: `sess-${nonce}`,
        metric: "accuracy",
        score: 86,
        sampleCount: 12,
        occurredAt: "2026-03-04T08:00:00.000Z",
      }),
    });
    expect(createEventResponse.status).toBe(201);
    const createExternalEventResponse = await app.request(
      "/api/v1/quality/events",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          replayJobId: `job-${nonce}`,
          metric: "accuracy",
          score: 91,
          sampleCount: 8,
          occurredAt: "2026-03-04T10:00:00.000Z",
          externalSource: {
            provider: "github",
            repo: `agentledger/${nonce}`,
            workflow: "ci-main",
            runId: `run-${nonce}`,
          },
        }),
      },
    );
    expect(createExternalEventResponse.status).toBe(201);
    const createExternalEventBody =
      (await createExternalEventResponse.json()) as {
        externalSource?: {
          provider?: string;
          repo?: string;
        };
      };
    expect(createExternalEventBody.externalSource?.provider).toBe("github");
    expect(createExternalEventBody.externalSource?.repo).toBe(
      `agentledger/${nonce}`,
    );

    const badDailyMetricsResponse = await app.request(
      "/api/v1/quality/metrics/daily?from=2026-03-06&to=2026-03-01",
      {
        headers: tenantAHeaders,
      },
    );
    expect(badDailyMetricsResponse.status).toBe(400);

    const dailyMetricsResponse = await app.request(
      "/api/v1/quality/metrics/daily?from=2026-03-04&to=2026-03-04",
      {
        headers: tenantAHeaders,
      },
    );
    expect(dailyMetricsResponse.status).toBe(200);
    const dailyMetricsBody = (await dailyMetricsResponse.json()) as {
      items: Array<{ date: string; totalEvents: number }>;
    };
    expect(
      dailyMetricsBody.items.some(
        (item) => item.date === "2026-03-04" && item.totalEvents >= 1,
      ),
    ).toBe(true);

    const groupedMetricsResponse = await app.request(
      `/api/v1/quality/metrics/daily?from=2026-03-04&to=2026-03-04&provider=github&groupBy=repo&limit=50`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(groupedMetricsResponse.status).toBe(200);
    const groupedMetricsBody = (await groupedMetricsResponse.json()) as {
      items: Array<{ date: string; totalEvents: number }>;
      groups?: Array<{ groupBy: string; value: string; totalEvents: number }>;
    };
    expect(groupedMetricsBody.items.length).toBeGreaterThanOrEqual(1);
    expect(
      groupedMetricsBody.groups?.some(
        (item) =>
          item.groupBy === "repo" &&
          item.value === `agentledger/${nonce}` &&
          item.totalEvents >= 1,
      ),
    ).toBe(true);

    const scorecardId = "accuracy";
    const upsertScorecardResponse = await app.request(
      `/api/v1/quality/scorecards/${encodeURIComponent(scorecardId)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          targetScore: 90,
          warningScore: 75,
          criticalScore: 60,
          weight: 1,
          enabled: true,
          updatedAt: "2026-03-04T08:10:00.000Z",
        }),
      },
    );
    expect(upsertScorecardResponse.status).toBe(200);

    const listScorecardsAResponse = await app.request(
      "/api/v1/quality/scorecards",
      {
        headers: tenantAHeaders,
      },
    );
    expect(listScorecardsAResponse.status).toBe(200);
    const listScorecardsABody = (await listScorecardsAResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(
      listScorecardsABody.items.some((item) => item.id === scorecardId),
    ).toBe(true);

    const listScorecardsBResponse = await app.request(
      "/api/v1/quality/scorecards",
      {
        headers: tenantBHeaders,
      },
    );
    expect(listScorecardsBResponse.status).toBe(200);
    const listScorecardsBBody = (await listScorecardsBResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(
      listScorecardsBBody.items.some((item) => item.id === scorecardId),
    ).toBe(false);
  });

  test("replay 路由：400/201/200 与租户隔离", async () => {
    resetReplayJobExecutionWorkerForTests();
    const nonce = createNonce("replay-routes");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Replay Tenant A ${nonce}`,
        slug: `replay-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建失败，缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Replay Tenant B ${nonce}`,
        slug: `replay-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建失败，缺少 tenantId。");
    }

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      auth.accessToken,
      auth.userId,
    );
    const tenantBHeaders = await issueTenantScopedAuthHeaders(
      tenantBId,
      auth.accessToken,
      auth.userId,
    );

    const badBaselineResponse = await app.request("/api/v1/replay/baselines", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({}),
    });
    expect(badBaselineResponse.status).toBe(400);

    const createBaselineResponse = await app.request(
      "/api/v1/replay/baselines",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          name: `Regression Baseline ${nonce}`,
          datasetId: "golden-set-v1",
          model: "gpt-4.1",
          sampleCount: 12,
        }),
      },
    );
    expect(createBaselineResponse.status).toBe(201);
    const baseline = (await createBaselineResponse.json()) as {
      id: string;
      tenantId: string;
    };
    expect(baseline.tenantId).toBe(tenantAId);

    const duplicateBaselineResponse = await app.request(
      "/api/v1/replay/baselines",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          name: `Regression Baseline ${nonce}`,
          datasetId: "golden-set-v2",
          model: "gpt-4.1",
        }),
      },
    );
    expect(duplicateBaselineResponse.status).toBe(409);

    const listBaselinesFilteredResponse = await app.request(
      "/api/v1/replay/baselines?model=gpt-4.1&datasetId=golden-set-v1&limit=10",
      {
        headers: tenantAHeaders,
      },
    );
    expect(listBaselinesFilteredResponse.status).toBe(200);
    const listBaselinesFilteredBody =
      (await listBaselinesFilteredResponse.json()) as {
        items: Array<{ id: string }>;
        total: number;
        filters: {
          model?: string;
          datasetId?: string;
          limit?: number;
        };
      };
    expect(listBaselinesFilteredBody.filters.model).toBe("gpt-4.1");
    expect(listBaselinesFilteredBody.filters.datasetId).toBe("golden-set-v1");
    expect(listBaselinesFilteredBody.filters.limit).toBe(10);
    expect(listBaselinesFilteredBody.total).toBeGreaterThanOrEqual(1);
    expect(
      listBaselinesFilteredBody.items.some((item) => item.id === baseline.id),
    ).toBe(true);

    const badBaselineFilterResponse = await app.request(
      "/api/v1/replay/baselines?from=invalid-date",
      {
        headers: tenantAHeaders,
      },
    );
    expect(badBaselineFilterResponse.status).toBe(400);

    const badCreateJobResponse = await app.request("/api/v1/replay/jobs", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        baselineId: baseline.id,
        candidateLabel: "candidate-bad",
        sampleLimit: 0,
      }),
    });
    expect(badCreateJobResponse.status).toBe(400);

    const missingBaselineJobResponse = await app.request(
      "/api/v1/replay/jobs",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          baselineId: "missing-baseline",
          candidateLabel: "candidate-missing",
          sampleLimit: 1,
        }),
      },
    );
    expect(missingBaselineJobResponse.status).toBe(404);

    const createJobResponse = await app.request("/api/v1/replay/jobs", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        baselineId: baseline.id,
        candidateLabel: "candidate-v2",
        sampleLimit: 12,
      }),
    });
    expect(createJobResponse.status).toBe(201);
    const replayJob = (await createJobResponse.json()) as {
      id: string;
      totalCases: number;
      status: string;
      processedCases: number;
      baselineId: string;
    };
    expect(replayJob.baselineId).toBe(baseline.id);
    expect(replayJob.status).toBe("pending");
    expect(replayJob.totalCases).toBe(12);
    expect(replayJob.processedCases).toBe(0);

    const createSafetyJobResponse = await app.request("/api/v1/replay/jobs", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...tenantAHeaders,
      },
      body: JSON.stringify({
        baselineId: baseline.id,
        candidateLabel: "candidate-safety",
        sampleLimit: 8,
        metadata: {
          metric: "safety",
        },
      }),
    });
    expect(createSafetyJobResponse.status).toBe(201);
    const safetyReplayJob = (await createSafetyJobResponse.json()) as {
      id: string;
      totalCases: number;
      status: string;
      baselineId: string;
    };
    expect(safetyReplayJob.baselineId).toBe(baseline.id);
    expect(safetyReplayJob.status).toBe("pending");
    expect(safetyReplayJob.totalCases).toBe(8);

    const listJobsAResponse = await app.request("/api/v1/replay/jobs", {
      headers: tenantAHeaders,
    });
    expect(listJobsAResponse.status).toBe(200);
    const listJobsABody = (await listJobsAResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(listJobsABody.items.some((item) => item.id === replayJob.id)).toBe(
      true,
    );

    const listJobsCandidateFilterResponse = await app.request(
      "/api/v1/replay/jobs?candidateLabel=safety&metric=safety&limit=10",
      {
        headers: tenantAHeaders,
      },
    );
    expect(listJobsCandidateFilterResponse.status).toBe(200);
    const listJobsCandidateFilterBody =
      (await listJobsCandidateFilterResponse.json()) as {
        items: Array<{ id: string }>;
        total: number;
        filters: {
          candidateLabel?: string;
          metric?: string;
          limit?: number;
        };
      };
    expect(listJobsCandidateFilterBody.filters.candidateLabel).toBe("safety");
    expect(listJobsCandidateFilterBody.filters.metric).toBe("safety");
    expect(listJobsCandidateFilterBody.filters.limit).toBe(10);
    expect(listJobsCandidateFilterBody.total).toBeGreaterThanOrEqual(1);
    expect(
      listJobsCandidateFilterBody.items.some(
        (item) => item.id === safetyReplayJob.id,
      ),
    ).toBe(true);
    expect(
      listJobsCandidateFilterBody.items.some(
        (item) => item.id === replayJob.id,
      ),
    ).toBe(false);

    const badMetricFilterResponse = await app.request(
      "/api/v1/replay/jobs?metric=unknown",
      {
        headers: tenantAHeaders,
      },
    );
    expect(badMetricFilterResponse.status).toBe(400);

    const badTimeRangeFilterResponse = await app.request(
      "/api/v1/replay/jobs?from=2026-03-10T00:00:00.000Z&to=2026-03-01T00:00:00.000Z",
      {
        headers: tenantAHeaders,
      },
    );
    expect(badTimeRangeFilterResponse.status).toBe(400);

    await flushReplayJobExecutionQueueForTests();

    const getJobAResponse = await app.request(
      `/api/v1/replay/jobs/${encodeURIComponent(replayJob.id)}`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(getJobAResponse.status).toBe(200);
    const getJobABody = (await getJobAResponse.json()) as {
      status: string;
      processedCases: number;
      totalCases: number;
    };
    expect(getJobABody.status).toBe("completed");
    expect(getJobABody.processedCases).toBe(getJobABody.totalCases);

    const diffResponse = await app.request(
      `/api/v1/replay/jobs/${encodeURIComponent(replayJob.id)}/diff`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(diffResponse.status).toBe(200);
    const diffBody = (await diffResponse.json()) as {
      jobId: string;
      summary: { totalCases: number };
    };
    expect(diffBody.jobId).toBe(replayJob.id);
    expect(diffBody.summary.totalCases).toBe(12);

    const listJobsBResponse = await app.request("/api/v1/replay/jobs", {
      headers: tenantBHeaders,
    });
    expect(listJobsBResponse.status).toBe(200);
    const listJobsBBody = (await listJobsBResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(listJobsBBody.items.some((item) => item.id === replayJob.id)).toBe(
      false,
    );
    expect(
      listJobsBBody.items.some((item) => item.id === safetyReplayJob.id),
    ).toBe(false);

    const getJobBResponse = await app.request(
      `/api/v1/replay/jobs/${encodeURIComponent(replayJob.id)}`,
      {
        headers: tenantBHeaders,
      },
    );
    expect(getJobBResponse.status).toBe(404);
    resetReplayJobExecutionWorkerForTests();
  });

  test("api-v2 quality 路由：评估写入、指标查询、成本相关性与评分卡更新", async () => {
    const nonce = createNonce("quality-v2-routes");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `QualityV2 Tenant A ${nonce}`,
        slug: `quality-v2-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建失败，缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `QualityV2 Tenant B ${nonce}`,
        slug: `quality-v2-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建失败，缺少 tenantId。");
    }

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      auth.accessToken,
      auth.userId,
    );
    const tenantBHeaders = await issueTenantScopedAuthHeaders(
      tenantBId,
      auth.accessToken,
      auth.userId,
    );

    const badEvaluationResponse = await app.request(
      "/api/v2/quality/evaluations",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          metric: "accuracy",
          score: 120,
        }),
      },
    );
    expect(badEvaluationResponse.status).toBe(400);

    const createEvaluationResponse = await app.request(
      "/api/v2/quality/evaluations",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          sessionId: `v2-sess-${nonce}`,
          metric: "accuracy",
          score: 88,
          sampleCount: 16,
          occurredAt: "2026-03-04T08:00:00.000Z",
        }),
      },
    );
    expect(createEvaluationResponse.status).toBe(201);

    const createExternalEvaluationResponse = await app.request(
      "/api/v2/quality/evaluations",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          replayRunId: `v2-run-${nonce}`,
          metric: "safety",
          score: 92,
          sampleCount: 9,
          occurredAt: "2026-03-04T10:00:00.000Z",
          externalSource: {
            provider: "github",
            repo: `agentledger/${nonce}`,
            workflow: "ci-main",
            runId: `run-${nonce}`,
          },
        }),
      },
    );
    expect(createExternalEvaluationResponse.status).toBe(201);

    const metricsResponse = await app.request(
      "/api/v2/quality/metrics?from=2026-03-04&to=2026-03-04&groupBy=repo&provider=github",
      {
        headers: tenantAHeaders,
      },
    );
    expect(metricsResponse.status).toBe(200);
    const metricsBody = (await metricsResponse.json()) as {
      items: Array<{ date: string; totalEvents: number }>;
      groups?: Array<{ groupBy: string; value: string }>;
    };
    expect(metricsBody.items.some((item) => item.date === "2026-03-04")).toBe(
      true,
    );
    expect(
      metricsBody.groups?.some(
        (group) =>
          group.groupBy === "repo" && group.value === `agentledger/${nonce}`,
      ),
    ).toBe(true);

    const correlationResponse = await app.request(
      "/api/v2/quality/reports/cost-correlation?from=2026-03-04&to=2026-03-04",
      {
        headers: tenantAHeaders,
      },
    );
    expect(correlationResponse.status).toBe(200);
    const correlationBody = (await correlationResponse.json()) as {
      items: Array<{ date: string; metric: string; totalEvents: number }>;
      summary: {
        metric: string;
      };
    };
    expect(correlationBody.summary.metric).toBe("all");
    expect(correlationBody.items.some((item) => item.metric === "all")).toBe(
      true,
    );
    expect(correlationBody.items.some((item) => item.totalEvents >= 1)).toBe(
      true,
    );

    const projectTrendsResponse = await app.request(
      "/api/v2/quality/reports/project-trends?from=2026-03-04&to=2026-03-04&provider=github&workflow=ci-main&limit=10",
      {
        headers: tenantAHeaders,
      },
    );
    expect(projectTrendsResponse.status).toBe(200);
    const projectTrendsBody = (await projectTrendsResponse.json()) as {
      items: Array<{
        project: string;
        metric: string;
        totalEvents: number;
        totalCost: number;
      }>;
      summary: {
        metric: string;
        totalEvents: number;
        from: string;
        to: string;
      };
      filters: {
        from: string | null;
        to: string | null;
        metric: string;
        provider: string | null;
        workflow: string | null;
        includeUnknown: boolean;
        limit: number;
      };
    };
    expect(projectTrendsBody.summary.metric).toBe("all");
    expect(projectTrendsBody.summary.from).toBe("2026-03-04T00:00:00.000Z");
    expect(projectTrendsBody.summary.to).toBe("2026-03-04T23:59:59.999Z");
    expect(
      projectTrendsBody.items.some(
        (item) => item.project === `agentledger/${nonce}`,
      ),
    ).toBe(true);
    expect(projectTrendsBody.items.some((item) => item.metric === "all")).toBe(
      true,
    );
    expect(projectTrendsBody.items.some((item) => item.totalEvents >= 1)).toBe(
      true,
    );
    expect(projectTrendsBody.items.every((item) => item.totalCost >= 0)).toBe(
      true,
    );
    expect(projectTrendsBody.filters).toEqual({
      from: "2026-03-04T00:00:00.000Z",
      to: "2026-03-04T23:59:59.999Z",
      metric: "all",
      provider: "github",
      workflow: "ci-main",
      includeUnknown: false,
      limit: 10,
    });

    const tenantBProjectTrendsResponse = await app.request(
      "/api/v2/quality/reports/project-trends?from=2026-03-04&to=2026-03-04&limit=10",
      {
        headers: tenantBHeaders,
      },
    );
    expect(tenantBProjectTrendsResponse.status).toBe(200);
    const tenantBProjectTrendsBody =
      (await tenantBProjectTrendsResponse.json()) as {
        items: Array<{ project: string }>;
      };
    expect(tenantBProjectTrendsBody.items.length).toBe(0);

    const upsertScorecardResponse = await app.request(
      `/api/v2/quality/scorecards/${encodeURIComponent("accuracy")}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          targetScore: 90,
          warningScore: 80,
          criticalScore: 70,
          weight: 1,
          enabled: true,
          updatedAt: "2026-03-04T12:00:00.000Z",
        }),
      },
    );
    expect(upsertScorecardResponse.status).toBe(200);

    const listScorecardsResponse = await app.request(
      "/api/v2/quality/scorecards",
      {
        headers: tenantAHeaders,
      },
    );
    expect(listScorecardsResponse.status).toBe(200);
    const listScorecardsBody = (await listScorecardsResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(
      listScorecardsBody.items.some((item) => item.id === "accuracy"),
    ).toBe(true);

    const listTenantBScorecardsResponse = await app.request(
      "/api/v2/quality/scorecards",
      {
        headers: tenantBHeaders,
      },
    );
    expect(listTenantBScorecardsResponse.status).toBe(200);
    const listTenantBScorecardsBody =
      (await listTenantBScorecardsResponse.json()) as {
        items: Array<{ id: string }>;
      };
    expect(
      listTenantBScorecardsBody.items.some((item) => item.id === "accuracy"),
    ).toBe(false);
  });

  test("api-v2 replay 路由：数据集、运行、差异与工件链路", async () => {
    resetReplayJobExecutionWorkerForTests();
    setReplayJobExecutionHandlerForTests(async ({ replayJob }) => ({
      status: "completed",
      summary: {
        metric: "accuracy",
        totalCases:
          typeof replayJob.summary["totalCases"] === "number"
            ? replayJob.summary["totalCases"]
            : 0,
        processedCases:
          typeof replayJob.summary["totalCases"] === "number"
            ? replayJob.summary["totalCases"]
            : 0,
        improvedCases: 1,
        regressedCases: 0,
        unchangedCases:
          typeof replayJob.summary["totalCases"] === "number"
            ? Math.max(0, replayJob.summary["totalCases"] - 1)
            : 0,
      },
      diff: {
        items: [
          {
            caseId: `case-${replayJob.id}`,
            metric: "accuracy",
            baselineScore: 0.72,
            candidateScore: 0.9,
            delta: 0.18,
            verdict: "improved",
            detail: "accuracy improved",
          },
        ],
      },
    }));
    try {
      const nonce = createNonce("replay-v2-routes");
      const auth = await getDefaultAuthContext();

      const tenantAResult = await createTenantByAuth(
        auth.accessToken,
        {
          name: `ReplayV2 Tenant A ${nonce}`,
          slug: `replay-v2-a-${nonce}`,
        },
        auth.userId,
      );
      assertApiStatus(tenantAResult, [201]);
      const tenantAId = extractEntityId(tenantAResult.payload);
      if (!tenantAId) {
        throw new Error("租户 A 创建失败，缺少 tenantId。");
      }

      const tenantBResult = await createTenantByAuth(
        auth.accessToken,
        {
          name: `ReplayV2 Tenant B ${nonce}`,
          slug: `replay-v2-b-${nonce}`,
        },
        auth.userId,
      );
      assertApiStatus(tenantBResult, [201]);
      const tenantBId = extractEntityId(tenantBResult.payload);
      if (!tenantBId) {
        throw new Error("租户 B 创建失败，缺少 tenantId。");
      }

      const tenantAHeaders = await issueTenantScopedAuthHeaders(
        tenantAId,
        auth.accessToken,
        auth.userId,
      );
      const tenantBHeaders = await issueTenantScopedAuthHeaders(
        tenantBId,
        auth.accessToken,
        auth.userId,
      );

      const createDatasetResponse = await app.request(
        "/api/v2/replay/datasets",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...tenantAHeaders,
          },
          body: JSON.stringify({
            name: `Replay Dataset ${nonce}`,
            datasetRef: `dataset-${nonce}`,
            model: "gpt-4.1",
            promptVersion: "v2",
            sampleCount: 14,
          }),
        },
      );
      expect(createDatasetResponse.status).toBe(201);
      const dataset = (await createDatasetResponse.json()) as {
        id: string;
        tenantId: string;
        datasetId?: string;
        datasetRef?: string | null;
      };
      expect(dataset.tenantId).toBe(tenantAId);
      expect(dataset.datasetId).toBe(dataset.id);
      expect(dataset.datasetRef).toBe(`dataset-${nonce}`);

      const createLegacyDatasetResponse = await app.request(
        "/api/v2/replay/datasets",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...tenantAHeaders,
          },
          body: JSON.stringify({
            name: `Replay Dataset Legacy ${nonce}`,
            datasetId: `legacy-dataset-${nonce}`,
            model: "gpt-4.1",
          }),
        },
      );
      expect(createLegacyDatasetResponse.status).toBe(201);
      const legacyDataset = (await createLegacyDatasetResponse.json()) as {
        datasetRef?: string | null;
      };
      expect(legacyDataset.datasetRef).toBe(`legacy-dataset-${nonce}`);

      const listDatasetsResponse = await app.request(
        "/api/v2/replay/datasets?limit=20",
        {
          headers: tenantAHeaders,
        },
      );
      expect(listDatasetsResponse.status).toBe(200);
      const listDatasetsBody = (await listDatasetsResponse.json()) as {
        items: Array<{ id: string }>;
      };
      expect(
        listDatasetsBody.items.some((item) => item.id === dataset.id),
      ).toBe(true);

      const replaceCasesResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/cases`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...tenantAHeaders,
          },
          body: JSON.stringify({
            items: [
              {
                caseId: "case-1",
                input: "用户询问发票流程",
                expectedOutput: "说明发票申请步骤",
              },
              {
                caseId: "case-2",
                input: "用户询问退款时效",
                baselineOutput: "原方案说明 3 个工作日",
                candidateInput: "候选方案说明 2 个工作日",
              },
            ],
          }),
        },
      );
      expect(replaceCasesResponse.status).toBe(200);
      const replaceCasesBody = (await replaceCasesResponse.json()) as {
        total: number;
      };
      expect(replaceCasesBody.total).toBe(2);

      const listCasesResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/cases?limit=10`,
        {
          headers: tenantAHeaders,
        },
      );
      expect(listCasesResponse.status).toBe(200);
      const listCasesBody = (await listCasesResponse.json()) as {
        items: Array<{ caseId: string }>;
      };
      expect(listCasesBody.items.some((item) => item.caseId === "case-1")).toBe(
        true,
      );

      const createRunResponse = await app.request("/api/v2/replay/runs", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          datasetId: dataset.id,
          candidateLabel: "candidate-v2",
          sampleLimit: 14,
        }),
      });
      expect(createRunResponse.status).toBe(201);
      const run = (await createRunResponse.json()) as {
        id: string;
        status: string;
        totalCases: number;
        datasetId: string;
        baselineId?: string;
      };
      expect(run.status).toBe("pending");
      expect(run.totalCases).toBe(2);
      expect(run.datasetId).toBe(dataset.id);
      expect(run.baselineId).toBe(dataset.id);

      const createAliasRunResponse = await app.request("/api/v2/replay/runs", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          baselineId: dataset.id,
          candidateLabel: "candidate-v2-legacy",
          sampleLimit: 2,
        }),
      });
      expect(createAliasRunResponse.status).toBe(201);
      const aliasRun = (await createAliasRunResponse.json()) as {
        id: string;
        datasetId: string;
        baselineId?: string;
      };
      expect(aliasRun.datasetId).toBe(dataset.id);
      expect(aliasRun.baselineId).toBe(dataset.id);

      await flushReplayJobExecutionQueueForTests();

      const getRunResponse = await app.request(
        `/api/v2/replay/runs/${encodeURIComponent(run.id)}`,
        {
          headers: tenantAHeaders,
        },
      );
      expect(getRunResponse.status).toBe(200);
      const getRunBody = (await getRunResponse.json()) as {
        status: string;
        processedCases: number;
        totalCases: number;
      };
      expect(getRunBody.status).toBe("completed");
      expect(getRunBody.processedCases).toBe(getRunBody.totalCases);

      const diffResponse = await app.request(
        `/api/v2/replay/runs/${encodeURIComponent(run.id)}/diffs?datasetId=${encodeURIComponent(
          dataset.id,
        )}&keyword=accuracy&limit=1`,
        {
          headers: tenantAHeaders,
        },
      );
      expect(diffResponse.status).toBe(200);
      const diffBody = (await diffResponse.json()) as {
        runId: string;
        jobId?: string;
        total: number;
        summary: {
          totalCases: number;
          executionSource?: string;
        };
        filters: {
          datasetId: string;
          baselineId?: string;
          runId: string;
          jobId?: string;
          keyword: string | null;
          limit: number | null;
        };
        diffs: Array<{ metric: string }>;
      };
      expect(diffBody.runId).toBe(run.id);
      expect(diffBody.jobId).toBe(run.id);
      expect(diffBody.total).toBe(1);
      expect(diffBody.summary.totalCases).toBe(2);
      expect(diffBody.summary.executionSource).toBe("dataset_cases");
      expect(diffBody.filters).toEqual({
        datasetId: dataset.id,
        baselineId: dataset.id,
        runId: run.id,
        jobId: run.id,
        keyword: "accuracy",
        limit: 1,
      });
      expect(diffBody.diffs).toHaveLength(1);
      expect(diffBody.diffs[0]?.metric).toBe("accuracy");

      const diffAliasResponse = await app.request(
        `/api/v2/replay/runs/${encodeURIComponent(run.id)}/diffs?baselineId=${encodeURIComponent(
          dataset.id,
        )}`,
        {
          headers: tenantAHeaders,
        },
      );
      expect(diffAliasResponse.status).toBe(200);

      const diffMismatchResponse = await app.request(
        `/api/v2/replay/runs/${encodeURIComponent(run.id)}/diffs?datasetId=wrong-baseline`,
        {
          headers: tenantAHeaders,
        },
      );
      expect(diffMismatchResponse.status).toBe(400);

      const artifactsResponse = await app.request(
        `/api/v2/replay/runs/${encodeURIComponent(run.id)}/artifacts`,
        {
          headers: tenantAHeaders,
        },
      );
      expect(artifactsResponse.status).toBe(200);
      const artifactsBody = (await artifactsResponse.json()) as {
        runId: string;
        jobId?: string;
        datasetId: string;
        total: number;
        items: Array<{
          type: string;
          name?: string;
          byteSize?: number;
          downloadName?: string;
          downloadUrl?: string;
          inline?: Record<string, unknown>;
        }>;
      };
      expect(artifactsBody.runId).toBe(run.id);
      expect(artifactsBody.jobId).toBe(run.id);
      expect(artifactsBody.datasetId).toBe(dataset.id);
      expect(artifactsBody.total).toBe(3);
      expect(artifactsBody.items.some((item) => item.type === "summary")).toBe(
        true,
      );
      expect(artifactsBody.items.some((item) => item.type === "diff")).toBe(
        true,
      );
      expect(artifactsBody.items.some((item) => item.type === "cases")).toBe(
        true,
      );
      expect(
        artifactsBody.items.find((item) => item.type === "summary")?.inline?.[
          "totalCases"
        ],
      ).toBe(2);
      expect(
        artifactsBody.items.every(
          (item) =>
            typeof item.name === "string" &&
            typeof item.downloadName === "string" &&
            typeof item.downloadUrl === "string" &&
            typeof item.byteSize === "number" &&
            item.byteSize >= 0,
        ),
      ).toBe(true);
      const summaryDownloadResponse = await app.request(
        `/api/v2/replay/runs/${encodeURIComponent(run.id)}/artifacts/summary/download`,
        {
          headers: tenantAHeaders,
        },
      );
      expect(summaryDownloadResponse.status).toBe(200);
      expect(summaryDownloadResponse.headers.get("content-type")).toContain(
        "application/json",
      );
      expect(
        summaryDownloadResponse.headers.get("content-disposition"),
      ).toContain("summary.json");
      const summaryDownloadBody = (await summaryDownloadResponse.json()) as {
        totalCases?: number;
        digest?: Record<string, unknown>;
      };
      expect(summaryDownloadBody.totalCases).toBe(2);
      expect(summaryDownloadBody.digest?.["runId"]).toBe(run.id);

      const invalidArtifactTypeResponse = await app.request(
        `/api/v2/replay/runs/${encodeURIComponent(run.id)}/artifacts/unknown/download`,
        {
          headers: tenantAHeaders,
        },
      );
      expect(invalidArtifactTypeResponse.status).toBe(400);

      const listTenantBRunsResponse = await app.request("/api/v2/replay/runs", {
        headers: tenantBHeaders,
      });
      expect(listTenantBRunsResponse.status).toBe(200);
      const listTenantBRunsBody = (await listTenantBRunsResponse.json()) as {
        items: Array<{ id: string }>;
      };
      expect(listTenantBRunsBody.items.some((item) => item.id === run.id)).toBe(
        false,
      );
    } finally {
      resetReplayJobExecutionWorkerForTests();
    }
  });

  test("api-v2 residency 路由：策略、区域映射、复制审批取消与租户隔离", async () => {
    const nonce = createNonce("residency-v2-routes");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `ResidencyV2 Tenant A ${nonce}`,
        slug: `residency-v2-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建失败，缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `ResidencyV2 Tenant B ${nonce}`,
        slug: `residency-v2-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建失败，缺少 tenantId。");
    }

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      auth.accessToken,
      auth.userId,
    );
    const tenantBHeaders = await issueTenantScopedAuthHeaders(
      tenantBId,
      auth.accessToken,
      auth.userId,
    );

    const upsertPolicyResponse = await app.request(
      "/api/v2/residency/policies/current",
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          mode: "active_active",
          primaryRegion: "cn-hangzhou",
          replicaRegions: ["cn-shanghai", "ap-southeast-1"],
          allowCrossRegionTransfer: true,
          requireTransferApproval: true,
        }),
      },
    );
    expect(upsertPolicyResponse.status).toBe(200);

    const getPolicyResponse = await app.request(
      "/api/v2/residency/policies/current",
      {
        headers: tenantAHeaders,
      },
    );
    expect(getPolicyResponse.status).toBe(200);
    const getPolicyBody = (await getPolicyResponse.json()) as {
      tenantId: string;
      mode: string;
      replicaRegions: string[];
    };
    expect(getPolicyBody.tenantId).toBe(tenantAId);
    expect(getPolicyBody.mode).toBe("active_active");
    expect(getPolicyBody.replicaRegions).toContain("cn-shanghai");

    const mappingsResponse = await app.request(
      "/api/v2/residency/region-mappings",
      {
        headers: tenantAHeaders,
      },
    );
    expect(mappingsResponse.status).toBe(200);
    const mappingsBody = (await mappingsResponse.json()) as {
      items: Array<{ regionId: string; role: string }>;
    };
    expect(
      mappingsBody.items.some(
        (item) => item.regionId === "cn-hangzhou" && item.role === "primary",
      ),
    ).toBe(true);
    expect(
      mappingsBody.items.some(
        (item) => item.regionId === "cn-shanghai" && item.role === "replica",
      ),
    ).toBe(true);

    const createReplicationResponse = await app.request(
      "/api/v2/residency/replications",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          sourceRegion: "cn-hangzhou",
          targetRegion: "cn-shanghai",
          reason: "v2 route integration",
        }),
      },
    );
    expect(createReplicationResponse.status).toBe(201);
    const replication = (await createReplicationResponse.json()) as {
      id: string;
      tenantId: string;
      status: string;
    };
    expect(replication.tenantId).toBe(tenantAId);
    expect(replication.status).toBe("pending");

    const approveReplicationResponse = await app.request(
      `/api/v2/residency/replications/${encodeURIComponent(replication.id)}/approvals`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          reason: "approve v2",
        }),
      },
    );
    expect(approveReplicationResponse.status).toBe(200);
    const approvedReplication = (await approveReplicationResponse.json()) as {
      status: string;
    };
    expect(approvedReplication.status).toBe("running");

    const cancelReplicationResponse = await app.request(
      `/api/v2/residency/replications/${encodeURIComponent(replication.id)}/cancel`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          reason: "cancel v2",
        }),
      },
    );
    expect(cancelReplicationResponse.status).toBe(200);
    const cancelledReplication = (await cancelReplicationResponse.json()) as {
      status: string;
    };
    expect(cancelledReplication.status).toBe("cancelled");

    const listTenantAResponse = await app.request(
      "/api/v2/residency/replications",
      {
        headers: tenantAHeaders,
      },
    );
    expect(listTenantAResponse.status).toBe(200);
    const listTenantABody = (await listTenantAResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(
      listTenantABody.items.some((item) => item.id === replication.id),
    ).toBe(true);

    const listTenantBResponse = await app.request(
      "/api/v2/residency/replications",
      {
        headers: tenantBHeaders,
      },
    );
    expect(listTenantBResponse.status).toBe(200);
    const listTenantBBody = (await listTenantBResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(
      listTenantBBody.items.some((item) => item.id === replication.id),
    ).toBe(false);
  });

  test("api-v2 replay 支持从历史会话物化样本并产出真实执行摘要", async () => {
    resetReplayJobExecutionWorkerForTests();
    const nonce = createNonce("replay-v2-materialize");
    const auth = await getDefaultAuthContext();

    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Replay Materialize Tenant ${nonce}`,
        slug: `replay-materialize-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("租户创建失败，缺少 tenantId。");
    }

    const headers = await issueTenantScopedAuthHeaders(
      tenantId,
      auth.accessToken,
      auth.userId,
    );
    const sourceResult = await createIdentitySourceByAuth(
      auth.accessToken,
      {
        tenantId,
        name: `Replay Materialize Source ${nonce}`,
        location: `/tmp/replay-materialize-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(sourceResult, [201]);
    const sourceId = extractEntityId(sourceResult.payload);
    if (!sourceId) {
      throw new Error("来源创建失败，缺少 sourceId。");
    }

    const insertedA = await insertSessionForSearch(sourceId, {
      provider: "codex",
      tool: "Codex CLI",
      model: "gpt-5-codex",
      startedAt: "2026-03-04T08:00:00.000Z",
      eventTexts: ["用户询问预算超额怎么办", "助手建议设置月度预算阈值与告警"],
    });
    const insertedB = await insertSessionForSearch(sourceId, {
      provider: "codex",
      tool: "Codex CLI",
      model: "gpt-5-codex",
      startedAt: "2026-03-05T08:00:00.000Z",
      eventTexts: ["用户询问审计日志怎么导出", "助手建议在审计中心导出取证包"],
    });

    try {
      const createDatasetResponse = await app.request(
        "/api/v2/replay/datasets",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            name: `Replay Materialize Dataset ${nonce}`,
            datasetRef: `replay-materialize-dataset-${nonce}`,
            model: "gpt-5-codex",
          }),
        },
      );
      expect(createDatasetResponse.status).toBe(201);
      const dataset = (await createDatasetResponse.json()) as { id: string };

      const materializeResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/materialize`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            sessionIds: [insertedA.id, insertedB.id],
            sampleLimit: 10,
          }),
        },
      );
      expect(materializeResponse.status).toBe(200);
      const materializeBody = (await materializeResponse.json()) as {
        materialized: number;
        skipped: number;
        sourceSummary?: Record<string, number>;
        items: Array<{ metadata: Record<string, unknown> }>;
      };
      expect(materializeBody.materialized).toBe(2);
      expect(materializeBody.skipped).toBe(0);
      expect(materializeBody.sourceSummary?.["session"]).toBe(2);
      expect(
        materializeBody.items.every(
          (item) => item.metadata["sourceType"] === "session",
        ),
      ).toBe(true);

      const createRunResponse = await app.request("/api/v2/replay/runs", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          datasetId: dataset.id,
          candidateLabel: "candidate-materialized",
          sampleLimit: 10,
        }),
      });
      expect(createRunResponse.status).toBe(201);
      const replayRun = (await createRunResponse.json()) as {
        id: string;
        totalCases: number;
      };
      expect(replayRun.totalCases).toBe(2);

      await flushReplayJobExecutionQueueForTests();

      const getRunResponse = await app.request(
        `/api/v2/replay/runs/${encodeURIComponent(replayRun.id)}`,
        {
          headers,
        },
      );
      expect(getRunResponse.status).toBe(200);
      const getRunBody = (await getRunResponse.json()) as {
        status: string;
        summary: {
          executionSource?: string;
          sourceSummary?: Record<string, number>;
          digest?: Record<string, unknown>;
        };
      };
      expect(getRunBody.status).toBe("completed");
      expect(getRunBody.summary.executionSource).toBe("session_materialized");
      expect(getRunBody.summary.sourceSummary?.["session"]).toBe(2);
      expect(getRunBody.summary.digest?.["runId"]).toBe(replayRun.id);

      const diffResponse = await app.request(
        `/api/v2/replay/runs/${encodeURIComponent(replayRun.id)}/diffs`,
        {
          headers,
        },
      );
      expect(diffResponse.status).toBe(200);
      const diffBody = (await diffResponse.json()) as {
        summary: {
          executionSource?: string;
          digest?: Record<string, unknown>;
        };
      };
      expect(diffBody.summary.executionSource).toBe("session_materialized");
      expect(diffBody.summary.digest?.["executionSource"]).toBe(
        "session_materialized",
      );
    } finally {
      await insertedA.cleanup();
      await insertedB.cleanup();
      resetReplayJobExecutionWorkerForTests();
    }
  });

  test("replay worker 失败流转：pending -> running -> failed", async () => {
    resetReplayJobExecutionWorkerForTests();
    setReplayJobExecutionHandlerForTests(async () => {
      throw new Error("mock replay worker failure");
    });

    try {
      const nonce = createNonce("replay-worker-failed");
      const auth = await getDefaultAuthContext();
      const tenantResult = await createTenantByAuth(
        auth.accessToken,
        {
          name: `Replay Worker Tenant ${nonce}`,
          slug: `replay-worker-${nonce}`,
        },
        auth.userId,
      );
      assertApiStatus(tenantResult, [201]);
      const tenantId = extractEntityId(tenantResult.payload);
      if (!tenantId) {
        throw new Error("租户创建失败，缺少 tenantId。");
      }

      const headers = await issueTenantScopedAuthHeaders(
        tenantId,
        auth.accessToken,
        auth.userId,
      );

      const createBaselineResponse = await app.request(
        "/api/v1/replay/baselines",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            name: `Worker Failed Baseline ${nonce}`,
            datasetId: "worker-failed-dataset",
            model: "gpt-4.1",
            sampleCount: 6,
          }),
        },
      );
      expect(createBaselineResponse.status).toBe(201);
      const baseline = (await createBaselineResponse.json()) as { id: string };

      const createJobResponse = await app.request("/api/v1/replay/jobs", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          baselineId: baseline.id,
          candidateLabel: "failing-candidate",
          sampleLimit: 6,
        }),
      });
      expect(createJobResponse.status).toBe(201);
      const replayJob = (await createJobResponse.json()) as {
        id: string;
        status: string;
      };
      expect(replayJob.status).toBe("pending");

      await flushReplayJobExecutionQueueForTests();

      const getJobResponse = await app.request(
        `/api/v1/replay/jobs/${encodeURIComponent(replayJob.id)}`,
        {
          headers,
        },
      );
      expect(getJobResponse.status).toBe(200);
      const getJobBody = (await getJobResponse.json()) as {
        id: string;
        status: string;
        error?: string;
      };
      expect(getJobBody.id).toBe(replayJob.id);
      expect(getJobBody.status).toBe("failed");
      expect(getJobBody.error).toContain("mock replay worker failure");

      const failedListResponse = await app.request(
        "/api/v1/replay/jobs?status=failed",
        {
          headers,
        },
      );
      expect(failedListResponse.status).toBe(200);
      const failedListBody = (await failedListResponse.json()) as {
        items: Array<{ id: string; status: string }>;
      };
      expect(
        failedListBody.items.some(
          (item) => item.id === replayJob.id && item.status === "failed",
        ),
      ).toBe(true);
    } finally {
      resetReplayJobExecutionWorkerForTests();
    }
  });
});
