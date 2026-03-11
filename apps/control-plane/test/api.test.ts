import { afterAll, describe, expect, test } from "bun:test";
import { createHash, createHmac } from "node:crypto";
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
import {
  __resetAlertExternalStatusSyncPublisherForTests,
  __setAlertExternalStatusSyncPublisherForTests,
} from "../src/routes/integration-event-publisher";
import {
  __drainIntegrationDlqRecoveryQueueForTests,
  __resetIntegrationDlqBackendForTests,
  __setIntegrationDlqBackendForTests,
} from "../src/routes/integration-dlq";
import {
  computeTokenPulseRuntimeIdempotencyKey,
  computeTokenPulseRuntimeSignature,
  TOKENPULSE_RUNTIME_DEFAULT_KEY_ID,
  TOKENPULSE_RUNTIME_SPEC_VERSION,
} from "../src/routes/tokenpulse-runtime-signature";
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
  await __resetIntegrationDlqBackendForTests();
  await __resetAlertExternalStatusSyncPublisherForTests();
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
      id: string;
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
        id: string;
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
      id: string;
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
        id: string;
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

  function buildTokenPulseRuntimeSignedRequest(
    secret: string,
    payload: Record<string, unknown>,
    options: {
      specVersion?: string;
      keyId?: string;
      timestamp?: string;
      idempotencyKey?: string;
      signature?: string;
    } = {},
  ): {
    init: RequestInit;
    specVersion: string;
    keyId: string;
    timestamp: string;
    idempotencyKey: string;
    signature: string;
  } {
    const specVersion = options.specVersion ?? TOKENPULSE_RUNTIME_SPEC_VERSION;
    const keyId = options.keyId ?? TOKENPULSE_RUNTIME_DEFAULT_KEY_ID;
    const timestamp = options.timestamp ?? String(Math.floor(Date.now() / 1000));
    const idempotencyKey =
      options.idempotencyKey ??
      computeTokenPulseRuntimeIdempotencyKey({
        tenantId: String(payload.tenantId ?? ""),
        traceId: String(payload.traceId ?? ""),
        provider: String(payload.provider ?? ""),
        model: String(payload.model ?? ""),
        startedAt: String(payload.startedAt ?? ""),
      });
    const body = JSON.stringify(payload);
    const signatureHex = computeTokenPulseRuntimeSignature(secret, {
        specVersion,
        keyId,
        timestamp,
        idempotencyKey,
        rawBody: body,
      });
    const signature = options.signature ?? `sha256=${signatureHex}`;

    return {
      init: {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-tokenpulse-spec-version": specVersion,
          "x-tokenpulse-key-id": keyId,
          "x-tokenpulse-timestamp": timestamp,
          "x-tokenpulse-idempotency-key": idempotencyKey,
          "x-tokenpulse-signature": signature,
        },
        body,
      },
      specVersion,
      keyId,
      timestamp,
      idempotencyKey,
      signature,
    };
  }

  async function postTokenPulseRuntimeEvent(
    secret: string,
    payload: Record<string, unknown>,
    options: {
      specVersion?: string;
      keyId?: string;
      timestamp?: string;
      idempotencyKey?: string;
      signature?: string;
    } = {},
  ): Promise<Response> {
    const request = buildTokenPulseRuntimeSignedRequest(secret, payload, options);
    return app.request("/api/v1/integrations/tokenpulse/runtime-events", request.init);
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
        id: "corp-saml",
        type: "saml",
        displayName: "企业 SAML",
        metadataUrl: "https://idp.example.com/metadata",
        ssoUrl: "https://idp.example.com/sso",
        acsUrl: "https://console.example.com/api/v1/auth/saml/acs",
        binding: "http-post",
      },
      {
        id: "corp-saml-invalid",
        type: "saml",
        displayName: "缺少发现入口的 SAML",
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
      expect(providerIds).toEqual(["github", "corp-oidc", "corp-saml"]);
      expect(result.payload.total).toBe(3);
      const samlProvider = providers.find(
        (item) => pickString(item, ["id"]) === "corp-saml",
      );
      expect(samlProvider).toBeDefined();
      expect(pickString(samlProvider, ["authorizationUrl"])).toBe(
        "https://idp.example.com/sso",
      );
      expect(pickString(samlProvider, ["metadataUrl"])).toBe(
        "https://idp.example.com/metadata",
      );
      expect(pickString(samlProvider, ["ssoUrl"])).toBe(
        "https://idp.example.com/sso",
      );
      expect(pickString(samlProvider, ["acsUrl"])).toBe(
        "https://console.example.com/api/v1/auth/saml/acs",
      );
      expect(pickString(samlProvider, ["binding"])).toBe("post");
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

  test("本地登录在启用 MFA 时要求 otpCode，provider discovery 支持 saml/requireMfa", async () => {
    const nonce = createNonce("auth-local-mfa");
    const originalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    const originalLocalMfaRequired = Bun.env.AUTH_LOCAL_MFA_REQUIRED;
    const originalLocalMfaCode = Bun.env.AUTH_LOCAL_MFA_STATIC_CODE;
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: "corp-saml",
        type: "saml",
        displayName: "企业 SAML",
        metadataUrl: "https://idp.example.com/metadata",
        ssoUrl: "https://idp.example.com/sso",
        enabled: true,
        requireMfa: true,
      },
    ]);
    Bun.env.AUTH_LOCAL_MFA_REQUIRED = "true";
    Bun.env.AUTH_LOCAL_MFA_STATIC_CODE = "246810";

    try {
      const registerResult = await registerLocalUser({
        email: `local-mfa-${nonce}@example.com`,
        password: `unit-test-pw-${nonce}`,
        displayName: `本地MFA-${nonce}`,
      });
      expect(registerResult.response.status).toBe(201);

      const providersResult = await requestFirstAvailable([{ path: "/api/v1/auth/providers" }]);
      expect(providersResult.response.status).toBe(200);
      if (!isRecord(providersResult.payload) || !Array.isArray(providersResult.payload.items)) {
        throw new Error("auth/providers 返回结构异常。");
      }
      const samlProvider = providersResult.payload.items.find(
        (item) => isRecord(item) && item.id === "corp-saml",
      ) as Record<string, unknown> | undefined;
      expect(samlProvider?.type).toBe("saml");
      expect(samlProvider?.requireMfa).toBe(true);

      const missingOtpResponse = await app.request("/api/v1/auth/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          email: `local-mfa-${nonce}@example.com`,
          password: `unit-test-pw-${nonce}`,
        }),
      });
      expect(missingOtpResponse.status).toBe(401);

      const successResponse = await app.request("/api/v1/auth/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          email: `local-mfa-${nonce}@example.com`,
          password: `unit-test-pw-${nonce}`,
          otpCode: "246810",
        }),
      });
      expect(successResponse.status).toBe(200);
    } finally {
      if (originalProviders === undefined) {
        delete Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
      } else {
        Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = originalProviders;
      }
      if (originalLocalMfaRequired === undefined) {
        delete Bun.env.AUTH_LOCAL_MFA_REQUIRED;
      } else {
        Bun.env.AUTH_LOCAL_MFA_REQUIRED = originalLocalMfaRequired;
      }
      if (originalLocalMfaCode === undefined) {
        delete Bun.env.AUTH_LOCAL_MFA_STATIC_CODE;
      } else {
        Bun.env.AUTH_LOCAL_MFA_STATIC_CODE = originalLocalMfaCode;
      }
    }
  });

  test("本地 MFA 启用但缺少静态码时返回 403 且错误可辨", async () => {
    const nonce = createNonce("auth-local-mfa-static-missing");
    const originalLocalMfaRequired = Bun.env.AUTH_LOCAL_MFA_REQUIRED;
    const originalLocalMfaCode = Bun.env.AUTH_LOCAL_MFA_STATIC_CODE;
    Bun.env.AUTH_LOCAL_MFA_REQUIRED = "true";
    delete Bun.env.AUTH_LOCAL_MFA_STATIC_CODE;

    try {
      const registerResult = await registerLocalUser({
        email: `local-mfa-static-missing-${nonce}@example.com`,
        password: `unit-test-pw-${nonce}`,
        displayName: `本地MFA静态码缺失-${nonce}`,
      });
      expect(registerResult.response.status).toBe(201);

      const response = await app.request("/api/v1/auth/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          email: `local-mfa-static-missing-${nonce}@example.com`,
          password: `unit-test-pw-${nonce}`,
          otpCode: "000000",
        }),
      });
      expect(response.status).toBe(403);
      const body = await readResponseAsUnknown(response);
      if (isRecord(body)) {
        expect(pickString(body, ["message"])).toContain("未配置静态验证码");
      }
    } finally {
      if (originalLocalMfaRequired === undefined) {
        delete Bun.env.AUTH_LOCAL_MFA_REQUIRED;
      } else {
        Bun.env.AUTH_LOCAL_MFA_REQUIRED = originalLocalMfaRequired;
      }
      if (originalLocalMfaCode === undefined) {
        delete Bun.env.AUTH_LOCAL_MFA_STATIC_CODE;
      } else {
        Bun.env.AUTH_LOCAL_MFA_STATIC_CODE = originalLocalMfaCode;
      }
    }
  });

  test("POST /api/v1/auth/external/login 支持 SAML provider 断言登录", async () => {
    const nonce = createNonce("auth-external-login-saml");
    const secret = `external-secret-${nonce}`;
    const originalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    const originalSecret = Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET;
    const originalTTL = Bun.env.AUTH_EXTERNAL_ASSERTION_TTL_SECONDS;
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: "corp-saml",
        type: "saml",
        displayName: "企业 SAML",
        metadataUrl: "https://idp.example.com/metadata",
        ssoUrl: "https://idp.example.com/sso",
        binding: "post",
        enabled: true,
      },
    ]);
    Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET = secret;
    Bun.env.AUTH_EXTERNAL_ASSERTION_TTL_SECONDS = "300";

    try {
      const payload = {
        providerId: "corp-saml",
        externalUserId: `saml-user-${nonce}`,
        email: `saml-${nonce}@example.com`,
        displayName: `SAML 用户-${nonce}`,
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
      payload.signature = createHmac("sha256", secret).update(canonical).digest("hex");

      const response = await app.request("/api/v1/auth/external/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify(payload),
      });
      const body = (await response.json()) as {
        provider?: {
          id?: string;
          type?: string;
        };
        user?: {
          email?: string;
        };
      };

      expect(response.status).toBe(200);
      expect(body.provider?.id).toBe("corp-saml");
      expect(body.provider?.type).toBe("saml");
      expect(body.user?.email).toBe(payload.email);
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

  test("POST /api/v1/auth/external/login 支持外部断言登录与签发会话", async () => {
    const nonce = createNonce("auth-external-login");
    const secret = `external-secret-${nonce}`;
    const clientIp = "203.0.113.10";
    const userAgent = "external-login-test/1.0";
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
          "x-forwarded-for": clientIp,
          "user-agent": userAgent,
        },
        body: JSON.stringify(payload),
      });
      const body = (await response.json()) as {
        user?: {
          userId?: string;
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
        riskDecision?: string;
      };

      expect(response.status).toBe(200);
      expect(body.provider?.id).toBe("corp-oidc");
      expect(body.provider?.type).toBe("oidc");
      expect(body.user?.email).toBe(payload.email);
      expect(typeof body.user?.userId).toBe("string");
      expect(typeof body.tokens?.accessToken).toBe("string");
      expect(typeof body.tokens?.refreshToken).toBe("string");
      expect(body.riskDecision).toBe("allowed");

      const audits = await queryAuditByAction(
        "auth.external_login",
        "/api/v1/auth/external/login",
        body.tokens?.accessToken ?? "",
        body.user?.userId ?? "",
      );
      const matched = audits.items.find(
        (item) =>
          item.action === "auth.external_login" &&
          item.metadata.route === "/api/v1/auth/external/login" &&
          item.metadata.providerId === "corp-oidc" &&
          item.metadata.providerType === "oidc" &&
          item.metadata.clientIp === clientIp &&
          item.metadata.userAgent === userAgent &&
          item.metadata.riskLevel === "low" &&
          Array.isArray(item.metadata.riskSignals) &&
          item.metadata.riskSignals.includes("assertion_login_succeeded") &&
          item.metadata.riskSignals.includes("signature_verified"),
      );
      expect(matched).toBeDefined();
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

  test(
    "POST /api/v1/auth/external/login 签名错误、时间窗异常与重放请求返回 401 并写入失败审计",
    async () => {
    const nonce = createNonce("auth-external-login-fail");
    const secret = `external-secret-${nonce}`;
    const clientIp = "198.51.100.22";
    const userAgent = "external-login-fail-test/1.0";
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
            "x-forwarded-for": clientIp,
            "user-agent": userAgent,
          },
          body: JSON.stringify({
            ...basePayload,
            signature: tamperedSignature,
          }),
        },
      );
      expect(badSignatureResponse.status).toBe(401);
      const badSignatureBody = (await badSignatureResponse.json()) as {
        riskDecision?: string;
      };
      expect(badSignatureBody.riskDecision).toBe("allow_with_risk");

      const expiredPayload = {
        ...basePayload,
        timestamp: new Date(Date.now() - 10 * 60 * 1000).toISOString(),
        nonce: `nonce-${nonce}-expired`,
      };
      const expiredCanonical = [
        expiredPayload.providerId,
        expiredPayload.externalUserId,
        expiredPayload.email,
        "",
        expiredPayload.timestamp,
        expiredPayload.nonce,
      ].join("\n");
      const expiredSignature = createHmac("sha256", secret)
        .update(expiredCanonical)
        .digest("hex");
      const expiredResponse = await app.request("/api/v1/auth/external/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-forwarded-for": clientIp,
          "user-agent": userAgent,
        },
        body: JSON.stringify({
          ...expiredPayload,
          signature: expiredSignature,
        }),
      });
      expect(expiredResponse.status).toBe(401);

      const firstResponse = await app.request("/api/v1/auth/external/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-forwarded-for": clientIp,
          "user-agent": userAgent,
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
          "x-forwarded-for": clientIp,
          "user-agent": userAgent,
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

      const signatureAudit = audits.items.find(
        (item) =>
          item.metadata.reason === "外部登录签名校验失败。" &&
          item.metadata.providerId === "corp-oidc" &&
          item.metadata.providerType === "oidc" &&
          item.metadata.clientIp === clientIp &&
          item.metadata.userAgent === userAgent &&
          item.metadata.riskLevel === "high" &&
          item.metadata.failureStage === "signature_validation" &&
          Array.isArray(item.metadata.riskSignals) &&
          item.metadata.riskSignals.includes("signature_invalid"),
      );
      expect(signatureAudit).toBeDefined();

      const expiredAudit = audits.items.find(
        (item) =>
          item.metadata.reason === "外部登录断言已过期。" &&
          item.metadata.riskLevel === "high" &&
          item.metadata.failureStage === "timestamp_validation" &&
          Array.isArray(item.metadata.riskSignals) &&
          item.metadata.riskSignals.includes("timestamp_out_of_window"),
      );
      expect(expiredAudit).toBeDefined();

      const replayAudit = audits.items.find(
        (item) =>
          item.metadata.reason === "外部登录请求疑似重放。" &&
          item.metadata.riskLevel === "high" &&
          item.metadata.failureStage === "nonce_validation" &&
          Array.isArray(item.metadata.riskSignals) &&
          item.metadata.riskSignals.includes("nonce_replay_detected"),
      );
      expect(replayAudit).toBeDefined();
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
    const clientIp = "203.0.113.33";
    const userAgent = "external-exchange-test/1.0";
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
          "x-forwarded-for": clientIp,
          "user-agent": userAgent,
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
      expect(pickString(body.user, ["userId"])).toBeTruthy();
      expect(typeof pickString(body.tokens, ["accessToken"])).toBe("string");
      expect(typeof pickString(body.tokens, ["refreshToken"])).toBe("string");
      expect(pickString(body, ["riskDecision"])).toBe("allowed");

      const audits = await queryAuditByAction(
        "auth.external_exchange",
        "/api/v1/auth/external/exchange",
        pickString(body.tokens, ["accessToken"]) ?? "",
        pickString(body.user, ["userId"]) ?? "",
      );
      const matched = audits.items.find(
        (item) =>
          item.action === "auth.external_exchange" &&
          item.metadata.route === "/api/v1/auth/external/exchange" &&
          item.metadata.providerId === providerId &&
          item.metadata.providerType === "oidc" &&
          item.metadata.clientIp === clientIp &&
          item.metadata.userAgent === userAgent &&
          item.metadata.riskLevel === "low" &&
          Array.isArray(item.metadata.riskSignals) &&
          item.metadata.riskSignals.includes(
            "authorization_code_exchange_succeeded",
          ) &&
          item.metadata.riskSignals.includes("upstream_token_succeeded") &&
          item.metadata.riskSignals.includes("upstream_userinfo_succeeded"),
      );
      expect(matched).toBeDefined();
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
    const clientIp = "198.51.100.44";
    const userAgent = "external-exchange-provider-fail-test/1.0";
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
            "x-forwarded-for": clientIp,
            "user-agent": userAgent,
          },
          body: JSON.stringify({
            providerId: "corp-oidc",
            code: `authorization-code-${nonce}-disabled`,
            redirectUri: `https://console.example.com/callback/${nonce}/disabled`,
          }),
        },
      );
      const disabledProviderBody = (await disabledProviderResponse.json()) as {
        riskDecision?: string;
      };
      const missingProviderResponse = await app.request(
        "/api/v1/auth/external/exchange",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-forwarded-for": clientIp,
            "user-agent": userAgent,
          },
          body: JSON.stringify({
            providerId: "missing-provider",
            code: `authorization-code-${nonce}-missing`,
            redirectUri: `https://console.example.com/callback/${nonce}/missing`,
          }),
        },
      );

      expect(disabledProviderResponse.status).toBe(401);
      expect(disabledProviderBody.riskDecision).toBe("allow_with_risk");
      expect(missingProviderResponse.status).toBe(401);
      expect(upstreamCalls).toBe(0);

      const auth = await getDefaultAuthContext();
      const audits = await queryAuditByAction(
        "auth.external_exchange_failed",
        "/api/v1/auth/external/exchange",
        auth.accessToken,
        auth.userId,
      );
      const disabledProviderAudit = audits.items.find(
        (item) =>
          item.metadata.reason === "外部登录提供方不可用或未启用授权码交换。" &&
          item.metadata.providerId === "corp-oidc" &&
          item.metadata.providerType === "oidc" &&
          item.metadata.clientIp === clientIp &&
          item.metadata.userAgent === userAgent &&
          item.metadata.riskLevel === "medium" &&
          item.metadata.failureStage === "provider_resolution" &&
          Array.isArray(item.metadata.riskSignals) &&
          item.metadata.riskSignals.includes("provider_unavailable"),
      );
      expect(disabledProviderAudit).toBeDefined();

      const missingProviderAudit = audits.items.find(
        (item) =>
          item.metadata.reason === "外部登录提供方不可用或未启用授权码交换。" &&
          item.metadata.providerId === "missing-provider" &&
          item.metadata.providerType === "unknown",
      );
      expect(missingProviderAudit).toBeDefined();
    } finally {
      if (originalProviders === undefined) {
        delete Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
      } else {
        Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = originalProviders;
      }
      globalThis.fetch = originalFetch;
    }
  });

  test("external exchange 在 requireMfa 缺少 mfaVerified 时返回 401 并阻断", async () => {
    const nonce = createNonce("auth-external-exchange-mfa-missing");
    const providerId = `corp-oidc-${nonce}`;
    const clientIp = "203.0.113.77";
    const userAgent = "external-exchange-mfa-missing-test/1.0";
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
        requireMfa: true,
        tokenEndpoint,
        userinfoEndpoint,
        clientId: `client-${nonce}`,
        clientSecret: `secret-${nonce}`,
      },
    ]);

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
          return new Response(
            JSON.stringify({
              access_token: idpAccessToken,
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
          const authorization = new Headers(init?.headers).get("authorization");
          expect(authorization).toBe(`Bearer ${idpAccessToken}`);
          return new Response(
            JSON.stringify({
              sub: `oidc-user-${nonce}`,
              email: `exchange-mfa-${nonce}@example.com`,
              name: `换会话MFA用户-${nonce}`,
            }),
            {
              status: 200,
              headers: {
                "content-type": "application/json",
              },
            },
          );
        }
        throw new Error(`unexpected fetch url in external/exchange mfa test: ${url}`);
      }) as unknown as typeof fetch;

      const response = await app.request("/api/v1/auth/external/exchange", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-forwarded-for": clientIp,
          "user-agent": userAgent,
        },
        body: JSON.stringify({
          providerId,
          code: `authorization-code-${nonce}`,
          redirectUri: `https://console.example.com/callback/${nonce}`,
          codeVerifier: `code-verifier-${nonce}`,
        }),
      });
      expect(response.status).toBe(401);
      const body = await readResponseAsUnknown(response);
      if (isRecord(body)) {
        expect(pickBoolean(body, ["mfaRequired"])).toBe(true);
        expect(pickString(body, ["riskDecision"])).toBe("allow_with_risk");
        expect(pickString(body, ["message"])).toContain("上游 MFA 验证");
      }

      const auth = await getDefaultAuthContext();
      const audits = await queryAuditByAction(
        "auth.external_exchange_failed",
        "/api/v1/auth/external/exchange",
        auth.accessToken,
        auth.userId,
      );
      const mfaAudit = audits.items.find(
        (item) =>
          item.metadata.reason === "外部授权码登录需要上游 MFA 验证。" &&
          item.metadata.providerId === providerId &&
          item.metadata.providerType === "oidc" &&
          item.metadata.clientIp === clientIp &&
          item.metadata.userAgent === userAgent &&
          item.metadata.riskLevel === "medium" &&
          item.metadata.failureStage === "mfa_validation" &&
          Array.isArray(item.metadata.riskSignals) &&
          item.metadata.riskSignals.includes("mfa_required"),
      );
      expect(mfaAudit).toBeDefined();
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
    const clientIp = "203.0.113.55";
    const userAgent = "external-exchange-upstream-fail-test/1.0";
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
            "x-forwarded-for": clientIp,
            "user-agent": userAgent,
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

      const auth = await getDefaultAuthContext();
      const audits = await queryAuditByAction(
        "auth.external_exchange_failed",
        "/api/v1/auth/external/exchange",
        auth.accessToken,
        auth.userId,
      );
      const tokenFailureAudit = audits.items.find(
        (item) =>
          item.metadata.providerId === providerId &&
          item.metadata.providerType === "oidc" &&
          item.metadata.clientIp === clientIp &&
          item.metadata.userAgent === userAgent &&
          item.metadata.riskLevel === "medium" &&
          item.metadata.failureStage === "token_endpoint" &&
          Array.isArray(item.metadata.riskSignals) &&
          item.metadata.riskSignals.includes("upstream_token_failed"),
      );
      expect(tokenFailureAudit).toBeDefined();

      const userinfoFailureAudit = audits.items.find(
        (item) =>
          item.metadata.providerId === providerId &&
          item.metadata.providerType === "oidc" &&
          item.metadata.riskLevel === "medium" &&
          item.metadata.failureStage === "userinfo_endpoint" &&
          Array.isArray(item.metadata.riskSignals) &&
          item.metadata.riskSignals.includes("upstream_userinfo_failed"),
      );
      expect(userinfoFailureAudit).toBeDefined();
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

  test("AUTH_EXTERNAL_RISK_MODE=block 时 external login/exchange 失败响应带 blocked", async () => {
    const nonce = createNonce("auth-external-risk-mode-block");
    const secret = `external-secret-${nonce}`;
    const originalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    const originalSecret = Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET;
    const originalRiskMode = Bun.env.AUTH_EXTERNAL_RISK_MODE;
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: "corp-oidc",
        type: "oidc",
        displayName: "企业 OIDC",
        enabled: true,
      },
    ]);
    Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET = secret;
    Bun.env.AUTH_EXTERNAL_RISK_MODE = "block";

    try {
      const response = await app.request("/api/v1/auth/external/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          providerId: "corp-oidc",
          externalUserId: `ext-user-${nonce}`,
          email: `external-${nonce}@example.com`,
          timestamp: new Date(Date.now() - 10 * 60 * 1000).toISOString(),
          nonce: `nonce-${nonce}`,
          signature: "0".repeat(64),
        }),
      });
      expect(response.status).toBe(401);
      const body = (await response.json()) as {
        riskDecision?: string;
      };
      expect(body.riskDecision).toBe("blocked");

      const exchangeResponse = await app.request("/api/v1/auth/external/exchange", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          providerId: "missing-provider",
          code: `authorization-code-${nonce}`,
          redirectUri: `https://console.example.com/callback/${nonce}`,
        }),
      });
      expect(exchangeResponse.status).toBe(401);
      const exchangeBody = (await exchangeResponse.json()) as {
        riskDecision?: string;
      };
      expect(exchangeBody.riskDecision).toBe("blocked");
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
      if (originalRiskMode === undefined) {
        delete Bun.env.AUTH_EXTERNAL_RISK_MODE;
      } else {
        Bun.env.AUTH_EXTERNAL_RISK_MODE = originalRiskMode;
      }
    }
  });

  test("AUTH_EXTERNAL_RISK_BLOCK_LEVEL=medium 时 medium 风险会被阻断", async () => {
    const nonce = createNonce("auth-external-risk-block-level-medium");
    const secret = `external-secret-${nonce}`;
    const originalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    const originalSecret = Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET;
    const originalRiskMode = Bun.env.AUTH_EXTERNAL_RISK_MODE;
    const originalRiskBlockLevel = Bun.env.AUTH_EXTERNAL_RISK_BLOCK_LEVEL;
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: "corp-saml",
        type: "saml",
        displayName: "企业 SAML",
        metadataUrl: "https://idp.example.com/metadata",
        ssoUrl: "https://idp.example.com/sso",
        binding: "post",
        enabled: true,
      },
    ]);
    Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET = secret;
    Bun.env.AUTH_EXTERNAL_RISK_MODE = "block";
    Bun.env.AUTH_EXTERNAL_RISK_BLOCK_LEVEL = "medium";

    try {
      const basePayload = {
        providerId: "corp-saml",
        externalUserId: `ext-user-${nonce}`,
        email: `external-${nonce}@example.com`,
        timestamp: new Date().toISOString(),
        nonce: `nonce-${nonce}`,
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

      const response = await app.request("/api/v1/auth/external/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-agentledger-risk-level": "medium",
          "user-agent": "risk-test/1.0",
          "x-forwarded-for": "203.0.113.101",
        },
        body: JSON.stringify({
          ...basePayload,
          signature,
        }),
      });
      expect(response.status).toBe(403);
      const body = (await response.json()) as { riskDecision?: string };
      expect(body.riskDecision).toBe("blocked");
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
      if (originalRiskMode === undefined) {
        delete Bun.env.AUTH_EXTERNAL_RISK_MODE;
      } else {
        Bun.env.AUTH_EXTERNAL_RISK_MODE = originalRiskMode;
      }
      if (originalRiskBlockLevel === undefined) {
        delete Bun.env.AUTH_EXTERNAL_RISK_BLOCK_LEVEL;
      } else {
        Bun.env.AUTH_EXTERNAL_RISK_BLOCK_LEVEL = originalRiskBlockLevel;
      }
    }
  });

  test("external login 在 requireMfa 与高风险头场景下会被拦截", async () => {
    const nonce = createNonce("auth-external-mfa-risk");
    const secret = `external-secret-${nonce}`;
    const originalProviders = Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON;
    const originalSecret = Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET;
    const originalRiskMode = Bun.env.AUTH_EXTERNAL_RISK_MODE;
    Bun.env.AUTH_EXTERNAL_PROVIDERS_JSON = JSON.stringify([
      {
        id: "corp-saml",
        type: "saml",
        displayName: "企业 SAML",
        metadataUrl: "https://idp.example.com/metadata",
        ssoUrl: "https://idp.example.com/sso",
        binding: "post",
        enabled: true,
        requireMfa: true,
      },
    ]);
    Bun.env.AUTH_EXTERNAL_ASSERTION_SECRET = secret;
    Bun.env.AUTH_EXTERNAL_RISK_MODE = "block";

    try {
      const basePayload = {
        providerId: "corp-saml",
        externalUserId: `ext-user-${nonce}`,
        email: `external-saml-${nonce}@example.com`,
        timestamp: new Date().toISOString(),
        nonce: `nonce-${nonce}`,
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

      const mfaMissingResponse = await app.request("/api/v1/auth/external/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          ...basePayload,
          signature,
        }),
      });
      expect(mfaMissingResponse.status).toBe(401);
      const mfaMissingBody = (await mfaMissingResponse.json()) as {
        mfaRequired?: boolean;
      };
      expect(mfaMissingBody.mfaRequired).toBe(true);

      const highRiskResponse = await app.request("/api/v1/auth/external/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-agentledger-risk-level": "high",
          "x-agentledger-risk-signals": "suspicious_ip,high_risk_network",
          "user-agent": "risk-test/1.0",
          "x-forwarded-for": "203.0.113.99",
        },
        body: JSON.stringify({
          ...basePayload,
          nonce: `nonce-${nonce}-risk`,
          signature: createHmac("sha256", secret)
            .update(
              [
                basePayload.providerId,
                basePayload.externalUserId,
                basePayload.email,
                "",
                basePayload.timestamp,
                `nonce-${nonce}-risk`,
              ].join("\n"),
            )
            .digest("hex"),
          mfaVerified: true,
        }),
      });
      expect(highRiskResponse.status).toBe(403);
      const highRiskBody = (await highRiskResponse.json()) as {
        riskDecision?: string;
      };
      expect(highRiskBody.riskDecision).toBe("blocked");
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
      if (originalRiskMode === undefined) {
        delete Bun.env.AUTH_EXTERNAL_RISK_MODE;
      } else {
        Bun.env.AUTH_EXTERNAL_RISK_MODE = originalRiskMode;
      }
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

  test("SCIM users/groups 支持最小同步与查询", async () => {
    const nonce = createNonce("identity-scim");
    const originalScimToken = Bun.env.SCIM_BEARER_TOKEN;
    Bun.env.SCIM_BEARER_TOKEN = `scim-token-${nonce}`;

    try {
      const owner = await registerAndLoginUser(`${nonce}-owner`);
      if (!owner.userId) {
        throw new Error("无法解析 SCIM owner userId。");
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
        throw new Error("SCIM 测试租户创建失败。");
      }

      const orgResponse = await app.request(
        `/api/v1/tenants/${encodeURIComponent(tenantId)}/organizations`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...(await issueTenantScopedAuthHeaders(tenantId, owner.accessToken, owner.userId)),
          },
          body: JSON.stringify({ name: `组织-${nonce}` }),
        },
      );
      expect(orgResponse.status).toBe(201);
      const orgBody = (await orgResponse.json()) as { id: string };

      const scimHeaders = {
        authorization: `Bearer ${Bun.env.SCIM_BEARER_TOKEN}`,
        "content-type": "application/json",
      };
      const scimEmail = `scim-${nonce}@example.com`;
      const upsertResponse = await app.request(
        `/api/v1/tenants/${encodeURIComponent(tenantId)}/scim/users`,
        {
          method: "POST",
          headers: scimHeaders,
          body: JSON.stringify({
            userName: scimEmail,
            displayName: `SCIM 用户 ${nonce}`,
            tenantRole: "maintainer",
            organizationId: orgBody.id,
            orgRole: "member",
          }),
        },
      );
      expect(upsertResponse.status).toBe(201);
      const upsertBody = (await upsertResponse.json()) as {
        id?: string;
        userName?: string;
      };
      const scimUserId = typeof upsertBody.id === "string" ? upsertBody.id : null;
      expect(scimUserId).toBeTruthy();
      expect(upsertBody.userName).toBe(scimEmail);

      const listUsersResponse = await app.request(
        `/api/v1/tenants/${encodeURIComponent(tenantId)}/scim/users`,
        {
          headers: {
            authorization: `Bearer ${Bun.env.SCIM_BEARER_TOKEN}`,
          },
        },
      );
      expect(listUsersResponse.status).toBe(200);
      const listUsersBody = (await listUsersResponse.json()) as {
        totalResults: number;
        Resources: Array<{ userName: string }>;
      };
      expect(listUsersBody.totalResults).toBeGreaterThanOrEqual(1);
      expect(
        listUsersBody.Resources.some(
          (item) => item.userName === scimEmail,
        ),
      ).toBe(true);

      const listGroupsResponse = await app.request(
        `/api/v1/tenants/${encodeURIComponent(tenantId)}/scim/groups`,
        {
          headers: {
            authorization: `Bearer ${Bun.env.SCIM_BEARER_TOKEN}`,
          },
        },
      );
      expect(listGroupsResponse.status).toBe(200);
      const listGroupsBody = (await listGroupsResponse.json()) as {
        totalResults: number;
        Resources: Array<{ id: string }>;
      };
      expect(listGroupsBody.totalResults).toBeGreaterThanOrEqual(1);
      expect(listGroupsBody.Resources.some((item) => item.id === orgBody.id)).toBe(true);

      const filteredUsersResponse = await app.request(
        `/api/v1/tenants/${encodeURIComponent(tenantId)}/scim/users?filter=${encodeURIComponent(
          `userName eq \"${scimEmail}\"`,
        )}`,
        {
          headers: {
            authorization: `Bearer ${Bun.env.SCIM_BEARER_TOKEN}`,
          },
        },
      );
      expect(filteredUsersResponse.status).toBe(200);
      const filteredUsersBody = (await filteredUsersResponse.json()) as {
        totalResults: number;
        Resources: Array<{ id: string; userName: string }>;
      };
      expect(filteredUsersBody.totalResults).toBe(1);
      expect(filteredUsersBody.Resources.length).toBe(1);
      expect(filteredUsersBody.Resources[0]?.userName).toBe(scimEmail);

      const pagedUsersResponse = await app.request(
        `/api/v1/tenants/${encodeURIComponent(tenantId)}/scim/users?startIndex=1&count=0`,
        {
          headers: {
            authorization: `Bearer ${Bun.env.SCIM_BEARER_TOKEN}`,
          },
        },
      );
      expect(pagedUsersResponse.status).toBe(200);
      const pagedUsersBody = (await pagedUsersResponse.json()) as {
        itemsPerPage: number;
        Resources: Array<{ userName: string }>;
      };
      expect(pagedUsersBody.itemsPerPage).toBe(0);
      expect(pagedUsersBody.Resources.length).toBe(0);

      const filteredGroupsResponse = await app.request(
        `/api/v1/tenants/${encodeURIComponent(tenantId)}/scim/groups?filter=${encodeURIComponent(
          `displayName eq \"组织-${nonce}\"`,
        )}`,
        {
          headers: {
            authorization: `Bearer ${Bun.env.SCIM_BEARER_TOKEN}`,
          },
        },
      );
      expect(filteredGroupsResponse.status).toBe(200);
      const filteredGroupsBody = (await filteredGroupsResponse.json()) as {
        totalResults: number;
        Resources: Array<{ id: string }>;
      };
      expect(filteredGroupsBody.totalResults).toBe(1);
      expect(filteredGroupsBody.Resources[0]?.id).toBe(orgBody.id);

      const membersBeforeResult = await listTenantMembersByAuth(
        owner.accessToken,
        tenantId,
        owner.userId,
      );
      assertApiStatus(membersBeforeResult, [200]);
      const membersBefore = extractListItems(membersBeforeResult.payload);
      const scimMemberBefore = membersBefore.find((item) => {
        const id = pickString(item, ["userId"]);
        return Boolean(scimUserId && id === scimUserId);
      });
      expect(scimMemberBefore).toBeDefined();
      if (scimMemberBefore && isRecord(scimMemberBefore)) {
        expect(pickString(scimMemberBefore, ["tenantRole"])).toBe("maintainer");
        expect(pickString(scimMemberBefore, ["organizationId"])).toBe(orgBody.id);
        expect(pickString(scimMemberBefore, ["orgRole"])).toBe("member");
      }

      const updateResponse = await app.request(
        `/api/v1/tenants/${encodeURIComponent(tenantId)}/scim/users/${encodeURIComponent(
          scimUserId ?? "missing",
        )}`,
        {
          method: "PUT",
          headers: scimHeaders,
          body: JSON.stringify({
            displayName: `SCIM 用户 Updated ${nonce}`,
            tenantRole: "readonly",
            organizationId: null,
          }),
        },
      );
      expect(updateResponse.status).toBe(200);
      const updateBody = (await updateResponse.json()) as {
        displayName?: string;
        roles?: Array<{ value?: string }>;
        meta?: Record<string, unknown>;
      };
      expect(updateBody.displayName).toBe(`SCIM 用户 Updated ${nonce}`);
      expect(updateBody.roles?.[0]?.value).toBe("readonly");
      expect((updateBody.meta ?? {})["organizationId"]).toBeUndefined();

      const membersAfterResult = await listTenantMembersByAuth(
        owner.accessToken,
        tenantId,
        owner.userId,
      );
      assertApiStatus(membersAfterResult, [200]);
      const membersAfter = extractListItems(membersAfterResult.payload);
      const scimMemberAfter = membersAfter.find((item) => {
        const id = pickString(item, ["userId"]);
        return Boolean(scimUserId && id === scimUserId);
      });
      expect(scimMemberAfter).toBeDefined();
      if (scimMemberAfter && isRecord(scimMemberAfter)) {
        expect(pickString(scimMemberAfter, ["tenantRole"])).toBe("readonly");
        expect(pickString(scimMemberAfter, ["organizationId"])).toBeUndefined();
        expect(pickString(scimMemberAfter, ["orgRole"])).toBeUndefined();
      }
    } finally {
      if (originalScimToken === undefined) {
        delete Bun.env.SCIM_BEARER_TOKEN;
      } else {
        Bun.env.SCIM_BEARER_TOKEN = originalScimToken;
      }
    }
  });

  test("SCIM token 缺失/未配置返回 401/503（契约稳定）", async () => {
    const nonce = createNonce("identity-scim-auth-errors");
    const originalScimToken = Bun.env.SCIM_BEARER_TOKEN;

    const owner = await registerAndLoginUser(`${nonce}-owner`);
    if (!owner.userId) {
      throw new Error("无法解析 SCIM auth-errors owner userId。");
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
      throw new Error("SCIM auth-errors 测试租户创建失败。");
    }

    const usersPath = `/api/v1/tenants/${encodeURIComponent(tenantId)}/scim/users`;
    const groupsPath = `/api/v1/tenants/${encodeURIComponent(tenantId)}/scim/groups`;

    try {
      Bun.env.SCIM_BEARER_TOKEN = `scim-token-${nonce}`;

      const missingHeaderResponse = await app.request(usersPath);
      expect(missingHeaderResponse.status).toBe(401);
      const missingHeaderBody = await readResponseAsUnknown(missingHeaderResponse);
      if (isRecord(missingHeaderBody)) {
        expect(pickString(missingHeaderBody, ["message"])).toBe(
          "SCIM 未认证：缺少或无效的 Bearer Token。",
        );
      }

      const wrongTokenResponse = await app.request(groupsPath, {
        headers: {
          authorization: `Bearer wrong-${nonce}`,
        },
      });
      expect(wrongTokenResponse.status).toBe(401);
      const wrongTokenBody = await readResponseAsUnknown(wrongTokenResponse);
      if (isRecord(wrongTokenBody)) {
        expect(pickString(wrongTokenBody, ["message"])).toBe(
          "SCIM 未认证：缺少或无效的 Bearer Token。",
        );
      }

      delete Bun.env.SCIM_BEARER_TOKEN;
      const notConfiguredResponse = await app.request(usersPath, {
        headers: {
          authorization: `Bearer whatever-${nonce}`,
        },
      });
      expect(notConfiguredResponse.status).toBe(503);
      const notConfiguredBody = await readResponseAsUnknown(notConfiguredResponse);
      if (isRecord(notConfiguredBody)) {
        expect(pickString(notConfiguredBody, ["message"])).toBe(
          "服务端未配置 SCIM_BEARER_TOKEN。",
        );
      }
    } finally {
      if (originalScimToken === undefined) {
        delete Bun.env.SCIM_BEARER_TOKEN;
      } else {
        Bun.env.SCIM_BEARER_TOKEN = originalScimToken;
      }
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

  test("告警外部联动：integration callback 可写入外部实体并在 ACK/Resolve 时同步本地状态映射", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const originalCallbackSecret = Bun.env.INTEGRATION_CALLBACK_SECRET;
    const callbackSecret = `integration-secret-${createNonce("cb-external-link")}`;
    const { alert, cleanup } = await createTestAlert(tenantId, "open");
    const publishedEvents: Array<Record<string, unknown>> = [];

    try {
      Bun.env.INTEGRATION_CALLBACK_SECRET = callbackSecret;
      __setAlertExternalStatusSyncPublisherForTests(async (events) => {
        publishedEvents.push(
          ...events.map(
            (event) => ({ ...event }) as Record<string, unknown>,
          ),
        );
        return {
          published: events.length,
          failed: 0,
          errors: [],
        };
      });

      const upsertLinkResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-external-link-create"),
          tenant_id: tenantId,
          action: "upsert_external_link",
          alert_id: alert.id,
          external_type: "ticket",
          external_system: "ticket",
          external_id: "ticket-1001",
          external_status: "open",
        },
      );
      expect(upsertLinkResponse.status).toBe(200);

      const alertsResponse = await app.request("/api/v1/alerts?limit=50", {
        headers: authHeaders,
      });
      expect(alertsResponse.status).toBe(200);
      const alertsBody = (await alertsResponse.json()) as {
        items: Array<{
          id: string;
          externalLinks?: Array<{
            externalId: string;
            externalStatus?: string;
          }>;
        }>;
      };
      const target = alertsBody.items.find((item) => item.id === alert.id);
      expect(target?.externalLinks?.[0]?.externalId).toBe("ticket-1001");
      expect(target?.externalLinks?.[0]?.externalStatus).toBe("open");

      const acknowledgeResponse = await app.request(
        `/api/v1/alerts/${encodeURIComponent(alert.id)}/status`,
        {
          method: "PATCH",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({ status: "acknowledged" }),
        },
      );
      expect(acknowledgeResponse.status).toBe(200);
      const acknowledgedAlert = (await acknowledgeResponse.json()) as {
        externalLinks?: Array<{
          externalStatus?: string;
          pendingExternalStatus?: string;
          publishStatus?: string;
          publishError?: string;
          lastSyncResult?: string;
          lastSyncError?: string;
        }>;
      };
      expect(acknowledgedAlert.externalLinks?.[0]?.externalStatus).toBe("open");
      expect(acknowledgedAlert.externalLinks?.[0]?.pendingExternalStatus).toBe(
        "acknowledged",
      );
      expect(acknowledgedAlert.externalLinks?.[0]?.publishStatus).toBe(
        "success",
      );
      expect(acknowledgedAlert.externalLinks?.[0]?.lastSyncResult).toBeUndefined();
      expect(publishedEvents).toContainEqual(
        expect.objectContaining({
          tenant_id: tenantId,
          alert_id: alert.id,
          external_type: "ticket",
          external_id: "ticket-1001",
          external_status: "acknowledged",
        }),
      );

      const acknowledgeSyncResultResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-external-link-ack-result"),
          tenant_id: tenantId,
          action: "sync_external_link_result",
          alert_id: alert.id,
          external_type: "ticket",
          external_system: "ticket",
          external_id: "ticket-1001",
          external_status: "acknowledged",
          sync_result: "success",
        },
      );
      expect(acknowledgeSyncResultResponse.status).toBe(200);

      const acknowledgedRefreshedResponse = await app.request(
        "/api/v1/alerts?limit=50",
        {
          headers: authHeaders,
        },
      );
      expect(acknowledgedRefreshedResponse.status).toBe(200);
      const acknowledgedRefreshedBody =
        (await acknowledgedRefreshedResponse.json()) as {
          items: Array<{
            id: string;
            externalLinks?: Array<{
              externalStatus?: string;
              pendingExternalStatus?: string;
              publishStatus?: string;
              lastSyncResult?: string;
              lastSyncError?: string;
            }>;
          }>;
        };
      const acknowledgedTarget = acknowledgedRefreshedBody.items.find(
        (item) => item.id === alert.id,
      );
      expect(acknowledgedTarget?.externalLinks?.[0]).toMatchObject({
        externalStatus: "acknowledged",
        publishStatus: "success",
        lastSyncResult: "success",
      });
      expect(
        acknowledgedTarget?.externalLinks?.[0]?.pendingExternalStatus,
      ).toBeUndefined();

      const resolveResponse = await app.request(
        `/api/v1/alerts/${encodeURIComponent(alert.id)}/status`,
        {
          method: "PATCH",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({ status: "resolved" }),
        },
      );
      expect(resolveResponse.status).toBe(200);
      const resolvedAlert = (await resolveResponse.json()) as {
        externalLinks?: Array<{
          externalStatus?: string;
          pendingExternalStatus?: string;
          publishStatus?: string;
          lastSyncResult?: string;
          lastSyncError?: string;
        }>;
      };
      expect(resolvedAlert.externalLinks?.[0]?.externalStatus).toBe(
        "acknowledged",
      );
      expect(resolvedAlert.externalLinks?.[0]?.pendingExternalStatus).toBe(
        "resolved",
      );
      expect(resolvedAlert.externalLinks?.[0]?.publishStatus).toBe("success");
      expect(resolvedAlert.externalLinks?.[0]?.lastSyncResult).toBeUndefined();
      expect(publishedEvents).toContainEqual(
        expect.objectContaining({
          tenant_id: tenantId,
          alert_id: alert.id,
          external_type: "ticket",
          external_id: "ticket-1001",
          external_status: "resolved",
        }),
      );

      const resolveSyncResultResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-external-link-resolve-result"),
          tenant_id: tenantId,
          action: "sync_external_link_result",
          alert_id: alert.id,
          external_type: "ticket",
          external_system: "ticket",
          external_id: "ticket-1001",
          external_status: "resolved",
          sync_result: "failed",
          sync_error: "downstream timeout",
          failure_stage: "dispatch_http",
          failure_code: "downstream_http_5xx",
        },
      );
      expect(resolveSyncResultResponse.status).toBe(200);

      const resolvedRefreshedResponse = await app.request("/api/v1/alerts?limit=50", {
        headers: authHeaders,
      });
      expect(resolvedRefreshedResponse.status).toBe(200);
      const resolvedRefreshedBody = (await resolvedRefreshedResponse.json()) as {
        items: Array<{
          id: string;
          externalLinks?: Array<{
            externalStatus?: string;
            pendingExternalStatus?: string;
            publishStatus?: string;
            lastSyncResult?: string;
            lastSyncError?: string;
            lastSyncFailureStage?: string;
            lastSyncFailureCode?: string;
          }>;
        }>;
      };
      const resolvedTarget = resolvedRefreshedBody.items.find(
        (item) => item.id === alert.id,
      );
      expect(resolvedTarget?.externalLinks?.[0]).toMatchObject({
        externalStatus: "acknowledged",
        pendingExternalStatus: "resolved",
        publishStatus: "success",
        lastSyncResult: "failed",
        lastSyncError: "downstream timeout",
        lastSyncFailureStage: "dispatch_http",
        lastSyncFailureCode: "downstream_http_5xx",
      });

      const opsResponse = await app.request(
        `/api/v1/alerts/${encodeURIComponent(alert.id)}/external-links?onlyFailed=true&externalType=ticket`,
        {
          headers: authHeaders,
        },
      );
      expect(opsResponse.status).toBe(200);
      const opsBody = (await opsResponse.json()) as {
        alertId: string;
        summary: {
          total: number;
          pending: number;
          failed: number;
        };
        items: Array<{
          externalId: string;
          syncState: string;
          retryable: boolean;
          lastSyncFailureStage?: string;
          lastSyncFailureCode?: string;
        }>;
        filters: {
          externalType?: string;
          onlyFailed?: boolean;
        };
      };
      expect(opsBody.alertId).toBe(alert.id);
      expect(opsBody.summary).toEqual({
        total: 1,
        pending: 0,
        failed: 1,
      });
      expect(opsBody.filters).toEqual({
        externalType: "ticket",
        onlyFailed: true,
      });
      expect(opsBody.items).toHaveLength(1);
      expect(opsBody.items[0]).toMatchObject({
        externalId: "ticket-1001",
        syncState: "failed",
        retryable: true,
        lastSyncFailureStage: "dispatch_http",
        lastSyncFailureCode: "downstream_http_5xx",
      });

      const failuresResponse = await app.request(
        `/api/v1/alerts/external-links/failures?alertId=${encodeURIComponent(
          alert.id,
        )}&externalSystem=ticket&syncState=failed&limit=10`,
        {
          headers: authHeaders,
        },
      );
      expect(failuresResponse.status).toBe(200);
      const failuresBody = (await failuresResponse.json()) as {
        summary: {
          total: number;
          pending: number;
          failed: number;
        };
        items: Array<{
          alertId: string;
          externalSystem: string;
          externalId: string;
          syncState: string;
          retryable: boolean;
        }>;
        filters: {
          alertId?: string;
          externalSystem?: string;
          syncState?: string;
          limit?: number;
        };
      };
      expect(failuresBody.summary).toEqual({
        total: 1,
        pending: 0,
        failed: 1,
      });
      expect(failuresBody.filters).toEqual({
        alertId: alert.id,
        externalSystem: "ticket",
        syncState: "failed",
        limit: 10,
      });
      expect(failuresBody.items[0]).toMatchObject({
        alertId: alert.id,
        externalSystem: "ticket",
        externalId: "ticket-1001",
        syncState: "failed",
        retryable: true,
      });

      const retryResponse = await app.request(
        `/api/v1/alerts/${encodeURIComponent(alert.id)}/external-links/retry-sync`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({
            externalType: "ticket",
            externalId: "ticket-1001",
          }),
        },
      );
      expect(retryResponse.status).toBe(200);
      const retriedAlert = (await retryResponse.json()) as {
        externalLinks?: Array<{
          externalStatus?: string;
          pendingExternalStatus?: string;
          publishStatus?: string;
          lastSyncResult?: string;
          lastSyncError?: string;
        }>;
      };
      expect(retriedAlert.externalLinks?.[0]).toMatchObject({
        externalStatus: "acknowledged",
        pendingExternalStatus: "resolved",
        publishStatus: "success",
      });
      expect(retriedAlert.externalLinks?.[0]?.lastSyncResult).toBeUndefined();
      expect(retriedAlert.externalLinks?.[0]?.lastSyncError).toBeUndefined();
      expect(publishedEvents).toContainEqual(
        expect.objectContaining({
          tenant_id: tenantId,
          alert_id: alert.id,
          external_type: "ticket",
          external_id: "ticket-1001",
          external_status: "resolved",
        }),
      );

      const retryAudits = await queryAuditByAction(
        "control_plane.alert_external_link_retry_completed",
        alert.id,
      );
      const retryAudit = retryAudits.items.find((item) => {
        return (
          item.action === "control_plane.alert_external_link_retry_completed" &&
          item.metadata.alertId === alert.id &&
          item.metadata.externalId === "ticket-1001" &&
          item.metadata.scope === "single"
        );
      });
      expect(retryAudit).toBeDefined();

      const batchRetryResponse = await app.request(
        `/api/v1/alerts/${encodeURIComponent(alert.id)}/external-links/retry-sync-batch`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({
            externalType: "ticket",
          }),
        },
      );
      expect(batchRetryResponse.status).toBe(200);
      const batchRetriedAlert = (await batchRetryResponse.json()) as {
        alertId: string;
        retriedCount: number;
        published: number;
        failed: number;
        items: Array<{
          externalId: string;
          syncState: string;
          retryable: boolean;
          pendingExternalStatus?: string;
        }>;
      };
      expect(batchRetriedAlert.alertId).toBe(alert.id);
      expect(batchRetriedAlert.retriedCount).toBe(1);
      expect(batchRetriedAlert.published).toBe(1);
      expect(batchRetriedAlert.failed).toBe(0);
      expect(batchRetriedAlert.items[0]).toMatchObject({
        externalId: "ticket-1001",
        syncState: "pending",
        retryable: true,
        pendingExternalStatus: "resolved",
      });

      const batchRetryAudits = await queryAuditByAction(
        "control_plane.alert_external_link_retry_completed",
        alert.id,
      );
      const batchRetryAudit = batchRetryAudits.items.find((item) => {
        return (
          item.action === "control_plane.alert_external_link_retry_completed" &&
          item.metadata.alertId === alert.id &&
          item.metadata.retriedCount === 1 &&
          item.metadata.scope === "batch"
        );
      });
      expect(batchRetryAudit).toBeDefined();

      const retrySyncResultResponse = await postIntegrationAlertCallback(
        callbackSecret,
        {
          callback_id: createNonce("cb-external-link-retry-result"),
          tenant_id: tenantId,
          action: "sync_external_link_result",
          alert_id: alert.id,
          external_type: "ticket",
          external_system: "ticket",
          external_id: "ticket-1001",
          external_status: "resolved",
          sync_result: "success",
        },
      );
      expect(retrySyncResultResponse.status).toBe(200);

      const retryRefreshedResponse = await app.request("/api/v1/alerts?limit=50", {
        headers: authHeaders,
      });
      expect(retryRefreshedResponse.status).toBe(200);
      const retryRefreshedBody = (await retryRefreshedResponse.json()) as {
        items: Array<{
          id: string;
          externalLinks?: Array<{
            externalStatus?: string;
            pendingExternalStatus?: string;
            publishStatus?: string;
            lastSyncResult?: string;
            lastSyncError?: string;
          }>;
        }>;
      };
      const retryTarget = retryRefreshedBody.items.find(
        (item) => item.id === alert.id,
      );
      expect(retryTarget?.externalLinks?.[0]).toMatchObject({
        externalStatus: "resolved",
        publishStatus: "success",
        lastSyncResult: "success",
      });
      expect(retryTarget?.externalLinks?.[0]?.pendingExternalStatus).toBeUndefined();
      expect(retryTarget?.externalLinks?.[0]?.lastSyncError).toBeUndefined();
    } finally {
      await __resetAlertExternalStatusSyncPublisherForTests();
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

  test("TokenPulse runtime events 路由支持 202/200、租户隔离与审计查询", async () => {
    const nonce = createNonce("tokenpulse-runtime");
    const ownerA = await registerAndLoginUser(`${nonce}-owner-a`);
    const ownerB = await registerAndLoginUser(`${nonce}-owner-b`);
    if (!ownerA.userId || !ownerB.userId) {
      throw new Error("无法解析 TokenPulse runtime 测试用户 ID。");
    }

    const tenantAResult = await createTenantByAuth(
      ownerA.accessToken,
      {
        name: `TokenPulse Runtime Tenant A ${nonce}`,
        slug: `tokenpulse-runtime-a-${nonce}`,
      },
      ownerA.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("租户 A 创建失败，缺少 tenantId。");
    }

    const tenantBResult = await createTenantByAuth(
      ownerB.accessToken,
      {
        name: `TokenPulse Runtime Tenant B ${nonce}`,
        slug: `tokenpulse-runtime-b-${nonce}`,
      },
      ownerB.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("租户 B 创建失败，缺少 tenantId。");
    }

    const tenantAHeaders = await issueTenantScopedAuthHeaders(
      tenantAId,
      ownerA.accessToken,
      ownerA.userId,
    );
    const tenantBHeaders = await issueTenantScopedAuthHeaders(
      tenantBId,
      ownerB.accessToken,
      ownerB.userId,
    );

    const originalWebhookSecret = Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET;
    const originalWebhookKeyId = Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_KEY_ID;
    const webhookSecret = `tokenpulse-secret-${nonce}`;
    Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET = webhookSecret;
    Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_KEY_ID = TOKENPULSE_RUNTIME_DEFAULT_KEY_ID;

    try {
      const payload = {
        tenantId: tenantAId,
        projectId: "project-risk-control",
        traceId: `trace-${nonce}`,
        provider: "claude",
        model: "claude-sonnet",
        resolvedModel: "claude:claude-3-7-sonnet-20250219",
        routePolicy: "latest_valid",
        accountId: "claude-account-01",
        status: "success",
        startedAt: "2026-03-08T09:59:58.123Z",
        finishedAt: "2026-03-08T09:59:59.204Z",
        cost: "0.002310",
      } as const;

      const firstResponse = await postTokenPulseRuntimeEvent(
        webhookSecret,
        payload,
      );
      expect(firstResponse.status).toBe(202);
      const firstBody = (await firstResponse.json()) as {
        duplicate: boolean;
        item: {
          tenantId: string;
          projectId?: string;
          traceId: string;
          provider: string;
          routePolicy: string;
          status: string;
          cost?: string;
          idempotencyKey: string;
          specVersion: string;
        };
      };
      expect(firstBody.duplicate).toBe(false);
      expect(firstBody.item.tenantId).toBe(tenantAId);
      expect(firstBody.item.projectId).toBe("project-risk-control");
      expect(firstBody.item.traceId).toBe(payload.traceId);
      expect(firstBody.item.provider).toBe("claude");
      expect(firstBody.item.routePolicy).toBe("latest_valid");
      expect(firstBody.item.status).toBe("success");
      expect(firstBody.item.cost).toBe("0.002310");
      expect(firstBody.item.specVersion).toBe("v1");

      const duplicateResponse = await postTokenPulseRuntimeEvent(
        webhookSecret,
        payload,
      );
      expect(duplicateResponse.status).toBe(200);
      const duplicateBody = (await duplicateResponse.json()) as {
        duplicate: boolean;
        item: {
          idempotencyKey: string;
          traceId: string;
        };
      };
      expect(duplicateBody.duplicate).toBe(true);
      expect(duplicateBody.item.idempotencyKey).toBe(firstBody.item.idempotencyKey);
      expect(duplicateBody.item.traceId).toBe(payload.traceId);

      const queryResponse = await app.request(
        `/api/v1/integrations/tokenpulse/runtime-events?traceId=${encodeURIComponent(payload.traceId)}`,
        {
          headers: tenantAHeaders,
        },
      );
      expect(queryResponse.status).toBe(200);
      const queryBody = (await queryResponse.json()) as {
        items: Array<{
          tenantId: string;
          traceId: string;
          provider: string;
          status: string;
          routePolicy: string;
          cost?: string;
        }>;
        total: number;
        filters: {
          traceId?: string;
        };
      };
      expect(queryBody.total).toBe(1);
      expect(queryBody.filters.traceId).toBe(payload.traceId);
      expect(queryBody.items).toHaveLength(1);
      expect(queryBody.items[0]?.tenantId).toBe(tenantAId);
      expect(queryBody.items[0]?.provider).toBe("claude");
      expect(queryBody.items[0]?.status).toBe("success");
      expect(queryBody.items[0]?.routePolicy).toBe("latest_valid");
      expect(queryBody.items[0]?.cost).toBe("0.002310");

      const crossTenantResponse = await app.request(
        `/api/v1/integrations/tokenpulse/runtime-events?traceId=${encodeURIComponent(payload.traceId)}`,
        {
          headers: tenantBHeaders,
        },
      );
      expect(crossTenantResponse.status).toBe(200);
      const crossTenantBody = (await crossTenantResponse.json()) as {
        items: unknown[];
        total: number;
      };
      expect(crossTenantBody.total).toBe(0);
      expect(crossTenantBody.items).toHaveLength(0);

      const ingestAudits = await queryAuditByActionWithHeaders(
        "control_plane.tokenpulse_runtime_event_ingested",
        payload.traceId,
        tenantAHeaders,
      );
      expect(
        ingestAudits.items.some(
          (item) =>
            item.action === "control_plane.tokenpulse_runtime_event_ingested" &&
            item.metadata.traceId === payload.traceId &&
            item.metadata.provider === "claude" &&
            item.metadata.status === "success" &&
            item.metadata.duplicate === false,
        ),
      ).toBe(true);

      const duplicateAudits = await queryAuditByActionWithHeaders(
        "control_plane.tokenpulse_runtime_event_duplicate",
        payload.traceId,
        tenantAHeaders,
      );
      expect(
        duplicateAudits.items.some(
          (item) =>
            item.action === "control_plane.tokenpulse_runtime_event_duplicate" &&
            item.metadata.traceId === payload.traceId &&
            item.metadata.duplicate === true,
        ),
      ).toBe(true);

      const queryAudits = await queryAuditByActionWithHeaders(
        "control_plane.tokenpulse_runtime_event_queried",
        payload.traceId,
        tenantAHeaders,
      );
      expect(
        queryAudits.items.some(
          (item) =>
            item.action === "control_plane.tokenpulse_runtime_event_queried" &&
            item.metadata.traceId === payload.traceId &&
            item.metadata.resultCount === 1,
        ),
      ).toBe(true);
    } finally {
      if (originalWebhookSecret === undefined) {
        delete Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET;
      } else {
        Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET = originalWebhookSecret;
      }
      if (originalWebhookKeyId === undefined) {
        delete Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_KEY_ID;
      } else {
        Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_KEY_ID = originalWebhookKeyId;
      }
    }
  });

  test("TokenPulse runtime events 路由对 header 和 payload 非法值返回 400/401", async () => {
    const originalWebhookSecret = Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET;
    const originalWebhookKeyId = Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_KEY_ID;
    const webhookSecret = `tokenpulse-secret-${createNonce("tokenpulse-runtime-invalid")}`;
    Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET = webhookSecret;
    Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_KEY_ID = TOKENPULSE_RUNTIME_DEFAULT_KEY_ID;

    try {
      const payload = {
        tenantId: "default",
        traceId: createNonce("tokenpulse-trace"),
        provider: "claude",
        model: "claude-sonnet",
        resolvedModel: "claude:claude-3-7-sonnet-20250219",
        routePolicy: "latest_valid",
        status: "success",
        startedAt: "2026-03-08T09:59:58.123Z",
      };

      const invalidSpecRequest = buildTokenPulseRuntimeSignedRequest(
        webhookSecret,
        payload,
        {
          specVersion: "v2",
        },
      );
      const invalidSpecResponse = await app.request(
        "/api/v1/integrations/tokenpulse/runtime-events",
        invalidSpecRequest.init,
      );
      expect(invalidSpecResponse.status).toBe(400);

      const invalidKeyIdResponse = await postTokenPulseRuntimeEvent(
        webhookSecret,
        payload,
        {
          keyId: "unknown-key-id",
        },
      );
      expect(invalidKeyIdResponse.status).toBe(401);

      const expiredTimestampResponse = await postTokenPulseRuntimeEvent(
        webhookSecret,
        payload,
        {
          timestamp: String(Math.floor(Date.now() / 1000) - 10 * 60),
        },
      );
      expect(expiredTimestampResponse.status).toBe(401);

      const invalidSignatureResponse = await postTokenPulseRuntimeEvent(
        webhookSecret,
        payload,
        {
          signature: "deadbeef",
        },
      );
      expect(invalidSignatureResponse.status).toBe(401);

      const idempotencyMismatchResponse = await postTokenPulseRuntimeEvent(
        webhookSecret,
        payload,
        {
          idempotencyKey: createNonce("tokenpulse-mismatch"),
        },
      );
      expect(idempotencyMismatchResponse.status).toBe(400);

      const invalidStatusResponse = await postTokenPulseRuntimeEvent(
        webhookSecret,
        {
          ...payload,
          status: "cancelled",
        },
      );
      expect(invalidStatusResponse.status).toBe(400);

      const invalidRoutePolicyResponse = await postTokenPulseRuntimeEvent(
        webhookSecret,
        {
          ...payload,
          status: "failure",
          routePolicy: "custom_policy",
        },
      );
      expect(invalidRoutePolicyResponse.status).toBe(400);

      const invalidCostResponse = await postTokenPulseRuntimeEvent(
        webhookSecret,
        {
          ...payload,
          status: "failure",
          cost: 0.123,
        },
      );
      expect(invalidCostResponse.status).toBe(400);
    } finally {
      if (originalWebhookSecret === undefined) {
        delete Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET;
      } else {
        Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET = originalWebhookSecret;
      }
      if (originalWebhookKeyId === undefined) {
        delete Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_KEY_ID;
      } else {
        Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_KEY_ID = originalWebhookKeyId;
      }
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

  test("GET/POST /api/v1/integrations/dlq/messages 支持查询、replay 与审计", async () => {
    const authHeaders = await resolveAuthHeaders();
    const nonce = createNonce("integration-dlq");
    const queriedItems = [
      {
        messageId: "INTEGRATION_DISPATCH_DLQ:101",
        stream: "INTEGRATION_DISPATCH_DLQ",
        subject: "integration.alert.external_status_sync",
        eventType: "alert_external_status_sync",
        channel: "ticket",
        callbackId: `sync-result:${nonce}`,
        tenantId: resolveTenantIdFromAuthHeaders(authHeaders),
        alertId: `alert-${nonce}`,
        externalType: "ticket",
        externalId: `ticket-${nonce}`,
        failedAt: "2026-03-08T10:00:00.000Z",
        attempt: 4,
        error: "downstream timeout",
        retryable: true,
        payload: {
          subject: "integration.alert.external_status_sync",
          event_type: "alert_external_status_sync",
        },
      },
    ];

    let listCalls = 0;
    let replayCalls = 0;
    __setIntegrationDlqBackendForTests({
      async listMessages(input) {
        listCalls += 1;
        expect(input).toEqual({
          tenantId: resolveTenantIdFromAuthHeaders(authHeaders),
          eventType: "alert_external_status_sync",
          channel: "ticket",
          callbackId: `sync-result:${nonce}`,
          alertId: `alert-${nonce}`,
          limit: 10,
        });
        return {
          items: queriedItems,
          total: queriedItems.length,
          filters: {
            eventType: input.eventType,
            channel: input.channel,
            callbackId: input.callbackId,
            alertId: input.alertId,
            limit: input.limit,
          },
        };
      },
      async replayMessages(input) {
        replayCalls += 1;
        expect(input).toEqual({
          tenantId: resolveTenantIdFromAuthHeaders(authHeaders),
          messageIds: ["INTEGRATION_DISPATCH_DLQ:101"],
        });
        return {
          replayedCount: 1,
          failedCount: 0,
          items: [
            {
              messageId: "INTEGRATION_DISPATCH_DLQ:101",
              status: "replayed",
            },
          ],
        };
      },
    });

    const listResponse = await app.request(
      `/api/v1/integrations/dlq/messages?eventType=alert_external_status_sync&channel=ticket&callbackId=${encodeURIComponent(
        `sync-result:${nonce}`,
      )}&alertId=${encodeURIComponent(`alert-${nonce}`)}&limit=10`,
      {
        headers: authHeaders,
      },
    );
    expect(listResponse.status).toBe(200);
    const listBody = (await listResponse.json()) as {
      total: number;
      items: Array<{ messageId: string; alertId?: string }>;
      filters: {
        eventType?: string;
        channel?: string;
        callbackId?: string;
        alertId?: string;
        limit?: number;
      };
    };
    expect(listBody.total).toBe(1);
    expect(listBody.items[0]?.messageId).toBe("INTEGRATION_DISPATCH_DLQ:101");
    expect(listBody.filters).toEqual({
      eventType: "alert_external_status_sync",
      channel: "ticket",
      callbackId: `sync-result:${nonce}`,
      alertId: `alert-${nonce}`,
      limit: 10,
    });

    const replayResponse = await app.request(
      "/api/v1/integrations/dlq/messages/replay",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          messageIds: ["INTEGRATION_DISPATCH_DLQ:101"],
        }),
      },
    );
    expect(replayResponse.status).toBe(200);
    const replayBody = (await replayResponse.json()) as {
      replayedCount: number;
      failedCount: number;
      items: Array<{ messageId: string; status: string }>;
    };
    expect(replayBody.replayedCount).toBe(1);
    expect(replayBody.failedCount).toBe(0);
    expect(replayBody.items[0]).toEqual({
      messageId: "INTEGRATION_DISPATCH_DLQ:101",
      status: "replayed",
    });

    expect(listCalls).toBe(1);
    expect(replayCalls).toBe(1);

    const queryAudits = await queryAuditByAction(
      "control_plane.integration_dlq_messages_queried",
      `alert-${nonce}`,
    );
    expect(
      queryAudits.items.some(
        (item) =>
          item.action === "control_plane.integration_dlq_messages_queried" &&
          (item.metadata.filters as { alertId?: string } | undefined)?.alertId ===
            `alert-${nonce}`,
      ),
    ).toBe(true);

    const replayAudits = await queryAuditByAction(
      "control_plane.integration_dlq_messages_replayed",
      "INTEGRATION_DISPATCH_DLQ:101",
    );
    expect(
      replayAudits.items.some(
        (item) =>
          item.action === "control_plane.integration_dlq_messages_replayed" &&
          Array.isArray(item.metadata.messageIds) &&
          item.metadata.messageIds.includes("INTEGRATION_DISPATCH_DLQ:101"),
      ),
    ).toBe(true);

    await __resetIntegrationDlqBackendForTests();
  });

  test("integration dlq 查询与 replay 在后端不可用时返回 503，参数非法返回 400", async () => {
    const authHeaders = await resolveAuthHeaders();
    __setIntegrationDlqBackendForTests({
      async listMessages() {
        throw new Error("dlq backend unavailable");
      },
      async replayMessages() {
        throw new Error("dlq backend unavailable");
      },
    });

    const badQueryResponse = await app.request(
      "/api/v1/integrations/dlq/messages?limit=0",
      {
        headers: authHeaders,
      },
    );
    expect(badQueryResponse.status).toBe(400);

    const queryResponse = await app.request(
      "/api/v1/integrations/dlq/messages?limit=10",
      {
        headers: authHeaders,
      },
    );
    expect(queryResponse.status).toBe(503);

    const badReplayResponse = await app.request(
      "/api/v1/integrations/dlq/messages/replay",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          messageIds: [],
        }),
      },
    );
    expect(badReplayResponse.status).toBe(400);

    const replayResponse = await app.request(
      "/api/v1/integrations/dlq/messages/replay",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          messageIds: ["INTEGRATION_DISPATCH_DLQ:999"],
        }),
      },
    );
    expect(replayResponse.status).toBe(503);

    await __resetIntegrationDlqBackendForTests();
  });

  test("integration dlq recovery jobs 支持创建、列表、详情与完成审计", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const nonce = createNonce("integration-dlq-recovery");

    __setIntegrationDlqBackendForTests({
      async listMessages(input) {
        expect(input.tenantId).toBe(tenantId);
        return {
          items: [
            {
              messageId: "INTEGRATION_DISPATCH_DLQ:201",
              stream: "INTEGRATION_DISPATCH_DLQ",
              subject: "integration.alert.external_status_sync",
              eventType: "alert_external_status_sync",
              channel: "ticket",
              callbackId: `sync-result:${nonce}`,
              tenantId,
              alertId: `alert-${nonce}`,
              externalType: "ticket",
              externalId: `ticket-${nonce}`,
              failedAt: "2026-03-08T11:00:00.000Z",
              attempt: 2,
              error: "timeout",
              retryable: true,
              payload: {},
            },
          ],
          total: 1,
          filters: {
            eventType: input.eventType,
            channel: input.channel,
            callbackId: input.callbackId,
            alertId: input.alertId,
            limit: input.limit,
          },
        };
      },
      async replayMessages(input) {
        expect(input.tenantId).toBe(tenantId);
        expect(input.messageIds).toEqual(["INTEGRATION_DISPATCH_DLQ:201"]);
        return {
          replayedCount: 1,
          failedCount: 0,
          items: [
            {
              messageId: "INTEGRATION_DISPATCH_DLQ:201",
              status: "replayed",
            },
          ],
        };
      },
    });

    const createResponse = await app.request(
      "/api/v1/integrations/dlq/recovery-jobs",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          filters: {
            alertId: `alert-${nonce}`,
            limit: 10,
          },
        }),
      },
    );
    expect(createResponse.status).toBe(202);
    const created = (await createResponse.json()) as {
      id: string;
      status: string;
      summary: { total: number; replayed: number; failed: number };
      messageIds: string[];
    };
    expect(created.status).toBe("queued");
    expect(created.summary).toEqual({
      total: 1,
      replayed: 0,
      failed: 0,
    });

    await __drainIntegrationDlqRecoveryQueueForTests();

    const listResponse = await app.request(
      "/api/v1/integrations/dlq/recovery-jobs?status=completed&limit=10",
      {
        headers: authHeaders,
      },
    );
    expect(listResponse.status).toBe(200);
    const listBody = (await listResponse.json()) as {
      items: Array<{ id: string; status: string }>;
      total: number;
      filters: { status?: string; limit?: number };
    };
    expect(listBody.total).toBeGreaterThanOrEqual(1);
    expect(listBody.filters).toEqual({
      status: "completed",
      limit: 10,
    });
    expect(
      listBody.items.some((item) => item.id === created.id && item.status === "completed"),
    ).toBe(true);

    const detailResponse = await app.request(
      `/api/v1/integrations/dlq/recovery-jobs/${encodeURIComponent(created.id)}`,
      {
        headers: authHeaders,
      },
    );
    expect(detailResponse.status).toBe(200);
    const detailBody = (await detailResponse.json()) as {
      id: string;
      status: string;
      summary: { total: number; replayed: number; failed: number };
      items: Array<{ messageId: string; status: string }>;
    };
    expect(detailBody.id).toBe(created.id);
    expect(detailBody.status).toBe("completed");
    expect(detailBody.summary).toEqual({
      total: 1,
      replayed: 1,
      failed: 0,
    });
    expect(detailBody.items).toEqual([
      {
        messageId: "INTEGRATION_DISPATCH_DLQ:201",
        status: "replayed",
      },
    ]);

    const createdAudits = await queryAuditByAction(
      "control_plane.integration_dlq_recovery_job_created",
      created.id,
    );
    expect(
      createdAudits.items.some(
        (item) =>
          item.action === "control_plane.integration_dlq_recovery_job_created" &&
          item.metadata.jobId === created.id,
      ),
    ).toBe(true);

    const completedAudits = await queryAuditByAction(
      "control_plane.integration_dlq_recovery_job_completed",
      created.id,
    );
    expect(
      completedAudits.items.some(
        (item) =>
          item.action === "control_plane.integration_dlq_recovery_job_completed" &&
          item.metadata.jobId === created.id,
      ),
    ).toBe(true);

    await __resetIntegrationDlqBackendForTests();
  });

  test("integration alert failure report 支持 summary 与过滤条件", async () => {
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
      throw new Error("repository.appendAuditLog 不可用，无法验证失败审计报表。");
    }

    const nonce = createNonce("integration-failure-report");
    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:${nonce}:1`,
      action: "control_plane.alert_external_link_retry_requested",
      level: "info",
      detail: "retry requested",
      metadata: {
        alertId: `alert-${nonce}`,
        externalSystem: "ticket",
        requestId: `req-${nonce}-1`,
      },
    });
    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:${nonce}:2`,
      action: "control_plane.alert_external_link_retry_failed",
      level: "warning",
      detail: "retry failed",
      metadata: {
        alertId: `alert-${nonce}`,
        externalSystem: "ticket",
        failureStage: "dispatch_http",
        failureCode: "downstream_http_5xx",
        requestId: `req-${nonce}-2`,
      },
    });
    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:${nonce}:3`,
      action: "control_plane.integration_dlq_recovery_job_completed",
      level: "info",
      detail: "recovery completed",
      metadata: {
        jobId: `job-${nonce}`,
        externalSystem: "ticket",
        requestId: `req-${nonce}-3`,
      },
    });

    const response = await app.request(
      `/api/v1/integrations/failure-reports/alerts?externalSystem=ticket&limit=10`,
      {
        headers: authHeaders,
      },
    );
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      summary: {
        totalEvents: number;
        retryRequested: number;
        retryFailed: number;
        recoveryJobsCompleted: number;
      };
      items: Array<{
        actionType: string;
        alertId?: string;
        externalSystem?: string;
        stage?: string;
        code?: string;
        status: string;
      }>;
      filters: {
        from?: string;
        to?: string;
        externalSystem?: string;
        stage?: string;
        actionType?: string;
        limit?: number;
      };
    };
    expect(body.summary.totalEvents).toBeGreaterThanOrEqual(3);
    expect(body.summary.retryRequested).toBeGreaterThanOrEqual(1);
    expect(body.summary.retryFailed).toBeGreaterThanOrEqual(1);
    expect(body.summary.recoveryJobsCompleted).toBeGreaterThanOrEqual(1);
    expect(body.filters).toEqual({
      from: undefined,
      to: undefined,
      externalSystem: "ticket",
      stage: undefined,
      actionType: undefined,
      limit: 10,
    });
    expect(
      body.items.some(
        (item) =>
          item.actionType === "retry_failed" &&
          item.alertId === `alert-${nonce}` &&
          item.stage === "dispatch_http" &&
          item.code === "downstream_http_5xx" &&
          item.status === "failed",
      ),
    ).toBe(true);
  });

  test("integration alert failure trends 支持按日趋势与容量聚合", async () => {
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
        createdAt?: string;
      }) => Promise<unknown>;
    };
    if (typeof repositoryWithAudit.appendAuditLog !== "function") {
      throw new Error("repository.appendAuditLog 不可用，无法验证失败趋势报表。");
    }

    const nonce = createNonce("integration-failure-trends");
    const externalSystem = `ops-${nonce}`;
    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:${nonce}:1`,
      action: "control_plane.alert_external_link_retry_failed",
      level: "warning",
      detail: "retry failed",
      metadata: {
        alertId: `alert-${nonce}-1`,
        externalSystem,
        failureStage: "dispatch_http",
        failureCode: "downstream_http_5xx",
      },
      createdAt: "2026-03-01T10:00:00.000Z",
    });
    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:${nonce}:2`,
      action: "control_plane.integration_dlq_recovery_job_completed",
      level: "info",
      detail: "recovery completed",
      metadata: {
        alertId: `alert-${nonce}-1`,
        externalSystem,
        stage: "dispatch_publish",
      },
      createdAt: "2026-03-01T11:00:00.000Z",
    });
    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:${nonce}:3`,
      action: "control_plane.alert_external_link_retry_requested",
      level: "info",
      detail: "retry requested",
      metadata: {
        alertId: `alert-${nonce}-2`,
        externalSystem,
        stage: "publish",
      },
      createdAt: "2026-03-02T09:00:00.000Z",
    });

    const response = await app.request(
      `/api/v1/integrations/failure-reports/alerts/trends?externalSystem=${encodeURIComponent(externalSystem)}&top=2`,
      {
        headers: authHeaders,
      },
    );
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      summary: {
        totalEvents: number;
        requestedEvents: number;
        successEvents: number;
        failedEvents: number;
        days: number;
        averageEventsPerDay: number;
        peakDate?: string;
        peakCount: number;
      };
      daily: Array<{
        date: string;
        totalEvents: number;
        requestedEvents: number;
        successEvents: number;
        failedEvents: number;
        uniqueAlerts: number;
        retryRequested: number;
        retryFailed: number;
        recoveryJobsCompleted: number;
      }>;
      capacity: {
        externalSystems: Array<{
          name: string;
          totalEvents: number;
          failedEvents: number;
          successEvents: number;
          requestedEvents: number;
          uniqueAlerts: number;
        }>;
        stages: Array<{
          name: string;
          totalEvents: number;
        }>;
      };
      filters: {
        externalSystem?: string;
        top: number;
      };
    };

    expect(body.summary.totalEvents).toBeGreaterThanOrEqual(3);
    expect(body.summary.requestedEvents).toBeGreaterThanOrEqual(1);
    expect(body.summary.successEvents).toBeGreaterThanOrEqual(1);
    expect(body.summary.failedEvents).toBeGreaterThanOrEqual(1);
    expect(body.summary.days).toBeGreaterThanOrEqual(2);
    expect(body.summary.averageEventsPerDay).toBeGreaterThan(0);
    expect(body.summary.peakDate).toBe("2026-03-01");
    expect(body.summary.peakCount).toBeGreaterThanOrEqual(2);
    expect(body.filters.externalSystem).toBe(externalSystem);
    expect(body.filters.top).toBe(2);

    expect(
      body.daily.some(
        (item) =>
          item.date === "2026-03-01" &&
          item.totalEvents >= 2 &&
          item.failedEvents >= 1 &&
          item.successEvents >= 1 &&
          item.recoveryJobsCompleted >= 1,
      ),
    ).toBe(true);
    expect(
      body.daily.some(
        (item) =>
          item.date === "2026-03-02" &&
          item.totalEvents >= 1 &&
          item.requestedEvents >= 1 &&
          item.retryRequested >= 1,
      ),
    ).toBe(true);

    expect(
      body.capacity.externalSystems.some(
        (item) =>
          item.name === externalSystem &&
          item.totalEvents >= 2 &&
          item.failedEvents >= 1 &&
          item.successEvents >= 1 &&
          item.uniqueAlerts >= 1,
      ),
    ).toBe(true);
    expect(
      body.capacity.stages.some((item) => item.name === "dispatch_http" && item.totalEvents >= 1),
    ).toBe(true);
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

  test("alerts/orchestration executions 列表支持 escalated 与 escalationReason 过滤", async () => {
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const repository = getControlPlaneRepository();
    const nonce = createNonce("alerts-orchestration-executions-escalated");
    const ruleId = `rule-escalated-${nonce}`;

    const upsertRuleResponse = await app.request(
      `/api/v1/alerts/orchestration/rules/${encodeURIComponent(ruleId)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          name: `升级规则-${nonce}`,
          enabled: true,
          eventType: "alert",
          dedupeWindowSeconds: 0,
          suppressionWindowSeconds: 0,
          mergeWindowSeconds: 0,
          slaMinutes: 15,
          channels: ["ticket"],
        }),
      },
    );
    expect(upsertRuleResponse.status).toBe(200);

    await repository.createAlertOrchestrationExecutionLog(tenantId, {
      id: `exec-escalated-${nonce}`,
      ruleId,
      eventType: "alert",
      alertId: `alert-escalated-${nonce}`,
      severity: "critical",
      channels: ["ticket"],
      dispatchMode: "rule",
      simulated: false,
      metadata: {
        dispatchMode: "rule",
        escalated: true,
        escalationReason: "sla_timeout",
        escalationTargetChannels: ["ticket"],
        slaMinutes: 15,
      },
    });

    await repository.createAlertOrchestrationExecutionLog(tenantId, {
      id: `exec-normal-${nonce}`,
      ruleId,
      eventType: "alert",
      alertId: `alert-normal-${nonce}`,
      severity: "critical",
      channels: ["ticket"],
      dispatchMode: "rule",
      simulated: false,
      metadata: {
        dispatchMode: "rule",
      },
    });

    const response = await app.request(
      `/api/v1/alerts/orchestration/executions?ruleId=${encodeURIComponent(
        ruleId,
      )}&escalated=true&escalationReason=sla_timeout&limit=10`,
      {
        headers: authHeaders,
      },
    );
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      items: Array<{
        id: string;
        escalated: boolean;
        escalationReason?: string;
        escalationTargetChannels?: string[];
        slaMinutes?: number;
      }>;
      filters: {
        escalated?: boolean;
        escalationReason?: string;
      };
    };
    expect(body.filters.escalated).toBe(true);
    expect(body.filters.escalationReason).toBe("sla_timeout");
    expect(body.items).toHaveLength(1);
    expect(body.items[0]).toMatchObject({
      id: `exec-escalated-${nonce}`,
      escalated: true,
      escalationReason: "sla_timeout",
      escalationTargetChannels: ["ticket"],
      slaMinutes: 15,
    });
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

  test("system-config agent runtime 视图、配置快照与 heartbeat 回填 identity lastSeenAt", async () => {
    const nonce = createNonce("system-config-agent-runtime");
    const auth = await getDefaultAuthContext();

    const createTenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `SystemConfig Agent Runtime ${nonce}`,
        slug: `system-config-agent-runtime-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(createTenantResult, [201]);
    const tenantId = extractEntityId(createTenantResult.payload);
    if (!tenantId) {
      throw new Error("system-config agent runtime 测试租户创建失败。");
    }

    const tenantHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      auth.accessToken,
      auth.userId,
    );

    const createSourceResult = await createIdentitySourceByAuth(
      auth.accessToken,
      {
        tenantId,
        name: `Runtime Source ${nonce}`,
        location: `~/.codex/sessions/runtime-${nonce}`,
        accessMode: "hybrid",
      },
      auth.userId,
    );
    assertApiStatus(createSourceResult, [201]);
    const sourceId = extractEntityId(createSourceResult.payload);
    if (!sourceId) {
      throw new Error("system-config agent runtime source 创建失败。");
    }

    const createDeviceResult = await createTenantDeviceByAuth(
      auth.accessToken,
      {
        tenantId,
        name: `Runtime Device ${nonce}`,
        slug: `runtime-device-${nonce}`,
        hostname: `runtime-host-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(createDeviceResult, [201]);
    const deviceId = extractEntityId(createDeviceResult.payload);
    if (!deviceId) {
      throw new Error("system-config agent runtime device 创建失败。");
    }

    const createAgentResult = await createTenantAgentByAuth(
      auth.accessToken,
      {
        tenantId,
        name: `Runtime Agent ${nonce}`,
        slug: `runtime-agent-${nonce}`,
        agentId: `runtime-agent-${nonce}`,
        deviceId,
      },
      auth.userId,
    );
    assertApiStatus(createAgentResult, [201]);
    const agentId = extractEntityId(createAgentResult.payload);
    if (!agentId) {
      throw new Error("system-config agent runtime agent 创建失败。");
    }

    const createBindingResult = await createTenantSourceBindingByAuth(
      auth.accessToken,
      {
        tenantId,
        sourceId,
        agentId,
      },
      auth.userId,
    );
    assertApiStatus(createBindingResult, [201]);

    const heartbeatOccurredAt = new Date().toISOString();
    const heartbeatConfigFetchedAt = new Date(Date.now() - 30_000).toISOString();
    const heartbeatResponse = await app.request(
      "/api/v1/system/config/agent-heartbeat",
      jsonRequest(
        "POST",
        {
          agentId,
          sessionId: `session-${nonce}`,
          hostname: `daemon-host-${nonce}`,
          version: "0.2.0",
          daemon: true,
          occurredAt: heartbeatOccurredAt,
          configVersion: `cfg:${nonce}`,
          configFetchedAt: heartbeatConfigFetchedAt,
          heartbeatIntervalSec: 45,
          ingestProtocol: "grpc",
          ingestEndpoint: "127.0.0.1:9091",
          sourceCount: 1,
          sourceIds: [sourceId],
          lastIngestStatusCode: 202,
          lastAccepted: 5,
          lastRejected: 1,
        },
        tenantHeaders,
      ),
    );
    expect(heartbeatResponse.status).toBe(202);
    const heartbeatBody = (await heartbeatResponse.json()) as {
      agentId: string;
      tenantId: string;
      configVersion: string;
      occurredAt: string;
      receivedAt: string;
    };
    expect(heartbeatBody.agentId).toBe(agentId);
    expect(heartbeatBody.tenantId).toBe(tenantId);
    expect(heartbeatBody.configVersion).toBe(`cfg:${nonce}`);
    expect(heartbeatBody.occurredAt).toBe(heartbeatOccurredAt);
    expect(typeof heartbeatBody.receivedAt).toBe("string");

    const runtimeViewsResponse = await app.request(
      "/api/v1/system/config/agents/views",
      {
        headers: tenantHeaders,
      },
    );
    expect(runtimeViewsResponse.status).toBe(200);
    const runtimeViewsBody = (await runtimeViewsResponse.json()) as {
      items: Array<{
        agentId: string;
        displayName: string;
        hostname: string;
        version?: string;
        sourceIds: string[];
        sourceNames: string[];
        runtimeStatus: "online" | "stale" | "never_seen";
        lastHeartbeatAt: string | null;
        lastConfigVersion?: string;
        lastIngestStatusCode: number | null;
        lastAccepted: number;
        lastRejected: number;
      }>;
      total: number;
      generatedAt: string;
    };
    expect(runtimeViewsBody.total).toBe(1);
    const runtimeView = runtimeViewsBody.items[0];
    expect(runtimeView?.agentId).toBe(agentId);
    expect(runtimeView?.displayName).toBe(`Runtime Agent ${nonce}`);
    expect(runtimeView?.hostname).toBe(`daemon-host-${nonce}`);
    expect(runtimeView?.version).toBe("0.2.0");
    expect(runtimeView?.runtimeStatus).toBe("online");
    expect(runtimeView?.lastHeartbeatAt).toBe(heartbeatOccurredAt);
    expect(runtimeView?.lastConfigVersion).toBe(`cfg:${nonce}`);
    expect(runtimeView?.lastIngestStatusCode).toBe(202);
    expect(runtimeView?.lastAccepted).toBe(5);
    expect(runtimeView?.lastRejected).toBe(1);
    expect(runtimeView?.sourceIds).toEqual([sourceId]);
    expect(runtimeView?.sourceNames).toEqual([`Runtime Source ${nonce}`]);
    expect(typeof runtimeViewsBody.generatedAt).toBe("string");

    const runtimeConfigResponse = await app.request(
      `/api/v1/system/config/agent-runtime?agentId=${encodeURIComponent(agentId)}`,
      {
        headers: tenantHeaders,
      },
    );
    expect(runtimeConfigResponse.status).toBe(200);
    const runtimeConfigBody = (await runtimeConfigResponse.json()) as {
      tenantId: string;
      agent: {
        agentId: string;
        deviceId?: string;
        hostname: string;
        displayName: string;
      };
      runtime: {
        heartbeatIntervalSeconds: number;
        staleAfterSeconds: number;
        ingestProtocol: "http" | "grpc";
        sampleGenerateCount: number;
      };
      bindings: {
        sourceCount: number;
        sourceIds: string[];
        sources: Array<{
          sourceId: string;
          name: string;
          accessMode: string;
          enabled: boolean;
          location: string;
        }>;
      };
      configVersion: string;
      updatedAt: string;
    };
    expect(runtimeConfigBody.tenantId).toBe(tenantId);
    expect(runtimeConfigBody.agent.agentId).toBe(agentId);
    expect(runtimeConfigBody.agent.deviceId).toBe(deviceId);
    expect(runtimeConfigBody.bindings.sourceCount).toBe(1);
    expect(runtimeConfigBody.bindings.sourceIds).toEqual([sourceId]);
    expect(runtimeConfigBody.bindings.sources[0]?.name).toBe(`Runtime Source ${nonce}`);
    expect(runtimeConfigBody.runtime.ingestProtocol).toBe("http");
    expect(runtimeConfigBody.runtime.sampleGenerateCount).toBeGreaterThanOrEqual(1);
    expect(typeof runtimeConfigBody.configVersion).toBe("string");
    expect(typeof runtimeConfigBody.updatedAt).toBe("string");

    const listAgentsResult = await listTenantAgentsByAuth(
      auth.accessToken,
      tenantId,
      auth.userId,
    );
    assertApiStatus(listAgentsResult, [200]);
    const listedAgents = extractListItems(listAgentsResult.payload);
    const listedAgent = listedAgents.find(
      (item) => pickString(item, ["id", "agentId"]) === agentId,
    ) as Record<string, unknown> | undefined;
    expect(listedAgent?.lastSeenAt).toBe(heartbeatOccurredAt);

    const listDevicesResult = await listTenantDevicesByAuth(
      auth.accessToken,
      tenantId,
      auth.userId,
    );
    assertApiStatus(listDevicesResult, [200]);
    const listedDevices = extractListItems(listDevicesResult.payload);
    const listedDevice = listedDevices.find(
      (item) => pickString(item, ["id", "deviceId"]) === deviceId,
    ) as Record<string, unknown> | undefined;
    expect(listedDevice?.lastSeenAt).toBe(heartbeatOccurredAt);
  });

  test("system-config packages create/publish/watch/latest 支持租户隔离", async () => {
    const nonce = createNonce("system-config-package");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `SystemConfig Package A ${nonce}`,
        slug: `system-config-package-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("system-config package 租户 A 创建失败。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `SystemConfig Package B ${nonce}`,
        slug: `system-config-package-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("system-config package 租户 B 创建失败。");
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

    const badCreateResponse = await app.request(
      "/api/v1/system/config/packages",
      jsonRequest(
        "POST",
        {
          version: "",
        },
        tenantAHeaders,
      ),
    );
    expect(badCreateResponse.status).toBe(400);

    const createResponse = await app.request(
      "/api/v1/system/config/packages",
      jsonRequest(
        "POST",
        {
          version: `agent-policy-${nonce}`,
          issuedAt: "2026-03-08T08:00:00Z",
          signatureStatus: "verified",
          requiresApproval: false,
          targetSelectors: {
            agentIds: [`agent-${nonce}`],
            deviceIds: [`device-${nonce}`],
            channels: ["beta"],
            hostnames: [`host-${nonce}`],
          },
          payload: {
            rollout: "stage-a",
            mode: "observe",
          },
        },
        tenantAHeaders,
      ),
    );
    expect(createResponse.status).toBe(201);
    const created = (await createResponse.json()) as {
      packageId: string;
      tenantId: string;
      version: string;
      issuedAt: string;
      signatureStatus: string;
      requiresApproval: boolean;
      requiredApprovals: number;
      isPublished: boolean;
      publishedAt?: string;
      targetSelectors: {
        agentIds?: string[];
        deviceIds?: string[];
        channels?: string[];
        hostnames?: string[];
      };
      payload: Record<string, unknown>;
    };
    expect(created.tenantId).toBe(tenantAId);
    expect(created.version).toBe(`agent-policy-${nonce}`);
    expect(created.issuedAt).toBe("2026-03-08T08:00:00Z");
    expect(created.signatureStatus).toBe("verified");
    expect(created.requiresApproval).toBe(false);
    expect(created.requiredApprovals).toBe(0);
    expect(created.isPublished).toBe(false);
    expect(created.publishedAt).toBeUndefined();
    expect(created.targetSelectors.agentIds).toEqual([`agent-${nonce}`]);
    expect(created.targetSelectors.deviceIds).toEqual([`device-${nonce}`]);
    expect(created.targetSelectors.channels).toEqual(["beta"]);
    expect(created.targetSelectors.hostnames).toEqual([`host-${nonce}`]);
    expect(created.payload.rollout).toBe("stage-a");

    const listAResponse = await app.request(
      "/api/v1/system/config/packages?limit=10",
      {
        headers: tenantAHeaders,
      },
    );
    expect(listAResponse.status).toBe(200);
    const listABody = (await listAResponse.json()) as {
      items: Array<{
        packageId: string;
        version: string;
        requiresApproval: boolean;
        requiredApprovals: number;
        isPublished: boolean;
      }>;
      total: number;
      filters: { limit?: number };
    };
    expect(listABody.total).toBeGreaterThanOrEqual(1);
    expect(listABody.filters.limit).toBe(10);
    expect(
      listABody.items.some(
        (item) =>
          item.packageId === created.packageId &&
          item.version === `agent-policy-${nonce}` &&
          item.requiresApproval === false &&
          item.requiredApprovals === 0 &&
          item.isPublished === false,
      ),
    ).toBe(true);

    const detailAResponse = await app.request(
      `/api/v1/system/config/packages/${encodeURIComponent(created.packageId)}`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(detailAResponse.status).toBe(200);
    const detailABody = (await detailAResponse.json()) as {
      packageId: string;
      version: string;
      signatureStatus: string;
      requiresApproval: boolean;
      requiredApprovals: number;
      isPublished: boolean;
      publishedAt?: string;
      targetSelectors: {
        channels?: string[];
      };
      payload: Record<string, unknown>;
    };
    expect(detailABody.packageId).toBe(created.packageId);
    expect(detailABody.version).toBe(`agent-policy-${nonce}`);
    expect(detailABody.signatureStatus).toBe("verified");
    expect(detailABody.requiresApproval).toBe(false);
    expect(detailABody.requiredApprovals).toBe(0);
    expect(detailABody.isPublished).toBe(false);
    expect(detailABody.publishedAt).toBeUndefined();
    expect(detailABody.targetSelectors.channels).toEqual(["beta"]);
    expect(detailABody.payload.mode).toBe("observe");

    const watchBeforePublishResponse = await app.request(
      `/api/v1/system/config/packages/watch/latest?agentId=${encodeURIComponent(
        `agent-${nonce}`,
      )}&deviceId=${encodeURIComponent(
        `device-${nonce}`,
      )}&channel=beta&hostname=${encodeURIComponent(`host-${nonce}`)}`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(watchBeforePublishResponse.status).toBe(404);

    const publishResponse = await app.request(
      `/api/v1/system/config/packages/${encodeURIComponent(created.packageId)}/publish`,
      {
        method: "POST",
        headers: tenantAHeaders,
      },
    );
    expect(publishResponse.status).toBe(200);
    const published = (await publishResponse.json()) as {
      packageId: string;
      isPublished: boolean;
      publishedAt?: string;
      requiresApproval: boolean;
      requiredApprovals: number;
    };
    expect(published.packageId).toBe(created.packageId);
    expect(published.isPublished).toBe(true);
    expect(typeof published.publishedAt).toBe("string");
    expect(published.requiresApproval).toBe(false);
    expect(published.requiredApprovals).toBe(0);

    const createNonMatchResponse = await app.request(
      "/api/v1/system/config/packages",
      jsonRequest(
        "POST",
        {
          version: `agent-policy-other-${nonce}`,
          issuedAt: "2026-03-08T09:00:00Z",
          signatureStatus: "verified",
          targetSelectors: {
            channels: ["stable"],
            hostnames: [`host-${nonce}`],
          },
          payload: {
            rollout: "stage-b",
          },
        },
        tenantAHeaders,
      ),
    );
    expect(createNonMatchResponse.status).toBe(201);
    const nonMatching = (await createNonMatchResponse.json()) as {
      packageId: string;
    };
    const publishNonMatchResponse = await app.request(
      `/api/v1/system/config/packages/${encodeURIComponent(nonMatching.packageId)}/publish`,
      {
        method: "POST",
        headers: tenantAHeaders,
      },
    );
    expect(publishNonMatchResponse.status).toBe(200);

    const watchPublishedResponse = await app.request(
      `/api/v1/system/config/packages/watch/latest?agentId=${encodeURIComponent(
        `agent-${nonce}`,
      )}&deviceId=${encodeURIComponent(
        `device-${nonce}`,
      )}&channel=beta&hostname=${encodeURIComponent(`host-${nonce}`)}`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(watchPublishedResponse.status).toBe(200);
    const watchPublishedBody = (await watchPublishedResponse.json()) as {
      packageId: string;
      version: string;
      isPublished: boolean;
      publishedAt?: string;
      requiresApproval: boolean;
      requiredApprovals: number;
    };
    expect(watchPublishedBody.packageId).toBe(created.packageId);
    expect(watchPublishedBody.version).toBe(`agent-policy-${nonce}`);
    expect(watchPublishedBody.isPublished).toBe(true);
    expect(watchPublishedBody.requiresApproval).toBe(false);
    expect(watchPublishedBody.requiredApprovals).toBe(0);
    expect(typeof watchPublishedBody.publishedAt).toBe("string");

    const listBResponse = await app.request("/api/v1/system/config/packages", {
      headers: tenantBHeaders,
    });
    expect(listBResponse.status).toBe(200);
    const listBBody = (await listBResponse.json()) as {
      items: Array<{ packageId: string }>;
    };
    expect(
      listBBody.items.some((item) => item.packageId === created.packageId),
    ).toBe(false);

    const detailBResponse = await app.request(
      `/api/v1/system/config/packages/${encodeURIComponent(created.packageId)}`,
      {
        headers: tenantBHeaders,
      },
    );
    expect(detailBResponse.status).toBe(404);

    const watchBResponse = await app.request(
      `/api/v1/system/config/packages/watch/latest?agentId=${encodeURIComponent(
        `agent-${nonce}`,
      )}&deviceId=${encodeURIComponent(
        `device-${nonce}`,
      )}&channel=beta&hostname=${encodeURIComponent(`host-${nonce}`)}`,
      {
        headers: tenantBHeaders,
      },
    );
    expect(watchBResponse.status).toBe(404);

    const createAudits = await queryAuditByActionWithHeaders(
      "control_plane.system_config_package_created",
      created.packageId,
      tenantAHeaders,
    );
    const createAudit = createAudits.items.find(
      (item) =>
        item.action === "control_plane.system_config_package_created" &&
        item.metadata.packageId === created.packageId &&
        item.metadata.tenantId === tenantAId,
    );
    expect(createAudit).toBeDefined();

    const publishAudits = await queryAuditByActionWithHeaders(
      "control_plane.system_config_package_published",
      created.packageId,
      tenantAHeaders,
    );
    const publishAudit = publishAudits.items.find(
      (item) =>
        item.action === "control_plane.system_config_package_published" &&
        item.metadata.packageId === created.packageId &&
        item.metadata.tenantId === tenantAId,
    );
    expect(publishAudit).toBeDefined();
  });

  test("system-config packages 审批达标后才能 publish，重复审批写 updated 审计", async () => {
    const nonce = createNonce("system-config-package-approval");
    const auth = await getDefaultAuthContext();

    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `SystemConfig Package Approval ${nonce}`,
        slug: `system-config-package-approval-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("system-config approval tenant 创建失败。");
    }
    const tenantHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      auth.accessToken,
      auth.userId,
    );

    const createdResponse = await app.request(
      "/api/v1/system/config/packages",
      jsonRequest(
        "POST",
        {
          version: `agent-policy-approval-${nonce}`,
          signatureStatus: "verified",
          requiresApproval: true,
          requiredApprovals: 1,
          payload: {
            mode: "enforce",
          },
        },
        tenantHeaders,
      ),
    );
    expect(createdResponse.status).toBe(201);
    const created = (await createdResponse.json()) as {
      packageId: string;
      requiredApprovals: number;
      isPublished: boolean;
    };
    expect(created.requiredApprovals).toBe(1);
    expect(created.isPublished).toBe(false);

    const publishBeforeApprovalResponse = await app.request(
      `/api/v1/system/config/packages/${encodeURIComponent(created.packageId)}/publish`,
      {
        method: "POST",
        headers: tenantHeaders,
      },
    );
    expect(publishBeforeApprovalResponse.status).toBe(409);

    const createApprovalResponse = await app.request(
      `/api/v1/system/config/packages/${encodeURIComponent(created.packageId)}/approvals`,
      jsonRequest(
        "POST",
        {
          decision: "approved",
          comment: "looks good",
        },
        tenantHeaders,
      ),
    );
    expect(createApprovalResponse.status).toBe(201);
    const createdApproval = (await createApprovalResponse.json()) as {
      approvalId: string;
      packageId: string;
      approverUserId: string;
      decision: string;
    };
    if (!auth.userId) {
      throw new Error("system config package approval 测试缺少 userId。");
    }
    expect(createdApproval.packageId).toBe(created.packageId);
    expect(createdApproval.approverUserId).toBe(auth.userId);
    expect(createdApproval.decision).toBe("approved");

    const updateApprovalResponse = await app.request(
      `/api/v1/system/config/packages/${encodeURIComponent(created.packageId)}/approvals`,
      jsonRequest(
        "POST",
        {
          decision: "rejected",
          comment: "need changes",
        },
        tenantHeaders,
      ),
    );
    expect(updateApprovalResponse.status).toBe(200);
    const updatedApproval = (await updateApprovalResponse.json()) as {
      approvalId: string;
      decision: string;
    };
    expect(updatedApproval.approvalId).toBe(createdApproval.approvalId);
    expect(updatedApproval.decision).toBe("rejected");

    const publishAfterRejectedResponse = await app.request(
      `/api/v1/system/config/packages/${encodeURIComponent(created.packageId)}/publish`,
      {
        method: "POST",
        headers: tenantHeaders,
      },
    );
    expect(publishAfterRejectedResponse.status).toBe(409);

    const approveAgainResponse = await app.request(
      `/api/v1/system/config/packages/${encodeURIComponent(created.packageId)}/approvals`,
      jsonRequest(
        "POST",
        {
          decision: "approved",
        },
        tenantHeaders,
      ),
    );
    expect(approveAgainResponse.status).toBe(200);

    const listApprovalsResponse = await app.request(
      `/api/v1/system/config/packages/${encodeURIComponent(created.packageId)}/approvals`,
      {
        headers: tenantHeaders,
      },
    );
    expect(listApprovalsResponse.status).toBe(200);
    const approvalsBody = (await listApprovalsResponse.json()) as {
      items: Array<{ approvalId: string; decision: string }>;
      total: number;
    };
    expect(approvalsBody.total).toBe(1);
    expect(approvalsBody.items[0]?.approvalId).toBe(createdApproval.approvalId);
    expect(approvalsBody.items[0]?.decision).toBe("approved");

    const publishAfterApprovalResponse = await app.request(
      `/api/v1/system/config/packages/${encodeURIComponent(created.packageId)}/publish`,
      {
        method: "POST",
        headers: tenantHeaders,
      },
    );
    expect(publishAfterApprovalResponse.status).toBe(200);

    const approvalCreatedAudits = await queryAuditByActionWithHeaders(
      "control_plane.system_config_package_approval_created",
      createdApproval.approvalId,
      tenantHeaders,
    );
    expect(
      approvalCreatedAudits.items.some(
        (item) =>
          item.action === "control_plane.system_config_package_approval_created" &&
          item.metadata.approvalId === createdApproval.approvalId,
      ),
    ).toBe(true);

    const approvalUpdatedAudits = await queryAuditByActionWithHeaders(
      "control_plane.system_config_package_approval_updated",
      createdApproval.approvalId,
      tenantHeaders,
    );
    expect(
      approvalUpdatedAudits.items.some(
        (item) =>
          item.action === "control_plane.system_config_package_approval_updated" &&
          item.metadata.approvalId === createdApproval.approvalId,
      ),
    ).toBe(true);
  });

  test("system agent releases create/list/detail/check 支持租户隔离与更新检查", async () => {
    const nonce = createNonce("agent-release");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Agent Release A ${nonce}`,
        slug: `agent-release-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("agent release 租户 A 创建失败。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Agent Release B ${nonce}`,
        slug: `agent-release-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("agent release 租户 B 创建失败。");
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

    const badCreateResponse = await app.request(
      "/api/v1/system/agent-releases",
      jsonRequest(
        "POST",
        {
          version: "",
          artifacts: [],
        },
        tenantAHeaders,
      ),
    );
    expect(badCreateResponse.status).toBe(400);

    const createV1Response = await app.request(
      "/api/v1/system/agent-releases",
      jsonRequest(
        "POST",
        {
          version: "1.0.0",
          channel: "stable",
          notes: "首个稳定版本",
          publishedAt: "2026-03-08T08:00:00Z",
          artifacts: [
            {
              os: "darwin",
              arch: "arm64",
              downloadUrl: "https://downloads.example.com/agent-1.0.0-darwin-arm64.zip",
              checksumSha256: "sha256-darwin-arm64-v1",
              signature: "c2lnbmF0dXJlLWRhcmd3aW4tYXJtNjQtdjE=",
              signatureAlgorithm: "ed25519",
              fileName: "agent-1.0.0-darwin-arm64.zip",
              installHint: "解压后覆盖 /usr/local/bin/agent",
            },
            {
              os: "linux",
              arch: "amd64",
              downloadUrl: "https://downloads.example.com/agent-1.0.0-linux-amd64.tar.gz",
              checksumSha256: "sha256-linux-amd64-v1",
              fileName: "agent-1.0.0-linux-amd64.tar.gz",
              installHint: "解压后覆盖 /usr/local/bin/agent",
            },
          ],
        },
        tenantAHeaders,
      ),
    );
    expect(createV1Response.status).toBe(201);
    const releaseV1 = (await createV1Response.json()) as {
      releaseId: string;
      tenantId: string;
      version: string;
      channel: string;
      publishedAt: string;
      artifacts: Array<{ os: string; arch: string; fileName?: string }>;
    };
    expect(releaseV1.tenantId).toBe(tenantAId);
    expect(releaseV1.version).toBe("1.0.0");
    expect(releaseV1.channel).toBe("stable");
    expect(releaseV1.publishedAt).toBe("2026-03-08T08:00:00Z");
    expect(
      releaseV1.artifacts.some(
        (item) => item.os === "darwin" && item.arch === "arm64",
      ),
    ).toBe(true);

    const createV11Response = await app.request(
      "/api/v1/system/agent-releases",
      jsonRequest(
        "POST",
        {
          version: "1.1.0",
          channel: "stable",
          notes: "新增 update check 兼容字段",
          publishedAt: "2026-03-08T09:00:00Z",
          artifacts: [
            {
              os: "darwin",
              arch: "arm64",
              downloadUrl: "https://downloads.example.com/agent-1.1.0-darwin-arm64.zip",
              checksumSha256: "sha256-darwin-arm64-v11",
              signature: "c2lnbmF0dXJlLWRhcmd3aW4tYXJtNjQtdjEx",
              signatureAlgorithm: "ed25519",
              rolloutRing: "stable",
              rolloutPercentage: 100,
              minAgentVersion: "1.0.0",
              fileName: "agent-1.1.0-darwin-arm64.zip",
              installHint: "解压后覆盖 /usr/local/bin/agent",
            },
          ],
        },
        tenantAHeaders,
      ),
    );
    expect(createV11Response.status).toBe(201);
    const releaseV11 = (await createV11Response.json()) as {
      releaseId: string;
      version: string;
      artifacts: Array<{ fileName?: string }>;
    };
    expect(releaseV11.version).toBe("1.1.0");
    expect(releaseV11.artifacts[0]?.fileName).toBe(
      "agent-1.1.0-darwin-arm64.zip",
    );

    const createTenantBResponse = await app.request(
      "/api/v1/system/agent-releases",
      jsonRequest(
        "POST",
        {
          version: "9.9.9",
          channel: "stable",
          publishedAt: "2026-03-08T10:00:00Z",
          artifacts: [
            {
              os: "darwin",
              arch: "arm64",
              downloadUrl: "https://downloads.example.com/agent-9.9.9-darwin-arm64.zip",
            },
          ],
        },
        tenantBHeaders,
      ),
    );
    expect(createTenantBResponse.status).toBe(201);

    const listAResponse = await app.request(
      "/api/v1/system/agent-releases?limit=10&channel=stable&os=darwin&arch=arm64",
      {
        headers: tenantAHeaders,
      },
    );
    expect(listAResponse.status).toBe(200);
    const listABody = (await listAResponse.json()) as {
      items: Array<{ releaseId: string; version: string }>;
      total: number;
      filters: { limit?: number; channel?: string; os?: string; arch?: string };
    };
    expect(listABody.total).toBe(2);
    expect(listABody.filters.limit).toBe(10);
    expect(listABody.filters.channel).toBe("stable");
    expect(listABody.items[0]?.version).toBe("1.1.0");
    expect(
      listABody.items.some((item) => item.releaseId === releaseV1.releaseId),
    ).toBe(true);

    const detailAResponse = await app.request(
      `/api/v1/system/agent-releases/${encodeURIComponent(releaseV11.releaseId)}`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(detailAResponse.status).toBe(200);
    const detailABody = (await detailAResponse.json()) as {
      releaseId: string;
      version: string;
      channel: string;
      notes?: string;
      artifacts: Array<{
        downloadUrl: string;
        signatureAlgorithm?: string;
        rolloutRing?: string;
        rolloutPercentage?: number;
        minAgentVersion?: string;
      }>;
    };
    expect(detailABody.releaseId).toBe(releaseV11.releaseId);
    expect(detailABody.version).toBe("1.1.0");
    expect(detailABody.channel).toBe("stable");
    expect(detailABody.notes).toContain("update check");
    expect(detailABody.artifacts[0]?.downloadUrl).toContain("1.1.0");
    expect(detailABody.artifacts[0]?.signatureAlgorithm).toBe("ed25519");
    expect(detailABody.artifacts[0]?.rolloutRing).toBe("stable");
    expect(detailABody.artifacts[0]?.rolloutPercentage).toBe(100);
    expect(detailABody.artifacts[0]?.minAgentVersion).toBe("1.0.0");

    const checkResponse = await app.request(
      "/api/v1/system/agent-releases/check?currentVersion=1.0.0&channel=stable&os=darwin&arch=arm64",
      {
        headers: tenantAHeaders,
      },
    );
    expect(checkResponse.status).toBe(200);
    const checkBody = (await checkResponse.json()) as {
      currentVersion: string;
      channel: string;
      updateAvailable: boolean;
      comparison: string;
      latestRelease: {
        releaseId: string;
        version: string;
      } | null;
      selectedArtifact: {
        os: string;
        arch: string;
        fileName?: string;
        signatureAlgorithm?: string;
        rolloutRing?: string;
        rolloutPercentage?: number;
        minAgentVersion?: string;
      } | null;
      instructions: string;
    };
    expect(checkBody.currentVersion).toBe("1.0.0");
    expect(checkBody.channel).toBe("stable");
    expect(checkBody.updateAvailable).toBe(true);
    expect(checkBody.comparison).toBe("upgrade_available");
    expect(checkBody.latestRelease?.releaseId).toBe(releaseV11.releaseId);
    expect(checkBody.latestRelease?.version).toBe("1.1.0");
    expect(checkBody.selectedArtifact?.os).toBe("darwin");
    expect(checkBody.selectedArtifact?.arch).toBe("arm64");
    expect(checkBody.selectedArtifact?.fileName).toBe(
      "agent-1.1.0-darwin-arm64.zip",
    );
    expect(checkBody.selectedArtifact?.signatureAlgorithm).toBe("ed25519");
    expect(checkBody.selectedArtifact?.rolloutRing).toBe("stable");
    expect(checkBody.selectedArtifact?.rolloutPercentage).toBe(100);
    expect(checkBody.selectedArtifact?.minAgentVersion).toBe("1.0.0");
    expect(checkBody.instructions).toContain("不执行真实下载升级");

    const badSignatureAlgorithmResponse = await app.request(
      "/api/v1/system/agent-releases",
      jsonRequest(
        "POST",
        {
          version: "1.2.0",
          channel: "stable",
          artifacts: [
            {
              os: "linux",
              arch: "amd64",
              downloadUrl: "https://downloads.example.com/agent-1.2.0-linux-amd64.tar.gz",
              signature: "c2lnbmF0dXJlLWJhZA==",
              signatureAlgorithm: "rsa-pss",
            },
          ],
        },
        tenantAHeaders,
      ),
    );
    expect(badSignatureAlgorithmResponse.status).toBe(400);

    const listBResponse = await app.request("/api/v1/system/agent-releases", {
      headers: tenantBHeaders,
    });
    expect(listBResponse.status).toBe(200);
    const listBBody = (await listBResponse.json()) as {
      items: Array<{ releaseId: string }>;
      total: number;
    };
    expect(listBBody.total).toBe(1);
    expect(
      listBBody.items.some((item) => item.releaseId === releaseV11.releaseId),
    ).toBe(false);

    const detailBResponse = await app.request(
      `/api/v1/system/agent-releases/${encodeURIComponent(releaseV11.releaseId)}`,
      {
        headers: tenantBHeaders,
      },
    );
    expect(detailBResponse.status).toBe(404);

    const createAudits = await queryAuditByActionWithHeaders(
      "control_plane.agent_release_created",
      releaseV11.releaseId,
      tenantAHeaders,
    );
    const createAudit = createAudits.items.find(
      (item) =>
        item.action === "control_plane.agent_release_created" &&
        item.metadata.releaseId === releaseV11.releaseId &&
        item.metadata.tenantId === tenantAId &&
        item.metadata.channel === "stable",
    );
    expect(createAudit).toBeDefined();
  });

  test("system agent releases check 支持 rollout ring/percentage/minAgentVersion 筛选", async () => {
    const nonce = createNonce("agent-release-rollout");
    const auth = await getDefaultAuthContext();

    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Agent Release Rollout ${nonce}`,
        slug: `agent-release-rollout-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("agent rollout tenant 创建失败。");
    }
    const tenantHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      auth.accessToken,
      auth.userId,
    );

    const rolloutAgentId = `agent-${nonce}`;
    const rolloutDeviceId = `device-${nonce}`;
    const rolloutHostname = `host-${nonce}`;
    const rolloutSeed = `${tenantId}:${rolloutDeviceId}`;
    const rolloutBucket =
      Number.parseInt(
        createHash("sha256").update(rolloutSeed).digest("hex").slice(0, 8),
        16,
      ) % 100;
    const matchedPercentage = Math.min(100, rolloutBucket + 1);

    const createRolloutReleaseResponse = await app.request(
      "/api/v1/system/agent-releases",
      jsonRequest(
        "POST",
        {
          version: "2.0.0",
          channel: "stable",
          artifacts: [
            {
              os: "darwin",
              arch: "arm64",
              downloadUrl: "https://downloads.example.com/agent-2.0.0-darwin-arm64.zip",
              rolloutRing: "beta-ring",
              rolloutPercentage: matchedPercentage,
              minAgentVersion: "1.5.0",
            },
          ],
        },
        tenantHeaders,
      ),
    );
    expect(createRolloutReleaseResponse.status).toBe(201);

    const noRingResponse = await app.request(
      "/api/v1/system/agent-releases/check?currentVersion=1.5.0&channel=stable&os=darwin&arch=arm64",
      {
        headers: tenantHeaders,
      },
    );
    expect(noRingResponse.status).toBe(200);
    const noRingBody = (await noRingResponse.json()) as {
      comparison: string;
      updateAvailable: boolean;
      latestRelease: unknown;
      selectedArtifact: unknown;
      evaluatedRing?: string;
      rolloutBucket?: number;
      selectionReason?: string;
    };
    expect(noRingBody.comparison).toBe("no_release");
    expect(noRingBody.updateAvailable).toBe(false);
    expect(noRingBody.latestRelease).toBeNull();
    expect(noRingBody.selectedArtifact).toBeNull();
    expect(noRingBody.evaluatedRing).toBe("stable");
    expect(typeof noRingBody.rolloutBucket).toBe("number");
    expect(noRingBody.selectionReason).toBe("ring_mismatch");

    const minVersionMissResponse = await app.request(
      `/api/v1/system/agent-releases/check?currentVersion=1.4.9&channel=stable&os=darwin&arch=arm64&ring=beta-ring&agentId=${encodeURIComponent(
        rolloutAgentId,
      )}&deviceId=${encodeURIComponent(
        rolloutDeviceId,
      )}&hostname=${encodeURIComponent(rolloutHostname)}`,
      {
        headers: tenantHeaders,
      },
    );
    expect(minVersionMissResponse.status).toBe(200);
    const minVersionMissBody = (await minVersionMissResponse.json()) as {
      comparison: string;
      selectedArtifact: unknown;
      selectionReason?: string;
    };
    expect(minVersionMissBody.comparison).toBe("no_release");
    expect(minVersionMissBody.selectedArtifact).toBeNull();
    expect(minVersionMissBody.selectionReason).toBe(
      "min_agent_version_blocked",
    );

    const matchedResponse = await app.request(
      `/api/v1/system/agent-releases/check?currentVersion=1.5.0&channel=stable&os=darwin&arch=arm64&ring=beta-ring&agentId=${encodeURIComponent(
        rolloutAgentId,
      )}&deviceId=${encodeURIComponent(
        rolloutDeviceId,
      )}&hostname=${encodeURIComponent(rolloutHostname)}`,
      {
        headers: tenantHeaders,
      },
    );
    expect(matchedResponse.status).toBe(200);
    const matchedBody = (await matchedResponse.json()) as {
      comparison: string;
      updateAvailable: boolean;
      latestRelease: { version: string } | null;
      selectedArtifact: {
        rolloutRing?: string;
        rolloutPercentage?: number;
        minAgentVersion?: string;
      } | null;
      evaluatedRing?: string;
      rolloutBucket?: number;
      selectionReason?: string;
    };
    expect(matchedBody.comparison).toBe("upgrade_available");
    expect(matchedBody.updateAvailable).toBe(true);
    expect(matchedBody.latestRelease?.version).toBe("2.0.0");
    expect(matchedBody.selectedArtifact?.rolloutRing).toBe("beta-ring");
    expect(matchedBody.selectedArtifact?.rolloutPercentage).toBe(
      matchedPercentage,
    );
    expect(matchedBody.selectedArtifact?.minAgentVersion).toBe("1.5.0");
    expect(matchedBody.evaluatedRing).toBe("beta-ring");
    expect(typeof matchedBody.rolloutBucket).toBe("number");
    expect(matchedBody.selectionReason).toBe("matched");
  });

  test("system agent releases check batch 支持多 sample 返回与参数校验", async () => {
    const nonce = createNonce("agent-release-batch");
    const auth = await getDefaultAuthContext();
    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Agent Release Batch ${nonce}`,
        slug: `agent-release-batch-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("agent release batch tenant 创建失败。");
    }
    const tenantHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      auth.accessToken,
      auth.userId,
    );

    const createResponse = await app.request(
      "/api/v1/system/agent-releases",
      jsonRequest(
        "POST",
        {
          version: "3.0.0",
          channel: "stable",
          artifacts: [
            {
              os: "darwin",
              arch: "amd64",
              downloadUrl: "https://downloads.example.com/agent-darwin-amd64",
              rolloutRing: "beta-ring",
              rolloutPercentage: 100,
              minAgentVersion: "1.0.0",
            },
          ],
        },
        tenantHeaders,
      ),
    );
    expect(createResponse.status).toBe(201);

    const badResponse = await app.request(
      "/api/v1/system/agent-releases/check/batch",
      jsonRequest(
        "POST",
        {
          channel: "stable",
          os: "darwin",
          arch: "amd64",
          samples: [],
        },
        tenantHeaders,
      ),
    );
    expect(badResponse.status).toBe(400);

    const batchResponse = await app.request(
      "/api/v1/system/agent-releases/check/batch",
      jsonRequest(
        "POST",
        {
          channel: "stable",
          os: "darwin",
          arch: "amd64",
          samples: [
            {
              label: "stable-default",
              currentVersion: "1.0.0",
              deviceId: "device-stable",
              ring: "stable",
            },
            {
              label: "beta-ring-1",
              currentVersion: "1.0.0",
              deviceId: "device-beta",
              ring: "beta-ring",
            },
          ],
        },
        tenantHeaders,
      ),
    );
    expect(batchResponse.status).toBe(200);
    const batchBody = (await batchResponse.json()) as {
      items: Array<{
        label: string;
        comparison: string;
        evaluatedRing?: string;
        rolloutBucket?: number;
        selectionReason?: string;
        selectedArtifact: null | {
          rolloutRing?: string;
          rolloutPercentage?: number;
        };
      }>;
      total: number;
    };
    expect(batchBody.total).toBe(2);
    expect(batchBody.items.map((item) => item.label)).toEqual([
      "stable-default",
      "beta-ring-1",
    ]);
    expect(batchBody.items[0]?.comparison).toBe("no_release");
    expect(batchBody.items[0]?.evaluatedRing).toBe("stable");
    expect(typeof batchBody.items[0]?.rolloutBucket).toBe("number");
    expect(batchBody.items[0]?.selectionReason).toBe("ring_mismatch");
    expect(batchBody.items[0]?.selectedArtifact).toBeNull();
    expect(batchBody.items[1]?.comparison).toBe("upgrade_available");
    expect(batchBody.items[1]?.evaluatedRing).toBe("beta-ring");
    expect(batchBody.items[1]?.selectionReason).toBe("matched");
    expect(batchBody.items[1]?.selectedArtifact?.rolloutRing).toBe("beta-ring");
    expect(batchBody.items[1]?.selectedArtifact?.rolloutPercentage).toBe(100);
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

  test("POST/GET /api/v1/audits/legal-holds 与 release 支持最小闭环并写入审计", async () => {
    const nonce = createNonce("audit-legal-hold-lifecycle");
    const authHeaders = await resolveAuthHeaders();

    const createResponse = await app.request("/api/v1/audits/legal-holds", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        resourceType: "audit_export",
        resourceId: `audit-export-${nonce}`,
        reason: `preserve export ${nonce}`,
      }),
    });
    const created = (await createResponse.json()) as {
      id: string;
      resourceType: string;
      resourceId: string;
      reason: string;
      releasedAt?: string;
    };
    expect(createResponse.status).toBe(201);
    expect(created.resourceType).toBe("audit_export");
    expect(created.resourceId).toBe(`audit-export-${nonce}`);
    expect(created.reason).toContain(nonce);
    expect(created.releasedAt).toBeUndefined();

    const listActiveResponse = await app.request(
      `/api/v1/audits/legal-holds?resourceType=audit_export&resourceId=${encodeURIComponent(
        `audit-export-${nonce}`,
      )}&active=true&limit=10`,
      {
        headers: authHeaders,
      },
    );
    const listActiveBody = (await listActiveResponse.json()) as {
      items: Array<{ id: string; resourceId: string; releasedAt?: string }>;
      total: number;
      filters: {
        resourceType?: string;
        resourceId?: string;
        active?: boolean;
        limit?: number;
      };
    };
    expect(listActiveResponse.status).toBe(200);
    expect(listActiveBody.total).toBeGreaterThanOrEqual(1);
    expect(listActiveBody.filters.resourceType).toBe("audit_export");
    expect(listActiveBody.filters.resourceId).toBe(`audit-export-${nonce}`);
    expect(listActiveBody.filters.active).toBe(true);
    expect(
      listActiveBody.items.some(
        (item) =>
          item.id === created.id &&
          item.resourceId === `audit-export-${nonce}` &&
          !item.releasedAt,
      ),
    ).toBe(true);

    const releaseResponse = await app.request(
      `/api/v1/audits/legal-holds/${encodeURIComponent(created.id)}/release`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          reason: `release export ${nonce}`,
        }),
      },
    );
    const released = (await releaseResponse.json()) as {
      id: string;
      resourceId: string;
      releasedAt?: string;
      releaseReason?: string;
    };
    expect(releaseResponse.status).toBe(200);
    expect(released.id).toBe(created.id);
    expect(released.resourceId).toBe(`audit-export-${nonce}`);
    expect(released.releasedAt).toBeTruthy();
    expect(released.releaseReason).toBe(`release export ${nonce}`);

    const listReleasedResponse = await app.request(
      `/api/v1/audits/legal-holds?resourceType=audit_export&resourceId=${encodeURIComponent(
        `audit-export-${nonce}`,
      )}&active=false&limit=10`,
      {
        headers: authHeaders,
      },
    );
    const listReleasedBody = (await listReleasedResponse.json()) as {
      items: Array<{ id: string; releasedAt?: string }>;
      total: number;
      filters: { active?: boolean };
    };
    expect(listReleasedResponse.status).toBe(200);
    expect(listReleasedBody.filters.active).toBe(false);
    expect(
      listReleasedBody.items.some(
        (item) => item.id === created.id && Boolean(item.releasedAt),
      ),
    ).toBe(true);

    const createAudits = await queryAuditByAction(
      "audit.legal_hold.create",
      `audit-export-${nonce}`,
    );
    expect(
      createAudits.items.some(
        (item) =>
          item.metadata.resourceType === "audit_export" &&
          item.metadata.resourceId === `audit-export-${nonce}`,
      ),
    ).toBe(true);

    const releaseAudits = await queryAuditByAction(
      "audit.legal_hold.release",
      created.id,
    );
    expect(
      releaseAudits.items.some(
        (item) =>
          item.metadata.legalHoldId === created.id &&
          item.metadata.resourceId === `audit-export-${nonce}`,
      ),
    ).toBe(true);
  });

  test("DELETE /api/v1/audits/:id 在 audit Legal Hold 生效时返回 409，释放后可删除", async () => {
    const nonce = createNonce("audit-legal-hold-delete");
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
      throw new Error("repository.appendAuditLog 不可用，无法验证 audit Legal Hold 删除保护。");
    }

    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:audit-delete-seed:${nonce}`,
      action: "test.audit.delete_seed",
      level: "warning",
      detail: `audit delete seed ${nonce}`,
      metadata: {
        nonce,
        route: "/api/v1/audits",
      },
    });

    const seededAudits = await queryAuditByAction("test.audit.delete_seed", nonce);
    const seededAudit = seededAudits.items.find((item) =>
      auditMatchesKeyword(item, "test.audit.delete_seed", nonce),
    );
    const auditId = seededAudit?.id;
    expect(auditId).toBeTruthy();
    if (!auditId) {
      throw new Error("未找到用于删除保护测试的 auditId。");
    }

    const createHoldResponse = await app.request("/api/v1/audits/legal-holds", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        resourceType: "audit",
        resourceId: auditId,
        reason: `hold audit ${nonce}`,
      }),
    });
    const holdBody = (await createHoldResponse.json()) as { id: string };
    expect(createHoldResponse.status).toBe(201);

    const deleteBlockedResponse = await app.request(
      `/api/v1/audits/${encodeURIComponent(auditId)}`,
      {
        method: "DELETE",
        headers: authHeaders,
      },
    );
    const deleteBlockedBody = (await deleteBlockedResponse.json()) as {
      message: string;
      legalHold: { id: string; resourceType: string; resourceId: string };
    };
    expect(deleteBlockedResponse.status).toBe(409);
    expect(deleteBlockedBody.legalHold.id).toBe(holdBody.id);
    expect(deleteBlockedBody.legalHold.resourceType).toBe("audit");
    expect(deleteBlockedBody.legalHold.resourceId).toBe(auditId);

    const blockedAudits = await queryAuditByAction("audit.delete_blocked", auditId);
    expect(
      blockedAudits.items.some(
        (item) =>
          item.metadata.auditId === auditId &&
          item.metadata.legalHoldId === holdBody.id,
      ),
    ).toBe(true);

    const releaseResponse = await app.request(
      `/api/v1/audits/legal-holds/${encodeURIComponent(holdBody.id)}/release`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({ reason: `release audit ${nonce}` }),
      },
    );
    expect(releaseResponse.status).toBe(200);

    const deleteResponse = await app.request(
      `/api/v1/audits/${encodeURIComponent(auditId)}`,
      {
        method: "DELETE",
        headers: authHeaders,
      },
    );
    expect(deleteResponse.status).toBe(204);

    const deleteAudits = await queryAuditByAction("audit.delete", auditId);
    expect(
      deleteAudits.items.some((item) => item.metadata.auditId === auditId),
    ).toBe(true);
  });

  test("GET /api/v1/audits/export 在 audit_export Legal Hold 生效时返回 409，释放后返回目标元信息", async () => {
    const nonce = createNonce("audit-export-legal-hold");
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const exportResourceId = `audit-export-target-${nonce}`;
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
      throw new Error("repository.appendAuditLog 不可用，无法验证 audit export Legal Hold。");
    }

    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:audit-export-seed:${nonce}`,
      action: "test.audit.exportable_legal_hold",
      level: "warning",
      detail: `audit export legal hold seed ${nonce}`,
      metadata: {
        nonce,
        route: "/api/v1/audits/export",
      },
    });

    const createHoldResponse = await app.request("/api/v1/audits/legal-holds", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...authHeaders,
      },
      body: JSON.stringify({
        resourceType: "audit_export",
        resourceId: exportResourceId,
        reason: `preserve export ${nonce}`,
      }),
    });
    const holdBody = (await createHoldResponse.json()) as { id: string };
    expect(createHoldResponse.status).toBe(201);

    const blockedResponse = await app.request(
      `/api/v1/audits/export?format=json&action=test.audit.exportable_legal_hold&keyword=${encodeURIComponent(
        nonce,
      )}&resourceId=${encodeURIComponent(exportResourceId)}`,
      {
        headers: authHeaders,
      },
    );
    const blockedBody = (await blockedResponse.json()) as {
      message: string;
      legalHold: { id: string; resourceId: string; resourceType: string };
    };
    expect(blockedResponse.status).toBe(409);
    expect(blockedBody.legalHold.id).toBe(holdBody.id);
    expect(blockedBody.legalHold.resourceType).toBe("audit_export");
    expect(blockedBody.legalHold.resourceId).toBe(exportResourceId);

    const blockedAudits = await queryAuditByAction("audit.export_blocked", exportResourceId);
    expect(
      blockedAudits.items.some(
        (item) =>
          item.metadata.resourceId === exportResourceId &&
          item.metadata.legalHoldId === holdBody.id,
      ),
    ).toBe(true);

    const releaseResponse = await app.request(
      `/api/v1/audits/legal-holds/${encodeURIComponent(holdBody.id)}/release`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({ reason: `release export ${nonce}` }),
      },
    );
    expect(releaseResponse.status).toBe(200);

    const exportResponse = await app.request(
      `/api/v1/audits/export?format=json&action=test.audit.exportable_legal_hold&keyword=${encodeURIComponent(
        nonce,
      )}&resourceId=${encodeURIComponent(exportResourceId)}`,
      {
        headers: authHeaders,
      },
    );
    const exportBody = (await exportResponse.json()) as {
      format: string;
      items: Array<{ action: string }>;
      targetResource: {
        resourceType: string;
        resourceId: string | null;
        legalHold: unknown;
      };
    };
    expect(exportResponse.status).toBe(200);
    expect(exportBody.format).toBe("json");
    expect(
      exportBody.items.some(
        (item) => item.action === "test.audit.exportable_legal_hold",
      ),
    ).toBe(true);
    expect(exportBody.targetResource.resourceType).toBe("audit_export");
    expect(exportBody.targetResource.resourceId).toBe(exportResourceId);
    expect(exportBody.targetResource.legalHold).toBeNull();
    expect(exportResponse.headers.get("x-agentledger-resource-id")).toBe(exportResourceId);
    expect(exportResponse.headers.get("x-agentledger-legal-hold-status")).toBe("none");
  });

  test("GET /api/v1/audits/export 支持 DLP block/redact", async () => {
    const auth = await getDefaultAuthContext();
    const headers = await resolveAuthHeaders(auth.accessToken, auth.userId);
    const tenantId = resolveTenantIdFromAuthHeaders(headers);
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
      throw new Error("repository.appendAuditLog 不可用，无法验证 DLP export。");
    }
    const nonce = createNonce("audit-export-dlp");
    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:${nonce}`,
      action: "test.audit.export_dlp",
      level: "warning",
      detail: `user email is secret-${nonce}@example.com and token=sk_${nonce}abc123`,
      metadata: {
        keyword: `dlp-${nonce}`,
      },
    });

    const blockResponse = await app.request(
      `/api/v1/audits/export?format=json&action=test.audit.export_dlp&keyword=${encodeURIComponent(`dlp-${nonce}`)}&dlpMode=block`,
      { headers },
    );
    expect(blockResponse.status).toBe(422);

    const redactResponse = await app.request(
      `/api/v1/audits/export?format=json&action=test.audit.export_dlp&keyword=${encodeURIComponent(`dlp-${nonce}`)}&dlpMode=redact`,
      { headers },
    );
    expect(redactResponse.status).toBe(200);
    const redactBody = (await redactResponse.json()) as {
      items: Array<{ detail: string }>;
    };
    expect(redactBody.items[0]?.detail).toContain("[REDACTED_EMAIL]");
    expect(redactBody.items[0]?.detail).toContain("[REDACTED_SECRET]");
  });

  test("AUDIT_DLP_EXTRA_PATTERNS_JSON 非法输入会被忽略，DLP block 不误杀", async () => {
    const auth = await getDefaultAuthContext();
    const headers = await resolveAuthHeaders(auth.accessToken, auth.userId);
    const tenantId = resolveTenantIdFromAuthHeaders(headers);
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
      throw new Error("repository.appendAuditLog 不可用，无法验证 DLP extra patterns。");
    }

    const nonce = createNonce("audit-export-dlp-extra-invalid");
    const needle = `custom-sensitive-${nonce}`;
    const keyword = `dlp-extra-invalid-${nonce}`;
    const action = "test.audit.export_dlp_extra_invalid";
    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:${nonce}`,
      action,
      level: "info",
      detail: `this audit contains ${needle} but should not be blocked when extra patterns invalid`,
      metadata: {
        keyword,
      },
    });

    const originalExtraPatterns = Bun.env.AUDIT_DLP_EXTRA_PATTERNS_JSON;
    try {
      const scenarios = [
        {
          name: "invalid_json",
          raw: "not-json",
        },
        {
          name: "not_array",
          raw: JSON.stringify({ pattern: needle }),
        },
        {
          name: "invalid_regex",
          raw: JSON.stringify(["["]),
        },
      ] as const;

      for (const scenario of scenarios) {
        Bun.env.AUDIT_DLP_EXTRA_PATTERNS_JSON = scenario.raw;
        const exportResponse = await app.request(
          `/api/v1/audits/export?format=json&action=${encodeURIComponent(action)}&keyword=${encodeURIComponent(keyword)}&dlpMode=block`,
          { headers },
        );
        expect(exportResponse.status).toBe(200);
        expect(exportResponse.headers.get("x-agentledger-dlp-mode")).toBe("block");
        expect(exportResponse.headers.get("x-agentledger-dlp-matched")).toBe("false");
        const exportBody = (await exportResponse.json()) as {
          format?: string;
          items?: Array<{ detail?: string }>;
        };
        expect(exportBody.format).toBe("json");
        const matchedItem = exportBody.items?.find(
          (item) => typeof item.detail === "string" && item.detail.includes(needle),
        );
        expect(matchedItem).toBeDefined();
      }
    } finally {
      if (originalExtraPatterns === undefined) {
        delete Bun.env.AUDIT_DLP_EXTRA_PATTERNS_JSON;
      } else {
        Bun.env.AUDIT_DLP_EXTRA_PATTERNS_JSON = originalExtraPatterns;
      }
    }
  });

  test("AUDIT_DLP_EXTRA_PATTERNS_JSON 命中时 block=422/redact=200，审计与返回结构稳定", async () => {
    const auth = await getDefaultAuthContext();
    const headers = await resolveAuthHeaders(auth.accessToken, auth.userId);
    const tenantId = resolveTenantIdFromAuthHeaders(headers);
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
      throw new Error("repository.appendAuditLog 不可用，无法验证 DLP extra patterns。");
    }

    const nonce = createNonce("audit-export-dlp-extra-hit");
    const needle = `custom-sensitive-${nonce}`;
    const keyword = `dlp-extra-hit-${nonce}`;
    const action = "test.audit.export_dlp_extra_hit";
    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:${nonce}`,
      action,
      level: "warning",
      detail: `audit detail contains ${needle} and should be redacted by extra patterns`,
      metadata: {
        keyword,
      },
    });

    const originalExtraPatterns = Bun.env.AUDIT_DLP_EXTRA_PATTERNS_JSON;
    Bun.env.AUDIT_DLP_EXTRA_PATTERNS_JSON = JSON.stringify([needle]);
    try {
      const blockResponse = await app.request(
        `/api/v1/audits/export?format=json&action=${encodeURIComponent(action)}&keyword=${encodeURIComponent(keyword)}&dlpMode=block`,
        { headers },
      );
      expect(blockResponse.status).toBe(422);
      const blockBody = await readResponseAsUnknown(blockResponse);
      if (isRecord(blockBody)) {
        expect(pickString(blockBody, ["message"])).toBe(
          "审计导出命中 DLP 规则，已阻止导出。",
        );
      }

      const redactResponse = await app.request(
        `/api/v1/audits/export?format=json&action=${encodeURIComponent(action)}&keyword=${encodeURIComponent(keyword)}&dlpMode=redact`,
        { headers },
      );
      expect(redactResponse.status).toBe(200);
      expect(redactResponse.headers.get("x-agentledger-dlp-mode")).toBe("redact");
      expect(redactResponse.headers.get("x-agentledger-dlp-matched")).toBe("true");
      const redactBody = (await redactResponse.json()) as {
        format?: string;
        exportedAt?: string;
        items?: Array<{ detail?: string; action?: string }>;
        filters?: { action?: string; keyword?: string };
        targetResource?: { resourceType?: string };
      };
      expect(redactBody.format).toBe("json");
      expect(typeof redactBody.exportedAt).toBe("string");
      expect(redactBody.filters?.action).toBe(action);
      expect(redactBody.filters?.keyword).toBe(keyword);
      expect(redactBody.targetResource?.resourceType).toBe("audit_export");
      const redactedItem = redactBody.items?.find(
        (item) => item.action === action,
      );
      expect(redactedItem).toBeDefined();
      expect(redactedItem?.detail).toContain("[REDACTED_DLP]");
      expect(redactedItem?.detail).not.toContain(needle);

      const exportAudits = await queryAuditByAction(
        "audit.export",
        keyword,
        auth.accessToken,
        auth.userId,
      );
      const exportAudit = exportAudits.items.find(
        (item) =>
          item.metadata.route === "/api/v1/audits/export" &&
          item.metadata.dlpMode === "redact" &&
          item.metadata.dlpMatched === true &&
          item.metadata.dlpRedacted === 1,
      );
      expect(exportAudit).toBeDefined();
    } finally {
      if (originalExtraPatterns === undefined) {
        delete Bun.env.AUDIT_DLP_EXTRA_PATTERNS_JSON;
      } else {
        Bun.env.AUDIT_DLP_EXTRA_PATTERNS_JSON = originalExtraPatterns;
      }
    }
  });

  test("GET /api/v1/audits/evidence-bundle 在 evidence_bundle Legal Hold 生效时返回 409，释放后返回目标元信息", async () => {
    const nonce = createNonce("audit-evidence-legal-hold");
    const authHeaders = await resolveAuthHeaders();
    const tenantId = resolveTenantIdFromAuthHeaders(authHeaders);
    const bundleResourceId = `audit-evidence-target-${nonce}`;
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
      throw new Error("repository.appendAuditLog 不可用，无法验证 evidence bundle Legal Hold。");
    }

    try {
      await repositoryWithAudit.appendAuditLog({
        tenantId,
        eventId: `cp:audit-evidence-seed:${nonce}`,
        action: "test.audit.evidence_hold_seed",
        level: "info",
        detail: `audit evidence hold seed ${nonce}`,
        metadata: {
          nonce,
          route: "/api/v1/audits/evidence-bundle",
        },
      });

      const createHoldResponse = await app.request("/api/v1/audits/legal-holds", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...authHeaders,
        },
        body: JSON.stringify({
          resourceType: "evidence_bundle",
          resourceId: bundleResourceId,
          reason: `preserve bundle ${nonce}`,
        }),
      });
      const holdBody = (await createHoldResponse.json()) as { id: string };
      expect(createHoldResponse.status).toBe(201);

      const blockedResponse = await app.request(
        `/api/v1/audits/evidence-bundle?action=test.audit.evidence_hold_seed&keyword=${encodeURIComponent(
          nonce,
        )}&resourceId=${encodeURIComponent(bundleResourceId)}`,
        {
          headers: authHeaders,
        },
      );
      const blockedBody = (await blockedResponse.json()) as {
        message: string;
        legalHold: { id: string; resourceType: string; resourceId: string };
      };
      expect(blockedResponse.status).toBe(409);
      expect(blockedBody.legalHold.id).toBe(holdBody.id);
      expect(blockedBody.legalHold.resourceType).toBe("evidence_bundle");
      expect(blockedBody.legalHold.resourceId).toBe(bundleResourceId);

      const blockedAudits = await queryAuditByAction(
        "audit.evidence_bundle.export_blocked",
        bundleResourceId,
      );
      expect(
        blockedAudits.items.some(
          (item) =>
            item.metadata.resourceId === bundleResourceId &&
            item.metadata.legalHoldId === holdBody.id,
        ),
      ).toBe(true);

      const releaseResponse = await app.request(
        `/api/v1/audits/legal-holds/${encodeURIComponent(holdBody.id)}/release`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...authHeaders,
          },
          body: JSON.stringify({ reason: `release bundle ${nonce}` }),
        },
      );
      expect(releaseResponse.status).toBe(200);

      const bundleResponse = await app.request(
        `/api/v1/audits/evidence-bundle?action=test.audit.evidence_hold_seed&keyword=${encodeURIComponent(
          nonce,
        )}&resourceId=${encodeURIComponent(bundleResourceId)}`,
        {
          headers: authHeaders,
        },
      );
      const bundleBody = (await bundleResponse.json()) as {
        manifest: {
          schemaVersion: string;
          tenantId: string;
          recordCount: number;
        };
        targetResource: {
          resourceType: string;
          resourceId: string | null;
          legalHold: unknown;
        };
        rootHash: string;
        signature: string;
      };
      expect(bundleResponse.status).toBe(200);
      expect(bundleBody.manifest.schemaVersion).toBe("evidence-bundle.v1");
      expect(bundleBody.targetResource.resourceType).toBe("evidence_bundle");
      expect(bundleBody.targetResource.resourceId).toBe(bundleResourceId);
      expect(bundleBody.targetResource.legalHold).toBeNull();
      expect(bundleResponse.headers.get("x-agentledger-resource-id")).toBe(bundleResourceId);
      expect(bundleResponse.headers.get("x-agentledger-legal-hold-status")).toBe("none");

      const verifyResult = verifyEvidenceBundle(bundleBody, signingKey);
      expect(verifyResult.success).toBe(true);
    } finally {
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
      requiredApprovals: number;
      scopeBinding: {
        organizations?: string[];
        projects?: string[];
        clients?: string[];
      };
    };
    expect(asset.tenantId).toBe(tenantAId);
    expect(asset.status).toBe("draft");
    expect(asset.requiredApprovals).toBe(1);
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
        item.metadata.operation === "created" &&
        item.metadata.version === 1 &&
        item.metadata.decision === "approved" &&
        item.metadata.approverUserId === auth.userId,
    );
    expect(approvalAudit).toBeDefined();
    expect(
      approvalAudits.items.filter(
        (item) =>
          item.action === "control_plane.rule_approval_created" &&
          item.metadata.resourceId === approval.id,
      ),
    ).toHaveLength(1);

    const updateApprovalResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/approvals`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          version: 1,
          decision: "rejected",
          reason: "需要补充验证",
        }),
      },
    );
    expect(updateApprovalResponse.status).toBe(200);
    const updatedApproval = (await updateApprovalResponse.json()) as {
      id: string;
      version: number;
      decision: string;
      reason?: string;
    };
    expect(updatedApproval.id).toBe(approval.id);
    expect(updatedApproval.version).toBe(1);
    expect(updatedApproval.decision).toBe("rejected");
    expect(updatedApproval.reason).toBe("需要补充验证");

    const updatedApprovalAudits = await queryAuditByActionWithHeaders(
      "control_plane.rule_approval_updated",
      approval.id,
      tenantAHeaders,
    );
    const updatedApprovalAudit = updatedApprovalAudits.items.find(
      (item) =>
        item.action === "control_plane.rule_approval_updated" &&
        item.metadata.resourceId === approval.id &&
        item.metadata.assetId === asset.id &&
        item.metadata.operation === "updated" &&
        item.metadata.version === 1 &&
        item.metadata.decision === "rejected" &&
        item.metadata.reason === "需要补充验证" &&
        item.metadata.approverUserId === auth.userId,
    );
    expect(updatedApprovalAudit).toBeDefined();

    const approvalAuditsAfterUpdate = await queryAuditByActionWithHeaders(
      "control_plane.rule_approval_created",
      approval.id,
      tenantAHeaders,
    );
    expect(
      approvalAuditsAfterUpdate.items.filter(
        (item) =>
          item.action === "control_plane.rule_approval_created" &&
          item.metadata.resourceId === approval.id,
      ),
    ).toHaveLength(1);

    const listApprovalsResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/approvals?version=1`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(listApprovalsResponse.status).toBe(200);
    const listApprovalsBody = (await listApprovalsResponse.json()) as {
      items: Array<{ version: number; decision: string; id: string }>;
    };
    expect(
      listApprovalsBody.items.some(
        (item) =>
          item.id === approval.id &&
          item.version === 1 &&
          item.decision === "rejected",
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

  test("rule hub 路由：双人审批资产发布门槛与版本 diff", async () => {
    const nonce = createNonce("rule-hub-dual-approval");
    const owner = await registerAndLoginUser(`${nonce}-owner`);
    const reviewer = await registerAndLoginUser(`${nonce}-reviewer`);
    if (!owner.userId || !reviewer.userId) {
      throw new Error("无法解析双人审批测试用户 ID。");
    }

    const tenantResult = await createTenantByAuth(
      owner.accessToken,
      {
        name: `RuleHub Dual Tenant ${nonce}`,
        slug: `rulehub-dual-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("双人审批测试租户创建失败，缺少 tenantId。");
    }

    const addMaintainerResult = await addTenantMemberByAuth(
      owner.accessToken,
      {
        tenantId,
        userId: reviewer.userId,
        tenantRole: "maintainer",
      },
      owner.userId,
    );
    assertApiStatus(addMaintainerResult, [200, 201]);

    const ownerHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      owner.accessToken,
      owner.userId,
    );
    const reviewerHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      reviewer.accessToken,
      reviewer.userId,
    );

    const createAssetResponse = await app.request("/api/v1/rules/assets", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...ownerHeaders,
      },
      body: JSON.stringify({
        name: `Dual approval asset ${nonce}`,
        description: "验证双人审批发布门槛",
        requiredApprovals: 2,
      }),
    });
    expect(createAssetResponse.status).toBe(201);
    const asset = (await createAssetResponse.json()) as {
      id: string;
      requiredApprovals: number;
    };
    expect(asset.requiredApprovals).toBe(2);

    const createVersion1Response = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/versions`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          content: "allow tool=github.read_repo\nrequire tag=verified",
          changelog: "v1",
        }),
      },
    );
    expect(createVersion1Response.status).toBe(201);

    const createVersion2Response = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/versions`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          content: "deny tool=github.delete_repo\nrequire tag=verified",
          changelog: "v2",
        }),
      },
    );
    expect(createVersion2Response.status).toBe(201);

    const diffResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/versions/diff?fromVersion=1&toVersion=2`,
      {
        headers: ownerHeaders,
      },
    );
    expect(diffResponse.status).toBe(200);
    const diffBody = (await diffResponse.json()) as {
      assetId: string;
      fromVersion: number;
      toVersion: number;
      lines: Array<{
        type: string;
        content: string;
        oldLineNumber?: number;
        newLineNumber?: number;
      }>;
      summary: {
        added: number;
        removed: number;
        unchanged: number;
        changed: boolean;
      };
    };
    expect(diffBody.assetId).toBe(asset.id);
    expect(diffBody.fromVersion).toBe(1);
    expect(diffBody.toVersion).toBe(2);
    expect(diffBody.summary).toEqual({
      added: 1,
      removed: 1,
      unchanged: 1,
      changed: true,
    });
    expect(
      diffBody.lines.some(
        (item) =>
          item.type === "removed" &&
          item.content === "allow tool=github.read_repo" &&
          item.oldLineNumber === 1,
      ),
    ).toBe(true);
    expect(
      diffBody.lines.some(
        (item) =>
          item.type === "added" &&
          item.content === "deny tool=github.delete_repo" &&
          item.newLineNumber === 1,
      ),
    ).toBe(true);
    expect(
      diffBody.lines.some(
        (item) =>
          item.type === "unchanged" &&
          item.content === "require tag=verified",
      ),
    ).toBe(true);

    const publishWithoutApprovalsResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/publish`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          version: 2,
        }),
      },
    );
    expect(publishWithoutApprovalsResponse.status).toBe(409);

    const firstApprovalResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/approvals`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          version: 2,
          decision: "approved",
          reason: "owner approved",
        }),
      },
    );
    expect(firstApprovalResponse.status).toBe(201);

    const publishWithSingleApprovalResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/publish`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          version: 2,
        }),
      },
    );
    expect(publishWithSingleApprovalResponse.status).toBe(409);

    const secondApprovalResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/approvals`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...reviewerHeaders,
        },
        body: JSON.stringify({
          version: 2,
          decision: "approved",
          reason: "reviewer approved",
        }),
      },
    );
    expect(secondApprovalResponse.status).toBe(201);

    const publishResponse = await app.request(
      `/api/v1/rules/assets/${encodeURIComponent(asset.id)}/publish`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
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
      ownerHeaders,
    );
    const publishAudit = publishAudits.items.find(
      (item) =>
        item.action === "control_plane.rule_asset_published" &&
        item.metadata.resourceId === asset.id &&
        item.metadata.version === 2 &&
        item.metadata.requiredApprovals === 2 &&
        item.metadata.approvedApprovals === 2,
    );
    expect(publishAudit).toBeDefined();
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

    const mismatchPolicyResponse = await app.request(
      `/api/v1/mcp/policies/${encodeURIComponent(`tool-mismatch-${nonce}`)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          riskLevel: "high",
          decision: "require_approval",
          approvalMode: "single_stage",
          stage1RequiredApprovals: 1,
          stage1Roles: ["owner"],
          stage2RequiredApprovals: 1,
          stage2Roles: ["owner"],
        }),
      },
    );
    expect(mismatchPolicyResponse.status).toBe(400);
    const mismatchPolicyBody = (await mismatchPolicyResponse.json()) as {
      message?: string;
    };
    expect(String(mismatchPolicyBody.message ?? "")).toContain(
      "approvalMode 与 approvalStages 阶段数量不一致",
    );

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

    const customStagePolicyResponse = await app.request(
      `/api/v1/mcp/policies/${encodeURIComponent(`tool-custom-stage-${nonce}`)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          riskLevel: "high",
          decision: "require_approval",
          approvalMode: "two_stage",
          approvalStages: [
            {
              nodeId: "maintainer-review",
              stage: "stage1",
              label: "Maintainer Review",
              requiredApprovals: 1,
              roles: ["maintainer"],
            },
            {
              nodeId: "owner-review",
              stage: "stage2",
              label: "Owner Review",
              requiredApprovals: 1,
              roles: ["owner"],
            },
          ],
        }),
      },
    );
    expect(customStagePolicyResponse.status).toBe(200);
    const customStagePolicy = (await customStagePolicyResponse.json()) as {
      approvalStages?: Array<{
        nodeId?: string;
        stage: string;
        label?: string;
      }>;
      approvalWorkflow?: {
        entryNodeId: string;
      };
    };
    expect(customStagePolicy.approvalStages?.[0]).toMatchObject({
      nodeId: "maintainer-review",
      stage: "stage1",
      label: "Maintainer Review",
    });
    expect(customStagePolicy.approvalStages?.[1]).toMatchObject({
      nodeId: "owner-review",
      stage: "stage2",
      label: "Owner Review",
    });
    expect(customStagePolicy.approvalWorkflow?.entryNodeId).toBe(
      "maintainer-review",
    );

    const customStageEvaluateResponse = await app.request(
      "/api/v1/mcp/evaluate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          toolId: `tool-custom-stage-${nonce}`,
          reason: "验证自定义审批节点在策略回读后仍生效",
        }),
      },
    );
    expect(customStageEvaluateResponse.status).toBe(200);
    const customStageEvaluate = (await customStageEvaluateResponse.json()) as {
      approvalMode?: string;
      currentNodeId?: string | null;
      currentStage?: string | null;
      approvalStages?: Array<{
        nodeId?: string;
        stage: string;
        label?: string;
      }>;
      pathHistory?: string[];
    };
    expect(customStageEvaluate.approvalMode).toBe("two_stage");
    expect(customStageEvaluate.currentNodeId).toBe("maintainer-review");
    expect(customStageEvaluate.currentStage).toBe("stage1");
    expect(customStageEvaluate.approvalStages?.[0]).toMatchObject({
      nodeId: "maintainer-review",
      stage: "stage1",
      label: "Maintainer Review",
    });
    expect(customStageEvaluate.pathHistory).toEqual(["maintainer-review"]);

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

  test("mcp 路由：支持 single_stage/two_stage 静态审批流转", async () => {
    const nonce = createNonce("mcp-static-approval");
    const auth = await getDefaultAuthContext();
    const maintainer = await registerAndLoginUser(`${nonce}-maintainer`);
    if (!maintainer.userId) {
      throw new Error("maintainer 测试用户缺少 userId。");
    }

    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `MCP Static Approval ${nonce}`,
        slug: `mcp-static-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("MCP 静态审批测试租户创建失败。");
    }

    const addMaintainerResult = await addTenantMemberByAuth(
      auth.accessToken,
      {
        tenantId,
        userId: maintainer.userId,
        tenantRole: "maintainer",
      },
      auth.userId,
    );
    assertApiStatus(addMaintainerResult, [200, 201]);

    const ownerHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      auth.accessToken,
      auth.userId,
    );
    const maintainerHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      maintainer.accessToken,
      maintainer.userId,
    );

    const policyResponse = await app.request(
      `/api/v1/mcp/policies/${encodeURIComponent(`tool-${nonce}`)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          riskLevel: "high",
          decision: "require_approval",
          reason: "静态审批流测试",
        }),
      },
    );
    expect(policyResponse.status).toBe(200);

    const singleStageEvaluateResponse = await app.request(
      "/api/v1/mcp/evaluate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          toolId: `tool-${nonce}`,
          reason: "single-stage 首次评估",
          approvalConfig: {
            mode: "single_stage",
            stage1: {
              requiredApprovals: 2,
              roles: ["owner", "maintainer"],
            },
          },
        }),
      },
    );
    expect(singleStageEvaluateResponse.status).toBe(200);
    const singleStageEvaluate = (await singleStageEvaluateResponse.json()) as {
      approvalRequestId?: string;
      approvalMode: string;
      currentStage: string | null;
      remainingApprovals: number;
      approvalConditionMatched: boolean;
      approvalStages: Array<{
        stage: string;
        requiredApprovals: number;
        roles: string[];
      }>;
    };
    expect(singleStageEvaluate.approvalMode).toBe("single_stage");
    expect(singleStageEvaluate.currentStage).toBe("stage1");
    expect(singleStageEvaluate.remainingApprovals).toBe(2);
    expect(singleStageEvaluate.approvalConditionMatched).toBe(true);
    expect(singleStageEvaluate.approvalStages).toHaveLength(1);
    expect(singleStageEvaluate.approvalStages[0]).toMatchObject({
      stage: "stage1",
      requiredApprovals: 2,
    });
    expect(singleStageEvaluate.approvalStages[0]?.roles).toEqual([
      "owner",
      "maintainer",
    ]);
    expect(typeof singleStageEvaluate.approvalRequestId).toBe("string");

    const singleStageFirstApproveResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(singleStageEvaluate.approvalRequestId as string)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...maintainerHeaders,
        },
        body: JSON.stringify({
          reason: "maintainer 第一票通过",
        }),
      },
    );
    expect(singleStageFirstApproveResponse.status).toBe(200);
    const singleStageFirstApprove =
      (await singleStageFirstApproveResponse.json()) as {
        status: string;
        currentStage: string | null;
        remainingApprovals: number;
      };
    expect(singleStageFirstApprove.status).toBe("pending");
    expect(singleStageFirstApprove.currentStage).toBe("stage1");
    expect(singleStageFirstApprove.remainingApprovals).toBe(1);

    const singleStageSecondApproveResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(singleStageEvaluate.approvalRequestId as string)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          reason: "owner 第二票通过",
        }),
      },
    );
    expect(singleStageSecondApproveResponse.status).toBe(200);
    const singleStageSecondApprove =
      (await singleStageSecondApproveResponse.json()) as {
        status: string;
        currentStage: string | null;
        remainingApprovals: number;
      };
    expect(singleStageSecondApprove.status).toBe("approved");
    expect(singleStageSecondApprove.currentStage).toBeNull();
    expect(singleStageSecondApprove.remainingApprovals).toBe(0);

    const singleStageApprovedEvaluateResponse = await app.request(
      "/api/v1/mcp/evaluate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          toolId: `tool-${nonce}`,
          approvalRequestId: singleStageEvaluate.approvalRequestId,
          reason: "single-stage 审批后复评",
        }),
      },
    );
    expect(singleStageApprovedEvaluateResponse.status).toBe(200);
    const singleStageApprovedEvaluate =
      (await singleStageApprovedEvaluateResponse.json()) as {
        result: string;
        currentStage: string | null;
        remainingApprovals: number;
        approvalConditionMatched: boolean;
      };
    expect(singleStageApprovedEvaluate.result).toBe("approved");
    expect(singleStageApprovedEvaluate.currentStage).toBeNull();
    expect(singleStageApprovedEvaluate.remainingApprovals).toBe(0);
    expect(singleStageApprovedEvaluate.approvalConditionMatched).toBe(true);

    const twoStageCreateResponse = await app.request("/api/v1/mcp/approvals", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...ownerHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        reason: "two-stage 手工申请",
        approvalConfig: {
          mode: "two_stage",
          stage1: {
            requiredApprovals: 1,
            roles: ["maintainer"],
          },
          stage2: {
            requiredApprovals: 1,
            roles: ["owner"],
          },
        },
      }),
    });
    expect(twoStageCreateResponse.status).toBe(201);
    const twoStageCreated = (await twoStageCreateResponse.json()) as {
      id: string;
      status: string;
      approvalMode: string;
      currentStage: string | null;
      remainingApprovals: number;
      approvalStages: Array<{
        stage: string;
        roles: string[];
        requiredApprovals: number;
      }>;
    };
    expect(twoStageCreated.status).toBe("pending");
    expect(twoStageCreated.approvalMode).toBe("two_stage");
    expect(twoStageCreated.currentStage).toBe("stage1");
    expect(twoStageCreated.remainingApprovals).toBe(1);
    expect(twoStageCreated.approvalStages).toHaveLength(2);
    expect(
      twoStageCreated.approvalStages.find((item) => item.stage === "stage1")
        ?.roles,
    ).toEqual([
      "maintainer",
    ]);
    expect(
      twoStageCreated.approvalStages.find((item) => item.stage === "stage2")
        ?.roles,
    ).toEqual(["owner"]);

    const stage1RoleForbiddenResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(twoStageCreated.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          reason: "owner 不应通过 stage1 角色校验",
        }),
      },
    );
    expect(stage1RoleForbiddenResponse.status).toBe(403);

    const stageJumpResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(twoStageCreated.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          stage: "stage2",
          reason: "越阶段审批应被拒绝",
        }),
      },
    );
    expect(stageJumpResponse.status).toBe(409);

    const twoStageStage1ApproveResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(twoStageCreated.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...maintainerHeaders,
        },
        body: JSON.stringify({
          stage: "stage1",
          reason: "maintainer 完成 stage1",
        }),
      },
    );
    expect(twoStageStage1ApproveResponse.status).toBe(200);
    const twoStageStage1Approve =
      (await twoStageStage1ApproveResponse.json()) as {
        status: string;
        currentStage: string | null;
        remainingApprovals: number;
      };
    expect(twoStageStage1Approve.status).toBe("pending");
    expect(twoStageStage1Approve.currentStage).toBe("stage2");
    expect(twoStageStage1Approve.remainingApprovals).toBe(1);

    const twoStagePendingEvaluateResponse = await app.request(
      "/api/v1/mcp/evaluate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          toolId: `tool-${nonce}`,
          approvalRequestId: twoStageCreated.id,
          reason: "two-stage stage1 完成后复评",
        }),
      },
    );
    expect(twoStagePendingEvaluateResponse.status).toBe(200);
    const twoStagePendingEvaluate =
      (await twoStagePendingEvaluateResponse.json()) as {
        result: string;
        currentStage: string | null;
        remainingApprovals: number;
        approvalConditionMatched: boolean;
      };
    expect(twoStagePendingEvaluate.result).toBe("blocked");
    expect(twoStagePendingEvaluate.currentStage).toBe("stage2");
    expect(twoStagePendingEvaluate.remainingApprovals).toBe(1);
    expect(twoStagePendingEvaluate.approvalConditionMatched).toBe(true);

    const twoStageRejectResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(twoStageCreated.id)}/reject`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          stage: "stage2",
          reason: "owner 在 stage2 驳回",
        }),
      },
    );
    expect(twoStageRejectResponse.status).toBe(200);
    const twoStageRejected = (await twoStageRejectResponse.json()) as {
      status: string;
      currentStage: string | null;
      remainingApprovals: number;
    };
    expect(twoStageRejected.status).toBe("rejected");
    expect(twoStageRejected.currentStage).toBeNull();
    expect(twoStageRejected.remainingApprovals).toBe(0);

    const twoStageRejectedEvaluateResponse = await app.request(
      "/api/v1/mcp/evaluate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          toolId: `tool-${nonce}`,
          approvalRequestId: twoStageCreated.id,
          reason: "two-stage 驳回后复评",
        }),
      },
    );
    expect(twoStageRejectedEvaluateResponse.status).toBe(200);
    const twoStageRejectedEvaluate =
      (await twoStageRejectedEvaluateResponse.json()) as {
        result: string;
        currentStage: string | null;
        remainingApprovals: number;
        approvalConditionMatched: boolean;
      };
    expect(twoStageRejectedEvaluate.result).toBe("blocked");
    expect(twoStageRejectedEvaluate.currentStage).toBeNull();
    expect(twoStageRejectedEvaluate.remainingApprovals).toBe(0);
    expect(twoStageRejectedEvaluate.approvalConditionMatched).toBe(true);

    const multiStageCreateResponse = await app.request("/api/v1/mcp/approvals", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...ownerHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        reason: "multi-stage 手工申请",
        approvalConfig: {
          approvalStages: [
            {
              requiredApprovals: 1,
              roles: ["maintainer"],
            },
            {
              requiredApprovals: 1,
              roles: ["owner"],
            },
            {
              requiredApprovals: 1,
              roles: ["owner"],
            },
          ],
        },
      }),
    });
    expect(multiStageCreateResponse.status).toBe(201);
    const multiStageCreated = (await multiStageCreateResponse.json()) as {
      id: string;
      approvalMode: string;
      currentStage: string | null;
      remainingApprovals: number;
      approvalStages: Array<{
        stage: string;
        requiredApprovals: number;
        roles: string[];
      }>;
    };
    expect(multiStageCreated.approvalMode).toBe("multi_stage");
    expect(multiStageCreated.currentStage).toBe("stage1");
    expect(multiStageCreated.remainingApprovals).toBe(1);
    expect(multiStageCreated.approvalStages.map((item) => item.stage)).toEqual([
      "stage1",
      "stage2",
      "stage3",
    ]);

    const multiStageStage1ApproveResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(multiStageCreated.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...maintainerHeaders,
        },
        body: JSON.stringify({
          stage: "stage1",
          reason: "maintainer 完成 multi-stage stage1",
        }),
      },
    );
    expect(multiStageStage1ApproveResponse.status).toBe(200);
    const multiStageStage1Approve =
      (await multiStageStage1ApproveResponse.json()) as {
        status: string;
        currentStage: string | null;
        remainingApprovals: number;
        approvalStages: Array<{
          stage: string;
          approvedApprovals: number;
        }>;
      };
    expect(multiStageStage1Approve.status).toBe("pending");
    expect(multiStageStage1Approve.currentStage).toBe("stage2");
    expect(multiStageStage1Approve.remainingApprovals).toBe(1);
    expect(
      multiStageStage1Approve.approvalStages.find((item) => item.stage === "stage1")
        ?.approvedApprovals,
    ).toBe(1);

    const multiStageStage2ApproveResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(multiStageCreated.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          stage: "stage2",
          reason: "owner 完成 multi-stage stage2",
        }),
      },
    );
    expect(multiStageStage2ApproveResponse.status).toBe(200);
    const multiStageStage2Approve =
      (await multiStageStage2ApproveResponse.json()) as {
        status: string;
        currentStage: string | null;
        remainingApprovals: number;
      };
    expect(multiStageStage2Approve.status).toBe("pending");
    expect(multiStageStage2Approve.currentStage).toBe("stage3");
    expect(multiStageStage2Approve.remainingApprovals).toBe(1);

    const multiStageStage3ApproveResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(multiStageCreated.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          stage: "stage3",
          reason: "owner 完成 multi-stage stage3",
        }),
      },
    );
    expect(multiStageStage3ApproveResponse.status).toBe(200);
    const multiStageStage3Approve =
      (await multiStageStage3ApproveResponse.json()) as {
        status: string;
        currentStage: string | null;
        remainingApprovals: number;
      };
    expect(multiStageStage3Approve.status).toBe("approved");
    expect(multiStageStage3Approve.currentStage).toBeNull();
    expect(multiStageStage3Approve.remainingApprovals).toBe(0);
  });

  test("mcp 路由：支持 approvalWorkflow 分支跳转与路径历史", async () => {
    const nonce = createNonce("mcp-branching-workflow");
    const owner = await getDefaultAuthContext();
    const maintainer = await registerAndLoginUser(`${nonce}-maintainer`);
    if (!maintainer.userId) {
      throw new Error("maintainer 测试用户缺少 userId。");
    }

    const tenantResult = await createTenantByAuth(
      owner.accessToken,
      {
        name: `MCP Branch Workflow ${nonce}`,
        slug: `mcp-branch-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("MCP 分支审批测试租户创建失败。");
    }

    const addMaintainerResult = await addTenantMemberByAuth(
      owner.accessToken,
      {
        tenantId,
        userId: maintainer.userId,
        tenantRole: "maintainer",
      },
      owner.userId,
    );
    assertApiStatus(addMaintainerResult, [200, 201]);

    const ownerHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      owner.accessToken,
      owner.userId,
    );
    const maintainerHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      maintainer.accessToken,
      maintainer.userId,
    );

    const workflow = {
      entryNodeId: "stage1-node",
      nodes: [
        {
          nodeId: "stage1-node",
          kind: "approval",
          label: "Stage 1",
          stage: "stage1",
          requiredApprovals: 1,
          roles: ["owner", "maintainer"],
        },
        {
          nodeId: "stage2-node",
          kind: "approval",
          label: "Stage 2",
          stage: "stage2",
          requiredApprovals: 1,
          roles: ["owner"],
        },
        {
          nodeId: "approved",
          kind: "terminal_approved",
          label: "Approved",
        },
        {
          nodeId: "rejected",
          kind: "terminal_rejected",
          label: "Rejected",
        },
      ],
      transitions: [
        {
          fromNodeId: "stage1-node",
          toNodeId: "stage2-node",
          condition: {
            tenantRoles: ["owner"],
          },
        },
        {
          fromNodeId: "stage1-node",
          toNodeId: "approved",
          condition: {
            default: true,
          },
        },
        {
          fromNodeId: "stage2-node",
          toNodeId: "approved",
          condition: {
            default: true,
          },
        },
      ],
    };

    const policyResponse = await app.request(
      `/api/v1/mcp/policies/${encodeURIComponent(`tool-${nonce}`)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          riskLevel: "high",
          decision: "require_approval",
          approvalMode: "two_stage",
          approvalWorkflow: workflow,
          reason: "分支审批流测试",
        }),
      },
    );
    expect(policyResponse.status).toBe(200);

    const ownerApprovalResponse = await app.request("/api/v1/mcp/approvals", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...ownerHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        reason: "owner 触发分支审批",
        approvalConfig: {
          approvalWorkflow: workflow,
        },
      }),
    });
    expect(ownerApprovalResponse.status).toBe(201);
    const ownerApproval = (await ownerApprovalResponse.json()) as {
      id: string;
      currentNodeId: string | null;
      currentStage: string | null;
      approvalWorkflow?: { entryNodeId: string };
      nextTransitionPreview?: { fromNodeId: string; toNodeId?: string };
      pathHistory?: string[];
    };
    expect(ownerApproval.currentNodeId).toBe("stage1-node");
    expect(ownerApproval.currentStage).toBe("stage1");
    expect(ownerApproval.approvalWorkflow?.entryNodeId).toBe("stage1-node");
    expect(ownerApproval.nextTransitionPreview?.fromNodeId).toBe("stage1-node");
    expect(ownerApproval.pathHistory).toEqual(["stage1-node"]);

    const ownerApproveStage1Response = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(ownerApproval.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          nodeId: "stage1-node",
          stage: "stage1",
          reason: "owner 完成 stage1",
        }),
      },
    );
    expect(ownerApproveStage1Response.status).toBe(200);
    const ownerApproveStage1 = (await ownerApproveStage1Response.json()) as {
      status: string;
      currentNodeId: string | null;
      currentStage: string | null;
      pathHistory?: string[];
      nextTransitionPreview?: { toNodeId?: string };
    };
    expect(ownerApproveStage1.status).toBe("pending");
    expect(ownerApproveStage1.currentNodeId).toBe("stage2-node");
    expect(ownerApproveStage1.currentStage).toBe("stage2");
    expect(ownerApproveStage1.pathHistory).toEqual(["stage1-node", "stage2-node"]);
    expect(ownerApproveStage1.nextTransitionPreview?.toNodeId).toBe("approved");

    const ownerRejectStage2Response = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(ownerApproval.id)}/reject`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          nodeId: "stage2-node",
          stage: "stage2",
          reason: "owner 在 stage2 驳回",
        }),
      },
    );
    expect(ownerRejectStage2Response.status).toBe(200);
    const ownerRejectStage2 = (await ownerRejectStage2Response.json()) as {
      status: string;
      currentNodeId: string | null;
      currentStage: string | null;
      pathHistory?: string[];
    };
    expect(ownerRejectStage2.status).toBe("rejected");
    expect(ownerRejectStage2.currentNodeId).toBe("rejected");
    expect(ownerRejectStage2.currentStage).toBeNull();
    expect(ownerRejectStage2.pathHistory).toEqual([
      "stage1-node",
      "stage2-node",
      "rejected",
    ]);

    const maintainerApprovalResponse = await app.request("/api/v1/mcp/approvals", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...maintainerHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        reason: "maintainer 触发默认分支",
        approvalConfig: {
          approvalWorkflow: workflow,
        },
      }),
    });
    expect(maintainerApprovalResponse.status).toBe(201);
    const maintainerApproval = (await maintainerApprovalResponse.json()) as {
      id: string;
      currentNodeId: string | null;
    };
    expect(maintainerApproval.currentNodeId).toBe("stage1-node");

    const maintainerApproveStage1Response = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(maintainerApproval.id)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...maintainerHeaders,
        },
        body: JSON.stringify({
          nodeId: "stage1-node",
          stage: "stage1",
          reason: "maintainer 走默认分支通过",
        }),
      },
    );
    expect(maintainerApproveStage1Response.status).toBe(200);
    const maintainerApproveStage1 =
      (await maintainerApproveStage1Response.json()) as {
        status: string;
        currentNodeId: string | null;
        currentStage: string | null;
        pathHistory?: string[];
      };
    expect(maintainerApproveStage1.status).toBe("approved");
    expect(maintainerApproveStage1.currentNodeId).toBe("approved");
    expect(maintainerApproveStage1.currentStage).toBeNull();
    expect(maintainerApproveStage1.pathHistory).toEqual([
      "stage1-node",
      "approved",
    ]);
  });

  test("mcp 路由：支持基于 evaluationTimestamp 的 timeWindow 分支匹配", async () => {
    const nonce = createNonce("mcp-time-window");
    const owner = await getDefaultAuthContext();

    const tenantResult = await createTenantByAuth(
      owner.accessToken,
      {
        name: `MCP Time Window ${nonce}`,
        slug: `mcp-time-window-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("MCP 时间窗审批测试租户创建失败。");
    }

    const ownerHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      owner.accessToken,
      owner.userId,
    );

    const workflow = {
      entryNodeId: "stage1-node",
      nodes: [
        {
          nodeId: "stage1-node",
          kind: "approval",
          label: "Stage 1",
          stage: "stage1",
          requiredApprovals: 1,
          roles: ["owner"],
        },
        {
          nodeId: "stage2-node",
          kind: "approval",
          label: "Night Shift",
          stage: "stage2",
          requiredApprovals: 1,
          roles: ["owner"],
        },
        {
          nodeId: "stage3-node",
          kind: "approval",
          label: "Final Review",
          stage: "stage3",
          requiredApprovals: 1,
          roles: ["owner"],
        },
        {
          nodeId: "approved",
          kind: "terminal_approved",
          label: "Approved",
        },
        {
          nodeId: "rejected",
          kind: "terminal_rejected",
          label: "Rejected",
        },
      ],
      transitions: [
        {
          fromNodeId: "stage1-node",
          toNodeId: "stage2-node",
          condition: {
            timeWindow: {
              timezone: "Asia/Shanghai",
              weekdays: [1],
              startTime: "22:00",
              endTime: "02:00",
            },
          },
        },
        {
          fromNodeId: "stage1-node",
          toNodeId: "approved",
          condition: {
            default: true,
          },
        },
        {
          fromNodeId: "stage2-node",
          toNodeId: "stage3-node",
          condition: {
            default: true,
          },
        },
        {
          fromNodeId: "stage3-node",
          toNodeId: "approved",
          condition: {
            default: true,
          },
        },
      ],
    };

    const policyResponse = await app.request(
      `/api/v1/mcp/policies/${encodeURIComponent(`tool-${nonce}`)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          riskLevel: "high",
          decision: "require_approval",
          approvalMode: "multi_stage",
          approvalWorkflow: workflow,
          reason: "时间窗审批流测试",
        }),
      },
    );
    expect(policyResponse.status).toBe(200);

    const evaluateInWindowResponse = await app.request("/api/v1/mcp/evaluate", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...ownerHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        reason: "夜间窗口内触发",
        evaluationTimestamp: "2026-03-09T15:30:00.000Z",
      }),
    });
    expect(evaluateInWindowResponse.status).toBe(200);
    const evaluateInWindow = (await evaluateInWindowResponse.json()) as {
      approvalRequestId?: string;
      currentNodeId?: string | null;
      nextTransitionPreview?: {
        toNodeId?: string;
        condition?: {
          timeWindow?: {
            timezone: string;
            weekdays?: number[];
            startTime: string;
            endTime: string;
          };
        };
      };
    };
    expect(evaluateInWindow.currentNodeId).toBe("stage1-node");
    expect(evaluateInWindow.nextTransitionPreview?.toNodeId).toBe("stage2-node");
    expect(evaluateInWindow.nextTransitionPreview?.condition?.timeWindow).toEqual({
      timezone: "Asia/Shanghai",
      weekdays: [1],
      startTime: "22:00",
      endTime: "02:00",
    });

    const inWindowApprovalResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(evaluateInWindow.approvalRequestId as string)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          nodeId: "stage1-node",
          stage: "stage1",
          reason: "窗口内通过 stage1",
        }),
      },
    );
    expect(inWindowApprovalResponse.status).toBe(200);
    const inWindowApproval = (await inWindowApprovalResponse.json()) as {
      status: string;
      currentNodeId: string | null;
      currentStage: string | null;
      pathHistory?: string[];
      nextTransitionPreview?: { toNodeId?: string };
    };
    expect(inWindowApproval.status).toBe("pending");
    expect(inWindowApproval.currentNodeId).toBe("stage2-node");
    expect(inWindowApproval.currentStage).toBe("stage2");
    expect(inWindowApproval.pathHistory).toEqual(["stage1-node", "stage2-node"]);
    expect(inWindowApproval.nextTransitionPreview?.toNodeId).toBe("stage3-node");

    const evaluateOutsideWindowResponse = await app.request("/api/v1/mcp/evaluate", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...ownerHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        reason: "非窗口时间触发",
        evaluationTimestamp: "2026-03-10T15:30:00.000Z",
      }),
    });
    expect(evaluateOutsideWindowResponse.status).toBe(200);
    const evaluateOutsideWindow = (await evaluateOutsideWindowResponse.json()) as {
      approvalRequestId?: string;
      currentNodeId?: string | null;
      nextTransitionPreview?: { toNodeId?: string; matchedBy?: string };
    };
    expect(evaluateOutsideWindow.currentNodeId).toBe("stage1-node");
    expect(evaluateOutsideWindow.nextTransitionPreview?.toNodeId).toBe("approved");
    expect(evaluateOutsideWindow.nextTransitionPreview?.matchedBy).toBe("default");

    const outsideWindowApprovalResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(evaluateOutsideWindow.approvalRequestId as string)}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          nodeId: "stage1-node",
          stage: "stage1",
          reason: "非窗口时间通过 stage1",
        }),
      },
    );
    expect(outsideWindowApprovalResponse.status).toBe(200);
    const outsideWindowApproval = (await outsideWindowApprovalResponse.json()) as {
      status: string;
      currentNodeId: string | null;
      currentStage: string | null;
      pathHistory?: string[];
    };
    expect(outsideWindowApproval.status).toBe("approved");
    expect(outsideWindowApproval.currentNodeId).toBe("approved");
    expect(outsideWindowApproval.currentStage).toBeNull();
    expect(outsideWindowApproval.pathHistory).toEqual(["stage1-node", "approved"]);
  });

  test("mcp 路由：approvalCondition 未命中时不触发审批", async () => {
    const nonce = createNonce("mcp-approval-condition");
    const owner = await getDefaultAuthContext();
    const maintainer = await registerAndLoginUser(`${nonce}-maintainer`);
    if (!maintainer.userId) {
      throw new Error("maintainer 测试用户缺少 userId。");
    }

    const tenantResult = await createTenantByAuth(
      owner.accessToken,
      {
        name: `MCP Approval Condition ${nonce}`,
        slug: `mcp-approval-condition-${nonce}`,
      },
      owner.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("MCP approvalCondition 测试租户创建失败。");
    }

    const addMaintainerResult = await addTenantMemberByAuth(
      owner.accessToken,
      {
        tenantId,
        userId: maintainer.userId,
        tenantRole: "maintainer",
      },
      owner.userId,
    );
    assertApiStatus(addMaintainerResult, [200, 201]);

    const ownerHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      owner.accessToken,
      owner.userId,
    );
    const maintainerHeaders = await issueTenantScopedAuthHeaders(
      tenantId,
      maintainer.accessToken,
      maintainer.userId,
    );

    const policyResponse = await app.request(
      `/api/v1/mcp/policies/${encodeURIComponent(`tool-${nonce}`)}`,
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...ownerHeaders,
        },
        body: JSON.stringify({
          riskLevel: "high",
          decision: "require_approval",
          approvalCondition: {
            tenantRoles: ["owner"],
          },
          reason: "仅 owner 触发审批",
        }),
      },
    );
    expect(policyResponse.status).toBe(200);

    const maintainerEvaluateResponse = await app.request("/api/v1/mcp/evaluate", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...maintainerHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        reason: "maintainer 不应触发审批",
      }),
    });
    expect(maintainerEvaluateResponse.status).toBe(200);
    const maintainerEvaluate = (await maintainerEvaluateResponse.json()) as {
      result: string;
      approvalRequestId?: string;
      approvalConditionMatched?: boolean;
    };
    expect(maintainerEvaluate.result).toBe("allowed");
    expect(maintainerEvaluate.approvalRequestId).toBeUndefined();
    expect(maintainerEvaluate.approvalConditionMatched).toBe(false);

    const maintainerApprovalsResponse = await app.request(
      "/api/v1/mcp/approvals?status=pending",
      {
        headers: maintainerHeaders,
      },
    );
    expect(maintainerApprovalsResponse.status).toBe(200);
    const maintainerApprovals = (await maintainerApprovalsResponse.json()) as {
      total: number;
      items: Array<{ id: string }>;
    };
    expect(maintainerApprovals.total).toBe(0);
    expect(maintainerApprovals.items).toHaveLength(0);

    const ownerEvaluateResponse = await app.request("/api/v1/mcp/evaluate", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...ownerHeaders,
      },
      body: JSON.stringify({
        toolId: `tool-${nonce}`,
        reason: "owner 命中审批条件",
      }),
    });
    expect(ownerEvaluateResponse.status).toBe(200);
    const ownerEvaluate = (await ownerEvaluateResponse.json()) as {
      result: string;
      approvalRequestId?: string;
      approvalConditionMatched?: boolean;
      currentStage?: string | null;
    };
    expect(ownerEvaluate.result).toBe("blocked");
    expect(typeof ownerEvaluate.approvalRequestId).toBe("string");
    expect(ownerEvaluate.approvalConditionMatched).toBe(true);
    expect(ownerEvaluate.currentStage).toBe("stage1");
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
      Boolean(openapiBody.paths?.["/api/v2/replay/datasets/{id}/versions"]),
    ).toBe(true);
    expect(
      Boolean(
        openapiBody.paths?.[
          "/api/v2/replay/datasets/{id}/versions/{versionId}/cases"
        ],
      ),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/datasets/{id}/promote"]),
    ).toBe(true);
    expect(Boolean(openapiBody.paths?.["/api/v2/replay/experiments"])).toBe(
      true,
    );
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/experiments/compare"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/experiments/{id}"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/experiments/{id}/run"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/experiments/{id}/cancel"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/experiments/{id}/results"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/experiments/{id}/compare"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/experiments/{id}/workflow"]),
    ).toBe(true);
    expect(
      Boolean(openapiBody.paths?.["/api/v2/replay/experiments/{id}/artifacts"]),
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
    const replayDatasetV2 = openapiBody.components?.schemas?.[
      "ReplayDatasetV2"
    ] as
      | {
          properties?: Record<string, { description?: string }>;
        }
      | undefined;
    const replayDatasetVersionsPath = openapiBody.paths?.[
      "/api/v2/replay/datasets/{id}/versions"
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
    const replayDatasetVersionCasesPath = openapiBody.paths?.[
      "/api/v2/replay/datasets/{id}/versions/{versionId}/cases"
    ] as
      | {
          get?: {
            parameters?: Array<{ name?: string; in?: string }>;
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
    const replayDatasetPromotePath = openapiBody.paths?.[
      "/api/v2/replay/datasets/{id}/promote"
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
    const replayExperimentPath = openapiBody.paths?.[
      "/api/v2/replay/experiments"
    ] as
      | {
          get?: {
            parameters?: Array<{ name?: string; in?: string }>;
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
    const replayExperimentBatchComparePath = openapiBody.paths?.[
      "/api/v2/replay/experiments/compare"
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
    const replayExperimentByIdPath = openapiBody.paths?.[
      "/api/v2/replay/experiments/{id}"
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
          patch?: {
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
    const replayExperimentComparePath = openapiBody.paths?.[
      "/api/v2/replay/experiments/{id}/compare"
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
    const replayExperimentWorkflowPath = openapiBody.paths?.[
      "/api/v2/replay/experiments/{id}/workflow"
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
    const replayExperimentArtifactsPath = openapiBody.paths?.[
      "/api/v2/replay/experiments/{id}/artifacts"
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
    const replayDatasetVersionCreateInputV2 = openapiBody.components?.schemas?.[
      "ReplayDatasetVersionCreateInputV2"
    ] as
      | {
          required?: string[];
          properties?: Record<string, { description?: string }>;
        }
      | undefined;
    const replayDatasetVersionListResponseV2 = openapiBody.components?.schemas?.[
      "ReplayDatasetVersionListResponseV2"
    ] as
      | {
          required?: string[];
          properties?: Record<string, { description?: string; $ref?: string }>;
        }
      | undefined;
    const replayDatasetVersionPromoteResponseV2 = openapiBody.components?.schemas?.[
      "ReplayDatasetVersionPromoteResponseV2"
    ] as
      | {
          required?: string[];
          properties?: Record<string, { $ref?: string }>;
        }
      | undefined;
    const replayDatasetVersionCasesResponseV2 = openapiBody.components?.schemas?.[
      "ReplayDatasetVersionCasesResponseV2"
    ] as
      | {
          required?: string[];
          properties?: Record<string, { $ref?: string; description?: string }>;
        }
      | undefined;
    const replayExperimentInputV2 = openapiBody.components?.schemas?.[
      "ReplayExperimentInputV2"
    ] as
      | {
          required?: string[];
          properties?: Record<string, { description?: string }>;
        }
      | undefined;
    const replayExperimentPatchInputV2 = openapiBody.components?.schemas?.[
      "ReplayExperimentPatchInputV2"
    ] as
      | {
          required?: string[];
          properties?: Record<string, { description?: string }>;
        }
      | undefined;
    const replayExperimentV2 = openapiBody.components?.schemas?.[
      "ReplayExperimentV2"
    ] as
      | {
          required?: string[];
          properties?: Record<string, { description?: string; $ref?: string }>;
        }
      | undefined;
    const replayExperimentCompareResponseV2 = openapiBody.components?.schemas?.[
      "ReplayExperimentCompareResponseV2"
    ] as
      | {
          properties?: Record<string, { $ref?: string }>;
        }
      | undefined;
    const replayExperimentBatchCompareResponseV2 = openapiBody.components?.schemas?.[
      "ReplayExperimentBatchCompareResponseV2"
    ] as
      | {
          properties?: Record<string, { $ref?: string }>;
        }
      | undefined;
    const replayExperimentWorkflowResponseV2 = openapiBody.components?.schemas?.[
      "ReplayExperimentWorkflowResponseV2"
    ] as
      | {
          properties?: Record<string, { $ref?: string }>;
        }
      | undefined;
    const replayExperimentArtifactsResponseV2 = openapiBody.components?.schemas?.[
      "ReplayExperimentArtifactsResponseV2"
    ] as
      | {
          properties?: Record<string, { $ref?: string }>;
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
      replayDatasetVersionCasesPath?.get?.parameters?.some(
        (parameter: { name?: string; in?: string }) =>
          parameter.name === "versionId" && parameter.in === "path",
      ),
    ).toBe(true);
    expect(
      replayDatasetVersionCasesPath?.get?.parameters?.some(
        (parameter: { name?: string; in?: string }) =>
          parameter.name === "limit" && parameter.in === "query",
      ),
    ).toBe(true);
    expect(
      replayDatasetVersionCasesPath?.get?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayDatasetVersionCasesResponseV2");
    expect(replayDatasetVersionCasesResponseV2?.required).toEqual(
      expect.arrayContaining(["datasetId", "versionId", "items", "total"]),
    );
    expect(
      replayDatasetVersionCasesResponseV2?.properties?.versionId,
    ).toBeDefined();
    expect(
      replayDatasetVersionCasesResponseV2?.properties?.items,
    ).toBeDefined();
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
    expect(replayDatasetV2?.properties?.currentVersionId?.description).toContain(
      "当前生效",
    );
    expect(
      replayDatasetV2?.properties?.currentVersionNumber?.description,
    ).toContain("当前生效");
    expect(
      replayDatasetVersionsPath?.get?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayDatasetVersionListResponseV2");
    expect(
      replayDatasetVersionsPath?.post?.requestBody?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayDatasetVersionCreateInputV2");
    expect(
      replayDatasetVersionsPath?.post?.responses?.["201"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayDatasetVersionV2");
    expect(
      replayDatasetPromotePath?.post?.requestBody?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayDatasetVersionPromoteInputV2");
    expect(
      replayDatasetPromotePath?.post?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayDatasetVersionPromoteResponseV2");
    expect(replayDatasetVersionCreateInputV2?.required).toContain("datasetRef");
    expect(
      replayDatasetVersionCreateInputV2?.required?.includes("versionDatasetId"),
    ).toBe(false);
    expect(
      replayDatasetVersionCreateInputV2?.properties?.versionDatasetId?.description,
    ).toContain("兼容别名");
    expect(replayDatasetVersionListResponseV2?.required).toContain(
      "currentVersionId",
    );
    expect(replayDatasetVersionListResponseV2?.required).toContain(
      "currentVersionNumber",
    );
    expect(
      replayDatasetVersionListResponseV2?.properties?.items?.$ref,
    ).toBeUndefined();
    expect(replayDatasetVersionPromoteResponseV2?.required).toContain("dataset");
    expect(replayDatasetVersionPromoteResponseV2?.required).toContain("version");
    expect(
      replayDatasetVersionPromoteResponseV2?.properties?.version?.$ref,
    ).toBe("#/components/schemas/ReplayDatasetVersionV2");
    expect(
      replayExperimentPath?.post?.requestBody?.content?.["application/json"]
        ?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayExperimentInputV2");
    expect(
      replayExperimentPath?.post?.responses?.["201"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayExperimentV2");
    expect(
      replayExperimentPath?.get?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayExperimentListResponseV2");
    expect(
      replayExperimentPath?.get?.parameters?.some(
        (parameter: { name?: string; in?: string }) =>
          parameter.name === "datasetId" && parameter.in === "query",
      ),
    ).toBe(true);
    expect(replayExperimentInputV2?.required).toContain("datasetId");
    expect(replayExperimentInputV2?.required?.includes("baselineId")).toBe(false);
    expect(
      replayExperimentInputV2?.required?.includes("baselineVersionId"),
    ).toBe(false);
    expect(
      replayExperimentInputV2?.properties?.baselineId?.description,
    ).toContain("兼容别名");
    expect(
      replayExperimentInputV2?.properties?.baselineVersionId?.description,
    ).toContain("数据集版本");
    expect(replayExperimentV2?.required).toContain("baselineVersionId");
    expect(
      replayExperimentByIdPath?.patch?.requestBody?.content?.["application/json"]
        ?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayExperimentPatchInputV2");
    expect(
      replayExperimentByIdPath?.patch?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayExperimentV2");
    expect(replayExperimentPatchInputV2?.required ?? []).toEqual([]);
    expect(
      replayExperimentPatchInputV2?.properties?.baselineVersionId?.description,
    ).toContain("dataset version");
    expect(replayExperimentV2?.properties?.metadata?.$ref).toBe(
      "#/components/schemas/ReplayExperimentMetadataV2",
    );
    expect(replayExperimentV2?.properties?.runStatusSummary?.$ref).toBe(
      "#/components/schemas/ReplayExperimentStatusSummaryV2",
    );
    expect(replayExperimentV2?.properties?.aggregateSummary?.$ref).toBe(
      "#/components/schemas/ReplayExperimentAggregateSummaryV2",
    );
    expect(
      replayExperimentV2?.properties?.baselineVersionId?.description,
    ).toContain("metadata");
    expect(
      replayExperimentBatchComparePath?.get?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayExperimentBatchCompareResponseV2");
    expect(
      replayExperimentByIdPath?.get?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayExperimentV2");
    expect(
      replayExperimentComparePath?.get?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayExperimentCompareResponseV2");
    expect(
      replayExperimentWorkflowPath?.get?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayExperimentWorkflowResponseV2");
    expect(
      replayExperimentArtifactsPath?.get?.responses?.["200"]?.content?.[
        "application/json"
      ]?.schema?.$ref,
    ).toBe("#/components/schemas/ReplayExperimentArtifactsResponseV2");
    expect(replayExperimentCompareResponseV2?.properties?.summary?.$ref).toBe(
      "#/components/schemas/ReplayExperimentCompareSummaryV2",
    );
    expect(
      replayExperimentBatchCompareResponseV2?.properties?.summary?.$ref,
    ).toBe("#/components/schemas/ReplayExperimentBatchCompareSummaryV2");
    expect(
      replayExperimentWorkflowResponseV2?.properties?.nodes?.$ref,
    ).toBeUndefined();
    expect(
      replayExperimentArtifactsResponseV2?.properties?.items?.$ref,
    ).toBeUndefined();
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

    const listBaselineVersionsResponse = await app.request(
      `/api/v1/replay/baselines/${encodeURIComponent(baseline.id)}/versions`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(listBaselineVersionsResponse.status).toBe(200);
    const listBaselineVersionsBody =
      (await listBaselineVersionsResponse.json()) as {
        items: Array<{
          id: string;
          version: number;
          datasetId: string;
          model: string;
          sampleCount: number;
          promotedAt: string | null;
        }>;
        total: number;
        currentVersionId: string | null;
        currentVersionNumber: number | null;
      };
    expect(listBaselineVersionsBody.total).toBe(1);
    expect(listBaselineVersionsBody.currentVersionNumber).toBe(1);
    expect(listBaselineVersionsBody.items[0]?.version).toBe(1);
    expect(listBaselineVersionsBody.items[0]?.datasetId).toBe("golden-set-v1");
    expect(listBaselineVersionsBody.items[0]?.model).toBe("gpt-4.1");

    const createBaselineVersionResponse = await app.request(
      `/api/v1/replay/baselines/${encodeURIComponent(baseline.id)}/versions`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          datasetId: "golden-set-v2",
          model: "gpt-4.1-mini",
          promptVersion: "prompt-v2",
          sampleCount: 8,
          note: "promote-ready",
          metadata: {
            rollout: "candidate",
          },
        }),
      },
    );
    expect(createBaselineVersionResponse.status).toBe(201);
    const createdBaselineVersion =
      (await createBaselineVersionResponse.json()) as {
        id: string;
        version: number;
        datasetId: string;
        model: string;
        promptVersion?: string;
        sampleCount: number;
        note?: string;
      };
    expect(createdBaselineVersion.version).toBe(2);
    expect(createdBaselineVersion.datasetId).toBe("golden-set-v2");
    expect(createdBaselineVersion.model).toBe("gpt-4.1-mini");
    expect(createdBaselineVersion.promptVersion).toBe("prompt-v2");
    expect(createdBaselineVersion.sampleCount).toBe(8);
    expect(createdBaselineVersion.note).toBe("promote-ready");

    const listBaselineVersionsAfterCreateResponse = await app.request(
      `/api/v1/replay/baselines/${encodeURIComponent(baseline.id)}/versions`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(listBaselineVersionsAfterCreateResponse.status).toBe(200);
    const listBaselineVersionsAfterCreateBody =
      (await listBaselineVersionsAfterCreateResponse.json()) as {
        items: Array<{ id: string; version: number; promotedAt: string | null }>;
        total: number;
        currentVersionId: string | null;
        currentVersionNumber: number | null;
      };
    expect(listBaselineVersionsAfterCreateBody.total).toBe(2);
    expect(listBaselineVersionsAfterCreateBody.currentVersionNumber).toBe(1);
    expect(listBaselineVersionsAfterCreateBody.currentVersionId).toBe(
      listBaselineVersionsBody.currentVersionId,
    );
    expect(
      listBaselineVersionsAfterCreateBody.items.some(
        (item) => item.id === createdBaselineVersion.id && item.version === 2,
      ),
    ).toBe(true);

    const promoteBaselineVersionResponse = await app.request(
      `/api/v1/replay/baselines/${encodeURIComponent(baseline.id)}/promote`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          versionId: createdBaselineVersion.id,
        }),
      },
    );
    expect(promoteBaselineVersionResponse.status).toBe(200);
    const promoteBaselineVersionBody =
      (await promoteBaselineVersionResponse.json()) as {
        baseline: {
          id: string;
          datasetId: string;
          model: string;
          promptVersion?: string;
          sampleCount: number;
          currentVersionId: string;
          currentVersionNumber: number;
          metadata: Record<string, unknown>;
        } | null;
        version: {
          id: string;
          version: number;
          datasetId: string;
          model: string;
          promptVersion?: string;
          sampleCount: number;
          promotedAt: string | null;
        };
      };
    expect(promoteBaselineVersionBody.version.id).toBe(createdBaselineVersion.id);
    expect(promoteBaselineVersionBody.version.version).toBe(2);
    expect(promoteBaselineVersionBody.version.datasetId).toBe("golden-set-v2");
    expect(promoteBaselineVersionBody.version.promotedAt).toEqual(
      expect.any(String),
    );
    expect(promoteBaselineVersionBody.baseline?.datasetId).toBe("golden-set-v2");
    expect(promoteBaselineVersionBody.baseline?.model).toBe("gpt-4.1-mini");
    expect(promoteBaselineVersionBody.baseline?.promptVersion).toBe("prompt-v2");
    expect(promoteBaselineVersionBody.baseline?.sampleCount).toBe(8);
    expect(promoteBaselineVersionBody.baseline?.metadata.rollout).toBe("candidate");
    expect(promoteBaselineVersionBody.baseline?.currentVersionId).toBe(
      createdBaselineVersion.id,
    );
    expect(promoteBaselineVersionBody.baseline?.currentVersionNumber).toBe(2);

    const listBaselineVersionsAfterPromoteResponse = await app.request(
      `/api/v1/replay/baselines/${encodeURIComponent(baseline.id)}/versions`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(listBaselineVersionsAfterPromoteResponse.status).toBe(200);
    const listBaselineVersionsAfterPromoteBody =
      (await listBaselineVersionsAfterPromoteResponse.json()) as {
        items: Array<{ id: string; promotedAt: string | null }>;
        total: number;
        currentVersionId: string | null;
        currentVersionNumber: number | null;
      };
    expect(listBaselineVersionsAfterPromoteBody.total).toBe(2);
    expect(listBaselineVersionsAfterPromoteBody.currentVersionId).toBe(
      createdBaselineVersion.id,
    );
    expect(listBaselineVersionsAfterPromoteBody.currentVersionNumber).toBe(2);
    expect(
      listBaselineVersionsAfterPromoteBody.items.find(
        (item) => item.id === createdBaselineVersion.id,
      )?.promotedAt,
    ).toEqual(expect.any(String));

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
    const createEvaluationBody = (await createEvaluationResponse.json()) as {
      automation?: {
        triggered?: boolean;
        reason?: string;
      };
    };
    expect(createEvaluationBody.automation?.triggered).toBe(false);
    expect(createEvaluationBody.automation?.reason).toBe("score_within_threshold");

    const defaultAutomationPolicyResponse = await app.request(
      "/api/v2/quality/automation-policy",
      {
        headers: tenantAHeaders,
      },
    );
    expect(defaultAutomationPolicyResponse.status).toBe(200);
    const defaultAutomationPolicy =
      (await defaultAutomationPolicyResponse.json()) as {
        toolId: string;
        scope: string;
        decision: string;
        evaluationScoreThreshold: number;
        triggerOnEvaluationFailure: boolean;
        triggerOnReplayRegression: boolean;
        defaultActionType?: string;
      };
    expect(defaultAutomationPolicy.toolId).toBe("quality.replay.advice.execute");
    expect(defaultAutomationPolicy.scope).toBe("quality_replay_advice");
    expect(defaultAutomationPolicy.decision).toBe("allow");
    expect(defaultAutomationPolicy.evaluationScoreThreshold).toBe(80);
    expect(defaultAutomationPolicy.triggerOnEvaluationFailure).toBe(true);
    expect(defaultAutomationPolicy.triggerOnReplayRegression).toBe(true);
    expect(defaultAutomationPolicy.defaultActionType).toBe(
      "scorecard_adjustment",
    );

    const updateAutomationPolicyResponse = await app.request(
      "/api/v2/quality/automation-policy",
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          riskLevel: "high",
          decision: "require_approval",
          reason: "低分评估需要审批后再自动治理",
          evaluationScoreThreshold: 75,
          triggerOnEvaluationFailure: true,
          triggerOnReplayRegression: false,
          strategyMatrix: [
            {
              ruleId: "critical-replay",
              metric: "accuracy",
              severity: "critical",
              trendDirection: "down",
              provider: "github",
              workflow: "ci-main",
              projectPattern: "agentledger/*",
              minSampleCount: 8,
              minPassRate: 0.6,
              minConfidence: 0.6,
              regressionProbabilityAtLeast: 0.5,
              replayRegressionAtLeast: 1,
              actionType: "scorecard_adjustment",
              requiresApproval: true,
              cooldownMinutes: 30,
            },
          ],
        }),
      },
    );
    expect(updateAutomationPolicyResponse.status).toBe(200);
    const updatedAutomationPolicy =
      (await updateAutomationPolicyResponse.json()) as {
        evaluationScoreThreshold: number;
        triggerOnEvaluationFailure: boolean;
        triggerOnReplayRegression: boolean;
      };
    expect(updatedAutomationPolicy.evaluationScoreThreshold).toBe(75);
    expect(updatedAutomationPolicy.triggerOnEvaluationFailure).toBe(true);
    expect(updatedAutomationPolicy.triggerOnReplayRegression).toBe(false);
    expect(
      Array.isArray((updatedAutomationPolicy as { strategyMatrix?: unknown[] }).strategyMatrix),
    ).toBe(true);
    expect(
      ((updatedAutomationPolicy as {
        strategyMatrix?: Array<{
          ruleId?: string;
          provider?: string;
          workflow?: string;
          projectPattern?: string;
          minSampleCount?: number;
          minPassRate?: number;
          reason?: string;
        }>;
      }).strategyMatrix ?? [])[0],
    ).toMatchObject({
      ruleId: "critical-replay",
      provider: "github",
      workflow: "ci-main",
      projectPattern: "agentledger/*",
      minSampleCount: 8,
      minPassRate: 0.6,
    });

    const invalidAutomationPolicyResponse = await app.request(
      "/api/v2/quality/automation-policy",
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          riskLevel: "high",
          decision: "require_approval",
          reason: "非法矩阵应被阻断",
          evaluationScoreThreshold: 75,
          triggerOnEvaluationFailure: true,
          triggerOnReplayRegression: false,
          strategyMatrix: [
            {
              ruleId: "invalid-replay",
              metric: "accuracy",
              minConfidence: 1.2,
              actionType: "scorecard_adjustment",
              requiresApproval: true,
            },
          ],
        }),
      },
    );
    expect(invalidAutomationPolicyResponse.status).toBe(400);
    await expect(invalidAutomationPolicyResponse.json()).resolves.toEqual({
      message: "strategyMatrix[0].minConfidence 必须小于等于 1。",
    });

    const invalidAutomationPolicyPassRateResponse = await app.request(
      "/api/v2/quality/automation-policy",
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          riskLevel: "high",
          decision: "require_approval",
          reason: "非法矩阵 minPassRate 应被阻断",
          evaluationScoreThreshold: 75,
          triggerOnEvaluationFailure: true,
          triggerOnReplayRegression: false,
          strategyMatrix: [
            {
              ruleId: "invalid-pass-rate",
              metric: "accuracy",
              minPassRate: 1.2,
              actionType: "scorecard_adjustment",
              requiresApproval: true,
            },
          ],
        }),
      },
    );
    expect(invalidAutomationPolicyPassRateResponse.status).toBe(400);
    await expect(
      invalidAutomationPolicyPassRateResponse.json(),
    ).resolves.toEqual({
      message: "strategyMatrix[0].minPassRate 必须小于等于 1。",
    });

    const simulateAutomationPolicyResponse = await app.request(
      "/api/v2/quality/automation-policy/simulate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          metric: "accuracy",
          score: 63,
          sampleCount: 10,
          trendDirection: "down",
          provider: "github",
          workflow: "ci-main",
          project: `agentledger/${nonce}`,
          confidence: 0.82,
          regressionProbability: 0.73,
          replayRegressionCount: 2,
        }),
      },
    );
    expect(simulateAutomationPolicyResponse.status).toBe(200);
    const simulateAutomationPolicyBody =
      (await simulateAutomationPolicyResponse.json()) as {
        matchedRuleId?: string | null;
        matchedRule?: {
          minSampleCount?: number | null;
          minPassRate?: number | null;
        } | null;
        resolvedAction?: string | null;
        recommendedActionType?: string | null;
        requiresApproval?: boolean;
      };
    expect(simulateAutomationPolicyBody.matchedRuleId).toBe("critical-replay");
    expect(simulateAutomationPolicyBody.matchedRule?.minSampleCount).toBe(8);
    expect(simulateAutomationPolicyBody.matchedRule?.minPassRate).toBe(0.6);
    expect(simulateAutomationPolicyBody.resolvedAction).toBe(
      "scorecard_adjustment",
    );
    expect(simulateAutomationPolicyBody.recommendedActionType).toBe(
      "scorecard_adjustment",
    );
    expect(simulateAutomationPolicyBody.requiresApproval).toBe(true);

    const simulateAutomationPolicyMismatchResponse = await app.request(
      "/api/v2/quality/automation-policy/simulate",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          metric: "accuracy",
          score: 63,
          sampleCount: 10,
          trendDirection: "down",
          provider: "slack",
          workflow: "ci-other",
          project: `other/${nonce}`,
          confidence: 0.82,
          regressionProbability: 0.73,
          replayRegressionCount: 2,
        }),
      },
    );
    expect(simulateAutomationPolicyMismatchResponse.status).toBe(200);
    const simulateAutomationPolicyMismatchBody =
      (await simulateAutomationPolicyMismatchResponse.json()) as {
        matchedRuleId?: string | null;
        recommendedActionType?: string | null;
      };
    expect(simulateAutomationPolicyMismatchBody.matchedRuleId).toBeNull();
    expect(simulateAutomationPolicyMismatchBody.recommendedActionType).toBe(
      "scorecard_adjustment",
    );

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

    const automationEvaluationResponse = await app.request(
      "/api/v2/quality/evaluations",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          sessionId: `v2-sess-automation-${nonce}`,
          metric: "accuracy",
          score: 62,
          sampleCount: 10,
          occurredAt: "2026-03-04T12:30:00.000Z",
        }),
      },
    );
    expect(automationEvaluationResponse.status).toBe(201);
    const automationEvaluationBody =
      (await automationEvaluationResponse.json()) as {
        automation?: {
          triggered?: boolean;
          reason?: string;
          execution?: {
            executionId?: string;
            adviceExecutionId?: string;
            status?: string;
            result?: string;
            approvalRequestId?: string;
            advice?: {
              summary?: string;
            };
          };
        };
      };
    expect(automationEvaluationBody.automation?.triggered).toBe(true);
    expect(automationEvaluationBody.automation?.reason).toBe(
      "score_below_threshold",
    );
    expect(automationEvaluationBody.automation?.execution?.status).toBe(
      "blocked",
    );
    expect(automationEvaluationBody.automation?.execution?.result).toBe(
      "blocked",
    );
    expect(
      typeof automationEvaluationBody.automation?.execution?.adviceExecutionId,
    ).toBe("string");
    expect(
      typeof automationEvaluationBody.automation?.execution?.approvalRequestId,
    ).toBe("string");
    expect(
      automationEvaluationBody.automation?.execution?.advice?.summary,
    ).toContain("accuracy");

    const automationAdviceExecutionsResponse = await app.request(
      "/api/v2/quality/advice/executions?limit=20",
      {
        headers: tenantAHeaders,
      },
    );
    expect(automationAdviceExecutionsResponse.status).toBe(200);
    const automationAdviceExecutionsBody =
      (await automationAdviceExecutionsResponse.json()) as {
        items: Array<{
          id: string;
          triggerSource: string;
          actionType: string;
          status: string;
          metric?: string;
          resultSummary?: Record<string, unknown>;
        }>;
      };
    const automationAdviceExecution = automationAdviceExecutionsBody.items.find(
      (item) =>
        item.id === automationEvaluationBody.automation?.execution?.adviceExecutionId,
    );
    expect(automationAdviceExecution).toBeDefined();
    expect(automationAdviceExecution?.triggerSource).toBe("automatic");
    expect(
      ["pending", "running", "completed", "failed"].includes(
        automationAdviceExecution?.status ?? "",
      ),
    ).toBe(true);
    if (automationEvaluationBody.automation?.execution?.approvalRequestId) {
      expect(automationAdviceExecution?.resultSummary?.["approvalRequestId"]).toBe(
        automationEvaluationBody.automation?.execution?.approvalRequestId,
      );
    }

    const approveAutomationResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(
        automationEvaluationBody.automation?.execution?.approvalRequestId as string,
      )}/approve`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          reason: "approve automation advice execution",
        }),
      },
    );
    expect(approveAutomationResponse.status).toBe(200);
    const approveAutomationBody = (await approveAutomationResponse.json()) as {
      status?: string;
      continuedExecution?: {
        id?: string;
        status?: string;
        scorecardKey?: string;
      } | null;
    };
    expect(approveAutomationBody.status).toBe("approved");
    expect(approveAutomationBody.continuedExecution?.id).toBe(
      automationEvaluationBody.automation?.execution?.adviceExecutionId,
    );
    expect(approveAutomationBody.continuedExecution?.status).toBe("completed");
    expect(approveAutomationBody.continuedExecution?.scorecardKey).toBe(
      "accuracy",
    );

    const approvedAdviceExecutionResponse = await app.request(
      `/api/v2/quality/advice/executions/${encodeURIComponent(
        automationEvaluationBody.automation?.execution?.adviceExecutionId as string,
      )}`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(approvedAdviceExecutionResponse.status).toBe(200);
    const approvedAdviceExecutionBody =
      (await approvedAdviceExecutionResponse.json()) as {
        status?: string;
        scorecardKey?: string;
        resultSummary?: Record<string, unknown>;
      };
    expect(approvedAdviceExecutionBody.status).toBe("completed");
    expect(approvedAdviceExecutionBody.scorecardKey).toBe("accuracy");
    expect(approvedAdviceExecutionBody.resultSummary?.["targetScore"]).toBe(75);

    const rejectAutomationEvaluationResponse = await app.request(
      "/api/v2/quality/evaluations",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          sessionId: `v2-sess-automation-reject-${nonce}`,
          metric: "accuracy",
          score: 58,
          sampleCount: 8,
          occurredAt: "2026-03-04T13:00:00.000Z",
        }),
      },
    );
    expect(rejectAutomationEvaluationResponse.status).toBe(201);
    const rejectAutomationEvaluationBody =
      (await rejectAutomationEvaluationResponse.json()) as {
        automation?: {
          execution?: {
            adviceExecutionId?: string;
            approvalRequestId?: string;
          };
        };
      };
    expect(
      typeof rejectAutomationEvaluationBody.automation?.execution?.approvalRequestId,
    ).toBe("string");

    const rejectAutomationResponse = await app.request(
      `/api/v1/mcp/approvals/${encodeURIComponent(
        rejectAutomationEvaluationBody.automation?.execution?.approvalRequestId as string,
      )}/reject`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          reason: "reject automation advice execution",
        }),
      },
    );
    expect(rejectAutomationResponse.status).toBe(200);
    const rejectAutomationBody = (await rejectAutomationResponse.json()) as {
      status?: string;
      continuedExecution?: {
        id?: string;
        status?: string;
        error?: string;
      } | null;
    };
    expect(rejectAutomationBody.status).toBe("rejected");
    expect(rejectAutomationBody.continuedExecution?.id).toBe(
      rejectAutomationEvaluationBody.automation?.execution?.adviceExecutionId,
    );
    expect(rejectAutomationBody.continuedExecution?.status).toBe("failed");
    expect(rejectAutomationBody.continuedExecution?.error).toBe(
      "automation_approval_rejected",
    );

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

    const automationInvocationsResponse = await app.request(
      `/api/v1/mcp/invocations?toolId=${encodeURIComponent("quality.replay.advice.execute")}&limit=20`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(automationInvocationsResponse.status).toBe(200);
    const automationInvocationsBody =
      (await automationInvocationsResponse.json()) as {
        items: Array<{
          id: string;
          toolId: string;
          approvalRequestId?: string;
          metadata: Record<string, unknown>;
        }>;
      };
    expect(
      automationInvocationsBody.items.some(
        (item) =>
          item.id === automationEvaluationBody.automation?.execution?.executionId &&
          item.toolId === "quality.replay.advice.execute" &&
          item.metadata["source"] === "quality.v2.automation",
      ),
    ).toBe(true);
  });

  test("api-v2 quality 路由：forecast 与 advice", async () => {
    const nonce = createNonce("quality-v2-forecast-advice");
    const auth = await getDefaultAuthContext();
    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Quality Forecast Tenant ${nonce}`,
        slug: `quality-forecast-${nonce}`,
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

    const createEvaluationResponse = await app.request(
      "/api/v2/quality/evaluations",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          replayRunId: `forecast-run-${nonce}`,
          metric: "accuracy",
          score: 72,
          sampleCount: 12,
          occurredAt: "2026-03-05T10:00:00.000Z",
          externalSource: {
            provider: "github",
            repo: `agentledger/${nonce}`,
            workflow: "ci-main",
            runId: `run-${nonce}`,
          },
        }),
      },
    );
    expect(createEvaluationResponse.status).toBe(201);

    const forecastResponse = await app.request(
      "/api/v2/quality/reports/forecast?from=2026-03-05&to=2026-03-05&provider=github&workflow=ci-main&limit=10",
      {
        headers,
      },
    );
    expect(forecastResponse.status).toBe(200);
    const forecastBody = (await forecastResponse.json()) as {
      items: Array<{
        project: string;
        metric: string;
        predictedScore: number;
        confidence: number;
        modelVersion?: string;
        regressionProbability?: number;
        rationale?: string;
        confidenceLabel?: string;
        riskDrivers?: string[];
        featureContributions?: unknown[];
      }>;
      total: number;
    };
    expect(forecastBody.total).toBeGreaterThanOrEqual(1);
    expect(
      forecastBody.items.some(
        (item) =>
          item.project === `agentledger/${nonce}` &&
          item.metric === "all" &&
          item.predictedScore >= 0 &&
          item.confidence > 0 &&
          item.modelVersion === "quality-heuristic-v2" &&
          typeof item.regressionProbability === "number" &&
          typeof item.rationale === "string" &&
          ["low", "medium", "high"].includes(item.confidenceLabel ?? "") &&
          Array.isArray(item.riskDrivers) &&
          Array.isArray(item.featureContributions),
      ),
    ).toBe(true);

    const timeseriesFallbackResponse = await app.request(
      "/api/v2/quality/reports/forecast?from=2026-03-05&to=2026-03-05&provider=github&workflow=ci-main&limit=10&modelVersion=quality-timeseries-v1",
      {
        headers,
      },
    );
    expect(timeseriesFallbackResponse.status).toBe(200);
    const timeseriesFallbackBody =
      (await timeseriesFallbackResponse.json()) as {
        items: Array<{
          project: string;
          modelVersion?: string;
        }>;
        total: number;
      };
    expect(timeseriesFallbackBody.total).toBeGreaterThanOrEqual(1);
    expect(
      timeseriesFallbackBody.items.some(
        (item) =>
          item.project === `agentledger/${nonce}` &&
          item.modelVersion === "quality-heuristic-v2",
      ),
    ).toBe(true);

    const adviceResponse = await app.request(
      "/api/v2/quality/reports/advice?from=2026-03-05&to=2026-03-05&provider=github&workflow=ci-main",
      {
        headers,
      },
    );
    expect(adviceResponse.status).toBe(200);
    const adviceBody = (await adviceResponse.json()) as {
      items: Array<{
        project: string;
        severity: string;
        recommendation: string;
        explanation?: string;
        automationReadiness?: string;
        executionOptions?: Array<{ actionType: string; availability: string }>;
        strategyMatrixMatch?: string | null;
        recommendedPlan?: Record<string, unknown>;
        autoExecutionDecision?: string;
      }>;
      total: number;
    };
    expect(adviceBody.total).toBeGreaterThanOrEqual(1);
    expect(
      adviceBody.items.some(
        (item) =>
          item.project === `agentledger/${nonce}` &&
          ["info", "warn", "critical"].includes(item.severity) &&
          item.recommendation.length > 0 &&
          typeof item.explanation === "string" &&
          ["monitor_only", "manual_review", "ready_for_execution", "execution_in_progress"].includes(
            item.automationReadiness ?? "",
          ) &&
          Array.isArray(item.executionOptions) &&
          "strategyMatrixMatch" in item &&
          typeof item.recommendedPlan === "object" &&
          typeof item.autoExecutionDecision === "string",
      ),
    ).toBe(true);
  });

  test("api-v2 quality 路由：forecast 支持 quality-timeseries-v1", async () => {
    const nonce = createNonce("quality-v2-forecast-timeseries");
    const auth = await getDefaultAuthContext();
    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Quality Forecast Timeseries Tenant ${nonce}`,
        slug: `quality-forecast-timeseries-${nonce}`,
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

    const project = `agentledger/${nonce}`;
    const dates = [
      "2026-03-01",
      "2026-03-02",
      "2026-03-03",
      "2026-03-04",
      "2026-03-05",
    ];
    for (const date of dates) {
      for (let i = 0; i < 2; i += 1) {
        const createEvaluationResponse = await app.request(
          "/api/v2/quality/evaluations",
          {
            method: "POST",
            headers: {
              "content-type": "application/json",
              ...headers,
            },
            body: JSON.stringify({
              replayRunId: `forecast-run-${nonce}-${date}-${i}`,
              metric: "accuracy",
              score: 70 + i,
              sampleCount: 12,
              occurredAt: `${date}T10:00:00.000Z`,
              externalSource: {
                provider: "github",
                repo: project,
                workflow: "ci-main",
                runId: `run-${nonce}-${date}-${i}`,
              },
            }),
          },
        );
        expect(createEvaluationResponse.status).toBe(201);
      }
    }

    const forecastResponse = await app.request(
      "/api/v2/quality/reports/forecast?from=2026-03-01&to=2026-03-05&provider=github&workflow=ci-main&limit=10&modelVersion=quality-timeseries-v1",
      {
        headers,
      },
    );
    expect(forecastResponse.status).toBe(200);
    const forecastBody = (await forecastResponse.json()) as {
      items: Array<{
        project: string;
        metric: string;
        modelVersion?: string;
        predictedScore: number;
        confidence: number;
      }>;
      total: number;
    };
    expect(forecastBody.total).toBeGreaterThanOrEqual(1);
    expect(
      forecastBody.items.some(
        (item) =>
          item.project === project &&
          item.metric === "all" &&
          item.modelVersion === "quality-timeseries-v1" &&
          item.predictedScore >= 0 &&
          item.confidence > 0,
      ),
    ).toBe(true);
  });

  test("api-v2 quality 路由：automation policy 非法 modelVersion 应返回 400", async () => {
    const nonce = createNonce("quality-v2-automation-policy-model-version-invalid");
    const auth = await getDefaultAuthContext();
    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Quality Automation Policy Invalid ModelVersion Tenant ${nonce}`,
        slug: `quality-automation-policy-invalid-model-version-${nonce}`,
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

    const response = await app.request("/api/v2/quality/automation-policy", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...headers,
      },
      body: JSON.stringify({
        riskLevel: "high",
        decision: "allow",
        reason: "非法 modelVersion 应被阻断",
        modelVersion: `quality-timeseries-${nonce}`,
      }),
    });
    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toEqual({
      message: "modelVersion 必须是 quality-heuristic-v2/quality-timeseries-v1 之一。",
    });
  });

  test("api-v2 quality 路由：automation policy modelVersion=quality-timeseries-v1 触发 automation 时应包含 timeseries 信号字段", async () => {
    const nonce = createNonce("quality-v2-automation-timeseries-signals");
    const auth = await getDefaultAuthContext();
    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Quality Automation Timeseries Tenant ${nonce}`,
        slug: `quality-automation-timeseries-${nonce}`,
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

    const policyResponse = await app.request("/api/v2/quality/automation-policy", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...headers,
      },
      body: JSON.stringify({
        riskLevel: "high",
        decision: "require_approval",
        reason: "timeseries 模型下的自动治理需要审批",
        evaluationScoreThreshold: 80,
        triggerOnEvaluationFailure: true,
        triggerOnReplayRegression: false,
        modelVersion: "quality-timeseries-v1",
      }),
    });
    expect(policyResponse.status).toBe(200);
    const policyBody = (await policyResponse.json()) as {
      modelVersion?: string;
      evaluationScoreThreshold?: number;
      triggerOnEvaluationFailure?: boolean;
      triggerOnReplayRegression?: boolean;
    };
    expect(policyBody.modelVersion).toBe("quality-timeseries-v1");
    expect(policyBody.evaluationScoreThreshold).toBe(80);
    expect(policyBody.triggerOnEvaluationFailure).toBe(true);
    expect(policyBody.triggerOnReplayRegression).toBe(false);

    const project = `agentledger/${nonce}`;
    const dates = [
      "2026-03-01",
      "2026-03-02",
      "2026-03-03",
      "2026-03-04",
      "2026-03-05",
      "2026-03-06",
    ];
    for (const date of dates) {
      for (let i = 0; i < 2; i += 1) {
        const createEvaluationResponse = await app.request(
          "/api/v2/quality/evaluations",
          {
            method: "POST",
            headers: {
              "content-type": "application/json",
              ...headers,
            },
            body: JSON.stringify({
              replayRunId: `automation-ts-run-${nonce}-${date}-${i}`,
              metric: "accuracy",
              score: 90 - i,
              sampleCount: 12,
              occurredAt: `${date}T10:00:00.000Z`,
              externalSource: {
                provider: "github",
                repo: project,
                workflow: "ci-main",
                runId: `run-${nonce}-${date}-${i}`,
              },
            }),
          },
        );
        expect(createEvaluationResponse.status).toBe(201);
      }
    }

    const triggerResponse = await app.request("/api/v2/quality/evaluations", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...headers,
      },
      body: JSON.stringify({
        sessionId: `v2-sess-automation-timeseries-trigger-${nonce}`,
        metric: "accuracy",
        score: 52,
        sampleCount: 10,
        occurredAt: "2026-03-06T12:30:00.000Z",
        externalSource: {
          provider: "github",
          repo: project,
          workflow: "ci-main",
          runId: `run-${nonce}-trigger`,
        },
      }),
    });
    expect(triggerResponse.status).toBe(201);
    const triggerBody = (await triggerResponse.json()) as {
      automation?: {
        triggered?: boolean;
        reason?: string;
        execution?: {
          adviceExecutionId?: string;
          metadata?: Record<string, unknown>;
        } | null;
      };
    };
    expect(triggerBody.automation?.triggered).toBe(true);
    expect(triggerBody.automation?.reason).toBe("score_below_threshold");
    expect(typeof triggerBody.automation?.execution?.adviceExecutionId).toBe("string");

    const metadata = triggerBody.automation?.execution?.metadata ?? {};
    expect(metadata["modelVersion"]).toBe("quality-timeseries-v1");
    expect(["up", "down", "flat"].includes(String(metadata["trendDirection"]))).toBe(
      true,
    );
    expect(metadata["projectedDelta"]).toSatisfy(
      (value) => typeof value === "number" && Number.isFinite(value),
    );
    expect(metadata["basisWindowCount"]).toSatisfy(
      (value) => typeof value === "number" && Number.isInteger(value) && value >= 10,
    );
    expect(metadata["forecastWindowStart"]).toEqual(expect.any(String));
    expect(metadata["forecastWindowEnd"]).toEqual(expect.any(String));

    const adviceExecutionId = triggerBody.automation?.execution?.adviceExecutionId as string;
    const adviceExecutionResponse = await app.request(
      `/api/v2/quality/advice/executions/${encodeURIComponent(adviceExecutionId)}`,
      { headers },
    );
    expect(adviceExecutionResponse.status).toBe(200);
    const adviceExecutionBody = (await adviceExecutionResponse.json()) as {
      resultSummary?: Record<string, unknown>;
    };
    const resultSummary = adviceExecutionBody.resultSummary ?? {};
    expect(resultSummary["modelVersion"]).toBe("quality-timeseries-v1");
    expect(["up", "down", "flat"].includes(String(resultSummary["trendDirection"]))).toBe(
      true,
    );
    expect(resultSummary["projectedDelta"]).toSatisfy(
      (value) => typeof value === "number" && Number.isFinite(value),
    );
    expect(resultSummary["basisWindowCount"]).toSatisfy(
      (value) => typeof value === "number" && Number.isInteger(value) && value >= 10,
    );
  });

  test("api-v2 quality 路由：advice execute/list/cancel", async () => {
    const nonce = createNonce("quality-advice-execution");
    const auth = await getDefaultAuthContext();
    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Quality Advice Execution ${nonce}`,
        slug: `quality-advice-exec-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantResult, [201]);
    const tenantId = extractEntityId(tenantResult.payload);
    if (!tenantId) {
      throw new Error("quality advice execution 测试租户创建失败。");
    }
    const headers = await issueTenantScopedAuthHeaders(
      tenantId,
      auth.accessToken,
      auth.userId,
    );

    const evaluationResponse = await app.request("/api/v2/quality/evaluations", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...headers,
      },
      body: JSON.stringify({
        metric: "accuracy",
        score: 62,
        sampleCount: 10,
        occurredAt: "2026-03-05T11:00:00.000Z",
        externalSource: {
          provider: "github",
          repo: `repo-${nonce}`,
          workflow: "ci-advice",
          runId: `run-${nonce}`,
        },
      }),
    });
    expect(evaluationResponse.status).toBe(201);

    const adviceResponse = await app.request(
      "/api/v2/quality/reports/advice?from=2026-03-05&to=2026-03-05&provider=github&workflow=ci-advice",
      { headers },
    );
    expect(adviceResponse.status).toBe(200);
    const adviceBody = (await adviceResponse.json()) as {
      items: Array<{
        id: string;
        project: string;
        severity: "info" | "warn" | "critical";
      }>;
    };
    const advice = adviceBody.items[0];
    expect(advice).toBeDefined();

    const createReplayDatasetResponse = await app.request(
      "/api/v2/replay/datasets",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          name: `Advice Replay Dataset ${nonce}`,
          datasetRef: `quality-advice-dataset-${nonce}`,
          model: "gpt-4.1-mini",
          sampleCount: 6,
        }),
      },
    );
    expect(createReplayDatasetResponse.status).toBe(201);
    const replayDataset = (await createReplayDatasetResponse.json()) as {
      id: string;
      currentVersionId?: string | null;
    };

    const replaceAdviceCasesResponse = await app.request(
      `/api/v2/replay/datasets/${encodeURIComponent(replayDataset.id)}/cases`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          items: [
            {
              caseId: `case-${nonce}-1`,
              sortOrder: 1,
              input: "请总结退款流程",
              expectedOutput: "退款流程摘要",
            },
            {
              caseId: `case-${nonce}-2`,
              sortOrder: 2,
              input: "请给出用户注册指引",
              expectedOutput: "注册指引摘要",
            },
            {
              caseId: `case-${nonce}-3`,
              sortOrder: 3,
              input: "请说明 SLA 的含义",
              expectedOutput: "SLA 解释摘要",
            },
          ],
        }),
      },
    );
    expect(replaceAdviceCasesResponse.status).toBe(200);
    const replaceAdviceCasesBody = (await replaceAdviceCasesResponse.json()) as {
      total: number;
    };
    expect(replaceAdviceCasesBody.total).toBe(3);

    const executeResponse = await app.request(
      `/api/v2/quality/advice/${encodeURIComponent(advice.id)}/execute`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          project: advice.project,
          severity: advice.severity,
          actionType: "scorecard_adjustment",
          metric: "accuracy",
          targetScore: 81,
          warningScore: 72,
          criticalScore: 63,
        }),
      },
    );
    expect(executeResponse.status).toBe(201);
    const executeBody = (await executeResponse.json()) as {
      id: string;
      status: string;
      resultSummary?: { scorecardKey?: string };
    };
    expect(executeBody.status).toBe("completed");
    expect(executeBody.resultSummary?.scorecardKey).toBe("accuracy");

    const replayExecuteResponse = await app.request(
      `/api/v2/quality/advice/${encodeURIComponent(advice.id)}/execute`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          project: advice.project,
          severity: advice.severity,
          actionType: "replay_experiment",
          metric: "accuracy",
          datasetId: replayDataset.id,
          candidateLabels: ["candidate-a", "candidate-b"],
        }),
      },
    );
    expect(replayExecuteResponse.status).toBe(201);
    const replayExecuteBody = (await replayExecuteResponse.json()) as {
      id: string;
      status: string;
      experimentId?: string;
      resultSummary?: {
        experimentId?: string;
        baselineVersionId?: string | null;
        runIds?: string[];
        candidateLabels?: string[];
      };
    };
    expect(replayExecuteBody.status).toBe("completed");
    expect(replayExecuteBody.experimentId).toEqual(expect.any(String));
    expect(replayExecuteBody.resultSummary?.experimentId).toBe(
      replayExecuteBody.experimentId,
    );
    expect(replayExecuteBody.resultSummary?.baselineVersionId ?? null).toBe(
      replayDataset.currentVersionId ?? null,
    );
    expect(replayExecuteBody.resultSummary?.candidateLabels).toEqual([
      "candidate-a",
      "candidate-b",
    ]);
    expect(replayExecuteBody.resultSummary?.runIds?.length).toBe(2);
    for (const runId of replayExecuteBody.resultSummary?.runIds ?? []) {
      const runResponse = await app.request(
        `/api/v2/replay/runs/${encodeURIComponent(runId)}`,
        { headers },
      );
      expect(runResponse.status).toBe(200);
      const runBody = (await runResponse.json()) as { totalCases: number };
      expect(runBody.totalCases).toBe(3);
    }

    const listResponse = await app.request(
      `/api/v2/quality/advice/executions?adviceId=${encodeURIComponent(advice.id)}&limit=10`,
      { headers },
    );
    expect(listResponse.status).toBe(200);
    const listBody = (await listResponse.json()) as {
      items: Array<{ id: string }>;
      total: number;
    };
    expect(listBody.total).toBeGreaterThanOrEqual(2);
    expect(listBody.items.some((item) => item.id === executeBody.id)).toBe(true);
    expect(listBody.items.some((item) => item.id === replayExecuteBody.id)).toBe(true);

    const cancelResponse = await app.request(
      `/api/v2/quality/advice/executions/${encodeURIComponent(replayExecuteBody.id)}/cancel`,
      {
        method: "POST",
        headers,
      },
    );
    expect(cancelResponse.status).toBe(200);
    const cancelBody = (await cancelResponse.json()) as { status: string };
    expect(cancelBody.status).toBe("cancelled");
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
        currentVersionId?: string | null;
        currentVersionNumber?: number | null;
      };
      expect(dataset.tenantId).toBe(tenantAId);
      expect(dataset.datasetId).toBe(dataset.id);
      expect(dataset.datasetRef).toBe(`dataset-${nonce}`);
      expect(dataset.currentVersionNumber).toBe(1);
      expect(dataset.currentVersionId).toEqual(expect.any(String));

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

      const listVersionsResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/versions`,
        {
          headers: tenantAHeaders,
        },
      );
      expect(listVersionsResponse.status).toBe(200);
      const listVersionsBody = (await listVersionsResponse.json()) as {
        datasetId: string;
        currentVersionId: string | null;
        currentVersionNumber: number | null;
        total: number;
        items: Array<{
          id: string;
          version: number;
          datasetId: string;
          datasetRef: string | null;
          model: string;
          sampleCount: number;
        }>;
      };
      expect(listVersionsBody.datasetId).toBe(dataset.id);
      expect(listVersionsBody.total).toBe(1);
      expect(listVersionsBody.currentVersionNumber).toBe(1);
      expect(listVersionsBody.items[0]?.datasetRef).toBe(`dataset-${nonce}`);

      const createVersionResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/versions`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...tenantAHeaders,
          },
          body: JSON.stringify({
            datasetId: `dataset-version-${nonce}`,
            model: "gpt-4.1-mini",
            promptVersion: "v3",
            sampleCount: 9,
            note: "candidate-version",
            metadata: {
              rollout: "candidate",
            },
          }),
        },
      );
      expect(createVersionResponse.status).toBe(201);
      const createdVersion = (await createVersionResponse.json()) as {
        id: string;
        version: number;
        datasetId: string;
        datasetRef: string | null;
        model: string;
        promptVersion: string | null;
        sampleCount: number;
        note: string | null;
      };
      expect(createdVersion.version).toBe(2);
      expect(createdVersion.datasetId).toBe(dataset.id);
      expect(createdVersion.datasetRef).toBe(`dataset-version-${nonce}`);
      expect(createdVersion.model).toBe("gpt-4.1-mini");
      expect(createdVersion.promptVersion).toBe("v3");
      expect(createdVersion.sampleCount).toBe(9);
      expect(createdVersion.note).toBe("candidate-version");

      const promoteVersionResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/promote`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...tenantAHeaders,
          },
          body: JSON.stringify({
            versionId: createdVersion.id,
          }),
        },
      );
      expect(promoteVersionResponse.status).toBe(200);
      const promoteVersionBody = (await promoteVersionResponse.json()) as {
        dataset: {
          id: string;
          datasetRef?: string | null;
          model: string;
          promptVersion?: string | null;
          sampleCount?: number;
          currentVersionId?: string | null;
          currentVersionNumber?: number | null;
          metadata?: Record<string, unknown>;
        } | null;
        version: {
          id: string;
          version: number;
          promotedAt: string | null;
        };
      };
      expect(promoteVersionBody.dataset?.currentVersionId).toBe(createdVersion.id);
      expect(promoteVersionBody.dataset?.currentVersionNumber).toBe(2);
      expect(promoteVersionBody.dataset?.model).toBe("gpt-4.1-mini");
      expect(promoteVersionBody.dataset?.promptVersion).toBe("v3");
      expect(promoteVersionBody.dataset?.metadata?.rollout).toBe("candidate");
      expect(promoteVersionBody.version.promotedAt).toEqual(expect.any(String));

      const listVersionsAfterPromoteResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/versions`,
        {
          headers: tenantAHeaders,
        },
      );
      expect(listVersionsAfterPromoteResponse.status).toBe(200);
      const listVersionsAfterPromoteBody =
        (await listVersionsAfterPromoteResponse.json()) as {
          currentVersionId: string | null;
          currentVersionNumber: number | null;
          items: Array<{ id: string; promotedAt: string | null }>;
        };
      expect(listVersionsAfterPromoteBody.currentVersionId).toBe(createdVersion.id);
      expect(listVersionsAfterPromoteBody.currentVersionNumber).toBe(2);
      expect(
        listVersionsAfterPromoteBody.items.find((item) => item.id === createdVersion.id)
          ?.promotedAt,
      ).toEqual(expect.any(String));

      const listDatasetsResponse = await app.request(
        "/api/v2/replay/datasets?limit=20",
        {
          headers: tenantAHeaders,
        },
      );
      expect(listDatasetsResponse.status).toBe(200);
      const listDatasetsBody = (await listDatasetsResponse.json()) as {
        items: Array<{
          id: string;
          currentVersionId?: string | null;
          currentVersionNumber?: number | null;
        }>;
      };
      const listedDataset = listDatasetsBody.items.find((item) => item.id === dataset.id);
      expect(listedDataset).toBeTruthy();
      expect(listedDataset?.currentVersionId).toBe(createdVersion.id);
      expect(listedDataset?.currentVersionNumber).toBe(2);

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

  test("api-v2 replay 路由：baselineVersionId 使用版本快照并支持样本回溯", async () => {
    resetReplayJobExecutionWorkerForTests();
    try {
      const nonce = createNonce("replay-version-snapshot");
      const auth = await getDefaultAuthContext();
      const tenantResult = await createTenantByAuth(
        auth.accessToken,
        {
          name: `Replay Version Snapshot ${nonce}`,
          slug: `replay-version-snapshot-${nonce}`,
        },
        auth.userId,
      );
      assertApiStatus(tenantResult, [201]);
      const tenantId = extractEntityId(tenantResult.payload);
      if (!tenantId) {
        throw new Error("replay version snapshot 测试租户创建失败。");
      }
      const headers = await issueTenantScopedAuthHeaders(
        tenantId,
        auth.accessToken,
        auth.userId,
      );

      const createDatasetResponse = await app.request("/api/v2/replay/datasets", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          name: `Replay Version Dataset ${nonce}`,
          datasetRef: `dataset-version-${nonce}`,
          model: "gpt-4.1-mini",
          sampleCount: 4,
        }),
      });
      expect(createDatasetResponse.status).toBe(201);
      const dataset = (await createDatasetResponse.json()) as {
        id: string;
        currentVersionId?: string | null;
      };
      expect(dataset.currentVersionId).toEqual(expect.any(String));
      const initialVersionId = dataset.currentVersionId;
      if (!initialVersionId) {
        throw new Error("replay version snapshot 缺少 currentVersionId。");
      }

      const seedCasesResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/cases`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            items: [
              {
                caseId: "case-v1",
                input: "请总结退款流程",
                expectedOutput: "总结退款流程的关键步骤",
              },
            ],
          }),
        },
      );
      expect(seedCasesResponse.status).toBe(200);

      const initialVersionCasesResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/versions/${encodeURIComponent(
          initialVersionId,
        )}/cases`,
        {
          headers,
        },
      );
      expect(initialVersionCasesResponse.status).toBe(200);
      const initialVersionCasesBody = (await initialVersionCasesResponse.json()) as {
        versionId: string;
        total: number;
        items: Array<{ caseId: string; input: string }>;
      };
      expect(initialVersionCasesBody.versionId).toBe(initialVersionId);
      expect(initialVersionCasesBody.total).toBe(1);
      expect(initialVersionCasesBody.items).toEqual([
        expect.objectContaining({
          caseId: "case-v1",
          input: "请总结退款流程",
        }),
      ]);

      const createVersionResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/versions`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            datasetId: `dataset-version-snapshot-${nonce}`,
            note: "snapshot-before-edit",
          }),
        },
      );
      expect(createVersionResponse.status).toBe(201);
      const createdVersion = (await createVersionResponse.json()) as {
        id: string;
        version: number;
      };
      expect(createdVersion.version).toBe(2);

      const createdVersionCasesResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/versions/${encodeURIComponent(
          createdVersion.id,
        )}/cases`,
        {
          headers,
        },
      );
      expect(createdVersionCasesResponse.status).toBe(200);
      const createdVersionCasesBody = (await createdVersionCasesResponse.json()) as {
        total: number;
        items: Array<{ caseId: string; input: string }>;
      };
      expect(createdVersionCasesBody.total).toBe(1);
      expect(createdVersionCasesBody.items).toEqual([
        expect.objectContaining({
          caseId: "case-v1",
          input: "请总结退款流程",
        }),
      ]);

      const replaceCurrentCasesResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/cases`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            items: [
              {
                caseId: "case-v1",
                input: "请总结退款流程（已改写）",
                expectedOutput: "总结新版退款流程",
              },
              {
                caseId: "case-v2",
                input: "请说明发票申请步骤",
                expectedOutput: "说明发票申请步骤",
              },
            ],
          }),
        },
      );
      expect(replaceCurrentCasesResponse.status).toBe(200);

      const currentCasesResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/cases?limit=10`,
        {
          headers,
        },
      );
      expect(currentCasesResponse.status).toBe(200);
      const currentCasesBody = (await currentCasesResponse.json()) as {
        total: number;
        items: Array<{ caseId: string; input: string }>;
      };
      expect(currentCasesBody.total).toBe(2);
      expect(currentCasesBody.items).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            caseId: "case-v1",
            input: "请总结退款流程（已改写）",
          }),
          expect.objectContaining({
            caseId: "case-v2",
            input: "请说明发票申请步骤",
          }),
        ]),
      );

      const snapshotCasesResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/versions/${encodeURIComponent(
          createdVersion.id,
        )}/cases`,
        {
          headers,
        },
      );
      expect(snapshotCasesResponse.status).toBe(200);
      const snapshotCasesBody = (await snapshotCasesResponse.json()) as {
        total: number;
        items: Array<{ caseId: string; input: string }>;
      };
      expect(snapshotCasesBody.total).toBe(1);
      expect(snapshotCasesBody.items).toEqual([
        expect.objectContaining({
          caseId: "case-v1",
          input: "请总结退款流程",
        }),
      ]);

      const createRunResponse = await app.request("/api/v2/replay/runs", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          datasetId: dataset.id,
          candidateLabel: "candidate-version-snapshot",
          sampleLimit: 10,
          baselineVersionId: createdVersion.id,
        }),
      });
      expect(createRunResponse.status).toBe(201);
      const replayRun = (await createRunResponse.json()) as {
        id: string;
        status: string;
      };
      expect(replayRun.status).toBe("pending");

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
        totalCases: number;
        processedCases: number;
        summary?: {
          baselineVersionId?: string | null;
          executionSource?: string;
        };
      };
      expect(getRunBody.status).toBe("completed");
      expect(getRunBody.totalCases).toBe(1);
      expect(getRunBody.processedCases).toBe(1);
      expect(getRunBody.summary?.baselineVersionId ?? null).toBe(createdVersion.id);
      expect(getRunBody.summary?.executionSource).toBe("dataset_cases");

      const runArtifactsResponse = await app.request(
        `/api/v2/replay/runs/${encodeURIComponent(replayRun.id)}/artifacts/summary/download`,
        {
          headers,
        },
      );
      expect(runArtifactsResponse.status).toBe(200);
      const runArtifactsBody = (await runArtifactsResponse.json()) as {
        totalCases?: number;
        baselineVersionId?: string | null;
      };
      expect(runArtifactsBody.totalCases).toBe(1);
      expect(runArtifactsBody.baselineVersionId ?? null).toBe(createdVersion.id);

      const promoteVersionResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/promote`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            versionId: createdVersion.id,
          }),
        },
      );
      expect(promoteVersionResponse.status).toBe(200);

      const casesAfterPromoteResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/cases?limit=10`,
        {
          headers,
        },
      );
      expect(casesAfterPromoteResponse.status).toBe(200);
      const casesAfterPromoteBody = (await casesAfterPromoteResponse.json()) as {
        total: number;
        items: Array<{ caseId: string; input: string }>;
      };
      expect(casesAfterPromoteBody.total).toBe(1);
      expect(casesAfterPromoteBody.items).toEqual([
        expect.objectContaining({
          caseId: "case-v1",
          input: "请总结退款流程",
        }),
      ]);
    } finally {
      resetReplayJobExecutionWorkerForTests();
    }
  });

  test("api-v2 replay 路由：experiments create/list/detail", async () => {
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
        unchangedCases: 0,
      },
      diff: {
        items: [],
      },
    }));
    try {
      const nonce = createNonce("replay-experiment-routes");
      const auth = await getDefaultAuthContext();
      const tenantResult = await createTenantByAuth(
        auth.accessToken,
        {
          name: `Replay Experiment Tenant ${nonce}`,
          slug: `replay-experiment-${nonce}`,
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

      const createDatasetResponse = await app.request(
        "/api/v2/replay/datasets",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            name: `Replay Experiment Dataset ${nonce}`,
            datasetRef: `dataset-exp-${nonce}`,
            model: "gpt-4.1",
            sampleCount: 5,
          }),
        },
      );
      expect(createDatasetResponse.status).toBe(201);
      const dataset = (await createDatasetResponse.json()) as { id: string };

      const createRunResponse = await app.request("/api/v2/replay/runs", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          datasetId: dataset.id,
          candidateLabel: "candidate-exp",
          sampleLimit: 5,
        }),
      });
      expect(createRunResponse.status).toBe(201);
      const replayRun = (await createRunResponse.json()) as { id: string };

      await flushReplayJobExecutionQueueForTests();

      const createExperimentResponse = await app.request(
        "/api/v2/replay/experiments",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            name: `Experiment ${nonce}`,
            datasetId: dataset.id,
            baselineVersionId: "baseline-version-exp-1",
            runIds: [replayRun.id],
          }),
        },
      );
      expect(createExperimentResponse.status).toBe(201);
      const experiment = (await createExperimentResponse.json()) as {
        id: string;
        datasetId: string;
        baselineVersionId?: string | null;
        metadata?: {
          baselineVersionId?: string | null;
        };
        runIds: string[];
        summary: {
          totalRuns: number;
          baselineVersionId?: string | null;
        };
      };
      expect(experiment.datasetId).toBe(dataset.id);
      expect(experiment.baselineVersionId).toBe("baseline-version-exp-1");
      expect(experiment.metadata?.baselineVersionId).toBe("baseline-version-exp-1");
      expect(experiment.runIds).toEqual([replayRun.id]);
      expect(experiment.summary.totalRuns).toBe(1);
      expect(experiment.summary.baselineVersionId).toBe("baseline-version-exp-1");

      const listExperimentsResponse = await app.request(
        `/api/v2/replay/experiments?datasetId=${encodeURIComponent(dataset.id)}`,
        {
          headers,
        },
      );
      expect(listExperimentsResponse.status).toBe(200);
      const listExperimentsBody = (await listExperimentsResponse.json()) as {
        items: Array<{
          id: string;
          baselineVersionId?: string | null;
          metadata?: {
            baselineVersionId?: string | null;
          };
        }>;
        total: number;
      };
      expect(listExperimentsBody.total).toBe(1);
      const listedExperiment = listExperimentsBody.items.find(
        (item) => item.id === experiment.id,
      );
      expect(listedExperiment).toBeTruthy();
      expect(listedExperiment?.baselineVersionId).toBe("baseline-version-exp-1");
      expect(listedExperiment?.metadata?.baselineVersionId).toBe(
        "baseline-version-exp-1",
      );

      const getExperimentResponse = await app.request(
        `/api/v2/replay/experiments/${encodeURIComponent(experiment.id)}`,
        {
          headers,
        },
      );
      expect(getExperimentResponse.status).toBe(200);
      const getExperimentBody = (await getExperimentResponse.json()) as {
        id: string;
        baselineVersionId?: string | null;
        metadata?: {
          baselineVersionId?: string | null;
        };
        runs: Array<{ id: string }>;
      };
      expect(getExperimentBody.id).toBe(experiment.id);
      expect(getExperimentBody.baselineVersionId).toBe("baseline-version-exp-1");
      expect(getExperimentBody.metadata?.baselineVersionId).toBe(
        "baseline-version-exp-1",
      );
      expect(getExperimentBody.runs.some((item) => item.id === replayRun.id)).toBe(
        true,
      );

      const patchExperimentResponse = await app.request(
        `/api/v2/replay/experiments/${encodeURIComponent(experiment.id)}`,
        {
          method: "PATCH",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            name: `Experiment Updated ${nonce}`,
            baselineVersionId: "baseline-version-exp-2",
            candidateLabels: ["candidate-a", " candidate-b ", "candidate-a"],
          }),
        },
      );
      expect(patchExperimentResponse.status).toBe(200);
      const patchExperimentBody = (await patchExperimentResponse.json()) as {
        id: string;
        name: string;
        baselineVersionId?: string | null;
        metadata?: {
          baselineVersionId?: string | null;
        };
        candidateLabels?: string[];
      };
      expect(patchExperimentBody.id).toBe(experiment.id);
      expect(patchExperimentBody.name).toBe(`Experiment Updated ${nonce}`);
      expect(patchExperimentBody.baselineVersionId).toBe("baseline-version-exp-2");
      expect(patchExperimentBody.metadata?.baselineVersionId).toBe("baseline-version-exp-2");
      expect(patchExperimentBody.candidateLabels).toEqual(["candidate-a", "candidate-b"]);
    } finally {
      resetReplayJobExecutionWorkerForTests();
    }
  });

  test("api-v2 replay 路由：experiment run/cancel/results/artifacts", async () => {
    resetReplayJobExecutionWorkerForTests();
    setReplayJobExecutionHandlerForTests(async ({ replayJob }) => ({
      status: "completed",
      summary: {
        metric: "accuracy",
        totalCases: 3,
        processedCases: 3,
        improvedCases: 2,
        regressedCases: 1,
      },
      diffs: [
        {
          caseId: `case-${replayJob.id}`,
          metric: "accuracy",
          baselineScore: 0.72,
          candidateScore: 0.84,
          delta: 0.12,
          verdict: "improved",
        },
      ],
    }));
    try {
      const nonce = createNonce("replay-experiment-workflow");
      const auth = await getDefaultAuthContext();
      const tenantResult = await createTenantByAuth(
        auth.accessToken,
        {
          name: `Replay Experiment Workflow ${nonce}`,
          slug: `replay-exp-flow-${nonce}`,
        },
        auth.userId,
      );
      assertApiStatus(tenantResult, [201]);
      const tenantId = extractEntityId(tenantResult.payload);
      if (!tenantId) {
        throw new Error("replay experiment workflow 测试租户创建失败。");
      }
      const headers = await issueTenantScopedAuthHeaders(
        tenantId,
        auth.accessToken,
        auth.userId,
      );

      const datasetResponse = await app.request("/api/v2/replay/datasets", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          name: `dataset-${nonce}`,
          datasetRef: `dataset-ref-${nonce}`,
          model: "gpt-5",
        }),
      });
      expect(datasetResponse.status).toBe(201);
      const dataset = (await datasetResponse.json()) as {
        id: string;
        currentVersionId?: string | null;
      };
      expect(dataset.currentVersionId).toEqual(expect.any(String));

      const replaceCasesResponse = await app.request(
        `/api/v2/replay/datasets/${encodeURIComponent(dataset.id)}/cases`,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            items: [
              {
                caseId: `case-${nonce}-1`,
                sortOrder: 1,
                input: "请总结退款流程",
                expectedOutput: "退款流程摘要",
              },
              {
                caseId: `case-${nonce}-2`,
                sortOrder: 2,
                input: "请给出用户注册指引",
                expectedOutput: "注册指引摘要",
              },
              {
                caseId: `case-${nonce}-3`,
                sortOrder: 3,
                input: "请说明 SLA 的含义",
                expectedOutput: "SLA 解释摘要",
              },
            ],
          }),
        },
      );
      expect(replaceCasesResponse.status).toBe(200);
      const replacedCasesBody = (await replaceCasesResponse.json()) as {
        total: number;
      };
      expect(replacedCasesBody.total).toBe(3);

      const createExperimentResponse = await app.request(
        "/api/v2/replay/experiments",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            name: `Experiment ${nonce}`,
            datasetId: dataset.id,
            candidateLabels: ["candidate-a", "candidate-b"],
            autoRun: true,
          }),
        },
      );
      expect(createExperimentResponse.status).toBe(201);
      const experiment = (await createExperimentResponse.json()) as {
        id: string;
        status?: string;
        baselineVersionId?: string | null;
        runIds: string[];
        runs?: Array<{ totalCases: number }>;
      };
      expect(["queued", "running", "completed"]).toContain(
        experiment.status ?? "queued",
      );
      expect(experiment.baselineVersionId ?? null).toBe(dataset.currentVersionId ?? null);
      expect(experiment.runIds.length).toBeGreaterThanOrEqual(2);
      expect(experiment.runs?.every((run) => run.totalCases === 3)).toBe(true);

      const createCompareTargetResponse = await app.request(
        "/api/v2/replay/experiments",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            name: `Experiment Compare ${nonce}`,
            datasetId: dataset.id,
            candidateLabels: ["candidate-c"],
            autoRun: true,
          }),
        },
      );
      expect(createCompareTargetResponse.status).toBe(201);
      const compareTarget = (await createCompareTargetResponse.json()) as {
        id: string;
      };

      await flushReplayJobExecutionQueueForTests();

      const resultsResponse = await app.request(
        `/api/v2/replay/experiments/${encodeURIComponent(experiment.id)}/results`,
        { headers },
      );
      expect(resultsResponse.status).toBe(200);
      const resultsBody = (await resultsResponse.json()) as {
        id: string;
        status?: string;
        runIds: string[];
      };
      expect(resultsBody.id).toBe(experiment.id);
      expect(resultsBody.runIds.length).toBeGreaterThanOrEqual(2);
      expect(resultsBody.status).toBe("completed");

      const compareResponse = await app.request(
        `/api/v2/replay/experiments/${encodeURIComponent(experiment.id)}/compare`,
        { headers },
      );
      expect(compareResponse.status).toBe(200);
      const compareBody = (await compareResponse.json()) as {
        experimentId: string;
        total: number;
        summary: {
          totalRuns: number;
          completedRuns: number;
          bestRunId?: string | null;
        };
        items: Array<{
          runId: string;
          candidateLabel: string;
          passRate: number;
          netDelta: number;
        }>;
      };
      expect(compareBody.experimentId).toBe(experiment.id);
      expect(compareBody.total).toBeGreaterThanOrEqual(2);
      expect(compareBody.summary.totalRuns).toBe(compareBody.total);
      expect(compareBody.summary.completedRuns).toBeGreaterThanOrEqual(2);
      expect(compareBody.summary.bestRunId).toBeTruthy();
      expect(
        compareBody.items.every(
          (item) =>
            item.runId.length > 0 &&
            item.candidateLabel.length > 0 &&
            Number.isFinite(item.passRate) &&
            Number.isFinite(item.netDelta),
        ),
      ).toBe(true);

      const workflowResponse = await app.request(
        `/api/v2/replay/experiments/${encodeURIComponent(experiment.id)}/workflow`,
        { headers },
      );
      expect(workflowResponse.status).toBe(200);
      const workflowBody = (await workflowResponse.json()) as {
        experimentId: string;
        status: string;
        nodes: Array<{ id: string; type: string; label: string; metadata?: Record<string, unknown> }>;
        edges: Array<{ from: string; to: string; label: string }>;
        summary: {
          totalNodes: number;
          totalRuns: number;
          completedRuns: number;
        };
      };
      expect(workflowBody.experimentId).toBe(experiment.id);
      expect(["queued", "running", "completed", "failed", "cancelled"]).toContain(
        workflowBody.status,
      );
      expect(workflowBody.summary.totalRuns).toBeGreaterThanOrEqual(2);
      expect(workflowBody.summary.totalNodes).toBe(workflowBody.nodes.length);
      expect(
        workflowBody.nodes.some(
          (node) =>
            node.type === "experiment" &&
            node.label === `Experiment ${nonce}` &&
            node.metadata?.["datasetId"] === dataset.id,
        ),
      ).toBe(true);
      expect(workflowBody.edges.length).toBeGreaterThanOrEqual(2);

      const artifactsResponse = await app.request(
        `/api/v2/replay/experiments/${encodeURIComponent(experiment.id)}/artifacts`,
        { headers },
      );
      expect(artifactsResponse.status).toBe(200);
      const artifactsBody = (await artifactsResponse.json()) as {
        experimentId: string;
        total: number;
      };
      expect(artifactsBody.experimentId).toBe(experiment.id);
      expect(artifactsBody.total).toBeGreaterThanOrEqual(2);

      const batchCompareResponse = await app.request(
        `/api/v2/replay/experiments/compare?experimentIds=${encodeURIComponent(
          `${experiment.id},${compareTarget.id}`,
        )}&datasetId=${encodeURIComponent(dataset.id)}`,
        { headers },
      );
      expect(batchCompareResponse.status).toBe(200);
      const batchCompareBody = (await batchCompareResponse.json()) as {
        total: number;
        summary: {
          comparedExperimentCount: number;
          bestExperimentId?: string | null;
        };
        items: Array<{
          experimentId: string;
          runs: Array<{ runId: string; netDelta: number }>;
        }>;
      };
      expect(batchCompareBody.total).toBe(2);
      expect(batchCompareBody.summary.comparedExperimentCount).toBe(2);
      expect(
        batchCompareBody.items.some(
          (item) =>
            item.experimentId === experiment.id &&
            item.runs.length >= 1 &&
            item.runs.every((run) => typeof run.runId === "string"),
        ),
      ).toBe(true);
      expect(
        batchCompareBody.items.some(
          (item) =>
            item.experimentId === compareTarget.id &&
            item.runs.every((run) => Number.isFinite(run.netDelta)),
        ),
      ).toBe(true);

      const cancelResponse = await app.request(
        `/api/v2/replay/experiments/${encodeURIComponent(experiment.id)}/cancel`,
        {
          method: "POST",
          headers,
        },
      );
      expect(cancelResponse.status).toBe(200);
      const cancelBody = (await cancelResponse.json()) as { status?: string };
      expect(cancelBody.status).toBe("cancelled");
    } finally {
      resetReplayJobExecutionWorkerForTests();
    }
  });

  test("api-v2 replay 路由：experiments batch compare", async () => {
    resetReplayJobExecutionWorkerForTests();
    setReplayJobExecutionHandlerForTests(async ({ replayJob }) => {
      const parameters =
        replayJob.parameters && typeof replayJob.parameters === "object"
          ? (replayJob.parameters as Record<string, unknown>)
          : {};
      const candidateLabel =
        typeof parameters.candidateLabel === "string"
          ? parameters.candidateLabel
          : "candidate";
      const isPreferred = candidateLabel.includes("preferred");
      return {
        status: "completed",
        summary: {
          metric: "accuracy",
          totalCases: 4,
          processedCases: 4,
          improvedCases: isPreferred ? 3 : 1,
          regressedCases: isPreferred ? 0 : 2,
          unchangedCases: isPreferred ? 1 : 1,
        },
        diffs: [
          {
            caseId: `case-${replayJob.id}`,
            metric: "accuracy",
            baselineScore: 0.7,
            candidateScore: isPreferred ? 0.92 : 0.63,
            delta: isPreferred ? 0.22 : -0.07,
            verdict: isPreferred ? "improved" : "regressed",
          },
        ],
      };
    });
    try {
      const nonce = createNonce("replay-experiment-batch-compare");
      const auth = await getDefaultAuthContext();
      const tenantResult = await createTenantByAuth(
        auth.accessToken,
        {
          name: `Replay Experiment Compare ${nonce}`,
          slug: `replay-exp-compare-${nonce}`,
        },
        auth.userId,
      );
      assertApiStatus(tenantResult, [201]);
      const tenantId = extractEntityId(tenantResult.payload);
      if (!tenantId) {
        throw new Error("replay experiment batch compare 测试租户创建失败。");
      }
      const headers = await issueTenantScopedAuthHeaders(
        tenantId,
        auth.accessToken,
        auth.userId,
      );

      const datasetResponse = await app.request("/api/v2/replay/datasets", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          name: `dataset-${nonce}`,
          datasetRef: `dataset-ref-${nonce}`,
          model: "gpt-5",
        }),
      });
      expect(datasetResponse.status).toBe(201);
      const dataset = (await datasetResponse.json()) as { id: string };

      const preferredResponse = await app.request(
        "/api/v2/replay/experiments",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            name: `Preferred ${nonce}`,
            datasetId: dataset.id,
            candidateLabels: ["candidate-preferred"],
            autoRun: true,
          }),
        },
      );
      expect(preferredResponse.status).toBe(201);
      const preferredExperiment = (await preferredResponse.json()) as { id: string };

      const baselineResponse = await app.request(
        "/api/v2/replay/experiments",
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            ...headers,
          },
          body: JSON.stringify({
            name: `Baseline ${nonce}`,
            datasetId: dataset.id,
            candidateLabels: ["candidate-baseline"],
            autoRun: true,
          }),
        },
      );
      expect(baselineResponse.status).toBe(201);
      const baselineExperiment = (await baselineResponse.json()) as { id: string };

      await flushReplayJobExecutionQueueForTests();

      const compareResponse = await app.request(
        `/api/v2/replay/experiments/compare?experimentIds=${encodeURIComponent(
          `${preferredExperiment.id},${baselineExperiment.id}`,
        )}&datasetId=${encodeURIComponent(dataset.id)}`,
        { headers },
      );
      expect(compareResponse.status).toBe(200);
      const compareBody = (await compareResponse.json()) as {
        items: Array<{
          experimentId: string;
          bestRunId?: string | null;
          netDelta: number;
          workflowStage: string;
        }>;
        total: number;
        summary: {
          comparedExperimentCount: number;
          bestExperimentId?: string | null;
          worstExperimentId?: string | null;
        };
      };
      expect(compareBody.total).toBe(2);
      expect(compareBody.summary.comparedExperimentCount).toBe(2);
      expect(compareBody.summary.bestExperimentId).toBe(preferredExperiment.id);
      expect(compareBody.summary.worstExperimentId).toBe(baselineExperiment.id);
      expect(
        compareBody.items.some(
          (item) =>
            item.experimentId === preferredExperiment.id &&
            item.netDelta > 0 &&
            item.workflowStage === "completed",
        ),
      ).toBe(true);
      expect(
        compareBody.items.some(
          (item) =>
            item.experimentId === baselineExperiment.id &&
            item.netDelta < 0 &&
            item.workflowStage === "completed",
        ),
      ).toBe(true);
    } finally {
      resetReplayJobExecutionWorkerForTests();
    }
  });

  test("api-v2 residency kms-key-mappings 非法输入返回 400（错误码一致）", async () => {
    const nonce = createNonce("residency-v2-kms-invalid");
    const auth = await getDefaultAuthContext();
    if (!auth.userId) {
      throw new Error("无法解析 auth userId，无法执行 residency kms invalid 测试。");
    }

    const tenantResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `ResidencyV2 KMS Invalid ${nonce}`,
        slug: `residency-v2-kms-invalid-${nonce}`,
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
    const response = await app.request("/api/v2/residency/kms-key-mappings", {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        ...headers,
      },
      body: JSON.stringify({
        items: [
          {
            regionId: "",
            keyProvider: "kms",
            keyRef: "kms://invalid",
            enabled: true,
          },
        ],
      }),
    });
    expect(response.status).toBe(400);
    const body = await readResponseAsUnknown(response);
    if (isRecord(body)) {
      expect(pickString(body, ["message"])).toBe(
        "items[0].regionId 必填且必须为非空字符串。",
      );
    }
  });

  test("api-v2 residency 路由：策略、KMS/归档映射、区域映射、复制审批取消与租户隔离", async () => {
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

    const upsertKmsResponse = await app.request(
      "/api/v2/residency/kms-key-mappings",
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          items: [
            {
              regionId: "cn-hangzhou",
              keyProvider: "kms",
              keyRef: "kms://primary-cn-hangzhou",
              enabled: true,
            },
            {
              regionId: "cn-shanghai",
              keyProvider: "kms",
              keyRef: "kms://replica-cn-shanghai",
              enabled: true,
            },
          ],
        }),
      },
    );
    expect(upsertKmsResponse.status).toBe(200);
    const upsertKmsBody = (await upsertKmsResponse.json()) as {
      items: Array<{ regionId: string; keyRef: string }>;
      total: number;
    };
    expect(upsertKmsBody.total).toBe(2);
    expect(
      upsertKmsBody.items.some(
        (item) =>
          item.regionId === "cn-hangzhou" &&
          item.keyRef === "kms://primary-cn-hangzhou",
      ),
    ).toBe(true);

    const listKmsResponse = await app.request(
      "/api/v2/residency/kms-key-mappings",
      {
        headers: tenantAHeaders,
      },
    );
    expect(listKmsResponse.status).toBe(200);
    const listKmsBody = (await listKmsResponse.json()) as {
      items: Array<{ regionId: string }>;
      total: number;
    };
    expect(listKmsBody.total).toBe(2);

    const upsertArchivePolicyResponse = await app.request(
      "/api/v2/residency/archive-region-policies",
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          items: [
            {
              sourceRegion: "cn-hangzhou",
              archiveRegion: "ap-southeast-1",
              archiveClass: "cold",
              enabled: true,
            },
          ],
        }),
      },
    );
    expect(upsertArchivePolicyResponse.status).toBe(200);
    const upsertArchivePolicyBody = (await upsertArchivePolicyResponse.json()) as {
      items: Array<{ sourceRegion: string; archiveRegion: string; archiveClass: string }>;
      total: number;
    };
    expect(upsertArchivePolicyBody.total).toBe(1);
    expect(upsertArchivePolicyBody.items[0]?.sourceRegion).toBe("cn-hangzhou");
    expect(upsertArchivePolicyBody.items[0]?.archiveRegion).toBe("ap-southeast-1");
    expect(upsertArchivePolicyBody.items[0]?.archiveClass).toBe("cold");

    const listArchivePolicyResponse = await app.request(
      "/api/v2/residency/archive-region-policies",
      {
        headers: tenantAHeaders,
      },
    );
    expect(listArchivePolicyResponse.status).toBe(200);
    const listArchivePolicyBody = (await listArchivePolicyResponse.json()) as {
      items: Array<{ sourceRegion: string }>;
      total: number;
    };
    expect(listArchivePolicyBody.total).toBe(1);

    const invalidArchivePolicyResponse = await app.request(
      "/api/v2/residency/archive-region-policies",
      {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          ...tenantAHeaders,
        },
        body: JSON.stringify({
          items: [
            {
              sourceRegion: "cn-hangzhou",
              archiveRegion: "cn-hangzhou",
              archiveClass: "cold",
              enabled: true,
            },
          ],
        }),
      },
    );
    expect(invalidArchivePolicyResponse.status).toBe(400);

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

    const tenantBKmsResponse = await app.request(
      "/api/v2/residency/kms-key-mappings",
      {
        headers: tenantBHeaders,
      },
    );
    expect(tenantBKmsResponse.status).toBe(200);
    const tenantBKmsBody = (await tenantBKmsResponse.json()) as {
      items: Array<{ regionId: string }>;
      total: number;
    };
    expect(tenantBKmsBody.total).toBe(0);
    expect(tenantBKmsBody.items).toHaveLength(0);

    const tenantBArchiveResponse = await app.request(
      "/api/v2/residency/archive-region-policies",
      {
        headers: tenantBHeaders,
      },
    );
    expect(tenantBArchiveResponse.status).toBe(200);
    const tenantBArchiveBody = (await tenantBArchiveResponse.json()) as {
      items: Array<{ sourceRegion: string }>;
      total: number;
    };
    expect(tenantBArchiveBody.total).toBe(0);
    expect(tenantBArchiveBody.items).toHaveLength(0);
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

  test("agent lifecycle events create/list 支持租户隔离并写入审计", async () => {
    const nonce = createNonce("agent-lifecycle-events");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Agent Lifecycle Tenant A ${nonce}`,
        slug: `agent-lifecycle-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("agent lifecycle 租户 A 创建失败。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Agent Lifecycle Tenant B ${nonce}`,
        slug: `agent-lifecycle-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("agent lifecycle 租户 B 创建失败。");
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

    const badCreateResponse = await app.request(
      "/api/v1/agents/lifecycle-events",
      jsonRequest(
        "POST",
        {
          agentId: "agent-bad",
          action: "unknown",
          result: "success",
        },
        tenantAHeaders,
      ),
    );
    expect(badCreateResponse.status).toBe(400);

    const createResponse = await app.request(
      "/api/v1/agents/lifecycle-events",
      jsonRequest(
        "POST",
        {
          tenantId: tenantAId,
          agentId: `agent-${nonce}`,
          deviceId: `device-${nonce}`,
          hostname: `host-${nonce}`,
          version: "1.2.3",
          action: "doctor",
          result: "warn",
          occurredAt: "2026-03-08T12:00:00Z",
          metadata: {
            command: "doctor",
            overallStatus: "warn",
          },
        },
        tenantAHeaders,
      ),
    );
    expect(createResponse.status).toBe(201);
    const created = (await createResponse.json()) as {
      id: string;
      tenantId: string;
      agentId: string;
      action: string;
      result: string;
      metadata: Record<string, unknown>;
    };
    expect(created.tenantId).toBe(tenantAId);
    expect(created.agentId).toBe(`agent-${nonce}`);
    expect(created.action).toBe("doctor");
    expect(created.result).toBe("warn");
    expect(created.metadata.command).toBe("doctor");

    const listAResponse = await app.request(
      `/api/v1/agents/lifecycle-events?agentId=${encodeURIComponent(
        created.agentId,
      )}&action=doctor&result=warn&limit=10`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(listAResponse.status).toBe(200);
    const listABody = (await listAResponse.json()) as {
      items: Array<{ id: string; agentId: string; action: string; result: string }>;
      total: number;
      filters: { agentId?: string; action?: string; result?: string; limit?: number };
    };
    expect(listABody.total).toBeGreaterThanOrEqual(1);
    expect(listABody.filters.agentId).toBe(created.agentId);
    expect(listABody.filters.action).toBe("doctor");
    expect(listABody.filters.result).toBe("warn");
    expect(listABody.filters.limit).toBe(10);
    expect(
      listABody.items.some(
        (item) =>
          item.id === created.id &&
          item.agentId === created.agentId &&
          item.action === "doctor" &&
          item.result === "warn",
      ),
    ).toBe(true);

    const listBResponse = await app.request("/api/v1/agents/lifecycle-events", {
      headers: tenantBHeaders,
    });
    expect(listBResponse.status).toBe(200);
    const listBBody = (await listBResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(listBBody.items.some((item) => item.id === created.id)).toBe(false);

    const audits = await queryAuditByActionWithHeaders(
      "identity.agent_lifecycle_event.created",
      created.agentId,
      tenantAHeaders,
    );
    expect(
      audits.items.some(
        (item) =>
          item.action === "identity.agent_lifecycle_event.created" &&
          item.metadata.agentId === created.agentId &&
          item.metadata.tenantId === tenantAId,
      ),
    ).toBe(true);
  });

  test("audit legal hold create/list/release 支持重复保护与租户隔离", async () => {
    const nonce = createNonce("audit-legal-hold");
    const auth = await getDefaultAuthContext();

    const tenantAResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Audit Legal Hold Tenant A ${nonce}`,
        slug: `audit-legal-hold-a-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantAResult, [201]);
    const tenantAId = extractEntityId(tenantAResult.payload);
    if (!tenantAId) {
      throw new Error("audit legal hold 租户 A 创建失败。");
    }

    const tenantBResult = await createTenantByAuth(
      auth.accessToken,
      {
        name: `Audit Legal Hold Tenant B ${nonce}`,
        slug: `audit-legal-hold-b-${nonce}`,
      },
      auth.userId,
    );
    assertApiStatus(tenantBResult, [201]);
    const tenantBId = extractEntityId(tenantBResult.payload);
    if (!tenantBId) {
      throw new Error("audit legal hold 租户 B 创建失败。");
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
    const resourceId = `audit-export-${nonce}`;

    const createResponse = await app.request(
      "/api/v1/audits/legal-holds",
      jsonRequest(
        "POST",
        {
          resourceType: "audit_export",
          resourceId,
          reason: `hold reason ${nonce}`,
        },
        tenantAHeaders,
      ),
    );
    expect(createResponse.status).toBe(201);
    const created = (await createResponse.json()) as {
      id: string;
      tenantId: string;
      resourceType: string;
      resourceId: string;
      reason: string;
      releasedAt?: string;
    };
    expect(created.tenantId).toBe(tenantAId);
    expect(created.resourceType).toBe("audit_export");
    expect(created.resourceId).toBe(resourceId);

    const duplicateResponse = await app.request(
      "/api/v1/audits/legal-holds",
      jsonRequest(
        "POST",
        {
          resourceType: "audit_export",
          resourceId,
          reason: "duplicate hold",
        },
        tenantAHeaders,
      ),
    );
    expect(duplicateResponse.status).toBe(409);

    const listAResponse = await app.request(
      `/api/v1/audits/legal-holds?resourceType=audit_export&resourceId=${encodeURIComponent(
        resourceId,
      )}&active=true&limit=10`,
      {
        headers: tenantAHeaders,
      },
    );
    expect(listAResponse.status).toBe(200);
    const listABody = (await listAResponse.json()) as {
      items: Array<{ id: string; resourceId: string }>;
      total: number;
      filters: { resourceId?: string; active?: boolean; limit?: number };
    };
    expect(listABody.total).toBeGreaterThanOrEqual(1);
    expect(listABody.filters.resourceId).toBe(resourceId);
    expect(listABody.filters.active).toBe(true);
    expect(listABody.filters.limit).toBe(10);
    expect(listABody.items.some((item) => item.id === created.id)).toBe(true);

    const listBResponse = await app.request("/api/v1/audits/legal-holds", {
      headers: tenantBHeaders,
    });
    expect(listBResponse.status).toBe(200);
    const listBBody = (await listBResponse.json()) as {
      items: Array<{ id: string }>;
    };
    expect(listBBody.items.some((item) => item.id === created.id)).toBe(false);

    const releaseResponse = await app.request(
      `/api/v1/audits/legal-holds/${encodeURIComponent(created.id)}/release`,
      jsonRequest(
        "POST",
        {
          reason: `released ${nonce}`,
        },
        tenantAHeaders,
      ),
    );
    expect(releaseResponse.status).toBe(200);
    const released = (await releaseResponse.json()) as {
      id: string;
      releasedAt?: string;
      releaseReason?: string;
    };
    expect(released.id).toBe(created.id);
    expect(released.releasedAt).toBeDefined();
    expect(released.releaseReason).toBe(`released ${nonce}`);

    const secondReleaseResponse = await app.request(
      `/api/v1/audits/legal-holds/${encodeURIComponent(created.id)}/release`,
      jsonRequest("POST", {}, tenantAHeaders),
    );
    expect(secondReleaseResponse.status).toBe(409);

    const createAudits = await queryAuditByActionWithHeaders(
      "audit.legal_hold.create",
      resourceId,
      tenantAHeaders,
    );
    expect(
      createAudits.items.some(
        (item) =>
          item.action === "audit.legal_hold.create" &&
          item.metadata.resourceId === resourceId,
      ),
    ).toBe(true);

    const releaseAudits = await queryAuditByActionWithHeaders(
      "audit.legal_hold.release",
      resourceId,
      tenantAHeaders,
    );
    expect(
      releaseAudits.items.some(
        (item) =>
          item.action === "audit.legal_hold.release" &&
          item.metadata.resourceId === resourceId,
      ),
    ).toBe(true);
  });

  test("audit export/evidence bundle legal hold 可回显并阻止受保护资源覆盖", async () => {
    const nonce = createNonce("audit-export-legal-hold");
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
      throw new Error("repository.appendAuditLog 不可用，无法验证 Legal Hold 导出。");
    }

    await repositoryWithAudit.appendAuditLog({
      tenantId,
      eventId: `cp:audit-legal-hold-export:${nonce}`,
      action: "test.audit.legal_hold_export",
      level: "info",
      detail: `audit legal hold export ${nonce}`,
      metadata: {
        nonce,
      },
    });

    const exportResourceId = `audit-export-resource-${nonce}`;
    const exportResponse = await app.request(
      `/api/v1/audits/export?format=json&action=test.audit.legal_hold_export&keyword=${encodeURIComponent(
        nonce,
      )}&resourceId=${encodeURIComponent(exportResourceId)}`,
      {
        headers: authHeaders,
      },
    );
    expect(exportResponse.status).toBe(200);
    expect(exportResponse.headers.get("x-agentledger-resource-id")).toBe(
      exportResourceId,
    );
    expect(
      exportResponse.headers.get("x-agentledger-legal-hold-status"),
    ).toBe("none");
    const exportBody = (await exportResponse.json()) as {
      targetResource?: {
        resourceId?: string | null;
        legalHold?: Record<string, unknown> | null;
      };
    };
    expect(exportBody.targetResource?.resourceId).toBe(exportResourceId);
    expect(exportBody.targetResource?.legalHold ?? null).toBeNull();

    const exportHoldResponse = await app.request(
      "/api/v1/audits/legal-holds",
      jsonRequest(
        "POST",
        {
          resourceType: "audit_export",
          resourceId: exportResourceId,
          reason: `hold export ${nonce}`,
        },
        authHeaders,
      ),
    );
    expect(exportHoldResponse.status).toBe(201);

    const blockedExportResponse = await app.request(
      `/api/v1/audits/export?format=json&action=test.audit.legal_hold_export&keyword=${encodeURIComponent(
        nonce,
      )}&resourceId=${encodeURIComponent(exportResourceId)}`,
      {
        headers: authHeaders,
      },
    );
    expect(blockedExportResponse.status).toBe(409);
    const blockedExportBody = (await blockedExportResponse.json()) as {
      message: string;
      legalHold?: { resourceId?: string };
    };
    expect(blockedExportBody.message).toContain("Legal Hold");
    expect(blockedExportBody.legalHold?.resourceId).toBe(exportResourceId);

    const originalSigningKey = Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY;
    Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY = `evidence-signing-key-${nonce}`;
    try {
      const evidenceResourceId = `evidence-bundle-resource-${nonce}`;
      const evidenceResponse = await app.request(
        `/api/v1/audits/evidence-bundle?action=test.audit.legal_hold_export&keyword=${encodeURIComponent(
          nonce,
        )}&resourceId=${encodeURIComponent(evidenceResourceId)}`,
        {
          headers: authHeaders,
        },
      );
      expect(evidenceResponse.status).toBe(200);
      expect(evidenceResponse.headers.get("x-agentledger-resource-id")).toBe(
        evidenceResourceId,
      );
      const evidenceBody = (await evidenceResponse.json()) as {
        targetResource?: {
          resourceId?: string | null;
          legalHold?: Record<string, unknown> | null;
        };
      };
      expect(evidenceBody.targetResource?.resourceId).toBe(evidenceResourceId);
      expect(evidenceBody.targetResource?.legalHold ?? null).toBeNull();

      const evidenceHoldResponse = await app.request(
        "/api/v1/audits/legal-holds",
        jsonRequest(
          "POST",
          {
            resourceType: "evidence_bundle",
            resourceId: evidenceResourceId,
            reason: `hold evidence ${nonce}`,
          },
          authHeaders,
        ),
      );
      expect(evidenceHoldResponse.status).toBe(201);

      const blockedEvidenceResponse = await app.request(
        `/api/v1/audits/evidence-bundle?action=test.audit.legal_hold_export&keyword=${encodeURIComponent(
          nonce,
        )}&resourceId=${encodeURIComponent(evidenceResourceId)}`,
        {
          headers: authHeaders,
        },
      );
      expect(blockedEvidenceResponse.status).toBe(409);
      const blockedEvidenceBody = (await blockedEvidenceResponse.json()) as {
        message: string;
        legalHold?: { resourceId?: string };
      };
      expect(blockedEvidenceBody.message).toContain("Legal Hold");
      expect(blockedEvidenceBody.legalHold?.resourceId).toBe(evidenceResourceId);
    } finally {
      if (originalSigningKey === undefined) {
        delete Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY;
      } else {
        Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY = originalSigningKey;
      }
    }
  });
});
