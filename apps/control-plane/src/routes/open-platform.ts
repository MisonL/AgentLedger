import { createCipheriv, createDecipheriv, createHash, createHmac, randomBytes } from "node:crypto";
import { Hono, type Context } from "hono";
import {
  validateApiKeyCreateInput,
  validateApiKeyListInput,
  validateApiKeyRevokeInput,
  validateWebhookReplayRequestInput,
  validateWebhookReplayTaskListInput,
  validateWebhookEndpointCreateInput,
  validateWebhookEndpointUpdateInput,
} from "../contracts";
import type { WebhookEventType } from "../contracts";
import type {
  AppendAuditLogInput,
  ApiKey,
  WebhookEndpoint,
  WebhookReplayTask,
} from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const openPlatformRoutes = new Hono<AppEnv>();

const repository = getControlPlaneRepository();
const WRITABLE_ROLES = new Set(["owner", "maintainer"]);
const API_KEY_SCOPE_SET = new Set(["read", "write", "admin"]);
const WEBHOOK_STATUS_SET = new Set(["active", "paused", "disabled"]);
const WEBHOOK_EVENT_TYPE_SET = new Set<WebhookEventType>([
  "api_key.created",
  "api_key.revoked",
  "quality.event.created",
  "quality.scorecard.updated",
  "replay.run.started",
  "replay.run.completed",
  "replay.run.regression_detected",
  "replay.run.failed",
  "replay.run.cancelled",
  "replay.job.started",
  "replay.job.completed",
  "replay.job.failed",
]);
const WEBHOOK_REPLAY_EXECUTOR = "builtin-webhook-replay";
const WEBHOOK_REPLAY_HTTP_TIMEOUT_MS = 10_000;
const WEBHOOK_SECRET_ENCRYPTION_KEY_ENV = "OPEN_PLATFORM_WEBHOOK_SECRET_KEY";
const WEBHOOK_SECRET_ENCRYPTION_VERSION = "v1";
const WEBHOOK_SECRET_ENCRYPTION_FALLBACK_KEY = "agentledger-open-platform-webhook-secret-dev-key";
const WEBHOOK_REPLAY_MAX_RETRIES_ENV = "WEBHOOK_REPLAY_MAX_RETRIES";
const WEBHOOK_REPLAY_RETRY_BASE_DELAY_MS_ENV = "WEBHOOK_REPLAY_RETRY_BASE_DELAY_MS";
const WEBHOOK_REPLAY_RETRY_MAX_DELAY_MS_ENV = "WEBHOOK_REPLAY_RETRY_MAX_DELAY_MS";
const WEBHOOK_REPLAY_DEFAULT_MAX_RETRIES = 2;
const WEBHOOK_REPLAY_DEFAULT_RETRY_BASE_DELAY_MS = 200;
const WEBHOOK_REPLAY_DEFAULT_RETRY_MAX_DELAY_MS = 2_000;
const WEBHOOK_SIGNATURE_VERSION = "v1";
const WEBHOOK_SIGNATURE_HEADER = "x-agentledger-signature";
const WEBHOOK_SIGNATURE_TIMESTAMP_HEADER = "x-agentledger-signature-timestamp";
const WEBHOOK_SIGNATURE_ALGORITHM_HEADER = "x-agentledger-signature-algorithm";
const WEBHOOK_SIGNATURE_ALGORITHM = "hmac-sha256";

type ApiKeyScope = "read" | "write" | "admin";
type ApiKeyStatus = "active" | "revoked" | "expired";
type WebhookEndpointStatus = "active" | "paused" | "disabled";
type WebhookReplayTaskTerminalStatus = "completed" | "failed";
type WebhookReplayExecutionTask = {
  tenantId: string;
  replayTaskId: string;
};
type WebhookReplayExecutionResult = {
  status?: WebhookReplayTaskTerminalStatus;
  result?: Record<string, unknown>;
  error?: string;
};
type WebhookReplayDispatchResult = {
  attempts: number;
  retryCount: number;
};
type WebhookReplayDispatchError = Error & {
  attempts: number;
  retryCount: number;
};
type WebhookReplayExecutionHandler = (input: {
  tenantId: string;
  task: WebhookReplayTask;
  endpoint: WebhookEndpoint;
}) => Promise<WebhookReplayExecutionResult>;

const webhookReplayExecutionQueue: WebhookReplayExecutionTask[] = [];
let webhookReplayExecutionDrainScheduled = false;
let webhookReplayExecutionDrainRunning = false;
let webhookReplayExecutionHandler: WebhookReplayExecutionHandler =
  defaultWebhookReplayExecutionHandler;

function firstNonEmptyString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeStringRecord(input: unknown): Record<string, unknown> {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return {};
  }
  return input as Record<string, unknown>;
}

function toIsoString(value: unknown): string | undefined {
  const normalized = firstNonEmptyString(value);
  if (!normalized) {
    return undefined;
  }
  const timestamp = Date.parse(normalized);
  if (!Number.isFinite(timestamp)) {
    return undefined;
  }
  return new Date(timestamp).toISOString();
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function randomSecret(prefix: string): string {
  const normalizedPrefix = prefix.replace(/[^a-zA-Z0-9_]/g, "");
  return `${normalizedPrefix}_${crypto.randomUUID().replace(/-/g, "")}`;
}

function toPositiveInteger(
  value: unknown,
  defaultsTo: number,
  options?: {
    min?: number;
    max?: number;
  }
): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return defaultsTo;
  }
  const truncated = Math.trunc(parsed);
  const min = options?.min ?? 0;
  const max = options?.max ?? Number.MAX_SAFE_INTEGER;
  if (truncated < min) {
    return min;
  }
  if (truncated > max) {
    return max;
  }
  return truncated;
}

function resolveWebhookReplayMaxRetries(): number {
  return toPositiveInteger(Bun.env[WEBHOOK_REPLAY_MAX_RETRIES_ENV], WEBHOOK_REPLAY_DEFAULT_MAX_RETRIES, {
    min: 0,
    max: 8,
  });
}

function resolveWebhookReplayRetryBaseDelayMs(): number {
  return toPositiveInteger(
    Bun.env[WEBHOOK_REPLAY_RETRY_BASE_DELAY_MS_ENV],
    WEBHOOK_REPLAY_DEFAULT_RETRY_BASE_DELAY_MS,
    {
      min: 0,
      max: 60_000,
    }
  );
}

function resolveWebhookReplayRetryMaxDelayMs(baseDelayMs: number): number {
  return toPositiveInteger(
    Bun.env[WEBHOOK_REPLAY_RETRY_MAX_DELAY_MS_ENV],
    WEBHOOK_REPLAY_DEFAULT_RETRY_MAX_DELAY_MS,
    {
      min: baseDelayMs,
      max: 120_000,
    }
  );
}

function resolveWebhookSecretEncryptionKeyMaterial(): string {
  return (
    firstNonEmptyString(Bun.env[WEBHOOK_SECRET_ENCRYPTION_KEY_ENV]) ??
    firstNonEmptyString(Bun.env.AUTH_TOKEN_SECRET) ??
    WEBHOOK_SECRET_ENCRYPTION_FALLBACK_KEY
  );
}

function resolveWebhookSecretEncryptionKey(): Buffer {
  return createHash("sha256").update(resolveWebhookSecretEncryptionKeyMaterial()).digest();
}

function encryptWebhookSecret(secret: string): string {
  const key = resolveWebhookSecretEncryptionKey();
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  const encrypted = Buffer.concat([cipher.update(secret, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return [
    WEBHOOK_SECRET_ENCRYPTION_VERSION,
    iv.toString("base64url"),
    tag.toString("base64url"),
    encrypted.toString("base64url"),
  ].join(".");
}

function decryptWebhookSecret(secretCiphertext: string | undefined): string | undefined {
  const normalizedCiphertext = firstNonEmptyString(secretCiphertext);
  if (!normalizedCiphertext) {
    return undefined;
  }
  const [version, ivRaw, tagRaw, payloadRaw] = normalizedCiphertext.split(".");
  if (
    version !== WEBHOOK_SECRET_ENCRYPTION_VERSION ||
    !ivRaw ||
    !tagRaw ||
    !payloadRaw
  ) {
    return undefined;
  }
  try {
    const key = resolveWebhookSecretEncryptionKey();
    const iv = Buffer.from(ivRaw, "base64url");
    const tag = Buffer.from(tagRaw, "base64url");
    const payload = Buffer.from(payloadRaw, "base64url");
    const decipher = createDecipheriv("aes-256-gcm", key, iv);
    decipher.setAuthTag(tag);
    const secret = Buffer.concat([decipher.update(payload), decipher.final()]).toString("utf8");
    return firstNonEmptyString(secret);
  } catch {
    return undefined;
  }
}

function buildWebhookSignature(secret: string, timestampSeconds: string, body: string): string {
  const digest = createHmac("sha256", secret)
    .update(`${timestampSeconds}.${body}`)
    .digest("hex");
  return `${WEBHOOK_SIGNATURE_VERSION}=${digest}`;
}

function isWebhookReplayRetryableHttpStatus(statusCode: number): boolean {
  return statusCode === 408 || statusCode === 429 || statusCode >= 500;
}

function isWebhookReplayRetryableError(error: unknown): boolean {
  if (error instanceof DOMException) {
    return error.name === "AbortError";
  }
  if (error instanceof TypeError) {
    return true;
  }
  if (error instanceof Error) {
    const message = error.message.toLowerCase();
    return (
      message.includes("network") ||
      message.includes("fetch") ||
      message.includes("timeout") ||
      message.includes("timed out") ||
      message.includes("abort")
    );
  }
  return false;
}

function computeWebhookReplayRetryDelayMs(retryCount: number): number {
  const baseDelayMs = resolveWebhookReplayRetryBaseDelayMs();
  const maxDelayMs = resolveWebhookReplayRetryMaxDelayMs(baseDelayMs);
  const exponentialDelay = baseDelayMs * 2 ** Math.max(0, retryCount - 1);
  return Math.min(maxDelayMs, exponentialDelay);
}

async function sleep(ms: number): Promise<void> {
  if (ms <= 0) {
    return;
  }
  await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function createWebhookReplayDispatchError(
  error: unknown,
  attempts: number,
  retryCount: number
): WebhookReplayDispatchError {
  const wrapped = new Error(toWebhookReplayExecutionErrorMessage(error)) as WebhookReplayDispatchError;
  wrapped.attempts = attempts;
  wrapped.retryCount = retryCount;
  return wrapped;
}

function isWebhookReplayDispatchError(error: unknown): error is WebhookReplayDispatchError {
  if (!(error instanceof Error)) {
    return false;
  }
  const dispatchError = error as Partial<WebhookReplayDispatchError>;
  return (
    typeof dispatchError.attempts === "number" &&
    Number.isFinite(dispatchError.attempts) &&
    typeof dispatchError.retryCount === "number" &&
    Number.isFinite(dispatchError.retryCount)
  );
}

function resolveApiKeyScope(apiKey: ApiKey): ApiKeyScope {
  for (const scope of apiKey.scopes) {
    const lowered = scope.toLowerCase();
    if (API_KEY_SCOPE_SET.has(lowered)) {
      return lowered as ApiKeyScope;
    }
  }
  return "read";
}

function resolveApiKeyStatus(apiKey: ApiKey): ApiKeyStatus {
  if (apiKey.revokedAt) {
    return "revoked";
  }
  return "active";
}

function mapApiKeyItem(apiKey: ApiKey) {
  return {
    id: apiKey.id,
    tenantId: apiKey.tenantId,
    name: apiKey.name,
    scope: resolveApiKeyScope(apiKey),
    status: resolveApiKeyStatus(apiKey),
    keyPrefix: apiKey.keyHash.slice(0, 12),
    createdByUserId: undefined,
    lastUsedAt: apiKey.lastUsedAt,
    expiresAt: undefined,
    revokedAt: apiKey.revokedAt,
    metadata: {},
    createdAt: apiKey.createdAt,
    updatedAt: apiKey.updatedAt,
  };
}

function toWebhookStatus(enabled: boolean): WebhookEndpointStatus {
  return enabled ? "active" : "paused";
}

function mapWebhookEndpoint(endpoint: WebhookEndpoint) {
  return {
    id: endpoint.id,
    tenantId: endpoint.tenantId,
    name: endpoint.name,
    url: endpoint.url,
    events: endpoint.eventTypes,
    status: toWebhookStatus(endpoint.enabled),
    secretHint: endpoint.secretHash ? `${endpoint.secretHash.slice(0, 8)}***` : undefined,
    failureCount: 0,
    lastSuccessAt: undefined,
    lastFailureAt: undefined,
    createdAt: endpoint.createdAt,
    updatedAt: endpoint.updatedAt,
  };
}

function mapWebhookReplayTask(task: WebhookReplayTask) {
  return {
    id: task.id,
    tenantId: task.tenantId,
    webhookId: task.webhookId,
    status: task.status,
    dryRun: task.dryRun,
    filters: {
      eventType: task.filters.eventType,
      from: task.filters.from,
      to: task.filters.to,
      limit: task.filters.limit,
    },
    requestedAt: task.requestedAt,
    startedAt: task.startedAt,
    finishedAt: task.finishedAt,
    error: task.error,
    result: task.result,
  };
}

function toWebhookReplayExecutionErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    const message = error.message.trim();
    return message.length > 0 ? message : "Webhook 回放执行失败。";
  }
  if (typeof error === "string") {
    const message = error.trim();
    return message.length > 0 ? message : "Webhook 回放执行失败。";
  }
  return "Webhook 回放执行失败。";
}

function toWebhookEventType(value: unknown): WebhookEventType | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return undefined;
  }
  return WEBHOOK_EVENT_TYPE_SET.has(normalized as WebhookEventType)
    ? (normalized as WebhookEventType)
    : undefined;
}

function resolveWebhookReplayEventTypes(
  task: WebhookReplayTask,
  endpoint: WebhookEndpoint
): WebhookEventType[] {
  const requested = toWebhookEventType(task.filters.eventType);
  if (requested) {
    return [requested];
  }
  const normalizedEndpointTypes = endpoint.eventTypes
    .map((item) => toWebhookEventType(item))
    .filter((item): item is WebhookEventType => Boolean(item));
  if (normalizedEndpointTypes.length > 0) {
    return [...new Set(normalizedEndpointTypes)];
  }
  return [];
}

async function dispatchWebhookReplayEvent(input: {
  endpoint: WebhookEndpoint;
  task: WebhookReplayTask;
  replayEvent: {
    id: string;
    eventType: WebhookEventType;
    occurredAt: string;
    payload: Record<string, unknown>;
  };
}): Promise<WebhookReplayDispatchResult> {
  const payload = {
    id: input.replayEvent.id,
    eventType: input.replayEvent.eventType,
    occurredAt: input.replayEvent.occurredAt,
    replayTaskId: input.task.id,
    replayedAt: new Date().toISOString(),
    tenantId: input.task.tenantId,
    webhookId: input.task.webhookId,
    payload: input.replayEvent.payload,
  };
  const payloadBody = JSON.stringify(payload);
  const maxRetries = resolveWebhookReplayMaxRetries();
  let retryCount = 0;
  const signingSecret = decryptWebhookSecret(input.endpoint.secretCiphertext);

  const baseHeaders = new Headers({
    "content-type": "application/json",
    "x-agentledger-replay": "true",
    "x-agentledger-replay-task-id": input.task.id,
    "x-agentledger-event-type": input.replayEvent.eventType,
  });
  for (const [headerKey, headerValue] of Object.entries(input.endpoint.headers)) {
    const key = firstNonEmptyString(headerKey);
    const value = firstNonEmptyString(headerValue);
    if (!key || !value) {
      continue;
    }
    baseHeaders.set(key, value);
  }

  for (let attempt = 1; attempt <= maxRetries + 1; attempt += 1) {
    const headers = new Headers(baseHeaders);
    if (signingSecret) {
      const timestampSeconds = String(Math.floor(Date.now() / 1_000));
      headers.set(
        WEBHOOK_SIGNATURE_HEADER,
        buildWebhookSignature(signingSecret, timestampSeconds, payloadBody)
      );
      headers.set(WEBHOOK_SIGNATURE_TIMESTAMP_HEADER, timestampSeconds);
      headers.set(WEBHOOK_SIGNATURE_ALGORITHM_HEADER, WEBHOOK_SIGNATURE_ALGORITHM);
    }

    const abortController = new AbortController();
    const timeout = setTimeout(() => {
      abortController.abort();
    }, WEBHOOK_REPLAY_HTTP_TIMEOUT_MS);

    let retryableByStatus = false;
    try {
      const response = await fetch(input.endpoint.url, {
        method: "POST",
        headers,
        body: payloadBody,
        signal: abortController.signal,
      });
      if (response.ok) {
        return {
          attempts: attempt,
          retryCount,
        };
      }
      retryableByStatus = isWebhookReplayRetryableHttpStatus(response.status);
      const body = await response.text().catch(() => "");
      const bodySnippet = body.trim().slice(0, 200);
      throw new Error(
        bodySnippet
          ? `Webhook 回放投递失败（HTTP ${response.status}）：${bodySnippet}`
          : `Webhook 回放投递失败（HTTP ${response.status}）。`
      );
    } catch (error) {
      const retryable = retryableByStatus || isWebhookReplayRetryableError(error);
      if (retryable && attempt <= maxRetries) {
        retryCount += 1;
        await sleep(computeWebhookReplayRetryDelayMs(retryCount));
        continue;
      }
      throw createWebhookReplayDispatchError(error, attempt, retryCount);
    } finally {
      clearTimeout(timeout);
    }
  }

  throw createWebhookReplayDispatchError("Webhook 回放投递失败。", maxRetries + 1, retryCount);
}

async function defaultWebhookReplayExecutionHandler(input: {
  tenantId: string;
  task: WebhookReplayTask;
  endpoint: WebhookEndpoint;
}): Promise<WebhookReplayExecutionResult> {
  const eventTypes = resolveWebhookReplayEventTypes(input.task, input.endpoint);
  const replayEvents =
    eventTypes.length > 0
      ? await repository.listWebhookReplayEvents(input.tenantId, {
          eventTypes,
          from: input.task.filters.from,
          to: input.task.filters.to,
          limit: input.task.filters.limit,
        })
      : [];

  let dispatchedEvents = 0;
  let retriedEvents = 0;
  let retryCount = 0;
  const dispatchFailures: Array<{
    id: string;
    eventType: WebhookEventType;
    attempts: number;
    retryCount: number;
    error: string;
  }> = [];

  if (!input.task.dryRun) {
    for (const replayEvent of replayEvents) {
      const eventType = toWebhookEventType(replayEvent.eventType);
      if (!eventType) {
        continue;
      }
      try {
        const dispatchResult = await dispatchWebhookReplayEvent({
          endpoint: input.endpoint,
          task: input.task,
          replayEvent: {
            id: replayEvent.id,
            eventType,
            occurredAt: replayEvent.occurredAt,
            payload: normalizeStringRecord(replayEvent.payload),
          },
        });
        dispatchedEvents += 1;
        retryCount += dispatchResult.retryCount;
        if (dispatchResult.retryCount > 0) {
          retriedEvents += 1;
        }
      } catch (error) {
        const dispatchError = isWebhookReplayDispatchError(error)
          ? error
          : createWebhookReplayDispatchError(error, 1, 0);
        retryCount += dispatchError.retryCount;
        if (dispatchError.retryCount > 0) {
          retriedEvents += 1;
        }
        dispatchFailures.push({
          id: replayEvent.id,
          eventType,
          attempts: dispatchError.attempts,
          retryCount: dispatchError.retryCount,
          error: toWebhookReplayExecutionErrorMessage(dispatchError),
        });
      }
    }
  }

  const resultPayload: Record<string, unknown> = {
    executor: WEBHOOK_REPLAY_EXECUTOR,
    tenantId: input.tenantId,
    webhookId: input.endpoint.id,
    dryRun: input.task.dryRun,
    filters: input.task.filters,
    eventTypes,
    scannedEvents: replayEvents.length,
    dispatchedEvents,
    failedEvents: dispatchFailures.length,
    retriedEvents,
    retryCount,
    maxRetries: resolveWebhookReplayMaxRetries(),
    sampledEventIds: replayEvents.slice(0, 20).map((item) => item.id),
  };

  if (dispatchFailures.length > 0) {
    resultPayload.dispatchFailures = dispatchFailures.slice(0, 5);
    return {
      status: "failed",
      error: `Webhook 回放投递失败：${dispatchFailures.length}/${replayEvents.length} 条事件投递失败。`,
      result: resultPayload,
    };
  }

  return {
    status: "completed",
    result: resultPayload,
  };
}

async function runWebhookReplayExecutionTask(task: WebhookReplayExecutionTask): Promise<void> {
  const replayTask = await repository.getWebhookReplayTaskById(task.tenantId, task.replayTaskId);
  if (!replayTask || replayTask.status !== "queued") {
    return;
  }
  const endpoint = await repository.getWebhookEndpointById(task.tenantId, replayTask.webhookId);
  if (!endpoint) {
    await repository.updateWebhookReplayTask(task.tenantId, task.replayTaskId, {
      fromStatuses: ["queued"],
      status: "failed",
      error: `未找到 Webhook：${replayTask.webhookId}`,
      finishedAt: new Date().toISOString(),
      result: {
        executor: WEBHOOK_REPLAY_EXECUTOR,
        dryRun: replayTask.dryRun,
        scannedEvents: 0,
        dispatchedEvents: 0,
        failedEvents: 0,
        retriedEvents: 0,
        retryCount: 0,
        maxRetries: resolveWebhookReplayMaxRetries(),
      },
    });
    return;
  }
  if (!endpoint.enabled) {
    await repository.updateWebhookReplayTask(task.tenantId, task.replayTaskId, {
      fromStatuses: ["queued"],
      status: "failed",
      error: `Webhook ${endpoint.id} 已禁用，无法执行回放。`,
      finishedAt: new Date().toISOString(),
      result: {
        executor: WEBHOOK_REPLAY_EXECUTOR,
        dryRun: replayTask.dryRun,
        scannedEvents: 0,
        dispatchedEvents: 0,
        failedEvents: 0,
        retriedEvents: 0,
        retryCount: 0,
        maxRetries: resolveWebhookReplayMaxRetries(),
      },
    });
    return;
  }

  const runningTask = await repository.updateWebhookReplayTask(task.tenantId, task.replayTaskId, {
    fromStatuses: ["queued"],
    status: "running",
    startedAt: new Date().toISOString(),
    finishedAt: null,
    error: null,
    result: {
      executor: WEBHOOK_REPLAY_EXECUTOR,
      dryRun: replayTask.dryRun,
      scannedEvents: 0,
      dispatchedEvents: 0,
      failedEvents: 0,
      retriedEvents: 0,
      retryCount: 0,
      maxRetries: resolveWebhookReplayMaxRetries(),
    },
  });
  if (!runningTask) {
    return;
  }

  try {
    const execution = await webhookReplayExecutionHandler({
      tenantId: task.tenantId,
      task: runningTask,
      endpoint,
    });
    const finishedAt = new Date().toISOString();
    if ((execution.status ?? "completed") === "completed") {
      await repository.updateWebhookReplayTask(task.tenantId, task.replayTaskId, {
        fromStatuses: ["running"],
        status: "completed",
        result: {
          ...runningTask.result,
          ...(execution.result ?? {}),
        },
        error: null,
        finishedAt,
      });
      return;
    }

    await repository.updateWebhookReplayTask(task.tenantId, task.replayTaskId, {
      fromStatuses: ["running"],
      status: "failed",
      result: {
        ...runningTask.result,
        ...(execution.result ?? {}),
      },
      error: firstNonEmptyString(execution.error) ?? "Webhook 回放执行失败。",
      finishedAt,
    });
  } catch (error) {
    await repository.updateWebhookReplayTask(task.tenantId, task.replayTaskId, {
      fromStatuses: ["running"],
      status: "failed",
      error: toWebhookReplayExecutionErrorMessage(error),
      finishedAt: new Date().toISOString(),
    });
  }
}

async function drainWebhookReplayExecutionQueue(): Promise<void> {
  if (webhookReplayExecutionDrainRunning) {
    return;
  }
  webhookReplayExecutionDrainRunning = true;

  try {
    while (webhookReplayExecutionQueue.length > 0) {
      const task = webhookReplayExecutionQueue.shift();
      if (!task) {
        continue;
      }
      try {
        await runWebhookReplayExecutionTask(task);
      } catch (error) {
        console.warn("[control-plane] webhook replay worker 执行失败。", error);
      }
    }
  } finally {
    webhookReplayExecutionDrainRunning = false;
    if (webhookReplayExecutionQueue.length > 0) {
      scheduleWebhookReplayExecutionDrain();
    }
  }
}

function scheduleWebhookReplayExecutionDrain(): void {
  if (webhookReplayExecutionDrainScheduled) {
    return;
  }
  webhookReplayExecutionDrainScheduled = true;
  setTimeout(() => {
    webhookReplayExecutionDrainScheduled = false;
    void drainWebhookReplayExecutionQueue();
  }, 0);
}

export function enqueueWebhookReplayExecution(tenantId: string, replayTaskId: string): void {
  const normalizedTenantId = firstNonEmptyString(tenantId);
  const normalizedReplayTaskId = firstNonEmptyString(replayTaskId);
  if (!normalizedTenantId || !normalizedReplayTaskId) {
    return;
  }
  webhookReplayExecutionQueue.push({
    tenantId: normalizedTenantId,
    replayTaskId: normalizedReplayTaskId,
  });
  scheduleWebhookReplayExecutionDrain();
}

export async function flushWebhookReplayExecutionQueueForTests(): Promise<void> {
  await drainWebhookReplayExecutionQueue();
}

export function setWebhookReplayExecutionHandlerForTests(
  handler?: WebhookReplayExecutionHandler
): void {
  webhookReplayExecutionHandler = handler ?? defaultWebhookReplayExecutionHandler;
}

export function resetWebhookReplayExecutionWorkerForTests(): void {
  webhookReplayExecutionHandler = defaultWebhookReplayExecutionHandler;
  webhookReplayExecutionQueue.length = 0;
  webhookReplayExecutionDrainScheduled = false;
  webhookReplayExecutionDrainRunning = false;
}

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 open-platform 审计日志失败。", error);
  }
}

function unauthorized(c: Context<AppEnv>) {
  return c.json({ message: "未认证：请先登录。" }, 401);
}

function forbidden(c: Context<AppEnv>, mode: "read" | "write") {
  if (mode === "write") {
    return c.json({ message: "无写入权限：仅 owner/maintainer 可执行写操作。" }, 403);
  }
  return c.json({ message: "无权访问该租户资源。" }, 403);
}

async function requireAuthContext(c: Context<AppEnv>) {
  const authResult = await authMiddleware(c, async () => {});
  if (authResult instanceof Response) {
    return authResult;
  }
  const auth = c.get("auth");
  if (!auth) {
    return unauthorized(c);
  }
  return auth;
}

async function requireTenantAccess(c: Context<AppEnv>, mode: "read" | "write") {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }
  const membership = await repository.getTenantMemberByUser(auth.tenantId, auth.userId);
  if (!membership) {
    return forbidden(c, mode);
  }
  if (mode === "write" && !WRITABLE_ROLES.has(membership.tenantRole)) {
    return forbidden(c, mode);
  }
  return auth;
}

export function buildOpenApiDocument() {
  const standardErrorSchema = {
    $ref: "#/components/schemas/ErrorResponse",
  };

  return {
    openapi: "3.0.3",
    info: {
      title: "AgentLedger Control Plane API",
      version: "1.2.0",
      description: "Open Platform / Quality / Replay 核心路径文档。",
    },
    tags: [
      { name: "open-platform", description: "开放平台与 Webhook 能力" },
      { name: "quality", description: "质量治理能力" },
      { name: "replay", description: "回放评测能力" },
      { name: "residency", description: "数据主权与跨区复制治理能力" },
    ],
    security: [{ bearerAuth: [] }],
    paths: {
      "/api/v1/openapi.json": {
        get: {
          summary: "获取 OpenAPI 文档",
          operationId: "getOpenApiDocument",
          tags: ["open-platform"],
          responses: {
            "200": {
              description: "OpenAPI 文档",
              content: {
                "application/json": {
                  schema: {
                    $ref: "#/components/schemas/OpenApiDocument",
                  },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
          },
        },
      },
      "/api/v1/api-keys": {
        get: {
          summary: "列出 API Keys",
          operationId: "listApiKeys",
          tags: ["open-platform"],
          parameters: [
            { $ref: "#/components/parameters/PageLimit" },
            { $ref: "#/components/parameters/PageCursor" },
            {
              name: "scope",
              in: "query",
              description: "按 scope 过滤",
              schema: { type: "string", enum: ["read", "write", "admin"] },
            },
            {
              name: "status",
              in: "query",
              description: "按状态过滤",
              schema: { type: "string", enum: ["active", "revoked", "expired"] },
            },
            {
              name: "keyword",
              in: "query",
              description: "名称/前缀关键字",
              schema: { type: "string" },
            },
            {
              name: "from",
              in: "query",
              description: "创建时间起点（ISO 日期）",
              schema: { type: "string", format: "date-time" },
            },
            {
              name: "to",
              in: "query",
              description: "创建时间终点（ISO 日期）",
              schema: { type: "string", format: "date-time" },
            },
          ],
          responses: {
            "200": {
              description: "API Key 列表",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ApiKeyListResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        post: {
          summary: "创建 API Key",
          operationId: "createApiKey",
          tags: ["open-platform"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ApiKeyCreateRequest" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ApiKeyCreateResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/api-keys/{id}/revoke": {
        post: {
          summary: "吊销 API Key",
          operationId: "revokeApiKey",
          tags: ["open-platform"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: false,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ApiKeyRevokeRequest" },
              },
            },
          },
          responses: {
            "200": {
              description: "吊销成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ApiKey" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/webhooks": {
        get: {
          summary: "列出 Webhook",
          operationId: "listWebhookEndpoints",
          tags: ["open-platform"],
          parameters: [
            { $ref: "#/components/parameters/PageLimit" },
            {
              name: "status",
              in: "query",
              description: "按状态过滤",
              schema: { type: "string", enum: ["active", "paused", "disabled"] },
            },
            {
              name: "keyword",
              in: "query",
              description: "Webhook 名称关键字",
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "Webhook 列表",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/WebhookListResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        post: {
          summary: "创建 Webhook",
          operationId: "createWebhookEndpoint",
          tags: ["open-platform"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/WebhookCreateRequest" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/WebhookCreateResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "409": { $ref: "#/components/responses/ConflictError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/webhooks/{id}": {
        put: {
          summary: "更新 Webhook",
          operationId: "updateWebhookEndpoint",
          tags: ["open-platform"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/WebhookUpdateRequest" },
              },
            },
          },
          responses: {
            "200": {
              description: "更新成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/WebhookCreateResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        delete: {
          summary: "删除 Webhook",
          operationId: "deleteWebhookEndpoint",
          tags: ["open-platform"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "删除结果",
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    required: ["success"],
                    properties: {
                      success: { type: "boolean" },
                    },
                  },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/webhooks/{id}/replay": {
        post: {
          summary: "重放 Webhook",
          operationId: "replayWebhookEndpoint",
          tags: ["open-platform"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: false,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/WebhookReplayRequest" },
              },
            },
          },
          responses: {
            "202": {
              description: "已接收重放请求",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/WebhookReplayTask" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/webhooks/replay-tasks": {
        get: {
          summary: "查询 Webhook 回放任务",
          operationId: "listWebhookReplayTasks",
          tags: ["open-platform"],
          parameters: [
            { $ref: "#/components/parameters/PageLimit" },
            {
              name: "webhookId",
              in: "query",
              description: "按 webhookId 过滤",
              schema: { type: "string" },
            },
            {
              name: "status",
              in: "query",
              description: "按任务状态过滤",
              schema: { type: "string", enum: ["queued", "running", "completed", "failed"] },
            },
            {
              name: "cursor",
              in: "query",
              description: "分页游标",
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "任务列表",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/WebhookReplayTaskListResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/webhooks/replay-tasks/{id}": {
        get: {
          summary: "获取单个 Webhook 回放任务",
          operationId: "getWebhookReplayTask",
          tags: ["open-platform"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "任务详情",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/WebhookReplayTask" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/quality/events": {
        post: {
          summary: "上报质量事件",
          operationId: "createQualityEvent",
          deprecated: true,
          description: "兼容接口，建议迁移到 /api/v2/quality/evaluations。",
          tags: ["quality"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/QualityEventInput" },
              },
            },
          },
          responses: {
            "201": {
              description: "事件已创建",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/QualityEvent" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/quality/metrics/daily": {
        get: {
          summary: "查询质量日报",
          operationId: "listQualityDailyMetrics",
          deprecated: true,
          description: "兼容接口，建议迁移到 /api/v2/quality/metrics。",
          tags: ["quality"],
          parameters: [
            { $ref: "#/components/parameters/PageLimit" },
            {
              name: "from",
              in: "query",
              description: "开始时间（ISO 或 yyyy-mm-dd）",
              schema: { type: "string" },
            },
            {
              name: "to",
              in: "query",
              description: "结束时间（ISO 或 yyyy-mm-dd）",
              schema: { type: "string" },
            },
            {
              name: "metric",
              in: "query",
              description: "质量指标",
              schema: {
                type: "string",
                enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
              },
            },
            {
              name: "provider",
              in: "query",
              description: "外部来源 provider",
              schema: { type: "string" },
            },
            {
              name: "repo",
              in: "query",
              description: "外部来源 repo",
              schema: { type: "string" },
            },
            {
              name: "workflow",
              in: "query",
              description: "外部来源 workflow",
              schema: { type: "string" },
            },
            {
              name: "runId",
              in: "query",
              description: "外部来源 runId",
              schema: { type: "string" },
            },
            {
              name: "groupBy",
              in: "query",
              description: "外部来源分组维度",
              schema: { type: "string", enum: ["provider", "repo", "workflow", "runId"] },
            },
          ],
          responses: {
            "200": {
              description: "质量日报",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/QualityDailyMetricsResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/quality/scorecards": {
        get: {
          summary: "列出质量评分卡",
          operationId: "listQualityScorecards",
          deprecated: true,
          description: "兼容接口，建议迁移到 /api/v2/quality/scorecards。",
          tags: ["quality"],
          parameters: [{ $ref: "#/components/parameters/PageLimit" }],
          responses: {
            "200": {
              description: "评分卡列表",
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    required: ["items"],
                    properties: {
                      items: {
                        type: "array",
                        items: { $ref: "#/components/schemas/QualityScorecard" },
                      },
                    },
                  },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/quality/scorecards/{id}": {
        put: {
          summary: "更新质量评分卡",
          operationId: "updateQualityScorecard",
          deprecated: true,
          description: "兼容接口，建议迁移到 /api/v2/quality/scorecards/{id}。",
          tags: ["quality"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/QualityScorecard" },
              },
            },
          },
          responses: {
            "200": {
              description: "更新后的评分卡",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/QualityScorecard" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/replay/baselines": {
        post: {
          summary: "创建回放基线",
          operationId: "createReplayBaseline",
          deprecated: true,
          description: "兼容接口，建议迁移到 /api/v2/replay/datasets。",
          tags: ["replay"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ReplayBaselineInput" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayBaseline" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        get: {
          summary: "列出回放基线",
          operationId: "listReplayBaselines",
          deprecated: true,
          description: "兼容接口，建议迁移到 /api/v2/replay/datasets。",
          tags: ["replay"],
          parameters: [{ $ref: "#/components/parameters/PageLimit" }],
          responses: {
            "200": {
              description: "回放基线列表",
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    required: ["items"],
                    properties: {
                      items: {
                        type: "array",
                        items: { $ref: "#/components/schemas/ReplayBaseline" },
                      },
                    },
                  },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/replay/jobs": {
        post: {
          summary: "创建回放任务",
          operationId: "createReplayJob",
          deprecated: true,
          description: "兼容接口，建议迁移到 /api/v2/replay/runs。",
          tags: ["replay"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ReplayJobInput" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayJob" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        get: {
          summary: "列出回放任务",
          operationId: "listReplayJobs",
          deprecated: true,
          description: "兼容接口，建议迁移到 /api/v2/replay/runs。",
          tags: ["replay"],
          parameters: [{ $ref: "#/components/parameters/PageLimit" }],
          responses: {
            "200": {
              description: "回放任务列表",
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    required: ["items"],
                    properties: {
                      items: {
                        type: "array",
                        items: { $ref: "#/components/schemas/ReplayJob" },
                      },
                    },
                  },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/replay/jobs/{id}": {
        get: {
          summary: "查询回放任务",
          operationId: "getReplayJob",
          deprecated: true,
          description: "兼容接口，建议迁移到 /api/v2/replay/runs/{id}。",
          tags: ["replay"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "回放任务详情",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayJob" },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v1/replay/jobs/{id}/diff": {
        get: {
          summary: "查询回放差异",
          operationId: "getReplayJobDiff",
          deprecated: true,
          description: "兼容接口，建议迁移到 /api/v2/replay/runs/{id}/diffs。",
          tags: ["replay"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "回放差异详情",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayDiff" },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/quality/evaluations": {
        post: {
          summary: "创建质量评估（v2）",
          operationId: "createQualityEvaluationV2",
          tags: ["quality"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/QualityEvaluationInputV2" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/QualityEvaluationV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/quality/metrics": {
        get: {
          summary: "查询质量指标（v2）",
          operationId: "listQualityMetricsV2",
          tags: ["quality"],
          parameters: [
            { $ref: "#/components/parameters/PageLimit" },
            {
              name: "from",
              in: "query",
              description: "开始时间（ISO 或 yyyy-mm-dd）",
              schema: { type: "string" },
            },
            {
              name: "to",
              in: "query",
              description: "结束时间（ISO 或 yyyy-mm-dd）",
              schema: { type: "string" },
            },
            {
              name: "metric",
              in: "query",
              description: "质量指标",
              schema: {
                type: "string",
                enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
              },
            },
            {
              name: "provider",
              in: "query",
              description: "外部来源 provider",
              schema: { type: "string" },
            },
            {
              name: "repo",
              in: "query",
              description: "外部来源 repo",
              schema: { type: "string" },
            },
            {
              name: "workflow",
              in: "query",
              description: "外部来源 workflow",
              schema: { type: "string" },
            },
            {
              name: "runId",
              in: "query",
              description: "外部来源 runId",
              schema: { type: "string" },
            },
            {
              name: "groupBy",
              in: "query",
              description: "外部来源分组维度",
              schema: { type: "string", enum: ["provider", "repo", "workflow", "runId"] },
            },
          ],
          responses: {
            "200": {
              description: "质量指标聚合结果",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/QualityMetricsResponseV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/quality/reports/cost-correlation": {
        get: {
          summary: "查询质量-成本相关性报告（v2）",
          operationId: "getQualityCostCorrelationV2",
          tags: ["quality"],
          parameters: [
            {
              name: "from",
              in: "query",
              description: "开始时间（ISO 或 yyyy-mm-dd）",
              schema: { type: "string" },
            },
            {
              name: "to",
              in: "query",
              description: "结束时间（ISO 或 yyyy-mm-dd）",
              schema: { type: "string" },
            },
            {
              name: "metric",
              in: "query",
              description: "质量指标",
              schema: {
                type: "string",
                enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
              },
            },
          ],
          responses: {
            "200": {
              description: "相关性报告",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/QualityCostCorrelationResponseV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/quality/reports/project-trends": {
        get: {
          summary: "查询质量-项目趋势报告（v2）",
          operationId: "getQualityProjectTrendsV2",
          tags: ["quality"],
          parameters: [
            {
              name: "from",
              in: "query",
              description: "开始时间（ISO 或 yyyy-mm-dd）",
              schema: { type: "string" },
            },
            {
              name: "to",
              in: "query",
              description: "结束时间（ISO 或 yyyy-mm-dd）",
              schema: { type: "string" },
            },
            {
              name: "metric",
              in: "query",
              description: "质量指标",
              schema: {
                type: "string",
                enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
              },
            },
            {
              name: "provider",
              in: "query",
              description: "外部来源 provider",
              schema: { type: "string" },
            },
            {
              name: "workflow",
              in: "query",
              description: "外部来源 workflow",
              schema: { type: "string" },
            },
            {
              name: "includeUnknown",
              in: "query",
              description: "是否包含 unknown 项目",
              schema: { type: "boolean", default: false },
            },
            {
              name: "limit",
              in: "query",
              description: "返回项目上限",
              schema: { type: "integer", minimum: 1, maximum: 200, default: 20 },
            },
          ],
          responses: {
            "200": {
              description: "项目趋势报告",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/QualityProjectTrendResponseV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/quality/scorecards": {
        get: {
          summary: "列出质量评分卡（v2）",
          operationId: "listQualityScorecardsV2",
          tags: ["quality"],
          parameters: [
            { $ref: "#/components/parameters/PageLimit" },
            {
              name: "metric",
              in: "query",
              description: "质量指标",
              schema: {
                type: "string",
                enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
              },
            },
          ],
          responses: {
            "200": {
              description: "评分卡列表",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/QualityScorecardListResponse" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/quality/scorecards/{id}": {
        put: {
          summary: "更新质量评分卡（v2）",
          operationId: "updateQualityScorecardV2",
          tags: ["quality"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/QualityScorecardInputV2" },
              },
            },
          },
          responses: {
            "200": {
              description: "更新后的评分卡",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/QualityScorecard" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/replay/datasets": {
        post: {
          summary: "创建回放数据集（v2）",
          operationId: "createReplayDatasetV2",
          tags: ["replay"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ReplayDatasetInputV2" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayDatasetV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "409": { $ref: "#/components/responses/ConflictError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        get: {
          summary: "列出回放数据集（v2）",
          operationId: "listReplayDatasetsV2",
          tags: ["replay"],
          parameters: [
            { $ref: "#/components/parameters/PageLimit" },
            {
              name: "keyword",
              in: "query",
              description: "关键字过滤",
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "数据集列表",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayDatasetListResponseV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/replay/datasets/{id}/cases": {
        get: {
          summary: "获取回放数据集样本（v2）",
          operationId: "listReplayDatasetCasesV2",
          tags: ["replay"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
            {
              name: "limit",
              in: "query",
              description: "返回样本上限",
              schema: { type: "integer", minimum: 1, maximum: 5000 },
            },
          ],
          responses: {
            "200": {
              description: "样本列表",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayDatasetCasesResponseV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        post: {
          summary: "替换回放数据集样本（v2）",
          operationId: "replaceReplayDatasetCasesV2",
          tags: ["replay"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ReplayDatasetCasesReplaceInputV2" },
              },
            },
          },
          responses: {
            "200": {
              description: "替换成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayDatasetCasesResponseV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/replay/datasets/{id}/materialize": {
        post: {
          summary: "从历史会话物化回放样本（v2）",
          operationId: "materializeReplayDatasetCasesV2",
          tags: ["replay"],
          description:
            "将历史 session 按显式 sessionIds 或筛选条件物化为 replay dataset cases，用于真实回放执行链。",
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ReplayDatasetMaterializeInputV2" },
              },
            },
          },
          responses: {
            "200": {
              description: "物化完成并返回最新样本快照",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayDatasetMaterializeResponseV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/replay/runs": {
        post: {
          summary: "创建回放运行（v2）",
          operationId: "createReplayRunV2",
          tags: ["replay"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ReplayRunInputV2" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayRunV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        get: {
          summary: "列出回放运行（v2）",
          operationId: "listReplayRunsV2",
          tags: ["replay"],
          parameters: [
            { $ref: "#/components/parameters/PageLimit" },
            {
              name: "status",
              in: "query",
              description: "运行状态过滤",
              schema: { type: "string", enum: ["pending", "running", "completed", "failed", "cancelled"] },
            },
            {
              name: "datasetId",
              in: "query",
              description: "按 datasetId 过滤",
              schema: { type: "string" },
            },
            {
              name: "baselineId",
              in: "query",
              description: "兼容别名：等价于 datasetId",
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "回放运行列表",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayRunListResponseV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/replay/runs/{id}": {
        get: {
          summary: "获取回放运行（v2）",
          operationId: "getReplayRunV2",
          tags: ["replay"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "回放运行详情",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayRunV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/replay/runs/{id}/diffs": {
        get: {
          summary: "获取回放差异（v2）",
          operationId: "getReplayRunDiffsV2",
          tags: ["replay"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
            {
              name: "datasetId",
              in: "query",
              description: "可选：校验回放运行所属 datasetId",
              schema: { type: "string", example: "dataset_demo_001" },
            },
            {
              name: "baselineId",
              in: "query",
              description: "兼容别名：等价于 datasetId",
              schema: { type: "string", example: "dataset_demo_001" },
            },
            {
              name: "keyword",
              in: "query",
              description: "按 caseId/detail/metric/verdict 过滤",
              schema: { type: "string", example: "regressed" },
            },
            {
              name: "limit",
              in: "query",
              description: "返回 diff 上限",
              schema: { type: "integer", minimum: 1, maximum: 500, example: 50 },
            },
          ],
          responses: {
            "200": {
              description: "回放差异详情",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayRunDiffsResponseV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/replay/runs/{id}/artifacts": {
        get: {
          summary: "获取回放工件（v2）",
          operationId: "getReplayRunArtifactsV2",
          tags: ["replay"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "回放工件",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplayRunArtifactsResponseV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/replay/runs/{id}/artifacts/{artifactType}/download": {
        get: {
          summary: "下载回放工件（v2）",
          operationId: "downloadReplayRunArtifactV2",
          tags: ["replay"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
            {
              name: "artifactType",
              in: "path",
              required: true,
              schema: { type: "string", enum: ["summary", "diff", "cases"] },
            },
          ],
          responses: {
            "200": {
              description: "回放工件下载流",
              content: {
                "application/octet-stream": {
                  schema: { type: "string", format: "binary" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/residency/policies/current": {
        get: {
          summary: "获取当前租户数据主权策略（v2）",
          operationId: "getResidencyPolicyV2",
          tags: ["residency"],
          responses: {
            "200": {
              description: "当前策略",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/TenantResidencyPolicyV2" },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        put: {
          summary: "更新当前租户数据主权策略（v2）",
          operationId: "updateResidencyPolicyV2",
          tags: ["residency"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/TenantResidencyPolicyInputV2" },
              },
            },
          },
          responses: {
            "200": {
              description: "更新后的策略",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/TenantResidencyPolicyV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/residency/region-mappings": {
        get: {
          summary: "列出数据主权区域映射（v2）",
          operationId: "listResidencyRegionMappingsV2",
          tags: ["residency"],
          responses: {
            "200": {
              description: "区域映射列表",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ResidencyRegionMappingListResponseV2" },
                },
              },
            },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/residency/replications": {
        get: {
          summary: "列出跨区复制任务（v2）",
          operationId: "listResidencyReplicationsV2",
          tags: ["residency"],
          parameters: [
            { $ref: "#/components/parameters/PageLimit" },
            {
              name: "status",
              in: "query",
              description: "任务状态过滤",
              schema: { type: "string", enum: ["pending", "running", "succeeded", "failed", "cancelled"] },
            },
            {
              name: "sourceRegion",
              in: "query",
              description: "源区域过滤",
              schema: { type: "string" },
            },
            {
              name: "targetRegion",
              in: "query",
              description: "目标区域过滤",
              schema: { type: "string" },
            },
          ],
          responses: {
            "200": {
              description: "复制任务列表",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplicationJobListResponseV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
        post: {
          summary: "创建跨区复制任务（v2）",
          operationId: "createResidencyReplicationV2",
          tags: ["residency"],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ReplicationJobCreateInputV2" },
              },
            },
          },
          responses: {
            "201": {
              description: "创建成功",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplicationJobV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/residency/replications/{id}/approvals": {
        post: {
          summary: "审批跨区复制任务（v2）",
          operationId: "approveResidencyReplicationV2",
          tags: ["residency"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: false,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ReplicationJobReasonInputV2" },
              },
            },
          },
          responses: {
            "200": {
              description: "审批后的任务",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplicationJobV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "409": { $ref: "#/components/responses/ConflictError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
      "/api/v2/residency/replications/{id}/cancel": {
        post: {
          summary: "取消跨区复制任务（v2）",
          operationId: "cancelResidencyReplicationV2",
          tags: ["residency"],
          parameters: [
            {
              name: "id",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          requestBody: {
            required: false,
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ReplicationJobReasonInputV2" },
              },
            },
          },
          responses: {
            "200": {
              description: "取消后的任务",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ReplicationJobV2" },
                },
              },
            },
            "400": { $ref: "#/components/responses/BadRequestError" },
            "401": { $ref: "#/components/responses/UnauthorizedError" },
            "403": { $ref: "#/components/responses/ForbiddenError" },
            "404": { $ref: "#/components/responses/NotFoundError" },
            "409": { $ref: "#/components/responses/ConflictError" },
            "500": { $ref: "#/components/responses/InternalServerError" },
          },
        },
      },
    },
    components: {
      securitySchemes: {
        bearerAuth: {
          type: "http",
          scheme: "bearer",
          bearerFormat: "JWT",
          description: "使用 Authorization: Bearer <access_token> 访问。",
        },
      },
      parameters: {
        PageLimit: {
          name: "limit",
          in: "query",
          description: "分页大小，默认 100，最大 500。",
          schema: {
            type: "integer",
            minimum: 1,
            maximum: 500,
            default: 100,
          },
        },
        PageCursor: {
          name: "cursor",
          in: "query",
          description: "分页游标（预留参数，当前可忽略）。",
          schema: {
            type: "string",
          },
        },
      },
      responses: {
        BadRequestError: {
          description: "请求参数不合法。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
        UnauthorizedError: {
          description: "未认证或 token 无效。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
        ForbiddenError: {
          description: "无权访问当前租户资源。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
        NotFoundError: {
          description: "资源不存在。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
        ConflictError: {
          description: "请求与当前资源状态冲突。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
        InternalServerError: {
          description: "服务端错误。",
          content: {
            "application/json": {
              schema: standardErrorSchema,
            },
          },
        },
      },
      schemas: {
        OpenApiDocument: {
          type: "object",
          required: ["openapi", "info", "paths", "components"],
          properties: {
            openapi: { type: "string" },
            info: { type: "object", additionalProperties: true },
            paths: { type: "object", additionalProperties: true },
            components: { type: "object", additionalProperties: true },
          },
        },
        ErrorResponse: {
          type: "object",
          required: ["message"],
          properties: {
            message: { type: "string" },
            requestId: { type: "string" },
            code: { type: "string" },
            details: {
              type: "object",
              additionalProperties: true,
            },
          },
        },
        PaginationMeta: {
          type: "object",
          required: ["total"],
          properties: {
            total: { type: "integer", minimum: 0 },
            nextCursor: { type: "string", nullable: true },
            limit: { type: "integer", minimum: 1 },
          },
        },
        ApiKey: {
          type: "object",
          required: ["id", "tenantId", "name", "scope", "status", "keyPrefix", "createdAt", "updatedAt"],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            name: { type: "string" },
            scope: { type: "string", enum: ["read", "write", "admin"] },
            status: { type: "string", enum: ["active", "revoked", "expired"] },
            keyPrefix: { type: "string" },
            createdByUserId: { type: "string", nullable: true },
            lastUsedAt: { type: "string", format: "date-time", nullable: true },
            expiresAt: { type: "string", format: "date-time", nullable: true },
            revokedAt: { type: "string", format: "date-time", nullable: true },
            metadata: { type: "object", additionalProperties: true },
            createdAt: { type: "string", format: "date-time" },
            updatedAt: { type: "string", format: "date-time" },
          },
        },
        ApiKeyCreateRequest: {
          type: "object",
          required: ["name"],
          properties: {
            name: { type: "string", minLength: 1, maxLength: 100 },
            scope: { type: "string", enum: ["read", "write", "admin"] },
          },
        },
        ApiKeyCreateResponse: {
          type: "object",
          required: ["id", "tenantId", "keyId", "keyPrefix", "secret", "createdAt"],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            keyId: { type: "string" },
            keyPrefix: { type: "string" },
            secret: { type: "string" },
            createdAt: { type: "string", format: "date-time" },
          },
        },
        ApiKeyRevokeRequest: {
          type: "object",
          properties: {
            reason: { type: "string", maxLength: 500 },
          },
        },
        ApiKeyListResponse: {
          type: "object",
          required: ["items", "total"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/ApiKey" },
            },
            total: { type: "integer", minimum: 0 },
            filters: {
              type: "object",
              additionalProperties: true,
            },
            pagination: { $ref: "#/components/schemas/PaginationMeta" },
          },
        },
        WebhookEndpoint: {
          type: "object",
          required: ["id", "tenantId", "name", "url", "events", "status", "createdAt", "updatedAt"],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            name: { type: "string" },
            url: { type: "string", format: "uri" },
            events: {
              type: "array",
              description:
                "Webhook 订阅事件类型。保留 replay.job.* 兼容事件，同时推荐新接入优先消费 replay.run.* 运行级事件。",
              minItems: 1,
              items: {
                type: "string",
                enum: Array.from(WEBHOOK_EVENT_TYPE_SET),
              },
              example: ["replay.run.completed", "replay.run.regression_detected"],
            },
            status: { type: "string", enum: ["active", "paused", "disabled"] },
            secretHint: { type: "string", nullable: true },
            failureCount: { type: "integer", minimum: 0 },
            lastSuccessAt: { type: "string", format: "date-time", nullable: true },
            lastFailureAt: { type: "string", format: "date-time", nullable: true },
            createdAt: { type: "string", format: "date-time" },
            updatedAt: { type: "string", format: "date-time" },
          },
        },
        WebhookCreateRequest: {
          type: "object",
          required: ["name", "url", "events"],
          properties: {
            name: { type: "string", minLength: 1, maxLength: 120 },
            url: { type: "string", format: "uri" },
            events: {
              type: "array",
              description:
                "订阅事件列表。replay.job.* 仅作兼容保留，建议优先使用 replay.run.started/completed/regression_detected/failed/cancelled。",
              minItems: 1,
              items: {
                type: "string",
                enum: Array.from(WEBHOOK_EVENT_TYPE_SET),
              },
              example: ["replay.run.started", "replay.run.completed"],
            },
            status: { type: "string", enum: ["active", "paused", "disabled"] },
            secret: { type: "string" },
          },
        },
        WebhookUpdateRequest: {
          type: "object",
          minProperties: 1,
          properties: {
            name: { type: "string", minLength: 1, maxLength: 120 },
            url: { type: "string", format: "uri" },
            events: {
              type: "array",
              description:
                "订阅事件列表。replay.job.* 仅作兼容保留，建议优先使用 replay.run.* 运行级事件。",
              minItems: 1,
              items: {
                type: "string",
                enum: Array.from(WEBHOOK_EVENT_TYPE_SET),
              },
              example: ["replay.run.completed", "replay.run.failed"],
            },
            status: { type: "string", enum: ["active", "paused", "disabled"] },
            secret: { type: "string" },
          },
        },
        WebhookCreateResponse: {
          allOf: [
            { $ref: "#/components/schemas/WebhookEndpoint" },
            {
              type: "object",
              properties: {
                secret: { type: "string", nullable: true },
              },
            },
          ],
        },
        WebhookListResponse: {
          type: "object",
          required: ["items", "total"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/WebhookEndpoint" },
            },
            total: { type: "integer", minimum: 0 },
            pagination: { $ref: "#/components/schemas/PaginationMeta" },
          },
        },
        WebhookReplayFilter: {
          type: "object",
          required: ["limit"],
          properties: {
            eventType: {
              type: "string",
              description:
                "可选：指定单一事件类型。为空时按 endpoint 已订阅事件回放；推荐优先使用 replay.run.*，replay.job.* 仅作兼容。",
              enum: Array.from(WEBHOOK_EVENT_TYPE_SET),
              nullable: true,
              example: "replay.run.regression_detected",
            },
            from: { type: "string", format: "date-time", nullable: true },
            to: { type: "string", format: "date-time", nullable: true },
            limit: { type: "integer", minimum: 1, maximum: 500 },
          },
        },
        WebhookReplayRequest: {
          type: "object",
          properties: {
            eventType: {
              type: "string",
              description:
                "可选：限制本次回放的事件类型。推荐使用 replay.run.*；保留 replay.job.* 兼容事件。",
              enum: Array.from(WEBHOOK_EVENT_TYPE_SET),
              example: "replay.run.regression_detected",
            },
            from: { type: "string", format: "date-time" },
            to: { type: "string", format: "date-time" },
            limit: { type: "integer", minimum: 1, maximum: 500 },
            dryRun: { type: "boolean" },
          },
        },
        WebhookReplayTask: {
          type: "object",
          required: [
            "id",
            "tenantId",
            "webhookId",
            "status",
            "dryRun",
            "filters",
            "requestedAt",
            "result",
          ],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            webhookId: { type: "string" },
            status: { type: "string", enum: ["queued", "running", "completed", "failed"] },
            dryRun: { type: "boolean" },
            filters: { $ref: "#/components/schemas/WebhookReplayFilter" },
            requestedAt: { type: "string", format: "date-time" },
            startedAt: { type: "string", format: "date-time", nullable: true },
            finishedAt: { type: "string", format: "date-time", nullable: true },
            error: { type: "string", nullable: true },
            result: { type: "object", additionalProperties: true },
          },
        },
        WebhookReplayTaskListResponse: {
          type: "object",
          required: ["items", "total", "nextCursor", "filters"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/WebhookReplayTask" },
            },
            total: { type: "integer", minimum: 0 },
            nextCursor: { type: "string", nullable: true },
            filters: {
              type: "object",
              properties: {
                webhookId: { type: "string", nullable: true },
                status: {
                  type: "string",
                  nullable: true,
                  enum: ["queued", "running", "completed", "failed"],
                },
                limit: { type: "integer", minimum: 1, maximum: 500 },
                cursor: { type: "string", nullable: true },
              },
            },
          },
        },
        WebhookReplayResponse: {
          $ref: "#/components/schemas/WebhookReplayTask",
        },
        QualityExternalSource: {
          type: "object",
          required: ["provider"],
          properties: {
            provider: { type: "string" },
            repo: { type: "string" },
            workflow: { type: "string" },
            runId: { type: "string" },
          },
        },
        QualityEventInput: {
          type: "object",
          required: ["metric", "score", "sampleCount", "occurredAt"],
          properties: {
            sessionId: { type: "string" },
            replayJobId: { type: "string" },
            externalSource: { $ref: "#/components/schemas/QualityExternalSource" },
            metric: {
              type: "string",
              enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
            },
            score: { type: "number", minimum: 0, maximum: 100 },
            sampleCount: { type: "integer", minimum: 1 },
            occurredAt: { type: "string", format: "date-time" },
            notes: { type: "string" },
            metadata: { type: "object", additionalProperties: true },
          },
        },
        QualityEvent: {
          type: "object",
          required: [
            "id",
            "tenantId",
            "metric",
            "score",
            "sampleCount",
            "occurredAt",
            "metadata",
            "createdAt",
          ],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            sessionId: { type: "string", nullable: true },
            replayJobId: { type: "string", nullable: true },
            externalSource: { $ref: "#/components/schemas/QualityExternalSource" },
            metric: {
              type: "string",
              enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
            },
            score: { type: "number" },
            sampleCount: { type: "integer", minimum: 1 },
            occurredAt: { type: "string", format: "date-time" },
            notes: { type: "string", nullable: true },
            metadata: { type: "object", additionalProperties: true },
            createdAt: { type: "string", format: "date-time" },
          },
        },
        QualityDailyMetric: {
          type: "object",
          required: ["date", "metric", "avgScore", "p50Score", "p90Score", "totalEvents"],
          properties: {
            date: { type: "string", format: "date" },
            metric: {
              type: "string",
              enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
            },
            avgScore: { type: "number" },
            p50Score: { type: "number" },
            p90Score: { type: "number" },
            totalEvents: { type: "integer", minimum: 0 },
          },
        },
        QualityExternalMetricGroup: {
          type: "object",
          required: [
            "groupBy",
            "value",
            "totalEvents",
            "passedEvents",
            "failedEvents",
            "passRate",
            "avgScore",
          ],
          properties: {
            groupBy: {
              type: "string",
              enum: ["provider", "repo", "workflow", "runId"],
            },
            value: { type: "string" },
            totalEvents: { type: "integer", minimum: 0 },
            passedEvents: { type: "integer", minimum: 0 },
            failedEvents: { type: "integer", minimum: 0 },
            passRate: { type: "number", minimum: 0, maximum: 1 },
            avgScore: { type: "number" },
          },
        },
        QualityDailyMetricsResponse: {
          type: "object",
          required: ["items", "total"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/QualityDailyMetric" },
            },
            total: { type: "integer", minimum: 0 },
            groups: {
              type: "array",
              items: { $ref: "#/components/schemas/QualityExternalMetricGroup" },
            },
          },
        },
        QualityScorecard: {
          type: "object",
          required: [
            "id",
            "metric",
            "targetScore",
            "warningScore",
            "criticalScore",
            "weight",
            "enabled",
            "updatedAt",
          ],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            metric: {
              type: "string",
              enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
            },
            targetScore: { type: "number" },
            warningScore: { type: "number" },
            criticalScore: { type: "number" },
            weight: { type: "number" },
            enabled: { type: "boolean" },
            updatedByUserId: { type: "string", nullable: true },
            updatedAt: { type: "string", format: "date-time" },
          },
        },
        ReplayBaselineInput: {
          type: "object",
          required: ["name"],
          properties: {
            name: { type: "string", minLength: 1, maxLength: 120 },
            description: { type: "string" },
            metadata: { type: "object", additionalProperties: true },
          },
        },
        ReplayBaseline: {
          type: "object",
          required: ["id", "name", "createdAt"],
          properties: {
            id: { type: "string" },
            name: { type: "string" },
            description: { type: "string" },
            metadata: { type: "object", additionalProperties: true },
            createdAt: { type: "string", format: "date-time" },
          },
        },
        ReplayJobInput: {
          type: "object",
          required: ["baselineId"],
          properties: {
            baselineId: { type: "string" },
            sourceSessionId: { type: "string" },
            targetSessionId: { type: "string" },
            options: { type: "object", additionalProperties: true },
          },
        },
        ReplayJob: {
          type: "object",
          required: ["id", "baselineId", "status", "createdAt"],
          properties: {
            id: { type: "string" },
            baselineId: { type: "string" },
            status: {
              type: "string",
              enum: ["pending", "running", "completed", "failed", "cancelled"],
            },
            score: { type: "number", nullable: true },
            createdAt: { type: "string", format: "date-time" },
            updatedAt: { type: "string", format: "date-time", nullable: true },
          },
        },
        ReplayDiff: {
          type: "object",
          required: ["jobId"],
          properties: {
            jobId: { type: "string" },
            summary: { type: "string" },
            regressions: { type: "integer", minimum: 0 },
            improvements: { type: "integer", minimum: 0 },
            details: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: true,
              },
            },
          },
        },
        QualityEvaluationInputV2: {
          type: "object",
          required: ["metric", "score", "sampleCount", "occurredAt"],
          properties: {
            sessionId: { type: "string" },
            replayJobId: { type: "string" },
            replayRunId: { type: "string" },
            externalSource: { $ref: "#/components/schemas/QualityExternalSource" },
            metric: {
              type: "string",
              enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
            },
            score: { type: "number", minimum: 0, maximum: 100 },
            sampleCount: { type: "integer", minimum: 1 },
            occurredAt: { type: "string", format: "date-time" },
            notes: { type: "string" },
            metadata: { type: "object", additionalProperties: true },
          },
        },
        QualityEvaluationV2: {
          type: "object",
          required: [
            "id",
            "tenantId",
            "metric",
            "score",
            "sampleCount",
            "occurredAt",
            "metadata",
            "createdAt",
          ],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            sessionId: { type: "string", nullable: true },
            replayRunId: { type: "string", nullable: true },
            externalSource: { $ref: "#/components/schemas/QualityExternalSource" },
            metric: {
              type: "string",
              enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
            },
            score: { type: "number" },
            sampleCount: { type: "integer", minimum: 1 },
            occurredAt: { type: "string", format: "date-time" },
            notes: { type: "string", nullable: true },
            metadata: { type: "object", additionalProperties: true },
            createdAt: { type: "string", format: "date-time" },
          },
        },
        QualityMetricsItemV2: {
          type: "object",
          required: [
            "date",
            "metric",
            "totalEvents",
            "passedEvents",
            "failedEvents",
            "avgScore",
            "passRate",
          ],
          properties: {
            date: { type: "string", format: "date" },
            metric: {
              type: "string",
              enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
            },
            totalEvents: { type: "integer", minimum: 0 },
            passedEvents: { type: "integer", minimum: 0 },
            failedEvents: { type: "integer", minimum: 0 },
            avgScore: { type: "number" },
            passRate: { type: "number", minimum: 0, maximum: 1 },
          },
        },
        QualityMetricsSummaryV2: {
          type: "object",
          required: ["totalEvents", "passedEvents", "failedEvents", "passRate", "avgScore"],
          properties: {
            totalEvents: { type: "integer", minimum: 0 },
            passedEvents: { type: "integer", minimum: 0 },
            failedEvents: { type: "integer", minimum: 0 },
            passRate: { type: "number", minimum: 0, maximum: 1 },
            avgScore: { type: "number" },
          },
        },
        QualityMetricsResponseV2: {
          type: "object",
          required: ["items", "total", "summary"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/QualityMetricsItemV2" },
            },
            total: { type: "integer", minimum: 0 },
            summary: { $ref: "#/components/schemas/QualityMetricsSummaryV2" },
            groups: {
              type: "array",
              items: { $ref: "#/components/schemas/QualityExternalMetricGroup" },
            },
          },
        },
        QualityCostCorrelationItemV2: {
          type: "object",
          required: [
            "date",
            "metric",
            "avgScore",
            "totalEvents",
            "cost",
            "tokens",
            "sessions",
            "costPerQualityPoint",
          ],
          properties: {
            date: { type: "string", format: "date" },
            metric: { type: "string" },
            avgScore: { type: "number" },
            totalEvents: { type: "integer", minimum: 0 },
            cost: { type: "number", minimum: 0 },
            tokens: { type: "integer", minimum: 0 },
            sessions: { type: "integer", minimum: 0 },
            costPerQualityPoint: { type: "number", minimum: 0 },
          },
        },
        QualityCostCorrelationSummaryV2: {
          type: "object",
          required: ["metric", "correlationCoefficient", "pairs"],
          properties: {
            metric: { type: "string" },
            correlationCoefficient: { type: "number", nullable: true },
            pairs: { type: "integer", minimum: 0 },
            from: { type: "string", format: "date-time", nullable: true },
            to: { type: "string", format: "date-time", nullable: true },
          },
        },
        QualityCostCorrelationResponseV2: {
          type: "object",
          required: ["items", "total", "summary"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/QualityCostCorrelationItemV2" },
            },
            total: { type: "integer", minimum: 0 },
            summary: { $ref: "#/components/schemas/QualityCostCorrelationSummaryV2" },
          },
        },
        QualityProjectTrendItemV2: {
          type: "object",
          required: [
            "project",
            "metric",
            "totalEvents",
            "passedEvents",
            "failedEvents",
            "passRate",
            "avgScore",
            "totalCost",
            "totalTokens",
            "totalSessions",
            "costPerQualityPoint",
          ],
          properties: {
            project: { type: "string" },
            metric: { type: "string" },
            totalEvents: { type: "integer", minimum: 0 },
            passedEvents: { type: "integer", minimum: 0 },
            failedEvents: { type: "integer", minimum: 0 },
            passRate: { type: "number", minimum: 0, maximum: 1 },
            avgScore: { type: "number" },
            totalCost: { type: "number", minimum: 0 },
            totalTokens: { type: "integer", minimum: 0 },
            totalSessions: { type: "integer", minimum: 0 },
            costPerQualityPoint: { type: "number", minimum: 0 },
          },
        },
        QualityProjectTrendSummaryV2: {
          type: "object",
          required: [
            "metric",
            "totalEvents",
            "passedEvents",
            "failedEvents",
            "passRate",
            "avgScore",
            "totalCost",
            "totalTokens",
            "totalSessions",
          ],
          properties: {
            metric: { type: "string" },
            totalEvents: { type: "integer", minimum: 0 },
            passedEvents: { type: "integer", minimum: 0 },
            failedEvents: { type: "integer", minimum: 0 },
            passRate: { type: "number", minimum: 0, maximum: 1 },
            avgScore: { type: "number" },
            totalCost: { type: "number", minimum: 0 },
            totalTokens: { type: "integer", minimum: 0 },
            totalSessions: { type: "integer", minimum: 0 },
            from: { type: "string", format: "date-time", nullable: true },
            to: { type: "string", format: "date-time", nullable: true },
          },
        },
        QualityProjectTrendFiltersV2: {
          type: "object",
          required: ["metric", "includeUnknown", "provider", "workflow", "limit"],
          properties: {
            from: { type: "string", format: "date-time", nullable: true },
            to: { type: "string", format: "date-time", nullable: true },
            metric: { type: "string" },
            includeUnknown: { type: "boolean" },
            provider: { type: "string", nullable: true },
            workflow: { type: "string", nullable: true },
            limit: { type: "integer", minimum: 1, maximum: 200 },
          },
        },
        QualityProjectTrendResponseV2: {
          type: "object",
          required: ["items", "total", "summary", "filters"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/QualityProjectTrendItemV2" },
            },
            total: { type: "integer", minimum: 0 },
            summary: { $ref: "#/components/schemas/QualityProjectTrendSummaryV2" },
            filters: { $ref: "#/components/schemas/QualityProjectTrendFiltersV2" },
          },
        },
        QualityScorecardInputV2: {
          type: "object",
          required: ["targetScore", "warningScore", "criticalScore", "enabled", "updatedAt"],
          properties: {
            targetScore: { type: "number", minimum: 0 },
            warningScore: { type: "number", minimum: 0 },
            criticalScore: { type: "number", minimum: 0 },
            weight: { type: "number", minimum: 0 },
            enabled: { type: "boolean" },
            updatedAt: { type: "string", format: "date-time" },
          },
        },
        QualityScorecardListResponse: {
          type: "object",
          required: ["items", "total"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/QualityScorecard" },
            },
            total: { type: "integer", minimum: 0 },
          },
        },
        ReplayDatasetInputV2: {
          type: "object",
          required: ["name", "datasetRef", "model"],
          properties: {
            name: { type: "string", minLength: 1 },
            description: { type: "string" },
            datasetRef: {
              type: "string",
              minLength: 1,
              description: "公开语义：数据集引用。用于关联外部数据集或业务侧基线。",
            },
            datasetId: {
              type: "string",
              minLength: 1,
              description: "兼容别名：等价于 datasetRef。",
            },
            model: { type: "string", minLength: 1 },
            promptVersion: { type: "string" },
            sampleCount: { type: "integer", minimum: 0 },
            metadata: { type: "object", additionalProperties: true },
          },
        },
        ReplayDatasetV2: {
          type: "object",
          required: [
            "id",
            "tenantId",
            "name",
            "caseCount",
            "model",
            "sampleCount",
            "metadata",
            "createdAt",
            "updatedAt",
          ],
          properties: {
            id: { type: "string", description: "数据集资源 ID。" },
            tenantId: { type: "string" },
            name: { type: "string" },
            datasetId: {
              type: "string",
              description: "兼容别名：等价于 id。",
            },
            datasetRef: {
              type: "string",
              nullable: true,
              description: "公开语义：外部数据集引用。",
            },
            model: { type: "string" },
            promptVersion: { type: "string", nullable: true },
            caseCount: { type: "integer", minimum: 0 },
            sampleCount: { type: "integer", minimum: 0 },
            metadata: { type: "object", additionalProperties: true },
            createdAt: { type: "string", format: "date-time" },
            updatedAt: { type: "string", format: "date-time" },
          },
        },
        ReplayDatasetListResponseV2: {
          type: "object",
          required: ["items", "total"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/ReplayDatasetV2" },
            },
            total: { type: "integer", minimum: 0 },
          },
        },
        ReplayDatasetCaseV2: {
          type: "object",
          required: [
            "id",
            "tenantId",
            "datasetId",
            "caseId",
            "sortOrder",
            "input",
            "metadata",
            "createdAt",
            "updatedAt",
          ],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            datasetId: { type: "string" },
            caseId: { type: "string" },
            sortOrder: { type: "integer", minimum: 0 },
            input: { type: "string", minLength: 1 },
            expectedOutput: { type: "string", nullable: true },
            baselineOutput: { type: "string", nullable: true },
            candidateInput: { type: "string", nullable: true },
            metadata: { type: "object", additionalProperties: true },
            checksum: { type: "string", nullable: true },
            createdAt: { type: "string", format: "date-time", nullable: true },
            updatedAt: { type: "string", format: "date-time", nullable: true },
          },
        },
        ReplayDatasetCaseWriteInputV2: {
          type: "object",
          required: ["input"],
          properties: {
            caseId: { type: "string" },
            sortOrder: { type: "integer", minimum: 0 },
            input: { type: "string", minLength: 1 },
            expectedOutput: { type: "string", nullable: true },
            baselineOutput: { type: "string", nullable: true },
            candidateInput: { type: "string", nullable: true },
            metadata: { type: "object", additionalProperties: true },
          },
        },
        ReplayDatasetCasesReplaceInputV2: {
          type: "object",
          required: ["items"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/ReplayDatasetCaseWriteInputV2" },
            },
          },
        },
        ReplayDatasetCasesResponseV2: {
          type: "object",
          required: ["datasetId", "items", "total"],
          properties: {
            datasetId: { type: "string" },
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/ReplayDatasetCaseV2" },
            },
            total: { type: "integer", minimum: 0 },
          },
        },
        ReplaySourceSummaryV2: {
          type: "object",
          description: "回放样本来源统计，key 为来源类型，value 为样本数量。",
          additionalProperties: {
            type: "integer",
            minimum: 0,
          },
        },
        ReplayDatasetMaterializeFiltersV2: {
          type: "object",
          properties: {
            sourceId: { type: "string" },
            keyword: { type: "string" },
            clientType: { type: "string" },
            tool: { type: "string" },
            host: { type: "string" },
            model: { type: "string" },
            project: { type: "string" },
            from: { type: "string", format: "date-time" },
            to: { type: "string", format: "date-time" },
          },
        },
        ReplayDatasetMaterializeInputV2: {
          type: "object",
          description: "至少提供 sessionIds 或 filters 之一，用于限定要物化的历史会话范围。",
          anyOf: [{ required: ["sessionIds"] }, { required: ["filters"] }],
          properties: {
            sessionIds: {
              type: "array",
              items: { type: "string" },
              minItems: 1,
            },
            filters: {
              $ref: "#/components/schemas/ReplayDatasetMaterializeFiltersV2",
            },
            sampleLimit: { type: "integer", minimum: 1, maximum: 5000 },
            sanitized: {
              type: "boolean",
              description: "是否在物化时使用脱敏后的文本快照，默认 true。",
            },
            snapshotVersion: {
              type: "string",
              description: "可选快照版本标签，用于区分不同物化批次。",
            },
          },
        },
        ReplayDatasetMaterializeResponseFiltersV2: {
          type: "object",
          required: ["datasetId"],
          properties: {
            datasetId: { type: "string" },
            sessionIds: {
              type: "array",
              items: { type: "string" },
            },
            filters: {
              $ref: "#/components/schemas/ReplayDatasetMaterializeFiltersV2",
            },
            sampleLimit: { type: "integer", minimum: 1, maximum: 5000 },
            sanitized: { type: "boolean" },
            snapshotVersion: { type: "string" },
          },
        },
        ReplayDatasetMaterializeResponseV2: {
          type: "object",
          required: [
            "datasetId",
            "sourceType",
            "materialized",
            "skipped",
            "items",
            "total",
            "filters",
          ],
          properties: {
            datasetId: { type: "string" },
            sourceType: { type: "string", enum: ["session"] },
            materialized: { type: "integer", minimum: 0 },
            skipped: { type: "integer", minimum: 0 },
            sourceSummary: { $ref: "#/components/schemas/ReplaySourceSummaryV2" },
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/ReplayDatasetCaseV2" },
            },
            total: { type: "integer", minimum: 0 },
            filters: {
              $ref: "#/components/schemas/ReplayDatasetMaterializeResponseFiltersV2",
            },
          },
        },
        ReplayRunInputV2: {
          type: "object",
          required: ["datasetId", "candidateLabel"],
          properties: {
            datasetId: {
              type: "string",
              description: "公开语义：运行所使用的数据集资源 ID。",
            },
            baselineId: {
              type: "string",
              description: "兼容别名：等价于 datasetId。",
            },
            candidateLabel: { type: "string", minLength: 1 },
            from: { type: "string", format: "date-time" },
            to: { type: "string", format: "date-time" },
            sampleLimit: { type: "integer", minimum: 1 },
            metadata: { type: "object", additionalProperties: true },
          },
        },
        ReplayDiffItemV2: {
          type: "object",
          required: ["caseId", "metric", "baselineScore", "candidateScore", "delta", "verdict"],
          properties: {
            caseId: { type: "string" },
            metric: {
              type: "string",
              enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
            },
            baselineScore: { type: "number" },
            candidateScore: { type: "number" },
            delta: {
              type: "number",
              description: "candidateScore - baselineScore。正值通常表示 improved，负值通常表示 regressed。",
              example: -0.12,
            },
            verdict: {
              type: "string",
              enum: ["improved", "regressed", "unchanged"],
              description: "差异结论，和 delta 的正负及阈值判断保持一致。",
              example: "regressed",
            },
            detail: {
              type: "string",
              nullable: true,
              description: "差异明细，可用于解释回归原因或补充对比上下文。",
              example: "Groundedness dropped after prompt truncation.",
            },
          },
        },
        ReplayRunTopRegressionItemV2: {
          type: "object",
          required: ["caseId", "metric", "delta", "baselineScore", "candidateScore"],
          properties: {
            caseId: { type: "string" },
            metric: {
              type: "string",
              enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
            },
            delta: { type: "number" },
            baselineScore: { type: "number" },
            candidateScore: { type: "number" },
            detail: { type: "string", nullable: true },
          },
        },
        ReplayRunSummaryDigestV2: {
          type: "object",
          description: "回放完成后生成的结果摘要，用于审计、告警和自动消费。",
          additionalProperties: true,
          properties: {
            tenantId: { type: "string" },
            datasetId: { type: "string" },
            baselineId: {
              type: "string",
              description: "兼容别名：等价于 datasetId。",
            },
            runId: {
              type: "string",
              description: "回放运行主标识，和 ReplayRunV2.id 恒等。",
              example: "run_demo_001",
            },
            jobId: {
              type: "string",
              description: "兼容别名：等价于 runId；新客户端不建议继续依赖 jobId 作为主语义字段。",
              example: "run_demo_001",
            },
            candidateLabel: { type: "string" },
            executionSource: {
              type: "string",
              enum: ["synthetic", "dataset_cases", "session_materialized"],
            },
            materializedCaseCount: { type: "integer", minimum: 0 },
            totalCases: { type: "integer", minimum: 0 },
            processedCases: { type: "integer", minimum: 0 },
            improvedCases: { type: "integer", minimum: 0 },
            regressedCases: { type: "integer", minimum: 0 },
            unchangedCases: { type: "integer", minimum: 0 },
            sourceSummary: { $ref: "#/components/schemas/ReplaySourceSummaryV2" },
            topRegressions: {
              type: "array",
              items: { $ref: "#/components/schemas/ReplayRunTopRegressionItemV2" },
            },
            finishedAt: { type: "string", format: "date-time" },
            artifactUrls: {
              type: "object",
              required: ["summary", "diff", "cases"],
              properties: {
                summary: { type: "string", format: "uri-reference", description: "summary 工件下载地址。" },
                diff: { type: "string", format: "uri-reference", description: "diff 工件下载地址。" },
                cases: { type: "string", format: "uri-reference", description: "cases 工件下载地址。" },
              },
            },
          },
        },
        ReplayRunSummaryV2: {
          type: "object",
          description: "回放运行汇总，pending/running 状态下仅包含基础计数；completed 时会补充 executionSource、sourceSummary 与 digest。",
          additionalProperties: true,
          properties: {
            metric: {
              type: "string",
              enum: ["accuracy", "consistency", "groundedness", "safety", "latency"],
            },
            totalCases: { type: "integer", minimum: 0 },
            processedCases: { type: "integer", minimum: 0 },
            improvedCases: { type: "integer", minimum: 0 },
            regressedCases: { type: "integer", minimum: 0 },
            unchangedCases: { type: "integer", minimum: 0 },
            executionSource: {
              type: "string",
              enum: ["synthetic", "dataset_cases", "session_materialized"],
            },
            materializedCaseCount: { type: "integer", minimum: 0 },
            sourceSummary: { $ref: "#/components/schemas/ReplaySourceSummaryV2" },
            digest: { $ref: "#/components/schemas/ReplayRunSummaryDigestV2" },
          },
        },
        ReplayRunV2: {
          type: "object",
          required: [
            "id",
            "tenantId",
            "datasetId",
            "candidateLabel",
            "status",
            "totalCases",
            "processedCases",
            "improvedCases",
            "regressedCases",
            "unchangedCases",
            "summary",
            "diffs",
            "createdAt",
            "updatedAt",
          ],
          properties: {
            id: {
              type: "string",
              description: "运行资源 ID，公开语义上等价于 runId。",
              example: "run_demo_001",
            },
            tenantId: { type: "string" },
            datasetId: {
              type: "string",
              description: "公开语义：运行所属数据集资源 ID。",
            },
            baselineId: {
              type: "string",
              description: "兼容别名：等价于 datasetId。",
            },
            candidateLabel: { type: "string" },
            status: {
              type: "string",
              enum: ["pending", "running", "completed", "failed", "cancelled"],
            },
            totalCases: { type: "integer", minimum: 0 },
            processedCases: { type: "integer", minimum: 0 },
            improvedCases: { type: "integer", minimum: 0 },
            regressedCases: { type: "integer", minimum: 0 },
            unchangedCases: { type: "integer", minimum: 0 },
            summary: { $ref: "#/components/schemas/ReplayRunSummaryV2" },
            diffs: {
              type: "array",
              items: { $ref: "#/components/schemas/ReplayDiffItemV2" },
            },
            error: { type: "string", nullable: true },
            startedAt: { type: "string", format: "date-time", nullable: true },
            finishedAt: { type: "string", format: "date-time", nullable: true },
            createdAt: { type: "string", format: "date-time" },
            updatedAt: { type: "string", format: "date-time" },
          },
        },
        ReplayRunListResponseV2: {
          type: "object",
          required: ["items", "total"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/ReplayRunV2" },
            },
            total: { type: "integer", minimum: 0 },
          },
        },
        ReplayRunDiffsResponseV2: {
          type: "object",
          required: ["runId", "datasetId", "diffs", "total", "summary", "filters"],
          properties: {
            runId: {
              type: "string",
              description: "回放运行主标识，和 ReplayRunV2.id 恒等。",
              example: "run_demo_001",
            },
            jobId: {
              type: "string",
              description: "兼容别名：等价于 runId；新客户端不建议继续依赖 jobId 作为主语义字段。",
              example: "run_demo_001",
            },
            datasetId: { type: "string", example: "dataset_demo_001" },
            diffs: {
              type: "array",
              items: { $ref: "#/components/schemas/ReplayDiffItemV2" },
            },
            total: { type: "integer", minimum: 0 },
            summary: { $ref: "#/components/schemas/ReplayRunSummaryV2" },
            filters: {
              type: "object",
              required: ["datasetId", "keyword", "limit"],
              properties: {
                datasetId: { type: "string", example: "dataset_demo_001" },
                baselineId: {
                  type: "string",
                  description: "兼容别名：等价于 datasetId。",
                  example: "dataset_demo_001",
                },
                runId: {
                  type: "string",
                  description: "回放运行主标识，和 ReplayRunV2.id 恒等。",
                  example: "run_demo_001",
                },
                jobId: {
                  type: "string",
                  description: "兼容别名：等价于 runId；新客户端不建议继续依赖 jobId 作为主语义字段。",
                  example: "run_demo_001",
                },
                keyword: {
                  type: "string",
                  nullable: true,
                  description: "按 caseId/detail/metric/verdict 过滤。",
                  example: "accuracy",
                },
                limit: {
                  type: "integer",
                  nullable: true,
                  minimum: 1,
                  maximum: 500,
                  example: 50,
                },
              },
            },
          },
        },
        ReplayArtifactInlinePreviewV2: {
          type: "object",
          description:
            "工件内联预览。数组或大对象会被截断，仅保留前几条 items 或 topRegressions，避免一次返回完整大文件。",
          additionalProperties: true,
        },
        ReplayArtifactItemV2: {
          type: "object",
          required: ["type", "contentType"],
          properties: {
            type: {
              type: "string",
              enum: ["summary", "diff", "cases"],
              description: "工件类别。",
              example: "summary",
            },
            name: { type: "string", nullable: true },
            description: { type: "string", nullable: true },
            contentType: {
              type: "string",
              description: "工件内容类型。",
              example: "application/json",
            },
            downloadName: { type: "string", nullable: true },
            downloadUrl: {
              type: "string",
              format: "uri-reference",
              nullable: true,
              description: "工件下载地址。",
              example: "/api/v2/replay/runs/run_demo_001/artifacts/summary/download",
            },
            byteSize: { type: "integer", minimum: 0, nullable: true },
            checksum: { type: "string", nullable: true },
            storageBackend: {
              type: "string",
              enum: ["local", "object", "hybrid"],
              nullable: true,
              description: "工件落盘后端。",
              example: "local",
            },
            storageKey: { type: "string", nullable: true },
            metadata: { type: "object", additionalProperties: true },
            createdAt: { type: "string", format: "date-time", nullable: true },
            inline: { $ref: "#/components/schemas/ReplayArtifactInlinePreviewV2" },
          },
        },
        ReplayRunArtifactsResponseV2: {
          type: "object",
          required: ["runId", "datasetId", "items", "total"],
          properties: {
            runId: {
              type: "string",
              description: "回放运行主标识，和 ReplayRunV2.id 恒等。",
              example: "run_demo_001",
            },
            jobId: {
              type: "string",
              description: "兼容别名：等价于 runId；新客户端不建议继续依赖 jobId 作为主语义字段。",
              example: "run_demo_001",
            },
            datasetId: { type: "string", example: "dataset_demo_001" },
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/ReplayArtifactItemV2" },
            },
            total: { type: "integer", minimum: 0 },
          },
        },
        TenantResidencyPolicyInputV2: {
          type: "object",
          required: [
            "mode",
            "primaryRegion",
            "replicaRegions",
            "allowCrossRegionTransfer",
            "requireTransferApproval",
          ],
          properties: {
            mode: { type: "string", enum: ["single_region", "active_active"] },
            primaryRegion: { type: "string" },
            replicaRegions: { type: "array", items: { type: "string" } },
            allowCrossRegionTransfer: { type: "boolean" },
            requireTransferApproval: { type: "boolean" },
            updatedAt: { type: "string", format: "date-time" },
          },
        },
        TenantResidencyPolicyV2: {
          type: "object",
          required: [
            "tenantId",
            "mode",
            "primaryRegion",
            "replicaRegions",
            "allowCrossRegionTransfer",
            "requireTransferApproval",
            "updatedAt",
          ],
          properties: {
            tenantId: { type: "string" },
            mode: { type: "string", enum: ["single_region", "active_active"] },
            primaryRegion: { type: "string" },
            replicaRegions: { type: "array", items: { type: "string" } },
            allowCrossRegionTransfer: { type: "boolean" },
            requireTransferApproval: { type: "boolean" },
            updatedAt: { type: "string", format: "date-time" },
          },
        },
        ResidencyRegionMappingItemV2: {
          type: "object",
          required: ["regionId", "regionName", "active", "role", "writable", "metadata"],
          properties: {
            regionId: { type: "string" },
            regionName: { type: "string" },
            active: { type: "boolean" },
            role: { type: "string", enum: ["primary", "replica", "available"] },
            writable: { type: "boolean" },
            metadata: { type: "object", additionalProperties: true },
          },
        },
        ResidencyRegionMappingListResponseV2: {
          type: "object",
          required: ["items", "total"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/ResidencyRegionMappingItemV2" },
            },
            total: { type: "integer", minimum: 0 },
          },
        },
        ReplicationJobCreateInputV2: {
          type: "object",
          required: ["sourceRegion", "targetRegion"],
          properties: {
            sourceRegion: { type: "string" },
            targetRegion: { type: "string" },
            reason: { type: "string" },
            metadata: { type: "object", additionalProperties: true },
          },
        },
        ReplicationJobReasonInputV2: {
          type: "object",
          properties: {
            reason: { type: "string" },
          },
        },
        ReplicationJobV2: {
          type: "object",
          required: [
            "id",
            "tenantId",
            "sourceRegion",
            "targetRegion",
            "status",
            "metadata",
            "createdAt",
            "updatedAt",
          ],
          properties: {
            id: { type: "string" },
            tenantId: { type: "string" },
            sourceRegion: { type: "string" },
            targetRegion: { type: "string" },
            status: {
              type: "string",
              enum: ["pending", "running", "succeeded", "failed", "cancelled"],
            },
            reason: { type: "string", nullable: true },
            createdByUserId: { type: "string", nullable: true },
            approvedByUserId: { type: "string", nullable: true },
            metadata: { type: "object", additionalProperties: true },
            createdAt: { type: "string", format: "date-time" },
            updatedAt: { type: "string", format: "date-time" },
            startedAt: { type: "string", format: "date-time", nullable: true },
            finishedAt: { type: "string", format: "date-time", nullable: true },
          },
        },
        ReplicationJobListResponseV2: {
          type: "object",
          required: ["items", "total"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/ReplicationJobV2" },
            },
            total: { type: "integer", minimum: 0 },
          },
        },
      },
    },
  };
}

openPlatformRoutes.get("/openapi.json", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  return c.json(buildOpenApiDocument());
});

openPlatformRoutes.get("/api-keys", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const validation = validateApiKeyListInput({
    ...c.req.query(),
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const filter = validation.data;
  const fromTimestamp = filter.from ? Date.parse(filter.from) : undefined;
  const toTimestamp = filter.to ? Date.parse(filter.to) : undefined;
  const keyword = firstNonEmptyString(filter.keyword)?.toLowerCase();
  const limit = filter.limit ?? 200;

  const items = (await repository.listApiKeys(auth.tenantId))
    .map(mapApiKeyItem)
    .filter((item) => (filter.scope ? item.scope === filter.scope : true))
    .filter((item) => (filter.status ? item.status === filter.status : true))
    .filter((item) => {
      if (!keyword) {
        return true;
      }
      return (
        item.name.toLowerCase().includes(keyword) ||
        item.keyPrefix.toLowerCase().includes(keyword)
      );
    })
    .filter((item) => {
      if (fromTimestamp === undefined && toTimestamp === undefined) {
        return true;
      }
      const createdAtTimestamp = Date.parse(item.createdAt);
      if (!Number.isFinite(createdAtTimestamp)) {
        return false;
      }
      if (fromTimestamp !== undefined && createdAtTimestamp < fromTimestamp) {
        return false;
      }
      if (toTimestamp !== undefined && createdAtTimestamp > toTimestamp) {
        return false;
      }
      return true;
    })
    .slice(0, limit);

  return c.json({
    items,
    total: items.length,
    filters: filter,
  });
});

openPlatformRoutes.post("/api-keys", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeStringRecord(body);
  const validation = validateApiKeyCreateInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const secret = randomSecret("sk_live");
  const keyHash = sha256(secret);
  const created = await repository.createApiKey(auth.tenantId, {
    name: validation.data.name,
    keyHash,
    scopes: [validation.data.scope],
  });

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.open_platform.api_key_created",
    level: "info",
    detail: `Created API key ${created.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      keyId: created.id,
      scope: validation.data.scope,
    },
  });

  return c.json(
    {
      id: created.id,
      tenantId: auth.tenantId,
      keyId: created.id,
      keyPrefix: secret.slice(0, 12),
      secret,
      createdAt: created.createdAt,
    },
    201
  );
});

openPlatformRoutes.post("/api-keys/:id/revoke", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const apiKeyId = c.req.param("id")?.trim();
  if (!apiKeyId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeStringRecord(body);
  const validation = validateApiKeyRevokeInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
    keyId: apiKeyId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const revoked = await repository.revokeApiKey(auth.tenantId, apiKeyId);
  if (!revoked) {
    return c.json({ message: `未找到 API Key：${apiKeyId}` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.open_platform.api_key_revoked",
    level: "warning",
    detail: `Revoked API key ${revoked.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      keyId: revoked.id,
      reason: validation.data.reason,
    },
  });

  return c.json(mapApiKeyItem(revoked));
});

openPlatformRoutes.get("/webhooks", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const limitQuery = c.req.query("limit");
  const limit = limitQuery ? Number(limitQuery) : 200;
  if (!Number.isInteger(limit) || limit <= 0 || limit > 500) {
    return c.json({ message: "limit 必须是 1 到 500 的整数。" }, 400);
  }
  const statusQuery = firstNonEmptyString(c.req.query("status"));
  if (statusQuery && !WEBHOOK_STATUS_SET.has(statusQuery)) {
    return c.json({ message: "status 必须是 active/paused/disabled 之一。" }, 400);
  }
  const keyword = firstNonEmptyString(c.req.query("keyword"))?.toLowerCase();

  const items = (await repository.listWebhookEndpoints(auth.tenantId, limit))
    .map(mapWebhookEndpoint)
    .filter((item) => (statusQuery ? item.status === statusQuery : true))
    .filter((item) => (keyword ? item.name.toLowerCase().includes(keyword) : true));

  return c.json({
    items,
    total: items.length,
  });
});

openPlatformRoutes.post("/webhooks", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeStringRecord(body);
  const validation = validateWebhookEndpointCreateInput({
    ...bodyRecord,
    tenantId: auth.tenantId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const secret = validation.data.secret ?? randomSecret("whsec");
  const secretHash = sha256(secret);
  const secretCiphertext = encryptWebhookSecret(secret);
  try {
    const endpoint = await repository.createWebhookEndpoint(auth.tenantId, {
      name: validation.data.name,
      url: validation.data.url,
      eventTypes: validation.data.events,
      enabled: validation.data.status === "active",
      secretHash,
      secretCiphertext,
      headers: {},
    });

    const requestId = c.get("requestId");
    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:${requestId}`,
      action: "control_plane.open_platform.webhook_created",
      level: "info",
      detail: `Created webhook ${endpoint.id}.`,
      metadata: {
        requestId,
        tenantId: auth.tenantId,
        webhookId: endpoint.id,
        status: validation.data.status,
      },
    });

    return c.json(
      {
        ...mapWebhookEndpoint(endpoint),
        secret,
      },
      201
    );
  } catch (error) {
    if (
      error instanceof Error &&
      error.message.startsWith("webhook_endpoint_name_already_exists:")
    ) {
      return c.json({ message: "Webhook 名称已存在，请更换后重试。" }, 409);
    }
    throw error;
  }
});

openPlatformRoutes.put("/webhooks/:id", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const endpointId = c.req.param("id")?.trim();
  if (!endpointId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const bodyRecord = normalizeStringRecord(body);
  const validation = validateWebhookEndpointUpdateInput({
    ...bodyRecord,
    endpointId,
  });
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const updatedSecret = validation.data.secret;
  const updated = await repository.updateWebhookEndpoint(auth.tenantId, endpointId, {
    name: validation.data.name,
    url: validation.data.url,
    eventTypes: validation.data.events,
    enabled:
      validation.data.status !== undefined ? validation.data.status === "active" : undefined,
    secretHash: updatedSecret ? sha256(updatedSecret) : undefined,
    secretCiphertext: updatedSecret ? encryptWebhookSecret(updatedSecret) : undefined,
  });
  if (!updated) {
    return c.json({ message: `未找到 Webhook：${endpointId}` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.open_platform.webhook_updated",
    level: "info",
    detail: `Updated webhook ${updated.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      webhookId: updated.id,
    },
  });

  return c.json({
    ...mapWebhookEndpoint(updated),
    secret: validation.data.secret,
  });
});

openPlatformRoutes.delete("/webhooks/:id", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const endpointId = c.req.param("id")?.trim();
  if (!endpointId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const deleted = await repository.deleteWebhookEndpoint(auth.tenantId, endpointId);
  if (!deleted) {
    return c.json({ message: `未找到 Webhook：${endpointId}` }, 404);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.open_platform.webhook_deleted",
    level: "warning",
    detail: `Deleted webhook ${endpointId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      webhookId: endpointId,
    },
  });

  return c.json({ success: true });
});

openPlatformRoutes.post("/webhooks/:id/replay", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const endpointId = c.req.param("id")?.trim();
  if (!endpointId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const body = await c.req.json().catch(() => undefined);
  const validation = validateWebhookReplayRequestInput(body);
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const endpoint = await repository.getWebhookEndpointById(auth.tenantId, endpointId);
  if (!endpoint) {
    return c.json({ message: `未找到 Webhook：${endpointId}` }, 404);
  }

  const replayTask = await repository.createWebhookReplayTask(auth.tenantId, {
    webhookId: endpoint.id,
    dryRun: validation.data.dryRun,
    filters: {
      eventType: validation.data.eventType,
      from: validation.data.from,
      to: validation.data.to,
      limit: validation.data.limit,
    },
    status: "queued",
    requestedAt: new Date().toISOString(),
    result: {
      executor: WEBHOOK_REPLAY_EXECUTOR,
      dryRun: validation.data.dryRun,
      scannedEvents: 0,
      dispatchedEvents: 0,
      failedEvents: 0,
      retriedEvents: 0,
      retryCount: 0,
      maxRetries: resolveWebhookReplayMaxRetries(),
    },
  });
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.open_platform.webhook_replayed",
    level: "info",
    detail: `Replay requested for webhook ${endpoint.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      webhookId: endpoint.id,
      replayTaskId: replayTask.id,
      replay: validation.data,
    },
  });

  enqueueWebhookReplayExecution(auth.tenantId, replayTask.id);

  return c.json(mapWebhookReplayTask(replayTask), 202);
});

openPlatformRoutes.get("/webhooks/replay-tasks", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const validation = validateWebhookReplayTaskListInput(c.req.query());
  if (!validation.success) {
    return c.json({ message: validation.error }, 400);
  }

  const result = await repository.listWebhookReplayTasks(auth.tenantId, validation.data);
  return c.json({
    items: result.items.map(mapWebhookReplayTask),
    total: result.total,
    nextCursor: result.nextCursor,
    filters: {
      webhookId: validation.data.webhookId,
      status: validation.data.status,
      limit: validation.data.limit,
      cursor: validation.data.cursor,
    },
  });
});

openPlatformRoutes.get("/webhooks/replay-tasks/:id", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const replayTaskId = c.req.param("id")?.trim();
  if (!replayTaskId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const replayTask = await repository.getWebhookReplayTaskById(auth.tenantId, replayTaskId);
  if (!replayTask) {
    return c.json({ message: `未找到 Webhook Replay Task：${replayTaskId}` }, 404);
  }

  return c.json(mapWebhookReplayTask(replayTask));
});
