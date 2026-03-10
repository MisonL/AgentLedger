import { Hono, type Context } from "hono";
import {
  validateIntegrationAlertFailureReportQueryInput,
  validateIntegrationAlertFailureTrendQueryInput,
  validateIntegrationDlqRecoveryJobCreateInput,
  validateIntegrationDlqRecoveryJobListInput,
  validateIntegrationDlqMessageQueryInput,
  validateIntegrationDlqReplayInput,
} from "../contracts";
import type { AppendAuditLogInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";
import {
  connect,
  type JetStreamClient,
  type JetStreamManager,
  type NatsConnection,
} from "nats";

const INTEGRATION_DLQ_STREAM_NAME = "INTEGRATION_DISPATCH_DLQ";
const INTEGRATION_DLQ_QUERY_LIMIT_DEFAULT = 20;
const INTEGRATION_ALERT_FAILURE_REPORT_LIMIT_DEFAULT = 50;
const INTEGRATION_ALERT_FAILURE_REPORT_MAX_PAGES = 1000;
const INTEGRATION_ALERT_FAILURE_TREND_TOP_DEFAULT = 5;
const INTEGRATION_DLQ_RECOVERY_JOB_LIMIT_DEFAULT = 20;

type IntegrationDlqRecoveryJobStatus =
  | "queued"
  | "running"
  | "completed"
  | "failed";

type IntegrationDlqStoredPayload = {
  subject?: string;
  event_type?: string;
  channel?: string;
  callback_id?: string;
  tenant_id?: string;
  alert_id?: string;
  external_type?: string;
  external_id?: string;
  event?: unknown;
  event_raw?: string;
  error?: string;
  retryable?: boolean;
  attempt?: number;
  failed_at?: string;
};

type IntegrationDlqMessageItem = {
  messageId: string;
  stream: string;
  subject: string;
  eventType: string;
  channel?: string;
  callbackId?: string;
  tenantId?: string;
  alertId?: string;
  externalType?: string;
  externalId?: string;
  failedAt: string;
  attempt: number;
  error: string;
  retryable: boolean;
  payload: Record<string, unknown>;
};

type IntegrationDlqListResponse = {
  items: IntegrationDlqMessageItem[];
  total: number;
  filters: {
    eventType?: string;
    channel?: string;
    callbackId?: string;
    alertId?: string;
    limit: number;
  };
};

type IntegrationDlqReplayResponse = {
  replayedCount: number;
  failedCount: number;
  items: Array<{
    messageId: string;
    status: "replayed" | "failed";
    error?: string;
  }>;
};

type IntegrationDlqRecoveryJob = {
  id: string;
  tenantId: string;
  status: IntegrationDlqRecoveryJobStatus;
  requestedAt: string;
  startedAt?: string;
  finishedAt?: string;
  filters?: {
    eventType?: string;
    channel?: string;
    callbackId?: string;
    alertId?: string;
    limit?: number;
  };
  messageIds: string[];
  summary: {
    total: number;
    replayed: number;
    failed: number;
  };
  items: Array<{
    messageId: string;
    status: "replayed" | "failed";
    error?: string;
  }>;
  error?: string;
};

type IntegrationAlertFailureReportActionType =
  | "retry_requested"
  | "retry_completed"
  | "retry_failed"
  | "dlq_queried"
  | "dlq_replayed"
  | "recovery_job_created"
  | "recovery_job_completed"
  | "recovery_job_failed";

type IntegrationAlertFailureReportItem = {
  occurredAt: string;
  action: string;
  actionType: IntegrationAlertFailureReportActionType;
  alertId?: string;
  externalSystem?: string;
  externalType?: string;
  externalId?: string;
  stage?: string;
  code?: string;
  status: "requested" | "success" | "failed";
  requestId?: string;
  metadata: Record<string, unknown>;
};

type IntegrationAlertFailureTrendPoint = {
  date: string;
  totalEvents: number;
  requestedEvents: number;
  successEvents: number;
  failedEvents: number;
  uniqueAlerts: number;
  retryRequested: number;
  retryCompleted: number;
  retryFailed: number;
  dlqQueried: number;
  dlqReplayed: number;
  recoveryJobsCreated: number;
  recoveryJobsCompleted: number;
  recoveryJobsFailed: number;
};

type IntegrationAlertFailureTrendCapacityBucket = {
  name: string;
  totalEvents: number;
  requestedEvents: number;
  successEvents: number;
  failedEvents: number;
  uniqueAlerts: number;
  lastOccurredAt?: string;
};

type IntegrationDlqBackend = {
  listMessages(input: {
    tenantId: string;
    eventType?: string;
    channel?: string;
    callbackId?: string;
    alertId?: string;
    limit: number;
  }): Promise<IntegrationDlqListResponse>;
  replayMessages(input: {
    tenantId: string;
    messageIds: string[];
  }): Promise<IntegrationDlqReplayResponse>;
};

let natsConnectionPromise: Promise<NatsConnection> | null = null;
let integrationDlqBackendOverride: IntegrationDlqBackend | null = null;
const integrationDlqRecoveryJobStore = new Map<string, IntegrationDlqRecoveryJob>();
const integrationDlqRecoveryJobQueue: string[] = [];
let integrationDlqRecoveryDrainScheduled = false;
let integrationDlqRecoveryDrainRunning = false;

export const integrationDlqRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();

function normalizeString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 integration dlq 审计日志失败。", error);
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

function resolveNatsURL(): string | null {
  const value = Bun.env.NATS_URL?.trim();
  return value && value.length > 0 ? value : null;
}

async function getNatsConnection(): Promise<NatsConnection> {
  if (!natsConnectionPromise) {
    const natsUrl = resolveNatsURL();
    if (!natsUrl) {
      throw new Error("NATS_URL 未配置。");
    }
    natsConnectionPromise = connect({
      servers: natsUrl,
      timeout: 1_000,
      reconnect: true,
      maxReconnectAttempts: 3,
      reconnectTimeWait: 250,
    }).catch((error) => {
      natsConnectionPromise = null;
      throw error;
    });
  }
  return natsConnectionPromise;
}

async function getJetStreamManager(): Promise<JetStreamManager> {
  const nc = await getNatsConnection();
  return nc.jetstreamManager();
}

async function getJetStreamClient(): Promise<JetStreamClient> {
  const nc = await getNatsConnection();
  return nc.jetstream();
}

function buildIntegrationDlqMessageId(stream: string, seq: number): string {
  return `${stream}:${seq}`;
}

function parseIntegrationDlqMessageId(
  messageId: string,
): { stream: string; seq: number } | null {
  const [stream, rawSeq] = messageId.split(":", 2);
  const seq = Number(rawSeq);
  if (!stream || !Number.isInteger(seq) || seq <= 0) {
    return null;
  }
  return { stream, seq };
}

function toPayloadRecord(value: unknown): Record<string, unknown> {
  if (!isRecord(value)) {
    return {};
  }
  return JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
}

function parseStoredDlqPayload(
  raw: string,
): { rawPayload: IntegrationDlqStoredPayload; payloadRecord: Record<string, unknown> } {
  if (!raw.trim()) {
    return { rawPayload: {}, payloadRecord: {} };
  }

  const parsed = JSON.parse(raw) as unknown;
  if (!isRecord(parsed)) {
    return { rawPayload: {}, payloadRecord: {} };
  }

  return {
    rawPayload: parsed as IntegrationDlqStoredPayload,
    payloadRecord: toPayloadRecord(parsed),
  };
}

function mapStoredDlqMessage(
  stream: string,
  seq: number,
  payloadRecord: Record<string, unknown>,
  rawPayload: IntegrationDlqStoredPayload,
): IntegrationDlqMessageItem {
  return {
    messageId: buildIntegrationDlqMessageId(stream, seq),
    stream,
    subject: normalizeString(rawPayload.subject) ?? "",
    eventType: normalizeString(rawPayload.event_type) ?? "",
    channel: normalizeString(rawPayload.channel),
    callbackId: normalizeString(rawPayload.callback_id),
    tenantId: normalizeString(rawPayload.tenant_id),
    alertId: normalizeString(rawPayload.alert_id),
    externalType: normalizeString(rawPayload.external_type),
    externalId: normalizeString(rawPayload.external_id),
    failedAt: normalizeString(rawPayload.failed_at) ?? "",
    attempt:
      typeof rawPayload.attempt === "number" && Number.isInteger(rawPayload.attempt)
        ? rawPayload.attempt
        : 0,
    error: normalizeString(rawPayload.error) ?? "",
    retryable: rawPayload.retryable === true,
    payload: payloadRecord,
  };
}

function matchesDlqFilters(
  item: IntegrationDlqMessageItem,
  input: {
    tenantId: string;
    eventType?: string;
    channel?: string;
    callbackId?: string;
    alertId?: string;
  },
): boolean {
  if (item.tenantId !== input.tenantId) {
    return false;
  }
  if (input.eventType && item.eventType !== input.eventType) {
    return false;
  }
  if (input.channel && item.channel !== input.channel) {
    return false;
  }
  if (input.callbackId && item.callbackId !== input.callbackId) {
    return false;
  }
  if (input.alertId && item.alertId !== input.alertId) {
    return false;
  }
  return true;
}

function reconstructDlqEventPayload(rawPayload: IntegrationDlqStoredPayload): Uint8Array {
  if (typeof rawPayload.event_raw === "string" && rawPayload.event_raw.trim().length > 0) {
    return new TextEncoder().encode(rawPayload.event_raw);
  }
  if (rawPayload.event !== undefined) {
    return new TextEncoder().encode(JSON.stringify(rawPayload.event));
  }
  throw new Error("DLQ 消息缺少 event/event_raw，无法 replay。");
}

async function listIntegrationDlqMessagesDefault(input: {
  tenantId: string;
  eventType?: string;
  channel?: string;
  callbackId?: string;
  alertId?: string;
  limit: number;
}): Promise<IntegrationDlqListResponse> {
  const jsm = await getJetStreamManager();
  const info = await jsm.streams.info(INTEGRATION_DLQ_STREAM_NAME);
  const firstSeq = info.state.first_seq;
  const lastSeq = info.state.last_seq;

  const items: IntegrationDlqMessageItem[] = [];
  let total = 0;
  for (let seq = lastSeq; seq >= firstSeq; seq -= 1) {
    let stored: Awaited<ReturnType<typeof jsm.streams.getMessage>>;
    try {
      stored = await jsm.streams.getMessage(INTEGRATION_DLQ_STREAM_NAME, { seq });
    } catch {
      continue;
    }

    const raw = stored.string();
    const { rawPayload, payloadRecord } = parseStoredDlqPayload(raw);
    const item = mapStoredDlqMessage(
      INTEGRATION_DLQ_STREAM_NAME,
      stored.seq,
      payloadRecord,
      rawPayload,
    );
    if (!matchesDlqFilters(item, input)) {
      continue;
    }
    total += 1;
    if (items.length < input.limit) {
      items.push(item);
    }
  }

  return {
    items,
    total,
    filters: {
      eventType: input.eventType,
      channel: input.channel,
      callbackId: input.callbackId,
      alertId: input.alertId,
      limit: input.limit,
    },
  };
}

async function replayIntegrationDlqMessagesDefault(input: {
  tenantId: string;
  messageIds: string[];
}): Promise<IntegrationDlqReplayResponse> {
  const jsm = await getJetStreamManager();
  const js = await getJetStreamClient();

  const storedMessages = await Promise.all(
    input.messageIds.map(async (messageId) => {
      const parsed = parseIntegrationDlqMessageId(messageId);
      if (!parsed || parsed.stream !== INTEGRATION_DLQ_STREAM_NAME) {
        throw new Error(`messageId 不合法：${messageId}`);
      }
      let stored: Awaited<ReturnType<typeof jsm.streams.getMessage>>;
      try {
        stored = await jsm.streams.getMessage(parsed.stream, { seq: parsed.seq });
      } catch {
        throw new Error(`message not found: ${messageId}`);
      }
      return { messageId, stored };
    }),
  );

  const items: IntegrationDlqReplayResponse["items"] = [];
  for (const item of storedMessages) {
    try {
      const { rawPayload, payloadRecord } = parseStoredDlqPayload(item.stored.string());
      const mapped = mapStoredDlqMessage(
        INTEGRATION_DLQ_STREAM_NAME,
        item.stored.seq,
        payloadRecord,
        rawPayload,
      );
      if (mapped.tenantId !== input.tenantId) {
        throw new Error("message not found");
      }
      const subject = normalizeString(rawPayload.subject);
      if (!subject) {
        throw new Error("DLQ 消息缺少 subject，无法 replay。");
      }
      const payload = reconstructDlqEventPayload(rawPayload);
      await js.publish(subject, payload);
      items.push({
        messageId: item.messageId,
        status: "replayed",
      });
    } catch (error) {
      items.push({
        messageId: item.messageId,
        status: "failed",
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  const replayedCount = items.filter((item) => item.status === "replayed").length;
  return {
    replayedCount,
    failedCount: items.length - replayedCount,
    items,
  };
}

function getIntegrationDlqBackend(): IntegrationDlqBackend {
  return (
    integrationDlqBackendOverride ?? {
      listMessages: listIntegrationDlqMessagesDefault,
      replayMessages: replayIntegrationDlqMessagesDefault,
    }
  );
}

const INTEGRATION_ALERT_FAILURE_AUDIT_ACTION_MAP: Record<
  string,
  {
    actionType: IntegrationAlertFailureReportActionType;
    status: "requested" | "success" | "failed";
  }
> = {
  "control_plane.alert_external_link_retry_requested": {
    actionType: "retry_requested",
    status: "requested",
  },
  "control_plane.alert_external_link_retry_completed": {
    actionType: "retry_completed",
    status: "success",
  },
  "control_plane.alert_external_link_retry_failed": {
    actionType: "retry_failed",
    status: "failed",
  },
  "control_plane.integration_dlq_messages_queried": {
    actionType: "dlq_queried",
    status: "requested",
  },
  "control_plane.integration_dlq_messages_replayed": {
    actionType: "dlq_replayed",
    status: "success",
  },
  "control_plane.integration_dlq_recovery_job_created": {
    actionType: "recovery_job_created",
    status: "requested",
  },
  "control_plane.integration_dlq_recovery_job_completed": {
    actionType: "recovery_job_completed",
    status: "success",
  },
  "control_plane.integration_dlq_recovery_job_failed": {
    actionType: "recovery_job_failed",
    status: "failed",
  },
};

async function listAllFailureReportAudits(input: {
  tenantId: string;
  from?: string;
  to?: string;
  limit: number;
}) {
  const items: Awaited<ReturnType<typeof repository.listAudits>>["items"] = [];
  let cursor: string | undefined;

  for (let page = 0; page < INTEGRATION_ALERT_FAILURE_REPORT_MAX_PAGES; page += 1) {
    const payload = await repository.listAudits(
      {
        from: input.from,
        to: input.to,
        limit: 200,
        cursor,
      },
      input.tenantId,
    );
    items.push(...payload.items);
    if (!payload.nextCursor) {
      break;
    }
    cursor = payload.nextCursor;
  }

  return items;
}

function toFailureReportStage(item: {
  action: string;
  metadata: Record<string, unknown>;
}): string | undefined {
  const metadata = item.metadata;
  return (
    normalizeString(metadata.lastSyncFailureStage) ??
    normalizeString(metadata.failureStage) ??
    normalizeString(metadata.stage) ??
    (item.action === "control_plane.alert_external_link_retry_failed"
      ? "publish"
      : undefined)
  );
}

function toFailureReportCode(metadata: Record<string, unknown>): string | undefined {
  return (
    normalizeString(metadata.lastSyncFailureCode) ??
    normalizeString(metadata.failureCode) ??
    normalizeString(metadata.code)
  );
}

function toFailureReportExternalSystem(metadata: Record<string, unknown>): string | undefined {
  return normalizeString(metadata.externalSystem) ?? normalizeString(metadata.external_system);
}

function toFailureReportAlertId(metadata: Record<string, unknown>): string | undefined {
  return normalizeString(metadata.alertId) ?? normalizeString(metadata.alert_id);
}

function toFailureReportItem(item: {
  action: string;
  createdAt: string;
  metadata: Record<string, unknown>;
}): IntegrationAlertFailureReportItem | null {
  const mapped = INTEGRATION_ALERT_FAILURE_AUDIT_ACTION_MAP[item.action];
  if (!mapped) {
    return null;
  }
  return {
    occurredAt: item.createdAt,
    action: item.action,
    actionType: mapped.actionType,
    alertId: toFailureReportAlertId(item.metadata),
    externalSystem: toFailureReportExternalSystem(item.metadata),
    externalType:
      normalizeString(item.metadata.externalType) ??
      normalizeString(item.metadata.external_type),
    externalId:
      normalizeString(item.metadata.externalId) ??
      normalizeString(item.metadata.external_id),
    stage: toFailureReportStage(item),
    code: toFailureReportCode(item.metadata),
    status: mapped.status,
    requestId: normalizeString(item.metadata.requestId) ?? normalizeString(item.metadata.request_id),
    metadata: item.metadata,
  };
}

function filterFailureReportItems(
  items: IntegrationAlertFailureReportItem[],
  input: {
    externalSystem?: string;
    stage?: string;
    actionType?: IntegrationAlertFailureReportActionType;
  },
) {
  return items
    .filter((item) => (input.externalSystem ? item.externalSystem === input.externalSystem : true))
    .filter((item) => (input.stage ? item.stage === input.stage : true))
    .filter((item) => (input.actionType ? item.actionType === input.actionType : true))
    .sort((left, right) => right.occurredAt.localeCompare(left.occurredAt));
}

function buildFailureReportSummary(items: IntegrationAlertFailureReportItem[]) {
  return items.reduce(
    (acc, item) => {
      acc.totalEvents += 1;
      switch (item.actionType) {
        case "retry_requested":
          acc.retryRequested += 1;
          break;
        case "retry_completed":
          acc.retryCompleted += 1;
          break;
        case "retry_failed":
          acc.retryFailed += 1;
          break;
        case "dlq_queried":
          acc.dlqQueried += 1;
          break;
        case "dlq_replayed":
          acc.dlqReplayed += 1;
          break;
        case "recovery_job_created":
          acc.recoveryJobsCreated += 1;
          break;
        case "recovery_job_completed":
          acc.recoveryJobsCompleted += 1;
          break;
        case "recovery_job_failed":
          acc.recoveryJobsFailed += 1;
          break;
      }
      return acc;
    },
    {
      totalEvents: 0,
      retryRequested: 0,
      retryCompleted: 0,
      retryFailed: 0,
      dlqQueried: 0,
      dlqReplayed: 0,
      recoveryJobsCreated: 0,
      recoveryJobsCompleted: 0,
      recoveryJobsFailed: 0,
    },
  );
}

function toFailureTrendDate(occurredAt: string): string {
  return occurredAt.slice(0, 10);
}

function buildFailureTrendDaily(items: IntegrationAlertFailureReportItem[]) {
  const buckets = new Map<
    string,
    IntegrationAlertFailureTrendPoint & { alertIds: Set<string> }
  >();

  for (const item of items) {
    const date = toFailureTrendDate(item.occurredAt);
    const bucket =
      buckets.get(date) ??
      {
        date,
        totalEvents: 0,
        requestedEvents: 0,
        successEvents: 0,
        failedEvents: 0,
        uniqueAlerts: 0,
        retryRequested: 0,
        retryCompleted: 0,
        retryFailed: 0,
        dlqQueried: 0,
        dlqReplayed: 0,
        recoveryJobsCreated: 0,
        recoveryJobsCompleted: 0,
        recoveryJobsFailed: 0,
        alertIds: new Set<string>(),
      };

    bucket.totalEvents += 1;
    if (item.status === "requested") {
      bucket.requestedEvents += 1;
    } else if (item.status === "success") {
      bucket.successEvents += 1;
    } else {
      bucket.failedEvents += 1;
    }
    if (item.alertId) {
      bucket.alertIds.add(item.alertId);
    }
    switch (item.actionType) {
      case "retry_requested":
        bucket.retryRequested += 1;
        break;
      case "retry_completed":
        bucket.retryCompleted += 1;
        break;
      case "retry_failed":
        bucket.retryFailed += 1;
        break;
      case "dlq_queried":
        bucket.dlqQueried += 1;
        break;
      case "dlq_replayed":
        bucket.dlqReplayed += 1;
        break;
      case "recovery_job_created":
        bucket.recoveryJobsCreated += 1;
        break;
      case "recovery_job_completed":
        bucket.recoveryJobsCompleted += 1;
        break;
      case "recovery_job_failed":
        bucket.recoveryJobsFailed += 1;
        break;
    }
    bucket.uniqueAlerts = bucket.alertIds.size;
    buckets.set(date, bucket);
  }

  return Array.from(buckets.values())
    .map(({ alertIds: _alertIds, ...item }) => item)
    .sort((left, right) => left.date.localeCompare(right.date));
}

function buildFailureTrendCapacity(
  items: IntegrationAlertFailureReportItem[],
  resolveKey: (item: IntegrationAlertFailureReportItem) => string,
  top: number,
): IntegrationAlertFailureTrendCapacityBucket[] {
  const buckets = new Map<
    string,
    IntegrationAlertFailureTrendCapacityBucket & { alertIds: Set<string> }
  >();

  for (const item of items) {
    const key = resolveKey(item);
    const bucket =
      buckets.get(key) ??
      {
        name: key,
        totalEvents: 0,
        requestedEvents: 0,
        successEvents: 0,
        failedEvents: 0,
        uniqueAlerts: 0,
        lastOccurredAt: undefined,
        alertIds: new Set<string>(),
      };

    bucket.totalEvents += 1;
    if (item.status === "requested") {
      bucket.requestedEvents += 1;
    } else if (item.status === "success") {
      bucket.successEvents += 1;
    } else {
      bucket.failedEvents += 1;
    }
    if (!bucket.lastOccurredAt || item.occurredAt > bucket.lastOccurredAt) {
      bucket.lastOccurredAt = item.occurredAt;
    }
    if (item.alertId) {
      bucket.alertIds.add(item.alertId);
    }
    bucket.uniqueAlerts = bucket.alertIds.size;
    buckets.set(key, bucket);
  }

  return Array.from(buckets.values())
    .map(({ alertIds: _alertIds, ...item }) => item)
    .sort(
      (left, right) =>
        right.totalEvents - left.totalEvents ||
        right.failedEvents - left.failedEvents ||
        left.name.localeCompare(right.name),
    )
    .slice(0, top);
}

function cloneIntegrationDlqRecoveryJob(
  job: IntegrationDlqRecoveryJob,
): IntegrationDlqRecoveryJob {
  return JSON.parse(JSON.stringify(job)) as IntegrationDlqRecoveryJob;
}

function listIntegrationDlqRecoveryJobs(input: {
  tenantId: string;
  status?: IntegrationDlqRecoveryJobStatus;
  limit: number;
}) {
  const items = Array.from(integrationDlqRecoveryJobStore.values())
    .filter((item) => item.tenantId === input.tenantId)
    .filter((item) => (input.status ? item.status === input.status : true))
    .sort(
      (left, right) =>
        right.requestedAt.localeCompare(left.requestedAt) || right.id.localeCompare(left.id),
    );
  return {
    items: items.slice(0, input.limit).map(cloneIntegrationDlqRecoveryJob),
    total: items.length,
    filters: {
      status: input.status,
      limit: input.limit,
    },
  };
}

function getIntegrationDlqRecoveryJobById(
  tenantId: string,
  jobId: string,
): IntegrationDlqRecoveryJob | null {
  const item = integrationDlqRecoveryJobStore.get(jobId);
  if (!item || item.tenantId !== tenantId) {
    return null;
  }
  return cloneIntegrationDlqRecoveryJob(item);
}

function updateIntegrationDlqRecoveryJob(
  jobId: string,
  update: Partial<IntegrationDlqRecoveryJob>,
): IntegrationDlqRecoveryJob | null {
  const current = integrationDlqRecoveryJobStore.get(jobId);
  if (!current) {
    return null;
  }
  const next: IntegrationDlqRecoveryJob = {
    ...current,
    ...update,
    summary: {
      ...current.summary,
      ...(update.summary ?? {}),
    },
    items: update.items ?? current.items,
  };
  integrationDlqRecoveryJobStore.set(jobId, next);
  return next;
}

async function runIntegrationDlqRecoveryJob(jobId: string): Promise<void> {
  const current = integrationDlqRecoveryJobStore.get(jobId);
  if (!current || current.status !== "queued") {
    return;
  }

  const running = updateIntegrationDlqRecoveryJob(jobId, {
    status: "running",
    startedAt: new Date().toISOString(),
    finishedAt: undefined,
    error: undefined,
  });
  if (!running) {
    return;
  }

  try {
    const payload = await getIntegrationDlqBackend().replayMessages({
      tenantId: running.tenantId,
      messageIds: running.messageIds,
    });
    const finishedAt = new Date().toISOString();
    const nextStatus: IntegrationDlqRecoveryJobStatus =
      payload.failedCount > 0 ? "failed" : "completed";
    const updated = updateIntegrationDlqRecoveryJob(jobId, {
      status: nextStatus,
      finishedAt,
      summary: {
        total: running.messageIds.length,
        replayed: payload.replayedCount,
        failed: payload.failedCount,
      },
      items: payload.items,
      error:
        payload.failedCount > 0
          ? `${payload.failedCount} 条消息恢复失败。`
          : undefined,
    });
    if (updated) {
      await appendAuditLogSafely({
        tenantId: updated.tenantId,
        eventId: `cp:integration-dlq-recovery:${updated.id}:${finishedAt}`,
        action:
          nextStatus === "completed"
            ? "control_plane.integration_dlq_recovery_job_completed"
            : "control_plane.integration_dlq_recovery_job_failed",
        level: nextStatus === "completed" ? "info" : "warning",
        detail:
          nextStatus === "completed"
            ? `Integration DLQ recovery job ${updated.id} completed.`
            : `Integration DLQ recovery job ${updated.id} finished with failures.`,
        metadata: {
          jobId: updated.id,
          tenantId: updated.tenantId,
          messageIds: updated.messageIds,
          total: updated.summary.total,
          replayed: updated.summary.replayed,
          failed: updated.summary.failed,
          filters: updated.filters,
        },
      });
    }
  } catch (error) {
    const failedAt = new Date().toISOString();
    const message = error instanceof Error ? error.message : String(error);
    const updated = updateIntegrationDlqRecoveryJob(jobId, {
      status: "failed",
      finishedAt: failedAt,
      error: message,
      summary: {
        total: running.messageIds.length,
        replayed: 0,
        failed: running.messageIds.length,
      },
      items: running.messageIds.map((messageId) => ({
        messageId,
        status: "failed",
        error: message,
      })),
    });
    if (updated) {
      await appendAuditLogSafely({
        tenantId: updated.tenantId,
        eventId: `cp:integration-dlq-recovery:${updated.id}:${failedAt}`,
        action: "control_plane.integration_dlq_recovery_job_failed",
        level: "error",
        detail: `Integration DLQ recovery job ${updated.id} failed.`,
        metadata: {
          jobId: updated.id,
          tenantId: updated.tenantId,
          messageIds: updated.messageIds,
          total: updated.summary.total,
          replayed: updated.summary.replayed,
          failed: updated.summary.failed,
          filters: updated.filters,
          error: message,
        },
      });
    }
  }
}

async function drainIntegrationDlqRecoveryQueue(): Promise<void> {
  if (integrationDlqRecoveryDrainRunning) {
    return;
  }
  integrationDlqRecoveryDrainRunning = true;
  try {
    while (integrationDlqRecoveryJobQueue.length > 0) {
      const jobId = integrationDlqRecoveryJobQueue.shift();
      if (!jobId) {
        continue;
      }
      try {
        await runIntegrationDlqRecoveryJob(jobId);
      } catch (error) {
        console.warn("[control-plane] integration dlq recovery worker 执行失败。", error);
      }
    }
  } finally {
    integrationDlqRecoveryDrainRunning = false;
    if (integrationDlqRecoveryJobQueue.length > 0) {
      scheduleIntegrationDlqRecoveryDrain();
    }
  }
}

function scheduleIntegrationDlqRecoveryDrain(): void {
  if (integrationDlqRecoveryDrainScheduled) {
    return;
  }
  integrationDlqRecoveryDrainScheduled = true;
  setTimeout(() => {
    integrationDlqRecoveryDrainScheduled = false;
    void drainIntegrationDlqRecoveryQueue();
  }, 0);
}

function enqueueIntegrationDlqRecoveryJob(jobId: string): void {
  if (!jobId.trim()) {
    return;
  }
  integrationDlqRecoveryJobQueue.push(jobId);
  scheduleIntegrationDlqRecoveryDrain();
}

export async function __resetIntegrationDlqBackendForTests(): Promise<void> {
  integrationDlqBackendOverride = null;
  integrationDlqRecoveryJobStore.clear();
  integrationDlqRecoveryJobQueue.length = 0;
  integrationDlqRecoveryDrainScheduled = false;
  integrationDlqRecoveryDrainRunning = false;
  if (!natsConnectionPromise) {
    return;
  }
  try {
    const connection = await natsConnectionPromise;
    await connection.drain();
  } catch {
    // ignore cleanup errors
  } finally {
    natsConnectionPromise = null;
  }
}

export function __setIntegrationDlqBackendForTests(
  backend: IntegrationDlqBackend | null,
): void {
  integrationDlqBackendOverride = backend;
}

export async function __drainIntegrationDlqRecoveryQueueForTests(): Promise<void> {
  await drainIntegrationDlqRecoveryQueue();
}

integrationDlqRoutes.post("/integrations/dlq/recovery-jobs", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateIntegrationDlqRecoveryJobCreateInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  let messageIds = result.data.messageIds ?? [];
  const filters = result.data.filters;
  if (filters) {
    try {
      const snapshot = await getIntegrationDlqBackend().listMessages({
        tenantId: auth.tenantId,
        eventType: filters.eventType,
        channel: filters.channel,
        callbackId: filters.callbackId,
        alertId: filters.alertId,
        limit: filters.limit ?? INTEGRATION_DLQ_QUERY_LIMIT_DEFAULT,
      });
      messageIds = snapshot.items.map((item) => item.messageId);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return c.json(
        { message: `当前未接入 integration DLQ 读取能力：${message}` },
        503,
      );
    }
  }

  if (messageIds.length === 0) {
    return c.json({ message: "当前没有可恢复的 DLQ 消息。" }, 409);
  }

  const requestedAt = new Date().toISOString();
  const job: IntegrationDlqRecoveryJob = {
    id: crypto.randomUUID(),
    tenantId: auth.tenantId,
    status: "queued",
    requestedAt,
    filters: filters ? { ...filters } : undefined,
    messageIds: [...messageIds],
    summary: {
      total: messageIds.length,
      replayed: 0,
      failed: 0,
    },
    items: [],
  };
  integrationDlqRecoveryJobStore.set(job.id, job);

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.integration_dlq_recovery_job_created",
    level: "info",
    detail: `Created integration DLQ recovery job ${job.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      userId: auth.userId,
      jobId: job.id,
      messageIds: job.messageIds,
      total: job.summary.total,
      filters: job.filters,
    },
  });

  enqueueIntegrationDlqRecoveryJob(job.id);
  return c.json(cloneIntegrationDlqRecoveryJob(job), 202);
});

integrationDlqRoutes.get("/integrations/dlq/recovery-jobs", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateIntegrationDlqRecoveryJobListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  return c.json(
    listIntegrationDlqRecoveryJobs({
      tenantId: auth.tenantId,
      status: result.data.status,
      limit: result.data.limit ?? INTEGRATION_DLQ_RECOVERY_JOB_LIMIT_DEFAULT,
    }),
  );
});

integrationDlqRoutes.get("/integrations/dlq/recovery-jobs/:id", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const jobId = c.req.param("id")?.trim();
  if (!jobId) {
    return c.json({ message: "id 必须为非空字符串。" }, 400);
  }

  const job = getIntegrationDlqRecoveryJobById(auth.tenantId, jobId);
  if (!job) {
    return c.json({ message: `未找到 Integration DLQ Recovery Job：${jobId}` }, 404);
  }

  return c.json(job);
});

integrationDlqRoutes.get("/integrations/dlq/messages", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateIntegrationDlqMessageQueryInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  try {
    const payload = await getIntegrationDlqBackend().listMessages({
      tenantId: auth.tenantId,
      eventType: result.data.eventType,
      channel: result.data.channel,
      callbackId: result.data.callbackId,
      alertId: result.data.alertId,
      limit: result.data.limit ?? INTEGRATION_DLQ_QUERY_LIMIT_DEFAULT,
    });
    const requestId = c.get("requestId");
    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:${requestId}`,
      action: "control_plane.integration_dlq_messages_queried",
      level: "info",
      detail: "Queried integration DLQ messages.",
      metadata: {
        requestId,
        tenantId: auth.tenantId,
        userId: auth.userId,
        filters: payload.filters,
        total: payload.total,
        returned: payload.items.length,
      },
    });
    return c.json(payload);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return c.json(
      {
        message: `当前未接入 integration DLQ 读取能力：${message}`,
      },
      503,
    );
  }
});

integrationDlqRoutes.get("/integrations/failure-reports/alerts", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateIntegrationAlertFailureReportQueryInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const audits = await listAllFailureReportAudits({
    tenantId: auth.tenantId,
    from: result.data.from,
    to: result.data.to,
    limit: result.data.limit ?? INTEGRATION_ALERT_FAILURE_REPORT_LIMIT_DEFAULT,
  });
  const filteredItems = filterFailureReportItems(
    audits
      .map((item) => toFailureReportItem(item))
      .filter((item): item is IntegrationAlertFailureReportItem => Boolean(item)),
    {
      externalSystem: result.data.externalSystem,
      stage: result.data.stage,
      actionType: result.data.actionType,
    },
  );

  return c.json({
    summary: buildFailureReportSummary(filteredItems),
    items: filteredItems.slice(
      0,
      result.data.limit ?? INTEGRATION_ALERT_FAILURE_REPORT_LIMIT_DEFAULT,
    ),
    filters: {
      from: result.data.from,
      to: result.data.to,
      externalSystem: result.data.externalSystem,
      stage: result.data.stage,
      actionType: result.data.actionType,
      limit: result.data.limit ?? INTEGRATION_ALERT_FAILURE_REPORT_LIMIT_DEFAULT,
    },
  });
});

integrationDlqRoutes.get("/integrations/failure-reports/alerts/trends", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateIntegrationAlertFailureTrendQueryInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const audits = await listAllFailureReportAudits({
    tenantId: auth.tenantId,
    from: result.data.from,
    to: result.data.to,
    limit: 200,
  });
  const filteredItems = filterFailureReportItems(
    audits
      .map((item) => toFailureReportItem(item))
      .filter((item): item is IntegrationAlertFailureReportItem => Boolean(item)),
    {
      externalSystem: result.data.externalSystem,
      stage: result.data.stage,
      actionType: result.data.actionType,
    },
  );
  const top = result.data.top ?? INTEGRATION_ALERT_FAILURE_TREND_TOP_DEFAULT;
  const daily = buildFailureTrendDaily(filteredItems);
  const peakPoint = daily.reduce<IntegrationAlertFailureTrendPoint | null>(
    (acc, item) => {
      if (!acc) {
        return item;
      }
      if (item.totalEvents > acc.totalEvents) {
        return item;
      }
      if (item.totalEvents === acc.totalEvents && item.date > acc.date) {
        return item;
      }
      return acc;
    },
    null,
  );

  return c.json({
    summary: {
      totalEvents: filteredItems.length,
      requestedEvents: filteredItems.filter((item) => item.status === "requested").length,
      successEvents: filteredItems.filter((item) => item.status === "success").length,
      failedEvents: filteredItems.filter((item) => item.status === "failed").length,
      days: daily.length,
      averageEventsPerDay:
        daily.length > 0 ? Number((filteredItems.length / daily.length).toFixed(2)) : 0,
      peakDate: peakPoint?.date,
      peakCount: peakPoint?.totalEvents ?? 0,
    },
    daily,
    capacity: {
      externalSystems: buildFailureTrendCapacity(
        filteredItems,
        (item) => item.externalSystem ?? "unknown",
        top,
      ),
      stages: buildFailureTrendCapacity(filteredItems, (item) => item.stage ?? "unknown", top),
    },
    filters: {
      from: result.data.from,
      to: result.data.to,
      externalSystem: result.data.externalSystem,
      stage: result.data.stage,
      actionType: result.data.actionType,
      top,
    },
  });
});

integrationDlqRoutes.post("/integrations/dlq/messages/replay", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateIntegrationDlqReplayInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  try {
    const payload = await getIntegrationDlqBackend().replayMessages({
      tenantId: auth.tenantId,
      messageIds: result.data.messageIds,
    });
    const requestId = c.get("requestId");
    await appendAuditLogSafely({
      tenantId: auth.tenantId,
      eventId: `cp:${requestId}`,
      action: "control_plane.integration_dlq_messages_replayed",
      level: payload.failedCount > 0 ? "warning" : "info",
      detail: `Replayed ${payload.replayedCount} integration DLQ messages.`,
      metadata: {
        requestId,
        tenantId: auth.tenantId,
        userId: auth.userId,
        messageIds: result.data.messageIds,
        replayedCount: payload.replayedCount,
        failedCount: payload.failedCount,
      },
    });
    return c.json(payload);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes("messageId")) {
      return c.json({ message }, 400);
    }
    if (message.includes("404") || message.includes("not found")) {
      return c.json({ message }, 404);
    }
    return c.json(
      {
        message: `当前未接入 integration DLQ replay 能力：${message}`,
      },
      503,
    );
  }
});
