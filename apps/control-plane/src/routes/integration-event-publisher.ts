import { connect, type NatsConnection } from "nats";

export interface AlertExternalStatusSyncEvent {
  callback_id: string;
  tenant_id: string;
  action: "upsert_external_link";
  alert_id: string;
  external_type: "ticket" | "case" | "incident";
  external_system: string;
  external_id: string;
  external_status: string;
  metadata?: Record<string, unknown>;
}

export interface AlertExternalStatusSyncPublishResult {
  published: number;
  failed: number;
  errors: Array<{
    callbackId: string;
    message: string;
  }>;
}

type AlertExternalStatusSyncPublisher = (
  events: AlertExternalStatusSyncEvent[],
) => Promise<AlertExternalStatusSyncPublishResult>;

type AlertExternalStatusSyncConnectionProvider = () => Promise<NatsConnection>;

const DEFAULT_ALERT_EXTERNAL_STATUS_SYNC_SUBJECT =
  "integration.alert.external_status_sync";

let connectionPromise: Promise<NatsConnection> | null = null;
let publisherOverride: AlertExternalStatusSyncPublisher | null = null;
let connectionProviderOverride: AlertExternalStatusSyncConnectionProvider | null =
  null;
const textEncoder = new TextEncoder();

function resolveNatsURL(): string | null {
  const value = Bun.env.NATS_URL?.trim();
  return value && value.length > 0 ? value : null;
}

function resolveAlertExternalStatusSyncSubject(): string {
  const configured =
    Bun.env.INTEGRATION_ALERT_EXTERNAL_STATUS_SYNC_SUBJECT?.trim();
  return configured && configured.length > 0
    ? configured
    : DEFAULT_ALERT_EXTERNAL_STATUS_SYNC_SUBJECT;
}

async function getNatsConnection(): Promise<NatsConnection> {
  if (connectionProviderOverride) {
    return connectionProviderOverride();
  }
  if (!connectionPromise) {
    const natsUrl = resolveNatsURL();
    if (!natsUrl) {
      throw new Error("NATS_URL 未配置。");
    }
    connectionPromise = connect({
      servers: natsUrl,
      timeout: 1_000,
      reconnect: true,
      maxReconnectAttempts: 3,
      reconnectTimeWait: 250,
    }).catch((error) => {
      connectionPromise = null;
      throw error;
    });
  }
  return connectionPromise;
}

export function __setAlertExternalStatusSyncPublisherForTests(
  publisher: AlertExternalStatusSyncPublisher | null,
): void {
  publisherOverride = publisher;
}

export function __setAlertExternalStatusSyncConnectionProviderForTests(
  provider: AlertExternalStatusSyncConnectionProvider | null,
): void {
  connectionProviderOverride = provider;
}

export async function __resetAlertExternalStatusSyncPublisherForTests(): Promise<void> {
  publisherOverride = null;
  connectionProviderOverride = null;
  if (!connectionPromise) {
    return;
  }
  try {
    const connection = await connectionPromise;
    await connection.drain();
  } catch {
    // ignore test cleanup errors
  } finally {
    connectionPromise = null;
  }
}

export async function publishAlertExternalStatusSyncEvents(
  events: AlertExternalStatusSyncEvent[],
): Promise<AlertExternalStatusSyncPublishResult> {
  if (!Array.isArray(events) || events.length === 0) {
    return { published: 0, failed: 0, errors: [] };
  }

  if (publisherOverride) {
    return publisherOverride(events);
  }

  const subject = resolveAlertExternalStatusSyncSubject();
  const result: AlertExternalStatusSyncPublishResult = {
    published: 0,
    failed: 0,
    errors: [],
  };

  let nc: NatsConnection;
  try {
    nc = await getNatsConnection();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      published: 0,
      failed: events.length,
      errors: events.map((event) => ({
        callbackId: event.callback_id,
        message,
      })),
    };
  }

  const js = nc.jetstream();

  for (const event of events) {
    try {
      const payload = textEncoder.encode(JSON.stringify(event));
      await js.publish(subject, payload, {
        msgID: event.callback_id,
      });
      result.published += 1;
    } catch (error) {
      result.failed += 1;
      result.errors.push({
        callbackId: event.callback_id,
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return result;
}
