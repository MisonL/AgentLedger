import { afterEach, describe, expect, test } from "bun:test";
import type { NatsConnection, PubAck } from "nats";
import {
  __resetAlertExternalStatusSyncPublisherForTests,
  __setAlertExternalStatusSyncConnectionProviderForTests,
  publishAlertExternalStatusSyncEvents,
  type AlertExternalStatusSyncEvent,
} from "../src/routes/integration-event-publisher";

afterEach(async () => {
  await __resetAlertExternalStatusSyncPublisherForTests();
});

function buildEvent(callbackId: string): AlertExternalStatusSyncEvent {
  return {
    callback_id: callbackId,
    tenant_id: "tenant-publisher-test",
    action: "upsert_external_link",
    alert_id: "alert-publisher-test",
    external_type: "ticket",
    external_system: "ticket",
    external_id: `ticket-${callbackId}`,
    external_status: "acknowledged",
  };
}

describe("integration-event-publisher", () => {
  test("JetStream PubAck 成功时返回 published 结果并传递 msgID", async () => {
    const publishCalls: Array<{
      subject: string;
      payload: string;
      msgID?: string;
    }> = [];

    __setAlertExternalStatusSyncConnectionProviderForTests(async () => {
      return {
        jetstream() {
          return {
            publish: async (
              subject: string,
              payload?: Uint8Array,
              options?: { msgID?: string },
            ) => {
              publishCalls.push({
                subject,
                payload: payload ? new TextDecoder().decode(payload) : "",
                msgID: options?.msgID,
              });
              return {
                stream: "INTEGRATION_ALERT_EXTERNAL_STATUS_SYNC_EVENTS",
                seq: publishCalls.length,
              } as PubAck;
            },
          };
        },
        drain: async () => {},
        close: () => {},
      } as unknown as NatsConnection;
    });

    const result = await publishAlertExternalStatusSyncEvents([
      buildEvent("cb-1"),
      buildEvent("cb-2"),
    ]);

    expect(result).toEqual({
      published: 2,
      failed: 0,
      errors: [],
    });
    expect(publishCalls).toHaveLength(2);
    expect(publishCalls[0]).toMatchObject({
      subject: "integration.alert.external_status_sync",
      msgID: "cb-1",
    });
    expect(publishCalls[1]).toMatchObject({
      subject: "integration.alert.external_status_sync",
      msgID: "cb-2",
    });
    expect(JSON.parse(publishCalls[0]!.payload)).toMatchObject({
      callback_id: "cb-1",
      external_id: "ticket-cb-1",
    });
  });

  test("JetStream publish 失败时返回 failed 结果", async () => {
    __setAlertExternalStatusSyncConnectionProviderForTests(async () => {
      return {
        jetstream() {
          return {
            publish: async () => {
              throw new Error("no stream matches subject");
            },
          };
        },
        drain: async () => {},
        close: () => {},
      } as unknown as NatsConnection;
    });

    const result = await publishAlertExternalStatusSyncEvents([
      buildEvent("cb-failed"),
    ]);

    expect(result.published).toBe(0);
    expect(result.failed).toBe(1);
    expect(result.errors).toEqual([
      {
        callbackId: "cb-failed",
        message: "no stream matches subject",
      },
    ]);
  });
});
