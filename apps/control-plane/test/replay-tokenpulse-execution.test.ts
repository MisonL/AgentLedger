import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { getControlPlaneRepository } from "../src/data/repository";
import {
  enqueueReplayJobExecution,
  flushReplayJobExecutionQueueForTests,
  resetReplayJobExecutionWorkerForTests,
} from "../src/routes/replay";

const repository = getControlPlaneRepository();

function createNonce(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function setEnv(key: string, value: string | undefined): () => void {
  const env = Bun.env as Record<string, string | undefined>;
  const previous = env[key];
  env[key] = value ?? "";
  return () => {
    env[key] = previous ?? "";
  };
}

describe("Replay TokenPulse Execution Backend", () => {
  const originalFetch = globalThis.fetch;
  const cleanups: Array<() => void> = [];

  beforeEach(() => {
    resetReplayJobExecutionWorkerForTests();
    cleanups.length = 0;
  });

  afterEach(() => {
    resetReplayJobExecutionWorkerForTests();
    globalThis.fetch = originalFetch;
    while (cleanups.length > 0) {
      const cleanup = cleanups.pop();
      try {
        cleanup?.();
      } catch {
        // noop
      }
    }
  });

  test("tokenpulse 执行成功时会产出 completed，并持久化 summary/diff/cases artifacts", async () => {
    const nonce = createNonce("replay-tokenpulse-success");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });
    const baseline = await repository.createReplayBaseline(tenant.id, {
      name: `baseline-${nonce}`,
      datasetRef: `dataset-${nonce}`,
      scenarioCount: 2,
      metadata: {
        model: "gpt-baseline",
      },
    });
    await repository.replaceReplayDatasetCases(tenant.id, baseline.id, [
      {
        caseId: `case-${nonce}-1`,
        input: "你好，简单回答 1+1 等于几？",
      },
      {
        caseId: `case-${nonce}-2`,
        input: "你好，简单回答 2+2 等于几？",
      },
    ]);

    const localRoot = `./data/replay-artifacts-${nonce}`;
    cleanups.push(setEnv("REPLAY_STORAGE_LOCAL_ROOT", localRoot));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_BASE_URL", "https://tokenpulse.test"));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_API_SECRET", "secret"));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_MAX_CONCURRENCY", "2"));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_TIMEOUT_MS", "3000"));

    const calls: Array<{ url: string; requestId: string; tenantId: string; model: string }> = [];
    globalThis.fetch = (async (url: string | URL, init?: RequestInit) => {
      const headers = new Headers(init?.headers);
      const requestId = headers.get("x-request-id") || "";
      const tenantId = headers.get("x-tokenpulse-tenant") || "";
      const bodyText = typeof init?.body === "string" ? init.body : "";
      const bodyJson = bodyText ? (JSON.parse(bodyText) as Record<string, unknown>) : {};
      const model = typeof bodyJson.model === "string" ? bodyJson.model : "";

      calls.push({ url: url.toString(), requestId, tenantId, model });

      return new Response(
        JSON.stringify({
          choices: [
            {
              message: {
                content: `ok:${model}:${tenantId}:${requestId}`,
              },
            },
          ],
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
            "x-request-id": requestId,
            "x-tokenpulse-provider": "openai",
            "x-tokenpulse-route-policy": "round_robin",
            "x-tokenpulse-account-id": "acct-1",
            "x-tokenpulse-fallback": "none",
          },
        },
      );
    }) as unknown as typeof globalThis.fetch;

    const replayJob = await repository.createReplayJob(tenant.id, {
      baselineId: baseline.id,
      status: "pending",
      parameters: {
        executionBackend: "tokenpulse",
        candidateModel: "gpt-candidate",
        sampleLimit: 2,
        metric: "accuracy",
      },
      summary: {
        metric: "accuracy",
        totalCases: 2,
        processedCases: 0,
      },
    });

    enqueueReplayJobExecution(tenant.id, replayJob.id);
    await flushReplayJobExecutionQueueForTests();

    const finished = await repository.getReplayJobById(tenant.id, replayJob.id);
    expect(finished?.status).toBe("completed");
    expect(typeof finished?.summary?.digest).toBe("object");

    const artifacts = await repository.listReplayArtifacts(tenant.id, replayJob.id, {
      limit: 10,
    });
    const types = new Set(artifacts.map((item) => item.artifactType));
    expect(types.has("summary")).toBe(true);
    expect(types.has("diff")).toBe(true);
    expect(types.has("cases")).toBe(true);

    // 2 cases：每个 case 会请求 baseline + candidate，共 4 次
    expect(calls.length).toBe(4);
    for (const call of calls) {
      expect(call.url).toBe("https://tokenpulse.test/v1/chat/completions");
      expect(call.requestId.length).toBeGreaterThan(0);
      expect(call.tenantId).toBe(tenant.id);
      expect(call.model === "gpt-baseline" || call.model === "gpt-candidate").toBe(true);
    }
  });

  test("tokenpulse 配置缺失时仍会将失败回放的 artifacts 落盘，便于排障", async () => {
    const nonce = createNonce("replay-tokenpulse-missing-config");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });
    const baseline = await repository.createReplayBaseline(tenant.id, {
      name: `baseline-${nonce}`,
      datasetRef: `dataset-${nonce}`,
      scenarioCount: 1,
      metadata: {
        model: "gpt-baseline",
      },
    });
    await repository.replaceReplayDatasetCases(tenant.id, baseline.id, [
      {
        caseId: `case-${nonce}-1`,
        input: "请输出一句话即可。",
      },
    ]);

    const localRoot = `./data/replay-artifacts-${nonce}`;
    cleanups.push(setEnv("REPLAY_STORAGE_LOCAL_ROOT", localRoot));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_BASE_URL", ""));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_API_SECRET", ""));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_FAIL_FAST_THRESHOLD", "0"));

    globalThis.fetch = (async (_url: string | URL, _init?: RequestInit) => {
      throw new Error("不应触发真实 fetch：配置缺失应在本地失败");
    }) as unknown as typeof globalThis.fetch;

    const replayJob = await repository.createReplayJob(tenant.id, {
      baselineId: baseline.id,
      status: "pending",
      parameters: {
        executionBackend: "tokenpulse",
        candidateModel: "gpt-candidate",
        sampleLimit: 1,
      },
      summary: {
        metric: "accuracy",
        totalCases: 1,
        processedCases: 0,
      },
    });

    enqueueReplayJobExecution(tenant.id, replayJob.id);
    await flushReplayJobExecutionQueueForTests();

    const finished = await repository.getReplayJobById(tenant.id, replayJob.id);
    expect(finished?.status).toBe("failed");
    expect((finished?.error || "").includes("tokenpulse")).toBe(true);

    const artifacts = await repository.listReplayArtifacts(tenant.id, replayJob.id, {
      limit: 10,
    });
    const types = new Set(artifacts.map((item) => item.artifactType));
    expect(types.has("summary")).toBe(true);
    expect(types.has("diff")).toBe(true);
    expect(types.has("cases")).toBe(true);

    const casesArtifact = await repository.getReplayArtifactByType(tenant.id, replayJob.id, "cases");
    const localPath = (casesArtifact?.metadata?.localPath as string | undefined) || "";
    expect(localPath.length).toBeGreaterThan(0);
    const file = Bun.file(localPath);
    expect(await file.exists()).toBe(true);
    const payload = (await file.json()) as Record<string, unknown>;
    expect(Array.isArray(payload.items)).toBe(true);
    const firstCase = (payload.items as any[])[0] as Record<string, unknown>;
    expect(typeof firstCase?.tokenpulse).toBe("object");
  });

  test("tokenpulse fail-fast 触发后会中止后续样本，并将失败 artifacts 持久化", async () => {
    const nonce = createNonce("replay-tokenpulse-failfast");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });
    const baseline = await repository.createReplayBaseline(tenant.id, {
      name: `baseline-${nonce}`,
      datasetRef: `dataset-${nonce}`,
      scenarioCount: 2,
      metadata: {
        model: "gpt-baseline",
      },
    });
    await repository.replaceReplayDatasetCases(tenant.id, baseline.id, [
      {
        caseId: `case-${nonce}-1`,
        input: "输出一个数字 1。",
      },
      {
        caseId: `case-${nonce}-2`,
        input: "输出一个数字 2。",
      },
    ]);

    const localRoot = `./data/replay-artifacts-${nonce}`;
    cleanups.push(setEnv("REPLAY_STORAGE_LOCAL_ROOT", localRoot));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_BASE_URL", "https://tokenpulse.test"));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_API_SECRET", "secret"));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_FAIL_FAST_THRESHOLD", "1"));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_MAX_CONCURRENCY", "1"));
    cleanups.push(setEnv("REPLAY_TOKENPULSE_TIMEOUT_MS", "3000"));

    let candidateFailures = 0;
    globalThis.fetch = (async (url: string | URL, init?: RequestInit) => {
      const headers = new Headers(init?.headers);
      const requestId = headers.get("x-request-id") || "";
      const bodyText = typeof init?.body === "string" ? init.body : "";
      const bodyJson = bodyText ? (JSON.parse(bodyText) as Record<string, unknown>) : {};
      const model = typeof bodyJson.model === "string" ? bodyJson.model : "";

      if (model === "gpt-candidate") {
        candidateFailures += 1;
        return new Response(JSON.stringify({ message: "provider_failed" }), {
          status: 502,
          headers: {
            "content-type": "application/json",
            "x-request-id": requestId,
            "x-tokenpulse-provider": "openai",
            "x-tokenpulse-route-policy": "round_robin",
            "x-tokenpulse-fallback": "none",
          },
        });
      }

      return new Response(
        JSON.stringify({
          choices: [
            {
              message: { content: `ok:${model}:${requestId}` },
            },
          ],
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json",
            "x-request-id": requestId,
            "x-tokenpulse-provider": "openai",
            "x-tokenpulse-route-policy": "round_robin",
            "x-tokenpulse-fallback": "none",
          },
        },
      );
    }) as unknown as typeof globalThis.fetch;

    const replayJob = await repository.createReplayJob(tenant.id, {
      baselineId: baseline.id,
      status: "pending",
      parameters: {
        executionBackend: "tokenpulse",
        candidateModel: "gpt-candidate",
        sampleLimit: 2,
      },
      summary: {
        metric: "accuracy",
        totalCases: 2,
        processedCases: 0,
      },
    });

    enqueueReplayJobExecution(tenant.id, replayJob.id);
    await flushReplayJobExecutionQueueForTests();

    const finished = await repository.getReplayJobById(tenant.id, replayJob.id);
    expect(finished?.status).toBe("failed");
    expect((finished?.error || "").includes("fail-fast")).toBe(true);
    expect(candidateFailures).toBe(1);

    const artifacts = await repository.listReplayArtifacts(tenant.id, replayJob.id, {
      limit: 10,
    });
    const types = new Set(artifacts.map((item) => item.artifactType));
    expect(types.has("summary")).toBe(true);
    expect(types.has("diff")).toBe(true);
    expect(types.has("cases")).toBe(true);
  });
});
