import { describe, expect, test } from "bun:test";
import { getControlPlaneRepository } from "../src/data/repository";

const repository = getControlPlaneRepository();

function createNonce(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

describe("Control Plane Repository - budget binding validation", () => {
  test("scope=org: organizationId 需要存在且属于当前 tenant", async () => {
    const nonce = createNonce("repo-budget-org");
    const tenantA = await repository.createTenant({
      id: `tenant-${nonce}-a`,
      name: `租户A-${nonce}`,
    });
    const tenantB = await repository.createTenant({
      id: `tenant-${nonce}-b`,
      name: `租户B-${nonce}`,
    });

    const orgA = await repository.createOrganization(tenantA.id, {
      name: `组织A-${nonce}`,
    });
    const orgB = await repository.createOrganization(tenantB.id, {
      name: `组织B-${nonce}`,
    });

    const valid = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "org",
      organizationId: orgA.id,
    });
    expect(valid).toBeNull();

    const crossTenant = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "org",
      organizationId: orgB.id,
    });
    expect(crossTenant?.field).toBe("organizationId");

    const missing = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "org",
      organizationId: `missing-org-${nonce}`,
    });
    expect(missing?.field).toBe("organizationId");
  });

  test("scope=user: userId 需要存在且属于当前 tenant", async () => {
    const nonce = createNonce("repo-budget-user");
    const tenantA = await repository.createTenant({
      id: `tenant-${nonce}-a`,
      name: `租户A-${nonce}`,
    });
    const tenantB = await repository.createTenant({
      id: `tenant-${nonce}-b`,
      name: `租户B-${nonce}`,
    });
    const member = await repository.createLocalUser({
      email: `member-${nonce}@example.com`,
      passwordHash: "hashed-password",
      displayName: `成员-${nonce}`,
    });
    const outsider = await repository.createLocalUser({
      email: `outsider-${nonce}@example.com`,
      passwordHash: "hashed-password",
      displayName: `外部成员-${nonce}`,
    });

    await repository.addTenantMember({
      tenantId: tenantA.id,
      userId: member.id,
      tenantRole: "member",
    });
    await repository.addTenantMember({
      tenantId: tenantB.id,
      userId: outsider.id,
      tenantRole: "member",
    });

    const valid = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "user",
      userId: member.id,
    });
    expect(valid).toBeNull();

    const crossTenant = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "user",
      userId: outsider.id,
    });
    expect(crossTenant?.field).toBe("userId");

    const missing = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "user",
      userId: `missing-user-${nonce}`,
    });
    expect(missing?.field).toBe("userId");
  });
});

describe("Control Plane Repository - replay job state machine", () => {
  test("replay job 成功流转：pending -> running -> completed", async () => {
    const nonce = createNonce("repo-replay-success");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });
    const baseline = await repository.createReplayBaseline(tenant.id, {
      name: `baseline-${nonce}`,
      datasetRef: `dataset-${nonce}`,
      scenarioCount: 5,
      metadata: {
        model: "gpt-4.1",
      },
    });
    const replayJob = await repository.createReplayJob(tenant.id, {
      baselineId: baseline.id,
      status: "pending",
      parameters: {
        candidateLabel: "candidate-success",
        sampleLimit: 5,
      },
      summary: {
        metric: "accuracy",
        totalCases: 5,
        processedCases: 0,
      },
    });
    expect(replayJob.status).toBe("pending");

    const running = await repository.updateReplayJob(tenant.id, replayJob.id, {
      fromStatuses: ["pending"],
      status: "running",
      startedAt: new Date().toISOString(),
      error: null,
    });
    expect(running?.status).toBe("running");
    expect(typeof running?.startedAt).toBe("string");

    const invalidTransition = await repository.updateReplayJob(tenant.id, replayJob.id, {
      fromStatuses: ["pending"],
      status: "completed",
    });
    expect(invalidTransition).toBeNull();

    const completed = await repository.updateReplayJob(tenant.id, replayJob.id, {
      fromStatuses: ["running"],
      status: "completed",
      summary: {
        metric: "accuracy",
        totalCases: 5,
        processedCases: 5,
        improvedCases: 1,
        regressedCases: 1,
        unchangedCases: 3,
      },
      diff: {
        items: [
          {
            caseId: "case-1",
            metric: "accuracy",
            baselineScore: 80,
            candidateScore: 88,
            delta: 8,
            verdict: "improved",
          },
        ],
      },
      finishedAt: new Date().toISOString(),
      error: null,
    });
    expect(completed?.status).toBe("completed");
    expect(typeof completed?.finishedAt).toBe("string");

    const fetched = await repository.getReplayJobById(tenant.id, replayJob.id);
    expect(fetched?.status).toBe("completed");
    expect((fetched?.summary.totalCases as number) ?? 0).toBe(5);
    expect(Array.isArray((fetched?.diff.items as unknown[]) ?? [])).toBe(true);
  });

  test("replay job 失败与取消流转：running -> failed 与 pending -> cancelled", async () => {
    const nonce = createNonce("repo-replay-failed");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });
    const baseline = await repository.createReplayBaseline(tenant.id, {
      name: `baseline-${nonce}`,
      datasetRef: `dataset-${nonce}`,
      scenarioCount: 3,
      metadata: {
        model: "gpt-4.1",
      },
    });

    const failedJob = await repository.createReplayJob(tenant.id, {
      baselineId: baseline.id,
      status: "pending",
      parameters: {
        candidateLabel: "candidate-failed",
      },
    });
    const running = await repository.updateReplayJob(tenant.id, failedJob.id, {
      fromStatuses: ["pending"],
      status: "running",
      startedAt: new Date().toISOString(),
    });
    expect(running?.status).toBe("running");

    const failed = await repository.updateReplayJob(tenant.id, failedJob.id, {
      fromStatuses: ["running"],
      status: "failed",
      error: "mock worker failed",
      finishedAt: new Date().toISOString(),
    });
    expect(failed?.status).toBe("failed");
    expect(failed?.error).toContain("mock worker failed");

    const noRetryFromRunning = await repository.updateReplayJob(tenant.id, failedJob.id, {
      fromStatuses: ["running"],
      status: "completed",
    });
    expect(noRetryFromRunning).toBeNull();

    const cancelledJob = await repository.createReplayJob(tenant.id, {
      baselineId: baseline.id,
      status: "pending",
      parameters: {
        candidateLabel: "candidate-cancelled",
      },
    });
    const cancelled = await repository.updateReplayJob(tenant.id, cancelledJob.id, {
      fromStatuses: ["pending"],
      status: "cancelled",
      finishedAt: new Date().toISOString(),
      error: null,
    });
    expect(cancelled?.status).toBe("cancelled");

    const failedItems = await repository.listReplayJobs(tenant.id, {
      status: "failed",
      limit: 20,
    });
    expect(failedItems.some((item) => item.id === failedJob.id)).toBe(true);

    const cancelledItems = await repository.listReplayJobs(tenant.id, {
      status: "cancelled",
      limit: 20,
    });
    expect(cancelledItems.some((item) => item.id === cancelledJob.id)).toBe(true);
  });

  test("replay canonical 模型支持 dataset cases 与 artifact 元数据", async () => {
    const nonce = createNonce("repo-replay-canonical");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });

    const dataset = await repository.createReplayDataset(tenant.id, {
      name: `dataset-${nonce}`,
      model: "gpt-4.1",
      externalDatasetId: `external-${nonce}`,
      metadata: {
        domain: "support",
      },
    });
    expect(dataset.model).toBe("gpt-4.1");

    const cases = await repository.replaceReplayDatasetCases(tenant.id, dataset.id, [
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
    ]);
    expect(cases).toHaveLength(2);

    const listedCases = await repository.listReplayDatasetCases(tenant.id, dataset.id, {
      limit: 10,
    });
    expect(listedCases).toHaveLength(2);
    expect(listedCases[0]?.caseId).toBe("case-1");

    const replayRun = await repository.createReplayRun(tenant.id, {
      datasetId: dataset.id,
      status: "pending",
      parameters: {
        candidateLabel: "candidate-canonical",
      },
      summary: {
        totalCases: 2,
        processedCases: 0,
      },
    });
    expect(replayRun.datasetId).toBe(dataset.id);

    const artifacts = await repository.upsertReplayArtifacts(tenant.id, replayRun.id, [
      {
        artifactType: "summary",
        name: "summary.json",
        description: "summary artifact",
        contentType: "application/json",
        byteSize: 128,
        checksum: "checksum-summary",
        storageBackend: "local",
        storageKey: `/tmp/${nonce}/summary.json`,
      },
      {
        artifactType: "diff",
        name: "diff.json",
        description: "diff artifact",
        contentType: "application/json",
        byteSize: 256,
        checksum: "checksum-diff",
        storageBackend: "hybrid",
        storageKey: `/tmp/${nonce}/diff.json`,
        metadata: {
          objectKey: `replay/${nonce}/diff.json`,
        },
      },
    ]);
    expect(artifacts.some((item) => item.artifactType === "summary")).toBe(true);
    expect(artifacts.some((item) => item.artifactType === "diff")).toBe(true);

    const listedArtifacts = await repository.listReplayArtifacts(tenant.id, replayRun.id, {
      limit: 10,
    });
    expect(listedArtifacts).toHaveLength(2);
    expect(
      listedArtifacts.some(
        (item) => item.artifactType === "diff" && item.storageBackend === "hybrid",
      ),
    ).toBe(true);

    const diffArtifact = await repository.getReplayArtifactByType(tenant.id, replayRun.id, "diff");
    expect(diffArtifact?.metadata["objectKey"]).toBe(`replay/${nonce}/diff.json`);
  });
});

describe("Control Plane Repository - webhook endpoint secrets", () => {
  test("secretCiphertext 支持创建、更新与清空", async () => {
    const nonce = createNonce("repo-webhook-secret");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });

    const created = await repository.createWebhookEndpoint(tenant.id, {
      name: `webhook-${nonce}`,
      url: "https://example.com/webhook",
      eventTypes: ["api_key.created"],
      secretHash: `hash-${nonce}-v1`,
      secretCiphertext: `cipher-${nonce}-v1`,
    });
    expect(created.secretHash).toBe(`hash-${nonce}-v1`);
    expect(created.secretCiphertext).toBe(`cipher-${nonce}-v1`);

    const updated = await repository.updateWebhookEndpoint(tenant.id, created.id, {
      secretHash: `hash-${nonce}-v2`,
      secretCiphertext: `cipher-${nonce}-v2`,
    });
    expect(updated?.secretHash).toBe(`hash-${nonce}-v2`);
    expect(updated?.secretCiphertext).toBe(`cipher-${nonce}-v2`);

    const cleared = await repository.updateWebhookEndpoint(tenant.id, created.id, {
      secretHash: null,
      secretCiphertext: null,
    });
    expect(cleared?.secretHash).toBeUndefined();
    expect(cleared?.secretCiphertext).toBeUndefined();
  });
});

describe("Control Plane Repository - webhook replay task state machine", () => {
  test("webhook replay task 成功流转与分页查询", async () => {
    const nonce = createNonce("repo-webhook-replay-success");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });
    const endpoint = await repository.createWebhookEndpoint(tenant.id, {
      name: `webhook-${nonce}`,
      url: "https://example.com/webhook",
      eventTypes: ["quality.event.created"],
    });

    const queuedTask = await repository.createWebhookReplayTask(tenant.id, {
      webhookId: endpoint.id,
      dryRun: true,
      filters: {
        eventType: "quality.event.created",
        limit: 20,
      },
      requestedAt: "2026-03-01T00:00:00.000Z",
    });
    expect(queuedTask.status).toBe("queued");

    const runningTask = await repository.updateWebhookReplayTask(tenant.id, queuedTask.id, {
      fromStatuses: ["queued"],
      status: "running",
      startedAt: "2026-03-01T00:01:00.000Z",
      error: null,
    });
    expect(runningTask?.status).toBe("running");

    const invalidTransition = await repository.updateWebhookReplayTask(tenant.id, queuedTask.id, {
      fromStatuses: ["queued"],
      status: "completed",
    });
    expect(invalidTransition).toBeNull();

    const completedTask = await repository.updateWebhookReplayTask(tenant.id, queuedTask.id, {
      fromStatuses: ["running"],
      status: "completed",
      result: {
        executor: "test-worker",
        matchedEvents: 10,
      },
      finishedAt: "2026-03-01T00:02:00.000Z",
      error: null,
    });
    expect(completedTask?.status).toBe("completed");
    expect(completedTask?.result["executor"]).toBe("test-worker");

    await repository.createWebhookReplayTask(tenant.id, {
      webhookId: endpoint.id,
      dryRun: false,
      filters: {
        eventType: "replay.job.completed",
        limit: 10,
      },
      requestedAt: "2026-03-02T00:00:00.000Z",
    });

    const completedItems = await repository.listWebhookReplayTasks(tenant.id, {
      status: "completed",
      limit: 20,
    });
    expect(completedItems.items.some((item) => item.id === queuedTask.id)).toBe(true);

    const firstPage = await repository.listWebhookReplayTasks(tenant.id, {
      webhookId: endpoint.id,
      limit: 1,
    });
    expect(firstPage.items.length).toBe(1);
    expect(firstPage.nextCursor).not.toBeNull();

    const secondPage = await repository.listWebhookReplayTasks(tenant.id, {
      webhookId: endpoint.id,
      limit: 1,
      cursor: firstPage.nextCursor ?? undefined,
    });
    expect(secondPage.items.length).toBe(1);
    expect(secondPage.items[0]?.id).not.toBe(firstPage.items[0]?.id);
  });

  test("webhook replay task 失败流转与租户隔离", async () => {
    const nonce = createNonce("repo-webhook-replay-failed");
    const tenantA = await repository.createTenant({
      id: `tenant-${nonce}-a`,
      name: `租户A-${nonce}`,
    });
    const tenantB = await repository.createTenant({
      id: `tenant-${nonce}-b`,
      name: `租户B-${nonce}`,
    });
    const endpoint = await repository.createWebhookEndpoint(tenantA.id, {
      name: `webhook-${nonce}`,
      url: "https://example.com/webhook",
      eventTypes: ["quality.event.created"],
    });
    const replayTask = await repository.createWebhookReplayTask(tenantA.id, {
      webhookId: endpoint.id,
      dryRun: true,
      filters: {
        limit: 30,
      },
    });

    const failed = await repository.updateWebhookReplayTask(tenantA.id, replayTask.id, {
      fromStatuses: ["queued"],
      status: "failed",
      error: "mock replay failed",
      finishedAt: new Date().toISOString(),
    });
    expect(failed?.status).toBe("failed");
    expect(failed?.error).toContain("mock replay failed");

    const crossTenantGet = await repository.getWebhookReplayTaskById(tenantB.id, replayTask.id);
    expect(crossTenantGet).toBeNull();

    const crossTenantUpdate = await repository.updateWebhookReplayTask(tenantB.id, replayTask.id, {
      status: "completed",
    });
    expect(crossTenantUpdate).toBeNull();

    const crossTenantList = await repository.listWebhookReplayTasks(tenantB.id, {
      limit: 10,
    });
    expect(crossTenantList.items.some((item) => item.id === replayTask.id)).toBe(false);
  });
});

describe("Control Plane Repository - webhook replay events", () => {
  test("按事件类型聚合 replay 数据并支持时间窗过滤", async () => {
    const nonce = createNonce("repo-webhook-replay-events");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });

    const apiKey = await repository.createApiKey(tenant.id, {
      name: `api-key-${nonce}`,
      keyHash: `hash-${nonce}`,
      scopes: ["write"],
      createdAt: "2026-03-01T00:00:00.000Z",
    });
    await repository.revokeApiKey(tenant.id, apiKey.id, "2026-03-01T00:10:00.000Z");

    await repository.createQualityEvent(tenant.id, {
      scorecardKey: `accuracy-${nonce}`,
      metricKey: "accuracy",
      score: 0.95,
      passed: true,
      metadata: {
        sessionId: `session-${nonce}`,
      },
      createdAt: "2026-03-01T00:20:00.000Z",
    });
    await repository.upsertQualityScorecard(tenant.id, {
      scorecardKey: `scorecard-${nonce}`,
      title: "质量评分卡",
      score: 0.88,
      dimensions: {
        accuracy: 0.9,
      },
      metadata: {
        source: "unit-test",
      },
      updatedAt: "2026-03-01T00:30:00.000Z",
    });

    const baseline = await repository.createReplayBaseline(tenant.id, {
      name: `baseline-${nonce}`,
      scenarioCount: 10,
      createdAt: "2026-03-01T00:40:00.000Z",
    });
    const completedReplay = await repository.createReplayJob(tenant.id, {
      baselineId: baseline.id,
      status: "running",
      startedAt: "2026-03-01T00:50:00.000Z",
      createdAt: "2026-03-01T00:49:00.000Z",
    });
    await repository.updateReplayJob(tenant.id, completedReplay.id, {
      fromStatuses: ["running"],
      status: "completed",
      finishedAt: "2026-03-01T01:00:00.000Z",
      summary: {
        totalCases: 10,
        processedCases: 10,
        regressedCases: 2,
      },
      diff: {
        items: [],
      },
      error: null,
    });

    const failedReplay = await repository.createReplayJob(tenant.id, {
      baselineId: baseline.id,
      status: "running",
      startedAt: "2026-03-01T01:10:00.000Z",
      createdAt: "2026-03-01T01:09:00.000Z",
    });
    await repository.updateReplayJob(tenant.id, failedReplay.id, {
      fromStatuses: ["running"],
      status: "failed",
      finishedAt: "2026-03-01T01:20:00.000Z",
      error: "mock failed",
    });

    const cancelledReplay = await repository.createReplayJob(tenant.id, {
      baselineId: baseline.id,
      status: "running",
      startedAt: "2026-03-01T01:25:00.000Z",
      createdAt: "2026-03-01T01:24:00.000Z",
    });
    await repository.updateReplayJob(tenant.id, cancelledReplay.id, {
      fromStatuses: ["running"],
      status: "cancelled",
      finishedAt: "2026-03-01T01:35:00.000Z",
      error: "cancelled by user",
    });

    const items = await repository.listWebhookReplayEvents(tenant.id, {
      eventTypes: [
        "api_key.created",
        "api_key.revoked",
        "quality.event.created",
        "quality.scorecard.updated",
        "replay.job.completed",
        "replay.job.failed",
        "replay.run.started",
        "replay.run.completed",
        "replay.run.regression_detected",
        "replay.run.failed",
        "replay.run.cancelled",
      ],
      limit: 20,
    });
    const eventTypes = new Set(items.map((item) => item.eventType));
    expect(eventTypes.has("api_key.created")).toBe(true);
    expect(eventTypes.has("api_key.revoked")).toBe(true);
    expect(eventTypes.has("quality.event.created")).toBe(true);
    expect(eventTypes.has("quality.scorecard.updated")).toBe(true);
    expect(eventTypes.has("replay.job.completed")).toBe(true);
    expect(eventTypes.has("replay.job.failed")).toBe(true);
    expect(eventTypes.has("replay.run.started")).toBe(true);
    expect(eventTypes.has("replay.run.completed")).toBe(true);
    expect(eventTypes.has("replay.run.regression_detected")).toBe(true);
    expect(eventTypes.has("replay.run.failed")).toBe(true);
    expect(eventTypes.has("replay.run.cancelled")).toBe(true);

    const filtered = await repository.listWebhookReplayEvents(tenant.id, {
      eventTypes: ["replay.run.failed", "replay.run.cancelled"],
      from: "2026-03-01T01:15:00.000Z",
      to: "2026-03-01T01:40:00.000Z",
      limit: 20,
    });
    expect(filtered.length).toBe(2);
    expect(filtered[0]?.eventType).toBe("replay.run.cancelled");
    expect(filtered.some((item) => item.eventType === "replay.run.failed")).toBe(true);
    expect(filtered.some((item) => item.eventType === "replay.run.cancelled")).toBe(true);

    const started = await repository.listWebhookReplayEvents(tenant.id, {
      eventTypes: ["replay.run.started"],
      from: "2026-03-01T01:20:00.000Z",
      to: "2026-03-01T01:30:00.000Z",
      limit: 20,
    });
    expect(started).toHaveLength(1);
    expect(started[0]?.eventType).toBe("replay.run.started");
  });
});
