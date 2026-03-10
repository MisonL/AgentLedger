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

  test("replay dataset version snapshot 支持 create 与 promote 恢复 working copy", async () => {
    const nonce = createNonce("repo-replay-version-snapshot");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });

    const dataset = await repository.createReplayDataset(tenant.id, {
      name: `dataset-${nonce}`,
      model: "gpt-4.1",
      externalDatasetId: `external-${nonce}`,
    });

    await repository.replaceReplayDatasetCases(tenant.id, dataset.id, [
      {
        caseId: "case-v1",
        input: "请总结退款流程",
        expectedOutput: "总结退款流程",
      },
    ]);

    const createdVersion = await repository.createReplayBaselineVersion(
      tenant.id,
      dataset.id,
      {
        datasetRef: `snapshot-${nonce}`,
        note: "snapshot-before-edit",
      },
    );
    expect(createdVersion.version).toBe(2);

    const versionCases = await repository.listReplayDatasetVersionCases(
      tenant.id,
      dataset.id,
      createdVersion.id,
      {
        limit: 10,
      },
    );
    expect(versionCases).toHaveLength(1);
    expect(versionCases[0]?.caseId).toBe("case-v1");
    expect(versionCases[0]?.input).toBe("请总结退款流程");

    await repository.replaceReplayDatasetCases(tenant.id, dataset.id, [
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
    ]);

    const updatedCases = await repository.listReplayDatasetCases(tenant.id, dataset.id, {
      limit: 10,
    });
    expect(updatedCases).toHaveLength(2);
    expect(updatedCases[0]?.input).toBe("请总结退款流程（已改写）");

    const preservedVersionCases = await repository.listReplayDatasetVersionCases(
      tenant.id,
      dataset.id,
      createdVersion.id,
      {
        limit: 10,
      },
    );
    expect(preservedVersionCases).toHaveLength(1);
    expect(preservedVersionCases[0]?.input).toBe("请总结退款流程");

    const promotedVersion = await repository.promoteReplayBaselineVersion(
      tenant.id,
      dataset.id,
      createdVersion.id,
    );
    expect(promotedVersion?.id).toBe(createdVersion.id);
    expect(promotedVersion?.promotedAt).toEqual(expect.any(String));

    const restoredCases = await repository.listReplayDatasetCases(tenant.id, dataset.id, {
      limit: 10,
    });
    expect(restoredCases).toHaveLength(1);
    expect(restoredCases[0]?.caseId).toBe("case-v1");
    expect(restoredCases[0]?.input).toBe("请总结退款流程");
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

describe("Control Plane Repository - quality advice executions and replay experiments", () => {
  test("quality advice execution 支持 upsert/list/get latest", async () => {
    const nonce = createNonce("repo-quality-advice");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });

    const created = await repository.upsertQualityAdviceExecution(tenant.id, {
      id: `quality-advice-exec-${nonce}`,
      adviceId: `advice-${nonce}`,
      project: `repo-${nonce}`,
      severity: "warn",
      actionType: "scorecard_adjustment",
      triggerSource: "manual",
      status: "running",
      metric: "accuracy",
      candidateLabels: ["candidate-a", "candidate-a", "candidate-b"],
      requestedAt: "2026-03-09T08:00:00.000Z",
      startedAt: "2026-03-09T08:00:00.000Z",
      updatedAt: "2026-03-09T08:00:00.000Z",
    });
    expect(created.candidateLabels).toEqual(["candidate-a", "candidate-b"]);
    expect(created.status).toBe("running");

    const completed = await repository.upsertQualityAdviceExecution(tenant.id, {
      ...created,
      status: "completed",
      scorecardKey: "accuracy",
      resultSummary: {
        approvalRequestId: `approval-${nonce}`,
        scorecardKey: "accuracy",
        targetScore: 82,
      },
      finishedAt: "2026-03-09T08:05:00.000Z",
      updatedAt: "2026-03-09T08:05:00.000Z",
    });
    expect(completed.status).toBe("completed");
    expect(completed.resultSummary?.["targetScore"]).toBe(82);

    const listed = await repository.listQualityAdviceExecutions(tenant.id, {
      adviceId: created.adviceId,
      limit: 10,
    });
    expect(listed).toHaveLength(1);
    expect(listed[0]?.id).toBe(created.id);
    expect(listed[0]?.status).toBe("completed");

    const fetched = await repository.getQualityAdviceExecutionById(tenant.id, created.id);
    expect(fetched?.scorecardKey).toBe("accuracy");

    const latest = await repository.getLatestQualityAdviceExecution(
      tenant.id,
      created.adviceId,
    );
    expect(latest?.id).toBe(created.id);
    expect(latest?.status).toBe("completed");

    const byApprovalRequestId =
      await repository.getQualityAdviceExecutionByApprovalRequestId(
        tenant.id,
        `approval-${nonce}`,
      );
    expect(byApprovalRequestId?.id).toBe(created.id);
    expect(byApprovalRequestId?.resultSummary?.["targetScore"]).toBe(82);
  });

  test("replay experiment 支持持久化 runIds 与状态更新", async () => {
    const nonce = createNonce("repo-replay-experiment");
    const tenant = await repository.createTenant({
      id: `tenant-${nonce}`,
      name: `租户-${nonce}`,
    });

    const dataset = await repository.createReplayDataset(tenant.id, {
      name: `dataset-${nonce}`,
      model: "gpt-4.1",
      externalDatasetId: `dataset-ref-${nonce}`,
    });
    const runA = await repository.createReplayRun(tenant.id, {
      datasetId: dataset.id,
      status: "completed",
      parameters: {
        candidateLabel: "candidate-a",
      },
    });
    const runB = await repository.createReplayRun(tenant.id, {
      datasetId: dataset.id,
      status: "pending",
      parameters: {
        candidateLabel: "candidate-b",
      },
    });

    const created = await repository.upsertReplayExperiment(tenant.id, {
      id: `replay-experiment-${nonce}`,
      name: `experiment-${nonce}`,
      datasetId: dataset.id,
      baselineId: dataset.id,
      triggerSource: "manual",
      executionMode: "manual",
      status: "queued",
      candidateLabels: ["candidate-a", "candidate-b"],
      runIds: [runA.id],
      createdAt: "2026-03-09T09:00:00.000Z",
      updatedAt: "2026-03-09T09:00:00.000Z",
    });
    expect(created.runIds).toEqual([runA.id]);

    const updated = await repository.upsertReplayExperiment(tenant.id, {
      ...created,
      status: "running",
      executionMode: "automatic",
      runIds: [runA.id, runB.id],
      startedAt: "2026-03-09T09:02:00.000Z",
      updatedAt: "2026-03-09T09:02:00.000Z",
    });
    expect(updated.runIds).toEqual([runA.id, runB.id]);
    expect(updated.status).toBe("running");

    const listed = await repository.listReplayExperiments(tenant.id, {
      datasetId: dataset.id,
      limit: 10,
    });
    expect(listed.some((item) => item.id === created.id)).toBe(true);

    const fetched = await repository.getReplayExperimentById(tenant.id, created.id);
    expect(fetched?.runIds).toEqual([runA.id, runB.id]);
    expect(fetched?.executionMode).toBe("automatic");
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

describe("Control Plane Repository - MCP approval workflow persistence", () => {
  test("createMcpApprovalRequest 在 DB 路径下会同事务写入审批请求与工作流", async () => {
    const repo = repository as unknown as {
      pool: {
        query: (
          text: string,
          values?: readonly unknown[],
        ) => Promise<{ rows: Record<string, unknown>[] }>;
        connect: () => Promise<{
          query: (
            text: string,
            values?: readonly unknown[],
          ) => Promise<{ rows: Record<string, unknown>[] }>;
          release: () => void;
        }>;
        on: (event: "error", listener: (error: unknown) => void) => void;
      } | null;
      initPromise: Promise<void> | null;
      loggedDbFallback: boolean;
      memoryMcpApprovalWorkflows: Map<string, unknown>;
    };
    const originalPool = repo.pool;
    const originalInitPromise = repo.initPromise;
    const originalLoggedDbFallback = repo.loggedDbFallback;
    const queryLog: Array<{ source: "pool" | "client"; text: string }> = [];
    let released = false;
    const nonce = createNonce("repo-mcp-create-tx");
    const tenantId = `tenant-${nonce}`;
    const approvalId = `approval-${nonce}`;
    const createdAt = "2026-03-07T00:00:00.000Z";

    repo.pool = {
      async query(text) {
        queryLog.push({ source: "pool", text });
        throw new Error(`unexpected pool query: ${text}`);
      },
      async connect() {
        return {
          async query(text) {
            queryLog.push({ source: "client", text });
            if (text === "BEGIN" || text === "COMMIT" || text === "ROLLBACK") {
              return { rows: [] };
            }
            if (text.includes("INSERT INTO mcp_approval_requests")) {
              return {
                rows: [
                  {
                    id: approvalId,
                    tenant_id: tenantId,
                    tool_id: "tool-tx-create",
                    status: "pending",
                    requested_by_user_id: "user-tx-create",
                    requested_by_email: "user-tx-create@example.com",
                    reason: "需要三阶段审批",
                    reviewed_by_user_id: null,
                    reviewed_by_email: null,
                    review_reason: null,
                    created_at: createdAt,
                    updated_at: createdAt,
                  },
                ],
              };
            }
            if (text.includes("INSERT INTO mcp_approval_workflows")) {
              return { rows: [] };
            }
            throw new Error(`unexpected client query: ${text}`);
          },
          release() {
            released = true;
          },
        };
      },
      on() {
        // noop
      },
    };
    repo.initPromise = Promise.resolve();
    repo.loggedDbFallback = false;

    try {
      const created = await repository.createMcpApprovalRequest(
        tenantId,
        {
          toolId: "tool-tx-create",
          reason: "需要三阶段审批",
        },
        {
          requestedByUserId: "user-tx-create",
          requestedByEmail: "user-tx-create@example.com",
          approvalConfig: {
            mode: "multi_stage",
            approvalStages: [
              {
                nodeId: "stage1",
                label: "Stage 1",
                stage: "stage1",
                requiredApprovals: 1,
                roles: ["maintainer"],
              },
              {
                nodeId: "stage2",
                label: "Stage 2",
                stage: "stage2",
                requiredApprovals: 1,
                roles: ["owner"],
              },
              {
                nodeId: "stage3",
                label: "Stage 3",
                stage: "stage3",
                requiredApprovals: 1,
                roles: ["owner"],
              },
            ],
          },
          approvalConditionMatched: true,
        },
      );

      expect(created.approval.id).toBe(approvalId);
      expect(created.workflow.approvalMode).toBe("multi_stage");
      expect(created.workflow.currentStage).toBe("stage1");
      expect(created.workflow.approvalStages.map((stage) => stage.stage)).toEqual([
        "stage1",
        "stage2",
        "stage3",
      ]);
      expect(
        created.workflow.approvalStages.find((stage) => stage.stage === "stage3")
          ?.roles,
      ).toEqual(["owner"]);
      expect(queryLog.filter((entry) => entry.source === "pool")).toHaveLength(0);
      expect(queryLog.map((entry) => entry.text)).toEqual([
        "BEGIN",
        expect.stringContaining("INSERT INTO mcp_approval_requests"),
        expect.stringContaining("INSERT INTO mcp_approval_workflows"),
        "COMMIT",
      ]);
      expect(released).toBe(true);
    } finally {
      repo.pool = originalPool;
      repo.initPromise = originalInitPromise;
      repo.loggedDbFallback = originalLoggedDbFallback;
      repo.memoryMcpApprovalWorkflows.delete(`${tenantId}:${approvalId}`);
    }
  });

  test("reviewMcpApprovalRequest 在 DB 路径下会同事务更新审批请求与工作流", async () => {
    const repo = repository as unknown as {
      pool: {
        query: (
          text: string,
          values?: readonly unknown[],
        ) => Promise<{ rows: Record<string, unknown>[] }>;
        connect: () => Promise<{
          query: (
            text: string,
            values?: readonly unknown[],
          ) => Promise<{ rows: Record<string, unknown>[] }>;
          release: () => void;
        }>;
        on: (event: "error", listener: (error: unknown) => void) => void;
      } | null;
      initPromise: Promise<void> | null;
      loggedDbFallback: boolean;
      memoryMcpApprovalWorkflows: Map<string, unknown>;
    };
    const originalPool = repo.pool;
    const originalInitPromise = repo.initPromise;
    const originalLoggedDbFallback = repo.loggedDbFallback;
    const queryLog: Array<{ source: "pool" | "client"; text: string }> = [];
    let released = false;
    const nonce = createNonce("repo-mcp-review-tx");
    const tenantId = `tenant-${nonce}`;
    const approvalId = `approval-${nonce}`;
    const createdAt = "2026-03-07T01:00:00.000Z";

    repo.memoryMcpApprovalWorkflows.set(`${tenantId}:${approvalId}`, {
      approvalRequestId: approvalId,
      tenantId,
      approvalMode: "multi_stage",
      approvalConditionMatched: true,
      approvalStages: [
        {
          stage: "stage1",
          requiredApprovals: 1,
          roles: ["maintainer"],
          approvals: [],
        },
        {
          stage: "stage2",
          requiredApprovals: 1,
          roles: ["owner"],
          approvals: [],
        },
        {
          stage: "stage3",
          requiredApprovals: 1,
          roles: ["owner"],
          approvals: [],
        },
      ],
    });

    repo.pool = {
      async query(text) {
        queryLog.push({ source: "pool", text });
        if (text.includes("SELECT id,")) {
          return {
            rows: [
              {
                id: approvalId,
                tenant_id: tenantId,
                tool_id: "tool-tx-review",
                status: "pending",
                requested_by_user_id: "user-requester",
                requested_by_email: "user-requester@example.com",
                reason: "审批 stage1",
                reviewed_by_user_id: null,
                reviewed_by_email: null,
                review_reason: null,
                created_at: createdAt,
                updated_at: createdAt,
              },
            ],
          };
        }
        throw new Error(`unexpected pool query: ${text}`);
      },
      async connect() {
        return {
          async query(text) {
            queryLog.push({ source: "client", text });
            if (text === "BEGIN" || text === "COMMIT" || text === "ROLLBACK") {
              return { rows: [] };
            }
            if (text.includes("UPDATE mcp_approval_requests")) {
              return {
                rows: [
                  {
                    id: approvalId,
                    tenant_id: tenantId,
                    tool_id: "tool-tx-review",
                    status: "pending",
                    requested_by_user_id: "user-requester",
                    requested_by_email: "user-requester@example.com",
                    reason: "审批 stage1",
                    reviewed_by_user_id: "user-stage1",
                    reviewed_by_email: "user-stage1@example.com",
                    review_reason: "stage1 通过",
                    created_at: createdAt,
                    updated_at: "2026-03-07T01:05:00.000Z",
                  },
                ],
              };
            }
            if (text.includes("INSERT INTO mcp_approval_workflows")) {
              return { rows: [] };
            }
            throw new Error(`unexpected client query: ${text}`);
          },
          release() {
            released = true;
          },
        };
      },
      on() {
        // noop
      },
    };
    repo.initPromise = Promise.resolve();
    repo.loggedDbFallback = false;

    try {
      const reviewed = await repository.reviewMcpApprovalRequest(
        tenantId,
        approvalId,
        "approved",
        {
          reason: "stage1 通过",
        },
        {
          reviewedByUserId: "user-stage1",
          reviewedByEmail: "user-stage1@example.com",
          reviewedByTenantRole: "maintainer",
          stage: "stage1",
        },
      );

      expect(reviewed).not.toBeNull();
      expect(reviewed?.approval.status).toBe("pending");
      expect(reviewed?.workflow.currentStage).toBe("stage2");
      expect(reviewed?.workflow.remainingApprovals).toBe(1);
      expect(
        reviewed?.workflow.approvalStages.find((stage) => stage.stage === "stage1")
          ?.approvedByUserIds,
      ).toEqual(["user-stage1"]);
      expect(
        queryLog.filter((entry) => entry.source === "pool").map((entry) => entry.text),
      ).toEqual([expect.stringContaining("SELECT id,")]);
      expect(
        queryLog.filter((entry) => entry.source === "client").map((entry) => entry.text),
      ).toEqual([
        "BEGIN",
        expect.stringContaining("UPDATE mcp_approval_requests"),
        expect.stringContaining("INSERT INTO mcp_approval_workflows"),
        "COMMIT",
      ]);
      expect(released).toBe(true);
    } finally {
      repo.pool = originalPool;
      repo.initPromise = originalInitPromise;
      repo.loggedDbFallback = originalLoggedDbFallback;
      repo.memoryMcpApprovalWorkflows.delete(`${tenantId}:${approvalId}`);
    }
  });

  test("getMcpApprovalWorkflowState 在 DB 路径下可恢复 approvalStages 数组", async () => {
    const repo = repository as unknown as {
      pool: {
        query: (
          text: string,
          values?: readonly unknown[],
        ) => Promise<{ rows: Record<string, unknown>[] }>;
        connect: () => Promise<{
          query: (
            text: string,
            values?: readonly unknown[],
          ) => Promise<{ rows: Record<string, unknown>[] }>;
          release: () => void;
        }>;
        on: (event: "error", listener: (error: unknown) => void) => void;
      } | null;
      initPromise: Promise<void> | null;
      loggedDbFallback: boolean;
      memoryMcpApprovalWorkflows: Map<string, unknown>;
    };
    const originalPool = repo.pool;
    const originalInitPromise = repo.initPromise;
    const originalLoggedDbFallback = repo.loggedDbFallback;
    const nonce = createNonce("repo-mcp-restore");
    const tenantId = `tenant-${nonce}`;
    const approvalId = `approval-${nonce}`;
    const createdAt = "2026-03-07T02:00:00.000Z";

    repo.pool = {
      async query(text) {
        if (text.includes("FROM mcp_approval_requests")) {
          return {
            rows: [
              {
                id: approvalId,
                tenant_id: tenantId,
                tool_id: "tool-tx-restore",
                status: "pending",
                requested_by_user_id: "user-requester",
                requested_by_email: "user-requester@example.com",
                reason: "恢复多阶段审批",
                reviewed_by_user_id: null,
                reviewed_by_email: null,
                review_reason: null,
                created_at: createdAt,
                updated_at: createdAt,
              },
            ],
          };
        }
        if (text.includes("FROM mcp_approval_workflows")) {
          return {
            rows: [
              {
                tenant_id: tenantId,
                approval_request_id: approvalId,
                approval_mode: "multi_stage",
                approval_condition_matched: true,
                approval_stages: [
                  {
                    stage: "stage1",
                    requiredApprovals: 1,
                    roles: ["maintainer"],
                    approvals: [
                      {
                        userId: "user-stage1",
                        email: "user-stage1@example.com",
                        reviewedAt: "2026-03-07T02:03:00.000Z",
                      },
                    ],
                  },
                  {
                    stage: "stage2",
                    requiredApprovals: 1,
                    roles: ["owner"],
                    approvals: [],
                  },
                  {
                    stage: "stage3",
                    requiredApprovals: 1,
                    roles: ["owner"],
                    approvals: [],
                  },
                ],
                stage1_required_approvals: 1,
                stage1_roles: ["maintainer"],
                stage1_approvals: [],
                stage1_rejected_by: null,
                stage2_required_approvals: 1,
                stage2_roles: ["owner"],
                stage2_approvals: [],
                stage2_rejected_by: null,
              },
            ],
          };
        }
        throw new Error(`unexpected pool query: ${text}`);
      },
      async connect() {
        throw new Error("unexpected connect for workflow restore");
      },
      on() {
        // noop
      },
    };
    repo.initPromise = Promise.resolve();
    repo.loggedDbFallback = false;

    try {
      const workflow = await repository.getMcpApprovalWorkflowState(
        tenantId,
        approvalId,
      );

      expect(workflow).not.toBeNull();
      expect(workflow?.approvalMode).toBe("multi_stage");
      expect(workflow?.currentStage).toBe("stage2");
      expect(workflow?.remainingApprovals).toBe(1);
      expect(workflow?.approvalStages.map((stage) => stage.stage)).toEqual([
        "stage1",
        "stage2",
        "stage3",
      ]);
      expect(
        workflow?.approvalStages.find((stage) => stage.stage === "stage1")
          ?.approvedByUserIds,
      ).toEqual(["user-stage1"]);
    } finally {
      repo.pool = originalPool;
      repo.initPromise = originalInitPromise;
      repo.loggedDbFallback = originalLoggedDbFallback;
      repo.memoryMcpApprovalWorkflows.delete(`${tenantId}:${approvalId}`);
    }
  });
});
