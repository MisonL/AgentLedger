import { describe, expect, test } from "bun:test";
import {
  validateReplayDatasetCasesReplaceInput,
  validateReplayDatasetCreateInput,
  validateReplayDatasetMaterializeInput,
  validateReplayRunCreateInput,
} from "../src/contracts";

describe("Replay v2 Contracts", () => {
  test("数据集创建校验：优先使用 datasetRef，并兼容 datasetId alias", () => {
    const canonical = validateReplayDatasetCreateInput({
      tenantId: "tenant-1",
      name: "dataset-a",
      datasetRef: "dataset-ref-a",
      model: "gpt-4.1",
    });
    expect(canonical.success).toBe(true);
    if (!canonical.success) {
      return;
    }
    expect(canonical.data.datasetRef).toBe("dataset-ref-a");
    expect(canonical.data.datasetId).toBe("dataset-ref-a");

    const legacy = validateReplayDatasetCreateInput({
      tenantId: "tenant-1",
      name: "dataset-b",
      datasetId: "legacy-dataset-id",
      model: "gpt-4.1",
    });
    expect(legacy.success).toBe(true);
    if (!legacy.success) {
      return;
    }
    expect(legacy.data.datasetRef).toBe("legacy-dataset-id");
    expect(legacy.data.datasetId).toBe("legacy-dataset-id");
  });

  test("运行创建校验：公开语义使用 datasetId，并保留 baselineId alias", () => {
    const canonical = validateReplayRunCreateInput({
      tenantId: "tenant-1",
      datasetId: "dataset-1",
      candidateLabel: "candidate-a",
      sampleLimit: 20,
    });
    expect(canonical.success).toBe(true);
    if (!canonical.success) {
      return;
    }
    expect(canonical.data.datasetId).toBe("dataset-1");
    expect(canonical.data.baselineId).toBe("dataset-1");

    const legacy = validateReplayRunCreateInput({
      tenantId: "tenant-1",
      baselineId: "dataset-legacy",
      candidateLabel: "candidate-b",
    });
    expect(legacy.success).toBe(true);
    if (!legacy.success) {
      return;
    }
    expect(legacy.data.datasetId).toBe("dataset-legacy");
    expect(legacy.data.baselineId).toBe("dataset-legacy");
  });

  test("数据集样本替换校验：返回规范化的 cases 写入结构", () => {
    const validation = validateReplayDatasetCasesReplaceInput({
      tenantId: "tenant-1",
      datasetId: "dataset-1",
      items: [
        {
          caseId: "case-1",
          input: "用户询问发票流程",
          expectedOutput: "说明开票步骤",
          metadata: {
            channel: "support",
          },
        },
        {
          input: "用户询问退款时效",
          baselineOutput: "3 个工作日",
          candidateInput: "2 个工作日",
        },
      ],
    });
    expect(validation.success).toBe(true);
    if (!validation.success) {
      return;
    }
    expect(validation.data.datasetId).toBe("dataset-1");
    expect(validation.data.items).toHaveLength(2);
    expect(validation.data.items[0]?.metadata).toEqual({ channel: "support" });
    expect(validation.data.items[1]?.caseId).toBeUndefined();
  });

  test("历史会话物化校验：支持显式 sessionIds 与 filters 两种来源限定", () => {
    const bySessions = validateReplayDatasetMaterializeInput({
      tenantId: "tenant-1",
      datasetId: "dataset-1",
      sessionIds: ["session-a", "session-b"],
      sampleLimit: 10,
      sanitized: false,
    });
    expect(bySessions.success).toBe(true);
    if (!bySessions.success) {
      return;
    }
    expect(bySessions.data.datasetId).toBe("dataset-1");
    expect(bySessions.data.sessionIds).toEqual(["session-a", "session-b"]);
    expect(bySessions.data.sanitized).toBe(false);

    const byFilters = validateReplayDatasetMaterializeInput({
      tenantId: "tenant-1",
      datasetId: "dataset-1",
      filters: {
        tool: "codex",
        model: "gpt-5",
        from: "2026-03-01T00:00:00.000Z",
        to: "2026-03-06T00:00:00.000Z",
      },
    });
    expect(byFilters.success).toBe(true);
    if (!byFilters.success) {
      return;
    }
    expect(byFilters.data.filters?.tool).toBe("codex");
    expect(byFilters.data.sanitized).toBe(true);
  });
});
