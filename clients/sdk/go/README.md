# Go SDK

该目录由 `bun run sdk:generate` 自动生成。

- OpenAPI 标题：AgentLedger Control Plane API
- OpenAPI 版本：1.2.0
- OpenAPI Spec：3.0.3
- 文档摘要：de8d8da4c92c012317f485438578d84c02dc733c63a9b5e2f5d1310ec0a9f178
- operation 数量：45

Replay v2 canonical：

- v2 replay 统一使用 `datasetId` / `runId` 作为主语义。
- `baselineId` / `jobId` 仍被客户端接受，但只作为兼容别名。
- 传给 v2 replay 的 `datasetId` 应使用 `createReplayDatasetV2` 返回的资源 ID。
- 支持通过 `materializeReplayDatasetCasesV2` 将历史 session 物化为真实 replay cases。

入口：`client.go`

示例：

```go
client := agentledgersdk.NewClient("https://control-plane.example.com", os.Getenv("AGENTLEDGER_TOKEN"))

datasetID := "dataset_123" // 来自 CreateReplayDatasetV2 返回值
runID := "run_456" // 来自 CreateReplayRunV2 / GetReplayRunV2

_, _ = client.ReplaceReplayDatasetCasesV2(ctx, &agentledgersdk.OperationRequest{
    PathParams: map[string]string{"datasetId": datasetID},
    Body: map[string]any{
        "items": []map[string]any{
            {"caseId": "case-1", "input": "hello", "expectedOutput": "world"},
        },
    },
})

_, _ = client.ListReplayDatasetCasesV2(ctx, &agentledgersdk.OperationRequest{
    PathParams: map[string]string{"datasetId": datasetID},
    Query: map[string]string{"limit": "20"},
})

_, _ = client.CreateReplayRunV2(ctx, &agentledgersdk.OperationRequest{
    Body: map[string]any{"datasetId": datasetID, "candidateLabel": "candidate-v2"},
})

_, _ = client.ListReplayRunsV2(ctx, &agentledgersdk.OperationRequest{
    Query: map[string]string{"datasetId": datasetID, "status": "completed"},
})

_, _ = client.GetReplayRunDiffsV2(ctx, &agentledgersdk.OperationRequest{
    PathParams: map[string]string{"runId": runID},
    Query: map[string]string{"datasetId": datasetID, "limit": "20"},
})

_, _ = client.GetReplayRunArtifactsV2(ctx, &agentledgersdk.OperationRequest{
    PathParams: map[string]string{"runId": runID},
})
```
