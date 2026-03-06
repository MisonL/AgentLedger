# C# SDK

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

入口：`AgentLedgerClient.cs`

示例：

```csharp
var client = new AgentLedgerClient("https://control-plane.example.com", Environment.GetEnvironmentVariable("AGENTLEDGER_TOKEN"));

var datasetId = "dataset_123"; // 来自 CreateReplayDatasetV2 返回值
var runId = "run_456"; // 来自 CreateReplayRunV2 / GetReplayRunV2

await client.ReplaceReplayDatasetCasesV2(new OperationRequest
{
    Path = new Dictionary<string, string> { ["datasetId"] = datasetId },
    Body = new
    {
        items = new[]
        {
            new { caseId = "case-1", input = "hello", expectedOutput = "world" },
        },
    },
});

await client.ListReplayDatasetCasesV2(new OperationRequest
{
    Path = new Dictionary<string, string> { ["datasetId"] = datasetId },
    Query = new Dictionary<string, string> { ["limit"] = "20" },
});

await client.CreateReplayRunV2(new OperationRequest
{
    Body = new { datasetId, candidateLabel = "candidate-v2" },
});

await client.ListReplayRunsV2(new OperationRequest
{
    Query = new Dictionary<string, string> { ["datasetId"] = datasetId, ["status"] = "completed" },
});

await client.GetReplayRunDiffsV2(new OperationRequest
{
    Path = new Dictionary<string, string> { ["runId"] = runId },
    Query = new Dictionary<string, string> { ["datasetId"] = datasetId, ["limit"] = "20" },
});

await client.GetReplayRunArtifactsV2(new OperationRequest
{
    Path = new Dictionary<string, string> { ["runId"] = runId },
});
```
