# Swift SDK

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

入口：`Sources/AgentLedgerSDK/AgentLedgerClient.swift`

示例：

```swift
let client = AgentLedgerClient(
    baseUrl: "https://control-plane.example.com",
    token: ProcessInfo.processInfo.environment["AGENTLEDGER_TOKEN"]
)

let datasetId = "dataset_123" // 来自 createReplayDatasetV2 返回值
let runId = "run_456" // 来自 createReplayRunV2 / getReplayRunV2

try await client.replaceReplayDatasetCasesV2(
    request: OperationRequest(
        path: ["datasetId": datasetId],
        body: [
            "items": [
                ["caseId": "case-1", "input": "hello", "expectedOutput": "world"],
            ],
        ]
    )
)

try await client.listReplayDatasetCasesV2(
    request: OperationRequest(
        path: ["datasetId": datasetId],
        query: ["limit": "20"]
    )
)

try await client.createReplayRunV2(
    request: OperationRequest(body: ["datasetId": datasetId, "candidateLabel": "candidate-v2"])
)

try await client.listReplayRunsV2(
    request: OperationRequest(query: ["datasetId": datasetId, "status": "completed"])
)

try await client.getReplayRunDiffsV2(
    request: OperationRequest(
        path: ["runId": runId],
        query: ["datasetId": datasetId, "limit": "20"]
    )
)

try await client.getReplayRunArtifactsV2(
    request: OperationRequest(path: ["runId": runId])
)
```
