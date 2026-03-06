# Java SDK

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

入口：`src/main/java/com/agentledger/sdk/AgentLedgerClient.java`

示例：

```java
var client = new AgentLedgerClient("https://control-plane.example.com", System.getenv("AGENTLEDGER_TOKEN"));

var datasetId = "dataset_123"; // 来自 createReplayDatasetV2 返回值
var runId = "run_456"; // 来自 createReplayRunV2 / getReplayRunV2

var replaceCases = new AgentLedgerClient.OperationRequest();
replaceCases.path.put("datasetId", datasetId);
replaceCases.body = """
{"items":[{"caseId":"case-1","input":"hello","expectedOutput":"world"}]}
""";
client.replaceReplayDatasetCasesV2(replaceCases);

var listCases = new AgentLedgerClient.OperationRequest();
listCases.path.put("datasetId", datasetId);
listCases.query.put("limit", "20");
client.listReplayDatasetCasesV2(listCases);

var createRun = new AgentLedgerClient.OperationRequest();
createRun.body = """
{"datasetId":"dataset_123","candidateLabel":"candidate-v2"}
""";
client.createReplayRunV2(createRun);

var listRuns = new AgentLedgerClient.OperationRequest();
listRuns.query.put("datasetId", datasetId);
listRuns.query.put("status", "completed");
client.listReplayRunsV2(listRuns);

var diffs = new AgentLedgerClient.OperationRequest();
diffs.path.put("runId", runId);
diffs.query.put("datasetId", datasetId);
diffs.query.put("limit", "20");
client.getReplayRunDiffsV2(diffs);

var artifacts = new AgentLedgerClient.OperationRequest();
artifacts.path.put("runId", runId);
client.getReplayRunArtifactsV2(artifacts);
```
