# PHP SDK

该目录由 `bun run sdk:generate` 自动生成。

- OpenAPI 标题：AgentLedger Control Plane API
- OpenAPI 版本：1.2.0
- OpenAPI Spec：3.0.3
- 文档摘要：4ad461c6af1877f454ce24f9c08c4b1efb0e0ccebaab64715d88b5996c97dab8
- operation 数量：45

Replay v2 canonical：

- v2 replay 统一使用 `datasetId` / `runId` 作为主语义。
- `baselineId` / `jobId` 仍被客户端接受，但只作为兼容别名。
- 传给 v2 replay 的 `datasetId` 应使用 `createReplayDatasetV2` 返回的资源 ID。
- 支持通过 `materializeReplayDatasetCasesV2` 将历史 session 物化为真实 replay cases。

入口：`src/AgentLedgerClient.php`

示例：

```php
$client = new AgentLedger\Sdk\AgentLedgerClient(
    'https://control-plane.example.com',
    getenv('AGENTLEDGER_TOKEN') ?: null,
);

$datasetId = 'dataset_123'; // 来自 createReplayDatasetV2 返回值
$runId = 'run_456'; // 来自 createReplayRunV2 / getReplayRunV2

$client->replaceReplayDatasetCasesV2([
    'path' => ['datasetId' => $datasetId],
    'body' => [
        'items' => [
            ['caseId' => 'case-1', 'input' => 'hello', 'expectedOutput' => 'world'],
        ],
    ],
]);

$client->listReplayDatasetCasesV2([
    'path' => ['datasetId' => $datasetId],
    'query' => ['limit' => 20],
]);

$client->createReplayRunV2([
    'body' => ['datasetId' => $datasetId, 'candidateLabel' => 'candidate-v2'],
]);

$client->listReplayRunsV2([
    'query' => ['datasetId' => $datasetId, 'status' => 'completed'],
]);

$client->getReplayRunDiffsV2([
    'path' => ['runId' => $runId],
    'query' => ['datasetId' => $datasetId, 'limit' => 20],
]);

$client->getReplayRunArtifactsV2([
    'path' => ['runId' => $runId],
]);
```
