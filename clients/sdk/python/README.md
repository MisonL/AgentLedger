# Python SDK

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

入口：`agentledger_sdk/client.py`

示例：

```python
from agentledger_sdk import AgentLedgerClient, OperationRequest

client = AgentLedgerClient(
    base_url="https://control-plane.example.com",
    token="${AGENTLEDGER_TOKEN}",
)

dataset_id = "dataset_123"  # 来自 createReplayDatasetV2 返回值
run_id = "run_456"  # 来自 createReplayRunV2 / getReplayRunV2

client.replace_replay_dataset_cases_v2(
    OperationRequest(
        path={"datasetId": dataset_id},
        body={
            "items": [
                {"caseId": "case-1", "input": "hello", "expectedOutput": "world"}
            ]
        },
    )
)

client.list_replay_dataset_cases_v2(
    OperationRequest(path={"datasetId": dataset_id}, query={"limit": 20})
)

client.create_replay_run_v2(
    OperationRequest(body={"datasetId": dataset_id, "candidateLabel": "candidate-v2"})
)

client.list_replay_runs_v2(
    OperationRequest(query={"datasetId": dataset_id, "status": "completed"})
)

client.get_replay_run_diffs_v2(
    OperationRequest(path={"runId": run_id}, query={"datasetId": dataset_id, "limit": 20})
)

client.get_replay_run_artifacts_v2(
    OperationRequest(path={"runId": run_id})
)
```
