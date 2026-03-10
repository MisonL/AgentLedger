# 2026-03-10 Wave5 主干稳定基线合入记录

- 负责人：员工
- 合入分支：`feat/wave5-parallel`
- 主干基线提交：`de1e231`
- 合入方式：`fast-forward`（保留 4 个能力提交边界）
- 目的：把已全量回归通过的一批企业治理增强能力收口到 `main`，为 Wave6 并行开发提供稳定基线。

## 1. 核心变更摘要

1. Integration routing E2E 扩充与回归脚本收口
   - integration routing E2E 覆盖真实通道 `dingtalk/email/email_webhook/ticket`，并验证 fallback 行为。
   - callback chain、governance -> integration 冒烟脚本收口并纳入回归执行记录。

2. Agent 分发链路与 Release/CI 门禁收口
   - 新增 `package:agent-distribution` / `verify:agent-distribution` 统一入口脚本，并在 CI/Release 复用。
   - `SHA256SUMS.txt` 统一为相对路径口径，避免不同工作目录导致校验歧义。

3. 企业治理能力闭环推进（治理页 + 控制面）
   - Rule Hub：资产/版本/diff/发布/回滚/审批/双人审批门槛/审计、最小 `scopeBinding` 接通。
   - MCP：`single_stage/two_stage/multi_stage`、workflow 分支跳转、timeWindow 条件评估与治理页编排回归。
   - Quality/Replay：基础能力上补齐 `forecast/advice` 查询、advice execution 执行链与 replay experiments（对比/工作流/工件）持久化与控制台工作台入口。

4. TokenPulse × AgentLedger 第二阶段最小联调（AgentLedger 侧）
   - 接入 TokenPulse 运行时摘要签名 webhook、幂等去重、traceId 联查与治理页只读面板。
   - 注意：对接基线文档冻结版本以 `docs/integration/TOKENPULSE_AGENTLEDGER_V1.md` 为准。

## 2. 回归与门禁证据

本轮合入前已完成全量回归与门禁验证，执行记录见：

- `docs/17-回归验证执行记录.md`

覆盖范围包括但不限于：

- `bun run lint` / `bun run build` / `bun run test` / `bun run test:coverage`
- `bun run sdk:check` / `bun run sdk:verify` / `bun run sdk:test`
- `bun run test:e2e-integration-routing` / `bun run test:e2e-governance-callback-chain`
- `bun run package:agent-distribution` / `bun run verify:agent-distribution`

## 3. 后续缺口与下一批建议

- 剩余缺口统一收口见：`docs/18-剩余缺口清单.md`
- Wave6 优先建议：`quality` 高级预测与建议深化、`replay` 实验室完全体、IAM/DLP 最小深化（按缺口与风险分批推进）。

