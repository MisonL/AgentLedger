# AgentLedger 接手现状与开发计划

## 目标
在不推翻既有主线的前提下，先把 AgentLedger 当前事实边界、与 TokenPulse 的联动状态、以及接手后的开发优先级重新收口，作为 2026-03-26 之后的执行基线。

## 现状梳理
- 当前主线分支是 `main`，提交为 `75b912850ed76a8995175632b9067558f825fbda`，工作区干净。
- TokenPulse v1 联调相关能力已进入主线：
  - 冻结协议文档：`docs/integration/TOKENPULSE_AGENTLEDGER_V1.md`
  - 联调 Runbook：`docs/integration/TOKENPULSE_AGENTLEDGER_V1_RUNBOOK.md`
  - 控制面接入路由：`apps/control-plane/src/routes/tokenpulse-runtime-events.ts`
  - 本地联调编排：`deploy/docker-compose.tokenpulse-v1.yml`
- 当前主线已完成“采集 -> 治理 -> integration 分发 -> control-plane / web-console 联动”的基本闭环，并有 `docs/17-回归验证执行记录.md` 作为回归证据入口。
- 当前待深化重点不再是 TokenPulse 协议设计，而是：
  - Quality 自动治理深化
  - Replay 实验室深化
  - 身份/合规深化
  - Agent C/S 与部署形态长期项
- `docs/21-后续开发计划.md` 仍是 2026-03-11 版本，计划窗口和负责人信息已滞后，不能直接作为当前接手后的执行基线。

## 当前判断
- AgentLedger 在 TokenPulse 联调这条线上已经具备“协议 + 接入 + 查询 + UI 联查 + 本地联调”最小可用能力，不需要回到协议草拟阶段。
- 当前最重要的不是继续扩 TokenPulse 接口，而是保证 TokenPulse 侧未合入工作与 AgentLedger 主线完成一次稳定收口，然后再回到 AgentLedger 自身的 P0 深化项。
- AgentLedger 后续开发要避免再把“跨仓协作”和“本仓能力深化”混在同一批次里，否则测试面和回写文档会继续漂移。

## 开发计划
- [ ] 任务 1：冻结 AgentLedger 当前联调基线，统一以 `main@75b9128` 作为 TokenPulse 联调对端 -> 验证：本地按 `deploy/docker-compose.tokenpulse-v1.yml` 能启动 control-plane / web-console / Postgres / NATS
- [ ] 任务 2：完成与 TokenPulse 当前功能分支的一次双仓联调复验，确认 `202/200/401/400` 四类核心响应语义仍与 frozen v1 一致 -> 验证：按 `docs/integration/TOKENPULSE_AGENTLEDGER_V1_RUNBOOK.md` 执行并留档 evidence
- [ ] 任务 3：在 AgentLedger 侧把 TokenPulse 联调相关内容从“阶段性补丁”收口为稳定基线，不再继续扩协议面 -> 验证：`docs/15`、`docs/17`、联调 Runbook 与代码行为一致
- [ ] 任务 4：优先推进 Quality 自动治理深化，收口策略矩阵、自动执行、审计与治理台展示一致性 -> 验证：`bun run --cwd apps/control-plane test test/api.test.ts`、`bun run --cwd apps/web-console test -- test/api.test.ts test/app.test.tsx`
- [ ] 任务 5：并行推进 Replay 实验室深化，重点补实验编排、基线锁定、防并发和结果摘要稳定性 -> 验证：`bun run test`、`bun run --cwd apps/web-console test`、`bun run sdk:check`
- [ ] 任务 6：在 Quality / Replay 两条 P0 收口后，再进入身份与合规域，优先做可验收的 IAM 最小闭环，而不是同时摊开所有企业蓝图 -> 验证：新增定向 auth / identity 回归与 `bun run test`
- [ ] 任务 7：将 Agent C/S、Updater、静默安装、部署形态作为第二阶段计划单独推进，不与 Quality / Replay 混批 -> 验证：`go test ./clients/agent`、`bun run smoke:fr505`、部署预检脚本通过

## 关键路径
- 关键路径 1：双仓联调复验
- 关键路径 2：Quality P0 深化
- 关键路径 3：Replay P0 深化
- 验证永远放在每个批次最后，且必须回写 `docs/17-回归验证执行记录.md`

## 暂不处理
- 不新增 AgentLedger -> TokenPulse 反向控制接口
- 不新增第二套 TokenPulse 协议版本
- 不在 Quality / Replay P0 期间同时启动 IAM、DLP、MCP、部署形态四条主线实现

## 完成标准
- [ ] TokenPulse v1 联调在 AgentLedger 主线基线上可复现通过
- [ ] AgentLedger 后续计划不再依赖 2026-03-11 的旧时间窗口
- [ ] P0 开发顺序收敛为“联调收口 -> Quality -> Replay”
- [ ] 每一批次都有明确回归命令与文档回写入口
