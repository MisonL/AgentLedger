# AgentLedger 发布说明：治理路由闭环与 Replay Run 事件

- 日期：2026-03-06
- 状态：发布说明（门禁已固化）
- 适用范围：`services/governance`、`services/integration`、`apps/control-plane`、`apps/web-console`

## 1. 版本概览

本轮交付重点不是继续补零散接口，而是把两条治理执行链真正收口为可对外消费的闭环：

1. Replay 事件链：开放平台 Webhook 在保留旧版 `replay.job.*` 兼容事件的同时，正式补齐 `replay.run.started`、`replay.run.completed`、`replay.run.regression_detected`、`replay.run.failed`、`replay.run.cancelled`。
2. Alert / Weekly 编排链：治理服务在事件发布前先完成 orchestration 规则匹配、去重/抑制判定与 execution log 落库，integration 再按路由结果分发到外部 channels。

## 2. 重点能力更新

### 2.1 Replay Webhook 事件模型扩展

- Webhook 订阅与回放请求已统一支持 `replay.run.*` 运行级事件。
- `replay.job.started/completed/failed` 继续保留为兼容事件，不强制现有接入方立即迁移。
- OpenAPI 示例、控制台事件选项与 contracts 已同步到同一口径。
- 所有主示例现已统一为 `replay.run.*` 优先；`replay.job.*` 仅作为兼容项继续保留。

推荐迁移策略：

1. 新客户端直接订阅 `replay.run.*`。
2. 已依赖 `replay.job.*` 的客户端可以继续运行，但后续新增能力应优先基于 `run` 语义接入。

### 2.2 Alert / Weekly 路由执行闭环

- Governance 在发布 `alert/weekly` 事件前会先执行 orchestration 规则匹配。
- execution log 正式输出 `dispatchMode=rule|fallback`、`dedupeHit`、`suppressed`、`conflictRuleIds`。
- Integration 优先消费 `orchestration.channels`；仅当 `fallback=true` 时回退到 legacy routing。
- `dedupeHit=true` 或 `suppressed=true` 且没有有效 channels 时，不再继续对外分发，但仍保留 execution log 与发布审计。

### 2.3 控制台可观测增强

- Governance 执行日志支持按 `dispatchMode`、`hasConflict` 过滤。
- 页面直接展示当前结果集的 `rule/fallback/conflict/dedupe/suppressed/simulated` 统计。
- 前端不再依赖 `metadata.dispatchMode` 这样的非正式字段推断分发模式。

## 3. 配置与升级提醒

### 3.1 新增或需要关注的配置

- `OPEN_PLATFORM_WEBHOOK_SECRET_KEY`
  - 用于 control-plane 对 Webhook secret 做加密/解密。
  - 生产环境建议显式配置，避免落回开发态默认 key。
- `WEBHOOK_REPLAY_MAX_RETRIES`
- `WEBHOOK_REPLAY_RETRY_BASE_DELAY_MS`
- `WEBHOOK_REPLAY_RETRY_MAX_DELAY_MS`
  - 仅作用于 control-plane 内置 Webhook replay worker。

### 3.2 升级建议

1. 若外部系统已消费 `replay.job.*`，本次无需强制改造，但建议规划迁移到 `replay.run.*`。
2. 若要使用 Webhook replay 的签名能力或 secret 持久化，生产环境必须补齐 `OPEN_PLATFORM_WEBHOOK_SECRET_KEY`。
3. 若下游通道依赖旧版 severity/broadcast 路由，请确认是否允许 Governance orchestration 直接覆盖 channels；`fallback=true` 才会走 legacy routing。

## 4. 验收与回归

建议至少执行以下检查：

```bash
bun run check:ts
bun test apps/control-plane/test/repository.test.ts apps/control-plane/test/api.test.ts --timeout 120000
cd apps/web-console && bun run test
go test ./services/governance ./services/integration
GOV_E2E_DATABASE_URL=... bun run test:e2e-governance-routing
```

其中真实治理链 E2E 会覆盖：

- `fallback`
- `dedupe`
- `suppressed`
- `fail-open`
- `weekly`

Release 工作流补充：

- `.github/workflows/release.yml` 的 `pre-release-gate` 已纳入 `bun run test:e2e-governance-routing`。
- 发布仓库需要预先配置 `GOV_E2E_DATABASE_URL` secret；未配置时 tag 发布会在门禁阶段直接失败。

## 5. 兼容性说明

- `replay.job.*` 事件未删除。
- `jobId` 作为 `runId` 兼容别名仍继续返回，但新客户端不建议再把它当成主语义字段。
- 本轮未引入强制数据库迁移脚本；治理链 E2E 使用的是最小测试 schema，不影响现有生产部署方式。
