# AgentLedger 发布说明：Source Region 显式治理与回填

- 日期：2026-03-06
- 状态：发布候选说明
- 适用范围：`apps/control-plane`、`apps/web-console`、`services/puller`、`scripts`

## 1. 本轮目标

本轮把 `sourceRegion` 从“散落在 metadata 的兼容信息”收口为正式治理字段，并补齐三条最关键的交付链：

1. Source 管理链：创建、编辑、缺失筛查、按租户主地域回填。
2. Puller 执行链：residency 检查优先使用正式字段，缺失 region 改为严格拒绝。
3. 运维交付链：提供 Bun CLI 做离线 dry-run / apply 回填。

## 2. 重点变化

### 2.1 Source 合同与管理接口

- `Source` / `CreateSourceInput` / `SystemConfigBackupSource` 正式支持 `sourceRegion`。
- `PATCH /api/v1/sources/:id` 支持更新 `sourceRegion` 等可编辑字段。
- `GET /api/v1/sources/missing-region` 用于列出显式 `source_region` 仍缺失的 Source。
- `POST /api/v1/sources/source-region/backfill`
  - 以租户 `primaryRegion` 为依据批量补齐缺失项。
  - 支持 `dryRun=true` 预演，不落库。
  - 未配置租户主地域时返回 `409`。

### 2.2 控制台 Source 区域治理

- Sources 列表新增 `Region` 列。
- 页面支持“仅看缺失 Region”筛选。
- 支持对单个 Source 进入编辑态补录 region。
- 支持按租户主地域批量回填，适合处理历史遗漏数据。

### 2.3 Puller residency 严格化

- puller `loadSource` 读取正式 `sources.source_region`。
- `resolveSourceResidencyRegion` 优先使用正式字段，仅在过渡期回退 metadata 别名。
- 配置 `PULLER_RESIDENCY_TARGET_REGION` 后：
  - `sourceRegion` 缺失直接拒绝。
  - `sourceRegion` 与目标地域不一致也直接拒绝。
- 这意味着此前“未知 source region 仍放行”的 fail-open 已关闭。

### 2.4 离线回填 CLI

新增脚本：

```bash
bun run source-region:backfill --tenant <tenantId> [--source <sourceId>] [--dry-run] [--output text|json]
```

说明：

- `--tenant` 必填。
- `--source` 可重复，用于限定回填范围。
- `--dry-run` 仅预演，返回 `would_update`。
- `--output json` 便于自动化系统直接消费结构化结果。

## 3. 升级提醒

1. `sourceRegion` 的批量回填依据是租户 `primaryRegion`，不是对每个 Source 的智能判定。
2. 多地域租户请先 `dry-run`，确认结果后再真实执行。
3. 开启 `PULLER_RESIDENCY_TARGET_REGION` 后，缺失 `sourceRegion` 的历史 Source 会被 puller 拒绝，建议先补齐再上线严格策略。

## 4. 建议验收

```bash
bun run check:ts
bun test apps/control-plane/test/api.test.ts --timeout 120000
cd apps/web-console && bun run test
go test ./services/puller
bun run test:scripts
```
