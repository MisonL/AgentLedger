# AgentLedger

面向企业的 AI 使用治理平台，用于统一采集、审计、预算与分析团队的 AI CLI/IDE 会话数据。

[![CI](https://github.com/MisonL/AgentLedger/actions/workflows/ci.yml/badge.svg)](https://github.com/MisonL/AgentLedger/actions/workflows/ci.yml)

## 核心价值

AgentLedger 面向企业研发与平台团队，目标是把分散在 AI CLI 与 AI IDE 的会话行为统一纳入治理闭环：

- 可采集：统一接入多客户端会话与使用量数据。
- 可审计：完整保留关键操作与治理动作审计记录。
- 可预算：按租户/组织/用户/模型设置阈值与告警。
- 可分析：提供热力图、会话检索、模型与成本统计。

## 当前功能

| 能力域 | 当前可用能力 |
| --- | --- |
| 会话与使用量 | 使用热力图（usage heatmap）、daily/monthly/models/sessions 聚合、会话详情与事件列表 |
| Source 管理 | source 新增/查询/删除、连通性测试、同步任务管理 |
| Agent 自动采集（新增） | `agent collect` 按 docs/09 的 P0/P1 客户端矩阵自动采集本机会话并上报；支持 `--tool=auto` 和显式 `--tool=<client-key>`。 |
| 预算治理 | budgets 读写、阈值分级、告警与状态流转 |
| 数据主权与复制治理（新增） | `residency policy / region mappings / replication jobs` 全链路；Governance 支持策略保存、复制任务创建、审批、取消与状态刷新 |
| 审计取证（新增） | 审计取证包导出（链式哈希 + HMAC 签名）与本地验签命令 |
| 集成分发 | 支持 `alert/weekly` 双事件；`webhook` 原样转发，`wecom/dingtalk/feishu` 使用 `text` 模板消息 |
| 回调链路 | governance -> integration -> control-plane callback 闭环 |
| 开放平台与质量回放（新增） | OpenAPI 摘要、API Key/Webhook 管理、`/api/v2/quality/*` 质量评估与项目趋势、`/api/v2/replay/*` datasets/runs/diffs/artifacts/download |
| Web Console | Dashboard / Sessions / Analytics / Governance / Sources / Pricing；Governance 内含 Residency 策略/复制审批工作台、Open Platform 工作台、Quality project-trends、Replay dataset/run/diff/artifacts/download 工作台 |
| 工程质量 | Bun + Go 混合 monorepo、基础 CI、脚本化门禁 |

## 本轮治理闭环更新

- Replay Webhook 事件已对外统一为“旧版 `replay.job.*` 兼容保留 + 新版 `replay.run.*` 正式事件”，覆盖 `started/completed/regression_detected/failed/cancelled`。
- `alert/weekly` 两类治理事件现在都会先经过 orchestration 规则匹配，再带着 `dispatchMode=rule|fallback`、`dedupeHit`、`suppressed`、`conflictRuleIds` 进入执行日志与 integration 路由。
- Governance 控制台已补齐 execution log 的 `dispatchMode` / `conflict` 筛选与结果集统计，不再依赖 `metadata.dispatchMode` 的非正式字段。
- 真实治理链 E2E 已覆盖 `fallback / dedupe / suppressed / fail-open / weekly`，使用真实 PostgreSQL + 嵌入式 NATS 验证主链路。

## 架构

```mermaid
flowchart LR
    A[AI 客户端<br/>CLI / IDE / Agent]
    B[Ingestion Gateway<br/>Go]
    C[NATS JetStream]
    D[Normalizer<br/>Go]
    E[Analytics<br/>Go]
    F[Governance<br/>Go]
    G[Integration<br/>Go]
    H[Control Plane API<br/>Bun + Hono]
    I[Web Console<br/>React + Vite]

    A --> B --> C
    C --> D
    C --> E
    C --> F
    C --> G
    D --> H
    E --> H
    F --> H
    G --> H
    H --> I
```

## 仓库结构

<details>
<summary>目录结构（精简）</summary>

```text
apps/
  control-plane/
  web-console/
services/
  ingestion-gateway/ normalizer/ analytics/ governance/ integration/ puller/ archiver/
packages/
  contracts/ proto/ gen/
clients/
  agent/
scripts/
docs/
deploy/
```

</details>

## 快速开始

### 1. 环境准备

- Bun `>= 1.3`
- Go `>= 1.24`
- CI 侧固定 Bun `1.3.9`（来自 `packageManager`），并使用 `bun install --frozen-lockfile`

### 2. 安装依赖

```bash
make install
```

### 3. 本地质量检查

```bash
make format
make lint
make test
make build
```

### 4. 本地运行

```bash
bun --cwd apps/control-plane run dev
bun --cwd apps/web-console run dev
```

### 4.1 Agent 自动采集（新增）

默认目录：

- `~/.codex/sessions`
- `~/.claude/projects`
- `~/.gemini/tmp`
- `~/.aider/sessions`
- `~/.opencode/sessions`
- `~/.qwen-code/sessions`
- `~/.kimi-cli/sessions`
- `~/.trae-cli/sessions`
- `~/.codebuddy-cli/sessions`
- `~/.cursor/sessions`
- `~/.vscode/sessions`
- `~/.vscode-insiders/sessions`
- `~/.trae-ide/sessions`
- `~/.windsurf/sessions`
- `~/.lingma/sessions`
- `~/.codebuddy-ide/sessions`
- `~/.zed/sessions`

典型命令：

```bash
agent collect
agent collect --help
```

如需启用 `sessions/search` 对 puller 的实时同步重试，可在启动 control-plane 前设置：

```bash
export PULLER_BASE_URL=http://127.0.0.1:8086
export PULLER_SYNC_TIMEOUT_MS=1200
export PULLER_SYNC_RETRY_MAX_ATTEMPTS=3
export PULLER_SYNC_RETRY_BASE_BACKOFF_MS=200
export PULLER_SYNC_RETRY_MAX_BACKOFF_MS=2000
```

如需启用 puller 后台 `sync_jobs` 失败重试，可在启动 `services/puller` 前设置：

```bash
export PULLER_JOB_MAX_RETRIES=3
export PULLER_JOB_RETRY_BASE_DELAY=5s
```

### 5. 回调链路联调（建议先跑）

```bash
bun run check:callback-stream-binding
bun run test:callback-chain-targeted
bun run test:e2e-governance-callback-chain
```

关键变量：`INTEGRATION_CALLBACK_STREAM`、`INTEGRATION_CALLBACK_SUBJECT`（或 `INTEGRATION_CALLBACK_TOPIC`）、`INTEGRATION_CALLBACK_DURABLE`、`CONTROL_PLANE_BASE_URL`、`INTEGRATION_CALLBACK_PATH`、`INTEGRATION_CALLBACK_SECRET`。详细说明见 `docs/13-环境变量参考.md`。

### 5.1 治理链真实 E2E（新增）

```bash
AGENTLEDGER_E2E=1 \
GOV_E2E_DATABASE_URL='postgres://agentledger:agentledger@127.0.0.1:55432/agentledger_governance_e2e?sslmode=disable' \
go test ./services/governance -run '^TestGovernanceE2E' -count=1 -v
```

说明：

- 只需要真实 PostgreSQL；NATS 由测试内部以嵌入式 JetStream 拉起。
- 重点覆盖 `fallback / dedupe / suppressed / fail-open / weekly` 五类治理分发场景。

### 6. 审计取证包导出与校验（新增）

```bash
# 导出（需配置 EVIDENCE_BUNDLE_SIGNING_KEY）
curl -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:8081/api/v1/audits/evidence-bundle?limit=200" \
  -o evidence-bundle.v1.json

# 本地验签
bun run evidence:verify -- --file ./evidence-bundle.v1.json --signing-key <your-secret>
```

### 7. SDK 一键构建（新增）

```bash
# SDK 只读门禁（CI 默认）：校验 + 覆盖测试 + SHA256 一致性
bun run sdk:check

# 需要单独定位时可拆分执行
bun run sdk:verify
bun run sdk:test

# 一键执行：生成 -> 校验 -> 测试 -> 打包
bun run sdk:build
```

构建结果默认输出到：

- 源码：`clients/sdk`
- 产物：`dist/sdk`（包含 `SHA256SUMS.txt`）

## 质量门禁

| 门禁目标 | 命令入口 | 对应脚本 |
| --- | --- | --- |
| TypeScript 类型检查 | `bun run lint` | `scripts/lint.sh` -> `scripts/ts-check.sh` |
| 测试门禁 | `bun run test` | `scripts/test.sh` |
| 构建门禁 | `bun run build` | `scripts/build.sh` |
| 覆盖率门禁 | `bun run test:coverage` | `scripts/test-coverage.sh` + `scripts/check-coverage-threshold.sh` |
| 文本规范（LF/BOM） | `bun run check:text-normalization` | `scripts/check-text-normalization.sh` |
| 支持矩阵一致性（P0/P1 + parser 入口） | `bun run check:support-matrix` | `scripts/check-support-matrix.ts` |
| 回调配置绑定一致性 | `bun run check:callback-stream-binding` | `scripts/check-callback-stream-binding.sh` |
| SDK 只读一致性门禁 | `bun run sdk:check` | `scripts/sdk-check.ts` |

### Coverage 阈值（当前执行）

1. `services/ingestion-gateway`: `>= 70%`
2. `services/puller`: `>= 70%`
3. `services/integration`: `>= 75%`
4. `apps/control-plane`: `All files` 行覆盖率 `>= 80%`

## 里程碑

| 里程碑 | 目标 |
| --- | --- |
| M1 工程底座 | Monorepo、基础 API/Web、脚本化质量检查 |
| M2 采集与解析 MVP | P0 客户端接入与统一事件模型 |
| M3 统计与搜索 MVP | 热力图、usage 聚合、会话检索 |
| M4 预算与治理 | 预算阈值、告警、回调闭环 |
| M5 稳定与发布 | 三平台构建、文档与验收闭环 |

详见 `docs/05-交付计划与验收策略.md`。

## 贡献

1. 在变更前阅读 `docs/` 内相关设计与验收文档。
2. 提交前至少执行：`bun run lint && bun run test && bun run build`。
3. 涉及客户端矩阵变更时，同步更新 `docs/09-主流AI客户端支持矩阵.md`。
4. 涉及回调链路或环境变量变更时，同步更新 `docs/13-环境变量参考.md`。
5. PR 描述需包含：变更范围、验证步骤、风险与回滚策略。
