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
| Agent 本地状态（新增） | `agent status` 输出本地 token 状态、默认协议/endpoint，以及可选本地配置包的 `version/issued_at/signature_status` 骨架。 |
| Agent 本地队列骨架（新增） | `agent run` 支持通过 `--queue-dir` 或 `AGENT_QUEUE_DIR` 启用本地 Store-and-Forward 目录，先落盘再顺序冲刷；`agent status` 可返回 `pending_count/oldest_enqueued_at/total_bytes`。 |
| Agent 守护模式与运行时视图（新增） | `agent run --daemon` 支持周期拉取服务端 runtime config、上报 heartbeat，并在 control-plane / Web Console 的 Agents 视图中查看在线状态、最近心跳、最近配置版本与 source 绑定快照。 |
| 归档与保留（新增） | `services/archiver` 支持 `local/object/hybrid` 三种归档模型；本地归档可选 JSONL + ZSTD 压缩落盘（`.jsonl.zst`），对象存储继续保持 `.jsonl`。 |
| 预算治理 | budgets 读写、阈值分级、告警与状态流转 |
| 数据主权与复制治理（新增） | `residency policy / region mappings / replication jobs` 全链路；Governance 支持策略保存、复制任务创建、审批、取消与状态刷新 |
| 审计取证（新增） | 审计取证包导出（链式哈希 + HMAC 签名）与本地验签命令 |
| 集成分发 | 支持 `alert/weekly` 双事件；`webhook` 原样转发，`wecom/dingtalk/feishu` 使用 `text` 模板消息 |
| 回调链路 | governance -> integration -> control-plane callback 闭环 |
| 开放平台与质量回放（新增） | OpenAPI 摘要、API Key/Webhook 管理、`/api/v2/quality/*` 质量评估与项目趋势、`/api/v2/replay/*` datasets/runs/diffs/artifacts/download |
| 产品增强（新增） | `Quality` 已支持 `forecast/advice` 查询与 `advice executions` 的 execute/list/cancel；`Replay` 已支持 experiments 的 create/list/detail/run/cancel/results/artifacts。 |
| 外部登录风控（新增） | `external login/exchange` 支持 `AUTH_EXTERNAL_RISK_MODE=audit_only|block`，响应与审计带 `riskDecision`。 |
| 配置包拉取与审批发布（新增） | control-plane 提供 `system/config/packages` 的 create/list/detail、`publish`、`watch/latest` 与最小 approvals；agent 支持 `config pull` / `config activate` / `config rollback` / `config watch`，已发布配置包才会被 watch 命中；Governance 已支持从现有 package `载入到表单` 与 `克隆为新包`。 |
| Agent Release / Updater（新增） | control-plane 提供 `system/agent-releases` 的 create/list/detail/check；agent 支持 `update check` / `download` / `apply` / `rollback` / `status` 分步执行升级，`download` 已支持可选 Ed25519 工件签名校验，`check` 已支持服务端 rollout ring / percentage / minVersion 筛选；Governance 已支持 artifact 编排视图与“回填到预览”。 |
| Web Console | Dashboard / Sessions / Analytics / Governance / Agents / Sources / Pricing；Governance 内含 Residency 策略/复制审批工作台、Config Packages 创建/克隆/审批/发布与 watch/latest 预览、Agent Releases artifact 编排视图与 rollout 预览、Open Platform 工作台、Quality project-trends、Replay dataset/run/diff/artifacts/download 工作台；Agents 页面提供守护状态、最近 heartbeat、runtime config 与 source 绑定快照 |
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
agent status
agent status --config-file ./agent-config.json
agent run --queue-dir ./agent-queue
agent run --daemon --control-plane http://127.0.0.1:8080
agent config pull --package-id pkg-demo
agent config activate --package-id pkg-demo
agent config rollback
agent config watch --iterations 1 --auto-activate
agent update check
agent update download --current-version 0.1.0
agent update download --current-version 0.1.0 --signature-public-key-file ./agent-release-public.pem
agent update apply
agent update status
agent update rollback
agent update check --channel beta
```

如需让 `config pull/watch` 与 `update check/download` 统一走企业控制面地址，可在执行前设置：

```bash
export AGENT_GATEWAY_URL=http://127.0.0.1:8080
export AGENT_RELEASE_CHANNEL=stable
export AGENT_RELEASE_SIGNING_PUBLIC_KEY_FILE=./agent-release-public.pem
export AGENT_DAEMON_HEARTBEAT_INTERVAL_SECONDS=30
export AGENT_DAEMON_STALE_AFTER_SECONDS=90
```

### 4.2 Agent 三平台静默安装模板（新增）

当前仓库提供“静默安装/部署模板 + 验证命令”，用于把现有二进制分发到目标机器；当前已支持 agent 本地分步升级执行，但仍不包含后台常驻自升级守护进程。

- Linux 模板：`docs/templates/agent-silent-install-linux.sh`
- macOS 模板：`docs/templates/agent-silent-install-macos.sh`
- Windows 模板：`docs/templates/agent-silent-install-windows.ps1`

通用验证命令：

```bash
agent version
agent status
agent update check
agent update status
```

### 4.2.1 Agent 分发包组装与验收（新增）

当前仓库还提供一组最小分发脚本，用于把 `dist/agent` 的跨平台产物组装成可交付目录结构：

```bash
bun run build:agent-cross
bash ./scripts/package-agent-distribution.sh
bun run ./scripts/verify-agent-distribution.ts
```

产物目录：

- `dist/agent-distribution/<os>/<arch>/agent-<os>-<arch>.tar.gz`
- `dist/agent-distribution/<os>/<arch>/release-manifest.json`
- `dist/agent-distribution/<os>/<arch>/package/.env.example`
- `dist/agent-distribution/<os>/<arch>/package/AGENT_RELEASE_SIGNING_PUBLIC_KEY.pem.example`
- `dist/agent-distribution/<os>/<arch>/package/SHA256SUMS.txt`

说明：

- 验收脚本默认只在当前主机平台执行本机分发包验收。
- 验收会校验 bundle/package 两层 `SHA256SUMS.txt`，运行 `version`、`status`、`update check`；当提供实际公钥且分发清单带签名时，还会通过本地 mock 跑一次 `update download`。
- 若需在目标机器上验收，直接使用对应分发目录内的二进制与模板脚本。

可选签名分发：

```bash
AGENT_RELEASE_SIGNING_PRIVATE_KEY_FILE=./secrets/agent-release-signing-private.pem \
AGENT_RELEASE_SIGNING_PUBLIC_KEY_FILE=./secrets/agent-release-signing-public.pem \
bash ./scripts/package-agent-distribution.sh

bun run ./scripts/verify-agent-distribution.ts \
  --signature-public-key-file ./secrets/agent-release-signing-public.pem
```

平台建议验证：

```bash
# Linux / macOS
agent update check --channel stable

# Windows PowerShell
agent.exe version
agent.exe status
agent.exe update check --channel stable
```

### 4.3 部署形态基线与预检（新增）

当前仓库已补一份独立部署基线文档：

- `docs/19-部署形态基线与预检.md`

覆盖范围：

- `private-single` 单集群私有化起步
- `multi-env` 企业 `dev/staging/prod`
- `saas-multi-tenant` 共享控制面 SaaS 基线

建议在部署前先执行预检：

```bash
bun run ./scripts/check-deployment-baseline.ts --profile private-single
bun run ./scripts/check-deployment-baseline.ts --profile multi-env --env-file ./.env.staging
bun run ./scripts/check-deployment-baseline.ts --profile saas-multi-tenant --env-file ./.env.prod
```

推荐 smoke 入口：

```bash
bun run smoke:fr505
GOV_E2E_DATABASE_URL=... bun run test:e2e-governance-integration-downstream
```

其中 `test:e2e-governance-integration-downstream` 当前会一并覆盖两条真实跨服务烟测：

- `governance -> integration -> downstream` 的 `alert/weekly` 外部下游分发
- `control-plane PATCH alert status -> integration.alert.external_status_sync -> downstream -> sync_external_link_result callback` 回写闭环

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

如需启用 `services/archiver` 本地 JSONL + ZSTD 压缩归档，可在启动前设置：

```bash
export ARCHIVE_MODE=local
export ARCHIVE_LOCAL_ROOT=/data/agentledger/archive/raw
export ARCHIVE_LOCAL_COMPRESSION=zstd
```

如需同时写本地和对象存储，可把 `ARCHIVE_MODE` 改为 `hybrid`。完整 `ARCHIVE_*` 说明见 `docs/13-环境变量参考.md`。

### 5. 回调链路联调（建议先跑）

```bash
bun run check:callback-stream-binding
bun run test:e2e-integration-routing
bun run test:callback-chain-targeted
bun run test:e2e-governance-callback-chain
```

关键变量：`INTEGRATION_CALLBACK_STREAM`、`INTEGRATION_CALLBACK_SUBJECT`（或 `INTEGRATION_CALLBACK_TOPIC`）、`INTEGRATION_CALLBACK_DURABLE`、`CONTROL_PLANE_BASE_URL`、`INTEGRATION_CALLBACK_PATH`、`INTEGRATION_CALLBACK_SECRET`。详细说明见 `docs/13-环境变量参考.md`。

### 5.1 Integration 真实消费分发 E2E（新增）

```bash
bun run test:e2e-integration-routing
```

说明：

- 使用嵌入式 JetStream + 本地 HTTP 下游，验证 `NATS -> integration consumer -> 外部 channel/control-plane callback` 的真实消费分发链。
- 当前覆盖 `alert orchestration override`、`fallback legacy routing`、`suppressed no-dispatch`、`weekly orchestration override`、`callback NATS forward`。

### 5.2 治理链真实 E2E（新增）

```bash
GOV_E2E_DATABASE_URL='postgres://agentledger:agentledger@127.0.0.1:55432/agentledger_governance_e2e?sslmode=disable' \
bun run test:e2e-governance-routing
```

说明：

- 只需要真实 PostgreSQL；NATS 由测试内部以嵌入式 JetStream 拉起。
- 重点覆盖 `fallback / dedupe / suppressed / fail-open / weekly` 五类治理分发场景。
- `scripts/check-governance-e2e.sh` 会强制校验 `GOV_E2E_DATABASE_URL`，并固定用 `AGENTLEDGER_E2E=1` 执行真实治理链 E2E。
- 该 E2E 仅验证 `services/governance` 在真实 PostgreSQL + 嵌入式 JetStream 上的发布与落库行为；`services/integration` 的真实消费分发仍需单独回归。

### 5.3 governance -> integration 跨服务 smoke（更新）

```bash
GOV_E2E_DATABASE_URL='postgres://agentledger:agentledger@127.0.0.1:55432/agentledger_governance_e2e?sslmode=disable' \
bun run test:e2e-governance-integration-downstream
```

说明：

- 使用真实 PostgreSQL + 嵌入式 JetStream，并拉起子进程 `integration` / `control-plane`。
- 当前同时覆盖 `governance -> integration -> downstream` 的 `alert/weekly` 下游分发，以及 `control-plane alert status PATCH -> integration.alert.external_status_sync -> downstream -> sync_external_link_result callback` 外部状态同步回写闭环。

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
# SDK 只读一致性校验：校验 + 覆盖测试 + SHA256 一致性
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
