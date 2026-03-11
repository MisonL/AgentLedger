# AgentLedger 项目规划与功能清单（单文件汇总）

- 文档版本：v0.1
- 更新时间：2026-03-11
- 目标：把“项目定位/边界、需求来源、当前可交付边界、已完成/进行中/待开发 Backlog、Release Gate 与回归入口”收敛到一个单文件入口，便于评审、派工与验收。
- 基线口径（冲突时的优先级）：
  - 对外契约：`docs/integration/TOKENPULSE_AGENTLEDGER_V1.md`（v1 frozen，冻结于 `2026-03-07 12:36:08 +0800`）
  - 项目为什么做、边界是什么：`docs/00-项目愿景与范围.md`
  - 当前主干可交付事实边界：`docs/15-核心功能清单.md` + `README.md`
  - 全量规划 vs 现状 vs 证据：`docs/16-全量规划功能核对矩阵.md`
  - “是否已验证通过”：`docs/17-回归验证执行记录.md` + `.github/workflows/*`
  - 缺口与后续 Backlog：`docs/18-剩余缺口清单.md` + `docs/21-后续开发计划.md`

---

## 1. 项目定位与边界

### 1.1 定位

AgentLedger 面向企业研发与平台团队，定位为“企业级 AI 使用治理平台”，用于统一采集、审计、预算与分析团队在 AI CLI/IDE/Agent 上的会话与使用量数据，并逐步扩展到企业治理、开放平台与合规能力。

### 1.2 V1 必做范围（MVP）

- 数据源管理：本地源 + SSH 远程源。
- 首批客户端支持：按 `docs/09` 的 P0/P1 支持矩阵推进。
- 统一会话浏览：筛选、搜索、按来源追溯。
- 统计看板：daily/monthly/session/model 维度 token/cost。
- 预算跟踪：按日/周/月预算阈值与消耗进度。
- 导出能力：CSV/JSON。
- 审计能力：保留原始来源引用与审计记录。

### 1.3 Non-Goals（V1 不做）

- 不做远程会话回写源端的双向同步（默认单向只读；支持可选同步缓存但仍是只读方向）。
- 不做移动端 App（以 Web Console 响应式为主）。
- 不做“一次性覆盖所有客户端”，按 P0/P1/P2 分批推进。
- 企业级身份与合规深化（SCIM/SAML/MFA/DLP/KMS 等）按 Backlog 分批推进，不把长期蓝图误记为“回归失败”。

---

## 2. 需求来源

### 2.1 需求与规划来源（文档侧）

- 基础功能需求：`docs/01-功能需求规格说明.md`
- 架构与数据模型：`docs/02-软件架构设计.md`、`docs/03-数据模型与解析规范.md`
- UI/UX：`docs/04-UIUX设计规范.md`
- 交付与验收策略：`docs/05-交付计划与验收策略.md`
- 客户端支持矩阵（P0/P1/P2）：`docs/09-主流AI客户端支持矩阵.md`
- 企业 C/S 与治理体系设计（长期蓝图与分阶段路线）：`docs/10-企业级C-S与治理体系设计.md`
- 企业级增强路线图（8 项能力）：`docs/11-企业级增强功能规划.md`
- 部署形态基线与预检：`docs/19-部署形态基线与预检.md`
- 发布说明（变更动机与兼容性）：`docs/releases/*`

### 2.2 对外对接基线（必须以基线文档为准）

- TokenPulse × AgentLedger v1：`docs/integration/TOKENPULSE_AGENTLEDGER_V1.md`
  - `v1` 冻结后：只允许向后兼容追加字段；禁止修改既有字段名/语义/必填性；破坏性变更必须升版本并重新评审。

---

## 3. 里程碑与 Release Gate

### 3.1 里程碑（来自 `docs/05`）

- M1 工程底座：monorepo、基础 API/Web、脚本体系；退出标准 `lint/test/build` 稳定执行。
- M2 采集解析：P0 connector + 统一事件模型 + 解析链路；退出标准 P0 最小采集闭环打通、失败重试可观测。
- M3 统计检索：usage 聚合、热力图、sessions 检索；退出标准关键路径有测试且可复现。
- M4 治理回调：budgets、alerts、integration callback；退出标准 governance -> integration -> control-plane 闭环通过脚本化回归。
- M5 稳定发布：跨平台构建、文档完备、发布检查；退出标准多架构构建与三平台最小 smoke 通过。

### 3.2 Release Gate（tag `v*` 严格门禁）

门禁入口以 `.github/workflows/release.yml` 与 `docs/05` 为准；本地可按同等命令复现执行。

---

## 4. 已完成（已实现且已验证；按功能域/里程碑）

> 本节条目均在 `docs/16` 标为“已实现且已验证”，并在 `docs/17` 有可复现记录或在工作流中执行。

### 4.1 基础账本与采集接入（M2/M3）

#### DONE-LEDGER-01 多源接入模型与 Source 生命周期（里程碑：M2）

- 目标：统一抽象 source（本地/远程）并支持 create/list/delete 与多租户隔离。
- 验收标准：sources API 可用且租户隔离生效；相关回归命令通过。
- 风险/依赖：依赖 `DATABASE_URL`；权限策略要求 source 路径只读、避免写入源端。
- 验证命令：

```bash
bun run test
```

#### DONE-LEDGER-02 SSH 实时读取 + realtime/sync/hybrid + 同步任务编排（里程碑：M2）

- 目标：支持 SSH Pull 实时读取，并提供 `realtime/sync/hybrid` 访问模式与同步任务编排能力。
- 验收标准：puller 逻辑与 control-plane 接口协同可用；相关回归命令通过。
- 风险/依赖：依赖 SSH 连通性与远端路径权限；依赖 `PULLER_*` 配置与 NATS/JetStream（见 `docs/19`）。
- 验证命令：

```bash
bun run test
go test ./services/puller
```

#### DONE-LEDGER-03 来源健康/解析失败追踪/同步失败重试（里程碑：M2）

- 目标：提供 source 健康指标、解析失败明细与可观测重试/降级行为。
- 验收标准：`/sources/:id/health`、`/parse-failures` 等接口可用；相关回归命令通过。
- 风险/依赖：依赖 puller 与 control-plane 数据一致性；风险在于客户端日志格式漂移导致解析失败（见 `docs/05` 风险表）。
- 验证命令：

```bash
bun run test
```

#### DONE-LEDGER-04 Agent 自动采集与 P0/P1 客户端矩阵（里程碑：M2）

- 目标：`agent collect` 按 `docs/09` 的 P0/P1 客户端矩阵自动采集并上报；矩阵与 connector/collector/parser 的声明保持可校验一致。
- 验收标准：`check:support-matrix` 与 `check:puller-p0-accuracy` 门禁通过；FR-505 smoke 通过。
- 风险/依赖：依赖客户端目录与日志格式稳定；新增/调整 P0/P1 客户端必须同步更新 `docs/09` 与对应声明并通过门禁。
- 验证命令：

```bash
bun run check:support-matrix
bun run check:puller-p0-accuracy
bun run smoke:fr505
```

#### DONE-LEDGER-05 会话检索/详情/事件时间线/导出（里程碑：M3）

- 目标：提供 sessions search、详情、事件列表与 JSON/CSV 导出。
- 验收标准：sessions/export 相关接口在控制面回归中覆盖且通过。
- 风险/依赖：依赖 `DATABASE_URL` 与导出权限/审计策略；风险为统计口径争议或导出滥用（见 `docs/05` 风险表）。
- 验证命令：

```bash
bun run test
```

#### DONE-LEDGER-06 usage 聚合/热力图/日期下钻/weekly summary（里程碑：M3）

- 目标：提供 tokens/cost/sessions 聚合与热力图展示，并支持下钻与周报摘要构建。
- 验收标准：analytics 与 control-plane 代理接口可用且回归通过。
- 风险/依赖：依赖统计口径一致性与定价目录；依赖 `services/analytics` 的聚合任务稳定。
- 验证命令：

```bash
bun run test
go test ./services/analytics
```

#### DONE-LEDGER-07 定价目录与成本口径（里程碑：M3）

- 目标：维护 pricing catalog，并以“日志优先/估算补齐”的口径输出 cost。
- 验收标准：pricing 读写与 usage 成本口径在回归中覆盖且通过。
- 风险/依赖：依赖定价目录更新流程；风险为模型定价变化导致历史对账偏差。
- 验证命令：

```bash
bun run test
```

#### DONE-LEDGER-08 预算策略/阈值分级/解冻审批与预算绑定校验（里程碑：M4）

- 目标：支持 budgets 读写、阈值与治理流转，并与回调链路做预算绑定校验。
- 验收标准：control-plane + governance 相关回归通过。
- 风险/依赖：依赖 governance 服务与数据库一致性；预算配置错误会影响治理动作（需审计可追溯）。
- 验证命令：

```bash
bun run test
go test ./services/governance
```

### 4.2 企业治理（告警/数据主权/Rule Hub/MCP）（M4）

#### DONE-GOV-01 告警列表与状态流转（里程碑：M4）

- 目标：支持 alerts list/ack/resolve 等最小生命周期。
- 验收标准：相关接口与控制台交互在回归中覆盖且通过。
- 风险/依赖：依赖租户隔离与审计字段；风险为状态机语义漂移导致外部联动误判。
- 验证命令：

```bash
bun run test
```

#### DONE-GOV-02 告警编排规则/模拟/执行日志（里程碑：M4）

- 目标：在治理服务侧形成告警编排（匹配、去重、抑制、冲突识别）与可观测执行日志。
- 验收标准：规则 CRUD、simulate、executions 过滤与治理工作台回归通过。
- 风险/依赖：依赖 governance 与 control-plane 的 contracts 一致；风险为规则引擎改语义导致历史执行对账困难。
- 验证命令：

```bash
bun run test
go test ./services/governance
```

#### DONE-GOV-03 alert/weekly -> governance -> integration 分发闭环（里程碑：M4）

- 目标：治理事件先经 orchestration 决策，再由 integration 真实消费并按 channels 分发。
- 验收标准：治理链真实 E2E + integration routing E2E 均通过；execution log 可看到去重/抑制/冲突等线索。
- 风险/依赖：依赖 NATS JetStream（stream/subject/durable）配置正确；风险为回调配置不一致导致闭环中断（见 `docs/05` 风险表）。
- 验证命令：

```bash
AGENTLEDGER_E2E=1 GOV_E2E_DATABASE_URL=... bun run test:e2e-governance-routing
bun run test:e2e-integration-routing
```

#### DONE-GOV-04 告警外部联动运维与失败治理深化（里程碑：M4）

- 目标：补齐外部联动失败治理闭环（失败聚合、重试审计、DLQ recovery、长期趋势/容量运维视图）。
- 验收标准：control-plane 与 web-console 定向回归通过；DLQ 与重试链路可观测。
- 风险/依赖：依赖 integration DLQ stream 与 recovery job；风险为下游通道不稳定导致失败堆积，需要容量与告警。
- 验证命令：

```bash
bun test apps/control-plane/test/api.test.ts -t "integration alert failure"
bun run --cwd apps/web-console test -- test/api.test.ts -t "fetchIntegrationAlertFailureTrends"
bun run --cwd apps/web-console test -- test/app.test.tsx -t "治理页支持加载长期趋势/容量运维视图"
```

#### DONE-GOV-05 数据主权策略/地域映射/复制任务（里程碑：M4）

- 目标：提供 residency policy、region mappings 与 replication jobs 的全链路治理入口。
- 验收标准：策略保存、复制任务创建/审批/取消与状态刷新回归通过。
- 风险/依赖：依赖 `sourceRegion` 治理字段与 puller residency 校验策略；依赖 KMS/归档映射等配置一致性（见 `docs/19`）。
- 验证命令：

```bash
bun run test
```

#### DONE-GOV-06 Rule Hub 首版闭环（里程碑：M4）

- 目标：规则资产中心支持资产/版本/发布/回滚/审批/审计与 scope 绑定。
- 验收标准：前后端回归覆盖且通过；治理页可查看资产与 scopeBinding。
- 风险/依赖：依赖审批审计口径稳定；风险为规则语义或 scope 绑定变更导致线上治理误用。
- 验证命令：

```bash
bun run test
```

#### DONE-GOV-07 Rule Hub 双人审批与版本 diff（里程碑：M4）

- 目标：支持 `requiredApprovals=1/2` 与 publish 前审批人数校验；提供版本 diff API 与控制台展示。
- 验收标准：control-plane 与 web-console 回归通过；发布说明与 contracts 对齐（见 `docs/releases/2026-03-07-*`）。
- 风险/依赖：依赖审批去重与审计“创建/更新”口径一致；风险为审批并发导致重复提交或审计不一致。
- 验证命令：

```bash
bun run --cwd apps/control-plane test test/api.test.ts
bun run --cwd apps/web-console test
```

#### DONE-GOV-08 MCP 工具治理首版闭环（里程碑：M4）

- 目标：提供 MCP policy/approvals/evaluate/invocations 闭环与租户隔离。
- 验收标准：全量测试覆盖 MCP 主链路并通过。
- 风险/依赖：依赖编排状态机与条件匹配语义稳定；风险为权限模型不足导致误放行。
- 验证命令：

```bash
bun run test
```

### 4.3 身份/审计/合规（M4）

#### DONE-IDENT-01 tenant/org/member/device/agent/source-binding（里程碑：M4）

- 目标：提供企业身份基础模型与绑定关系（含 agent/source-binding）。
- 验收标准：identity 全量 API 测试覆盖正常流与租户隔离并通过。
- 风险/依赖：依赖控制面数据库 schema 与租户隔离；风险为绑定模型变更引发数据迁移成本。
- 验证命令：

```bash
bun run test
```

#### DONE-IDENT-02 provider discovery + OAuth/OIDC code exchange + 外部断言登录（里程碑：M4）

- 目标：支持多 provider discovery、OAuth/OIDC 登录换取平台会话，以及外部断言登录入口。
- 验收标准：`auth/providers`、`external/login/exchange` 等在全量测试中覆盖且通过。
- 风险/依赖：依赖外部 IdP 配置与回调 URL 正确；风险为风险登录策略升级导致兼容性变化（需审计可追溯）。
- 验证命令：

```bash
bun run test
```

#### DONE-COMPLIANCE-01 审计查询/导出/合规取证包（里程碑：M4）

- 目标：支持 audits 查询/导出与 evidence bundle（链式哈希 + 签名）取证包，并提供本地验签命令。
- 验收标准：audits API 与 `evidence:verify` 验签命令均可通过回归与脚本验证。
- 风险/依赖：依赖签名密钥管理与 DLP 策略；风险为证据口径变更导致历史包无法验签（需稳定版本化）。
- 验证命令：

```bash
bun run test
bun run evidence:verify -- --file <bundle.json>
```

#### DONE-AGENT-OPS-01 Agent 生命周期审计（里程碑：M5）

- 目标：支持 agent lifecycle events（create/list）与 doctor/status/update check 的可选生命周期上报。
- 验收标准：control-plane 与 agent 侧测试覆盖并通过。
- 风险/依赖：依赖 agent 端稳定上报与服务端存储；风险为事件泛滥导致存储与查询压力。
- 验证命令：

```bash
bun run test
go test ./clients/agent
```

### 4.4 开放平台/事件模型/集成回调（M4）

#### DONE-OPEN-01 OpenAPI + API Key + Webhook + Webhook replay（里程碑：M4）

- 目标：开放平台提供 `/openapi.json`、API Key 生命周期、Webhook CRUD 与 replay tasks。
- 验收标准：open-platform 相关全量测试覆盖且通过；SDK 门禁可通过（见“质量与发布门禁”条目）。
- 风险/依赖：依赖 `OPEN_PLATFORM_WEBHOOK_SECRET_KEY` 等安全配置；风险为 webhook secret 或签名策略变更造成下游验签失败。
- 验证命令：

```bash
bun run test
```

#### DONE-OPEN-02 Replay 事件：兼容 replay.job.* + 正式 replay.run.*（里程碑：M4）

- 目标：对外事件模型在保留兼容事件的同时，统一主口径为 `replay.run.*`。
- 验收标准：contracts/OpenAPI/README/release note 口径一致；`sdk:check` 不报缺失 operation。
- 风险/依赖：依赖外部系统消费兼容策略；风险为事件名变更导致下游订阅断裂（必须向后兼容或升版本）。
- 验证命令：

```bash
bun run test
bun run sdk:check
```

#### DONE-OPEN-03 quality v2 基础版（里程碑：M4）

- 目标：质量评估基础能力（评估写入、日报、相关性、项目趋势、评分卡）与控制台工作台。
- 验收标准：control-plane 与 web-console 回归覆盖并通过。
- 风险/依赖：依赖质量指标口径稳定；风险为评估数据不足导致趋势误判（需在高级版中补齐置信度与策略矩阵）。
- 验证命令：

```bash
bun run test
bun run --cwd apps/web-console test
```

#### DONE-OPEN-04 replay v2 基础回放能力（里程碑：M4）

- 目标：提供 datasets/runs/diffs/artifacts/materialize 基础回放 API 与控制台工作台。
- 验收标准：control-plane 回归与 SDK 校验通过。
- 风险/依赖：依赖 artifacts 存储与下载安全策略；风险为数据脱敏与合规策略未覆盖导致泄露风险。
- 验证命令：

```bash
bun run test
bun run sdk:check
```

#### DONE-INTEG-01 Integration 外部通道分发 + callback 闭环（里程碑：M4）

- 目标：integration 支持多通道分发（webhook/wecom/dingtalk/feishu/email/email_webhook/ticket），并与 control-plane callback 形成闭环（签名/幂等/防重放）。
- 验收标准：callback targeted 回归与 integration routing E2E 通过；通道覆盖与配置校验符合 `docs/19`。
- 风险/依赖：依赖 `INTEGRATION_CALLBACK_SECRET` 与回调签名 TTL 等配置一致；风险为新增通道后 E2E 覆盖缺失导致“通道可配但不可回归”。
- 验证命令：

```bash
bun run test:callback-chain-targeted
bun run test:e2e-integration-routing
bun run test:e2e-governance-callback-chain
```

#### DONE-SMOKE-01 governance -> integration -> downstream 跨服务 smoke（里程碑：M4/M5）

- 目标：覆盖跨服务链路：governance 发布 -> integration 消费分发 -> 下游 -> control-plane 外部状态回写。
- 验收标准：在真实 PostgreSQL + 嵌入式 NATS 环境可实跑通过（见 `docs/17` 记录）。
- 风险/依赖：强依赖 `AGENTLEDGER_E2E=1` 与 `GOV_E2E_DATABASE_URL`；风险为环境配置不齐导致 smoke 无法复现。
- 验证命令：

```bash
AGENTLEDGER_E2E=1 GOV_E2E_DATABASE_URL=... bun run test:e2e-governance-integration-downstream
```

#### DONE-TP-01 TokenPulse × AgentLedger v1 运行时摘要事件对接（里程碑：M4）

- 目标：按冻结基线接收 TokenPulse 单向终态运行时摘要事件，完成验签、幂等登记与 traceId 联查。
- 验收标准：对接契约遵循 `docs/integration/TOKENPULSE_AGENTLEDGER_V1.md`；相关路由回归通过（见 `docs/17`）。
- 风险/依赖：依赖 shared secret、时间窗（±300s）与幂等键策略一致；风险为对外字段语义漂移（v1 frozen 下禁止破坏性变更）。
- 验证命令：

```bash
bun run --cwd apps/control-plane test test/api.test.ts -t "TokenPulse runtime events 路由"
bun run --cwd apps/web-console test -- test/app.test.tsx -t "TokenPulse 运行时摘要|按 traceId 查询 TokenPulse"
```

### 4.5 Agent/C-S/归档/三平台交付（M5）

#### DONE-AGENT-01 Agent Push（HTTP/gRPC）+ OIDC + doctor（里程碑：M5）

- 目标：agent 提供采集、OIDC、诊断与 push 上报入口，并与服务端接入链路兼容。
- 验收标准：全量测试与三平台 smoke 通过。
- 风险/依赖：依赖服务端 endpoint/协议兼容；风险为证书/代理/网络环境差异导致企业落地失败。
- 验证命令：

```bash
bun run test
bun run smoke:fr505
```

#### DONE-AGENT-02 gRPC TLS/mTLS 端到端验证入口（里程碑：M5）

- 目标：提供“真实握手 + 真实请求 + 真实响应”的本地 gRPC mTLS E2E 验证入口。
- 验收标准：Go E2E 测试可单独执行且通过；发布说明已固化（见 `docs/releases/2026-03-07-*`）。
- 风险/依赖：依赖本机端口与证书生成；风险为 TLS 参数变化导致客户端与服务端兼容性断裂。
- 验证命令：

```bash
go test ./clients/agent -run TestSendIngestRequestGRPC_MTLSEndToEnd -count=1
```

#### DONE-ARCHIVE-01 归档存储 local/object/hybrid + 本地 ZSTD（里程碑：M5）

- 目标：支持三种归档模式并可选本地 `.jsonl.zst` 压缩落盘。
- 验收标准：archiver 单测覆盖并通过。
- 风险/依赖：依赖 `ARCHIVE_*` 环境变量与存储权限；风险为落盘路径/对象存储策略配置错误导致归档失败或成本异常。
- 验证命令：

```bash
go test ./services/archiver
```

#### DONE-SMOKE-02 FR-505 三平台最小冒烟（里程碑：M5）

- 目标：三平台最小安装运行冒烟入口可在 CI 与本地执行。
- 验收标准：`smoke:fr505` 脚本可复现通过（见 `docs/17`）。
- 风险/依赖：依赖本机运行环境与端口；风险为跨平台路径差异与权限导致脚本不稳定。
- 验证命令：

```bash
bun run smoke:fr505
```

#### DONE-RELEASE-01 Agent 多架构构建与产物校验（里程碑：M5）

- 目标：覆盖 linux/darwin/windows 多架构构建与产物校验，形成可发布的构建工件。
- 验收标准：构建与校验脚本通过，并产出 `SHA256SUMS.txt`（见 release workflow）。
- 风险/依赖：依赖 Go 交叉编译与平台差异；风险为依赖升级导致产物不可复现。
- 验证命令：

```bash
bun run build:agent-cross
bun run verify:agent-cross
```

#### DONE-RELEASE-02 Release Gate（tag v* 强门禁）与 CI/Release 工作流（里程碑：M1/M5）

- 目标：将发布前强门禁固化到工作流，确保 tag 发布不会绕过关键回归与 smoke。
- 验收标准：`.github/workflows/release.yml` 的 `pre-release-gate` 包含部署基线预检、质量门禁、治理链 E2E、integration routing E2E、跨服务 smoke；任一步失败则中止发布。
- 风险/依赖：强依赖 `GOV_E2E_DATABASE_URL` secret；依赖 `scripts/check-deployment-baseline.ts` 的通道与配置校验；风险为门禁漂移导致“文档通过但 workflow 不执行/反之”。
- 验证命令：

```bash
# 复现 release gate 关键步骤（本地执行时需要准备真实 PG，并设置 GOV_E2E_DATABASE_URL）
bun run ./scripts/check-deployment-baseline.ts --profile release-gate
bun run lint
bun run test
bun run build
bun run check:support-matrix
bun run sdk:verify
bun run sdk:test
bun run check:callback-stream-binding

AGENTLEDGER_E2E=1 GOV_E2E_DATABASE_URL=... bun run test:e2e-governance-routing
bun run test:e2e-integration-routing
AGENTLEDGER_E2E=1 GOV_E2E_DATABASE_URL=... bun run test:e2e-governance-integration-downstream
```

---

## 5. 进行中（含当前 dirty 改动提示）

### 5.1 当前仓库状态（参考）

- 本仓库以 `main` 为主线开发分支；发布以 tag `v*` 触发 release gate（见 `.github/workflows/release.yml`）。
- 若本地存在未提交改动，合入前必须先通过：`bun run lint`、`bun run test`、`bun run build`、`bun run sdk:check`。
- 实际改动范围请以 `git status --porcelain` 为准。

复查命令：

```bash
git status --porcelain
git rev-parse --abbrev-ref HEAD
git rev-parse HEAD
```

### 5.2 进行中（P0，计划窗口：2026-03-11 至 2026-03-24）

#### INPROG-P0-01 quality 高级版预测与建议（策略矩阵深化）

- 目标：在现有 `forecast/advice + advice executions + automation policy` 基础上，补齐更细粒度自动执行策略矩阵的可配置与可解释能力，并保证 simulate 回显、审计字段与治理页展示一致。
- 验收标准：满足 `docs/21` 的 P0-01 目标交付；相关定向回归与全量门禁通过，并把可复现结果回写到 `docs/17`。
- 风险/依赖：依赖指标口径与模型版本策略稳定（`quality-heuristic-v2` 与 `quality-timeseries-v1`）；风险为策略矩阵改语义导致历史 advice execution 解释不一致。
- 验证命令：

```bash
bun run --cwd apps/control-plane test test/api.test.ts -t "api-v2 quality 路由：forecast 与 advice"
bun run --cwd apps/control-plane test test/api.test.ts -t "api-v2 quality 路由：forecast 支持 quality-timeseries-v1"
bun run --cwd apps/web-console test -- test/api.test.ts test/app.test.tsx

bun run lint
bun run test
bun run build
```

#### INPROG-P0-02 replay “实验室”完全体（实验编排与基线管理扩展）

- 目标：在现有 datasets/runs/diffs/artifacts/materialize + experiments 基础上，继续补实验编排与基线管理扩展，并确保工作台真实可复现。
- 验收标准：满足 `docs/21` 的 P0-02 目标交付；新增实验编排/基线能力具备至少一条可复现回归用例，并回写 `docs/17`。
- 风险/依赖：依赖 artifacts 存储与下载权限策略；风险为只补 API 不补控制台/回归导致“功能不可用但难发现”。
- 验证命令：

```bash
bun run test
bun run --cwd apps/web-console test
bun run sdk:check
```

---

## 6. 待开发 Backlog（按优先级 P0/P1/P2）

> 本节以 `docs/21-后续开发计划.md` 为主入口，结合 `docs/18-剩余缺口清单.md` 补充缺口背景；所有条目完成后需按 `docs/21` 的 DoD 回写 `docs/16` 与 `docs/17`。

### 6.1 P0（两周内主线增强）

> 见“进行中”章节：INPROG-P0-01、INPROG-P0-02。

### 6.2 P1（身份/合规深化）

#### BACKLOG-P1-01 SCIM / SAML / MFA / 风险登录（企业 IAM 深化）

- 目标：在现有 OAuth/OIDC/assertion login 与最小 SCIM 同步入口基础上，补齐可落地的企业 IAM 最小闭环（SAML 登录闭环或 SCIM 目录同步深化二选一可验收里程碑），并增强风险策略与审计可观测字段。
- 验收标准：完成 `docs/21` 的 P1-01 目标交付；补齐 auth/identity 回归矩阵并可定向执行。
- 风险/依赖：依赖外部 IdP 与企业安全评审；风险为引入破坏性 contract 变更（需要先冻结字段语义并保持兼容）。
- 验证命令：

```bash
bun run test
bash ./scripts/test-identity-targeted.sh
```

#### BACKLOG-P1-02 DLP / Legal Hold / KMS 管理密钥（合规深化）

- 目标：在现有 Legal Hold 与 evidence bundle / audit export 的 DLP `off/redact/block` 最小治理基础上，补齐更完整敏感信息策略或 KMS 管理密钥能力的最小里程碑（建议先定一条主线）。
- 验收标准：完成 `docs/21` 的 P1-02 目标交付；DLP 覆盖面扩展具备可复现回归与证据包验签不回归。
- 风险/依赖：依赖密钥管理与合规策略评审；若新增配置项，需先同步到 `docs/13-环境变量参考.md` 再合入代码。
- 验证命令：

```bash
bun run test
bun run evidence:verify -- --file <bundle.json>
```

### 6.3 P2（长期蓝图/体验深化）

#### BACKLOG-P2-01 MCP 更复杂审批编排（企业编排器与图形化设计器）

- 目标：在现有 multi-stage + workflow 分支 + timeWindow + 持久化恢复基础上，补齐企业条件库与编排体验的一个可验收里程碑（建议优先“运行中节点可视化 + 条件库扩展”）。
- 验收标准：完成 `docs/21` 的 P2-01 目标交付；前后端定向回归可复现通过并回写 `docs/17`。
- 风险/依赖：依赖条件解释/回显语义稳定；风险为状态机复杂度上升导致回归覆盖不足。
- 验证命令：

```bash
bun run --cwd apps/control-plane test test/api.test.ts -t "mcp 路由：支持"
bun run --cwd apps/web-console test -- test/app.test.tsx -t "MCP|approvalStages|multi_stage"
```

#### BACKLOG-P2-02 Store-and-Forward / Policy Agent / Updater / 分发链路（企业 C/S 完整体）

- 目标：在现有“本地持久队列 + status 观测 + config packages MVP + updater MVP + 静默安装模板 + 分发包组装验收”基础上，补齐企业分发链路与后台常驻升级守护的一个可验收里程碑（优先自动升级守护与失败回滚编排）。
- 验收标准：完成 `docs/21` 的 P2-02 目标交付；新增“配置下发/升级/回滚”E2E 验收脚本，并在 `docs/17` 增加可复现记录。
- 风险/依赖：依赖 rollout 策略、签名校验与回滚策略的兼容；风险为破坏现有分步手工升级模式（需要明确兼容策略）。
- 验证命令：

```bash
go test ./clients/agent
bun run smoke:fr505

bun run package:agent-distribution
bun run verify:agent-distribution
```

#### BACKLOG-P2-03 多环境 / 多集群 / SaaS 多租户部署形态（IaC 与运维资产）

- 目标：在现有部署基线文档与预检脚本基础上，补齐“可复制的部署资产”最小集合（目录结构约定、环境分层样例、发布门禁配置样例与 smoke 入口）。
- 验收标准：完成 `docs/21` 的 P2-03 目标交付；部署样例与预检 profile 可复现，并具备跨服务 smoke 入口（至少 staging 形态）。
- 风险/依赖：依赖外部基础设施与 secret 管理；风险为把 IaC 当作“单仓短期开发”导致交付不可复现（需以资产与 smoke 为核心）。
- 验证命令：

```bash
bun run ./scripts/check-deployment-baseline.ts --profile governance --env-file ./.env
bun run ./scripts/check-deployment-baseline.ts --profile integration --env-file ./.env
bun run smoke:fr505
```

---

## 7. 维护规则（避免文档漂移）

- 新增能力或改语义：先代码与测试，再同步 `docs/15` 与 `docs/16`，最后在 `docs/17` 增加可复现命令与结果。
- 对外契约变更：不得在 `v1 frozen` 上做破坏性修改；必须升版本并重新评审（见 `docs/integration/TOKENPULSE_AGENTLEDGER_V1.md`）。
- 任意 Backlog 条目完成：按 `docs/21` 的 DoD 回写矩阵状态与回归证据，避免“文档胜出但代码不可复现”。

