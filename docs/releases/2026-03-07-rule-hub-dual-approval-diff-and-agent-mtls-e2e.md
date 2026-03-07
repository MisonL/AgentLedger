# AgentLedger 发布说明：Rule Hub 双人审批 / 版本 Diff 与 Agent gRPC mTLS 验证

- 日期：2026-03-07
- 状态：发布候选说明
- 适用范围：`apps/control-plane`、`apps/web-console`、`clients/agent`、`docs`

## 1. 本轮目标

本轮交付重点不是继续扩散能力面，而是把三条此前仍有缺口或缺验证入口的主线收口成可回归、可交接的稳定事实：

1. Rule Hub 企业治理增强
   - 补齐双人审批门槛
   - 补齐版本 diff 查询与控制台展示
   - 修正审批重复提交时的审计口径，区分“创建”与“更新”
2. Agent Push gRPC mTLS 本地端到端验证
   - 不只保留参数支持和单测
   - 补一条真实握手、真实请求、真实响应的本地 Go E2E
3. 规划与回归事实固化
   - 把 `docs/11`、`docs/15`、`docs/16`、`docs/17`、`docs/18` 同步到同一口径
   - 把仍待开发 / 待补强项归档到明确 backlog

## 2. 重点变化

### 2.1 Rule Hub：双人审批与版本 diff

- 规则资产新增 `requiredApprovals`
  - 当前只支持 `1` 或 `2`
  - 默认值为 `1`
- `POST /api/v1/rules/assets/:id/publish`
  - 在发布前先校验目标版本是否存在
  - 若资产要求双人审批，则继续校验该版本 `approved` 审批人数是否达标
- `GET /api/v1/rules/assets/:id/versions/diff?fromVersion=&toVersion=`
  - 返回行级 diff 与 summary
  - 控制台治理页支持默认填充 diff 版本号并直接渲染对比结果
- 审批审计链路
  - 首次审批写入 `control_plane.rule_approval_created`
  - 同一审批人对同一资产版本重复提交时写入 `control_plane.rule_approval_updated`
  - 避免把“更新审批”伪造为“新建审批”

### 2.2 Agent Push：gRPC mTLS 本地 Go E2E

- 新增测试入口：`clients/agent/run_grpc_e2e_test.go`
- 该测试会临时生成：
  - CA
  - 服务端证书
  - 客户端证书
- 测试中会真实启动一个要求 mTLS 的本地 gRPC server，并通过 agent 侧 `sendIngestRequestGRPC()` 验证：
  - mTLS 握手成功
  - 服务端能校验客户端证书
  - `authorization` metadata 可透传
  - 请求能拿到正确的 `PushBatchResponse`

### 2.3 文档与 backlog 收口

- `docs/11-企业级增强功能规划.md`
  - 已明确 Rule Hub 双人审批与版本 diff 已从规划项转为已实现项
- `docs/15-核心功能清单.md`
  - 已同步 Rule Hub 当前事实边界与审批审计口径
- `docs/16-全量规划功能核对矩阵.md`
  - 已同步 Rule Hub 双人审批 / 版本 diff、Agent gRPC mTLS E2E 的实现与验证状态
- `docs/17-回归验证执行记录.md`
  - 已记录 `bun run test`、`bun run build` 与 gRPC mTLS 定向命令
- `docs/18-剩余缺口清单.md`
  - 已把剩余项分成“待补强项 / 待开发项 / 实施顺序”
  - 当前 backlog 不再混入已完成能力

## 3. 兼容性与升级提醒

1. `requiredApprovals` 的当前边界只有 `1` / `2`
   - 历史资产会按默认 `1` 人审批口径收敛
   - 本轮未引入更复杂的 N 人审批模型

2. 发布门槛只作用于 `publish`
   - `rollback` 仍保持当前实现，不额外卡审批人数
   - 这是有意保守收口，避免改变既有回滚运维路径

3. 版本 diff 当前是轻量行级对比
   - 不引入额外 diff 依赖
   - 适合治理页查看规则文本变更
   - 更复杂的实验室级对比仍归在后续 backlog

## 4. 建议验收

```bash
bun run test
bun run build
go test ./clients/agent -run TestSendIngestRequestGRPC_MTLSEndToEnd -count=1
```

其中：

- `bun run test` 已覆盖 Go 全量、control-plane 全量、web-console 全量
- `bun run build` 已覆盖 `go build ./...` 与 web-console 生产构建
- gRPC mTLS 定向命令用于给 Agent Push 本地端到端验证留一个明确入口

## 5. 后续开发入口

本轮不会把长期蓝图误记成“回归失败”。仍需继续推进的项，统一以 `docs/18-剩余缺口清单.md` 为准，优先级如下：

1. 第一批：告警编排中心工单联动、SLA 升级链、数据主权 KMS / 区域归档映射
2. 第二批：MCP 更复杂审批编排、quality 高级版预测与建议、replay 实验室完全体
3. 第三批：Agent 生命周期审计、SCIM / SAML / MFA / 风险登录、DLP / Legal Hold
4. 第四批：Store-and-Forward、本地持久队列、远程配置、Updater、静默安装、多环境多集群部署
