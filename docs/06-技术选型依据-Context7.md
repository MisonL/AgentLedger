# AgentLedger 技术选型依据（Context7）

- 文档版本：v1.0
- 更新时间：2026-03-01

## 1. 目标

本文件用于记录 AgentLedger 前置阶段的关键技术选型依据，避免后续实现偏离。

## 2. 选型结论

## 2.1 Bun（后端运行时）

结论：采用 Bun 作为后端与脚本主运行时。

依据（Context7 摘要）：

1. 支持 `Bun.spawn()` 执行子进程并读取 stdout/stderr 流，适配 SSH 实时读取与同步任务模式。
2. 支持 `bun:sqlite` 与 WAL 模式，适合本地分析型存储。
3. 提供一体化工具链（runtime、test、package manager）。

官方参考：

1. https://bun.sh/docs/runtime/child-process
2. https://bun.sh/docs/guides/process/spawn
3. https://bun.sh/docs/runtime/sqlite

## 2.2 Hono（API 框架）

结论：采用 Hono 构建 API 层。

依据（Context7 摘要）：

1. Hono 支持 Bun 运行时适配。
2. 中间件体系完整，便于接入鉴权、校验、限流。
3. 可提供 WebSocket 能力，便于实时查询反馈。

官方参考：

1. https://hono.dev/docs/getting-started/bun
2. https://hono.dev/docs/guides/validation
3. https://hono.dev/docs/helpers/websocket

## 2.3 TanStack Query（前端数据层）

结论：采用 TanStack Query 管理前端数据请求与缓存。

依据（Context7 摘要）：

1. 提供查询缓存、失效刷新（invalidate）机制。
2. 支持预取与后台更新，适配统计看板高频切换场景。
3. 适合多过滤条件下的服务端状态管理。

官方参考：

1. https://tanstack.com/query/v5/docs
2. https://tanstack.com/query/v5/docs/reference/QueryClient

## 3. 与需求的映射关系

1. 远程访问模式（FR-003）：Bun `spawn + stream` 支撑实时/同步/混合调用。
2. 远程查询回退（FR-204）：Hono API + WebSocket 可回传实时结果与缓存新鲜度。
3. 报表切换性能（FR-301~FR-304）：TanStack Query 缓存与失效策略支撑。

## 4. 实施注意事项

1. Bun 子进程调用必须加超时、输出上限、命令白名单。
2. SQLite 默认启用 WAL，并设置合理 checkpoint 策略。
3. 前端 Query Key 规范化，防止缓存污染。
