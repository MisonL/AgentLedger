# TokenPulse × AgentLedger v1 联调 Runbook

本文档是联调操作手册，契约基线以 `v1 frozen` 文档为准：

- `docs/integration/TOKENPULSE_AGENTLEDGER_V1.md`

## 1. 接口与响应语义

### 1.1 入站 Webhook

- `POST /api/v1/integrations/tokenpulse/runtime-events`

返回码语义（必须与基线一致）：

- `202`：首次成功接收，且已完成幂等登记与持久化保存
- `200`：幂等命中，且可确认该事件已在去重窗口内完成持久化保存

### 1.2 联查查询

- `GET /api/v1/integrations/tokenpulse/runtime-events`

说明：

- 该接口需要租户内鉴权（owner/maintainer 可写入；成员可读联查），用于联调验证与排障。

## 2. AgentLedger 侧启用条件（control-plane）

必须配置环境变量：

- `AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET`：共享签名密钥，未配置时 `POST` 返回 `500`

可选配置：

- `AGENTLEDGER_TOKENPULSE_WEBHOOK_KEY_ID`：默认 `tokenpulse-runtime-v1`

本地启动（示例）：

```bash
cd apps/control-plane
export AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET='tp_agl_v1_shared_secret'
export AGENTLEDGER_TOKENPULSE_WEBHOOK_KEY_ID='tokenpulse-runtime-v1'
bun run dev
```

## 3. 发送一条最小事件（curl 示例）

注意：签名覆盖 `raw-request-body`，必须对将要发送的**原始请求体**计算签名，不可在签名后再格式化 JSON。

```bash
BASE_URL='http://127.0.0.1:8787' # 按实际 control-plane 地址修改
SECRET='tp_agl_v1_shared_secret'
SPEC_VERSION='v1'
KEY_ID='tokenpulse-runtime-v1'
TIMESTAMP="$(date +%s)"

TENANT_ID='default'
TRACE_ID='trace-oauth-runtime-20260308-0001'
PROVIDER='claude'
MODEL='claude-sonnet'
STARTED_AT='2026-03-08T09:59:58.123Z'

# canonical_json（键顺序与基线一致，且不插入额外空白）
IDEMPOTENCY_PAYLOAD="$(printf '{\"tenantId\":\"%s\",\"traceId\":\"%s\",\"provider\":\"%s\",\"model\":\"%s\",\"startedAt\":\"%s\"}' \
  "$TENANT_ID" "$TRACE_ID" "$PROVIDER" "$MODEL" "$STARTED_AT")"
IDEMPOTENCY_KEY="$(printf '%s' "$IDEMPOTENCY_PAYLOAD" | openssl dgst -sha256 | sed 's/^.*= //')"

BODY="$(printf '{\"tenantId\":\"%s\",\"traceId\":\"%s\",\"provider\":\"%s\",\"model\":\"%s\",\"resolvedModel\":\"%s\",\"routePolicy\":\"%s\",\"status\":\"%s\",\"startedAt\":\"%s\"}' \
  "$TENANT_ID" "$TRACE_ID" "$PROVIDER" "$MODEL" 'claude:claude-3-7-sonnet-20250219' 'latest_valid' 'success' "$STARTED_AT")"

SIGNATURE_HEX="$(printf '%s\n%s\n%s\n%s\n%s' "$SPEC_VERSION" "$KEY_ID" "$TIMESTAMP" "$IDEMPOTENCY_KEY" "$BODY" \
  | openssl dgst -sha256 -hmac "$SECRET" | sed 's/^.*= //')"
SIGNATURE="sha256=$SIGNATURE_HEX"

curl -i -sS "$BASE_URL/api/v1/integrations/tokenpulse/runtime-events" \
  -H 'content-type: application/json' \
  -H "x-tokenpulse-spec-version: $SPEC_VERSION" \
  -H "x-tokenpulse-key-id: $KEY_ID" \
  -H "x-tokenpulse-timestamp: $TIMESTAMP" \
  -H "x-tokenpulse-idempotency-key: $IDEMPOTENCY_KEY" \
  -H "x-tokenpulse-signature: $SIGNATURE" \
  --data "$BODY"
```

预期：

- 首次发送返回 `202`，响应体 `duplicate=false`
- 用相同的 `X-TokenPulse-Idempotency-Key` 重发，返回 `200`，响应体 `duplicate=true`

## 4. 常见错误与排障

### 4.1 401：signature 无效

常见原因：

- `X-TokenPulse-Timestamp` 超出 `±300s`
- `X-TokenPulse-Key-Id` 与 AgentLedger 侧期望值不一致
- 签名使用的 body 与实际发送的 raw body 不一致（例如多了空格、换行、字段顺序变了）

### 4.2 400：idempotency-key 与请求体不一致

常见原因：

- 幂等键不是按基线的 `canonical_json`（键顺序固定、无空白）生成
- `startedAt` 取值与 body 中不一致（包括毫秒/时区差异）

### 4.3 500：未配置 TokenPulse webhook secret

- AgentLedger 侧未设置 `AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET`

## 5. 联调验收清单（最小）

- `202` 首次落盘成功：写入后可在联查接口与控制台面板中查到
- `200` 幂等命中：重放同一幂等键不会产生重复入账
- `401` 验签失败：修改 signature/key-id/timestamp 任一项应拒绝
- `400` 幂等键不一致：header 与 body 不一致应拒绝

## 6. 回滚与止血

1. TokenPulse 侧：暂停投递（或将投递地址切到黑洞环境）以止血。
2. AgentLedger 侧：移除 `AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET` 后入站接口会返回 `500`，用于紧急关闭接入。

