# ingestion-gateway TLS/mTLS/OIDC 运行指引

- 文档版本：v1.0
- 更新时间：2026-03-07
- 适用组件：`services/ingestion-gateway`、`clients/agent`

## 1. OIDC 必填项（按场景）

### 1.1 服务启动必填

`ingestion-gateway` 启动时会初始化 JWT 验签器，以下变量缺一不可：

- `OIDC_ISSUER`
- `OIDC_AUDIENCE`
- `OIDC_JWKS_URI`

缺失任一项都会导致进程启动失败。

### 1.2 设备码代理接口必填

- `POST /v1/oidc/device/start` 需要 `OIDC_DEVICE_AUTH_ENDPOINT`，否则返回 `503`。
- `POST /v1/oidc/device/poll` 需要 `OIDC_TOKEN_ENDPOINT`，否则返回 `503`。
- `OIDC_CLIENT_ID` 未配置时，客户端请求体必须显式传 `client_id`。

## 2. gRPC TLS 默认行为

- `GRPC_TLS_ENABLED` 默认值是 `true`。
- 只要 TLS 开启，`GRPC_TLS_CERT_FILE` 和 `GRPC_TLS_KEY_FILE` 必须同时配置。
- `GRPC_MTLS_ENABLED` 默认值是 `false`。
- 开启 mTLS 时必须再配置 `GRPC_TLS_CLIENT_CA_FILE`。

## 3. TLS / mTLS 环境变量组合

### 3.1 TLS（单向认证）

```bash
NATS_URL=nats://127.0.0.1:4222
HTTP_ADDR=:8081
GRPC_ADDR=:9091

OIDC_ISSUER=https://idp.example.com/
OIDC_AUDIENCE=agentledger-ingest
OIDC_JWKS_URI=https://idp.example.com/.well-known/jwks.json
OIDC_DEVICE_AUTH_ENDPOINT=https://idp.example.com/oauth/device/code
OIDC_TOKEN_ENDPOINT=https://idp.example.com/oauth/token
OIDC_CLIENT_ID=agent-cli

GRPC_TLS_ENABLED=true
GRPC_TLS_CERT_FILE=/etc/agentledger/tls/server.crt
GRPC_TLS_KEY_FILE=/etc/agentledger/tls/server.key
GRPC_MTLS_ENABLED=false
```

### 3.2 mTLS（双向认证）

```bash
NATS_URL=nats://127.0.0.1:4222
HTTP_ADDR=:8081
GRPC_ADDR=:9091

OIDC_ISSUER=https://idp.example.com/
OIDC_AUDIENCE=agentledger-ingest
OIDC_JWKS_URI=https://idp.example.com/.well-known/jwks.json

GRPC_TLS_ENABLED=true
GRPC_TLS_CERT_FILE=/etc/agentledger/tls/server.crt
GRPC_TLS_KEY_FILE=/etc/agentledger/tls/server.key
GRPC_MTLS_ENABLED=true
GRPC_TLS_CLIENT_CA_FILE=/etc/agentledger/tls/client-ca.crt
```

## 4. agent run/doctor 在 gRPC 下的参数组合

### 4.1 合法组合

| 场景 | 必选参数 | 可选参数 |
| --- | --- | --- |
| 默认 TLS | `--protocol=grpc` | `--endpoint`（默认 `127.0.0.1:9091`）、`--grpc-ca-file`、`--grpc-server-name`、`--grpc-insecure-skip-verify` |
| gRPC mTLS 客户端证书 | `--protocol=grpc --grpc-cert-file <cert> --grpc-key-file <key>` | 同上 |
| gRPC 明文（仅开发） | `--protocol=grpc --grpc-plaintext` | `--endpoint` |

### 4.2 非法组合（会直接报参数错误）

- `--protocol=http` 同时传任意 `--grpc-*` 参数。
- `--grpc-plaintext` 与任何 TLS 相关参数同时使用（`--grpc-ca-file` / `--grpc-server-name` / `--grpc-cert-file` / `--grpc-key-file` / `--grpc-insecure-skip-verify`）。
- 只传 `--grpc-cert-file` 或只传 `--grpc-key-file`（必须成对）。
- endpoint 使用 `grpcs://` 且同时设置 `--grpc-plaintext`。

### 4.3 doctor 在 gRPC 模式的检查范围

- `doctor --protocol=grpc` 会检查：
  - token 文件状态；
  - gRPC 参数组合合法性；
  - endpoint 语法合法性；
  - 到 `host:port` 的 TCP 连通性。
- 当前不会执行 gRPC TLS 握手与证书链校验探测。

### 4.4 gRPC mTLS 端到端验证入口

- 已有本地 Go 端到端测试：`go test ./clients/agent -run TestSendIngestRequestGRPC_MTLSEndToEnd -count=1 -v`
- 该测试会临时生成 CA、服务端证书、客户端证书，启动要求 mTLS 的本地 gRPC server，并通过 `sendIngestRequestGRPC()` 验证：
  - mTLS 握手成功；
  - 服务端收到并校验客户端证书；
  - `authorization` metadata 能透传；
  - 请求能拿到 `PushBatchResponse` 响应。

## 5. 三平台命令示例

以下假设二进制在当前目录（`./agent` 或 `.\agent.exe`）。

### 5.1 macOS（zsh，TLS）

```bash
./agent run \
  --protocol=grpc \
  --endpoint=grpcs://ingest.example.com:9091 \
  --grpc-ca-file=/Users/you/certs/ca.crt \
  --token-file=/Users/you/.agentledger/token.json

./agent doctor \
  --protocol=grpc \
  --endpoint=grpcs://ingest.example.com:9091 \
  --grpc-ca-file=/Users/you/certs/ca.crt
```

### 5.2 Linux（bash，mTLS）

```bash
./agent run \
  --protocol=grpc \
  --endpoint=ingest.example.com:9091 \
  --grpc-ca-file=/etc/agentledger/ca.crt \
  --grpc-cert-file=/etc/agentledger/client.crt \
  --grpc-key-file=/etc/agentledger/client.key \
  --token-file=/home/agent/.agentledger/token.json

./agent doctor \
  --protocol=grpc \
  --endpoint=ingest.example.com:9091 \
  --grpc-ca-file=/etc/agentledger/ca.crt \
  --grpc-cert-file=/etc/agentledger/client.crt \
  --grpc-key-file=/etc/agentledger/client.key
```

### 5.3 Windows（PowerShell，明文联调）

```powershell
.\agent.exe run `
  --protocol=grpc `
  --endpoint=127.0.0.1:9091 `
  --grpc-plaintext `
  --token-file="$env:USERPROFILE\.agentledger\token.json"

.\agent.exe doctor `
  --protocol=grpc `
  --endpoint=127.0.0.1:9091 `
  --grpc-plaintext
```

## 6. integration webhook 与 DATABASE_URL 关系

- `services/integration` 默认通道是 `webhook`，因此最少需要 `INTEGRATION_WEBHOOK_URL`。
- `services/integration` 当前不依赖 `DATABASE_URL` 即可运行 webhook 分发链路。
- 需要数据库连接的是其他服务（如 normalizer/analytics/governance/puller/archiver）与控制面持久化场景。
