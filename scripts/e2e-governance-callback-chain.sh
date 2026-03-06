#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/5] 校验 callback stream 绑定..."
bash ./scripts/check-callback-stream-binding.sh

echo "[2/5] 运行 governance 关键规则测试..."
go test ./services/governance -run 'Threshold|Scope|Freeze|Budget'

echo "[3/5] 运行 integration routing E2E..."
bash ./scripts/e2e-integration-routing.sh

echo "[4/5] 运行 integration callback 分发测试..."
go test ./services/integration -run 'HandleCallbackMessage|CallbackHTTPHandler|LoadIntegrationConfig'

echo "[5/5] 运行 control-plane callback 闭环测试..."
bun run --cwd apps/control-plane test -- --test-name-pattern 'integrations/callbacks/alerts'

echo "governance -> integration -> control-plane 路由/回调闭环冒烟通过。"
