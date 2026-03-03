#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/3] 检查 callback stream 绑定..."
bash ./scripts/check-callback-stream-binding.sh

echo "[2/3] 运行 integration callback 相关测试..."
go test ./services/integration -run 'Callback|LoadIntegrationConfig'

echo "[3/3] 运行 control-plane callback 相关测试..."
bun run --cwd apps/control-plane test -- --test-name-pattern 'integrations/callbacks/alerts'

echo "callback targeted 回归通过。"
