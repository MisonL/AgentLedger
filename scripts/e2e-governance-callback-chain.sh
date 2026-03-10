#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TOTAL_STEPS=5
STEP=0
announce_step() {
  STEP=$((STEP + 1))
  echo "[${STEP}/${TOTAL_STEPS}] $1"
}

announce_step "校验 callback stream 绑定..."
bash ./scripts/check-callback-stream-binding.sh

announce_step "运行 governance 关键规则测试..."
go test ./services/governance -run 'Threshold|Scope|Freeze|Budget'

announce_step "运行 integration routing E2E..."
bash ./scripts/e2e-integration-routing.sh

announce_step "运行 integration callback 分发测试..."
go test ./services/integration -run 'HandleCallbackMessage|CallbackHTTPHandler|LoadIntegrationConfig'

announce_step "运行 control-plane callback 闭环测试..."
bun run --cwd apps/control-plane test -- --test-name-pattern 'integrations/callbacks/alerts'

echo "governance -> integration -> control-plane 路由/回调闭环冒烟通过。"
