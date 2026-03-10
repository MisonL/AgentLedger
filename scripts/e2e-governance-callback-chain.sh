#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CURRENT_STEP=""

begin_group() {
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    echo "::group::$1"
  else
    echo ""
    echo "==> $1"
  fi
}

end_group() {
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    echo "::endgroup::"
  fi
}

on_err() {
  local exit_code=$?
  echo "[callback-chain] 失败于步骤: ${CURRENT_STEP:-未知} (exit=${exit_code})" >&2
  exit "$exit_code"
}
trap on_err ERR

run_step() {
  local step_id="$1"
  local step_desc="$2"
  shift 2

  CURRENT_STEP="$step_id"
  begin_group "callback-chain: ${step_id} | ${step_desc}"
  "$@"
  end_group
}

run_step \
  "callback-stream-binding" \
  "校验 callback stream 绑定" \
  bash ./scripts/check-callback-stream-binding.sh

run_step \
  "governance-rules" \
  "运行 governance 关键规则测试" \
  go test -count=1 ./services/governance -run 'Threshold|Scope|Freeze|Budget'

run_step \
  "integration-routing-e2e" \
  "运行 integration routing E2E" \
  bash ./scripts/e2e-integration-routing.sh

run_step \
  "integration-callback-dispatch" \
  "运行 integration callback 分发测试" \
  go test -count=1 ./services/integration -run 'HandleCallbackMessage|CallbackHTTPHandler|LoadIntegrationConfig'

run_step \
  "control-plane-callback-loop" \
  "运行 control-plane callback 闭环测试" \
  bun run --cwd apps/control-plane test -- --test-name-pattern 'integrations/callbacks/alerts'

CURRENT_STEP="done"
echo "[callback-chain] governance -> integration -> control-plane 路由/回调闭环冒烟通过。"
