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
  local step_pos="$1"
  local step_id="$2"
  local step_desc="$3"
  shift 3

  CURRENT_STEP="[${step_pos}] ${step_id}"
  begin_group "callback-chain: [${step_pos}] ${step_id} | ${step_desc}"
  "$@"
  end_group
}

step_callback_stream_binding() {
  bash ./scripts/check-callback-stream-binding.sh
}

step_governance_rules() {
  go test -count=1 ./services/governance -run 'Threshold|Scope|Freeze|Budget'
}

step_integration_routing_e2e() {
  bash ./scripts/e2e-integration-routing.sh
}

step_integration_callback_dispatch() {
  go test -count=1 ./services/integration -run 'HandleCallbackMessage|CallbackHTTPHandler|LoadIntegrationConfig'
}

step_control_plane_callback_loop() {
  bun run --cwd apps/control-plane test -- --test-name-pattern 'integrations/callbacks/alerts'
}

STEP_IDS=(
  "callback-stream-binding"
  "governance-rules"
  "integration-routing-e2e"
  "integration-callback-dispatch"
  "control-plane-callback-loop"
)
STEP_DESCS=(
  "校验 callback stream 绑定"
  "运行 governance 关键规则测试"
  "运行 integration routing E2E"
  "运行 integration callback 分发测试"
  "运行 control-plane callback 闭环测试"
)
STEP_FNS=(
  "step_callback_stream_binding"
  "step_governance_rules"
  "step_integration_routing_e2e"
  "step_integration_callback_dispatch"
  "step_control_plane_callback_loop"
)

if [[ "${#STEP_IDS[@]}" -ne "${#STEP_DESCS[@]}" ]] || [[ "${#STEP_IDS[@]}" -ne "${#STEP_FNS[@]}" ]]; then
  echo "[callback-chain] 内部错误: STEP_IDS/STEP_DESCS/STEP_FNS 数组长度不一致。" >&2
  exit 1
fi

total_steps="${#STEP_IDS[@]}"
for i in "${!STEP_IDS[@]}"; do
  pos="$((i + 1))/${total_steps}"
  run_step "$pos" "${STEP_IDS[$i]}" "${STEP_DESCS[$i]}" "${STEP_FNS[$i]}"
done

CURRENT_STEP="done"
echo "[callback-chain] governance -> integration -> control-plane 路由/回调闭环冒烟通过。"
