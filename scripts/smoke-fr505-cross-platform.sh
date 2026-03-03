#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

pick_free_port() {
  bun -e 'import { createServer } from "node:net";
const server = createServer();
server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  if (!address || typeof address === "string") {
    process.exitCode = 1;
    server.close();
    return;
  }
  process.stdout.write(String(address.port));
  server.close();
});'
}

CONTROL_PLANE_PORT="${FR505_CONTROL_PLANE_PORT:-$(pick_free_port)}"
MOCK_INGEST_PORT="${FR505_MOCK_INGEST_PORT:-$(pick_free_port)}"
if [[ "$CONTROL_PLANE_PORT" == "$MOCK_INGEST_PORT" ]]; then
  MOCK_INGEST_PORT="$(pick_free_port)"
fi
WAIT_TIMEOUT_MS="${FR505_SMOKE_TIMEOUT_MS:-20000}"
WAIT_INTERVAL_MS="${FR505_SMOKE_INTERVAL_MS:-250}"

AGENT_OUT_DIR="$ROOT_DIR/dist/agent"
AGENT_BIN="$AGENT_OUT_DIR/agent-smoke-native"
if [[ "$(go env GOOS)" == "windows" ]]; then
  AGENT_BIN="${AGENT_BIN}.exe"
fi

CONTROL_PLANE_LOG="$(mktemp)"
MOCK_INGEST_LOG="$(mktemp)"
CONTROL_PLANE_PID=""
MOCK_INGEST_PID=""

cleanup() {
  if [[ -n "$MOCK_INGEST_PID" ]] && kill -0 "$MOCK_INGEST_PID" 2>/dev/null; then
    kill "$MOCK_INGEST_PID" 2>/dev/null || true
    wait "$MOCK_INGEST_PID" 2>/dev/null || true
  fi
  if [[ -n "$CONTROL_PLANE_PID" ]] && kill -0 "$CONTROL_PLANE_PID" 2>/dev/null; then
    kill "$CONTROL_PLANE_PID" 2>/dev/null || true
    wait "$CONTROL_PLANE_PID" 2>/dev/null || true
  fi
}

show_logs_on_error() {
  echo "FR-505 冒烟失败，输出最近日志："
  if [[ -f "$CONTROL_PLANE_LOG" ]]; then
    echo "--- control-plane 日志 ---"
    tail -n 80 "$CONTROL_PLANE_LOG" || true
  fi
  if [[ -f "$MOCK_INGEST_LOG" ]]; then
    echo "--- mock-ingestion 日志 ---"
    tail -n 80 "$MOCK_INGEST_LOG" || true
  fi
}

trap show_logs_on_error ERR
trap cleanup EXIT

echo "[1/6] 启动 control-plane 并等待健康检查..."
PORT="$CONTROL_PLANE_PORT" bun --cwd apps/control-plane src/index.ts >"$CONTROL_PLANE_LOG" 2>&1 &
CONTROL_PLANE_PID=$!

FR505_WAIT_URL="http://127.0.0.1:${CONTROL_PLANE_PORT}/api/v1/health" \
FR505_WAIT_EXPECT_STATUS="ok" \
FR505_WAIT_TIMEOUT_MS="$WAIT_TIMEOUT_MS" \
FR505_WAIT_INTERVAL_MS="$WAIT_INTERVAL_MS" \
  bun ./scripts/wait-http-json.ts

echo "[2/6] 启动 ingestion mock server..."
FR505_MOCK_INGEST_PORT="$MOCK_INGEST_PORT" bun ./scripts/mock-ingestion-smoke.ts >"$MOCK_INGEST_LOG" 2>&1 &
MOCK_INGEST_PID=$!

echo "[3/6] 等待 ingestion mock server 就绪..."
FR505_WAIT_URL="http://127.0.0.1:${MOCK_INGEST_PORT}/healthz" \
FR505_WAIT_EXPECT_STATUS="ok" \
FR505_WAIT_TIMEOUT_MS="$WAIT_TIMEOUT_MS" \
FR505_WAIT_INTERVAL_MS="$WAIT_INTERVAL_MS" \
  bun ./scripts/wait-http-json.ts

echo "[4/6] 构建本机 agent 二进制..."
if [[ ! -f "$ROOT_DIR/packages/gen/go/ingestion/v1/ingestion.pb.go" ]]; then
  echo "未检测到 Go Proto 生成代码，先执行 proto 生成..."
  bash "$ROOT_DIR/scripts/proto-gen.sh"
fi
mkdir -p "$AGENT_OUT_DIR"
go build -trimpath -o "$AGENT_BIN" ./clients/agent

echo "[5/6] 校验 agent version..."
version_output="$("$AGENT_BIN" version --short)"
if [[ -z "${version_output// }" ]]; then
  echo "agent version --short 输出为空"
  exit 1
fi
echo "agent version --short => $version_output"

echo "[6/6] 校验 agent run 最小上报..."
run_output=""
run_status=""
run_accepted=""
run_rejected=""
run_success=0

for attempt in 1 2 3; do
  run_exit_code=0
  if run_output="$("$AGENT_BIN" run \
    --endpoint "http://127.0.0.1:${MOCK_INGEST_PORT}/v1/ingest" \
    --generate 1 \
    --timeout 5s \
    2>&1)"; then
    run_exit_code=0
  else
    run_exit_code=$?
  fi
  echo "$run_output"

  run_status="$(printf '%s\n' "$run_output" | grep -Eo 'status=[0-9]+' | tail -n 1 | cut -d'=' -f2 || true)"
  run_accepted="$(printf '%s\n' "$run_output" | grep -Eo 'accepted=[0-9]+' | tail -n 1 | cut -d'=' -f2 || true)"
  run_rejected="$(printf '%s\n' "$run_output" | grep -Eo 'rejected=[0-9]+' | tail -n 1 | cut -d'=' -f2 || true)"

  if [[ "$run_exit_code" -eq 0 ]] &&
    [[ "$run_status" =~ ^2[0-9][0-9]$ ]] &&
    [[ "$run_accepted" =~ ^[0-9]+$ ]] &&
    [[ "$run_rejected" =~ ^[0-9]+$ ]] &&
    [[ "$run_accepted" -eq 1 ]] &&
    [[ "$run_rejected" -eq 0 ]]; then
    run_success=1
    break
  fi

  if [[ "$attempt" -lt 3 ]]; then
    sleep "$attempt"
  fi
done

if [[ "$run_success" -ne 1 ]]; then
  echo "agent run 校验失败: status=${run_status:-n/a} accepted=${run_accepted:-n/a} rejected=${run_rejected:-n/a}"
  exit 1
fi

echo "FR-505 三平台最小冒烟通过。"
