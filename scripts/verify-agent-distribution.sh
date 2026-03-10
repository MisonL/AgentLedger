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

hash_file() {
  local file_path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file_path" | awk '{print $1}'
    return 0
  fi
  shasum -a 256 "$file_path" | awk '{print $1}'
}

verify_checksums() {
  local bundle_dir="$1"
  local checksum_file="${bundle_dir}/SHA256SUMS.txt"
  while read -r sum relative_path; do
    [[ -z "${sum:-}" ]] && continue
    local target_path="$relative_path"
    if [[ ! -f "$target_path" ]]; then
      target_path="${bundle_dir}/${relative_path}"
    fi
    if [[ ! -f "$target_path" ]]; then
      echo "校验失败，缺失文件: ${relative_path}"
      exit 1
    fi
    local actual
    actual="$(hash_file "$target_path")"
    if [[ "$actual" != "$sum" ]]; then
      echo "校验失败，哈希不匹配: ${relative_path}"
      exit 1
    fi
  done <"$checksum_file"
}

HOST_OS="$(go env GOOS)"
HOST_ARCH="$(go env GOARCH)"
BUNDLE_DIR="${ROOT_DIR}/dist/agent-distribution/${HOST_OS}/${HOST_ARCH}"
PACKAGE_DIR="${BUNDLE_DIR}/package"

if [[ ! -d "$BUNDLE_DIR" ]]; then
  echo "未检测到 ${HOST_OS}/${HOST_ARCH} 分发包，先执行组装..."
  bash "$ROOT_DIR/scripts/package-agent-distribution.sh"
fi

if [[ "$HOST_OS" == "windows" ]]; then
  echo "当前 verify-agent-distribution.sh 仅支持在 Unix-like 主机执行。"
  exit 1
fi

BINARY_PATH="${PACKAGE_DIR}/agent"
if [[ ! -x "$BINARY_PATH" ]]; then
  echo "缺少本机分发二进制: ${BINARY_PATH}"
  exit 1
fi

for required_file in "SHA256SUMS.txt" "release-manifest.json" "agent-${HOST_OS}-${HOST_ARCH}.tar.gz"; do
  if [[ ! -f "${BUNDLE_DIR}/${required_file}" ]]; then
    echo "缺少分发文件: ${required_file}"
    exit 1
  fi
done

for required_file in ".env.example" "AGENT_RELEASE_SIGNING_PUBLIC_KEY.pem.example" "SHA256SUMS.txt"; do
  if [[ ! -f "${PACKAGE_DIR}/${required_file}" ]]; then
    echo "缺少 package 文件: ${required_file}"
    exit 1
  fi
done

verify_checksums "$PACKAGE_DIR"
verify_checksums "$BUNDLE_DIR"
verify_checksums "${ROOT_DIR}/dist/agent-distribution"

TMP_DIR="$(mktemp -d)"
PUBLIC_KEY_PATH="${TMP_DIR}/agent-release-public.pem"
TOKEN_FILE="${TMP_DIR}/token.json"
CONFIG_DIR="${TMP_DIR}/config"
QUEUE_DIR="${TMP_DIR}/queue"
PORT="${AGENT_DISTRIBUTION_VERIFY_PORT:-$(pick_free_port)}"
MOCK_LOG="$(mktemp)"
MOCK_PID=""

cleanup() {
  if [[ -n "$MOCK_PID" ]] && kill -0 "$MOCK_PID" 2>/dev/null; then
    kill "$MOCK_PID" 2>/dev/null || true
    wait "$MOCK_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

cat >"$TOKEN_FILE" <<'EOF'
{
  "access_token": "distribution-token",
  "token_type": "Bearer",
  "expires_at": "2099-01-01T00:00:00Z"
}
EOF

bun ./scripts/mock-agent-distribution-update.ts \
  --port="$PORT" \
  --public-key-out="$PUBLIC_KEY_PATH" \
  --os="$HOST_OS" \
  --arch="$HOST_ARCH" >"$MOCK_LOG" 2>&1 &
MOCK_PID=$!

FR505_WAIT_URL="http://127.0.0.1:${PORT}/healthz" \
FR505_WAIT_EXPECT_STATUS="ok" \
FR505_WAIT_TIMEOUT_MS="15000" \
FR505_WAIT_INTERVAL_MS="200" \
  bun ./scripts/wait-http-json.ts

"$BINARY_PATH" version --short
"$BINARY_PATH" status --config-dir "$CONFIG_DIR" --queue-dir "$QUEUE_DIR" >/dev/null
"$BINARY_PATH" update check \
  --gateway "http://127.0.0.1:${PORT}" \
  --token-file "$TOKEN_FILE" \
  --current-version "0.1.0" \
  --os "$HOST_OS" \
  --arch "$HOST_ARCH" >/dev/null
"$BINARY_PATH" update download \
  --gateway "http://127.0.0.1:${PORT}" \
  --token-file "$TOKEN_FILE" \
  --config-dir "$CONFIG_DIR" \
  --current-version "0.1.0" \
  --os "$HOST_OS" \
  --arch "$HOST_ARCH" \
  --signature-public-key-file "$PUBLIC_KEY_PATH" >/dev/null

STATUS_OUTPUT="$("$BINARY_PATH" update status --config-dir "$CONFIG_DIR")"
printf '%s\n' "$STATUS_OUTPUT"
if [[ "$STATUS_OUTPUT" != *'"status": "downloaded"'* ]]; then
  echo "update status 未进入 downloaded 状态"
  exit 1
fi
if [[ "$STATUS_OUTPUT" != *'"downloaded_signature_status": "verified"'* ]]; then
  echo "update status 未显示 signature verified"
  exit 1
fi

echo "Agent 分发包验收通过: ${HOST_OS}/${HOST_ARCH}"
