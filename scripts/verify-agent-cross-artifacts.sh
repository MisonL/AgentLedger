#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="$ROOT_DIR/dist/agent"
declare -a REQUIRED_ARTIFACTS=(
  "agent-linux-amd64"
  "agent-linux-arm64"
  "agent-darwin-amd64"
  "agent-darwin-arm64"
  "agent-windows-amd64.exe"
  "agent-windows-arm64.exe"
)

missing_count=0

echo "检查跨平台 Agent 构建产物..."
for artifact in "${REQUIRED_ARTIFACTS[@]}"; do
  artifact_path="${OUT_DIR}/${artifact}"
  if [[ ! -f "$artifact_path" ]]; then
    echo "缺失产物: ${artifact}"
    missing_count=$((missing_count + 1))
    continue
  fi

  size_bytes="$(wc -c < "$artifact_path" | tr -d ' ')"
  echo " - ${artifact} (${size_bytes} bytes)"

  if [[ "$artifact" != *.exe ]] && [[ ! -x "$artifact_path" ]]; then
    echo "产物不可执行: ${artifact}"
    missing_count=$((missing_count + 1))
  fi
done

if [[ "$missing_count" -ne 0 ]]; then
  echo "跨平台 Agent 产物检查失败，问题数: ${missing_count}"
  exit 1
fi

echo "跨平台 Agent 产物检查通过。"
