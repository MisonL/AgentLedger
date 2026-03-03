#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="dist/agent"
MAIN_PKG="./clients/agent"

mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/agent-*

declare -a TARGETS=(
  "linux amd64 agent-linux-amd64"
  "linux arm64 agent-linux-arm64"
  "darwin amd64 agent-darwin-amd64"
  "darwin arm64 agent-darwin-arm64"
  "windows amd64 agent-windows-amd64.exe"
  "windows arm64 agent-windows-arm64.exe"
)

echo "开始构建 Agent 多平台产物..."
for target in "${TARGETS[@]}"; do
  read -r goos goarch output_name <<<"$target"
  output_path="${OUT_DIR}/${output_name}"

  echo "-> GOOS=${goos} GOARCH=${goarch} 输出=${output_path}"
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build -trimpath -o "$output_path" "$MAIN_PKG"
  echo "   已生成: ${output_path}"
done

echo "构建完成，产物清单："
for artifact in "$OUT_DIR"/agent-*; do
  if [[ -f "$artifact" ]]; then
    size_bytes="$(wc -c < "$artifact" | tr -d ' ')"
    echo " - $(basename "$artifact") (${size_bytes} bytes)"
  fi
done
