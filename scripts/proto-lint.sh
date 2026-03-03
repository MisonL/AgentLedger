#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BUF_CMD=(buf)
if ! command -v buf >/dev/null 2>&1; then
  if command -v bunx >/dev/null 2>&1; then
    echo "未检测到系统 buf，改用 bunx 临时执行 @bufbuild/buf。"
    BUF_CMD=(bunx --bun @bufbuild/buf)
  else
    echo "错误：未找到 buf CLI，也未找到 bunx。"
    echo "请先安装 buf，或通过 Bun 安装 @bufbuild/buf。"
    exit 1
  fi
fi

echo "执行 Proto 契约 lint（buf lint）..."
"${BUF_CMD[@]}" lint --config buf.yaml packages/proto

echo "proto lint 完成。"
