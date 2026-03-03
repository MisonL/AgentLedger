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

mkdir -p packages/gen/go packages/gen/ts

echo "执行 Proto 代码生成（buf generate）..."
if ! "${BUF_CMD[@]}" generate packages/proto --template buf.gen.yaml; then
  echo "buf generate 执行失败。"
  echo "若报错来自 TS 插件拉取，可先在 buf.gen.yaml 注释 TS 插件后重试。"
  exit 1
fi

echo "proto generate 完成。"
