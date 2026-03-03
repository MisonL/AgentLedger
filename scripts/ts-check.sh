#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TS_CHECKED=0

while IFS= read -r config; do
  [ -z "$config" ] && continue
  TS_CHECKED=1
  echo ">>> 使用 ${config} 执行 tsc --noEmit"
  if bun run --silent tsc --version >/dev/null 2>&1; then
    bun run tsc --noEmit -p "$config"
  else
    bun x tsc --noEmit -p "$config"
  fi
done < <(
  find . -type f \
    \( -name "tsconfig.json" -o -name "tsconfig.*.json" \) \
    -not -path "./.git/*" \
    -not -path "./docs/*" \
    -not -path "./node_modules/*" \
    | sort
)

if [ "$TS_CHECKED" -eq 0 ]; then
  echo "未发现 tsconfig 文件，跳过 TypeScript 检查。"
  exit 0
fi
