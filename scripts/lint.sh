#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "执行 TypeScript 检查..."
bash ./scripts/ts-check.sh
echo "lint 完成。"
