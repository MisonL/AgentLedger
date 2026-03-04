#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[identity-targeted] 执行 control-plane identity 定向测试..."
bun test apps/control-plane/test/api.test.ts -t "Identity"
echo "[identity-targeted] 测试通过。"
