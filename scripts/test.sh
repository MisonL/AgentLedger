#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "执行 Go 测试..."
bash ./scripts/go-test-all.sh

echo "执行控制面测试..."
bun run --cwd apps/control-plane test

echo "执行 Web 控制台测试..."
bun run --cwd apps/web-console test

echo "test 完成。"
