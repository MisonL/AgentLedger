#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "执行 Go 编译检查..."
go build ./...

echo "构建 Web 控制台..."
bun run --cwd apps/web-console build

echo "build 完成。"
