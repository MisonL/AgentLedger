#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[integration-e2e] 运行 integration 真实消费分发 E2E..."
go test ./services/integration -run '^TestIntegrationE2E' -count=1 -v

echo "[integration-e2e] integration 真实消费分发 E2E 通过。"
