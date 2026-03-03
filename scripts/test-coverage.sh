#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "[test-coverage] 开始覆盖率流程..."
bash ./scripts/coverage-go.sh
bash ./scripts/coverage-control-plane.sh
bash ./scripts/check-coverage-threshold.sh
echo "[test-coverage] 完成。"
