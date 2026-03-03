#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COVERAGE_DIR="${ROOT_DIR}/.coverage"
CONTROL_PLANE_COVERAGE_TEXT="${COVERAGE_DIR}/control-plane-coverage.txt"

mkdir -p "${COVERAGE_DIR}"

echo "[coverage-control-plane] 生成 control-plane 覆盖率..."
(
  cd "${ROOT_DIR}/apps/control-plane"
  # Bun 的覆盖率表默认输出到 stderr，需要并入文本报告供后续阈值脚本解析。
  NO_COLOR=1 bun test --coverage --coverage-reporter=text 2>&1
) | tee "${CONTROL_PLANE_COVERAGE_TEXT}"

echo "[coverage-control-plane] 完成。"
echo "[coverage-control-plane] report: ${CONTROL_PLANE_COVERAGE_TEXT}"
