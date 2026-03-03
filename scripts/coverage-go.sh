#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COVERAGE_DIR="${ROOT_DIR}/.coverage"
GO_COVERAGE_PROFILE="${COVERAGE_DIR}/go-cover.out"
GO_COVERAGE_FUNC_REPORT="${COVERAGE_DIR}/go-cover-func.txt"

mkdir -p "${COVERAGE_DIR}"

echo "[coverage-go] 生成 Go 覆盖率..."
(
  cd "${ROOT_DIR}"
  go test ./... -covermode=count -coverprofile="${GO_COVERAGE_PROFILE}"
)

echo "[coverage-go] 导出 go tool cover 报告..."
go tool cover -func="${GO_COVERAGE_PROFILE}" > "${GO_COVERAGE_FUNC_REPORT}"

echo "[coverage-go] 完成。"
echo "[coverage-go] profile: ${GO_COVERAGE_PROFILE}"
echo "[coverage-go] summary: ${GO_COVERAGE_FUNC_REPORT}"
