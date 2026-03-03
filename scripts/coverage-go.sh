#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COVERAGE_DIR="${ROOT_DIR}/.coverage"
GO_COVERAGE_PROFILE="${COVERAGE_DIR}/go-cover.out"
GO_COVERAGE_FUNC_REPORT="${COVERAGE_DIR}/go-cover-func.txt"

mkdir -p "${COVERAGE_DIR}"

if [ ! -f "${ROOT_DIR}/packages/gen/go/ingestion/v1/ingestion.pb.go" ]; then
  echo "[coverage-go] 未检测到 Go Proto 生成代码，先执行 proto 生成..."
  bash "${ROOT_DIR}/scripts/proto-gen.sh"
fi

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
