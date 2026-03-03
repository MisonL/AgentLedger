#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "执行 Go 代码格式化（gofmt）..."
GO_FILES="$(find . -type f -name '*.go' -not -path './vendor/*')"
if [[ -n "${GO_FILES}" ]]; then
  gofmt -w ${GO_FILES}
fi

echo "format 完成。"
