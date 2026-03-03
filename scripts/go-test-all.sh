#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ ! -f "$ROOT_DIR/packages/gen/go/ingestion/v1/ingestion.pb.go" ]; then
  echo "未检测到 Go Proto 生成代码，先执行 proto 生成..."
  bash "$ROOT_DIR/scripts/proto-gen.sh"
fi

GO_MODULE_TESTED=0

while IFS= read -r mod; do
  [ -z "$mod" ] && continue
  GO_MODULE_TESTED=1
  mod_dir="$(dirname "$mod")"
  echo ">>> 在 ${mod_dir#./} 执行 go test ./..."
  (
    cd "$mod_dir"
    go test ./...
  )
done < <(
  find . -type f -name "go.mod" \
    -not -path "./.git/*" \
    -not -path "./docs/*" \
    -not -path "./node_modules/*" \
    | sort
)

if [ "$GO_MODULE_TESTED" -eq 0 ]; then
  echo "未发现 go.mod 文件，跳过 Go 测试。"
  exit 0
fi
