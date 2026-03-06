#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

THRESHOLD="${PULLER_P0_GOLDEN_ACCURACY_THRESHOLD:-99}"

echo "[puller-p0-gate] 执行 P0 解析准确率门禁，阈值 >= ${THRESHOLD}%"
bun run ./scripts/puller-p0-accuracy.ts --threshold "${THRESHOLD}"
echo "[puller-p0-gate] 门禁通过。"
