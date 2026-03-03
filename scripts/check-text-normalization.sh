#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[text-normalization] 当前目录不是 Git 仓库，无法执行检查。" >&2
  exit 1
fi

echo "[text-normalization] 检查 Git 索引中的文本内容（CRLF / UTF-8 BOM）..."

# 使用 --cached 读取索引内容，避免 Windows 工作区换行符转换导致误报。
CRLF_MATCHES="$(git grep --cached -nI $'\r' -- . || true)"
BOM_MATCHES="$(git grep --cached -nI $'\xEF\xBB\xBF' -- . || true)"

FAILED=0

if [[ -n "${CRLF_MATCHES}" ]]; then
  FAILED=1
  echo "[text-normalization] 发现 CRLF（或裸 CR）内容：" >&2
  echo "${CRLF_MATCHES}" >&2
fi

if [[ -n "${BOM_MATCHES}" ]]; then
  FAILED=1
  echo "[text-normalization] 发现 UTF-8 BOM 内容：" >&2
  echo "${BOM_MATCHES}" >&2
fi

if [[ "${FAILED}" -ne 0 ]]; then
  echo "[text-normalization] 失败：请将文本文件统一为 UTF-8（无 BOM）+ LF 行尾后重试。" >&2
  exit 1
fi

echo "[text-normalization] 通过。"
