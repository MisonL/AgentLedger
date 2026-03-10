#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ "${AGENTLEDGER_E2E:-1}" != "1" ]]; then
  echo "错误: AGENTLEDGER_E2E 必须为 1，才能执行 Rule Hub 审批的 Postgres 回归。" >&2
  exit 1
fi

if [[ -z "${RULEHUB_E2E_DATABASE_URL:-}" && -z "${DATABASE_URL:-}" ]]; then
  echo "错误: 未配置 RULEHUB_E2E_DATABASE_URL 或 DATABASE_URL，无法执行 Rule Hub 审批 Postgres 回归。" >&2
  echo "示例: RULEHUB_E2E_DATABASE_URL='postgres://user:pass@127.0.0.1:5432/db?sslmode=disable' AGENTLEDGER_E2E=1 bun run test:e2e-rulehub-approvals-db" >&2
  exit 1
fi

export AGENTLEDGER_E2E=1
export DATABASE_URL="${RULEHUB_E2E_DATABASE_URL:-${DATABASE_URL}}"

echo "执行 Rule Hub 审批在 Postgres ON CONFLICT upsert 场景下的 created/updated 回归..."
bun run --cwd apps/control-plane test -- --test-name-pattern 'rule hub 路由：400'

