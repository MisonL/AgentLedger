#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ "${AGENTLEDGER_E2E:-1}" != "1" ]]; then
  echo "错误: AGENTLEDGER_E2E 必须为 1，才能执行 governance -> integration -> downstream 跨服务 smoke。" >&2
  exit 1
fi

if [[ -z "${GOV_E2E_DATABASE_URL:-}" ]]; then
  echo "错误: 未配置 GOV_E2E_DATABASE_URL，无法执行 governance -> integration -> downstream 跨服务 smoke。" >&2
  echo "示例: GOV_E2E_DATABASE_URL='postgres://user:pass@127.0.0.1:5432/db?sslmode=disable' bun run test:e2e-governance-integration-downstream" >&2
  exit 1
fi

export AGENTLEDGER_E2E=1

echo "执行 governance -> integration -> downstream 跨服务 smoke..."
go test ./services/governance -run '^TestGovernanceIntegrationDownstreamSmoke$' -count=1 -v
