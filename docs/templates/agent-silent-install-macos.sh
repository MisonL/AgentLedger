#!/usr/bin/env bash
set -euo pipefail

AGENT_SOURCE_BIN="${AGENT_SOURCE_BIN:-./agent}"
INSTALL_ROOT="${INSTALL_ROOT:-/Applications/AgentLedger}"
BIN_PATH="${BIN_PATH:-/usr/local/bin/agent}"
CONFIG_DIR="${CONFIG_DIR:-/Library/Application Support/AgentLedger}"
QUEUE_DIR="${QUEUE_DIR:-${CONFIG_DIR}/queue}"
MANAGED_CONFIG_DIR="${MANAGED_CONFIG_DIR:-${CONFIG_DIR}/config}"

install -d -m 0755 "$INSTALL_ROOT" "$CONFIG_DIR" "$QUEUE_DIR" "$MANAGED_CONFIG_DIR"
install -m 0755 "$AGENT_SOURCE_BIN" "$INSTALL_ROOT/agent"
ln -sf "$INSTALL_ROOT/agent" "$BIN_PATH"

cat > "${CONFIG_DIR}/agent.env" <<'EOF'
AGENT_GATEWAY_URL=http://127.0.0.1:8080
AGENT_RELEASE_CHANNEL=stable
AGENT_CONFIG_DIR=/Library/Application Support/AgentLedger/config
AGENT_QUEUE_DIR=/Library/Application Support/AgentLedger/queue
AGENT_RELEASE_SIGNING_PUBLIC_KEY_FILE=/Library/Application Support/AgentLedger/agent-release-public.pem
EOF

printf '%s\n' "macOS silent install template completed."
printf '%s\n' "Binary: ${BIN_PATH}"
printf '%s\n' "Env file: ${CONFIG_DIR}/agent.env"
printf '%s\n' "Validate with: agent version && agent status && agent update check && agent update status"
