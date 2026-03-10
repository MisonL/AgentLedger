#!/usr/bin/env bash
set -euo pipefail

AGENT_SOURCE_BIN="${AGENT_SOURCE_BIN:-./agent}"
INSTALL_ROOT="${INSTALL_ROOT:-/opt/agentledger-agent}"
BIN_PATH="${BIN_PATH:-/usr/local/bin/agent}"
CONFIG_DIR="${CONFIG_DIR:-/etc/agentledger}"
STATE_DIR="${STATE_DIR:-/var/lib/agentledger}"
QUEUE_DIR="${QUEUE_DIR:-${STATE_DIR}/queue}"
MANAGED_CONFIG_DIR="${MANAGED_CONFIG_DIR:-${STATE_DIR}/config}"

install -d -m 0755 "$INSTALL_ROOT" "$CONFIG_DIR" "$STATE_DIR" "$QUEUE_DIR" "$MANAGED_CONFIG_DIR"
install -m 0755 "$AGENT_SOURCE_BIN" "$INSTALL_ROOT/agent"
ln -sf "$INSTALL_ROOT/agent" "$BIN_PATH"

cat > "${CONFIG_DIR}/agent.env" <<'EOF'
AGENT_GATEWAY_URL=http://127.0.0.1:8080
AGENT_RELEASE_CHANNEL=stable
AGENT_CONFIG_DIR=/var/lib/agentledger/config
AGENT_QUEUE_DIR=/var/lib/agentledger/queue
AGENT_RELEASE_SIGNING_PUBLIC_KEY_FILE=/etc/agentledger/agent-release-public.pem
EOF

printf '%s\n' "Linux silent install template completed."
printf '%s\n' "Binary: ${BIN_PATH}"
printf '%s\n' "Env file: ${CONFIG_DIR}/agent.env"
printf '%s\n' "Validate with: agent version && agent status && agent update check && agent update status"
