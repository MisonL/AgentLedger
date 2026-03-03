#!/usr/bin/env bash
set -euo pipefail

integration_stream="${INTEGRATION_STREAM:-GOVERNANCE_ALERTS}"
integration_subject="${INTEGRATION_SUBJECT:-governance.alerts}"
callback_stream="${INTEGRATION_CALLBACK_STREAM:-INTEGRATION_CALLBACK_EVENTS}"
callback_subject="${INTEGRATION_CALLBACK_SUBJECT:-${INTEGRATION_CALLBACK_TOPIC:-integration.callback.events}}"
callback_durable="${INTEGRATION_CALLBACK_DURABLE:-INTEGRATION_CALLBACK_EVENT_SINK}"

if [[ -n "${INTEGRATION_CALLBACK_SUBJECT:-}" && -n "${INTEGRATION_CALLBACK_TOPIC:-}" && "${INTEGRATION_CALLBACK_SUBJECT}" != "${INTEGRATION_CALLBACK_TOPIC}" ]]; then
  echo "[ERROR] INTEGRATION_CALLBACK_SUBJECT 与 INTEGRATION_CALLBACK_TOPIC 同时设置但值不一致。" >&2
  echo "        SUBJECT=${INTEGRATION_CALLBACK_SUBJECT}" >&2
  echo "        TOPIC=${INTEGRATION_CALLBACK_TOPIC}" >&2
  exit 1
fi

if [[ -z "${callback_stream}" ]]; then
  echo "[ERROR] INTEGRATION_CALLBACK_STREAM 不能为空。" >&2
  exit 1
fi
if [[ -z "${callback_subject}" ]]; then
  echo "[ERROR] INTEGRATION_CALLBACK_SUBJECT/INTEGRATION_CALLBACK_TOPIC 不能为空。" >&2
  exit 1
fi
if [[ -z "${callback_durable}" ]]; then
  echo "[ERROR] INTEGRATION_CALLBACK_DURABLE 不能为空。" >&2
  exit 1
fi

if [[ "${callback_stream}" == "${integration_stream}" && "${callback_subject}" != "${integration_subject}" ]]; then
  echo "[ERROR] callback 与 alerts 共享同一 Stream 但 Subject 不同，易触发 JetStream consumer 绑定失败。" >&2
  echo "        INTEGRATION_STREAM=${integration_stream}" >&2
  echo "        INTEGRATION_SUBJECT=${integration_subject}" >&2
  echo "        INTEGRATION_CALLBACK_STREAM=${callback_stream}" >&2
  echo "        INTEGRATION_CALLBACK_SUBJECT=${callback_subject}" >&2
  echo "        请改为独立 callback stream，或确保该 stream 已显式包含 callback subject。" >&2
  exit 1
fi

echo "[OK] callback stream 绑定检查通过。"
echo "     alerts : stream=${integration_stream} subject=${integration_subject}"
echo "     callback: stream=${callback_stream} subject=${callback_subject} durable=${callback_durable}"
