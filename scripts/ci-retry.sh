#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "用法: $0 <max_attempts>=正整数 <sleep_seconds>=非负整数 <command...>" >&2
  exit 2
fi

max_attempts="$1"
sleep_seconds="$2"
shift 2

if ! [[ "$max_attempts" =~ ^[0-9]+$ ]] || (( max_attempts < 1 )); then
  echo "max_attempts 必须是 >= 1 的整数: $max_attempts" >&2
  exit 2
fi

if ! [[ "$sleep_seconds" =~ ^[0-9]+$ ]]; then
  echo "sleep_seconds 必须是 >= 0 的整数: $sleep_seconds" >&2
  exit 2
fi

attempt=1
while true; do
  echo "[ci-retry] attempt ${attempt}/${max_attempts}: $*"
  if "$@"; then
    if (( attempt > 1 )); then
      echo "[ci-retry] succeeded on attempt ${attempt}."
    fi
    exit 0
  else
    exit_code=$?
  fi

  if (( attempt >= max_attempts )); then
    echo "[ci-retry] failed after ${attempt} attempts (exit=${exit_code})." >&2
    exit "$exit_code"
  fi

  echo "[ci-retry] attempt ${attempt} failed (exit=${exit_code}), retrying in ${sleep_seconds}s..." >&2
  sleep "$sleep_seconds"
  attempt=$((attempt + 1))
done
