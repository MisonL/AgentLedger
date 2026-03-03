#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COVERAGE_DIR="${ROOT_DIR}/.coverage"
GO_COVERAGE_PROFILE="${COVERAGE_DIR}/go-cover.out"
CONTROL_PLANE_COVERAGE_TEXT="${COVERAGE_DIR}/control-plane-coverage.txt"
COVERAGE_GATE_LOG="${COVERAGE_DIR}/coverage-gate.log"

GO_SERVICE_ORDER=("ingestion-gateway" "puller" "integration")
CONTROL_PLANE_LINE_THRESHOLD="80"

mkdir -p "${COVERAGE_DIR}"
exec > >(tee "${COVERAGE_GATE_LOG}") 2>&1

require_file() {
  local file_path="$1"
  if [[ ! -f "${file_path}" ]]; then
    echo "[coverage-check] 缺少覆盖率文件: ${file_path}" >&2
    echo "[coverage-check] 请先执行: bash ./scripts/test-coverage.sh" >&2
    exit 1
  fi
}

show_control_plane_report_tail() {
  echo "[coverage-check] control-plane 覆盖率报告末尾 40 行："
  tail -n 40 "${CONTROL_PLANE_COVERAGE_TEXT}" || true
}

strip_ansi() {
  sed -E 's/\x1B\[[0-9;]*[[:alpha:]]//g'
}

is_below_threshold() {
  local actual="$1"
  local threshold="$2"
  awk -v actual="${actual}" -v threshold="${threshold}" 'BEGIN { exit (actual + 0 < threshold + 0 ? 0 : 1) }'
}

read_go_service_coverages() {
  awk '
    NR == 1 { next }
    {
      split($1, file_range, ":")
      file = file_range[1]
      stmts = $2 + 0
      count = $3 + 0

      service = ""
      if (file ~ /(^|\/)services\/ingestion-gateway\//) {
        service = "ingestion-gateway"
      } else if (file ~ /(^|\/)services\/puller\//) {
        service = "puller"
      } else if (file ~ /(^|\/)services\/integration\//) {
        service = "integration"
      }

      if (service == "") {
        next
      }

      total[service] += stmts
      if (count > 0) {
        covered[service] += stmts
      }
    }
    END {
      for (service in total) {
        if (total[service] <= 0) {
          continue
        }
        printf "%s %.2f\n", service, (covered[service] / total[service]) * 100
      }
    }
  ' "${GO_COVERAGE_PROFILE}"
}

read_control_plane_all_files_line_coverage() {
  local all_files_line
  all_files_line="$(
    strip_ansi < "${CONTROL_PLANE_COVERAGE_TEXT}" \
      | grep -E 'All files[[:space:]]*\|' \
      | tail -n 1
  )"

  if [[ -z "${all_files_line}" ]]; then
    echo "[coverage-check] 未找到 control-plane 覆盖率中的 All files 行。" >&2
    show_control_plane_report_tail
    return 1
  fi

  awk -F'|' '
    {
      value = $3
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      print value
    }
  ' <<< "${all_files_line}"
}

go_threshold_for_service() {
  local service="$1"
  case "${service}" in
    ingestion-gateway)
      echo "70"
      ;;
    puller)
      echo "70"
      ;;
    integration)
      echo "75"
      ;;
    *)
      return 1
      ;;
  esac
}

read_go_service_coverage() {
  local service="$1"
  local coverages_raw="$2"

  awk -v target="${service}" '
    $1 == target {
      print $2
      found = 1
      exit 0
    }
    END {
      if (!found) {
        exit 1
      }
    }
  ' <<< "${coverages_raw}"
}

require_file "${GO_COVERAGE_PROFILE}"
require_file "${CONTROL_PLANE_COVERAGE_TEXT}"

echo "[coverage-check] 使用覆盖率输入文件："
echo "  - ${GO_COVERAGE_PROFILE}"
echo "  - ${CONTROL_PLANE_COVERAGE_TEXT}"
echo "[coverage-check] 诊断日志输出：${COVERAGE_GATE_LOG}"

GO_COVERAGES_RAW="$(read_go_service_coverages)"

CONTROL_PLANE_LINE_COVERAGE="$(read_control_plane_all_files_line_coverage)"
if [[ ! "${CONTROL_PLANE_LINE_COVERAGE}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "[coverage-check] control-plane 行覆盖率解析失败: ${CONTROL_PLANE_LINE_COVERAGE}" >&2
  show_control_plane_report_tail
  exit 1
fi

FAILED=0

echo "[coverage-check] Go 包覆盖率阈值校验:"
for service in "${GO_SERVICE_ORDER[@]}"; do
  if ! threshold="$(go_threshold_for_service "${service}")"; then
    echo "  [FAIL] ${service}: 未配置阈值" >&2
    FAILED=1
    continue
  fi

  if ! actual="$(read_go_service_coverage "${service}" "${GO_COVERAGES_RAW}")"; then
    echo "  [FAIL] ${service}: 未找到覆盖率数据" >&2
    FAILED=1
    continue
  fi

  if is_below_threshold "${actual}" "${threshold}"; then
    echo "  [FAIL] ${service}: ${actual}% < ${threshold}%"
    FAILED=1
  else
    echo "  [PASS] ${service}: ${actual}% >= ${threshold}%"
  fi
done

echo "[coverage-check] control-plane All files 行覆盖率阈值校验:"
if is_below_threshold "${CONTROL_PLANE_LINE_COVERAGE}" "${CONTROL_PLANE_LINE_THRESHOLD}"; then
  echo "  [FAIL] control-plane: ${CONTROL_PLANE_LINE_COVERAGE}% < ${CONTROL_PLANE_LINE_THRESHOLD}%"
  FAILED=1
else
  echo "  [PASS] control-plane: ${CONTROL_PLANE_LINE_COVERAGE}% >= ${CONTROL_PLANE_LINE_THRESHOLD}%"
fi

if [[ "${FAILED}" -ne 0 ]]; then
  echo "[coverage-check] 覆盖率门禁未通过。"
  exit 1
fi

echo "[coverage-check] 覆盖率门禁通过。"
