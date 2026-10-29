#!/usr/bin/env bash
# Cross-process matrix runner for subscriber strategy flags.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_ROOT="${AORTA_STRATEGY_TEST_LOG_ROOT:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-strategy-matrix.XXXXXX")}"

POLICY_LIST="${AORTA_SCHEMA_ATTACH_MATRIX:-never always}"
MODE_LIST="${AORTA_SUB_STRATEGY_MATRIX:-baseline header_context dedup reorder full}"
read -r -a POLICIES <<<"${POLICY_LIST}"
read -r -a MODES <<<"${MODE_LIST}"

for policy in "${POLICIES[@]}"; do
  case "${policy}" in
    always | never) ;;
    *)
      echo "ERROR: unsupported schema policy '${policy}'. Use only 'always' or 'never'."
      exit 2
      ;;
  esac
done

strategy_args_for_mode() {
  local mode="$1"
  case "${mode}" in
    baseline) echo "" ;;
    header_context) echo "--header-context" ;;
    dedup) echo "--dedup --dedup-window 64" ;;
    reorder) echo "--reorder-check" ;;
    full) echo "--header-context --dedup --dedup-window 64 --reorder-check --drop-out-of-order" ;;
    *)
      echo "ERROR: unsupported strategy mode '${mode}'."
      echo "Supported modes: baseline header_context dedup reorder full"
      exit 2
      ;;
  esac
}

assert_log_contains() {
  local log_file="$1"
  local pattern="$2"
  local description="$3"
  if ! grep -Eq "${pattern}" "${log_file}"; then
    echo "ERROR: missing '${description}' in ${log_file}"
    echo "--- listener log ---"
    cat "${log_file}"
    exit 1
  fi
}

validate_mode_markers() {
  local mode="$1"
  local log_file="$2"
  case "${mode}" in
    baseline)
      assert_log_contains "${log_file}" "subscriber strategy: header_context=false dedup=false" \
        "baseline strategy line"
      assert_log_contains "${log_file}" "reorder_check=false drop_out_of_order=false" \
        "baseline reorder/drop flags"
      ;;
    header_context)
      assert_log_contains "${log_file}" "subscriber strategy: header_context=true" \
        "header_context strategy line"
      assert_log_contains "${log_file}" "ctx header: seq=" "header context output"
      ;;
    dedup)
      assert_log_contains "${log_file}" "dedup=true" "dedup enabled marker"
      assert_log_contains "${log_file}" "dedup_window=64" "dedup window marker"
      ;;
    reorder)
      assert_log_contains "${log_file}" "reorder_check=true" "reorder enabled marker"
      ;;
    full)
      assert_log_contains "${log_file}" "subscriber strategy: header_context=true dedup=true dedup_window=64 reorder_check=true drop_out_of_order=true" \
        "full strategy line"
      assert_log_contains "${log_file}" "ctx header: seq=" "full mode header output"
      ;;
  esac
}

echo "=== Aorta Cross-Process Subscriber Strategy Matrix ==="
echo "schema_policies=${POLICIES[*]}"
echo "strategy_modes=${MODES[*]}"
echo "log_root=${LOG_ROOT}"

for policy in "${POLICIES[@]}"; do
  for mode in "${MODES[@]}"; do
    listener_args="$(strategy_args_for_mode "${mode}")"

    echo ""
    echo "--- Policy: ${policy} | Mode: ${mode} | Rust -> C++ ---"
    AORTA_SCHEMA_ATTACH="${policy}" \
      AORTA_LISTENER_EXTRA_ARGS="${listener_args}" \
      AORTA_TEST_LOG_DIR="${LOG_ROOT}/${policy}/${mode}/rust_to_cpp" \
      "${SCRIPT_DIR}/test_rust_talker_cpp_listener.sh"
    validate_mode_markers "${mode}" \
      "${LOG_ROOT}/${policy}/${mode}/rust_to_cpp/cpp_listener_${policy}.log"

    echo ""
    echo "--- Policy: ${policy} | Mode: ${mode} | C++ -> Rust ---"
    AORTA_SCHEMA_ATTACH="${policy}" \
      AORTA_LISTENER_EXTRA_ARGS="${listener_args}" \
      AORTA_TEST_LOG_DIR="${LOG_ROOT}/${policy}/${mode}/cpp_to_rust" \
      "${SCRIPT_DIR}/test_cpp_talker_rust_listener.sh"
    validate_mode_markers "${mode}" \
      "${LOG_ROOT}/${policy}/${mode}/cpp_to_rust/rust_listener_${policy}.log"
  done
done

echo ""
echo "PASS: subscriber strategy matrix completed"
echo "log_root=${LOG_ROOT}"
