#!/usr/bin/env bash
# Cross-process matrix runner for schema attachment policies.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_ROOT="${AORTA_TEST_LOG_ROOT:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-attach-matrix.XXXXXX")}"

POLICY_LIST="${AORTA_SCHEMA_ATTACH_MATRIX:-never always}"
read -r -a POLICIES <<<"${POLICY_LIST}"

for policy in "${POLICIES[@]}"; do
  case "${policy}" in
    always | never) ;;
    *)
      echo "ERROR: unsupported policy '${policy}'. Use only 'always' or 'never'."
      exit 2
      ;;
  esac
done

echo "=== Aorta Cross-Process Schema Attach Matrix ==="
echo "policies=${POLICIES[*]}"
echo "log_root=${LOG_ROOT}"

for policy in "${POLICIES[@]}"; do
  echo ""
  echo "--- Policy: ${policy} | Rust -> C++ ---"
  AORTA_SCHEMA_ATTACH="${policy}" \
    AORTA_TEST_LOG_DIR="${LOG_ROOT}/${policy}/rust_to_cpp" \
    "${SCRIPT_DIR}/test_rust_talker_cpp_listener.sh"

  echo ""
  echo "--- Policy: ${policy} | C++ -> Rust ---"
  AORTA_SCHEMA_ATTACH="${policy}" \
    AORTA_TEST_LOG_DIR="${LOG_ROOT}/${policy}/cpp_to_rust" \
    "${SCRIPT_DIR}/test_cpp_talker_rust_listener.sh"
done

echo ""
echo "PASS: schema attach matrix completed"
echo "log_root=${LOG_ROOT}"
