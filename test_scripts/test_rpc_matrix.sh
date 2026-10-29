#!/usr/bin/env bash
# Cross-process RPC matrix runner.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_ROOT="${AORTA_RPC_TEST_LOG_ROOT:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-rpc-matrix.XXXXXX")}"
POLICIES="${AORTA_RPC_SCHEMA_ATTACH_MATRIX:-never always}"

echo "=== Aorta Cross-Process RPC Matrix ==="
echo "policies=${POLICIES}"
echo "log_root=${LOG_ROOT}"

for policy in ${POLICIES}; do
  if [[ "${policy}" != "never" && "${policy}" != "always" && "${policy}" != "inherit" ]]; then
    echo "ERROR: unsupported policy '${policy}' (expected never|always|inherit)"
    exit 2
  fi

  echo ""
  echo "--- Policy: ${policy} | Rust RPC client -> C++ RPC server ---"
  AORTA_SCHEMA_ATTACH="${policy}" \
    AORTA_RPC_TEST_LOG_DIR="${LOG_ROOT}/${policy}/rust_client_cpp_server" \
    "${SCRIPT_DIR}/test_rust_rpc_client_cpp_rpc_server.sh"

  echo ""
  echo "--- Policy: ${policy} | C++ RPC client -> Rust RPC server ---"
  AORTA_SCHEMA_ATTACH="${policy}" \
    AORTA_RPC_TEST_LOG_DIR="${LOG_ROOT}/${policy}/cpp_client_rust_server" \
    "${SCRIPT_DIR}/test_cpp_rpc_client_rust_rpc_server.sh"
done

echo ""
echo "PASS: RPC matrix completed"
echo "log_root=${LOG_ROOT}"
