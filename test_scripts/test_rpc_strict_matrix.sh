#!/usr/bin/env bash
# Cross-process RPC strict-mode matrix with two active providers.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SERVICE_PREFIX="${AORTA_RPC_STRICT_SERVICE_PREFIX:-/demo/echo/strict_matrix}"
LOG_ROOT="${AORTA_RPC_STRICT_LOG_ROOT:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-rpc-strict-matrix.XXXXXX")}"
SERVER_STARTUP_DELAY="${AORTA_RPC_SERVER_STARTUP_DELAY_SEC:-2}"
CLIENT_TIMEOUT="${AORTA_RPC_CLIENT_TIMEOUT_SEC:-4s}"
STRICT_WINDOW_MS="${AORTA_RPC_MULTI_REPLY_CHECK_WINDOW_MS:-500}"
MATRIX_MODES="${AORTA_RPC_STRICT_MATRIX:-off on}"
SESSION_CONFIG_URI="${AORTA_RPC_ZENOH_SESSION_CONFIG_URI:-}"

RUST_CLIENT_BIN="${REPO_ROOT}/bazel-bin/rust/examples/echo_client"
CPP_CLIENT_BIN="${REPO_ROOT}/bazel-bin/cpp/examples/echo_client"
RUST_SERVER_BIN="${REPO_ROOT}/bazel-bin/rust/examples/echo_server"
CPP_SERVER_BIN="${REPO_ROOT}/bazel-bin/cpp/examples/echo_server"

run_with_timeout() {
  local timeout_value="$1"
  shift

  if command -v timeout >/dev/null 2>&1; then
    timeout "${timeout_value}" "$@"
    return $?
  fi
  if command -v gtimeout >/dev/null 2>&1; then
    gtimeout "${timeout_value}" "$@"
    return $?
  fi

  local seconds="${timeout_value%s}"
  if [[ -z "${seconds}" || ! "${seconds}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: timeout '${timeout_value}' is unsupported without timeout/gtimeout (use like 4s)"
    return 2
  fi

  "$@" &
  local cmd_pid=$!
  (
    sleep "${seconds}"
    if kill -0 "${cmd_pid}" 2>/dev/null; then
      kill "${cmd_pid}" 2>/dev/null || true
    fi
  ) &
  local guard_pid=$!

  wait "${cmd_pid}"
  local rc=$?
  kill "${guard_pid}" 2>/dev/null || true
  wait "${guard_pid}" 2>/dev/null || true

  if [[ "${rc}" -eq 143 ]]; then
    return 124
  fi
  return "${rc}"
}

for bin in "${RUST_CLIENT_BIN}" "${CPP_CLIENT_BIN}" "${RUST_SERVER_BIN}" "${CPP_SERVER_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    cat <<EOF
ERROR: binary not found or not executable: ${bin}
Please build required targets first (developer-run):
  bazel build //rust/examples:echo_server
  bazel build //rust/examples:echo_client
  bazel build //cpp/examples:echo_server
  bazel build //cpp/examples:echo_client
EOF
    exit 2
  fi
done

echo "=== Aorta RPC Strict Multi-Provider Matrix ==="
echo "modes=${MATRIX_MODES}"
echo "service_prefix=${SERVICE_PREFIX}"
echo "log_root=${LOG_ROOT}"
if [[ -n "${SESSION_CONFIG_URI}" ]]; then
  echo "zenoh_session_config_uri=${SESSION_CONFIG_URI}"
fi

run_case() {
  local mode="$1"
  local case_name="$2"
  local client_bin="$3"
  local server_bin="$4"

  local case_log_dir="${LOG_ROOT}/${mode}/${case_name}"
  mkdir -p "${case_log_dir}"
  local service_name="${SERVICE_PREFIX}/${case_name}/${mode}"

  local server1_log="${case_log_dir}/server1.log"
  local server2_log="${case_log_dir}/server2.log"
  local client_log="${case_log_dir}/client.log"

  local server1_name="${case_name}_${mode}_server1"
  local server2_name="${case_name}_${mode}_server2"
  local client_name="${case_name}_${mode}_client"

  echo ""
  echo "--- Case: ${case_name} | strict=${mode} ---"
  echo "service=${service_name}"
  echo "logs=${case_log_dir}"

  if [[ -n "${SESSION_CONFIG_URI}" ]]; then
    ZENOH_SESSION_CONFIG_URI="${SESSION_CONFIG_URI}" \
      "${server_bin}" \
      --name "${server1_name}" \
      --service "${service_name}" \
      --schema-attach never \
      >"${server1_log}" 2>&1 &
  else
    "${server_bin}" \
      --name "${server1_name}" \
      --service "${service_name}" \
      --schema-attach never \
      >"${server1_log}" 2>&1 &
  fi
  local server1_pid=$!

  if [[ -n "${SESSION_CONFIG_URI}" ]]; then
    ZENOH_SESSION_CONFIG_URI="${SESSION_CONFIG_URI}" \
      "${server_bin}" \
      --name "${server2_name}" \
      --service "${service_name}" \
      --schema-attach never \
      >"${server2_log}" 2>&1 &
  else
    "${server_bin}" \
      --name "${server2_name}" \
      --service "${service_name}" \
      --schema-attach never \
      >"${server2_log}" 2>&1 &
  fi
  local server2_pid=$!

  local cleanup_done=0
  cleanup_case() {
    if [[ "${cleanup_done}" -eq 1 ]]; then
      return
    fi
    cleanup_done=1
    kill "${server1_pid}" 2>/dev/null || true
    kill "${server2_pid}" 2>/dev/null || true
    wait "${server1_pid}" 2>/dev/null || true
    wait "${server2_pid}" 2>/dev/null || true
  }
  trap cleanup_case RETURN

  sleep "${SERVER_STARTUP_DELAY}"

  local -a CLIENT_ARGS=(
    --name "${client_name}"
    --service "${service_name}"
    --schema-attach never
    --multi-reply-window-ms "${STRICT_WINDOW_MS}"
  )
  if [[ "${mode}" == "on" ]]; then
    CLIENT_ARGS+=(--enforce-single-provider)
  elif [[ "${mode}" != "off" ]]; then
    echo "ERROR: unsupported strict mode '${mode}', expected off|on"
    exit 2
  fi

  set +e
  if [[ -n "${SESSION_CONFIG_URI}" ]]; then
    run_with_timeout "${CLIENT_TIMEOUT}" \
      env ZENOH_SESSION_CONFIG_URI="${SESSION_CONFIG_URI}" \
      "${client_bin}" "${CLIENT_ARGS[@]}" \
      >"${client_log}" 2>&1
  else
    run_with_timeout "${CLIENT_TIMEOUT}" \
      "${client_bin}" "${CLIENT_ARGS[@]}" \
      >"${client_log}" 2>&1
  fi
  local client_rc=$?
  set -e

  cleanup_case
  trap - RETURN

  if [[ "${client_rc}" -ne 0 && "${client_rc}" -ne 124 && "${client_rc}" -ne 137 ]]; then
    echo "ERROR: client exited with rc=${client_rc}"
    echo "--- client log ---"
    cat "${client_log}"
    if grep -Eq "Unable to create POSIX shm segment" "${client_log}"; then
      echo "HINT: SHM init failed; provide a config with shared_memory disabled via"
      echo "      AORTA_RPC_ZENOH_SESSION_CONFIG_URI=/path/to/zenoh_config.json5"
    fi
    exit 1
  fi

  if [[ "${mode}" == "on" ]]; then
    if ! grep -Eq "multiple replies" "${client_log}"; then
      echo "ERROR: strict mode did not report multi-provider conflict"
      echo "--- client log ---"
      cat "${client_log}"
      echo "--- server1 log ---"
      cat "${server1_log}"
      echo "--- server2 log ---"
      cat "${server2_log}"
      exit 1
    fi
  else
    if ! grep -Eq "\\[echo ok\\]" "${client_log}"; then
      echo "ERROR: non-strict mode did not receive echo response"
      echo "--- client log ---"
      cat "${client_log}"
      echo "--- server1 log ---"
      cat "${server1_log}"
      echo "--- server2 log ---"
      cat "${server2_log}"
      exit 1
    fi
  fi

  if ! grep -Eq "Replied (typed )?echo request" "${server1_log}"; then
    echo "ERROR: server1 did not handle request"
    echo "--- server1 log ---"
    cat "${server1_log}"
    echo "--- client log ---"
    cat "${client_log}"
    exit 1
  fi

  if ! grep -Eq "Replied (typed )?echo request" "${server2_log}"; then
    echo "ERROR: server2 did not handle request"
    echo "--- server2 log ---"
    cat "${server2_log}"
    echo "--- client log ---"
    cat "${client_log}"
    exit 1
  fi

  echo "PASS: ${case_name} strict=${mode}"
  echo "server1_log=${server1_log}"
  echo "server2_log=${server2_log}"
  echo "client_log=${client_log}"
}

for mode in ${MATRIX_MODES}; do
  run_case "${mode}" "rust_client_cpp_servers" "${RUST_CLIENT_BIN}" "${CPP_SERVER_BIN}"
  run_case "${mode}" "cpp_client_rust_servers" "${CPP_CLIENT_BIN}" "${RUST_SERVER_BIN}"
done

echo ""
echo "PASS: RPC strict multi-provider matrix completed"
echo "log_root=${LOG_ROOT}"
