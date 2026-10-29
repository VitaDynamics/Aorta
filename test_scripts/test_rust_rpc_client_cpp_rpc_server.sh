#!/usr/bin/env bash
# Test: Rust RPC client -> C++ RPC server.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SERVICE_NAME="${AORTA_RPC_TEST_SERVICE:-/demo/echo/rust_client_to_cpp_server}"
SERVER_STARTUP_DELAY="${AORTA_RPC_SERVER_STARTUP_DELAY_SEC:-2}"
CLIENT_TIMEOUT="${AORTA_RPC_CLIENT_TIMEOUT_SEC:-4s}"
SCHEMA_ATTACH="${AORTA_SCHEMA_ATTACH:-never}"
REQUEST_SCHEMA_HASH="${AORTA_RPC_REQUEST_SCHEMA_HASH:-}"
RESPONSE_SCHEMA_HASH="${AORTA_RPC_RESPONSE_SCHEMA_HASH:-}"
LOG_DIR="${AORTA_RPC_TEST_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-rpc-rust-to-cpp.XXXXXX")}"

RUST_CLIENT_BIN="${REPO_ROOT}/bazel-bin/rust/examples/echo_client"
CPP_SERVER_BIN="${REPO_ROOT}/bazel-bin/cpp/examples/echo_server"
ECHO_BFBS="${REPO_ROOT}/bazel-bin/message/schemas/service/example/echo.bfbs"

compute_sha256_hex() {
  local file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${file}" | awk '{print $1}'
    return 0
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${file}" | awk '{print $1}'
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$file" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
print(hashlib.sha256(path.read_bytes()).hexdigest())
PY
    return 0
  fi
  echo "ERROR: missing sha256 tool (need sha256sum/shasum/python3)" >&2
  return 2
}

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

for bin in "${RUST_CLIENT_BIN}" "${CPP_SERVER_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    cat <<EOF
ERROR: binary not found or not executable: ${bin}
Please build required targets first (developer-run):
  bazel build //rust/examples:echo_client
  bazel build //cpp/examples:echo_server
EOF
    exit 2
  fi
done

if [[ "${SCHEMA_ATTACH}" != "never" && "${SCHEMA_ATTACH}" != "always" && "${SCHEMA_ATTACH}" != "inherit" ]]; then
  echo "ERROR: AORTA_SCHEMA_ATTACH must be one of never|always|inherit, got '${SCHEMA_ATTACH}'"
  exit 2
fi

if [[ "${SCHEMA_ATTACH}" == "always" ]]; then
  if [[ -z "${REQUEST_SCHEMA_HASH}" || -z "${RESPONSE_SCHEMA_HASH}" ]]; then
    if [[ ! -f "${ECHO_BFBS}" ]]; then
      cat <<EOF
ERROR: schema attach policy is 'always' but echo schema bfbs is missing: ${ECHO_BFBS}
Please build required targets first (developer-run):
  bazel build //message:fbs_schema_echo_bfbs
EOF
      exit 2
    fi
    SCHEMA_HASH="$(compute_sha256_hex "${ECHO_BFBS}")"
    REQUEST_SCHEMA_HASH="${REQUEST_SCHEMA_HASH:-${SCHEMA_HASH}}"
    RESPONSE_SCHEMA_HASH="${RESPONSE_SCHEMA_HASH:-${SCHEMA_HASH}}"
  fi
fi

for hash in "${REQUEST_SCHEMA_HASH}" "${RESPONSE_SCHEMA_HASH}"; do
  if [[ -n "${hash}" && ! "${hash}" =~ ^[0-9a-f]{64}$ ]]; then
    echo "ERROR: schema hash must be 64-byte lowercase hex, got '${hash}'"
    exit 2
  fi
done

mkdir -p "${LOG_DIR}"
SERVER_LOG="${LOG_DIR}/cpp_echo_server.log"
CLIENT_LOG="${LOG_DIR}/rust_echo_client.log"

SERVER_PID=""
cleanup() {
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "=== Cross-Process RPC: Rust Client + C++ Server ==="
echo "service=${SERVICE_NAME}"
echo "schema_attach=${SCHEMA_ATTACH}"
echo "logs=${LOG_DIR}"

SERVER_ARGS=(
  --name cpp_echo_server
  --service "${SERVICE_NAME}"
  --schema-attach "${SCHEMA_ATTACH}"
)
if [[ -n "${REQUEST_SCHEMA_HASH}" ]]; then
  SERVER_ARGS+=(--request-schema-hash "${REQUEST_SCHEMA_HASH}")
fi
if [[ -n "${RESPONSE_SCHEMA_HASH}" ]]; then
  SERVER_ARGS+=(--response-schema-hash "${RESPONSE_SCHEMA_HASH}")
fi

"${CPP_SERVER_BIN}" "${SERVER_ARGS[@]}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

sleep "${SERVER_STARTUP_DELAY}"

CLIENT_ARGS=(
  --name rust_echo_client
  --service "${SERVICE_NAME}"
  --schema-attach "${SCHEMA_ATTACH}"
)
if [[ -n "${REQUEST_SCHEMA_HASH}" ]]; then
  CLIENT_ARGS+=(--request-schema-hash "${REQUEST_SCHEMA_HASH}")
fi
if [[ -n "${RESPONSE_SCHEMA_HASH}" ]]; then
  CLIENT_ARGS+=(--response-schema-hash "${RESPONSE_SCHEMA_HASH}")
fi

set +e
run_with_timeout "${CLIENT_TIMEOUT}" \
  "${RUST_CLIENT_BIN}" "${CLIENT_ARGS[@]}" \
  >"${CLIENT_LOG}" 2>&1
CLIENT_RC=$?
set -e

if [[ "${CLIENT_RC}" -ne 0 && "${CLIENT_RC}" -ne 124 && "${CLIENT_RC}" -ne 137 ]]; then
  echo "ERROR: rust echo_client exited with rc=${CLIENT_RC}"
  echo "--- rust echo_client log ---"
  cat "${CLIENT_LOG}"
  exit 1
fi

sleep 1
if [[ -n "${SERVER_PID}" ]]; then
  kill "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
  SERVER_PID=""
fi

if ! grep -Eq "\\[echo ok\\]" "${CLIENT_LOG}"; then
  echo "ERROR: Rust client did not receive echo response"
  echo "--- rust echo_client log ---"
  cat "${CLIENT_LOG}"
  echo "--- C++ echo_server log ---"
  cat "${SERVER_LOG}"
  exit 1
fi

if ! grep -Eq "Replied (typed )?echo request" "${SERVER_LOG}"; then
  echo "ERROR: C++ server did not handle echo request"
  echo "--- C++ echo_server log ---"
  cat "${SERVER_LOG}"
  echo "--- rust echo_client log ---"
  cat "${CLIENT_LOG}"
  exit 1
fi

echo "PASS: Rust RPC client -> C++ RPC server"
echo "server_log=${SERVER_LOG}"
echo "client_log=${CLIENT_LOG}"
