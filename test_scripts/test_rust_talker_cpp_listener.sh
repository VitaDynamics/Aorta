#!/usr/bin/env bash
# Test: Rust talker -> C++ listener (single direction)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SCHEMA_ATTACH="${AORTA_SCHEMA_ATTACH:-never}"
case "${SCHEMA_ATTACH}" in
  always | never) ;;
  *)
    echo "ERROR: AORTA_SCHEMA_ATTACH must be 'always' or 'never', got '${SCHEMA_ATTACH}'"
    exit 2
    ;;
esac

TOPIC="${AORTA_TEST_TOPIC:-/demo/chatter/rust_to_cpp/${SCHEMA_ATTACH}}"
LISTENER_STARTUP_DELAY="${AORTA_LISTENER_STARTUP_DELAY_SEC:-2}"
TALKER_TIMEOUT="${AORTA_TALKER_TIMEOUT_SEC:-3s}"
LOG_DIR="${AORTA_TEST_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-rust-to-cpp.XXXXXX")}"
LISTENER_EXTRA_ARGS_RAW="${AORTA_LISTENER_EXTRA_ARGS:-}"

RUST_TALKER_BIN="${REPO_ROOT}/bazel-bin/rust/examples/talker"
CPP_LISTENER_BIN="${REPO_ROOT}/bazel-bin/cpp/examples/listener"

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
    echo "ERROR: timeout '${timeout_value}' is unsupported without timeout/gtimeout (use like 3s)"
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

  # Normalize SIGTERM exit to timeout-like code for consistent handling.
  if [[ "${rc}" -eq 143 ]]; then
    return 124
  fi
  return "${rc}"
}

for bin in "${RUST_TALKER_BIN}" "${CPP_LISTENER_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    cat <<EOF
ERROR: binary not found or not executable: ${bin}
Please build required targets first (developer-run):
  bazel build //rust/examples:talker
  bazel build //cpp/examples:listener
EOF
    exit 2
  fi
done

mkdir -p "${LOG_DIR}"
LISTENER_LOG="${LOG_DIR}/cpp_listener_${SCHEMA_ATTACH}.log"
TALKER_LOG="${LOG_DIR}/rust_talker_${SCHEMA_ATTACH}.log"

LISTENER_PID=""

cleanup() {
  if [[ -n "${LISTENER_PID}" ]]; then
    kill "${LISTENER_PID}" 2>/dev/null || true
    wait "${LISTENER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "=== Cross-Process Test: Rust Talker + C++ Listener (${SCHEMA_ATTACH}) ==="
echo "topic=${TOPIC}"
echo "logs=${LOG_DIR}"
if [[ -n "${LISTENER_EXTRA_ARGS_RAW}" ]]; then
  echo "listener_extra_args=${LISTENER_EXTRA_ARGS_RAW}"
fi

if [[ -n "${LISTENER_EXTRA_ARGS_RAW}" ]]; then
  # shellcheck disable=SC2086
  AORTA_SCHEMA_ATTACH="${SCHEMA_ATTACH}" "${CPP_LISTENER_BIN}" \
    --name cpp_listener \
    --topic "${TOPIC}" \
    ${LISTENER_EXTRA_ARGS_RAW} \
    >"${LISTENER_LOG}" 2>&1 &
else
  AORTA_SCHEMA_ATTACH="${SCHEMA_ATTACH}" "${CPP_LISTENER_BIN}" \
    --name cpp_listener \
    --topic "${TOPIC}" \
    >"${LISTENER_LOG}" 2>&1 &
fi
LISTENER_PID=$!

sleep "${LISTENER_STARTUP_DELAY}"

set +e
run_with_timeout "${TALKER_TIMEOUT}" \
  env AORTA_SCHEMA_ATTACH="${SCHEMA_ATTACH}" \
  "${RUST_TALKER_BIN}" \
  --name rust_talker \
  --topic "${TOPIC}" \
  >"${TALKER_LOG}" 2>&1
TALKER_RC=$?
set -e

if [[ "${TALKER_RC}" -ne 0 && "${TALKER_RC}" -ne 124 && "${TALKER_RC}" -ne 137 ]]; then
  echo "ERROR: rust talker exited with rc=${TALKER_RC}"
  echo "--- rust talker log ---"
  cat "${TALKER_LOG}"
  exit 1
fi

sleep 1

if [[ -n "${LISTENER_PID}" ]]; then
  kill "${LISTENER_PID}" 2>/dev/null || true
  wait "${LISTENER_PID}" 2>/dev/null || true
  LISTENER_PID=""
fi

if ! grep -Eq "from rust_talker" "${LISTENER_LOG}"; then
  echo "ERROR: C++ listener did not receive expected messages from rust_talker"
  echo "--- C++ listener log ---"
  cat "${LISTENER_LOG}"
  echo "--- Rust talker log ---"
  cat "${TALKER_LOG}"
  exit 1
fi

echo "PASS: Rust -> C++ (${SCHEMA_ATTACH})"
echo "listener_log=${LISTENER_LOG}"
echo "talker_log=${TALKER_LOG}"
