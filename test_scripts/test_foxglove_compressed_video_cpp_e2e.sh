#!/usr/bin/env bash
# Minimal E2E test for Foxglove CompressedVideo topic flow (C++ publisher).
# Validates:
#   1) publisher registers topic schema_by_key
#   2) aorta-cli schema get can resolve schema details
#   3) aorta-cli topic echo can decode payload fields

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

GROUP="${AORTA_TEST_GROUP:-default}"
RAW_RUN_ID="${AORTA_TEST_RUN_ID:-$$_$RANDOM}"
RUN_ID="$(echo "${RAW_RUN_ID}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9_]+/_/g')"
if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="run"
fi
if [[ ! "${RUN_ID}" =~ ^[a-z] ]]; then
  RUN_ID="r_${RUN_ID}"
fi

TOPIC="${AORTA_TEST_TOPIC:-/foxglove/${RUN_ID}/compressed_video_cpp}"
STARTUP_TIMEOUT_SEC="${AORTA_FOXGLOVE_STARTUP_TIMEOUT_SEC:-12}"
ECHO_COUNT="${AORTA_FOXGLOVE_ECHO_COUNT:-2}"
LOG_DIR="${AORTA_TEST_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-foxglove-cpp-e2e.XXXXXX")}"

AORTA_BIN="${REPO_ROOT}/bazel-bin/tools/aorta-cli/aorta"
PUBLISHER_BIN="${REPO_ROOT}/bazel-bin/cpp/examples/foxglove_compressed_video_publisher"

for bin in "${AORTA_BIN}" "${PUBLISHER_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    cat <<EOF
ERROR: binary not found or not executable: ${bin}
Please build required targets first (developer-run):
  bazel build //tools/aorta-cli:aorta
  bazel build //cpp/examples:foxglove_compressed_video_publisher
EOF
    exit 2
  fi
done

mkdir -p "${LOG_DIR}"
PUBLISHER_LOG="${LOG_DIR}/foxglove_cpp_publisher.log"
SCHEMA_LOG="${LOG_DIR}/schema_get.log"
ECHO_LOG="${LOG_DIR}/topic_echo.log"

PUBLISHER_PID=""

cleanup() {
  if [[ -n "${PUBLISHER_PID}" ]]; then
    kill "${PUBLISHER_PID}" 2>/dev/null || true
    wait "${PUBLISHER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

wait_for_pattern() {
  local file="$1"
  local pattern="$2"
  local timeout_s="$3"
  local waited=0

  while (( waited < timeout_s * 10 )); do
    if [[ -f "${file}" ]] && grep -Fq "${pattern}" "${file}"; then
      return 0
    fi
    sleep 0.1
    waited=$((waited + 1))
  done
  return 1
}

assert_contains() {
  local text="$1"
  local expected="$2"
  local what="$3"
  if [[ "${text}" != *"${expected}"* ]]; then
    echo "ERROR: expected '${expected}' in ${what}"
    echo "----- ${what} output begin -----"
    echo "${text}"
    echo "----- ${what} output end -----"
    exit 1
  fi
}

retry_cli_capture() {
  local out_file="$1"
  local expected="$2"
  local timeout_s="$3"
  shift 3

  local attempts=$((timeout_s * 2))
  local output=""
  for ((i = 1; i <= attempts; i++)); do
    if output="$("${AORTA_BIN}" --group "${GROUP}" "$@" 2>&1)"; then
      echo "${output}" >"${out_file}"
      if [[ "${output}" == *"${expected}"* ]]; then
        echo "${output}"
        return 0
      fi
    fi
    sleep 0.5
  done

  echo "ERROR: timeout waiting '${expected}' from CLI command: $*"
  echo "Last output:"
  echo "${output}"
  exit 1
}

echo "=== Foxglove CompressedVideo minimal E2E (C++) ==="
echo "group=${GROUP}"
echo "topic=${TOPIC}"
echo "log_dir=${LOG_DIR}"

"${PUBLISHER_BIN}" \
  --group "${GROUP}" \
  --topic "${TOPIC}" \
  >"${PUBLISHER_LOG}" 2>&1 &
PUBLISHER_PID=$!

if ! wait_for_pattern "${PUBLISHER_LOG}" "publisher ready" "${STARTUP_TIMEOUT_SEC}"; then
  echo "ERROR: foxglove C++ publisher did not become ready in time"
  echo "--- foxglove C++ publisher log ---"
  cat "${PUBLISHER_LOG}" || true
  exit 1
fi

echo "[check] schema get --describe"
schema_out="$(retry_cli_capture "${SCHEMA_LOG}" "Kind: topic" "${STARTUP_TIMEOUT_SEC}" \
  schema get "${TOPIC}" --describe)"
assert_contains "${schema_out}" "Schema key suffix: pub/" "schema get"
assert_contains "${schema_out}" "CompressedVideo" "schema get"

echo "[check] topic echo decode"
if ! "${AORTA_BIN}" --group "${GROUP}" topic echo "${TOPIC}" --count "${ECHO_COUNT}" \
  >"${ECHO_LOG}" 2>&1; then
  echo "ERROR: topic echo failed"
  cat "${ECHO_LOG}" || true
  exit 1
fi
echo_out="$(cat "${ECHO_LOG}")"
assert_contains "${echo_out}" "frame_id" "topic echo"
assert_contains "${echo_out}" "h264" "topic echo"

echo "PASS: Foxglove CompressedVideo minimal E2E (C++)"
echo "publisher_log=${PUBLISHER_LOG}"
echo "schema_log=${SCHEMA_LOG}"
echo "echo_log=${ECHO_LOG}"
