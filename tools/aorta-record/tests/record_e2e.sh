#!/usr/bin/env bash
# End-to-end smoke/regression test for aorta-record (developer-run or Bazel sh_test).

set -euo pipefail
set +b
set +m

RECORD_BIN=""
TALKER_BIN=""
PROFILE="quick"

for arg in "$@"; do
  case "${arg}" in
    --record-bin=*)
      RECORD_BIN="${arg#*=}"
      ;;
    --talker-bin=*)
      TALKER_BIN="${arg#*=}"
      ;;
    --profile=*)
      PROFILE="${arg#*=}"
      ;;
    *)
      echo "ERROR: unknown argument '${arg}'"
      exit 2
      ;;
  esac
done

if [[ -z "${RECORD_BIN}" || -z "${TALKER_BIN}" ]]; then
  cat <<EOF
ERROR: missing required binaries.
Expected:
  --record-bin=<path>
  --talker-bin=<path>
EOF
  exit 2
fi

if [[ "${PROFILE}" != "quick" && "${PROFILE}" != "full" ]]; then
  echo "ERROR: unsupported profile '${PROFILE}' (expected quick|full)"
  exit 2
fi

for bin in "${RECORD_BIN}" "${TALKER_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    echo "ERROR: binary not executable: ${bin}"
    exit 2
  fi
done

GROUP="${AORTA_RECORD_TEST_GROUP:-default}"
RAW_RUN_ID="${AORTA_RECORD_TEST_RUN_ID:-$$_$RANDOM}"
RUN_ID="$(echo "${RAW_RUN_ID}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9_]+/_/g')"
if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="run"
fi
if [[ ! "${RUN_ID}" =~ ^[a-z] ]]; then
  RUN_ID="r_${RUN_ID}"
fi

TOPIC="${AORTA_RECORD_TEST_TOPIC:-/cli/${RUN_ID}/record_chatter}"
TOPIC_SUFFIX="${TOPIC#/}"
TALKER_NAME="${AORTA_RECORD_TEST_TALKER_NAME:-record_talker_${RUN_ID}}"

TEST_TMP_ROOT="${TEST_TMPDIR:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-record-e2e.XXXXXX")}"
LOG_DIR="${AORTA_RECORD_TEST_LOG_DIR:-${TEST_TMP_ROOT}/aorta-record-${RUN_ID}}"
mkdir -p "${LOG_DIR}"

# Shared session topology, mirroring cli_e2e.sh: one peer-mode config exported
# via ZENOH_SESSION_CONFIG_URI so every process joins ONE mesh. Without this,
# each process falls back to the per-process local test router (TEST_TMPDIR is
# set under Bazel), i.e. talker and recorder land on two disjoint routers and
# the recorder records nothing. The recorder's default client-mode override
# (DRY-04) stays active on top of this config, exercising the shipped default:
# a client discovers the peer talker via multicast scouting, exactly like the
# aorta-cli e2e commands do.
ZENOH_CONFIG_FILE="${LOG_DIR}/zenoh-test.json5"
cat >"${ZENOH_CONFIG_FILE}" <<'EOF'
{
  mode: "peer",
  listen: {
    endpoints: ["tcp/127.0.0.1:0"]
  },
  transport: {
    shared_memory: { enabled: false }
  }
}
EOF
export ZENOH_SESSION_CONFIG_URI="${ZENOH_CONFIG_FILE}"

MCAP_MAIN="${LOG_DIR}/record_main.mcap"
MCAP_BLACKLIST="${LOG_DIR}/record_blacklist.mcap"
MCAP_COMPRESSION_ZSTD="${LOG_DIR}/record_compression_zstd.mcap"
MCAP_COMPRESSION_NONE="${LOG_DIR}/record_compression_none.mcap"
MCAP_COMPRESSION_LZ4="${LOG_DIR}/record_compression_lz4.mcap"
MCAP_NO_CHUNKING="${LOG_DIR}/record_no_chunking.mcap"

declare -a PIDS=()

stop_process() {
  local pid="$1"
  if [[ "${pid}" -le 0 ]]; then
    return
  fi
  if ! kill -0 "${pid}" 2>/dev/null; then
    wait "${pid}" 2>/dev/null || true
    return
  fi
  kill -INT "${pid}" 2>/dev/null || true
  for _ in {1..30}; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      wait "${pid}" 2>/dev/null || true
      return
    fi
    sleep 0.1
  done
  kill -TERM "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
}

cleanup() {
  for (( idx=${#PIDS[@]}-1; idx>=0; idx-- )); do
    stop_process "${PIDS[$idx]}"
  done
}
trap cleanup EXIT

start_bg() {
  local name="$1"
  shift
  local log_file="${LOG_DIR}/${name}.log"
  "$@" >"${log_file}" 2>&1 &
  local pid=$!
  PIDS+=("${pid}")
  echo "${pid}"
}

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

run_record_capture() {
  local name="$1"
  shift
  local log_file="${LOG_DIR}/${name}.out"
  if ! "${RECORD_BIN}" "$@" >"${log_file}" 2>&1; then
    echo "ERROR: command failed: ${RECORD_BIN} $*"
    cat "${log_file}" || true
    return 1
  fi
  cat "${log_file}"
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

extract_counter() {
  local text="$1"
  local key="$2"
  local value
  value="$(echo "${text}" | grep -Eo "${key}=[0-9]+" | head -n 1 | cut -d '=' -f 2 || true)"
  if [[ -z "${value}" ]]; then
    echo "ERROR: unable to extract counter '${key}'"
    echo "${text}"
    exit 1
  fi
  echo "${value}"
}

assert_file_nonempty() {
  local file="$1"
  if [[ ! -f "${file}" ]]; then
    echo "ERROR: expected output file not found: ${file}"
    exit 1
  fi
  if [[ ! -s "${file}" ]]; then
    echo "ERROR: output file is empty: ${file}"
    exit 1
  fi
}

file_size() {
  local file="$1"
  if [[ ! -f "${file}" ]]; then
    echo "ERROR: cannot measure missing file: ${file}"
    exit 1
  fi
  stat -c '%s' "${file}" 2>/dev/null || stat -f '%z' "${file}"
}

echo "=== Aorta Recorder ${PROFILE} e2e ==="
echo "group=${GROUP}"
echo "topic=${TOPIC}"
echo "log_dir=${LOG_DIR}"

echo "[setup] starting cli_talker..."
talker_pid="$(start_bg "talker" \
  "${TALKER_BIN}" \
  --name "${TALKER_NAME}" \
  --group "${GROUP}" \
  --topic "${TOPIC}" \
  --schema-attach always)"

if ! wait_for_pattern "${LOG_DIR}/talker.log" "schema registered hash=" 12; then
  echo "ERROR: talker did not become ready in time"
  cat "${LOG_DIR}/talker.log" || true
  exit 1
fi
echo "[setup] talker ready"

echo "[check] record included topic"
record_main_out="$(run_record_capture record_main \
  --group "${GROUP}" \
  -o "${MCAP_MAIN}" \
  --include "${TOPIC_SUFFIX}" \
  --duration 8 \
  --max-messages 4)"
assert_contains "${record_main_out}" "Recorder summary:" "record main"

accepted_main="$(extract_counter "${record_main_out}" "accepted_messages")"
if [[ "${accepted_main}" -le 0 ]]; then
  echo "ERROR: expected accepted_messages > 0, got ${accepted_main}"
  exit 1
fi

assert_file_nonempty "${MCAP_MAIN}"
if ! grep -aFq "aorta.recorder.start" "${MCAP_MAIN}"; then
  echo "ERROR: expected recorder metadata in ${MCAP_MAIN}"
  exit 1
fi
if ! grep -aFq "aorta.storage.compression" "${MCAP_MAIN}"; then
  echo "ERROR: expected storage compression metadata in ${MCAP_MAIN}"
  exit 1
fi
if ! grep -aFq "aorta/${GROUP}/pub/${TOPIC_SUFFIX}" "${MCAP_MAIN}"; then
  echo "ERROR: expected topic key in ${MCAP_MAIN}"
  exit 1
fi

if [[ "${PROFILE}" == "full" ]]; then
  echo "[check-full] blacklist excludes topic"
  record_blacklist_out="$(run_record_capture record_blacklist \
    --group "${GROUP}" \
    -o "${MCAP_BLACKLIST}" \
    --include "cli/${RUN_ID}/**" \
    --blacklist "${TOPIC_SUFFIX}" \
    --duration 2)"
  assert_contains "${record_blacklist_out}" "Recorder summary:" "record blacklist"
  accepted_blacklist="$(extract_counter "${record_blacklist_out}" "accepted_messages")"
  if [[ "${accepted_blacklist}" -ne 0 ]]; then
    echo "ERROR: expected accepted_messages == 0 in blacklist run, got ${accepted_blacklist}"
    exit 1
  fi
  assert_file_nonempty "${MCAP_BLACKLIST}"

  # Run zstd / none / lz4 with identical parameters so their output sizes
  # are directly comparable — bytes-per-message from the quick-profile main
  # record is dominated by per-file overhead when max-messages is small.
  compression_duration=6
  compression_max_messages=200

  echo "[check-full] compression matrix: zstd"
  record_zstd_out="$(run_record_capture record_compression_zstd \
    --group "${GROUP}" \
    -o "${MCAP_COMPRESSION_ZSTD}" \
    --include "${TOPIC_SUFFIX}" \
    --compression zstd \
    --duration "${compression_duration}" \
    --max-messages "${compression_max_messages}")"
  assert_contains "${record_zstd_out}" "Recorder summary:" "record compression zstd"
  assert_file_nonempty "${MCAP_COMPRESSION_ZSTD}"
  accepted_zstd="$(extract_counter "${record_zstd_out}" "accepted_messages")"

  echo "[check-full] compression matrix: none"
  record_none_out="$(run_record_capture record_compression_none \
    --group "${GROUP}" \
    -o "${MCAP_COMPRESSION_NONE}" \
    --include "${TOPIC_SUFFIX}" \
    --compression none \
    --duration "${compression_duration}" \
    --max-messages "${compression_max_messages}")"
  assert_contains "${record_none_out}" "Recorder summary:" "record compression none"
  assert_file_nonempty "${MCAP_COMPRESSION_NONE}"
  accepted_none="$(extract_counter "${record_none_out}" "accepted_messages")"

  echo "[check-full] compression matrix: lz4"
  record_lz4_out="$(run_record_capture record_compression_lz4 \
    --group "${GROUP}" \
    -o "${MCAP_COMPRESSION_LZ4}" \
    --include "${TOPIC_SUFFIX}" \
    --compression lz4 \
    --duration "${compression_duration}" \
    --max-messages "${compression_max_messages}")"
  assert_contains "${record_lz4_out}" "Recorder summary:" "record compression lz4"
  assert_file_nonempty "${MCAP_COMPRESSION_LZ4}"

  echo "[check-full] compression matrix: no-chunking"
  record_nochunk_out="$(run_record_capture record_no_chunking \
    --group "${GROUP}" \
    -o "${MCAP_NO_CHUNKING}" \
    --include "${TOPIC_SUFFIX}" \
    --compression none \
    --no-chunking \
    --duration "${compression_duration}" \
    --max-messages "${compression_max_messages}")"
  assert_contains "${record_nochunk_out}" "Recorder summary:" "record no-chunking"
  assert_file_nonempty "${MCAP_NO_CHUNKING}"

  if [[ "${accepted_zstd}" -le 0 || "${accepted_none}" -le 0 ]]; then
    echo "ERROR: compression matrix captured no messages (zstd=${accepted_zstd} none=${accepted_none})"
    exit 1
  fi

  size_zstd="$(file_size "${MCAP_COMPRESSION_ZSTD}")"
  size_none="$(file_size "${MCAP_COMPRESSION_NONE}")"
  zstd_bpm=$(( size_zstd / accepted_zstd ))
  none_bpm=$(( size_none / accepted_none ))
  echo "[check-full] bytes/msg zstd=${zstd_bpm} none=${none_bpm}"
  if (( zstd_bpm * 10 >= none_bpm * 9 )); then
    echo "ERROR: expected zstd bytes/msg < 0.9 * none bytes/msg; zstd=${zstd_bpm} none=${none_bpm}"
    exit 1
  fi
fi

echo "PASS: aorta-record ${PROFILE} e2e"
