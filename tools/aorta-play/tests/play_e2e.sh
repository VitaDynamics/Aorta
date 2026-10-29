#!/usr/bin/env bash
# End-to-end coverage for the aorta-play wrappers and snapshot contract.

set -euo pipefail
set +b
set +m

PLAY_BIN=""
CLI_BIN=""
RECORD_BIN=""
TALKER_BIN=""
PLAY_FAULT_BIN=""
CLI_FAULT_BIN=""
PROFILE="full"

for arg in "$@"; do
  case "${arg}" in
    --play-bin=*)
      PLAY_BIN="${arg#*=}"
      ;;
    --cli-bin=*)
      CLI_BIN="${arg#*=}"
      ;;
    --record-bin=*)
      RECORD_BIN="${arg#*=}"
      ;;
    --talker-bin=*)
      TALKER_BIN="${arg#*=}"
      ;;
    --play-fault-bin=*)
      PLAY_FAULT_BIN="${arg#*=}"
      ;;
    --cli-fault-bin=*)
      CLI_FAULT_BIN="${arg#*=}"
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

if [[ -z "${PLAY_BIN}" || -z "${CLI_BIN}" || -z "${RECORD_BIN}" || -z "${TALKER_BIN}" || -z "${PLAY_FAULT_BIN}" || -z "${CLI_FAULT_BIN}" ]]; then
  cat <<EOF
ERROR: missing required binaries.
Expected:
  --play-bin=<path>
  --cli-bin=<path>
  --record-bin=<path>
  --talker-bin=<path>
  --play-fault-bin=<path>
  --cli-fault-bin=<path>
EOF
  exit 2
fi

if [[ "${PROFILE}" != "quick" && "${PROFILE}" != "full" ]]; then
  echo "ERROR: unsupported profile '${PROFILE}' (expected quick|full)"
  exit 2
fi

for bin in "${PLAY_BIN}" "${CLI_BIN}" "${RECORD_BIN}" "${TALKER_BIN}" "${PLAY_FAULT_BIN}" "${CLI_FAULT_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    echo "ERROR: binary not executable: ${bin}"
    exit 2
  fi
done

# The group tests set this explicitly for the child that needs it. Keeping the
# parent environment clear makes the metadata-only case deterministic.
unset AORTA_GROUP || true

RAW_RUN_ID="${AORTA_PLAY_TEST_RUN_ID:-$$_$RANDOM}"
RUN_ID="$(echo "${RAW_RUN_ID}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9_]+/_/g')"
if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="run"
fi
if [[ ! "${RUN_ID}" =~ ^[a-z] ]]; then
  RUN_ID="r_${RUN_ID}"
fi

RECORDED_GROUP="play_recorded_${RUN_ID}"
AMBIENT_GROUP="play_ambient_${RUN_ID}"
OVERRIDE_GROUP="play_override_${RUN_ID}"
TOPIC="/e2e/${RUN_ID}/play_chatter"
TOPIC_SUFFIX="${TOPIC#/}"
TALKER_NAME="play_talker_${RUN_ID}"

if [[ -n "${TEST_TMPDIR:-}" ]]; then
  TEST_TMP_ROOT="${TEST_TMPDIR}"
else
  TEST_TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/aorta-play-e2e.XXXXXX")"
fi
LOG_DIR="${AORTA_PLAY_TEST_LOG_DIR:-${TEST_TMP_ROOT}/aorta-play-${RUN_ID}}"
mkdir -p "${LOG_DIR}"

# One peer-mode session config is exported to every process. This is required
# for a multi-process Bazel test: otherwise each process can create a private
# local router and the publisher, recorder, player, and subscriber are split.
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
MCAP_NO_SUMMARY="${LOG_DIR}/record_no_summary.mcap"
MCAP_LIVE="${LOG_DIR}/record_live.mcap"

declare -a PIDS=()
LAST_PID=""
CHILD_RC=0

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
  for ((idx=${#PIDS[@]}-1; idx>=0; idx--)); do
    stop_process "${PIDS[$idx]}"
  done
}
trap cleanup EXIT

start_bg() {
  local name="$1"
  shift
  local log_file="${LOG_DIR}/${name}.log"
  "$@" >"${log_file}" 2>&1 &
  LAST_PID=$!
  PIDS+=("${LAST_PID}")
}

wait_for_pattern() {
  local file="$1"
  local pattern="$2"
  local timeout_s="$3"
  local waited=0
  while ((waited < timeout_s * 10)); do
    if [[ -f "${file}" ]] && grep -Fq "${pattern}" "${file}"; then
      return 0
    fi
    sleep 0.1
    waited=$((waited + 1))
  done
  return 1
}

wait_for_file_nonempty() {
  local file="$1"
  local timeout_s="$2"
  local waited=0
  while ((waited < timeout_s * 10)); do
    if [[ -s "${file}" ]]; then
      return 0
    fi
    sleep 0.1
    waited=$((waited + 1))
  done
  return 1
}

assert_running() {
  local pid="$1"
  if ! kill -0 "${pid}" 2>/dev/null; then
    echo "ERROR: expected process ${pid} to still be running"
    return 1
  fi
}

assert_message_count() {
  local file="$1"
  local expected="$2"
  local actual
  actual="$(awk '/^--- Message [0-9]+ ---$/ { count++ } END { print count + 0 }' "${file}")"
  if ((actual < expected)); then
    echo "ERROR: expected at least ${expected} messages in ${file}, got ${actual}"
    cat "${file}" || true
    return 1
  fi
}

wait_for_child() {
  local pid="$1"
  local timeout_s="$2"
  local waited=0
  while kill -0 "${pid}" 2>/dev/null && ((waited < timeout_s * 10)); do
    sleep 0.1
    waited=$((waited + 1))
  done
  if kill -0 "${pid}" 2>/dev/null; then
    return 124
  fi
  set +e
  wait "${pid}"
  CHILD_RC=$?
  set -e
  return 0
}

start_subscriber() {
  local name="$1"
  local group="$2"
  local expected="$3"
  start_bg "${name}" \
    env "ZENOH_SESSION_CONFIG_URI=${ZENOH_CONFIG_FILE}" \
    "${CLI_BIN}" --no-client-override --group "${group}" \
    --zenoh-config "${ZENOH_CONFIG_FILE}" \
    topic echo "${TOPIC}" --count "${expected}" --raw
  local pid="${LAST_PID}"
  if ! wait_for_pattern "${LOG_DIR}/${name}.log" "Subscribing to topic" 15; then
    echo "ERROR: subscriber '${name}' did not become ready"
    cat "${LOG_DIR}/${name}.log" || true
    return 1
  fi
}

run_record() {
  local name="$1"
  local output="$2"
  shift 2
  local log_file="${LOG_DIR}/${name}.out"
  set +e
  "${RECORD_BIN}" --group "${RECORDED_GROUP}" \
    --zenoh-config "${ZENOH_CONFIG_FILE}" -o "${output}" "$@" \
    >"${log_file}" 2>&1
  local rc=$?
  set -e
  if ((rc != 0)); then
    echo "ERROR: recorder '${name}' exited ${rc}"
    cat "${log_file}" || true
    return 1
  fi
  echo "recorder ${name}: exit=${rc}"
}

run_play_and_verify() {
  local name="$1"
  local wrapper="$2"
  local input="$3"
  local target_group="$4"
  local expected="$5"
  shift 5
  local log_file="${LOG_DIR}/${name}.out"
  local -a player_env=("$@")
  local -a command

  if [[ "${wrapper}" == "standalone" ]]; then
    command=("${PLAY_BIN}" "${input}" \
      --zenoh-config "${ZENOH_CONFIG_FILE}" \
      --rate 1000 --match-timeout 2 --preflight-timeout 2)
  else
    command=("${CLI_BIN}" --zenoh-config "${ZENOH_CONFIG_FILE}")
    if [[ "${target_group}" != "-" ]]; then
      command+=(--group "${target_group}")
    fi
    command+=(play "${input}" --rate 1000 --match-timeout 2 --preflight-timeout 2)
  fi
  if [[ "${target_group}" != "-" && "${wrapper}" == "standalone" ]]; then
    command+=(--group "${target_group}")
  fi

  local subscriber_group="${RECORDED_GROUP}"
  if [[ "${target_group}" != "-" ]]; then
    subscriber_group="${target_group}"
  fi
  start_subscriber "${name}-subscriber" "${subscriber_group}" "${expected}"
  local subscriber_pid="${LAST_PID}"

  set +e
  env "${player_env[@]}" "${command[@]}" >"${log_file}" 2>&1
  local player_rc=$?
  set -e
  if ((player_rc != 0)); then
    echo "ERROR: ${wrapper} player '${name}' exited ${player_rc}"
    cat "${log_file}" || true
    return 1
  fi

  if ! wait_for_child "${subscriber_pid}" 20; then
    echo "ERROR: subscriber for '${name}' did not exit within 20 seconds"
    cat "${LOG_DIR}/${name}-subscriber.log" || true
    return 1
  fi
  local subscriber_rc="${CHILD_RC}"
  if ((subscriber_rc != 0)); then
    echo "ERROR: subscriber for '${name}' exited ${subscriber_rc}"
    cat "${LOG_DIR}/${name}-subscriber.log" || true
    return 1
  fi
  assert_message_count "${LOG_DIR}/${name}-subscriber.log" "${expected}"
  echo "play ${name}: player_exit=${player_rc} subscriber_exit=${subscriber_rc} messages>=${expected}"
}

run_cli_mode_and_logging_check() {
  local name="$1"
  local expected_mode="$2"
  local no_client_override="$3"
  local log_file="${LOG_DIR}/${name}.out"
  local -a command=("${CLI_BIN}" --zenoh-config "${ZENOH_CONFIG_FILE}" --group "${RECORDED_GROUP}")
  if [[ "${no_client_override}" == "yes" ]]; then
    command+=(--no-client-override)
  fi
  command+=(play "${MCAP_MAIN}" --rate 1000 --match-timeout 2 --preflight-timeout 2 --log-level debug)

  start_subscriber "${name}-subscriber" "${RECORDED_GROUP}" 3
  local subscriber_pid="${LAST_PID}"
  set +e
  "${command[@]}" >"${log_file}" 2>&1
  local player_rc=$?
  set -e
  if ((player_rc != 0)); then
    echo "ERROR: cli mode/logging check '${name}' exited ${player_rc}"
    cat "${log_file}" || true
    return 1
  fi
  if ! wait_for_child "${subscriber_pid}" 20; then
    echo "ERROR: subscriber for '${name}' did not exit within 20 seconds"
    cat "${LOG_DIR}/${name}-subscriber.log" || true
    return 1
  fi
  if ((CHILD_RC != 0)); then
    echo "ERROR: subscriber for '${name}' exited ${CHILD_RC}"
    cat "${LOG_DIR}/${name}-subscriber.log" || true
    return 1
  fi
  assert_message_count "${LOG_DIR}/${name}-subscriber.log" 3
  if ! grep -Fq "zenoh.mode=\"${expected_mode}\"" "${log_file}"; then
    echo "ERROR: ${name} did not construct zenoh.mode=\"${expected_mode}\""
    cat "${log_file}" || true
    return 1
  fi
  if ! grep -Fq "aorta-play: snapshot ready" "${log_file}"; then
    echo "ERROR: ${name} did not emit the debug snapshot line"
    cat "${log_file}" || true
    return 1
  fi
  echo "cli ${name}: mode=${expected_mode} debug_log=present player_exit=${player_rc}"
}

run_forced_exit() {
  local name="$1"
  local wrapper="$2"
  local block_iterator="$3"
  local log_file="${LOG_DIR}/${name}.log"
  local -a command
  if [[ "${wrapper}" == "standalone" ]]; then
    command=("${PLAY_FAULT_BIN}" "${MCAP_MAIN}" --group "${RECORDED_GROUP}" \
      --zenoh-config "${ZENOH_CONFIG_FILE}" --no-client-override \
      --match-timeout 0 --preflight-timeout 2)
  else
    command=("${CLI_FAULT_BIN}" --no-client-override --group "${RECORDED_GROUP}" \
      --zenoh-config "${ZENOH_CONFIG_FILE}" play "${MCAP_MAIN}" \
      --match-timeout 0 --preflight-timeout 2)
  fi
  local -a env_args=("AORTA_PLAY_TEST_WEDGE=1")
  if [[ "${block_iterator}" == "yes" ]]; then
    env_args+=("AORTA_PLAY_TEST_BLOCK_SIGNAL_ITERATOR=1")
  fi

  start_bg "${name}" env "${env_args[@]}" "${command[@]}"
  local pid="${LAST_PID}"
  if ! wait_for_pattern "${log_file}" "AORTA_PLAY_TEST_WEDGE_ARMED=1" 15; then
    echo "ERROR: forced-exit child '${name}' did not reach wedge"
    cat "${log_file}" || true
    return 1
  fi
  if [[ "${block_iterator}" == "yes" ]] && ! wait_for_pattern "${log_file}" "AORTA_PLAY_TEST_SIGNAL_ITERATOR_BLOCKED=1" 15; then
    echo "ERROR: forced-exit child '${name}' did not block its iterator"
    cat "${log_file}" || true
    return 1
  fi

  kill -INT "${pid}"
  if [[ "${block_iterator}" == "yes" ]] && ! wait_for_pattern "${log_file}" "AORTA_PLAY_TEST_SIGNAL_ARMED=1" 15; then
    echo "ERROR: forced-exit child '${name}' did not acknowledge first SIGINT"
    cat "${log_file}" || true
    return 1
  fi
  sleep 0.5
  assert_running "${pid}"

  kill -INT "${pid}"
  local waited=0
  while kill -0 "${pid}" 2>/dev/null && ((waited < 150)); do
    sleep 0.1
    waited=$((waited + 1))
  done
  if kill -0 "${pid}" 2>/dev/null; then
    echo "ERROR: forced-exit child '${name}' did not exit within 15 seconds"
    return 1
  fi
  set +e
  wait "${pid}"
  local rc=$?
  set -e
  if ((rc != 130)); then
    echo "ERROR: forced-exit child '${name}' exited ${rc}, expected 130"
    cat "${log_file}" || true
    return 1
  fi
  echo "forced-exit ${name}: first SIGINT graceful, second SIGINT exit=${rc}"
}

echo "=== Aorta Player ${PROFILE} e2e ==="
echo "recorded_group=${RECORDED_GROUP}"
echo "topic=${TOPIC}"
echo "log_dir=${LOG_DIR}"

echo "[setup] starting cli_talker..."
start_bg "talker" \
  env "AORTA_GROUP=${RECORDED_GROUP}" \
  "ZENOH_SESSION_CONFIG_URI=${ZENOH_CONFIG_FILE}" \
  "${TALKER_BIN}" --name "${TALKER_NAME}" --group "${RECORDED_GROUP}" \
  --topic "${TOPIC}" --schema-attach always
TALKER_PID="${LAST_PID}"
if ! wait_for_pattern "${LOG_DIR}/talker.log" "schema registered hash=" 15; then
  echo "ERROR: talker did not become ready"
  cat "${LOG_DIR}/talker.log" || true
  exit 1
fi

echo "[record] complete MCAP"
run_record main "${MCAP_MAIN}" --include "${TOPIC_SUFFIX}" --duration 8 --max-messages 4
if [[ ! -s "${MCAP_MAIN}" ]]; then
  echo "ERROR: complete recording is missing or empty"
  exit 1
fi

echo "[record] no-summary MCAP"
run_record no_summary "${MCAP_NO_SUMMARY}" --include "${TOPIC_SUFFIX}" \
  --duration 8 --max-messages 4 --no-summary
if [[ ! -s "${MCAP_NO_SUMMARY}" ]]; then
  echo "ERROR: no-summary recording is missing or empty"
  exit 1
fi

kill -INT "${TALKER_PID}" 2>/dev/null || true
sleep 0.5

echo "[play] standalone metadata group and no-summary input"
run_play_and_verify standalone_metadata standalone "${MCAP_MAIN}" - 3
run_play_and_verify standalone_no_summary standalone "${MCAP_NO_SUMMARY}" - 3

echo "[play] aorta-cli group precedence triple"
run_play_and_verify cli_explicit cli "${MCAP_MAIN}" "${OVERRIDE_GROUP}" 3 \
  "AORTA_GROUP=${AMBIENT_GROUP}"
run_play_and_verify cli_ambient_only cli "${MCAP_MAIN}" - 3 \
  "AORTA_GROUP=${AMBIENT_GROUP}"
run_play_and_verify cli_recorded_only cli "${MCAP_MAIN}" - 3

echo "[play] aorta-cli client-mode override and debug logging"
run_cli_mode_and_logging_check cli_mode_default client no
run_cli_mode_and_logging_check cli_mode_preserved peer yes

echo "[play] live non-rotating recorder must fail snapshot verification"
start_bg "live_talker" \
  env "AORTA_GROUP=${RECORDED_GROUP}" \
  "ZENOH_SESSION_CONFIG_URI=${ZENOH_CONFIG_FILE}" \
  "${TALKER_BIN}" --name "${TALKER_NAME}_live" --group "${RECORDED_GROUP}" \
  --topic "${TOPIC}" --schema-attach always
LIVE_TALKER_PID="${LAST_PID}"
if ! wait_for_pattern "${LOG_DIR}/live_talker.log" "schema registered hash=" 15; then
  echo "ERROR: live talker did not become ready"
  cat "${LOG_DIR}/live_talker.log" || true
  exit 1
fi
start_bg "live_recorder" env "ZENOH_SESSION_CONFIG_URI=${ZENOH_CONFIG_FILE}" \
  "${RECORD_BIN}" --group "${RECORDED_GROUP}" --zenoh-config "${ZENOH_CONFIG_FILE}" \
  -o "${MCAP_LIVE}" --include "${TOPIC_SUFFIX}"
LIVE_RECORDER_PID="${LAST_PID}"
if ! wait_for_file_nonempty "${MCAP_LIVE}" 15; then
  echo "ERROR: live recorder did not write an input"
  exit 1
fi
set +e
env AORTA_PLAY_TEST_SNAPSHOT_DELAY_MS=1000 "${PLAY_FAULT_BIN}" "${MCAP_LIVE}" \
  --group "${RECORDED_GROUP}" --zenoh-config "${ZENOH_CONFIG_FILE}" \
  --match-timeout 0 --preflight-timeout 2 \
  >"${LOG_DIR}/live_player.out" 2>&1
LIVE_PLAYER_RC=$?
set -e
if ((LIVE_PLAYER_RC == 0)) || ! grep -Fq "input changed during snapshot" "${LOG_DIR}/live_player.out"; then
  echo "ERROR: live recorder snapshot test did not reject the changing input (exit=${LIVE_PLAYER_RC})"
  cat "${LOG_DIR}/live_player.out" || true
  exit 1
fi
echo "live snapshot refusal: player_exit=${LIVE_PLAYER_RC}"
kill -INT "${LIVE_RECORDER_PID}" 2>/dev/null || true
kill -INT "${LIVE_TALKER_PID}" 2>/dev/null || true

echo "[signals] standalone wrapper"
run_forced_exit standalone_forced_exit standalone no
run_forced_exit standalone_forced_exit_armed standalone yes
echo "[signals] aorta-cli wrapper"
run_forced_exit cli_forced_exit cli no
run_forced_exit cli_forced_exit_armed cli yes

echo "PASS: aorta-play record -> play -> subscribe, group precedence, no-summary, live snapshot refusal, and forced-exit signal tests"
