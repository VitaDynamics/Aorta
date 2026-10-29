#!/usr/bin/env bash
# End-to-end smoke/regression test for aorta-cli (developer-run or Bazel sh_test).

set -euo pipefail
# Silence job control notifications for background process cleanup.
set +b
set +m

AORTA_BIN=""
TALKER_BIN=""
SERVER_BIN=""
ACTION_SERVER_BIN=""
PROFILE="quick"

for arg in "$@"; do
  case "${arg}" in
    --aorta-bin=*)
      AORTA_BIN="${arg#*=}"
      ;;
    --talker-bin=*)
      TALKER_BIN="${arg#*=}"
      ;;
    --server-bin=*)
      SERVER_BIN="${arg#*=}"
      ;;
    --action-server-bin=*)
      ACTION_SERVER_BIN="${arg#*=}"
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

if [[ -z "${AORTA_BIN}" || -z "${TALKER_BIN}" || -z "${SERVER_BIN}" || -z "${ACTION_SERVER_BIN}" ]]; then
  cat <<EOF
ERROR: missing required binaries.
Expected:
  --aorta-bin=<path>
  --talker-bin=<path>
  --server-bin=<path>
  --action-server-bin=<path>
EOF
  exit 2
fi

if [[ "${PROFILE}" != "quick" && "${PROFILE}" != "full" ]]; then
  echo "ERROR: unsupported profile '${PROFILE}' (expected quick|full)"
  exit 2
fi

for bin in "${AORTA_BIN}" "${TALKER_BIN}" "${SERVER_BIN}" "${ACTION_SERVER_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    echo "ERROR: binary not executable: ${bin}"
    exit 2
  fi
done

GROUP="${AORTA_CLI_TEST_GROUP:-default}"
RAW_RUN_ID="${AORTA_CLI_TEST_RUN_ID:-$$_$RANDOM}"
RUN_ID="$(echo "${RAW_RUN_ID}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9_]+/_/g')"
if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="run"
fi
if [[ ! "${RUN_ID}" =~ ^[a-z] ]]; then
  RUN_ID="r_${RUN_ID}"
fi
TOPIC="${AORTA_CLI_TEST_TOPIC:-/cli/${RUN_ID}/chatter}"
SERVICE="${AORTA_CLI_TEST_SERVICE:-/cli/${RUN_ID}/echo_schema}"
TALKER_NAME="${AORTA_CLI_TEST_TALKER_NAME:-cli_talker_${RUN_ID}}"
SERVER_NAME="${AORTA_CLI_TEST_SERVER_NAME:-cli_echo_server_${RUN_ID}}"
ACTION="${AORTA_CLI_TEST_ACTION:-/example/flow_resume_probe}"
ACTION_SERVER_NAME="${AORTA_CLI_TEST_ACTION_SERVER_NAME:-cli_execute_task_server_${RUN_ID}}"
REQUEST_JSON="${AORTA_CLI_TEST_REQUEST_JSON:-{\"message\":\"hello cli smoke\"}}"
ACTION_QUICK_JSON='{"source":"quick","input_type":"ProbePreset","input":{"path":"demo/quick","args":[]}}'
ACTION_SLOW_JSON='{"source":"slow","input_type":"ProbePreset","input":{"path":"demo/slow","args":[]}}'
ACTION_CANCEL_JSON='{"source":"cancel","input_type":"ProbePreset","input":{"path":"demo/cancel","args":[]}}'

TOPIC_SUFFIX="${TOPIC#/}"
SERVICE_SUFFIX="${SERVICE#/}"
TOPIC_DISPLAY="/${TOPIC_SUFFIX}"
SERVICE_DISPLAY="/${SERVICE_SUFFIX}"
ACTION_SUFFIX="${ACTION#/}"
ACTION_DISPLAY="/${ACTION_SUFFIX}"
TEST_TMP_ROOT="${TEST_TMPDIR:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-cli-e2e.XXXXXX")}"
LOG_DIR="${AORTA_CLI_TEST_LOG_DIR:-${TEST_TMP_ROOT}/aorta-cli-${RUN_ID}}"
mkdir -p "${LOG_DIR}"
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

START_BG_PID=""

start_bg() {
  local name="$1"
  shift
  local log_file="${LOG_DIR}/${name}.log"
  "$@" >"${log_file}" 2>&1 &
  START_BG_PID=$!
  PIDS+=("${START_BG_PID}")
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

run_cli_capture() {
  local name="$1"
  shift
  local log_file="${LOG_DIR}/${name}.out"
  if ! "${AORTA_BIN}" --group "${GROUP}" "$@" >"${log_file}" 2>&1; then
    echo "ERROR: command failed: ${AORTA_BIN} --group ${GROUP} $*"
    cat "${log_file}" || true
    return 1
  fi
  cat "${log_file}"
}

run_cli_capture_for_seconds() {
  local name="$1"
  local seconds="$2"
  shift 2
  local log_file="${LOG_DIR}/${name}.out"
  "${AORTA_BIN}" --group "${GROUP}" "$@" >"${log_file}" 2>&1 &
  local pid=$!
  sleep "${seconds}"
  stop_process "${pid}"
  cat "${log_file}"
}

extract_prefixed_value() {
  local text="$1"
  local prefix="$2"
  awk -v prefix="${prefix}: " 'index($0, prefix) == 1 { print substr($0, length(prefix) + 1); exit }' <<<"${text}"
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

assert_not_contains() {
  local text="$1" expected="$2" what="$3"
  if [[ "${text}" == *"${expected}"* ]]; then
    echo "ERROR: unexpected '${expected}' in ${what}"
    exit 1
  fi
}

assert_cli_contains_retry() {
  local name="$1"
  local expected="$2"
  local timeout_s="$3"
  shift 3
  local attempts=$((timeout_s * 2))
  local output=""
  for ((i=1; i<=attempts; i++)); do
    if output="$(run_cli_capture "${name}_attempt_${i}" "$@")"; then
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

echo "=== Aorta CLI ${PROFILE} e2e ==="
echo "group=${GROUP}"
echo "topic=${TOPIC}"
echo "service=${SERVICE}"
echo "action=${ACTION}"
echo "log_dir=${LOG_DIR}"

echo "[setup] starting cli_talker, cli_echo_server, and cli_execute_task_server..."
start_bg "talker" \
  "${TALKER_BIN}" \
  --name "${TALKER_NAME}" \
  --group "${GROUP}" \
  --topic "${TOPIC}" \
  --schema-attach always
talker_pid="${START_BG_PID}"
start_bg "server" \
  "${SERVER_BIN}" \
  --name "${SERVER_NAME}" \
  --group "${GROUP}" \
  --service "${SERVICE}" \
  --schema-attach always
server_pid="${START_BG_PID}"
start_bg "action_server" \
  "${ACTION_SERVER_BIN}" \
  --name "${ACTION_SERVER_NAME}" \
  --group "${GROUP}"
action_server_pid="${START_BG_PID}"

if ! wait_for_pattern "${LOG_DIR}/talker.log" "schema registered hash=" 12; then
  echo "ERROR: talker did not become ready in time"
  cat "${LOG_DIR}/talker.log" || true
  exit 1
fi
if ! wait_for_pattern "${LOG_DIR}/server.log" "Echo service is ready." 12; then
  echo "ERROR: service server did not become ready in time"
  cat "${LOG_DIR}/server.log" || true
  exit 1
fi
if ! wait_for_pattern "${LOG_DIR}/action_server.log" "Action server is ready." 12; then
  echo "ERROR: action server did not become ready in time"
  cat "${LOG_DIR}/action_server.log" || true
  exit 1
fi
echo "[setup] publishers/services/actions ready"

echo "[check] topic list"
topic_list_out="$(assert_cli_contains_retry topic_list "${TOPIC_DISPLAY}" 8 topic list --timeout 2)"
assert_contains "${topic_list_out}" "Active topics:" "topic list"
# The default rows are the pre-#388 bare names; scripts line-match them.
assert_contains "${topic_list_out}" "  ${TOPIC_DISPLAY}" "topic list row shape"
assert_not_contains "${topic_list_out}" "  live  " "topic list default row shape"

echo "[check] topic echo"
topic_echo_out="$(run_cli_capture topic_echo topic echo "${TOPIC}" --count 2)"
assert_contains "${topic_echo_out}" "--- Message 1 ---" "topic echo"
assert_contains "${topic_echo_out}" "${TALKER_NAME}" "topic echo"

echo "[check] service list"
service_list_out="$(assert_cli_contains_retry service_list "${SERVICE_DISPLAY}" 8 service list)"
assert_contains "${service_list_out}" "Active services in group" "service list"
assert_contains "${service_list_out}" "  ${SERVICE_DISPLAY}" "service list row shape"
assert_not_contains "${service_list_out}" "  live  " "service list default row shape"

echo "[check] service call"
service_call_out="$(run_cli_capture service_call service call "${SERVICE}" "${REQUEST_JSON}" --timeout 2)"
assert_contains "${service_call_out}" "Service reply:" "service call"
assert_contains "${service_call_out}" "echo from ${SERVER_NAME}" "service call"

echo "[check] action list"
action_list_out="$(assert_cli_contains_retry action_list "${ACTION_DISPLAY}" 8 action list)"
assert_contains "${action_list_out}" "${ACTION_SERVER_NAME}" "action list"

echo "[check] action info"
action_info_out="$(run_cli_capture action_info action info "${ACTION}")"
assert_contains "${action_info_out}" "Server node: ${ACTION_SERVER_NAME}" "action info"
assert_contains "${action_info_out}" "Supports cancel: yes" "action info"

echo "[check] action send + wait"
action_send_out="$(run_cli_capture action_send action send "${ACTION}" --json "${ACTION_QUICK_JSON}")"
assert_contains "${action_send_out}" "Accepted: yes" "action send"
assert_contains "${action_send_out}" "Next steps:" "action send"
assert_contains "${action_send_out}" "action wait ${ACTION}" "action send"
quick_goal_id="$(extract_prefixed_value "${action_send_out}" "Goal ID")"
quick_goal_secret="$(extract_prefixed_value "${action_send_out}" "Goal Secret")"
if [[ -z "${quick_goal_id}" || -z "${quick_goal_secret}" ]]; then
  echo "ERROR: failed to parse Goal ID / Goal Secret from action send output"
  echo "${action_send_out}"
  exit 1
fi
action_wait_out="$(run_cli_capture action_wait action wait "${ACTION}" --goal-id "${quick_goal_id}" --goal-secret "${quick_goal_secret}" --timeout 10)"
assert_contains "${action_wait_out}" "Status: SUCCEEDED" "action wait"
assert_contains "${action_wait_out}" "\"duration_ms\"" "action wait"

echo "[check] schema get (service)"
schema_srv_out="$(run_cli_capture schema_service schema get "srv/${SERVICE_SUFFIX}")"
assert_contains "${schema_srv_out}" "Kind: service" "schema get service"
assert_contains "${schema_srv_out}" "Service hash:" "schema get service"

echo "[check] nodes list"
nodes_out="$(assert_cli_contains_retry nodes_list "${TALKER_NAME}" 8 nodes list)"
assert_contains "${nodes_out}" "${SERVER_NAME}" "nodes list"

if [[ "${PROFILE}" == "full" ]]; then
  echo "[check-full] service info"
  service_info_out="$(run_cli_capture service_info service info "${SERVICE}")"
  assert_contains "${service_info_out}" "Provider endpoints:" "service info"
  assert_contains "${service_info_out}" "Provider nodes:" "service info"

  echo "[check-full] topic list verbose"
  topic_list_verbose_out="$(run_cli_capture topic_list_verbose topic list --timeout 2 --verbose)"
  assert_contains "${topic_list_verbose_out}" "Topic provider details:" "topic list verbose"
  assert_contains "${topic_list_verbose_out}" "endpoints:" "topic list verbose"
  assert_contains "${topic_list_verbose_out}" "${TOPIC_DISPLAY}  live  " "topic list verbose row shape"

  echo "[check-full] topic list node filter"
  topic_list_node_out="$(run_cli_capture topic_list_node topic list --timeout 2 --node "${TALKER_NAME}")"
  assert_contains "${topic_list_node_out}" "${TOPIC_DISPLAY}" "topic list node filter"

  echo "[check-full] service list verbose"
  service_list_verbose_out="$(run_cli_capture service_list_verbose service list --verbose)"
  assert_contains "${service_list_verbose_out}" "Service provider details:" "service list verbose"
  assert_contains "${service_list_verbose_out}" "nodes:" "service list verbose"
  assert_contains "${service_list_verbose_out}" "${SERVICE_DISPLAY}  live  " "service list verbose row shape"

  echo "[check-full] service list node filter"
  service_list_node_out="$(run_cli_capture service_list_node service list --node "${SERVER_NAME}")"
  assert_contains "${service_list_node_out}" "${SERVICE_DISPLAY}" "service list node filter"

  echo "[check-full] topic hz"
  topic_hz_out="$(run_cli_capture_for_seconds topic_hz 4 topic hz "${TOPIC}" --window 128)"
  assert_contains "${topic_hz_out}" "average rate:" "topic hz"
  assert_contains "${topic_hz_out}" "window:" "topic hz"

  echo "[check-full] topic info"
  topic_info_out="$(run_cli_capture topic_info topic info "${TOPIC}")"
  assert_contains "${topic_info_out}" "Schema hash:" "topic info"
  assert_contains "${topic_info_out}" "Present in admin snapshot: true" "topic info"

  echo "[check-full] schema get (topic)"
  schema_topic_out="$(run_cli_capture schema_topic schema get "${TOPIC}")"
  assert_contains "${schema_topic_out}" "Kind: topic" "schema get topic"

  echo "[check-full] nodes info"
  nodes_info_out="$(run_cli_capture nodes_info nodes info "${SERVER_NAME}")"
  assert_contains "${nodes_info_out}" "Services (" "nodes info"
  assert_contains "${nodes_info_out}" "${SERVICE_DISPLAY}" "nodes info"

  echo "[check-full] nodes list verbose"
  nodes_list_verbose_out="$(run_cli_capture nodes_list_verbose nodes list --verbose)"
  assert_contains "${nodes_list_verbose_out}" "ENDPOINTS" "nodes list verbose"
  assert_contains "${nodes_list_verbose_out}" "endpoints:" "nodes list verbose"

  echo "[check-full] nodes list with-resources"
  nodes_list_resources_out="$(run_cli_capture nodes_list_resources nodes list --with-resources)"
  assert_contains "${nodes_list_resources_out}" "TOPICS" "nodes list with-resources"
  assert_contains "${nodes_list_resources_out}" "SERVICES" "nodes list with-resources"
  assert_contains "${nodes_list_resources_out}" "${SERVER_NAME}" "nodes list with-resources"

  echo "[check-full] action send (slow goal)"
  action_send_slow_out="$(run_cli_capture action_send_slow action send "${ACTION}" --json "${ACTION_SLOW_JSON}")"
  assert_contains "${action_send_slow_out}" "Accepted: yes" "action send slow"
  assert_contains "${action_send_slow_out}" "Next steps:" "action send slow"
  slow_goal_id="$(extract_prefixed_value "${action_send_slow_out}" "Goal ID")"
  slow_goal_secret="$(extract_prefixed_value "${action_send_slow_out}" "Goal Secret")"
  if [[ -z "${slow_goal_id}" || -z "${slow_goal_secret}" ]]; then
    echo "ERROR: failed to parse Goal ID / Goal Secret from slow action send output"
    echo "${action_send_slow_out}"
    exit 1
  fi

  echo "[check-full] action status"
  action_status_out="$(assert_cli_contains_retry action_status "${slow_goal_id}" 6 action status "${ACTION}" --goal-id "${slow_goal_id}")"
  assert_contains "${action_status_out}" "Status:" "action status"

  echo "[check-full] action goals"
  action_goals_out="$(assert_cli_contains_retry action_goals "${slow_goal_id}" 6 action goals "${ACTION}")"
  assert_contains "${action_goals_out}" "Active goals for action" "action goals"

  echo "[check-full] action feedback"
  action_feedback_out="$(run_cli_capture_for_seconds action_feedback 2 action feedback "${ACTION}" --goal-id "${slow_goal_id}")"
  assert_contains "${action_feedback_out}" "Goal ID: ${slow_goal_id}" "action feedback"
  assert_contains "${action_feedback_out}" "Data:" "action feedback"

  echo "[check-full] action wait (slow goal)"
  action_wait_slow_out="$(run_cli_capture action_wait_slow action wait "${ACTION}" --goal-id "${slow_goal_id}" --goal-secret "${slow_goal_secret}" --timeout 15)"
  assert_contains "${action_wait_slow_out}" "Status: SUCCEEDED" "action wait slow"

  echo "[check-full] action result"
  action_result_out="$(run_cli_capture action_result action result "${ACTION}" --goal-id "${slow_goal_id}" --goal-secret "${slow_goal_secret}")"
  assert_contains "${action_result_out}" "Status: SUCCEEDED" "action result"
  assert_contains "${action_result_out}" "\"duration_ms\"" "action result"

  echo "[check-full] action send + cancel"
  action_send_cancel_out="$(run_cli_capture action_send_cancel action send "${ACTION}" --json "${ACTION_CANCEL_JSON}")"
  assert_contains "${action_send_cancel_out}" "Accepted: yes" "action send cancel"
  cancel_goal_id="$(extract_prefixed_value "${action_send_cancel_out}" "Goal ID")"
  cancel_goal_secret="$(extract_prefixed_value "${action_send_cancel_out}" "Goal Secret")"
  if [[ -z "${cancel_goal_id}" || -z "${cancel_goal_secret}" ]]; then
    echo "ERROR: failed to parse Goal ID / Goal Secret from cancel action send output"
    echo "${action_send_cancel_out}"
    exit 1
  fi
  sleep 0.5
  action_cancel_out="$(run_cli_capture action_cancel action cancel "${ACTION}" --goal-id "${cancel_goal_id}" --goal-secret "${cancel_goal_secret}")"
  assert_contains "${action_cancel_out}" "Accepted: yes" "action cancel"
  action_wait_cancel_out="$(run_cli_capture action_wait_cancel action wait "${ACTION}" --goal-id "${cancel_goal_id}" --goal-secret "${cancel_goal_secret}" --timeout 15)"
  assert_contains "${action_wait_cancel_out}" "Status: CANCELED" "action wait cancel"

  echo "[check-full] schema dump"
  dump_base="${LOG_DIR}/schema_dump/service_schema"
  dump_out="$(run_cli_capture schema_dump schema get "srv/${SERVICE_SUFFIX}" --dump-bfbs "${dump_base}")"
  assert_contains "${dump_out}" "Dumped bfbs to" "schema dump"
  [[ -f "${dump_base}" ]] || { echo "ERROR: missing ${dump_base}"; exit 1; }

  echo "[check-full] clean shm"
  clean_out="$(run_cli_capture clean_shm clean shm --stale-timeout 1)"
  if [[ "${clean_out}" != *"No SHM directory found"* && "${clean_out}" != *"Scanned "* ]]; then
    echo "ERROR: unexpected clean shm output"
    echo "${clean_out}"
    exit 1
  fi

  echo "[check-full] nodes watch OFFLINE event"
  watch_log="${LOG_DIR}/nodes_watch.log"
  "${AORTA_BIN}" --group "${GROUP}" nodes list --watch >"${watch_log}" 2>&1 &
  watch_pid=$!
  PIDS+=("${watch_pid}")
  sleep 1

  stop_process "${talker_pid}"
  talker_pid=0
  if ! wait_for_pattern "${watch_log}" "OFFLINE" 10; then
    echo "ERROR: nodes watch did not report OFFLINE event"
    cat "${watch_log}" || true
    exit 1
  fi
  if ! grep -Fq "${TALKER_NAME}" "${watch_log}"; then
    echo "ERROR: nodes watch OFFLINE event does not include expected node '${TALKER_NAME}'"
    cat "${watch_log}" || true
    exit 1
  fi
fi

echo "PASS: aorta-cli ${PROFILE} e2e"
