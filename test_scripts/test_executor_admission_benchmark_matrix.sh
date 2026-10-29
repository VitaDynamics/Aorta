#!/usr/bin/env bash
# Executor admission/scheduler benchmark matrix (developer-run, prebuilt binaries only).

set -euo pipefail
# Ensure async child termination does not print noisy job-control notifications.
set +b
set +m

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

LISTENER_BIN="${REPO_ROOT}/bazel-bin/rust/examples/perf_listener"
TALKER_BIN="${REPO_ROOT}/bazel-bin/rust/examples/perf_talker"
RPC_CLIENT_BIN="${REPO_ROOT}/bazel-bin/rust/examples/echo_client"

DURATION_SEC="${AORTA_BENCH_DURATION_SEC:-20}"
WARMUP_SEC="${AORTA_BENCH_WARMUP_SEC:-2}"
REPORT_INTERVAL_MS="${AORTA_BENCH_REPORT_INTERVAL_MS:-1000}"
MAX_SAMPLES="${AORTA_BENCH_MAX_SAMPLES:-20000}"
TALKER_RATE_HZ="${AORTA_BENCH_TALKER_RATE_HZ:-2000}"
PAYLOAD_SIZE="${AORTA_BENCH_PAYLOAD_SIZE:-1024}"
EXECUTOR_QUEUE="${AORTA_BENCH_EXECUTOR_QUEUE:-4096}"
RPC_INTERVAL_MS="${AORTA_BENCH_RPC_INTERVAL_MS:-20}"
TIMER_PERIOD_MS="${AORTA_BENCH_TIMER_PERIOD_MS:-10}"
SERVICE_REPLY_DELAY_MS="${AORTA_BENCH_SERVICE_REPLY_DELAY_MS:-0}"

INGRESS_MATRIX="${AORTA_BENCH_INGRESS_MATRIX:-off on}"
SCHEDULER_MATRIX="${AORTA_BENCH_SCHEDULER_MATRIX:-fifo priority}"
LOAD_MATRIX="${AORTA_BENCH_LOAD_MATRIX:-sub_only mixed}"
DRIVE_MODE_MATRIX="${AORTA_BENCH_DRIVE_MODE_MATRIX:-spin_once spin}"
SPIN_ONCE_TIMEOUT_MS="${AORTA_BENCH_SPIN_ONCE_TIMEOUT_MS:-10}"
SPIN_ONCE_PROFILE="${AORTA_BENCH_SPIN_ONCE_PROFILE:-adaptive}"

SESSION_CONFIG_URI="${AORTA_BENCH_ZENOH_SESSION_CONFIG_URI:-}"
HEADER_INCLUDE_PUBLISHER_NODE="${AORTA_BENCH_HEADER_INCLUDE_PUBLISHER_NODE:-1}"
LOG_ROOT="${AORTA_BENCH_LOG_ROOT:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-executor-bench.XXXXXX")}"
SUMMARY_CSV="${LOG_ROOT}/summary.csv"

extract_kv() {
  local line="$1"
  local key="$2"
  echo "${line}" | sed -n "s/.*${key}=\\([^ ]*\\).*/\\1/p"
}

avg_file() {
  local file="$1"
  if [[ ! -s "${file}" ]]; then
    echo "NA"
    return
  fi
  awk '{sum+=$1; n+=1} END {if (n>0) printf "%.2f", sum/n; else print "NA"}' "${file}"
}

last_nonzero_perf_line() {
  local file="$1"
  rg 'perf_stats count=' "${file}" | awk '
    {
      rate = 0;
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^rate=/) {
          x = $i;
          sub(/^rate=/, "", x);
          sub(/msg\/s$/, "", x);
          rate = x + 0;
        }
      }
      if (rate > 0) {
        line = $0;
      }
    }
    END {
      if (line != "") print line;
    }
  '
}

wait_for_exit() {
  local pid="$1"
  local timeout_sec="$2"
  local ticks=0
  local max_ticks=$((timeout_sec * 10))
  while kill -0 "${pid}" 2>/dev/null; do
    if (( ticks >= max_ticks )); then
      return 1
    fi
    sleep 0.1
    ticks=$((ticks + 1))
  done
  return 0
}

stop_process() {
  local pid="$1"
  local int_timeout_sec="${2:-3}"
  if [[ "${pid}" -le 0 ]]; then
    return 0
  fi
  if ! kill -0 "${pid}" 2>/dev/null; then
    wait "${pid}" 2>/dev/null || true
    return 0
  fi

  kill -INT "${pid}" 2>/dev/null || true
  if ! wait_for_exit "${pid}" "${int_timeout_sec}"; then
    kill -TERM "${pid}" 2>/dev/null || true
    if ! wait_for_exit "${pid}" 3; then
      kill -KILL "${pid}" 2>/dev/null || true
      wait_for_exit "${pid}" 2 || true
    fi
  fi
  wait "${pid}" 2>/dev/null || true
}

start_cpu_monitor() {
  local pid="$1"
  local out_file="$2"
  (
    while kill -0 "${pid}" 2>/dev/null; do
      cpu=""
      cpu="$(ps -p "${pid}" -o %cpu= 2>/dev/null | awk '{print $1}' || true)"
      if [[ -n "${cpu}" ]]; then
        echo "${cpu}" >>"${out_file}"
      fi
      sleep 1
    done
  ) >/dev/null 2>&1 &
  echo $!
}

for bin in "${LISTENER_BIN}" "${TALKER_BIN}" "${RPC_CLIENT_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    cat <<EOF
ERROR: binary not found or not executable: ${bin}
Please build required targets first (developer-run):
  bazel build //rust/examples:perf_listener
  bazel build //rust/examples:perf_talker
  bazel build //rust/examples:echo_client
EOF
    exit 2
  fi
done

cat >"${SUMMARY_CSV}" <<EOF
case,ingress,scheduler,load,drive_mode,spin_once_timeout_ms,spin_once_profile,rate_msg_s,throughput_mb_s,p50_us,p90_us,p95_us,p99_us,decode_p99_us,peak_events,peak_bytes,drop_full,drop_budget_events,service_calls,timer_ticks,cpu_listener_pct,cpu_talker_pct,cpu_rpc_pct
EOF

echo "=== Aorta Executor Admission Benchmark Matrix ==="
echo "ingress=${INGRESS_MATRIX}"
echo "scheduler=${SCHEDULER_MATRIX}"
echo "load=${LOAD_MATRIX}"
echo "drive_mode=${DRIVE_MODE_MATRIX}"
echo "spin_once_timeout_ms=${SPIN_ONCE_TIMEOUT_MS} spin_once_profile=${SPIN_ONCE_PROFILE}"
echo "duration_sec=${DURATION_SEC} warmup_sec=${WARMUP_SEC}"
echo "talker_rate_hz=${TALKER_RATE_HZ} payload_size=${PAYLOAD_SIZE}B executor_queue=${EXECUTOR_QUEUE}"
echo "header_include_publisher_node=${HEADER_INCLUDE_PUBLISHER_NODE}"
echo "log_root=${LOG_ROOT}"
if [[ -n "${SESSION_CONFIG_URI}" ]]; then
  echo "zenoh_session_config_uri=${SESSION_CONFIG_URI}"
fi

run_case() {
  local ingress="$1"
  local scheduler="$2"
  local load="$3"
  local drive_mode="$4"

  local ingress_flag
  if [[ "${ingress}" == "on" ]]; then
    ingress_flag=1
  elif [[ "${ingress}" == "off" ]]; then
    ingress_flag=0
  else
    echo "ERROR: invalid ingress mode '${ingress}', expected on|off"
    exit 2
  fi

  local case_id="${ingress}_${scheduler}_${load}_${drive_mode}"
  local case_dir="${LOG_ROOT}/${case_id}"
  mkdir -p "${case_dir}"

  local topic="/bench/admission/${case_id}"
  local service="/bench/echo/${case_id}"

  local listener_log="${case_dir}/listener.log"
  local talker_log="${case_dir}/talker.log"
  local rpc_log="${case_dir}/rpc_client.log"
  local cpu_listener_log="${case_dir}/cpu_listener.log"
  local cpu_talker_log="${case_dir}/cpu_talker.log"
  local cpu_rpc_log="${case_dir}/cpu_rpc.log"

  echo ""
  echo "--- Case: ${case_id} ---"
  echo "topic=${topic}"
  echo "service=${service}"

  local -a listener_args=(
    --name "bench_listener_${case_id}"
    --topic "${topic}"
    --report-interval-ms "${REPORT_INTERVAL_MS}"
    --max-samples "${MAX_SAMPLES}"
    --executor-queue "${EXECUTOR_QUEUE}"
    --scheduler "${scheduler}"
    --drive-mode "${drive_mode}"
    --spin-once-timeout-ms "${SPIN_ONCE_TIMEOUT_MS}"
    --spin-once-profile "${SPIN_ONCE_PROFILE}"
  )
  if [[ "${load}" == "mixed" ]]; then
    listener_args+=(
      --service "${service}"
      --service-reply-delay-ms "${SERVICE_REPLY_DELAY_MS}"
      --timer-period-ms "${TIMER_PERIOD_MS}"
    )
  elif [[ "${load}" != "sub_only" ]]; then
    echo "ERROR: invalid load mode '${load}', expected sub_only|mixed"
    exit 2
  fi
  if [[ "${drive_mode}" != "spin_once" && "${drive_mode}" != "spin" ]]; then
    echo "ERROR: invalid drive mode '${drive_mode}', expected spin_once|spin"
    exit 2
  fi

  if [[ -n "${SESSION_CONFIG_URI}" ]]; then
    env ZENOH_SESSION_CONFIG_URI="${SESSION_CONFIG_URI}" AORTA_EXECUTOR_MULTI_INGRESS="${ingress_flag}" AORTA_HEADER_INCLUDE_PUBLISHER_NODE="${HEADER_INCLUDE_PUBLISHER_NODE}" \
      "${LISTENER_BIN}" "${listener_args[@]}" >"${listener_log}" 2>&1 &
  else
    env AORTA_EXECUTOR_MULTI_INGRESS="${ingress_flag}" AORTA_HEADER_INCLUDE_PUBLISHER_NODE="${HEADER_INCLUDE_PUBLISHER_NODE}" \
      "${LISTENER_BIN}" "${listener_args[@]}" >"${listener_log}" 2>&1 &
  fi
  local listener_pid=$!

  if [[ -n "${SESSION_CONFIG_URI}" ]]; then
    env ZENOH_SESSION_CONFIG_URI="${SESSION_CONFIG_URI}" AORTA_EXECUTOR_MULTI_INGRESS="${ingress_flag}" AORTA_HEADER_INCLUDE_PUBLISHER_NODE="${HEADER_INCLUDE_PUBLISHER_NODE}" \
      "${TALKER_BIN}" \
      --name "bench_talker_${case_id}" \
      --topic "${topic}" \
      --size "${PAYLOAD_SIZE}" \
      --rate-hz "${TALKER_RATE_HZ}" \
      --publish-api builder \
      --pattern increment >"${talker_log}" 2>&1 &
  else
    env AORTA_EXECUTOR_MULTI_INGRESS="${ingress_flag}" AORTA_HEADER_INCLUDE_PUBLISHER_NODE="${HEADER_INCLUDE_PUBLISHER_NODE}" \
      "${TALKER_BIN}" \
      --name "bench_talker_${case_id}" \
      --topic "${topic}" \
      --size "${PAYLOAD_SIZE}" \
      --rate-hz "${TALKER_RATE_HZ}" \
      --publish-api builder \
      --pattern increment >"${talker_log}" 2>&1 &
  fi
  local talker_pid=$!

  local rpc_pid=0
  if [[ "${load}" == "mixed" ]]; then
    if [[ -n "${SESSION_CONFIG_URI}" ]]; then
      env ZENOH_SESSION_CONFIG_URI="${SESSION_CONFIG_URI}" AORTA_EXECUTOR_MULTI_INGRESS="${ingress_flag}" AORTA_HEADER_INCLUDE_PUBLISHER_NODE="${HEADER_INCLUDE_PUBLISHER_NODE}" \
        "${RPC_CLIENT_BIN}" \
        --name "bench_rpc_client_${case_id}" \
        --service "${service}" \
        --schema-attach never \
        --interval-ms "${RPC_INTERVAL_MS}" >"${rpc_log}" 2>&1 &
    else
      env AORTA_EXECUTOR_MULTI_INGRESS="${ingress_flag}" AORTA_HEADER_INCLUDE_PUBLISHER_NODE="${HEADER_INCLUDE_PUBLISHER_NODE}" \
        "${RPC_CLIENT_BIN}" \
        --name "bench_rpc_client_${case_id}" \
        --service "${service}" \
        --schema-attach never \
        --interval-ms "${RPC_INTERVAL_MS}" >"${rpc_log}" 2>&1 &
    fi
    rpc_pid=$!
  fi

  local cleanup_done=0
  cleanup_case() {
    if [[ "${cleanup_done}" -eq 1 ]]; then
      return
    fi
    cleanup_done=1
    # Stop RPC client first while listener service is still alive.
    if [[ "${rpc_pid}" -gt 0 ]]; then
      stop_process "${rpc_pid}" 8
    fi
    stop_process "${talker_pid}" 3
    stop_process "${listener_pid}" 8
  }
  trap cleanup_case RETURN

  sleep "${WARMUP_SEC}"
  if ! kill -0 "${listener_pid}" 2>/dev/null; then
    echo "ERROR: listener exited during warmup"
    cat "${listener_log}"
    exit 1
  fi
  if ! kill -0 "${talker_pid}" 2>/dev/null; then
    echo "ERROR: talker exited during warmup"
    cat "${talker_log}"
    exit 1
  fi
  if [[ "${rpc_pid}" -gt 0 ]] && ! kill -0 "${rpc_pid}" 2>/dev/null; then
    echo "ERROR: rpc client exited during warmup"
    cat "${rpc_log}"
    exit 1
  fi

  local monitor_listener_pid
  local monitor_talker_pid
  local monitor_rpc_pid=0
  monitor_listener_pid="$(start_cpu_monitor "${listener_pid}" "${cpu_listener_log}")"
  monitor_talker_pid="$(start_cpu_monitor "${talker_pid}" "${cpu_talker_log}")"
  if [[ "${rpc_pid}" -gt 0 ]]; then
    monitor_rpc_pid="$(start_cpu_monitor "${rpc_pid}" "${cpu_rpc_log}")"
  fi

  sleep "${DURATION_SEC}"

  # During measurement window, producer/consumer should stay alive.
  if ! kill -0 "${listener_pid}" 2>/dev/null; then
    echo "ERROR: listener exited during measurement"
    cat "${listener_log}"
    exit 1
  fi
  if ! kill -0 "${talker_pid}" 2>/dev/null; then
    echo "ERROR: talker exited during measurement"
    cat "${talker_log}"
    exit 1
  fi
  if [[ "${rpc_pid}" -gt 0 ]] && ! kill -0 "${rpc_pid}" 2>/dev/null; then
    echo "ERROR: rpc client exited during measurement"
    cat "${rpc_log}"
    exit 1
  fi

  kill "${monitor_listener_pid}" 2>/dev/null || true
  kill "${monitor_talker_pid}" 2>/dev/null || true
  if [[ "${monitor_rpc_pid}" -gt 0 ]]; then
    kill "${monitor_rpc_pid}" 2>/dev/null || true
  fi
  wait "${monitor_listener_pid}" 2>/dev/null || true
  wait "${monitor_talker_pid}" 2>/dev/null || true
  if [[ "${monitor_rpc_pid}" -gt 0 ]]; then
    wait "${monitor_rpc_pid}" 2>/dev/null || true
  fi

  local perf_line
  local pub_lat_line
  local decode_lat_line
  local admission_line
  local mixed_line
  perf_line="$(last_nonzero_perf_line "${listener_log}" || true)"
  if [[ -z "${perf_line}" ]]; then
    perf_line="$(rg 'perf_stats count=' "${listener_log}" | tail -n1 || true)"
  fi
  pub_lat_line="$(rg 'latency_us publish_to_cb' "${listener_log}" | tail -n1 || true)"
  decode_lat_line="$(rg 'latency_us decode' "${listener_log}" | tail -n1 || true)"
  admission_line="$(rg 'admission_stats' "${listener_log}" | tail -n1 || true)"
  mixed_line="$(rg 'mixed_load service_calls_total' "${listener_log}" | tail -n1 || true)"

  cleanup_case
  trap - RETURN

  local rate
  local throughput
  local p50
  local p90
  local p95
  local p99
  local decode_p99
  local peak_events
  local peak_bytes
  local drop_full
  local drop_budget_events
  local service_calls
  local timer_ticks

  rate="$(extract_kv "${perf_line}" "rate")"
  throughput="$(extract_kv "${perf_line}" "throughput")"
  p50="$(extract_kv "${pub_lat_line}" "p50")"
  p90="$(extract_kv "${pub_lat_line}" "p90")"
  p95="$(extract_kv "${pub_lat_line}" "p95")"
  p99="$(extract_kv "${pub_lat_line}" "p99")"
  decode_p99="$(extract_kv "${decode_lat_line}" "p99")"
  peak_events="$(extract_kv "${admission_line}" "peak_events")"
  peak_bytes="$(extract_kv "${admission_line}" "peak_bytes")"
  drop_full="$(extract_kv "${admission_line}" "drop_full")"
  drop_budget_events="$(extract_kv "${admission_line}" "drop_budget_events")"
  service_calls="$(extract_kv "${mixed_line}" "service_calls_total")"
  timer_ticks="$(extract_kv "${mixed_line}" "timer_ticks_total")"

  rate="${rate:-NA}"
  throughput="${throughput:-NA}"
  p50="${p50:-NA}"
  p90="${p90:-NA}"
  p95="${p95:-NA}"
  p99="${p99:-NA}"
  decode_p99="${decode_p99:-NA}"
  peak_events="${peak_events:-NA}"
  peak_bytes="${peak_bytes:-NA}"
  drop_full="${drop_full:-NA}"
  drop_budget_events="${drop_budget_events:-NA}"
  service_calls="${service_calls:-0}"
  timer_ticks="${timer_ticks:-0}"

  local cpu_listener_avg
  local cpu_talker_avg
  local cpu_rpc_avg
  cpu_listener_avg="$(avg_file "${cpu_listener_log}")"
  cpu_talker_avg="$(avg_file "${cpu_talker_log}")"
  if [[ "${rpc_pid}" -gt 0 ]]; then
    cpu_rpc_avg="$(avg_file "${cpu_rpc_log}")"
  else
    cpu_rpc_avg="NA"
  fi

  echo "${case_id},${ingress},${scheduler},${load},${drive_mode},${SPIN_ONCE_TIMEOUT_MS},${SPIN_ONCE_PROFILE},${rate},${throughput},${p50},${p90},${p95},${p99},${decode_p99},${peak_events},${peak_bytes},${drop_full},${drop_budget_events},${service_calls},${timer_ticks},${cpu_listener_avg},${cpu_talker_avg},${cpu_rpc_avg}" \
    >>"${SUMMARY_CSV}"

  echo "RESULT case=${case_id} rate=${rate}msg/s throughput=${throughput}MB/s p50=${p50}us p90=${p90}us p99=${p99}us peak_events=${peak_events} cpu_listener=${cpu_listener_avg}% cpu_talker=${cpu_talker_avg}% cpu_rpc=${cpu_rpc_avg}%"
}

for ingress in ${INGRESS_MATRIX}; do
  for scheduler in ${SCHEDULER_MATRIX}; do
    for load in ${LOAD_MATRIX}; do
      for drive_mode in ${DRIVE_MODE_MATRIX}; do
        run_case "${ingress}" "${scheduler}" "${load}" "${drive_mode}"
      done
    done
  done
done

echo ""
echo "PASS: benchmark matrix completed"
echo "summary_csv=${SUMMARY_CSV}"
echo "log_root=${LOG_ROOT}"
