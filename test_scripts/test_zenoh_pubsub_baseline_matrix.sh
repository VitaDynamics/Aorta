#!/usr/bin/env bash
# Pure Zenoh pub/sub baseline benchmark matrix (developer-run, prebuilt binaries only).

set -euo pipefail
set +b
set +m

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

LISTENER_BIN="${REPO_ROOT}/bazel-bin/rust/examples/zenoh_perf_listener"
TALKER_BIN="${REPO_ROOT}/bazel-bin/rust/examples/zenoh_perf_talker"

DURATION_SEC="${AORTA_BENCH_DURATION_SEC:-20}"
WARMUP_SEC="${AORTA_BENCH_WARMUP_SEC:-2}"
REPORT_INTERVAL_MS="${AORTA_BENCH_REPORT_INTERVAL_MS:-1000}"
MAX_SAMPLES="${AORTA_BENCH_MAX_SAMPLES:-20000}"
TALKER_RATE_HZ="${AORTA_BENCH_TALKER_RATE_HZ:-2000}"
PAYLOAD_SIZE="${AORTA_BENCH_PAYLOAD_SIZE:-1024}"
PATTERN="${AORTA_BENCH_PATTERN:-increment}"
FORMAT_MATRIX="${AORTA_ZENOH_BASELINE_FORMAT_MATRIX:-raw flatbuffer}"

SESSION_CONFIG_URI="${AORTA_BENCH_ZENOH_SESSION_CONFIG_URI:-}"
LOG_ROOT="${AORTA_BENCH_LOG_ROOT:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-zenoh-baseline.XXXXXX")}"
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

for bin in "${LISTENER_BIN}" "${TALKER_BIN}"; do
  if [[ ! -x "${bin}" ]]; then
    cat <<EOF
ERROR: binary not found or not executable: ${bin}
Please build required targets first (developer-run):
  bazel build //rust/examples:zenoh_perf_listener
  bazel build //rust/examples:zenoh_perf_talker
EOF
    exit 2
  fi
done

cat >"${SUMMARY_CSV}" <<EOF
case,format,rate_msg_s,throughput_mb_s,p50_us,p90_us,p95_us,p99_us,invalid_payload,clock_skew,cpu_listener_pct,cpu_talker_pct
EOF

echo "=== Zenoh Pub/Sub Baseline Matrix ==="
echo "format=${FORMAT_MATRIX}"
echo "duration_sec=${DURATION_SEC} warmup_sec=${WARMUP_SEC}"
echo "talker_rate_hz=${TALKER_RATE_HZ} payload_size=${PAYLOAD_SIZE}B pattern=${PATTERN}"
echo "log_root=${LOG_ROOT}"
if [[ -n "${SESSION_CONFIG_URI}" ]]; then
  echo "zenoh_session_config_uri=${SESSION_CONFIG_URI}"
fi

run_case() {
  local format="$1"
  local case_id="zenoh_${format}"
  local case_dir="${LOG_ROOT}/${case_id}"
  mkdir -p "${case_dir}"

  local topic="bench/zenoh/${case_id}"
  local listener_log="${case_dir}/listener.log"
  local talker_log="${case_dir}/talker.log"
  local cpu_listener_log="${case_dir}/cpu_listener.log"
  local cpu_talker_log="${case_dir}/cpu_talker.log"

  echo ""
  echo "--- Case: ${case_id} ---"
  echo "topic=${topic}"

  if [[ -n "${SESSION_CONFIG_URI}" ]]; then
    env ZENOH_SESSION_CONFIG_URI="${SESSION_CONFIG_URI}" \
      "${LISTENER_BIN}" \
      --name "bench_listener_${case_id}" \
      --topic "${topic}" \
      --report-interval-ms "${REPORT_INTERVAL_MS}" \
      --max-samples "${MAX_SAMPLES}" \
      --format "${format}" >"${listener_log}" 2>&1 &
  else
    "${LISTENER_BIN}" \
      --name "bench_listener_${case_id}" \
      --topic "${topic}" \
      --report-interval-ms "${REPORT_INTERVAL_MS}" \
      --max-samples "${MAX_SAMPLES}" \
      --format "${format}" >"${listener_log}" 2>&1 &
  fi
  local listener_pid=$!

  if [[ -n "${SESSION_CONFIG_URI}" ]]; then
    env ZENOH_SESSION_CONFIG_URI="${SESSION_CONFIG_URI}" \
      "${TALKER_BIN}" \
      --name "bench_talker_${case_id}" \
      --topic "${topic}" \
      --size "${PAYLOAD_SIZE}" \
      --rate-hz "${TALKER_RATE_HZ}" \
      --pattern "${PATTERN}" \
      --format "${format}" >"${talker_log}" 2>&1 &
  else
    "${TALKER_BIN}" \
      --name "bench_talker_${case_id}" \
      --topic "${topic}" \
      --size "${PAYLOAD_SIZE}" \
      --rate-hz "${TALKER_RATE_HZ}" \
      --pattern "${PATTERN}" \
      --format "${format}" >"${talker_log}" 2>&1 &
  fi
  local talker_pid=$!

  local cleanup_done=0
  cleanup_case() {
    if [[ "${cleanup_done}" -eq 1 ]]; then
      return
    fi
    cleanup_done=1
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

  local monitor_listener_pid
  local monitor_talker_pid
  monitor_listener_pid="$(start_cpu_monitor "${listener_pid}" "${cpu_listener_log}")"
  monitor_talker_pid="$(start_cpu_monitor "${talker_pid}" "${cpu_talker_log}")"

  sleep "${DURATION_SEC}"

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

  kill "${monitor_listener_pid}" 2>/dev/null || true
  kill "${monitor_talker_pid}" 2>/dev/null || true
  wait "${monitor_listener_pid}" 2>/dev/null || true
  wait "${monitor_talker_pid}" 2>/dev/null || true

  local perf_line
  local pub_lat_line
  local err_line
  perf_line="$(last_nonzero_perf_line "${listener_log}" || true)"
  if [[ -z "${perf_line}" ]]; then
    perf_line="$(rg 'perf_stats count=' "${listener_log}" | tail -n1 || true)"
  fi
  pub_lat_line="$(rg 'latency_us publish_to_cb' "${listener_log}" | tail -n1 || true)"
  err_line="$(rg '^errors ' "${listener_log}" | tail -n1 || true)"

  cleanup_case
  trap - RETURN

  local rate
  local throughput
  local p50
  local p90
  local p95
  local p99
  local invalid_payload
  local clock_skew
  rate="$(extract_kv "${perf_line}" "rate")"
  throughput="$(extract_kv "${perf_line}" "throughput")"
  p50="$(extract_kv "${pub_lat_line}" "p50")"
  p90="$(extract_kv "${pub_lat_line}" "p90")"
  p95="$(extract_kv "${pub_lat_line}" "p95")"
  p99="$(extract_kv "${pub_lat_line}" "p99")"
  invalid_payload="$(extract_kv "${err_line}" "invalid_payload")"
  clock_skew="$(extract_kv "${err_line}" "clock_skew")"

  rate="${rate:-NA}"
  throughput="${throughput:-NA}"
  p50="${p50:-NA}"
  p90="${p90:-NA}"
  p95="${p95:-NA}"
  p99="${p99:-NA}"
  invalid_payload="${invalid_payload:-0}"
  clock_skew="${clock_skew:-0}"

  local cpu_listener_avg
  local cpu_talker_avg
  cpu_listener_avg="$(avg_file "${cpu_listener_log}")"
  cpu_talker_avg="$(avg_file "${cpu_talker_log}")"

  echo "${case_id},${format},${rate},${throughput},${p50},${p90},${p95},${p99},${invalid_payload},${clock_skew},${cpu_listener_avg},${cpu_talker_avg}" \
    >>"${SUMMARY_CSV}"

  echo "RESULT case=${case_id} rate=${rate}msg/s throughput=${throughput}MB/s p50=${p50}us p90=${p90}us p99=${p99}us cpu_listener=${cpu_listener_avg}% cpu_talker=${cpu_talker_avg}% invalid_payload=${invalid_payload}"
}

for format in ${FORMAT_MATRIX}; do
  run_case "${format}"
done

echo ""
echo "PASS: zenoh baseline matrix completed"
echo "summary_csv=${SUMMARY_CSV}"
echo "log_root=${LOG_ROOT}"
