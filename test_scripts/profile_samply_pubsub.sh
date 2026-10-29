#!/usr/bin/env bash
# Samply profiling helper for Aorta perf talker/listener (developer-run, prebuilt binaries only).

set -euo pipefail
set +b
set +m

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

LISTENER_BIN="${AORTA_SAMPLY_LISTENER_BIN:-${REPO_ROOT}/bazel-bin/rust/examples/perf_listener}"
TALKER_BIN="${AORTA_SAMPLY_TALKER_BIN:-${REPO_ROOT}/bazel-bin/rust/examples/perf_talker}"

TARGET="${AORTA_SAMPLY_TARGET:-listener}"            # listener|talker|both
DURATION_SEC="${AORTA_SAMPLY_DURATION_SEC:-20}"
SAMPLING_RATE_HZ="${AORTA_SAMPLY_RATE_HZ:-1000}"
WARMUP_SEC="${AORTA_SAMPLY_WARMUP_SEC:-10}"
AUTO_STOP_MARGIN_SEC="${AORTA_SAMPLY_AUTO_STOP_MARGIN_SEC:-3}"
LOG_ROOT="${AORTA_SAMPLY_LOG_ROOT:-$(mktemp -d "${TMPDIR:-/tmp}/aorta-samply.XXXXXX")}"

TOPIC="${AORTA_SAMPLY_TOPIC:-/bench/samply/profile}"
GROUP="${AORTA_SAMPLY_GROUP:-default}"
SESSION_CONFIG_URI="${AORTA_SAMPLY_ZENOH_SESSION_CONFIG_URI:-}"
MULTI_INGRESS_MODE="${AORTA_SAMPLY_MULTI_INGRESS:-off}" # off|on|0|1|false|true
HEADER_INCLUDE_PUBLISHER_NODE="${AORTA_SAMPLY_HEADER_INCLUDE_PUBLISHER_NODE:-1}"

SAMPLY_SAVE_ONLY="${AORTA_SAMPLY_SAVE_ONLY:-1}"
SAMPLY_NO_OPEN="${AORTA_SAMPLY_NO_OPEN:-1}"
SAMPLY_MAIN_THREAD_ONLY="${AORTA_SAMPLY_MAIN_THREAD_ONLY:-0}"
SAMPLY_PRESYMBOLICATE="${AORTA_SAMPLY_PRESYMBOLICATE:-1}"
SAMPLY_SYMBOL_DIR="${AORTA_SAMPLY_SYMBOL_DIR:-${REPO_ROOT}/bazel-bin/rust/examples}"
SUMMARY_ENABLE="${AORTA_SAMPLY_SUMMARY_ENABLE:-1}"
TRIM_HEAD_MS="${AORTA_SAMPLY_TRIM_HEAD_MS:-3000}"
TRIM_TAIL_MS="${AORTA_SAMPLY_TRIM_TAIL_MS:-3000}"
SUMMARY_TOPN="${AORTA_SAMPLY_SUMMARY_TOPN:-12}"

TALKER_SIZE="${AORTA_SAMPLY_TALKER_SIZE:-1024}"
TALKER_RATE_HZ="${AORTA_SAMPLY_TALKER_RATE_HZ:-1000}"
TALKER_PATTERN="${AORTA_SAMPLY_TALKER_PATTERN:-increment}"
TALKER_PUBLISH_API="${AORTA_SAMPLY_TALKER_PUBLISH_API:-builder}"

LISTENER_REPORT_INTERVAL_MS="${AORTA_SAMPLY_LISTENER_REPORT_INTERVAL_MS:-60000}"
LISTENER_MAX_SAMPLES="${AORTA_SAMPLY_LISTENER_MAX_SAMPLES:-}"
LISTENER_MAX_SAMPLES_EXPLICIT=0
if [[ -n "${AORTA_SAMPLY_LISTENER_MAX_SAMPLES+x}" ]]; then
  LISTENER_MAX_SAMPLES_EXPLICIT=1
fi
LISTENER_EXECUTOR_QUEUE="${AORTA_SAMPLY_LISTENER_EXECUTOR_QUEUE:-4096}"
LISTENER_SCHEDULER="${AORTA_SAMPLY_LISTENER_SCHEDULER:-fifo}"
LISTENER_DRIVE_MODE="${AORTA_SAMPLY_LISTENER_DRIVE_MODE:-spin_once}"
LISTENER_SPIN_ONCE_TIMEOUT_MS="${AORTA_SAMPLY_LISTENER_SPIN_ONCE_TIMEOUT_MS:-10}"
LISTENER_SPIN_ONCE_PROFILE="${AORTA_SAMPLY_LISTENER_SPIN_ONCE_PROFILE:-adaptive}"
TALKER_COUNT="${AORTA_SAMPLY_TALKER_COUNT:-}"
TALKER_COUNT_EXPLICIT=0
if [[ -n "${AORTA_SAMPLY_TALKER_COUNT+x}" ]]; then
  TALKER_COUNT_EXPLICIT=1
fi

usage() {
  cat <<'EOF'
Usage:
  bash test_scripts/profile_samply_pubsub.sh [options]

Options:
  --target listener|talker|both
  --duration-sec N
  --sampling-rate-hz N
  --warmup-sec N
  --topic TOPIC
  --group GROUP
  --log-root DIR
  --session-config-uri PATH
  --multi-ingress off|on
  --talker-size BYTES
  --talker-rate-hz N
  --talker-pattern zero|increment|random
  --talker-publish-api closure|builder|bytes|all
  --talker-count N|auto
  --listener-max-samples N|auto
  --listener-executor-queue N
  --listener-scheduler fifo|priority
  --listener-drive-mode spin|spin_once
  --listener-spin-once-timeout-ms N
  --listener-spin-once-profile fixed|adaptive
  --trim-head-ms N
  --trim-tail-ms N
  --summary-topn N
  --no-summary
  -h, --help

Environment variable equivalents:
  AORTA_SAMPLY_TARGET
  AORTA_SAMPLY_DURATION_SEC
  AORTA_SAMPLY_RATE_HZ
  AORTA_SAMPLY_WARMUP_SEC
  AORTA_SAMPLY_TOPIC
  AORTA_SAMPLY_GROUP
  AORTA_SAMPLY_LOG_ROOT
  AORTA_SAMPLY_ZENOH_SESSION_CONFIG_URI
  AORTA_SAMPLY_MULTI_INGRESS
  AORTA_SAMPLY_HEADER_INCLUDE_PUBLISHER_NODE
  AORTA_SAMPLY_TALKER_SIZE
  AORTA_SAMPLY_TALKER_RATE_HZ
  AORTA_SAMPLY_TALKER_PATTERN
  AORTA_SAMPLY_TALKER_PUBLISH_API
  AORTA_SAMPLY_TALKER_COUNT
  AORTA_SAMPLY_LISTENER_MAX_SAMPLES
  AORTA_SAMPLY_AUTO_STOP_MARGIN_SEC
  AORTA_SAMPLY_LISTENER_EXECUTOR_QUEUE
  AORTA_SAMPLY_LISTENER_SCHEDULER
  AORTA_SAMPLY_LISTENER_DRIVE_MODE
  AORTA_SAMPLY_LISTENER_SPIN_ONCE_TIMEOUT_MS
  AORTA_SAMPLY_LISTENER_SPIN_ONCE_PROFILE
  AORTA_SAMPLY_PRESYMBOLICATE
  AORTA_SAMPLY_SYMBOL_DIR
  AORTA_SAMPLY_SUMMARY_ENABLE
  AORTA_SAMPLY_TRIM_HEAD_MS
  AORTA_SAMPLY_TRIM_TAIL_MS
  AORTA_SAMPLY_SUMMARY_TOPN
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    --duration-sec) DURATION_SEC="$2"; shift 2 ;;
    --sampling-rate-hz) SAMPLING_RATE_HZ="$2"; shift 2 ;;
    --warmup-sec) WARMUP_SEC="$2"; shift 2 ;;
    --topic) TOPIC="$2"; shift 2 ;;
    --group) GROUP="$2"; shift 2 ;;
    --log-root) LOG_ROOT="$2"; shift 2 ;;
    --session-config-uri) SESSION_CONFIG_URI="$2"; shift 2 ;;
    --multi-ingress) MULTI_INGRESS_MODE="$2"; shift 2 ;;
    --talker-size) TALKER_SIZE="$2"; shift 2 ;;
    --talker-rate-hz) TALKER_RATE_HZ="$2"; shift 2 ;;
    --talker-pattern) TALKER_PATTERN="$2"; shift 2 ;;
    --talker-publish-api) TALKER_PUBLISH_API="$2"; shift 2 ;;
    --talker-count) TALKER_COUNT="$2"; TALKER_COUNT_EXPLICIT=1; shift 2 ;;
    --listener-max-samples) LISTENER_MAX_SAMPLES="$2"; LISTENER_MAX_SAMPLES_EXPLICIT=1; shift 2 ;;
    --listener-executor-queue) LISTENER_EXECUTOR_QUEUE="$2"; shift 2 ;;
    --listener-scheduler) LISTENER_SCHEDULER="$2"; shift 2 ;;
    --listener-drive-mode) LISTENER_DRIVE_MODE="$2"; shift 2 ;;
    --listener-spin-once-timeout-ms) LISTENER_SPIN_ONCE_TIMEOUT_MS="$2"; shift 2 ;;
    --listener-spin-once-profile) LISTENER_SPIN_ONCE_PROFILE="$2"; shift 2 ;;
    --trim-head-ms) TRIM_HEAD_MS="$2"; shift 2 ;;
    --trim-tail-ms) TRIM_TAIL_MS="$2"; shift 2 ;;
    --summary-topn) SUMMARY_TOPN="$2"; shift 2 ;;
    --no-summary) SUMMARY_ENABLE=0; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "ERROR: unknown option: $1"
      usage
      exit 2
      ;;
  esac
done

case "${TARGET}" in
  listener|talker|both) ;;
  *)
    echo "ERROR: invalid target '${TARGET}', expected listener|talker|both"
    exit 2
    ;;
esac

case "${MULTI_INGRESS_MODE}" in
  on|1|true|TRUE|True) MULTI_INGRESS_FLAG="1" ;;
  off|0|false|FALSE|False) MULTI_INGRESS_FLAG="0" ;;
  *)
    echo "ERROR: invalid multi ingress mode '${MULTI_INGRESS_MODE}', expected off|on"
    exit 2
    ;;
esac

if [[ "${LISTENER_DRIVE_MODE}" != "spin" && "${LISTENER_DRIVE_MODE}" != "spin_once" ]]; then
  echo "ERROR: invalid listener drive mode '${LISTENER_DRIVE_MODE}', expected spin|spin_once"
  exit 2
fi

if [[ "${LISTENER_SCHEDULER}" != "fifo" && "${LISTENER_SCHEDULER}" != "priority" ]]; then
  echo "ERROR: invalid listener scheduler '${LISTENER_SCHEDULER}', expected fifo|priority"
  exit 2
fi

if ! [[ "${DURATION_SEC}" =~ ^[0-9]+$ ]] || ! [[ "${WARMUP_SEC}" =~ ^[0-9]+$ ]] || ! [[ "${TALKER_RATE_HZ}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: duration/warmup/talker-rate must be integers"
  exit 2
fi
if ! [[ "${AUTO_STOP_MARGIN_SEC}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: auto stop margin must be an integer"
  exit 2
fi
if ! [[ "${TRIM_HEAD_MS}" =~ ^[0-9]+$ ]] || ! [[ "${TRIM_TAIL_MS}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: trim head/tail must be non-negative integers (ms)"
  exit 2
fi
if ! [[ "${SUMMARY_TOPN}" =~ ^[0-9]+$ ]] || [[ "${SUMMARY_TOPN}" -le 0 ]]; then
  echo "ERROR: summary topn must be a positive integer"
  exit 2
fi

auto_sample_budget=$(( TALKER_RATE_HZ * (DURATION_SEC + WARMUP_SEC + AUTO_STOP_MARGIN_SEC) ))
if (( auto_sample_budget < 100 )); then
  auto_sample_budget=100
fi

if [[ "${LISTENER_MAX_SAMPLES_EXPLICIT}" -eq 0 || "${LISTENER_MAX_SAMPLES}" == "auto" || -z "${LISTENER_MAX_SAMPLES}" ]]; then
  LISTENER_MAX_SAMPLES="${auto_sample_budget}"
fi
if ! [[ "${LISTENER_MAX_SAMPLES}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: listener max samples must be an integer or 'auto'"
  exit 2
fi

if [[ "${TALKER_COUNT_EXPLICIT}" -eq 0 || "${TALKER_COUNT}" == "auto" || -z "${TALKER_COUNT}" ]]; then
  TALKER_COUNT="${auto_sample_budget}"
fi
if ! [[ "${TALKER_COUNT}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: talker count must be an integer or 'auto'"
  exit 2
fi

if ! command -v samply >/dev/null 2>&1; then
  cat <<'EOF'
ERROR: 'samply' not found in PATH.
Install first (developer-run):
  cargo install samply
EOF
  exit 2
fi

require_bin() {
  local bin="$1"
  local target_name="$2"
  if [[ ! -x "${bin}" ]]; then
    cat <<EOF
ERROR: binary not found or not executable: ${bin}
Please build required target first (developer-run):
  bazel build ${target_name}
EOF
    exit 2
  fi
}

require_bin "${LISTENER_BIN}" "//rust/examples:perf_listener"
require_bin "${TALKER_BIN}" "//rust/examples:perf_talker"

mkdir -p "${LOG_ROOT}"
STAMP="$(date +%Y%m%d_%H%M%S)"
LISTENER_NAME="samply_listener_${STAMP}"
TALKER_NAME="samply_talker_${STAMP}"

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

start_name_guard() {
  local match_name="$1"
  local delay_sec="$2"
  (
    sleep "${delay_sec}"
    if ! command -v pgrep >/dev/null 2>&1; then
      exit 0
    fi
    local pids
    pids="$(pgrep -f "${match_name}" || true)"
    if [[ -z "${pids}" ]]; then
      exit 0
    fi
    while IFS= read -r pid; do
      if [[ -n "${pid}" ]]; then
        kill -INT "${pid}" 2>/dev/null || true
      fi
    done <<<"${pids}"
  ) >/dev/null 2>&1 &
  echo $!
}

stop_guard() {
  local pid="$1"
  if [[ -z "${pid}" || "${pid}" -le 0 ]]; then
    return 0
  fi
  kill "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
}

listener_args=(
  --name "${LISTENER_NAME}"
  --group "${GROUP}"
  --topic "${TOPIC}"
  --report-interval-ms "${LISTENER_REPORT_INTERVAL_MS}"
  --max-samples "${LISTENER_MAX_SAMPLES}"
  --executor-queue "${LISTENER_EXECUTOR_QUEUE}"
  --scheduler "${LISTENER_SCHEDULER}"
  --drive-mode "${LISTENER_DRIVE_MODE}"
  --spin-once-timeout-ms "${LISTENER_SPIN_ONCE_TIMEOUT_MS}"
  --spin-once-profile "${LISTENER_SPIN_ONCE_PROFILE}"
)

talker_args=(
  --name "${TALKER_NAME}"
  --group "${GROUP}"
  --topic "${TOPIC}"
  --size "${TALKER_SIZE}"
  --rate-hz "${TALKER_RATE_HZ}"
  --pattern "${TALKER_PATTERN}"
  --publish-api "${TALKER_PUBLISH_API}"
  --count "${TALKER_COUNT}"
)

samply_common_args=(
  record
  -r "${SAMPLING_RATE_HZ}"
  -d "${DURATION_SEC}"
)
if [[ "${SAMPLY_SAVE_ONLY}" == "1" ]]; then
  samply_common_args+=(-s)
fi
if [[ "${SAMPLY_NO_OPEN}" == "1" ]]; then
  samply_common_args+=(-n)
fi
if [[ "${SAMPLY_MAIN_THREAD_ONLY}" == "1" ]]; then
  samply_common_args+=(--main-thread-only)
fi
if [[ "${SAMPLY_PRESYMBOLICATE}" == "1" ]]; then
  samply_common_args+=(--unstable-presymbolicate)
fi
if [[ -d "${SAMPLY_SYMBOL_DIR}" ]]; then
  samply_common_args+=(--symbol-dir "${SAMPLY_SYMBOL_DIR}")
fi

env_prefix=(
  "AORTA_EXECUTOR_MULTI_INGRESS=${MULTI_INGRESS_FLAG}"
  "AORTA_HEADER_INCLUDE_PUBLISHER_NODE=${HEADER_INCLUDE_PUBLISHER_NODE}"
)
if [[ -n "${SESSION_CONFIG_URI}" ]]; then
  env_prefix+=("ZENOH_SESSION_CONFIG_URI=${SESSION_CONFIG_URI}")
fi

start_listener_bg() {
  local log_file="$1"
  env "${env_prefix[@]}" "${LISTENER_BIN}" "${listener_args[@]}" >"${log_file}" 2>&1 &
  echo $!
}

start_talker_bg() {
  local log_file="$1"
  env "${env_prefix[@]}" "${TALKER_BIN}" "${talker_args[@]}" >"${log_file}" 2>&1 &
  echo $!
}

verify_profile_generated() {
  local profile_file="$1"
  local samply_log="$2"
  if [[ -s "${profile_file}" ]]; then
    return 0
  fi
  echo "ERROR: samply did not produce profile: ${profile_file}"
  echo "samply_log=${samply_log}"
  if [[ -f "${samply_log}" ]]; then
    cat "${samply_log}"
  fi
  return 1
}

emit_profile_summary() {
  local profile_file="$1"
  local syms_file="$2"
  local app_module="$3"
  local label="$4"
  local summary_file="${LOG_ROOT}/${label}_summary_${STAMP}.log"

  if [[ "${SUMMARY_ENABLE}" != "1" ]]; then
    return 0
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    echo "WARN: python3 not found, skip summary for ${label}"
    return 0
  fi

  python3 - "${profile_file}" "${syms_file}" "${app_module}" "${TRIM_HEAD_MS}" "${TRIM_TAIL_MS}" "${SUMMARY_TOPN}" >"${summary_file}" <<'PY'
import bisect
import collections
import gzip
import json
import os
import sys

profile_file, syms_file, app_module, trim_head_ms, trim_tail_ms, topn = sys.argv[1:]
trim_head_ms = float(trim_head_ms)
trim_tail_ms = float(trim_tail_ms)
topn = int(topn)
kernel_module = "libsystem_kernel.dylib"

with gzip.open(profile_file, "rt") as f:
    profile = json.load(f)


def get_string(arr, idx):
    if isinstance(idx, int) and 0 <= idx < len(arr):
        return arr[idx]
    return None


records = []
for thread in profile.get("threads", []):
    samples = thread.get("samples", {})
    stacks = samples.get("stack", [])
    times = samples.get("time", [])
    cpu_deltas = samples.get("threadCPUDelta", [])
    if not stacks or not times:
        continue
    if not cpu_deltas or len(cpu_deltas) != len(stacks):
        cpu_deltas = [0] * len(stacks)
    stack_table = thread.get("stackTable", {})
    frame_table = thread.get("frameTable", {})
    func_table = thread.get("funcTable", {})
    resource_table = thread.get("resourceTable", {})
    strings = thread.get("stringArray", [])

    thread_name = thread.get("name", "unknown")
    for stack_idx, tm, cpu_delta in zip(stacks, times, cpu_deltas):
        if stack_idx is None:
            continue
        frame_idx = stack_table["frame"][stack_idx]
        func_idx = frame_table["func"][frame_idx]
        func_name = get_string(strings, func_table["name"][func_idx]) or "unknown"
        res_idx = func_table["resource"][func_idx]
        module = "unknown"
        if isinstance(res_idx, int) and 0 <= res_idx < resource_table["length"]:
            module_name_idx = resource_table["name"][res_idx]
            module = get_string(strings, module_name_idx) or "unknown"
        addr = frame_table["address"][frame_idx]
        records.append((tm, thread_name, module, func_name, addr, cpu_delta))

if not records:
    print("SUMMARY: no samples")
    sys.exit(0)

times = [r[0] for r in records]
t_min = min(times)
t_max = max(times)
window_start = t_min + trim_head_ms
window_end = t_max - trim_tail_ms
if window_end <= window_start:
    window_start = t_min
    window_end = t_max

steady = [r for r in records if window_start <= r[0] <= window_end]
if not steady:
    steady = records
    window_start = t_min
    window_end = t_max

steady_cpu = [r for r in steady if r[5] > 0]
steady_no_kernel = [r for r in steady if r[2] != kernel_module]
steady_cpu_no_kernel = [r for r in steady_cpu if r[2] != kernel_module]

module_counter = collections.Counter(r[2] for r in steady)
thread_counter = collections.Counter(r[1] for r in steady)
func_counter = collections.Counter(r[3] for r in steady)
module_cpu_counter = collections.Counter(r[2] for r in steady_cpu)
func_cpu_counter = collections.Counter(r[3] for r in steady_cpu)
module_no_kernel_counter = collections.Counter(r[2] for r in steady_no_kernel)
func_no_kernel_counter = collections.Counter(r[3] for r in steady_no_kernel)
module_cpu_no_kernel_counter = collections.Counter(r[2] for r in steady_cpu_no_kernel)
func_cpu_no_kernel_counter = collections.Counter(r[3] for r in steady_cpu_no_kernel)

symbol_starts = []
symbol_sizes = []
symbol_names = []
if os.path.isfile(syms_file):
    with open(syms_file, "r") as f:
        syms = json.load(f)
    string_table = syms.get("string_table", [])
    for entry in syms.get("data", []):
        if entry.get("debug_name") != app_module:
            continue
        rows = sorted(entry.get("symbol_table", []), key=lambda x: x.get("rva", 0))
        symbol_starts = [row.get("rva", 0) for row in rows]
        symbol_sizes = [row.get("size", 0) for row in rows]
        symbol_names = [string_table[row.get("symbol", 0)] for row in rows]
        break


def resolve_symbol(addr):
    if not symbol_starts:
        return f"0x{addr:x}"
    pos = bisect.bisect_right(symbol_starts, addr) - 1
    if pos < 0:
        return f"0x{addr:x}"
    start = symbol_starts[pos]
    size = symbol_sizes[pos]
    if size and addr >= start + size:
        return f"0x{addr:x}"
    return f"{symbol_names[pos]}+0x{addr-start:x}"


app_symbol_counter = collections.Counter(
    resolve_symbol(addr) for _, _, module, _, addr, _ in steady if module == app_module
)
app_symbol_cpu_counter = collections.Counter(
    resolve_symbol(addr) for _, _, module, _, addr, cpu_delta in steady if module == app_module and cpu_delta > 0
)

print(
    f"SUMMARY: profile={os.path.basename(profile_file)} "
    f"total_samples={len(records)} steady_samples={len(steady)} "
    f"steady_cpu_samples={len(steady_cpu)} "
    f"steady_no_kernel_samples={len(steady_no_kernel)} "
    f"steady_cpu_no_kernel_samples={len(steady_cpu_no_kernel)} "
    f"steady_range_ms=[{window_start:.1f},{window_end:.1f}] "
    f"trim_head_ms={trim_head_ms:.0f} trim_tail_ms={trim_tail_ms:.0f}"
)
print("SUMMARY: top_threads")
for name, count in thread_counter.most_common(topn):
    print(f"  {count:8d}  {name}")
print("SUMMARY: top_modules")
for name, count in module_counter.most_common(topn):
    print(f"  {count:8d}  {name}")
print("SUMMARY: top_leaf_functions")
for name, count in func_counter.most_common(topn):
    print(f"  {count:8d}  {name}")
print("SUMMARY: top_modules_cpu_only")
for name, count in module_cpu_counter.most_common(topn):
    print(f"  {count:8d}  {name}")
print("SUMMARY: top_leaf_functions_cpu_only")
for name, count in func_cpu_counter.most_common(topn):
    print(f"  {count:8d}  {name}")
print(f"SUMMARY: top_modules_no_{kernel_module}")
for name, count in module_no_kernel_counter.most_common(topn):
    print(f"  {count:8d}  {name}")
print(f"SUMMARY: top_leaf_functions_no_{kernel_module}")
for name, count in func_no_kernel_counter.most_common(topn):
    print(f"  {count:8d}  {name}")
print(f"SUMMARY: top_modules_cpu_only_no_{kernel_module}")
for name, count in module_cpu_no_kernel_counter.most_common(topn):
    print(f"  {count:8d}  {name}")
print(f"SUMMARY: top_leaf_functions_cpu_only_no_{kernel_module}")
for name, count in func_cpu_no_kernel_counter.most_common(topn):
    print(f"  {count:8d}  {name}")
if app_symbol_counter:
    print(f"SUMMARY: top_{app_module}_symbols")
    for name, count in app_symbol_counter.most_common(topn):
        print(f"  {count:8d}  {name}")
if app_symbol_cpu_counter:
    print(f"SUMMARY: top_{app_module}_symbols_cpu_only")
    for name, count in app_symbol_cpu_counter.most_common(topn):
        print(f"  {count:8d}  {name}")
PY

  echo "summary_log=${summary_file}"
  sed -n '1,220p' "${summary_file}"
}

run_profile_listener() {
  local listener_profile="${LOG_ROOT}/listener_${STAMP}.json.gz"
  local listener_syms="${LOG_ROOT}/listener_${STAMP}.json.syms.json"
  local talker_log="${LOG_ROOT}/talker_bg_for_listener_${STAMP}.log"
  local samply_log="${LOG_ROOT}/samply_listener_${STAMP}.log"
  local guard_seconds=$((DURATION_SEC + AUTO_STOP_MARGIN_SEC))

  echo ""
  echo "--- Profile Target: listener ---"
  echo "topic=${TOPIC}"

  local talker_pid
  talker_pid="$(start_talker_bg "${talker_log}")"

  local cleanup_done=0
  local guard_pid=""
  cleanup_listener_case() {
    if [[ "${cleanup_done}" -eq 1 ]]; then
      return
    fi
    cleanup_done=1
    stop_guard "${guard_pid:-0}"
    stop_process "${talker_pid}" 3
  }
  trap cleanup_listener_case RETURN

  sleep "${WARMUP_SEC}"
  if ! kill -0 "${talker_pid}" 2>/dev/null; then
    echo "ERROR: talker exited during warmup"
    cat "${talker_log}"
    exit 1
  fi

  guard_pid="$(start_name_guard "${LISTENER_NAME}" "${guard_seconds}")"
  env "${env_prefix[@]}" samply "${samply_common_args[@]}" -o "${listener_profile}" -- \
    "${LISTENER_BIN}" "${listener_args[@]}" >"${samply_log}" 2>&1
  verify_profile_generated "${listener_profile}" "${samply_log}"
  emit_profile_summary "${listener_profile}" "${listener_syms}" "perf_listener" "listener"

  cleanup_listener_case
  trap - RETURN
  echo "listener_profile=${listener_profile}"
  if [[ -s "${listener_syms}" ]]; then
    echo "listener_syms=${listener_syms}"
  fi
  echo "samply_log=${samply_log}"
  echo "talker_log=${talker_log}"
  if [[ "${SAMPLY_PRESYMBOLICATE}" == "1" ]]; then
    echo "NOTE: keep .json.gz and .json.syms.json together for symbolized view."
  fi
}

run_profile_talker() {
  local talker_profile="${LOG_ROOT}/talker_${STAMP}.json.gz"
  local talker_syms="${LOG_ROOT}/talker_${STAMP}.json.syms.json"
  local listener_log="${LOG_ROOT}/listener_bg_for_talker_${STAMP}.log"
  local samply_log="${LOG_ROOT}/samply_talker_${STAMP}.log"

  echo ""
  echo "--- Profile Target: talker ---"
  echo "topic=${TOPIC}"

  local listener_pid
  listener_pid="$(start_listener_bg "${listener_log}")"

  local cleanup_done=0
  cleanup_talker_case() {
    if [[ "${cleanup_done}" -eq 1 ]]; then
      return
    fi
    cleanup_done=1
    stop_process "${listener_pid}" 6
  }
  trap cleanup_talker_case RETURN

  sleep "${WARMUP_SEC}"
  if ! kill -0 "${listener_pid}" 2>/dev/null; then
    echo "ERROR: listener exited during warmup"
    cat "${listener_log}"
    exit 1
  fi

  env "${env_prefix[@]}" samply "${samply_common_args[@]}" -o "${talker_profile}" -- \
    "${TALKER_BIN}" "${talker_args[@]}" >"${samply_log}" 2>&1
  verify_profile_generated "${talker_profile}" "${samply_log}"
  emit_profile_summary "${talker_profile}" "${talker_syms}" "perf_talker" "talker"

  cleanup_talker_case
  trap - RETURN
  echo "talker_profile=${talker_profile}"
  if [[ -s "${talker_syms}" ]]; then
    echo "talker_syms=${talker_syms}"
  fi
  echo "samply_log=${samply_log}"
  echo "listener_log=${listener_log}"
  if [[ "${SAMPLY_PRESYMBOLICATE}" == "1" ]]; then
    echo "NOTE: keep .json.gz and .json.syms.json together for symbolized view."
  fi
}

echo "=== Aorta Samply Profile Helper ==="
echo "target=${TARGET}"
echo "duration_sec=${DURATION_SEC} sampling_rate_hz=${SAMPLING_RATE_HZ}"
echo "warmup_sec=${WARMUP_SEC}"
echo "auto_stop_margin_sec=${AUTO_STOP_MARGIN_SEC}"
echo "listener_guard_stop_after_sec=$((DURATION_SEC + AUTO_STOP_MARGIN_SEC))"
echo "summary_enable=${SUMMARY_ENABLE} trim_head_ms=${TRIM_HEAD_MS} trim_tail_ms=${TRIM_TAIL_MS} summary_topn=${SUMMARY_TOPN}"
echo "topic=${TOPIC} group=${GROUP}"
echo "listener: scheduler=${LISTENER_SCHEDULER} drive_mode=${LISTENER_DRIVE_MODE} executor_queue=${LISTENER_EXECUTOR_QUEUE} max_samples=${LISTENER_MAX_SAMPLES}"
echo "talker: size=${TALKER_SIZE} rate_hz=${TALKER_RATE_HZ} pattern=${TALKER_PATTERN} publish_api=${TALKER_PUBLISH_API} count=${TALKER_COUNT}"
echo "multi_ingress=${MULTI_INGRESS_FLAG}"
echo "header_include_publisher_node=${HEADER_INCLUDE_PUBLISHER_NODE}"
echo "samply_presymbolicate=${SAMPLY_PRESYMBOLICATE}"
if [[ -d "${SAMPLY_SYMBOL_DIR}" ]]; then
  echo "samply_symbol_dir=${SAMPLY_SYMBOL_DIR}"
fi
if [[ -n "${SESSION_CONFIG_URI}" ]]; then
  echo "zenoh_session_config_uri=${SESSION_CONFIG_URI}"
fi
echo "log_root=${LOG_ROOT}"

case "${TARGET}" in
  listener)
    run_profile_listener
    ;;
  talker)
    run_profile_talker
    ;;
  both)
    run_profile_listener
    run_profile_talker
    ;;
esac

echo ""
echo "PASS: samply profiling completed"
echo "log_root=${LOG_ROOT}"
