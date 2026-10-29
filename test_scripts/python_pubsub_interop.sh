#!/usr/bin/env bash
# Cross-language pub/sub interop gate for the Aorta Python SDK.
#
# Proves the Python SDK's wire format (byte-identical AortaHeader + demo::Chat
# layout) interoperates with the Rust SDK, both directions:
#   1. Python talker  -> Rust listener   (Rust decodes Python's Chat)
#   2. Rust talker    -> Python listener  (Python decodes Rust's Chat)
#
# Uses PREBUILT binaries only (never invokes bazel) — build first:
#   bazel build //core:aorta_core_cdylib //rust/examples:talker //rust/examples:listener
#
# Env overrides: PYTHON (interpreter with `flatbuffers` installed),
# AORTA_CORE_FFI_LIB (path to libaorta_core.{so,dylib}).
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$REPO_ROOT/python${PYTHONPATH:+:$PYTHONPATH}"

if [[ -z "${AORTA_CORE_FFI_LIB:-}" ]]; then
  for cand in "$REPO_ROOT/bazel-bin/core/libaorta_core.dylib" \
              "$REPO_ROOT/bazel-bin/core/libaorta_core.so"; do
    [[ -f "$cand" ]] && export AORTA_CORE_FFI_LIB="$cand" && break
  done
fi
PY="${PYTHON:-python3}"
RUST_TALKER="$REPO_ROOT/bazel-bin/rust/examples/talker"
RUST_LISTENER="$REPO_ROOT/bazel-bin/rust/examples/listener"

for f in "${AORTA_CORE_FFI_LIB:-/nonexistent}" "$RUST_TALKER" "$RUST_LISTENER"; do
  [[ -f "$f" ]] || { echo "MISSING prerequisite: $f (build it first)"; exit 2; }
done

TMP="$(mktemp -d)"
PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; rm -rf "$TMP"; }
trap cleanup EXIT
fail() { echo "INTEROP FAIL: $1"; exit 1; }

# Unique topics per run so overlapping runs / stragglers never cross-talk.
SUFFIX="$$_$RANDOM"
TOPIC_P2R="/demo/p2r_$SUFFIX"
TOPIC_R2P="/demo/r2p_$SUFFIX"

# wait_for <file> <pattern> <count> <timeout_s>
wait_for() {
  local file="$1" pat="$2" want="$3" deadline=$(( $(date +%s) + $4 ))
  while (( $(date +%s) < deadline )); do
    [[ "$(grep -c "$pat" "$file" 2>/dev/null || true)" -ge "$want" ]] && return 0
    sleep 0.1
  done
  return 1
}

echo "== 1. Python talker -> Rust listener (topic $TOPIC_P2R) =="
AORTA_LOG_STDERR=true "$RUST_LISTENER" --topic "$TOPIC_P2R" > "$TMP/rust.out" 2>&1 &
PIDS+=($!)
sleep 2.0
"$PY" "$REPO_ROOT/python/examples/talker.py" --topic "$TOPIC_P2R" --text py2rust --count 3 --interval 0.3 >/dev/null
wait_for "$TMP/rust.out" "node=py_talker text=py2rust" 3 8 \
  || { cat "$TMP/rust.out"; fail "Rust listener did not decode 3 Python messages"; }
for i in 0 1 2; do
  grep -q "seq=$i node=py_talker text=py2rust #$i" "$TMP/rust.out" \
    || fail "Rust listener missing/incorrect py2rust #$i header+text"
done
echo "   OK: Rust decoded Python's header(seq,node) + text for 3 messages"

echo "== 2. Rust talker -> Python listener (topic $TOPIC_R2P) =="
"$PY" "$REPO_ROOT/python/examples/listener.py" --topic "$TOPIC_R2P" --count 3 > "$TMP/py.out" 2>/dev/null &
PIDS+=($!)
sleep 1.5
# The Rust talker streams continuously (no self-exit), so background it and
# stop once the listener has its 3.
"$RUST_TALKER" --topic "$TOPIC_R2P" >/dev/null 2>&1 &
PIDS+=($!)
wait_for "$TMP/py.out" "seq=.* from=talker" 3 8 \
  || { cat "$TMP/py.out"; fail "Python listener did not decode 3 Rust messages"; }
for i in 0 1 2; do
  # Assert the full decode: text + sequence + publisher node (the Rust talker
  # publishes text="hello <i> from talker").
  grep -q "text=hello $i from talker seq=$i from=talker" "$TMP/py.out" \
    || fail "Python listener missing/incorrect Rust msg #$i (text+seq+node)"
done
echo "   OK: Python decoded Rust's header(seq,node) + text for 3 messages"

echo "INTEROP PASS: Python <-> Rust pub/sub byte-compatible (header + demo::Chat)"
