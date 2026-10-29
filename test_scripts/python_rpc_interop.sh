#!/usr/bin/env bash
# Cross-PROCESS RPC interop gate for the Aorta Python SDK.
#
# Runs a Python echo SERVICE and a Python CLIENT in separate processes so the
# RPC query/reply travels over a real Zenoh session boundary (not intra-session
# like the in-process pytest suite). The header/message wire format is already
# proven byte-identical cross-language by the pub/sub interop gate; this proves
# the request/reply *mechanism* works across process boundaries.
#
# Uses the PREBUILT cdylib only (never invokes bazel) — build first:
#   bazel build //core:aorta_core_cdylib
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

[[ -f "${AORTA_CORE_FFI_LIB:-/nonexistent}" ]] || {
  echo "MISSING prerequisite: AORTA_CORE_FFI_LIB (build //core:aorta_core_cdylib)"; exit 2; }

TMP="$(mktemp -d)"
PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; rm -rf "$TMP"; }
trap cleanup EXIT
fail() { echo "RPC INTEROP FAIL: $1"; exit 1; }

# Unique service per run so overlapping runs / stragglers never cross-talk.
SERVICE="/demo/py_rpc_$$_$RANDOM"
COUNT=3

echo "== Python echo service <- Python client (service $SERVICE) =="
"$PY" "$REPO_ROOT/python/examples/echo_server.py" \
  --service "$SERVICE" --name py_echo_srv --count "$COUNT" > "$TMP/srv.out" 2>&1 &
PIDS+=($!)
"$PY" "$REPO_ROOT/python/examples/echo_client.py" \
  --service "$SERVICE" --text hi --count "$COUNT" --timeout-ms 6000 --settle 1.5 \
  > "$TMP/cli.out" 2>"$TMP/cli.err"
rc=$?
[[ $rc -eq 0 ]] || { cat "$TMP/cli.err" "$TMP/cli.out"; fail "client exited $rc"; }

for i in 0 1 2; do
  grep -q "response #$i: echo from py_echo_srv: hi #$i" "$TMP/cli.out" \
    || { cat "$TMP/cli.out"; fail "missing/incorrect response #$i"; }
done
echo "   OK: client received $COUNT correct echo replies across the process boundary"

echo "RPC INTEROP PASS: Python <-> Python cross-process RPC (request/reply)"
