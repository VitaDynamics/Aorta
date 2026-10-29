#!/usr/bin/env bash
# Typed-example gate for the Aorta Python SDK: proves that GENERATED FlatBuffers
# message definitions import and drive the SDK across pub/sub, service, and
# action — including the aorta.* namespace coexistence (the SDK `aorta` package
# must resolve before the generated `aorta.sys.*` submodules).
#
# Uses PREBUILT artifacts only (never invokes bazel) — build first:
#   bazel build //core:aorta_core_cdylib \
#               //message/schemas/topic/example:demo_chat_py \
#               //message/schemas/topic/example:demo_chat_schema_meta_py
#
# In a real app you instead `deps = [..._py_lib, ..._schema_meta_py_lib]` and let
# Bazel place the generated modules on the path; this script reproduces that path
# layout by hand so the examples run against the dev tree.
#
# Env overrides: PYTHON (interpreter with `flatbuffers`), AORTA_CORE_FFI_LIB.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EX_BIN="$REPO_ROOT/bazel-bin/message/schemas/topic/example"

# SDK first (its __init__ runs pkgutil.extend_path), then the generated trees:
#   <demo_chat_py>        -> demo.Chat + aorta.sys.AortaHeader
#   <example bin dir>     -> demo_chat_schema_meta
export PYTHONPATH="$REPO_ROOT/python:$EX_BIN/demo_chat_py:$EX_BIN${PYTHONPATH:+:$PYTHONPATH}"

if [[ -z "${AORTA_CORE_FFI_LIB:-}" ]]; then
  for cand in "$REPO_ROOT/bazel-bin/core/libaorta_core.dylib" \
              "$REPO_ROOT/bazel-bin/core/libaorta_core.so"; do
    [[ -f "$cand" ]] && export AORTA_CORE_FFI_LIB="$cand" && break
  done
fi
PY="${PYTHON:-python3}"

for f in "${AORTA_CORE_FFI_LIB:-/nonexistent}" \
         "$EX_BIN/demo_chat_py/demo/Chat.py" \
         "$EX_BIN/demo_chat_schema_meta.py"; do
  [[ -f "$f" ]] || { echo "MISSING prerequisite: $f (build it first)"; exit 2; }
done

fail() { echo "TYPED-EXAMPLES FAIL: $1"; exit 1; }

run_example() {
  local script="$1" marker="$2"
  echo "== $script =="
  local out
  out="$("$PY" "$REPO_ROOT/python/examples/$script" 2>&1)" || { echo "$out"; fail "$script exited non-zero"; }
  grep -q "$marker" <<<"$out" || { echo "$out"; fail "$script missing '$marker'"; }
  grep -vE '^[0-9]{4}-|INFO aorta|WARN aorta|schema_by_key|queryable' <<<"$out"
}

run_example pubsub_typed.py  "PUBSUB_TYPED OK"
run_example service_typed.py "SERVICE_TYPED OK"
run_example action_typed.py  "ACTION_TYPED OK"

echo "TYPED EXAMPLES PASS (generated demo.Chat drives pub/sub + service + action)"
