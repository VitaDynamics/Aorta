#!/usr/bin/env bash
# Flow TCK manifest-driven runner (test/flow_tck/SCHEMA.md "Scenario
# manifest (scenarios.json)"). Builds both scenario emitters, then walks
# test/flow_tck/scenarios.json and runs test/flow_tck/driver.py once per
# (scenario, run-variant) pair for every scenario whose wave <= MAX_WAVE
# (env var, default 3 — the current implementation level; see below).
# Scenarios above MAX_WAVE print a SKIP line and are counted, not run.
# Single-plane (plane-qualified, SCHEMA.md D2) rows print their manifest
# `qualification` text for the plane they don't run.
#
# Runs manually (builds both emitters itself) AND as the Bazel test
# //test/flow_tck:flow_tck — under Bazel the two emitter binaries arrive
# pre-built via AORTA_TCK_RUST_BIN / AORTA_TCK_CPP_BIN and the build
# steps are skipped.
#
# No extra env export is needed for wave 3's two Rust-plane RPC scenarios
# (budget_expiry_rpc_call, budget_expiry_queued_server): both set
# AORTA_TEST_LOCAL_ROUTER=1 on themselves before opening a Session
# (rust/examples/flow_tck_emitter.rs), matching the existing
# rust/tests/cross_namespace_sdk.rs precedent for driving the in-process
# local test router from non-`#[cfg(test)]` code — verified empirically
# (see the wave-3 delivery report's VALIDATE section).
#
# Usage: MAX_WAVE=3 test/flow_tck/run_tck.sh
set -u

# GATE MODE detection: TEST_SRCDIR is injected by the Bazel test wrapper
# for EVERY sh_test unconditionally — it is not sourced from the
# target's `env` attribute, so no BUILD.bazel-only edit can spoof or
# remove it (a MAX_WAVE/REQUIRE_ALL pair
# living only in BUILD.bazel's `env` dict was a single-file bypass —
# whoever could edit that dict could silently narrow coverage back to
# green). In gate mode the coverage requirement is HARDCODED here,
# independent of any environment variable: every wave in the manifest
# runs, and any skip fails the run. MAX_WAVE stays an env knob for the
# manual/dev-loop invocation only.
if [ -n "${TEST_SRCDIR:-}" ]; then
  GATE_MODE=1
  REQUIRE_ALL=1
  MAX_WAVE=999999
else
  GATE_MODE=0
  REQUIRE_ALL="${AORTA_TCK_REQUIRE_ALL:-0}"
  MAX_WAVE="${MAX_WAVE:-3}"
fi

if [ "$GATE_MODE" = "1" ]; then
  # Bazel sh_test mode: binaries and data arrive via runfiles; resolve
  # everything relative to TEST_SRCDIR (the script itself is flattened to
  # <package>/<target_name>, so $0-relative navigation breaks here — same
  # contract as cpp/test/expectfail/run_expectfail.sh's Bazel mode).
  : "${AORTA_TCK_RUST_BIN:?gate mode (TEST_SRCDIR set) requires AORTA_TCK_RUST_BIN}"
  : "${AORTA_TCK_CPP_BIN:?gate mode (TEST_SRCDIR set) requires AORTA_TCK_CPP_BIN}"
  REPO_ROOT="${TEST_SRCDIR}/${TEST_WORKSPACE:?}"
  RUST_BIN="$(cd "$(dirname "$AORTA_TCK_RUST_BIN")" && pwd)/$(basename "$AORTA_TCK_RUST_BIN")"
  CPP_BIN="$(cd "$(dirname "$AORTA_TCK_CPP_BIN")" && pwd)/$(basename "$AORTA_TCK_CPP_BIN")"
else
  cd "$(dirname "$0")"
  REPO_ROOT=$(cd ../.. && pwd)

  echo "== flow_tck: building the Rust emitter =="
  if ! (cd "$REPO_ROOT/rust" && cargo build --example flow_tck_emitter --features examples); then
    echo "flow_tck: FAIL (rust emitter build)"
    exit 2
  fi
  RUST_BIN="$REPO_ROOT/target/debug/examples/flow_tck_emitter"

  echo "== flow_tck: building the C++ emitter =="
  if ! (cd "$REPO_ROOT" && bazel build //cpp:flow_tck_emitter); then
    echo "flow_tck: FAIL (cpp emitter build)"
    exit 2
  fi
  CPP_BIN="$REPO_ROOT/bazel-bin/cpp/flow_tck_emitter"
fi
if [ ! -x "$RUST_BIN" ]; then
  echo "flow_tck: FAIL (rust emitter binary not found at $RUST_BIN)"
  exit 2
fi
if [ ! -x "$CPP_BIN" ]; then
  echo "flow_tck: FAIL (cpp emitter binary not found at $CPP_BIN)"
  exit 2
fi

echo "== flow_tck: running the manifest (scenarios.json, MAX_WAVE=$MAX_WAVE, GATE_MODE=$GATE_MODE) =="
MAX_WAVE="$MAX_WAVE" RUST_BIN="$RUST_BIN" CPP_BIN="$CPP_BIN" REPO_ROOT="$REPO_ROOT" \
REQUIRE_ALL="$REQUIRE_ALL" \
  python3 - <<'PYEOF'
import json
import os
import pathlib
import shlex
import subprocess
import sys

repo_root = pathlib.Path(os.environ["REPO_ROOT"])
rust_bin = os.environ["RUST_BIN"]
cpp_bin = os.environ["CPP_BIN"]
max_wave = int(os.environ["MAX_WAVE"])
require_all = os.environ.get("REQUIRE_ALL", "0") == "1"
driver = repo_root / "test" / "flow_tck" / "driver.py"
manifest_path = repo_root / "test" / "flow_tck" / "scenarios.json"

manifest = json.loads(manifest_path.read_text())
scenarios = manifest["scenarios"]


def env_prefix(env):
    """'env K=V ...' prefix (SCHEMA.md `runs` env variants) — driver.py's
    shlex.split() parses this as the standard POSIX `env` binary form."""
    if not env:
        return ""
    parts = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env.items())
    return f"env {parts} "


def plane_cmd(binary, scenario_id, prefix):
    return f"{prefix}{shlex.quote(binary)} {scenario_id}"


pass_count = 0
skip_count = 0
fail_count = 0

for sc in scenarios:
    sid = sc["id"]
    wave = sc["wave"]
    planes = sc["planes"]
    qualification = sc.get("qualification")

    if wave > max_wave:
        print(f"SKIP (wave {wave}): {sid}")
        skip_count += 1
        continue

    # SCHEMA.md "Scenario manifest": a single-plane row is plane-qualified
    # (D2 rule) — print the qualification for whichever plane it doesn't run.
    if qualification:
        for plane in ("rust", "cpp"):
            if plane not in planes:
                print(f"QUALIFICATION ({sid}, skips {plane}): {qualification}")

    runs = sc.get("runs") or [{"env": {}}]

    for run_idx, run in enumerate(runs):
        env = run.get("env", {})
        prefix = env_prefix(env)
        args = ["python3", str(driver), "--scenario", sid]
        if "rust" in planes:
            args += ["--rust-cmd", plane_cmd(rust_bin, sid, prefix)]
        if "cpp" in planes:
            args += ["--cpp-cmd", plane_cmd(cpp_bin, sid, prefix)]

        env_desc = env if env else "{}"
        label = sid if len(runs) == 1 else f"{sid} (run {run_idx}: env={env_desc})"
        proc = subprocess.run(args, capture_output=True, text=True)
        sys.stdout.write(proc.stdout)
        if proc.stdout and not proc.stdout.endswith("\n"):
            sys.stdout.write("\n")
        sys.stderr.write(proc.stderr)

        if proc.returncode == 0:
            print(f"OK: {label}")
            pass_count += 1
        elif proc.returncode == 1:
            print(f"DIVERGE: {label}")
            fail_count += 1
        else:
            print(f"HARNESS ERROR ({proc.returncode}): {label}")
            fail_count += 1

print()
print(
    f"== flow_tck summary: pass={pass_count} skip={skip_count} fail={fail_count} "
    f"(MAX_WAVE={max_wave}) =="
)
if require_all and skip_count > 0:
    # Gate mode (AORTA_TCK_REQUIRE_ALL=1, set by //test/flow_tck:flow_tck):
    # a skipped scenario means coverage was silently narrowed — fail.
    print(f"flow_tck: REQUIRE_ALL set and {skip_count} scenario(s) skipped — failing")
    sys.exit(1)
sys.exit(1 if fail_count > 0 else 0)
PYEOF
verdict=$?

if [ $verdict -eq 0 ]; then
  echo "== flow_tck: ALL PASS/SKIP =="
else
  echo "== flow_tck: FAILURES PRESENT =="
fi
exit $verdict
