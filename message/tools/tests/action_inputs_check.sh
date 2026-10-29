#!/usr/bin/env bash
# Action-input membership check.
#
# Verifies that flatc/bfbs genrule actions declare their FULL transitive
# `.fbs` include closure as Bazel `Inputs`, not just the registry's direct
# `dep_fbs` edges. This is a `bazel aquery` membership check on ACTUAL
# built action metadata — not a build-success check (a stale/incomplete
# `srcs` list still builds successfully today on an unsandboxed local
# executor, since flatc's `-I` flags happen to reach the file on disk
# regardless of whether Bazel declared it as an input; the ONLY way to see
# the bug is by inspecting the declared inputs directly).
#
# This is intentionally a plain script, not a `sh_test`/`bazel test` target:
# running `bazel aquery`/`bazel build` recursively from inside a Bazel test
# sandbox is impractical (nested Bazel invocations, no writable output base
# inside the sandbox, etc). Run manually from the repo root, or wire into CI as a
# plain script step if one exists.
#
# Usage:
#   message/tools/tests/action_inputs_check.sh
#
# Exit status: 0 if every check passes, 1 otherwise (with a diagnostic on
# stderr naming which target/expected-input pair failed).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${REPO_ROOT}"

FAILED=0

# check_target_has_inputs <label> <expected-input-substring>...
# Runs `bazel aquery` for the Genrule action of <label> and asserts every
# <expected-input-substring> appears in its declared `Inputs:` line.
check_target_has_inputs() {
  local label="$1"
  shift
  local query="mnemonic(\"Genrule\", ${label})"
  local aquery_out
  if ! aquery_out="$(bazel aquery "${query}" 2>&1)"; then
    echo "FAIL: bazel aquery failed for ${label}" >&2
    echo "${aquery_out}" >&2
    FAILED=1
    return
  fi

  local inputs_line
  inputs_line="$(printf '%s\n' "${aquery_out}" | grep -m1 '^  Inputs: ' || true)"
  if [[ -z "${inputs_line}" ]]; then
    echo "FAIL: no 'Inputs:' line found in aquery output for ${label}" >&2
    printf '%s\n' "${aquery_out}" >&2
    FAILED=1
    return
  fi

  local missing=()
  local expected
  for expected in "$@"; do
    if [[ "${inputs_line}" != *"${expected}"* ]]; then
      missing+=("${expected}")
    fi
  done

  if [[ "${#missing[@]}" -gt 0 ]]; then
    echo "FAIL: ${label} action inputs missing: ${missing[*]}" >&2
    echo "  Actual inputs line: ${inputs_line}" >&2
    FAILED=1
  else
    echo "PASS: ${label} action inputs include: $*"
  fi
}

# --- Case 1: guard_action_probe_cpp (the F2 shape the codegen redesign
# proposal's red-first repro had on the product execute_task action — now
# carried by an Aorta-owned fixture: direct dep_fbs is [action_common.fbs,
# aorta_base.fbs, guard_service_probe.fbs]; the transitive closure ALSO needs
# status.fbs, reached only via guard_service_probe.fbs). ---
check_target_has_inputs \
  "//message/schemas/action/example:guard_action_probe_cpp" \
  "message/schemas/system/aorta_base.fbs" \
  "message/schemas/shared/service_common/status.fbs"

# --- Case 2: guard_action_probe_bfbs (same closure requirement, bfbs
# pipeline). ---
check_target_has_inputs \
  "//message/schemas/action/example:guard_action_probe_bfbs" \
  "message/schemas/system/aorta_base.fbs" \
  "message/schemas/shared/service_common/status.fbs"

# --- Case 3: ordered_fields_probe_cpp (action whose ONLY direct deps are the
# action/base common schemas; the closure must still carry both). ---
check_target_has_inputs \
  "//message/schemas/action/example:ordered_fields_probe_cpp" \
  "message/schemas/shared/action_common/action_common.fbs" \
  "message/schemas/system/aorta_base.fbs"

# --- Case 4: PointCloud_cpp — grandchild closure via Pose.fbs (which is a
# DIRECT dep) reaching Quaternion.fbs/Vector3.fbs (grandchildren, NOT
# direct deps after the Phase 1 direct-edge cleanup). ---
check_target_has_inputs \
  "//message/schemas/topic/foxglove_schema:PointCloud_cpp" \
  "message/schemas/topic/foxglove_schema/Quaternion.fbs" \
  "message/schemas/topic/foxglove_schema/Vector3.fbs"

if [[ "${FAILED}" -ne 0 ]]; then
  echo "action_inputs_check.sh: one or more checks FAILED" >&2
  exit 1
fi

echo "action_inputs_check.sh: all checks PASSED"
