#!/usr/bin/env bash
# The reentrancy-guard completeness lint (simplified version): a python
# lexical scanner + in-code annotations. The guarded-entry seed list is
# regenerated from core/src/ffi/c_api/*.rs at run time; every C++ call
# site of a guarded symbol must be preceded by an AORTA_CHECK_NOT_REENTRANT
# in the same function body or carry a `// reentrancy:` annotation.
#
# Wiring mirrors run_flow_resume_lint.sh (Bazel runfiles:
# AORTA_BAZEL_RUNFILES=1). Manual run:
#   bash cpp/lint/run_reentrancy_lint.sh
set -u

if [ "${AORTA_BAZEL_RUNFILES:-0}" = "1" ]; then
  REPO_ROOT="$(pwd)"
else
  REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
fi

python3 "$REPO_ROOT/cpp/lint/check_reentrancy_guards.py" "$REPO_ROOT" || exit 1
echo "reentrancy guard completeness lint PASSED"
