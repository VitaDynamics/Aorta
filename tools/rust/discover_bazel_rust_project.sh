#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"

args=()
if [[ $# -gt 0 ]]; then
  args+=("$1")
else
  # Avoid falling back to //... when rust-analyzer sends no path argument.
  args+=("rust/BUILD.bazel")
fi

# rust-analyzer treats stderr output as a command failure.
exec bazel run @rules_rust//tools/rust_analyzer:discover_bazel_rust_project -- "${args[@]}" 2>/dev/null
