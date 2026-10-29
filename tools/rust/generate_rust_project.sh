#!/usr/bin/env bash

set -euo pipefail

GEN_TOOL="${1:-}"
if [[ -z "${GEN_TOOL}" ]]; then
  echo "missing gen_rust_project tool path" >&2
  exit 2
fi
shift || true

# Resolve tool path before changing working directory.
if [[ "${GEN_TOOL}" != /* ]]; then
  GEN_TOOL="$(cd "$(dirname "${GEN_TOOL}")" && pwd)/$(basename "${GEN_TOOL}")"
fi

# Bazel run executes under runfiles; switch back to workspace root.
WORKSPACE_DIR="${BUILD_WORKSPACE_DIRECTORY:-${PWD}}"
cd "${WORKSPACE_DIR}"

# Default to Aorta Rust-related packages to avoid unrelated workspace target failures.
if [[ $# -eq 0 ]]; then
  set -- \
    "//core/..." \
    "//rust/..." \
    "//message/..." \
    "//tools/aorta-cli/..." \
    "//tools/aorta-record/..." \
    "//tools/zenohd/..." \
    "//benchmark/aorta_rust/..."
fi

"${GEN_TOOL}" "$@"

RUST_PROJECT_JSON="${PWD}/rust-project.json"
if [[ ! -f "${RUST_PROJECT_JSON}" ]]; then
  echo "warning: rust-project.json not found at ${RUST_PROJECT_JSON}" >&2
  exit 0
fi

python3 - "${RUST_PROJECT_JSON}" <<'PY'
import json
import os
import sys

project_path = sys.argv[1]
repo_root = os.path.realpath(os.getcwd())

with open(project_path, "r", encoding="utf-8") as f:
    project = json.load(f)

changed = False
patched = 0

for crate in project.get("crates", []):
    if not crate.get("is_workspace_member"):
        continue

    root_module = crate.get("root_module")
    if not isinstance(root_module, str) or not root_module:
        continue

    root_module_real = os.path.realpath(root_module)
    if not root_module_real.startswith(repo_root + os.sep):
        continue

    root_dir = os.path.dirname(root_module_real)
    extra_dirs = [root_dir]

    for sibling in ("../test", "../ffi"):
        candidate = os.path.realpath(os.path.join(root_dir, sibling))
        if os.path.isdir(candidate):
            extra_dirs.append(candidate)

    source = crate.get("source")
    if not isinstance(source, dict):
        source = {"include_dirs": [], "exclude_dirs": []}

    include_dirs = list(source.get("include_dirs") or [])
    exclude_dirs = list(source.get("exclude_dirs") or [])

    merged_include_dirs = []
    for path in include_dirs + extra_dirs:
        if path not in merged_include_dirs:
            merged_include_dirs.append(path)

    if (
        source.get("include_dirs") != merged_include_dirs
        or source.get("exclude_dirs") != exclude_dirs
        or crate.get("source") != source
    ):
        source["include_dirs"] = merged_include_dirs
        source["exclude_dirs"] = exclude_dirs
        crate["source"] = source
        changed = True
        patched += 1

if changed:
    with open(project_path, "w", encoding="utf-8") as f:
        json.dump(project, f, indent=2)
        f.write("\n")

print(
    f"patched rust-project.json source roots for {patched} workspace crate(s)"
)
PY
