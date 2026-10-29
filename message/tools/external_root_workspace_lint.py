#!/usr/bin/env python3
"""`bazel run //<pkg>:<name>_workspace_lint` — physical source-tree census.

A Bazel runfiles tree is itself a forest of symlinks,
so no test running inside it can tell an AUTHOR's symlink from Bazel's own.
This tool therefore refuses to run without `BUILD_WORKSPACE_DIRECTORY` and
re-discovers the source root with `lstat` on the REAL checkout:

  * every `.fbs` under `source_prefix` is found by walking the filesystem, NOT
    by trusting the caller's own `source_inventory` — a file the author forgot
    to glob is exactly the failure mode this exists to catch;
  * any symlink on the way (directory or file) is rejected outright;
  * the discovered set and the fragment's declared items must be a bijection.

Only a root whose sources live in the caller's own workspace gets this target.
A cross-module root's physical census belongs to the source repository's own
standalone lint, proven from `git ls-files`; that is a separate gate and is not
substitutable by this one.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from external_root_lint import ExternalRootLintError, load_manifest


def discover_sources(source_root: Path, workspace: Path) -> tuple[list[Path], list[str]]:
    errors: list[str] = []
    found: list[Path] = []
    if not source_root.is_dir():
        return [], [f"source root does not exist: {source_root}"]
    for path in sorted(source_root.rglob("*")):
        relative = path.relative_to(workspace)
        current = workspace
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                errors.append(f"symlink rejected in the source root: {current}")
                break
        else:
            if path.is_file() and path.suffix == ".fbs":
                found.append(path)
    return found, errors


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args(argv)

    workspace_env = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if not workspace_env:
        print(
            "external-root workspace lint must run via `bazel run` "
            "(BUILD_WORKSPACE_DIRECTORY is unset); running it from a runfiles tree "
            "would inspect Bazel's own symlinks instead of the author's files.",
            file=sys.stderr,
        )
        return 2
    workspace = Path(workspace_env)

    manifest = load_manifest(Path(args.manifest))
    package = manifest["package"]
    if manifest["source_package"] != package:
        raise ExternalRootLintError(
            "workspace lint only applies to a root whose sources live in the caller's "
            f"own package ({package} vs {manifest['source_package']})"
        )
    package_dir = workspace / package[2:]
    prefix = manifest["source_prefix"]
    source_root = package_dir / prefix if prefix else package_dir

    found, errors = discover_sources(source_root, workspace)
    declared = {item["fields"]["root_fbs"] for item in manifest["items"]}

    relative_found = {}
    for path in found:
        rel = path.relative_to(source_root).as_posix()
        if "/" in rel:
            errors.append(
                f"source {rel} is nested under {prefix or '.'}; canonical slots are flat "
                "per package, so a nested source has no slot"
            )
            continue
        relative_found[rel] = path

    unregistered = sorted(set(relative_found) - declared)
    missing = sorted(declared - set(relative_found))
    if unregistered:
        errors.append(
            f"physical .fbs file(s) in {source_root} are not registered in the "
            f"fragment: {unregistered}"
        )
    if missing:
        errors.append(f"fragment item(s) with no physical source in {source_root}: {missing}")

    if errors:
        print(
            "external schema root workspace lint failed ({}:{}):\n  - {}".format(
                package, manifest["name"], "\n  - ".join(errors)
            ),
            file=sys.stderr,
        )
        return 1
    print(
        f"external schema root workspace OK: {package}:{manifest['name']} — "
        f"{len(relative_found)} physical source(s) under {source_root}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
