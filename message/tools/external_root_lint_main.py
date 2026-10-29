#!/usr/bin/env python3
"""`:<name>_lint_test` entry point for an external schema root.

Auto-registered by `aorta_external_schema_package`: every
external root gets this test, and CI must run it — a build alone is NOT
evidence, because the analysis phase cannot read a single byte of the fragment,
of the sources or of the canonical outputs.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from external_root_lint import (
    format_errors,
    lint_external_root,
    load_manifest,
)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--fragment", required=True)
    parser.add_argument("--registry-manifest")
    parser.add_argument("--internal-root-allowlist")
    # `$(locations ...)` expands to a SPACE-SEPARATED list inside one `args`
    # entry, so each flag takes one-or-more values and extends the list.
    parser.add_argument("--source", action="extend", nargs="+", default=[])
    parser.add_argument("--canonical-output", action="extend", nargs="+", default=[])
    # Independent `glob()` census of the calling package: passed one
    # flag per file so an EMPTY census — the normal shape for a shadow
    # package — stays expressible.
    parser.add_argument("--package-census-fbs", action="extend", nargs="*", default=[])
    args = parser.parse_args(argv)

    manifest_path = Path(args.manifest)
    manifest = load_manifest(manifest_path)
    registry_manifest = None
    if args.registry_manifest:
        registry_manifest = json.loads(
            Path(args.registry_manifest).read_text(encoding="utf-8")
        )
    allowlist_text = None
    if args.internal_root_allowlist:
        allowlist_text = Path(args.internal_root_allowlist).read_text(encoding="utf-8")

    errors = lint_external_root(
        manifest=manifest,
        fragment_path=Path(args.fragment),
        source_paths=[Path(path) for path in args.source],
        canonical_output_paths=[Path(path) for path in args.canonical_output],
        registry_manifest=registry_manifest,
        internal_root_allowlist_text=allowlist_text,
        package_census_paths=[Path(path) for path in args.package_census_fbs],
    )
    if errors:
        print(format_errors(manifest_path, errors), file=sys.stderr)
        return 1
    print(
        "external schema root OK: {}:{} — {} item(s), {} physical source(s), "
        "{} file(s) in the calling-package census".format(
            manifest["package"],
            manifest["name"],
            len(manifest["items"]),
            len(manifest["source_inventory"]),
            len(args.package_census_fbs),
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
