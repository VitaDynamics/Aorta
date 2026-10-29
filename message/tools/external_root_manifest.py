#!/usr/bin/env python3
"""Write an external schema root's `external_root_manifest_version = 1` JSON.

The manifest is the machine interface between an
external root and its lints — NOT a release document: it never enters Aorta's
registry manifest v3, because a user's schema is not Aorta release content.

This tool contributes exactly the facts an analysis-phase macro cannot know:
the SHA-256 of the fragment, of every physical source `.fbs`, and of every
canonical copy output. Everything else is passed through from the macro's
`--declarations` document. The lints re-derive all of it and never trust the
recorded values; recording them is what makes a tampered manifest
DETECTABLE rather than authoritative.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_entry(path: str) -> dict:
    real = Path(path)
    return {
        "path": path,
        "basename": real.name,
        "sha256": sha256_file(real),
    }


def build_manifest(
    declarations: dict,
    fragment: str,
    sources: list[str],
    canonical_outputs: list[str],
) -> dict:
    manifest = dict(declarations)
    manifest["fragment"] = {
        "label": declarations["fragment_label"],
        "symbol": declarations["fragment_symbol"],
        **_file_entry(fragment),
    }
    manifest.pop("fragment_label")
    manifest.pop("fragment_symbol")
    manifest["source_inventory"] = [_file_entry(path) for path in sorted(sources)]

    outputs_by_name = {}
    for path in canonical_outputs:
        entry = _file_entry(path)
        if entry["basename"] in outputs_by_name:
            raise SystemExit(
                f"external-root manifest: two canonical outputs share the basename "
                f"{entry['basename']!r}: {outputs_by_name[entry['basename']]['path']} "
                f"vs {entry['path']}"
            )
        outputs_by_name[entry["basename"]] = entry

    sources_by_name: dict[str, list[dict]] = {}
    for entry in manifest["source_inventory"]:
        sources_by_name.setdefault(entry["basename"], []).append(entry)

    imports = []
    for item in manifest["items"]:
        root_fbs = item["fields"]["root_fbs"]
        output = outputs_by_name.get(root_fbs)
        if output is None:
            raise SystemExit(
                f"external-root manifest: item {item['stem']!r} has no canonical "
                f"output named {root_fbs!r} (outputs: {sorted(outputs_by_name)})"
            )
        candidates = sources_by_name.get(root_fbs, [])
        if len(candidates) != 1:
            raise SystemExit(
                f"external-root manifest: item {item['stem']!r} must have exactly one "
                f"source named {root_fbs!r} in the inventory, found {len(candidates)}"
            )
        source = candidates[0]
        imports.append(
            {
                "stem": item["stem"],
                "source_label": item["source_label"],
                "source_path": source["path"],
                "source_sha256": source["sha256"],
                "canonical_output_label": item["canonical_output_label"],
                "canonical_output_path": output["path"],
                "canonical_output_sha256": output["sha256"],
            }
        )
    manifest["imports"] = imports

    direct_deps: list[str] = []
    for item in manifest["items"]:
        for dep in item["fields"].get("dep_fbs", []) or []:
            if dep not in direct_deps:
                direct_deps.append(dep)
    manifest["direct_dep_labels"] = sorted(direct_deps)
    return manifest


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--declarations", required=True)
    parser.add_argument("--fragment", required=True)
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument("--canonical-output", action="append", default=[])
    args = parser.parse_args(argv)

    declarations = json.loads(Path(args.declarations).read_text(encoding="utf-8"))
    manifest = build_manifest(
        declarations=declarations,
        fragment=args.fragment,
        sources=args.source,
        canonical_outputs=args.canonical_output,
    )
    Path(args.out).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
