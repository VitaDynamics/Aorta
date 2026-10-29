#!/usr/bin/env python3
"""Build deterministic comparison BFBS files from a materialized schema tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path


def _schema_relative(path: Path) -> Path:
    parts = path.parts
    for index in range(len(parts) - 1):
        if parts[index : index + 2] == ("message", "schemas"):
            return Path(*parts[index + 2 :])
    raise ValueError(f"schema input is outside message/schemas: {path}")


def _copy_inputs(inputs: list[Path], schema_root: Path) -> None:
    for source in inputs:
        if source.is_dir():
            source_root = source / "message" / "schemas"
            if not source_root.is_dir():
                raise ValueError(f"schema tree has no message/schemas: {source}")
            candidates = source_root.rglob("*.fbs")
            for path in candidates:
                destination = schema_root / path.relative_to(source_root)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, destination)
            continue
        if source.suffix != ".fbs":
            continue
        relative = _schema_relative(source)
        destination = schema_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)


def _dep_relative(label_path: str) -> Path:
    prefix = "message/schemas/"
    if not label_path.startswith(prefix):
        raise ValueError(f"dependency is outside message/schemas: {label_path}")
    return Path(label_path.removeprefix(prefix))


def build(
    flatc: Path,
    action_wrapper: Path,
    spec_path: Path,
    output: Path,
    inputs: list[Path],
) -> None:
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if set(spec) != {"version", "entries"} or spec["version"] != 1:
        raise ValueError("comparison BFBS spec must contain version=1 and entries")
    output.mkdir(parents=True, exist_ok=True)
    entries = []
    with tempfile.TemporaryDirectory(prefix="aorta_oss_comparison_bfbs.") as tmp:
        schema_root = Path(tmp) / "message" / "schemas"
        _copy_inputs(inputs, schema_root)
        for item in spec["entries"]:
            package = item["pkg"].removeprefix("//message/schemas/")
            root = schema_root / package / item["root_fbs"]
            if not root.is_file():
                raise ValueError(f"comparison BFBS root is missing: {root}")
            original_root = root.read_bytes()
            if item["action_schema"]:
                generated_root = Path(tmp) / "action" / package / item["root_fbs"]
                generated_root.parent.mkdir(parents=True, exist_ok=True)
                subprocess.run(
                    [
                        str(action_wrapper),
                        "--root-fbs",
                        str(root),
                        "--out",
                        str(generated_root),
                    ],
                    check=True,
                )
                root.write_bytes(generated_root.read_bytes())
            destination_dir = output / "comparison_bfbs" / package
            destination_dir.mkdir(parents=True, exist_ok=True)
            include_dirs = [root.parent]
            for dependency in item["dep_fbs"]:
                include_dirs.append((schema_root / _dep_relative(dependency)).parent)
            include_dirs.append(schema_root.parent)
            command = [
                str(flatc),
                "--binary",
                "--schema",
                "--bfbs-builtins",
                "--bfbs-filenames",
                str(root.parent),
                "-o",
                str(destination_dir),
            ]
            for include_dir in dict.fromkeys(include_dirs):
                command.extend(["-I", str(include_dir)])
            command.append(str(root))
            subprocess.run(command, check=True)
            root.write_bytes(original_root)
            generated = destination_dir / f"{Path(item['root_fbs']).stem}.bfbs"
            destination = destination_dir / f"{item['stem']}.bfbs"
            if generated != destination:
                generated.replace(destination)
            data = destination.read_bytes()
            entries.append(
                {
                    "pkg": item["pkg"],
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "stem": item["stem"],
                }
            )
    index = {"version": 1, "entries": entries}
    (output / "comparison_bfbs" / "index.json").write_text(
        json.dumps(index, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flatc", type=Path, required=True)
    parser.add_argument("--action-wrapper", type=Path, required=True)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--input", type=Path, action="append", required=True)
    args = parser.parse_args()
    build(args.flatc, args.action_wrapper, args.spec, args.output, args.input)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
