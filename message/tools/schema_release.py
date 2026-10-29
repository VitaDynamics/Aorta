"""Stage and verify versioned schema release inputs supplied by Bazel."""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil

from schema_include_utils import SchemaIncludeResolutionError, parse_schema_includes, resolve_schema_include_path

RELEASE_ARTIFACT_VERSION = 1
ROLES = {"rust", "bfbs", "schema_meta_rust", "schema_meta_cpp", "schema_meta_py", "action_schema", "rust_wrapper"}


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _relative(value):
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or str(path) != value or value == ".":
        raise ValueError("UNSAFE_PATH: " + value)
    return value


def _read(path):
    path = Path(path)
    if not path.is_file():
        raise ValueError("MISSING_INPUT: " + str(path))
    return path


def _inventory(root):
    paths = [root] + sorted(root.rglob("*"))
    for path in paths:
        if path.is_symlink():
            raise ValueError("SYMLINK: " + str(path))
    return {p.relative_to(root).as_posix(): _digest(p) for p in paths if p.is_file() and p != root / "STAMP.json"}


def stage(plan, registry_path, output):
    if plan["release_artifact_version"] != RELEASE_ARTIFACT_VERSION:
        raise ValueError("ARTIFACT_VERSION")
    registry_path = _read(registry_path)
    registry = json.loads(registry_path.read_text())
    expected = []
    for entry in plan["entries"]:
        row = dict(entry)
        row["schema_definition"] = row.pop("row")
        expected.append(row)
    key = lambda row: (row["origin_module"], row["fbs_label"])
    if sorted(expected, key=key) != sorted(registry["entries"], key=key):
        raise ValueError("REGISTRY_MISMATCH: configured fragments differ from registry")
    if registry["rules_api"] != 2 or registry["composed_manifest_version"] != 2 or not registry.get("verified_tools"):
        raise ValueError("REGISTRY_CONTRACT")
    tools = plan["tools"]
    expected_tools = {
        "flatc_version": "flatc version " + tools["flatbuffers_version"],
        "flatc_sha256": _digest(_read(tools["flatc"])),
        "codegen_sha256": {path: _digest(_read(path)) for path in tools["codegen_sources"]},
    }
    if registry["verified_tools"] != expected_tools or registry["codegen_version"] != tools["codegen_version"] or registry["flatbuffers_version"] != tools["flatbuffers_version"]:
        raise ValueError("TOOL_MISMATCH")
    by_logical = {e["logical_include_path"]: e for e in plan["entries"]}
    if len(by_logical) != len(plan["entries"]):
        raise ValueError("SOURCE_OWNER: duplicate logical source")
    selected = {e["logical_include_path"] for e in plan["entries"] if e["origin_module"] == plan["identity"]["module_name"]}
    if not selected:
        raise ValueError("MODULE_MISSING")
    selected.update(plan["implicit_sources"])
    declared_sources = {Path(e["source_identity"]["path"]).resolve(): logical for logical, e in by_logical.items()}
    def closure(roots):
        result = set(roots)
        pending = list(result)
        while pending:
            logical = pending.pop()
            if logical not in by_logical:
                raise ValueError("INCLUDE_MISSING: " + logical)
            source = _read(by_logical[logical]["source_identity"]["path"])
            for include in parse_schema_includes(source):
                try:
                    if include.startswith("schemas/"):
                        if include not in by_logical:
                            raise ValueError("INCLUDE_MISSING: " + include)
                        resolved = Path(by_logical[include]["source_identity"]["path"]).resolve()
                    else:
                        resolved = resolve_schema_include_path(source, include, list(declared_sources)).resolve()
                except SchemaIncludeResolutionError as error:
                    raise ValueError("INCLUDE_MISSING: " + include) from error
                if resolved not in declared_sources:
                    raise ValueError("INCLUDE_MISSING: " + include)
                dependency = declared_sources[resolved]
                if dependency not in result:
                    result.add(dependency)
                    pending.append(dependency)
        return result

    selected = closure(selected)
    entries = [by_logical[name] for name in sorted(selected)]
    by_label = {e["fbs_label"]: e for e in entries}
    source_closures = {
        e["fbs_label"]: {by_logical[logical]["source_identity"]["path"] for logical in closure([e["logical_include_path"]] + plan["implicit_sources"])}
        for e in entries
    }
    files = {}

    def add(path, relative):
        relative = _relative(relative)
        source = _read(path)
        if relative in files and files[relative] != source:
            raise ValueError("DESTINATION_COLLISION: " + relative)
        files[relative] = source

    for entry in entries:
        add(entry["source_identity"]["path"], "sources/" + entry["logical_include_path"])
    artifacts = []
    roles = set()
    for item in plan["artifacts"]:
        if item["entry"] not in by_label:
            continue
        source = by_label[item["entry"]]["source_identity"]["path"]
        ancestry = set(item["sources"])
        source_paths = source_closures[item["entry"]]
        if item["role"] == "rust_support":
            valid = bool(ancestry & source_paths) and ancestry <= source_paths
        else:
            valid = source in ancestry and ancestry <= source_paths
        if not valid:
            raise ValueError("OUTPUT_OWNER: " + item["path"])
        add(item["path"], item["relative"])
        roles.add((item["entry"], item["role"]))
        artifacts.append({k: v for k, v in item.items() if k not in {"path", "sources"}})
    for entry in entries:
        for role in ROLES & entry["output_labels"].keys():
            if (entry["fbs_label"], role) not in roles:
                raise ValueError("MISSING_OUTPUT: " + entry["fbs_label"] + " " + role)
    modules = {e["origin_module"] for e in entries}
    licensed = set()
    for item in plan["licenses"]:
        if item["module"] in modules:
            add(item["path"], item["relative"])
            licensed.add(item["module"])
    if modules - licensed:
        raise ValueError("LICENSE_MISSING: " + ", ".join(sorted(modules - licensed)))
    add(registry_path, "registry.json")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError("OUTPUT_NOT_EMPTY")
    for relative, source in sorted(files.items()):
        dest = output / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, dest)
    manifest = {
        "release_artifact_version": RELEASE_ARTIFACT_VERSION,
        "identity": plan["identity"],
        "verified_tools": registry["verified_tools"],
        "sources": [{"logical_include_path": e["logical_include_path"], "origin_module": e["origin_module"], "owner": e["source_identity"]["owner"]} for e in entries],
        "artifacts": artifacts,
        "sdk_crate_owners": plan.get("sdk_crate_owners", {}),
        "rust_crates": {label: value for label, value in plan.get("rust_crates", {}).items() if label in by_label},
    }
    (output / "ARTIFACTS.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    stamp = {"release_artifact_version": RELEASE_ARTIFACT_VERSION, "identity": plan["identity"], "files": _inventory(output)}
    (output / "STAMP.json").write_text(json.dumps(stamp, indent=2, sort_keys=True) + "\n")
    verify(output, plan["identity"])


def verify(root, expected_identity):
    root = Path(root)
    inventory = _inventory(root)
    stamp = json.loads(_read(root / "STAMP.json").read_text())
    if stamp.get("release_artifact_version") != RELEASE_ARTIFACT_VERSION or stamp.get("identity") != expected_identity:
        raise ValueError("STAMP_MISMATCH")
    if stamp.get("files") != inventory:
        raise ValueError("CONTENT_MISMATCH")
    manifest = json.loads((root / "ARTIFACTS.json").read_text())
    if manifest["identity"] != expected_identity or manifest["release_artifact_version"] != RELEASE_ARTIFACT_VERSION:
        raise ValueError("STAMP_MISMATCH")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    stage(json.loads(Path(args.plan).read_text()), args.registry, args.output)


if __name__ == "__main__":
    main()
