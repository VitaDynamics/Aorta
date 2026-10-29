#!/usr/bin/env python3
"""Collect locked cargo-about evidence, then render it in the Bazel archive graph.

`--collect` runs cargo-about over the workspace graph (the policy in about.toml)
and, for every release target triple, records which crates `cargo tree -e normal
--target <triple> -p aorta_core` reaches — the crates a binary built from
aorta_core for that triple links. Rendering without `--target` covers the whole
workspace graph (the C++ SDK bundle); rendering with `--target` restricts the
notices to that reachable set (the Python SDK wheel bundling libaorta_core).
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit

# The crate whose linked closure the per-target notices describe, and the
# triples the release wheels are built for (the crate_universe platform set).
REACHABLE_ROOT = "aorta_core"
REACHABLE_TARGETS = (
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
)

_TREE_LINE = re.compile(
    r"(?P<name>\S+) v(?P<version>\S+)(?: \(proc-macro\))?(?: \((?P<source>(?!\*\))[^)]*)\))?(?: \(\*\))?"
)
# cargo colours the dedupe marker when CARGO_TERM_COLOR=always (CI containers).
_SGR = re.compile(r"\x1b\[[0-9;]*m")

# MPL-2.0 §3.2(a): a binary distribution of MPL-covered code must say how to
# obtain its source. Every MPL-2.0 component gets this sentence, whatever the
# rest of its license expression says.
MPL_SOURCE_SENTENCE = "Source code for this component is available at https://crates.io/crates/{name}/{version}."
# Cargo source IDs of the crates.io index, as (kind, origin, path): the git index
# that Cargo.lock records and the sparse protocol index, both over HTTPS.
_CRATES_IO_INDEXES = {
    ("registry", "github.com", "/rust-lang/crates.io-index"),
    ("sparse", "index.crates.io", "/"),
}
_SPDX_OPERATORS = {"AND", "OR", "WITH"}


def from_crates_io(source):
    """True when a Cargo source ID is exactly a canonical crates.io index.

    Cargo writes these IDs without a port, user info, query or fragment, so any
    of those (an explicit :443 included), another scheme or another path names
    some other registry.
    """
    kind, _, url = (source or "").partition("+")
    if "?" in url or "#" in url:
        return False
    parts = urlsplit(url)
    return (parts.scheme == "https"
            and (kind, parts.netloc, parts.path) in _CRATES_IO_INDEXES)


def is_mpl_2_0(expression):
    """True when an SPDX license expression names MPL-2.0 anywhere in it."""
    tokens = re.findall(r"[A-Za-z0-9.+-]+", expression or "")
    return any(token not in _SPDX_OPERATORS and re.fullmatch(r"MPL-2\.0(\+|-.*)?", token)
               for token in tokens)


def parse_cargo_tree(output):
    """`cargo tree --prefix none --format {p}` lines -> sorted [name, version, kind] rows.

    kind is `path` for workspace members, `git` for git sources and `registry`
    otherwise; the dedupe marker `(*)` and the `(proc-macro)` tag are dropped.
    """
    rows = set()
    for line in output.splitlines():
        line = _SGR.sub("", line).strip()
        if not line:
            continue
        match = _TREE_LINE.fullmatch(line)
        if match is None:
            raise ValueError(f"unparseable cargo tree line: {line!r}")
        source = match.group("source") or ""
        kind = "path" if source.startswith("/") else "git" if source else "registry"
        rows.add((match.group("name"), match.group("version"), kind))
    return [list(row) for row in sorted(rows)]


def reachable_crates(manifest, root, target):
    output = subprocess.check_output([
        "cargo", "tree", "--manifest-path", str(manifest), "--locked", "-e", "normal",
        "--target", target, "-p", root, "--prefix", "none", "--format", "{p}", "--color", "never",
    ], text=True)
    return parse_cargo_tree(output)


def render(data, target=None):
    """Markdown notices for the crates in `data`, optionally only those reachable for `target`."""
    if target is None:
        selected = None
    else:
        rows = data.get("reachable", {}).get(target)
        if rows is None:
            raise ValueError(f"no reachability evidence for {target}; rerun --collect")
        selected = {(name, version) for name, version, _ in rows}
        expected = {(name, version) for name, version, kind in rows if kind != "path"}
        present = {(item["package"]["name"], item["package"]["version"])
                   for item in data["crates"] if item["package"].get("source")}
        missing = sorted(expected - present)
        if missing:
            raise ValueError("LICENSE GATE FAIL\n" + "\n".join(
                f"{name} {version}: reachable for {target} but absent from the cargo-about evidence"
                for name, version in missing))

    def wanted(crate):
        return selected is None or (crate["name"], crate["version"]) in selected

    covered = {}
    sections = []
    for license in data["licenses"]:
        if not license["text"].strip():
            raise ValueError(f"empty license text: {license['id']}")
        users = []
        for user in license["used_by"]:
            crate = user["crate"]
            if not wanted(crate):
                continue
            key = (crate["name"], crate["version"], crate.get("source") or "local")
            covered.setdefault(key, set()).add(license["id"])
            users.append(f"{key[0]} {key[1]} ({key[2]})")
        if users:
            sections.append((license["id"], sorted(users), license["text"]))

    errors = []
    for item in data["crates"]:
        crate = item["package"]
        if not crate.get("source") or not wanted(crate):
            continue  # First-party workspace crates use the bundle LICENSE.
        key = (crate["name"], crate["version"], crate["source"])
        if item["license"] in ("Unknown", "Ignore"):
            errors.append(f"{key[0]} {key[1]}: {item['license']}")
        elif key not in covered:
            errors.append(f"{key[0]} {key[1]}: no license text")
    if errors:
        raise ValueError("LICENSE GATE FAIL\n" + "\n".join(errors))

    if target is None:
        scope = ["Generated by cargo-about 0.9.2 from Cargo.lock. The workspace graph for",
                 "the release targets (all features, including build dependencies)",
                 "conservatively covers the crates linked into libaorta_core.so."]
    else:
        root = data.get("reachable_root", REACHABLE_ROOT)
        scope = ["Generated by cargo-about 0.9.2 from Cargo.lock, restricted to the crates",
                 f"reachable from {root} over normal dependencies for {target}",
                 f"(`cargo tree -e normal --target {target} -p {root}`). That set",
                 "conservatively covers the crates linked into the libaorta_core bundled",
                 "in this wheel (procedural-macro crates only run at compile time)."]
    lines = ["# Third-party notices", "", *scope,
             "This is a license notice set, not a complete SBOM.", "",
             "| Crate | Version | Selected license IDs | Source |",
             "|---|---|---|---|"]
    for (name, version, source), licenses in sorted(covered.items()):
        ids = " AND ".join(sorted(licenses))
        print(f"LICENSE PASS {name} {version}: {ids}")
        lines.append(f"| {name} | {version} | {ids} | {source} |")
    if not covered:
        raise ValueError("LICENSE GATE FAIL: empty crate license list")
    mpl = set()
    for item in data["crates"]:
        crate = item["package"]
        if not crate.get("source") or not wanted(crate):
            continue
        key = (crate["name"], crate["version"], crate["source"])
        if not (is_mpl_2_0(item["license"]) or any(map(is_mpl_2_0, covered.get(key, ())))):
            continue
        if not from_crates_io(crate["source"]):
            raise ValueError(f"LICENSE GATE FAIL\n{key[0]} {key[1]}: MPL-2.0 component from "
                             f"{key[2]}, not crates.io; no source location to state")
        mpl.add(key[:2])
    if mpl:
        lines.extend(["", "## MPL-2.0 source code availability", ""])
        lines.extend(f"- {name} {version}: " + MPL_SOURCE_SENTENCE.format(name=name, version=version)
                     for name, version in sorted(mpl))
    print(f"LICENSE GATE PASS: {len(covered)} crates; rejected=[]")
    for id, users, text in sorted(sections):
        lines.extend(["", f"## {id}", "", "Used by: " + ", ".join(users), "", text])
    return "\n".join(lines) + "\n"


def compare_evidence(first, second):
    """Compare license content and reachability without local package metadata."""
    for key in ("cargo_lock_sha256", "policy_sha256", "reachable_root"):
        if first[key] != second[key]:
            raise ValueError(f"license evidence differs: {key}")
    closures = [{target: {tuple(row) for row in rows} for target, rows in data["reachable"].items()}
                for data in (first, second)]
    if closures[0] != closures[1]:
        raise ValueError("license evidence differs: reachable crates")
    for target in (None, *sorted(first["reachable"])):
        if render(first, target=target) != render(second, target=target):
            raise ValueError(f"license evidence differs: notices for {target or 'workspace'}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--collect", action="store_true")
    parser.add_argument("--licenses", type=Path, required=True)
    parser.add_argument("--compare", type=Path, help="semantically compare Bazel evidence with --licenses")
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--reachable-root", default=REACHABLE_ROOT)
    parser.add_argument("--reachable-target", action="append", default=[],
                        help="triple to record reachability for (default: every release triple)")
    parser.add_argument("--target", help="restrict the rendered notices to this triple's reachable crates")
    parser.add_argument("--flatbuffers-license", type=Path)
    parser.add_argument("--runtime-notices", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    stamps = {
        "cargo_lock_sha256": hashlib.sha256(args.lock.read_bytes()).hexdigest(),
        "policy_sha256": hashlib.sha256(args.config.read_bytes()).hexdigest(),
    }
    if args.collect:
        args.licenses.unlink(missing_ok=True)
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "licenses.json"
            result = subprocess.run([
                "cargo", "about", "generate", "--manifest-path", str(args.manifest),
                "--workspace", "--all-features", "--locked", "--fail",
                "--config", str(args.config), "--format", "json", "--output-file", str(output),
            ], capture_output=True, text=True)
            if result.returncode:
                print(result.stderr, file=sys.stderr)
                metadata = subprocess.check_output([
                    "cargo", "metadata", "--manifest-path", str(args.manifest),
                    "--all-features", "--locked", "--format-version", "1",
                ], text=True)
                for crate in json.loads(metadata)["packages"]:
                    identity = f"'{crate['name']} {crate['version']}'"
                    if crate["manifest_path"] in result.stderr or identity in result.stderr:
                        print(f"LICENSE REJECT {crate['name']} {crate['version']}: "
                              f"{crate.get('license') or 'MISSING'}", file=sys.stderr)
                result.check_returncode()
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            data = json.loads(output.read_text())
        data["reachable_root"] = args.reachable_root
        data["reachable"] = {
            target: reachable_crates(args.manifest, args.reachable_root, target)
            for target in (args.reachable_target or REACHABLE_TARGETS)
        }
        render(data)
        for target in data["reachable"]:
            render(data, target=target)
        data.update(stamps)
        args.licenses.write_text(json.dumps(data, sort_keys=True) + "\n")
        return

    data = json.loads(args.licenses.read_text())
    for key, expected in stamps.items():
        if data.get(key) != expected:
            raise ValueError(f"stale license evidence: {key}; rerun --collect")
    if args.compare:
        compare_evidence(data, json.loads(args.compare.read_text()))
        return
    output = render(data, target=args.target)
    if args.flatbuffers_license:
        output += "\n## Vendored FlatBuffers headers — Apache-2.0\n\n"
        output += args.flatbuffers_license.read_text()
    if args.runtime_notices:
        output += "\n" + args.runtime_notices.read_text()
    args.output.write_text(output)


if __name__ == "__main__":
    main()
