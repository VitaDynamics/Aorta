#!/usr/bin/env python3
"""Regenerate committed telemetry FlatBuffers Rust bindings + BFBS constants.

Also regenerates the standalone `core/src/contextstore_bfbs_generated.rs`
(BFBS only — `ContextValue` keeps its hand-written binding); see
`regen_contextstore_bfbs`.

The telemetry families intentionally live OUTSIDE `registry.bzl`: their Rust
bindings and BFBS bytes are committed under `core/src/telemetry/` and a
Cargo-only `core/build.rs` drift test guards them against the `.fbs` sources.
This script is the reproducible "how to regenerate" that the drift-test failure
message refers to.

For each schema it: flatc --rust → insert the module-level `#![allow]` header
(matches the committed convention) → append `pub const <STEM>_BFBS` for
published families → rustfmt the file.

Note: unlike the SDK schemas, telemetry composition does NOT need
`patch_flatc_rust.py` — flatc emits valid `crate::<stem>_generated::*` paths for
`include`d schemas (resolved by the crate-root `mod` decls in `core/src/lib.rs`),
and the committed convention deliberately omits the `pub use self::aorta::*;`
re-export (fb.rs reaches types via the full `::aorta::telemetry` path).

Usage:
  python3 message/tools/regen_telemetry_fb.py            # regen all committed files
  python3 message/tools/regen_telemetry_fb.py --verify   # drift-check every committed
                                                         # BFBS constant vs a fresh
                                                         # compile (no write); safe as
                                                         # a PR gate. BFBS bytes are
                                                         # version-stable, the --rust
                                                         # binding text is not.
Requires flatc (FLATC env or on PATH) and rustfmt.
"""
import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MESSAGE = ROOT / "message"
SYS = MESSAGE / "schemas" / "system"
OUT = ROOT / "core" / "src" / "telemetry"
CORE_SRC = ROOT / "core" / "src"
PATCH = MESSAGE / "tools" / "patch_flatc_rust.py"
FLATC = os.environ.get("FLATC", "flatc")

ALLOW_HEADER = """#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(mismatched_lifetime_syntaxes)]
#![allow(clippy::extra_unused_lifetimes)]
// FlatBuffers-generated; the unsafe blocks here come straight from flatc and
// would be wiped on the next regen. Suppressing the lint here is preferable to
// per-block SAFETY comments that don't survive a schema update.
#![allow(clippy::undocumented_unsafe_blocks)]
"""

BFBS_COMMENT = (
    "// BFBS (binary reflection schema), committed so the core runtime can\n"
    "// register the schema without a Bazel codegen dependency. Regenerate with\n"
    "// flatc + `cargo fmt`; core/build.rs drift-tests it against the .fbs.\n"
)

# (schema stem, BFBS const name | None, reexport types at module root?)
#  - const None  → never published standalone (the shared KeyValue carrier).
#  - reexport    → this schema is `include`d by another; flatc references its
#    types through a root glob `use crate::<stem>_generated::*`, so the
#    namespaced types must be re-exported at the module root. The root schema
#    (vital_snapshot) and standalone families (runtime_metrics) don't need it.
SCHEMAS = [
    ("telemetry_common", None, True),
    ("telemetry_node_identity", "TELEMETRY_NODE_IDENTITY_BFBS", True),
    ("telemetry_resource_inventory", "TELEMETRY_RESOURCE_INVENTORY_BFBS", True),
    ("telemetry_runtime_metrics", "TELEMETRY_RUNTIME_METRICS_BFBS", False),
    ("telemetry_vital_snapshot", "TELEMETRY_VITAL_SNAPSHOT_BFBS", False),
]


def run(*args):
    subprocess.run([str(a) for a in args], check=True)


def insert_allows(rs: Path):
    text = rs.read_text(encoding="utf-8")
    if "#![allow(clippy::undocumented_unsafe_blocks)]" in text:
        return
    out, inserted = [], False
    for ln in text.splitlines(keepends=True):
        out.append(ln)
        if not inserted and ln.strip() == "// @generated":
            out.append(ALLOW_HEADER)
            inserted = True
    if not inserted:
        sys.exit(f"no `// @generated` marker in {rs}")
    rs.write_text("".join(out), encoding="utf-8")


def append_bfbs(rs: Path, stem: str, const: str, out_dir: Path):
    run(FLATC, "--binary", "--schema", "--bfbs-builtins", "-o", out_dir,
        "-I", MESSAGE, SYS / f"{stem}.fbs")
    bfbs = (out_dir / f"{stem}.bfbs").read_bytes()
    (out_dir / f"{stem}.bfbs").unlink()
    literals = ", ".join(f"0x{b:02x}" for b in bfbs)
    with rs.open("a", encoding="utf-8") as f:
        f.write(f"\n{BFBS_COMMENT}pub const {const}: &[u8] = &[{literals}];\n")


def append_type_reexport(rs: Path):
    with rs.open("a", encoding="utf-8") as f:
        f.write(
            "\n// Re-export the namespaced types at the module root so sibling generated\n"
            "// modules can resolve them through flatc's `use crate::<stem>_generated::*`\n"
            "// globs (this schema is `include`d by telemetry_vital_snapshot).\n"
            "pub use self::aorta::telemetry::*;\n"
        )


def regen_one(stem: str, const, reexport: bool, out_dir: Path):
    run(FLATC, "--rust", "-o", out_dir, "-I", MESSAGE, SYS / f"{stem}.fbs")
    rs = out_dir / f"{stem}_generated.rs"
    insert_allows(rs)
    if reexport:
        append_type_reexport(rs)
    if const:
        append_bfbs(rs, stem, const, out_dir)
    run("rustfmt", "--edition", "2021", rs)
    return rs


def regen_contextstore_bfbs():
    """Emit `core/src/contextstore_bfbs_generated.rs` (BFBS const only).

    `ContextValue` has a HAND-WRITTEN wire binding (`core/src/contextstore_fb.rs`),
    so unlike the telemetry families we do NOT run `flatc --rust` for it — only
    its committed BFBS is generated, in a standalone file that contextstore_fb.rs
    re-exports. The recorder embeds this so MCAP readers (Foxglove) parse typed
    `aorta.ctx.ContextValue` channels instead of an opaque raw encoding.
    """
    stem = "contextstore"
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        run(FLATC, "--binary", "--schema", "--bfbs-builtins", "-o", td,
            "-I", MESSAGE, SYS / f"{stem}.fbs")
        bfbs = (td / f"{stem}.bfbs").read_bytes()
    literals = ", ".join(f"0x{b:02x}" for b in bfbs)
    header = (
        "// @generated — do not edit by hand; produced by\n"
        "// message/tools/regen_telemetry_fb.py from\n"
        "// message/schemas/system/contextstore.fbs.\n"
        "//\n"
        "// BFBS (binary reflection schema) for `aorta.ctx.ContextValue`. ContextValue\n"
        "// has a hand-written wire binding (core/src/contextstore_fb.rs), so only its\n"
        "// BFBS is generated here — committed (like the telemetry `*_BFBS` constants)\n"
        "// so the recorder can embed it in MCAP and write a typed `flatbuffer` channel\n"
        "// without a Bazel codegen dependency. core/build.rs drift-tests it.\n"
    )
    rs = CORE_SRC / "contextstore_bfbs_generated.rs"
    rs.write_text(
        f"{header}pub const CONTEXT_VALUE_BFBS: &[u8] = &[{literals}];\n",
        encoding="utf-8",
    )
    run("rustfmt", "--edition", "2021", rs)
    return rs


def _compile_bfbs(fbs, out_dir):
    """Compile `fbs` to its BFBS bytes (the version-stable reflection schema).

    Takes a full path rather than a stem: the committed BFBS constants are no
    longer all rooted at `schemas/system/` (`foxglove.Log` lives under
    `schemas/topic/foxglove_schema/`).
    """
    run(FLATC, "--binary", "--schema", "--bfbs-builtins", "-o", out_dir,
        "-I", MESSAGE, fbs)
    return (out_dir / f"{fbs.stem}.bfbs").read_bytes()


def _committed_const_bytes(rs_path, const):
    """Extract the byte array of a `pub const <const>: &[u8] = &[...]` from a .rs."""
    text = rs_path.read_text(encoding="utf-8")
    m = re.search(rf"{const}\s*:\s*&\[u8\]\s*=\s*&\[(.*?)\]\s*;", text, re.S)
    if not m:
        sys.exit(f"VERIFY ERROR: const {const} not found in {rs_path}")
    return bytes(int(x, 16) for x in re.findall(r"0x[0-9a-fA-F]{2}", m.group(1)))


def verify_bfbs():
    """Drift-check EVERY committed BFBS constant against a freshly-compiled one.

    We compare BFBS BYTES, not the generated `--rust` text. BFBS is stable across
    flatc versions; the Rust binding text is NOT (older flatc emits `::flatbuffers::`,
    newer emits `flatbuffers::`), so the committed bindings are intentionally not
    byte-reproducible here. The BFBS constant is what the runtime/recorder embeds and
    what core/build.rs drift-tests — the invariant worth gating. Covers the COMPLETE
    set of committed BFBS constants: the telemetry families, ContextValue, and
    foxglove.Log. Exits non-zero if any is stale; safe to call from a PR-gating CI job.
    """
    stale = []
    # (fbs path, BFBS const, committed .rs). The telemetry families this script
    # generates, PLUS the two committed elsewhere — ContextValue (hand-written
    # binding) and foxglove.Log (core/build.rs flatc). This is the COMPLETE set
    # of committed BFBS constants in the tree, so one --verify run is a full
    # gate. Paths are explicit because these no longer all live under
    # `schemas/system/`.
    targets = [
        (SYS / f"{stem}.fbs", const, OUT / f"{stem}_generated.rs")
        for stem, const, _ in SCHEMAS
        if const
    ] + [
        (SYS / "contextstore.fbs", "CONTEXT_VALUE_BFBS",
         CORE_SRC / "contextstore_bfbs_generated.rs"),
        (MESSAGE / "schemas" / "topic" / "foxglove_schema" / "Log.fbs",
         "FOXGLOVE_LOG_BFBS", CORE_SRC / "foxglove_log_generated.rs"),
    ]
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        for fbs, const, rs_path in targets:
            fresh = _compile_bfbs(fbs, td)
            committed = _committed_const_bytes(rs_path, const)
            ok = fresh == committed
            print(f"  {'OK   ' if ok else 'STALE'} {const}  ({fbs.name})")
            if not ok:
                stale.append(const)
    if stale:
        sys.exit(f"VERIFY FAILED: {len(stale)} stale BFBS constant(s) — re-generate "
                 f"with the pinned flatc and commit: {', '.join(stale)}")
    print(f"VERIFY OK: all {len(targets)} committed BFBS constants match freshly-compiled schemas")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", action="store_true",
                    help="drift-check every committed BFBS constant vs freshly-compiled schemas (no write)")
    args = ap.parse_args()

    if args.verify:
        verify_bfbs()
        return

    for stem, const, reexport in SCHEMAS:
        rs = regen_one(stem, const, reexport, OUT)
        print(f"regenerated {rs.name}" + (f" (+{const})" if const else ""))

    # ContextValue: hand-written binding, BFBS-only committed file.
    rs = regen_contextstore_bfbs()
    print(f"regenerated {rs.name} (+CONTEXT_VALUE_BFBS)")


if __name__ == "__main__":
    main()
