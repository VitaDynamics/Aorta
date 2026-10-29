#!/usr/bin/env python3
import argparse
import os
import re
import hashlib
import sys
from pathlib import Path
from typing import List, Optional

TOOL_DIR = Path(__file__).resolve().parent
if str(TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(TOOL_DIR))

import schema_reflection

# ---------------------------------------------------------------------------
# Every value this tool emits is computed from the compiled `.bfbs` via
# `schema_reflection.load_bfbs` (flatc's own semantic model) — `--bfbs` is
# REQUIRED for ALL languages. The computation first moved over while keeping
# the regex parsers (STRUCT_RE et al., parse_structs/parse_tables/parse_root/
# parse_namespace/extract_includes/has_aorta_header_field/compute_struct_size)
# in place, unused, as a surgical-diff aid; they are now deleted, together
# with the then-dead `--src` CLI plumbing (both this tool's own `--src` flag
# and the `.bzl` callers that passed it). Their known defects
# (grandchild includes invisible to compute_struct_size; bare-name last-wins
# dict updates; first-namespace-only parse_namespace; name-only
# has_aorta_header_field) are exactly what the reflection path fixes — see
# gen_schema_meta_reflection_test.py.
# ---------------------------------------------------------------------------


def reflection_fixed_size(loaded) -> Optional[int]:
    """FIXED_SIZE from flatc's ground truth: a STRUCT root's own
    `Object.bytesize` (grandchild-complete by construction — flatc computed
    it over the full include closure); a table root or an absent root has no
    fixed size. NOTE: the pinned flatc rejects `root_type <struct>` ("root
    type must be a table"), so every flatc-compilable schema takes the None
    branch today — the struct branch is the honest semantic kept for the
    reflection data model (exercised via a synthetically re-rooted bfbs in
    gen_schema_meta_reflection_test.py), replacing the old regex
    `compute_struct_size` walk that silently emitted None whenever a struct
    lived in a grandchild include."""
    root_id = loaded.root_type
    if root_id is None or root_id.kind != "struct":
        return None
    return loaded.structs[root_id.qualified_name].bytesize


def reflection_has_aorta_header(loaded, type_id) -> bool:
    """Tightened HAS_AORTA_HEADER semantic: a table carries an
    Aorta header iff it declares a non-deprecated field NAMED `aorta_header`
    whose type is EXACTLY the table `aorta.sys.AortaHeader` (qualified-name
    match on the field's referenced TypeId; the `shape` check additionally
    rejects e.g. a vector-of-AortaHeader field). The old regex check matched
    the field NAME alone, so any `aorta_header`-named field of any type set
    the flag. Deprecated fields do not count: flatc
    emits no accessors for them, so a deprecated header field is not
    readable as a header at runtime (no registry schema has one)."""
    if type_id is None or type_id.kind != "table":
        return False
    obj = loaded.tables.get(type_id.qualified_name)
    if obj is None:
        return False
    for field in obj.fields:
        if field.name != "aorta_header" or field.deprecated:
            continue
        return (
            field.shape == "table:AortaHeader"
            and field.target is not None
            and field.target.qualified_name == "aorta.sys.AortaHeader"
        )
    return False


def reflection_local_type_ids(loaded) -> list:
    """Root-local table/struct TypeIds for the C++ emitter, in canonical
    order: the root type first (ALWAYS included, even if the `root_type`
    statement names an include-declared type — matching the old emitter's
    unconditional `local_type_names.append(root_type)`), then every other
    root-local table/struct sorted by qualified name. For a single-namespace
    root file (every current registry schema) the qualified-name sort equals
    the old bare-name sort (shared prefix), which the PRE/POST byte-diff
    gate for the switch to bfbs metadata depends on. Root-local
    classification comes from the bfbs's own include graph
    (`schema_reflection.is_root_local`), not from which files happened to
    be passed as `--src`."""
    root_id = loaded.root_type
    local = [
        od.type_id
        for od in list(loaded.tables.values()) + list(loaded.structs.values())
        if schema_reflection.is_root_local(od.type_id, loaded)
    ]
    rest = sorted(
        (t for t in local if root_id is None or t.qualified_name != root_id.qualified_name),
        key=lambda t: t.qualified_name,
    )
    return ([root_id] if root_id is not None else []) + rest


def reflection_rootless_namespaces(loaded) -> List[str]:
    """Namespace parts for the (empty) forward-declaration block of a
    schema with NO local tables/structs — the enums-only shared-types case
    (e.g. cpp/test/tier2_action_codegen/xfer_types.fbs). The old emitter
    used the first textual `namespace` statement; reflection has no textual
    statement order, so the namespace is derived from the root-local
    DECLARED types (enums/unions — there are no tables/structs here).
    Byte-compat justification: this block contains no declarations, so its
    namespace is cosmetic; every real rootless schema declares exactly one
    namespace, and multiple distinct namespaces hard-error (fail-closed,
    matching the repo convention) rather than guessing which one the old
    single-`NAMESPACE_RE.search` would have picked."""
    namespaces = sorted(
        {
            d.type_id.namespace
            for d in list(loaded.enums.values()) + list(loaded.unions.values())
            if schema_reflection.is_root_local(d.type_id, loaded) and d.type_id.namespace
        }
    )
    if not namespaces:
        return []
    if len(namespaces) > 1:
        raise schema_reflection.SchemaLoadError(
            "rootless schema declares root-local types under multiple "
            "namespaces {} — cannot derive a single namespace for the "
            "forward-declaration block (fail-closed; no registry schema "
            "has this shape)".format(namespaces)
        )
    return namespaces[0].split(".")


def write_cpp_forward_decls(f, ordered_ids, fallback_namespaces) -> None:
    """Forward-declare every local type under ITS OWN namespace (the old
    emitter wrapped everything in the root file's FIRST namespace).
    Consecutive same-namespace types share one
    namespace block, so a single-namespace schema (every current registry
    schema) produces byte-identical output to the old single-block emitter;
    `fallback_namespaces` reproduces the old (empty) block for schemas with
    no local tables/structs at all. Exactly one blank line follows the
    section, as before."""
    if not ordered_ids:
        for ns in fallback_namespaces:
            f.write(f"namespace {ns} {{\n")
        for ns in reversed(fallback_namespaces):
            f.write(f"}}  // namespace {ns}\n")
        f.write("\n")
        return
    idx = 0
    while idx < len(ordered_ids):
        namespace = ordered_ids[idx].namespace
        run = [ordered_ids[idx]]
        idx += 1
        while idx < len(ordered_ids) and ordered_ids[idx].namespace == namespace:
            run.append(ordered_ids[idx])
            idx += 1
        parts = namespace.split(".") if namespace else []
        for part in parts:
            f.write(f"namespace {part} {{\n")
        for type_id in run:
            f.write(f"struct {type_id.name};\n")
        for part in reversed(parts):
            f.write(f"}}  // namespace {part}\n")
    f.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, help="root .fbs file")
    parser.add_argument("--out", required=True, help="output rust file")
    parser.add_argument(
        "--lang",
        choices=["rust", "cpp", "python"],
        default="rust",
        help="output language (default: rust)",
    )
    parser.add_argument(
        "--bfbs",
        required=True,
        help="compiled bfbs file path (all metadata is computed from it via "
        "schema_reflection)",
    )
    args = parser.parse_args()

    # NOTE: `--root` is used ONLY for the output file's basename-derived
    # identifiers — its TEXT is never read (all metadata comes from
    # `--bfbs`).
    root_path = args.root

    with open(args.bfbs, "rb") as f:
        bfbs = f.read()
    loaded = schema_reflection.load_bfbs(bfbs)

    root_id = loaded.root_type
    fixed_size = reflection_fixed_size(loaded)

    if args.lang == "rust":
        with open(args.out, "w", encoding="utf-8") as f:
            if fixed_size is None:
                f.write("pub const AORTA_FIXED_SIZE: Option<usize> = None;\n")
            else:
                f.write(
                    f"pub const AORTA_FIXED_SIZE: Option<usize> = Some({fixed_size});\n"
                )
        return 0

    bfbs_hash = hashlib.sha256(bfbs).hexdigest()

    base_name = os.path.basename(root_path)
    stem = os.path.splitext(base_name)[0]
    ident = re.sub(r"[^A-Za-z0-9_]", "_", stem).upper()

    # An enums-only schema (e.g. a shared types schema included by several
    # message/action schemas) has no root_type and no message table to
    # specialize SchemaMeta for. Python meta re-exports the root message class so
    # it still requires a root; C++ meta can emit the bfbs blob with no
    # per-type specializations.
    if root_id is None and args.lang == "python":
        print("root_type missing in schema", file=sys.stderr)
        return 2

    local_type_ids = reflection_local_type_ids(loaded)

    if args.lang == "python":
        dotted_root = root_id.qualified_name
        root_type = root_id.name
        root_has_header = reflection_has_aorta_header(loaded, root_id)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write('"""Generated Aorta schema metadata — DO NOT EDIT.\n\n')
            f.write(f"Source schema: {base_name}\n")
            f.write("Produced by message/tools/gen_schema_meta.py --lang=python.\n")
            f.write("SCHEMA_BFBS is byte-identical to the cpp/rust meta (same\n")
            f.write("`:stem_bfbs` artifact), so SCHEMA_HASH_HEX matches across languages.\n")
            f.write('"""\n\n')
            f.write("from __future__ import annotations\n\n")
            f.write("from typing import Optional\n\n")
            f.write(f"SCHEMA_HASH_HEX: str = {bfbs_hash!r}\n")
            f.write(f"ROOT_TYPE_NAME: str = {dotted_root!r}\n")
            if fixed_size is None:
                f.write("FIXED_SIZE: Optional[int] = None\n")
            else:
                f.write(f"FIXED_SIZE: Optional[int] = {fixed_size}\n")
            f.write(f"HAS_AORTA_HEADER: bool = {root_has_header}\n\n")
            # Emit the exact .bfbs bytes via bytes.fromhex; adjacent string
            # literals concatenate, so this round-trips byte-for-byte.
            hexstr = bfbs.hex()
            f.write("SCHEMA_BFBS: bytes = bytes.fromhex(\n")
            for i in range(0, len(hexstr), 76):
                f.write(f"    {hexstr[i:i + 76]!r}\n")
            f.write(")\n")
            # Re-export the generated message class + builders so a SINGLE import
            # gives both the message definition AND its schema metadata — e.g.
            # `import demo_chat_schema_meta as chat` -> chat.Chat (class),
            # chat.ChatStart (builder), chat.SCHEMA_BFBS (meta). The Bazel
            # `<stem>_schema_meta_py_lib` deps on `<stem>_py_lib`, so one dep + one
            # import is all a consumer needs. Guarded so the metadata still loads
            # standalone (message package not on sys.path) — e.g. metadata-only use.
            f.write("\ntry:\n")
            f.write(f"    from {dotted_root} import *  # noqa: F401,F403,E402\n")
            f.write(f"    ROOT_TYPE = {root_type}  # the generated root message class\n")
            f.write("except ImportError:  # message classes absent; metadata still usable\n")
            f.write("    ROOT_TYPE = None\n")
        return 0

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("#pragma once\n\n")
        f.write("#include <cstddef>\n")
        f.write("#include <cstdint>\n")
        f.write("#include <optional>\n\n")
        f.write("namespace aorta {\n")
        f.write("template <typename T>\n")
        f.write("struct SchemaMeta;\n")
        f.write("}  // namespace aorta\n\n")

        fallback_namespaces = (
            reflection_rootless_namespaces(loaded) if not local_type_ids else []
        )
        write_cpp_forward_decls(f, local_type_ids, fallback_namespaces)

        f.write("namespace aorta {\n")
        f.write("namespace schema_meta_internal {\n")
        f.write(f"inline constexpr std::uint8_t {ident}_BFBS[] = {{\n")
        for i, byte in enumerate(bfbs):
            if i % 12 == 0:
                f.write("    ")
            f.write(f"0x{byte:02x}")
            if i + 1 != len(bfbs):
                f.write(", ")
            if (i + 1) % 12 == 0 or i + 1 == len(bfbs):
                f.write("\n")
        f.write("};\n")
        f.write(
            f"inline constexpr std::size_t {ident}_BFBS_SIZE = sizeof({ident}_BFBS);\n"
        )
        if fixed_size is None:
            f.write(
                f"inline constexpr std::optional<std::size_t> {ident}_FIXED_SIZE = std::nullopt;\n"
            )
        else:
            f.write(
                f"inline constexpr std::optional<std::size_t> {ident}_FIXED_SIZE = {fixed_size};\n"
            )
        f.write(f'inline constexpr char {ident}_HASH_HEX[] = "{bfbs_hash}";\n')
        f.write("}  // namespace schema_meta_internal\n\n")

        for type_id in local_type_ids:
            type_namespaces = type_id.namespace.split(".") if type_id.namespace else []
            fq_type = "::".join(type_namespaces + [type_id.name])
            dotted_type = type_id.qualified_name
            type_has_aorta_header = reflection_has_aorta_header(loaded, type_id)
            fixed_size_expr = (
                f"schema_meta_internal::{ident}_FIXED_SIZE"
                if root_id is not None
                and type_id.qualified_name == root_id.qualified_name
                else "std::nullopt"
            )

            f.write("template <>\n")
            f.write(f"struct SchemaMeta<{fq_type}> {{\n")
            f.write("  static const std::uint8_t* SchemaBfbs() {\n")
            f.write(f"    return schema_meta_internal::{ident}_BFBS;\n")
            f.write("  }\n")
            f.write("  static std::size_t SchemaBfbsSize() {\n")
            f.write(f"    return schema_meta_internal::{ident}_BFBS_SIZE;\n")
            f.write("  }\n")
            f.write("  static std::optional<std::size_t> FixedSize() {\n")
            f.write(f"    return {fixed_size_expr};\n")
            f.write("  }\n")
            f.write("  static std::optional<std::size_t> ShmCapacityHint() {\n")
            f.write(f"    return {fixed_size_expr};\n")
            f.write("  }\n")
            f.write("  static const char* SchemaHashHex() {\n")
            f.write(f"    return schema_meta_internal::{ident}_HASH_HEX;\n")
            f.write("  }\n")
            f.write("  static const char* RootTypeName() {\n")
            f.write(f'    return "{dotted_type}";\n')
            f.write("  }\n")
            f.write("  static constexpr bool HasAortaHeader() {\n")
            f.write(f"    return {'true' if type_has_aorta_header else 'false'};\n")
            f.write("  }\n")
            f.write("};\n\n")
        f.write("}  // namespace aorta\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
