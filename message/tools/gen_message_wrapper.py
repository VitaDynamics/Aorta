#!/usr/bin/env python3
"""Generate Rust/C++ message wrapper sources for Aorta message bindings."""

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import sys

TOOL_DIR = Path(__file__).resolve().parent
if str(TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(TOOL_DIR))

from schema_include_utils import (
    flatc_rust_namespace_path,
    flatc_rust_namespace_segment,
    parse_schema_includes,
    resolve_schema_include_path,
)

# The declared transitive `.fbs` include closure for the
# CURRENT invocation's root schema, populated once from `--include-fbs` in
# `main()`. `resolve_include_fbs_path` below consults it before falling
# back to directory-guessing — required for Tier-2 external Bzlmod
# consumers, whose includes may resolve into a DIFFERENT module's sandboxed
# tree (e.g. `external/aorta~/message/schemas/...`) that no path heuristic
# can discover from a schema living entirely outside any `message/` tree
# (see `schema_include_utils.resolve_schema_include_path`'s docstring).
# A plain module-level list (not threaded through the half-dozen functions
# between here and `_walk_include_fbs_closure`'s callers) is a deliberate,
# minimal-footprint choice: this file is a single-purpose CLI that resolves
# includes for exactly ONE root schema per process invocation — there is no
# reentrancy or multi-schema concern a global could get wrong.
_INCLUDE_MANIFEST: list[Path] = []

CPP_SCALAR_TYPES = {
    "bool": "bool",
    "byte": "std::int8_t",
    "int8": "std::int8_t",
    "ubyte": "std::uint8_t",
    "uint8": "std::uint8_t",
    "short": "std::int16_t",
    "int16": "std::int16_t",
    "ushort": "std::uint16_t",
    "uint16": "std::uint16_t",
    "int": "std::int32_t",
    "int32": "std::int32_t",
    "uint": "std::uint32_t",
    "uint32": "std::uint32_t",
    "long": "std::int64_t",
    "int64": "std::int64_t",
    "ulong": "std::uint64_t",
    "uint64": "std::uint64_t",
    "float": "float",
    "float32": "float",
    "double": "double",
    "float64": "double",
}

RUST_SCALAR_TYPES = {
    "bool": "bool",
    "byte": "i8",
    "int8": "i8",
    "ubyte": "u8",
    "uint8": "u8",
    "short": "i16",
    "int16": "i16",
    "ushort": "u16",
    "uint16": "u16",
    "int": "i32",
    "int32": "i32",
    "uint": "u32",
    "uint32": "u32",
    "long": "i64",
    "int64": "i64",
    "ulong": "u64",
    "uint64": "u64",
    "float": "f32",
    "float32": "f32",
    "double": "f64",
    "float64": "f64",
}

RUST_BUILDER_SUFFIX_FIELDS = {
    "i8",
    "u8",
    "i16",
    "u16",
    "i32",
    "u32",
    "i64",
    "u64",
    "f32",
    "f64",
}

RUST_RESERVED_WORDS = {
    "as",
    "break",
    "const",
    "continue",
    "crate",
    "else",
    "enum",
    "extern",
    "false",
    "fn",
    "for",
    "if",
    "impl",
    "in",
    "let",
    "loop",
    "match",
    "mod",
    "move",
    "mut",
    "pub",
    "ref",
    "return",
    "self",
    "Self",
    "static",
    "struct",
    "super",
    "trait",
    "true",
    "type",
    "unsafe",
    "use",
    "where",
    "while",
    "async",
    "await",
    "dyn",
}


class GenerationError(RuntimeError):
    pass


@dataclass
class SchemaDoc:
    path: Path
    stem: str
    namespace_full: str
    include_paths: list[str]
    local_type_names: list[str]
    struct_names: set[str]
    union_defs: dict[str, list[tuple[str, str]]]  # union_name -> [(alias, target), ...]
    enum_bases: dict[str, str]
    table_fields: dict[str, list[dict[str, str | None]]]


@dataclass
class TableDef:
    gen_mod: str
    namespace_full: str
    fields: list[dict[str, str | None]]


def parse_includes(root_fbs: str) -> list[str]:
    return parse_schema_includes(root_fbs)


def resolve_include_fbs_path(root_fbs: str, include_path: str) -> Path:
    return resolve_schema_include_path(root_fbs, include_path, manifest=_INCLUDE_MANIFEST)


def message_root_path(root_fbs: str, schema_rel: str) -> Path:
    path = Path(root_fbs)
    for ancestor in path.parents:
        candidate = ancestor / "message"
        if (candidate / schema_rel).exists():
            return candidate
    message_root = path.parent
    for _ in Path(schema_rel).parts:
        message_root = message_root.parent
    return message_root


def schema_path_relative_to_message_root(schema_path: Path, message_root: Path) -> Path:
    try:
        return schema_path.relative_to(message_root)
    except ValueError:
        pass

    try:
        return schema_path.resolve().relative_to(message_root.resolve())
    except (FileNotFoundError, ValueError):
        pass

    parts = schema_path.parts
    if "message" in parts:
        index = len(parts) - 1 - parts[::-1].index("message")
        if index + 1 < len(parts):
            return Path(*parts[index + 1 :])

    raise GenerationError(
        "schema path '{}' is not under message root '{}'".format(
            schema_path,
            message_root,
        ),
    )


def _walk_include_fbs_closure(root_fbs: str) -> list[Path]:
    """Depth-first walk of the `include` graph rooted at `root_fbs`, returning
    every INCLUDED schema's path (the root itself excluded), deduplicated by
    resolved filesystem path. This is the one remaining filesystem-derived
    fact under the loader path (see `collect_include_generated_modules`'s
    docstring): the bfbs cannot supply a consumer's on-disk generated-module
    layout, so the include graph itself is still walked from the `.fbs`
    source text (`parse_includes`/`resolve_include_fbs_path`), same as every
    other include-resolution site in this file — it does not need a
    schema's semantic content (tables/unions/enums/...), only its existence
    and its own further includes, so no `SchemaDoc` construction is needed
    here."""
    visited: set[Path] = set()
    included: list[Path] = []

    def visit(schema_path: Path) -> None:
        resolved = schema_path.resolve()
        if resolved in visited:
            return
        visited.add(resolved)

        for include_path in parse_includes(str(schema_path)):
            include_file = resolve_include_fbs_path(str(schema_path), include_path)
            if include_file.exists():
                visit(include_file)

        if resolved != Path(root_fbs).resolve():
            included.append(schema_path)

    visit(Path(root_fbs))
    return included


def collect_include_generated_modules(
    root_fbs: str,
    schema_rel: str,
) -> list[tuple[str, str]]:
    """Collect (module name, generated .rs path) pairs for every included schema.

    The returned list — and
    therefore the Rust `#[path = ...] pub mod ...;` block order emitted by
    `write_rust` — is sorted by the normalized declaration path
    (`generated_rel`), not by include-DFS encounter order. This is the only
    "generated modules" ordering site: the C++ wrapper does not itself
    enumerate per-include headers (it `#include`s only its own
    `<stem>_generated.h`, whose transitive include chain is entirely
    flatc-owned and outside this tool's control), so no analogous C++ change
    applies here.

    This is the ONE fact that stays
    text/filesystem-derived even though `--root-bfbs` is otherwise the sole
    semantic source (see `_walk_include_fbs_closure`'s docstring for why).
    """
    message_root = message_root_path(root_fbs, schema_rel)
    modules: list[tuple[str, str]] = []
    seen_mods: dict[str, str] = {}
    for include_path in _walk_include_fbs_closure(root_fbs):
        stem = include_path.stem
        gen_mod = f"{stem}_generated"
        generated_rel = (
            schema_path_relative_to_message_root(include_path, message_root)
            .with_name(f"{stem}_generated.rs")
            .as_posix()
        )
        previous = seen_mods.get(gen_mod)
        if previous is not None and previous != generated_rel:
            raise GenerationError(
                "duplicate include module name '{}' resolves to '{}' and '{}'".format(
                    gen_mod,
                    previous,
                    generated_rel,
                ),
            )
        if previous is None and stem != "aorta_base":
            seen_mods[gen_mod] = generated_rel
            modules.append((gen_mod, generated_rel))
    modules.sort(key=lambda entry: entry[1])
    return modules


# ---------------------------------------------------------------------------
# The bfbs loader (`schema_reflection.py`) is now
# the ONLY semantic source for the wrapper emitters — `--root-bfbs` is
# REQUIRED (`main()` hard-errors without it). The regex/text `collect_*`/
# `parse_*` scanners (table/union/enum/struct/root/
# namespace/include-type facts) and the `--shadow-check` cross-check
# machinery that compared them against the loader are gone: there is only
# one path left, so there is nothing left to shadow.
#
# Only ONE fact remains text/filesystem-derived: `collect_include_generated_
# modules` (the Rust `#[path = ...] pub mod ...;` block, via
# `_walk_include_fbs_closure` above). Its generated-`.rs` path is the include
# schema's location RELATIVE TO THE MESSAGE ROOT, which the bfbs's
# `declaration_file` string cannot supply — that string is relativized
# against the ROOT schema's directory (with `../` chains in-repo, and a
# symlink-resolved absolute path in an external Bzlmod consumer; see
# `schema_reflection.is_root_local`'s docstring for why its SHAPE is unsound
# to parse). The bfbs is authoritative for WHICH types exist and their
# kinds/namespaces, but not for a consumer's on-disk generated-module layout,
# so that one path computation stays on the filesystem walk. The include
# MODULE NAME (`<stem>_generated`) IS recovered from the loader, via the
# declaration_file's trailing basename — the last path segment is always
# `<stem>.fbs` regardless of the prefix's shape (verified empirically
# in-repo and external), and it matches the text walk's
# `Path(include_file).stem` exactly.
# ---------------------------------------------------------------------------

# Always set by `main()` from the required `--root-bfbs`. A plain
# module-level handle, mirroring `_INCLUDE_MANIFEST`'s single-invocation-CLI
# rationale (one root schema per process; no reentrancy).
_LOADED_SCHEMA = None


def _reflection():
    """Lazy accessor for the reflection loader module. Imported ONLY here
    (not at module level) so that a caller which imports this file as a
    plain Python module without ever calling `main()` — e.g. a unit test
    exercising a single pure helper — is not forced to have the build-time
    reflection Python bindings (`//message/tools:reflection_py_lib`) on
    `sys.path` just to import this file. Every caller below runs only when
    `_LOADED_SCHEMA` is set, which `main()` sets from the SAME import
    (required, since `--root-bfbs` is mandatory), so the bindings are
    guaranteed present here."""
    import schema_reflection

    return schema_reflection


def _loaded_stem_from_decl(declaration_file: str) -> str:
    """The schema stem for a loaded type, from its bfbs `declaration_file`'s
    trailing basename (`.../<stem>.fbs` -> `<stem>`). Only the LAST path
    segment is consulted — the prefix's shape is context-dependent and
    unsound to parse (see the section comment above), but the filename at the
    end is always the schema's own `<stem>.fbs`."""
    return Path(declaration_file).stem


def _shape_to_source_type(shape: str, target) -> str:
    """Reconstruct an emitter-facing `.fbs`-style field type string from the
    loader's canonical `shape` + referenced `TypeId` (`target`). The emitters
    classify field types through `is_scalar`/`is_string`/`split_vector_type`/
    `local_type_base` and `qualify_{cpp,rust}_user_type`, all of which key off
    the bare type name and vector bracketing; a named type is reconstructed as
    its FULLY-QUALIFIED name (`namespace.name`, or bare name when the
    namespace is empty). Fully-qualified is output-identical to whatever
    spelling the source used: `qualify_*_user_type` routes an included type
    through the include map (same result qualified or bare) and a root-local
    type through the shared `namespace_full`/`gen_mod` fallback (same result
    too, since a root-local type's namespace IS `namespace_full`). Scalars
    reconstruct to the canonical short keyword, which the emitters' scalar
    tables (`CPP_SCALAR_TYPES`/`RUST_SCALAR_TYPES`) map identically to the
    long form."""
    if shape.startswith("vector<") and shape.endswith(">"):
        return "[" + _shape_to_source_type(shape[len("vector<"):-1], target) + "]"
    if shape.startswith("array<") and shape.endswith(">"):
        inner, _, length = shape[len("array<"):-1].rpartition(",")
        return "[" + _shape_to_source_type(inner, target) + ":" + length + "]"
    if shape == "string":
        return "string"
    if shape.startswith("scalar:"):
        return shape[len("scalar:"):]
    # Named type (table:/struct:/enum:/union:<bare>): the qualified TypeId is
    # the emitter-facing reconstruction.
    if target is not None:
        return target.qualified_name
    raise GenerationError(
        "cannot reconstruct a field type from loader shape {!r} without a "
        "referenced type (this indicates an unmodeled shape)".format(shape),
    )


def _loaded_field_dict(field) -> dict[str, object]:
    """Project one loader `FieldDef` into the emitter's field dict shape
    (`{"name", "type", "default", "id", "optional"}`). `default` is unused by
    the emitters (verified: only `name`/`type`/`id`/`optional` are read) and
    left `None`; `type` is the reconstructed source-style string (see
    `_shape_to_source_type`). `optional` (the loader's `FieldDef.optional`
    -- a `= null` scalar/enum) is threaded through so the Rust owned-root clone
    can emit `root.x()` directly (the accessor already returns `Option<T>` for
    an optional scalar) instead of `Some(root.x())` (which double-wraps to
    `Option<Option<T>>` -- a probe-proven compile bomb)."""
    return {
        "name": field.name,
        "type": _shape_to_source_type(field.shape, field.target),
        "default": None,
        "id": field.id,
        "optional": field.optional,
        # `required` reference fields (string/table/vector) have a
        # flatc accessor that returns the value DIRECTLY (not `Option<…>`), so
        # the Rust owned-root clone must emit `Some(root.x()…)`, not the optional
        # `.map(...)` form (an E0599 compile bomb). Threaded here alongside
        # `optional`, which was the same class of dropped-modifier defect.
        "required": field.required,
    }


def collect_table_defs_from_loaded(loaded, root_fbs: str) -> dict[str, "TableDef"]:
    """Loader-backed `collect_table_defs`: every table (root + include closure)
    keyed by bare name in canonical `(namespace, name)` order. Mirrors the
    text version's `gen_mod` derivation (`<root_stem>_generated` for root-local
    tables, `<include_stem>_generated` otherwise) and its same-bare-name
    hard-error policy (`GenerationError`). Deprecated fields are dropped
    (flatc emits no accessors/builders for them), matching `parse_table_fields`."""
    is_root_local = _reflection().is_root_local
    root_stem = Path(root_fbs).stem
    entries: dict[str, TableDef] = {}
    for obj in loaded.tables.values():
        type_id = obj.type_id
        bare = type_id.name
        if is_root_local(type_id, loaded):
            gen_mod = "{}_generated".format(root_stem)
        else:
            gen_mod = "{}_generated".format(_loaded_stem_from_decl(type_id.declaration_file))
        existing = entries.get(bare)
        if existing is not None:
            if existing.gen_mod != gen_mod or existing.namespace_full != type_id.namespace:
                raise GenerationError(
                    "duplicate table name '{}' found in multiple schemas; wrapper generation requires unique table names across include closure".format(
                        bare,
                    ),
                )
            continue
        entries[bare] = TableDef(
            gen_mod=gen_mod,
            namespace_full=type_id.namespace,
            fields=[_loaded_field_dict(f) for f in obj.fields if not f.deprecated],
        )
    return {
        name: entries[name]
        for name in sorted(entries, key=lambda n: (entries[n].namespace_full, n))
    }


def _loaded_named_defs(mapping):
    """Yield `(bare_name, namespace, entry)` for every value in one of the
    loader's qualified-name-keyed maps, hard-erroring on a same-bare-name
    collision across namespaces — the shared policy behind the loader-backed
    `collect_union_defs`/`collect_enum_bases`/`collect_struct_names`, matching
    each text collector's own dual (namespace/schema) hard-error."""
    seen: dict[str, str] = {}
    ordered: list = []
    for entry in mapping.values():
        type_id = entry.type_id
        bare = type_id.name
        prior_ns = seen.get(bare)
        if prior_ns is not None and prior_ns != type_id.namespace:
            raise GenerationError(
                "duplicate name '{}' found in namespaces '{}' and '{}'; wrapper "
                "generation requires unique names across the include closure — "
                "same-bare-name-across-namespaces is unsupported".format(
                    bare, prior_ns, type_id.namespace,
                ),
            )
        if prior_ns is None:
            seen[bare] = type_id.namespace
            ordered.append((bare, type_id.namespace, entry))
    return ordered


def collect_union_defs_from_loaded(loaded) -> dict[str, list[tuple[str, str]]]:
    """Loader-backed `collect_union_defs`: each union's `(alias, target_bare)`
    variant list, keyed by bare name in canonical `(namespace, name)` order.
    Variant order is the loader's `(value, alias)` sort, matching the text
    parser's flatc-discriminant replay. The target is the BARE type name
    (`local_type_base` in the text parser)."""
    ordered = _loaded_named_defs(loaded.unions)
    result = {
        bare: [(v.alias, v.target.name) for v in union.variants]
        for bare, _ns, union in ordered
    }
    return {
        name: result[name]
        for name in sorted(result, key=lambda n: (_union_ns(ordered, n), n))
    }


def _union_ns(ordered, name):
    for bare, ns, _entry in ordered:
        if bare == name:
            return ns
    return ""


def collect_enum_bases_from_loaded(loaded) -> dict[str, str]:
    """Loader-backed `collect_enum_bases`: each enum's base type keyed by bare
    name in canonical `(namespace, name)` order. The base is the loader's
    canonical short keyword (e.g. `ubyte`); the text version stores the raw
    source spelling (e.g. `uint8`). Both forms are keys in
    `CPP_SCALAR_TYPES`/`RUST_SCALAR_TYPES` mapping to the same emitted type, so
    the value change is output-invariant — and the value is only read where an
    enum-vector field emits its underlying scalar (`CPP_SCALAR_TYPES[base]`),
    which is identical either way; elsewhere only the KEY set is consulted."""
    ordered = _loaded_named_defs(loaded.enums)
    ns_of = {bare: ns for bare, ns, _e in ordered}
    result = {bare: enum.base_type for bare, _ns, enum in ordered}
    return {name: result[name] for name in sorted(result, key=lambda n: (ns_of[n], n))}


def collect_struct_names_from_loaded(loaded) -> set[str]:
    """Loader-backed `collect_struct_names`: bare names of every struct in the
    closure, hard-erroring on a same-bare-name collision (matching the text
    version's cross-closure uniqueness requirement)."""
    return {bare for bare, _ns, _s in _loaded_named_defs(loaded.structs)}


def root_type_from_loaded(loaded) -> str | None:
    """Loader-backed `parse_root_type`: the BARE root type name (the emitters
    look the root type up in `all_table_defs`, which is bare-name keyed). This
    also fixes the qualified-`root_type` mis-parse the text scanner has (it
    would return the qualified spelling and miss the table)."""
    return loaded.root_type.name if loaded.root_type is not None else None


def root_type_qualified_from_loaded(loaded) -> str | None:
    """The root type's OWN fully-qualified (dotted) name -- its actual namespace,
    NOT `namespace_full.<root>`. These DIFFER when the `root_type` names a FOREIGN
    type in a namespace other than the local types': a schema with
    local `mrlocal.inner.Marker` and `root_type mrforeign.deep.ForeignRoot` has
    `namespace_full = "mrlocal.inner"` but a root FQN of
    `mrforeign.deep.ForeignRoot`. The schema-meta root-type name is wire-relevant
    (schema discovery / `SchemaMeta::root_type`), so it MUST be the real FQN."""
    return loaded.root_type.qualified_name if loaded.root_type is not None else None


def namespace_full_from_loaded(loaded) -> str:
    """Loader-backed `parse_namespace_full` for the ROOT schema's namespace:
    the (unique) namespace of the root-locally-declared types. Hard-errors if
    the root file declares types across multiple namespaces (mid-file
    namespace change) — the emitters assume ONE root namespace; the loader is
    what makes such a schema visible instead of silently taking the first
    `namespace` statement. Falls back to the root type's namespace, then empty,
    when the root file declares no local types of its own."""
    is_root_local = _reflection().is_root_local
    namespaces: list[str] = []
    seen: set[str] = set()
    for mapping in (loaded.tables, loaded.structs, loaded.enums, loaded.unions):
        for entry in mapping.values():
            type_id = entry.type_id
            if is_root_local(type_id, loaded) and type_id.namespace not in seen:
                seen.add(type_id.namespace)
                namespaces.append(type_id.namespace)
    if len(namespaces) > 1:
        raise GenerationError(
            "root schema declares types in multiple namespaces {} — a single "
            "root namespace is required (mid-file namespace changes are "
            "unsupported for the root wrapper)".format(
                sorted(namespaces),
            ),
        )
    if namespaces:
        return namespaces[0]
    if loaded.root_type is not None:
        return loaded.root_type.namespace
    return ""


def namespace_top_from_loaded(loaded) -> str:
    """Loader-backed `parse_namespace_top`: first segment of the root
    namespace, or `"schema"` when there is none."""
    namespace = namespace_full_from_loaded(loaded)
    if not namespace:
        return "schema"
    return namespace.split(".")[0]


def include_type_map_from_loaded(loaded) -> dict[str, list[tuple[str, str, str]]]:
    """Loader-backed `parse_include_type_map`: bare type name -> list of
    `(include_module, include_namespace, kind)` for every INCLUDE (non-root-
    local) type. `include_module` is `<stem>_generated` from the type's bfbs
    declaration_file basename; `kind` is the loader `TypeId.kind`. Like the
    text version, this does NOT hard-error on a bare name appearing in two
    includes — it accumulates entries, and `qualify_*`/`_include_type_kind`
    only act on the unambiguous single-entry case."""
    is_root_local = _reflection().is_root_local
    mapping: dict[str, list[tuple[str, str, str]]] = {}
    for kind_map in (loaded.tables, loaded.structs, loaded.enums, loaded.unions):
        for entry in kind_map.values():
            type_id = entry.type_id
            if is_root_local(type_id, loaded):
                continue
            module = "{}_generated".format(_loaded_stem_from_decl(type_id.declaration_file))
            record = (module, type_id.namespace, type_id.kind)
            bucket = mapping.setdefault(type_id.name, [])
            if record not in bucket:
                bucket.append(record)
    return mapping


def include_enum_bases_from_loaded(loaded) -> dict[str, str]:
    """Loader-backed `parse_include_enum_bases`: include enum bare name -> base
    keyword, dropping any name that resolves to conflicting bases across
    includes (matching the text version's conflict handling). Base spelling is
    canonical short; output-invariant for the same reason as
    `collect_enum_bases_from_loaded`."""
    is_root_local = _reflection().is_root_local
    enum_bases: dict[str, str] = {}
    conflicting: set[str] = set()
    for entry in loaded.enums.values():
        type_id = entry.type_id
        if is_root_local(type_id, loaded):
            continue
        name = type_id.name
        if name in conflicting:
            continue
        existing = enum_bases.get(name)
        if existing is None:
            enum_bases[name] = entry.base_type
        elif existing != entry.base_type:
            conflicting.add(name)
            del enum_bases[name]
    return enum_bases


def root_schema_doc_from_loaded(loaded, root_fbs: str) -> "SchemaDoc":
    """Loader-backed root `SchemaDoc`: the emitters read `include_paths`
    (kept text-derived — include statements drive the `#[path]` module walk),
    `namespace_full`, `local_type_names` (root-local table+struct names; the
    emitters re-sort this, so order is immaterial), `table_fields.keys()`
    (root-local table names), and `union_defs.keys()` (root-local union names).
    Every semantic field is projected from the loader restricted to
    root-local types; `include_paths` alone comes from the source text."""
    is_root_local = _reflection().is_root_local
    root_path = Path(root_fbs)
    table_fields: dict[str, list[dict[str, str | None]]] = {}
    struct_names: set[str] = set()
    local_type_names: list[str] = []
    for obj in loaded.tables.values():
        if is_root_local(obj.type_id, loaded):
            table_fields[obj.type_id.name] = [
                _loaded_field_dict(f) for f in obj.fields if not f.deprecated
            ]
            local_type_names.append(obj.type_id.name)
    for obj in loaded.structs.values():
        if is_root_local(obj.type_id, loaded):
            struct_names.add(obj.type_id.name)
            local_type_names.append(obj.type_id.name)
    union_defs: dict[str, list[tuple[str, str]]] = {}
    for union in loaded.unions.values():
        if is_root_local(union.type_id, loaded):
            union_defs[union.type_id.name] = [
                (v.alias, v.target.name) for v in union.variants
            ]
    enum_bases: dict[str, str] = {}
    for enum in loaded.enums.values():
        if is_root_local(enum.type_id, loaded):
            enum_bases[enum.type_id.name] = enum.base_type
    return SchemaDoc(
        path=root_path,
        stem=root_path.stem,
        namespace_full=namespace_full_from_loaded(loaded),
        include_paths=parse_includes(str(root_path)),
        local_type_names=local_type_names,
        struct_names=struct_names,
        union_defs=union_defs,
        enum_bases=enum_bases,
        table_fields=table_fields,
    )


# --- Emitter-facing dispatchers: thin wrappers over the loader-backed
# builders above. `write_rust`/`write_cpp` call these exclusively; `main()`
# guarantees `_LOADED_SCHEMA` is set (from the required `--root-bfbs`) before
# either is invoked. Kept as named functions (rather than inlining the
# `*_from_loaded` calls directly into the emitters) so `write_rust`/
# `write_cpp` stayed unchanged across the loader switch and the text-parser
# fallback removal alike — only this dispatch layer's body changed.

def emit_root_schema_doc(root_fbs: str) -> "SchemaDoc":
    return root_schema_doc_from_loaded(_LOADED_SCHEMA, root_fbs)


def emit_root_type(root_fbs: str) -> str | None:
    del root_fbs  # unused: the loaded schema alone determines the root type.
    return root_type_from_loaded(_LOADED_SCHEMA)


def emit_root_type_qualified(root_fbs: str) -> str | None:
    del root_fbs  # unused: the loaded schema alone determines the root type.
    return root_type_qualified_from_loaded(_LOADED_SCHEMA)


def emit_namespace_top(root_fbs: str) -> str:
    del root_fbs
    return namespace_top_from_loaded(_LOADED_SCHEMA)


def emit_table_defs(root_fbs: str) -> dict[str, "TableDef"]:
    return collect_table_defs_from_loaded(_LOADED_SCHEMA, root_fbs)


def emit_union_defs(root_fbs: str) -> dict[str, list[tuple[str, str]]]:
    del root_fbs
    return collect_union_defs_from_loaded(_LOADED_SCHEMA)


def emit_enum_bases(root_fbs: str) -> dict[str, str]:
    del root_fbs
    return collect_enum_bases_from_loaded(_LOADED_SCHEMA)


def emit_struct_names(root_fbs: str) -> set[str]:
    del root_fbs
    return collect_struct_names_from_loaded(_LOADED_SCHEMA)


def emit_include_type_map(
    root_fbs: str, include_paths: list[str]
) -> dict[str, list[tuple[str, str, str]]]:
    del root_fbs, include_paths  # unused: loader-derived facts need neither.
    return include_type_map_from_loaded(_LOADED_SCHEMA)


def emit_include_enum_bases(root_fbs: str, include_paths: list[str]) -> dict[str, str]:
    del root_fbs, include_paths
    return include_enum_bases_from_loaded(_LOADED_SCHEMA)


def sanitize_rust_ident(value: str) -> str:
    chars = []
    for idx, ch in enumerate(value):
        if ch.isascii() and (ch.isalnum() or ch == "_"):
            if idx == 0 and ch.isdigit():
                chars.append("_")
            chars.append(ch)
        else:
            chars.append("_")
    ident = "".join(chars).strip("_")
    if not ident:
        return "SchemaType"
    if ident[0].isdigit():
        ident = "_" + ident
    return ident


def camel_to_snake(value: str) -> str:
    out: list[str] = []
    for idx, ch in enumerate(value):
        if ch.isupper():
            if idx > 0 and (not value[idx - 1].isupper() or (idx + 1 < len(value) and value[idx + 1].islower())):
                out.append("_")
            out.append(ch.lower())
        else:
            out.append(ch)
    return "".join(out)


def const_prefix_from_stem(stem: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", stem).upper()


def cpp_guard_macro(category: str, namespace_full: str, symbol_name: str) -> str:
    parts = ["AORTA_TYPED_WRAPPER", category]
    if namespace_full:
        parts.extend(namespace_full.split("."))
    parts.append(symbol_name)
    parts.append("DEFINED")
    return "_".join(
        re.sub(r"[^A-Za-z0-9_]", "_", part).upper() for part in parts if part
    )


def cpp_detail_namespace(stem: str, namespace_full: str) -> str:
    parts: list[str] = []
    if namespace_full:
        parts.extend(namespace_full.split("."))
    parts.append(stem)
    return "typed_" + sanitize_rust_ident("_".join(parts))


def split_vector_type(field_type: str) -> str | None:
    if field_type.startswith("[") and field_type.endswith("]"):
        return field_type[1:-1].strip()
    return None


def local_type_base(field_type: str) -> str:
    return field_type.split(".")[-1]


def rust_builder_field_name(field_name: str) -> str:
    if field_name in RUST_BUILDER_SUFFIX_FIELDS or field_name in RUST_RESERVED_WORDS:
        return field_name + "_"
    return field_name


def rust_msg_field_name(field_name: str) -> str:
    if field_name in RUST_RESERVED_WORDS:
        return f"r#{field_name}"
    return field_name


def is_scalar(field_type: str) -> bool:
    return local_type_base(field_type) in CPP_SCALAR_TYPES


def is_string(field_type: str) -> bool:
    return field_type == "string"


def is_header_field(field: dict[str, str | None]) -> bool:
    field_name = str(field["name"])
    field_type = str(field["type"])
    return field_name == "aorta_header" and local_type_base(field_type) == "AortaHeader"


def derive_action_binding_names(
    schema_rel: str,
    stem: str,
    local_table_names: set[str],
    action_name_override: str | None = None,
) -> dict[str, str] | None:
    normalized_rel = schema_rel.strip("/")
    action_prefix = "schemas/action/"
    # An EXPLICIT but empty `--action-name` (`""`, distinct from an ABSENT
    # `None`) is never a valid routing name. main() rejects it up front, but the
    # invariant is enforced here too so ANY caller (including the matrix wiring
    # test) fails closed rather than emitting an action bound to the empty name.
    if action_name_override is not None and action_name_override == "":
        raise GenerationError(
            "--action-name was supplied but empty for schema stem '{}'; an "
            "explicit action routing name must be a non-empty string".format(stem)
        )
    # Tier-1 (in-repo) action schemas live under schemas/action/ and derive their
    # routing name from that path. Tier-2 (local, e.g. aorta_action_library)
    # schemas have no such path, so they pass an explicit action_name_override,
    # which both forces emission and supplies the routing name.
    if action_name_override is None and not normalized_rel.startswith(action_prefix):
        return None

    # Type-support matrix hard-fail (an --action-name override with
    # non-conforming tables -> GenerationError): when the caller EXPLICITLY
    # passes --action-name, it is asserting "this schema IS an action named X".
    # Silently returning None (emitting NO action aggregate) then
    # hides a genuine authoring error behind a wrapper that compiles but has no
    # ActionWire/ActionSchema impl. Fail closed and name the schema + reason.
    # Path-inferred callers (override is None, under schemas/action/) stay
    # lenient: those schemas are gen_action_wrapper-expanded and always conform,
    # so a None there is not an authoring error to surface.
    def _reject(reason: str) -> None:
        if action_name_override is not None:
            raise GenerationError(
                "--action-name '{}' was supplied for schema stem '{}', but its "
                "tables do not form a valid action protocol: {}. An action schema "
                "must declare exactly one '<Prefix>Goal', '<Prefix>Result' and "
                "'<Prefix>FeedbackData' sharing one prefix, expanded (via "
                "gen_action_wrapper) into the full 8-table protocol.".format(
                    action_name_override, stem, reason,
                )
            )
        return None

    goal_names = sorted(
        name
        for name in local_table_names
        if name.endswith("Goal") and not name.endswith("SendGoal")
    )
    result_names = sorted(
        name
        for name in local_table_names
        if name.endswith("Result") and not name.endswith("GetResult")
    )
    feedback_names = sorted(name for name in local_table_names if name.endswith("FeedbackData"))

    if len(goal_names) != 1 or len(result_names) != 1 or len(feedback_names) != 1:
        return _reject(
            "expected exactly one each of *Goal / *Result / *FeedbackData, found "
            "goals={}, results={}, feedbacks={}".format(
                goal_names, result_names, feedback_names,
            )
        )

    goal_name = goal_names[0]
    result_name = result_names[0]
    feedback_name = feedback_names[0]
    action_type_stem = goal_name[: -len("Goal")]
    if not action_type_stem:
        return _reject("the '*Goal' table has an empty action prefix")

    if result_name[: -len("Result")] != action_type_stem:
        return _reject(
            "prefix mismatch: goal '{}' implies prefix '{}', but result is '{}'".format(
                goal_name, action_type_stem, result_name,
            )
        )
    if feedback_name[: -len("FeedbackData")] != action_type_stem:
        return _reject(
            "prefix mismatch: goal '{}' implies prefix '{}', but feedback is '{}'".format(
                goal_name, action_type_stem, feedback_name,
            )
        )

    if action_name_override is not None:
        action_name = action_name_override
    else:
        action_path = normalized_rel[len(action_prefix):].strip("/")
        action_name = f"{action_path}/{stem}" if action_path else stem
    wrapper_table_names = {
        f"{action_type_stem}SendGoalRequest",
        f"{action_type_stem}SendGoalResponse",
        f"{action_type_stem}CancelGoalRequest",
        f"{action_type_stem}CancelGoalResponse",
        f"{action_type_stem}Feedback",
        f"{action_type_stem}PublicStatus",
        f"{action_type_stem}GetResultRequest",
        f"{action_type_stem}GetResultResponse",
    }
    if not wrapper_table_names.issubset(local_table_names):
        return _reject(
            "the expanded 8-table protocol is incomplete; missing {}".format(
                sorted(wrapper_table_names - set(local_table_names))
            )
        )

    return {
        "action_marker": f"{sanitize_rust_ident(action_type_stem)}Action",
        "action_name": action_name,
        "goal": goal_name,
        "result": result_name,
        "feedback_data": feedback_name,
        "send_goal_request": f"{action_type_stem}SendGoalRequest",
        "send_goal_response": f"{action_type_stem}SendGoalResponse",
        "cancel_goal_request": f"{action_type_stem}CancelGoalRequest",
        "cancel_goal_response": f"{action_type_stem}CancelGoalResponse",
        "feedback": f"{action_type_stem}Feedback",
        "public_status": f"{action_type_stem}PublicStatus",
        "get_result_request": f"{action_type_stem}GetResultRequest",
        "get_result_response": f"{action_type_stem}GetResultResponse",
    }


def _include_type_kind(
    type_base: str,
    include_type_map: dict[str, list[tuple[str, str, str]]] | None,
) -> str | None:
    if include_type_map is None:
        return None
    include_entries = include_type_map.get(type_base)
    if include_entries and len(include_entries) == 1:
        return include_entries[0][2]
    return None


def qualify_cpp_user_type(
    field_type: str,
    namespace_full: str,
    include_type_map: dict[str, list[tuple[str, str, str]]] | None = None,
) -> str:
    if "." in field_type:
        return "::" + field_type.replace(".", "::")

    if include_type_map is not None:
        include_entries = include_type_map.get(field_type)
        if include_entries and len(include_entries) == 1:
            _, include_ns, _ = include_entries[0]
            if include_ns:
                return "::" + include_ns.replace(".", "::") + "::" + field_type

    if namespace_full:
        return "::" + namespace_full.replace(".", "::") + "::" + field_type
    return "::" + field_type


# flatc's Rust namespace naming (`flatc_rust_namespace_path` = per-segment
# keyword-escape + snake_case) lives in schema_include_utils as the single source
# of truth shared with patch_flatc_rust's absolute foreign-reference rewrite
# — see its docstring. Imported above.


def qualify_rust_user_type(
    field_type: str,
    namespace_full: str,
    gen_mod: str,
    include_type_map: dict[str, list[tuple[str, str, str]]] | None = None,
) -> str:
    if "." in field_type:
        namespace_part, type_name = field_type.rsplit(".", 1)
        if include_type_map is not None:
            include_entries = include_type_map.get(type_name)
            if include_entries and len(include_entries) == 1:
                include_mod, include_ns, _ = include_entries[0]
                if include_ns == namespace_part:
                    if include_ns:
                        return (
                            f"crate::{include_mod}::{flatc_rust_namespace_path(include_ns)}::{type_name}"
                        )
                    return f"crate::{include_mod}::{type_name}"
        return f"crate::{gen_mod}::{flatc_rust_namespace_path(namespace_part)}::{type_name}"

    if include_type_map is not None:
        include_entries = include_type_map.get(field_type)
        if include_entries and len(include_entries) == 1:
            include_mod, include_ns, _ = include_entries[0]
            if include_ns:
                return f"crate::{include_mod}::{flatc_rust_namespace_path(include_ns)}::{field_type}"
            return f"crate::{include_mod}::{field_type}"

    if namespace_full:
        return f"crate::{gen_mod}::{flatc_rust_namespace_path(namespace_full)}::{field_type}"
    return f"crate::{gen_mod}::{field_type}"


def cpp_msg_field_type(
    field_type: str,
    namespace_full: str,
    local_struct_names: set[str],
    local_table_names: set[str],
    local_union_names: set[str],
    local_enum_bases: dict[str, str],
    include_type_map: dict[str, list[tuple[str, str, str]]] | None = None,
) -> tuple[str, str]:
    vec_elem = split_vector_type(field_type)
    if vec_elem is not None:
        if is_string(vec_elem):
            return ("std::vector<std::string>", "vector_string")
        elem_base = local_type_base(vec_elem)
        if elem_base in CPP_SCALAR_TYPES:
            elem_type = CPP_SCALAR_TYPES[elem_base]
            return (f"std::vector<{elem_type}>", "vector")

        if elem_base in local_enum_bases:
            elem_type = qualify_cpp_user_type(
                vec_elem, namespace_full, include_type_map
            )
            return (f"std::vector<{elem_type}>", "vector_enum")

        if elem_base in local_struct_names:
            elem_type = qualify_cpp_user_type(
                vec_elem, namespace_full, include_type_map
            )
            return (f"std::vector<{elem_type}>", "vector_struct")

        if elem_base in local_union_names:
            return (f"std::vector<{elem_base}Msg>", "vector_union")

        if elem_base in local_table_names:
            return (f"std::vector<std::shared_ptr<{elem_base}Msg>>", "vector_table")

        include_kind = _include_type_kind(elem_base, include_type_map)
        if include_kind == "table":
            return (f"std::vector<std::shared_ptr<{elem_base}Msg>>", "vector_table")
        if include_kind == "union":
            return (f"std::vector<{elem_base}Msg>", "vector_union")
        if include_kind == "enum":
            elem_type = qualify_cpp_user_type(
                vec_elem, namespace_full, include_type_map
            )
            return (f"std::vector<{elem_type}>", "vector_enum")
        if include_kind == "struct":
            elem_type = qualify_cpp_user_type(
                vec_elem, namespace_full, include_type_map
            )
            return (f"std::vector<{elem_type}>", "vector_struct")

        elem_type = qualify_cpp_user_type(vec_elem, namespace_full, include_type_map)
        return (f"std::vector<{elem_type}>", "vector")

    base = local_type_base(field_type)
    if base in CPP_SCALAR_TYPES:
        return (CPP_SCALAR_TYPES[base], "scalar")
    if is_string(field_type):
        return ("std::string", "string")

    if base in local_union_names:
        return (f"{base}Msg", "union")

    if base in local_table_names:
        return (f"std::shared_ptr<{base}Msg>", "table")

    include_kind = _include_type_kind(base, include_type_map)
    if include_kind == "table":
        return (f"std::shared_ptr<{base}Msg>", "table")
    if include_kind == "union":
        return (f"{base}Msg", "union")
    if include_kind == "struct":
        return (
            qualify_cpp_user_type(field_type, namespace_full, include_type_map),
            "struct",
        )

    if base in local_struct_names:
        return (
            qualify_cpp_user_type(field_type, namespace_full, include_type_map),
            "struct",
        )

    return (qualify_cpp_user_type(field_type, namespace_full, include_type_map), "user")


def rust_msg_field_type(
    field_type: str,
    namespace_full: str,
    gen_mod: str,
    local_struct_names: set[str],
    local_table_names: set[str],
    local_union_names: set[str],
    local_enum_bases: dict[str, str],
    include_type_map: dict[str, list[tuple[str, str, str]]] | None = None,
) -> tuple[str, str]:
    vec_elem = split_vector_type(field_type)
    if vec_elem is not None:
        if is_string(vec_elem):
            return ("Vec<String>", "vector_string")
        elem_base = local_type_base(vec_elem)
        if elem_base in RUST_SCALAR_TYPES:
            elem_type = RUST_SCALAR_TYPES[elem_base]
            return (f"&'a [{elem_type}]", "vector_scalar")
        if elem_base in local_enum_bases:
            elem_type = qualify_rust_user_type(
                vec_elem,
                namespace_full,
                gen_mod,
                include_type_map,
            )
            return (f"Vec<{elem_type}>", "vector_enum")
        if elem_base in local_struct_names:
            elem_type = qualify_rust_user_type(
                vec_elem,
                namespace_full,
                gen_mod,
                include_type_map,
            )
            return (f"Vec<{elem_type}>", "vector")

        if elem_base in local_union_names:
            raise GenerationError(
                "vector<union> field '{}' is not supported by Rust flatc in this repository".format(
                    field_type,
                ),
            )

        if elem_base in local_table_names:
            return (f"Vec<{elem_base}Msg>", "vector_table")

        include_kind = _include_type_kind(elem_base, include_type_map)
        if include_kind == "table":
            return (f"Vec<{elem_base}Msg>", "vector_table")
        if include_kind == "union":
            raise GenerationError(
                "vector<union> field '{}' is not supported by Rust flatc in this repository".format(
                    field_type,
                ),
            )
        if include_kind == "enum":
            elem_type = qualify_rust_user_type(
                vec_elem,
                namespace_full,
                gen_mod,
                include_type_map,
            )
            return (f"Vec<{elem_type}>", "vector_enum")
        if include_kind == "struct":
            elem_type = qualify_rust_user_type(
                vec_elem,
                namespace_full,
                gen_mod,
                include_type_map,
            )
            return (f"Vec<{elem_type}>", "vector")
        raise GenerationError(
            f"unsupported Rust vector field type '{field_type}' for typed wrapper generation",
        )

    base = local_type_base(field_type)
    if base in RUST_SCALAR_TYPES:
        return (RUST_SCALAR_TYPES[base], "scalar")
    if base in local_enum_bases:
        return (
            qualify_rust_user_type(
                field_type,
                namespace_full,
                gen_mod,
                include_type_map,
            ),
            "enum",
        )
    if is_string(field_type):
        return ("String", "string")

    if base in local_union_names:
        return (f"{base}Msg", "union")

    if base in local_table_names:
        return (f"Box<{base}Msg>", "table")

    include_kind = _include_type_kind(base, include_type_map)
    if include_kind == "table":
        return (f"Box<{base}Msg>", "table")
    if include_kind == "union":
        return (f"{base}Msg", "union")
    if include_kind == "enum":
        return (
            qualify_rust_user_type(
                field_type,
                namespace_full,
                gen_mod,
                include_type_map,
            ),
            "enum",
        )
    if include_kind == "struct":
        return (
            qualify_rust_user_type(
                field_type,
                namespace_full,
                gen_mod,
                include_type_map,
            ),
            "struct",
        )

    if base in local_struct_names:
        return (
            qualify_rust_user_type(
                field_type,
                namespace_full,
                gen_mod,
                include_type_map,
            ),
            "struct",
        )
    raise GenerationError(
        f"unsupported Rust field type '{field_type}' for typed wrapper generation",
    )


# ===========================================================================
# Explicit type-support matrix.
#
# The generator carries an EXPLICIT declaration of which (field category ×
# language × emission path) combinations are supported. Every combination is
# either SUPPORTED (and exercised by a compiled golden schema — all_types.fbs,
# all_types_ext.fbs, all_types_cpp_vector_union.fbs, and the shapes_probe
# golden action, round-tripped in CI in both languages) or a hard
# `GenerationError` naming the schema, table, field, and shape (no silent
# emission of code that will not compile).
#
# Logical field categories (language-agnostic; a vector's element recurses one
# level — FlatBuffers disallows vector-of-vector):
FIELD_SUPPORT_CATEGORIES = (
    "scalar", "enum", "string", "struct", "table", "union",
    "vector_scalar", "vector_enum", "vector_string",
    "vector_struct", "vector_table", "vector_union",
)
#
# Emission paths (typed build, typed borrow, owned clone-from-msg, owned
# clone-from-root, C++ copy helper), each language-scoped:
#   ("rust", "typed_build")             build_<table>_typed  (Msg -> wire)
#   ("rust", "owned_clone_from_msg")    clone_<t>_owned_from_msg   (action)
#   ("rust", "owned_clone_from_root")   clone_<t>_owned_from_root  (action)
#   ("rust", "owned_borrow")            borrow_<t>_owned_as_typed  (action)
#   ("cpp",  "typed_build")             Build<Table>Typed    (Msg -> wire)
#   ("cpp",  "copy_helper")             Copy<Table>Msg       (root -> Msg, action)
#
# UNSUPPORTED cells: (category, language, path) -> reason. EVERYTHING NOT
# LISTED IS SUPPORTED. The single genuinely-unsupported shape is
# vector-of-union:
#   * Rust: `vector<union>` cannot be emitted at all — the pinned flatc's Rust
#     backend does not generate the accessors/builders the wrapper needs (the
#     documented repository limitation; see all_types_cpp_vector_union.fbs,
#     which is C++-only). Because
#     every local table gets a Rust typed-build helper, a `vector<union>` field
#     anywhere in a Rust-bound schema is rejected at the `typed_build`
#     chokepoint before any owned-clone path can run; the owned paths are
#     therefore declared unsupported for symmetry (each has a unit test on this
#     table) but cannot be reached independently.
#   * C++: `vector<union>` IS supported in `typed_build` (all_types_cpp_vector_union
#     round-trips it), but the action `copy_helper` (root -> owned Msg) does not
#     model the parallel discriminator/value vector reconstruction, so a
#     `vector<union>` field in an action Result/FeedbackData hard-fails.
UNSUPPORTED_FIELD_MATRIX: dict[tuple[str, str, str], str] = {
    ("vector_union", "rust", "typed_build"):
        "the pinned flatc Rust backend does not support vector-of-union; author "
        "the schema as C++-only (cpp_wrapper only) or replace the field with a "
        "discriminated table-of-arms (see all_types_cpp_vector_union.fbs)",
    ("vector_union", "rust", "owned_clone_from_msg"):
        "vector-of-union is unsupported on the Rust side (see typed_build)",
    ("vector_union", "rust", "owned_clone_from_root"):
        "vector-of-union is unsupported on the Rust side (see typed_build)",
    ("vector_union", "rust", "owned_borrow"):
        "vector-of-union is unsupported on the Rust side (see typed_build)",
    ("vector_union", "cpp", "copy_helper"):
        "the C++ action copy helper does not reconstruct a vector-of-union from "
        "its parallel discriminator/value vectors; keep vector-of-union fields "
        "out of action Result/FeedbackData tables",
}

# Valid emission paths per language. Every (language, path) pair here is
# a REAL emitter chokepoint; a `require_field_supported` call naming any other
# language, or a path not valid for the language, is a generator-side bug, so
# it hard-fails (fail-CLOSED) rather than silently passing. This is also
# the single source that defines the matrix's language + path axes.
LANGUAGE_EMISSION_PATHS: dict[str, tuple[str, ...]] = {
    "rust": (
        "typed_build",
        "owned_clone_from_msg",
        "owned_clone_from_root",
        "owned_borrow",
    ),
    "cpp": (
        "typed_build",
        "copy_helper",
    ),
}


def _build_field_support_matrix() -> dict[tuple[str, str, str], str | None]:
    """Materialize the COMPLETE state table: EVERY
    `(category × language × path)` cell over the Cartesian product of
    `FIELD_SUPPORT_CATEGORIES` and `LANGUAGE_EMISSION_PATHS` carries an EXPLICIT
    verdict — `None` == SUPPORTED, a reason string == UNSUPPORTED. Nothing is
    left implicit: a cell that is neither declared UNSUPPORTED nor a valid axis
    combination simply does not exist in the table, and `require_field_supported`
    hard-fails on any category/language/path outside these axes. Building the
    table from the product (rather than a hand-maintained denylist) guarantees
    a new category or emission path cannot silently create an unclassified,
    fail-open cell."""
    matrix: dict[tuple[str, str, str], str | None] = {}
    for category in FIELD_SUPPORT_CATEGORIES:
        for language, paths in LANGUAGE_EMISSION_PATHS.items():
            for path in paths:
                key = (category, language, path)
                matrix[key] = UNSUPPORTED_FIELD_MATRIX.get(key)
    # Every declared UNSUPPORTED cell must land on a real axis combination —
    # a typo'd category/language/path would otherwise be silently ignored
    # (the fail-open failure mode this table exists to kill).
    stray = set(UNSUPPORTED_FIELD_MATRIX) - set(matrix)
    if stray:
        raise GenerationError(
            "UNSUPPORTED_FIELD_MATRIX declares cell(s) outside the "
            "(category × language × path) axes: {}".format(sorted(stray)),
        )
    return matrix


# The complete, materialized (category × language × path) -> verdict table.
FIELD_SUPPORT_MATRIX: dict[tuple[str, str, str], str | None] = _build_field_support_matrix()


def _logical_element_kind(
    field_type: str,
    local_struct_names: set[str],
    local_table_names: set[str],
    local_union_names: set[str],
    local_enum_bases: dict[str, str],
    include_type_map: dict[str, list[tuple[str, str, str]]] | None,
) -> str:
    """Classify a non-vector field type into one of scalar/enum/string/struct/
    table/union, or "other" when it resolves to none of those (an unresolved
    name). Since the matrix is fail-CLOSED, "other" is REJECTED by
    `require_field_supported` (an unresolved type is a generator gap, not a
    licence to emit) — it is no longer a silent pass-through."""
    if is_string(field_type):
        return "string"
    base = local_type_base(field_type)
    if base in CPP_SCALAR_TYPES:
        return "scalar"
    if base in local_enum_bases:
        return "enum"
    if base in local_union_names:
        return "union"
    if base in local_struct_names:
        return "struct"
    if base in local_table_names:
        return "table"
    include_kind = _include_type_kind(base, include_type_map)
    if include_kind in ("enum", "union", "struct", "table"):
        return include_kind
    return "other"


def logical_field_category(
    field_type: str,
    local_struct_names: set[str],
    local_table_names: set[str],
    local_union_names: set[str],
    local_enum_bases: dict[str, str],
    include_type_map: dict[str, list[tuple[str, str, str]]] | None = None,
) -> str:
    """Language-agnostic logical category for a field's source-style type
    string, keyed against `FIELD_SUPPORT_CATEGORIES`. Independent of whether
    emission succeeds (unlike `rust_msg_field_type`, which raises for
    vector-of-union before it can return a category) — this is what the matrix
    check must consult."""
    vec_elem = split_vector_type(field_type)
    if vec_elem is not None:
        kind = _logical_element_kind(
            vec_elem,
            local_struct_names,
            local_table_names,
            local_union_names,
            local_enum_bases,
            include_type_map,
        )
        return "other" if kind == "other" else "vector_" + kind
    return _logical_element_kind(
        field_type,
        local_struct_names,
        local_table_names,
        local_union_names,
        local_enum_bases,
        include_type_map,
    )


def require_field_supported(
    category: str,
    language: str,
    path: str,
    stem: str,
    table_name: str,
    field_name: str,
    field_shape: str,
) -> None:
    """The single enforcement primitive for the complete type-support matrix
    FAIL-CLOSED: everything is rejected unless the matrix
    explicitly says SUPPORTED. Concretely:

      1. an unclassifiable/unknown `category` (including the `"other"`
         fall-through that `logical_field_category` returns for a field whose
         type resolves to nothing) is a generator-side gap, NOT a licence to
         emit code that may not compile — hard-fail;
      2. an unknown `language`, or a `path` not valid for that language, is a
         generator bug — hard-fail;
      3. a declared-UNSUPPORTED cell hard-fails with its reason;
      4. only an explicitly-SUPPORTED cell (`FIELD_SUPPORT_MATRIX[...] is None`)
         is a no-op.

    Every message names schema + table + field + shape + language + path +
    category, so a failure points straight at the offending declaration. Every
    branch has a unit test (`gen_message_wrapper_matrix_test`)."""
    def _fail(problem: str, reason: str) -> None:
        raise GenerationError(
            "unsupported field shape for {lang} {path}: schema '{stem}' table "
            "'{table}' field '{field}' has shape '{shape}' (category '{cat}') -- "
            "{problem}: {reason}".format(
                lang=language,
                path=path,
                stem=stem,
                table=table_name,
                field=field_name,
                shape=field_shape,
                cat=category,
                problem=problem,
                reason=reason,
            )
        )

    if category not in FIELD_SUPPORT_CATEGORIES:
        _fail(
            "unknown/unclassifiable field category",
            "category '{}' is not one of the supported categories {} (an "
            "unresolved type name classifies as 'other', which is never "
            "silently emitted)".format(category, list(FIELD_SUPPORT_CATEGORIES)),
        )
    if language not in LANGUAGE_EMISSION_PATHS:
        _fail(
            "unknown emission language",
            "language '{}' is not one of {}".format(
                language, sorted(LANGUAGE_EMISSION_PATHS)
            ),
        )
    if path not in LANGUAGE_EMISSION_PATHS[language]:
        _fail(
            "unknown emission path",
            "path '{}' is not a valid {} emission path {}".format(
                path, language, list(LANGUAGE_EMISSION_PATHS[language])
            ),
        )
    reason = FIELD_SUPPORT_MATRIX[(category, language, path)]
    if reason is None:
        return
    _fail("unsupported (category × language × path) cell", reason)


def write_rust(
    out: str,
    stem: str,
    schema_rel: str,
    root_fbs: str,
    with_schema_meta: bool,
    local_module_dir: str | None = None,
    local_module_prefix: str | None = None,
    action_name: str | None = None,
) -> None:
    # Semantic facts come from the bfbs loader (the
    # `emit_*` dispatchers). `collect_include_generated_modules` alone stays
    # text-derived (the `#[path]` block's message-root-relative generated
    # `.rs` paths — see the section comment above `_LOADED_SCHEMA`).
    root_doc = emit_root_schema_doc(root_fbs)
    # `top_mod` names a real flatc-generated Rust MODULE (re-exported
    # by `pub use {gen_mod}::{top_mod};` and by the prelude), so it takes the
    # same per-segment mangling flatc applies -- `p05` -> `p_05`. The
    # `== "aorta"` branch below is unaffected (mangling is identity there).
    top_mod = flatc_rust_namespace_segment(emit_namespace_top(root_fbs))
    include_paths = root_doc.include_paths
    include_type_map = emit_include_type_map(root_fbs, include_paths)
    include_modules = collect_include_generated_modules(root_fbs, schema_rel)
    namespace_full = root_doc.namespace_full
    # flatc SNAKE_CASES every namespace component when it emits Rust
    # modules, so a raw `.` -> `::` replacement dangles for any component with
    # a case/digit boundary (`p05.probe` -> `pub mod p_05`). Always route
    # through the shared converter.
    namespace_rust = flatc_rust_namespace_path(namespace_full) if namespace_full else ""
    root_type = emit_root_type(root_fbs)
    # The root type's OWN fully-qualified name (its real namespace) -- differs
    # from `namespace_full.<root>` only when the root_type is a FOREIGN type in a
    # different namespace than the local types.
    root_type_qualified = emit_root_type_qualified(root_fbs)
    local_type_names = list(root_doc.local_type_names)
    local_table_names = set(root_doc.table_fields.keys())
    all_struct_names = emit_struct_names(root_fbs)
    all_union_defs = emit_union_defs(root_fbs)
    all_union_names = set(all_union_defs.keys())
    all_enum_bases = emit_enum_bases(root_fbs)
    all_table_defs = emit_table_defs(root_fbs)
    all_table_names = set(all_table_defs.keys())

    # The root type is a
    # fixed-position marker (always first — an explicit, deliberate exception
    # to name-sorting: root-type markers sit at
    # explicit fixed positions); every other local type marker is then
    # sorted by qualified TypeId. All entries here share the root schema's
    # own namespace, so sorting by bare name is equivalent to sorting by
    # (namespace, name) — this replaces the previous "tables in declaration
    # order, then structs in declaration order" grouping from
    # `parse_local_type_names`, which did not track true source order across
    # interleaved table/struct declarations anyway.
    remaining_type_names = sorted(t for t in local_type_names if t != root_type)
    if root_type:
        local_type_names = [root_type] + remaining_type_names
    else:
        local_type_names = remaining_type_names

    const_prefix = const_prefix_from_stem(stem)
    gen_mod = f"{stem}_generated"
    meta_mod = f"{stem}_schema_meta"
    action_binding = derive_action_binding_names(
        schema_rel, stem, local_table_names, action_name
    )
    build_fn_map = {
        table_name: "build_{}_typed".format(sanitize_rust_ident(table_name).lower())
        for table_name in all_table_defs.keys()
    }
    union_build_fn_map = {
        union_name: "build_{}_union_typed".format(
            sanitize_rust_ident(union_name).lower()
        )
        for union_name in all_union_defs.keys()
    }

    all_tables: list[tuple[str, list[dict[str, str | None]]]] = list(
        (table_name, table_def.fields)
        for table_name, table_def in all_table_defs.items()
    )
    # Derive typed_tables from the already-canonical `all_table_defs` (which
    # `collect_table_defs` returns in qualified (namespace, name) order),
    # filtered to root-local tables with an `aorta_header` field -- NOT by
    # iterating `root_doc.table_fields.items()` directly, which is a plain
    # dict in source-declaration order. That direct-iteration form drove
    # Rust `HeaderInjector`/`TypedPublishMessage` impl emission order
    # (below) from source text order, exactly the defect this phase closes
    # for every other type-ordering site.
    typed_tables: list[tuple[str, list[dict[str, str | None]]]] = []
    for table_name, table_def in all_table_defs.items():
        if table_name not in local_table_names:
            continue
        if any(is_header_field(field) for field in table_def.fields):
            typed_tables.append((table_name, table_def.fields))

    # Type-support matrix enforcement: reject any UNSUPPORTED
    # (category x rust x typed_build) cell up front, before any emitter helper
    # runs, with a clear schema/table/field/shape message. Rust build helpers
    # are generated for EVERY table in the closure, so this sweep is the
    # chokepoint that rejects a `vector<union>` field anywhere in a Rust-bound
    # schema (the pinned flatc's Rust backend cannot emit it) -- ahead of the
    # deeper, less specific raise in `rust_msg_field_type`, and ahead of the
    # action owned-clone paths (which therefore cannot be reached with an
    # unsupported field; see UNSUPPORTED_FIELD_MATRIX's comment).
    for _mx_table_name, _mx_fields in all_tables:
        for _mx_field in _mx_fields:
            if is_header_field(_mx_field):
                continue
            _mx_type = str(_mx_field["type"])
            require_field_supported(
                logical_field_category(
                    _mx_type,
                    all_struct_names,
                    all_table_names,
                    all_union_names,
                    all_enum_bases,
                    include_type_map,
                ),
                "rust",
                "typed_build",
                stem,
                _mx_table_name,
                str(_mx_field["name"]),
                _mx_type,
            )

    with open(out, "w", encoding="utf-8") as f:
        def rust_module_path(path: str) -> str:
            if local_module_prefix:
                return f"{local_module_prefix}{Path(path).name}"
            if local_module_dir:
                return f"{local_module_dir}/{Path(path).name}"
            return f"../../{path}"

        bfbs_path = (
            f"{local_module_prefix}{stem}.bfbs"
            if local_module_prefix
            else (
                f"{local_module_dir}/{stem}.bfbs"
                if local_module_dir
                else f"../../{schema_rel}/{stem}.bfbs"
            )
        )

        f.write("//! Message bindings wrapper (generated)\n\n")
        f.write(
            "#![allow(\n"
            "    dead_code,\n"
            "    unused_imports,\n"
            "    non_snake_case,\n"
            "    mismatched_lifetime_syntaxes,\n"
            "    clippy::all\n"
            ")]\n\n",
        )

        f.write(f'#[path = "{rust_module_path("schemas/system/aorta_base_generated.rs")}"]\n')
        f.write("pub mod aorta_base_generated;\n\n")

        for include_mod, include_generated in include_modules:
            f.write(f'#[path = "{rust_module_path(include_generated)}"]\n')
            f.write(f"pub mod {include_mod};\n\n")

        f.write(f'#[path = "{rust_module_path(f"{schema_rel}/{stem}_generated.rs")}"]\n')
        f.write(f"pub mod {gen_mod};\n\n")

        if with_schema_meta:
            f.write(
                f'#[path = "{rust_module_path(f"{schema_rel}/{stem}_schema_meta.rs")}"]\n'
            )
            f.write(f"pub mod {meta_mod};\n\n")

        f.write("pub use aorta_base_generated::aorta;\n")
        if top_mod != "aorta":
            f.write(f"pub use {gen_mod}::{top_mod};\n\n")
        else:
            f.write(f"pub use {gen_mod}::aorta as schema_aorta;\n\n")

        f.write(
            f'pub const {const_prefix}_BFBS: &[u8] = include_bytes!("{bfbs_path}");\n\n',
        )

        if with_schema_meta:
            f.write(
                f"pub const {const_prefix}_FIXED_SIZE: Option<usize> = {meta_mod}::AORTA_FIXED_SIZE;\n",
            )
        else:
            f.write(f"pub const {const_prefix}_FIXED_SIZE: Option<usize> = None;\n")

        if local_type_names:
            f.write("\n")
            f.write("pub mod schema {\n")
            f.write("    use aorta_core::schema::SchemaMeta;\n\n")
            for type_name in local_type_names:
                marker = f"{sanitize_rust_ident(type_name)}Schema"
                if (
                    root_type is not None
                    and type_name == root_type
                    and root_type_qualified is not None
                ):
                    # The root type's OWN fully-qualified name. For a LOCAL root
                    # this equals `namespace_full.<root>` (byte-identical output);
                    # for a FOREIGN root in a different namespace it is the real
                    # FQN (`mrforeign.deep.ForeignRoot`, not the local-namespace
                    # `mrlocal.inner.ForeignRoot`), which schema discovery needs.
                    qualified = root_type_qualified
                else:
                    qualified = (
                        f"{namespace_full}.{type_name}" if namespace_full else type_name
                    )
                f.write(f"    pub struct {marker};\n")
                f.write(f"    impl SchemaMeta for {marker} {{\n")
                f.write("        fn schema_bfbs() -> &'static [u8] {\n")
                f.write(f"            super::{const_prefix}_BFBS\n")
                f.write("        }\n")
                if root_type is not None and type_name == root_type:
                    f.write("        fn fixed_size() -> Option<usize> {\n")
                    f.write(f"            super::{const_prefix}_FIXED_SIZE\n")
                    f.write("        }\n")
                f.write("        fn root_type() -> Option<&'static str> {\n")
                f.write(f'            Some("{qualified}")\n')
                f.write("        }\n")
                f.write("    }\n\n")

                if type_name in local_table_names:
                    if namespace_rust:
                        table_path = f"super::{gen_mod}::{namespace_rust}::{type_name}"
                    else:
                        table_path = f"super::{gen_mod}::{type_name}"
                    f.write(
                        f"    impl aorta_core::schema::TypedSubscribeMessage for {marker} {{\n"
                    )
                    f.write(
                        f"        type Root<'a> = <{table_path}<'a> as flatbuffers::Follow<'a>>::Inner;\n\n"
                    )
                    f.write(
                        "        fn decode_typed_root<'a>(payload: &'a [u8]) -> aorta_core::error::AortaResult<Self::Root<'a>> {\n"
                    )
                    f.write(
                        f"            aorta_core::schema::decode_flatbuffer_root::<{table_path}<'a>>(payload)\n"
                    )
                    f.write("        }\n")
                    f.write("    }\n\n")
            f.write("}\n")

        if action_binding is not None:
            f.write("\n")
            f.write(f"pub struct {action_binding['action_marker']};\n\n")
            f.write(
                f"impl aorta_core::action::ActionSchema for {action_binding['action_marker']} {{\n"
            )
            for assoc_name, table_name in (
                ("Goal", action_binding["goal"]),
                ("Result", action_binding["result"]),
                ("FeedbackData", action_binding["feedback_data"]),
                ("SendGoalRequest", action_binding["send_goal_request"]),
                ("SendGoalResponse", action_binding["send_goal_response"]),
                ("CancelGoalRequest", action_binding["cancel_goal_request"]),
                ("CancelGoalResponse", action_binding["cancel_goal_response"]),
                ("Feedback", action_binding["feedback"]),
                ("PublicStatus", action_binding["public_status"]),
                ("GetResultRequest", action_binding["get_result_request"]),
                ("GetResultResponse", action_binding["get_result_response"]),
            ):
                f.write(
                    f"    type {assoc_name} = schema::{sanitize_rust_ident(table_name)}Schema;\n"
                )
            f.write("\n")
            f.write(
                f"    const ACTION_NAME: &'static str = \"{action_binding['action_name']}\";\n"
            )
            f.write("}\n")

        needs_u8_vector_fast_path = False
        needs_i8_vector_fast_path = False
        for _, fields in all_tables:
            for field in fields:
                if is_header_field(field):
                    continue
                vec_elem = split_vector_type(str(field["type"]))
                if vec_elem is None:
                    continue
                elem_base = local_type_base(vec_elem)
                if elem_base == "ubyte":
                    needs_u8_vector_fast_path = True
                elif elem_base == "byte":
                    needs_i8_vector_fast_path = True

        if needs_u8_vector_fast_path or needs_i8_vector_fast_path:
            f.write("\n")
        if needs_u8_vector_fast_path:
            f.write("fn create_u8_vector_fast<'a>(\n")
            f.write("    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n")
            f.write("    data: &[u8],\n")
            f.write(") -> flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>> {\n")
            f.write(
                "    flatbuffers::WIPOffset::new(builder.create_byte_string(data).value())\n"
            )
            f.write("}\n")
        if needs_i8_vector_fast_path:
            f.write("fn create_i8_vector_fast<'a>(\n")
            f.write("    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n")
            f.write("    data: &[i8],\n")
            f.write(") -> flatbuffers::WIPOffset<flatbuffers::Vector<'a, i8>> {\n")
            f.write(
                "    let bytes = unsafe { core::slice::from_raw_parts(data.as_ptr() as *const u8, data.len()) };\n"
            )
            f.write(
                "    flatbuffers::WIPOffset::new(builder.create_byte_string(bytes).value())\n"
            )
            f.write("}\n")

        if all_tables or all_union_defs:
            # Determine which Msg structs need a lifetime parameter.
            # A struct needs <'a> if it has any scalar-vector field (category "vector_scalar")
            # because those are generated as &'a [T] instead of Vec<T>.
            tables_needing_lifetime: set[str] = set()
            for table_name, fields in all_tables:
                for field in fields:
                    if is_header_field(field):
                        continue
                    field_type = str(field["type"])
                    _, category = rust_msg_field_type(
                        field_type,
                        namespace_full,
                        gen_mod,
                        all_struct_names,
                        all_table_names,
                        all_union_names,
                        all_enum_bases,
                        include_type_map,
                    )
                    if category == "vector_scalar":
                        tables_needing_lifetime.add(table_name)
                        break

            # Propagate lifetime requirements transitively through table/union
            # references.  If TableA contains a field of type TableB and TableB
            # needs <'a>, then TableA also needs <'a>.  Same for union fields
            # whose variants include a lifetime-needing table.
            table_table_deps: dict[str, set[str]] = {}
            for table_name, fields in all_tables:
                deps: set[str] = set()
                for field in fields:
                    if is_header_field(field):
                        continue
                    field_type = str(field["type"])
                    _, category = rust_msg_field_type(
                        field_type,
                        namespace_full,
                        gen_mod,
                        all_struct_names,
                        all_table_names,
                        all_union_names,
                        all_enum_bases,
                        include_type_map,
                    )
                    base = local_type_base(field_type)
                    if category == "table":
                        deps.add(base)
                    elif category == "union":
                        # A union's lifetime is determined by its variant
                        # tables (the TARGET each variant's alias resolves
                        # to, not the alias itself).
                        for _alias, target in all_union_defs.get(base, []):
                            deps.add(target)
                    elif category == "vector_table":
                        vec_elem = split_vector_type(field_type)
                        if vec_elem:
                            deps.add(local_type_base(vec_elem))
                table_table_deps[table_name] = deps

            changed = True
            while changed:
                changed = False
                for tbl, deps in table_table_deps.items():
                    if tbl not in tables_needing_lifetime:
                        if deps & tables_needing_lifetime:
                            tables_needing_lifetime.add(tbl)
                            changed = True

            # Derive which unions need a lifetime parameter — a union needs <'a>
            # if any of its variant tables (targets) needs <'a>.
            unions_needing_lifetime: set[str] = set()
            for u_name, variants in all_union_defs.items():
                if any(target in tables_needing_lifetime for _alias, target in variants):
                    unions_needing_lifetime.add(u_name)

            def _annotate_field_lt(
                rust_type: str,
                category: str,
                field_type: str,
            ) -> str:
                """Add <'a> to a Rust field type when the referenced table/union
                needs a lifetime parameter."""
                base = local_type_base(field_type)
                if category == "table" and base in tables_needing_lifetime:
                    return f"Box<{base}Msg<'a>>"
                if category == "union" and base in unions_needing_lifetime:
                    return f"{base}Msg<'a>"
                if category == "vector_table":
                    vec_elem = split_vector_type(field_type)
                    if vec_elem:
                        vbase = local_type_base(vec_elem)
                        if vbase in tables_needing_lifetime:
                            return f"Vec<{vbase}Msg<'a>>"
                return rust_type

            f.write("\n")
            f.write("pub mod typed {\n")
            for union_name, variants in all_union_defs.items():
                union_msg_name = f"{union_name}Msg"
                # Check if any variant's target table needs a lifetime parameter.
                union_needs_lt = any(
                    target in tables_needing_lifetime for _alias, target in variants
                )
                f.write("    #[derive(Clone, Debug)]\n")
                if union_needs_lt:
                    f.write(f"    pub enum {union_msg_name}<'a> {{\n")
                else:
                    f.write(f"    pub enum {union_msg_name} {{\n")
                # The enum VARIANT identifier is the ALIAS (what
                # flatc's own generated union enum names the member as --
                # ev.name, see idl_gen_rust.cpp's Name(ev)); the boxed Msg
                # struct type is the TARGET's (what the alias resolves to).
                # These are the same string for every union in this repo
                # today (no `Alias:Target` member exists yet), but must not
                # be conflated in general.
                for alias, target in variants:
                    if target not in all_table_names:
                        raise GenerationError(
                            "union variant '{}.{}' (target '{}') is not supported by typed wrapper generation (only local table variants are supported)".format(
                                union_name,
                                alias,
                                target,
                            ),
                        )
                    if target in tables_needing_lifetime:
                        f.write(
                            f"        {alias}(Box<{target}Msg<'a>>),\n"
                        )
                    else:
                        f.write(f"        {alias}(Box<{target}Msg>),\n")
                f.write("    }\n\n")

            for table_name, fields in all_tables:
                msg_name = f"{table_name}Msg"
                needs_lt = table_name in tables_needing_lifetime
                f.write("    #[derive(Clone, Debug, Default)]\n")
                if needs_lt:
                    f.write(f"    pub struct {msg_name}<'a> {{\n")
                else:
                    f.write(f"    pub struct {msg_name} {{\n")
                for field in fields:
                    if is_header_field(field):
                        continue
                    field_name = str(field["name"])
                    msg_field_name = rust_msg_field_name(field_name)
                    field_type = str(field["type"])
                    rust_type, category = rust_msg_field_type(
                        field_type,
                        namespace_full,
                        gen_mod,
                        all_struct_names,
                        all_table_names,
                        all_union_names,
                        all_enum_bases,
                        include_type_map,
                    )
                    rust_type = _annotate_field_lt(rust_type, category, field_type)
                    f.write(f"        pub {msg_field_name}: Option<{rust_type}>,\n")
                f.write("    }\n\n")
            f.write("}\n")

            for union_name, variants in all_union_defs.items():
                union_msg_name = f"{union_name}Msg"
                union_helper_fn = union_build_fn_map[union_name]
                union_type_path = qualify_rust_user_type(
                    union_name,
                    namespace_full,
                    gen_mod,
                    include_type_map,
                )
                union_needs_lt = any(
                    target in tables_needing_lifetime for _alias, target in variants
                )
                union_msg_lt = (
                    f"typed::{union_msg_name}<'_>"
                    if union_needs_lt
                    else f"typed::{union_msg_name}"
                )
                f.write("\n")
                f.write(f"fn {union_helper_fn}<'a>(\n")
                f.write("    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n")
                f.write(f"    msg: &{union_msg_lt},\n")
                f.write(") -> (\n")
                f.write(f"    {union_type_path},\n")
                f.write("    flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,\n")
                f.write(") {\n")
                f.write("    match msg {\n")
                # `{union_type_path}::{alias}` matches flatc's own
                # generated union discriminant enum, whose variant identifier
                # is the ALIAS (idl_gen_rust.cpp: `U_ELEMENT_NAME` <-
                # `namer_.Function(ev.name)`), not the target's bare name;
                # `child_helper` resolves the TARGET table's own builder fn.
                for alias, target in variants:
                    child_helper = build_fn_map.get(target)
                    if child_helper is None:
                        raise GenerationError(
                            "failed to resolve helper for union variant '{}.{}' (target '{}')".format(
                                union_name,
                                alias,
                                target,
                            ),
                        )
                    f.write(
                        f"        typed::{union_msg_name}::{alias}(value) => {{\n"
                    )
                    f.write(
                        f"            let offset = {child_helper}(builder, None, value.as_ref());\n",
                    )
                    f.write(
                        f"            ({union_type_path}::{alias}, offset.as_union_value())\n",
                    )
                    f.write("        }\n")
                f.write("    }\n")
                f.write("}\n")

            for table_name, fields in all_tables:
                msg_name = f"{table_name}Msg"
                helper_fn = build_fn_map[table_name]
                table_def = all_table_defs[table_name]
                table_namespace_rust = (
                    flatc_rust_namespace_path(table_def.namespace_full)
                    if table_def.namespace_full
                    else ""
                )
                if table_namespace_rust:
                    table_path = (
                        f"{table_def.gen_mod}::{table_namespace_rust}::{table_name}"
                    )
                else:
                    table_path = f"{table_def.gen_mod}::{table_name}"
                builder_path = f"{table_path}Builder"
                has_header = any(is_header_field(field) for field in fields)
                needs_lt = table_name in tables_needing_lifetime
                msg_lt = f"typed::{msg_name}<'_>" if needs_lt else f"typed::{msg_name}"

                f.write("\n")
                f.write(f"fn {helper_fn}<'a>(\n")
                f.write("    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n")
                f.write(
                    "    header: Option<flatbuffers::WIPOffset<aorta::sys::AortaHeader<'a>>>,\n"
                )
                f.write(f"    msg: &{msg_lt},\n")
                f.write(f") -> flatbuffers::WIPOffset<{table_path}<'a>> {{\n")

                for field in fields:
                    if is_header_field(field):
                        continue
                    field_name = str(field["name"])
                    msg_field_name = rust_msg_field_name(field_name)
                    field_type = str(field["type"])
                    _, category = rust_msg_field_type(
                        field_type,
                        namespace_full,
                        gen_mod,
                        all_struct_names,
                        all_table_names,
                        all_union_names,
                        all_enum_bases,
                        include_type_map,
                    )
                    if category == "string":
                        f.write(
                            f"    let {field_name}_offset = msg.{msg_field_name}.as_ref().map(|value| builder.create_string(value));\n",
                        )
                    elif category == "vector_scalar":
                        vec_elem = split_vector_type(field_type)
                        elem_base = local_type_base(vec_elem or "")
                        if elem_base == "ubyte":
                            f.write(
                                f"    let {field_name}_offset = msg.{msg_field_name}.map(|value| create_u8_vector_fast(builder, value));\n",
                            )
                        elif elem_base == "byte":
                            f.write(
                                f"    let {field_name}_offset = msg.{msg_field_name}.map(|value| create_i8_vector_fast(builder, value));\n",
                            )
                        else:
                            f.write(
                                f"    let {field_name}_offset = msg.{msg_field_name}.map(|value| builder.create_vector(value));\n",
                            )
                    elif category in ("vector", "vector_enum"):
                        f.write(
                            f"    let {field_name}_offset = msg.{msg_field_name}.as_ref().map(|value| builder.create_vector(value.as_slice()));\n",
                        )
                    elif category == "vector_string":
                        f.write(
                            f"    let {field_name}_offset = msg.{msg_field_name}.as_ref().map(|value| {{\n",
                        )
                        f.write(
                            "        let mut offsets = Vec::with_capacity(value.len());\n",
                        )
                        f.write("        for item in value.iter() {\n")
                        f.write(
                            "            offsets.push(builder.create_string(item));\n"
                        )
                        f.write("        }\n")
                        f.write("        builder.create_vector(offsets.as_slice())\n")
                        f.write("    });\n")
                    elif category == "table":
                        child_table = local_type_base(field_type)
                        child_helper = build_fn_map.get(child_table)
                        if child_helper is None:
                            raise GenerationError(
                                f"failed to resolve helper for table field '{field_name}:{field_type}'",
                            )
                        f.write(
                            f"    let {field_name}_offset = msg.{msg_field_name}.as_ref().map(|value| {child_helper}(builder, None, value.as_ref()));\n",
                        )
                    elif category == "vector_table":
                        child_table = local_type_base(
                            split_vector_type(field_type) or ""
                        )
                        child_helper = build_fn_map.get(child_table)
                        if child_helper is None:
                            raise GenerationError(
                                f"failed to resolve helper for vector<table> field '{field_name}:{field_type}'",
                            )
                        f.write(
                            f"    let {field_name}_offset = msg.{msg_field_name}.as_ref().map(|value| {{\n",
                        )
                        f.write(
                            "        let mut offsets = Vec::with_capacity(value.len());\n",
                        )
                        f.write("        for item in value.iter() {\n")
                        f.write(
                            f"            offsets.push({child_helper}(builder, None, item));\n",
                        )
                        f.write("        }\n")
                        f.write("        builder.create_vector(offsets.as_slice())\n")
                        f.write("    });\n")
                    elif category == "union":
                        union_name = local_type_base(field_type)
                        union_helper_fn = union_build_fn_map.get(union_name)
                        if union_helper_fn is None:
                            raise GenerationError(
                                "failed to resolve helper for union field '{}:{}'".format(
                                    field_name,
                                    field_type,
                                ),
                            )
                        f.write(
                            f"    let {field_name}_union = msg.{msg_field_name}.as_ref().map(|value| {union_helper_fn}(builder, value));\n",
                        )

                f.write(f"    let mut root_builder = {builder_path}::new(builder);\n")
                if has_header:
                    f.write("    if let Some(value) = header {\n")
                    f.write("        root_builder.add_aorta_header(value);\n")
                    f.write("    }\n")
                else:
                    f.write("    let _ = header;\n")

                for field in fields:
                    if is_header_field(field):
                        continue
                    field_name = str(field["name"])
                    msg_field_name = rust_msg_field_name(field_name)
                    field_type = str(field["type"])
                    _, category = rust_msg_field_type(
                        field_type,
                        namespace_full,
                        gen_mod,
                        all_struct_names,
                        all_table_names,
                        all_union_names,
                        all_enum_bases,
                        include_type_map,
                    )
                    method_name = rust_builder_field_name(field_name)
                    if category in (
                        "string",
                        "vector",
                        "vector_scalar",
                        "vector_enum",
                        "vector_string",
                        "table",
                        "vector_table",
                    ):
                        f.write(f"    if let Some(value) = {field_name}_offset {{\n")
                        f.write(f"        root_builder.add_{method_name}(value);\n")
                        f.write("    }\n")
                    elif category == "union":
                        f.write(
                            f"    if let Some((union_type, union_value)) = {field_name}_union {{\n"
                        )
                        f.write(
                            f"        root_builder.add_{method_name}_type(union_type);\n"
                        )
                        f.write(
                            f"        root_builder.add_{method_name}(union_value);\n"
                        )
                        f.write("    }\n")
                    elif category == "struct":
                        f.write(
                            f"    if let Some(value) = msg.{msg_field_name}.as_ref() {{\n"
                        )
                        f.write(f"        root_builder.add_{method_name}(value);\n")
                        f.write("    }\n")
                    elif category == "scalar":
                        f.write(f"    if let Some(value) = msg.{msg_field_name} {{\n")
                        f.write(f"        root_builder.add_{method_name}(value);\n")
                        f.write("    }\n")
                    elif category == "enum":
                        f.write(
                            f"    if let Some(value) = msg.{msg_field_name}.as_ref() {{\n"
                        )
                        f.write(f"        root_builder.add_{method_name}(*value);\n")
                        f.write("    }\n")
                    else:
                        raise GenerationError(
                            "unsupported Rust field category '{}' for '{}:{}'".format(
                                category,
                                table_name,
                                field_name,
                            ),
                        )

                f.write("    root_builder.finish()\n")
                f.write("}\n")

            for table_name, _ in typed_tables:
                marker = f"{sanitize_rust_ident(table_name)}Schema"
                msg_name = f"{table_name}Msg"
                if namespace_rust:
                    table_path = f"{gen_mod}::{namespace_rust}::{table_name}"
                else:
                    table_path = f"{gen_mod}::{table_name}"
                helper_fn = build_fn_map[table_name]

                f.write("\n")
                f.write(
                    f"impl aorta_core::schema::HeaderInjector for schema::{marker} {{\n"
                )
                f.write("    type Header<'a> = aorta::sys::AortaHeader<'a>;\n\n")
                f.write("    fn build_header<'b>(\n")
                f.write("        builder: &mut flatbuffers::FlatBufferBuilder<'b>,\n")
                f.write("        values: &aorta_core::schema::HeaderValues<'_>,\n")
                f.write("    ) -> flatbuffers::WIPOffset<Self::Header<'b>> {\n")
                f.write(
                    "        let node = builder.create_string(values.publisher_node);\n"
                )
                f.write("        aorta::sys::AortaHeader::create(\n")
                f.write("            builder,\n")
                f.write("            &aorta::sys::AortaHeaderArgs {\n")
                f.write("                publish_stamp_ns: values.publish_stamp_ns,\n")
                f.write("                publisher_node: Some(node),\n")
                f.write("                sequence: values.sequence,\n")
                f.write("            },\n")
                f.write("        )\n")
                f.write("    }\n")
                f.write("}\n")

                f.write("\n")
                f.write(
                    f"impl aorta_core::schema::TypedPublishMessage for schema::{marker} {{\n"
                )
                needs_lt = table_name in tables_needing_lifetime
                if needs_lt:
                    f.write(f"    type Msg<'a> = typed::{msg_name}<'a>;\n")
                else:
                    f.write(f"    type Msg<'a> = typed::{msg_name};\n")
                f.write(f"    type Root<'a> = {table_path}<'a>;\n\n")
                f.write("    fn build_typed_root<'a>(\n")
                f.write("        builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n")
                f.write("        header: flatbuffers::WIPOffset<Self::Header<'a>>,\n")
                f.write("        msg: &Self::Msg<'_>,\n")
                f.write("    ) -> flatbuffers::WIPOffset<Self::Root<'a>> {\n")
                f.write(f"        {helper_fn}(builder, Some(header), msg)\n")
                f.write("    }\n")
                f.write("}\n")

            if action_binding is not None:
                def _action_msg_type(table_name: str, lifetime: str) -> str:
                    if table_name in tables_needing_lifetime:
                        return f"typed::{table_name}Msg<{lifetime}>"
                    return f"typed::{table_name}Msg"

                goal_msg_ty = _action_msg_type(action_binding["goal"], "'a")
                result_msg_ty = _action_msg_type(action_binding["result"], "'a")
                feedback_msg_ty = _action_msg_type(
                    action_binding["feedback_data"],
                    "'a",
                )
                result_builder_fn = build_fn_map[action_binding["result"]]
                feedback_data_builder_fn = build_fn_map[action_binding["feedback_data"]]
                send_goal_request_builder_fn = build_fn_map[
                    action_binding["send_goal_request"]
                ]
                send_goal_response_builder_fn = build_fn_map[
                    action_binding["send_goal_response"]
                ]
                cancel_goal_request_builder_fn = build_fn_map[
                    action_binding["cancel_goal_request"]
                ]
                cancel_goal_response_builder_fn = build_fn_map[
                    action_binding["cancel_goal_response"]
                ]
                feedback_builder_fn = build_fn_map[action_binding["feedback"]]
                public_status_builder_fn = build_fn_map[action_binding["public_status"]]

                if namespace_rust:
                    send_goal_request_path = (
                        f"{gen_mod}::{namespace_rust}::{action_binding['send_goal_request']}"
                    )
                    send_goal_response_path = (
                        f"{gen_mod}::{namespace_rust}::{action_binding['send_goal_response']}"
                    )
                    cancel_goal_request_path = (
                        f"{gen_mod}::{namespace_rust}::{action_binding['cancel_goal_request']}"
                    )
                    cancel_goal_response_path = (
                        f"{gen_mod}::{namespace_rust}::{action_binding['cancel_goal_response']}"
                    )
                    public_status_path = (
                        f"{gen_mod}::{namespace_rust}::{action_binding['public_status']}"
                    )
                else:
                    send_goal_request_path = (
                        f"{gen_mod}::{action_binding['send_goal_request']}"
                    )
                    send_goal_response_path = (
                        f"{gen_mod}::{action_binding['send_goal_response']}"
                    )
                    cancel_goal_request_path = (
                        f"{gen_mod}::{action_binding['cancel_goal_request']}"
                    )
                    cancel_goal_response_path = (
                        f"{gen_mod}::{action_binding['cancel_goal_response']}"
                    )
                    public_status_path = (
                        f"{gen_mod}::{action_binding['public_status']}"
                    )

                table_fields_by_name = {name: fields for name, fields in all_tables}
                owned_msg_clone_helper_names: dict[str, str] = {}
                owned_root_helper_names: dict[str, str] = {}
                owned_msg_borrow_helper_names: dict[str, str] = {}
                owned_union_clone_helper_names: dict[str, str] = {}
                owned_union_borrow_helper_names: dict[str, str] = {}
                # Collected helper-fn bodies, flushed to `f` at module scope AFTER
                # every (possibly recursive) seeding call below has finished. These
                # helpers recurse into one another to clone/borrow nested tables and
                # unions; writing a child body to `f` inline — while a parent body's
                # struct/match literal is still open — would splice a `fn { ... }`
                # into the middle of that literal and emit invalid Rust. Buffering
                # each body and flushing once keeps every fn contiguous; definition
                # order is irrelevant for free fns.
                pending_action_helper_defs: list[str] = []

                def _table_root_path(table_name: str) -> str:
                    table_def = all_table_defs[table_name]
                    table_namespace_rust = (
                        flatc_rust_namespace_path(table_def.namespace_full)
                        if table_def.namespace_full
                        else ""
                    )
                    if table_namespace_rust:
                        return f"{table_def.gen_mod}::{table_namespace_rust}::{table_name}"
                    return f"{table_def.gen_mod}::{table_name}"

                def _union_root_path(union_name: str) -> str:
                    union_def = all_table_defs.get(union_name)
                    union_namespace = namespace_full
                    union_gen_mod = gen_mod
                    if union_def is not None:
                        union_namespace = union_def.namespace_full
                        union_gen_mod = union_def.gen_mod
                    return qualify_rust_user_type(
                        union_name,
                        union_namespace,
                        union_gen_mod,
                        include_type_map,
                    )

                def _typed_union_msg_type(union_name: str, lifetime: str) -> str:
                    if union_name in unions_needing_lifetime:
                        return f"typed::{union_name}Msg<{lifetime}>"
                    return f"typed::{union_name}Msg"

                def _owned_msg_type(table_name: str) -> str:
                    return f"owned::{table_name}Msg"

                def _owned_union_msg_type(union_name: str) -> str:
                    return f"owned::{union_name}Msg"

                def _owned_field_type(field_type: str) -> str:
                    rust_type, category = rust_msg_field_type(
                        field_type,
                        namespace_full,
                        gen_mod,
                        all_struct_names,
                        all_table_names,
                        all_union_names,
                        all_enum_bases,
                        include_type_map,
                    )
                    if category == "vector_scalar":
                        vec_elem = split_vector_type(field_type)
                        if vec_elem is None:
                            raise GenerationError(
                                f"failed to resolve Rust owned vector scalar field '{field_type}'",
                            )
                        elem_base = local_type_base(vec_elem)
                        elem_type = RUST_SCALAR_TYPES.get(elem_base)
                        if elem_type is None:
                            raise GenerationError(
                                f"failed to resolve Rust owned vector scalar element '{field_type}'",
                            )
                        return f"Vec<{elem_type}>"
                    if category == "table":
                        return f"Box<{local_type_base(field_type)}Msg>"
                    if category == "vector_table":
                        vec_elem = split_vector_type(field_type)
                        if vec_elem is None:
                            raise GenerationError(
                                f"failed to resolve Rust owned vector<table> field '{field_type}'",
                            )
                        return f"Vec<{local_type_base(vec_elem)}Msg>"
                    if category == "union":
                        return f"{local_type_base(field_type)}Msg"
                    return rust_type

                f.write("\n")
                f.write("pub mod owned {\n")
                for union_name, variants in all_union_defs.items():
                    f.write("    #[derive(Clone, Debug)]\n")
                    f.write(f"    pub enum {union_name}Msg {{\n")
                    for alias, target in variants:
                        if target not in table_fields_by_name:
                            raise GenerationError(
                                "Rust action owned union only supports local table variants; '{}.{}' (target '{}') is unsupported".format(
                                    union_name,
                                    alias,
                                    target,
                                )
                            )
                        f.write(f"        {alias}(Box<{target}Msg>),\n")
                    f.write("    }\n\n")

                for table_name, fields in all_tables:
                    f.write("    #[derive(Clone, Debug, Default)]\n")
                    f.write(f"    pub struct {table_name}Msg {{\n")
                    for field in fields:
                        if is_header_field(field):
                            continue
                        field_name = str(field["name"])
                        msg_field_name = rust_msg_field_name(field_name)
                        field_type = str(field["type"])
                        f.write(
                            f"        pub {msg_field_name}: Option<{_owned_field_type(field_type)}>,\n"
                        )
                    f.write("    }\n\n")
                f.write("}\n")

                def _owned_union_msg_clone_helper(union_name: str) -> str:
                    helper_name = owned_union_clone_helper_names.get(union_name)
                    if helper_name is not None:
                        return helper_name

                    helper_name = f"clone_{sanitize_rust_ident(union_name).lower()}_owned_from_msg"
                    owned_union_clone_helper_names[union_name] = helper_name
                    msg_ty = _typed_union_msg_type(union_name, "'_")
                    parts: list[str] = []
                    parts.append("\n")
                    parts.append(f"fn {helper_name}(msg: &{msg_ty}) -> {_owned_union_msg_type(union_name)} {{\n")
                    parts.append("    match msg {\n")
                    for alias, target in all_union_defs[union_name]:
                        child_helper = _owned_msg_clone_helper(target)
                        parts.append(
                            f"        typed::{union_name}Msg::{alias}(value) => {_owned_union_msg_type(union_name)}::{alias}(Box::new({child_helper}(value.as_ref()))),\n"
                        )
                    parts.append("    }\n")
                    parts.append("}\n")
                    pending_action_helper_defs.append("".join(parts))
                    return helper_name

                def _owned_msg_clone_helper(table_name: str) -> str:
                    helper_name = owned_msg_clone_helper_names.get(table_name)
                    if helper_name is not None:
                        return helper_name

                    helper_name = f"clone_{sanitize_rust_ident(table_name).lower()}_owned_from_msg"
                    owned_msg_clone_helper_names[table_name] = helper_name
                    msg_ty = _action_msg_type(table_name, "'_")
                    fields = table_fields_by_name[table_name]
                    parts: list[str] = []
                    parts.append("\n")
                    parts.append(f"fn {helper_name}(msg: &{msg_ty}) -> {_owned_msg_type(table_name)} {{\n")
                    parts.append(f"    {_owned_msg_type(table_name)} {{\n")
                    for field in fields:
                        if is_header_field(field):
                            continue
                        field_name = str(field["name"])
                        msg_field_name = rust_msg_field_name(field_name)
                        field_type = str(field["type"])
                        _, category = rust_msg_field_type(
                            field_type,
                            namespace_full,
                            gen_mod,
                            all_struct_names,
                            all_table_names,
                            all_union_names,
                            all_enum_bases,
                            include_type_map,
                        )
                        if category in (
                            "scalar",
                            "enum",
                            "struct",
                            "string",
                            "vector",
                            "vector_enum",
                            "vector_string",
                        ):
                            expr = f"msg.{msg_field_name}.clone()"
                        elif category == "vector_scalar":
                            expr = f"msg.{msg_field_name}.map(|values| values.to_vec())"
                        elif category == "table":
                            child_table = local_type_base(field_type)
                            child_helper = _owned_msg_clone_helper(child_table)
                            expr = (
                                f"msg.{msg_field_name}.as_ref().map(|value| Box::new({child_helper}(value.as_ref())))"
                            )
                        elif category == "vector_table":
                            child_table = local_type_base(
                                split_vector_type(field_type) or ""
                            )
                            child_helper = _owned_msg_clone_helper(child_table)
                            expr = (
                                f"msg.{msg_field_name}.as_ref().map(|values| values.iter().map(|value| {child_helper}(value)).collect())"
                            )
                        elif category == "union":
                            union_name = local_type_base(field_type)
                            union_helper = _owned_union_msg_clone_helper(union_name)
                            expr = (
                                f"msg.{msg_field_name}.as_ref().map(|value| {union_helper}(value))"
                            )
                        else:
                            raise GenerationError(
                                "unsupported Rust action owned clone field '{}:{}'".format(
                                    field_name,
                                    field_type,
                                )
                            )
                        parts.append(f"        {msg_field_name}: {expr},\n")
                    parts.append("    }\n")
                    parts.append("}\n")
                    pending_action_helper_defs.append("".join(parts))
                    return helper_name

                def _owned_root_helper(table_name: str) -> str:
                    helper_name = owned_root_helper_names.get(table_name)
                    if helper_name is not None:
                        return helper_name

                    helper_name = f"clone_{sanitize_rust_ident(table_name).lower()}_owned_from_root"
                    owned_root_helper_names[table_name] = helper_name
                    fields = table_fields_by_name[table_name]
                    table_path = _table_root_path(table_name)
                    parts: list[str] = []
                    parts.append("\n")
                    parts.append(
                        f"fn {helper_name}(root: {table_path}<'_>) -> {_owned_msg_type(table_name)} {{\n"
                    )
                    parts.append(f"    {_owned_msg_type(table_name)} {{\n")
                    for field in fields:
                        if is_header_field(field):
                            continue
                        field_name = str(field["name"])
                        msg_field_name = rust_msg_field_name(field_name)
                        accessor_name = rust_builder_field_name(field_name)
                        field_type = str(field["type"])
                        # A `required` reference field (string /
                        # table / vector) has a flatc accessor that returns the
                        # value DIRECTLY (verified via rustc: `&str`, `Leaf<'a>`,
                        # `Vector<'a, T>`), not `Option<…>`. The owned Msg field is
                        # still `Option<T>`, so emit `Some(<transform>(root.x()))`
                        # instead of the optional `root.x().map(|v| <transform>(v))`
                        # (which is E0599 -- `&str`/`Vector` have no `.map`).
                        req = bool(field.get("required"))
                        _, category = rust_msg_field_type(
                            field_type,
                            namespace_full,
                            gen_mod,
                            all_struct_names,
                            all_table_names,
                            all_union_names,
                            all_enum_bases,
                            include_type_map,
                        )
                        if category in ("scalar", "enum"):
                            # Type-support matrix: a NON-optional
                            # scalar/enum is returned BY VALUE by flatc's
                            # accessor -> `Some(root.acc())`. A `= null` OPTIONAL
                            # scalar/enum's accessor already returns `Option<T>`,
                            # so `Some(root.acc())` would be `Option<Option<T>>`
                            # against the owned `Option<T>` field -- a
                            # compile error. Emit the accessor's `Option<T>`
                            # directly when the field is optional.
                            if field.get("optional"):
                                expr = f"root.{accessor_name}()"
                            else:
                                expr = f"Some(root.{accessor_name}())"
                        elif category == "struct":
                            # Type-support matrix: an OPTIONAL
                            # struct field's flatc accessor returns `Option<&T>`,
                            # so `Some(root.acc())` yields `Option<Option<&T>>`
                            # against an owned `Option<T>` field -- a probe-proven
                            # compile bomb. flatc structs are `Copy`;
                            # `.copied()` maps `Option<&T>` -> `Option<T>`.
                            # flatc allows `required` on struct
                            # fields too (idl_parser.cpp), whose accessor returns
                            # `&T` DIRECTLY (no Option) -- `.copied()` is then an
                            # E0599 bomb. Deref the `&T` (structs are `Copy`) and
                            # wrap in `Some`.
                            if req:
                                expr = f"Some(*root.{accessor_name}())"
                            else:
                                expr = f"root.{accessor_name}().copied()"
                        elif category == "string":
                            if req:
                                expr = f"Some(root.{accessor_name}().to_string())"
                            else:
                                expr = (
                                    f"root.{accessor_name}().map(|value| value.to_string())"
                                )
                        elif category == "vector_scalar":
                            # Type-support matrix: flatc's Rust
                            # scalar-vector accessor returns `Vector<'a, T>` for
                            # EVERY scalar element (numeric AND `bool`), not a
                            # `&[T]` slice -- `Vector` has no `.to_vec()` (a
                            # probe-proven compile bomb, surfaced by the golden's
                            # `[bool]`; `bool` additionally has no `SafeSliceAccess`
                            # impl so a slice route is impossible). `.iter()`
                            # yields each element BY VALUE (scalars are
                            # `Follow<Inner = Self>`), mirroring the `vector_enum`
                            # idiom below. (No pre-existing action golden had a
                            # scalar vector, so this branch was never emitted
                            # before -- the change is inert for existing outputs.)
                            if req:
                                expr = f"Some(root.{accessor_name}().iter().collect())"
                            else:
                                expr = f"root.{accessor_name}().map(|values| values.iter().collect())"
                        elif category == "vector":
                            # Type-support matrix: a `[struct]`
                            # field iterates as `&T` (structs are yielded by
                            # reference), so `values.iter().collect()` builds a
                            # `Vec<&T>` into an owned `Vec<T>` field -- a
                            # probe-proven compile bomb. `.copied()` widens
                            # each `&T` to an owned `T` (structs are `Copy`).
                            if req:
                                expr = f"Some(root.{accessor_name}().iter().copied().collect())"
                            else:
                                expr = (
                                    f"root.{accessor_name}().map(|values| values.iter().copied().collect())"
                                )
                        elif category == "vector_enum":
                            # Enum vectors iterate BY VALUE (flatc enums are
                            # `Follow<Inner = Self>`), so `.iter().collect()`
                            # already yields an owned `Vec<Enum>`.
                            if req:
                                expr = f"Some(root.{accessor_name}().iter().collect())"
                            else:
                                expr = (
                                    f"root.{accessor_name}().map(|values| values.iter().collect())"
                                )
                        elif category == "vector_string":
                            if req:
                                expr = (
                                    f"Some(root.{accessor_name}().iter().map(|value| value.to_string()).collect())"
                                )
                            else:
                                expr = (
                                    f"root.{accessor_name}().map(|values| values.iter().map(|value| value.to_string()).collect())"
                                )
                        elif category == "table":
                            child_table = local_type_base(field_type)
                            if child_table not in table_fields_by_name:
                                raise GenerationError(
                                    "Rust action get_result root cloning only supports local nested tables; field '{}:{}' is unsupported".format(
                                        field_name, field_type
                                    )
                                )
                            child_helper = _owned_root_helper(child_table)
                            if req:
                                expr = f"Some(Box::new({child_helper}(root.{accessor_name}())))"
                            else:
                                expr = (
                                    f"root.{accessor_name}().map(|value| Box::new({child_helper}(value)))"
                                )
                        elif category == "vector_table":
                            child_table = local_type_base(
                                split_vector_type(field_type) or ""
                            )
                            if child_table not in table_fields_by_name:
                                raise GenerationError(
                                    "Rust action get_result root cloning only supports local nested tables; field '{}:{}' is unsupported".format(
                                        field_name, field_type
                                    )
                                )
                            child_helper = _owned_root_helper(child_table)
                            if req:
                                expr = (
                                    f"Some(root.{accessor_name}().iter().map(|value| {child_helper}(value)).collect())"
                                )
                            else:
                                expr = (
                                    f"root.{accessor_name}().map(|values| values.iter().map(|value| {child_helper}(value)).collect())"
                                )
                        elif category == "union":
                            union_name = local_type_base(field_type)
                            union_type_path = _union_root_path(union_name)
                            variants = all_union_defs.get(union_name)
                            if not variants:
                                raise GenerationError(
                                    "Rust action get_result root cloning could not resolve union variants for field '{}:{}'".format(
                                        field_name,
                                        field_type,
                                    )
                                )
                            expr_lines = ["match root.{}_type() {{".format(accessor_name)]
                            # flatc's own generated discriminant
                            # enum (`union_type_path::{alias}`, matched
                            # against `root.{accessor}_type()`) AND its
                            # per-variant accessor method
                            # (`{accessor}_as_{snake(alias)}()`) are both
                            # keyed by the flatc-assigned ALIAS
                            # (idl_gen_rust.cpp: `U_ELEMENT_NAME` <-
                            # `namer_.Function(ev.name)`), never the target's
                            # bare name. `_owned_root_helper` clones the
                            # TARGET table.
                            for alias, target in variants:
                                if target not in table_fields_by_name:
                                    raise GenerationError(
                                        "Rust action get_result root cloning only supports local union table variants; field '{}:{}' variant '{}' (target '{}') is unsupported".format(
                                            field_name,
                                            field_type,
                                            alias,
                                            target,
                                        )
                                    )
                                child_helper = _owned_root_helper(target)
                                expr_lines.append(
                                    "    {}::{} => root.{}_as_{}().map(|value| {}::{}(Box::new({}(value)))),".format(
                                        union_type_path,
                                        alias,
                                        accessor_name,
                                        camel_to_snake(alias),
                                        _owned_union_msg_type(union_name),
                                        alias,
                                        child_helper,
                                    )
                                )
                            expr_lines.append("    _ => None,")
                            expr_lines.append("}")
                            expr = "\n".join(expr_lines)
                        else:
                            raise GenerationError(
                                "unsupported Rust action get_result field '{}:{}'".format(
                                    field_name, field_type
                                )
                            )
                        if "\n" in expr:
                            parts.append(f"        {msg_field_name}: {expr},\n")
                        else:
                            parts.append(f"        {msg_field_name}: {expr},\n")
                    parts.append("    }\n")
                    parts.append("}\n")
                    pending_action_helper_defs.append("".join(parts))
                    return helper_name

                def _borrow_owned_union_helper(union_name: str) -> str:
                    helper_name = owned_union_borrow_helper_names.get(union_name)
                    if helper_name is not None:
                        return helper_name

                    helper_name = f"borrow_{sanitize_rust_ident(union_name).lower()}_owned_as_typed"
                    owned_union_borrow_helper_names[union_name] = helper_name
                    owned_ty = _owned_union_msg_type(union_name)
                    typed_ty = _typed_union_msg_type(union_name, "'a")
                    parts: list[str] = []
                    parts.append("\n")
                    parts.append(f"fn {helper_name}<'a>(msg: &'a {owned_ty}) -> {typed_ty} {{\n")
                    parts.append("    match msg {\n")
                    for alias, target in all_union_defs[union_name]:
                        child_helper = _borrow_owned_msg_helper(target)
                        parts.append(
                            f"        {owned_ty}::{alias}(value) => typed::{union_name}Msg::{alias}(Box::new({child_helper}(value.as_ref()))),\n"
                        )
                    parts.append("    }\n")
                    parts.append("}\n")
                    pending_action_helper_defs.append("".join(parts))
                    return helper_name

                def _borrow_owned_msg_helper(table_name: str) -> str:
                    helper_name = owned_msg_borrow_helper_names.get(table_name)
                    if helper_name is not None:
                        return helper_name

                    helper_name = f"borrow_{sanitize_rust_ident(table_name).lower()}_owned_as_typed"
                    owned_msg_borrow_helper_names[table_name] = helper_name
                    owned_ty = _owned_msg_type(table_name)
                    typed_ty = _action_msg_type(table_name, "'a")
                    fields = table_fields_by_name[table_name]
                    parts: list[str] = []
                    parts.append("\n")
                    parts.append(f"fn {helper_name}<'a>(msg: &'a {owned_ty}) -> {typed_ty} {{\n")
                    parts.append(f"    typed::{table_name}Msg {{\n")
                    for field in fields:
                        if is_header_field(field):
                            continue
                        field_name = str(field["name"])
                        msg_field_name = rust_msg_field_name(field_name)
                        field_type = str(field["type"])
                        _, category = rust_msg_field_type(
                            field_type,
                            namespace_full,
                            gen_mod,
                            all_struct_names,
                            all_table_names,
                            all_union_names,
                            all_enum_bases,
                            include_type_map,
                        )
                        if category in (
                            "scalar",
                            "enum",
                            "struct",
                            "string",
                            "vector",
                            "vector_enum",
                            "vector_string",
                        ):
                            expr = f"msg.{msg_field_name}.clone()"
                        elif category == "vector_scalar":
                            expr = f"msg.{msg_field_name}.as_deref()"
                        elif category == "table":
                            child_table = local_type_base(field_type)
                            child_helper = _borrow_owned_msg_helper(child_table)
                            expr = (
                                f"msg.{msg_field_name}.as_ref().map(|value| Box::new({child_helper}(value.as_ref())))"
                            )
                        elif category == "vector_table":
                            child_table = local_type_base(
                                split_vector_type(field_type) or ""
                            )
                            child_helper = _borrow_owned_msg_helper(child_table)
                            expr = (
                                f"msg.{msg_field_name}.as_ref().map(|values| values.iter().map(|value| {child_helper}(value)).collect())"
                            )
                        elif category == "union":
                            union_name = local_type_base(field_type)
                            union_helper = _borrow_owned_union_helper(union_name)
                            expr = (
                                f"msg.{msg_field_name}.as_ref().map(|value| {union_helper}(value))"
                            )
                        else:
                            raise GenerationError(
                                "unsupported Rust action owned borrow field '{}:{}'".format(
                                    field_name,
                                    field_type,
                                )
                            )
                        parts.append(f"        {msg_field_name}: {expr},\n")
                    parts.append("    }\n")
                    parts.append("}\n")
                    pending_action_helper_defs.append("".join(parts))
                    return helper_name

                feedback_owned_msg_clone_helper = _owned_msg_clone_helper(
                    action_binding["feedback_data"]
                )
                result_owned_msg_clone_helper = _owned_msg_clone_helper(
                    action_binding["result"]
                )

                feedback_owned_root_helper = _owned_root_helper(
                    action_binding["feedback_data"]
                )
                result_owned_root_helper = _owned_root_helper(action_binding["result"])
                feedback_owned_borrow_helper = _borrow_owned_msg_helper(
                    action_binding["feedback_data"]
                )
                result_owned_borrow_helper = _borrow_owned_msg_helper(
                    action_binding["result"]
                )

                # Every helper body has now been collected (children before their
                # parents); emit them as contiguous free fns at module scope.
                for helper_def in pending_action_helper_defs:
                    f.write(helper_def)

                owned_feedback_msg_ty = _owned_msg_type(action_binding["feedback_data"])
                owned_result_msg_ty = _owned_msg_type(action_binding["result"])
                get_result_request_builder_fn = build_fn_map[
                    action_binding["get_result_request"]
                ]
                get_result_response_builder_fn = build_fn_map[
                    action_binding["get_result_response"]
                ]

                if namespace_rust:
                    get_result_request_path = (
                        f"{gen_mod}::{namespace_rust}::{action_binding['get_result_request']}"
                    )
                    get_result_response_path = (
                        f"{gen_mod}::{namespace_rust}::{action_binding['get_result_response']}"
                    )
                else:
                    get_result_request_path = (
                        f"{gen_mod}::{action_binding['get_result_request']}"
                    )
                    get_result_response_path = (
                        f"{gen_mod}::{action_binding['get_result_response']}"
                    )

                f.write("\n")
                f.write(
                    f"impl aorta_core::action::ActionWire for {action_binding['action_marker']} {{\n"
                )
                f.write(f"    type GoalMsg<'a> = {goal_msg_ty};\n")
                f.write(f"    type OwnedFeedbackDataMsg = {owned_feedback_msg_ty};\n")
                f.write(f"    type OwnedResultMsg = {owned_result_msg_ty};\n")
                f.write(f"    type ResultMsg<'a> = {result_msg_ty};\n")
                f.write(f"    type FeedbackDataMsg<'a> = {feedback_msg_ty};\n\n")

                f.write("    fn encode_send_goal_request(\n")
                f.write("        goal_id: &str,\n")
                f.write("        goal: &Self::GoalMsg<'_>,\n")
                f.write("    ) -> aorta_core::error::AortaResult<Vec<u8>> {\n")
                f.write("        let msg = typed::")
                f.write(action_binding["send_goal_request"])
                f.write("Msg {\n")
                f.write("            goal_id: Some(goal_id.to_string()),\n")
                f.write("            goal: Some(Box::new(goal.clone())),\n")
                f.write("        };\n")
                f.write("        let mut builder = flatbuffers::FlatBufferBuilder::new();\n")
                f.write(
                    f"        let root = {send_goal_request_builder_fn}(&mut builder, None, &msg);\n"
                )
                f.write("        builder.finish(root, None);\n")
                f.write("        Ok(builder.finished_data().to_vec())\n")
                f.write("    }\n\n")

                f.write(
                    "    fn decode_send_goal_request_goal_id(payload: &[u8]) -> aorta_core::error::AortaResult<String> {\n"
                )
                f.write(
                    f"        let root = <schema::{sanitize_rust_ident(action_binding['send_goal_request'])}Schema as aorta_core::schema::TypedSubscribeMessage>::decode_typed_root(payload)?;\n"
                )
                f.write(
                    "        root.goal_id().map(|value| value.to_string()).ok_or_else(|| aorta_core::error::AortaError::invalid_argument(\"action send_goal request missing goal_id\"))\n"
                )
                f.write("    }\n\n")

                f.write(
                    "    fn decode_goal_from_send_goal_request<'a>(\n        payload: &'a [u8],\n    ) -> aorta_core::error::AortaResult<<Self::Goal as aorta_core::schema::TypedSubscribeMessage>::Root<'a>> {\n"
                )
                f.write(
                    f"        let root = aorta_core::schema::decode_flatbuffer_root::<{send_goal_request_path}<'a>>(payload)?;\n"
                )
                f.write(
                    "        root.goal().ok_or_else(|| aorta_core::error::AortaError::invalid_argument(\"action send_goal request missing goal payload\"))\n"
                )
                f.write("    }\n\n")

                f.write(
                    "    fn encode_send_goal_response(\n        response: &aorta_core::action::ActionSendGoalResponse,\n    ) -> aorta_core::error::AortaResult<Vec<u8>> {\n"
                )
                f.write("        let msg = typed::")
                f.write(action_binding["send_goal_response"])
                f.write("Msg {\n")
                f.write("            accepted: Some(response.accepted),\n")
                f.write("            goal_id: Some(response.goal_id.clone()),\n")
                f.write("            goal_secret: Some(response.goal_secret.clone()),\n")
                f.write(
                    "            code: Some(crate::action_common_generated::aorta::action::common::ActionReplyCode(response.code as i16)),\n"
                )
                f.write(
                    "            status: Some(crate::action_common_generated::aorta::action::common::ActionStatus(response.status as i8)),\n"
                )
                f.write("            message: Some(response.message.clone()),\n")
                f.write("            server_epoch: Some(response.server_epoch.clone()),\n")
                f.write("        };\n")
                f.write("        let mut builder = flatbuffers::FlatBufferBuilder::new();\n")
                f.write(
                    f"        let root = {send_goal_response_builder_fn}(&mut builder, None, &msg);\n"
                )
                f.write("        builder.finish(root, None);\n")
                f.write("        Ok(builder.finished_data().to_vec())\n")
                f.write("    }\n\n")

                f.write(
                    "    fn decode_send_goal_response(payload: &[u8]) -> aorta_core::error::AortaResult<aorta_core::action::ActionSendGoalResponse> {\n"
                )
                f.write(
                    f"        let root = aorta_core::schema::decode_flatbuffer_root::<{send_goal_response_path}<'_>>(payload)?;\n"
                )
                f.write("        Ok(aorta_core::action::ActionSendGoalResponse {\n")
                f.write(
                    "            accepted: root.accepted(),\n"
                    "            goal_id: root.goal_id().unwrap_or(\"\").to_string(),\n"
                    "            goal_secret: root.goal_secret().unwrap_or(\"\").to_string(),\n"
                    "            code: aorta_core::action::ActionReplyCode::try_from(root.code().0)?,\n"
                    "            status: aorta_core::action::ActionStatus::try_from(root.status().0)?,\n"
                    "            message: root.message().unwrap_or(\"\").to_string(),\n"
                    "            server_epoch: root.server_epoch().unwrap_or(\"\").to_string(),\n"
                )
                f.write("        })\n")
                f.write("    }\n\n")

                f.write(
                    "    fn encode_cancel_goal_request(\n        goal_id: &str,\n        goal_secret: &str,\n    ) -> aorta_core::error::AortaResult<Vec<u8>> {\n"
                )
                f.write("        let msg = typed::")
                f.write(action_binding["cancel_goal_request"])
                f.write("Msg {\n")
                f.write("            goal_id: Some(goal_id.to_string()),\n")
                f.write("            goal_secret: Some(goal_secret.to_string()),\n")
                f.write("        };\n")
                f.write("        let mut builder = flatbuffers::FlatBufferBuilder::new();\n")
                f.write(
                    f"        let root = {cancel_goal_request_builder_fn}(&mut builder, None, &msg);\n"
                )
                f.write("        builder.finish(root, None);\n")
                f.write("        Ok(builder.finished_data().to_vec())\n")
                f.write("    }\n\n")

                f.write(
                    "    fn decode_cancel_goal_request(payload: &[u8]) -> aorta_core::error::AortaResult<(String, String)> {\n"
                )
                f.write(
                    f"        let root = aorta_core::schema::decode_flatbuffer_root::<{cancel_goal_request_path}<'_>>(payload)?;\n"
                )
                f.write("        Ok((\n")
                f.write("            root.goal_id().unwrap_or(\"\").to_string(),\n")
                f.write("            root.goal_secret().unwrap_or(\"\").to_string(),\n")
                f.write("        ))\n")
                f.write("    }\n\n")

                f.write(
                    "    fn encode_cancel_goal_response(\n        response: &aorta_core::action::ActionCancelGoalResponse,\n    ) -> aorta_core::error::AortaResult<Vec<u8>> {\n"
                )
                f.write("        let msg = typed::")
                f.write(action_binding["cancel_goal_response"])
                f.write("Msg {\n")
                f.write("            accepted: Some(response.accepted),\n")
                f.write("            goal_id: Some(response.goal_id.clone()),\n")
                f.write(
                    "            code: Some(crate::action_common_generated::aorta::action::common::ActionReplyCode(response.code as i16)),\n"
                )
                f.write(
                    "            status: Some(crate::action_common_generated::aorta::action::common::ActionStatus(response.status as i8)),\n"
                )
                f.write("            message: Some(response.message.clone()),\n")
                f.write("        };\n")
                f.write("        let mut builder = flatbuffers::FlatBufferBuilder::new();\n")
                f.write(
                    f"        let root = {cancel_goal_response_builder_fn}(&mut builder, None, &msg);\n"
                )
                f.write("        builder.finish(root, None);\n")
                f.write("        Ok(builder.finished_data().to_vec())\n")
                f.write("    }\n\n")

                f.write(
                    "    fn decode_cancel_goal_response(payload: &[u8]) -> aorta_core::error::AortaResult<aorta_core::action::ActionCancelGoalResponse> {\n"
                )
                f.write(
                    f"        let root = aorta_core::schema::decode_flatbuffer_root::<{cancel_goal_response_path}<'_>>(payload)?;\n"
                )
                f.write("        Ok(aorta_core::action::ActionCancelGoalResponse {\n")
                f.write(
                    "            accepted: root.accepted(),\n"
                    "            goal_id: root.goal_id().unwrap_or(\"\").to_string(),\n"
                    "            code: aorta_core::action::ActionReplyCode::try_from(root.code().0)?,\n"
                    "            status: aorta_core::action::ActionStatus::try_from(root.status().0)?,\n"
                    "            message: root.message().unwrap_or(\"\").to_string(),\n"
                )
                f.write("        })\n")
                f.write("    }\n\n")

                f.write(
                    "    fn encode_get_result_request(\n        goal_id: &str,\n        goal_secret: &str,\n    ) -> aorta_core::error::AortaResult<Vec<u8>> {\n"
                )
                f.write("        let msg = typed::")
                f.write(action_binding["get_result_request"])
                f.write("Msg {\n")
                f.write("            goal_id: Some(goal_id.to_string()),\n")
                f.write("            goal_secret: Some(goal_secret.to_string()),\n")
                f.write("        };\n")
                f.write("        let mut builder = flatbuffers::FlatBufferBuilder::new();\n")
                f.write(
                    f"        let root = {get_result_request_builder_fn}(&mut builder, None, &msg);\n"
                )
                f.write("        builder.finish(root, None);\n")
                f.write("        Ok(builder.finished_data().to_vec())\n")
                f.write("    }\n\n")

                f.write(
                    "    fn decode_get_result_request(payload: &[u8]) -> aorta_core::error::AortaResult<(String, String)> {\n"
                )
                f.write(
                    f"        let root = aorta_core::schema::decode_flatbuffer_root::<{get_result_request_path}<'_>>(payload)?;\n"
                )
                f.write("        Ok((\n")
                f.write("            root.goal_id().unwrap_or(\"\").to_string(),\n")
                f.write("            root.goal_secret().unwrap_or(\"\").to_string(),\n")
                f.write("        ))\n")
                f.write("    }\n\n")

                f.write(
                    "    fn clone_feedback_data_owned(\n        data: &Self::FeedbackDataMsg<'_>,\n    ) -> aorta_core::error::AortaResult<Self::OwnedFeedbackDataMsg> {\n"
                )
                f.write(f"        Ok({feedback_owned_msg_clone_helper}(data))\n")
                f.write("    }\n\n")

                f.write(
                    "    fn clone_result_owned(\n        result: &Self::ResultMsg<'_>,\n    ) -> aorta_core::error::AortaResult<Self::OwnedResultMsg> {\n"
                )
                f.write(f"        Ok({result_owned_msg_clone_helper}(result))\n")
                f.write("    }\n\n")

                f.write(
                    "    fn encode_get_result_response(\n        response: &aorta_core::action::ActionGetResultResponse<Self::OwnedFeedbackDataMsg, Self::OwnedResultMsg>,\n    ) -> aorta_core::error::AortaResult<Vec<u8>> {\n"
                )
                f.write("        let msg = typed::")
                f.write(action_binding["get_result_response"])
                f.write("Msg {\n")
                f.write("            goal_id: Some(response.goal_id.clone()),\n")
                f.write(
                    "            status: Some(crate::action_common_generated::aorta::action::common::ActionStatus(response.status as i8)),\n"
                )
                f.write("            accepted_stamp_ns: Some(response.accepted_stamp_ns),\n")
                f.write("            started_stamp_ns: Some(response.started_stamp_ns),\n")
                f.write("            finished_stamp_ns: Some(response.finished_stamp_ns),\n")
                f.write("            last_feedback_seq: Some(response.last_feedback_seq),\n")
                f.write(
                    f"            feedback_snapshot: Some(Box::new({feedback_owned_borrow_helper}(&response.feedback_snapshot))),\n"
                )
                f.write(
                    f"            result: response.result.as_ref().map(|value| Box::new({result_owned_borrow_helper}(value))),\n"
                )
                f.write(
                    "            code: Some(crate::action_common_generated::aorta::action::common::ActionReplyCode(response.code as i16)),\n"
                )
                f.write("            message: Some(response.message.clone()),\n")
                f.write("            server_epoch: Some(response.server_epoch.clone()),\n")
                f.write("        };\n")
                f.write("        let mut builder = flatbuffers::FlatBufferBuilder::new();\n")
                f.write(
                    f"        let root = {get_result_response_builder_fn}(&mut builder, None, &msg);\n"
                )
                f.write("        builder.finish(root, None);\n")
                f.write("        Ok(builder.finished_data().to_vec())\n")
                f.write("    }\n\n")

                f.write(
                    "    fn decode_get_result_response(payload: &[u8]) -> aorta_core::error::AortaResult<aorta_core::action::ActionGetResultResponse<Self::OwnedFeedbackDataMsg, Self::OwnedResultMsg>> {\n"
                )
                f.write(
                    f"        let root = aorta_core::schema::decode_flatbuffer_root::<{get_result_response_path}<'_>>(payload)?;\n"
                )
                f.write("        Ok(aorta_core::action::ActionGetResultResponse {\n")
                f.write(
                    "            goal_id: root.goal_id().unwrap_or(\"\").to_string(),\n"
                    "            status: aorta_core::action::ActionStatus::try_from(root.status().0)?,\n"
                    "            accepted_stamp_ns: root.accepted_stamp_ns(),\n"
                    "            started_stamp_ns: root.started_stamp_ns(),\n"
                    "            finished_stamp_ns: root.finished_stamp_ns(),\n"
                    "            last_feedback_seq: root.last_feedback_seq(),\n"
                )
                f.write(
                    f"            feedback_snapshot: root.feedback_snapshot().map({feedback_owned_root_helper}).unwrap_or_default(),\n"
                )
                f.write(
                    f"            result: root.result().map({result_owned_root_helper}),\n"
                )
                f.write(
                    "            code: aorta_core::action::ActionReplyCode::try_from(root.code().0)?,\n"
                    "            message: root.message().unwrap_or(\"\").to_string(),\n"
                    "            server_epoch: root.server_epoch().unwrap_or(\"\").to_string(),\n"
                )
                f.write("        })\n")
                f.write("    }\n\n")

                f.write(
                    "    fn encode_feedback(\n        goal_id: &str,\n        seq: u64,\n        stamp_ns: u64,\n        status: aorta_core::action::ActionStatus,\n        data: &Self::FeedbackDataMsg<'_>,\n        message: Option<&str>,\n    ) -> aorta_core::error::AortaResult<Vec<u8>> {\n"
                )
                f.write("        let msg = typed::")
                f.write(action_binding["feedback"])
                f.write("Msg {\n")
                f.write("            goal_id: Some(goal_id.to_string()),\n")
                f.write("            seq: Some(seq),\n")
                f.write("            stamp_ns: Some(stamp_ns),\n")
                f.write(
                    "            status: Some(crate::action_common_generated::aorta::action::common::ActionStatus(status as i8)),\n"
                )
                f.write("            data: Some(Box::new(data.clone())),\n")
                f.write("            message: message.map(|value| value.to_string()),\n")
                f.write("        };\n")
                f.write("        let mut builder = flatbuffers::FlatBufferBuilder::new();\n")
                f.write(
                    f"        let root = {feedback_builder_fn}(&mut builder, None, &msg);\n"
                )
                f.write("        builder.finish(root, None);\n")
                f.write("        Ok(builder.finished_data().to_vec())\n")
                f.write("    }\n\n")

                f.write(
                    "    fn decode_feedback_goal_id(payload: &[u8]) -> aorta_core::error::AortaResult<String> {\n"
                )
                f.write(
                    f"        let root = <schema::{sanitize_rust_ident(action_binding['feedback'])}Schema as aorta_core::schema::TypedSubscribeMessage>::decode_typed_root(payload)?;\n"
                )
                f.write(
                    "        root.goal_id().map(|value| value.to_string()).ok_or_else(|| aorta_core::error::AortaError::invalid_argument(\"action feedback missing goal_id\"))\n"
                )
                f.write("    }\n\n")

                f.write(
                    "    fn encode_feedback_data(data: &Self::FeedbackDataMsg<'_>) -> aorta_core::error::AortaResult<Vec<u8>> {\n"
                )
                f.write("        let mut builder = flatbuffers::FlatBufferBuilder::new();\n")
                f.write(
                    f"        let root = {feedback_data_builder_fn}(&mut builder, None, data);\n"
                )
                f.write("        builder.finish(root, None);\n")
                f.write("        Ok(builder.finished_data().to_vec())\n")
                f.write("    }\n\n")

                f.write(
                    "    fn encode_result(result: &Self::ResultMsg<'_>) -> aorta_core::error::AortaResult<Vec<u8>> {\n"
                )
                f.write("        let mut builder = flatbuffers::FlatBufferBuilder::new();\n")
                f.write(
                    f"        let root = {result_builder_fn}(&mut builder, None, result);\n"
                )
                f.write("        builder.finish(root, None);\n")
                f.write("        Ok(builder.finished_data().to_vec())\n")
                f.write("    }\n\n")

                f.write(
                    "    fn encode_public_status(\n        status: &aorta_core::action::ActionPublicStatus,\n    ) -> aorta_core::error::AortaResult<Vec<u8>> {\n"
                )
                f.write("        let msg = typed::")
                f.write(action_binding["public_status"])
                f.write("Msg {\n")
                f.write("            goal_id: Some(status.goal_id.clone()),\n")
                f.write(
                    "            status: Some(crate::action_common_generated::aorta::action::common::ActionStatus(status.status as i8)),\n"
                )
                f.write(
                    "            code: Some(crate::action_common_generated::aorta::action::common::ActionReplyCode(status.code as i16)),\n"
                )
                f.write("            accepted_stamp_ns: Some(status.accepted_stamp_ns),\n")
                f.write("            started_stamp_ns: Some(status.started_stamp_ns),\n")
                f.write("            finished_stamp_ns: Some(status.finished_stamp_ns),\n")
                f.write("            last_feedback_seq: Some(status.last_feedback_seq),\n")
                f.write("            server_epoch: Some(status.server_epoch.clone()),\n")
                f.write("        };\n")
                f.write("        let mut builder = flatbuffers::FlatBufferBuilder::new();\n")
                f.write(
                    f"        let root = {public_status_builder_fn}(&mut builder, None, &msg);\n"
                )
                f.write("        builder.finish(root, None);\n")
                f.write("        Ok(builder.finished_data().to_vec())\n")
                f.write("    }\n\n")

                f.write(
                    "    fn decode_public_status(payload: &[u8]) -> aorta_core::error::AortaResult<aorta_core::action::ActionPublicStatus> {\n"
                )
                f.write(
                    f"        let root = aorta_core::schema::decode_flatbuffer_root::<{public_status_path}<'_>>(payload)?;\n"
                )
                f.write("        Ok(aorta_core::action::ActionPublicStatus {\n")
                f.write(
                    "            goal_id: root.goal_id().unwrap_or(\"\").to_string(),\n"
                    "            status: aorta_core::action::ActionStatus::try_from(root.status().0)?,\n"
                    "            code: aorta_core::action::ActionReplyCode::try_from(root.code().0)?,\n"
                    "            accepted_stamp_ns: root.accepted_stamp_ns(),\n"
                    "            started_stamp_ns: root.started_stamp_ns(),\n"
                    "            finished_stamp_ns: root.finished_stamp_ns(),\n"
                    "            last_feedback_seq: root.last_feedback_seq(),\n"
                    "            server_epoch: root.server_epoch().unwrap_or(\"\").to_string(),\n"
                )
                f.write("        })\n")
                f.write("    }\n")
                f.write("}\n")

        f.write("\n")
        f.write("pub mod prelude {\n")
        f.write("    pub use super::aorta;\n")
        if top_mod != "aorta":
            f.write(f"    pub use super::{top_mod};\n")
        else:
            f.write("    pub use super::schema_aorta as messages;\n")
        if local_type_names:
            f.write("    pub use super::schema::*;\n")
        if action_binding is not None:
            f.write(f"    pub use super::{action_binding['action_marker']};\n")
        if all_tables or all_union_defs:
            f.write("    pub use super::typed::*;\n")
        f.write(f"    pub use super::{const_prefix}_BFBS;\n")
        f.write(f"    pub use super::{const_prefix}_FIXED_SIZE;\n")
        f.write("}\n")


def write_cpp(
    out: str,
    stem: str,
    schema_rel: str,
    root_fbs: str,
    action_name: str | None = None,
) -> None:
    # Semantic facts come from the bfbs loader (the
    # `emit_*` dispatchers). See the matching comment in `write_rust`.
    root_doc = emit_root_schema_doc(root_fbs)
    include_paths = root_doc.include_paths
    root_table_names = set(root_doc.table_fields.keys())
    root_union_names = set(root_doc.union_defs.keys())
    root_type = emit_root_type(root_fbs)
    include_type_map = emit_include_type_map(root_fbs, include_paths)
    include_enum_bases = emit_include_enum_bases(root_fbs, include_paths)
    namespace_full = root_doc.namespace_full
    detail_ns = cpp_detail_namespace(stem, namespace_full)
    all_union_defs = emit_union_defs(root_fbs)
    all_union_names = set(all_union_defs.keys())
    all_enum_bases = emit_enum_bases(root_fbs)
    all_table_defs = emit_table_defs(root_fbs)
    all_table_names = set(all_table_defs.keys())
    all_struct_names = emit_struct_names(root_fbs)
    action_binding = derive_action_binding_names(
        schema_rel, stem, root_table_names, action_name
    )
    build_fn_map = {
        table_name: "Build{}Typed".format(sanitize_rust_ident(table_name))
        for table_name in all_table_defs.keys()
    }
    union_build_fn_map = {
        union_name: "Build{}UnionTyped".format(sanitize_rust_ident(union_name))
        for union_name in all_union_defs.keys()
    }
    all_tables: list[tuple[str, list[dict[str, str | None]]]] = list(
        (table_name, table_def.fields)
        for table_name, table_def in all_table_defs.items()
    )
    # Same canonical-source fix as write_rust's typed_tables above -- derive
    # from `all_table_defs` (already in qualified (namespace, name) order),
    # filtered to root-local tables with an `aorta_header` field, instead of
    # iterating `root_doc.table_fields.items()` (source-declaration order),
    # which drove C++ `TypedPublishTraits<T>` specialization emission order.
    typed_tables: list[tuple[str, list[dict[str, str | None]]]] = []
    for table_name, table_def in all_table_defs.items():
        if table_name not in root_table_names:
            continue
        if any(is_header_field(field) for field in table_def.fields):
            typed_tables.append((table_name, table_def.fields))

    # Type-support matrix enforcement: C++ typed_build supports
    # every field category (including vector-of-union, which all_types_cpp_vector_union.fbs
    # round-trips), so this sweep never fires for a valid schema today -- it is
    # the explicit, symmetric counterpart to write_rust's sweep and the single
    # place a future C++-typed_build UNSUPPORTED cell would be enforced. The
    # C++-specific hole (vector-of-union in the action copy helper) is enforced
    # at that helper's own field loop below.
    for _mx_table_name, _mx_fields in all_tables:
        for _mx_field in _mx_fields:
            if is_header_field(_mx_field):
                continue
            _mx_type = str(_mx_field["type"])
            require_field_supported(
                logical_field_category(
                    _mx_type,
                    all_struct_names,
                    all_table_names,
                    all_union_names,
                    all_enum_bases,
                    include_type_map,
                ),
                "cpp",
                "typed_build",
                stem,
                _mx_table_name,
                str(_mx_field["name"]),
                _mx_type,
            )

    with open(out, "w", encoding="utf-8") as f:
        f.write("#pragma once\n\n")
        f.write("#include <cstddef>\n")
        f.write("#include <cstdint>\n")
        f.write("#include <memory>\n")
        f.write("#include <optional>\n")
        f.write("#include <string>\n")
        f.write("#include <utility>\n")
        f.write("#include <variant>\n")
        f.write("#include <vector>\n\n")
        f.write('#include "aorta/array_view.h"\n')
        # Primary-template visibility: with include-table
        # TypedSubscribeTraits specializations gone (see subscribe_tables
        # below), a TU that includes only this wrapper header and calls
        # `TypedSubscribeTraits<IncludedTable>` needs the PRIMARY TEMPLATE
        # (generic VerifyBuffer<T>/GetRoot<T> body) to still be visible, not
        # just a forward declaration. typed_subscribe.h provides that
        # definition; typed_publish.h only forward-declares TypedPublishTraits
        # (its specializations are always root-local -- see `typed_tables`
        # above -- so no analogous visibility gap exists there, but the
        # forward declaration below is kept since typed_publish.h itself only
        # forward-declares it too).
        f.write('#include "aorta/typed_subscribe.h"\n')
        f.write("// IWYU pragma: begin_exports\n")
        f.write(f'#include "{stem}_generated.h"\n')
        f.write(f'#include "{stem}_schema_meta.h"\n')
        f.write("// IWYU pragma: end_exports\n")
        f.write("\n")
        f.write("namespace aorta {\n")
        f.write("template <typename T>\n")
        f.write("struct TypedPublishTraits;\n")
        f.write("}  // namespace aorta\n\n")

        if not all_tables and not all_union_defs:
            return

        ns_parts = namespace_full.split(".") if namespace_full else []

        if ns_parts:
            for part in ns_parts:
                f.write(f"namespace {part} {{\n")
            f.write("\n")

        for table_name, _ in all_tables:
            msg_name = f"{table_name}Msg"
            f.write(f"struct {msg_name};\n")
        f.write("\n")

        # TABLE_MSG / UNION_MSG guards are unconditional (root tables
        # /unions get the same guard as include tables/unions), not just when
        # `is_include_union`/`is_include_table`. Include-table Msg structs
        # cannot be dropped (nested Msg fields reference them), so symmetry --
        # not owner-side-unguarded -- is the correct fix here: previously an
        # owner's root-local (unguarded) Msg struct collided with an
        # includer's guarded re-emission of the identical symbol. Guard macro
        # naming is unchanged.
        for union_name, variants in all_union_defs.items():
            union_msg_name = f"{union_name}Msg"
            union_guard = cpp_guard_macro("UNION_MSG", namespace_full, union_msg_name)
            f.write(f"#ifndef {union_guard}\n")
            f.write(f"#define {union_guard} 1\n")
            f.write(f"using {union_msg_name} = std::variant<\n")
            f.write("    std::monostate")
            # This `std::variant` is TYPE-keyed (dispatched via
            # std::holds_alternative<std::shared_ptr<TargetMsg>>), so only
            # the TARGET matters here -- the alias has no representation in
            # this alternative-type list (see the discriminant-tag sites
            # below, which DO need the alias).
            for alias, target in variants:
                if target not in all_table_names:
                    raise GenerationError(
                        "union variant '{}.{}' (target '{}') is not supported by typed wrapper generation (only local table variants are supported)".format(
                            union_name,
                            alias,
                            target,
                        ),
                    )
                f.write(",\n")
                f.write(f"    std::shared_ptr<{target}Msg>")
            f.write(">;\n")
            f.write(f"#endif  // {union_guard}\n")
            f.write("\n")

        for table_name, fields in all_tables:
            msg_name = f"{table_name}Msg"
            table_guard = cpp_guard_macro("TABLE_MSG", namespace_full, msg_name)
            f.write(f"#ifndef {table_guard}\n")
            f.write(f"#define {table_guard} 1\n")
            f.write(f"struct {msg_name} {{\n")
            for field in fields:
                if is_header_field(field):
                    continue
                field_name = str(field["name"])
                field_type = str(field["type"])
                cpp_type, _ = cpp_msg_field_type(
                    field_type,
                    namespace_full,
                    all_struct_names,
                    all_table_names,
                    all_union_names,
                    all_enum_bases,
                    include_type_map,
                )
                f.write(f"  std::optional<{cpp_type}> {field_name};\n")
            f.write("};\n")
            f.write(f"#endif  // {table_guard}\n")
            f.write("\n")

        if ns_parts:
            for part in reversed(ns_parts):
                f.write(f"}}  // namespace {part}\n")
            f.write("\n")

        f.write("namespace aorta {\n")
        f.write("namespace detail {\n\n")
        f.write("#ifndef AORTA_DETAIL_SCRATCH_VECTOR_DEFINED\n")
        f.write("#define AORTA_DETAIL_SCRATCH_VECTOR_DEFINED 1\n")
        f.write("template <typename T>\n")
        f.write("class ScratchVector {\n")
        f.write(" public:\n")
        f.write("  ScratchVector() : data_(Take()) { data_.clear(); }\n")
        f.write("  ~ScratchVector() { Pool().push_back(std::move(data_)); }\n")
        f.write("  std::vector<T>& get() { return data_; }\n\n")
        f.write(" private:\n")
        f.write("  static std::vector<T> Take() {\n")
        f.write("    auto& pool = Pool();\n")
        f.write("    if (pool.empty()) {\n")
        f.write("      return {};\n")
        f.write("    }\n")
        f.write("    auto out = std::move(pool.back());\n")
        f.write("    pool.pop_back();\n")
        f.write("    return out;\n")
        f.write("  }\n\n")
        f.write("  static std::vector<std::vector<T>>& Pool() {\n")
        f.write("    thread_local std::vector<std::vector<T>> pool;\n")
        f.write("    return pool;\n")
        f.write("  }\n\n")
        f.write("  std::vector<T> data_;\n")
        f.write("};\n")
        f.write("#endif  // AORTA_DETAIL_SCRATCH_VECTOR_DEFINED\n\n")
        f.write(f"namespace {detail_ns} {{\n\n")

        # Forward declarations for mutually-recursive table/union helper builders.
        for table_name in all_table_defs.keys():
            msg_name = f"{table_name}Msg"
            helper_fn = build_fn_map[table_name]
            table_def = all_table_defs[table_name]
            table_ns_parts = (
                table_def.namespace_full.split(".") if table_def.namespace_full else []
            )
            fq_table = (
                "::" + "::".join(table_ns_parts + [table_name])
                if table_ns_parts
                else f"::{table_name}"
            )
            fq_msg = (
                "::" + "::".join(ns_parts + [msg_name]) if ns_parts else f"::{msg_name}"
            )
            f.write(f"inline flatbuffers::Offset<{fq_table[2:]}> {helper_fn}(\n")
            f.write("    flatbuffers::FlatBufferBuilder& builder,\n")
            f.write(f"    const {fq_msg}& msg,\n")
            f.write("    flatbuffers::Offset<aorta::sys::AortaHeader> header);\n")
        if all_table_defs:
            f.write("\n")

        for union_name in all_union_defs.keys():
            union_helper_fn = union_build_fn_map[union_name]
            union_cpp_type = qualify_cpp_user_type(
                union_name,
                namespace_full,
                include_type_map,
            )
            union_msg_type = "::" + "::".join(ns_parts + [f"{union_name}Msg"])
            f.write(
                f"inline std::pair<{union_cpp_type}, flatbuffers::Offset<void>> {union_helper_fn}(\n",
            )
            f.write("    flatbuffers::FlatBufferBuilder& builder,\n")
            f.write(f"    const {union_msg_type}& msg);\n")
        if all_union_defs:
            f.write("\n")

        copy_helper_fn_map = {
            table_name: "Copy{}Msg".format(sanitize_rust_ident(table_name))
            for table_name in all_table_defs.keys()
        }
        if action_binding is not None:
            for table_name in all_table_defs.keys():
                msg_name = f"{table_name}Msg"
                table_def = all_table_defs[table_name]
                table_ns_parts = (
                    table_def.namespace_full.split(".") if table_def.namespace_full else []
                )
                fq_table = (
                    "::" + "::".join(table_ns_parts + [table_name])
                    if table_ns_parts
                    else f"::{table_name}"
                )
                fq_msg = (
                    "::" + "::".join(ns_parts + [msg_name])
                    if ns_parts
                    else f"::{msg_name}"
                )
                f.write(f"inline {fq_msg} {copy_helper_fn_map[table_name]}(\n")
                f.write(f"    const {fq_table}& root);\n")
            if all_table_defs:
                f.write("\n")

        for union_name, variants in all_union_defs.items():
            union_helper_fn = union_build_fn_map[union_name]
            union_cpp_type = qualify_cpp_user_type(
                union_name,
                namespace_full,
                include_type_map,
            )
            union_msg_type = "::" + "::".join(ns_parts + [f"{union_name}Msg"])
            none_value = f"{union_cpp_type}_{'NONE'}"
            f.write(
                f"inline std::pair<{union_cpp_type}, flatbuffers::Offset<void>> {union_helper_fn}(\n",
            )
            f.write("    flatbuffers::FlatBufferBuilder& builder,\n")
            f.write(f"    const {union_msg_type}& msg) {{\n")
            # `child_helper`/`child_msg_type` resolve the variant's
            # TARGET table (`std::variant`'s alternative type is type-keyed,
            # see the declaration site above); `variant_value` is the
            # flatc-generated discriminant CONSTANT, which is keyed by the
            # ALIAS (idl_gen_cpp.cpp: `Name(ev)` == `ev.name`, no case
            # transform for C++, unlike Rust's snake_case).
            for alias, target in variants:
                child_helper = build_fn_map.get(target)
                if child_helper is None:
                    raise GenerationError(
                        "failed to resolve helper for union variant '{}.{}' (target '{}')".format(
                            union_name,
                            alias,
                            target,
                        ),
                    )
                child_msg_type = "::" + "::".join(ns_parts + [f"{target}Msg"])
                variant_value = f"{union_cpp_type}_{alias}"
                f.write(
                    "  if (std::holds_alternative<std::shared_ptr<{}>>(msg)) {{\n".format(
                        child_msg_type,
                    ),
                )
                f.write(
                    "    const auto& value = std::get<std::shared_ptr<{}>>(msg);\n".format(
                        child_msg_type,
                    ),
                )
                f.write("    if (!value) {\n")
                f.write(
                    f"      return std::make_pair({none_value}, flatbuffers::Offset<void>());\n"
                )
                f.write("    }\n")
                f.write(f"    auto offset = {child_helper}(builder, *value, {{}});\n")
                f.write(
                    f"    return std::make_pair({variant_value}, offset.Union());\n",
                )
                f.write("  }\n")
            f.write(
                f"  return std::make_pair({none_value}, flatbuffers::Offset<void>());\n"
            )
            f.write("}\n\n")

        for table_name, fields in all_tables:
            msg_name = f"{table_name}Msg"
            helper_fn = build_fn_map[table_name]
            table_def = all_table_defs[table_name]
            table_ns_parts = (
                table_def.namespace_full.split(".") if table_def.namespace_full else []
            )
            fq_table = (
                "::" + "::".join(table_ns_parts + [table_name])
                if table_ns_parts
                else f"::{table_name}"
            )
            fq_builder = (
                "::" + "::".join(table_ns_parts + [f"{table_name}Builder"])
                if table_ns_parts
                else f"::{table_name}Builder"
            )
            fq_msg = (
                "::" + "::".join(ns_parts + [msg_name]) if ns_parts else f"::{msg_name}"
            )
            has_header = any(is_header_field(field) for field in fields)

            f.write(f"inline flatbuffers::Offset<{fq_table[2:]}> {helper_fn}(\n")
            f.write("    flatbuffers::FlatBufferBuilder& builder,\n")
            f.write(f"    const {fq_msg}& msg,\n")
            f.write("    flatbuffers::Offset<aorta::sys::AortaHeader> header) {\n")

            for field in fields:
                if is_header_field(field):
                    continue
                field_name = str(field["name"])
                field_type = str(field["type"])
                _, category = cpp_msg_field_type(
                    field_type,
                    namespace_full,
                    all_struct_names,
                    all_table_names,
                    all_union_names,
                    all_enum_bases,
                    include_type_map,
                )
                if category == "string":
                    f.write(
                        f"  flatbuffers::Offset<flatbuffers::String> {field_name}_offset;\n",
                    )
                    f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                    f.write(
                        f"    {field_name}_offset = builder.CreateString(msg.{field_name}.value());\n",
                    )
                    f.write("  }\n")
                elif category == "vector":
                    elem = split_vector_type(field_type)
                    assert elem is not None
                    elem_type, _ = cpp_msg_field_type(
                        elem,
                        namespace_full,
                        all_struct_names,
                        all_table_names,
                        all_union_names,
                        all_enum_bases,
                        include_type_map,
                    )
                    if local_type_base(elem) == "bool":
                        # Type-support matrix: a `[bool]` field's
                        # Msg storage is `std::vector<bool>`, the bit-packed
                        # standard-library specialization that has NO `.data()`
                        # member -- the plain `CreateVector(ptr, size)` path
                        # below is a probe-proven compile bomb. flatc stores
                        # a bool vector as `Vector<uint8_t>` (its accessor/builder
                        # both use `uint8_t`), so copy each element through an
                        # uninitialized `uint8_t` buffer, mirroring the
                        # `vector_enum` idiom below (which has the same
                        # element-widening shape).
                        f.write(
                            f"  flatbuffers::Offset<flatbuffers::Vector<uint8_t>> {field_name}_offset;\n",
                        )
                        f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                        f.write(
                            f"    const auto& bool_values = msg.{field_name}.value();\n"
                        )
                        f.write("    uint8_t* values = nullptr;\n")
                        f.write(
                            f"    {field_name}_offset = builder.CreateUninitializedVector<uint8_t>(bool_values.size(), &values);\n",
                        )
                        f.write(
                            "    for (std::size_t i = 0; i < bool_values.size(); ++i) {\n"
                        )
                        f.write("      values[i] = bool_values[i] ? 1 : 0;\n")
                        f.write("    }\n")
                        f.write("  }\n")
                    else:
                        f.write(
                            f"  flatbuffers::Offset<flatbuffers::Vector<{elem_type}>> {field_name}_offset;\n",
                        )
                        f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                        f.write(
                            f"    {field_name}_offset = builder.CreateVector(msg.{field_name}.value().data(), msg.{field_name}.value().size());\n",
                        )
                        f.write("  }\n")
                elif category == "vector_struct":
                    vec_elem = split_vector_type(field_type)
                    assert vec_elem is not None
                    elem_type = qualify_cpp_user_type(
                        vec_elem,
                        namespace_full,
                        include_type_map,
                    )
                    f.write(
                        f"  flatbuffers::Offset<flatbuffers::Vector<const {elem_type} *>> {field_name}_offset;\n",
                    )
                    f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                    f.write(
                        f"    {field_name}_offset = builder.CreateVectorOfStructs(msg.{field_name}.value());\n",
                    )
                    f.write("  }\n")
                elif category == "vector_enum":
                    vec_elem = split_vector_type(field_type)
                    assert vec_elem is not None
                    elem_base = local_type_base(vec_elem)
                    enum_base = all_enum_bases.get(elem_base)
                    if enum_base is None:
                        include_kind = _include_type_kind(elem_base, include_type_map)
                        if include_kind == "enum":
                            enum_base = include_enum_bases.get(elem_base)
                    if enum_base is None or enum_base not in CPP_SCALAR_TYPES:
                        raise GenerationError(
                            "failed to resolve underlying scalar for enum vector field '{}:{}'".format(
                                field_name,
                                field_type,
                            ),
                        )
                    scalar_type = CPP_SCALAR_TYPES[enum_base]
                    f.write(
                        f"  flatbuffers::Offset<flatbuffers::Vector<{scalar_type}>> {field_name}_offset;\n",
                    )
                    f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                    f.write(
                        f"    const auto& enum_values = msg.{field_name}.value();\n"
                    )
                    f.write(f"    {scalar_type}* values = nullptr;\n")
                    f.write(
                        f"    {field_name}_offset = builder.CreateUninitializedVector<{scalar_type}>(enum_values.size(), &values);\n",
                    )
                    f.write(
                        "    for (std::size_t i = 0; i < enum_values.size(); ++i) {\n"
                    )
                    f.write(
                        f"      values[i] = static_cast<{scalar_type}>(enum_values[i]);\n"
                    )
                    f.write("    }\n")
                    f.write("  }\n")
                elif category == "vector_string":
                    f.write(
                        f"  flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>> {field_name}_offset;\n",
                    )
                    f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                    f.write(
                        "    ::aorta::detail::ScratchVector<flatbuffers::Offset<flatbuffers::String>> offsets_scratch;\n"
                    )
                    f.write("    auto& offsets = offsets_scratch.get();\n")
                    f.write(
                        f"    offsets.reserve(msg.{field_name}.value().size());\n",
                    )
                    f.write(
                        f"    for (const auto& item : msg.{field_name}.value()) {{\n"
                    )
                    f.write("      offsets.push_back(builder.CreateString(item));\n")
                    f.write("    }\n")
                    f.write(
                        f"    {field_name}_offset = builder.CreateVector(offsets);\n",
                    )
                    f.write("  }\n")
                elif category == "union":
                    union_name = local_type_base(field_type)
                    union_helper_fn = union_build_fn_map.get(union_name)
                    if union_helper_fn is None:
                        raise GenerationError(
                            "failed to resolve helper for union field '{}:{}'".format(
                                field_name,
                                field_type,
                            ),
                        )
                    union_cpp_type = qualify_cpp_user_type(
                        union_name,
                        namespace_full,
                        include_type_map,
                    )
                    none_value = f"{union_cpp_type}_NONE"
                    f.write(
                        f"  {union_cpp_type} {field_name}_type_value = {none_value};\n"
                    )
                    f.write(f"  flatbuffers::Offset<void> {field_name}_value_offset;\n")
                    f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                    f.write(
                        f"    auto {field_name}_union = {union_helper_fn}(builder, msg.{field_name}.value());\n",
                    )
                    f.write(
                        f"    {field_name}_type_value = {field_name}_union.first;\n"
                    )
                    f.write(
                        f"    {field_name}_value_offset = {field_name}_union.second;\n"
                    )
                    f.write("  }\n")
                elif category == "vector_union":
                    vec_elem = split_vector_type(field_type)
                    assert vec_elem is not None
                    union_name = local_type_base(vec_elem)
                    union_helper_fn = union_build_fn_map.get(union_name)
                    if union_helper_fn is None:
                        raise GenerationError(
                            "failed to resolve helper for vector<union> field '{}:{}'".format(
                                field_name,
                                field_type,
                            ),
                        )
                    f.write(
                        f"  flatbuffers::Offset<flatbuffers::Vector<uint8_t>> {field_name}_type_offset;\n",
                    )
                    f.write(
                        f"  flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<void>>> {field_name}_offset;\n",
                    )
                    f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                    f.write("    ::aorta::detail::ScratchVector<uint8_t> type_values_scratch;\n")
                    f.write("    auto& type_values = type_values_scratch.get();\n")
                    f.write(
                        "    ::aorta::detail::ScratchVector<flatbuffers::Offset<void>> value_offsets_scratch;\n"
                    )
                    f.write("    auto& value_offsets = value_offsets_scratch.get();\n")
                    f.write(
                        f"    type_values.reserve(msg.{field_name}.value().size());\n",
                    )
                    f.write(
                        f"    value_offsets.reserve(msg.{field_name}.value().size());\n",
                    )
                    f.write(
                        f"    for (const auto& union_item : msg.{field_name}.value()) {{\n"
                    )
                    f.write(
                        f"      auto union_pair = {union_helper_fn}(builder, union_item);\n",
                    )
                    f.write(
                        "      type_values.push_back(static_cast<uint8_t>(union_pair.first));\n"
                    )
                    f.write("      value_offsets.push_back(union_pair.second);\n")
                    f.write("    }\n")
                    f.write(
                        f"    {field_name}_type_offset = builder.CreateVector(type_values);\n",
                    )
                    f.write(
                        f"    {field_name}_offset = builder.CreateVector(value_offsets);\n",
                    )
                    f.write("  }\n")
                elif category == "table":
                    child_table = local_type_base(field_type)
                    child_helper = build_fn_map.get(child_table)
                    if child_helper is None:
                        raise GenerationError(
                            f"failed to resolve helper for table field '{field_name}:{field_type}'",
                        )
                    child_cpp_type = qualify_cpp_user_type(
                        child_table,
                        namespace_full,
                        include_type_map,
                    )
                    f.write(
                        f"  flatbuffers::Offset<{child_cpp_type}> {field_name}_offset;\n",
                    )
                    f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                    f.write(f"    if (msg.{field_name}.value()) {{\n")
                    f.write(
                        f"      {field_name}_offset = {child_helper}(builder, *msg.{field_name}.value(), {{}});\n",
                    )
                    f.write("    }\n")
                    f.write("  }\n")
                elif category == "vector_table":
                    vec_elem = split_vector_type(field_type)
                    assert vec_elem is not None
                    child_table = local_type_base(vec_elem)
                    child_helper = build_fn_map.get(child_table)
                    if child_helper is None:
                        raise GenerationError(
                            f"failed to resolve helper for vector<table> field '{field_name}:{field_type}'",
                        )
                    child_cpp_type = qualify_cpp_user_type(
                        child_table,
                        namespace_full,
                        include_type_map,
                    )
                    f.write(
                        f"  flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<{child_cpp_type}>>> {field_name}_offset;\n",
                    )
                    f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                    f.write(
                        f"    ::aorta::detail::ScratchVector<flatbuffers::Offset<{child_cpp_type}>> offsets_scratch;\n",
                    )
                    f.write("    auto& offsets = offsets_scratch.get();\n")
                    f.write(
                        f"    offsets.reserve(msg.{field_name}.value().size());\n",
                    )
                    f.write(
                        f"    for (const auto& item_ptr : msg.{field_name}.value()) {{\n",
                    )
                    f.write("      if (!item_ptr) {\n")
                    f.write("        continue;\n")
                    f.write("      }\n")
                    f.write(
                        f"      offsets.push_back({child_helper}(builder, *item_ptr, {{}}));\n",
                    )
                    f.write("    }\n")
                    f.write(
                        f"    {field_name}_offset = builder.CreateVector(offsets);\n",
                    )
                    f.write("  }\n")

            f.write(f"  {fq_builder} root_builder(builder);\n")
            if has_header:
                f.write("  if (header.o != 0) {\n")
                f.write("    root_builder.add_aorta_header(header);\n")
                f.write("  }\n")
            else:
                f.write("  (void)header;\n")

            for field in fields:
                if is_header_field(field):
                    continue
                field_name = str(field["name"])
                field_type = str(field["type"])
                _, category = cpp_msg_field_type(
                    field_type,
                    namespace_full,
                    all_struct_names,
                    all_table_names,
                    all_union_names,
                    all_enum_bases,
                    include_type_map,
                )
                if category in (
                    "string",
                    "vector",
                    "vector_struct",
                    "vector_enum",
                    "vector_string",
                    "table",
                    "vector_table",
                ):
                    f.write(f"  if ({field_name}_offset.o != 0) {{\n")
                    f.write(
                        f"    root_builder.add_{field_name}({field_name}_offset);\n"
                    )
                    f.write("  }\n")
                elif category == "union":
                    union_name = local_type_base(field_type)
                    union_cpp_type = qualify_cpp_user_type(
                        union_name,
                        namespace_full,
                        include_type_map,
                    )
                    none_value = f"{union_cpp_type}_NONE"
                    f.write(f"  if ({field_name}_type_value != {none_value}) {{\n")
                    f.write(
                        f"    root_builder.add_{field_name}_type({field_name}_type_value);\n"
                    )
                    f.write("  }\n")
                    f.write(f"  if ({field_name}_value_offset.o != 0) {{\n")
                    f.write(
                        f"    root_builder.add_{field_name}({field_name}_value_offset);\n"
                    )
                    f.write("  }\n")
                elif category == "vector_union":
                    f.write(f"  if ({field_name}_type_offset.o != 0) {{\n")
                    f.write(
                        f"    root_builder.add_{field_name}_type({field_name}_type_offset);\n",
                    )
                    f.write("  }\n")
                    f.write(f"  if ({field_name}_offset.o != 0) {{\n")
                    f.write(
                        f"    root_builder.add_{field_name}({field_name}_offset);\n"
                    )
                    f.write("  }\n")
                elif category == "struct":
                    f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                    f.write(
                        f"    root_builder.add_{field_name}(&msg.{field_name}.value());\n",
                    )
                    f.write("  }\n")
                else:
                    f.write(f"  if (msg.{field_name}.has_value()) {{\n")
                    f.write(
                        f"    root_builder.add_{field_name}(msg.{field_name}.value());\n",
                    )
                    f.write("  }\n")

            f.write("  return root_builder.Finish();\n")
            f.write("}\n\n")

        if action_binding is not None:
            for table_name, fields in all_tables:
                msg_name = f"{table_name}Msg"
                copy_helper_fn = copy_helper_fn_map[table_name]
                table_def = all_table_defs[table_name]
                table_ns_parts = (
                    table_def.namespace_full.split(".") if table_def.namespace_full else []
                )
                fq_table = (
                    "::" + "::".join(table_ns_parts + [table_name])
                    if table_ns_parts
                    else f"::{table_name}"
                )
                fq_msg = (
                    "::" + "::".join(ns_parts + [msg_name]) if ns_parts else f"::{msg_name}"
                )
                f.write(f"inline {fq_msg} {copy_helper_fn}(\n")
                f.write(f"    const {fq_table}& root) {{\n")
                f.write(f"  {fq_msg} out;\n")
                for field in fields:
                    if is_header_field(field):
                        continue
                    field_name = str(field["name"])
                    field_type = str(field["type"])
                    # Type-support matrix enforcement: the C++
                    # action copy helper is the ONLY C++ path with an UNSUPPORTED
                    # cell (vector-of-union -- its parallel discriminator/value
                    # vectors are not reconstructed here, unlike the typed_build
                    # path). Reject it with the standard schema/table/field/shape
                    # message before dispatching on category.
                    require_field_supported(
                        logical_field_category(
                            field_type,
                            all_struct_names,
                            all_table_names,
                            all_union_names,
                            all_enum_bases,
                            include_type_map,
                        ),
                        "cpp",
                        "copy_helper",
                        stem,
                        table_name,
                        field_name,
                        field_type,
                    )
                    cpp_type, category = cpp_msg_field_type(
                        field_type,
                        namespace_full,
                        all_struct_names,
                        all_table_names,
                        all_union_names,
                        all_enum_bases,
                        include_type_map,
                    )
                    if category in ("scalar", "enum", "user"):
                        f.write(f"  out.{field_name} = root.{field_name}();\n")
                    elif category == "string":
                        f.write(f"  if (const auto* value = root.{field_name}()) {{\n")
                        f.write(f"    out.{field_name} = value->str();\n")
                        f.write("  }\n")
                    elif category == "struct":
                        f.write(f"  if (const auto* value = root.{field_name}()) {{\n")
                        f.write(f"    out.{field_name} = *value;\n")
                        f.write("  }\n")
                    elif category == "vector":
                        vec_elem = split_vector_type(field_type)
                        if vec_elem is None:
                            raise GenerationError(
                                "failed to resolve vector element type for '{}:{}'".format(
                                    table_name,
                                    field_name,
                                )
                            )
                        elem_type, _ = cpp_msg_field_type(
                            vec_elem,
                            namespace_full,
                            all_struct_names,
                            all_table_names,
                            all_union_names,
                            all_enum_bases,
                            include_type_map,
                        )
                        f.write(f"  if (const auto* values = root.{field_name}()) {{\n")
                        f.write(f"    std::vector<{elem_type}> copied;\n")
                        f.write("    copied.reserve(values->size());\n")
                        f.write("    for (uint32_t i = 0; i < values->size(); ++i) {\n")
                        f.write("      copied.push_back(values->Get(i));\n")
                        f.write("    }\n")
                        f.write(f"    out.{field_name} = std::move(copied);\n")
                        f.write("  }\n")
                    elif category == "vector_enum":
                        vec_elem = split_vector_type(field_type)
                        if vec_elem is None:
                            raise GenerationError(
                                "failed to resolve vector enum element type for '{}:{}'".format(
                                    table_name,
                                    field_name,
                                )
                            )
                        elem_type, _ = cpp_msg_field_type(
                            vec_elem,
                            namespace_full,
                            all_struct_names,
                            all_table_names,
                            all_union_names,
                            all_enum_bases,
                            include_type_map,
                        )
                        f.write(f"  if (const auto* values = root.{field_name}()) {{\n")
                        f.write(f"    std::vector<{elem_type}> copied;\n")
                        f.write("    copied.reserve(values->size());\n")
                        f.write("    for (uint32_t i = 0; i < values->size(); ++i) {\n")
                        f.write(
                            f"      copied.push_back(static_cast<{elem_type}>(values->Get(i)));\n"
                        )
                        f.write("    }\n")
                        f.write(f"    out.{field_name} = std::move(copied);\n")
                        f.write("  }\n")
                    elif category == "vector_struct":
                        vec_elem = split_vector_type(field_type)
                        if vec_elem is None:
                            raise GenerationError(
                                "failed to resolve vector struct element type for '{}:{}'".format(
                                    table_name,
                                    field_name,
                                )
                            )
                        elem_type = qualify_cpp_user_type(
                            vec_elem,
                            namespace_full,
                            include_type_map,
                        )
                        f.write(f"  if (const auto* values = root.{field_name}()) {{\n")
                        f.write(f"    std::vector<{elem_type}> copied;\n")
                        f.write("    copied.reserve(values->size());\n")
                        f.write("    for (uint32_t i = 0; i < values->size(); ++i) {\n")
                        f.write("      copied.push_back(*values->Get(i));\n")
                        f.write("    }\n")
                        f.write(f"    out.{field_name} = std::move(copied);\n")
                        f.write("  }\n")
                    elif category == "vector_string":
                        f.write(f"  if (const auto* values = root.{field_name}()) {{\n")
                        f.write("    std::vector<std::string> copied;\n")
                        f.write("    copied.reserve(values->size());\n")
                        f.write("    for (uint32_t i = 0; i < values->size(); ++i) {\n")
                        f.write("      copied.push_back(values->Get(i)->str());\n")
                        f.write("    }\n")
                        f.write(f"    out.{field_name} = std::move(copied);\n")
                        f.write("  }\n")
                    elif category == "table":
                        child_table = local_type_base(field_type)
                        child_helper = copy_helper_fn_map.get(child_table)
                        if child_helper is None:
                            raise GenerationError(
                                "C++ action copy helper only supports local nested tables; field '{}:{}' is unsupported".format(
                                    field_name,
                                    field_type,
                                )
                            )
                        child_msg_type = (
                            "::" + "::".join(ns_parts + [f"{child_table}Msg"])
                            if ns_parts
                            else f"::{child_table}Msg"
                        )
                        f.write(f"  if (const auto* value = root.{field_name}()) {{\n")
                        f.write(
                            f"    out.{field_name} = std::make_shared<{child_msg_type}>({child_helper}(*value));\n",
                        )
                        f.write("  }\n")
                    elif category == "vector_table":
                        vec_elem = split_vector_type(field_type)
                        child_table = local_type_base(vec_elem or "")
                        child_helper = copy_helper_fn_map.get(child_table)
                        if vec_elem is None or child_helper is None:
                            raise GenerationError(
                                "C++ action copy helper only supports local nested tables; field '{}:{}' is unsupported".format(
                                    field_name,
                                    field_type,
                                )
                            )
                        child_msg_type = (
                            "::" + "::".join(ns_parts + [f"{child_table}Msg"])
                            if ns_parts
                            else f"::{child_table}Msg"
                        )
                        f.write(f"  if (const auto* values = root.{field_name}()) {{\n")
                        f.write(
                            f"    std::vector<std::shared_ptr<{child_msg_type}>> copied;\n",
                        )
                        f.write("    copied.reserve(values->size());\n")
                        f.write("    for (uint32_t i = 0; i < values->size(); ++i) {\n")
                        f.write("      if (const auto* value = values->Get(i)) {\n")
                        f.write(
                            f"        copied.push_back(std::make_shared<{child_msg_type}>({child_helper}(*value)));\n",
                        )
                        f.write("      }\n")
                        f.write("    }\n")
                        f.write(f"    out.{field_name} = std::move(copied);\n")
                        f.write("  }\n")
                    elif category == "union":
                        union_name = local_type_base(field_type)
                        union_cpp_type = qualify_cpp_user_type(
                            union_name,
                            namespace_full,
                            include_type_map,
                        )
                        union_msg_type = (
                            "::" + "::".join(ns_parts + [f"{union_name}Msg"])
                            if ns_parts
                            else f"::{union_name}Msg"
                        )
                        none_value = f"{union_cpp_type}_NONE"
                        variants = all_union_defs.get(union_name)
                        if not variants:
                            raise GenerationError(
                                "C++ action copy helper could not resolve union variants for field '{}:{}'".format(
                                    field_name,
                                    field_type,
                                )
                            )
                        f.write(f"  switch (root.{field_name}_type()) {{\n")
                        f.write(f"    case {none_value}:\n")
                        f.write("      break;\n")
                        # `child_helper`/`child_msg_type` resolve
                        # the variant's TARGET table; `variant_value` (the
                        # `switch` case label, matched against flatc's
                        # discriminant) and the `_as_{...}()` accessor
                        # method suffix are both keyed by the ALIAS
                        # (idl_gen_cpp.cpp: `Name(ev)` == `ev.name`, used
                        # verbatim -- C++ does not snake_case it).
                        for alias, target in variants:
                            child_helper = copy_helper_fn_map.get(target)
                            if child_helper is None:
                                raise GenerationError(
                                    "C++ action copy helper only supports table union variants; field '{}:{}' variant '{}' (target '{}') is unsupported".format(
                                        field_name,
                                        field_type,
                                        alias,
                                        target,
                                    )
                                )
                            child_msg_type = (
                                "::" + "::".join(ns_parts + [f"{target}Msg"])
                                if ns_parts
                                else f"::{target}Msg"
                            )
                            variant_value = f"{union_cpp_type}_{alias}"
                            f.write(f"    case {variant_value}: {{\n")
                            f.write(
                                f"      if (const auto* value = root.{field_name}_as_{alias}()) {{\n"
                            )
                            f.write(
                                f"        {union_msg_type} copied = std::make_shared<{child_msg_type}>({child_helper}(*value));\n"
                            )
                            f.write(f"        out.{field_name} = std::move(copied);\n")
                            f.write("      }\n")
                            f.write("      break;\n")
                            f.write("    }\n")
                        f.write("    default:\n")
                        f.write("      break;\n")
                        f.write("  }\n")
                    elif category == "vector_union":
                        raise GenerationError(
                            "C++ action copy helper does not support field '{}:{}'".format(
                                field_name,
                                field_type,
                            )
                        )
                    else:
                        raise GenerationError(
                            "unsupported C++ action copy helper field category '{}' for '{}:{}'".format(
                                category,
                                table_name,
                                field_name,
                            )
                        )
                f.write("  return out;\n")
                f.write("}\n\n")

        f.write(f"}}  // namespace {detail_ns}\n")
        f.write("}  // namespace detail\n\n")

        for table_name, _ in typed_tables:
            msg_name = f"{table_name}Msg"
            fq_table = (
                "::" + "::".join(ns_parts + [table_name])
                if ns_parts
                else f"::{table_name}"
            )
            fq_msg = (
                "::" + "::".join(ns_parts + [msg_name]) if ns_parts else f"::{msg_name}"
            )
            helper_fn = build_fn_map[table_name]

            f.write("template <>\n")
            f.write(f"struct TypedPublishTraits<{fq_table}> {{\n")
            f.write(f"  using Msg = {fq_msg};\n")
            f.write("  static constexpr bool kHasAortaHeader = true;\n\n")
            f.write("  static flatbuffers::Offset<")
            f.write(f"{fq_table[2:]}> Build(\n")
            f.write("      flatbuffers::FlatBufferBuilder& builder,\n")
            f.write("      flatbuffers::Offset<aorta::sys::AortaHeader> header,\n")
            f.write("      const Msg& msg) {\n")
            f.write(f"    return detail::{detail_ns}::{helper_fn}(builder, msg, header);\n")
            f.write("  }\n")
            f.write("};\n\n")

        # TypedSubscribeTraits specializations are emitted ONLY for
        # root-local tables. This matches the Rust side's deliberate
        # local-only impl scoping and removes the owner/includer guard
        # asymmetry: previously `subscribe_tables = list(all_tables)`
        # emitted a specialization for every table reachable via the include
        # closure, unguarded when the CURRENT schema was its owner (root-local)
        # and guarded when it was merely an includer -- co-including an
        # owner's and an includer's wrapper header was a redefinition error in
        # either order. No owner-side guard is added: single-owner emission is
        # sufficient, and a guard would mask genuine inconsistent
        # specializations from out-of-tree generators. Callers reaching an
        # include-table type (e.g. `TypedSubscribeTraits<IncludedTable>`)
        # resolve through the primary template in aorta/typed_subscribe.h
        # (now included below), whose generic `VerifyBuffer<T>(nullptr)` /
        # `GetRoot<T>` body is exactly what the removed specialization did for
        # a non-root type -- no behavior change.
        subscribe_tables = [
            (table_name, fields)
            for table_name, fields in all_tables
            if table_name in root_table_names
        ]

        for table_name, _ in subscribe_tables:
            table_def = all_table_defs[table_name]
            table_ns_parts = (
                table_def.namespace_full.split(".") if table_def.namespace_full else []
            )
            fq_table = (
                "::" + "::".join(table_ns_parts + [table_name])
                if table_ns_parts
                else f"::{table_name}"
            )
            f.write("template <>\n")
            f.write(f"struct TypedSubscribeTraits<{fq_table}> {{\n")
            f.write("  static bool Verify(const uint8_t* data, std::size_t size) {\n")
            f.write("    if (data == nullptr || size == 0) {\n")
            f.write("      return false;\n")
            f.write("    }\n")
            f.write("    flatbuffers::Verifier verifier(data, size);\n")
            if root_type is not None and table_name == root_type:
                verify_fn = (
                    "::" + "::".join(ns_parts + [f"Verify{table_name}Buffer"])
                    if ns_parts
                    else f"::Verify{table_name}Buffer"
                )
                f.write(f"    return {verify_fn}(verifier);\n")
            else:
                f.write(f"    return verifier.VerifyBuffer<{fq_table[2:]}>(nullptr);\n")
            f.write("  }\n\n")
            f.write(f"  static const {fq_table[2:]}* GetRoot(const uint8_t* data) {{\n")
            f.write("    if (data == nullptr) {\n")
            f.write("      return nullptr;\n")
            f.write("    }\n")
            if root_type is not None and table_name == root_type:
                get_root_fn = (
                    "::" + "::".join(ns_parts + [f"Get{table_name}"])
                    if ns_parts
                    else f"::Get{table_name}"
                )
                f.write(f"    return {get_root_fn}(data);\n")
            else:
                f.write(f"    return flatbuffers::GetRoot<{fq_table[2:]}>(data);\n")
            f.write("  }\n")
            f.write("};\n\n")

        f.write("}  // namespace aorta\n")

        if action_binding is not None:
            if ns_parts:
                f.write("\n")
                for part in ns_parts:
                    f.write(f"namespace {part} {{\n")
                f.write("\n")

            action_marker = action_binding["action_marker"]
            f.write(f"struct {action_marker} {{\n")
            for alias_name, table_name in (
                ("Goal", action_binding["goal"]),
                ("GoalMsg", f"{action_binding['goal']}Msg"),
                ("Result", action_binding["result"]),
                ("ResultMsg", f"{action_binding['result']}Msg"),
                ("FeedbackData", action_binding["feedback_data"]),
                ("FeedbackDataMsg", f"{action_binding['feedback_data']}Msg"),
                ("SendGoalRequest", action_binding["send_goal_request"]),
                ("SendGoalResponse", action_binding["send_goal_response"]),
                ("CancelGoalRequest", action_binding["cancel_goal_request"]),
                ("CancelGoalResponse", action_binding["cancel_goal_response"]),
                ("Feedback", action_binding["feedback"]),
                ("PublicStatus", action_binding["public_status"]),
                ("GetResultRequest", action_binding["get_result_request"]),
                ("GetResultResponse", action_binding["get_result_response"]),
            ):
                f.write(f"  using {alias_name} = {table_name};\n")
            f.write("\n")
            f.write("  static constexpr const char* ActionName() {\n")
            f.write(f'    return "{action_binding["action_name"]}";\n')
            f.write("  }\n\n")
            f.write(
                "  static std::vector<uint8_t> EncodeSendGoalRequest(\n"
                "      const std::string& goal_id,\n"
                "      const GoalMsg& goal) {\n"
            )
            f.write(f"    {action_binding['send_goal_request']}Msg msg;\n")
            f.write("    msg.goal_id = goal_id;\n")
            f.write("    msg.goal = std::make_shared<GoalMsg>(goal);\n")
            f.write("    flatbuffers::FlatBufferBuilder builder;\n")
            f.write(
                f"    auto root = ::aorta::detail::{detail_ns}::{build_fn_map[action_binding['send_goal_request']]}(builder, msg, {{}});\n",
            )
            f.write("    builder.Finish(root);\n")
            f.write(
                "    return std::vector<uint8_t>(builder.GetBufferPointer(),\n"
                "                                builder.GetBufferPointer() + builder.GetSize());\n"
            )
            f.write("  }\n\n")
            f.write(
                "  static std::vector<uint8_t> EncodeCancelGoalRequest(\n"
                "      const std::string& goal_id,\n"
                "      const std::string& goal_secret) {\n"
            )
            f.write(f"    {action_binding['cancel_goal_request']}Msg msg;\n")
            f.write("    msg.goal_id = goal_id;\n")
            f.write("    msg.goal_secret = goal_secret;\n")
            f.write("    flatbuffers::FlatBufferBuilder builder;\n")
            f.write(
                f"    auto root = ::aorta::detail::{detail_ns}::{build_fn_map[action_binding['cancel_goal_request']]}(builder, msg, {{}});\n",
            )
            f.write("    builder.Finish(root);\n")
            f.write(
                "    return std::vector<uint8_t>(builder.GetBufferPointer(),\n"
                "                                builder.GetBufferPointer() + builder.GetSize());\n"
            )
            f.write("  }\n\n")
            f.write(
                "  static std::vector<uint8_t> EncodeGetResultRequest(\n"
                "      const std::string& goal_id,\n"
                "      const std::string& goal_secret) {\n"
            )
            f.write(f"    {action_binding['get_result_request']}Msg msg;\n")
            f.write("    msg.goal_id = goal_id;\n")
            f.write("    msg.goal_secret = goal_secret;\n")
            f.write("    flatbuffers::FlatBufferBuilder builder;\n")
            f.write(
                f"    auto root = ::aorta::detail::{detail_ns}::{build_fn_map[action_binding['get_result_request']]}(builder, msg, {{}});\n",
            )
            f.write("    builder.Finish(root);\n")
            f.write(
                "    return std::vector<uint8_t>(builder.GetBufferPointer(),\n"
                "                                builder.GetBufferPointer() + builder.GetSize());\n"
            )
            f.write("  }\n\n")
            f.write(
                "  static std::vector<uint8_t> EncodeSendGoalResponse(\n"
                "      bool accepted,\n"
                "      const std::string& goal_id,\n"
                "      const std::string& goal_secret,\n"
                "      ::aorta::action::common::ActionReplyCode code,\n"
                "      ::aorta::action::common::ActionStatus status,\n"
                "      const std::string& message,\n"
                "      const std::string& server_epoch) {\n"
            )
            f.write(f"    {action_binding['send_goal_response']}Msg msg;\n")
            f.write("    msg.accepted = accepted;\n")
            f.write("    msg.goal_id = goal_id;\n")
            f.write("    msg.goal_secret = goal_secret;\n")
            f.write("    msg.code = code;\n")
            f.write("    msg.status = status;\n")
            f.write("    msg.message = message;\n")
            f.write("    msg.server_epoch = server_epoch;\n")
            f.write("    flatbuffers::FlatBufferBuilder builder;\n")
            f.write(
                f"    auto root = ::aorta::detail::{detail_ns}::{build_fn_map[action_binding['send_goal_response']]}(builder, msg, {{}});\n",
            )
            f.write("    builder.Finish(root);\n")
            f.write(
                "    return std::vector<uint8_t>(builder.GetBufferPointer(),\n"
                "                                builder.GetBufferPointer() + builder.GetSize());\n"
            )
            f.write("  }\n\n")
            f.write(
                "  static std::vector<uint8_t> EncodeCancelGoalResponse(\n"
                "      bool accepted,\n"
                "      const std::string& goal_id,\n"
                "      ::aorta::action::common::ActionReplyCode code,\n"
                "      ::aorta::action::common::ActionStatus status,\n"
                "      const std::string& message) {\n"
            )
            f.write(f"    {action_binding['cancel_goal_response']}Msg msg;\n")
            f.write("    msg.accepted = accepted;\n")
            f.write("    msg.goal_id = goal_id;\n")
            f.write("    msg.code = code;\n")
            f.write("    msg.status = status;\n")
            f.write("    msg.message = message;\n")
            f.write("    flatbuffers::FlatBufferBuilder builder;\n")
            f.write(
                f"    auto root = ::aorta::detail::{detail_ns}::{build_fn_map[action_binding['cancel_goal_response']]}(builder, msg, {{}});\n",
            )
            f.write("    builder.Finish(root);\n")
            f.write(
                "    return std::vector<uint8_t>(builder.GetBufferPointer(),\n"
                "                                builder.GetBufferPointer() + builder.GetSize());\n"
            )
            f.write("  }\n\n")
            f.write(
                "  static std::vector<uint8_t> EncodeFeedbackData(\n"
                "      const FeedbackDataMsg& data) {\n"
            )
            f.write("    flatbuffers::FlatBufferBuilder builder;\n")
            f.write(
                f"    auto root = ::aorta::detail::{detail_ns}::{build_fn_map[action_binding['feedback_data']]}(builder, data, {{}});\n",
            )
            f.write("    builder.Finish(root);\n")
            f.write(
                "    return std::vector<uint8_t>(builder.GetBufferPointer(),\n"
                "                                builder.GetBufferPointer() + builder.GetSize());\n"
            )
            f.write("  }\n\n")
            f.write(
                "  static std::vector<uint8_t> EncodeResult(\n"
                "      const ResultMsg& result) {\n"
            )
            f.write("    flatbuffers::FlatBufferBuilder builder;\n")
            f.write(
                f"    auto root = ::aorta::detail::{detail_ns}::{build_fn_map[action_binding['result']]}(builder, result, {{}});\n",
            )
            f.write("    builder.Finish(root);\n")
            f.write(
                "    return std::vector<uint8_t>(builder.GetBufferPointer(),\n"
                "                                builder.GetBufferPointer() + builder.GetSize());\n"
            )
            f.write("  }\n\n")
            f.write(
                "  static std::vector<uint8_t> EncodeFeedback(\n"
                "      const std::string& goal_id,\n"
                "      uint64_t seq,\n"
                "      uint64_t stamp_ns,\n"
                "      ::aorta::action::common::ActionStatus status,\n"
                "      const FeedbackDataMsg& data,\n"
                "      const std::string& message) {\n"
            )
            f.write(f"    {action_binding['feedback']}Msg msg;\n")
            f.write("    msg.goal_id = goal_id;\n")
            f.write("    msg.seq = seq;\n")
            f.write("    msg.stamp_ns = stamp_ns;\n")
            f.write("    msg.status = status;\n")
            f.write("    msg.data = std::make_shared<FeedbackDataMsg>(data);\n")
            f.write("    msg.message = message;\n")
            f.write("    flatbuffers::FlatBufferBuilder builder;\n")
            f.write(
                f"    auto root = ::aorta::detail::{detail_ns}::{build_fn_map[action_binding['feedback']]}(builder, msg, {{}});\n",
            )
            f.write("    builder.Finish(root);\n")
            f.write(
                "    return std::vector<uint8_t>(builder.GetBufferPointer(),\n"
                "                                builder.GetBufferPointer() + builder.GetSize());\n"
            )
            f.write("  }\n\n")
            f.write(
                "  static std::vector<uint8_t> EncodePublicStatus(\n"
                "      const std::string& goal_id,\n"
                "      ::aorta::action::common::ActionStatus status,\n"
                "      ::aorta::action::common::ActionReplyCode code,\n"
                "      uint64_t accepted_stamp_ns,\n"
                "      uint64_t started_stamp_ns,\n"
                "      uint64_t finished_stamp_ns,\n"
                "      uint64_t last_feedback_seq,\n"
                "      const std::string& server_epoch) {\n"
            )
            f.write(f"    {action_binding['public_status']}Msg msg;\n")
            f.write("    msg.goal_id = goal_id;\n")
            f.write("    msg.status = status;\n")
            f.write("    msg.code = code;\n")
            f.write("    msg.accepted_stamp_ns = accepted_stamp_ns;\n")
            f.write("    msg.started_stamp_ns = started_stamp_ns;\n")
            f.write("    msg.finished_stamp_ns = finished_stamp_ns;\n")
            f.write("    msg.last_feedback_seq = last_feedback_seq;\n")
            f.write("    msg.server_epoch = server_epoch;\n")
            f.write("    flatbuffers::FlatBufferBuilder builder;\n")
            f.write(
                f"    auto root = ::aorta::detail::{detail_ns}::{build_fn_map[action_binding['public_status']]}(builder, msg, {{}});\n",
            )
            f.write("    builder.Finish(root);\n")
            f.write(
                "    return std::vector<uint8_t>(builder.GetBufferPointer(),\n"
                "                                builder.GetBufferPointer() + builder.GetSize());\n"
            )
            f.write("  }\n\n")
            f.write(
                "  static std::vector<uint8_t> EncodeGetResultResponse(\n"
                "      const std::string& goal_id,\n"
                "      ::aorta::action::common::ActionStatus status,\n"
                "      uint64_t accepted_stamp_ns,\n"
                "      uint64_t started_stamp_ns,\n"
                "      uint64_t finished_stamp_ns,\n"
                "      uint64_t last_feedback_seq,\n"
                "      const FeedbackDataMsg& feedback_snapshot,\n"
                "      const std::optional<ResultMsg>& result,\n"
                "      ::aorta::action::common::ActionReplyCode code,\n"
                "      const std::string& message,\n"
                "      const std::string& server_epoch) {\n"
            )
            f.write(f"    {action_binding['get_result_response']}Msg msg;\n")
            f.write("    msg.goal_id = goal_id;\n")
            f.write("    msg.status = status;\n")
            f.write("    msg.accepted_stamp_ns = accepted_stamp_ns;\n")
            f.write("    msg.started_stamp_ns = started_stamp_ns;\n")
            f.write("    msg.finished_stamp_ns = finished_stamp_ns;\n")
            f.write("    msg.last_feedback_seq = last_feedback_seq;\n")
            f.write("    msg.feedback_snapshot = std::make_shared<FeedbackDataMsg>(feedback_snapshot);\n")
            f.write("    if (result.has_value()) {\n")
            f.write("      msg.result = std::make_shared<ResultMsg>(result.value());\n")
            f.write("    }\n")
            f.write("    msg.code = code;\n")
            f.write("    msg.message = message;\n")
            f.write("    msg.server_epoch = server_epoch;\n")
            f.write("    flatbuffers::FlatBufferBuilder builder;\n")
            f.write(
                f"    auto root = ::aorta::detail::{detail_ns}::{build_fn_map[action_binding['get_result_response']]}(builder, msg, {{}});\n",
            )
            f.write("    builder.Finish(root);\n")
            f.write(
                "    return std::vector<uint8_t>(builder.GetBufferPointer(),\n"
                "                                builder.GetBufferPointer() + builder.GetSize());\n"
            )
            f.write("  }\n\n")
            f.write(
                "  static ResultMsg CopyResult(const Result& root) {\n"
            )
            f.write(
                f"    return ::aorta::detail::{detail_ns}::{copy_helper_fn_map[action_binding['result']]}(root);\n",
            )
            f.write("  }\n\n")
            f.write(
                "  static FeedbackDataMsg CopyFeedbackData(const FeedbackData& root) {\n"
            )
            f.write(
                f"    return ::aorta::detail::{detail_ns}::{copy_helper_fn_map[action_binding['feedback_data']]}(root);\n",
            )
            f.write("  }\n")
            f.write("};\n")

            if ns_parts:
                f.write("\n")
                for part in reversed(ns_parts):
                    f.write(f"}}  // namespace {part}\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", choices=["rust", "cpp"], required=True)
    parser.add_argument("--stem", required=True)
    parser.add_argument("--schema-rel", help="relative schema path from message root")
    parser.add_argument("--root-fbs", help="root fbs file path")
    parser.add_argument(
        "--with-schema-meta",
        action="store_true",
        help="include rust schema_meta wrapper and fixed-size constant from schema meta",
    )
    parser.add_argument(
        "--rust-local-module-dir",
        help="optional wrapper-local directory used for Rust generated modules and bfbs",
    )
    parser.add_argument(
        "--rust-local-module-prefix",
        help="optional wrapper-local filename prefix used for Rust generated modules and bfbs",
    )
    parser.add_argument(
        "--action-name",
        help=(
            "explicit action routing name for local (Tier-2) action schemas; "
            "forces emission of the <Prefix>Action wrapper independent of the "
            "schemas/action/ path convention"
        ),
    )
    parser.add_argument(
        "--include-fbs",
        nargs="*",
        default=[],
        help=(
            "declared transitive .fbs include closure for this invocation's "
            "root schema (the same closure that already drives Bazel action "
            "hermeticity). When given, include resolution matches the "
            "manifest by workspace-relative path suffix before falling back "
            "to directory guessing — required for Tier-2 external Bzlmod "
            "consumers, whose includes may resolve into a DIFFERENT "
            "module's sandboxed tree (e.g. external/aorta~/message/schemas/...) "
            "that no path heuristic can discover. Optional for backward "
            "compatibility with invocations that omit it."
        ),
    )
    parser.add_argument(
        "--root-bfbs",
        metavar="BFBS_PATH",
        required=True,
        help=(
            "REQUIRED. The compiled `.bfbs` for "
            "this invocation's ROOT schema (for actions, the AGGREGATE's "
            "bfbs). The wrapper emitters take their table/union/enum/struct/"
            "root/namespace/include-type facts from schema_reflection.py's "
            "qualified TypeId/UnionVariantDef model — the wrapper action is "
            "thereby closure-hermetic by construction (its facts come from "
            "the bfbs, whose closure Bazel already declares). The only "
            "remaining text-derived fact is the Rust `#[path]` include-"
            "module walk, whose message-root-relative generated `.rs` paths "
            "the bfbs cannot supply (see `collect_include_generated_modules`)."
        ),
    )
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    # Distinguish an EXPLICIT empty override (`--action-name=""`) from an
    # ABSENT one (`None`, path-inferred). `args.action_name or None` collapses
    # the two, silently turning an explicit-empty assertion into "no override"
    # (and dropping the action wrapper). An explicit `--action-name` is an
    # assertion that the schema IS an action named X; the empty string is never
    # a valid routing name, so hard-fail instead of falling back.
    if args.action_name == "":
        print(
            "gen_message_wrapper error: --action-name was given but empty; an "
            "explicit action routing name must be a non-empty string",
            file=sys.stderr,
        )
        return 2
    action_name = args.action_name

    global _INCLUDE_MANIFEST
    _INCLUDE_MANIFEST = [Path(p) for p in args.include_fbs]

    # Load the bfbs once; the `emit_*` dispatchers read this handle. The
    # import is local (via `_reflection()`), so a caller that only needs the
    # include-closure/path helpers (e.g. `gen_action_wrapper.py`, which does
    # not import this module at all) never pays for the reflection bindings.
    global _LOADED_SCHEMA
    try:
        _LOADED_SCHEMA = _reflection().load_bfbs_file(args.root_bfbs)
    except Exception as err:  # SchemaLoadError and any load failure
        print(
            "gen_message_wrapper error: could not load --root-bfbs "
            "'{}': {}".format(args.root_bfbs, err),
            file=sys.stderr,
        )
        return 2

    try:
        if args.lang == "rust":
            # `--schema-rel` is only LOAD-BEARING for the relative-`#[path]`
            # emission mode, where every module is addressed as
            # `../../<schema-rel>/<name>` from the wrapper's own directory. With
            # `--rust-local-module-{prefix,dir}` the modules sit next to (or
            # under a directory beside) the wrapper and are addressed by
            # basename, so an EXTERNAL schema root — whose package has no
            # `//message/`-rooted relative path to name at all — legitimately
            # passes `""`, exactly as the C++
            # leg already does. Requiring it unconditionally would have forced
            # an external root to invent a fake in-repo path, which is precisely
            # the kind of value that must never reach the schema identity.
            if not args.schema_rel and not (
                args.rust_local_module_prefix or args.rust_local_module_dir
            ):
                print(
                    "--schema-rel is required for rust unless "
                    "--rust-local-module-prefix/--rust-local-module-dir is used",
                    file=sys.stderr,
                )
                return 2
            if not args.root_fbs:
                print("--root-fbs is required for rust", file=sys.stderr)
                return 2
            write_rust(
                out=args.out,
                stem=args.stem,
                schema_rel=args.schema_rel,
                root_fbs=args.root_fbs,
                with_schema_meta=args.with_schema_meta,
                local_module_dir=args.rust_local_module_dir,
                local_module_prefix=args.rust_local_module_prefix,
                action_name=action_name,
            )
        else:
            if not args.root_fbs:
                print("--root-fbs is required for cpp", file=sys.stderr)
                return 2
            write_cpp(
                out=args.out,
                stem=args.stem,
                schema_rel=args.schema_rel or "",
                root_fbs=args.root_fbs,
                action_name=action_name,
            )

        return 0
    except GenerationError as err:
        print(f"gen_message_wrapper error: {err}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
