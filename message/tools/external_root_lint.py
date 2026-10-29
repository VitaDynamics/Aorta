#!/usr/bin/env python3
"""Fail-closed content lint for an EXTERNAL schema root.

The macro's analysis-time gate can only check what a Starlark VALUE proves.
This module checks what only FILE CONTENT can prove, and it re-derives every
fact rather than believing the manifest's own claims:

  1. the fragment's ORIGINAL text is re-parsed with the shared pure-data grammar
     (no `load`, no `def`, no comprehension, no rule call, sorted, unique keys)
     under the external root's field/provenance policy;
  2. the fragment entries for the calling package and the manifest's declared
     items must be EXACTLY equal, field by field;
  3. the physical `.fbs` inventory and the declared items must be a bijection,
     and an INDEPENDENT `glob()` census of the calling package must contain
     nothing but those declared sources (a stray canonical `.fbs` is invisible
     to every declaration-derived check);
  4. include text and `dep_fbs` direct edges must agree in BOTH directions;
  5. every source is copied byte-for-byte into its canonical slot, and both
     hashes must match what the manifest recorded;
  6. flags follow the first-class policy: an item with a
     root type enables the whole supported-language set — C++, Python AND Rust
     — an include-only item enables none of it, and any flag
     Aorta does not yet generate a surface for is refused rather than dropped;
     "has a root type" is read from the FILE, not from the flag, so a schema
     cannot pose as include-only to escape the policy;
  7. target / alias / wrapper / crate names are unique inside the root, and
     every crate name (defaulted or explicit) is a legal Rust identifier;
  8. `internal` provenance is only claimed by roots Aorta itself declared, and
     a canonical slot already owned by an Aorta registry entry is never
     re-claimed — including from another repository, where the collision would
     otherwise only surface as a bfbs staging hard error (the staging tree
     strips `external/<repo>/`, so two owners of `message/schemas/<rel>` land in
     the same slot);
  9. the policy constants the manifest carries are held to a FLOOR, so a root
     cannot narrow the rules it is checked against.

Trust boundary: an EDU repository owner can edit their own BUILD,
fragment, sources and test selection at the same time, so this lint does not
pretend to authenticate them. What it does guarantee is that the OFFICIAL
macro, when executed, fails closed — and that no `external` root can reach
Aorta's v3 registry, its release projection, or `@aorta//message`.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable, Sequence

from registry_lint import RegistryLintError, lint_fragment_text
from schema_include_utils import parse_schema_includes, strip_fbs_comments

# The action-source rule below is gen_action_wrapper's own rule, imported from
# gen_action_wrapper rather than restated here: the generator is what actually
# expands an action schema, so a second copy of "which root_type is legal"
# could only ever drift away from the thing it claims to describe.
from gen_action_wrapper import (
    GenerationError,
    derive_action_prefix,
    parse_root_type,
    parse_table_names,
)

EXTERNAL_ROOT_MANIFEST_VERSION = 1

MANIFEST_TOP_LEVEL_KEYS = frozenset(
    {
        "allowed_item_fields",
        "direct_dep_labels",
        "expected_provenance",
        "external_root_manifest_version",
        "fragment",
        "imports",
        "include_only_forbidden_flags",
        "items",
        "name",
        "package",
        "package_targets",
        "raw_language_flags",
        "refused_flags",
        "required_item_fields",
        "source_inventory",
        "source_package",
        "source_prefix",
        "typed_marker_flag",
        "typed_required_flags",
    }
)

ALLOWED_PROVENANCE = ("external", "internal")

# Policy FLOOR (see `_check_policy_floor`). The macro owns the live constants
# and ships them in the manifest so the two gates cannot drift; these are the
# minima a manifest may never fall below, plus the known field universe.
POLICY_TYPED_MARKER_FLAG = "bfbs"
POLICY_FLOOR_TYPED_REQUIRED_FLAGS = frozenset(
    {
        "bfbs",
        "cpp",
        "cpp_wrapper",
        "python",
        "rust",
        "rust_wrapper",
        "schema_meta_cpp",
        "schema_meta_rust",
    }
)
POLICY_FLOOR_INCLUDE_ONLY_FORBIDDEN_FLAGS = frozenset(
    {
        "action_schema",
        "bfbs",
        "cpp_wrapper",
        "rust_wrapper",
        "schema_meta_cpp",
        "schema_meta_py",
        "schema_meta_rust",
    }
)
POLICY_FLOOR_RAW_LANGUAGE_FLAGS = frozenset({"cpp", "python", "rust"})
POLICY_FLOOR_REQUIRED_ITEM_FIELDS = frozenset({"pkg", "provenance", "root_fbs", "stem"})
# `edu_public` is the EDU schema pack's projection flag: the composed
# registry of a consuming schema module selects its pack surface by it (Aorta's
# own base registry sets it nowhere since 2026-09-16). It is a KNOWN field (an
# `internal` root's items are registry entries and may carry it), but it is
# forbidden to a consumer-(b) root — see `_check_flags`: a user's repository
# must not be able to nominate its schema for the pack. The macro also leaves
# it out of an external root's `allowed_item_fields`, so for that consumer the
# AST parse rejects it one layer earlier.
POLICY_EDU_PUBLIC_FIELD = "edu_public"
# `include_in_examples_bfbs` is Aorta's own aggregate-selection field, and it
# is here for the same reason `edu_public` is: it selects the entry into
# `//message:examples_bfbs`, so dropping it on the way through would silently
# shrink that aggregate the moment a family migrates. Like `edu_public` it is
# forbidden to a consumer-(b) root — a user's repository must not be able to
# nominate its schema for an Aorta aggregate — and the macro leaves it out of
# an EXTERNAL root's `allowed_item_fields`, so the AST parse rejects it one
# layer earlier for that consumer.
POLICY_EXAMPLES_BFBS_FIELD = "include_in_examples_bfbs"
POLICY_AORTA_ONLY_ITEM_FIELDS = (POLICY_EDU_PUBLIC_FIELD, POLICY_EXAMPLES_BFBS_FIELD)
POLICY_KNOWN_ITEM_FIELDS = frozenset(
    {
        "edu_public",
        "include_in_examples_bfbs",
        "action_name",
        "action_schema",
        "alias_id",
        "bfbs",
        "cc_alias",
        "cpp",
        "cpp_lib_name",
        "cpp_wrapper",
        "cpp_wrapper_name",
        "dep_fbs",
        "extra_cc_deps",
        "pkg",
        "provenance",
        "python",
        "root_fbs",
        "rust",
        "rust_wrapper",
        "rust_wrapper_crate_name",
        "rust_wrapper_facade_name",
        "rust_wrapper_name",
        "schema_meta_cpp",
        "schema_meta_py",
        "schema_meta_rust",
        "stem",
    }
)

CANONICAL_SCHEMA_PACKAGE_PREFIX = "//message/schemas/"

# The internal-root allowlist is itself a pure-data `.bzl` list, so it is
# parsed with the very same grammar core (no second parser, no second set of
# holes).
INTERNAL_ROOT_LIST_NAME = "INTERNAL_EXTERNAL_SCHEMA_ROOTS"
INTERNAL_ROOT_FIELDS = frozenset({"pkg", "name", "source_package", "test_only"})
INTERNAL_ROOT_REQUIRED_FIELDS = frozenset({"pkg", "name", "source_package"})


class ExternalRootLintError(ValueError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_internal_root_allowlist(text: str, path: str) -> list[dict[str, Any]]:
    return lint_fragment_text(
        text,
        path,
        INTERNAL_ROOT_LIST_NAME,
        allowed_provenance=None,
        allowed_fields=INTERNAL_ROOT_FIELDS,
        required_fields=INTERNAL_ROOT_REQUIRED_FIELDS,
        key_fields=("pkg", "name"),
    )


def _package_path(package_label: str) -> str:
    return package_label[2:] if package_label.startswith("//") else package_label


def _dep_key(dep: str, package_label: str) -> tuple[str, str]:
    """(package path, basename) of a `dep_fbs` label, for include matching."""
    if dep.startswith("@"):
        _repo, _, rest = dep.partition("//")
        pkg, _, name = rest.partition(":")
        return pkg, name or Path(rest).name
    if dep.startswith("//"):
        pkg, _, name = dep[2:].partition(":")
        return pkg, name or Path(pkg).name
    if dep.startswith(":"):
        return _package_path(package_label), dep[1:]
    return _package_path(package_label), dep


def _include_key(include: str) -> tuple[str, str]:
    include = include.strip()
    if "/" in include:
        head, _, name = include.rpartition("/")
        return head, name
    return "", include


def _matches(dep_key: tuple[str, str], include_key: tuple[str, str]) -> bool:
    if dep_key[1] != include_key[1]:
        return False
    if not include_key[0]:
        return True
    # An include is written relative to a flatc include root, so its directory
    # part is a SUFFIX of the dependency's package path
    # (`schemas/system/aorta_base.fbs` vs `message/schemas/system`).
    return dep_key[0] == include_key[0] or dep_key[0].endswith("/" + include_key[0])


def _flag(fields: dict[str, Any], name: str, default: Any = False) -> Any:
    value = fields.get(name)
    return default if value is None else value


def _message_base(alias_id: str) -> str:
    return alias_id if alias_id.endswith("_msg") else alias_id + "_msg"


_CRATE_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

# Facade module names are emitted as `pub use ::<crate> as <module>;`, so they
# must be legal Rust identifiers AND not collide with a Rust keyword (the `as`
# binding would need raw-identifier spelling). Conservative strict set.
_RUST_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RUST_KEYWORDS = frozenset({
    "as", "break", "const", "continue", "crate", "else", "enum", "extern",
    "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod",
    "move", "mut", "pub", "ref", "return", "self", "Self", "static", "struct",
    "super", "trait", "true", "type", "unsafe", "use", "where", "while",
    "async", "await", "dyn", "abstract", "become", "box", "do", "final",
    "macro", "override", "priv", "typeof", "unsized", "virtual", "yield",
    "try", "union",
})


def _sanitize_crate_name(text: str) -> str:
    """Same normalization the macro applies to a root name."""
    return "".join(c if c.isascii() and (c.islower() or c.isdigit() or c == "_") else "_"
                   for c in text.lower())


def _check_manifest_shape(manifest: dict, errors: list[str]) -> bool:
    version = manifest.get("external_root_manifest_version")
    if version != EXTERNAL_ROOT_MANIFEST_VERSION:
        errors.append(
            f"unknown external_root_manifest_version {version!r} "
            f"(this Aorta understands exactly {EXTERNAL_ROOT_MANIFEST_VERSION})"
        )
        # A manifest of an unknown version is not interpretable: every later
        # check would be reading fields whose meaning is not defined here.
        return False
    unknown = sorted(set(manifest) - MANIFEST_TOP_LEVEL_KEYS)
    missing = sorted(MANIFEST_TOP_LEVEL_KEYS - set(manifest))
    if unknown:
        errors.append(f"manifest has unknown top-level field(s): {unknown}")
    if missing:
        errors.append(f"manifest is missing top-level field(s): {missing}")
    return not (unknown or missing)


def _check_policy_floor(manifest: dict, errors: list[str]) -> None:
    """The manifest carries the macro's policy constants — but not a weakened copy.

    Sharing the constants through the manifest is what keeps the Starlark gate
    and this gate from drifting apart (stage 3 widened both at once by widening
    one list, which is exactly how this is meant to work). What must NOT follow
    from that is a manifest that quietly shrinks the policy it is checked
    against, so the values are held to a FLOOR here: a superset is fine, a
    subset is not — and the floor below is the STAGE-3 one, so a manifest built
    by a stage-2 macro (no Rust in `typed_required_flags`) is rejected instead
    of silently grading an all-language root on a C++/Python-only curve.
    """
    floors = {
        "typed_required_flags": POLICY_FLOOR_TYPED_REQUIRED_FLAGS,
        "include_only_forbidden_flags": POLICY_FLOOR_INCLUDE_ONLY_FORBIDDEN_FLAGS,
        "raw_language_flags": POLICY_FLOOR_RAW_LANGUAGE_FLAGS,
        "required_item_fields": POLICY_FLOOR_REQUIRED_ITEM_FIELDS,
    }
    for field, floor in floors.items():
        missing = sorted(floor - set(manifest[field]))
        if missing:
            errors.append(
                f"manifest {field} is missing {missing}: an external root cannot "
                "narrow the policy it is checked against"
            )
    if manifest["typed_marker_flag"] != POLICY_TYPED_MARKER_FLAG:
        errors.append(
            f"manifest typed_marker_flag must be {POLICY_TYPED_MARKER_FLAG!r}, got "
            f"{manifest['typed_marker_flag']!r}"
        )
    extra_fields = sorted(set(manifest["allowed_item_fields"]) - POLICY_KNOWN_ITEM_FIELDS)
    if extra_fields:
        errors.append(
            f"manifest allows unknown item field(s) {extra_fields}: the fragment "
            "grammar's field set is Aorta's, not the root's"
        )


def _check_paths(manifest: dict, errors: list[str]) -> None:
    package = manifest["package"]
    if not package.startswith("//"):
        errors.append(f"package must be an absolute label, got {package!r}")
    source_package = manifest["source_package"]
    if not (source_package.startswith("//") or source_package.startswith("@")):
        errors.append(f"source_package must be an absolute label, got {source_package!r}")
    prefix = manifest["source_prefix"]
    if prefix:
        if prefix.startswith("/") or prefix.endswith("/"):
            errors.append(f"source_prefix must be a relative directory, got {prefix!r}")
        for segment in prefix.split("/"):
            if segment in ("", ".", ".."):
                errors.append(f"source_prefix must not contain {segment!r}: {prefix!r}")
    if manifest["expected_provenance"] not in ALLOWED_PROVENANCE:
        errors.append(
            f"expected_provenance must be one of {list(ALLOWED_PROVENANCE)}, "
            f"got {manifest['expected_provenance']!r}"
        )


def _check_fragment(manifest: dict, fragment_path: Path, errors: list[str]) -> None:
    recorded = manifest["fragment"]
    actual = sha256_file(fragment_path)
    if recorded.get("sha256") != actual:
        errors.append(
            "fragment sha256 mismatch: manifest says "
            f"{recorded.get('sha256')!r}, the file hashes to {actual!r}"
        )
    try:
        entries = lint_fragment_text(
            fragment_path.read_text(encoding="utf-8"),
            recorded.get("label", str(fragment_path)),
            recorded["symbol"],
            allowed_provenance={manifest["expected_provenance"]},
            allowed_fields=set(manifest["allowed_item_fields"]),
            required_fields=set(manifest["required_item_fields"]),
        )
    except RegistryLintError as err:
        errors.append(f"fragment AST lint failed: {err}")
        return

    package = manifest["package"]
    fragment_items = {
        entry["stem"]: entry for entry in entries if entry.get("pkg") == package
    }
    declared = {item["stem"]: dict(item["fields"]) for item in manifest["items"]}

    missing = sorted(set(fragment_items) - set(declared))
    extra = sorted(set(declared) - set(fragment_items))
    if missing:
        errors.append(
            f"fragment declares item(s) {missing} for {package} that the macro call "
            "did not materialize (the root must pass every entry of its own package)"
        )
    if extra:
        errors.append(
            f"the macro call materialized item(s) {extra} that the fragment "
            f"{recorded.get('label')} does not declare for {package}"
        )
    for stem in sorted(set(fragment_items) & set(declared)):
        fragment_fields = dict(fragment_items[stem])
        declared_fields = declared[stem]
        if fragment_fields != declared_fields:
            differing = sorted(
                key
                for key in set(fragment_fields) | set(declared_fields)
                if fragment_fields.get(key) != declared_fields.get(key)
            )
            errors.append(
                f"item {stem!r} differs between the fragment text and the "
                f"materialized declaration on field(s) {differing}"
            )


def _check_inventory(
    manifest: dict,
    source_paths: Sequence[Path],
    errors: list[str],
) -> None:
    recorded = {entry["path"]: entry for entry in manifest["source_inventory"]}
    actual_paths = {path.as_posix() for path in source_paths}
    if actual_paths != set(recorded):
        errors.append(
            "source inventory mismatch between the manifest and the declared "
            f"Bazel inputs: manifest-only={sorted(set(recorded) - actual_paths)}, "
            f"inputs-only={sorted(actual_paths - set(recorded))}"
        )

    prefix = manifest["source_prefix"]
    basenames: dict[str, str] = {}
    for path in sorted(source_paths, key=lambda p: p.as_posix()):
        text = path.as_posix()
        if not text.endswith(".fbs"):
            errors.append(f"source inventory entry is not a .fbs file: {text}")
            continue
        if ".." in Path(text).parts:
            errors.append(f"source inventory entry escapes its root: {text}")
            continue
        expected_tail = f"/{prefix}/{path.name}" if prefix else f"/{path.name}"
        if not text.endswith(expected_tail):
            errors.append(
                f"source {text} does not sit directly under the declared "
                f"source_prefix {prefix!r} (expected a path ending in {expected_tail!r}); "
                "canonical slots are flat per package, so a nested source has no slot"
            )
        if path.name in basenames:
            errors.append(
                f"two physical sources share the basename {path.name!r}: "
                f"{basenames[path.name]} and {text}"
            )
        else:
            basenames[path.name] = text
        entry = recorded.get(text)
        if entry is not None:
            actual = sha256_file(path)
            if entry.get("sha256") != actual:
                errors.append(
                    f"source {text} sha256 mismatch: manifest says "
                    f"{entry.get('sha256')!r}, the file hashes to {actual!r}"
                )

    declared_roots = {item["fields"]["root_fbs"] for item in manifest["items"]}
    unregistered = sorted(set(basenames) - declared_roots)
    missing_files = sorted(declared_roots - set(basenames))
    if unregistered:
        errors.append(
            f"physical .fbs file(s) under the source root are not registered in the "
            f"fragment: {unregistered} (include-only files get an item with every "
            "flag false — there is no implicit exception table)"
        )
    if missing_files:
        errors.append(
            f"fragment item(s) without exactly one physical source file: {missing_files}"
        )


def _check_package_census(
    manifest: dict,
    package_census_paths: Sequence[Path],
    source_paths: Sequence[Path],
    errors: list[str],
) -> None:
    """Independent physical census of the CALLING package.

    Everything else in this file compares declared things against other
    declared things: the manifest's items, the fragment's entries, the
    `source_inventory` the caller passed, the `<name>_canonical_fbs` the macro
    built. None of those can see a `.fbs` that somebody simply DROPPED into the
    canonical package — it is in no item, in no inventory and in no filegroup,
    so it is in no input set either, and every check stays green while a second
    physical owner of a canonical slot sits in the tree.

    The census breaks that circularity the same way the registry bijection lint does:
    the macro passes a `glob()` of the calling package, i.e. a set the
    FILESYSTEM produced. Here it is compared against the declared sources —
    the only physical `.fbs` an external root may keep in its calling package.

    A package NESTED inside the calling package is beyond `glob()`'s reach; it
    is covered by the recursive `all_source_fbs` the lint test depends on (see
    `_validate_package_census` in external_schema_root.bzl).
    """
    declared = {path.as_posix() for path in source_paths}
    prefix = manifest["source_prefix"]
    local_sources = manifest["source_package"] == manifest["package"]
    seen: set[str] = set()
    for path in sorted(package_census_paths, key=lambda p: p.as_posix()):
        text = path.as_posix()
        if text in declared or text in seen:
            continue
        seen.add(text)
        if local_sources:
            errors.append(
                f"undeclared schema file {text} sits in the canonical package: every "
                f"canonical .fbs of an external root is a GENERATED copy output, so the "
                f"only physical .fbs allowed here are this root's declared sources under "
                f"{prefix!r}"
            )
        else:
            errors.append(
                f"undeclared schema file {text} sits in the canonical package, whose "
                f"sources live in {manifest['source_package']!r}: a shadow package must "
                "contain NO tracked .fbs at all"
            )


def _check_imports(
    manifest: dict,
    source_paths: Sequence[Path],
    canonical_output_paths: Sequence[Path],
    errors: list[str],
) -> None:
    sources = {path.name: path for path in source_paths}
    outputs = {path.name: path for path in canonical_output_paths}
    declared_roots = {item["fields"]["root_fbs"] for item in manifest["items"]}

    stray = sorted(set(outputs) - declared_roots)
    if stray:
        errors.append(f"canonical output(s) with no fragment item: {stray}")

    for record in manifest["imports"]:
        stem = record["stem"]
        root_fbs = f"{stem}.fbs"
        source = sources.get(root_fbs)
        output = outputs.get(root_fbs)
        if source is None:
            errors.append(f"import {stem!r}: no source named {root_fbs!r}")
            continue
        if output is None:
            errors.append(f"import {stem!r}: no canonical output named {root_fbs!r}")
            continue
        source_bytes = source.read_bytes()
        output_bytes = output.read_bytes()
        if source_bytes != output_bytes:
            errors.append(
                f"import {stem!r} is not a byte-for-byte copy: {source} differs from {output}"
            )
        digest = hashlib.sha256(output_bytes).hexdigest()
        if record.get("canonical_output_sha256") != digest:
            errors.append(
                f"import {stem!r} canonical output sha256 mismatch: manifest says "
                f"{record.get('canonical_output_sha256')!r}, the file hashes to {digest!r}"
            )
        if record.get("source_sha256") != hashlib.sha256(source_bytes).hexdigest():
            errors.append(f"import {stem!r} source sha256 mismatch")
        expected_source_label = "{}:{}{}".format(
            manifest["source_package"],
            f"{manifest['source_prefix']}/" if manifest["source_prefix"] else "",
            root_fbs,
        )
        if record.get("source_label") != expected_source_label:
            errors.append(
                f"import {stem!r} source label {record.get('source_label')!r} does not "
                f"match source_package/source_prefix ({expected_source_label!r})"
            )
        expected_output_label = f"{manifest['package']}:{root_fbs}"
        if record.get("canonical_output_label") != expected_output_label:
            errors.append(
                f"import {stem!r} canonical output label "
                f"{record.get('canonical_output_label')!r} != {expected_output_label!r}"
            )


def _check_includes(
    manifest: dict,
    canonical_output_paths: Sequence[Path],
    errors: list[str],
) -> None:
    outputs = {path.name: path for path in canonical_output_paths}
    package = manifest["package"]
    for item in manifest["items"]:
        stem = item["stem"]
        fields = item["fields"]
        root_fbs = fields["root_fbs"]
        path = outputs.get(root_fbs)
        if path is None:
            continue
        deps = list(_flag(fields, "dep_fbs", []) or [])
        dep_keys = [(dep, _dep_key(dep, package)) for dep in deps]
        includes = parse_schema_includes(path)

        for include in includes:
            include_key = _include_key(include)
            matched = [dep for dep, key in dep_keys if _matches(key, include_key)]
            if not matched:
                errors.append(
                    f"item {stem!r} includes {include!r} but declares no matching "
                    f"dep_fbs edge (declared: {deps})"
                )
            elif len(matched) > 1:
                errors.append(
                    f"item {stem!r} include {include!r} matches several dep_fbs "
                    f"edges: {matched}"
                )
        include_keys = [_include_key(include) for include in includes]
        for dep, key in dep_keys:
            if not any(_matches(key, include_key) for include_key in include_keys):
                errors.append(
                    f"item {stem!r} declares dep_fbs {dep!r} but {root_fbs} contains no "
                    "matching include statement"
                )


_ROOT_TYPE_RE = re.compile(r"^\s*root_type\s+[A-Za-z0-9_.]+\s*;", re.MULTILINE)


def declares_root_type(schema_text: str) -> bool:
    """Whether a `.fbs` declares a `root_type`, comments stripped.

    Comment stripping is the shared `schema_include_utils` one, so a
    `// root_type Ghost;` line cannot fake a root type and a commented-out real
    declaration cannot hide one (the comment-blind scanner defect class).
    """
    return _ROOT_TYPE_RE.search(strip_fbs_comments(schema_text)) is not None


def _check_action_root_type(
    stem: str,
    root_fbs: str,
    schema_text: str,
    errors: list[str],
) -> None:
    """An action source's `root_type`, if present, must name its `*Goal` table.

    Both halves come from gen_action_wrapper: `parse_root_type` (which can see
    a namespace-QUALIFIED declaration — a bare-name-only regex silently reads
    `ns.Type` as "no root_type") and `derive_action_prefix` (the one-Goal /
    one-Result / one-FeedbackData shape). A schema whose tables do not form an
    action protocol is reported here rather than surfacing later as an opaque
    genrule failure.
    """
    scan_text = strip_fbs_comments(schema_text)
    declared = parse_root_type(scan_text)
    if declared is None:
        return
    try:
        _prefix, goal_name, _result, _feedback = derive_action_prefix(
            parse_table_names(scan_text)
        )
    except GenerationError as err:
        errors.append(
            f"item {stem!r} is an action but {root_fbs} does not form an action "
            f"protocol: {err}"
        )
        return
    if declared.rsplit(".", 1)[-1] != goal_name:
        errors.append(
            f"item {stem!r} is an action but {root_fbs} declares root_type "
            f"{declared!r}, which does not name its Goal table {goal_name!r}: the "
            "action aggregate's root type IS the Goal table"
        )


def _check_root_type_closure(
    manifest: dict,
    canonical_output_paths: Sequence[Path],
    errors: list[str],
) -> None:
    """`bfbs` must mean what the FILE says.

    The typed/include-only classification is mechanically self-consistent —
    without `bfbs = True` no BFBS, schema-meta or typed wrapper is generated —
    but that is only half a contract. Read from the other side, a schema that
    really HAS a root type could declare itself raw/include-only and quietly
    escape the "first class in every supported language" policy, while the code
    comments claim `bfbs` exists "exactly when the schema has a root type".
    This closes the loop against the canonical file's own content.

    Actions are the one legitimate asymmetry, and the rule is
    `gen_action_wrapper`'s, not a stricter local one: the aggregate's root type
    IS the Goal table, so the minimal schema may either omit `root_type`
    entirely or declare exactly that Goal table (bare or namespace-qualified).
    Anything else is the mismatch gen_action_wrapper hard-errors on, and it is
    caught here first, against the canonical copy, with the schema named.

    An earlier form of this check forbade `root_type` on an action source
    outright. That was the shape of the one in-repo EDU fixture, not the
    contract: every one of Aorta's own migrated action schemas declares
    `root_type <Prefix>Goal`, so the strict rule would have made a family's
    `.fbs` un-migratable without editing it — a semantic edit smuggled into a
    file move, which is exactly what the migration exists to avoid.
    """
    outputs = {path.name: path for path in canonical_output_paths}
    for item in manifest["items"]:
        stem = item["stem"]
        fields = item["fields"]
        path = outputs.get(fields.get("root_fbs", ""))
        if path is None:
            continue
        schema_text = path.read_text(encoding="utf-8")
        has_root_type = declares_root_type(schema_text)
        typed = bool(_flag(fields, manifest["typed_marker_flag"]))
        if _flag(fields, "action_schema"):
            if has_root_type:
                _check_action_root_type(stem, fields["root_fbs"], schema_text, errors)
            if not typed:
                errors.append(f"item {stem!r} is an action but does not enable bfbs")
            continue
        if has_root_type and not typed:
            errors.append(
                f"item {stem!r} declares {manifest['typed_marker_flag']}=False but "
                f"{fields['root_fbs']} declares a root_type: a schema with a root type "
                "must be first class in every supported language, not raw/include-only"
            )
        elif typed and not has_root_type:
            errors.append(
                f"item {stem!r} enables {manifest['typed_marker_flag']} but "
                f"{fields['root_fbs']} declares no root_type"
            )


def _check_flags(manifest: dict, errors: list[str]) -> None:
    """The first-class-language policy, re-derived from the fragment's own flags.

    The policy CONSTANTS travel in the manifest (they are the macro's, not this
    file's), so the Starlark gate and this gate can never drift into disagreeing
    about what "first class" means.
    """
    marker = manifest["typed_marker_flag"]
    typed_required = list(manifest["typed_required_flags"])
    include_only_forbidden = list(manifest["include_only_forbidden_flags"])
    raw_flags = list(manifest["raw_language_flags"])
    refused = list(manifest["refused_flags"])
    items_by_stem = {item["stem"]: item for item in manifest["items"]}
    package = manifest["package"]

    for item in manifest["items"]:
        stem = item["stem"]
        fields = item["fields"]
        for flag in refused:
            if _flag(fields, flag):
                errors.append(
                    f"item {stem!r} enables {flag!r}, which this Aorta refuses for "
                    "external roots (the surface is not generated yet)"
                )
        if _flag(fields, marker):
            # SCOPE (see the matching comment in external_schema_root.bzl):
            # the first-class policy binds consumer (b). An `internal` root's items
            # are Aorta registry entries living in another repository; their
            # flag policy is Aorta's registry policy, enforced by
            # registry_lint + the frozen v3 manifest golden, and 86 of them
            # deliberately are not first class. `internal` is granted only by
            # Aorta's own allowlist (`_check_provenance_gate`), so a user's
            # repository cannot reach this branch.
            if manifest["expected_provenance"] == "external":
                missing = [flag for flag in typed_required if not _flag(fields, flag)]
                if missing:
                    errors.append(
                        f"item {stem!r} has a root type ({marker}=True) but does not "
                        f"enable {missing}: such an item must be first class in every "
                        f"supported language — the full set is {typed_required}"
                    )
                # Same scope as the floor above (and as the matching Starlark
                # gate): the Python schema-meta module is part of owner
                # decision 4's every-language surface. An `internal` root's
                # C++-only typed entry legitimately has no Python codegen, so
                # its EFFECTIVE `schema_meta_py` is already False and demanding
                # True there would reject a valid registry entry. The default
                # mirrors `schema_meta_py_enabled` in message/schemas/registry.bzl
                # (`python and bfbs`), which the external floor forces to True.
                effective_schema_meta_py = fields.get(
                    "schema_meta_py",
                    fields.get("python", False) and fields.get("bfbs", False),
                )
                if not effective_schema_meta_py:
                    errors.append(f"item {stem!r} is typed but disables schema_meta_py")
        else:
            for flag in include_only_forbidden:
                if flag != marker and _flag(fields, flag):
                    errors.append(
                        f"item {stem!r} has no root type ({marker}=False) but enables "
                        f"{flag!r}, which only exists for a root type"
                    )
            enabled_raw = [flag for flag in raw_flags if _flag(fields, flag)]
            if enabled_raw and len(enabled_raw) != len(raw_flags):
                errors.append(
                    f"item {stem!r} enables raw {enabled_raw} but not "
                    f"{[f for f in raw_flags if f not in enabled_raw]}: raw codegen for "
                    "an include-only schema is per-language symmetric"
                )
        # An in-root dependency must carry at least the raw codegen its
        # dependents need — flatc's generated header/module for a schema
        # includes the generated header/module of everything it includes.
        for dep in _flag(fields, "dep_fbs", []) or []:
            dep_pkg, dep_name = _dep_key(dep, package)
            if dep_pkg != _package_path(package):
                continue
            dep_item = items_by_stem.get(dep_name[: -len(".fbs")])
            if dep_item is None:
                continue
            for language in raw_flags:
                if _flag(fields, language) and not _flag(dep_item["fields"], language):
                    errors.append(
                        f"item {stem!r} enables {language!r} but its in-root dependency "
                        f"{dep_item['stem']!r} does not"
                    )
        if _flag(fields, "action_schema") and not _flag(fields, "action_name", ""):
            errors.append(f"item {stem!r} sets action_schema without action_name")
        if _flag(fields, "action_name", "") and not _flag(fields, "action_schema"):
            errors.append(f"item {stem!r} sets action_name without action_schema")
        for aorta_only in POLICY_AORTA_ONLY_ITEM_FIELDS:
            if manifest["expected_provenance"] != "internal" and aorta_only in fields:
                errors.append(
                    f"item {stem!r} sets {aorta_only!r}: that flag selects an AORTA "
                    "aggregate and must not appear in an EXTERNAL-root fragment"
                )
            if fields.get(aorta_only, True) is not True:
                errors.append(
                    f"item {stem!r} sets {aorta_only!r} to a non-true value; "
                    "the flag is omitted unless it is True"
                )
        if fields.get("provenance") != manifest["expected_provenance"]:
            errors.append(
                f"item {stem!r} declares provenance {fields.get('provenance')!r}, "
                f"expected {manifest['expected_provenance']!r}"
            )
        if fields.get("pkg") != manifest["package"]:
            errors.append(
                f"item {stem!r} declares pkg {fields.get('pkg')!r}, but the root "
                f"expands in {manifest['package']!r}"
            )
        if fields.get("root_fbs") != f"{stem}.fbs":
            errors.append(
                f"item {stem!r} root_fbs {fields.get('root_fbs')!r} must be '<stem>.fbs'"
            )


def _check_name_uniqueness(manifest: dict, errors: list[str]) -> None:
    marker = manifest["typed_marker_flag"]
    owners: dict[str, str] = {}
    crate_owners: dict[str, str] = {}
    facade_owners: dict[str, str] = {}

    def claim(target: str, what: str) -> None:
        if target in owners:
            errors.append(f"target name collision on {target!r}: {owners[target]} vs {what}")
        else:
            owners[target] = what

    def claim_crate(crate_name: str, what: str) -> None:
        # Checked for EVERY typed item, not just the ones that spell
        # `rust_wrapper_crate_name` out: two items whose DEFAULT crate names
        # coincide collide just as hard, and only surface as an ambiguous-name
        # rustc error in whichever downstream crate happens to depend on both.
        if not _CRATE_NAME_RE.match(crate_name):
            errors.append(
                f"{what}: crate name {crate_name!r} must be lower-case ASCII letters, "
                "digits and underscores, and must not start with a digit"
            )
        if crate_name in crate_owners:
            errors.append(
                f"Rust crate name collision on {crate_name!r}: "
                f"{crate_owners[crate_name]} vs {what}"
            )
        else:
            crate_owners[crate_name] = what

    def claim_facade(module_name: str, what: str) -> None:
        # The root Rust facade emits `pub use ::<crate> as <module>;` per typed
        # item. The module name (defaulted from alias_id, or spelled out
        # via rust_wrapper_facade_name) must be a legal Rust identifier, must
        # not be a Rust keyword, and must be unique across the root's items —
        # otherwise the facade is uncompilable or two items silently shadow
        # each other (rustc error only later, in a downstream crate).
        if not _RUST_IDENT_RE.match(module_name):
            errors.append(
                f"{what}: facade module name {module_name!r} is not a legal Rust "
                "identifier ([A-Za-z_][A-Za-z0-9_]*); alias_id values with "
                "hyphens need an explicit rust_wrapper_facade_name override"
            )
        elif module_name in _RUST_KEYWORDS:
            errors.append(
                f"{what}: facade module name {module_name!r} is a Rust keyword "
                "and cannot be used as a `pub use ... as <name>` binding"
            )
        if module_name in facade_owners:
            errors.append(
                f"Rust facade module name collision on {module_name!r}: "
                f"{facade_owners[module_name]} vs {what}"
            )
        else:
            facade_owners[module_name] = what

    for item in manifest["items"]:
        stem = item["stem"]
        fields = item["fields"]
        alias_id = _flag(fields, "alias_id", stem)
        claim(fields["root_fbs"], f"canonical output of {stem!r}")
        claim(f"{stem}_fbs_closure", f"closure of {stem!r}")
        claim(f"{stem}_rust_closure", f"Rust module closure of {stem!r}")
        if _flag(fields, "cpp"):
            claim(_flag(fields, "cpp_lib_name", f"{stem}_cpp_lib"), f"C++ library of {stem!r}")
            claim(_flag(fields, "cc_alias", f"{stem}_cc"), f"C++ alias of {stem!r}")
        if _flag(fields, "python"):
            claim(f"{stem}_py_lib", f"python library of {stem!r}")
        if _flag(fields, "rust"):
            claim(f"{stem}_rust_all", f"raw Rust aggregate of {stem!r}")
        if _flag(fields, marker):
            claim(f"{stem}_bfbs", f"bfbs of {stem!r}")
            claim(f"{stem}_schema_meta", f"Rust schema meta of {stem!r}")
        # The typed wrappers follow their OWN flags, mirroring the macro: for an
        # external root both are required, for an internal one the registry
        # entry decides whether the binding exists at all.
        if _flag(fields, marker) and _flag(fields, "cpp_wrapper"):
            claim(
                _flag(fields, "cpp_wrapper_name", _message_base(alias_id) + "_cc"),
                f"typed C++ binding of {stem!r}",
            )
        if _flag(fields, marker) and _flag(fields, "rust_wrapper"):
            rust_name = _flag(fields, "rust_wrapper_name", _message_base(alias_id) + "_rs")
            claim(rust_name, f"typed Rust binding of {stem!r}")
            claim_crate(
                _flag(fields, "rust_wrapper_crate_name", _message_base(alias_id)),
                f"typed Rust binding of {stem!r}",
            )
            claim_facade(
                _flag(fields, "rust_wrapper_facade_name", _message_base(alias_id)),
                f"Rust facade module of {stem!r}",
            )

    name = manifest["name"]
    for suffix in ("_cc", "_py", "_py_tree", "_rs", "_canonical_fbs", "_manifest", "_lint_test"):
        claim(name + suffix, "root aggregate")
    claim_crate(_sanitize_crate_name(name), f"root Rust facade {name + '_rs'!r}")


def _check_provenance_gate(
    manifest: dict,
    internal_root_allowlist_text: str | None,
    errors: list[str],
) -> None:
    package = manifest["package"]
    name = manifest["name"]
    expected = manifest["expected_provenance"]
    if internal_root_allowlist_text is None:
        if expected == "internal":
            errors.append(
                "expected_provenance='internal' cannot be verified without Aorta's "
                "internal-root allowlist"
            )
        return
    try:
        entries = parse_internal_root_allowlist(
            internal_root_allowlist_text, "message/schemas/external_roots.bzl"
        )
    except RegistryLintError as err:
        errors.append(f"internal-root allowlist is not valid pure data: {err}")
        return
    match = next(
        (e for e in entries if e["pkg"] == package and e["name"] == name),
        None,
    )
    if expected == "internal":
        if match is None:
            errors.append(
                f"root {package}:{name} claims provenance 'internal' but Aorta's "
                "allowlist (message/schemas/external_roots.bzl) does not declare it; "
                "a repository outside Aorta's release ownership cannot grant itself "
                "Aorta provenance"
            )
        elif match["source_package"] != manifest["source_package"]:
            errors.append(
                f"root {package}:{name} declares source_package "
                f"{manifest['source_package']!r} but Aorta's allowlist pins "
                f"{match['source_package']!r}"
            )
    elif match is not None:
        errors.append(
            f"root {package}:{name} is declared as an INTERNAL root by Aorta but "
            f"claims provenance {expected!r}"
        )


def _check_canonical_slots(
    manifest: dict,
    registry_manifest: dict | None,
    errors: list[str],
) -> None:
    package = manifest["package"]
    expected = manifest["expected_provenance"]
    under_canonical_root = package.startswith(CANONICAL_SCHEMA_PACKAGE_PREFIX)
    if expected == "internal" and not under_canonical_root:
        errors.append(
            f"internal root {package} is not under {CANONICAL_SCHEMA_PACKAGE_PREFIX}, so "
            "its outputs have no canonical slot in the release tree"
        )
    if registry_manifest is None:
        return
    registry_slots: dict[str, tuple[str, str]] = {}
    for entry in registry_manifest.get("schema_definitions", []):
        pkg = entry.get("pkg", "")
        if not pkg.startswith(CANONICAL_SCHEMA_PACKAGE_PREFIX):
            continue
        rel = f"{pkg[len(CANONICAL_SCHEMA_PACKAGE_PREFIX):]}/{entry.get('root_fbs')}"
        registry_slots[rel] = (pkg, entry.get("stem", ""))

    if not under_canonical_root:
        # An EDU root lives entirely in the user's own repository: it owns no
        # canonical slot at all, so it cannot collide with one.
        return
    package_rel = package[len(CANONICAL_SCHEMA_PACKAGE_PREFIX):]
    for item in manifest["items"]:
        stem = item["stem"]
        rel = f"{package_rel}/{item['fields']['root_fbs']}"
        owner = registry_slots.get(rel)
        if owner is None:
            continue
        if owner != (package, stem):
            errors.append(
                f"canonical slot {rel!r} is already owned by the Aorta registry entry "
                f"{owner[0]}:{owner[1]}; an external root may not re-claim it"
            )
        elif expected != "internal":
            errors.append(
                f"canonical slot {rel!r} belongs to Aorta registry entry "
                f"{owner[0]}:{owner[1]}, which requires expected_provenance='internal'"
            )


def lint_external_root(
    manifest: dict,
    fragment_path: Path,
    source_paths: Sequence[Path],
    canonical_output_paths: Sequence[Path],
    registry_manifest: dict | None = None,
    internal_root_allowlist_text: str | None = None,
    package_census_paths: Sequence[Path] = (),
) -> list[str]:
    """Run every content check; returns the (possibly empty) error list."""
    errors: list[str] = []
    if not _check_manifest_shape(manifest, errors):
        return errors
    _check_policy_floor(manifest, errors)
    _check_paths(manifest, errors)
    _check_fragment(manifest, fragment_path, errors)
    _check_inventory(manifest, source_paths, errors)
    _check_package_census(manifest, package_census_paths, source_paths, errors)
    _check_imports(manifest, source_paths, canonical_output_paths, errors)
    _check_includes(manifest, canonical_output_paths, errors)
    _check_root_type_closure(manifest, canonical_output_paths, errors)
    _check_flags(manifest, errors)
    _check_name_uniqueness(manifest, errors)
    _check_provenance_gate(manifest, internal_root_allowlist_text, errors)
    _check_canonical_slots(manifest, registry_manifest, errors)
    return errors


def load_manifest(path: Path) -> dict:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ExternalRootLintError(f"{path}: external-root manifest must be a JSON object")
    return document


def format_errors(manifest_path: Path, errors: Iterable[str]) -> str:
    listed = "\n  - ".join(errors)
    return f"external schema root lint failed ({manifest_path}):\n  - {listed}"
