#!/usr/bin/env python3
"""Fail-closed checks for pure-data schema registry fragments."""

from __future__ import annotations

import ast
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


class RegistryLintError(ValueError):
    pass


@dataclass(frozen=True)
class RegistryEntry:
    values: dict[str, Any]
    fragment: str

    @property
    def key(self) -> tuple[str, str]:
        return self.values["pkg"], self.values["stem"]

    @property
    def provenance(self) -> str:
        return self.values["provenance"]


# Aorta's registry is base-only — one local
# fragment, public/fixture provenance only. Product entries live in the
# self-hosted schema module and never enter this lint.
FRAGMENT_SPECS = (
    (
        "message/schemas/registry_local.bzl",
        "LOCAL_SCHEMA_DEFINITIONS",
        frozenset({"public", "fixture"}),
        None,
    ),
)

# Copy-root (external-root) fragments: the four domain fragment names an
# external schema root's registry entries may arrive under, keyed by basename
# because the lint receives them as runfiles paths (see
# `external_fragment_paths`). Post-A1 no production root declares any — the
# mechanism is exercised only by the test-only fixture root and the bijection
# tests' synthetic migrated package — but the grammar stays so a copy root's
# entries are linted with the same provenance/domain rules as before.
EXTERNAL_FRAGMENT_SPECS = {
    "fragment_topic.bzl": ("PRODUCT_TOPIC_SCHEMAS", frozenset({"internal"}), "topic"),
    "fragment_service.bzl": ("PRODUCT_SERVICE_SCHEMAS", frozenset({"internal"}), "service"),
    "fragment_action.bzl": ("PRODUCT_ACTION_SCHEMAS", frozenset({"internal"}), "action"),
    "fragment_shared_internal.bzl": (
        "PRODUCT_SHARED_SCHEMAS",
        frozenset({"internal"}),
        "shared",
    ),
}

# Space-separated runfiles paths of copy-root fragments (tests only post-A1).
EXTERNAL_FRAGMENTS_ENV = "AORTA_SCHEMA_EXTERNAL_FRAGMENTS"

INTERNAL_EXTERNAL_ROOTS_REL = "message/schemas/external_roots.bzl"
_INTERNAL_ROOT_LIST_NAME = "INTERNAL_EXTERNAL_SCHEMA_ROOTS"
_INTERNAL_ROOT_FIELDS = frozenset({"pkg", "name", "source_package", "test_only"})
_INTERNAL_ROOT_REQUIRED_FIELDS = frozenset({"pkg", "name", "source_package"})

_CONSTANT_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_FIXTURE_SHARED_PACKAGES = {
    "abcd_dep",
    "branch_common",
    "camelcase_probe",
    "collide_fixtures",
    "deepshadow_dep",
    "enum_abcd_dep",
    "mixedroot_dep",
    "nested_dep",
    "sharedfork_dep",
    "union_dep",
    "xtree_common",
}
def repo_root() -> Path:
    srcdir = os.environ.get("TEST_SRCDIR")
    workspace = os.environ.get("TEST_WORKSPACE")
    if srcdir and workspace:
        candidate = Path(srcdir) / workspace
        if (candidate / "message/schemas/registry_local.bzl").is_file():
            return candidate
    candidate = Path(__file__).resolve().parents[2]
    if (candidate / "message/schemas/registry_local.bzl").is_file():
        return candidate
    raise RegistryLintError("could not locate the Aorta repository root")


def _literal_value(node: ast.AST, constants: dict[str, str], path: str) -> Any:
    if isinstance(node, ast.Constant) and (
        node.value is None or isinstance(node.value, (str, bool, int, float))
    ):
        return node.value
    if isinstance(node, ast.List):
        return [_literal_value(item, constants, path) for item in node.elts]
    if isinstance(node, ast.Name):
        if node.id not in constants:
            raise RegistryLintError(
                f"{path}:{node.lineno}: identifier '{node.id}' must reference a previously declared string constant"
            )
        return constants[node.id]
    raise RegistryLintError(
        f"{path}:{getattr(node, 'lineno', '?')}: values must be scalar/list literals or local string constants"
    )


def _parse_entry(
    node: ast.AST,
    constants: dict[str, str],
    path: str,
    allowed_provenance: frozenset[str] | None,
    allowed_fields: frozenset[str] | None = None,
    required_fields: frozenset[str] = frozenset({"pkg", "stem", "root_fbs", "provenance"}),
) -> dict[str, Any]:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name) or node.func.id != "struct":
        raise RegistryLintError(f"{path}:{getattr(node, 'lineno', '?')}: list entries must be struct(...) calls")
    if node.args:
        raise RegistryLintError(f"{path}:{node.lineno}: struct(...) only accepts named fields")
    values: dict[str, Any] = {}
    for keyword in node.keywords:
        if keyword.arg is None:
            raise RegistryLintError(f"{path}:{node.lineno}: **kwargs are forbidden")
        if keyword.arg in values:
            raise RegistryLintError(f"{path}:{node.lineno}: duplicate field '{keyword.arg}'")
        values[keyword.arg] = _literal_value(keyword.value, constants, path)

    missing = set(required_fields) - values.keys()
    if missing:
        raise RegistryLintError(f"{path}:{node.lineno}: entry missing fields {sorted(missing)}")
    # An external-root fragment declares its own allowed field
    # set, so a typo (or a smuggled-in projection field like `edu_public`,
    # which only the schema module's fragments may set) is an error instead
    # of a silently ignored key. Aorta's own fragments keep the historical
    # no-allowlist behaviour (allowed_fields=None).
    if allowed_fields is not None:
        unknown = sorted(values.keys() - set(allowed_fields))
        if unknown:
            raise RegistryLintError(
                f"{path}:{node.lineno}: entry has unknown field(s) {unknown} "
                f"(allowed: {sorted(allowed_fields)})"
            )
    if allowed_provenance is not None:
        provenance = values["provenance"]
        if provenance not in allowed_provenance:
            raise RegistryLintError(
                f"{path}:{node.lineno}: provenance '{provenance}' is not allowed in this fragment"
            )
    if "edu_public" in values and values["edu_public"] is not True:
        raise RegistryLintError(f"{path}:{node.lineno}: edu_public must be omitted unless it is true")
    return values


def lint_fragment_text(
    text: str,
    path: str,
    list_name: str,
    allowed_provenance: frozenset[str] | set[str] | None,
    allowed_fields: frozenset[str] | set[str] | None = None,
    required_fields: frozenset[str] | set[str] = frozenset(
        {"pkg", "stem", "root_fbs", "provenance"}
    ),
    key_fields: tuple[str, ...] = ("pkg", "stem"),
) -> list[dict[str, Any]]:
    """Parse a PURE-DATA fragment: module-level assignments, string constants
    declared before the list, and `struct(...)` literals only.

    The grammar core is parameterized (pure syntax parsing is kept separate
    from domain/provenance derivation) so an EXTERNAL root's fragment — and Aorta's
    own internal-root allowlist — reuse the exact same AST allowlist instead of
    growing a second parser with its own holes. `allowed_provenance=None`
    disables the provenance check for lists that carry no provenance field.
    """
    try:
        module = ast.parse(text, filename=path)
    except SyntaxError as err:
        raise RegistryLintError(f"{path}:{err.lineno}: invalid fragment syntax: {err.msg}") from err

    constants: dict[str, str] = {}
    entries: list[dict[str, Any]] | None = None
    for statement in module.body:
        # A module docstring is inert data in both Python and Starlark, and an
        # external-root fragment is a file a USER maintains — it must be able
        # to explain itself. Nothing else in expression position is allowed.
        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant):
            if isinstance(statement.value.value, str):
                continue
            raise RegistryLintError(
                f"{path}:{statement.lineno}: only a docstring may appear in expression position"
            )
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            raise RegistryLintError(
                f"{path}:{getattr(statement, 'lineno', '?')}: only module-level assignments are allowed"
            )
        target = statement.targets[0]
        if not isinstance(target, ast.Name):
            raise RegistryLintError(f"{path}:{statement.lineno}: assignment target must be a name")
        if target.id == list_name:
            if entries is not None:
                raise RegistryLintError(f"{path}:{statement.lineno}: duplicate '{list_name}' assignment")
            if not isinstance(statement.value, ast.List):
                raise RegistryLintError(f"{path}:{statement.lineno}: '{list_name}' must be a list literal")
            entries = [
                _parse_entry(
                    item,
                    constants,
                    path,
                    None if allowed_provenance is None else frozenset(allowed_provenance),
                    None if allowed_fields is None else frozenset(allowed_fields),
                    frozenset(required_fields),
                )
                for item in statement.value.elts
            ]
            continue
        if entries is not None:
            raise RegistryLintError(f"{path}:{statement.lineno}: constants must precede the entry list")
        if not _CONSTANT_NAME_RE.fullmatch(target.id):
            raise RegistryLintError(f"{path}:{statement.lineno}: invalid string constant name '{target.id}'")
        if target.id in constants:
            raise RegistryLintError(f"{path}:{statement.lineno}: duplicate string constant '{target.id}'")
        if not isinstance(statement.value, ast.Constant) or not isinstance(statement.value.value, str):
            raise RegistryLintError(f"{path}:{statement.lineno}: fragment constants must be string literals")
        constants[target.id] = statement.value.value

    if entries is None:
        raise RegistryLintError(f"{path}: missing '{list_name}' assignment")
    keys = [tuple(entry[field] for field in key_fields) for entry in entries]
    if keys != sorted(keys):
        raise RegistryLintError(
            f"{path}: entries must be sorted by ({', '.join(key_fields)})"
        )
    if len(keys) != len(set(keys)):
        raise RegistryLintError(f"{path}: duplicate ({', '.join(key_fields)}) key")
    return entries


def _source_path(entry: RegistryEntry) -> str:
    pkg = entry.values["pkg"]
    root_fbs = entry.values["root_fbs"]
    prefix = "//message/schemas/"
    if not isinstance(pkg, str) or not pkg.startswith(prefix):
        raise RegistryLintError(f"{entry.fragment}: invalid schema package '{pkg}'")
    if not isinstance(root_fbs, str) or Path(root_fbs).name != root_fbs or root_fbs in {".", ".."}:
        raise RegistryLintError(f"{entry.fragment}: invalid root_fbs '{root_fbs}'")
    return f"message/schemas/{pkg.removeprefix(prefix)}/{root_fbs}"


def _expected_provenance(source_path: str) -> str:
    relative = source_path.removeprefix("message/schemas/")
    parts = relative.split("/")
    if len(parts) < 2:
        raise RegistryLintError(f"invalid schema source path '{source_path}'")
    domain = parts[0]
    package = parts[1]
    if domain == "system":
        return "public"
    if domain == "shared":
        if package in {"audio_common", "follow_common", "fsm_common"}:
            return "internal"
        if package in _FIXTURE_SHARED_PACKAGES:
            return "fixture"
        return "public"
    if domain in {"action", "service", "topic"}:
        if package == "example":
            return "fixture"
        if domain == "topic" and package == "foxglove_schema":
            return "public"
        return "internal"
    raise RegistryLintError(f"unknown schema domain in '{source_path}'")


def external_fragment_paths() -> list[Path]:
    """Runfiles paths of copy-root (external) schema fragments.

    Fail-CLOSED: a caller that declared production external roots but cannot
    see their fragments would silently lint a registry that is missing every
    migrated family, and the bijection would then "prove" a slot has no owner.
    So the absence of the environment variable is not "no migrated entries" —
    `load_registry_entries` treats it as an error whenever the allowlist has a
    production entry. Post-A1 the allowlist has none, so this returns [] in
    the real tree and only the tests feed it.
    """
    raw = os.environ.get(EXTERNAL_FRAGMENTS_ENV, "").strip()
    if not raw:
        return []
    paths = []
    for token in raw.split():
        path = Path(token)
        if not path.is_absolute():
            path = Path.cwd() / path
        if path.name not in EXTERNAL_FRAGMENT_SPECS:
            raise RegistryLintError(
                f"{EXTERNAL_FRAGMENTS_ENV} names an unknown fragment '{path.name}'; "
                f"expected one of {sorted(EXTERNAL_FRAGMENT_SPECS)}"
            )
        if not path.is_file():
            raise RegistryLintError(f"{EXTERNAL_FRAGMENTS_ENV} names a missing file: {path}")
        paths.append(path)
    return paths


def load_internal_external_roots(root: Path) -> list[dict[str, Any]]:
    """Aorta's allowlist of consumer-(a)-class external schema roots.

    Parsed with the SAME pure-data grammar core as a fragment, so the allowlist
    cannot grow logic and the lint never trusts an already-evaluated Starlark
    value.
    """
    path = root / INTERNAL_EXTERNAL_ROOTS_REL
    return lint_fragment_text(
        path.read_text(encoding="utf-8"),
        INTERNAL_EXTERNAL_ROOTS_REL,
        _INTERNAL_ROOT_LIST_NAME,
        allowed_provenance=None,
        allowed_fields=_INTERNAL_ROOT_FIELDS,
        required_fields=_INTERNAL_ROOT_REQUIRED_FIELDS,
        key_fields=("pkg", "name"),
    )


def migrated_packages(root: Path) -> dict[str, dict[str, Any]]:
    """`pkg -> allowlist entry` for every PRODUCTION internal external root.

    Fixture roots (`test_only = True`) are excluded: they exercise the
    mechanism and are never part of the release tree, so their packages are not
    migrated product packages.
    """
    return {
        entry["pkg"]: entry
        for entry in load_internal_external_roots(root)
        if not entry.get("test_only", False)
    }


def _load_fragment(
    path: Path,
    label: str,
    list_name: str,
    allowed_provenance,
    expected_domain: str | None,
) -> list[RegistryEntry]:
    entries = []
    for item in lint_fragment_text(
        path.read_text(encoding="utf-8"),
        label,
        list_name,
        allowed_provenance,
    ):
        entry = RegistryEntry(item, label)
        source_path = _source_path(entry)
        actual_domain = source_path.removeprefix("message/schemas/").split("/", 1)[0]
        if expected_domain is not None and actual_domain != expected_domain:
            raise RegistryLintError(
                f"{label}: entry {entry.key} belongs to domain '{actual_domain}', expected '{expected_domain}'"
            )
        expected_provenance = _expected_provenance(source_path)
        if entry.provenance != expected_provenance:
            raise RegistryLintError(
                f"{label}: entry {entry.key} has provenance '{entry.provenance}', expected '{expected_provenance}'"
            )
        entries.append(entry)
    return entries


def load_registry_entries(root: Path) -> list[RegistryEntry]:
    """Every registry entry, in-repo and migrated alike.

    A migrated family's `.fbs` left this repository; its registry ENTRY did
    not. Both fragment sources are parsed with the identical grammar, domain
    and provenance rules, so from here down there is exactly one registry —
    which is what keeps the v3 manifest, the alias face and the EDU projection
    unchanged across a migration.
    """
    entries: list[RegistryEntry] = []
    for relative, list_name, allowed_provenance, expected_domain in FRAGMENT_SPECS:
        entries.extend(
            _load_fragment(
                root / relative, relative, list_name, allowed_provenance, expected_domain
            )
        )

    external_paths = external_fragment_paths()
    if not external_paths and migrated_packages(root):
        raise RegistryLintError(
            f"{INTERNAL_EXTERNAL_ROOTS_REL} declares migrated package(s) "
            f"{sorted(migrated_packages(root))} but {EXTERNAL_FRAGMENTS_ENV} is unset: "
            "refusing to lint a registry that is missing every migrated entry"
        )
    for path in external_paths:
        list_name, allowed_provenance, expected_domain = EXTERNAL_FRAGMENT_SPECS[path.name]
        entries.extend(
            _load_fragment(
                path,
                str(path),
                list_name,
                allowed_provenance,
                expected_domain,
            )
        )
    return entries


def _duplicates(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for item in items:
        if item in seen:
            duplicates.add(item)
        seen.add(item)
    return sorted(duplicates)


def validate_alias_uniqueness(entries: list[RegistryEntry]) -> int:
    root_aliases = []
    package_aliases = []
    for entry in entries:
        item = entry.values
        stem = item["stem"]
        alias_id = item.get("alias_id", stem)
        for field, suffix in (
            ("rust", "rust"),
            ("cpp", "cpp"),
            ("python", "py"),
            ("bfbs", "bfbs"),
            ("schema_meta_rust", "schema_meta"),
            ("schema_meta_cpp", "schema_meta_cpp"),
        ):
            if item.get(field, False):
                root_aliases.append(f"fbs_schema_{alias_id}_{suffix}")

        cpp_lib = item.get("cpp_lib", item.get("cpp", False))
        cpp_lib_name = item.get("cpp_lib_name", stem + "_cpp_lib")
        if cpp_lib:
            root_aliases.append(f"fbs_schema_{alias_id}_cpp_lib")
        if "cc_target" in item:
            cc_target = item["cc_target"]
        elif "cc_alias" in item:
            cc_target = item["cc_alias"]
        elif cpp_lib:
            cc_target = stem + "_cc"
        else:
            cc_target = None
        if cc_target is not None:
            root_aliases.append(f"fbs_schema_{alias_id}_cc")
        if cpp_lib and cc_target is not None and cc_target != cpp_lib_name:
            package_aliases.append(f"{item['pkg']}:{cc_target}")

        message_base = alias_id if alias_id.endswith("_msg") else alias_id + "_msg"
        if item.get("rust_wrapper", False):
            root_aliases.append(item.get("rust_wrapper_name", message_base + "_rs"))
        if item.get("cpp_wrapper", False):
            root_aliases.append(item.get("cpp_wrapper_name", message_base + "_cc"))

    duplicate_root = _duplicates(root_aliases)
    duplicate_package = _duplicates(package_aliases)
    if duplicate_root or duplicate_package:
        raise RegistryLintError(
            f"duplicate aliases: root={duplicate_root}, package={duplicate_package}"
        )
    return len(root_aliases) + len(package_aliases)


def _schema_package_export_files(build_text: str, build_path: str, pkg: str) -> bool:
    try:
        module = ast.parse(build_text, filename=build_path)
    except SyntaxError as err:
        raise RegistryLintError(f"{build_path}:{err.lineno}: invalid BUILD syntax: {err.msg}") from err

    package_values = []
    for statement in module.body:
        if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
            continue
        target = statement.targets[0]
        if isinstance(target, ast.Name) and target.id == "_SCHEMA_PACKAGE":
            if not isinstance(statement.value, ast.Constant) or not isinstance(statement.value.value, str):
                raise RegistryLintError(f"{build_path}:{statement.lineno}: _SCHEMA_PACKAGE must be a string literal")
            package_values.append(statement.value.value)
    if package_values != [pkg]:
        raise RegistryLintError(
            f"{build_path}: expected exactly one _SCHEMA_PACKAGE assignment to '{pkg}'"
        )

    calls = [
        node
        for node in ast.walk(module)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "aorta_define_schema_package"
    ]
    if len(calls) != 1:
        raise RegistryLintError(f"{build_path}: expected exactly one aorta_define_schema_package call")

    call = calls[0]
    if not any(isinstance(statement, ast.Expr) and statement.value is call for statement in module.body):
        raise RegistryLintError(f"{build_path}:{call.lineno}: aorta_define_schema_package must be a top-level call")
    if len(call.args) != 1:
        raise RegistryLintError(
            f"{build_path}:{call.lineno}: aorta_define_schema_package must have exactly one positional argument"
        )
    items_call = call.args[0]
    if (
        not isinstance(items_call, ast.Call)
        or not isinstance(items_call.func, ast.Name)
        or items_call.func.id != "schema_items_for_package"
        or len(items_call.args) != 1
        or items_call.keywords
        or not isinstance(items_call.args[0], ast.Name)
        or items_call.args[0].id != "_SCHEMA_PACKAGE"
    ):
        raise RegistryLintError(
            f"{build_path}:{call.lineno}: first argument must be schema_items_for_package(_SCHEMA_PACKAGE)"
        )

    export_keywords = [keyword for keyword in call.keywords if keyword.arg == "export_files"]
    if len(export_keywords) > 1:
        raise RegistryLintError(f"{build_path}:{call.lineno}: duplicate export_files keyword")
    if not export_keywords:
        return True
    value = export_keywords[0].value
    if not isinstance(value, ast.Constant) or not isinstance(value.value, bool):
        raise RegistryLintError(f"{build_path}:{value.lineno}: export_files must be a boolean literal")
    return value.value


def _external_schema_package_call(build_text: str, build_path: str, pkg: str, name: str) -> None:
    """AST gate on a MIGRATED package's BUILD (bijection rules 3/4).

    The in-repo half of the bijection is `_schema_package_export_files`: a
    package with registry entries must contain exactly one
    `aorta_define_schema_package(schema_items_for_package(_SCHEMA_PACKAGE))`.
    A migrated package fails that shape and used to be skipped entirely — its
    entries had no physical `.fbs`, so every path through the old lint either
    crashed or silently stopped checking. This is the shape it must have
    instead, and it is checked with the same fail-closed AST discipline:

      * exactly one `aorta_external_schema_package` call, at top level;
      * exactly one `_SCHEMA_PACKAGE` assignment, equal to `pkg`;
      * `name` equal to the allowlist's, `expected_provenance = "internal"`;
      * `items = schema_items_for_package(_SCHEMA_PACKAGE)`, so the macro
        materializes the registry's OWN slice of this package and cannot be
        handed a hand-written subset;
      * NO `aorta_define_schema_package` call — one canonical owner per slot,
        never two macros racing for the same output name.
    """
    try:
        module = ast.parse(build_text, filename=build_path)
    except SyntaxError as err:
        raise RegistryLintError(f"{build_path}:{err.lineno}: invalid BUILD syntax: {err.msg}") from err

    package_values = [
        statement.value.value
        for statement in module.body
        if isinstance(statement, ast.Assign)
        and len(statement.targets) == 1
        and isinstance(statement.targets[0], ast.Name)
        and statement.targets[0].id == "_SCHEMA_PACKAGE"
        and isinstance(statement.value, ast.Constant)
    ]
    if package_values != [pkg]:
        raise RegistryLintError(
            f"{build_path}: expected exactly one _SCHEMA_PACKAGE assignment to '{pkg}'"
        )

    def _calls(func_name):
        return [
            node
            for node in ast.walk(module)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == func_name
        ]

    if _calls("aorta_define_schema_package"):
        raise RegistryLintError(
            f"{build_path}: a migrated package must not call "
            "aorta_define_schema_package; aorta_external_schema_package owns "
            "every canonical output here"
        )
    calls = _calls("aorta_external_schema_package")
    if len(calls) != 1:
        raise RegistryLintError(
            f"{build_path}: expected exactly one aorta_external_schema_package call"
        )
    call = calls[0]
    if not any(isinstance(s, ast.Expr) and s.value is call for s in module.body):
        raise RegistryLintError(
            f"{build_path}:{call.lineno}: aorta_external_schema_package must be a top-level call"
        )
    if call.args:
        raise RegistryLintError(
            f"{build_path}:{call.lineno}: aorta_external_schema_package takes only named arguments"
        )
    kwargs = {}
    for keyword in call.keywords:
        if keyword.arg is None:
            raise RegistryLintError(f"{build_path}:{call.lineno}: **kwargs are forbidden")
        if keyword.arg in kwargs:
            raise RegistryLintError(
                f"{build_path}:{call.lineno}: duplicate argument '{keyword.arg}'"
            )
        kwargs[keyword.arg] = keyword.value

    for arg, expected in (("name", name), ("expected_provenance", "internal")):
        node = kwargs.get(arg)
        if not isinstance(node, ast.Constant) or node.value != expected:
            raise RegistryLintError(
                f"{build_path}:{call.lineno}: {arg} must be the string literal {expected!r}"
            )
    items = kwargs.get("items")
    if (
        not isinstance(items, ast.Call)
        or not isinstance(items.func, ast.Name)
        or items.func.id != "schema_items_for_package"
        or len(items.args) != 1
        or items.keywords
        or not isinstance(items.args[0], ast.Name)
        or items.args[0].id != "_SCHEMA_PACKAGE"
    ):
        raise RegistryLintError(
            f"{build_path}:{call.lineno}: items must be "
            "schema_items_for_package(_SCHEMA_PACKAGE)"
        )


def validate_bijection(root: Path, entries: list[RegistryEntry]) -> dict[str, int]:
    keys = [f"{entry.key[0]}:{entry.key[1]}" for entry in entries]
    duplicate_keys = _duplicates(keys)
    if duplicate_keys:
        raise RegistryLintError(f"duplicate registry keys: {duplicate_keys}")

    source_paths = [_source_path(entry) for entry in entries]
    duplicate_paths = _duplicates(source_paths)
    if duplicate_paths:
        raise RegistryLintError(f"duplicate canonical path: {duplicate_paths}")

    alias_count = validate_alias_uniqueness(entries)

    # Rules 3/4. Every canonical slot has EXACTLY ONE owner, and the
    # allowlist decides which KIND of owner is legal for it:
    #
    #   in-repo package  -> exactly one physical `.fbs` at the slot
    #   migrated package -> NO physical `.fbs`; the single owner is the external
    #                       root's copy output, whose byte-identity to the
    #                       source is proven by that root's own `<name>_lint_test`
    #
    # Splitting the two populations is what makes the check total. The old form
    # demanded a physical file for every entry, so a migrated entry could only
    # ever fail; simply exempting it would have left its slot unowned by
    # anything this lint can see.
    migrated = migrated_packages(root)
    in_repo_paths = [
        path
        for path, entry in zip(source_paths, entries)
        if entry.values["pkg"] not in migrated
    ]
    migrated_slots = {
        path: entry
        for path, entry in zip(source_paths, entries)
        if entry.values["pkg"] in migrated
    }

    missing_files = sorted(path for path in in_repo_paths if not (root / path).is_file())
    if missing_files:
        raise RegistryLintError(f"registry entries without one physical .fbs: {missing_files}")

    resurrected = sorted(path for path in migrated_slots if (root / path).is_file())
    if resurrected:
        raise RegistryLintError(
            "migrated registry entries with a physical .fbs in this repository: "
            f"{resurrected}; the canonical slot of a migrated family has exactly one "
            "owner — the external root's copy output — and a local file would be a "
            "second, silently-winning owner of the wire identity"
        )

    physical_paths = sorted(
        str(path.relative_to(root)) for path in (root / "message/schemas").rglob("*.fbs")
    )
    missing_entries = sorted(set(physical_paths) - set(in_repo_paths))
    extra_entries = sorted(set(in_repo_paths) - set(physical_paths))
    if missing_entries or extra_entries:
        raise RegistryLintError(
            "physical .fbs and registry entries differ: "
            f"missing entries={missing_entries}, missing files={extra_entries}"
        )

    entry_packages = {entry.values["pkg"] for entry in entries}
    orphan_roots = sorted(set(migrated) - entry_packages)
    if orphan_roots:
        raise RegistryLintError(
            f"{INTERNAL_EXTERNAL_ROOTS_REL} declares migrated package(s) {orphan_roots} "
            "with no registry entry: a migrated family keeps its entries in "
            "SCHEMA_DEFINITIONS, so an empty one means the family silently left the "
            "release tree"
        )

    bad_builds = []
    shadow_sources = []
    for pkg in sorted(entry_packages):
        package_dir = root / pkg.removeprefix("//")
        build_path = package_dir / "BUILD.bazel"
        if not build_path.is_file():
            bad_builds.append(f"{pkg}: missing BUILD.bazel")
            continue
        build_text = build_path.read_text(encoding="utf-8")
        build_rel = str(build_path.relative_to(root))
        if pkg in migrated:
            try:
                _external_schema_package_call(build_text, build_rel, pkg, migrated[pkg]["name"])
            except RegistryLintError as err:
                bad_builds.append(str(err))
                continue
            local_fbs = sorted(path.name for path in package_dir.glob("*.fbs"))
            if local_fbs:
                shadow_sources.append(f"{pkg}: {local_fbs}")
            continue
        try:
            export_files = _schema_package_export_files(build_text, build_rel, pkg)
        except RegistryLintError as err:
            bad_builds.append(str(err))
            continue
        if not export_files:
            local_fbs = sorted(path.name for path in package_dir.glob("*.fbs"))
            if local_fbs:
                shadow_sources.append(f"{pkg}: {local_fbs}")
    if bad_builds:
        raise RegistryLintError(f"entries without one package output: {bad_builds}")
    if shadow_sources:
        raise RegistryLintError(f"shadow packages contain tracked product .fbs: {shadow_sources}")

    internal_entries = [entry for entry in entries if entry.provenance == "internal"]
    return {
        "entries": len(entries),
        "internal_entries": len(internal_entries),
        "physical_files": len(physical_paths),
        "packages": len(entry_packages),
        "aliases": alias_count,
        "migrated_packages": len(migrated),
        "migrated_entries": len(migrated_slots),
    }
