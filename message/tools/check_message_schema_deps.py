#!/usr/bin/env python3
"""Validate FlatBuffers include dependency wiring for Aorta message schemas.

Checks:
1. For each schema target with rust/cpp enabled, included `.fbs` files must
   define matching rust/cpp generation targets.
2. Each Rust message binding must point to an existing schema target with
   rust=True.
3. Each C++ message binding `schema_cc_target` must export generated headers
   required by root schema + transitive includes (cc_library hdrs completeness).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

TOOL_DIR = Path(__file__).resolve().parent
if str(TOOL_DIR) not in sys.path:
    sys.path.insert(0, str(TOOL_DIR))

from schema_include_utils import parse_schema_includes  # noqa: E402


MESSAGE_REL = Path("message")
SCHEMAS_REL = MESSAGE_REL / "schemas"
SCHEMA_MANIFEST_REL = MESSAGE_REL / "schemas_registry_manifest.json"
SCHEMA_MANIFEST_VERSION = 3
STRING_ATTR_RE = r'\b{attr}\s*=\s*"([^"]+)"'
DICT_STRING_ATTR_RE = r'"{attr}"\s*:\s*"([^"]+)"'
BOOL_TRUE_ATTR_RE = r"\b{attr}\s*=\s*True\b"
BOOL_ATTR_RE = r"\b{attr}\s*=\s*(True|False)\b"
DICT_BOOL_ATTR_RE = r'"{attr}"\s*:\s*(True|False)\b'
NONE_ATTR_RE = r"\b{attr}\s*=\s*None\b"
DICT_NONE_ATTR_RE = r'"{attr}"\s*:\s*None\b'
LIST_ATTR_RE = r"\b{attr}\s*=\s*\[(.*?)\]"
DICT_LIST_ATTR_RE = r'"{attr}"\s*:\s*\[(.*?)\]'
STRING_LITERAL_RE = re.compile(r'"([^"]+)"')
IDENTIFIER_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b")
STRING_ASSIGNMENT_RE = re.compile(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"([^"]+)"\s*$', re.MULTILINE)


@dataclass
class SchemaTargetFlags:
    rust: bool
    cpp: bool
    schema_meta_cpp: bool


@dataclass
class SchemaItem:
    stem: str
    root_fbs: str
    dep_fbs: List[str]
    rust: bool
    cpp: bool
    schema_meta_cpp: bool
    cpp_lib: bool
    cpp_lib_name: str
    cc_alias: str | None
    extra_cc_deps: List[str]


@dataclass
class RegistrySchemaItem:
    pkg: str
    stem: str
    root_fbs: str
    dep_fbs: List[str]
    rust: bool
    cpp: bool
    python: bool
    bfbs: bool
    schema_meta_rust: bool
    schema_meta_cpp: bool
    cpp_lib: bool
    cpp_lib_name: str
    cc_alias: str | None
    alias_id: str
    rust_wrapper: bool
    cpp_wrapper: bool
    rust_wrapper_name: str | None
    cpp_wrapper_name: str | None
    cpp_wrapper_schema_cc_target: str | None
    extra_cc_deps: List[str]


@dataclass
class RustBinding:
    name: str
    schema_pkg: str
    schema_stem: str


@dataclass
class CppBinding:
    name: str
    schema_pkg: str
    schema_stem: str
    schema_cc_target: str


_REGISTRY_SCHEMA_ITEMS_CACHE: Dict[Path, List[RegistrySchemaItem]] = {}
_SCHEMA_MANIFEST_CACHE: Dict[Path, Dict[str, object]] = {}
_PACKAGE_ALIASES_BY_PKG_CACHE: Dict[Path, Dict[str, List[tuple[str, str]]]] = {}


def _find_repo_root(explicit: str | None) -> Path:
    if explicit:
        root = Path(explicit).resolve()
        if not (root / MESSAGE_REL).exists():
            raise ValueError(f"repo root '{root}' does not contain {MESSAGE_REL}")
        return root

    here = Path(__file__).resolve()
    for candidate in [here.parent] + list(here.parents):
        if (candidate / MESSAGE_REL).exists():
            return candidate
    raise ValueError("failed to locate repository root")


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _extract_call_blocks(text: str, fn_name: str) -> List[str]:
    blocks: List[str] = []
    token = f"{fn_name}("
    pos = 0
    while True:
        start = text.find(token, pos)
        if start < 0:
            break
        i = start + len(fn_name)
        depth = 0
        in_str = False
        escaped = False
        end = -1
        while i < len(text):
            ch = text[i]
            if in_str:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_str = False
                i += 1
                continue
            if ch == '"':
                in_str = True
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
            i += 1
        if end < 0:
            break
        blocks.append(text[start:end])
        pos = end
    return blocks


def _extract_dict_literal_blocks(text: str) -> List[str]:
    blocks: List[str] = []
    start = -1
    depth = 0
    in_str = False
    escaped = False

    for idx, ch in enumerate(text):
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_str = False
            continue

        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            if depth == 0:
                start = idx
            depth += 1
            continue
        if ch == "}" and depth > 0:
            depth -= 1
            if depth == 0 and start >= 0:
                blocks.append(text[start : idx + 1])
                start = -1
    return blocks


def _read_variable_assignments(text: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for name, value in STRING_ASSIGNMENT_RE.findall(text):
        result[name] = value
    return result


def _search_attr_string(block: str, attr: str) -> str | None:
    struct_match = re.search(STRING_ATTR_RE.format(attr=attr), block)
    if struct_match:
        return struct_match.group(1)
    dict_match = re.search(DICT_STRING_ATTR_RE.format(attr=attr), block)
    if dict_match:
        return dict_match.group(1)
    return None


def _search_attr_bool(block: str, attr: str) -> bool | None:
    struct_match = re.search(BOOL_ATTR_RE.format(attr=attr), block)
    if struct_match:
        return struct_match.group(1) == "True"
    dict_match = re.search(DICT_BOOL_ATTR_RE.format(attr=attr), block)
    if dict_match:
        return dict_match.group(1) == "True"
    return None


def _has_attr_none(block: str, attr: str) -> bool:
    return bool(
        re.search(NONE_ATTR_RE.format(attr=attr), block)
        or re.search(DICT_NONE_ATTR_RE.format(attr=attr), block)
    )


def _search_attr_list_text(block: str, attr: str) -> str | None:
    struct_match = re.search(LIST_ATTR_RE.format(attr=attr), block, re.DOTALL)
    if struct_match:
        return struct_match.group(1)
    dict_match = re.search(DICT_LIST_ATTR_RE.format(attr=attr), block, re.DOTALL)
    if dict_match:
        return dict_match.group(1)
    return None


def _parse_label_list_attr(
    block: str,
    attr: str,
    variables: Dict[str, str],
) -> List[str]:
    text = _search_attr_list_text(block, attr)
    if not text:
        return []

    values: List[str] = []
    for literal in STRING_LITERAL_RE.findall(text):
        values.append(literal)
    for ident in IDENTIFIER_RE.findall(text):
        if ident in variables:
            values.append(variables[ident])
    deduped: List[str] = []
    seen: set[str] = set()
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _parse_schema_items(text: str) -> List[SchemaItem]:
    variables = _read_variable_assignments(text)
    blocks = _extract_call_blocks(text, "struct") + _extract_dict_literal_blocks(text)

    items: List[SchemaItem] = []
    for block in blocks:
        stem = _search_attr_string(block, "stem")
        root_fbs = _search_attr_string(block, "root_fbs")
        if not (stem and root_fbs):
            continue

        rust = _search_attr_bool(block, "rust") or False
        cpp = _search_attr_bool(block, "cpp") or False
        schema_meta_cpp = _search_attr_bool(block, "schema_meta_cpp") or False
        dep_fbs = _parse_label_list_attr(block, "dep_fbs", variables)
        extra_cc_deps = _parse_label_list_attr(block, "extra_cc_deps", variables)

        cpp_lib_bool = _search_attr_bool(block, "cpp_lib")
        cpp_lib = cpp if cpp_lib_bool is None else cpp_lib_bool
        if cpp_lib and not cpp:
            raise ValueError(f"schema '{stem}' has cpp_lib=True but cpp=False")

        cpp_lib_name = _search_attr_string(block, "cpp_lib_name")
        if cpp_lib_name is None:
            cpp_lib_name = f"{stem}_cpp_lib"

        cc_alias: str | None
        cc_alias_string = _search_attr_string(block, "cc_alias")
        if cc_alias_string is not None:
            cc_alias = cc_alias_string
        elif _has_attr_none(block, "cc_alias"):
            cc_alias = None
        else:
            cc_alias = f"{stem}_cc"

        items.append(
            SchemaItem(
                stem=stem,
                root_fbs=root_fbs,
                dep_fbs=dep_fbs,
                rust=rust,
                cpp=cpp,
                schema_meta_cpp=schema_meta_cpp,
                cpp_lib=cpp_lib,
                cpp_lib_name=cpp_lib_name,
                cc_alias=cc_alias,
                extra_cc_deps=extra_cc_deps,
            )
        )

    return items


def _package_label_for_build_path(repo_root: Path, build_path: Path) -> str | None:
    try:
        rel = build_path.parent.relative_to(repo_root)
    except ValueError:
        return None
    return f"//{rel.as_posix()}"


def _to_schema_item(item: RegistrySchemaItem) -> SchemaItem:
    return SchemaItem(
        stem=item.stem,
        root_fbs=item.root_fbs,
        dep_fbs=item.dep_fbs,
        rust=item.rust,
        cpp=item.cpp,
        schema_meta_cpp=item.schema_meta_cpp,
        cpp_lib=item.cpp_lib,
        cpp_lib_name=item.cpp_lib_name,
        cc_alias=item.cc_alias,
        extra_cc_deps=item.extra_cc_deps,
    )


def _read_manifest(repo_root: Path) -> Dict[str, object]:
    if repo_root in _SCHEMA_MANIFEST_CACHE:
        return _SCHEMA_MANIFEST_CACHE[repo_root]

    manifest_path = repo_root / SCHEMA_MANIFEST_REL
    if not manifest_path.exists():
        raise FileNotFoundError(
            "required schema registry manifest file is missing: {}. "
            "Ensure Bazel runfiles include //message:schemas_registry_manifest.".format(
                manifest_path
            )
        )

    try:
        data = json.loads(_read_text(manifest_path))
    except json.JSONDecodeError as err:
        raise ValueError(f"failed to parse schema manifest json '{manifest_path}': {err}") from err

    if not isinstance(data, dict):
        raise ValueError(f"schema manifest '{manifest_path}' must contain a JSON object root")

    version = data.get("manifest_version")
    if version != SCHEMA_MANIFEST_VERSION:
        raise ValueError(
            "unsupported schema manifest version '{}', expected '{}' (file: {}).".format(
                version,
                SCHEMA_MANIFEST_VERSION,
                manifest_path,
            )
        )

    _SCHEMA_MANIFEST_CACHE[repo_root] = data
    return data


def _manifest_list(manifest: Dict[str, object], key: str) -> List[object]:
    value = manifest.get(key)
    if not isinstance(value, list):
        raise ValueError(f"schema manifest missing list field '{key}'")
    return value


def _manifest_required_str(raw: Dict[str, object], key: str, ctx: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{ctx}: field '{key}' must be string")
    return value


def _manifest_optional_str(raw: Dict[str, object], key: str, ctx: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{ctx}: field '{key}' must be string or null")
    return value


def _manifest_bool(raw: Dict[str, object], key: str, default: bool, ctx: str) -> bool:
    value = raw.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"{ctx}: field '{key}' must be bool")
    return value


def _manifest_str_list(raw: Dict[str, object], key: str, ctx: str) -> List[str]:
    value = raw.get(key, [])
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{ctx}: field '{key}' must be list[str]")
    result: List[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError(f"{ctx}: field '{key}' must be list[str]")
        result.append(item)
    return result


def _parse_registry_schema_items(repo_root: Path) -> List[RegistrySchemaItem]:
    if repo_root in _REGISTRY_SCHEMA_ITEMS_CACHE:
        return _REGISTRY_SCHEMA_ITEMS_CACHE[repo_root]

    manifest = _read_manifest(repo_root)
    items: List[RegistrySchemaItem] = []
    for raw_item in _manifest_list(manifest, "schema_definitions"):
        if not isinstance(raw_item, dict):
            raise ValueError("schema manifest field 'schema_definitions' must contain objects")

        ctx = "schema manifest item"
        pkg = _manifest_required_str(raw_item, "pkg", ctx)
        stem = _manifest_required_str(raw_item, "stem", ctx)
        root_fbs = _manifest_required_str(raw_item, "root_fbs", ctx)
        dep_fbs = _manifest_str_list(raw_item, "dep_fbs", ctx)
        rust = _manifest_bool(raw_item, "rust", False, ctx)
        cpp = _manifest_bool(raw_item, "cpp", False, ctx)
        python = _manifest_bool(raw_item, "python", False, ctx)
        bfbs = _manifest_bool(raw_item, "bfbs", False, ctx)
        schema_meta_rust = _manifest_bool(raw_item, "schema_meta_rust", False, ctx)
        schema_meta_cpp = _manifest_bool(raw_item, "schema_meta_cpp", False, ctx)
        cpp_lib = _manifest_bool(raw_item, "cpp_lib", cpp, ctx)
        cpp_lib_name = _manifest_required_str(raw_item, "cpp_lib_name", ctx)
        cc_alias = _manifest_optional_str(raw_item, "cc_alias", ctx)
        alias_id = _manifest_required_str(raw_item, "alias_id", ctx)
        rust_wrapper = _manifest_bool(raw_item, "rust_wrapper", False, ctx)
        cpp_wrapper = _manifest_bool(raw_item, "cpp_wrapper", False, ctx)
        rust_wrapper_name = _manifest_optional_str(raw_item, "rust_wrapper_name", ctx)
        cpp_wrapper_name = _manifest_optional_str(raw_item, "cpp_wrapper_name", ctx)
        cpp_wrapper_schema_cc_target = _manifest_optional_str(
            raw_item,
            "cpp_wrapper_schema_cc_target",
            ctx,
        )
        extra_cc_deps = _manifest_str_list(raw_item, "extra_cc_deps", ctx)

        if cpp_lib and not cpp:
            raise ValueError(f"schema '{stem}' has cpp_lib=True but cpp=False")

        items.append(
            RegistrySchemaItem(
                pkg=pkg,
                stem=stem,
                root_fbs=root_fbs,
                dep_fbs=dep_fbs,
                rust=rust,
                cpp=cpp,
                python=python,
                bfbs=bfbs,
                schema_meta_rust=schema_meta_rust,
                schema_meta_cpp=schema_meta_cpp,
                cpp_lib=cpp_lib,
                cpp_lib_name=cpp_lib_name,
                cc_alias=cc_alias,
                alias_id=alias_id,
                rust_wrapper=rust_wrapper,
                cpp_wrapper=cpp_wrapper,
                rust_wrapper_name=rust_wrapper_name,
                cpp_wrapper_name=cpp_wrapper_name,
                cpp_wrapper_schema_cc_target=cpp_wrapper_schema_cc_target,
                extra_cc_deps=extra_cc_deps,
            )
        )

    _REGISTRY_SCHEMA_ITEMS_CACHE[repo_root] = items
    return items


def _registry_schema_items_by_pkg(repo_root: Path) -> Dict[str, List[RegistrySchemaItem]]:
    result: Dict[str, List[RegistrySchemaItem]] = {}
    for item in _parse_registry_schema_items(repo_root):
        result.setdefault(item.pkg, []).append(item)
    return result


def _schema_items_for_build(
    build_path: Path,
    text: str,
    repo_root: Path | None,
) -> List[SchemaItem]:
    by_stem: Dict[str, SchemaItem] = {item.stem: item for item in _parse_schema_items(text)}
    if repo_root is None:
        return list(by_stem.values())

    pkg_label = _package_label_for_build_path(repo_root, build_path)
    if pkg_label is None:
        return list(by_stem.values())

    for item in _registry_schema_items_by_pkg(repo_root).get(pkg_label, []):
        if item.stem in by_stem:
            continue
        by_stem[item.stem] = _to_schema_item(item)
    return list(by_stem.values())


def _insert_unique_alias(result: Dict[str, str], name: str, actual: str, source: str) -> None:
    previous = result.get(name)
    if previous is None:
        result[name] = actual
        return
    raise ValueError(
        f"duplicate alias '{name}' detected in {source}: '{previous}' vs '{actual}'"
    )


def _manifest_alias_pairs(repo_root: Path, field: str) -> List[tuple[str, str]]:
    manifest = _read_manifest(repo_root)
    pairs: List[tuple[str, str]] = []
    for raw_item in _manifest_list(manifest, field):
        if not isinstance(raw_item, dict):
            raise ValueError(f"schema manifest field '{field}' must contain objects")
        ctx = f"schema manifest alias '{field}'"
        name = _manifest_required_str(raw_item, "name", ctx)
        actual = _manifest_required_str(raw_item, "actual", ctx)
        pairs.append((name, actual))
    return pairs


def _manifest_package_aliases_by_pkg(repo_root: Path) -> Dict[str, List[tuple[str, str]]]:
    if repo_root in _PACKAGE_ALIASES_BY_PKG_CACHE:
        return _PACKAGE_ALIASES_BY_PKG_CACHE[repo_root]

    manifest = _read_manifest(repo_root)
    by_pkg: Dict[str, List[tuple[str, str]]] = {}
    for raw_item in _manifest_list(manifest, "package_aliases"):
        if not isinstance(raw_item, dict):
            raise ValueError("schema manifest field 'package_aliases' must contain objects")
        ctx = "schema manifest package_aliases item"
        pkg = _manifest_required_str(raw_item, "pkg", ctx)
        name = _manifest_required_str(raw_item, "name", ctx)
        actual = _manifest_required_str(raw_item, "actual", ctx)
        by_pkg.setdefault(pkg, []).append((name, actual))

    _PACKAGE_ALIASES_BY_PKG_CACHE[repo_root] = by_pkg
    return by_pkg


def _root_schema_alias_actuals(repo_root: Path) -> Dict[str, str]:
    result: Dict[str, str] = {}
    source = str(SCHEMA_MANIFEST_REL)
    for name, actual in _manifest_alias_pairs(repo_root, "schema_output_aliases"):
        _insert_unique_alias(result, name, actual, source)
    for name, actual in _manifest_alias_pairs(repo_root, "message_binding_aliases"):
        _insert_unique_alias(result, name, actual, source)
    return result


def _dep_cpp_lib_label(dep_fbs_label: str) -> str:
    if not dep_fbs_label.endswith(".fbs"):
        return dep_fbs_label

    stem = dep_fbs_label[:-4]
    if dep_fbs_label.startswith("//"):
        return f"{stem}_cpp_lib"
    if dep_fbs_label.startswith(":"):
        return f":{stem[1:]}_cpp_lib"
    return f":{stem}_cpp_lib"


def _dedupe_keep_order(items: List[str]) -> List[str]:
    return list(dict.fromkeys(items))


def _parse_schema_targets(
    build_path: Path,
    repo_root: Path | None = None,
) -> Dict[str, SchemaTargetFlags]:
    if not build_path.exists():
        return {}
    text = _read_text(build_path)
    targets: Dict[str, SchemaTargetFlags] = {}
    for block in _extract_call_blocks(text, "aorta_define_schema_targets"):
        stem_match = re.search(STRING_ATTR_RE.format(attr="stem"), block)
        if not stem_match:
            continue
        stem = stem_match.group(1)
        rust = bool(re.search(BOOL_TRUE_ATTR_RE.format(attr="rust"), block))
        cpp = bool(re.search(BOOL_TRUE_ATTR_RE.format(attr="cpp"), block))
        schema_meta_cpp = bool(
            re.search(BOOL_TRUE_ATTR_RE.format(attr="schema_meta_cpp"), block)
        )
        targets[stem] = SchemaTargetFlags(
            rust=rust,
            cpp=cpp,
            schema_meta_cpp=schema_meta_cpp,
        )

    for item in _schema_items_for_build(build_path, text, repo_root):
        targets[item.stem] = SchemaTargetFlags(
            rust=item.rust,
            cpp=item.cpp,
            schema_meta_cpp=item.schema_meta_cpp,
        )
    return targets


def _resolve_include_path(message_root: Path, root_fbs: Path, include_path: str) -> Path:
    if include_path.startswith("schemas/"):
        return message_root / include_path
    return root_fbs.parent / include_path


def _parse_rust_bindings(repo_root: Path) -> List[RustBinding]:
    bindings: List[RustBinding] = []
    manifest = _read_manifest(repo_root)
    for raw_item in _manifest_list(manifest, "rust_message_bindings"):
        if not isinstance(raw_item, dict):
            raise ValueError("schema manifest field 'rust_message_bindings' must contain objects")
        ctx = "schema manifest rust_message_bindings item"
        bindings.append(
            RustBinding(
                name=_manifest_required_str(raw_item, "name", ctx),
                schema_pkg=_manifest_required_str(raw_item, "schema_pkg", ctx),
                schema_stem=_manifest_required_str(raw_item, "schema_stem", ctx),
            )
        )
    return bindings


def _parse_cpp_bindings(repo_root: Path) -> List[CppBinding]:
    bindings: List[CppBinding] = []
    manifest = _read_manifest(repo_root)
    for raw_item in _manifest_list(manifest, "cpp_message_bindings"):
        if not isinstance(raw_item, dict):
            raise ValueError("schema manifest field 'cpp_message_bindings' must contain objects")
        ctx = "schema manifest cpp_message_bindings item"
        bindings.append(
            CppBinding(
                name=_manifest_required_str(raw_item, "name", ctx),
                schema_pkg=_manifest_required_str(raw_item, "schema_pkg", ctx),
                schema_stem=_manifest_required_str(raw_item, "schema_stem", ctx),
                schema_cc_target=_manifest_required_str(raw_item, "schema_cc_target", ctx),
            )
        )
    return bindings


def _normalize_label(pkg_label: str, label: str) -> str:
    if label.startswith("//"):
        return label
    if label.startswith(":"):
        return f"{pkg_label}{label}"
    return f"{pkg_label}:{label}"


def _split_target_label(label: str) -> tuple[str, str]:
    pos = label.rfind(":")
    if pos < 0 or not label.startswith("//"):
        raise ValueError(f"invalid label '{label}'")
    return label[:pos], label[pos + 1 :]


def _parse_alias_actuals(
    build_path: Path,
    repo_root: Path | None = None,
) -> Dict[str, str]:
    if not build_path.exists():
        return {}
    text = _read_text(build_path)
    result: Dict[str, str] = {}
    for block in _extract_call_blocks(text, "alias"):
        name_match = re.search(STRING_ATTR_RE.format(attr="name"), block)
        actual_match = re.search(STRING_ATTR_RE.format(attr="actual"), block)
        if not (name_match and actual_match):
            continue
        _insert_unique_alias(
            result,
            name_match.group(1),
            actual_match.group(1),
            str(build_path),
        )

    if repo_root is not None:
        pkg_label = _package_label_for_build_path(repo_root, build_path)
        if pkg_label is not None:
            for name, actual in _manifest_package_aliases_by_pkg(repo_root).get(pkg_label, []):
                _insert_unique_alias(
                    result,
                    name,
                    actual,
                    str(SCHEMA_MANIFEST_REL),
                )

        message_build = repo_root / MESSAGE_REL / "BUILD.bazel"
        if build_path.resolve() == message_build.resolve():
            for name, actual in _root_schema_alias_actuals(repo_root).items():
                _insert_unique_alias(
                    result,
                    name,
                    actual,
                    str(SCHEMA_MANIFEST_REL),
                )
    return result


def _parse_cc_libraries(
    build_path: Path,
    repo_root: Path | None = None,
) -> Dict[str, Dict[str, List[str]]]:
    if not build_path.exists():
        return {}
    text = _read_text(build_path)
    result: Dict[str, Dict[str, List[str]]] = {}
    for block in _extract_call_blocks(text, "cc_library"):
        name_match = re.search(STRING_ATTR_RE.format(attr="name"), block)
        if not name_match:
            continue
        target_name = name_match.group(1)
        hdrs: List[str] = []
        hdrs_match = re.search(LIST_ATTR_RE.format(attr="hdrs"), block, re.DOTALL)
        if hdrs_match:
            for item in STRING_LITERAL_RE.finditer(hdrs_match.group(1)):
                hdrs.append(item.group(1))
        deps: List[str] = []
        deps_match = re.search(LIST_ATTR_RE.format(attr="deps"), block, re.DOTALL)
        if deps_match:
            for item in STRING_LITERAL_RE.finditer(deps_match.group(1)):
                deps.append(item.group(1))
        result[target_name] = {
            "hdrs": hdrs,
            "deps": deps,
        }

    schema_items = _schema_items_for_build(build_path, text, repo_root)
    local_cpp_lib_by_fbs: Dict[str, str] = {}
    local_cpp_no_lib_by_fbs: Dict[str, str] = {}

    for item in schema_items:
        if not item.cpp:
            continue

        fbs_keys = [item.root_fbs]
        if not item.root_fbs.startswith("//"):
            if item.root_fbs.startswith(":"):
                fbs_keys.append(item.root_fbs[1:])
            else:
                fbs_keys.append(f":{item.root_fbs}")

        for key in fbs_keys:
            if item.cpp_lib:
                local_cpp_lib_by_fbs[key] = item.cpp_lib_name
            else:
                local_cpp_no_lib_by_fbs[key] = item.stem

    for item in schema_items:
        if not item.cpp_lib:
            continue
        hdrs = [f":{item.stem}_cpp"]
        if item.schema_meta_cpp:
            hdrs.append(f":{item.stem}_schema_meta_cpp")

        deps: List[str] = []
        for dep in item.dep_fbs:
            if dep in local_cpp_lib_by_fbs:
                deps.append(f":{local_cpp_lib_by_fbs[dep]}")
            elif dep in local_cpp_no_lib_by_fbs:
                raise ValueError(
                    f"stem '{item.stem}' depends on '{dep}' whose schema item "
                    f"'{local_cpp_no_lib_by_fbs[dep]}' sets cpp_lib=False. "
                    "Set cpp_lib=True on that dependency or provide explicit extra_cc_deps."
                )
            else:
                deps.append(_dep_cpp_lib_label(dep))
        deps.extend(item.extra_cc_deps)
        deps.append("@flatbuffers//:flatbuffers")

        result[item.cpp_lib_name] = {
            "hdrs": hdrs,
            "deps": _dedupe_keep_order(deps),
        }
    return result

def _collect_schema_include_errors(repo_root: Path) -> List[str]:
    errors: List[str] = []
    message_root = repo_root / MESSAGE_REL
    schema_root = repo_root / SCHEMAS_REL

    build_cache: Dict[Path, Dict[str, SchemaTargetFlags]] = {}

    def get_targets(pkg_dir: Path) -> Dict[str, SchemaTargetFlags]:
        if pkg_dir not in build_cache:
            build_cache[pkg_dir] = _parse_schema_targets(
                pkg_dir / "BUILD.bazel",
                repo_root = repo_root,
            )
        return build_cache[pkg_dir]

    for fbs_path in sorted(schema_root.rglob("*.fbs")):
        root_targets = get_targets(fbs_path.parent)
        root_flags = root_targets.get(fbs_path.stem)
        if root_flags is None:
            continue

        includes = parse_schema_includes(fbs_path)
        if not includes:
            continue

        for include in includes:
            include_fbs = _resolve_include_path(message_root, fbs_path, include)
            if not include_fbs.exists():
                errors.append(
                    f"{fbs_path}: include '{include}' resolves to missing file '{include_fbs}'."
                )
                continue

            include_targets = get_targets(include_fbs.parent)
            include_flags = include_targets.get(include_fbs.stem)
            if include_flags is None:
                errors.append(
                    f"{fbs_path}: include '{include}' requires target stem '{include_fbs.stem}' "
                    f"in {include_fbs.parent / 'BUILD.bazel'}."
                )
                continue

            if root_flags.rust and not include_flags.rust:
                errors.append(
                    f"{fbs_path}: rust=True but include '{include}' does not provide rust=True "
                    f"(target stem '{include_fbs.stem}')."
                )
            if root_flags.cpp and not include_flags.cpp:
                errors.append(
                    f"{fbs_path}: cpp=True but include '{include}' does not provide cpp=True "
                    f"(target stem '{include_fbs.stem}')."
                )

    return errors


def _collect_rust_binding_include_errors(repo_root: Path) -> List[str]:
    errors: List[str] = []
    bindings = _parse_rust_bindings(repo_root)
    build_cache: Dict[Path, Dict[str, SchemaTargetFlags]] = {}

    def get_targets(pkg_dir: Path) -> Dict[str, SchemaTargetFlags]:
        if pkg_dir not in build_cache:
            build_cache[pkg_dir] = _parse_schema_targets(
                pkg_dir / "BUILD.bazel",
                repo_root = repo_root,
            )
        return build_cache[pkg_dir]

    for item in bindings:
        schema_pkg_rel = item.schema_pkg.removeprefix("//")
        root_fbs = repo_root / schema_pkg_rel / f"{item.schema_stem}.fbs"
        if not root_fbs.exists():
            errors.append(
                f"binding '{item.name}': root schema file missing: {root_fbs}"
            )
            continue

        root_flags = get_targets(root_fbs.parent).get(item.schema_stem)
        if root_flags is None:
            errors.append(
                f"binding '{item.name}': missing schema target stem '{item.schema_stem}' "
                f"in {root_fbs.parent / 'BUILD.bazel'}."
            )
            continue
        if not root_flags.rust:
            errors.append(
                f"binding '{item.name}': schema target '{item.schema_stem}' must set rust=True."
            )

    return errors


def _collect_include_fbs_recursive(
    message_root: Path,
    root_fbs: Path,
) -> List[Path]:
    includes: List[Path] = []
    visited: set[Path] = set()
    stack: List[Path] = [root_fbs]

    while stack:
        current = stack.pop()
        for include in parse_schema_includes(current):
            include_fbs = _resolve_include_path(message_root, current, include)
            if include_fbs in visited:
                continue
            visited.add(include_fbs)
            includes.append(include_fbs)
            if include_fbs.exists():
                stack.append(include_fbs)

    return includes


def _collect_cpp_binding_export_errors(repo_root: Path) -> List[str]:
    errors: List[str] = []
    message_root = repo_root / MESSAGE_REL
    bindings = _parse_cpp_bindings(repo_root)

    schema_target_cache: Dict[Path, Dict[str, SchemaTargetFlags]] = {}
    cc_library_cache: Dict[Path, Dict[str, Dict[str, List[str]]]] = {}
    alias_cache: Dict[Path, Dict[str, str]] = {}

    def schema_targets(pkg_dir: Path) -> Dict[str, SchemaTargetFlags]:
        if pkg_dir not in schema_target_cache:
            schema_target_cache[pkg_dir] = _parse_schema_targets(
                pkg_dir / "BUILD.bazel",
                repo_root = repo_root,
            )
        return schema_target_cache[pkg_dir]

    def cc_libraries(pkg_dir: Path) -> Dict[str, Dict[str, List[str]]]:
        if pkg_dir not in cc_library_cache:
            cc_library_cache[pkg_dir] = _parse_cc_libraries(
                pkg_dir / "BUILD.bazel",
                repo_root = repo_root,
            )
        return cc_library_cache[pkg_dir]

    def aliases(pkg_dir: Path) -> Dict[str, str]:
        if pkg_dir not in alias_cache:
            alias_cache[pkg_dir] = _parse_alias_actuals(
                pkg_dir / "BUILD.bazel",
                repo_root = repo_root,
            )
        return alias_cache[pkg_dir]

    def resolve_alias(label: str) -> str:
        current = label
        seen: set[str] = set()
        while True:
            if current in seen:
                raise ValueError(f"alias cycle detected at '{current}'")
            seen.add(current)
            pkg_label, target_name = _split_target_label(current)
            pkg_dir = repo_root / pkg_label.removeprefix("//")
            actual = aliases(pkg_dir).get(target_name)
            if actual is None:
                return current
            current = _normalize_label(pkg_label, actual)

    def collect_exported_headers(cc_label: str, visited: set[str]) -> set[str]:
        if cc_label in visited:
            return set()
        visited.add(cc_label)

        try:
            resolved = resolve_alias(cc_label)
        except ValueError:
            return set()

        cc_pkg_label, cc_target_name = _split_target_label(resolved)
        cc_pkg_dir = repo_root / cc_pkg_label.removeprefix("//")
        cc_target = cc_libraries(cc_pkg_dir).get(cc_target_name)
        if cc_target is None:
            return set()

        exported: set[str] = {
            _normalize_label(cc_pkg_label, header_label)
            for header_label in cc_target["hdrs"]
        }
        for dep in cc_target["deps"]:
            if dep.startswith("@"):
                continue
            dep_label = _normalize_label(cc_pkg_label, dep)
            exported.update(collect_exported_headers(dep_label, visited))
        return exported

    for item in bindings:
        schema_pkg_rel = item.schema_pkg.removeprefix("//")
        schema_pkg_dir = repo_root / schema_pkg_rel
        root_fbs = schema_pkg_dir / f"{item.schema_stem}.fbs"
        if not root_fbs.exists():
            errors.append(
                f"cpp binding '{item.name}': root schema file missing: {root_fbs}"
            )
            continue

        root_flags = schema_targets(schema_pkg_dir).get(item.schema_stem)
        if root_flags is None:
            errors.append(
                f"cpp binding '{item.name}': missing schema target stem '{item.schema_stem}' "
                f"in {schema_pkg_dir / 'BUILD.bazel'}."
            )
            continue
        if not root_flags.cpp:
            errors.append(
                f"cpp binding '{item.name}': schema target '{item.schema_stem}' must set cpp=True."
            )
            continue
        if not root_flags.schema_meta_cpp:
            errors.append(
                f"cpp binding '{item.name}': schema target '{item.schema_stem}' must set "
                "schema_meta_cpp=True (wrapper includes <stem>_schema_meta.h)."
            )
            continue

        try:
            resolved_schema_cc_target = resolve_alias(item.schema_cc_target)
            cc_pkg_label, cc_target_name = _split_target_label(resolved_schema_cc_target)
        except ValueError as err:
            errors.append(f"cpp binding '{item.name}': {err}")
            continue
        cc_pkg_dir = repo_root / cc_pkg_label.removeprefix("//")
        cc_targets = cc_libraries(cc_pkg_dir)
        cc_target = cc_targets.get(cc_target_name)
        if cc_target is None:
            errors.append(
                f"cpp binding '{item.name}': schema_cc_target '{item.schema_cc_target}' "
                f"is not a cc_library in {cc_pkg_dir / 'BUILD.bazel'}."
            )
            continue

        exported = collect_exported_headers(item.schema_cc_target, visited = set())

        expected: set[str] = {
            _normalize_label(item.schema_pkg, f":{item.schema_stem}_cpp"),
            _normalize_label(item.schema_pkg, f":{item.schema_stem}_schema_meta_cpp"),
        }

        for include_fbs in _collect_include_fbs_recursive(message_root, root_fbs):
            include_pkg = f"//{include_fbs.parent.relative_to(repo_root).as_posix()}"
            include_flags = schema_targets(include_fbs.parent).get(include_fbs.stem)
            if include_flags is None or not include_flags.cpp:
                continue
            expected.add(_normalize_label(include_pkg, f":{include_fbs.stem}_cpp"))

        missing = sorted(expected - exported)
        if missing:
            errors.append(
                f"cpp binding '{item.name}': schema_cc_target '{item.schema_cc_target}' "
                f"missing exported hdr labels: {missing}"
            )

    return errors


# ---------------------------------------------------------------------------
# Hermeticity invariants:
#
#   (i)  declared direct `dep_fbs` == the textual direct `include` statements
#        of the item's own root `.fbs` file.
#   (ii) the registry-expanded transitive `dep_fbs` closure == the textual
#        transitive include closure reachable from the root `.fbs` file.
#
# Both invariants are evaluated over the SAME manifest data
# (`RegistrySchemaItem.dep_fbs`, always DIRECT edges per the registry
# authoring convention) as the single source of truth; the transitive
# closure below independently mirrors registry.bzl's `_collect_transitive_dep_fbs`
# BFS in Python so the checker does not depend on Starlark evaluation. Text
# scanning goes through the shared `schema_include_utils.parse_schema_includes`
# scanner (comment-stripped) rather than a private regex.
# ---------------------------------------------------------------------------


def _registry_item_label(item: RegistrySchemaItem) -> str:
    return _normalize_label(item.pkg, item.root_fbs)


def _registry_items_by_label(
    items: List[RegistrySchemaItem],
) -> Dict[str, RegistrySchemaItem]:
    return {_registry_item_label(item): item for item in items}


def _label_to_fbs_path(repo_root: Path, owner_pkg: str, label: str) -> Path:
    """Resolve a registry `dep_fbs` (or root_fbs) label to its file path.

    `label` may already be a fully-qualified `//pkg:file.fbs` label (as
    produced by the transitive-closure BFS below) or a same-package form
    (`:file.fbs` / bare `file.fbs`) relative to `owner_pkg`, mirroring
    registry.bzl's `_normalize_dep_fbs_label` / `_schema_fbs_label`.
    """
    normalized = _normalize_label(owner_pkg, label)
    pkg_label, file_name = _split_target_label(normalized)
    return repo_root / pkg_label.removeprefix("//") / file_name


def _collect_transitive_dep_fbs_labels(
    item: RegistrySchemaItem,
    item_by_label: Dict[str, RegistrySchemaItem],
) -> List[str]:
    """Transitive `.fbs` label closure of `item.dep_fbs`.

    Mirrors `registry.bzl`'s `_collect_transitive_dep_fbs` BFS (same direct
    `dep_fbs` edges, same "registry-expanded dep_fbs" semantics) but is an
    independent Python implementation over the manifest — the checker cannot
    execute Starlark, so DRY is preserved at the DATA level (one direct-edge
    source: `dep_fbs`), not by sharing code across languages.
    """
    labels: List[str] = []
    seen: set[str] = set()
    frontier: List[str] = []
    for dep in item.dep_fbs:
        normalized = _normalize_label(item.pkg, dep)
        if normalized not in seen:
            seen.add(normalized)
            labels.append(normalized)
            frontier.append(normalized)

    idx = 0
    while idx < len(frontier):
        current_label = frontier[idx]
        idx += 1
        dep_item = item_by_label.get(current_label)
        if dep_item is None:
            continue
        for dep in dep_item.dep_fbs:
            normalized = _normalize_label(dep_item.pkg, dep)
            if normalized not in seen:
                seen.add(normalized)
                labels.append(normalized)
                frontier.append(normalized)

    return labels


def _collect_registry_direct_edge_errors(repo_root: Path) -> List[str]:
    """Invariant (i): declared direct `dep_fbs` == textual direct includes."""
    errors: List[str] = []
    message_root = repo_root / MESSAGE_REL

    for item in _parse_registry_schema_items(repo_root):
        root_fbs_path = _label_to_fbs_path(repo_root, item.pkg, item.root_fbs)
        if not root_fbs_path.exists():
            errors.append(
                f"registry item '{item.stem}' ({item.pkg}): root fbs file missing: {root_fbs_path}"
            )
            continue

        declared_paths: Dict[Path, str] = {}
        for dep in item.dep_fbs:
            dep_path = _label_to_fbs_path(repo_root, item.pkg, dep).resolve()
            declared_paths[dep_path] = dep

        textual_paths: Dict[Path, str] = {}
        for include in parse_schema_includes(root_fbs_path):
            include_path = _resolve_include_path(message_root, root_fbs_path, include).resolve()
            textual_paths[include_path] = include

        missing = sorted(set(textual_paths) - set(declared_paths), key=str)
        extra = sorted(set(declared_paths) - set(textual_paths), key=str)

        for path in missing:
            errors.append(
                f"registry item '{item.stem}' ({item.pkg}): '{item.root_fbs}' textually includes "
                f"'{textual_paths[path]}' ({path}) but dep_fbs does not declare it as a direct dep."
            )
        for path in extra:
            errors.append(
                f"registry item '{item.stem}' ({item.pkg}): dep_fbs declares '{declared_paths[path]}' "
                f"({path}) as a DIRECT dep, but '{item.root_fbs}' does not textually include it "
                "(looks like a transitive-only dep listed as direct, or a stale entry)."
            )

    return errors


def _collect_registry_transitive_closure_errors(repo_root: Path) -> List[str]:
    """Invariant (ii): registry-expanded dep_fbs closure == textual transitive closure."""
    errors: List[str] = []
    message_root = repo_root / MESSAGE_REL
    items = _parse_registry_schema_items(repo_root)
    item_by_label = _registry_items_by_label(items)

    for item in items:
        root_fbs_path = _label_to_fbs_path(repo_root, item.pkg, item.root_fbs)
        if not root_fbs_path.exists():
            continue  # already reported by the direct-edge invariant above

        expanded_labels = _collect_transitive_dep_fbs_labels(item, item_by_label)
        expanded_paths = {
            _label_to_fbs_path(repo_root, item.pkg, label).resolve(): label
            for label in expanded_labels
        }

        textual_paths = {
            path.resolve()
            for path in _collect_include_fbs_recursive(message_root, root_fbs_path)
        }

        missing = sorted(str(p) for p in textual_paths - set(expanded_paths))
        extra = sorted(str(p) for p in set(expanded_paths) - textual_paths)

        if missing:
            errors.append(
                f"registry item '{item.stem}' ({item.pkg}): transitive textual includes reach "
                f"{missing} but the registry-expanded dep_fbs closure does not — some dep_fbs edge "
                "in the chain is missing an entry."
            )
        if extra:
            errors.append(
                f"registry item '{item.stem}' ({item.pkg}): registry-expanded dep_fbs closure "
                f"reaches {extra} but no transitive textual include does — some dep_fbs edge in the "
                "chain is stale or over-declared."
            )

    return errors


def run_checks(repo_root: Path) -> List[str]:
    errors: List[str] = []
    try:
        _parse_registry_schema_items(repo_root)
    except (FileNotFoundError, ValueError) as err:
        return [str(err)]
    errors.extend(_collect_schema_include_errors(repo_root))
    errors.extend(_collect_rust_binding_include_errors(repo_root))
    try:
        errors.extend(_collect_cpp_binding_export_errors(repo_root))
    except ValueError as err:
        errors.append(str(err))
    errors.extend(_collect_registry_direct_edge_errors(repo_root))
    errors.extend(_collect_registry_transitive_closure_errors(repo_root))
    return errors


def _format_errors(errors: Iterable[str]) -> str:
    lines = ["Aorta message schema dependency check failed:"]
    for idx, err in enumerate(errors, start=1):
        lines.append(f"{idx}. {err}")
    return "\n".join(lines)


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repo-root",
        help="repository root path; auto-detected when omitted",
    )
    args = parser.parse_args(argv)

    try:
        repo_root = _find_repo_root(args.repo_root)
    except ValueError as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 2

    errors = run_checks(repo_root)
    if errors:
        print(_format_errors(errors), file=sys.stderr)
        return 1

    print("PASS: message schema include dependency check")
    return 0


if __name__ == "__main__":
    sys.exit(main())
