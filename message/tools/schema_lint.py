"""Schema declaration agreement over explicit data and file readers."""

import ast
import json
from pathlib import Path, PurePosixPath

from message.tools.schema_include_utils import parse_schema_includes

from message.tools.registry_lint import RegistryLintError, lint_fragment_text


# The trusted data file is also loaded by Starlark; never execute its source.
_CONTRACT = {
    node.targets[0].id: ast.literal_eval(node.value)
    for node in ast.parse(Path(__file__).with_name("schema_contract.bzl").read_text()).body
    if isinstance(node, ast.Assign)
}
SCHEMA_ITEM_FIELDS = [field for group in _CONTRACT["SCHEMA_FIELDS"].values() for field in group]


class SchemaLintError(ValueError):
    pass


def _key(item):
    return item["pkg"], item["stem"]


def _same(left, right):
    return json.dumps(left, sort_keys=True) == json.dumps(right, sort_keys=True)


def check_audit_schema(audit):
    for field, expected in (("rules_api", _CONTRACT["SCHEMA_RULES_API"]),
                            ("audit_version", _CONTRACT["SCHEMA_AUDIT_VERSION"])):
        if audit.get(field) != expected:
            raise SchemaLintError(f"AORTA_LINT_SCHEMA {field}: expected {expected}, got {audit.get(field)!r}")
    if sorted(audit.get("field_names", [])) != sorted(SCHEMA_ITEM_FIELDS):
        raise SchemaLintError("AORTA_LINT_SCHEMA field_names: incompatible declaration fields")


def check_fragment_agreement(audit, read_text):
    check_audit_schema(audit)
    parsed = {}
    for fragment in audit["fragments"]:
        path = fragment["file"]
        try:
            items = lint_fragment_text(read_text(path), path, fragment["symbol"], None,
                                       allowed_fields=audit["field_names"])
        except RegistryLintError as error:
            raise SchemaLintError(f"AORTA_L4_SYNTAX {path}: {error}") from error
        packages = fragment["packages"]
        for item in items:
            if packages is not None and item["pkg"] not in packages:
                continue
            key = _key(item)
            if key in parsed:
                raise SchemaLintError(f"AORTA_L4_COVERAGE {path}: duplicate descriptor coverage for {key}")
            parsed[key] = item

    declarations = {_key(r["original"]): r for r in audit["declarations"]}
    entries = {_key(e): e for e in audit["entries"]}
    if (len(declarations) != len(audit["declarations"]) or len(entries) != len(audit["entries"])
            or parsed.keys() != declarations.keys() or entries.keys() != declarations.keys()):
        raise SchemaLintError("AORTA_L4_COVERAGE declaration coverage differs: "
                              f"text={sorted(parsed)}, original={sorted(declarations)}, entries={sorted(entries)}")
    for key, record in declarations.items():
        original = record["original"]
        for field in parsed[key].keys() | original.keys():
            if field not in parsed[key] or field not in original:
                raise SchemaLintError(f"AORTA_L4_MISMATCH {original['root_fbs']}:{field} original field presence differs")
            bindings = record.get("label_bindings", {}).get(field, {})
            text_value = parsed[key].get(field)
            original_value = original.get(field)
            if bindings:
                if not isinstance(text_value, list) or not isinstance(original_value, list):
                    raise SchemaLintError(f"AORTA_L4_MISMATCH {original['root_fbs']}:{field} requires label lists")
                text_value = [bindings.get(value, value) for value in text_value]
                original_value = [bindings.get(value, value) for value in original_value]
            if not _same(text_value, original_value):
                raise SchemaLintError(f"AORTA_L4_MISMATCH {original['root_fbs']}:{field} original text/value differs")
        effective = dict(original)
        if record["overrides"].keys() - {"dep_fbs"}:
            raise SchemaLintError(f"AORTA_L4_OVERRIDE {key}: unsupported effective override")
        effective.update(record["overrides"])
        if not _same(effective, record["effective"]):
            raise SchemaLintError(f"AORTA_L4_MISMATCH {key}: effective values differ from recorded overrides")
        entry = entries[key]
        if len(entry["dep_fbs"]) != len(set(entry["dep_fbs"])):
            raise SchemaLintError(f"AORTA_L4_DEPENDENCY {original['root_fbs']}: duplicate direct dependency")
        for field, value in record["expected"].items():
            if field not in entry or not _same(entry[field], value):
                raise SchemaLintError(f"AORTA_L4_MISMATCH {original['root_fbs']}:{field} provider projection differs")


def check_census(audit):
    scope = audit["scope"]

    def identity(source):
        if not source["is_source"]:
            raise SchemaLintError(f"AORTA_CENSUS_GENERATED {source['path']}: expected a physical source File")
        if source["repo"] != scope["repo"] or (scope["package"] is not None and source["package"] != scope["package"]):
            raise SchemaLintError(f"AORTA_CENSUS_FOREIGN {source['repo']}:{source['path']}: outside local scope")
        return source["repo"], source["path"]

    physical = {identity(source) for source in audit["census"]}
    declared = {}
    for fragment in audit["fragments"]:
        if fragment["repo"] != scope["repo"]:
            raise SchemaLintError(f"AORTA_CENSUS_FOREIGN {fragment['repo']}: not a local fragment")
        for source in fragment["sources"]:
            key = identity(source["file"])
            entry = source["entry"]
            if key in declared and not _same(declared[key], entry):
                raise SchemaLintError(f"AORTA_CENSUS_DUPLICATE {key}: distinct declarations own one File")
            declared[key] = entry
    missing = declared.keys() - physical
    extra = physical - declared.keys()
    if missing:
        raise SchemaLintError(f"AORTA_CENSUS_MISSING declared sources absent from census: {sorted(missing)}")
    if extra:
        raise SchemaLintError(f"AORTA_CENSUS_UNREGISTERED physical sources without declarations: {sorted(extra)}")


def check_include_edges(entries, dependency_entries, source_path):
    owners = {}
    logical = {}
    physical = {}
    for entry in entries + dependency_entries:
        label = entry["fbs_label"]
        if label in owners:
            if not _same(owners[label], entry):
                raise SchemaLintError(f"AORTA_INCLUDE_OWNER {label}: conflicting owner declarations")
            continue
        owners[label] = entry
        key = entry["logical_include_path"]
        if key in logical:
            raise SchemaLintError(f"AORTA_INCLUDE_KEY {key}: ambiguous owners {logical[key]} and {label}")
        logical[key] = label
        physical.setdefault(entry["source_identity"]["path"], set()).add(label)

    for entry in entries:
        label = entry["fbs_label"]
        source = entry["source_identity"]["path"]
        declared = set(entry["dep_fbs"])
        for dep in declared:
            if dep not in owners:
                raise SchemaLintError(f"AORTA_INCLUDE_OWNER {label}: dependency has no provider owner: {dep}")
            if entry["dependency_sources"][dep] != owners[dep]["source_identity"]:
                raise SchemaLintError(f"AORTA_INCLUDE_FILE {label}: configured dependency differs from owner: {dep}")
        actual = set()
        for include in parse_schema_includes(source_path(source)):
            if (include.startswith("/") or "\\" in include
                    or any(part in ("", ".", "..") for part in include.split("/"))):
                raise SchemaLintError(f"AORTA_INCLUDE_PATH {label}: unsupported or escaping include {include!r}")
            matches = set()
            if not include.startswith("schemas/"):
                matches = physical.get(str(PurePosixPath(source).parent / include), set())
            if not matches and "/" in include and include in logical:
                matches = {logical[include]}
            if len(matches) != 1:
                raise SchemaLintError(f"AORTA_INCLUDE_UNRESOLVED {label}: include {include!r} has owners {sorted(matches)}")
            actual.update(matches)
        if actual != declared:
            raise SchemaLintError(f"AORTA_INCLUDE_EDGES {label}: undeclared={sorted(actual - declared)}, unused={sorted(declared - actual)}")
