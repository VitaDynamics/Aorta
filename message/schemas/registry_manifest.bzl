"""Generate a machine-readable schema registry manifest from SCHEMA_DEFINITIONS."""

load("//message/tools:schema_names.bzl", "schema_item_manifest_row")
load(
    "//message:schemas/registry.bzl",
    "CPP_MESSAGE_BINDINGS",
    "MESSAGE_BINDING_ALIASES",
    "RUST_MESSAGE_BINDINGS",
    "SCHEMA_DEFINITIONS",
    "SCHEMA_OUTPUT_ALIASES",
    "SCHEMA_PACKAGE_ALIASES",
)

_MANIFEST_VERSION = 3


# The row shape lives in `//message/tools:schema_names.bzl` so the L2 fragment
# provider and the L3 composer can emit the same
# v3-shaped row WITHOUT loading this file — which, through
# `schemas/registry.bzl`, would drag a separate product-schema module into
# their load graph. The frozen v3 golden is therefore also the regression test
# on that shared definition.
_schema_item_manifest = schema_item_manifest_row


def _rust_binding_manifest(item):
    return {
        "name": item.name,
        "crate_name": item.crate_name,
        "schema_pkg": item.schema_pkg,
        "schema_stem": item.schema_stem,
        "include_schema_meta": item.include_schema_meta,
    }


def _cpp_binding_manifest(item):
    return {
        "name": item.name,
        "schema_pkg": item.schema_pkg,
        "schema_stem": item.schema_stem,
        "schema_cc_target": item.schema_cc_target,
        "wrapper_header": item.wrapper_header,
    }


def _alias_pairs(alias_items):
    pairs = []
    for name, actual in alias_items:
        pairs.append({
            "name": name,
            "actual": actual,
        })
    return pairs


def _package_alias_entries():
    entries = []
    for item in SCHEMA_PACKAGE_ALIASES:
        entries.append({
            "pkg": item.pkg,
            "name": item.name,
            "actual": item.actual,
        })
    return entries


def _schema_manifest():
    return {
        "manifest_version": _MANIFEST_VERSION,
        "schema_definitions": [_schema_item_manifest(item) for item in SCHEMA_DEFINITIONS],
        "rust_message_bindings": [_rust_binding_manifest(item) for item in RUST_MESSAGE_BINDINGS],
        "cpp_message_bindings": [_cpp_binding_manifest(item) for item in CPP_MESSAGE_BINDINGS],
        "schema_output_aliases": _alias_pairs(SCHEMA_OUTPUT_ALIASES),
        "message_binding_aliases": _alias_pairs(MESSAGE_BINDING_ALIASES),
        "package_aliases": _package_alias_entries(),
    }


def _aorta_schema_registry_manifest_impl(ctx):
    ctx.actions.write(
        output = ctx.outputs.out,
        content = json.encode(_schema_manifest()) + "\n",
    )
    return [DefaultInfo(files = depset([ctx.outputs.out]))]


aorta_schema_registry_manifest = rule(
    implementation = _aorta_schema_registry_manifest_impl,
    outputs = {"out": "%{name}.json"},
)
