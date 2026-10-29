"""Frozen API-2 versions and field sets shared by Starlark and Python."""

SCHEMA_RULES_API = 2
SCHEMA_AUDIT_VERSION = 2

SCHEMA_FIELDS = {
    "flags": [
        "rust", "cpp", "python", "bfbs", "cpp_lib", "rust_wrapper", "cpp_wrapper",
        "schema_meta_rust", "schema_meta_cpp", "action_schema", "edu_public",
        "include_in_examples_bfbs",
    ],
    "declaration": [
        "pkg", "stem", "root_fbs", "provenance", "publication", "codegen_policy",
        "schema_meta_py", "dep_fbs", "extra_cc_deps", "python_include_dirs",
        "action_name", "alias_id", "cpp_lib_name", "cc_alias", "rust_wrapper_name",
        "rust_wrapper_crate_name", "rust_wrapper_facade_name", "cpp_wrapper_name",
        "cpp_wrapper_header",
    ],
    "source_contract": [
        "schema_kind", "root_type", "namespace", "file_identifier",
        "request_type", "response_type",
    ],
}

SCHEMA_ENTRY_FIELDS = [
    "action_name", "codegen_policy", "dep_fbs", "dependency_sources", "derived",
    "extra_cc_deps", "facade_label", "fbs_label", "flags", "logical_include_path",
    "naming_overrides", "origin_module", "origin_repo", "output_aliases",
    "output_files", "output_labels", "pkg", "provenance", "publication",
    "python_include_dirs", "root_fbs", "row", "source", "source_contract",
    "source_identity", "stem", "typed",
]
