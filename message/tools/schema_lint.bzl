"""Module-local declaration and independent physical source checks."""

load("//message/tools:schema_contract.bzl", "SCHEMA_AUDIT_VERSION", "SCHEMA_RULES_API", "SCHEMA_FIELDS")
load("//message/tools:schema_fragment.bzl", "AortaSchemaFragmentInfo")
load("//message/tools:schema_source_actions.bzl", "source_reflection_attrs", "source_reflections")
load("//message/tools:schema_lint_test.bzl", "schema_lint_test")

def schema_fragment_lint_test(name, audit, visibility = None):
    schema_lint_test(name = name, audit = audit, visibility = visibility)

def schema_include_lint_test(name, audit, visibility = None):
    schema_lint_test(name = name, audit = audit, mode = "include", visibility = visibility)

def _source_record(file):
    prefix = file.owner.workspace_root + "/" if file.owner.workspace_root else ""
    return {
        "repo": file.owner.repo_name,
        "path": file.path[len(prefix):] if prefix and file.path.startswith(prefix) else file.path,
        "package": file.owner.package,
        "is_source": file.is_source,
    }

def _census_inputs_impl(ctx):
    fragments = []
    files = list(ctx.files.census)
    seen = {}
    for target in ctx.attr.fragments:
        if target.label in seen:
            continue
        seen[target.label] = True
        info = target[AortaSchemaFragmentInfo]
        audit = dict(info.lint_declaration)
        audit["entries"] = info.entries
        fragments.append({
            "repo": info.origin_repo,
            "audit": audit,
            "sources": [{"entry": entry, "file": _source_record(info.source_files[Label(entry["fbs_label"])])} for entry in info.entries],
        })
        files.extend(info.fragment_files)
        files.extend(info.sources.to_list())
    evidence, artifacts = source_reflections(ctx, [target[AortaSchemaFragmentInfo] for target in ctx.attr.fragments], [target[AortaSchemaFragmentInfo] for target in ctx.attr.dependency_fragments]) if not ctx.attr.package_only else ({}, [])
    files.extend(artifacts)
    payload = {
        "audit_version": SCHEMA_AUDIT_VERSION,
        "rules_api": SCHEMA_RULES_API,
        "field_names": [field for group in SCHEMA_FIELDS.values() for field in group],
        "source_evidence": evidence,
        "scope": {"repo": ctx.label.repo_name, "package": ctx.label.package if ctx.attr.package_only else None},
        "census": [_source_record(f) for f in ctx.files.census],
        "fragments": fragments,
        "dependency_entries": [entry for target in ctx.attr.dependency_fragments for entry in target[AortaSchemaFragmentInfo].entries],
        "files": {
            f.path: f.short_path[3:] if f.short_path.startswith("../") else ctx.workspace_name + "/" + f.short_path
            for f in files
        },
    }
    output = ctx.actions.declare_file(ctx.label.name + ".json")
    ctx.actions.write(output, json.encode(payload))
    return [DefaultInfo(files = depset([output]), runfiles = ctx.runfiles(files = files + [output]))]

_census_inputs = rule(
    implementation = _census_inputs_impl,
    attrs = {
        "fragments": attr.label_list(providers = [AortaSchemaFragmentInfo], mandatory = True),
        "census": attr.label_list(allow_files = [".fbs"], mandatory = True),
        "dependency_fragments": attr.label_list(providers = [AortaSchemaFragmentInfo]),
        "package_only": attr.bool(),
    } | source_reflection_attrs(),
)

def aorta_schema_module_lint_test(name, fragments, census, dependency_fragments = [], visibility = None):
    _census_inputs(
        name = name + "_inputs",
        fragments = fragments,
        census = census,
        dependency_fragments = dependency_fragments,
        visibility = ["//visibility:private"],
    )
    schema_lint_test(name = name, audit = ":" + name + "_inputs", mode = "module", visibility = visibility)

def schema_package_census_lint_test(name, fragment, census, visibility = None):
    _census_inputs(
        name = name + "_inputs",
        fragments = [fragment],
        census = census,
        package_only = True,
        visibility = ["//visibility:private"],
    )
    schema_lint_test(name = name, audit = ":" + name + "_inputs", mode = "census", visibility = visibility)
