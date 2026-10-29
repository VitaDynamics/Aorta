"""L2 of the schema-decoupling design: expose a module's registry fragment as a
provider (layer L2).

WHY A TARGET AND NOT A `load()`
-------------------------------
The composer (`aorta_registry`, L3) runs in the CONSUMER's root and must see
every module's entries. It cannot `load()` them: `load()` is a static top-level
statement, so a rule/macro cannot evaluate a `.bzl` file chosen by its caller
(review §A3), and a rule implementation cannot read a source file's text during
analysis either (review §A2). The only mechanism that carries *already
evaluated* Starlark values across a module boundary into the analysis phase is
a provider on a target.

So each module `load()`s its OWN fragment in its OWN BUILD file — which is
loading-phase, module-local, and creates no aorta→product edge — normalizes the
entries, and publishes them here. `aorta_registry` then reads providers only.

ORIGIN AND COLLECTIONS
----------------------
`origin_repo` is Bazel's opaque canonical repository token, used for identity
without parsing. `origin_module` is an explicit display name; `collection_name`
labels a subset within that repository. Neither display field proves ownership.
Source and dependency Labels and Files carry actual repository attribution.
Publication is public/fixture/internal; codegen_policy is external/internal.

L4
--
Authored or generated `fragment_files` travel with original/effective values.
The private audit rule serializes these inputs for the module's own execution
test; fragments and composers do not depend on that test.
"""

load("//message/tools:schema_contract.bzl", "SCHEMA_AUDIT_VERSION", _RULES_API = "SCHEMA_RULES_API")
load("//message/tools:schema_names.bzl", "SCHEMA_ITEM_FIELDS", "normalize_schema_entry", "normalize_dep_label", "item_get", "schema_output_labels")
load("//message/tools:schema_lint_test.bzl", "schema_lint_test")
load("//message/tools:schema_source_actions.bzl", "source_reflection_attrs", "source_reflections")
load("//message/tools:schema_output_provenance.bzl", "SchemaOutputProvenanceInfo", "schema_output_provenance")

# Bumped for ANY change a consumer could observe: macro signatures, codegen
# semantics, FlatBuffers runtime compatibility, or BFBS bytes (review §A1 —
# a Git override removes the module from version resolution entirely, so
# `bazel_dep(version = ...)` proves nothing and this number is the only real
# contract).
SCHEMA_RULES_API = _RULES_API
SCHEMA_CODEGEN_VERSION = "p1.2"
SCHEMA_FLATBUFFERS_VERSION = "25.12.19"


AortaSchemaFragmentInfo = provider(
    doc = "Normalized registry entries of ONE schema module, for L3 composition.",
    fields = {
        "origin_repo": (
            "Opaque canonical repository token of the target that declared " +
            "this fragment (`ctx.label.repo_name`). Equality only — never " +
            "parsed, never displayed as an API value."
        ),
        "origin_module": "Explicit display name, not repository identity.",
        "collection_name": "Display name of this collection within its module.",
        "tool_identity": "Selected tool labels/files plus required codegen and FlatBuffers versions.",
        "tool_sources": "Codegen source Files whose bytes the composed manifest hashes.",
        "source_files": "Canonical declaration Label to source File.",
        "output_targets": "Requested canonical output Label to configured Target (aliases resolved).",
        "rules_api": "int: the `SCHEMA_RULES_API` the declaring module compiled against.",
        "entries": "list of normalized entry dicts (see `normalize_schema_entry`).",
        "sources": "depset of the `.fbs` `File`s backing the entries.",
        "fragment_files": "list of the fragment `.bzl` `File`s, for the L4 text lint.",
        "lint_declaration": "Original/effective declarations, descriptors and configured label bindings for L4.",
        "dep_labels": "Deduplicated Label values resolved in the declaring module, never root-relative strings.",
    },
)

def _generated_output_files(target):
    files = target[DefaultInfo].files.to_list()
    if CcInfo in target:
        files.extend(target[CcInfo].compilation_context.direct_headers)
    return depset(files).to_list()

def _output_generator_path(files, role):
    name = "gen_action_wrapper.py" if role == "action_schema" else ("gen_schema_meta.py" if role.startswith("schema_meta") else "gen_message_wrapper.py")
    return [file.path for file in files if file.basename == name][0]

def _fragment_impl(ctx):
    entries = json.decode(ctx.attr.entries_json)
    audit = json.decode(ctx.attr.audit_json)
    if type(entries) != "list":
        fail("entries_json must decode to a list, got {}".format(type(entries)))

    # Per-entry source `File`, keyed on the entry's own `<pkg>|<stem>`. A
    # `label_keyed_string_dict` is what makes this exact: matching on basename
    # would be ambiguous the moment one fragment spans two packages that share
    # a stem, which `(pkg, stem)` explicitly allows.
    sources_by_key = {}
    for target, key in ctx.attr.srcs.items():
        files = target[DefaultInfo].files.to_list()
        if len(files) != 1:
            fail(
                "schema source '{}' must resolve to exactly one file, got {}".format(
                    str(target.label),
                    len(files),
                ),
            )
        if key in sources_by_key:
            fail("two schema sources claim the entry key '{}'".format(key))
        sources_by_key[key] = files[0]

    dependency_sources = {}
    for label, target in zip(ctx.attr.dependency_labels, ctx.attr.dependency_sources):
        files = target[DefaultInfo].files.to_list()
        if len(files) != 1:
            fail("dependency '{}' must resolve to one source File".format(label))
        dependency_sources[label] = files[0]
    output_targets = {label: target for label, target in zip(ctx.attr.output_labels, ctx.attr.output_targets)}
    provenance_targets = {label: target for label, target in zip(ctx.attr.provenance_labels, ctx.attr.provenance_targets)}
    audit["output_provenance"] = {}
    source_files = []
    source_file_map = {}
    for entry, canonical_target, record in zip(entries, ctx.attr.canonical_sources, audit["declarations"]):
        key = "{}|{}".format(entry["pkg"], entry["stem"])
        source = sources_by_key.get(key)
        if source == None:
            fail(
                ("fragment '{}' declares entry {} but no source file was wired for " +
                 "it; `aorta_schema_fragment` derives `srcs` from `items`, so this " +
                 "means the `.fbs` target does not exist in that package").format(
                    str(ctx.label),
                    key,
                ),
            )
        canonical_files = canonical_target[DefaultInfo].files.to_list()
        if len(canonical_files) != 1 or canonical_files[0] != source:
            fail("AORTA_SOURCE_FILE_MISMATCH {}: canonical label must resolve to the selected source File".format(entry["fbs_label"]))
        entry["source"] = source.short_path
        entry["source_identity"] = {"owner": str(source.owner), "path": source.path}
        entry["dependency_sources"] = {
            dep: {"owner": str(dependency_sources[dep].owner), "path": dependency_sources[dep].path}
            for dep in entry["dep_fbs"]
        }
        entry["output_files"] = {
            role: [f.path for f in output_targets[label][DefaultInfo].files.to_list()]
            for role, label in entry["output_labels"].items()
        }
        if entry.get("source_contract") != None:
            audit["output_provenance"][entry["fbs_label"]] = {
                role: [{"path": f.path, "generated": not f.is_source,
                        "required_tool": ctx.executable._flatc.path if role in ["bfbs", "rust", "cpp", "python", "cpp_lib"] else _output_generator_path(ctx.files._codegen_sources, role),
                        "sources": [source.path for source in provenance_targets[label][SchemaOutputProvenanceInfo].files.get(f, depset()).to_list()]}
                       for f in _generated_output_files(output_targets[label])]
                for role, label in entry["output_labels"].items()
            }
        if entry["facade_label"] != None:
            entry["facade_label"] = str(output_targets[entry["facade_label"]].label)
            record["expected"]["facade_label"] = entry["facade_label"]
        record["expected"]["source_identity"] = {"owner": str(canonical_files[0].owner), "path": canonical_files[0].path}
        record["expected"]["dependency_sources"] = {
            label: {"owner": str(dependency_sources[label].owner), "path": dependency_sources[label].path}
            for label in record["expected"]["dep_fbs"]
        }
        record["expected"]["output_files"] = {
            role: [f.path for f in output_targets[label][DefaultInfo].files.to_list()]
            for role, label in record["expected"]["output_labels"].items()
        }
        source_file_map[Label(entry["fbs_label"])] = source
        entry["origin_repo"] = ctx.label.repo_name
        entry["origin_module"] = ctx.attr.module_name
        source_files.append(source)

    dep_labels = []
    seen_deps = {}
    for entry in entries:
        for dep in entry["dep_fbs"]:
            if dep not in seen_deps:
                seen_deps[dep] = True
                dep_labels.append(Label(dep))

    fragment_files = ctx.files.fragment
    descriptors = []
    if ctx.attr.fragment:
        for target, descriptor in zip(ctx.attr.fragment, audit["fragments"]):
            files = target[DefaultInfo].files.to_list()
            if len(files) != 1:
                fail("L4 fragment descriptor must resolve to exactly one File: {}".format(target.label))
            descriptor["file"] = files[0].path
            descriptors.append(descriptor)
    else:
        generated = ctx.actions.declare_file(ctx.label.name + ".declaration.bzl")
        rows = []
        originals = {(r["original"]["pkg"], r["original"]["stem"]): r["original"] for r in audit["declarations"]}
        for key in sorted(originals):
            item = originals[key]
            rows.append("    struct(" + ", ".join([field + " = " + repr(item[field]) for field in sorted(item)]) + "),")
        ctx.actions.write(generated, "ENTRIES = [\n" + "\n".join(rows) + "\n]\n")
        fragment_files = [generated]
        descriptors = [{"file": generated.path, "symbol": "ENTRIES", "packages": None, "mode": "generated"}]
    audit["fragments"] = descriptors
    outputs = source_files + fragment_files
    return [
        DefaultInfo(files = depset(outputs)),
        AortaSchemaFragmentInfo(
            origin_repo = ctx.label.repo_name,
            origin_module = ctx.attr.module_name,
            rules_api = ctx.attr.rules_api,
            collection_name = ctx.attr.collection_name or ctx.attr.module_name,
            tool_sources = depset(ctx.files._codegen_sources),
            tool_identity = {
                "codegen_version": SCHEMA_CODEGEN_VERSION,
                "flatbuffers_version": SCHEMA_FLATBUFFERS_VERSION,
                "flatc_label": str(ctx.attr._flatc.label),
                "flatc_executable": ctx.executable._flatc.path,
                "codegen_sources": [f.path for f in ctx.files._codegen_sources],
            },
            source_files = source_file_map,
            output_targets = {Label(label): target for label, target in output_targets.items()},
            entries = entries,
            sources = depset(source_files),
            fragment_files = fragment_files,
            lint_declaration = audit,
            dep_labels = dep_labels,
        ),
    ]

_aorta_schema_fragment = rule(
    implementation = _fragment_impl,
    doc = "L2 metadata face of one schema module's registry fragment.",
    attrs = {
        "entries_json": attr.string(
            mandatory = True,
            doc = "`json.encode` of the normalized entry list (the macro writes it).",
        ),
        "audit_json": attr.string(mandatory = True),
        "canonical_sources": attr.label_list(allow_files = [".fbs"]),
        "srcs": attr.label_keyed_string_dict(
            mandatory = True,
            allow_files = [".fbs"],
            doc = "`.fbs` label -> `<pkg>|<stem>` of the entry it backs.",
        ),
        "dependency_sources": attr.label_list(allow_files = [".fbs"]),
        "dependency_labels": attr.string_list(),
        "output_targets": attr.label_list(),
        "provenance_targets": attr.label_list(aspects = [schema_output_provenance]),
        "provenance_labels": attr.string_list(),
        "output_labels": attr.string_list(),
        "collection_name": attr.string(),
        "_flatc": attr.label(default = Label("//message/tools:flatc"), executable = True, cfg = "exec"),
        "_codegen_sources": attr.label_list(default = [
            Label("//message/tools:schema_codegen.bzl"),
            Label("//message/tools:schema_package.bzl"),
            Label("//message/tools:schema_names.bzl"),
            Label("//message/tools:schema_contract.bzl"),
            Label("//message/tools:flatbuffers_rules.bzl"),
            Label("//message/tools:bfbs_stage.sh"),
            Label("//message/tools:message_bindings.bzl"),
            Label("//message/tools:schema_include_utils.py"),
            Label("//message/tools:patch_flatc_rust.py"),
            Label("//message/tools:gen_message_wrapper.py"),
            Label("//message/tools:gen_schema_meta.py"),
            Label("//message/tools:gen_action_wrapper.py"),
            Label("//message/tools:gen_python_fastpath.py"),
            Label("//message/tools:schema_reflection.py"),
            Label("//message/tools:python_outputs.bzl"),
        ], allow_files = True),
        "fragment": attr.label_list(
            allow_files = [".bzl"],
            doc = "The pure-data fragment `.bzl` file(s), for the L4 text lint.",
        ),
        "module_name": attr.string(
            doc = "Explicit module display name, separate from the repository token.",
        ),
        "rules_api": attr.int(
            mandatory = True,
            doc = "`SCHEMA_RULES_API` the declaring module was written against.",
        ),
    },
)

def _schema_lint_inputs_impl(ctx):
    info = ctx.attr.fragment[AortaSchemaFragmentInfo]
    audit = dict(info.lint_declaration)
    audit["entries"] = info.entries
    audit["dependency_entries"] = [entry for target in ctx.attr.dependencies for entry in target[AortaSchemaFragmentInfo].entries]
    evidence, artifacts = source_reflections(ctx, [info], [target[AortaSchemaFragmentInfo] for target in ctx.attr.dependencies])
    audit["source_evidence"] = evidence
    inputs = info.fragment_files + info.sources.to_list() + artifacts
    audit["files"] = {
        f.path: f.short_path[3:] if f.short_path.startswith("../") else ctx.workspace_name + "/" + f.short_path
        for f in inputs
    }
    audit["owner"] = {"origin_repo": info.origin_repo, "origin_module": info.origin_module, "collection_name": info.collection_name}
    out = ctx.actions.declare_file(ctx.label.name + ".json")
    ctx.actions.write(out, json.encode(audit))
    return [DefaultInfo(files = depset([out]), runfiles = ctx.runfiles(files = inputs + [out]))]

_schema_lint_inputs = rule(
    implementation = _schema_lint_inputs_impl,
    attrs = {
        "fragment": attr.label(providers = [AortaSchemaFragmentInfo]),
        "dependencies": attr.label_list(providers = [AortaSchemaFragmentInfo]),
    } | source_reflection_attrs(),
)

def aorta_schema_fragment(
        name,
        items,
        api,
        fragment = None,
        fragment_symbols = {},
        fragment_packages = None,
        lint_dependencies = [],
        original_items = None,
        dep_fbs_overrides = None,
        srcs = None,
        module_name = "",
        collection_name = "",
        wrapper_packages = {},
        facade_targets = {},
        visibility = None):
    """Publish one module's registry entries as `AortaSchemaFragmentInfo`.

    Args:
      name: target name.
      items: the ALREADY LOADED fragment list (structs or dicts). The macro
        normalizes and `json.encode`s it, because a rule attribute cannot
        carry Starlark structs.
      fragment: label(s) of the pure-data `.bzl` file(s) `items` came from.
        Carried as a `File` for the module's own L4 text lint.
      fragment_symbols: fragment label to pure-data list symbol (default ENTRIES).
      fragment_packages: selected package list, or None for the complete files.
      lint_dependencies: provider owners for include lint only, not codegen edges.
      original_items: pre-override items, supplied by the package adapter.
      dep_fbs_overrides: recorded per-stem dependency replacements.
      srcs: optional `{fbs label: "<pkg>|<stem>"}` override. Omit it and the
        macro derives one entry per item from `pkg` + `root_fbs`, which is
        correct for every module that owns its sources.
      module_name: human-readable owning module name (e.g. `"aorta"`).
      api: explicit `SCHEMA_RULES_API` this module was written against.
      collection_name: optional display name for this subset of the module.
      wrapper_packages: language-to-package mapping for legacy global wrappers.
      facade_targets: package-to-label mapping for actual containing facades.
      visibility: target visibility.
    """
    if api != SCHEMA_RULES_API:
        fail(
            ("aorta_schema_fragment('{}'): api = {} but this aorta provides " +
             "SCHEMA_RULES_API = {}. Bump the schema module's `api` (and re-run " +
             "its own tests) or pin an aorta that provides the requested API.").format(
                name,
                api,
                SCHEMA_RULES_API,
            ),
        )
    if not items:
        fail("aorta_schema_fragment('{}'): items must be a non-empty list".format(name))

    if not module_name:
        fail("aorta_schema_fragment requires an explicit module_name display name")
    original_items = items if original_items == None else original_items
    if len(original_items) != len(items):
        fail("L4 original/effective declaration lengths differ")
    originals = [dict(item) if type(item) == "dict" else {field: getattr(item, field) for field in dir(item) if field not in ["to_json", "to_proto"]} for item in original_items]
    effective = [dict(item) if type(item) == "dict" else {field: getattr(item, field) for field in dir(item) if field not in ["to_json", "to_proto"]} for item in items]
    normalized = [normalize_schema_entry(item) for item in items]
    dependencies = {}
    output_labels = {}
    for item, entry in zip(items, normalized):
        entry["fbs_label"] = str(native.package_relative_label(entry["fbs_label"]))
        resolved_deps = []
        for dep in entry["dep_fbs"]:
            label = native.package_relative_label(dep)
            if "[unknown repo" in str(label):
                fail("dependency '{}' has an unresolved repository".format(dep))
            resolved_deps.append(str(label))
            dependencies[str(label)] = label
        entry["dep_fbs"] = resolved_deps
        entry["extra_cc_deps"] = [str(native.package_relative_label(dep)) for dep in item_get(item, "extra_cc_deps", [])]
        entry["output_labels"] = {
            role: str(native.package_relative_label(label))
            for role, label in schema_output_labels(item, wrapper_packages).items()
        }
        entry["facade_label"] = str(native.package_relative_label(facade_targets[entry["pkg"]])) if entry["pkg"] in facade_targets else None
        if entry["facade_label"] != None:
            entry["output_labels"]["rust_facade"] = entry["facade_label"]
        for label in entry["output_labels"].values():
            output_labels[label] = True

    if srcs == None:
        srcs = {}
        for entry in normalized:
            label = entry["fbs_label"]
            key = "{}|{}".format(entry["pkg"], entry["stem"])
            if label in srcs:
                fail(
                    ("aorta_schema_fragment('{}'): two entries claim the same .fbs " +
                     "label '{}'").format(name, label),
                )
            srcs[label] = key

    fragment_labels = fragment
    if fragment_labels == None:
        fragment_labels = []
    elif type(fragment_labels) == "string":
        fragment_labels = [fragment_labels]

    declarations = []
    for original, item, entry in zip(originals, effective, normalized):
        overrides = {}
        if dep_fbs_overrides != None and original["stem"] in dep_fbs_overrides:
            overrides["dep_fbs"] = dep_fbs_overrides[original["stem"]]
        bindings = {}
        context = native.package_relative_label(":__lint_context")
        for field in ["dep_fbs", "extra_cc_deps"]:
            values = {}
            for declaration in [original, item]:
                for spelling in declaration.get(field, []):
                    absolute = normalize_dep_label(declaration["pkg"], spelling) if field == "dep_fbs" else spelling
                    label = native.package_relative_label(absolute)
                    canonical = str(label)
                    # An overridden original may name an unavailable repository.
                    # Preserve its raw spelling; only effective edges must resolve.
                    if "[unknown repo" in canonical:
                        continue
                    values[spelling] = canonical
                    values[absolute] = canonical
                    values[canonical] = canonical
                    if label.repo_name == context.repo_name:
                        values["//" + label.package + ":" + label.name] = canonical
                        source_package = native.package_relative_label(declaration["pkg"] + ":__lint_context").package if field == "dep_fbs" else context.package
                        if label.package == source_package:
                            values[":" + label.name] = canonical
                            values[label.name] = canonical
            bindings[field] = values
        declarations.append({"original": original, "effective": item, "overrides": overrides, "expected": entry, "label_bindings": bindings})
    audit = {
        "audit_version": SCHEMA_AUDIT_VERSION,
        "rules_api": SCHEMA_RULES_API,
        "field_names": SCHEMA_ITEM_FIELDS,
        "declarations": declarations,
        "fragments": [{"symbol": fragment_symbols.get(label, "ENTRIES"), "packages": fragment_packages, "mode": "authored"} for label in fragment_labels],
    }

    _aorta_schema_fragment(
        name = name,
        entries_json = json.encode(normalized),
        audit_json = json.encode(audit),
        canonical_sources = [entry["fbs_label"] for entry in normalized],
        srcs = srcs,
        fragment = fragment_labels,
        module_name = module_name,
        rules_api = api,
        collection_name = collection_name,
        dependency_sources = dependencies.values(),
        dependency_labels = dependencies.keys(),
        provenance_targets = {label: True for entry in normalized if entry.get("source_contract") != None for label in entry["output_labels"].values()}.keys(),
        provenance_labels = {label: True for entry in normalized if entry.get("source_contract") != None for label in entry["output_labels"].values()}.keys(),
        output_targets = output_labels.keys(),
        output_labels = output_labels.keys(),
        visibility = visibility,
    )
    _schema_lint_inputs(name = name + "_lint_inputs", fragment = ":" + name, dependencies = lint_dependencies, visibility = ["//visibility:private"])
    schema_lint_test(name = name + "_fragment_lint_test", audit = ":" + name + "_lint_inputs", visibility = visibility)
    schema_lint_test(name = name + "_include_lint_test", audit = ":" + name + "_lint_inputs", mode = "include", visibility = visibility)
    schema_lint_test(name = name + "_source_lint_test", audit = ":" + name + "_lint_inputs", mode = "source", visibility = visibility)
