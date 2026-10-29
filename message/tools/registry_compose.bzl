"""L3 of the schema-decoupling design: compose module fragments in the CONSUMER
root and fail on conflicts at analysis time.

Phase discipline:

- Target generation stays where it already is — loading phase, in each
  module's own BUILD, through `aorta_schema_package`. A composed JSON cannot
  retroactively create a `native.alias` / `cc_library` / `rust_library`.
- Composition and conflict detection happen HERE, during analysis, reading
  `AortaSchemaFragmentInfo` providers. Nothing reads a `.bzl` file's text; a
  rule implementation cannot, and this one does not try.
- Text-vs-value agreement is L4, an execution-phase lint owned by each module.

Direction: consumer -> {aorta, schema modules...} and schema module -> aorta.
This rule lives in aorta and is instantiated by the consumer; a dependency
module must never reference the consumer's registry (S does not depend on V,
and such a back-edge would also close a target cycle).

REQUIRED, NOT AUTOMATIC: creating `//msg:registry` does not make unrelated
builds depend on it. `--nobuild` will not run its action and an unreferenced
target is never built, so the consumer's CI has to build it explicitly. The
conflict checks themselves are analysis-time `fail()`s, so they fire for any
build that configures this target.
"""

load("//message/tools:schema_contract.bzl", "SCHEMA_AUDIT_VERSION", "SCHEMA_ENTRY_FIELDS", "SCHEMA_FIELDS")
load("//message/tools:schema_fragment.bzl", "AortaSchemaFragmentInfo", "SCHEMA_RULES_API", "SCHEMA_CODEGEN_VERSION", "SCHEMA_FLATBUFFERS_VERSION")

COMPOSED_MANIFEST_VERSION = 2

def _entry_identity(entry):
    """Everything that makes two declarations of one schema THE SAME one.

    Diamond dedupe: the same fragment target reached through two
    paths yields byte-identical entries and is collapsed; two DIFFERENT
    declarations of the same `(origin_repo, pkg, stem)` are a conflict.
    `json.encode` of the whole normalized entry is the identity, so a
    difference in any derived name or flag counts as different.
    """
    return json.encode(entry)

def _fmt(where):
    return "\n    - " + "\n    - ".join(where)

def _check_unique(bucket, key, description, owner, errors):
    previous = bucket.get(key)
    if previous == None:
        bucket[key] = owner
        return
    if previous == owner:
        return
    errors.append(
        "{} '{}' is claimed twice:{}".format(
            description,
            key,
            _fmt([previous, owner]),
        ),
    )

def _owner_of(entry):
    return "{} ({}:{})".format(entry["origin_module"], entry["pkg"], entry["stem"])

def _compose(fragments):
    """(entries, modules, errors) for a list of AortaSchemaFragmentInfo."""
    errors = []

    # Display names are not repository identity. A module may publish several
    # named collections, but the same module display name must not impersonate
    # two simultaneously contributing repositories.
    repo_by_module = {}
    tool_identity = None
    for info in fragments:
        repo = info.origin_repo
        module = info.origin_module
        if module in repo_by_module and repo_by_module[module] != repo:
            errors.append(
                ("module name '{}' is claimed by two different repositories " +
                 "('{}' and '{}'); a root-local target cannot adopt another " +
                 "module's identity").format(module, repo_by_module[module], repo),
            )
        repo_by_module[module] = repo

        if info.tool_identity["codegen_version"] != SCHEMA_CODEGEN_VERSION or info.tool_identity["flatbuffers_version"] != SCHEMA_FLATBUFFERS_VERSION:
            errors.append("incompatible codegen/FlatBuffers versions in '{}'".format(module))
        if tool_identity != None and info.tool_identity != tool_identity:
            errors.append("incompatible selected tool identity in '{}'".format(module))
        tool_identity = info.tool_identity

        if info.rules_api != SCHEMA_RULES_API:
            errors.append(
                ("module '{}' was built against SCHEMA_RULES_API {} but this aorta " +
                 "provides {}").format(module, info.rules_api, SCHEMA_RULES_API),
            )

        audit = info.lint_declaration
        fields = [field for group in SCHEMA_FIELDS.values() for field in group]
        if audit.get("rules_api") != SCHEMA_RULES_API or audit.get("audit_version") != SCHEMA_AUDIT_VERSION or sorted(audit.get("field_names", [])) != sorted(fields):
            errors.append("AORTA_SCHEMA_FIELDS {}: incompatible declaration audit schema".format(module))
        for entry in info.entries:
            if sorted(entry.keys()) != sorted(SCHEMA_ENTRY_FIELDS):
                errors.append("AORTA_SCHEMA_FIELDS {}: incompatible normalized entry fields".format(entry.get("fbs_label", module)))

    if errors:
        fail("\n".join(errors))

    # --- entry composition -------------------------------------------------
    entries = []
    by_owner_key = {}            # (origin_repo, pkg, stem) -> identity
    logical_paths = {}           # schemas/... -> owner
    output_aliases = {}          # fbs_schema_* -> owner
    crate_names = {}             # rust crate -> owner
    binding_targets = {}         # <pkg>:<binding target> -> owner
    facade_names = {}            # rust facade module -> owner
    cpp_headers = {}
    python_modules = {}          # <stem>_schema_meta.py -> owner

    for info in fragments:
        for entry in info.entries:
            owner_key = "{}\t{}\t{}".format(entry["origin_repo"], entry["pkg"], entry["stem"])
            identity = _entry_identity(entry)
            previous = by_owner_key.get(owner_key)
            if previous != None:
                if previous != identity:
                    errors.append(
                        ("schema '{}:{}' of module '{}' is declared twice with " +
                         "DIFFERENT metadata; the two declarations differ, so this " +
                         "is not a diamond").format(
                            entry["pkg"],
                            entry["stem"],
                            entry["origin_module"],
                        ),
                    )

                # Diamond: identical declaration reached twice. Dedupe.
                continue
            by_owner_key[owner_key] = identity
            entries.append(entry)

            owner = _owner_of(entry)
            derived = entry["derived"]

            # 1. logical FlatBuffers include identity, ACROSS modules. Two
            #    modules exporting `schemas/topic/x/y.fbs` export one wire
            #    identity through two files; bfbs_stage.sh already rejects it
            #    at execution time, this reports it at analysis time and for
            #    every language at once.
            _check_unique(
                logical_paths,
                entry["logical_include_path"],
                "logical include path",
                owner,
                errors,
            )

            # 2. derived root alias face (`//message:fbs_schema_*`).
            for alias in entry["output_aliases"]:
                _check_unique(output_aliases, alias, "schema output alias", owner, errors)

            # 3. derived Rust crate name — two crates with one name only
            #    surface as an ambiguous-name rustc error in whoever depends
            #    on both.
            if derived["rust_crate_name"] != None:
                _check_unique(
                    crate_names,
                    derived["rust_crate_name"],
                    "Rust crate name",
                    owner,
                    errors,
                )
            if derived["rust_facade_name"] != None:
                _check_unique(
                    facade_names,
                    "{}|{}".format(entry["facade_label"] or (entry["origin_repo"] + entry["pkg"]), derived["rust_facade_name"]),
                    "Rust facade module name",
                    owner,
                    errors,
                )

            # 4. derived wrapper target names, per package (the label face a
            #    consumer writes down).
            for role in ["rust_wrapper", "cpp_wrapper"]:
                target = entry["output_labels"].get(role)
                if target != None:
                    _check_unique(
                        binding_targets,
                        str(info.output_targets[Label(target)].label),
                        "message binding target",
                        owner,
                        errors,
                    )

            if derived["cpp_binding_header"] != None:
                _check_unique(cpp_headers, derived["cpp_binding_header"], "C++ wrapper header", owner, errors)

            # 5. Python installed path. `python_msgs_tree` copies every
            #    schema-meta module to the wheel tree ROOT with `cp -f`
            #    (flatbuffers_rules.bzl:105-106), so two packages that share a
            #    stem silently overwrite each other's module.
            if derived["schema_meta_py_module"] != None:
                _check_unique(
                    python_modules,
                    derived["schema_meta_py_module"],
                    "Python schema-meta module path",
                    owner,
                    errors,
                )

    owners = {entry["fbs_label"]: entry for entry in entries}
    for entry in entries:
        for dep in entry["dep_fbs"]:
            owner = owners.get(dep)
            if owner == None:
                errors.append("{}: dependency has no contributing owner: '{}'".format(_owner_of(entry), dep))
            elif entry["dependency_sources"][dep] != owner["source_identity"]:
                errors.append("{}: dependency source disagrees with owner: '{}'".format(_owner_of(entry), dep))

    modules = []
    seen_modules = {}
    for info in fragments:
        key = "{}\t{}\t{}".format(info.origin_repo, info.origin_module, info.collection_name)
        if key not in seen_modules:
            collection = {
                "origin_repo": info.origin_repo,
                "origin_module": info.origin_module,
                "collection_name": info.collection_name,
                "tool_identity": info.tool_identity,
                "rules_api": info.rules_api,
                "entry_count": 0,
                "entries": [],
            }
            seen_modules[key] = collection
            modules.append(collection)
        collection = seen_modules[key]
        members = {label: True for label in collection["entries"]}
        for entry in info.entries:
            members[entry["fbs_label"]] = True
        collection["entries"] = sorted(members)
        collection["entry_count"] = len(members)

    return entries, modules, errors

def _registry_impl(ctx):
    fragments = [dep[AortaSchemaFragmentInfo] for dep in ctx.attr.fragments]
    entries, modules, errors = _compose(fragments)

    if errors:
        fail(
            "aorta_registry({}): the composed schema registry has {} conflict(s):{}".format(
                str(ctx.label),
                len(errors),
                _fmt(errors),
            ),
        )

    # NEW document, separately versioned. The existing v3
    # per-module manifest and its frozen golden are untouched: each row below
    # is a verbatim v3 row with `origin_module` alongside it, so a v3 reader
    # keeps reading v3 and only a composition-aware reader needs this file.
    rows = []
    for entry in entries:
        row = dict(entry)
        row["schema_definition"] = row.pop("row")
        rows.append(row)

    manifest = {
        "composed_manifest_version": COMPOSED_MANIFEST_VERSION,
        "schema_definition_manifest_version": 3,
        "rules_api": SCHEMA_RULES_API,
        "codegen_version": SCHEMA_CODEGEN_VERSION,
        "flatbuffers_version": SCHEMA_FLATBUFFERS_VERSION,
        "modules": modules,
        "entries": rows,
    }
    out = ctx.actions.declare_file(ctx.label.name + ".json")
    template = ctx.actions.declare_file(ctx.label.name + ".unverified.json")
    ctx.actions.write(output = template, content = json.encode_indent(manifest) + "\n")
    sources = depset(transitive = [info.tool_sources for info in fragments]).to_list()
    args = ctx.actions.args()
    args.add_all([template.path, out.path, ctx.executable._flatc.path])
    args.add_all(sources)
    ctx.actions.run(
        executable = ctx.executable._tool_identity_writer,
        env = {"PYTHONDONTWRITEBYTECODE": "1"},
        inputs = [template] + sources,
        tools = [ctx.executable._flatc],
        outputs = [out],
        arguments = [args],
        mnemonic = "AortaRegistryContract",
    )
    return [DefaultInfo(files = depset([out]))]

aorta_registry = rule(
    implementation = _registry_impl,
    doc = """Compose schema fragments from several modules and check conflicts.

Instantiated in the CONSUMER root, never in a dependency module. Fails during
analysis on: a duplicate `(origin_module, pkg, stem)` with differing metadata;
a logical FlatBuffers include path claimed by two modules; a derived schema
alias / Rust crate / Rust facade module / message-binding target / Python
schema-meta module path claimed twice; a module built against a different
`SCHEMA_RULES_API`; or two targets claiming one module identity.

A diamond — the same fragment target reached through two paths — is deduped by
identity, not reported.""",
    attrs = {
        "_tool_identity_writer": attr.label(default = Label("//message/tools:registry_tool_identity"), executable = True, cfg = "exec"),
        "_flatc": attr.label(default = Label("//message/tools:flatc"), executable = True, cfg = "exec"),
        "fragments": attr.label_list(
            mandatory = True,
            providers = [AortaSchemaFragmentInfo],
            doc = "`aorta_schema_fragment` targets, one or more per module.",
        ),
    },
)
