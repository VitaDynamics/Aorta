"""Version 1 release artifacts, separate from the frozen schema rules API 2."""

load("@rules_rust//rust/private:providers.bzl", "CrateInfo")
load(":schema_fragment.bzl", "AortaSchemaFragmentInfo")
load(":schema_output_provenance.bzl", "SchemaOutputProvenanceInfo", "schema_output_provenance")

AortaSchemaReleaseInfo = provider(fields = ["version", "tree", "identity"])
_ROLES = ["rust", "bfbs", "schema_meta_rust", "schema_meta_cpp", "schema_meta_py", "action_schema", "rust_wrapper"]

def _release_impl(ctx):
    entries = {}
    artifacts = []
    rust_crates = {}
    inputs = []
    implicit = []
    tool_sources = depset(transitive = [f[AortaSchemaFragmentInfo].tool_sources for f in ctx.attr.fragments]).to_list()
    tool_identity = ctx.attr.fragments[0][AortaSchemaFragmentInfo].tool_identity
    tool_schemas = {source: True for source in ctx.files._reflection_schema}
    for fragment in ctx.attr.fragments:
        info = fragment[AortaSchemaFragmentInfo]
        if info.rules_api != 2:
            fail("release staging requires rules API 2")
        ancestry = fragment[SchemaOutputProvenanceInfo].files
        for entry in info.entries:
            label = entry["fbs_label"]
            if label in entries:
                if entries[label] != entry:
                    fail("release source identity conflict: " + label)
                continue
            entries[label] = entry
            source = info.source_files[Label(label)]
            if entry["source_identity"] != {"owner": str(source.owner), "path": source.path}:
                fail("release source owner mismatch: " + label)
            inputs.append(source)
            if entry["origin_module"] == ctx.attr.module_name and "rust_wrapper" in entry["output_labels"]:
                implicit = ["schemas/system/aorta_base.fbs"]
            for role in _ROLES:
                output_label = entry["output_labels"].get(role)
                if output_label == None:
                    continue
                target = info.output_targets[Label(output_label)]
                if role == "rust_wrapper":
                    if CrateInfo not in target:
                        fail("release Rust wrapper lacks CrateInfo: " + output_label)
                    crate = target[CrateInfo]
                    rust_crates[label] = {
                        "name": crate.name,
                        "dependencies": [{"name": dep.crate_info.name, "owner": str(dep.crate_info.owner)} for dep in crate.deps.to_list() if dep.crate_info != None],
                    }
                    files = depset([crate.root], transitive = [crate.srcs, crate.compile_data]).to_list()
                    directory = "bindings/" + crate.name + "/"
                else:
                    files = target[DefaultInfo].files.to_list()
                    if [f.path for f in files] != entry["output_files"][role]:
                        fail("release output mapping mismatch: " + output_label)
                    directory = "generated/" + entry["logical_include_path"] + "/" + role + "/"
                if not files:
                    fail("release missing output: " + output_label)
                for file in files:
                    if file.is_source or file.is_directory:
                        fail("release expects generated file: " + file.path)
                    file_role = "rust_support" if role == "rust_wrapper" and file != crate.root else role
                    input_sources = [f.path for f in ancestry.get(file, depset()).to_list() if f.extension == "fbs" and f not in tool_schemas]
                    artifacts.append({
                        "entry": label,
                        "role": file_role,
                        "path": file.path,
                        "owner": str(file.owner),
                        "sources": input_sources,
                        "relative": directory + file.basename,
                    })
                    inputs.append(file)
    licenses = []
    for target, module in ctx.attr.licenses.items():
        for file in target[DefaultInfo].files.to_list():
            licenses.append({"module": module, "path": file.path, "relative": "licenses/" + module + "/" + file.basename})
            inputs.append(file)
    identity = {"module_name": ctx.attr.module_name, "module_revision": ctx.attr.module_revision, "aorta_revision": ctx.attr.aorta_revision}
    tree = ctx.actions.declare_directory(ctx.label.name)
    plan = ctx.actions.declare_file(ctx.label.name + ".plan.json")
    ctx.actions.write(plan, json.encode({
        "release_artifact_version": 1,
        "identity": identity,
        "tools": {
            "flatc": ctx.executable._flatc.path,
            "codegen_sources": [f.path for f in tool_sources],
            "codegen_version": tool_identity["codegen_version"],
            "flatbuffers_version": tool_identity["flatbuffers_version"],
        },
        "entries": entries.values(),
        "artifacts": artifacts,
        "rust_crates": rust_crates,
        "sdk_crate_owners": {"aorta_core": str(Label("//core:aorta_core")), "aorta_sdk": str(Label("//rust:aorta_sdk"))},
        "licenses": licenses,
        "implicit_sources": implicit,
    }))
    args = ctx.actions.args()
    args.add_all(["--plan", plan.path, "--registry", ctx.file.registry.path, "--output", tree.path])
    ctx.actions.run(
        executable = ctx.executable._tool,
        env = {"PYTHONDONTWRITEBYTECODE": "1"},
        inputs = depset([plan, ctx.file.registry, ctx.executable._flatc] + tool_sources + inputs),
        outputs = [tree],
        arguments = [args],
        mnemonic = "SchemaReleaseStage",
    )
    return [DefaultInfo(files = depset([tree])), AortaSchemaReleaseInfo(version = 1, tree = tree, identity = identity)]

aorta_schema_release = rule(
    implementation = _release_impl,
    attrs = {
        "registry": attr.label(mandatory = True, allow_single_file = [".json"]),
        "fragments": attr.label_list(mandatory = True, providers = [AortaSchemaFragmentInfo], aspects = [schema_output_provenance]),
        "module_name": attr.string(mandatory = True),
        "module_revision": attr.string(mandatory = True),
        "aorta_revision": attr.string(mandatory = True),
        "licenses": attr.label_keyed_string_dict(mandatory = True, allow_files = True),
        "_reflection_schema": attr.label(default = Label("@flatbuffers//reflection:reflection_fbs_schema"), allow_files = [".fbs"]),
        "_flatc": attr.label(default = Label("//message/tools:flatc"), executable = True, cfg = "exec"),
        "_tool": attr.label(default = Label("//message/tools:schema_release_tool"), executable = True, cfg = "exec"),
    },
    doc = "Stage a module's canonical source closure and configured release outputs.",
)
