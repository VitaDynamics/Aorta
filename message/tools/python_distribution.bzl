"""Owner-exclusive distribution assembly, independent of frozen registry fields."""
load(":schema_fragment.bzl", "AortaSchemaFragmentInfo")
load(":schema_names.bzl", "schema_meta_py_module")
load(":python_outputs.bzl", "PythonDirectOutputsInfo")
load("@rules_python//python:packaging.bzl", "PyWheelInfo", "py_wheel")

PythonDistributionInfo = provider(fields = ["tree", "manifest"])

def _impl(ctx):
    rows = []
    inputs = [ctx.file.registry]
    for fragment in ctx.attr.fragments:
        info = fragment[AortaSchemaFragmentInfo]
        for entry in info.entries:
            if "python" not in entry["output_labels"]:
                continue
            target = info.output_targets[Label(entry["output_labels"]["python"])]
            direct = target[PythonDirectOutputsInfo]
            expected = info.source_files[Label(entry["fbs_label"])]
            if "action_schema" in entry["output_labels"]:
                expected = info.output_targets[Label(entry["output_labels"]["action_schema"])][DefaultInfo].files.to_list()[0]
            if direct.source != expected:
                fail("Python direct source owner mismatch: " + entry["fbs_label"])
            trees = target[DefaultInfo].files.to_list()
            row = {"schema": entry["fbs_label"], "module": entry["origin_module"],
                   "direct": direct.tree.path, "trees": [t.path for t in trees]}
            inputs.extend([direct.tree] + trees)
            if direct.fastpath:
                row["fastpath"] = direct.fastpath.path
                inputs.append(direct.fastpath)
            if "schema_meta_py" in entry["output_labels"]:
                meta = info.output_targets[Label(entry["output_labels"]["schema_meta_py"])][DefaultInfo].files.to_list()[0]
                row["meta"] = {"path": meta.path, "install": schema_meta_py_module(entry)}
                inputs.append(meta)
            rows.append(row)
    plan = ctx.actions.declare_file(ctx.label.name + ".plan.json")
    tree = ctx.actions.declare_directory(ctx.label.name)
    manifest = ctx.actions.declare_file(ctx.label.name + ".ownership.json")
    runtime = [{"path": f.path, "install": "aorta_msgs_runtime/" + f.basename}
               for f in ctx.files._runtime_sources]
    inputs.extend(ctx.files._runtime_sources)
    ctx.actions.write(plan, json.encode({"schemas": rows, "runtime": runtime}))
    ctx.actions.run(
        executable = ctx.executable._tool,
        env = {"PYTHONDONTWRITEBYTECODE": "1"},
        inputs = depset(inputs + [plan]),
        outputs = [tree, manifest],
        arguments = [plan.path, ctx.file.registry.path, ctx.attr.module_name, tree.path, manifest.path],
        mnemonic = "PythonDistributionOwnership",
    )
    return [DefaultInfo(files = depset([tree])),
            OutputGroupInfo(ownership = depset([manifest]), plan = depset([plan])),
            PythonDistributionInfo(tree = tree, manifest = manifest)]

python_distribution_tree = rule(
    implementation = _impl,
    attrs = {
        "registry": attr.label(allow_single_file = [".json"], mandatory = True),
        "fragments": attr.label_list(providers = [AortaSchemaFragmentInfo], mandatory = True),
        "module_name": attr.string(mandatory = True),
        "_runtime_sources": attr.label(default = Label("//message/runtime:python_fastpath_sources")),
        "_tool": attr.label(default = Label("//message/tools:python_distribution"), executable = True, cfg = "exec"),
    },
)

def schema_python_wheel(name, registry, fragments, module_name, distribution, version, tested_aorta_version, requires_file = None, **kwargs):
    """Build a wheel with literal pins or a complete generated requirements file."""
    tree = name + "_tree"
    python_distribution_tree(name = tree, registry = registry, fragments = fragments, module_name = module_name)
    py_wheel(
        name = name,
        distribution = distribution,
        version = version,
        python_tag = "py3",
        python_requires = ">=3.10",
        requires = ["flatbuffers==25.12.19", "aorta-msgs == " + tested_aorta_version, "aorta-sdk == " + tested_aorta_version] if requires_file == None else [],
        requires_file = requires_file,
        strip_path_prefixes = [(native.package_name() + "/" if native.package_name() else "") + tree],
        deps = [":" + tree],
        **kwargs
    )

def _sdk_manifest_impl(ctx):
    files = {}
    for file in ctx.files.package:
        path = file.short_path
        if path.startswith("../"):
            path = path.split("/", 2)[2]
        if not path.startswith("python/"):
            fail("SDK package file outside python/: " + path)
        files[path[len("python/"):]] = {"module": "aorta-sdk"}
    out = ctx.actions.declare_file(ctx.label.name + ".json")
    ctx.actions.write(out, json.encode({"version": 1, "module": "aorta-sdk", "files": files}))
    return [DefaultInfo(files = depset([out]))]

sdk_python_ownership = rule(
    implementation = _sdk_manifest_impl,
    attrs = {"package": attr.label(mandatory = True)},
)

def _license_files_impl(ctx):
    # py_wheel names its output after the distribution tags, so the finished
    # wheel keeps that exact basename in this package; the payload-only stage
    # wheel must therefore live in another package (see //python/stage).
    stage = ctx.attr.wheel[PyWheelInfo]
    out = ctx.actions.declare_file(stage.wheel.basename)
    args = ctx.actions.args()
    args.add("licenses")
    args.add(stage.wheel)
    args.add(out)
    args.add(ctx.attr.license_expression)
    inputs = [stage.wheel]
    seen = {}
    for target, name in ctx.attr.license_files.items():
        files = target[DefaultInfo].files.to_list()
        if len(files) != 1:
            fail("license_files entry %s must be exactly one file" % name)
        if name in seen:
            fail("license_files declares %s twice" % name)
        seen[name] = True
        args.add(name + "=" + files[0].path)
        inputs.append(files[0])
    if not seen:
        fail("license_files must name at least the distribution LICENSE")
    ctx.actions.run(
        executable = ctx.executable._tool,
        env = {"PYTHONDONTWRITEBYTECODE": "1"},
        inputs = inputs,
        outputs = [out],
        arguments = [args],
        mnemonic = "PythonWheelLicenses",
        progress_message = "Adding license files to %s" % out.short_path,
    )
    return [DefaultInfo(files = depset([out]), runfiles = ctx.runfiles(files = [out])),
            PyWheelInfo(wheel = out, name_file = stage.name_file)]

python_wheel_licenses = rule(
    doc = """Finish a py_wheel with PEP 639 license files.

Each `license_files` entry lands at `<dist>-<ver>.dist-info/licenses/<name>`
and is listed as a `License-File` metadata entry next to `License-Expression`,
so the files install through the standard channel and the ownership verifier
treats them as distribution-owned metadata rather than schema-owned payload.""",
    implementation = _license_files_impl,
    attrs = {
        "wheel": attr.label(providers = [PyWheelInfo], mandatory = True),
        "license_files": attr.label_keyed_string_dict(allow_files = True, mandatory = True),
        "license_expression": attr.string(default = "Apache-2.0"),
        "_tool": attr.label(default = Label("//message/tools:python_distribution"), executable = True, cfg = "exec"),
    },
)

def _notices_impl(ctx):
    out = ctx.actions.declare_file(ctx.attr.out)
    ctx.actions.run(
        executable = ctx.executable._tool,
        env = {"PYTHONDONTWRITEBYTECODE": "1"},
        inputs = [ctx.file.ownership, ctx.file.upstream_license],
        outputs = [out],
        arguments = ["notices", ctx.file.ownership.path, ctx.attr.distribution, ctx.file.upstream_license.path, out.path],
        mnemonic = "PythonDistributionNotices",
    )
    return [DefaultInfo(files = depset([out]))]

python_distribution_notices = rule(
    doc = """Third-party notices for a generated-schema wheel, derived from its ownership manifest.

Names the installed modules generated from foxglove-sdk derived schemas and
reproduces the upstream MIT text, so the notice tracks the composed registry
instead of a hand-maintained list.""",
    implementation = _notices_impl,
    attrs = {
        "ownership": attr.label(allow_single_file = [".json"], mandatory = True),
        "distribution": attr.string(mandatory = True),
        "upstream_license": attr.label(allow_single_file = True, mandatory = True),
        "out": attr.string(mandatory = True),
        "_tool": attr.label(default = Label("//message/tools:python_distribution"), executable = True, cfg = "exec"),
    },
)
