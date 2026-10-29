"""Builds one comparison BFBS for every OSS registry entry."""


def _label_path(label, package):
    if not label.startswith("//"):
        return package[2:] + "/" + label
    value = label[2:]
    parts = value.split(":")
    if len(parts) == 1:
        return value
    return parts[0] + "/" + parts[1]


def _spec(encoded):
    rows = []
    for key in sorted(encoded):
        pkg, stem = key.split("|")
        values = encoded[key]
        rows.append({
            "action_schema": values[1] == "1",
            "dep_fbs": values[2:],
            "pkg": pkg,
            "root_fbs": values[0],
            "stem": stem,
        })
    return json.encode_indent({"entries": rows, "version": 1}) + "\n"


def _oss_comparison_bfbs_impl(ctx):
    output = ctx.actions.declare_directory(ctx.label.name)
    spec = ctx.actions.declare_file(ctx.label.name + "_inputs/spec.json")
    ctx.actions.write(spec, _spec(ctx.attr.registry_entries))
    args = ctx.actions.args()
    args.add("--flatc", ctx.executable._flatc.path)
    args.add("--action-wrapper", ctx.executable._action_wrapper.path)
    args.add("--spec", spec.path)
    args.add("--output", output.path)
    args.add_all(ctx.files.schemas, before_each = "--input")
    ctx.actions.run(
        executable = ctx.executable._builder,
        env = {"PYTHONDONTWRITEBYTECODE": "1"},
        inputs = depset([spec], transitive = [ctx.attr.schemas[DefaultInfo].files]),
        tools = [ctx.executable._action_wrapper, ctx.executable._flatc],
        outputs = [output],
        arguments = [args],
        mnemonic = "AortaOssComparisonBfbs",
        progress_message = "Building OSS comparison BFBS for %s" % ctx.label,
    )
    return [DefaultInfo(files = depset([output]))]


_oss_comparison_bfbs = rule(
    implementation = _oss_comparison_bfbs_impl,
    attrs = {
        "registry_entries": attr.string_list_dict(mandatory = True),
        "schemas": attr.label(mandatory = True),
        "_builder": attr.label(
            default = "//message/tools:build_oss_comparison_bfbs",
            executable = True,
            cfg = "exec",
        ),
        "_action_wrapper": attr.label(
            default = "//message/tools:gen_action_wrapper",
            executable = True,
            cfg = "exec",
        ),
        "_flatc": attr.label(
            default = "//message/tools:flatc",
            executable = True,
            cfg = "exec",
        ),
    },
)


def aorta_oss_comparison_bfbs(name, registry_entries, schemas, **kwargs):
    encoded = {}
    for item in registry_entries:
        encoded[item.pkg + "|" + item.stem] = [
            item.root_fbs,
            "1" if getattr(item, "action_schema", False) else "0",
        ] + [
            _label_path(value, item.pkg)
            for value in getattr(item, "dep_fbs", [])
        ]
    _oss_comparison_bfbs(
        name = name,
        registry_entries = encoded,
        schemas = schemas,
        **kwargs
    )
