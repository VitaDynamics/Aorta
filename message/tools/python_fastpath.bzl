def _python_fastpath_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)
    getters = ctx.files.getters
    if len(getters) != 1 or not getters[0].is_directory:
        fail("getters must be the matching flatbuffers_python tree")
    args = ctx.actions.args()
    args.add_all(["--bfbs", ctx.file.bfbs.path, "--getters", getters[0].path,
                  "--stem", ctx.attr.stem, "--out", out.path])
    ctx.actions.run(
        executable = ctx.executable._generator,
        env = {"PYTHONDONTWRITEBYTECODE": "1"},
        inputs = depset([ctx.file.bfbs] + getters),
        outputs = [out],
        arguments = [args],
        mnemonic = "PythonFlatbuffersFastpath",
    )
    return [DefaultInfo(files = depset([out]))]

python_fastpath = rule(
    implementation = _python_fastpath_impl,
    attrs = {
        "bfbs": attr.label(allow_single_file = [".bfbs"], mandatory = True),
        "getters": attr.label(mandatory = True),
        "stem": attr.string(mandatory = True),
        "_generator": attr.label(
            default = Label("//message/tools:gen_python_fastpath"),
            executable = True,
            cfg = "exec",
        ),
    },
)
