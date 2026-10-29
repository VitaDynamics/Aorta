load(":python_outputs.bzl", "PythonDirectOutputsInfo")
load(":python_distribution.bzl", "python_distribution_tree")
load("//message/tools:schema_names.bzl", "schema_meta_py_module")

def _flatbuffers_python_impl(ctx):
    out_dir = ctx.actions.declare_directory(ctx.label.name)
    args = ctx.actions.args()
    python_options = ["--python", "--gen-object-api"]
    args.add_all(python_options)
    # Emit the object API (`<Type>T` with Pack/InitFromBuf/InitFromObj) too. It is
    # additive (the reader class + builders are unchanged) and is what lets the
    # generic aorta.action.FlatbuffersActionCodec splice a pre-serialized domain
    # message as a real NESTED TABLE in the action wrapper (cross-language-correct).
    args.add("-o")
    args.add(out_dir.path)
    include_dirs = []
    seen = {}

    # `-I` flags are
    # a DIRECT-dependency-derived concern, kept separate from `srcs` (which
    # carries the full transitive closure for hermetic action inputs — flatc
    # needs every grandchild file materialized, but not every grandchild
    # DIRECTORY as an explicit search root; each direct dep's own package
    # already gets one, and that package's own `-I` reaches its includes
    # recursively). `direct_srcs` defaults to `srcs` for callers that don't
    # pass it, preserving prior behavior.
    _include_srcs = ctx.files.direct_srcs if ctx.attr.direct_srcs else ctx.files.srcs

    for src in _include_srcs:
        if src.dirname not in seen:
            seen[src.dirname] = True
            include_dirs.append(src.dirname)

    for inc in ctx.attr.include_dirs:
        if inc == "message":
            for src in _include_srcs:
                if "/schemas/" in src.path:
                    resolved = src.path.split("/schemas/", 1)[0]
                elif src.path.startswith("schemas/"):
                    resolved = "."
                else:
                    continue
                if resolved in seen:
                    continue
                seen[resolved] = True
                include_dirs.append(resolved)
            continue

        if inc in seen:
            continue
        seen[inc] = True
        include_dirs.append(inc)

    for inc in include_dirs:
        args.add("-I")
        args.add(inc)
    for src in ctx.files.srcs:
        args.add(src.path)

    ctx.actions.run(
        executable = ctx.executable._flatc,
        inputs = ctx.files.srcs,
        outputs = [out_dir],
        arguments = [args],
        mnemonic = "FlatcPython",
    )

    fastpath = None
    if ctx.file.bfbs:
        fastpath = ctx.actions.declare_directory(ctx.label.name + ".fastpath")
        ctx.actions.run(
            executable = ctx.executable.fastpath_generator,
            env = {"PYTHONDONTWRITEBYTECODE": "1"},
            inputs = [ctx.file.bfbs, out_dir],
            outputs = [fastpath],
            arguments = ["--bfbs", ctx.file.bfbs.path, "--getters", out_dir.path,
                         "--stem", ctx.attr.stem, "--out", fastpath.path],
            mnemonic = "PythonFlatbuffersFastpath",
        )

    providers = [DefaultInfo(files = depset([out_dir]))]
    if ctx.file.root_src:
        direct = ctx.actions.declare_directory(ctx.label.name + ".direct")
        direct_args = ctx.actions.args()
        direct_args.add_all(python_options)
        direct_args.add_all(["-o", direct.path])
        for inc in include_dirs:
            direct_args.add_all(["-I", inc])
        direct_args.add(ctx.file.root_src.path)
        ctx.actions.run(
            executable = ctx.executable._flatc,
            inputs = ctx.files.srcs,
            outputs = [direct],
            arguments = [direct_args],
            mnemonic = "FlatcPythonDirect",
        )
        providers.append(PythonDirectOutputsInfo(tree = direct, source = ctx.file.root_src, fastpath = fastpath))
    providers.append(OutputGroupInfo(fastpath = depset([fastpath] if fastpath else [])))
    return providers


flatbuffers_python = rule(
    implementation = _flatbuffers_python_impl,
    attrs = {
        "bfbs": attr.label(allow_single_file = [".bfbs"]),
        "stem": attr.string(),
        "fastpath_generator": attr.label(executable = True, cfg = "exec"),
        "root_src": attr.label(allow_single_file = [".fbs"]),
        "srcs": attr.label_list(allow_files = [".fbs"]),
        # Optional DIRECT-deps-only source list driving `-I` flag derivation
        # (see the impl comment above). Omit to derive `-I` from every entry
        # in `srcs` (prior behavior; used by no in-repo caller after Phase 1).
        "direct_srcs": attr.label_list(allow_files = [".fbs"]),
        "include_dirs": attr.string_list(),
        "_flatc": attr.label(
            default = Label("//message/tools:flatc"),
            executable = True,
            cfg = "exec",
        ),
    },
    doc = "Generate Python FlatBuffers sources into a tree artifact.",
)


# SDK subpackages under `aorta/` whose name is also a schema namespace root
# (`namespace aorta.<name>;`) in ANY module that generates its Python tree with
# this rule — aorta's own registry and the product schema module alike, since
# both trees are installed next to the same `aorta_sdk`. `capture` is the
# product module's `aorta.capture` namespace; it stays listed even though no
# schema in this repository uses it. python/tests/test_architecture.py checks
# that every name here is a real SDK package and that every collision visible
# in this repository is listed.
_SDK_OWNED_SUBPACKAGES = [
    "action",
    "capture",
]


def _python_msgs_tree_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)
    cmds = ['mkdir -p "{0}"'.format(out.path)]
    for tree in ctx.files.trees:
        # Copied files inherit Bazel's read-only perms. Distinct schema trees
        # share the generated `aorta/sys/*` header modules (every schema with an
        # aorta_header re-emits them, byte-identical), so a later tree overwrites
        # files an earlier one already wrote — chmod the dest writable after each
        # copy or the overwrite hits EACCES.
        cmds.append('cp -R "{0}/." "{1}/"'.format(tree.path, out.path))
        cmds.append('chmod -R u+w "{0}"'.format(out.path))
    installed_metas = {}
    for meta in ctx.files.metas:
        basename = meta.basename
        if basename.endswith("_schema_meta.py"):
            basename = schema_meta_py_module({
                "pkg": "//" + meta.owner.package,
                "stem": basename[:-len("_schema_meta.py")],
            })
        previous = installed_metas.get(basename)
        if previous != None and previous.path != meta.path:
            fail("duplicate schema-meta install basename '{}': '{}' and '{}'".format(
                basename, previous.path, meta.path,
            ))
        installed_metas[basename] = meta
    for basename, meta in installed_metas.items():
        destination = out.path + "/" + basename
        # A generated message tree must not silently replace a metadata slot.
        cmds.append('test ! -e "{0}" || {{ echo "schema-meta install path already exists: {0}" >&2; exit 1; }}'.format(destination))
        cmds.append('cp "{0}" "{1}"'.format(meta.path, destination))
    # Ensure the whole tree is writable before deleting from it below (rm needs a
    # writable parent directory).
    cmds.append('chmod -R u+w "{0}"'.format(out.path))
    # The flatc tree emits an empty top-level `aorta/__init__.py` (from the
    # included aorta.sys.* header). In a wheel that would CLOBBER the SDK's real
    # `aorta/__init__.py` when both install into the same site-packages. Drop it
    # so `aorta` is a namespace the SDK owns (and a PEP 420 namespace when this
    # package installs standalone). `aorta/sys/__init__.py` is kept (the SDK
    # ships no `aorta/sys`).
    cmds.append('rm -f "{0}/aorta/__init__.py"'.format(out.path))

    # Same hazard one level down: schemas under `aorta.<name>` where `<name>` is
    # ALSO an SDK subpackage make flatc emit an empty `aorta/<name>/__init__.py`
    # that would clobber the SDK's real one. Drop those too, so the SDK owns
    # `aorta.<name>` and the generated `aorta/<name>/<domain>/` message modules
    # attach as SUBPACKAGES of it. Installed standalone, `aorta/<name>/` stays a
    # PEP 420 namespace, so `aorta.action.rcp` still imports without the SDK.
    #
    # Every SDK subpackage whose name is also a schema namespace root MUST be
    # listed here, and the SDK side MUST be a PACKAGE (`aorta/<name>/__init__.py`),
    # never a flat module (`aorta/<name>.py`): a flat module cannot host the
    # generated submodules, and a generated regular package would shadow it
    # outright. `python/tests/test_architecture.py` enforces both halves.
    # See message/README.md "Python Bindings".
    for pkg in _SDK_OWNED_SUBPACKAGES:
        cmds.append('rm -f "{0}/aorta/{1}/__init__.py"'.format(out.path, pkg))
    ctx.actions.run_shell(
        inputs = ctx.files.trees + ctx.files.metas,
        outputs = [out],
        command = " && ".join(cmds),
        mnemonic = "AortaMsgsTree",
    )
    return [DefaultInfo(files = depset([out]))]


_python_msgs_tree = rule(
    implementation = _python_msgs_tree_impl,
    attrs = {
        "trees": attr.label_list(
            allow_files = True,
            doc = "flatc python tree-artifacts (<stem>_py) to merge.",
        ),
        "metas": attr.label_list(
            allow_files = [".py"],
            doc = "schema-meta .py modules (<stem>_schema_meta_py) to include.",
        ),
    },
    doc = "Assemble generated message modules into one importable tree for an " +
          "external msgs wheel, dropping the conflicting top-level aorta/__init__.py.",
)


def python_msgs_tree(name, registry = None, fragments = [], module_name = "", **kwargs):
    """Assemble a distribution from direct owners, or a local development tree."""
    if registry != None:
        python_distribution_tree(name = name, registry = registry, fragments = fragments,
                                 module_name = module_name, **kwargs)
    else:
        _python_msgs_tree(name = name, **kwargs)
