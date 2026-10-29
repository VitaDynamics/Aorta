"""Macros for message binding targets (Rust/C++) and related aliases."""

load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_rust//rust:defs.bzl", "rust_library")

# Anchored with `Label()` so they resolve through AORTA's repo mapping when
# this file is loaded by an external module's package (external schema
# roots). `@aorta_crates` is Aorta's PRIVATE
# crate-universe repo, so it must go through the //message/tools alias.
_DEFAULT_RUST_DEPS = [
    Label("//core:aorta_core"),
    Label("//message/tools:flatbuffers_rust"),
]
_MESSAGE_ROOT_PREFIX = "//message/"
_GEN_MESSAGE_WRAPPER = Label("//message/tools:gen_message_wrapper")

# Every generated Rust wrapper opens with `pub mod aorta_base_generated;`
# unconditionally (gen_message_wrapper.write_rust), so the base module has to
# be in the crate whether or not the schema declares `aorta_base.fbs` as a
# direct edge. Anchored with `Label()` for the same reason as the deps above:
# an external schema root expands this macro inside an EXTERNAL module's
# package, where a
# bare `//message/...` string would resolve against THAT module's root.
_AORTA_BASE_RUST = Label("//message/schemas/system:aorta_base_rust")

# Providing target for cpp/include/aorta/*.h (array_view.h, typed_subscribe.h,
# typed_publish.h, ...). Generated wrapper
# headers `#include "aorta/array_view.h"` AND
# `#include "aorta/typed_subscribe.h"`, so the wrapper cc_library needs this
# as a dep for the header search path to resolve regardless of what a
# downstream consumer happens to depend on. Mirrors the Tier-2 external-Bzlmod
# wrapper macro (`_AORTA_CPP_HEADERS` in message/tools/aorta_flatbuffers.bzl),
# which already wires this; Tier-1 (in-repo, this file) previously did not --
# every existing consumer just happened to also depend on `:aorta_cpp_headers`
# directly (e.g. cpp/BUILD.bazel's `publisher_test`), which is no longer
# required but is harmless to keep.
_AORTA_CPP_HEADERS = Label("//cpp:aorta_cpp_headers")


def _schema_rel_path(schema_pkg, schema_rel = None):
    """`--schema-rel` for gen_message_wrapper.

    In-repo (Tier-1) callers derive it from the `//message/`-rooted schema
    package. An EXTERNAL schema root has no `//message/` ancestor at all, so
    it passes `schema_rel` EXPLICITLY — `""`
    for a plain message/service package, since the only thing the C++ emitter
    reads it for is the in-repo `schemas/action/` action-name convention,
    which an external root replaces with an explicit `action_name`.
    """
    if schema_rel != None:
        if schema_rel.startswith("/") or ".." in schema_rel.split("/"):
            fail("schema_rel must be a relative path without '..', got '{}'".format(schema_rel))
        return schema_rel
    if not schema_pkg.startswith(_MESSAGE_ROOT_PREFIX):
        fail(
            "schema_pkg must start with '{}', got '{}'".format(
                _MESSAGE_ROOT_PREFIX,
                schema_pkg,
            ),
        )
    return schema_pkg[len(_MESSAGE_ROOT_PREFIX):]


def _extract_include_paths(fbs_label):
    # `native.read` does not exist in any shipped Bazel release (it was never
    # promoted out of an early proposal) — this branch always fires, and
    # `struct(parsed = False)` is the ONLY value this function has ever
    # returned in practice. It is kept (rather than deleted) purely because
    # `_collect_rust_include_targets` below still calls it as the
    # `rust_module_targets` auto-detect attempt (a separate, out-of-scope
    # parameter — the registry always passes `rust_module_targets`
    # explicitly, so that fallback is likewise never exercised in practice).
    # The `schema_deps` requirement — the property this helper was
    # ORIGINALLY written for — no longer calls this function at all; see
    # `_genrule_schema_srcs`, which now requires `schema_deps` explicitly.
    read_fn = getattr(native, "read", None)
    if read_fn == None:
        return struct(paths = [], parsed = False)

    if not fbs_label.startswith("//"):
        return struct(paths = [], parsed = False)
    pos = fbs_label.find(":")
    if pos < 0:
        return struct(paths = [], parsed = False)
    pkg = fbs_label[2:pos]
    file_name = fbs_label[pos + 1:]
    text = read_fn(pkg + "/" + file_name)
    includes = []
    marker = 'include "'
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line.startswith(marker):
            continue
        rest = line[len(marker):]
        end = rest.find('"')
        if end <= 0:
            continue
        include_path = rest[:end].strip()
        if include_path:
            includes.append(include_path)
    return struct(paths = includes, parsed = True)


def _package_from_label(label):
    pos = label.find(":")
    if pos < 0:
        fail("expected label with ':', got '{}'".format(label))
    return label[:pos]


def _sanitize_target_name(text):
    result = []
    for i in range(len(text)):
        ch = text[i]
        if (
                ("a" <= ch and ch <= "z") or
                ("A" <= ch and ch <= "Z") or
                ("0" <= ch and ch <= "9")):
            result.append(ch)
        else:
            result.append("_")
    return "".join(result)


def _target_name_from_label(label):
    pos = label.rfind(":")
    if pos < 0:
        fail("expected label with ':', got '{}'".format(label))
    return label[pos + 1:]


def _single_output_name_for_target(label):
    target = _target_name_from_label(label)
    if target.endswith("_rust"):
        return target[:-5] + "_generated.rs"
    if target.endswith("_schema_meta"):
        return target[:-12] + "_schema_meta.rs"
    if target.endswith("_bfbs"):
        return target[:-5] + ".bfbs"
    fail("unsupported generated target label '{}'".format(label))


def _rust_crate_local_modules_impl(ctx):
    """Copy every generated `.rs` in a closure next to the wrapper source.

    The Rust wrapper addresses its modules RELATIVELY (`#[path =
    "<prefix><basename>"]`), so each generated module has to exist as a file
    beside the wrapper — which is what `_copy_generated_file` does one label at
    a time. It cannot do a FILEGROUP: a genrule must name its `outs` at
    analysis time, and the whole point of `<stem>_rust_closure` is that the
    caller does NOT know how many files (or whose) are in it. A rule can, since
    it sees the resolved `File`s.

    Collision policy: two DIFFERENT files whose basenames
    coincide would map onto one crate-local slot. That is a real possibility
    once two modules' schemas meet in one closure — `(pkg, stem)` is the
    registry key, so the same stem may legitimately exist in two packages — and
    silently keeping the first (or the last) would compile a crate against the
    WRONG schema. It is a hard analysis-time error instead, naming both
    sources.
    """
    outs = []
    origin = {}
    for src in ctx.files.srcs:
        out_name = ctx.attr.prefix + src.basename
        previous = origin.get(out_name)
        if previous != None:
            if previous != src.path:
                fail(
                    ("crate-local Rust module collision on '{}': '{}' and '{}' are " +
                     "different files with the same basename, so they would occupy " +
                     "the same slot next to the wrapper source. Rename one schema " +
                     "stem (last-writer-wins would silently compile against the " +
                     "wrong schema).").format(out_name, previous, src.path),
                )
            continue
        origin[out_name] = src.path
        out = ctx.actions.declare_file(out_name)
        ctx.actions.symlink(output = out, target_file = src)
        outs.append(out)
    return [DefaultInfo(files = depset(outs))]


aorta_rust_crate_local_modules = rule(
    implementation = _rust_crate_local_modules_impl,
    doc = """Materialize a transitive Rust module closure as crate-local sources.

PRIVATE implementation detail of `aorta_rust_message_binding`'s
`rust_module_filegroups` path.""",
    attrs = {
        "srcs": attr.label_list(
            mandatory = True,
            allow_files = [".rs"],
            doc = "`_rust_closure` filegroups (and/or single `_rust` outputs).",
        ),
        "prefix": attr.string(
            mandatory = True,
            doc = "Crate-local filename prefix, matching `--rust-local-module-prefix`.",
        ),
    },
)


def _copy_generated_file(name, src, out):
    target_name = "{}__copy_{}".format(
        name,
        _sanitize_target_name(out),
    )
    native.genrule(
        name = target_name,
        srcs = [src],
        outs = [out],
        cmd = "mkdir -p $(@D) && cp $(location {}) $@".format(src),
    )
    return ":" + target_name


def _resolve_include_fbs_label(schema_pkg, include_path):
    if include_path.startswith("schemas/"):
        slash = include_path.rfind("/")
        if slash < 0:
            fail("invalid include path '{}'".format(include_path))
        pkg_rel = include_path[:slash]
        file_name = include_path[slash + 1:]
        return "//message/{}:{}".format(pkg_rel, file_name)

    if include_path.startswith("//"):
        return include_path
    if include_path.startswith(":"):
        return schema_pkg + include_path
    if include_path.startswith("../") or include_path.startswith("./"):
        fail(
            "relative include '{}' is not supported in message binding macro; use includes rooted at 'schemas/' or package-local file names.".format(
                include_path,
            ),
        )

    return "{}:{}".format(schema_pkg, include_path)


def _to_rust_target_label(fbs_label):
    if not fbs_label.endswith(".fbs"):
        fail("expected .fbs label, got '{}'".format(fbs_label))
    return fbs_label[:-4] + "_rust"


def _genrule_schema_srcs(schema_pkg, root_fbs, schema_deps):
    """Resolve the include-closure `.fbs` labels for a wrapper genrule.

    `schema_deps` is REQUIRED —
    the previous "auto-detect via `native.read`" fallback tier was dead code
    (`native.read` does not exist in any shipped Bazel release; the
    early-return in `_extract_include_paths` always fired), which silently
    degraded to a root-only `srcs` list whenever a caller omitted
    `schema_deps` on a schema with includes. That is exactly the class of
    bug the explicit closure exists to close (undeclared transitive `.fbs` inputs →
    stale generated wrapper, or a stale remote-cache hit, when only an
    included schema changes). Callers must now pass the explicit transitive
    `.fbs` closure — `[]` for a genuinely root-only schema with no includes.
    """
    if schema_deps == None:
        fail((
            "message_bindings.bzl: '{}' (root_fbs = '{}') must pass an explicit " +
            "`schema_deps` — the list of `.fbs` labels transitively included by " +
            "`root_fbs`, or `[]` for a schema with no includes. Auto-detection was " +
            "removed (it silently degraded to a root-only srcs list)."
        ).format(schema_pkg, root_fbs))
    return list(schema_deps)


def _collect_rust_include_targets(schema_pkg, fbs_label, visited):
    parsed = _extract_include_paths(fbs_label)
    if not parsed.parsed:
        return struct(targets = [], parsed = False)

    targets = []
    for include_path in parsed.paths:
        include_fbs = _resolve_include_fbs_label(schema_pkg, include_path)
        if include_fbs in visited:
            continue
        visited[include_fbs] = True

        targets.append(_to_rust_target_label(include_fbs))
        child = _collect_rust_include_targets(
            _package_from_label(include_fbs),
            include_fbs,
            visited,
        )
        if not child.parsed:
            return struct(targets = [], parsed = False)
        targets.extend(child.targets)

    deduped = []
    seen = {}
    for target in targets:
        if target in seen:
            continue
        seen[target] = True
        deduped.append(target)
    return struct(targets = deduped, parsed = True)


def aorta_rust_message_binding(
        name,
        schema_pkg,
        schema_stem,
        crate_name = None,
        include_schema_meta = True,
        root_fbs_label = None,
        include_graph_root_fbs_label = None,
        rust_module_targets = None,
        rust_module_filegroups = None,
        deps = None,
        extra_compile_data = None,
        schema_deps = None,
        schema_dep_filegroups = None,
        schema_rel = None,
        action_name = None,
        visibility = None):
    """Defines a Rust message binding library with auto-generated wrapper source.

    Args:
      name: rust_library target name.
      schema_pkg: schema package label prefix (for example,
          "//message/schemas/topic/example").
      schema_stem: schema stem (for example, "demo_chat", "echo").
      crate_name: rust crate name used by `use ...::...`; defaults to `name`.
      include_schema_meta: whether to include `<stem>_schema_meta` in compile_data.
      deps: optional extra rust deps list; appended after default deps.
      extra_compile_data: optional extra files/targets to add into compile_data.
      schema_deps: REQUIRED (no default: pass `[]` for a root-only schema
          with no includes) explicit list of `.fbs` labels transitively
          included by `root_fbs`, so wrapper action keys depend on
          included-schema content — otherwise a grandchild-include edit
          would neither re-run the action locally nor invalidate a remote
          cache hit. `fail()`s if omitted; there is no auto-detection (the
          `native.read`-based fallback this used to have was dead code —
          `native.read` does not exist in any shipped Bazel — and it
          silently degraded to a root-only srcs list).
      rust_module_filegroups: alternative to `rust_module_targets`
          for EXTERNAL schema roots: `<stem>_rust_closure` FILEGROUP labels
          carrying the schema's transitive generated Rust modules. An external
          root cannot enumerate another module's transitive schema set as
          individual labels — the closure filegroup is the only
          cross-module-correct spelling — so the crate-local copies are made by
          a rule (`aorta_rust_crate_local_modules`) instead of one genrule per
          label. Mutually exclusive with `rust_module_targets`.
      schema_dep_filegroups: alternative to `schema_deps`, exactly
          as on `aorta_cpp_message_wrapper`: `_fbs_closure` filegroups expanded
          with `$(locations ...)` (plural), serving as BOTH the declared action
          inputs and the `--include-fbs` resolution manifest.
      schema_rel: explicit `--schema-rel` (external roots; see
          `_schema_rel_path`). `""` for an external root, whose package has no
          `//message/` ancestor to derive one from.
      action_name: explicit action routing name, forcing the aggregate
          `<Prefix>Action` / `ActionSchema` / `ActionWire` emission for an
          EXTERNAL action root. Mirrors `aorta_cpp_message_wrapper`'s parameter
          of the same name and exists for the same reason: the Tier-1 name is
          inferred from the in-repo `schemas/action/<pkg>` path, which an
          external root does not have. Without it the wrapper would silently
          emit no action surface at all.
      visibility: visibility of the generated `rust_library`.

    Note: the wrapper genrule always passes `--root-bfbs=$(location
    {schema_pkg}:{schema_stem}_bfbs)` (the
    compiled bfbs is the wrapper emitters' only semantic source, so every
    schema item with a wrapper MUST also have a `{stem}_bfbs` target — true
    of every current registry entry with `rust_wrapper`/`cpp_wrapper` = True;
    an item without one is a Bazel analysis-time error [missing label], not
    a silent degradation to a text-parsing fallback, which no longer
    exists).
    """
    schema_rel = _schema_rel_path(schema_pkg, schema_rel)
    wrapper_src = "{}.rs".format(name)
    wrapper_gen = "{}_wrapper_src".format(name)
    if root_fbs_label == None:
        root_fbs = "{}:{}.fbs".format(schema_pkg, schema_stem)
    else:
        root_fbs = root_fbs_label
    if include_graph_root_fbs_label == None:
        include_graph_root_fbs_label = "{}:{}.fbs".format(schema_pkg, schema_stem)

    local_module_prefix = None
    local_module_srcs = []
    local_compile_data = []

    if rust_module_filegroups != None and rust_module_targets != None:
        fail(
            "aorta_rust_message_binding '{}': pass rust_module_targets OR ".format(name) +
            "rust_module_filegroups, not both",
        )

    if rust_module_filegroups != None:
        # External schema roots. Same emission path as Tier-1 —
        # `--rust-local-module-prefix` plus crate-local copies — but the copies
        # come from CLOSURE FILEGROUPS, because an external root cannot name
        # another module's transitive schema set label by label.
        local_module_prefix = name + "__"
        # `aorta_base_generated` is emitted unconditionally by the wrapper, so
        # the macro anchors it here rather than trusting every caller to
        # remember a module they never asked for.
        copy_srcs = list(rust_module_filegroups) + [_AORTA_BASE_RUST]
        if include_schema_meta:
            copy_srcs.append("{}:{}_schema_meta".format(schema_pkg, schema_stem))
        modules_target = "{}__crate_local_modules".format(name)
        aorta_rust_crate_local_modules(
            name = modules_target,
            prefix = local_module_prefix,
            srcs = copy_srcs,
        )
        local_module_srcs.append(":" + modules_target)
        local_compile_data.append(
            _copy_generated_file(
                name,
                "{}:{}_bfbs".format(schema_pkg, schema_stem),
                local_module_prefix + schema_stem + ".bfbs",
            ),
        )
        uses_local_modules = True
    else:
        include_targets = struct(targets = [], parsed = False)
        module_labels = None
        if rust_module_targets != None:
            module_labels = []
            for label in rust_module_targets:
                module_labels.append(label)
        if module_labels == None:
            include_targets = _collect_rust_include_targets(
                schema_pkg = schema_pkg,
                fbs_label = include_graph_root_fbs_label,
                visited = {},
            )
            if include_targets.parsed:
                module_labels = [
                    "//message/schemas/system:aorta_base_rust",
                    "{}:{}_rust".format(schema_pkg, schema_stem),
                ] + include_targets.targets

        uses_local_modules = module_labels != None
        if module_labels != None:
            local_module_prefix = name + "__"
            if include_schema_meta:
                module_labels.append("{}:{}_schema_meta".format(schema_pkg, schema_stem))

            seen_module_outs = {}
            for src in module_labels:
                out_name = _single_output_name_for_target(src)
                previous = seen_module_outs.get(out_name)
                if previous != None:
                    if previous != src:
                        # Same crate-local slot, different
                        # source. Keeping the first would compile the crate
                        # against the WRONG schema.
                        fail(
                            ("aorta_rust_message_binding '{}': crate-local Rust module " +
                             "collision on '{}' — '{}' and '{}' are different targets " +
                             "with the same generated basename").format(
                                name,
                                out_name,
                                previous,
                                src,
                            ),
                        )
                    continue
                seen_module_outs[out_name] = src
                local_module_srcs.append(
                    _copy_generated_file(
                        name,
                        src,
                        local_module_prefix + out_name,
                    ),
                )

            local_compile_data.append(
                _copy_generated_file(
                    name,
                    "{}:{}_bfbs".format(schema_pkg, schema_stem),
                    local_module_prefix + schema_stem + ".bfbs",
                ),
            )

    if schema_dep_filegroups != None:
        if schema_deps != None:
            fail(
                "aorta_rust_message_binding '{}': pass schema_deps OR ".format(name) +
                "schema_dep_filegroups, not both",
            )
        resolved_schema_deps = []
        closure_filegroups = list(schema_dep_filegroups)
    else:
        resolved_schema_deps = _genrule_schema_srcs(schema_pkg, root_fbs, schema_deps)
        closure_filegroups = []

    cmd_parts = [
        "$(location {})".format(_GEN_MESSAGE_WRAPPER),
        "--lang=rust",
        "--stem={}".format(schema_stem),
        "--schema-rel={}".format(schema_rel),
        "--root-fbs=$(location {})".format(root_fbs),
    ]
    if include_schema_meta:
        cmd_parts.append("--with-schema-meta")
    if local_module_prefix != None:
        cmd_parts.append("--rust-local-module-prefix={}".format(local_module_prefix))

    # Pass the declared transitive `.fbs` closure as an explicit manifest so
    # `resolve_schema_include_path` can resolve an include by exact
    # path-suffix match instead of directory-guessing — see the matching
    # comment in aorta_flatbuffers.bzl (Tier-2) for the full rationale.
    # `resolved_schema_deps` entries here are individual single-file `.fbs`
    # labels (not filegroups), so `$(location dep)` (singular) is correct.
    if resolved_schema_deps or closure_filegroups:
        cmd_parts.append("--include-fbs")
        for _dep in resolved_schema_deps:
            cmd_parts.append("$(location {})".format(_dep))
        for _group in closure_filegroups:
            cmd_parts.append("$(locations {})".format(_group))

    # The compiled bfbs is the wrapper emitters'
    # ONLY semantic source (--root-bfbs is required by gen_message_wrapper.py
    # itself) — always wired, unconditionally, from this item's own
    # `{schema_pkg}:{schema_stem}_bfbs` target. This also makes the wrapper
    # action closure-hermetic by construction (its facts come from the bfbs,
    # whose transitive closure Bazel already declares via `schema_deps`).
    bfbs_target = "{}:{}_bfbs".format(schema_pkg, schema_stem)
    cmd_parts.append("--root-bfbs=$(location {})".format(bfbs_target))
    if action_name != None:
        if not action_name:
            fail("aorta_rust_message_binding '{}': action_name must be non-empty".format(name))
        cmd_parts.append("--action-name={}".format(action_name))
    cmd_parts.append("--out=$@")

    native.genrule(
        name = wrapper_gen,
        srcs = [root_fbs] + resolved_schema_deps + closure_filegroups + [bfbs_target],
        outs = [wrapper_src],
        tools = [_GEN_MESSAGE_WRAPPER],
        cmd = " ".join(cmd_parts),
    )

    module_srcs = list(local_module_srcs)
    if not uses_local_modules:
        module_srcs = [
            "//message/schemas/system:aorta_base_rust",
        ]
        # Fallback path when native.read is unavailable in this Bazel version.
        module_srcs.append("{}:{}_rust_all".format(schema_pkg, schema_stem))

    compile_data = list(local_compile_data)
    if not uses_local_modules:
        compile_data.append("{}:{}_bfbs".format(schema_pkg, schema_stem))
    if include_schema_meta and not uses_local_modules:
        module_srcs.append("{}:{}_schema_meta".format(schema_pkg, schema_stem))
    if extra_compile_data != None:
        compile_data.extend(extra_compile_data)

    dedup_module_srcs = []
    seen_module_srcs = {}
    for item in module_srcs:
        if item in seen_module_srcs:
            continue
        seen_module_srcs[item] = True
        dedup_module_srcs.append(item)
    module_srcs = dedup_module_srcs

    dedup_compile_data = []
    seen_compile_data = {}
    for item in compile_data:
        if item in seen_compile_data:
            continue
        seen_compile_data[item] = True
        dedup_compile_data.append(item)
    compile_data = dedup_compile_data

    resolved_deps = list(_DEFAULT_RUST_DEPS)
    if deps != None:
        for dep in deps:
            if dep not in resolved_deps:
                resolved_deps.append(dep)

    if crate_name == None:
        crate_name = name

    rust_library(
        name = name,
        crate_name = crate_name,
        srcs = [":" + wrapper_gen] + module_srcs,
        compile_data = compile_data,
        edition = "2021",
        deps = resolved_deps,
        visibility = visibility,
    )


def aorta_cpp_message_wrapper(
        name,
        schema_pkg,
        schema_stem,
        schema_cc_target,
        wrapper_header = None,
        root_fbs_label = None,
        schema_deps = None,
        schema_dep_filegroups = None,
        schema_rel = None,
        action_name = None,
        extra_deps = None,
        visibility = None):
    """Defines a C++ single-include wrapper target with auto-generated wrapper header.

    Args:
      name: cc_library target name.
      schema_pkg: schema package label prefix (for example,
          "//message/schemas/topic/example").
      schema_stem: schema stem (for example, "demo_chat", "echo").
      schema_cc_target: schema cc target label exporting generated + schema meta headers.
      wrapper_header: generated wrapper header file name (defaults to "<name>.h").
      schema_deps: REQUIRED unless `schema_dep_filegroups` is given (no
          default: pass `[]` for a root-only schema with no includes)
          explicit list of `.fbs` labels transitively
          included by `root_fbs`, so wrapper action keys depend on
          included-schema content — otherwise a grandchild-include edit
          would neither re-run the action locally nor invalidate a remote
          cache hit. `fail()`s if omitted; there is no auto-detection (the
          `native.read`-based fallback this used to have was dead code —
          `native.read` does not exist in any shipped Bazel — and it
          silently degraded to a root-only srcs list).
      schema_dep_filegroups: alternative to `schema_deps` for
          EXTERNAL schema roots: `_fbs_closure` FILEGROUP labels expanded
          with `$(locations ...)` (plural). An external root cannot enumerate
          another module's transitive `.fbs` set as individual labels — the
          closure filegroup is the only cross-module-correct spelling — and
          the same list doubles as the `--include-fbs` resolution manifest.
      schema_rel: explicit `--schema-rel` (external roots; see
          `_schema_rel_path`).
      action_name: explicit action routing name, forcing aggregate
          `<Prefix>Action` traits emission for an external action root (the
          in-repo `schemas/action/` path convention does not exist there).

    Note: see the matching note in `aorta_rust_message_binding` above —
    `--root-bfbs` is always wired unconditionally.
    """
    if wrapper_header == None:
        wrapper_header = "{}.h".format(name)
    wrapper_gen = "{}_wrapper_hdr".format(name)
    if root_fbs_label == None:
        root_fbs = "{}:{}.fbs".format(schema_pkg, schema_stem)
    else:
        root_fbs = root_fbs_label

    if schema_dep_filegroups != None:
        if schema_deps != None:
            fail(
                "aorta_cpp_message_wrapper '{}': pass schema_deps OR ".format(name) +
                "schema_dep_filegroups, not both",
            )
        resolved_schema_deps = []
        closure_filegroups = list(schema_dep_filegroups)
    else:
        resolved_schema_deps = _genrule_schema_srcs(schema_pkg, root_fbs, schema_deps)
        closure_filegroups = []

    cmd_parts = [
        "$(location {})".format(_GEN_MESSAGE_WRAPPER),
        "--lang=cpp",
        "--stem={}".format(schema_stem),
        "--schema-rel={}".format(_schema_rel_path(schema_pkg, schema_rel)),
        "--root-fbs=$(location {})".format(root_fbs),
    ]

    # See the matching comment in aorta_rust_message_binding above /
    # aorta_flatbuffers.bzl (Tier-2) for the full rationale.
    if resolved_schema_deps or closure_filegroups:
        cmd_parts.append("--include-fbs")
        for _dep in resolved_schema_deps:
            cmd_parts.append("$(location {})".format(_dep))
        for _group in closure_filegroups:
            cmd_parts.append("$(locations {})".format(_group))

    # See the matching comment in
    # `aorta_rust_message_binding` above — --root-bfbs is always wired
    # unconditionally and drives emission from the loader's qualified model.
    bfbs_target = "{}:{}_bfbs".format(schema_pkg, schema_stem)
    cmd_parts.append("--root-bfbs=$(location {})".format(bfbs_target))
    if action_name != None:
        if not action_name:
            fail("aorta_cpp_message_wrapper '{}': action_name must be non-empty".format(name))
        cmd_parts.append("--action-name={}".format(action_name))
    cmd_parts.append("--out=$@")

    native.genrule(
        name = wrapper_gen,
        srcs = [root_fbs] + resolved_schema_deps + closure_filegroups + [bfbs_target],
        outs = [wrapper_header],
        tools = [_GEN_MESSAGE_WRAPPER],
        cmd = " ".join(cmd_parts),
    )

    cc_deps = [schema_cc_target, _AORTA_CPP_HEADERS]
    if extra_deps != None:
        for dep in extra_deps:
            if dep not in cc_deps:
                cc_deps.append(dep)

    cc_library(
        name = name,
        hdrs = [":" + wrapper_gen],
        # `includes = ["."]` is rejected in the ROOT package (it would resolve
        # to the workspace root); an external root at `//schemas` is a normal
        # package, but a module that expands the macro at its own root is not.
        includes = ["."] if native.package_name() else [],
        deps = cc_deps,
        visibility = visibility,
    )


def aorta_define_rust_bindings(items):
    """Defines Rust message bindings from a list of structs.

    Each item should contain:
      - name (required)
      - crate_name (optional, defaults to `name`)
      - schema_pkg (required)
      - schema_stem (required)
      - include_schema_meta (optional, defaults to True)
      - deps (optional)
      - schema_deps (REQUIRED: list of `.fbs` labels transitively included
        by `root_fbs`, or `[]` for a root-only schema) — `aorta_rust_message_binding`
        / `aorta_cpp_message_wrapper` `fail()`s if the item omits it (no
        auto-detection; see their docstrings).
    """
    for item in items:
        aorta_rust_message_binding(
            name = item.name,
            crate_name = getattr(item, "crate_name", None),
            schema_pkg = item.schema_pkg,
            schema_stem = item.schema_stem,
            include_schema_meta = getattr(item, "include_schema_meta", True),
            root_fbs_label = getattr(item, "root_fbs_label", None),
            include_graph_root_fbs_label = getattr(
                item,
                "include_graph_root_fbs_label",
                None,
            ),
            rust_module_targets = getattr(item, "rust_module_targets", None),
            deps = getattr(item, "deps", None),
            extra_compile_data = getattr(item, "extra_compile_data", None),
            schema_deps = getattr(item, "schema_deps", None),
        )


def aorta_define_cpp_bindings(items):
    """Defines C++ message wrappers from a list of structs.

    Each item should contain:
      - name (required)
      - schema_pkg (required)
      - schema_stem (required)
      - schema_cc_target (required)
      - wrapper_header (optional)
      - schema_deps (REQUIRED: list of `.fbs` labels transitively included
        by `root_fbs`, or `[]` for a root-only schema) — `aorta_rust_message_binding`
        / `aorta_cpp_message_wrapper` `fail()`s if the item omits it (no
        auto-detection; see their docstrings).
    """
    for item in items:
        aorta_cpp_message_wrapper(
            name = item.name,
            schema_pkg = item.schema_pkg,
            schema_stem = item.schema_stem,
            schema_cc_target = item.schema_cc_target,
            wrapper_header = getattr(item, "wrapper_header", None),
            root_fbs_label = getattr(item, "root_fbs_label", None),
            schema_deps = getattr(item, "schema_deps", None),
        )


def aorta_define_aliases(items):
    """Defines aliases from a list of (name, actual) tuples."""
    for name, actual in items:
        native.alias(
            name = name,
            actual = actual,
        )
