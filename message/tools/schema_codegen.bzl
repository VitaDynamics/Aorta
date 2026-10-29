"""Shared FlatBuffers schema codegen macros for Aorta message schemas."""

load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_python//python:defs.bzl", "py_library")
load("//message/tools:bfbs_stage.bzl", "BFBS_STAGE_SCRIPT", "flatc_bfbs_staged_cmd")
load("//message/tools:flatbuffers_rules.bzl", "flatbuffers_python")
load("//message/tools:schema_names.bzl", "schema_meta_py_enabled")

# Every tool/runtime label is anchored with `Label()` so it resolves through
# AORTA's repo mapping, not the caller's. That is what makes
# this file a caller-repo-safe engine: `aorta_external_schema_package`
# (external_schema_root.bzl) expands it inside an EXTERNAL module's package,
# where a bare "//message/tools:flatc" string would resolve against that
# module's root and fail. `str(Label(...))` renders the canonical
# `@@aorta+//...` form, which `$(location ...)` accepts — the same technique
# the Tier-2 macros in aorta_flatbuffers.bzl already use.
_FLATC = Label("//message/tools:flatc")
_SCHEMA_META_GEN = Label("//message/tools:gen_schema_meta")
_PATCH_FLATC_RUST = Label("//message/tools:patch_flatc_rust")
_ACTION_WRAPPER_GEN = Label("//message/tools:gen_action_wrapper")
_DEFAULT_INCLUDE_DIRS = ["message"]
_FLATBUFFERS_CC_DEP = Label("//message/tools:flatbuffers_cc")
_FLATBUFFERS_PY_DEP = Label("//message/tools:flatbuffers_py")
_PY_FASTPATH_RUNTIME = Label("//message/runtime:python_fastpath")
_PY_FASTPATH_GENERATOR = Label("//message/tools:gen_python_fastpath")
_MISSING = "__AORTA_SCHEMA_ITEM_MISSING__"


def _flatc_cmd(mode, root_fbs, dep_fbs = [], include_dir_groups = []):
    """flatc invocation whose `-I` roots come from the DIRECT edges.

    `include_dir_groups` adds filegroup labels — expanded with
    `$(locations ...)`, plural — whose files ALSO contribute `-I` roots. An
    external schema root needs it: when a schema's only direct edge is a
    GENERATED sibling in the same shadow package, the `-I <message root>`
    fallback derived from that edge points into `bazel-out/.../message`, which
    does not contain the source-tree `schemas/system/...` a grandchild include
    still resolves against. Passing the closure here makes every root in the
    include graph searchable, while `dep_fbs` stays direct-only for the
    `_rust_all`/`_cpp_all` aggregates.
    """
    include_srcs = _dedupe([root_fbs] + dep_fbs)
    include_locations = " ".join(
        ["$(location {})".format(src) for src in include_srcs] +
        ["$(locations {})".format(group) for group in _dedupe(include_dir_groups)],
    )

    return """set -e
ROOT_FBS="$(location {root_fbs})"
INCLUDE_FLAGS=""
for src in {include_locations}; do
  INCLUDE_FLAGS="$$INCLUDE_FLAGS -I $$(dirname "$$src")"
  case "$$src" in
    schemas/*)
      INCLUDE_FLAGS="$$INCLUDE_FLAGS -I ."
      ;;
    */schemas/*)
      INCLUDE_FLAGS="$$INCLUDE_FLAGS -I $${{src%%/schemas/*}}"
      ;;
  esac
done
$(location {flatc}) {mode} -o $(@D) $$INCLUDE_FLAGS "$$ROOT_FBS" """.format(
        root_fbs = root_fbs,
        include_locations = include_locations,
        flatc = _FLATC,
        mode = mode,
    )

def _dep_schema_target(dep_fbs_label, lang_suffix):
    if not dep_fbs_label.endswith(".fbs"):
        fail("dep_fbs item must be a .fbs label, got '{}'".format(dep_fbs_label))
    return dep_fbs_label[:-4] + "_" + lang_suffix


def _dep_suffixed_target(dep_fbs_label, suffix):
    if not dep_fbs_label.endswith(".fbs"):
        fail("dep_fbs item must be a .fbs label, got '{}'".format(dep_fbs_label))

    stem_label = dep_fbs_label[:-4]

    # `@repo//pkg:x.fbs` -> `@repo//pkg:x<suffix>`.
    # Without this branch a cross-MODULE dep fell through to the bare-name case
    # and produced the nonsense label `:@repo//pkg:x_fbs_closure`, which is why
    # an external schema root could never express an `@aorta//...` dependency.
    if dep_fbs_label.startswith("@"):
        return stem_label + suffix
    if dep_fbs_label.startswith("//"):
        return stem_label + suffix
    if dep_fbs_label.startswith(":"):
        return ":" + stem_label[1:] + suffix
    return ":" + stem_label + suffix


def dep_fbs_closure_target(dep_fbs_label):
    """Public: `<dep>.fbs` label -> its `<dep>_fbs_closure` filegroup label.

    The external-root macro needs the SAME mapping this file uses internally,
    including the `@repo//` case, to build a cross-module hermetic closure
    without re-walking the Aorta registry graph (which does not know about
    another module's schemas at all).
    """
    return _dep_fbs_closure_target(dep_fbs_label)

def dep_rust_closure_target(dep_fbs_label):
    """Public: `<dep>.fbs` label -> its `<dep>_rust_closure` filegroup label.

    The Rust analogue of `dep_fbs_closure_target`. A generated
    Rust wrapper declares `#[path = ...] pub mod <stem>_generated;` for EVERY
    schema in its include closure, so the crate needs the transitive set of
    `<stem>_rust` outputs — not just the direct edges `<stem>_rust_all`
    carries. Inside Aorta the registry graph walk supplies that list
    (`registry.bzl::_collect_transitive_rust_module_targets`); an EXTERNAL
    schema root has no such graph for another module's schemas and gets the
    closure through Bazel's own dependency graph instead.
    """
    return _dep_rust_closure_target(dep_fbs_label)

def _dep_cpp_lib_target(dep_fbs_label):
    return _dep_suffixed_target(dep_fbs_label, "_cpp_lib")

def _dep_fbs_closure_target(dep_fbs_label):
    return _dep_suffixed_target(dep_fbs_label, "_fbs_closure")

def _dep_rust_closure_target(dep_fbs_label):
    return _dep_suffixed_target(dep_fbs_label, "_rust_closure")


def _item_get(item, key, default = _MISSING):
    if type(item) == "dict":
        if key in item:
            return item[key]
    elif hasattr(item, key):
        return getattr(item, key)

    if default != _MISSING:
        return default
    fail("schema item missing required key '{}'".format(key))


def _dedupe(items):
    seen = {}
    result = []
    for item in items:
        if item in seen:
            continue
        seen[item] = True
        result.append(item)
    return result


def _local_fbs_keys(root_fbs):
    keys = [root_fbs]

    # A repository-qualified root (`@repo//pkg:x.fbs`) is never package-local,
    # so the bare/`:`-prefixed spellings do not name it. Without this guard the
    # `@` case fell through to the bare branch and registered the nonsense key
    # `:@repo//pkg:x.fbs` in `local_cpp_lib_by_fbs` — harmless today only
    # because no dep is ever spelled that way, and a silent mismatch the moment
    # a cross-module root has an intra-package dependency.
    if root_fbs.startswith("//") or root_fbs.startswith("@"):
        return keys
    if root_fbs.startswith(":"):
        keys.append(root_fbs[1:])
    else:
        keys.append(":" + root_fbs)
    return keys


def _action_wrapper_target(stem):
    return stem + "_action_schema"


def _label_basename(label):
    """`@repo//pkg:x.fbs` / `//pkg:x.fbs` / `:x.fbs` / `x.fbs` -> `x.fbs`."""
    colon = label.rfind(":")
    if colon >= 0:
        return label[colon + 1:]
    slash = label.rfind("/")
    if slash >= 0:
        return label[slash + 1:]
    return label


def _action_wrapper_out(root_fbs, stem):
    # `root_fbs` may be a LABEL (a cross-module canonical source, or a
    # package-local `:x.fbs`); an output name must be a plain relative path.
    return stem + "_action/" + _label_basename(root_fbs)


def aorta_define_schema_targets(
        stem,
        root_fbs,
        dep_fbs = [],
        closure_srcs = None,
        rust = False,
        cpp = False,
        bfbs = False,
        include_dir_groups = [],
        schema_meta_rust = False,
        schema_meta_cpp = False,
        schema_meta_py = False,
        python = False,
        python_include_dirs = None):
    """Defines generated targets for a single `.fbs` schema.

    Generated target names follow existing conventions:
      - `<stem>_rust` -> `<stem>_generated.rs`
      - `<stem>_cpp` -> `<stem>_generated.h`
      - `<stem>_bfbs` -> `<stem>.bfbs`
      - `<stem>_schema_meta` -> `<stem>_schema_meta.rs`
      - `<stem>_schema_meta_cpp` -> `<stem>_schema_meta.h`
      - `<stem>_py` (tree artifact)
      - `<stem>_fbs_closure` -> filegroup of own `.fbs` + transitive closure
        of `dep_fbs`'s own `_fbs_closure` targets (Tier-2 external consumers
        reference this to get a hermetic closure for a schema they depend on
        without re-walking the registry graph themselves).

    `dep_fbs` is always the DIRECT edges (registry authoring convention,
    unchanged) — it drives flatc `-I` include-flag computation
    (`_flatc_cmd`), the `_rust_all`/`_cpp_all` direct-dependency aggregate
    filegroups, and the `_fbs_closure` filegroup's recursive composition. It
    must NEVER be the expanded transitive list — that would push
    closure-sized `-I`/aggregate sets through code paths that should only
    ever see direct edges.

    `closure_srcs` (defaults to `dep_fbs` for callers that never expand it,
    preserving the direct-edges-only behavior) is ADDITIONAL to `dep_fbs`
    and is used ONLY to widen the genrule/python-rule `srcs` (declared
    Bazel action inputs) to the full transitive closure — see
    `aorta_define_schema_package`, which passes
    `registry.collect_transitive_dep_fbs(item)` here. This is what makes
    every flatc/bfbs/schema-meta action hermetic: editing a
    grandchild-included `.fbs` now correctly invalidates and re-runs these
    actions instead of leaving stale generated output / a stale
    `SCHEMA_HASH_HEX`, without also perturbing the direct-edge-only
    `-I`/aggregate computations above.
    """
    if closure_srcs == None:
        closure_srcs = dep_fbs

    # gen_schema_meta computes ALL metadata from the compiled
    # bfbs (schema_reflection), so every schema-meta output requires the
    # `<stem>_bfbs` target — including the Rust one, which used to run on
    # regex text parsing alone.
    if schema_meta_rust and not bfbs:
        fail("schema_meta_rust=True requires bfbs=True (stem={})".format(stem))

    if schema_meta_cpp and not bfbs:
        fail("schema_meta_cpp=True requires bfbs=True (stem={})".format(stem))

    if schema_meta_py and not bfbs:
        fail("schema_meta_py=True requires bfbs=True (stem={})".format(stem))

    # The Python schema-meta module re-exports the message classes (one-stop
    # import), so its py_library deps on `<stem>_py_lib` — which needs python=True.
    if schema_meta_py and not python:
        fail("schema_meta_py=True requires python=True (stem={})".format(stem))

    # `hermetic_srcs` is the ONLY thing `closure_srcs` feeds — every genrule
    # `srcs=` / the python rule's `srcs=` below. `-I` flags (`_flatc_cmd`)
    # and the direct-aggregate filegroups all keep using `dep_fbs` (direct)
    # directly, never `hermetic_srcs`/`closure_srcs`.
    #
    # NOTE: `[root_fbs] + closure_srcs` only (NOT `+ dep_fbs` too) —
    # `closure_srcs` is contractually already a superset of `dep_fbs`
    # (registry.bzl's `_collect_transitive_dep_fbs` BFS always starts by
    # including every direct edge; the `closure_srcs == dep_fbs` fallback
    # for plain-dict callers is trivially a superset of itself). Appending
    # `dep_fbs` again would be a no-op AT BEST and a hard Bazel "duplicate
    # label in srcs" failure at worst: `dep_fbs` entries are as-authored
    # (possibly `:Foo.fbs`-relative) while `closure_srcs` entries are always
    # fully-normalized (`//pkg:Foo.fbs`) — `_dedupe` is a plain string
    # dedupe and cannot see that both forms name the same target.
    hermetic_srcs = _dedupe([root_fbs] + closure_srcs)

    # Tier-2 closure mechanism: always exported,
    # regardless of which language outputs this item enables, so an external
    # `aorta_fbs_library` depending on this schema (Tier-1, possibly across
    # a Bzlmod module boundary) can pull in the complete transitive `.fbs`
    # closure without re-walking the registry graph. Built purely from
    # `dep_fbs` (direct) + Bazel's own dependency graph (each dep's own
    # `_fbs_closure` already carries ITS grandchildren).
    native.filegroup(
        name = stem + "_fbs_closure",
        srcs = _dedupe([root_fbs] + [
            _dep_fbs_closure_target(dep)
            for dep in dep_fbs
        ]),
    )

    # The Rust analogue of `_fbs_closure`, and declared with the
    # same unconditional/recursive shape for the same reason.
    #
    # A generated Rust wrapper emits one `#[path = ...] pub mod
    # <stem>_generated;` per schema in its TRANSITIVE include closure, so the
    # crate must be handed that transitive set of flatc `.rs` outputs.
    # `_rust_all` above is direct-edges-only by design and
    # cannot serve; the registry graph walk can, but only for schemas that live
    # in `SCHEMA_DEFINITIONS` — which by construction excludes every schema of
    # ANOTHER Bazel module. Composing each direct dep's own `_rust_closure`
    # pushes the transitivity into Bazel's dependency graph, which crosses
    # module boundaries the registry cannot see.
    #
    # UNCONDITIONAL (even for `rust = False`): a dependent's closure names
    # `<dep>_rust_closure` without knowing the dep's flags, exactly as
    # `_fbs_closure` does. A `rust = False` schema simply contributes no module
    # of its own — it still forwards ITS dependencies' modules.
    native.filegroup(
        name = stem + "_rust_closure",
        srcs = _dedupe(
            ([":" + stem + "_rust"] if rust else []) + [
                _dep_rust_closure_target(dep)
                for dep in dep_fbs
            ],
        ),
    )

    if rust:
        # When a bfbs exists, feed it to patch_flatc_rust so it can
        # rewrite foreign type references to absolute `crate::<dep>_generated::…`
        # paths from the include closure's namespace/type map (ground truth). A
        # non-bfbs leaf schema only gets the local-namespace flattening re-export
        # (its same-namespace foreign refs resolve bare via the file-top glob).
        rust_srcs = hermetic_srcs
        patch_cmd = "$(location {}) --path $@".format(_PATCH_FLATC_RUST)
        if bfbs:
            rust_srcs = _dedupe(hermetic_srcs + [":" + stem + "_bfbs"])
            patch_cmd += " --root-bfbs $(location :{}_bfbs)".format(stem)
        rust_cmd = "{} && {}".format(
            _flatc_cmd("--rust", root_fbs, dep_fbs, include_dir_groups),
            patch_cmd,
        )
        native.genrule(
            name = stem + "_rust",
            srcs = rust_srcs,
            outs = [stem + "_generated.rs"],
            tools = [_FLATC, _PATCH_FLATC_RUST],
            cmd = rust_cmd,
        )
        native.filegroup(
            name = stem + "_rust_all",
            srcs = [":" + stem + "_rust"] + [
                _dep_schema_target(dep, "rust")
                for dep in dep_fbs
            ],
        )

    if cpp:
        native.genrule(
            name = stem + "_cpp",
            srcs = hermetic_srcs,
            outs = [stem + "_generated.h"],
            tools = [_FLATC],
            cmd = _flatc_cmd("--cpp", root_fbs, dep_fbs, include_dir_groups),
        )
        native.filegroup(
            name = stem + "_cpp_all",
            srcs = [":" + stem + "_cpp"] + [
                _dep_schema_target(dep, "cpp")
                for dep in dep_fbs
            ],
        )

    if bfbs:
        native.genrule(
            name = stem + "_bfbs",
            srcs = hermetic_srcs,
            outs = [stem + ".bfbs"],
            tools = [_FLATC, BFBS_STAGE_SCRIPT],
            # `--bfbs-builtins` keeps built-in attrs (bit_flags, key,
            # required, id, ...) in the bfbs; matches the main pipeline in
            # `aorta_flatbuffers.bzl` so reflection consumers see the same
            # attribute set regardless of which codegen path produced the
            # bfbs.
            cmd = flatc_bfbs_staged_cmd(
                "--binary --schema --bfbs-builtins",
                root_fbs,
                dep_fbs,
                closure_srcs = hermetic_srcs,
                flatc = _FLATC,
                # Explicit slot: the generated action aggregate lives in an
                # extra `<stem>_action/` subdirectory that must not leak
                # into the canonical staged layout.
                root_stage_rel = "{}/{}.fbs".format(native.package_name(), stem),
                dedupe = _dedupe,
            ),
        )

    if schema_meta_rust:
        cmd = "$(location {}) --root $(location {}) --bfbs $(location :{}_bfbs) --out $@".format(
            _SCHEMA_META_GEN,
            root_fbs,
            stem,
        )
        native.genrule(
            name = stem + "_schema_meta",
            srcs = hermetic_srcs + [":" + stem + "_bfbs"],
            outs = [stem + "_schema_meta.rs"],
            tools = [_SCHEMA_META_GEN],
            cmd = cmd,
        )

    if schema_meta_cpp:
        cmd = (
            "$(location {}) --lang=cpp --root $(location {}) --bfbs $(location :{}_bfbs) --out $@"
        ).format(
            _SCHEMA_META_GEN,
            root_fbs,
            stem,
        )
        native.genrule(
            name = stem + "_schema_meta_cpp",
            srcs = hermetic_srcs + [":" + stem + "_bfbs"],
            outs = [stem + "_schema_meta.h"],
            tools = [_SCHEMA_META_GEN],
            cmd = cmd,
        )

    if schema_meta_py:
        cmd = (
            "$(location {}) --lang=python --root $(location {}) --bfbs $(location :{}_bfbs) --out $@"
        ).format(
            _SCHEMA_META_GEN,
            root_fbs,
            stem,
        )
        native.genrule(
            name = stem + "_schema_meta_py",
            srcs = hermetic_srcs + [":" + stem + "_bfbs"],
            outs = [stem + "_schema_meta.py"],
            tools = [_SCHEMA_META_GEN],
            cmd = cmd,
        )

        # ONE-STOP importable module: `import <stem>_schema_meta as m` gives the
        # schema metadata (m.SCHEMA_BFBS / m.SCHEMA_HASH_HEX / m.ROOT_TYPE_NAME /
        # m.FIXED_SIZE / m.HAS_AORTA_HEADER / m.ROOT_TYPE) AND the generated message
        # class + builders (re-exported via `from <ns>.<Root> import *`, e.g.
        # m.Chat / m.ChatStart). Depends on `<stem>_py_lib` so a SINGLE dep + a
        # SINGLE import is all a consumer needs (no separate message-class dep).
        py_library(
            name = stem + "_schema_meta_py_lib",
            srcs = [":" + stem + "_schema_meta_py"],
            imports = ["."],
            deps = [":" + stem + "_py_lib"],
        )

    if python:
        include_dirs = python_include_dirs
        if include_dirs == None:
            include_dirs = _DEFAULT_INCLUDE_DIRS
        flatbuffers_python(
            name = stem + "_py",
            bfbs = ":" + stem + "_bfbs" if bfbs else None,
            fastpath_generator = _PY_FASTPATH_GENERATOR if bfbs else None,
            stem = stem,
            root_src = root_fbs,
            srcs = hermetic_srcs,
            direct_srcs = _dedupe([root_fbs] + dep_fbs + include_dir_groups),
            include_dirs = include_dirs,
        )

        if bfbs:
            native.filegroup(
                name = stem + "_fastpath",
                srcs = [":" + stem + "_py"],
                output_group = "fastpath",
            )

        # Importable wrapper around the flatc tree-artifact: a consumer that
        # `deps = [":<stem>_py_lib"]` can `import <ns>.<Type>` (e.g. demo.Chat,
        # aorta.sys.AortaHeader). The tree-artifact root is put on sys.path via
        # `imports`; generated FlatBuffers code needs `flatbuffers` at runtime.
        py_library(
            name = stem + "_py_lib",
            data = [":" + stem + "_py"] + ([":" + stem + "_fastpath"] if bfbs else []),
            imports = [stem + "_py"] + ([stem + "_py.fastpath"] if bfbs else []),
            deps = [_FLATBUFFERS_PY_DEP] + ([_PY_FASTPATH_RUNTIME] if bfbs else []),
        )


def aorta_define_schema_package(
        schema_items,
        export_files = True,
        closure_srcs_by_stem = None,
        include_dir_groups_by_stem = None,
        closure_expander = None):
    """Defines a schema package from a single list of schema item configs.

    Each item supports the same flags as `aorta_define_schema_targets`, plus:
      - include_in_examples_bfbs (optional, defaults to False)
      - cpp_lib (optional, defaults to cpp flag)
      - cpp_lib_name (optional, defaults to `<stem>_cpp_lib`)
      - cc_alias (optional, defaults to `<stem>_cc`; set to None to disable)
      - extra_cc_deps (optional list)

    `closure_srcs_by_stem` is an optional `{stem: [labels]}`
    override for the hermetic `srcs` closure. The default (None) keeps the
    registry graph walk, which is the right answer for in-repo packages and
    the WRONG one for an EXTERNAL schema root: `collect_transitive_dep_fbs`
    can only expand deps that exist in `SCHEMA_DEFINITIONS`, so a dep in
    another Bazel module would silently degrade to a direct-edge-only srcs
    list (the stale-grandchild defect class again). `aorta_external_schema_package`
    passes each item's direct deps' `_fbs_closure` filegroups instead, which
    carry the full transitive closure through Bazel's own graph and work
    across module boundaries.
    """

    root_fbs_files = []
    rust_targets = []
    cpp_targets = []
    python_targets = []
    bfbs_targets = []
    schema_meta_rust_targets = []
    schema_meta_cpp_targets = []
    schema_meta_py_targets = []
    examples_bfbs_targets = []
    local_cpp_lib_by_fbs = {}
    local_cpp_no_lib_by_fbs = {}

    for item in schema_items:
        stem = _item_get(item, "stem")
        root_fbs = _item_get(item, "root_fbs")
        cpp = _item_get(item, "cpp", False)
        cpp_lib = _item_get(item, "cpp_lib", cpp)
        if cpp_lib and not cpp:
            fail("cpp_lib=True requires cpp=True (stem={})".format(stem))
        for key in _local_fbs_keys(root_fbs):
            if cpp_lib:
                cpp_lib_name = _item_get(item, "cpp_lib_name", stem + "_cpp_lib")
                local_cpp_lib_by_fbs[key] = cpp_lib_name
            elif cpp:
                local_cpp_no_lib_by_fbs[key] = stem

    for item in schema_items:
        stem = _item_get(item, "stem")
        root_fbs = _item_get(item, "root_fbs")
        dep_fbs = _item_get(item, "dep_fbs", [])
        rust = _item_get(item, "rust", False)
        cpp = _item_get(item, "cpp", False)
        bfbs = _item_get(item, "bfbs", False)
        schema_meta_rust = _item_get(item, "schema_meta_rust", False)
        schema_meta_cpp = _item_get(item, "schema_meta_cpp", False)
        python = _item_get(item, "python", False)
        # Default schema-meta-py ON for every Python schema that has a bfbs (both
        # cheap; typed publish/service/action needs the generated
        # `<stem>_schema_meta` module). `schema_meta_py_enabled` is the single
        # source shared with the `aorta_msgs` wheel package list so the two never
        # drift; an explicit `schema_meta_py` on the item still wins.
        schema_meta_py = schema_meta_py_enabled(item)
        python_include_dirs = _item_get(item, "python_include_dirs", None)
        include_in_examples_bfbs = _item_get(item, "include_in_examples_bfbs", False)
        cpp_lib = _item_get(item, "cpp_lib", cpp)
        action_schema = _item_get(item, "action_schema", False)

        effective_root_fbs = root_fbs

        # genrule `srcs` must ADDITIONALLY declare the FULL transitive
        # `.fbs` closure, or flatc reads grandchild includes as undeclared
        # inputs and their edits never invalidate the action — but `dep_fbs`
        # itself stays DIRECT-only (it also drives `-I` flags and the
        # direct-aggregate filegroups inside `aorta_define_schema_targets`,
        # which must never see the expanded list). The closure is threaded
        # through the SEPARATE `closure_srcs` param instead. Registry items
        # (structs with `.pkg`) get the closure via the registry's own graph
        # walk; a plain-dict item (no registry graph to walk) falls back to
        # its direct `dep_fbs` — the unchanged direct-edges behavior — since there
        # is no broader item set to expand against.
        if closure_srcs_by_stem != None:
            if stem not in closure_srcs_by_stem:
                fail(
                    ("closure_srcs_by_stem was supplied but has no entry for stem " +
                     "'{}' (keys: {})").format(stem, sorted(closure_srcs_by_stem.keys())),
                )
            closure_srcs = closure_srcs_by_stem[stem]
        elif type(item) == "dict":
            closure_srcs = dep_fbs
        elif closure_expander != None:
            closure_srcs = closure_expander(item)
        else:
            # A struct item is a REGISTRY entry, and expanding its closure needs
            # the registry graph — which this file must not load
            # (`aorta_schema_package` has to be usable by a module
            # that has no aorta registry at all, and a static `load()` of
            # `schemas/registry.bzl` would put any product-schema module's
            # fragments in every such module's load graph). Refuse loudly rather
            # than silently degrading to a direct-edge-only `srcs` list, which is
            # the stale-grandchild bug this parameter exists to prevent.
            fail(
                ("aorta_define_schema_package: item '{}' is a registry struct but " +
                 "neither `closure_srcs_by_stem` nor `closure_expander` was " +
                 "supplied. In-repo registry packages load this macro from " +
                 "//message/tools:schema_codegen_registry.bzl, which injects the " +
                 "registry graph walk; a self-hosted schema package uses " +
                 "//message/tools:schema_package.bzl, which supplies the " +
                 "per-dependency closure filegroups instead.").format(stem),
            )

        if action_schema:
            native.genrule(
                name = _action_wrapper_target(stem),
                srcs = [root_fbs],
                outs = [_action_wrapper_out(root_fbs, stem)],
                tools = [_ACTION_WRAPPER_GEN],
                cmd = "$(location {}) --root-fbs $(location {}) --out $@".format(
                    _ACTION_WRAPPER_GEN,
                    root_fbs,
                ),
            )
            effective_root_fbs = ":" + _action_wrapper_target(stem)

        include_dir_groups = []
        if include_dir_groups_by_stem != None:
            include_dir_groups = include_dir_groups_by_stem.get(stem, [])

        aorta_define_schema_targets(
            stem = stem,
            root_fbs = effective_root_fbs,
            dep_fbs = dep_fbs,
            closure_srcs = closure_srcs,
            include_dir_groups = include_dir_groups,
            rust = rust,
            cpp = cpp,
            bfbs = bfbs,
            schema_meta_rust = schema_meta_rust,
            schema_meta_cpp = schema_meta_cpp,
            schema_meta_py = schema_meta_py,
            python = python,
            python_include_dirs = python_include_dirs,
        )

        root_fbs_files.append(root_fbs)
        if rust:
            rust_targets.append(":{}_rust".format(stem))
        if cpp:
            cpp_targets.append(":{}_cpp".format(stem))
        if python:
            python_targets.append(":{}_py".format(stem))
        if bfbs:
            bfbs_targets.append(":{}_bfbs".format(stem))
            if include_in_examples_bfbs:
                examples_bfbs_targets.append(":{}_bfbs".format(stem))
        if schema_meta_rust:
            schema_meta_rust_targets.append(":{}_schema_meta".format(stem))
        if schema_meta_cpp:
            schema_meta_cpp_targets.append(":{}_schema_meta_cpp".format(stem))
        if schema_meta_py:
            schema_meta_py_targets.append(":{}_schema_meta_py".format(stem))

        if cpp_lib:
            cpp_lib_name = _item_get(item, "cpp_lib_name", stem + "_cpp_lib")
            cc_alias = _item_get(item, "cc_alias", stem + "_cc")
            extra_cc_deps = _item_get(item, "extra_cc_deps", [])

            cc_hdrs = [":{}_cpp".format(stem)]
            if schema_meta_cpp:
                cc_hdrs.append(":{}_schema_meta_cpp".format(stem))

            cc_deps = []
            for dep in dep_fbs:
                if dep in local_cpp_lib_by_fbs:
                    cc_deps.append(":{}".format(local_cpp_lib_by_fbs[dep]))
                elif dep in local_cpp_no_lib_by_fbs:
                    fail(
                        (
                            "stem '{}' depends on '{}' whose schema item '{}' sets cpp_lib=False. " +
                            "Set cpp_lib=True on that dependency or provide explicit extra_cc_deps."
                        ).format(
                            stem,
                            dep,
                            local_cpp_no_lib_by_fbs[dep],
                        )
                    )
                else:
                    cc_deps.append(_dep_cpp_lib_target(dep))
            cc_deps.extend(extra_cc_deps)
            cc_deps.append(_FLATBUFFERS_CC_DEP)

            cc_library(
                name = cpp_lib_name,
                hdrs = cc_hdrs,
                includes = ["."],
                deps = _dedupe(cc_deps),
            )

            if cc_alias and cc_alias != cpp_lib_name:
                native.alias(
                    name = cc_alias,
                    actual = ":{}".format(cpp_lib_name),
                )

    if export_files:
        native.exports_files(root_fbs_files)

    native.filegroup(
        name = "all_fbs",
        srcs = root_fbs_files,
    )

    # PHYSICAL census of this package, and — transitively — of every package
    # nested under it.
    #
    # `glob()` and `subpackages()` both STOP at a package boundary, and
    # `subpackages()` does not report a package nested inside another package.
    # So `//message:schema_source_files`, which asks for the first-level schema
    # packages' `all_source_fbs`, used to be blind to a `.fbs` checked in under
    # a package nested inside one of them: the file was in no input set, and
    # `registry_bijection_lint_test` — whose whole job is "every physical .fbs
    # has exactly one registry item" — passed while it sat in the tree.
    #
    # Each level therefore reports its own subpackages, which closes the
    # recursion in the only way Bazel allows: a package can enumerate ITS
    # subpackages, but can never glob another package's files. That also makes
    # the rule fail closed for a NON-cooperating nested package — one that is
    # not a schema package has no `all_source_fbs`, so this label does not
    # resolve and every consumer of the census fails at analysis instead of
    # silently censusing less than the tree contains.
    native.filegroup(
        name = "all_source_fbs",
        srcs = native.glob(["**/*.fbs"], allow_empty = True) + [
            "//{}/{}:all_source_fbs".format(native.package_name(), subpackage)
            for subpackage in native.subpackages(include = ["**"], allow_empty = True)
        ],
        visibility = ["//visibility:public"],
    )

    native.filegroup(
        name = "all_rust",
        srcs = rust_targets,
    )

    native.filegroup(
        name = "all_cpp",
        srcs = cpp_targets,
    )

    native.filegroup(
        name = "all_python",
        srcs = python_targets,
    )

    native.filegroup(
        name = "all_bfbs",
        srcs = bfbs_targets,
    )

    native.filegroup(
        name = "all_schema_meta_rust",
        srcs = schema_meta_rust_targets,
    )

    native.filegroup(
        name = "all_schema_meta_cpp",
        srcs = schema_meta_cpp_targets,
    )

    native.filegroup(
        name = "all_schema_meta_py",
        srcs = schema_meta_py_targets,
    )

    native.filegroup(
        name = "examples_bfbs",
        srcs = examples_bfbs_targets,
    )
