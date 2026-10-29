"""
Aorta FlatBuffers Build Rules

Unified build rules for generating FlatBuffers bindings in Rust, C++, and Python.
Provides consistent API across languages with automatic dependency management.

Example:
    load("//message/tools:aorta_flatbuffers.bzl", "aorta_fbs_library")
    
    aorta_fbs_library(
        name = "demo_chat",
        srcs = ["demo_chat.fbs"],
        deps = [":aorta_base"],
    )
    
    # Usage in other BUILD files:
    # Rust:   deps = ["//message:fbs_schema_demo_chat_rust"]
    # C++:    deps = ["//message:fbs_schema_demo_chat_cc"]
    # Python: deps = ["//message:fbs_schema_demo_chat_py"]
"""

load("@rules_rust//rust:defs.bzl", "rust_library")
load(":bfbs_stage.bzl", "BFBS_STAGE_SCRIPT", "flatc_bfbs_staged_cmd")
load(":flatbuffers_rules.bzl", "flatbuffers_python")

# Anchor tool labels to THIS repository so they resolve correctly when
# external projects load this .bzl file.  native rule attrs like `tools`
# and `deps` need Label objects (or their string representation) rather
# than bare "//…" strings, which would be resolved relative to the
# *caller's* repository.
_GEN_SCHEMA_META = Label("//message:gen_schema_meta")
_GEN_MESSAGE_WRAPPER = Label("//message/tools:gen_message_wrapper")
_GEN_ACTION_WRAPPER = Label("//message/tools:gen_action_wrapper")
_PATCH_FLATC_RUST = Label("//message/tools:patch_flatc_rust")
_AORTA_CPP_HEADERS = Label("//cpp:aorta_cpp_headers")

# Cross-repo dependencies (flatc binary, flatbuffers C++/Rust runtimes) go
# through Aorta-owned aliases in //message/tools — a raw "@flatbuffers"/
# "@aorta_crates" string here would resolve through the CONSUMER's repo
# mapping and break every external Bzlmod build. The lint
# job enforces zero raw cross-repo strings in this file.
_FLATC = Label("//message/tools:flatc")
_FLATBUFFERS_CC = Label("//message/tools:flatbuffers_cc")
_FLATBUFFERS_RUST = Label("//message/tools:flatbuffers_rust")

def _is_external_dep(dep):
    """Check if a dep is a full label (cross-repo) vs bare stem (same-package)."""
    return "//" in dep or dep.startswith("@")

def _normalize_local_dep(dep):
    """Strip leading ':' from a local dep to get the bare stem."""
    if dep.startswith(":"):
        return dep[1:]
    return dep

def _dep_fbs_closure_target(dep):
    """`{name}_fbs_closure` label for a local (bare stem) or external (full label) dep."""
    if dep.startswith("//") or dep.startswith("@"):
        return dep + "_fbs_closure"
    return ":" + dep + "_fbs_closure"

def _dedupe(items):
    seen = {}
    result = []
    for item in items:
        if item in seen:
            continue
        seen[item] = True
        result.append(item)
    return result

def _flatc_include_cmd(fbs_file, dep_fbs_locations, flatc_tool, mode, flatc_args = ""):
    """Build a flatc command with proper -I include path resolution.

    For each dependency .fbs file, adds:
      1. -I $(dirname $dep)  — covers same-directory includes
      2. -I ${dep%%/schemas/*} — for paths matching */message/schemas/*, adds the
         message root so that 'include "schemas/system/aorta_base.fbs"' resolves correctly.

    This replicates the logic from schema_codegen.bzl _flatc_cmd(). BFBS
    generation does NOT go through here — it uses the shared
    `flatc_bfbs_staged_cmd` (bfbs_stage.bzl) so its output bytes (hashed
    into SCHEMA_HASH_HEX and embedded into SDK binaries as the runtime
    schema payload) are deterministic across build contexts.
    """
    return """
        SRCDIR=$$(dirname $(location {fbs}))
        INCLUDE_FLAGS="-I $$SRCDIR"
        DEPS="{dep_locations}"
        for src in $$DEPS; do
          INCLUDE_FLAGS="$$INCLUDE_FLAGS -I $$(dirname $$src)"
          case "$$src" in
            */message/schemas/*|message/schemas/*)
              INCLUDE_FLAGS="$$INCLUDE_FLAGS -I $${{src%%/schemas/*}}"
              ;;
          esac
        done
        $(location {flatc_tool}) {mode} {flatc_args} -o $(@D) $$INCLUDE_FLAGS $(location {fbs})
    """.format(
        fbs = fbs_file,
        dep_locations = dep_fbs_locations,
        flatc_tool = flatc_tool,
        mode = mode,
        flatc_args = flatc_args,
    )

def aorta_fbs_library(
    name,
    srcs,
    deps = [],
    visibility = None,
    cpp_wrapper = False,
    flatc_args = [],
    _action_name = None,
    _cpp_only = False):
    """Generate FlatBuffers bindings for Rust, C++, and Python from .fbs schema.

    Creates the following targets:
        {name}_rust_gen     - Generated Rust code (.rs files)
        {name}_cpp_gen      - Generated C++ code (.h files)
        {name}_py_gen       - Generated Python code
        {name}_bfbs         - Binary schema (.bfbs file)
        {name}_rust         - Rust library (recommended)
        {name}_cc           - C++ library (+ typed wrapper when cpp_wrapper=True)
        {name}_py           - Python library

    Args:
        name: Base name for all generated targets (e.g., "demo_chat")
        srcs: List of .fbs schema files (usually a single file)
        deps: List of schema dependencies. Supports two formats:
              - Bare stem for same-package deps (e.g., ":aorta_base")
              - Full label for cross-repo deps (e.g., "@aorta//message/schemas/system:aorta_base")
        visibility: Visibility for generated targets (default: public within aorta)
        cpp_wrapper: If True, generate typed wrapper header ({name}_msg.h) with
                     TypedPublishTraits/TypedSubscribeTraits for use with aorta typed APIs.
        flatc_args: Additional arguments to flatc compiler
    """

    if len(srcs) != 1:
        fail("aorta_fbs_library currently only supports a single .fbs file")

    fbs_file = srcs[0]

    # Classify deps: local (bare stem) vs external (full label)
    # Normalize local deps to strip leading ':' (e.g., ":aorta_base" -> "aorta_base")
    # so that string concatenation (dep + ".fbs", dep + "_cpp_gen") produces valid labels.
    _local_deps = [_normalize_local_dep(dep) for dep in deps if not _is_external_dep(dep)]
    _external_deps = [dep for dep in deps if _is_external_dep(dep)]

    # Local deps: derive .fbs and generated target names via string concatenation
    _local_fbs = [dep + ".fbs" for dep in _local_deps]
    _local_cpp_gen = [":" + dep + "_cpp_gen" for dep in _local_deps]
    _local_rust_gen = [dep + "_rust_gen" for dep in _local_deps]

    # External deps: derive .fbs file label and C++ library label.
    # Aorta's schema registry generates C++ targets with "_cpp" suffix.
    _external_fbs = [dep + ".fbs" for dep in _external_deps]
    _external_cc = [dep + "_cpp" for dep in _external_deps]

    all_fbs_srcs = srcs + _local_fbs + _external_fbs

    # Build dep .fbs location string for flatc -I resolution
    _dep_fbs_list = _local_fbs + _external_fbs
    _dep_fbs_locations = " ".join(["$(location {})".format(f) for f in _dep_fbs_list]) if _dep_fbs_list else ""

    # Tier-2 closure mechanism: flatc reads a
    # dep's own includes (grandchildren) as UNDECLARED workspace inputs when
    # `srcs` only lists the DIRECT dep `.fbs` files above — editing a
    # grandchild then never invalidates these actions. Each dep (local or
    # external/cross-repo) exports its own `{dep}_fbs_closure` filegroup
    # (Tier-1: schema_codegen.bzl; Tier-2: this macro, recursively) carrying
    # its full transitive closure; consuming those closure targets as
    # additional `srcs` makes every action below hermetic. `_dep_fbs_locations`
    # above stays DIRECT-only — flatc's `-I` flags only need the direct dep
    # directories (the `-I <message-root>` fallback already resolves
    # `schemas/...`-rooted grandchild includes once the file is a declared
    # input), so include-flag computation is unaffected.
    _local_closure_targets = [_dep_fbs_closure_target(dep) for dep in _local_deps]
    _external_closure_targets = [_dep_fbs_closure_target(dep) for dep in _external_deps]
    _hermetic_srcs = _dedupe(all_fbs_srcs + _local_closure_targets + _external_closure_targets)

    # Default visibility if not specified. Must happen BEFORE the closure
    # filegroup below: a consumer in another package
    # references the PRODUCER's `{name}_fbs_closure` via `_dep_fbs_closure_target`
    # (line ~173) as one of its own `_hermetic_srcs`, so that filegroup needs
    # the SAME cross-package visibility as the macro's `_cc`/`_py` targets —
    # defaulting visibility afterward left it at Bazel's implicit
    # `//visibility:private` whenever a caller didn't pass an explicit
    # `visibility` kwarg, silently rejecting an otherwise-valid cross-package
    # dep.
    if visibility == None:
        visibility = ["//:__subpackages__"]

    native.filegroup(
        name = name + "_fbs_closure",
        srcs = _dedupe(srcs + _local_closure_targets + _external_closure_targets),
        visibility = visibility,
    )

    # ========================================
    # 1. Generate Rust code
    # ========================================
    # The Rust wrapper only wires same-package deps, so it cannot express an
    # external dep's generated module. _cpp_only callers (aorta_action_library,
    # whose expanded schema depends on the external action_common) skip the Rust
    # and Python surfaces entirely rather than emit broken targets.
    if not _cpp_only:
        native.genrule(
            name = name + "_rust_gen",
            # The `{name}_bfbs` target (created unconditionally
            # below) feeds patch_flatc_rust its ground-truth namespace/type map
            # for the absolute foreign-reference rewrite.
            srcs = _dedupe(_hermetic_srcs + [":" + name + "_bfbs"]),
            outs = [name + "_generated.rs"],
            tools = [_FLATC, _PATCH_FLATC_RUST],
            # Mirrors Tier-1's rust genrule (schema_codegen.bzl `_flatc_cmd`
            # + `patch_flatc_rust`): flatc's Rust output has cross-crate foreign
            # type references rewritten to absolute `crate::<dep>_generated::…`
            # paths; Tier-2 previously emitted unpatched flatc output.
            cmd = _flatc_include_cmd(
                fbs_file = fbs_file,
                dep_fbs_locations = _dep_fbs_locations,
                flatc_tool = _FLATC,
                mode = "--rust",
                flatc_args = " ".join(flatc_args),
            ).strip() + " && $(location {}) --path $@ --root-bfbs $(location :{}_bfbs)".format(
                _PATCH_FLATC_RUST,
                name,
            ),
        )

    # ========================================
    # 2. Generate C++ code
    # ========================================
    native.genrule(
        name = name + "_cpp_gen",
        srcs = _hermetic_srcs,
        outs = [name + "_generated.h"],
        tools = [_FLATC],
        cmd = _flatc_include_cmd(
            fbs_file = fbs_file,
            dep_fbs_locations = _dep_fbs_locations,
            flatc_tool = _FLATC,
            mode = "--cpp",
            flatc_args = " ".join(flatc_args),
        ),
    )
    
    # ========================================
    # 3. Generate Python code
    # ========================================
    if not _cpp_only:
        flatbuffers_python(
            name = name + "_py_gen",
            srcs = _hermetic_srcs,
            # `-I` derivation is a direct-deps-only
            # concern, kept separate from `srcs` (the hermetic closure).
            direct_srcs = all_fbs_srcs,
            include_dirs = ["message"],
        )

    # ========================================
    # 4. Generate binary schema (.bfbs)
    # ========================================
    native.genrule(
        name = name + "_bfbs",
        srcs = _hermetic_srcs,
        outs = [name + ".bfbs"],
        tools = [_FLATC, BFBS_STAGE_SCRIPT],
        # `--bfbs-builtins` preserves built-in attributes (bit_flags,
        # key, required, explicit id, ...) in the binary schema so
        # reflection-time consumers (e.g. DisplayPrinter in
        # tools/aorta-introspect) can see them after
        # Parser::Deserialize. Without it flatc silently strips them.
        # Shared staged command (bfbs_stage.bzl): bfbs bytes are hashed
        # into SCHEMA_HASH_HEX and embedded into SDK binaries, so their
        # path strings must be byte-deterministic across build contexts —
        # including for action aggregates whose root .fbs is GENERATED
        # (bazel-out) while its includes are sources. root_stage_rel is
        # derived (None): a Tier-2 root's config/external-prefix-stripped
        # path IS its canonical slot (no `<stem>_action/` subdirectory
        # indirection like Tier-1's registry aggregates).
        cmd = flatc_bfbs_staged_cmd(
            "--binary --schema --bfbs-builtins",
            fbs_file,
            _dep_fbs_list,
            closure_srcs = _hermetic_srcs,
            flatc = _FLATC,
        ),
    )

    # ========================================
    # 4.1 Generate schema meta (fixed size hint) -- Rust only
    # ========================================
    if not _cpp_only:
        # --bfbs is required for every language (metadata comes
        # from the compiled bfbs via schema_reflection); `{name}_bfbs`
        # exists unconditionally above.
        native.genrule(
            name = name + "_schema_meta_gen",
            srcs = _hermetic_srcs + [":" + name + "_bfbs"],
            outs = [name + "_schema_meta.rs"],
            tools = [_GEN_SCHEMA_META],
            cmd = """
                $(location {tool}) \
                    --root $(location {fbs}) \
                    --bfbs $(location :{name}_bfbs) \
                    --out $@
            """.format(
                tool = _GEN_SCHEMA_META,
                fbs = fbs_file,
                name = name,
            ),
        )

    # ========================================
    # 4.2 Generate C++ schema meta (SchemaMeta<T> specialization)
    # ========================================
    native.genrule(
        name = name + "_schema_meta_cpp_gen",
        srcs = _hermetic_srcs + [":" + name + "_bfbs"],
        outs = [name + "_schema_meta.h"],
        tools = [_GEN_SCHEMA_META],
        cmd = """
            $(location {tool}) \
                --lang=cpp \
                --root $(location {fbs}) \
                --bfbs $(location :{name}_bfbs) \
                --out $@
        """.format(
            tool = _GEN_SCHEMA_META,
            fbs = fbs_file,
            name = name,
        ),
    )
    
    # ========================================
    # 5. Create Rust library wrapper
    # NOTE: Rust wrapper currently only supports local (same-package) deps.
    # External dep support for Rust is out of scope (requires --schema-rel fix).
    # ========================================
    if not _cpp_only:
        _generate_rust_wrapper(
            name = name + "_rust_wrapper_gen",
            msg_name = name,
            deps = _local_deps,
        )

        rust_library(
            name = name + "_rust",
            srcs = [":" + name + "_rust_wrapper_gen"],
            compile_data = [
                ":" + name + "_rust_gen",
                ":" + name + "_bfbs",
                ":" + name + "_schema_meta_gen",
            ] + _local_rust_gen,
            edition = "2021",
            deps = [_FLATBUFFERS_RUST],
            visibility = visibility,
        )

    # ========================================
    # 6. Create C++ library
    # ========================================
    _cc_hdrs = [
        ":" + name + "_cpp_gen",
        ":" + name + "_schema_meta_cpp_gen",
    ] + _local_cpp_gen

    _cc_deps = [_FLATBUFFERS_CC] + _external_cc

    if cpp_wrapper:
        # Generate typed wrapper header ({name}_msg.h)
        _wrapper_gen_name = name + "_cpp_wrapper_gen"
        _wrapper_cmd_args = [
            "$(location {})".format(_GEN_MESSAGE_WRAPPER),
            "--lang=cpp",
            "--stem=" + name,
            "--schema-rel=",
            "--root-fbs=$(location {})".format(fbs_file),
        ]

        # Pass the declared transitive `.fbs` closure as an explicit
        # manifest so `resolve_schema_include_path` can resolve an include
        # by exact path-suffix match instead of directory-guessing. This is
        # the ONLY correct mechanism for a Tier-2 EXTERNAL consumer whose
        # own schema (in ITS OWN package, no `message/` ancestor at all)
        # includes e.g. `schemas/system/aorta_base.fbs` — that file lives in
        # the SEPARATE `@aorta` module's sandboxed tree
        # (`external/aorta~/message/schemas/system/aorta_base.fbs`), a path
        # no directory walk rooted at the consumer's own schema can ever
        # construct. `{dep}_fbs_closure` already carries each direct dep's
        # own `.fbs` file plus its transitive closure, so the local +
        # external closure targets alone give the full manifest.
        _include_fbs_targets = _local_closure_targets + _external_closure_targets
        if _include_fbs_targets:
            _wrapper_cmd_args.append("--include-fbs")
            for _include_fbs_target in _include_fbs_targets:
                _wrapper_cmd_args.append("$(locations {})".format(_include_fbs_target))

        # Drive the C++ wrapper emitters from
        # the compiled bfbs's qualified model. `{name}_bfbs` is built from
        # `fbs_file` above — for an action library that IS the generated
        # AGGREGATE root (not a source schema), so --root-bfbs correctly
        # carries the aggregate's bfbs. This also makes the wrapper action
        # closure-hermetic (its facts come from the bfbs, whose closure is
        # already declared via `_hermetic_srcs`).
        _wrapper_cmd_args.append("--root-bfbs=$(location :{}_bfbs)".format(name))

        _wrapper_cmd_args.append("--out=$@")

        # Tier-2 action schemas (via aorta_action_library) pass an explicit
        # action name so the generator emits the aggregate <Prefix>Action traits
        # struct without relying on the in-repo schemas/action/ path convention.
        if _action_name:
            _wrapper_cmd_args.append("--action-name=" + _action_name)

        native.genrule(
            name = _wrapper_gen_name,
            srcs = _hermetic_srcs + [":" + name + "_bfbs"],
            outs = [name + "_msg.h"],
            tools = [_GEN_MESSAGE_WRAPPER],
            cmd = " ".join(_wrapper_cmd_args),
        )
        _cc_hdrs.append(":" + _wrapper_gen_name)
        _cc_deps.append(_AORTA_CPP_HEADERS)

    # `includes = ["."]` exports the package dir so dependents can include
    # the generated headers by bare name. In the ROOT package (an external
    # consumer expanding the macro at its module root) "." would resolve to
    # the workspace root, which Bazel rejects outright — and is also
    # unnecessary there: a root-package generated header's bin-dir path IS
    # its bare name, so the default quote-include dirs already resolve it.
    native.cc_library(
        name = name + "_cc",
        hdrs = _cc_hdrs,
        includes = ["."] if native.package_name() else [],
        deps = _cc_deps,
        visibility = visibility,
    )
    
    # ========================================
    # 7. Python library (alias to generated code)
    # ========================================
    if not _cpp_only:
        native.alias(
            name = name + "_py",
            actual = ":" + name + "_py_gen",
            visibility = visibility,
        )

def _generate_rust_wrapper(name, msg_name, deps):
    """Generate Rust wrapper file that includes generated code and exports modules."""
    
    # Build include statements for dependencies
    dep_includes = []
    for dep in deps:
        dep_includes.append('#[path = "{}_generated.rs"]'.format(dep))
        dep_includes.append('pub mod {}_generated;'.format(dep))
    
    dep_include_str = "\n".join(dep_includes) if dep_includes else "// No dependencies"
    
    # Determine namespace from message name (e.g., "demo_chat" -> "demo")
    # This is a heuristic; adjust if your naming convention differs
    namespace_parts = msg_name.split("_")
    if len(namespace_parts) > 1:
        # Assume last part is message type, rest is namespace
        namespace = "_".join(namespace_parts[:-1])
    else:
        namespace = msg_name
    
    wrapper_content = '''//! {msg_name} message bindings (auto-generated by aorta_fbs_library)
//!
//! This file is automatically generated. Do not edit manually.

#![allow(dead_code, unused_imports, non_snake_case, clippy::all)]

// Include dependency modules
{dep_includes}

// Include this message's generated code
#[path = "{msg_name}_generated.rs"]
pub mod {msg_name}_generated;

// Include schema meta (fixed size hint)
#[path = "{msg_name}_schema_meta.rs"]
pub mod {msg_name}_schema_meta;

// Re-export commonly used modules for easier access
// Note: The exact module structure depends on your .fbs file's namespace
pub use {msg_name}_generated::*;

// Binary schema for runtime reflection
pub const {msg_name_upper}_BFBS: &[u8] = include_bytes!("{msg_name}.bfbs");

// Fixed-size hint (None if variable-size)
pub const {msg_name_upper}_FIXED_SIZE: Option<usize> = {msg_name}_schema_meta::AORTA_FIXED_SIZE;
'''.format(
        msg_name = msg_name,
        msg_name_upper = msg_name.upper(),
        dep_includes = dep_include_str,
    )
    
    native.genrule(
        name = name,
        outs = [msg_name + "_msg.rs"],
        cmd = "cat > $@ <<'EOF'\n{}\nEOF".format(wrapper_content),
    )

# ========================================
# Convenience macro for message with standard base header
# ========================================
def aorta_message(name, srcs, **kwargs):
    """Shorthand for creating a message that depends on aorta_base.

    Equivalent to:
        aorta_fbs_library(
            name = name,
            srcs = srcs,
            deps = [":aorta_base"],
            **kwargs
        )
    """
    aorta_fbs_library(
        name = name,
        srcs = srcs,
        deps = kwargs.pop("deps", []) + [":aorta_base"],
        **kwargs
    )

# ========================================
# Convenience macro for service schemas
# ========================================
def aorta_service_schema(name, srcs, **kwargs):
    """Shorthand for creating a service schema with typed C++ wrapper.

    Generates full C++ codegen including typed wrapper header ({name}_msg.h)
    with TypedPublishTraits/TypedSubscribeTraits specializations, ready for use
    with CreateServiceTypedView<Req, Resp>, TypedPublisher<T>, TypedSubscriber<T>.

    Example (external project):
        load("@aorta//message/tools:aorta_flatbuffers.bzl", "aorta_service_schema")

        aorta_service_schema(
            name = "micro_action",
            srcs = ["micro_action.fbs"],
            deps = ["@aorta//message/schemas/system:aorta_base"],
        )
        # Produces: micro_action_cc (cc_library)
        # Headers:  micro_action_generated.h
        #           micro_action_schema_meta.h
        #           micro_action_msg.h

    Args:
        name: Base name for all generated targets
        srcs: List containing a single .fbs schema file
        **kwargs: Additional arguments passed to aorta_fbs_library (deps, visibility, etc.)
    """
    aorta_fbs_library(
        name = name,
        srcs = srcs,
        cpp_wrapper = True,
        **kwargs
    )

# ========================================
# Convenience macro for action schemas
# ========================================
def aorta_action_library(
        name,
        srcs,
        action_name,
        deps = [],
        visibility = None,
        flatc_args = []):
    """Generate typed Aorta action bindings for a local (Tier-2) action schema.

    Unlike aorta_service_schema, an action needs an aggregate `<Prefix>Action`
    traits type (tying together the goal/result/feedback domain types and the
    8-table wrapper protocol) consumed by `ActionServer<Action>` and
    `node->CreateActionClient<Action>()`. This macro:

      1. Expands a *minimal* single-action schema -- exactly one `*Goal`,
         `*Result`, and `*FeedbackData` table plus
         `include "schemas/shared/action_common/action_common.fbs"` -- into the
         full action protocol via gen_action_wrapper (the same expansion the
         in-repo registry runs for schemas/action/**).
      2. Emits the aggregate `<Prefix>Action` C++ traits struct into
         `{name}_msg.h`, with `ActionName()` returning `action_name`.

    The keyexpr name is supplied explicitly here; it is not derived from a
    `schemas/action/` path, which only exists inside the aorta repo.

    Example (downstream project):
        load("@aorta//message/tools:aorta_flatbuffers.bzl", "aorta_action_library")

        aorta_action_library(
            name = "map_upload_action",
            srcs = ["map_upload.fbs"],   # MapUploadGoal/Result/FeedbackData only
            action_name = "slam/map_asset/upload",
            deps = ["@aorta//message/schemas/shared/action_common:action_common"],
        )
        # Produces: map_upload_action_cc (cc_library)
        # Header:   map_upload_action_msg.h -> aorta::action::<ns>::MapUploadAction

    Args:
        name: Base name for all generated targets.
        srcs: List containing a single minimal action .fbs schema file.
        action_name: Runtime action keyexpr name baked into ActionName()
                     (e.g. "slam/map_asset/upload"). Required, non-empty.
        deps: Schema dependencies; must include the action_common schema.
        visibility: Visibility for generated targets.
        flatc_args: Additional arguments to the flatc compiler.
    """
    if len(srcs) != 1:
        fail("aorta_action_library '{}' requires exactly one .fbs file".format(name))
    if not action_name:
        fail("aorta_action_library '{}' requires a non-empty action_name".format(name))

    # 1. Expand the minimal single-action schema into the full action protocol.
    #    aorta_fbs_library / flatc derive the generated .bfbs / _generated.h
    #    filenames from the .fbs file stem and declare genrule outputs as
    #    "<name>.*", so the expanded file's stem must equal `name`. Emit it into
    #    a private subdirectory so the basename can equal `name` without ever
    #    colliding with the user's source .fbs (mirrors the Tier-1 registry).
    _expanded_fbs = name + "_action_gen/" + name + ".fbs"
    native.genrule(
        name = name + "_action_expand",
        srcs = [srcs[0]],
        outs = [_expanded_fbs],
        tools = [_GEN_ACTION_WRAPPER],
        cmd = "$(location {}) --root-fbs $(location {}) --out $@".format(
            _GEN_ACTION_WRAPPER,
            srcs[0],
        ),
    )

    # 2. Generate FlatBuffers bindings + the aggregate action traits struct from
    #    the expanded schema, forcing emission via the explicit action_name.
    #    C++-only: the all-language Rust wrapper cannot express the external
    #    action_common dep the expanded schema requires, so Rust/Python surfaces
    #    are suppressed (Tier-2 Rust action bindings are a separate feature).
    aorta_fbs_library(
        name = name,
        srcs = [":" + _expanded_fbs],
        deps = deps,
        visibility = visibility,
        cpp_wrapper = True,
        flatc_args = flatc_args,
        _action_name = action_name,
        _cpp_only = True,
    )
