"""Public-mirror schema registry derived from the OSS projection."""

load("//message:schemas/registry_local.bzl", "LOCAL_SCHEMA_DEFINITIONS")
load("//message/tools:schema_names.bzl", _schema_meta_py_enabled = "schema_meta_py_enabled")

_FBS_SCHEMA_ALIAS_PREFIX = "fbs_schema_"

SCHEMA_DEFINITIONS = LOCAL_SCHEMA_DEFINITIONS

def _dedupe(items):
    seen = {}
    deduped = []
    for item in items:
        if item in seen:
            continue
        seen[item] = True
        deduped.append(item)
    return deduped


def _dedupe_aliases(items):
    seen = {}
    deduped = []
    for name, actual in items:
        if name in seen:
            fail(
                "duplicate alias '{}' detected: '{}' vs '{}'".format(
                    name,
                    seen[name],
                    actual,
                ),
            )
        seen[name] = actual
        deduped.append((name, actual))
    return deduped


def _is_cpp_lib_enabled(item):
    return getattr(item, "cpp_lib", getattr(item, "cpp", False))


# Shared with the L2 fragment provider and binding generators.
schema_meta_py_enabled = _schema_meta_py_enabled


def _cpp_lib_target(item):
    return getattr(item, "cpp_lib_target", getattr(item, "cpp_lib_name", item.stem + "_cpp_lib"))


def _cc_target(item):
    if hasattr(item, "cc_target"):
        return item.cc_target
    if hasattr(item, "cc_alias"):
        return item.cc_alias
    if _is_cpp_lib_enabled(item):
        return item.stem + "_cc"
    return None


def _schema_alias_id(item):
    return getattr(item, "alias_id", item.stem)


def _schema_target(pkg, target):
    return "{}:{}".format(pkg, target)


def _schema_generated_target(pkg, stem, suffix):
    return _schema_target(pkg, "{}_{}".format(stem, suffix))


def schema_items_for_package(pkg):
    return [item for item in SCHEMA_DEFINITIONS if item.pkg == pkg]


SCHEMA_PACKAGES = _dedupe([item.pkg for item in SCHEMA_DEFINITIONS])

SCHEMA_ALL_FBS_TARGETS = [
    "{}:all_fbs".format(pkg)
    for pkg in SCHEMA_PACKAGES
]

SCHEMA_SOURCE_FBS_TARGETS = [
    "{}:all_source_fbs".format(pkg)
    for pkg in SCHEMA_PACKAGES
]

SCHEMA_BUILD_FILE_TARGETS = [
    "{}:build_file".format(pkg)
    for pkg in SCHEMA_PACKAGES
]

# Package-level aggregate exports. Each schema package should expose these
# filegroups so root aggregation does not need per-schema updates.
SCHEMA_RUST_GENERATED_TARGETS = [
    "{}:all_rust".format(pkg)
    for pkg in SCHEMA_PACKAGES
]

SCHEMA_CPP_GENERATED_TARGETS = [
    "{}:all_cpp".format(pkg)
    for pkg in SCHEMA_PACKAGES
]

SCHEMA_PYTHON_GENERATED_TARGETS = [
    "{}:all_python".format(pkg)
    for pkg in SCHEMA_PACKAGES
]

SCHEMA_BFBS_TARGETS = [
    "{}:all_bfbs".format(pkg)
    for pkg in SCHEMA_PACKAGES
]

SCHEMA_META_RUST_TARGETS = [
    "{}:all_schema_meta_rust".format(pkg)
    for pkg in SCHEMA_PACKAGES
]

SCHEMA_META_CPP_TARGETS = [
    "{}:all_schema_meta_cpp".format(pkg)
    for pkg in SCHEMA_PACKAGES
]

# Only packages with at least one schema-meta-py item; an empty `all_schema_meta_py`
# filegroup would fail the wheel's `.py`-typed `metas` attribute.
SCHEMA_META_PY_TARGETS = [
    "{}:all_schema_meta_py".format(pkg)
    for pkg in _dedupe([
        item.pkg
        for item in SCHEMA_DEFINITIONS
        if schema_meta_py_enabled(item)
    ])
]

SCHEMA_EXAMPLES_BFBS_TARGETS = [
    "{}:examples_bfbs".format(pkg)
    for pkg in SCHEMA_PACKAGES
]


def _append_output_aliases(aliases, item, attr, suffix):
    if not getattr(item, attr, False):
        return

    alias_id = _schema_alias_id(item)
    target = _schema_generated_target(item.pkg, item.stem, suffix)
    aliases.append(
        ("{}{}_{}".format(_FBS_SCHEMA_ALIAS_PREFIX, alias_id, suffix), target),
    )


def _build_schema_output_aliases():
    aliases = []
    for item in SCHEMA_DEFINITIONS:
        _append_output_aliases(aliases, item, "rust", "rust")
        _append_output_aliases(aliases, item, "cpp", "cpp")
        _append_output_aliases(aliases, item, "python", "py")
        _append_output_aliases(aliases, item, "bfbs", "bfbs")
        _append_output_aliases(aliases, item, "schema_meta_rust", "schema_meta")
        _append_output_aliases(aliases, item, "schema_meta_cpp", "schema_meta_cpp")

        if _is_cpp_lib_enabled(item):
            alias_id = _schema_alias_id(item)
            aliases.append(
                (
                    "{}{}_cpp_lib".format(_FBS_SCHEMA_ALIAS_PREFIX, alias_id),
                    _schema_target(item.pkg, _cpp_lib_target(item)),
                ),
            )

        cc_target = _cc_target(item)
        if cc_target != None:
            alias_id = _schema_alias_id(item)
            aliases.append(
                (
                    "{}{}_cc".format(_FBS_SCHEMA_ALIAS_PREFIX, alias_id),
                    _schema_target(item.pkg, cc_target),
                ),
            )

    return _dedupe_aliases(aliases)


def _build_schema_package_aliases():
    aliases = []
    seen = {}
    for item in SCHEMA_DEFINITIONS:
        if not _is_cpp_lib_enabled(item):
            continue
        cc_target = _cc_target(item)
        cpp_lib_target = _cpp_lib_target(item)
        if cc_target == None or cc_target == cpp_lib_target:
            continue

        key = "{}:{}".format(item.pkg, cc_target)
        actual = ":{}".format(cpp_lib_target)
        if key in seen:
            fail(
                "duplicate package alias '{}': '{}' vs '{}'".format(
                    key,
                    seen[key],
                    actual,
                ),
            )
        seen[key] = actual
        aliases.append(
            struct(
                pkg = item.pkg,
                name = cc_target,
                actual = actual,
            ),
        )
    return aliases


# Generated from `SCHEMA_DEFINITIONS`.
# Naming: `fbs_schema_<schema_id>_<artifact>`.
SCHEMA_OUTPUT_ALIASES = _build_schema_output_aliases()
SCHEMA_PACKAGE_ALIASES = _build_schema_package_aliases()


def _wrapper_msg_base_name(item):
    name = _schema_alias_id(item)
    if name.endswith("_msg"):
        return name
    return name + "_msg"


def _rust_binding_name(item):
    return getattr(item, "rust_wrapper_name", _wrapper_msg_base_name(item) + "_rs")


def _rust_binding_crate_name(item):
    return getattr(item, "rust_wrapper_crate_name", _wrapper_msg_base_name(item))


def _rust_binding_include_schema_meta(item):
    if hasattr(item, "rust_wrapper_include_schema_meta"):
        return item.rust_wrapper_include_schema_meta
    return getattr(item, "schema_meta_rust", False)


def _wrapper_root_fbs_label(item):
    if getattr(item, "action_schema", False):
        return "{}:{}_action_schema".format(item.pkg, item.stem)
    return "{}:{}".format(item.pkg, item.root_fbs)


def _wrapper_include_graph_root_fbs_label(item):
    return "{}:{}".format(item.pkg, item.root_fbs)


def _schema_fbs_label(item):
    return "{}:{}".format(item.pkg, item.root_fbs)


def _normalize_dep_fbs_label(owner_item, dep_fbs):
    # A repository-qualified dep is already absolute. Without this branch it
    # fell through to the bare-name case and produced the nonsense label
    # `//owner/pkg:@repo//dep/pkg:x.fbs`, which silently made the dep
    # unresolvable in the registry graph walk (so `collect_transitive_dep_fbs`
    # and `_collect_transitive_rust_module_targets` quietly dropped it).
    if dep_fbs.startswith("@"):
        return dep_fbs
    if dep_fbs.startswith("//"):
        return dep_fbs
    if dep_fbs.startswith(":"):
        return owner_item.pkg + dep_fbs
    return "{}:{}".format(owner_item.pkg, dep_fbs)


def _schema_item_by_fbs():
    mapping = {}
    for item in SCHEMA_DEFINITIONS:
        mapping[_schema_fbs_label(item)] = item
    return mapping


def _collect_transitive_rust_module_targets(item, item_by_fbs):
    labels = []
    seen_labels = {}
    visited = {}
    frontier = {_schema_fbs_label(item): True}

    for _ in range(len(SCHEMA_DEFINITIONS)):
        progressed = False
        for current in SCHEMA_DEFINITIONS:
            current_fbs = _schema_fbs_label(current)
            if current_fbs not in frontier or current_fbs in visited:
                continue
            visited[current_fbs] = True
            progressed = True

            if getattr(current, "rust", False):
                label = "{}:{}_rust".format(current.pkg, current.stem)
                if label not in seen_labels:
                    seen_labels[label] = True
                    labels.append(label)

            for dep_fbs in getattr(current, "dep_fbs", []):
                dep_item = item_by_fbs.get(_normalize_dep_fbs_label(current, dep_fbs))
                if dep_item != None:
                    frontier[_schema_fbs_label(dep_item)] = True

        if not progressed:
            break

    return labels


def _collect_transitive_dep_fbs(item, item_by_fbs):
    """Transitive `.fbs` closure of `item.dep_fbs`, including indirect includes.

    Used as `schema_deps` for the wrapper genrules so action keys depend on
    every schema gen_message_wrapper parses. Critical on Bazel < 8 where
    `native.read` is unavailable and the auto-detect path in
    `message_bindings.bzl` is a no-op.
    """
    labels = []
    seen = {}
    frontier = []
    for dep in getattr(item, "dep_fbs", []):
        normalized = _normalize_dep_fbs_label(item, dep)
        if normalized not in seen:
            seen[normalized] = True
            labels.append(normalized)
            frontier.append(item_by_fbs.get(normalized))

    for _ in range(len(SCHEMA_DEFINITIONS)):
        if len(frontier) == 0:
            break
        next_frontier = []
        for dep_item in frontier:
            if dep_item == None:
                continue
            for dep in getattr(dep_item, "dep_fbs", []):
                normalized = _normalize_dep_fbs_label(dep_item, dep)
                if normalized in seen:
                    continue
                seen[normalized] = True
                labels.append(normalized)
                next_frontier.append(item_by_fbs.get(normalized))
        frontier = next_frontier

    return labels


def collect_transitive_dep_fbs(item):
    """Public wrapper around `_collect_transitive_dep_fbs` for the FULL registry graph.

    Codegen redesign Phase 1 (§4.4 F2 fix): `aorta_define_schema_package`
    (schema_codegen.bzl) calls this to pass the registry's own transitive
    `.fbs` closure — not just `item.dep_fbs`'s direct edges — as the `srcs`
    for every flatc/bfbs/schema-meta genrule, so those actions declare their
    full include closure as Bazel inputs (hermetic action keys; no more
    stale `SCHEMA_HASH_HEX` when a grandchild include changes). The registry
    authoring convention (`dep_fbs` = direct edges only) is unchanged; this
    function is the single place the expansion happens, over the complete
    `SCHEMA_DEFINITIONS` graph regardless of which package subset the caller
    was constructed from.
    """
    return _collect_transitive_dep_fbs(item, _schema_item_by_fbs())

def _build_rust_message_bindings():
    bindings = []
    item_by_fbs = _schema_item_by_fbs()
    for item in SCHEMA_DEFINITIONS:
        if not getattr(item, "rust_wrapper", False):
            continue
        if not getattr(item, "rust", False):
            fail("schema '{}' enables rust_wrapper but rust=False".format(item.stem))
        if not getattr(item, "bfbs", False):
            # Codegen redesign Phase 2D: gen_message_wrapper.py's --root-bfbs
            # is required (the compiled bfbs is the wrapper emitters' only
            # semantic source) — every rust_wrapper=True item needs its own
            # `bfbs` target, unconditionally now, not just when a shadow-check
            # opted in. Fails here at analysis time with a clear message,
            # rather than a missing-label error from
            # aorta_rust_message_binding's genrule.
            fail("schema '{}' enables rust_wrapper but bfbs=False".format(item.stem))
        rust_module_targets = [
            "//message/schemas/system:aorta_base_rust",
        ]
        rust_module_targets.extend(
            _collect_transitive_rust_module_targets(item, item_by_fbs),
        )
        bindings.append(
            struct(
                name = _rust_binding_name(item),
                crate_name = _rust_binding_crate_name(item),
                schema_pkg = item.pkg,
                schema_stem = item.stem,
                include_schema_meta = _rust_binding_include_schema_meta(item),
                root_fbs_label = _wrapper_root_fbs_label(item),
                include_graph_root_fbs_label = _wrapper_include_graph_root_fbs_label(item),
                rust_module_targets = rust_module_targets,
                deps = getattr(item, "rust_wrapper_deps", None),
                extra_compile_data = getattr(item, "rust_wrapper_extra_compile_data", None),
                schema_deps = _collect_transitive_dep_fbs(item, item_by_fbs),
            ),
        )
    return bindings


def _cpp_binding_name(item):
    return getattr(item, "cpp_wrapper_name", _wrapper_msg_base_name(item) + "_cc")


def _cpp_binding_header(item):
    return getattr(item, "cpp_wrapper_header", _wrapper_msg_base_name(item) + ".h")


def _cpp_binding_schema_cc_target(item):
    default_target = "//message:{}{}_cc".format(
        _FBS_SCHEMA_ALIAS_PREFIX,
        _schema_alias_id(item),
    )
    return getattr(item, "cpp_wrapper_schema_cc_target", default_target)


def _build_cpp_message_bindings():
    bindings = []
    item_by_fbs = _schema_item_by_fbs()
    for item in SCHEMA_DEFINITIONS:
        if not getattr(item, "cpp_wrapper", False):
            continue
        if not getattr(item, "cpp", False):
            fail("schema '{}' enables cpp_wrapper but cpp=False".format(item.stem))
        if not getattr(item, "schema_meta_cpp", False):
            fail(
                "schema '{}' enables cpp_wrapper but schema_meta_cpp=False".format(item.stem),
            )
        if not getattr(item, "bfbs", False):
            # Codegen redesign Phase 2D — see the matching comment in
            # _build_rust_message_bindings.
            fail("schema '{}' enables cpp_wrapper but bfbs=False".format(item.stem))
        bindings.append(
            struct(
                name = _cpp_binding_name(item),
                schema_pkg = item.pkg,
                schema_stem = item.stem,
                schema_cc_target = _cpp_binding_schema_cc_target(item),
                wrapper_header = _cpp_binding_header(item),
                root_fbs_label = _wrapper_root_fbs_label(item),
                schema_deps = _collect_transitive_dep_fbs(item, item_by_fbs),
            ),
        )
    return bindings


RUST_MESSAGE_BINDINGS = _build_rust_message_bindings()
CPP_MESSAGE_BINDINGS = _build_cpp_message_bindings()


def _binding_actual(lang, name):
    return "//message/bindings/{}:{}".format(lang, name)


MESSAGE_BINDING_ALIASES = (
    [(item.name, _binding_actual("rust", item.name)) for item in RUST_MESSAGE_BINDINGS] +
    [(item.name, _binding_actual("cpp", item.name)) for item in CPP_MESSAGE_BINDINGS]
)
