"""The collapsed schema-package macro.

`aorta_external_schema_package` exists because a schema's SOURCE lived in one
repository and its CANONICAL SLOT in another, so every file had to be copied
into aorta and the copy had to be proved byte-identical to the source
(`source_package` / `source_inventory` / `expected_provenance` / the import
rule / the provenance lint). Reverse the dependency direction and that whole
apparatus has nothing left to do: the schema module owns the canonical slot,
`srcs` IS the canonical source, and the copy is the identity function.

What this macro is, therefore:

    aorta_schema_package(
        name = "peripheral",
        srcs = glob(["*.fbs"], allow_empty = False),
        items = [e for e in PRODUCT_SERVICE_SCHEMAS
                 if e.pkg == "//" + package_name()],
        api = 2,
    )

Removed relative to the external-root macro: `source_package`,
`source_inventory`, `source_prefix`, `expected_provenance`, the
`aorta_import_external_fbs` copy step and the `<name>_manifest` /
`<name>_lint_test` / `<name>_workspace_lint` provenance triple. The new
`<name>_lint_test` is a module-local integrity suite, not a copy-pair gate.

CAVEAT: only the source-vs-COPY claim disappears with the
copy. The removed lints also checked declared `dep_fbs` against the actual
`include` statements (`external_root_lint.py:574`), actual root types against
the typed/include-only declaration (`:664`), fragment text against fragment
value (`:1045`), and took an independent physical census of the source package
(`external_schema_root.bzl:960`). Text agreement and an independent physical
census are restored as execution tests, as are actual include/direct-edge checks.
The early loading check still compares caller-supplied literal `srcs` with
`items`; the census independently scans the whole package even when both caller
lists omit a source. Root-type checks are not restored yet.

Kept, unchanged and deliberately: the per-item `dep_fbs` closure mechanism.
`dep_fbs` does not only feed flatc `-I` — it drives the `_fbs_closure` and
`_rust_closure` filegroups, the per-dependency C++ libraries, the Python
input closure, BFBS staging and the typed wrappers. Replacing it
with a package-level `deps` that only gathers include directories would leave
every one of those wrong. What changes is only the SPELLING of a dep label: a
schema module writes `@aorta//message/schemas/system:aorta_base.fbs` where an
in-aorta fragment writes `//message/schemas/system:aorta_base.fbs`.

`pkg` convention: a fragment entry's `pkg` carries the `//` prefix
(`fragment_service.bzl:25` — `pkg = "//message/schemas/service/actmode"`),
while `native.package_name()` does not. The sample above is right to write
`"//" + package_name()`; the `e.pkg == package_name()` spelling matches ZERO
entries. This macro enforces the `//` form.
"""

load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_python//python:defs.bzl", "py_library")
load("@rules_rust//rust:defs.bzl", "rust_library")
load(
    "//message/tools:external_schema_root.bzl",
    "aorta_external_root_rust_facade_source",
    "validate_schema_items",
)
load("//message/tools:flatbuffers_rules.bzl", "python_msgs_tree")
load(
    "//message/tools:message_bindings.bzl",
    "aorta_cpp_message_wrapper",
    "aorta_rust_message_binding",
)
load("//message/tools:schema_lint_test.bzl", "schema_lint_test")
load("//message/tools:schema_lint.bzl", "schema_fragment_lint_test", "schema_package_census_lint_test", "schema_include_lint_test")
load("//message/tools:schema_codegen.bzl", "aorta_define_schema_package", "dep_fbs_closure_target")
load("//message/tools:schema_fragment.bzl", "aorta_schema_fragment", _FRAGMENT_RULES_API = "SCHEMA_RULES_API")
load(
    "//message/tools:schema_names.bzl",
    "item_get",
    "item_has",
    "sanitize_crate_name",
    "schema_cpp_binding_header",
    "schema_cpp_binding_name",
    "schema_cpp_lib_name",
    "schema_meta_py_enabled",
    "schema_rust_binding_name",
    "schema_rust_crate_name",
    "schema_rust_facade_name",
)

# Re-exported from here so a schema module can `load` the macro and the API
# number from ONE label; `schema_fragment.bzl` holds the definition because the
# L2 provider needs it too and must stay free of this file's rule loads.
SCHEMA_RULES_API = _FRAGMENT_RULES_API

_AORTA_CPP_HEADERS = Label("//cpp:aorta_cpp_headers")
_FLATBUFFERS_PY_DEP = Label("//message/tools:flatbuffers_py")

# `<name><suffix>` package-level targets this macro defines. Shorter than the
# external root's list by exactly the three provenance targets.
_PACKAGE_AGGREGATE_SUFFIXES = [
    "_cc",
    "_py",
    "_py_tree",
    "_rs",
    "_rs_src",
    "_canonical_fbs",
    "_fragment",
    "_fragment_lint_inputs",
    "_fragment_fragment_lint_test",
    "_fragment_include_lint_test",
    "_include_lint_test",
    "_source_lint_test",
    "_fragment_source_lint_test",
    "_fragment_lint_test",
    "_census_lint_test",
    "_census_lint_test_inputs",
    "_lint_test",
]

# Fields forwarded verbatim to the codegen engine. Copied by DECLARED-ness, not
# by value: `cc_alias = None` means "no `<stem>_cc` alias" and must not be
# rewritten into the default by a value-based copy.
_CODEGEN_FIELDS = [
    "stem",
    "dep_fbs",
    "rust",
    "cpp",
    "python",
    "bfbs",
    "schema_meta_rust",
    "schema_meta_cpp",
    "schema_meta_py",
    "python_include_dirs",
    "include_in_examples_bfbs",
    "cpp_lib",
    "cpp_lib_name",
    "cc_alias",
    "extra_cc_deps",
    "action_schema",
    "action_name",
]

def _codegen_item(item, root_label):
    """Registry entry -> the plain dict `aorta_define_schema_package` consumes.

    `root_fbs` is replaced by the RESOLVED label of the source file. In the
    steady state that is `<stem>.fbs`, a package-local source; the prototype
    shim (`src_package`) makes it a cross-module label instead, which is the
    same substitution a real consumer performs when it depends on a schema in
    another module.
    """
    out = {}
    for field in _CODEGEN_FIELDS:
        if item_has(item, field):
            out[field] = item_get(item, field)
    out["root_fbs"] = root_label
    return out

def aorta_schema_package(
        name,
        srcs,
        items,
        api,
        module_name,
        fragment = None,
        fragment_symbols = {},
        lint_dependencies = [],
        dep_fbs = None,
        src_package = None,
        visibility = None):
    """Generate a schema package's full binding surface from canonical sources.

    Args:
      name: package root name; prefix of every `<name>_*` aggregate and the
        crate name of the Rust facade.
      srcs: the canonical `.fbs` of this package (normally
        `glob(["*.fbs"], allow_empty = False)`). Declared so the package has a
        single source face; the per-item root label is derived from each
        entry's `root_fbs`.
      items: the fragment entries whose `pkg` is this package. Every entry's
        `pkg` must equal `"//" + native.package_name()`; a multi-package
        collection calls the macro once per package (`glob` does not cross a
        package boundary, so there is no recursive-discovery promise).
      api: the `SCHEMA_RULES_API` this module was written against. Compared at
        LOADING phase — a Git-pinned module is outside version resolution
        entirely, so this number, not `bazel_dep(version = ...)`,
        is the compatibility contract.
      module_name: explicit module display name for the automatically emitted L2 fragment.
      fragment: authored pure-data File(s); omitted declarations are generated.
      fragment_symbols: fragment label to list symbol, audited for this package.
      lint_dependencies: provider owners for include lint only, not codegen edges.
      dep_fbs: optional `{stem: [labels]}` override of the entries' own
        `dep_fbs`. Same authoring convention as the registry: DIRECT edges
        only, never an expanded closure.
      src_package: STAGE-1 PROTOTYPE SHIM, not part of the proposed API.
        Package label the `.fbs` actually live in when they are not yet in the
        calling package (`@product_schemas//message/schemas/...`).
        Stage 2 makes the schema module self-hosting and this argument goes
        away. It exists to prove the crux question — that a cross-module
        source label flows through the whole closure machinery — before the
        sources move.
      visibility: visibility of the public aggregates.
    """
    package_label = "//" + native.package_name()
    original_items = items
    effective_items = []
    for item in items:
        effective = dict(item) if type(item) == "dict" else {field: getattr(item, field) for field in dir(item) if field not in ["to_json", "to_proto"]}
        stem = item_get(item, "stem")
        if dep_fbs != None and stem in dep_fbs:
            effective["dep_fbs"] = dep_fbs[stem]
        effective_items.append(effective)
    items = effective_items

    if api != SCHEMA_RULES_API:
        fail(
            ("aorta_schema_package('{}') in {}: api = {} but this aorta provides " +
             "SCHEMA_RULES_API = {}").format(name, package_label, api, SCHEMA_RULES_API),
        )

    errors = []
    if not srcs:
        errors.append("srcs must be a non-empty list of canonical .fbs files")
    if not items:
        errors.append(
            ("items must be a non-empty list of fragment entries for this package. " +
             "Fragment entries carry a '//'-prefixed pkg, so the selector is " +
             "`e.pkg == \"//\" + package_name()` — `e.pkg == package_name()` " +
             "matches nothing."),
        )
    # Source <-> registry bijection. Removing the copy step removes the
    # source/copy inventory PAIR, not the obligation that every tracked `.fbs`
    # has exactly one entry and every entry has a file. When
    # `srcs` is a plain glob result this is decidable at loading phase, so it
    # is decided here rather than deferred to a lint.
    if src_package == None:
        declared = {}
        for item in items:
            declared[item_get(item, "root_fbs", "")] = item_get(item, "stem", "?")
        globbed = [s for s in srcs if type(s) == "string" and s.endswith(".fbs")]
        if len(globbed) == len(srcs):
            for src in globbed:
                if src not in declared:
                    errors.append(
                        ("source '{}' is in srcs but no item declares it; every " +
                         "canonical .fbs needs exactly one registry entry").format(src),
                    )
            for root_fbs in declared:
                if root_fbs not in globbed:
                    errors.append(
                        ("item '{}' declares root_fbs '{}' but no such file is in " +
                         "srcs").format(declared[root_fbs], root_fbs),
                    )

    for item in items:
        pkg = item_get(item, "pkg", None)
        if pkg != package_label:
            errors.append(
                ("item '{}' declares pkg '{}' but the macro expands in '{}' " +
                 "(entry pkg carries the '//' prefix)").format(
                    item_get(item, "stem", "<no stem>"),
                    pkg,
                    package_label,
                ),
            )

    # Same per-item and collision gate an external root is graded by; see
    # `external_schema_root.validate_schema_items`.
    validate_schema_items(
        name = name,
        items = items,
        package_label = package_label,
        errors = errors,
        expected_provenance = None,
        root_aggregate_suffixes = _PACKAGE_AGGREGATE_SUFFIXES,
        emits_imports = False,
    )
    if errors:
        fail(
            "aorta_schema_package('{}') in {}:\n  - {}".format(
                name,
                package_label,
                "\n  - ".join(errors),
            ),
        )

    # Steady state: the sources are package-local, so the root is the bare
    # `<stem>.fbs` an in-repo package already uses — `exports_files` and the
    # generated `outs` both require a bare name, not a label. Only the shim
    # makes the root a cross-package label.
    def _root_label(root_fbs):
        if src_package:
            return "{}:{}".format(src_package, root_fbs)
        return root_fbs

    def _root_dep_label(root_fbs):
        if src_package:
            return "{}:{}".format(src_package, root_fbs)
        return ":" + root_fbs

    codegen_items = []
    closure_srcs_by_stem = {}
    include_dir_groups_by_stem = {}
    canonical_labels = []
    py_trees = []
    py_metas = []
    cc_targets = []
    rs_facade_entries = []

    for item in items:
        stem = item_get(item, "stem")
        root_fbs = item_get(item, "root_fbs")
        root_label = _root_label(root_fbs)
        canonical_labels.append(_root_dep_label(root_fbs))

        deps = item_get(item, "dep_fbs", [])

        # THE mechanism this macro keeps. Each DIRECT edge
        # contributes its own `<dep>_fbs_closure` filegroup, which already
        # carries that dep's grandchildren, so transitivity is computed by
        # BAZEL's graph rather than by walking aorta's registry — the only
        # formulation that crosses a module boundary at all, since a registry
        # walk is blind to another module's schemas. The direct edges stay in
        # the list because they also drive flatc's `-I` derivation through
        # `$(location <dep>)`, which needs each direct `.fbs` to be a declared
        # input in its own right.
        closure_targets = [dep_fbs_closure_target(dep) for dep in deps]
        closure_srcs_by_stem[stem] = deps + closure_targets
        include_dir_groups_by_stem[stem] = closure_targets

        codegen = _codegen_item(item, root_label)
        codegen["dep_fbs"] = deps
        codegen_items.append(codegen)

        if item_get(item, "python", False):
            py_trees.append(":{}_py".format(stem))
        if schema_meta_py_enabled(item):
            py_metas.append(":{}_schema_meta_py".format(stem))
        if item_get(item, "bfbs", False):
            if item_get(item, "cpp_wrapper", False):
                cc_targets.append(":" + schema_cpp_binding_name(item))
            if item_get(item, "rust_wrapper", False):
                rs_facade_entries.append((
                    schema_rust_facade_name(item),
                    schema_rust_crate_name(item),
                    schema_rust_binding_name(item),
                ))
        elif item_get(item, "cpp", False):
            # Raw-only (include-only) item: still export its generated header
            # so `:<name>_cc` is the package's complete C++ surface.
            cc_targets.append(":" + schema_cpp_lib_name(item))

    aorta_define_schema_package(
        codegen_items,
        # The `.fbs` are already the canonical sources, so they are exported
        # for cross-package `dep_fbs` edges — except under the prototype shim,
        # where they belong to another module and `exports_files` on a
        # cross-module label is a load error.
        export_files = src_package == None,
        closure_srcs_by_stem = closure_srcs_by_stem,
        include_dir_groups_by_stem = include_dir_groups_by_stem,
    )

    for item in items:
        if not item_get(item, "bfbs", False):
            continue
        stem = item_get(item, "stem")
        root_fbs_label = _root_dep_label(item_get(item, "root_fbs"))
        if item_get(item, "action_schema", False):
            root_fbs_label = ":{}_action_schema".format(stem)

        if item_get(item, "cpp_wrapper", False):
            aorta_cpp_message_wrapper(
                name = schema_cpp_binding_name(item),
                schema_pkg = package_label,
                schema_stem = stem,
                schema_cc_target = ":" + schema_cpp_lib_name(item),
                wrapper_header = schema_cpp_binding_header(item),
                root_fbs_label = root_fbs_label,
                # BOTH the declared action input set AND the `--include-fbs`
                # resolution manifest — the only spelling that works when an
                # include lives in another Bzlmod module's sandboxed tree.
                schema_dep_filegroups = [":{}_fbs_closure".format(stem)],
                schema_rel = "",
                action_name = item_get(item, "action_name", None),
                visibility = visibility,
            )

        if not item_get(item, "rust_wrapper", False):
            continue

        aorta_rust_message_binding(
            name = schema_rust_binding_name(item),
            crate_name = schema_rust_crate_name(item),
            schema_pkg = package_label,
            schema_stem = stem,
            include_schema_meta = item_get(item, "schema_meta_rust", False),
            root_fbs_label = root_fbs_label,
            rust_module_filegroups = [":{}_rust_closure".format(stem)],
            schema_dep_filegroups = [":{}_fbs_closure".format(stem)],
            schema_rel = "",
            action_name = item_get(item, "action_name", None),
            visibility = visibility,
        )

    facade_lines = [
        "//! Rust facade for the schema package `{}` (generated).".format(name),
        "//!",
        "//! Each schema is re-exported as a submodule named after its message",
        "//! base, so `<package>::<message base>::schema::<Type>Schema` is the",
        "//! stable path. Deliberately NOT a flattened namespace.",
        "",
    ]
    for facade_module_name, crate_name, _target in rs_facade_entries:
        if facade_module_name == crate_name:
            facade_lines.append("pub use ::{};".format(crate_name))
        else:
            facade_lines.append("pub use ::{} as {};".format(crate_name, facade_module_name))
    aorta_external_root_rust_facade_source(
        name = name + "_rs_src",
        out = name + "_rs.rs",
        content = "\n".join(facade_lines) + "\n",
    )
    rust_library(
        name = name + "_rs",
        srcs = [":" + name + "_rs_src"],
        crate_root = ":" + name + "_rs_src",
        crate_name = sanitize_crate_name(name),
        edition = "2021",
        deps = [":" + target for _module, _crate, target in rs_facade_entries],
        visibility = visibility,
    )

    # Retained under its historical name so `//message:canonical_schema_source_*`
    # (which builds `<pkg>:<name>_canonical_fbs` from the internal-root ledger)
    # still resolves while the ledger exists. With no copy step it is simply
    # the canonical sources.
    native.filegroup(
        name = name + "_canonical_fbs",
        srcs = canonical_labels,
        visibility = visibility,
    )

    cc_library(
        name = name + "_cc",
        deps = cc_targets + [_AORTA_CPP_HEADERS],
        visibility = visibility,
    )

    # Two layers on purpose: a merged importable tree plus the `py_library`
    # that actually carries `PyInfo`. A tree rule returns only `DefaultInfo`,
    # so "the tree built" is not the same claim as "a consumer can import it".
    python_msgs_tree(
        name = name + "_py_tree",
        trees = py_trees,
        metas = py_metas,
        visibility = visibility,
    )

    py_library(
        name = name + "_py",
        data = [":" + name + "_py_tree"],
        imports = [name + "_py_tree"],
        deps = [_FLATBUFFERS_PY_DEP],
        visibility = visibility,
    )

    aorta_schema_fragment(
        name = name + "_fragment",
        api = api,
        items = items,
        module_name = module_name,
        collection_name = name,
        original_items = original_items,
        dep_fbs_overrides = dep_fbs,
        fragment = fragment,
        fragment_symbols = fragment_symbols,
        lint_dependencies = lint_dependencies,
        fragment_packages = [package_label],
        srcs = {(_root_dep_label(item_get(item, "root_fbs"))): "{}|{}".format(package_label, item_get(item, "stem")) for item in items},
        facade_targets = {package_label: ":" + name + "_rs"},
        visibility = visibility,
    )
    schema_fragment_lint_test(name = name + "_fragment_lint_test", audit = ":" + name + "_fragment_lint_inputs", visibility = visibility)
    schema_package_census_lint_test(
        name = name + "_census_lint_test",
        fragment = ":" + name + "_fragment",
        census = native.glob(["**/*.fbs"], allow_empty = True),
        visibility = visibility,
    )
    schema_include_lint_test(name = name + "_include_lint_test", audit = ":" + name + "_fragment_lint_inputs", visibility = visibility)
    schema_lint_test(name = name + "_source_lint_test", audit = ":" + name + "_fragment_lint_inputs", mode = "source", visibility = visibility)
    native.test_suite(name = name + "_lint_test", tests = [":" + name + "_fragment_lint_test", ":" + name + "_census_lint_test", ":" + name + "_include_lint_test", ":" + name + "_source_lint_test"], visibility = visibility)
