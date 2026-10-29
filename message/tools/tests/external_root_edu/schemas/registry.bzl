"""EDU external schema root fixture — the pure-data fragment.

This is the file an EDU user writes. It reuses the fragment grammar verbatim (a
module-level list of `struct(...)` literals, string constants declared first,
sorted by `(pkg, stem)`), and differs from Aorta's own fragments only in
policy: `provenance = "external"` everywhere, and `edu_public` — the EDU
schema pack's projection flag, owned by the schema module — is forbidden.

Every physical `.fbs` under the source root is registered, include-only files
included (`edu_demo_units`, which carries RAW codegen only and nothing that
presupposes a root type): there is no implicit exception table, so
"one physical file ↔ exactly one item" is the only rule.

Each item with a root type is first class in ALL THREE languages:
`cpp`/`python`/`rust` raw output, one `bfbs`, C++/Rust/Python
schema-meta, and the C++/Rust typed wrappers. There is no per-call `languages`
knob to opt out of one of them.
"""

# Deliberately TWO spellings: an EDU repo writes `@aorta//...` (its only
# option), and this fixture must actually exercise that engine branch —
# `_dep_suffixed_target`'s `@repo//pkg:x.fbs` -> `@repo//pkg:x_fbs_closure`
# mapping, the cross-module `_cpp_lib` dep and the `-I` derivation. The
# in-repo `//message/...` spelling stays on the action dep so both forms are
# covered by one fixture.
_AORTA_ACTION_COMMON_FBS = "//message/schemas/shared/action_common:action_common.fbs"
_AORTA_BASE_FBS = "@aorta//message/schemas/system:aorta_base.fbs"
_EDU_SCHEMAS_PKG = "//message/tools/tests/external_root_edu/schemas"

EDU_SCHEMAS = [
    struct(
        pkg = _EDU_SCHEMAS_PKG,
        stem = "edu_demo_job",
        root_fbs = "edu_demo_job.fbs",
        provenance = "external",
        dep_fbs = [
            _AORTA_ACTION_COMMON_FBS,
            _AORTA_BASE_FBS,
        ],
        action_schema = True,
        action_name = "edu/demo/job",
        cpp = True,
        python = True,
        rust = True,
        bfbs = True,
        schema_meta_cpp = True,
        schema_meta_rust = True,
        cpp_wrapper = True,
        rust_wrapper = True,
    ),
    struct(
        pkg = _EDU_SCHEMAS_PKG,
        stem = "edu_demo_service",
        root_fbs = "edu_demo_service.fbs",
        provenance = "external",
        dep_fbs = [
            _AORTA_BASE_FBS,
            ":edu_demo_units.fbs",
        ],
        cpp = True,
        python = True,
        rust = True,
        bfbs = True,
        schema_meta_cpp = True,
        schema_meta_rust = True,
        cpp_wrapper = True,
        rust_wrapper = True,
    ),
    struct(
        pkg = _EDU_SCHEMAS_PKG,
        stem = "edu_demo_text",
        root_fbs = "edu_demo_text.fbs",
        provenance = "external",
        dep_fbs = [_AORTA_BASE_FBS],
        cpp = True,
        python = True,
        rust = True,
        bfbs = True,
        schema_meta_cpp = True,
        schema_meta_rust = True,
        cpp_wrapper = True,
        rust_wrapper = True,
    ),
    # Include-only (no root type): no bfbs, no schema-meta, no typed wrapper —
    # but RAW codegen is on in every language, because `edu_demo_service`
    # includes it and flatc's generated header/module for the service includes
    # the generated header/module of every schema it includes. `rust = True` is
    # what puts `edu_demo_units_generated` into the service crate's module
    # closure, which the Rust wrapper's `#[path]` block names unconditionally.
    struct(
        pkg = _EDU_SCHEMAS_PKG,
        stem = "edu_demo_units",
        root_fbs = "edu_demo_units.fbs",
        provenance = "external",
        cpp = True,
        python = True,
        rust = True,
    ),
]
