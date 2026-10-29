"""Internal-class external schema root fixture — the pure-data fragment.

This stands in for a private schema source module's `registry:fragment_*.bzl`:
the same fragment grammar, `provenance = "internal"`, and a `source_package` that is
NOT the calling package (the shadow package under `message/schemas/**` is). The
mechanism under test is identical to the production cross-module path.
"""

_AORTA_BASE_FBS = "//message/schemas/system:aorta_base.fbs"
_PROBE_PKG = "//message/schemas/external_fixture/probe"

PROBE_SCHEMAS = [
    struct(
        pkg = _PROBE_PKG,
        stem = "probe_alpha",
        root_fbs = "probe_alpha.fbs",
        provenance = "internal",
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
    struct(
        pkg = _PROBE_PKG,
        stem = "probe_beta",
        root_fbs = "probe_beta.fbs",
        provenance = "internal",
        dep_fbs = [":probe_alpha.fbs"],
        cpp = True,
        python = True,
        rust = True,
        bfbs = True,
        schema_meta_cpp = True,
        schema_meta_rust = True,
        cpp_wrapper = True,
        rust_wrapper = True,
    ),
]
