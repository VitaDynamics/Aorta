"""Probe fragment — pure data, same grammar as a real schema module.

Every aorta dependency is REPOSITORY-QUALIFIED (`@aorta//...`), which is the
spelling a schema module in another Bazel module must adopt. Inside
aorta `@aorta` resolves to aorta itself, so this fixture exercises the label
STRING handling of every `dep_fbs` consumer without needing a second module;
the `external/<repo>/` PATH handling is covered separately by
`//message/schemas/service/peripheral`, whose sources really do come from
another module.
"""

_AORTA_BASE_FBS = "@aorta//message/schemas/system:aorta_base.fbs"
_PKG = "//message/tools/tests/schema_package_probe"

PROBE_SCHEMAS = [
    struct(
        pkg = _PKG,
        stem = "probe_reading",
        root_fbs = "probe_reading.fbs",
        schema_kind = "message",
        root_type = "probe.demo.ProbeReading",
        namespace = "probe.demo",
        file_identifier = "",
        provenance = "external",
        dep_fbs = [
            _AORTA_BASE_FBS,
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
        pkg = _PKG,
        stem = "probe_units",
        root_fbs = "probe_units.fbs",
        schema_kind = "include_only",
        root_type = "",
        namespace = "probe.demo",
        file_identifier = "",
        provenance = "external",
        cpp = True,
        python = True,
        rust = True,
    ),
]
