"""Aorta-owned test-only external schema root."""

INTERNAL_EXTERNAL_SCHEMA_ROOTS = [
    struct(
        pkg = "//message/schemas/external_fixture/probe",
        name = "probe",
        source_package = "//message/tools/tests/external_root_internal",
        test_only = True,
    ),
]
