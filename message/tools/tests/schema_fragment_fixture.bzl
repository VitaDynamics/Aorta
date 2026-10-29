"""Metadata-only fixtures: configure real output targets without running flatc."""

load("//message/tools:schema_fragment.bzl", _fragment = "aorta_schema_fragment")
load("//message/tools:schema_names.bzl", "schema_output_labels")

def fixture_fragment(name, items, **kwargs):
    for item in items:
        for label in schema_output_labels(item).values():
            target = label.split(":")[-1]
            if native.existing_rule(target) == None:
                native.filegroup(name = target, srcs = [])
    _fragment(name = name, items = items, **kwargs)
