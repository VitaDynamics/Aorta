"""Helpers over Aorta's external-root allowlist.

The allowlist itself (`//message:schemas/external_roots.bzl`) is PURE DATA, in
exactly the same grammar as a schema fragment, so the lint can re-parse the
original text with the shared AST core instead of trusting an evaluated value.
The logic that reads it therefore lives here, next to the macro, and never in
the data file.

The allowlist does not manufacture canonical
release inputs. `//message:canonical_schema_source_*` takes only Aorta's own
`:schemas`; the former `internal_external_root_canonical_targets()` extension
point is gone with the production entries, so an external root — fixture or
otherwise — has no path into a base release artifact.
"""

load("//message:schemas/external_roots.bzl", "INTERNAL_EXTERNAL_SCHEMA_ROOTS")


def internal_external_root_entry(pkg, name):
    """The allowlist entry for `pkg:name`, or None when it is not declared."""
    for entry in INTERNAL_EXTERNAL_SCHEMA_ROOTS:
        if entry.pkg == pkg and entry.name == name:
            return entry
    return None

