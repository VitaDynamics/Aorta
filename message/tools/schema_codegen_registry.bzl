"""`aorta_define_schema_package` for IN-REPO packages, with the registry graph walk.

This file exists purely to hold ONE `load()`: the one that reaches
`//message:schemas/registry.bzl`.

`schema_codegen.bzl` used to load it, for `collect_transitive_dep_fbs`. That put
the registry's fragment loads, including any separate product-schema module (for
example, `@product_schemas`), into the load graph of every package that loads the
codegen engine — including every SELF-HOSTED schema package in another module,
which has no aorta registry and must never need access to a product-schema
module to evaluate its own BUILD file. Splitting the walk out is what keeps
`aorta_schema_package`'s load graph free of product-schema modules.

Who loads what, after the split:

| caller | file | closure source |
|---|---|---|
| aorta's own 23 schema packages | THIS file | the registry graph walk over `SCHEMA_DEFINITIONS` |
| `aorta_external_schema_package` | `schema_codegen.bzl` | per-dep `_fbs_closure` filegroups |
| `aorta_schema_package` (self-hosted) | `schema_codegen.bzl` | per-dep `_fbs_closure` filegroups |

The symbol NAME is deliberately unchanged: `registry_lint.py:514` and the
bijection/sweep lints parse BUILD files for a call named
`aorta_define_schema_package`, and they key on the call, not on the load label.
"""

load("//message:schemas/registry.bzl", "collect_transitive_dep_fbs")
load(
    "//message/tools:schema_codegen.bzl",
    _aorta_define_schema_package = "aorta_define_schema_package",
)

def aorta_define_schema_package(
        schema_items,
        export_files = True,
        closure_srcs_by_stem = None,
        include_dir_groups_by_stem = None):
    """`schema_codegen.aorta_define_schema_package` with the registry walk bound.

    Behaviourally identical to the pre-split macro for every in-repo caller:
    the walk it injects IS `collect_transitive_dep_fbs`, over the complete
    `SCHEMA_DEFINITIONS` graph, exactly as before.
    """
    _aorta_define_schema_package(
        schema_items,
        export_files = export_files,
        closure_srcs_by_stem = closure_srcs_by_stem,
        include_dir_groups_by_stem = include_dir_groups_by_stem,
        closure_expander = collect_transitive_dep_fbs,
    )
