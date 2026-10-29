#!/usr/bin/env python3
"""Perturbation test for canonical emission order (loader path).

`gen_message_wrapper.py`'s output order used to
follow source-DFS include order and dict-insertion order. This test proves
the canonicalized generator no longer does: reordering a fixture schema's
`include` statements and its TOP-LEVEL type declarations (tables/enums —
never fields within a table, which would change implicit field ids / wire
layout, and is explicitly out of scope) must yield
byte-identical Rust and C++ wrapper output.

Ported to the loader path (`--root-bfbs` is now
required; the regex/text scanners this test originally exercised via
`gmw.write_rust`/`gmw.write_cpp` directly on dynamically-generated fixture
TEXT are gone). The ORIGINAL/PERTURBED fixture schemas now live as real,
flatc-COMPILABLE files under `tests/canonical_order_fixtures/` (genrules in
BUILD.bazel compile each to a `.bfbs`, same pattern as
`wrapper_loader_fixture_*`) rather than in-test Python string constants: the
original constants referenced `IncATable`/`IncBTable` UNQUALIFIED across a
namespace boundary, which real flatc rejects (cross-namespace field types
must be fully qualified) — the old text-only path never actually validated
this, since the regex scanners never ran the schema through flatc at all.
The fixture content is otherwise unchanged: same two variants (original /
perturbed), same include and top-level-declaration reordering, same
RootHeaderA/RootHeaderB pair for the `typed_tables`-order pin below.

The red run is not re-run here since the pre-canonicalization generator no
longer exists in the tree. `RootFoo`/`RootBar` carry no
`aorta_header` field, so the fixture never exercised `typed_tables`'s own
ordering (the list driving Rust `HeaderInjector`/`TypedPublishMessage` impl
emission and C++ `TypedPublishTraits<T>` specialization emission) —
`RootHeaderA`/`RootHeaderB` (both carrying `aorta_header`) close that
coverage gap: declared A-then-B in the original variant and B-then-A in the
perturbed variant, so a pre-fix generator would emit their
`HeaderInjector`/`TypedPublishTraits` impls in different relative order
between the two variants, while the canonical-order fix always emits them in
qualified-name order (`RootHeaderA` before `RootHeaderB`) regardless of
source order. This property is guaranteed by construction under the loader
path too: `collect_table_defs_from_loaded` explicitly sorts by
`(namespace_full, name)` (see gen_message_wrapper.py) independent of
declaration order or the bfbs's own internal object order — this test pins
the ACTUAL EMITTED ORDER end to end (through `write_rust`/`write_cpp`, not
just the fact-collection layer) via two independently-compiled bfbs.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import gen_message_wrapper as gmw  # noqa: E402
import schema_reflection as sr  # noqa: E402

_FIXTURE_DIR = "message/tools/tests/canonical_order_fixtures"


def _repo_root() -> Path:
    srcdir = os.environ.get("TEST_SRCDIR")
    workspace = os.environ.get("TEST_WORKSPACE")
    if srcdir and workspace:
        candidate = Path(srcdir) / workspace
        if (candidate / _FIXTURE_DIR).exists():
            return candidate
    return Path(__file__).resolve().parent.parent.parent


def _schema_rel(variant: str) -> str:
    # Both variants' schema_rel includes their own `original`/`perturbed`
    # subdirectory (see the genrule comment in BUILD.bazel for why the
    # ROOT FILENAME itself must be identical -- `root.fbs` -- across variants).
    return "tools/tests/canonical_order_fixtures/{}".format(variant)


def _root_fbs(variant: str) -> Path:
    return _repo_root() / _FIXTURE_DIR / variant / "root.fbs"


def _root_bfbs(variant: str) -> Path:
    return _repo_root() / _FIXTURE_DIR / variant / "root.bfbs"


def _load(variant: str) -> None:
    """Set `gmw._LOADED_SCHEMA` from this variant's compiled bfbs -- the
    module-global `write_rust`/`write_cpp` read, exactly as `main()` sets it
    from the required `--root-bfbs`."""
    gmw._LOADED_SCHEMA = sr.load_bfbs_file(str(_root_bfbs(variant)))


def _normalize_variant_dir(text: str) -> str:
    """Neutralize the ONE expected, ordering-UNRELATED difference between the
    two variants' Rust output: the `#[path = "..."]` include-module strings
    (`collect_include_generated_modules`) embed the schema's location
    RELATIVE TO THE MESSAGE ROOT, which genuinely differs here because the
    two variants are checked-in files that must live in DIFFERENT real
    directories (`original/` vs `perturbed/`) -- unlike the original
    (text-parser) version of this test, whose fixtures lived under two
    unrelated OS tmpdirs that coincidentally shared the exact same
    `schemas/topic/example/...` relative shape. This substitution collapses
    that one, expected, non-ordering difference so the comparison isolates
    what this test actually checks: declaration/include ORDER."""
    return text.replace(
        "canonical_order_fixtures/original/", "canonical_order_fixtures/VARIANT/"
    ).replace("canonical_order_fixtures/perturbed/", "canonical_order_fixtures/VARIANT/")


class CanonicalOrderPerturbationTest(unittest.TestCase):
    def test_rust_output_is_byte_identical_under_reordering(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            out_original = Path(tmp) / "out_original.rs"
            out_perturbed = Path(tmp) / "out_perturbed.rs"

            _load("original")
            gmw.write_rust(
                out=str(out_original),
                stem="root",
                schema_rel=_schema_rel("original"),
                root_fbs=str(_root_fbs("original")),
                with_schema_meta=False,
            )
            _load("perturbed")
            gmw.write_rust(
                out=str(out_perturbed),
                stem="root",  # same stem: isolates ordering, not naming
                schema_rel=_schema_rel("perturbed"),
                root_fbs=str(_root_fbs("perturbed")),
                with_schema_meta=False,
            )

            text_original = out_original.read_text(encoding="utf-8")
            text_perturbed = out_perturbed.read_text(encoding="utf-8")
            self.assertEqual(
                _normalize_variant_dir(text_original),
                _normalize_variant_dir(text_perturbed),
                "canonical Rust wrapper output must be byte-identical regardless "
                "of include/top-level-declaration order in the source .fbs",
            )
            # Pin down the actual canonical order, not just "both variants
            # agree with each other" (which a differently-broken-but-consistent
            # scheme could also satisfy by coincidence). RootHeaderA/RootHeaderB
            # share a namespace, so canonical (namespace, name) order puts A's
            # HeaderInjector/TypedPublishMessage impl block strictly before B's.
            pos_a = text_original.index("HeaderInjector for schema::RootHeaderASchema")
            pos_b = text_original.index("HeaderInjector for schema::RootHeaderBSchema")
            self.assertLess(
                pos_a,
                pos_b,
                "typed_tables must emit RootHeaderA's impls before RootHeaderB's "
                "(canonical qualified-name order), regardless of source order",
            )

    def test_cpp_output_is_byte_identical_under_reordering(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            out_original = Path(tmp) / "out_original.h"
            out_perturbed = Path(tmp) / "out_perturbed.h"

            _load("original")
            gmw.write_cpp(
                out=str(out_original),
                stem="root",
                schema_rel=_schema_rel("original"),
                root_fbs=str(_root_fbs("original")),
            )
            _load("perturbed")
            gmw.write_cpp(
                out=str(out_perturbed),
                stem="root",
                schema_rel=_schema_rel("perturbed"),
                root_fbs=str(_root_fbs("perturbed")),
            )

            text_original = out_original.read_text(encoding="utf-8")
            text_perturbed = out_perturbed.read_text(encoding="utf-8")
            self.assertEqual(
                _normalize_variant_dir(text_original),
                _normalize_variant_dir(text_perturbed),
                "canonical C++ wrapper output must be byte-identical regardless "
                "of include/top-level-declaration order in the source .fbs",
            )
            # Same canonical-order pin as the Rust test above, for C++
            # TypedPublishTraits<T> specialization emission order.
            pos_a = text_original.index("TypedPublishTraits<::perturb::root::RootHeaderA>")
            pos_b = text_original.index("TypedPublishTraits<::perturb::root::RootHeaderB>")
            self.assertLess(
                pos_a,
                pos_b,
                "typed_tables must emit RootHeaderA's TypedPublishTraits before "
                "RootHeaderB's (canonical qualified-name order), regardless of "
                "source order",
            )


if __name__ == "__main__":
    unittest.main()
