#!/usr/bin/env python3
"""Unit tests for the explicit type-support matrix.

Covers:
  - Every UNSUPPORTED matrix cell (`UNSUPPORTED_FIELD_MATRIX`) hard-fails via
    `require_field_supported` with a message naming schema + table + field +
    shape + category ("no silent emission").
  - SUPPORTED cells -- including the FIXED holes ([bool] -> vector_scalar,
    struct, [struct] -> vector_struct) -- are no-ops.
  - `logical_field_category` classifies shapes independently of whether
    emission would succeed.
  - `--action-name` override with non-conforming tables hard-fails
    (`derive_action_binding_names`), while path-inferred callers stay lenient.
  - WIRING: the matrix actually fires through the real emitters --
    `write_rust` rejects a vector<union> field (typed_build sweep) and
    `write_cpp` rejects a vector<union> field in an action Result (copy helper).
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

import gen_message_wrapper as gmw  # noqa: E402
import schema_reflection as sr  # noqa: E402


def _repo_root() -> Path:
    srcdir = os.environ.get("TEST_SRCDIR")
    workspace = os.environ.get("TEST_WORKSPACE")
    if srcdir and workspace:
        candidate = Path(srcdir) / workspace
        if (candidate / "message" / "tools").exists():
            return candidate
    return THIS_DIR.parent.parent


_FIXTURE_DIR = "message/tools/tests/matrix_fixtures"


# The exact set of UNSUPPORTED cells the matrix declares. Kept here as an
# independent copy so a silent edit to `UNSUPPORTED_FIELD_MATRIX` that adds or
# drops a cell fails `test_matrix_cell_set_is_exactly_the_declared_holes`.
_EXPECTED_UNSUPPORTED = {
    ("vector_union", "rust", "typed_build"),
    ("vector_union", "rust", "owned_clone_from_msg"),
    ("vector_union", "rust", "owned_clone_from_root"),
    ("vector_union", "rust", "owned_borrow"),
    ("vector_union", "cpp", "copy_helper"),
}

# Independent re-declaration of the matrix axes. If a future edit changes
# `FIELD_SUPPORT_CATEGORIES` or `LANGUAGE_EMISSION_PATHS` in the generator, the
# completeness assertion below fails until this copy is reconciled — no axis can
# drift silently.
_KNOWN_CATEGORIES = (
    "scalar", "enum", "string", "struct", "table", "union",
    "vector_scalar", "vector_enum", "vector_string",
    "vector_struct", "vector_table", "vector_union",
)
_LANG_PATHS = {
    "rust": ("typed_build", "owned_clone_from_msg", "owned_clone_from_root", "owned_borrow"),
    "cpp": ("typed_build", "copy_helper"),
}


def _full_cartesian() -> set:
    cells = set()
    for category in _KNOWN_CATEGORIES:
        for language, paths in _LANG_PATHS.items():
            for path in paths:
                cells.add((category, language, path))
    return cells


class MatrixCellTest(unittest.TestCase):
    def test_axes_match_the_independent_declaration(self) -> None:
        self.assertEqual(tuple(gmw.FIELD_SUPPORT_CATEGORIES), _KNOWN_CATEGORIES)
        self.assertEqual(
            {k: tuple(v) for k, v in gmw.LANGUAGE_EMISSION_PATHS.items()},
            _LANG_PATHS,
        )

    def test_state_table_is_the_complete_cartesian_product(self) -> None:
        # No cell missing, no extra cell — the WHOLE (category × language ×
        # path) product is materialized, not just the 5 holes.
        expected = _full_cartesian()
        self.assertEqual(set(gmw.FIELD_SUPPORT_MATRIX), expected)
        self.assertEqual(
            len(gmw.FIELD_SUPPORT_MATRIX),
            len(_KNOWN_CATEGORIES) * sum(len(p) for p in _LANG_PATHS.values()),
        )

    def test_every_cell_has_an_explicit_verdict(self) -> None:
        # Each cell is either SUPPORTED (None) or UNSUPPORTED (non-empty
        # reason). The UNSUPPORTED subset is EXACTLY the declared holes.
        unsupported = set()
        for cell, verdict in gmw.FIELD_SUPPORT_MATRIX.items():
            if verdict is None:
                continue
            self.assertIsInstance(verdict, str)
            self.assertTrue(verdict.strip())
            unsupported.add(cell)
        self.assertEqual(unsupported, _EXPECTED_UNSUPPORTED)

    def test_matrix_cell_set_is_exactly_the_declared_holes(self) -> None:
        self.assertEqual(set(gmw.UNSUPPORTED_FIELD_MATRIX), _EXPECTED_UNSUPPORTED)

    def test_every_unsupported_cell_hard_fails_naming_schema_table_field_shape(self) -> None:
        for category, language, path in sorted(_EXPECTED_UNSUPPORTED):
            with self.assertRaises(gmw.GenerationError) as ctx:
                gmw.require_field_supported(
                    category, language, path, "my_schema", "MyTable", "my_field", "[my.ns.Choice]"
                )
            msg = str(ctx.exception)
            self.assertIn("my_schema", msg)
            self.assertIn("MyTable", msg)
            self.assertIn("my_field", msg)
            self.assertIn("[my.ns.Choice]", msg)
            self.assertIn(language, msg)
            self.assertIn(path, msg)
            self.assertIn(category, msg)

    def test_every_supported_cell_is_a_noop(self) -> None:
        # Drive require_field_supported over EVERY SUPPORTED cell in the full
        # product (not a hand-picked sample) — none may raise.
        for cell, verdict in gmw.FIELD_SUPPORT_MATRIX.items():
            if verdict is not None:
                continue
            category, language, path = cell
            gmw.require_field_supported(category, language, path, "s", "T", "f", "sh")

    def test_unknown_category_including_other_hard_fails(self) -> None:
        # Fail-CLOSED: "other" (the classifier's unresolved fall-through) and any
        # bogus category are rejected, NOT silently emitted (fail-open fix).
        for category in ("other", "totally_bogus", ""):
            with self.assertRaises(gmw.GenerationError) as ctx:
                gmw.require_field_supported(
                    category, "rust", "typed_build", "s", "T", "f", "sh"
                )
            self.assertIn("unknown/unclassifiable field category", str(ctx.exception))

    def test_unknown_language_hard_fails(self) -> None:
        for language in ("python", "go", ""):
            with self.assertRaises(gmw.GenerationError) as ctx:
                gmw.require_field_supported(
                    "scalar", language, "typed_build", "s", "T", "f", "sh"
                )
            self.assertIn("unknown emission language", str(ctx.exception))

    def test_unknown_path_hard_fails(self) -> None:
        # A real path but wrong language (copy_helper is cpp-only; owned_borrow
        # is rust-only) and a bogus path are all rejected.
        for language, path in (
            ("rust", "copy_helper"),
            ("cpp", "owned_borrow"),
            ("cpp", "owned_clone_from_root"),
            ("rust", "bogus_path"),
        ):
            with self.assertRaises(gmw.GenerationError) as ctx:
                gmw.require_field_supported(
                    "scalar", language, path, "s", "T", "f", "sh"
                )
            self.assertIn("unknown emission path", str(ctx.exception))


class LogicalCategoryTest(unittest.TestCase):
    def test_scalar_and_bool_vector(self) -> None:
        self.assertEqual(gmw.logical_field_category("bool", set(), set(), set(), {}), "scalar")
        self.assertEqual(gmw.logical_field_category("[bool]", set(), set(), set(), {}), "vector_scalar")
        self.assertEqual(gmw.logical_field_category("[int32]", set(), set(), set(), {}), "vector_scalar")

    def test_named_shapes(self) -> None:
        self.assertEqual(gmw.logical_field_category("Pt", {"Pt"}, set(), set(), {}), "struct")
        self.assertEqual(gmw.logical_field_category("[Pt]", {"Pt"}, set(), set(), {}), "vector_struct")
        self.assertEqual(gmw.logical_field_category("Tbl", set(), {"Tbl"}, set(), {}), "table")
        self.assertEqual(gmw.logical_field_category("[Tbl]", set(), {"Tbl"}, set(), {}), "vector_table")
        self.assertEqual(gmw.logical_field_category("U", set(), set(), {"U"}, {}), "union")
        self.assertEqual(gmw.logical_field_category("[U]", set(), set(), {"U"}, {}), "vector_union")
        self.assertEqual(gmw.logical_field_category("E", set(), set(), set(), {"E": "ubyte"}), "enum")
        self.assertEqual(gmw.logical_field_category("[E]", set(), set(), set(), {"E": "ubyte"}), "vector_enum")

    def test_string_and_unresolved(self) -> None:
        self.assertEqual(gmw.logical_field_category("string", set(), set(), set(), {}), "string")
        self.assertEqual(gmw.logical_field_category("[string]", set(), set(), set(), {}), "vector_string")
        # A name that resolves to nothing classifies as "other" (never blocked).
        self.assertEqual(gmw.logical_field_category("Unknown", set(), set(), set(), {}), "other")


class ActionNameHardFailTest(unittest.TestCase):
    """An --action-name override with non-conforming tables -> GenerationError."""

    def test_override_with_non_conforming_tables_hard_fails(self) -> None:
        cases = [
            {"Foo", "Bar"},
            {"AlphaGoal", "BetaResult", "AlphaFeedbackData"},
            {"AlphaGoal", "AlphaResult", "AlphaFeedbackData"},  # no wrapper tables
        ]
        for names in cases:
            with self.assertRaises(gmw.GenerationError) as ctx:
                gmw.derive_action_binding_names("", "probe", names, action_name_override="demo/x/do")
            msg = str(ctx.exception)
            self.assertIn("--action-name 'demo/x/do'", msg)
            self.assertIn("probe", msg)

    def test_explicit_empty_override_hard_fails(self) -> None:
        # `--action-name=""` (explicit empty) is distinct from absent
        # (None) and must hard-fail, not silently emit an empty-named action.
        with self.assertRaises(gmw.GenerationError) as ctx:
            gmw.derive_action_binding_names(
                "schemas/action/foo", "probe", {"Foo"}, action_name_override=""
            )
        msg = str(ctx.exception)
        self.assertIn("empty", msg)
        self.assertIn("probe", msg)

    def test_path_inferred_non_conforming_stays_lenient(self) -> None:
        # Under schemas/action/ but not conforming, with no override: returns
        # None (unchanged pre-matrix behavior), NOT a hard error.
        self.assertIsNone(
            gmw.derive_action_binding_names("schemas/action/foo", "probe", {"Foo"}, None)
        )

    def test_valid_override_returns_binding(self) -> None:
        names = {
            "XGoal", "XResult", "XFeedbackData",
            "XSendGoalRequest", "XSendGoalResponse", "XCancelGoalRequest",
            "XCancelGoalResponse", "XFeedback", "XPublicStatus",
            "XGetResultRequest", "XGetResultResponse",
        }
        binding = gmw.derive_action_binding_names("", "x", names, action_name_override="a/b")
        self.assertIsNotNone(binding)
        self.assertEqual(binding["action_name"], "a/b")


class MatrixWiringTest(unittest.TestCase):
    """Proves the matrix actually fires through the real emitters, not just the
    `require_field_supported` primitive in isolation."""

    def test_write_rust_rejects_vector_of_union(self) -> None:
        gmw._LOADED_SCHEMA = sr.load_bfbs_file(
            str(_repo_root() / _FIXTURE_DIR / "vu_topic.bfbs")
        )
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.rs"
            with self.assertRaises(gmw.GenerationError) as ctx:
                gmw.write_rust(
                    out=str(out),
                    stem="vu_topic",
                    schema_rel=_FIXTURE_DIR,
                    root_fbs=str(_repo_root() / _FIXTURE_DIR / "vu_topic.fbs"),
                    with_schema_meta=False,
                )
        msg = str(ctx.exception)
        self.assertIn("rust", msg)
        self.assertIn("typed_build", msg)
        self.assertIn("vector_union", msg)
        self.assertIn("many", msg)

    def test_write_cpp_action_copy_helper_rejects_vector_of_union(self) -> None:
        gmw._LOADED_SCHEMA = sr.load_bfbs_file(
            str(_repo_root() / _FIXTURE_DIR / "vu_action_action" / "vu_action.bfbs")
        )
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.h"
            with self.assertRaises(gmw.GenerationError) as ctx:
                gmw.write_cpp(
                    out=str(out),
                    stem="vu_action",
                    schema_rel=_FIXTURE_DIR,
                    root_fbs=str(
                        _repo_root() / _FIXTURE_DIR / "vu_action_action" / "vu_action.fbs"
                    ),
                    action_name="mtx/vu",
                )
        msg = str(ctx.exception)
        self.assertIn("cpp", msg)
        self.assertIn("copy_helper", msg)
        self.assertIn("vector_union", msg)
        self.assertIn("picks", msg)


if __name__ == "__main__":
    unittest.main()
