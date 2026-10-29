#!/usr/bin/env python3
"""Self-tests for the content-identity harness (`content_identity_2a.py`).

Under adversarial testing, every attempt at an AUTOMATED "is this reorder one of
the known-safe ones" mechanism (group-wide tag, per-key partition +
structural anchors, schema ground truth, allowlist-by-extracted-name,
allowlist-by-exact-header-text) was found to false-pass real content
drift in a genuinely new shape (see `content_identity_2a.py`'s own module
docstring "Design history" section for the full blow-by-blow). The final
design abandons that whole class of mechanism rather than attempting
another refinement: there is no reorder-tolerance mechanism left at all.

- Every statement body, in every file, compares STRICTLY and
  UNCONDITIONALLY -- no allowlist, no exception, for any `(file,
  function)` pair, known or unknown.
- The 3 real DataUploadGoal functions identified as legitimate
  canonical-order reorders now correctly show up as ordinary content
  drift in this harness's own output. Their safety argument is NOT this
  harness -- it is the compile+round-trip tests
  (`//cpp:data_upload_goal_reorder_roundtrip_test`,
  `//rust:data_upload_goal_reorder_roundtrip_test`), plus explicit human
  sign-off.

This file's tests are organized around that contract:

- `WithinFunctionBodyReorderTest` / `CrossFunctionStatementMoveTest` /
  `IdentifierRenameTest`: generic statement reordering, cross-function
  content movement, and identifier renames must all be reported as drift
  -- there is no allowlist left for any of them to be exempted by.
- `LegitimateDeclarationReorderStillPassesTest`: positive control for the
  UNRELATED, long-standing mechanism that lets whole
  declarations (top-level functions, struct/table fields) reorder freely
  relative to each other -- this is not part of the strict-comparison
  redesign at all (it has no wire-format or behavioral implication regardless of
  position) and must keep passing.
- `DefinitionBeforeUseInversionTest` / `DuplicateFieldStatementReorderTest`
  / `MixedTaggedAndUntaggedSiblingsTest`: three realistic generated-code
  shapes from earlier adversarial testing, kept as regression
  coverage for the parser (`depth0_chunks`/`_split_header_body_footer`)
  and the strict default -- all three now fail for the SAME simple reason
  (every statement body is strict, unconditionally), not because of any
  shape-specific classifier logic, since that logic no longer exists.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import content_identity_2a as cid  # noqa: E402


def _write(tmp: Path, name: str, text: str) -> Path:
    path = tmp / name
    path.write_text(text, encoding="utf-8")
    return path


def _write_pair(tmp: Path, basename: str, old_text: str, new_text: str) -> tuple[Path, Path]:
    """Write `old_text`/`new_text` to SEPARATE `old/`/`new/` subdirectories,
    both under the SAME real `basename` -- required whenever a test needs
    to use the ACTUAL basename a prior round's now-deleted allowlist was
    keyed on (`compare_file`'s allowlist lookup used `new_path.name`):
    writing both sides directly into one shared directory under the
    identical name would silently make the second write overwrite the
    first (same path), leaving nothing to compare. A regression test that
    does not use the real basename never actually exercises the deleted
    mechanism's lookup key, so it would pass identically whether or not
    that mechanism still existed -- see `KnownDataUploadGoalReorderNowFailsTest`."""
    old_dir = tmp / "old"
    new_dir = tmp / "new"
    old_dir.mkdir(exist_ok=True)
    new_dir.mkdir(exist_ok=True)
    return _write(old_dir, basename, old_text), _write(new_dir, basename, new_text)


class WithinFunctionBodyReorderTest(unittest.TestCase):
    """Swapping two GENERIC statements within ONE function body must FAIL --
    every statement body compares strictly, unconditionally."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_swapped_generic_statements_in_one_function_body_fails(self) -> None:
        old_text = (
            "fn some_helper(a: i32, b: i32) -> i32 {\n"
            "    let doubled = a * 2;\n"
            "    let tripled = b * 3;\n"
            "    doubled + tripled\n"
            "}\n"
        )
        new_text = (
            "fn some_helper(a: i32, b: i32) -> i32 {\n"
            "    let tripled = b * 3;\n"
            "    let doubled = a * 2;\n"
            "    doubled + tripled\n"
            "}\n"
        )
        old_path = _write(self.tmp, "old.rs", old_text)
        new_path = _write(self.tmp, "new.rs", new_text)
        ok, detail = cid.compare_file(old_path, new_path)
        self.assertFalse(
            ok,
            "swapping two statements WITHIN one function body must be "
            f"reported as drift (harness said: {detail!r})",
        )


class CrossFunctionStatementMoveTest(unittest.TestCase):
    """Moving a statement from one function to a DIFFERENT function must
    FAIL -- unambiguous real content drift, never a canonical-order
    concern. Closed by identity-paired recursion (never
    pooling two different parents' children together)."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_statement_moved_between_two_functions_fails(self) -> None:
        old_text = (
            "fn build_a_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            ") -> flatbuffers::WIPOffset<A<'a>> {\n"
            "    let x_offset = builder.create_string(\"x\");\n"
            "    let mut root_builder = ABuilder::new(builder);\n"
            "    root_builder.finish()\n"
            "}\n"
            "\n"
            "fn build_b_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            ") -> flatbuffers::WIPOffset<B<'a>> {\n"
            "    let mut root_builder = BBuilder::new(builder);\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        # `let x_offset = ...;` moved from build_a_typed into build_b_typed.
        new_text = (
            "fn build_a_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            ") -> flatbuffers::WIPOffset<A<'a>> {\n"
            "    let mut root_builder = ABuilder::new(builder);\n"
            "    root_builder.finish()\n"
            "}\n"
            "\n"
            "fn build_b_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            ") -> flatbuffers::WIPOffset<B<'a>> {\n"
            "    let x_offset = builder.create_string(\"x\");\n"
            "    let mut root_builder = BBuilder::new(builder);\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        old_path = _write(self.tmp, "old.rs", old_text)
        new_path = _write(self.tmp, "new.rs", new_text)
        ok, detail = cid.compare_file(old_path, new_path)
        self.assertFalse(
            ok,
            "moving a statement from one function to a DIFFERENT function "
            "must be reported as content drift -- this is unambiguously a "
            f"real content change (harness said: {detail!r})",
        )


class IdentifierRenameTest(unittest.TestCase):
    """A raw identifier rename, with everything else held constant, must
    still FAIL -- this exercises the (unchanged) declaration-level
    unordered comparison, not the strict statement-body default."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_renamed_field_fails(self) -> None:
        old_text = (
            "pub struct DataUploadGoalMsg {\n"
            "    pub bucket: std::optional<std::string>,\n"
            "    pub prefix: std::optional<std::string>,\n"
            "}\n"
        )
        new_text = (
            "pub struct DataUploadGoalMsg {\n"
            "    pub bucketRENAMED: std::optional<std::string>,\n"
            "    pub prefix: std::optional<std::string>,\n"
            "}\n"
        )
        old_path = _write(self.tmp, "old.rs", old_text)
        new_path = _write(self.tmp, "new.rs", new_text)
        ok, detail = cid.compare_file(old_path, new_path)
        self.assertFalse(ok, f"a raw identifier rename must fail (harness said: {detail!r})")


class LegitimateDeclarationReorderStillPassesTest(unittest.TestCase):
    """Positive control for the mechanism the strict-comparison redesign
    does NOT touch: whole declarations (top-level functions; a struct/table's own
    fields) may reorder relative to each other freely -- these are
    self-contained, so their POSITION has no wire-format or behavioral
    implication regardless of statement-body strictness. Two independent
    whole functions swapping position, and a struct's fields reordering
    (the real DataUploadGoal id-based-sort shape), must both still PASS."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_two_whole_functions_swapped_passes(self) -> None:
        fn_a = (
            "fn build_a_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            ") -> flatbuffers::WIPOffset<A<'a>> {\n"
            "    let mut root_builder = ABuilder::new(builder);\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        fn_b = (
            "fn build_b_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            ") -> flatbuffers::WIPOffset<B<'a>> {\n"
            "    let mut root_builder = BBuilder::new(builder);\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        old_path = _write(self.tmp, "old.rs", fn_a + "\n" + fn_b)
        new_path = _write(self.tmp, "new.rs", fn_b + "\n" + fn_a)
        ok, detail = cid.compare_file(old_path, new_path)
        self.assertTrue(
            ok,
            "two whole, byte-identical functions merely swapping position "
            f"must still PASS (harness said: {detail!r})",
        )

    def test_struct_field_reorder_still_passes(self) -> None:
        # Mirrors the real DataUploadGoal shape: `type` carries `(id: 8)`
        # but is declared 2nd textually -- canonical (id-sorted) output
        # emits it last. Field order WITHIN one struct/table changing is
        # explicitly in the canonical-order scope and must stay tolerated --
        # this is the declaration-list mechanism, never the strict
        # statement-body default the actual builder/copy functions for
        # this same reorder are held to (see
        # data_upload_goal_reorder_roundtrip_test for those).
        old_text = (
            "pub struct DataUploadGoalMsg {\n"
            "    pub bucket: std::optional<std::string>,\n"
            "    pub type_: std::optional<UploadType>,\n"
            "    pub prefix: std::optional<std::string>,\n"
            "}\n"
        )
        new_text = (
            "pub struct DataUploadGoalMsg {\n"
            "    pub bucket: std::optional<std::string>,\n"
            "    pub prefix: std::optional<std::string>,\n"
            "    pub type_: std::optional<UploadType>,\n"
            "}\n"
        )
        old_path = _write(self.tmp, "old.rs", old_text)
        new_path = _write(self.tmp, "new.rs", new_text)
        ok, detail = cid.compare_file(old_path, new_path)
        self.assertTrue(
            ok,
            "reordering fields WITHIN one struct/table (the real "
            f"DataUploadGoal shape) must still PASS (harness said: {detail!r})",
        )


class KnownDataUploadGoalReorderNowFailsTest(unittest.TestCase):
    """The strict-comparison design's actual point, stated as its own
    regression test: the 3 REAL `DataUploadGoal.type` reorder shapes (the
    `type` field's statement moving from position 2 to last, id=8 being
    the highest explicit id on the table -- the ONLY shapes the
    registry-wide enumeration found across all 121 real artifacts, both
    languages) used to be tolerated via `REORDER_ALLOWLIST`. That
    mechanism is gone: these exact shapes must now be reported as
    content drift, like anything else that reorders a statement body.
    Their actual safety argument lives entirely in
    `//cpp:data_upload_goal_reorder_roundtrip_test` and
    `//rust:data_upload_goal_reorder_roundtrip_test`, never in this
    harness -- if a future edit to `compare_groups` accidentally
    reintroduces tolerance for any of these (or any other) statement-body
    reorder, these tests catch it.

    Fixtures use the REAL basenames the now-deleted `REORDER_ALLOWLIST`
    was keyed on (`data_upload_msg_rs.rs`, `data_upload_msg.h`), via
    `_write_pair` -- using a placeholder basename like `new.rs` would make
    these tests pass identically whether or not the old allowlist
    mechanism still existed (it was keyed on `new_path.name`
    specifically), which would defeat their entire point as a regression
    guard against reintroducing it. Each of the 3 known functions gets its
    OWN, independently isolated test method/fixture (the two C++
    functions sharing ONE fixture/assertion could not tell "both still
    strict" apart from "one function's exception was
    silently reinstated but the OTHER one's mismatch alone still made the
    whole file fail" -- verified directly against the parent commit's
    real allowlist: allowlisting ONLY `CopyDataUploadGoalMsg`, or ONLY
    `BuildDataUploadGoalTyped`, each independently still produced
    `ok=False` for the combined fixture, so a combined
    `assertFalse(ok)` could not have caught either partial regression)."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_rust_build_datauploadgoal_typed_reorder_is_reported_as_drift(self) -> None:
        # Byte-exact header for the real generated `build_datauploadgoal_typed`
        # (verified directly against /tmp/p2a_r6_old_all/data_upload_msg_rs.rs).
        # The BODY, unlike the C++ fixture below, is a HAND-APPROXIMATED
        # stand-in, not a verbatim extraction -- e.g. its `root_builder`
        # declaration omits the real `aorta::action::data_upload::` middle
        # namespace segment. It faithfully reproduces the real reorder
        # PATTERN (which fields' if-blocks appear, and in what order), but
        # individual body lines should not be assumed byte-exact. What
        # matters for this test -- the real basename, the real header
        # (hence real function identity), and the real relative reorder of
        # the `type` statement -- IS accurate.
        header = (
            "fn build_datauploadgoal_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            "    header: Option<flatbuffers::WIPOffset<aorta::sys::AortaHeader<'a>>>,\n"
            "    msg: &typed::DataUploadGoalMsg,\n"
            ") -> flatbuffers::WIPOffset<data_upload_generated::aorta::action::data_upload::DataUploadGoal<'a>> {\n"
        )
        old_text = (
            header
            + "    let bucket_offset = msg.bucket.as_ref().map(|value| builder.create_string(value));\n"
            "    let mut root_builder = data_upload_generated::DataUploadGoalBuilder::new(builder);\n"
            "    let _ = header;\n"
            "    if let Some(value) = msg.created_at_ms {\n"
            "        root_builder.add_created_at_ms(value);\n"
            "    }\n"
            "    if let Some(value) = msg.r#type.as_ref() {\n"
            "        root_builder.add_type_(*value);\n"
            "    }\n"
            "    if let Some(value) = bucket_offset {\n"
            "        root_builder.add_bucket(value);\n"
            "    }\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        # ONLY the `type` field's if-block moved -- from right after
        # created_at_ms (textual declaration order) to the very end
        # (canonical id order, id=8 is the highest) -- exactly mirroring
        # the real data_upload_msg_rs.rs diff.
        new_text = (
            header
            + "    let bucket_offset = msg.bucket.as_ref().map(|value| builder.create_string(value));\n"
            "    let mut root_builder = data_upload_generated::DataUploadGoalBuilder::new(builder);\n"
            "    let _ = header;\n"
            "    if let Some(value) = msg.created_at_ms {\n"
            "        root_builder.add_created_at_ms(value);\n"
            "    }\n"
            "    if let Some(value) = bucket_offset {\n"
            "        root_builder.add_bucket(value);\n"
            "    }\n"
            "    if let Some(value) = msg.r#type.as_ref() {\n"
            "        root_builder.add_type_(*value);\n"
            "    }\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        old_path, new_path = _write_pair(self.tmp, "data_upload_msg_rs.rs", old_text, new_text)
        ok, detail = cid.compare_file(old_path, new_path)
        self.assertFalse(
            ok,
            "the known DataUploadGoal.type reorder in "
            "build_datauploadgoal_typed must be reported as content "
            "drift; no allowlist tolerates this (basename, function) pair "
            f"(harness said: {detail!r})",
        )

    def test_cpp_build_datauploadgoal_typed_reorder_is_reported_as_drift(self) -> None:
        # Body lines extracted VERBATIM (byte-for-byte, diffed directly
        # against /tmp/p2a_r6_old_all(/new_all)/data_upload_msg.h) from the
        # real generated BuildDataUploadGoalTyped -- a representative
        # SUBSET of its statements (the real function also builds
        # dir/path_globs/window/bucket/key_template offsets this fixture
        # omits), but every line present here IS real. This fixture
        # contains ONLY this one function (a combined fixture
        # with CopyDataUploadGoalMsg could not tell "both still strict"
        # apart from "only ONE function's exception was reinstated" --
        # verified against the parent commit's real allowlist that
        # allowlisting either function ALONE still left the OTHER
        # function's mismatch making the whole file fail, so a single
        # combined assertFalse(ok) could not catch a regression isolated
        # to just one of the two).
        old_text = (
            "inline flatbuffers::Offset<aorta::action::data_upload::DataUploadGoal> BuildDataUploadGoalTyped(\n"
            "    flatbuffers::FlatBufferBuilder& builder,\n"
            "    const ::aorta::action::data_upload::DataUploadGoalMsg& msg,\n"
            "    flatbuffers::Offset<aorta::sys::AortaHeader> header) {\n"
            "  ::aorta::action::data_upload::DataUploadGoalBuilder root_builder(builder);\n"
            "  (void)header;\n"
            "  if (msg.created_at_ms.has_value()) {\n"
            "    root_builder.add_created_at_ms(msg.created_at_ms.value());\n"
            "  }\n"
            "  if (msg.type.has_value()) {\n"
            "    root_builder.add_type(msg.type.value());\n"
            "  }\n"
            "  if (msg.compress.has_value()) {\n"
            "    root_builder.add_compress(msg.compress.value());\n"
            "  }\n"
            "  return root_builder.Finish();\n"
            "}\n"
        )
        # ONLY the `type` field's statement moved -- from right after
        # created_at_ms (declaration order) to the very end (canonical id
        # order, id=8 is the highest) -- mirroring the real
        # data_upload_msg.h diff for this function.
        new_text = (
            "inline flatbuffers::Offset<aorta::action::data_upload::DataUploadGoal> BuildDataUploadGoalTyped(\n"
            "    flatbuffers::FlatBufferBuilder& builder,\n"
            "    const ::aorta::action::data_upload::DataUploadGoalMsg& msg,\n"
            "    flatbuffers::Offset<aorta::sys::AortaHeader> header) {\n"
            "  ::aorta::action::data_upload::DataUploadGoalBuilder root_builder(builder);\n"
            "  (void)header;\n"
            "  if (msg.created_at_ms.has_value()) {\n"
            "    root_builder.add_created_at_ms(msg.created_at_ms.value());\n"
            "  }\n"
            "  if (msg.compress.has_value()) {\n"
            "    root_builder.add_compress(msg.compress.value());\n"
            "  }\n"
            "  if (msg.type.has_value()) {\n"
            "    root_builder.add_type(msg.type.value());\n"
            "  }\n"
            "  return root_builder.Finish();\n"
            "}\n"
        )
        old_path, new_path = _write_pair(self.tmp, "data_upload_msg.h", old_text, new_text)
        ok, detail = cid.compare_file(old_path, new_path)
        self.assertFalse(
            ok,
            "the known DataUploadGoal.type reorder in "
            "BuildDataUploadGoalTyped ALONE must be reported as "
            "content drift; no allowlist tolerates this (basename, function) "
            f"pair (harness said: {detail!r})",
        )

    def test_cpp_copy_datauploadgoal_msg_reorder_is_reported_as_drift(self) -> None:
        # Body lines extracted VERBATIM from the real generated
        # CopyDataUploadGoalMsg (a representative subset -- the real
        # function also copies dir/path_globs/window/bucket/key_template).
        # Isolated to ONLY this one function -- see
        # test_cpp_build_datauploadgoal_typed_reorder_is_reported_as_drift's
        # docstring for why a combined fixture with the other C++ function
        # cannot independently pin either one.
        old_text = (
            "inline ::aorta::action::data_upload::DataUploadGoalMsg CopyDataUploadGoalMsg(\n"
            "    const ::aorta::action::data_upload::DataUploadGoal& root) {\n"
            "  ::aorta::action::data_upload::DataUploadGoalMsg out;\n"
            "  out.created_at_ms = root.created_at_ms();\n"
            "  out.type = root.type();\n"
            "  out.include_manifest = root.include_manifest();\n"
            "  out.compress = root.compress();\n"
            "  return out;\n"
            "}\n"
        )
        # ONLY the `type` field's statement moved -- from right after
        # created_at_ms (declaration order) to the very end (canonical id
        # order, id=8 is the highest) -- mirroring the real
        # data_upload_msg.h diff for this function.
        new_text = (
            "inline ::aorta::action::data_upload::DataUploadGoalMsg CopyDataUploadGoalMsg(\n"
            "    const ::aorta::action::data_upload::DataUploadGoal& root) {\n"
            "  ::aorta::action::data_upload::DataUploadGoalMsg out;\n"
            "  out.created_at_ms = root.created_at_ms();\n"
            "  out.include_manifest = root.include_manifest();\n"
            "  out.compress = root.compress();\n"
            "  out.type = root.type();\n"
            "  return out;\n"
            "}\n"
        )
        old_path, new_path = _write_pair(self.tmp, "data_upload_msg.h", old_text, new_text)
        ok, detail = cid.compare_file(old_path, new_path)
        self.assertFalse(
            ok,
            "the known DataUploadGoal.type reorder in "
            "CopyDataUploadGoalMsg ALONE must be reported as content "
            "drift; no allowlist tolerates this (basename, function) pair "
            f"(harness said: {detail!r})",
        )


class DefinitionBeforeUseInversionTest(unittest.TestCase):
    """Regression coverage for a realistic generated-code shape from an
    earlier adversarial testing: a define-before-use pair
    (`let token = acquire();` then `consume(token);`) alongside an
    unrelated `root_builder.finish();` call. Swapping the define-before-use
    pair changes program meaning and must FAIL -- under the strict design
    this is simply "every statement body is strict, unconditionally," not
    a shape-specific classifier rule (the mechanism that used to reason
    about this shape specifically no longer exists)."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_definition_before_use_swap_fails(self) -> None:
        old_text = (
            "fn build_x_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            ") -> flatbuffers::WIPOffset<X<'a>> {\n"
            "    let token = acquire();\n"
            "    consume(token);\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        new_text = (
            "fn build_x_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            ") -> flatbuffers::WIPOffset<X<'a>> {\n"
            "    consume(token);\n"
            "    let token = acquire();\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        old_path = _write(self.tmp, "old.rs", old_text)
        new_path = _write(self.tmp, "new.rs", new_text)
        ok, detail = cid.compare_file(old_path, new_path)
        self.assertFalse(
            ok,
            f"a define-before-use inversion must be reported as content drift (harness said: {detail!r})",
        )


class DuplicateFieldStatementReorderTest(unittest.TestCase):
    """Regression coverage: two `root_builder.add_x(...)` statements for
    the SAME field, reordered, must FAIL -- exact list equality (not just
    a shared "looks like a builder call" classification) is what the
    strict default checks."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_duplicate_add_x_statements_reordered_fails(self) -> None:
        old_text = (
            "fn build_y_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            ") -> flatbuffers::WIPOffset<Y<'a>> {\n"
            "    let mut root_builder = YBuilder::new(builder);\n"
            "    root_builder.add_x(1);\n"
            "    root_builder.add_x(2);\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        new_text = (
            "fn build_y_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            ") -> flatbuffers::WIPOffset<Y<'a>> {\n"
            "    let mut root_builder = YBuilder::new(builder);\n"
            "    root_builder.add_x(2);\n"
            "    root_builder.add_x(1);\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        old_path = _write(self.tmp, "old.rs", old_text)
        new_path = _write(self.tmp, "new.rs", new_text)
        ok, detail = cid.compare_file(old_path, new_path)
        self.assertFalse(
            ok,
            "two same-field builder statements reordered relative to EACH "
            f"OTHER must be reported as content drift (harness said: {detail!r})",
        )


class MixedTaggedAndUntaggedSiblingsTest(unittest.TestCase):
    """Regression coverage: a function body mixing a builder-shaped
    statement with unrelated calls -- swapping the unrelated calls must
    still FAIL under the strict default."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_unrelated_calls_swapped_alongside_one_tagged_block_fails(self) -> None:
        old_text = (
            "fn build_z_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            "    msg: &typed::ZMsg,\n"
            ") -> flatbuffers::WIPOffset<Z<'a>> {\n"
            "    validate();\n"
            "    if let Some(value) = msg.foo {\n"
            "        root_builder.add_foo(value);\n"
            "    }\n"
            "    commit();\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        new_text = (
            "fn build_z_typed<'a>(\n"
            "    builder: &mut flatbuffers::FlatBufferBuilder<'a>,\n"
            "    msg: &typed::ZMsg,\n"
            ") -> flatbuffers::WIPOffset<Z<'a>> {\n"
            "    commit();\n"
            "    if let Some(value) = msg.foo {\n"
            "        root_builder.add_foo(value);\n"
            "    }\n"
            "    validate();\n"
            "    root_builder.finish()\n"
            "}\n"
        )
        old_path = _write(self.tmp, "old.rs", old_text)
        new_path = _write(self.tmp, "new.rs", new_text)
        ok, detail = cid.compare_file(old_path, new_path)
        self.assertFalse(
            ok,
            "swapping two unrelated calls must be reported as content "
            f"drift under the strict default (harness said: {detail!r})",
        )


if __name__ == "__main__":
    unittest.main()
