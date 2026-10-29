#!/usr/bin/env python3
"""Unit tests for gen_message_wrapper's bfbs-loader-backed semantic builders.

The wrapper emitters take
their table/union/enum/struct/root/namespace facts from schema_reflection.py's
qualified TypeId/UnionVariantDef model (the `*_from_loaded` builders in
gen_message_wrapper.py) when `--root-bfbs` is set. These tests exercise those
builders directly against small flatc-compiled `.bfbs` fixtures.

Required cases (parser unit tests rewritten against the loader, with the
same-line-table and comment-blind-type regression cases preserved and new
cases: qualified root_type, mid-file namespace, union member alias, same bare
name in two namespaces):
  - qualified root_type              -> `QualifiedRootTypeTest`
  - mid-file namespace               -> `MidFileNamespaceTest`
  - union member alias               -> `UnionMemberAliasTest`
  - same bare name in two namespaces -> `SameBareNameCollisionTest`
  - same-line table                  -> `Msg02SameLineTableTest`
  - comment-blind types              -> `Msg03CommentedGhostTest`
Plus field-type reconstruction / scalar canonicalization coverage
(`FieldTypeReconstructionTest`), which underpins the u8/i8 vector fast-path
correction (the one intended output diff of the loader switch).

`--root-bfbs` is now REQUIRED and the regex/text
scanners are deleted, so this is the ONLY parser-level test file for
gen_message_wrapper.py's semantic facts. `UnionExplicitValueOrderingTest`
below additionally ports `gen_message_wrapper_union_semantics_test.py`'s
`UnionVariantOrderingTest` (retired along with the other text-path test
files).
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace as NS

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

import gen_message_wrapper as gmw  # noqa: E402
import schema_include_utils  # noqa: E402
import schema_reflection as sr  # noqa: E402


def _repo_root() -> Path:
    srcdir = os.environ.get("TEST_SRCDIR")
    workspace = os.environ.get("TEST_WORKSPACE")
    if srcdir and workspace:
        candidate = Path(srcdir) / workspace
        if (candidate / "message" / "tools").exists():
            return candidate
    return THIS_DIR.parent.parent


_FIXTURE_DIR = "message/tools/tests/wrapper_loader_fixtures"


def _load(stem: str) -> sr.LoadedSchema:
    return sr.load_bfbs_file(str(_repo_root() / _FIXTURE_DIR / (stem + ".bfbs")))


def _root_fbs(stem: str) -> str:
    # A path whose stem is `stem` — the loader builders only take
    # `Path(root_fbs).stem` from it for the root gen_mod (no file read).
    return "{}/{}.fbs".format(_FIXTURE_DIR, stem)


class QualifiedRootTypeTest(unittest.TestCase):
    def test_qualified_root_type_returns_bare_name(self) -> None:
        loaded = _load("qualified_root")
        # `root_type demo.thing.Root;` — the loader returns the BARE name the
        # bare-name-keyed emitter model needs (the text scanner would return
        # the qualified spelling and miss the table).
        self.assertEqual(gmw.root_type_from_loaded(loaded), "Root")
        self.assertEqual(gmw.namespace_full_from_loaded(loaded), "demo.thing")

    def test_root_type_qualified_is_the_full_dotted_name(self) -> None:
        # The schema-meta root-type name is the root type's OWN FQN.
        # For a LOCAL root it equals `namespace_full.<root>` (demo.thing.Root).
        loaded = _load("qualified_root")
        self.assertEqual(
            gmw.root_type_qualified_from_loaded(loaded), "demo.thing.Root"
        )

    def test_root_type_qualified_uses_root_own_namespace(self) -> None:
        # A FOREIGN root_type in a DIFFERENT namespace than
        # the local types -> the FQN is the ROOT's own namespace
        # (`mrforeign.deep.ForeignRoot`), NOT `namespace_full.<root>`
        # (`mrlocal.inner.ForeignRoot`, the schema-discovery-breaking pre-fix value).
        loaded = NS(
            root_type=NS(
                qualified_name="mrforeign.deep.ForeignRoot", name="ForeignRoot"
            )
        )
        self.assertEqual(
            gmw.root_type_qualified_from_loaded(loaded), "mrforeign.deep.ForeignRoot"
        )
        self.assertEqual(gmw.root_type_from_loaded(loaded), "ForeignRoot")
        self.assertIsNone(gmw.root_type_qualified_from_loaded(NS(root_type=None)))


class MidFileNamespaceTest(unittest.TestCase):
    def test_each_type_keeps_its_own_namespace(self) -> None:
        loaded = _load("mid_namespace")
        table_defs = gmw.collect_table_defs_from_loaded(loaded, _root_fbs("mid_namespace"))
        self.assertEqual(table_defs["AlphaMsg"].namespace_full, "alpha")
        self.assertEqual(table_defs["BetaMsg"].namespace_full, "beta")

    def test_root_namespace_hard_errors_on_multiple_namespaces(self) -> None:
        loaded = _load("mid_namespace")
        # A single root wrapper namespace is unrepresentable across a mid-file
        # namespace change — fail closed rather than silently emit under the
        # first namespace (the text scanner's behavior).
        with self.assertRaises(gmw.GenerationError) as ctx:
            gmw.namespace_full_from_loaded(loaded)
        self.assertIn("multiple namespaces", str(ctx.exception))


class UnionMemberAliasTest(unittest.TestCase):
    def test_alias_and_target_are_split(self) -> None:
        loaded = _load("union_alias")
        unions = gmw.collect_union_defs_from_loaded(loaded)
        # `union Shape { LeafAlias: Leaf, Node }`: the aliased member keeps its
        # alias name with the real target; the plain member's alias == target.
        self.assertEqual(unions["Shape"], [("LeafAlias", "Leaf"), ("Node", "Node")])

    def test_alias_present_in_root_union_doc(self) -> None:
        loaded = _load("union_alias")
        root_doc = gmw.root_schema_doc_from_loaded(loaded, str(_repo_root() / _FIXTURE_DIR / "union_alias.fbs"))
        self.assertEqual(root_doc.union_defs["Shape"], [("LeafAlias", "Leaf"), ("Node", "Node")])


class UnionAliasEmissionTest(unittest.TestCase):
    """Port of
    gen_message_wrapper_union_semantics_test.py's (retired)
    `UnionAliasTargetTest` emission-correctness cases. `UnionMemberAliasTest`
    above only proves the FACT-COLLECTION layer
    (`collect_union_defs_from_loaded`) splits alias from target correctly;
    this class proves the DOWNSTREAM EMITTERS (`write_rust`/`write_cpp`) use
    the right half at every consuming call site -- a parse-only fix could
    still leave emission reading the wrong one. Reuses the existing
    `union_alias.fbs` fixture (`union Shape { LeafAlias: Leaf, Node }`)."""

    def _write_rust(self) -> str:
        gmw._LOADED_SCHEMA = _load("union_alias")
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.rs"
            gmw.write_rust(
                out=str(out),
                stem="union_alias",
                schema_rel=_FIXTURE_DIR,
                root_fbs=str(_repo_root() / _FIXTURE_DIR / "union_alias.fbs"),
                with_schema_meta=False,
            )
            return out.read_text(encoding="utf-8")

    def _write_cpp(self) -> str:
        gmw._LOADED_SCHEMA = _load("union_alias")
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.h"
            gmw.write_cpp(
                out=str(out),
                stem="union_alias",
                schema_rel=_FIXTURE_DIR,
                root_fbs=str(_repo_root() / _FIXTURE_DIR / "union_alias.fbs"),
            )
            return out.read_text(encoding="utf-8")

    def test_rust_emission_uses_alias_for_enum_and_target_for_msg_type(self) -> None:
        text = self._write_rust()
        # `typed` enum declaration: variant identifier is the ALIAS, boxed
        # payload type is the TARGET's Msg struct.
        self.assertIn("LeafAlias(Box<LeafMsg>)", text)
        self.assertNotIn("Leaf(Box<LeafMsg>)", text)
        # Union builder match arm: matches the wrapper's OWN `typed::ShapeMsg`
        # enum by ALIAS...
        self.assertIn("typed::ShapeMsg::LeafAlias(value) => {", text)
        # ...and constructs flatc's OWN generated discriminant enum
        # (`...::Shape::{alias}`) by ALIAS too -- flatc's codegen names union
        # discriminant values after `ev.name` (the alias), never the target's
        # bare name.
        self.assertIn("::Shape::LeafAlias, offset.as_union_value())", text)
        self.assertNotIn("::Shape::Leaf,", text)

    def test_cpp_emission_uses_target_for_variant_type_and_alias_for_discriminant(
        self,
    ) -> None:
        text = self._write_cpp()
        # `std::variant` alternative type list is TYPE-keyed -- only the
        # TARGET appears there.
        self.assertIn("std::shared_ptr<LeafMsg>", text)
        # Union builder body: dispatches on the TARGET's std::shared_ptr
        # alternative, but the returned discriminant constant is keyed by
        # ALIAS (flatc's `Shape_{ev.name}`, verbatim -- C++ does not
        # snake_case it, unlike Rust).
        self.assertIn("std::holds_alternative<std::shared_ptr<::demo::LeafMsg>>", text)
        self.assertIn("::demo::Shape_LeafAlias, offset.Union())", text)
        self.assertNotIn("Shape_Leaf,", text)


class UnionAliasActionEmissionTest(unittest.TestCase):
    """Port of
    gen_message_wrapper_union_semantics_test.py's (retired)
    `UnionAliasTargetTest` ACTION-specific emission cases -- the union lives
    in the Result table, exercising the "owned"/root-cloning call sites
    (Rust `clone_*_owned_from_root`, C++ copy-helper switch) the simpler
    non-action `union_alias.fbs` fixture cannot reach (root-cloning is scoped
    to Result/FeedbackData, not Goal). Fixture:
    `tests/wrapper_loader_fixtures/probe_action.fbs`, expanded through
    `gen_action_wrapper` by the `wrapper_loader_fixture_probe_action_expand`
    genrule (BUILD.bazel) -- `union Choice { Renamed: RealTarget }` in
    `ProbeResult`."""

    def _expanded_root_fbs(self) -> Path:
        return (
            _repo_root()
            / _FIXTURE_DIR
            / "probe_action_action"
            / "probe_action.fbs"
        )

    def _load_probe_action(self) -> None:
        gmw._LOADED_SCHEMA = sr.load_bfbs_file(
            str(_repo_root() / _FIXTURE_DIR / "probe_action_action" / "probe_action.bfbs")
        )

    def test_rust_action_root_cloning_uses_alias_for_accessor_and_target_for_clone(
        self,
    ) -> None:
        self._load_probe_action()
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.rs"
            gmw.write_rust(
                out=str(out),
                stem="probe_action",
                schema_rel=_FIXTURE_DIR,
                root_fbs=str(self._expanded_root_fbs()),
                with_schema_meta=False,
                action_name="test/probe_action",
            )
            text = out.read_text(encoding="utf-8")
        # The action get_result root-cloning match arm: flatc's discriminant
        # enum matched by ALIAS, its accessor method suffix is
        # snake_case(ALIAS) ("renamed", NOT "real_target"), and the cloned
        # value goes through the TARGET's own root-clone helper.
        self.assertIn(
            "Choice::Renamed => root.payload_as_renamed().map(|value| "
            "owned::ChoiceMsg::Renamed(Box::new(clone_realtarget_owned_from_root(value)))),",
            text,
        )

    def test_cpp_action_copy_helper_uses_alias_for_case_and_accessor(self) -> None:
        self._load_probe_action()
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.h"
            gmw.write_cpp(
                out=str(out),
                stem="probe_action",
                schema_rel=_FIXTURE_DIR,
                root_fbs=str(self._expanded_root_fbs()),
                action_name="test/probe_action",
            )
            text = out.read_text(encoding="utf-8")
        # The C++ action copy helper's switch: case label and the `_as_X()`
        # accessor are both keyed by ALIAS verbatim (C++ does not
        # snake_case it); the copied value uses the TARGET's Msg type and
        # copy helper.
        self.assertIn("case ::demo::action::Choice_Renamed: {", text)
        self.assertIn("root.payload_as_Renamed()", text)
        self.assertIn(
            "std::make_shared<::demo::action::RealTargetMsg>(CopyRealTargetMsg(*value))",
            text,
        )


class CrossNamespaceUnionCollisionTest(unittest.TestCase):
    """Port of
    gen_message_wrapper_union_semantics_test.py's (retired)
    `CrossNamespaceCollisionTest` -- `_loaded_named_defs` (the shared helper
    behind `collect_union_defs_from_loaded`/`collect_enum_bases_from_loaded`/
    `collect_struct_names_from_loaded`) must hard-error the same way
    `collect_table_defs_from_loaded` already does (`SameBareNameCollisionTest`
    above) on a genuine cross-namespace same-bare-name collision. Fixture:
    `dup_bare_union.fbs` (mid-file namespace change, no includes needed --
    same pattern as `dup_bare_name.fbs`'s table analog), two unrelated
    `union Clash { ... }` declarations in namespaces `one`/`two`. The
    "diamond include" de-duplication case the retired text-path test also
    covered (`test_collect_union_defs_still_merges_identical_diamond_include`)
    needs no loader-path analog: `loaded.unions` is a dict keyed by qualified
    name, built directly from the bfbs's own deduplicated `Schema.objects` --
    there is exactly ONE entry per type regardless of how many include paths
    reach it, unlike the old text parser's own DFS which needed explicit
    "visited" tracking to avoid re-parsing the same file twice."""

    def test_collect_union_defs_hard_errors_on_cross_namespace_bare_name_collision(
        self,
    ) -> None:
        loaded = _load("dup_bare_union")
        with self.assertRaises(gmw.GenerationError) as ctx:
            gmw.collect_union_defs_from_loaded(loaded)
        self.assertIn("Clash", str(ctx.exception))


class SameBareNameCollisionTest(unittest.TestCase):
    def test_duplicate_table_bare_name_hard_errors(self) -> None:
        loaded = _load("dup_bare_name")
        with self.assertRaises(gmw.GenerationError) as ctx:
            gmw.collect_table_defs_from_loaded(loaded, _root_fbs("dup_bare_name"))
        self.assertIn("duplicate table name", str(ctx.exception))


class Msg02SameLineTableTest(unittest.TestCase):
    """A same-line table must keep its own fields and not swallow the
    next table's. flatc parses natively, so the loader path is immune."""

    def test_same_line_table_fields_intact(self) -> None:
        loaded = _load("same_line_table")
        table_defs = gmw.collect_table_defs_from_loaded(loaded, _root_fbs("same_line_table"))
        self.assertEqual(
            [f["name"] for f in table_defs["Inline"].fields], ["a", "b"]
        )
        self.assertEqual([f["name"] for f in table_defs["After"].fields], ["c"])


class Msg03CommentedGhostTest(unittest.TestCase):
    """A block-commented type must not appear as live."""

    def test_commented_ghost_absent(self) -> None:
        loaded = _load("commented_ghost")
        table_defs = gmw.collect_table_defs_from_loaded(loaded, _root_fbs("commented_ghost"))
        self.assertIn("Real", table_defs)
        self.assertNotIn("Ghost", table_defs)


class FieldTypeReconstructionTest(unittest.TestCase):
    """Field-type reconstruction (`_shape_to_source_type`) — the emitter-facing
    `.fbs`-style type string the loader path feeds the unchanged emitter
    helpers. Scalars canonicalize to the short keyword: `[uint8]`/`[int8]`
    become `[ubyte]`/`[byte]`, which is what makes the u8/i8 vector fast-path
    apply consistently (the loader switch's one intended output diff)."""

    def _fields(self):
        loaded = _load("recon_types")
        table_defs = gmw.collect_table_defs_from_loaded(loaded, _root_fbs("recon_types"))
        return {f["name"]: f["type"] for f in table_defs["Recon"].fields}

    def test_scalar_and_vector_reconstruction(self) -> None:
        types = self._fields()
        self.assertEqual(types["n"], "int")
        self.assertEqual(types["txt"], "string")
        self.assertEqual(types["names"], "[string]")

    def test_long_form_scalar_vectors_canonicalize(self) -> None:
        types = self._fields()
        # `[uint8]` -> `[ubyte]`, `[int8]` -> `[byte]`: canonical short form,
        # which the u8/i8 fast-path gate (== "ubyte"/"byte") recognizes.
        self.assertEqual(types["b"], "[ubyte]")
        self.assertEqual(types["s"], "[byte]")

    def test_named_type_reconstructs_qualified(self) -> None:
        types = self._fields()
        # A local table reference reconstructs to its fully-qualified name;
        # `qualify_*_user_type` maps that to the same output as the bare source
        # spelling.
        self.assertEqual(types["child"], "demo.Child")


class UnionExplicitValueOrderingTest(unittest.TestCase):
    """Port of
    gen_message_wrapper_union_semantics_test.py's (retired) UnionVariantOrderingTest.
    The OLD text parser REPLAYED flatc's `EnumValBuilder` arithmetic in Python
    to compute each variant's numeric discriminant; the loader has no such
    replay -- it reads flatc's OWN computed value straight from the compiled
    bfbs (`schema_reflection.UnionDef.variants`, sorted by `(value, alias)`).
    This is a regression guard for OUR sort key using flatc's actual,
    compiled ground truth (a bug in `_union_variants_from_enum`'s sort could
    silently break behavior for any schema using an explicit `= N` override
    -- none does today, so it would otherwise go unnoticed)."""

    def test_explicit_value_reassigns_subsequent_implicit_values(self) -> None:
        # union_explicit_values.fbs: declaration order A, B, C, D; values
        # A=5(explicit), B=6(5+1), C=1(explicit), D=2(1+1) -- position and
        # value diverge once C's explicit override takes effect. Sorted by
        # value: C(1), D(2), A(5), B(6).
        loaded = _load("union_explicit_values")
        unions = gmw.collect_union_defs_from_loaded(loaded)
        self.assertEqual(
            [alias for alias, _target in unions["Picked"]],
            ["C", "D", "A", "B"],
            "union variants must sort by flatc's real computed numeric value, "
            "not declaration position (which would give a different order "
            "once C's explicit override makes position and value diverge)",
        )


class DigitBoundaryNamespaceRustPathTest(unittest.TestCase):
    """Regression: a namespace COMPONENT with a letter->digit
    boundary (`p05.probe`).

    flatc's Rust namer snake_cases every namespace component
    (`ConvertCase(..., kSnake)`), and `CamelToSnake` inserts an `_` at a
    lowercase->digit boundary, so `namespace p05.probe;` compiles to
    `pub mod p_05 { pub mod probe { ... } }`. `write_rust` used to build
    `namespace_rust` (and the two per-table `table_namespace_rust` variants)
    with a RAW `.` -> `::` replacement, emitting `p05::probe::...` — a module
    flatc never generated, so the generated crate failed to compile. `top_mod`
    (the `pub use {gen_mod}::{top_mod};` / prelude re-export of the TOP-LEVEL
    namespace module) took the namespace segment verbatim and dangled the same
    way. All four now route through `schema_include_utils`'s
    `flatc_rust_namespace_path` / `flatc_rust_namespace_segment`, the shared
    converters `qualify_rust_user_type` (and patch_flatc_rust's
    foreign-reference rewrite) already used.

    Fixture: `tests/wrapper_loader_fixtures/digit_ns.fbs`, expanded through
    `gen_action_wrapper` by `wrapper_loader_fixture_digit_ns_expand`
    (BUILD.bazel). Action-shaped on purpose: one render reaches every
    namespace-path call site in `write_rust` — the `super::`-qualified typed
    Root impls, the per-table typed_build helper signatures/builders, the
    action send_goal/cancel_goal/public_status and get_result wire paths, the
    union root-cloning `_table_root_path` helper, and the top-level module
    re-export.
    """

    _NS_FIXTURE_DIR = _FIXTURE_DIR + "/digit_ns_action"

    def _render_rust(self) -> str:
        root = _repo_root() / self._NS_FIXTURE_DIR
        gmw._LOADED_SCHEMA = sr.load_bfbs_file(str(root / "digit_ns.bfbs"))
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out.rs"
            gmw.write_rust(
                out=str(out),
                stem="digit_ns",
                schema_rel=_FIXTURE_DIR,
                root_fbs=str(root / "digit_ns.fbs"),
                with_schema_meta=False,
                action_name="test/digit_ns",
            )
            return out.read_text(encoding="utf-8")

    def test_no_emitted_rust_identifier_uses_the_unmangled_segment(self) -> None:
        text = self._render_rust()
        self.assertNotIn(
            "p05::",
            text,
            "wrapper emitted a Rust path built by a raw `.` -> `::` "
            "replacement; flatc generates `pub mod p_05`, so this path dangles",
        )
        # Broader guard, so a NEW unmangled site (e.g. another module
        # re-export) is caught too: the ONLY legitimate `p05` spelling left in
        # the output is the DOTTED schema FQN inside string literals /doc
        # comments (`"p05.probe.DigitNsGoal"`). Every `p05` that is not
        # followed by a `.` is a Rust identifier, and must be `p_05`.
        stray = re.search(r"\bp05\b(?!\.)", text)
        self.assertIsNone(
            stray,
            "emitted an unmangled `p05` Rust identifier"
            + (" near: " + text[max(0, stray.start() - 60):stray.end() + 20] if stray else ""),
        )

    def test_every_namespace_path_call_site_uses_flatc_mangling(self) -> None:
        text = self._render_rust()
        # One anchor per distinct construction site in write_rust, so a
        # partial revert (any single site going back to `.replace`) fails:
        for expected in (
            # `namespace_rust`, `super::`-qualified typed Root impl
            "super::digit_ns_generated::p_05::probe::DigitNsGoal<'a>",
            # `table_namespace_rust`, per-table typed_build helper + builder
            "flatbuffers::WIPOffset<digit_ns_generated::p_05::probe::Leaf<'a>>",
            "digit_ns_generated::p_05::probe::LeafBuilder::new(builder)",
            # `namespace_rust`, action send_goal / cancel_goal / public_status
            "digit_ns_generated::p_05::probe::DigitNsSendGoalRequest<'a>",
            "digit_ns_generated::p_05::probe::DigitNsCancelGoalRequest<'_>",
            "digit_ns_generated::p_05::probe::DigitNsPublicStatus<'_>",
            # `namespace_rust`, action get_result
            "digit_ns_generated::p_05::probe::DigitNsGetResultRequest<'_>",
            "digit_ns_generated::p_05::probe::DigitNsGetResultResponse<'_>",
            # `table_namespace_rust` in `_table_root_path` (union root-cloning)
            "clone_leaf_owned_from_root(root: digit_ns_generated::p_05::probe::Leaf<'_>)",
            # `top_mod`: the top-level namespace module re-export + prelude
            "pub use digit_ns_generated::p_05;",
            "    pub use super::p_05;",
        ):
            self.assertIn(expected, text)

    def test_converter_pins_the_fixture_namespace_expectation(self) -> None:
        # Pins the anchors above to the shared converter, so they are not a
        # hand-written guess. This does not invoke flatc: the converter's
        # `CamelToSnake` port (keywords, mixed case, `HTTPThing`) is covered in
        # patch_flatc_rust_test.py, and the Bazel fixture in
        # tests/digit_ns_binding is the compile-time proof against real flatc.
        self.assertEqual(
            schema_include_utils.flatc_rust_namespace_path("p05.probe"),
            "p_05::probe",
        )


if __name__ == "__main__":
    unittest.main()
