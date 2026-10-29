#!/usr/bin/env python3
"""Unit tests for patch_flatc_rust.py's absolute-path foreign-reference rewrite.

The pass replaces every foreign (cross-crate) `super::…::<rel path>::<Type>`
reference flatc emits with an absolute `crate::<dep>_generated::<escaped full
namespace>::<Type>` path, computed from the root bfbs. The adversarial namespace
shapes that broke the previous synthesis-based design
are included here as REGRESSION tests — each is now trivially handled (distinct
references -> distinct absolute paths, or a hard-fail on a genuinely ambiguous
reference).
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace as NS

sys.path.insert(0, str(Path(__file__).resolve().parent))

from patch_flatc_rust import (
    ForeignRefConflict,
    _build_rewrites,
    _inject_module_use_aliases,
    _reference_and_absolute,
    _referenced_foreign_refs,
    _rewrite_foreign_refs,
    patch_text,
)


class ReferenceComputationTest(unittest.TestCase):
    """The exact `super::…` reference flatc emits, and its absolute replacement,
    for representative real shapes (verified against the compiled fixtures)."""

    def test_disjoint_single_super_fsm(self) -> None:
        # local `fsm` -> foreign aorta.sys.AortaHeader (common prefix empty).
        ref, absolute = _reference_and_absolute(
            ["fsm"], ["aorta", "sys"], "AortaHeader", "aorta_base_generated"
        )
        self.assertEqual(ref, "super::aorta::sys::AortaHeader")
        self.assertEqual(
            absolute, "crate::aorta_base_generated::aorta::sys::AortaHeader"
        )

    def test_shared_prefix_double_super_stereo_sys(self) -> None:
        # local aorta.services.stereo -> foreign aorta.sys (common prefix aorta).
        ref, absolute = _reference_and_absolute(
            ["aorta", "services", "stereo"], ["aorta", "sys"], "AortaHeader",
            "aorta_base_generated",
        )
        self.assertEqual(ref, "super::super::sys::AortaHeader")
        self.assertEqual(
            absolute, "crate::aorta_base_generated::aorta::sys::AortaHeader"
        )

    def test_disjoint_foxglove_camera_calibration_stereo(self) -> None:
        # The stereo/CameraCalibration needle case: aorta.services.stereo ->
        # foxglove.CameraCalibration (disjoint root, 3 supers).
        ref, absolute = _reference_and_absolute(
            ["aorta", "services", "stereo"], ["foxglove"], "CameraCalibration",
            "CameraCalibration_generated",
        )
        self.assertEqual(ref, "super::super::super::foxglove::CameraCalibration")
        self.assertEqual(
            absolute, "crate::CameraCalibration_generated::foxglove::CameraCalibration"
        )

    def test_deep_shared_prefix_branch(self) -> None:
        # Shape: local aorta.branch.consumer -> aorta.branch.common
        # (common prefix aorta.branch, single super).
        ref, absolute = _reference_and_absolute(
            ["aorta", "branch", "consumer"], ["aorta", "branch", "common"],
            "BranchPhase", "branch_common_generated",
        )
        self.assertEqual(ref, "super::common::BranchPhase")
        self.assertEqual(
            absolute,
            "crate::branch_common_generated::aorta::branch::common::BranchPhase",
        )

    def test_keyword_namespace_and_type_escaping(self) -> None:
        # `follow` escapes to `follow_` in BOTH the reference and the absolute.
        ref, absolute = _reference_and_absolute(
            ["xvln"], ["aorta", "follow", "common"], "FollowPhase",
            "follow_common_generated",
        )
        self.assertEqual(ref, "super::aorta::follow_::common::FollowPhase")
        self.assertEqual(
            absolute,
            "crate::follow_common_generated::aorta::follow_::common::FollowPhase",
        )

    def test_mixed_case_namespace_is_snake_cased(self) -> None:
        # flatc's `Case::kSnake` for namespaces --
        # `ProbeCamel.HTTPThing` -> `probe_camel::httpthing` (type name kept).
        ref, absolute = _reference_and_absolute(
            ["aorta", "services", "camel"], ["ProbeCamel", "HTTPThing"],
            "CamelEnum", "camelcase_probe_generated",
        )
        self.assertEqual(ref, "super::super::super::probe_camel::httpthing::CamelEnum")
        self.assertEqual(
            absolute,
            "crate::camelcase_probe_generated::probe_camel::httpthing::CamelEnum",
        )

    def test_empty_foreign_namespace(self) -> None:
        # A dep with no `namespace` -> no namespace segment in either path.
        ref, absolute = _reference_and_absolute(
            ["aorta", "svc"], [], "TopType", "dep_generated"
        )
        self.assertEqual(ref, "super::super::TopType")
        self.assertEqual(absolute, "crate::dep_generated::TopType")


class BuildRewritesTest(unittest.TestCase):
    """`_build_rewrites` takes references `(emission_namespace, (foreign_ns,
    foreign_name, crate))` and returns `(path_rewrites, bare_alias_modules)`. Each
    reference is computed relative to ITS OWN emission namespace."""

    @staticmethod
    def _ref(emission, foreign_ns, name, crate):
        return (emission, (foreign_ns, name, crate))

    def test_same_namespace_foreign_goes_to_bare_alias_module(self) -> None:
        # A same-namespace foreign type (emission == foreign ns) is
        # emitted BARE -> a `use` ALIAS keyed by the EMISSION namespace, NOT a text
        # rewrite. `path_rewrites` stays empty at any depth.
        path1, modules1 = _build_rewrites(
            [self._ref("foxglove", "foxglove", "Time", "time_generated")]
        )
        self.assertEqual(path1, {})
        self.assertEqual(
            modules1, {("foxglove",): {"Time": "crate::time_generated::foxglove::Time"}}
        )
        path2, modules2 = _build_rewrites(
            [
                self._ref(
                    "aorta.nestdep.common",
                    "aorta.nestdep.common",
                    "NestedRoot",
                    "nested_dep_generated",
                )
            ]
        )
        self.assertEqual(path2, {})
        self.assertEqual(
            modules2,
            {
                ("aorta", "nestdep", "common"): {
                    "NestedRoot": (
                        "crate::nested_dep_generated::aorta::nestdep::common::NestedRoot"
                    )
                }
            },
        )

    def test_multi_emission_namespace_aliases_per_module(self) -> None:
        # A Tier-2 schema with root-local types in TWO namespaces --
        # each same-namespace foreign reference is computed against ITS OWN emission
        # namespace and lands in ITS OWN module, not a single first namespace.
        path, modules = _build_rewrites(
            [
                self._ref("p.alpha", "p.alpha", "AlphaDep", "dep_generated"),
                self._ref("p.beta", "p.beta", "BetaDep", "dep_generated"),
            ]
        )
        self.assertEqual(path, {})
        self.assertEqual(
            modules,
            {
                ("p", "alpha"): {"AlphaDep": "crate::dep_generated::p::alpha::AlphaDep"},
                ("p", "beta"): {"BetaDep": "crate::dep_generated::p::beta::BetaDep"},
            },
        )

    def test_same_namespace_post_escape_collision_hard_fails(self) -> None:
        # Two same-namespace foreign types `a.Self` and `a.Self_` both
        # escape to `Self_` in module `a` -> one Rust name, two crate paths, a `use`
        # clash -> hard-fail.
        with self.assertRaises(ForeignRefConflict) as ctx:
            _build_rewrites(
                [
                    self._ref("a", "a", "Self", "crate_a_generated"),
                    self._ref("a", "a", "Self_", "crate_b_generated"),
                ]
            )
        self.assertIn("Self_", str(ctx.exception))

    def test_bare_foreign_collides_with_root_local_escaped_name_hard_fails(self) -> None:
        # A foreign `a.Self` escapes to `Self_`, the SAME identifier a
        # root-local `a.Self_` produces -> a `use` alias would clash -> hard-fail.
        with self.assertRaises(ForeignRefConflict) as ctx:
            _build_rewrites(
                [self._ref("a", "a", "Self", "dep_generated")],  # foreign a.Self -> `Self_`
                {("a",): {"Self_"}},  # root-local a.Self_ -> `Self_`
            )
        self.assertIn("Self_", str(ctx.exception))

    def test_ambiguous_cross_namespace_path_hard_fails(self) -> None:
        # Two emission namespaces both emit `super::a::b::X` but for
        # DIFFERENT foreign types -> one `super::…` string, two crate paths, an
        # un-rewritable global text substitution -> hard-fail.
        with self.assertRaises(ForeignRefConflict) as ctx:
            _build_rewrites(
                [
                    self._ref("p.q", "p.a.b", "X", "dep_generated"),
                    self._ref("r.s", "r.a.b", "X", "dep_generated"),
                ]
            )
        self.assertIn("super::a::b::X", str(ctx.exception))

    def test_cross_namespace_foreign_does_not_collide_with_local_name(self) -> None:
        # A CROSS-namespace reference is a `super::…::Name` path (a TEXT rewrite),
        # textually distinct from a bare local `Name`, so a same-escaped-name
        # root-local type is NOT a collision (only BARE same-namespace aliases are).
        path_rewrites, bare_alias_modules = _build_rewrites(
            [self._ref("a.b", "a", "Thing", "dep_generated")],  # super::…::Thing
            {("a", "b"): {"Thing"}},
        )
        self.assertEqual(
            path_rewrites, {"super::Thing": "crate::dep_generated::a::Thing"}
        )
        self.assertEqual(bare_alias_modules, {})


class ReferencedKeysTest(unittest.TestCase):
    """FALSE-POSITIVE fix: the conflict check is scoped to the
    foreign types the ROOT actually references (root-local bfbs field/variant
    TypeIds), not the whole include closure."""

    @staticmethod
    def _is_root_local(tid, _loaded):
        return tid.declaration_file == "root.fbs"

    @staticmethod
    def _tid(ns, name, decl):
        return NS(namespace=ns, name=name, declaration_file=decl)

    @staticmethod
    def _foreign_keys(refs):
        # refs are (emission_namespace, (foreign_ns, foreign_name, crate)); the
        # membership tests only care about which foreign (ns, name) are referenced.
        return {(fns, fname) for _emission, (fns, fname, _crate) in refs}

    def test_only_referenced_targets_are_collected(self) -> None:
        tid = self._tid
        header = tid("aorta.sys", "AortaHeader", "aorta_base.fbs")
        loaded = NS(
            tables={
                # root-local: a field references AortaHeader.
                "Root": NS(
                    type_id=tid("app", "Root", "root.fbs"),
                    fields=[NS(target=header, union_variants=None)],
                ),
                # in the closure but NOT root-local and NOT referenced.
                "Unref": NS(
                    type_id=tid("other.pkg", "Unref", "other.fbs"), fields=[]
                ),
            },
            structs={},
            unions={},
            enums={},
        )
        keys = self._foreign_keys(_referenced_foreign_refs(loaded, self._is_root_local))
        self.assertIn(("aorta.sys", "AortaHeader"), keys)
        # An unreferenced closure type is NOT collected -> its conflicts (if any)
        # are unreachable and won't fail generation.
        self.assertNotIn(("other.pkg", "Unref"), keys)

    def test_root_local_union_variant_targets_are_collected(self) -> None:
        tid = self._tid
        variant = tid("dep.ns", "ArmA", "dep.fbs")
        loaded = NS(
            tables={},
            structs={},
            unions={
                "Choice": NS(
                    type_id=tid("app", "Choice", "root.fbs"),
                    variants=[NS(target=variant)],
                )
            },
            enums={},
        )
        keys = self._foreign_keys(_referenced_foreign_refs(loaded, self._is_root_local))
        self.assertIn(("dep.ns", "ArmA"), keys)

    def test_deprecated_fields_are_excluded(self) -> None:
        # flatc emits no accessor for a deprecated field -> no
        # reference -> its target must NOT enter the conflict/rewrite set.
        tid = self._tid
        live = tid("dep.ns", "Live", "dep.fbs")
        dead = tid("dep.ns", "Dead", "dep.fbs")
        loaded = NS(
            tables={
                "Root": NS(
                    type_id=tid("app", "Root", "root.fbs"),
                    fields=[
                        NS(target=live, union_variants=None, deprecated=False),
                        NS(target=dead, union_variants=None, deprecated=True),
                    ],
                )
            },
            structs={},
            unions={},
            enums={},
        )
        keys = self._foreign_keys(_referenced_foreign_refs(loaded, self._is_root_local))
        self.assertIn(("dep.ns", "Live"), keys)
        self.assertNotIn(("dep.ns", "Dead"), keys)

    def test_nested_flatbuffer_target_is_collected(self) -> None:
        # A `[ubyte] (nested_flatbuffer: "…")` field has target None
        # but flatc emits a `*_nested_flatbuffer()` accessor referencing the
        # nested root -> the modelled target must be collected.
        tid = self._tid
        nested = tid("aorta.nestdep.common", "NestedRoot", "nested_dep.fbs")
        loaded = NS(
            tables={
                "Root": NS(
                    type_id=tid("aorta.nestdep.consumer", "Root", "root.fbs"),
                    fields=[
                        NS(
                            target=None,
                            union_variants=None,
                            deprecated=False,
                            nested_flatbuffer=nested,
                        )
                    ],
                )
            },
            structs={},
            unions={},
            enums={},
        )
        keys = self._foreign_keys(_referenced_foreign_refs(loaded, self._is_root_local))
        self.assertIn(("aorta.nestdep.common", "NestedRoot"), keys)

    def test_foreign_root_type_is_not_in_field_keys(self) -> None:
        # `_referenced_foreign_keys` is the LOCAL-emission set (field /
        # variant / nested refs). The foreign root_type is emitted in ITS OWN
        # namespace (a separate nest), so it is handled separately by
        # `_compute_foreign_ref_rewrites` and must NOT appear here.
        tid = self._tid
        froot = tid("aorta.nestdep.common", "NestedRoot", "nested_dep.fbs")
        loaded = NS(
            tables={},
            structs={},
            unions={},
            enums={},
            root_type=froot,
        )
        keys = self._foreign_keys(_referenced_foreign_refs(loaded, self._is_root_local))
        self.assertNotIn(("aorta.nestdep.common", "NestedRoot"), keys)

    def test_round17_two_providers_same_subtree_distinct_refs(self) -> None:
        # FollowPhase (crate A) and FollowExtra (crate B) both in
        # aorta.follow.common -> DISTINCT references -> both rewritten, no
        # collision.
        rewrites, _ = _build_rewrites(
            [
                ("zzz", ("aorta.follow.common", "FollowPhase", "follow_common_generated")),
                ("zzz", ("aorta.follow.common", "FollowExtra", "follow_common_extra_generated")),
            ]
        )
        self.assertEqual(
            rewrites["super::aorta::follow_::common::FollowPhase"],
            "crate::follow_common_generated::aorta::follow_::common::FollowPhase",
        )
        self.assertEqual(
            rewrites["super::aorta::follow_::common::FollowExtra"],
            "crate::follow_common_extra_generated::aorta::follow_::common::FollowExtra",
        )

    def test_round19_20_shared_and_disjoint_same_segment_distinct_refs(self) -> None:
        # Three-layer xfoo shape: local aorta.xfoo.consumer, shared
        # aorta.xfoo.common, disjoint xfoo. The shared and disjoint references
        # are DISTINCT absolute paths (different crates) -> both handled, no
        # synthesis, no idempotency question.
        rewrites, _ = _build_rewrites(
            [
                ("aorta.xfoo.consumer", ("aorta.xfoo.common", "CollideShared", "collide_shared_generated")),
                ("aorta.xfoo.consumer", ("xfoo", "CollideDisjA", "collide_disjoint_a_generated")),
                ("aorta.xfoo.consumer", ("xfoo", "CollideDisjB", "collide_disjoint_b_generated")),
            ]
        )
        self.assertEqual(
            rewrites["super::common::CollideShared"],
            "crate::collide_shared_generated::aorta::xfoo::common::CollideShared",
        )
        self.assertEqual(
            rewrites["super::super::super::xfoo::CollideDisjA"],
            "crate::collide_disjoint_a_generated::xfoo::CollideDisjA",
        )
        self.assertEqual(
            rewrites["super::super::super::xfoo::CollideDisjB"],
            "crate::collide_disjoint_b_generated::xfoo::CollideDisjB",
        )

    def test_same_fqn_from_two_crates_hard_fails(self) -> None:
        # Same fully-qualified type from 2 crates -> same reference, different
        # absolute -> ambiguous.
        with self.assertRaises(ForeignRefConflict) as ctx:
            _build_rewrites(
                [
                    ("zzz", ("aorta.follow.common", "FollowPhase", "crate_a_generated")),
                    ("zzz", ("aorta.follow.common", "FollowPhase", "crate_b_generated")),
                ]
            )
        self.assertIn("FollowPhase", str(ctx.exception))

    def test_escaped_namespace_collision_hard_fails(self) -> None:
        # Escaped-segment shape: raw `aorta.type` and `aorta.type_`
        # both escape to `aorta::type_`; the SAME-named type in each yields the
        # SAME reference but different crates -> ambiguous (flatc cannot compile
        # this either).
        with self.assertRaises(ForeignRefConflict) as ctx:
            _build_rewrites(
                [
                    ("zzz", ("aorta.type", "X", "crate_a_generated")),
                    ("zzz", ("aorta.type_", "X", "crate_b_generated")),
                ]
            )
        self.assertIn("type_", str(ctx.exception))

    def test_round19_local_type_vs_foreign_namespace_is_harmless(self) -> None:
        # Shape (local type `bar` at aorta.foo + foreign namespace
        # aorta.foo.bar, a DESCENDANT of the local ns -> super_count 0): under
        # absolute paths the local type stays a bare `bar` in the local module
        # and the foreign `aorta.foo.bar.Thing` becomes
        # `crate::dep::aorta::foo::bar::Thing` -- DIFFERENT identifier spaces, no
        # collision, no hard-fail (the SYNTH design's `pub mod bar` E0428 is
        # gone because no module is synthesized).
        rewrites, _ = _build_rewrites(
            [("aorta.foo", ("aorta.foo.bar", "Thing", "dep_generated"))]
        )
        # descendant reference has no `super::` prefix.
        self.assertEqual(
            rewrites["bar::Thing"], "crate::dep_generated::aorta::foo::bar::Thing"
        )


class RewriteApplicationTest(unittest.TestCase):
    _R = {
        "super::aorta::sys::AortaHeader": "crate::aorta_base_generated::aorta::sys::AortaHeader",
    }

    def test_basic_rewrite_all_occurrences(self) -> None:
        text = (
            "pub fn h(&self) -> Option<super::aorta::sys::AortaHeader<'a>> {}\n"
            "pub aorta_header: Option<super::aorta::sys::AortaHeader<'a>>,\n"
        )
        patched, changed = _rewrite_foreign_refs(text, self._R)
        self.assertTrue(changed)
        self.assertNotIn("super::aorta::sys::AortaHeader", patched)
        self.assertEqual(
            patched.count("crate::aorta_base_generated::aorta::sys::AortaHeader"), 2
        )

    def test_shorter_chain_not_matched_inside_longer(self) -> None:
        # A rewrite for `super::sys::X` must NOT corrupt `super::super::sys::X`.
        text = "Option<super::super::sys::AortaHeader>\n"
        rewrites = {"super::sys::AortaHeader": "crate::A::foo::sys::AortaHeader"}
        patched, changed = _rewrite_foreign_refs(text, rewrites)
        self.assertFalse(changed)
        self.assertEqual(patched, text)

    def test_trailing_boundary_no_partial_type_match(self) -> None:
        # `AortaHeader` must not match inside `AortaHeaderBuilder`.
        text = "x: super::aorta::sys::AortaHeaderBuilder,\n"
        patched, changed = _rewrite_foreign_refs(text, self._R)
        self.assertFalse(changed)
        self.assertEqual(patched, text)

    def test_enum_variant_suffix_preserved(self) -> None:
        rewrites = {
            "super::aorta::follow_::common::FollowPhase":
            "crate::follow_common_generated::aorta::follow_::common::FollowPhase",
        }
        text = "Some(super::aorta::follow_::common::FollowPhase::UNKNOWN)\n"
        patched, _ = _rewrite_foreign_refs(text, rewrites)
        self.assertIn(
            "crate::follow_common_generated::aorta::follow_::common::FollowPhase::UNKNOWN",
            patched,
        )

    def test_path_ref_inside_string_literal_is_masked(self) -> None:
        # String literals are masked from PATH rewriting too: a code `super::…`
        # ref on a line is rewritten, but the same text inside a `"…"` literal is
        # not (defensive -- flatc does not emit `super::` in a string, but masking
        # makes it structurally impossible to corrupt any literal).
        text = (
            'let s = "super::aorta::sys::AortaHeader";'
            " let _: super::aorta::sys::AortaHeader;\n"
        )
        patched, changed = _rewrite_foreign_refs(text, self._R)
        self.assertTrue(changed)
        self.assertIn('"super::aorta::sys::AortaHeader"', patched)  # string verbatim
        self.assertIn(
            "_: crate::aorta_base_generated::aorta::sys::AortaHeader;", patched
        )

    def test_comment_lines_untouched(self) -> None:
        text = "  // super::aorta::sys::AortaHeader is the header\n"
        patched, changed = _rewrite_foreign_refs(text, self._R)
        self.assertFalse(changed)
        self.assertEqual(patched, text)

    def test_empty_rewrites_is_noop(self) -> None:
        text = "super::aorta::sys::AortaHeader\n"
        patched, changed = _rewrite_foreign_refs(text, {})
        self.assertFalse(changed)
        self.assertEqual(patched, text)


class InjectModuleUseAliasesTest(unittest.TestCase):
    """Same-namespace foreign types are resolved by a `use` alias
    injected into the module where flatc emits their BARE references, so Rust
    name resolution covers EVERY type position (incl. the `*const T` struct cast)."""

    def test_aliases_injected_into_leaf_module(self) -> None:
        # A deep (aorta.shadowdeep.common) namespace: the `use` lines land just
        # inside the LEAF `pub mod common {`, not the intermediate modules.
        text = (
            "pub mod aorta {\n"
            "  use crate::deepshadow_dep_generated::*;\n"
            "pub mod shadowdeep {\n"
            "pub mod common {\n"
            "  use crate::deepshadow_dep_generated::*;\n"
            "  pub fn child(&self) -> &ShadowStruct {\n"
            "    unsafe { &*(p as *const ShadowStruct) }\n"
            "  }\n"
            "}  // pub mod common\n"
            "}  // pub mod shadowdeep\n"
            "}  // pub mod aorta\n"
        )
        aliases = {
            "ShadowStruct": (
                "crate::deepshadow_dep_generated::aorta::shadowdeep::common::ShadowStruct"
            ),
            "ShadowEnum": (
                "crate::deepshadow_dep_generated::aorta::shadowdeep::common::ShadowEnum"
            ),
        }
        patched, changed = _inject_module_use_aliases(
            text, {("aorta", "shadowdeep", "common"): aliases}
        )
        self.assertTrue(changed)
        # both `use` lines sit immediately after the LEAF `pub mod common {`.
        common_open = patched.index("pub mod common {\n")
        after = patched[common_open:]
        self.assertTrue(
            after.startswith(
                "pub mod common {\n"
                "  use crate::deepshadow_dep_generated::aorta::shadowdeep::common::ShadowEnum;\n"
                "  use crate::deepshadow_dep_generated::aorta::shadowdeep::common::ShadowStruct;\n"
            )
        )
        # they were NOT injected into the intermediate `aorta` / `shadowdeep`.
        self.assertNotIn(
            "pub mod aorta {\n  use crate::deepshadow_dep_generated::aorta::shadowdeep",
            patched,
        )

    def test_two_nests_inject_into_respective_leaves(self) -> None:
        # Local types in `mrlocal.inner` + a foreign root_type in
        # `mrforeign.deep` -> TWO pub-mod nests. Each module's alias lands in ITS
        # OWN leaf (the foreign-root alias in `mrforeign::deep`, not the local one).
        text = (
            "pub mod mrforeign {\n"
            "pub mod deep {\n"
            "  use crate::dep_generated::*;\n"
            "  pub fn root_as_x() -> ForeignRoot {}\n"
            "}  // pub mod deep\n"
            "}  // pub mod mrforeign\n"
            "pub mod mrlocal {\n"
            "pub mod inner {\n"
            "  use crate::dep_generated::*;\n"
            "  pub struct Marker;\n"
            "}  // pub mod inner\n"
            "}  // pub mod mrlocal\n"
        )
        modules = {
            ("mrforeign", "deep"): {
                "ForeignRoot": "crate::dep_generated::mrforeign::deep::ForeignRoot"
            },
        }
        patched, changed = _inject_module_use_aliases(text, modules)
        self.assertTrue(changed)
        deep_open = patched.index("pub mod deep {\n")
        self.assertTrue(
            patched[deep_open:].startswith(
                "pub mod deep {\n"
                "  use crate::dep_generated::mrforeign::deep::ForeignRoot;\n"
            )
        )
        # the local leaf got NO alias.
        inner_open = patched.index("pub mod inner {\n")
        self.assertTrue(
            patched[inner_open:].startswith(
                "pub mod inner {\n  use crate::dep_generated::*;\n"
            )
        )

    def test_shared_prefix_fork_resolves_leaf_by_full_path(self) -> None:
        # A foreign root at `sfork::mid::leaf` and a local leaf at
        # `sfork::leaf` -- TWO modules named `leaf`. The alias for module
        # (sfork, leaf) must land in the DIRECT child of `sfork`, NOT the first
        # `leaf` (which is nested inside `sfork::mid`). The stack finder matches by
        # FULL path.
        text = (
            "pub mod sfork {\n"
            "pub mod mid {\n"
            "pub mod leaf {\n"
            "  use crate::dep_generated::*;\n"
            "  pub fn root_as_x() -> ForeignRoot {}\n"
            "}  // pub mod leaf\n"
            "}  // pub mod mid\n"
            "pub mod leaf {\n"
            "  use crate::dep_generated::*;\n"
            "  pub fn dep() -> LocalDep {}\n"
            "}  // pub mod leaf\n"
            "}  // pub mod sfork\n"
        )
        modules = {
            ("sfork", "leaf"): {"LocalDep": "crate::dep_generated::sfork::leaf::LocalDep"},
            ("sfork", "mid", "leaf"): {
                "ForeignRoot": "crate::dep_generated::sfork::mid::leaf::ForeignRoot"
            },
        }
        patched, changed = _inject_module_use_aliases(text, modules)
        self.assertTrue(changed)
        # LocalDep landed in the DIRECT `sfork::leaf` (the one holding `dep()`).
        self.assertIn(
            "pub mod leaf {\n"
            "  use crate::dep_generated::sfork::leaf::LocalDep;\n"
            "  use crate::dep_generated::*;\n"
            "  pub fn dep() -> LocalDep {}\n",
            patched,
        )
        # ForeignRoot landed in `sfork::mid::leaf` (the one holding `root_as_x`).
        self.assertIn(
            "pub mod leaf {\n"
            "  use crate::dep_generated::sfork::mid::leaf::ForeignRoot;\n"
            "  use crate::dep_generated::*;\n"
            "  pub fn root_as_x() -> ForeignRoot {}\n",
            patched,
        )
        # NOT swapped: the ForeignRoot leaf (root_as_x) comes before the LocalDep leaf.
        self.assertLess(patched.index("root_as_x"), patched.index("dep() -> LocalDep"))

    def test_no_aliases_is_noop(self) -> None:
        text = "pub mod a {\n}\n"
        patched, changed = _inject_module_use_aliases(text, {})
        self.assertFalse(changed)
        self.assertEqual(patched, text)
        patched, changed = _inject_module_use_aliases(text, {("a",): {}})
        self.assertFalse(changed)
        self.assertEqual(patched, text)

    def test_missing_module_chain_is_noop(self) -> None:
        # If the (escaped) chain is not found the text is returned unchanged
        # rather than mis-injected.
        text = "pub mod other {\n}\n"
        patched, changed = _inject_module_use_aliases(
            text, {("a", "b"): {"X": "crate::dep::a::b::X"}}
        )
        self.assertFalse(changed)
        self.assertEqual(patched, text)

    def test_empty_namespace_injects_at_crate_root(self) -> None:
        text = "// @generated\nuse core::mem;\n\npub fn root_as_x() {}\n"
        patched, changed = _inject_module_use_aliases(
            text, {(): {"X": "crate::dep::X"}}
        )
        self.assertTrue(changed)
        self.assertIn("  use crate::dep::X;\n", patched)
        # injected before the first item.
        self.assertLess(patched.index("use crate::dep::X;"), patched.index("pub fn root_as_x"))


class PatchTextTest(unittest.TestCase):
    def test_rewrite_plus_namespace_reexport(self) -> None:
        text = (
            "pub mod fsm {\n"
            "  pub fn h(&self) -> Option<super::aorta::sys::AortaHeader> {}\n"
            "}\n"
        )
        rewrites = {
            "super::aorta::sys::AortaHeader": "crate::aorta_base_generated::aorta::sys::AortaHeader",
        }
        patched, changed = patch_text(text, rewrites)
        self.assertTrue(changed)
        self.assertIn(
            "crate::aorta_base_generated::aorta::sys::AortaHeader", patched
        )
        self.assertIn("pub use self::fsm::*;", patched)

    def test_patch_text_injects_bare_alias_and_rewrites_path(self) -> None:
        # PATH ref text-rewritten; BARE ref resolved by an injected `use` alias in
        # the leaf module -- both via patch_text.
        text = (
            "pub mod app {\n"
            "  use crate::dep_generated::*;\n"
            "  pub fn h(&self) -> Option<super::aorta::sys::AortaHeader> {}\n"
            "  pub fn child(&self) -> &Nested { unsafe { &*(p as *const Nested) } }\n"
            "}  // pub mod app\n"
        )
        path_rewrites = {
            "super::aorta::sys::AortaHeader": "crate::aorta_base_generated::aorta::sys::AortaHeader",
        }
        bare_alias_modules = {("app",): {"Nested": "crate::dep_generated::app::Nested"}}
        patched, changed = patch_text(text, path_rewrites, bare_alias_modules)
        self.assertTrue(changed)
        self.assertIn(
            "crate::aorta_base_generated::aorta::sys::AortaHeader", patched
        )
        self.assertIn("  use crate::dep_generated::app::Nested;\n", patched)
        # the bare `*const Nested` cast is left as-is (resolved by the alias).
        self.assertIn("as *const Nested)", patched)
        self.assertIn("pub use self::app::*;", patched)

    def test_two_top_level_nests_both_reexported(self) -> None:
        # A file with two top-level pub-mod nests flattens BOTH tops.
        text = (
            "pub mod mrforeign {\n"
            "pub mod deep {\n"
            "}  // pub mod deep\n"
            "}  // pub mod mrforeign\n"
            "pub mod mrlocal {\n"
            "pub mod inner {\n"
            "}  // pub mod inner\n"
            "}  // pub mod mrlocal\n"
        )
        patched, changed = patch_text(text, None)
        self.assertTrue(changed)
        self.assertIn("pub use self::mrforeign::*;", patched)
        self.assertIn("pub use self::mrlocal::*;", patched)

    def test_no_rewrites_still_appends_namespace_reexport(self) -> None:
        # A leaf schema (no --root-bfbs): only the local flattening re-export.
        text = "pub mod foxglove {\n}\n"
        patched, changed = patch_text(text, None)
        self.assertTrue(changed)
        self.assertIn("pub use self::foxglove::*;", patched)

    def test_idempotent_namespace_reexport(self) -> None:
        text = "pub mod fsm {\n}\n\npub use self::fsm::*;\n"
        patched, changed = patch_text(text, None)
        self.assertFalse(changed)
        self.assertEqual(patched, text)


if __name__ == "__main__":
    unittest.main()
