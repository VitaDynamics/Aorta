#!/usr/bin/env python3
"""Unit tests for message/tools/schema_reflection.py.

Two fixture classes:

- Real, flatc-compiled `.bfbs` files: three test-only fixtures under
  `message/tools/tests/schema_reflection_fixtures/*.fbs` (one per
  reachable `advanced_features` bit) plus three REAL production schemas
  already in the registry (`all_types`, `guard_fixture_owner`,
  `guard_fixture_includer` — all `bfbs = True`), reused here rather than
  duplicated, for field-shape and root-local/include-classification
  coverage.
- Synthetic `.bfbs` buffers built directly via the reflection Python
  object API's `Pack()` (`_pack_schema_bytes`/`_pack_minimal_schema`): used
  where no `.fbs` source text can produce the shape through this pinned
  flatc at all — notably `DefaultVectorsAndStrings`, whose `--binary`
  output kind is categorically excluded from
  `Parser::SupportsDefaultVectorsAndStrings()`'s allowed-language check (so
  no `.fbs` using a vector/string default can ever compile to a `.bfbs`
  through the CLI) — and for isolated loader-mechanics assertions
  (out-of-order field ids, ambiguous bare names, a malformed
  discriminator) that don't need a realistic whole-schema fixture.
"""

from __future__ import annotations

import importlib
import os
import math
import struct
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import flatbuffers

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from reflection.AdvancedFeatures import AdvancedFeatures  # noqa: E402
from reflection.BaseType import BaseType  # noqa: E402
from reflection.Enum import EnumT  # noqa: E402
from reflection.EnumVal import EnumValT  # noqa: E402
from reflection.Field import FieldT  # noqa: E402
from reflection.Object import ObjectT  # noqa: E402
from reflection.Schema import SchemaT  # noqa: E402
from reflection.SchemaFile import SchemaFileT  # noqa: E402
from reflection.Type import TypeT  # noqa: E402

import schema_reflection as sr  # noqa: E402


def _repo_root() -> Path:
    srcdir = os.environ.get("TEST_SRCDIR")
    workspace = os.environ.get("TEST_WORKSPACE")
    if srcdir and workspace:
        candidate = Path(srcdir) / workspace
        if (candidate / "message" / "schemas" / "registry.bzl").exists():
            return candidate
    # Fallback for running directly with `python3` outside `bazel test`
    # (needs `reflection`/`flatbuffers` on PYTHONPATH some other way).
    return THIS_DIR.parent.parent


def _load_fixture(rel_path: str) -> sr.LoadedSchema:
    return sr.load_bfbs_file(str(_repo_root() / rel_path))


def _pack_schema_bytes(schema_t: SchemaT) -> bytes:
    builder = flatbuffers.Builder(256)
    root = schema_t.Pack(builder)
    builder.Finish(root)
    return bytes(builder.Output())


def _pack_minimal_schema(
    advanced_features: int = 0, objects=None, enums=None, with_fbs_files: bool = True
) -> bytes:
    # Every bfbs the pinned flatc produces carries `fbs_files` (the include
    # graph `load_bfbs` derives root-local classification from), so the
    # synthetic fixtures carry a minimal single-file graph by default to
    # stay representative; `with_fbs_files=False` exists only for the
    # dedicated missing-graph hard-error test.
    fbs_files = [SchemaFileT(filename="//synthetic.fbs", includedFilenames=[])]
    return _pack_schema_bytes(
        SchemaT(
            objects=objects if objects is not None else [],
            enums=enums if enums is not None else [],
            advancedFeatures=advanced_features,
            fbsFiles=fbs_files if with_fbs_files else None,
        )
    )


class AdvancedFeaturesBitmaskTest(unittest.TestCase):
    """Requirement: 'Declare and CHECK a supported Schema.advanced_features
    bitmask — hard error on anything unsupported, explicitly including
    DefaultVectorsAndStrings ... each supported/unsupported feature bit
    gets a loader fixture.'"""

    def test_no_advanced_features_is_accepted(self) -> None:
        sr.load_bfbs(_pack_minimal_schema(0))

    def test_advanced_array_features_bit_accepted(self) -> None:
        sr.load_bfbs(_pack_minimal_schema(AdvancedFeatures.AdvancedArrayFeatures))

    def test_advanced_union_features_bit_accepted(self) -> None:
        sr.load_bfbs(_pack_minimal_schema(AdvancedFeatures.AdvancedUnionFeatures))

    def test_optional_scalars_bit_accepted(self) -> None:
        sr.load_bfbs(_pack_minimal_schema(AdvancedFeatures.OptionalScalars))

    def test_all_supported_bits_together_accepted(self) -> None:
        sr.load_bfbs(_pack_minimal_schema(sr.SUPPORTED_ADVANCED_FEATURES))

    def test_default_vectors_and_strings_bit_hard_errors(self) -> None:
        # Not producible via a real flatc --binary compile at all (see
        # module docstring) — this is the ONE bit that can only be tested
        # via a synthetic buffer, which is exactly why it needs one.
        with self.assertRaises(sr.SchemaLoadError) as ctx:
            sr.load_bfbs(_pack_minimal_schema(AdvancedFeatures.DefaultVectorsAndStrings))
        self.assertIn("DefaultVectorsAndStrings", str(ctx.exception))

    def test_unsupported_bit_mixed_with_supported_bits_still_hard_errors(self) -> None:
        combined = sr.SUPPORTED_ADVANCED_FEATURES | AdvancedFeatures.DefaultVectorsAndStrings
        with self.assertRaises(sr.SchemaLoadError):
            sr.load_bfbs(_pack_minimal_schema(combined))


class RealCompiledFixtureTest(unittest.TestCase):
    """Real flatc --binary --schema output for the three test-only fixture
    schemas (one per non-synthetic advanced_features bit)."""

    def test_optional_scalars_fixture_loads_and_sets_the_bit(self) -> None:
        loaded = _load_fixture(
            "message/tools/tests/schema_reflection_fixtures/optional_scalars.bfbs"
        )
        self.assertEqual(loaded.advanced_features, AdvancedFeatures.OptionalScalars)
        field = loaded.tables["fx.Foo"].fields[0]
        self.assertEqual(field.name, "x")
        self.assertTrue(field.optional)

    def test_advanced_union_features_fixture_folds_vector_discriminator(self) -> None:
        loaded = _load_fixture(
            "message/tools/tests/schema_reflection_fixtures/advanced_union.bfbs"
        )
        self.assertEqual(loaded.advanced_features, AdvancedFeatures.AdvancedUnionFeatures)
        fields = loaded.tables["fx.Foo"].fields
        # The synthesized `items_type` VECTOR discriminator (flatc emits a
        # parallel [ubyte] of per-element discriminators for a
        # vector-of-union field, not a single scalar UType) must be folded
        # away, leaving exactly the one logical `items` field.
        self.assertEqual([f.name for f in fields], ["items"])
        self.assertEqual(fields[0].shape, "vector<union:MyUnion>")
        self.assertEqual(
            {v.alias for v in fields[0].union_variants}, {"A", "B"}
        )

    def test_advanced_array_features_fixture_loads_correctly(self) -> None:
        # Real production usage exists (a fixed-size array inside a struct,
        # such as a quaternion stored as `[float:4]`) — discovered via the full-registry
        # `--shadow-check` sweep, not anticipated up front.
        loaded = _load_fixture(
            "message/tools/tests/schema_reflection_fixtures/advanced_array.bfbs"
        )
        self.assertEqual(loaded.advanced_features, AdvancedFeatures.AdvancedArrayFeatures)
        struct_def = loaded.structs["fx.Foo"]
        self.assertEqual(struct_def.fields[0].name, "vals")
        self.assertEqual(struct_def.fields[0].shape, "array<scalar:int,4>")


class RealProductionSchemaTest(unittest.TestCase):
    """Field-shape and root-local/include-classification coverage against
    REAL production schemas already in the registry — not duplicated as
    new fixtures."""

    def test_all_types_covers_every_field_shape_category(self) -> None:
        loaded = _load_fixture("message/schemas/topic/example/all_types.bfbs")
        fields_by_name = {f.name: f for f in loaded.tables["demo.AllTypes"].fields}
        self.assertEqual(fields_by_name["aorta_header"].shape, "table:AortaHeader")
        self.assertEqual(fields_by_name["flag"].shape, "scalar:bool")
        self.assertEqual(fields_by_name["text"].shape, "string")
        self.assertEqual(fields_by_name["color"].shape, "enum:Color")
        self.assertEqual(fields_by_name["pose"].shape, "struct:Vec3")
        self.assertEqual(fields_by_name["child"].shape, "table:Child")
        self.assertEqual(fields_by_name["payload"].shape, "union:PayloadUnion")
        self.assertEqual(fields_by_name["bytes"].shape, "vector<scalar:ubyte>")
        self.assertEqual(fields_by_name["names"].shape, "vector<string>")
        self.assertEqual(fields_by_name["poses"].shape, "vector<struct:Vec3>")
        self.assertEqual(fields_by_name["children"].shape, "vector<table:Child>")
        self.assertEqual(fields_by_name["colors"].shape, "vector<enum:Color>")

        # Fields come back sorted by id (0..27), independent of any
        # bfbs-native ordering (verified separately not to be id order).
        self.assertEqual(
            [f.id for f in loaded.tables["demo.AllTypes"].fields],
            sorted(f.id for f in loaded.tables["demo.AllTypes"].fields),
        )

        vec3 = loaded.structs["demo.Vec3"]
        self.assertEqual(vec3.bytesize, 12)  # 3 x float32

        self.assertEqual(loaded.enums["demo.Color"].base_type, "ubyte")
        self.assertEqual(
            loaded.enums["demo.Color"].values, [("RED", 0), ("GREEN", 1), ("BLUE", 2)]
        )

        self.assertEqual(loaded.root_type.name, "AllTypes")
        self.assertEqual(loaded.root_type.kind, "table")

    def test_guard_fixture_union_variant_aliases(self) -> None:
        loaded = _load_fixture("message/schemas/topic/example/guard_fixture_owner.bfbs")
        union = loaded.unions["aorta.example.guard_fixture.FixtureUnion"]
        # In THIS repo, every union member's flatc-assigned alias happens
        # to equal its bare target-type name (no schema uses a custom
        # `Alias:Target` union member) — the OLD regex parser's
        # `local_type_base`-derived variant list assumes this always holds.
        # This test documents that the assumption is currently harmless in
        # practice, not that the loader itself assumes it (it doesn't —
        # `UnionVariantDef.alias` is read straight from `EnumVal.name`).
        self.assertEqual(
            [(v.alias, v.target.name) for v in union.variants],
            [("FixtureLeaf", "FixtureLeaf"), ("FixtureMeta", "FixtureMeta")],
        )

    def test_root_local_vs_include_classification(self) -> None:
        owner_loaded = _load_fixture("message/schemas/topic/example/guard_fixture_owner.bfbs")
        owner_leaf = owner_loaded.tables["aorta.example.guard_fixture.FixtureLeaf"].type_id
        owner_header = owner_loaded.tables["aorta.sys.AortaHeader"].type_id
        self.assertTrue(sr.is_root_local(owner_leaf, owner_loaded))
        self.assertFalse(sr.is_root_local(owner_header, owner_loaded))

        includer_loaded = _load_fixture(
            "message/schemas/topic/example/guard_fixture_includer.bfbs"
        )
        includer_root = includer_loaded.tables[
            "aorta.example.guard_fixture.FixtureIncluderRoot"
        ].type_id
        includer_leaf = includer_loaded.tables[
            "aorta.example.guard_fixture.FixtureLeaf"
        ].type_id
        self.assertTrue(sr.is_root_local(includer_root, includer_loaded))
        # FixtureLeaf is declared in guard_fixture_owner.fbs even though
        # it's visible (transitively, via the owner include) from the
        # includer's root — must classify as included, not local, from the
        # includer's point of view.
        self.assertFalse(sr.is_root_local(includer_leaf, includer_loaded))


class RootDeclarationFileDerivationTest(unittest.TestCase):
    """`_derive_root_declaration_file`: the fbs_files include graph is the
    ONLY sound source for root-local classification — a previous
    implementation assumed `declaration_file` is always `"//" + <root
    basename>`, which held for every in-repo bfbs but broke in an external
    Bzlmod consumer (local_path_override), where even the root file's own
    entry is a `../`-chain plus the symlink-resolved ABSOLUTE path. These
    tests pin the graph derivation itself, including the encodings and
    graph shapes that broke or would break shape-based alternatives."""

    def test_single_file_schema_is_its_own_root(self) -> None:
        self.assertEqual(
            sr._derive_root_declaration_file([("//only.fbs", [])]),
            "//only.fbs",
        )

    def test_include_chain_root_is_the_never_included_file(self) -> None:
        self.assertEqual(
            sr._derive_root_declaration_file(
                [
                    ("//../../system/aorta_base.fbs", []),
                    ("//echo.fbs", ["//../../system/aorta_base.fbs"]),
                ]
            ),
            "//echo.fbs",
        )

    def test_diamond_includes_still_yield_one_root(self) -> None:
        self.assertEqual(
            sr._derive_root_declaration_file(
                [
                    ("//root.fbs", ["//a.fbs", "//b.fbs"]),
                    ("//a.fbs", ["//shared.fbs"]),
                    ("//b.fbs", ["//shared.fbs"]),
                    ("//shared.fbs", []),
                ]
            ),
            "//root.fbs",
        )

    def test_external_consumer_absolute_path_encoding_still_derives_root(self) -> None:
        # The EXACT encoding shape observed in the release validator's
        # external-consumer build (../-chain to filesystem root + resolved
        # absolute path) — the case that broke the basename-equality
        # implementation. The graph is self-consistent, so derivation
        # works regardless of the string's shape.
        root = "//../../../../../../Users/u/repo/message/schemas/service/example/echo.fbs"
        include = "//../../../../../../Users/u/repo/message/schemas/system/aorta_base.fbs"
        self.assertEqual(
            sr._derive_root_declaration_file([(include, []), (root, [include])]),
            root,
        )

    def test_include_sharing_the_root_basename_is_not_the_root(self) -> None:
        # A root including a DIFFERENT file that happens to carry the
        # root's own basename — the case a basename-vs-basename comparison
        # would misclassify (the include's types would look root-local).
        self.assertEqual(
            sr._derive_root_declaration_file(
                [
                    ("//echo.fbs", ["//sub/echo.fbs"]),
                    ("//sub/echo.fbs", []),
                ]
            ),
            "//echo.fbs",
        )

    def test_empty_graph_hard_errors(self) -> None:
        with self.assertRaises(sr.SchemaLoadError):
            sr._derive_root_declaration_file([])

    def test_load_bfbs_without_fbs_files_hard_errors(self) -> None:
        # End-to-end: a bfbs missing the include graph entirely (an
        # incompatible producer) must be rejected at load time, not
        # silently classified.
        with self.assertRaises(sr.SchemaLoadError) as ctx:
            sr.load_bfbs(_pack_minimal_schema(with_fbs_files=False))
        self.assertIn("fbs_files", str(ctx.exception))

    def test_multiple_root_candidates_hard_error(self) -> None:
        with self.assertRaises(sr.SchemaLoadError):
            sr._derive_root_declaration_file([("//a.fbs", []), ("//b.fbs", [])])

    def test_cycle_with_no_root_candidate_hard_errors(self) -> None:
        with self.assertRaises(sr.SchemaLoadError):
            sr._derive_root_declaration_file(
                [("//a.fbs", ["//b.fbs"]), ("//b.fbs", ["//a.fbs"])]
            )

    def test_real_fixture_root_matches_declared_tables(self) -> None:
        # End-to-end on a real bfbs: the derived root_declaration_file is
        # exactly the declaration_file of the tables genuinely declared in
        # the root schema, and not of the included ones.
        loaded = _load_fixture("message/schemas/topic/example/guard_fixture_includer.bfbs")
        root_table = loaded.tables["aorta.example.guard_fixture.FixtureIncluderRoot"].type_id
        included_table = loaded.tables["aorta.example.guard_fixture.FixtureLeaf"].type_id
        self.assertEqual(loaded.root_declaration_file, root_table.declaration_file)
        self.assertNotEqual(loaded.root_declaration_file, included_table.declaration_file)


class LoaderMechanicsTest(unittest.TestCase):
    """Synthetic-buffer coverage for loader mechanics that don't need a
    realistic whole-schema fixture."""

    def test_fields_are_sorted_by_id_regardless_of_declaration_order(self) -> None:
        obj = ObjectT(
            name="ns.Foo",
            isStruct=False,
            declarationFile="//foo.fbs",
            fields=[
                FieldT(name="b", type=TypeT(baseType=BaseType.Int), id=1),
                FieldT(name="a", type=TypeT(baseType=BaseType.Int), id=0),
                FieldT(name="c", type=TypeT(baseType=BaseType.Int), id=2),
            ],
        )
        loaded = sr.load_bfbs(_pack_minimal_schema(objects=[obj]))
        fields = loaded.tables["ns.Foo"].fields
        self.assertEqual([f.name for f in fields], ["a", "b", "c"])
        self.assertEqual([f.id for f in fields], [0, 1, 2])

    def test_resolve_bare_is_unambiguous_for_a_unique_name(self) -> None:
        obj = ObjectT(name="ns.X", isStruct=False, declarationFile="//a.fbs", fields=[])
        loaded = sr.load_bfbs(_pack_minimal_schema(objects=[obj]))
        self.assertEqual(loaded.resolve_bare(loaded.tables, "X"), "ns.X")

    def test_resolve_bare_hard_errors_on_cross_namespace_collision(self) -> None:
        obj1 = ObjectT(name="ns1.X", isStruct=False, declarationFile="//a.fbs", fields=[])
        obj2 = ObjectT(name="ns2.X", isStruct=False, declarationFile="//b.fbs", fields=[])
        loaded = sr.load_bfbs(_pack_minimal_schema(objects=[obj1, obj2]))
        with self.assertRaises(sr.SchemaLoadError) as ctx:
            loaded.resolve_bare(loaded.tables, "X")
        self.assertIn("ambiguous", str(ctx.exception))

    def test_resolve_bare_hard_errors_on_unknown_name(self) -> None:
        loaded = sr.load_bfbs(_pack_minimal_schema())
        with self.assertRaises(sr.SchemaLoadError):
            loaded.resolve_bare(loaded.tables, "DoesNotExist")

    def test_malformed_discriminator_without_type_suffix_hard_errors(self) -> None:
        # A UType-typed field that does NOT end in "_type" violates the
        # discriminator-folding assumption outright.
        obj = ObjectT(
            name="ns.Foo",
            isStruct=False,
            declarationFile="//foo.fbs",
            fields=[FieldT(name="weird", type=TypeT(baseType=BaseType.UType), id=0)],
        )
        with self.assertRaises(sr.SchemaLoadError) as ctx:
            sr.load_bfbs(_pack_minimal_schema(objects=[obj]))
        self.assertIn("does not end in '_type'", str(ctx.exception))

    def test_discriminator_without_sibling_field_hard_errors(self) -> None:
        obj = ObjectT(
            name="ns.Foo",
            isStruct=False,
            declarationFile="//foo.fbs",
            fields=[FieldT(name="payload_type", type=TypeT(baseType=BaseType.UType), id=0)],
        )
        with self.assertRaises(sr.SchemaLoadError) as ctx:
            sr.load_bfbs(_pack_minimal_schema(objects=[obj]))
        self.assertIn("no sibling union field", str(ctx.exception))

    def test_field_target_distinguishes_same_bare_name_across_namespaces(self) -> None:
        """Regression: `_load_object_fields` previously discarded the
        qualified TypeId `_field_shape` computes for a referenced type
        (`shape, _ = _field_shape(...)`), keeping only the bare-name
        `shape` string -- directly contradicting the requirement that
        the model carry qualified TypeId, not bare names, precisely to
        distinguish two fields referencing `ns1.X` and `ns2.X`: genuinely
        different qualified types sharing one bare name `X`."""
        obj_x1 = ObjectT(name="ns1.X", isStruct=False, declarationFile="//a.fbs", fields=[])
        obj_x2 = ObjectT(name="ns2.X", isStruct=False, declarationFile="//b.fbs", fields=[])
        container = ObjectT(
            name="ns.Container",
            isStruct=False,
            declarationFile="//c.fbs",
            fields=[
                FieldT(name="ref1", type=TypeT(baseType=BaseType.Obj, index=0), id=0),
                FieldT(name="ref2", type=TypeT(baseType=BaseType.Obj, index=1), id=1),
            ],
        )
        loaded = sr.load_bfbs(_pack_minimal_schema(objects=[obj_x1, obj_x2, container]))
        fields_by_name = {f.name: f for f in loaded.tables["ns.Container"].fields}
        ref1, ref2 = fields_by_name["ref1"], fields_by_name["ref2"]

        # Both fields carry the IDENTICAL bare-name shape string -- shape
        # alone cannot tell them apart.
        self.assertEqual(ref1.shape, "table:X")
        self.assertEqual(ref2.shape, "table:X")
        # ...but `target`, the qualified TypeId, must distinguish them.
        self.assertIsNotNone(ref1.target)
        self.assertIsNotNone(ref2.target)
        self.assertEqual(ref1.target.namespace, "ns1")
        self.assertEqual(ref2.target.namespace, "ns2")
        self.assertNotEqual(ref1.target, ref2.target)

    def test_scalar_field_target_is_none(self) -> None:
        """Regression guard for `target`'s other half: a genuine scalar
        field (no referenced type) must keep `target is None` -- proves
        the qualified-target fix didn't turn `target` into an
        always-populated field."""
        obj = ObjectT(
            name="ns.Foo",
            isStruct=False,
            declarationFile="//foo.fbs",
            fields=[FieldT(name="x", type=TypeT(baseType=BaseType.Int), id=0)],
        )
        loaded = sr.load_bfbs(_pack_minimal_schema(objects=[obj]))
        self.assertIsNone(loaded.tables["ns.Foo"].fields[0].target)

    def test_union_variant_with_non_object_target_hard_errors(self) -> None:
        """The pinned flatc allows a union member's target
        type to be a STRING (idl_parser.cpp's ParseEnum accepts
        BASE_TYPE_STRING for a union value, not just BASE_TYPE_STRUCT), not
        modeled by this loader. A non-Object variant must hard-error, not
        silently construct `target=None` -- previously indistinguishable
        from "this is the already-filtered NONE=0 member" (a case that in
        fact never reaches UnionVariantDef construction at all)."""
        union_enum = EnumT(
            name="ns.Choice",
            isUnion=True,
            declarationFile="//foo.fbs",
            values=[
                EnumValT(name="NONE", value=0, unionType=TypeT(baseType=BaseType.None_)),
                EnumValT(name="AsString", value=1, unionType=TypeT(baseType=BaseType.String)),
            ],
        )
        with self.assertRaises(sr.SchemaLoadError) as ctx:
            sr.load_bfbs(_pack_minimal_schema(enums=[union_enum]))
        self.assertIn("unsupported variant kind", str(ctx.exception))


class PythonFastpathFactsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from fx_fastpath import Command, ScalarDefaults

        cls.command_module = Command
        cls.table_module = ScalarDefaults
        cls.loaded = _load_fixture(
            "message/tools/tests/schema_reflection_fixtures/python_fastpath.bfbs"
        )
        cls.table = cls.loaded.tables["fx_fastpath.ScalarDefaults"]
        cls.fields = {f.name: f for f in cls.table.fields}

    def test_struct_wire_layout_and_enum_scalar(self):
        command = self.loaded.structs["fx_fastpath.Command"]
        self.assertEqual((command.bytesize, command.minalign), (16, 16))
        self.assertEqual(
            [(f.name, f.wire_offset, f.base_type) for f in command.fields],
            [("mode", 0, BaseType.Byte), ("value", 4, BaseType.Float),
             ("counter", 8, BaseType.ULong)],
        )
        self.assertEqual(self.loaded.enums["fx_fastpath.Mode"].base_type, "byte")
        builder = flatbuffers.Builder(1)
        offset = self.command_module.CreateCommand(builder, 127, 1.25, 2**64 - 1)
        payload = bytes(builder.Bytes[builder.Head():])
        self.assertEqual(len(payload), command.bytesize)
        self.assertEqual(offset, command.bytesize)
        for field, fmt, value in zip(command.fields, ("b", "f", "Q"), (127, 1.25, 2**64 - 1)):
            self.assertEqual(struct.unpack_from("<" + fmt, payload, field.wire_offset)[0], value)

    def test_table_slots_and_existing_semantic_flags(self):
        self.assertIsNone(self.table.bytesize)
        self.assertIsNone(self.table.minalign)
        self.assertEqual([f.wire_offset for f in self.table.fields],
                         [4 + 2 * f.id for f in self.table.fields])
        self.assertEqual(self.fields["mode"].base_type, BaseType.Byte)
        self.assertEqual(self.fields["commands"].base_type, BaseType.Vector)
        self.assertTrue(self.fields["optional_float"].optional)
        # Existing model keeps the numeric default separately from optionality.
        self.assertEqual(self.fields["optional_float"].default, 0.0)
        self.assertTrue(self.fields["deprecated_value"].deprecated)

    def test_generated_default_oracle_preserves_bfbs_discrepancies(self):
        builder = flatbuffers.Builder(32)
        self.table_module.ScalarDefaultsStart(builder)
        builder.Finish(self.table_module.ScalarDefaultsEnd(builder))
        absent = self.table_module.ScalarDefaults.GetRootAs(builder.Output())
        facts = sr.python_scalar_facts(self.table, self.table_module.ScalarDefaults)
        self.assertEqual(self.fields["ulong_max"].default, 0)
        self.assertEqual(facts["ulong_max"].default, absent.UlongMax())
        self.assertEqual(facts["ulong_max"].default, 2**64 - 1)
        self.assertFalse(facts["ulong_max"].matches_bfbs)
        for name, getter in (("negative_float", absent.NegativeFloat),
                             ("negative_double", absent.NegativeDouble)):
            with self.subTest(field=name):
                self.assertEqual(math.copysign(1.0, self.fields[name].default), 1.0)
                self.assertEqual(struct.pack("<d", facts[name].default), struct.pack("<d", getter()))
                self.assertEqual(math.copysign(1.0, facts[name].default), -1.0)
                self.assertFalse(facts[name].matches_bfbs)
        self.assertTrue(math.isnan(facts["nan_float"].default))
        self.assertTrue(facts["nan_float"].matches_bfbs)
        self.assertIsNone(facts["optional_float"].default)
        self.assertTrue(facts["optional_float"].matches_bfbs)
        self.assertIs(facts["enabled"].default, True)
        self.assertTrue(facts["enabled"].matches_bfbs)
        self.assertEqual(facts["mode"].default, 1)
        self.assertTrue(facts["mode"].matches_bfbs)

    def test_oracle_rejects_shadowed_flatc_accessors(self):
        for suffix, accessor, expected in (("isnone", "FooIsNone", False),
                                            ("length", "FooLength", 3)):
            with self.subTest(collision=suffix):
                stem = "python_fastpath_" + suffix + "_collision"
                loaded = _load_fixture(
                    "message/tools/tests/schema_reflection_fixtures/" + stem + ".bfbs"
                )
                module = importlib.import_module("fx_fastpath_" + suffix + "_collision.Child")
                builder = flatbuffers.Builder(64)
                module.ChildStartFooVector(builder, 3)
                for value in (30, 20, 10):
                    builder.PrependInt32(value)
                vector = builder.EndVector()
                module.ChildStart(builder)
                getattr(module, "ChildAdd" + accessor)(builder, 7)
                module.ChildAddFoo(builder, vector)
                builder.Finish(module.ChildEnd(builder))
                child = module.Child.GetRootAs(builder.Output())
                self.assertEqual(getattr(child, accessor)(), expected)
                self.assertEqual(struct.unpack_from("<i", child._tab.Bytes,
                                                  child._tab.Pos + child._tab.Offset(4))[0], 7)
                with self.assertRaisesRegex(ValueError, "accessor"):
                    sr.python_scalar_facts(loaded.tables[module.__name__], module.Child)

    def test_oracle_rejects_replaced_live_callable(self):
        table = self.table_module.ScalarDefaults
        with patch.object(table, "Foo_bar", table.GpsPosition):
            with self.assertRaisesRegex(ValueError, "accessor"):
                sr.python_scalar_facts(self.table, table)

    def test_oracle_uses_actual_pinned_flatc_accessor_names(self):
        facts = sr.python_scalar_facts(self.table, self.table_module.ScalarDefaults)
        self.assertEqual(facts["foo__bar"].accessor, "Foo_bar")
        self.assertEqual(facts["GPS_position"].accessor, "GpsPosition")
        self.assertNotIn("deprecated_value", facts)
        self.assertNotIn("commands", facts)
        builder = flatbuffers.Builder(32)
        self.table_module.ScalarDefaultsStart(builder)
        self.table_module.ScalarDefaultsAddFoo_bar(builder, 7)
        self.table_module.ScalarDefaultsAddGpsPosition(builder, 11)
        builder.Finish(self.table_module.ScalarDefaultsEnd(builder))
        view = self.table_module.ScalarDefaults.GetRootAs(builder.Output())
        self.assertEqual(getattr(view, facts["foo__bar"].accessor)(), 7)
        self.assertEqual(getattr(view, facts["GPS_position"].accessor)(), 11)


if __name__ == "__main__":
    unittest.main()
